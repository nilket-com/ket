//! ADR 0699 S4: the ParentDecisions reader.
//!
//! A table query in this mode answers, for each key, the newest version whose
//! parent is recorded Accepted at the statement's cut K, and nothing else. The
//! answer is built from a positive inventory, never from whatever rows happen
//! to be indexed:
//! - K is the leader's read index after a linearizable barrier, and this node
//!   has applied at least K. Statuses are read at K, so a later decision never
//!   enlarges the statement's view.
//! - The inventory is every child of every parent Accepted at K for the table.
//!   Each child's complete local intent is checked against the recorded child
//!   (content, count, canonical order, owner of every version).
//! - The winners are chosen from the inventory before any predicate runs.
//!   Every winner must be stored here with the manifest's content, or the
//!   child is repaired from its plan, or the statement refuses. A partial
//!   answer is never given.
//! - The statement's read authority is checked before the scan, after each
//!   batch and before the success terminal. S5 attaches its certified
//!   generation and K_min checks to [`ReadAuthority::check`].

use super::child_writes::{child_request_meta, stored_row_digest, stored_row_projected, writer_of};
use super::*;
use crate::consensus::decision_mode::DecisionMode;
use crate::consensus::decisions::{ParentNamespace, ParentStatus, Signer};
use crate::network::decision_proposals::CutResponse;
use crate::storage::index::parent_intents::{OwnedVersion, ParentIntentState};

/// Where the executor's per-range rows come from: today's base scan, or rows
/// this reader selected and proved.
#[derive(Clone, Copy)]
pub(super) enum ScanSource<'a> {
	Base,
	Proved(&'a [Vec<(Identity, Locality, RecordLocation)>]),
}

/// How long a reader waits for its cut to be applied here.
const CUT_BOUND: Duration = Duration::from_secs(5);
/// The inventory bound when the request's own row budget is unbounded.
const CHECKPOINT_MAX_VERSIONS: u64 = 1 << 20;

/// Why a statement is not answered.
#[derive(Debug)]
pub(super) enum ReadRefusal {
	/// Retry: the cut, a child's copy or the read authority is not there yet.
	Transient(String),
	/// The inventory exceeds the statement's budget.
	Budget,
	/// Not served in this mode, or the record and the store disagree.
	Final(String),
	/// The statement's own end: cancelled, or past the caller's deadline.
	Status(ReadStatus),
}

impl ReadRefusal {
	fn plan(self, meta: &QueryRequestMeta, local: NodeId) -> std::io::Result<QueryExecutionPlan> {
		match self {
			ReadRefusal::Transient(why) => {
				tracing::debug!(reason = %why, "ParentDecisions read refused, retryable");
				Ok(query_plan_error(meta, ReadStatus::BACKPRESSURED, local))
			}
			ReadRefusal::Budget => Ok(query_plan_error(
				meta,
				ReadStatus::QUERY_BUDGET_EXCEEDED,
				local,
			)),
			ReadRefusal::Status(status) => Ok(query_plan_error(meta, status, local)),
			ReadRefusal::Final(why) => Err(Error::new(
				ErrorKind::Unsupported,
				format!("ParentDecisions read: {why}"),
			)),
		}
	}
}

/// The statement's own bounds: the caller's deadline, derived once at entry,
/// and its cancellation.
struct StatementBound<'a> {
	deadline: Option<Instant>,
	cancellation: &'a CancellationToken,
}

impl StatementBound<'_> {
	fn check(&self) -> Result<(), ReadRefusal> {
		if self.cancellation.is_cancelled() {
			return Err(ReadRefusal::Status(ReadStatus::QUERY_CANCELLED));
		}
		if self
			.deadline
			.is_some_and(|deadline| Instant::now() >= deadline)
		{
			return Err(ReadRefusal::Status(ReadStatus::QUERY_DEADLINE_EXCEEDED));
		}
		Ok(())
	}

	/// `work`, unless the statement ends first. Dropping `work` abandons no owned
	/// mutation: a repair's persistence runs in its own task under the parent's
	/// lock and finishes.
	async fn run<T>(&self, work: impl std::future::Future<Output = T>) -> Result<T, ReadRefusal> {
		self.check()?;
		let deadline = async {
			match self.deadline {
				Some(deadline) => {
					tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await
				}
				None => std::future::pending().await,
			}
		};
		tokio::select! {
			biased;
			_ = self.cancellation.cancelled() => Err(ReadRefusal::Status(ReadStatus::QUERY_CANCELLED)),
			_ = deadline => Err(ReadRefusal::Status(ReadStatus::QUERY_DEADLINE_EXCEEDED)),
			done = work => Ok(done),
		}
	}
}

/// The S4 portion of a statement's read authority: no local distrust, tier
/// authority ready, this store admitted at its signer's incarnation and
/// serving in the last configuration, and K applied.
#[derive(Clone)]
pub(super) struct ReadAuthority {
	slab: Arc<SlabManager>,
	state_machine: WendelonStateMachine,
	distrust: Arc<RwLock<Option<String>>>,
	signer: Signer,
	k: u64,
	/// ADR 0699 S5a: the local base and the behind-boundary refusal.
	index: Option<Arc<crate::storage::index::PersistentLocalIndex>>,
	behind: Arc<std::sync::atomic::AtomicBool>,
	validated: Arc<std::sync::atomic::AtomicBool>,
	#[cfg(test)]
	checks_left: Arc<std::sync::atomic::AtomicUsize>,
}

impl ReadAuthority {
	pub(super) async fn check(&self) -> Result<(), String> {
		#[cfg(test)]
		if self
			.checks_left
			.fetch_update(
				std::sync::atomic::Ordering::AcqRel,
				std::sync::atomic::Ordering::Acquire,
				|left| left.checked_sub(1),
			)
			.is_err()
		{
			return Err("test: read authority withdrawn".into());
		}
		if let Some(reason) =
			recover_rwlock_read(&self.distrust, "local_persistence_distrust_reason").clone()
		{
			return Err(reason);
		}
		if let Some(reason) = self.slab.tier_authority_readiness_reason() {
			return Err(reason);
		}
		let (signer, k) = (self.signer, self.k);
		self.state_machine
			.read_state(move |state| {
				let current = state
					.incarnations
					.get(&signer.node)
					.and_then(|registry| registry.current);
				if current != Some(signer.incarnation) {
					return Err("this store is not the admitted incarnation".to_string());
				}
				let serving = state
					.epoch_history
					.last_key_value()
					.is_some_and(|(_, config)| {
						config
							.peers
							.values()
							.any(|peer| peer.node_id.id == signer.node)
					});
				if !serving {
					return Err("this node is not in the serving configuration".into());
				}
				if state.last_applied_log_id.map_or(0, |id| id.index) < k {
					return Err("the cut is not applied".into());
				}
				let generations = state
					.decisions
					.as_ref()
					.map(|decisions| decisions.generations().clone())
					.unwrap_or_default();
				if k < generations.k_min() {
					return Err("cut expired: below K_min".into());
				}
				Ok(generations.current_gid())
			})
			.await
			.and_then(|current_gid| {
				if self.behind.load(std::sync::atomic::Ordering::Acquire) {
					return Err("this store is behind the certified boundary".into());
				}
				if !self.validated.load(std::sync::atomic::Ordering::Acquire) {
					return Err("the certified base is not validated since this start".into());
				}
				Ok(current_gid)
			})
			.map(|current_gid| (current_gid, self.index.clone()))
			.map(|(current_gid, index)| async move {
				// The local base must be the committed generation's.
				match index {
					None => Ok(()),
					Some(index) => match index.base_meta().await {
						Ok(meta) if meta.active_gid == current_gid => Ok(()),
						Ok(_) => Err("the committed generation is not active here".to_string()),
						Err(error) => Err(error.to_string()),
					},
				}
			})?
			.await
	}

	/// The writer's per-batch hook for this authority.
	fn read_check(&self) -> super::table_query::ReadCheck {
		let authority = self.clone();
		Box::new(move || {
			let authority = authority.clone();
			Box::pin(async move {
				authority.check().await.map_err(|reason| {
					Error::new(
						ErrorKind::Interrupted,
						format!("ParentDecisions read authority lost: {reason}"),
					)
				})
			})
		})
	}
}

/// One child of a parent Accepted at K.
#[derive(Clone, Debug)]
pub(super) struct AcceptedChild {
	pub(super) rsv: u64,
	pub(super) ordinal: u16,
	pub(super) claim_digest: [u8; 32],
	pub(super) rows: u32,
	pub(super) content: [u8; 32],
	pub(super) commit_hlc: HybridTimestamp,
}

/// Every child of every parent Accepted at `k` for the table, checked to name
/// this store in its plan and bounded by `max_versions`.
fn accepted_children(
	state: &SmState,
	tenant: TenantId,
	table: TableObjectId,
	k: u64,
	local: Signer,
	max_versions: u64,
) -> Result<Vec<AcceptedChild>, ReadRefusal> {
	accepted_children_in(
		state,
		tenant,
		ParentNamespace::Table(table),
		None,
		k,
		local,
		max_versions,
	)
}

/// The inventory of a namespace at `k`, optionally only the children whose
/// range covers one position (a KV point read).
fn accepted_children_in(
	state: &SmState,
	tenant: TenantId,
	namespace: ParentNamespace,
	position: Option<u64>,
	k: u64,
	local: Signer,
	max_versions: u64,
) -> Result<Vec<AcceptedChild>, ReadRefusal> {
	let decisions = state
		.decisions
		.as_ref()
		.ok_or_else(|| ReadRefusal::Final("no decision state".into()))?;
	let mut children = Vec::new();
	let mut versions = 0u64;
	for body in decisions.bodies().range(..=k).map(|(_, body)| body) {
		if body.claim.key.tenant != tenant
			|| body.claim.key.namespace != namespace
			|| !matches!(body.status_at(k), Some(ParentStatus::Accepted { .. }))
		{
			continue;
		}
		for (ordinal, child) in body.claim.children.iter().enumerate() {
			if position
				.is_some_and(|position| position < child.range_start || child.range_end < position)
			{
				continue;
			}
			if !child.plan.contains(&local) {
				return Err(ReadRefusal::Final(
					"a child's plan excludes this store; this checkpoint reads only complete local copies"
						.into(),
				));
			}
			versions = versions
				.checked_add(u64::from(child.rows))
				.filter(|versions| *versions <= max_versions)
				.ok_or(ReadRefusal::Budget)?;
			children.push(AcceptedChild {
				rsv: body.rsv,
				ordinal: ordinal as u16,
				claim_digest: body.claim_digest,
				rows: child.rows,
				content: child.content,
				commit_hlc: body.claim.commit_hlc,
			});
		}
	}
	Ok(children)
}

/// The routing locality of a selected version, derived from its own verified
/// bytes by the admission the writer ran, never from the key's current
/// locality, which a later version may have moved.
pub(super) fn version_locality(
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	key: Identity,
	hlc: HybridTimestamp,
	stored: &RecordBatch,
) -> Result<Locality, ReadRefusal> {
	let fail = |why: String| ReadRefusal::Final(format!("a selected version's locality: {why}"));
	let row = stored_row_projected(descriptor, stored).map_err(fail)?;
	let probe = crate::consensus::decisions::ParentKey {
		tenant,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: [0; 16],
		sequence_no: 1,
	};
	let meta = child_request_meta(
		&probe,
		descriptor.table_object_id,
		descriptor.schema_generation,
		1,
		hlc,
	)
	.map_err(|error| fail(error.to_string()))?;
	let admission =
		validate_table_write_request_batch(&meta, &row, descriptor, writer_of(hlc), hlc)
			.map_err(|error| fail(error.to_string()))?;
	if admission.key_identities.first() != Some(&key) {
		return Err(fail("the stored row is another key".into()));
	}
	admission
		.localities
		.first()
		.copied()
		.ok_or_else(|| fail("no locality".into()))
}

/// A KV key's table identity and ring position, by the admission the KV write
/// ran over the default KV table.
fn kv_identity_and_position(
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	key: &[u8],
) -> Result<(Identity, u64), String> {
	let batch = RecordBatch::try_new(
		Arc::new(
			descriptor
				.arrow_schema()
				.map_err(|error| error.to_string())?,
		),
		vec![
			Arc::new(arrow::array::LargeBinaryArray::from(vec![key])),
			Arc::new(arrow::array::LargeBinaryArray::from(vec![&b""[..]])),
		],
	)
	.map_err(|error| error.to_string())?;
	let probe = crate::consensus::decisions::ParentKey {
		tenant,
		namespace: ParentNamespace::Kv,
		client_uuid: [0; 16],
		sequence_no: 1,
	};
	let hlc = HybridTimestamp {
		physical_ms: 1,
		logical: 0,
		node_id: 0,
	};
	let meta = child_request_meta(&probe, TableObjectId::DEFAULT_KV, 0, 1, hlc)
		.map_err(|error| error.to_string())?;
	let admission =
		validate_table_write_request_batch(&meta, &batch, descriptor, writer_of(hlc), hlc)
			.map_err(|error| error.to_string())?;
	let identity = *admission.key_identities.first().ok_or("no identity")?;
	let locality = *admission.localities.first().ok_or("no locality")?;
	let position = super::coordinator::row_position(descriptor, locality)
		.map_err(|error| error.to_string())?;
	Ok((identity, position))
}

/// Whether a child's local copy is usable, needs repair, or contradicts the record.
enum LocalChild {
	Ready(Vec<OwnedVersion>),
	Missing,
}

/// A selected version and the child it comes from.
#[derive(Clone, Copy)]
struct Winner {
	hlc: HybridTimestamp,
	row_digest: [u8; 32],
	/// The retained child it comes from; `None` for a certified base entry.
	child: Option<usize>,
	/// Another parent's version at the same full HLC.
	conflict: bool,
}

fn merge_winner(winner: Option<Winner>, candidate: Winner) -> Winner {
	match winner {
		Some(current) if current.hlc > candidate.hlc => current,
		Some(current) if current.hlc == candidate.hlc => Winner {
			conflict: true,
			..current
		},
		_ => candidate,
	}
}

/// A certified base entry as a winner candidate: no retained parent.
fn base_winner(entry: &crate::storage::index::certified_base::BaseEntry) -> Winner {
	Winner {
		hlc: entry.hlc,
		row_digest: entry.row_digest,
		child: None,
		conflict: false,
	}
}

impl WendelonDataServer {
	/// The child's complete local intent, checked against the recorded child.
	async fn local_child(
		&self,
		index: &crate::storage::index::PersistentLocalIndex,
		table: TableObjectId,
		tenant: TenantId,
		child: &AcceptedChild,
	) -> Result<LocalChild, ReadRefusal> {
		let storage = |error: std::io::Error| ReadRefusal::Transient(error.to_string());
		let Some(intent) = index
			.parent_intent(child.rsv, child.ordinal)
			.await
			.map_err(storage)?
		else {
			return Ok(LocalChild::Missing);
		};
		if intent.state != ParentIntentState::Complete {
			return Ok(LocalChild::Missing);
		}
		let manifest = &intent.manifest;
		let canonical = manifest.windows(2).all(|pair| pair[0] < pair[1]);
		if intent.tenant != tenant
			|| intent.claim_digest != child.claim_digest
			|| intent.content != child.content
			|| manifest.len() != child.rows as usize
			|| !canonical
			|| super::child_writes::manifest_content(manifest) != child.content
			|| manifest
				.iter()
				.any(|version| version.table != table || version.hlc != child.commit_hlc)
		{
			return Err(ReadRefusal::Final(format!(
				"the complete intent of ({}, {}) disagrees with its recorded child",
				child.rsv, child.ordinal
			)));
		}
		for version in manifest {
			if index
				.version_owner(tenant, *version)
				.await
				.map_err(storage)?
				!= Some((child.rsv, child.ordinal))
			{
				return Err(ReadRefusal::Final(format!(
					"a version of ({}, {}) is not owned by it",
					child.rsv, child.ordinal
				)));
			}
		}
		Ok(LocalChild::Ready(intent.manifest))
	}

	/// The child's manifest, repairing the child once if the local copy is
	/// missing or incomplete.
	pub(super) async fn proved_manifest(
		&self,
		index: &crate::storage::index::PersistentLocalIndex,
		table: TableObjectId,
		tenant: TenantId,
		generation: u64,
		child: &AcceptedChild,
	) -> Result<Vec<OwnedVersion>, ReadRefusal> {
		if let LocalChild::Ready(manifest) = self.local_child(index, table, tenant, child).await? {
			return Ok(manifest);
		}
		#[cfg(test)]
		self.decisions
			.pause_at(super::child_writes::PauseStage::ReaderBeforeRepair)
			.await;
		self.repair_child(child.rsv, child.ordinal, generation)
			.await
			.map_err(|refusal| {
				ReadRefusal::Transient(format!(
					"({}, {}) could not be repaired: {refusal:?}",
					child.rsv, child.ordinal
				))
			})?;
		match self.local_child(index, table, tenant, child).await? {
			LocalChild::Ready(manifest) => Ok(manifest),
			LocalChild::Missing => Err(ReadRefusal::Transient(format!(
				"({}, {}) is still incomplete after repair",
				child.rsv, child.ordinal
			))),
		}
	}

	/// One version's stored location here, every copy checked against its
	/// recorded content (a tombstone by its typed locations). `None` when it is
	/// absent or differs. The certified-base build proves its survivors with it.
	pub(super) async fn proved_version(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		key: Identity,
		hlc: HybridTimestamp,
		row_digest: [u8; 32],
	) -> Result<Option<(Locality, RecordLocation)>, ReadRefusal> {
		let winner = Winner {
			hlc,
			row_digest,
			child: None,
			conflict: false,
		};
		self.proved_location(tenant, descriptor, key, &winner).await
	}

	/// The winner's stored location here, every copy checked against the
	/// manifest's content. `None` when it is absent or differs.
	async fn proved_location(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		key: Identity,
		winner: &Winner,
	) -> Result<Option<(Locality, RecordLocation)>, ReadRefusal> {
		let transient = |error: std::io::Error| ReadRefusal::Transient(error.to_string());
		let locations = self
			.slab
			.version_locations(tenant, key, winner.hlc)
			.map_err(transient)?;
		let Some(first) = locations.first().copied() else {
			return Ok(None);
		};
		if locations.iter().all(|location| location.is_tombstone) {
			// Suppressed: never placed in a range.
			return Ok(Some((Locality::from(key), first)));
		}
		let stored = self
			.slab
			.stored_rows_at_version(tenant, key, winner.hlc)
			.await
			.map_err(transient)?;
		for row in &stored {
			match stored_row_digest(descriptor, row) {
				Ok(digest) if digest == winner.row_digest => {}
				_ => return Ok(None),
			}
		}
		let Some(row) = stored.first() else {
			return Ok(None);
		};
		Ok(Some((
			version_locality(tenant, descriptor, key, winner.hlc, row)?,
			first,
		)))
	}

	/// The statement's answer set: each key's newest Accepted version at K,
	/// proved stored here, bucketed into the request's ranges.
	#[allow(clippy::too_many_arguments)]
	async fn prove_rows(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		ranges: &[QueryScanRange],
		k: u64,
		local: Signer,
		max_versions: u64,
	) -> Result<Vec<Vec<(Identity, Locality, RecordLocation)>>, ReadRefusal> {
		let table = descriptor.table_object_id;
		let children = self
			.state_machine
			.read_state(move |state| {
				accepted_children(state, tenant, table, k, local, max_versions)
			})
			.await?;
		let index = self
			.slab
			.persistent_index()
			.ok_or_else(|| ReadRefusal::Final("no persistent index".into()))?;
		let mut winners: HashMap<Identity, Winner> = HashMap::new();
		for (position, child) in children.iter().enumerate() {
			let manifest = self
				.proved_manifest(&index, table, tenant, descriptor.schema_generation, child)
				.await?;
			for version in manifest {
				let candidate = Winner {
					hlc: version.hlc,
					row_digest: version.row_digest,
					child: Some(position),
					conflict: false,
				};
				winners
					.entry(Identity::new(version.key_identity))
					.and_modify(|current| {
						if candidate.hlc > current.hlc {
							*current = candidate;
						} else if candidate.hlc == current.hlc {
							current.conflict = true;
						}
					})
					.or_insert(candidate);
			}
		}
		// The certified base: one entry per key, below every retained parent.
		for entry in index
			.base_entries(false)
			.await
			.map_err(|error| ReadRefusal::Transient(error.to_string()))?
			.into_iter()
			.filter(|entry| entry.tenant == tenant && entry.table == table)
		{
			let key = Identity::new(entry.key_identity);
			let merged = merge_winner(winners.get(&key).copied(), base_winner(&entry));
			winners.insert(key, merged);
		}
		if winners.values().any(|winner| winner.conflict) {
			return Err(ReadRefusal::Final(
				"two parents hold one key at an equal full HLC: no winner".into(),
			));
		}
		let mut buckets = vec![Vec::new(); ranges.len()];
		let mut repaired = std::collections::BTreeSet::new();
		for (key, winner) in &winners {
			let mut proved = self
				.proved_location(tenant, descriptor, *key, winner)
				.await?;
			if proved.is_none()
				&& let Some(at) = winner.child
				&& repaired.insert(at)
			{
				let child = &children[at];
				self.repair_child(child.rsv, child.ordinal, descriptor.schema_generation)
					.await
					.map_err(|refusal| {
						ReadRefusal::Transient(format!(
							"({}, {}) lacks a verified stored row and could not be repaired: {refusal:?}",
							child.rsv, child.ordinal
						))
					})?;
				proved = self
					.proved_location(tenant, descriptor, *key, winner)
					.await?;
			}
			let Some((locality, location)) = proved else {
				return Err(ReadRefusal::Transient(
					"an Accepted version is not stored here with its content".into(),
				));
			};
			// A tombstone wins its key and suppresses it.
			if location.is_tombstone {
				continue;
			}
			if let Some(bucket) = ranges
				.iter()
				.position(|range| range.start <= locality && locality <= range.end)
			{
				buckets[bucket].push((*key, locality, location));
			}
		}
		for bucket in &mut buckets {
			bucket.sort_by_key(|(key, locality, _)| (*locality, *key));
		}
		Ok(buckets)
	}

	/// A table query under ParentDecisions. Only a streamed LINEARIZABLE read
	/// of the latest state is served, by complete inventory, with no migration
	/// pull and no index plan.
	pub(super) async fn parent_decisions_table_query(
		&self,
		meta: &TableQueryRequestMeta,
		bytecode: &[u8],
		runtime_filter: DecodedRuntimeFilter,
		cancellation_token: &CancellationToken,
		stream_writer: Option<&mut TableQueryResponseStreamWriter<'_>>,
	) -> std::io::Result<QueryExecutionPlan> {
		let range_meta = table_query_as_range_query(meta);
		let local = self.local_node;
		let refuse = |why: &str| ReadRefusal::Final(why.into()).plan(&range_meta, local);
		// The caller's deadline, derived once: the cut, the proof, any repair and
		// the execution all spend the same one.
		let budget = table_query_execution_budget(meta);
		let bound = StatementBound {
			deadline: budget.caller_deadline,
			cancellation: cancellation_token,
		};
		let Some(writer) = stream_writer else {
			return refuse("only a streamed table query is served");
		};
		if meta.consistency != Consistency::LINEARIZABLE {
			return refuse("only LINEARIZABLE reads are served");
		}
		if meta.as_of_hlc
			!= (HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			}) {
			return refuse("historical reads are not served");
		}
		let (Some(proposals), Some(signer)) = (
			self.decisions.proposals.clone(),
			self.decisions.signer.clone(),
		) else {
			return refuse("no decision authority");
		};
		let row_limit = match table_query_row_limit(meta) {
			Ok(row_limit) => row_limit,
			Err(status) => return Ok(query_plan_error(&range_meta, status, local)),
		};
		let Some(row_identity_projection) =
			RowIdentityProjection::from_u8(meta.row_identity_projection)
		else {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_INVALID_PROGRAM,
				local,
			));
		};
		if meta.expected_epoch < self.admission_snapshot().current_epoch {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::EPOCH_STALE,
				local,
			));
		}
		if let Err(status) = self.enforce_read_policy(meta.tenant_id).await {
			return Ok(query_plan_error(&range_meta, status, local));
		}

		// K: the leader's read index, applied here.
		let cut = match bound.run(proposals.cut()).await {
			Ok(cut) => cut,
			Err(refusal) => return refusal.plan(&range_meta, local),
		};
		let k = match cut {
			CutResponse::Cut(k) => k,
			CutResponse::NotLeader { .. } | CutResponse::Indeterminate(_) => {
				return ReadRefusal::Transient("no cut".into()).plan(&range_meta, local);
			}
			CutResponse::Refused(refusal) => {
				return refuse(&format!("the cut was refused: {refusal:?}"));
			}
		};
		match bound.run(proposals.wait_applied(k, CUT_BOUND)).await {
			Ok(Ok(())) => {}
			Ok(Err(error)) => {
				return ReadRefusal::Transient(error.to_string()).plan(&range_meta, local);
			}
			Err(refusal) => return refusal.plan(&range_meta, local),
		}
		#[cfg(test)]
		self.decisions
			.pause_at(super::child_writes::PauseStage::ReaderAfterCut)
			.await;
		let authority = ReadAuthority {
			slab: self.slab.clone(),
			state_machine: self.state_machine.clone(),
			distrust: self.local_persistence_distrust_reason.clone(),
			signer: signer.signer,
			k,
			index: self.slab.persistent_index(),
			behind: self.decisions.behind_boundary.clone(),
			validated: self.decisions.base_validated.clone(),
			#[cfg(test)]
			checks_left: self.decisions.read_checks_left.clone(),
		};
		if let Err(refusal) = bound.check() {
			return refusal.plan(&range_meta, local);
		}
		if let Err(reason) = authority.check().await {
			return ReadRefusal::Transient(reason).plan(&range_meta, local);
		}

		// The request against the state at K.
		let (tenant, table, generation) =
			(meta.tenant_id, meta.table_object_id, meta.schema_generation);
		let checked = self
			.state_machine
			.read_state(move |state| {
				if state.table_drop_blocks_admission(tenant, table) {
					return Err(ReadStatus::QUERY_INVALID_PROGRAM);
				}
				let latest = state
					.table_schemas
					.latest_by_id(tenant, table)
					.ok_or(ReadStatus::QUERY_INVALID_PROGRAM)?;
				// Children do not record their generation; one generation per
				// table is what this checkpoint reads.
				if latest.schema_generation != generation
					|| (1..generation)
						.any(|older| state.table_schemas.get(tenant, table, older).is_some())
				{
					return Err(ReadStatus::TABLE_DESCRIPTOR_EVOLVED);
				}
				let migrating = state
					.migrations
					.values()
					.any(|entry| entry.state != MigrationState::Completed);
				Ok((latest.clone(), migrating))
			})
			.await;
		let (descriptor, migrating) = match checked {
			Ok(checked) => checked,
			Err(status) => return Ok(query_plan_error(&range_meta, status, local)),
		};
		if migrating {
			return refuse("a migration is in flight");
		}
		if meta.physical_descriptor_fingerprint
			!= *descriptor.physical_descriptor_fingerprint.as_bytes()
		{
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::TABLE_DESCRIPTOR_EVOLVED,
				local,
			));
		}
		let effective_retention = self.table_effective_retention_from_state(
			&self.state_machine.state_snapshot().await,
			tenant,
			table,
		);
		// ADR 0699 S5a: the certified survivor rule keeps every winning version;
		// a retention floor would hide some. Frozen as none in this mode.
		if effective_retention.floor.is_some() {
			return refuse("a retention floor is not served under ParentDecisions");
		}
		let Ok(program) = pbc::Program::compile(bytecode) else {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_INVALID_PROGRAM,
				local,
			));
		};
		let ranges = match table_query_scan_ranges(tenant, &descriptor, Some(&program)) {
			Ok(ranges) => ranges,
			Err(status) => return Ok(query_plan_error(&range_meta, status, local)),
		};
		let Ok(schema) = descriptor.arrow_schema() else {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_INVALID_PROGRAM,
				local,
			));
		};
		let user_column_count = schema.fields().len();
		let Ok(mut requirement) = analyze_pbc_column_requirement(&program, user_column_count)
		else {
			return Ok(query_plan_error(
				&range_meta,
				ReadStatus::QUERY_INVALID_PROGRAM,
				local,
			));
		};
		let runtime_filter = prepare_runtime_filter_application(
			&runtime_filter,
			&descriptor,
			&schema,
			&mut requirement,
			user_column_count,
		);
		let projected_execution = match row_identity_projection {
			RowIdentityProjection::None => QueryProjectedExecution::Pbc {
				requirement,
				user_column_count,
				runtime_filter,
			},
			RowIdentityProjection::Append => QueryProjectedExecution::Disabled,
		};

		// The answer set, proved, then executed under the same authority and the
		// same deadline.
		let max_versions = if budget.max_rows == u64::MAX {
			CHECKPOINT_MAX_VERSIONS
		} else {
			budget.max_rows.min(CHECKPOINT_MAX_VERSIONS)
		};
		let proved = bound
			.run(self.prove_rows(tenant, &descriptor, &ranges, k, signer.signer, max_versions))
			.await;
		let rows = match proved.and_then(|proved| proved) {
			Ok(rows) => rows,
			Err(refusal) => return refusal.plan(&range_meta, local),
		};
		if let Err(refusal) = bound.check() {
			return refusal.plan(&range_meta, local);
		}
		if let Err(reason) = authority.check().await {
			return ReadRefusal::Transient(reason).plan(&range_meta, local);
		}
		if !writer.set_read_check(authority.read_check()) {
			return refuse("the stream carries no read check");
		}
		let plan = plan_query_execution_for_ranges_with_program_maybe_cancelled(
			&self.slab,
			local,
			&range_meta,
			&program,
			&ranges,
			budget,
			row_limit,
			Some(&descriptor),
			projected_execution,
			row_identity_projection,
			effective_retention.floor,
			Some(cancellation_token),
			Some(writer),
			ScanSource::Proved(&rows),
		)
		.await?;
		// No success after the statement ended, an empty answer included.
		if plan.response.status == ReadStatus::OK
			|| plan.response.status == ReadStatus::KEY_NOT_FOUND
		{
			if let Err(refusal) = bound.check() {
				return refusal.plan(&range_meta, local);
			}
		}
		Ok(plan)
	}

	/// Test-only: cancel a registered table query, as its cancel frame would.
	#[cfg(test)]
	pub(crate) fn cancel_table_query_for_test(
		&self,
		tenant: TenantId,
		request_id: crate::schema::TableQueryRequestId,
	) {
		let _ = self.table_query_cancellation.cancel(tenant, request_id);
	}

	/// A row's winner proved stored here: a tombstone by its typed locations, a
	/// live row by its bytes.
	async fn winner_proved(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		row: Identity,
		winner: &Winner,
		typed_tombstone: bool,
	) -> Result<bool, ReadRefusal> {
		if typed_tombstone {
			let locations = self
				.slab
				.version_locations(tenant, row, winner.hlc)
				.map_err(|error| ReadRefusal::Transient(error.to_string()))?;
			return Ok(
				!locations.is_empty() && locations.iter().all(|location| location.is_tombstone)
			);
		}
		Ok(self
			.proved_location(tenant, descriptor, row, winner)
			.await?
			.is_some_and(|(_, location)| !location.is_tombstone))
	}

	/// ADR 0699 S4b: one row's proved state at `k`, for a delete's evaluation:
	/// the newest Accepted version of exactly `row` over the complete point
	/// inventory of its table, proved stored here (repaired once if not). A
	/// live winner is `Deleted`, a tombstone `AlreadyDeleted`, none `NotFound`.
	/// Anything unproved is an error, never `NotFound`.
	pub(super) async fn proved_point(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		row: Identity,
		position: u64,
		k: u64,
	) -> Result<crate::consensus::decisions::DeleteOutcome, ReadRefusal> {
		use crate::consensus::decisions::DeleteOutcome;
		let signer = self
			.decisions
			.signer
			.clone()
			.ok_or_else(|| ReadRefusal::Final("no decision authority".into()))?;
		let authority = ReadAuthority {
			slab: self.slab.clone(),
			state_machine: self.state_machine.clone(),
			distrust: self.local_persistence_distrust_reason.clone(),
			signer: signer.signer,
			k,
			index: self.slab.persistent_index(),
			behind: self.decisions.behind_boundary.clone(),
			validated: self.decisions.base_validated.clone(),
			#[cfg(test)]
			checks_left: self.decisions.read_checks_left.clone(),
		};
		authority.check().await.map_err(ReadRefusal::Transient)?;
		let (table, local) = (descriptor.table_object_id, signer.signer);
		let children = self
			.state_machine
			.read_state(move |state| {
				accepted_children_in(
					state,
					tenant,
					ParentNamespace::Table(table),
					Some(position),
					k,
					local,
					CHECKPOINT_MAX_VERSIONS,
				)
			})
			.await?;
		let index = self
			.slab
			.persistent_index()
			.ok_or_else(|| ReadRefusal::Final("no persistent index".into()))?;
		let mut winner: Option<Winner> = None;
		for (at, child) in children.iter().enumerate() {
			let manifest = self
				.proved_manifest(&index, table, tenant, descriptor.schema_generation, child)
				.await?;
			for version in manifest
				.iter()
				.filter(|version| version.key_identity == *row.as_bytes())
			{
				let candidate = Winner {
					hlc: version.hlc,
					row_digest: version.row_digest,
					child: Some(at),
					conflict: false,
				};
				winner = Some(match winner {
					Some(current) if current.hlc > candidate.hlc => current,
					Some(current) if current.hlc == candidate.hlc => Winner {
						conflict: true,
						..current
					},
					_ => candidate,
				});
			}
		}
		if let Some(entry) = index
			.base_entry(tenant, table, *row.as_bytes())
			.await
			.map_err(|error| ReadRefusal::Transient(error.to_string()))?
		{
			winner = Some(merge_winner(winner, base_winner(&entry)));
		}
		let Some(winner) = winner else {
			// Absence is an evaluated answer: the same final authority applies.
			authority.check().await.map_err(ReadRefusal::Transient)?;
			return Ok(DeleteOutcome::NotFound);
		};
		if winner.conflict {
			return Err(ReadRefusal::Final(
				"two parents hold the row at an equal full HLC".into(),
			));
		}
		let child = winner.child.map(|at| &children[at]);
		let typed_tombstone = super::deletes::is_tombstone_version(
			descriptor,
			&crate::storage::index::parent_intents::OwnedVersion {
				table,
				key_identity: *row.as_bytes(),
				hlc: winner.hlc,
				row_digest: winner.row_digest,
			},
		);
		if !self
			.winner_proved(tenant, descriptor, row, &winner, typed_tombstone)
			.await?
		{
			// A certified base winner has no retained parent to repair from.
			let Some(child) = child else {
				return Err(ReadRefusal::Transient(
					"a certified base version is not stored here".into(),
				));
			};
			self.repair_child(child.rsv, child.ordinal, descriptor.schema_generation)
				.await
				.map_err(|refusal| ReadRefusal::Transient(format!("{refusal:?}")))?;
			if !self
				.winner_proved(tenant, descriptor, row, &winner, typed_tombstone)
				.await?
			{
				return Err(ReadRefusal::Transient(
					"the row's winner is not proved stored here".into(),
				));
			}
		}
		authority.check().await.map_err(ReadRefusal::Transient)?;
		// A certified base winner's parent was erased: rsv 0 names the base.
		let winner_rsv = child.map_or(0, |child| child.rsv);
		Ok(if typed_tombstone {
			DeleteOutcome::AlreadyDeleted {
				winner_rsv,
				winner_hlc: winner.hlc,
			}
		} else {
			DeleteOutcome::Deleted {
				winner_rsv,
				winner_hlc: winner.hlc,
			}
		})
	}

	/// ADR 0699 S4b: a single-key KV read under ParentDecisions. Only a
	/// LINEARIZABLE read of the latest state, at a cut from the leader. The
	/// winner is the newest Accepted version of exactly this key, proved stored
	/// here or repaired; no Accepted version answers KEY_NOT_FOUND.
	pub(crate) async fn parent_decisions_kv_read(
		&self,
		meta: ReadRequestMeta,
		key: RecordBatch,
	) -> std::io::Result<(ReadResponseMeta, Option<RecordBatch>)> {
		let local = self.local_node;
		let respond = |status| Ok((error_read_response(&meta, status, local), None));
		let unsupported = |why: &str| {
			Err(Error::new(
				ErrorKind::Unsupported,
				format!("ParentDecisions KV read: {why}"),
			))
		};
		let zero = HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		};
		if meta.consistency != Consistency::LINEARIZABLE || meta.as_of_hlc != zero {
			return unsupported("only a LINEARIZABLE read of the latest state is served");
		}
		// A causal lower bound is not proved by a log cut: not served here.
		if meta.min_hlc != zero {
			return unsupported("a nonzero min_hlc is not served in this checkpoint");
		}
		if meta.expected_epoch < self.admission_snapshot().current_epoch {
			return respond(ReadStatus::EPOCH_STALE);
		}
		if let Err(status) = self.enforce_read_policy(meta.tenant_id).await {
			return respond(status);
		}
		let (Some(proposals), Some(signer)) = (
			self.decisions.proposals.clone(),
			self.decisions.signer.clone(),
		) else {
			return unsupported("no decision authority");
		};
		let tenant = meta.tenant_id;
		let Some(key_bytes) = key
			.column_by_name("key")
			.and_then(|column| {
				column
					.as_any()
					.downcast_ref::<arrow::array::LargeBinaryArray>()
			})
			.filter(|column| key.num_rows() == 1 && column.is_valid(0))
			.map(|column| column.value(0).to_vec())
		else {
			return unsupported("one key");
		};
		if Identity::blake3_tenant(tenant, &key_bytes) != meta.key_identity {
			return Err(Error::new(ErrorKind::InvalidData, "READ_REQ key identity"));
		}
		// The key's table identity and position, as the KV write derived them.
		let Ok(descriptor) = crate::schema::table::default_kv_table_descriptor_v1() else {
			return unsupported("the KV descriptor");
		};
		let (identity, position) = match kv_identity_and_position(tenant, &descriptor, &key_bytes) {
			Ok(found) => found,
			Err(why) => return unsupported(&why),
		};
		#[cfg(test)]
		let position = match self.decisions.kv_position_override.lock().unwrap().as_ref() {
			Some((overridden, at)) if *overridden == key_bytes => *at,
			_ => position,
		};
		let k = match proposals.cut().await {
			CutResponse::Cut(k) => k,
			CutResponse::Refused(refusal) => return unsupported(&format!("{refusal:?}")),
			_ => return respond(ReadStatus::BACKPRESSURED),
		};
		if proposals.wait_applied(k, CUT_BOUND).await.is_err() {
			return respond(ReadStatus::BACKPRESSURED);
		}
		let authority = ReadAuthority {
			slab: self.slab.clone(),
			state_machine: self.state_machine.clone(),
			distrust: self.local_persistence_distrust_reason.clone(),
			signer: signer.signer,
			k,
			index: self.slab.persistent_index(),
			behind: self.decisions.behind_boundary.clone(),
			validated: self.decisions.base_validated.clone(),
			#[cfg(test)]
			checks_left: self.decisions.read_checks_left.clone(),
		};
		if authority.check().await.is_err() {
			return respond(ReadStatus::BACKPRESSURED);
		}
		let local_signer = signer.signer;
		let children = match self
			.state_machine
			.read_state(move |state| {
				accepted_children_in(
					state,
					tenant,
					ParentNamespace::Kv,
					Some(position),
					k,
					local_signer,
					CHECKPOINT_MAX_VERSIONS,
				)
			})
			.await
		{
			Ok(children) => children,
			Err(ReadRefusal::Budget) => return respond(ReadStatus::QUERY_BUDGET_EXCEEDED),
			Err(ReadRefusal::Final(why)) => return unsupported(&why),
			Err(_) => return respond(ReadStatus::BACKPRESSURED),
		};
		let Some(index) = self.slab.persistent_index() else {
			return unsupported("no persistent index");
		};
		// Exactly this key: a different key at the same position never answers.
		let mut winner: Option<Winner> = None;
		for (position_in, child) in children.iter().enumerate() {
			let manifest = match self
				.proved_manifest(&index, TableObjectId::DEFAULT_KV, tenant, 0, child)
				.await
			{
				Ok(manifest) => manifest,
				Err(ReadRefusal::Final(why)) => return unsupported(&why),
				Err(_) => return respond(ReadStatus::BACKPRESSURED),
			};
			for version in manifest
				.iter()
				.filter(|version| version.key_identity == *identity.as_bytes())
			{
				let candidate = Winner {
					hlc: version.hlc,
					row_digest: version.row_digest,
					child: Some(position_in),
					conflict: false,
				};
				winner = Some(match winner {
					Some(current) if current.hlc > candidate.hlc => current,
					Some(current) if current.hlc == candidate.hlc => Winner {
						conflict: true,
						..current
					},
					_ => candidate,
				});
			}
		}
		match index
			.base_entry(tenant, TableObjectId::DEFAULT_KV, *identity.as_bytes())
			.await
		{
			Ok(Some(entry)) => winner = Some(merge_winner(winner, base_winner(&entry))),
			Ok(None) => {}
			Err(_) => return respond(ReadStatus::BACKPRESSURED),
		}
		let Some(winner) = winner else {
			// Absence is an answer: it needs the same final authority.
			if authority.check().await.is_err() {
				return respond(ReadStatus::BACKPRESSURED);
			}
			return respond(ReadStatus::KEY_NOT_FOUND);
		};
		if winner.conflict {
			return unsupported("two parents hold the key at an equal full HLC");
		}
		let mut proved = match self
			.proved_location(tenant, &descriptor, identity, &winner)
			.await
		{
			Ok(proved) => proved,
			Err(ReadRefusal::Final(why)) => return unsupported(&why),
			Err(_) => return respond(ReadStatus::BACKPRESSURED),
		};
		if proved.is_none() {
			let Some(at) = winner.child else {
				return respond(ReadStatus::BACKPRESSURED);
			};
			let child = &children[at];
			if self
				.repair_child(child.rsv, child.ordinal, 0)
				.await
				.is_err()
			{
				return respond(ReadStatus::BACKPRESSURED);
			}
			proved = match self
				.proved_location(tenant, &descriptor, identity, &winner)
				.await
			{
				Ok(proved) => proved,
				Err(_) => return respond(ReadStatus::BACKPRESSURED),
			};
		}
		let Some((_, location)) = proved else {
			return respond(ReadStatus::BACKPRESSURED);
		};
		if location.is_tombstone {
			if authority.check().await.is_err() {
				return respond(ReadStatus::BACKPRESSURED);
			}
			return respond(ReadStatus::KEY_NOT_FOUND);
		}
		let stored = match self
			.slab
			.stored_rows_at_version(tenant, identity, winner.hlc)
			.await
		{
			Ok(stored) if !stored.is_empty() => stored,
			_ => return respond(ReadStatus::BACKPRESSURED),
		};
		let value_bytes = extract_first_binary(&stored[0], "value")?;
		let value_batch = build_value_batch(value_bytes)?;
		if authority.check().await.is_err() {
			return respond(ReadStatus::BACKPRESSURED);
		}
		Ok((
			ReadResponseMeta {
				status: ReadStatus::OK,
				_pad1: [0; 7],
				value_hlc: winner.hlc,
				read_at_index: k,
				epoch: meta.expected_epoch,
				leader_hint: local,
				value_len: value_bytes.len() as u32,
				_pad2: [0; 4],
			},
			Some(value_batch),
		))
	}

	pub(super) fn parent_decisions(&self) -> bool {
		self.decision_mode == DecisionMode::ParentDecisions
	}
}

/// Test-only: whether the table's inventory at the applied state fits within
/// `max_versions`.
#[cfg(test)]
pub(crate) fn inventory_within_for_test(
	state: &SmState,
	tenant: TenantId,
	table: TableObjectId,
	local: Signer,
	max_versions: u64,
) -> bool {
	let k = state.last_applied_log_id.map_or(0, |id| id.index);
	!matches!(
		accepted_children(state, tenant, table, k, local, max_versions),
		Err(ReadRefusal::Budget)
	)
}

/// Test-only: an in-memory stream for the data server's real stream dispatch.
#[cfg(test)]
struct DuplexTransport(tokio::io::DuplexStream);

#[cfg(test)]
impl tokio::io::AsyncRead for DuplexTransport {
	fn poll_read(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
	}
}

#[cfg(test)]
impl tokio::io::AsyncWrite for DuplexTransport {
	fn poll_write(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<std::io::Result<usize>> {
		std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
	}
	fn poll_flush(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::Pin::new(&mut self.0).poll_flush(cx)
	}
	fn poll_shutdown(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
	}
}

#[cfg(test)]
#[async_trait]
impl TransportStream for DuplexTransport {
	fn stream_id(&self) -> u64 {
		1
	}
	async fn finish(&mut self) -> std::io::Result<()> {
		use tokio::io::AsyncWriteExt;
		self.0.shutdown().await
	}
}

/// Test-only: what a streamed table query answered.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestQueryAnswer {
	/// The terminal frame's status, or `None` when no terminal arrived.
	pub(crate) status: Option<ReadStatus>,
	pub(crate) batches: Vec<RecordBatch>,
}

#[cfg(test)]
impl WendelonDataServer {
	/// A streamed table query through this server's real stream dispatch, as a
	/// node actor. A dispatch refusal is the `Err`.
	pub(crate) async fn table_query_for_test(
		&self,
		meta: TableQueryRequestMeta,
	) -> std::io::Result<TestQueryAnswer> {
		use tokio::io::AsyncWriteExt;
		let (mut client, server) = tokio::io::duplex(64 << 20);
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: std::mem::size_of::<TableQueryRequestMeta>() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		client.write_all(bytemuck::bytes_of(&meta)).await?;
		let context = RequestContext {
			remote: Some(self.local_node),
			actor: ActorIdentity::data_node(self.local_node),
		};
		let serving = self.handle_data_stream(
			context,
			header,
			Box::new(DuplexTransport(server)),
			self.pool.clone(),
		);
		let pool = self.pool.clone();
		let reading = async move {
			let mut answer = TestQueryAnswer {
				status: None,
				batches: Vec::new(),
			};
			while let Ok(frame) = read_frame(&mut client, &pool).await {
				if frame.header.flags & FRAME_FLAG_STREAM_END != 0 {
					let size = std::mem::size_of::<QueryResponseMeta>();
					if frame.meta.len() >= size {
						let response: QueryResponseMeta =
							*bytemuck::from_bytes(&frame.meta[..size]);
						answer.status = Some(response.status);
					}
					break;
				}
				if let Some(arrow) = frame.arrow_payload.as_ref() {
					answer
						.batches
						.push(crate::network::ipc::wire_decode_batch(&arrow[..])?);
				}
			}
			Ok::<_, std::io::Error>(answer)
		};
		let (served, answer) = tokio::join!(serving, reading);
		served?;
		answer
	}
}

#[cfg(test)]
impl WendelonDataServer {
	/// Test-only: a KV write through this node's write path.
	pub(crate) async fn kv_write_for_test(
		&self,
		meta: WriteRequestMeta,
		payload: RecordBatch,
	) -> WriteResponseMeta {
		self.write(meta, payload).await.unwrap().0
	}

	/// Test-only: a KV read through this node's read path.
	pub(crate) async fn kv_read_for_test(
		&self,
		meta: ReadRequestMeta,
		key: RecordBatch,
	) -> std::io::Result<(ReadResponseMeta, Option<RecordBatch>)> {
		self.read(meta, key).await
	}

	/// Test-only: a KV key's ring position and the holder of its lineage.
	pub(crate) async fn kv_primary_for_test(&self, tenant: TenantId, key: &[u8]) -> (u64, u32) {
		let descriptor = crate::schema::table::default_kv_table_descriptor_v1().unwrap();
		let (_, position) = kv_identity_and_position(tenant, &descriptor, key).unwrap();
		let holder = self
			.state_machine
			.read_state(move |state| {
				state
					.decisions
					.as_ref()?
					.lineages()
					.range(..=position)
					.next_back()
					.map(|(_, lineage)| lineage.holder.id)
			})
			.await
			.unwrap();
		(position, holder)
	}
}
