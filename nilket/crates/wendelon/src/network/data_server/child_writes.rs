//! ADR 0699 S3b: child writes under the ParentDecisions authority.
//!
//! A child write carries the parent's own user rows for one child. Every
//! receiver re-derives the row identities, ring positions and manifest from
//! those rows and the replicated claim, and builds its own storage frames:
//! nothing a sender computed is trusted. This module holds the parts that do
//! not touch the network: the manifest, ring positions and the parent locks.

// The frames and dispatch that call this pipeline arrive with the coordinator
// path (ADR 0699 S3c); until then only its controls reach it.
#![cfg_attr(not(test), allow(dead_code))]

use super::*;
use crate::schema::ClientWriteId;
use crate::storage::index::parent_intents::OwnedVersion;

/// Stripes of the per-parent lock. A fixed set: no entry is created or freed
/// per request, so nothing leaks and one `rsv` always maps to one mutex.
pub(super) const PARENT_LOCK_STRIPES: usize = 256;

/// The lock every step of one parent's child write and its refused cleanup
/// takes. Guards are owned, so work spawned under a guard keeps it until that
/// work finishes, even if the request that started it is cancelled.
pub(crate) struct ParentLocks(Vec<Arc<tokio::sync::Mutex<()>>>);

impl ParentLocks {
	pub(super) fn new() -> Self {
		Self(
			(0..PARENT_LOCK_STRIPES)
				.map(|_| Arc::new(tokio::sync::Mutex::new(())))
				.collect(),
		)
	}

	fn stripe(&self, rsv: u64) -> &Arc<tokio::sync::Mutex<()>> {
		&self.0[(rsv % PARENT_LOCK_STRIPES as u64) as usize]
	}

	pub(super) async fn lock(&self, rsv: u64) -> tokio::sync::OwnedMutexGuard<()> {
		self.stripe(rsv).clone().lock_owned().await
	}
}

/// Why a child's rows cannot form a manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ManifestRefusal {
	/// Only primary-key tables are admitted in this mode: append-only identities
	/// depend on the row's place in the original batch, and content-hash
	/// identities are shared across parents.
	UnsupportedTable,
	/// The rows fail today's admission for this table.
	Admission(String),
	/// A row's canonical bytes cannot be formed.
	Canonical(String),
	/// Two rows name the same version.
	DuplicateVersion,
}

/// A row's content commitment: its table, schema generation and canonical bytes.
pub(super) fn row_digest(
	table: TableObjectId,
	schema_generation: u64,
	canonical: &[u8],
) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new_derive_key("wendelon parent child row v1");
	hasher.update(table.as_bytes());
	hasher.update(&schema_generation.to_le_bytes());
	hasher.update(&(canonical.len() as u64).to_le_bytes());
	hasher.update(canonical);
	*hasher.finalize().as_bytes()
}

/// A child's content commitment: its manifest, as the claim binds it.
pub(super) fn manifest_content(manifest: &[OwnedVersion]) -> [u8; 32] {
	let bytes = postcard::to_stdvec(manifest).expect("a manifest encodes");
	let mut hasher = blake3::Hasher::new_derive_key("wendelon parent child manifest v1");
	hasher.update(&bytes);
	*hasher.finalize().as_bytes()
}

/// The request meta a receiver rebuilds from the claim, for today's admission.
pub(super) fn child_request_meta(
	key: &crate::consensus::decisions::ParentKey,
	table_object_id: TableObjectId,
	schema_generation: u64,
	rows: usize,
	commit_hlc: HybridTimestamp,
) -> std::io::Result<TableWriteRequestMeta> {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.parent_write_id = ClientWriteId {
		client_uuid: key.client_uuid,
		sequence_no: key.sequence_no,
		idempotency_ttl_ms: 0,
		_pad: 0,
	};
	meta.client_hlc = commit_hlc;
	meta.tenant_id = key.tenant;
	meta.table_object_id = table_object_id;
	meta.schema_generation = schema_generation;
	meta.row_count = crate::schema::table::encode_table_write_row_count(rows)?;
	Ok(meta)
}

/// The writer identity of the coordinator that stamped `commit_hlc`.
pub(super) fn writer_of(commit_hlc: HybridTimestamp) -> WriterNodeId {
	WriterNodeId::from_node_id(NodeId {
		id: commit_hlc.node_id,
		_reserved: 0,
	})
}

/// The manifest of a child's rows: one version per row, sorted, no repeats.
pub(super) fn child_manifest(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	commit_hlc: HybridTimestamp,
) -> Result<Vec<OwnedVersion>, ManifestRefusal> {
	if !matches!(
		descriptor.row_identity_policy,
		crate::schema::RowIdentityPolicy::PrimaryKey(_)
	) {
		return Err(ManifestRefusal::UnsupportedTable);
	}
	let admission = validate_table_write_request_batch(
		meta,
		batch,
		descriptor,
		writer_of(commit_hlc),
		commit_hlc,
	)
	.map_err(|error| ManifestRefusal::Admission(error.to_string()))?;
	let mut manifest = Vec::with_capacity(admission.key_identities.len());
	for (row, key_identity) in admission.key_identities.iter().enumerate() {
		let canonical = crate::storage::content_hash::encode_row_canonical(descriptor, batch, row)
			.map_err(|error| ManifestRefusal::Canonical(format!("{error:?}")))?;
		manifest.push(OwnedVersion {
			table: descriptor.table_object_id,
			key_identity: *key_identity.as_bytes(),
			hlc: commit_hlc,
			row_digest: row_digest(
				descriptor.table_object_id,
				descriptor.schema_generation,
				canonical.as_slice(),
			),
		});
	}
	manifest.sort();
	if manifest.windows(2).any(|pair| {
		(pair[0].table, pair[0].key_identity, pair[0].hlc)
			== (pair[1].table, pair[1].key_identity, pair[1].hlc)
	}) {
		return Err(ManifestRefusal::DuplicateVersion);
	}
	Ok(manifest)
}

/// The ring position a stored record is placed by: its partition for a
/// partitioned record, its raw locality otherwise (ADR 0090).
pub(super) fn record_position(child: &BifurcatedTableChildWrite) -> u64 {
	child
		.partition
		.map(|partition| partition.get())
		.unwrap_or_else(|| child.locality.get())
}

/// What a child write names: the reservation, the child and the descriptor
/// generation of its rows. Everything else comes from the replicated claim.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChildWriteMeta {
	pub rsv: u64,
	pub ordinal: u16,
	pub claim_digest: [u8; 32],
	pub schema_generation: u64,
}

/// Which hop a child write is: coordinator to the child's holder, or holder to
/// one of the child's plan members.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChildRole {
	Holder,
	Replica,
	/// ADR 0699 S4: a plan member's verified copy of an Accepted child, fetched
	/// from another plan member. Persists without an acknowledgement.
	Repair,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChildRefusal {
	ModeToday,
	NotCaughtUp,
	UnknownReservation,
	ClaimMismatch,
	/// The authenticated sender is not the role the claim records.
	WrongSender,
	/// This store is not in the child's plan at its current incarnation.
	NotInPlan,
	LineageClosed,
	/// No unexpired local lease covers the child range.
	LeaseMissing,
	SchemaUnknown,
	Rows(ManifestRefusal),
	/// A record's ring position is outside the child range.
	OutOfRange,
	/// The rows' manifest is not the one the claim commits to.
	ContentMismatch,
	ParentRefused,
	/// Accepted, and this store holds no complete intent to acknowledge.
	ParentAccepted,
	/// A repair of a parent not recorded Accepted.
	NotAccepted,
	Intent(crate::storage::index::parent_intents::IntentRefusal),
	/// The stored rows do not prove the manifest: a version absent, a copy with
	/// other content, or ownership inconsistent.
	Unverified(String),
	Storage(String),
}

/// A child write checked against replicated state, ready for the storage step.
#[derive(Clone, Debug)]
pub(crate) struct ChildPlan {
	pub(crate) intent: crate::storage::index::parent_intents::ParentIntent,
	pub(crate) descriptor: TableSchemaDescriptor,
	pub(crate) secondary_indexes: Vec<SecondaryIndexDescriptor>,
	pub(crate) batch: RecordBatch,
	/// Each row's version, in batch order.
	pub(crate) row_versions: Vec<OwnedVersion>,
	pub(crate) ack: crate::consensus::decisions::ChildAckBody,
	/// ADR 0699 S4b: the delete this child publishes a tombstone for.
	pub(crate) tombstone: Option<crate::consensus::decisions::DeleteRequest>,
}

/// The manifest of a child's rows: one version per row in batch order, and
/// the sorted manifest the claim commits to.
pub(super) fn child_versions(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptor,
	commit_hlc: HybridTimestamp,
) -> Result<(Vec<OwnedVersion>, Vec<OwnedVersion>), ManifestRefusal> {
	let manifest = child_manifest(meta, batch, descriptor, commit_hlc)?;
	let admission = validate_table_write_request_batch(
		meta,
		batch,
		descriptor,
		writer_of(commit_hlc),
		commit_hlc,
	)
	.map_err(|error| ManifestRefusal::Admission(error.to_string()))?;
	let in_order = admission
		.key_identities
		.iter()
		.map(|key| {
			*manifest
				.iter()
				.find(|version| version.key_identity == *key.as_bytes())
				.expect("every admitted row has a manifest entry")
		})
		.collect();
	Ok((in_order, manifest))
}

/// Checks a child write against the applied state, independently of whoever
/// sent it: the reservation and claim, the sender's recorded role, this store's
/// place in the plan, the holder's lineage and lease, and the rows themselves.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_child(
	state: &SmState,
	local: crate::consensus::decisions::Signer,
	sender: u32,
	role: ChildRole,
	meta: &ChildWriteMeta,
	batch: RecordBatch,
	lease_covers: impl Fn(u64, u64) -> bool,
) -> Result<ChildPlan, ChildRefusal> {
	use crate::consensus::decisions::{ChildAckBody, lineage_is_open};
	let decisions = state.decisions.as_ref().ok_or(ChildRefusal::ModeToday)?;
	let body = decisions
		.body(meta.rsv)
		.ok_or(ChildRefusal::UnknownReservation)?;
	if body.claim_digest != meta.claim_digest {
		return Err(ChildRefusal::ClaimMismatch);
	}
	let claim = &body.claim;
	let child = claim
		.children
		.get(usize::from(meta.ordinal))
		.ok_or(ChildRefusal::ClaimMismatch)?;
	let table = parent_table(claim.key.namespace);
	let expected_sender = match role {
		ChildRole::Holder => claim
			.children
			.get(usize::from(claim.coordinator))
			.map(|coordinator| coordinator.holder.node),
		ChildRole::Replica => Some(child.holder.node),
		ChildRole::Repair => child
			.plan
			.iter()
			.map(|member| member.node)
			.find(|member| *member == sender && *member != local.node),
	};
	if expected_sender != Some(sender) {
		return Err(ChildRefusal::WrongSender);
	}
	let current = state
		.incarnations
		.get(&local.node)
		.and_then(|registry| registry.current);
	if current != Some(local.incarnation) || !child.plan.contains(&local) {
		return Err(ChildRefusal::NotInPlan);
	}
	if role == ChildRole::Holder {
		if child.holder != local {
			return Err(ChildRefusal::WrongSender);
		}
		if !lineage_is_open(state, child.lineage, child.holder) {
			return Err(ChildRefusal::LineageClosed);
		}
		if !lease_covers(child.range_start, child.range_end) {
			return Err(ChildRefusal::LeaseMissing);
		}
	}
	let descriptor = table_descriptor(state, claim.key.tenant, table, meta.schema_generation)
		.ok_or(ChildRefusal::SchemaUnknown)?;
	// ADR 0699 S4b: a delete child's version is derived from the recorded claim;
	// its payload only names the row, which must be the claim's.
	let (row_versions, manifest, tombstone) =
		if let crate::consensus::decisions::Operation::Delete { request, .. } = &claim.operation {
			if super::deletes::row_id_of(&batch) != Some(request.row)
				|| request.schema_generation != meta.schema_generation
			{
				return Err(ChildRefusal::ContentMismatch);
			}
			let manifest = vec![super::deletes::tombstone_version(request, claim.commit_hlc)];
			(manifest.clone(), manifest, Some(*request))
		} else {
			let request = child_request_meta(
				&claim.key,
				table,
				meta.schema_generation,
				batch.num_rows(),
				claim.commit_hlc,
			)
			.map_err(|_| ChildRefusal::Rows(ManifestRefusal::Admission("row count".into())))?;
			let (row_versions, manifest) =
				child_versions(&request, &batch, &descriptor, claim.commit_hlc)
					.map_err(ChildRefusal::Rows)?;
			(row_versions, manifest, None)
		};
	if manifest.len() != child.rows as usize || manifest_content(&manifest) != child.content {
		return Err(ChildRefusal::ContentMismatch);
	}
	let secondary_indexes = WendelonDataServer::secondary_index_admission_descriptors(
		&state.secondary_indexes,
		claim.key.tenant,
		&descriptor,
	);
	Ok(ChildPlan {
		intent: crate::storage::index::parent_intents::ParentIntent {
			tenant: claim.key.tenant,
			rsv: meta.rsv,
			ordinal: meta.ordinal,
			claim_digest: meta.claim_digest,
			range_start: child.range_start,
			range_end: child.range_end,
			content: child.content,
			manifest,
			state: crate::storage::index::parent_intents::ParentIntentState::Intent,
		},
		descriptor,
		secondary_indexes,
		batch,
		row_versions,
		tombstone,
		ack: ChildAckBody {
			key: claim.key,
			reservation: meta.rsv,
			claim_digest: meta.claim_digest,
			ordinal: meta.ordinal,
			range_start: child.range_start,
			range_end: child.range_end,
			lineage: child.lineage,
			holder_incarnation: child.holder.incarnation,
			rows: child.rows,
			content: child.content,
			acker: local,
		},
	})
}

/// ADR 0699 S4b: the table a parent's rows belong to. A KV parent writes one
/// row of the default KV table.
pub(crate) fn parent_table(
	namespace: crate::consensus::decisions::ParentNamespace,
) -> TableObjectId {
	use crate::consensus::decisions::ParentNamespace;
	match namespace {
		ParentNamespace::Table(table) => table,
		ParentNamespace::Kv => TableObjectId::DEFAULT_KV,
	}
}

/// The descriptor of `table` at `generation` from applied state; the default KV
/// table's is implicit, at generation 0, and never registered.
pub(crate) fn table_descriptor(
	state: &SmState,
	tenant: TenantId,
	table: TableObjectId,
	generation: u64,
) -> Option<TableSchemaDescriptor> {
	if table == TableObjectId::DEFAULT_KV {
		return (generation == 0)
			.then(crate::schema::table::default_kv_table_descriptor_v1)
			.and_then(Result::ok);
	}
	state.table_schemas.get(tenant, table, generation).cloned()
}

/// The recorded status of reservation `rsv`, read from the applied state.
async fn parent_status(
	state_machine: &WendelonStateMachine,
	rsv: u64,
) -> Option<crate::consensus::decisions::ParentStatus> {
	state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.body(rsv))
				.map(|body| body.status)
		})
		.await
}

pub(super) fn replica_children(bifurcated: &BifurcatedTableWrite) -> Vec<TableReplicaChildPayload> {
	to_replica_children(bifurcated)
}

fn to_replica_children(bifurcated: &BifurcatedTableWrite) -> Vec<TableReplicaChildPayload> {
	bifurcated
		.children
		.iter()
		.map(|child| TableReplicaChildPayload {
			child_ordinal: child.child_ordinal,
			key_identity: child.key_identity,
			locality: child.locality,
			record_row_index: child.record_row_index,
			frame_bytes: child.frame_bytes.clone(),
			payload_hash: child.payload_hash,
			charged_bytes: child.charged_bytes,
		})
		.collect()
}

fn bifurcate_child(
	slab: &SlabManager,
	plan: &ChildPlan,
	batch: &RecordBatch,
) -> Result<BifurcatedTableWrite, ChildRefusal> {
	let hlc = plan.ack_hlc();
	let request = child_request_meta(
		&plan.ack.key,
		plan.descriptor.table_object_id,
		plan.descriptor.schema_generation,
		batch.num_rows(),
		hlc,
	)
	.map_err(|error| ChildRefusal::Storage(error.to_string()))?;
	let admission =
		validate_table_write_request_batch(&request, batch, &plan.descriptor, writer_of(hlc), hlc)
			.map_err(|error| ChildRefusal::Rows(ManifestRefusal::Admission(error.to_string())))?;
	bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
		&request,
		batch,
		&plan.descriptor,
		&admission,
		hlc,
		slab.row_group_writer_config(),
		slab.encoding_writer_config(),
		slab.l2_chunk_layout_config(),
	)
	.map_err(|error| ChildRefusal::Rows(ManifestRefusal::Admission(error.to_string())))
}

impl ChildPlan {
	fn ack_hlc(&self) -> HybridTimestamp {
		self.intent.manifest[0].hlc
	}
}

/// The live leases this node holds, as `(start, end, epoch)`, read when asked.
/// ADR 0700: the applied grants to `local` at the current epoch that have not
/// expired at `now_ms`, whatever leader issued them.
pub(crate) fn applied_leases(
	state: &SmState,
	local: u32,
	now_ms: u64,
) -> Vec<crate::schema::LeaseDescriptor> {
	let Some((_, config)) = state.epoch_history.last_key_value() else {
		return Vec::new();
	};
	let now = HybridTimestamp {
		physical_ms: now_ms,
		logical: 0,
		node_id: 0,
	};
	state
		.leases
		.values()
		.filter(|lease| {
			lease.holder.id == local && lease.epoch == config.epoch && now < lease.lease_expiry
		})
		.copied()
		.collect()
}

pub(crate) type LeaseSource = Arc<
	dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<(u64, u64, u64)>> + Send>>
		+ Send
		+ Sync,
>;

/// What a store needs to acknowledge a child: its signer, and for the holder
/// its live leases, re-read once the parent's lock is held.
#[derive(Clone)]
pub(crate) struct ChildAuthority {
	pub(crate) signer: Arc<DecisionSigner>,
	pub(crate) role: ChildRole,
	pub(crate) leases: Option<LeaseSource>,
	/// Test-only pause after persistence and before the final check and
	/// signature: `(reached, release)`.
	#[cfg(test)]
	pub(crate) before_sign: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

/// A stored row's content commitment, from its stored bytes: the row projected
/// to the descriptor's columns, encoded canonically.
pub(super) fn stored_row_digest(
	descriptor: &TableSchemaDescriptor,
	stored: &RecordBatch,
) -> Result<[u8; 32], String> {
	let row = stored_row_projected(descriptor, stored)?;
	let canonical = crate::storage::content_hash::encode_row_canonical(descriptor, &row, 0)
		.map_err(|error| format!("{error:?}"))?;
	Ok(row_digest(
		descriptor.table_object_id,
		descriptor.schema_generation,
		canonical.as_slice(),
	))
}

/// A stored row projected to the descriptor's columns: the user row it was
/// written as.
pub(super) fn stored_row_projected(
	descriptor: &TableSchemaDescriptor,
	stored: &RecordBatch,
) -> Result<RecordBatch, String> {
	let schema = Arc::new(
		descriptor
			.arrow_schema()
			.map_err(|error| error.to_string())?,
	);
	let columns = schema
		.fields()
		.iter()
		.map(|field| {
			stored
				.column_by_name(field.name())
				.cloned()
				.ok_or_else(|| format!("stored row lacks column {}", field.name()))
		})
		.collect::<Result<Vec<_>, _>>()?;
	RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}

/// The proof an acknowledgement attests: every manifest version is stored, every
/// stored copy of it carries the manifest's content, and every version is owned
/// by this `(rsv, ordinal)`.
pub(super) async fn verify_owned_rows(
	slab: &SlabManager,
	index: &crate::storage::index::PersistentLocalIndex,
	intent: &crate::storage::index::parent_intents::ParentIntent,
	descriptor: &TableSchemaDescriptor,
) -> Result<(), ChildRefusal> {
	let unverified = |detail: String| ChildRefusal::Unverified(detail);
	for version in &intent.manifest {
		// ADR 0699 S4b: a typed tombstone is proved by its stored tombstone
		// locations and owner, never by the absence of a row.
		if super::deletes::is_tombstone_version(descriptor, version) {
			let owner = index
				.version_owner(intent.tenant, *version)
				.await
				.map_err(|error| unverified(error.to_string()))?;
			let locations = slab
				.version_locations(
					intent.tenant,
					Identity::new(version.key_identity),
					version.hlc,
				)
				.map_err(|error| unverified(error.to_string()))?;
			if owner != Some((intent.rsv, intent.ordinal))
				|| locations.is_empty()
				|| !locations.iter().all(|location| location.is_tombstone)
			{
				return Err(unverified("a tombstone is not stored as owned".into()));
			}
			continue;
		}
		let owner = index
			.version_owner(intent.tenant, *version)
			.await
			.map_err(|error| unverified(error.to_string()))?;
		if owner != Some((intent.rsv, intent.ordinal)) {
			return Err(unverified(
				"a manifest version is not owned by this child".into(),
			));
		}
		let rows = slab
			.stored_rows_at_version(
				intent.tenant,
				Identity::new(version.key_identity),
				version.hlc,
			)
			.await
			.map_err(|error| unverified(error.to_string()))?;
		if rows.is_empty() {
			return Err(unverified("a manifest version is not stored".into()));
		}
		for row in &rows {
			if stored_row_digest(descriptor, row).map_err(unverified)? != version.row_digest {
				return Err(unverified("a stored copy differs from the manifest".into()));
			}
		}
	}
	Ok(())
}

/// The holder's authority, re-read under the lock: its lineage is open and a
/// live lease of the current epoch covers the child range.
async fn holder_authority(
	state_machine: &WendelonStateMachine,
	plan: &ChildPlan,
	authority: &ChildAuthority,
) -> Result<(), ChildRefusal> {
	if authority.role != ChildRole::Holder {
		return Ok(());
	}
	let holder = crate::consensus::decisions::Signer {
		node: authority.signer.signer.node,
		incarnation: plan.ack.holder_incarnation,
	};
	let lineage = plan.ack.lineage;
	let (open, epoch) = state_machine
		.read_state(move |state| {
			(
				crate::consensus::decisions::lineage_is_open(state, lineage, holder),
				state
					.epoch_history
					.last_key_value()
					.map(|(_, config)| config.epoch),
			)
		})
		.await;
	if !open {
		return Err(ChildRefusal::LineageClosed);
	}
	let leases = match &authority.leases {
		Some(source) => source().await,
		None => Vec::new(),
	};
	let (start, end) = (plan.intent.range_start, plan.intent.range_end);
	if !leases.iter().any(|(low, high, lease_epoch)| {
		Some(*lease_epoch) == epoch && *low <= start && end <= *high
	}) {
		return Err(ChildRefusal::LeaseMissing);
	}
	Ok(())
}

/// The recorded decision, re-read under the lock. Only a recorded Refused
/// refuses (and marks the intent); an absent decision refuses as unknown.
async fn decision_allows(
	state_machine: &WendelonStateMachine,
	index: &crate::storage::index::PersistentLocalIndex,
	rsv: u64,
	ordinal: u16,
) -> Result<crate::consensus::decisions::ParentStatus, ChildRefusal> {
	use crate::consensus::decisions::ParentStatus;
	use crate::storage::index::parent_intents::ParentIntentState::{Complete, Intent, Refused};
	let storage = |error: std::io::Error| ChildRefusal::Storage(error.to_string());
	match parent_status(state_machine, rsv).await {
		None => Err(ChildRefusal::UnknownReservation),
		Some(ParentStatus::Refused { .. }) => {
			if index
				.parent_intent(rsv, ordinal)
				.await
				.map_err(storage)?
				.is_some()
			{
				index
					.set_parent_intent_state(rsv, ordinal, &[Intent, Complete], Refused)
					.await
					.map_err(storage)?
					.map_err(ChildRefusal::Intent)?;
			}
			Err(ChildRefusal::ParentRefused)
		}
		Some(status) => Ok(status),
	}
}

/// The storage step and the acknowledgement, as one operation under the
/// parent's lock, inside a task that keeps the lock until it finishes, so a
/// cancelled request cannot release it early. Under the lock: the decision and
/// the holder's authority are re-read; the intent is recorded; only the missing
/// versions are appended; the stored rows are verified against the manifest and
/// owners before Complete and before any retry acknowledgement; the decision is
/// read once more; and only then is the acknowledgement signed.
pub(crate) async fn persist_child(
	slab: Arc<SlabManager>,
	state_machine: WendelonStateMachine,
	locks: Arc<ParentLocks>,
	plan: ChildPlan,
	authority: ChildAuthority,
) -> Result<
	crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>,
	ChildRefusal,
> {
	if authority.role == ChildRole::Repair {
		return Err(ChildRefusal::WrongSender);
	}
	persist_locked(slab, state_machine, locks, plan, authority)
		.await?
		.ok_or(ChildRefusal::WrongSender)
}

/// The persistence of [`persist_child`], shared with the typed repair, which
/// signs nothing.
pub(super) async fn persist_locked(
	slab: Arc<SlabManager>,
	state_machine: WendelonStateMachine,
	locks: Arc<ParentLocks>,
	plan: ChildPlan,
	authority: ChildAuthority,
) -> Result<
	Option<crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>>,
	ChildRefusal,
> {
	use crate::consensus::decisions::ParentStatus;
	use crate::storage::index::parent_intents::ParentIntentState::{Complete, Intent, Refused};
	let guard = locks.lock(plan.intent.rsv).await;
	let work = tokio::spawn(async move {
		let _guard = guard;
		let index = slab
			.persistent_index()
			.ok_or_else(|| ChildRefusal::Storage("no persistent index".into()))?;
		let (rsv, ordinal) = (plan.intent.rsv, plan.intent.ordinal);
		let storage = |error: std::io::Error| ChildRefusal::Storage(error.to_string());
		let status = decision_allows(&state_machine, &index, rsv, ordinal).await?;
		let existing = index.parent_intent(rsv, ordinal).await.map_err(storage)?;
		let recorded_complete = existing.as_ref().is_some_and(|existing| {
			existing.state == Complete && existing.manifest == plan.intent.manifest
		});
		// A repair re-checks a complete intent's stored versions and appends any
		// that are missing; every other role takes a complete intent as written.
		let complete = recorded_complete && authority.role != ChildRole::Repair;
		let accepted = matches!(status, ParentStatus::Accepted { .. });
		// A fresh write of an Accepted child is refused; only the typed repair
		// of an Accepted child persists one, and repair needs Accepted.
		if authority.role == ChildRole::Repair && !accepted {
			return Err(ChildRefusal::NotAccepted);
		}
		if accepted && !complete && authority.role != ChildRole::Repair {
			return Err(ChildRefusal::ParentAccepted);
		}
		if !complete {
			holder_authority(&state_machine, &plan, &authority).await?;
			// Every record lands inside the child range before anything is recorded.
			if let Some(request) = plan.tombstone {
				let position = crate::consensus::decisions::delete_row_position(
					plan.intent.tenant,
					&plan.descriptor,
					request.row,
				);
				if position != Some(plan.intent.range_start)
					|| plan.intent.range_start != plan.intent.range_end
				{
					return Err(ChildRefusal::OutOfRange);
				}
			} else {
				let full = bifurcate_child(&slab, &plan, &plan.batch)?;
				if full.children.iter().any(|record| {
					let position = record_position(record);
					position < plan.intent.range_start || position > plan.intent.range_end
				}) {
					return Err(ChildRefusal::OutOfRange);
				}
			}
			match index
				.begin_parent_intent(plan.intent.clone())
				.await
				.map_err(storage)?
				.map_err(ChildRefusal::Intent)?
			{
				Refused => return Err(ChildRefusal::ParentRefused),
				Intent | Complete => {}
			}
			// A tombstone is appended once: only when no copy of it is stored.
			if let Some(request) = plan.tombstone {
				let stored = slab
					.version_locations(plan.intent.tenant, request.row, plan.ack_hlc())
					.map_err(storage)?;
				if stored.is_empty() {
					super::deletes::append_tombstone(
						&slab,
						plan.intent.tenant,
						&plan.descriptor,
						&request,
						plan.ack_hlc(),
						&plan.secondary_indexes,
					)
					.await
					.map_err(storage)?;
				}
			}
			// Append only the rows whose version has no stored copy yet.
			let mut missing = Vec::new();
			for (row, version) in plan
				.row_versions
				.iter()
				.enumerate()
				.filter(|_| plan.tombstone.is_none())
			{
				let stored = slab
					.stored_rows_at_version(
						plan.intent.tenant,
						Identity::new(version.key_identity),
						version.hlc,
					)
					.await
					.map_err(storage)?;
				if stored.is_empty() {
					missing.push(row as u32);
				}
			}
			if !missing.is_empty() {
				let rows = arrow::compute::take_record_batch(
					&plan.batch,
					&arrow::array::UInt32Array::from(missing),
				)
				.map_err(|error| ChildRefusal::Storage(error.to_string()))?;
				let bifurcated = bifurcate_child(&slab, &plan, &rows)?;
				append_table_children_durable(
					&slab,
					plan.intent.tenant,
					plan.ack_hlc(),
					&to_replica_children(&bifurcated),
					&plan.secondary_indexes,
				)
				.await
				.map_err(storage)?;
			}
		}
		// The proof the acknowledgement attests, for a new and a retried Complete.
		verify_owned_rows(&slab, &index, &plan.intent, &plan.descriptor).await?;
		if !recorded_complete {
			index
				.set_parent_intent_state(rsv, ordinal, &[Intent], Complete)
				.await
				.map_err(storage)?
				.map_err(ChildRefusal::Intent)?;
		}
		#[cfg(test)]
		if let Some((reached, release)) = &authority.before_sign {
			reached.notify_one();
			release.notified().await;
		}
		// No acknowledgement after an observed refusal.
		decision_allows(&state_machine, &index, rsv, ordinal).await?;
		Ok((authority.role != ChildRole::Repair).then(|| {
			crate::consensus::decisions::evidence::sign(
				&authority.signer.key,
				plan.ack.clone(),
				authority.signer.signer,
			)
		}))
	});
	work.await
		.map_err(|error| ChildRefusal::Storage(error.to_string()))?
}

/// Marks every intent whose parent is recorded Refused, under each parent's
/// lock. An intent whose decision is absent or Reserved is left alone.
pub(crate) async fn refuse_recorded_intents(
	slab: &SlabManager,
	state_machine: &WendelonStateMachine,
	locks: &ParentLocks,
) -> std::io::Result<usize> {
	use crate::consensus::decisions::ParentStatus;
	use crate::storage::index::parent_intents::ParentIntentState::{Complete, Intent, Refused};
	let Some(index) = slab.persistent_index() else {
		return Ok(0);
	};
	let mut refused = 0;
	for (rsv, ordinal) in index.parent_intents_in(&[Intent, Complete]).await? {
		let _guard = locks.lock(rsv).await;
		if matches!(
			parent_status(state_machine, rsv).await,
			Some(ParentStatus::Refused { .. })
		) && index
			.set_parent_intent_state(rsv, ordinal, &[Intent, Complete], Refused)
			.await?
			.is_ok()
		{
			refused += 1;
		}
	}
	Ok(refused)
}

/// The data server's decision state: parent locks, and in this mode the
/// store's signer. One field in the bounded root.
#[derive(Clone)]
pub(crate) struct DecisionPeer {
	pub(crate) locks: Arc<ParentLocks>,
	pub(crate) signer: Option<Arc<DecisionSigner>>,
	/// ADR 0699 S3c: this node's path to the leader for decisions.
	pub(crate) proposals: Option<Arc<crate::network::decision_proposals::DecisionProposals>>,
	/// ADR 0699 S5a: set at close; the maintenance loop stops between steps,
	/// so no lifecycle permit is dropped mid-step by an orderly close.
	pub(crate) maintenance_stop: Arc<tokio::sync::watch::Sender<bool>>,
	/// ADR 0699 S5a: the generation whose staged candidate this run built, or
	/// rebuilt and matched, with its proofs (0: none). Activating any other
	/// candidate does not inherit this start's validation.
	pub(crate) built_gid: Arc<std::sync::atomic::AtomicU64>,
	/// ADR 0699 S5b: the last sealed slab a cleanup step visited, and the
	/// debt sweep's position.
	pub(crate) cleanup_cursor: Arc<std::sync::atomic::AtomicU64>,
	/// Sealed slabs found debt-free, with the active generation then: new
	/// debt appears only at an activation, so they are skipped until one.
	pub(crate) debt_free: Arc<std::sync::Mutex<std::collections::BTreeMap<u64, u64>>>,
	pub(crate) debt_cursor:
		Arc<std::sync::Mutex<Option<crate::storage::index::certified_base::VersionKey>>>,
	/// ADR 0699 S5a: a committed generation has no matching staged candidate
	/// here; the store refuses to serve until an operator decides (no reset).
	pub(crate) behind_boundary: Arc<std::sync::atomic::AtomicBool>,
	/// ADR 0699 S5a: the certified base validated since this start. Reads
	/// refuse until it is.
	pub(crate) base_validated: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only: how many more read-authority checks pass before every
	/// later one fails.
	#[cfg(test)]
	pub(crate) read_checks_left: Arc<std::sync::atomic::AtomicUsize>,
	/// Test-only faults: refuse child replicates, refuse fetches, or alter the
	/// first row of a served fetch.
	#[cfg(test)]
	pub(crate) refuse_replicas: Arc<std::sync::atomic::AtomicBool>,
	#[cfg(test)]
	pub(crate) refuse_fetches: Arc<std::sync::atomic::AtomicBool>,
	#[cfg(test)]
	pub(crate) tamper_fetches: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only: leave a committed generation staged, the crash row between
	/// Commit applied and local activation.
	#[cfg(test)]
	pub(crate) hold_activation: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only: base validation fails, as a physical mismatch would.
	#[cfg(test)]
	pub(crate) fail_base_validation: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only: the next cleanup copy read fails once, as a transient read
	/// error would.
	#[cfg(test)]
	pub(crate) fail_copy_read_once: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (ADR 0703): this store's local certified cleanup and its own
	/// LSM step are paused, so a test drives certified LSM itself.
	#[cfg(test)]
	pub(crate) pause_cleanup: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only (ADR 0706): only the local certified cleanup step is paused,
	/// so a held certified LSM job is the only work on its slabs.
	#[cfg(test)]
	pub(crate) pause_local_cleanup: Arc<std::sync::atomic::AtomicBool>,
	/// Test-only: a KV key read as if at another position, the seam for a
	/// different key at the same position.
	#[cfg(test)]
	pub(crate) kv_position_override: Arc<std::sync::Mutex<Option<(Vec<u8>, u64)>>>,
	/// Test-only pause points in the coordinator, taken once each, in order.
	#[cfg(test)]
	pub(crate) pauses: Arc<
		std::sync::Mutex<
			std::collections::VecDeque<(
				PauseStage,
				Arc<tokio::sync::Notify>,
				Arc<tokio::sync::Notify>,
			)>,
		>,
	>,
	/// Test-only (ADR 0706): what each held certified step returned.
	#[cfg(test)]
	pub(crate) held_outcomes: Arc<std::sync::Mutex<Vec<HeldStepOutcome>>>,
}

/// Where a test may pause a coordinated write.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PauseStage {
	/// After the lookup found the key absent and eligible.
	AfterAbsent,
	/// After a Reserve answer, before the record is read again.
	AfterReserve,
	/// After the children are written, before Finalize.
	BeforeFinalize,
	/// In a reader, after its cut is applied and before its inventory.
	ReaderAfterCut,
	/// In a reader, before it repairs a child it lacks.
	ReaderBeforeRepair,
	/// In a delete, after its cut and before its evaluation.
	DeleteEvaluation,
	/// In a certification step, holding its lifecycle permit.
	CertificationStep,
	/// ADR 0707: in a certification step, after its candidate is durably
	/// staged and its report signed, before the report is proposed.
	CertificationBeforeReport,
	/// In a cleanup step, after its plan reclaims something and before the
	/// rewrite.
	CleanupBeforeRewrite,
	/// ADR 0706: in the leader's certified tier step, holding its lifecycle
	/// permit, before its work.
	CertifiedTierStep,
	/// ADR 0706: in the leader's certified LSM step, holding its lifecycle
	/// permit, before its work.
	CertifiedLsmStep,
}

/// Test-only (ADR 0706): the terminal return of a certified step that a test
/// held, recorded after its permit completed.
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) enum HeldStepOutcome {
	Tier(Result<crate::storage::manager::certified_tier::CertifiedTierStep, String>),
	Lsm(Result<Option<crate::storage::CompactionOutcome>, String>),
}

#[cfg(test)]
impl DecisionPeer {
	/// Pauses here if the next queued pause is for this stage; whether it
	/// did.
	pub(crate) async fn pause_at(&self, stage: PauseStage) -> bool {
		let next = {
			let mut queue = self.pauses.lock().unwrap();
			match queue.front() {
				Some((at, _, _)) if *at == stage => queue.pop_front(),
				_ => None,
			}
		};
		match next {
			Some((_, reached, release)) => {
				reached.notify_one();
				release.notified().await;
				true
			}
			None => false,
		}
	}
}

impl Default for DecisionPeer {
	fn default() -> Self {
		Self {
			locks: Arc::new(ParentLocks::new()),
			signer: None,
			proposals: None,
			maintenance_stop: Arc::new(tokio::sync::watch::channel(false).0),
			built_gid: Default::default(),
			cleanup_cursor: Default::default(),
			debt_free: Default::default(),
			debt_cursor: Default::default(),
			#[cfg(test)]
			pauses: Default::default(),
			#[cfg(test)]
			held_outcomes: Default::default(),
			behind_boundary: Default::default(),
			base_validated: Default::default(),
			#[cfg(test)]
			read_checks_left: Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX)),
			#[cfg(test)]
			refuse_replicas: Default::default(),
			#[cfg(test)]
			refuse_fetches: Default::default(),
			#[cfg(test)]
			tamper_fetches: Default::default(),
			#[cfg(test)]
			hold_activation: Default::default(),
			#[cfg(test)]
			fail_base_validation: Default::default(),
			#[cfg(test)]
			fail_copy_read_once: Default::default(),
			#[cfg(test)]
			pause_cleanup: Default::default(),
			#[cfg(test)]
			pause_local_cleanup: Default::default(),
			#[cfg(test)]
			kv_position_override: Default::default(),
		}
	}
}

impl DecisionPeer {
	/// ADR 0700: whether `msg_type` passes this mode's service restriction.
	/// Exactly the checkpoint's surfaces are served: a table write and its
	/// child frames, a streamed table query, a single-key KV write and read, and
	/// a single-row delete. Each is then served only in its supported shape (the
	/// reader, the coordinator and the delete path refuse the rest). Every
	/// other data message (migration, bulk, counters, frames, other mutation
	/// paths) stays refused.
	pub(crate) fn opens(&self, msg_type: u8) -> bool {
		const SERVED: [MsgType; 8] = [
			MsgType::TABLE_WRITE_REQ,
			MsgType::TABLE_CHILD_WRITE_REQ,
			MsgType::TABLE_CHILD_REPLICATE_REQ,
			MsgType::TABLE_CHILD_FETCH_REQ,
			MsgType::TABLE_QUERY_REQ,
			MsgType::WRITE_REQ,
			MsgType::READ_REQ,
			MsgType::TABLE_DELETE_REQ,
		];
		SERVED.iter().any(|served| served.0 == msg_type)
	}
}

/// This store's decision signer: its key and the incarnation it signs as.
pub(crate) struct DecisionSigner {
	pub(crate) key: ed25519_dalek::SigningKey,
	pub(crate) signer: crate::consensus::decisions::Signer,
}

/// Bounds checked before any sender-sized allocation is decoded.
pub(crate) const MAX_CHILD_META_BYTES: usize = 4096;
pub(crate) const MAX_CHILD_ROWS_BYTES: usize = 64 << 20;
/// How long a receiver waits for its applied state to reach the reservation.
const CATCH_UP: std::time::Duration = std::time::Duration::from_secs(2);

/// What a child write answers: the signed acknowledgements gathered (the
/// holder's own first, then its plan members'), or why it was refused.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ChildWriteResponse {
	Acked(Vec<crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>>),
	Refused(String),
}

/// Decodes a child write frame, bounding both parts before decoding them.
pub(crate) fn decode_child_frame(
	meta: &[u8],
	rows: Option<&[u8]>,
) -> Result<(ChildWriteMeta, RecordBatch), String> {
	if meta.len() > MAX_CHILD_META_BYTES {
		return Err("child write meta exceeds its bound".into());
	}
	let rows = rows.ok_or("child write has no rows")?;
	if rows.len() > MAX_CHILD_ROWS_BYTES {
		return Err("child write rows exceed their bound".into());
	}
	let meta = postcard::from_bytes(meta).map_err(|error| error.to_string())?;
	let batch = crate::storage::multirow::decode_batch(rows).map_err(|error| error.to_string())?;
	if batch.num_rows() == 0
		|| batch.num_rows() > crate::consensus::decisions::MAX_ROWS_PER_CHILD as usize
	{
		return Err("child write row count out of bounds".into());
	}
	Ok((meta, batch))
}

impl WendelonDataServer {
	pub(crate) fn with_decision_mode(
		mut self,
		decision_mode: crate::consensus::decision_mode::DecisionMode,
	) -> Self {
		self.decision_mode = decision_mode;
		// ADR 0699 S4: no rewrite drops a stored version in this mode until S5.
		if decision_mode == crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			self.slab.contain_version_reclamation();
		}
		self
	}

	pub(crate) fn with_decision_signer(mut self, signer: Option<Arc<DecisionSigner>>) -> Self {
		self.decisions.signer = signer;
		self
	}

	/// Test-only: the node's decision proposals.
	#[cfg(test)]
	pub(crate) fn decision_proposals_for_test(
		&self,
	) -> Arc<crate::network::decision_proposals::DecisionProposals> {
		self.decisions
			.proposals
			.clone()
			.expect("a decision mode node")
	}

	/// Test-only: the store's decision signer identity.
	#[cfg(test)]
	pub(crate) fn decision_signer_for_test(&self) -> crate::consensus::decisions::Signer {
		self.decisions
			.signer
			.as_ref()
			.expect("a decision mode node")
			.signer
	}

	/// Test-only: the decision peer's fault switches and counters.
	#[cfg(test)]
	pub(crate) fn decision_peer_for_test(&self) -> &DecisionPeer {
		&self.decisions
	}

	/// Test-only: the lease ranges this node holds now, as a holder sees them
	/// (`(low, high, epoch)`).
	#[cfg(test)]
	pub(crate) async fn held_leases_for_test(&self) -> Vec<(u64, u64, u64)> {
		self.lease_source()().await
	}

	/// Test-only: queue a pause in this node's coordinator.
	#[cfg(test)]
	pub(crate) fn pause_coordinator_for_test(
		&self,
		stage: PauseStage,
	) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
		let (reached, release) = (
			Arc::new(tokio::sync::Notify::new()),
			Arc::new(tokio::sync::Notify::new()),
		);
		self.decisions
			.pauses
			.lock()
			.unwrap()
			.push_back((stage, reached.clone(), release.clone()));
		(reached, release)
	}

	pub(crate) fn with_decision_proposals(
		mut self,
		proposals: Option<Arc<crate::network::decision_proposals::DecisionProposals>>,
	) -> Self {
		self.decisions.proposals = proposals;
		self
	}

	/// ADR 0699 S3c: marks intents whose parent is recorded Refused.
	pub(crate) async fn sweep_refused_intents(&self) -> std::io::Result<usize> {
		refuse_recorded_intents(&self.slab, &self.state_machine, &self.decisions.locks).await
	}

	/// The holder's write of its child, then its plan's acknowledgements.
	pub(super) async fn holder_write(
		&self,
		sender: u32,
		meta: ChildWriteMeta,
		batch: RecordBatch,
		rows: Arc<Vec<u8>>,
	) -> Vec<crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>> {
		match self
			.child_write(sender, ChildRole::Holder, meta.clone(), batch)
			.await
		{
			Ok(own) => {
				let mut acks = vec![own];
				acks.extend(self.fan_out_child(&meta, rows).await);
				acks
			}
			Err(_refusal) => {
				#[cfg(test)]
				eprintln!("COORD holder write: {_refusal:?}");
				Vec::new()
			}
		}
	}

	/// ADR 0699 S5a: the decision maintenance loop's stop signal.
	pub(crate) fn decision_maintenance_stop(&self) -> tokio::sync::watch::Receiver<bool> {
		self.decisions.maintenance_stop.subscribe()
	}

	/// ADR 0699 S5a: asks the decision maintenance loop to stop after its
	/// current step.
	pub(crate) fn stop_decision_maintenance(&self) {
		self.decisions.maintenance_stop.send_replace(true);
	}

	/// ADR 0700: why this store cannot serve in ParentDecisions mode now, or
	/// `None`. A revocable observation, not a capability: admission and read
	/// authority are still checked at use time. It requires this store's
	/// decision signer bound at its current incarnation, and every range the
	/// current ring makes it primary for (derived from the ring, so an empty
	/// lineage set never passes) held as a lineage at that incarnation and
	/// epoch and covered by its own lease at the current epoch. It never gates
	/// the lease, recovery or control traffic that makes it true.
	pub(crate) async fn decision_readiness_reason(&self) -> Option<String> {
		if !self.decision_mode.refuses_service() {
			return None;
		}
		let Some(signer) = self.decisions.signer.as_ref().map(|signer| signer.signer) else {
			return Some("DecisionSignerMissing".into());
		};
		let leases = self.lease_source()().await;
		self.state_machine
			.read_state(move |state| {
				let Some(decisions) = state.decisions.as_ref() else {
					return Some("DecisionStateMissing".to_string());
				};
				let current = state
					.incarnations
					.get(&signer.node)
					.and_then(|registry| registry.current);
				if current != Some(signer.incarnation) {
					return Some("IncarnationNotCurrent".into());
				}
				if decisions.signer_key(signer).is_none() {
					return Some("DecisionSignerUnbound".into());
				}
				let Some((_, config)) = state.epoch_history.last_key_value() else {
					return Some("NoEpoch".into());
				};
				let ring = crate::network::ring::RingState::build(
					&crate::network::ring::membership_from_epoch_config(config),
					1,
				);
				let node = crate::schema::NodeId {
					id: signer.node,
					_reserved: 0,
				};
				for (start, end) in ring.primary_ranges_for(node) {
					let held = decisions.lineages().get(&start).is_some_and(|lineage| {
						lineage.range_end == end
							&& lineage.holder.id == signer.node
							&& lineage.incarnation == signer.incarnation
							&& lineage.epoch == config.epoch
					});
					let leased = leases.iter().any(|(low, high, epoch)| {
						*epoch == config.epoch && *low <= start && end <= *high
					});
					if !held || !leased {
						return Some("PrimaryRangeNotLeased".into());
					}
				}
				None
			})
			.await
	}

	pub(crate) fn decision_mode_refuses_service(&self) -> bool {
		self.decision_mode.refuses_service()
	}

	/// ADR 0699 S5a: a store under ParentDecisions is not Ready until this
	/// start has validated its certified base.
	pub(crate) fn certified_base_unvalidated(&self) -> bool {
		self.decisions.signer.is_some()
			&& !self
				.decisions
				.base_validated
				.load(std::sync::atomic::Ordering::Acquire)
	}

	/// A child frame from an authenticated node, by its kind.
	pub(super) async fn dispatch_child_frame(
		&self,
		context: &RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.header.msg_type == MsgType::TABLE_CHILD_FETCH_REQ.0 {
			let sender = context.require_authenticated_node("TABLE_CHILD_FETCH_REQ")?;
			return self.handle_child_fetch(sender.id, frame).await;
		}
		let (role, name) = if frame.header.msg_type == MsgType::TABLE_CHILD_WRITE_REQ.0 {
			(ChildRole::Holder, "TABLE_CHILD_WRITE_REQ")
		} else {
			(ChildRole::Replica, "TABLE_CHILD_REPLICATE_REQ")
		};
		let sender = context.require_authenticated_node(name)?;
		self.handle_child_frame(sender.id, role, frame).await
	}

	/// Waits, bounded, until the applied state includes reservation `rsv`.
	pub(super) async fn await_applied(&self, rsv: u64) -> Result<(), ChildRefusal> {
		let deadline = tokio::time::Instant::now() + CATCH_UP;
		loop {
			let applied = self
				.state_machine
				.read_state(|state| state.last_applied_log_id.map(|id| id.index))
				.await;
			if applied.is_some_and(|applied| applied >= rsv) {
				return Ok(());
			}
			if tokio::time::Instant::now() >= deadline {
				return Err(ChildRefusal::NotCaughtUp);
			}
			tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		}
	}

	/// ADR 0700: the ranges this store holds now, `(low, high, epoch)`. In this
	/// mode the committed lease record is the authority, not the current
	/// leader's signature on a cached grant: a lineage continues across a change
	/// of issuer (`admit_lease_grant`), so an applied, unexpired grant of the
	/// current epoch to this store still covers after a failover. It is read
	/// with the current epoch in one state read; no uncommitted, cached or
	/// prospective grant counts, and the issuer's signature must still verify.
	/// It grants nothing by itself: holder admission, repair and readiness keep
	/// their lineage (generation, holder incarnation), sender and plan checks.
	fn lease_source(&self) -> LeaseSource {
		let (state_machine, clock, authority, local) = (
			self.state_machine.clone(),
			self.clock.clone(),
			self.lease_authority.clone(),
			self.local_node.id,
		);
		Arc::new(move || {
			let (state_machine, clock, authority) =
				(state_machine.clone(), clock.clone(), authority.clone());
			Box::pin(async move {
				let now_ms = clock.now_ms();
				state_machine
					.read_state(move |state| applied_leases(state, local, now_ms))
					.await
					.into_iter()
					.filter(|lease| authority.verify_lease(lease).is_ok())
					.map(|lease| (lease.range_start, lease.range_end, lease.epoch))
					.collect()
			})
		})
	}

	/// One child write at this store: checked against applied state, persisted
	/// under the parent's lock, and acknowledged with this store's signature.
	pub(crate) async fn child_write(
		&self,
		sender: u32,
		role: ChildRole,
		meta: ChildWriteMeta,
		batch: RecordBatch,
	) -> Result<
		crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>,
		ChildRefusal,
	> {
		if self.decision_mode != crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			return Err(ChildRefusal::ModeToday);
		}
		#[cfg(test)]
		if role == ChildRole::Replica
			&& self
				.decisions
				.refuse_replicas
				.load(std::sync::atomic::Ordering::Acquire)
		{
			return Err(ChildRefusal::Storage("test: replicates refused".into()));
		}
		let signer = self
			.decisions
			.signer
			.clone()
			.ok_or(ChildRefusal::ModeToday)?;
		self.await_applied(meta.rsv).await?;
		let leases = (self.lease_source())().await;
		let local = signer.signer;
		let plan = self
			.state_machine
			.read_state(move |state| {
				let epoch = state
					.epoch_history
					.last_key_value()
					.map(|(_, config)| config.epoch);
				plan_child(state, local, sender, role, &meta, batch, |start, end| {
					leases.iter().any(|(low, high, lease_epoch)| {
						Some(*lease_epoch) == epoch && *low <= start && end <= *high
					})
				})
			})
			.await?;
		persist_child(
			self.slab.clone(),
			self.state_machine.clone(),
			self.decisions.locks.clone(),
			plan,
			ChildAuthority {
				signer,
				role,
				leases: (role == ChildRole::Holder).then(|| self.lease_source()),
				#[cfg(test)]
				before_sign: None,
			},
		)
		.await
	}

	/// The holder forwards the same rows to each other plan member and gathers
	/// their signed acknowledgements; failures are left for the coordinator to
	/// count, since quorum is the reducer's to judge.
	async fn fan_out_child(
		&self,
		meta: &ChildWriteMeta,
		rows: Arc<Vec<u8>>,
	) -> Vec<crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>> {
		let local = self.local_node.id;
		let rsv = meta.rsv;
		let ordinal = usize::from(meta.ordinal);
		let members: Vec<u32> = self
			.state_machine
			.read_state(move |state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.body(rsv))
					.and_then(|body| body.claim.children.get(ordinal))
					.map(|child| child.plan.iter().map(|member| member.node).collect())
					.unwrap_or_default()
			})
			.await;
		let Ok(meta_bytes) = postcard::to_stdvec(meta) else {
			return Vec::new();
		};
		let mut sends = tokio::task::JoinSet::new();
		for member in members.into_iter().filter(|member| *member != local) {
			sends.spawn(send_child_frame(
				self.factory.clone(),
				self.pool.clone(),
				NodeId {
					id: member,
					_reserved: 0,
				},
				MsgType::TABLE_CHILD_REPLICATE_REQ,
				meta_bytes.clone(),
				rows.clone(),
			));
		}
		let mut acks = Vec::new();
		while let Some(response) = sends.join_next().await {
			if let Ok(Ok(ChildWriteResponse::Acked(member_acks))) = response {
				acks.extend(member_acks);
			}
		}
		acks
	}

	/// A child write or child replicate frame from an authenticated sender.
	pub(super) async fn handle_child_frame(
		&self,
		sender: u32,
		role: ChildRole,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let rows: Option<&[u8]> = frame.arrow_payload.as_ref().map(|bytes| bytes.as_ref());
		let response = match decode_child_frame(frame.meta.as_ref(), rows) {
			Err(reason) => ChildWriteResponse::Refused(reason),
			Ok((meta, batch)) if role == ChildRole::Holder => {
				let rows = Arc::new(rows.unwrap_or_default().to_vec());
				let acks = self.holder_write(sender, meta, batch, rows).await;
				if acks.is_empty() {
					ChildWriteResponse::Refused("the holder could not write its child".into())
				} else {
					ChildWriteResponse::Acked(acks)
				}
			}
			Ok((meta, batch)) => match self.child_write(sender, role, meta, batch).await {
				Err(refusal) => ChildWriteResponse::Refused(format!("{refusal:?}")),
				Ok(own) => ChildWriteResponse::Acked(vec![own]),
			},
		};
		let msg_type = match role {
			ChildRole::Holder => MsgType::TABLE_CHILD_WRITE_RESP,
			ChildRole::Replica => MsgType::TABLE_CHILD_REPLICATE_RESP,
			ChildRole::Repair => MsgType::TABLE_CHILD_FETCH_RESP,
		};
		let bytes = postcard::to_stdvec(&response).map_err(Error::other)?;
		let mut meta = self.pool.acquire();
		meta.as_mut_vec().extend_from_slice(&bytes);
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		})
	}
}

/// Sends one child frame and decodes its bounded response.
pub(crate) async fn send_child_frame(
	factory: Arc<TransportProviderFactory>,
	pool: Arc<BufPool>,
	target: NodeId,
	msg_type: MsgType,
	meta: Vec<u8>,
	rows: Arc<Vec<u8>>,
) -> std::io::Result<ChildWriteResponse> {
	let conn = factory.get_connection(target).await?;
	let mut stream = conn.open_bi().await?;
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: msg_type.0,
		flags: 0,
		stream_id: 0,
		meta_len: meta.len() as u32,
		arrow_len: rows.len() as u32,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &header, &meta, Some(rows.as_slice()), None).await?;
	stream
		.finish()
		.await
		.map_err(|error| Error::new(ErrorKind::BrokenPipe, error))?;
	let response = read_frame(&mut *stream, &pool).await?;
	let expected = if msg_type == MsgType::TABLE_CHILD_WRITE_REQ {
		MsgType::TABLE_CHILD_WRITE_RESP
	} else {
		MsgType::TABLE_CHILD_REPLICATE_RESP
	};
	if response.header.msg_type != expected.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"unexpected child write response 0x{:02x}",
				response.header.msg_type
			),
		));
	}
	if response.meta.len() > crate::network::decision_proposals::MAX_DECISION_FRAME_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"child write response exceeds its bound",
		));
	}
	postcard::from_bytes(response.meta.as_ref())
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

#[cfg(test)]
#[path = "child_writes/tests.rs"]
mod tests;
