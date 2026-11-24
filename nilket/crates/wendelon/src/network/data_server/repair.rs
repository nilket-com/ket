//! ADR 0699 S4: the verified transfer of an Accepted child between members of
//! its recorded plan.
//!
//! A store in a child's plan that lacks the child's complete intent fetches the
//! rows from another plan member. The source serves only a recorded Accepted
//! parent, under the parent's lock, from rows it verifies against its own
//! complete intent. The requester trusts nothing it receives: the rows go
//! through the replica plan checks against the recorded claim and the shared
//! persistence under its own parent lock, typed as a repair. Nothing is
//! acknowledged, and an ordinary child write of an Accepted parent stays
//! refused.

use super::child_writes::{
	ChildAuthority, ChildRefusal, ChildRole, ChildWriteMeta, MAX_CHILD_META_BYTES,
	MAX_CHILD_ROWS_BYTES, persist_locked, plan_child_keyed, stored_row_projected,
	verify_owned_rows,
};
use super::*;
use crate::consensus::decisions::{ParentStatus, Signer};
use crate::storage::index::parent_intents::ParentIntentState;

/// How long one source may take to answer one fetch.
const SOURCE_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

/// What a fetch names: the child, by its reservation, ordinal and claim, and the
/// descriptor generation its rows are read in.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChildFetchMeta {
	pub rsv: u64,
	pub ordinal: u16,
	pub claim_digest: [u8; 32],
	pub schema_generation: u64,
}

/// A fetch's answer. The rows travel as the frame's Arrow payload.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ChildFetchResponse {
	Rows,
	Refused(String),
}

/// The source's checks against its applied state: the parent is recorded
/// Accepted with this claim, and both the requester and this store are in the
/// child's recorded plan at their current registered incarnations.
fn fetch_source_check(
	state: &SmState,
	local: Signer,
	requester: u32,
	meta: &ChildFetchMeta,
) -> Result<
	(
		crate::consensus::decisions::ChildClaim,
		TableSchemaDescriptor,
		TenantId,
	),
	ChildRefusal,
> {
	let decisions = state.decisions.as_ref().ok_or(ChildRefusal::ModeToday)?;
	let body = decisions
		.body(meta.rsv)
		.ok_or(ChildRefusal::UnknownReservation)?;
	if body.claim_digest != meta.claim_digest {
		return Err(ChildRefusal::ClaimMismatch);
	}
	if !matches!(body.status, ParentStatus::Accepted { .. }) {
		return Err(ChildRefusal::NotAccepted);
	}
	let table = super::child_writes::parent_table(body.claim.key.namespace);
	let child = body
		.claim
		.children
		.get(usize::from(meta.ordinal))
		.ok_or(ChildRefusal::ClaimMismatch)?;
	let current = |node: u32| {
		state
			.incarnations
			.get(&node)
			.and_then(|registry| registry.current)
	};
	// ADR 0106 (wendelon): a requester is eligible as the plan member it is, or
	// as the current successor of one (its recorded ancestry). Wendelon ADR 0108:
	// the source itself must be its slot's current store, named by the plan at an
	// incarnation in its recorded ancestry (below).
	let eligible = |node: u32| {
		child.plan.iter().any(|member| {
			member.node == node
				&& current(node).is_some_and(|current| {
					current == member.incarnation
						|| crate::consensus::decisions::in_recorded_ancestry(
							state,
							node,
							current,
							member.incarnation,
						)
				})
		})
	};
	if requester == local.node || !eligible(requester) {
		return Err(ChildRefusal::WrongSender);
	}
	// Wendelon ADR 0108: this store serves a child whose plan names the slot at
	// an incarnation in its recorded ancestry (ADR 0107 C2), so an activated
	// replacement is a source for the children it recovered.
	let named = child.plan.iter().any(|member| {
		member.node == local.node
			&& crate::consensus::decisions::in_recorded_ancestry(
				state,
				local.node,
				local.incarnation,
				member.incarnation,
			)
	});
	if current(local.node) != Some(local.incarnation) || !named {
		return Err(ChildRefusal::NotInPlan);
	}
	let descriptor = super::child_writes::recorded_table_descriptor(
		state,
		body.claim.key.tenant,
		table,
		meta.schema_generation,
	)
	.ok_or(ChildRefusal::SchemaUnknown)?;
	Ok((child.clone(), descriptor, body.claim.key.tenant))
}

impl WendelonDataServer {
	/// Source side of a fetch, from an authenticated node.
	pub(super) async fn handle_child_fetch(
		&self,
		sender: u32,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let (response, rows) = if frame.meta.len() > MAX_CHILD_META_BYTES {
			(
				ChildFetchResponse::Refused("fetch meta exceeds its bound".into()),
				None,
			)
		} else {
			match postcard::from_bytes::<ChildFetchMeta>(frame.meta.as_ref()) {
				Err(error) => (ChildFetchResponse::Refused(error.to_string()), None),
				Ok(meta) => match self.serve_child_fetch(sender, meta).await {
					Ok(served) => (ChildFetchResponse::Rows, Some(served)),
					Err(refusal) => (ChildFetchResponse::Refused(format!("{refusal:?}")), None),
				},
			}
		};
		let bytes = postcard::to_stdvec(&response).map_err(Error::other)?;
		let mut meta = self.pool.acquire();
		meta.as_mut_vec().extend_from_slice(&bytes);
		// Wendelon ADR 0119 D5: an append-only child's rows travel with their
		// identities in the trailing section.
		let (rows, trailing) = match rows {
			Some((rows, trailing)) => (Some(rows), (!trailing.is_empty()).then_some(trailing)),
			None => (None, None),
		};
		let arrow_len = rows.as_ref().map_or(0, Vec::len) as u32;
		let trailing_len = trailing.as_ref().map_or(0, Vec::len) as u32;
		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: crate::consensus::decision_mode::RAFT_FRAME_VERSION_PARENT_DECISIONS,
				msg_type: MsgType::TABLE_CHILD_FETCH_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta.len() as u32,
				arrow_len,
				trailing_len,
			},
			meta,
			arrow_payload: rows.map(|rows| {
				let mut payload = self.pool.acquire();
				payload.as_mut_vec().extend_from_slice(&rows);
				payload
			}),
			trailing: trailing.map(|trailing| {
				let mut payload = self.pool.acquire();
				payload.as_mut_vec().extend_from_slice(&trailing);
				payload
			}),
		})
	}

	/// The child's rows, rebuilt only from stored copies verified against this
	/// store's own complete intent, under the parent's lock.
	async fn serve_child_fetch(
		&self,
		requester: u32,
		meta: ChildFetchMeta,
	) -> Result<(Vec<u8>, Vec<u8>), ChildRefusal> {
		if self.decision_mode != crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			return Err(ChildRefusal::ModeToday);
		}
		#[cfg(test)]
		if self
			.decisions
			.refuse_fetches
			.load(std::sync::atomic::Ordering::Acquire)
		{
			return Err(ChildRefusal::Storage("test: fetches refused".into()));
		}
		#[cfg(test)]
		let tamper = self
			.decisions
			.tamper_fetches
			.load(std::sync::atomic::Ordering::Acquire);
		let local = self
			.decisions
			.signer
			.as_ref()
			.ok_or(ChildRefusal::ModeToday)?
			.signer;
		self.await_applied(meta.rsv).await?;
		let checked_meta = meta.clone();
		let (child, descriptor, tenant) = self
			.state_machine
			.read_state(move |state| fetch_source_check(state, local, requester, &checked_meta))
			.await?;
		let slab = self.slab.clone();
		let guard = self.decisions.locks.lock(meta.rsv).await;
		let work = tokio::spawn(async move {
			let _guard = guard;
			let storage = |error: std::io::Error| ChildRefusal::Storage(error.to_string());
			let index = slab
				.persistent_index()
				.ok_or_else(|| ChildRefusal::Storage("no persistent index".into()))?;
			let intent = index
				.parent_intent(meta.rsv, meta.ordinal)
				.await
				.map_err(storage)?
				.filter(|intent| {
					intent.state == ParentIntentState::Complete
						&& intent.tenant == tenant
						&& intent.claim_digest == meta.claim_digest
						&& intent.content == child.content
						&& intent.manifest.len() == child.rows as usize
				})
				.ok_or_else(|| ChildRefusal::Unverified("no complete intent to serve".into()))?;
			verify_owned_rows(&slab, &index, &intent, &descriptor).await?;
			// ADR 0699 S4b, wendelon ADR 0112: a delete child's payload is its row ids,
			// in manifest (identity) order; its typed tombstones were just verified.
			if !intent.manifest.is_empty()
				&& intent
					.manifest
					.iter()
					.all(|version| super::deletes::is_tombstone_version(&descriptor, version))
			{
				let rows: Vec<Identity> = intent
					.manifest
					.iter()
					.map(|version| Identity::new(version.key_identity))
					.collect();
				let batch = super::deletes::row_id_batch(&rows).map_err(storage)?;
				return Ok((
					crate::storage::multirow::encode_batch(&batch).map_err(storage)?,
					Vec::new(),
				));
			}
			let mut rows = Vec::with_capacity(intent.manifest.len());
			for version in &intent.manifest {
				let stored = slab
					.stored_rows_at_version(
						tenant,
						Identity::new(version.key_identity),
						version.hlc,
						None,
					)
					.await
					.map_err(storage)?;
				let first = stored
					.first()
					.ok_or_else(|| ChildRefusal::Unverified("a version vanished".into()))?;
				rows.push(
					stored_row_projected(&descriptor, first).map_err(ChildRefusal::Unverified)?,
				);
			}
			let schema = Arc::new(
				descriptor
					.arrow_schema()
					.map_err(|error| ChildRefusal::Storage(error.to_string()))?,
			);
			let batch = arrow::compute::concat_batches(&schema, &rows)
				.map_err(|error| ChildRefusal::Storage(error.to_string()))?;
			#[cfg(test)]
			let batch = if tamper { tampered(batch) } else { batch };
			let bytes = crate::storage::multirow::encode_batch(&batch).map_err(storage)?;
			#[cfg(test)]
			if let Some(served) = SERVED_FETCH_ROW_BYTES
				.lock()
				.unwrap()
				.get_mut(&(meta.claim_digest, meta.ordinal))
			{
				served.push(bytes.len());
			}
			if bytes.len() > MAX_CHILD_ROWS_BYTES {
				return Err(ChildRefusal::Storage(
					"child rows exceed their bound".into(),
				));
			}
			// Wendelon ADR 0119 D5: an append-only child's served rows carry their
			// identities, in row order (the manifest's), for the requester to check
			// against the signed content.
			let trailing = if matches!(
				descriptor.row_identity_policy,
				crate::schema::RowIdentityPolicy::AppendOnly
			) {
				let identities: Vec<Identity> = intent
					.manifest
					.iter()
					.map(|version| Identity::new(version.key_identity))
					.collect();
				super::child_writes::encode_identities(&identities)
			} else {
				Vec::new()
			};
			Ok((bytes, trailing))
		});
		work.await
			.map_err(|error| ChildRefusal::Storage(error.to_string()))?
	}

	/// Repairs this store's copy of an Accepted child from the other members of
	/// its recorded plan, holder first, one bounded attempt per source. The rows
	/// are checked against the recorded claim and persisted as a repair.
	pub(super) async fn repair_child(
		&self,
		rsv: u64,
		ordinal: u16,
		schema_generation: u64,
	) -> Result<(), ChildRefusal> {
		let context = self.repair_context().ok_or(ChildRefusal::ModeToday)?;
		repair_child_in(&context, rsv, ordinal, schema_generation).await
	}

	/// Test-only: the production repair of one child on this store, as a
	/// reader or certification repair invokes it.
	#[cfg(test)]
	pub(crate) async fn repair_child_for_test(
		&self,
		rsv: u64,
		ordinal: u16,
		schema_generation: u64,
	) -> Result<(), ChildRefusal> {
		self.repair_child(rsv, ordinal, schema_generation).await
	}

	/// What this server's repairs use: its signer, store, locks and its
	/// factory's connections. `None` without a signer (Today mode).
	pub(super) fn repair_context(&self) -> Option<RepairContext> {
		let signer = self.decisions.signer.clone()?;
		let factory = self.factory.clone();
		Some(RepairContext {
			local: signer.signer,
			signer,
			state_machine: self.state_machine.clone(),
			slab: self.slab.clone(),
			locks: self.decisions.locks.clone(),
			pool: self.pool.clone(),
			index_buffers: true,
			connect: Arc::new(move |node| {
				let factory = factory.clone();
				Box::pin(async move {
					factory
						.get_connection(NodeId {
							id: node,
							_reserved: 0,
						})
						.await
				})
			}),
		})
	}
}

/// ADR 0106 (wendelon): how a repair reaches a node by its slot.
pub(crate) type Connect = Arc<
	dyn Fn(
			u32,
		) -> std::pin::Pin<
			Box<
				dyn std::future::Future<
						Output = std::io::Result<
							Arc<dyn crate::network::transport::TransportConnection>,
						>,
					> + Send,
			>,
		> + Send
		+ Sync,
>;

/// ADR 0106 (wendelon): what a repair needs, so a recovering control-only
/// replacement can repair without a data server.
pub(crate) struct RepairContext {
	pub(crate) local: crate::consensus::decisions::Signer,
	pub(crate) signer: Arc<super::child_writes::DecisionSigner>,
	pub(crate) state_machine: WendelonStateMachine,
	pub(crate) slab: Arc<SlabManager>,
	pub(crate) locks: Arc<super::child_writes::ParentLocks>,
	pub(crate) pool: Arc<BufPool>,
	pub(crate) connect: Connect,
	/// Wendelon ADR 0108 D5: whether repaired rows enter this store's index
	/// buffers. A ControlOnly replacement runs no index lifecycle, so no cut
	/// proves its buffers: it writes none, and its Full start rebuilds them.
	pub(crate) index_buffers: bool,
}

/// Repairs `local`'s copy of an Accepted child from the other members of its
/// recorded plan, holder first, one bounded attempt per source. The rows are
/// checked against the recorded claim and persisted as a repair: no ack.
pub(crate) async fn repair_child_in(
	ctx: &RepairContext,
	rsv: u64,
	ordinal: u16,
	schema_generation: u64,
) -> Result<(), ChildRefusal> {
	let signer = ctx.signer.clone();
	let local = ctx.local;
	let found = ctx
		.state_machine
		.read_state(move |state| {
			let body = state.decisions.as_ref()?.body(rsv)?;
			if !matches!(body.status, ParentStatus::Accepted { .. }) {
				return None;
			}
			let child = body.claim.children.get(usize::from(ordinal))?;
			let mut sources = vec![child.holder.node];
			sources.extend(child.plan.iter().map(|member| member.node));
			sources.retain(|node| *node != local.node);
			let mut seen = std::collections::BTreeSet::new();
			sources.retain(|node| seen.insert(*node));
			Some((body.claim_digest, sources))
		})
		.await;
	let Some((claim_digest, sources)) = found else {
		return Err(ChildRefusal::NotAccepted);
	};
	let meta = ChildFetchMeta {
		rsv,
		ordinal,
		claim_digest,
		schema_generation,
	};
	let mut last = ChildRefusal::Unverified("no plan member to fetch from".into());
	for source in sources {
		let fetched = tokio::time::timeout(
			SOURCE_BOUND,
			fetch_child(ctx.connect.clone(), ctx.pool.clone(), source, &meta),
		)
		.await;
		let (batch, given) = match fetched {
			Ok(Ok(fetched)) => fetched,
			Ok(Err(refusal)) => {
				last = refusal;
				continue;
			}
			Err(_) => {
				last = ChildRefusal::Storage(format!("node {source} did not answer in time"));
				continue;
			}
		};
		let write = ChildWriteMeta {
			rsv,
			ordinal,
			claim_digest,
			schema_generation,
		};
		// Wendelon ADR 0108 D5: the index admission guard is taken after the
		// fetch and before the plan reads its descriptors; a failed plan drops
		// it before the next source is asked.
		let plan =
			super::child_writes::AdmittedPlan::admit(&ctx.slab, &ctx.state_machine, move |state| {
				plan_child_keyed(
					state,
					local,
					source,
					ChildRole::Repair,
					&write,
					batch,
					given,
					|_, _| false,
				)
			})
			.await;
		let plan = match plan {
			Ok(plan) if ctx.index_buffers => plan,
			Ok(plan) => plan.without_index_buffers(),
			Err(refusal) => {
				last = refusal;
				continue;
			}
		};
		match persist_locked(
			ctx.slab.clone(),
			ctx.state_machine.clone(),
			ctx.locks.clone(),
			plan,
			ChildAuthority {
				signer: signer.clone(),
				role: ChildRole::Repair,
				leases: None,
				lease_evidence: None,
				#[cfg(test)]
				before_sign: None,
			},
			crate::storage::manager::tier_authority::RetirementWait::NONE,
		)
		.await
		{
			Ok(_) => return Ok(()),
			Err(refusal) => last = refusal,
		}
	}
	Err(last)
}

/// Sends one fetch and decodes its bounded answer.
async fn fetch_child(
	connect: Connect,
	pool: Arc<BufPool>,
	source: u32,
	meta: &ChildFetchMeta,
) -> Result<(RecordBatch, super::child_writes::GivenKeys), ChildRefusal> {
	let io = |error: std::io::Error| ChildRefusal::Storage(error.to_string());
	let bytes =
		postcard::to_stdvec(meta).map_err(|error| ChildRefusal::Storage(error.to_string()))?;
	let conn = connect(source).await.map_err(io)?;
	let mut stream = conn.open_bi().await.map_err(io)?;
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: crate::consensus::decision_mode::RAFT_FRAME_VERSION_PARENT_DECISIONS,
		msg_type: MsgType::TABLE_CHILD_FETCH_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: bytes.len() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &header, &bytes, None, None)
		.await
		.map_err(io)?;
	stream.finish().await.map_err(io)?;
	let response = read_frame(&mut *stream, &pool).await.map_err(io)?;
	crate::consensus::decision_mode::check_peer_exchange_version(response.header.version)
		.map_err(io)?;
	if response.header.msg_type != MsgType::TABLE_CHILD_FETCH_RESP.0
		|| response.meta.len() > MAX_CHILD_META_BYTES
	{
		return Err(ChildRefusal::Storage("unexpected fetch response".into()));
	}
	match postcard::from_bytes::<ChildFetchResponse>(response.meta.as_ref())
		.map_err(|error| ChildRefusal::Storage(error.to_string()))?
	{
		ChildFetchResponse::Refused(reason) => Err(ChildRefusal::Unverified(reason)),
		ChildFetchResponse::Rows => {
			let rows = response
				.arrow_payload
				.as_ref()
				.map(|bytes| bytes.as_ref())
				.ok_or_else(|| ChildRefusal::Storage("a fetch answered no rows".into()))?;
			if rows.len() > MAX_CHILD_ROWS_BYTES {
				return Err(ChildRefusal::Storage(
					"fetched rows exceed their bound".into(),
				));
			}
			let batch = crate::storage::multirow::decode_batch(rows)
				.map_err(|error| ChildRefusal::Storage(error.to_string()))?;
			if batch.num_rows() == 0
				|| batch.num_rows() > crate::consensus::decisions::MAX_ROWS_PER_CHILD as usize
			{
				return Err(ChildRefusal::Storage(
					"fetched row count out of bounds".into(),
				));
			}
			// Wendelon ADR 0119 D5: identities, when the source sent them, stay
			// undecoded until the plan knows the table's policy and the child.
			let given = super::child_writes::GivenKeys::of_section(&Arc::new(
				response
					.trailing
					.as_ref()
					.map(|bytes| bytes.as_ref().to_vec())
					.unwrap_or_default(),
			));
			Ok((batch, given))
		}
	}
}

/// Test-only: the batch with its last column's first value replaced, as a
/// source serving other bytes than it stored.
#[cfg(test)]
fn tampered(batch: RecordBatch) -> RecordBatch {
	let mut columns = batch.columns().to_vec();
	let last = columns.len() - 1;
	let values: Vec<Option<String>> = (0..batch.num_rows())
		.map(|row| {
			Some(if row == 0 {
				"tampered".to_owned()
			} else {
				format!("row {row}")
			})
		})
		.collect();
	let replacement = arrow::compute::cast(
		&(Arc::new(arrow::array::StringArray::from(values)) as arrow::array::ArrayRef),
		columns[last].data_type(),
	)
	.expect("a string column");
	columns[last] = replacement;
	RecordBatch::try_new(batch.schema(), columns).expect("the same schema")
}

/// Test-only: the encoded row bytes of each fetch answer served for an armed
/// child, by its claim digest and ordinal (wendelon ADR 0119 D5's near-limit
/// repair control). Nothing is recorded for a child no test armed; a test arms
/// its own child and takes its record back, so no other test can satisfy or
/// perturb it, and nothing outlives the test (Codex 01a1f9fb).
#[cfg(test)]
static SERVED_FETCH_ROW_BYTES: std::sync::Mutex<
	std::collections::BTreeMap<([u8; 32], u16), Vec<usize>>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// Test-only: records the fetch answers served for this child from now on.
#[cfg(test)]
pub(crate) fn arm_served_fetch(claim_digest: [u8; 32], ordinal: u16) {
	SERVED_FETCH_ROW_BYTES
		.lock()
		.unwrap()
		.insert((claim_digest, ordinal), Vec::new());
}

/// Test-only: this child's recorded answers, and its arming removed.
#[cfg(test)]
pub(crate) fn take_served_fetch(claim_digest: [u8; 32], ordinal: u16) -> Vec<usize> {
	SERVED_FETCH_ROW_BYTES
		.lock()
		.unwrap()
		.remove(&(claim_digest, ordinal))
		.unwrap_or_default()
}
