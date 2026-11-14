//! ADR 0699 S4b: a single-row delete under ParentDecisions.
//!
//! A delete names exactly one row id of a table whose locality is a function
//! of the id alone. The holder of that id's position coordinates it. After the
//! lookup, it evaluates the id at a cut K with the S4 reader: proved winner,
//! proved tombstone or proved absence; an unproved copy is repaired or the
//! answer is indeterminate. The selection is frozen in the record:
//! - a live winner publishes one tombstone for the key at a new HLC strictly
//!   above the winner's, as a one-child parent (select at K, publish at dec);
//! - otherwise the coordinator signs a RecordNoop, which linearizes at K.
//!
//! The tombstone suppresses every lower-HLC version of the key, including a
//! concurrent write ordered below it; the counts describe the frozen selection.

use super::*;
use crate::consensus::decisions::{
	ChildClaim, Claim, DeleteOutcome, DeleteRequest, DeleteSelection, LineageRef, Lookup, NoopBody,
	Operation, ParentKey, ParentNamespace, ParentStatus, Signer, delete_request_digest,
	delete_row_locality, delete_row_position,
};
use crate::network::decision_proposals::{
	CutResponse, ForwardedDecision, LookupRequest, LookupResponse, ProposalOutcome,
	ProposalResponse,
};
use crate::storage::index::parent_intents::OwnedVersion;

/// The column a delete child's payload carries: the row id.
const ROW_ID: &str = "row_id";

/// A tombstone's content commitment: the key's deletion in this table and
/// generation, under its own derivation domain.
pub(crate) fn tombstone_digest(table: TableObjectId, generation: u64, row: Identity) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new_derive_key("wendelon tombstone v1");
	hasher.update(table.as_bytes());
	hasher.update(&generation.to_le_bytes());
	hasher.update(row.as_bytes());
	*hasher.finalize().as_bytes()
}

/// The one version a matched delete publishes.
pub(crate) fn tombstone_version(request: &DeleteRequest, hlc: HybridTimestamp) -> OwnedVersion {
	OwnedVersion {
		table: request.table,
		key_identity: *request.row.as_bytes(),
		hlc,
		row_digest: tombstone_digest(request.table, request.schema_generation, request.row),
	}
}

/// Whether a manifest version is a typed tombstone of this descriptor.
pub(crate) fn is_tombstone_version(
	descriptor: &TableSchemaDescriptor,
	version: &OwnedVersion,
) -> bool {
	version.row_digest
		== tombstone_digest(
			descriptor.table_object_id,
			descriptor.schema_generation,
			Identity::new(version.key_identity),
		)
}

/// A delete child's payload: one row, the id.
pub(crate) fn row_id_batch(row: Identity) -> std::io::Result<RecordBatch> {
	let schema = Arc::new(arrow::datatypes::Schema::new(vec![
		arrow::datatypes::Field::new(ROW_ID, arrow::datatypes::DataType::Binary, false),
	]));
	RecordBatch::try_new(
		schema,
		vec![Arc::new(arrow::array::BinaryArray::from(vec![
			&row.as_bytes()[..],
		]))],
	)
	.map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

/// The id a delete child's payload names.
pub(crate) fn row_id_of(batch: &RecordBatch) -> Option<Identity> {
	let column = batch
		.column_by_name(ROW_ID)?
		.as_any()
		.downcast_ref::<arrow::array::BinaryArray>()?;
	if batch.num_rows() != 1 || !column.is_valid(0) {
		return None;
	}
	let bytes: [u8; 32] = column.value(0).try_into().ok()?;
	Some(Identity::new(bytes))
}

/// Appends the tombstone of `request.row` at `hlc` through today's tombstone
/// packing and the child append path.
pub(crate) async fn append_tombstone(
	slab: &SlabManager,
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	request: &DeleteRequest,
	hlc: HybridTimestamp,
	secondary_indexes: &[SecondaryIndexDescriptor],
) -> std::io::Result<()> {
	let invalid = |why: &str| Error::new(ErrorKind::InvalidData, why.to_owned());
	let routing_locality = delete_row_locality(tenant, descriptor, request.row)
		.ok_or_else(|| invalid("an unsupported delete shape"))?;
	let partition =
		crate::storage::multirow::write_partition_for_row(descriptor, routing_locality)?;
	let rows = [crate::storage::multirow::MultiRowSourceRow {
		source_row_index: 0,
		key_identity: request.row,
		routing_locality,
		row_hlc: hlc,
		is_tombstone: true,
		partition,
		table_object_id: Some(descriptor.table_object_id),
	}];
	let layout = tombstone_marker_layout(descriptor).map_err(|_| invalid("tombstone layout"))?;
	let packed = crate::storage::multirow::pack_tombstone_multirow_source_rows_with_policy(
		layout,
		&rows,
		descriptor.record_body_compression_policy,
	)?;
	let mut meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = 1;
	let bifurcated = bifurcate_table_delete(&meta, descriptor, packed)?;
	append_table_children_durable(
		slab,
		tenant,
		hlc,
		&super::child_writes::replica_children(&bifurcated),
		secondary_indexes,
	)
	.await
}

/// How a delete ended, with its frozen counts.
enum DeleteSettled {
	Committed {
		dec: u64,
		commit_hlc: HybridTimestamp,
		outcome: DeleteOutcome,
	},
	KnownNotCommitted,
	Redirect(NodeId),
	Indeterminate(WriteStatus),
	/// The caller's deadline passed: indeterminate, never known-not-committed.
	Deadline,
	Refused(WriteStatus, PbcRejectCode),
}

impl WendelonDataServer {
	/// A single-row delete of a ParentDecisions node.
	pub(super) async fn coordinate_table_delete(
		&self,
		meta: TableDeleteRowsRequestMeta,
		row_key_ids: Vec<Identity>,
	) -> TableDeleteRowsResponseMeta {
		let settled = self.coordinate_delete(&meta, &row_key_ids).await;
		let stop = if matches!(settled, DeleteSettled::Deadline) {
			MutationStopReason::DEADLINE
		} else {
			MutationStopReason::NONE
		};
		let (status, knowledge, dec, commit_hlc, hint, counts, reject) = match settled {
			DeleteSettled::Committed {
				dec,
				commit_hlc,
				outcome,
			} => {
				let counts = match outcome {
					DeleteOutcome::Deleted { .. } => [1, 0, 0],
					DeleteOutcome::AlreadyDeleted { .. } => [0, 1, 0],
					DeleteOutcome::NotFound => [0, 0, 1],
				};
				(
					WriteStatus::OK,
					MutationCommitKnowledge::COMMITTED,
					dec,
					commit_hlc,
					self.local_node,
					counts,
					PbcRejectCode::NONE,
				)
			}
			DeleteSettled::KnownNotCommitted => (
				WriteStatus::WRITE_REFUSED,
				MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
				0,
				meta.client_hlc,
				self.local_node,
				[0, 0, 0],
				PbcRejectCode::NONE,
			),
			DeleteSettled::Redirect(holder) => (
				WriteStatus::NOT_LEADER,
				MutationCommitKnowledge::INDETERMINATE,
				0,
				meta.client_hlc,
				holder,
				[0, 0, 0],
				PbcRejectCode::NONE,
			),
			DeleteSettled::Deadline => (
				WriteStatus::WRITE_DEADLINE_EXCEEDED,
				MutationCommitKnowledge::INDETERMINATE,
				0,
				meta.client_hlc,
				self.local_node,
				[0, 0, 0],
				PbcRejectCode::NONE,
			),
			DeleteSettled::Indeterminate(status) => (
				status,
				MutationCommitKnowledge::INDETERMINATE,
				0,
				meta.client_hlc,
				self.local_node,
				[0, 0, 0],
				PbcRejectCode::NONE,
			),
			DeleteSettled::Refused(status, reject) => (
				status,
				MutationCommitKnowledge::INDETERMINATE,
				0,
				meta.client_hlc,
				self.local_node,
				[0, 0, 0],
				reject,
			),
		};
		let write = WriteResponseMeta {
			status,
			_pad1: [0; 7],
			log_index: dec,
			term: 0,
			epoch: meta.expected_epoch,
			commit_hlc,
			leader_hint: hint,
			echoed_write_id: meta.parent_write_id,
			has_finalization: 0,
			_pad2: [0; 7],
		}
		.with_mutation_outcome(knowledge, stop);
		table_delete_rows_response(write, counts[0], counts[1], counts[2], reject)
	}

	async fn coordinate_delete(
		&self,
		meta: &TableDeleteRowsRequestMeta,
		row_key_ids: &[Identity],
	) -> DeleteSettled {
		let stalled = DeleteSettled::Indeterminate(WriteStatus::WRITE_STALLED);
		let unsupported = |reject| DeleteSettled::Refused(WriteStatus::INTERNAL_ERROR, reject);
		// The caller's deadline, derived once. A recorded outcome is answered
		// whatever it is; a fresh evaluation, and everything after a proposal,
		// spends it.
		let deadline = caller_deadline_from_remaining_millis(meta.deadline_remaining_millis);
		let expired = || deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline);
		// Exactly one id in this checkpoint.
		let [row] = row_key_ids else {
			return unsupported(PbcRejectCode::PBC_REJECT_ROW_COUNT_MISMATCH);
		};
		if meta.row_count != 1 {
			return unsupported(PbcRejectCode::PBC_REJECT_ROW_COUNT_MISMATCH);
		}
		let (Some(proposals), Some(signer)) = (
			self.decisions.proposals.clone(),
			self.decisions.signer.clone(),
		) else {
			return unsupported(PbcRejectCode::NONE);
		};
		let (tenant, table, generation) =
			(meta.tenant_id, meta.table_object_id, meta.schema_generation);
		let request = DeleteRequest {
			table,
			schema_generation: generation,
			row: *row,
		};
		let Some(descriptor) = self
			.state_machine
			.read_state(move |state| state.table_schemas.get(tenant, table, generation).cloned())
			.await
		else {
			return unsupported(PbcRejectCode::NONE);
		};
		let Some(position) = delete_row_position(tenant, &descriptor, *row) else {
			return unsupported(PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED);
		};
		let key = ParentKey {
			tenant,
			namespace: ParentNamespace::Table(table),
			client_uuid: meta.parent_write_id.client_uuid,
			sequence_no: meta.parent_write_id.sequence_no,
		};
		let digest = delete_request_digest(tenant, &request);
		let mut proposed = false;
		let (claim, rsv) = loop {
			let lookup = proposals
				.lookup(LookupRequest {
					key,
					request_digest: digest,
					sent_ms: proposals.now_ms(),
				})
				.await;
			match lookup {
				LookupResponse::Found(Lookup::Noop { record }) => {
					return DeleteSettled::Committed {
						dec: record.dec,
						commit_hlc: meta.client_hlc,
						outcome: record.body.selection.outcome,
					};
				}
				LookupResponse::Found(Lookup::Recorded { record }) => {
					let Operation::Delete { selection, .. } = record.claim.operation else {
						return DeleteSettled::Refused(
							WriteStatus::WRITE_KEY_CONFLICT,
							PbcRejectCode::NONE,
						);
					};
					match record.status {
						ParentStatus::Accepted { dec } => {
							return DeleteSettled::Committed {
								dec,
								commit_hlc: record.claim.commit_hlc,
								outcome: selection.outcome,
							};
						}
						ParentStatus::Refused { .. } => return DeleteSettled::KnownNotCommitted,
						ParentStatus::Noop { .. } => return stalled,
						ParentStatus::Reserved => {
							// Only the recorded coordinator drives it (S3c rule).
							let only = &record.claim.children[0];
							if only.holder.node != signer.signer.node {
								// ADR 0729: only while its recorded lineage is open
								// and leased; otherwise a stall.
								return match self
									.route_for_recorded_coordinator(only.lineage, only.holder)
									.await
								{
									Some(route) => DeleteSettled::Redirect(route),
									None => stalled,
								};
							}
							let (lineage, holder) = (only.lineage, only.holder);
							let open = self
								.state_machine
								.read_state(move |state| {
									crate::consensus::decisions::lineage_is_open(
										state, lineage, holder,
									)
								})
								.await;
							if holder != signer.signer || !open {
								return stalled;
							}
							break (record.claim, record.rsv);
						}
					}
				}
				LookupResponse::Found(Lookup::Absent { eligible: true }) if !proposed => {
					// Only the holder of the row's position coordinates.
					let holder = self
						.state_machine
						.read_state(move |state| {
							state
								.decisions
								.as_ref()?
								.lineages()
								.range(..=position)
								.next_back()
								.filter(|(_, lineage)| position <= lineage.range_end)
								.map(|(start, lineage)| (*start, *lineage))
						})
						.await;
					let Some((lineage_start, lineage)) = holder else {
						return stalled;
					};
					if lineage.holder.id != signer.signer.node
						|| lineage.incarnation != signer.signer.incarnation
					{
						// ADR 0729: the holder of this row's range, only while that
						// range is leased; re-read, since the lineage may have moved.
						return match self.route_for_position(position).await {
							Some(route) if route.id != signer.signer.node => {
								DeleteSettled::Redirect(route)
							}
							_ => stalled,
						};
					}
					// The one evaluation both a tombstone and a noop come from.
					let selection = match bounded(
						deadline,
						self.evaluate_delete(tenant, &descriptor, *row, position),
					)
					.await
					{
						Some(Ok(selection)) => selection,
						Some(Err(())) => return stalled,
						None => return DeleteSettled::Deadline,
					};
					// No fresh proposal from an evaluation that outlived its deadline.
					if expired() {
						return DeleteSettled::Deadline;
					}
					let lineage_ref = LineageRef {
						range_start: lineage_start,
						generation: lineage.generation,
					};
					let proposal = match selection.outcome {
						DeleteOutcome::Deleted { winner_hlc, .. } => {
							// ADR 0729: and above the lineage's floor.
							let Some(commit_hlc) = tombstone_hlc(
								&self.hlc,
								winner_hlc,
								super::coordinator::above_floor(meta.client_hlc, lineage.floor_ms),
							) else {
								return stalled;
							};
							let plan = self
								.state_machine
								.read_state(move |state| point_plan(state, tenant, position))
								.await;
							let Some(plan) = plan else {
								return stalled;
							};
							let version = tombstone_version(&request, commit_hlc);
							let claim = Claim {
								key,
								request_digest: digest,
								commit_hlc,
								coordinator: 0,
								first_row_position: position,
								children: vec![ChildClaim {
									range_start: position,
									range_end: position,
									lineage: lineage_ref,
									holder: signer.signer,
									plan,
									rows: 1,
									content: super::child_writes::manifest_content(&[version]),
								}],
								operation: Operation::Delete { request, selection },
							};
							ForwardedDecision::Reserve {
								claim: crate::consensus::decisions::evidence::sign(
									&signer.key,
									claim,
									signer.signer,
								),
							}
						}
						DeleteOutcome::AlreadyDeleted { .. } | DeleteOutcome::NotFound => {
							ForwardedDecision::RecordNoop {
								noop: crate::consensus::decisions::evidence::sign(
									&signer.key,
									NoopBody {
										key,
										request_digest: digest,
										request,
										selection,
										position,
										coordinator: signer.signer,
										lineage: lineage_ref,
									},
									signer.signer,
								),
							}
						}
					};
					match bounded(deadline, proposals.propose(proposal)).await {
						// Whatever it answers, the record decides.
						Some(ProposalResponse::Applied { .. }) => proposed = true,
						Some(_) => return stalled,
						None => return DeleteSettled::Deadline,
					}
				}
				LookupResponse::Found(Lookup::Absent { eligible: false }) => {
					return DeleteSettled::Indeterminate(WriteStatus::WRITE_OPERATION_EXPIRED);
				}
				LookupResponse::Found(Lookup::Conflict) => {
					return DeleteSettled::Refused(
						WriteStatus::WRITE_KEY_CONFLICT,
						PbcRejectCode::NONE,
					);
				}
				LookupResponse::Found(Lookup::InvalidIdentity) => {
					return DeleteSettled::Refused(
						WriteStatus::WRITE_IDENTITY_INVALID,
						PbcRejectCode::NONE,
					);
				}
				_ => return stalled,
			}
		};
		// Publish the tombstone child, then Finalize.
		let Ok(batch) = row_id_batch(*row) else {
			return stalled;
		};
		// Locked persistence runs in its own task and finishes if this ends.
		let Some(acks) = bounded(
			deadline,
			self.write_children(&claim, rsv, &descriptor, &batch, &[vec![0]]),
		)
		.await
		else {
			return DeleteSettled::Deadline;
		};
		#[cfg(test)]
		self.decisions
			.pause_at(super::child_writes::PauseStage::BeforeFinalize)
			.await;
		let Some(finalized) = bounded(
			deadline,
			proposals.propose(ForwardedDecision::Finalize {
				key,
				request_digest: digest,
				acks,
			}),
		)
		.await
		else {
			return DeleteSettled::Deadline;
		};
		match finalized {
			ProposalResponse::Applied {
				outcome:
					ProposalOutcome::Decision(crate::consensus::decisions::DecisionResult::Terminal {
						status: ParentStatus::Accepted { dec },
						..
					}),
				..
			} => {
				let Operation::Delete { selection, .. } = claim.operation else {
					return stalled;
				};
				DeleteSettled::Committed {
					dec,
					commit_hlc: claim.commit_hlc,
					outcome: selection.outcome,
				}
			}
			ProposalResponse::Applied {
				outcome:
					ProposalOutcome::Decision(crate::consensus::decisions::DecisionResult::Terminal {
						status: ParentStatus::Refused { .. },
						..
					}),
				..
			} => DeleteSettled::KnownNotCommitted,
			_ => stalled,
		}
	}

	/// The delete's evaluation at a fresh cut: the S4 reader's proved winner of
	/// the row (repaired if this store lacks it), a proved tombstone, or proved
	/// absence over the complete inventory. Anything unproved is an error,
	/// never NotFound.
	async fn evaluate_delete(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		row: Identity,
		position: u64,
	) -> Result<DeleteSelection, ()> {
		let proposals = self.decisions.proposals.clone().ok_or(())?;
		let CutResponse::Cut(k) = proposals.cut().await else {
			return Err(());
		};
		proposals
			.wait_applied(k, std::time::Duration::from_secs(5))
			.await
			.map_err(|_| ())?;
		#[cfg(test)]
		self.decisions
			.pause_at(super::child_writes::PauseStage::DeleteEvaluation)
			.await;
		let outcome = self
			.proved_point(tenant, descriptor, row, position, k)
			.await
			.map_err(|_| ())?;
		Ok(DeleteSelection { cut: k, outcome })
	}
}

/// `work`, unless the caller's deadline passes first (`None`).
async fn bounded<T>(
	deadline: Option<std::time::Instant>,
	work: impl std::future::Future<Output = T>,
) -> Option<T> {
	match deadline {
		// An expired deadline never polls the work: a first poll could start it.
		Some(deadline) if std::time::Instant::now() >= deadline => None,
		Some(deadline) => tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
			.await
			.ok(),
		None => Some(work.await),
	}
}

/// A tombstone's commit HLC: strictly above the selected winner on any clock.
/// The winner is merged first, so a coordinator whose clock is behind it still
/// mints above it; `None` only if the result is somehow not above.
fn tombstone_hlc(
	hlc: &crate::consensus::hlc::HybridLogicalClockDriver,
	winner_hlc: HybridTimestamp,
	client_hlc: HybridTimestamp,
) -> Option<HybridTimestamp> {
	hlc.merge(winner_hlc);
	let commit_hlc = hlc.merge(client_hlc);
	(commit_hlc > winner_hlc).then_some(commit_hlc)
}

#[cfg(test)]
mod tests {
	use super::bounded;
	use std::sync::atomic::{AtomicUsize, Ordering};

	/// ADR 0700 (replaces the +600 s premise of the S4b control): a coordinator
	/// whose clock is behind the selected winner still mints a tombstone whose
	/// full HLC is strictly above it. Omitting the winner merge does not.
	#[test]
	fn a_tombstone_is_minted_strictly_above_a_winner_ahead_of_the_clock() {
		use crate::consensus::hlc::HybridLogicalClockDriver;
		use crate::consensus::monitor::FakeClock;
		use crate::schema::{HybridTimestamp, NodeId};
		let local = NodeId {
			id: 2,
			_reserved: 0,
		};
		let winner = HybridTimestamp {
			physical_ms: 5_000,
			logical: 7,
			node_id: 9,
		};
		let client = HybridTimestamp {
			physical_ms: 1,
			logical: 0,
			node_id: 0,
		};
		let behind =
			|| HybridLogicalClockDriver::new(local, std::sync::Arc::new(FakeClock::new(1_000)));
		let minted = super::tombstone_hlc(&behind(), winner, client).unwrap();
		assert!(minted > winner, "{minted:?} not above {winner:?}");
		// Same physical millisecond as the winner: ordered by the full HLC.
		assert_eq!(minted.physical_ms, winner.physical_ms);
		// Mutation control: without the winner merge the clock mints below it.
		let unmerged = behind().merge(client);
		assert!(unmerged <= winner, "{unmerged:?}");
	}

	#[tokio::test]
	async fn an_expired_deadline_never_polls_the_work() {
		let polled = AtomicUsize::new(0);
		let expired = std::time::Instant::now() - std::time::Duration::from_millis(1);
		let answer = bounded(Some(expired), async {
			polled.fetch_add(1, Ordering::SeqCst);
			7
		})
		.await;
		assert_eq!(answer, None);
		assert_eq!(polled.load(Ordering::SeqCst), 0);
		let open = std::time::Instant::now() + std::time::Duration::from_secs(5);
		assert_eq!(bounded(Some(open), async { 7 }).await, Some(7));
		assert_eq!(bounded(None, async { 7 }).await, Some(7));
	}
}

/// The replica plan of the segment holding `position`, holder first, each at
/// its current incarnation.
fn point_plan(state: &SmState, tenant: TenantId, position: u64) -> Option<Vec<Signer>> {
	let (_, config) = state.epoch_history.last_key_value()?;
	let record = state.tenants.get(&tenant)?;
	let ring = crate::network::ring::RingState::build(
		&crate::network::ring::membership_from_epoch_config(config),
		record.replication_factor as usize,
	);
	let segment = ring
		.replica_segments()
		.into_iter()
		.find(|segment| segment.start <= position && position <= segment.end)?;
	segment
		.replicas
		.iter()
		.map(|node| {
			state
				.incarnations
				.get(&node.id)
				.and_then(|registry| registry.current)
				.map(|incarnation| Signer {
					node: node.id,
					incarnation,
				})
		})
		.collect()
}

#[cfg(test)]
impl WendelonDataServer {
	/// Test-only: a table delete through this node's delete path.
	pub(crate) async fn table_delete_for_test(
		&self,
		meta: TableDeleteRowsRequestMeta,
		row_key_ids: Vec<Identity>,
	) -> TableDeleteRowsResponseMeta {
		WendelonDataServer::table_delete(self, meta, row_key_ids)
			.await
			.unwrap()
			.0
	}

	/// Test-only: the last-attempt rule applied to a delete answer this node built for
	/// `status`, as its handler applies it before the answer goes out.
	pub(crate) async fn finalized_delete_answer_for_test(
		&self,
		meta: TableDeleteRowsRequestMeta,
		status: WriteStatus,
		knowledge: MutationCommitKnowledge,
	) -> (TableDeleteRowsResponseMeta, TableDeleteRowsResponseMeta) {
		let mut built = self.table_delete_error_response(&meta, status, PbcRejectCode::NONE);
		built.write = built
			.write
			.with_mutation_outcome(knowledge, crate::schema::MutationStopReason::NONE);
		let sent = self
			.finalize_table_delete_attempt(meta, built)
			.await
			.unwrap();
		(built, sent)
	}

	/// Test-only: rewind this node's HLC, as a coordinator whose clock is behind.
	pub(crate) fn rewind_hlc_for_test(&self) {
		self.hlc.rewind_for_test();
	}
}
