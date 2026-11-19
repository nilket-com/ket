//! ADR 0699 S4b, as wendelon ADR 0112 makes it: one delete statement under
//! ParentDecisions is one parent decision.
//!
//! A statement names 1 to 512 row ids of a table whose locality is a function
//! of the id alone, in canonical (identity) order. After the lookup, a node that
//! covers every requested position evaluates every row at ONE cut K with the S4
//! reader: proved winner, proved tombstone or proved absence; an unproved copy
//! is repaired or the whole statement is indeterminate. The selection is frozen
//! in the record, and its signer is the holder of the lowest matched position
//! (of the lowest requested one if nothing matched):
//! - live winners publish one tombstone each at one new HLC strictly above
//!   every selected winner, as the children of one parent (select at K, publish
//!   at dec), so a reader sees all of them gone or none;
//! - otherwise the signer records one RecordNoop, which linearizes at K.
//!
//! A tombstone suppresses every lower-HLC version of its key, including a
//! concurrent write ordered below it; the counts describe the frozen selection
//! at K, not that no other statement deleted the same rows (ADR 0699).

use super::*;
use crate::consensus::decisions::{
	ChildClaim, Claim, DeleteOutcome, DeleteRequest, DeleteSelection, LineageRef, Lookup,
	MAX_DELETE_ROWS, NoopBody, Operation, ParentKey, ParentNamespace, ParentStatus, Signer,
	TombstoneTarget, delete_request_digest, delete_row_locality, delete_row_position,
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

/// The version a matched row's tombstone publishes.
pub(crate) fn tombstone_version(target: &TombstoneTarget, hlc: HybridTimestamp) -> OwnedVersion {
	OwnedVersion {
		table: target.table,
		key_identity: *target.row.as_bytes(),
		hlc,
		row_digest: tombstone_digest(target.table, target.schema_generation, target.row),
	}
}

/// Wendelon ADR 0112: one child's rows of a matched delete statement, derived
/// from the recorded claim alone: the matched rows whose position lies in the
/// child's range, in identity order. The coordinator builds a child's content
/// from this and a holder checks its payload against it; `None` for a row with
/// no position.
pub(crate) fn child_rows(
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	request: &DeleteRequest,
	selection: &DeleteSelection,
	child: &ChildClaim,
) -> Option<Vec<Identity>> {
	let mut rows = Vec::new();
	for (row, _) in selection.matched(request) {
		let position = delete_row_position(tenant, descriptor, row)?;
		if child.range_start <= position && position <= child.range_end {
			rows.push(row);
		}
	}
	Some(rows)
}

/// The manifest of one child's tombstones at `hlc`, in identity order.
pub(crate) fn child_manifest(
	request: &DeleteRequest,
	rows: &[Identity],
	hlc: HybridTimestamp,
) -> Vec<OwnedVersion> {
	rows.iter()
		.map(|row| tombstone_version(&request.target(*row), hlc))
		.collect()
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

/// A delete child's payload: its row ids, in order.
pub(crate) fn row_id_batch(rows: &[Identity]) -> std::io::Result<RecordBatch> {
	let schema = Arc::new(arrow::datatypes::Schema::new(vec![
		arrow::datatypes::Field::new(ROW_ID, arrow::datatypes::DataType::Binary, false),
	]));
	RecordBatch::try_new(
		schema,
		vec![Arc::new(arrow::array::BinaryArray::from(
			rows.iter()
				.map(|row| &row.as_bytes()[..])
				.collect::<Vec<_>>(),
		))],
	)
	.map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

/// The ids a delete child's payload names, in order: at least one, each valid.
pub(crate) fn row_ids_of(batch: &RecordBatch) -> Option<Vec<Identity>> {
	let column = batch
		.column_by_name(ROW_ID)?
		.as_any()
		.downcast_ref::<arrow::array::BinaryArray>()?;
	if batch.num_rows() == 0 {
		return None;
	}
	(0..batch.num_rows())
		.map(|at| {
			if !column.is_valid(at) {
				return None;
			}
			let bytes: [u8; 32] = column.value(at).try_into().ok()?;
			Some(Identity::new(bytes))
		})
		.collect()
}

/// Appends the tombstone of `target.row` at `hlc` through today's tombstone
/// packing and the child append path.
pub(crate) async fn append_tombstone(
	slab: &SlabManager,
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	target: &TombstoneTarget,
	hlc: HybridTimestamp,
	secondary_indexes: &[SecondaryIndexDescriptor],
) -> std::io::Result<()> {
	let invalid = |why: &str| Error::new(ErrorKind::InvalidData, why.to_owned());
	let routing_locality = delete_row_locality(tenant, descriptor, target.row)
		.ok_or_else(|| invalid("an unsupported delete shape"))?;
	let partition =
		crate::storage::multirow::write_partition_for_row(descriptor, routing_locality)?;
	let rows = [crate::storage::multirow::MultiRowSourceRow {
		source_row_index: 0,
		key_identity: target.row,
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
		/// (deleted, already deleted, not found) of the frozen selection.
		counts: [u32; 3],
	},
	KnownNotCommitted,
	Redirect(NodeId),
	Indeterminate(WriteStatus),
	/// The caller's deadline passed: indeterminate, never known-not-committed.
	Deadline,
	Refused(WriteStatus, PbcRejectCode),
}

impl WendelonDataServer {
	/// A delete statement of a ParentDecisions node (wendelon ADR 0112).
	pub(super) async fn coordinate_table_delete(
		&self,
		meta: TableDeleteRowsRequestMeta,
		row_key_ids: Vec<Identity>,
	) -> TableDeleteRowsResponseMeta {
		let settled = self.coordinate_delete(&meta, &row_key_ids).await;
		self.delete_answer(&meta, settled)
	}

	/// Wendelon ADR 0112: the answer to a statement whose DECLARED count is over
	/// the bound, given before its Arrow payload is decoded: typed, no effect, and
	/// no identity bound.
	pub(super) fn delete_statement_too_large(
		&self,
		meta: &TableDeleteRowsRequestMeta,
	) -> TableDeleteRowsResponseMeta {
		self.delete_answer(
			meta,
			DeleteSettled::Refused(
				WriteStatus::PAYLOAD_TOO_LARGE,
				PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
			),
		)
	}

	/// The answer a settled delete statement gives.
	fn delete_answer(
		&self,
		meta: &TableDeleteRowsRequestMeta,
		settled: DeleteSettled,
	) -> TableDeleteRowsResponseMeta {
		let stop = if matches!(settled, DeleteSettled::Deadline) {
			MutationStopReason::DEADLINE
		} else {
			MutationStopReason::NONE
		};
		let (status, knowledge, dec, commit_hlc, hint, counts, reject) = match settled {
			DeleteSettled::Committed {
				dec,
				commit_hlc,
				counts,
			} => (
				WriteStatus::OK,
				MutationCommitKnowledge::COMMITTED,
				dec,
				commit_hlc,
				self.local_node,
				counts,
				PbcRejectCode::NONE,
			),
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
		// Wendelon ADR 0112: the statement's shape, checked before any lookup. A
		// shape refusal binds nothing to the identity.
		if row_key_ids.is_empty() || row_key_ids.len() != meta.row_count as usize {
			return unsupported(PbcRejectCode::PBC_REJECT_ROW_COUNT_MISMATCH);
		}
		if row_key_ids.len() > MAX_DELETE_ROWS {
			return DeleteSettled::Refused(
				WriteStatus::PAYLOAD_TOO_LARGE,
				PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
			);
		}
		// Canonical at ingress: identity order, no duplicate.
		let mut rows = row_key_ids.to_vec();
		rows.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
		if rows.windows(2).any(|pair| pair[0] == pair[1]) {
			return unsupported(PbcRejectCode::PBC_REJECT_DUPLICATE_PK_IN_BATCH);
		}
		let (Some(proposals), Some(signer)) = (
			self.decisions.proposals.clone(),
			self.decisions.signer.clone(),
		) else {
			return unsupported(PbcRejectCode::NONE);
		};
		let (tenant, table, generation) =
			(meta.tenant_id, meta.table_object_id, meta.schema_generation);
		let Some(descriptor) = self
			.state_machine
			.read_state(move |state| state.table_schemas.get(tenant, table, generation).cloned())
			.await
		else {
			return unsupported(PbcRejectCode::NONE);
		};
		let Some(positions) = rows
			.iter()
			.map(|row| delete_row_position(tenant, &descriptor, *row))
			.collect::<Option<Vec<u64>>>()
		else {
			return unsupported(PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED);
		};
		let request = DeleteRequest {
			table,
			schema_generation: generation,
			rows,
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
			// Recorded decisions replay before any coverage is required.
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
						counts: record.body.selection.counts(),
					};
				}
				LookupResponse::Found(Lookup::Recorded { record }) => {
					let Operation::Delete { selection, .. } = &record.claim.operation else {
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
								counts: selection.counts(),
							};
						}
						ParentStatus::Refused { .. } => return DeleteSettled::KnownNotCommitted,
						ParentStatus::Noop { .. } => return stalled,
						ParentStatus::Reserved => {
							// Only the recorded coordinator drives it (S3c rule).
							let Some(coordinating) = record
								.claim
								.children
								.get(usize::from(record.claim.coordinator))
							else {
								return stalled;
							};
							let (lineage, holder) = (coordinating.lineage, coordinating.holder);
							if holder.node != signer.signer.node {
								// ADR 0729: only while its recorded lineage is open
								// and leased; otherwise a stall.
								return match self
									.route_for_recorded_coordinator(lineage, holder)
									.await
								{
									Some(route) => DeleteSettled::Redirect(route),
									None => stalled,
								};
							}
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
					let proposal = match self
						.select_and_route(
							meta,
							&signer.signer,
							&descriptor,
							&request,
							&positions,
							deadline,
						)
						.await
					{
						Routed::Propose(build) => build,
						Routed::Settled(settled) => return settled,
					};
					// No fresh proposal from an evaluation that outlived its deadline.
					if expired() {
						return DeleteSettled::Deadline;
					}
					let proposal = match proposal {
						Proposed::Noop {
							selection,
							position,
							lineage,
						} => ForwardedDecision::RecordNoop {
							noop: crate::consensus::decisions::evidence::sign(
								&signer.key,
								NoopBody {
									key,
									request_digest: digest,
									request: request.clone(),
									selection,
									position,
									coordinator: signer.signer,
									lineage,
								},
								signer.signer,
							),
						},
						Proposed::Claim {
							selection,
							commit_hlc,
							children,
						} => {
							let mut claim = Claim {
								key,
								request_digest: digest,
								commit_hlc,
								coordinator: 0,
								first_row_position: children[0].range_start,
								children,
								operation: Operation::Delete {
									request: request.clone(),
									selection,
								},
							};
							// Each child's rows and content, from the claim as recorded.
							let Some(contents) = claim_child_contents(tenant, &descriptor, &claim)
							else {
								return stalled;
							};
							for (child, (rows, content)) in claim.children.iter_mut().zip(contents)
							{
								child.rows = rows;
								child.content = content;
							}
							ForwardedDecision::Reserve {
								claim: crate::consensus::decisions::evidence::sign(
									&signer.key,
									claim,
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
		// Publish the tombstone children, re-derived from the recorded claim and
		// never re-evaluated, then Finalize.
		let Operation::Delete {
			request: recorded,
			selection,
		} = &claim.operation
		else {
			return stalled;
		};
		let mut batch_rows = Vec::new();
		let mut indices = Vec::with_capacity(claim.children.len());
		for child in &claim.children {
			let Some(rows) = child_rows(tenant, &descriptor, recorded, selection, child) else {
				return stalled;
			};
			indices.push(
				(batch_rows.len() as u32..(batch_rows.len() + rows.len()) as u32)
					.collect::<Vec<u32>>(),
			);
			batch_rows.extend(rows);
		}
		let Ok(batch) = row_id_batch(&batch_rows) else {
			return stalled;
		};
		let counts = selection.counts();
		// Locked persistence runs in its own task and finishes if this ends.
		let Some(acks) = bounded(
			deadline,
			self.write_children(&claim, rsv, &descriptor, &batch, &indices),
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
			} => DeleteSettled::Committed {
				dec,
				commit_hlc: claim.commit_hlc,
				counts,
			},
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

	/// Wendelon ADR 0112 D2, for an unrecorded identity: the one eligibility
	/// predicate, the same at every entry. From one placement view, the fallback
	/// holder F holds the lowest requested position; a request F does not cover
	/// (F is not in the replica plan of every requested position) is unsupported
	/// before any evaluation. A node that covers the request evaluates it at one
	/// cut; one that does not sends it to F. After selection the signer S holds the lowest
	/// matched position (F if none matched): S proposes, another node sends it to
	/// S if S covers it. Anything else is unsupported, typed, with no proposal.
	/// A placement change during the evaluation stalls, never proposes.
	async fn select_and_route(
		&self,
		meta: &TableDeleteRowsRequestMeta,
		local: &Signer,
		descriptor: &TableSchemaDescriptor,
		request: &DeleteRequest,
		positions: &[u64],
		deadline: Option<std::time::Instant>,
	) -> Routed {
		let stalled = Routed::Settled(DeleteSettled::Indeterminate(WriteStatus::WRITE_STALLED));
		let unsupported = Routed::Settled(DeleteSettled::Refused(
			WriteStatus::INTERNAL_ERROR,
			PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED,
		));
		let tenant = meta.tenant_id;
		let view = {
			let positions = positions.to_vec();
			self.state_machine
				.read_state(move |state| Placement::of(state, tenant, &positions))
				.await
		};
		let Some(view) = view else {
			return stalled;
		};
		let fallback = view.lowest(0..positions.len());
		// The fallback holder's coverage first, whatever the entry: a statement F
		// cannot serve is unsupported at every node, before any evaluation.
		let holder = view.placed[fallback].holder();
		if !view.covers(&holder) {
			return unsupported;
		}
		// An entry that does not cover the request sends it to F, which does.
		if !view.covers(local) {
			return self.redirect_to(positions[fallback]).await;
		}
		// The one evaluation both the tombstones and a noop come from.
		let selection = match bounded(
			deadline,
			self.evaluate_delete(tenant, descriptor, &request.rows, positions),
		)
		.await
		{
			Some(Ok(selection)) => selection,
			Some(Err(())) => return stalled,
			None => return Routed::Settled(DeleteSettled::Deadline),
		};
		// One coherent view: the placement the evaluation ran under.
		let again = {
			let positions = positions.to_vec();
			self.state_machine
				.read_state(move |state| Placement::of(state, tenant, &positions))
				.await
		};
		if again.as_ref() != Some(&view) {
			return stalled;
		}
		let matched: Vec<usize> = (0..positions.len())
			.filter(|at| matches!(selection.outcomes[*at], DeleteOutcome::Deleted { .. }))
			.collect();
		let signing = if matched.is_empty() {
			fallback
		} else {
			view.lowest(matched.iter().copied())
		};
		let holder = view.placed[signing].holder();
		if holder != *local {
			if holder.node != local.node && view.covers(&holder) {
				return self.redirect_to(positions[signing]).await;
			}
			return if holder.node == local.node {
				stalled
			} else {
				unsupported
			};
		}
		if matched.is_empty() {
			let placed = &view.placed[fallback];
			return Routed::Propose(Proposed::Noop {
				selection,
				position: positions[fallback],
				lineage: placed.lineage_ref(),
			});
		}
		// The children: runs of matched positions, contiguous in placement order,
		// inside one lineage, with one replica plan throughout.
		let mut order = matched;
		order.sort_unstable_by_key(|at| positions[*at]);
		let mut children: Vec<(usize, usize)> = Vec::new();
		for at in order {
			match children.last_mut() {
				Some((_, last)) if view.same_child(*last, at) => *last = at,
				_ => children.push((at, at)),
			}
		}
		if children.len() > crate::consensus::decisions::MAX_CHILDREN {
			return Routed::Settled(DeleteSettled::Refused(
				WriteStatus::PAYLOAD_TOO_LARGE,
				PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE,
			));
		}
		// ADR 0729: above every child lineage's floor, and strictly above every
		// selected winner.
		let floor_ms = children
			.iter()
			.map(|(first, _)| view.placed[*first].lineage.floor_ms)
			.max()
			.unwrap_or(0);
		let Some(winner) = selection.max_deleted_winner() else {
			return stalled;
		};
		let Some(commit_hlc) = tombstone_hlc(
			&self.hlc,
			winner,
			super::coordinator::above_floor(meta.client_hlc, floor_ms),
		) else {
			return stalled;
		};
		let children = children
			.into_iter()
			.map(|(first, last)| {
				let placed = &view.placed[first];
				ChildClaim {
					range_start: positions[first],
					range_end: positions[last],
					lineage: placed.lineage_ref(),
					holder: placed.holder(),
					plan: placed.plan.clone(),
					rows: 0,
					content: [0; 32],
				}
			})
			.collect::<Vec<_>>();
		Routed::Propose(Proposed::Claim {
			selection,
			commit_hlc,
			children,
		})
	}

	/// A redirect to the holder of `position`, only while its range is leased.
	async fn redirect_to(&self, position: u64) -> Routed {
		Routed::Settled(match self.route_for_position(position).await {
			Some(route) if route.id != self.local_node.id => DeleteSettled::Redirect(route),
			_ => DeleteSettled::Indeterminate(WriteStatus::WRITE_STALLED),
		})
	}

	/// The statement's evaluation at one fresh cut: for each row, the S4
	/// reader's proved winner (repaired if this store lacks it), a proved
	/// tombstone, or proved absence over the complete inventory. Any unproved
	/// row makes the whole statement an error, never NotFound.
	async fn evaluate_delete(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		rows: &[Identity],
		positions: &[u64],
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
		let mut outcomes = Vec::with_capacity(rows.len());
		for (row, position) in rows.iter().zip(positions) {
			outcomes.push(
				self.proved_point(tenant, descriptor, *row, *position, k)
					.await
					.map_err(|_| ())?,
			);
		}
		Ok(DeleteSelection { cut: k, outcomes })
	}
}

/// Where the routing rule ended for a fresh statement.
enum Routed {
	Propose(Proposed),
	Settled(DeleteSettled),
}

/// What this node, as the statement's signer, proposes.
enum Proposed {
	Noop {
		selection: DeleteSelection,
		position: u64,
		lineage: LineageRef,
	},
	/// The children's rows and contents are filled from the claim itself.
	Claim {
		selection: DeleteSelection,
		commit_hlc: HybridTimestamp,
		children: Vec<ChildClaim>,
	},
}

/// One requested position's placement: its lineage and its ring segment's
/// replica plan, each member at its current incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Placed {
	position: u64,
	lineage_start: u64,
	lineage: crate::consensus::decisions::Lineage,
	segment: usize,
	plan: Vec<Signer>,
}

impl Placed {
	fn holder(&self) -> Signer {
		Signer {
			node: self.lineage.holder.id,
			incarnation: self.lineage.incarnation,
		}
	}

	fn lineage_ref(&self) -> LineageRef {
		LineageRef {
			range_start: self.lineage_start,
			generation: self.lineage.generation,
		}
	}
}

/// One coherent placement view of a statement: each requested position's
/// placement, and every ring segment's replica set (for grouping across
/// segments).
#[derive(Debug, PartialEq, Eq)]
struct Placement {
	placed: Vec<Placed>,
	segments: Vec<Vec<u32>>,
}

impl Placement {
	fn of(state: &SmState, tenant: TenantId, positions: &[u64]) -> Option<Self> {
		let (_, config) = state.epoch_history.last_key_value()?;
		let record = state.tenants.get(&tenant)?;
		let ring = crate::network::ring::RingState::build(
			&crate::network::ring::membership_from_epoch_config(config),
			record.replication_factor as usize,
		);
		let all = ring.replica_segments();
		let lineages = state.decisions.as_ref()?.lineages();
		let mut placed = Vec::with_capacity(positions.len());
		for position in positions {
			let segment = all
				.iter()
				.position(|segment| segment.start <= *position && *position <= segment.end)?;
			let (lineage_start, lineage) = lineages
				.range(..=*position)
				.next_back()
				.filter(|(_, lineage)| *position <= lineage.range_end)
				.map(|(start, lineage)| (*start, *lineage))?;
			let plan = all[segment]
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
				.collect::<Option<Vec<_>>>()?;
			placed.push(Placed {
				position: *position,
				lineage_start,
				lineage,
				segment,
				plan,
			});
		}
		let segments = all
			.iter()
			.map(|segment| segment.replicas.iter().map(|node| node.id).collect())
			.collect();
		Some(Self { placed, segments })
	}

	/// Whether `node` is in the replica plan of every requested position.
	fn covers(&self, node: &Signer) -> bool {
		self.placed.iter().all(|placed| placed.plan.contains(node))
	}

	/// The index, among `at`, of the lowest position.
	fn lowest(&self, at: impl Iterator<Item = usize>) -> usize {
		at.min_by_key(|at| self.placed[*at].position)
			.expect("a statement names a row")
	}

	/// Whether matched positions `a` then `b` (b next in placement order) fall in
	/// one child: one lineage, and one replica set over every segment between.
	fn same_child(&self, a: usize, b: usize) -> bool {
		let (a, b) = (&self.placed[a], &self.placed[b]);
		a.lineage_start == b.lineage_start
			&& a.plan == b.plan
			&& a.segment <= b.segment
			&& self.segments[a.segment..=b.segment]
				.iter()
				.all(|replicas| *replicas == self.segments[a.segment])
	}
}

/// The row count and content of each child of a matched delete claim, from the
/// claim alone. `None` when a row has no position.
fn claim_child_contents(
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	claim: &Claim,
) -> Option<Vec<(u32, [u8; 32])>> {
	let Operation::Delete { request, selection } = &claim.operation else {
		return None;
	};
	claim
		.children
		.iter()
		.map(|child| {
			let rows = child_rows(tenant, descriptor, request, selection, child)?;
			let content = super::child_writes::manifest_content(&child_manifest(
				request,
				&rows,
				claim.commit_hlc,
			));
			Some((rows.len() as u32, content))
		})
		.collect()
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
