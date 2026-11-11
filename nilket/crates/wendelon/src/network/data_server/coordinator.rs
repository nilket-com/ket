//! ADR 0699 S3c: the coordinator of a table write under ParentDecisions.
//!
//! The node that receives a table write coordinates it only if it holds the
//! lineage of the first input row's ring position; otherwise it redirects the
//! caller there. It looks the parent up first: a recorded parent is resumed
//! from its original claim or answered from its recorded status, and only an
//! absent, eligible key gets a new claim. A new claim is stamped, signed and
//! reserved through the leader; each child's holder then writes and
//! acknowledges it; and Finalize decides. Only a recorded Accepted answers
//! COMMITTED and only a recorded Refused answers KNOWN_NOT_COMMITTED.

use super::child_writes::{ChildWriteMeta, child_request_meta, child_versions};
use super::write_deadline::{
	AttemptGate, EarlyAnswer, EarlyTableWriteAnswer, OwnedAttempt, answer_by_deadline,
};
use super::*;
use crate::consensus::decisions::{
	ChildAckBody, ChildClaim, Claim, DecisionResult, LineageRef, Lookup, ParentKey,
	ParentNamespace, ParentStatus, Signed, Signer,
};
use crate::network::decision_proposals::{
	ForwardedDecision, LookupRequest, LookupResponse, ProposalOutcome, ProposalResponse,
};
use std::collections::BTreeMap;

/// Test-only trace of why a coordinated write stopped.
macro_rules! diag {
	($($arg:tt)*) => {{
		#[cfg(test)]
		{
			eprintln!("COORD {}", format!($($arg)*));
		}
		#[cfg(not(test))]
		{
			let _ = format_args!($($arg)*);
		}
	}};
}

/// A row's ring position: its partition for a partitioned table, its locality
/// otherwise (ADR 0090), as a stored record of it is placed.
pub(super) fn row_position(
	descriptor: &TableSchemaDescriptor,
	locality: Locality,
) -> std::io::Result<u64> {
	match crate::schema::table::partitioned_prefix_width(descriptor) {
		Some(bits) => Ok(crate::schema::table::partition_for_locality(locality, bits)?.get()),
		None => Ok(locality.get()),
	}
}

/// The request digest a key is bound to: the table, the schema generation and
/// each row's identity and content, independent of order and of the HLC.
pub(super) fn request_digest(
	tenant: TenantId,
	descriptor: &TableSchemaDescriptor,
	manifest: &[crate::storage::index::parent_intents::OwnedVersion],
) -> [u8; 32] {
	let mut hasher = blake3::Hasher::new_derive_key("wendelon parent table request v1");
	hasher.update(&tenant.get().to_le_bytes());
	hasher.update(descriptor.table_object_id.as_bytes());
	hasher.update(&descriptor.schema_generation.to_le_bytes());
	for version in manifest {
		hasher.update(&version.key_identity);
		hasher.update(&version.row_digest);
	}
	*hasher.finalize().as_bytes()
}

/// How a coordinated write ends, before it becomes a wire response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Settled {
	Committed {
		dec: u64,
		commit_hlc: HybridTimestamp,
	},
	KnownNotCommitted,
	Redirect(NodeId),
	/// Not decided here; the same identity may be retried.
	Indeterminate(WriteStatus),
	/// Refused for this identity; retrying it as-is cannot help.
	Refused(WriteStatus),
	/// The caller's deadline is the answer. Never known-not-committed: this
	/// attempt or an earlier one of the same identity may have reserved.
	Deadline,
}

/// Builds a claim for a fresh parent, or says why this node cannot. Rows are
/// grouped by the replica segment holding their position, each group a child
/// of its lineage. Which rows each child carries is `rows_of_claim`'s to say.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_claim(
	state: &SmState,
	local: Signer,
	key: ParentKey,
	request_digest: [u8; 32],
	descriptor: &TableSchemaDescriptor,
	batch: &RecordBatch,
	positions: &[u64],
	commit_hlc: HybridTimestamp,
) -> Result<Claim, Settled> {
	let stalled = Settled::Indeterminate(WriteStatus::WRITE_STALLED);
	let decisions = state.decisions.as_ref().ok_or(stalled)?;
	let lineage_at = |position: u64| {
		decisions
			.lineages()
			.range(..=position)
			.next_back()
			.filter(|(_, lineage)| position <= lineage.range_end)
			.map(|(start, lineage)| (*start, *lineage))
	};
	let first = *positions
		.first()
		.ok_or(Settled::Refused(WriteStatus::INTERNAL_ERROR))?;
	let (_, first_lineage) = lineage_at(first).ok_or(stalled)?;
	if first_lineage.holder.id != local.node || first_lineage.incarnation != local.incarnation {
		return Err(Settled::Redirect(first_lineage.holder));
	}
	let (_, config) = state.epoch_history.last_key_value().ok_or(stalled)?;
	let tenant = state.tenants.get(&key.tenant).ok_or(stalled)?;
	let ring = crate::network::ring::RingState::build(
		&crate::network::ring::membership_from_epoch_config(config),
		tenant.replication_factor as usize,
	);
	let segments = ring.replica_segments();
	let mut groups: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
	for (row, position) in positions.iter().enumerate() {
		let segment = segments
			.iter()
			.find(|segment| segment.start <= *position && *position <= segment.end)
			.ok_or(stalled)?;
		groups.entry(segment.start).or_default().push(row as u32);
	}
	let mut children = Vec::with_capacity(groups.len());
	let mut coordinator = None;
	for (segment_start, rows) in groups {
		let segment = segments
			.iter()
			.find(|segment| segment.start == segment_start)
			.expect("grouped by an existing segment");
		let low = rows
			.iter()
			.map(|row| positions[*row as usize])
			.min()
			.expect("a non-empty group");
		let high = rows
			.iter()
			.map(|row| positions[*row as usize])
			.max()
			.expect("a non-empty group");
		let (lineage_start, lineage) = lineage_at(low).ok_or(stalled)?;
		let current = |node: u32| {
			state
				.incarnations
				.get(&node)
				.and_then(|registry| registry.current)
				.map(|incarnation| Signer { node, incarnation })
		};
		let plan = segment
			.replicas
			.iter()
			.map(|node| current(node.id))
			.collect::<Option<Vec<_>>>()
			.ok_or(stalled)?;
		let sub = arrow::compute::take_record_batch(
			batch,
			&arrow::array::UInt32Array::from(rows.clone()),
		)
		.map_err(|_| Settled::Refused(WriteStatus::INTERNAL_ERROR))?;
		let request = child_request_meta(
			&key,
			descriptor.table_object_id,
			descriptor.schema_generation,
			sub.num_rows(),
			commit_hlc,
		)
		.map_err(|_| Settled::Refused(WriteStatus::INTERNAL_ERROR))?;
		let (_, manifest) = child_versions(&request, &sub, descriptor, commit_hlc)
			.map_err(|_| Settled::Refused(WriteStatus::INTERNAL_ERROR))?;
		if rows.contains(&0) {
			coordinator = Some(children.len() as u16);
		}
		children.push(ChildClaim {
			range_start: low,
			range_end: high,
			lineage: LineageRef {
				range_start: lineage_start,
				generation: lineage.generation,
			},
			holder: Signer {
				node: lineage.holder.id,
				incarnation: lineage.incarnation,
			},
			plan,
			rows: manifest.len() as u32,
			content: super::child_writes::manifest_content(&manifest),
		});
	}
	Ok(Claim {
		key,
		request_digest,
		commit_hlc,
		coordinator: coordinator.ok_or(stalled)?,
		first_row_position: first,
		children,
		operation: crate::consensus::decisions::Operation::Write,
	})
}

/// A write attempt's proposal of a Reserve or a Finalize, which only an attempt
/// that claimed its gate may make.
async fn propose_owned(
	proposals: &crate::network::decision_proposals::DecisionProposals,
	_owned: &OwnedAttempt,
	decision: ForwardedDecision,
) -> ProposalResponse {
	proposals.propose(decision).await
}

/// The rows of each child of a retained claim, by the child ranges.
pub(super) fn rows_of_claim(claim: &Claim, positions: &[u64]) -> Option<Vec<Vec<u32>>> {
	let mut rows = vec![Vec::new(); claim.children.len()];
	for (row, position) in positions.iter().enumerate() {
		let child = claim
			.children
			.iter()
			.position(|child| child.range_start <= *position && *position <= child.range_end)?;
		rows[child].push(row as u32);
	}
	let complete = claim
		.children
		.iter()
		.zip(&rows)
		.all(|(child, rows)| child.rows as usize == rows.len());
	complete.then_some(rows)
}

impl WendelonDataServer {
	pub(super) fn merge_commit_hlc(&self, client_hlc: HybridTimestamp) -> HybridTimestamp {
		self.hlc.merge(client_hlc)
	}

	fn settled_response(
		&self,
		meta: &TableWriteRequestMeta,
		settled: Settled,
	) -> TableWriteResponseMeta {
		let stop = if settled == Settled::Deadline {
			MutationStopReason::DEADLINE
		} else {
			MutationStopReason::NONE
		};
		let (status, knowledge, log_index, commit_hlc, hint) = match settled {
			Settled::Committed { dec, commit_hlc } => (
				WriteStatus::OK,
				MutationCommitKnowledge::COMMITTED,
				dec,
				commit_hlc,
				self.local_node,
			),
			Settled::KnownNotCommitted => (
				WriteStatus::OK,
				MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
				0,
				meta.client_hlc,
				self.local_node,
			),
			Settled::Redirect(holder) => (
				WriteStatus::NOT_LEADER,
				MutationCommitKnowledge::INDETERMINATE,
				0,
				meta.client_hlc,
				holder,
			),
			Settled::Indeterminate(status) | Settled::Refused(status) => (
				status,
				MutationCommitKnowledge::INDETERMINATE,
				0,
				meta.client_hlc,
				self.local_node,
			),
			Settled::Deadline => (
				WriteStatus::WRITE_DEADLINE_EXCEEDED,
				MutationCommitKnowledge::INDETERMINATE,
				0,
				meta.client_hlc,
				self.local_node,
			),
		};
		table_write_response(
			self.counted_write_response(
				WriteResponseMeta {
					status,
					_pad1: [0; 7],
					log_index,
					term: 0,
					epoch: meta.expected_epoch,
					commit_hlc,
					leader_hint: hint,
					echoed_write_id: meta.parent_write_id,
					has_finalization: 0,
					_pad2: [0; 7],
				}
				.with_mutation_outcome(knowledge, stop),
			),
			PbcRejectCode::NONE,
		)
	}

	/// The table write of a ParentDecisions node.
	///
	/// A caller's deadline is one absolute budget, fixed here once and never
	/// renewed, with every lookup inside it. By it the coordinator has its
	/// answer: its own result, or WRITE_DEADLINE_EXCEEDED, INDETERMINATE, stop
	/// reason DEADLINE. That answer is never recorded and is no abort. An
	/// attempt that had not yet claimed its gate is dropped, having submitted
	/// nothing. One that had is given to `early` and still polled here to its
	/// end, so this call returns only when the coordination has ended; with no
	/// `early` its deadline answer is returned then. A cooperative bound on
	/// the coordinator's answer, plus scheduling slack: not a real-time
	/// guarantee, and not a bound on what the handler does with an ordinary
	/// result afterwards (the last-attempt outcome record is outside it).
	pub(super) async fn coordinate_table_write(
		&self,
		meta: TableWriteRequestMeta,
		batch: RecordBatch,
		early: Option<EarlyAnswer>,
	) -> std::io::Result<(TableWriteResponseMeta, Option<FinalizationMarker>)> {
		let namespace = ParentNamespace::Table(meta.table_object_id);
		if meta.table_object_id == crate::schema::TableObjectId::DEFAULT_KV {
			let settled = Settled::Refused(WriteStatus::INTERNAL_ERROR);
			return Ok((self.settled_response(&meta, settled), None));
		}
		let gate = AttemptGate::until(caller_deadline_from_remaining_millis(
			meta.deadline_remaining_millis,
		));
		let mut given = None;
		let settled = answer_by_deadline(
			&gate,
			self.coordinate(namespace, &meta, &batch, &gate),
			|| {
				let response = self.settled_response(&meta, Settled::Deadline);
				if let Some(early) = early {
					early.give(EarlyTableWriteAnswer {
						request: meta,
						response,
					});
				}
				given = Some(response);
			},
		)
		.await;
		let response = match (settled, given) {
			(Some(settled), _) => self.settled_response(&meta, settled),
			// Already answered, and counted once: the coordination's own result
			// is not a second answer.
			(None, Some(given)) => given,
			(None, None) => self.settled_response(&meta, Settled::Deadline),
		};
		Ok((response, None))
	}

	/// ADR 0699 S4b: a single-key KV write of a ParentDecisions node, as a
	/// one-row parent of the default KV table under the `Kv` namespace. The
	/// wire identity must be the key's.
	pub(crate) async fn coordinate_kv_write(
		&self,
		meta: WriteRequestMeta,
		payload: RecordBatch,
	) -> WriteResponseMeta {
		let mut table_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
		table_meta.parent_write_id = meta.write_id;
		table_meta.tenant_id = meta.tenant_id;
		table_meta.table_object_id = crate::schema::TableObjectId::DEFAULT_KV;
		table_meta.schema_generation = 0;
		table_meta.expected_epoch = meta.expected_epoch;
		table_meta.client_hlc = meta.client_hlc;
		let refused = |status| {
			self.settled_response(&table_meta, Settled::Refused(status))
				.write
		};
		let key = payload.column_by_name("key").and_then(|column| {
			column
				.as_any()
				.downcast_ref::<arrow::array::LargeBinaryArray>()
		});
		let Some(key) = key.filter(|key| payload.num_rows() == 1 && key.is_valid(0)) else {
			return refused(WriteStatus::INTERNAL_ERROR);
		};
		if Identity::blake3_tenant(meta.tenant_id, key.value(0)) != meta.key_identity {
			return refused(WriteStatus::WRITE_IDENTITY_INVALID);
		}
		let Ok(row_count) = crate::schema::encode_table_write_row_count(1) else {
			return refused(WriteStatus::INTERNAL_ERROR);
		};
		table_meta.row_count = row_count;
		// A KV write carries no deadline on the wire.
		let settled = self
			.coordinate(
				ParentNamespace::Kv,
				&table_meta,
				&payload,
				&AttemptGate::unbounded(),
			)
			.await;
		self.settled_response(&table_meta, settled).write
	}

	/// One attempt. `gate` is claimed at the attempt's two boundary points and
	/// nowhere else: immediately before the Reserve proposal is first polled,
	/// and immediately before a recorded Reserved parent is resumed. Nothing
	/// before a claim submits a Reserve, a child or a Finalize (it reads, and
	/// merges the commit clock); a refused claim ends the attempt with the
	/// deadline's answer, having proposed and resumed nothing.
	async fn coordinate(
		&self,
		namespace: ParentNamespace,
		meta: &TableWriteRequestMeta,
		batch: &RecordBatch,
		gate: &AttemptGate,
	) -> Settled {
		let stalled = Settled::Indeterminate(WriteStatus::WRITE_STALLED);
		let (Some(proposals), Some(signer)) = (
			self.decisions.proposals.clone(),
			self.decisions.signer.clone(),
		) else {
			return Settled::Refused(WriteStatus::INTERNAL_ERROR);
		};
		let (tenant, table, generation) =
			(meta.tenant_id, meta.table_object_id, meta.schema_generation);
		let Some(descriptor) = self
			.state_machine
			.read_state(move |state| {
				super::child_writes::table_descriptor(state, tenant, table, generation)
			})
			.await
		else {
			return Settled::Refused(WriteStatus::INTERNAL_ERROR);
		};
		let key = ParentKey {
			tenant,
			namespace,
			client_uuid: meta.parent_write_id.client_uuid,
			sequence_no: meta.parent_write_id.sequence_no,
		};
		// The request's identity, rows and positions: none depends on the HLC.
		let Ok(probe) =
			child_request_meta(&key, table, generation, batch.num_rows(), meta.client_hlc)
		else {
			return Settled::Refused(WriteStatus::INTERNAL_ERROR);
		};
		let Ok((_, manifest)) = child_versions(&probe, batch, &descriptor, meta.client_hlc) else {
			return Settled::Refused(WriteStatus::INTERNAL_ERROR);
		};
		let digest = request_digest(tenant, &descriptor, &manifest);
		let Ok(admission) = validate_table_write_request_batch(
			&probe,
			batch,
			&descriptor,
			super::child_writes::writer_of(meta.client_hlc),
			meta.client_hlc,
		) else {
			return Settled::Refused(WriteStatus::INTERNAL_ERROR);
		};
		let Ok(positions) = admission
			.localities
			.iter()
			.map(|locality| row_position(&descriptor, *locality))
			.collect::<std::io::Result<Vec<_>>>()
		else {
			return Settled::Refused(WriteStatus::INTERNAL_ERROR);
		};
		// The record decides. Lookup first, with no clock and no current
		// authority before it; after any Reserve answer, look up again and act
		// only on the recorded claim, never on a locally built one.
		let mut reserved_once = false;
		// This attempt's claim of its gate, once it has one.
		let mut owned: Option<OwnedAttempt> = None;
		let positions = &positions;
		let (claim, rsv, child_rows, owned) = loop {
			let lookup = proposals
				.lookup(LookupRequest {
					key,
					request_digest: digest,
					sent_ms: proposals.now_ms(),
				})
				.await;
			match lookup {
				LookupResponse::Found(Lookup::Recorded { record }) => match record.status {
					ParentStatus::Accepted { dec } => {
						return Settled::Committed {
							dec,
							commit_hlc: record.claim.commit_hlc,
						};
					}
					ParentStatus::Refused { .. } => return Settled::KnownNotCommitted,
					// A body never carries a noop status.
					ParentStatus::Noop { .. } => return stalled,
					ParentStatus::Reserved => {
						// Only the recorded coordinator drives a Reserved parent.
						let Some((coordinator, lineage)) = record
							.claim
							.children
							.get(usize::from(record.claim.coordinator))
							.map(|child| (child.holder, child.lineage))
						else {
							return stalled;
						};
						if coordinator.node != signer.signer.node {
							diag!(
								"resume: redirect to recorded coordinator {}",
								coordinator.node
							);
							return Settled::Redirect(NodeId {
								id: coordinator.node,
								_reserved: 0,
							});
						}
						let open = self
							.state_machine
							.read_state(move |state| {
								crate::consensus::decisions::lineage_is_open(
									state,
									lineage,
									coordinator,
								)
							})
							.await;
						if coordinator != signer.signer || !open {
							// The reserving coordinator's authority has ended; recorded
							// closure and Resolve settle it.
							return stalled;
						}
						let Some(rows) = rows_of_claim(&record.claim, positions) else {
							return Settled::Refused(WriteStatus::WRITE_KEY_CONFLICT);
						};
						// The resume boundary: the last reads are done and nothing is
						// awaited between this claim and the resumed work. An attempt
						// that reserved above already owns its gate.
						let Some(owned) = owned.take().or_else(|| gate.claim()) else {
							return Settled::Deadline;
						};
						break (record.claim, record.rsv, rows, owned);
					}
				},
				LookupResponse::Found(Lookup::Absent { eligible: true }) if !reserved_once => {
					#[cfg(test)]
					self.decisions
						.pause_at(super::child_writes::PauseStage::AfterAbsent)
						.await;
					let commit_hlc = self.merge_commit_hlc(meta.client_hlc);
					let local = signer.signer;
					let claim_descriptor = descriptor.clone();
					let built = self
						.state_machine
						.read_state(move |state| {
							build_claim(
								state,
								local,
								key,
								digest,
								&claim_descriptor,
								batch,
								positions,
								commit_hlc,
							)
						})
						.await;
					let built = match built {
						Ok(built) => built,
						Err(settled) => {
							diag!("build_claim: {settled:?}");
							return settled;
						}
					};
					let reserve = ForwardedDecision::Reserve {
						claim: crate::consensus::decisions::evidence::sign(
							&signer.key,
							built,
							signer.signer,
						),
					};
					// The Reserve boundary: nothing is awaited between this claim
					// and the proposal's first poll.
					let Some(claimed) = gate.claim() else {
						return Settled::Deadline;
					};
					let reserved = propose_owned(&proposals, &claimed, reserve).await;
					owned = Some(claimed);
					match reserved {
						// A fresh reservation, a duplicate of a racing one, or one already
						// terminal: in every case the record, read again, decides.
						ProposalResponse::Applied {
							outcome:
								ProposalOutcome::Decision(
									DecisionResult::Reserved { .. }
									| DecisionResult::Terminal { .. },
								),
							..
						} => {
							#[cfg(test)]
							self.decisions
								.pause_at(super::child_writes::PauseStage::AfterReserve)
								.await;
							reserved_once = true;
						}
						other => {
							diag!("reserve: {other:?}");
							return stalled;
						}
					}
				}
				LookupResponse::Found(Lookup::Absent { eligible: false }) => {
					return Settled::Indeterminate(WriteStatus::WRITE_OPERATION_EXPIRED);
				}
				// A write's digest never matches a delete's: a recorded noop under
				// this key is another request.
				LookupResponse::Found(Lookup::Noop { .. } | Lookup::Conflict) => {
					return Settled::Refused(WriteStatus::WRITE_KEY_CONFLICT);
				}
				LookupResponse::Found(Lookup::InvalidIdentity) => {
					return Settled::Refused(WriteStatus::WRITE_IDENTITY_INVALID);
				}
				// Absent after this attempt reserved, or the lookup failed: unknown.
				other => {
					diag!("lookup: {other:?}");
					return stalled;
				}
			}
		};
		let acks = self
			.write_children(&claim, rsv, &descriptor, batch, &child_rows)
			.await;
		#[cfg(test)]
		self.decisions
			.pause_at(super::child_writes::PauseStage::BeforeFinalize)
			.await;
		let finalized = propose_owned(
			&proposals,
			&owned,
			ForwardedDecision::Finalize {
				key: claim.key,
				request_digest: claim.request_digest,
				acks,
			},
		)
		.await;
		match finalized {
			ProposalResponse::Applied {
				outcome:
					ProposalOutcome::Decision(DecisionResult::Terminal {
						status: ParentStatus::Accepted { dec },
						..
					}),
				..
			} => Settled::Committed {
				dec,
				commit_hlc: claim.commit_hlc,
			},
			ProposalResponse::Applied {
				outcome:
					ProposalOutcome::Decision(DecisionResult::Terminal {
						status: ParentStatus::Refused { .. },
						..
					}),
				..
			} => Settled::KnownNotCommitted,
			other => {
				diag!("finalize: {other:?} with {} acks", claim.children.len());
				stalled
			}
		}
	}

	/// Each child written at its holder, which writes its own copy and gathers
	/// its plan's acknowledgements. Failures leave acknowledgements out; quorum
	/// is the reducer's to judge.
	pub(super) async fn write_children(
		&self,
		claim: &Claim,
		rsv: u64,
		descriptor: &TableSchemaDescriptor,
		batch: &RecordBatch,
		child_rows: &[Vec<u32>],
	) -> Vec<Signed<ChildAckBody>> {
		let digest = crate::consensus::decisions::claim_digest(claim);
		let mut acks = Vec::new();
		for (ordinal, (child, rows)) in claim.children.iter().zip(child_rows).enumerate() {
			let Ok(sub) = arrow::compute::take_record_batch(
				batch,
				&arrow::array::UInt32Array::from(rows.clone()),
			) else {
				continue;
			};
			let Ok(rows_bytes) = crate::storage::multirow::encode_batch(&sub) else {
				continue;
			};
			let meta = ChildWriteMeta {
				rsv,
				ordinal: ordinal as u16,
				claim_digest: digest,
				schema_generation: descriptor.schema_generation,
			};
			if child.holder.node == self.local_node.id {
				acks.extend(
					self.holder_write(self.local_node.id, meta, sub, Arc::new(rows_bytes))
						.await,
				);
			} else if let Ok(meta_bytes) = postcard::to_stdvec(&meta) {
				let response = super::child_writes::send_child_frame(
					self.factory.clone(),
					self.pool.clone(),
					NodeId {
						id: child.holder.node,
						_reserved: 0,
					},
					super::child_writes::ChildRpc::Write,
					meta_bytes,
					Arc::new(rows_bytes),
				)
				.await;
				match response {
					Ok(super::child_writes::ChildWriteResponse::Acked(child_acks)) => {
						acks.extend(child_acks)
					}
					other => diag!("child {ordinal} at {}: {other:?}", child.holder.node),
				}
			}
		}
		acks
	}
}

#[cfg(test)]
impl WendelonDataServer {
	/// The holder of the lineage at a batch's first row's position, from this
	/// node's applied state, with no write: how a control picks a row whose
	/// primary is or is not a given node.
	pub(crate) async fn primary_for_test(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		batch: &RecordBatch,
	) -> Option<u32> {
		let hlc = self.merge_commit_hlc(HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		});
		let key = ParentKey {
			tenant,
			namespace: ParentNamespace::Table(descriptor.table_object_id),
			client_uuid: [0; 16],
			sequence_no: 1,
		};
		let meta = child_request_meta(
			&key,
			descriptor.table_object_id,
			descriptor.schema_generation,
			batch.num_rows(),
			hlc,
		)
		.ok()?;
		let admission = validate_table_write_request_batch(
			&meta,
			batch,
			descriptor,
			super::child_writes::writer_of(hlc),
			hlc,
		)
		.ok()?;
		let position = row_position(descriptor, *admission.localities.first()?).ok()?;
		self.state_machine
			.read_state(move |state| {
				state
					.decisions
					.as_ref()?
					.lineages()
					.range(..=position)
					.next_back()
					.filter(|(_, lineage)| position <= lineage.range_end)
					.map(|(_, lineage)| lineage.holder.id)
			})
			.await
	}

	/// A one-row batch's key identity and routing locality, as admission
	/// derives them.
	pub(crate) fn row_identity_and_locality_for_test(
		&self,
		tenant: TenantId,
		descriptor: &TableSchemaDescriptor,
		batch: &RecordBatch,
	) -> (Identity, Locality) {
		let hlc = self.merge_commit_hlc(HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		});
		let key = ParentKey {
			tenant,
			namespace: ParentNamespace::Table(descriptor.table_object_id),
			client_uuid: [0; 16],
			sequence_no: 1,
		};
		let meta = child_request_meta(
			&key,
			descriptor.table_object_id,
			descriptor.schema_generation,
			1,
			hlc,
		)
		.unwrap();
		let admission = validate_table_write_request_batch(
			&meta,
			batch,
			descriptor,
			super::child_writes::writer_of(hlc),
			hlc,
		)
		.unwrap();
		(admission.key_identities[0], admission.localities[0])
	}

	/// One table write attempt under a caller's deadline with nothing racing
	/// it: what the attempt's own claim points decide when the clock is past
	/// the deadline and no timer has run.
	pub(crate) async fn coordinate_until_for_test(
		&self,
		meta: TableWriteRequestMeta,
		batch: RecordBatch,
		deadline: std::time::Instant,
	) -> TableWriteResponseMeta {
		let settled = self
			.coordinate(
				ParentNamespace::Table(meta.table_object_id),
				&meta,
				&batch,
				&AttemptGate::until(Some(deadline)),
			)
			.await;
		self.settled_response(&meta, settled)
	}

	/// The node's table write, for in-crate cluster controls.
	pub(crate) async fn table_write_for_test(
		&self,
		meta: TableWriteRequestMeta,
		batch: RecordBatch,
	) -> std::io::Result<(TableWriteResponseMeta, Option<FinalizationMarker>)> {
		self.table_write(meta, batch).await
	}
}

#[cfg(test)]
#[path = "coordinator/tests.rs"]
mod tests;
