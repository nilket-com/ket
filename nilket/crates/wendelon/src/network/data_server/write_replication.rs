use super::*;
use crate::metrics::{LeaseGateWaitOutcome, LeaseGateWaitReason};

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub(super) enum WriteOutcome {
	DataBranchPending,
	DurableQuorumReached,
	FinalizationCommitted {
		response: WriteResponseMeta,
		trailing: Option<FinalizationMarker>,
	},
	FinalizationRecoverablePending {
		response: WriteResponseMeta,
		pending_finalization_id: PendingFinalizationId,
	},
	FinalizationTerminalFailure {
		response: WriteResponseMeta,
	},
	NoopCommitted {
		response: WriteResponseMeta,
	},
}

impl WriteOutcome {
	pub(super) fn is_cacheable(self) -> bool {
		matches!(
			self,
			Self::FinalizationCommitted { .. }
				| Self::FinalizationRecoverablePending { .. }
				| Self::FinalizationTerminalFailure { .. }
				| Self::NoopCommitted { .. }
		)
	}
}

#[derive(Clone, Copy, Debug)]
pub(super) struct DurableQuorumTracker {
	required_durable_acks: usize,
	durable_acks: usize,
	leader_local_durable: bool,
}

impl DurableQuorumTracker {
	pub(super) fn new(required_durable_acks: usize, leader_local_required: bool) -> Self {
		Self {
			required_durable_acks,
			durable_acks: 0,
			leader_local_durable: !leader_local_required,
		}
	}

	pub(super) fn note_local_durable(&mut self) {
		debug_assert!(
			!self.leader_local_durable,
			"leader-local durable leg counted more than once"
		);
		self.leader_local_durable = true;
		self.durable_acks += 1;
	}

	pub(super) fn note_remote_ack(&mut self, level: ReplicaAckLevel) {
		if level == ReplicaAckLevel::DURABLE {
			self.durable_acks += 1;
		}
	}

	#[cfg(test)]
	pub(super) fn durable_ack_count(&self) -> usize {
		self.durable_acks
	}

	pub(super) fn quorum_reached(&self) -> bool {
		self.durable_acks >= self.required_durable_acks
	}

	pub(super) fn is_satisfied(&self) -> bool {
		self.quorum_reached() && self.leader_local_durable
	}
}

#[derive(Clone, Default)]
pub(super) struct PendingFinalizationClaims {
	inflight: Arc<DashMap<PendingFinalizationId, ()>>,
}

impl PendingFinalizationClaims {
	pub(super) fn try_claim(&self, id: PendingFinalizationId) -> Option<PendingFinalizationClaim> {
		match self.inflight.entry(id) {
			DashEntry::Occupied(_) => None,
			DashEntry::Vacant(entry) => {
				entry.insert(());
				Some(PendingFinalizationClaim {
					inflight: self.inflight.clone(),
					id,
				})
			}
		}
	}
}

impl WendelonDataServer {
	pub(super) async fn handle_write(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let (meta, payload) = self.decode_write_request_frame(&frame, "WRITE_REQ")?;
		let span = tracing::info_span!(
			"data_ring.write.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			await_finalization = meta.await_finalization != 0,
			durability = meta.durability.0,
			write_sequence = meta.write_id.sequence_no,
		);
		self.emit_write_requested_audit(actor, &meta, frame.header.arrow_len as u64)?;
		let (resp, trailing) = match self.write(meta, payload).instrument(span).await {
			Ok(ok) => ok,
			Err(_) => self.counted_write_result(
				error_response(&meta, WriteStatus::INTERNAL_ERROR, self.local_node),
				None,
			),
		};
		self.emit_write_completion_audit(actor, &meta, &resp, frame.header.arrow_len as u64)?;
		Ok(self.build_write_response_frame(
			MsgType::WRITE_RESP,
			frame.header.stream_id,
			resp,
			trailing,
		))
	}

	pub(super) async fn handle_table_write(
		&self,
		_actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let (meta, payload) = self.decode_table_write_request_frame(&frame)?;
		let span = tracing::info_span!(
			"data_ring.table_write.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
			schema_generation = meta.schema_generation,
			row_count = meta.decoded_row_count(),
			await_finalization = meta.await_finalization != 0,
			durability = meta.durability.0,
			write_sequence = meta.parent_write_id.sequence_no,
		);
		let (resp, trailing) = self.table_write(meta, payload).instrument(span).await?;
		let resp = self.finalize_table_write_attempt(meta, resp).await?;
		Ok(self.build_table_write_response_frame(frame.header.stream_id, resp, trailing))
	}

	pub(super) async fn handle_table_delete(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let (meta, row_key_ids) = self.decode_table_delete_request_frame(&frame)?;
		let distinct_target_count = row_key_ids.iter().copied().collect::<HashSet<_>>().len();
		let span = tracing::info_span!(
			"data_ring.table_delete.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
			schema_generation = meta.schema_generation,
			row_count = meta.row_count,
			await_finalization = meta.await_finalization != 0,
			durability = meta.durability.0,
			write_sequence = meta.parent_write_id.sequence_no,
		);
		self.emit_table_delete_requested_audit(actor, &meta, distinct_target_count)?;
		let (resp, trailing) = self
			.table_delete(meta, row_key_ids)
			.instrument(span)
			.await?;
		let resp = self.finalize_table_delete_attempt(meta, resp).await?;
		self.emit_table_delete_completion_audit(actor, &meta, &resp, distinct_target_count)?;
		Ok(self.build_table_delete_response_frame(frame.header.stream_id, resp, trailing))
	}

	pub(super) async fn handle_read(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		if frame.meta.len() < std::mem::size_of::<ReadRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"READ_REQ meta too small",
			));
		}
		let meta: ReadRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReadRequestMeta>()]);
		let key_bytes = frame
			.arrow_payload
			.as_ref()
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "READ_REQ missing Arrow payload"))?;
		let key = crate::network::ipc::wire_decode_batch(key_bytes)?;
		validate_read_request_batch(&meta, &key)?;

		let span = tracing::info_span!(
			"data_ring.read.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			consistency = meta.consistency.0,
		);
		let (resp, value_batch) = self.read(meta, key).instrument(span).await?;
		self.emit_read_completion_audit(actor, &meta, &resp)?;
		let arrow_payload = value_batch.map(|batch| {
			let bytes = wire_encode_batch_for_path(&batch, NetworkCompressionPath::ReadResponse)?;
			let mut buf = self.pool.acquire();
			buf.as_mut_vec().extend_from_slice(&bytes);
			Ok::<_, std::io::Error>(buf)
		});
		let arrow_payload = match arrow_payload.transpose()? {
			Some(buf) => Some(buf),
			None => None,
		};
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));

		Ok(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::READ_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_payload
					.as_ref()
					.map(|buf| buf.len() as u32)
					.unwrap_or(0),
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload,
			trailing: None,
		})
	}

	/// Handle an inbound REPLICATE_REQ frame: decode the POD
	/// metadata, verify the payload hash, append to the local
	/// slab, and build the REPLICATE_RESP frame. Surfaces both
	/// transient and logical failures via the `status` byte rather
	/// than tearing down the HRFT stream — the sender's ack count
	/// is the only signal that matters for quorum.
	pub(super) async fn handle_replicate(&self, frame: HrftFrame) -> std::io::Result<HrftFrame> {
		// ADR 0699 S3b: never an ingress under ParentDecisions; child writes are.
		if self.decision_mode != crate::consensus::decision_mode::DecisionMode::Today {
			return Err(self.decision_mode.service_refusal());
		}
		let observation = self.slab.finality_observation_registry().begin(
			crate::storage::finality_observation::WorkScope::Node,
			crate::storage::finality_observation::WorkKind::Mutation,
		);
		crate::storage::finality_observation::with_work(observation, async {
			if frame.meta.len() < std::mem::size_of::<ReplicaWriteMeta>() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"REPLICATE_REQ meta too small",
				));
			}
			let meta: ReplicaWriteMeta =
				*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReplicaWriteMeta>()]);
			// Peer liveness/drift ingestion (ADR 001, ADR 012). The HLC
			// originator in `commit_hlc.node_id` is another Data Ring
			// member, so its physical_ms feeds directly into the
			// NodeTracker without further validation.
			if let Some(tracker) = &self.tracker {
				let peer = NodeId {
					id: meta.commit_hlc.node_id,
					_reserved: 0,
				};
				tracker.record_hlc(peer, meta.commit_hlc);
			}
			let arrow = frame.arrow_payload.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"REPLICATE_REQ missing arrow payload",
				)
			})?;

			let arrow_bytes: &[u8] = &arrow[..];
			let (status, ack_level) = if self.degraded_reason().is_some() {
				(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
			} else if payload_digest(arrow_bytes) != meta.payload_hash {
				(ReplicaWriteStatus::HASH_MISMATCH, ReplicaAckLevel::NONE)
			} else {
				match self
					.slab
					.append_durable(
						meta.tenant_id,
						meta.key_identity,
						meta.commit_hlc,
						arrow_bytes,
					)
					.await
				{
					Ok(_) => {
						let pending = self.pending_finalization_record_from_replica_meta(&meta);
						match self.pending_finalizations.upsert(pending).await {
							Ok(()) => {
								self.refresh_pending_finalization_backlog_metric();
								(ReplicaWriteStatus::OK, ReplicaAckLevel::DURABLE)
							}
							Err(err) => {
								tracing::error!(
									write_id = ?meta.write_id,
									tenant_id = meta.tenant_id.get(),
									error = %err,
									"replica failed to persist pending-finalization witness after durable append"
								);
								self.trip_local_persistence_distrust(format!(
									"replica pending finalization witness persistence failed after durable append: {err}"
								))
								.await;
								(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
							}
						}
					}
					Err(_) => (ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE),
				}
			};

			let resp = ReplicaWriteRespMeta {
				status,
				ack_level,
				_pad: [0; 6],
			};
			let mut body = self.pool.acquire();
			body.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(&resp));
			let header = FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::REPLICATE_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: body.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			};
			Ok(HrftFrame {
				header,
				meta: body,
				arrow_payload: None,
				trailing: None,
			})
		})
		.await
	}

	pub(super) async fn handle_table_replicate(
		&self,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		// ADR 0699 S3b: never an ingress under ParentDecisions; child writes are.
		if self.decision_mode != crate::consensus::decision_mode::DecisionMode::Today {
			return Err(self.decision_mode.service_refusal());
		}
		let observation = self.slab.finality_observation_registry().begin(
			crate::storage::finality_observation::WorkScope::Node,
			crate::storage::finality_observation::WorkKind::Mutation,
		);
		crate::storage::finality_observation::with_work(observation, async {

		if frame.meta.len() < std::mem::size_of::<TableReplicaWriteMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_REPLICATE_REQ meta too small",
			));
		}
		let meta: TableReplicaWriteMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableReplicaWriteMeta>()]);
		if let Some(tracker) = &self.tracker {
			let peer = NodeId {
				id: meta.commit_hlc.node_id,
				_reserved: 0,
			};
			tracker.record_hlc(peer, meta.commit_hlc);
		}
		let arrow = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"TABLE_REPLICATE_REQ missing arrow payload",
			)
		})?;

		let (status, ack_level) = if self.degraded_reason().is_some() {
			(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
		} else {
			let _secondary_index_admission =
				self.slab.acquire_secondary_index_admission_guard().await;
			let (table_descriptor, secondary_index_descriptors) = {
				let state = self.state_machine.state_snapshot().await;
				let descriptor = state
					.table_schemas
					.latest_by_id(meta.tenant_id, meta.table_object_id)
					.cloned()
					.or_else(|| {
						let saga = state
							.ctas_sagas
							.operation_for_object(meta.tenant_id, meta.table_object_id)?;
						saga.contains_row_write_id(meta.parent_write_id)
							.then(|| saga.descriptor.clone())
					});
				let indexes = descriptor
					.as_ref()
					.map(|descriptor| {
						Self::secondary_index_admission_descriptors(
							&state.secondary_indexes,
							meta.tenant_id,
							descriptor,
						)
					})
					.unwrap_or_default();
				(descriptor, indexes)
			};
			// Replica wire v1 does not carry a schema generation. The registry's
			// closure is object-wide and drains the sum of every generation, so a
			// descriptor-less follower may safely account this under generation zero
			// without weakening the DROP barrier.
			let mutation_drain = self.mutation_drain.admit(
				meta.tenant_id,
				meta.table_object_id,
				table_descriptor
					.as_ref()
					.map(|descriptor| descriptor.schema_generation)
					.unwrap_or(0),
			);
			if mutation_drain.is_err() {
				(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
			} else {
				let _mutation_drain = mutation_drain.expect("checked above");
				match decode_table_replica_children(&arrow[..]).and_then(|children| {
					validate_table_replica_payload(&meta, &children).map(|()| children)
				}) {
					Ok(children) => {
						let append_result = append_table_children_durable(
							&self.slab,
							meta.tenant_id,
							meta.commit_hlc,
							&children,
							&secondary_index_descriptors,
						)
						.await;
						match append_result {
							Ok(()) => {
								let cdc_staging_ok = if let Some(descriptor) =
									table_descriptor.as_ref()
								{
									match self
										.stage_cdc_table_replica_children(
											meta.tenant_id,
											meta.table_object_id,
											descriptor,
											meta.commit_hlc,
											&children,
											false,
										)
										.await
									{
										Ok(staged) => {
											self.cdc_stage_pending_finalization_marker(
												CdcFinalizationKey::from_table_replica_meta(&meta),
												staged,
											);
											true
										}
										Err(err) => {
											tracing::error!(
												write_id = ?meta.parent_write_id,
												tenant_id = meta.tenant_id.get(),
												table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
												error = %err,
												"replica failed to stage CDC rows after durable table append"
											);
											self.trip_local_persistence_distrust(format!(
												"replica CDC staging failed after durable table append: {err}"
											))
											.await;
											false
										}
									}
								} else {
									tracing::warn!(
										write_id = ?meta.parent_write_id,
										tenant_id = meta.tenant_id.get(),
										table_object_id = %format_bytes_hex(meta.table_object_id.as_bytes()),
										"replica skipped CDC staging because table descriptor is unavailable"
									);
									true
								};
								// A split table-replication frame is only proof for its child
								// group; the aggregate pending witness belongs to the coordinator
								// unless this frame carries the whole parent batch.
								if !cdc_staging_ok {
									(ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE)
								} else if meta.group_record_count == meta.record_count
									&& meta.group_payload_hash == meta.payload_hash
								{
									let pending = self
										.pending_finalization_record_from_table_replica_meta(&meta);
									match self.pending_finalizations.upsert(pending).await {
										Ok(()) => {
											self.refresh_pending_finalization_backlog_metric();
											(ReplicaWriteStatus::OK, ReplicaAckLevel::DURABLE)
										}
										Err(err) => {
											tracing::error!(
												write_id = ?meta.parent_write_id,
												tenant_id = meta.tenant_id.get(),
												error = %err,
												"replica failed to persist table pending-finalization witness after durable append"
											);
											self.trip_local_persistence_distrust(format!(
											"table replica pending finalization witness persistence failed after durable append: {err}"
										))
										.await;
											(
												ReplicaWriteStatus::INTERNAL_ERROR,
												ReplicaAckLevel::NONE,
											)
										}
									}
								} else {
									(ReplicaWriteStatus::OK, ReplicaAckLevel::DURABLE)
								}
							}
							Err(_) => (ReplicaWriteStatus::INTERNAL_ERROR, ReplicaAckLevel::NONE),
						}
					}
					Err(err) => {
						tracing::warn!(
							write_id = ?meta.parent_write_id,
							tenant_id = meta.tenant_id.get(),
							error = %err,
							"replica rejected malformed table batch"
						);
						(ReplicaWriteStatus::HASH_MISMATCH, ReplicaAckLevel::NONE)
					}
				}
			}
		};

		let resp = ReplicaWriteRespMeta {
			status,
			ack_level,
			_pad: [0; 6],
		};
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_REPLICATE_RESP.0,
			flags: 0,
			stream_id: frame.header.stream_id,
			meta_len: body.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		Ok(HrftFrame {
			header,
			meta: body,
			arrow_payload: None,
			trailing: None,
		})

		}).await
	}
}

#[cfg(test)]
pub(super) async fn append_bifurcated_table_children_durable(
	slab: &SlabManager,
	tenant_id: TenantId,
	commit_hlc: HybridTimestamp,
	children: &[BifurcatedTableChildWrite],
	secondary_index_descriptors: &[SecondaryIndexDescriptor],
) -> std::io::Result<()> {
	let mut wrote_a_child = false;
	append_table_children_tracking_effects(
		slab,
		tenant_id,
		commit_hlc,
		children,
		secondary_index_descriptors,
		&mut wrote_a_child,
	)
	.await
}

/// How long one append waits out a refusal that clears by itself. Routine tier retirement raises a
/// store-wide fence for a moment, so a write that meets it should wait, not fail. Bounded so that a
/// fence which does not clear, or which is waiting on this very write, still ends in its answer.
#[cfg(not(test))]
const APPEND_REFUSAL_WAIT: Duration = Duration::from_secs(5);
#[cfg(test)]
const APPEND_REFUSAL_WAIT: Duration = Duration::from_millis(1_000);
/// A missed wake-up costs this much, and no more.
const APPEND_REFUSAL_RECHECK: Duration = Duration::from_millis(50);

/// Attempt one append until admitted or out of time. A refused attempt has done nothing, so trying
/// the same append again is the same append: nothing is partly applied, and the client sees no
/// retry. Any other outcome returns at once. The last refusal is returned as it came.
pub(super) async fn append_waiting_out_a_refusal<T, F, Fut>(
	slab: &SlabManager,
	mut append: F,
) -> std::io::Result<T>
where
	F: FnMut() -> Fut,
	Fut: std::future::Future<Output = std::io::Result<T>>,
{
	let deadline = tokio::time::Instant::now() + APPEND_REFUSAL_WAIT;
	loop {
		let attempt = append().await;
		let refused = attempt
			.as_ref()
			.err()
			.is_some_and(crate::storage::manager::tier_authority::append_refused_before_effects);
		let left = deadline.saturating_duration_since(tokio::time::Instant::now());
		if !refused || left.is_zero() {
			return attempt;
		}
		slab.tier_authority_change_within(left.min(APPEND_REFUSAL_RECHECK))
			.await;
	}
}

/// One proof-carrying refusal must not outlive its proof: once an earlier child of this mutation
/// is durable, a later child's refusal no longer shows the mutation did nothing here, so it is
/// handed on as a plain failure, on the conservative path, until a retry of a partly applied
/// mutation is shown safe.
pub(super) async fn append_table_children_tracking_effects(
	slab: &SlabManager,
	tenant_id: TenantId,
	commit_hlc: HybridTimestamp,
	children: &[BifurcatedTableChildWrite],
	secondary_index_descriptors: &[SecondaryIndexDescriptor],
	wrote_a_child: &mut bool,
) -> std::io::Result<()> {
	for child in children {
		let earlier_child = *wrote_a_child;
		let proof = move |error: std::io::Error| {
			if earlier_child {
				crate::storage::manager::tier_authority::without_before_effects_proof(error)
			} else {
				error
			}
		};
		let decoded = decode_batch(child.frame_bytes.as_slice())?;
		if has_storage_synthetic_columns(&decoded) {
			let entries = storage_index_entries_from_batch(&decoded)?;
			let base_location = append_waiting_out_a_refusal(slab, || {
				slab.append_durable_multirow_with_locality(
					tenant_id,
					child.frame_bytes.as_slice(),
					&entries,
				)
			})
			.await
			.map_err(proof)?;
			*wrote_a_child = true;
			append_secondary_index_active_buffers_for_storage_batch(
				slab,
				tenant_id,
				secondary_index_descriptors,
				&decoded,
				base_location,
			)
			.await;
			metrics().observe_storage_multirow_record_written(entries.len() as u64);
		} else {
			let location = append_waiting_out_a_refusal(slab, || {
				slab.append_durable_with_locality(
					tenant_id,
					child.key_identity,
					child.locality,
					commit_hlc,
					child.frame_bytes.as_slice(),
				)
			})
			.await
			.map_err(proof)?;
			*wrote_a_child = true;
			append_secondary_index_active_buffers_for_single_row(
				slab,
				tenant_id,
				secondary_index_descriptors,
				&decoded,
				child.key_identity,
				commit_hlc,
				location,
			)
			.await;
		}
	}
	Ok(())
}

/// How long a Today table write or delete waits at its lease gate for a lease that is temporarily
/// unavailable. A fixed cap, not a guarantee that a renewal arrives inside it.
pub(super) const LEASE_GATE_WAIT: Duration = Duration::from_secs(2);

/// How a wait at the lease gate ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LeaseGateWaitEnd {
	/// The gate answered `Valid` before the deadline. This admits nothing by itself.
	Recovered,
	/// The deadline came first, in a sleep or in an evaluation.
	AtDeadline,
	/// The gate answered `InvalidSignature`.
	InvalidLease,
	/// The leader watch closed: no change of leader can be seen any more.
	WatchClosed,
}

/// The one absolute deadline of a wait at the lease gate: the cap from now, or the write's own
/// clocks where they end sooner. Nothing renews it.
pub(super) fn lease_gate_deadline(cap: Duration, clocks: WriteClocks) -> tokio::time::Instant {
	let (clocks_remaining, _) = clocks.next_expiry();
	tokio::time::Instant::now() + cap.min(clocks_remaining)
}

/// Completes with `true` once the watched leader differs from `before`, and with `false` when the
/// watch has closed.
async fn lease_gate_leader_changed<M>(
	leader: &mut watch::Receiver<M>,
	leader_of: &impl Fn(&M) -> Option<u32>,
	before: Option<u32>,
) -> bool {
	loop {
		if leader.changed().await.is_err() {
			return false;
		}
		if leader_of(&leader.borrow()) != before {
			return true;
		}
	}
}

/// Re-evaluate the lease gate each time a lease is cached or the leader changes, until it stops
/// answering `Missing` or `deadline` comes. The deadline covers every evaluation as well as every
/// sleep, and a result that arrives at the deadline does not win.
///
/// Both wake sources are marked seen BEFORE each evaluation, so a grant or a change of leader that
/// lands after the gate was read and before the sleep begins ends that sleep at once.
///
/// The leader watch carries every Raft metric. Only the leader's identity is compared: a metric
/// that moves with the leader unchanged neither ends a sleep nor causes an evaluation. A leader
/// that leaves and returns to the same identity wholly inside one evaluation is therefore not
/// seen as a change; that wait ends at its deadline, in the refusal.
pub(super) async fn wait_for_lease_gate<M, E, Fut>(
	grants: &mut watch::Receiver<u64>,
	leader: &mut watch::Receiver<M>,
	leader_of: impl Fn(&M) -> Option<u32>,
	deadline: tokio::time::Instant,
	mut evaluate: E,
	#[cfg(test)] hooks: &LeaseGateWaitHooks,
) -> LeaseGateWaitEnd
where
	E: FnMut() -> Fut,
	Fut: std::future::Future<Output = LeaseGateResult>,
{
	#[cfg(test)]
	let mut forced_leader = hooks.forced_leader.subscribe();
	loop {
		grants.borrow_and_update();
		let leader_before = leader_of(&leader.borrow_and_update());
		#[cfg(test)]
		forced_leader.borrow_and_update();
		match tokio::time::timeout_at(deadline, evaluate()).await {
			Err(_) => return LeaseGateWaitEnd::AtDeadline,
			Ok(_) if tokio::time::Instant::now() >= deadline => {
				return LeaseGateWaitEnd::AtDeadline;
			}
			Ok(LeaseGateResult::Valid) => return LeaseGateWaitEnd::Recovered,
			Ok(LeaseGateResult::InvalidSignature) => return LeaseGateWaitEnd::InvalidLease,
			Ok(LeaseGateResult::Missing(_)) => {}
		}
		if leader.has_changed().is_err() {
			return LeaseGateWaitEnd::WatchClosed;
		}
		// Consume whatever the watch published while the gate was being read. The gate is read
		// again only if the leader itself is now another; the sleep below judges every later
		// value against the same `leader_before`.
		if leader_of(&leader.borrow_and_update()) != leader_before {
			continue;
		}
		#[cfg(test)]
		hooks.reached_sleep().await;
		let leader_changed = lease_gate_leader_changed(leader, &leader_of, leader_before);
		#[cfg(test)]
		let leader_changed = async {
			tokio::select! {
				changed = leader_changed => changed,
				forced = forced_leader.changed() => forced.is_ok(),
			}
		};
		let woke = tokio::time::timeout_at(deadline, async {
			tokio::select! {
				// The sender is the server's own field: it cannot close under a waiter.
				granted = grants.changed() => granted.is_ok(),
				changed = leader_changed => changed,
			}
		})
		.await;
		match woke {
			Err(_) => return LeaseGateWaitEnd::AtDeadline,
			// A closed watch refuses. It is never armed again, so it cannot spin.
			Ok(false) => return LeaseGateWaitEnd::WatchClosed,
			Ok(true) => {}
		}
	}
}

type LeaseGateAnswer = (WriteResponseMeta, Option<FinalizationMarker>);

impl WendelonDataServer {
	/// The leader the lease gate judges a lease's issuer by.
	async fn lease_gate_leader(&self) -> Option<u32> {
		#[cfg(test)]
		if let Some(forced) = *self.lease_gate_wait_hooks.forced_leader.borrow() {
			return forced;
		}
		self.raft.current_leader().await
	}

	pub(super) async fn lease_gate_result(
		&self,
		locality: Locality,
		epoch: u64,
		now: HybridTimestamp,
	) -> LeaseGateResult {
		let lease = {
			let guard = recover_rwlock_read(&self.leases, "lease_cache");
			let covers = |l: &&LeaseDescriptor| {
				l.holder.id == self.local_node.id
					&& l.epoch == epoch
					&& l.range_start <= locality.get()
					&& locality.get() <= l.range_end
			};
			// Selection is unchanged: any covering entry that has not expired is taken, wherever
			// it sits. Only when there is none is the miss named, and an expired covering entry
			// names it `Expired`.
			match guard
				.iter()
				.find(|l| covers(l) && now < l.lease_expiry)
				.copied()
			{
				Some(lease) => lease,
				None if guard.iter().any(|l| covers(&l)) => {
					return LeaseGateResult::Missing(LeaseGateWaitReason::Expired);
				}
				None => return LeaseGateResult::Missing(LeaseGateWaitReason::NoLease),
			}
		};

		let Some(current_leader) = self.lease_gate_leader().await else {
			return LeaseGateResult::Missing(LeaseGateWaitReason::NoLeader);
		};
		if lease.issuer.id != current_leader {
			return LeaseGateResult::InvalidSignature;
		}
		match self.lease_authority.verify_lease(&lease) {
			Ok(()) => LeaseGateResult::Valid,
			Err(err) => {
				tracing::error!(
					holder = lease.holder.id,
					issuer = lease.issuer.id,
					epoch = lease.epoch,
					range_start = lease.range_start,
					range_end = lease.range_end,
					error = %err,
					"lease signature verification failed at data-ring gate"
				);
				LeaseGateResult::InvalidSignature
			}
		}
	}

	/// The gate for every child of one table write, in order: the first answer that is not `Valid`,
	/// else `Valid`.
	async fn table_children_lease_gate(
		&self,
		children: &[BifurcatedTableChildWrite],
		epoch: u64,
		now: HybridTimestamp,
	) -> LeaseGateResult {
		let mut result = LeaseGateResult::Valid;
		#[cfg(test)]
		let mut valid_children = 0usize;
		for child in children {
			result = self.lease_gate_result(child.locality, epoch, now).await;
			if result != LeaseGateResult::Valid {
				break;
			}
			#[cfg(test)]
			{
				valid_children += 1;
			}
		}
		#[cfg(test)]
		self.lease_gate_wait_hooks
			.gate_passes
			.lock()
			.unwrap()
			.push(valid_children);
		result
	}

	fn lease_gate_wait_cap(&self) -> Duration {
		#[cfg(test)]
		if let Some(cap) = *self.lease_gate_wait_hooks.cap.lock().unwrap() {
			return cap;
		}
		LEASE_GATE_WAIT
	}

	fn lease_gate_refusal(
		&self,
		synthetic: &WriteRequestMeta,
		status: WriteStatus,
	) -> LeaseGateAnswer {
		self.counted_write_result(error_response(synthetic, status, self.local_node), None)
	}

	/// The write's own clocks ended it before any durable work: the outcome of the pre-durable
	/// clock check in `write_table_bifurcated`, recorded the same way.
	async fn lease_gate_clocks_expired(
		&self,
		meta: &TableWriteRequestMeta,
		synthetic: &WriteRequestMeta,
		expiry: super::write_clocks::WriteExpiry,
		delete_counts: Option<[u32; 3]>,
	) -> std::io::Result<LeaseGateAnswer> {
		let response = mutation_expiry_response(
			synthetic,
			expiry,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		);
		self.record_mutation_outcome(
			meta.parent_write_id,
			meta.tenant_id,
			response,
			None,
			delete_counts,
		)
		.await?;
		Ok((response, None))
	}

	/// The pending-finalization replay of `write_table_bifurcated`, for a write that waited.
	async fn lease_gate_pending_replay(
		&self,
		meta: &TableWriteRequestMeta,
	) -> std::io::Result<Option<LeaseGateAnswer>> {
		let Some(record) = self
			.pending_finalizations
			.find_by_write_id(meta.tenant_id, meta.parent_write_id)
			.await?
		else {
			return Ok(None);
		};
		let outcome = self.recoverable_pending_outcome_for_record(&record);
		self.idempotency.insert(
			Self::idempotency_key_for(meta.tenant_id, meta.parent_write_id),
			CachedWriteOutcome::new(outcome),
		);
		self.replay_write_outcome(meta.tenant_id, outcome)
			.await
			.map(Some)
	}

	/// The one exit of every write that waited at the lease gate and is not admitted. `status` and
	/// `outcome` are the refusal the caller arrived at; what is answered is decided here, in this
	/// order, so that no exit can skip a step:
	///
	/// 1. an outcome recorded for this write, or a pending finalization of it, is replayed. One
	///    that appeared while the attempt waited is returned as it stands and nothing is recorded
	///    over it;
	/// 2. if the write's own clocks have expired, theirs is the outcome, at a tie with the cap
	///    too, recorded as the pre-durable clock check records it;
	/// 3. otherwise the refusal stands.
	async fn lease_gate_exit_unadmitted(
		&self,
		meta: &TableWriteRequestMeta,
		synthetic: &WriteRequestMeta,
		clocks: WriteClocks,
		delete_counts: Option<[u32; 3]>,
		status: WriteStatus,
		outcome: LeaseGateWaitOutcome,
	) -> std::io::Result<(LeaseGateWaitOutcome, Option<LeaseGateAnswer>)> {
		if let Some(replayed) = self.durable_mutation_replay(meta).await? {
			return Ok((LeaseGateWaitOutcome::Replayed, Some(replayed)));
		}
		if let Some(replayed) = self.lease_gate_pending_replay(meta).await? {
			return Ok((LeaseGateWaitOutcome::Replayed, Some(replayed)));
		}
		if let Some(expiry) = clocks.expired() {
			return Ok((
				LeaseGateWaitOutcome::ClocksExpired,
				Some(
					self.lease_gate_clocks_expired(meta, synthetic, expiry, delete_counts)
						.await?,
				),
			));
		}
		Ok((outcome, Some(self.lease_gate_refusal(synthetic, status))))
	}

	/// The lease gate of a Today table write or delete. `None` admits; `Some` is the answer to
	/// return.
	///
	/// With a lease and a leader in place this is one pass over the children and nothing else. On
	/// `Missing` the write waits, bounded by one absolute deadline, for a lease to be cached or the
	/// leader to change. The wait starts no renewal, and the caller's `clocks` are read, never
	/// advanced or rebuilt. A gate that answers `Valid` again admits only through
	/// `readmit_after_lease_gate_wait`.
	pub(super) async fn admit_at_table_lease_gate(
		&self,
		meta: &TableWriteRequestMeta,
		synthetic: &WriteRequestMeta,
		children: &[BifurcatedTableChildWrite],
		clocks: WriteClocks,
		delete_counts: Option<[u32; 3]>,
	) -> std::io::Result<Option<LeaseGateAnswer>> {
		let reason = match self
			.table_children_lease_gate(children, meta.expected_epoch, meta.client_hlc)
			.await
		{
			LeaseGateResult::Valid => return Ok(None),
			LeaseGateResult::InvalidSignature => {
				return Ok(Some(self.lease_gate_refusal(
					synthetic,
					WriteStatus::INVALID_LEASE_SIGNATURE,
				)));
			}
			LeaseGateResult::Missing(reason) => reason,
		};
		let deadline = lease_gate_deadline(self.lease_gate_wait_cap(), clocks);
		let end = wait_for_lease_gate(
			&mut self.lease_grants.subscribe(),
			&mut self.raft.metrics(),
			|raft| raft.current_leader,
			deadline,
			|| self.table_children_lease_gate(children, meta.expected_epoch, meta.client_hlc),
			#[cfg(test)]
			&self.lease_gate_wait_hooks,
		)
		.await;
		let unadmitted = |status, outcome| {
			self.lease_gate_exit_unadmitted(meta, synthetic, clocks, delete_counts, status, outcome)
		};
		let ended = match end {
			LeaseGateWaitEnd::Recovered => {
				self.readmit_after_lease_gate_wait(
					meta,
					synthetic,
					children,
					clocks,
					delete_counts,
					deadline,
				)
				.await
			}
			LeaseGateWaitEnd::AtDeadline => {
				unadmitted(
					WriteStatus::LEASE_EXPIRED,
					LeaseGateWaitOutcome::RefusedAtCap,
				)
				.await
			}
			LeaseGateWaitEnd::InvalidLease => {
				unadmitted(
					WriteStatus::INVALID_LEASE_SIGNATURE,
					LeaseGateWaitOutcome::RefusedInvalidLease,
				)
				.await
			}
			LeaseGateWaitEnd::WatchClosed => {
				unadmitted(
					WriteStatus::LEASE_EXPIRED,
					LeaseGateWaitOutcome::WatchClosed,
				)
				.await
			}
		};
		// One count for the whole wait, whatever the number of wakes. A recheck that failed with
		// an error admitted nothing either.
		let outcome = match &ended {
			Ok((outcome, _)) => *outcome,
			Err(_) => LeaseGateWaitOutcome::RefusedOnRecheck,
		};
		metrics().observe_lease_gate_wait(reason, outcome);
		#[cfg(test)]
		self.lease_gate_wait_hooks
			.completed
			.lock()
			.unwrap()
			.push((reason, outcome));
		ended.map(|(_, answer)| answer)
	}

	/// A wait at the lease gate ended with a valid lease. Everything `write_table_bifurcated`
	/// established before the wait is now stale, so it is established again, in this order, and
	/// nothing here waits for a lease a second time:
	///
	/// 1. a recorded outcome is replayed first: one recorded while this attempt waited is returned,
	///    never replaced by a refusal computed here;
	/// 2. the admission epoch is read again: a request epoch behind it is EPOCH_STALE;
	/// 3. the replica groups are computed again from a fresh migrations snapshot and the router,
	///    and this node must be primary for all of them;
	/// 4. the orphan-witness barrier and the pending-finalization replay, as before the wait;
	/// 5. every child's gate once more, under the wait's own deadline: whatever that pass
	///    answers, a deadline that has come is judged first;
	/// 6. the caller's clocks, last, before any durable work.
	///
	/// Every step that does not admit leaves through `lease_gate_exit_unadmitted`, so each refusal
	/// here is preceded by the replays and by the caller's clocks.
	pub(super) async fn readmit_after_lease_gate_wait(
		&self,
		meta: &TableWriteRequestMeta,
		synthetic: &WriteRequestMeta,
		children: &[BifurcatedTableChildWrite],
		clocks: WriteClocks,
		delete_counts: Option<[u32; 3]>,
		deadline: tokio::time::Instant,
	) -> std::io::Result<(LeaseGateWaitOutcome, Option<LeaseGateAnswer>)> {
		let unadmitted = |status, outcome| {
			self.lease_gate_exit_unadmitted(meta, synthetic, clocks, delete_counts, status, outcome)
		};
		let refused_on_recheck =
			|status| unadmitted(status, LeaseGateWaitOutcome::RefusedOnRecheck);
		if let Some(replayed) = self.durable_mutation_replay(meta).await? {
			return Ok((LeaseGateWaitOutcome::Replayed, Some(replayed)));
		}
		if meta.expected_epoch < self.admission_snapshot().current_epoch {
			return refused_on_recheck(WriteStatus::EPOCH_STALE).await;
		}
		let migrations = self.state_machine.state_snapshot().await.migrations.clone();
		let Some(groups) = self.table_batch_replica_groups(children, &migrations) else {
			return refused_on_recheck(WriteStatus::INTERNAL_ERROR).await;
		};
		let primary_for_all = groups.iter().all(|group| {
			group
				.replicas
				.first()
				.is_some_and(|primary| primary.id == self.local_node.id)
		});
		if !primary_for_all {
			return refused_on_recheck(WriteStatus::NOT_LEADER).await;
		}
		if !self
			.ensure_orphan_witnesses_recovered_for_leadership()
			.await?
		{
			return refused_on_recheck(WriteStatus::NOT_LEADER).await;
		}
		if let Some(replayed) = self.lease_gate_pending_replay(meta).await? {
			return Ok((LeaseGateWaitOutcome::Replayed, Some(replayed)));
		}
		// This pass, not the wake, is what admits. The deadline is judged before the result,
		// whatever the result: an answer that is only ready at or after it is the deadline's.
		let final_pass = tokio::time::timeout_at(
			deadline,
			self.table_children_lease_gate(children, meta.expected_epoch, meta.client_hlc),
		)
		.await;
		let at_deadline = tokio::time::Instant::now() >= deadline;
		match final_pass {
			Ok(LeaseGateResult::Valid) if !at_deadline => {}
			Ok(LeaseGateResult::Missing(_)) if !at_deadline => {
				return refused_on_recheck(WriteStatus::LEASE_EXPIRED).await;
			}
			Ok(LeaseGateResult::InvalidSignature) if !at_deadline => {
				return unadmitted(
					WriteStatus::INVALID_LEASE_SIGNATURE,
					LeaseGateWaitOutcome::RefusedInvalidLease,
				)
				.await;
			}
			Ok(_) | Err(_) => {
				return unadmitted(
					WriteStatus::LEASE_EXPIRED,
					LeaseGateWaitOutcome::RefusedAtCap,
				)
				.await;
			}
		}
		if clocks.expired().is_some() {
			// The exit finds the same expiry: these clocks are never advanced.
			return unadmitted(
				WriteStatus::LEASE_EXPIRED,
				LeaseGateWaitOutcome::RefusedAtCap,
			)
			.await;
		}
		Ok((LeaseGateWaitOutcome::Admitted, None))
	}
}

/// What a test may set and observe of a wait at the lease gate.
#[cfg(test)]
pub(super) struct LeaseGateWaitHooks {
	/// Replaces `LEASE_GATE_WAIT` for this server.
	pub(super) cap: StdMutex<Option<Duration>>,
	/// How many times a waiter has reached its sleep with both wake sources armed.
	pub(super) installed: watch::Sender<u64>,
	/// Holds a waiter between its evaluation and its sleep until notified.
	pub(super) hold_before_sleep: StdMutex<Option<Arc<Notify>>>,
	/// The leader the gate observes instead of Raft's, when set. A change also wakes a waiter:
	/// this drives the leader branch only, and stands in for no other authority.
	pub(super) forced_leader: watch::Sender<Option<Option<u32>>>,
	/// How many children answered `Valid` in each pass of the table gate, in order.
	pub(super) gate_passes: StdMutex<Vec<usize>>,
	/// Every completed wait: the reason seen at entry and the outcome.
	pub(super) completed: StdMutex<Vec<(LeaseGateWaitReason, LeaseGateWaitOutcome)>>,
}

#[cfg(test)]
impl Default for LeaseGateWaitHooks {
	fn default() -> Self {
		Self {
			cap: StdMutex::new(None),
			installed: watch::Sender::new(0),
			hold_before_sleep: StdMutex::new(None),
			forced_leader: watch::Sender::new(None),
			gate_passes: StdMutex::new(Vec::new()),
			completed: StdMutex::new(Vec::new()),
		}
	}
}

#[cfg(test)]
impl LeaseGateWaitHooks {
	async fn reached_sleep(&self) {
		self.installed.send_modify(|count| *count += 1);
		let hold = self.hold_before_sleep.lock().unwrap().clone();
		if let Some(release) = hold {
			release.notified().await;
		}
	}
}
