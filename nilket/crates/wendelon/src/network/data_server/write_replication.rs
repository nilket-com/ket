use super::*;

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
