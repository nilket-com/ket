//! ADR 542 durable mutation replay, resolution, and response framing.

use super::*;

impl WendelonDataServer {
	pub(super) async fn handle_mutation_outcome_pin_request(
		&self,
		actor: &ActorIdentity,
		frame: &HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		if frame.meta.len() != std::mem::size_of::<MutationOutcomePinRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"MUTATION_OUTCOME_PIN_REQ meta width mismatch",
			));
		}
		let meta: MutationOutcomePinRequestMeta = *bytemuck::from_bytes(&frame.meta);
		self.authorize_actor_for_data(actor, Some(meta.tenant_id), DataOperation::Write)?;
		let status = if meta.action == MutationOutcomePinAction::PIN {
			match self
				.mutation_outcomes
				.pin(meta.operation_id, meta.tenant_id)
				.await
			{
				Ok(()) => MutationOutcomePinStatus::APPLIED,
				Err(error) if error.kind() == ErrorKind::WouldBlock => {
					MutationOutcomePinStatus::CAPACITY_EXHAUSTED
				}
				Err(error) => return Err(error),
			}
		} else if meta.action == MutationOutcomePinAction::ACKNOWLEDGE {
			self.mutation_outcomes
				.acknowledge_pin(meta.operation_id, meta.tenant_id)
				.await?;
			MutationOutcomePinStatus::APPLIED
		} else {
			MutationOutcomePinStatus::INVALID_ACTION
		};
		Ok(Some(self.build_mutation_outcome_pin_response_frame(
			frame.header.stream_id,
			MutationOutcomePinResponseMeta {
				status,
				_pad: [0; 7],
			},
		)))
	}

	/// Acquire the node-wide mutation slot before table admission or batch construction.
	/// A queue expiry precedes every durability action, so its commit knowledge is exact.
	pub(super) async fn acquire_table_mutation_permit(
		&self,
		meta: &TableWriteRequestMeta,
		clocks: WriteClocks,
	) -> std::io::Result<
		Result<OwnedSemaphorePermit, (TableWriteResponseMeta, Option<FinalizationMarker>)>,
	> {
		match acquire_mutation_work_permit(self.mutation_work_slots.clone(), clocks).await? {
			Ok(permit) => Ok(Ok(permit)),
			Err(expiry) => {
				let synthetic = table_write_synthetic_meta(meta, Identity::ZERO, 0);
				let write = mutation_expiry_response(
					&synthetic,
					expiry,
					MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
				);
				self.record_mutation_outcome(
					meta.parent_write_id,
					meta.tenant_id,
					write,
					None,
					None,
				)
				.await?;
				Ok(Err((
					table_write_response(write, PbcRejectCode::NONE),
					None,
				)))
			}
		}
	}

	pub(super) async fn acquire_delete_mutation_permit(
		&self,
		meta: &TableDeleteRowsRequestMeta,
		clocks: WriteClocks,
	) -> std::io::Result<
		Result<OwnedSemaphorePermit, (TableDeleteRowsResponseMeta, Option<FinalizationMarker>)>,
	> {
		match acquire_mutation_work_permit(self.mutation_work_slots.clone(), clocks).await? {
			Ok(permit) => Ok(Ok(permit)),
			Err(expiry) => {
				let synthetic = table_delete_synthetic_write_meta(meta, Identity::ZERO, 0);
				let write = mutation_expiry_response(
					&synthetic,
					expiry,
					MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
				);
				self.record_mutation_outcome(
					meta.parent_write_id,
					meta.tenant_id,
					write,
					None,
					Some([0; 3]),
				)
				.await?;
				Ok(Err((
					table_delete_rows_response(write, 0, 0, 0, PbcRejectCode::NONE),
					None,
				)))
			}
		}
	}

	pub(super) async fn finalize_table_write_attempt(
		&self,
		meta: TableWriteRequestMeta,
		mut response: TableWriteResponseMeta,
	) -> std::io::Result<TableWriteResponseMeta> {
		if meta.attempts_remaining() != 1 || !retryable_mutation_status(response.write.status) {
			return Ok(response);
		}
		let knowledge = if response.write.status == WriteStatus::FINALIZATION_PENDING {
			MutationCommitKnowledge::INDETERMINATE
		} else {
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED
		};
		response.write.status = WriteStatus::WRITE_ATTEMPTS_EXHAUSTED;
		response.write = response
			.write
			.with_mutation_outcome(knowledge, MutationStopReason::ATTEMPT_EXHAUSTION);
		self.record_mutation_outcome(
			meta.parent_write_id,
			meta.tenant_id,
			response.write,
			None,
			None,
		)
		.await?;
		Ok(response)
	}

	pub(super) async fn finalize_table_delete_attempt(
		&self,
		meta: TableDeleteRowsRequestMeta,
		mut response: TableDeleteRowsResponseMeta,
	) -> std::io::Result<TableDeleteRowsResponseMeta> {
		if meta.attempts_remaining() != 1 || !retryable_mutation_status(response.write.status) {
			return Ok(response);
		}
		let knowledge = if response.write.status == WriteStatus::FINALIZATION_PENDING {
			MutationCommitKnowledge::INDETERMINATE
		} else {
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED
		};
		response.write.status = WriteStatus::WRITE_ATTEMPTS_EXHAUSTED;
		response.write = response
			.write
			.with_mutation_outcome(knowledge, MutationStopReason::ATTEMPT_EXHAUSTION);
		self.record_mutation_outcome(
			meta.parent_write_id,
			meta.tenant_id,
			response.write,
			None,
			Some([
				response.deleted_count,
				response.already_deleted_count,
				response.not_found_count,
			]),
		)
		.await?;
		Ok(response)
	}

	pub(super) async fn complete_pending_finalization(
		&self,
		record: &PendingFinalizationRecord,
		resolution: PendingFinalizationResolution,
		delete_counts_hint: Option<[u32; 3]>,
	) -> std::io::Result<(WriteResponseMeta, Option<FinalizationMarker>)> {
		let marker = match resolution {
			PendingFinalizationResolution::CommittedNow(marker) => {
				self.note_committed_write(
					record.finalization.tenant_id,
					record.finalization.charged_bytes,
				);
				marker
			}
			PendingFinalizationResolution::ObservedApplied(marker) => marker,
		};
		let (response, trailing) = self.committed_response_for(record, marker);
		let delete_counts = match delete_counts_hint {
			Some(counts) => Some(counts),
			None => self.pending_finalizations.delete_counts(record.id).await?,
		};
		// Persist the terminal outcome before deleting the pending witness. A crash
		// can therefore leave both records, never a window containing neither.
		self.record_mutation_outcome(
			record.write_id,
			record.finalization.tenant_id,
			response,
			trailing,
			delete_counts,
		)
		.await?;
		let outcome = WriteOutcome::FinalizationCommitted { response, trailing };
		self.idempotency.insert(
			Self::idempotency_key_for(record.finalization.tenant_id, record.write_id),
			CachedWriteOutcome::new(outcome),
		);
		self.cdc_release_pending_finalization(record.id).await;
		if let Err(err) = self.pending_finalizations.remove(record.id).await {
			tracing::warn!(
				pending_id = %format_bytes_hex(record.id.as_bytes()),
				error = %err,
				"failed to remove reconciled pending finalization"
			);
		} else {
			self.refresh_pending_finalization_backlog_metric();
		}
		Ok((response, trailing))
	}

	pub(super) async fn record_mutation_outcome(
		&self,
		write_id: crate::schema::ClientWriteId,
		tenant_id: TenantId,
		response: WriteResponseMeta,
		finalization: Option<FinalizationMarker>,
		delete_counts: Option<[u32; 3]>,
	) -> std::io::Result<()> {
		self.mutation_outcomes
			.upsert(DurableMutationOutcome::new(
				write_id,
				tenant_id,
				response,
				finalization,
				delete_counts,
			))
			.await
	}

	pub(super) async fn durable_mutation_replay(
		&self,
		meta: &TableWriteRequestMeta,
	) -> std::io::Result<Option<(WriteResponseMeta, Option<FinalizationMarker>)>> {
		match self
			.mutation_outcomes
			.resolve(meta.parent_write_id, meta.tenant_id)
			.await?
		{
			MutationResolution::Present(outcome) => {
				let mut response = outcome.response()?;
				if response.mutation_commit_knowledge() == MutationCommitKnowledge::COMMITTED {
					response.status = WriteStatus::DUPLICATE;
				}
				Ok(Some((response, outcome.finalization()?)))
			}
			MutationResolution::Expired => {
				let synthetic = table_write_synthetic_meta(meta, Identity::ZERO, 0);
				let response = error_response(
					&synthetic,
					WriteStatus::WRITE_OPERATION_EXPIRED,
					self.local_node,
				)
				.with_mutation_outcome(
					MutationCommitKnowledge::INDETERMINATE,
					MutationStopReason::NONE,
				);
				Ok(Some((response, None)))
			}
			MutationResolution::Unknown => Ok(None),
		}
	}

	pub(super) async fn durable_delete_replay(
		&self,
		meta: &TableDeleteRowsRequestMeta,
	) -> std::io::Result<Option<(TableDeleteRowsResponseMeta, Option<FinalizationMarker>)>> {
		match self
			.mutation_outcomes
			.resolve(meta.parent_write_id, meta.tenant_id)
			.await?
		{
			MutationResolution::Present(outcome) => {
				let mut write = outcome.response()?;
				if write.mutation_commit_knowledge() == MutationCommitKnowledge::COMMITTED {
					write.status = WriteStatus::DUPLICATE;
				}
				let [deleted, already_deleted, not_found] = outcome.delete_counts.unwrap_or([0; 3]);
				Ok(Some((
					table_delete_rows_response(
						write,
						deleted,
						already_deleted,
						not_found,
						PbcRejectCode::NONE,
					),
					outcome.finalization()?,
				)))
			}
			MutationResolution::Expired => {
				let synthetic = table_delete_synthetic_write_meta(meta, Identity::ZERO, 0);
				let write = error_response(
					&synthetic,
					WriteStatus::WRITE_OPERATION_EXPIRED,
					self.local_node,
				)
				.with_mutation_outcome(
					MutationCommitKnowledge::INDETERMINATE,
					MutationStopReason::NONE,
				);
				Ok(Some((
					table_delete_rows_response(write, 0, 0, 0, PbcRejectCode::NONE),
					None,
				)))
			}
			MutationResolution::Unknown => Ok(None),
		}
	}

	pub(super) async fn resolve_mutation_outcome(
		&self,
		meta: MutationOutcomeRequestMeta,
	) -> std::io::Result<MutationOutcomeResponseMeta> {
		match self
			.mutation_outcomes
			.resolve(meta.operation_id, meta.tenant_id)
			.await?
		{
			MutationResolution::Present(outcome) => {
				let counts = outcome.delete_counts.unwrap_or([0; 3]);
				Ok(MutationOutcomeResponseMeta {
					write: outcome.response()?,
					deleted_count: counts[0],
					already_deleted_count: counts[1],
					not_found_count: counts[2],
					resolution: MutationResolutionStatus::PRESENT,
					operation_kind: if outcome.delete_counts.is_some() {
						MutationOperationKind::DELETE
					} else {
						MutationOperationKind::WRITE
					},
					_pad: [0; 2],
				})
			}
			MutationResolution::Expired => Ok(MutationOutcomeResponseMeta {
				write: self.mutation_resolution_response(
					meta.operation_id,
					WriteStatus::WRITE_OPERATION_EXPIRED,
				),
				deleted_count: 0,
				already_deleted_count: 0,
				not_found_count: 0,
				resolution: MutationResolutionStatus::EXPIRED,
				operation_kind: MutationOperationKind::WRITE,
				_pad: [0; 2],
			}),
			MutationResolution::Unknown => {
				if let Some(record) = self
					.pending_finalizations
					.find_by_write_id(meta.tenant_id, meta.operation_id)
					.await?
					.filter(|record| record.finalization.tenant_id == meta.tenant_id)
				{
					let delete_counts = self.pending_finalizations.delete_counts(record.id).await?;
					let counts = delete_counts.unwrap_or([0; 3]);
					return Ok(MutationOutcomeResponseMeta {
						write: self
							.recoverable_pending_response(
								record.write_id,
								record.finalization.marker.epoch,
								record.finalization.marker.commit_hlc,
							)
							.with_mutation_outcome(
								MutationCommitKnowledge::INDETERMINATE,
								MutationStopReason::NONE,
							),
						deleted_count: counts[0],
						already_deleted_count: counts[1],
						not_found_count: counts[2],
						resolution: MutationResolutionStatus::PRESENT,
						operation_kind: if delete_counts.is_some() {
							MutationOperationKind::DELETE
						} else {
							MutationOperationKind::WRITE
						},
						_pad: [0; 2],
					});
				}
				Ok(MutationOutcomeResponseMeta {
					write: self.mutation_resolution_response(
						meta.operation_id,
						WriteStatus::WRITE_OPERATION_UNKNOWN,
					),
					deleted_count: 0,
					already_deleted_count: 0,
					not_found_count: 0,
					resolution: MutationResolutionStatus::UNKNOWN,
					operation_kind: MutationOperationKind::WRITE,
					_pad: [0; 2],
				})
			}
		}
	}

	fn mutation_resolution_response(
		&self,
		operation_id: crate::schema::ClientWriteId,
		status: WriteStatus,
	) -> WriteResponseMeta {
		WriteResponseMeta {
			status,
			_pad1: [0; 7],
			log_index: 0,
			term: 0,
			epoch: self.admission_snapshot().current_epoch,
			commit_hlc: HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			},
			leader_hint: self.local_node,
			echoed_write_id: operation_id,
			has_finalization: 0,
			_pad2: [0; 7],
		}
		.with_mutation_outcome(
			MutationCommitKnowledge::INDETERMINATE,
			MutationStopReason::NONE,
		)
	}

	pub(super) fn decode_table_delete_request_frame(
		&self,
		frame: &HrftFrame,
	) -> std::io::Result<(TableDeleteRowsRequestMeta, Vec<Identity>)> {
		if frame.meta.len() < std::mem::size_of::<TableDeleteRowsRequestMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"TABLE_DELETE_REQ meta too small",
			));
		}
		let meta: TableDeleteRowsRequestMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableDeleteRowsRequestMeta>()]);
		let payload_bytes = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"TABLE_DELETE_REQ missing Arrow payload",
			)
		})?;
		let payload = crate::network::ipc::wire_decode_batch(payload_bytes)?;
		let row_key_ids = decode_table_delete_rows_batch(&meta, &payload)?;
		Ok((meta, row_key_ids))
	}

	pub(super) fn build_write_response_frame(
		&self,
		msg_type: MsgType,
		stream_id: u32,
		resp: WriteResponseMeta,
		trailing: Option<FinalizationMarker>,
	) -> HrftFrame {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		let trailing = trailing.map(|marker| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(&marker));
			buf
		});
		let trailing_len = trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0);
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		}
	}

	pub(super) fn build_table_write_response_frame(
		&self,
		stream_id: u32,
		resp: TableWriteResponseMeta,
		trailing: Option<FinalizationMarker>,
	) -> HrftFrame {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		let trailing = trailing.map(|marker| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(&marker));
			buf
		});
		let trailing_len = trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0);
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_WRITE_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		}
	}

	pub(super) fn build_table_delete_response_frame(
		&self,
		stream_id: u32,
		resp: TableDeleteRowsResponseMeta,
		trailing: Option<FinalizationMarker>,
	) -> HrftFrame {
		let mut meta_buf = self.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		let trailing = trailing.map(|marker| {
			let mut buf = self.pool.acquire();
			buf.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(&marker));
			buf
		});
		let trailing_len = trailing.as_ref().map(|buf| buf.len() as u32).unwrap_or(0);
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_DELETE_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing,
		}
	}

	pub(super) fn build_mutation_outcome_response_frame(
		&self,
		stream_id: u32,
		response: MutationOutcomeResponseMeta,
	) -> HrftFrame {
		let mut meta = self.pool.acquire();
		meta.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&response));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::MUTATION_OUTCOME_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		}
	}

	pub(super) fn build_mutation_outcome_pin_response_frame(
		&self,
		stream_id: u32,
		response: MutationOutcomePinResponseMeta,
	) -> HrftFrame {
		let mut meta = self.pool.acquire();
		meta.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&response));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::MUTATION_OUTCOME_PIN_RESP.0,
				flags: 0,
				stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		}
	}
}

fn retryable_mutation_status(status: WriteStatus) -> bool {
	status == WriteStatus::NOT_LEADER
		|| status == WriteStatus::EPOCH_STALE
		|| status == WriteStatus::LEASE_EXPIRED
		|| status == WriteStatus::INVALID_LEASE_SIGNATURE
		|| status == WriteStatus::FINALIZATION_PENDING
		|| status == WriteStatus::BACKPRESSURED
}

pub(super) fn mutation_expiry_response(
	meta: &WriteRequestMeta,
	expiry: super::write_clocks::WriteExpiry,
	knowledge: MutationCommitKnowledge,
) -> WriteResponseMeta {
	error_response(
		meta,
		expiry.status,
		NodeId {
			id: 0,
			_reserved: 0,
		},
	)
	.with_mutation_outcome(knowledge, expiry.reason)
}
