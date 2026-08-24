use super::*;

impl WendelonDataServer {
	pub(super) async fn handle_stream_finalization_stream(
		&self,
		frame: HrftFrame,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		let request = self.decode_stream_finalization_request(&frame)?;
		let mut rx = self
			.stream_finalization(request.start_log_index, request.epoch)
			.await?;

		while let Some(marker) = rx.recv().await {
			let header = FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::STREAM_FIN_EVT.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: std::mem::size_of::<FinalizationMarker>() as u32,
				arrow_len: 0,
				trailing_len: 0,
			};
			write_frame(
				&mut *stream,
				&header,
				bytemuck::bytes_of(&marker),
				None,
				None,
			)
			.await?;
		}

		stream.finish().await?;
		Ok(())
	}

	pub(super) async fn handle_bulk_stream(
		&self,
		actor: &ActorIdentity,
		first_frame: HrftFrame,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		let mut next_frame = Some(first_frame);
		loop {
			let frame = if let Some(frame) = next_frame.take() {
				frame
			} else {
				match read_frame(&mut *stream, &self.pool).await {
					Ok(frame) => frame,
					Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
					Err(e) => return Err(e),
				}
			};

			if frame.header.msg_type != MsgType::BULK_WRITE_REQ.0 {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"BULK_WRITE stream expected BULK_WRITE_REQ item, got msg_type=0x{:02x}",
						frame.header.msg_type
					),
				));
			}

			let (meta, payload) = self.decode_write_request_frame(&frame, "BULK_WRITE_REQ")?;
			let span = tracing::info_span!(
				"data_ring.bulk_write.request",
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
			let response = self.build_write_response_frame(
				MsgType::BULK_WRITE_RESP,
				frame.header.stream_id,
				resp,
				trailing,
			);
			write_frame(
				&mut *stream,
				&response.header,
				&response.meta,
				response.arrow_payload.as_deref(),
				response.trailing.as_deref(),
			)
			.await?;
		}

		stream.finish().await?;
		Ok(())
	}

	pub(super) async fn handle_cdc_subscribe_stream(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
		req: CdcSubscribeRequestMeta,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		let subscription_id = CdcSubscriptionId::new(
			(u128::from(self.local_node.id) << 64) | u128::from(frame.header.stream_id),
		);
		let bound = match req.bound() {
			Ok(bound) => bound,
			Err(_) => {
				let reject =
					CdcRejectMeta::new(subscription_id, CdcRejectCode::InternalEngineError);
				cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
				stream.finish().await?;
				return Ok(());
			}
		};
		let requested_logical_schema_fingerprint = req.decoded_logical_schema_fingerprint();
		let (descriptor, drop_fenced, effective_retention) = {
			let state = self.state_machine.state_snapshot().await;
			let descriptor = state
				.table_schemas
				.latest_by_id(req.tenant_id, req.table_object_id)
				.cloned();
			let drop_fenced = state.table_drop_blocks_admission(req.tenant_id, req.table_object_id);
			let retention = self.table_effective_retention_from_state(
				&state,
				req.tenant_id,
				req.table_object_id,
			);
			(descriptor, drop_fenced, retention)
		};
		if drop_fenced {
			let reject = CdcRejectMeta::new(subscription_id, CdcRejectCode::UnknownTable);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		let Some(descriptor) = descriptor else {
			let reject = CdcRejectMeta::new(subscription_id, CdcRejectCode::UnknownTable);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		};
		if descriptor.logical_schema_fingerprint != requested_logical_schema_fingerprint {
			let reject = CdcRejectMeta::new(
				subscription_id,
				CdcRejectCode::SchemaFingerprintIncompatible,
			);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		if req.start_cursor != CdcCursor::MIN
			&& check_read_against_retention(req.start_cursor.row_hlc, &effective_retention).is_err()
		{
			let reject = CdcRejectMeta::new(subscription_id, CdcRejectCode::BelowRetentionFloor);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		let source_ranges = match table_query_scan_ranges(req.tenant_id, &descriptor, None) {
			Ok(ranges) => ranges,
			Err(_) => {
				let reject =
					CdcRejectMeta::new(subscription_id, CdcRejectCode::InternalEngineError);
				cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
				stream.finish().await?;
				return Ok(());
			}
		};
		if !self.cdc_table_maybe_replicated_here(&source_ranges) {
			let reject = CdcRejectMeta::new(subscription_id, CdcRejectCode::TableNotReplicatedHere);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		self.register_cdc_heartbeat_localities(&source_ranges);
		let snapshot = CatchupSnapshot::new(self.cdc_capture_snapshot_hlc(&source_ranges).await);
		if let Err(code) = self
			.cdc_open_time_ring_continuity_check(&source_ranges, &snapshot)
			.await
		{
			let reject = CdcRejectMeta::new(subscription_id, code);
			cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
			stream.finish().await?;
			return Ok(());
		}
		if let Some(resume_digest) = req.resume_digest() {
			let visible_rows = self
				.compute_cdc_visible_row_keys_at_cursor(
					req.tenant_id,
					&descriptor,
					&source_ranges,
					req.start_cursor,
				)
				.await?;
			let visible_digest =
				cdc_visible_rows_digest(req.tenant_id, descriptor.table_object_id, visible_rows);
			if visible_digest != resume_digest {
				let reject =
					CdcRejectMeta::new(subscription_id, CdcRejectCode::CheckpointDigestMismatch);
				cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
				stream.finish().await?;
				return Ok(());
			}
		}

		let mut live_registration = if bound == CdcSubscribeBound::Live {
			Some(self.cdc_registry.register(
				req.tenant_id,
				req.table_object_id,
				snapshot.ceiling(),
				bound,
				requested_logical_schema_fingerprint,
			))
		} else {
			None
		};
		let stream_subscription_id = live_registration
			.as_ref()
			.map(|(handle, _)| handle.subscription_id)
			.unwrap_or(subscription_id);
		if let Err(err) =
			self.emit_cdc_subscription_opened_audit(actor, &req, stream_subscription_id, bound)
		{
			if let Some((handle, _)) = &live_registration {
				self.cdc_registry.unregister(
					req.tenant_id,
					req.table_object_id,
					handle.subscription_id,
					crate::metrics::CdcSubscriptionMetricResult::Cancelled,
				);
			}
			return Err(err);
		}
		let catchup_floor_hlc = if req.start_cursor == CdcCursor::MIN {
			effective_retention
				.floor
				.unwrap_or(req.start_cursor.row_hlc)
		} else {
			req.start_cursor.row_hlc
		};
		let catchup_started = Instant::now();
		let catchup_events = match self
			.collect_cdc_catchup_events(
				req.tenant_id,
				&descriptor,
				&source_ranges,
				req.start_cursor,
				snapshot.ceiling(),
				catchup_floor_hlc,
			)
			.await
		{
			Ok(events) => events,
			Err(err) => {
				if let Some((handle, _)) = &live_registration {
					self.cdc_registry.unregister(
						req.tenant_id,
						req.table_object_id,
						handle.subscription_id,
						crate::metrics::CdcSubscriptionMetricResult::Cancelled,
					);
				}
				tracing::warn!(error = ?err, "CDC catchup collection failed");
				let reject =
					CdcRejectMeta::new(stream_subscription_id, CdcRejectCode::InternalEngineError);
				self.emit_cdc_subscription_closed_audit(
					actor,
					req.tenant_id,
					req.table_object_id,
					stream_subscription_id,
					"reject_internal_engine_error",
					req.start_cursor,
				)?;
				cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await?;
				stream.finish().await?;
				return Ok(());
			}
		};
		let checkpoint_visible_rows = self
			.compute_cdc_visible_row_keys_at_cursor(
				req.tenant_id,
				&descriptor,
				&source_ranges,
				snapshot.ceiling(),
			)
			.await?;
		let checkpoint_digest = cdc_visible_rows_digest(
			req.tenant_id,
			descriptor.table_object_id,
			checkpoint_visible_rows,
		);

		match bound {
			CdcSubscribeBound::SnapshotAtOpen => {
				self.emit_cdc_catchup_events(
					&mut *stream,
					frame.header.stream_id,
					subscription_id,
					req.logical_schema_fingerprint,
					&catchup_events,
				)
				.await?;
				let checkpoint = CdcCheckpointMeta {
					subscription_id,
					cursor: snapshot.ceiling(),
					visible_state_digest: checkpoint_digest,
					digest_domain_raw: CdcDigestDomain::TableRowVisible.as_raw(),
					_pad: [0; 4],
				};
				cdc_emit_checkpoint(
					&mut *stream,
					frame.header.stream_id,
					&checkpoint,
					CdcFrameSource::Catchup,
				)
				.await?;
				let end = CdcFrameMeta {
					subscription_id,
					frame_low_cursor: snapshot.ceiling(),
					frame_high_cursor: snapshot.ceiling(),
					logical_schema_fingerprint: req.logical_schema_fingerprint,
					event_count: RowCount::ZERO,
					source_raw: CdcFrameSource::Catchup.as_raw(),
					_pad: [0; 7],
				};
				cdc_emit_empty_frame(&mut *stream, frame.header.stream_id, &end, true).await?;
				self.emit_cdc_subscription_closed_audit(
					actor,
					req.tenant_id,
					req.table_object_id,
					subscription_id,
					"snapshot_complete",
					snapshot.ceiling(),
				)?;
				stream.finish().await?;
				Ok(())
			}
			CdcSubscribeBound::Live => {
				let Some((handle, mut recv)) = live_registration.take() else {
					return Ok(());
				};
				self.emit_cdc_catchup_events(
					&mut *stream,
					frame.header.stream_id,
					handle.subscription_id,
					req.logical_schema_fingerprint,
					&catchup_events,
				)
				.await?;
				let checkpoint = CdcCheckpointMeta {
					subscription_id: handle.subscription_id,
					cursor: snapshot.ceiling(),
					visible_state_digest: checkpoint_digest,
					digest_domain_raw: CdcDigestDomain::TableRowVisible.as_raw(),
					_pad: [0; 4],
				};
				cdc_emit_checkpoint(
					&mut *stream,
					frame.header.stream_id,
					&checkpoint,
					CdcFrameSource::Catchup,
				)
				.await?;
				metrics().observe_cdc_catchup_to_live_handoff(catchup_started.elapsed());
				let mut pending_live_entries = self
					.cdc_collect_ring_entries_after(
						req.tenant_id,
						req.table_object_id,
						&source_ranges,
						snapshot.ceiling(),
					)
					.await;
				let mut checkpoint_cadence = CdcCheckpointCadence::new(Instant::now());
				let mut finalizations = self.state_machine.subscribe_finalizations();
				let mut finalization_stream_open = true;
				loop {
					if let Some(code) = self
						.cdc_live_retention_reject_code(
							req.tenant_id,
							req.table_object_id,
							handle.cursor_high_water.load(),
						)
						.await
					{
						let reject = CdcRejectMeta::new(handle.subscription_id, code);
						let emit =
							cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject).await;
						self.cdc_registry.remove(
							req.tenant_id,
							req.table_object_id,
							handle.subscription_id,
						);
						self.emit_cdc_subscription_closed_audit(
							actor,
							req.tenant_id,
							req.table_object_id,
							handle.subscription_id,
							cdc_reject_audit_reason(code),
							handle.cursor_high_water.load(),
						)?;
						emit?;
						stream.finish().await?;
						return Ok(());
					}
					let safe_emission_hlc = self.cdc_safe_emission_hlc(&source_ranges).await;
					if let Some(recovery_upper) = self
						.cdc_live_recovery_upper_after(
							req.tenant_id,
							req.table_object_id,
							&source_ranges,
							handle.cursor_high_water.load(),
							safe_emission_hlc,
						)
						.await
					{
						let recovery_start = handle.cursor_high_water.load();
						let scan_floor = self
							.cdc_catchup_scan_floor_hlc(
								req.tenant_id,
								req.table_object_id,
								recovery_start,
							)
							.await;
						let recovery_events = match self
							.collect_cdc_catchup_events(
								req.tenant_id,
								&descriptor,
								&source_ranges,
								recovery_start,
								recovery_upper,
								scan_floor,
							)
							.await
						{
							Ok(events) => events,
							Err(err) => {
								tracing::warn!(error = ?err, "CDC live-tail recovery catchup failed");
								let reject = CdcRejectMeta::new(
									handle.subscription_id,
									CdcRejectCode::InternalEngineError,
								);
								let emit =
									cdc_emit_reject(&mut *stream, frame.header.stream_id, &reject)
										.await;
								self.cdc_registry.remove(
									req.tenant_id,
									req.table_object_id,
									handle.subscription_id,
								);
								self.emit_cdc_subscription_closed_audit(
									actor,
									req.tenant_id,
									req.table_object_id,
									handle.subscription_id,
									"reject_internal_engine_error",
									handle.cursor_high_water.load(),
								)?;
								emit?;
								stream.finish().await?;
								return Ok(());
							}
						};
						if self
							.emit_cdc_live_recovery_catchup_events(
								&mut *stream,
								frame.header.stream_id,
								&handle,
								req.logical_schema_fingerprint,
								&recovery_events,
								recovery_upper,
							)
							.await
							.is_err()
						{
							self.emit_cdc_subscription_closed_audit(
								actor,
								req.tenant_id,
								req.table_object_id,
								handle.subscription_id,
								"client_cancel",
								handle.cursor_high_water.load(),
							)?;
							self.cdc_registry.unregister(
								req.tenant_id,
								req.table_object_id,
								handle.subscription_id,
								crate::metrics::CdcSubscriptionMetricResult::Cancelled,
							);
							return Ok(());
						}
					}
					pending_live_entries.extend(
						self.cdc_collect_ring_entries_after(
							req.tenant_id,
							req.table_object_id,
							&source_ranges,
							handle.cursor_high_water.load(),
						)
						.await,
					);
					if self
						.drain_cdc_ready_live_entries(
							&mut *stream,
							frame.header.stream_id,
							&handle,
							req.tenant_id,
							&descriptor,
							&source_ranges,
							&mut pending_live_entries,
							safe_emission_hlc,
							&mut checkpoint_cadence,
						)
						.await
						.is_err()
					{
						self.emit_cdc_subscription_closed_audit(
							actor,
							req.tenant_id,
							req.table_object_id,
							handle.subscription_id,
							"client_cancel",
							handle.cursor_high_water.load(),
						)?;
						self.cdc_registry.unregister(
							req.tenant_id,
							req.table_object_id,
							handle.subscription_id,
							crate::metrics::CdcSubscriptionMetricResult::Cancelled,
						);
						return Ok(());
					}

					tokio::select! {
						maybe_entry = recv.recv() => {
							let Some(entry) = maybe_entry else {
								break;
							};
							pending_live_entries.push(entry);
						}
						() = self.cdc_locality_frontiers.wait_for_advance() => {}
						finalization = finalizations.recv(), if finalization_stream_open => {
							match finalization {
								Ok(_) => {}
								Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
								Err(tokio::sync::broadcast::error::RecvError::Closed) => {
									finalization_stream_open = false;
								}
							}
						}
					}
				}
				self.cdc_registry.unregister(
					req.tenant_id,
					req.table_object_id,
					handle.subscription_id,
					crate::metrics::CdcSubscriptionMetricResult::Closed,
				);
				self.emit_cdc_subscription_closed_audit(
					actor,
					req.tenant_id,
					req.table_object_id,
					handle.subscription_id,
					"closed",
					handle.cursor_high_water.load(),
				)?;
				stream.finish().await?;
				Ok(())
			}
		}
	}
}
