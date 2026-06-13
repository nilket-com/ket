use super::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RowGroupFetchAuditCounters {
	pub(super) row_group_fetches: u64,
	pub(super) columnar_buffer_fetches: u64,
	pub(super) whole_record_fetches: u64,
	pub(super) fallback_fetches: u64,
	pub(super) row_groups_fetched: u64,
	pub(super) row_groups_total: u64,
	pub(super) predicate_fetched_groups: u64,
}

impl RowGroupFetchAuditCounters {
	pub(super) fn from_l2_strategy(strategy: &L2FetchStrategy) -> Self {
		let mut counters = Self::default();
		counters.observe_l2_strategy(strategy);
		counters
	}

	pub(super) fn observe_l2_strategy(&mut self, strategy: &L2FetchStrategy) {
		match strategy {
			L2FetchStrategy::RowGroup(plan) => {
				self.row_group_fetches = self.row_group_fetches.saturating_add(1);
				self.row_groups_fetched = self
					.row_groups_fetched
					.saturating_add(plan.surviving_groups.len() as u64);
				self.row_groups_total = self
					.row_groups_total
					.saturating_add(plan.directory.row_groups.len() as u64);
				self.predicate_fetched_groups = self
					.predicate_fetched_groups
					.saturating_add(plan.predicate_fetched_groups.len() as u64);
			}
			L2FetchStrategy::Columnar(_) => {
				self.columnar_buffer_fetches = self.columnar_buffer_fetches.saturating_add(1);
			}
			L2FetchStrategy::WholeRecord => {
				self.whole_record_fetches = self.whole_record_fetches.saturating_add(1);
			}
			L2FetchStrategy::Fallback(_) => {
				self.fallback_fetches = self.fallback_fetches.saturating_add(1);
			}
		}
	}

	pub(super) fn merge(&mut self, other: Self) {
		self.row_group_fetches = self
			.row_group_fetches
			.saturating_add(other.row_group_fetches);
		self.columnar_buffer_fetches = self
			.columnar_buffer_fetches
			.saturating_add(other.columnar_buffer_fetches);
		self.whole_record_fetches = self
			.whole_record_fetches
			.saturating_add(other.whole_record_fetches);
		self.fallback_fetches = self.fallback_fetches.saturating_add(other.fallback_fetches);
		self.row_groups_fetched = self
			.row_groups_fetched
			.saturating_add(other.row_groups_fetched);
		self.row_groups_total = self.row_groups_total.saturating_add(other.row_groups_total);
		self.predicate_fetched_groups = self
			.predicate_fetched_groups
			.saturating_add(other.predicate_fetched_groups);
	}

	pub(super) fn used_row_group(&self) -> bool {
		self.row_group_fetches > 0
	}

	pub(super) fn dominant_strategy(&self) -> &'static str {
		let candidates = [
			("row_group", self.row_group_fetches),
			("columnar_buffer", self.columnar_buffer_fetches),
			("whole_record", self.whole_record_fetches),
			("fallback", self.fallback_fetches),
		];
		candidates
			.into_iter()
			.max_by_key(|(_, count)| *count)
			.map(|(strategy, _)| strategy)
			.unwrap_or("row_group")
	}
}

#[derive(Clone, Debug)]
pub(super) enum QueryProjectedExecution {
	Disabled,
	Pbc {
		requirement: ColumnFetchRequirement,
		user_column_count: usize,
		runtime_filter: Option<RuntimeFilterApplication>,
	},
	Aggregate {
		requirement: ColumnFetchRequirement,
		user_column_count: usize,
		request: AggregateRequest,
	},
}

impl QueryProjectedExecution {
	pub(super) fn requirement(&self) -> Option<&ColumnFetchRequirement> {
		match self {
			Self::Disabled => None,
			Self::Pbc { requirement, .. } | Self::Aggregate { requirement, .. } => {
				Some(requirement)
			}
		}
	}
}

impl WendelonDataServer {
	/// Unary compatibility path for QUERY_REQ. The production HRFT
	/// path goes through [`Self::handle_query_stream`] so large
	/// result sets can flow as multiple `QUERY_RESP` frames without
	/// one-frame concatenation. This helper keeps direct frame-based
	/// tests and any remaining unary callers working by packaging the
	/// entire response into one terminal frame.
	pub(super) async fn handle_query(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		metrics().data_ring_query_requests_total.inc();
		let (meta, bytecode) = self.decode_query_request_frame(&frame)?;
		let span = tracing::info_span!(
			"data_ring.query.request",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			consistency = meta.consistency.0,
		);
		let (resp_meta, arrow_bytes) = match self
			.execute_query_with_migration_merge(&meta, &bytecode)
			.instrument(span)
			.await
		{
			Ok(result) => result,
			Err(err) => {
				self.emit_row_group_invariant_audit_for_error(
					actor,
					meta.tenant_id,
					&err,
					"query",
				)?;
				self.emit_encoding_invariant_audit_for_error(actor, meta.tenant_id, &err, "query")?;
				return Err(err);
			}
		};
		self.emit_query_completion_audit(
			actor,
			&meta,
			&resp_meta,
			arrow_bytes
				.as_ref()
				.map(|bytes| bytes.len() as u64)
				.unwrap_or(0),
		)?;
		Ok(self.build_query_response_frame(
			frame.header.stream_id,
			resp_meta,
			arrow_bytes,
			FRAME_FLAG_STREAM_END,
		))
	}

	pub(super) async fn handle_table_query(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		metrics().data_ring_query_requests_total.inc();
		let (meta, bytecode, runtime_filter) = self.decode_table_query_request_frame(&frame)?;
		let audit_meta = table_query_as_range_query(&meta);
		let trace_carrier = decode_table_query_trace_carrier(&meta);
		let trace_started_at = Instant::now();
		let linked_span = linked_table_query_span(&meta, trace_carrier);
		let runtime_filter_state = table_query_runtime_filter_state(&runtime_filter);
		let active_query = match self
			.table_query_cancellation
			.register(meta.tenant_id, meta.request_id)
		{
			ActiveTableQueryRegistration::Active(guard) => guard,
			ActiveTableQueryRegistration::CancelledBeforeStart => {
				let response =
					query_error_meta(&audit_meta, ReadStatus::QUERY_CANCELLED, self.local_node);
				metrics().observe_table_query_cancellation("cancelled_response");
				self.emit_query_completion_audit(actor, &audit_meta, &response, 0)?;
				record_linked_table_query_span_response(
					&linked_span,
					&response,
					runtime_filter_state,
					"not_planned",
					"cancelled_before_start",
					0,
					0,
					trace_started_at,
				);
				return Ok(self.build_query_response_frame_with_type(
					MsgType::TABLE_QUERY_RESP,
					frame.header.stream_id,
					response,
					None,
					FRAME_FLAG_STREAM_END,
				));
			}
		};
		let cancellation_token = active_query.token();
		let span = local_table_query_span(&meta, frame.header.stream_id, trace_carrier);
		let plan = match self
			.prepare_table_query_execution_plan_with_migration_with_cancellation(
				&meta,
				&bytecode,
				runtime_filter,
				&cancellation_token,
			)
			.instrument(span)
			.await
		{
			Ok(plan) => plan,
			Err(err) => {
				self.emit_row_group_invariant_audit_for_error(
					actor,
					meta.tenant_id,
					&err,
					"table_query",
				)?;
				self.emit_encoding_invariant_audit_for_error(
					actor,
					meta.tenant_id,
					&err,
					"table_query",
				)?;
				record_linked_table_query_span_error(
					&linked_span,
					runtime_filter_state,
					trace_started_at,
				);
				return Err(err);
			}
		};
		if plan.response.status != ReadStatus::OK || plan.batches.is_empty() {
			if plan.response.status == ReadStatus::QUERY_CANCELLED {
				metrics().observe_table_query_cancellation("cancelled_response");
			}
			self.emit_query_completion_audit(actor, &audit_meta, &plan.response, 0)?;
			self.emit_row_group_fetch_sampled_audit(
				actor,
				&audit_meta,
				&plan.row_group_fetch_audit,
			)?;
			self.emit_encoding_materialize_sampled_audit(
				actor,
				&audit_meta,
				&plan.encoding_materialize_audit,
			)?;
			record_linked_table_query_span_response(
				&linked_span,
				&plan.response,
				runtime_filter_state,
				table_query_planner_path(&plan),
				table_query_cancellation_outcome(plan.response.status),
				0,
				plan.batches.len(),
				trace_started_at,
			);
			return Ok(self.build_query_response_frame_with_type(
				MsgType::TABLE_QUERY_RESP,
				frame.header.stream_id,
				plan.response,
				None,
				FRAME_FLAG_STREAM_END,
			));
		}

		if cancellation_token.is_cancelled() {
			let response =
				query_error_meta(&audit_meta, ReadStatus::QUERY_CANCELLED, self.local_node);
			metrics().observe_table_query_cancellation("cancelled_response");
			self.emit_query_completion_audit(actor, &audit_meta, &response, 0)?;
			record_linked_table_query_span_response(
				&linked_span,
				&response,
				runtime_filter_state,
				table_query_planner_path(&plan),
				"cancelled",
				0,
				plan.batches.len(),
				trace_started_at,
			);
			return Ok(self.build_query_response_frame_with_type(
				MsgType::TABLE_QUERY_RESP,
				frame.header.stream_id,
				response,
				None,
				FRAME_FLAG_STREAM_END,
			));
		}
		let schema = plan.batches[0].schema();
		let concatenated = match concat_batches(&schema, &plan.batches) {
			Ok(concatenated) => concatenated,
			Err(err) => {
				record_linked_table_query_span_error(
					&linked_span,
					runtime_filter_state,
					trace_started_at,
				);
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!("table query concat: {err}"),
				));
			}
		};
		if cancellation_token.is_cancelled() {
			let response =
				query_error_meta(&audit_meta, ReadStatus::QUERY_CANCELLED, self.local_node);
			metrics().observe_table_query_cancellation("cancelled_response");
			self.emit_query_completion_audit(actor, &audit_meta, &response, 0)?;
			record_linked_table_query_span_response(
				&linked_span,
				&response,
				runtime_filter_state,
				table_query_planner_path(&plan),
				"cancelled",
				0,
				plan.batches.len(),
				trace_started_at,
			);
			return Ok(self.build_query_response_frame_with_type(
				MsgType::TABLE_QUERY_RESP,
				frame.header.stream_id,
				response,
				None,
				FRAME_FLAG_STREAM_END,
			));
		}
		let arrow_bytes = match wire_encode_batch_for_path(
			&concatenated,
			NetworkCompressionPath::QueryResponse,
		) {
			Ok(arrow_bytes) => arrow_bytes,
			Err(err) => {
				record_linked_table_query_span_error(
					&linked_span,
					runtime_filter_state,
					trace_started_at,
				);
				return Err(err);
			}
		};
		if cancellation_token.is_cancelled() {
			let response =
				query_error_meta(&audit_meta, ReadStatus::QUERY_CANCELLED, self.local_node);
			metrics().observe_table_query_cancellation("cancelled_response");
			self.emit_query_completion_audit(actor, &audit_meta, &response, 0)?;
			record_linked_table_query_span_response(
				&linked_span,
				&response,
				runtime_filter_state,
				table_query_planner_path(&plan),
				"cancelled",
				arrow_bytes.len(),
				plan.batches.len(),
				trace_started_at,
			);
			return Ok(self.build_query_response_frame_with_type(
				MsgType::TABLE_QUERY_RESP,
				frame.header.stream_id,
				response,
				None,
				FRAME_FLAG_STREAM_END,
			));
		}
		let response = QueryResponseMeta {
			arrow_len: arrow_bytes.len() as u32,
			..plan.response
		};
		self.emit_query_completion_audit(actor, &audit_meta, &response, arrow_bytes.len() as u64)?;
		self.emit_row_group_fetch_sampled_audit(actor, &audit_meta, &plan.row_group_fetch_audit)?;
		self.emit_encoding_materialize_sampled_audit(
			actor,
			&audit_meta,
			&plan.encoding_materialize_audit,
		)?;
		record_linked_table_query_span_response(
			&linked_span,
			&response,
			runtime_filter_state,
			table_query_planner_path(&plan),
			"none",
			arrow_bytes.len(),
			plan.batches.len(),
			trace_started_at,
		);
		Ok(self.build_query_response_frame_with_type(
			MsgType::TABLE_QUERY_RESP,
			frame.header.stream_id,
			response,
			Some(arrow_bytes),
			FRAME_FLAG_STREAM_END,
		))
	}

	pub(super) async fn handle_table_query_stream(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		metrics().data_ring_query_requests_total.inc();
		let (meta, bytecode, runtime_filter) = self.decode_table_query_request_frame(&frame)?;
		let audit_meta = table_query_as_range_query(&meta);
		let trace_carrier = decode_table_query_trace_carrier(&meta);
		let trace_started_at = Instant::now();
		let linked_span = linked_table_query_span(&meta, trace_carrier);
		let runtime_filter_state = table_query_runtime_filter_state(&runtime_filter);
		let provisional_response = query_ok_provisional_meta(&audit_meta, self.local_node);
		let active_query = match self
			.table_query_cancellation
			.register(meta.tenant_id, meta.request_id)
		{
			ActiveTableQueryRegistration::Active(guard) => guard,
			ActiveTableQueryRegistration::CancelledBeforeStart => {
				let response =
					query_error_meta(&audit_meta, ReadStatus::QUERY_CANCELLED, self.local_node);
				metrics().observe_table_query_cancellation("cancelled_response");
				self.emit_query_completion_audit(actor, &audit_meta, &response, 0)?;
				record_linked_table_query_span_response(
					&linked_span,
					&response,
					runtime_filter_state,
					"not_planned",
					"cancelled_before_start",
					0,
					0,
					trace_started_at,
				);
				let mut writer = TableQueryResponseStreamWriter::new(
					&mut *stream,
					frame.header.stream_id,
					provisional_response,
				);
				writer.emit_terminal(response).await?;
				stream.finish().await?;
				return Ok(());
			}
		};
		let cancellation_token = active_query.token();
		let span = local_table_query_span(&meta, frame.header.stream_id, trace_carrier);
		let mut writer = TableQueryResponseStreamWriter::new(
			&mut *stream,
			frame.header.stream_id,
			provisional_response,
		);
		let plan = match self
			.prepare_table_query_execution_plan_with_migration_with_cancellation_and_stream(
				&meta,
				&bytecode,
				runtime_filter,
				&cancellation_token,
				Some(&mut writer),
			)
			.instrument(span)
			.await
		{
			Ok(plan) => plan,
			Err(err) => {
				self.emit_row_group_invariant_audit_for_error(
					actor,
					meta.tenant_id,
					&err,
					"table_query",
				)?;
				self.emit_encoding_invariant_audit_for_error(
					actor,
					meta.tenant_id,
					&err,
					"table_query",
				)?;
				record_linked_table_query_span_error(
					&linked_span,
					runtime_filter_state,
					trace_started_at,
				);
				if writer.has_started() {
					metrics().observe_table_query_cancellation("stream_dropped");
				}
				return Err(err);
			}
		};
		let mut response = plan.response;
		if response.status == ReadStatus::KEY_NOT_FOUND {
			response.status = ReadStatus::OK;
		}
		if response.status == ReadStatus::QUERY_CANCELLED {
			metrics().observe_table_query_cancellation("cancelled_response");
		}
		self.emit_row_group_fetch_sampled_audit(actor, &audit_meta, &plan.row_group_fetch_audit)?;
		self.emit_encoding_materialize_sampled_audit(
			actor,
			&audit_meta,
			&plan.encoding_materialize_audit,
		)?;

		let arrow_bytes = writer.total_arrow_bytes();
		let batch_count = writer.batch_count();
		self.emit_query_completion_audit(actor, &audit_meta, &response, arrow_bytes as u64)?;
		record_linked_table_query_span_response(
			&linked_span,
			&response,
			runtime_filter_state,
			table_query_planner_path(&plan),
			table_query_cancellation_outcome(response.status),
			arrow_bytes,
			batch_count,
			trace_started_at,
		);
		if let Err(err) = writer.emit_terminal(response).await {
			metrics().observe_table_query_cancellation("stream_dropped");
			return Err(err);
		}
		if let Err(err) = stream.finish().await {
			metrics().observe_table_query_cancellation("stream_dropped");
			return Err(err);
		}
		Ok(())
	}

	pub(super) async fn handle_table_query_cancel(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let meta = self.decode_table_query_cancel_request_frame(&frame)?;
		let authorized = self
			.authorize_actor_for_data(actor, Some(meta.tenant_id), DataOperation::Query)
			.is_ok();
		let tenant_visible = authorized && self.enforce_read_policy(meta.tenant_id).await.is_ok();
		let status = if !tenant_visible {
			TableQueryCancelStatus::TERMINAL
		} else {
			self.table_query_cancellation
				.cancel(meta.tenant_id, meta.request_id)
		};
		metrics().observe_table_query_cancellation(status.as_str());
		Ok(self.build_table_query_cancel_response_frame(frame.header.stream_id, status))
	}

	pub(super) async fn handle_aggregate(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
		request: AggregateRequest,
	) -> std::io::Result<HrftFrame> {
		metrics().data_ring_query_requests_total.inc();
		let audit_meta =
			aggregate_as_range_query(&request, self.admission_snapshot().current_epoch);
		let span = tracing::info_span!(
			"data_ring.aggregate.request",
			stream_id = frame.header.stream_id,
			tenant_id = request.tenant_id.get(),
			table_object_id = %format_bytes_hex(request.table_object_id.as_bytes()),
			schema_generation = request.schema_generation,
			consistency = request.consistency.0,
			aggregate_count = request.aggregates.len(),
		);
		let response_result = if actor.role == ActorRole::DataNode {
			self.execute_aggregate_local(&request)
				.instrument(span)
				.await
		} else {
			self.execute_aggregate_with_migration_merge(&request)
				.instrument(span)
				.await
		};
		let response = match response_result {
			Ok(response) => response,
			Err(err) => {
				self.emit_row_group_invariant_audit_for_error(
					actor,
					request.tenant_id,
					&err,
					"aggregate",
				)?;
				self.emit_encoding_invariant_audit_for_error(
					actor,
					request.tenant_id,
					&err,
					"aggregate",
				)?;
				return Err(err);
			}
		};
		let returned_bytes = response
			.partials
			.iter()
			.map(|partial| partial.state.len() as u64)
			.sum::<u64>();
		let audit_response = aggregate_response_as_query_meta(
			&request,
			&response,
			self.local_node,
			audit_meta.expected_epoch,
		);
		self.emit_query_completion_audit(actor, &audit_meta, &audit_response, returned_bytes)?;
		self.build_aggregate_response_frame(
			frame.header.stream_id,
			&response,
			FRAME_FLAG_STREAM_END,
		)
	}

	pub(super) async fn handle_query_stream(
		&self,
		actor: &ActorIdentity,
		frame: HrftFrame,
		mut stream: Box<dyn TransportStream>,
	) -> std::io::Result<()> {
		metrics().data_ring_query_requests_total.inc();
		let (meta, bytecode) = self.decode_query_request_frame(&frame)?;
		let span = tracing::info_span!(
			"data_ring.query.stream",
			stream_id = frame.header.stream_id,
			tenant_id = meta.tenant_id.get(),
			expected_epoch = meta.expected_epoch,
			consistency = meta.consistency.0,
		);
		let plan = match self
			.prepare_query_execution_plan_with_migration(&meta, &bytecode)
			.instrument(span)
			.await
		{
			Ok(plan) => plan,
			Err(err) => {
				self.emit_row_group_invariant_audit_for_error(
					actor,
					meta.tenant_id,
					&err,
					"query_stream",
				)?;
				self.emit_encoding_invariant_audit_for_error(
					actor,
					meta.tenant_id,
					&err,
					"query_stream",
				)?;
				return Err(err);
			}
		};

		if plan.response.status != ReadStatus::OK || plan.batches.is_empty() {
			self.emit_query_completion_audit(actor, &meta, &plan.response, 0)?;
			self.emit_row_group_fetch_sampled_audit(actor, &meta, &plan.row_group_fetch_audit)?;
			self.emit_encoding_materialize_sampled_audit(
				actor,
				&meta,
				&plan.encoding_materialize_audit,
			)?;
			let response = self.build_query_response_frame(
				frame.header.stream_id,
				plan.response,
				None,
				FRAME_FLAG_STREAM_END,
			);
			write_frame(&mut *stream, &response.header, &response.meta, None, None).await?;
			stream.finish().await?;
			return Ok(());
		}

		let QueryExecutionPlan {
			response,
			batches,
			deadline,
			row_group_fetch_audit,
			encoding_materialize_audit,
			..
		} = plan;
		let Some(encoded_batches) = encode_query_stream_batches_with_deadline(batches, deadline)?
		else {
			let budget_resp =
				query_error_meta(&meta, ReadStatus::QUERY_BUDGET_EXCEEDED, self.local_node);
			self.emit_query_completion_audit(actor, &meta, &budget_resp, 0)?;
			self.emit_row_group_fetch_sampled_audit(actor, &meta, &row_group_fetch_audit)?;
			self.emit_encoding_materialize_sampled_audit(
				actor,
				&meta,
				&encoding_materialize_audit,
			)?;
			let response = self.build_query_response_frame(
				frame.header.stream_id,
				budget_resp,
				None,
				FRAME_FLAG_STREAM_END,
			);
			write_frame(&mut *stream, &response.header, &response.meta, None, None).await?;
			stream.finish().await?;
			return Ok(());
		};
		let total_bytes = encoded_batches.iter().map(|arrow| arrow.len() as u64).sum();
		self.emit_query_completion_audit(actor, &meta, &response, total_bytes)?;
		self.emit_row_group_fetch_sampled_audit(actor, &meta, &row_group_fetch_audit)?;
		self.emit_encoding_materialize_sampled_audit(actor, &meta, &encoding_materialize_audit)?;
		let batch_count = encoded_batches.len();
		for (index, arrow) in encoded_batches.into_iter().enumerate() {
			let is_last = index + 1 == batch_count;
			let flags = if is_last { FRAME_FLAG_STREAM_END } else { 0 };
			if index == 0 {
				let response = QueryResponseMeta {
					arrow_len: arrow.len() as u32,
					..response
				};
				let meta_bytes = bytemuck::bytes_of(&response);
				let header = FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::QUERY_RESP.0,
					flags,
					stream_id: frame.header.stream_id,
					meta_len: meta_bytes.len() as u32,
					arrow_len: arrow.len() as u32,
					trailing_len: 0,
				};
				write_frame(&mut *stream, &header, meta_bytes, Some(&arrow), None).await?;
			} else {
				let header = FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::QUERY_RESP.0,
					flags,
					stream_id: frame.header.stream_id,
					meta_len: 0,
					arrow_len: arrow.len() as u32,
					trailing_len: 0,
				};
				write_frame(&mut *stream, &header, &[], Some(&arrow), None).await?;
			}
		}

		stream.finish().await?;
		Ok(())
	}
}
