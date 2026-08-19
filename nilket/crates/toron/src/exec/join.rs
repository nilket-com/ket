use super::*;

use crate::phase_ledger::OperatorKind as PhaseOperatorKind;

mod build_profile;

mod build_rows;

mod columns;

mod global_fold_profile;

mod grouped_chain_profile;

mod membership_chain_profile;

mod partitioned_build_profile;

mod residency_profile;

mod spill;

mod spill_placement;

mod spilled_hash;

mod streamed_spill;

pub(in crate::exec) mod stage_attribution;

mod streaming;

mod throughput_profile;

pub(in crate::exec) use self::build_profile::{
	record_existence_summary_profile, record_join_build_profile,
};

pub(super) use self::build_rows::{BuildRowList, JoinBuildRows, build_in_memory_join_rows};

pub(super) use self::columns::{collect_program_column_ordinals, grouped_aggregate_input_columns};

pub(in crate::exec) use self::global_fold_profile::{
	GlobalFoldJoinFilterProfile, GlobalFoldProfileLeaf,
};

pub(in crate::exec) use self::grouped_chain_profile::{
	GroupedChainPinOutcome, GroupedChainProfile, GroupedChainProfileLeaf,
	GroupedChainSpillObserver, GroupedChainSubLeaf,
};

pub(in crate::exec) use self::membership_chain_profile::{
	MembershipChainOperation, MembershipChainPhase, MembershipChainProfile, MembershipSpillRole,
};

pub(in crate::exec) use self::spill::SpilledInnerJoinResidencySnapshot;

pub(in crate::exec) use self::spill::{PinnedSpilledInnerBuildStats, SpilledInnerJoinBuild};

pub(in crate::exec) use self::spill::{
	SpilledBatchRowRef, SpilledInnerJoinMatch, SpilledJoinBuild, SpilledSemiAntiExistenceBuild,
	partition_bucket_if_non_null_with_scratch,
};

pub(crate) use self::spill::{partition_bucket, partition_bucket_with_scratch};

pub(in crate::exec) use self::spill_placement::default_join_spill_dir;

pub(super) use self::spilled_hash::*;

pub(super) use self::streamed_spill::*;

#[cfg(test)]
pub(in crate::exec) use self::streaming::{
	StreamingSemiAntiExistence, StreamingSemiAntiRuntime, StreamingSemiAntiStage,
	extract_streaming_inner_prefix_semi_anti_chain,
};

pub(in crate::exec) use self::throughput_profile::{
	SpillCacheEvictions, StreamedSpilledLeftOuterProfile, ThroughputOperationPhase,
};

/// ADR 335: the raw 64 MiB task cap false-trips accepted sf=1 controls; the output guard

/// targets the GiB-scale materializations that cgroup-kill before the engine can report a row.

pub(super) const JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER: u64 = 16;

/// ADR 336: Phase 0 measured the largest accepted spill-completer at 440,765,522

/// retained bytes and the retained-input wall at 1,423,571,737; 16x the default task memory

/// cap sits in that gap.

pub(super) const JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER: u64 = 16;

pub(super) fn grouped_aggregate_is_order_insensitive(aggr_expr: &[BoundAggregateCall]) -> bool {
	// ADR 561: the classification is registry metadata; this predicate only
	// applies the float qualification the `InsensitiveUnlessFloat` rule names.
	aggr_expr.iter().all(|call| {
		match crate::aggregate_kernel::stable_spec_for_function(&call.function).order_sensitivity {
			crate::aggregate_kernel::AggregateOrderSensitivity::Insensitive => true,
			crate::aggregate_kernel::AggregateOrderSensitivity::InsensitiveUnlessFloat => {
				call.args.roots.iter().all(|root| {
					!matches!(
						call.args.node_type(*root),
						Some(DataType::Float32 | DataType::Float64)
					)
				})
			}
			crate::aggregate_kernel::AggregateOrderSensitivity::Sensitive => false,
		}
	})
}

pub(super) fn projected_schema_for_columns(
	schema: &SchemaRef,

	columns: &[ColumnOrdinal],
) -> Result<SchemaRef, ExecutionError> {
	let fields = columns
		.iter()
		.map(|ordinal| {
			schema
				.fields()
				.get(ordinal.0 as usize)
				.map(|field| field.as_ref().clone())
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("projected spill schema ordinal {} out of bounds", ordinal.0),
				})
		})
		.collect::<Result<Vec<_>, _>>()?;

	Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn project_batch_columns(
	batch: &RecordBatch,

	columns: &[ColumnOrdinal],
) -> Result<RecordBatch, ExecutionError> {
	let projected_schema = projected_schema_for_columns(&batch.schema(), columns)?;

	let projected_columns = columns
		.iter()
		.map(|ordinal| {
			batch
				.columns()
				.get(ordinal.0 as usize)
				.cloned()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("projected spill column ordinal {} out of bounds", ordinal.0),
				})
		})
		.collect::<Result<Vec<_>, _>>()?;

	Ok(RecordBatch::try_new(projected_schema, projected_columns)?)
}

pub(super) fn remap_column_ordinals(
	projection: &[ColumnOrdinal],

	ordinals: &[ColumnOrdinal],
) -> Result<Vec<ColumnOrdinal>, ExecutionError> {
	ordinals
		.iter()
		.map(|ordinal| {
			projection
				.iter()
				.position(|candidate| candidate == ordinal)
				.map(|position| ColumnOrdinal(position as u32))
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("projected spill key ordinal {} was not retained", ordinal.0),
				})
		})
		.collect()
}

pub(super) fn default_array_for_unused_column(
	data_type: &DataType,

	rows: usize,
) -> Result<ArrayRef, ExecutionError> {
	Ok(match data_type {
		DataType::Boolean => Arc::new(BooleanArray::from(vec![false; rows])) as ArrayRef,

		DataType::Int32 => Arc::new(Int32Array::from(vec![0; rows])) as ArrayRef,

		DataType::Int64 => Arc::new(Int64Array::from(vec![0; rows])) as ArrayRef,

		DataType::Float32 => Arc::new(Float32Array::from(vec![0.0; rows])) as ArrayRef,

		DataType::Float64 => Arc::new(Float64Array::from(vec![0.0; rows])) as ArrayRef,

		DataType::Date32 => Arc::new(Date32Array::from(vec![0; rows])) as ArrayRef,

		DataType::Utf8 => Arc::new(StringArray::from(vec![Some(""); rows])) as ArrayRef,

		DataType::Decimal128(precision, scale) => Arc::new(
			Decimal128Array::from(vec![Some(0); rows])
				.with_precision_and_scale(*precision, *scale)
				.map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
		) as ArrayRef,

		_ => arrow::array::new_null_array(data_type, rows),
	})
}

pub(super) fn duration_micros(started: Instant) -> u64 {
	u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// One task-local timing handle for join execution.

///

/// ADR 551: free join functions must not receive a telemetry sink separately

/// from the phase ledger that nets nested acquisition out of operator time. The

/// handle keeps those two views from drifting and exposes the only operator-scope

/// construction point used by this module.

#[derive(Clone)]

pub(super) struct JoinTiming {
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,

	scan_meter: crate::scan_metering::ScanMeter,
}

impl JoinTiming {
	pub(super) fn from_context(context: &ExecutionContext) -> Self {
		Self {
			telemetry: context.task_telemetry.clone(),

			scan_meter: context.scan_meter.clone(),
		}
	}

	#[cfg(test)]

	pub(super) fn disabled(telemetry: Option<Arc<Mutex<TaskTelemetry>>>) -> Self {
		Self {
			telemetry,

			scan_meter: crate::scan_metering::ScanMeter::disabled(),
		}
	}

	pub(super) fn open_operator_transition(
		&self,

		kind: crate::phase_ledger::OperatorKind,
	) -> crate::phase_ledger::OperatorTransition {
		crate::phase_ledger::OperatorTransition::open(self.scan_meter.phases(), kind)
	}
}

impl std::ops::Deref for JoinTiming {
	type Target = Option<Arc<Mutex<TaskTelemetry>>>;

	fn deref(&self) -> &Self::Target {
		&self.telemetry
	}
}

pub(super) fn record_task_telemetry(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,

	update: impl FnOnce(&mut TaskTelemetry),
) {
	let Some(telemetry) = telemetry else {
		return;
	};

	let lock_started = Instant::now();

	if let Ok(mut telemetry) = telemetry.lock() {
		telemetry.phases.telemetry_lock_micros = telemetry
			.phases
			.telemetry_lock_micros
			.saturating_add(duration_micros(lock_started));

		update(&mut telemetry);
	}
}

pub(super) fn record_join_runtime_observation(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,

	mode: &JoinRuntimeObservationMode,

	observation: JoinRuntimeObservation,
) {
	if !mode.measures(observation.replay_id) {
		return;
	}

	record_task_telemetry(telemetry, |task| {
		task.join_observations.push(observation);
	});
}

pub(super) fn record_join_spill_observation(
	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,

	replay_id: JoinReplayId,

	path: crate::distributed::task::JoinSpillPath,

	spill_bytes: u64,
) {
	record_task_telemetry(telemetry, |task| {
		task.join_spill_observations
			.push(crate::distributed::task::JoinSpillObservation {
				replay_id,

				path,

				spill_bytes,
			});
	});
}

fn task_spill_bytes(telemetry: &Option<Arc<Mutex<TaskTelemetry>>>) -> u64 {
	telemetry
		.as_ref()
		.and_then(|telemetry| telemetry.lock().ok())
		.map(|telemetry| telemetry.counters.spill_bytes)
		.unwrap_or(0)
}

pub(super) fn record_batch_row_count(batches: &[RecordBatch]) -> u64 {
	batches
		.iter()
		.map(|batch| batch.num_rows() as u64)
		.sum::<u64>()
}

#[allow(clippy::too_many_arguments)]

pub(super) async fn execute_hash_join(
	left_schema: arrow::datatypes::SchemaRef,

	right_schema: arrow::datatypes::SchemaRef,

	left_batches: &[RecordBatch],

	right_batches: &[RecordBatch],

	kind: JoinKind,

	left_keys: &[crate::ir::ColumnOrdinal],

	right_keys: &[crate::ir::ColumnOrdinal],

	match_filter: Option<&BoundExprProgram>,

	null_safe_keys: bool,

	replay_id: JoinReplayId,

	strategy: JoinStrategy,

	output_schema: &arrow::datatypes::SchemaRef,

	task_budget: TaskBudget,

	telemetry: JoinTiming,

	join_runtime_observation_mode: JoinRuntimeObservationMode,

	spill_file_ops: Arc<dyn AsyncFileOps>,

	spill_dir: PathBuf,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	if left_keys.len() != right_keys.len() {
		return Err(ExecutionError::InvalidExpression {
			message: "join key arity mismatch".to_string(),
		});
	}

	if let Some(output) = try_execute_streamed_spilled_left_semi_join(
		left_schema.clone(),
		right_schema.clone(),
		left_batches,
		right_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		replay_id,
		strategy,
		output_schema,
		task_budget,
		telemetry.clone(),
		join_runtime_observation_mode.clone(),
		spill_file_ops.clone(),
		spill_dir.clone(),
	)
	.await?
	{
		return Ok(output);
	}

	if let Some(output) = try_execute_streamed_spilled_inner_join(
		left_schema.clone(),
		right_schema.clone(),
		left_batches,
		right_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		replay_id,
		strategy,
		output_schema,
		task_budget,
		telemetry.clone(),
		join_runtime_observation_mode.clone(),
		spill_file_ops.clone(),
		spill_dir.clone(),
	)
	.await?
	{
		return Ok(output);
	}

	if let Some(output) = try_execute_partitioned_spilled_left_outer_join(
		left_schema.clone(),
		right_schema.clone(),
		left_batches,
		right_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		replay_id,
		strategy,
		output_schema,
		task_budget,
		telemetry.clone(),
		join_runtime_observation_mode.clone(),
		spill_file_ops.clone(),
		spill_dir.clone(),
	)
	.await?
	{
		return Ok(output);
	}

	let left_batch = concat_or_empty(left_schema, left_batches)?;

	let right_batch = concat_or_empty(right_schema, right_batches)?;

	// ADR 239: CROSS is keyless — route through the explicit match-all path instead of the hash

	// build/probe. The hash path would bucket every row under `join_key_bytes([])`, which is the

	// single-bucket distributed trap; the explicit path emits every build×probe pair deliberately.

	// One bounded path serves both the in-memory and would-be-spilled cases, so they cannot diverge.

	if matches!(kind, JoinKind::Cross) {
		return execute_cross_join_match_all(
			&left_batch,
			&right_batch,
			output_schema,
			task_budget.memory.get(),
			replay_id,
			strategy,
			&telemetry,
			&join_runtime_observation_mode,
		);
	}

	let left_key_arrays = key_arrays(&left_batch, left_keys)?;

	let right_key_arrays = key_arrays(&right_batch, right_keys)?;

	record_task_telemetry(&telemetry, |task| {
		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(left_batch.num_rows() as u64);

		task.counters.probe_rows = task
			.counters
			.probe_rows
			.saturating_add(right_batch.num_rows() as u64);
	});

	if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
		> task_budget.memory.get()
	{
		let result = execute_spilled_hash_join(
			kind,
			&left_batch,
			&right_batch,
			&left_key_arrays,
			&right_key_arrays,
			output_schema,
			match_filter,
			null_safe_keys,
			task_budget,
			telemetry.clone(),
			spill_file_ops,
			spill_dir,
		)
		.await;

		if let Ok(batches) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,

					strategy,

					build_rows: RowCount::new(left_batch.num_rows() as u64),

					probe_rows: RowCount::new(right_batch.num_rows() as u64),

					output_rows: RowCount::new(record_batch_row_count(batches)),
				},
			);
		}

		return result;
	}

	let join_key_plan =
		JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, null_safe_keys);

	let build_started = Instant::now();

	// ADR0551-SITE: join/execute_hash_join/Build/1

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_rows = match build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	) {
		Ok(Some(build_rows)) => build_rows,

		Err(error) => {
			build_scope.discard_before_measurement();

			return Err(error);
		}

		Ok(None) => {
			build_scope.discard_before_measurement();

			let result = execute_spilled_hash_join(
				kind,
				&left_batch,
				&right_batch,
				&left_key_arrays,
				&right_key_arrays,
				output_schema,
				match_filter,
				null_safe_keys,
				task_budget,
				telemetry.clone(),
				spill_file_ops,
				spill_dir,
			)
			.await;

			if let Ok(batches) = &result {
				record_join_runtime_observation(
					&telemetry,
					&join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id,

						strategy,

						build_rows: RowCount::new(left_batch.num_rows() as u64),

						probe_rows: RowCount::new(right_batch.num_rows() as u64),

						output_rows: RowCount::new(record_batch_row_count(batches)),
					},
				);
			}

			return result;
		}
	};

	build_scope.finish_at_measurement();

	let build_micros = duration_micros(build_started);

	record_task_telemetry(&telemetry, |_task| {
		build_scope.commit_after_write();
	});

	record_join_build_profile(
		&telemetry,
		replay_id,
		strategy,
		join_key_plan,
		&build_rows,
		build_micros,
	);

	// ADR0551-SITE: join/execute_hash_join/Probe/1

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

	let selection = match probe_in_memory_join_rows(
		kind,
		join_key_plan,
		&build_rows,
		left_batch.num_rows(),
		&right_key_arrays,
		right_batch.num_rows(),
		match_filter,
		&left_batch,
		&right_batch,
		null_safe_keys,
	) {
		Ok(selection) => selection,

		Err(error) => {
			probe_scope.discard_before_measurement();

			return Err(error);
		}
	};

	record_task_telemetry(&telemetry, |task| {
		probe_scope.finish_at_measurement();

		task.counters.matched_rows = task
			.counters
			.matched_rows
			.saturating_add(selection.len() as u64);
	});

	probe_scope.commit_after_write();

	let output = vec![materialize_join_selection(
		kind,
		&left_batch,
		&right_batch,
		&selection,
		output_schema,
		task_budget.memory.get(),
	)?];

	record_join_runtime_observation(
		&telemetry,
		&join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,

			strategy,

			build_rows: RowCount::new(left_batch.num_rows() as u64),

			probe_rows: RowCount::new(right_batch.num_rows() as u64),

			output_rows: RowCount::new(record_batch_row_count(&output)),
		},
	);

	Ok(output)
}

#[allow(clippy::too_many_arguments)]

pub(super) async fn execute_hash_join_into_global_aggregate(
	executor: &Executor,

	left_schema: arrow::datatypes::SchemaRef,

	right_schema: arrow::datatypes::SchemaRef,

	left_batches: &[RecordBatch],

	right_batches: &[RecordBatch],

	kind: JoinKind,

	left_keys: &[crate::ir::ColumnOrdinal],

	right_keys: &[crate::ir::ColumnOrdinal],

	match_filter: Option<&BoundExprProgram>,

	null_safe_keys: bool,

	replay_id: JoinReplayId,

	strategy: JoinStrategy,

	join_output_schema: &arrow::datatypes::SchemaRef,

	filter_predicate: Option<&BoundExprProgram>,

	aggr_expr: &[BoundAggregateCall],

	aggregate_state: &mut [AggState],

	task_budget: TaskBudget,

	telemetry: JoinTiming,

	join_runtime_observation_mode: JoinRuntimeObservationMode,

	spill_file_ops: Arc<dyn AsyncFileOps>,

	spill_dir: PathBuf,
) -> Result<(), ExecutionError> {
	if left_keys.len() != right_keys.len() {
		return Err(ExecutionError::InvalidExpression {
			message: "join key arity mismatch".to_string(),
		});
	}

	if kind != JoinKind::Inner {
		return Err(ExecutionError::InvalidExpression {
			message: "streaming collapsing aggregate supports only inner joins".to_string(),
		});
	}

	// ADR0551-SITE: join/execute_hash_join_into_global_aggregate/Build/1

	let mut streamed_build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let streamed_build = match try_create_streamed_spilled_inner_build(
		left_schema.clone(),
		right_schema.clone(),
		left_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		task_budget,
		spill_file_ops.clone(),
		spill_dir.clone(),
	)
	.await
	{
		Ok(build) => build,

		Err(error) => {
			streamed_build_scope.discard_before_measurement();

			return Err(error);
		}
	};

	if let Some((mut spilled, key_plan)) = streamed_build {
		let attributed_spill_bytes = spilled.spill_bytes();

		record_task_telemetry(&telemetry, |task| {
			streamed_build_scope.finish_at_measurement();

			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(spilled.input_row_count());

			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});

		streamed_build_scope.commit_after_write();

		// ADR0551-SITE: join/execute_hash_join_into_global_aggregate/Probe/1

		let mut streamed_probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

		let result = fold_streamed_spilled_inner_join_into_global_aggregate(
			executor,
			&mut spilled,
			right_batches,
			right_keys,
			key_plan,
			null_safe_keys,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			aggregate_state,
			task_budget.memory.get(),
		)
		.await;

		let build_rows = spilled.input_row_count();

		record_task_telemetry(&telemetry, |task| {
			streamed_probe_scope.finish_at_measurement();

			if let Ok((probe_rows, output_rows)) = &result {
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);

				task.counters.matched_rows =
					task.counters.matched_rows.saturating_add(*output_rows);
			}
		});

		streamed_probe_scope.commit_after_write();

		if let Ok((probe_rows, output_rows)) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,

					strategy,

					build_rows: RowCount::new(build_rows),

					probe_rows: RowCount::new(*probe_rows),

					output_rows: RowCount::new(*output_rows),
				},
			);
		}

		record_join_spill_observation(
			&telemetry,
			replay_id,
			crate::distributed::task::JoinSpillPath::StreamedSpilledBuild,
			attributed_spill_bytes,
		);

		spilled.cleanup().await;

		result?;

		return Ok(());
	}

	streamed_build_scope.discard_before_measurement();

	let left_batch = concat_or_empty(left_schema, left_batches)?;

	let right_batch = concat_or_empty(right_schema, right_batches)?;

	let left_key_arrays = key_arrays(&left_batch, left_keys)?;

	let right_key_arrays = key_arrays(&right_batch, right_keys)?;

	record_task_telemetry(&telemetry, |task| {
		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(left_batch.num_rows() as u64);

		task.counters.probe_rows = task
			.counters
			.probe_rows
			.saturating_add(right_batch.num_rows() as u64);
	});

	if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
		> task_budget.memory.get()
	{
		let spill_before = task_spill_bytes(&telemetry);

		let result = execute_spilled_hash_join_into_global_aggregate(
			executor,
			kind,
			&left_batch,
			&right_batch,
			&left_key_arrays,
			&right_key_arrays,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			aggregate_state,
			match_filter,
			null_safe_keys,
			task_budget,
			telemetry.clone(),
			spill_file_ops,
			spill_dir,
		)
		.await;

		record_join_spill_observation(
			&telemetry,
			replay_id,
			crate::distributed::task::JoinSpillPath::EstimateSpillFallback,
			task_spill_bytes(&telemetry).saturating_sub(spill_before),
		);

		if let Ok(output_rows) = &result {
			record_join_runtime_observation(
				&telemetry,
				&join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,

					strategy,

					build_rows: RowCount::new(left_batch.num_rows() as u64),

					probe_rows: RowCount::new(right_batch.num_rows() as u64),

					output_rows: RowCount::new(*output_rows),
				},
			);
		}

		return result.map(|_| ());
	}

	let join_key_plan =
		JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, null_safe_keys);

	let build_started = Instant::now();

	// ADR0551-SITE: join/execute_hash_join_into_global_aggregate/Build/2

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_rows = match build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	) {
		Ok(Some(build_rows)) => build_rows,

		Err(error) => {
			build_scope.discard_before_measurement();

			return Err(error);
		}

		Ok(None) => {
			build_scope.discard_before_measurement();

			let spill_before = task_spill_bytes(&telemetry);

			let result = execute_spilled_hash_join_into_global_aggregate(
				executor,
				kind,
				&left_batch,
				&right_batch,
				&left_key_arrays,
				&right_key_arrays,
				join_output_schema,
				filter_predicate,
				aggr_expr,
				aggregate_state,
				match_filter,
				null_safe_keys,
				task_budget,
				telemetry.clone(),
				spill_file_ops,
				spill_dir,
			)
			.await;

			record_join_spill_observation(
				&telemetry,
				replay_id,
				crate::distributed::task::JoinSpillPath::BuildSpillFallback,
				task_spill_bytes(&telemetry).saturating_sub(spill_before),
			);

			if let Ok(output_rows) = &result {
				record_join_runtime_observation(
					&telemetry,
					&join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id,

						strategy,

						build_rows: RowCount::new(left_batch.num_rows() as u64),

						probe_rows: RowCount::new(right_batch.num_rows() as u64),

						output_rows: RowCount::new(*output_rows),
					},
				);
			}

			return result.map(|_| ());
		}
	};

	build_scope.finish_at_measurement();

	let build_micros = duration_micros(build_started);

	record_task_telemetry(&telemetry, |_task| {
		build_scope.commit_after_write();
	});

	record_join_build_profile(
		&telemetry,
		replay_id,
		strategy,
		join_key_plan,
		&build_rows,
		build_micros,
	);

	// ADR0551-SITE: join/execute_hash_join_into_global_aggregate/Probe/2

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

	let selection = match probe_in_memory_join_rows(
		kind,
		join_key_plan,
		&build_rows,
		left_batch.num_rows(),
		&right_key_arrays,
		right_batch.num_rows(),
		match_filter,
		&left_batch,
		&right_batch,
		null_safe_keys,
	) {
		Ok(selection) => selection,

		Err(error) => {
			probe_scope.discard_before_measurement();

			return Err(error);
		}
	};

	record_task_telemetry(&telemetry, |task| {
		probe_scope.finish_at_measurement();

		task.counters.matched_rows = task
			.counters
			.matched_rows
			.saturating_add(selection.len() as u64);
	});

	probe_scope.commit_after_write();

	fold_join_selection_chunks_into_global_aggregate(
		executor,
		kind,
		&left_batch,
		&right_batch,
		&selection,
		join_output_schema,
		filter_predicate,
		aggr_expr,
		aggregate_state,
		task_budget.memory.get(),
	)?;

	record_join_runtime_observation(
		&telemetry,
		&join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,

			strategy,

			build_rows: RowCount::new(left_batch.num_rows() as u64),

			probe_rows: RowCount::new(right_batch.num_rows() as u64),

			output_rows: RowCount::new(selection.len() as u64),
		},
	);

	record_join_spill_observation(
		&telemetry,
		replay_id,
		crate::distributed::task::JoinSpillPath::InMemory,
		0,
	);

	Ok(())
}

/// Maximum output rows accumulated before a CROSS-join batch is flushed — bounds the match-all

/// selection allocation so a large cartesian streams out in chunks (ADR 239 D5).

pub(super) const CROSS_JOIN_OUTPUT_CHUNK_ROWS: usize = 8192;

/// Explicit keyless match-all for `JoinKind::Cross` (ADR 239 D5): emit every build×probe pair.

/// Unlike the hash path this never keys on `join_key_bytes([])`, so a CROSS join cannot be mistaken

/// for — or silently degrade into — a single-bucket empty-key hash join. The output is chunked to

/// bound allocation; because there is exactly one match-all path, in-memory and would-be-spilled

/// execution produce identical cartesian rows.

pub(super) fn execute_cross_join_match_all(
	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	output_schema: &arrow::datatypes::SchemaRef,

	memory_cap: u64,

	replay_id: JoinReplayId,

	strategy: JoinStrategy,

	telemetry: &Option<Arc<Mutex<TaskTelemetry>>>,

	join_runtime_observation_mode: &JoinRuntimeObservationMode,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let left_rows = left_batch.num_rows();

	let right_rows = right_batch.num_rows();

	record_task_telemetry(telemetry, |task| {
		task.counters.build_rows = task.counters.build_rows.saturating_add(left_rows as u64);

		task.counters.probe_rows = task.counters.probe_rows.saturating_add(right_rows as u64);
	});

	let mut output = Vec::new();

	if left_rows > 0 && right_rows > 0 {
		let mut selection: Vec<JoinOutputRow> = Vec::new();

		for left in 0..left_rows {
			let left = join_build_row_index(left)?;

			for right in 0..right_rows {
				let right = join_probe_row_index(right)?;

				selection.push(JoinOutputRow::Paired { left, right });

				if selection.len() >= CROSS_JOIN_OUTPUT_CHUNK_ROWS {
					output.push(materialize_join_selection(
						JoinKind::Cross,
						left_batch,
						right_batch,
						&selection,
						output_schema,
						memory_cap,
					)?);

					selection.clear();
				}
			}
		}

		if !selection.is_empty() {
			output.push(materialize_join_selection(
				JoinKind::Cross,
				left_batch,
				right_batch,
				&selection,
				output_schema,
				memory_cap,
			)?);
		}
	}

	if output.is_empty() {
		output.push(RecordBatch::new_empty(output_schema.clone()));
	}

	let output_rows = record_batch_row_count(&output);

	record_task_telemetry(telemetry, |task| {
		task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
	});

	record_join_runtime_observation(
		telemetry,
		join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,

			strategy,

			build_rows: RowCount::new(left_rows as u64),

			probe_rows: RowCount::new(right_rows as u64),

			output_rows: RowCount::new(output_rows),
		},
	);

	Ok(output)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub(super) enum JoinOutputRow {
	Paired { left: u32, right: u32 },

	LeftFillNull { left: u32 },

	RightFillNull { right: u32 },

	LeftPreserved { left: u32 },

	RightPreserved { right: u32 },
}

pub(super) fn materialize_join_selection(
	kind: JoinKind,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	selection: &[JoinOutputRow],

	output_schema: &arrow::datatypes::SchemaRef,

	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let estimated_output_bytes =
		estimate_join_materialization_bytes(kind, left_batch, right_batch, selection.len());

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);

	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let output_columns = match kind {
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter => {
			let mut left_indices = Vec::with_capacity(selection.len());

			let mut right_indices = Vec::with_capacity(selection.len());

			for row in selection {
				match *row {
					JoinOutputRow::Paired { left, right } => {
						left_indices.push(Some(left));

						right_indices.push(Some(right));
					}

					JoinOutputRow::LeftFillNull { left } => {
						left_indices.push(Some(left));

						right_indices.push(None);
					}

					JoinOutputRow::RightFillNull { right } => {
						left_indices.push(None);

						right_indices.push(Some(right));
					}

					JoinOutputRow::LeftPreserved { .. } | JoinOutputRow::RightPreserved { .. } => {
						return Err(invalid_join_selection(kind, row));
					}
				}
			}

			let left_indices = UInt32Array::from(left_indices);

			let right_indices = UInt32Array::from(right_indices);

			let mut columns =
				Vec::with_capacity(left_batch.num_columns() + right_batch.num_columns());

			for column in left_batch.columns() {
				columns.push(arrow::compute::take(column, &left_indices, None)?);
			}

			for column in right_batch.columns() {
				columns.push(arrow::compute::take(column, &right_indices, None)?);
			}

			columns
		}

		JoinKind::LeftSemi | JoinKind::LeftAnti => {
			let mut left_indices = Vec::with_capacity(selection.len());

			for row in selection {
				match *row {
					JoinOutputRow::LeftPreserved { left } => left_indices.push(Some(left)),

					_ => return Err(invalid_join_selection(kind, row)),
				}
			}

			let left_indices = UInt32Array::from(left_indices);

			let mut columns = Vec::with_capacity(left_batch.num_columns());

			for column in left_batch.columns() {
				columns.push(arrow::compute::take(column, &left_indices, None)?);
			}

			columns
		}

		JoinKind::RightSemi | JoinKind::RightAnti => {
			let mut right_indices = Vec::with_capacity(selection.len());

			for row in selection {
				match *row {
					JoinOutputRow::RightPreserved { right } => right_indices.push(Some(right)),

					_ => return Err(invalid_join_selection(kind, row)),
				}
			}

			let right_indices = UInt32Array::from(right_indices);

			let mut columns = Vec::with_capacity(right_batch.num_columns());

			for column in right_batch.columns() {
				columns.push(arrow::compute::take(column, &right_indices, None)?);
			}

			columns
		}
	};

	Ok(RecordBatch::try_new(output_schema.clone(), output_columns)?)
}

pub(super) fn materialize_reoriented_inner_selection(
	probe_batch: &RecordBatch,

	build_batch: &RecordBatch,

	selection: &[JoinOutputRow],

	output_schema: &arrow::datatypes::SchemaRef,

	memory_cap: u64,

	probe_columns_first: bool,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let estimated_output_bytes = estimate_join_materialization_bytes(
		JoinKind::Inner,
		probe_batch,
		build_batch,
		selection.len(),
	);

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);

	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"reoriented join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut probe_indices = Vec::with_capacity(selection.len());

	let mut build_indices = Vec::with_capacity(selection.len());

	for row in selection {
		match *row {
			JoinOutputRow::Paired { left, right } => {
				build_indices.push(Some(left));

				probe_indices.push(Some(right));
			}

			_ => return Err(invalid_join_selection(JoinKind::Inner, row)),
		}
	}

	let probe_indices = UInt32Array::from(probe_indices);

	let build_indices = UInt32Array::from(build_indices);

	let mut columns = Vec::with_capacity(probe_batch.num_columns() + build_batch.num_columns());

	if probe_columns_first {
		for column in probe_batch.columns() {
			columns.push(arrow::compute::take(column, &probe_indices, None)?);
		}

		for column in build_batch.columns() {
			columns.push(arrow::compute::take(column, &build_indices, None)?);
		}
	} else {
		for column in build_batch.columns() {
			columns.push(arrow::compute::take(column, &build_indices, None)?);
		}

		for column in probe_batch.columns() {
			columns.push(arrow::compute::take(column, &probe_indices, None)?);
		}
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

pub(super) fn materialize_preserved_selection(
	preserved_batch: &RecordBatch,

	selection: &[u32],

	output_schema: &arrow::datatypes::SchemaRef,

	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if selection.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let estimated_output_bytes = join_take_index_bytes(selection.len()).saturating_add(
		estimate_selected_batch_bytes(preserved_batch, selection.len()),
	);

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);

	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"semi/anti preserved output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let indices = UInt32Array::from(selection.iter().copied().map(Some).collect::<Vec<_>>());

	let mut columns = Vec::with_capacity(preserved_batch.num_columns());

	for column in preserved_batch.columns() {
		columns.push(arrow::compute::take(column, &indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

pub(super) fn estimate_join_materialization_bytes(
	kind: JoinKind,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	output_rows: usize,
) -> u64 {
	let mut bytes = 0u64;

	match kind {
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(left_batch, output_rows))
				.saturating_add(estimate_selected_batch_bytes(right_batch, output_rows));
		}

		JoinKind::LeftSemi | JoinKind::LeftAnti => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(left_batch, output_rows));
		}

		JoinKind::RightSemi | JoinKind::RightAnti => {
			bytes = bytes
				.saturating_add(join_take_index_bytes(output_rows))
				.saturating_add(estimate_selected_batch_bytes(right_batch, output_rows));
		}
	}

	bytes
}

pub(super) fn join_output_materialization_memory_cap(memory_cap: u64) -> u64 {
	memory_cap.saturating_mul(JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER)
}

pub(super) fn join_take_index_bytes(rows: usize) -> u64 {
	let rows = u64::try_from(rows).unwrap_or(u64::MAX);

	rows.saturating_mul(std::mem::size_of::<u32>() as u64)
		.saturating_add(rows.saturating_add(7) / 8)
}

pub(super) fn join_interleave_index_bytes(rows: usize) -> u64 {
	let rows = u64::try_from(rows).unwrap_or(u64::MAX);

	rows.saturating_mul(std::mem::size_of::<(usize, usize)>() as u64)
}

pub(super) fn estimate_selected_batch_bytes(batch: &RecordBatch, output_rows: usize) -> u64 {
	batch
		.columns()
		.iter()
		.map(|column| estimate_selected_column_bytes(column, batch.num_rows(), output_rows))
		.fold(0u64, u64::saturating_add)
}

pub(super) fn estimate_spilled_inner_build_selection_bytes(
	build_pages: &[RecordBatch],

	output_rows: usize,
) -> u64 {
	let source_rows = build_pages
		.iter()
		.map(RecordBatch::num_rows)
		.fold(0usize, usize::saturating_add);

	let Some(first_page) = build_pages.first() else {
		return 0;
	};

	(0..first_page.num_columns())
		.map(|column_index| {
			let source_bytes = build_pages
				.iter()
				.map(|page| {
					u64::try_from(page.column(column_index).get_array_memory_size())
						.unwrap_or(u64::MAX)
				})
				.fold(0u64, u64::saturating_add);

			estimate_selected_bytes_from_source(
				source_bytes,
				source_rows,
				first_page.column(column_index).data_type(),
				output_rows,
			)
		})
		.fold(0u64, u64::saturating_add)
}

pub(super) fn estimate_selected_column_bytes(
	column: &ArrayRef,

	source_rows: usize,

	output_rows: usize,
) -> u64 {
	if output_rows == 0 {
		return 0;
	}

	let bytes_per_row = if source_rows == 0 {
		empty_source_column_row_width(column.data_type())
	} else {
		let source_bytes = u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX);

		let source_rows = u64::try_from(source_rows).unwrap_or(u64::MAX);

		source_bytes.saturating_add(source_rows.saturating_sub(1)) / source_rows
	};

	let output_rows = u64::try_from(output_rows).unwrap_or(u64::MAX);

	bytes_per_row.saturating_mul(output_rows)
}

pub(super) fn estimate_selected_bytes_from_source(
	source_bytes: u64,

	source_rows: usize,

	data_type: &DataType,

	output_rows: usize,
) -> u64 {
	if output_rows == 0 {
		return 0;
	}

	let bytes_per_row = if source_rows == 0 {
		empty_source_column_row_width(data_type)
	} else {
		let source_rows = u64::try_from(source_rows).unwrap_or(u64::MAX);

		source_bytes.saturating_add(source_rows.saturating_sub(1)) / source_rows
	};

	let output_rows = u64::try_from(output_rows).unwrap_or(u64::MAX);

	bytes_per_row.saturating_mul(output_rows)
}

pub(super) fn empty_source_column_row_width(data_type: &DataType) -> u64 {
	match data_type {
		DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,

		DataType::Int16 | DataType::UInt16 => 2,

		DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,

		DataType::Int64
		| DataType::UInt64
		| DataType::Float64
		| DataType::Timestamp(_, _)
		| DataType::Duration(_) => 8,

		DataType::Decimal128(_, _) => 16,

		DataType::Utf8 | DataType::Binary => 8,

		DataType::LargeUtf8 | DataType::LargeBinary => 16,

		_ => 16,
	}
}

pub(super) fn invalid_join_selection(kind: JoinKind, row: &JoinOutputRow) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("invalid {kind:?} join selection row: {row:?}"),
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]

pub(super) enum JoinKey {
	Int32(i32),

	Int64(i64),

	Generic(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]

pub(super) enum JoinKeyPlan {
	Int32,

	Int64,

	Generic,
}

impl JoinKeyPlan {
	pub(super) fn label(self) -> &'static str {
		match self {
			Self::Int32 => "int32",

			Self::Int64 => "int64",

			Self::Generic => "generic",
		}
	}

	pub(super) fn from_key_arrays(
		left_key_arrays: &[ArrayRef],

		right_key_arrays: &[ArrayRef],

		null_safe_keys: bool,
	) -> Self {
		if null_safe_keys {
			return Self::Generic;
		}

		if left_key_arrays.len() != 1 || right_key_arrays.len() != 1 {
			return Self::Generic;
		}

		if left_key_arrays[0].data_type() != right_key_arrays[0].data_type() {
			return Self::Generic;
		}

		match left_key_arrays[0].data_type() {
			DataType::Int32 => Self::Int32,

			DataType::Int64 => Self::Int64,

			_ => Self::Generic,
		}
	}

	pub(super) fn from_build_arrays_and_probe_schema(
		build_key_arrays: &[ArrayRef],

		probe_schema: &SchemaRef,

		probe_keys: &[crate::ir::ColumnOrdinal],

		null_safe_keys: bool,
	) -> Result<Self, ExecutionError> {
		if null_safe_keys {
			return Ok(Self::Generic);
		}

		if build_key_arrays.len() != 1 || probe_keys.len() != 1 {
			return Ok(Self::Generic);
		}

		let probe_index = usize::try_from(probe_keys[0].0).expect("column ordinal fits usize");

		let Some(probe_field) = probe_schema.fields().get(probe_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("probe join key ordinal {probe_index} out of bounds"),
			});
		};

		if build_key_arrays[0].data_type() != probe_field.data_type() {
			return Ok(Self::Generic);
		}

		Ok(match build_key_arrays[0].data_type() {
			DataType::Int32 => Self::Int32,

			DataType::Int64 => Self::Int64,

			_ => Self::Generic,
		})
	}

	pub(super) fn from_build_schema_and_probe_schema(
		build_schema: &SchemaRef,

		build_keys: &[crate::ir::ColumnOrdinal],

		probe_schema: &SchemaRef,

		probe_keys: &[crate::ir::ColumnOrdinal],

		null_safe_keys: bool,
	) -> Result<Self, ExecutionError> {
		if null_safe_keys {
			return Ok(Self::Generic);
		}

		if build_keys.len() != 1 || probe_keys.len() != 1 {
			return Ok(Self::Generic);
		}

		let build_index = usize::try_from(build_keys[0].0).expect("column ordinal fits usize");

		let probe_index = usize::try_from(probe_keys[0].0).expect("column ordinal fits usize");

		let Some(build_field) = build_schema.fields().get(build_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("build join key ordinal {build_index} out of bounds"),
			});
		};

		let Some(probe_field) = probe_schema.fields().get(probe_index) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("probe join key ordinal {probe_index} out of bounds"),
			});
		};

		if build_field.data_type() != probe_field.data_type() {
			return Ok(Self::Generic);
		}

		Ok(match build_field.data_type() {
			DataType::Int32 => Self::Int32,

			DataType::Int64 => Self::Int64,

			_ => Self::Generic,
		})
	}
}

pub(super) fn join_key_at(
	join_key_plan: JoinKeyPlan,

	key_arrays: &[ArrayRef],

	row: usize,

	null_safe_keys: bool,
) -> Result<Option<JoinKey>, ExecutionError> {
	Ok(match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = single_join_key_array::<Int32Array>(key_arrays, "Int32")?;

			if values.is_null(row) {
				None
			} else {
				Some(JoinKey::Int32(values.value(row)))
			}
		}

		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(key_arrays, "Int64")?;

			if values.is_null(row) {
				None
			} else {
				Some(JoinKey::Int64(values.value(row)))
			}
		}

		JoinKeyPlan::Generic if null_safe_keys => {
			Some(JoinKey::Generic(join_key_bytes_null_safe(key_arrays, row)?))
		}

		JoinKeyPlan::Generic => join_key_bytes(key_arrays, row)?.map(JoinKey::Generic),
	})
}

/// ADR 497: drive one primitive-key probe arm through the staged batch window

/// (or the historical one-key-at-a-time lookup under the ablation), leaving

/// per-row match handling identical to the scalar loop it replaces.

fn probe_primitive_key_rows<'table, K: Copy>(
	kind: JoinKind,

	rows: usize,

	key_at: impl Fn(usize) -> Option<K>,

	lookup_scalar: impl Fn(K) -> Option<&'table BuildRowList>,

	lookup_window: impl Fn(&[Option<K>], &mut Vec<Option<&'table BuildRowList>>),

	matched_build_rows: &mut Option<Vec<u64>>,

	selection: &mut Vec<JoinOutputRow>,

	residual: &mut JoinMatchResidual,

	mut profile: Option<&mut InMemoryProbeProfile>,
) -> Result<(), ExecutionError> {
	let batched = build_rows::batched_probe_prefetch_enabled();

	let window = if batched {
		build_rows::PROBE_BATCH_WINDOW
	} else {
		1
	};

	let mut window_keys = Vec::with_capacity(window);

	let mut window_lookups = Vec::with_capacity(window);

	let mut row = 0usize;

	while row < rows {
		let end = (row + window).min(rows);

		window_lookups.clear();

		let scalar_key = (!batched).then(|| key_at(row));

		if batched {
			window_keys.clear();

			window_keys.extend((row..end).map(&key_at));
		}

		if let Some(profile) = profile.as_deref_mut() {
			let keys = if batched {
				window_keys.iter().filter(|key| key.is_some()).count()
			} else {
				usize::from(scalar_key.flatten().is_some())
			};

			profile.record_lookup_window(keys);
		}

		let lookup_started = profile.as_ref().map(|_| Instant::now());

		if batched {
			lookup_window(&window_keys, &mut window_lookups);
		} else {
			window_lookups.push(scalar_key.flatten().and_then(&lookup_scalar));
		}

		if let (Some(profile), Some(started)) = (profile.as_deref_mut(), lookup_started) {
			profile.record_lookup_duration(started.elapsed());
		}

		let match_started = profile.as_ref().map(|_| Instant::now());

		for (offset, lookup) in window_lookups.iter().enumerate() {
			let row = row + offset;

			let matched = if let Some(matches) = lookup {
				append_join_matches_for_kind(
					kind,
					row,
					matches.rows(),
					matched_build_rows.as_deref_mut(),
					selection,
					residual,
				)?
			} else {
				false
			};

			if !matched {
				append_unmatched_probe_row(kind, row, selection)?;
			}
		}

		if let (Some(profile), Some(started)) = (profile.as_deref_mut(), match_started) {
			profile.record_match_expand_duration(started.elapsed());
		}

		row = end;
	}

	Ok(())
}

pub(super) fn probe_in_memory_join_rows(
	kind: JoinKind,

	join_key_plan: JoinKeyPlan,

	build_rows: &JoinBuildRows,

	left_rows: usize,

	right_key_arrays: &[ArrayRef],

	rows: usize,

	match_filter: Option<&BoundExprProgram>,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	null_safe_keys: bool,
) -> Result<Vec<JoinOutputRow>, ExecutionError> {
	probe_in_memory_join_rows_profiled(
		kind,
		join_key_plan,
		build_rows,
		left_rows,
		right_key_arrays,
		rows,
		match_filter,
		left_batch,
		right_batch,
		null_safe_keys,
		None,
	)
}

#[derive(Default)]

pub(super) struct InMemoryProbeProfile {
	pub(super) lookup: std::time::Duration,

	pub(super) match_expand: std::time::Duration,

	pub(super) lookup_windows: u64,

	pub(super) non_null_keys: u64,

	pub(super) supported: bool,

	pub(super) error: Option<&'static str>,
}

impl InMemoryProbeProfile {
	fn record_lookup_window(&mut self, non_null_keys: usize) {
		self.supported = true;

		let Some(lookup_windows) = self.lookup_windows.checked_add(1) else {
			self.error = Some("suffix lookup-window count overflowed");

			return;
		};

		let Ok(non_null_keys) = u64::try_from(non_null_keys) else {
			self.error = Some("suffix non-null-key count overflowed");

			return;
		};

		let Some(total_non_null_keys) = self.non_null_keys.checked_add(non_null_keys) else {
			self.error = Some("suffix non-null-key count overflowed");

			return;
		};

		self.lookup_windows = lookup_windows;

		self.non_null_keys = total_non_null_keys;
	}

	fn record_lookup_duration(&mut self, elapsed: std::time::Duration) {
		let Some(total) = self.lookup.checked_add(elapsed) else {
			self.error = Some("suffix lookup duration overflowed");

			return;
		};

		self.lookup = total;
	}

	fn record_match_expand_duration(&mut self, elapsed: std::time::Duration) {
		let Some(total) = self.match_expand.checked_add(elapsed) else {
			self.error = Some("suffix match-expand duration overflowed");

			return;
		};

		self.match_expand = total;
	}
}

#[allow(clippy::too_many_arguments)]

pub(super) fn probe_in_memory_join_rows_profiled(
	kind: JoinKind,

	join_key_plan: JoinKeyPlan,

	build_rows: &JoinBuildRows,

	left_rows: usize,

	right_key_arrays: &[ArrayRef],

	rows: usize,

	match_filter: Option<&BoundExprProgram>,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	null_safe_keys: bool,

	mut profile: Option<&mut InMemoryProbeProfile>,
) -> Result<Vec<JoinOutputRow>, ExecutionError> {
	let mut selection = Vec::new();

	let mut matched_build_rows = build_match_bitmap(kind, left_rows);

	let mut residual = JoinMatchResidual::new(match_filter, left_batch, right_batch);

	match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = single_join_key_array::<Int32Array>(right_key_arrays, "Int32")?;

			probe_primitive_key_rows(
				kind,
				rows,
				|row| (!values.is_null(row)).then(|| values.value(row)),
				|key| build_rows.get_int32(key),
				|keys, out| build_rows.get_int32_batch(keys, out),
				&mut matched_build_rows,
				&mut selection,
				&mut residual,
				profile.as_deref_mut(),
			)?;
		}

		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(right_key_arrays, "Int64")?;

			probe_primitive_key_rows(
				kind,
				rows,
				|row| (!values.is_null(row)).then(|| values.value(row)),
				|key| build_rows.get_int64(key),
				|keys, out| build_rows.get_int64_batch(keys, out),
				&mut matched_build_rows,
				&mut selection,
				&mut residual,
				profile.as_deref_mut(),
			)?;
		}

		JoinKeyPlan::Generic => {
			if let Some(profile) = profile.as_deref_mut() {
				profile.supported = false;
			}

			for row in 0..rows {
				let key = if null_safe_keys {
					join_key_bytes_null_safe(right_key_arrays, row)?
				} else {
					let Some(key) = join_key_bytes(right_key_arrays, row)? else {
						append_unmatched_probe_row(kind, row, &mut selection)?;

						continue;
					};

					key
				};

				let key = JoinKey::Generic(key);

				let matched = if let Some(matches) = build_rows.get(&key) {
					append_join_matches_for_kind(
						kind,
						row,
						matches.rows(),
						matched_build_rows.as_deref_mut(),
						&mut selection,
						&mut residual,
					)?
				} else {
					false
				};

				if !matched {
					append_unmatched_probe_row(kind, row, &mut selection)?;
				}
			}
		}
	}

	append_build_side_after_probe_rows(
		kind,
		left_rows,
		matched_build_rows.as_deref(),
		&mut selection,
	)?;

	Ok(selection)
}

pub(super) fn single_join_key_array<'a, T: 'static>(
	key_arrays: &'a [ArrayRef],

	label: &'static str,
) -> Result<&'a T, ExecutionError> {
	key_arrays[0]
		.as_any()
		.downcast_ref::<T>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("join key array is not {label}"),
		})
}

#[inline(always)]

pub(super) fn insert_join_build_row(build_rows: &mut JoinBuildRows, key: JoinKey, row: u32) {
	build_rows.insert(key, row);
}

pub(super) fn append_join_matches_for_kind(
	kind: JoinKind,

	probe_row: usize,

	matches: impl Iterator<Item = u32>,

	mut matched_build_rows: Option<&mut [u64]>,

	selection: &mut Vec<JoinOutputRow>,

	residual: &mut JoinMatchResidual<'_>,
) -> Result<bool, ExecutionError> {
	let right_index = join_probe_row_index(probe_row)?;

	let mut matched = false;

	for left_index in matches {
		if !residual.matches(left_index, right_index)? {
			continue;
		}

		matched = true;

		if let Some(bitmap) = matched_build_rows.as_deref_mut() {
			set_build_row_matched(bitmap, left_index);
		}

		match kind {
			JoinKind::Inner
			| JoinKind::Cross
			| JoinKind::LeftOuter
			| JoinKind::RightOuter
			| JoinKind::FullOuter => selection.push(JoinOutputRow::Paired {
				left: left_index,

				right: right_index,
			}),

			JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::RightSemi | JoinKind::RightAnti => {
			}
		}
	}

	if matched && kind == JoinKind::RightSemi {
		selection.push(JoinOutputRow::RightPreserved { right: right_index });
	}

	Ok(matched)
}

pub(super) struct JoinMatchResidual<'a> {
	program: Option<&'a BoundExprProgram>,

	left_batch: &'a RecordBatch,

	right_batch: &'a RecordBatch,

	schema: Option<SchemaRef>,
}

impl<'a> JoinMatchResidual<'a> {
	pub(super) fn new(
		program: Option<&'a BoundExprProgram>,

		left_batch: &'a RecordBatch,

		right_batch: &'a RecordBatch,
	) -> Self {
		let schema = program.map(|_| {
			let fields = left_batch
				.schema()
				.fields()
				.iter()
				.chain(right_batch.schema().fields().iter())
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>();

			Arc::new(Schema::new(fields))
		});

		Self {
			program,

			left_batch,

			right_batch,

			schema,
		}
	}

	pub(super) fn matches(
		&mut self,

		left_row: u32,

		right_row: u32,
	) -> Result<bool, ExecutionError> {
		let Some(program) = self.program else {
			return Ok(true);
		};

		let batch = join_match_pair_batch(
			self.left_batch,
			self.right_batch,
			left_row,
			right_row,
			self.schema
				.as_ref()
				.expect("residual schema exists when program exists")
				.clone(),
		)?;

		let mut roots =
			Executor::evaluate_bound_program_without_storage(program, &batch)?.into_iter();

		let Some(root) = roots.next() else {
			return Err(ExecutionError::InvalidExpression {
				message: "JOIN match filter must have exactly one root".to_string(),
			});
		};

		if roots.next().is_some() {
			return Err(ExecutionError::InvalidExpression {
				message: "JOIN match filter must have exactly one root".to_string(),
			});
		}

		let bools = root
			.as_any()
			.downcast_ref::<BooleanArray>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: "JOIN match filter must return Boolean".to_string(),
			})?;

		Ok(bools.is_valid(0) && bools.value(0))
	}
}

pub(super) fn join_match_pair_batch(
	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	left_row: u32,

	right_row: u32,

	schema: SchemaRef,
) -> Result<RecordBatch, ExecutionError> {
	let left_index = UInt32Array::from(vec![left_row]);

	let right_index = UInt32Array::from(vec![right_row]);

	let mut columns = Vec::with_capacity(left_batch.num_columns() + right_batch.num_columns());

	for column in left_batch.columns() {
		columns.push(take(column, &left_index, None)?);
	}

	for column in right_batch.columns() {
		columns.push(take(column, &right_index, None)?);
	}

	Ok(RecordBatch::try_new(schema, columns)?)
}

pub(super) fn append_unmatched_probe_row(
	kind: JoinKind,

	probe_row: usize,

	selection: &mut Vec<JoinOutputRow>,
) -> Result<(), ExecutionError> {
	let right_index = join_probe_row_index(probe_row)?;

	match kind {
		JoinKind::RightOuter | JoinKind::FullOuter => {
			selection.push(JoinOutputRow::RightFillNull { right: right_index });
		}

		JoinKind::RightAnti => {
			selection.push(JoinOutputRow::RightPreserved { right: right_index });
		}

		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::LeftSemi
		| JoinKind::RightSemi
		| JoinKind::LeftAnti => {}
	}

	Ok(())
}

pub(super) fn append_build_side_after_probe_rows(
	kind: JoinKind,

	left_rows: usize,

	matched_build_rows: Option<&[u64]>,

	selection: &mut Vec<JoinOutputRow>,
) -> Result<(), ExecutionError> {
	let Some(bitmap) = matched_build_rows else {
		return Ok(());
	};

	for row in 0..left_rows {
		let matched = build_row_matched(bitmap, row);

		match kind {
			JoinKind::LeftOuter | JoinKind::FullOuter if !matched => {
				selection.push(JoinOutputRow::LeftFillNull {
					left: join_build_row_index(row)?,
				});
			}

			JoinKind::LeftSemi if matched => {
				selection.push(JoinOutputRow::LeftPreserved {
					left: join_build_row_index(row)?,
				});
			}

			JoinKind::LeftAnti if !matched => {
				selection.push(JoinOutputRow::LeftPreserved {
					left: join_build_row_index(row)?,
				});
			}

			_ => {}
		}
	}

	Ok(())
}

pub(super) fn build_match_bitmap(kind: JoinKind, rows: usize) -> Option<Vec<u64>> {
	match kind {
		JoinKind::LeftOuter | JoinKind::FullOuter | JoinKind::LeftSemi | JoinKind::LeftAnti => {
			Some(vec![0; rows.div_ceil(64)])
		}

		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::RightOuter
		| JoinKind::RightSemi
		| JoinKind::RightAnti => None,
	}
}

pub(super) fn set_build_row_matched(bitmap: &mut [u64], row: u32) {
	let row = row as usize;

	let word = row / 64;

	let bit = row % 64;

	if let Some(slot) = bitmap.get_mut(word) {
		*slot |= 1u64 << bit;
	}
}

pub(super) fn build_row_matched(bitmap: &[u64], row: usize) -> bool {
	let word = row / 64;

	let bit = row % 64;

	bitmap
		.get(word)
		.is_some_and(|slot| (*slot & (1u64 << bit)) != 0)
}

pub(super) fn join_build_row_index(row: usize) -> Result<u32, ExecutionError> {
	u32::try_from(row).map_err(|_| ExecutionError::Kernel {
		message: "join build row index exceeds UInt32 take index".to_string(),
	})
}

pub(super) fn join_probe_row_index(row: usize) -> Result<u32, ExecutionError> {
	u32::try_from(row).map_err(|_| ExecutionError::Kernel {
		message: "join probe row index exceeds UInt32 take index".to_string(),
	})
}

#[inline(always)]

pub(super) fn join_build_memory_fits(
	key_arrays: &[ArrayRef],

	rows: usize,

	memory_cap: u64,
) -> bool {
	estimated_build_hash_bytes(key_arrays, rows) <= memory_cap
}

pub(super) fn estimated_build_hash_bytes_for_batches(
	batches: &[RecordBatch],

	keys: &[crate::ir::ColumnOrdinal],
) -> Result<u64, ExecutionError> {
	let mut bytes = 0u64;

	for batch in batches {
		let key_arrays = key_arrays(batch, keys)?;

		bytes = bytes.saturating_add(estimated_build_hash_bytes(&key_arrays, batch.num_rows()));
	}

	Ok(bytes)
}
