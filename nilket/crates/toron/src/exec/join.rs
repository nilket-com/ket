use super::*;

mod build_profile;
mod build_rows;
mod columns;
mod global_fold_profile;
mod grouped_chain_profile;
mod membership_chain_profile;
mod residency_profile;
mod spill;
mod spill_placement;
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
	aggr_expr.iter().all(|call| match call.function {
		AggregateFunction::Sum | AggregateFunction::Avg => call.args.roots.iter().all(|root| {
			!matches!(
				call.args.node_type(*root),
				Some(DataType::Float32 | DataType::Float64)
			)
		}),
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Min
		| AggregateFunction::Max => true,
		AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => false,
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
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
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
	let Some(build_rows) = build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	)?
	else {
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
	};
	let build_micros = duration_micros(build_started);
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task.phases.build_micros.saturating_add(build_micros);
	});
	record_join_build_profile(
		&telemetry,
		replay_id,
		strategy,
		join_key_plan,
		&build_rows,
		build_micros,
	);

	let probe_started = Instant::now();
	let selection = probe_in_memory_join_rows(
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
	)?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		task.counters.matched_rows = task
			.counters
			.matched_rows
			.saturating_add(selection.len() as u64);
	});

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
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
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

	let build_started = Instant::now();
	if let Some((mut spilled, key_plan)) = try_create_streamed_spilled_inner_build(
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
	.await?
	{
		let attributed_spill_bytes = spilled.spill_bytes();
		record_task_telemetry(&telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(spilled.input_row_count());
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});
		let probe_started = Instant::now();
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
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			if let Ok((probe_rows, output_rows)) = &result {
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);
				task.counters.matched_rows =
					task.counters.matched_rows.saturating_add(*output_rows);
			}
		});
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
	let Some(build_rows) = build_in_memory_join_rows(
		join_key_plan,
		&left_key_arrays,
		left_batch.num_rows(),
		task_budget.memory.get(),
		null_safe_keys,
	)?
	else {
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
	};
	let build_micros = duration_micros(build_started);
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task.phases.build_micros.saturating_add(build_micros);
	});
	record_join_build_profile(
		&telemetry,
		replay_id,
		strategy,
		join_key_plan,
		&build_rows,
		build_micros,
	);

	let probe_started = Instant::now();
	let selection = probe_in_memory_join_rows(
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
	)?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		task.counters.matched_rows = task
			.counters
			.matched_rows
			.saturating_add(selection.len() as u64);
	});

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
		if batched {
			window_keys.clear();
			window_keys.extend((row..end).map(&key_at));
			lookup_window(&window_keys, &mut window_lookups);
		} else {
			window_lookups.push(key_at(row).and_then(&lookup_scalar));
		}
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
			)?;
		}
		JoinKeyPlan::Generic => {
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn try_execute_streamed_spilled_left_semi_join(
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
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	join_runtime_observation_mode: JoinRuntimeObservationMode,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
	if kind != JoinKind::LeftSemi || match_filter.is_some() {
		return Ok(None);
	}
	let estimated_build_bytes = estimated_build_hash_bytes_for_batches(left_batches, left_keys)?;
	if estimated_build_bytes <= task_budget.memory.get() {
		return Ok(None);
	}

	let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
		&left_schema,
		left_keys,
		&right_schema,
		right_keys,
		null_safe_keys,
	)?;
	let mut spilled = SpilledInnerJoinBuild::create(task_budget, spill_file_ops, spill_dir).await?;
	spilled.use_compact_resident_index();
	let build_started = Instant::now();
	for batch in left_batches {
		spilled
			.write_batch_chunks(batch, key_plan, left_keys, null_safe_keys, task_budget)
			.await?;
	}
	spilled.finish(task_budget).await?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(spilled.input_row_count());
		task.phases.spill_micros = task
			.phases
			.spill_micros
			.saturating_add(duration_micros(build_started));
		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});

	let probe_started = Instant::now();
	let result = probe_streamed_spilled_left_semi_join(
		&mut spilled,
		right_batches,
		right_keys,
		key_plan,
		null_safe_keys,
		output_schema,
		task_budget.memory.get(),
	)
	.await;
	let build_rows = spilled.input_row_count();
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		if let Ok((_, probe_rows, output_rows)) = &result {
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(*output_rows);
		}
	});
	let result = result.map(|(batches, probe_rows, output_rows)| {
		record_join_runtime_observation(
			&telemetry,
			&join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id,
				strategy,
				build_rows: RowCount::new(build_rows),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);
		batches
	});
	spilled.cleanup().await;
	result.map(Some)
}

pub(super) async fn probe_streamed_spilled_left_semi_join(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<(Vec<RecordBatch>, u64, u64), ExecutionError> {
	let mut matched: BTreeMap<usize, BTreeSet<u32>> = BTreeMap::new();
	let mut probe_rows = 0u64;
	for batch in right_batches {
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let probe_chunk = batch.slice(offset, len);
			probe_rows = probe_rows.saturating_add(probe_chunk.num_rows() as u64);
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			for row in 0..probe_chunk.num_rows() {
				let Some(key) = join_key_at(key_plan, &right_key_arrays, row, null_safe_keys)?
				else {
					continue;
				};
				for build in spilled.lookup(&key).await? {
					matched
						.entry(build.page_index)
						.or_default()
						.insert(build.row);
				}
			}
		}
	}

	let mut output = Vec::new();
	let mut refs = Vec::new();
	let mut output_rows = 0u64;
	for (page_index, rows) in matched {
		for row in rows {
			refs.push(SpilledBatchRowRef { page_index, row });
			if refs.len() == CROSS_JOIN_OUTPUT_CHUNK_ROWS {
				let batch = materialize_streamed_spilled_build_rows(
					spilled,
					&refs,
					output_schema,
					memory_cap,
				)
				.await?;
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				if batch.num_rows() > 0 {
					output.push(batch);
				}
				refs.clear();
			}
		}
	}
	if !refs.is_empty() {
		let batch =
			materialize_streamed_spilled_build_rows(spilled, &refs, output_schema, memory_cap)
				.await?;
		output_rows = output_rows.saturating_add(batch.num_rows() as u64);
		if batch.num_rows() > 0 {
			output.push(batch);
		}
	}
	if output.is_empty() {
		output.push(RecordBatch::new_empty(output_schema.clone()));
	}
	Ok((output, probe_rows, output_rows))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn try_execute_partitioned_spilled_left_outer_join(
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
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	join_runtime_observation_mode: JoinRuntimeObservationMode,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
	if kind != JoinKind::LeftOuter || match_filter.is_some() {
		return Ok(None);
	}
	let estimated_build_bytes = estimated_build_hash_bytes_for_batches(left_batches, left_keys)?;
	if estimated_build_bytes <= task_budget.memory.get() {
		return Ok(None);
	}

	let partition_count =
		streamed_spill_partition_count(estimated_build_bytes, task_budget.memory.get())?;
	let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
		&left_schema,
		left_keys,
		&right_schema,
		right_keys,
		null_safe_keys,
	)?;
	let left_partition_buckets = left_batches
		.iter()
		.map(|batch| {
			precompute_join_partition_buckets(
				batch,
				left_keys,
				partition_count,
				null_safe_keys,
				true,
			)
		})
		.collect::<Result<Vec<_>, _>>()?;
	let right_partition_buckets = right_batches
		.iter()
		.map(|batch| {
			precompute_join_partition_buckets(
				batch,
				right_keys,
				partition_count,
				null_safe_keys,
				false,
			)
		})
		.collect::<Result<Vec<_>, _>>()?;
	let mut output = Vec::new();
	let output_cap = join_output_materialization_memory_cap(task_budget.memory.get());
	let mut output_bytes = 0u64;
	let mut total_build_rows = 0u64;
	let mut total_probe_rows = 0u64;
	let mut total_output_rows = 0u64;

	for partition in 0..partition_count {
		let mut spilled =
			SpilledInnerJoinBuild::create(task_budget, spill_file_ops.clone(), spill_dir.clone())
				.await?;
		let build_started = Instant::now();
		for (batch, buckets) in left_batches.iter().zip(&left_partition_buckets) {
			let partitioned = select_join_partition_batch_with_buckets(batch, buckets, partition)?;
			spilled
				.write_preserved_batch_chunks(
					&partitioned,
					key_plan,
					left_keys,
					null_safe_keys,
					task_budget,
				)
				.await?;
		}
		spilled.finish(task_budget).await?;
		let build_rows = spilled.input_row_count();
		total_build_rows = total_build_rows.saturating_add(build_rows);
		record_task_telemetry(&telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.counters.build_rows = task.counters.build_rows.saturating_add(build_rows);
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});

		let probe_started = Instant::now();
		let mut visit = |batch: RecordBatch| -> Result<(), ExecutionError> {
			if batch.num_rows() == 0 {
				return Ok(());
			}
			output_bytes = output_bytes.saturating_add(record_batch_memory_bytes(&batch));
			if output_bytes > output_cap {
				return Err(budget_exhausted(
					BudgetDimension::Memory,
					"partitioned left outer output accumulator",
					output_bytes,
					output_cap,
				));
			}
			total_output_rows = total_output_rows.saturating_add(batch.num_rows() as u64);
			output.push(batch);
			Ok(())
		};
		let (probe_rows, output_rows) = visit_partitioned_spilled_left_outer_join_chunks(
			&mut spilled,
			right_batches,
			&right_partition_buckets,
			right_keys,
			partition,
			key_plan,
			null_safe_keys,
			&right_schema,
			output_schema,
			task_budget.memory.get(),
			&mut visit,
		)
		.await?;
		total_probe_rows = total_probe_rows.saturating_add(probe_rows);
		record_task_telemetry(&telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
		});
		spilled.cleanup().await;
	}

	if output.is_empty() {
		output.push(RecordBatch::new_empty(output_schema.clone()));
	}
	if output.len() > 1 {
		output = vec![concat_batches(output_schema, &output)?];
	}
	record_join_runtime_observation(
		&telemetry,
		&join_runtime_observation_mode,
		JoinRuntimeObservation {
			replay_id,
			strategy,
			build_rows: RowCount::new(total_build_rows),
			probe_rows: RowCount::new(total_probe_rows),
			output_rows: RowCount::new(total_output_rows),
		},
	);
	Ok(Some(output))
}

pub(super) fn streamed_spill_partition_count(
	estimated_build_bytes: u64,
	memory_cap: u64,
) -> Result<u32, ExecutionError> {
	let target = (memory_cap / 2).max(1);
	let needed = estimated_build_bytes.saturating_add(target.saturating_sub(1)) / target;
	let needed = needed.max(1);
	let partitions = needed.next_power_of_two().min(1024);
	u32::try_from(partitions).map_err(|_| ExecutionError::Kernel {
		message: "streamed spill partition count exceeds u32".to_string(),
	})
}

pub(super) fn select_join_partition_batch(
	batch: &RecordBatch,
	keys: &[crate::ir::ColumnOrdinal],
	partition_count: u32,
	partition: u32,
	null_safe_keys: bool,
	retain_unkeyed_rows: bool,
) -> Result<RecordBatch, ExecutionError> {
	let key_arrays = key_arrays(batch, keys)?;
	let mut scratch = Vec::new();
	let mut indices = Vec::new();
	for row in 0..batch.num_rows() {
		let row_partition = if null_safe_keys {
			Some(
				u32::try_from(partition_bucket_with_scratch(
					&key_arrays,
					row,
					partition_count,
					&mut scratch,
				)?)
				.expect("partition bucket fits u32"),
			)
		} else if let Some(bucket) = partition_bucket_if_non_null_with_scratch(
			&key_arrays,
			row,
			partition_count,
			&mut scratch,
		)? {
			Some(u32::try_from(bucket).expect("partition bucket fits u32"))
		} else if retain_unkeyed_rows {
			Some(0)
		} else {
			None
		};
		if row_partition == Some(partition) {
			indices.push(Some(join_probe_row_index(row)?));
		}
	}

	if indices.is_empty() {
		return Ok(RecordBatch::new_empty(batch.schema()));
	}
	let indices = UInt32Array::from(indices);
	let columns = batch
		.columns()
		.iter()
		.map(|column| take(column, &indices, None))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

pub(super) fn precompute_join_partition_buckets(
	batch: &RecordBatch,
	keys: &[crate::ir::ColumnOrdinal],
	partition_count: u32,
	null_safe_keys: bool,
	retain_unkeyed_rows: bool,
) -> Result<Vec<Option<u32>>, ExecutionError> {
	let key_arrays = key_arrays(batch, keys)?;
	let mut scratch = Vec::new();
	let mut buckets = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		let bucket = if null_safe_keys {
			let bucket =
				partition_bucket_with_scratch(&key_arrays, row, partition_count, &mut scratch)?;
			Some(u32::try_from(bucket).expect("partition bucket fits u32"))
		} else if let Some(bucket) = partition_bucket_if_non_null_with_scratch(
			&key_arrays,
			row,
			partition_count,
			&mut scratch,
		)? {
			Some(u32::try_from(bucket).expect("partition bucket fits u32"))
		} else if retain_unkeyed_rows {
			Some(0)
		} else {
			None
		};
		buckets.push(bucket);
	}
	Ok(buckets)
}

pub(super) fn select_join_partition_batch_with_buckets(
	batch: &RecordBatch,
	buckets: &[Option<u32>],
	partition: u32,
) -> Result<RecordBatch, ExecutionError> {
	if buckets.len() != batch.num_rows() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"partition bucket count {} did not match batch rows {}",
				buckets.len(),
				batch.num_rows()
			),
		});
	}

	let mut indices = Vec::new();
	for (row, row_partition) in buckets.iter().enumerate() {
		if *row_partition == Some(partition) {
			indices.push(Some(join_probe_row_index(row)?));
		}
	}
	if indices.is_empty() {
		return Ok(RecordBatch::new_empty(batch.schema()));
	}
	let indices = UInt32Array::from(indices);
	let columns = batch
		.columns()
		.iter()
		.map(|column| take(column, &indices, None))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn visit_partitioned_spilled_left_outer_join_chunks<F>(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_partition_buckets: &[Vec<Option<u32>>],
	right_keys: &[crate::ir::ColumnOrdinal],
	partition: u32,
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	right_schema: &SchemaRef,
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
	visit_batch: &mut F,
) -> Result<(u64, u64), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let mut matched = spilled.empty_page_match_bitmap();
	let mut probe_rows = 0u64;
	let mut output_rows = 0u64;
	if right_partition_buckets.len() != right_batches.len() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"right partition bucket batch count {} did not match right batch count {}",
				right_partition_buckets.len(),
				right_batches.len()
			),
		});
	}
	for (batch, buckets) in right_batches.iter().zip(right_partition_buckets) {
		let partitioned = select_join_partition_batch_with_buckets(batch, buckets, partition)?;
		if partitioned.num_rows() == 0 {
			continue;
		}
		probe_rows = probe_rows.saturating_add(partitioned.num_rows() as u64);
		let right_key_arrays = key_arrays(&partitioned, right_keys)?;
		for offset in (0..partitioned.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(partitioned.num_rows() - offset);
			let probe_chunk = partitioned.slice(offset, len);
			let right_key_arrays = right_key_arrays
				.iter()
				.map(|array| array.slice(offset, len))
				.collect::<Vec<_>>();
			let mut matches = Vec::new();
			for row in 0..probe_chunk.num_rows() {
				let Some(key) = join_key_at(key_plan, &right_key_arrays, row, null_safe_keys)?
				else {
					continue;
				};
				for build in spilled.lookup(&key).await? {
					spilled.mark_page_row_matched(&mut matched, build);
					matches.push(SpilledInnerJoinMatch {
						build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let batch = materialize_streamed_spilled_left_outer_matches(
					spilled,
					&probe_chunk,
					chunk,
					output_schema,
					memory_cap,
				)
				.await?;
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
			}
		}
	}

	let mut unmatched = Vec::new();
	for page_index in 0..spilled.page_count() {
		let rows = spilled.page_row_count(page_index)?;
		for row in 0..rows {
			if spilled.page_row_matched(&matched, page_index, row as usize) {
				continue;
			}
			unmatched.push(SpilledBatchRowRef {
				page_index,
				row: join_build_row_index(row as usize)?,
			});
			if unmatched.len() == CROSS_JOIN_OUTPUT_CHUNK_ROWS {
				let batch = materialize_streamed_spilled_left_outer_unmatched(
					spilled,
					&unmatched,
					right_schema,
					output_schema,
					memory_cap,
				)
				.await?;
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
				unmatched.clear();
			}
		}
	}
	if !unmatched.is_empty() {
		let batch = materialize_streamed_spilled_left_outer_unmatched(
			spilled,
			&unmatched,
			right_schema,
			output_schema,
			memory_cap,
		)
		.await?;
		output_rows = output_rows.saturating_add(batch.num_rows() as u64);
		visit_batch(batch)?;
	}

	Ok((probe_rows, output_rows))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn try_execute_streamed_spilled_inner_join(
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
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	join_runtime_observation_mode: JoinRuntimeObservationMode,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
	let build_started = Instant::now();
	let Some((mut spilled, key_plan)) = try_create_streamed_spilled_inner_build(
		left_schema,
		right_schema,
		left_batches,
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys,
		task_budget,
		spill_file_ops,
		spill_dir,
	)
	.await?
	else {
		return Ok(None);
	};
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(spilled.input_row_count());
		task.phases.spill_micros = task
			.phases
			.spill_micros
			.saturating_add(duration_micros(build_started));
		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});

	let probe_started = Instant::now();
	let result = probe_streamed_spilled_inner_join(
		&mut spilled,
		right_batches,
		right_keys,
		key_plan,
		null_safe_keys,
		output_schema,
		task_budget.memory.get(),
	);
	let result = result.await;
	let build_rows = spilled.input_row_count();
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		if let Ok((_, probe_rows, output_rows)) = &result {
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(*output_rows);
		}
	});
	let result = result.map(|(batches, probe_rows, output_rows)| {
		record_join_runtime_observation(
			&telemetry,
			&join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id,
				strategy,
				build_rows: RowCount::new(build_rows),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);
		batches
	});
	spilled.cleanup().await;
	result.map(Some)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn try_create_streamed_spilled_inner_build(
	left_schema: arrow::datatypes::SchemaRef,
	right_schema: arrow::datatypes::SchemaRef,
	left_batches: &[RecordBatch],
	kind: JoinKind,
	left_keys: &[crate::ir::ColumnOrdinal],
	right_keys: &[crate::ir::ColumnOrdinal],
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	task_budget: TaskBudget,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<Option<(SpilledInnerJoinBuild, JoinKeyPlan)>, ExecutionError> {
	if kind != JoinKind::Inner || match_filter.is_some() {
		return Ok(None);
	}
	let estimated_build_bytes = estimated_build_hash_bytes_for_batches(left_batches, left_keys)?;
	if estimated_build_bytes <= task_budget.memory.get() {
		return Ok(None);
	}

	let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
		&left_schema,
		left_keys,
		&right_schema,
		right_keys,
		null_safe_keys,
	)?;
	let mut spilled = SpilledInnerJoinBuild::create(task_budget, spill_file_ops, spill_dir).await?;
	spilled.use_compact_resident_index();
	for batch in left_batches {
		spilled
			.write_batch_chunks(batch, key_plan, left_keys, null_safe_keys, task_budget)
			.await?;
	}
	spilled.finish(task_budget).await?;
	Ok(Some((spilled, key_plan)))
}

pub(super) async fn probe_streamed_spilled_inner_join(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<(Vec<RecordBatch>, u64, u64), ExecutionError> {
	let mut output = Vec::new();
	let (probe_rows, output_rows) = streaming::visit_streamed_spilled_inner_join_chunks(
		spilled,
		right_batches,
		right_keys,
		key_plan,
		null_safe_keys,
		output_schema,
		memory_cap,
		|batch| {
			if batch.num_rows() > 0 {
				output.push(batch);
			}
			Ok(())
		},
	)
	.await?;
	if output.is_empty() {
		output.push(RecordBatch::new_empty(output_schema.clone()));
	}
	Ok((output, probe_rows, output_rows))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn visit_streamed_spilled_inner_join_filtered_chunks<F>(
	executor: &Executor,
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: &BoundExprProgram,
	memory_cap: u64,
	mut visit_filtered_batch: F,
) -> Result<(u64, u64), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	streaming::visit_streamed_spilled_inner_join_chunks(
		spilled,
		right_batches,
		right_keys,
		key_plan,
		null_safe_keys,
		join_output_schema,
		memory_cap,
		|batch| {
			let batch = executor.filter_batch_with_bound_predicate(
				filter_predicate,
				batch,
				"filter predicate",
			)?;
			if batch.num_rows() > 0 {
				visit_filtered_batch(batch)?;
			}
			Ok(())
		},
	)
	.await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn fold_streamed_spilled_inner_join_into_global_aggregate(
	executor: &Executor,
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
	aggregate_state: &mut [AggState],
	memory_cap: u64,
) -> Result<(u64, u64), ExecutionError> {
	streaming::visit_streamed_spilled_inner_join_chunks(
		spilled,
		right_batches,
		right_keys,
		key_plan,
		null_safe_keys,
		join_output_schema,
		memory_cap,
		|batch| {
			let batch = if let Some(predicate) = filter_predicate {
				executor.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
			} else {
				batch
			};
			executor.fold_global_aggregate_state(aggregate_state, aggr_expr, &batch)?;
			Ok(())
		},
	)
	.await
}

pub(super) async fn materialize_streamed_spilled_build_rows(
	spilled: &mut SpilledInnerJoinBuild,
	refs: &[SpilledBatchRowRef],
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if refs.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in refs {
		page_sources.entry(row.page_index).or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let mut build_pages = Vec::with_capacity(page_sources.len());
	for page_index in page_indices {
		build_pages.push(spilled.read_batch_page(page_index).await?);
	}

	let estimated_output_bytes = join_interleave_index_bytes(refs.len()).saturating_add(
		estimate_spilled_inner_build_selection_bytes(&build_pages, refs.len()),
	);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"streamed preserved join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(refs.len());
	for row in refs {
		let source_index =
			*page_sources
				.get(&row.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"streamed preserved join missing spilled page source {}",
						row.page_index
					),
				})?;
		build_indices.push((source_index, row.row as usize));
	}

	let mut columns = Vec::with_capacity(
		build_pages
			.first()
			.map(RecordBatch::num_columns)
			.unwrap_or(0),
	);
	for column_index in 0..build_pages
		.first()
		.map(RecordBatch::num_columns)
		.unwrap_or(0)
	{
		let source_columns = build_pages
			.iter()
			.map(|batch| batch.column(column_index).as_ref())
			.collect::<Vec<_>>();
		columns.push(interleave(&source_columns, &build_indices)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

pub(super) async fn materialize_projected_streamed_spilled_build_rows(
	spilled: &mut SpilledInnerJoinBuild,
	refs: &[SpilledBatchRowRef],
	output_schema: &arrow::datatypes::SchemaRef,
	retained_columns: &[ColumnOrdinal],
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if refs.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in refs {
		page_sources.entry(row.page_index).or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let mut build_pages = Vec::with_capacity(page_sources.len());
	for page_index in page_indices {
		build_pages.push(spilled.read_batch_page(page_index).await?);
	}

	let estimated_output_bytes = join_interleave_index_bytes(refs.len()).saturating_add(
		estimate_spilled_inner_build_selection_bytes(&build_pages, refs.len()),
	);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"projected streamed preserved join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(refs.len());
	for row in refs {
		let source_index =
			*page_sources
				.get(&row.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"projected streamed preserved join missing spilled page source {}",
						row.page_index
					),
				})?;
		build_indices.push((source_index, row.row as usize));
	}

	let retained_positions = retained_columns
		.iter()
		.enumerate()
		.map(|(position, ordinal)| (ordinal.0 as usize, position))
		.collect::<BTreeMap<_, _>>();
	let mut columns = Vec::with_capacity(output_schema.fields().len());
	for (column_index, field) in output_schema.fields().iter().enumerate() {
		if let Some(retained_position) = retained_positions.get(&column_index).copied() {
			let source_columns = build_pages
				.iter()
				.map(|batch| batch.column(retained_position).as_ref())
				.collect::<Vec<_>>();
			columns.push(interleave(&source_columns, &build_indices)?);
		} else {
			columns.push(default_array_for_unused_column(
				field.data_type(),
				refs.len(),
			)?);
		}
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

pub(super) async fn materialize_streamed_spilled_left_outer_matches(
	spilled: &mut SpilledInnerJoinBuild,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if matches.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in matches {
		page_sources
			.entry(row.build.page_index)
			.or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let mut build_pages = Vec::with_capacity(page_sources.len());
	for page_index in page_indices {
		build_pages.push(spilled.read_batch_page(page_index).await?);
	}

	let estimated_output_bytes = join_take_index_bytes(matches.len())
		.saturating_add(join_interleave_index_bytes(matches.len()))
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			matches.len(),
		))
		.saturating_add(estimate_selected_batch_bytes(probe_batch, matches.len()));
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"partitioned left outer output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(matches.len());
	let mut probe_indices = Vec::with_capacity(matches.len());
	for row in matches {
		let source_index =
			*page_sources
				.get(&row.build.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"partitioned left outer missing spilled page source {}",
						row.build.page_index
					),
				})?;
		build_indices.push((source_index, row.build.row as usize));
		probe_indices.push(Some(row.probe));
	}

	let mut columns = Vec::with_capacity(
		build_pages
			.first()
			.map(RecordBatch::num_columns)
			.unwrap_or(0)
			.saturating_add(probe_batch.num_columns()),
	);
	for column_index in 0..build_pages
		.first()
		.map(RecordBatch::num_columns)
		.unwrap_or(0)
	{
		let source_columns = build_pages
			.iter()
			.map(|batch| batch.column(column_index).as_ref())
			.collect::<Vec<_>>();
		columns.push(interleave(&source_columns, &build_indices)?);
	}
	let probe_indices = UInt32Array::from(probe_indices);
	for column in probe_batch.columns() {
		columns.push(take(column, &probe_indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

pub(super) async fn materialize_streamed_spilled_left_outer_unmatched(
	spilled: &mut SpilledInnerJoinBuild,
	refs: &[SpilledBatchRowRef],
	right_schema: &SchemaRef,
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if refs.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in refs {
		page_sources.entry(row.page_index).or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let mut build_pages = Vec::with_capacity(page_sources.len());
	for page_index in page_indices {
		build_pages.push(spilled.read_batch_page(page_index).await?);
	}

	let estimated_output_bytes = join_interleave_index_bytes(refs.len()).saturating_add(
		estimate_spilled_inner_build_selection_bytes(&build_pages, refs.len()),
	);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"partitioned left outer unmatched materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(refs.len());
	for row in refs {
		let source_index =
			*page_sources
				.get(&row.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"partitioned left outer missing unmatched page source {}",
						row.page_index
					),
				})?;
		build_indices.push((source_index, row.row as usize));
	}

	let mut columns = Vec::with_capacity(output_schema.fields().len());
	for column_index in 0..build_pages
		.first()
		.map(RecordBatch::num_columns)
		.unwrap_or(0)
	{
		let source_columns = build_pages
			.iter()
			.map(|batch| batch.column(column_index).as_ref())
			.collect::<Vec<_>>();
		columns.push(interleave(&source_columns, &build_indices)?);
	}
	for field in right_schema.fields() {
		columns.push(arrow::array::new_null_array(field.data_type(), refs.len()));
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

pub(super) fn materialize_streamed_spilled_inner_matches(
	page_map: &BTreeMap<usize, RecordBatch>,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
) -> Result<RecordBatch, ExecutionError> {
	if matches.is_empty() {
		return Ok(RecordBatch::new_empty(output_schema.clone()));
	}

	let materialization_cap = join_output_materialization_memory_cap(memory_cap);
	let mut page_sources = BTreeMap::new();
	for row in matches {
		page_sources
			.entry(row.build.page_index)
			.or_insert(usize::MAX);
	}
	let page_indices = page_sources.keys().copied().collect::<Vec<_>>();
	for (source_index, page_index) in page_indices.iter().copied().enumerate() {
		page_sources.insert(page_index, source_index);
	}
	let mut build_pages = Vec::with_capacity(page_sources.len());
	for page_index in page_indices {
		build_pages.push(page_map.get(&page_index).cloned().ok_or_else(|| {
			ExecutionError::Kernel {
				message: format!("streamed inner join missing preloaded spilled page {page_index}"),
			}
		})?);
	}

	let estimated_output_bytes = join_take_index_bytes(matches.len())
		.saturating_add(join_interleave_index_bytes(matches.len()))
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			matches.len(),
		))
		.saturating_add(estimate_selected_batch_bytes(probe_batch, matches.len()));
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"streamed inner join output materialization",
			estimated_output_bytes,
			materialization_cap,
		));
	}

	let mut build_indices = Vec::with_capacity(matches.len());
	let mut probe_indices = Vec::with_capacity(matches.len());
	for row in matches {
		let source_index =
			*page_sources
				.get(&row.build.page_index)
				.ok_or_else(|| ExecutionError::Kernel {
					message: format!(
						"streamed inner join missing spilled page source {}",
						row.build.page_index
					),
				})?;
		build_indices.push((source_index, row.build.row as usize));
		probe_indices.push(Some(row.probe));
	}

	let mut columns = Vec::with_capacity(
		build_pages
			.first()
			.map(RecordBatch::num_columns)
			.unwrap_or(0)
			.saturating_add(probe_batch.num_columns()),
	);
	for column_index in 0..build_pages
		.first()
		.map(RecordBatch::num_columns)
		.unwrap_or(0)
	{
		let source_columns = build_pages
			.iter()
			.map(|batch| batch.column(column_index).as_ref())
			.collect::<Vec<_>>();
		columns.push(interleave(&source_columns, &build_indices)?);
	}
	let probe_indices = UInt32Array::from(probe_indices);
	for column in probe_batch.columns() {
		columns.push(take(column, &probe_indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_spilled_hash_join(
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	left_key_arrays: &[ArrayRef],
	right_key_arrays: &[ArrayRef],
	output_schema: &arrow::datatypes::SchemaRef,
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	task_budget: TaskBudget,
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let key_plan = JoinKeyPlan::from_key_arrays(left_key_arrays, right_key_arrays, null_safe_keys);
	let mut build_rows: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
	let build_started = Instant::now();
	for row in 0..left_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(left_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(left_key_arrays, row)? else {
				continue;
			};
			key
		};
		build_rows
			.entry(key)
			.or_default()
			.push(u32::try_from(row).map_err(|_| ExecutionError::Kernel {
				message: "join build row index exceeds UInt32 take index".to_string(),
			})?);
	}
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});
	if build_rows.is_empty() {
		let mut selection = Vec::new();
		for row in 0..right_batch.num_rows() {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
		append_build_side_after_probe_rows(
			kind,
			left_batch.num_rows(),
			build_match_bitmap(kind, left_batch.num_rows()).as_deref(),
			&mut selection,
		)?;
		return Ok(vec![materialize_join_selection(
			kind,
			left_batch,
			right_batch,
			&selection,
			output_schema,
			task_budget.memory.get(),
		)?]);
	}

	guard_spill_retained_entry_memory(
		left_batch,
		right_batch,
		&build_rows,
		task_budget.memory.get(),
	)?;

	let spill_started = Instant::now();
	let mut spilled =
		SpilledJoinBuild::write(build_rows, key_plan, task_budget, spill_file_ops, spill_dir)
			.await?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.spill_micros = task
			.phases
			.spill_micros
			.saturating_add(duration_micros(spill_started));
		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});
	let probe_started = Instant::now();
	let result = probe_spilled_hash_join(
		kind,
		&mut spilled,
		right_batch,
		right_key_arrays,
		left_batch,
		output_schema,
		match_filter,
		null_safe_keys,
		task_budget.memory.get(),
	)
	.await;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		if let Ok(batches) = &result {
			let matched_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(matched_rows as u64);
		}
	});
	spilled.cleanup().await;
	result
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_spilled_hash_join_into_global_aggregate(
	executor: &Executor,
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	left_key_arrays: &[ArrayRef],
	right_key_arrays: &[ArrayRef],
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
	aggregate_state: &mut [AggState],
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	task_budget: TaskBudget,
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
) -> Result<u64, ExecutionError> {
	let key_plan = JoinKeyPlan::from_key_arrays(left_key_arrays, right_key_arrays, null_safe_keys);
	let mut build_rows: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
	let build_started = Instant::now();
	for row in 0..left_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(left_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(left_key_arrays, row)? else {
				continue;
			};
			key
		};
		build_rows
			.entry(key)
			.or_default()
			.push(u32::try_from(row).map_err(|_| ExecutionError::Kernel {
				message: "join build row index exceeds UInt32 take index".to_string(),
			})?);
	}
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});
	if build_rows.is_empty() {
		let mut selection = Vec::new();
		for row in 0..right_batch.num_rows() {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
		append_build_side_after_probe_rows(
			kind,
			left_batch.num_rows(),
			build_match_bitmap(kind, left_batch.num_rows()).as_deref(),
			&mut selection,
		)?;
		let output_rows = selection.len() as u64;
		fold_join_selection_chunks_into_global_aggregate(
			executor,
			kind,
			left_batch,
			right_batch,
			&selection,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			aggregate_state,
			task_budget.memory.get(),
		)?;
		return Ok(output_rows);
	}

	guard_spill_retained_entry_memory(
		left_batch,
		right_batch,
		&build_rows,
		task_budget.memory.get(),
	)?;

	let spill_started = Instant::now();
	let mut spilled =
		SpilledJoinBuild::write(build_rows, key_plan, task_budget, spill_file_ops, spill_dir)
			.await?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.spill_micros = task
			.phases
			.spill_micros
			.saturating_add(duration_micros(spill_started));
		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});
	let probe_started = Instant::now();
	let result = probe_spilled_hash_join_into_global_aggregate(
		executor,
		kind,
		&mut spilled,
		right_batch,
		right_key_arrays,
		left_batch,
		join_output_schema,
		filter_predicate,
		aggr_expr,
		aggregate_state,
		match_filter,
		null_safe_keys,
		task_budget.memory.get(),
	)
	.await;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		if let Ok(output_rows) = &result {
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(*output_rows);
		}
	});
	spilled.cleanup().await;
	result
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn visit_spilled_hash_join_filtered_chunks<F>(
	executor: &Executor,
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	left_key_arrays: &[ArrayRef],
	right_key_arrays: &[ArrayRef],
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	task_budget: TaskBudget,
	telemetry: Option<Arc<Mutex<TaskTelemetry>>>,
	spill_file_ops: Arc<dyn AsyncFileOps>,
	spill_dir: PathBuf,
	visit_filtered_batch: &mut F,
) -> Result<u64, ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let key_plan = JoinKeyPlan::from_key_arrays(left_key_arrays, right_key_arrays, null_safe_keys);
	let mut build_rows: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
	let build_started = Instant::now();
	for row in 0..left_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(left_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(left_key_arrays, row)? else {
				continue;
			};
			key
		};
		build_rows
			.entry(key)
			.or_default()
			.push(u32::try_from(row).map_err(|_| ExecutionError::Kernel {
				message: "join build row index exceeds UInt32 take index".to_string(),
			})?);
	}
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});
	if build_rows.is_empty() {
		let mut selection = Vec::new();
		for row in 0..right_batch.num_rows() {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
		append_build_side_after_probe_rows(
			kind,
			left_batch.num_rows(),
			build_match_bitmap(kind, left_batch.num_rows()).as_deref(),
			&mut selection,
		)?;
		let output_rows = selection.len() as u64;
		visit_join_selection_filtered_chunks(
			executor,
			kind,
			left_batch,
			right_batch,
			&selection,
			join_output_schema,
			filter_predicate,
			task_budget.memory.get(),
			visit_filtered_batch,
		)?;
		return Ok(output_rows);
	}

	guard_spill_retained_entry_memory(
		left_batch,
		right_batch,
		&build_rows,
		task_budget.memory.get(),
	)?;

	let spill_started = Instant::now();
	let mut spilled =
		SpilledJoinBuild::write(build_rows, key_plan, task_budget, spill_file_ops, spill_dir)
			.await?;
	record_task_telemetry(&telemetry, |task| {
		task.phases.spill_micros = task
			.phases
			.spill_micros
			.saturating_add(duration_micros(spill_started));
		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});
	let probe_started = Instant::now();
	let result = probe_spilled_hash_join_filtered_chunks(
		executor,
		kind,
		&mut spilled,
		right_batch,
		right_key_arrays,
		left_batch,
		join_output_schema,
		filter_predicate,
		match_filter,
		null_safe_keys,
		task_budget.memory.get(),
		visit_filtered_batch,
	)
	.await;
	record_task_telemetry(&telemetry, |task| {
		task.phases.probe_micros = task
			.phases
			.probe_micros
			.saturating_add(duration_micros(probe_started));
		if let Ok(output_rows) = &result {
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(*output_rows);
		}
	});
	spilled.cleanup().await;
	result
}

pub(super) async fn probe_spilled_hash_join(
	kind: JoinKind,
	spilled: &mut SpilledJoinBuild,
	right_batch: &RecordBatch,
	right_key_arrays: &[ArrayRef],
	left_batch: &RecordBatch,
	output_schema: &arrow::datatypes::SchemaRef,
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	memory_cap: u64,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let mut selection = Vec::new();
	let mut matched_build_rows = build_match_bitmap(kind, left_batch.num_rows());
	let mut residual = JoinMatchResidual::new(match_filter, left_batch, right_batch);
	for row in 0..right_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(right_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(right_key_arrays, row)? else {
				append_unmatched_probe_row(kind, row, &mut selection)?;
				continue;
			};
			key
		};
		let matches = spilled.lookup(&key).await?;
		let matched = append_join_matches_for_kind(
			kind,
			row,
			matches.into_iter(),
			matched_build_rows.as_deref_mut(),
			&mut selection,
			&mut residual,
		)?;
		if !matched {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
	}
	append_build_side_after_probe_rows(
		kind,
		left_batch.num_rows(),
		matched_build_rows.as_deref(),
		&mut selection,
	)?;

	Ok(vec![materialize_join_selection(
		kind,
		left_batch,
		right_batch,
		&selection,
		output_schema,
		memory_cap,
	)?])
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn probe_spilled_hash_join_into_global_aggregate(
	executor: &Executor,
	kind: JoinKind,
	spilled: &mut SpilledJoinBuild,
	right_batch: &RecordBatch,
	right_key_arrays: &[ArrayRef],
	left_batch: &RecordBatch,
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
	aggregate_state: &mut [AggState],
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	memory_cap: u64,
) -> Result<u64, ExecutionError> {
	let mut selection = Vec::new();
	let mut matched_build_rows = build_match_bitmap(kind, left_batch.num_rows());
	let mut residual = JoinMatchResidual::new(match_filter, left_batch, right_batch);
	for row in 0..right_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(right_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(right_key_arrays, row)? else {
				append_unmatched_probe_row(kind, row, &mut selection)?;
				continue;
			};
			key
		};
		let matches = spilled.lookup(&key).await?;
		let matched = append_join_matches_for_kind(
			kind,
			row,
			matches.into_iter(),
			matched_build_rows.as_deref_mut(),
			&mut selection,
			&mut residual,
		)?;
		if !matched {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
	}
	append_build_side_after_probe_rows(
		kind,
		left_batch.num_rows(),
		matched_build_rows.as_deref(),
		&mut selection,
	)?;

	let output_rows = selection.len() as u64;
	fold_join_selection_chunks_into_global_aggregate(
		executor,
		kind,
		left_batch,
		right_batch,
		&selection,
		join_output_schema,
		filter_predicate,
		aggr_expr,
		aggregate_state,
		memory_cap,
	)?;
	Ok(output_rows)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn probe_spilled_hash_join_filtered_chunks<F>(
	executor: &Executor,
	kind: JoinKind,
	spilled: &mut SpilledJoinBuild,
	right_batch: &RecordBatch,
	right_key_arrays: &[ArrayRef],
	left_batch: &RecordBatch,
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	match_filter: Option<&BoundExprProgram>,
	null_safe_keys: bool,
	memory_cap: u64,
	visit_filtered_batch: &mut F,
) -> Result<u64, ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let mut selection = Vec::new();
	let mut matched_build_rows = build_match_bitmap(kind, left_batch.num_rows());
	let mut residual = JoinMatchResidual::new(match_filter, left_batch, right_batch);
	for row in 0..right_batch.num_rows() {
		let key = if null_safe_keys {
			join_key_bytes_null_safe(right_key_arrays, row)?
		} else {
			let Some(key) = join_key_bytes(right_key_arrays, row)? else {
				append_unmatched_probe_row(kind, row, &mut selection)?;
				continue;
			};
			key
		};
		let matches = spilled.lookup(&key).await?;
		let matched = append_join_matches_for_kind(
			kind,
			row,
			matches.into_iter(),
			matched_build_rows.as_deref_mut(),
			&mut selection,
			&mut residual,
		)?;
		if !matched {
			append_unmatched_probe_row(kind, row, &mut selection)?;
		}
	}
	append_build_side_after_probe_rows(
		kind,
		left_batch.num_rows(),
		matched_build_rows.as_deref(),
		&mut selection,
	)?;

	let output_rows = selection.len() as u64;
	visit_join_selection_filtered_chunks(
		executor,
		kind,
		left_batch,
		right_batch,
		&selection,
		join_output_schema,
		filter_predicate,
		memory_cap,
		visit_filtered_batch,
	)?;
	Ok(output_rows)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fold_join_selection_chunks_into_global_aggregate(
	executor: &Executor,
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	selection: &[JoinOutputRow],
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	aggr_expr: &[BoundAggregateCall],
	aggregate_state: &mut [AggState],
	memory_cap: u64,
) -> Result<(), ExecutionError> {
	for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
		let batch = materialize_join_selection(
			kind,
			left_batch,
			right_batch,
			chunk,
			join_output_schema,
			memory_cap,
		)?;
		let batch = if let Some(predicate) = filter_predicate {
			executor.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
		} else {
			batch
		};
		executor.fold_global_aggregate_state(aggregate_state, aggr_expr, &batch)?;
	}
	Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn visit_join_selection_filtered_chunks<F>(
	executor: &Executor,
	kind: JoinKind,
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	selection: &[JoinOutputRow],
	join_output_schema: &arrow::datatypes::SchemaRef,
	filter_predicate: Option<&BoundExprProgram>,
	memory_cap: u64,
	visit_filtered_batch: &mut F,
) -> Result<(), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
		let batch = materialize_join_selection(
			kind,
			left_batch,
			right_batch,
			chunk,
			join_output_schema,
			memory_cap,
		)?;
		let batch = if let Some(predicate) = filter_predicate {
			executor.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
		} else {
			batch
		};
		if batch.num_rows() > 0 {
			visit_filtered_batch(batch)?;
		}
	}
	Ok(())
}

pub(super) fn key_arrays(
	batch: &RecordBatch,
	keys: &[crate::ir::ColumnOrdinal],
) -> Result<Vec<ArrayRef>, ExecutionError> {
	keys.iter()
		.map(|key| {
			let ordinal = usize::try_from(key.0).expect("column ordinal fits usize");
			batch
				.columns()
				.get(ordinal)
				.cloned()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("join key column ordinal {ordinal} out of bounds"),
				})
		})
		.collect()
}

pub(super) fn join_key_bytes(
	key_arrays: &[ArrayRef],
	row: usize,
) -> Result<Option<Vec<u8>>, ExecutionError> {
	let mut encoded = Vec::new();
	for array in key_arrays {
		if array.is_null(row) {
			return Ok(None);
		}
		encode_partition_key_value(array.as_ref(), row, &mut encoded)?;
	}
	Ok(Some(encoded))
}

pub(super) fn join_key_bytes_null_safe(
	key_arrays: &[ArrayRef],
	row: usize,
) -> Result<Vec<u8>, ExecutionError> {
	let mut encoded = Vec::new();
	for array in key_arrays {
		encode_partition_key_value(array.as_ref(), row, &mut encoded)?;
	}
	Ok(encoded)
}

pub(super) fn estimated_build_hash_bytes(key_arrays: &[ArrayRef], rows: usize) -> u64 {
	let key_width = key_arrays
		.iter()
		.map(|array| crate::ir::hash_join_build_key_width(array.data_type()))
		.sum::<usize>();
	crate::ir::estimated_hash_join_build_bytes(rows, key_width)
}

pub(super) fn estimate_spill_retained_entry_bytes(
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	build_rows: &HashMap<Vec<u8>, Vec<u32>>,
) -> u64 {
	record_batch_memory_bytes(left_batch)
		.saturating_add(record_batch_memory_bytes(right_batch))
		.saturating_add(estimate_spill_build_map_bytes(build_rows))
}

pub(super) fn guard_spill_retained_entry_memory(
	left_batch: &RecordBatch,
	right_batch: &RecordBatch,
	build_rows: &HashMap<Vec<u8>, Vec<u32>>,
	memory_cap: u64,
) -> Result<(), ExecutionError> {
	let retained_bytes = estimate_spill_retained_entry_bytes(left_batch, right_batch, build_rows);
	let retained_cap = join_spill_retained_footprint_memory_cap(memory_cap);
	if retained_bytes > retained_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"join spill retained entry",
			retained_bytes,
			retained_cap,
		));
	}
	Ok(())
}

pub(super) fn join_spill_retained_footprint_memory_cap(memory_cap: u64) -> u64 {
	memory_cap.saturating_mul(JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER)
}

pub(super) fn record_batch_memory_bytes(batch: &RecordBatch) -> u64 {
	u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX)
}

pub(super) fn estimate_spill_build_map_bytes(build_rows: &HashMap<Vec<u8>, Vec<u32>>) -> u64 {
	let bucket_bytes = build_rows
		.capacity()
		.saturating_mul(std::mem::size_of::<(Vec<u8>, Vec<u32>)>().saturating_add(16));
	build_rows
		.iter()
		.map(|(key, rows)| {
			key.capacity()
				.saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<u32>()))
		})
		.fold(
			u64::try_from(bucket_bytes).unwrap_or(u64::MAX),
			|total, bytes| total.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX)),
		)
}

pub(super) fn budget_exhausted(
	dimension: BudgetDimension,
	source: &'static str,
	requested: u64,
	available: u64,
) -> ExecutionError {
	ExecutionError::BudgetExhausted {
		dimension,
		producer: source,
		requested: ByteCount::new(requested),
		available: ByteCount::new(available),
		detail: Default::default(),
	}
}

pub(super) fn budget_exhausted_with_detail(
	dimension: BudgetDimension,
	source: &'static str,
	requested: u64,
	available: u64,
	detail: crate::error::BudgetExhaustedDetail,
) -> ExecutionError {
	ExecutionError::BudgetExhausted {
		dimension,
		producer: source,
		requested: ByteCount::new(requested),
		available: ByteCount::new(available),
		detail,
	}
}
