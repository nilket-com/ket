use super::*;

#[allow(clippy::too_many_arguments)]

pub(in crate::exec) async fn execute_spilled_hash_join(
	kind: JoinKind,

	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	left_key_arrays: &[ArrayRef],

	right_key_arrays: &[ArrayRef],

	output_schema: &arrow::datatypes::SchemaRef,

	match_filter: Option<&BoundExprProgram>,

	null_safe_keys: bool,

	task_budget: TaskBudget,

	telemetry: JoinTiming,

	spill_file_ops: Arc<dyn AsyncFileOps>,

	spill_dir: PathBuf,
) -> Result<Vec<RecordBatch>, ExecutionError> {
	let key_plan = JoinKeyPlan::from_key_arrays(left_key_arrays, right_key_arrays, null_safe_keys);

	let mut build_rows: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();

	// ADR0551-SITE: join/execute_spilled_hash_join/Build/1

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_result = (|| -> Result<(), ExecutionError> {
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

		Ok(())
	})();

	if let Err(error) = build_result {
		build_scope.discard_before_measurement();

		return Err(error);
	}

	record_task_telemetry(&telemetry, |_task| {
		build_scope.finish_at_measurement();
	});

	build_scope.commit_after_write();

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
			crate::exec::join::SideTakeTiming::AccountedElsewhere,
		)?]);
	}

	guard_spill_retained_entry_memory(
		left_batch,
		right_batch,
		&build_rows,
		task_budget.memory.get(),
	)?;

	// ADR0551-SITE: join/execute_spilled_hash_join/Spill/1

	let mut spill_scope = telemetry.open_operator_transition(PhaseOperatorKind::Spill);

	let mut spilled =
		match SpilledJoinBuild::write(build_rows, key_plan, task_budget, spill_file_ops, spill_dir)
			.await
		{
			Ok(spilled) => spilled,

			Err(error) => {
				spill_scope.discard_before_measurement();

				return Err(error);
			}
		};

	record_task_telemetry(&telemetry, |task| {
		spill_scope.finish_at_measurement();

		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});

	spill_scope.commit_after_write();

	// ADR0551-SITE: join/execute_spilled_hash_join/Probe/1

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

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
		probe_scope.finish_at_measurement();

		if let Ok(batches) = &result {
			let matched_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();

			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(matched_rows as u64);
		}
	});

	probe_scope.commit_after_write();

	spilled.cleanup().await;

	result
}

#[allow(clippy::too_many_arguments)]

pub(in crate::exec) async fn execute_spilled_hash_join_into_global_aggregate(
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

	telemetry: JoinTiming,

	spill_file_ops: Arc<dyn AsyncFileOps>,

	spill_dir: PathBuf,
) -> Result<u64, ExecutionError> {
	let key_plan = JoinKeyPlan::from_key_arrays(left_key_arrays, right_key_arrays, null_safe_keys);

	let mut build_rows: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();

	// ADR0551-SITE: join/execute_spilled_hash_join_into_global_aggregate/Build/1

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_result = (|| -> Result<(), ExecutionError> {
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

		Ok(())
	})();

	if let Err(error) = build_result {
		build_scope.discard_before_measurement();

		return Err(error);
	}

	record_task_telemetry(&telemetry, |_task| {
		build_scope.finish_at_measurement();
	});

	build_scope.commit_after_write();

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

	// ADR0551-SITE: join/execute_spilled_hash_join_into_global_aggregate/Spill/1

	let mut spill_scope = telemetry.open_operator_transition(PhaseOperatorKind::Spill);

	let mut spilled =
		match SpilledJoinBuild::write(build_rows, key_plan, task_budget, spill_file_ops, spill_dir)
			.await
		{
			Ok(spilled) => spilled,

			Err(error) => {
				spill_scope.discard_before_measurement();

				return Err(error);
			}
		};

	record_task_telemetry(&telemetry, |task| {
		spill_scope.finish_at_measurement();

		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});

	spill_scope.commit_after_write();

	// ADR0551-SITE: join/execute_spilled_hash_join_into_global_aggregate/Probe/1

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

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
		probe_scope.finish_at_measurement();

		if let Ok(output_rows) = &result {
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(*output_rows);
		}
	});

	probe_scope.commit_after_write();

	spilled.cleanup().await;

	result
}

#[allow(clippy::too_many_arguments)]

pub(in crate::exec) async fn visit_spilled_hash_join_filtered_chunks<F>(
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

	telemetry: JoinTiming,

	spill_file_ops: Arc<dyn AsyncFileOps>,

	spill_dir: PathBuf,

	visit_filtered_batch: &mut F,
) -> Result<u64, ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let key_plan = JoinKeyPlan::from_key_arrays(left_key_arrays, right_key_arrays, null_safe_keys);

	let mut build_rows: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();

	// ADR0551-SITE: join/visit_spilled_hash_join_filtered_chunks/Build/1

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_result = (|| -> Result<(), ExecutionError> {
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

		Ok(())
	})();

	if let Err(error) = build_result {
		build_scope.discard_before_measurement();

		return Err(error);
	}

	record_task_telemetry(&telemetry, |_task| {
		build_scope.finish_at_measurement();
	});

	build_scope.commit_after_write();

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

	// ADR0551-SITE: join/visit_spilled_hash_join_filtered_chunks/Spill/1

	let mut spill_scope = telemetry.open_operator_transition(PhaseOperatorKind::Spill);

	let mut spilled =
		match SpilledJoinBuild::write(build_rows, key_plan, task_budget, spill_file_ops, spill_dir)
			.await
		{
			Ok(spilled) => spilled,

			Err(error) => {
				spill_scope.discard_before_measurement();

				return Err(error);
			}
		};

	record_task_telemetry(&telemetry, |task| {
		spill_scope.finish_at_measurement();

		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});

	spill_scope.commit_after_write();

	// ADR0551-SITE: join/visit_spilled_hash_join_filtered_chunks/Probe/1

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

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
		probe_scope.finish_at_measurement();

		if let Ok(output_rows) = &result {
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(*output_rows);
		}
	});

	probe_scope.commit_after_write();

	spilled.cleanup().await;

	result
}

pub(in crate::exec) async fn probe_spilled_hash_join(
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
		crate::exec::join::SideTakeTiming::AccountedElsewhere,
	)?])
}

#[allow(clippy::too_many_arguments)]

pub(in crate::exec) async fn probe_spilled_hash_join_into_global_aggregate(
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

pub(in crate::exec) async fn probe_spilled_hash_join_filtered_chunks<F>(
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

pub(in crate::exec) fn fold_join_selection_chunks_into_global_aggregate(
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
			crate::exec::join::SideTakeTiming::AccountedElsewhere,
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

pub(in crate::exec) fn visit_join_selection_filtered_chunks<F>(
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
			crate::exec::join::SideTakeTiming::AccountedElsewhere,
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

pub(in crate::exec) fn key_arrays(
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

pub(in crate::exec) fn join_key_bytes(
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

pub(in crate::exec) fn join_key_bytes_null_safe(
	key_arrays: &[ArrayRef],

	row: usize,
) -> Result<Vec<u8>, ExecutionError> {
	let mut encoded = Vec::new();

	for array in key_arrays {
		encode_partition_key_value(array.as_ref(), row, &mut encoded)?;
	}

	Ok(encoded)
}

pub(in crate::exec) fn estimated_build_hash_bytes(key_arrays: &[ArrayRef], rows: usize) -> u64 {
	let key_width = key_arrays
		.iter()
		.map(|array| crate::ir::hash_join_build_key_width(array.data_type()))
		.sum::<usize>();

	crate::ir::estimated_hash_join_build_bytes(rows, key_width)
}

pub(in crate::exec) fn estimate_spill_retained_entry_bytes(
	left_batch: &RecordBatch,

	right_batch: &RecordBatch,

	build_rows: &HashMap<Vec<u8>, Vec<u32>>,
) -> u64 {
	record_batch_memory_bytes(left_batch)
		.saturating_add(record_batch_memory_bytes(right_batch))
		.saturating_add(estimate_spill_build_map_bytes(build_rows))
}

pub(in crate::exec) fn guard_spill_retained_entry_memory(
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

pub(in crate::exec) fn join_spill_retained_footprint_memory_cap(memory_cap: u64) -> u64 {
	memory_cap.saturating_mul(JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER)
}

pub(in crate::exec) fn record_batch_memory_bytes(batch: &RecordBatch) -> u64 {
	u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX)
}

pub(in crate::exec) fn estimate_spill_build_map_bytes(
	build_rows: &HashMap<Vec<u8>, Vec<u32>>,
) -> u64 {
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

pub(in crate::exec) fn budget_exhausted(
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

pub(in crate::exec) fn budget_exhausted_with_detail(
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
