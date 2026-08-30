use super::*;

#[allow(clippy::too_many_arguments)]

pub(in crate::exec) async fn try_execute_streamed_spilled_left_semi_join(
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

	// ADR0551-SITE: join/try_execute_streamed_spilled_left_semi_join/Build/1

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let build_result = async {
		for batch in left_batches {
			spilled
				.write_batch_chunks(batch, key_plan, left_keys, null_safe_keys, task_budget)
				.await?;
		}

		spilled.finish(task_budget).await
	}
	.await;

	if let Err(error) = build_result {
		build_scope.discard_before_measurement();

		return Err(error);
	}

	record_task_telemetry(&telemetry, |task| {
		build_scope.finish_at_measurement();

		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(spilled.input_row_count());

		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});

	build_scope.commit_after_write();

	// ADR0551-SITE: join/try_execute_streamed_spilled_left_semi_join/Probe/1

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

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
		probe_scope.finish_at_measurement();

		if let Ok((_, probe_rows, output_rows)) = &result {
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);

			task.counters.add_matched_rows(*output_rows, false);
		}
	});

	probe_scope.commit_after_write();

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

pub(in crate::exec) async fn probe_streamed_spilled_left_semi_join(
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

pub(in crate::exec) async fn try_execute_partitioned_spilled_left_outer_join(
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

		// ADR0551-SITE: join/try_execute_partitioned_spilled_left_outer_join/Build/1

		let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

		let build_result = async {
			for (batch, buckets) in left_batches.iter().zip(&left_partition_buckets) {
				let partitioned =
					select_join_partition_batch_with_buckets(batch, buckets, partition)?;

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

			spilled.finish(task_budget).await
		}
		.await;

		if let Err(error) = build_result {
			build_scope.discard_before_measurement();

			return Err(error);
		}

		let build_rows = spilled.input_row_count();

		total_build_rows = total_build_rows.saturating_add(build_rows);

		record_task_telemetry(&telemetry, |task| {
			build_scope.finish_at_measurement();

			task.counters.build_rows = task.counters.build_rows.saturating_add(build_rows);

			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});

		build_scope.commit_after_write();

		// ADR0551-SITE: join/try_execute_partitioned_spilled_left_outer_join/Probe/1

		let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

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

		let (probe_rows, output_rows) = match visit_partitioned_spilled_left_outer_join_chunks(
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
		.await
		{
			Ok(outcome) => outcome,

			Err(error) => {
				probe_scope.discard_before_measurement();

				return Err(error);
			}
		};

		total_probe_rows = total_probe_rows.saturating_add(probe_rows);

		record_task_telemetry(&telemetry, |task| {
			probe_scope.finish_at_measurement();

			task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);

			task.counters.add_matched_rows(output_rows, false);
		});

		probe_scope.commit_after_write();

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

pub(in crate::exec) fn streamed_spill_partition_count(
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

pub(in crate::exec) fn select_join_partition_batch(
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

pub(in crate::exec) fn precompute_join_partition_buckets(
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

pub(in crate::exec) fn select_join_partition_batch_with_buckets(
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

pub(in crate::exec) async fn visit_partitioned_spilled_left_outer_join_chunks<F>(
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

pub(in crate::exec) async fn try_execute_streamed_spilled_inner_join(
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
) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
	// ADR0551-SITE: join/try_execute_streamed_spilled_inner_join/Build/1

	let mut build_scope = telemetry.open_operator_transition(PhaseOperatorKind::Build);

	let (mut spilled, key_plan) = match try_create_streamed_spilled_inner_build(
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
	.await
	{
		Ok(Some(build)) => build,

		Ok(None) => {
			build_scope.discard_before_measurement();

			return Ok(None);
		}

		Err(error) => {
			build_scope.discard_before_measurement();

			return Err(error);
		}
	};

	record_task_telemetry(&telemetry, |task| {
		build_scope.finish_at_measurement();

		task.counters.build_rows = task
			.counters
			.build_rows
			.saturating_add(spilled.input_row_count());

		task.counters.spill_bytes = task
			.counters
			.spill_bytes
			.saturating_add(spilled.spill_bytes());
	});

	build_scope.commit_after_write();

	// ADR0551-SITE: join/try_execute_streamed_spilled_inner_join/Probe/1

	let mut probe_scope = telemetry.open_operator_transition(PhaseOperatorKind::Probe);

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
		probe_scope.finish_at_measurement();

		if let Ok((_, probe_rows, output_rows)) = &result {
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(*probe_rows);

			task.counters.add_matched_rows(*output_rows, false);
		}
	});

	probe_scope.commit_after_write();

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

pub(in crate::exec) async fn try_create_streamed_spilled_inner_build(
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

pub(in crate::exec) async fn probe_streamed_spilled_inner_join(
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

pub(in crate::exec) async fn visit_streamed_spilled_inner_join_filtered_chunks<F>(
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

pub(in crate::exec) async fn fold_streamed_spilled_inner_join_into_global_aggregate(
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

pub(in crate::exec) async fn materialize_streamed_spilled_build_rows(
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

pub(in crate::exec) async fn materialize_projected_streamed_spilled_build_rows(
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

pub(in crate::exec) async fn materialize_streamed_spilled_left_outer_matches(
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

pub(in crate::exec) async fn materialize_streamed_spilled_left_outer_unmatched(
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

pub(in crate::exec) fn materialize_streamed_spilled_inner_matches(
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
