use super::*;

pub(in crate::exec) struct ReorientedInnerJoinStage<'a> {
	replay_id: JoinReplayId,
	probe_schema: SchemaRef,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	build: &'a ExecutionPlan,
	build_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	strategy: JoinStrategy,
	post_filter: Option<&'a BoundExprProgram>,
	probe_columns_first: bool,
}

pub(in crate::exec) struct ReorientedInnerJoinRuntime<'a> {
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	build_batch: RecordBatch,
	build_rows: HashMap<JoinKey, BuildRowList>,
	key_plan: JoinKeyPlan,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	post_filter: Option<&'a BoundExprProgram>,
	probe_columns_first: bool,
	probe_rows: u64,
	output_rows: u64,
}

pub(in crate::exec) struct StreamingSemiAntiStage<'a> {
	pub(in crate::exec) replay_id: JoinReplayId,
	pub(in crate::exec) kind: JoinKind,
	pub(in crate::exec) preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(in crate::exec) existence: &'a ExecutionPlan,
	pub(in crate::exec) existence_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(in crate::exec) match_filter: &'a BoundExprProgram,
	pub(in crate::exec) output_schema: SchemaRef,
	pub(in crate::exec) strategy: JoinStrategy,
}

#[allow(dead_code)]
pub(in crate::exec) enum StreamingSemiAntiExistence {
	InMemory {
		batch: RecordBatch,
		rows: HashMap<JoinKey, BuildRowList>,
	},
	Spilled(SpilledSemiAntiExistenceBuild),
}

pub(in crate::exec) enum StreamingExistenceOp<'a> {
	Filter(&'a BoundExprProgram),
	Project {
		exprs: &'a BoundExprProgram,
		output_schema: SchemaRef,
	},
	Exchange {
		partition_count: u32,
		key: &'a BoundExprProgram,
	},
}

pub(in crate::exec) struct StreamingSemiAntiRuntime<'a> {
	pub(in crate::exec) replay_id: JoinReplayId,
	pub(in crate::exec) kind: JoinKind,
	pub(in crate::exec) strategy: JoinStrategy,
	pub(in crate::exec) existence: StreamingSemiAntiExistence,
	pub(in crate::exec) existence_row_count: u64,
	pub(in crate::exec) key_plan: JoinKeyPlan,
	pub(in crate::exec) preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(in crate::exec) match_filter: &'a BoundExprProgram,
	pub(in crate::exec) output_schema: SchemaRef,
	pub(in crate::exec) probe_rows: u64,
	pub(in crate::exec) output_rows: u64,
}

pub(in crate::exec) struct StreamingLeftOuterRecombineRuntime<'a> {
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	scalar_batch: RecordBatch,
	scalar_rows: HashMap<JoinKey, BuildRowList>,
	key_plan: JoinKeyPlan,
	preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	match_filter: Option<&'a BoundExprProgram>,
	null_safe_keys: bool,
	output_schema: SchemaRef,
	probe_rows: u64,
	output_rows: u64,
}

impl StreamingSemiAntiRuntime<'_> {
	pub(in crate::exec) async fn cleanup(self) {
		if let StreamingSemiAntiExistence::Spilled(spilled) = self.existence {
			spilled.cleanup().await;
		}
	}
}

pub(in crate::exec) struct StreamingSemiAntiChain<'a> {
	base: &'a ExecutionPlan,
	inner_stages: Vec<ReorientedInnerJoinStage<'a>>,
	semi_stage: StreamingSemiAntiStage<'a>,
	anti_stage: StreamingSemiAntiStage<'a>,
}

pub(in crate::exec) fn extract_reoriented_inner_chain(
	top: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Vec<ReorientedInnerJoinStage<'_>>)> {
	let mut current = top;
	let mut stages = Vec::new();
	let ExecutionPlan::Join { replay_id, .. } = current else {
		return None;
	};
	if replay_id.0 == 0 || *replay_id == JoinReplayId::SYNTHETIC {
		return None;
	}
	let mut expected = replay_id.0;
	let mut post_filter = None;
	loop {
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
			..
		} = current
		else {
			return None;
		};
		if *replay_id != JoinReplayId(expected) {
			return None;
		}
		let (probe, probe_keys, build, build_keys, probe_columns_first, continuation) =
			if let Some(continuation) = reoriented_chain_continuation(left.as_ref(), expected) {
				(
					left.as_ref(),
					left_keys,
					right.as_ref(),
					right_keys,
					true,
					continuation,
				)
			} else if let Some(continuation) =
				reoriented_chain_continuation(right.as_ref(), expected)
			{
				(
					right.as_ref(),
					right_keys,
					left.as_ref(),
					left_keys,
					false,
					continuation,
				)
			} else {
				return None;
			};
		stages.push(ReorientedInnerJoinStage {
			replay_id: *replay_id,
			probe_schema: probe.schema(),
			probe_keys: probe_keys.clone(),
			build,
			build_keys: build_keys.clone(),
			output_schema: output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
			post_filter,
			probe_columns_first,
		});
		if expected == 1 {
			stages.reverse();
			return Some((probe, stages));
		}
		current = continuation.plan;
		post_filter = continuation.post_filter;
		expected -= 1;
	}
}

struct ReorientedChainContinuation<'a> {
	plan: &'a ExecutionPlan,
	post_filter: Option<&'a BoundExprProgram>,
}

fn reoriented_chain_continuation(
	plan: &ExecutionPlan,
	expected: u32,
) -> Option<ReorientedChainContinuation<'_>> {
	let ExecutionPlan::Exchange { input, .. } = plan else {
		return None;
	};
	let (candidate, post_filter) = match input.as_ref() {
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition: FilterDisposition::EvaluateInToron,
		} => (input.as_ref(), Some(predicate)),
		ExecutionPlan::Filter {
			input,
			disposition: FilterDisposition::FullyPushed,
			..
		} => (input.as_ref(), None),
		other => (other, None),
	};
	let ExecutionPlan::Join { replay_id, .. } = candidate else {
		return None;
	};
	if expected == 1 {
		if *replay_id == JoinReplayId(0) {
			Some(ReorientedChainContinuation {
				plan: candidate,
				post_filter,
			})
		} else {
			None
		}
	} else if *replay_id == JoinReplayId(expected - 1) {
		Some(ReorientedChainContinuation {
			plan: candidate,
			post_filter,
		})
	} else {
		None
	}
}

pub(in crate::exec) fn filter_input<'a>(
	plan: &'a ExecutionPlan,
) -> Option<(&'a ExecutionPlan, &'a BoundExprProgram)> {
	let ExecutionPlan::Filter {
		input,
		predicate,
		disposition: FilterDisposition::EvaluateInToron,
	} = plan
	else {
		return None;
	};
	Some((input.as_ref(), predicate))
}

pub(in crate::exec) fn exchange_input(plan: &ExecutionPlan) -> Option<&ExecutionPlan> {
	let ExecutionPlan::Exchange { input, .. } = plan else {
		return None;
	};
	Some(input.as_ref())
}
pub(in crate::exec) fn decompose_streaming_existence_plan(
	plan: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Vec<StreamingExistenceOp<'_>>)> {
	let mut current = plan;
	let mut ops = Vec::new();
	loop {
		match current {
			ExecutionPlan::Exchange {
				input,
				partition_count,
				key,
			} => {
				ops.push(StreamingExistenceOp::Exchange {
					partition_count: *partition_count,
					key,
				});
				current = input.as_ref();
			}
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => {
				if *disposition == FilterDisposition::EvaluateInToron {
					ops.push(StreamingExistenceOp::Filter(predicate));
				}
				current = input.as_ref();
			}
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				ops.push(StreamingExistenceOp::Project {
					exprs,
					output_schema: output_schema.clone(),
				});
				current = input.as_ref();
			}
			ExecutionPlan::Scan { .. } => {
				ops.reverse();
				return Some((current, ops));
			}
			_ => return None,
		}
	}
}

pub(in crate::exec) fn extract_streaming_inner_prefix_semi_anti_chain(
	top: &ExecutionPlan,
) -> Option<StreamingSemiAntiChain<'_>> {
	let ExecutionPlan::Join {
		replay_id: anti_replay_id,
		left: anti_left,
		right: anti_right,
		kind: JoinKind::LeftAnti,
		left_keys: anti_left_keys,
		right_keys: anti_right_keys,
		match_filter: Some(anti_filter),
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: anti_output_schema,
		..
	} = top
	else {
		return None;
	};
	if *anti_replay_id != JoinReplayId(4) {
		return None;
	}
	let semi = exchange_input(anti_left)?;
	let ExecutionPlan::Join {
		replay_id: semi_replay_id,
		left: semi_left,
		right: semi_right,
		kind: JoinKind::LeftSemi,
		left_keys: semi_left_keys,
		right_keys: semi_right_keys,
		match_filter: Some(semi_filter),
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: semi_output_schema,
		..
	} = semi
	else {
		return None;
	};
	if *semi_replay_id != JoinReplayId(3) {
		return None;
	}

	let (join2, join2_post_filter) = filter_input(exchange_input(semi_left)?)?;
	let ExecutionPlan::Join {
		replay_id: join2_replay_id,
		left: join2_left,
		right: join2_right,
		kind: JoinKind::Inner,
		left_keys: join2_left_keys,
		right_keys: join2_right_keys,
		match_filter: None,
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: join2_output_schema,
		..
	} = join2
	else {
		return None;
	};
	if *join2_replay_id != JoinReplayId(2) {
		return None;
	}

	let (join1, join1_post_filter) = filter_input(exchange_input(join2_left)?)?;
	let ExecutionPlan::Join {
		replay_id: join1_replay_id,
		left: join1_left,
		right: join1_right,
		kind: JoinKind::Inner,
		left_keys: join1_left_keys,
		right_keys: join1_right_keys,
		match_filter: None,
		null_safe_keys: false,
		strategy: JoinStrategy::HashPartitioned,
		output_schema: join1_output_schema,
		..
	} = join1
	else {
		return None;
	};
	if *join1_replay_id != JoinReplayId(1) {
		return None;
	}

	Some(StreamingSemiAntiChain {
		base: join1_left.as_ref(),
		inner_stages: vec![
			ReorientedInnerJoinStage {
				replay_id: *join1_replay_id,
				probe_schema: join1_left.schema(),
				probe_keys: join1_left_keys.clone(),
				build: join1_right.as_ref(),
				build_keys: join1_right_keys.clone(),
				output_schema: join1_output_schema.clone(),
				strategy: JoinStrategy::HashPartitioned,
				post_filter: Some(join1_post_filter),
				probe_columns_first: true,
			},
			ReorientedInnerJoinStage {
				replay_id: *join2_replay_id,
				probe_schema: join2_left.schema(),
				probe_keys: join2_left_keys.clone(),
				build: join2_right.as_ref(),
				build_keys: join2_right_keys.clone(),
				output_schema: join2_output_schema.clone(),
				strategy: JoinStrategy::HashPartitioned,
				post_filter: Some(join2_post_filter),
				probe_columns_first: true,
			},
		],
		semi_stage: StreamingSemiAntiStage {
			replay_id: *semi_replay_id,
			kind: JoinKind::LeftSemi,
			preserved_keys: semi_left_keys.clone(),
			existence: semi_right.as_ref(),
			existence_keys: semi_right_keys.clone(),
			match_filter: semi_filter,
			output_schema: semi_output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
		},
		anti_stage: StreamingSemiAntiStage {
			replay_id: *anti_replay_id,
			kind: JoinKind::LeftAnti,
			preserved_keys: anti_left_keys.clone(),
			existence: anti_right.as_ref(),
			existence_keys: anti_right_keys.clone(),
			match_filter: anti_filter,
			output_schema: anti_output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
		},
	})
}
impl Executor {
	pub(in crate::exec) async fn try_streaming_join0_filter(
		&self,
		input: &ExecutionPlan,
		predicate: &BoundExprProgram,
		disposition: FilterDisposition,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		if disposition == FilterDisposition::FullyPushed {
			return Ok(None);
		}

		let mut output = Vec::new();
		let mut accumulated_bytes = 0u64;
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				input,
				predicate,
				disposition,
				morsel,
				|batch| {
					accumulated_bytes =
						accumulated_bytes.saturating_add(record_batch_memory_bytes(&batch));
					if accumulated_bytes > accumulator_cap {
						return Err(budget_exhausted(
							BudgetDimension::Memory,
							"streamed join filter accumulator",
							accumulated_bytes,
							accumulator_cap,
						));
					}
					if batch.num_rows() > 0 {
						output.push(batch);
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		if output.is_empty() {
			output.push(RecordBatch::new_empty(input.schema()));
		}
		Ok(Some(output))
	}

	pub(in crate::exec) async fn try_streaming_join0_filter_exchange(
		&self,
		input: &ExecutionPlan,
		partition_count: u32,
		key: &BoundExprProgram,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Filter {
			input: filter_input,
			predicate,
			disposition,
		} = input
		else {
			return Ok(None);
		};
		if partition_count == 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "exchange partition_count must be greater than zero".to_string(),
			});
		}

		let schema = input.schema();
		let partition_count_usize =
			usize::try_from(partition_count).expect("partition count fits usize");
		let mut partition_batches: Vec<Vec<RecordBatch>> =
			(0..partition_count_usize).map(|_| Vec::new()).collect();
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let mut accumulated_bytes = 0u64;
		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				filter_input,
				predicate,
				*disposition,
				morsel,
				|batch| {
					for (partition, partitioned) in
						self.repartition_exchange_batch(&batch, partition_count, key)?
					{
						accumulated_bytes = accumulated_bytes
							.saturating_add(record_batch_memory_bytes(&partitioned));
						if accumulated_bytes > accumulator_cap {
							return Err(budget_exhausted(
								BudgetDimension::Memory,
								"streamed exchange output accumulator",
								accumulated_bytes,
								accumulator_cap,
							));
						}
						partition_batches[partition].push(partitioned);
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		Ok(Some(finalize_exchange_partitions(
			schema,
			partition_batches,
		)?))
	}

	pub(in crate::exec) async fn try_streaming_correlated_scalar_left_outer_global_aggregate(
		&self,
		input: &ExecutionPlan,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Filter {
			input: recombine,
			predicate: recombine_filter,
			disposition: FilterDisposition::EvaluateInToron,
		} = input
		else {
			return Ok(None);
		};
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::LeftOuter,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: recombine_output_schema,
			..
		} = recombine.as_ref()
		else {
			return Ok(None);
		};
		let ExecutionPlan::Exchange {
			input: preserved_exchange_input,
			partition_count,
			key: preserved_exchange_key,
		} = left.as_ref()
		else {
			return Ok(None);
		};
		let ExecutionPlan::Filter {
			input: preserved_filter_input,
			predicate: preserved_filter,
			disposition,
		} = preserved_exchange_input.as_ref()
		else {
			return Ok(None);
		};
		if *partition_count == 0 {
			return Err(ExecutionError::InvalidExpression {
				message: "exchange partition_count must be greater than zero".to_string(),
			});
		}

		let scalar_batches = self.execute_bound(right, morsel).await?;
		let scalar_batch = concat_or_empty(right.schema(), &scalar_batches)?;
		let scalar_key_arrays = key_arrays(&scalar_batch, right_keys)?;
		let key_plan = JoinKeyPlan::from_build_arrays_and_probe_schema(
			&scalar_key_arrays,
			&left.schema(),
			left_keys,
			*null_safe_keys,
		)?;
		let build_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let estimated_build_bytes =
			estimated_build_hash_bytes(&scalar_key_arrays, scalar_batch.num_rows());
		if estimated_build_bytes > build_cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"correlated scalar recombine build accumulator",
				estimated_build_bytes,
				build_cap,
			));
		}
		let Some(scalar_rows) = build_in_memory_join_rows(
			key_plan,
			&scalar_key_arrays,
			scalar_batch.num_rows(),
			build_cap,
			*null_safe_keys,
		)?
		else {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"correlated scalar recombine build accumulator",
				estimated_build_bytes,
				build_cap,
			));
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(scalar_batch.num_rows() as u64);
		});
		let mut runtime = StreamingLeftOuterRecombineRuntime {
			replay_id: *replay_id,
			strategy: *strategy,
			scalar_batch,
			scalar_rows,
			key_plan,
			preserved_keys: left_keys.clone(),
			match_filter: match_filter.as_ref(),
			null_safe_keys: *null_safe_keys,
			output_schema: recombine_output_schema.clone(),
			probe_rows: 0,
			output_rows: 0,
		};

		let partition_count_usize =
			usize::try_from(*partition_count).expect("partition count fits usize");
		let mut partition_batches: Vec<Vec<RecordBatch>> =
			(0..partition_count_usize).map(|_| Vec::new()).collect();
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let mut accumulated_bytes = 0u64;
		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				preserved_filter_input,
				preserved_filter,
				*disposition,
				morsel,
				|batch| {
					for (partition, partitioned) in self.repartition_exchange_batch(
						&batch,
						*partition_count,
						preserved_exchange_key,
					)? {
						accumulated_bytes = accumulated_bytes
							.saturating_add(record_batch_memory_bytes(&partitioned));
						if accumulated_bytes > accumulator_cap {
							return Err(budget_exhausted(
								BudgetDimension::Memory,
								"correlated scalar preserved-side exchange accumulator",
								accumulated_bytes,
								accumulator_cap,
							));
						}
						if partitioned.num_rows() > 0 {
							partition_batches[partition].push(partitioned);
						}
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}

		let mut state = init_global_aggregate_state(aggr_expr)?;
		for partition in partition_batches {
			for preserved_batch in partition {
				let recombined_batches =
					self.probe_streaming_left_outer_recombine(&mut runtime, &preserved_batch)?;
				for recombined in recombined_batches {
					let filtered = self.filter_batch_with_bound_predicate(
						recombine_filter,
						recombined,
						"filter predicate",
					)?;
					if filtered.num_rows() > 0 {
						self.fold_global_aggregate_state(&mut state, aggr_expr, &filtered)?;
					}
				}
			}
		}
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: runtime.replay_id,
				strategy: runtime.strategy,
				build_rows: RowCount::new(runtime.scalar_batch.num_rows() as u64),
				probe_rows: RowCount::new(runtime.probe_rows),
				output_rows: RowCount::new(runtime.output_rows),
			},
		);

		finalize_global_aggregate_state(&state, aggr_expr, output_schema).map(Some)
	}

	pub(in crate::exec) async fn try_visit_streaming_join0_filter_chunks<F>(
		&self,
		input: &ExecutionPlan,
		predicate: &BoundExprProgram,
		disposition: FilterDisposition,
		morsel: Option<(MorselId, u32)>,
		visit_filtered_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if disposition == FilterDisposition::FullyPushed {
			return Ok(None);
		}
		self.try_visit_streaming_join0_chunks(input, Some(predicate), morsel, visit_filtered_batch)
			.await
	}

	pub(in crate::exec) async fn try_visit_streaming_join0_chunks<F>(
		&self,
		input: &ExecutionPlan,
		filter_predicate: Option<&BoundExprProgram>,
		morsel: Option<(MorselId, u32)>,
		mut visit_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema,
			..
		} = input
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) {
			return Ok(None);
		}

		let left_batches = self.execute_bound(left, morsel).await?;
		let right_batches = self.execute_bound(right, morsel).await?;
		let build_started = Instant::now();
		if let Some((mut spilled, key_plan)) = try_create_streamed_spilled_inner_build(
			left.schema(),
			right.schema(),
			&left_batches,
			JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter.as_ref(),
			*null_safe_keys,
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?
		{
			record_task_telemetry(&self.context.task_telemetry, |task| {
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
			let result = if let Some(predicate) = filter_predicate {
				visit_streamed_spilled_inner_join_filtered_chunks(
					self,
					&mut spilled,
					&right_batches,
					right_keys,
					key_plan,
					*null_safe_keys,
					output_schema,
					predicate,
					self.context.task_budget.memory.get(),
					&mut visit_batch,
				)
				.await
			} else {
				visit_streamed_spilled_inner_join_chunks(
					&mut spilled,
					&right_batches,
					right_keys,
					key_plan,
					*null_safe_keys,
					output_schema,
					self.context.task_budget.memory.get(),
					&mut visit_batch,
				)
				.await
			};
			let build_rows = spilled.input_row_count();
			record_task_telemetry(&self.context.task_telemetry, |task| {
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
					&self.context.task_telemetry,
					&self.context.join_runtime_observation_mode,
					JoinRuntimeObservation {
						replay_id: *replay_id,
						strategy: *strategy,
						build_rows: RowCount::new(build_rows),
						probe_rows: RowCount::new(*probe_rows),
						output_rows: RowCount::new(*output_rows),
					},
				);
			}
			spilled.cleanup().await;
			result?;
			return Ok(Some(()));
		}

		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let right_batch = concat_or_empty(right.schema(), &right_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		let right_key_arrays = key_arrays(&right_batch, right_keys)?;
		record_task_telemetry(&self.context.task_telemetry, |task| {
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
			> self.context.task_budget.memory.get()
		{
			let output_rows = visit_spilled_hash_join_filtered_chunks(
				self,
				JoinKind::Inner,
				&left_batch,
				&right_batch,
				&left_key_arrays,
				&right_key_arrays,
				output_schema,
				filter_predicate,
				match_filter.as_ref(),
				*null_safe_keys,
				self.context.task_budget,
				self.context.task_telemetry.clone(),
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				&mut visit_batch,
			)
			.await?;
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: *replay_id,
					strategy: *strategy,
					build_rows: RowCount::new(left_batch.num_rows() as u64),
					probe_rows: RowCount::new(right_batch.num_rows() as u64),
					output_rows: RowCount::new(output_rows),
				},
			);
			return Ok(Some(()));
		}
		let join_key_plan =
			JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, *null_safe_keys);
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&left_key_arrays,
			left_batch.num_rows(),
			self.context.task_budget.memory.get(),
			*null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		let probe_started = Instant::now();
		let selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			join_key_plan,
			&build_rows,
			left_batch.num_rows(),
			&right_key_arrays,
			right_batch.num_rows(),
			match_filter.as_ref(),
			&left_batch,
			&right_batch,
			*null_safe_keys,
		)?;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});
		visit_join_selection_filtered_chunks(
			self,
			JoinKind::Inner,
			&left_batch,
			&right_batch,
			&selection,
			output_schema,
			filter_predicate,
			self.context.task_budget.memory.get(),
			&mut visit_batch,
		)?;
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(right_batch.num_rows() as u64),
				output_rows: RowCount::new(selection.len() as u64),
			},
		);
		Ok(Some(()))
	}

	pub(in crate::exec) async fn try_streaming_collapsing_global_aggregate(
		&self,
		input: &ExecutionPlan,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let (join, filter_predicate) = match input {
			ExecutionPlan::Join {
				kind: JoinKind::Inner,
				..
			} => (input, None),
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => {
				let ExecutionPlan::Join {
					kind: JoinKind::Inner,
					..
				} = input.as_ref()
				else {
					return Ok(None);
				};
				let filter_predicate = match disposition {
					FilterDisposition::FullyPushed => None,
					FilterDisposition::EvaluateInToron => Some(predicate),
				};
				(input.as_ref(), filter_predicate)
			}
			_ => return Ok(None),
		};

		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: join_output_schema,
			..
		} = join
		else {
			return Ok(None);
		};

		let left_batches = self.execute_bound(left, morsel).await?;
		let right_batches = self.execute_bound(right, morsel).await?;
		let mut state = init_global_aggregate_state(aggr_expr)?;
		execute_hash_join_into_global_aggregate(
			self,
			left.schema(),
			right.schema(),
			&left_batches,
			&right_batches,
			*kind,
			left_keys,
			right_keys,
			match_filter.as_ref(),
			*null_safe_keys,
			*replay_id,
			*strategy,
			join_output_schema,
			filter_predicate,
			aggr_expr,
			&mut state,
			self.context.task_budget,
			self.context.task_telemetry.clone(),
			self.context.join_runtime_observation_mode.clone(),
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		finalize_global_aggregate_state(&state, aggr_expr, output_schema).map(Some)
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn try_streaming_projected_left_semi_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: aggregate_input,
			partition_count,
			key: exchange_key,
		} = input
		else {
			return Ok(None);
		};
		if exchange_key != group_expr && !grouped_aggregate_is_order_insensitive(aggr_expr) {
			return Ok(None);
		}
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::LeftSemi,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys,
			strategy,
			output_schema: semi_output_schema,
			..
		} = aggregate_input.as_ref()
		else {
			return Ok(None);
		};

		let retained_columns =
			grouped_aggregate_input_columns(group_expr, aggr_expr, exchange_key, left_keys);
		if retained_columns.is_empty()
			|| retained_columns.len() >= semi_output_schema.fields().len()
		{
			return Ok(None);
		}
		let projected_left_keys = remap_column_ordinals(&retained_columns, left_keys)?;
		let projected_schema = projected_schema_for_columns(semi_output_schema, &retained_columns)?;
		let left_batches = self.execute_bound(left, morsel).await?;
		let estimated_build_bytes =
			estimated_build_hash_bytes_for_batches(&left_batches, left_keys)?;
		if estimated_build_bytes <= self.context.task_budget.memory.get() {
			return Ok(None);
		}

		let right_batches = self.execute_bound(right, morsel).await?;
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&projected_schema,
			&projected_left_keys,
			&right.schema(),
			right_keys,
			*null_safe_keys,
		)?;
		let mut spilled = SpilledInnerJoinBuild::create(
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		let build_started = Instant::now();
		for batch in &left_batches {
			spilled
				.write_projected_batch_chunks(
					batch,
					&retained_columns,
					key_plan,
					&projected_left_keys,
					*null_safe_keys,
					self.context.task_budget,
				)
				.await?;
		}
		spilled.finish(self.context.task_budget).await?;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(spilled.input_row_count());
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});

		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();
		let probe_started = Instant::now();
		let result = self
			.fold_projected_streamed_left_semi_grouped_aggregate(
				&mut spilled,
				&right_batches,
				right_keys,
				key_plan,
				*null_safe_keys,
				semi_output_schema,
				&retained_columns,
				*partition_count,
				exchange_key,
				group_expr,
				aggr_expr,
				&mut groups,
				&mut group_types,
			)
			.await;
		let build_rows = spilled.input_row_count();
		record_task_telemetry(&self.context.task_telemetry, |task| {
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
		let output = result.and_then(|(probe_rows, output_rows)| {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: *replay_id,
					strategy: *strategy,
					build_rows: RowCount::new(build_rows),
					probe_rows: RowCount::new(probe_rows),
					output_rows: RowCount::new(output_rows),
				},
			);
			self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema)
		});
		spilled.cleanup().await;
		output.map(Some)
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn fold_projected_streamed_left_semi_grouped_aggregate(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		right_batches: &[RecordBatch],
		right_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		semi_output_schema: &arrow::datatypes::SchemaRef,
		retained_columns: &[ColumnOrdinal],
		partition_count: u32,
		exchange_key: &BoundExprProgram,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
	) -> Result<(u64, u64), ExecutionError> {
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

		let mut refs = Vec::new();
		let mut output_rows = 0u64;
		for (page_index, rows) in matched {
			for row in rows {
				refs.push(SpilledBatchRowRef { page_index, row });
				if refs.len() == CROSS_JOIN_OUTPUT_CHUNK_ROWS {
					self.fold_projected_left_semi_refs(
						spilled,
						&refs,
						semi_output_schema,
						retained_columns,
						partition_count,
						exchange_key,
						group_expr,
						aggr_expr,
						groups,
						group_types,
					)
					.await?;
					output_rows = output_rows.saturating_add(refs.len() as u64);
					refs.clear();
				}
			}
		}
		if !refs.is_empty() {
			self.fold_projected_left_semi_refs(
				spilled,
				&refs,
				semi_output_schema,
				retained_columns,
				partition_count,
				exchange_key,
				group_expr,
				aggr_expr,
				groups,
				group_types,
			)
			.await?;
			output_rows = output_rows.saturating_add(refs.len() as u64);
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn fold_projected_left_semi_refs(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		refs: &[SpilledBatchRowRef],
		semi_output_schema: &arrow::datatypes::SchemaRef,
		retained_columns: &[ColumnOrdinal],
		partition_count: u32,
		exchange_key: &BoundExprProgram,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
	) -> Result<(), ExecutionError> {
		let batch = materialize_projected_streamed_spilled_build_rows(
			spilled,
			refs,
			semi_output_schema,
			retained_columns,
			self.context.task_budget.memory.get(),
		)
		.await?;
		if batch.num_rows() == 0 {
			return Ok(());
		}
		for (_partition, partitioned) in
			self.repartition_exchange_batch(&batch, partition_count, exchange_key)?
		{
			if partitioned.num_rows() == 0 {
				continue;
			}
			self.fold_bound_grouped_aggregate_batch(
				groups,
				group_types,
				group_expr,
				aggr_expr,
				&partitioned,
			)?;
		}
		Ok(())
	}

	pub(in crate::exec) async fn try_streaming_reoriented_inner_chain_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: aggregate_input,
			..
		} = input
		else {
			return Ok(None);
		};
		let ExecutionPlan::Project {
			input: project_input,
			exprs: project_exprs,
			output_schema: project_output_schema,
		} = aggregate_input.as_ref()
		else {
			return Ok(None);
		};
		let (chain_input, outer_predicate) = match project_input.as_ref() {
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition: FilterDisposition::EvaluateInToron,
			} => (input.as_ref(), Some(predicate)),
			ExecutionPlan::Filter {
				input,
				disposition: FilterDisposition::FullyPushed,
				..
			} => (input.as_ref(), None),
			other => (other, None),
		};
		let Some((base, stages)) = extract_reoriented_inner_chain(chain_input) else {
			return Ok(None);
		};

		let mut runtimes = Vec::with_capacity(stages.len());
		for stage in stages {
			let Some(runtime) = self
				.prepare_reoriented_inner_join_runtime(stage, morsel)
				.await?
			else {
				return Ok(None);
			};
			runtimes.push(runtime);
		}

		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();
		let matched = self
			.try_visit_streaming_reoriented_inner_chain_base(base, morsel, |base_batch| {
				let mut batches = vec![base_batch];
				for runtime in runtimes.iter_mut() {
					let mut next = Vec::new();
					for batch in batches {
						next.extend(self.probe_reoriented_inner_join(runtime, &batch)?);
					}
					batches = next;
					if batches.is_empty() {
						break;
					}
					if let Some(predicate) = runtime.post_filter {
						let mut filtered = Vec::new();
						for batch in batches {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								filtered.push(batch);
							}
						}
						batches = filtered;
					}
				}

				for batch in batches {
					let filtered = if let Some(predicate) = outer_predicate {
						self.filter_batch_with_bound_predicate(
							predicate,
							batch,
							"filter predicate",
						)?
					} else {
						batch
					};
					if filtered.num_rows() == 0 {
						continue;
					}
					let projected = RecordBatch::try_new(
						project_output_schema.clone(),
						self.evaluate_bound_program(project_exprs, &filtered)?,
					)?;
					self.fold_bound_grouped_aggregate_batch(
						&mut groups,
						&mut group_types,
						group_expr,
						aggr_expr,
						&projected,
					)?;
				}
				Ok(())
			})
			.await?;
		if matched.is_none() {
			return Ok(None);
		}

		for runtime in &runtimes {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.build_batch.num_rows() as u64),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
		}

		self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema)
			.map(Some)
	}

	pub(in crate::exec) async fn try_streaming_inner_prefix_semi_anti_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: aggregate_input,
			..
		} = input
		else {
			return Ok(None);
		};
		let Some(chain) = extract_streaming_inner_prefix_semi_anti_chain(aggregate_input) else {
			return Ok(None);
		};

		let mut inner_runtimes = Vec::with_capacity(chain.inner_stages.len());
		for stage in chain.inner_stages {
			let Some(runtime) = self
				.prepare_reoriented_inner_join_runtime(stage, morsel)
				.await?
			else {
				return Ok(None);
			};
			inner_runtimes.push(runtime);
		}
		let Some(mut semi_runtime) = self
			.prepare_streaming_semi_anti_runtime(chain.semi_stage, morsel)
			.await?
		else {
			return Ok(None);
		};
		let Some(mut anti_runtime) = self
			.prepare_streaming_semi_anti_runtime(chain.anti_stage, morsel)
			.await?
		else {
			return Ok(None);
		};

		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();
		let ExecutionPlan::Exchange {
			input: base_input,
			partition_count,
			key: base_exchange_key,
		} = chain.base
		else {
			return Ok(None);
		};
		let ExecutionPlan::Filter {
			input: join0,
			predicate: join0_filter,
			disposition,
		} = base_input.as_ref()
		else {
			return Ok(None);
		};
		if *disposition == FilterDisposition::FullyPushed {
			return Ok(None);
		}
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema: join0_output_schema,
			..
		} = join0.as_ref()
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) {
			return Ok(None);
		}

		let left_batches = self.execute_bound(left, morsel).await?;
		let right_batches = self.execute_bound(right, morsel).await?;
		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let right_batch = concat_or_empty(right.schema(), &right_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		let right_key_arrays = key_arrays(&right_batch, right_keys)?;
		if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan =
			JoinKeyPlan::from_key_arrays(&left_key_arrays, &right_key_arrays, *null_safe_keys);
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&left_key_arrays,
			left_batch.num_rows(),
			self.context.task_budget.memory.get(),
			*null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(left_batch.num_rows() as u64);
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(right_batch.num_rows() as u64);
		});
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		let probe_started = Instant::now();
		let selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			join_key_plan,
			&build_rows,
			left_batch.num_rows(),
			&right_key_arrays,
			right_batch.num_rows(),
			match_filter.as_ref(),
			&left_batch,
			&right_batch,
			*null_safe_keys,
		)?;
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});

		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_join_selection(
				JoinKind::Inner,
				&left_batch,
				&right_batch,
				chunk,
				join0_output_schema,
				self.context.task_budget.memory.get(),
			)?;
			let batch =
				self.filter_batch_with_bound_predicate(join0_filter, batch, "filter predicate")?;
			if batch.num_rows() == 0 {
				continue;
			}
			for (_partition, partitioned) in
				self.repartition_exchange_batch(&batch, *partition_count, base_exchange_key)?
			{
				if partitioned.num_rows() == 0 {
					continue;
				}
				let mut batches = vec![partitioned];
				for runtime in inner_runtimes.iter_mut() {
					let mut next = Vec::new();
					for batch in batches {
						next.extend(self.probe_reoriented_inner_join(runtime, &batch)?);
					}
					batches = next;
					if let Some(predicate) = runtime.post_filter {
						let mut filtered = Vec::new();
						for batch in batches {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								filtered.push(batch);
							}
						}
						batches = filtered;
					}
					if batches.is_empty() {
						break;
					}
				}

				let mut semi_batches = Vec::new();
				for batch in batches {
					semi_batches.extend(
						self.probe_streaming_semi_anti(&mut semi_runtime, &batch)
							.await?,
					);
				}
				let mut anti_batches = Vec::new();
				for batch in semi_batches {
					anti_batches.extend(
						self.probe_streaming_semi_anti(&mut anti_runtime, &batch)
							.await?,
					);
				}
				for batch in anti_batches {
					self.fold_bound_grouped_aggregate_batch(
						&mut groups,
						&mut group_types,
						group_expr,
						aggr_expr,
						&batch,
					)?;
				}
			}
		}
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(right_batch.num_rows() as u64),
				output_rows: RowCount::new(selection.len() as u64),
			},
		);

		for runtime in &inner_runtimes {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.build_batch.num_rows() as u64),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
		}
		for runtime in [&semi_runtime, &anti_runtime] {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.existence_row_count),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
		}

		let output =
			self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema);
		semi_runtime.cleanup().await;
		anti_runtime.cleanup().await;
		output.map(Some)
	}

	pub(in crate::exec) async fn prepare_reoriented_inner_join_runtime<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		let build_batches = self.execute_bound(stage.build, morsel).await?;
		let build_batch = concat_or_empty(stage.build.schema(), &build_batches)?;
		let build_key_arrays = key_arrays(&build_batch, &stage.build_keys)?;
		let build_memory_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let key_plan = JoinKeyPlan::from_build_arrays_and_probe_schema(
			&build_key_arrays,
			&stage.probe_schema,
			&stage.probe_keys,
			false,
		)?;
		let estimated_build_bytes =
			estimated_build_hash_bytes(&build_key_arrays, build_batch.num_rows());
		if estimated_build_bytes > build_memory_cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"reoriented join build accumulator",
				estimated_build_bytes,
				build_memory_cap,
			));
		}
		let Some(build_rows) = build_in_memory_join_rows(
			key_plan,
			&build_key_arrays,
			build_batch.num_rows(),
			build_memory_cap,
			false,
		)?
		else {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"reoriented join build accumulator",
				estimated_build_bytes,
				build_memory_cap,
			));
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(build_batch.num_rows() as u64);
		});
		Ok(Some(ReorientedInnerJoinRuntime {
			replay_id: stage.replay_id,
			strategy: stage.strategy,
			build_batch,
			build_rows,
			key_plan,
			probe_keys: stage.probe_keys,
			output_schema: stage.output_schema,
			post_filter: stage.post_filter,
			probe_columns_first: stage.probe_columns_first,
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	pub(in crate::exec) async fn prepare_streaming_semi_anti_runtime<'a>(
		&self,
		stage: StreamingSemiAntiStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<StreamingSemiAntiRuntime<'a>>, ExecutionError> {
		debug_assert!(matches!(
			stage.kind,
			JoinKind::LeftSemi | JoinKind::LeftAnti
		));
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&stage.existence.schema(),
			&stage.existence_keys,
			&stage.output_schema,
			&stage.preserved_keys,
			false,
		)?;
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		let Some(()) = self
			.write_streaming_semi_anti_existence(
				stage.existence,
				&stage.existence_keys,
				key_plan,
				morsel,
				&mut spilled,
			)
			.await?
		else {
			spilled.cleanup().await;
			return Ok(None);
		};
		spilled.finish(self.context.task_budget).await?;
		let existence_row_count = spilled.input_row_count();
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(existence_row_count);
			task.counters.spill_bytes = task
				.counters
				.spill_bytes
				.saturating_add(spilled.spill_bytes());
		});
		Ok(Some(StreamingSemiAntiRuntime {
			replay_id: stage.replay_id,
			kind: stage.kind,
			strategy: stage.strategy,
			existence: StreamingSemiAntiExistence::Spilled(spilled),
			existence_row_count,
			key_plan,
			preserved_keys: stage.preserved_keys,
			match_filter: stage.match_filter,
			output_schema: stage.output_schema,
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	pub(in crate::exec) async fn write_streaming_semi_anti_existence(
		&self,
		plan: &ExecutionPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		morsel: Option<(MorselId, u32)>,
		spilled: &mut SpilledSemiAntiExistenceBuild,
	) -> Result<Option<()>, ExecutionError> {
		let Some((scan_plan, ops)) = decompose_streaming_existence_plan(plan) else {
			return Ok(None);
		};
		let ExecutionPlan::Scan {
			local_table_id,
			table,
			fingerprint,
			projection,
			pbc_program,
			row_limit,
			row_identity,
			profile_hint,
			..
		} = scan_plan
		else {
			return Ok(None);
		};
		let req = ScanRequest {
			session_id: self.context.session_id,
			query_id: self.context.query_id,
			tenant_id: self.context.tenant_id,
			table_id: *local_table_id,
			table_object_id: *table,
			physical_descriptor_fingerprint: *fingerprint,
			read_bound: self.context.read_bound,
			projected_columns: projection.clone(),
			pbc_predicate: pbc_program.as_ref().map(|program| program.0.clone()),
			runtime_filter: self.context.runtime_filter.clone(),
			row_identity: *row_identity,
			profile_hint: Some(*profile_hint),
			trace_context: self.context.trace_context,
			morsel,
			row_limit: *row_limit,
			downstream_cancellation: self.context.downstream_cancellation.clone(),
		};
		let span = crate::telemetry::storage_scan_span(&req);
		let stream = self.storage.scan(req).instrument(span).await?;
		let mut stream = stream.into_batches();
		let mut remaining = *row_limit;
		while let Some(batch) = stream.next().await {
			let mut batch = batch?;
			if let Some(limit) = remaining.as_mut() {
				if *limit == 0 {
					break;
				}
				if batch.num_rows() > *limit {
					batch = batch.slice(0, *limit);
				}
				*limit = (*limit).saturating_sub(batch.num_rows());
			}
			let mut batches = vec![batch];
			for op in &ops {
				let mut next = Vec::new();
				for batch in batches {
					match op {
						StreamingExistenceOp::Filter(predicate) => {
							let batch = self.filter_batch_with_bound_predicate(
								predicate,
								batch,
								"filter predicate",
							)?;
							if batch.num_rows() > 0 {
								next.push(batch);
							}
						}
						StreamingExistenceOp::Project {
							exprs,
							output_schema,
						} => {
							let columns = self.evaluate_bound_program(exprs, &batch)?;
							next.push(RecordBatch::try_new(output_schema.clone(), columns)?);
						}
						StreamingExistenceOp::Exchange {
							partition_count,
							key,
						} => {
							if *partition_count == 0 {
								return Err(ExecutionError::InvalidExpression {
									message: "exchange partition_count must be greater than zero"
										.to_string(),
								});
							}
							for (_partition, partitioned) in
								self.repartition_exchange_batch(&batch, *partition_count, key)?
							{
								if partitioned.num_rows() > 0 {
									next.push(partitioned);
								}
							}
						}
					}
				}
				batches = next;
				if batches.is_empty() {
					break;
				}
			}
			for batch in batches {
				spilled
					.write_batch_chunks(&batch, key_plan, existence_keys, self.context.task_budget)
					.await?;
			}
		}
		Ok(Some(()))
	}

	pub(in crate::exec) async fn try_visit_streaming_reoriented_inner_chain_base<F>(
		&self,
		base: &ExecutionPlan,
		morsel: Option<(MorselId, u32)>,
		mut visit_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} = base
		else {
			return Ok(None);
		};
		let (join0, filter_predicate) = match input.as_ref() {
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition: FilterDisposition::EvaluateInToron,
			} => (input.as_ref(), Some(predicate)),
			ExecutionPlan::Filter {
				input,
				disposition: FilterDisposition::FullyPushed,
				..
			} => (input.as_ref(), None),
			other => (other, None),
		};

		let matched = self
			.try_visit_streaming_join0_chunks(join0, filter_predicate, morsel, |batch| {
				for (_partition, partitioned) in
					self.repartition_exchange_batch(&batch, *partition_count, key)?
				{
					if partitioned.num_rows() > 0 {
						visit_batch(partitioned)?;
					}
				}
				Ok(())
			})
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		Ok(Some(()))
	}

	pub(in crate::exec) async fn probe_streaming_semi_anti(
		&self,
		runtime: &mut StreamingSemiAntiRuntime<'_>,
		preserved_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if preserved_batch.num_rows() == 0 {
			return Ok(Vec::new());
		}
		let preserved_key_arrays = key_arrays(preserved_batch, &runtime.preserved_keys)?;
		runtime.probe_rows = runtime
			.probe_rows
			.saturating_add(preserved_batch.num_rows() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(preserved_batch.num_rows() as u64);
		});

		let mut preserved_by_key: HashMap<JoinKey, Vec<u32>> = HashMap::new();
		for row in 0..preserved_batch.num_rows() {
			if let Some(key) = join_key_at(runtime.key_plan, &preserved_key_arrays, row, false)? {
				preserved_by_key
					.entry(key)
					.or_default()
					.push(join_probe_row_index(row)?);
			}
		}
		let matched_rows = match &mut runtime.existence {
			StreamingSemiAntiExistence::InMemory { batch, rows } => {
				matching_preserved_rows_in_memory(
					preserved_batch,
					&preserved_by_key,
					runtime.match_filter,
					batch,
					rows,
				)?
			}
			StreamingSemiAntiExistence::Spilled(spilled) => {
				spilled
					.matching_preserved_rows(
						preserved_batch,
						&preserved_by_key,
						runtime.match_filter,
					)
					.await?
			}
		};
		let mut selected = Vec::<u32>::new();
		for row in 0..preserved_batch.num_rows() {
			let matched = matched_rows[row];
			if (runtime.kind == JoinKind::LeftSemi && matched)
				|| (runtime.kind == JoinKind::LeftAnti && !matched)
			{
				selected.push(join_probe_row_index(row)?);
			}
		}
		runtime.output_rows = runtime.output_rows.saturating_add(selected.len() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selected.len() as u64);
		});

		let mut output = Vec::new();
		for chunk in selected.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_preserved_selection(
				preserved_batch,
				chunk,
				&runtime.output_schema,
				self.context.task_budget.memory.get(),
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok(output)
	}

	pub(in crate::exec) fn probe_streaming_left_outer_recombine(
		&self,
		runtime: &mut StreamingLeftOuterRecombineRuntime<'_>,
		preserved_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if preserved_batch.num_rows() == 0 {
			return Ok(Vec::new());
		}
		let preserved_key_arrays = key_arrays(preserved_batch, &runtime.preserved_keys)?;
		runtime.probe_rows = runtime
			.probe_rows
			.saturating_add(preserved_batch.num_rows() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(preserved_batch.num_rows() as u64);
		});

		let mut selection = Vec::new();
		let mut residual =
			JoinMatchResidual::new(runtime.match_filter, preserved_batch, &runtime.scalar_batch);
		for row in 0..preserved_batch.num_rows() {
			let left = join_build_row_index(row)?;
			let matched = if let Some(key) = join_key_at(
				runtime.key_plan,
				&preserved_key_arrays,
				row,
				runtime.null_safe_keys,
			)? {
				if let Some(matches) = runtime.scalar_rows.get(&key) {
					let mut matched = false;
					for right in matches.rows() {
						if residual.matches(left, right)? {
							matched = true;
							selection.push(JoinOutputRow::Paired { left, right });
						}
					}
					matched
				} else {
					false
				}
			} else {
				false
			};
			if !matched {
				selection.push(JoinOutputRow::LeftFillNull { left });
			}
		}
		runtime.output_rows = runtime.output_rows.saturating_add(selection.len() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});

		let mut output = Vec::new();
		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_join_selection(
				JoinKind::LeftOuter,
				preserved_batch,
				&runtime.scalar_batch,
				chunk,
				&runtime.output_schema,
				self.context.task_budget.memory.get(),
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok(output)
	}

	pub(in crate::exec) fn probe_reoriented_inner_join(
		&self,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		probe_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		if probe_batch.num_rows() == 0 {
			return Ok(Vec::new());
		}
		let probe_key_arrays = key_arrays(probe_batch, &runtime.probe_keys)?;
		runtime.probe_rows = runtime
			.probe_rows
			.saturating_add(probe_batch.num_rows() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task
				.counters
				.probe_rows
				.saturating_add(probe_batch.num_rows() as u64);
		});
		let mut selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			runtime.key_plan,
			&runtime.build_rows,
			runtime.build_batch.num_rows(),
			&probe_key_arrays,
			probe_batch.num_rows(),
			None,
			&runtime.build_batch,
			probe_batch,
			false,
		)?;
		runtime.output_rows = runtime.output_rows.saturating_add(selection.len() as u64);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.matched_rows = task
				.counters
				.matched_rows
				.saturating_add(selection.len() as u64);
		});
		let mut output = Vec::new();
		for chunk in selection.chunks_mut(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_reoriented_inner_selection(
				probe_batch,
				&runtime.build_batch,
				chunk,
				&runtime.output_schema,
				self.context.task_budget.memory.get(),
				runtime.probe_columns_first,
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok(output)
	}
}
fn matching_preserved_rows_in_memory(
	preserved_batch: &RecordBatch,
	preserved_by_key: &HashMap<JoinKey, Vec<u32>>,
	match_filter: &BoundExprProgram,
	existence_batch: &RecordBatch,
	existence_rows: &HashMap<JoinKey, BuildRowList>,
) -> Result<Vec<bool>, ExecutionError> {
	let mut matched = vec![false; preserved_batch.num_rows()];
	let mut residual = JoinMatchResidual::new(Some(match_filter), preserved_batch, existence_batch);
	for (key, preserved_rows) in preserved_by_key {
		let Some(matches) = existence_rows.get(key) else {
			continue;
		};
		for preserved_row in preserved_rows {
			if matched[*preserved_row as usize] {
				continue;
			}
			for existence_row in matches.rows() {
				if residual.matches(*preserved_row, existence_row)? {
					matched[*preserved_row as usize] = true;
					break;
				}
			}
		}
	}
	Ok(matched)
}
