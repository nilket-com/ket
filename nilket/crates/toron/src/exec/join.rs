use super::*;

pub(super) const JOIN_SPILL_PAGE_SIZE: usize = 64 * 1024;
pub(super) const JOIN_SPILL_CACHE_TARGET_BYTES: u64 = 8 * 1024 * 1024;
/// ADR 335: the raw 64 MiB task cap false-trips accepted sf=1 controls; the output guard
/// targets the GiB-scale materializations that cgroup-kill before the engine can report a row.
pub(super) const JOIN_OUTPUT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER: u64 = 16;
/// ADR 336: Phase 0 measured the largest accepted spill-completer at 440,765,522
/// retained bytes and the retained-input wall at 1,423,571,737; 16x the default task memory
/// cap sits in that gap.
pub(super) const JOIN_SPILL_RETAINED_FOOTPRINT_MEMORY_CAP_MULTIPLIER: u64 = 16;
pub(super) static NEXT_JOIN_SPILL_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct ReorientedInnerJoinStage<'a> {
	replay_id: JoinReplayId,
	probe_schema: SchemaRef,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	build: &'a ExecutionPlan,
	build_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	strategy: JoinStrategy,
	post_filter: Option<&'a BoundExprProgram>,
}

pub(super) struct ReorientedInnerJoinRuntime<'a> {
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	build_batch: RecordBatch,
	build_rows: HashMap<JoinKey, BuildRowList>,
	key_plan: JoinKeyPlan,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	post_filter: Option<&'a BoundExprProgram>,
	probe_rows: u64,
	output_rows: u64,
}

pub(super) struct StreamingSemiAntiStage<'a> {
	pub(super) replay_id: JoinReplayId,
	pub(super) kind: JoinKind,
	pub(super) preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(super) existence: &'a ExecutionPlan,
	pub(super) existence_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(super) match_filter: &'a BoundExprProgram,
	pub(super) output_schema: SchemaRef,
	pub(super) strategy: JoinStrategy,
}

#[allow(dead_code)]
pub(super) enum StreamingSemiAntiExistence {
	InMemory {
		batch: RecordBatch,
		rows: HashMap<JoinKey, BuildRowList>,
	},
	Spilled(SpilledSemiAntiExistenceBuild),
}

pub(super) enum StreamingExistenceOp<'a> {
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

pub(super) struct StreamingSemiAntiRuntime<'a> {
	pub(super) replay_id: JoinReplayId,
	pub(super) kind: JoinKind,
	pub(super) strategy: JoinStrategy,
	pub(super) existence: StreamingSemiAntiExistence,
	pub(super) existence_row_count: u64,
	pub(super) key_plan: JoinKeyPlan,
	pub(super) preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	pub(super) match_filter: &'a BoundExprProgram,
	pub(super) output_schema: SchemaRef,
	pub(super) probe_rows: u64,
	pub(super) output_rows: u64,
}

pub(super) struct StreamingLeftOuterRecombineRuntime<'a> {
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
	pub(super) async fn cleanup(self) {
		if let StreamingSemiAntiExistence::Spilled(spilled) = self.existence {
			spilled.cleanup().await;
		}
	}
}

pub(super) struct StreamingSemiAntiChain<'a> {
	base: &'a ExecutionPlan,
	inner_stages: Vec<ReorientedInnerJoinStage<'a>>,
	semi_stage: StreamingSemiAntiStage<'a>,
	anti_stage: StreamingSemiAntiStage<'a>,
}

pub(super) fn extract_reoriented_inner_chain(
	top: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Vec<ReorientedInnerJoinStage<'_>>)> {
	let mut current = top;
	let mut stages = Vec::new();
	for expected in (1..=4).rev() {
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
		let ExecutionPlan::Exchange {
			input: left_input, ..
		} = left.as_ref()
		else {
			return None;
		};
		stages.push(ReorientedInnerJoinStage {
			replay_id: *replay_id,
			probe_schema: left.schema(),
			probe_keys: left_keys.clone(),
			build: right.as_ref(),
			build_keys: right_keys.clone(),
			output_schema: output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
			post_filter: None,
		});
		if expected == 1 {
			stages.reverse();
			return Some((left.as_ref(), stages));
		}
		current = left_input.as_ref();
	}
	None
}

pub(super) fn filter_input<'a>(
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

pub(super) fn exchange_input(plan: &ExecutionPlan) -> Option<&ExecutionPlan> {
	let ExecutionPlan::Exchange { input, .. } = plan else {
		return None;
	};
	Some(input.as_ref())
}

pub(super) fn collect_program_column_ordinals(
	program: &BoundExprProgram,
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	for root in &program.roots {
		collect_program_node_column_ordinals(program, *root, columns);
	}
}

pub(super) fn collect_program_node_column_ordinals(
	program: &BoundExprProgram,
	id: BoundExprId,
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	match program.node(id) {
		Some(BoundExprNode::Column(column)) => {
			columns.insert(column.ordinal);
		}
		Some(BoundExprNode::Literal(_)) | Some(BoundExprNode::NullLiteral) | None => {}
		Some(BoundExprNode::Cast { input, .. })
		| Some(BoundExprNode::TryCast { input, .. })
		| Some(BoundExprNode::Unary { input, .. })
		| Some(BoundExprNode::Like { expr: input, .. })
		| Some(BoundExprNode::Substring { input, .. })
		| Some(BoundExprNode::TimeBucket { input, .. })
		| Some(BoundExprNode::AtTimeZone { input, .. })
		| Some(BoundExprNode::Extract { input, .. })
		| Some(BoundExprNode::DateTrunc { input, .. }) => {
			collect_program_node_column_ordinals(program, *input, columns);
		}
		Some(BoundExprNode::Binary { left, right, .. }) => {
			collect_program_node_column_ordinals(program, *left, columns);
			collect_program_node_column_ordinals(program, *right, columns);
		}
		Some(BoundExprNode::InList { expr, list, .. }) => {
			collect_program_node_column_ordinals(program, *expr, columns);
			for item in list {
				collect_program_node_column_ordinals(program, *item, columns);
			}
		}
		Some(BoundExprNode::Case {
			when_then,
			else_input,
		}) => {
			for (condition, result) in when_then {
				collect_program_node_column_ordinals(program, *condition, columns);
				collect_program_node_column_ordinals(program, *result, columns);
			}
			collect_program_node_column_ordinals(program, *else_input, columns);
		}
	}
}

pub(super) fn grouped_aggregate_input_columns(
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	exchange_key: &BoundExprProgram,
	join_keys: &[ColumnOrdinal],
) -> Vec<ColumnOrdinal> {
	let mut columns = BTreeSet::new();
	collect_program_column_ordinals(group_expr, &mut columns);
	collect_program_column_ordinals(exchange_key, &mut columns);
	for call in aggr_expr {
		collect_program_column_ordinals(&call.args, &mut columns);
	}
	columns.extend(join_keys.iter().copied());
	columns.into_iter().collect()
}

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

pub(super) fn decompose_streaming_existence_plan(
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

pub(super) fn extract_streaming_inner_prefix_semi_anti_chain(
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
	pub(super) async fn try_streaming_join0_filter(
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

	pub(super) async fn try_streaming_join0_filter_exchange(
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

	pub(super) async fn try_streaming_correlated_scalar_left_outer_global_aggregate(
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

	pub(super) async fn try_visit_streaming_join0_filter_chunks<F>(
		&self,
		input: &ExecutionPlan,
		predicate: &BoundExprProgram,
		disposition: FilterDisposition,
		morsel: Option<(MorselId, u32)>,
		mut visit_filtered_batch: F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if disposition == FilterDisposition::FullyPushed {
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
			let result = visit_streamed_spilled_inner_join_filtered_chunks(
				self,
				&mut spilled,
				&right_batches,
				right_keys,
				key_plan,
				*null_safe_keys,
				output_schema,
				predicate,
				self.context.task_budget.memory.get(),
				&mut visit_filtered_batch,
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
				predicate,
				match_filter.as_ref(),
				*null_safe_keys,
				self.context.task_budget,
				self.context.task_telemetry.clone(),
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
				&mut visit_filtered_batch,
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
			predicate,
			self.context.task_budget.memory.get(),
			&mut visit_filtered_batch,
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

	pub(super) async fn try_streaming_collapsing_global_aggregate(
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
	pub(super) async fn try_streaming_projected_left_semi_grouped_aggregate(
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
	pub(super) async fn fold_projected_streamed_left_semi_grouped_aggregate(
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
	pub(super) async fn fold_projected_left_semi_refs(
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

	pub(super) async fn try_streaming_reoriented_inner_chain_grouped_aggregate(
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
		let ExecutionPlan::Filter {
			input: filter_input,
			predicate: outer_predicate,
			disposition: FilterDisposition::EvaluateInToron,
		} = project_input.as_ref()
		else {
			return Ok(None);
		};
		let Some((base, stages)) = extract_reoriented_inner_chain(filter_input) else {
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
				}

				for batch in batches {
					let filtered = self.filter_batch_with_bound_predicate(
						outer_predicate,
						batch,
						"filter predicate",
					)?;
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

	pub(super) async fn try_streaming_inner_prefix_semi_anti_grouped_aggregate(
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

	pub(super) async fn prepare_reoriented_inner_join_runtime<'a>(
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
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	pub(super) async fn prepare_streaming_semi_anti_runtime<'a>(
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

	pub(super) async fn write_streaming_semi_anti_existence(
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

	pub(super) async fn try_visit_streaming_reoriented_inner_chain_base<F>(
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
		let ExecutionPlan::Filter {
			input: filter_input,
			predicate,
			disposition,
		} = input.as_ref()
		else {
			return Ok(None);
		};

		let matched = self
			.try_visit_streaming_join0_filter_chunks(
				filter_input,
				predicate,
				*disposition,
				morsel,
				|batch| {
					for (_partition, partitioned) in
						self.repartition_exchange_batch(&batch, *partition_count, key)?
					{
						if partitioned.num_rows() > 0 {
							visit_batch(partitioned)?;
						}
					}
					Ok(())
				},
			)
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		Ok(Some(()))
	}

	pub(super) async fn probe_streaming_semi_anti(
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

	pub(super) fn probe_streaming_left_outer_recombine(
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

	pub(super) fn probe_reoriented_inner_join(
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
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok(output)
	}
}

pub(super) fn matching_preserved_rows_in_memory(
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
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});

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
	record_task_telemetry(&telemetry, |task| {
		task.phases.build_micros = task
			.phases
			.build_micros
			.saturating_add(duration_micros(build_started));
	});

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
	for column in probe_batch.columns() {
		columns.push(arrow::compute::take(column, &probe_indices, None)?);
	}
	for column in build_batch.columns() {
		columns.push(arrow::compute::take(column, &build_indices, None)?);
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

#[derive(Debug, Clone)]
pub(super) struct BuildRowList {
	first: u32,
	overflow: Option<Vec<u32>>,
}

impl BuildRowList {
	pub(super) fn new(first: u32) -> Self {
		Self {
			first,
			overflow: None,
		}
	}

	pub(super) fn push(&mut self, row: u32) {
		self.overflow.get_or_insert_with(Vec::new).push(row);
	}

	pub(super) fn rows(&self) -> impl Iterator<Item = u32> + '_ {
		std::iter::once(self.first).chain(self.overflow.iter().flatten().copied())
	}

	#[cfg(test)]
	pub(super) fn overflow_len(&self) -> usize {
		self.overflow.as_ref().map(Vec::len).unwrap_or(0)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinKeyPlan {
	Int32,
	Int64,
	Generic,
}

impl JoinKeyPlan {
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

pub(super) fn build_in_memory_join_rows(
	join_key_plan: JoinKeyPlan,
	left_key_arrays: &[ArrayRef],
	rows: usize,
	memory_cap: u64,
	null_safe_keys: bool,
) -> Result<Option<HashMap<JoinKey, BuildRowList>>, ExecutionError> {
	let mut build_rows = HashMap::with_capacity(rows);
	let mut inserted_rows = 0usize;

	match join_key_plan {
		JoinKeyPlan::Int32 => {
			let values = single_join_key_array::<Int32Array>(left_key_arrays, "Int32")?;
			for row in 0..rows {
				if values.is_null(row) {
					continue;
				}
				if !join_build_memory_fits(
					left_key_arrays,
					inserted_rows.saturating_add(1),
					memory_cap,
				) {
					return Ok(None);
				}
				insert_join_build_row(
					&mut build_rows,
					JoinKey::Int32(values.value(row)),
					join_build_row_index(row)?,
				);
				inserted_rows = inserted_rows.saturating_add(1);
			}
		}
		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(left_key_arrays, "Int64")?;
			for row in 0..rows {
				if values.is_null(row) {
					continue;
				}
				if !join_build_memory_fits(
					left_key_arrays,
					inserted_rows.saturating_add(1),
					memory_cap,
				) {
					return Ok(None);
				}
				insert_join_build_row(
					&mut build_rows,
					JoinKey::Int64(values.value(row)),
					join_build_row_index(row)?,
				);
				inserted_rows = inserted_rows.saturating_add(1);
			}
		}
		JoinKeyPlan::Generic => {
			for row in 0..rows {
				let key = if null_safe_keys {
					join_key_bytes_null_safe(left_key_arrays, row)?
				} else {
					let Some(key) = join_key_bytes(left_key_arrays, row)? else {
						continue;
					};
					key
				};
				if !join_build_memory_fits(
					left_key_arrays,
					inserted_rows.saturating_add(1),
					memory_cap,
				) {
					return Ok(None);
				}
				insert_join_build_row(
					&mut build_rows,
					JoinKey::Generic(key),
					join_build_row_index(row)?,
				);
				inserted_rows = inserted_rows.saturating_add(1);
			}
		}
	}

	Ok(Some(build_rows))
}

pub(super) fn probe_in_memory_join_rows(
	kind: JoinKind,
	join_key_plan: JoinKeyPlan,
	build_rows: &HashMap<JoinKey, BuildRowList>,
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
			for row in 0..rows {
				if values.is_null(row) {
					append_unmatched_probe_row(kind, row, &mut selection)?;
				} else {
					let key = JoinKey::Int32(values.value(row));
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
		JoinKeyPlan::Int64 => {
			let values = single_join_key_array::<Int64Array>(right_key_arrays, "Int64")?;
			for row in 0..rows {
				if values.is_null(row) {
					append_unmatched_probe_row(kind, row, &mut selection)?;
				} else {
					let key = JoinKey::Int64(values.value(row));
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

pub(super) fn insert_join_build_row(
	build_rows: &mut HashMap<JoinKey, BuildRowList>,
	key: JoinKey,
	row: u32,
) {
	match build_rows.entry(key) {
		Entry::Occupied(mut entry) => entry.get_mut().push(row),
		Entry::Vacant(entry) => {
			entry.insert(BuildRowList::new(row));
		}
	}
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
		for batch in left_batches {
			let partitioned = select_join_partition_batch(
				batch,
				left_keys,
				partition_count,
				partition,
				null_safe_keys,
				true,
			)?;
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
			right_keys,
			partition_count,
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
	let partition = usize::try_from(partition).expect("partition fits usize");
	let mut indices = Vec::new();
	for row in 0..batch.num_rows() {
		let row_partition = if null_safe_keys {
			Some(partition_bucket(&key_arrays, row, partition_count)?)
		} else if join_key_bytes(&key_arrays, row)?.is_some() {
			Some(partition_bucket(&key_arrays, row, partition_count)?)
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn visit_partitioned_spilled_left_outer_join_chunks<F>(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	partition_count: u32,
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
	for batch in right_batches {
		let partitioned = select_join_partition_batch(
			batch,
			right_keys,
			partition_count,
			partition,
			null_safe_keys,
			false,
		)?;
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
	let (probe_rows, output_rows) = visit_streamed_spilled_inner_join_chunks(
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
pub(super) async fn visit_streamed_spilled_inner_join_chunks<F>(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	output_schema: &arrow::datatypes::SchemaRef,
	memory_cap: u64,
	mut visit_batch: F,
) -> Result<(u64, u64), ExecutionError>
where
	F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
{
	let mut probe_rows = 0u64;
	let mut output_rows = 0u64;
	for batch in right_batches {
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let probe_chunk = batch.slice(offset, len);
			probe_rows = probe_rows.saturating_add(probe_chunk.num_rows() as u64);
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			let mut matches = Vec::new();
			for row in 0..probe_chunk.num_rows() {
				let Some(key) = join_key_at(key_plan, &right_key_arrays, row, null_safe_keys)?
				else {
					continue;
				};
				for build in spilled.lookup(&key).await? {
					matches.push(SpilledInnerJoinMatch {
						build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let batch = materialize_streamed_spilled_inner_matches(
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
	Ok((probe_rows, output_rows))
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
	visit_streamed_spilled_inner_join_chunks(
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
	visit_streamed_spilled_inner_join_chunks(
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

pub(super) async fn materialize_streamed_spilled_inner_matches(
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
		SpilledJoinBuild::write(build_rows, task_budget, spill_file_ops, spill_dir).await?;
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
		SpilledJoinBuild::write(build_rows, task_budget, spill_file_ops, spill_dir).await?;
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
	filter_predicate: &BoundExprProgram,
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
		SpilledJoinBuild::write(build_rows, task_budget, spill_file_ops, spill_dir).await?;
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
	filter_predicate: &BoundExprProgram,
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
	filter_predicate: &BoundExprProgram,
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
		let batch = executor.filter_batch_with_bound_predicate(
			filter_predicate,
			batch,
			"filter predicate",
		)?;
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
		.map(|array| match array.data_type() {
			DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
			DataType::Int16 | DataType::UInt16 => 2,
			DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
			DataType::Date32 => 4,
			DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Timestamp(_, _) => 8,
			DataType::Utf8 => 32,
			_ => 16,
		})
		.sum::<usize>();
	u64::try_from(rows.saturating_mul(112 + key_width)).unwrap_or(u64::MAX)
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

#[derive(Debug, Clone, Copy)]
pub(super) struct JoinSpillPage {
	offset: u64,
	ciphertext_len: usize,
	plaintext_len: usize,
}

#[derive(Debug, Clone)]
pub(super) struct CachedJoinSpillPage {
	bytes: Vec<u8>,
}

pub(super) struct SpilledJoinBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	pages: Vec<JoinSpillPage>,
	index: HashMap<Vec<u8>, Vec<usize>>,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
}

impl SpilledJoinBuild {
	pub(super) async fn write(
		build_rows: HashMap<Vec<u8>, Vec<u32>>,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!("spill_{}_{}.join", std::process::id(), spill_id));
		match Self::write_inner(
			spill_id,
			path.clone(),
			build_rows,
			task_budget,
			file_ops.clone(),
		)
		.await
		{
			Ok(spill) => Ok(spill),
			Err(error) => {
				let _ = file_ops.unlink(&path).await;
				Err(error)
			}
		}
	}

	pub(super) async fn write_inner(
		spill_id: u64,
		path: PathBuf,
		build_rows: HashMap<Vec<u8>, Vec<u32>>,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> Result<Self, ExecutionError> {
		let spill_dir = path.parent().ok_or_else(|| ExecutionError::Kernel {
			message: "join spill path has no parent directory".to_string(),
		})?;
		file_ops
			.create_dir_all(spill_dir)
			.await
			.map_err(|error| join_spill_io_error("create_dir_all", error))?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| ExecutionError::Kernel {
			message: format!("join spill key generation failed: {error:?}"),
		})?;
		let unbound = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("join spill key construction failed: {error:?}"),
			}
		})?;
		let key = LessSafeKey::new(unbound);
		let file = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create_new: true,
					..OpenFlags::default()
				},
			)
			.await
			.map_err(|error| ExecutionError::Kernel {
				message: format!("join spill open failed: {error}"),
			})?;
		let mut pages = Vec::new();
		let mut index: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
		let mut current_entries: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
		let mut current_size = std::mem::size_of::<u32>();
		let mut spill_bytes = 0u64;
		let mut writer_peak = 0u64;
		let mut next_offset = 0u64;

		for (join_key, rows) in build_rows {
			let entry_size = encoded_join_spill_entry_size(&join_key, &rows);
			if !current_entries.is_empty()
				&& current_size.saturating_add(entry_size) > JOIN_SPILL_PAGE_SIZE
			{
				let page_index = pages.len();
				let page = write_join_spill_page(
					file_ops.as_ref(),
					&file,
					&key,
					spill_id,
					page_index,
					next_offset,
					&current_entries,
				)
				.await?;
				spill_bytes = spill_bytes.saturating_add(page.ciphertext_len as u64);
				writer_peak = writer_peak.max(page.plaintext_len as u64);
				if spill_bytes > task_budget.spill_bytes.get() {
					return Err(budget_exhausted(
						BudgetDimension::SpillBytes,
						"join spill writer",
						spill_bytes,
						task_budget.spill_bytes.get(),
					));
				}
				for (key, _) in &current_entries {
					index.entry(key.clone()).or_default().push(page_index);
				}
				next_offset = next_offset.saturating_add(page.ciphertext_len as u64);
				pages.push(page);
				current_entries.clear();
				current_size = std::mem::size_of::<u32>();
			}

			current_size = current_size.saturating_add(entry_size);
			current_entries.push((join_key, rows));
		}

		if !current_entries.is_empty() {
			let page_index = pages.len();
			let page = write_join_spill_page(
				file_ops.as_ref(),
				&file,
				&key,
				spill_id,
				page_index,
				next_offset,
				&current_entries,
			)
			.await?;
			spill_bytes = spill_bytes.saturating_add(page.ciphertext_len as u64);
			writer_peak = writer_peak.max(page.plaintext_len as u64);
			if spill_bytes > task_budget.spill_bytes.get() {
				return Err(budget_exhausted(
					BudgetDimension::SpillBytes,
					"join spill writer",
					spill_bytes,
					task_budget.spill_bytes.get(),
				));
			}
			for (key, _) in &current_entries {
				index.entry(key.clone()).or_default().push(page_index);
			}
			next_offset = next_offset.saturating_add(page.ciphertext_len as u64);
			pages.push(page);
		}
		file_ops
			.set_len(&file, next_offset)
			.await
			.map_err(|error| join_spill_io_error("set_len", error))?;
		file_ops
			.fdatasync(&file)
			.await
			.map_err(|error| join_spill_io_error("fdatasync", error))?;

		let index_bytes = join_spill_index_bytes(&index);
		let resident_without_cache = index_bytes
			.saturating_add(writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		if resident_without_cache > task_budget.memory.get() {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"join spill writer resident state",
				resident_without_cache,
				task_budget.memory.get(),
			));
		}
		let cache_cap = JOIN_SPILL_CACHE_TARGET_BYTES.min(
			task_budget
				.memory
				.get()
				.saturating_sub(resident_without_cache),
		);

		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			pages,
			index,
			cache: HashMap::new(),
			lru: VecDeque::new(),
			cache_bytes: 0,
			cache_cap,
		})
	}

	pub(super) fn spill_bytes(&self) -> u64 {
		self.pages
			.iter()
			.map(|page| page.ciphertext_len as u64)
			.sum()
	}

	pub(super) async fn lookup(&mut self, join_key: &[u8]) -> Result<Vec<u32>, ExecutionError> {
		let Some(page_indices) = self.index.get(join_key).cloned() else {
			return Ok(Vec::new());
		};
		let mut rows = Vec::new();
		for page_index in page_indices {
			let page = self.read_page(page_index).await?;
			rows.extend(decode_join_spill_page_for_key(&page, join_key)?);
		}
		Ok(rows)
	}

	pub(super) async fn read_page(&mut self, page_index: usize) -> Result<Vec<u8>, ExecutionError> {
		if let Some(cached) = self.cache.get(&page_index) {
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			return Ok(bytes);
		}

		let page = self
			.pages
			.get(page_index)
			.copied()
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("join spill page {page_index} is missing"),
			})?;
		let mut ciphertext = read_plain_exact(
			self.file_ops.as_ref(),
			&self.file,
			page.offset,
			page.ciphertext_len,
		)
		.await?;
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("join spill decrypt failed: {error:?}"),
			})?
			.to_vec();
		self.insert_cache_entry(page_index, plaintext.clone());
		Ok(plaintext)
	}

	pub(super) async fn cleanup(self) {
		let file_ops = self.file_ops.clone();
		let path = self.path.clone();
		drop(self);
		let _ = file_ops.unlink(&path).await;
	}

	pub(super) fn touch_cache_entry(&mut self, page_index: usize) {
		self.lru.retain(|cached| *cached != page_index);
		self.lru.push_back(page_index);
	}

	pub(super) fn insert_cache_entry(&mut self, page_index: usize, bytes: Vec<u8>) {
		let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		if self.cache_cap == 0 || bytes_len > self.cache_cap {
			return;
		}
		while self.cache_bytes.saturating_add(bytes_len) > self.cache_cap {
			let Some(evicted) = self.lru.pop_front() else {
				break;
			};
			if let Some(page) = self.cache.remove(&evicted) {
				self.cache_bytes = self
					.cache_bytes
					.saturating_sub(u64::try_from(page.bytes.len()).unwrap_or(u64::MAX));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache.insert(page_index, CachedJoinSpillPage { bytes });
		self.touch_cache_entry(page_index);
	}
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SpilledBatchRowRef {
	page_index: usize,
	row: u32,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SpilledInnerJoinMatch {
	build: SpilledBatchRowRef,
	probe: u32,
}

pub(super) struct SpilledInnerJoinBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	pages: Vec<JoinSpillPage>,
	page_rows: Vec<u32>,
	index: HashMap<JoinKey, Vec<SpilledBatchRowRef>>,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
	spill_bytes: u64,
	writer_peak: u64,
	rows: u64,
	input_rows: u64,
	input_batches_written: u64,
	next_offset: u64,
}

impl SpilledInnerJoinBuild {
	pub(super) async fn create(
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!("spill_{}_{}.inner", std::process::id(), spill_id));
		match Self::create_inner(spill_id, path.clone(), task_budget, file_ops.clone()).await {
			Ok(spill) => Ok(spill),
			Err(error) => {
				let _ = file_ops.unlink(&path).await;
				Err(error)
			}
		}
	}

	pub(super) async fn create_inner(
		spill_id: u64,
		path: PathBuf,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> Result<Self, ExecutionError> {
		let spill_dir = path.parent().ok_or_else(|| ExecutionError::Kernel {
			message: "inner join batch spill path has no parent directory".to_string(),
		})?;
		file_ops
			.create_dir_all(spill_dir)
			.await
			.map_err(|error| join_spill_io_error("create_dir_all", error))?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| ExecutionError::Kernel {
			message: format!("inner join batch spill key generation failed: {error:?}"),
		})?;
		let unbound = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("inner join batch spill key construction failed: {error:?}"),
			}
		})?;
		let key = LessSafeKey::new(unbound);
		let file = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create_new: true,
					..OpenFlags::default()
				},
			)
			.await
			.map_err(|error| ExecutionError::Kernel {
				message: format!("inner join batch spill open failed: {error}"),
			})?;
		let cache_cap = JOIN_SPILL_CACHE_TARGET_BYTES.min(task_budget.memory.get());
		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			pages: Vec::new(),
			page_rows: Vec::new(),
			index: HashMap::new(),
			cache: HashMap::new(),
			lru: VecDeque::new(),
			cache_bytes: 0,
			cache_cap,
			spill_bytes: 0,
			writer_peak: 0,
			rows: 0,
			input_rows: 0,
			input_batches_written: 0,
			next_offset: 0,
		})
	}

	pub(super) async fn write_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.write_page_batch(
				&chunk,
				key_plan,
				build_keys,
				null_safe_keys,
				false,
				task_budget,
			)
			.await?;
		}
		Ok(())
	}

	pub(super) async fn write_preserved_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.write_page_batch(
				&chunk,
				key_plan,
				build_keys,
				null_safe_keys,
				true,
				task_budget,
			)
			.await?;
		}
		Ok(())
	}

	pub(super) async fn write_projected_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		retained_columns: &[ColumnOrdinal],
		key_plan: JoinKeyPlan,
		projected_build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			let projected = project_batch_columns(&chunk, retained_columns)?;
			self.write_page_batch(
				&projected,
				key_plan,
				projected_build_keys,
				null_safe_keys,
				false,
				task_budget,
			)
			.await?;
		}
		Ok(())
	}

	pub(super) async fn write_page_batch(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		retain_unkeyed_rows: bool,
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		let key_arrays = key_arrays(batch, build_keys)?;
		let mut refs = Vec::new();
		for row in 0..batch.num_rows() {
			let Some(key) = join_key_at(key_plan, &key_arrays, row, null_safe_keys)? else {
				continue;
			};
			refs.push((key, join_build_row_index(row)?));
		}
		if refs.is_empty() && !retain_unkeyed_rows {
			return Ok(());
		}

		let page_index = self.pages.len();
		let plaintext = encode_record_batch_ipc(batch)?;
		let page = write_join_spill_plaintext_page(
			self.file_ops.as_ref(),
			&self.file,
			&self.key,
			self.spill_id,
			page_index,
			self.next_offset,
			plaintext,
		)
		.await?;
		self.spill_bytes = self.spill_bytes.saturating_add(page.ciphertext_len as u64);
		self.writer_peak = self.writer_peak.max(page.plaintext_len as u64);
		if self.spill_bytes > task_budget.spill_bytes.get() {
			return Err(budget_exhausted(
				BudgetDimension::SpillBytes,
				"streamed inner join build spill writer",
				self.spill_bytes,
				task_budget.spill_bytes.get(),
			));
		}
		for (key, row) in refs {
			self.index
				.entry(key)
				.or_default()
				.push(SpilledBatchRowRef { page_index, row });
			self.rows = self.rows.saturating_add(1);
		}
		self.next_offset = self.next_offset.saturating_add(page.ciphertext_len as u64);
		self.page_rows.push(join_build_row_index(batch.num_rows())?);
		self.pages.push(page);
		self.guard_resident_state(task_budget.memory.get())?;
		Ok(())
	}

	pub(super) async fn finish(&self, task_budget: TaskBudget) -> Result<(), ExecutionError> {
		self.file_ops
			.set_len(&self.file, self.next_offset)
			.await
			.map_err(|error| join_spill_io_error("set_len", error))?;
		self.file_ops
			.fdatasync(&self.file)
			.await
			.map_err(|error| join_spill_io_error("fdatasync", error))?;
		self.guard_resident_state(task_budget.memory.get())
	}

	pub(super) fn guard_resident_state(&self, memory_cap: u64) -> Result<(), ExecutionError> {
		let resident = spilled_batch_join_index_bytes(&self.index)
			.saturating_add(self.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		if resident > cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"streamed inner join build spill resident state",
				resident,
				cap,
			));
		}
		Ok(())
	}

	pub(super) async fn lookup(
		&mut self,
		join_key: &JoinKey,
	) -> Result<Vec<SpilledBatchRowRef>, ExecutionError> {
		Ok(self.index.get(join_key).cloned().unwrap_or_default())
	}

	pub(super) fn page_count(&self) -> usize {
		self.pages.len()
	}

	pub(super) fn page_row_count(&self, page_index: usize) -> Result<usize, ExecutionError> {
		self.page_rows
			.get(page_index)
			.copied()
			.map(|rows| rows as usize)
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("inner join batch spill page {page_index} is missing row count"),
			})
	}

	pub(super) fn empty_page_match_bitmap(&self) -> Vec<Vec<u64>> {
		self.page_rows
			.iter()
			.map(|rows| vec![0; (*rows as usize).div_ceil(64)])
			.collect()
	}

	pub(super) fn mark_page_row_matched(&self, matched: &mut [Vec<u64>], row: SpilledBatchRowRef) {
		let Some(bitmap) = matched.get_mut(row.page_index) else {
			return;
		};
		set_build_row_matched(bitmap, row.row);
	}

	pub(super) fn page_row_matched(
		&self,
		matched: &[Vec<u64>],
		page_index: usize,
		row: usize,
	) -> bool {
		matched
			.get(page_index)
			.is_some_and(|bitmap| build_row_matched(bitmap, row))
	}

	pub(super) async fn read_batch_page(
		&mut self,
		page_index: usize,
	) -> Result<RecordBatch, ExecutionError> {
		let bytes = self.read_page(page_index).await?;
		decode_record_batch_ipc(&bytes)
	}

	pub(super) async fn read_page(&mut self, page_index: usize) -> Result<Vec<u8>, ExecutionError> {
		if let Some(cached) = self.cache.get(&page_index) {
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			return Ok(bytes);
		}

		let page = self
			.pages
			.get(page_index)
			.copied()
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("inner join batch spill page {page_index} is missing"),
			})?;
		let mut ciphertext = read_plain_exact(
			self.file_ops.as_ref(),
			&self.file,
			page.offset,
			page.ciphertext_len,
		)
		.await?;
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("inner join batch spill decrypt failed: {error:?}"),
			})?
			.to_vec();
		self.insert_cache_entry(page_index, plaintext.clone());
		Ok(plaintext)
	}

	pub(super) async fn cleanup(self) {
		let file_ops = self.file_ops.clone();
		let path = self.path.clone();
		drop(self);
		let _ = file_ops.unlink(&path).await;
	}

	pub(super) fn touch_cache_entry(&mut self, page_index: usize) {
		self.lru.retain(|cached| *cached != page_index);
		self.lru.push_back(page_index);
	}

	pub(super) fn insert_cache_entry(&mut self, page_index: usize, bytes: Vec<u8>) {
		let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		if self.cache_cap == 0 || bytes_len > self.cache_cap {
			return;
		}
		while self.cache_bytes.saturating_add(bytes_len) > self.cache_cap {
			let Some(evicted) = self.lru.pop_front() else {
				break;
			};
			if let Some(page) = self.cache.remove(&evicted) {
				self.cache_bytes = self
					.cache_bytes
					.saturating_sub(u64::try_from(page.bytes.len()).unwrap_or(u64::MAX));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache.insert(page_index, CachedJoinSpillPage { bytes });
		self.touch_cache_entry(page_index);
	}

	#[cfg(test)]
	pub(super) fn input_batches_written(&self) -> u64 {
		self.input_batches_written
	}

	pub(super) fn input_row_count(&self) -> u64 {
		self.input_rows
	}

	pub(super) fn spill_bytes(&self) -> u64 {
		self.spill_bytes
	}
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SpilledExistenceRowRef {
	page_index: usize,
	row: u32,
}

pub(super) struct SpilledSemiAntiExistenceBuild {
	spill_id: u64,
	path: PathBuf,
	file_ops: Arc<dyn AsyncFileOps>,
	file: FileHandle,
	key: LessSafeKey,
	pages: Vec<JoinSpillPage>,
	index: HashMap<JoinKey, Vec<SpilledExistenceRowRef>>,
	cache: HashMap<usize, CachedJoinSpillPage>,
	lru: VecDeque<usize>,
	cache_bytes: u64,
	cache_cap: u64,
	spill_bytes: u64,
	writer_peak: u64,
	rows: u64,
	input_rows: u64,
	input_batches_written: u64,
	next_offset: u64,
}

impl SpilledSemiAntiExistenceBuild {
	pub(super) async fn create(
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
		spill_dir: PathBuf,
	) -> Result<Self, ExecutionError> {
		let spill_id = NEXT_JOIN_SPILL_ID.fetch_add(1, Ordering::SeqCst);
		let path = spill_dir.join(format!(
			"spill_{}_{}.existence",
			std::process::id(),
			spill_id
		));
		match Self::create_inner(spill_id, path.clone(), task_budget, file_ops.clone()).await {
			Ok(spill) => Ok(spill),
			Err(error) => {
				let _ = file_ops.unlink(&path).await;
				Err(error)
			}
		}
	}

	pub(super) async fn create_inner(
		spill_id: u64,
		path: PathBuf,
		task_budget: TaskBudget,
		file_ops: Arc<dyn AsyncFileOps>,
	) -> Result<Self, ExecutionError> {
		let spill_dir = path.parent().ok_or_else(|| ExecutionError::Kernel {
			message: "semi/anti existence spill path has no parent directory".to_string(),
		})?;
		file_ops
			.create_dir_all(spill_dir)
			.await
			.map_err(|error| join_spill_io_error("create_dir_all", error))?;
		let mut key_bytes = [0u8; 32];
		aws_lc_rs::rand::fill(&mut key_bytes).map_err(|error| ExecutionError::Kernel {
			message: format!("semi/anti existence spill key generation failed: {error:?}"),
		})?;
		let unbound = UnboundKey::new(&AES_256_GCM_SIV, &key_bytes).map_err(|error| {
			ExecutionError::Kernel {
				message: format!("semi/anti existence spill key construction failed: {error:?}"),
			}
		})?;
		let key = LessSafeKey::new(unbound);
		let file = file_ops
			.open(
				&path,
				OpenFlags {
					read: true,
					write: true,
					create_new: true,
					..OpenFlags::default()
				},
			)
			.await
			.map_err(|error| ExecutionError::Kernel {
				message: format!("semi/anti existence spill open failed: {error}"),
			})?;
		let cache_cap = JOIN_SPILL_CACHE_TARGET_BYTES.min(task_budget.memory.get());
		Ok(Self {
			spill_id,
			path,
			file_ops,
			file,
			key,
			pages: Vec::new(),
			index: HashMap::new(),
			cache: HashMap::new(),
			lru: VecDeque::new(),
			cache_bytes: 0,
			cache_cap,
			spill_bytes: 0,
			writer_peak: 0,
			rows: 0,
			input_rows: 0,
			input_batches_written: 0,
			next_offset: 0,
		})
	}

	pub(super) async fn write_batch_chunks(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		self.input_batches_written = self.input_batches_written.saturating_add(1);
		self.input_rows = self.input_rows.saturating_add(batch.num_rows() as u64);
		for offset in (0..batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(batch.num_rows() - offset);
			let chunk = batch.slice(offset, len);
			self.write_page_batch(&chunk, key_plan, existence_keys, task_budget)
				.await?;
		}
		Ok(())
	}

	pub(super) async fn write_page_batch(
		&mut self,
		batch: &RecordBatch,
		key_plan: JoinKeyPlan,
		existence_keys: &[crate::ir::ColumnOrdinal],
		task_budget: TaskBudget,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			return Ok(());
		}
		let key_arrays = key_arrays(batch, existence_keys)?;
		let mut refs = Vec::new();
		for row in 0..batch.num_rows() {
			let Some(key) = join_key_at(key_plan, &key_arrays, row, false)? else {
				continue;
			};
			refs.push((key, join_build_row_index(row)?));
		}
		if refs.is_empty() {
			return Ok(());
		}

		let page_index = self.pages.len();
		let plaintext = encode_record_batch_ipc(batch)?;
		let page = write_join_spill_plaintext_page(
			self.file_ops.as_ref(),
			&self.file,
			&self.key,
			self.spill_id,
			page_index,
			self.next_offset,
			plaintext,
		)
		.await?;
		self.spill_bytes = self.spill_bytes.saturating_add(page.ciphertext_len as u64);
		self.writer_peak = self.writer_peak.max(page.plaintext_len as u64);
		if self.spill_bytes > task_budget.spill_bytes.get() {
			return Err(budget_exhausted(
				BudgetDimension::SpillBytes,
				"semi/anti existence-set spill writer",
				self.spill_bytes,
				task_budget.spill_bytes.get(),
			));
		}
		for (key, row) in refs {
			self.index
				.entry(key)
				.or_default()
				.push(SpilledExistenceRowRef { page_index, row });
			self.rows = self.rows.saturating_add(1);
		}
		self.next_offset = self.next_offset.saturating_add(page.ciphertext_len as u64);
		self.pages.push(page);
		self.guard_resident_state(task_budget.memory.get())?;
		Ok(())
	}

	pub(super) async fn finish(&self, task_budget: TaskBudget) -> Result<(), ExecutionError> {
		self.file_ops
			.set_len(&self.file, self.next_offset)
			.await
			.map_err(|error| join_spill_io_error("set_len", error))?;
		self.file_ops
			.fdatasync(&self.file)
			.await
			.map_err(|error| join_spill_io_error("fdatasync", error))?;
		self.guard_resident_state(task_budget.memory.get())
	}

	pub(super) fn guard_resident_state(&self, memory_cap: u64) -> Result<(), ExecutionError> {
		let resident = semi_anti_existence_spill_index_bytes(&self.index)
			.saturating_add(self.writer_peak)
			.saturating_add(JOIN_SPILL_PAGE_SIZE as u64);
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		if resident > cap {
			return Err(budget_exhausted(
				BudgetDimension::Memory,
				"semi/anti existence-set spill resident state",
				resident,
				cap,
			));
		}
		Ok(())
	}

	#[cfg(test)]
	pub(super) fn row_count(&self) -> u64 {
		self.rows
	}

	pub(super) fn input_row_count(&self) -> u64 {
		self.input_rows
	}

	pub(super) fn spill_bytes(&self) -> u64 {
		self.spill_bytes
	}

	#[cfg(test)]
	pub(super) fn input_batches_written(&self) -> u64 {
		self.input_batches_written
	}

	pub(super) async fn matching_preserved_rows(
		&mut self,
		preserved_batch: &RecordBatch,
		preserved_by_key: &HashMap<JoinKey, Vec<u32>>,
		match_filter: &BoundExprProgram,
	) -> Result<Vec<bool>, ExecutionError> {
		let mut page_work: BTreeMap<usize, Vec<(Vec<u32>, Vec<u32>)>> = BTreeMap::new();
		for (key, preserved_rows) in preserved_by_key {
			let Some(row_refs) = self.index.get(key) else {
				continue;
			};
			let mut rows_by_page: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
			for row_ref in row_refs {
				rows_by_page
					.entry(row_ref.page_index)
					.or_default()
					.push(row_ref.row);
			}
			for (page_index, existence_rows) in rows_by_page {
				page_work
					.entry(page_index)
					.or_default()
					.push((preserved_rows.clone(), existence_rows));
			}
		}

		let mut matched = vec![false; preserved_batch.num_rows()];
		for (page_index, groups) in page_work {
			let existence_batch = self.read_batch_page(page_index).await?;
			let mut residual =
				JoinMatchResidual::new(Some(match_filter), preserved_batch, &existence_batch);
			for (preserved_rows, existence_rows) in groups {
				for preserved_row in preserved_rows {
					if matched[preserved_row as usize] {
						continue;
					}
					for existence_row in &existence_rows {
						if residual.matches(preserved_row, *existence_row)? {
							matched[preserved_row as usize] = true;
							break;
						}
					}
				}
			}
		}
		Ok(matched)
	}

	pub(super) async fn read_batch_page(
		&mut self,
		page_index: usize,
	) -> Result<RecordBatch, ExecutionError> {
		let bytes = self.read_page(page_index).await?;
		decode_record_batch_ipc(&bytes)
	}

	pub(super) async fn read_page(&mut self, page_index: usize) -> Result<Vec<u8>, ExecutionError> {
		if let Some(cached) = self.cache.get(&page_index) {
			let bytes = cached.bytes.clone();
			self.touch_cache_entry(page_index);
			return Ok(bytes);
		}

		let page = self
			.pages
			.get(page_index)
			.copied()
			.ok_or_else(|| ExecutionError::Kernel {
				message: format!("semi/anti existence spill page {page_index} is missing"),
			})?;
		let mut ciphertext = read_plain_exact(
			self.file_ops.as_ref(),
			&self.file,
			page.offset,
			page.ciphertext_len,
		)
		.await?;
		let nonce = Nonce::assume_unique_for_key(join_spill_nonce(self.spill_id, page_index));
		let plaintext = self
			.key
			.open_in_place(nonce, Aad::empty(), &mut ciphertext)
			.map_err(|error| ExecutionError::Kernel {
				message: format!("semi/anti existence spill decrypt failed: {error:?}"),
			})?
			.to_vec();
		self.insert_cache_entry(page_index, plaintext.clone());
		Ok(plaintext)
	}

	pub(super) async fn cleanup(self) {
		let file_ops = self.file_ops.clone();
		let path = self.path.clone();
		drop(self);
		let _ = file_ops.unlink(&path).await;
	}

	pub(super) fn touch_cache_entry(&mut self, page_index: usize) {
		self.lru.retain(|cached| *cached != page_index);
		self.lru.push_back(page_index);
	}

	pub(super) fn insert_cache_entry(&mut self, page_index: usize, bytes: Vec<u8>) {
		let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		if self.cache_cap == 0 || bytes_len > self.cache_cap {
			return;
		}
		while self.cache_bytes.saturating_add(bytes_len) > self.cache_cap {
			let Some(evicted) = self.lru.pop_front() else {
				break;
			};
			if let Some(page) = self.cache.remove(&evicted) {
				self.cache_bytes = self
					.cache_bytes
					.saturating_sub(u64::try_from(page.bytes.len()).unwrap_or(u64::MAX));
			}
		}
		self.cache_bytes = self.cache_bytes.saturating_add(bytes_len);
		self.cache.insert(page_index, CachedJoinSpillPage { bytes });
		self.touch_cache_entry(page_index);
	}
}

pub(super) async fn write_join_spill_page(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	key: &LessSafeKey,
	spill_id: u64,
	page_index: usize,
	offset: u64,
	entries: &[(Vec<u8>, Vec<u32>)],
) -> Result<JoinSpillPage, ExecutionError> {
	let plaintext = encode_join_spill_page(entries)?;
	write_join_spill_plaintext_page(file_ops, file, key, spill_id, page_index, offset, plaintext)
		.await
}

pub(super) async fn write_join_spill_plaintext_page(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	key: &LessSafeKey,
	spill_id: u64,
	page_index: usize,
	offset: u64,
	mut plaintext: Vec<u8>,
) -> Result<JoinSpillPage, ExecutionError> {
	let plaintext_len = plaintext.len();
	let nonce = Nonce::assume_unique_for_key(join_spill_nonce(spill_id, page_index));
	key.seal_in_place_append_tag(nonce, Aad::empty(), &mut plaintext)
		.map_err(|error| ExecutionError::Kernel {
			message: format!("join spill encrypt failed: {error:?}"),
		})?;
	let ciphertext_len = plaintext.len();
	write_plain_all(file_ops, file, offset, plaintext).await?;
	Ok(JoinSpillPage {
		offset,
		ciphertext_len,
		plaintext_len,
	})
}

pub(super) async fn write_plain_all(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	offset: u64,
	bytes: Vec<u8>,
) -> Result<(), ExecutionError> {
	let mut written_total = 0usize;
	while written_total < bytes.len() {
		let written = file_ops
			.write_plain_at(
				file,
				offset.saturating_add(written_total as u64),
				bytes[written_total..].to_vec(),
			)
			.await
			.map_err(|error| join_spill_io_error("write", error))?;
		if written == 0 {
			return Err(join_spill_io_error(
				"write",
				std::io::Error::new(
					ErrorKind::WriteZero,
					"join spill write completed with zero bytes",
				),
			));
		}
		written_total = written_total.saturating_add(written);
	}
	Ok(())
}

pub(super) async fn read_plain_exact(
	file_ops: &dyn AsyncFileOps,
	file: &FileHandle,
	offset: u64,
	len: usize,
) -> Result<Vec<u8>, ExecutionError> {
	let mut out = Vec::with_capacity(len);
	while out.len() < len {
		let chunk = file_ops
			.read_plain_at(
				file,
				offset.saturating_add(out.len() as u64),
				len.saturating_sub(out.len()),
			)
			.await
			.map_err(|error| join_spill_io_error("read", error))?;
		if chunk.is_empty() {
			return Err(join_spill_io_error(
				"read",
				std::io::Error::new(ErrorKind::UnexpectedEof, "join spill page truncated"),
			));
		}
		out.extend_from_slice(&chunk);
	}
	Ok(out)
}

pub(super) fn join_spill_io_error(
	operation: &'static str,
	error: std::io::Error,
) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("join spill {operation} failed: {error}"),
	}
}

pub(super) fn encode_join_spill_page(
	entries: &[(Vec<u8>, Vec<u32>)],
) -> Result<Vec<u8>, ExecutionError> {
	let mut out = Vec::new();
	out.extend_from_slice(
		&u32::try_from(entries.len())
			.map_err(|_| ExecutionError::Kernel {
				message: "join spill page has too many entries".to_string(),
			})?
			.to_le_bytes(),
	);
	for (join_key, rows) in entries {
		out.extend_from_slice(
			&u32::try_from(join_key.len())
				.map_err(|_| ExecutionError::Kernel {
					message: "join spill key is too large".to_string(),
				})?
				.to_le_bytes(),
		);
		out.extend_from_slice(join_key);
		out.extend_from_slice(
			&u32::try_from(rows.len())
				.map_err(|_| ExecutionError::Kernel {
					message: "join spill row list is too large".to_string(),
				})?
				.to_le_bytes(),
		);
		for row in rows {
			out.extend_from_slice(&row.to_le_bytes());
		}
	}
	Ok(out)
}

pub(super) fn decode_join_spill_page_for_key(
	page: &[u8],
	lookup_key: &[u8],
) -> Result<Vec<u32>, ExecutionError> {
	let mut cursor = 0usize;
	let entries = read_u32_le(page, &mut cursor)? as usize;
	let mut rows = Vec::new();
	for _ in 0..entries {
		let key_len = read_u32_le(page, &mut cursor)? as usize;
		let key_end = cursor
			.checked_add(key_len)
			.ok_or_else(join_spill_decode_error)?;
		let key = page
			.get(cursor..key_end)
			.ok_or_else(join_spill_decode_error)?;
		cursor = key_end;
		let row_count = read_u32_le(page, &mut cursor)? as usize;
		for _ in 0..row_count {
			let row = read_u32_le(page, &mut cursor)?;
			if key == lookup_key {
				rows.push(row);
			}
		}
	}
	Ok(rows)
}

pub(super) fn encode_record_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ExecutionError> {
	let mut buffer = Vec::new();
	{
		let options =
			IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?;
		let mut writer = StreamWriter::try_new_with_options(&mut buffer, &batch.schema(), options)?;
		writer.write(batch)?;
		writer.finish()?;
	}
	Ok(buffer)
}

pub(super) fn decode_record_batch_ipc(bytes: &[u8]) -> Result<RecordBatch, ExecutionError> {
	let cursor = Cursor::new(bytes);
	let mut reader = StreamReader::try_new(cursor, None)?;
	let Some(batch) = reader.next() else {
		return Err(ExecutionError::Kernel {
			message: "semi/anti existence spill page contained no batch".to_string(),
		});
	};
	let batch = batch?;
	if reader.next().is_some() {
		return Err(ExecutionError::Kernel {
			message: "semi/anti existence spill page contained multiple batches".to_string(),
		});
	}
	Ok(batch)
}

pub(super) fn read_u32_le(page: &[u8], cursor: &mut usize) -> Result<u32, ExecutionError> {
	let end = cursor.checked_add(4).ok_or_else(join_spill_decode_error)?;
	let bytes = page.get(*cursor..end).ok_or_else(join_spill_decode_error)?;
	*cursor = end;
	Ok(u32::from_le_bytes(bytes.try_into().expect("four bytes")))
}

pub(super) fn join_spill_decode_error() -> ExecutionError {
	ExecutionError::Kernel {
		message: "join spill page decode failed".to_string(),
	}
}

pub(super) fn encoded_join_spill_entry_size(join_key: &[u8], rows: &[u32]) -> usize {
	std::mem::size_of::<u32>()
		.saturating_add(join_key.len())
		.saturating_add(std::mem::size_of::<u32>())
		.saturating_add(rows.len().saturating_mul(std::mem::size_of::<u32>()))
}

pub(super) fn join_spill_index_bytes(index: &HashMap<Vec<u8>, Vec<usize>>) -> u64 {
	index
		.iter()
		.map(|(key, pages)| {
			u64::try_from(
				64usize
					.saturating_add(key.len())
					.saturating_add(pages.len().saturating_mul(std::mem::size_of::<usize>())),
			)
			.unwrap_or(u64::MAX)
		})
		.fold(0u64, u64::saturating_add)
}

pub(super) fn semi_anti_existence_spill_index_bytes(
	index: &HashMap<JoinKey, Vec<SpilledExistenceRowRef>>,
) -> u64 {
	index
		.iter()
		.map(|(key, refs)| {
			u64::try_from(
				64usize
					.saturating_add(join_key_heap_bytes(key))
					.saturating_add(
						refs.len()
							.saturating_mul(std::mem::size_of::<SpilledExistenceRowRef>()),
					),
			)
			.unwrap_or(u64::MAX)
		})
		.fold(0u64, u64::saturating_add)
}

pub(super) fn spilled_batch_join_index_bytes(
	index: &HashMap<JoinKey, Vec<SpilledBatchRowRef>>,
) -> u64 {
	index
		.iter()
		.map(|(key, refs)| {
			u64::try_from(
				64usize
					.saturating_add(join_key_heap_bytes(key))
					.saturating_add(
						refs.len()
							.saturating_mul(std::mem::size_of::<SpilledBatchRowRef>()),
					),
			)
			.unwrap_or(u64::MAX)
		})
		.fold(0u64, u64::saturating_add)
}

pub(super) fn join_key_heap_bytes(key: &JoinKey) -> usize {
	match key {
		JoinKey::Int32(_) => std::mem::size_of::<i32>(),
		JoinKey::Int64(_) => std::mem::size_of::<i64>(),
		JoinKey::Generic(bytes) => bytes.len(),
	}
}

pub(super) fn join_spill_nonce(spill_id: u64, page_index: usize) -> [u8; 12] {
	let mut nonce = [0u8; 12];
	nonce[..8].copy_from_slice(&spill_id.to_le_bytes());
	nonce[8..].copy_from_slice(&u32::try_from(page_index).unwrap_or(u32::MAX).to_le_bytes());
	nonce
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
	}
}
