use super::*;

pub(in crate::exec) struct ReorientedInnerJoinStage<'a> {
	replay_id: JoinReplayId,
	probe_schema: SchemaRef,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	build: &'a ExecutionPlan,
	build_keys: Vec<crate::ir::ColumnOrdinal>,
	retained_build_columns: Option<Vec<crate::ir::ColumnOrdinal>>,
	output_schema: SchemaRef,
	strategy: JoinStrategy,
	post_filter: Option<&'a BoundExprProgram>,
	probe_columns_first: bool,
}

pub(in crate::exec) struct ReorientedInnerJoinRuntime<'a> {
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	build: ReorientedInnerJoinBuild,
	key_plan: JoinKeyPlan,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	post_filter: Option<&'a BoundExprProgram>,
	probe_columns_first: bool,
	probe_rows: u64,
	output_rows: u64,
}

pub(in crate::exec) enum ReorientedInnerJoinBuild {
	InMemory {
		batch: RecordBatch,
		rows: HashMap<JoinKey, BuildRowList>,
	},
	Spilled {
		build: SpilledInnerJoinBuild,
		materialized_output_schema: SchemaRef,
		build_column_count: usize,
		projection: Option<ProjectedInnerBuildProjection>,
	},
}

pub(in crate::exec) struct ProjectedInnerBuildProjection {
	full_build_schema: SchemaRef,
	retained_columns: Vec<crate::ir::ColumnOrdinal>,
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

impl ReorientedInnerJoinRuntime<'_> {
	pub(in crate::exec) fn build_row_count(&self) -> u64 {
		match &self.build {
			ReorientedInnerJoinBuild::InMemory { batch, .. } => batch.num_rows() as u64,
			ReorientedInnerJoinBuild::Spilled { build, .. } => build.input_row_count(),
		}
	}

	pub(in crate::exec) async fn cleanup(self) {
		if let ReorientedInnerJoinBuild::Spilled { build, .. } = self.build {
			build.cleanup().await;
		}
	}
}

pub(in crate::exec) struct StreamingSemiAntiChain<'a> {
	base: &'a ExecutionPlan,
	inner_stages: Vec<ReorientedInnerJoinStage<'a>>,
	semi_stage: StreamingSemiAntiStage<'a>,
	anti_stage: StreamingSemiAntiStage<'a>,
}

pub(in crate::exec) struct StreamingScanProbeInput<'a> {
	scan: &'a ExecutionPlan,
	filter_predicate: Option<&'a BoundExprProgram>,
	exchange: Option<StreamingScanProbeExchange<'a>>,
}

#[derive(Clone, Copy)]
pub(in crate::exec) struct StreamingScanProbeExchange<'a> {
	partition_count: u32,
	key: &'a BoundExprProgram,
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
			retained_build_columns: None,
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

fn collect_aggregate_columns(
	aggr_expr: &[BoundAggregateCall],
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	for call in aggr_expr {
		collect_program_column_ordinals(&call.args, columns);
	}
}

fn collect_left_side_program_columns(
	program: &BoundExprProgram,
	left_column_count: usize,
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	let mut referenced = BTreeSet::new();
	collect_program_column_ordinals(program, &mut referenced);
	for column in referenced {
		if (column.0 as usize) < left_column_count {
			columns.insert(column);
		}
	}
}

fn mark_reoriented_inner_stage_projections(
	stages: &mut [ReorientedInnerJoinStage<'_>],
	mut required_after: BTreeSet<ColumnOrdinal>,
) -> Result<(), ExecutionError> {
	for stage in stages.iter_mut().rev() {
		let mut required_output = required_after.clone();
		if let Some(predicate) = stage.post_filter {
			collect_program_column_ordinals(predicate, &mut required_output);
		}
		let retained = retained_build_columns_for_reoriented_stage(stage, &required_output)?;
		let build_column_count = stage.build.schema().fields().len();
		stage.retained_build_columns = if retained.len() < build_column_count {
			Some(retained)
		} else {
			None
		};
		required_after = required_probe_columns_for_reoriented_stage(stage, &required_output)?;
	}
	Ok(())
}

fn retained_build_columns_for_reoriented_stage(
	stage: &ReorientedInnerJoinStage<'_>,
	required_output: &BTreeSet<ColumnOrdinal>,
) -> Result<Vec<ColumnOrdinal>, ExecutionError> {
	let probe_column_count = stage.probe_schema.fields().len();
	let build_column_count = stage.build.schema().fields().len();
	let output_column_count = stage.output_schema.fields().len();
	let mut retained = BTreeSet::new();
	retained.extend(stage.build_keys.iter().copied());
	for column in required_output {
		let index = column.0 as usize;
		if index >= output_column_count {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"reoriented inner required output column {} exceeds output width {}",
					column.0, output_column_count
				),
			});
		}
		let build_index = if stage.probe_columns_first {
			index.checked_sub(probe_column_count)
		} else if index < build_column_count {
			Some(index)
		} else {
			None
		};
		if let Some(build_index) = build_index {
			if build_index < build_column_count {
				retained.insert(ColumnOrdinal(build_index as u32));
			}
		}
	}
	Ok(retained.into_iter().collect())
}

fn required_probe_columns_for_reoriented_stage(
	stage: &ReorientedInnerJoinStage<'_>,
	required_output: &BTreeSet<ColumnOrdinal>,
) -> Result<BTreeSet<ColumnOrdinal>, ExecutionError> {
	let probe_column_count = stage.probe_schema.fields().len();
	let build_column_count = stage.build.schema().fields().len();
	let output_column_count = stage.output_schema.fields().len();
	let mut required = BTreeSet::new();
	required.extend(stage.probe_keys.iter().copied());
	for column in required_output {
		let index = column.0 as usize;
		if index >= output_column_count {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"reoriented inner required output column {} exceeds output width {}",
					column.0, output_column_count
				),
			});
		}
		let probe_index = if stage.probe_columns_first {
			if index < probe_column_count {
				Some(index)
			} else {
				None
			}
		} else {
			index
				.checked_sub(build_column_count)
				.filter(|index| *index < probe_column_count)
		};
		if let Some(probe_index) = probe_index {
			required.insert(ColumnOrdinal(probe_index as u32));
		}
	}
	Ok(required)
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

pub(in crate::exec) fn decompose_streaming_scan_probe_input(
	plan: &ExecutionPlan,
) -> Option<StreamingScanProbeInput<'_>> {
	match plan {
		ExecutionPlan::Scan { .. } => Some(StreamingScanProbeInput {
			scan: plan,
			filter_predicate: None,
			exchange: None,
		}),
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => {
			let ExecutionPlan::Scan { .. } = input.as_ref() else {
				return None;
			};
			let filter_predicate = match disposition {
				FilterDisposition::EvaluateInToron => Some(predicate),
				FilterDisposition::FullyPushed => None,
			};
			Some(StreamingScanProbeInput {
				scan: input.as_ref(),
				filter_predicate,
				exchange: None,
			})
		}
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => {
			let mut scan_input = decompose_streaming_scan_probe_input(input.as_ref())?;
			if scan_input.exchange.is_some() {
				return None;
			}
			scan_input.exchange = Some(StreamingScanProbeExchange {
				partition_count: *partition_count,
				key,
			});
			Some(scan_input)
		}
		_ => None,
	}
}

fn concat_join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
	Arc::new(Schema::new(
		left.fields()
			.iter()
			.chain(right.fields().iter())
			.map(|field| field.as_ref().clone())
			.collect::<Vec<_>>(),
	))
}

fn reorder_spilled_inner_probe_before_build(
	batch: RecordBatch,
	build_column_count: usize,
	output_schema: &SchemaRef,
) -> Result<RecordBatch, ExecutionError> {
	let column_count = batch.num_columns();
	if build_column_count > column_count || output_schema.fields().len() != column_count {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"streamed spilled inner join column restore expected {} build columns in {} output columns",
				build_column_count, column_count
			),
		});
	}
	let mut columns = Vec::with_capacity(column_count);
	for column in &batch.columns()[build_column_count..] {
		columns.push(column.clone());
	}
	for column in &batch.columns()[..build_column_count] {
		columns.push(column.clone());
	}
	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

#[allow(clippy::too_many_arguments)]
async fn visit_projected_streamed_spilled_inner_join_chunks<F>(
	spilled: &mut SpilledInnerJoinBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	full_build_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	output_schema: &SchemaRef,
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
				let batch = materialize_projected_streamed_spilled_inner_matches(
					spilled,
					&probe_chunk,
					chunk,
					full_build_schema,
					retained_columns,
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

async fn materialize_projected_streamed_spilled_inner_matches(
	spilled: &mut SpilledInnerJoinBuild,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
	full_build_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	output_schema: &SchemaRef,
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

	let retained_positions = retained_columns
		.iter()
		.enumerate()
		.map(|(position, ordinal)| (ordinal.0 as usize, position))
		.collect::<BTreeMap<_, _>>();
	let mut default_columns = BTreeMap::new();
	let mut default_column_bytes = 0u64;
	for (column_index, field) in full_build_schema.fields().iter().enumerate() {
		if retained_positions.contains_key(&column_index) {
			continue;
		}
		let column = default_array_for_unused_column(field.data_type(), matches.len())?;
		default_column_bytes = default_column_bytes
			.saturating_add(u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX));
		default_columns.insert(column_index, column);
	}

	let estimated_output_bytes = join_take_index_bytes(matches.len())
		.saturating_add(join_interleave_index_bytes(matches.len()))
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			matches.len(),
		))
		.saturating_add(estimate_selected_batch_bytes(probe_batch, matches.len()))
		.saturating_add(default_column_bytes);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"projected streamed inner join output materialization",
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
						"projected streamed inner join missing spilled page source {}",
						row.build.page_index
					),
				})?;
		build_indices.push((source_index, row.build.row as usize));
		probe_indices.push(Some(row.probe));
	}

	let mut columns =
		Vec::with_capacity(full_build_schema.fields().len() + probe_batch.num_columns());
	for column_index in 0..full_build_schema.fields().len() {
		if let Some(retained_position) = retained_positions.get(&column_index).copied() {
			let source_columns = build_pages
				.iter()
				.map(|batch| batch.column(retained_position).as_ref())
				.collect::<Vec<_>>();
			columns.push(interleave(&source_columns, &build_indices)?);
		} else {
			columns.push(default_columns.remove(&column_index).ok_or_else(|| {
				ExecutionError::Kernel {
					message: format!(
						"projected streamed inner join missing default column {}",
						column_index
					),
				}
			})?);
		}
	}
	let probe_indices = UInt32Array::from(probe_indices);
	for column in probe_batch.columns() {
		columns.push(take(column, &probe_indices, None)?);
	}

	Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

fn predicate_references_only_probe_prefix(
	predicate: &BoundExprProgram,
	probe_columns_first: bool,
	probe_column_count: usize,
) -> bool {
	if !probe_columns_first {
		return false;
	}
	let mut columns = BTreeSet::new();
	collect_program_column_ordinals(predicate, &mut columns);
	columns
		.iter()
		.all(|column| (column.0 as usize) < probe_column_count)
}

fn decompose_join_or_filtered_join(
	plan: &ExecutionPlan,
) -> Option<(&ExecutionPlan, Option<&BoundExprProgram>)> {
	match plan {
		ExecutionPlan::Join {
			kind: JoinKind::Inner,
			..
		} => Some((plan, None)),
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
				return None;
			};
			let filter_predicate = match disposition {
				FilterDisposition::FullyPushed => None,
				FilterDisposition::EvaluateInToron => Some(predicate),
			};
			Some((input.as_ref(), filter_predicate))
		}
		_ => None,
	}
}

fn limit_streaming_scan_batch(
	batch: RecordBatch,
	remaining: &mut Option<usize>,
) -> Option<RecordBatch> {
	let Some(limit) = remaining.as_mut() else {
		return Some(batch);
	};
	if *limit == 0 {
		return None;
	}
	let batch = if batch.num_rows() > *limit {
		batch.slice(0, *limit)
	} else {
		batch
	};
	*limit = (*limit).saturating_sub(batch.num_rows());
	Some(batch)
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
				retained_build_columns: None,
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
				retained_build_columns: None,
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

	#[allow(clippy::too_many_arguments)]
	pub(in crate::exec) async fn try_visit_streaming_inner_join_scan_probe_chunks<F>(
		&self,
		build_plan: &ExecutionPlan,
		probe_plan: &ExecutionPlan,
		build_batches: Option<&[RecordBatch]>,
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		replay_id: JoinReplayId,
		strategy: JoinStrategy,
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if probe_columns_first && match_filter.is_some() {
			return Ok(None);
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(probe_plan) else {
			return Ok(None);
		};
		if self.context.task_budget.memory.get() < 64 * 1024 * 1024 {
			return Ok(None);
		}
		let probe_prefilter_predicate = filter_predicate.filter(|predicate| {
			predicate_references_only_probe_prefix(
				predicate,
				probe_columns_first,
				probe_plan.schema().fields().len(),
			)
		});
		let join_filter_predicate = if probe_prefilter_predicate.is_some() {
			None
		} else {
			filter_predicate
		};

		let build_started = Instant::now();
		if let Some((mut spilled, key_plan)) = self
			.try_create_streamed_spilled_inner_scan_build(
				build_plan,
				probe_plan,
				build_keys,
				probe_keys,
				match_filter,
				null_safe_keys,
				morsel,
			)
			.await?
		{
			let materialized_output_schema = if probe_columns_first {
				concat_join_schema(&build_plan.schema(), &probe_plan.schema())
			} else {
				output_schema.clone()
			};
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
			let result = self
				.visit_streaming_spilled_inner_scan_probe_chunks(
					&mut spilled,
					scan_input,
					probe_keys,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					output_schema,
					probe_prefilter_predicate,
					join_filter_predicate,
					probe_columns_first,
					build_plan.schema().fields().len(),
					morsel,
					visit_batch,
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
			return Ok(Some(()));
		}

		let Some(build_batches) = build_batches else {
			return Ok(None);
		};
		if let Some((mut spilled, key_plan)) = try_create_streamed_spilled_inner_build(
			build_plan.schema(),
			probe_plan.schema(),
			build_batches,
			JoinKind::Inner,
			build_keys,
			probe_keys,
			match_filter,
			null_safe_keys,
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?
		{
			let materialized_output_schema = if probe_columns_first {
				concat_join_schema(&build_plan.schema(), &probe_plan.schema())
			} else {
				output_schema.clone()
			};
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
			let result = self
				.visit_streaming_spilled_inner_scan_probe_chunks(
					&mut spilled,
					scan_input,
					probe_keys,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					output_schema,
					probe_prefilter_predicate,
					join_filter_predicate,
					probe_columns_first,
					build_plan.schema().fields().len(),
					morsel,
					visit_batch,
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
			return Ok(Some(()));
		}

		let build_batch = concat_or_empty(build_plan.schema(), build_batches)?;
		let build_key_arrays = key_arrays(&build_batch, build_keys)?;
		if estimated_build_hash_bytes(&build_key_arrays, build_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_plan.schema(),
			build_keys,
			&probe_plan.schema(),
			probe_keys,
			null_safe_keys,
		)?;
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&build_key_arrays,
			build_batch.num_rows(),
			self.context.task_budget.memory.get(),
			null_safe_keys,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(build_batch.num_rows() as u64);
		});

		let probe_started = Instant::now();
		let result = self
			.visit_streaming_in_memory_inner_scan_probe_chunks(
				&build_batch,
				&build_rows,
				join_key_plan,
				scan_input,
				probe_keys,
				match_filter,
				null_safe_keys,
				output_schema,
				probe_prefilter_predicate,
				join_filter_predicate,
				probe_columns_first,
				morsel,
				visit_batch,
			)
			.await;
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
		let (probe_rows, output_rows) = result?;
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id,
				strategy,
				build_rows: RowCount::new(build_batch.num_rows() as u64),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);
		Ok(Some(()))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_create_streamed_spilled_inner_scan_build(
		&self,
		build_plan: &ExecutionPlan,
		probe_plan: &ExecutionPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<(SpilledInnerJoinBuild, JoinKeyPlan)>, ExecutionError> {
		if match_filter.is_some() {
			return Ok(None);
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(build_plan) else {
			return Ok(None);
		};
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_plan.schema(),
			build_keys,
			&probe_plan.schema(),
			probe_keys,
			null_safe_keys,
		)?;
		let mut spilled = SpilledInnerJoinBuild::create(
			self.context.task_budget,
			self.context.join_spill_file_ops.clone(),
			self.context.join_spill_dir.clone(),
		)
		.await?;
		self.write_streaming_scan_inner_build_batches(
			&mut spilled,
			scan_input,
			None,
			build_keys,
			key_plan,
			null_safe_keys,
			morsel,
		)
		.await?;
		spilled.finish(self.context.task_budget).await?;
		Ok(Some((spilled, key_plan)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn write_streaming_scan_inner_build_batches(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(), ExecutionError> {
		if let Some(exchange) = scan_input.exchange {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan build expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				for (_, partitioned) in
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key)?
				{
					self.write_streaming_scan_inner_build_batch(
						spilled,
						&partitioned,
						retained_columns,
						key_plan,
						build_keys,
						null_safe_keys,
					)
					.await?;
				}
			}
		} else {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan build expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				self.write_streaming_scan_inner_build_batch(
					spilled,
					&batch,
					retained_columns,
					key_plan,
					build_keys,
					null_safe_keys,
				)
				.await?;
			}
		}
		Ok(())
	}

	async fn write_streaming_scan_inner_build_batch(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		batch: &RecordBatch,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
	) -> Result<(), ExecutionError> {
		if let Some(retained_columns) = retained_columns {
			spilled
				.write_projected_batch_chunks(
					batch,
					retained_columns,
					key_plan,
					build_keys,
					null_safe_keys,
					self.context.task_budget,
				)
				.await
		} else {
			spilled
				.write_batch_chunks(
					batch,
					key_plan,
					build_keys,
					null_safe_keys,
					self.context.task_budget,
				)
				.await
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_spilled_inner_scan_probe_chunks<F>(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		scan_input: StreamingScanProbeInput<'_>,
		right_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &arrow::datatypes::SchemaRef,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		build_column_count: usize,
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		if let Some(exchange) = scan_input.exchange {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(predicate) = probe_prefilter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"probe filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				for (_, partitioned) in
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key)?
				{
					let (batch_probe_rows, batch_output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							spilled,
							&partitioned,
							right_keys,
							key_plan,
							null_safe_keys,
							materialized_output_schema,
							output_schema,
							filter_predicate,
							probe_columns_first,
							build_column_count,
							None,
							&mut *visit_batch,
						)
						.await?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			}
		} else {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(predicate) = probe_prefilter_predicate {
					batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"probe filter predicate",
					)?;
					if batch.num_rows() == 0 {
						continue;
					}
				}
				let (batch_probe_rows, batch_output_rows) = self
					.visit_streaming_spilled_inner_probe_batch(
						spilled,
						&batch,
						right_keys,
						key_plan,
						null_safe_keys,
						materialized_output_schema,
						output_schema,
						filter_predicate,
						probe_columns_first,
						build_column_count,
						None,
						&mut *visit_batch,
					)
					.await?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_spilled_inner_probe_batch<F>(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		probe_batch: &RecordBatch,
		right_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &arrow::datatypes::SchemaRef,
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		build_column_count: usize,
		projection: Option<&ProjectedInnerBuildProjection>,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut visit_output = |batch: RecordBatch| -> Result<(), ExecutionError> {
			let batch = if probe_columns_first {
				reorder_spilled_inner_probe_before_build(batch, build_column_count, output_schema)?
			} else {
				batch
			};
			let batch = if let Some(predicate) = filter_predicate {
				self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
			} else {
				batch
			};
			if batch.num_rows() > 0 {
				visit_batch(batch)?;
			}
			Ok(())
		};
		if let Some(projection) = projection {
			visit_projected_streamed_spilled_inner_join_chunks(
				spilled,
				std::slice::from_ref(probe_batch),
				right_keys,
				key_plan,
				null_safe_keys,
				&projection.full_build_schema,
				&projection.retained_columns,
				materialized_output_schema,
				self.context.task_budget.memory.get(),
				&mut visit_output,
			)
			.await
		} else {
			visit_streamed_spilled_inner_join_chunks(
				spilled,
				std::slice::from_ref(probe_batch),
				right_keys,
				key_plan,
				null_safe_keys,
				materialized_output_schema,
				self.context.task_budget.memory.get(),
				&mut visit_output,
			)
			.await
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_in_memory_inner_scan_probe_chunks<F>(
		&self,
		build_batch: &RecordBatch,
		build_rows: &HashMap<JoinKey, BuildRowList>,
		join_key_plan: JoinKeyPlan,
		scan_input: StreamingScanProbeInput<'_>,
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		let mut process_probe_batch = |right_batch: RecordBatch| -> Result<(), ExecutionError> {
			probe_rows = probe_rows.saturating_add(right_batch.num_rows() as u64);
			let right_key_arrays = key_arrays(&right_batch, probe_keys)?;
			let selection = probe_in_memory_join_rows(
				JoinKind::Inner,
				join_key_plan,
				build_rows,
				build_batch.num_rows(),
				&right_key_arrays,
				right_batch.num_rows(),
				match_filter,
				build_batch,
				&right_batch,
				null_safe_keys,
			)?;
			output_rows = output_rows.saturating_add(selection.len() as u64);
			self.visit_reoriented_inner_selection_filtered_chunks(
				build_batch,
				&right_batch,
				&selection,
				output_schema,
				filter_predicate,
				probe_columns_first,
				visit_batch,
			)?;
			Ok(())
		};
		if let Some(exchange) = scan_input.exchange {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining)
				else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(predicate) = probe_prefilter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"probe filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				for (_, partitioned) in self.repartition_exchange_batch(
					&right_batch,
					exchange.partition_count,
					exchange.key,
				)? {
					process_probe_batch(partitioned)?;
				}
			}
		} else {
			let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
				return Err(ExecutionError::InvalidExpression {
					message: "streaming scan probe expected a scan input".to_string(),
				});
			};
			let mut remaining = req.row_limit;
			let span = crate::telemetry::storage_scan_span(&req);
			let mut stream = self
				.storage
				.scan(req)
				.instrument(span)
				.await?
				.into_batches();
			while let Some(batch) = stream.next().await {
				let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining)
				else {
					break;
				};
				if let Some(predicate) = scan_input.filter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				if let Some(predicate) = probe_prefilter_predicate {
					right_batch = self.filter_batch_with_bound_predicate(
						predicate,
						right_batch,
						"probe filter predicate",
					)?;
					if right_batch.num_rows() == 0 {
						continue;
					}
				}
				process_probe_batch(right_batch)?;
			}
		}
		Ok((probe_rows, output_rows))
	}

	fn visit_reoriented_inner_selection_filtered_chunks<F>(
		&self,
		build_batch: &RecordBatch,
		probe_batch: &RecordBatch,
		selection: &[JoinOutputRow],
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		visit_batch: &mut F,
	) -> Result<(), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_reoriented_inner_selection(
				probe_batch,
				build_batch,
				chunk,
				output_schema,
				self.context.task_budget.memory.get(),
				probe_columns_first,
			)?;
			let batch = if let Some(predicate) = filter_predicate {
				self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
			} else {
				batch
			};
			if batch.num_rows() > 0 {
				visit_batch(batch)?;
			}
		}
		Ok(())
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

		let left_is_scan_probe = decompose_streaming_scan_probe_input(left).is_some();
		let right_is_scan_probe = decompose_streaming_scan_probe_input(right).is_some();
		let mut left_batches = None;
		let mut right_batches = None;
		if left_is_scan_probe {
			if right_is_scan_probe
				&& self
					.try_visit_streaming_inner_join_scan_probe_chunks(
						right,
						left,
						None,
						right_keys,
						left_keys,
						match_filter.as_ref(),
						*null_safe_keys,
						*replay_id,
						*strategy,
						output_schema,
						filter_predicate,
						true,
						morsel,
						&mut visit_batch,
					)
					.await?
					.is_some()
			{
				return Ok(Some(()));
			}
			let build_batches = self.execute_bound(right, morsel).await?;
			if self
				.try_visit_streaming_inner_join_scan_probe_chunks(
					right,
					left,
					Some(&build_batches),
					right_keys,
					left_keys,
					match_filter.as_ref(),
					*null_safe_keys,
					*replay_id,
					*strategy,
					output_schema,
					filter_predicate,
					true,
					morsel,
					&mut visit_batch,
				)
				.await?
				.is_some()
			{
				return Ok(Some(()));
			}
			right_batches = Some(build_batches);
		}
		if right_is_scan_probe {
			if left_is_scan_probe
				&& self
					.try_visit_streaming_inner_join_scan_probe_chunks(
						left,
						right,
						None,
						left_keys,
						right_keys,
						match_filter.as_ref(),
						*null_safe_keys,
						*replay_id,
						*strategy,
						output_schema,
						filter_predicate,
						false,
						morsel,
						&mut visit_batch,
					)
					.await?
					.is_some()
			{
				return Ok(Some(()));
			}
			let build_batches = match left_batches.take() {
				Some(batches) => batches,
				None => self.execute_bound(left, morsel).await?,
			};
			if self
				.try_visit_streaming_inner_join_scan_probe_chunks(
					left,
					right,
					Some(&build_batches),
					left_keys,
					right_keys,
					match_filter.as_ref(),
					*null_safe_keys,
					*replay_id,
					*strategy,
					output_schema,
					filter_predicate,
					false,
					morsel,
					&mut visit_batch,
				)
				.await?
				.is_some()
			{
				return Ok(Some(()));
			}
			left_batches = Some(build_batches);
		}
		let left_batches = match left_batches {
			Some(batches) => batches,
			None => self.execute_bound(left, morsel).await?,
		};
		let right_batches = match right_batches {
			Some(batches) => batches,
			None => self.execute_bound(right, morsel).await?,
		};
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
		let (join, join_filter_predicate, project, project_filter_predicate) = match input {
			ExecutionPlan::Join {
				kind: JoinKind::Inner,
				..
			} => (input, None, None, None),
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => match input.as_ref() {
				ExecutionPlan::Join {
					kind: JoinKind::Inner,
					..
				} => {
					let filter_predicate = match disposition {
						FilterDisposition::FullyPushed => None,
						FilterDisposition::EvaluateInToron => Some(predicate),
					};
					(input.as_ref(), filter_predicate, None, None)
				}
				ExecutionPlan::Project {
					input,
					exprs,
					output_schema,
				} => {
					let Some((join, join_filter_predicate)) =
						decompose_join_or_filtered_join(input.as_ref())
					else {
						return Ok(None);
					};
					let project_filter_predicate = match disposition {
						FilterDisposition::FullyPushed => None,
						FilterDisposition::EvaluateInToron => Some(predicate),
					};
					(
						join,
						join_filter_predicate,
						Some((exprs, output_schema)),
						project_filter_predicate,
					)
				}
				_ => return Ok(None),
			},
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				let Some((join, filter_predicate)) =
					decompose_join_or_filtered_join(input.as_ref())
				else {
					return Ok(None);
				};
				(join, filter_predicate, Some((exprs, output_schema)), None)
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

		let mut state = init_global_aggregate_state(aggr_expr)?;
		if self
			.try_visit_streaming_join0_chunks(join, join_filter_predicate, morsel, |batch| {
				let batch = if let Some((exprs, project_schema)) = project {
					let cols = self.evaluate_bound_program(exprs, &batch)?;
					RecordBatch::try_new(project_schema.clone(), cols)?
				} else {
					batch
				};
				let batch = if let Some(predicate) = project_filter_predicate {
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
				} else {
					batch
				};
				if batch.num_rows() == 0 {
					Ok(())
				} else {
					self.fold_global_aggregate_state(&mut state, aggr_expr, &batch)
				}
			})
			.await?
			.is_some()
		{
			return finalize_global_aggregate_state(&state, aggr_expr, output_schema).map(Some);
		}
		if project.is_some() {
			return Ok(None);
		}

		let left_batches = self.execute_bound(left, morsel).await?;
		let right_batches = self.execute_bound(right, morsel).await?;
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
			join_filter_predicate,
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
		let Some((base, mut stages)) = extract_reoriented_inner_chain(chain_input) else {
			return Ok(None);
		};
		let mut required_output = BTreeSet::new();
		collect_program_column_ordinals(project_exprs, &mut required_output);
		if let Some(predicate) = outer_predicate {
			collect_program_column_ordinals(predicate, &mut required_output);
		}
		mark_reoriented_inner_stage_projections(&mut stages, required_output)?;

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
					build_rows: RowCount::new(runtime.build_row_count()),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
		}

		let output =
			self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema);
		for runtime in runtimes {
			runtime.cleanup().await;
		}
		output.map(Some)
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
		let Some(mut chain) = extract_streaming_inner_prefix_semi_anti_chain(aggregate_input)
		else {
			return Ok(None);
		};
		let mut required_output = BTreeSet::new();
		required_output.extend(chain.semi_stage.preserved_keys.iter().copied());
		required_output.extend(chain.anti_stage.preserved_keys.iter().copied());
		collect_left_side_program_columns(
			chain.semi_stage.match_filter,
			chain.semi_stage.output_schema.fields().len(),
			&mut required_output,
		);
		collect_left_side_program_columns(
			chain.anti_stage.match_filter,
			chain.anti_stage.output_schema.fields().len(),
			&mut required_output,
		);
		collect_program_column_ordinals(group_expr, &mut required_output);
		collect_aggregate_columns(aggr_expr, &mut required_output);
		mark_reoriented_inner_stage_projections(&mut chain.inner_stages, required_output)?;

		let mut inner_runtimes = Vec::with_capacity(chain.inner_stages.len());
		for stage in chain.inner_stages {
			let Some(runtime) = self
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(stage, morsel)
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
		if self
			.try_streaming_inner_prefix_semi_anti_scan_probe_base(
				chain.base,
				&mut inner_runtimes,
				&mut semi_runtime,
				&mut anti_runtime,
				group_expr,
				aggr_expr,
				&mut groups,
				&mut group_types,
				morsel,
			)
			.await?
			.is_none()
		{
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
				let batch = self.filter_batch_with_bound_predicate(
					join0_filter,
					batch,
					"filter predicate",
				)?;
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
							next.extend(
								self.probe_reoriented_inner_join_streaming(runtime, &batch)
									.await?,
							);
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
		}

		for runtime in &inner_runtimes {
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id: runtime.replay_id,
					strategy: runtime.strategy,
					build_rows: RowCount::new(runtime.build_row_count()),
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
		for runtime in inner_runtimes {
			runtime.cleanup().await;
		}
		semi_runtime.cleanup().await;
		anti_runtime.cleanup().await;
		output.map(Some)
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_inner_prefix_semi_anti_scan_probe_base(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		semi_runtime: &mut StreamingSemiAntiRuntime<'_>,
		anti_runtime: &mut StreamingSemiAntiRuntime<'_>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<()>, ExecutionError> {
		let ExecutionPlan::Exchange {
			input: base_input,
			partition_count,
			key: base_exchange_key,
		} = base
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
		let Some(scan_input) = decompose_streaming_scan_probe_input(right) else {
			return Ok(None);
		};

		let left_batches = self.execute_bound(left, morsel).await?;
		let left_batch = concat_or_empty(left.schema(), &left_batches)?;
		let left_key_arrays = key_arrays(&left_batch, left_keys)?;
		if estimated_build_hash_bytes(&left_key_arrays, left_batch.num_rows())
			> self.context.task_budget.memory.get()
		{
			return Ok(None);
		}
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&left.schema(),
			left_keys,
			&right.schema(),
			right_keys,
			*null_safe_keys,
		)?;
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
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});

		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Ok(None);
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let mut stream = self
			.storage
			.scan(req)
			.instrument(span)
			.await?
			.into_batches();
		let probe_started = Instant::now();
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		while let Some(batch) = stream.next().await {
			let Some(mut right_batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				right_batch = self.filter_batch_with_bound_predicate(
					predicate,
					right_batch,
					"filter predicate",
				)?;
				if right_batch.num_rows() == 0 {
					continue;
				}
			}
			probe_rows = probe_rows.saturating_add(right_batch.num_rows() as u64);
			let right_key_arrays = key_arrays(&right_batch, right_keys)?;
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
			output_rows = output_rows.saturating_add(selection.len() as u64);

			for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let batch = materialize_join_selection(
					JoinKind::Inner,
					&left_batch,
					&right_batch,
					chunk,
					join0_output_schema,
					self.context.task_budget.memory.get(),
				)?;
				let batch = self.filter_batch_with_bound_predicate(
					join0_filter,
					batch,
					"filter predicate",
				)?;
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
							next.extend(
								self.probe_reoriented_inner_join_streaming(runtime, &batch)
									.await?,
							);
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
						semi_batches
							.extend(self.probe_streaming_semi_anti(semi_runtime, &batch).await?);
					}
					let mut anti_batches = Vec::new();
					for batch in semi_batches {
						anti_batches
							.extend(self.probe_streaming_semi_anti(anti_runtime, &batch).await?);
					}
					for batch in anti_batches {
						self.fold_bound_grouped_aggregate_batch(
							groups,
							group_types,
							group_expr,
							aggr_expr,
							&batch,
						)?;
					}
				}
			}
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
		});
		record_join_runtime_observation(
			&self.context.task_telemetry,
			&self.context.join_runtime_observation_mode,
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: *strategy,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);
		Ok(Some(()))
	}

	pub(in crate::exec) async fn prepare_reoriented_inner_join_runtime<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		self.prepare_reoriented_inner_join_runtime_with_scan_build(stage, morsel, false)
			.await
	}

	pub(in crate::exec) async fn prepare_reoriented_inner_join_runtime_with_streamed_scan_build<
		'a,
	>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		self.prepare_reoriented_inner_join_runtime_with_scan_build(stage, morsel, true)
			.await
	}

	async fn prepare_reoriented_inner_join_runtime_with_scan_build<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
		stream_scan_build: bool,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		if stream_scan_build && decompose_streaming_scan_probe_input(stage.build).is_some() {
			let build_schema = stage.build.schema();
			let projected_build = match stage.retained_build_columns.as_deref() {
				Some(retained_columns) if retained_columns.len() < build_schema.fields().len() => {
					let projected_build_keys =
						remap_column_ordinals(retained_columns, &stage.build_keys)?;
					let projected_schema =
						projected_schema_for_columns(&build_schema, retained_columns)?;
					Some((
						retained_columns,
						projected_build_keys,
						projected_schema,
						ProjectedInnerBuildProjection {
							full_build_schema: build_schema.clone(),
							retained_columns: retained_columns.to_vec(),
						},
					))
				}
				_ => None,
			};
			let (key_plan, write_build_keys, retained_columns, projection) = if let Some((
				retained_columns,
				projected_build_keys,
				projected_schema,
				projection,
			)) = &projected_build
			{
				(
					JoinKeyPlan::from_build_schema_and_probe_schema(
						projected_schema,
						projected_build_keys,
						&stage.probe_schema,
						&stage.probe_keys,
						false,
					)?,
					projected_build_keys.as_slice(),
					Some(*retained_columns),
					Some(ProjectedInnerBuildProjection {
						full_build_schema: projection.full_build_schema.clone(),
						retained_columns: projection.retained_columns.clone(),
					}),
				)
			} else {
				(
					JoinKeyPlan::from_build_schema_and_probe_schema(
						&build_schema,
						&stage.build_keys,
						&stage.probe_schema,
						&stage.probe_keys,
						false,
					)?,
					stage.build_keys.as_slice(),
					None,
					None,
				)
			};
			let mut spilled = SpilledInnerJoinBuild::create(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
			)
			.await?;
			let build_started = Instant::now();
			self.write_streaming_scan_inner_build_batches(
				&mut spilled,
				decompose_streaming_scan_probe_input(stage.build).expect("checked scan build"),
				retained_columns,
				write_build_keys,
				key_plan,
				false,
				morsel,
			)
			.await?;
			spilled.finish(self.context.task_budget).await?;
			let build_rows = spilled.input_row_count();
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.counters.build_rows = task.counters.build_rows.saturating_add(build_rows);
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.spill_bytes = task
					.counters
					.spill_bytes
					.saturating_add(spilled.spill_bytes());
			});
			let materialized_output_schema = if stage.probe_columns_first {
				concat_join_schema(&stage.build.schema(), &stage.probe_schema)
			} else {
				stage.output_schema.clone()
			};
			return Ok(Some(ReorientedInnerJoinRuntime {
				replay_id: stage.replay_id,
				strategy: stage.strategy,
				build: ReorientedInnerJoinBuild::Spilled {
					build: spilled,
					materialized_output_schema,
					build_column_count: stage.build.schema().fields().len(),
					projection,
				},
				key_plan,
				probe_keys: stage.probe_keys,
				output_schema: stage.output_schema,
				post_filter: stage.post_filter,
				probe_columns_first: stage.probe_columns_first,
				probe_rows: 0,
				output_rows: 0,
			}));
		}

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
			build: ReorientedInnerJoinBuild::InMemory {
				batch: build_batch,
				rows: build_rows,
			},
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
		let Some(req) = self.bound_scan_request(scan_plan, morsel) else {
			return Ok(None);
		};
		let mut remaining = req.row_limit;
		let span = crate::telemetry::storage_scan_span(&req);
		let stream = self.storage.scan(req).instrument(span).await?;
		let mut stream = stream.into_batches();
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
		let (output, probe_rows, output_rows) = {
			let ReorientedInnerJoinBuild::InMemory { batch, rows } = &runtime.build else {
				return Err(ExecutionError::InvalidExpression {
					message: "synchronous reoriented inner probe requires an in-memory build"
						.to_string(),
				});
			};
			self.probe_reoriented_inner_join_in_memory(
				runtime.key_plan,
				&runtime.probe_keys,
				&runtime.output_schema,
				runtime.probe_columns_first,
				batch,
				rows,
				probe_batch,
			)?
		};
		self.record_reoriented_inner_probe(runtime, probe_rows, output_rows);
		Ok(output)
	}

	pub(in crate::exec) async fn probe_reoriented_inner_join_streaming(
		&self,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		probe_batch: &RecordBatch,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let (output, probe_rows, output_rows) = match &mut runtime.build {
			ReorientedInnerJoinBuild::InMemory { batch, rows } => self
				.probe_reoriented_inner_join_in_memory(
					runtime.key_plan,
					&runtime.probe_keys,
					&runtime.output_schema,
					runtime.probe_columns_first,
					batch,
					rows,
					probe_batch,
				)?,
			ReorientedInnerJoinBuild::Spilled {
				build,
				materialized_output_schema,
				build_column_count,
				projection,
			} => {
				if probe_batch.num_rows() == 0 {
					(Vec::new(), 0, 0)
				} else {
					let mut output = Vec::new();
					let (probe_rows, output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							build,
							probe_batch,
							&runtime.probe_keys,
							runtime.key_plan,
							false,
							materialized_output_schema,
							&runtime.output_schema,
							None,
							runtime.probe_columns_first,
							*build_column_count,
							projection.as_ref(),
							&mut |batch| {
								output.push(batch);
								Ok(())
							},
						)
						.await?;
					(output, probe_rows, output_rows)
				}
			}
		};
		self.record_reoriented_inner_probe(runtime, probe_rows, output_rows);
		Ok(output)
	}

	fn record_reoriented_inner_probe(
		&self,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		probe_rows: u64,
		output_rows: u64,
	) {
		runtime.probe_rows = runtime.probe_rows.saturating_add(probe_rows);
		runtime.output_rows = runtime.output_rows.saturating_add(output_rows);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
			task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
		});
	}

	fn probe_reoriented_inner_join_in_memory(
		&self,
		key_plan: JoinKeyPlan,
		probe_keys: &[crate::ir::ColumnOrdinal],
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		build_batch: &RecordBatch,
		build_rows: &HashMap<JoinKey, BuildRowList>,
		probe_batch: &RecordBatch,
	) -> Result<(Vec<RecordBatch>, u64, u64), ExecutionError> {
		if probe_batch.num_rows() == 0 {
			return Ok((Vec::new(), 0, 0));
		}
		let probe_key_arrays = key_arrays(probe_batch, probe_keys)?;
		let mut selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			key_plan,
			build_rows,
			build_batch.num_rows(),
			&probe_key_arrays,
			probe_batch.num_rows(),
			None,
			build_batch,
			probe_batch,
			false,
		)?;
		let output_rows = selection.len() as u64;
		let mut output = Vec::new();
		for chunk in selection.chunks_mut(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_reoriented_inner_selection(
				probe_batch,
				build_batch,
				chunk,
				output_schema,
				self.context.task_budget.memory.get(),
				probe_columns_first,
			)?;
			if batch.num_rows() > 0 {
				output.push(batch);
			}
		}
		Ok((output, probe_batch.num_rows() as u64, output_rows))
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::error::StorageError;
	use crate::ir::{
		BoundExprProgram, BoundExprProgramBuilder, BroadcastReplayEligibility, ColumnName,
		ColumnOrdinal, QueryProfileHint, ResolvedColumn, TableId,
	};
	use crate::storage::{ScanRequest, ScanStream, StorageScan};
	use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::collections::HashMap;
	use std::sync::{Arc, Mutex};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy,
		TableObjectId, TableSchemaDescriptor,
	};

	struct RecordingMultiBatchTableStorageScan {
		batches: HashMap<TableId, Vec<RecordBatch>>,
		requested_tables: Arc<Mutex<Vec<TableId>>>,
	}

	#[async_trait::async_trait]
	impl StorageScan for RecordingMultiBatchTableStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			self.requested_tables
				.lock()
				.expect("requested tables lock")
				.push(request.table_id);
			let batches = self.batches.get(&request.table_id).cloned().ok_or(
				StorageError::TableNotFound {
					table: request.table_id,
				},
			)?;
			let schema = batches
				.first()
				.map(RecordBatch::schema)
				.unwrap_or_else(|| Arc::new(Schema::empty()));
			Ok(ScanStream::from_batches(
				schema,
				request.session_id,
				request.query_id,
				batches,
			))
		}
	}

	fn test_scan_plan_with_first_field_key(
		table_id: TableId,
		schema: Arc<Schema>,
		object_byte: u8,
	) -> ExecutionPlan {
		let primary_key = schema.field(0).name().to_string();
		let descriptor = TableSchemaDescriptor::new(
			TableObjectId::new([object_byte; 16]),
			1,
			"default",
			"join_test",
			schema.as_ref(),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor");
		ExecutionPlan::Scan {
			local_table_id: table_id,
			table: TableObjectId::new([object_byte; 16]),
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema,
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			row_limit: None,
			profile_hint: QueryProfileHint::Scan,
			primary_key_columns: vec![ColumnOrdinal(0)],
			column_ndv: Vec::new(),
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		}
	}

	fn concat_test_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
		Arc::new(Schema::new(
			left.fields()
				.iter()
				.chain(right.fields().iter())
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>(),
		))
	}

	fn inner_join_plan(
		left: ExecutionPlan,
		right: ExecutionPlan,
		left_keys: Vec<ColumnOrdinal>,
		right_keys: Vec<ColumnOrdinal>,
		output_schema: SchemaRef,
	) -> ExecutionPlan {
		ExecutionPlan::Join {
			replay_id: JoinReplayId(0),
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(left),
			right: Box::new(right),
			kind: JoinKind::Inner,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
		}
	}

	#[tokio::test]
	async fn streaming_inner_join_scan_probe_visits_probe_batches_without_materializing_scan() {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("left_value", DataType::Int64, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("right_value", DataType::Int64, false),
		]));
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![10, 20])),
			],
		)
		.expect("left batch");
		let right_batch_one = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1])),
				Arc::new(Int64Array::from(vec![100])),
			],
		)
		.expect("right batch one");
		let right_batch_two = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![2])),
				Arc::new(Int64Array::from(vec![200])),
			],
		)
		.expect("right batch two");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([
				(TableId(1), vec![left_batch]),
				(TableId(2), vec![right_batch_one, right_batch_two]),
			]),
			requested_tables: requested_tables.clone(),
		});
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(TaskBudget {
				memory: ByteCount::new(256 * 1024 * 1024),
				spill_bytes: ByteCount::new(256 * 1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			}),
		);
		let join = inner_join_plan(
			test_scan_plan_with_first_field_key(TableId(1), left_schema.clone(), 1),
			test_scan_plan_with_first_field_key(TableId(2), right_schema.clone(), 2),
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			concat_test_schema(&left_schema, &right_schema),
		);
		let mut visited = Vec::new();

		let matched = executor
			.try_visit_streaming_join0_chunks(&join, None, None, |batch| {
				visited.push(batch);
				Ok(())
			})
			.await
			.expect("stream scan probe");

		assert!(matched.is_some());
		assert_eq!(
			visited.len(),
			1,
			"the scan-probe side should be joined and visited without first materializing it"
		);
		let joined =
			concat_or_empty(join.schema(), &visited).expect("joined scan-probe output batches");
		assert_eq!(joined.num_rows(), 2);
		let left_values = joined
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left value");
		let right_values = joined
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right value");
		assert_eq!(left_values.values(), &[10, 20]);
		assert_eq!(right_values.values(), &[100, 200]);
		assert_eq!(
			requested_tables
				.lock()
				.expect("requested tables")
				.as_slice(),
			&[TableId(2), TableId(1)]
		);
	}

	#[tokio::test]
	async fn streaming_inner_join_left_scan_probe_restores_original_column_order() {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("left_value", DataType::Int64, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("right_value", DataType::Int64, false),
		]));
		let left_batch_one = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1])),
				Arc::new(Int64Array::from(vec![10])),
			],
		)
		.expect("left batch one");
		let left_batch_two = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![2])),
				Arc::new(Int64Array::from(vec![20])),
			],
		)
		.expect("left batch two");
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![100, 200])),
			],
		)
		.expect("right batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([
				(TableId(1), vec![left_batch_one, left_batch_two]),
				(TableId(2), vec![right_batch]),
			]),
			requested_tables: requested_tables.clone(),
		});
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(TaskBudget {
				memory: ByteCount::new(256 * 1024 * 1024),
				spill_bytes: ByteCount::new(256 * 1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			}),
		);
		let join = inner_join_plan(
			test_scan_plan_with_first_field_key(TableId(1), left_schema.clone(), 1),
			test_scan_plan_with_first_field_key(TableId(2), right_schema.clone(), 2),
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			concat_test_schema(&left_schema, &right_schema),
		);
		let mut visited = Vec::new();

		let matched = executor
			.try_visit_streaming_join0_chunks(&join, None, None, |batch| {
				visited.push(batch);
				Ok(())
			})
			.await
			.expect("stream left scan probe");

		assert!(matched.is_some());
		assert_eq!(
			visited.len(),
			2,
			"left scan probe should stream each scan batch after building the right side"
		);
		let joined =
			concat_or_empty(join.schema(), &visited).expect("joined scan-probe output batches");
		assert_eq!(joined.schema(), join.schema());
		let left_values = joined
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("left value");
		let right_values = joined
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("right value");
		assert_eq!(left_values.values(), &[10, 20]);
		assert_eq!(right_values.values(), &[100, 200]);
		assert_eq!(
			requested_tables
				.lock()
				.expect("requested tables")
				.as_slice(),
			&[TableId(2), TableId(1)]
		);
	}

	#[tokio::test]
	async fn reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order() {
		for value_kind in [StreamingValueKind::Float64, StreamingValueKind::Decimal] {
			let value_type = value_kind.data_type();
			let build_schema = Arc::new(Schema::new(vec![
				Field::new("build_key", DataType::Int64, false),
				Field::new("build_value", value_type.clone(), true),
			]));
			let probe_schema = Arc::new(Schema::new(vec![
				Field::new("probe_key", DataType::Int64, false),
				Field::new("probe_tag", DataType::Int64, false),
			]));
			let output_schema = concat_test_schema(&probe_schema, &build_schema);
			let build_a = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![1, 2])),
					value_kind.values(&[100, 200]),
				],
			)
			.expect("build batch a");
			let build_b = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![3])),
					value_kind.values(&[300]),
				],
			)
			.expect("build batch b");
			let probe = RecordBatch::try_new(
				probe_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![3, 1, 2])),
					Arc::new(Int64Array::from(vec![30, 10, 20])),
				],
			)
			.expect("probe batch");
			let requested_tables = Arc::new(Mutex::new(Vec::new()));
			let storage = Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([(TableId(3), vec![build_a, build_b])]),
				requested_tables: requested_tables.clone(),
			});
			let executor = Executor::with_context(
				storage,
				ExecutionContext::local_test().with_task_budget(TaskBudget {
					memory: ByteCount::new(256 * 1024 * 1024),
					spill_bytes: ByteCount::new(256 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				}),
			);
			let scan = test_scan_plan_with_first_field_key(TableId(3), build_schema.clone(), 3);
			let build = ExecutionPlan::Exchange {
				input: Box::new(scan),
				partition_count: 1,
				key: bound_column_root("build_key", ColumnOrdinal(0), DataType::Int64, false),
			};
			let mut streamed_runtime = executor
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
					reoriented_scan_build_stage(
						&build,
						probe_schema.clone(),
						output_schema.clone(),
					),
					None,
				)
				.await
				.expect("streamed runtime")
				.expect("scan build should stream");
			let ReorientedInnerJoinBuild::Spilled { build: spilled, .. } = &streamed_runtime.build
			else {
				panic!("scan build side must use the streamed-spilled build");
			};
			assert_eq!(
				spilled.input_batches_written(),
				2,
				"scan build batches must be spilled one at a time, not concat_or_empty'd"
			);
			let mut materialized_runtime = executor
				.prepare_reoriented_inner_join_runtime(
					reoriented_scan_build_stage(
						&build,
						probe_schema.clone(),
						output_schema.clone(),
					),
					None,
				)
				.await
				.expect("materialized runtime")
				.expect("materialized build should prepare");

			let streamed = executor
				.probe_reoriented_inner_join_streaming(&mut streamed_runtime, &probe)
				.await
				.expect("streamed probe");
			let materialized = executor
				.probe_reoriented_inner_join(&mut materialized_runtime, &probe)
				.expect("materialized probe");
			assert_record_batches_equal(&streamed, &materialized);
			assert_eq!(
				streamed.first().expect("streamed output").schema().as_ref(),
				output_schema.as_ref(),
				"streamed spilled build must restore probe-first column order"
			);
			assert_eq!(
				requested_tables
					.lock()
					.expect("requested tables")
					.as_slice(),
				&[TableId(3), TableId(3)]
			);

			streamed_runtime.cleanup().await;
			materialized_runtime.cleanup().await;
		}
	}

	#[tokio::test]
	async fn projected_reoriented_inner_runtime_remaps_keys_and_restores_observable_schema() {
		for value_kind in [StreamingValueKind::Float64, StreamingValueKind::Decimal] {
			let value_type = value_kind.data_type();
			let build_schema = Arc::new(Schema::new(vec![
				Field::new("build_key", DataType::Int64, false),
				Field::new("build_value", value_type.clone(), true),
				Field::new("dead_payload", DataType::Utf8, false),
			]));
			let probe_schema = Arc::new(Schema::new(vec![
				Field::new("probe_key", DataType::Int64, false),
				Field::new("probe_tag", DataType::Int64, false),
			]));
			let output_schema = concat_test_schema(&probe_schema, &build_schema);
			let build_a = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![1, 2])),
					value_kind.values(&[100, 200]),
					Arc::new(StringArray::from(vec!["wide-a", "wide-b"])),
				],
			)
			.expect("projected build batch a");
			let build_b = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![3])),
					value_kind.values(&[300]),
					Arc::new(StringArray::from(vec!["wide-c"])),
				],
			)
			.expect("projected build batch b");
			let probe = RecordBatch::try_new(
				probe_schema.clone(),
				vec![
					Arc::new(Int64Array::from(vec![3, 1, 2])),
					Arc::new(Int64Array::from(vec![30, 10, 20])),
				],
			)
			.expect("probe batch");
			let requested_tables = Arc::new(Mutex::new(Vec::new()));
			let storage = Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([(TableId(4), vec![build_a, build_b])]),
				requested_tables,
			});
			let executor = Executor::with_context(
				storage,
				ExecutionContext::local_test().with_task_budget(TaskBudget {
					memory: ByteCount::new(256 * 1024 * 1024),
					spill_bytes: ByteCount::new(256 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				}),
			);
			let scan = test_scan_plan_with_first_field_key(TableId(4), build_schema.clone(), 4);
			let build = ExecutionPlan::Exchange {
				input: Box::new(scan),
				partition_count: 1,
				key: bound_column_root("build_key", ColumnOrdinal(0), DataType::Int64, false),
			};
			let mut projected_stage =
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone());
			projected_stage.retained_build_columns = Some(vec![ColumnOrdinal(0), ColumnOrdinal(1)]);
			let mut projected_runtime = executor
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
					projected_stage,
					None,
				)
				.await
				.expect("projected streamed runtime")
				.expect("projected scan build should stream");
			{
				let ReorientedInnerJoinBuild::Spilled {
					build: spilled,
					projection,
					..
				} = &mut projected_runtime.build
				else {
					panic!("projected scan build must use the streamed-spilled build");
				};
				assert!(projection.is_some(), "projection metadata must be carried");
				assert_eq!(spilled.input_batches_written(), 2);
				let page = spilled.read_batch_page(0).await.expect("projected page");
				assert_eq!(
					page.num_columns(),
					2,
					"projected reoriented build pages should omit dead payload columns"
				);
			}

			let mut materialized_runtime = executor
				.prepare_reoriented_inner_join_runtime(
					reoriented_scan_build_stage(
						&build,
						probe_schema.clone(),
						output_schema.clone(),
					),
					None,
				)
				.await
				.expect("materialized runtime")
				.expect("materialized build should prepare");

			let projected = executor
				.probe_reoriented_inner_join_streaming(&mut projected_runtime, &probe)
				.await
				.expect("projected streamed probe");
			let materialized = executor
				.probe_reoriented_inner_join(&mut materialized_runtime, &probe)
				.expect("materialized probe");
			let projected_batch =
				concat_or_empty(output_schema.clone(), &projected).expect("projected concat");
			let materialized_batch =
				concat_or_empty(output_schema.clone(), &materialized).expect("materialized concat");
			assert_eq!(projected_batch.schema(), output_schema);
			assert_eq!(projected_batch.num_rows(), materialized_batch.num_rows());
			for column in 0..4 {
				assert_eq!(
					format!("{:?}", projected_batch.column(column)),
					format!("{:?}", materialized_batch.column(column)),
					"retained output column {column} differs"
				);
			}
			let dead_payload = projected_batch
				.column(4)
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("dead payload defaults");
			assert!(
				(0..dead_payload.len()).all(|row| dead_payload.value(row).is_empty()),
				"dead payload is restored only as sparse ordinal-compatible defaults"
			);

			projected_runtime.cleanup().await;
			materialized_runtime.cleanup().await;
		}
	}

	#[derive(Clone, Copy)]
	enum StreamingValueKind {
		Float64,
		Decimal,
	}

	impl StreamingValueKind {
		fn data_type(self) -> DataType {
			match self {
				Self::Float64 => DataType::Float64,
				Self::Decimal => DataType::Decimal128(20, 2),
			}
		}

		fn values(self, cents: &[i128]) -> ArrayRef {
			match self {
				Self::Float64 => Arc::new(Float64Array::from(
					cents
						.iter()
						.map(|value| Some(*value as f64 / 100.0))
						.collect::<Vec<_>>(),
				)),
				Self::Decimal => Arc::new(
					Decimal128Array::from(
						cents.iter().map(|value| Some(*value)).collect::<Vec<_>>(),
					)
					.with_precision_and_scale(20, 2)
					.expect("decimal build values"),
				),
			}
		}
	}

	fn bound_column_root(
		name: &str,
		ordinal: ColumnOrdinal,
		data_type: DataType,
		nullable: bool,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let column = builder.append_column(ResolvedColumn {
			name: ColumnName(name.to_string()),
			ordinal,
			data_type,
			nullable,
			pbc_eligible: false,
		});
		builder.push_root(column);
		builder.build()
	}

	fn reoriented_scan_build_stage(
		build: &ExecutionPlan,
		probe_schema: SchemaRef,
		output_schema: SchemaRef,
	) -> ReorientedInnerJoinStage<'_> {
		ReorientedInnerJoinStage {
			replay_id: JoinReplayId(1),
			probe_schema,
			probe_keys: vec![ColumnOrdinal(0)],
			build,
			build_keys: vec![ColumnOrdinal(0)],
			retained_build_columns: None,
			output_schema,
			strategy: JoinStrategy::HashPartitioned,
			post_filter: None,
			probe_columns_first: true,
		}
	}

	fn assert_record_batches_equal(left: &[RecordBatch], right: &[RecordBatch]) {
		let left_schema = left.first().expect("left batches").schema();
		let right_schema = right.first().expect("right batches").schema();
		assert_eq!(left_schema, right_schema);
		let left_batch =
			arrow::compute::concat_batches(&left_schema, left.iter()).expect("left concat");
		let right_batch =
			arrow::compute::concat_batches(&right_schema, right.iter()).expect("right concat");
		assert_eq!(left_batch.num_rows(), right_batch.num_rows());
		assert_eq!(left_batch.num_columns(), right_batch.num_columns());
		for column in 0..left_batch.num_columns() {
			assert_eq!(
				format!("{:?}", left_batch.column(column)),
				format!("{:?}", right_batch.column(column)),
				"column {column} differs"
			);
		}
	}
}
