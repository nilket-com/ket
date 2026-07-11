use super::*;

const STREAMED_INNER_LOADED_BUILD_ESTIMATE_SAFETY_MULTIPLIER: u64 = 64;
const STREAMED_INNER_BUILD_MAX_PARTITIONS: u32 = 1024;

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
	build: ReorientedInnerJoinBuild<'a>,
	key_plan: JoinKeyPlan,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	output_schema: SchemaRef,
	post_filter: Option<&'a BoundExprProgram>,
	probe_columns_first: bool,
	probe_rows: u64,
	output_rows: u64,
}

enum ReorientedStageScanBuildDecision {
	Stream,
	StreamMaterializedBatches(Vec<RecordBatch>),
	Materialized,
	MaterializedBatches(Vec<RecordBatch>),
}

fn reoriented_stage_scan_build_decision_for_materialized_batches(
	scan_output_bytes: u64,
	build_hash_bytes: u64,
	materialization_cap: u64,
	batches: Vec<RecordBatch>,
) -> ReorientedStageScanBuildDecision {
	// A scan-shaped build can fit as Arrow batches while the hash accumulator would not.
	if scan_output_bytes > materialization_cap {
		ReorientedStageScanBuildDecision::Stream
	} else if build_hash_bytes > materialization_cap {
		ReorientedStageScanBuildDecision::StreamMaterializedBatches(batches)
	} else {
		ReorientedStageScanBuildDecision::MaterializedBatches(batches)
	}
}

pub(in crate::exec) enum ReorientedInnerJoinBuild<'a> {
	InMemory {
		batch: RecordBatch,
		rows: JoinBuildRows,
	},
	Spilled {
		build: SpilledInnerJoinBuild,
		materialized_output_schema: SchemaRef,
		build_column_count: usize,
		projection: Option<ProjectedInnerBuildProjection>,
	},
	PartitionedScan {
		scan_input: StreamingScanProbeInput<'a>,
		morsel: Option<(MorselId, u32)>,
		partition_count: u32,
		group_count: u32,
		partition_build_keys: Vec<crate::ir::ColumnOrdinal>,
		write_build_keys: Vec<crate::ir::ColumnOrdinal>,
		materialized_output_schema: SchemaRef,
		build_column_count: usize,
		projection: Option<ProjectedInnerBuildProjection>,
		estimated_build_bytes: u64,
		observed_build_rows: u64,
		loaded_builds: Vec<Option<LoadedSpilledInnerBuild>>,
		loaded_resident_bytes: u64,
		loaded_build_attempted: bool,
	},
}

#[derive(Clone)]
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
	pub(in crate::exec) match_filter: Option<&'a BoundExprProgram>,
	pub(in crate::exec) output_schema: SchemaRef,
	pub(in crate::exec) strategy: JoinStrategy,
}

#[allow(dead_code)]
pub(in crate::exec) enum StreamingSemiAntiExistence {
	InMemory {
		batch: RecordBatch,
		rows: JoinBuildRows,
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
	pub(in crate::exec) match_filter: Option<&'a BoundExprProgram>,
	pub(in crate::exec) output_schema: SchemaRef,
	pub(in crate::exec) probe_rows: u64,
	pub(in crate::exec) output_rows: u64,
}

pub(in crate::exec) struct StreamingLeftOuterRecombineRuntime<'a> {
	replay_id: JoinReplayId,
	strategy: JoinStrategy,
	scalar_batch: RecordBatch,
	scalar_rows: JoinBuildRows,
	key_plan: JoinKeyPlan,
	preserved_keys: Vec<crate::ir::ColumnOrdinal>,
	match_filter: Option<&'a BoundExprProgram>,
	null_safe_keys: bool,
	output_schema: SchemaRef,
	probe_rows: u64,
	output_rows: u64,
}

struct StreamingLeftOuterGroupedInput<'a> {
	join: &'a ExecutionPlan,
	exchange: Option<(u32, &'a BoundExprProgram)>,
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
			ReorientedInnerJoinBuild::PartitionedScan {
				observed_build_rows,
				..
			} => *observed_build_rows,
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

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
struct StreamingScanJoinPartition<'a> {
	keys: &'a [crate::ir::ColumnOrdinal],
	partition_count: u32,
	partition: u32,
	null_safe_keys: bool,
}

struct LoadedPartitionWork {
	partition: usize,
	build_pages: Vec<RecordBatch>,
	probe_batch: RecordBatch,
	probe_keys: Vec<crate::ir::ColumnOrdinal>,
	write_build_keys: Vec<crate::ir::ColumnOrdinal>,
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	materialized_output_schema: SchemaRef,
	output_schema: SchemaRef,
	probe_columns_first: bool,
	build_column_count: usize,
	projection: Option<ProjectedInnerBuildProjection>,
	memory_cap: u64,
}

struct LoadedPartitionResult {
	partition: usize,
	output: Vec<RecordBatch>,
	probe_rows: u64,
	output_rows: u64,
}

#[derive(Clone, Copy)]
struct ParallelPartitionPlan {
	k: usize,
	resident_peak: u64,
}

fn partition_streaming_join_batch_with_buckets(
	batch: &RecordBatch,
	buckets: &[Option<u32>],
	partition_count: u32,
) -> Result<Vec<(usize, RecordBatch)>, ExecutionError> {
	if buckets.len() != batch.num_rows() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"partition bucket count {} did not match batch rows {}",
				buckets.len(),
				batch.num_rows()
			),
		});
	}

	let mut builders: Vec<Vec<u32>> = (0..partition_count).map(|_| Vec::new()).collect();
	for (row, row_partition) in buckets.iter().enumerate() {
		let Some(partition) = row_partition else {
			continue;
		};
		let partition = usize::try_from(*partition).map_err(|_| ExecutionError::Kernel {
			message: "join partition bucket did not fit usize".to_string(),
		})?;
		let Some(indices) = builders.get_mut(partition) else {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"join partition bucket {} exceeded partition count {}",
					partition, partition_count
				),
			});
		};
		indices.push(join_probe_row_index(row)?);
	}

	let mut partitioned = Vec::new();
	for (partition, indices) in builders.into_iter().enumerate() {
		if indices.is_empty() {
			continue;
		}
		let indices = UInt32Array::from(indices);
		let columns = batch
			.columns()
			.iter()
			.map(|column| take(column, &indices, None))
			.collect::<Result<Vec<_>, _>>()?;
		partitioned.push((partition, RecordBatch::try_new(batch.schema(), columns)?));
	}
	Ok(partitioned)
}

fn partition_streaming_join_batch_active_group_with_buckets(
	batch: &RecordBatch,
	buckets: &[Option<u32>],
	partition_count: u32,
	partition_start: usize,
	partition_end: usize,
) -> Result<Vec<(usize, RecordBatch)>, ExecutionError> {
	if buckets.len() != batch.num_rows() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"partition bucket count {} did not match batch rows {}",
				buckets.len(),
				batch.num_rows()
			),
		});
	}
	let partition_count_usize =
		usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
			message: "join partition count did not fit usize".to_string(),
		})?;
	if partition_start > partition_end || partition_end > partition_count_usize {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"active join partition range {partition_start}..{partition_end} exceeded partition count {partition_count}"
			),
		});
	}

	let mut builders: Vec<Vec<u32>> = (partition_start..partition_end)
		.map(|_| Vec::new())
		.collect();
	for (row, row_partition) in buckets.iter().enumerate() {
		let Some(partition) = row_partition else {
			continue;
		};
		let partition = usize::try_from(*partition).map_err(|_| ExecutionError::Kernel {
			message: "join partition bucket did not fit usize".to_string(),
		})?;
		if partition >= partition_count_usize {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"join partition bucket {} exceeded partition count {}",
					partition, partition_count
				),
			});
		}
		if partition < partition_start || partition >= partition_end {
			continue;
		}
		builders[partition - partition_start].push(join_probe_row_index(row)?);
	}

	let mut partitioned = Vec::new();
	for (offset, indices) in builders.into_iter().enumerate() {
		if indices.is_empty() {
			continue;
		}
		let indices = UInt32Array::from(indices);
		let columns = batch
			.columns()
			.iter()
			.map(|column| take(column, &indices, None))
			.collect::<Result<Vec<_>, _>>()?;
		partitioned.push((
			partition_start + offset,
			RecordBatch::try_new(batch.schema(), columns)?,
		));
	}
	Ok(partitioned)
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

fn retained_existence_columns_for_semi_anti_stage(
	stage: &StreamingSemiAntiStage<'_>,
) -> Result<Option<Vec<ColumnOrdinal>>, ExecutionError> {
	let existence_schema = stage.existence.schema();
	let existence_width = existence_schema.fields().len();
	let mut columns = BTreeSet::new();
	columns.extend(stage.existence_keys.iter().copied());
	if let Some(match_filter) = stage.match_filter {
		let preserved_width = stage.output_schema.fields().len();
		let mut referenced = BTreeSet::new();
		collect_program_column_ordinals(match_filter, &mut referenced);
		for column in referenced {
			let index = column.0 as usize;
			if index < preserved_width {
				continue;
			}
			let existence_index = index - preserved_width;
			if existence_index >= existence_width {
				return Err(ExecutionError::InvalidExpression {
					message: format!(
						"semi/anti residual existence column {} exceeds existence width {}",
						existence_index, existence_width
					),
				});
			}
			columns.insert(ColumnOrdinal(existence_index as u32));
		}
	}
	let retained = columns.into_iter().collect::<Vec<_>>();
	if retained.is_empty() || retained.len() >= existence_width {
		Ok(None)
	} else {
		Ok(Some(retained))
	}
}

fn retained_left_outer_grouped_columns(
	left_column_count: usize,
	left_keys: &[ColumnOrdinal],
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	exchange: Option<(u32, &BoundExprProgram)>,
) -> Vec<ColumnOrdinal> {
	let mut columns = BTreeSet::new();
	columns.extend(left_keys.iter().copied());
	collect_left_side_program_columns(group_expr, left_column_count, &mut columns);
	if let Some((_partition_count, exchange_key)) = exchange {
		collect_left_side_program_columns(exchange_key, left_column_count, &mut columns);
	}
	for call in aggr_expr {
		collect_left_side_program_columns(&call.args, left_column_count, &mut columns);
	}
	columns.into_iter().collect()
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

fn decompose_streaming_left_outer_grouped_input<'a>(
	input: &'a ExecutionPlan,
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
) -> Result<Option<StreamingLeftOuterGroupedInput<'a>>, ExecutionError> {
	match input {
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => {
			if *partition_count == 0 {
				return Err(ExecutionError::InvalidExpression {
					message: "exchange partition_count must be greater than zero".to_string(),
				});
			}
			if key != group_expr && !grouped_aggregate_is_order_insensitive(aggr_expr) {
				return Ok(None);
			}
			if !is_streaming_left_outer_grouped_join(input.as_ref()) {
				return Ok(None);
			}
			Ok(Some(StreamingLeftOuterGroupedInput {
				join: input.as_ref(),
				exchange: Some((*partition_count, key)),
			}))
		}
		_ if is_streaming_left_outer_grouped_join(input) => {
			Ok(Some(StreamingLeftOuterGroupedInput {
				join: input,
				exchange: None,
			}))
		}
		_ => Ok(None),
	}
}

fn is_streaming_left_outer_grouped_join(input: &ExecutionPlan) -> bool {
	let ExecutionPlan::Join {
		right,
		kind: JoinKind::LeftOuter,
		strategy: JoinStrategy::HashPartitioned,
		..
	} = input
	else {
		return false;
	};
	decompose_streaming_scan_probe_input(right).is_some()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReorientedInnerChainGroupedBaseScanProbeMode {
	FilteredJoin0,
	BareJoin0,
}

fn reoriented_inner_chain_grouped_base_scan_probe_mode(
	base: &ExecutionPlan,
) -> Option<ReorientedInnerChainGroupedBaseScanProbeMode> {
	let ExecutionPlan::Exchange {
		input: base_input, ..
	} = base
	else {
		return None;
	};
	let (join0, mode) = match base_input.as_ref() {
		ExecutionPlan::Filter {
			input,
			disposition: FilterDisposition::EvaluateInToron,
			..
		} => (
			input.as_ref(),
			ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0,
		),
		other @ ExecutionPlan::Join { .. } => (
			other,
			ReorientedInnerChainGroupedBaseScanProbeMode::BareJoin0,
		),
		_ => return None,
	};
	let ExecutionPlan::Join {
		replay_id,
		right,
		kind: JoinKind::Inner,
		..
	} = join0
	else {
		return None;
	};
	(*replay_id == JoinReplayId(0)
		&& decompose_streaming_scan_probe_input(right.as_ref()).is_some())
	.then_some(mode)
}

fn reoriented_grouped_required_output_columns(
	project_exprs: Option<&BoundExprProgram>,
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	outer_predicate: Option<&BoundExprProgram>,
) -> BTreeSet<ColumnOrdinal> {
	let mut required_output = BTreeSet::new();
	if let Some(project_exprs) = project_exprs {
		collect_program_column_ordinals(project_exprs, &mut required_output);
	} else {
		collect_program_column_ordinals(group_expr, &mut required_output);
		collect_aggregate_columns(aggr_expr, &mut required_output);
	}
	if let Some(predicate) = outer_predicate {
		collect_program_column_ordinals(predicate, &mut required_output);
	}
	required_output
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
			let page_map = load_streamed_inner_match_pages(spilled, &matches).await?;
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let batch = materialize_projected_streamed_spilled_inner_matches(
					&page_map,
					&probe_chunk,
					chunk,
					full_build_schema,
					retained_columns,
					output_schema,
					memory_cap,
				)?;
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
			}
		}
	}
	Ok((probe_rows, output_rows))
}

pub(super) async fn load_streamed_inner_match_pages(
	spilled: &mut SpilledInnerJoinBuild,
	matches: &[SpilledInnerJoinMatch],
) -> Result<BTreeMap<usize, RecordBatch>, ExecutionError> {
	let mut page_sources = BTreeMap::new();
	for row in matches {
		page_sources
			.entry(row.build.page_index)
			.or_insert(usize::MAX);
	}
	let mut pages = BTreeMap::new();
	for page_index in page_sources.keys().copied() {
		pages.insert(page_index, spilled.read_batch_page(page_index).await?);
	}
	Ok(pages)
}

fn materialize_projected_streamed_spilled_inner_matches(
	page_map: &BTreeMap<usize, RecordBatch>,
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
		build_pages.push(page_map.get(&page_index).cloned().ok_or_else(|| {
			ExecutionError::Kernel {
				message: format!(
					"projected streamed inner join missing preloaded spilled page {page_index}"
				),
			}
		})?);
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

fn run_loaded_partition_work(
	work: LoadedPartitionWork,
) -> Result<LoadedPartitionResult, ExecutionError> {
	let LoadedPartitionWork {
		partition,
		build_pages,
		probe_batch,
		probe_keys,
		write_build_keys,
		key_plan,
		null_safe_keys,
		materialized_output_schema,
		output_schema,
		probe_columns_first,
		build_column_count,
		projection,
		memory_cap,
	} = work;
	let loaded = LoadedSpilledInnerBuild::new(
		build_pages,
		key_plan,
		&write_build_keys,
		null_safe_keys,
		memory_cap,
	)?;
	visit_loaded_partition_work(
		&loaded,
		partition,
		probe_batch,
		&probe_keys,
		key_plan,
		null_safe_keys,
		&materialized_output_schema,
		&output_schema,
		probe_columns_first,
		build_column_count,
		projection.as_ref(),
		memory_cap,
	)
}

#[allow(clippy::too_many_arguments)]
fn visit_loaded_partition_work(
	loaded: &LoadedSpilledInnerBuild,
	partition: usize,
	probe_batch: RecordBatch,
	probe_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
	materialized_output_schema: &SchemaRef,
	output_schema: &SchemaRef,
	probe_columns_first: bool,
	build_column_count: usize,
	projection: Option<&ProjectedInnerBuildProjection>,
	memory_cap: u64,
) -> Result<LoadedPartitionResult, ExecutionError> {
	let mut output = Vec::new();
	let (probe_rows, output_rows) = if let Some(projection) = projection {
		visit_loaded_projected_streamed_spilled_inner_join_chunks(
			loaded,
			std::slice::from_ref(&probe_batch),
			probe_keys,
			key_plan,
			null_safe_keys,
			&projection.full_build_schema,
			&projection.retained_columns,
			materialized_output_schema,
			memory_cap,
			|batch| {
				let batch = if probe_columns_first {
					reorder_spilled_inner_probe_before_build(
						batch,
						build_column_count,
						output_schema,
					)?
				} else {
					batch
				};
				if batch.num_rows() > 0 {
					output.push(batch);
				}
				Ok(())
			},
		)?
	} else {
		visit_loaded_streamed_spilled_inner_join_chunks(
			loaded,
			std::slice::from_ref(&probe_batch),
			probe_keys,
			key_plan,
			null_safe_keys,
			materialized_output_schema,
			memory_cap,
			|batch| {
				let batch = if probe_columns_first {
					reorder_spilled_inner_probe_before_build(
						batch,
						build_column_count,
						output_schema,
					)?
				} else {
					batch
				};
				if batch.num_rows() > 0 {
					output.push(batch);
				}
				Ok(())
			},
		)?
	};
	Ok(LoadedPartitionResult {
		partition,
		output,
		probe_rows,
		output_rows,
	})
}

pub(in crate::exec::join) struct LoadedSpilledInnerBuild {
	pages: Vec<RecordBatch>,
	index: HashMap<JoinKey, Vec<SpilledBatchRowRef>>,
	resident_bytes: u64,
}

impl LoadedSpilledInnerBuild {
	fn new(
		pages: Vec<RecordBatch>,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		memory_cap: u64,
	) -> Result<Self, ExecutionError> {
		let mut index = HashMap::new();
		let mut resident_index_bytes = 0u64;
		let cap = join_spill_retained_footprint_memory_cap(memory_cap);
		let loaded_page_bytes = pages_loaded_bytes(&pages);
		for (page_index, page) in pages.iter().enumerate() {
			let key_arrays = key_arrays(page, build_keys)?;
			for row in 0..page.num_rows() {
				let Some(key) = join_key_at(key_plan, &key_arrays, row, null_safe_keys)? else {
					continue;
				};
				append_loaded_spilled_batch_row_ref(
					&mut index,
					&mut resident_index_bytes,
					key,
					SpilledBatchRowRef {
						page_index,
						row: join_build_row_index(row)?,
					},
				);
			}
			let resident = resident_index_bytes
				.saturating_add(loaded_page_bytes)
				.saturating_add(64 * 1024);
			if resident > cap {
				return Err(budget_exhausted(
					BudgetDimension::Memory,
					"parallel streamed inner join loaded partition resident state",
					resident,
					cap,
				));
			}
		}
		let resident_bytes = resident_index_bytes
			.saturating_add(loaded_page_bytes)
			.saturating_add(64 * 1024);
		Ok(Self {
			pages,
			index,
			resident_bytes,
		})
	}

	fn lookup(&self, join_key: &JoinKey) -> &[SpilledBatchRowRef] {
		self.index.get(join_key).map(Vec::as_slice).unwrap_or(&[])
	}

	fn resident_bytes(&self) -> u64 {
		self.resident_bytes
	}
}

fn append_loaded_spilled_batch_row_ref(
	index: &mut HashMap<JoinKey, Vec<SpilledBatchRowRef>>,
	resident_index_bytes: &mut u64,
	key: JoinKey,
	row_ref: SpilledBatchRowRef,
) {
	match index.entry(key) {
		Entry::Occupied(mut entry) => {
			entry.get_mut().push(row_ref);
		}
		Entry::Vacant(entry) => {
			*resident_index_bytes =
				resident_index_bytes.saturating_add(loaded_join_key_index_base_bytes(entry.key()));
			entry.insert(vec![row_ref]);
		}
	}
	*resident_index_bytes =
		resident_index_bytes.saturating_add(std::mem::size_of::<SpilledBatchRowRef>() as u64);
}

fn loaded_join_key_index_base_bytes(key: &JoinKey) -> u64 {
	let heap_bytes = match key {
		JoinKey::Int32(_) => std::mem::size_of::<i32>(),
		JoinKey::Int64(_) => std::mem::size_of::<i64>(),
		JoinKey::Generic(bytes) => bytes.len(),
	};
	u64::try_from(64usize.saturating_add(heap_bytes)).unwrap_or(u64::MAX)
}

fn pages_loaded_bytes(pages: &[RecordBatch]) -> u64 {
	pages
		.iter()
		.map(record_batch_memory_bytes)
		.fold(0u64, u64::saturating_add)
}

#[allow(clippy::too_many_arguments)]
fn visit_loaded_projected_streamed_spilled_inner_join_chunks<F>(
	loaded: &LoadedSpilledInnerBuild,
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
				for build in loaded.lookup(&key) {
					matches.push(SpilledInnerJoinMatch {
						build: *build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let batch = materialize_loaded_projected_streamed_spilled_inner_matches(
					loaded,
					&probe_chunk,
					chunk,
					full_build_schema,
					retained_columns,
					output_schema,
					memory_cap,
				)?;
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
			}
		}
	}
	Ok((probe_rows, output_rows))
}

#[allow(clippy::too_many_arguments)]
fn visit_loaded_streamed_spilled_inner_join_chunks<F>(
	loaded: &LoadedSpilledInnerBuild,
	right_batches: &[RecordBatch],
	right_keys: &[crate::ir::ColumnOrdinal],
	key_plan: JoinKeyPlan,
	null_safe_keys: bool,
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
				for build in loaded.lookup(&key) {
					matches.push(SpilledInnerJoinMatch {
						build: *build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
				let batch = materialize_loaded_streamed_spilled_inner_matches(
					loaded,
					&probe_chunk,
					chunk,
					output_schema,
					memory_cap,
				)?;
				output_rows = output_rows.saturating_add(batch.num_rows() as u64);
				visit_batch(batch)?;
			}
		}
	}
	Ok((probe_rows, output_rows))
}

fn materialize_loaded_projected_streamed_spilled_inner_matches(
	loaded: &LoadedSpilledInnerBuild,
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
	let build_pages = page_indices
		.iter()
		.map(|page_index| loaded_page(loaded, *page_index).cloned())
		.collect::<Result<Vec<_>, _>>()?;

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
			"parallel projected streamed inner join output materialization",
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
						"parallel projected streamed inner join missing loaded page source {}",
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
						"parallel projected streamed inner join missing default column {}",
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

fn materialize_loaded_streamed_spilled_inner_matches(
	loaded: &LoadedSpilledInnerBuild,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
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
	let build_pages = page_indices
		.iter()
		.map(|page_index| loaded_page(loaded, *page_index).cloned())
		.collect::<Result<Vec<_>, _>>()?;

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
			"parallel streamed inner join output materialization",
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
						"parallel streamed inner join missing loaded page source {}",
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

fn loaded_page(
	loaded: &LoadedSpilledInnerBuild,
	page_index: usize,
) -> Result<&RecordBatch, ExecutionError> {
	loaded
		.pages
		.get(page_index)
		.ok_or_else(|| ExecutionError::Kernel {
			message: format!("parallel streamed inner join loaded page {page_index} is missing"),
		})
}

async fn materialize_projected_streamed_spilled_left_outer_matches(
	spilled: &mut SpilledInnerJoinBuild,
	probe_batch: &RecordBatch,
	matches: &[SpilledInnerJoinMatch],
	full_left_schema: &SchemaRef,
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
	for (column_index, field) in full_left_schema.fields().iter().enumerate() {
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
			"projected spilled left outer output materialization",
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
						"projected spilled left outer missing spilled page source {}",
						row.build.page_index
					),
				})?;
		build_indices.push((source_index, row.build.row as usize));
		probe_indices.push(Some(row.probe));
	}

	let mut columns =
		Vec::with_capacity(full_left_schema.fields().len() + probe_batch.num_columns());
	for column_index in 0..full_left_schema.fields().len() {
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
						"projected spilled left outer missing default column {}",
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

async fn materialize_projected_streamed_spilled_left_outer_unmatched(
	spilled: &mut SpilledInnerJoinBuild,
	refs: &[SpilledBatchRowRef],
	full_left_schema: &SchemaRef,
	retained_columns: &[ColumnOrdinal],
	right_schema: &SchemaRef,
	output_schema: &SchemaRef,
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

	let retained_positions = retained_columns
		.iter()
		.enumerate()
		.map(|(position, ordinal)| (ordinal.0 as usize, position))
		.collect::<BTreeMap<_, _>>();
	let mut default_columns = BTreeMap::new();
	let mut default_column_bytes = 0u64;
	for (column_index, field) in full_left_schema.fields().iter().enumerate() {
		if retained_positions.contains_key(&column_index) {
			continue;
		}
		let column = default_array_for_unused_column(field.data_type(), refs.len())?;
		default_column_bytes = default_column_bytes
			.saturating_add(u64::try_from(column.get_array_memory_size()).unwrap_or(u64::MAX));
		default_columns.insert(column_index, column);
	}

	let estimated_output_bytes = join_interleave_index_bytes(refs.len())
		.saturating_add(estimate_spilled_inner_build_selection_bytes(
			&build_pages,
			refs.len(),
		))
		.saturating_add(default_column_bytes);
	if estimated_output_bytes > materialization_cap {
		return Err(budget_exhausted(
			BudgetDimension::Memory,
			"projected spilled left outer unmatched materialization",
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
						"projected spilled left outer missing unmatched page source {}",
						row.page_index
					),
				})?;
		build_indices.push((source_index, row.row as usize));
	}

	let mut columns = Vec::with_capacity(output_schema.fields().len());
	for column_index in 0..full_left_schema.fields().len() {
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
						"projected spilled left outer missing unmatched default column {}",
						column_index
					),
				}
			})?);
		}
	}
	for field in right_schema.fields() {
		columns.push(arrow::array::new_null_array(field.data_type(), refs.len()));
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

fn estimate_streaming_scan_build_batch_bytes(
	batch: &RecordBatch,
	retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
	build_keys: &[crate::ir::ColumnOrdinal],
	_null_safe_keys: bool,
) -> Result<u64, ExecutionError> {
	let projected;
	let batch = if let Some(retained_columns) = retained_columns {
		projected = project_batch_columns(batch, retained_columns)?;
		&projected
	} else {
		batch
	};
	let key_arrays = key_arrays(batch, build_keys)?;
	Ok(estimated_build_hash_bytes(&key_arrays, batch.num_rows())
		.max(record_batch_memory_bytes(batch)))
}

fn add_streaming_scan_build_partition_byte_estimates(
	partition_bytes: &mut [u64],
	batch: &RecordBatch,
	retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
	partition_build_keys: &[crate::ir::ColumnOrdinal],
	write_build_keys: &[crate::ir::ColumnOrdinal],
	partition_count: u32,
	null_safe_keys: bool,
) -> Result<(), ExecutionError> {
	let buckets = precompute_join_partition_buckets(
		batch,
		partition_build_keys,
		partition_count,
		null_safe_keys,
		false,
	)?;
	for partition in 0..partition_count {
		let partitioned = select_join_partition_batch_with_buckets(batch, &buckets, partition)?;
		if partitioned.num_rows() == 0 {
			continue;
		}
		let partition_index = usize::try_from(partition).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition exceeds usize".to_string(),
		})?;
		partition_bytes[partition_index] = partition_bytes[partition_index].saturating_add(
			estimate_streaming_scan_build_batch_bytes(
				&partitioned,
				retained_columns,
				write_build_keys,
				null_safe_keys,
			)?,
		);
	}
	Ok(())
}

fn estimate_materialized_inner_build_partition_bytes(
	build_batches: &[RecordBatch],
	build_keys: &[crate::ir::ColumnOrdinal],
	partition_count: u32,
	null_safe_keys: bool,
) -> Result<Vec<u64>, ExecutionError> {
	let partition_count_usize =
		usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds usize".to_string(),
		})?;
	let mut partition_bytes = vec![0u64; partition_count_usize];
	for batch in build_batches {
		add_streaming_scan_build_partition_byte_estimates(
			&mut partition_bytes,
			batch,
			None,
			build_keys,
			build_keys,
			partition_count,
			null_safe_keys,
		)?;
	}
	Ok(partition_bytes)
}

fn streamed_inner_build_partition_count(
	estimated_build_bytes: u64,
	memory_cap: u64,
	spill_budget: u64,
) -> Result<u32, ExecutionError> {
	let resident_cap = join_spill_retained_footprint_memory_cap(memory_cap);
	let memory_partitions = streamed_spill_partition_count(estimated_build_bytes, resident_cap)?;
	let spill_partitions = partition_count_for_target_bytes(
		estimated_build_bytes,
		streamed_inner_build_writer_target_bytes(spill_budget),
		"streamed inner join build spill partition sizing",
		spill_budget,
	)?;
	Ok(memory_partitions.max(spill_partitions))
}

#[cfg(test)]
fn streamed_inner_build_group_count(
	estimated_build_bytes: u64,
	spill_budget: u64,
	partition_count: u32,
) -> Result<u32, ExecutionError> {
	let partition_count_usize =
		usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds usize".to_string(),
		})?;
	if partition_count_usize == 0 {
		return Err(ExecutionError::Kernel {
			message: "streamed inner build partition count must be positive".to_string(),
		});
	}
	let per_partition =
		evenly_distributed_partition_bytes(estimated_build_bytes, partition_count_usize)?;
	let (group_count, _) =
		streamed_inner_build_group_count_for_partition_spill_bytes(&per_partition, spill_budget)?;
	Ok(group_count)
}

fn streamed_inner_build_writer_target_bytes(spill_budget: u64) -> u64 {
	spill_budget.saturating_div(4).max(1)
}

fn streamed_inner_build_group_spill_budget(spill_budget: u64) -> u64 {
	spill_budget.saturating_mul(3).saturating_div(4).max(1)
}

#[cfg(test)]
fn streamed_inner_build_group_overhead_reserve_bytes(spill_budget: u64) -> u64 {
	spill_budget.saturating_sub(streamed_inner_build_group_spill_budget(spill_budget))
}

fn ceil_div_u64(numerator: u64, denominator: u64) -> u64 {
	if numerator == 0 {
		return 0;
	}
	let denominator = denominator.max(1);
	numerator.saturating_add(denominator.saturating_sub(1)) / denominator
}

#[cfg(test)]
fn evenly_distributed_partition_bytes(
	total_bytes: u64,
	partition_count: usize,
) -> Result<Vec<u64>, ExecutionError> {
	if partition_count == 0 {
		return Err(ExecutionError::Kernel {
			message: "streamed inner build partition count must be positive".to_string(),
		});
	}
	let partition_count_u64 =
		u64::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds u64".to_string(),
		})?;
	let base = total_bytes / partition_count_u64;
	let remainder = total_bytes % partition_count_u64;
	let mut bytes = vec![base; partition_count];
	for slot in bytes.iter_mut().take(
		usize::try_from(remainder)
			.unwrap_or(usize::MAX)
			.min(partition_count),
	) {
		*slot = slot.saturating_add(1);
	}
	Ok(bytes)
}

fn max_partition_group_spill_bytes(
	partition_spill_bytes: &[u64],
	group_count: u32,
) -> Result<u64, ExecutionError> {
	let partition_count =
		u32::try_from(partition_spill_bytes.len()).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds u32".to_string(),
		})?;
	if partition_count == 0 {
		return Err(ExecutionError::Kernel {
			message: "streamed inner build partition count must be positive".to_string(),
		});
	}
	let mut max_group = 0u64;
	for group in 0..group_count {
		let range = partition_group_bounds(partition_count, group_count, group)?;
		let group_bytes = partition_spill_bytes[range]
			.iter()
			.fold(0u64, |acc, bytes| acc.saturating_add(*bytes));
		max_group = max_group.max(group_bytes);
	}
	Ok(max_group)
}

fn streamed_inner_build_group_count_for_partition_spill_bytes(
	partition_spill_bytes: &[u64],
	spill_budget: u64,
) -> Result<(u32, u64), ExecutionError> {
	let partition_count =
		u32::try_from(partition_spill_bytes.len()).map_err(|_| ExecutionError::Kernel {
			message: "streamed inner build partition count exceeds u32".to_string(),
		})?;
	if partition_count == 0 {
		return Err(ExecutionError::Kernel {
			message: "streamed inner build partition count must be positive".to_string(),
		});
	}
	let group_spill_budget = streamed_inner_build_group_spill_budget(spill_budget);
	let total_spill = partition_spill_bytes
		.iter()
		.fold(0u64, |acc, bytes| acc.saturating_add(*bytes));
	let target_groups = ceil_div_u64(total_spill, group_spill_budget)
		.max(1)
		.min(u64::from(partition_count));
	let mut group_count = u32::try_from(target_groups).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build group count exceeds u32".to_string(),
	})?;
	if partition_count > 1 {
		group_count = group_count.max(2).min(partition_count);
	}
	loop {
		let max_group = max_partition_group_spill_bytes(partition_spill_bytes, group_count)?;
		if max_group <= group_spill_budget {
			return Ok((group_count, max_group));
		}
		if group_count >= partition_count {
			return Err(budget_exhausted(
				BudgetDimension::SpillBytes,
				"streamed inner join build partition group sizing",
				max_group,
				group_spill_budget,
			));
		}
		group_count = group_count.saturating_add(1).min(partition_count);
	}
}

fn next_streamed_inner_build_partition_count(partition_count: u32) -> Option<u32> {
	if partition_count >= STREAMED_INNER_BUILD_MAX_PARTITIONS {
		return None;
	}
	Some(
		partition_count
			.saturating_mul(2)
			.max(1)
			.min(STREAMED_INNER_BUILD_MAX_PARTITIONS),
	)
}

fn choose_materialized_inner_build_partitioning(
	estimated_build_bytes: u64,
	memory_cap: u64,
	spill_budget: u64,
	build_batches: &[RecordBatch],
	build_keys: &[crate::ir::ColumnOrdinal],
	null_safe_keys: bool,
) -> Result<(u32, u32, u64), ExecutionError> {
	let mut partition_count =
		streamed_inner_build_partition_count(estimated_build_bytes, memory_cap, spill_budget)?;
	loop {
		let partition_bytes = estimate_materialized_inner_build_partition_bytes(
			build_batches,
			build_keys,
			partition_count,
			null_safe_keys,
		)?;
		match streamed_inner_build_group_count_for_partition_spill_bytes(
			&partition_bytes,
			spill_budget,
		) {
			Ok((group_count, max_group_spill)) => {
				return Ok((partition_count, group_count, max_group_spill));
			}
			Err(error) => {
				let Some(next_partition_count) =
					next_streamed_inner_build_partition_count(partition_count)
				else {
					return Err(error);
				};
				if next_partition_count == partition_count {
					return Err(error);
				}
				partition_count = next_partition_count;
			}
		}
	}
}

fn partition_count_for_target_bytes(
	estimated_bytes: u64,
	target_bytes: u64,
	producer: &'static str,
	available: u64,
) -> Result<u32, ExecutionError> {
	const MAX_PARTITIONS: u64 = 1024;
	let target = target_bytes.max(1);
	let needed = estimated_bytes.saturating_add(target.saturating_sub(1)) / target;
	let needed = needed.max(1);
	let Some(partitions) = needed.checked_next_power_of_two() else {
		return Err(budget_exhausted(
			BudgetDimension::SpillBytes,
			producer,
			estimated_bytes,
			available,
		));
	};
	if partitions > MAX_PARTITIONS {
		return Err(budget_exhausted(
			BudgetDimension::SpillBytes,
			producer,
			estimated_bytes,
			available,
		));
	}
	u32::try_from(partitions).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build partition count exceeds u32".to_string(),
	})
}

fn partition_group_bounds(
	partition_count: u32,
	group_count: u32,
	group: u32,
) -> Result<std::ops::Range<usize>, ExecutionError> {
	if partition_count == 0 || group_count == 0 || group >= group_count {
		return Err(ExecutionError::Kernel {
			message: "invalid streamed inner build partition group".to_string(),
		});
	}
	let partition_count = usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build partition count exceeds usize".to_string(),
	})?;
	let group_count = usize::try_from(group_count).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build group count exceeds usize".to_string(),
	})?;
	let group = usize::try_from(group).map_err(|_| ExecutionError::Kernel {
		message: "streamed inner build group index exceeds usize".to_string(),
	})?;
	let start = group.saturating_mul(partition_count) / group_count;
	let end = (group.saturating_add(1)).saturating_mul(partition_count) / group_count;
	Ok(start..end)
}

fn parallel_partition_plan(
	partitioned_builds: &[Option<SpilledInnerJoinBuild>],
	group_probe_batches: &[(usize, RecordBatch)],
	key_plan: JoinKeyPlan,
	configured_parallelism: usize,
	memory_cap: u64,
) -> ParallelPartitionPlan {
	let configured = configured_parallelism.max(1);
	let available = std::thread::available_parallelism()
		.map(std::num::NonZeroUsize::get)
		.unwrap_or(1)
		.max(1);
	let requested = configured.min(available).max(1);
	let resident_peak = group_probe_batches
		.iter()
		.filter_map(|(partition, probe)| {
			let spilled = partitioned_builds.get(*partition)?.as_ref()?;
			Some(estimate_parallel_partition_resident(
				spilled, probe, key_plan,
			))
		})
		.fold(0u64, u64::max);
	let budget = join_spill_retained_footprint_memory_cap(memory_cap).max(1);
	let memory_k = if resident_peak == 0 {
		requested
	} else {
		usize::try_from(budget / resident_peak)
			.unwrap_or(usize::MAX)
			.max(1)
	};
	ParallelPartitionPlan {
		k: requested.min(memory_k).max(1),
		resident_peak,
	}
}

fn estimate_parallel_partition_resident(
	spilled: &SpilledInnerJoinBuild,
	probe: &RecordBatch,
	key_plan: JoinKeyPlan,
) -> u64 {
	let loaded_pages = spilled.loaded_page_bytes_estimate();
	let index =
		estimate_loaded_partition_index_bytes(spilled.input_row_count(), key_plan, loaded_pages);
	loaded_pages
		.saturating_add(index)
		.saturating_add(spilled.writer_peak_bytes())
		.saturating_add(64 * 1024)
		.saturating_add(record_batch_memory_bytes(probe))
}

fn estimate_loaded_partition_index_bytes(
	rows: u64,
	key_plan: JoinKeyPlan,
	loaded_pages_bytes: u64,
) -> u64 {
	if rows == 0 {
		return 0;
	}
	let key_heap_bytes = match key_plan {
		JoinKeyPlan::Int32 => std::mem::size_of::<i32>() as u64,
		JoinKeyPlan::Int64 => std::mem::size_of::<i64>() as u64,
		JoinKeyPlan::Generic => loaded_pages_bytes
			.saturating_add(rows.saturating_sub(1))
			.saturating_div(rows)
			.max(16),
	};
	rows.saturating_mul(
		64u64
			.saturating_add(key_heap_bytes)
			.saturating_add(std::mem::size_of::<SpilledBatchRowRef>() as u64),
	)
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

	let (join2, join2_post_filter) = decompose_join_or_filtered_join(exchange_input(semi_left)?)?;
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

	let (join1, join1_post_filter) = decompose_join_or_filtered_join(exchange_input(join2_left)?)?;
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
				post_filter: join1_post_filter,
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
				post_filter: join2_post_filter,
				probe_columns_first: true,
			},
		],
		semi_stage: StreamingSemiAntiStage {
			replay_id: *semi_replay_id,
			kind: JoinKind::LeftSemi,
			preserved_keys: semi_left_keys.clone(),
			existence: semi_right.as_ref(),
			existence_keys: semi_right_keys.clone(),
			match_filter: Some(semi_filter),
			output_schema: semi_output_schema.clone(),
			strategy: JoinStrategy::HashPartitioned,
		},
		anti_stage: StreamingSemiAntiStage {
			replay_id: *anti_replay_id,
			kind: JoinKind::LeftAnti,
			preserved_keys: anti_left_keys.clone(),
			existence: anti_right.as_ref(),
			existence_keys: anti_right_keys.clone(),
			match_filter: Some(anti_filter),
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
		let Some((preserved_join, preserved_filter)) =
			decompose_join_or_filtered_join(preserved_exchange_input.as_ref())
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
			.try_visit_streaming_join0_chunks(preserved_join, preserved_filter, morsel, |batch| {
				for (partition, partitioned) in self.repartition_exchange_batch(
					&batch,
					*partition_count,
					preserved_exchange_key,
				)? {
					accumulated_bytes =
						accumulated_bytes.saturating_add(record_batch_memory_bytes(&partitioned));
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
			})
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
		if let Some((build_rows, probe_rows, output_rows)) = self
			.try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks(
				build_plan,
				probe_plan,
				scan_input,
				build_keys,
				probe_keys,
				match_filter,
				null_safe_keys,
				output_schema,
				probe_prefilter_predicate,
				join_filter_predicate,
				probe_columns_first,
				morsel,
				&mut *visit_batch,
			)
			.await?
		{
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,
					strategy,
					build_rows: RowCount::new(build_rows),
					probe_rows: RowCount::new(probe_rows),
					output_rows: RowCount::new(output_rows),
				},
			);
			return Ok(Some(()));
		}
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
		if let Some((build_rows, probe_rows, output_rows)) = self
			.try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks(
				build_plan,
				probe_plan,
				scan_input,
				build_batches,
				build_keys,
				probe_keys,
				match_filter,
				null_safe_keys,
				output_schema,
				probe_prefilter_predicate,
				join_filter_predicate,
				probe_columns_first,
				morsel,
				&mut *visit_batch,
			)
			.await?
		{
			record_join_runtime_observation(
				&self.context.task_telemetry,
				&self.context.join_runtime_observation_mode,
				JoinRuntimeObservation {
					replay_id,
					strategy,
					build_rows: RowCount::new(build_rows),
					probe_rows: RowCount::new(probe_rows),
					output_rows: RowCount::new(output_rows),
				},
			);
			return Ok(Some(()));
		}
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
	async fn try_visit_partitioned_streamed_inner_scan_build_scan_probe_chunks<F>(
		&self,
		build_plan: &ExecutionPlan,
		probe_plan: &ExecutionPlan,
		probe_scan_input: StreamingScanProbeInput<'_>,
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<Option<(u64, u64, u64)>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if match_filter.is_some() {
			return Ok(None);
		}
		let Some(build_scan_input) = decompose_streaming_scan_probe_input(build_plan) else {
			return Ok(None);
		};
		let estimated_build_bytes = self
			.estimate_streaming_scan_inner_build_bytes(
				build_scan_input,
				None,
				build_keys,
				null_safe_keys,
				morsel,
			)
			.await?;
		if estimated_build_bytes <= self.context.task_budget.memory.get() {
			return Ok(None);
		}
		let (partition_count, group_count, _max_group_spill) = self
			.choose_streaming_scan_inner_build_partitioning(
				estimated_build_bytes,
				build_scan_input,
				None,
				build_keys,
				build_keys,
				null_safe_keys,
				morsel,
			)
			.await?;
		if partition_count <= 1 {
			return Ok(None);
		}
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_plan.schema(),
			build_keys,
			&probe_plan.schema(),
			probe_keys,
			null_safe_keys,
		)?;
		let materialized_output_schema = if probe_columns_first {
			concat_join_schema(&build_plan.schema(), &probe_plan.schema())
		} else {
			output_schema.clone()
		};

		let group_count = group_count.max(1).min(partition_count);
		let mut total_build_rows = 0u64;
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count).map(|_| None).collect();
			for partition in group_range.clone() {
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await;
				let created = match created {
					Ok(created) => created,
					Err(error) => {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				partitioned_builds[partition] = Some(created);
			}

			let build_started = Instant::now();
			let mut group_spill_bytes = 0u64;
			let build_result = self
				.write_partitioned_streaming_scan_inner_build_batches(
					&mut partitioned_builds,
					&mut group_spill_bytes,
					build_scan_input,
					None,
					build_keys,
					build_keys,
					key_plan,
					null_safe_keys,
					morsel,
					group_range.start,
					group_range.end,
					true,
				)
				.await;
			if let Err(error) = build_result {
				Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
				return Err(error);
			}

			let mut group_build_rows = 0u64;
			for partition in group_range.clone() {
				let Some(spilled) = partitioned_builds
					.get_mut(partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				group_build_rows = group_build_rows.saturating_add(spilled.input_row_count());
				if spilled.input_row_count() > 0 {
					if let Err(error) = spilled
						.rebuild_index_from_pages(
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await
					{
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				}
			}
			total_build_rows = total_build_rows.saturating_add(group_build_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows =
					task.counters.build_rows.saturating_add(group_build_rows);
				task.counters.spill_bytes =
					task.counters.spill_bytes.saturating_add(group_spill_bytes);
			});

			let probe_started = Instant::now();
			let result = self
				.visit_partitioned_streaming_scan_probe_group_chunks(
					&mut partitioned_builds,
					probe_scan_input,
					probe_keys,
					partition_count,
					group_range.start,
					group_range.end,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					output_schema,
					probe_prefilter_predicate,
					filter_predicate,
					probe_columns_first,
					build_plan.schema().fields().len(),
					morsel,
					visit_batch,
				)
				.await;
			let (probe_rows, output_rows) = match result {
				Ok(result) => result,
				Err(error) => {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
			};
			total_probe_rows = total_probe_rows.saturating_add(probe_rows);
			total_output_rows = total_output_rows.saturating_add(output_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
				task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
			});
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}
		Ok(Some((
			total_build_rows,
			total_probe_rows,
			total_output_rows,
		)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_visit_partitioned_streamed_inner_materialized_build_probe_chunks<F>(
		&self,
		build_schema: arrow::datatypes::SchemaRef,
		probe_schema: arrow::datatypes::SchemaRef,
		build_batches: &[RecordBatch],
		probe_batches: &[RecordBatch],
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		_morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<Option<(u64, u64, u64)>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if match_filter.is_some() {
			return Ok(None);
		}
		let estimated_build_bytes =
			estimated_build_hash_bytes_for_batches(build_batches, build_keys)?;
		if estimated_build_bytes <= self.context.task_budget.memory.get() {
			return Ok(None);
		}
		let (partition_count, group_count, _max_group_spill) =
			choose_materialized_inner_build_partitioning(
				estimated_build_bytes,
				self.context.task_budget.memory.get(),
				self.context.task_budget.spill_bytes.get(),
				build_batches,
				build_keys,
				null_safe_keys,
			)?;
		if partition_count <= 1 {
			return Ok(None);
		}
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_schema,
			build_keys,
			&probe_schema,
			probe_keys,
			null_safe_keys,
		)?;
		let build_partition_buckets = build_batches
			.iter()
			.map(|batch| {
				precompute_join_partition_buckets(
					batch,
					build_keys,
					partition_count,
					null_safe_keys,
					false,
				)
			})
			.collect::<Result<Vec<_>, _>>()?;
		let probe_partition_buckets = probe_batches
			.iter()
			.map(|batch| {
				precompute_join_partition_buckets(
					batch,
					probe_keys,
					partition_count,
					null_safe_keys,
					false,
				)
			})
			.collect::<Result<Vec<_>, _>>()?;

		let group_count = group_count.max(1).min(partition_count);
		let group_spill_budget =
			streamed_inner_build_group_spill_budget(self.context.task_budget.spill_bytes.get());
		let mut total_build_rows = 0u64;
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count).map(|_| None).collect();
			for partition in group_range.clone() {
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await;
				let created = match created {
					Ok(created) => created,
					Err(error) => {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				partitioned_builds[partition] = Some(created);
			}

			let build_started = Instant::now();
			let mut group_spill_bytes = 0u64;
			for (batch, buckets) in build_batches.iter().zip(&build_partition_buckets) {
				for partition in group_range.clone() {
					let partition_u32 =
						u32::try_from(partition).map_err(|_| ExecutionError::Kernel {
							message: "streamed inner build partition exceeds u32".to_string(),
						})?;
					let partitioned =
						select_join_partition_batch_with_buckets(batch, buckets, partition_u32)?;
					if partitioned.num_rows() == 0 {
						continue;
					}
					let Some(spilled) = partitioned_builds
						.get_mut(partition)
						.and_then(Option::as_mut)
					else {
						continue;
					};
					let before = spilled.spill_bytes();
					spilled
						.write_batch_chunks_unindexed(
							&partitioned,
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await?;
					group_spill_bytes = group_spill_bytes
						.saturating_add(spilled.spill_bytes().saturating_sub(before));
					if group_spill_bytes > group_spill_budget {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(budget_exhausted(
							BudgetDimension::SpillBytes,
							"streamed inner join build partition group spill writer",
							group_spill_bytes,
							group_spill_budget,
						));
					}
				}
			}
			let mut group_build_rows = 0u64;
			for partition in group_range.clone() {
				let Some(spilled) = partitioned_builds
					.get_mut(partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				group_build_rows = group_build_rows.saturating_add(spilled.input_row_count());
				if spilled.input_row_count() > 0 {
					if let Err(error) = spilled
						.rebuild_index_from_pages(
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await
					{
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				}
			}
			total_build_rows = total_build_rows.saturating_add(group_build_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows =
					task.counters.build_rows.saturating_add(group_build_rows);
				task.counters.spill_bytes =
					task.counters.spill_bytes.saturating_add(group_spill_bytes);
			});

			let probe_started = Instant::now();
			let mut group_probe_rows = 0u64;
			let mut group_output_rows = 0u64;
			for (batch, buckets) in probe_batches.iter().zip(&probe_partition_buckets) {
				for partition in group_range.clone() {
					let partition_u32 =
						u32::try_from(partition).map_err(|_| ExecutionError::Kernel {
							message: "streamed inner probe partition exceeds u32".to_string(),
						})?;
					let partitioned =
						select_join_partition_batch_with_buckets(batch, buckets, partition_u32)?;
					if partitioned.num_rows() == 0 {
						continue;
					}
					let Some(spilled) = partitioned_builds
						.get_mut(partition)
						.and_then(Option::as_mut)
					else {
						continue;
					};
					if spilled.input_row_count() == 0 {
						continue;
					}
					let (probe_rows, output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							spilled,
							&partitioned,
							probe_keys,
							key_plan,
							null_safe_keys,
							output_schema,
							output_schema,
							filter_predicate,
							false,
							build_schema.fields().len(),
							None,
							visit_batch,
						)
						.await?;
					group_probe_rows = group_probe_rows.saturating_add(probe_rows);
					group_output_rows = group_output_rows.saturating_add(output_rows);
				}
			}
			total_probe_rows = total_probe_rows.saturating_add(group_probe_rows);
			total_output_rows = total_output_rows.saturating_add(group_output_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				task.counters.probe_rows =
					task.counters.probe_rows.saturating_add(group_probe_rows);
				task.counters.matched_rows =
					task.counters.matched_rows.saturating_add(group_output_rows);
			});
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}

		Ok(Some((
			total_build_rows,
			total_probe_rows,
			total_output_rows,
		)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_visit_partitioned_streamed_inner_materialized_build_scan_probe_chunks<F>(
		&self,
		build_plan: &ExecutionPlan,
		probe_plan: &ExecutionPlan,
		scan_input: StreamingScanProbeInput<'_>,
		build_batches: &[RecordBatch],
		build_keys: &[crate::ir::ColumnOrdinal],
		probe_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		output_schema: &arrow::datatypes::SchemaRef,
		probe_prefilter_predicate: Option<&BoundExprProgram>,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		morsel: Option<(MorselId, u32)>,
		visit_batch: &mut F,
	) -> Result<Option<(u64, u64, u64)>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if match_filter.is_some() {
			return Ok(None);
		}
		let estimated_build_bytes =
			estimated_build_hash_bytes_for_batches(build_batches, build_keys)?;
		if estimated_build_bytes <= self.context.task_budget.memory.get() {
			return Ok(None);
		}
		let (partition_count, group_count, _max_group_spill) =
			choose_materialized_inner_build_partitioning(
				estimated_build_bytes,
				self.context.task_budget.memory.get(),
				self.context.task_budget.spill_bytes.get(),
				build_batches,
				build_keys,
				null_safe_keys,
			)?;
		if partition_count <= 1 {
			return Ok(None);
		}
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_plan.schema(),
			build_keys,
			&probe_plan.schema(),
			probe_keys,
			null_safe_keys,
		)?;
		let materialized_output_schema = if probe_columns_first {
			concat_join_schema(&build_plan.schema(), &probe_plan.schema())
		} else {
			output_schema.clone()
		};
		let build_partition_buckets = build_batches
			.iter()
			.map(|batch| {
				precompute_join_partition_buckets(
					batch,
					build_keys,
					partition_count,
					null_safe_keys,
					false,
				)
			})
			.collect::<Result<Vec<_>, _>>()?;

		let group_count = group_count.max(1).min(partition_count);
		let group_spill_budget =
			streamed_inner_build_group_spill_budget(self.context.task_budget.spill_bytes.get());
		let mut total_build_rows = 0u64;
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count).map(|_| None).collect();
			for partition in group_range.clone() {
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await;
				let created = match created {
					Ok(created) => created,
					Err(error) => {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				partitioned_builds[partition] = Some(created);
			}

			let build_started = Instant::now();
			let mut group_spill_bytes = 0u64;
			for (batch, buckets) in build_batches.iter().zip(&build_partition_buckets) {
				for partition in group_range.clone() {
					let partition_u32 =
						u32::try_from(partition).map_err(|_| ExecutionError::Kernel {
							message: "streamed inner build partition exceeds u32".to_string(),
						})?;
					let partitioned =
						select_join_partition_batch_with_buckets(batch, buckets, partition_u32)?;
					if partitioned.num_rows() == 0 {
						continue;
					}
					let Some(spilled) = partitioned_builds
						.get_mut(partition)
						.and_then(Option::as_mut)
					else {
						continue;
					};
					let before = spilled.spill_bytes();
					spilled
						.write_batch_chunks_unindexed(
							&partitioned,
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await?;
					group_spill_bytes = group_spill_bytes
						.saturating_add(spilled.spill_bytes().saturating_sub(before));
					if group_spill_bytes > group_spill_budget {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(budget_exhausted(
							BudgetDimension::SpillBytes,
							"streamed inner join build partition group spill writer",
							group_spill_bytes,
							group_spill_budget,
						));
					}
				}
			}
			let mut group_build_rows = 0u64;
			for partition in group_range.clone() {
				let Some(spilled) = partitioned_builds
					.get_mut(partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				group_build_rows = group_build_rows.saturating_add(spilled.input_row_count());
				if spilled.input_row_count() > 0 {
					if let Err(error) = spilled
						.rebuild_index_from_pages(
							key_plan,
							build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await
					{
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				}
			}
			total_build_rows = total_build_rows.saturating_add(group_build_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.build_micros = task
					.phases
					.build_micros
					.saturating_add(duration_micros(build_started));
				task.phases.spill_micros = task
					.phases
					.spill_micros
					.saturating_add(duration_micros(build_started));
				task.counters.build_rows =
					task.counters.build_rows.saturating_add(group_build_rows);
				task.counters.spill_bytes =
					task.counters.spill_bytes.saturating_add(group_spill_bytes);
			});

			let probe_started = Instant::now();
			let result = self
				.visit_partitioned_streaming_scan_probe_group_chunks(
					&mut partitioned_builds,
					scan_input,
					probe_keys,
					partition_count,
					group_range.start,
					group_range.end,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					output_schema,
					probe_prefilter_predicate,
					filter_predicate,
					probe_columns_first,
					build_plan.schema().fields().len(),
					morsel,
					visit_batch,
				)
				.await;
			let (probe_rows, output_rows) = match result {
				Ok(result) => result,
				Err(error) => {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
			};
			total_probe_rows = total_probe_rows.saturating_add(probe_rows);
			total_output_rows = total_output_rows.saturating_add(output_rows);
			record_task_telemetry(&self.context.task_telemetry, |task| {
				task.phases.probe_micros = task
					.phases
					.probe_micros
					.saturating_add(duration_micros(probe_started));
				task.counters.probe_rows = task.counters.probe_rows.saturating_add(probe_rows);
				task.counters.matched_rows = task.counters.matched_rows.saturating_add(output_rows);
			});
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}
		Ok(Some((
			total_build_rows,
			total_probe_rows,
			total_output_rows,
		)))
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
			false,
			None,
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
		retain_unkeyed_rows: bool,
		join_partition: Option<StreamingScanJoinPartition<'_>>,
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
					let partitioned = if let Some(join_partition) = join_partition {
						select_join_partition_batch(
							&partitioned,
							join_partition.keys,
							join_partition.partition_count,
							join_partition.partition,
							join_partition.null_safe_keys,
							false,
						)?
					} else {
						partitioned
					};
					if partitioned.num_rows() == 0 {
						continue;
					}
					self.write_streaming_scan_inner_build_batch(
						spilled,
						&partitioned,
						retained_columns,
						key_plan,
						build_keys,
						null_safe_keys,
						retain_unkeyed_rows,
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
				let batch = if let Some(join_partition) = join_partition {
					select_join_partition_batch(
						&batch,
						join_partition.keys,
						join_partition.partition_count,
						join_partition.partition,
						join_partition.null_safe_keys,
						false,
					)?
				} else {
					batch
				};
				if batch.num_rows() == 0 {
					continue;
				}
				self.write_streaming_scan_inner_build_batch(
					spilled,
					&batch,
					retained_columns,
					key_plan,
					build_keys,
					null_safe_keys,
					retain_unkeyed_rows,
				)
				.await?;
			}
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn write_partitioned_streaming_scan_inner_build_batches(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		total_spill_bytes: &mut u64,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
		partition_start: usize,
		partition_end: usize,
		enforce_group_spill_budget: bool,
	) -> Result<(), ExecutionError> {
		let partition_count =
			u32::try_from(partitioned_builds.len()).map_err(|_| ExecutionError::Kernel {
				message: "partitioned streamed inner build count exceeds u32".to_string(),
			})?;
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
					self.write_partitioned_streaming_scan_inner_build_batch(
						partitioned_builds,
						total_spill_bytes,
						&partitioned,
						retained_columns,
						partition_build_keys,
						write_build_keys,
						partition_count,
						key_plan,
						null_safe_keys,
						partition_start,
						partition_end,
						enforce_group_spill_budget,
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
				self.write_partitioned_streaming_scan_inner_build_batch(
					partitioned_builds,
					total_spill_bytes,
					&batch,
					retained_columns,
					partition_build_keys,
					write_build_keys,
					partition_count,
					key_plan,
					null_safe_keys,
					partition_start,
					partition_end,
					enforce_group_spill_budget,
				)
				.await?;
			}
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn write_partitioned_streaming_scan_inner_build_batch(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		total_spill_bytes: &mut u64,
		batch: &RecordBatch,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		partition_count: u32,
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		partition_start: usize,
		partition_end: usize,
		enforce_group_spill_budget: bool,
	) -> Result<(), ExecutionError> {
		let buckets = precompute_join_partition_buckets(
			batch,
			partition_build_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		for (partition, partitioned) in partition_streaming_join_batch_active_group_with_buckets(
			batch,
			&buckets,
			partition_count,
			partition_start,
			partition_end,
		)? {
			let Some(spilled) = partitioned_builds
				.get_mut(partition)
				.and_then(Option::as_mut)
			else {
				continue;
			};
			let before = spilled.spill_bytes();
			if let Some(retained_columns) = retained_columns {
				spilled
					.write_projected_batch_chunks_unindexed(
						&partitioned,
						retained_columns,
						key_plan,
						write_build_keys,
						null_safe_keys,
						self.context.task_budget,
					)
					.await?;
			} else {
				spilled
					.write_batch_chunks_unindexed(
						&partitioned,
						key_plan,
						write_build_keys,
						null_safe_keys,
						self.context.task_budget,
					)
					.await?;
			}
			*total_spill_bytes =
				total_spill_bytes.saturating_add(spilled.spill_bytes().saturating_sub(before));
			let group_spill_budget =
				streamed_inner_build_group_spill_budget(self.context.task_budget.spill_bytes.get());
			if enforce_group_spill_budget && *total_spill_bytes > group_spill_budget {
				return Err(budget_exhausted(
					BudgetDimension::SpillBytes,
					"partitioned streamed inner join build spill writer",
					*total_spill_bytes,
					group_spill_budget,
				));
			}
		}
		Ok(())
	}

	async fn estimate_streaming_scan_inner_build_bytes(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<u64, ExecutionError> {
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
		let mut estimated = 0u64;
		while let Some(batch) = stream.next().await {
			let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				batch =
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
			}
			if let Some(exchange) = scan_input.exchange {
				for (_, partitioned) in
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key)?
				{
					estimated =
						estimated.saturating_add(estimate_streaming_scan_build_batch_bytes(
							&partitioned,
							retained_columns,
							build_keys,
							null_safe_keys,
						)?);
				}
			} else {
				estimated = estimated.saturating_add(estimate_streaming_scan_build_batch_bytes(
					&batch,
					retained_columns,
					build_keys,
					null_safe_keys,
				)?);
			}
		}
		Ok(estimated)
	}

	async fn estimate_streaming_scan_inner_build_partition_bytes(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		partition_count: u32,
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<u64>, ExecutionError> {
		let partition_count_usize =
			usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
				message: "streamed inner build partition count exceeds usize".to_string(),
			})?;
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
		let mut partition_bytes = vec![0u64; partition_count_usize];
		while let Some(batch) = stream.next().await {
			let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				batch =
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
			}
			if let Some(exchange) = scan_input.exchange {
				for (_, partitioned) in
					self.repartition_exchange_batch(&batch, exchange.partition_count, exchange.key)?
				{
					add_streaming_scan_build_partition_byte_estimates(
						&mut partition_bytes,
						&partitioned,
						retained_columns,
						partition_build_keys,
						write_build_keys,
						partition_count,
						null_safe_keys,
					)?;
				}
			} else {
				add_streaming_scan_build_partition_byte_estimates(
					&mut partition_bytes,
					&batch,
					retained_columns,
					partition_build_keys,
					write_build_keys,
					partition_count,
					null_safe_keys,
				)?;
			}
		}
		Ok(partition_bytes)
	}

	#[allow(clippy::too_many_arguments)]
	async fn choose_streaming_scan_inner_build_partitioning(
		&self,
		estimated_build_bytes: u64,
		scan_input: StreamingScanProbeInput<'_>,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(u32, u32, u64), ExecutionError> {
		let mut partition_count = streamed_inner_build_partition_count(
			estimated_build_bytes,
			self.context.task_budget.memory.get(),
			self.context.task_budget.spill_bytes.get(),
		)?;
		loop {
			let partition_bytes = self
				.estimate_streaming_scan_inner_build_partition_bytes(
					scan_input,
					retained_columns,
					partition_build_keys,
					write_build_keys,
					partition_count,
					null_safe_keys,
					morsel,
				)
				.await?;
			match streamed_inner_build_group_count_for_partition_spill_bytes(
				&partition_bytes,
				self.context.task_budget.spill_bytes.get(),
			) {
				Ok((group_count, max_group_spill)) => {
					return Ok((partition_count, group_count, max_group_spill));
				}
				Err(error) => {
					let Some(next_partition_count) =
						next_streamed_inner_build_partition_count(partition_count)
					else {
						return Err(error);
					};
					if next_partition_count == partition_count {
						return Err(error);
					}
					partition_count = next_partition_count;
				}
			}
		}
	}

	async fn materialize_streaming_scan_output_for_cap(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(u64, Vec<RecordBatch>), ExecutionError> {
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
		let mut estimated = 0u64;
		let mut batches = Vec::new();
		while let Some(batch) = stream.next().await {
			let Some(mut batch) = limit_streaming_scan_batch(batch?, &mut remaining) else {
				break;
			};
			if let Some(predicate) = scan_input.filter_predicate {
				batch =
					self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
			}
			estimated = estimated.saturating_add(batch.get_array_memory_size() as u64);
			batches.push(batch);
		}
		Ok((estimated, batches))
	}

	async fn reoriented_inner_stage_scan_build_decision(
		&self,
		stage: &ReorientedInnerJoinStage<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<ReorientedStageScanBuildDecision, ExecutionError> {
		let Some(scan_input) = decompose_streaming_scan_probe_input(stage.build) else {
			return Ok(ReorientedStageScanBuildDecision::Materialized);
		};
		let task_memory_cap = self.context.task_budget.memory.get();
		if task_memory_cap < 64 * 1024 * 1024 {
			return Ok(ReorientedStageScanBuildDecision::Materialized);
		}
		let scan_output_cap = join_output_materialization_memory_cap(task_memory_cap);
		let (estimated, batches) = self
			.materialize_streaming_scan_output_for_cap(scan_input, morsel)
			.await?;
		let estimated_build_hash_bytes =
			estimated_build_hash_bytes_for_batches(&batches, &stage.build_keys)?;
		Ok(
			reoriented_stage_scan_build_decision_for_materialized_batches(
				estimated,
				estimated_build_hash_bytes,
				scan_output_cap,
				batches,
			),
		)
	}

	async fn reoriented_inner_stage_scan_build_exceeds_scan_output_cap(
		&self,
		stage: &ReorientedInnerJoinStage<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<bool, ExecutionError> {
		Ok(matches!(
			self.reoriented_inner_stage_scan_build_decision(stage, morsel)
				.await?,
			ReorientedStageScanBuildDecision::Stream
		))
	}

	fn reoriented_terminal_scan_build_estimated_over_cap(
		&self,
		stage: &ReorientedInnerJoinStage<'_>,
		morsel: Option<(MorselId, u32)>,
	) -> bool {
		if morsel.is_some() || self.context.task_budget.memory.get() < 64 * 1024 * 1024 {
			return false;
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(stage.build) else {
			return false;
		};
		let ExecutionPlan::Scan {
			estimated_byte_size,
			..
		} = scan_input.scan
		else {
			return false;
		};
		estimated_byte_size
			.map(|bytes| {
				// This shortcut skips the measured scan-build classifier, so it only fires
				// when metadata says the terminal scan is catastrophically beyond the
				// materialization cap. At >=8x cap, the old grouped path needs many
				// scan passes before it can make progress, so one cap-guarded attempt to
				// build the other side is the safer first move. Moderate over-cap scans
				// keep the old measured path because projected metadata can be much
				// larger than the actual filtered/materialized build.
				bytes
					> join_output_materialization_memory_cap(self.context.task_budget.memory.get())
						.saturating_mul(8)
			})
			.unwrap_or(false)
	}

	async fn write_streaming_scan_inner_build_batch(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		batch: &RecordBatch,
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		key_plan: JoinKeyPlan,
		build_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		retain_unkeyed_rows: bool,
	) -> Result<(), ExecutionError> {
		if let Some(retained_columns) = retained_columns {
			if retain_unkeyed_rows {
				spilled
					.write_projected_preserved_batch_chunks(
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
					.write_projected_batch_chunks(
						batch,
						retained_columns,
						key_plan,
						build_keys,
						null_safe_keys,
						self.context.task_budget,
					)
					.await
			}
		} else {
			if retain_unkeyed_rows {
				spilled
					.write_preserved_batch_chunks(
						batch,
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
	async fn visit_partitioned_streaming_scan_probe_group_chunks<F>(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		scan_input: StreamingScanProbeInput<'_>,
		right_keys: &[crate::ir::ColumnOrdinal],
		partition_count: u32,
		partition_start: usize,
		partition_end: usize,
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
						.visit_partitioned_streaming_scan_probe_batch(
							partitioned_builds,
							&partitioned,
							right_keys,
							partition_count,
							partition_start,
							partition_end,
							key_plan,
							null_safe_keys,
							materialized_output_schema,
							output_schema,
							filter_predicate,
							probe_columns_first,
							build_column_count,
							visit_batch,
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
					.visit_partitioned_streaming_scan_probe_batch(
						partitioned_builds,
						&batch,
						right_keys,
						partition_count,
						partition_start,
						partition_end,
						key_plan,
						null_safe_keys,
						materialized_output_schema,
						output_schema,
						filter_predicate,
						probe_columns_first,
						build_column_count,
						visit_batch,
					)
					.await?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_partitioned_streaming_scan_probe_batch<F>(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		batch: &RecordBatch,
		right_keys: &[crate::ir::ColumnOrdinal],
		partition_count: u32,
		partition_start: usize,
		partition_end: usize,
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &arrow::datatypes::SchemaRef,
		output_schema: &arrow::datatypes::SchemaRef,
		filter_predicate: Option<&BoundExprProgram>,
		probe_columns_first: bool,
		build_column_count: usize,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let buckets = precompute_join_partition_buckets(
			batch,
			right_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		for (partition, partitioned) in
			partition_streaming_join_batch_with_buckets(batch, &buckets, partition_count)?
		{
			if partition < partition_start || partition >= partition_end {
				continue;
			}
			let Some(spilled) = partitioned_builds
				.get_mut(partition)
				.and_then(Option::as_mut)
			else {
				continue;
			};
			if spilled.input_row_count() == 0 || partitioned.num_rows() == 0 {
				continue;
			}
			let (partition_probe_rows, partition_output_rows) = self
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
					visit_batch,
				)
				.await?;
			probe_rows = probe_rows.saturating_add(partition_probe_rows);
			output_rows = output_rows.saturating_add(partition_output_rows);
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
		let _ = spilled
			.try_pin_decoded_pages(self.context.task_budget.memory.get())
			.await?;
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
		build_rows: &JoinBuildRows,
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
		if let Some((build_rows, probe_rows, output_rows)) = self
			.try_visit_partitioned_streamed_inner_materialized_build_probe_chunks(
				left.schema(),
				right.schema(),
				&left_batches,
				&right_batches,
				left_keys,
				right_keys,
				match_filter.as_ref(),
				*null_safe_keys,
				output_schema,
				filter_predicate,
				morsel,
				&mut visit_batch,
			)
			.await?
		{
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
			return Ok(Some(()));
		}
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

	pub(in crate::exec) async fn try_streaming_left_outer_grouped_aggregate(
		&self,
		input: &ExecutionPlan,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		output_schema: &arrow::datatypes::SchemaRef,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let Some(streaming_input) =
			decompose_streaming_left_outer_grouped_input(input, group_expr, aggr_expr)?
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
			strategy: JoinStrategy::HashPartitioned,
			output_schema: join_output_schema,
			..
		} = streaming_input.join
		else {
			return Ok(None);
		};
		let Some(scan_input) = decompose_streaming_scan_probe_input(right) else {
			return Ok(None);
		};
		if self.context.task_budget.memory.get() < 64 * 1024 * 1024 {
			return Ok(None);
		}

		if match_filter.is_none() {
			if let Some(left_scan_input) = decompose_streaming_scan_probe_input(left) {
				let retained_columns = retained_left_outer_grouped_columns(
					left.schema().fields().len(),
					left_keys,
					group_expr,
					aggr_expr,
					streaming_input.exchange,
				);
				if !retained_columns.is_empty() {
					let projected_left_keys = remap_column_ordinals(&retained_columns, left_keys)?;
					let projected_left_schema =
						projected_schema_for_columns(&left.schema(), &retained_columns)?;
					let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
						&projected_left_schema,
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
					self.write_streaming_scan_inner_build_batches(
						&mut spilled,
						left_scan_input,
						Some(&retained_columns),
						&projected_left_keys,
						key_plan,
						*null_safe_keys,
						true,
						None,
						morsel,
					)
					.await?;
					spilled.finish(self.context.task_budget).await?;
					let build_rows = spilled.input_row_count();
					record_task_telemetry(&self.context.task_telemetry, |task| {
						task.counters.build_rows =
							task.counters.build_rows.saturating_add(build_rows);
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

					let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
					let mut group_types = Vec::new();
					let mut folded_input = false;
					let mut matched_build_rows = spilled.empty_page_match_bitmap();
					let probe_started = Instant::now();
					let (probe_rows, mut output_rows) = self
						.visit_streaming_spilled_left_outer_scan_probe_chunks(
							&mut spilled,
							scan_input,
							left.schema(),
							&retained_columns,
							&mut matched_build_rows,
							key_plan,
							right_keys,
							*null_safe_keys,
							join_output_schema,
							streaming_input.exchange,
							group_expr,
							aggr_expr,
							&mut groups,
							&mut group_types,
							&mut folded_input,
							morsel,
						)
						.await?;
					output_rows = output_rows.saturating_add(
						self.fold_streaming_spilled_left_outer_unmatched_rows(
							&mut spilled,
							&matched_build_rows,
							left.schema(),
							&retained_columns,
							right.schema(),
							join_output_schema,
							streaming_input.exchange,
							group_expr,
							aggr_expr,
							&mut groups,
							&mut group_types,
							&mut folded_input,
						)
						.await?,
					);
					if !folded_input {
						self.fold_bound_grouped_aggregate_batch(
							&mut groups,
							&mut group_types,
							group_expr,
							aggr_expr,
							&RecordBatch::new_empty(join_output_schema.clone()),
						)?;
					}
					record_task_telemetry(&self.context.task_telemetry, |task| {
						task.phases.probe_micros = task
							.phases
							.probe_micros
							.saturating_add(duration_micros(probe_started));
						task.counters.probe_rows =
							task.counters.probe_rows.saturating_add(probe_rows);
						task.counters.matched_rows =
							task.counters.matched_rows.saturating_add(output_rows);
					});
					record_join_runtime_observation(
						&self.context.task_telemetry,
						&self.context.join_runtime_observation_mode,
						JoinRuntimeObservation {
							replay_id: *replay_id,
							strategy: JoinStrategy::HashPartitioned,
							build_rows: RowCount::new(build_rows),
							probe_rows: RowCount::new(probe_rows),
							output_rows: RowCount::new(output_rows),
						},
					);
					let output = self.finalize_bound_grouped_aggregate(
						&groups,
						&group_types,
						aggr_expr,
						output_schema,
					);
					spilled.cleanup().await;
					return output.map(Some);
				}
			}
		}

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

		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();
		let mut folded_input = false;
		let mut matched_build_rows = build_match_bitmap(JoinKind::LeftOuter, left_batch.num_rows())
			.ok_or_else(|| ExecutionError::Kernel {
				message: "left outer streaming expected a build match bitmap".to_string(),
			})?;

		let probe_started = Instant::now();
		let (probe_rows, mut output_rows) = self
			.visit_streaming_left_outer_scan_probe_chunks(
				scan_input,
				&left_batch,
				&build_rows,
				&mut matched_build_rows,
				join_key_plan,
				right_keys,
				match_filter.as_ref(),
				*null_safe_keys,
				join_output_schema,
				streaming_input.exchange,
				group_expr,
				aggr_expr,
				&mut groups,
				&mut group_types,
				&mut folded_input,
				morsel,
			)
			.await?;
		output_rows = output_rows.saturating_add(self.fold_streaming_left_outer_unmatched_rows(
			&left_batch,
			&matched_build_rows,
			right.schema(),
			join_output_schema,
			streaming_input.exchange,
			group_expr,
			aggr_expr,
			&mut groups,
			&mut group_types,
			&mut folded_input,
		)?);
		if !folded_input {
			self.fold_bound_grouped_aggregate_batch(
				&mut groups,
				&mut group_types,
				group_expr,
				aggr_expr,
				&RecordBatch::new_empty(join_output_schema.clone()),
			)?;
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
				strategy: JoinStrategy::HashPartitioned,
				build_rows: RowCount::new(left_batch.num_rows() as u64),
				probe_rows: RowCount::new(probe_rows),
				output_rows: RowCount::new(output_rows),
			},
		);

		self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema)
			.map(Some)
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_spilled_left_outer_scan_probe_chunks(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		scan_input: StreamingScanProbeInput<'_>,
		full_left_schema: SchemaRef,
		retained_columns: &[ColumnOrdinal],
		matched_build_rows: &mut [Vec<u64>],
		join_key_plan: JoinKeyPlan,
		right_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(u64, u64), ExecutionError> {
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Err(ExecutionError::InvalidExpression {
				message: "streaming spilled left outer probe expected a scan input".to_string(),
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
			if let Some(scan_exchange) = scan_input.exchange {
				for (_partition, partitioned) in self.repartition_exchange_batch(
					&right_batch,
					scan_exchange.partition_count,
					scan_exchange.key,
				)? {
					let (batch_probe_rows, batch_output_rows) = self
						.visit_streaming_spilled_left_outer_probe_batch(
							spilled,
							&partitioned,
							&full_left_schema,
							retained_columns,
							matched_build_rows,
							join_key_plan,
							right_keys,
							null_safe_keys,
							join_output_schema,
							exchange,
							group_expr,
							aggr_expr,
							groups,
							group_types,
							folded_input,
						)
						.await?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			} else {
				let (batch_probe_rows, batch_output_rows) = self
					.visit_streaming_spilled_left_outer_probe_batch(
						spilled,
						&right_batch,
						&full_left_schema,
						retained_columns,
						matched_build_rows,
						join_key_plan,
						right_keys,
						null_safe_keys,
						join_output_schema,
						exchange,
						group_expr,
						aggr_expr,
						groups,
						group_types,
						folded_input,
					)
					.await?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_spilled_left_outer_probe_batch(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		right_batch: &RecordBatch,
		full_left_schema: &SchemaRef,
		retained_columns: &[ColumnOrdinal],
		matched_build_rows: &mut [Vec<u64>],
		join_key_plan: JoinKeyPlan,
		right_keys: &[crate::ir::ColumnOrdinal],
		null_safe_keys: bool,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(u64, u64), ExecutionError> {
		let mut output_rows = 0u64;
		for offset in (0..right_batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(right_batch.num_rows() - offset);
			let probe_chunk = right_batch.slice(offset, len);
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			let mut matches = Vec::new();
			for row in 0..probe_chunk.num_rows() {
				let Some(key) = join_key_at(join_key_plan, &right_key_arrays, row, null_safe_keys)?
				else {
					continue;
				};
				for build in spilled.lookup(&key).await? {
					spilled.mark_page_row_matched(matched_build_rows, build);
					matches.push(SpilledInnerJoinMatch {
						build,
						probe: join_probe_row_index(row)?,
					});
				}
			}
			output_rows = output_rows.saturating_add(matches.len() as u64);
			self.fold_projected_spilled_left_outer_join_selection_chunks(
				spilled,
				&probe_chunk,
				&matches,
				full_left_schema,
				retained_columns,
				join_output_schema,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)
			.await?;
		}
		Ok((right_batch.num_rows() as u64, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn fold_streaming_spilled_left_outer_unmatched_rows(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		matched_build_rows: &[Vec<u64>],
		full_left_schema: SchemaRef,
		retained_columns: &[ColumnOrdinal],
		right_schema: arrow::datatypes::SchemaRef,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<u64, ExecutionError> {
		let mut unmatched = Vec::new();
		let mut output_rows = 0u64;
		for page_index in 0..spilled.page_count() {
			let rows = spilled.page_row_count(page_index)?;
			for row in 0..rows {
				if spilled.page_row_matched(matched_build_rows, page_index, row) {
					continue;
				}
				unmatched.push(SpilledBatchRowRef {
					page_index,
					row: join_build_row_index(row)?,
				});
				if unmatched.len() == CROSS_JOIN_OUTPUT_CHUNK_ROWS {
					output_rows = output_rows.saturating_add(unmatched.len() as u64);
					let batch = materialize_projected_streamed_spilled_left_outer_unmatched(
						spilled,
						&unmatched,
						&full_left_schema,
						retained_columns,
						&right_schema,
						join_output_schema,
						self.context.task_budget.memory.get(),
					)
					.await?;
					self.fold_left_outer_grouped_batch(
						batch,
						exchange,
						group_expr,
						aggr_expr,
						groups,
						group_types,
						folded_input,
					)?;
					unmatched.clear();
				}
			}
		}
		if !unmatched.is_empty() {
			output_rows = output_rows.saturating_add(unmatched.len() as u64);
			let batch = materialize_projected_streamed_spilled_left_outer_unmatched(
				spilled,
				&unmatched,
				&full_left_schema,
				retained_columns,
				&right_schema,
				join_output_schema,
				self.context.task_budget.memory.get(),
			)
			.await?;
			self.fold_left_outer_grouped_batch(
				batch,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
		}
		Ok(output_rows)
	}

	#[allow(clippy::too_many_arguments)]
	async fn fold_projected_spilled_left_outer_join_selection_chunks(
		&self,
		spilled: &mut SpilledInnerJoinBuild,
		right_batch: &RecordBatch,
		matches: &[SpilledInnerJoinMatch],
		full_left_schema: &SchemaRef,
		retained_columns: &[ColumnOrdinal],
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(), ExecutionError> {
		for chunk in matches.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_projected_streamed_spilled_left_outer_matches(
				spilled,
				right_batch,
				chunk,
				full_left_schema,
				retained_columns,
				join_output_schema,
				self.context.task_budget.memory.get(),
			)
			.await?;
			self.fold_left_outer_grouped_batch(
				batch,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn visit_streaming_left_outer_scan_probe_chunks(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		left_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		matched_build_rows: &mut [u64],
		join_key_plan: JoinKeyPlan,
		right_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
		morsel: Option<(MorselId, u32)>,
	) -> Result<(u64, u64), ExecutionError> {
		let Some(req) = self.bound_scan_request(scan_input.scan, morsel) else {
			return Err(ExecutionError::InvalidExpression {
				message: "streaming left outer probe expected a scan input".to_string(),
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
			if let Some(scan_exchange) = scan_input.exchange {
				for (_partition, partitioned) in self.repartition_exchange_batch(
					&right_batch,
					scan_exchange.partition_count,
					scan_exchange.key,
				)? {
					let (batch_probe_rows, batch_output_rows) = self
						.visit_streaming_left_outer_probe_batch(
							left_batch,
							&partitioned,
							build_rows,
							matched_build_rows,
							join_key_plan,
							right_keys,
							match_filter,
							null_safe_keys,
							join_output_schema,
							exchange,
							group_expr,
							aggr_expr,
							groups,
							group_types,
							folded_input,
						)?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			} else {
				let (batch_probe_rows, batch_output_rows) = self
					.visit_streaming_left_outer_probe_batch(
						left_batch,
						&right_batch,
						build_rows,
						matched_build_rows,
						join_key_plan,
						right_keys,
						match_filter,
						null_safe_keys,
						join_output_schema,
						exchange,
						group_expr,
						aggr_expr,
						groups,
						group_types,
						folded_input,
					)?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	fn visit_streaming_left_outer_probe_batch(
		&self,
		left_batch: &RecordBatch,
		right_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		matched_build_rows: &mut [u64],
		join_key_plan: JoinKeyPlan,
		right_keys: &[crate::ir::ColumnOrdinal],
		match_filter: Option<&BoundExprProgram>,
		null_safe_keys: bool,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(u64, u64), ExecutionError> {
		let mut output_rows = 0u64;
		for offset in (0..right_batch.num_rows()).step_by(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let len = CROSS_JOIN_OUTPUT_CHUNK_ROWS.min(right_batch.num_rows() - offset);
			let probe_chunk = right_batch.slice(offset, len);
			let right_key_arrays = key_arrays(&probe_chunk, right_keys)?;
			let mut selection = Vec::new();
			let mut residual = JoinMatchResidual::new(match_filter, left_batch, &probe_chunk);
			match join_key_plan {
				JoinKeyPlan::Int32 => {
					let values = single_join_key_array::<Int32Array>(&right_key_arrays, "Int32")?;
					for row in 0..probe_chunk.num_rows() {
						if values.is_null(row) {
							continue;
						}
						if let Some(matches) = build_rows.get_int32(values.value(row)) {
							append_join_matches_for_kind(
								JoinKind::LeftOuter,
								row,
								matches.rows(),
								Some(matched_build_rows),
								&mut selection,
								&mut residual,
							)?;
						}
					}
				}
				JoinKeyPlan::Int64 => {
					let values = single_join_key_array::<Int64Array>(&right_key_arrays, "Int64")?;
					for row in 0..probe_chunk.num_rows() {
						if values.is_null(row) {
							continue;
						}
						if let Some(matches) = build_rows.get_int64(values.value(row)) {
							append_join_matches_for_kind(
								JoinKind::LeftOuter,
								row,
								matches.rows(),
								Some(matched_build_rows),
								&mut selection,
								&mut residual,
							)?;
						}
					}
				}
				JoinKeyPlan::Generic => {
					for row in 0..probe_chunk.num_rows() {
						let key = if null_safe_keys {
							join_key_bytes_null_safe(&right_key_arrays, row)?
						} else {
							let Some(key) = join_key_bytes(&right_key_arrays, row)? else {
								continue;
							};
							key
						};
						if let Some(matches) = build_rows.get(&JoinKey::Generic(key)) {
							append_join_matches_for_kind(
								JoinKind::LeftOuter,
								row,
								matches.rows(),
								Some(matched_build_rows),
								&mut selection,
								&mut residual,
							)?;
						}
					}
				}
			}
			output_rows = output_rows.saturating_add(selection.len() as u64);
			self.fold_left_outer_join_selection_chunks(
				left_batch,
				&probe_chunk,
				&selection,
				join_output_schema,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
		}
		Ok((right_batch.num_rows() as u64, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	fn fold_streaming_left_outer_unmatched_rows(
		&self,
		left_batch: &RecordBatch,
		matched_build_rows: &[u64],
		right_schema: arrow::datatypes::SchemaRef,
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<u64, ExecutionError> {
		let empty_right = RecordBatch::new_empty(right_schema);
		let mut selection = Vec::new();
		let mut output_rows = 0u64;
		for row in 0..left_batch.num_rows() {
			if build_row_matched(matched_build_rows, row) {
				continue;
			}
			selection.push(JoinOutputRow::LeftFillNull {
				left: join_build_row_index(row)?,
			});
			if selection.len() == CROSS_JOIN_OUTPUT_CHUNK_ROWS {
				output_rows = output_rows.saturating_add(selection.len() as u64);
				self.fold_left_outer_join_selection_chunks(
					left_batch,
					&empty_right,
					&selection,
					join_output_schema,
					exchange,
					group_expr,
					aggr_expr,
					groups,
					group_types,
					folded_input,
				)?;
				selection.clear();
			}
		}
		if !selection.is_empty() {
			output_rows = output_rows.saturating_add(selection.len() as u64);
			self.fold_left_outer_join_selection_chunks(
				left_batch,
				&empty_right,
				&selection,
				join_output_schema,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
		}
		Ok(output_rows)
	}

	#[allow(clippy::too_many_arguments)]
	fn fold_left_outer_join_selection_chunks(
		&self,
		left_batch: &RecordBatch,
		right_batch: &RecordBatch,
		selection: &[JoinOutputRow],
		join_output_schema: &arrow::datatypes::SchemaRef,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(), ExecutionError> {
		for chunk in selection.chunks(CROSS_JOIN_OUTPUT_CHUNK_ROWS) {
			let batch = materialize_join_selection(
				JoinKind::LeftOuter,
				left_batch,
				right_batch,
				chunk,
				join_output_schema,
				self.context.task_budget.memory.get(),
			)?;
			self.fold_left_outer_grouped_batch(
				batch,
				exchange,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				folded_input,
			)?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn fold_left_outer_grouped_batch(
		&self,
		batch: RecordBatch,
		exchange: Option<(u32, &BoundExprProgram)>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		folded_input: &mut bool,
	) -> Result<(), ExecutionError> {
		if batch.num_rows() == 0 {
			if !*folded_input {
				self.fold_bound_grouped_aggregate_batch(
					groups,
					group_types,
					group_expr,
					aggr_expr,
					&batch,
				)?;
				*folded_input = true;
			}
			return Ok(());
		}
		if let Some((partition_count, exchange_key)) = exchange {
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
				*folded_input = true;
			}
		} else {
			self.fold_bound_grouped_aggregate_batch(
				groups,
				group_types,
				group_expr,
				aggr_expr,
				&batch,
			)?;
			*folded_input = true;
		}
		Ok(())
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
	async fn try_streaming_scan_left_semi_grouped_aggregate(
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
		let ExecutionPlan::Filter {
			input: semi_input,
			predicate: post_filter,
			disposition: FilterDisposition::EvaluateInToron,
		} = aggregate_input.as_ref()
		else {
			return Ok(None);
		};
		let semi_input = semi_input.as_ref();
		let ExecutionPlan::Join {
			replay_id,
			left,
			right,
			kind: JoinKind::LeftSemi,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys: false,
			strategy,
			output_schema: semi_output_schema,
			..
		} = semi_input
		else {
			return Ok(None);
		};
		let Some(scan_input) = decompose_streaming_scan_probe_input(left) else {
			return Ok(None);
		};
		if decompose_streaming_existence_plan(right).is_none() {
			return Ok(None);
		}

		let stage = StreamingSemiAntiStage {
			replay_id: *replay_id,
			kind: JoinKind::LeftSemi,
			preserved_keys: left_keys.clone(),
			existence: right.as_ref(),
			existence_keys: right_keys.clone(),
			match_filter: match_filter.as_ref(),
			output_schema: semi_output_schema.clone(),
			strategy: *strategy,
		};
		let Some(mut runtime) = self
			.prepare_streaming_semi_anti_runtime(stage, morsel)
			.await?
		else {
			return Ok(None);
		};

		let output = async {
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
			let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
			let mut group_types = Vec::new();
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
				batch =
					self.filter_batch_with_bound_predicate(post_filter, batch, "filter predicate")?;
				if batch.num_rows() == 0 {
					continue;
				}
				let mut semi_batches = self.probe_streaming_semi_anti(&mut runtime, &batch).await?;
				for batch in semi_batches.drain(..) {
					if batch.num_rows() == 0 {
						continue;
					}
					for (_partition, partitioned) in
						self.repartition_exchange_batch(&batch, *partition_count, exchange_key)?
					{
						if partitioned.num_rows() == 0 {
							continue;
						}
						self.fold_bound_grouped_aggregate_batch(
							&mut groups,
							&mut group_types,
							group_expr,
							aggr_expr,
							&partitioned,
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
					build_rows: RowCount::new(runtime.existence_row_count),
					probe_rows: RowCount::new(runtime.probe_rows),
					output_rows: RowCount::new(runtime.output_rows),
				},
			);
			self.finalize_bound_grouped_aggregate(&groups, &group_types, aggr_expr, output_schema)
				.map(Some)
		}
		.await;
		runtime.cleanup().await;
		output
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
		if let Some(output) = self
			.try_streaming_scan_left_semi_grouped_aggregate(
				input,
				group_expr,
				aggr_expr,
				output_schema,
				morsel,
			)
			.await?
		{
			return Ok(Some(output));
		}
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
		let (aggregate_input, aggregate_input_wrapped) = match input {
			ExecutionPlan::Exchange { input, .. } => (input.as_ref(), true),
			other => (other, false),
		};
		let (project_input, project_exprs, project_output_schema) = match aggregate_input {
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => (input.as_ref(), Some(exprs), Some(output_schema)),
			other => (other, None, None),
		};
		let (chain_input, outer_predicate) = match project_input {
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
		let required_output = reoriented_grouped_required_output_columns(
			project_exprs,
			group_expr,
			aggr_expr,
			outer_predicate,
		);
		mark_reoriented_inner_stage_projections(&mut stages, required_output)?;

		let base_scan_probe_mode = reoriented_inner_chain_grouped_base_scan_probe_mode(base);
		let force_stream_projected_filtered_base = project_exprs.is_some()
			&& matches!(
				base_scan_probe_mode,
				Some(ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0)
			);
		let stage_count = stages.len();
		let mut runtimes = Vec::with_capacity(stage_count);
		let mut deferred_stream_stage = None;
		for (stage_index, stage) in stages.into_iter().enumerate() {
			if stage_index + 1 == stage_count
				&& self.reoriented_terminal_scan_build_estimated_over_cap(&stage, morsel)
			{
				deferred_stream_stage = Some((stage, None, true));
				break;
			}
			let scan_build_decision = if force_stream_projected_filtered_base
				&& matches!(
					base_scan_probe_mode,
					Some(ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0)
				) {
				ReorientedStageScanBuildDecision::Stream
			} else if aggregate_input_wrapped {
				if self
					.reoriented_inner_stage_scan_build_exceeds_scan_output_cap(&stage, morsel)
					.await?
				{
					ReorientedStageScanBuildDecision::Stream
				} else {
					ReorientedStageScanBuildDecision::Materialized
				}
			} else {
				self.reoriented_inner_stage_scan_build_decision(&stage, morsel)
					.await?
			};
			let scan_build_decision = if stage_index + 1 == stage_count {
				match scan_build_decision {
					ReorientedStageScanBuildDecision::Stream => {
						deferred_stream_stage = Some((stage, None, false));
						break;
					}
					ReorientedStageScanBuildDecision::StreamMaterializedBatches(batches) => {
						deferred_stream_stage = Some((stage, Some(batches), false));
						break;
					}
					other => other,
				}
			} else {
				scan_build_decision
			};
			let runtime = match scan_build_decision {
				ReorientedStageScanBuildDecision::Stream
				| ReorientedStageScanBuildDecision::StreamMaterializedBatches(_) => {
					self.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
						stage, morsel,
					)
					.await?
				}
				ReorientedStageScanBuildDecision::Materialized => {
					self.prepare_reoriented_inner_join_runtime(stage, morsel)
						.await?
				}
				ReorientedStageScanBuildDecision::MaterializedBatches(build_batches) => {
					self.prepare_reoriented_inner_join_runtime_from_build_batches(
						stage,
						build_batches,
					)
					.await?
				}
			};
			let Some(runtime) = runtime else {
				return Ok(None);
			};
			runtimes.push(runtime);
		}

		let mut groups: BTreeMap<Vec<GroupValue>, Vec<AggState>> = BTreeMap::new();
		let mut group_types = Vec::new();
		let mut matched = None;
		if let Some((stage, scan_probe_batches, needs_decision_fallback)) = deferred_stream_stage {
			if let Some(candidate) =
				self.reoriented_adaptive_partitioned_scan_candidate(&stage, morsel)?
			{
				runtimes.push(candidate);
				matched = self
					.try_streaming_reoriented_inner_chain_adaptive_last_stage_grouped(
						base,
						&mut runtimes,
						outer_predicate,
						project_exprs,
						project_output_schema,
						group_expr,
						aggr_expr,
						&mut groups,
						&mut group_types,
						morsel,
						scan_probe_batches,
					)
					.await?;
				if matched.is_none() {
					runtimes.pop();
				}
			}
			if matched.is_none() {
				let runtime = if needs_decision_fallback {
					let scan_build_decision = if force_stream_projected_filtered_base
						&& matches!(
							base_scan_probe_mode,
							Some(ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0)
						) {
						ReorientedStageScanBuildDecision::Stream
					} else if aggregate_input_wrapped {
						if self
							.reoriented_inner_stage_scan_build_exceeds_scan_output_cap(
								&stage, morsel,
							)
							.await?
						{
							ReorientedStageScanBuildDecision::Stream
						} else {
							ReorientedStageScanBuildDecision::Materialized
						}
					} else {
						self.reoriented_inner_stage_scan_build_decision(&stage, morsel)
							.await?
					};
					match scan_build_decision {
						ReorientedStageScanBuildDecision::Stream
						| ReorientedStageScanBuildDecision::StreamMaterializedBatches(_) => {
							self.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
								stage, morsel,
							)
							.await?
						}
						ReorientedStageScanBuildDecision::Materialized => {
							self.prepare_reoriented_inner_join_runtime(stage, morsel)
								.await?
						}
						ReorientedStageScanBuildDecision::MaterializedBatches(build_batches) => {
							self.prepare_reoriented_inner_join_runtime_from_build_batches(
								stage,
								build_batches,
							)
							.await?
						}
					}
				} else {
					self.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(
						stage, morsel,
					)
					.await?
				};
				let Some(runtime) = runtime else {
					for runtime in runtimes {
						runtime.cleanup().await;
					}
					return Ok(None);
				};
				runtimes.push(runtime);
			}
		}
		if matched.is_none() {
			matched = self
				.try_streaming_reoriented_inner_chain_scan_probe_base_grouped(
					base,
					&mut runtimes,
					outer_predicate,
					project_exprs,
					project_output_schema,
					group_expr,
					aggr_expr,
					&mut groups,
					&mut group_types,
					morsel,
				)
				.await?;
		}
		if matched.is_none() {
			matched = self
				.try_streaming_reoriented_inner_chain_generic_base_grouped(
					base,
					&mut runtimes,
					outer_predicate,
					project_exprs,
					project_output_schema,
					group_expr,
					aggr_expr,
					&mut groups,
					&mut group_types,
					morsel,
				)
				.await?;
		}
		if matched.is_none() {
			for runtime in runtimes {
				runtime.cleanup().await;
			}
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

	fn reoriented_adaptive_partitioned_scan_candidate<'a>(
		&self,
		stage: &ReorientedInnerJoinStage<'a>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		let Some(scan_input) = decompose_streaming_scan_probe_input(stage.build) else {
			return Ok(None);
		};
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&stage.build.schema(),
			&stage.build_keys,
			&stage.probe_schema,
			&stage.probe_keys,
			false,
		)?;
		let materialized_output_schema = if stage.probe_columns_first {
			concat_join_schema(&stage.build.schema(), &stage.probe_schema)
		} else {
			stage.output_schema.clone()
		};
		Ok(Some(ReorientedInnerJoinRuntime {
			replay_id: stage.replay_id,
			strategy: stage.strategy,
			build: ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel,
				partition_count: 1,
				group_count: 1,
				partition_build_keys: stage.build_keys.clone(),
				write_build_keys: stage.build_keys.clone(),
				materialized_output_schema,
				build_column_count: stage.build.schema().fields().len(),
				projection: None,
				estimated_build_bytes: 0,
				observed_build_rows: 0,
				loaded_builds: vec![None],
				loaded_resident_bytes: 0,
				loaded_build_attempted: false,
			},
			key_plan,
			probe_keys: stage.probe_keys.clone(),
			output_schema: stage.output_schema.clone(),
			post_filter: stage.post_filter,
			probe_columns_first: stage.probe_columns_first,
			probe_rows: 0,
			output_rows: 0,
		}))
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_reoriented_inner_chain_adaptive_last_stage_grouped(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
		scan_probe_batches: Option<Vec<RecordBatch>>,
	) -> Result<Option<()>, ExecutionError> {
		// The first adaptive cut only handles a terminal over-cap scan-build stage. If
		// later stages still need async probing, buffering the flipped output would
		// cross the output-order/materialization boundary this cut deliberately avoids.
		let Some((runtime, prefix)) = inner_runtimes.split_last_mut() else {
			return Ok(None);
		};
		if !prefix.is_empty() {
			return Ok(None);
		}
		let mut visit_output = |batch: RecordBatch| -> Result<(), ExecutionError> {
			self.fold_reoriented_grouped_output_batch(
				groups,
				group_types,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				batch,
			)?;
			Ok(())
		};
		self.try_visit_reoriented_adaptive_derived_last_stage(
			base,
			runtime,
			morsel,
			scan_probe_batches,
			&mut visit_output,
		)
		.await
	}

	async fn try_visit_reoriented_adaptive_derived_last_stage<F>(
		&self,
		base: &ExecutionPlan,
		runtime: &mut ReorientedInnerJoinRuntime<'_>,
		morsel: Option<(MorselId, u32)>,
		scan_probe_batches: Option<Vec<RecordBatch>>,
		visit_batch: &mut F,
	) -> Result<Option<()>, ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let (
			scan_input,
			scan_probe_keys,
			runtime_probe_keys,
			output_schema,
			post_filter,
			original_probe_columns_first,
		) = match &runtime.build {
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				partition_build_keys,
				..
			} => (
				*scan_input,
				partition_build_keys.clone(),
				runtime.probe_keys.clone(),
				runtime.output_schema.clone(),
				runtime.post_filter,
				runtime.probe_columns_first,
			),
			_ => return Ok(None),
		};
		let Some(derived_batches) = self
			.materialize_reoriented_inner_chain_base_under_cap(base, morsel)
			.await?
		else {
			return Ok(None);
		};
		if derived_batches.is_empty() {
			return Ok(Some(()));
		}
		let derived_schema = derived_batches[0].schema();
		let derived_batch = concat_or_empty(derived_schema.clone(), &derived_batches)?;
		let derived_key_arrays = key_arrays(&derived_batch, &runtime_probe_keys)?;
		let build_memory_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let estimated_build_bytes =
			estimated_build_hash_bytes(&derived_key_arrays, derived_batch.num_rows());
		if estimated_build_bytes > build_memory_cap {
			return Ok(None);
		}
		let scan_schema = scan_input.scan.schema();
		let join_key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&derived_schema,
			&runtime_probe_keys,
			&scan_schema,
			&scan_probe_keys,
			false,
		)?;
		let build_started = Instant::now();
		let Some(build_rows) = build_in_memory_join_rows(
			join_key_plan,
			&derived_key_arrays,
			derived_batch.num_rows(),
			build_memory_cap,
			false,
		)?
		else {
			return Ok(None);
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(derived_batch.num_rows() as u64);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
		});
		if let ReorientedInnerJoinBuild::PartitionedScan {
			observed_build_rows,
			..
		} = &mut runtime.build
		{
			*observed_build_rows = derived_batch.num_rows() as u64;
		}

		let probe_started = Instant::now();
		let mut visit_output = |batch: RecordBatch| -> Result<(), ExecutionError> {
			let batch = if let Some(predicate) = post_filter {
				self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
			} else {
				batch
			};
			if batch.num_rows() > 0 {
				visit_batch(batch)?;
			}
			Ok(())
		};
		let (probe_rows, output_rows) = if let Some(scan_probe_batches) = scan_probe_batches {
			self.visit_reoriented_adaptive_derived_probe_batches(
				&derived_batch,
				&build_rows,
				join_key_plan,
				scan_input,
				&scan_probe_keys,
				&output_schema,
				!original_probe_columns_first,
				&mut visit_output,
				scan_probe_batches,
			)?
		} else {
			self.visit_streaming_in_memory_inner_scan_probe_chunks(
				&derived_batch,
				&build_rows,
				join_key_plan,
				scan_input,
				&scan_probe_keys,
				None,
				false,
				&output_schema,
				None,
				None,
				!original_probe_columns_first,
				morsel,
				&mut visit_output,
			)
			.await?
		};
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.probe_micros = task
				.phases
				.probe_micros
				.saturating_add(duration_micros(probe_started));
		});
		self.record_reoriented_inner_probe(runtime, probe_rows, output_rows);
		Ok(Some(()))
	}

	#[allow(clippy::too_many_arguments)]
	fn visit_reoriented_adaptive_derived_probe_batches<F>(
		&self,
		build_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		join_key_plan: JoinKeyPlan,
		scan_input: StreamingScanProbeInput<'_>,
		probe_keys: &[crate::ir::ColumnOrdinal],
		output_schema: &arrow::datatypes::SchemaRef,
		probe_columns_first: bool,
		visit_batch: &mut F,
		scan_probe_batches: Vec<RecordBatch>,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		let mut probe_rows = 0u64;
		let mut output_rows = 0u64;
		for probe_batch in scan_probe_batches {
			if let Some(exchange) = scan_input.exchange {
				for (_, partitioned) in self.repartition_exchange_batch(
					&probe_batch,
					exchange.partition_count,
					exchange.key,
				)? {
					let (batch_probe_rows, batch_output_rows) = self
						.visit_reoriented_adaptive_derived_probe_batch(
							build_batch,
							build_rows,
							join_key_plan,
							&partitioned,
							probe_keys,
							output_schema,
							probe_columns_first,
							&mut *visit_batch,
						)?;
					probe_rows = probe_rows.saturating_add(batch_probe_rows);
					output_rows = output_rows.saturating_add(batch_output_rows);
				}
			} else {
				let (batch_probe_rows, batch_output_rows) = self
					.visit_reoriented_adaptive_derived_probe_batch(
						build_batch,
						build_rows,
						join_key_plan,
						&probe_batch,
						probe_keys,
						output_schema,
						probe_columns_first,
						&mut *visit_batch,
					)?;
				probe_rows = probe_rows.saturating_add(batch_probe_rows);
				output_rows = output_rows.saturating_add(batch_output_rows);
			}
		}
		Ok((probe_rows, output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	fn visit_reoriented_adaptive_derived_probe_batch<F>(
		&self,
		build_batch: &RecordBatch,
		build_rows: &JoinBuildRows,
		join_key_plan: JoinKeyPlan,
		probe_batch: &RecordBatch,
		probe_keys: &[crate::ir::ColumnOrdinal],
		output_schema: &arrow::datatypes::SchemaRef,
		probe_columns_first: bool,
		visit_batch: &mut F,
	) -> Result<(u64, u64), ExecutionError>
	where
		F: FnMut(RecordBatch) -> Result<(), ExecutionError>,
	{
		if probe_batch.num_rows() == 0 {
			return Ok((0, 0));
		}
		let probe_key_arrays = key_arrays(probe_batch, probe_keys)?;
		let selection = probe_in_memory_join_rows(
			JoinKind::Inner,
			join_key_plan,
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
		self.visit_reoriented_inner_selection_filtered_chunks(
			build_batch,
			probe_batch,
			&selection,
			output_schema,
			None,
			probe_columns_first,
			visit_batch,
		)?;
		Ok((probe_batch.num_rows() as u64, output_rows))
	}

	async fn materialize_reoriented_inner_chain_base_under_cap(
		&self,
		base: &ExecutionPlan,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<Vec<RecordBatch>>, ExecutionError> {
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let mut accumulated_bytes = 0u64;
		let mut batches = Vec::new();
		let matched = self
			.try_visit_streaming_reoriented_inner_chain_base(base, morsel, |batch| {
				accumulated_bytes =
					accumulated_bytes.saturating_add(record_batch_memory_bytes(&batch));
				if accumulated_bytes > accumulator_cap {
					return Err(budget_exhausted(
						BudgetDimension::Memory,
						"adaptive derived-side build accumulator",
						accumulated_bytes,
						accumulator_cap,
					));
				}
				if batch.num_rows() > 0 {
					batches.push(batch);
				}
				Ok(())
			})
			.await;
		match matched {
			Ok(Some(())) => Ok(Some(batches)),
			Ok(None) => Ok(None),
			Err(ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				..
			}) => Ok(None),
			Err(error) => Err(error),
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_reoriented_inner_chain_scan_probe_base_grouped(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
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
		let (join0, join0_filter) = match base_input.as_ref() {
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition: FilterDisposition::EvaluateInToron,
			} => (input.as_ref(), Some(predicate)),
			other => (other, None),
		};
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
		} = join0
		else {
			return Ok(None);
		};
		if *replay_id != JoinReplayId(0) {
			return Ok(None);
		}
		let Some(scan_input) = decompose_streaming_scan_probe_input(right.as_ref()) else {
			return Ok(None);
		};

		let left_batches = self.execute_bound(left.as_ref(), morsel).await?;
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
				let batch = if let Some(join0_filter) = join0_filter {
					self.filter_batch_with_bound_predicate(join0_filter, batch, "filter predicate")?
				} else {
					batch
				};
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

					for batch in batches {
						self.fold_reoriented_grouped_output_batch(
							groups,
							group_types,
							outer_predicate,
							project_exprs,
							project_output_schema,
							group_expr,
							aggr_expr,
							batch,
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

	#[allow(clippy::too_many_arguments)]
	async fn try_streaming_reoriented_inner_chain_generic_base_grouped(
		&self,
		base: &ExecutionPlan,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Option<()>, ExecutionError> {
		let mut base_batches = Vec::new();
		let mut accumulated_bytes = 0u64;
		let accumulator_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let matched = self
			.try_visit_streaming_reoriented_inner_chain_base(base, morsel, |base_batch| {
				accumulated_bytes =
					accumulated_bytes.saturating_add(record_batch_memory_bytes(&base_batch));
				if accumulated_bytes > accumulator_cap {
					return Err(budget_exhausted(
						BudgetDimension::Memory,
						"reoriented inner chain base accumulator",
						accumulated_bytes,
						accumulator_cap,
					));
				}
				if base_batch.num_rows() > 0 {
					base_batches.push(base_batch);
				}
				Ok(())
			})
			.await?;
		if matched.is_none() {
			return Ok(None);
		}
		if self
			.try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir(
				&base_batches,
				inner_runtimes,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				groups,
				group_types,
				morsel,
			)
			.await?
		{
			return Ok(Some(()));
		}
		for base_batch in base_batches {
			self.process_reoriented_inner_chain_grouped_base_batch(
				base_batch,
				inner_runtimes,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				groups,
				group_types,
			)
			.await?;
		}
		Ok(Some(()))
	}

	#[allow(clippy::too_many_arguments)]
	async fn process_reoriented_inner_chain_grouped_base_batch(
		&self,
		base_batch: RecordBatch,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
	) -> Result<(), ExecutionError> {
		self.process_reoriented_inner_chain_grouped_suffix_batch(
			vec![base_batch],
			inner_runtimes,
			outer_predicate,
			project_exprs,
			project_output_schema,
			group_expr,
			aggr_expr,
			groups,
			group_types,
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	async fn process_reoriented_inner_chain_grouped_suffix_batch(
		&self,
		mut batches: Vec<RecordBatch>,
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
	) -> Result<(), ExecutionError> {
		for runtime in inner_runtimes.iter_mut() {
			let mut next = Vec::new();
			for batch in batches {
				next.extend(
					self.probe_reoriented_inner_join_streaming(runtime, &batch)
						.await?,
				);
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
			self.fold_reoriented_grouped_output_batch(
				groups,
				group_types,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				batch,
			)?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir(
		&self,
		base_batches: &[RecordBatch],
		inner_runtimes: &mut [ReorientedInnerJoinRuntime<'_>],
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		morsel: Option<(MorselId, u32)>,
	) -> Result<bool, ExecutionError> {
		let Some((runtime, suffix_runtimes)) = inner_runtimes.split_first_mut() else {
			return Ok(false);
		};
		let (
			scan_input,
			build_morsel,
			partition_count,
			group_count,
			partition_build_keys,
			write_build_keys,
			key_plan,
			null_safe_keys,
			materialized_output_schema,
			output_schema,
			probe_columns_first,
			build_column_count,
			projection,
			probe_keys,
			post_filter,
		) = match &runtime.build {
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel: build_morsel,
				partition_count,
				group_count,
				partition_build_keys,
				write_build_keys,
				materialized_output_schema,
				build_column_count,
				projection,
				..
			} if *partition_count > 1 && *group_count > 1 => (
				*scan_input,
				*build_morsel,
				*partition_count,
				*group_count,
				partition_build_keys.clone(),
				write_build_keys.clone(),
				runtime.key_plan,
				false,
				materialized_output_schema.clone(),
				runtime.output_schema.clone(),
				runtime.probe_columns_first,
				*build_column_count,
				projection.clone(),
				runtime.probe_keys.clone(),
				runtime.post_filter,
			),
			_ => return Ok(false),
		};

		let build_result = self
			.build_partitioned_streaming_scan_inner_build_reservoir(
				scan_input,
				build_morsel.or(morsel),
				partition_count,
				&partition_build_keys,
				&write_build_keys,
				key_plan,
				null_safe_keys,
				projection.as_ref(),
			)
			.await;
		let (mut reservoir, build_rows) = match build_result {
			Ok(Some(result)) => result,
			Ok(None) => return Ok(false),
			Err(error) => return Err(error),
		};

		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let result = async {
			let Some(first_base_batch) = base_batches.first() else {
				return Ok::<(), ExecutionError>(());
			};
			let base_batch = concat_or_empty(first_base_batch.schema(), base_batches)?;
			if base_batch.num_rows() == 0 {
				return Ok(());
			}
			let (mut output, probe_rows, output_rows) = self
				.probe_partitioned_streamed_inner_scan_build_reservoir(
					&mut reservoir,
					partition_count,
					group_count,
					&base_batch,
					&probe_keys,
					&write_build_keys,
					key_plan,
					null_safe_keys,
					&materialized_output_schema,
					&output_schema,
					probe_columns_first,
					build_column_count,
					projection.as_ref(),
				)
				.await?;
			total_probe_rows = total_probe_rows.saturating_add(probe_rows);
			total_output_rows = total_output_rows.saturating_add(output_rows);
			if let Some(predicate) = post_filter {
				let mut filtered = Vec::new();
				for batch in output {
					let batch = self.filter_batch_with_bound_predicate(
						predicate,
						batch,
						"filter predicate",
					)?;
					if batch.num_rows() > 0 {
						filtered.push(batch);
					}
				}
				output = filtered;
			}
			self.process_reoriented_inner_chain_grouped_suffix_batch(
				output,
				suffix_runtimes,
				outer_predicate,
				project_exprs,
				project_output_schema,
				group_expr,
				aggr_expr,
				groups,
				group_types,
			)
			.await?;
			Ok::<(), ExecutionError>(())
		}
		.await;
		Self::cleanup_partitioned_streamed_inner_builds(reservoir).await;
		result?;
		if let ReorientedInnerJoinBuild::PartitionedScan {
			observed_build_rows,
			..
		} = &mut runtime.build
		{
			*observed_build_rows = observed_build_rows.saturating_add(build_rows);
		}
		self.record_reoriented_inner_probe(runtime, total_probe_rows, total_output_rows);
		Ok(true)
	}

	fn fold_reoriented_grouped_output_batch(
		&self,
		groups: &mut BTreeMap<Vec<GroupValue>, Vec<AggState>>,
		group_types: &mut Vec<DataType>,
		outer_predicate: Option<&BoundExprProgram>,
		project_exprs: Option<&BoundExprProgram>,
		project_output_schema: Option<&arrow::datatypes::SchemaRef>,
		group_expr: &BoundExprProgram,
		aggr_expr: &[BoundAggregateCall],
		batch: RecordBatch,
	) -> Result<(), ExecutionError> {
		let filtered = if let Some(predicate) = outer_predicate {
			self.filter_batch_with_bound_predicate(predicate, batch, "filter predicate")?
		} else {
			batch
		};
		if filtered.num_rows() == 0 {
			return Ok(());
		}
		let aggregate_input = match (project_exprs, project_output_schema) {
			(Some(project_exprs), Some(project_output_schema)) => RecordBatch::try_new(
				project_output_schema.clone(),
				self.evaluate_bound_program(project_exprs, &filtered)?,
			)?,
			(None, None) => filtered,
			_ => {
				return Err(ExecutionError::InvalidExpression {
					message: "reoriented grouped projection metadata is incomplete".to_string(),
				});
			}
		};
		self.fold_bound_grouped_aggregate_batch(
			groups,
			group_types,
			group_expr,
			aggr_expr,
			&aggregate_input,
		)
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
		if let Some(match_filter) = chain.semi_stage.match_filter {
			collect_left_side_program_columns(
				match_filter,
				chain.semi_stage.output_schema.fields().len(),
				&mut required_output,
			);
		}
		if let Some(match_filter) = chain.anti_stage.match_filter {
			collect_left_side_program_columns(
				match_filter,
				chain.anti_stage.output_schema.fields().len(),
				&mut required_output,
			);
		}
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
			let Some((join0, join0_filter)) = decompose_join_or_filtered_join(base_input.as_ref())
			else {
				return Ok(None);
			};
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
			} = join0
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
				let batch = if let Some(join0_filter) = join0_filter {
					self.filter_batch_with_bound_predicate(join0_filter, batch, "filter predicate")?
				} else {
					batch
				};
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
		let Some((join0, join0_filter)) = decompose_join_or_filtered_join(base_input.as_ref())
		else {
			return Ok(None);
		};
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
		} = join0
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
				let batch = if let Some(join0_filter) = join0_filter {
					self.filter_batch_with_bound_predicate(join0_filter, batch, "filter predicate")?
				} else {
					batch
				};
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
			let scan_input =
				decompose_streaming_scan_probe_input(stage.build).expect("checked scan build");
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
			let materialized_output_schema = if stage.probe_columns_first {
				concat_join_schema(&stage.build.schema(), &stage.probe_schema)
			} else {
				stage.output_schema.clone()
			};
			let estimated_build_bytes = self
				.estimate_streaming_scan_inner_build_bytes(
					scan_input,
					retained_columns,
					write_build_keys,
					false,
					morsel,
				)
				.await?;
			let (partition_count, group_count, _max_group_spill) = self
				.choose_streaming_scan_inner_build_partitioning(
					estimated_build_bytes,
					scan_input,
					retained_columns,
					&stage.build_keys,
					write_build_keys,
					false,
					morsel,
				)
				.await?;
			if partition_count > 1 {
				let partition_count_usize =
					usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
						message: "streamed inner build partition count exceeds usize".to_string(),
					})?;
				return Ok(Some(ReorientedInnerJoinRuntime {
					replay_id: stage.replay_id,
					strategy: stage.strategy,
					build: ReorientedInnerJoinBuild::PartitionedScan {
						scan_input,
						morsel,
						partition_count,
						group_count,
						partition_build_keys: stage.build_keys.clone(),
						write_build_keys: write_build_keys.to_vec(),
						materialized_output_schema,
						build_column_count: stage.build.schema().fields().len(),
						projection,
						estimated_build_bytes,
						observed_build_rows: 0,
						loaded_builds: (0..partition_count_usize).map(|_| None).collect(),
						loaded_resident_bytes: 0,
						loaded_build_attempted: false,
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
			let mut spilled = SpilledInnerJoinBuild::create(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
			)
			.await?;
			let build_started = Instant::now();
			self.write_streaming_scan_inner_build_batches(
				&mut spilled,
				scan_input,
				retained_columns,
				write_build_keys,
				key_plan,
				false,
				false,
				None,
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
		self.prepare_reoriented_inner_join_runtime_from_build_batches(stage, build_batches)
			.await
	}

	async fn prepare_reoriented_inner_join_runtime_from_build_batches<'a>(
		&self,
		stage: ReorientedInnerJoinStage<'a>,
		build_batches: Vec<RecordBatch>,
	) -> Result<Option<ReorientedInnerJoinRuntime<'a>>, ExecutionError> {
		let build_memory_cap =
			join_output_materialization_memory_cap(self.context.task_budget.memory.get());
		let estimated_build_bytes =
			estimated_build_hash_bytes_for_batches(&build_batches, &stage.build_keys)?;
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&stage.build.schema(),
			&stage.build_keys,
			&stage.probe_schema,
			&stage.probe_keys,
			false,
		)?;
		if estimated_build_bytes > build_memory_cap {
			let mut spilled = SpilledInnerJoinBuild::create(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
			)
			.await?;
			spilled.use_compact_resident_index();
			let build_started = Instant::now();
			for batch in &build_batches {
				spilled
					.write_batch_chunks(
						batch,
						key_plan,
						&stage.build_keys,
						false,
						self.context.task_budget,
					)
					.await?;
			}
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
					projection: None,
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
		let build_batch = concat_or_empty(stage.build.schema(), &build_batches)?;
		let build_key_arrays = key_arrays(&build_batch, &stage.build_keys)?;
		let build_started = Instant::now();
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
		let build_micros = duration_micros(build_started);
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.phases.build_micros = task.phases.build_micros.saturating_add(build_micros);
			task.counters.build_rows = task
				.counters
				.build_rows
				.saturating_add(build_batch.num_rows() as u64);
		});
		record_join_build_profile(
			&self.context.task_telemetry,
			stage.replay_id,
			stage.strategy,
			key_plan,
			&build_rows,
			build_micros,
		);
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
		let full_existence_schema = stage.existence.schema();
		let retained_existence_columns = retained_existence_columns_for_semi_anti_stage(&stage)?;
		let (build_schema, write_existence_keys) =
			if let Some(retained_columns) = retained_existence_columns.as_ref() {
				(
					projected_schema_for_columns(&full_existence_schema, retained_columns)?,
					remap_column_ordinals(retained_columns, &stage.existence_keys)?,
				)
			} else {
				(full_existence_schema.clone(), stage.existence_keys.clone())
			};
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&build_schema,
			&write_existence_keys,
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
		if let Some(retained_columns) = retained_existence_columns.as_ref() {
			spilled.set_projection(full_existence_schema.clone(), retained_columns.clone())?;
		}
		let membership_only = stage.match_filter.is_none();
		let Some(()) = self
			.write_streaming_semi_anti_existence(
				stage.existence,
				&write_existence_keys,
				key_plan,
				retained_existence_columns.as_deref(),
				membership_only,
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
		retained_columns: Option<&[crate::ir::ColumnOrdinal]>,
		membership_only: bool,
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
				if membership_only {
					if retained_columns.is_some() {
						spilled.write_projected_membership_key_chunks(
							&batch,
							key_plan,
							existence_keys,
							self.context.task_budget,
						)?;
					} else {
						spilled.write_membership_key_chunks(
							&batch,
							key_plan,
							existence_keys,
							self.context.task_budget,
						)?;
					}
				} else if retained_columns.is_some() {
					spilled
						.write_projected_batch_chunks(
							&batch,
							key_plan,
							existence_keys,
							self.context.task_budget,
						)
						.await?;
				} else {
					spilled
						.write_batch_chunks(
							&batch,
							key_plan,
							existence_keys,
							self.context.task_budget,
						)
						.await?;
				}
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
			ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel,
				partition_count,
				partition_build_keys,
				write_build_keys,
				materialized_output_schema,
				build_column_count,
				projection,
				estimated_build_bytes,
				observed_build_rows,
				group_count,
				loaded_builds,
				loaded_resident_bytes,
				loaded_build_attempted,
			} => {
				if probe_batch.num_rows() == 0 {
					(Vec::new(), 0, 0)
				} else {
					if !*loaded_build_attempted {
						*loaded_build_attempted = true;
						if let Some((loaded, resident_bytes, build_rows)) = self
							.try_load_resident_partitioned_streamed_inner_scan_build(
								*scan_input,
								*morsel,
								*partition_count,
								*group_count,
								partition_build_keys,
								write_build_keys,
								runtime.key_plan,
								false,
								projection.as_ref(),
								*estimated_build_bytes,
							)
							.await?
						{
							*loaded_builds = loaded;
							*loaded_resident_bytes = resident_bytes;
							*observed_build_rows = observed_build_rows.saturating_add(build_rows);
						}
					}
					if loaded_builds.iter().any(Option::is_some) {
						let (output, probe_rows, output_rows) = self
							.probe_loaded_partitioned_streamed_inner_scan_build(
								loaded_builds,
								*partition_count,
								probe_batch,
								&runtime.probe_keys,
								runtime.key_plan,
								false,
								materialized_output_schema,
								&runtime.output_schema,
								runtime.probe_columns_first,
								*build_column_count,
								projection.as_ref(),
							)?;
						(output, probe_rows, output_rows)
					} else {
						let (output, probe_rows, output_rows, build_rows) = self
							.probe_partitioned_streamed_inner_scan_build(
								*scan_input,
								*morsel,
								*partition_count,
								*group_count,
								partition_build_keys,
								write_build_keys,
								probe_batch,
								&runtime.probe_keys,
								runtime.key_plan,
								false,
								materialized_output_schema,
								&runtime.output_schema,
								runtime.probe_columns_first,
								*build_column_count,
								projection.as_ref(),
							)
							.await?;
						*observed_build_rows = observed_build_rows.saturating_add(build_rows);
						(output, probe_rows, output_rows)
					}
				}
			}
		};
		self.record_reoriented_inner_probe(runtime, probe_rows, output_rows);
		Ok(output)
	}

	#[allow(clippy::too_many_arguments)]
	async fn try_load_resident_partitioned_streamed_inner_scan_build(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		morsel: Option<(MorselId, u32)>,
		partition_count: u32,
		group_count: u32,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		projection: Option<&ProjectedInnerBuildProjection>,
		estimated_build_bytes: u64,
	) -> Result<Option<(Vec<Option<LoadedSpilledInnerBuild>>, u64, u64)>, ExecutionError> {
		let partition_count_usize =
			usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
				message: "streamed inner build partition count exceeds usize".to_string(),
			})?;
		let group_count = group_count.max(1).min(partition_count);
		let loaded_cap =
			join_spill_retained_footprint_memory_cap(self.context.task_budget.memory.get());
		let estimated_resident_floor = estimated_build_bytes
			.saturating_mul(STREAMED_INNER_LOADED_BUILD_ESTIMATE_SAFETY_MULTIPLIER)
			.saturating_add(64 * 1024);
		if estimated_resident_floor > loaded_cap {
			return Ok(None);
		}
		let mut loaded_builds: Vec<Option<LoadedSpilledInnerBuild>> =
			(0..partition_count_usize).map(|_| None).collect();
		let mut loaded_resident_bytes = 0u64;
		let mut total_build_rows = 0u64;
		let mut total_spill_bytes = 0u64;
		let build_started = Instant::now();

		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count_usize).map(|_| None).collect();
			for partition in group_range.clone() {
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await?;
				partitioned_builds[partition] = Some(created);
			}

			let mut group_spill_bytes = 0u64;
			let build_result = self
				.write_partitioned_streaming_scan_inner_build_batches(
					&mut partitioned_builds,
					&mut group_spill_bytes,
					scan_input,
					projection.map(|projection| projection.retained_columns.as_slice()),
					partition_build_keys,
					write_build_keys,
					key_plan,
					null_safe_keys,
					morsel,
					group_range.start,
					group_range.end,
					true,
				)
				.await;
			if let Err(error) = build_result {
				Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
				if matches!(error, ExecutionError::BudgetExhausted { .. }) {
					return Ok(None);
				}
				return Err(error);
			}
			total_spill_bytes = total_spill_bytes.saturating_add(group_spill_bytes);

			for partition in group_range {
				let Some(mut spilled) =
					partitioned_builds.get_mut(partition).and_then(Option::take)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					spilled.cleanup().await;
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				if spilled.input_row_count() == 0 {
					spilled.cleanup().await;
					continue;
				}
				let loaded_page_bytes = spilled.loaded_page_bytes_estimate();
				let estimated_index = estimate_loaded_partition_index_bytes(
					spilled.input_row_count(),
					key_plan,
					loaded_page_bytes,
				);
				let estimated_resident = loaded_page_bytes
					.saturating_add(estimated_index)
					.saturating_add(64 * 1024);
				if loaded_resident_bytes.saturating_add(estimated_resident) > loaded_cap {
					spilled.cleanup().await;
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Ok(None);
				}
				let build_pages = match spilled.read_all_batch_pages().await {
					Ok(pages) => pages,
					Err(error) => {
						spilled.cleanup().await;
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				let build_rows = spilled.input_row_count();
				spilled.cleanup().await;
				let loaded = LoadedSpilledInnerBuild::new(
					build_pages,
					key_plan,
					write_build_keys,
					null_safe_keys,
					self.context.task_budget.memory.get(),
				)?;
				if loaded_resident_bytes.saturating_add(loaded.resident_bytes()) > loaded_cap {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Ok(None);
				}
				loaded_resident_bytes =
					loaded_resident_bytes.saturating_add(loaded.resident_bytes());
				total_build_rows = total_build_rows.saturating_add(build_rows);
				loaded_builds[partition] = Some(loaded);
			}
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}

		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(total_build_rows);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.spill_bytes = task.counters.spill_bytes.saturating_add(total_spill_bytes);
		});
		Ok(Some((
			loaded_builds,
			loaded_resident_bytes,
			total_build_rows,
		)))
	}

	#[allow(clippy::too_many_arguments)]
	async fn build_partitioned_streaming_scan_inner_build_reservoir(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		morsel: Option<(MorselId, u32)>,
		partition_count: u32,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		projection: Option<&ProjectedInnerBuildProjection>,
	) -> Result<Option<(Vec<Option<SpilledInnerJoinBuild>>, u64)>, ExecutionError> {
		let partition_count_usize =
			usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
				message: "streamed inner build partition count exceeds usize".to_string(),
			})?;
		let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
			(0..partition_count_usize).map(|_| None).collect();
		for slot in &mut partitioned_builds {
			let created = SpilledInnerJoinBuild::create(
				self.context.task_budget,
				self.context.join_spill_file_ops.clone(),
				self.context.join_spill_dir.clone(),
			)
			.await?;
			*slot = Some(created);
		}

		let build_started = Instant::now();
		let mut total_spill_bytes = 0u64;
		let build_result = self
			.write_partitioned_streaming_scan_inner_build_batches(
				&mut partitioned_builds,
				&mut total_spill_bytes,
				scan_input,
				projection.map(|projection| projection.retained_columns.as_slice()),
				partition_build_keys,
				write_build_keys,
				key_plan,
				null_safe_keys,
				morsel,
				0,
				partition_count_usize,
				false,
			)
			.await;
		if let Err(error) = build_result {
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
			if matches!(error, ExecutionError::BudgetExhausted { .. }) {
				return Ok(None);
			}
			return Err(error);
		}

		let mut total_build_rows = 0u64;
		for partition in 0..partition_count_usize {
			let Some(spilled) = partitioned_builds
				.get_mut(partition)
				.and_then(Option::as_mut)
			else {
				continue;
			};
			if let Err(error) = spilled.finish(self.context.task_budget).await {
				Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
				return Err(error);
			}
			spilled.release_write_scratch();
			total_build_rows = total_build_rows.saturating_add(spilled.input_row_count());
			if spilled.input_row_count() == 0 {
				let Some(empty) = partitioned_builds.get_mut(partition).and_then(Option::take)
				else {
					continue;
				};
				empty.cleanup().await;
			}
		}

		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(total_build_rows);
			task.phases.build_micros = task
				.phases
				.build_micros
				.saturating_add(duration_micros(build_started));
			task.phases.spill_micros = task
				.phases
				.spill_micros
				.saturating_add(duration_micros(build_started));
			task.counters.spill_bytes = task.counters.spill_bytes.saturating_add(total_spill_bytes);
		});
		Ok(Some((partitioned_builds, total_build_rows)))
	}

	#[allow(clippy::too_many_arguments)]
	fn probe_loaded_partitioned_streamed_inner_scan_build(
		&self,
		loaded_builds: &[Option<LoadedSpilledInnerBuild>],
		partition_count: u32,
		probe_batch: &RecordBatch,
		probe_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &SchemaRef,
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		build_column_count: usize,
		projection: Option<&ProjectedInnerBuildProjection>,
	) -> Result<(Vec<RecordBatch>, u64, u64), ExecutionError> {
		let mut output = Vec::new();
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let probe_partition_buckets = precompute_join_partition_buckets(
			probe_batch,
			probe_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		for (partition, partitioned_probe) in partition_streaming_join_batch_with_buckets(
			probe_batch,
			&probe_partition_buckets,
			partition_count,
		)? {
			let Some(loaded) = loaded_builds.get(partition).and_then(Option::as_ref) else {
				continue;
			};
			let result = visit_loaded_partition_work(
				loaded,
				partition,
				partitioned_probe,
				probe_keys,
				key_plan,
				null_safe_keys,
				materialized_output_schema,
				output_schema,
				probe_columns_first,
				build_column_count,
				projection,
				self.context.task_budget.memory.get(),
			)?;
			total_probe_rows = total_probe_rows.saturating_add(result.probe_rows);
			total_output_rows = total_output_rows.saturating_add(result.output_rows);
			output.extend(result.output);
		}
		Ok((output, total_probe_rows, total_output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn probe_partitioned_streamed_inner_scan_build_reservoir(
		&self,
		partitioned_builds: &mut [Option<SpilledInnerJoinBuild>],
		partition_count: u32,
		group_count: u32,
		probe_batch: &RecordBatch,
		probe_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &SchemaRef,
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		build_column_count: usize,
		projection: Option<&ProjectedInnerBuildProjection>,
	) -> Result<(Vec<RecordBatch>, u64, u64), ExecutionError> {
		let mut output = Vec::new();
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let probe_partition_buckets = precompute_join_partition_buckets(
			probe_batch,
			probe_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		let partitioned_probe_batches = partition_streaming_join_batch_with_buckets(
			probe_batch,
			&probe_partition_buckets,
			partition_count,
		)?;
		if partitioned_probe_batches.is_empty() {
			return Ok((output, 0, 0));
		}
		let group_count = group_count.max(1).min(partition_count);
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			for (partition, partitioned_probe) in partitioned_probe_batches
				.iter()
				.filter(|(partition, _)| group_range.contains(partition))
			{
				let Some(spilled) = partitioned_builds
					.get_mut(*partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if spilled.input_row_count() == 0 {
					continue;
				}
				let memory_cap = self.context.task_budget.memory.get();
				let loaded_resident =
					estimate_parallel_partition_resident(spilled, partitioned_probe, key_plan);
				let loaded_cap = join_spill_retained_footprint_memory_cap(memory_cap);
				if loaded_resident <= loaded_cap {
					let build_pages = spilled.read_all_batch_pages().await?;
					let work = LoadedPartitionWork {
						partition: *partition,
						build_pages,
						probe_batch: partitioned_probe.clone(),
						probe_keys: probe_keys.to_vec(),
						write_build_keys: write_build_keys.to_vec(),
						key_plan,
						null_safe_keys,
						materialized_output_schema: materialized_output_schema.clone(),
						output_schema: output_schema.clone(),
						probe_columns_first,
						build_column_count,
						projection: projection.cloned(),
						memory_cap,
					};
					let result = run_loaded_partition_work(work)?;
					total_probe_rows = total_probe_rows.saturating_add(result.probe_rows);
					total_output_rows = total_output_rows.saturating_add(result.output_rows);
					output.extend(result.output);
				} else {
					if let Err(error) = spilled
						.rebuild_index_from_pages(
							key_plan,
							write_build_keys,
							null_safe_keys,
							self.context.task_budget,
						)
						.await
					{
						return Err(error);
					}
					let mut partition_output = Vec::new();
					let (probe_rows, output_rows) = self
						.visit_streaming_spilled_inner_probe_batch(
							spilled,
							partitioned_probe,
							probe_keys,
							key_plan,
							null_safe_keys,
							materialized_output_schema,
							output_schema,
							None,
							probe_columns_first,
							build_column_count,
							projection,
							&mut |batch| {
								partition_output.push(batch);
								Ok(())
							},
						)
						.await?;
					total_probe_rows = total_probe_rows.saturating_add(probe_rows);
					total_output_rows = total_output_rows.saturating_add(output_rows);
					output.extend(partition_output);
				}
			}
		}
		Ok((output, total_probe_rows, total_output_rows))
	}

	#[allow(clippy::too_many_arguments)]
	async fn probe_partitioned_streamed_inner_scan_build(
		&self,
		scan_input: StreamingScanProbeInput<'_>,
		morsel: Option<(MorselId, u32)>,
		partition_count: u32,
		group_count: u32,
		partition_build_keys: &[crate::ir::ColumnOrdinal],
		write_build_keys: &[crate::ir::ColumnOrdinal],
		probe_batch: &RecordBatch,
		probe_keys: &[crate::ir::ColumnOrdinal],
		key_plan: JoinKeyPlan,
		null_safe_keys: bool,
		materialized_output_schema: &SchemaRef,
		output_schema: &SchemaRef,
		probe_columns_first: bool,
		build_column_count: usize,
		projection: Option<&ProjectedInnerBuildProjection>,
	) -> Result<(Vec<RecordBatch>, u64, u64, u64), ExecutionError> {
		let mut output = Vec::new();
		let mut total_probe_rows = 0u64;
		let mut total_output_rows = 0u64;
		let probe_partition_buckets = precompute_join_partition_buckets(
			probe_batch,
			probe_keys,
			partition_count,
			null_safe_keys,
			false,
		)?;
		let partitioned_probe_batches = partition_streaming_join_batch_with_buckets(
			probe_batch,
			&probe_partition_buckets,
			partition_count,
		)?;
		if partitioned_probe_batches.is_empty() {
			return Ok((output, 0, 0, 0));
		}
		let build_started = Instant::now();
		let mut total_build_rows = 0u64;
		let mut cumulative_spill_bytes = 0u64;
		let group_count = group_count.max(1).min(partition_count);
		for group in 0..group_count {
			let group_range = partition_group_bounds(partition_count, group_count, group)?;
			let group_probe_batches = partitioned_probe_batches
				.iter()
				.filter(|(partition, _)| group_range.contains(partition))
				.map(|(partition, batch)| (*partition, batch.clone()))
				.collect::<Vec<_>>();
			if group_probe_batches.is_empty() {
				continue;
			}

			let mut partitioned_builds: Vec<Option<SpilledInnerJoinBuild>> =
				(0..partition_count).map(|_| None).collect();
			for (partition, _) in &group_probe_batches {
				if *partition >= partitioned_builds.len() {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(ExecutionError::Kernel {
						message: format!("active join partition {partition} is out of range"),
					});
				}
				let created = SpilledInnerJoinBuild::create(
					self.context.task_budget,
					self.context.join_spill_file_ops.clone(),
					self.context.join_spill_dir.clone(),
				)
				.await;
				let created = match created {
					Ok(created) => created,
					Err(error) => {
						Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
						return Err(error);
					}
				};
				partitioned_builds[*partition] = Some(created);
			}

			let mut group_spill_bytes = 0u64;
			let build_result = self
				.write_partitioned_streaming_scan_inner_build_batches(
					&mut partitioned_builds,
					&mut group_spill_bytes,
					scan_input,
					projection.map(|projection| projection.retained_columns.as_slice()),
					partition_build_keys,
					write_build_keys,
					key_plan,
					null_safe_keys,
					morsel,
					group_range.start,
					group_range.end,
					true,
				)
				.await;
			if let Err(error) = build_result {
				Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
				return Err(error);
			}
			cumulative_spill_bytes = cumulative_spill_bytes.saturating_add(group_spill_bytes);

			for (partition, _) in &group_probe_batches {
				let Some(spilled) = partitioned_builds
					.get_mut(*partition)
					.and_then(Option::as_mut)
				else {
					continue;
				};
				if let Err(error) = spilled.finish(self.context.task_budget).await {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				spilled.release_write_scratch();
				total_build_rows = total_build_rows.saturating_add(spilled.input_row_count());
			}

			let parallel_plan = parallel_partition_plan(
				&partitioned_builds,
				&group_probe_batches,
				key_plan,
				self.context.partition_parallelism,
				self.context.task_budget.memory.get(),
			);
			debug_assert!(parallel_plan.resident_peak > 0 || group_probe_batches.is_empty());
			if parallel_plan.k > 1 {
				let partition_count_usize =
					usize::try_from(partition_count).map_err(|_| ExecutionError::Kernel {
						message: "streamed inner build partition count exceeds usize".to_string(),
					})?;
				let semaphore = Arc::new(tokio::sync::Semaphore::new(parallel_plan.k));
				let mut handles = Vec::new();
				let mut first_error = None;
				for (partition, partitioned_probe) in group_probe_batches {
					let Some(mut spilled) =
						partitioned_builds.get_mut(partition).and_then(Option::take)
					else {
						continue;
					};
					if spilled.input_row_count() == 0 {
						spilled.cleanup().await;
						continue;
					}
					let permit = match semaphore.clone().acquire_owned().await {
						Ok(permit) => permit,
						Err(error) => {
							spilled.cleanup().await;
							first_error.get_or_insert(ExecutionError::Kernel {
								message: format!(
									"parallel streamed inner join semaphore closed: {error}"
								),
							});
							break;
						}
					};
					let build_pages = match spilled.read_all_batch_pages().await {
						Ok(pages) => pages,
						Err(error) => {
							spilled.cleanup().await;
							first_error.get_or_insert(error);
							break;
						}
					};
					spilled.cleanup().await;
					let work = LoadedPartitionWork {
						partition,
						build_pages,
						probe_batch: partitioned_probe,
						probe_keys: probe_keys.to_vec(),
						write_build_keys: write_build_keys.to_vec(),
						key_plan,
						null_safe_keys,
						materialized_output_schema: materialized_output_schema.clone(),
						output_schema: output_schema.clone(),
						probe_columns_first,
						build_column_count,
						projection: projection.cloned(),
						memory_cap: self.context.task_budget.memory.get(),
					};
					handles.push(tokio::task::spawn_blocking(move || {
						let _permit = permit;
						run_loaded_partition_work(work)
					}));
				}
				let mut slots: Vec<Option<LoadedPartitionResult>> =
					(0..partition_count_usize).map(|_| None).collect();
				let ordered_output_slot_cap =
					join_output_materialization_memory_cap(self.context.task_budget.memory.get());
				let mut ordered_output_slot_bytes = 0u64;
				for handle in handles {
					let result = match handle.await {
						Ok(Ok(result)) => result,
						Ok(Err(error)) => {
							first_error.get_or_insert(error);
							continue;
						}
						Err(error) => {
							first_error.get_or_insert(ExecutionError::Kernel {
								message: format!(
									"parallel streamed inner join partition task failed: {error}"
								),
							});
							continue;
						}
					};
					if result.partition >= slots.len() {
						first_error.get_or_insert(ExecutionError::Kernel {
							message: format!(
								"parallel streamed inner join partition {} is out of range",
								result.partition
							),
						});
						continue;
					}
					if slots[result.partition].is_some() {
						first_error.get_or_insert(ExecutionError::Kernel {
							message: format!(
								"parallel streamed inner join duplicate partition {}",
								result.partition
							),
						});
						continue;
					}
					let result_output_bytes = result
						.output
						.iter()
						.map(record_batch_memory_bytes)
						.fold(0u64, u64::saturating_add);
					ordered_output_slot_bytes =
						ordered_output_slot_bytes.saturating_add(result_output_bytes);
					if ordered_output_slot_bytes > ordered_output_slot_cap {
						first_error.get_or_insert(budget_exhausted(
							BudgetDimension::Memory,
							"parallel streamed inner join ordered output slots",
							ordered_output_slot_bytes,
							ordered_output_slot_cap,
						));
						continue;
					}
					let partition = result.partition;
					slots[partition] = Some(result);
				}
				if let Some(error) = first_error {
					Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
					return Err(error);
				}
				for result in slots.into_iter().flatten() {
					total_probe_rows = total_probe_rows.saturating_add(result.probe_rows);
					total_output_rows = total_output_rows.saturating_add(result.output_rows);
					output.extend(result.output);
				}
			} else {
				for (partition, partitioned_probe) in group_probe_batches {
					let Some(mut spilled) =
						partitioned_builds.get_mut(partition).and_then(Option::take)
					else {
						continue;
					};
					if spilled.input_row_count() == 0 {
						spilled.cleanup().await;
						continue;
					}
					let memory_cap = self.context.task_budget.memory.get();
					let loaded_resident = estimate_parallel_partition_resident(
						&spilled,
						&partitioned_probe,
						key_plan,
					);
					let loaded_cap = join_spill_retained_footprint_memory_cap(memory_cap);
					if loaded_resident <= loaded_cap {
						let build_pages = match spilled.read_all_batch_pages().await {
							Ok(pages) => pages,
							Err(error) => {
								spilled.cleanup().await;
								Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds)
									.await;
								return Err(error);
							}
						};
						spilled.cleanup().await;
						let work = LoadedPartitionWork {
							partition,
							build_pages,
							probe_batch: partitioned_probe,
							probe_keys: probe_keys.to_vec(),
							write_build_keys: write_build_keys.to_vec(),
							key_plan,
							null_safe_keys,
							materialized_output_schema: materialized_output_schema.clone(),
							output_schema: output_schema.clone(),
							probe_columns_first,
							build_column_count,
							projection: projection.cloned(),
							memory_cap,
						};
						let result = match run_loaded_partition_work(work) {
							Ok(result) => result,
							Err(error) => {
								Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds)
									.await;
								return Err(error);
							}
						};
						total_probe_rows = total_probe_rows.saturating_add(result.probe_rows);
						total_output_rows = total_output_rows.saturating_add(result.output_rows);
						output.extend(result.output);
					} else {
						if let Err(error) = spilled
							.rebuild_index_from_pages(
								key_plan,
								write_build_keys,
								null_safe_keys,
								self.context.task_budget,
							)
							.await
						{
							spilled.cleanup().await;
							Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds)
								.await;
							return Err(error);
						}

						let mut partition_output = Vec::new();
						let result = self
							.visit_streaming_spilled_inner_probe_batch(
								&mut spilled,
								&partitioned_probe,
								probe_keys,
								key_plan,
								null_safe_keys,
								materialized_output_schema,
								output_schema,
								None,
								probe_columns_first,
								build_column_count,
								projection,
								&mut |batch| {
									partition_output.push(batch);
									Ok(())
								},
							)
							.await;
						spilled.cleanup().await;
						let (probe_rows, output_rows) = match result {
							Ok(result) => result,
							Err(error) => {
								Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds)
									.await;
								return Err(error);
							}
						};
						total_probe_rows = total_probe_rows.saturating_add(probe_rows);
						total_output_rows = total_output_rows.saturating_add(output_rows);
						output.extend(partition_output);
					}
				}
			}
			Self::cleanup_partitioned_streamed_inner_builds(partitioned_builds).await;
		}
		record_task_telemetry(&self.context.task_telemetry, |task| {
			task.counters.build_rows = task.counters.build_rows.saturating_add(total_build_rows);
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
				.saturating_add(cumulative_spill_bytes);
		});
		Ok((
			output,
			total_probe_rows,
			total_output_rows,
			total_build_rows,
		))
	}

	async fn cleanup_partitioned_streamed_inner_builds(
		partitioned_builds: Vec<Option<SpilledInnerJoinBuild>>,
	) {
		for spilled in partitioned_builds.into_iter().flatten() {
			spilled.cleanup().await;
		}
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
		build_rows: &JoinBuildRows,
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
	match_filter: Option<&BoundExprProgram>,
	existence_batch: &RecordBatch,
	existence_rows: &JoinBuildRows,
) -> Result<Vec<bool>, ExecutionError> {
	let mut matched = vec![false; preserved_batch.num_rows()];
	let mut residual = JoinMatchResidual::new(match_filter, preserved_batch, existence_batch);
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
	use crate::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
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

	struct CountingFileOps {
		inner: BlockingFileOps,
		read_plain_calls: std::sync::atomic::AtomicU64,
	}

	impl CountingFileOps {
		fn new() -> Self {
			Self {
				inner: BlockingFileOps,
				read_plain_calls: std::sync::atomic::AtomicU64::new(0),
			}
		}

		fn read_plain_calls(&self) -> u64 {
			self.read_plain_calls
				.load(std::sync::atomic::Ordering::SeqCst)
		}
	}

	#[test]
	fn reoriented_scan_build_decision_checks_hash_accumulator_bytes() {
		let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
		match reoriented_stage_scan_build_decision_for_materialized_batches(
			512,
			1025,
			1024,
			vec![batch.clone()],
		) {
			ReorientedStageScanBuildDecision::StreamMaterializedBatches(batches) => {
				assert_eq!(batches.len(), 1)
			}
			_ => panic!("build hash over cap should stream while preserving measured batches"),
		}
		assert!(matches!(
			reoriented_stage_scan_build_decision_for_materialized_batches(
				1025,
				512,
				1024,
				vec![batch.clone()]
			),
			ReorientedStageScanBuildDecision::Stream
		));
		let decision = reoriented_stage_scan_build_decision_for_materialized_batches(
			512,
			1024,
			1024,
			vec![batch],
		);
		match decision {
			ReorientedStageScanBuildDecision::MaterializedBatches(batches) => {
				assert_eq!(batches.len(), 1)
			}
			_ => panic!("build should stay materialized when scan and hash estimates fit"),
		}
	}

	#[async_trait::async_trait]
	impl AsyncFileOps for CountingFileOps {
		async fn open(
			&self,
			path: &std::path::Path,
			flags: OpenFlags,
		) -> std::io::Result<FileHandle> {
			self.inner.open(path, flags).await
		}

		async fn read_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			len: usize,
		) -> std::io::Result<Vec<u8>> {
			self.read_plain_calls
				.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			self.inner.read_plain_at(handle, offset, len).await
		}

		async fn write_plain_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			bytes: Vec<u8>,
		) -> std::io::Result<usize> {
			self.inner.write_plain_at(handle, offset, bytes).await
		}

		async fn read_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: wendelon::storage::AlignedBuf,
		) -> std::io::Result<(usize, wendelon::storage::AlignedBuf)> {
			self.inner.read_at(handle, offset, buf).await
		}

		async fn write_at(
			&self,
			handle: &FileHandle,
			offset: u64,
			buf: wendelon::storage::AlignedBuf,
		) -> std::io::Result<(usize, wendelon::storage::AlignedBuf)> {
			self.inner.write_at(handle, offset, buf).await
		}

		async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fsync(handle).await
		}

		async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
			self.inner.fdatasync(handle).await
		}

		async fn read_all(&self, path: &std::path::Path) -> std::io::Result<Vec<u8>> {
			self.inner.read_all(path).await
		}

		async fn write_all(&self, path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
			self.inner.write_all(path, bytes).await
		}

		async fn file_len(&self, path: &std::path::Path) -> std::io::Result<Option<u64>> {
			self.inner.file_len(path).await
		}

		async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
			self.inner.set_len(handle, len).await
		}

		async fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
			self.inner.create_dir_all(path).await
		}

		async fn read_dir(
			&self,
			path: &std::path::Path,
		) -> std::io::Result<Vec<wendelon::storage::DirectoryEntry>> {
			self.inner.read_dir(path).await
		}

		async fn fsync_dir(&self, path: &std::path::Path) -> std::io::Result<()> {
			self.inner.fsync_dir(path).await
		}

		async fn rename(
			&self,
			from: &std::path::Path,
			to: &std::path::Path,
		) -> std::io::Result<()> {
			self.inner.rename(from, to).await
		}

		async fn unlink(&self, path: &std::path::Path) -> std::io::Result<()> {
			self.inner.unlink(path).await
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
			estimated_byte_size: None,
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

	fn left_semi_join_plan(
		left: ExecutionPlan,
		right: ExecutionPlan,
		left_keys: Vec<ColumnOrdinal>,
		right_keys: Vec<ColumnOrdinal>,
		output_schema: SchemaRef,
	) -> ExecutionPlan {
		ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(left),
			right: Box::new(right),
			kind: JoinKind::LeftSemi,
			left_keys,
			right_keys,
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema,
		}
	}

	fn exchange_plan(
		input: ExecutionPlan,
		key: BoundExprProgram,
		partition_count: u32,
	) -> ExecutionPlan {
		ExecutionPlan::Exchange {
			input: Box::new(input),
			partition_count,
			key,
		}
	}

	#[test]
	fn reoriented_inner_chain_grouped_base_classifies_filtered_and_bare_scan_probe() {
		let left_schema = Arc::new(Schema::new(vec![Field::new(
			"left_key",
			DataType::Int64,
			false,
		)]));
		let right_schema = Arc::new(Schema::new(vec![Field::new(
			"right_key",
			DataType::Int64,
			false,
		)]));
		let left = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(1), left_schema.clone(), 1),
			bound_column_root("left_key", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let right = exchange_plan(
			test_scan_plan_with_first_field_key(TableId(2), right_schema.clone(), 2),
			bound_column_root("right_key", ColumnOrdinal(0), DataType::Int64, false),
			4,
		);
		let join = inner_join_plan(
			left,
			right,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			concat_test_schema(&left_schema, &right_schema),
		);
		let base_key = bound_column_root("left_key", ColumnOrdinal(0), DataType::Int64, false);
		let bare_base = exchange_plan(join.clone(), base_key.clone(), 4);
		assert!(matches!(
			reoriented_inner_chain_grouped_base_scan_probe_mode(&bare_base),
			Some(ReorientedInnerChainGroupedBaseScanProbeMode::BareJoin0)
		));

		let filtered_base = exchange_plan(
			ExecutionPlan::Filter {
				input: Box::new(join),
				predicate: less_than_filter_program(ColumnOrdinal(0), 10),
				disposition: FilterDisposition::EvaluateInToron,
			},
			base_key,
			4,
		);
		assert!(matches!(
			reoriented_inner_chain_grouped_base_scan_probe_mode(&filtered_base),
			Some(ReorientedInnerChainGroupedBaseScanProbeMode::FilteredJoin0)
		));
	}

	#[tokio::test]
	async fn partition_reservoir_scans_source_once_and_matches_group_rescan_path() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, false),
			Field::new("build_payload", DataType::Int64, false),
		]));
		let probe_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_tag", DataType::Int64, false),
		]));
		let materialized_output_schema = concat_test_schema(&build_schema, &probe_schema);
		let output_schema = concat_test_schema(&probe_schema, &build_schema);
		let build_rows = 64usize;
		let build_batch = RecordBatch::try_new(
			build_schema.clone(),
			vec![
				Arc::new(Int64Array::from_iter_values(
					0..i64::try_from(build_rows).expect("build rows fit"),
				)),
				Arc::new(Int64Array::from_iter_values(
					(0..i64::try_from(build_rows).expect("build rows fit")).map(|row| row * 10),
				)),
			],
		)
		.expect("build batch");
		let probe_batch = RecordBatch::try_new(
			probe_schema.clone(),
			vec![
				Arc::new(Int64Array::from_iter_values(
					(0..i64::try_from(build_rows).expect("build rows fit")).rev(),
				)),
				Arc::new(Int64Array::from_iter_values(
					(0..i64::try_from(build_rows).expect("build rows fit"))
						.rev()
						.map(|row| row * 100),
				)),
			],
		)
		.expect("probe batch");
		let build_scan = test_scan_plan_with_first_field_key(TableId(40), build_schema.clone(), 40);
		let build_exchange = exchange_plan(
			build_scan,
			bound_column_root("build_key", ColumnOrdinal(0), DataType::Int64, false),
			8,
		);
		let scan_input =
			decompose_streaming_scan_probe_input(&build_exchange).expect("build scan input");
		let budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let build_keys = vec![ColumnOrdinal(0)];
		let probe_keys = vec![ColumnOrdinal(0)];
		let make_executor = |requested_tables: Arc<Mutex<Vec<TableId>>>| {
			Executor::with_context(
				Arc::new(RecordingMultiBatchTableStorageScan {
					batches: HashMap::from([(TableId(40), vec![build_batch.clone()])]),
					requested_tables,
				}),
				ExecutionContext::local_test().with_task_budget(budget),
			)
		};

		let reservoir_requested_tables = Arc::new(Mutex::new(Vec::new()));
		let reservoir_executor = make_executor(reservoir_requested_tables.clone());
		let (mut reservoir, reservoir_build_rows) = reservoir_executor
			.build_partitioned_streaming_scan_inner_build_reservoir(
				scan_input,
				None,
				8,
				&build_keys,
				&build_keys,
				JoinKeyPlan::Int64,
				false,
				None,
			)
			.await
			.expect("reservoir build")
			.expect("reservoir should fit");
		assert_eq!(
			reservoir_build_rows, build_rows as u64,
			"reservoir should write every build row exactly once"
		);
		assert_eq!(
			reservoir_requested_tables
				.lock()
				.expect("reservoir requested tables")
				.iter()
				.filter(|table| **table == TableId(40))
				.count(),
			1,
			"reservoir build must scan the source once, not once per partition group"
		);
		let reservoir_probe = reservoir_executor
			.probe_partitioned_streamed_inner_scan_build_reservoir(
				&mut reservoir,
				8,
				4,
				&probe_batch,
				&probe_keys,
				&build_keys,
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await;
		Executor::cleanup_partitioned_streamed_inner_builds(reservoir).await;
		let (reservoir_output, reservoir_probe_rows, reservoir_output_rows) =
			reservoir_probe.expect("reservoir probe");

		let group_requested_tables = Arc::new(Mutex::new(Vec::new()));
		let group_executor = make_executor(group_requested_tables.clone());
		let (group_output, group_probe_rows, group_output_rows, _) = group_executor
			.probe_partitioned_streamed_inner_scan_build(
				scan_input,
				None,
				8,
				4,
				&build_keys,
				&build_keys,
				&probe_batch,
				&probe_keys,
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await
			.expect("group re-scan probe");
		let group_scan_count = group_requested_tables
			.lock()
			.expect("group requested tables")
			.iter()
			.filter(|table| **table == TableId(40))
			.count();
		assert!(
			group_scan_count > 1,
			"fixture must exercise the old repeated-scan comparison path"
		);

		assert_eq!(reservoir_probe_rows, group_probe_rows);
		assert_eq!(reservoir_output_rows, group_output_rows);
		assert_eq!(
			canonical_arrow_ipc_digest(&reservoir_output, SortPolicy::None)
				.expect("reservoir digest"),
			canonical_arrow_ipc_digest(&group_output, SortPolicy::None)
				.expect("group re-scan digest"),
			"one-pass reservoir output must match the old per-group scan path row-for-row"
		);
	}

	#[tokio::test]
	async fn reoriented_inner_chain_generic_base_consumes_streamed_stage_runtime() {
		let dim0_schema = Arc::new(Schema::new(vec![
			Field::new("d0_key", DataType::Int64, false),
			Field::new("d0_label", DataType::Utf8, false),
		]));
		let dim1_schema = Arc::new(Schema::new(vec![
			Field::new("d1_key", DataType::Int64, false),
			Field::new("d1_label", DataType::Utf8, false),
		]));
		let fact_schema = Arc::new(Schema::new(vec![
			Field::new("d0_key", DataType::Int64, false),
			Field::new("d1_key", DataType::Int64, false),
			Field::new("amount", DataType::Int64, false),
		]));
		let dim0_batch = RecordBatch::try_new(
			dim0_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![0, 1])),
				Arc::new(StringArray::from(vec!["A", "B"])),
			],
		)
		.expect("dim0 batch");
		let fact_batch = RecordBatch::try_new(
			fact_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..32)
						.map(|row| if row % 2 == 0 { 0 } else { 1 })
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(
					(0..32)
						.map(|row| i64::try_from(row % 4).expect("d1 key fits"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(
					(0..32)
						.map(|row| i64::try_from(row + 1).expect("amount fits"))
						.collect::<Vec<_>>(),
				)),
			],
		)
		.expect("fact batch");
		let dim1_rows = 512usize;
		let dim1_labels = (0..dim1_rows)
			.map(|row| format!("label-{row}-{}", "x".repeat(192)))
			.collect::<Vec<_>>();
		let dim1_batch = RecordBatch::try_new(
			dim1_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..dim1_rows)
						.map(|row| i64::try_from(row).expect("d1 key fits"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(StringArray::from(dim1_labels)),
			],
		)
		.expect("dim1 batch");

		let build_plan = |budget, wrap_aggregate_input: bool| {
			let requested_tables = Arc::new(Mutex::new(Vec::new()));
			let storage = Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([
					(TableId(1), vec![dim0_batch.clone()]),
					(TableId(2), vec![fact_batch.clone()]),
					(TableId(3), vec![dim1_batch.clone()]),
				]),
				requested_tables: requested_tables.clone(),
			});
			let executor = Executor::with_context(
				storage,
				ExecutionContext::local_test().with_task_budget(budget),
			);
			let dim0 = exchange_plan(
				test_scan_plan_with_first_field_key(TableId(1), dim0_schema.clone(), 1),
				bound_column_root("d0_key", ColumnOrdinal(0), DataType::Int64, false),
				4,
			);
			let fact = exchange_plan(
				test_scan_plan_with_first_field_key(TableId(2), fact_schema.clone(), 2),
				bound_column_root("d0_key", ColumnOrdinal(0), DataType::Int64, false),
				4,
			);
			let join0_schema = concat_test_schema(&dim0_schema, &fact_schema);
			let join0 = inner_join_plan(
				dim0,
				fact,
				vec![ColumnOrdinal(0)],
				vec![ColumnOrdinal(0)],
				join0_schema.clone(),
			);
			let base = exchange_plan(
				ExecutionPlan::Filter {
					input: Box::new(join0),
					predicate: less_than_filter_program(ColumnOrdinal(0), 10),
					disposition: FilterDisposition::FullyPushed,
				},
				bound_column_root("d1_key", ColumnOrdinal(3), DataType::Int64, false),
				4,
			);
			assert!(
				reoriented_inner_chain_grouped_base_scan_probe_mode(&base).is_none(),
				"fixture must force the generic base traversal, not the specialized base path"
			);

			let dim1 = exchange_plan(
				test_scan_plan_with_first_field_key(TableId(3), dim1_schema.clone(), 3),
				bound_column_root("d1_key", ColumnOrdinal(0), DataType::Int64, false),
				4,
			);
			let join1_schema = concat_test_schema(&dim1_schema, &join0_schema);
			let join1 = ExecutionPlan::Join {
				replay_id: JoinReplayId(1),
				replay_eligibility: BroadcastReplayEligibility::Terminal,
				left: Box::new(dim1),
				right: Box::new(base),
				kind: JoinKind::Inner,
				left_keys: vec![ColumnOrdinal(0)],
				right_keys: vec![ColumnOrdinal(3)],
				match_filter: None,
				null_safe_keys: false,
				strategy: JoinStrategy::HashPartitioned,
				output_schema: join1_schema,
			};
			let group_expr = bound_column_root("d1_label", ColumnOrdinal(1), DataType::Utf8, false);
			let aggr_expr = vec![BoundAggregateCall {
				function: AggregateFunction::Sum,
				args: bound_column_root("amount", ColumnOrdinal(6), DataType::Int64, false),
			}];
			let output_schema = Arc::new(Schema::new(vec![
				Field::new("d1_label", DataType::Utf8, false),
				Field::new("sum_amount", DataType::Int64, true),
			]));
			let aggregate_input = if wrap_aggregate_input {
				exchange_plan(join1, group_expr.clone(), 4)
			} else {
				join1
			};
			let plan = ExecutionPlan::Aggregate {
				input: Box::new(aggregate_input),
				group_expr,
				aggr_expr,
				output_schema,
			};
			(executor, plan, requested_tables)
		};

		let low_budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		for (wrap_aggregate_input, shape) in [
			(true, "exchange-wrapped aggregate input"),
			(false, "bare aggregate input"),
		] {
			let (streaming_executor, streaming_plan, streaming_requested_tables) =
				build_plan(low_budget, wrap_aggregate_input);
			let streamed = streaming_executor
				.execute_bound(&streaming_plan, None)
				.await
				.expect("generic base should consume streamed stage runtime");
			let dim1_scan_count = streaming_requested_tables
				.lock()
				.expect("requested tables")
				.iter()
				.filter(|table| **table == TableId(3))
				.count();
			assert_eq!(
				dim1_scan_count, 1,
				"streamed generic base should build the partition reservoir from the scan once for {shape}"
			);

			let (materialized_executor, materialized_plan, _) =
				build_plan(high_test_budget(), wrap_aggregate_input);
			let materialized = materialized_executor
				.execute_bound(&materialized_plan, None)
				.await
				.expect("materialized baseline");

			assert_eq!(
				canonical_arrow_ipc_digest(&streamed, SortPolicy::FullRowLex)
					.expect("streamed digest"),
				canonical_arrow_ipc_digest(&materialized, SortPolicy::FullRowLex)
					.expect("materialized digest"),
				"generic base traversal must feed non-in-memory stage runtimes through the streaming probe for {shape}"
			);
		}
	}

	#[test]
	fn reoriented_grouped_required_columns_cover_identity_and_projected_inputs() {
		let group_expr = bound_column_root("group_key", ColumnOrdinal(2), DataType::Int64, false);
		let aggr_arg = bound_column_root("amount", ColumnOrdinal(5), DataType::Int64, true);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: aggr_arg,
		}];
		let outer_predicate = less_than_filter_program(ColumnOrdinal(7), 10);

		let identity_required = reoriented_grouped_required_output_columns(
			None,
			&group_expr,
			&aggr_expr,
			Some(&outer_predicate),
		);
		assert_eq!(
			identity_required,
			BTreeSet::from([ColumnOrdinal(2), ColumnOrdinal(5), ColumnOrdinal(7)])
		);

		let mut builder = BoundExprProgramBuilder::new();
		let projected_a = builder.append_column(ResolvedColumn {
			name: ColumnName("projected_a".to_string()),
			ordinal: ColumnOrdinal(1),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let projected_b = builder.append_column(ResolvedColumn {
			name: ColumnName("projected_b".to_string()),
			ordinal: ColumnOrdinal(4),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		builder.push_root(projected_a);
		builder.push_root(projected_b);
		let project_exprs = builder.build();

		let projected_required = reoriented_grouped_required_output_columns(
			Some(&project_exprs),
			&group_expr,
			&aggr_expr,
			None,
		);
		assert_eq!(
			projected_required,
			BTreeSet::from([ColumnOrdinal(1), ColumnOrdinal(4)])
		);
	}

	fn less_than_filter_program(ordinal: ColumnOrdinal, upper: i64) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let value = builder.append_column(ResolvedColumn {
			name: ColumnName("value".to_string()),
			ordinal,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let upper = builder.append_literal(ScalarValue::Int64(Some(upper)));
		let predicate = builder.append_binary(BoundBinaryOp::Lt, value, upper, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	fn high_test_budget() -> TaskBudget {
		TaskBudget {
			memory: ByteCount::new(128 * 1024 * 1024),
			spill_bytes: ByteCount::new(256 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		}
	}

	fn partition_parallel_batch(
		keys: &[i64],
		payload_len: usize,
		schema: SchemaRef,
	) -> RecordBatch {
		let payload = "p".repeat(payload_len);
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(keys.to_vec())),
				Arc::new(StringArray::from(vec![payload.as_str(); keys.len()])),
			],
		)
		.expect("partition parallel batch")
	}

	#[test]
	fn active_group_partitioning_matches_all_partitions_filtered() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80])) as ArrayRef,
				Arc::new(StringArray::from(vec![
					"p0", "p2a", "p3a", "skip", "p1", "p2b", "p4", "p3b",
				])) as ArrayRef,
			],
		)
		.expect("partition test batch");
		let buckets = vec![
			Some(0),
			Some(2),
			Some(3),
			None,
			Some(1),
			Some(2),
			Some(4),
			Some(3),
		];
		let all = partition_streaming_join_batch_with_buckets(&batch, &buckets, 5)
			.expect("all partitions");
		let expected = all
			.into_iter()
			.filter(|(partition, _)| (2..4).contains(partition))
			.collect::<Vec<_>>();
		let active =
			partition_streaming_join_batch_active_group_with_buckets(&batch, &buckets, 5, 2, 4)
				.expect("active partitions");

		assert_eq!(
			active
				.iter()
				.map(|(partition, _)| *partition)
				.collect::<Vec<_>>(),
			vec![2, 3]
		);
		assert_eq!(
			active
				.iter()
				.map(|(partition, _)| *partition)
				.collect::<Vec<_>>(),
			expected
				.iter()
				.map(|(partition, _)| *partition)
				.collect::<Vec<_>>()
		);
		for ((active_partition, active_batch), (expected_partition, expected_batch)) in
			active.iter().zip(expected.iter())
		{
			assert_eq!(active_partition, expected_partition);
			assert_record_batches_equal(&[active_batch.clone()], &[expected_batch.clone()]);
		}

		let empty =
			partition_streaming_join_batch_active_group_with_buckets(&batch, &buckets, 5, 1, 1)
				.expect("empty active group");
		assert!(empty.is_empty());
	}

	#[test]
	fn loaded_partition_work_is_send_static() {
		fn assert_send_static<T: Send + 'static>() {}
		assert_send_static::<LoadedPartitionWork>();
	}

	#[tokio::test]
	async fn parallel_partition_plan_uses_active_group_max_not_first_partition() {
		let budget = high_test_budget();
		let spill_dir = std::env::temp_dir().join(format!(
			"toron_parallel_partition_skew_{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&spill_dir);
		std::fs::create_dir_all(&spill_dir).expect("spill dir");
		let schema = Arc::new(Schema::new(vec![
			Field::new("k", DataType::Int64, false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let mut small =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("small spill");
		let mut large =
			SpilledInnerJoinBuild::create(budget, Arc::new(BlockingFileOps), spill_dir.clone())
				.await
				.expect("large spill");
		let keys = [ColumnOrdinal(0)];
		small
			.write_batch_chunks_unindexed(
				&partition_parallel_batch(&[1], 1, schema.clone()),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("small write");
		large
			.write_batch_chunks_unindexed(
				&partition_parallel_batch(&(0..256).collect::<Vec<_>>(), 2048, schema.clone()),
				JoinKeyPlan::Int64,
				&keys,
				false,
				budget,
			)
			.await
			.expect("large write");
		let probe = partition_parallel_batch(&[1], 1, schema);
		let small_est = estimate_parallel_partition_resident(&small, &probe, JoinKeyPlan::Int64);
		let large_est = estimate_parallel_partition_resident(&large, &probe, JoinKeyPlan::Int64);
		assert!(
			large_est > small_est,
			"fixture must put the larger partition after the first"
		);

		let builds = vec![Some(small), Some(large)];
		let plan = parallel_partition_plan(
			&builds,
			&[(0, probe.clone()), (1, probe)],
			JoinKeyPlan::Int64,
			8,
			budget.memory.get(),
		);
		assert_eq!(
			plan.resident_peak, large_est,
			"K must be calibrated from the active-group maximum, not the first partition"
		);
		assert!(plan.k >= 1);

		for spilled in builds.into_iter().flatten() {
			spilled.cleanup().await;
		}
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	#[tokio::test]
	async fn parallel_partitioned_scan_build_matches_sequential_ordered_emit() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, false),
			Field::new("build_payload", DataType::Utf8, false),
		]));
		let probe_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_payload", DataType::Utf8, false),
		]));
		let build_batch =
			partition_parallel_batch(&[1, 2, 3, 4, 5, 6, 7, 8], 8, build_schema.clone());
		let probe_batch =
			partition_parallel_batch(&[8, 1, 6, 3, 2, 7, 4, 5], 4, probe_schema.clone());
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([(TableId(9), vec![build_batch])]),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		});
		let scan = test_scan_plan_with_first_field_key(TableId(9), build_schema.clone(), 9);
		let materialized_output_schema = concat_test_schema(&build_schema, &probe_schema);
		let output_schema = concat_test_schema(&probe_schema, &build_schema);
		let budget = high_test_budget();
		let sequential = Executor::with_context(
			storage.clone(),
			ExecutionContext::local_test()
				.with_task_budget(budget)
				.with_partition_parallelism(1),
		);
		let parallel = Executor::with_context(
			storage,
			ExecutionContext::local_test()
				.with_task_budget(budget)
				.with_partition_parallelism(4),
		);
		let scan_input = StreamingScanProbeInput {
			scan: &scan,
			filter_predicate: None,
			exchange: None,
		};

		let (sequential_output, sequential_probe_rows, sequential_output_rows, _) = sequential
			.probe_partitioned_streamed_inner_scan_build(
				scan_input,
				None,
				4,
				1,
				&[ColumnOrdinal(0)],
				&[ColumnOrdinal(0)],
				&probe_batch,
				&[ColumnOrdinal(0)],
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await
			.expect("sequential partitioned probe");
		let (parallel_output, parallel_probe_rows, parallel_output_rows, _) = parallel
			.probe_partitioned_streamed_inner_scan_build(
				scan_input,
				None,
				4,
				1,
				&[ColumnOrdinal(0)],
				&[ColumnOrdinal(0)],
				&probe_batch,
				&[ColumnOrdinal(0)],
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await
			.expect("parallel partitioned probe");

		assert_eq!(parallel_probe_rows, sequential_probe_rows);
		assert_eq!(parallel_output_rows, sequential_output_rows);
		assert_record_batches_equal(&parallel_output, &sequential_output);
		assert_eq!(
			canonical_arrow_ipc_digest(&parallel_output, SortPolicy::None)
				.expect("parallel digest"),
			canonical_arrow_ipc_digest(&sequential_output, SortPolicy::None)
				.expect("sequential digest")
		);
	}

	#[tokio::test]
	async fn sequential_partitioned_scan_build_loads_spill_pages_once_for_repeated_matches() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, false),
			Field::new("build_payload", DataType::Utf8, false),
		]));
		let probe_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_payload", DataType::Utf8, false),
		]));
		let build_rows = CROSS_JOIN_OUTPUT_CHUNK_ROWS * 2;
		let build_keys = vec![1; build_rows];
		let build_batch = partition_parallel_batch(&build_keys, 1, build_schema.clone());
		let probe_batch = partition_parallel_batch(&[1, 1, 1, 1], 1, probe_schema.clone());
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([(TableId(10), vec![build_batch])]),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		});
		let spill_dir = std::env::temp_dir().join(format!(
			"toron_loaded_partition_once_{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&spill_dir);
		let file_ops = Arc::new(CountingFileOps::new());
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test()
				.with_task_budget(high_test_budget())
				.with_partition_parallelism(1)
				.with_join_spill_store(file_ops.clone(), spill_dir.clone()),
		);
		let scan = test_scan_plan_with_first_field_key(TableId(10), build_schema.clone(), 10);
		let materialized_output_schema = concat_test_schema(&build_schema, &probe_schema);
		let output_schema = concat_test_schema(&probe_schema, &build_schema);

		let (output, probe_rows, output_rows, observed_build_rows) = executor
			.probe_partitioned_streamed_inner_scan_build(
				StreamingScanProbeInput {
					scan: &scan,
					filter_predicate: None,
					exchange: None,
				},
				None,
				1,
				1,
				&[ColumnOrdinal(0)],
				&[ColumnOrdinal(0)],
				&probe_batch,
				&[ColumnOrdinal(0)],
				JoinKeyPlan::Int64,
				false,
				&materialized_output_schema,
				&output_schema,
				true,
				build_schema.fields().len(),
				None,
			)
			.await
			.expect("sequential loaded partition probe");

		assert_eq!(observed_build_rows, build_rows as u64);
		assert_eq!(probe_rows, probe_batch.num_rows() as u64);
		assert_eq!(
			output_rows,
			(build_rows as u64).saturating_mul(probe_batch.num_rows() as u64)
		);
		assert_eq!(
			output.iter().map(RecordBatch::num_rows).sum::<usize>(),
			output_rows as usize
		);
		assert_eq!(
			file_ops.read_plain_calls(),
			2,
			"the resident-safe sequential branch must decode each spilled build page once, not once per output chunk"
		);
		let _ = std::fs::remove_dir_all(spill_dir);
	}

	fn filtered_scan_left_semi_grouped_aggregate_fixture(
		rows: usize,
		payload_len: usize,
		budget: TaskBudget,
	) -> (Executor, ExecutionPlan) {
		let left_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
			Field::new("quantity", DataType::Decimal128(20, 2), false),
			Field::new("payload", DataType::Utf8, false),
		]));
		let right_schema = Arc::new(Schema::new(vec![
			Field::new("pk", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
		]));
		let payload = "x".repeat(payload_len);
		let left_keys = (0..rows)
			.map(|value| i64::try_from(value).expect("row fits i64"))
			.collect::<Vec<_>>();
		let quantity = Decimal128Array::from(
			(0..rows)
				.map(|row| Some(((row % 11) as i128 + 1) * 100))
				.collect::<Vec<_>>(),
		)
		.with_precision_and_scale(20, 2)
		.expect("quantity decimal");
		let left_batch = RecordBatch::try_new(
			left_schema.clone(),
			vec![
				Arc::new(Int64Array::from(left_keys.clone())),
				Arc::new(Int64Array::from(left_keys.clone())),
				Arc::new(quantity),
				Arc::new(StringArray::from(vec![payload.as_str(); rows])),
			],
		)
		.expect("filtered scan preserved batch");
		let right_keys = (0..rows)
			.flat_map(|value| {
				let value = i64::try_from(value).expect("row fits i64");
				[value, value]
			})
			.collect::<Vec<_>>();
		let right_batch = RecordBatch::try_new(
			right_schema.clone(),
			vec![
				Arc::new(Int64Array::from(
					(0..right_keys.len())
						.map(|value| i64::try_from(value).expect("row fits i64"))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Int64Array::from(right_keys)),
			],
		)
		.expect("filtered scan existence batch");
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::from([
				(TableId(1), vec![left_batch]),
				(TableId(2), vec![right_batch]),
			]),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		});
		let executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(budget),
		);
		let semi = left_semi_join_plan(
			test_scan_plan_with_first_field_key(TableId(1), left_schema.clone(), 1),
			test_scan_plan_with_first_field_key(TableId(2), right_schema, 2),
			vec![ColumnOrdinal(1)],
			vec![ColumnOrdinal(1)],
			left_schema,
		);
		let filtered = ExecutionPlan::Filter {
			input: Box::new(semi),
			predicate: less_than_filter_program(ColumnOrdinal(0), 4),
			disposition: FilterDisposition::EvaluateInToron,
		};
		let group_expr = bound_column_root("order_key", ColumnOrdinal(1), DataType::Int64, false);
		let aggr_expr = vec![BoundAggregateCall {
			function: AggregateFunction::Sum,
			args: bound_column_root(
				"quantity",
				ColumnOrdinal(2),
				DataType::Decimal128(20, 2),
				false,
			),
		}];
		let output_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("sum_quantity", DataType::Decimal128(20, 2), true),
		]));
		let plan = ExecutionPlan::Aggregate {
			input: Box::new(exchange_plan(filtered, group_expr.clone(), 1)),
			group_expr,
			aggr_expr,
			output_schema,
		};
		(executor, plan)
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
			&[TableId(2), TableId(2), TableId(1)]
		);
	}

	#[tokio::test]
	async fn scan_filtered_left_semi_grouped_aggregate_bypasses_scan_output_accumulator() {
		let rows = 512usize;
		let low_budget = TaskBudget {
			memory: ByteCount::new(64 * 1024),
			spill_bytes: ByteCount::new(64 * 1024 * 1024),
			..TaskBudget::from_query_budget(QueryBudget::test_default())
		};
		let (stream_executor, plan) =
			filtered_scan_left_semi_grouped_aggregate_fixture(rows, 4096, low_budget);
		let streamed = stream_executor
			.execute_bound(&plan, None)
			.await
			.expect("streamed scan left-semi grouped aggregate");

		let (baseline_executor, baseline_plan) =
			filtered_scan_left_semi_grouped_aggregate_fixture(rows, 4096, high_test_budget());
		let materialized = baseline_executor
			.execute_bound(&baseline_plan, None)
			.await
			.expect("materialized scan left-semi grouped aggregate");

		assert_eq!(
			canonical_arrow_ipc_digest(&streamed, SortPolicy::None).expect("streamed digest"),
			canonical_arrow_ipc_digest(&materialized, SortPolicy::None)
				.expect("materialized digest"),
			"streamed scan left-semi grouped aggregate must match materialized output"
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
			&[TableId(2), TableId(2), TableId(1)]
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
				.probe_reoriented_inner_join_streaming(&mut materialized_runtime, &probe)
				.await
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
				&[TableId(3), TableId(3), TableId(3), TableId(3)],
				"streamed scan build reads once for total estimation, once for per-partition spill sizing, once for the live group, and once for the materialized comparison path"
			);

			streamed_runtime.cleanup().await;
			materialized_runtime.cleanup().await;
		}
	}

	#[tokio::test]
	async fn reoriented_materialized_build_over_cap_spills_compact_from_first_batch() {
		let build_schema = Arc::new(Schema::new(vec![
			Field::new("build_key", DataType::Int64, false),
			Field::new("build_value", DataType::Int64, false),
		]));
		let probe_schema = Arc::new(Schema::new(vec![
			Field::new("probe_key", DataType::Int64, false),
			Field::new("probe_tag", DataType::Int64, false),
		]));
		let output_schema = concat_test_schema(&probe_schema, &build_schema);
		let first_rows = 5_000;
		let second_rows = 5_000;
		let mut first_keys = vec![1; first_rows];
		first_keys[first_rows - 1] = 2;
		let mut second_keys = vec![1; second_rows];
		second_keys[second_rows - 1] = 2;
		let build_a = RecordBatch::try_new(
			build_schema.clone(),
			vec![
				Arc::new(Int64Array::from(first_keys)),
				Arc::new(Int64Array::from_iter_values(0..first_rows as i64)),
			],
		)
		.expect("build batch a");
		let build_b = RecordBatch::try_new(
			build_schema.clone(),
			vec![
				Arc::new(Int64Array::from(second_keys)),
				Arc::new(Int64Array::from_iter_values(
					first_rows as i64..(first_rows + second_rows) as i64,
				)),
			],
		)
		.expect("build batch b");
		let probe = RecordBatch::try_new(
			probe_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![2])),
				Arc::new(Int64Array::from(vec![10])),
			],
		)
		.expect("probe batch");
		let build = test_scan_plan_with_first_field_key(TableId(30), build_schema.clone(), 30);
		let storage = Arc::new(RecordingMultiBatchTableStorageScan {
			batches: HashMap::new(),
			requested_tables: Arc::new(Mutex::new(Vec::new())),
		});
		let low_budget_executor = Executor::with_context(
			storage.clone(),
			ExecutionContext::local_test().with_task_budget(TaskBudget {
				memory: ByteCount::new(64 * 1024),
				spill_bytes: ByteCount::new(64 * 1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			}),
		);
		let mut spilled_runtime = low_budget_executor
			.prepare_reoriented_inner_join_runtime_from_build_batches(
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone()),
				vec![build_a.clone(), build_b.clone()],
			)
			.await
			.expect("spilled materialized runtime")
			.expect("runtime");
		let ReorientedInnerJoinBuild::Spilled { build: spilled, .. } = &spilled_runtime.build
		else {
			panic!("over-cap materialized build must spill instead of erroring");
		};
		assert_eq!(
			spilled.input_batches_written(),
			2,
			"already-materialized build batches must be spilled batch-by-batch, not concat-first"
		);
		assert!(
			spilled.uses_compact_int64_index(),
			"the reoriented materialized spill path must select the compact Int64 index"
		);
		assert_eq!(
			spilled.legacy_index_insert_count(),
			0,
			"the compact path must be selected before the first build-row append"
		);
		assert_eq!(
			spilled.resident_index_bytes(),
			spilled.computed_index_bytes()
		);
		assert_eq!(spilled.input_row_count(), (first_rows + second_rows) as u64);

		let high_budget_executor = Executor::with_context(
			storage,
			ExecutionContext::local_test().with_task_budget(high_test_budget()),
		);
		let mut in_memory_runtime = high_budget_executor
			.prepare_reoriented_inner_join_runtime_from_build_batches(
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone()),
				vec![build_a, build_b],
			)
			.await
			.expect("in-memory materialized runtime")
			.expect("runtime");
		assert!(
			matches!(
				in_memory_runtime.build,
				ReorientedInnerJoinBuild::InMemory { .. }
			),
			"high-budget comparison path should still use the in-memory accumulator"
		);

		let spilled_output = low_budget_executor
			.probe_reoriented_inner_join_streaming(&mut spilled_runtime, &probe)
			.await
			.expect("spilled probe");
		let in_memory_output = high_budget_executor
			.probe_reoriented_inner_join_streaming(&mut in_memory_runtime, &probe)
			.await
			.expect("in-memory probe");
		assert_record_batches_equal(&spilled_output, &in_memory_output);
		assert_eq!(
			spilled_output
				.iter()
				.map(RecordBatch::num_rows)
				.sum::<usize>(),
			2
		);
		assert_eq!(
			spilled_output
				.first()
				.expect("spilled output")
				.schema()
				.as_ref(),
			output_schema.as_ref(),
			"spilled materialized build must restore probe-first column order"
		);

		spilled_runtime.cleanup().await;
		in_memory_runtime.cleanup().await;
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
				.probe_reoriented_inner_join_streaming(&mut materialized_runtime, &probe)
				.await
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

	#[test]
	fn streamed_inner_build_partition_count_uses_effective_resident_cap() {
		let memory_cap = 64 * 1024 * 1024;
		let estimated_build_bytes = memory_cap * 2;
		let spill_budget = 512 * 1024 * 1024;
		let raw_cap_partitions =
			streamed_spill_partition_count(estimated_build_bytes, memory_cap).unwrap();
		let retained_cap_partitions =
			streamed_inner_build_partition_count(estimated_build_bytes, memory_cap, spill_budget)
				.unwrap();

		assert!(
			retained_cap_partitions < raw_cap_partitions,
			"streamed inner build partitioning must size against the effective retained-footprint cap, not the raw task-memory input"
		);
		assert_eq!(retained_cap_partitions, 1);
	}

	#[test]
	fn streamed_inner_build_partitioning_uses_spill_budget_and_ordered_groups() {
		let memory_cap = 4_u64 * 1024 * 1024 * 1024;
		let spill_budget = 512_u64 * 1024 * 1024;
		let estimated_build_bytes = spill_budget + 1;

		let partition_count =
			streamed_inner_build_partition_count(estimated_build_bytes, memory_cap, spill_budget)
				.expect("partition count");
		let group_count =
			streamed_inner_build_group_count(estimated_build_bytes, spill_budget, partition_count)
				.expect("group count");

		assert_eq!(
			partition_count, 8,
			"spill budget should raise P even when the memory cap alone would not"
		);
		assert_eq!(
			group_count, 2,
			"live spill grouping should bound aggregate live spill without scanning once per writer partition"
		);
		assert_eq!(
			partition_group_bounds(partition_count, group_count, 0).expect("group 0"),
			0..4
		);
		assert_eq!(
			partition_group_bounds(partition_count, group_count, 1).expect("group 1"),
			4..8
		);
	}

	#[test]
	fn streamed_inner_build_group_count_uses_max_group_spill_not_average() {
		let spill_budget = 1024;
		let group_spill_budget = streamed_inner_build_group_spill_budget(spill_budget);
		let overhead_reserve = streamed_inner_build_group_overhead_reserve_bytes(spill_budget);
		assert_eq!(group_spill_budget, 768);
		assert_eq!(overhead_reserve, 256);

		let partition_bytes = vec![400, 400, 10, 10];
		assert!(
			partition_bytes.iter().sum::<u64>() / 2 <= group_spill_budget,
			"average two-group sizing would look safe"
		);
		assert!(
			max_partition_group_spill_bytes(&partition_bytes, 2).expect("two groups")
				> group_spill_budget,
			"but the max active group is not safe"
		);

		let (group_count, max_group_spill) =
			streamed_inner_build_group_count_for_partition_spill_bytes(
				&partition_bytes,
				spill_budget,
			)
			.expect("max-bounded group count");
		assert_eq!(group_count, 3);
		assert_eq!(max_group_spill, 400);
		assert!(max_group_spill <= group_spill_budget);
	}

	#[test]
	fn streamed_inner_build_group_count_names_skew_floor_when_single_partition_exceeds_budget() {
		let spill_budget = 1024;
		let group_spill_budget = streamed_inner_build_group_spill_budget(spill_budget);
		let partition_bytes = vec![group_spill_budget + 1, 1, 1, 1];
		let error = streamed_inner_build_group_count_for_partition_spill_bytes(
			&partition_bytes,
			spill_budget,
		)
		.expect_err("hot partition must not be claimed bounded");

		match error {
			ExecutionError::BudgetExhausted {
				dimension,
				producer,
				requested,
				available,
				..
			} => {
				assert_eq!(dimension, BudgetDimension::SpillBytes);
				assert_eq!(producer, "streamed inner join build partition group sizing");
				assert_eq!(requested.get(), group_spill_budget + 1);
				assert_eq!(available.get(), group_spill_budget);
			}
			other => panic!("expected budget exhausted skew floor, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn partitioned_reoriented_inner_runtime_matches_single_spill_projected_path() {
		for value_kind in [StreamingValueKind::Float64, StreamingValueKind::Decimal] {
			let rows = 8_192usize;
			let value_type = value_kind.data_type();
			let build_schema = Arc::new(Schema::new(vec![
				Field::new("dead_payload", DataType::Utf8, false),
				Field::new("build_key", DataType::Int64, false),
				Field::new("build_value", value_type.clone(), true),
			]));
			let probe_schema = Arc::new(Schema::new(vec![
				Field::new("probe_key", DataType::Int64, false),
				Field::new("probe_tag", DataType::Int64, false),
			]));
			let output_schema = concat_test_schema(&probe_schema, &build_schema);
			let build_keys = (0..rows)
				.map(|row| i64::try_from(row).expect("build key fits"))
				.collect::<Vec<_>>();
			let build_values = (0..rows)
				.map(|row| i128::try_from(row * 10).expect("value fits"))
				.collect::<Vec<_>>();
			let dead_payload = (0..rows)
				.map(|row| format!("dead-{row}"))
				.collect::<Vec<_>>();
			let build_batch = RecordBatch::try_new(
				build_schema.clone(),
				vec![
					Arc::new(StringArray::from(dead_payload)),
					Arc::new(Int64Array::from(build_keys)),
					value_kind.values(&build_values),
				],
			)
			.expect("partitioned build batch");
			let build_batches = vec![
				build_batch.slice(0, rows / 2),
				build_batch.slice(rows / 2, rows / 2),
			];
			let probe_rows = 128usize;
			let probe_keys = (0..probe_rows)
				.rev()
				.map(|row| i64::try_from(row).expect("probe key fits"))
				.collect::<Vec<_>>();
			let probe_tags = (0..probe_rows)
				.rev()
				.map(|row| i64::try_from(row * 3).expect("probe tag fits"))
				.collect::<Vec<_>>();
			let probe = RecordBatch::try_new(
				probe_schema.clone(),
				vec![
					Arc::new(Int64Array::from(probe_keys)),
					Arc::new(Int64Array::from(probe_tags)),
				],
			)
			.expect("partitioned probe batch");
			let scan = test_scan_plan_with_first_field_key(TableId(5), build_schema.clone(), 5);
			let build = ExecutionPlan::Exchange {
				input: Box::new(scan),
				partition_count: 1,
				key: bound_column_root("build_key", ColumnOrdinal(1), DataType::Int64, false),
			};
			let mut stage =
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone());
			stage.build_keys = vec![ColumnOrdinal(1)];
			stage.retained_build_columns = Some(vec![ColumnOrdinal(1), ColumnOrdinal(2)]);

			let partitioned_requested_tables = Arc::new(Mutex::new(Vec::new()));
			let partitioned_executor = Executor::with_context(
				Arc::new(RecordingMultiBatchTableStorageScan {
					batches: HashMap::from([(TableId(5), build_batches.clone())]),
					requested_tables: partitioned_requested_tables.clone(),
				}),
				ExecutionContext::local_test().with_task_budget(TaskBudget {
					memory: ByteCount::new(64 * 1024),
					spill_bytes: ByteCount::new(64 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				}),
			);
			let mut partitioned_runtime = partitioned_executor
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(stage, None)
				.await
				.expect("partitioned runtime")
				.expect("partitioned scan build should prepare");
			let (partition_count, group_count) = match &partitioned_runtime.build {
				ReorientedInnerJoinBuild::PartitionedScan {
					group_count,
					partition_count,
					..
				} => (*partition_count, *group_count),
				_ => panic!("low-budget scan build should use the partitioned runtime"),
			};
			assert!(
				partition_count > 1,
				"test fixture must force more than one active build partition"
			);
			assert!(
				group_count >= 1,
				"partitioned runtime must record at least one live partition group"
			);

			let mut single_stage =
				reoriented_scan_build_stage(&build, probe_schema.clone(), output_schema.clone());
			single_stage.build_keys = vec![ColumnOrdinal(1)];
			single_stage.retained_build_columns = Some(vec![ColumnOrdinal(1), ColumnOrdinal(2)]);
			let single_executor = Executor::with_context(
				Arc::new(RecordingMultiBatchTableStorageScan {
					batches: HashMap::from([(TableId(5), build_batches)]),
					requested_tables: Arc::new(Mutex::new(Vec::new())),
				}),
				ExecutionContext::local_test().with_task_budget(TaskBudget {
					memory: ByteCount::new(256 * 1024 * 1024),
					spill_bytes: ByteCount::new(64 * 1024 * 1024),
					..TaskBudget::from_query_budget(QueryBudget::test_default())
				}),
			);
			let mut single_runtime = single_executor
				.prepare_reoriented_inner_join_runtime_with_streamed_scan_build(single_stage, None)
				.await
				.expect("single-spill runtime")
				.expect("single-spill scan build should prepare");
			let ReorientedInnerJoinBuild::Spilled { .. } = &single_runtime.build else {
				panic!("high-budget scan build should stay on the single-spill runtime");
			};

			let partitioned = partitioned_executor
				.probe_reoriented_inner_join_streaming(&mut partitioned_runtime, &probe)
				.await
				.expect("partitioned probe");
			let single = single_executor
				.probe_reoriented_inner_join_streaming(&mut single_runtime, &probe)
				.await
				.expect("single-spill probe");
			assert_eq!(
				partitioned.first().expect("partitioned output").schema(),
				single.first().expect("single output").schema()
			);
			assert_eq!(
				canonical_arrow_ipc_digest(&partitioned, SortPolicy::FullRowLex)
					.expect("partitioned digest"),
				canonical_arrow_ipc_digest(&single, SortPolicy::FullRowLex)
					.expect("single-spill digest"),
				"partitioned streamed inner build must preserve the same rows and sparse schema restoration as the single-spill projected path"
			);
			let expected_scan_count =
				2 + usize::try_from(group_count).expect("group count fits usize");
			let expected_tables = vec![TableId(5); expected_scan_count];
			assert_eq!(
				partitioned_requested_tables
					.lock()
					.expect("partitioned requested tables")
					.as_slice(),
				expected_tables.as_slice(),
				"partitioned scan build should scan once for total estimation, once for per-partition group sizing, and once per live partition group, not once per writer partition"
			);

			partitioned_runtime.cleanup().await;
			single_runtime.cleanup().await;
		}
	}

	#[tokio::test]
	async fn adaptive_derived_last_stage_build_scans_leaf_once_and_restores_order() {
		let customer_schema = Arc::new(Schema::new(vec![
			Field::new("cust_key", DataType::Int64, false),
			Field::new("cust_tag", DataType::Int64, false),
		]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("order_custkey", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
		]));
		let line_schema = Arc::new(Schema::new(vec![
			Field::new("line_orderkey", DataType::Int64, false),
			Field::new("line_value", DataType::Int64, false),
		]));
		let base_schema = concat_test_schema(&customer_schema, &orders_schema);
		let output_schema = concat_test_schema(&base_schema, &line_schema);
		let customer = RecordBatch::try_new(
			customer_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])),
				Arc::new(Int64Array::from(vec![10, 20])),
			],
		)
		.expect("customer batch");
		let orders = RecordBatch::try_new(
			orders_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])),
				Arc::new(Int64Array::from(vec![100, 200, 300])),
			],
		)
		.expect("orders batch");
		let lineitem = RecordBatch::try_new(
			line_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![200, 100, 999])),
				Arc::new(Int64Array::from(vec![2_000, 1_000, 9_990])),
			],
		)
		.expect("lineitem batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([
					(TableId(20), vec![customer]),
					(TableId(21), vec![orders]),
					(TableId(22), vec![lineitem]),
				]),
				requested_tables: requested_tables.clone(),
			}),
			ExecutionContext::local_test().with_task_budget(high_test_budget()),
		);

		let customer_scan =
			test_scan_plan_with_first_field_key(TableId(20), customer_schema.clone(), 20);
		let orders_scan = test_scan_plan_with_first_field_key(TableId(21), orders_schema, 21);
		let base_join = inner_join_plan(
			customer_scan,
			orders_scan,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			base_schema.clone(),
		);
		let base = exchange_plan(
			base_join,
			bound_column_root("order_key", ColumnOrdinal(3), DataType::Int64, false),
			1,
		);
		let line_scan = test_scan_plan_with_first_field_key(TableId(22), line_schema.clone(), 22);
		let line_exchange = exchange_plan(
			line_scan,
			bound_column_root("line_orderkey", ColumnOrdinal(0), DataType::Int64, false),
			1,
		);
		let scan_input =
			decompose_streaming_scan_probe_input(&line_exchange).expect("line scan input");
		let mut runtime = ReorientedInnerJoinRuntime {
			replay_id: JoinReplayId(1),
			strategy: JoinStrategy::HashPartitioned,
			build: ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel: None,
				partition_count: 4,
				group_count: 4,
				partition_build_keys: vec![ColumnOrdinal(0)],
				write_build_keys: vec![ColumnOrdinal(0)],
				materialized_output_schema: concat_test_schema(&line_schema, &base_schema),
				build_column_count: line_schema.fields().len(),
				projection: None,
				estimated_build_bytes: u64::MAX,
				observed_build_rows: 0,
				loaded_builds: (0..4).map(|_| None).collect(),
				loaded_resident_bytes: 0,
				loaded_build_attempted: false,
			},
			key_plan: JoinKeyPlan::Int64,
			probe_keys: vec![ColumnOrdinal(3)],
			output_schema: output_schema.clone(),
			post_filter: None,
			probe_columns_first: true,
			probe_rows: 0,
			output_rows: 0,
		};

		let mut output = Vec::new();
		let matched = executor
			.try_visit_reoriented_adaptive_derived_last_stage(
				&base,
				&mut runtime,
				None,
				None,
				&mut |batch| {
					output.push(batch);
					Ok(())
				},
			)
			.await
			.expect("adaptive derived build");
		assert!(matched.is_some(), "fitting derived side should flip");
		assert_eq!(runtime.build_row_count(), 2);
		assert_eq!(runtime.probe_rows, 3);
		assert_eq!(runtime.output_rows, 2);
		let requested = requested_tables.lock().expect("requested tables");
		assert_eq!(
			requested
				.iter()
				.filter(|table| **table == TableId(22))
				.count(),
			1,
			"adaptive flip must scan the terminal leaf side exactly once"
		);
		let output = arrow::compute::concat_batches(&output_schema, output.iter())
			.expect("adaptive output concat");
		assert_eq!(int64_column_values(&output, 0), vec![2, 1]);
		assert_eq!(int64_column_values(&output, 3), vec![200, 100]);
		assert_eq!(int64_column_values(&output, 5), vec![2_000, 1_000]);
	}

	#[tokio::test]
	async fn adaptive_derived_last_stage_declines_when_measured_build_exceeds_cap() {
		let customer_schema = Arc::new(Schema::new(vec![Field::new(
			"cust_key",
			DataType::Int64,
			false,
		)]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("order_custkey", DataType::Int64, false),
			Field::new("order_key", DataType::Int64, false),
		]));
		let line_schema = Arc::new(Schema::new(vec![Field::new(
			"line_orderkey",
			DataType::Int64,
			false,
		)]));
		let base_schema = concat_test_schema(&customer_schema, &orders_schema);
		let output_schema = concat_test_schema(&base_schema, &line_schema);
		let rows = 8_192usize;
		let customer = RecordBatch::try_new(
			customer_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1]))],
		)
		.expect("customer batch");
		let orders = RecordBatch::try_new(
			orders_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![1; rows])),
				Arc::new(Int64Array::from_iter_values(
					0..i64::try_from(rows).expect("rows fit"),
				)),
			],
		)
		.expect("orders batch");
		let lineitem = RecordBatch::try_new(
			line_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![0]))],
		)
		.expect("lineitem batch");
		let requested_tables = Arc::new(Mutex::new(Vec::new()));
		let executor = Executor::with_context(
			Arc::new(RecordingMultiBatchTableStorageScan {
				batches: HashMap::from([
					(TableId(23), vec![customer]),
					(TableId(24), vec![orders]),
					(TableId(25), vec![lineitem]),
				]),
				requested_tables: requested_tables.clone(),
			}),
			ExecutionContext::local_test().with_task_budget(TaskBudget {
				memory: ByteCount::new(32 * 1024),
				spill_bytes: ByteCount::new(64 * 1024 * 1024),
				..TaskBudget::from_query_budget(QueryBudget::test_default())
			}),
		);
		let customer_scan =
			test_scan_plan_with_first_field_key(TableId(23), customer_schema.clone(), 23);
		let orders_scan = test_scan_plan_with_first_field_key(TableId(24), orders_schema, 24);
		let base_join = inner_join_plan(
			customer_scan,
			orders_scan,
			vec![ColumnOrdinal(0)],
			vec![ColumnOrdinal(0)],
			base_schema.clone(),
		);
		let base = exchange_plan(
			base_join,
			bound_column_root("order_key", ColumnOrdinal(2), DataType::Int64, false),
			1,
		);
		let line_scan = test_scan_plan_with_first_field_key(TableId(25), line_schema.clone(), 25);
		let line_exchange = exchange_plan(
			line_scan,
			bound_column_root("line_orderkey", ColumnOrdinal(0), DataType::Int64, false),
			1,
		);
		let scan_input =
			decompose_streaming_scan_probe_input(&line_exchange).expect("line scan input");
		let mut runtime = ReorientedInnerJoinRuntime {
			replay_id: JoinReplayId(1),
			strategy: JoinStrategy::HashPartitioned,
			build: ReorientedInnerJoinBuild::PartitionedScan {
				scan_input,
				morsel: None,
				partition_count: 4,
				group_count: 4,
				partition_build_keys: vec![ColumnOrdinal(0)],
				write_build_keys: vec![ColumnOrdinal(0)],
				materialized_output_schema: concat_test_schema(&line_schema, &base_schema),
				build_column_count: line_schema.fields().len(),
				projection: None,
				estimated_build_bytes: u64::MAX,
				observed_build_rows: 0,
				loaded_builds: (0..4).map(|_| None).collect(),
				loaded_resident_bytes: 0,
				loaded_build_attempted: false,
			},
			key_plan: JoinKeyPlan::Int64,
			probe_keys: vec![ColumnOrdinal(2)],
			output_schema,
			post_filter: None,
			probe_columns_first: true,
			probe_rows: 0,
			output_rows: 0,
		};
		let matched = executor
			.try_visit_reoriented_adaptive_derived_last_stage(
				&base,
				&mut runtime,
				None,
				None,
				&mut |_batch| Ok(()),
			)
			.await
			.expect("adaptive derived build");
		assert!(
			matched.is_none(),
			"oversized measured derived side must decline the adaptive flip"
		);
		assert_eq!(
			requested_tables
				.lock()
				.expect("requested tables")
				.iter()
				.filter(|table| **table == TableId(25))
				.count(),
			0,
			"declined flip must not scan the terminal leaf side"
		);
	}

	#[tokio::test]
	async fn projected_semi_anti_existence_build_keys_only_matches_unprojected() {
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("dead_payload", DataType::Utf8, false),
			Field::new("dead_value", DataType::Int64, false),
		]));
		let preserved_schema = Arc::new(Schema::new(vec![Field::new(
			"order_key",
			DataType::Int64,
			false,
		)]));
		let existence = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 3])),
				Arc::new(StringArray::from(vec!["unused-a", "unused-b"])),
				Arc::new(Int64Array::from(vec![10, 30])),
			],
		)
		.expect("existence batch");
		let preserved = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.expect("preserved batch");
		let preserved_by_key = HashMap::from([
			(JoinKey::Int64(1), vec![0]),
			(JoinKey::Int64(2), vec![1]),
			(JoinKey::Int64(3), vec![2]),
		]);
		let keys = vec![ColumnOrdinal(0)];
		let mut projected = projected_existence_spill(
			existence.clone(),
			preserved_schema.clone(),
			&keys,
			vec![ColumnOrdinal(0)],
		)
		.await;
		let page = projected.read_batch_page(0).await.expect("projected page");
		assert_eq!(
			page.num_columns(),
			1,
			"keys-only semi existence spill should not write dead payload columns"
		);
		let mut unprojected = unprojected_existence_spill(existence, preserved_schema, &keys).await;

		let projected_matches = projected
			.matching_preserved_rows(&preserved, &preserved_by_key, None)
			.await
			.expect("projected keys-only matches");
		let unprojected_matches = unprojected
			.matching_preserved_rows(&preserved, &preserved_by_key, None)
			.await
			.expect("unprojected keys-only matches");

		assert_eq!(projected_matches, unprojected_matches);
		assert_eq!(projected_matches, vec![true, false, true]);
		projected.cleanup().await;
		unprojected.cleanup().await;
	}

	#[tokio::test]
	async fn membership_only_projected_semi_anti_existence_writes_no_pages() {
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("dead_payload", DataType::Utf8, false),
		]));
		let preserved_schema = Arc::new(Schema::new(vec![Field::new(
			"order_key",
			DataType::Int64,
			false,
		)]));
		let existence = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 1, 3])),
				Arc::new(StringArray::from(vec!["unused-a", "unused-b", "unused-c"])),
			],
		)
		.expect("existence batch");
		let preserved = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.expect("preserved batch");
		let preserved_by_key = HashMap::from([
			(JoinKey::Int64(1), vec![0]),
			(JoinKey::Int64(2), vec![1]),
			(JoinKey::Int64(3), vec![2]),
		]);
		let keys = vec![ColumnOrdinal(0)];
		let mut projected = projected_membership_existence_spill(
			existence.clone(),
			preserved_schema.clone(),
			&keys,
			vec![ColumnOrdinal(0)],
		)
		.await;
		assert_eq!(
			projected.page_count(),
			0,
			"membership-only semi/anti existence should answer from the key index"
		);
		assert_eq!(
			projected.spill_bytes(),
			0,
			"membership-only semi/anti existence should not write page payloads"
		);
		let mut unprojected = unprojected_existence_spill(existence, preserved_schema, &keys).await;

		let projected_matches = projected
			.matching_preserved_rows(&preserved, &preserved_by_key, None)
			.await
			.expect("projected membership matches");
		let unprojected_matches = unprojected
			.matching_preserved_rows(&preserved, &preserved_by_key, None)
			.await
			.expect("unprojected membership matches");

		assert_eq!(projected_matches, unprojected_matches);
		assert_eq!(projected_matches, vec![true, false, true]);
		projected.cleanup().await;
		unprojected.cleanup().await;
	}

	#[tokio::test]
	async fn projected_semi_anti_existence_build_retains_residual_columns() {
		let existence_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("supp_key", DataType::Int64, false),
			Field::new("dead_payload", DataType::Utf8, false),
		]));
		let preserved_schema = Arc::new(Schema::new(vec![
			Field::new("order_key", DataType::Int64, false),
			Field::new("supp_key", DataType::Int64, false),
		]));
		let existence = RecordBatch::try_new(
			existence_schema,
			vec![
				Arc::new(Int64Array::from(vec![10, 10, 20])),
				Arc::new(Int64Array::from(vec![1, 2, 1])),
				Arc::new(StringArray::from(vec!["dead-a", "dead-b", "dead-c"])),
			],
		)
		.expect("existence batch");
		let preserved = RecordBatch::try_new(
			preserved_schema.clone(),
			vec![
				Arc::new(Int64Array::from(vec![10, 10, 20, 30])),
				Arc::new(Int64Array::from(vec![1, 2, 1, 9])),
			],
		)
		.expect("preserved batch");
		let preserved_by_key = HashMap::from([
			(JoinKey::Int64(10), vec![0, 1]),
			(JoinKey::Int64(20), vec![2]),
			(JoinKey::Int64(30), vec![3]),
		]);
		let keys = vec![ColumnOrdinal(0)];
		let mut projected = projected_existence_spill(
			existence.clone(),
			preserved_schema.clone(),
			&keys,
			vec![ColumnOrdinal(0), ColumnOrdinal(1)],
		)
		.await;
		let page = projected.read_batch_page(0).await.expect("projected page");
		assert_eq!(
			page.num_columns(),
			2,
			"residual semi/anti existence spill must keep key plus residual payload only"
		);
		let mut unprojected = unprojected_existence_spill(existence, preserved_schema, &keys).await;
		let residual = not_equal_residual_program(
			ColumnOrdinal(1),
			ColumnOrdinal(3),
			"preserved_supp",
			"existence_supp",
		);

		let projected_matches = projected
			.matching_preserved_rows(&preserved, &preserved_by_key, Some(&residual))
			.await
			.expect("projected residual matches");
		let unprojected_matches = unprojected
			.matching_preserved_rows(&preserved, &preserved_by_key, Some(&residual))
			.await
			.expect("unprojected residual matches");

		assert_eq!(projected_matches, unprojected_matches);
		assert_eq!(projected_matches, vec![true, true, false, false]);
		projected.cleanup().await;
		unprojected.cleanup().await;
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

	fn not_equal_residual_program(
		left_ordinal: ColumnOrdinal,
		right_ordinal: ColumnOrdinal,
		left_name: &str,
		right_name: &str,
	) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let left = builder.append_column(ResolvedColumn {
			name: ColumnName(left_name.to_string()),
			ordinal: left_ordinal,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let right = builder.append_column(ResolvedColumn {
			name: ColumnName(right_name.to_string()),
			ordinal: right_ordinal,
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		});
		let predicate = builder.append_binary(BoundBinaryOp::Neq, left, right, DataType::Boolean);
		builder.push_root(predicate);
		builder.build()
	}

	async fn projected_existence_spill(
		existence: RecordBatch,
		preserved_schema: SchemaRef,
		existence_keys: &[ColumnOrdinal],
		retained_columns: Vec<ColumnOrdinal>,
	) -> SpilledSemiAntiExistenceBuild {
		let context = ExecutionContext::local_test().with_task_budget(high_test_budget());
		let projected_schema = projected_schema_for_columns(&existence.schema(), &retained_columns)
			.expect("projected existence schema");
		let projected_keys =
			remap_column_ordinals(&retained_columns, existence_keys).expect("projected keys");
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&projected_schema,
			&projected_keys,
			&preserved_schema,
			existence_keys,
			false,
		)
		.expect("projected key plan");
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			context.task_budget,
			context.join_spill_file_ops.clone(),
			context.join_spill_dir.clone(),
		)
		.await
		.expect("projected existence spill");
		spilled
			.set_projection(existence.schema(), retained_columns)
			.expect("set projection");
		spilled
			.write_projected_batch_chunks(
				&existence,
				key_plan,
				&projected_keys,
				context.task_budget,
			)
			.await
			.expect("write projected existence");
		spilled
			.finish(context.task_budget)
			.await
			.expect("finish spill");
		spilled
	}

	async fn projected_membership_existence_spill(
		existence: RecordBatch,
		preserved_schema: SchemaRef,
		existence_keys: &[ColumnOrdinal],
		retained_columns: Vec<ColumnOrdinal>,
	) -> SpilledSemiAntiExistenceBuild {
		let context = ExecutionContext::local_test().with_task_budget(high_test_budget());
		let projected_schema = projected_schema_for_columns(&existence.schema(), &retained_columns)
			.expect("projected existence schema");
		let projected_keys =
			remap_column_ordinals(&retained_columns, existence_keys).expect("projected keys");
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&projected_schema,
			&projected_keys,
			&preserved_schema,
			existence_keys,
			false,
		)
		.expect("projected membership key plan");
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			context.task_budget,
			context.join_spill_file_ops.clone(),
			context.join_spill_dir.clone(),
		)
		.await
		.expect("projected membership existence spill");
		spilled
			.set_projection(existence.schema(), retained_columns)
			.expect("set projection");
		spilled
			.write_projected_membership_key_chunks(
				&existence,
				key_plan,
				&projected_keys,
				context.task_budget,
			)
			.expect("write projected membership existence");
		spilled
			.finish(context.task_budget)
			.await
			.expect("finish spill");
		spilled
	}

	async fn unprojected_existence_spill(
		existence: RecordBatch,
		preserved_schema: SchemaRef,
		existence_keys: &[ColumnOrdinal],
	) -> SpilledSemiAntiExistenceBuild {
		let context = ExecutionContext::local_test().with_task_budget(high_test_budget());
		let key_plan = JoinKeyPlan::from_build_schema_and_probe_schema(
			&existence.schema(),
			existence_keys,
			&preserved_schema,
			existence_keys,
			false,
		)
		.expect("unprojected key plan");
		let mut spilled = SpilledSemiAntiExistenceBuild::create(
			context.task_budget,
			context.join_spill_file_ops.clone(),
			context.join_spill_dir.clone(),
		)
		.await
		.expect("unprojected existence spill");
		spilled
			.write_batch_chunks(&existence, key_plan, existence_keys, context.task_budget)
			.await
			.expect("write unprojected existence");
		spilled
			.finish(context.task_budget)
			.await
			.expect("finish spill");
		spilled
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

	fn int64_column_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
		batch
			.column(column)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("int64 column")
			.iter()
			.map(|value| value.expect("non-null int64"))
			.collect()
	}
}
