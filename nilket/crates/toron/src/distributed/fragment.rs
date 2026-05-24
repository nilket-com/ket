use arrow::datatypes::SchemaRef;
use thiserror::Error;
use wendelon::schema::{PhysicalDescriptorFingerprint, TableObjectId};

use crate::distributed::budget::TaskBudget;
use crate::distributed::context::TaskExecutionContext;
use crate::distributed::identity::{
	AttemptId, ExchangeId, PartitionId, QueryId, StageId, TaskId, TaskIdentity,
};
use crate::distributed::scheduler::{StagePlan, TaskPlan};
use crate::distributed::task::{
	PartitionCount, ScanShare, TaskInputBinding, TaskOutputBinding, TaskSpec,
	shuffle_input_table_id,
};
use crate::exec::runtime_filter::runtime_filter_key_type;
use crate::ir::{
	BoundAggregateCall, BoundExprProgram, ColumnOrdinal, ExecutionPlan, ExecutionSortKey,
	FilterDisposition, JoinKind, JoinStrategy, QueryProfileHint,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentPlanConfig {
	pub query_id: QueryId,
	pub task_context: TaskExecutionContext,
	pub task_budget: TaskBudget,
	pub worker_count: u32,
	pub runtime_filters_enabled: bool,
}

impl FragmentPlanConfig {
	pub const fn new(query_id: QueryId, task_budget: TaskBudget) -> Self {
		Self {
			query_id,
			task_context: TaskExecutionContext::local_test(),
			task_budget,
			worker_count: 1,
			runtime_filters_enabled: true,
		}
	}

	pub const fn with_task_context(mut self, task_context: TaskExecutionContext) -> Self {
		self.task_context = task_context;
		self
	}

	pub const fn with_worker_count(mut self, worker_count: u32) -> Self {
		self.worker_count = worker_count;
		self
	}

	pub const fn with_runtime_filters_enabled(mut self, enabled: bool) -> Self {
		self.runtime_filters_enabled = enabled;
		self
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FragmentError {
	#[error("exchange partition count must be greater than zero")]
	ZeroPartitionCount,
	#[error("only one exchange boundary is supported in the seed fragmenter; observed {count}")]
	MultipleExchangeBoundaries { count: usize },
	#[error("exchange input has no scan metadata to anchor a shuffle-input scan")]
	MissingScanMetadata,
	#[error("join input must terminate in an explicit exchange")]
	JoinInputNotExchanged,
	#[error("broadcast strategy is only supported for INNER joins; observed {kind:?}")]
	BroadcastNonInner { kind: JoinKind },
	#[error("broadcast build input must terminate in a single-partition exchange")]
	BroadcastBuildNotPublished,
	#[error("CREATE TABLE is a coordinator-only catalog operation")]
	CreateTableNotFragmentable,
	#[error("DROP TABLE is a coordinator-only catalog operation")]
	DropTableNotFragmentable,
	#[error("DELETE is a coordinator-only data-plane operation")]
	DeleteNotFragmentable,
	#[error("UPDATE is a coordinator-only data-plane operation")]
	UpdateNotFragmentable,
	#[error("INSERT is a coordinator-only data-plane operation")]
	InsertNotFragmentable,
	#[error("INSERT INTO ... SELECT is a coordinator-only data-plane operation")]
	InsertSelectNotFragmentable,
	#[error("CREATE TABLE AS SELECT is a coordinator-only data-plane operation")]
	CreateTableAsSelectNotFragmentable,
}

pub fn fragment_plan(
	plan: ExecutionPlan,
	config: FragmentPlanConfig,
) -> Result<Vec<StagePlan>, FragmentError> {
	if matches!(
		plan,
		ExecutionPlan::CreateTable { .. } | ExecutionPlan::CreateLogicalType { .. }
	) {
		return Err(FragmentError::CreateTableNotFragmentable);
	}
	if matches!(plan, ExecutionPlan::DropTable { .. }) {
		return Err(FragmentError::DropTableNotFragmentable);
	}
	if matches!(plan, ExecutionPlan::Delete { .. }) {
		return Err(FragmentError::DeleteNotFragmentable);
	}
	if matches!(plan, ExecutionPlan::Update { .. }) {
		return Err(FragmentError::UpdateNotFragmentable);
	}
	if matches!(plan, ExecutionPlan::Insert { .. }) {
		return Err(FragmentError::InsertNotFragmentable);
	}
	if matches!(plan, ExecutionPlan::InsertSelect { .. }) {
		return Err(FragmentError::InsertSelectNotFragmentable);
	}
	if matches!(plan, ExecutionPlan::CreateTableAsSelect { .. }) {
		return Err(FragmentError::CreateTableAsSelectNotFragmentable);
	}

	if contains_join(&plan) {
		return fragment_join_plan(plan, config);
	}

	let exchange_count = count_exchanges(&plan);
	if exchange_count == 0 {
		return Ok(vec![single_task_stage(plan, config)]);
	}
	// ADR 015: this exact Sort-over-Aggregate composition has two exchanges;
	// every other non-join multi-exchange shape still rejects below.
	if let Some(detected) = is_terminal_sort_over_grouped_aggregate(&plan) {
		return fragment_terminal_sort_over_grouped_aggregate(config, detected);
	}
	if exchange_count > 1 {
		return Err(FragmentError::MultipleExchangeBoundaries {
			count: exchange_count,
		});
	}

	let boundary = find_exchange(&plan).expect("exchange count was non-zero");
	if boundary.partition_count == 0 {
		return Err(FragmentError::ZeroPartitionCount);
	}

	let exchange_id = ExchangeId::new(0);
	let producer_stage_id = StageId::new(0);
	let consumer_stage_id = StageId::new(1);
	let scan_metadata =
		first_scan_metadata(&boundary.input).ok_or(FragmentError::MissingScanMetadata)?;
	let consumer_scan = shuffle_input_scan(scan_metadata, boundary.input.schema(), exchange_id);
	let consumer_plan = replace_exchange_with_scan(plan, &consumer_scan);
	let producer_task = TaskPlan::new(TaskSpec {
		identity: TaskIdentity::new(
			config.query_id,
			producer_stage_id,
			TaskId::new(0),
			AttemptId::first(),
		),
		partition: None,
		context: config.task_context,
		plan: ExecutionPlan::Exchange {
			input: Box::new(boundary.input),
			partition_count: boundary.partition_count,
			key: boundary.key.clone(),
		},
		inputs: vec![TaskInputBinding::Scan {
			share: None,
			runtime_filter: None,
		}],
		output: TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count: PartitionCount::new(boundary.partition_count),
			partition_key: boundary.key,
		},
		budget: config.task_budget,
		trace: None,
	});

	let consumer_tasks = (0..boundary.partition_count)
		.map(|partition| {
			let partition_id = PartitionId::new(partition);
			TaskPlan::new(TaskSpec {
				identity: TaskIdentity::new(
					config.query_id,
					consumer_stage_id,
					TaskId::new(partition),
					AttemptId::first(),
				),
				partition: Some(partition_id),
				context: config.task_context,
				plan: consumer_plan.clone(),
				inputs: vec![TaskInputBinding::ShuffleConsumer {
					exchange_id,
					producer_stage_id,
					partition_id,
					producer_partitions: Vec::new(),
				}],
				output: TaskOutputBinding::FinalResult,
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();

	Ok(vec![
		StagePlan {
			stage_id: producer_stage_id,
			upstream_stages: vec![],
			tasks: vec![producer_task],
		},
		StagePlan {
			stage_id: consumer_stage_id,
			upstream_stages: vec![producer_stage_id],
			tasks: consumer_tasks,
		},
	])
}

fn fragment_join_plan(
	plan: ExecutionPlan,
	config: FragmentPlanConfig,
) -> Result<Vec<StagePlan>, FragmentError> {
	let join = first_join(&plan).expect("join presence checked");
	match join.strategy {
		JoinStrategy::HashPartitioned => fragment_hash_join_plan(plan, join, config),
		JoinStrategy::Broadcast => fragment_broadcast_join_plan(plan, join, config),
	}
}

fn fragment_hash_join_plan(
	plan: ExecutionPlan,
	join: JoinBoundary,
	config: FragmentPlanConfig,
) -> Result<Vec<StagePlan>, FragmentError> {
	let runtime_filter_enabled =
		config.runtime_filters_enabled && runtime_filter_join_eligible(&join);
	let left_boundary = extract_exchange(*join.left)?;
	let right_boundary = extract_exchange(*join.right)?;
	if left_boundary.partition_count == 0 || right_boundary.partition_count == 0 {
		return Err(FragmentError::ZeroPartitionCount);
	}
	if left_boundary.partition_count != right_boundary.partition_count {
		return Err(FragmentError::JoinInputNotExchanged);
	}

	let left_exchange_id = ExchangeId::new(0);
	let right_exchange_id = ExchangeId::new(1);
	let left_stage_id = StageId::new(0);
	let right_stage_id = StageId::new(1);
	let join_stage_id = StageId::new(2);
	let left_scan_metadata =
		first_scan_metadata(&left_boundary.input).ok_or(FragmentError::MissingScanMetadata)?;
	let right_scan_metadata =
		first_scan_metadata(&right_boundary.input).ok_or(FragmentError::MissingScanMetadata)?;
	let join_replacement = ExecutionPlan::Join {
		left: Box::new(shuffle_input_scan(
			left_scan_metadata,
			left_boundary.input.schema(),
			left_exchange_id,
		)),
		right: Box::new(shuffle_input_scan(
			right_scan_metadata,
			right_boundary.input.schema(),
			right_exchange_id,
		)),
		kind: join.kind,
		left_keys: join.left_keys,
		right_keys: join.right_keys,
		strategy: join.strategy,
		output_schema: join.output_schema,
	};
	let terminal_gather = terminal_sort_gather(&plan);
	let terminal_exchange_id = ExchangeId::new(2);
	let (consumer_plan, consumer_output, terminal_stage) = if let Some(gather) = terminal_gather {
		let consumer_plan = replace_first_join(gather.input.clone(), join_replacement);
		let terminal_stage = terminal_sort_stage(
			config,
			StageId::new(3),
			join_stage_id,
			terminal_exchange_id,
			gather,
		)?;
		(
			consumer_plan,
			TaskOutputBinding::ShuffleProducer {
				exchange_id: terminal_exchange_id,
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
			},
			Some(terminal_stage),
		)
	} else {
		(
			replace_first_join(plan, join_replacement),
			TaskOutputBinding::FinalResult,
			None,
		)
	};
	let partition_count = left_boundary.partition_count;
	let consumer_tasks = (0..partition_count)
		.map(|partition| {
			let partition_id = PartitionId::new(partition);
			TaskPlan::new(TaskSpec {
				identity: TaskIdentity::new(
					config.query_id,
					join_stage_id,
					TaskId::new(partition),
					AttemptId::first(),
				),
				partition: Some(partition_id),
				context: config.task_context,
				plan: consumer_plan.clone(),
				inputs: vec![
					TaskInputBinding::ShuffleConsumer {
						exchange_id: left_exchange_id,
						producer_stage_id: left_stage_id,
						partition_id,
						producer_partitions: Vec::new(),
					},
					TaskInputBinding::ShuffleConsumer {
						exchange_id: right_exchange_id,
						producer_stage_id: right_stage_id,
						partition_id,
						producer_partitions: Vec::new(),
					},
				],
				output: consumer_output.clone(),
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();

	let mut stages = vec![
		StagePlan {
			stage_id: left_stage_id,
			upstream_stages: vec![],
			tasks: vec![producer_task(
				config,
				left_stage_id,
				left_exchange_id,
				left_boundary,
			)],
		},
		StagePlan {
			stage_id: right_stage_id,
			upstream_stages: if runtime_filter_enabled {
				vec![left_stage_id]
			} else {
				vec![]
			},
			tasks: vec![producer_task(
				config,
				right_stage_id,
				right_exchange_id,
				right_boundary,
			)],
		},
		StagePlan {
			stage_id: join_stage_id,
			upstream_stages: vec![left_stage_id, right_stage_id],
			tasks: consumer_tasks,
		},
	];
	if let Some(stage) = terminal_stage {
		stages.push(stage);
	}
	Ok(stages)
}

fn fragment_broadcast_join_plan(
	plan: ExecutionPlan,
	join: JoinBoundary,
	config: FragmentPlanConfig,
) -> Result<Vec<StagePlan>, FragmentError> {
	if join.kind != JoinKind::Inner {
		return Err(FragmentError::BroadcastNonInner { kind: join.kind });
	}
	let build_boundary = extract_exchange(*join.left)?;
	if build_boundary.partition_count != 1 {
		return Err(FragmentError::BroadcastBuildNotPublished);
	}
	if matches!(join.right.as_ref(), ExecutionPlan::Exchange { .. }) {
		return Err(FragmentError::BroadcastBuildNotPublished);
	}

	let build_exchange_id = ExchangeId::new(0);
	let build_stage_id = StageId::new(0);
	let join_stage_id = StageId::new(1);
	let worker_count = config.worker_count.max(1);
	let build_scan_metadata =
		first_scan_metadata(&build_boundary.input).ok_or(FragmentError::MissingScanMetadata)?;
	let join_replacement = ExecutionPlan::Join {
		left: Box::new(shuffle_input_scan(
			build_scan_metadata,
			build_boundary.input.schema(),
			build_exchange_id,
		)),
		right: join.right,
		kind: join.kind,
		left_keys: join.left_keys,
		right_keys: join.right_keys,
		strategy: join.strategy,
		output_schema: join.output_schema,
	};
	let terminal_gather = terminal_sort_gather(&plan);
	let terminal_exchange_id = ExchangeId::new(1);
	let (consumer_plan, consumer_output, terminal_stage) = if let Some(gather) = terminal_gather {
		let consumer_plan = replace_first_join(gather.input.clone(), join_replacement);
		let terminal_stage = terminal_sort_stage(
			config,
			StageId::new(2),
			join_stage_id,
			terminal_exchange_id,
			gather,
		)?;
		(
			consumer_plan,
			TaskOutputBinding::ShuffleProducer {
				exchange_id: terminal_exchange_id,
				partition_count: PartitionCount::new(1),
				partition_key: BoundExprProgram::empty(),
			},
			Some(terminal_stage),
		)
	} else {
		(
			replace_first_join(plan, join_replacement),
			TaskOutputBinding::FinalResult,
			None,
		)
	};
	let consumer_tasks = (0..worker_count)
		.map(|worker_index| {
			TaskPlan::new(TaskSpec {
				identity: TaskIdentity::new(
					config.query_id,
					join_stage_id,
					TaskId::new(worker_index),
					AttemptId::first(),
				),
				partition: None,
				context: config.task_context,
				plan: consumer_plan.clone(),
				inputs: vec![
					TaskInputBinding::Scan {
						share: Some(ScanShare {
							worker_index,
							worker_count,
						}),
						runtime_filter: None,
					},
					TaskInputBinding::ShuffleConsumer {
						exchange_id: build_exchange_id,
						producer_stage_id: build_stage_id,
						partition_id: PartitionId::new(0),
						producer_partitions: Vec::new(),
					},
				],
				output: consumer_output.clone(),
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();

	let mut stages = vec![
		StagePlan {
			stage_id: build_stage_id,
			upstream_stages: vec![],
			tasks: vec![producer_task(
				config,
				build_stage_id,
				build_exchange_id,
				build_boundary,
			)],
		},
		StagePlan {
			stage_id: join_stage_id,
			upstream_stages: vec![build_stage_id],
			tasks: consumer_tasks,
		},
	];
	if let Some(stage) = terminal_stage {
		stages.push(stage);
	}
	Ok(stages)
}

fn producer_task(
	config: FragmentPlanConfig,
	stage_id: StageId,
	exchange_id: ExchangeId,
	boundary: ExchangeBoundary,
) -> TaskPlan {
	TaskPlan::new(TaskSpec {
		identity: TaskIdentity::new(
			config.query_id,
			stage_id,
			TaskId::new(0),
			AttemptId::first(),
		),
		partition: None,
		context: config.task_context,
		plan: ExecutionPlan::Exchange {
			input: Box::new(boundary.input),
			partition_count: boundary.partition_count,
			key: boundary.key.clone(),
		},
		inputs: vec![TaskInputBinding::Scan {
			share: None,
			runtime_filter: None,
		}],
		output: TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count: PartitionCount::new(boundary.partition_count),
			partition_key: boundary.key,
		},
		budget: config.task_budget,
		trace: None,
	})
}

fn single_task_stage(plan: ExecutionPlan, config: FragmentPlanConfig) -> StagePlan {
	StagePlan {
		stage_id: StageId::new(0),
		upstream_stages: vec![],
		tasks: vec![TaskPlan::new(TaskSpec {
			identity: TaskIdentity::new(
				config.query_id,
				StageId::new(0),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: None,
			context: config.task_context,
			plan,
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: config.task_budget,
			trace: None,
		})],
	}
}

#[derive(Debug, Clone)]
struct ExchangeBoundary {
	input: ExecutionPlan,
	partition_count: u32,
	key: BoundExprProgram,
}

#[derive(Debug, Clone)]
struct JoinBoundary {
	left: Box<ExecutionPlan>,
	right: Box<ExecutionPlan>,
	kind: JoinKind,
	left_keys: Vec<ColumnOrdinal>,
	right_keys: Vec<ColumnOrdinal>,
	strategy: JoinStrategy,
	output_schema: SchemaRef,
}

#[derive(Debug, Clone)]
struct TerminalSortGather {
	input: ExecutionPlan,
	order_by: Vec<ExecutionSortKey>,
	limit: Option<usize>,
	offset: usize,
	output_schema: SchemaRef,
}

#[derive(Debug, Clone)]
struct TerminalSortOverAggregate {
	inner_input: ExecutionPlan,
	group_partition_count: u32,
	group_key: BoundExprProgram,
	group_expr: BoundExprProgram,
	aggr_expr: Vec<BoundAggregateCall>,
	aggregate_output_schema: SchemaRef,
	having: Option<(BoundExprProgram, FilterDisposition)>,
	projection: Option<(BoundExprProgram, SchemaRef)>,
	terminal_gather: TerminalSortGather,
}

fn terminal_sort_gather(plan: &ExecutionPlan) -> Option<TerminalSortGather> {
	let ExecutionPlan::Sort {
		input,
		order_by,
		limit,
		offset,
		output_schema,
	} = plan
	else {
		return None;
	};
	let ExecutionPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		return None;
	};
	if *partition_count != 1 || !key.is_empty_roots() || !contains_join(input) {
		return None;
	}

	Some(TerminalSortGather {
		input: input.as_ref().clone(),
		order_by: order_by.clone(),
		limit: *limit,
		offset: *offset,
		output_schema: output_schema.clone(),
	})
}

fn is_terminal_sort_over_grouped_aggregate(
	plan: &ExecutionPlan,
) -> Option<TerminalSortOverAggregate> {
	let ExecutionPlan::Sort {
		input,
		order_by,
		limit,
		offset,
		output_schema,
	} = plan
	else {
		return None;
	};
	let ExecutionPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		return None;
	};
	if *partition_count != 1 || !key.is_empty_roots() {
		return None;
	}
	let gathered_input = input.as_ref().clone();
	let mut aggregate = input.as_ref();
	let mut projection = None;
	if let ExecutionPlan::Project {
		input,
		exprs,
		output_schema,
	} = aggregate
	{
		projection = Some((exprs.clone(), output_schema.clone()));
		aggregate = input.as_ref();
	}
	let mut having = None;
	if let ExecutionPlan::Filter {
		input,
		predicate,
		disposition,
	} = aggregate
	{
		having = Some((predicate.clone(), *disposition));
		aggregate = input.as_ref();
	}
	let ExecutionPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
		output_schema: aggregate_output_schema,
	} = aggregate
	else {
		return None;
	};
	let ExecutionPlan::Exchange {
		input,
		partition_count: group_partition_count,
		key: group_key,
	} = input.as_ref()
	else {
		return None;
	};

	Some(TerminalSortOverAggregate {
		inner_input: input.as_ref().clone(),
		group_partition_count: *group_partition_count,
		group_key: group_key.clone(),
		group_expr: group_expr.clone(),
		aggr_expr: aggr_expr.clone(),
		aggregate_output_schema: aggregate_output_schema.clone(),
		having,
		projection,
		terminal_gather: TerminalSortGather {
			input: gathered_input,
			order_by: order_by.clone(),
			limit: *limit,
			offset: *offset,
			output_schema: output_schema.clone(),
		},
	})
}

fn fragment_terminal_sort_over_grouped_aggregate(
	config: FragmentPlanConfig,
	detected: TerminalSortOverAggregate,
) -> Result<Vec<StagePlan>, FragmentError> {
	let TerminalSortOverAggregate {
		inner_input,
		group_partition_count,
		group_key,
		group_expr,
		aggr_expr,
		aggregate_output_schema,
		having,
		projection,
		terminal_gather,
	} = detected;
	if group_partition_count == 0 {
		return Err(FragmentError::ZeroPartitionCount);
	}

	let producer_stage_id = StageId::new(0);
	let aggregate_stage_id = StageId::new(1);
	let sort_stage_id = StageId::new(2);
	let group_exchange_id = ExchangeId::new(0);
	let terminal_exchange_id = ExchangeId::new(1);
	let scan_metadata =
		first_scan_metadata(&inner_input).ok_or(FragmentError::MissingScanMetadata)?;
	let input_schema = inner_input.schema();
	let aggregate_input = shuffle_input_scan(scan_metadata, input_schema, group_exchange_id);
	let aggregate_plan = ExecutionPlan::Aggregate {
		input: Box::new(aggregate_input),
		group_expr,
		aggr_expr,
		output_schema: aggregate_output_schema,
	};
	let aggregate_plan = if let Some((predicate, disposition)) = having {
		ExecutionPlan::Filter {
			input: Box::new(aggregate_plan),
			predicate,
			disposition,
		}
	} else {
		aggregate_plan
	};
	let aggregate_plan = if let Some((exprs, output_schema)) = projection {
		ExecutionPlan::Project {
			input: Box::new(aggregate_plan),
			exprs,
			output_schema,
		}
	} else {
		aggregate_plan
	};
	let producer_task = producer_task(
		config,
		producer_stage_id,
		group_exchange_id,
		ExchangeBoundary {
			input: inner_input,
			partition_count: group_partition_count,
			key: group_key,
		},
	);
	let aggregate_tasks = (0..group_partition_count)
		.map(|partition| {
			let partition_id = PartitionId::new(partition);
			TaskPlan::new(TaskSpec {
				identity: TaskIdentity::new(
					config.query_id,
					aggregate_stage_id,
					TaskId::new(partition),
					AttemptId::first(),
				),
				partition: Some(partition_id),
				context: config.task_context,
				plan: aggregate_plan.clone(),
				inputs: vec![TaskInputBinding::ShuffleConsumer {
					exchange_id: group_exchange_id,
					producer_stage_id,
					partition_id,
					producer_partitions: Vec::new(),
				}],
				output: TaskOutputBinding::ShuffleProducer {
					exchange_id: terminal_exchange_id,
					partition_count: PartitionCount::new(1),
					partition_key: BoundExprProgram::empty(),
				},
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();
	let sort_stage = terminal_sort_stage(
		config,
		sort_stage_id,
		aggregate_stage_id,
		terminal_exchange_id,
		terminal_gather,
	)?;

	Ok(vec![
		StagePlan {
			stage_id: producer_stage_id,
			upstream_stages: vec![],
			tasks: vec![producer_task],
		},
		StagePlan {
			stage_id: aggregate_stage_id,
			upstream_stages: vec![producer_stage_id],
			tasks: aggregate_tasks,
		},
		sort_stage,
	])
}

fn terminal_sort_stage(
	config: FragmentPlanConfig,
	stage_id: StageId,
	producer_stage_id: StageId,
	exchange_id: ExchangeId,
	gather: TerminalSortGather,
) -> Result<StagePlan, FragmentError> {
	let scan_metadata =
		first_scan_metadata(&gather.input).ok_or(FragmentError::MissingScanMetadata)?;
	let input_schema = gather.input.schema();
	let sort_plan = ExecutionPlan::Sort {
		input: Box::new(shuffle_input_scan(scan_metadata, input_schema, exchange_id)),
		order_by: gather.order_by,
		limit: gather.limit,
		offset: gather.offset,
		output_schema: gather.output_schema,
	};

	Ok(StagePlan {
		stage_id,
		upstream_stages: vec![producer_stage_id],
		tasks: vec![TaskPlan::new(TaskSpec {
			identity: TaskIdentity::new(
				config.query_id,
				stage_id,
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: Some(PartitionId::new(0)),
			context: config.task_context,
			plan: sort_plan,
			inputs: vec![TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id,
				partition_id: PartitionId::new(0),
				producer_partitions: Vec::new(),
			}],
			output: TaskOutputBinding::FinalResult,
			budget: config.task_budget,
			trace: None,
		})],
	})
}

fn runtime_filter_join_eligible(join: &JoinBoundary) -> bool {
	if !matches!(join.kind, JoinKind::Inner)
		|| join.left_keys.len() != 1
		|| join.right_keys.len() != 1
	{
		return false;
	}
	supported_join_key(join.left.as_ref(), join.left_keys[0])
		.zip(supported_join_key(join.right.as_ref(), join.right_keys[0]))
		.is_some_and(|(left, right)| left == right)
}

fn supported_join_key(
	plan: &ExecutionPlan,
	key: ColumnOrdinal,
) -> Option<wendelon::schema::RuntimeFilterKeyType> {
	let schema = plan.schema();
	let field = schema.fields().get(usize::try_from(key.0).ok()?)?;
	runtime_filter_key_type(field.data_type())
}

fn contains_join(plan: &ExecutionPlan) -> bool {
	first_join(plan).is_some()
}

fn first_join(plan: &ExecutionPlan) -> Option<JoinBoundary> {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			kind,
			left_keys,
			right_keys,
			strategy,
			output_schema,
		} => Some(JoinBoundary {
			left: left.clone(),
			right: right.clone(),
			kind: *kind,
			left_keys: left_keys.clone(),
			right_keys: right_keys.clone(),
			strategy: *strategy,
			output_schema: output_schema.clone(),
		}),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => first_join(input),
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => None,
	}
}

fn extract_exchange(plan: ExecutionPlan) -> Result<ExchangeBoundary, FragmentError> {
	match plan {
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => Ok(ExchangeBoundary {
			input: *input,
			partition_count,
			key,
		}),
		_ => Err(FragmentError::JoinInputNotExchanged),
	}
}

fn find_exchange(plan: &ExecutionPlan) -> Option<ExchangeBoundary> {
	match plan {
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => Some(ExchangeBoundary {
			input: input.as_ref().clone(),
			partition_count: *partition_count,
			key: key.clone(),
		}),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Sort { input, .. } => find_exchange(input),
		ExecutionPlan::Join { left, right, .. } => {
			find_exchange(left).or_else(|| find_exchange(right))
		}
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => None,
	}
}

fn count_exchanges(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Exchange { input, .. } => 1 + count_exchanges(input),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Sort { input, .. } => count_exchanges(input),
		ExecutionPlan::Join { left, right, .. } => count_exchanges(left) + count_exchanges(right),
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => 0,
	}
}

#[derive(Debug, Clone, Copy)]
struct ScanMetadata {
	table: TableObjectId,
	fingerprint: PhysicalDescriptorFingerprint,
}

fn first_scan_metadata(plan: &ExecutionPlan) -> Option<ScanMetadata> {
	match plan {
		ExecutionPlan::Scan {
			table, fingerprint, ..
		} => Some(ScanMetadata {
			table: *table,
			fingerprint: *fingerprint,
		}),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => first_scan_metadata(input),
		ExecutionPlan::Join { left, right, .. } => {
			first_scan_metadata(left).or_else(|| first_scan_metadata(right))
		}
		ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => None,
	}
}

fn shuffle_input_scan(
	metadata: ScanMetadata,
	schema: SchemaRef,
	exchange_id: ExchangeId,
) -> ExecutionPlan {
	ExecutionPlan::Scan {
		legacy_table_id: shuffle_input_table_id(exchange_id),
		table: metadata.table,
		fingerprint: metadata.fingerprint,
		schema,
		projection: None,
		pbc_program: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: Vec::new(),
	}
}

fn replace_exchange_with_scan(plan: ExecutionPlan, scan: &ExecutionPlan) -> ExecutionPlan {
	match plan {
		ExecutionPlan::Exchange { .. } => scan.clone(),
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => ExecutionPlan::Filter {
			input: Box::new(replace_exchange_with_scan(*input, scan)),
			predicate,
			disposition,
		},
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} => ExecutionPlan::Project {
			input: Box::new(replace_exchange_with_scan(*input, scan)),
			exprs,
			output_schema,
		},
		ExecutionPlan::Aggregate {
			input,
			group_expr,
			aggr_expr,
			output_schema,
		} => ExecutionPlan::Aggregate {
			input: Box::new(replace_exchange_with_scan(*input, scan)),
			group_expr,
			aggr_expr,
			output_schema,
		},
		ExecutionPlan::Sort {
			input,
			order_by,
			limit,
			offset,
			output_schema,
		} => ExecutionPlan::Sort {
			input: Box::new(replace_exchange_with_scan(*input, scan)),
			order_by,
			limit,
			offset,
			output_schema,
		},
		ExecutionPlan::Join {
			left,
			right,
			kind,
			left_keys,
			right_keys,
			strategy,
			output_schema,
		} => ExecutionPlan::Join {
			left: Box::new(replace_exchange_with_scan(*left, scan)),
			right: Box::new(replace_exchange_with_scan(*right, scan)),
			kind,
			left_keys,
			right_keys,
			strategy,
			output_schema,
		},
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => plan,
	}
}

fn replace_first_join(plan: ExecutionPlan, replacement: ExecutionPlan) -> ExecutionPlan {
	match plan {
		ExecutionPlan::Join { .. } => replacement,
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => ExecutionPlan::Filter {
			input: Box::new(replace_first_join(*input, replacement)),
			predicate,
			disposition,
		},
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} => ExecutionPlan::Project {
			input: Box::new(replace_first_join(*input, replacement)),
			exprs,
			output_schema,
		},
		ExecutionPlan::Aggregate {
			input,
			group_expr,
			aggr_expr,
			output_schema,
		} => ExecutionPlan::Aggregate {
			input: Box::new(replace_first_join(*input, replacement)),
			group_expr,
			aggr_expr,
			output_schema,
		},
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => ExecutionPlan::Exchange {
			input: Box::new(replace_first_join(*input, replacement)),
			partition_count,
			key,
		},
		ExecutionPlan::Sort {
			input,
			order_by,
			limit,
			offset,
			output_schema,
		} => ExecutionPlan::Sort {
			input: Box::new(replace_first_join(*input, replacement)),
			order_by,
			limit,
			offset,
			output_schema,
		},
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => plan,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::budget::{QueryBudget, TaskBudget};
	use crate::ir::{
		AggregateFunction, BoundAggregateCall, BoundExprProgram, BoundExprProgramBuilder,
		ColumnName, ColumnOrdinal, ResolvedColumn, TableId,
	};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::sync::Arc;
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableSchemaDescriptor,
	};

	#[test]
	fn non_shuffle_plan_fragments_to_single_final_stage() {
		let plan = scan_plan();

		let stages = fragment_plan(plan, config()).expect("fragment");

		assert_eq!(stages.len(), 1);
		assert_eq!(stages[0].stage_id, StageId::new(0));
		assert!(stages[0].upstream_stages.is_empty());
		assert_eq!(stages[0].tasks.len(), 1);
		assert!(matches!(
			stages[0].tasks[0].spec.output,
			TaskOutputBinding::FinalResult
		));
	}

	#[test]
	fn exchange_plan_fragments_to_producer_and_partition_consumers() {
		let plan = final_aggregate_plan(ExecutionPlan::Exchange {
			input: Box::new(scan_plan()),
			partition_count: 3,
			key: key_expr(),
		});

		let stages = fragment_plan(plan, config()).expect("fragment");

		assert_eq!(stages.len(), 2);
		assert_eq!(stages[0].stage_id, StageId::new(0));
		assert_eq!(stages[1].stage_id, StageId::new(1));
		assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
		assert_eq!(stages[0].tasks.len(), 1);
		assert_eq!(stages[1].tasks.len(), 3);
		assert!(matches!(
			stages[0].tasks[0].spec.output,
			TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count,
				..
			} if exchange_id == ExchangeId::new(0)
				&& partition_count == PartitionCount::new(3)
		));
		for (idx, task) in stages[1].tasks.iter().enumerate() {
			assert_eq!(
				task.spec.partition,
				Some(PartitionId::new(u32::try_from(idx).unwrap()))
			);
			assert_eq!(task.spec.identity.stage_id, StageId::new(1));
			assert_eq!(
				task.spec.identity.task_id,
				TaskId::new(u32::try_from(idx).unwrap())
			);
			assert!(matches!(
				&task.spec.inputs[0],
				TaskInputBinding::ShuffleConsumer {
					exchange_id,
					producer_stage_id,
					producer_partitions,
					..
				} if *exchange_id == ExchangeId::new(0)
					&& *producer_stage_id == StageId::new(0)
					&& producer_partitions.is_empty()
			));
		}
	}

	#[test]
	fn exchange_ids_are_structural_for_repeat_fragmentation() {
		let plan = ExecutionPlan::Exchange {
			input: Box::new(scan_plan()),
			partition_count: 2,
			key: key_expr(),
		};

		let left = fragment_plan(plan.clone(), config()).expect("left");
		let right = fragment_plan(plan, config()).expect("right");

		let TaskOutputBinding::ShuffleProducer {
			exchange_id: left_id,
			..
		} = left[0].tasks[0].spec.output
		else {
			panic!("left producer output");
		};
		let TaskOutputBinding::ShuffleProducer {
			exchange_id: right_id,
			..
		} = right[0].tasks[0].spec.output
		else {
			panic!("right producer output");
		};
		assert_eq!(left_id, ExchangeId::new(0));
		assert_eq!(right_id, left_id);
	}

	#[test]
	fn zero_partition_exchange_is_rejected() {
		let plan = ExecutionPlan::Exchange {
			input: Box::new(scan_plan()),
			partition_count: 0,
			key: key_expr(),
		};

		assert_eq!(
			fragment_plan(plan, config()).expect_err("zero partition count"),
			FragmentError::ZeroPartitionCount
		);
	}

	#[test]
	fn multiple_exchange_boundaries_are_rejected_for_seed_fragmenter() {
		let plan = ExecutionPlan::Exchange {
			input: Box::new(ExecutionPlan::Exchange {
				input: Box::new(scan_plan()),
				partition_count: 2,
				key: key_expr(),
			}),
			partition_count: 2,
			key: key_expr(),
		};

		assert_eq!(
			fragment_plan(plan, config()).expect_err("multiple exchanges"),
			FragmentError::MultipleExchangeBoundaries { count: 2 }
		);
	}

	#[test]
	fn hash_join_fragments_to_two_producers_and_multi_upstream_consumer() {
		let plan = join_plan(
			exchanged_scan(TableId(1), 2),
			exchanged_scan(TableId(2), 2),
			JoinStrategy::HashPartitioned,
		);

		let stages = fragment_plan(plan, config()).expect("fragment hash join");

		assert_eq!(stages.len(), 3);
		assert_eq!(stages[0].tasks.len(), 1);
		assert_eq!(stages[1].tasks.len(), 1);
		assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
		assert_eq!(
			stages[2].upstream_stages,
			vec![StageId::new(0), StageId::new(1)]
		);
		assert_eq!(stages[2].tasks.len(), 2);
		for (partition, task) in stages[2].tasks.iter().enumerate() {
			assert_eq!(
				task.spec.partition,
				Some(PartitionId::new(u32::try_from(partition).unwrap()))
			);
			assert_eq!(task.spec.inputs.len(), 2);
			assert!(matches!(
				&task.spec.inputs[0],
				TaskInputBinding::ShuffleConsumer {
					exchange_id,
					producer_stage_id,
					partition_id,
					..
				} if *exchange_id == ExchangeId::new(0)
					&& *producer_stage_id == StageId::new(0)
					&& *partition_id == PartitionId::new(u32::try_from(partition).unwrap())
			));
			assert!(matches!(
				&task.spec.inputs[1],
				TaskInputBinding::ShuffleConsumer {
					exchange_id,
					producer_stage_id,
					partition_id,
					..
				} if *exchange_id == ExchangeId::new(1)
					&& *producer_stage_id == StageId::new(1)
					&& *partition_id == PartitionId::new(u32::try_from(partition).unwrap())
			));
		}
		match &stages[2].tasks[0].spec.plan {
			ExecutionPlan::Join { left, right, .. } => {
				assert!(matches!(
					&**left,
					ExecutionPlan::Scan { legacy_table_id, .. }
						if *legacy_table_id == crate::distributed::task::shuffle_input_table_id(ExchangeId::new(0))
				));
				assert!(matches!(
					&**right,
					ExecutionPlan::Scan { legacy_table_id, .. }
						if *legacy_table_id == crate::distributed::task::shuffle_input_table_id(ExchangeId::new(1))
				));
			}
			other => panic!("expected join consumer plan, got {other:?}"),
		}
	}

	#[test]
	fn hash_join_runtime_filter_disabled_preserves_parallel_producers() {
		let plan = join_plan(
			exchanged_scan(TableId(1), 2),
			exchanged_scan(TableId(2), 2),
			JoinStrategy::HashPartitioned,
		);

		let stages = fragment_plan(plan, config().with_runtime_filters_enabled(false))
			.expect("fragment hash join");

		assert!(stages[0].upstream_stages.is_empty());
		assert!(stages[1].upstream_stages.is_empty());
		assert_eq!(
			stages[2].upstream_stages,
			vec![StageId::new(0), StageId::new(1)]
		);
	}

	#[test]
	fn hash_join_float_key_is_not_runtime_filter_eligible() {
		let plan = join_plan(
			exchanged_scan_with_key_type(TableId(1), 2, DataType::Float64),
			exchanged_scan_with_key_type(TableId(2), 2, DataType::Float64),
			JoinStrategy::HashPartitioned,
		);

		let stages = fragment_plan(plan, config()).expect("fragment hash join");

		assert!(stages[1].upstream_stages.is_empty());
		assert_eq!(
			stages[2].upstream_stages,
			vec![StageId::new(0), StageId::new(1)]
		);
	}

	#[test]
	fn hash_join_composite_key_is_not_runtime_filter_eligible() {
		let plan = ExecutionPlan::Join {
			left: Box::new(exchanged_scan(TableId(1), 2)),
			right: Box::new(exchanged_scan(TableId(2), 2)),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(0), ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0), ColumnOrdinal(0)],
			strategy: JoinStrategy::HashPartitioned,
			output_schema: Arc::new(Schema::new(vec![
				Field::new("key", DataType::Int32, false),
				Field::new("key", DataType::Int32, false),
			])),
		};

		let stages = fragment_plan(plan, config()).expect("fragment hash join");

		assert!(stages[1].upstream_stages.is_empty());
		assert_eq!(
			stages[2].upstream_stages,
			vec![StageId::new(0), StageId::new(1)]
		);
	}

	#[test]
	fn runtime_filter_eligible_false_for_every_non_inner_kind() {
		for kind in [
			JoinKind::LeftOuter,
			JoinKind::RightOuter,
			JoinKind::FullOuter,
			JoinKind::LeftSemi,
			JoinKind::RightSemi,
			JoinKind::LeftAnti,
			JoinKind::RightAnti,
		] {
			let plan = join_plan_with_kind(
				exchanged_scan(TableId(1), 2),
				exchanged_scan(TableId(2), 2),
				JoinStrategy::HashPartitioned,
				kind,
			);

			let stages = fragment_plan(plan, config()).expect("fragment hash join");

			assert!(stages[1].upstream_stages.is_empty(), "{kind:?}");
			assert_eq!(
				stages[2].upstream_stages,
				vec![StageId::new(0), StageId::new(1)]
			);
		}
	}

	#[test]
	fn broadcast_join_fragments_to_build_publish_and_scan_share_consumers() {
		let plan = join_plan(
			exchanged_scan(TableId(1), 1),
			scan_plan_with_table(TableId(2), 2),
			JoinStrategy::Broadcast,
		);

		let stages =
			fragment_plan(plan, config().with_worker_count(3)).expect("fragment broadcast join");

		assert_eq!(stages.len(), 2);
		assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
		assert_eq!(stages[1].tasks.len(), 3);
		for (worker_index, task) in stages[1].tasks.iter().enumerate() {
			assert_eq!(task.spec.inputs.len(), 2);
			assert!(matches!(
				&task.spec.inputs[0],
				TaskInputBinding::Scan {
					share: Some(ScanShare {
						worker_index: observed,
						worker_count,
					}),
					..
				} if *observed == u32::try_from(worker_index).unwrap() && *worker_count == 3
			));
			assert!(matches!(
				&task.spec.inputs[1],
				TaskInputBinding::ShuffleConsumer {
					exchange_id,
					producer_stage_id,
					partition_id,
					..
				} if *exchange_id == ExchangeId::new(0)
					&& *producer_stage_id == StageId::new(0)
					&& *partition_id == PartitionId::new(0)
			));
		}
	}

	#[test]
	fn broadcast_non_inner_join_is_rejected_by_fragmenter() {
		let plan = join_plan_with_kind(
			exchanged_scan(TableId(1), 1),
			scan_plan_with_table(TableId(2), 2),
			JoinStrategy::Broadcast,
			JoinKind::LeftOuter,
		);

		assert_eq!(
			fragment_plan(plan, config().with_worker_count(3)).expect_err("reject broadcast"),
			FragmentError::BroadcastNonInner {
				kind: JoinKind::LeftOuter
			}
		);
	}

	fn config() -> FragmentPlanConfig {
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		)
	}

	fn scan_plan() -> ExecutionPlan {
		scan_plan_with_table(TableId(1), 1)
	}

	fn scan_plan_with_table(table_id: TableId, object_byte: u8) -> ExecutionPlan {
		let descriptor = descriptor();
		let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
		ExecutionPlan::Scan {
			legacy_table_id: table_id,
			table: TableObjectId::new([object_byte; 16]),
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema,
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			profile_hint: QueryProfileHint::Scan,
			primary_key_columns: vec![ColumnOrdinal(0)],
		}
	}

	fn exchanged_scan(table_id: TableId, partition_count: u32) -> ExecutionPlan {
		exchanged_scan_with_key_type(table_id, partition_count, DataType::Int32)
	}

	fn exchanged_scan_with_key_type(
		table_id: TableId,
		partition_count: u32,
		data_type: DataType,
	) -> ExecutionPlan {
		ExecutionPlan::Exchange {
			input: Box::new(scan_plan_with_table_and_type(
				table_id,
				table_id.0 as u8,
				data_type.clone(),
			)),
			partition_count,
			key: key_expr_with_type(data_type),
		}
	}

	fn scan_plan_with_table_and_type(
		table_id: TableId,
		object_byte: u8,
		data_type: DataType,
	) -> ExecutionPlan {
		let descriptor = descriptor();
		let schema = Arc::new(Schema::new(vec![Field::new("key", data_type, false)]));
		ExecutionPlan::Scan {
			legacy_table_id: table_id,
			table: TableObjectId::new([object_byte; 16]),
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema,
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			profile_hint: QueryProfileHint::Scan,
			primary_key_columns: vec![ColumnOrdinal(0)],
		}
	}

	fn join_plan(
		left: ExecutionPlan,
		right: ExecutionPlan,
		strategy: JoinStrategy,
	) -> ExecutionPlan {
		join_plan_with_kind(left, right, strategy, JoinKind::Inner)
	}

	fn join_plan_with_kind(
		left: ExecutionPlan,
		right: ExecutionPlan,
		strategy: JoinStrategy,
		kind: JoinKind,
	) -> ExecutionPlan {
		ExecutionPlan::Join {
			left: Box::new(left),
			right: Box::new(right),
			kind,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			strategy,
			output_schema: Arc::new(Schema::new(vec![
				Field::new("key", DataType::Int32, false),
				Field::new("key", DataType::Int32, false),
			])),
		}
	}

	fn final_aggregate_plan(input: ExecutionPlan) -> ExecutionPlan {
		ExecutionPlan::Aggregate {
			input: Box::new(input),
			group_expr: key_expr(),
			aggr_expr: vec![BoundAggregateCall {
				function: AggregateFunction::CountAll,
				args: BoundExprProgram::empty(),
			}],
			output_schema: Arc::new(Schema::new(vec![
				Field::new("key", DataType::Int32, false),
				Field::new("count", DataType::Int64, false),
			])),
		}
	}

	fn key_expr() -> BoundExprProgram {
		key_expr_with_type(DataType::Int32)
	}

	fn key_expr_with_type(data_type: DataType) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let key = builder.append_column(ResolvedColumn {
			name: ColumnName("key".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type,
			nullable: false,
			pbc_eligible: true,
		});
		builder.push_root(key);
		builder.build()
	}

	fn descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![Field::new("key", DataType::Int32, false)]);
		TableSchemaDescriptor::new(
			TableObjectId::new([1; 16]),
			1,
			"default",
			"fragment_test",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("key")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
