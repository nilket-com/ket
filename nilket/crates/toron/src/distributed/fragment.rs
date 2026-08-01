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
	JoinRuntimeObservationMode, PartitionCount, ScanShare, TaskInputBinding, TaskOutputBinding,
	TaskSpec, shuffle_input_table_id,
};
use crate::exec::runtime_filter::runtime_filter_key_type;
use crate::ir::{
	BoundExprProgram, BoundExprProgramBuilder, BroadcastReplayEligibility, ColumnName,
	ColumnOrdinal, ExecutionPlan, ExecutionSortKey, FilterDisposition, JoinKind, JoinReplayId,
	JoinStrategy, QueryProfileHint, ResolvedColumn,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentPlanConfig {
	pub query_id: QueryId,
	pub task_context: TaskExecutionContext,
	pub task_budget: TaskBudget,
	pub worker_count: u32,
	pub runtime_filters_enabled: bool,
	pub join_runtime_observation_mode: JoinRuntimeObservationMode,
	pub id_namespace: u32,
}

impl FragmentPlanConfig {
	pub fn new(query_id: QueryId, task_budget: TaskBudget) -> Self {
		Self {
			query_id,
			task_context: TaskExecutionContext::local_test(),
			task_budget,
			worker_count: 1,
			runtime_filters_enabled: true,
			join_runtime_observation_mode: JoinRuntimeObservationMode::Disabled,
			id_namespace: 0,
		}
	}

	pub fn with_task_context(mut self, task_context: TaskExecutionContext) -> Self {
		self.task_context = task_context;
		self
	}

	pub fn with_worker_count(mut self, worker_count: u32) -> Self {
		self.worker_count = worker_count;
		self
	}

	pub fn with_runtime_filters_enabled(mut self, enabled: bool) -> Self {
		self.runtime_filters_enabled = enabled;
		self
	}

	pub fn with_join_runtime_observation_mode(mut self, mode: JoinRuntimeObservationMode) -> Self {
		self.join_runtime_observation_mode = mode;
		self
	}

	pub fn with_id_namespace(mut self, id_namespace: u32) -> Self {
		self.id_namespace = id_namespace;
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
	#[error(
		"aggregate above a fragmented join is not partition-decomposable at the join boundary; {group_key_count} group key roots"
	)]
	JoinConsumerAggregateNotDecomposable { group_key_count: usize },
	#[error("global sort barriers inside join inputs are not supported")]
	SortBarrierInsideJoinInput,
	#[error("broadcast strategy is only supported for INNER joins; observed {kind:?}")]
	BroadcastNonInner { kind: JoinKind },
	#[error("broadcast build input must terminate in a single-partition exchange")]
	BroadcastBuildNotPublished,
	#[error("broadcast probe must be scan-linear or a left-deep broadcast star")]
	BroadcastProbeNotStarLinear,
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
	#[error("fragment id namespace {namespace} exceeds the stage/exchange id range")]
	FragmentIdNamespaceOverflow { namespace: u32 },
}

const FRAGMENT_ID_NAMESPACE_STRIDE: u32 = 1_000_000;

#[derive(Debug)]
struct FragmentIdAllocator {
	next_stage: u32,
	next_exchange: u32,
}

impl FragmentIdAllocator {
	fn new(id_namespace: u32) -> Result<Self, FragmentError> {
		let base = id_namespace
			.checked_mul(FRAGMENT_ID_NAMESPACE_STRIDE)
			.ok_or(FragmentError::FragmentIdNamespaceOverflow {
				namespace: id_namespace,
			})?;
		Ok(Self {
			next_stage: base,
			next_exchange: base,
		})
	}

	fn alloc_stage(&mut self) -> StageId {
		let id = StageId::new(self.next_stage);
		self.next_stage = self.next_stage.saturating_add(1);
		id
	}

	fn alloc_exchange(&mut self) -> ExchangeId {
		let id = ExchangeId::new(self.next_exchange);
		self.next_exchange = self.next_exchange.saturating_add(1);
		id
	}
}

#[derive(Debug, Clone)]
enum FragmentTerminalOutput {
	FinalResult,
	ShuffleProducer {
		exchange_id: ExchangeId,
		partition_count: u32,
		partition_key: BoundExprProgram,
	},
}

impl FragmentTerminalOutput {
	fn task_output(&self) -> TaskOutputBinding {
		match self {
			Self::FinalResult => TaskOutputBinding::FinalResult,
			Self::ShuffleProducer {
				exchange_id,
				partition_count,
				partition_key,
			} => TaskOutputBinding::ShuffleProducer {
				exchange_id: *exchange_id,
				partition_count: PartitionCount::new(*partition_count),
				partition_key: partition_key.clone(),
			},
		}
	}

	fn wrap_plan(&self, plan: ExecutionPlan) -> ExecutionPlan {
		match self {
			Self::FinalResult => plan,
			Self::ShuffleProducer {
				partition_count,
				partition_key,
				..
			} => {
				if matches!(
					&plan,
					ExecutionPlan::Exchange {
						partition_count: existing_partition_count,
						key,
						..
					} if existing_partition_count == partition_count && key == partition_key
				) {
					plan
				} else {
					ExecutionPlan::Exchange {
						input: Box::new(plan),
						partition_count: *partition_count,
						key: partition_key.clone(),
					}
				}
			}
		}
	}
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

	let mut allocator = FragmentIdAllocator::new(config.id_namespace)?;
	let (stages, _) = fragment_execution_plan(
		plan,
		FragmentTerminalOutput::FinalResult,
		&mut allocator,
		config,
	)?;
	Ok(stages)
}

fn fragment_execution_plan(
	plan: ExecutionPlan,
	terminal_output: FragmentTerminalOutput,
	allocator: &mut FragmentIdAllocator,
	config: FragmentPlanConfig,
) -> Result<(Vec<StagePlan>, StageId), FragmentError> {
	if let Some(barrier) = global_sort_barrier(&plan) {
		let exchange_id = allocator.alloc_exchange();
		let (mut stages, input_terminal_stage) = fragment_execution_plan(
			barrier.input.clone(),
			FragmentTerminalOutput::ShuffleProducer {
				exchange_id,
				partition_count: 1,
				partition_key: BoundExprProgram::empty(),
			},
			allocator,
			config.clone(),
		)?;
		let sort_stage_id = allocator.alloc_stage();
		stages.push(global_sort_barrier_stage(
			config,
			sort_stage_id,
			input_terminal_stage,
			exchange_id,
			barrier,
			terminal_output,
		)?);
		return Ok((stages, sort_stage_id));
	}
	if let Some(boundary) = leading_exchange_before_join(&plan) {
		return fragment_leading_exchange_plan(plan, boundary, terminal_output, allocator, config);
	}

	fragment_base_plan(plan, terminal_output, allocator, config)
}

/// Fragment a plan at its outermost exchange `boundary`: peel that exchange, recursively fragment
/// its input as a `ShuffleProducer` (so any inner exchange fragments too), then build a consumer
/// stage that reads the shuffle and runs the operators above the boundary. Shared by the post-join
/// leading-exchange path and the stacked-exchange path (ADR 247) — both peel the *outermost*
/// exchange, so the recursion fragments an arbitrary linear stack one boundary at a time.
fn fragment_leading_exchange_plan(
	plan: ExecutionPlan,
	boundary: ExchangeBoundary,
	terminal_output: FragmentTerminalOutput,
	allocator: &mut FragmentIdAllocator,
	config: FragmentPlanConfig,
) -> Result<(Vec<StagePlan>, StageId), FragmentError> {
	if boundary.partition_count == 0 {
		return Err(FragmentError::ZeroPartitionCount);
	}

	let exchange_id = allocator.alloc_exchange();
	let (mut stages, input_terminal_stage) = fragment_execution_plan(
		boundary.input.clone(),
		FragmentTerminalOutput::ShuffleProducer {
			exchange_id,
			partition_count: boundary.partition_count,
			partition_key: boundary.key.clone(),
		},
		allocator,
		config.clone(),
	)?;
	let consumer_stage_id = allocator.alloc_stage();
	let scan_metadata =
		first_scan_metadata(&boundary.input).ok_or(FragmentError::MissingScanMetadata)?;
	let consumer_scan = shuffle_input_scan(scan_metadata, boundary.input.schema(), exchange_id);
	let consumer_plan = terminal_output.wrap_plan(replace_exchange_with_scan(plan, &consumer_scan));
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
				join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
				plan: consumer_plan.clone(),
				inputs: vec![TaskInputBinding::ShuffleConsumer {
					exchange_id,
					producer_stage_id: input_terminal_stage,
					partition_id,
					producer_partitions: Vec::new(),
				}],
				output: terminal_output.task_output(),
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();
	stages.push(StagePlan {
		stage_id: consumer_stage_id,
		upstream_stages: vec![input_terminal_stage],
		tasks: consumer_tasks,
	});
	Ok((stages, consumer_stage_id))
}

fn fragment_base_plan(
	plan: ExecutionPlan,
	terminal_output: FragmentTerminalOutput,
	allocator: &mut FragmentIdAllocator,
	config: FragmentPlanConfig,
) -> Result<(Vec<StagePlan>, StageId), FragmentError> {
	if contains_join(&plan) {
		return fragment_join_plan(plan, terminal_output, allocator, config);
	}
	if let ExecutionPlan::Union { .. } = plan {
		return fragment_union_plan(plan, terminal_output, allocator, config);
	}

	let exchange_count = count_exchanges(&plan);
	if exchange_count == 0 {
		let stage_id = allocator.alloc_stage();
		return Ok((
			vec![single_task_stage(plan, terminal_output, stage_id, config)],
			stage_id,
		));
	}
	if exchange_count > 1 {
		// ADR 247: a linear stack of exchanges (non-join, non-union — those keep their own paths)
		// fragments by peeling the outermost exchange and recursing on its input. Each peel drops the
		// count by one until the single-exchange base case below applies.
		let boundary = find_exchange(&plan).expect("exchange count > 1 has an exchange");
		return fragment_leading_exchange_plan(plan, boundary, terminal_output, allocator, config);
	}

	let boundary = find_exchange(&plan).expect("exchange count was non-zero");
	if boundary.partition_count == 0 {
		return Err(FragmentError::ZeroPartitionCount);
	}

	let exchange_id = allocator.alloc_exchange();
	let producer_stage_id = allocator.alloc_stage();
	let consumer_stage_id = allocator.alloc_stage();
	let scan_metadata =
		first_scan_metadata(&boundary.input).ok_or(FragmentError::MissingScanMetadata)?;
	let consumer_scan = shuffle_input_scan(scan_metadata, boundary.input.schema(), exchange_id);
	let consumer_plan = terminal_output.wrap_plan(replace_exchange_with_scan(plan, &consumer_scan));
	let producer_task = TaskPlan::new(TaskSpec {
		identity: TaskIdentity::new(
			config.query_id,
			producer_stage_id,
			TaskId::new(0),
			AttemptId::first(),
		),
		partition: None,
		context: config.task_context,
		join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
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
				join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
				plan: consumer_plan.clone(),
				inputs: vec![TaskInputBinding::ShuffleConsumer {
					exchange_id,
					producer_stage_id,
					partition_id,
					producer_partitions: Vec::new(),
				}],
				output: terminal_output.task_output(),
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();

	let stages = vec![
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
	];
	Ok((stages, consumer_stage_id))
}

fn fragment_union_plan(
	plan: ExecutionPlan,
	terminal_output: FragmentTerminalOutput,
	allocator: &mut FragmentIdAllocator,
	config: FragmentPlanConfig,
) -> Result<(Vec<StagePlan>, StageId), FragmentError> {
	let ExecutionPlan::Union {
		left,
		right,
		output_schema,
	} = plan
	else {
		unreachable!("fragment_union_plan called with non-union plan");
	};
	let exchange_count = count_exchanges(&left) + count_exchanges(&right);
	if exchange_count != 0 {
		return Err(FragmentError::MultipleExchangeBoundaries {
			count: exchange_count,
		});
	}

	let partition_count = config.worker_count.max(1);
	let left_exchange_id = allocator.alloc_exchange();
	let right_exchange_id = allocator.alloc_exchange();
	let left_stage_id = allocator.alloc_stage();
	let right_stage_id = allocator.alloc_stage();
	let union_stage_id = allocator.alloc_stage();
	let left_schema = left.schema();
	let right_schema = right.schema();
	let left_scan_metadata =
		first_scan_metadata(&left).ok_or(FragmentError::MissingScanMetadata)?;
	let right_scan_metadata =
		first_scan_metadata(&right).ok_or(FragmentError::MissingScanMetadata)?;
	let left_key = all_output_partition_key(&left_schema);
	let right_key = all_output_partition_key(&right_schema);

	let left_tasks = union_producer_tasks(
		config.clone(),
		left_stage_id,
		left_exchange_id,
		*left,
		partition_count,
		left_key.clone(),
	);
	let right_tasks = union_producer_tasks(
		config.clone(),
		right_stage_id,
		right_exchange_id,
		*right,
		partition_count,
		right_key.clone(),
	);
	let consumer_plan = terminal_output.wrap_plan(ExecutionPlan::Union {
		left: Box::new(shuffle_input_scan(
			left_scan_metadata,
			left_schema,
			left_exchange_id,
		)),
		right: Box::new(shuffle_input_scan(
			right_scan_metadata,
			right_schema,
			right_exchange_id,
		)),
		output_schema,
	});
	let consumer_output = terminal_output.task_output();
	let consumer_tasks = (0..partition_count)
		.map(|partition| {
			let partition_id = PartitionId::new(partition);
			TaskPlan::new(TaskSpec {
				identity: TaskIdentity::new(
					config.query_id,
					union_stage_id,
					TaskId::new(partition),
					AttemptId::first(),
				),
				partition: Some(partition_id),
				context: config.task_context,
				join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
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

	Ok((
		vec![
			StagePlan {
				stage_id: left_stage_id,
				upstream_stages: vec![],
				tasks: left_tasks,
			},
			StagePlan {
				stage_id: right_stage_id,
				upstream_stages: vec![],
				tasks: right_tasks,
			},
			StagePlan {
				stage_id: union_stage_id,
				upstream_stages: vec![left_stage_id, right_stage_id],
				tasks: consumer_tasks,
			},
		],
		union_stage_id,
	))
}

fn union_producer_tasks(
	config: FragmentPlanConfig,
	stage_id: StageId,
	exchange_id: ExchangeId,
	input: ExecutionPlan,
	partition_count: u32,
	key: BoundExprProgram,
) -> Vec<TaskPlan> {
	(0..partition_count)
		.map(|worker_index| {
			TaskPlan::new(TaskSpec {
				identity: TaskIdentity::new(
					config.query_id,
					stage_id,
					TaskId::new(worker_index),
					AttemptId::first(),
				),
				partition: None,
				context: config.task_context,
				join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
				plan: ExecutionPlan::Exchange {
					input: Box::new(input.clone()),
					partition_count,
					key: key.clone(),
				},
				inputs: vec![TaskInputBinding::Scan {
					share: Some(ScanShare {
						worker_index,
						worker_count: partition_count,
					}),
					runtime_filter: None,
				}],
				output: TaskOutputBinding::ShuffleProducer {
					exchange_id,
					partition_count: PartitionCount::new(partition_count),
					partition_key: key.clone(),
				},
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect()
}

fn all_output_partition_key(schema: &SchemaRef) -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	for (idx, field) in schema.fields().iter().enumerate() {
		let column = builder.append_column(ResolvedColumn {
			name: ColumnName(field.name().clone()),
			ordinal: ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow")),
			data_type: field.data_type().clone(),
			nullable: field.is_nullable(),
			pbc_eligible: false,
		});
		builder.push_root(column);
	}
	builder.build()
}

fn fragment_join_plan(
	plan: ExecutionPlan,
	terminal_output: FragmentTerminalOutput,
	allocator: &mut FragmentIdAllocator,
	config: FragmentPlanConfig,
) -> Result<(Vec<StagePlan>, StageId), FragmentError> {
	let join = first_join(&plan).expect("join presence checked");
	match join.strategy {
		JoinStrategy::HashPartitioned => {
			fragment_hash_join_plan(plan, join, terminal_output, allocator, config)
		}
		JoinStrategy::Broadcast => {
			fragment_broadcast_join_plan(plan, join, terminal_output, allocator, config)
		}
	}
}

fn fragment_hash_join_plan(
	plan: ExecutionPlan,
	join: JoinBoundary,
	terminal_output: FragmentTerminalOutput,
	allocator: &mut FragmentIdAllocator,
	config: FragmentPlanConfig,
) -> Result<(Vec<StagePlan>, StageId), FragmentError> {
	let runtime_filter_enabled =
		config.runtime_filters_enabled && runtime_filter_join_eligible(&join);
	reject_sort_barrier_inside_join_inputs(&join)?;
	reject_undecomposable_aggregate_in_join_consumer(&plan)?;
	let left_boundary = extract_exchange(*join.left)?;
	let right_boundary = extract_exchange(*join.right)?;
	if left_boundary.partition_count == 0 || right_boundary.partition_count == 0 {
		return Err(FragmentError::ZeroPartitionCount);
	}
	if left_boundary.partition_count != right_boundary.partition_count {
		return Err(FragmentError::JoinInputNotExchanged);
	}

	let left_exchange_id = allocator.alloc_exchange();
	let right_exchange_id = allocator.alloc_exchange();
	let left_stage_id = allocator.alloc_stage();
	let right_stage_id = allocator.alloc_stage();
	let join_stage_id = allocator.alloc_stage();
	let left_scan_metadata =
		first_scan_metadata(&left_boundary.input).ok_or(FragmentError::MissingScanMetadata)?;
	let right_scan_metadata =
		first_scan_metadata(&right_boundary.input).ok_or(FragmentError::MissingScanMetadata)?;
	let join_replacement = ExecutionPlan::Join {
		replay_id: join.replay_id,
		replay_eligibility: join.replay_eligibility,
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
		match_filter: join.match_filter,
		null_safe_keys: join.null_safe_keys,
		strategy: join.strategy,
		output_schema: join.output_schema,
	};
	let consumer_plan = terminal_output.wrap_plan(replace_first_join(plan, join_replacement));
	let consumer_output = terminal_output.task_output();
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
				join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
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

	let stages = vec![
		StagePlan {
			stage_id: left_stage_id,
			upstream_stages: vec![],
			tasks: vec![producer_task(
				config.clone(),
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
				config.clone(),
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
	Ok((stages, join_stage_id))
}

fn fragment_broadcast_join_plan(
	plan: ExecutionPlan,
	join: JoinBoundary,
	terminal_output: FragmentTerminalOutput,
	allocator: &mut FragmentIdAllocator,
	config: FragmentPlanConfig,
) -> Result<(Vec<StagePlan>, StageId), FragmentError> {
	if !safe_broadcast_physical_join_kind(join.kind) {
		return Err(FragmentError::BroadcastNonInner { kind: join.kind });
	}
	reject_sort_barrier_inside_join_inputs(&join)?;
	reject_undecomposable_aggregate_in_join_consumer(&plan)?;
	let BroadcastStarPlan {
		plan: consumer_plan,
		builds,
	} = collect_broadcast_star_plan(plan, allocator)?;
	if builds.is_empty() {
		return Err(FragmentError::BroadcastProbeNotStarLinear);
	}
	let join_stage_id = allocator.alloc_stage();
	let worker_count = config.worker_count.max(1);
	let consumer_plan = terminal_output.wrap_plan(consumer_plan);
	let consumer_output = terminal_output.task_output();
	let build_inputs = builds
		.iter()
		.map(|build| TaskInputBinding::ShuffleConsumer {
			exchange_id: build.exchange_id,
			producer_stage_id: build.stage_id,
			partition_id: PartitionId::new(0),
			producer_partitions: Vec::new(),
		})
		.collect::<Vec<_>>();
	let consumer_tasks = (0..worker_count)
		.map(|worker_index| {
			let mut inputs = Vec::with_capacity(1 + build_inputs.len());
			inputs.push(TaskInputBinding::Scan {
				share: Some(ScanShare {
					worker_index,
					worker_count,
				}),
				runtime_filter: None,
			});
			inputs.extend(build_inputs.clone());
			TaskPlan::new(TaskSpec {
				identity: TaskIdentity::new(
					config.query_id,
					join_stage_id,
					TaskId::new(worker_index),
					AttemptId::first(),
				),
				partition: None,
				context: config.task_context,
				join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
				plan: consumer_plan.clone(),
				inputs,
				output: consumer_output.clone(),
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();

	let mut upstream_stages = Vec::with_capacity(builds.len());
	let mut stages = Vec::with_capacity(builds.len() + 1);
	for build in builds {
		upstream_stages.push(build.stage_id);
		stages.push(StagePlan {
			stage_id: build.stage_id,
			upstream_stages: vec![],
			tasks: vec![producer_task(
				config.clone(),
				build.stage_id,
				build.exchange_id,
				build.boundary,
			)],
		});
	}
	stages.push(StagePlan {
		stage_id: join_stage_id,
		upstream_stages,
		tasks: consumer_tasks,
	});
	Ok((stages, join_stage_id))
}

fn collect_broadcast_star_plan(
	plan: ExecutionPlan,
	allocator: &mut FragmentIdAllocator,
) -> Result<BroadcastStarPlan, FragmentError> {
	let mut builds = Vec::new();
	let plan = rewrite_broadcast_star_probe(plan, allocator, &mut builds)?;
	Ok(BroadcastStarPlan { plan, builds })
}

fn rewrite_broadcast_star_probe(
	plan: ExecutionPlan,
	allocator: &mut FragmentIdAllocator,
	builds: &mut Vec<BroadcastStarBuild>,
) -> Result<ExecutionPlan, FragmentError> {
	rewrite_broadcast_star_probe_inner(plan, allocator, builds, true)
}

fn rewrite_broadcast_star_probe_inner(
	plan: ExecutionPlan,
	allocator: &mut FragmentIdAllocator,
	builds: &mut Vec<BroadcastStarBuild>,
	root: bool,
) -> Result<ExecutionPlan, FragmentError> {
	match plan {
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left,
			right,
			kind,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema,
		} => {
			if root {
				if !safe_broadcast_physical_join_kind(kind) {
					return Err(FragmentError::BroadcastNonInner { kind });
				}
			} else if !matches!(kind, JoinKind::Inner | JoinKind::Cross) {
				// ADR 239: a left-deep comma fold (`Cross(Cross(a, b), c)`) nests CROSS inside the
				// broadcast star. CROSS composes like INNER here — each level match-alls the broadcast
				// build against the continuing probe with no null-fill — so it is a valid non-root kind.
				return Err(FragmentError::BroadcastNonInner { kind });
			}
			if strategy != JoinStrategy::Broadcast {
				return Err(FragmentError::BroadcastProbeNotStarLinear);
			}
			let build_boundary = extract_exchange(*left)?;
			if build_boundary.partition_count != 1 {
				return Err(FragmentError::BroadcastBuildNotPublished);
			}
			let build_scan_metadata = first_scan_metadata(&build_boundary.input)
				.ok_or(FragmentError::MissingScanMetadata)?;
			let build_exchange_id = allocator.alloc_exchange();
			let build_stage_id = allocator.alloc_stage();
			let build_scan = shuffle_input_scan(
				build_scan_metadata,
				build_boundary.input.schema(),
				build_exchange_id,
			);
			builds.push(BroadcastStarBuild {
				exchange_id: build_exchange_id,
				stage_id: build_stage_id,
				boundary: build_boundary,
			});
			Ok(ExecutionPlan::Join {
				replay_id,
				replay_eligibility,
				left: Box::new(build_scan),
				right: Box::new(rewrite_broadcast_star_probe_inner(
					*right, allocator, builds, false,
				)?),
				kind,
				left_keys,
				right_keys,
				match_filter,
			null_safe_keys,
				strategy,
				output_schema,
			})
		}
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => Ok(ExecutionPlan::Filter {
			input: Box::new(rewrite_broadcast_star_probe_inner(
				*input, allocator, builds, root,
			)?),
			predicate,
			disposition,
		}),
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} => Ok(ExecutionPlan::Project {
			input: Box::new(rewrite_broadcast_star_probe_inner(
				*input, allocator, builds, root,
			)?),
			exprs,
			output_schema,
		}),
		ExecutionPlan::Repeat {
			input,
			count_column,
			output_schema,
		} => Ok(ExecutionPlan::Repeat {
			input: Box::new(rewrite_broadcast_star_probe_inner(
				*input, allocator, builds, root,
			)?),
			count_column,
			output_schema,
		}),
		leaf @ ExecutionPlan::Scan {
			row_limit: None, ..
		} => Ok(leaf),
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::Union { .. }
		| ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Aggregate { .. }
		| ExecutionPlan::Exchange { .. }
		| ExecutionPlan::Sort { .. }
		| ExecutionPlan::Window { .. }
		| ExecutionPlan::Take { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => Err(FragmentError::BroadcastProbeNotStarLinear),
	}
}

fn safe_broadcast_physical_join_kind(kind: JoinKind) -> bool {
	// ADR 239: CROSS is broadcast-only (a keyless cartesian; a hash-partition on `[]` collapses to a
	// single bucket), so it joins Inner and the right-build kinds as a broadcast-safe physical kind.
	matches!(
		kind,
		JoinKind::Inner
			| JoinKind::Cross
			| JoinKind::RightOuter
			| JoinKind::RightSemi
			| JoinKind::RightAnti
	)
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
		join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
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

fn single_task_stage(
	plan: ExecutionPlan,
	terminal_output: FragmentTerminalOutput,
	stage_id: StageId,
	config: FragmentPlanConfig,
) -> StagePlan {
	let plan = terminal_output.wrap_plan(plan);
	StagePlan {
		stage_id,
		upstream_stages: vec![],
		tasks: vec![TaskPlan::new(TaskSpec {
			identity: TaskIdentity::new(
				config.query_id,
				stage_id,
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: None,
			context: config.task_context,
			join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
			plan,
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: terminal_output.task_output(),
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
	replay_id: JoinReplayId,
	replay_eligibility: BroadcastReplayEligibility,
	left: Box<ExecutionPlan>,
	right: Box<ExecutionPlan>,
	kind: JoinKind,
	left_keys: Vec<ColumnOrdinal>,
	right_keys: Vec<ColumnOrdinal>,
	match_filter: Option<BoundExprProgram>,
	null_safe_keys: bool,
	strategy: JoinStrategy,
	output_schema: SchemaRef,
}

#[derive(Debug, Clone)]
struct BroadcastStarBuild {
	exchange_id: ExchangeId,
	stage_id: StageId,
	boundary: ExchangeBoundary,
}

#[derive(Debug, Clone)]
struct BroadcastStarPlan {
	plan: ExecutionPlan,
	builds: Vec<BroadcastStarBuild>,
}

#[derive(Debug, Clone)]
struct GlobalSortBarrier {
	input: ExecutionPlan,
	order_by: Vec<ExecutionSortKey>,
	limit: Option<usize>,
	offset: usize,
	output_schema: SchemaRef,
	tail: Vec<UnaryPlanTail>,
}

#[derive(Debug, Clone)]
enum UnaryPlanTail {
	Filter {
		predicate: BoundExprProgram,
		disposition: FilterDisposition,
	},
	Project {
		exprs: BoundExprProgram,
		output_schema: SchemaRef,
	},
}

impl UnaryPlanTail {
	fn apply(self, input: ExecutionPlan) -> ExecutionPlan {
		match self {
			Self::Filter {
				predicate,
				disposition,
			} => ExecutionPlan::Filter {
				input: Box::new(input),
				predicate,
				disposition,
			},
			Self::Project {
				exprs,
				output_schema,
			} => ExecutionPlan::Project {
				input: Box::new(input),
				exprs,
				output_schema,
			},
		}
	}
}

fn global_sort_barrier(plan: &ExecutionPlan) -> Option<GlobalSortBarrier> {
	let mut cursor = plan;
	let mut tail = Vec::new();
	loop {
		match cursor {
			ExecutionPlan::Filter {
				input,
				predicate,
				disposition,
			} => {
				tail.push(UnaryPlanTail::Filter {
					predicate: predicate.clone(),
					disposition: *disposition,
				});
				cursor = input.as_ref();
			}
			ExecutionPlan::Project {
				input,
				exprs,
				output_schema,
			} => {
				tail.push(UnaryPlanTail::Project {
					exprs: exprs.clone(),
					output_schema: output_schema.clone(),
				});
				cursor = input.as_ref();
			}
			ExecutionPlan::Sort {
				input,
				order_by,
				limit,
				offset,
				output_schema,
			} => {
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
				return Some(GlobalSortBarrier {
					input: input.as_ref().clone(),
					order_by: order_by.clone(),
					limit: *limit,
					offset: *offset,
					output_schema: output_schema.clone(),
					tail,
				});
			}
			_ => return None,
		}
	}
}

fn global_sort_barrier_stage(
	config: FragmentPlanConfig,
	stage_id: StageId,
	producer_stage_id: StageId,
	exchange_id: ExchangeId,
	barrier: GlobalSortBarrier,
	terminal_output: FragmentTerminalOutput,
) -> Result<StagePlan, FragmentError> {
	let scan_metadata =
		first_scan_metadata(&barrier.input).ok_or(FragmentError::MissingScanMetadata)?;
	let input_schema = barrier.input.schema();
	let sort_plan = ExecutionPlan::Sort {
		input: Box::new(shuffle_input_scan(scan_metadata, input_schema, exchange_id)),
		order_by: barrier.order_by,
		limit: barrier.limit,
		offset: barrier.offset,
		output_schema: barrier.output_schema,
	};
	let plan = barrier
		.tail
		.into_iter()
		.rev()
		.fold(sort_plan, |input, tail| tail.apply(input));
	let plan = terminal_output.wrap_plan(plan);

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
			join_runtime_observation_mode: config.join_runtime_observation_mode.clone(),
			plan,
			inputs: vec![TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id,
				partition_id: PartitionId::new(0),
				producer_partitions: Vec::new(),
			}],
			output: terminal_output.task_output(),
			budget: config.task_budget,
			trace: None,
		})],
	})
}

fn reject_sort_barrier_inside_join_inputs(join: &JoinBoundary) -> Result<(), FragmentError> {
	if global_sort_barrier_count(join.left.as_ref()) > 0
		|| global_sort_barrier_count(join.right.as_ref()) > 0
	{
		return Err(FragmentError::SortBarrierInsideJoinInput);
	}
	Ok(())
}

/// ADR 531 backstop: everything above a fragmented join is cloned verbatim into each
/// partition's consumer task, so an aggregate there must be partition-decomposable at the
/// join boundary — equal group keys proven co-located by the join's hash partitioning. A
/// global aggregate never is: it folds every input row into one state, and per-partition
/// execution would hand the coordinator one partial per partition to concatenate. The
/// binder's gather funnel is the valid shape for that class (peeled as a leading exchange
/// before this path), so this arm firing means a plan slipped past the binder discipline.
fn reject_undecomposable_aggregate_in_join_consumer(
	plan: &ExecutionPlan,
) -> Result<(), FragmentError> {
	match plan {
		// The join's inputs fragment behind their own exchange boundaries; only the
		// consumer region above the join is cloned per partition.
		ExecutionPlan::Join { .. } => Ok(()),
		ExecutionPlan::Aggregate {
			input, group_expr, ..
		} => {
			if !crate::binder::aggregate_exchange::aggregate_partition_decomposable(input, group_expr)
			{
				return Err(FragmentError::JoinConsumerAggregateNotDecomposable {
					group_key_count: group_expr.roots.len(),
				});
			}
			reject_undecomposable_aggregate_in_join_consumer(input)
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => {
			reject_undecomposable_aggregate_in_join_consumer(input)
		}
		ExecutionPlan::Union { left, right, .. } => {
			reject_undecomposable_aggregate_in_join_consumer(left)?;
			reject_undecomposable_aggregate_in_join_consumer(right)
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => Ok(()),
	}
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

fn leading_exchange_before_join(plan: &ExecutionPlan) -> Option<ExchangeBoundary> {
	match plan {
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} if contains_join(input) => Some(ExchangeBoundary {
			input: input.as_ref().clone(),
			partition_count: *partition_count,
			key: key.clone(),
		}),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => leading_exchange_before_join(input),
		ExecutionPlan::Union { left, right, .. } => {
			leading_exchange_before_join(left).or_else(|| leading_exchange_before_join(right))
		}
		ExecutionPlan::Exchange { .. }
		| ExecutionPlan::Join { .. }
		| ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => None,
	}
}

fn first_join(plan: &ExecutionPlan) -> Option<JoinBoundary> {
	match plan {
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left,
			right,
			kind,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema,
		} => Some(JoinBoundary {
			replay_id: *replay_id,
			replay_eligibility: *replay_eligibility,
			left: left.clone(),
			right: right.clone(),
			kind: *kind,
			left_keys: left_keys.clone(),
			right_keys: right_keys.clone(),
			match_filter: match_filter.clone(),
			null_safe_keys: *null_safe_keys,
			strategy: *strategy,
			output_schema: output_schema.clone(),
		}),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => first_join(input),
		ExecutionPlan::Union { left, right, .. } => first_join(left).or_else(|| first_join(right)),
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => None,
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
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => find_exchange(input),
		ExecutionPlan::Join { left, right, .. } => {
			find_exchange(left).or_else(|| find_exchange(right))
		}
		ExecutionPlan::Union { left, right, .. } => {
			find_exchange(left).or_else(|| find_exchange(right))
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => None,
	}
}

fn count_exchanges(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Exchange { input, .. } => 1 + count_exchanges(input),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => count_exchanges(input),
		ExecutionPlan::Join { left, right, .. } => count_exchanges(left) + count_exchanges(right),
		ExecutionPlan::Union { left, right, .. } => count_exchanges(left) + count_exchanges(right),
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => 0,
	}
}

fn global_sort_barrier_count(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Sort { input, .. } => {
			let here = match input.as_ref() {
				ExecutionPlan::Exchange {
					partition_count,
					key,
					..
				} if *partition_count == 1 && key.is_empty_roots() => 1,
				_ => 0,
			};
			here + global_sort_barrier_count(input)
		}
		ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => global_sort_barrier_count(input),
		ExecutionPlan::Join { left, right, .. } => {
			global_sort_barrier_count(left) + global_sort_barrier_count(right)
		}
		ExecutionPlan::Union { left, right, .. } => {
			global_sort_barrier_count(left) + global_sort_barrier_count(right)
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => 0,
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
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => first_scan_metadata(input),
		ExecutionPlan::Join { left, right, .. } => {
			first_scan_metadata(left).or_else(|| first_scan_metadata(right))
		}
		ExecutionPlan::Union { left, right, .. } => {
			first_scan_metadata(left).or_else(|| first_scan_metadata(right))
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => None,
	}
}

fn shuffle_input_scan(
	metadata: ScanMetadata,
	schema: SchemaRef,
	exchange_id: ExchangeId,
) -> ExecutionPlan {
	ExecutionPlan::Scan {
		local_table_id: shuffle_input_table_id(exchange_id),
		table: metadata.table,
		fingerprint: metadata.fingerprint,
		schema,
		projection: None,
		pbc_program: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		row_limit: None,
		profile_hint: QueryProfileHint::Scan,
		estimated_byte_size: None,
		primary_key_columns: Vec::new(),
		column_ndv: Vec::new(),
		column_range: Vec::new(),
		column_range_distributions: Vec::new(),
		column_heavy_hitters: Vec::new(),
		column_group_top_k: Vec::new(),
		column_group_ndv: Vec::new(),
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
		ExecutionPlan::Repeat {
			input,
			count_column,
			output_schema,
		} => ExecutionPlan::Repeat {
			input: Box::new(replace_exchange_with_scan(*input, scan)),
			count_column,
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
		ExecutionPlan::Window {
			input,
			partition_by,
			order_by,
			functions,
			output_schema,
		} => ExecutionPlan::Window {
			input: Box::new(replace_exchange_with_scan(*input, scan)),
			partition_by,
			order_by,
			functions,
			output_schema,
		},
		ExecutionPlan::Take {
			input,
			limit,
			output_schema,
		} => ExecutionPlan::Take {
			input: Box::new(replace_exchange_with_scan(*input, scan)),
			limit,
			output_schema,
		},
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left,
			right,
			kind,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema,
		} => ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left: Box::new(replace_exchange_with_scan(*left, scan)),
			right: Box::new(replace_exchange_with_scan(*right, scan)),
			kind,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy,
			output_schema,
		},
		ExecutionPlan::Union {
			left,
			right,
			output_schema,
		} => ExecutionPlan::Union {
			left: Box::new(replace_exchange_with_scan(*left, scan)),
			right: Box::new(replace_exchange_with_scan(*right, scan)),
			output_schema,
		},
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => plan,
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
		ExecutionPlan::Repeat {
			input,
			count_column,
			output_schema,
		} => ExecutionPlan::Repeat {
			input: Box::new(replace_first_join(*input, replacement)),
			count_column,
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
		ExecutionPlan::Window {
			input,
			partition_by,
			order_by,
			functions,
			output_schema,
		} => ExecutionPlan::Window {
			input: Box::new(replace_first_join(*input, replacement)),
			partition_by,
			order_by,
			functions,
			output_schema,
		},
		ExecutionPlan::Take {
			input,
			limit,
			output_schema,
		} => ExecutionPlan::Take {
			input: Box::new(replace_first_join(*input, replacement)),
			limit,
			output_schema,
		},
		ExecutionPlan::Union {
			left,
			right,
			output_schema,
		} => ExecutionPlan::Union {
			left: Box::new(replace_first_join(*left, replacement.clone())),
			right: Box::new(replace_first_join(*right, replacement)),
			output_schema,
		},
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE runs as one self-contained local task; its body is invisible to
		// outer join/exchange fragmentation. Both nodes are opaque local leaves to every walker here.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => plan,
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
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy,
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

	/// A linear stack of `depth` exchanges over a scan.
	fn stacked_exchanges(depth: usize) -> ExecutionPlan {
		let mut plan = scan_plan();
		for _ in 0..depth {
			plan = ExecutionPlan::Exchange {
				input: Box::new(plan),
				partition_count: 2,
				key: key_expr(),
			};
		}
		plan
	}

	fn produced_exchange(stage: &StagePlan) -> ExchangeId {
		match &stage.tasks[0].spec.output {
			TaskOutputBinding::ShuffleProducer { exchange_id, .. } => *exchange_id,
			other => panic!("expected a ShuffleProducer output, got {other:?}"),
		}
	}

	fn consumed_exchange(stage: &StagePlan) -> ExchangeId {
		match &stage.tasks[0].spec.inputs[0] {
			TaskInputBinding::ShuffleConsumer { exchange_id, .. } => *exchange_id,
			other => panic!("expected a ShuffleConsumer input, got {other:?}"),
		}
	}

	#[test]
	fn stacked_exchanges_fragment_into_n_plus_one_stages() {
		// ADR 247: two stacked exchanges fragment into three chained stages — reconciles the former
		// `multiple_exchange_boundaries_are_rejected_for_seed_fragmenter`, which asserted a reject.
		let stages =
			fragment_plan(stacked_exchanges(2), config()).expect("fragment two stacked exchanges");
		assert_eq!(stages.len(), 3, "two exchanges -> three stages");
		assert_eq!(stages[0].upstream_stages, Vec::<StageId>::new());
		assert_eq!(stages[1].upstream_stages, vec![stages[0].stage_id]);
		assert_eq!(stages[2].upstream_stages, vec![stages[1].stage_id]);
		assert!(
			matches!(
				stages[2].tasks[0].spec.output,
				TaskOutputBinding::FinalResult
			),
			"the outermost stage emits the final result"
		);
	}

	#[test]
	fn three_stacked_exchanges_fragment_into_four_stages() {
		// Depth generalizes — the recursion is not hard-capped at two.
		let stages = fragment_plan(stacked_exchanges(3), config())
			.expect("fragment three stacked exchanges");
		assert_eq!(stages.len(), 4, "three exchanges -> four stages");
		assert!(matches!(
			stages.last().unwrap().tasks[0].spec.output,
			TaskOutputBinding::FinalResult
		));
	}

	#[test]
	fn stacked_exchange_stage_wiring_is_chained() {
		// The middle stage consumes the inner exchange and produces the outer one; the final stage
		// consumes the middle exchange. The exchange ids chain producer -> consumer through the stack.
		let stages =
			fragment_plan(stacked_exchanges(2), config()).expect("fragment stacked exchanges");
		assert_eq!(
			consumed_exchange(&stages[1]),
			produced_exchange(&stages[0]),
			"middle stage consumes the inner stage's exchange"
		);
		assert_eq!(
			consumed_exchange(&stages[2]),
			produced_exchange(&stages[1]),
			"final stage consumes the middle stage's exchange"
		);
	}

	#[test]
	fn single_exchange_plan_still_fragments_to_two_stages() {
		// Regression guard: the unchanged one-exchange path still produces a producer + consumer pair.
		let stages = fragment_plan(exchanged_scan(TableId(1), 2), config())
			.expect("fragment single exchange");
		assert_eq!(stages.len(), 2, "one exchange -> producer + consumer");
		assert_eq!(stages[1].upstream_stages, vec![stages[0].stage_id]);
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
					ExecutionPlan::Scan { local_table_id, .. }
						if *local_table_id == crate::distributed::task::shuffle_input_table_id(ExchangeId::new(0))
				));
				assert!(matches!(
					&**right,
					ExecutionPlan::Scan { local_table_id, .. }
						if *local_table_id == crate::distributed::task::shuffle_input_table_id(ExchangeId::new(1))
				));
			}
			other => panic!("expected join consumer plan, got {other:?}"),
		}
	}

	#[test]
	fn adr489_multiworker_fragmenter_rejects_a_local_hash_shape_loudly() {
		// ADR 489's local form deliberately has no terminal exchanges. It is valid only
		// for a direct single-partition executor; a distributed fragment must refuse it
		// rather than silently running each worker's local subset of the join.
		let plan = join_plan(
			scan_plan_with_table(TableId(1), 1),
			scan_plan_with_table(TableId(2), 2),
			JoinStrategy::HashPartitioned,
		);

		assert_eq!(
			fragment_plan(plan, config().with_worker_count(3))
				.expect_err("a local hash join must never survive multi-worker fragmentation"),
			FragmentError::JoinInputNotExchanged
		);
	}

	#[test]
	fn adr531_scalar_aggregate_above_hash_join_consumer_rejects_loudly() {
		// A global aggregate above a fragmented join would execute once per partition and
		// hand the coordinator partials to concatenate. The valid shape is the binder's
		// gather funnel; a plan without it must fail typed, not diverge.
		let plan = scalar_aggregate_plan(join_plan(
			exchanged_scan(TableId(1), 2),
			exchanged_scan(TableId(2), 2),
			JoinStrategy::HashPartitioned,
		));

		assert_eq!(
			fragment_plan(plan, config())
				.expect_err("a scalar aggregate must never ride a partitioned join consumer"),
			FragmentError::JoinConsumerAggregateNotDecomposable { group_key_count: 0 }
		);
	}

	#[test]
	fn adr531_uncovered_group_keys_above_hash_join_consumer_reject_loudly() {
		// Group keys that are not covered by the join's partition keys split each group
		// across partitions; per-partition aggregation would emit duplicate group rows.
		let mut builder = BoundExprProgramBuilder::new();
		let uncovered = builder.append_column(ResolvedColumn {
			name: ColumnName("value".to_string()),
			ordinal: ColumnOrdinal(2),
			data_type: DataType::Int32,
			nullable: false,
			pbc_eligible: true,
		});
		builder.push_root(uncovered);
		let plan = ExecutionPlan::Aggregate {
			input: Box::new(join_plan(
				exchanged_scan(TableId(1), 2),
				exchanged_scan(TableId(2), 2),
				JoinStrategy::HashPartitioned,
			)),
			group_expr: builder.build(),
			aggr_expr: vec![BoundAggregateCall {
				function: AggregateFunction::CountAll,
				args: BoundExprProgram::empty(),
			}],
			output_schema: Arc::new(Schema::new(vec![
				Field::new("value", DataType::Int32, false),
				Field::new("count", DataType::Int64, false),
			])),
		};

		assert_eq!(
			fragment_plan(plan, config())
				.expect_err("uncovered group keys must never ride a partitioned join consumer"),
			FragmentError::JoinConsumerAggregateNotDecomposable { group_key_count: 1 }
		);
	}

	#[test]
	fn adr531_covered_group_keys_above_hash_join_consumer_still_fragment() {
		// The redundant-exchange collapse legitimately leaves an aggregate directly on a
		// join when the group keys are the join keys — equal groups are co-located, so the
		// backstop must accept exactly what that collapse emits.
		let plan = final_aggregate_plan(join_plan(
			exchanged_scan(TableId(1), 2),
			exchanged_scan(TableId(2), 2),
			JoinStrategy::HashPartitioned,
		));

		let stages = fragment_plan(plan, config()).expect("covered group keys fragment");
		assert_eq!(stages.len(), 3);
		assert!(matches!(
			stages[2].tasks[0].spec.plan,
			ExecutionPlan::Aggregate { .. }
		));
	}

	#[test]
	fn adr531_gathered_global_aggregate_over_join_fragments_to_single_task_terminal_stage() {
		// The binder's gather funnel peels as a leading exchange: the join fragments as a
		// producer below it and the global aggregate lands in a one-task terminal stage.
		let plan = scalar_aggregate_plan(ExecutionPlan::Exchange {
			input: Box::new(join_plan(
				exchanged_scan(TableId(1), 2),
				exchanged_scan(TableId(2), 2),
				JoinStrategy::HashPartitioned,
			)),
			partition_count: 1,
			key: BoundExprProgram::empty(),
		});

		let stages = fragment_plan(plan, config()).expect("fragment gathered global aggregate");
		let terminal = stages.last().expect("terminal stage");
		assert_eq!(
			stages.len(),
			4,
			"two producers, join stage, aggregate stage"
		);
		assert_eq!(terminal.tasks.len(), 1, "the gather funnels to one task");
		assert!(matches!(
			terminal.tasks[0].spec.plan,
			ExecutionPlan::Aggregate { .. }
		));
		assert!(matches!(
			terminal.tasks[0].spec.output,
			TaskOutputBinding::FinalResult
		));
	}

	#[test]
	fn adr531_scalar_aggregate_above_broadcast_join_consumer_rejects_loudly() {
		// The broadcast probe is scan-shared across workers, so no aggregate above it can
		// prove co-location; the same backstop guards this path.
		let plan = scalar_aggregate_plan(join_plan(
			exchanged_scan(TableId(1), 1),
			scan_plan_with_table(TableId(2), 2),
			JoinStrategy::Broadcast,
		));

		assert_eq!(
			fragment_plan(plan, config().with_worker_count(3))
				.expect_err("a scalar aggregate must never ride a broadcast join consumer"),
			FragmentError::JoinConsumerAggregateNotDecomposable { group_key_count: 0 }
		);
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
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(exchanged_scan(TableId(1), 2)),
			right: Box::new(exchanged_scan(TableId(2), 2)),
			kind: JoinKind::Inner,
			left_keys: vec![ColumnOrdinal(0), ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0), ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
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
	fn broadcast_composite_build_fragments_to_single_build_producer() {
		let build = join_plan(
			exchanged_scan(TableId(1), 2),
			exchanged_scan(TableId(2), 2),
			JoinStrategy::HashPartitioned,
		);
		let plan = join_plan(
			ExecutionPlan::Exchange {
				input: Box::new(build),
				partition_count: 1,
				key: key_expr(),
			},
			scan_plan_with_table(TableId(3), 3),
			JoinStrategy::Broadcast,
		);

		let stages =
			fragment_plan(plan, config().with_worker_count(3)).expect("fragment composite build");

		assert_eq!(stages.len(), 2);
		assert!(contains_join(&stages[0].tasks[0].spec.plan));
		assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
		assert_eq!(stages[1].tasks.len(), 3);
		for task in &stages[1].tasks {
			assert_eq!(task.spec.inputs.len(), 2);
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
	fn three_way_broadcast_star_flattens_to_single_probe_stage() {
		let inner = join_plan(
			exchanged_scan(TableId(1), 1),
			scan_plan_with_table(TableId(2), 2),
			JoinStrategy::Broadcast,
		);
		let outer = join_plan(
			exchanged_scan(TableId(3), 1),
			inner,
			JoinStrategy::Broadcast,
		);

		let stages =
			fragment_plan(outer, config().with_worker_count(4)).expect("fragment star broadcast");

		assert_eq!(stages.len(), 3);
		assert_eq!(
			stages[2].upstream_stages,
			vec![StageId::new(0), StageId::new(1)]
		);
		assert_eq!(stages[2].tasks.len(), 4);
		for (worker_index, task) in stages[2].tasks.iter().enumerate() {
			assert_eq!(task.spec.inputs.len(), 3);
			assert!(matches!(
				&task.spec.inputs[0],
				TaskInputBinding::Scan {
					share: Some(ScanShare {
						worker_index: observed,
						worker_count,
					}),
					..
				} if *observed == u32::try_from(worker_index).unwrap() && *worker_count == 4
			));
			for (input_index, (exchange_id, producer_stage_id)) in [
				(ExchangeId::new(0), StageId::new(0)),
				(ExchangeId::new(1), StageId::new(1)),
			]
			.into_iter()
			.enumerate()
			{
				assert!(matches!(
					&task.spec.inputs[input_index + 1],
					TaskInputBinding::ShuffleConsumer {
						exchange_id: observed_exchange,
						producer_stage_id: observed_stage,
						partition_id,
						..
					} if *observed_exchange == exchange_id
						&& *observed_stage == producer_stage_id
						&& *partition_id == PartitionId::new(0)
				));
			}
		}
	}

	#[test]
	fn pinned_broadcast_unsupported_nested_probe_rejects() {
		let nested_hash_probe = join_plan(
			exchanged_scan(TableId(2), 2),
			exchanged_scan(TableId(3), 2),
			JoinStrategy::HashPartitioned,
		);
		let plan = join_plan(
			exchanged_scan(TableId(1), 1),
			nested_hash_probe,
			JoinStrategy::Broadcast,
		);

		assert_eq!(
			fragment_plan(plan, config().with_worker_count(3)).expect_err("reject nested probe"),
			FragmentError::BroadcastProbeNotStarLinear
		);
	}

	#[test]
	fn broadcast_safe_non_inner_physical_roots_fragment() {
		for kind in [
			JoinKind::RightOuter,
			JoinKind::RightSemi,
			JoinKind::RightAnti,
		] {
			let plan = join_plan_with_kind(
				exchanged_scan(TableId(1), 1),
				scan_plan_with_table(TableId(2), 2),
				JoinStrategy::Broadcast,
				kind,
			);

			let stages = fragment_plan(plan, config().with_worker_count(3))
				.unwrap_or_else(|error| panic!("safe {kind:?} should fragment: {error:?}"));

			assert_eq!(stages.len(), 2, "{kind:?}");
			assert_eq!(stages[1].tasks.len(), 3, "{kind:?}");
			assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)], "{kind:?}");
		}
	}

	#[test]
	fn broadcast_non_inner_star_child_remains_rejected_by_fragmenter() {
		let unsafe_child = join_plan_with_kind(
			exchanged_scan(TableId(1), 1),
			scan_plan_with_table(TableId(2), 2),
			JoinStrategy::Broadcast,
			JoinKind::RightOuter,
		);
		let root = join_plan_with_kind(
			exchanged_scan(TableId(3), 1),
			unsafe_child,
			JoinStrategy::Broadcast,
			JoinKind::RightOuter,
		);

		assert_eq!(
			fragment_plan(root, config().with_worker_count(3))
				.expect_err("non-inner broadcast child remains outside broadcast-star recursion"),
			FragmentError::BroadcastNonInner {
				kind: JoinKind::RightOuter
			}
		);
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
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(left),
			right: Box::new(right),
			kind,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
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

	fn scalar_aggregate_plan(input: ExecutionPlan) -> ExecutionPlan {
		ExecutionPlan::Aggregate {
			input: Box::new(input),
			group_expr: BoundExprProgram::empty(),
			aggr_expr: vec![BoundAggregateCall {
				function: AggregateFunction::CountAll,
				args: BoundExprProgram::empty(),
			}],
			output_schema: Arc::new(Schema::new(vec![Field::new(
				"count",
				DataType::Int64,
				false,
			)])),
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("key")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
