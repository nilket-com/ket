use arrow::datatypes::SchemaRef;
use thiserror::Error;
use wendelon::schema::{PhysicalDescriptorFingerprint, TableObjectId};

use crate::distributed::budget::TaskBudget;
use crate::distributed::identity::{
	AttemptId, ExchangeId, PartitionId, QueryId, StageId, TaskId, TaskIdentity,
};
use crate::distributed::scheduler::{StagePlan, TaskPlan};
use crate::distributed::task::{
	PartitionCount, ScanShare, TaskInputBinding, TaskOutputBinding, TaskSpec,
	shuffle_input_table_id,
};
use crate::ir::{ExecutionExpr, ExecutionPlan, JoinKind, JoinStrategy, QueryProfileHint};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentPlanConfig {
	pub query_id: QueryId,
	pub task_budget: TaskBudget,
	pub worker_count: u32,
}

impl FragmentPlanConfig {
	pub const fn new(query_id: QueryId, task_budget: TaskBudget) -> Self {
		Self {
			query_id,
			task_budget,
			worker_count: 1,
		}
	}

	pub const fn with_worker_count(mut self, worker_count: u32) -> Self {
		self.worker_count = worker_count;
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
	#[error("broadcast build input must terminate in a single-partition exchange")]
	BroadcastBuildNotPublished,
}

pub fn fragment_plan(
	plan: ExecutionPlan,
	config: FragmentPlanConfig,
) -> Result<Vec<StagePlan>, FragmentError> {
	if contains_join(&plan) {
		return fragment_join_plan(plan, config);
	}

	let exchange_count = count_exchanges(&plan);
	if exchange_count == 0 {
		return Ok(vec![single_task_stage(plan, config)]);
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
		plan: ExecutionPlan::Exchange {
			input: Box::new(boundary.input),
			partition_count: boundary.partition_count,
			key: boundary.key.clone(),
		},
		inputs: vec![TaskInputBinding::Scan { share: None }],
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
	let consumer_plan = replace_first_join(plan, join_replacement);
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
				output: TaskOutputBinding::FinalResult,
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();

	Ok(vec![
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
			upstream_stages: vec![],
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
	])
}

fn fragment_broadcast_join_plan(
	plan: ExecutionPlan,
	join: JoinBoundary,
	config: FragmentPlanConfig,
) -> Result<Vec<StagePlan>, FragmentError> {
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
	let consumer_plan = replace_first_join(plan, join_replacement);
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
				plan: consumer_plan.clone(),
				inputs: vec![
					TaskInputBinding::Scan {
						share: Some(ScanShare {
							worker_index,
							worker_count,
						}),
					},
					TaskInputBinding::ShuffleConsumer {
						exchange_id: build_exchange_id,
						producer_stage_id: build_stage_id,
						partition_id: PartitionId::new(0),
						producer_partitions: Vec::new(),
					},
				],
				output: TaskOutputBinding::FinalResult,
				budget: config.task_budget,
				trace: None,
			})
		})
		.collect();

	Ok(vec![
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
	])
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
		plan: ExecutionPlan::Exchange {
			input: Box::new(boundary.input),
			partition_count: boundary.partition_count,
			key: boundary.key.clone(),
		},
		inputs: vec![TaskInputBinding::Scan { share: None }],
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
			plan,
			inputs: vec![TaskInputBinding::Scan { share: None }],
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
	key: Vec<ExecutionExpr>,
}

#[derive(Debug, Clone)]
struct JoinBoundary {
	left: Box<ExecutionPlan>,
	right: Box<ExecutionPlan>,
	kind: JoinKind,
	left_keys: Vec<crate::ir::ColumnOrdinal>,
	right_keys: Vec<crate::ir::ColumnOrdinal>,
	strategy: JoinStrategy,
	output_schema: SchemaRef,
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
		| ExecutionPlan::Exchange { input, .. } => first_join(input),
		ExecutionPlan::Scan { .. } => None,
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
		| ExecutionPlan::Aggregate { input, .. } => find_exchange(input),
		ExecutionPlan::Join { left, right, .. } => {
			find_exchange(left).or_else(|| find_exchange(right))
		}
		ExecutionPlan::Scan { .. } => None,
	}
}

fn count_exchanges(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Exchange { input, .. } => 1 + count_exchanges(input),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. } => count_exchanges(input),
		ExecutionPlan::Join { left, right, .. } => count_exchanges(left) + count_exchanges(right),
		ExecutionPlan::Scan { .. } => 0,
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
		| ExecutionPlan::Exchange { input, .. } => first_scan_metadata(input),
		ExecutionPlan::Join { left, right, .. } => {
			first_scan_metadata(left).or_else(|| first_scan_metadata(right))
		}
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
		ExecutionPlan::Scan { .. } => plan,
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
		ExecutionPlan::Scan { .. } => plan,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::budget::{QueryBudget, TaskBudget};
	use crate::ir::{
		AggregateCall, AggregateFunction, ColumnName, ColumnOrdinal, ResolvedColumn, TableId,
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
			key: vec![key_expr()],
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
			key: vec![key_expr()],
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
			key: vec![key_expr()],
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
				key: vec![key_expr()],
			}),
			partition_count: 2,
			key: vec![key_expr()],
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
					})
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
			profile_hint: QueryProfileHint::Scan,
			primary_key_columns: vec![ColumnOrdinal(0)],
		}
	}

	fn exchanged_scan(table_id: TableId, partition_count: u32) -> ExecutionPlan {
		ExecutionPlan::Exchange {
			input: Box::new(scan_plan_with_table(table_id, table_id.0 as u8)),
			partition_count,
			key: vec![key_expr()],
		}
	}

	fn join_plan(
		left: ExecutionPlan,
		right: ExecutionPlan,
		strategy: JoinStrategy,
	) -> ExecutionPlan {
		ExecutionPlan::Join {
			left: Box::new(left),
			right: Box::new(right),
			kind: JoinKind::Inner,
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
			group_expr: vec![key_expr()],
			aggr_expr: vec![AggregateCall {
				function: AggregateFunction::CountAll,
				args: vec![key_expr()],
			}],
			output_schema: Arc::new(Schema::new(vec![
				Field::new("key", DataType::Int32, false),
				Field::new("count", DataType::Int64, false),
			])),
		}
	}

	fn key_expr() -> ExecutionExpr {
		ExecutionExpr::Column(ResolvedColumn {
			name: ColumnName("key".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int32,
			nullable: false,
			pbc_eligible: true,
		})
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
