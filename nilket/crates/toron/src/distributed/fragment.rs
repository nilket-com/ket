use arrow::datatypes::SchemaRef;
use thiserror::Error;
use wendelon::schema::{PhysicalDescriptorFingerprint, TableObjectId};

use crate::distributed::budget::TaskBudget;
use crate::distributed::identity::{
	AttemptId, ExchangeId, PartitionId, QueryId, StageId, TaskId, TaskIdentity,
};
use crate::distributed::scheduler::{StagePlan, TaskPlan};
use crate::distributed::task::{PartitionCount, TaskInputBinding, TaskOutputBinding, TaskSpec};
use crate::ir::{ExecutionExpr, ExecutionPlan, QueryProfileHint, TableId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentPlanConfig {
	pub query_id: QueryId,
	pub task_budget: TaskBudget,
}

impl FragmentPlanConfig {
	pub const fn new(query_id: QueryId, task_budget: TaskBudget) -> Self {
		Self {
			query_id,
			task_budget,
		}
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
}

pub fn fragment_plan(
	plan: ExecutionPlan,
	config: FragmentPlanConfig,
) -> Result<Vec<StagePlan>, FragmentError> {
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
	let consumer_scan = shuffle_input_scan(scan_metadata, boundary.input.schema());
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
		inputs: vec![TaskInputBinding::Scan],
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
			upstream_stage: None,
			tasks: vec![producer_task],
		},
		StagePlan {
			stage_id: consumer_stage_id,
			upstream_stage: Some(producer_stage_id),
			tasks: consumer_tasks,
		},
	])
}

fn single_task_stage(plan: ExecutionPlan, config: FragmentPlanConfig) -> StagePlan {
	StagePlan {
		stage_id: StageId::new(0),
		upstream_stage: None,
		tasks: vec![TaskPlan::new(TaskSpec {
			identity: TaskIdentity::new(
				config.query_id,
				StageId::new(0),
				TaskId::new(0),
				AttemptId::first(),
			),
			partition: None,
			plan,
			inputs: vec![TaskInputBinding::Scan],
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
		ExecutionPlan::Scan { .. } => None,
	}
}

fn count_exchanges(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Exchange { input, .. } => 1 + count_exchanges(input),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. } => count_exchanges(input),
		ExecutionPlan::Scan { .. } => 0,
	}
}

#[derive(Debug, Clone, Copy)]
struct ScanMetadata {
	legacy_table_id: TableId,
	table: TableObjectId,
	fingerprint: PhysicalDescriptorFingerprint,
}

fn first_scan_metadata(plan: &ExecutionPlan) -> Option<ScanMetadata> {
	match plan {
		ExecutionPlan::Scan {
			legacy_table_id,
			table,
			fingerprint,
			..
		} => Some(ScanMetadata {
			legacy_table_id: *legacy_table_id,
			table: *table,
			fingerprint: *fingerprint,
		}),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. } => first_scan_metadata(input),
	}
}

fn shuffle_input_scan(metadata: ScanMetadata, schema: SchemaRef) -> ExecutionPlan {
	ExecutionPlan::Scan {
		legacy_table_id: metadata.legacy_table_id,
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
		ExecutionPlan::Scan { .. } => plan,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::budget::{QueryBudget, TaskBudget};
	use crate::ir::{AggregateCall, AggregateFunction, ColumnName, ColumnOrdinal, ResolvedColumn};
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
		assert_eq!(stages[0].upstream_stage, None);
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
		assert_eq!(stages[1].upstream_stage, Some(StageId::new(0)));
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

	fn config() -> FragmentPlanConfig {
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		)
	}

	fn scan_plan() -> ExecutionPlan {
		let descriptor = descriptor();
		ExecutionPlan::Scan {
			legacy_table_id: TableId(1),
			table: descriptor.table_object_id,
			fingerprint: descriptor.physical_descriptor_fingerprint,
			schema: Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
			projection: None,
			pbc_program: None,
			profile_hint: QueryProfileHint::Scan,
			primary_key_columns: vec![ColumnOrdinal(0)],
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
