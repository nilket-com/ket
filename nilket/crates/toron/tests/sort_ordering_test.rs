use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::distributed::budget::{BudgetDimension, ByteCount, QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{ExchangeId, QueryId, StageId};
use toron::distributed::task::{
	PartitionCount, ShufflePublishMode, TaskInputBinding, TaskOutputBinding,
};
use toron::error::{BindingError, ExecutionError, StorageError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	BoundExprProgram, BoundExprProgramBuilder, BroadcastReplayEligibility, ColumnName,
	ColumnOrdinal, ExecutionPlan, ExecutionSortKey, Expr, FilterDisposition, JoinKind,
	JoinReplayId, JoinStrategy, LogicalPlan, QueryProfileHint, ResolvedColumn, ScalarValue,
	SortExpr, TableId,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

#[test]
fn order_by_lowers_to_sort_over_single_partition_exchange() {
	let plan = SqlPlanner::new()
		.plan("SELECT id, group_key FROM t ORDER BY group_key DESC NULLS FIRST LIMIT 2 OFFSET 1")
		.expect("plan sql");

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected sort");
	};
	assert_eq!(limit, Some(2));
	assert_eq!(offset, 1);
	assert_eq!(order_by.len(), 1);
	assert!(order_by[0].descending);
	assert!(order_by[0].nulls_first);
	assert!(matches!(
		input.as_ref(),
		LogicalPlan::Exchange {
			partition_count: 1,
			key,
			input,
		} if key.is_empty() && matches!(input.as_ref(), LogicalPlan::Project { .. })
	));
}

#[tokio::test]
async fn bind_appends_full_output_tie_break_order() {
	let (catalog, _) = fixture();
	let logical = SqlPlanner::new()
		.plan("SELECT id, group_key FROM t ORDER BY group_key DESC")
		.expect("plan sql");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	let ExecutionPlan::Sort { order_by, .. } = bound else {
		panic!("expected sort");
	};
	assert_eq!(
		order_by,
		vec![
			ExecutionSortKey {
				ordinal: ColumnOrdinal(1),
				descending: true,
				nulls_first: false,
			},
			ExecutionSortKey {
				ordinal: ColumnOrdinal(0),
				descending: false,
				nulls_first: false,
			},
		]
	);
}

#[tokio::test]
async fn bind_grouped_aggregate_sort_ordinal_targets_aggregate_output() {
	let (catalog, _) = fixture();
	let logical = SqlPlanner::new()
		.plan("SELECT group_key, SUM(id) FROM t GROUP BY group_key ORDER BY 2 DESC")
		.expect("plan grouped aggregate sort");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind grouped aggregate sort");

	let ExecutionPlan::Sort { order_by, .. } = bound else {
		panic!("expected sort");
	};
	assert_eq!(order_by[0].ordinal, ColumnOrdinal(1));
	assert!(order_by[0].descending);
}

#[tokio::test]
async fn order_by_limit_offset_executes_in_deterministic_order() {
	let batch =
		run_sql("SELECT id, group_key FROM t ORDER BY group_key ASC, id DESC LIMIT 3 OFFSET 1")
			.await;

	assert_i64_values(&batch, 0, &[3, 1, 5]);
}

#[tokio::test]
async fn sql_derived_top_k_executes() {
	let batch =
		run_sql("SELECT id FROM (SELECT id FROM t ORDER BY id DESC LIMIT 2) AS p WHERE id > 0")
			.await;

	assert_i64_values(&batch, 0, &[9, 5]);
}

#[tokio::test]
async fn sql_derived_offset_executes() {
	let batch =
		run_sql("SELECT id FROM (SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2) AS p").await;

	assert_i64_values(&batch, 0, &[3, 4]);
}

#[tokio::test]
async fn sql_derived_sort_alias_columns_apply_after_sort() {
	let batch =
		run_sql("SELECT renamed FROM (SELECT id FROM t ORDER BY id DESC LIMIT 1) AS p(renamed)")
			.await;

	assert_i64_values(&batch, 0, &[9]);
}

#[test]
fn limit_without_order_by_lowers_to_unordered_take() {
	let plan = SqlPlanner::new()
		.plan("SELECT id FROM t LIMIT 2")
		.expect("plan bare limit");

	let LogicalPlan::Take { input, limit } = plan else {
		panic!("expected unordered take");
	};
	assert_eq!(limit, 2);
	assert!(matches!(input.as_ref(), LogicalPlan::Project { .. }));
}

#[test]
fn offset_without_order_by_still_rejects() {
	let err = SqlPlanner::new()
		.plan("SELECT id FROM t LIMIT 1 OFFSET 1")
		.expect_err("unordered offset should reject");
	assert!(
		err.to_string()
			.contains("LIMIT/OFFSET without ORDER BY is not supported"),
		"{err}"
	);
}

#[tokio::test]
async fn sql_bare_limit_executes_as_unordered_take() {
	let batch = run_sql("SELECT id FROM t LIMIT 2").await;
	assert_i64_values_in(&batch, 0, 2, &[1, 2, 3, 4, 5, 9]);

	let derived = run_sql("SELECT id FROM (SELECT id FROM t LIMIT 3) AS p LIMIT 2").await;
	assert_i64_values_in(&derived, 0, 2, &[1, 2, 3, 4, 5, 9]);

	let empty = run_sql("SELECT id FROM t LIMIT 0").await;
	assert_eq!(empty.num_rows(), 0);
}

#[tokio::test]
async fn bind_bare_limit_pushes_scan_row_limit_into_fully_pushed_scan() {
	let (catalog, _) = fixture();
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM t WHERE id > 2 LIMIT 2")
		.expect("plan filtered bare limit");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind filtered bare limit");

	let ExecutionPlan::Take { input, limit, .. } = bound else {
		panic!("expected take");
	};
	assert_eq!(limit, 2);
	let ExecutionPlan::Scan {
		row_limit,
		pbc_program,
		..
	} = scan_under(input.as_ref())
	else {
		panic!("expected scan under take");
	};
	assert_eq!(*row_limit, Some(2));
	assert!(pbc_program.is_some());
}

#[tokio::test]
async fn bind_bare_limit_does_not_push_scan_row_limit_below_residual_filter() {
	let (catalog, _) = fixture();
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM t WHERE label LIKE 'a%' LIMIT 2")
		.expect("plan residual filtered bare limit");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind residual filtered bare limit");

	let ExecutionPlan::Take { input, limit, .. } = bound else {
		panic!("expected take");
	};
	assert_eq!(limit, 2);
	let ExecutionPlan::Filter {
		input, disposition, ..
	} = input.as_ref()
	else {
		panic!("expected residual filter under take");
	};
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	let ExecutionPlan::Scan { row_limit, .. } = input.as_ref() else {
		panic!("expected scan under residual filter");
	};
	assert_eq!(*row_limit, None);
}

#[test]
fn three_global_sort_barriers_allocate_unique_stage_and_exchange_ids() {
	let bound = ordered_top_k_plan(
		ordered_top_k_plan(
			ordered_top_k_plan(scan_plan(TableId(1), 1), true, Some(5)),
			false,
			Some(3),
		),
		true,
		Some(2),
	);

	let stages = fragment_plan(bound, fragment_config()).expect("fragment recursive barriers");

	assert_eq!(stage_ids(&stages), vec![0, 1, 2, 3]);
	assert!(stages[0].upstream_stages.is_empty());
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[2].upstream_stages, vec![StageId::new(1)]);
	assert_eq!(stages[3].upstream_stages, vec![StageId::new(2)]);
	assert_eq!(final_result_task_count(&stages), 1);
	assert!(matches!(
		stages[3].tasks[0].spec.output,
		TaskOutputBinding::FinalResult
	));

	let mut exchange_ids = stages
		.iter()
		.flat_map(|stage| &stage.tasks)
		.filter_map(|task| match &task.spec.output {
			TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count,
				partition_key,
				publish_mode,
				..
			} => {
				assert_eq!(*partition_count, PartitionCount::new(1));
				assert!(partition_key.is_empty_roots());
				assert_eq!(*publish_mode, ShufflePublishMode::Sealed);
				Some(exchange_id.get())
			}
			TaskOutputBinding::FinalResult => None,
		})
		.collect::<Vec<_>>();
	exchange_ids.sort_unstable();
	assert_eq!(exchange_ids, vec![0, 1, 2]);

	for stage_index in 1..stages.len() {
		assert_single_shuffle_input(
			&stages[stage_index].tasks[0].spec,
			stages[stage_index - 1].stage_id,
		);
	}
}

#[tokio::test]
async fn order_by_digest_preserves_row_order() {
	let left = run_sql("SELECT id FROM t ORDER BY id DESC").await;
	let right = run_sql("SELECT id FROM t ORDER BY id DESC").await;

	assert_i64_values(&left, 0, &[9, 5, 4, 3, 2, 1]);
	assert_eq!(
		canonical_arrow_ipc_digest(&[left], SortPolicy::None).expect("left digest"),
		canonical_arrow_ipc_digest(&[right], SortPolicy::None).expect("right digest")
	);
}

#[tokio::test]
async fn order_by_non_output_column_is_rejected_at_bind() {
	let (catalog, _) = fixture();
	let logical = SqlPlanner::new()
		.plan("SELECT id FROM t ORDER BY group_key")
		.expect("plan sql");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("bind should reject non-output order key");

	assert!(matches!(err, BindingError::UnknownColumn { .. }));
}

#[tokio::test]
async fn order_by_invalid_ordinal_is_rejected_at_bind() {
	let (catalog, _) = fixture();
	let logical = SqlPlanner::new()
		.plan("SELECT id FROM t ORDER BY 0")
		.expect("plan sql");
	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("bind should reject zero ordinal");

	assert!(matches!(err, BindingError::UnsupportedExpression { .. }));
}

#[tokio::test]
async fn order_by_computed_key_is_rejected_at_bind() {
	let (catalog, _) = fixture();
	let logical = LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(LogicalPlan::ScanNamed {
				table: toron::catalog::TableRef::new("t"),
				projection: None,
			}),
			output_names: vec![None],
			exprs: vec![Expr::column("id")],
		}),
		order_by: vec![SortExpr {
			expr: Expr::Eq(
				Box::new(Expr::column("id")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
			descending: false,
			nulls_first: false,
		}],
		limit: None,
		offset: 0,
	};

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("bind should reject computed sort key");
	assert!(matches!(err, BindingError::UnsupportedExpression { .. }));
}

#[tokio::test]
async fn sort_float_keys_follow_arrow_total_order() {
	let schema = Arc::new(Schema::new(vec![
		Field::new("value", DataType::Float64, true),
		Field::new("row", DataType::Int64, false),
	]));
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			Arc::new(Float64Array::from(vec![
				Some(1.0),
				None,
				Some(f64::NAN),
				Some(-0.0),
				Some(0.0),
				Some(-1.0),
			])),
			Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
		],
	)
	.expect("batch");
	let plan = scan_sort_plan(schema, vec![batch], vec![sort_key(0)], None, 0);

	let results = Executor::new(Arc::new(SingleBatchScan::new(plan.batch.clone())))
		.execute_bound(&plan.plan, None)
		.await
		.expect("execute sort");
	let values = results[0]
		.column(0)
		.as_any()
		.downcast_ref::<Float64Array>()
		.expect("float column");

	assert_eq!(values.value(0), -1.0);
	assert_eq!(values.value(1).to_bits(), (-0.0f64).to_bits());
	assert_eq!(values.value(2).to_bits(), 0.0f64.to_bits());
	assert_eq!(values.value(3), 1.0);
	assert!(values.value(4).is_nan());
	assert!(values.is_null(5));
}

#[tokio::test]
async fn sort_over_budget_fails_cleanly() {
	let schema = sort_schema();
	let batch = sort_batch(schema.clone());
	let plan = scan_sort_plan(schema, vec![batch], vec![sort_key(1)], None, 0);
	let budget = TaskBudget {
		memory: ByteCount::new(1),
		..TaskBudget::from_query_budget(QueryBudget::test_default())
	};
	let context = ExecutionContext::local_test().with_task_budget(budget);
	let err = Executor::with_context(Arc::new(SingleBatchScan::new(plan.batch)), context)
		.execute_bound(&plan.plan, None)
		.await
		.expect_err("sort should exceed budget");

	assert!(matches!(
		err,
		ExecutionError::BudgetExhausted {
			dimension: BudgetDimension::Memory,
			..
		}
	));
}

#[tokio::test]
async fn sort_materialization_allows_raw_overage_within_family_cap() {
	let schema = sort_budget_schema();
	let batch = sort_budget_batch(schema.clone(), 24, 64);
	let raw_cap = 1_024u64;
	let family_cap = raw_cap * 16;
	let requested = sort_materialization_estimate(&batch);
	assert!(
		requested > raw_cap,
		"fixture must exceed the raw per-task cap"
	);
	assert!(
		requested <= family_cap,
		"fixture must fit the aligned sort family cap"
	);

	let plan = scan_sort_plan(schema, vec![batch], vec![sort_key(0)], None, 0);
	let budget = TaskBudget {
		memory: ByteCount::new(raw_cap),
		..TaskBudget::from_query_budget(QueryBudget::test_default())
	};
	let context = ExecutionContext::local_test().with_task_budget(budget);
	let results = Executor::with_context(Arc::new(SingleBatchScan::new(plan.batch)), context)
		.execute_bound(&plan.plan, None)
		.await
		.expect("sort within family cap");

	let keys = results[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("sorted key column");
	let expected = (0..24).map(i64::from).collect::<Vec<_>>();
	assert_eq!(keys.values(), expected.as_slice());
}

#[tokio::test]
async fn sort_materialization_still_fails_over_family_cap() {
	let schema = sort_budget_schema();
	let batch = sort_budget_batch(schema.clone(), 128, 256);
	let raw_cap = 1_024u64;
	let family_cap = raw_cap * 16;
	let requested = sort_materialization_estimate(&batch);
	assert!(
		requested > family_cap,
		"fixture must exceed the aligned sort family cap"
	);

	let plan = scan_sort_plan(schema, vec![batch], vec![sort_key(0)], None, 0);
	let budget = TaskBudget {
		memory: ByteCount::new(raw_cap),
		..TaskBudget::from_query_budget(QueryBudget::test_default())
	};
	let context = ExecutionContext::local_test().with_task_budget(budget);
	let err = Executor::with_context(Arc::new(SingleBatchScan::new(plan.batch)), context)
		.execute_bound(&plan.plan, None)
		.await
		.expect_err("sort over family cap must still fail");

	match err {
		ExecutionError::BudgetExhausted {
			dimension,
			producer,
			requested: observed_requested,
			available,
			..
		} => {
			assert_eq!(dimension, BudgetDimension::Memory);
			assert_eq!(producer, "sort materialization");
			assert_eq!(observed_requested, ByteCount::new(requested));
			assert_eq!(available, ByteCount::new(family_cap));
		}
		other => panic!("expected sort materialization budget error, got {other:?}"),
	}
}

#[test]
fn hash_join_ordered_plan_fragments_to_terminal_gather_sort() {
	let stages = fragment_plan(
		terminal_sort_plan(join_plan(
			exchanged_scan(TableId(1), 2),
			exchanged_scan(TableId(2), 2),
			JoinStrategy::HashPartitioned,
		)),
		fragment_config(),
	)
	.expect("fragment");

	assert_eq!(stages.len(), 4);
	assert_eq!(stages[3].upstream_stages, vec![StageId::new(2)]);
	assert_eq!(stages[3].tasks.len(), 1);
	assert!(matches!(
		&stages[2].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
			publish_mode,
			runtime_filter_probe_target: None,
		} if *exchange_id == ExchangeId::new(0)
			&& *partition_count == PartitionCount::new(1)
			&& partition_key.is_empty_roots()
			&& *publish_mode == ShufflePublishMode::Sealed
	));
	assert_terminal_sort_task(
		&stages[3].tasks[0].spec,
		ExchangeId::new(0),
		StageId::new(2),
	);
}

#[test]
fn broadcast_join_ordered_plan_fragments_to_terminal_gather_sort() {
	let stages = fragment_plan(
		terminal_sort_plan(join_plan(
			exchanged_scan(TableId(1), 1),
			scan_plan(TableId(2), 2),
			JoinStrategy::Broadcast,
		)),
		fragment_config().with_worker_count(3),
	)
	.expect("fragment");

	assert_eq!(stages.len(), 3);
	assert_eq!(stages[2].upstream_stages, vec![StageId::new(1)]);
	assert_eq!(stages[1].tasks.len(), 3);
	assert!(matches!(
		&stages[1].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
			publish_mode,
			runtime_filter_probe_target: None,
		} if *exchange_id == ExchangeId::new(0)
			&& *partition_count == PartitionCount::new(1)
			&& partition_key.is_empty_roots()
			&& *publish_mode == ShufflePublishMode::Sealed
	));
	assert_terminal_sort_task(
		&stages[2].tasks[0].spec,
		ExchangeId::new(0),
		StageId::new(1),
	);
}

#[test]
fn barrier_inside_hash_join_input_rejects_with_pin() {
	let left = ExecutionPlan::Exchange {
		input: Box::new(ordered_top_k_plan(scan_plan(TableId(1), 1), true, Some(1))),
		partition_count: 2,
		key: resolved_key_program("key", ColumnOrdinal(0)),
	};
	let plan = join_plan(
		left,
		exchanged_scan(TableId(2), 2),
		JoinStrategy::HashPartitioned,
	);

	assert_eq!(
		fragment_plan(plan, fragment_config()).expect_err("sort barrier inside join input"),
		FragmentError::SortBarrierInsideJoinInput
	);
}

async fn run_sql(sql: &str) -> RecordBatch {
	let (catalog, storage) = fixture();
	let logical = SqlPlanner::new().plan(sql).expect("plan sql");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute");
	assert_eq!(batches.len(), 1);
	batches.into_iter().next().expect("batch")
}

fn assert_i64_values(batch: &RecordBatch, column: usize, expected: &[i64]) {
	let values = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("i64 column");
	let actual = (0..values.len())
		.map(|row| values.value(row))
		.collect::<Vec<_>>();
	assert_eq!(actual, expected);
}

fn assert_i64_values_in(batch: &RecordBatch, column: usize, expected_len: usize, allowed: &[i64]) {
	let values = batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("i64 column");
	assert_eq!(values.len(), expected_len);
	for row in 0..values.len() {
		let value = values.value(row);
		assert!(
			allowed.contains(&value),
			"unexpected unordered take row {value}"
		);
	}
}

fn scan_under(plan: &ExecutionPlan) -> &ExecutionPlan {
	match plan {
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => scan_under(input),
		ExecutionPlan::Join { .. } => panic!("join has multiple scans"),
		other => other,
	}
}

fn fixture() -> (MockCatalog, Arc<FixtureScan>) {
	let schema = sort_schema();
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor(1, "t", schema.as_ref()))
		.expect("register t");
	let storage = Arc::new(FixtureScan {
		batches: HashMap::from([(TableId(1), sort_batch(schema))]),
	});
	(catalog, storage)
}

fn sort_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("group_key", DataType::Int64, true),
		Field::new("label", DataType::Utf8, true),
	]))
}

fn sort_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![5, 3, 1, 9, 2, 4])),
			Arc::new(Int64Array::from(vec![
				Some(2),
				Some(1),
				Some(1),
				None,
				Some(2),
				Some(1),
			])),
			Arc::new(StringArray::from(vec![
				Some("e"),
				Some("c"),
				Some("a"),
				Some("z"),
				Some("b"),
				Some("d"),
			])),
		],
	)
	.expect("sort batch")
}

fn sort_budget_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("k", DataType::Int64, false),
		Field::new("payload", DataType::Utf8, false),
	]))
}

fn sort_budget_batch(schema: SchemaRef, rows: usize, payload_len: usize) -> RecordBatch {
	let keys = (0..rows).rev().map(|row| row as i64).collect::<Vec<_>>();
	let payload = "x".repeat(payload_len);
	let payloads = (0..rows).map(|_| payload.as_str()).collect::<Vec<_>>();
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(keys)),
			Arc::new(StringArray::from(payloads)),
		],
	)
	.expect("sort budget batch")
}

fn sort_materialization_estimate(batch: &RecordBatch) -> u64 {
	let estimate = batch
		.get_array_memory_size()
		.saturating_add(batch.num_rows() * std::mem::size_of::<u32>());
	u64::try_from(estimate).expect("sort estimate fits u64")
}

struct FixtureScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for FixtureScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch =
			self.batches
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;
		if let Some(columns) = request.projected_columns {
			let indices = columns
				.into_iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![batch],
		))
	}
}

struct SingleBatchPlan {
	plan: ExecutionPlan,
	batch: RecordBatch,
}

fn scan_sort_plan(
	schema: SchemaRef,
	batches: Vec<RecordBatch>,
	order_by: Vec<ExecutionSortKey>,
	limit: Option<usize>,
	offset: usize,
) -> SingleBatchPlan {
	let batch = arrow::compute::concat_batches(&schema, &batches).expect("concat");
	SingleBatchPlan {
		plan: ExecutionPlan::Sort {
			input: Box::new(scan_with_schema(TableId(1), schema.clone(), 1)),
			order_by,
			limit,
			offset,
			output_schema: schema,
		},
		batch,
	}
}

struct SingleBatchScan {
	batch: RecordBatch,
}

impl SingleBatchScan {
	fn new(batch: RecordBatch) -> Self {
		Self { batch }
	}
}

#[async_trait::async_trait]
impl StorageScan for SingleBatchScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		Ok(ScanStream::from_batches(
			self.batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![self.batch.clone()],
		))
	}
}

fn descriptor(id: u8, name: &str, schema: &Schema) -> TableSchemaDescriptor {
	let primary_key = schema
		.fields()
		.iter()
		.find(|field| !field.is_nullable())
		.or_else(|| schema.fields().first())
		.expect("descriptor schema has a field")
		.name()
		.clone();
	TableSchemaDescriptor::new(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn fragment_config() -> FragmentPlanConfig {
	FragmentPlanConfig::new_for_test(
		QueryId::from_u128(1),
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
}

fn terminal_sort_plan(input: ExecutionPlan) -> ExecutionPlan {
	let projected = project_join_output(input);
	let schema = projected.schema();
	ExecutionPlan::Sort {
		input: Box::new(ExecutionPlan::Exchange {
			input: Box::new(projected),
			partition_count: 1,
			key: BoundExprProgram::empty(),
		}),
		order_by: vec![sort_key(0), sort_key(1)],
		limit: None,
		offset: 0,
		output_schema: schema,
	}
}

fn ordered_top_k_plan(
	input: ExecutionPlan,
	descending: bool,
	limit: Option<usize>,
) -> ExecutionPlan {
	let schema = input.schema();
	ExecutionPlan::Sort {
		input: Box::new(ExecutionPlan::Exchange {
			input: Box::new(input),
			partition_count: 1,
			key: BoundExprProgram::empty(),
		}),
		order_by: vec![ExecutionSortKey {
			ordinal: ColumnOrdinal(0),
			descending,
			nulls_first: false,
		}],
		limit,
		offset: 0,
		output_schema: schema,
	}
}

fn project_join_output(input: ExecutionPlan) -> ExecutionPlan {
	let output_schema = join_output_schema();
	ExecutionPlan::Project {
		input: Box::new(input),
		exprs: resolved_project_exprs(),
		output_schema,
	}
}

fn join_plan(left: ExecutionPlan, right: ExecutionPlan, strategy: JoinStrategy) -> ExecutionPlan {
	ExecutionPlan::Join {
		replay_id: JoinReplayId::SYNTHETIC,
		replay_eligibility: BroadcastReplayEligibility::Terminal,
		left: Box::new(left),
		right: Box::new(right),
		kind: JoinKind::Inner,
		left_keys: vec![ColumnOrdinal(0)],
		right_keys: vec![ColumnOrdinal(0)],
		match_filter: None,
		strategy,
		null_safe_keys: false,
		output_schema: join_output_schema(),
	}
}

fn exchanged_scan(table_id: TableId, partition_count: u32) -> ExecutionPlan {
	ExecutionPlan::Exchange {
		input: Box::new(scan_plan(table_id, table_id.0 as u8)),
		partition_count,
		key: resolved_key_program("key", ColumnOrdinal(0)),
	}
}

fn scan_plan(table_id: TableId, object_byte: u8) -> ExecutionPlan {
	scan_with_schema(table_id, key_schema(), object_byte)
}

fn scan_with_schema(table_id: TableId, schema: SchemaRef, object_byte: u8) -> ExecutionPlan {
	let descriptor = descriptor(object_byte, "fragment_test", schema.as_ref());
	ExecutionPlan::Scan {
		local_table_id: table_id,
		table: TableObjectId::new([object_byte; 16]),
		fingerprint: descriptor.physical_descriptor_fingerprint,
		schema,
		projection: None,
		pbc_program: None,
		row_identity: toron::storage::RowIdentityProjection::None,
		row_limit: None,
		profile_hint: QueryProfileHint::Scan,
		estimated_byte_size: None,
		primary_key_columns: vec![ColumnOrdinal(0)],
		column_ndv: Vec::new(),
		column_range: Vec::new(),
		column_range_distributions: Vec::new(),
		column_heavy_hitters: Vec::new(),
		column_group_top_k: Vec::new(),
		column_group_ndv: Vec::new(),
	}
}

fn key_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]))
}

fn join_output_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("left_key", DataType::Int32, false),
		Field::new("right_key", DataType::Int32, false),
	]))
}

fn sort_key(ordinal: u32) -> ExecutionSortKey {
	ExecutionSortKey {
		ordinal: ColumnOrdinal(ordinal),
		descending: false,
		nulls_first: false,
	}
}

fn resolved_project_exprs() -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let left = append_resolved_key_expr(&mut builder, "left_key", ColumnOrdinal(0));
	let right = append_resolved_key_expr(&mut builder, "right_key", ColumnOrdinal(1));
	builder.push_root(left);
	builder.push_root(right);
	builder.build()
}

fn resolved_key_program(name: &str, ordinal: ColumnOrdinal) -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let root = append_resolved_key_expr(&mut builder, name, ordinal);
	builder.push_root(root);
	builder.build()
}

fn append_resolved_key_expr(
	builder: &mut BoundExprProgramBuilder,
	name: &str,
	ordinal: ColumnOrdinal,
) -> toron::ir::BoundExprId {
	builder.append_column(ResolvedColumn {
		name: ColumnName(name.to_string()),
		ordinal,
		data_type: DataType::Int32,
		nullable: false,
		pbc_eligible: true,
	})
}

fn stage_ids(stages: &[toron::distributed::scheduler::StagePlan]) -> Vec<u32> {
	stages.iter().map(|stage| stage.stage_id.get()).collect()
}

fn final_result_task_count(stages: &[toron::distributed::scheduler::StagePlan]) -> usize {
	stages
		.iter()
		.flat_map(|stage| &stage.tasks)
		.filter(|task| matches!(task.spec.output, TaskOutputBinding::FinalResult))
		.count()
}

fn assert_single_shuffle_input(
	spec: &toron::distributed::task::TaskSpec,
	producer_stage_id: StageId,
) {
	assert_eq!(spec.inputs.len(), 1);
	assert!(matches!(
		&spec.inputs[0],
		TaskInputBinding::ShuffleConsumer {
			producer_stage_id: observed_stage,
			partition_id,
			..
		} if *observed_stage == producer_stage_id && *partition_id == toron::distributed::identity::PartitionId::new(0)
	));
}

fn assert_terminal_sort_task(
	spec: &toron::distributed::task::TaskSpec,
	exchange_id: ExchangeId,
	producer_stage_id: StageId,
) {
	assert!(matches!(spec.output, TaskOutputBinding::FinalResult));
	assert!(matches!(
		&spec.inputs[0],
		TaskInputBinding::ShuffleConsumer {
			exchange_id: observed_exchange,
			producer_stage_id: observed_stage,
			..
		} if *observed_exchange == exchange_id && *observed_stage == producer_stage_id
	));
	assert!(matches!(
		&spec.plan,
		ExecutionPlan::Sort {
			input,
			order_by,
			..
		} if order_by.len() == 2
			&& matches!(
				input.as_ref(),
				ExecutionPlan::Scan { local_table_id, .. }
					if *local_table_id == toron::distributed::task::shuffle_input_table_id(exchange_id)
			)
	));
}
