use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{ExchangeId, QueryId, StageId};
use toron::distributed::task::{PartitionCount, TaskInputBinding, TaskOutputBinding};
use toron::error::{BindingError, FrontendError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	AggregateFunction, ColumnOrdinal, ExecutionPlan, Expr, LogicalPlan, ScalarValue, SortExpr,
	TableId,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[test]
fn group_by_lowers_to_aggregate_over_group_keyed_exchange() {
	let plan = plan_sql("SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region");

	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = plan
	else {
		panic!("expected aggregate plan");
	};
	assert_eq!(group_expr.len(), 1);
	assert_column(&group_expr[0], "region");
	assert_eq!(aggr_expr.len(), 2);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::Sum));
	assert_column(&aggr_expr[0].1[0], "amount");
	assert!(matches!(aggr_expr[1].0, AggregateFunction::CountAll));

	let LogicalPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected exchange below aggregate");
	};
	assert_eq!(*partition_count, 4);
	assert_eq!(key.len(), 1);
	assert_column(&key[0], "region");
	assert!(matches!(input.as_ref(), LogicalPlan::ScanNamed { .. }));
}

#[test]
fn group_by_requires_at_least_one_aggregate_call() {
	assert_unsupported("SELECT region FROM sales GROUP BY region");
}

#[test]
fn group_by_projection_must_be_exact_full_key_list_then_aggregates() {
	let plan = plan_sql("SELECT region, status, COUNT(*) FROM sales GROUP BY region, status");

	let LogicalPlan::Aggregate {
		group_expr,
		aggr_expr,
		..
	} = plan
	else {
		panic!("expected aggregate plan");
	};
	assert_eq!(group_expr.len(), 2);
	assert_column(&group_expr[0], "region");
	assert_column(&group_expr[1], "status");
	assert_eq!(aggr_expr.len(), 1);
	assert!(matches!(aggr_expr[0].0, AggregateFunction::CountAll));
}

#[test]
fn group_by_projection_subset_reorder_alias_or_aggregate_first_rejects() {
	for sql in [
		"SELECT region, COUNT(*) FROM sales GROUP BY region, status",
		"SELECT status, region, COUNT(*) FROM sales GROUP BY region, status",
		"SELECT COUNT(*), region FROM sales GROUP BY region",
		"SELECT region AS r, COUNT(*) FROM sales GROUP BY region",
		"SELECT region, COUNT(*) AS n FROM sales GROUP BY region",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn group_by_accepts_only_stable_aggregate_set() {
	for sql in [
		"SELECT region, COUNT(*) FROM sales GROUP BY region",
		"SELECT region, COUNT(amount) FROM sales GROUP BY region",
		"SELECT region, SUM(amount) FROM sales GROUP BY region",
	] {
		assert!(plan_sql(sql).is_aggregate(), "{sql}");
	}
	for sql in [
		"SELECT region, LINEAR_REGRESS(amount, id) FROM sales GROUP BY region",
		"SELECT region, APPROX_QUANTILE(amount, 0.5) FROM sales GROUP BY region",
		"SELECT region, AVG(amount) FROM sales GROUP BY region",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn group_by_keys_must_be_simple_unqualified_input_columns() {
	for sql in [
		"SELECT region, COUNT(*) FROM sales GROUP BY sales.region",
		"SELECT region, COUNT(*) FROM sales GROUP BY 1",
		"SELECT region, COUNT(*) FROM sales GROUP BY region + 1",
		"SELECT sales.region, COUNT(*) FROM sales GROUP BY region",
	] {
		assert_unsupported(sql);
	}
}

#[test]
fn group_by_over_explicit_join_rejects() {
	assert_unsupported(
		"SELECT region, COUNT(*) FROM sales \
		 INNER JOIN regions ON sales.region = regions.region \
		 GROUP BY region",
	);
}

#[test]
fn group_by_over_decorrelated_exists_rejects() {
	assert_unsupported(
		"SELECT region, COUNT(*) FROM sales \
		 WHERE EXISTS (SELECT 1 FROM regions WHERE regions.region = sales.region) \
		 GROUP BY region",
	);
}

#[test]
fn group_by_with_order_by_lowers_to_terminal_sort_over_aggregate() {
	let plan = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY region");

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected sort over grouped aggregate");
	};
	assert_eq!(limit, None);
	assert_eq!(offset, 0);
	assert_eq!(order_by.len(), 1);
	assert_column(&order_by[0].expr, "region");
	assert!(!order_by[0].descending);

	let LogicalPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected terminal exchange below sort");
	};
	assert_eq!(*partition_count, 1);
	assert!(key.is_empty());
	assert_grouped_aggregate_over_exchange(input.as_ref());
}

#[test]
fn group_by_with_order_by_ordinal_limit_offset_lowers() {
	let plan = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 ORDER BY 2 DESC LIMIT 10 OFFSET 1",
	);

	let LogicalPlan::Sort {
		input,
		order_by,
		limit,
		offset,
	} = plan
	else {
		panic!("expected sort over grouped aggregate");
	};
	assert_eq!(limit, Some(10));
	assert_eq!(offset, 1);
	assert_eq!(order_by.len(), 1);
	assert!(order_by[0].descending);
	assert!(matches!(
		order_by[0].expr,
		Expr::Literal(ScalarValue::Int32(Some(2)))
	));
	assert!(matches!(
		input.as_ref(),
		LogicalPlan::Exchange {
			partition_count: 1,
			key,
			input,
		} if key.is_empty() && matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
	));
}

#[test]
fn group_by_order_by_aggregate_call_rejects() {
	assert_unsupported(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount)",
	);
}

#[test]
fn having_is_unsupported_not_dropped() {
	assert_unsupported("SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1");
}

#[test]
fn distinct_is_unsupported_not_dropped() {
	assert_unsupported("SELECT DISTINCT region, COUNT(*) FROM sales GROUP BY region");
}

#[tokio::test]
async fn grouped_aggregate_sql_matches_hand_built_aggregate_plan() {
	let sql =
		plan_sql("SELECT region, COUNT(*), COUNT(amount), SUM(amount) FROM sales GROUP BY region");
	let hand_built = hand_built_grouped_plan();
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::FullRowLex).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::FullRowLex)
			.expect("hand digest")
	);
	assert_eq!(
		grouped_rows(&sql_results),
		HashMap::from([
			(Some("east".to_string()), (3, 2, Some(40))),
			(Some("west".to_string()), (2, 1, Some(20))),
			(None, (1, 1, Some(7))),
		])
	);
}

#[tokio::test]
async fn grouped_aggregate_order_by_binds_against_aggregate_output() {
	let (catalog, _) = fixture();
	for (sql, expected_ordinal, expected_descending) in [
		(
			"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region",
			ColumnOrdinal(0),
			false,
		),
		(
			"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY sum DESC",
			ColumnOrdinal(1),
			true,
		),
		(
			"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 2 DESC",
			ColumnOrdinal(1),
			true,
		),
	] {
		let logical = plan_sql(sql);
		let bound = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind grouped sort");
		let ExecutionPlan::Sort { order_by, .. } = bound else {
			panic!("expected sort for {sql}");
		};
		assert_eq!(order_by[0].ordinal, expected_ordinal, "{sql}");
		assert_eq!(order_by[0].descending, expected_descending, "{sql}");
	}
}

#[tokio::test]
async fn grouped_aggregate_order_by_duplicate_generated_name_is_ambiguous() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT region, COUNT(*), COUNT(amount) FROM sales GROUP BY region ORDER BY count DESC",
	);

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("duplicate generated aggregate name should be ambiguous");
	assert!(matches!(
		err,
		BindingError::AmbiguousColumn { column } if column == "count"
	));
}

#[tokio::test]
async fn grouped_aggregate_order_by_executes_after_aggregation() {
	let top_two = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 2 DESC LIMIT 2",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&top_two),
		vec![
			(Some("east".to_string()), Some(40)),
			(Some("west".to_string()), Some(20)),
		]
	);

	let offset = run_logical(&plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 2 DESC LIMIT 1 OFFSET 1",
	))
	.await;
	assert_eq!(
		ordered_region_sum_rows(&offset),
		vec![(Some("west".to_string()), Some(20))]
	);
}

#[tokio::test]
async fn ordered_grouped_aggregate_sql_matches_hand_built_sort_plan() {
	let sql = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY 2 DESC LIMIT 10 OFFSET 1",
	);
	let hand_built = hand_built_ordered_grouped_sum_plan(Some(10), 1);
	let sql_results = run_logical(&sql).await;
	let hand_built_results = run_logical(&hand_built).await;

	assert_eq!(
		canonical_arrow_ipc_digest(&sql_results, SortPolicy::None).expect("sql digest"),
		canonical_arrow_ipc_digest(&hand_built_results, SortPolicy::None).expect("hand digest")
	);
}

#[tokio::test]
async fn grouped_aggregate_bound_plan_fragments_to_single_group_keyed_exchange() {
	let (catalog, _) = fixture();
	let logical = plan_sql("SELECT region, COUNT(*) FROM sales GROUP BY region");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment");

	assert_eq!(stages.len(), 2);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert_eq!(stages[1].stage_id, StageId::new(1));
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[0].tasks.len(), 1);
	assert_eq!(stages[1].tasks.len(), 4);
	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			partition_count,
			partition_key,
			..
		} if *partition_count == PartitionCount::new(4) && partition_key.len() == 1
	));
	for task in &stages[1].tasks {
		assert!(matches!(task.spec.output, TaskOutputBinding::FinalResult));
		assert!(matches!(
			task.spec.inputs[0],
			TaskInputBinding::ShuffleConsumer { .. }
		));
		assert!(matches!(task.spec.plan, ExecutionPlan::Aggregate { .. }));
	}
}

#[tokio::test]
async fn grouped_aggregate_order_by_fragments_to_terminal_sort_stage() {
	let (catalog, _) = fixture();
	let logical = plan_sql(
		"SELECT region, SUM(amount) FROM sales GROUP BY region \
		 ORDER BY 2 DESC LIMIT 10 OFFSET 1",
	);
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	let stages = fragment_plan(bound, fragment_config()).expect("fragment ordered aggregate");

	assert_eq!(stages.len(), 3);
	assert_eq!(stages[0].stage_id, StageId::new(0));
	assert_eq!(stages[1].stage_id, StageId::new(1));
	assert_eq!(stages[2].stage_id, StageId::new(2));
	assert!(stages[0].upstream_stages.is_empty());
	assert_eq!(stages[1].upstream_stages, vec![StageId::new(0)]);
	assert_eq!(stages[2].upstream_stages, vec![StageId::new(1)]);

	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count,
			partition_key,
		} if *exchange_id == ExchangeId::new(0)
			&& *partition_count == PartitionCount::new(4)
			&& partition_key.len() == 1
	));
	assert_eq!(stages[1].tasks.len(), 4);
	for (partition, task) in stages[1].tasks.iter().enumerate() {
		assert!(matches!(task.spec.plan, ExecutionPlan::Aggregate { .. }));
		assert!(matches!(
			&task.spec.inputs[0],
			TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id,
				partition_id,
				..
			} if *exchange_id == ExchangeId::new(0)
				&& *producer_stage_id == StageId::new(0)
				&& partition_id.0 == u32::try_from(partition).unwrap()
		));
		assert!(matches!(
			&task.spec.output,
			TaskOutputBinding::ShuffleProducer {
				exchange_id,
				partition_count,
				partition_key,
			} if *exchange_id == ExchangeId::new(1)
				&& *partition_count == PartitionCount::new(1)
				&& partition_key.is_empty()
		));
	}

	assert_eq!(stages[2].tasks.len(), 1);
	let terminal = &stages[2].tasks[0].spec;
	assert!(matches!(terminal.output, TaskOutputBinding::FinalResult));
	assert!(matches!(
		&terminal.inputs[0],
		TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_stage_id,
			partition_id,
			..
		} if *exchange_id == ExchangeId::new(1)
			&& *producer_stage_id == StageId::new(1)
			&& partition_id.0 == 0
	));
	assert!(matches!(
		&terminal.plan,
		ExecutionPlan::Sort {
			order_by,
			limit: Some(10),
			offset: 1,
			..
		} if order_by[0].ordinal == ColumnOrdinal(1) && order_by[0].descending
	));
}

#[tokio::test]
async fn non_aggregate_two_exchange_terminal_sort_still_rejects() {
	let (catalog, _) = fixture();
	let bound = bind(
		&project_between_exchanges_sort_plan(),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind non-aggregate two-exchange sort");

	assert_eq!(
		fragment_plan(bound, fragment_config()).expect_err("two exchanges should reject"),
		FragmentError::MultipleExchangeBoundaries { count: 2 }
	);
}

async fn run_logical(logical: &LogicalPlan) -> Vec<RecordBatch> {
	let (catalog, storage) = fixture();
	let bound = bind(logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("execute")
}

fn hand_built_grouped_plan() -> LogicalPlan {
	LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("sales"),
				projection: None,
			}),
			partition_count: 4,
			key: vec![Expr::column("region")],
		}),
		group_expr: vec![Expr::column("region")],
		aggr_expr: vec![
			(AggregateFunction::CountAll, vec![Expr::column("count")]),
			(AggregateFunction::Count, vec![Expr::column("amount")]),
			(AggregateFunction::Sum, vec![Expr::column("amount")]),
		],
	}
}

fn hand_built_ordered_grouped_sum_plan(limit: Option<usize>, offset: usize) -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::Aggregate {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(LogicalPlan::ScanNamed {
						table: TableRef::new("sales"),
						projection: None,
					}),
					partition_count: 4,
					key: vec![Expr::column("region")],
				}),
				group_expr: vec![Expr::column("region")],
				aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
			}),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::Literal(ScalarValue::Int32(Some(2))),
			descending: true,
			nulls_first: false,
		}],
		limit,
		offset,
	}
}

fn project_between_exchanges_sort_plan() -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::Project {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(LogicalPlan::ScanNamed {
						table: TableRef::new("sales"),
						projection: None,
					}),
					partition_count: 4,
					key: vec![Expr::column("region")],
				}),
				exprs: vec![Expr::column("region")],
			}),
			partition_count: 1,
			key: Vec::new(),
		}),
		order_by: vec![SortExpr {
			expr: Expr::column("region"),
			descending: false,
			nulls_first: false,
		}],
		limit: None,
		offset: 0,
	}
}

fn grouped_rows(results: &[RecordBatch]) -> HashMap<Option<String>, (i64, i64, Option<i64>)> {
	let mut rows = HashMap::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let count_all = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count all column");
		let count_amount = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count amount column");
		let sums = batch
			.column(3)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let sum = if sums.is_null(row) {
				None
			} else {
				Some(sums.value(row))
			};
			rows.insert(region, (count_all.value(row), count_amount.value(row), sum));
		}
	}
	rows
}

fn ordered_region_sum_rows(results: &[RecordBatch]) -> Vec<(Option<String>, Option<i64>)> {
	let mut rows = Vec::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let sums = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let sum = if sums.is_null(row) {
				None
			} else {
				Some(sums.value(row))
			};
			rows.push((region, sum));
		}
	}
	rows
}

fn assert_grouped_aggregate_over_exchange(plan: &LogicalPlan) {
	let LogicalPlan::Aggregate {
		input,
		group_expr,
		aggr_expr,
	} = plan
	else {
		panic!("expected aggregate below terminal exchange");
	};
	assert_eq!(group_expr.len(), 1);
	assert_column(&group_expr[0], "region");
	assert_eq!(aggr_expr.len(), 1);
	let LogicalPlan::Exchange {
		input,
		partition_count,
		key,
	} = input.as_ref()
	else {
		panic!("expected group-keyed exchange below aggregate");
	};
	assert_eq!(*partition_count, 4);
	assert_eq!(key.len(), 1);
	assert_column(&key[0], "region");
	assert!(matches!(input.as_ref(), LogicalPlan::ScanNamed { .. }));
}

fn plan_sql(sql: &str) -> LogicalPlan {
	SqlPlanner::new().plan(sql).expect("plan sql")
}

fn assert_unsupported(sql: &str) {
	let err = SqlPlanner::new()
		.plan(sql)
		.expect_err("query should be unsupported");
	assert!(
		matches!(err, FrontendError::Unsupported { .. }),
		"{sql}: expected unsupported, got {err}"
	);
}

fn assert_column(expr: &Expr, expected: &str) {
	match expr {
		Expr::Column(column) => assert_eq!(column.name.0, expected),
		other => panic!("expected column {expected}, got {other:?}"),
	}
}

fn fragment_config() -> FragmentPlanConfig {
	FragmentPlanConfig::new(
		QueryId::new(),
		TaskBudget::from_query_budget(QueryBudget::test_default()),
	)
}

trait LogicalPlanExt {
	fn is_aggregate(&self) -> bool;
}

impl LogicalPlanExt for LogicalPlan {
	fn is_aggregate(&self) -> bool {
		matches!(self, LogicalPlan::Aggregate { .. })
	}
}

fn fixture() -> (MockCatalog, Arc<FixtureScan>) {
	let sales_schema = sales_schema();
	let regions_schema = regions_schema();
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(1, "sales", sales_schema.as_ref(), "id"),
		)
		.expect("register sales");
	catalog
		.register_descriptor(
			TableId(2),
			descriptor(2, "regions", regions_schema.as_ref(), "region"),
		)
		.expect("register regions");
	let storage = Arc::new(FixtureScan {
		batches: HashMap::from([
			(TableId(1), sales_batch(sales_schema)),
			(TableId(2), regions_batch(regions_schema)),
		]),
	});
	(catalog, storage)
}

fn sales_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::Utf8, true),
		Field::new("amount", DataType::Int32, true),
		Field::new("status", DataType::Utf8, true),
	]))
}

fn regions_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("region", DataType::Utf8, false),
		Field::new("active", DataType::Int32, false),
	]))
}

fn sales_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
			Arc::new(StringArray::from(vec![
				Some("east"),
				Some("west"),
				Some("east"),
				Some("east"),
				Some("west"),
				None,
			])),
			Arc::new(Int32Array::from(vec![
				Some(10),
				Some(20),
				None,
				Some(30),
				None,
				Some(7),
			])),
			Arc::new(StringArray::from(vec![
				Some("open"),
				Some("open"),
				Some("closed"),
				Some("open"),
				Some("open"),
				Some("open"),
			])),
		],
	)
	.expect("sales batch")
}

fn regions_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(vec!["east", "west"])),
			Arc::new(Int32Array::from(vec![1, 1])),
		],
	)
	.expect("regions batch")
}

fn descriptor(
	id_byte: u8,
	table: &str,
	schema: &Schema,
	primary_key: &str,
) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"default",
		table,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

struct FixtureScan {
	batches: HashMap<TableId, RecordBatch>,
}

#[async_trait::async_trait]
impl StorageScan for FixtureScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch = self
			.batches
			.get(&request.table_id)
			.expect("fixture table")
			.clone();
		if let Some(projected_columns) = request.projected_columns.as_ref() {
			let schema = batch.schema();
			let fields = projected_columns
				.iter()
				.map(|ordinal| schema.field(ordinal.0 as usize).as_ref().clone())
				.collect::<Vec<_>>();
			let columns = projected_columns
				.iter()
				.map(|ordinal| batch.column(ordinal.0 as usize).clone())
				.collect::<Vec<_>>();
			batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}
