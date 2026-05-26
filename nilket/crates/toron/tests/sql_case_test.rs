use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::{BindingError, ExecutionError};
use toron::exec::Executor;
use toron::ir::{
	BoundExprNode, ExecutionPlan, Expr, FilterDisposition, LogicalPlan, LogicalTypeId, TableId,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::InMemoryStorageScan;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[tokio::test]
async fn case_searched_projection_executes() {
	let batches =
		run_sql("SELECT CASE WHEN amount > 20 THEN 'big' ELSE 'small' END FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("small".to_string()),
			Some("small".to_string()),
			Some("small".to_string()),
			Some("big".to_string()),
			Some("big".to_string()),
			Some("small".to_string()),
		]
	);
}

#[tokio::test]
async fn case_simple_desugars_and_executes() {
	let plan = plan_sql(
		"SELECT CASE status WHEN 'open' THEN 'O' WHEN 'closed' THEN 'C' ELSE 'X' END FROM sales",
	);
	let LogicalPlan::Project { exprs, .. } = &plan else {
		panic!("expected project");
	};
	let Expr::Case { when_then, .. } = &exprs[0] else {
		panic!("expected CASE expression, got {:?}", exprs[0]);
	};
	assert!(matches!(when_then[0].0.as_ref(), Expr::Eq(_, _)));

	let batches = run_logical(&plan).await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("O".to_string()),
			Some("O".to_string()),
			Some("C".to_string()),
			Some("O".to_string()),
			Some("X".to_string()),
			Some("X".to_string()),
		]
	);
}

#[tokio::test]
async fn case_first_true_wins() {
	let batches = run_sql(
		"SELECT CASE WHEN id > 0 THEN 'first' WHEN id = 1 THEN 'second' ELSE 'else' END FROM sales",
	)
	.await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("first".to_string()); 6]
	);
}

#[tokio::test]
async fn case_no_match_uses_else() {
	let batches = run_sql("SELECT CASE WHEN id < 0 THEN 'hit' ELSE 'miss' END FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("miss".to_string()); 6]
	);
}

#[tokio::test]
async fn case_missing_else_returns_null() {
	let batches = run_sql("SELECT CASE WHEN id = 1 THEN amount END FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), None, None, None, None, None]
	);
}

#[tokio::test]
async fn case_nullable_condition_is_no_match() {
	let batches = run_sql("SELECT CASE WHEN flag THEN 'yes' ELSE 'no' END FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("yes".to_string()),
			Some("no".to_string()),
			Some("no".to_string()),
			Some("yes".to_string()),
			Some("no".to_string()),
			Some("no".to_string()),
		]
	);
}

#[tokio::test]
async fn case_numeric_branches_widen() {
	let batches = run_sql("SELECT CASE WHEN id = 1 THEN amount ELSE 1.5 END FROM sales").await;
	assert_eq!(
		float64_column_rows(&batches, 0),
		vec![
			Some(10.0),
			Some(1.5),
			Some(1.5),
			Some(1.5),
			Some(1.5),
			Some(1.5)
		]
	);
}

#[tokio::test]
async fn case_identical_text_branches_execute() {
	let batches =
		run_sql("SELECT CASE WHEN id = 1 THEN status ELSE 'fallback' END FROM sales").await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![
			Some("open".to_string()),
			Some("fallback".to_string()),
			Some("fallback".to_string()),
			Some("fallback".to_string()),
			Some("fallback".to_string()),
			Some("fallback".to_string()),
		]
	);
}

#[tokio::test]
async fn case_type_mismatch_rejects_at_bind() {
	let err = bind_sql_result("SELECT CASE WHEN id = 1 THEN amount ELSE status END FROM sales")
		.await
		.expect_err("CASE type mismatch should reject");
	match err {
		BindingError::TypeMismatch { message } => {
			assert!(message.contains("CASE result branches cannot be unified"));
		}
		other => panic!("unexpected error: {other:?}"),
	}
}

#[tokio::test]
async fn case_in_where_executes_as_residual() {
	let sql = "SELECT id FROM sales WHERE CASE WHEN amount > 20 THEN true ELSE false END";
	let bound = bind_sql(sql).await;
	let (predicate, disposition) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
	assert!(matches!(
		predicate
			.single_root()
			.and_then(|root| predicate.node(root)),
		Some(BoundExprNode::Case { .. })
	));

	let batches = execute_bound(&bound).await.expect("execute CASE WHERE");
	assert_eq!(ids(&batches), vec![4, 5]);
}

#[tokio::test]
async fn case_in_order_by_executes() {
	let batches = run_sql(
		"SELECT SUM(amount) FROM sales ORDER BY CASE WHEN SUM(amount) > 0 THEN 0 ELSE 1 END",
	)
	.await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(87)]);
}

#[tokio::test]
async fn case_in_aggregate_argument_executes() {
	let batches =
		run_sql("SELECT SUM(CASE WHEN status = 'open' THEN amount ELSE 0 END) FROM sales").await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(40)]);
}

#[test]
fn case_nested_aggregate_in_aggregate_argument_rejects() {
	let err = SqlPlanner::new()
		.plan("SELECT SUM(CASE WHEN amount > 0 THEN COUNT(*) ELSE 0 END) FROM sales")
		.expect_err("nested aggregate inside CASE should reject");
	assert!(err
		.to_string()
		.contains("aggregate arguments must not contain aggregate calls"));
}

#[tokio::test]
async fn case_in_having_executes() {
	let batches = run_sql(
		"SELECT SUM(amount) FROM sales HAVING CASE WHEN SUM(amount) > 0 THEN true ELSE false END",
	)
	.await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(87)]);
}

#[tokio::test]
async fn case_result_is_naked() {
	let bound = bind_sql("SELECT CASE WHEN id = 1 THEN amount ELSE 0 END FROM sales").await;
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	assert_eq!(exprs.root_logical_type(0), Some(LogicalTypeId::NAKED));
}

#[tokio::test]
async fn case_divide_guard_is_safe_via_null() {
	let batches =
		run_sql("SELECT CASE WHEN amount <> 0 THEN 100 / amount ELSE 0 END FROM sales").await;
	assert_eq!(
		int32_column_rows(&batches, 0),
		vec![Some(10), Some(0), Some(0), Some(3), Some(2), Some(14)]
	);
}

#[tokio::test]
async fn case_branches_eval_eagerly_masked_hard_error_fails() {
	let err = run_sql_result("SELECT CASE WHEN id < 0 THEN 2147483647 + 1 ELSE 0 END FROM sales")
		.await
		.expect_err("masked overflow branch should still fail");
	match err {
		ExecutionError::Kernel { message } => {
			assert!(message.starts_with("arithmetic overflow: Add(Int32)"));
		}
		other => panic!("unexpected error: {other:?}"),
	}
}

fn plan_sql(sql: &str) -> LogicalPlan {
	SqlPlanner::new().plan(sql).expect("plan SQL")
}

async fn bind_sql(sql: &str) -> ExecutionPlan {
	bind_sql_result(sql).await.expect("bind SQL")
}

async fn bind_sql_result(sql: &str) -> Result<ExecutionPlan, BindingError> {
	let catalog = catalog();
	bind(&plan_sql(sql), &catalog, &PlannerConfig::default()).await
}

async fn run_sql(sql: &str) -> Vec<RecordBatch> {
	run_sql_result(sql).await.expect("execute SQL")
}

async fn run_sql_result(sql: &str) -> Result<Vec<RecordBatch>, ExecutionError> {
	let bound = bind_sql(sql).await;
	execute_bound(&bound).await
}

async fn run_logical(plan: &LogicalPlan) -> Vec<RecordBatch> {
	let catalog = catalog();
	let bound = bind(plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind logical plan");
	execute_bound(&bound).await.expect("execute logical plan")
}

async fn execute_bound(bound: &ExecutionPlan) -> Result<Vec<RecordBatch>, ExecutionError> {
	Executor::new(Arc::new(InMemoryStorageScan {
		table_data: sales_batch(schema()),
	}))
	.execute_bound(bound, None)
	.await
}

fn find_filter(plan: &ExecutionPlan) -> (&toron::ir::BoundExprProgram, &FilterDisposition) {
	match plan {
		ExecutionPlan::Project { input, .. } | ExecutionPlan::Sort { input, .. } => {
			find_filter(input)
		}
		ExecutionPlan::Filter {
			predicate,
			disposition,
			..
		} => (predicate, disposition),
		other => panic!("expected filter plan, got {other:?}"),
	}
}

fn ids(batches: &[RecordBatch]) -> Vec<i32> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(0)
				.as_any()
				.downcast_ref::<Int32Array>()
				.expect("id column")
				.iter()
				.map(|value| value.expect("id value"))
				.collect::<Vec<_>>()
		})
		.collect()
}

fn int32_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<i32>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<Int32Array>()
				.expect("Int32 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn int64_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<i64>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("Int64 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn float64_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<f64>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<Float64Array>()
				.expect("Float64 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn string_column_rows(batches: &[RecordBatch], column_idx: usize) -> Vec<Option<String>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column_idx)
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("Utf8 column")
				.iter()
				.map(|value| value.map(str::to_string))
				.collect::<Vec<_>>()
		})
		.collect()
}

fn catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	let schema = schema();
	catalog
		.register_descriptor(TableId(1), descriptor(schema.as_ref()))
		.expect("register sales");
	catalog
}

fn schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int32, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("status", DataType::Utf8, true),
		Field::new("flag", DataType::Boolean, true),
	]))
}

fn sales_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
			Arc::new(Int32Array::from(vec![
				Some(10),
				Some(0),
				None,
				Some(30),
				Some(40),
				Some(7),
			])),
			Arc::new(StringArray::from(vec![
				Some("open"),
				Some("open"),
				Some("closed"),
				Some("open"),
				None,
				Some("pending"),
			])),
			Arc::new(arrow::array::BooleanArray::from(vec![
				Some(true),
				Some(false),
				None,
				Some(true),
				None,
				Some(false),
			])),
		],
	)
	.expect("sales batch")
}

fn descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([108; 16]),
		1,
		"default",
		"sales",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}
