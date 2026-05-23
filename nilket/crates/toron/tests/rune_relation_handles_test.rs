use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::error::{BindingError, FrontendError, StorageError};
use toron::exec::Executor;
use toron::ir::{ExecutionPlan, FilterDisposition, LogicalPlan, TableId};
use toron::rune_engine::{RuneEngine, RuneSandboxConfig};
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

fn sandbox_config() -> RuneSandboxConfig {
	RuneSandboxConfig {
		wall_timeout: Duration::from_secs(2),
		..RuneSandboxConfig::v1_default()
	}
}

async fn rune_plan(script: &str) -> Result<LogicalPlan, FrontendError> {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");
	engine.execute_script(script, &config).await
}

#[tokio::test]
async fn rune_ctx_sql_select_returns_lazy_dataframe() {
	let plan = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id, amount FROM sales")
        }
    "#,
	)
	.await
	.expect("ctx.sql SELECT");

	assert!(contains_scan_named(&plan, "sales"));
	assert!(!is_terminal_plan(&plan));
	assert!(matches!(plan, LogicalPlan::Project { .. }));
}

#[tokio::test]
async fn rune_ctx_sql_rejects_multi_statement_input() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id FROM sales; DROP TABLE sales")
        }
    "#,
	)
	.await
	.expect_err("ctx.sql must reject multiple statements");

	assert_frontend_message_contains(error, "exactly one SQL statement");
}

#[tokio::test]
async fn rune_ctx_sql_rejects_create_insert_update_delete_drop() {
	for query in [
		"CREATE TABLE created (id INT)",
		"CREATE TABLE created AS SELECT id FROM sales",
		"INSERT INTO sales (id, region, amount, status) VALUES (7, 'east', 9, 'open')",
		"UPDATE sales SET amount = 99 WHERE id = 1",
		"DELETE FROM sales WHERE id = 1",
		"DROP TABLE sales",
	] {
		let script = format!(
			r#"
        pub fn main(ctx) {{
            ctx.sql({query:?})
        }}
    "#
		);

		let error = rune_plan(&script)
			.await
			.expect_err("ctx.sql must reject terminal statements");
		assert_frontend_message_contains(error, "ctx.sql accepts single query relations only");
	}
}

#[tokio::test]
async fn rune_sql_group_by_alias_can_feed_outer_sum_and_bind() {
	let logical = sql_group_by_alias_outer_sum_plan().await;
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind Rune aggregate over SQL alias");

	assert_eq!(bound.schema().fields().len(), 1);
	assert_eq!(bound.schema().field(0).name(), "sum");
}

#[tokio::test]
async fn rune_sql_group_by_alias_outer_sum_executes_if_existing_harness_supports_it() {
	let batches = run_logical(&sql_group_by_alias_outer_sum_plan().await).await;

	assert_eq!(int64_column_rows(&batches, 0), vec![Some(6)]);
}

#[tokio::test]
async fn rune_sql_filter_then_rune_aggregate_composes_single_logical_plan() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM sales WHERE status = 'open'")
               .filter(col("amount").gt(lit(10)))
               .aggregate([], [sum(col("amount"))])
        }
    "#,
	)
	.await
	.expect("compose SQL filter with Rune aggregate");

	let LogicalPlan::Aggregate { input, .. } = &logical else {
		panic!("expected outer Rune aggregate, got {logical:?}");
	};
	assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));

	let batches = run_logical(&logical).await;
	assert_eq!(int64_column_rows(&batches, 0), vec![Some(50)]);
}

#[tokio::test]
async fn rune_sql_composition_preserves_existing_pushdown_path() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM sales")
               .filter(col("amount").gt(lit(10)))
        }
    "#,
	)
	.await
	.expect("compose SQL scan with Rune filter");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind composed Rune SQL filter");
	let (_, disposition, scan) = find_filter(&bound);

	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	assert!(
		scan_pbc(scan).is_some(),
		"composed filter must reach scan PBC"
	);
}

#[tokio::test]
async fn rune_sql_parse_error_maps_to_frontend_error_without_capability_escape() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM")
        }
    "#,
	)
	.await
	.expect_err("invalid SQL should fail through frontend error path");

	let message = frontend_message(error);
	assert!(message.contains("SQL parse failed"), "{message}");
	assert!(!message.contains("Rune sandbox violation"), "{message}");
	assert!(!message.contains("/home/"), "{message}");
}

#[tokio::test]
async fn rune_project_named_sql_multi_hop_continuation_binds_and_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT region, amount FROM sales WHERE status = 'open'")
               .filter(col("amount").gt(lit(10)))
               .project_named([
                   col("region").named("bucket"),
                   col("amount").named("net"),
               ])
               .aggregate([col("bucket")], [sum(col("net")), count_all()])
               .project_named([
                   col("bucket").named("bucket"),
                   col("sum").named("gross"),
                   col("count").named("n"),
               ])
               .filter(col("gross").gt(lit(20)))
        }
    "#,
	)
	.await
	.expect("multi-hop named continuation");

	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind multi-hop named continuation");
	assert_schema_names(bound.schema().as_ref(), &["bucket", "gross", "n"]);

	let batches = run_logical(&logical).await;
	assert_eq!(
		string_column_rows(&batches, 0),
		vec![Some("east".to_string())]
	);
	assert_eq!(int64_column_rows(&batches, 1), vec![Some(30)]);
	assert_eq!(int64_column_rows(&batches, 2), vec![Some(1)]);
}

#[tokio::test]
async fn rune_project_named_sets_project_output_names() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("region").named("bucket"),
                   col("amount").named("net"),
               ])
        }
    "#,
	)
	.await
	.expect("named project plan");

	let LogicalPlan::Project { output_names, .. } = logical else {
		panic!("expected named Project, got {logical:?}");
	};
	assert_eq!(
		output_names,
		vec![Some("bucket".to_string()), Some("net".to_string())]
	);
}

#[tokio::test]
async fn rune_project_named_binds_visible_schema() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .project_named([
                   col("region").named("bucket"),
                   col("amount").named("net"),
               ])
        }
    "#,
	)
	.await
	.expect("named project plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind named project");

	assert_schema_names(bound.schema().as_ref(), &["bucket", "net"]);
}

#[tokio::test]
async fn rune_aggregate_helper_pack_binds_stable_outputs() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [
                   count_all(),
                   count(col("amount")),
                   sum(col("amount")),
                   avg(col("amount")),
                   min(col("amount")),
                   max(col("amount")),
               ])
        }
    "#,
	)
	.await
	.expect("aggregate helper pack plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind stable aggregate helpers");

	assert_schema_names(
		bound.schema().as_ref(),
		&["count", "count", "sum", "avg", "min", "max"],
	);
}

#[tokio::test]
async fn rune_project_named_post_aggregate_renames_outputs() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("amount"))])
               .project_named([col("sum").named("gross")])
        }
    "#,
	)
	.await
	.expect("post aggregate named project plan");
	let bound = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind post aggregate rename");

	assert_schema_names(bound.schema().as_ref(), &["gross"]);
}

#[tokio::test]
async fn rune_duplicate_aggregate_output_name_is_ambiguous() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("amount")), sum(col("id"))])
               .project_named([col("sum").named("gross")])
        }
    "#,
	)
	.await
	.expect("duplicate aggregate output plan");
	let error = bind(&logical, &catalog(), &PlannerConfig::default())
		.await
		.expect_err("duplicate aggregate output name should be ambiguous");

	assert!(matches!(
		error,
		BindingError::AmbiguousColumn { column } if column == "sum"
	));
}

#[tokio::test]
async fn rune_named_expr_rejects_misused_named_expression() {
	let error = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .aggregate([], [sum(col("amount").named("net"))])
        }
    "#,
	)
	.await
	.expect_err("RuneNamedExpr must not be accepted as RuneExpr");

	let message = frontend_message(error);
	assert!(!message.contains("Rune capability denied"), "{message}");
	assert!(!message.contains("/home/"), "{message}");
}

#[tokio::test]
async fn rune_named_projection_preserves_adr079_sql_relation_guards() {
	let multi_statement = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT id FROM sales; DROP TABLE sales")
        }
    "#,
	)
	.await
	.expect_err("multi statement SQL must remain rejected");
	assert_frontend_message_contains(multi_statement, "exactly one SQL statement");

	let ddl = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("DROP TABLE sales")
        }
    "#,
	)
	.await
	.expect_err("terminal SQL must remain rejected");
	assert_frontend_message_contains(ddl, "ctx.sql accepts single query relations only");

	let alias_bound = bind(
		&sql_group_by_alias_outer_sum_plan().await,
		&catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("ADR079 SQL alias continuation still binds");
	assert_schema_names(alias_bound.schema().as_ref(), &["sum"]);

	let pushdown = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM sales")
               .filter(col("amount").gt(lit(10)))
        }
    "#,
	)
	.await
	.expect("compose SQL scan with Rune filter");
	let bound = bind(&pushdown, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind composed Rune SQL filter");
	let (_, disposition, scan) = find_filter(&bound);
	assert_eq!(*disposition, FilterDisposition::FullyPushed);
	assert!(scan_pbc(scan).is_some());
}

async fn sql_group_by_alias_outer_sum_plan() -> LogicalPlan {
	rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT region, COUNT(1) AS cnt FROM sales GROUP BY region")
               .aggregate([], [sum(col("cnt"))])
        }
    "#,
	)
	.await
	.expect("Rune aggregate over SQL group-by alias")
}

async fn run_logical(logical: &LogicalPlan) -> Vec<RecordBatch> {
	let bound = bind(logical, &catalog(), &PlannerConfig::default())
		.await
		.expect("bind");
	Executor::new(Arc::new(FixtureScan {
		batches: HashMap::from([(TableId(1), sales_batch(sales_schema()))]),
	}))
	.execute_bound(&bound, None)
	.await
	.expect("execute")
}

fn catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(1, "sales", sales_schema().as_ref(), "id"),
		)
		.expect("register sales");
	catalog
}

fn sales_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::Utf8, true),
		Field::new("amount", DataType::Int32, true),
		Field::new("status", DataType::Utf8, true),
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
			batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
				.expect("project fixture batch");
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}

fn contains_scan_named(plan: &LogicalPlan, table: &str) -> bool {
	match plan {
		LogicalPlan::ScanNamed { table: name, .. } => name == &TableRef::new(table),
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::CreateTableAsSelect { source: input, .. }
		| LogicalPlan::InsertSelect { source: input, .. } => contains_scan_named(input, table),
		LogicalPlan::Join { left, right, .. } => {
			contains_scan_named(left, table) || contains_scan_named(right, table)
		}
		_ => false,
	}
}

fn is_terminal_plan(plan: &LogicalPlan) -> bool {
	matches!(
		plan,
		LogicalPlan::CreateTable { .. }
			| LogicalPlan::CreateTableAsSelect { .. }
			| LogicalPlan::DropTable { .. }
			| LogicalPlan::Delete { .. }
			| LogicalPlan::Update { .. }
			| LogicalPlan::Insert { .. }
			| LogicalPlan::InsertSelect { .. }
	)
}

fn find_filter(
	plan: &ExecutionPlan,
) -> (
	&toron::ir::BoundExprProgram,
	&FilterDisposition,
	&ExecutionPlan,
) {
	match plan {
		ExecutionPlan::Project { input, .. } | ExecutionPlan::Sort { input, .. } => {
			find_filter(input)
		}
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => (predicate, disposition, input),
		other => panic!("expected filter plan, got {other:?}"),
	}
}

fn scan_pbc(plan: &ExecutionPlan) -> Option<&[u8]> {
	match plan {
		ExecutionPlan::Scan { pbc_program, .. } => pbc_program.as_ref().map(|pbc| pbc.0.as_slice()),
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => scan_pbc(input),
		ExecutionPlan::Join { left, right, .. } => scan_pbc(left).or_else(|| scan_pbc(right)),
		_ => None,
	}
}

fn int64_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("Int64 column")
				.iter()
				.collect::<Vec<_>>()
		})
		.collect()
}

fn string_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<String>> {
	batches
		.iter()
		.flat_map(|batch| {
			batch
				.column(column)
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("Utf8 column")
				.iter()
				.map(|value| value.map(str::to_string))
				.collect::<Vec<_>>()
		})
		.collect()
}

fn assert_schema_names(schema: &Schema, expected: &[&str]) {
	let actual = schema
		.fields()
		.iter()
		.map(|field| field.name().as_str())
		.collect::<Vec<_>>();
	assert_eq!(actual, expected);
}

fn frontend_message(error: FrontendError) -> String {
	match error {
		FrontendError::SqlParse { message }
		| FrontendError::RuneCompile { message }
		| FrontendError::RuneRuntime { message }
		| FrontendError::Unsupported { message }
		| FrontendError::RuneSandbox { message, .. } => message,
	}
}

fn assert_frontend_message_contains(error: FrontendError, expected: &str) {
	let message = frontend_message(error);
	assert!(
		message.contains(expected),
		"expected frontend error containing {expected:?}, got {message:?}"
	);
}
