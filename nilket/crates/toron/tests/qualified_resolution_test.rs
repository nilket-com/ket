use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::error::{BindingError, ExecutionError, FrontendError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	Expr, JoinStrategy, JoinStrategyIntent, JoinStrategyPinSource, LogicalPlan, TableId,
};
use toron::rune_engine::{RuneEngine, RuneSandboxConfig};
use toron::sql_planner::SqlPlanner;
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[tokio::test]
async fn qualified_alias_projection_disambiguates_join_outputs() {
	bind_sql("SELECT l.id, r.id FROM left_t AS l JOIN right_t AS r ON l.id = r.id")
		.await
		.expect("qualified aliases should disambiguate duplicate output names");
}

#[tokio::test]
async fn qualified_where_resolves_right_join_input() {
	bind_sql("SELECT l.id FROM left_t AS l JOIN right_t AS r ON l.id = r.id WHERE r.flag = 1")
		.await
		.expect("qualified WHERE should resolve the right join input");
}

#[tokio::test]
async fn qualified_order_by_resolves_projected_source_column() {
	bind_sql("SELECT l.id FROM left_t AS l JOIN right_t AS r ON l.id = r.id ORDER BY l.id")
		.await
		.expect("qualified ORDER BY should resolve a projected source column");
}

#[tokio::test]
async fn qualified_group_by_single_source_resolves() {
	bind_sql("SELECT s.region, COUNT(*) FROM sales AS s GROUP BY s.region")
		.await
		.expect("qualified GROUP BY key should resolve");
}

#[tokio::test]
async fn qualified_having_group_key_resolves() {
	bind_sql(
		"SELECT s.region, COUNT(*) FROM sales AS s GROUP BY s.region HAVING s.region = 'east'",
	)
	.await
	.expect("qualified HAVING group key should resolve");
}

#[tokio::test]
async fn qualified_aggregate_argument_resolves() {
	bind_sql("SELECT s.region, SUM(s.amount) FROM sales AS s GROUP BY s.region")
		.await
		.expect("qualified aggregate argument should resolve");
}

#[tokio::test]
async fn alias_hides_base_table_qualifier() {
	let err = bind_sql("SELECT sales.id FROM sales AS s")
		.await
		.expect_err("alias must hide the base table qualifier");

	assert!(matches!(
		err,
		BindingError::UnknownTableRef { qualifier } if qualifier == "sales"
	));
}

#[tokio::test]
async fn alias_column_list_renames_qualified_column_scope() {
	bind_sql("SELECT p.pid FROM sales AS p(pid, r, amt, st) WHERE p.amt > 10")
		.await
		.expect("alias column list should rename qualified scope names");
}

#[tokio::test]
async fn duplicate_relation_alias_rejects_as_ambiguous() {
	let err = bind(
		&duplicate_alias_join_plan(),
		&catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("duplicate visible aliases must reject as ambiguous");

	assert!(
		matches!(
			err,
			BindingError::AmbiguousColumnRef { .. } | BindingError::AmbiguousTableRef { .. }
		),
		"expected ambiguous qualifier/column error, got {err}"
	);
}

#[tokio::test]
async fn unknown_qualified_column_reports_relation_context() {
	let err = bind_sql("SELECT s.missing FROM sales AS s")
		.await
		.expect_err("unknown qualified column should reject");

	assert!(matches!(
		err,
		BindingError::UnknownColumnInRelation {
			qualifier,
			column,
			available
		} if qualifier == "s" && column == "missing" && available.iter().any(|name| name == "amount")
	));
}

#[tokio::test]
async fn rune_alias_and_qcol_filter_execute() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .alias("s")
               .filter(qcol("s", "amount").add(lit(0)).gt(lit(10)))
               .project([qcol("s", "id")])
        }
    "#,
	)
	.await
	.expect("Rune alias/qcol plan");

	let rows = execute_logical(&logical)
		.await
		.expect("execute Rune qcol filter");
	assert_eq!(int64_column_rows(&rows, 0), vec![Some(2), Some(4)]);
}

#[tokio::test]
async fn rune_qcol_project_named_over_joined_sql_executes() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT sales.id, regions.active FROM sales JOIN regions ON sales.region = regions.region")
               .project_named([qcol("sales", "id").named("sid")])
        }
    "#,
	)
	.await
	.expect("Rune qcol over SQL join plan");

	let rows = execute_logical(&logical)
		.await
		.expect("execute Rune qcol over SQL join");
	assert_eq!(rows[0].schema().field(0).name(), "sid");
	let mut ids = int64_column_rows(&rows, 0);
	ids.sort();
	assert_eq!(ids, vec![Some(1), Some(2), Some(4)]);
}

#[tokio::test]
async fn rune_qcol_constructs_qualified_column_ref() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales").project([qcol("sales", "id")])
        }
    "#,
	)
	.await
	.expect("Rune qcol project");
	let LogicalPlan::Project { exprs, .. } = logical else {
		panic!("expected project plan");
	};
	let Expr::Column(column) = &exprs[0] else {
		panic!("qcol should construct a column expression");
	};
	assert_eq!(
		column
			.qualifier
			.as_ref()
			.map(|qualifier| qualifier.table.as_str()),
		Some("sales")
	);
	assert_eq!(column.name.0, "id");
}

#[test]
fn qualified_wildcard_stays_rejected() {
	assert_frontend_unsupported("SELECT s.* FROM sales AS s", "qualified wildcard");
}

#[test]
fn qualified_returning_stays_rejected() {
	assert_frontend_unsupported(
		"INSERT INTO sales (id, region, amount, status) VALUES (7, 'east', 1, 'open') RETURNING sales.id",
		"qualified RETURNING columns are not supported",
	);
}

async fn bind_sql(sql: &str) -> Result<toron::ir::ExecutionPlan, BindingError> {
	let plan = SqlPlanner::new().plan(sql).expect("plan sql");
	bind(&plan, &catalog(), &PlannerConfig::default()).await
}

async fn rune_plan(script: &str) -> Result<LogicalPlan, FrontendError> {
	let config = RuneSandboxConfig {
		wall_timeout: Duration::from_secs(2),
		..RuneSandboxConfig::v1_default()
	};
	let engine = RuneEngine::with_config(config).expect("engine");
	engine.execute_script(script, &config).await
}

async fn execute_logical(logical: &LogicalPlan) -> Result<Vec<RecordBatch>, ExecutionError> {
	let bound = bind(logical, &catalog(), &PlannerConfig::default()).await?;
	Executor::new(Arc::new(FixtureScan {
		batches: HashMap::from([
			(TableId(1), sales_batch(sales_schema())),
			(TableId(2), regions_batch(regions_schema())),
			(TableId(3), left_batch(left_schema())),
			(TableId(4), right_batch(right_schema())),
		]),
	}))
	.execute_bound(&bound, None)
	.await
}

fn assert_frontend_unsupported(sql: &str, expected: &str) {
	let err = SqlPlanner::new()
		.plan(sql)
		.expect_err("query should stay unsupported");
	assert!(
		matches!(&err, FrontendError::Unsupported { message } if message.contains(expected)),
		"expected unsupported message containing {expected:?}, got {err}"
	);
}

fn duplicate_alias_join_plan() -> LogicalPlan {
	let left = LogicalPlan::Alias {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("left_t"),
			projection: None,
		}),
		alias: TableRef::new("x"),
		columns: Vec::new(),
	};
	let right = LogicalPlan::Alias {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("right_t"),
			projection: None,
		}),
		alias: TableRef::new("x"),
		columns: Vec::new(),
	};
	LogicalPlan::Join {
		left: Box::new(left),
		right: Box::new(right),
		kind: toron::ir::JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("x"), "id"),
			Expr::qualified_column(TableRef::new("x"), "id"),
		)],
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Direct,
		},
	}
}

fn catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	for (id, name, schema, primary_key) in [
		(1, "sales", sales_schema(), "id"),
		(2, "regions", regions_schema(), "region"),
		(3, "left_t", left_schema(), "id"),
		(4, "right_t", right_schema(), "id"),
	] {
		catalog
			.register_descriptor(
				TableId(id),
				descriptor(id as u8, name, schema.as_ref(), primary_key),
			)
			.expect("register descriptor");
	}
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

fn regions_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("region", DataType::Utf8, false),
		Field::new("active", DataType::Int32, false),
	]))
}

fn left_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("l_value", DataType::Int32, true),
	]))
}

fn right_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("flag", DataType::Int32, true),
	]))
}

fn sales_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
			Arc::new(StringArray::from(vec![
				Some("east"),
				Some("west"),
				None,
				Some("east"),
			])),
			Arc::new(Int32Array::from(vec![Some(10), Some(20), None, Some(30)])),
			Arc::new(StringArray::from(vec![
				Some("open"),
				Some("open"),
				Some("closed"),
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

fn left_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2])),
			Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
		],
	)
	.expect("left batch")
}

fn right_batch(schema: SchemaRef) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2])),
			Arc::new(Int32Array::from(vec![Some(0), Some(1)])),
		],
	)
	.expect("right batch")
}

fn int64_column_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
	batches
		.iter()
		.flat_map(|batch| {
			let array = batch
				.column(column)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("int64 column");
			(0..array.len()).map(|row| array.is_valid(row).then(|| array.value(row)))
		})
		.collect()
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
			if projected_columns.is_empty() {
				let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
				batch = RecordBatch::try_new_with_options(
					Arc::new(Schema::empty()),
					Vec::new(),
					&options,
				)?;
			} else {
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
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}
