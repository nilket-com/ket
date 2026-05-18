use std::sync::Arc;

use arrow::array::{Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{Catalog, MockCatalog, TableBinding, TableRef, WendelonCatalog};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::distributed::worker_registry::StaticWorkerRegistry;
use toron::error::{CatalogError, ExecutionError, QueryError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{ExecutionPlan, LogicalPlan, SessionId, TableId};
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{HlcBound, InMemoryStorageScan, InMemoryTableStorage, TenantId};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptor,
};

#[test]
fn planner_accepts_drop_table_surface() {
	let plan = SqlPlanner::new()
		.plan("DROP TABLE sales")
		.expect("plan drop table");
	let LogicalPlan::DropTable { name, if_exists } = plan else {
		panic!("expected drop table plan");
	};
	assert_eq!(name, TableRef::new("sales"));
	assert!(!if_exists);

	let plan = SqlPlanner::new()
		.plan("DROP TABLE IF EXISTS app.sales")
		.expect("plan drop table if exists");
	let LogicalPlan::DropTable { name, if_exists } = plan else {
		panic!("expected drop table plan");
	};
	assert_eq!(name, TableRef::with_schema("app", "sales"));
	assert!(if_exists);
}

#[test]
fn planner_rejects_unsupported_drop_table_shapes() {
	for (sql, diagnostic) in [
		("DROP VIEW v", "only TABLE is supported"),
		("DROP TABLE a, b", "exactly one target table"),
		("DROP TABLE sales CASCADE", "CASCADE"),
		("DROP TABLE sales RESTRICT", "RESTRICT"),
		("DROP TABLE sales PURGE", "PURGE"),
		("DROP TEMPORARY TABLE sales", "TEMPORARY"),
		("DROP TABLE a.b.c", "only one- or two-part"),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[tokio::test]
async fn binder_emits_identity_free_drop_table() {
	let catalog = MockCatalog::new();
	let plan = SqlPlanner::new()
		.plan("DROP TABLE IF EXISTS app.sales")
		.expect("plan drop table");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind drop table");
	let ExecutionPlan::DropTable { name, if_exists } = bound else {
		panic!("expected drop table execution plan");
	};
	assert_eq!(name, TableRef::with_schema("app", "sales"));
	assert!(if_exists);
}

#[tokio::test]
async fn mock_catalog_drop_table_semantics() {
	let catalog = MockCatalog::new();
	let name = TableRef::new("sales");
	let created = catalog
		.create_table(
			&name,
			schema(&[Field::new("id", DataType::Int32, false)]),
			false,
		)
		.await
		.expect("create table");

	let dropped = catalog
		.drop_table(&name, false)
		.await
		.expect("drop table")
		.expect("existing table returns binding");
	assert_eq!(dropped.legacy_table_id, created.legacy_table_id);
	assert!(catalog.resolve_table(&name).await.is_err());

	let missing = expect_drop_catalog_err(catalog.drop_table(&name, false).await);
	assert!(matches!(
		missing,
		CatalogError::TableNotFound { ref table } if table == "sales"
	));

	let absent = catalog
		.drop_table(&name, true)
		.await
		.expect("drop missing if exists");
	assert!(absent.is_none());
}

#[tokio::test]
async fn mock_catalog_drop_table_schema_alias_cleanup_is_exact() {
	let catalog = MockCatalog::new();
	let app_name = TableRef::with_schema("app", "sales");
	let other_name = TableRef::with_schema("other", "sales");
	let schema = schema(&[Field::new("id", DataType::Int32, false)]);

	let app = catalog
		.create_table(&app_name, schema.clone(), false)
		.await
		.expect("create app.sales");
	let other = catalog
		.create_table(&other_name, schema, false)
		.await
		.expect("create other.sales");
	let unqualified = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("resolve unqualified alias");
	assert_eq!(unqualified.legacy_table_id, app.legacy_table_id);

	catalog
		.drop_table(&app_name, false)
		.await
		.expect("drop app.sales");

	assert!(catalog.resolve_table(&app_name).await.is_err());
	assert!(
		catalog
			.resolve_table(&TableRef::new("sales"))
			.await
			.is_err()
	);
	let still_other = catalog
		.resolve_table(&other_name)
		.await
		.expect("other.sales remains");
	assert_eq!(still_other.legacy_table_id, other.legacy_table_id);
}

#[tokio::test]
async fn query_coordinator_executes_drop_table() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (1)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let before_drop = concat_query(&coordinator, &planner, "SELECT id FROM sales").await;
	assert_eq!(int32_column(&before_drop, 0).values(), &[1]);

	let dropped = coordinator
		.execute_query(
			&planner.plan("DROP TABLE sales").expect("plan drop table"),
			None,
		)
		.await
		.expect("execute drop table");
	assert!(dropped.is_empty());

	let err = coordinator
		.execute_query(
			&planner.plan("SELECT id FROM sales").expect("plan select"),
			None,
		)
		.await
		.expect_err("select after drop should fail at bind");
	assert!(
		err.to_string().contains("table not found"),
		"unexpected error: {err}"
	);

	assert!(
		catalog
			.resolve_table(&TableRef::new("sales"))
			.await
			.is_err()
	);
	assert!(
		coordinator
			.execute_query(
				&planner
					.plan("DROP TABLE IF EXISTS missing")
					.expect("plan drop missing"),
				None,
			)
			.await
			.expect("drop missing if exists")
			.is_empty()
	);
}

#[tokio::test]
async fn query_coordinator_recreate_after_drop_isolated() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (1)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	coordinator
		.execute_query(
			&planner.plan("DROP TABLE sales").expect("plan drop table"),
			None,
		)
		.await
		.expect("drop table");
	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"INSERT INTO sales VALUES (2)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan recreate SQL"), None)
			.await
			.expect(sql);
	}

	let after_recreate = concat_query(&coordinator, &planner, "SELECT id FROM sales").await;
	assert_eq!(int32_column(&after_recreate, 0).values(), &[2]);
}

#[tokio::test]
async fn query_coordinator_ctas_cleanup_allows_rerun() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1), (2)",
		"CREATE TABLE copy AS SELECT id FROM src",
		"DROP TABLE copy",
		"CREATE TABLE copy AS SELECT id FROM src",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let copy = concat_query(&coordinator, &planner, "SELECT id FROM copy ORDER BY id").await;
	assert_eq!(int32_column(&copy, 0).values(), &[1, 2]);
}

#[tokio::test]
async fn distributed_coordinator_executes_drop_table_before_fragment() {
	let catalog = Arc::new(MockCatalog::new());
	catalog
		.create_table(
			&TableRef::new("sales"),
			schema(&[Field::new("id", DataType::Int32, false)]),
			false,
		)
		.await
		.expect("create table");
	let coordinator = DistributedQueryCoordinator::new(
		catalog.clone(),
		Arc::new(StaticWorkerRegistry::new([])),
		Arc::new(MemoryManager::new(1, 1024 * 1024)),
		Arc::new(FailingTransport),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let plan = SqlPlanner::new()
		.plan("DROP TABLE sales")
		.expect("plan drop table");

	let results = coordinator
		.execute_query(&plan)
		.await
		.expect("distributed drop table");

	assert!(results.is_empty());
	assert!(
		catalog
			.resolve_table(&TableRef::new("sales"))
			.await
			.is_err()
	);
}

#[tokio::test]
async fn wendelon_catalog_drop_table_semantics() {
	let descriptor = mock_descriptor(
		&TableRef::with_schema("bench", "orders"),
		schema(&[Field::new("id", DataType::Int32, false)]).as_ref(),
		table_object_id(TableId(7)),
	);
	let catalog =
		WendelonCatalog::from_descriptors([(TableId(7), descriptor)]).expect("wendelon catalog");

	let known = expect_drop_catalog_err(
		catalog
			.drop_table(&TableRef::with_schema("bench", "orders"), true)
			.await,
	);
	assert!(matches!(known, CatalogError::AdminUnavailable { .. }));
	assert!(
		known
			.to_string()
			.contains("DROP TABLE is not wired to Wendelon catalog administration")
	);

	let missing = catalog
		.drop_table(&TableRef::new("missing"), true)
		.await
		.expect("missing if exists");
	assert!(missing.is_none());
	let err = expect_drop_catalog_err(catalog.drop_table(&TableRef::new("missing"), false).await);
	assert!(matches!(
		err,
		CatalogError::TableNotFound { ref table } if table == "missing"
	));
}

#[tokio::test]
async fn executor_and_fragmenter_reject_direct_drop_table_execution() {
	let executor = Executor::with_context(
		empty_storage(),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);
	let plan = ExecutionPlan::DropTable {
		name: TableRef::new("sales"),
		if_exists: false,
	};

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("executor should reject DROP TABLE");
	assert!(matches!(
		err,
		ExecutionError::NotImplemented {
			feature: "DROP TABLE execution is coordinator-only"
		}
	));

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject DROP TABLE");
	assert_eq!(err, FragmentError::DropTableNotFragmentable);
}

fn coordinator() -> (
	Arc<MockCatalog>,
	Arc<InMemoryTableStorage>,
	QueryCoordinator,
	SqlPlanner,
) {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator = QueryCoordinator::with_writer(
		catalog.clone(),
		executor,
		PlannerConfig::default(),
		storage.clone(),
	);
	(catalog, storage, coordinator, SqlPlanner::new())
}

async fn concat_query(
	coordinator: &QueryCoordinator,
	planner: &SqlPlanner,
	sql: &str,
) -> RecordBatch {
	let results = coordinator
		.execute_query(&planner.plan(sql).expect("plan query"), None)
		.await
		.expect("execute query");
	let schema = results[0].schema();
	arrow::compute::concat_batches(&schema, results.iter()).expect("concat query results")
}

fn assert_frontend_error(sql: &str, diagnostic: &str) {
	let err = SqlPlanner::new().plan(sql).expect_err("SQL should reject");
	assert!(
		err.to_string().contains(diagnostic),
		"expected diagnostic {diagnostic:?}, got {err}"
	);
}

fn expect_drop_catalog_err(result: Result<Option<TableBinding>, CatalogError>) -> CatalogError {
	match result {
		Ok(_) => panic!("expected catalog error"),
		Err(error) => error,
	}
}

fn schema(fields: &[Field]) -> SchemaRef {
	Arc::new(Schema::new(fields.to_vec()))
}

fn empty_storage() -> Arc<InMemoryStorageScan> {
	let schema = Arc::new(Schema::empty());
	let batch = RecordBatch::new_empty(schema);
	Arc::new(InMemoryStorageScan { table_data: batch })
}

fn mock_descriptor(
	name: &TableRef,
	schema: &Schema,
	table: TableObjectId,
) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		table,
		1,
		name.schema.as_deref().unwrap_or("default"),
		&name.table,
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

fn table_object_id(table_id: TableId) -> TableObjectId {
	let mut bytes = [0_u8; 16];
	bytes[8..].copy_from_slice(&table_id.0.to_be_bytes());
	TableObjectId::new(bytes)
}

fn int32_column(batch: &RecordBatch, index: usize) -> &Int32Array {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("Int32 column")
}

struct FailingTransport;

#[async_trait]
impl ToronTransport for FailingTransport {
	async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		Err(QueryError::Internal {
			message: "transport should not be used for DROP TABLE".to_string(),
		})
	}
}
