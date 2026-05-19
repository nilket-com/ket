use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{Catalog, MockCatalog, TableBinding, TableRef};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::distributed::worker_registry::StaticWorkerRegistry;
use toron::error::{ExecutionError, QueryError, StorageError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{ExecutionPlan, LogicalPlan, QueryProfileHint, SessionId};
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	HlcBound, InMemoryStorageScan, InMemoryTableStorage, StorageWrite, TenantId, WriteOutcome,
	WriteRequest,
};

#[test]
fn planner_accepts_create_table_as_select_surface() {
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE copy AS SELECT id, name FROM people")
		.expect("plan CTAS");

	let LogicalPlan::CreateTableAsSelect {
		table,
		or_replace,
		source,
	} = plan
	else {
		panic!("expected CTAS logical plan");
	};

	assert_eq!(table, TableRef::new("copy"));
	assert!(!or_replace);
	assert!(matches!(*source, LogicalPlan::Project { .. }));
}

#[test]
fn planner_accepts_create_or_replace_table_as_select_surface() {
	let plan = SqlPlanner::new()
		.plan("CREATE OR REPLACE TABLE copy AS SELECT id, name FROM people")
		.expect("plan CTAS replace");

	let LogicalPlan::CreateTableAsSelect {
		table,
		or_replace,
		source,
	} = plan
	else {
		panic!("expected CTAS logical plan");
	};

	assert_eq!(table, TableRef::new("copy"));
	assert!(or_replace);
	assert!(matches!(*source, LogicalPlan::Project { .. }));
}

#[test]
fn planner_rejects_unsupported_ctas_shapes() {
	for (sql, diagnostic) in [
		(
			"CREATE TABLE copy (id INT) AS SELECT id FROM people",
			"explicit column lists",
		),
		(
			"CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people",
			"IF NOT EXISTS",
		),
		("CREATE TABLE copy LIKE people", "LIKE"),
		("CREATE TABLE copy CLONE people", "CLONE"),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[test]
fn ctas_if_not_exists_remains_rejected() {
	assert_frontend_error(
		"CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people",
		"CTAS does not support IF NOT EXISTS in v1",
	);
}

#[test]
fn ctas_or_replace_rejects_if_not_exists_combo() {
	assert_frontend_error(
		"CREATE OR REPLACE TABLE IF NOT EXISTS copy AS SELECT id FROM people",
		"CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS",
	);
}

#[tokio::test]
async fn binder_infers_ctas_schema_without_target_identity() {
	let catalog = people_catalog().await;
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE copy AS SELECT id, name FROM people")
		.expect("plan CTAS");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind CTAS");
	let ExecutionPlan::CreateTableAsSelect {
		table,
		schema,
		or_replace,
		source,
	} = bound
	else {
		panic!("expected CTAS execution plan");
	};

	assert_eq!(table, TableRef::new("copy"));
	assert!(!or_replace);
	assert_eq!(schema.fields().len(), 2);
	assert_field(&schema, 0, "id", &DataType::Int32, false);
	assert_field(&schema, 1, "name", &DataType::Utf8, true);
	assert!(matches!(*source, ExecutionPlan::Project { .. }));
}

#[tokio::test]
async fn create_or_replace_table_as_select_creates_when_absent() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')",
		"CREATE OR REPLACE TABLE copy AS SELECT id, name FROM people",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT id, name FROM copy ORDER BY id",
	)
	.await;
	assert_eq!(int32_column(&batch, 0).values(), &[1, 2]);
	assert_eq!(string_column(&batch, 1).value(0), "Alice");
	assert_eq!(string_column(&batch, 1).value(1), "Bob");
}

#[tokio::test]
async fn create_or_replace_table_as_select_replaces_existing_table() {
	let (catalog, storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')",
		"CREATE TABLE copy (old_id INT NOT NULL)",
		"INSERT INTO copy VALUES (99)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let before = catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("resolve old copy");

	coordinator
		.execute_query(
			&planner
				.plan("CREATE OR REPLACE TABLE copy AS SELECT name FROM people")
				.expect("plan replace CTAS"),
			None,
		)
		.await
		.expect("replace CTAS");

	let after = catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("resolve new copy");
	assert_ne!(before.table, after.table);
	assert_eq!(storage.table_count(), 2);
	assert_field(&after.schema, 0, "name", &DataType::Utf8, true);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT name FROM copy ORDER BY name",
	)
	.await;
	assert_eq!(string_column(&batch, 0).value(0), "Alice");
	assert_eq!(string_column(&batch, 0).value(1), "Bob");
}

#[tokio::test]
async fn ctas_or_replace_source_failure_preserves_existing_table() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE target (id INT NOT NULL)",
		"INSERT INTO target VALUES (7)",
		"CREATE TABLE source_without_storage (id INT NOT NULL)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}
	let before = catalog
		.resolve_table(&TableRef::new("target"))
		.await
		.expect("resolve target before replace");

	let err = coordinator
		.execute_query(
			&planner
				.plan("CREATE OR REPLACE TABLE target AS SELECT id FROM source_without_storage")
				.expect("plan replace CTAS"),
			None,
		)
		.await
		.expect_err("source storage failure should reject");
	assert!(
		err.to_string().contains("table not found"),
		"unexpected error: {err}"
	);

	let after = catalog
		.resolve_table(&TableRef::new("target"))
		.await
		.expect("target remains after source failure");
	assert_eq!(before.legacy_table_id, after.legacy_table_id);
	assert_eq!(before.table, after.table);
	assert_eq!(before.fingerprint, after.fingerprint);

	let batch = concat_query(&coordinator, &planner, "SELECT id FROM target").await;
	assert_eq!(int32_column(&batch, 0).values(), &[7]);
}

#[tokio::test]
async fn binder_rejects_invalid_ctas_inferred_schema() {
	for (sql, diagnostic) in [
		(
			"CREATE TABLE dup AS SELECT id, id FROM people",
			"duplicate CTAS output column",
		),
		(
			"CREATE TABLE reserved AS SELECT id AS __row_hlc__ FROM people",
			"reserved for storage metadata",
		),
	] {
		let catalog = people_catalog().await;
		let plan = SqlPlanner::new().plan(sql).expect("plan CTAS");
		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("invalid CTAS schema should reject");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}

	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("dated"),
			schema(&[Field::new("d", DataType::Date32, false)]),
			false,
		)
		.await
		.expect("create Date32 source");
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE unsupported AS SELECT d FROM dated")
		.expect("plan CTAS");
	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unsupported CTAS type should reject");
	assert!(
		err.to_string()
			.contains("unsupported CTAS output column type"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn query_coordinator_executes_projection_ctas() {
	let (catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE people (id INT NOT NULL, name TEXT)",
		"INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')",
		"CREATE TABLE copy AS SELECT id, name FROM people",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let binding = catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("resolve CTAS table");
	assert_field(&binding.schema, 0, "id", &DataType::Int32, false);
	assert_field(&binding.schema, 1, "name", &DataType::Utf8, true);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT id, name FROM copy ORDER BY id",
	)
	.await;
	let ids = int32_column(&batch, 0);
	let names = string_column(&batch, 1);
	assert_eq!(ids.values(), &[1, 2]);
	assert_eq!(names.value(0), "Alice");
	assert_eq!(names.value(1), "Bob");
}

#[tokio::test]
async fn query_coordinator_executes_aggregate_and_join_ctas() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE sales (region TEXT, amount BIGINT NOT NULL)",
		"INSERT INTO sales VALUES ('east', 100), ('east', 200)",
		"CREATE TABLE totals AS SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
		"CREATE TABLE left_table (id INT NOT NULL, label TEXT)",
		"CREATE TABLE right_table (left_id INT NOT NULL, score BIGINT NOT NULL)",
		"INSERT INTO left_table VALUES (1, 'a'), (2, 'b')",
		"INSERT INTO right_table VALUES (1, 100), (2, 200)",
		"CREATE TABLE joined AS SELECT left_table.id AS id, label, score FROM left_table INNER JOIN right_table ON left_table.id = right_table.left_id",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let totals = concat_query(&coordinator, &planner, "SELECT region, total FROM totals").await;
	assert_eq!(string_column(&totals, 0).value(0), "east");
	assert_eq!(int64_column(&totals, 1).value(0), 300);

	let joined = concat_query(
		&coordinator,
		&planner,
		"SELECT id, label, score FROM joined ORDER BY id",
	)
	.await;
	assert_eq!(int32_column(&joined, 0).values(), &[1, 2]);
	assert_eq!(string_column(&joined, 1).value(0), "a");
	assert_eq!(string_column(&joined, 1).value(1), "b");
	assert_eq!(int64_column(&joined, 2).values(), &[100, 200]);
}

#[tokio::test]
async fn query_coordinator_empty_source_ctas_initializes_queryable_table() {
	let (_catalog, _storage, coordinator, planner) = coordinator();

	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1)",
		"CREATE TABLE empty_copy AS SELECT id FROM src WHERE id > 100",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let batch = concat_query(&coordinator, &planner, "SELECT id FROM empty_copy").await;
	assert_eq!(batch.num_rows(), 0);
	assert_eq!(batch.num_columns(), 1);
}

#[tokio::test]
async fn query_coordinator_ctas_failures_before_catalog_mutation_do_not_create_target() {
	for (sql, diagnostic, target) in [
		(
			"CREATE TABLE ghost AS SELECT missing FROM src",
			"unknown column",
			"ghost",
		),
		(
			"CREATE TABLE dup AS SELECT id, id FROM src",
			"duplicate CTAS output column",
			"dup",
		),
		(
			"CREATE TABLE reserved AS SELECT id AS __row_hlc__ FROM src",
			"reserved for storage metadata",
			"reserved",
		),
	] {
		let (catalog, _storage, coordinator, planner) = coordinator();
		coordinator
			.execute_query(
				&planner
					.plan("CREATE TABLE src (id INT NOT NULL)")
					.expect("plan create src"),
				None,
			)
			.await
			.expect("create src");

		let err = coordinator
			.execute_query(&planner.plan(sql).expect("plan CTAS"), None)
			.await
			.expect_err("invalid CTAS should reject");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
		assert!(
			catalog.resolve_table(&TableRef::new(target)).await.is_err(),
			"target table {target} should not be created"
		);
	}
}

#[tokio::test]
async fn query_coordinator_ctas_write_failure_leaves_catalog_entry_without_storage() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let planner = SqlPlanner::new();
	let setup = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	);

	for sql in [
		"CREATE TABLE src (id INT NOT NULL)",
		"INSERT INTO src VALUES (1)",
	] {
		setup
			.execute_query(&planner.plan(sql).expect("plan setup SQL"), None)
			.await
			.expect(sql);
	}

	let failing = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		Arc::new(FailingWrite),
	);
	let err = failing
		.execute_query(
			&planner
				.plan("CREATE TABLE failed_copy AS SELECT id FROM src")
				.expect("plan CTAS"),
			None,
		)
		.await
		.expect_err("CTAS write should fail");
	assert!(
		err.to_string().contains("forced CTAS write failure"),
		"unexpected error: {err}"
	);

	let binding = catalog
		.resolve_table(&TableRef::new("failed_copy"))
		.await
		.expect("CTAS write failure leaves catalog entry");
	assert_field(&binding.schema, 0, "id", &DataType::Int32, false);

	let err = setup
		.execute_query(
			&planner
				.plan("SELECT id FROM failed_copy")
				.expect("plan failed_copy scan"),
			None,
		)
		.await
		.expect_err("failed write should not initialize storage");
	assert!(
		err.to_string().contains("table not found"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn distributed_coordinator_ctas_uses_distributed_source_path() {
	let catalog = Arc::new(people_catalog().await);
	let coordinator = DistributedQueryCoordinator::new(
		catalog,
		Arc::new(StaticWorkerRegistry::new([])),
		Arc::new(MemoryManager::new(1, 1024 * 1024)),
		Arc::new(FailingTransport),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE copy AS SELECT id, name FROM people")
		.expect("plan CTAS");

	let err = coordinator
		.execute_query(&plan)
		.await
		.expect_err("distributed CTAS should request source workers");

	assert!(
		err.to_string().contains("no live worker available"),
		"unexpected error: {err}"
	);
	assert!(
		!err.to_string()
			.contains("distributed CREATE TABLE AS SELECT execution is not wired"),
		"old coordinator reject should be lifted: {err}"
	);
}

#[tokio::test]
async fn executor_and_fragmenter_reject_direct_ctas_execution() {
	let catalog = people_catalog().await;
	let source = catalog
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let plan = ExecutionPlan::CreateTableAsSelect {
		table: TableRef::new("copy"),
		schema: source.schema.clone(),
		or_replace: false,
		source: Box::new(scan_plan(&source)),
	};
	let executor = Executor::with_context(
		empty_storage(),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("executor should reject CTAS");
	assert!(matches!(
		err,
		ExecutionError::NotImplemented {
			feature: "CREATE TABLE AS SELECT execution is coordinator-only"
		}
	));

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject CTAS");
	assert_eq!(err, FragmentError::CreateTableAsSelectNotFragmentable);
}

async fn people_catalog() -> MockCatalog {
	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("people"),
			schema(&[
				Field::new("id", DataType::Int32, false),
				Field::new("name", DataType::Utf8, true),
			]),
			false,
		)
		.await
		.expect("create people");
	catalog
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
	)
	.with_storage_admin(storage.clone());
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

fn assert_field(schema: &SchemaRef, idx: usize, name: &str, data_type: &DataType, nullable: bool) {
	let field = schema.field(idx);
	assert_eq!(field.name(), name);
	assert_eq!(field.data_type(), data_type);
	assert_eq!(field.is_nullable(), nullable);
}

fn schema(fields: &[Field]) -> SchemaRef {
	Arc::new(Schema::new(fields.to_vec()))
}

fn empty_storage() -> Arc<InMemoryStorageScan> {
	let schema = Arc::new(Schema::empty());
	let batch = RecordBatch::new_empty(schema);
	Arc::new(InMemoryStorageScan { table_data: batch })
}

fn scan_plan(binding: &TableBinding) -> ExecutionPlan {
	ExecutionPlan::Scan {
		legacy_table_id: binding.legacy_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema.clone(),
		projection: None,
		pbc_program: None,
		row_identity: toron::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: vec![],
	}
}

fn int32_column(batch: &RecordBatch, index: usize) -> &Int32Array {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("Int32 column")
}

fn int64_column(batch: &RecordBatch, index: usize) -> &Int64Array {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("Int64 column")
}

fn string_column(batch: &RecordBatch, index: usize) -> &StringArray {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("Utf8 column")
}

struct FailingTransport;

#[async_trait]
impl ToronTransport for FailingTransport {
	async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		Err(QueryError::Internal {
			message: "transport should not be used for CTAS".to_string(),
		})
	}
}

struct FailingWrite;

#[async_trait]
impl StorageWrite for FailingWrite {
	async fn write(&self, _request: WriteRequest) -> Result<WriteOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "forced CTAS write failure".to_string(),
		})
	}
}
