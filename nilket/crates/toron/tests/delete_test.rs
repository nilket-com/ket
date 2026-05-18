use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{Catalog, MockCatalog, TableRef};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::distributed::worker_registry::StaticWorkerRegistry;
use toron::error::QueryError;
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{ExecutionPlan, LogicalPlan, QueryProfileHint};
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{InMemoryStorageScan, InMemoryTableStorage};

#[test]
fn planner_lowers_delete_where() {
	let plan = SqlPlanner::new()
		.plan("DELETE FROM app.sales WHERE id = 7")
		.expect("plan delete");

	let LogicalPlan::Delete { table, predicate } = plan else {
		panic!("expected delete plan");
	};
	assert_eq!(table, TableRef::with_schema("app", "sales"));
	assert!(predicate.is_some());
}

#[test]
fn planner_lowers_delete_without_where() {
	let plan = SqlPlanner::new()
		.plan("DELETE FROM sales")
		.expect("plan delete");

	let LogicalPlan::Delete { table, predicate } = plan else {
		panic!("expected delete plan");
	};
	assert_eq!(table, TableRef::new("sales"));
	assert!(predicate.is_none());
}

#[test]
fn planner_rejects_unsupported_delete_shapes() {
	for (sql, diagnostic) in [
		("DELETE sales WHERE id = 1", "requires FROM keyword"),
		("DELETE FROM sales AS s WHERE id = 1", "table alias"),
		("DELETE FROM sales RETURNING id", "RETURNING"),
		("DELETE FROM sales ORDER BY id", "ORDER BY"),
		("DELETE FROM sales LIMIT 1", "LIMIT"),
		("DELETE FROM app.public.sales", "one- or two-part"),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[tokio::test]
async fn binder_resolves_delete_shape_a_identity() {
	let catalog = people_catalog().await;
	let plan = SqlPlanner::new()
		.plan("DELETE FROM people WHERE id = 1")
		.expect("plan delete");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind delete");
	let ExecutionPlan::Delete {
		legacy_table_id,
		table,
		fingerprint,
		schema,
		predicate,
		pbc_program,
		profile_hint,
	} = bound
	else {
		panic!("expected delete execution plan");
	};
	let binding = catalog
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	assert_eq!(legacy_table_id, binding.legacy_table_id);
	assert_eq!(table, binding.table);
	assert_eq!(fingerprint, binding.fingerprint);
	assert_eq!(schema.field(0).name(), "id");
	assert!(predicate.is_some());
	assert!(pbc_program.is_some());
	assert_eq!(profile_hint, QueryProfileHint::Scan);
}

#[tokio::test]
async fn binder_rejects_unknown_delete_column() {
	let catalog = people_catalog().await;
	let plan = SqlPlanner::new()
		.plan("DELETE FROM people WHERE missing = 1")
		.expect("plan delete");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unknown column should fail");
	assert!(
		err.to_string().contains("unknown column"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn binder_rejects_non_boolean_delete_predicate() {
	let catalog = people_catalog().await;
	let plan = SqlPlanner::new()
		.plan("DELETE FROM people WHERE id + 1")
		.expect("plan delete");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("non-boolean predicate should fail");
	assert!(
		err.to_string().contains("must return Boolean"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn coordinator_deletes_rows_with_pbc_predicate() {
	let (_catalog, _storage, coordinator, planner) = coordinator();
	setup_sales(&coordinator, &planner).await;

	let result = execute(&coordinator, &planner, "DELETE FROM sales WHERE id = 2").await;
	assert_eq!(deleted_count(&result), 1);

	let remaining = concat_query(&coordinator, &planner, "SELECT id FROM sales ORDER BY id").await;
	assert_eq!(int32_column(&remaining, 0).values(), &[1, 3, 4, 5]);
}

#[tokio::test]
async fn coordinator_deletes_rows_with_residual_predicate() {
	let (_catalog, _storage, coordinator, planner) = coordinator();
	setup_sales(&coordinator, &planner).await;

	let result = execute(
		&coordinator,
		&planner,
		"DELETE FROM sales WHERE amount + 1 > 40",
	)
	.await;
	assert_eq!(deleted_count(&result), 2);

	let remaining = concat_query(&coordinator, &planner, "SELECT id FROM sales ORDER BY id").await;
	assert_eq!(int32_column(&remaining, 0).values(), &[1, 2, 3]);
}

#[tokio::test]
async fn coordinator_deletes_all_rows_without_where() {
	let (_catalog, _storage, coordinator, planner) = coordinator();
	setup_sales(&coordinator, &planner).await;

	let result = execute(&coordinator, &planner, "DELETE FROM sales").await;
	assert_eq!(deleted_count(&result), 5);

	let remaining = concat_query(&coordinator, &planner, "SELECT id FROM sales").await;
	assert_eq!(remaining.num_rows(), 0);
}

#[tokio::test]
async fn coordinator_delete_zero_matches_returns_zero_count() {
	let (_catalog, _storage, coordinator, planner) = coordinator();
	setup_sales(&coordinator, &planner).await;

	let result = execute(&coordinator, &planner, "DELETE FROM sales WHERE id = 100").await;
	assert_eq!(deleted_count(&result), 0);

	let remaining = concat_query(&coordinator, &planner, "SELECT id FROM sales ORDER BY id").await;
	assert_eq!(int32_column(&remaining, 0).values(), &[1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn coordinator_repeated_delete_reports_deleted_only() {
	let (_catalog, _storage, coordinator, planner) = coordinator();
	setup_sales(&coordinator, &planner).await;

	let first = execute(&coordinator, &planner, "DELETE FROM sales WHERE id = 1").await;
	let second = execute(&coordinator, &planner, "DELETE FROM sales WHERE id = 1").await;
	assert_eq!(deleted_count(&first), 1);
	assert_eq!(deleted_count(&second), 0);
}

#[tokio::test]
async fn coordinator_empty_candidate_set_short_circuits() {
	let (_catalog, _storage, coordinator, planner) = coordinator();
	setup_sales(&coordinator, &planner).await;

	let result = execute(
		&coordinator,
		&planner,
		"DELETE FROM sales WHERE amount + 1 > 1000",
	)
	.await;
	assert_eq!(deleted_count(&result), 0);

	let remaining = concat_query(&coordinator, &planner, "SELECT id FROM sales ORDER BY id").await;
	assert_eq!(int32_column(&remaining, 0).values(), &[1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn distributed_coordinator_rejects_delete() {
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
		.plan("DELETE FROM people WHERE id = 1")
		.expect("plan delete");

	let err = coordinator
		.execute_query(&plan)
		.await
		.expect_err("distributed delete should reject");
	assert!(
		err.to_string()
			.contains("distributed DELETE execution is not wired"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn fragmenter_rejects_delete_defensively() {
	let catalog = people_catalog().await;
	let binding = catalog
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let plan = ExecutionPlan::Delete {
		legacy_table_id: binding.legacy_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		predicate: None,
		pbc_program: None,
		profile_hint: QueryProfileHint::Scan,
	};

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject delete");
	assert_eq!(err, FragmentError::DeleteNotFragmentable);
}

#[tokio::test]
async fn executor_rejects_direct_delete_execution() {
	let catalog = people_catalog().await;
	let binding = catalog
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let plan = ExecutionPlan::Delete {
		legacy_table_id: binding.legacy_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		predicate: None,
		pbc_program: None,
		profile_hint: QueryProfileHint::Scan,
	};
	let executor = Executor::with_context(empty_storage(), ExecutionContext::local_test());

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("direct delete should reject");
	assert!(
		err.to_string()
			.contains("DELETE execution is coordinator-only"),
		"unexpected error: {err}"
	);
}

async fn setup_sales(coordinator: &QueryCoordinator, planner: &SqlPlanner) {
	for sql in [
		"CREATE TABLE sales (id INT NOT NULL, amount INT NOT NULL, name TEXT)",
		"INSERT INTO sales VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd'), (5, 50, 'e')",
	] {
		execute(coordinator, planner, sql).await;
	}
}

async fn execute(
	coordinator: &QueryCoordinator,
	planner: &SqlPlanner,
	sql: &str,
) -> Vec<RecordBatch> {
	coordinator
		.execute_query(&planner.plan(sql).expect("plan SQL"), None)
		.await
		.expect(sql)
}

async fn concat_query(
	coordinator: &QueryCoordinator,
	planner: &SqlPlanner,
	sql: &str,
) -> RecordBatch {
	let results = execute(coordinator, planner, sql).await;
	let schema = results[0].schema();
	arrow::compute::concat_batches(&schema, results.iter()).expect("concat query results")
}

fn deleted_count(results: &[RecordBatch]) -> i64 {
	assert_eq!(results.len(), 1);
	assert_eq!(results[0].num_rows(), 1);
	assert_eq!(results[0].schema().field(0).name(), "deleted");
	assert_eq!(results[0].schema().field(0).data_type(), &DataType::Int64);
	results[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("deleted column")
		.value(0)
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
	let coordinator = QueryCoordinator::with_writer_and_deleter(
		catalog.clone(),
		executor,
		PlannerConfig::default(),
		storage.clone(),
		storage.clone(),
	);
	(catalog, storage, coordinator, SqlPlanner::new())
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

fn assert_frontend_error(sql: &str, diagnostic: &str) {
	let err = SqlPlanner::new().plan(sql).expect_err("SQL should reject");
	assert!(
		err.to_string().contains(diagnostic),
		"expected diagnostic {diagnostic:?}, got {err}"
	);
}

fn schema(fields: &[Field]) -> SchemaRef {
	Arc::new(Schema::new(fields.to_vec()))
}

fn empty_storage() -> Arc<InMemoryStorageScan> {
	let schema = Arc::new(Schema::empty());
	let batch = RecordBatch::new_empty(schema);
	Arc::new(InMemoryStorageScan { table_data: batch })
}

fn int32_column(batch: &RecordBatch, index: usize) -> &Int32Array {
	batch
		.column(index)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("Int32 column")
}

#[allow(dead_code)]
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
			message: "transport should not be used for DELETE".to_string(),
		})
	}
}
