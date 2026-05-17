use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, TABLE_WRITE_MAX_ROWS, bind};
use toron::catalog::{Catalog, MockCatalog, TableBinding, TableRef};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::distributed::worker_registry::StaticWorkerRegistry;
use toron::error::{BindingError, ExecutionError, QueryError, StorageError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	ExecutionPlan, InsertLiteral, LogicalPlan, QueryProfileHint, ScalarValue, SessionId, TableId,
};
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	HlcBound, InMemoryStorageScan, InMemoryTableStorage, ScanRequest, StorageScan, StorageWrite,
	TenantId, WriteRequest,
};
use wendelon::schema::TableObjectId;

#[test]
fn planner_accepts_literal_insert_surface() {
	let plan = SqlPlanner::new()
		.plan("INSERT INTO app.sales (name, id) VALUES ('Alice', 1), ('Bob', 2)")
		.expect("plan insert");

	let LogicalPlan::Insert {
		table,
		columns,
		rows,
	} = plan
	else {
		panic!("expected insert plan");
	};

	assert_eq!(table, TableRef::with_schema("app", "sales"));
	assert_eq!(columns, Some(vec!["name".to_string(), "id".to_string()]));
	assert_eq!(
		rows,
		vec![
			vec![
				InsertLiteral::Utf8("Alice".to_string()),
				InsertLiteral::Int64(1)
			],
			vec![
				InsertLiteral::Utf8("Bob".to_string()),
				InsertLiteral::Int64(2)
			],
		]
	);
}

#[test]
fn planner_accepts_full_column_null_and_negative_literals() {
	let plan = SqlPlanner::new()
		.plan("INSERT INTO readings VALUES (NULL, -1)")
		.expect("plan insert");

	let LogicalPlan::Insert {
		table,
		columns,
		rows,
	} = plan
	else {
		panic!("expected insert plan");
	};

	assert_eq!(table, TableRef::new("readings"));
	assert_eq!(columns, None);
	assert_eq!(
		rows,
		vec![vec![InsertLiteral::Null, InsertLiteral::Int64(-1)]]
	);
}

#[test]
fn planner_rejects_unsupported_insert_shapes() {
	for (sql, diagnostic) in [
		("INSERT INTO t SELECT 1", "INSERT source must be VALUES"),
		("INSERT t (a) VALUES (1)", "missing INTO"),
		("INSERT INTO TABLE t (a) VALUES (1)", "TABLE keyword"),
		("INSERT INTO t (a) VALUES (1) RETURNING a", "RETURNING"),
		(
			"INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING",
			"ON CONFLICT",
		),
		("INSERT OR REPLACE INTO t (a) VALUES (1)", "OR"),
		("INSERT IGNORE INTO t (a) VALUES (1)", "IGNORE"),
		("REPLACE INTO t (a) VALUES (1)", "REPLACE INTO"),
		(
			"INSERT INTO catalog.public.t (a) VALUES (1)",
			"only one- or two-part",
		),
		("INSERT OVERWRITE TABLE t (a) VALUES (1)", "OVERWRITE"),
		(
			"INSERT INTO t (a) VALUES (DATE '2024-01-01')",
			"must be literals",
		),
		("INSERT INTO t (a) VALUES (1 + 2)", "must be literals"),
		("INSERT INTO t (a) VALUES (now())", "must be literals"),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[tokio::test]
async fn binder_reorders_columns_and_fills_typed_nulls() {
	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("people"),
			schema(&[
				Field::new("id", DataType::Int32, false),
				Field::new("name", DataType::Utf8, true),
				Field::new("score", DataType::Float64, true),
			]),
			false,
		)
		.await
		.expect("create table");
	let plan = SqlPlanner::new()
		.plan("INSERT INTO people (name, id) VALUES ('Alice', 7)")
		.expect("plan insert");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind insert");
	let ExecutionPlan::Insert { rows, schema, .. } = bound else {
		panic!("expected insert execution plan");
	};

	assert_eq!(
		rows,
		vec![vec![
			ScalarValue::Int32(Some(7)),
			ScalarValue::Utf8(Some("Alice".to_string())),
			ScalarValue::Float64(None),
		]]
	);
	assert_eq!(schema.field(0).name(), "id");
	assert_eq!(schema.field(1).name(), "name");
	assert_eq!(schema.field(2).name(), "score");
}

#[tokio::test]
async fn binder_coerces_all_create_table_types() {
	let catalog = MockCatalog::new();
	catalog
		.create_table(
			&TableRef::new("metrics"),
			schema(&[
				Field::new("i32_value", DataType::Int32, false),
				Field::new("i64_value", DataType::Int64, false),
				Field::new("f32_value", DataType::Float32, false),
				Field::new("f64_value", DataType::Float64, false),
				Field::new("label", DataType::Utf8, false),
				Field::new("active", DataType::Boolean, false),
			]),
			false,
		)
		.await
		.expect("create metrics");
	let plan = SqlPlanner::new()
		.plan("INSERT INTO metrics VALUES (1, 9007199254740991, 1.25, 2.5, 'ok', TRUE)")
		.expect("plan insert");

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind insert");
	let ExecutionPlan::Insert { rows, .. } = bound else {
		panic!("expected insert execution plan");
	};

	assert_eq!(
		rows,
		vec![vec![
			ScalarValue::Int32(Some(1)),
			ScalarValue::Int64(Some(9_007_199_254_740_991)),
			ScalarValue::Float32(Some(1.25)),
			ScalarValue::Float64(Some(2.5)),
			ScalarValue::Utf8(Some("ok".to_string())),
			ScalarValue::Boolean(Some(true)),
		]]
	);
}

#[tokio::test]
async fn binder_rejects_invalid_insert_bindings() {
	for (sql, diagnostic) in [
		(
			"INSERT INTO people (id, id) VALUES (1, 2)",
			"duplicate INSERT target column",
		),
		("INSERT INTO people (missing) VALUES (1)", "unknown column"),
		(
			"INSERT INTO people (id, name) VALUES (1)",
			"has 1 values; expected 2",
		),
		(
			"INSERT INTO people (name) VALUES ('Alice')",
			"omits non-nullable column",
		),
		(
			"INSERT INTO people (id) VALUES (NULL)",
			"NULL is not allowed",
		),
		(
			"INSERT INTO people (id) VALUES (99999999999)",
			"overflows column",
		),
		(
			"INSERT INTO people (name) VALUES (1)",
			"not compatible with column",
		),
	] {
		let catalog = people_catalog().await;
		let plan = SqlPlanner::new().plan(sql).expect("plan insert");
		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("bind should reject");
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}
}

#[tokio::test]
async fn binder_rejects_insert_row_cap() {
	let catalog = people_catalog().await;
	let rows = vec![vec![InsertLiteral::Int64(1)]; TABLE_WRITE_MAX_ROWS + 1];
	let plan = LogicalPlan::Insert {
		table: TableRef::new("people"),
		columns: Some(vec!["id".to_string()]),
		rows,
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("row cap should reject");

	assert!(matches!(err, BindingError::UnsupportedExpression { .. }));
	assert!(err.to_string().contains("exceeds maximum"));
}

#[tokio::test]
async fn binder_rejects_empty_insert_rows() {
	let catalog = people_catalog().await;
	let plan = LogicalPlan::Insert {
		table: TableRef::new("people"),
		columns: Some(vec!["id".to_string()]),
		rows: Vec::new(),
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("empty insert rows should reject");

	assert!(matches!(err, BindingError::UnsupportedExpression { .. }));
	assert!(err.to_string().contains("at least one row"));
}

#[tokio::test]
async fn query_coordinator_executes_insert_and_select_round_trip() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let executor = Executor::new(storage.clone());
	let coordinator =
		QueryCoordinator::with_writer(catalog, executor, PlannerConfig::default(), storage.clone());
	let planner = SqlPlanner::new();

	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE people (id INT NOT NULL, name TEXT)")
				.expect("plan create"),
			None,
		)
		.await
		.expect("create");
	coordinator
		.execute_query(
			&planner
				.plan("INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob')")
				.expect("plan insert"),
			None,
		)
		.await
		.expect("insert");

	let results = coordinator
		.execute_query(
			&planner
				.plan("SELECT id, name FROM people ORDER BY id")
				.expect("plan select"),
			None,
		)
		.await
		.expect("select");
	let schema = results[0].schema();
	let batch = arrow::compute::concat_batches(&schema, results.iter()).expect("concat results");

	assert_eq!(batch.num_rows(), 2);
	let ids = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("id column");
	let names = batch
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("name column");
	assert_eq!(ids.value(0), 1);
	assert_eq!(ids.value(1), 2);
	assert_eq!(names.value(0), "Alice");
	assert_eq!(names.value(1), "Bob");
}

#[tokio::test]
async fn in_memory_table_storage_seeds_appends_and_rejects_invalid_writes() {
	let binding = people_catalog()
		.await
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let storage = Arc::new(InMemoryTableStorage::new());

	let mut missing_scan = scan_request(&binding);
	missing_scan.table_id = TableId(binding.legacy_table_id.0 + 1000);
	let missing_err = match storage.scan(missing_scan).await {
		Ok(_) => panic!("missing table scan should reject"),
		Err(err) => err,
	};
	assert!(matches!(
		missing_err,
		StorageError::TableNotFound { table } if table == TableId(binding.legacy_table_id.0 + 1000)
	));

	let first = storage
		.write(write_request(
			&binding,
			people_batch(binding.schema.clone(), vec![1], vec![Some("Alice")]),
		))
		.await
		.expect("seed table");
	let second = storage
		.write(write_request(
			&binding,
			people_batch(binding.schema.clone(), vec![2], vec![Some("Bob")]),
		))
		.await
		.expect("append table");
	assert_eq!(first.rows, 1);
	assert_eq!(second.rows, 1);

	let batches = Executor::new(storage.clone())
		.execute_bound(&scan_plan(&binding), None)
		.await
		.expect("scan appended table");
	let batch =
		arrow::compute::concat_batches(&binding.schema, batches.iter()).expect("concat scan");
	assert_eq!(batch.num_rows(), 2);
	let ids = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("id column");
	let names = batch
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("name column");
	assert_eq!(ids.value(0), 1);
	assert_eq!(ids.value(1), 2);
	assert_eq!(names.value(0), "Alice");
	assert_eq!(names.value(1), "Bob");

	let bad_schema_err = storage
		.write(write_request(&binding, mismatched_people_batch()))
		.await
		.expect_err("schema mismatch should reject");
	assert!(
		matches!(bad_schema_err, StorageError::Internal { ref message } if message.contains("schema does not match")),
		"unexpected error: {bad_schema_err}"
	);

	let mut bad_identity = write_request(
		&binding,
		people_batch(binding.schema.clone(), vec![3], vec![Some("Carol")]),
	);
	bad_identity.table_object_id = TableObjectId::new([0xED; 16]);
	let bad_identity_err = storage
		.write(bad_identity)
		.await
		.expect_err("identity mismatch should reject");
	assert!(matches!(bad_identity_err, StorageError::DescriptorEvolved));
}

#[tokio::test]
async fn query_coordinator_default_writer_rejects_insert() {
	let catalog = Arc::new(people_catalog().await);
	let executor = Executor::new(empty_storage());
	let coordinator = QueryCoordinator::new(catalog, executor, PlannerConfig::default());
	let plan = SqlPlanner::new()
		.plan("INSERT INTO people (id, name) VALUES (1, 'Alice')")
		.expect("plan insert");

	let err = coordinator
		.execute_query(&plan, None)
		.await
		.expect_err("default writer should reject insert");

	assert!(
		err.to_string()
			.contains("INSERT execution requires a configured StorageWrite"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn distributed_query_coordinator_rejects_insert_before_dispatch() {
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
		.plan("INSERT INTO people (id, name) VALUES (1, 'Alice')")
		.expect("plan insert");

	let err = coordinator
		.execute_query(&plan)
		.await
		.expect_err("distributed insert should reject");

	assert!(
		err.to_string()
			.contains("distributed INSERT execution is not wired"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn executor_rejects_direct_insert_execution() {
	let executor = Executor::with_context(
		empty_storage(),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);
	let binding = people_catalog()
		.await
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let plan = ExecutionPlan::Insert {
		legacy_table_id: binding.legacy_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		rows: vec![vec![
			ScalarValue::Int32(Some(1)),
			ScalarValue::Utf8(Some("Alice".to_string())),
		]],
	};

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("executor should reject insert");

	assert!(matches!(
		err,
		ExecutionError::NotImplemented {
			feature: "INSERT execution is coordinator-only"
		}
	));
}

#[tokio::test]
async fn fragmenter_rejects_direct_insert_fragmentation() {
	let binding = people_catalog()
		.await
		.resolve_table(&TableRef::new("people"))
		.await
		.expect("resolve people");
	let plan = ExecutionPlan::Insert {
		legacy_table_id: binding.legacy_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		rows: vec![vec![
			ScalarValue::Int32(Some(1)),
			ScalarValue::Utf8(Some("Alice".to_string())),
		]],
	};

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject insert");

	assert_eq!(err, FragmentError::InsertNotFragmentable);
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

fn write_request(binding: &TableBinding, payload: RecordBatch) -> WriteRequest {
	WriteRequest {
		session_id: SessionId(1),
		query_id: QueryId::from_u128(1),
		tenant_id: TenantId(1),
		table_id: binding.legacy_table_id,
		table_object_id: binding.table,
		physical_descriptor_fingerprint: binding.fingerprint,
		payload,
		profile_hint: None,
		trace_context: None,
	}
}

fn scan_request(binding: &TableBinding) -> ScanRequest {
	ScanRequest {
		session_id: SessionId(1),
		query_id: QueryId::from_u128(1),
		tenant_id: TenantId(1),
		table_id: binding.legacy_table_id,
		table_object_id: binding.table,
		physical_descriptor_fingerprint: binding.fingerprint,
		read_bound: HlcBound(1),
		projected_columns: None,
		pbc_predicate: None,
		runtime_filter: None,
		profile_hint: None,
		trace_context: None,
		morsel: None,
	}
}

fn scan_plan(binding: &TableBinding) -> ExecutionPlan {
	ExecutionPlan::Scan {
		legacy_table_id: binding.legacy_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema.clone(),
		projection: None,
		pbc_program: None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: vec![],
	}
}

fn people_batch(schema: SchemaRef, ids: Vec<i32>, names: Vec<Option<&str>>) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from(ids)),
			Arc::new(StringArray::from(names)),
		],
	)
	.expect("people batch")
}

fn mismatched_people_batch() -> RecordBatch {
	RecordBatch::try_new(
		schema(&[
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]),
		vec![
			Arc::new(Int64Array::from(vec![3_i64])),
			Arc::new(StringArray::from(vec![Some("Carol")])),
		],
	)
	.expect("mismatched people batch")
}

struct FailingTransport;

#[async_trait]
impl ToronTransport for FailingTransport {
	async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		Err(QueryError::Internal {
			message: "transport should not be used for INSERT".to_string(),
		})
	}
}
