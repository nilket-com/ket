use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{
	Catalog, MockCatalog, TableBinding, TableRef, TableRowIdentity, WendelonCatalog,
};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{QueryBudget, TaskBudget};
use toron::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::QueryId;
use toron::distributed::worker_registry::StaticWorkerRegistry;
use toron::error::{CatalogError, ExecutionError, QueryError};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	ColumnOrdinal, CreateTableColumnType, CreateTableRowIdentity, CreateTableSchema, ExecutionPlan,
	LogicalPlan, QueryProfileHint, SessionId, TableId,
};
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{HlcBound, InMemoryStorageScan, InMemoryTableStorage, TenantId};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptor,
};

#[test]
fn planner_accepts_create_table_surface() {
	let plan = SqlPlanner::new()
		.plan(
			"CREATE TABLE sales (
				id INT,
				amount BIGINT,
				price DOUBLE,
				score REAL,
				name TEXT,
				active BOOLEAN
			)",
		)
		.expect("plan create table");

	let LogicalPlan::CreateTable {
		name,
		schema,
		row_identity,
		if_not_exists,
		or_replace,
	} = plan
	else {
		panic!("expected create table plan");
	};

	assert_eq!(name, TableRef::new("sales"));
	assert!(!if_not_exists);
	assert!(!or_replace);
	assert_eq!(row_identity, CreateTableRowIdentity::AppendOnly);
	assert_create_column(&schema, 0, "id", &DataType::Int32, true);
	assert_create_column(&schema, 1, "amount", &DataType::Int64, true);
	assert_create_column(&schema, 2, "price", &DataType::Float64, true);
	assert_create_column(&schema, 3, "score", &DataType::Float32, true);
	assert_create_column(&schema, 4, "name", &DataType::Utf8, true);
	assert_create_column(&schema, 5, "active", &DataType::Boolean, true);
}

#[test]
fn planner_accepts_if_not_exists_qualified_name_and_nullability() {
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE IF NOT EXISTS app.sales (id INT NOT NULL, note VARCHAR(128) NULL)")
		.expect("plan create table");

	let LogicalPlan::CreateTable {
		name,
		schema,
		row_identity,
		if_not_exists,
		or_replace,
	} = plan
	else {
		panic!("expected create table plan");
	};

	assert_eq!(name, TableRef::with_schema("app", "sales"));
	assert!(if_not_exists);
	assert!(!or_replace);
	assert_eq!(row_identity, CreateTableRowIdentity::AppendOnly);
	assert_create_column(&schema, 0, "id", &DataType::Int32, false);
	assert_create_column(&schema, 1, "note", &DataType::Utf8, true);
}

#[test]
fn planner_preserves_quoted_object_name_parts_after_sqlparser_062() {
	let plan = SqlPlanner::new()
		.plan(r#"CREATE TABLE "App"."Sales" ("Id" INT NOT NULL)"#)
		.expect("plan quoted create table");

	let LogicalPlan::CreateTable { name, schema, .. } = plan else {
		panic!("expected create table");
	};
	assert_eq!(name, TableRef::with_schema("App", "Sales"));
	assert_create_column(&schema, 0, "Id", &DataType::Int32, false);
}

#[test]
fn planner_accepts_create_or_replace_table_surface() {
	let plan = SqlPlanner::new()
		.plan("CREATE OR REPLACE TABLE sales (id INT NOT NULL)")
		.expect("plan create or replace table");

	let LogicalPlan::CreateTable {
		name,
		schema,
		row_identity,
		if_not_exists,
		or_replace,
	} = plan
	else {
		panic!("expected create table plan");
	};

	assert_eq!(name, TableRef::new("sales"));
	assert!(!if_not_exists);
	assert!(or_replace);
	assert_eq!(row_identity, CreateTableRowIdentity::AppendOnly);
	assert_create_column(&schema, 0, "id", &DataType::Int32, false);
}

#[test]
fn planner_accepts_column_primary_key() {
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, note TEXT)")
		.expect("plan column primary key");

	let LogicalPlan::CreateTable {
		name,
		schema,
		row_identity,
		if_not_exists,
		or_replace,
	} = plan
	else {
		panic!("expected create table plan");
	};

	assert_eq!(name, TableRef::new("sales"));
	assert!(!if_not_exists);
	assert!(!or_replace);
	assert_eq!(
		row_identity,
		CreateTableRowIdentity::PrimaryKey {
			column: "id".to_string()
		}
	);
	assert_create_column(&schema, 0, "id", &DataType::Int32, false);
	assert_create_column(&schema, 1, "note", &DataType::Utf8, true);
}

#[test]
fn planner_accepts_table_primary_key() {
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE sales (id BIGINT NOT NULL, note TEXT, PRIMARY KEY (id))")
		.expect("plan table primary key");

	let LogicalPlan::CreateTable {
		name,
		schema,
		row_identity,
		if_not_exists,
		or_replace,
	} = plan
	else {
		panic!("expected create table plan");
	};

	assert_eq!(name, TableRef::new("sales"));
	assert!(!if_not_exists);
	assert!(!or_replace);
	assert_eq!(
		row_identity,
		CreateTableRowIdentity::PrimaryKey {
			column: "id".to_string()
		}
	);
	assert_create_column(&schema, 0, "id", &DataType::Int64, false);
	assert_create_column(&schema, 1, "note", &DataType::Utf8, true);
}

#[test]
fn planner_rejects_unsupported_create_table_shapes() {
	for (sql, diagnostic) in [
		("CREATE TABLE t ()", "requires at least one column"),
		(
			"CREATE TABLE t (a INT, a BIGINT)",
			"column names must be unique",
		),
		(
			"CREATE TABLE catalog.public.t (a INT)",
			"only one- or two-part table names",
		),
		(
			"CREATE TEMPORARY TABLE t (a INT)",
			"unsupported CREATE TABLE clause: TEMPORARY",
		),
		(
			"CREATE TABLE t LIKE other",
			"unsupported CREATE TABLE clause: LIKE",
		),
		(
			"CREATE TABLE t (a INT DEFAULT 1)",
			"unsupported CREATE TABLE column option: DEFAULT",
		),
		("CREATE TABLE t (a INT UNIQUE)", "column option: UNIQUE"),
		("CREATE TABLE t (a INT, UNIQUE(a))", "table-level UNIQUE"),
		(
			"CREATE TABLE t (a INT CHECK (a > 0))",
			"unsupported CREATE TABLE column option: CHECK",
		),
		(
			"CREATE TABLE t (a INT REFERENCES x(b))",
			"unsupported CREATE TABLE column option: REFERENCES",
		),
		(
			"CREATE TABLE t (a INT CONSTRAINT nn NOT NULL)",
			"named constraint",
		),
		(
			"CREATE TABLE t (a DECIMAL(10,2))",
			"unsupported CREATE TABLE column type: DECIMAL",
		),
		(
			"CREATE TABLE t (a FLOAT)",
			"unsupported CREATE TABLE column type: FLOAT",
		),
		(
			"CREATE TABLE t (a TIMESTAMP)",
			"unsupported CREATE TABLE column type: TIMESTAMP",
		),
		(
			"CREATE TABLE t (a INT NULL NOT NULL)",
			"nullability is specified more than once",
		),
		(
			"CREATE TABLE t (a INT INVISIBLE)",
			"unsupported CREATE TABLE column option",
		),
		(
			"CREATE TABLE t (a INT) WITH (foo = 'bar')",
			"unsupported CREATE TABLE clause: WITH options",
		),
	] {
		assert_frontend_error(sql, diagnostic);
	}
}

#[test]
fn planner_rejects_primary_key_without_not_null_column_form() {
	assert_frontend_error(
		"CREATE TABLE t (id INT PRIMARY KEY)",
		"PRIMARY KEY column must be declared NOT NULL",
	);
}

#[test]
fn planner_rejects_primary_key_without_not_null_table_form() {
	assert_frontend_error(
		"CREATE TABLE t (id INT, PRIMARY KEY(id))",
		"PRIMARY KEY column must be declared NOT NULL",
	);
}

#[test]
fn planner_rejects_null_primary_key_conflict() {
	assert_frontend_error(
		"CREATE TABLE t (id INT NULL PRIMARY KEY)",
		"PRIMARY KEY column must be declared NOT NULL",
	);
}

#[test]
fn planner_rejects_composite_primary_key() {
	assert_frontend_error(
		"CREATE TABLE t (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY(id, name))",
		"PRIMARY KEY must reference exactly one column",
	);
}

#[test]
fn planner_rejects_duplicate_primary_key_declarations() {
	assert_frontend_error(
		"CREATE TABLE t (id INT NOT NULL PRIMARY KEY, PRIMARY KEY(id))",
		"PRIMARY KEY more than once",
	);
}

#[test]
fn planner_rejects_unique_constraint() {
	assert_frontend_error("CREATE TABLE t (id INT UNIQUE)", "column option: UNIQUE");
	assert_frontend_error("CREATE TABLE t (id INT, UNIQUE(id))", "table-level UNIQUE");
}

#[test]
fn planner_rejects_unsupported_primary_key_type() {
	assert_frontend_error(
		"CREATE TABLE t (active BOOLEAN NOT NULL PRIMARY KEY)",
		"unsupported CREATE TABLE PRIMARY KEY column type",
	);
}

#[test]
fn planner_rejects_ctas_primary_key() {
	assert_frontend_error(
		"CREATE TABLE t (id INT NOT NULL PRIMARY KEY) AS SELECT 1",
		"CTAS does not support explicit column lists",
	);
}

#[test]
fn planner_rejects_storage_synthetic_column_names() {
	for column in [
		"__row_hlc__",
		"__row_key_id__",
		"__row_routing_locality__",
		"__row_table_object_id__",
		"__row_tombstone__",
	] {
		assert_frontend_error(
			&format!("CREATE TABLE t ({column} INT)"),
			"reserved for storage metadata",
		);
	}
}

#[test]
fn create_or_replace_table_rejects_if_not_exists_combo() {
	assert_frontend_error(
		"CREATE OR REPLACE TABLE IF NOT EXISTS sales (id INT)",
		"CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS",
	);
}

#[test]
fn planner_preserves_non_create_table_statement_rejects() {
	for sql in [
		"ALTER TABLE t ADD COLUMN b INT",
		"CREATE INDEX i ON t (a)",
		"CREATE VIEW v AS SELECT 1",
	] {
		assert_frontend_error(
			sql,
			"unsupported SQL statement; only SELECT, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE are supported",
		);
	}
}

#[tokio::test]
async fn mock_catalog_create_table_resolves_binding() {
	let catalog = MockCatalog::new();
	let schema = schema(&[Field::new("id", DataType::Int32, false)]);
	let name = TableRef::with_schema("app", "sales");

	let binding = catalog
		.create_table(&name, schema.clone(), false)
		.await
		.expect("create table");
	let resolved = catalog.resolve_table(&name).await.expect("resolve table");
	let unqualified = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("resolve unqualified alias");

	assert_eq!(binding.legacy_table_id, TableId(1));
	assert_eq!(resolved.legacy_table_id, binding.legacy_table_id);
	assert_eq!(unqualified.legacy_table_id, binding.legacy_table_id);
	assert_eq!(resolved.schema.as_ref(), schema.as_ref());
	assert_eq!(
		&binding.table.as_bytes()[8..],
		&binding.legacy_table_id.0.to_be_bytes()
	);
	assert_eq!(
		binding.fingerprint,
		mock_descriptor(&name, schema.as_ref(), binding.table).physical_descriptor_fingerprint
	);
	assert_eq!(binding.row_identity, TableRowIdentity::AppendOnly);
	assert!(binding.primary_key_columns.is_empty());
}

#[tokio::test]
async fn catalog_create_with_pk_builds_primary_key_descriptor() {
	let catalog = MockCatalog::new();
	let schema = schema(&[
		Field::new("id", DataType::Int64, false),
		Field::new("note", DataType::Utf8, true),
	]);
	let name = TableRef::new("sales");

	let outcome = catalog
		.create_table_with_outcome(
			&name,
			schema.clone(),
			CreateTableRowIdentity::PrimaryKey {
				column: "id".to_string(),
			},
			false,
		)
		.await
		.expect("create primary-key table");

	assert!(outcome.created);
	assert_eq!(
		outcome.binding.row_identity,
		TableRowIdentity::PrimaryKey {
			column: "id".to_string(),
			ordinal: ColumnOrdinal(0),
		}
	);
	assert!(
		outcome
			.binding
			.primary_key_columns
			.contains(&ColumnOrdinal(0))
	);
	assert_eq!(outcome.binding.schema.as_ref(), schema.as_ref());
}

#[tokio::test]
async fn mock_catalog_create_table_if_not_exists_semantics() {
	let catalog = MockCatalog::new();
	let name = TableRef::new("sales");
	let initial_schema = schema(&[Field::new("id", DataType::Int32, false)]);
	let different = schema(&[Field::new("id", DataType::Int64, false)]);

	let first = catalog
		.create_table(&name, initial_schema.clone(), false)
		.await
		.expect("create table");
	let second = catalog
		.create_table(&name, initial_schema, true)
		.await
		.expect("idempotent create table");
	assert_eq!(first.legacy_table_id, second.legacy_table_id);

	let mismatch = expect_catalog_err(catalog.create_table(&name, different, true).await);
	assert!(matches!(
		mismatch,
		CatalogError::SchemaMismatch { ref table } if table == "sales"
	));

	let exists = expect_catalog_err(
		catalog
			.create_table(
				&name,
				schema(&[Field::new("id", DataType::Int32, false)]),
				false,
			)
			.await,
	);
	assert!(matches!(
		exists,
		CatalogError::TableAlreadyExists { ref table } if table == "sales"
	));
}

#[tokio::test]
async fn if_not_exists_same_schema_same_identity_skips() {
	let catalog = MockCatalog::new();
	let name = TableRef::new("sales");
	let schema = schema(&[
		Field::new("id", DataType::Int64, false),
		Field::new("sku", DataType::Utf8, false),
	]);
	let identity = CreateTableRowIdentity::PrimaryKey {
		column: "id".to_string(),
	};

	let created = catalog
		.create_table_with_outcome(&name, schema.clone(), identity.clone(), true)
		.await
		.expect("create primary-key table");
	let existing = catalog
		.create_table_with_outcome(&name, schema.clone(), identity, true)
		.await
		.expect("same schema and identity skips");

	assert!(created.created);
	assert!(!existing.created);
	assert_eq!(
		created.binding.legacy_table_id,
		existing.binding.legacy_table_id
	);
}

#[tokio::test]
async fn if_not_exists_same_schema_different_identity_schema_mismatch() {
	let catalog = MockCatalog::new();
	let name = TableRef::new("sales");
	let schema = schema(&[
		Field::new("id", DataType::Int64, false),
		Field::new("sku", DataType::Utf8, false),
	]);

	catalog
		.create_table_with_outcome(
			&name,
			schema.clone(),
			CreateTableRowIdentity::PrimaryKey {
				column: "id".to_string(),
			},
			false,
		)
		.await
		.expect("create primary-key table");

	let mismatch = match catalog
		.create_table_with_outcome(
			&name,
			schema,
			CreateTableRowIdentity::PrimaryKey {
				column: "sku".to_string(),
			},
			true,
		)
		.await
	{
		Ok(_) => panic!("different identity should mismatch"),
		Err(error) => error,
	};

	assert!(matches!(
		mismatch,
		CatalogError::SchemaMismatch { ref table } if table == "sales"
	));
}

#[tokio::test]
async fn mock_catalog_schema_qualified_alias_does_not_overwrite() {
	let mut catalog = MockCatalog::new();
	let schema = schema(&[Field::new("id", DataType::Int32, false)]);
	let app_name = TableRef::with_schema("app", "sales");
	let default_name = TableRef::with_schema("default", "sales");
	let other_name = TableRef::with_schema("other", "sales");

	let app = catalog
		.create_table(&app_name, schema.clone(), false)
		.await
		.expect("create app.sales");
	let default_binding = binding_for(TableId(99), &default_name, schema.as_ref());
	catalog
		.register_table(default_name, default_binding)
		.expect("register default.sales");
	let other = catalog
		.create_table(&other_name, schema, false)
		.await
		.expect("create other.sales");

	let unqualified = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("resolve unqualified alias");
	assert_eq!(unqualified.legacy_table_id, app.legacy_table_id);
	assert_ne!(other.legacy_table_id, app.legacy_table_id);
}

#[tokio::test]
async fn wendelon_catalog_create_table_requires_admin_client() {
	let catalog = WendelonCatalog::default();
	let err = expect_catalog_err(
		catalog
			.create_table(
				&TableRef::new("sales"),
				schema(&[Field::new("id", DataType::Int32, false)]),
				false,
			)
			.await,
	);

	assert!(matches!(err, CatalogError::AdminFailed { .. }));
	assert!(
		err.to_string()
			.contains("CREATE TABLE requires a configured Wendelon catalog admin client")
	);
}

#[tokio::test]
async fn query_coordinator_executes_create_table_as_catalog_operation() {
	let catalog = Arc::new(MockCatalog::new());
	let executor = Executor::new(empty_storage());
	let coordinator = QueryCoordinator::new(catalog.clone(), executor, PlannerConfig::default());
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE app.sales (id INT NOT NULL)")
		.expect("plan create table");

	let results = coordinator
		.execute_query(&plan, None)
		.await
		.expect("execute create table");

	assert!(results.is_empty());
	let resolved = catalog
		.resolve_table(&TableRef::with_schema("app", "sales"))
		.await
		.expect("created table is visible in catalog");
	assert_field(&resolved.schema, 0, "id", &DataType::Int32, false);
	assert_eq!(resolved.row_identity, TableRowIdentity::AppendOnly);
}

#[tokio::test]
async fn sql_created_pk_descriptor_enables_point_read_profile() {
	let catalog = Arc::new(MockCatalog::new());
	let executor = Executor::new(empty_storage());
	let coordinator = QueryCoordinator::new(catalog.clone(), executor, PlannerConfig::default());
	let planner = SqlPlanner::new();

	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, note TEXT)")
				.expect("plan primary-key create"),
			None,
		)
		.await
		.expect("execute primary-key create");

	let binding = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("created table is visible");
	assert_eq!(
		binding.row_identity,
		TableRowIdentity::PrimaryKey {
			column: "id".to_string(),
			ordinal: ColumnOrdinal(0),
		}
	);

	let select = planner
		.plan("SELECT note FROM sales WHERE id = 1")
		.expect("plan point read");
	let bound = bind(&select, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect("bind point read");
	assert_point_read_scan(&bound);
}

#[tokio::test]
async fn create_or_replace_table_creates_when_absent() {
	let (catalog, _storage, coordinator, planner) = storage_coordinator();

	let results = coordinator
		.execute_query(
			&planner
				.plan("CREATE OR REPLACE TABLE sales (id INT NOT NULL)")
				.expect("plan create or replace"),
			None,
		)
		.await
		.expect("create or replace absent");

	assert!(results.is_empty());
	let binding = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("created table is visible");
	assert_field(&binding.schema, 0, "id", &DataType::Int32, false);
}

#[tokio::test]
async fn create_or_replace_table_replaces_existing_schema() {
	let (catalog, _storage, coordinator, planner) = storage_coordinator();

	for sql in [
		"CREATE TABLE sales (id INT NOT NULL)",
		"CREATE OR REPLACE TABLE sales (amount BIGINT, note TEXT)",
	] {
		coordinator
			.execute_query(&planner.plan(sql).expect("plan SQL"), None)
			.await
			.expect(sql);
	}

	let binding = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("replacement binding");
	assert_eq!(binding.schema.fields().len(), 2);
	assert_field(&binding.schema, 0, "amount", &DataType::Int64, true);
	assert_field(&binding.schema, 1, "note", &DataType::Utf8, true);
}

#[tokio::test]
async fn create_or_replace_table_produces_fresh_object_id() {
	let (catalog, _storage, coordinator, planner) = storage_coordinator();

	coordinator
		.execute_query(
			&planner
				.plan("CREATE TABLE sales (id INT NOT NULL)")
				.expect("plan create"),
			None,
		)
		.await
		.expect("create table");
	let before = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("resolve before replace");

	coordinator
		.execute_query(
			&planner
				.plan("CREATE OR REPLACE TABLE sales (id INT NOT NULL)")
				.expect("plan replace"),
			None,
		)
		.await
		.expect("replace table");
	let after = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("resolve after replace");

	assert_ne!(before.legacy_table_id, after.legacy_table_id);
	assert_ne!(before.table, after.table);
	assert_ne!(before.fingerprint, after.fingerprint);
}

#[tokio::test]
async fn distributed_query_coordinator_intercepts_create_table_before_dispatch() {
	let catalog = Arc::new(MockCatalog::new());
	let coordinator = DistributedQueryCoordinator::new(
		catalog.clone(),
		Arc::new(StaticWorkerRegistry::new([])),
		Arc::new(MemoryManager::new(1, 1024 * 1024)),
		Arc::new(FailingTransport),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE app.distributed_sales (id INT)")
		.expect("plan create table");

	let results = coordinator
		.execute_query(&plan)
		.await
		.expect("distributed create table");

	assert!(results.is_empty());
	catalog
		.resolve_table(&TableRef::with_schema("app", "distributed_sales"))
		.await
		.expect("created table is visible in catalog");
}

#[tokio::test]
async fn executor_rejects_direct_create_table_execution() {
	let executor = Executor::with_context(
		empty_storage(),
		ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(1),
			TenantId(1),
			HlcBound(1),
		),
	);
	let plan = ExecutionPlan::CreateTable {
		name: TableRef::new("sales"),
		schema: schema(&[Field::new("id", DataType::Int32, false)]),
		row_identity: CreateTableRowIdentity::AppendOnly,
		if_not_exists: false,
		or_replace: false,
	};

	let err = executor
		.execute_bound(&plan, None)
		.await
		.expect_err("executor should reject DDL");

	assert!(matches!(
		err,
		ExecutionError::NotImplemented {
			feature: "CREATE TABLE execution is coordinator-only"
		}
	));
}

#[test]
fn fragmenter_rejects_direct_create_table_fragmentation() {
	let plan = ExecutionPlan::CreateTable {
		name: TableRef::new("sales"),
		schema: schema(&[Field::new("id", DataType::Int32, false)]),
		row_identity: CreateTableRowIdentity::AppendOnly,
		if_not_exists: false,
		or_replace: false,
	};

	let err = fragment_plan(
		plan,
		FragmentPlanConfig::new(
			QueryId::from_u128(1),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect_err("fragmenter should reject DDL");

	assert_eq!(err, FragmentError::CreateTableNotFragmentable);
}

fn assert_frontend_error(sql: &str, diagnostic: &str) {
	let err = SqlPlanner::new().plan(sql).expect_err("SQL should reject");
	assert!(
		err.to_string().contains(diagnostic),
		"expected diagnostic {diagnostic:?}, got {err}"
	);
}

fn expect_catalog_err(result: Result<TableBinding, CatalogError>) -> CatalogError {
	match result {
		Ok(_) => panic!("expected catalog error"),
		Err(error) => error,
	}
}

fn assert_field(schema: &SchemaRef, idx: usize, name: &str, data_type: &DataType, nullable: bool) {
	let field = schema.field(idx);
	assert_eq!(field.name(), name);
	assert_eq!(field.data_type(), data_type);
	assert_eq!(field.is_nullable(), nullable);
}

fn assert_create_column(
	schema: &CreateTableSchema,
	idx: usize,
	name: &str,
	data_type: &DataType,
	nullable: bool,
) {
	let column = &schema.columns[idx];
	assert_eq!(column.name, name);
	assert_eq!(column.nullable, nullable);
	assert_eq!(
		column.column_type,
		CreateTableColumnType::Physical(data_type.clone())
	);
}

fn assert_point_read_scan(plan: &ExecutionPlan) {
	match plan {
		ExecutionPlan::Project { input, .. } | ExecutionPlan::Filter { input, .. } => {
			assert_point_read_scan(input)
		}
		ExecutionPlan::Scan {
			profile_hint,
			primary_key_columns,
			..
		} => {
			assert_eq!(*profile_hint, QueryProfileHint::PointRead);
			assert!(primary_key_columns.is_empty());
		}
		other => panic!("expected point-read scan under plan, got {other:?}"),
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

fn storage_coordinator() -> (
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

fn binding_for(table_id: TableId, name: &TableRef, schema: &Schema) -> TableBinding {
	let descriptor = mock_descriptor(name, schema, table_object_id(table_id));
	TableBinding::new(
		table_id,
		descriptor.table_object_id,
		descriptor.physical_descriptor_fingerprint,
		Arc::new(schema.clone()),
	)
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

struct FailingTransport;

#[async_trait]
impl ToronTransport for FailingTransport {
	async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
		Err(QueryError::Internal {
			message: "transport should not be used for CREATE TABLE".to_string(),
		})
	}
}
