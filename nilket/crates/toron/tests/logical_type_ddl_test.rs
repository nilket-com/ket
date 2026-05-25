use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{Catalog, MockCatalog, TableRef};
use toron::coordinator::QueryCoordinator;
use toron::error::{BindingError, CatalogError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	BoundExprNode, CreateTableColumnType, DURABLE_LOGICAL_TYPE_MIN_ID, ExecutionPlan,
	LOGICAL_TYPE_ID_METADATA_KEY, LogicalPlan, LogicalTypeId, decode_column_logical_type,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	CreateLogicalTypeOutcome, CreateLogicalTypeRequest, DropTableStorageOutcome,
	DropTableStorageRequest, InMemoryStorageScan, InMemoryTableStorage, StorageAdmin,
};
use wendelon::integration::compute::LogicalTypeCatalogV1;

#[tokio::test]
async fn ddl_define_then_use_creates_durable_typed_descriptor() {
	let (catalog, coordinator, planner) = logical_type_coordinator();

	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE orders (customer_id CustomerId NOT NULL, note TEXT)",
	)
	.await;

	let binding = catalog
		.resolve_table(&TableRef::new("orders"))
		.await
		.expect("created table resolves");
	let field = binding.schema.field(0);
	assert_eq!(field.name(), "customer_id");
	assert_eq!(field.data_type(), &DataType::Int64);
	assert!(!field.is_nullable());
	assert_eq!(
		field.metadata()[LOGICAL_TYPE_ID_METADATA_KEY],
		DURABLE_LOGICAL_TYPE_MIN_ID.to_string()
	);
	assert_eq!(
		decode_column_logical_type(field).expect("decode durable logical id"),
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID)
	);
	assert!(
		!binding
			.schema
			.field(1)
			.metadata()
			.contains_key(LOGICAL_TYPE_ID_METADATA_KEY)
	);
}

#[tokio::test]
async fn ddl_static_seed_does_not_satisfy_sql_name() {
	let catalog = MockCatalog::new();
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE orders (customer_id CustomerId)")
		.expect("planner records logical type name without catalog lookup");

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("seed CustomerId must not satisfy SQL DDL");

	assert!(matches!(
		err,
		BindingError::Catalog(CatalogError::LogicalTypeNotFound { ref name })
			if name == "CustomerId"
	));
}

#[tokio::test]
async fn ddl_two_logical_types_then_bare_compare_rejects() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE ProductId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE orders (customer_id CustomerId, product_id ProductId)",
	)
	.await;

	let err = bind(
		&planner
			.plan("SELECT * FROM orders WHERE customer_id = product_id")
			.expect("plan comparison"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("different durable logical identities reject");

	assert!(matches!(
		err,
		BindingError::NominalTypeMismatch { physical, .. } if physical == "Int64"
	));
}

#[tokio::test]
async fn ddl_literal_policy_allows_naked_literal_against_logical_column() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE orders (customer_id CustomerId)",
	)
	.await;

	bind(
		&planner
			.plan("SELECT * FROM orders WHERE customer_id = 7")
			.expect("plan literal comparison"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("naked literal can compare against logical column");
}

#[tokio::test]
async fn ddl_primitive_column_stays_naked() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE orders (customer_id BIGINT)",
	)
	.await;

	let binding = catalog
		.resolve_table(&TableRef::new("orders"))
		.await
		.expect("created table resolves");
	assert!(
		!binding
			.schema
			.field(0)
			.metadata()
			.contains_key(LOGICAL_TYPE_ID_METADATA_KEY)
	);
}

#[test]
fn ddl_planner_records_custom_type_name_without_catalog_resolution() {
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE orders (customer_id CustomerId NOT NULL)")
		.expect("planner records custom type name");
	let toron::ir::LogicalPlan::CreateTable { schema, .. } = plan else {
		panic!("expected create table");
	};

	let column = &schema.columns[0];
	assert_eq!(column.name, "customer_id");
	assert!(!column.nullable);
	assert_eq!(
		column.column_type,
		CreateTableColumnType::LogicalName("CustomerId".to_string())
	);
}

#[test]
fn ddl_rejects_bounded_invalid_logical_type_name_shapes() {
	for (sql, diagnostic) in [
		("CREATE TABLE orders (customer_id naked)", "naked sentinel"),
		(
			r#"CREATE TABLE orders (customer_id "CustomerId")"#,
			"unquoted ASCII identifiers",
		),
		(
			"CREATE TABLE orders (customer_id app.CustomerId)",
			"one-part names",
		),
		(
			"CREATE TABLE orders (customer_id CustomerId(1))",
			"do not support modifiers",
		),
	] {
		let err = SqlPlanner::new().plan(sql).expect_err(sql);
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}
}

#[test]
fn logical_cast_planner_records_custom_target_without_catalog_resolution() {
	let plan = SqlPlanner::new()
		.plan("SELECT CAST(raw_customer_id AS CustomerId) AS customer_id FROM raw_orders")
		.expect("planner records custom CAST target");
	let LogicalPlan::Project { exprs, .. } = plan else {
		panic!("expected project");
	};
	let toron::ir::Expr::SqlLogicalCast {
		logical_name,
		try_cast,
		..
	} = &exprs[0]
	else {
		panic!("expected transient SQL logical cast");
	};
	assert_eq!(logical_name, "CustomerId");
	assert!(!try_cast);
}

#[test]
fn logical_cast_planner_rejects_bounded_invalid_logical_type_name_shapes() {
	for (sql, diagnostic) in [
		(
			"SELECT CAST(raw_customer_id AS naked) FROM raw_orders",
			"naked sentinel",
		),
		(
			r#"SELECT CAST(raw_customer_id AS "CustomerId") FROM raw_orders"#,
			"unquoted ASCII identifiers",
		),
		(
			"SELECT CAST(raw_customer_id AS app.CustomerId) FROM raw_orders",
			"one-part names",
		),
		(
			"SELECT CAST(raw_customer_id AS CustomerId(1)) FROM raw_orders",
			"do not support modifiers",
		),
	] {
		let err = SqlPlanner::new().plan(sql).expect_err(sql);
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}
}

#[tokio::test]
async fn logical_cast_static_seed_does_not_satisfy_sql_name() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE raw_orders (raw_customer_id BIGINT)",
	)
	.await;

	let err = bind(
		&planner
			.plan("SELECT CAST(raw_customer_id AS CustomerId) FROM raw_orders")
			.expect("plan logical CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("static CustomerId seed must not satisfy SQL logical CAST");

	assert!(matches!(
		err,
		BindingError::Catalog(CatalogError::LogicalTypeNotFound { ref name })
			if name == "CustomerId"
	));
}

#[tokio::test]
async fn logical_cast_defined_type_stamps_projection_identity() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE raw_orders (raw_customer_id BIGINT)",
	)
	.await;

	let bound = bind(
		&planner
			.plan("SELECT CAST(raw_customer_id AS CustomerId) AS customer_id FROM raw_orders")
			.expect("plan logical CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("defined SQL logical CAST binds");
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};

	assert_eq!(
		exprs.root_logical_type(0),
		Some(LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID))
	);
	assert_eq!(
		decode_column_logical_type(output_schema.field(0)).expect("decode projection identity"),
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID)
	);
}

#[tokio::test]
async fn logical_cast_restates_same_identity_and_conflict_rejects() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE ProductId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE orders (customer_id CustomerId, product_id ProductId)",
	)
	.await;

	bind(
		&planner
			.plan("SELECT CAST(customer_id AS CustomerId) FROM orders")
			.expect("plan same identity restate"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("same logical identity restates");

	let err = bind(
		&planner
			.plan("SELECT CAST(product_id AS CustomerId) FROM orders")
			.expect("plan conflicting identity cast"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("conflicting logical identities reject");
	assert!(matches!(
		err,
		BindingError::NominalTypeMismatch { physical, .. } if physical == "Int64"
	));
}

#[tokio::test]
async fn logical_cast_explicit_primitive_erase_allows_relabel() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE ProductId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE orders (product_id ProductId)",
	)
	.await;

	let bound = bind(
		&planner
			.plan("SELECT CAST(CAST(product_id AS BIGINT) AS CustomerId) FROM orders")
			.expect("plan erase then relabel"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("explicit primitive erase permits relabel");
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	assert_eq!(
		exprs.root_logical_type(0),
		Some(LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID))
	);
}

#[tokio::test]
async fn logical_cast_numeric_physical_coercion_reuses_cast_kernel() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE raw_orders (raw_customer_id INT)",
	)
	.await;

	let bound = bind(
		&planner
			.plan("SELECT CAST(raw_customer_id AS CustomerId) FROM raw_orders")
			.expect("plan numeric logical CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("numeric physical coercion binds");
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	let root = exprs.single_root().expect("single projection root");
	assert!(matches!(exprs.node(root), Some(BoundExprNode::Cast { .. })));
	assert_eq!(exprs.root_type(0), Some(&DataType::Int64));
	assert_eq!(
		exprs.root_logical_type(0),
		Some(LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID))
	);
}

#[tokio::test]
async fn logical_cast_same_physical_utf8_ascribes_without_cast_kernel() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(&coordinator, &planner, "CREATE LOGICAL TYPE Sku AS TEXT").await;
	execute(&coordinator, &planner, "CREATE TABLE raw_items (sku TEXT)").await;

	let bound = bind(
		&planner
			.plan("SELECT CAST(sku AS Sku) FROM raw_items")
			.expect("plan Utf8 logical CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("same physical Utf8 logical CAST binds");
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	let root = exprs.single_root().expect("single projection root");
	assert!(!matches!(
		exprs.node(root),
		Some(BoundExprNode::Cast { .. } | BoundExprNode::TryCast { .. })
	));
	assert_eq!(exprs.root_type(0), Some(&DataType::Utf8));
	assert_eq!(
		exprs.root_logical_type(0),
		Some(LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID))
	);
}

#[tokio::test]
async fn logical_cast_unsupported_cross_physical_rejects() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE raw_orders (raw_customer_id TEXT)",
	)
	.await;

	let err = bind(
		&planner
			.plan("SELECT CAST(raw_customer_id AS CustomerId) FROM raw_orders")
			.expect("plan unsupported logical CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("Utf8 to Int64 logical CAST must reject");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("unsupported CAST: Utf8 to logical type CustomerId(Int64)")),
		"unexpected error: {err:?}"
	);
}

#[tokio::test]
async fn logical_cast_try_cast_to_logical_target_rejects() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE raw_orders (raw_customer_id BIGINT)",
	)
	.await;

	let err = bind(
		&planner
			.plan("SELECT TRY_CAST(raw_customer_id AS CustomerId) FROM raw_orders")
			.expect("planner records logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("logical TRY_CAST must reject in bind");
	assert!(
		matches!(err, BindingError::UnsupportedExpression { ref message } if message.contains("TRY_CAST/SAFE_CAST to logical type is not supported")),
		"unexpected error: {err:?}"
	);
}

#[tokio::test]
async fn logical_cast_ctas_materializes_durable_identity() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let admin = Arc::new(RegisteringLogicalTypeAdmin::new(catalog.clone()));
	let coordinator = QueryCoordinator::with_writer(
		catalog.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage,
	)
	.with_storage_admin(admin);
	let planner = SqlPlanner::new();

	for sql in [
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
		"CREATE TABLE raw_orders (raw_customer_id BIGINT)",
		"INSERT INTO raw_orders VALUES (30)",
		"CREATE TABLE typed_orders AS SELECT CAST(raw_customer_id AS CustomerId) AS customer_id FROM raw_orders",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let binding = catalog
		.resolve_table(&TableRef::new("typed_orders"))
		.await
		.expect("CTAS target resolves");
	assert_eq!(binding.schema.field(0).name(), "customer_id");
	assert_eq!(binding.schema.field(0).data_type(), &DataType::Int64);
	assert_eq!(
		decode_column_logical_type(binding.schema.field(0)).expect("decode CTAS identity"),
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID)
	);
}

fn logical_type_coordinator() -> (Arc<MockCatalog>, QueryCoordinator, SqlPlanner) {
	let catalog = Arc::new(MockCatalog::new());
	let storage = empty_storage();
	let admin = Arc::new(RegisteringLogicalTypeAdmin::new(catalog.clone()));
	let coordinator = QueryCoordinator::new(
		catalog.clone(),
		Executor::new(storage),
		PlannerConfig::default(),
	)
	.with_storage_admin(admin);
	(catalog, coordinator, SqlPlanner::new())
}

async fn execute(coordinator: &QueryCoordinator, planner: &SqlPlanner, sql: &str) {
	coordinator
		.execute_query(&planner.plan(sql).expect("plan SQL"), None)
		.await
		.expect(sql);
}

struct RegisteringLogicalTypeAdmin {
	catalog: Arc<MockCatalog>,
	definitions: Mutex<LogicalTypeCatalogV1>,
}

impl RegisteringLogicalTypeAdmin {
	fn new(catalog: Arc<MockCatalog>) -> Self {
		Self {
			catalog,
			definitions: Mutex::new(LogicalTypeCatalogV1::default()),
		}
	}
}

#[async_trait]
impl StorageAdmin for RegisteringLogicalTypeAdmin {
	async fn drop_table_storage(
		&self,
		_request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "drop table not used in logical type DDL tests".to_string(),
		})
	}

	async fn create_logical_type(
		&self,
		request: CreateLogicalTypeRequest,
	) -> Result<CreateLogicalTypeOutcome, StorageError> {
		let definition = self
			.definitions
			.lock()
			.expect("logical type catalog lock poisoned")
			.create(
				wendelon::schema::TenantId::new(request.tenant_id.0),
				wendelon::integration::compute::LogicalTypeDefinitionCreateV1 {
					name: request.name,
					physical: request.physical,
				},
			)
			.map_err(|error| StorageError::Internal {
				message: error.to_string(),
			})?;
		self.catalog
			.register_logical_type_for_tenant(request.tenant_id, definition.clone())
			.map_err(|error| StorageError::Internal {
				message: error.to_string(),
			})?;
		Ok(CreateLogicalTypeOutcome { definition })
	}
}

fn empty_storage() -> Arc<InMemoryStorageScan> {
	let schema = Arc::new(Schema::empty());
	let batch = RecordBatch::new_empty(schema);
	Arc::new(InMemoryStorageScan { table_data: batch })
}
