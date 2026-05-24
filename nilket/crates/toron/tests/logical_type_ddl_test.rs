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
	CreateTableColumnType, DURABLE_LOGICAL_TYPE_MIN_ID, LOGICAL_TYPE_ID_METADATA_KEY,
	LogicalTypeId, decode_column_logical_type,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	CreateLogicalTypeOutcome, CreateLogicalTypeRequest, DropTableStorageOutcome,
	DropTableStorageRequest, InMemoryStorageScan, StorageAdmin,
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
