use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef, WendelonDescriptorRegistry};
use toron::coordinator::QueryCoordinator;
use toron::error::StorageError;
use toron::exec::Executor;
use toron::ir::{
	CUSTOMER_ID_LOGICAL_TYPE, DURABLE_LOGICAL_TYPE_MIN_ID, ExecutionPlan,
	LOGICAL_TYPE_ID_METADATA_KEY, LogicalPlan, LogicalTypeId, TableId, decode_column_logical_type,
	field_with_logical_type_metadata,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	CreateLogicalTypeOutcome, CreateLogicalTypeRequest, DropTableStorageOutcome,
	DropTableStorageRequest, InMemoryStorageScan, StorageAdmin, TenantId,
};
use wendelon::integration::compute::LogicalTypeCatalog;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};
use wendelon_client::{LogicalTypeDefinition, LogicalTypePhysical};

#[test]
fn planner_accepts_create_logical_type_surface() {
	let plan = SqlPlanner::new()
		.plan("CREATE LOGICAL TYPE CustomerId AS BIGINT")
		.expect("plan create logical type");

	let LogicalPlan::CreateLogicalType {
		name,
		physical,
		refinement,
		unit,
	} = plan
	else {
		panic!("expected create logical type");
	};
	assert_eq!(name, "CustomerId");
	assert_eq!(physical, DataType::Int64);
	assert!(refinement.is_none());
	assert!(unit.is_none());
}

#[test]
fn planner_rejects_invalid_create_logical_type_shapes() {
	assert_frontend_error(
		"CREATE LOGICAL TYPE naked AS BIGINT",
		"cannot define the naked sentinel",
	);
	assert_frontend_error(
		"CREATE LOGICAL TYPE app.CustomerId AS BIGINT",
		"one-part tenant-global names",
	);
	assert_frontend_error(
		"CREATE LOGICAL TYPE CustomerId AS BLOB",
		"unsupported CREATE LOGICAL TYPE physical type",
	);
}

#[tokio::test]
async fn create_logical_type_executes_through_storage_admin() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = empty_storage();
	let admin = Arc::new(LogicalTypeAdmin::default());
	let coordinator =
		QueryCoordinator::new(catalog, Executor::new(storage), PlannerConfig::default())
			.with_storage_admin(admin.clone());
	let plan = SqlPlanner::new()
		.plan("CREATE LOGICAL TYPE PersonId AS INT64")
		.expect("plan create logical type");

	let batches = coordinator
		.execute_query(&plan, None)
		.await
		.expect("execute create logical type");

	assert!(batches.is_empty());
	let definition = admin
		.definition(TenantId(1), "PersonId")
		.expect("definition");
	assert_eq!(definition.logical_type_id, DURABLE_LOGICAL_TYPE_MIN_ID);
	assert_eq!(definition.name, "PersonId");
	assert_eq!(definition.physical, LogicalTypePhysical::Int64);
}

#[tokio::test]
async fn duplicate_definition_same_physical_is_idempotent_and_different_rejects() {
	let catalog = Arc::new(MockCatalog::new());
	let storage = empty_storage();
	let admin = Arc::new(LogicalTypeAdmin::default());
	let coordinator =
		QueryCoordinator::new(catalog, Executor::new(storage), PlannerConfig::default())
			.with_storage_admin(admin);
	let planner = SqlPlanner::new();

	coordinator
		.execute_query(
			&planner
				.plan("CREATE LOGICAL TYPE PersonId AS INT64")
				.expect("plan first"),
			None,
		)
		.await
		.expect("first create");
	coordinator
		.execute_query(
			&planner
				.plan("CREATE LOGICAL TYPE PersonId AS BIGINT")
				.expect("plan duplicate"),
			None,
		)
		.await
		.expect("same physical is idempotent");

	let err = coordinator
		.execute_query(
			&planner
				.plan("CREATE LOGICAL TYPE PersonId AS INT32")
				.expect("plan conflicting duplicate"),
			None,
		)
		.await
		.expect_err("different physical should reject");
	assert!(
		err.to_string().contains("different physical type"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn decode_resolves_durable_id_against_synced_registry() {
	let registry = WendelonDescriptorRegistry::default();
	registry
		.register_logical_type_for_tenant(TenantId(7), person_id_definition())
		.expect("register logical type");
	registry
		.register_descriptor_for_tenant(
			TenantId(7),
			TableId(11),
			descriptor_with_logical_id(LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID)),
		)
		.expect("descriptor resolves registered logical id");

	let binding = registry
		.resolve_table(TenantId(7), &TableRef::with_schema("default", "people"))
		.expect("resolve descriptor");
	assert_eq!(
		binding.schema.field(0).metadata()[LOGICAL_TYPE_ID_METADATA_KEY],
		DURABLE_LOGICAL_TYPE_MIN_ID.to_string()
	);
}

#[test]
fn logical_type_definition_survives_catalog_resync() {
	let mut durable_catalog = LogicalTypeCatalog::default();
	let tenant = wendelon::schema::TenantId::new(7);
	let definition = durable_catalog
		.create(
			tenant,
			wendelon::integration::compute::LogicalTypeDefinitionCreate {
				name: "PersonId".to_string(),
				physical: LogicalTypePhysical::Int64,
				refinement: None,
				unit: None,
			},
		)
		.expect("create durable definition");

	let resynced = WendelonDescriptorRegistry::default();
	for definition in durable_catalog.definitions_for_tenant(tenant) {
		resynced
			.register_logical_type_for_tenant(TenantId(7), definition)
			.expect("sync definition");
	}
	resynced
		.register_descriptor_for_tenant(
			TenantId(7),
			TableId(11),
			descriptor_with_logical_id(LogicalTypeId(definition.logical_type_id)),
		)
		.expect("resynced registry resolves descriptor logical id");
}

#[test]
fn descriptor_referencing_undefined_id_fails_closed() {
	let registry = WendelonDescriptorRegistry::default();
	let err = registry
		.register_descriptor_for_tenant(
			TenantId(7),
			TableId(11),
			descriptor_with_logical_id(LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID)),
		)
		.expect_err("undefined durable id must fail closed");

	assert!(
		err.to_string().contains("references unknown id 1025"),
		"unexpected error: {err}"
	);
}

#[test]
fn static_and_durable_ids_are_disjoint() {
	let durable_min = std::hint::black_box(DURABLE_LOGICAL_TYPE_MIN_ID);
	assert!(durable_min > 2);
}

#[test]
fn static_seed_still_resolves_for_rune_and_tests() {
	let field = field_with_logical_type_metadata(
		"customer_id",
		DataType::Int64,
		false,
		CUSTOMER_ID_LOGICAL_TYPE,
	);

	assert_eq!(
		decode_column_logical_type(&field).expect("decode static seed"),
		CUSTOMER_ID_LOGICAL_TYPE
	);
}

#[test]
fn sql_explicit_typed_ctas_column_list_still_rejects() {
	assert_frontend_error(
		"CREATE TABLE copied (customer_id CustomerId) AS SELECT customer_id FROM orders",
		"explicit column lists",
	);
}

#[tokio::test]
async fn binder_lowers_create_logical_type_without_engine_surface_changes() {
	let catalog = MockCatalog::new();
	let plan = SqlPlanner::new()
		.plan("CREATE LOGICAL TYPE PersonId AS INT64")
		.expect("plan create logical type");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind create logical type");

	let ExecutionPlan::CreateLogicalType {
		name,
		physical,
		refinement,
		unit,
	} = bound
	else {
		panic!("expected create logical type execution plan");
	};
	assert_eq!(name, "PersonId");
	assert_eq!(physical, DataType::Int64);
	assert!(refinement.is_none());
	assert!(unit.is_none());
}

#[derive(Default)]
struct LogicalTypeAdmin {
	catalog: Mutex<LogicalTypeCatalog>,
}

impl LogicalTypeAdmin {
	fn definition(&self, tenant_id: TenantId, name: &str) -> Option<LogicalTypeDefinition> {
		self.catalog
			.lock()
			.expect("logical type admin lock poisoned")
			.get_by_name(wendelon::schema::TenantId::new(tenant_id.0), name)
			.cloned()
	}
}

#[async_trait]
impl StorageAdmin for LogicalTypeAdmin {
	async fn drop_table_storage(
		&self,
		_request: DropTableStorageRequest,
	) -> Result<DropTableStorageOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "drop table not used in logical type tests".to_string(),
		})
	}

	async fn create_logical_type(
		&self,
		request: CreateLogicalTypeRequest,
	) -> Result<CreateLogicalTypeOutcome, StorageError> {
		let definition = self
			.catalog
			.lock()
			.expect("logical type admin lock poisoned")
			.create(
				wendelon::schema::TenantId::new(request.tenant_id.0),
				wendelon::integration::compute::LogicalTypeDefinitionCreate {
					name: request.name,
					physical: request.physical,
					refinement: request.refinement,
					unit: request.unit,
				},
			)
			.map_err(|error| StorageError::Internal {
				message: error.to_string(),
			})?;
		Ok(CreateLogicalTypeOutcome { definition })
	}
}

fn person_id_definition() -> LogicalTypeDefinition {
	LogicalTypeDefinition {
		logical_type_id: DURABLE_LOGICAL_TYPE_MIN_ID,
		name: "PersonId".to_string(),
		physical: LogicalTypePhysical::Int64,
		refinement: None,
		unit: None,
	}
}

fn descriptor_with_logical_id(logical_type: LogicalTypeId) -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		field_with_logical_type_metadata("person_id", DataType::Int64, false, logical_type),
		Field::new("name", DataType::Utf8, true),
	]);
	TableSchemaDescriptor::new(
		TableObjectId::new([7; 16]),
		1,
		"default",
		"people",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("person_id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn empty_storage() -> Arc<InMemoryStorageScan> {
	let schema = Arc::new(Schema::empty());
	let batch = arrow::record_batch::RecordBatch::new_empty(schema);
	Arc::new(InMemoryStorageScan { table_data: batch })
}

fn assert_frontend_error(sql: &str, diagnostic: &str) {
	let err = SqlPlanner::new().plan(sql).expect_err("SQL should reject");
	assert!(
		err.to_string().contains(diagnostic),
		"expected diagnostic {diagnostic:?}, got {err}"
	);
}
