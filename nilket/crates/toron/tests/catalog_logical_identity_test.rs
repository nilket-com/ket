use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{Catalog, MockCatalog, TableRef};
use toron::coordinator::QueryCoordinator;
use toron::error::{BindingError, CatalogError, QueryError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	AggregateFunction, CUSTOMER_ID_LOGICAL_TYPE, ExecutionPlan, Expr, JoinKind, JoinStrategy,
	LOGICAL_TYPE_ID_METADATA_KEY, LogicalPlan, LogicalTypeId, PRODUCT_ID_LOGICAL_TYPE, QueryId,
	ScalarValue, SessionId, TableId, decode_column_logical_type,
};
use toron::storage::{
	InMemoryTableStorage, ScanRequest, ScanStream, StorageScan, StorageWrite, TenantId,
	WriteRequest,
};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

#[tokio::test]
async fn catalog_logical_column_binds_and_executes_against_literal() {
	let logical = LogicalPlan::Filter {
		input: Box::new(scan("sales")),
		predicate: Expr::Eq(
			Box::new(Expr::column("customer_id")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
		),
	};
	let rows = execute_logical(&logical, sales_catalog(), sales_batch()).await;

	assert_eq!(int64_rows(&rows, 0), vec![Some(1)]);
}

#[tokio::test]
async fn catalog_logical_same_identity_comparison_binds() {
	bind(
		&filter(
			"sales",
			Expr::Eq(
				Box::new(Expr::column("customer_id")),
				Box::new(Expr::column("alt_customer_id")),
			),
		),
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("same catalog-carried identity comparison binds");
}

#[tokio::test]
async fn catalog_logical_different_identity_comparison_rejects() {
	let error = bind(
		&filter(
			"sales",
			Expr::Eq(
				Box::new(Expr::column("customer_id")),
				Box::new(Expr::column("product_id")),
			),
		),
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("different catalog-carried identities reject");

	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");
}

#[tokio::test]
async fn catalog_logical_vs_naked_non_literal_rejects() {
	let error = bind(
		&filter(
			"sales",
			Expr::Eq(
				Box::new(Expr::column("customer_id")),
				Box::new(Expr::column("raw_id")),
			),
		),
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("catalog identity vs naked non-literal rejects");

	assert_nominal_mismatch(error, "CustomerId", "naked", "Int64");
}

#[test]
fn catalog_logical_invalid_metadata_rejects_at_descriptor_binding() {
	for (schema, primary_key, expected) in [
		(
			Schema::new(vec![logical_field_with_raw(
				"id",
				DataType::Int64,
				"not-a-u32",
			)]),
			"id",
			"decimal u32",
		),
		(
			Schema::new(vec![logical_field_with_raw("id", DataType::Int64, "999")]),
			"id",
			"unknown id 999",
		),
		(
			Schema::new(vec![
				Field::new("pk", DataType::Int64, false),
				logical_field("id", DataType::Float64, CUSTOMER_ID_LOGICAL_TYPE),
			]),
			"pk",
			"declares CustomerId(Int64) on Float64",
		),
	] {
		let mut catalog = MockCatalog::new();
		let error = catalog
			.register_descriptor(TableId(9), descriptor(9, "bad", &schema, primary_key))
			.expect_err("invalid logical metadata rejects during descriptor binding");

		match error {
			CatalogError::FingerprintResolution { message } => {
				assert!(message.contains(expected), "{message}");
			}
			other => panic!("expected descriptor fingerprint resolution error, got {other:?}"),
		}
	}
}

#[tokio::test]
async fn catalog_logical_same_identity_join_binds() {
	bind(
		&join_plan(
			Expr::qualified_column(TableRef::new("left_t"), "join_id"),
			Expr::qualified_column(TableRef::new("right_t"), "join_id"),
		),
		&join_catalog(CUSTOMER_ID_LOGICAL_TYPE, CUSTOMER_ID_LOGICAL_TYPE),
		&PlannerConfig::default(),
	)
	.await
	.expect("same catalog-carried join key identity binds");
}

#[tokio::test]
async fn catalog_logical_different_identity_join_rejects() {
	let error = bind(
		&join_plan(
			Expr::qualified_column(TableRef::new("left_t"), "join_id"),
			Expr::qualified_column(TableRef::new("right_t"), "join_id"),
		),
		&join_catalog(CUSTOMER_ID_LOGICAL_TYPE, PRODUCT_ID_LOGICAL_TYPE),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("different catalog-carried join key identities reject");

	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");
}

#[tokio::test]
async fn catalog_logical_filter_above_join_sees_source_identities() {
	let joined = join_plan(
		Expr::qualified_column(TableRef::new("left_t"), "join_id"),
		Expr::qualified_column(TableRef::new("right_t"), "join_id"),
	);
	let logical = LogicalPlan::Filter {
		input: Box::new(joined),
		predicate: Expr::Eq(
			Box::new(Expr::column("customer_id")),
			Box::new(Expr::column("product_id")),
		),
	};
	let error = bind(
		&logical,
		&join_catalog(CUSTOMER_ID_LOGICAL_TYPE, CUSTOMER_ID_LOGICAL_TYPE),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("filter above join must see cloned source identities");

	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");
}

#[tokio::test]
async fn catalog_logical_explicit_restate_contradict_and_cast_reascribe() {
	bind(
		&filter(
			"sales",
			Expr::Eq(
				Box::new(ascribe(
					Expr::column("customer_id"),
					CUSTOMER_ID_LOGICAL_TYPE,
				)),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		),
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("explicit restatement of catalog identity binds");

	let error = bind(
		&filter(
			"sales",
			Expr::Eq(
				Box::new(ascribe(
					Expr::column("customer_id"),
					PRODUCT_ID_LOGICAL_TYPE,
				)),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		),
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("explicit contradiction of catalog identity rejects");
	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");

	bind(
		&filter(
			"sales",
			Expr::Eq(
				Box::new(ascribe(
					Expr::Cast(Box::new(Expr::column("customer_id")), DataType::Int64),
					PRODUCT_ID_LOGICAL_TYPE,
				)),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		),
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("explicit cast erases catalog identity before re-ascription");
}

#[tokio::test]
async fn projection_alias_preserves_identity() {
	let schema = bound_schema(project_sales(
		vec![Expr::column("customer_id")],
		vec![Some("cid")],
	))
	.await;

	assert_eq!(schema.field(0).name(), "cid");
	assert_field_logical_type(&schema, 0, CUSTOMER_ID_LOGICAL_TYPE);
}

#[tokio::test]
async fn projection_alias_same_identity_binds() {
	bind(
		&LogicalPlan::Filter {
			input: Box::new(project_sales(
				vec![Expr::column("customer_id"), Expr::column("alt_customer_id")],
				vec![Some("cid"), Some("aid")],
			)),
			predicate: Expr::Eq(Box::new(Expr::column("cid")), Box::new(Expr::column("aid"))),
		},
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("same identity aliases bind");
}

#[tokio::test]
async fn projection_alias_different_identity_rejects() {
	let error = bind(
		&LogicalPlan::Filter {
			input: Box::new(project_sales(
				vec![Expr::column("customer_id"), Expr::column("product_id")],
				vec![Some("cid"), Some("pid")],
			)),
			predicate: Expr::Eq(Box::new(Expr::column("cid")), Box::new(Expr::column("pid"))),
		},
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("different identity aliases reject");

	assert_nominal_mismatch(error, "CustomerId", "ProductId", "Int64");
}

#[tokio::test]
async fn computed_projection_stays_naked() {
	bind(
		&LogicalPlan::Filter {
			input: Box::new(project_sales(
				vec![
					add_zero(Expr::column("customer_id")),
					Expr::column("raw_id"),
				],
				vec![Some("cid_value"), None],
			)),
			predicate: Expr::Eq(
				Box::new(Expr::column("cid_value")),
				Box::new(Expr::column("raw_id")),
			),
		},
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("computed projection is naked and can compare to naked raw id");

	let schema = bound_schema(project_sales(
		vec![add_zero(Expr::column("customer_id"))],
		vec![Some("cid_value")],
	))
	.await;
	assert_field_logical_type(&schema, 0, LogicalTypeId::NAKED);
}

#[tokio::test]
async fn explicit_ascribed_computed_projection_carries_identity() {
	let schema = bound_schema(project_sales(
		vec![ascribe(
			add_zero(Expr::column("customer_id")),
			CUSTOMER_ID_LOGICAL_TYPE,
		)],
		vec![Some("cid_value")],
	))
	.await;
	assert_field_logical_type(&schema, 0, CUSTOMER_ID_LOGICAL_TYPE);

	bind(
		&LogicalPlan::Filter {
			input: Box::new(project_sales(
				vec![
					ascribe(
						add_zero(Expr::column("customer_id")),
						CUSTOMER_ID_LOGICAL_TYPE,
					),
					Expr::column("alt_customer_id"),
				],
				vec![Some("cid_value"), Some("aid")],
			)),
			predicate: Expr::Eq(
				Box::new(Expr::column("cid_value")),
				Box::new(Expr::column("aid")),
			),
		},
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("explicitly ascribed computed identity binds against same identity");
}

#[tokio::test]
async fn aggregate_group_key_preserves_identity() {
	let schema = bound_schema(LogicalPlan::Aggregate {
		input: Box::new(scan("sales")),
		group_expr: vec![Expr::column("customer_id"), Expr::column("raw_id")],
		aggr_expr: vec![(AggregateFunction::CountAll, vec![])],
	})
	.await;

	assert_field_logical_type(&schema, 0, CUSTOMER_ID_LOGICAL_TYPE);
	assert_field_logical_type(&schema, 1, LogicalTypeId::NAKED);

	let error = bind(
		&LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Aggregate {
				input: Box::new(scan("sales")),
				group_expr: vec![Expr::column("customer_id"), Expr::column("raw_id")],
				aggr_expr: vec![(AggregateFunction::CountAll, vec![])],
			}),
			predicate: Expr::Eq(
				Box::new(Expr::column("customer_id")),
				Box::new(Expr::column("raw_id")),
			),
		},
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("group key identity survives into post-aggregate filter");

	assert_nominal_mismatch(error, "CustomerId", "naked", "Int64");
}

#[tokio::test]
async fn aggregate_output_stays_naked() {
	let schema = bound_schema(LogicalPlan::Aggregate {
		input: Box::new(scan("sales")),
		group_expr: vec![],
		aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("customer_id")])],
	})
	.await;

	assert_field_logical_type(&schema, 0, LogicalTypeId::NAKED);
}

#[tokio::test]
async fn project_pruning_preserves_identity() {
	let schema = bound_schema(LogicalPlan::Project {
		input: Box::new(project_sales(
			vec![Expr::column("customer_id"), Expr::column("product_id")],
			vec![Some("cid"), Some("pid")],
		)),
		exprs: vec![Expr::column("cid")],
		output_names: vec![None],
	})
	.await;

	assert_eq!(schema.field(0).name(), "cid");
	assert_field_logical_type(&schema, 0, CUSTOMER_ID_LOGICAL_TYPE);
}

#[tokio::test]
async fn ctas_direct_projection_materializes_identity_in_descriptor() {
	let schema = bound_ctas_schema(project_sales(
		vec![Expr::column("customer_id")],
		vec![Some("cid")],
	))
	.await;

	assert_eq!(schema.field(0).name(), "cid");
	assert_field_logical_type(&schema, 0, CUSTOMER_ID_LOGICAL_TYPE);
}

#[tokio::test]
async fn ctas_computed_projection_stays_naked_in_descriptor() {
	let schema = bound_ctas_schema(project_sales(
		vec![add_zero(Expr::column("customer_id"))],
		vec![Some("cid_value")],
	))
	.await;

	assert_field_logical_type(&schema, 0, LogicalTypeId::NAKED);
}

#[tokio::test]
async fn ctas_if_not_exists_over_naked_target_rejects_schema_mismatch() {
	let catalog = Arc::new(sales_catalog());
	catalog
		.create_table(
			&TableRef::new("copy"),
			Arc::new(Schema::new(vec![Field::new("cid", DataType::Int64, false)])),
			false,
		)
		.await
		.expect("create naked target");
	let storage = Arc::new(InMemoryTableStorage::new());
	let coordinator = logical_identity_coordinator(catalog, storage);

	let error = coordinator
		.execute_query(&ctas_project_customer_id_as_cid(true), None)
		.await
		.expect_err("naked target is incompatible with identity-bearing CTAS schema");

	assert!(
		matches!(
			error,
			QueryError::Catalog(CatalogError::SchemaMismatch { .. })
		),
		"unexpected error: {error}"
	);
}

#[tokio::test]
async fn ctas_if_not_exists_identity_target_is_idempotent_on_reissue() {
	let (catalog, storage, coordinator) = seeded_logical_identity_coordinator().await;
	coordinator
		.execute_query(&ctas_project_customer_id_as_cid(true), None)
		.await
		.expect("first CTAS creates target");

	let results = coordinator
		.execute_query(&ctas_project_customer_id_as_cid(true), None)
		.await
		.expect("second identical CTAS is a no-op");

	assert!(results.is_empty());
	let binding = catalog
		.resolve_table(&TableRef::new("copy"))
		.await
		.expect("resolve CTAS target");
	assert_field_logical_type(&binding.schema, 0, CUSTOMER_ID_LOGICAL_TYPE);
	assert_eq!(storage.table_count(), 2);
}

#[tokio::test]
async fn ctas_created_logical_projection_rebinds_as_source_without_fail_closed() {
	let (catalog, _storage, coordinator) = seeded_logical_identity_coordinator().await;
	coordinator
		.execute_query(&ctas_project_customer_id_as_cid(false), None)
		.await
		.expect("CTAS creates identity-bearing target");

	bind(
		&filter(
			"copy",
			Expr::Eq(
				Box::new(Expr::column("cid")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("CTAS-created logical metadata rebinds cleanly");
}

#[tokio::test]
async fn projection_alias_round_trips_without_fail_closed() {
	bind(
		&LogicalPlan::Filter {
			input: Box::new(project_sales(
				vec![Expr::column("customer_id")],
				vec![Some("cid")],
			)),
			predicate: Expr::Eq(
				Box::new(Expr::column("cid")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		},
		&sales_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("alias-preserved identity decodes and binds downstream");
}

fn filter(table: &str, predicate: Expr) -> LogicalPlan {
	LogicalPlan::Filter {
		input: Box::new(scan(table)),
		predicate,
	}
}

fn scan(table: &str) -> LogicalPlan {
	LogicalPlan::ScanNamed {
		table: TableRef::new(table),
		projection: None,
	}
}

fn ascribe(expr: Expr, logical_type: LogicalTypeId) -> Expr {
	Expr::LogicalAscription {
		expr: Box::new(expr),
		logical_type,
	}
}

fn add_zero(expr: Expr) -> Expr {
	Expr::Add(
		Box::new(expr),
		Box::new(Expr::Literal(ScalarValue::Int64(Some(0)))),
	)
}

fn project_sales(exprs: Vec<Expr>, output_names: Vec<Option<&str>>) -> LogicalPlan {
	LogicalPlan::Project {
		input: Box::new(scan("sales")),
		exprs,
		output_names: output_names
			.into_iter()
			.map(|name| name.map(str::to_string))
			.collect(),
	}
}

fn ctas_project_customer_id_as_cid(if_not_exists: bool) -> LogicalPlan {
	LogicalPlan::CreateTableAsSelect {
		table: TableRef::new("copy"),
		if_not_exists,
		or_replace: false,
		source: Box::new(project_sales(
			vec![Expr::column("customer_id")],
			vec![Some("cid")],
		)),
	}
}

async fn bound_schema(logical: LogicalPlan) -> SchemaRef {
	bind(&logical, &sales_catalog(), &PlannerConfig::default())
		.await
		.expect("bind")
		.schema()
}

async fn bound_ctas_schema(source: LogicalPlan) -> SchemaRef {
	let plan = LogicalPlan::CreateTableAsSelect {
		table: TableRef::new("copy"),
		if_not_exists: false,
		or_replace: false,
		source: Box::new(source),
	};
	let bound = bind(&plan, &sales_catalog(), &PlannerConfig::default())
		.await
		.expect("bind CTAS");
	let ExecutionPlan::CreateTableAsSelect { schema, .. } = bound else {
		panic!("expected CTAS execution plan");
	};
	schema
}

async fn seeded_logical_identity_coordinator() -> (
	Arc<MockCatalog>,
	Arc<InMemoryTableStorage>,
	QueryCoordinator,
) {
	let catalog = Arc::new(sales_catalog());
	let storage = Arc::new(InMemoryTableStorage::new());
	seed_sales_storage(catalog.as_ref(), storage.as_ref()).await;
	let coordinator = logical_identity_coordinator(catalog.clone(), storage.clone());
	(catalog, storage, coordinator)
}

fn logical_identity_coordinator(
	catalog: Arc<MockCatalog>,
	storage: Arc<InMemoryTableStorage>,
) -> QueryCoordinator {
	QueryCoordinator::with_writer(
		catalog,
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(storage)
}

async fn seed_sales_storage(catalog: &MockCatalog, storage: &InMemoryTableStorage) {
	let binding = catalog
		.resolve_table(&TableRef::new("sales"))
		.await
		.expect("resolve sales");
	storage
		.write(WriteRequest {
			session_id: SessionId(1),
			query_id: QueryId::from_u128(1),
			tenant_id: TenantId(0),
			table_id: binding.legacy_table_id,
			table_object_id: binding.table,
			physical_descriptor_fingerprint: binding.fingerprint,
			payload: sales_batch(),
			profile_hint: None,
			trace_context: None,
		})
		.await
		.expect("seed sales storage");
}

fn join_plan(left_key: Expr, right_key: Expr) -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::Exchange {
			input: Box::new(scan("left_t")),
			partition_count: 2,
			key: vec![Expr::qualified_column(TableRef::new("left_t"), "join_id")],
		}),
		right: Box::new(LogicalPlan::Exchange {
			input: Box::new(scan("right_t")),
			partition_count: 2,
			key: vec![Expr::qualified_column(TableRef::new("right_t"), "join_id")],
		}),
		kind: JoinKind::Inner,
		on: vec![(left_key, right_key)],
		strategy: JoinStrategy::HashPartitioned,
	}
}

fn sales_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(1, "sales", sales_schema().as_ref(), "customer_id"),
		)
		.expect("register sales");
	catalog
}

fn join_catalog(left_join_id: LogicalTypeId, right_join_id: LogicalTypeId) -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			descriptor(
				1,
				"left_t",
				left_join_schema(left_join_id).as_ref(),
				"join_id",
			),
		)
		.expect("register left");
	catalog
		.register_descriptor(
			TableId(2),
			descriptor(
				2,
				"right_t",
				right_join_schema(right_join_id).as_ref(),
				"join_id",
			),
		)
		.expect("register right");
	catalog
}

fn sales_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		logical_field("customer_id", DataType::Int64, CUSTOMER_ID_LOGICAL_TYPE),
		logical_field("alt_customer_id", DataType::Int64, CUSTOMER_ID_LOGICAL_TYPE),
		logical_field("product_id", DataType::Int64, PRODUCT_ID_LOGICAL_TYPE),
		Field::new("raw_id", DataType::Int64, false),
		Field::new("region", DataType::Utf8, true),
	]))
}

fn left_join_schema(join_id: LogicalTypeId) -> SchemaRef {
	Arc::new(Schema::new(vec![
		logical_field("join_id", DataType::Int64, join_id),
		logical_field("customer_id", DataType::Int64, CUSTOMER_ID_LOGICAL_TYPE),
	]))
}

fn right_join_schema(join_id: LogicalTypeId) -> SchemaRef {
	Arc::new(Schema::new(vec![
		logical_field("join_id", DataType::Int64, join_id),
		logical_field("product_id", DataType::Int64, PRODUCT_ID_LOGICAL_TYPE),
	]))
}

fn logical_field(name: &str, data_type: DataType, logical_type: LogicalTypeId) -> Field {
	logical_field_with_raw(name, data_type, &logical_type.0.to_string())
}

fn logical_field_with_raw(name: &str, data_type: DataType, raw: &str) -> Field {
	Field::new(name, data_type, false).with_metadata(HashMap::from([(
		LOGICAL_TYPE_ID_METADATA_KEY.to_string(),
		raw.to_string(),
	)]))
}

fn assert_field_logical_type(schema: &SchemaRef, index: usize, expected: LogicalTypeId) {
	let actual = decode_column_logical_type(schema.field(index)).expect("decode logical metadata");
	assert_eq!(actual, expected);
}

fn sales_batch() -> RecordBatch {
	RecordBatch::try_new(
		sales_schema(),
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3])),
			Arc::new(Int64Array::from(vec![1, 2, 4])),
			Arc::new(Int64Array::from(vec![10, 20, 30])),
			Arc::new(Int64Array::from(vec![1, 2, 3])),
			Arc::new(StringArray::from(vec![
				Some("east"),
				Some("west"),
				Some("east"),
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

async fn execute_logical(
	logical: &LogicalPlan,
	catalog: MockCatalog,
	batch: RecordBatch,
) -> Vec<RecordBatch> {
	let bound = bind(logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	Executor::new(Arc::new(FixtureScan {
		batches: HashMap::from([(TableId(1), batch)]),
	}))
	.execute_bound(&bound, None)
	.await
	.expect("execute")
}

fn int64_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
	let array = batches[0]
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("int64 column");
	(0..array.len())
		.map(|row| {
			if array.is_null(row) {
				None
			} else {
				Some(array.value(row))
			}
		})
		.collect()
}

fn assert_nominal_mismatch(
	error: BindingError,
	expected_left: &str,
	expected_right: &str,
	expected_physical: &str,
) {
	match error {
		BindingError::NominalTypeMismatch {
			left,
			right,
			physical,
		} => {
			assert_eq!(left, expected_left);
			assert_eq!(right, expected_right);
			assert_eq!(physical, expected_physical);
		}
		other => panic!("expected nominal type mismatch, got {other:?}"),
	}
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
		if let Some(pbc) = request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
		if let Some(projected_columns) = request.projected_columns.as_ref() {
			let row_count = batch.num_rows();
			let schema = batch.schema();
			let fields = projected_columns
				.iter()
				.map(|ordinal| schema.field(ordinal.0 as usize).as_ref().clone())
				.collect::<Vec<_>>();
			let columns = projected_columns
				.iter()
				.map(|ordinal| batch.column(ordinal.0 as usize).clone())
				.collect::<Vec<_>>();
			let projected_schema = Arc::new(Schema::new(fields));
			batch = if columns.is_empty() {
				RecordBatch::try_new_with_options(
					projected_schema,
					columns,
					&RecordBatchOptions::new().with_row_count(Some(row_count)),
				)
				.expect("project fixture batch")
			} else {
				RecordBatch::try_new(projected_schema, columns).expect("project fixture batch")
			};
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}
