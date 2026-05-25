use std::sync::{Arc, Mutex};

use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{Catalog, MockCatalog, TableRef, WendelonDescriptorRegistry};
use toron::coordinator::QueryCoordinator;
use toron::error::{BindingError, CatalogError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	BoundExprNode, BoundExprProgram, CreateTableColumnType, DURABLE_LOGICAL_TYPE_MIN_ID,
	ExecutionPlan, LOGICAL_TYPE_ID_METADATA_KEY, LogicalPlan, LogicalTypeId,
	decode_column_logical_type,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	CreateLogicalTypeOutcome, CreateLogicalTypeRequest, DropTableStorageOutcome,
	DropTableStorageRequest, InMemoryStorageScan, InMemoryTableStorage, StorageAdmin, TenantId,
};
use wendelon::integration::compute::LogicalTypeCatalogV1;
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptor,
};
use wendelon_client::{LogicalTypeDefinitionV1, LogicalTypePhysicalV1};

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
async fn try_cast_widening_logical_type_binds_nullable() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();

	for sql in [
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
		"CREATE TABLE raw_orders (raw_customer_id INT NOT NULL)",
		"INSERT INTO raw_orders VALUES (7)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let bound = bind(
		&planner
			.plan("SELECT TRY_CAST(raw_customer_id AS CustomerId) AS customer_id FROM raw_orders")
			.expect("plan widening logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("widening logical TRY_CAST binds");
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};
	assert_try_cast_root(
		&exprs,
		DataType::Int64,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
	assert!(output_schema.field(0).is_nullable());
	assert_eq!(
		decode_column_logical_type(output_schema.field(0)).expect("decode projection identity"),
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID)
	);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT TRY_CAST(raw_customer_id AS CustomerId) AS customer_id FROM raw_orders",
	)
	.await;
	assert_eq!(int64_rows(&batch, 0), vec![Some(7)]);
}

#[tokio::test]
async fn try_cast_narrowing_failure_yields_null_keeps_identity() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();

	for sql in [
		"CREATE LOGICAL TYPE SmallCustomerId AS INT",
		"CREATE TABLE raw_orders (raw_customer_id BIGINT NOT NULL)",
		"INSERT INTO raw_orders VALUES (1)",
		"INSERT INTO raw_orders VALUES (3000000000)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let bound = bind(
		&planner
			.plan(
				"SELECT TRY_CAST(raw_customer_id AS SmallCustomerId) AS small_customer_id FROM raw_orders",
			)
			.expect("plan narrowing logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("narrowing logical TRY_CAST binds");
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	assert_try_cast_root(
		&exprs,
		DataType::Int32,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT raw_customer_id, TRY_CAST(raw_customer_id AS SmallCustomerId) AS small_customer_id FROM raw_orders ORDER BY raw_customer_id",
	)
	.await;
	let field = batch.schema().field(1).clone();
	assert!(field.is_nullable());
	assert_eq!(
		decode_column_logical_type(&field).expect("decode projection identity"),
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID)
	);
	assert_eq!(int32_rows(&batch, 1), vec![Some(1), None]);
}

#[tokio::test]
async fn try_cast_same_physical_numeric_binds_nullable() {
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
		"CREATE TABLE raw_orders (raw_customer_id BIGINT NOT NULL)",
	)
	.await;

	let bound = bind(
		&planner
			.plan("SELECT TRY_CAST(raw_customer_id AS CustomerId) AS customer_id FROM raw_orders")
			.expect("plan same physical numeric logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("same physical numeric logical TRY_CAST binds");
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};
	assert_try_cast_root(
		&exprs,
		DataType::Int64,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
	assert!(output_schema.field(0).is_nullable());
}

#[tokio::test]
async fn try_cast_same_physical_utf8_binds_nullable_without_new_kernel() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();

	for sql in [
		"CREATE LOGICAL TYPE Sku AS TEXT",
		"CREATE TABLE raw_items (sku TEXT NOT NULL)",
		"INSERT INTO raw_items VALUES ('A-1')",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let bound = bind(
		&planner
			.plan("SELECT TRY_CAST(sku AS Sku) AS sku FROM raw_items")
			.expect("plan same physical Utf8 logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("same physical Utf8 logical TRY_CAST binds");
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};
	assert_try_cast_root(
		&exprs,
		DataType::Utf8,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
	assert!(output_schema.field(0).is_nullable());

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT TRY_CAST(sku AS Sku) FROM raw_items",
	)
	.await;
	assert_eq!(string_rows(&batch, 0), vec![Some("A-1".to_string())]);
}

#[tokio::test]
async fn try_cast_same_physical_boolean_binds_nullable() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();

	for sql in [
		"CREATE LOGICAL TYPE FeatureFlag AS BOOLEAN",
		"CREATE TABLE raw_flags (enabled BOOLEAN NOT NULL)",
		"INSERT INTO raw_flags VALUES (true)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let bound = bind(
		&planner
			.plan("SELECT TRY_CAST(enabled AS FeatureFlag) AS enabled FROM raw_flags")
			.expect("plan same physical boolean logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("same physical boolean logical TRY_CAST binds");
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};
	assert_try_cast_root(
		&exprs,
		DataType::Boolean,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
	assert!(output_schema.field(0).is_nullable());

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT TRY_CAST(enabled AS FeatureFlag) FROM raw_flags",
	)
	.await;
	assert_eq!(bool_rows(&batch, 0), vec![Some(true)]);
}

#[tokio::test]
async fn try_cast_conflicting_identity_rejects_at_bind() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
		"CREATE LOGICAL TYPE ProductId AS BIGINT",
		"CREATE TABLE orders (product_id ProductId)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let err = bind(
		&planner
			.plan("SELECT TRY_CAST(product_id AS CustomerId) FROM orders")
			.expect("plan conflicting logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("conflicting logical identities reject before TRY_CAST");
	assert!(matches!(
		err,
		BindingError::NominalTypeMismatch { physical, .. } if physical == "Int64"
	));
}

#[tokio::test]
async fn try_cast_explicit_primitive_erase_then_relabel_binds() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
		"CREATE LOGICAL TYPE ProductId AS BIGINT",
		"CREATE TABLE orders (product_id ProductId)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let bound = bind(
		&planner
			.plan("SELECT TRY_CAST(CAST(product_id AS BIGINT) AS CustomerId) FROM orders")
			.expect("plan erase then logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("explicit primitive erase permits logical TRY_CAST relabel");
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	assert_try_cast_root(
		&exprs,
		DataType::Int64,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
}

#[tokio::test]
async fn try_cast_undefined_fails_closed() {
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
	.expect_err("undefined logical TRY_CAST target must fail closed");
	assert!(matches!(
		err,
		BindingError::Catalog(CatalogError::LogicalTypeNotFound { ref name })
			if name == "CustomerId"
	));
}

#[tokio::test]
async fn try_cast_unsupported_cross_physical_rejects() {
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
			.plan("SELECT TRY_CAST(raw_customer_id AS CustomerId) FROM raw_orders")
			.expect("plan unsupported logical TRY_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("Utf8 to Int64 logical TRY_CAST must reject");
	assert!(
		matches!(err, BindingError::TypeMismatch { ref message } if message.contains("unsupported TRY_CAST: Utf8 to logical type CustomerId(Int64)")),
		"unexpected error: {err:?}"
	);
}

#[tokio::test]
async fn safe_cast_logical_type_aliases_try_cast() {
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
		"CREATE TABLE raw_orders (raw_customer_id INT NOT NULL)",
	)
	.await;

	let bound = bind(
		&planner
			.plan("SELECT SAFE_CAST(raw_customer_id AS CustomerId) AS customer_id FROM raw_orders")
			.expect("plan logical SAFE_CAST"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("logical SAFE_CAST binds as TRY_CAST");
	let ExecutionPlan::Project { exprs, .. } = bound else {
		panic!("expected project");
	};
	assert_try_cast_root(
		&exprs,
		DataType::Int64,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
}

#[tokio::test]
async fn logical_cast_ctas_materializes_durable_identity() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();

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

#[tokio::test]
async fn ctas_try_cast_materializes_nullable_logical_identity_and_survives_resync() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();

	for sql in [
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
		"CREATE TABLE raw_orders (raw_customer_id BIGINT NOT NULL)",
		"INSERT INTO raw_orders VALUES (30)",
		"CREATE TABLE typed_orders AS SELECT TRY_CAST(raw_customer_id AS CustomerId) AS customer_id FROM raw_orders",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let binding = catalog
		.resolve_table(&TableRef::new("typed_orders"))
		.await
		.expect("CTAS target resolves");
	let field = binding.schema.field(0);
	assert_eq!(field.name(), "customer_id");
	assert_eq!(field.data_type(), &DataType::Int64);
	assert!(field.is_nullable());
	assert_eq!(
		decode_column_logical_type(field).expect("decode CTAS identity"),
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID)
	);

	let resynced = WendelonDescriptorRegistry::default();
	resynced
		.register_logical_type_for_tenant(
			TenantId(1),
			LogicalTypeDefinitionV1 {
				logical_type_id: DURABLE_LOGICAL_TYPE_MIN_ID,
				name: "CustomerId".to_string(),
				physical: LogicalTypePhysicalV1::Int64,
			},
		)
		.expect("sync logical type definition");
	resynced
		.register_descriptor_for_tenant(
			TenantId(1),
			binding.legacy_table_id,
			descriptor_from_schema("typed_orders", binding.schema.as_ref()),
		)
		.expect("resynced descriptor resolves logical id");
	let resynced_binding = resynced
		.resolve_table(TenantId(1), &TableRef::new("typed_orders"))
		.expect("resolve resynced CTAS descriptor");
	let resynced_field = resynced_binding.schema.field(0);
	assert!(resynced_field.is_nullable());
	assert_eq!(
		decode_column_logical_type(resynced_field).expect("decode resynced CTAS identity"),
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

fn logical_type_writer_coordinator() -> (Arc<MockCatalog>, QueryCoordinator, SqlPlanner) {
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
	(catalog, coordinator, SqlPlanner::new())
}

async fn execute(coordinator: &QueryCoordinator, planner: &SqlPlanner, sql: &str) {
	coordinator
		.execute_query(&planner.plan(sql).expect("plan SQL"), None)
		.await
		.expect(sql);
}

async fn concat_query(
	coordinator: &QueryCoordinator,
	planner: &SqlPlanner,
	sql: &str,
) -> RecordBatch {
	let results = coordinator
		.execute_query(&planner.plan(sql).expect("plan query"), None)
		.await
		.expect(sql);
	let schema = results[0].schema();
	arrow::compute::concat_batches(&schema, results.iter()).expect("concat query results")
}

fn assert_try_cast_root(
	exprs: &BoundExprProgram,
	expected_type: DataType,
	expected_logical_type: LogicalTypeId,
) {
	let root = exprs.single_root().expect("single projection root");
	match exprs.node(root) {
		Some(BoundExprNode::TryCast { target, .. }) => {
			assert_eq!(target, &expected_type);
		}
		other => panic!("expected TryCast root, got {other:?}"),
	}
	assert_eq!(exprs.root_type(0), Some(&expected_type));
	assert_eq!(exprs.root_nullable(0), Some(true));
	assert_eq!(exprs.root_logical_type(0), Some(expected_logical_type));
}

fn int32_rows(batch: &RecordBatch, column: usize) -> Vec<Option<i32>> {
	batch
		.column(column)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("Int32 column")
		.iter()
		.collect()
}

fn int64_rows(batch: &RecordBatch, column: usize) -> Vec<Option<i64>> {
	batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("Int64 column")
		.iter()
		.collect()
}

fn string_rows(batch: &RecordBatch, column: usize) -> Vec<Option<String>> {
	batch
		.column(column)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("Utf8 column")
		.iter()
		.map(|value| value.map(str::to_string))
		.collect()
}

fn bool_rows(batch: &RecordBatch, column: usize) -> Vec<Option<bool>> {
	batch
		.column(column)
		.as_any()
		.downcast_ref::<BooleanArray>()
		.expect("Boolean column")
		.iter()
		.collect()
}

fn descriptor_from_schema(table: &str, schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([9; 16]),
		1,
		"default",
		table,
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
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
