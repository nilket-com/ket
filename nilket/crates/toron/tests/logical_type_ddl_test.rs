use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{
	Catalog, CreateTableOutcome, DescriptorRefreshMetrics, MockCatalog, TableBinding, TableRef,
	WendelonDescriptorRegistry,
};
use toron::coordinator::QueryCoordinator;
use toron::error::{BindingError, CatalogError, StorageError};
use toron::exec::Executor;
use toron::ir::{
	BoundExprNode, BoundExprProgram, CUSTOMER_ID_LOGICAL_TYPE, CreateTableColumnType,
	DURABLE_LOGICAL_TYPE_MIN_ID, ExecutionPlan, Expr, LOGICAL_TYPE_ID_METADATA_KEY, LogicalPlan,
	LogicalTypeId, ScalarValue, TableId, decode_column_logical_type,
	field_with_logical_type_metadata,
};
use toron::sql_planner::SqlPlanner;
use toron::storage::{
	CreateLogicalTypeOutcome, CreateLogicalTypeRequest, DropTableStorageOutcome,
	DropTableStorageRequest, InMemoryStorageScan, InMemoryTableStorage, ScanRequest, ScanStream,
	StorageAdmin, StorageScan, TenantId,
};
use wendelon::integration::compute::LogicalTypeCatalog;
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, TableObjectId, TableSchemaCatalogV1, TableSchemaDescriptor,
	TenantId as WendelonTenantId,
};
use wendelon_client::{
	LogicalTypeDefinition, LogicalTypePhysical, LogicalTypeRefinementComparisonOp,
	LogicalTypeRefinementPredicate, LogicalTypeRefinementValue,
};

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

#[test]
fn ddl_create_logical_type_check_lowers_portable_refinement() {
	let plan = SqlPlanner::new()
		.plan("CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0 AND VALUE <= 100)")
		.expect("plan refined logical type");

	let LogicalPlan::CreateLogicalType {
		name,
		physical,
		refinement: Some(refinement),
	} = plan
	else {
		panic!("expected refined create logical type");
	};

	assert_eq!(name, "PersonId");
	assert_eq!(physical, DataType::Int64);
	assert_eq!(
		refinement,
		LogicalTypeRefinementPredicate::And(vec![
			LogicalTypeRefinementPredicate::Comparison {
				left: LogicalTypeRefinementValue::Value,
				op: LogicalTypeRefinementComparisonOp::Gt,
				right: LogicalTypeRefinementValue::Int64(0),
			},
			LogicalTypeRefinementPredicate::Comparison {
				left: LogicalTypeRefinementValue::Value,
				op: LogicalTypeRefinementComparisonOp::Le,
				right: LogicalTypeRefinementValue::Int64(100),
			},
		])
	);
}

#[test]
fn ddl_create_logical_type_check_rejects_unbounded_boolean_shapes() {
	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0 OR VALUE < -10)",
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (NOT VALUE > 0)",
	] {
		let err = SqlPlanner::new()
			.plan(sql)
			.expect_err("unsupported refinement shape rejects");
		assert!(format!("{err:?}").contains("does not support"));
	}
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
			LogicalTypeDefinition {
				logical_type_id: DURABLE_LOGICAL_TYPE_MIN_ID,
				name: "CustomerId".to_string(),
				physical: LogicalTypePhysical::Int64,
				refinement: None,
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

#[tokio::test]
async fn refined_logical_type_insert_values_allows_null_and_rejects_violation() {
	let (_catalog, coordinator, planner) = logical_type_writer_coordinator();

	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0)",
		"CREATE TABLE people (id PersonId, note TEXT)",
		"INSERT INTO people VALUES (1, 'ok')",
		"INSERT INTO people VALUES (NULL, 'nullable')",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	expect_query_error_contains(
		&coordinator,
		&planner,
		"INSERT INTO people VALUES (-1, 'bad')",
		"violates logical type refinement",
	)
	.await;

	let batch = concat_query(&coordinator, &planner, "SELECT id FROM people").await;
	assert_eq!(int64_rows(&batch, 0), vec![Some(1), None]);
}

#[tokio::test]
async fn refined_logical_type_write_boundary_covers_select_update_and_ctas() {
	let (_catalog, coordinator, planner) = logical_type_writer_coordinator();

	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0)",
		"CREATE TABLE raw_ids (id BIGINT)",
		"INSERT INTO raw_ids VALUES (2)",
		"INSERT INTO raw_ids VALUES (-2)",
		"CREATE TABLE people (pk BIGINT NOT NULL PRIMARY KEY, id PersonId)",
		"INSERT INTO people (pk, id) SELECT id, id FROM raw_ids WHERE id > 0",
		"CREATE TABLE ctas_ok AS SELECT CAST(id AS PersonId) AS id FROM raw_ids WHERE id > 0",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	expect_query_error_contains(
		&coordinator,
		&planner,
		"INSERT INTO people (pk, id) SELECT id, id FROM raw_ids WHERE id < 0",
		"violates logical type refinement",
	)
	.await;
	expect_query_error_contains(
		&coordinator,
		&planner,
		"UPDATE people SET id = 0 WHERE pk = 2",
		"violates logical type refinement",
	)
	.await;
	expect_query_error_contains(
		&coordinator,
		&planner,
		"CREATE TABLE ctas_bad AS SELECT CAST(id AS PersonId) AS id FROM raw_ids WHERE id < 0",
		"violates logical type refinement",
	)
	.await;

	let people = concat_query(&coordinator, &planner, "SELECT id FROM people").await;
	assert_eq!(int64_rows(&people, 0), vec![Some(2)]);
	let ctas_ok = concat_query(&coordinator, &planner, "SELECT id FROM ctas_ok").await;
	assert_eq!(int64_rows(&ctas_ok, 0), vec![Some(2)]);
}

#[tokio::test]
async fn refined_positive_column_eq_negative_elides_to_empty_relation() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_logical(&catalog, people_filter(int64_eq("id", -5))).await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn refined_positive_column_range_disjoint_elides() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(&catalog, &planner, "SELECT id FROM people WHERE id < 0").await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn derived_table_transparent_projection_refinement_elides_to_empty_relation() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT person_id FROM (SELECT id AS person_id FROM people) AS p WHERE person_id < 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn derived_table_projection_subset_reorder_and_rename_preserve_refinement_elision() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT renamed_id FROM (SELECT note, id AS renamed_id FROM people) AS p WHERE renamed_id < 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn derived_table_computed_projection_binds_but_does_not_refinement_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT id FROM (SELECT id + 0 AS id FROM people) AS p WHERE id < 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
}

#[tokio::test]
async fn derived_table_outer_projection_is_unconstrained() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT person_id + 1 FROM (SELECT id AS person_id FROM people) AS p WHERE person_id < 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn derived_table_outer_filter_keeps_098_eager_error_fence() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT person_id FROM (SELECT id AS person_id FROM people) AS p WHERE person_id < 0 AND CAST(2147483648.0 AS INT) = 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
}

#[tokio::test]
async fn alias_columns_base_table_rename_output() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT pid FROM people AS p(pid, label)",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "pid");
}

#[tokio::test]
async fn alias_columns_explicit_derived_rename_output() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT pid FROM (SELECT id FROM people) AS p(pid)",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "pid");
}

#[tokio::test]
async fn alias_columns_wildcard_derived_rename_output() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT pid FROM (SELECT * FROM people) AS p(pid, label)",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "pid");
}

#[tokio::test]
async fn explicit_derived_alias_columns_collapse_adjacent_identity_project() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT * FROM (SELECT id, note FROM people) AS p(pid, label)",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "pid");
	assert_eq!(bound.schema().field(1).name(), "label");
	assert_eq!(project_count(&bound), 1);
}

#[tokio::test]
async fn explicit_derived_alias_columns_collapse_after_outer_projection() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT pid FROM (SELECT id, note FROM people) AS p(pid, label)",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "pid");
	assert_eq!(project_count(&bound), 1);
}

#[tokio::test]
async fn plain_derived_outer_projection_collapses_adjacent_projects() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT id FROM (SELECT id, note FROM people) AS p",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "id");
	assert_eq!(project_count(&bound), 1);
}

#[tokio::test]
async fn base_table_alias_columns_keep_project_boundary() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(&catalog, &planner, "SELECT * FROM people AS p(pid, label)").await;

	assert_eq!(bound.schema().field(0).name(), "pid");
	assert_eq!(bound.schema().field(1).name(), "label");
	assert_eq!(project_count(&bound), 1);
}

#[tokio::test]
async fn wildcard_derived_alias_columns_keep_project_boundary() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT * FROM (SELECT * FROM people) AS p(pid, label)",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "pid");
	assert_eq!(bound.schema().field(1).name(), "label");
	assert_eq!(project_count(&bound), 1);
}

#[tokio::test]
async fn alias_columns_hide_original_names() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;
	let plan = planner
		.plan("SELECT id FROM people AS p(pid, label)")
		.expect("plan alias column query");

	let err = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect_err("old output name should not bind");

	assert!(matches!(
		err,
		BindingError::UnknownColumn { column } if column == "id"
	));
}

#[tokio::test]
async fn alias_columns_arity_mismatch_rejects_at_bind() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	for sql in [
		"SELECT a FROM people AS p(a)",
		"SELECT a FROM (SELECT id, note FROM people) AS p(a)",
		"SELECT a FROM (SELECT * FROM people) AS p(a)",
	] {
		let plan = planner
			.plan(sql)
			.expect("alias column arity is validated at bind");
		let err = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
			.await
			.expect_err("alias column arity should fail during binding");
		let message = format!("{err:?}");
		for needle in ["arity mismatch", "expected", "got"] {
			assert!(
				message.contains(needle),
				"expected binding error for {sql:?} to contain {needle:?}, got {message}"
			);
		}
	}
}

#[tokio::test]
async fn alias_columns_preserve_refinement_elision_base_explicit_and_wildcard() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	for sql in [
		"SELECT pid FROM people AS p(pid, label) WHERE pid < 0",
		"SELECT pid FROM (SELECT id FROM people) AS p(pid) WHERE pid < 0",
		"SELECT pid FROM (SELECT * FROM people) AS p(pid, label) WHERE pid < 0",
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;

		assert!(
			find_empty_relation(&bound).is_some(),
			"expected EmptyRelation for {sql}"
		);
		assert!(!contains_scan(&bound), "expected scan elision for {sql}");
	}
}

#[tokio::test]
async fn alias_columns_computed_projection_still_not_transparent() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT computed FROM (SELECT id + 0 FROM people) AS p(computed) WHERE computed < 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
}

#[tokio::test]
async fn alias_columns_reject_type_annotations() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	for sql in [
		"SELECT pid FROM people AS p(pid BIGINT)",
		"SELECT pid FROM (SELECT id FROM people) AS p(pid BIGINT)",
	] {
		expect_plan_or_bind_error_contains(
			&catalog,
			&planner,
			sql,
			"table alias column type annotations are not supported",
		)
		.await;
	}
}

#[tokio::test]
async fn alias_columns_duplicate_names_inherit_ambiguity() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;
	let plan = planner
		.plan("SELECT dup FROM people AS p(dup, dup)")
		.expect("plan duplicate alias columns");

	let err = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect_err("duplicate alias output should be ambiguous");

	assert!(matches!(
		err,
		BindingError::AmbiguousColumn { column } if column == "dup"
	));
}

#[tokio::test]
async fn alias_columns_compose() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT label FROM (SELECT id, note FROM people) AS p(pid, label) WHERE pid > 0 ORDER BY label",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "label");
}

#[tokio::test]
async fn derived_table_source_scope_rejects_general_subquery_features() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	for sql in [
		"SELECT id FROM (SELECT id FROM people) WHERE id < 0",
		"SELECT id FROM LATERAL (SELECT id FROM people) AS p WHERE id < 0",
		"SELECT id FROM (SELECT id FROM (SELECT id FROM people) AS q) AS p WHERE id < 0",
		"SELECT id FROM (SELECT people.id FROM people JOIN people AS other ON people.id = other.id) AS p WHERE id < 0",
		"SELECT id FROM (SELECT id FROM people) AS p JOIN people AS other ON p.id = other.id",
		"SELECT id FROM (SELECT COUNT(*) AS id FROM people) AS p WHERE id < 0",
		"SELECT id FROM (SELECT id, COUNT(*) AS n FROM people GROUP BY id) AS p WHERE id < 0",
		"SELECT id FROM (SELECT id FROM people ORDER BY id LIMIT 1) AS p WHERE id < 0",
		"SELECT id FROM (WITH q AS (SELECT id FROM people) SELECT id FROM q) AS p WHERE id < 0",
		"SELECT id FROM (SELECT id FROM people UNION SELECT id FROM people) AS p WHERE id < 0",
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, "derived").await;
	}
}

#[tokio::test]
async fn refinement_and_safe_conjunct_elides_to_empty_relation() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT id FROM people WHERE id < 0 AND note = 'x'",
	)
	.await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn refinement_nested_and_safe_conjunct_elides_to_empty_relation() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_logical(
		&catalog,
		people_filter(Expr::And(
			Box::new(Expr::Eq(
				Box::new(Expr::column("note")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("x".to_string())))),
			)),
			Box::new(Expr::And(
				Box::new(int64_lt("id", 0)),
				Box::new(Expr::IsNull(Box::new(Expr::column("note")))),
			)),
		)),
	)
	.await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn refined_positive_column_overlapping_range_does_not_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	for sql in [
		"SELECT id FROM people WHERE id = 5",
		"SELECT id FROM people WHERE id > 100",
		"SELECT id FROM people WHERE id = 5 AND note = 'x'",
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert!(find_empty_relation(&bound).is_none(), "{sql}");
		assert!(contains_scan(&bound), "{sql}");
	}
}

#[tokio::test]
async fn refinement_and_strict_cast_sibling_does_not_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_logical(
		&catalog,
		people_filter(Expr::And(
			Box::new(int64_lt("id", 0)),
			Box::new(Expr::Eq(
				Box::new(Expr::Cast(
					Box::new(Expr::Literal(ScalarValue::Float64(Some(2147483648.0)))),
					DataType::Int32,
				)),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(0)))),
			)),
		)),
	)
	.await;

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
}

#[tokio::test]
async fn refinement_or_not_root_still_do_not_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let or_bound = bind_sql(
		&catalog,
		&planner,
		"SELECT id FROM people WHERE id < 0 OR note = 'x'",
	)
	.await;
	assert!(find_empty_relation(&or_bound).is_none());
	assert!(contains_scan(&or_bound));

	let not_bound = bind_logical(
		&catalog,
		people_filter(Expr::Not(Box::new(Expr::Gt(
			Box::new(Expr::column("id")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(0)))),
		)))),
	)
	.await;
	assert!(find_empty_relation(&not_bound).is_none());
	assert!(contains_scan(&not_bound));
}

#[tokio::test]
async fn refinement_and_non_receipt_shapes_do_not_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;
	let plan = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("people"),
				projection: None,
			}),
			exprs: vec![
				Expr::Cast(Box::new(Expr::column("id")), DataType::Int64),
				Expr::column("note"),
			],
			output_names: vec![Some("id".to_string()), Some("note".to_string())],
		}),
		predicate: Expr::And(
			Box::new(int64_lt("id", 0)),
			Box::new(Expr::Eq(
				Box::new(Expr::column("note")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("x".to_string())))),
			)),
		),
	};
	let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect("bind projected refined AND plan");

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
}

#[tokio::test]
async fn refinement_exclusion_point_elides() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE NonZeroId AS BIGINT CHECK (VALUE != 0)",
		"CREATE TABLE non_zero_people (id NonZeroId, note TEXT)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT id FROM non_zero_people WHERE id = 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn cast_ascribed_projected_column_does_not_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0)",
		"CREATE TABLE raw_people (raw_id BIGINT)",
	] {
		execute(&coordinator, &planner, sql).await;
	}
	let plan = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("raw_people"),
				projection: None,
			}),
			exprs: vec![Expr::SqlLogicalCast {
				expr: Box::new(Expr::column("raw_id")),
				logical_name: "PersonId".to_string(),
				try_cast: false,
			}],
			output_names: vec![Some("id".to_string())],
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("id")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(-5)))),
		),
	};
	let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect("bind cast-ascribed non-receipt plan");

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
}

#[tokio::test]
async fn refinement_through_transparent_project_elides() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;
	let plan = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("people"),
				projection: None,
			}),
			exprs: vec![Expr::column("id")],
			output_names: vec![Some("id".to_string())],
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("id")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(-5)))),
		),
	};
	let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect("bind projected refined transparent plan");

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn nullable_refined_column_is_null_does_not_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(&catalog, &planner, "SELECT id FROM people WHERE id IS NULL").await;

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
}

#[tokio::test]
async fn naked_or_unrefined_column_does_not_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
		"CREATE TABLE customers (id CustomerId, note TEXT)",
		"CREATE TABLE raw_ids (id BIGINT, note TEXT)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	for sql in [
		"SELECT id FROM customers WHERE id < 0",
		"SELECT id FROM raw_ids WHERE id < 0",
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert!(find_empty_relation(&bound).is_none(), "{sql}");
		assert!(contains_scan(&bound), "{sql}");
	}
}

#[tokio::test]
async fn refinement_elision_uses_durable_registry_not_seed_lookup() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;
	let durable_bound = bind_logical(&catalog, people_filter(int64_eq("id", -5))).await;
	assert!(find_empty_relation(&durable_bound).is_some());

	let mut seed_catalog = MockCatalog::new();
	let seed_schema = Arc::new(Schema::new(vec![field_with_logical_type_metadata(
		"id",
		DataType::Int64,
		true,
		CUSTOMER_ID_LOGICAL_TYPE,
	)]));
	seed_catalog
		.register_descriptor(
			TableId(77),
			descriptor_from_schema("seed_people", seed_schema.as_ref()),
		)
		.expect("register seed typed table");
	let seed_plan = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("seed_people"),
			projection: None,
		}),
		predicate: int64_eq("id", -5),
	};
	let seed_bound = bind(&seed_plan, &seed_catalog, &PlannerConfig::default())
		.await
		.expect("bind seed logical type query");

	assert!(find_empty_relation(&seed_bound).is_none());
	assert!(contains_scan(&seed_bound));
}

#[tokio::test]
async fn float_refinement_decodes_bits_for_ordering() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE Price AS DOUBLE CHECK (VALUE >= 0.0)",
		"CREATE TABLE prices (price Price)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let bound = bind_logical(
		&catalog,
		LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("prices"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::column("price")),
				Box::new(Expr::Literal(ScalarValue::Float64(Some(-1.0)))),
			),
		},
	)
	.await;

	assert!(find_empty_relation(&bound).is_some());
	assert!(!contains_scan(&bound));
}

#[tokio::test]
async fn boolean_and_utf8_refinement_singletons_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE EnabledFlag AS BOOLEAN CHECK (VALUE = TRUE)",
		"CREATE TABLE feature_flags (enabled EnabledFlag)",
		"CREATE LOGICAL TYPE SafeSku AS TEXT CHECK (VALUE != 'bad')",
		"CREATE TABLE skus (sku SafeSku)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	for sql in [
		"SELECT enabled FROM feature_flags WHERE enabled = FALSE",
		"SELECT sku FROM skus WHERE sku = 'bad'",
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert!(find_empty_relation(&bound).is_some(), "{sql}");
		assert!(!contains_scan(&bound), "{sql}");
	}
}

#[tokio::test]
async fn count_star_over_refinement_elided_filter_returns_single_zero() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;
	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT COUNT(*) FROM people WHERE id < 0",
	)
	.await;
	assert!(find_empty_relation(&bound).is_some());

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT COUNT(*) FROM people WHERE id < 0",
	)
	.await;

	assert_eq!(int64_rows(&batch, 0), vec![Some(0)]);
}

#[tokio::test]
async fn update_or_insert_select_source_filter_elides_through_shared_planner() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0)",
		"CREATE TABLE update_people (pk BIGINT NOT NULL PRIMARY KEY, id PersonId, note TEXT)",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let bound = bind_sql(
		&catalog,
		&planner,
		"UPDATE update_people SET note = 'miss' WHERE id < 0",
	)
	.await;
	let ExecutionPlan::Update { source, .. } = &bound else {
		panic!("expected update plan, got {bound:?}");
	};
	let ExecutionPlan::Project { input, .. } = source.as_ref() else {
		panic!("expected update source project, got {source:?}");
	};
	assert!(
		matches!(input.as_ref(), ExecutionPlan::EmptyRelation { .. }),
		"expected refinement-elided update source, got {input:?}"
	);
}

#[tokio::test]
async fn information_schema_logical_types_lists_current_tenant_durable_definitions() {
	let (_catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0 AND VALUE <= 100)",
		"CREATE LOGICAL TYPE Sku AS TEXT",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT logical_type_id, name, physical, refinement FROM information_schema.logical_types ORDER BY logical_type_id",
	)
	.await;

	assert_eq!(
		int64_rows(&batch, 0),
		vec![
			Some(i64::from(DURABLE_LOGICAL_TYPE_MIN_ID)),
			Some(i64::from(DURABLE_LOGICAL_TYPE_MIN_ID + 1)),
		]
	);
	assert_eq!(
		string_rows(&batch, 1),
		vec![Some("PersonId".to_string()), Some("Sku".to_string())]
	);
	assert_eq!(
		string_rows(&batch, 2),
		vec![Some("BIGINT".to_string()), Some("TEXT".to_string())]
	);
	assert_eq!(
		string_rows(&batch, 3),
		vec![Some("CHECK (VALUE > 0 AND VALUE <= 100)".to_string()), None,]
	);
}

#[tokio::test]
async fn information_schema_logical_types_renders_sql_literals_and_excludes_seed() {
	let (_catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE EnabledFlag AS BOOLEAN CHECK (VALUE = TRUE)",
		"CREATE LOGICAL TYPE SafeSku AS TEXT CHECK (VALUE != 'bad''sku')",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT name, physical, refinement FROM information_schema.logical_types ORDER BY name",
	)
	.await;

	assert_eq!(
		string_rows(&batch, 0),
		vec![Some("EnabledFlag".to_string()), Some("SafeSku".to_string())]
	);
	assert_eq!(
		string_rows(&batch, 1),
		vec![Some("BOOLEAN".to_string()), Some("TEXT".to_string())]
	);
	assert_eq!(
		string_rows(&batch, 2),
		vec![
			Some("CHECK (VALUE = TRUE)".to_string()),
			Some("CHECK (VALUE != 'bad''sku')".to_string()),
		]
	);
	assert!(
		!string_rows(&batch, 0)
			.into_iter()
			.flatten()
			.any(|name| name == "CustomerId" || name == "ProductId" || name == "naked")
	);
}

#[tokio::test]
async fn information_schema_logical_types_supports_query_ops_and_ctas_source() {
	let (_catalog, coordinator, planner) = logical_type_writer_coordinator();
	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0)",
		"CREATE LOGICAL TYPE Sku AS TEXT",
		"CREATE TABLE logical_type_names AS SELECT name FROM information_schema.logical_types WHERE physical = 'BIGINT'",
	] {
		execute(&coordinator, &planner, sql).await;
	}

	let filtered = concat_query(
		&coordinator,
		&planner,
		"SELECT name FROM information_schema.logical_types WHERE physical = 'BIGINT' ORDER BY name",
	)
	.await;
	assert_eq!(
		string_rows(&filtered, 0),
		vec![Some("PersonId".to_string())]
	);

	let count = concat_query(
		&coordinator,
		&planner,
		"SELECT COUNT(*) FROM information_schema.logical_types",
	)
	.await;
	assert_eq!(int64_rows(&count, 0), vec![Some(2)]);

	let copied = concat_query(
		&coordinator,
		&planner,
		"SELECT name FROM logical_type_names",
	)
	.await;
	assert_eq!(string_rows(&copied, 0), vec![Some("PersonId".to_string())]);
}

#[tokio::test]
async fn information_schema_logical_types_is_tenant_scoped() {
	let catalog = Arc::new(MockCatalog::new());
	catalog
		.register_logical_type_for_tenant(
			TenantId(1),
			logical_type_definition(
				DURABLE_LOGICAL_TYPE_MIN_ID,
				"TenantOne",
				LogicalTypePhysical::Int64,
				None,
			),
		)
		.expect("tenant one definition");
	catalog
		.register_logical_type_for_tenant(
			TenantId(2),
			logical_type_definition(
				DURABLE_LOGICAL_TYPE_MIN_ID + 1,
				"TenantTwo",
				LogicalTypePhysical::Utf8,
				None,
			),
		)
		.expect("tenant two definition");
	let planner = SqlPlanner::new();
	let plan = planner
		.plan("SELECT name FROM information_schema.logical_types")
		.expect("plan introspection");

	let tenant_one = Arc::clone(&catalog).scoped(TenantId(1));
	let tenant_two = Arc::clone(&catalog).scoped(TenantId(2));
	let batch_one = execute_bound_query(
		bind(&plan, tenant_one.as_ref(), &PlannerConfig::default())
			.await
			.unwrap(),
	)
	.await;
	let batch_two = execute_bound_query(
		bind(&plan, tenant_two.as_ref(), &PlannerConfig::default())
			.await
			.unwrap(),
	)
	.await;

	assert_eq!(
		string_rows(&batch_one, 0),
		vec![Some("TenantOne".to_string())]
	);
	assert_eq!(
		string_rows(&batch_two, 0),
		vec![Some("TenantTwo".to_string())]
	);
}

#[tokio::test]
async fn information_schema_logical_types_refreshes_before_read() {
	let inner = Arc::new(MockCatalog::new());
	let refreshes = Arc::new(AtomicU64::new(0));
	let catalog = Arc::new(RefreshingLogicalTypeCatalog {
		inner,
		tenant_id: TenantId(1),
		definitions: vec![logical_type_definition(
			DURABLE_LOGICAL_TYPE_MIN_ID + 10,
			"RefreshedId",
			LogicalTypePhysical::Int64,
			None,
		)],
		refreshes: Arc::clone(&refreshes),
	});
	let planner = SqlPlanner::new();
	let bound = bind(
		&planner
			.plan("SELECT name FROM information_schema.logical_types")
			.expect("plan introspection"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind refreshed information schema");
	let batch = execute_bound_query(bound).await;

	assert_eq!(refreshes.load(Ordering::SeqCst), 1);
	assert_eq!(
		string_rows(&batch, 0),
		vec![Some("RefreshedId".to_string())]
	);
}

#[tokio::test]
async fn information_schema_tables_lists_authoritative_tenant_tables() {
	let descriptor = descriptor_from_schema_with_identity(
		11,
		1,
		"analytics",
		"events",
		&Schema::new(vec![arrow::datatypes::Field::new(
			"event_id",
			DataType::Int64,
			false,
		)]),
	);
	let refreshes = Arc::new(AtomicU64::new(0));
	let catalog: Arc<dyn Catalog> = Arc::new(RefreshingTableDescriptorCatalog {
		inner: Arc::new(MockCatalog::new()),
		tenant_id: TenantId(1),
		descriptors: vec![descriptor],
		definitions: Vec::new(),
		refreshes: Arc::clone(&refreshes),
		fail_message: None,
	});
	let coordinator = QueryCoordinator::new(
		catalog,
		Executor::new(empty_storage()),
		PlannerConfig::default(),
	);
	let planner = SqlPlanner::new();

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY table_schema, table_name",
	)
	.await;

	assert_eq!(refreshes.load(Ordering::SeqCst), 1);
	assert_eq!(string_rows(&batch, 0), vec![Some("analytics".to_string())]);
	assert_eq!(string_rows(&batch, 1), vec![Some("events".to_string())]);
	assert_eq!(string_rows(&batch, 2), vec![Some("BASE TABLE".to_string())]);
}

#[tokio::test]
async fn information_schema_columns_lists_authoritative_columns() {
	let descriptor = descriptor_from_schema_with_identity(
		12,
		1,
		"analytics",
		"events",
		&Schema::new(vec![
			arrow::datatypes::Field::new("event_id", DataType::Int64, false),
			arrow::datatypes::Field::new("event_name", DataType::Utf8, true),
		]),
	);
	let catalog: Arc<dyn Catalog> = Arc::new(RefreshingTableDescriptorCatalog {
		inner: Arc::new(MockCatalog::new()),
		tenant_id: TenantId(1),
		descriptors: vec![descriptor],
		definitions: Vec::new(),
		refreshes: Arc::new(AtomicU64::new(0)),
		fail_message: None,
	});
	let coordinator = QueryCoordinator::new(
		catalog,
		Executor::new(empty_storage()),
		PlannerConfig::default(),
	);
	let planner = SqlPlanner::new();

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT table_schema, table_name, column_name, ordinal_position, data_type, is_nullable, logical_type_name FROM information_schema.columns ORDER BY ordinal_position",
	)
	.await;

	assert_eq!(
		string_rows(&batch, 0),
		vec![Some("analytics".to_string()), Some("analytics".to_string())]
	);
	assert_eq!(
		string_rows(&batch, 1),
		vec![Some("events".to_string()), Some("events".to_string())]
	);
	assert_eq!(
		string_rows(&batch, 2),
		vec![Some("event_id".to_string()), Some("event_name".to_string())]
	);
	assert_eq!(int64_rows(&batch, 3), vec![Some(1), Some(2)]);
	assert_eq!(
		string_rows(&batch, 4),
		vec![Some("BIGINT".to_string()), Some("TEXT".to_string())]
	);
	assert_eq!(
		string_rows(&batch, 5),
		vec![Some("NO".to_string()), Some("YES".to_string())]
	);
	assert_eq!(string_rows(&batch, 6), vec![None, None]);
}

#[tokio::test]
async fn information_schema_columns_renders_logical_type_names_after_refresh() {
	let logical_type_id = DURABLE_LOGICAL_TYPE_MIN_ID + 50;
	let descriptor = descriptor_from_schema_with_identity(
		13,
		1,
		"analytics",
		"accounts",
		&Schema::new(vec![field_with_logical_type_metadata(
			"tenant_id",
			DataType::Int64,
			false,
			LogicalTypeId(logical_type_id),
		)]),
	);
	let catalog: Arc<dyn Catalog> = Arc::new(RefreshingTableDescriptorCatalog {
		inner: Arc::new(MockCatalog::new()),
		tenant_id: TenantId(1),
		descriptors: vec![descriptor],
		definitions: vec![logical_type_definition(
			logical_type_id,
			"TenantId",
			LogicalTypePhysical::Int64,
			None,
		)],
		refreshes: Arc::new(AtomicU64::new(0)),
		fail_message: None,
	});
	let coordinator = QueryCoordinator::new(
		catalog,
		Executor::new(empty_storage()),
		PlannerConfig::default(),
	);
	let planner = SqlPlanner::new();

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT column_name, logical_type_name FROM information_schema.columns",
	)
	.await;

	assert_eq!(string_rows(&batch, 0), vec![Some("tenant_id".to_string())]);
	assert_eq!(string_rows(&batch, 1), vec![Some("TenantId".to_string())]);
}

#[tokio::test]
async fn information_schema_tables_and_columns_are_tenant_scoped() {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor_for_tenant(
			TenantId(1),
			TableId(101),
			descriptor_from_schema_with_identity(
				21,
				1,
				"tenant_one",
				"events",
				&Schema::new(vec![arrow::datatypes::Field::new(
					"tenant_one_id",
					DataType::Int64,
					false,
				)]),
			),
		)
		.expect("register tenant one descriptor");
	catalog
		.register_descriptor_for_tenant(
			TenantId(2),
			TableId(201),
			descriptor_from_schema_with_identity(
				22,
				1,
				"tenant_two",
				"events",
				&Schema::new(vec![arrow::datatypes::Field::new(
					"tenant_two_id",
					DataType::Int64,
					false,
				)]),
			),
		)
		.expect("register tenant two descriptor");
	let catalog = Arc::new(catalog);
	let planner = SqlPlanner::new();
	let tables_plan = planner
		.plan("SELECT table_schema FROM information_schema.tables")
		.expect("plan tables");
	let columns_plan = planner
		.plan("SELECT column_name FROM information_schema.columns")
		.expect("plan columns");
	let tenant_one = Arc::clone(&catalog).scoped(TenantId(1));
	let tenant_two = Arc::clone(&catalog).scoped(TenantId(2));

	let tenant_one_tables = execute_bound_query(
		bind(&tables_plan, tenant_one.as_ref(), &PlannerConfig::default())
			.await
			.unwrap(),
	)
	.await;
	let tenant_two_tables = execute_bound_query(
		bind(&tables_plan, tenant_two.as_ref(), &PlannerConfig::default())
			.await
			.unwrap(),
	)
	.await;
	let tenant_one_columns = execute_bound_query(
		bind(
			&columns_plan,
			tenant_one.as_ref(),
			&PlannerConfig::default(),
		)
		.await
		.unwrap(),
	)
	.await;
	let tenant_two_columns = execute_bound_query(
		bind(
			&columns_plan,
			tenant_two.as_ref(),
			&PlannerConfig::default(),
		)
		.await
		.unwrap(),
	)
	.await;

	assert_eq!(
		string_rows(&tenant_one_tables, 0),
		vec![Some("tenant_one".to_string())]
	);
	assert_eq!(
		string_rows(&tenant_two_tables, 0),
		vec![Some("tenant_two".to_string())]
	);
	assert_eq!(
		string_rows(&tenant_one_columns, 0),
		vec![Some("tenant_one_id".to_string())]
	);
	assert_eq!(
		string_rows(&tenant_two_columns, 0),
		vec![Some("tenant_two_id".to_string())]
	);
}

#[tokio::test]
async fn information_schema_descriptor_listing_failure_fails_query_without_stale_fallback() {
	let mut inner = MockCatalog::new();
	inner
		.register_descriptor_for_tenant(
			TenantId(1),
			TableId(301),
			descriptor_from_schema_with_identity(
				31,
				1,
				"stale",
				"stale_events",
				&Schema::new(vec![arrow::datatypes::Field::new(
					"id",
					DataType::Int64,
					false,
				)]),
			),
		)
		.expect("register stale descriptor");
	let catalog = RefreshingTableDescriptorCatalog {
		inner: Arc::new(inner),
		tenant_id: TenantId(1),
		descriptors: Vec::new(),
		definitions: Vec::new(),
		refreshes: Arc::new(AtomicU64::new(0)),
		fail_message: Some("descriptor list unavailable".to_string()),
	};
	let planner = SqlPlanner::new();

	let err = bind(
		&planner
			.plan("SELECT table_name FROM information_schema.tables")
			.expect("plan tables"),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect_err("descriptor list failure must fail binding");

	assert!(
		format!("{err:?}").contains("descriptor list unavailable"),
		"unexpected stale fallback error: {err:?}"
	);
}

#[tokio::test]
async fn information_schema_tables_columns_are_latest_only() {
	let mut source = TableSchemaCatalogV1::default();
	let tenant = WendelonTenantId::new(1);
	source
		.upsert(
			tenant,
			descriptor_from_schema_with_identity(
				61,
				1,
				"analytics",
				"events",
				&Schema::new(vec![arrow::datatypes::Field::new(
					"old_event_id",
					DataType::Int64,
					false,
				)]),
			),
		)
		.expect("upsert old descriptor");
	source
		.upsert(
			tenant,
			descriptor_from_schema_with_identity(
				61,
				2,
				"analytics",
				"events",
				&Schema::new(vec![
					arrow::datatypes::Field::new("event_id", DataType::Int64, false),
					arrow::datatypes::Field::new("event_name", DataType::Utf8, true),
				]),
			),
		)
		.expect("upsert latest descriptor");
	let descriptors = source.latest_descriptors_for_tenant(tenant);
	assert_eq!(descriptors.len(), 1);
	let catalog: Arc<dyn Catalog> = Arc::new(RefreshingTableDescriptorCatalog {
		inner: Arc::new(MockCatalog::new()),
		tenant_id: TenantId(1),
		descriptors,
		definitions: Vec::new(),
		refreshes: Arc::new(AtomicU64::new(0)),
		fail_message: None,
	});
	let coordinator = QueryCoordinator::new(
		catalog,
		Executor::new(empty_storage()),
		PlannerConfig::default(),
	);
	let planner = SqlPlanner::new();

	let tables = concat_query(
		&coordinator,
		&planner,
		"SELECT table_name FROM information_schema.tables",
	)
	.await;
	let columns = concat_query(
		&coordinator,
		&planner,
		"SELECT column_name, ordinal_position FROM information_schema.columns ORDER BY ordinal_position",
	)
	.await;

	assert_eq!(string_rows(&tables, 0), vec![Some("events".to_string())]);
	assert_eq!(
		string_rows(&columns, 0),
		vec![Some("event_id".to_string()), Some("event_name".to_string())]
	);
}

#[tokio::test]
async fn information_schema_tables_and_columns_compose_with_query_ops() {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor_for_tenant(
			TenantId(1),
			TableId(401),
			descriptor_from_schema_with_identity(
				41,
				1,
				"analytics",
				"events",
				&Schema::new(vec![
					arrow::datatypes::Field::new("event_id", DataType::Int64, false),
					arrow::datatypes::Field::new("event_name", DataType::Utf8, true),
				]),
			),
		)
		.expect("register descriptor");
	let catalog: Arc<dyn Catalog> = Arc::new(catalog);
	let coordinator = QueryCoordinator::new(
		catalog,
		Executor::new(empty_storage()),
		PlannerConfig::default(),
	);
	let planner = SqlPlanner::new();

	let filtered = concat_query(
		&coordinator,
		&planner,
		"SELECT column_name, ordinal_position FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'events' ORDER BY ordinal_position",
	)
	.await;
	let counted = concat_query(
		&coordinator,
		&planner,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_type = 'BASE TABLE'",
	)
	.await;

	assert_eq!(
		string_rows(&filtered, 0),
		vec![Some("event_id".to_string()), Some("event_name".to_string())]
	);
	assert_eq!(int64_rows(&counted, 0), vec![Some(1)]);
}

#[tokio::test]
async fn information_schema_namespace_is_read_only_and_scoped() {
	let (catalog, _coordinator, planner) = logical_type_writer_coordinator();

	for table in ["logical_types", "tables", "columns"] {
		let sql = format!("SELECT * FROM {table}");
		let unqualified = bind(
			&planner.plan(&sql).expect("plan unqualified name"),
			catalog.as_ref(),
			&PlannerConfig::default(),
		)
		.await
		.expect_err("unqualified information_schema member remains normal table lookup");
		assert!(matches!(
			unqualified,
			BindingError::Catalog(CatalogError::TableNotFound { table: ref missing }) if missing == table
		));
	}

	let unknown = bind(
		&planner
			.plan("SELECT * FROM information_schema.schemata")
			.expect("plan unknown information schema table"),
		catalog.as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("unknown information schema relation rejects");
	assert!(
		format!("{unknown:?}").contains("unknown information_schema relation"),
		"unexpected unknown relation error: {unknown:?}"
	);

	for sql in [
		"CREATE TABLE information_schema.logical_types (id BIGINT)",
		"CREATE TABLE information_schema.tables (id BIGINT)",
		"CREATE TABLE information_schema.copy AS SELECT name FROM information_schema.logical_types",
		"DROP TABLE information_schema.logical_types",
		"DROP TABLE information_schema.columns",
		"INSERT INTO information_schema.logical_types VALUES (1, 'X', 'BIGINT', NULL)",
		"INSERT INTO information_schema.tables VALUES ('public', 't', 'BASE TABLE')",
		"UPDATE information_schema.logical_types SET name = 'X'",
		"UPDATE information_schema.columns SET column_name = 'X'",
		"DELETE FROM information_schema.logical_types",
		"DELETE FROM information_schema.tables",
	] {
		let result = bind(
			&planner
				.plan(sql)
				.unwrap_or_else(|error| panic!("{sql}: {error}")),
			catalog.as_ref(),
			&PlannerConfig::default(),
		)
		.await;
		let err = result.expect_err(sql);
		assert!(
			format!("{err:?}").contains("information_schema is read-only"),
			"{sql} produced unexpected error: {err:?}"
		);
	}
}

#[tokio::test]
async fn system_table_executes_without_storage_scan() {
	let mut catalog = MockCatalog::new();
	catalog
		.register_logical_type_for_tenant(
			TenantId(1),
			logical_type_definition(
				DURABLE_LOGICAL_TYPE_MIN_ID,
				"PersonId",
				LogicalTypePhysical::Int64,
				None,
			),
		)
		.expect("register definition");
	catalog
		.register_descriptor_for_tenant(
			TenantId(1),
			TableId(501),
			descriptor_from_schema_with_identity(
				51,
				1,
				"analytics",
				"events",
				&Schema::new(vec![arrow::datatypes::Field::new(
					"event_id",
					DataType::Int64,
					false,
				)]),
			),
		)
		.expect("register descriptor");
	let coordinator = QueryCoordinator::new(
		Arc::new(catalog),
		Executor::new(Arc::new(FailingStorageScan)),
		PlannerConfig::default(),
	);
	let planner = SqlPlanner::new();

	let logical_types = concat_query(
		&coordinator,
		&planner,
		"SELECT name FROM information_schema.logical_types",
	)
	.await;
	let tables = concat_query(
		&coordinator,
		&planner,
		"SELECT table_name FROM information_schema.tables",
	)
	.await;
	let columns = concat_query(
		&coordinator,
		&planner,
		"SELECT column_name FROM information_schema.columns",
	)
	.await;

	assert_eq!(
		string_rows(&logical_types, 0),
		vec![Some("PersonId".to_string())]
	);
	assert_eq!(string_rows(&tables, 0), vec![Some("events".to_string())]);
	assert_eq!(string_rows(&columns, 0), vec![Some("event_id".to_string())]);
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

async fn setup_person_id_people(coordinator: &QueryCoordinator, planner: &SqlPlanner) {
	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0)",
		"CREATE TABLE people (id PersonId, note TEXT)",
	] {
		execute(coordinator, planner, sql).await;
	}
}

async fn bind_sql(catalog: &MockCatalog, planner: &SqlPlanner, sql: &str) -> ExecutionPlan {
	bind(
		&planner.plan(sql).expect("plan SQL"),
		catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect(sql)
}

async fn expect_plan_or_bind_error_contains(
	catalog: &MockCatalog,
	planner: &SqlPlanner,
	sql: &str,
	needle: &str,
) {
	let plan = match planner.plan(sql) {
		Ok(plan) => plan,
		Err(err) => {
			let message = format!("{err:?}");
			assert!(
				message.contains(needle),
				"expected planning error for {sql:?} to contain {needle:?}, got {message}"
			);
			return;
		}
	};
	let err = bind(&plan, catalog, &PlannerConfig::default())
		.await
		.expect_err(sql);
	let message = format!("{err:?}");
	assert!(
		message.contains(needle),
		"expected binding error for {sql:?} to contain {needle:?}, got {message}"
	);
}

async fn bind_logical(catalog: &MockCatalog, plan: LogicalPlan) -> ExecutionPlan {
	bind(&plan, catalog, &PlannerConfig::default())
		.await
		.expect("bind logical plan")
}

fn people_filter(predicate: Expr) -> LogicalPlan {
	LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("people"),
			projection: None,
		}),
		predicate,
	}
}

fn int64_eq(column: &str, value: i64) -> Expr {
	Expr::Eq(
		Box::new(Expr::column(column)),
		Box::new(Expr::Literal(ScalarValue::Int64(Some(value)))),
	)
}

fn int64_lt(column: &str, value: i64) -> Expr {
	Expr::Lt(
		Box::new(Expr::column(column)),
		Box::new(Expr::Literal(ScalarValue::Int64(Some(value)))),
	)
}

async fn expect_query_error_contains(
	coordinator: &QueryCoordinator,
	planner: &SqlPlanner,
	sql: &str,
	needle: &str,
) {
	let err = coordinator
		.execute_query(&planner.plan(sql).expect("plan SQL"), None)
		.await
		.expect_err(sql);
	let debug = format!("{err:?}");
	assert!(
		debug.contains(needle),
		"expected error containing {needle:?}, got {debug}"
	);
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

async fn execute_bound_query(bound: ExecutionPlan) -> RecordBatch {
	let results = Executor::new(empty_storage())
		.execute_bound(&bound, None)
		.await
		.expect("execute bound plan");
	let schema = results[0].schema();
	arrow::compute::concat_batches(&schema, results.iter()).expect("concat bound query results")
}

fn logical_type_definition(
	logical_type_id: u32,
	name: &str,
	physical: LogicalTypePhysical,
	refinement: Option<LogicalTypeRefinementPredicate>,
) -> LogicalTypeDefinition {
	LogicalTypeDefinition {
		logical_type_id,
		name: name.to_string(),
		physical,
		refinement,
	}
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

fn find_empty_relation(plan: &ExecutionPlan) -> Option<&Schema> {
	match plan {
		ExecutionPlan::EmptyRelation { schema } => Some(schema.as_ref()),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => find_empty_relation(input),
		ExecutionPlan::Join { left, right, .. } => {
			find_empty_relation(left).or_else(|| find_empty_relation(right))
		}
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => find_empty_relation(source),
		_ => None,
	}
}

fn contains_scan(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Scan { .. } => true,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => contains_scan(input),
		ExecutionPlan::Join { left, right, .. } => contains_scan(left) || contains_scan(right),
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => contains_scan(source),
		_ => false,
	}
}

fn project_count(plan: &ExecutionPlan) -> usize {
	match plan {
		ExecutionPlan::Project { input, .. } => 1 + project_count(input),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => project_count(input),
		ExecutionPlan::Join { left, right, .. } => project_count(left) + project_count(right),
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => project_count(source),
		_ => 0,
	}
}

fn descriptor_from_schema(table: &str, schema: &Schema) -> TableSchemaDescriptor {
	descriptor_from_schema_with_identity(9, 1, "default", table, schema)
}

fn descriptor_from_schema_with_identity(
	id_byte: u8,
	generation: u64,
	namespace: &str,
	table: &str,
	schema: &Schema,
) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([id_byte; 16]),
		generation,
		namespace,
		table,
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

struct RefreshingTableDescriptorCatalog {
	inner: Arc<MockCatalog>,
	tenant_id: TenantId,
	descriptors: Vec<TableSchemaDescriptor>,
	definitions: Vec<LogicalTypeDefinition>,
	refreshes: Arc<AtomicU64>,
	fail_message: Option<String>,
}

impl RefreshingTableDescriptorCatalog {
	fn delegate(&self) -> Arc<dyn Catalog> {
		Arc::clone(&self.inner).scoped(self.tenant_id)
	}
}

#[async_trait]
impl Catalog for RefreshingTableDescriptorCatalog {
	fn scoped(self: Arc<Self>, tenant_id: TenantId) -> Arc<dyn Catalog> {
		Arc::new(Self {
			inner: Arc::clone(&self.inner),
			tenant_id,
			descriptors: self.descriptors.clone(),
			definitions: self.definitions.clone(),
			refreshes: Arc::clone(&self.refreshes),
			fail_message: self.fail_message.clone(),
		})
	}

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		let delegate = self.delegate();
		delegate.descriptor_refresh_metrics()
	}

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let delegate = self.delegate();
		delegate.resolve_table(table).await
	}

	async fn resolve_table_local(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let delegate = self.delegate();
		delegate.resolve_table_local(table).await
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		let delegate = self.delegate();
		delegate.resolve_table_id(table).await
	}

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<toron::ir::LogicalTypeSpec, CatalogError> {
		let delegate = self.delegate();
		delegate.resolve_durable_logical_type_by_name(name).await
	}

	fn logical_type_registry(&self) -> toron::ir::LogicalTypeRegistry {
		let delegate = self.delegate();
		delegate.logical_type_registry()
	}

	async fn refreshed_logical_type_registry(
		&self,
	) -> Result<toron::ir::LogicalTypeRegistry, CatalogError> {
		let delegate = self.delegate();
		delegate.refreshed_logical_type_registry().await
	}

	async fn refreshed_table_schema_descriptors(
		&self,
	) -> Result<Vec<TableSchemaDescriptor>, CatalogError> {
		self.refreshes.fetch_add(1, Ordering::SeqCst);
		if let Some(message) = &self.fail_message {
			return Err(CatalogError::AdminFailed {
				message: message.clone(),
			});
		}
		for definition in &self.definitions {
			self.inner
				.register_logical_type_for_tenant(self.tenant_id, definition.clone())?;
		}
		Ok(self.descriptors.clone())
	}

	async fn invalidate_table_descriptor(
		&self,
		table: TableId,
	) -> Result<Option<toron::catalog::InvalidatedDescriptor>, CatalogError> {
		let delegate = self.delegate();
		delegate.invalidate_table_descriptor(table).await
	}

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: Arc<Schema>,
		row_identity: toron::ir::CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError> {
		let delegate = self.delegate();
		delegate
			.create_table_with_outcome(name, schema, row_identity, if_not_exists)
			.await
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		let delegate = self.delegate();
		delegate.drop_table(name, if_exists).await
	}
}

struct RegisteringLogicalTypeAdmin {
	catalog: Arc<MockCatalog>,
	definitions: Mutex<LogicalTypeCatalog>,
}

impl RegisteringLogicalTypeAdmin {
	fn new(catalog: Arc<MockCatalog>) -> Self {
		Self {
			catalog,
			definitions: Mutex::new(LogicalTypeCatalog::default()),
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
				wendelon::integration::compute::LogicalTypeDefinitionCreate {
					name: request.name,
					physical: request.physical,
					refinement: request.refinement,
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

struct FailingStorageScan;

#[async_trait]
impl StorageScan for FailingStorageScan {
	async fn scan(&self, _request: ScanRequest) -> Result<ScanStream, StorageError> {
		Err(StorageError::Internal {
			message: "system table query should not scan storage".to_string(),
		})
	}
}

struct RefreshingLogicalTypeCatalog {
	inner: Arc<MockCatalog>,
	tenant_id: TenantId,
	definitions: Vec<LogicalTypeDefinition>,
	refreshes: Arc<AtomicU64>,
}

impl RefreshingLogicalTypeCatalog {
	fn delegate(&self) -> Arc<dyn Catalog> {
		Arc::clone(&self.inner).scoped(self.tenant_id)
	}
}

#[async_trait]
impl Catalog for RefreshingLogicalTypeCatalog {
	fn scoped(self: Arc<Self>, tenant_id: TenantId) -> Arc<dyn Catalog> {
		Arc::new(Self {
			inner: Arc::clone(&self.inner),
			tenant_id,
			definitions: self.definitions.clone(),
			refreshes: Arc::clone(&self.refreshes),
		})
	}

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		let delegate = self.delegate();
		delegate.descriptor_refresh_metrics()
	}

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let delegate = self.delegate();
		delegate.resolve_table(table).await
	}

	async fn resolve_table_local(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let delegate = self.delegate();
		delegate.resolve_table_local(table).await
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		let delegate = self.delegate();
		delegate.resolve_table_id(table).await
	}

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<toron::ir::LogicalTypeSpec, CatalogError> {
		let delegate = self.delegate();
		delegate.resolve_durable_logical_type_by_name(name).await
	}

	fn logical_type_registry(&self) -> toron::ir::LogicalTypeRegistry {
		let delegate = self.delegate();
		delegate.logical_type_registry()
	}

	async fn refreshed_logical_type_registry(
		&self,
	) -> Result<toron::ir::LogicalTypeRegistry, CatalogError> {
		self.refreshes.fetch_add(1, Ordering::SeqCst);
		for definition in &self.definitions {
			self.inner
				.register_logical_type_for_tenant(self.tenant_id, definition.clone())?;
		}
		Ok(self.delegate().logical_type_registry())
	}

	async fn invalidate_table_descriptor(
		&self,
		table: TableId,
	) -> Result<Option<toron::catalog::InvalidatedDescriptor>, CatalogError> {
		let delegate = self.delegate();
		delegate.invalidate_table_descriptor(table).await
	}

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: Arc<Schema>,
		row_identity: toron::ir::CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError> {
		let delegate = self.delegate();
		delegate
			.create_table_with_outcome(name, schema, row_identity, if_not_exists)
			.await
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		let delegate = self.delegate();
		delegate.drop_table(name, if_exists).await
	}
}
