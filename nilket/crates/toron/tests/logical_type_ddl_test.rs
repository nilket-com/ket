use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
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
	BoundExprNode, BoundExprProgram, CUSTOMER_ID_LOGICAL_TYPE, ColumnOrdinal,
	CreateTableColumnType, DURABLE_LOGICAL_TYPE_MIN_ID, ExecutionPlan, Expr,
	LOGICAL_TYPE_ID_METADATA_KEY, LogicalPlan, LogicalTypeId, ScalarValue, TableId,
	decode_column_logical_type, field_with_logical_type_metadata,
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
	SchemaEvolutionPolicy, TableObjectId, TableSchemaCatalog, TableSchemaDescriptor,
	TenantId as WendelonTenantId,
};
use wendelon_client::{
	LogicalTypeDefinition, LogicalTypePhysical, LogicalTypeRefinementComparisonOp,
	LogicalTypeRefinementPredicate, LogicalTypeRefinementValue, UnitOffset, UnitQuantityKind,
	UnitScale,
};

const TEMPERATURE_CELSIUS_DELTA_ID: LogicalTypeId = LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID);
const TEMPERATURE_CELSIUS_ID: LogicalTypeId = LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 2);
const TEMPERATURE_KELVIN_ID: LogicalTypeId = LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 3);

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
		unit,
	} = plan
	else {
		panic!("expected refined create logical type");
	};

	assert_eq!(name, "PersonId");
	assert_eq!(physical, DataType::Int64);
	assert!(unit.is_none());
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
fn ddl_create_logical_type_unit_metadata_lowers() {
	let plan = SqlPlanner::new()
		.plan("CREATE LOGICAL TYPE Mile AS DOUBLE DIMENSION Length SCALE_TO_BASE 1609.344")
		.expect("plan dimensioned logical type");

	let LogicalPlan::CreateLogicalType {
		name,
		physical,
		refinement,
		unit: Some(unit),
	} = plan
	else {
		panic!("expected dimensioned create logical type");
	};

	assert_eq!(name, "Mile");
	assert_eq!(physical, DataType::Float64);
	assert!(refinement.is_none());
	assert_eq!(unit.dimension, "Length");
	assert_eq!(
		unit.scale_to_base,
		UnitScale {
			numerator: 201168,
			denominator: 125,
		}
	);
	assert_eq!(unit.quantity_kind, UnitQuantityKind::Delta);
	assert_eq!(unit.offset_to_base, UnitOffset::ZERO);
	assert_eq!(unit.delta_unit, None);
}

#[test]
fn ddl_create_logical_type_affine_metadata_lowers() {
	let plan = SqlPlanner::new()
		.plan(
			"CREATE LOGICAL TYPE Celsius AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 OFFSET_TO_BASE 273.15 POINT DELTA_UNIT CelsiusDelta",
		)
		.expect("plan affine dimensioned logical type");

	let LogicalPlan::CreateLogicalType {
		name,
		physical,
		refinement,
		unit: Some(unit),
	} = plan
	else {
		panic!("expected affine create logical type");
	};

	assert_eq!(name, "Celsius");
	assert_eq!(physical, DataType::Float64);
	assert!(refinement.is_none());
	assert_eq!(unit.dimension, "Temperature");
	assert_eq!(
		unit.scale_to_base,
		UnitScale {
			numerator: 1,
			denominator: 1,
		}
	);
	assert_eq!(unit.quantity_kind, UnitQuantityKind::Point);
	assert_eq!(
		unit.offset_to_base,
		UnitOffset {
			numerator: 5463,
			denominator: 20,
		}
	);
	assert_eq!(unit.delta_unit.as_deref(), Some("CelsiusDelta"));
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

#[test]
fn ddl_create_logical_type_unit_metadata_rejects_non_float64_and_invalid_scale() {
	for (sql, diagnostic) in [
		(
			"CREATE LOGICAL TYPE Bad AS BIGINT DIMENSION Length SCALE_TO_BASE 1",
			"unit metadata requires",
		),
		(
			"CREATE LOGICAL TYPE Bad AS DOUBLE DIMENSION Length SCALE_TO_BASE 0",
			"must be greater than zero",
		),
		(
			"CREATE LOGICAL TYPE Bad AS DOUBLE DIMENSION app.Length SCALE_TO_BASE 1",
			"one-part unquoted ASCII identifier",
		),
		(
			"CREATE LOGICAL TYPE Bad AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 OFFSET_TO_BASE 273.15",
			"nonzero OFFSET_TO_BASE requires POINT",
		),
		(
			"CREATE LOGICAL TYPE Bad AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 OFFSET_TO_BASE 273.15 DELTA",
			"nonzero OFFSET_TO_BASE requires POINT",
		),
		(
			"CREATE LOGICAL TYPE Bad AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 DELTA DELTA_UNIT CelsiusDelta",
			"DELTA_UNIT requires POINT",
		),
		(
			"CREATE LOGICAL TYPE Bad AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 DELTA_UNIT CelsiusDelta",
			"DELTA_UNIT requires POINT",
		),
		(
			"CREATE LOGICAL TYPE Bad AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 POINT DELTA_UNIT",
			"DELTA_UNIT requires an unquoted logical type identifier",
		),
		(
			"CREATE LOGICAL TYPE Bad AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 POINT DELTA_UNIT app.CelsiusDelta",
			"must be unquoted ASCII identifiers",
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
async fn delta_unit_pairing_validated_at_create() {
	let (_catalog, coordinator, planner) = logical_type_writer_coordinator();

	expect_query_error_contains(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE ForwardPoint AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 POINT DELTA_UNIT ForwardDelta",
		"not found",
	)
	.await;

	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE CelsiusDelta AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 DELTA",
	)
	.await;
	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE Celsius AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 OFFSET_TO_BASE 273.15 POINT DELTA_UNIT CelsiusDelta",
	)
	.await;

	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE Kelvin AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 POINT",
	)
	.await;
	expect_query_error_contains(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE WrongKind AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 POINT DELTA_UNIT Kelvin",
		"must be a Delta",
	)
	.await;

	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE LengthDelta AS DOUBLE DIMENSION Length SCALE_TO_BASE 1 DELTA",
	)
	.await;
	expect_query_error_contains(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE WrongDimension AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 POINT DELTA_UNIT LengthDelta",
		"dimension must match",
	)
	.await;

	execute(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE FahrenheitDelta AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 0.5555555555555556 DELTA",
	)
	.await;
	expect_query_error_contains(
		&coordinator,
		&planner,
		"CREATE LOGICAL TYPE WrongScale AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 POINT DELTA_UNIT FahrenheitDelta",
		"scale must match",
	)
	.await;
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
async fn case_uniform_logical_identity_propagates_to_projection_schema() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT CASE WHEN flag THEN customer_id ELSE customer_id END AS id FROM orders",
	)
	.await;

	assert_project_identity(bound, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));
}

#[tokio::test]
async fn case_missing_else_preserves_uniform_logical_identity() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT CASE WHEN flag THEN customer_id END AS id FROM orders",
	)
	.await;

	assert_project_identity(bound, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));
}

#[tokio::test]
async fn coalesce_logical_identity_preserves_through_null_fallback() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT COALESCE(NULL, customer_id) AS id FROM orders",
	)
	.await;

	assert_project_identity(bound, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));
}

#[tokio::test]
async fn if_logical_identity_preserves_through_null_branch() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT IF(flag, customer_id, NULL) AS id FROM orders",
	)
	.await;

	assert_project_identity(bound, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));
}

#[tokio::test]
async fn logical_cast_null_projects_typed_logical_null() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT CAST(NULL AS CustomerId), TRY_CAST(NULL AS CustomerId) FROM orders",
	)
	.await;
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};
	let customer_id = LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID);
	for idx in 0..2 {
		assert_eq!(exprs.root_type(idx), Some(&DataType::Int64));
		assert_eq!(exprs.root_nullable(idx), Some(true));
		assert_eq!(exprs.root_logical_type(idx), Some(customer_id));
		assert_eq!(
			decode_column_logical_type(output_schema.field(idx))
				.expect("decode logical NULL projection identity"),
			customer_id
		);
	}
}

#[tokio::test]
async fn case_literal_fallback_can_infer_logical_identity() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT CASE WHEN flag THEN customer_id ELSE 0 END AS id FROM orders",
	)
	.await;

	assert_project_identity(bound, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));
}

#[tokio::test]
async fn case_mixed_logical_identities_reject() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let err = bind_sql_error(
		&catalog,
		&planner,
		"SELECT CASE WHEN flag THEN customer_id ELSE product_id END FROM orders",
	)
	.await;

	assert!(matches!(
		err,
		BindingError::NominalTypeMismatch { physical, .. } if physical == "Int64"
	));
}

#[tokio::test]
async fn case_logical_identity_plus_naked_nonliteral_rejects() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let err = bind_sql_error(
		&catalog,
		&planner,
		"SELECT CASE WHEN flag THEN customer_id ELSE raw_bigint END FROM orders",
	)
	.await;

	assert!(matches!(
		err,
		BindingError::NominalTypeMismatch { physical, .. } if physical == "Int64"
	));
}

#[tokio::test]
async fn case_identity_branch_rejects_physical_widening() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let err = bind_sql_error(
		&catalog,
		&planner,
		"SELECT CASE WHEN flag THEN customer_id ELSE 1.5 END FROM orders",
	)
	.await;

	assert!(matches!(
		err,
		BindingError::TypeMismatch { ref message }
			if message.contains("CASE logical result")
				&& message.contains("cast explicitly to erase identity before widening")
	));
}

#[tokio::test]
async fn case_produced_identity_is_nominally_checked_downstream() {
	let (catalog, coordinator, planner) = logical_type_coordinator();
	setup_case_identity_orders(&coordinator, &planner).await;

	let err = bind_sql_error(
		&catalog,
		&planner,
		"SELECT CASE WHEN flag THEN customer_id ELSE customer_id END = product_id FROM orders",
	)
	.await;

	assert!(matches!(
		err,
		BindingError::NominalTypeMismatch { physical, .. } if physical == "Int64"
	));
}

#[tokio::test]
async fn case_computed_identity_does_not_refinement_elide() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT id FROM (SELECT CASE WHEN note IS NOT NULL THEN id ELSE id END AS id FROM people) AS p WHERE id < 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
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

#[test]
fn convert_planner_records_custom_target_without_catalog_resolution() {
	let plan = SqlPlanner::new()
		.plan("SELECT CONVERT(distance, Kilometer) AS km FROM trips")
		.expect("planner records custom CONVERT target");
	let LogicalPlan::Project { exprs, .. } = plan else {
		panic!("expected project");
	};
	let toron::ir::Expr::SqlConvert {
		target_logical_name,
		..
	} = &exprs[0]
	else {
		panic!("expected transient SQL CONVERT");
	};
	assert_eq!(target_logical_name, "Kilometer");
}

#[test]
fn convert_planner_rejects_bounded_invalid_logical_type_name_shapes() {
	for (sql, diagnostic) in [
		(
			"SELECT CONVERT(distance, naked) FROM trips",
			"naked sentinel",
		),
		(
			r#"SELECT CONVERT(distance, "Kilometer") FROM trips"#,
			"unquoted ASCII identifiers",
		),
		(
			"SELECT CONVERT(distance, app.Kilometer) FROM trips",
			"one-part names",
		),
		(
			"SELECT CONVERT(distance, Kilometer(1)) FROM trips",
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
async fn convert_cast_mile_to_kilometer_preserves_quantity_and_carries_target_identity() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT CONVERT(distance, Kilometer) AS km FROM trips",
	)
	.await;
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};
	let root = exprs.single_root().expect("single projection root");
	assert!(matches!(
		exprs.node(root),
		Some(BoundExprNode::Binary {
			op: toron::ir::BoundBinaryOp::Mul,
			..
		})
	));
	assert_eq!(exprs.root_type(0), Some(&DataType::Float64));
	assert_eq!(
		exprs.root_logical_type(0),
		Some(LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 1))
	);
	assert_eq!(
		decode_column_logical_type(output_schema.field(0)).expect("decode projection identity"),
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 1)
	);

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT CONVERT(distance, Kilometer) AS km FROM trips",
	)
	.await;
	let rows = float64_rows(&batch, 0);
	let [Some(value)] = rows.as_slice() else {
		panic!("expected single converted value");
	};
	assert!((*value - 1.609344).abs() < 1e-12);
}

#[tokio::test]
async fn convert_rejects_naked_source_nondimensioned_target_and_cross_dimension() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	for (sql, diagnostic) in [
		(
			"SELECT CONVERT(raw, Kilometer) FROM trips",
			"source must be a dimensioned",
		),
		(
			"SELECT CONVERT(distance, CustomerId) FROM trips",
			"target must be a dimensioned",
		),
		(
			"SELECT CONVERT(distance, Kilogram) FROM trips",
			"dimension mismatch",
		),
	] {
		let err = bind_sql_error(&catalog, &planner, sql).await;
		let message = format!("{err:?}");
		assert!(
			message.contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {message}"
		);
	}
}

#[tokio::test]
async fn convert_temperature_point_uses_affine_offset_and_preserves_target_identity() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	let sql = "SELECT CONVERT(celsius, Kelvin) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	assert_project_identity(bound, TEMPERATURE_KELVIN_ID);

	let batch = concat_query(&coordinator, &planner, sql).await;
	let rows = float64_rows(&batch, 0);
	let [Some(value)] = rows.as_slice() else {
		panic!("expected single Kelvin value");
	};
	assert!((*value - 273.15).abs() < 1e-12);
}

#[tokio::test]
async fn convert_fahrenheit_point_to_celsius_point_uses_offset() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	let sql = "SELECT CONVERT(fahrenheit, Celsius) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	assert_project_identity(bound, TEMPERATURE_CELSIUS_ID);

	let batch = concat_query(&coordinator, &planner, sql).await;
	let rows = float64_rows(&batch, 0);
	let [Some(value)] = rows.as_slice() else {
		panic!("expected single Celsius value");
	};
	assert!(
		value.abs() < 1e-10,
		"expected 32F to convert to 0C, got {value}"
	);
}

#[tokio::test]
async fn convert_temperature_delta_uses_scale_only() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	let sql = "SELECT CONVERT(fahrenheit_delta, CelsiusDelta) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	assert_project_identity(bound, TEMPERATURE_CELSIUS_DELTA_ID);

	let batch = concat_query(&coordinator, &planner, sql).await;
	let rows = float64_rows(&batch, 0);
	let [Some(value)] = rows.as_slice() else {
		panic!("expected single Celsius delta value");
	};
	assert!((*value - 5.0).abs() < 1e-10);
}

#[tokio::test]
async fn point_delta_convert_mismatch_rejects() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	for sql in [
		"SELECT CONVERT(celsius, CelsiusDelta) FROM temperature_samples",
		"SELECT CONVERT(celsius_delta, Celsius) FROM temperature_samples",
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, "point/delta mismatch").await;
	}
}

#[tokio::test]
async fn point_plus_point_and_point_scaling_still_reject() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	for sql in [
		"SELECT celsius + celsius FROM temperature_samples",
		"SELECT kelvin + kelvin FROM temperature_samples",
		"SELECT celsius * 2 FROM temperature_samples",
		"SELECT celsius / 2 FROM temperature_samples",
		"SELECT 2 * celsius FROM temperature_samples",
		"SELECT SUM(celsius) FROM temperature_samples",
		"SELECT SUM(kelvin) FROM temperature_samples",
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, "point/delta quantity algebra")
			.await;
	}
}

#[tokio::test]
async fn point_minus_point_yields_declared_delta() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	let sql = "SELECT CAST(37.0 AS Celsius) - CAST(36.0 AS Celsius) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	assert_project_identity(bound, TEMPERATURE_CELSIUS_DELTA_ID);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(1.0)]);
}

#[tokio::test]
async fn point_plus_delta_yields_point() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	let sql = "SELECT celsius + celsius_delta, celsius_delta + celsius, celsius - celsius_delta FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(schema.as_ref(), 0, TEMPERATURE_CELSIUS_ID);
	assert_schema_identity(schema.as_ref(), 1, TEMPERATURE_CELSIUS_ID);
	assert_schema_identity(schema.as_ref(), 2, TEMPERATURE_CELSIUS_ID);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(5.0)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(5.0)]);
	assert_eq!(float64_rows(&batch, 2), vec![Some(-5.0)]);
}

#[tokio::test]
async fn avg_of_points_is_point_sum_of_points_rejects() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	let sql = "SELECT AVG(celsius), AVG(kelvin) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(schema.as_ref(), 0, TEMPERATURE_CELSIUS_ID);
	assert_schema_identity(schema.as_ref(), 1, TEMPERATURE_KELVIN_ID);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(0.0)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(273.15)]);

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT SUM(celsius) FROM temperature_samples",
		"point/delta quantity algebra",
	)
	.await;
}

#[tokio::test]
async fn point_without_delta_unit_subtraction_rejects() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT kelvin - kelvin FROM temperature_samples",
		"has no declared delta unit",
	)
	.await;
}

#[tokio::test]
async fn cross_unit_point_subtraction_requires_convert() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT celsius - fahrenheit FROM temperature_samples",
		"CONVERT explicitly",
	)
	.await;

	let sql = "SELECT celsius - CONVERT(fahrenheit, Celsius) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	assert_project_identity(bound, TEMPERATURE_CELSIUS_DELTA_ID);
	let batch = concat_query(&coordinator, &planner, sql).await;
	let rows = float64_rows(&batch, 0);
	let [Some(value)] = rows.as_slice() else {
		panic!("expected single converted point delta");
	};
	assert!(value.abs() < 1e-10, "expected zero delta, got {value}");
}

#[tokio::test]
async fn mismatched_delta_arithmetic_rejects() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT celsius + fahrenheit_delta FROM temperature_samples",
		"not the declared delta unit",
	)
	.await;
	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT celsius_delta - celsius FROM temperature_samples",
		"delta minus point",
	)
	.await;

	let sql = "SELECT celsius + CONVERT(fahrenheit_delta, CelsiusDelta) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	assert_project_identity(bound, TEMPERATURE_CELSIUS_ID);
	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(5.0)]);
}

#[tokio::test]
async fn point_delta_roundtrip() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	let sql = "SELECT celsius + (celsius - celsius) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	assert_project_identity(bound, TEMPERATURE_CELSIUS_ID);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(0.0)]);
}

#[tokio::test]
async fn point_comparison_and_selection_stay_valid() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_temperature_samples(&coordinator, &planner).await;

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT celsius = CAST(0.0 AS Celsius), CONVERT(fahrenheit, Celsius) < CAST(1.0 AS Celsius), kelvin > kelvin FROM temperature_samples",
	)
	.await;
	assert_eq!(bool_rows(&batch, 0), vec![Some(true)]);
	assert_eq!(bool_rows(&batch, 1), vec![Some(true)]);
	assert_eq!(bool_rows(&batch, 2), vec![Some(false)]);

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT celsius > fahrenheit FROM temperature_samples",
		"require explicit CONVERT",
	)
	.await;

	let sql =
		"SELECT MIN(celsius), MAX(celsius), COUNT(celsius), MIN(kelvin) FROM temperature_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(schema.as_ref(), 0, TEMPERATURE_CELSIUS_ID);
	assert_schema_identity(schema.as_ref(), 1, TEMPERATURE_CELSIUS_ID);
	assert_schema_identity(schema.as_ref(), 2, LogicalTypeId::NAKED);
	assert_schema_identity(schema.as_ref(), 3, TEMPERATURE_KELVIN_ID);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(0.0)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(0.0)]);
	assert_eq!(int64_rows(&batch, 2), vec![Some(1)]);
	assert_eq!(float64_rows(&batch, 3), vec![Some(273.15)]);
}

#[tokio::test]
async fn same_unit_add_preserves_unit() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	let sum = bind_sql(&catalog, &planner, "SELECT distance + distance FROM trips").await;
	assert_project_identity(sum, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));
	let sum = concat_query(
		&coordinator,
		&planner,
		"SELECT distance + distance FROM trips",
	)
	.await;
	assert_eq!(float64_rows(&sum, 0), vec![Some(2.0)]);
}

#[tokio::test]
async fn same_dimension_different_unit_arithmetic_requires_convert() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT distance + other_distance FROM trips",
		"require explicit CONVERT",
	)
	.await;
}

#[tokio::test]
async fn cross_dimension_arithmetic_rejects() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT distance + mass FROM trips",
		"dimension mismatch",
	)
	.await;
}

#[tokio::test]
async fn unit_plus_naked_numeric_rejects() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	for sql in [
		"SELECT distance + raw FROM trips",
		"SELECT distance + 3.0 FROM trips",
		"SELECT 3.0 + distance FROM trips",
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, "requires both operands").await;
	}
}

#[tokio::test]
async fn dimensioned_scalar_multiplication_preserves_unit() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	for (sql, expected) in [
		("SELECT distance * 2.0 FROM trips", 2.0),
		("SELECT 2.0 * distance FROM trips", 2.0),
		("SELECT distance / 2.0 FROM trips", 0.5),
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert_project_identity(bound, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));
		let batch = concat_query(&coordinator, &planner, sql).await;
		assert_eq!(float64_rows(&batch, 0), vec![Some(expected)]);
	}
}

#[tokio::test]
async fn derived_dimension_multiplication_is_deferred() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	for (sql, diagnostic) in [
		(
			"SELECT distance * distance FROM trips",
			"derived dimensions",
		),
		("SELECT distance / distance FROM trips", "creates ratios"),
		(
			"SELECT 2.0 / distance FROM trips",
			"naked numeric divided by a unit",
		),
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, diagnostic).await;
	}
}

#[tokio::test]
async fn dimensioned_comparison_requires_same_unit_or_explicit_convert() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_dimensioned_trips(&coordinator, &planner).await;

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT distance > CAST(0.5 AS Mile) FROM trips",
	)
	.await;
	assert_eq!(bool_rows(&batch, 0), vec![Some(true)]);

	for (sql, diagnostic) in [
		(
			"SELECT distance > CAST(1.0 AS Kilometer) FROM trips",
			"require explicit CONVERT",
		),
		(
			"SELECT distance > raw FROM trips",
			"unit comparison requires",
		),
	] {
		let err = bind_sql_error(&catalog, &planner, sql).await;
		let message = format!("{err:?}");
		assert!(
			message.contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {message}"
		);
	}
}

#[tokio::test]
async fn aggregation_is_type_unit_aware() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let sql = "SELECT MIN(customer_id), SUM(distance), COUNT(distance) FROM aggregate_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(
		schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 3),
	);
	assert_schema_identity(
		schema.as_ref(),
		1,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
	assert_schema_identity(schema.as_ref(), 2, LogicalTypeId::NAKED);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(int64_rows(&batch, 0), vec![Some(7)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(3.0)]);
	assert_eq!(int64_rows(&batch, 2), vec![Some(2)]);
}

#[tokio::test]
async fn quantity_addition_and_aggregation_still_work() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let addition_sql = "SELECT distance + distance FROM aggregate_samples";
	let addition = bind_sql(&catalog, &planner, addition_sql).await;
	assert_project_identity(addition, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));
	let addition = concat_query(&coordinator, &planner, addition_sql).await;
	assert_eq!(float64_rows(&addition, 0), vec![Some(2.0), Some(4.0)]);

	let sql = "SELECT SUM(distance), AVG(distance) FROM aggregate_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(
		schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
	assert_schema_identity(
		schema.as_ref(),
		1,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(3.0)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(1.5)]);
}

#[tokio::test]
async fn aggregate_unit_convert_preserves_quantity() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let sql = "SELECT CONVERT(SUM(distance), Kilometer) FROM aggregate_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(
		schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 1),
	);

	let batch = concat_query(&coordinator, &planner, sql).await;
	let rows = float64_rows(&batch, 0);
	let [Some(value)] = rows.as_slice() else {
		panic!("expected single converted aggregate value");
	};
	assert!((*value - 4.828032).abs() < 1e-12);
}

#[tokio::test]
async fn sum_after_convert_preserves_target_unit() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let sql = "SELECT SUM(CONVERT(distance, Kilometer)) FROM aggregate_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(
		schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 1),
	);

	let batch = concat_query(&coordinator, &planner, sql).await;
	let rows = float64_rows(&batch, 0);
	let [Some(value)] = rows.as_slice() else {
		panic!("expected single converted aggregate value");
	};
	assert!((*value - 4.828032).abs() < 1e-12);
}

#[tokio::test]
async fn pruned_sum_unit_identity_matches_full_input_aggregate() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let full = bind_sql(
		&catalog,
		&planner,
		"SELECT MIN(customer_id), SUM(distance), SUM(other_distance), SUM(mass), SUM(raw) FROM aggregate_samples",
	)
	.await;
	let full_identity =
		decode_column_logical_type(full.schema().field(1)).expect("decode full aggregate identity");
	assert_eq!(full_identity, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));

	let pruned_sql = "SELECT SUM(distance) FROM aggregate_samples";
	let pruned = bind_sql(&catalog, &planner, pruned_sql).await;
	let pruned_identity = decode_column_logical_type(pruned.schema().field(0))
		.expect("decode pruned aggregate identity");
	assert_eq!(pruned_identity, full_identity);
	assert!(
		contains_scan_projection(&pruned, &[ColumnOrdinal(1)]),
		"expected single-column distance scan projection after pruning, got {pruned:?}"
	);

	let batch = concat_query(&coordinator, &planner, pruned_sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(3.0)]);
}

#[tokio::test]
async fn min_max_preserve_unit_identity() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let sql = "SELECT MIN(distance), MAX(distance) FROM aggregate_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(
		schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
	assert_schema_identity(
		schema.as_ref(),
		1,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(1.0)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(2.0)]);
}

#[tokio::test]
async fn strict_identity_arithmetic_rejects_all_operators() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	for sql in [
		"SELECT customer_id + 1 FROM aggregate_samples",
		"SELECT customer_id + customer_id FROM aggregate_samples",
		"SELECT customer_id - 1 FROM aggregate_samples",
		"SELECT customer_id * 2 FROM aggregate_samples",
		"SELECT customer_id / 2 FROM aggregate_samples",
		"SELECT customer_id - customer_id FROM aggregate_samples",
		"SELECT customer_id * customer_id FROM aggregate_samples",
		"SELECT customer_id / customer_id FROM aggregate_samples",
		"SELECT SUM(customer_id) FROM aggregate_samples",
		"SELECT AVG(customer_id) FROM aggregate_samples",
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, "not an arithmetic quantity")
			.await;
	}
}

#[tokio::test]
async fn explicit_cast_erases_identity_for_all_arithmetic() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	for (sql, expected) in [
		(
			"SELECT CAST(customer_id AS BIGINT) + 1 FROM aggregate_samples",
			vec![Some(8), Some(8)],
		),
		(
			"SELECT CAST(customer_id AS BIGINT) - 1 FROM aggregate_samples",
			vec![Some(6), Some(6)],
		),
		(
			"SELECT CAST(customer_id AS BIGINT) * 2 FROM aggregate_samples",
			vec![Some(14), Some(14)],
		),
		(
			"SELECT CAST(customer_id AS BIGINT) / 2 FROM aggregate_samples",
			vec![Some(3), Some(3)],
		),
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert_project_identity(bound, LogicalTypeId::NAKED);

		let batch = concat_query(&coordinator, &planner, sql).await;
		assert_eq!(int64_rows(&batch, 0), expected);
	}
}

#[tokio::test]
async fn quantity_subtraction_and_scalar_scaling_unchanged() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	for (sql, expected) in [
		(
			"SELECT distance - distance FROM aggregate_samples",
			vec![Some(0.0), Some(0.0)],
		),
		(
			"SELECT distance * 2 FROM aggregate_samples",
			vec![Some(2.0), Some(4.0)],
		),
		(
			"SELECT 2 * distance FROM aggregate_samples",
			vec![Some(2.0), Some(4.0)],
		),
		(
			"SELECT distance / 2 FROM aggregate_samples",
			vec![Some(0.5), Some(1.0)],
		),
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert_project_identity(bound, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));

		let batch = concat_query(&coordinator, &planner, sql).await;
		assert_eq!(float64_rows(&batch, 0), expected);
	}

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT distance - other_distance FROM aggregate_samples",
		"require explicit CONVERT",
	)
	.await;
}

#[tokio::test]
async fn logical_null_arithmetic_rejects_for_all_operators() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	for sql in [
		"SELECT customer_id + NULL FROM aggregate_samples",
		"SELECT NULL + customer_id FROM aggregate_samples",
		"SELECT customer_id - NULL FROM aggregate_samples",
		"SELECT NULL - customer_id FROM aggregate_samples",
		"SELECT customer_id * NULL FROM aggregate_samples",
		"SELECT NULL * customer_id FROM aggregate_samples",
		"SELECT customer_id / NULL FROM aggregate_samples",
		"SELECT NULL / customer_id FROM aggregate_samples",
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, "not an arithmetic quantity")
			.await;
	}

	for sql in [
		"SELECT distance + NULL FROM aggregate_samples",
		"SELECT NULL + distance FROM aggregate_samples",
		"SELECT distance - NULL FROM aggregate_samples",
		"SELECT NULL - distance FROM aggregate_samples",
		"SELECT distance * NULL FROM aggregate_samples",
		"SELECT NULL * distance FROM aggregate_samples",
		"SELECT distance / NULL FROM aggregate_samples",
		"SELECT NULL / distance FROM aggregate_samples",
	] {
		expect_plan_or_bind_error_contains(
			&catalog,
			&planner,
			sql,
			"unit arithmetic with untyped NULL",
		)
		.await;
	}

	for sql in [
		"SELECT distance * CAST(NULL AS DOUBLE) FROM aggregate_samples",
		"SELECT CAST(NULL AS DOUBLE) * distance FROM aggregate_samples",
		"SELECT distance / CAST(NULL AS DOUBLE) FROM aggregate_samples",
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert_project_identity(bound, LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID));

		let batch = concat_query(&coordinator, &planner, sql).await;
		assert_eq!(float64_rows(&batch, 0), vec![None, None]);
	}
}

#[tokio::test]
async fn explicit_cast_sum_avg_erasure_stays_valid() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let sql = "SELECT SUM(CAST(customer_id AS BIGINT)), AVG(CAST(customer_id AS BIGINT)) FROM aggregate_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(schema.as_ref(), 0, LogicalTypeId::NAKED);
	assert_schema_identity(schema.as_ref(), 1, LogicalTypeId::NAKED);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(int64_rows(&batch, 0), vec![Some(14)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(7.0)]);
}

#[tokio::test]
async fn identity_comparison_and_selection_still_allowed() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let selection_sql = "SELECT customer_id FROM aggregate_samples WHERE customer_id = 7";
	let selection = bind_sql(&catalog, &planner, selection_sql).await;
	let schema = selection.schema();
	assert_schema_identity(
		schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 3),
	);
	let batch = concat_query(&coordinator, &planner, selection_sql).await;
	assert_eq!(int64_rows(&batch, 0), vec![Some(7), Some(7)]);

	let comparison_sql = "SELECT customer_id = customer_id FROM aggregate_samples";
	let comparison = bind_sql(&catalog, &planner, comparison_sql).await;
	assert_project_identity(comparison, LogicalTypeId::NAKED);
	let batch = concat_query(&coordinator, &planner, comparison_sql).await;
	assert_eq!(bool_rows(&batch, 0), vec![Some(true), Some(true)]);

	let aggregate_sql =
		"SELECT MIN(customer_id), MAX(customer_id), COUNT(customer_id) FROM aggregate_samples";
	let aggregate = bind_sql(&catalog, &planner, aggregate_sql).await;
	let schema = aggregate.schema();
	assert_schema_identity(
		schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 3),
	);
	assert_schema_identity(
		schema.as_ref(),
		1,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 3),
	);
	assert_schema_identity(schema.as_ref(), 2, LogicalTypeId::NAKED);
	let batch = concat_query(&coordinator, &planner, aggregate_sql).await;
	assert_eq!(int64_rows(&batch, 0), vec![Some(7)]);
	assert_eq!(int64_rows(&batch, 1), vec![Some(7)]);
	assert_eq!(int64_rows(&batch, 2), vec![Some(2)]);
}

#[tokio::test]
async fn naked_numeric_arithmetic_unchanged() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	for (sql, expected) in [
		(
			"SELECT raw + raw FROM aggregate_samples",
			vec![Some(4.0), Some(8.0)],
		),
		(
			"SELECT raw - raw FROM aggregate_samples",
			vec![Some(0.0), Some(0.0)],
		),
		(
			"SELECT raw * raw FROM aggregate_samples",
			vec![Some(4.0), Some(16.0)],
		),
		(
			"SELECT raw / raw FROM aggregate_samples",
			vec![Some(1.0), Some(1.0)],
		),
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert_project_identity(bound, LogicalTypeId::NAKED);
		let batch = concat_query(&coordinator, &planner, sql).await;
		assert_eq!(float64_rows(&batch, 0), expected);
	}

	let sql = "SELECT SUM(raw), AVG(raw), SUM(CAST(distance AS DOUBLE)) FROM aggregate_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(schema.as_ref(), 0, LogicalTypeId::NAKED);
	assert_schema_identity(schema.as_ref(), 1, LogicalTypeId::NAKED);
	assert_schema_identity(schema.as_ref(), 2, LogicalTypeId::NAKED);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(float64_rows(&batch, 0), vec![Some(6.0)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(3.0)]);
	assert_eq!(float64_rows(&batch, 2), vec![Some(3.0)]);
}

#[tokio::test]
async fn count_of_dimensioned_unit_is_naked() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let sql = "SELECT COUNT(distance), COUNT(*) FROM aggregate_samples";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(schema.as_ref(), 0, LogicalTypeId::NAKED);
	assert_schema_identity(schema.as_ref(), 1, LogicalTypeId::NAKED);
}

#[tokio::test]
async fn experimental_unit_aggregates_are_naked() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let sql = "SELECT LINEAR_REGRESS(distance, raw), APPROX_QUANTILE(distance, 0.5) FROM aggregate_samples";
	let config = PlannerConfig {
		allow_experimental_kernels: true,
		..PlannerConfig::default()
	};
	let bound = bind(
		&planner.plan(sql).expect("plan experimental aggregates"),
		catalog.as_ref(),
		&config,
	)
	.await
	.expect(sql);
	let schema = bound.schema();
	assert_schema_identity(schema.as_ref(), 0, LogicalTypeId::NAKED);
	assert_schema_identity(schema.as_ref(), 1, LogicalTypeId::NAKED);
}

#[tokio::test]
async fn aggregated_unit_composes_with_167_safety() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	for (sql, diagnostic) in [
		(
			"SELECT SUM(distance) + AVG(mass) FROM aggregate_samples",
			"dimension mismatch",
		),
		(
			"SELECT SUM(distance) + AVG(other_distance) FROM aggregate_samples",
			"require explicit CONVERT",
		),
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, diagnostic).await;
	}
}

#[tokio::test]
async fn group_by_with_type_unit_aggregate_preserves_identity() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	let sql = "SELECT customer_id, SUM(distance), MIN(customer_id) FROM aggregate_samples GROUP BY customer_id";
	let bound = bind_sql(&catalog, &planner, sql).await;
	let schema = bound.schema();
	assert_schema_identity(
		schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 3),
	);
	assert_schema_identity(
		schema.as_ref(),
		1,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
	assert_schema_identity(
		schema.as_ref(),
		2,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID + 3),
	);

	let batch = concat_query(&coordinator, &planner, sql).await;
	assert_eq!(int64_rows(&batch, 0), vec![Some(7)]);
	assert_eq!(float64_rows(&batch, 1), vec![Some(3.0)]);
	assert_eq!(int64_rows(&batch, 2), vec![Some(7)]);
}

#[tokio::test]
async fn unit_aggregate_identity_flows_through_ctas() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;
	execute(
		&coordinator,
		&planner,
		"CREATE TABLE aggregate_totals AS SELECT SUM(distance) AS total_distance FROM aggregate_samples",
	)
	.await;

	let binding = catalog
		.resolve_table(&TableRef::new("aggregate_totals"))
		.await
		.expect("CTAS aggregate target resolves");
	assert_schema_identity(
		binding.schema.as_ref(),
		0,
		LogicalTypeId(DURABLE_LOGICAL_TYPE_MIN_ID),
	);
}

#[tokio::test]
async fn sum_mixed_unit_expression_requires_convert_before_aggregate() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_type_unit_aggregate_samples(&coordinator, &planner).await;

	expect_plan_or_bind_error_contains(
		&catalog,
		&planner,
		"SELECT SUM(distance + other_distance) FROM aggregate_samples",
		"require explicit CONVERT",
	)
	.await;
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
				unit: None,
			},
		)
		.expect("sync logical type definition");
	resynced
		.register_descriptor_for_tenant(
			TenantId(1),
			binding.local_table_id,
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
async fn refinement_identity_arithmetic_rejects() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;
	execute(
		&coordinator,
		&planner,
		"INSERT INTO people VALUES (1, 'ok')",
	)
	.await;

	for sql in [
		"SELECT id + 0 FROM people",
		"SELECT id + id FROM people",
		"SELECT id - 1 FROM people",
		"SELECT id * 2 FROM people",
		"SELECT id / 2 FROM people",
		"SELECT SUM(id) FROM people",
		"SELECT AVG(id) FROM people",
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, "not an arithmetic quantity")
			.await;
	}

	for (sql, expected) in [
		("SELECT CAST(id AS BIGINT) + 0 FROM people", vec![Some(1)]),
		("SELECT CAST(id AS BIGINT) - 1 FROM people", vec![Some(0)]),
		("SELECT CAST(id AS BIGINT) * 2 FROM people", vec![Some(2)]),
		("SELECT CAST(id AS BIGINT) / 1 FROM people", vec![Some(1)]),
	] {
		let bound = bind_sql(&catalog, &planner, sql).await;
		assert_project_identity(bound, LogicalTypeId::NAKED);

		let batch = concat_query(&coordinator, &planner, sql).await;
		assert_eq!(int64_rows(&batch, 0), expected);
	}
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
		"SELECT id FROM (SELECT CAST(id AS BIGINT) + 0 AS id FROM people) AS p WHERE id < 0",
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
		"SELECT CAST(person_id AS BIGINT) + 1 FROM (SELECT id AS person_id FROM people) AS p WHERE person_id < 0",
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
		"SELECT computed FROM (SELECT CAST(id AS BIGINT) + 0 FROM people) AS p(computed) WHERE computed < 0",
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
async fn aggregate_derived_alias_columns_rename_outputs() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT pid, total FROM (SELECT id, COUNT(*) AS n FROM people GROUP BY id) AS p(pid, total)",
	)
	.await;

	assert_eq!(bound.schema().field(0).name(), "pid");
	assert_eq!(bound.schema().field(1).name(), "total");
	assert!(contains_aggregate(&bound));
}

#[tokio::test]
async fn aggregate_derived_outputs_are_not_refinement_transparent() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let bound = bind_sql(
		&catalog,
		&planner,
		"SELECT pid FROM (SELECT id AS pid, COUNT(*) AS n FROM people GROUP BY id) AS p WHERE pid < 0",
	)
	.await;

	assert!(find_empty_relation(&bound).is_none());
	assert!(contains_scan(&bound));
	assert!(contains_aggregate(&bound));
}

#[tokio::test]
async fn derived_table_source_scope_rejects_general_subquery_features() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	for sql in [
		"SELECT id FROM (SELECT id FROM people) WHERE id < 0",
		"SELECT id FROM LATERAL (SELECT id FROM people) AS p WHERE id < 0",
		"SELECT id FROM (SELECT id FROM (SELECT id FROM people) AS q) AS p WHERE id < 0",
		// ADR 254: a JOIN inside a derived table now lowers (presented as a named relation), so it is no
		// longer rejected here — its current logical-type bind gap is the ADR-265 known-gap test below.
		"SELECT id FROM (SELECT id FROM people) AS p LEFT JOIN people AS other ON p.id = other.id",
		"SELECT id FROM (WITH q AS (SELECT id FROM people) SELECT id FROM q) AS p WHERE id < 0",
		"SELECT id FROM (SELECT id FROM people UNION SELECT id FROM people) AS p WHERE id < 0",
	] {
		expect_plan_or_bind_error_contains(&catalog, &planner, sql, "derived").await;
	}
}

/// ADR 264 → ADR 265 known gap: a JOIN inside a derived table is supported (ADR 254 lowers it as a named
/// relation), but a *logical-typed* column does not yet survive the join-bodied derived-table schema
/// propagation — it errors at bind with `unknown logical type`. ADR 265 will thread the logical-type
/// registry through and flip this assertion to a positive bind+execute. Discriminating: it fails the day
/// 265 lands, forcing the update.
#[tokio::test]
async fn join_bodied_derived_table_with_logical_type_is_adr265_known_gap() {
	let (catalog, coordinator, planner) = logical_type_writer_coordinator();
	setup_person_id_people(&coordinator, &planner).await;

	let sql = "SELECT id FROM (SELECT people.id FROM people JOIN people AS other ON people.id = other.id) AS p WHERE id < 0";
	let plan = planner
		.plan(sql)
		.expect("ADR 254: a join inside a derived table plans");
	let error = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect_err(
			"ADR 265 gap: logical type does not yet propagate through a join-bodied derived table",
		);
	assert!(
		format!("{error:?}").contains("unknown logical type"),
		"the known gap is the logical-type propagation bind error, not a derived-table rejection: {error:?}"
	);
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
			arrow::datatypes::Field::new("event_date", DataType::Date32, false),
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
		vec![
			Some("analytics".to_string()),
			Some("analytics".to_string()),
			Some("analytics".to_string())
		]
	);
	assert_eq!(
		string_rows(&batch, 1),
		vec![
			Some("events".to_string()),
			Some("events".to_string()),
			Some("events".to_string())
		]
	);
	assert_eq!(
		string_rows(&batch, 2),
		vec![
			Some("event_id".to_string()),
			Some("event_name".to_string()),
			Some("event_date".to_string())
		]
	);
	assert_eq!(int64_rows(&batch, 3), vec![Some(1), Some(2), Some(3)]);
	assert_eq!(
		string_rows(&batch, 4),
		vec![
			Some("BIGINT".to_string()),
			Some("TEXT".to_string()),
			Some("DATE".to_string())
		]
	);
	assert_eq!(
		string_rows(&batch, 5),
		vec![
			Some("NO".to_string()),
			Some("YES".to_string()),
			Some("NO".to_string())
		]
	);
	assert_eq!(string_rows(&batch, 6), vec![None, None, None]);
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
async fn information_schema_schemata_lists_descriptor_namespaces_distinct_sorted() {
	let descriptors = vec![
		descriptor_from_schema_with_identity(
			14,
			1,
			"analytics",
			"events",
			&Schema::new(vec![arrow::datatypes::Field::new(
				"event_id",
				DataType::Int64,
				false,
			)]),
		),
		descriptor_from_schema_with_identity(
			15,
			1,
			"analytics",
			"clicks",
			&Schema::new(vec![arrow::datatypes::Field::new(
				"click_id",
				DataType::Int64,
				false,
			)]),
		),
		descriptor_from_schema_with_identity(
			16,
			1,
			"default",
			"people",
			&Schema::new(vec![arrow::datatypes::Field::new(
				"person_id",
				DataType::Int64,
				false,
			)]),
		),
	];
	let refreshes = Arc::new(AtomicU64::new(0));
	let catalog: Arc<dyn Catalog> = Arc::new(RefreshingTableDescriptorCatalog {
		inner: Arc::new(MockCatalog::new()),
		tenant_id: TenantId(1),
		descriptors,
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
		"SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
	)
	.await;

	assert_eq!(refreshes.load(Ordering::SeqCst), 1);
	assert_eq!(
		string_rows(&batch, 0),
		vec![Some("analytics".to_string()), Some("default".to_string())]
	);
}

#[tokio::test]
async fn information_schema_schemata_is_tenant_scoped() {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor_for_tenant(
			TenantId(1),
			TableId(151),
			descriptor_from_schema_with_identity(
				17,
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
			TableId(152),
			descriptor_from_schema_with_identity(
				18,
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
	let schemata_plan = planner
		.plan("SELECT schema_name FROM information_schema.schemata")
		.expect("plan schemata");
	let tenant_one = Arc::clone(&catalog).scoped(TenantId(1));
	let tenant_two = Arc::clone(&catalog).scoped(TenantId(2));

	let tenant_one_schemata = execute_bound_query(
		bind(
			&schemata_plan,
			tenant_one.as_ref(),
			&PlannerConfig::default(),
		)
		.await
		.unwrap(),
	)
	.await;
	let tenant_two_schemata = execute_bound_query(
		bind(
			&schemata_plan,
			tenant_two.as_ref(),
			&PlannerConfig::default(),
		)
		.await
		.unwrap(),
	)
	.await;

	assert_eq!(
		string_rows(&tenant_one_schemata, 0),
		vec![Some("tenant_one".to_string())]
	);
	assert_eq!(
		string_rows(&tenant_two_schemata, 0),
		vec![Some("tenant_two".to_string())]
	);
}

#[tokio::test]
async fn information_schema_schemata_is_latest_only() {
	let mut source = TableSchemaCatalog::default();
	let tenant = WendelonTenantId::new(1);
	source
		.upsert(
			tenant,
			descriptor_from_schema_with_identity(
				19,
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
				19,
				2,
				"analytics",
				"events",
				&Schema::new(vec![arrow::datatypes::Field::new(
					"event_id",
					DataType::Int64,
					false,
				)]),
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

	let batch = concat_query(
		&coordinator,
		&planner,
		"SELECT schema_name FROM information_schema.schemata",
	)
	.await;

	assert_eq!(string_rows(&batch, 0), vec![Some("analytics".to_string())]);
}

#[tokio::test]
async fn information_schema_schemata_empty_catalog_has_no_rows() {
	let catalog: Arc<dyn Catalog> = Arc::new(RefreshingTableDescriptorCatalog {
		inner: Arc::new(MockCatalog::new()),
		tenant_id: TenantId(1),
		descriptors: Vec::new(),
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
		"SELECT schema_name FROM information_schema.schemata",
	)
	.await;

	assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn information_schema_schemata_listing_failure_fails_query_without_stale_fallback() {
	let mut inner = MockCatalog::new();
	inner
		.register_descriptor_for_tenant(
			TenantId(1),
			TableId(153),
			descriptor_from_schema_with_identity(
				20,
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
			.plan("SELECT schema_name FROM information_schema.schemata")
			.expect("plan schemata"),
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
async fn information_schema_schemata_composes_with_query_ops() {
	let descriptors = vec![
		descriptor_from_schema_with_identity(
			23,
			1,
			"analytics",
			"events",
			&Schema::new(vec![arrow::datatypes::Field::new(
				"event_id",
				DataType::Int64,
				false,
			)]),
		),
		descriptor_from_schema_with_identity(
			24,
			1,
			"default",
			"people",
			&Schema::new(vec![arrow::datatypes::Field::new(
				"person_id",
				DataType::Int64,
				false,
			)]),
		),
	];
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

	let filtered = concat_query(
		&coordinator,
		&planner,
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name <> 'default' ORDER BY schema_name",
	)
	.await;
	let counted = concat_query(
		&coordinator,
		&planner,
		"SELECT COUNT(*) FROM information_schema.schemata",
	)
	.await;

	assert_eq!(
		string_rows(&filtered, 0),
		vec![Some("analytics".to_string())]
	);
	assert_eq!(int64_rows(&counted, 0), vec![Some(2)]);
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
	let mut source = TableSchemaCatalog::default();
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

	for table in ["logical_types", "tables", "columns", "schemata"] {
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
			.plan("SELECT * FROM information_schema.views")
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
		"CREATE TABLE information_schema.schemata (schema_name TEXT)",
		"CREATE TABLE information_schema.copy AS SELECT name FROM information_schema.logical_types",
		"DROP TABLE information_schema.logical_types",
		"DROP TABLE information_schema.columns",
		"DROP TABLE information_schema.schemata",
		"INSERT INTO information_schema.logical_types VALUES (1, 'X', 'BIGINT', NULL)",
		"INSERT INTO information_schema.tables VALUES ('public', 't', 'BASE TABLE')",
		"INSERT INTO information_schema.schemata VALUES ('analytics')",
		"UPDATE information_schema.logical_types SET name = 'X'",
		"UPDATE information_schema.columns SET column_name = 'X'",
		"UPDATE information_schema.schemata SET schema_name = 'X'",
		"DELETE FROM information_schema.logical_types",
		"DELETE FROM information_schema.tables",
		"DELETE FROM information_schema.schemata",
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
	let schemata = concat_query(
		&coordinator,
		&planner,
		"SELECT schema_name FROM information_schema.schemata",
	)
	.await;

	assert_eq!(
		string_rows(&logical_types, 0),
		vec![Some("PersonId".to_string())]
	);
	assert_eq!(string_rows(&tables, 0), vec![Some("events".to_string())]);
	assert_eq!(string_rows(&columns, 0), vec![Some("event_id".to_string())]);
	assert_eq!(
		string_rows(&schemata, 0),
		vec![Some("analytics".to_string())]
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

async fn setup_person_id_people(coordinator: &QueryCoordinator, planner: &SqlPlanner) {
	for sql in [
		"CREATE LOGICAL TYPE PersonId AS BIGINT CHECK (VALUE > 0)",
		"CREATE TABLE people (id PersonId, note TEXT)",
	] {
		execute(coordinator, planner, sql).await;
	}
}

async fn setup_case_identity_orders(coordinator: &QueryCoordinator, planner: &SqlPlanner) {
	for sql in [
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
		"CREATE LOGICAL TYPE ProductId AS BIGINT",
		"CREATE TABLE orders (customer_id CustomerId, product_id ProductId, raw_bigint BIGINT, flag BOOLEAN)",
	] {
		execute(coordinator, planner, sql).await;
	}
}

async fn setup_dimensioned_trips(coordinator: &QueryCoordinator, planner: &SqlPlanner) {
	for sql in [
		"CREATE LOGICAL TYPE Mile AS DOUBLE DIMENSION Length SCALE_TO_BASE 1609.344",
		"CREATE LOGICAL TYPE Kilometer AS DOUBLE DIMENSION Length SCALE_TO_BASE 1000",
		"CREATE LOGICAL TYPE Kilogram AS DOUBLE DIMENSION Mass SCALE_TO_BASE 1",
		"CREATE LOGICAL TYPE CustomerId AS BIGINT",
		"CREATE TABLE trips (distance Mile, other_distance Kilometer, mass Kilogram, raw DOUBLE)",
		"INSERT INTO trips VALUES (1.0, 1.0, 1.0, 2.0)",
	] {
		execute(coordinator, planner, sql).await;
	}
}

async fn setup_type_unit_aggregate_samples(coordinator: &QueryCoordinator, planner: &SqlPlanner) {
	setup_dimensioned_trips(coordinator, planner).await;
	for sql in [
		"CREATE TABLE aggregate_samples (customer_id CustomerId, distance Mile, other_distance Kilometer, mass Kilogram, raw DOUBLE)",
		"INSERT INTO aggregate_samples VALUES (7, 1.0, 1.0, 1.0, 2.0)",
		"INSERT INTO aggregate_samples VALUES (7, 2.0, 2.0, 3.0, 4.0)",
	] {
		execute(coordinator, planner, sql).await;
	}
}

async fn setup_temperature_samples(coordinator: &QueryCoordinator, planner: &SqlPlanner) {
	for sql in [
		"CREATE LOGICAL TYPE CelsiusDelta AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 DELTA",
		"CREATE LOGICAL TYPE FahrenheitDelta AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 0.5555555555555556 DELTA",
		"CREATE LOGICAL TYPE Celsius AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 OFFSET_TO_BASE 273.15 POINT DELTA_UNIT CelsiusDelta",
		"CREATE LOGICAL TYPE Kelvin AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 1 POINT",
		"CREATE LOGICAL TYPE Fahrenheit AS DOUBLE DIMENSION Temperature SCALE_TO_BASE 0.5555555555555556 OFFSET_TO_BASE 255.3722222222222 POINT DELTA_UNIT FahrenheitDelta",
		"CREATE TABLE temperature_samples (celsius Celsius, kelvin Kelvin, fahrenheit Fahrenheit, celsius_delta CelsiusDelta, fahrenheit_delta FahrenheitDelta)",
		"INSERT INTO temperature_samples VALUES (0.0, 273.15, 32.0, 5.0, 9.0)",
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

async fn bind_sql_error(catalog: &MockCatalog, planner: &SqlPlanner, sql: &str) -> BindingError {
	bind(
		&planner.plan(sql).expect("plan SQL"),
		catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect_err(sql)
}

fn assert_project_identity(bound: ExecutionPlan, expected: LogicalTypeId) {
	let ExecutionPlan::Project {
		exprs,
		output_schema,
		..
	} = bound
	else {
		panic!("expected project");
	};
	assert_eq!(exprs.root_logical_type(0), Some(expected));
	assert_eq!(
		decode_column_logical_type(output_schema.field(0))
			.expect("decode projection logical identity"),
		expected
	);
}

fn assert_schema_identity(schema: &Schema, index: usize, expected: LogicalTypeId) {
	assert_eq!(
		decode_column_logical_type(schema.field(index)).expect("decode field logical identity"),
		expected
	);
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
		unit: None,
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

fn float64_rows(batch: &RecordBatch, column: usize) -> Vec<Option<f64>> {
	batch
		.column(column)
		.as_any()
		.downcast_ref::<Float64Array>()
		.expect("Float64 column")
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

fn contains_scan_projection(plan: &ExecutionPlan, expected: &[ColumnOrdinal]) -> bool {
	match plan {
		ExecutionPlan::Scan { projection, .. } => projection.as_deref() == Some(expected),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => contains_scan_projection(input, expected),
		ExecutionPlan::Join { left, right, .. } => {
			contains_scan_projection(left, expected) || contains_scan_projection(right, expected)
		}
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => contains_scan_projection(source, expected),
		_ => false,
	}
}

fn contains_aggregate(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Aggregate { .. } => true,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => contains_aggregate(input),
		ExecutionPlan::Join { left, right, .. } => {
			contains_aggregate(left) || contains_aggregate(right)
		}
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => contains_aggregate(source),
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
		SchemaEvolutionPolicy::AdditiveOnly,
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
					unit: request.unit,
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
