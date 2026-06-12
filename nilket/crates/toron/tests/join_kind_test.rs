use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use toron::binder::{PlannerConfig, bind, join_schema};
use toron::catalog::{MockCatalog, TableRef};
use toron::error::BindingError;
use toron::ir::{
	BroadcastBuildSide, CUSTOMER_ID_LOGICAL_TYPE, Expr, JoinKind, JoinStrategy, JoinStrategyIntent,
	JoinStrategyPinSource, LogicalPlan, LogicalTypeId, PRODUCT_ID_LOGICAL_TYPE, TableId,
};
use toron::sql_planner::SqlPlanner;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

#[test]
fn join_kind_serde_round_trips_all_eight_variants() {
	for kind in all_join_kinds() {
		let encoded = postcard::to_allocvec(&kind).expect("encode kind");
		let decoded: JoinKind = postcard::from_bytes(&encoded).expect("decode kind");

		assert_eq!(decoded, kind);
	}
}

#[test]
fn output_schema_nullability_table_for_each_kind() {
	let left = Arc::new(Schema::new(vec![
		Field::new("left_id", DataType::Int64, false),
		Field::new("left_value", DataType::Utf8, true),
	]));
	let right = Arc::new(Schema::new(vec![
		Field::new("right_id", DataType::Int64, false),
		Field::new("right_value", DataType::Int32, false),
	]));

	assert_schema(
		&join_schema(JoinKind::Inner, &left, &right),
		4,
		&[false, true, false, false],
	);
	assert_schema(
		&join_schema(JoinKind::LeftOuter, &left, &right),
		4,
		&[false, true, true, true],
	);
	assert_schema(
		&join_schema(JoinKind::RightOuter, &left, &right),
		4,
		&[true, true, false, false],
	);
	assert_schema(
		&join_schema(JoinKind::FullOuter, &left, &right),
		4,
		&[true, true, true, true],
	);
	assert_schema(
		&join_schema(JoinKind::LeftSemi, &left, &right),
		2,
		&[false, true],
	);
	assert_schema(
		&join_schema(JoinKind::LeftAnti, &left, &right),
		2,
		&[false, true],
	);
	assert_schema(
		&join_schema(JoinKind::RightSemi, &left, &right),
		2,
		&[false, false],
	);
	assert_schema(
		&join_schema(JoinKind::RightAnti, &left, &right),
		2,
		&[false, false],
	);
}

#[tokio::test]
async fn bind_join_applies_broadcast_side_matrix_for_non_inner_kind() {
	let catalog = catalog();
	for (kind, safe_side, rejected_side) in [
		(
			JoinKind::LeftOuter,
			BroadcastBuildSide::Right,
			BroadcastBuildSide::Left,
		),
		(
			JoinKind::LeftSemi,
			BroadcastBuildSide::Right,
			BroadcastBuildSide::Left,
		),
		(
			JoinKind::LeftAnti,
			BroadcastBuildSide::Right,
			BroadcastBuildSide::Left,
		),
		(
			JoinKind::RightOuter,
			BroadcastBuildSide::Left,
			BroadcastBuildSide::Right,
		),
		(
			JoinKind::RightSemi,
			BroadcastBuildSide::Left,
			BroadcastBuildSide::Right,
		),
		(
			JoinKind::RightAnti,
			BroadcastBuildSide::Left,
			BroadcastBuildSide::Right,
		),
	] {
		bind(
			&broadcast_join_plan(kind, safe_side),
			&catalog,
			&PlannerConfig::default(),
		)
		.await
		.unwrap_or_else(|error| panic!("{kind:?} safe-side broadcast should bind: {error}"));

		let err = bind(
			&broadcast_join_plan(kind, rejected_side),
			&catalog,
			&PlannerConfig::default(),
		)
		.await
		.expect_err("preserved-side broadcast must fail");
		assert!(matches!(
			err,
			BindingError::UnsupportedExpression { message }
				if message.contains("target the null-supplied side or drop the hint")
		));
	}

	for side in [BroadcastBuildSide::Left, BroadcastBuildSide::Right] {
		let err = bind(
			&broadcast_join_plan(JoinKind::FullOuter, side),
			&catalog,
			&PlannerConfig::default(),
		)
		.await
		.expect_err("full outer broadcast must fail without demotion");
		assert!(matches!(
			err,
			BindingError::UnsupportedExpression { message }
				if message.contains("FullOuter requires demotion before broadcast can be pinned")
		));
	}
}

#[tokio::test]
async fn bind_join_promotes_outer_and_semi_anti_schemas() {
	let catalog = catalog();

	for (kind, expected_len, expected_nullable) in [
		(JoinKind::LeftOuter, 4, vec![false, true, true, true]),
		(JoinKind::RightOuter, 4, vec![true, true, false, false]),
		(JoinKind::FullOuter, 4, vec![true, true, true, true]),
		(JoinKind::LeftSemi, 2, vec![false, true]),
		(JoinKind::RightSemi, 2, vec![false, false]),
		(JoinKind::LeftAnti, 2, vec![false, true]),
		(JoinKind::RightAnti, 2, vec![false, false]),
	] {
		let bound = bind(&hash_join_plan(kind), &catalog, &PlannerConfig::default())
			.await
			.expect("bind join");

		assert_schema(&bound.schema(), expected_len, &expected_nullable);
	}
}

#[tokio::test]
async fn pure_join_gate_rejects_nominal_mismatch() {
	let err = bind(
		&nominal_join_plan(CUSTOMER_ID_LOGICAL_TYPE, PRODUCT_ID_LOGICAL_TYPE),
		&catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("mismatched logical join keys must reject");

	assert!(matches!(
		err,
		BindingError::NominalTypeMismatch { left, right, physical }
			if left == "CustomerId" && right == "ProductId" && physical == "Int64"
	));
}

#[tokio::test]
async fn pure_join_gate_accepts_nominal_match() {
	bind(
		&nominal_join_plan(CUSTOMER_ID_LOGICAL_TYPE, CUSTOMER_ID_LOGICAL_TYPE),
		&catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect("matching logical join keys should bind");
}

#[test]
fn sql_outer_semi_anti_joins_lower_to_join_kind() {
	for (sql, expected) in [
		(
			"SELECT * FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id",
			JoinKind::LeftOuter,
		),
		(
			"SELECT * FROM left_t RIGHT JOIN right_t ON left_t.id = right_t.id",
			JoinKind::RightOuter,
		),
		(
			"SELECT * FROM left_t FULL JOIN right_t ON left_t.id = right_t.id",
			JoinKind::FullOuter,
		),
		(
			"SELECT * FROM left_t SEMI JOIN right_t ON left_t.id = right_t.id",
			JoinKind::LeftSemi,
		),
		(
			"SELECT * FROM left_t LEFT SEMI JOIN right_t ON left_t.id = right_t.id",
			JoinKind::LeftSemi,
		),
		(
			"SELECT * FROM left_t RIGHT SEMI JOIN right_t ON left_t.id = right_t.id",
			JoinKind::RightSemi,
		),
		(
			"SELECT * FROM left_t ANTI JOIN right_t ON left_t.id = right_t.id",
			JoinKind::LeftAnti,
		),
		(
			"SELECT * FROM left_t LEFT ANTI JOIN right_t ON left_t.id = right_t.id",
			JoinKind::LeftAnti,
		),
		(
			"SELECT * FROM left_t RIGHT ANTI JOIN right_t ON left_t.id = right_t.id",
			JoinKind::RightAnti,
		),
	] {
		assert_eq!(planned_join_kind(sql), expected, "{sql}");
	}
}

#[test]
fn sql_uncorrelated_exists_predicate_remains_unsupported() {
	let err = SqlPlanner::new()
		.plan("SELECT * FROM left_t WHERE EXISTS (SELECT * FROM right_t)")
		.expect_err("uncorrelated EXISTS stays unsupported");

	assert!(err.to_string().contains("unsupported"));
}

fn planned_join_kind(sql: &str) -> JoinKind {
	match SqlPlanner::new().plan(sql).expect("plan sql") {
		LogicalPlan::Join { kind, .. } => kind,
		other => panic!("expected join plan, got {other:?}"),
	}
}

fn nominal_join_plan(left_logical: LogicalTypeId, right_logical: LogicalTypeId) -> LogicalPlan {
	let logical_key = |table: &str, logical_type| Expr::LogicalAscription {
		expr: Box::new(Expr::qualified_column(TableRef::new(table), "id")),
		logical_type,
	};

	LogicalPlan::Join {
		left: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("left_t"),
				projection: None,
			}),
			partition_count: 2,
			key: vec![Expr::qualified_column(TableRef::new("left_t"), "id")],
		}),
		right: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("right_t"),
				projection: None,
			}),
			partition_count: 2,
			key: vec![Expr::qualified_column(TableRef::new("right_t"), "id")],
		}),
		kind: JoinKind::Inner,
		on: vec![(
			logical_key("left_t", left_logical),
			logical_key("right_t", right_logical),
		)],
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	}
}

fn hash_join_plan(kind: JoinKind) -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("left_t"),
				projection: None,
			}),
			partition_count: 2,
			key: vec![Expr::qualified_column(TableRef::new("left_t"), "id")],
		}),
		right: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("right_t"),
				projection: None,
			}),
			partition_count: 2,
			key: vec![Expr::qualified_column(TableRef::new("right_t"), "id")],
		}),
		kind,
		on: vec![(
			Expr::qualified_column(TableRef::new("left_t"), "id"),
			Expr::qualified_column(TableRef::new("right_t"), "id"),
		)],
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	}
}

fn broadcast_join_plan(kind: JoinKind, build_side: BroadcastBuildSide) -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("left_t"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("right_t"),
			projection: None,
		}),
		kind,
		on: vec![(
			Expr::qualified_column(TableRef::new("left_t"), "id"),
			Expr::qualified_column(TableRef::new("right_t"), "id"),
		)],
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Direct,
			build_side: Some(build_side),
		},
	}
}

fn assert_schema(schema: &SchemaRef, expected_len: usize, nullability: &[bool]) {
	assert_eq!(schema.fields().len(), expected_len);
	assert_eq!(
		schema
			.fields()
			.iter()
			.map(|field| field.is_nullable())
			.collect::<Vec<_>>(),
		nullability
	);
}

fn all_join_kinds() -> Vec<JoinKind> {
	vec![
		JoinKind::Inner,
		JoinKind::LeftOuter,
		JoinKind::RightOuter,
		JoinKind::FullOuter,
		JoinKind::LeftSemi,
		JoinKind::RightSemi,
		JoinKind::LeftAnti,
		JoinKind::RightAnti,
	]
}

fn catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor(1, "left_t", left_schema().as_ref()))
		.expect("register left");
	catalog
		.register_descriptor(
			TableId(2),
			descriptor(2, "right_t", right_schema().as_ref()),
		)
		.expect("register right");
	catalog
}

fn left_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("left_value", DataType::Utf8, true),
	]))
}

fn right_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("right_value", DataType::Int32, false),
	]))
}

fn descriptor(id: u8, name: &str, schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}
