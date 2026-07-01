use super::*;
use crate::catalog::{
	DescriptorColumnNdvFact, DescriptorColumnRangeDistributionFact, DescriptorColumnRangeFact,
	DescriptorOptimizerFacts, MockCatalog, RangeDistributionBucket, RangeFact, WendelonCatalog,
};
use crate::coordinator::QueryCoordinator;
use crate::distributed::arrow_ipc::encode_record_batch;
use crate::distributed::identity::QueryId;
use crate::ir::{BoundBinaryOp, BoundExprNode, QueryProfileHint, ScalarValue, SessionId, TableId};
use crate::rune_engine::{RuneEngine, RuneSandboxConfig};
use crate::sql_planner::SqlPlanner;
use crate::storage::{InMemoryTableStorage, StorageWrite, TenantId, WriteRequest};
use arrow::array::{
	Array, ArrayRef, BinaryArray, Date32Array, DurationNanosecondArray, Int32Array, Int64Array,
	LargeStringArray, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{Field, Schema};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};
use wendelon_client::{LogicalTypeDefinition, LogicalTypePhysical};

const DAY_NANOS: i64 = 86_400_000_000_000;

#[test]
fn predicate_pushdown_emitted_splits_preserve_kleene_3vl() {
	for program in [
		property_common_or_factor_program(),
		property_common_or_three_branch_program(),
		property_or_branch_strips_to_true_program(),
		property_not_or_program(),
		property_not_or_partial_push_program(),
		property_top_level_and_program(),
	] {
		let (pushed, residual) = partition_bound_predicate(&program).expect("predicate split");
		assert!(
			pushed.is_some(),
			"property fixture must emit a pushed predicate"
		);

		for p in TestK3::VALUES {
			for r1 in TestK3::VALUES {
				for r2 in TestK3::VALUES {
					let row = [p, r1, r2];
					let original = eval_test_program(&program, &row);
					let split = k3_and(
						pushed
							.as_ref()
							.map(|program| eval_test_program(program, &row))
							.unwrap_or(TestK3::True),
						residual
							.as_ref()
							.map(|program| eval_test_program(program, &row))
							.unwrap_or(TestK3::True),
					);
					assert_eq!(split, original, "split must preserve exact K3 value");
				}
			}
		}
	}
}

#[test]
fn where_not_or_all_branch_negations_push_removes_residual() {
	let mut builder = BoundExprProgramBuilder::new();
	let p = builder.append_column(test_bool_column("p", 0, true));
	let r1 = builder.append_column(test_bool_column("r1", 1, true));
	let disjunction = builder.append_binary(BoundBinaryOp::Or, p, r1, DataType::Boolean);
	let root = builder.append_unary(BoundUnaryOp::Not, disjunction, DataType::Boolean);
	builder.push_root(root);
	let program = builder.build();

	let split = try_extract_not_or_branch_negations(&program, root, disjunction)
		.expect("extract not-or")
		.expect("all branch negations should push");
	assert!(split.pushed.is_some());
	assert!(
		split.residual.is_none(),
		"all pushed NOT(OR) branches must remove residual"
	);
}

#[tokio::test]
async fn binder_resolves_columns_to_ordinals() {
	let catalog = catalog();
	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		output_names: vec![None],
		exprs: vec![Expr::column("amount")],
	};

	let plan = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	match plan {
		ExecutionPlan::Project { input, exprs, .. } => {
			let column = exprs.single_column_root().expect("project column root");
			assert_eq!(column.ordinal, ColumnOrdinal(0));
			assert_eq!(column.data_type, DataType::Int32);
			assert!(column.nullable);
			assert!(column.pbc_eligible);
			match *input {
				ExecutionPlan::Scan {
					projection, schema, ..
				} => {
					assert_eq!(projection, Some(vec![ColumnOrdinal(1)]));
					assert_eq!(schema.field(0).name(), "amount");
				}
				other => panic!("expected scan input, got {other:?}"),
			}
		}
		other => panic!("expected project, got {other:?}"),
	}
}

#[tokio::test]
async fn binder_pushes_exact_projection_union_through_filter() {
	let catalog = catalog();
	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Gt(
				Box::new(Expr::column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
			),
		}),
		output_names: vec![None],
		exprs: vec![Expr::column("id")],
	};

	let plan = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");
	match plan {
		ExecutionPlan::Project { input, exprs, .. } => {
			let column = exprs.single_column_root().expect("project column root");
			assert_eq!(column.ordinal, ColumnOrdinal(0));
			match *input {
				ExecutionPlan::Filter {
					input,
					predicate,
					disposition,
				} => {
					assert_eq!(disposition, FilterDisposition::FullyPushed);
					let root = predicate.single_root().expect("predicate root");
					match predicate.node(root).expect("predicate node") {
						BoundExprNode::Binary {
							op: BoundBinaryOp::Gt,
							left,
							..
						} => match predicate.node(*left).expect("predicate left") {
							BoundExprNode::Column(column) => {
								assert_eq!(column.ordinal, ColumnOrdinal(1))
							}
							other => panic!("expected predicate column, got {other:?}"),
						},
						other => panic!("expected GT predicate, got {other:?}"),
					}
					match *input {
						ExecutionPlan::Scan {
							projection, schema, ..
						} => {
							assert_eq!(projection, Some(vec![ColumnOrdinal(0)]));
							assert_eq!(schema.fields().len(), 1);
							assert_eq!(schema.field(0).name(), "id");
						}
						other => panic!("expected scan input, got {other:?}"),
					}
				}
				other => panic!("expected filter input, got {other:?}"),
			}
		}
		other => panic!("expected project, got {other:?}"),
	}
}

#[tokio::test]
async fn binder_rejects_unknown_columns() {
	let catalog = catalog();
	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		output_names: vec![None],
		exprs: vec![Expr::column("missing")],
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unknown column should fail");
	assert!(matches!(
		err,
		BindingError::UnknownColumn { column } if column == "missing"
	));
}

#[tokio::test]
async fn binder_gates_experimental_kernels_by_default() {
	let catalog = catalog();
	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		output_names: vec![None],
		exprs: vec![Expr::TimeBucket(
			std::time::Duration::from_secs(60),
			Box::new(Expr::column("ts")),
		)],
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("experimental kernel should fail by default");
	assert!(matches!(
		err,
		BindingError::ExperimentalKernel {
			kernel: "TimeBucket"
		}
	));

	let plan = bind(
		&plan,
		&catalog,
		&PlannerConfig {
			allow_experimental_kernels: true,
			..PlannerConfig::default()
		},
	)
	.await
	.expect("experimental kernel enabled");
	assert!(matches!(plan, ExecutionPlan::Project { .. }));
}

#[tokio::test]
async fn binder_shapes_grouped_aggregate_plan() {
	let catalog = catalog();
	let plan = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		group_expr: vec![Expr::column("region")],
		aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
	};

	let plan = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind grouped aggregate");
	match plan {
		ExecutionPlan::Aggregate { output_schema, .. } => {
			assert_eq!(output_schema.field(0).name(), "region");
			assert_eq!(output_schema.field(1).name(), "sum");
		}
		other => panic!("expected aggregate, got {other:?}"),
	}
}

#[tokio::test]
async fn join_projection_pushdown_keeps_join_keys_and_parent_columns() {
	fn scan_projection(plan: &ExecutionPlan, table_id: TableId) -> Option<Vec<ColumnOrdinal>> {
		match plan {
			ExecutionPlan::Scan {
				local_table_id,
				projection,
				..
			} if *local_table_id == table_id => projection.clone(),
			ExecutionPlan::Filter { input, .. }
			| ExecutionPlan::Project { input, .. }
			| ExecutionPlan::Aggregate { input, .. }
			| ExecutionPlan::Exchange { input, .. }
			| ExecutionPlan::Sort { input, .. }
			| ExecutionPlan::Window { input, .. }
			| ExecutionPlan::Take { input, .. }
			| ExecutionPlan::Repeat { input, .. } => scan_projection(input, table_id),
			ExecutionPlan::Join { left, right, .. } | ExecutionPlan::Union { left, right, .. } => {
				scan_projection(left, table_id).or_else(|| scan_projection(right, table_id))
			}
			_ => None,
		}
	}

	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(71),
			named_descriptor(
				71,
				"orders",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
					Field::new("amount", DataType::Int64, false),
					Field::new("comment", DataType::Utf8, true),
				]),
			),
		)
		.expect("register orders");
	catalog
		.register_descriptor(
			TableId(72),
			named_descriptor(
				72,
				"customers",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("region", DataType::Utf8, true),
					Field::new("comment", DataType::Utf8, true),
				]),
			),
		)
		.expect("register customers");
	let join = LogicalPlan::Join {
		left: Box::new(named_scan("orders")),
		right: Box::new(named_scan("customers")),
		kind: JoinKind::Inner,
		on: vec![(qcol("orders", "customer_id"), qcol("customers", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let aggregate = LogicalPlan::Aggregate {
		input: Box::new(join),
		group_expr: vec![qcol("customers", "region")],
		aggr_expr: vec![(AggregateFunction::Sum, vec![qcol("orders", "amount")])],
	};

	let plan = bind(&aggregate, &catalog, &PlannerConfig::default())
		.await
		.expect("bind aggregate join");

	assert_eq!(
		scan_projection(&plan, TableId(71)),
		Some(vec![ColumnOrdinal(1), ColumnOrdinal(2)])
	);
	assert_eq!(
		scan_projection(&plan, TableId(72)),
		Some(vec![ColumnOrdinal(0), ColumnOrdinal(1)])
	);
}

#[tokio::test]
async fn binder_attaches_pbc_program_for_eligible_scan_filter() {
	let catalog = catalog();
	let plan = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Gt(
			Box::new(Expr::column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
		),
	};

	let plan = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind eligible filter");
	match plan {
		ExecutionPlan::Filter {
			input, disposition, ..
		} => {
			assert_eq!(disposition, FilterDisposition::FullyPushed);
			match *input {
				ExecutionPlan::Scan { pbc_program, .. } => assert!(pbc_program.is_some()),
				other => panic!("expected scan input, got {other:?}"),
			}
		}
		other => panic!("expected filter, got {other:?}"),
	}
}

#[tokio::test]
async fn converted_linear_predicate_pushes_as_bare_column() {
	let catalog = converted_unit_catalog(false);
	let scoped = bind_sql_scoped(
		&catalog,
		"SELECT * FROM customers WHERE CONVERT(distance_mile, Kilometer) > CAST(10.0 AS Kilometer)",
	)
	.await;

	let (disposition, pbc_attached) =
		first_filter_scan_state(&scoped.plan).expect("converted filter");
	assert_eq!(disposition, FilterDisposition::EvaluateInToron);
	assert!(pbc_attached, "converted linear predicate should attach PBC");
}

#[tokio::test]
async fn converted_linear_predicate_spends_source_range_fact() {
	let catalog = converted_unit_catalog(true);
	let scoped = bind_sql_scoped(
		&catalog,
		"SELECT * FROM customers WHERE CONVERT(distance_mile, Kilometer) > CAST(10.0 AS Kilometer)",
	)
	.await;

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(938)));
}

#[tokio::test]
async fn linear_convert_pushdown_matches_all_range_directions() {
	let catalog = converted_unit_catalog(false);
	for op in ["<", "<=", ">", ">="] {
		let scoped = bind_sql_scoped(
			&catalog,
			&format!(
				"SELECT * FROM customers WHERE CONVERT(distance_mile, Kilometer) {op} CAST(10.0 AS Kilometer)"
			),
		)
		.await;
		let (disposition, pbc_attached) =
			first_filter_scan_state(&scoped.plan).expect("converted filter");
		assert_eq!(disposition, FilterDisposition::EvaluateInToron);
		assert!(pbc_attached, "converted predicate {op} should attach PBC");
	}
}

#[tokio::test]
async fn affine_point_converted_predicate_stays_residual() {
	let catalog = point_unit_catalog();
	let scoped = bind_sql_scoped(
		&catalog,
		"SELECT * FROM temperatures WHERE CONVERT(temp_c, Fahrenheit) > CAST(50.0 AS Fahrenheit)",
	)
	.await;

	let (disposition, pbc_attached) =
		first_filter_scan_state(&scoped.plan).expect("affine converted filter");
	assert_eq!(disposition, FilterDisposition::EvaluateInToron);
	assert!(!pbc_attached, "affine point CONVERT stays residual-only");
}

#[tokio::test]
async fn equality_on_converted_column_stays_residual() {
	let catalog = converted_unit_catalog(false);
	let scoped = bind_sql_scoped(
		&catalog,
		"SELECT * FROM customers WHERE CONVERT(distance_mile, Kilometer) = CAST(10.0 AS Kilometer)",
	)
	.await;

	let (disposition, pbc_attached) =
		first_filter_scan_state(&scoped.plan).expect("converted equality filter");
	assert_eq!(disposition, FilterDisposition::EvaluateInToron);
	assert!(!pbc_attached, "converted equality is deferred");
}

#[tokio::test]
async fn naked_arithmetic_scaled_predicate_stays_residual() {
	let catalog = converted_unit_catalog(false);
	let scoped = bind_sql_scoped(
		&catalog,
		"SELECT * FROM customers WHERE distance_mile * 1.609344 > CAST(10.0 AS Mile)",
	)
	.await;

	let (disposition, pbc_attached) =
		first_filter_scan_state(&scoped.plan).expect("scaled arithmetic filter");
	assert_eq!(disposition, FilterDisposition::EvaluateInToron);
	assert!(!pbc_attached, "shape-only scaled arithmetic must not push");
}

#[tokio::test]
async fn converted_pushdown_rejects_factor_metadata_mismatch() {
	let catalog = converted_unit_catalog(false);
	let registry = catalog.logical_type_registry();
	let binding = catalog
		.resolve_table(&TableRef::new("customers"))
		.await
		.expect("resolve customers");
	let scan = scan_plan(binding, None);
	let mut builder = BoundExprProgramBuilder::new();
	let source = builder.append_column(ResolvedColumn {
		name: ColumnName("distance_mile".to_string()),
		ordinal: ColumnOrdinal(1),
		data_type: DataType::Float64,
		nullable: true,
		pbc_eligible: true,
	});
	assert!(builder.set_node_logical_type(source, mile_logical_type()));
	let factor = builder.append_literal(ScalarValue::Float64(Some(1.0)));
	let converted = builder.append_binary(BoundBinaryOp::Mul, source, factor, DataType::Float64);
	assert!(builder.set_node_logical_type(converted, kilometer_logical_type()));
	let constant = builder.append_literal(ScalarValue::Float64(Some(10.0)));
	let root = builder.append_binary(BoundBinaryOp::Gt, converted, constant, DataType::Boolean);
	builder.push_root(root);
	let program = builder.build();

	let pushdown = predicate_pushdown_plan(program, &scan, &registry);
	assert!(pushdown.pbc_program.is_none());
	assert!(pushdown.residual_predicate.is_some());
}

#[tokio::test]
async fn binder_selects_point_read_profile_for_primary_key_equality() {
	let catalog = catalog();
	let plan = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("id")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
		),
	};

	let plan = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind primary-key equality");

	match plan {
		ExecutionPlan::Filter { input, .. } => match *input {
			ExecutionPlan::Scan {
				profile_hint,
				primary_key_columns,
				..
			} => {
				assert_eq!(profile_hint, QueryProfileHint::PointRead);
				assert_eq!(primary_key_columns, vec![ColumnOrdinal(0)]);
			}
			other => panic!("expected scan input, got {other:?}"),
		},
		other => panic!("expected filter, got {other:?}"),
	}
}

#[tokio::test]
async fn binder_keeps_scan_profile_for_non_primary_key_equality() {
	let catalog = catalog();
	let plan = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(7)))),
		),
	};

	let plan = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind non-primary-key equality");

	match plan {
		ExecutionPlan::Filter { input, .. } => match *input {
			ExecutionPlan::Scan { profile_hint, .. } => {
				assert_eq!(profile_hint, QueryProfileHint::Scan);
			}
			other => panic!("expected scan input, got {other:?}"),
		},
		other => panic!("expected filter, got {other:?}"),
	}
}

#[tokio::test]
async fn binder_marks_ineligible_filters_for_toron_evaluation() {
	let catalog = catalog();
	let plan = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
		),
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("type mismatch should fail before pushdown");
	assert!(matches!(err, BindingError::TypeMismatch { .. }));
}

#[tokio::test]
async fn bind_hash_join_resolves_qualified_keys_and_duplicate_output_names() {
	let catalog = join_catalog();
	let plan = LogicalPlan::Join {
		left: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			partition_count: 2,
			key: vec![Expr::qualified_column(
				TableRef::new("orders"),
				"customer_id",
			)],
		}),
		right: Box::new(LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			partition_count: 2,
			key: vec![Expr::qualified_column(TableRef::new("customers"), "id")],
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};

	let plan = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind join");

	match plan {
		ExecutionPlan::Join {
			left_keys,
			right_keys,
			output_schema,
			..
		} => {
			assert_eq!(left_keys, vec![ColumnOrdinal(1)]);
			assert_eq!(right_keys, vec![ColumnOrdinal(0)]);
			assert_eq!(output_schema.fields().len(), 4);
			assert_eq!(output_schema.field(0).name(), "id");
			assert_eq!(output_schema.field(2).name(), "id");
		}
		other => panic!("expected join, got {other:?}"),
	}
}

#[tokio::test]
async fn bind_hash_join_auto_swaps_reversed_on_pair() {
	let catalog = join_catalog();
	let plan = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
		right: Box::new(exchanged_named_scan("customers", "id", 2)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};

	let plan = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind join");

	match plan {
		ExecutionPlan::Join {
			left_keys,
			right_keys,
			..
		} => {
			assert_eq!(left_keys, vec![ColumnOrdinal(1)]);
			assert_eq!(right_keys, vec![ColumnOrdinal(0)]);
		}
		other => panic!("expected join, got {other:?}"),
	}
}

#[tokio::test]
async fn bind_join_rejects_ambiguous_unqualified_column() {
	let catalog = join_catalog();
	let plan = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("orders", "id", 2)),
		right: Box::new(exchanged_named_scan("customers", "id", 2)),
		kind: JoinKind::Inner,
		on: vec![(Expr::column("id"), Expr::column("id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("ambiguous join key should fail");

	assert!(matches!(err, BindingError::AmbiguousColumnRef { name, .. } if name == "id"));
}

#[tokio::test]
async fn bind_join_rejects_unknown_qualifier() {
	let catalog = join_catalog();
	let plan = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
		right: Box::new(exchanged_named_scan("customers", "id", 2)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("missing"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};

	let err = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("unknown qualifier should fail");

	assert!(matches!(
		err,
		BindingError::UnknownTableRef { qualifier } if qualifier == "missing"
	));
}

#[tokio::test]
async fn bind_join_rejects_non_equi_and_computed_keys() {
	let catalog = join_catalog();
	let non_equi = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
		right: Box::new(exchanged_named_scan("customers", "id", 2)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::Eq(
				Box::new(Expr::qualified_column(
					TableRef::new("orders"),
					"customer_id",
				)),
				Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
			),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};
	let computed = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
		right: Box::new(exchanged_named_scan("customers", "id", 2)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::Literal(ScalarValue::Int64(Some(7))),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};

	assert!(matches!(
		bind(&non_equi, &catalog, &PlannerConfig::default())
			.await
			.expect_err("non-equi should fail"),
		BindingError::JoinPredicateNotEqui
	));
	assert!(matches!(
		bind(&computed, &catalog, &PlannerConfig::default())
			.await
			.expect_err("computed key should fail"),
		BindingError::JoinComputedKeyUnsupported
	));
}

#[tokio::test]
async fn bind_join_rejects_key_type_mismatch_and_float_keys() {
	let catalog = join_catalog();
	let mismatch = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
		right: Box::new(exchanged_named_scan("customers", "region", 2)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "region"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};

	assert!(matches!(
		bind(&mismatch, &catalog, &PlannerConfig::default())
			.await
			.expect_err("type mismatch should fail"),
		BindingError::JoinKeyTypeMismatch { .. }
	));

	let catalog = float_join_catalog();
	let float_key = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("left_floats", "f", 2)),
		right: Box::new(exchanged_named_scan("right_floats", "f", 2)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("left_floats"), "f"),
			Expr::qualified_column(TableRef::new("right_floats"), "f"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};

	assert!(matches!(
		bind(&float_key, &catalog, &PlannerConfig::default())
			.await
			.expect_err("float key should fail"),
		BindingError::JoinKeyTypeUnsupported { .. }
	));
}

#[tokio::test]
async fn bind_join_rejects_exchange_partition_and_key_shape_mismatches() {
	let catalog = join_catalog();
	let partition_mismatch = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
		right: Box::new(exchanged_named_scan("customers", "id", 3)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};
	let key_mismatch = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("orders", "id", 2)),
		right: Box::new(exchanged_named_scan("customers", "id", 2)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};

	assert!(matches!(
		bind(&partition_mismatch, &catalog, &PlannerConfig::default())
			.await
			.expect_err("partition mismatch should fail"),
		BindingError::JoinPartitionCountMismatch { left: 2, right: 3 }
	));
	assert!(matches!(
		bind(&key_mismatch, &catalog, &PlannerConfig::default())
			.await
			.expect_err("exchange key mismatch should fail"),
		BindingError::JoinKeyExchangeMismatch
	));
}

#[tokio::test]
async fn bind_pinned_broadcast_physicalizes_left_build() {
	let catalog = join_catalog();
	let plan = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Direct,
			build_side: Some(BroadcastBuildSide::Left),
		},
	};

	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind pinned broadcast");
	let ExecutionPlan::Join {
		left,
		right,
		strategy,
		..
	} = bound
	else {
		panic!("expected broadcast join");
	};
	assert_eq!(strategy, JoinStrategy::Broadcast);
	assert!(matches!(terminal_exchange(&left), Some((1, _))));
	assert!(terminal_exchange(&right).is_none());
}

#[tokio::test]
async fn bind_pinned_broadcast_rejects_preshaped_probe_exchange() {
	let catalog = join_catalog();
	let prepublished_probe = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		right: Box::new(exchanged_named_scan("orders", "customer_id", 1)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Direct,
			build_side: Some(BroadcastBuildSide::Left),
		},
	};

	assert!(matches!(
		bind(&prepublished_probe, &catalog, &PlannerConfig::default())
			.await
			.expect_err("probe-side publish must fail"),
		BindingError::BroadcastBuildSidePlacement
	));
}

#[tokio::test]
async fn auto_broadcast_sql_unhinted_tiny_left_join_infers_broadcast() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
		.expect("plan SQL join");
	assert!(matches!(
		logical,
		LogicalPlan::Join {
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
			..
		}
	));

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind inferred broadcast");

	let ExecutionPlan::Join {
		left,
		right,
		strategy,
		..
	} = plan
	else {
		panic!("expected join");
	};
	assert_eq!(strategy, JoinStrategy::Broadcast);
	assert!(matches!(terminal_exchange(&left), Some((1, _))));
	assert!(terminal_exchange(&right).is_none());
}

#[tokio::test]
async fn left_outer_null_rejecting_right_filter_binds_inner_and_broadcasts_filtered_dim() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o LEFT JOIN customers c \
			 ON o.customer_id = c.id WHERE c.region = 'north'",
		)
		.expect("plan null-rejected outer join");

	let (plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind demoted outer join");

	assert_eq!(render_trace.outer_join_demotions.len(), 1);
	assert_eq!(
		render_trace.outer_join_demotions[0].from_kind,
		JoinKind::LeftOuter
	);
	assert_eq!(
		render_trace.outer_join_demotions[0].to_kind,
		JoinKind::Inner
	);
	assert_eq!(trace.join_decisions.len(), 1);
	assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
	assert_eq!(
		trace.join_decisions[0].placement,
		JoinPhysicalPlacement::SwappedRightBuild
	);
	let ExecutionPlan::Join {
		kind,
		strategy,
		left,
		..
	} = first_join_plan(&plan)
	else {
		panic!("demoted plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::Inner);
	assert_eq!(*strategy, JoinStrategy::Broadcast);
	assert!(
		first_filter_scan_state(left).is_some(),
		"demotion-owned predicate should move to the broadcast build input"
	);
}

#[tokio::test]
async fn rune_filter_after_left_join_gets_same_demotion_as_sql() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let sql = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o LEFT JOIN customers c \
			 ON o.customer_id = c.id WHERE c.region = 'north'",
		)
		.expect("plan SQL null-rejected outer join");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o")
                .left_join(
                    ctx.table("customers").alias("c"),
                    qcol("o", "customer_id").eq(qcol("c", "id"))
                )
                .filter(qcol("c", "region").eq(lit_str("north")))
        }
    "#,
	)
	.await;

	let mut observed = Vec::new();
	for (source, logical) in [("sql", sql), ("rune", rune)] {
		let (plan, _trace, render_trace) =
			bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(4_096))
				.await
				.unwrap_or_else(|err| panic!("{source} demoted outer join should bind: {err}"));

		assert_eq!(
			render_trace.outer_join_demotions.len(),
			1,
			"{source} should emit one demotion receipt"
		);
		assert_eq!(
			render_trace.outer_join_demotions[0].from_kind,
			JoinKind::LeftOuter,
			"{source} should demote a left outer join"
		);
		assert_eq!(
			render_trace.outer_join_demotions[0].to_kind,
			JoinKind::Inner,
			"{source} should demote to inner"
		);
		let ExecutionPlan::Join { kind, .. } = first_join_plan(&plan) else {
			panic!("{source} demoted plan should contain a join");
		};
		assert_eq!(*kind, JoinKind::Inner, "{source} effective join kind");
		observed.push((
			render_trace.outer_join_demotions[0].from_kind,
			render_trace.outer_join_demotions[0].to_kind,
			*kind,
		));
	}
	assert_eq!(
		observed[0], observed[1],
		"Rune should preserve SQL demotion receipt and effective join kind"
	);
}

#[tokio::test]
async fn is_null_on_null_supplied_side_does_not_demote() {
	let catalog = join_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o LEFT JOIN customers c \
			 ON o.customer_id = c.id WHERE c.region IS NULL",
		)
		.expect("plan IS NULL outer join filter");

	let (plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind non-demoted outer join");

	assert!(render_trace.outer_join_demotions.is_empty());
	assert_eq!(trace.join_decisions.len(), 1);
	let ExecutionPlan::Join { kind, strategy, .. } = first_join_plan(&plan) else {
		panic!("plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::LeftOuter);
	assert_eq!(*strategy, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn full_outer_one_side_null_rejection_demotes_stepwise() {
	let catalog = join_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o FULL OUTER JOIN customers c \
			 ON o.customer_id = c.id WHERE o.id IS NOT NULL",
		)
		.expect("plan full outer stepwise demotion");

	let (plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind stepwise demoted full outer join");

	assert_eq!(render_trace.outer_join_demotions.len(), 1);
	assert_eq!(
		render_trace.outer_join_demotions[0].from_kind,
		JoinKind::FullOuter
	);
	assert_eq!(
		render_trace.outer_join_demotions[0].to_kind,
		JoinKind::LeftOuter
	);
	assert_eq!(
		trace.join_decisions[0].strategy,
		JoinStrategy::HashPartitioned
	);
	let ExecutionPlan::Join { kind, .. } = first_join_plan(&plan) else {
		panic!("plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::LeftOuter);
}

#[tokio::test]
async fn full_outer_both_sides_null_rejection_demotes_to_inner() {
	let catalog = join_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o FULL OUTER JOIN customers c \
			 ON o.customer_id = c.id WHERE o.id IS NOT NULL AND c.id IS NOT NULL",
		)
		.expect("plan full outer both-side demotion");

	let (plan, _trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind inner-demoted full outer join");

	assert_eq!(render_trace.outer_join_demotions.len(), 1);
	assert_eq!(
		render_trace.outer_join_demotions[0].from_kind,
		JoinKind::FullOuter
	);
	assert_eq!(
		render_trace.outer_join_demotions[0].to_kind,
		JoinKind::Inner
	);
	let ExecutionPlan::Join { kind, .. } = first_join_plan(&plan) else {
		panic!("plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::Inner);
}

#[tokio::test]
async fn full_outer_without_demote_stays_hash_non_inner() {
	let catalog = join_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o FULL OUTER JOIN customers c \
			 ON o.customer_id = c.id",
		)
		.expect("plan full outer without null rejection");

	let (plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind non-demoted full outer join");

	assert!(render_trace.outer_join_demotions.is_empty());
	assert_eq!(
		trace.join_decisions[0].strategy,
		JoinStrategy::HashPartitioned
	);
	let ExecutionPlan::Join { kind, .. } = first_join_plan(&plan) else {
		panic!("plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::FullOuter);
}

#[tokio::test]
async fn positive_in_demotes_negated_in_does_not() {
	let catalog = join_catalog();
	let join = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		kind: JoinKind::LeftOuter,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let positive = LogicalPlan::Filter {
		input: Box::new(join.clone()),
		predicate: Expr::InList {
			expr: Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
			list: vec![Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))],
			negated: false,
		},
	};
	let negated = LogicalPlan::Filter {
		input: Box::new(join),
		predicate: Expr::InList {
			expr: Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
			list: vec![Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))],
			negated: true,
		},
	};

	let (positive_plan, _, positive_render) =
		bind_with_explain_trace(&positive, &catalog, &PlannerConfig::default())
			.await
			.expect("bind positive IN demotion");
	assert_eq!(positive_render.outer_join_demotions.len(), 1);
	let ExecutionPlan::Join { kind, .. } = first_join_plan(&positive_plan) else {
		panic!("positive IN plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::Inner);

	let (negated_plan, _, negated_render) =
		bind_with_explain_trace(&negated, &catalog, &PlannerConfig::default())
			.await
			.expect("bind negated IN without demotion");
	assert!(negated_render.outer_join_demotions.is_empty());
	let ExecutionPlan::Join { kind, .. } = first_join_plan(&negated_plan) else {
		panic!("negated IN plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::LeftOuter);
}

#[tokio::test]
async fn positive_like_demotes_negated_like_does_not() {
	let catalog = join_catalog();
	let positive = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o LEFT JOIN customers c \
			 ON o.customer_id = c.id WHERE c.region LIKE 'nor%'",
		)
		.expect("plan LIKE demotion");
	let negated = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o LEFT JOIN customers c \
			 ON o.customer_id = c.id WHERE c.region NOT LIKE 'nor%'",
		)
		.expect("plan NOT LIKE no-demotion");

	let (positive_plan, _, positive_render) =
		bind_with_explain_trace(&positive, &catalog, &PlannerConfig::default())
			.await
			.expect("bind positive LIKE demotion");
	assert_eq!(positive_render.outer_join_demotions.len(), 1);
	let ExecutionPlan::Join { kind, .. } = first_join_plan(&positive_plan) else {
		panic!("positive LIKE plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::Inner);

	let (negated_plan, _, negated_render) =
		bind_with_explain_trace(&negated, &catalog, &PlannerConfig::default())
			.await
			.expect("bind negated LIKE without demotion");
	assert!(negated_render.outer_join_demotions.is_empty());
	let ExecutionPlan::Join { kind, .. } = first_join_plan(&negated_plan) else {
		panic!("negated LIKE plan should contain a join");
	};
	assert_eq!(*kind, JoinKind::LeftOuter);
}

#[tokio::test]
async fn path_scoped_filter_demotes_nested_left_join_through_inner_chain() {
	let catalog = mixed_outer_join_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT o.id, c.id, p.id, s.id \
			 FROM orders o \
			 LEFT JOIN customers c ON o.customer_id = c.id \
			 INNER JOIN products p ON o.product_id = p.id \
			 INNER JOIN stores s ON o.store_id = s.id \
			 WHERE c.region = 'north'",
		)
		.expect("plan mixed-chain SQL");

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(receipts[0].from_kind, JoinKind::LeftOuter);
	assert_eq!(receipts[0].to_kind, JoinKind::Inner);
	assert_eq!(
		receipts[0].propagation_path,
		vec![JoinSideStep::Left, JoinSideStep::Left]
	);
	assert!(
		logical_join_kinds(&normalized)
			.into_iter()
			.all(|kind| kind == JoinKind::Inner),
		"all joins should be inner after nested demotion: {normalized:?}"
	);
}

#[tokio::test]
async fn filter_does_not_move_below_take_limit() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Take {
			input: Box::new(mixed_chain_one_inner_hop()),
			limit: 1,
		}),
		predicate: customers_region_is_north(),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert!(receipts.is_empty());
	assert!(matches!(normalized, LogicalPlan::Filter { .. }));
	assert!(
		logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
		"Take must keep the nested outer join intact: {normalized:?}"
	);
}

#[tokio::test]
async fn filter_does_not_move_below_sort_limit_or_offset() {
	let catalog = mixed_outer_join_catalog();
	for (name, limit, offset) in [("top_n", Some(1), 0), ("offset", None, 1)] {
		let logical = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Sort {
				input: Box::new(mixed_chain_one_inner_hop()),
				order_by: vec![orders_id_sort_key()],
				limit,
				offset,
			}),
			predicate: customers_region_is_north(),
		};

		let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

		assert!(receipts.is_empty(), "{name} sort must stop propagation");
		assert!(
			matches!(normalized, LogicalPlan::Filter { .. }),
			"{name} sort should leave the original filter in place"
		);
		assert!(
			logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
			"{name} sort must keep the nested outer join intact: {normalized:?}"
		);
	}
}

#[tokio::test]
async fn pure_sort_allows_path_scoped_demote_with_results_match() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Sort {
			input: Box::new(mixed_chain_one_inner_hop()),
			order_by: vec![orders_id_sort_key()],
			limit: None,
			offset: 0,
		}),
		predicate: customers_region_is_north(),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(receipts[0].propagation_path, vec![JoinSideStep::Left]);
	assert!(
		logical_join_kinds(&normalized)
			.into_iter()
			.all(|kind| kind == JoinKind::Inner),
		"pure sort should allow the nested edge to demote: {normalized:?}"
	);
}

#[tokio::test]
async fn exchange_allows_path_scoped_demote() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Exchange {
			input: Box::new(mixed_chain_one_inner_hop()),
			partition_count: 4,
			key: vec![qcol("orders", "id")],
		}),
		predicate: customers_region_is_north(),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(receipts[0].propagation_path, vec![JoinSideStep::Left]);
	assert!(
		logical_join_kinds(&normalized)
			.into_iter()
			.all(|kind| kind == JoinKind::Inner),
		"exchange should allow the nested edge to demote: {normalized:?}"
	);
}

#[tokio::test]
async fn predicate_does_not_cross_outer_boundary_to_reach_deeper_edge() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Join {
			left: Box::new(orders_customers_left_join()),
			right: Box::new(named_scan("products")),
			kind: JoinKind::LeftOuter,
			on: vec![(qcol("orders", "product_id"), qcol("products", "id"))],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
		}),
		predicate: customers_region_is_north(),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert!(receipts.is_empty());
	assert_eq!(
		logical_join_kinds(&normalized)
			.into_iter()
			.filter(|kind| *kind == JoinKind::LeftOuter)
			.count(),
		2,
		"the first outer boundary should stop descent: {normalized:?}"
	);
}

#[tokio::test]
async fn cross_side_conjunct_stays_residual_above_original_filter() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(mixed_chain_one_inner_hop()),
		predicate: Expr::And(
			Box::new(customers_region_is_north()),
			Box::new(Expr::Eq(
				Box::new(qcol("products", "id")),
				Box::new(int64_literal(100)),
			)),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	let LogicalPlan::Filter { predicate, .. } = normalized else {
		panic!("cross-side residual should keep a root filter");
	};
	let predicate_text = format!("{predicate:?}");
	assert!(
		predicate_text.contains("products"),
		"product-side predicate should remain residual: {predicate_text}"
	);
	assert!(
		!predicate_text.contains("region"),
		"demotion-owned predicate should not remain residual: {predicate_text}"
	);
}

#[tokio::test]
async fn is_null_and_negated_in_remain_non_demoting_inside_mixed_chain() {
	let catalog = mixed_outer_join_catalog();
	for (name, predicate) in [
		(
			"is_null",
			Expr::IsNull(Box::new(qcol("customers", "region"))),
		),
		(
			"negated_in",
			Expr::InList {
				expr: Box::new(qcol("customers", "region")),
				list: vec![utf8_literal("north")],
				negated: true,
			},
		),
	] {
		let logical = LogicalPlan::Filter {
			input: Box::new(mixed_chain_one_inner_hop()),
			predicate,
		};

		let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

		assert!(receipts.is_empty(), "{name} must not demote");
		assert!(
			logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
			"{name} should leave the nested outer join intact: {normalized:?}"
		);
	}
}

#[tokio::test]
async fn rune_mixed_chain_null_rejection_matches_sql() {
	let catalog = mixed_outer_join_catalog();
	let sql = SqlPlanner::new()
		.plan(
			"SELECT o.id, c.id, p.id \
			 FROM orders o \
			 LEFT JOIN customers c ON o.customer_id = c.id \
			 INNER JOIN products p ON o.product_id = p.id \
			 WHERE c.region = 'north'",
		)
		.expect("plan SQL mixed chain");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o")
                .left_join(
                    ctx.table("customers").alias("c"),
                    qcol("o", "customer_id").eq(qcol("c", "id"))
                )
                .inner_join(
                    ctx.table("products").alias("p"),
                    qcol("o", "product_id").eq(qcol("p", "id"))
                )
                .filter(qcol("c", "region").eq(lit_str("north")))
        }
    "#,
	)
	.await;

	let (_, sql_receipts) = demote_outer_join_plan(&sql, &catalog).await;
	let (_, rune_receipts) = demote_outer_join_plan(&rune, &catalog).await;

	assert_eq!(sql_receipts.len(), 1);
	assert_eq!(rune_receipts.len(), 1);
	assert_eq!(sql_receipts[0].from_kind, rune_receipts[0].from_kind);
	assert_eq!(sql_receipts[0].to_kind, rune_receipts[0].to_kind);
	assert_eq!(
		sql_receipts[0].propagation_path,
		rune_receipts[0].propagation_path
	);
}

#[tokio::test]
async fn propagation_receipt_reports_descent_path() {
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT o.id, c.id, p.id \
			 FROM orders o \
			 LEFT JOIN customers c ON o.customer_id = c.id \
			 INNER JOIN products p ON o.product_id = p.id \
			 WHERE c.region = 'north'",
		)
		.expect("plan EXPLAIN mixed chain");

	let (_schema, rows) = bind_explain_rows(
		&logical,
		&mixed_outer_join_catalog(),
		&PlannerConfig::default(),
	)
	.await;
	let row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("outer_join_demotion".to_string())))
		.expect("outer join demotion receipt row");

	assert_eq!(row[1], ScalarValue::Utf8(Some("prebind".to_string())));
	assert_eq!(
		row[2],
		ScalarValue::Utf8(Some("left_outer->inner".to_string()))
	);
	assert_utf8_contains(&row[3], "null_rejecting_filter:");
	assert_utf8_contains(&row[3], "propagated_path:left");
	assert_utf8_contains(&row[4], "region");
}

#[tokio::test]
async fn recognizers_still_decline_true_outer_join_after_non_demoting_filter() {
	let catalog = mixed_outer_join_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT o.id, c.id, p.id \
			 FROM orders o \
			 LEFT JOIN customers c ON o.customer_id = c.id \
			 INNER JOIN products p ON o.product_id = p.id \
			 WHERE c.region IS NULL",
		)
		.expect("plan true-outer mixed chain");

	let (plan, _trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind true-outer mixed chain");

	assert!(render_trace.outer_join_demotions.is_empty());
	assert!(!render_trace.has_recognized_star_order());
	assert!(
		execution_join_kinds(&plan).contains(&JoinKind::LeftOuter),
		"true outer join should remain a recognizer fence: {plan:?}"
	);
}

#[tokio::test]
async fn full_outer_stepwise_demotes_via_propagation() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Join {
			left: Box::new(orders_customers_full_join()),
			right: Box::new(named_scan("products")),
			kind: JoinKind::Inner,
			on: vec![(qcol("orders", "product_id"), qcol("products", "id"))],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
		}),
		predicate: Expr::IsNotNull(Box::new(qcol("orders", "id"))),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(receipts[0].from_kind, JoinKind::FullOuter);
	assert_eq!(receipts[0].to_kind, JoinKind::LeftOuter);
	assert_eq!(receipts[0].propagation_path, vec![JoinSideStep::Left]);
	assert!(
		logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
		"full outer should demote stepwise through propagation: {normalized:?}"
	);
}

#[tokio::test]
async fn multi_conjuncts_can_demote_two_edges_in_one_pass() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Join {
			left: Box::new(orders_customers_left_join()),
			right: Box::new(products_stores_left_join()),
			kind: JoinKind::Inner,
			on: vec![(qcol("orders", "product_id"), qcol("products", "id"))],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
		}),
		predicate: Expr::And(
			Box::new(customers_region_is_north()),
			Box::new(Expr::Gt(
				Box::new(qcol("stores", "id")),
				Box::new(int64_literal(0)),
			)),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 2);
	assert_eq!(receipts[0].propagation_path, vec![JoinSideStep::Left]);
	assert_eq!(receipts[1].propagation_path, vec![JoinSideStep::Right]);
	assert!(
		logical_join_kinds(&normalized)
			.into_iter()
			.all(|kind| kind == JoinKind::Inner),
		"both nested outer edges should demote: {normalized:?}"
	);
}

#[tokio::test]
async fn or_both_branches_null_rejecting_demotes_left_join() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(orders_customers_left_join()),
		predicate: Expr::Or(
			Box::new(customers_region_is_north()),
			Box::new(Expr::Eq(
				Box::new(qcol("customers", "region")),
				Box::new(utf8_literal("south")),
			)),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(receipts[0].from_kind, JoinKind::LeftOuter);
	assert_eq!(receipts[0].to_kind, JoinKind::Inner);
	assert_eq!(logical_join_kinds(&normalized), vec![JoinKind::Inner]);
}

#[tokio::test]
async fn or_one_branch_is_null_does_not_demote() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(orders_customers_left_join()),
		predicate: Expr::Or(
			Box::new(customers_region_is_north()),
			Box::new(Expr::IsNull(Box::new(qcol("customers", "region")))),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert!(receipts.is_empty());
	assert!(
		logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
		"IS NULL branch should keep the left join outer: {normalized:?}"
	);
}

#[tokio::test]
async fn or_branches_reject_different_sides_do_not_demote() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(orders_customers_left_join()),
		predicate: Expr::Or(
			Box::new(Expr::Eq(
				Box::new(qcol("orders", "id")),
				Box::new(int64_literal(1)),
			)),
			Box::new(customers_region_is_north()),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert!(receipts.is_empty());
	assert!(
		logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
		"different-side OR branches should not demote: {normalized:?}"
	);
}

#[tokio::test]
async fn or_cross_side_conjunct_demotes_without_moving() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(orders_customers_left_join()),
		predicate: Expr::Or(
			Box::new(customers_region_is_north()),
			Box::new(Expr::And(
				Box::new(Expr::Eq(
					Box::new(qcol("customers", "region")),
					Box::new(utf8_literal("south")),
				)),
				Box::new(Expr::Eq(
					Box::new(qcol("orders", "id")),
					Box::new(int64_literal(10)),
				)),
			)),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(receipts[0].from_kind, JoinKind::LeftOuter);
	assert_eq!(receipts[0].to_kind, JoinKind::Inner);
	let LogicalPlan::Filter { predicate, input } = normalized else {
		panic!("cross-side OR should stay as a residual root filter");
	};
	let predicate_text = format!("{predicate:?}");
	assert!(predicate_text.contains("orders"), "{predicate_text}");
	assert!(predicate_text.contains("customers"), "{predicate_text}");
	assert_eq!(logical_join_kinds(&input), vec![JoinKind::Inner]);
}

#[tokio::test]
async fn alias_wrapped_left_join_filter_reaches_demotion() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Alias {
			input: Box::new(orders_customers_left_join()),
			alias: TableRef::new("oc"),
			columns: vec![
				"order_id".to_string(),
				"order_customer_id".to_string(),
				"order_product_id".to_string(),
				"order_store_id".to_string(),
				"customer_id".to_string(),
				"customer_region".to_string(),
			],
		}),
		predicate: Expr::Eq(
			Box::new(qcol("oc", "customer_region")),
			Box::new(utf8_literal("north")),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(
		receipts[0].propagation_path,
		vec![JoinSideStep::Alias(TableRef::new("oc"))]
	);
	assert_eq!(logical_join_kinds(&normalized), vec![JoinKind::Inner]);
}

#[tokio::test]
async fn alias_empty_columns_qualifier_only_remap_reaches_demotion() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Alias {
			input: Box::new(orders_customers_left_join()),
			alias: TableRef::new("oc"),
			columns: Vec::new(),
		}),
		predicate: Expr::Eq(
			Box::new(qcol("oc", "region")),
			Box::new(utf8_literal("north")),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(
		receipts[0].propagation_path,
		vec![JoinSideStep::Alias(TableRef::new("oc"))]
	);
	assert_eq!(logical_join_kinds(&normalized), vec![JoinKind::Inner]);
}

#[tokio::test]
async fn alias_remap_ambiguous_resolution_stops() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Alias {
			input: Box::new(orders_customers_left_join()),
			alias: TableRef::new("oc"),
			columns: Vec::new(),
		}),
		predicate: Expr::Eq(Box::new(qcol("oc", "id")), Box::new(int64_literal(1))),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert!(receipts.is_empty());
	assert!(
		logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
		"ambiguous alias output should stop motion: {normalized:?}"
	);
}

#[tokio::test]
async fn project_passthrough_filter_reaches_demotion() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(orders_customers_left_join()),
			exprs: vec![qcol("orders", "id"), qcol("customers", "region")],
			output_names: vec![
				Some("order_id".to_string()),
				Some("customer_region".to_string()),
			],
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("customer_region")),
			Box::new(utf8_literal("north")),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert_eq!(receipts.len(), 1);
	assert_eq!(receipts[0].propagation_path, vec![JoinSideStep::Project]);
	assert_eq!(logical_join_kinds(&normalized), vec![JoinKind::Inner]);
}

#[tokio::test]
async fn computed_project_column_stops_motion() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(orders_customers_left_join()),
			exprs: vec![Expr::Add(
				Box::new(qcol("customers", "id")),
				Box::new(int64_literal(1)),
			)],
			output_names: vec![Some("customer_id_plus".to_string())],
		}),
		predicate: Expr::Gt(
			Box::new(Expr::column("customer_id_plus")),
			Box::new(int64_literal(1)),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert!(receipts.is_empty());
	assert!(matches!(normalized, LogicalPlan::Filter { .. }));
	assert!(
		logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
		"computed projection should stop motion: {normalized:?}"
	);
}

#[tokio::test]
async fn duplicate_or_missing_project_output_name_stops_motion() {
	let catalog = mixed_outer_join_catalog();
	let duplicate = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(orders_customers_left_join()),
			exprs: vec![qcol("orders", "id"), qcol("customers", "region")],
			output_names: vec![
				Some("customer_region".to_string()),
				Some("customer_region".to_string()),
			],
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("customer_region")),
			Box::new(utf8_literal("north")),
		),
	};
	let missing = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(orders_customers_left_join()),
			exprs: vec![qcol("customers", "region")],
			output_names: vec![None],
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("region")),
			Box::new(utf8_literal("north")),
		),
	};

	for (name, logical) in [("duplicate", duplicate), ("missing", missing)] {
		let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

		assert!(receipts.is_empty(), "{name}");
		assert!(
			logical_join_kinds(&normalized).contains(&JoinKind::LeftOuter),
			"{name} project output should stop motion: {normalized:?}"
		);
	}
}

#[tokio::test]
async fn alias_project_residuals_preserve_original_filter_shape() {
	let catalog = mixed_outer_join_catalog();
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Alias {
			input: Box::new(LogicalPlan::Take {
				input: Box::new(orders_customers_left_join()),
				limit: 1,
			}),
			alias: TableRef::new("oc"),
			columns: vec![
				"order_id".to_string(),
				"order_customer_id".to_string(),
				"order_product_id".to_string(),
				"order_store_id".to_string(),
				"customer_id".to_string(),
				"customer_region".to_string(),
			],
		}),
		predicate: Expr::Eq(
			Box::new(qcol("oc", "customer_region")),
			Box::new(utf8_literal("north")),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert!(receipts.is_empty());
	let LogicalPlan::Filter { predicate, .. } = normalized else {
		panic!("stopped wrapper descent should preserve the root filter");
	};
	let predicate_text = format!("{predicate:?}");
	assert!(predicate_text.contains("oc"), "{predicate_text}");
	assert!(
		predicate_text.contains("customer_region"),
		"{predicate_text}"
	);

	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(LogicalPlan::Take {
				input: Box::new(orders_customers_left_join()),
				limit: 1,
			}),
			exprs: vec![qcol("orders", "id"), qcol("customers", "region")],
			output_names: vec![
				Some("order_id".to_string()),
				Some("customer_region".to_string()),
			],
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("customer_region")),
			Box::new(utf8_literal("north")),
		),
	};

	let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;

	assert!(receipts.is_empty());
	let LogicalPlan::Filter { predicate, .. } = normalized else {
		panic!("stopped project descent should preserve the root filter");
	};
	let predicate_text = format!("{predicate:?}");
	assert!(
		predicate_text.contains("customer_region"),
		"{predicate_text}"
	);
}

#[tokio::test]
async fn remapped_conjunct_results_match_unwrapped_query() {
	let catalog = mixed_outer_join_catalog();
	let unwrapped = LogicalPlan::Filter {
		input: Box::new(orders_customers_left_join()),
		predicate: customers_region_is_north(),
	};
	let alias_wrapped = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Alias {
			input: Box::new(orders_customers_left_join()),
			alias: TableRef::new("oc"),
			columns: Vec::new(),
		}),
		predicate: Expr::Eq(
			Box::new(qcol("oc", "region")),
			Box::new(utf8_literal("north")),
		),
	};
	let project_wrapped = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Project {
			input: Box::new(orders_customers_left_join()),
			exprs: vec![qcol("orders", "id"), qcol("customers", "region")],
			output_names: vec![
				Some("order_id".to_string()),
				Some("customer_region".to_string()),
			],
		}),
		predicate: Expr::Eq(
			Box::new(Expr::column("customer_region")),
			Box::new(utf8_literal("north")),
		),
	};

	let (unwrapped, unwrapped_receipts) = demote_outer_join_plan(&unwrapped, &catalog).await;
	let (alias_wrapped, alias_receipts) = demote_outer_join_plan(&alias_wrapped, &catalog).await;
	let (project_wrapped, project_receipts) =
		demote_outer_join_plan(&project_wrapped, &catalog).await;

	for (name, normalized, receipts) in [
		("unwrapped", unwrapped, unwrapped_receipts),
		("alias", alias_wrapped, alias_receipts),
		("project", project_wrapped, project_receipts),
	] {
		assert_eq!(receipts.len(), 1, "{name} should demote once");
		assert_eq!(receipts[0].to_kind, JoinKind::Inner, "{name}");
		assert_eq!(
			logical_join_kinds(&normalized),
			vec![JoinKind::Inner],
			"{name}: {normalized:?}"
		);
	}
}

#[tokio::test]
async fn rune_or_filter_and_alias_descent_match_sql() {
	let catalog = mixed_outer_join_catalog();
	let sql_or = LogicalPlan::Filter {
		input: Box::new(orders_customers_left_join()),
		predicate: Expr::Or(
			Box::new(customers_region_is_north()),
			Box::new(Expr::Eq(
				Box::new(qcol("customers", "region")),
				Box::new(utf8_literal("south")),
			)),
		),
	};
	let rune_or = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders")
                .left_join(
                    ctx.table("customers"),
                    qcol("orders", "customer_id").eq(qcol("customers", "id"))
                )
                .filter(
                    qcol("customers", "region").eq(lit_str("north"))
                        .or(qcol("customers", "region").eq(lit_str("south")))
                )
        }
    "#,
	)
	.await;
	let rune_alias = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders")
                .left_join(
                    ctx.table("customers"),
                    qcol("orders", "customer_id").eq(qcol("customers", "id"))
                )
                .alias("oc")
                .filter(qcol("oc", "region").eq(lit_str("north")))
        }
    "#,
	)
	.await;

	for (name, logical) in [
		("sql_or", sql_or),
		("rune_or", rune_or),
		("rune_alias", rune_alias),
	] {
		let (normalized, receipts) = demote_outer_join_plan(&logical, &catalog).await;
		assert_eq!(receipts.len(), 1, "{name} should demote once");
		assert_eq!(receipts[0].to_kind, JoinKind::Inner, "{name}");
		assert_eq!(
			logical_join_kinds(&normalized),
			vec![JoinKind::Inner],
			"{name}: {normalized:?}"
		);
	}
}

#[tokio::test]
async fn propagation_receipt_reports_alias_and_project_steps() {
	let logical = LogicalPlan::Explain {
		input: Box::new(LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Project {
				input: Box::new(orders_customers_left_join()),
				exprs: vec![qcol("orders", "id"), qcol("customers", "region")],
				output_names: vec![
					Some("order_id".to_string()),
					Some("customer_region".to_string()),
				],
			}),
			predicate: Expr::Eq(
				Box::new(Expr::column("customer_region")),
				Box::new(utf8_literal("north")),
			),
		}),
	};

	let (_schema, rows) = bind_explain_rows(
		&logical,
		&mixed_outer_join_catalog(),
		&PlannerConfig::default(),
	)
	.await;
	let row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("outer_join_demotion".to_string())))
		.expect("outer join demotion receipt row");

	assert_utf8_contains(&row[3], "propagated_path:project");
	assert_utf8_contains(&row[4], "customer_region");
}

#[tokio::test]
async fn left_outer_right_build_auto_broadcast_executes_as_physical_right_outer() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		kind: JoinKind::LeftOuter,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind safe left outer broadcast");
	let output_schema = plan.schema();
	assert_eq!(
		field_names(&output_schema),
		vec!["id", "customer_id", "id", "region"]
	);
	let ExecutionPlan::Join { kind, strategy, .. } = first_join_plan(&plan) else {
		panic!("plan should contain broadcast join");
	};
	assert_eq!(*kind, JoinKind::RightOuter);
	assert_eq!(*strategy, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn right_outer_natural_left_build_auto_broadcast_keeps_physical_right_outer() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		kind: JoinKind::RightOuter,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let (plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind right outer preserved-probe broadcast");
	assert_eq!(
		trace.join_decisions[0].placement,
		JoinPhysicalPlacement::Natural
	);
	let ExecutionPlan::Join { kind, strategy, .. } = first_join_plan(&plan) else {
		panic!("plan should contain broadcast join");
	};
	assert_eq!(*kind, JoinKind::RightOuter);
	assert_eq!(*strategy, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn left_semi_and_left_anti_auto_broadcast_build_right_without_restore_projection() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);

	for (declared, physical) in [
		(JoinKind::LeftSemi, JoinKind::RightSemi),
		(JoinKind::LeftAnti, JoinKind::RightAnti),
	] {
		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			kind: declared,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let (plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind one-sided safe broadcast");
		assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
		assert_eq!(
			trace.join_decisions[0].placement,
			JoinPhysicalPlacement::SwappedRightBuild
		);
		assert!(
			!matches!(plan, ExecutionPlan::Project { .. }),
			"one-sided broadcast must not use the two-sided restore projection"
		);
		let output_schema = plan.schema();
		assert_eq!(field_names(&output_schema), vec!["id", "customer_id"]);
		let ExecutionPlan::Join { kind, .. } = first_join_plan(&plan) else {
			panic!("plan should contain broadcast join");
		};
		assert_eq!(*kind, physical);
	}
}

#[tokio::test]
async fn direct_non_inner_preserved_side_broadcast_pin_stays_rejected() {
	let catalog = join_catalog();
	let logical = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("customers", "id", 1)),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		kind: JoinKind::RightOuter,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Direct,
			build_side: Some(BroadcastBuildSide::Right),
		},
	};

	let err = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect_err("direct non-inner preserved-side broadcast remains an authoring error");
	assert!(
		err.to_string()
			.contains("target the null-supplied side or drop the hint"),
		"{err}"
	);
}

#[tokio::test]
async fn sql_unhinted_unknown_and_large_joins_stay_hash() {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
		.expect("plan SQL join");

	let unknown = bind(&logical, &join_catalog(), &auto_broadcast_config(4_096))
		.await
		.expect("bind unknown-size join");
	assert_join_strategy(&unknown, JoinStrategy::HashPartitioned);

	let mut large_catalog = join_catalog();
	seed_byte_upper_bound(&mut large_catalog, "customers", 8_192);
	let large = bind(&logical, &large_catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind large join");
	assert_join_strategy(&large, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn planner_default_on_missing_facts_still_hashes() {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
		.expect("plan SQL join");

	let (plan, trace) = bind_with_trace(&logical, &join_catalog(), &PlannerConfig::default())
		.await
		.expect("bind missing-facts default join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
	assert_eq!(trace.join_decisions.len(), 1);
	assert_eq!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::MissingBuildByteUpperBound,
		}
	);
}

#[tokio::test]
async fn sql_fact_left_tiny_right_infers_broadcast() {
	let mut right_tiny_catalog = join_catalog();
	seed_byte_upper_bound(&mut right_tiny_catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut right_tiny_catalog, "customers", 1_024);
	let right_tiny = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan right-tiny SQL join");
	let right_tiny = bind(
		&right_tiny,
		&right_tiny_catalog,
		&auto_broadcast_config(4_096),
	)
	.await
	.expect("bind right-tiny join");
	assert_join_strategy(&right_tiny, JoinStrategy::Broadcast);
	let ExecutionPlan::Project {
		input,
		exprs,
		output_schema,
	} = right_tiny
	else {
		panic!("right-build broadcast must restore declared order with a project");
	};
	assert_eq!(
		field_names(&output_schema),
		vec!["id", "customer_id", "id", "region"]
	);
	assert_eq!(
		exprs
			.root_columns()
			.expect("right-build restore roots are columns")
			.into_iter()
			.map(|column| column.ordinal)
			.collect::<Vec<_>>(),
		vec![
			ColumnOrdinal(2),
			ColumnOrdinal(3),
			ColumnOrdinal(0),
			ColumnOrdinal(1)
		]
	);
	let ExecutionPlan::Join {
		left,
		right,
		left_keys,
		right_keys,
		output_schema,
		..
	} = input.as_ref()
	else {
		panic!("restore project must wrap the physical join");
	};
	assert_eq!(
		field_names(output_schema),
		vec!["id", "region", "id", "customer_id"]
	);
	assert!(matches!(terminal_exchange(left), Some((1, _))));
	assert!(terminal_exchange(right).is_none());
	assert_eq!(left_keys, &vec![ColumnOrdinal(0)]);
	assert_eq!(right_keys, &vec![ColumnOrdinal(1)]);
}

#[tokio::test]
async fn right_build_swapped_plan_is_valid_left_build_broadcast() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan right-tiny SQL join");

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind right-tiny join");
	let ExecutionPlan::Project { input, .. } = plan else {
		panic!("right-build broadcast must be restored after a physical swap");
	};
	let ExecutionPlan::Join {
		left,
		right,
		strategy,
		..
	} = input.as_ref()
	else {
		panic!("restore project must wrap physical broadcast join");
	};

	assert_eq!(*strategy, JoinStrategy::Broadcast);
	assert!(matches!(terminal_exchange(left), Some((1, _))));
	assert!(terminal_exchange(right).is_none());
}

#[tokio::test]
async fn both_sides_eligible_right_materially_smaller_prefers_right_build() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 3_072);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan SQL join");

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind dominance-selected right-build join");

	let ExecutionPlan::Project {
		input,
		output_schema,
		..
	} = plan
	else {
		panic!("right dominance should swap physical build and restore declared order");
	};
	assert_eq!(
		field_names(&output_schema),
		vec!["id", "customer_id", "id", "region"]
	);
	let ExecutionPlan::Join {
		left,
		right,
		strategy,
		..
	} = input.as_ref()
	else {
		panic!("restore project must wrap physical broadcast join");
	};
	assert_eq!(*strategy, JoinStrategy::Broadcast);
	assert!(matches!(terminal_exchange(left), Some((1, _))));
	assert!(terminal_exchange(right).is_none());

	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(3_072)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(1_024)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&auto_broadcast_config(4_096),
	)
	.expect("choose join strategy");
	assert_eq!(decision.placement, JoinPhysicalPlacement::SwappedRightBuild);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side: BroadcastBuildSide::Right,
			build_bytes: ByteCount::new(1_024),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: Some(ByteCount::new(3_072)),
		}
	);
}

#[tokio::test]
async fn trace_records_broadcast_inference_with_rejected_magnitude() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 3_072);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan SQL join");

	let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind with trace");

	assert_eq!(trace.join_decisions.len(), 1);
	let receipt = trace.join_decisions[0];
	assert_eq!(receipt.target.join_index, 0);
	assert_eq!(receipt.strategy, JoinStrategy::Broadcast);
	assert_eq!(receipt.placement, JoinPhysicalPlacement::SwappedRightBuild);
	assert_eq!(
		receipt.reason,
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side: BroadcastBuildSide::Right,
			build_bytes: ByteCount::new(1_024),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: Some(ByteCount::new(3_072)),
		}
	);
}

#[tokio::test]
async fn trace_records_left_tie_with_rejected_right_bytes() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 1_024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan SQL join");

	let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind with trace");

	assert_eq!(trace.join_decisions.len(), 1);
	let receipt = trace.join_decisions[0];
	assert_eq!(receipt.target.join_index, 0);
	assert_eq!(receipt.strategy, JoinStrategy::Broadcast);
	assert_eq!(receipt.placement, JoinPhysicalPlacement::Natural);
	assert_eq!(
		receipt.reason,
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side: BroadcastBuildSide::Left,
			build_bytes: ByteCount::new(1_024),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: Some(ByteCount::new(1_024)),
		}
	);
}

#[tokio::test]
async fn both_sides_tiny_equal_keeps_left_build() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 1_024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan SQL join");

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind tiny-tie join");

	let ExecutionPlan::Join {
		left,
		right,
		strategy,
		output_schema,
		..
	} = plan
	else {
		panic!("left tie winner should keep natural join shape");
	};
	assert_eq!(strategy, JoinStrategy::Broadcast);
	assert_eq!(
		field_names(&output_schema),
		vec!["id", "customer_id", "id", "region"]
	);
	assert!(matches!(terminal_exchange(&left), Some((1, _))));
	assert!(terminal_exchange(&right).is_none());

	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(1_024)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(1_024)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&auto_broadcast_config(4_096),
	)
	.expect("choose join strategy");
	assert_eq!(decision.placement, JoinPhysicalPlacement::Natural);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side: BroadcastBuildSide::Left,
			build_bytes: ByteCount::new(1_024),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: Some(ByteCount::new(1_024)),
		}
	);
}

#[test]
fn both_sides_eligible_near_tie_keeps_left_build() {
	let config = auto_broadcast_config(4_096);
	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(1_536)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(1_024)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&config,
	)
	.expect("choose join strategy");

	assert_eq!(decision.strategy, JoinStrategy::Broadcast);
	assert_eq!(decision.placement, JoinPhysicalPlacement::Natural);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side: BroadcastBuildSide::Left,
			build_bytes: ByteCount::new(1_536),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: Some(ByteCount::new(1_024)),
		}
	);
}

#[test]
fn hash_join_probe_not_scan_linear_uses_carried_input_cause() {
	let config = auto_broadcast_config(4_096);
	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input_with_linearity(
			PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(64 * 1024 * 1024)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			},
			ProbeLinearity::Blocked(ProbeLinearityFence::Sort),
		),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(1_024)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&config,
	)
	.expect("choose join strategy");

	assert_eq!(decision.strategy, JoinStrategy::HashPartitioned);
	assert_eq!(decision.placement, JoinPhysicalPlacement::Natural);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::ProbeNotScanLinear(ProbeLinearityFence::Sort),
		}
	);
}

#[tokio::test]
async fn right_tiny_left_deep_join_infers_broadcast_star() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
		)
		.expect("plan left-deep right-tiny SQL join");

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind left-deep probe right-tiny join");

	assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 2);
}

#[tokio::test]
async fn join_replay_id_deterministic_across_rebind() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
		)
		.expect("plan left-deep SQL join");

	let (plan_a, trace_a) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("first bind");
	let (plan_b, trace_b) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("second bind");

	let targets_a: Vec<_> = trace_a
		.join_decisions
		.iter()
		.map(|receipt| receipt.target.join_index)
		.collect();
	let targets_b: Vec<_> = trace_b
		.join_decisions
		.iter()
		.map(|receipt| receipt.target.join_index)
		.collect();
	assert_eq!(targets_a, vec![0, 1]);
	assert_eq!(targets_a, targets_b);

	let mut ids_a = Vec::new();
	let mut ids_b = Vec::new();
	collect_join_replay_ids(&plan_a, &mut ids_a);
	collect_join_replay_ids(&plan_b, &mut ids_b);
	assert_eq!(ids_a, vec![JoinReplayId(1), JoinReplayId(0)]);
	assert_eq!(ids_a, ids_b);
}

#[tokio::test]
async fn force_hash_join_ids_demotes_only_targeted_join() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
		)
		.expect("plan left-deep SQL join");
	let config = PlannerConfig {
		auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
		force_hash_join_ids: BTreeSet::from([JoinReplayId(1)]),
		..PlannerConfig::default()
	};

	let (plan, trace) = bind_with_trace(&logical, &catalog, &config)
		.await
		.expect("bind targeted replay");

	assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 1);
	assert_join_strategy_count(&plan, JoinStrategy::HashPartitioned, 1);
	assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
	assert_eq!(
		trace.join_decisions[1].strategy,
		JoinStrategy::HashPartitioned
	);
	assert!(matches!(
		trace.join_decisions[1].reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::HashAfterBroadcastCap
		}
	));
}

#[tokio::test]
async fn star_query_infers_broadcast_without_seed_optimizer_facts() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
		)
		.expect("plan descriptor-sidecar SQL join");

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind descriptor-sidecar broadcast star");

	assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 2);
}

#[tokio::test]
async fn rune_ctx_sql_star_query_infers_broadcast_without_seed_optimizer_facts() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM orders o \
                     INNER JOIN customers c1 ON o.customer_id = c1.id \
                     INNER JOIN customers c2 ON o.customer_id = c2.id")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind descriptor-sidecar Rune broadcast star");

	assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 2);
}

#[tokio::test]
async fn string_bearing_tiny_dim_infers_broadcast_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(12));
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM orders o \
                     INNER JOIN customers c ON o.customer_id = c.id")
        }
    "#,
	)
	.await;

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind estimate-backed Rune join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert_eq!(trace.join_decisions.len(), 1);
	match trace.join_decisions[0].reason {
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side,
			estimated_byte_size,
			threshold,
			rejected_side_bytes,
		} => {
			assert_eq!(build_side, BroadcastBuildSide::Right);
			assert!(estimated_byte_size < threshold);
			assert_eq!(rejected_side_bytes, None);
		}
		other => panic!("expected estimated broadcast receipt, got {other:?}"),
	}
}

#[tokio::test]
async fn pk_filtered_string_dim_broadcasts_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, None);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
		),
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered_customers)
		.await
		.expect("bind PK-filtered string dimension");
	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(1)));
	assert!(scoped.facts.estimated_byte_size.is_some());

	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(filtered_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind PK-filtered estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn direct_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(
			ColumnOrdinal(1),
			DistinctCountFact::Approx {
				estimate: 10_000,
				rel_err_ppm: 26_000,
			},
		)],
	);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
			Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
		),
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered_customers)
		.await
		.expect("bind NDV-filtered dimension");
	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(2)));
	assert!(scoped.facts.estimated_byte_size.is_some());

	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(filtered_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind NDV-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn in_list_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(
			ColumnOrdinal(1),
			DistinctCountFact::Approx {
				estimate: 10_000,
				rel_err_ppm: 26_000,
			},
		)],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.region IN ('north', 'south')",
		)
		.expect("plan SQL IN-list NDV-filtered dimension join");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL IN-list NDV-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn in_list_ndv_estimates_distinct_non_null_literals() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![utf8_literal("north"), utf8_literal("south")],
			false,
		),
	)
	.await
	.expect("bind IN-list NDV-filtered dimension");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(4)));
	assert!(scoped.facts.estimated_byte_size.is_some());
}

#[tokio::test]
async fn in_list_ndv_deduplicates_duplicate_literals() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![
				utf8_literal("north"),
				utf8_literal("north"),
				utf8_literal("south"),
			],
			false,
		),
	)
	.await
	.expect("bind duplicate-literal IN-list");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(4)));
}

#[tokio::test]
async fn in_list_ndv_ignores_null_literals() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![
				utf8_literal("north"),
				Expr::Literal(ScalarValue::Utf8(None)),
				utf8_literal("south"),
			],
			false,
		),
	)
	.await
	.expect("bind NULL-containing IN-list");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(4)));
}

#[tokio::test]
async fn date32_in_list_ndv_estimates_distinct_non_null_literals() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(10_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("signup_date", DataType::Date32, true),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"signup_date",
			vec![
				date32_literal(19_723),
				date32_literal(19_723),
				Expr::Literal(ScalarValue::Date32(None)),
				date32_literal(19_724),
			],
			false,
		),
	)
	.await
	.expect("bind Date32 IN-list NDV-filtered dimension");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(10_000)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(200)));
	assert!(scoped.facts.estimated_byte_size.is_some());
}

#[tokio::test]
async fn in_list_ndv_all_null_list_fails_open() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![
				Expr::Literal(ScalarValue::Utf8(None)),
				Expr::Literal(ScalarValue::Utf8(None)),
			],
			false,
		),
	)
	.await
	.expect("bind all-NULL IN-list");

	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn in_list_non_finite_float_literal_fails_open() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(1_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("score", DataType::Float64, true),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"score",
			vec![
				Expr::Literal(ScalarValue::Float64(Some(0.5))),
				Expr::Literal(ScalarValue::Float64(Some(f64::INFINITY))),
			],
			false,
		),
	)
	.await
	.expect("bind non-finite float IN-list");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(1_000)));
	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn in_list_k_exceeds_ndv_caps_at_base_rows() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(10),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(3))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![
				utf8_literal("north"),
				utf8_literal("south"),
				utf8_literal("east"),
				utf8_literal("west"),
			],
			false,
		),
	)
	.await
	.expect("bind IN-list with k above NDV");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(10)));
}

#[tokio::test]
async fn skewed_hot_key_filter_records_uniform_ndv_miss() {
	const BASE_ROWS: u64 = 10_000;
	const NDV: u64 = 100;
	const ACTUAL_HOT_ROWS: u64 = 5_000;
	const MISS_RATIO_THRESHOLD: u64 = 25;
	let uniform_estimate = RowCount::new(BASE_ROWS.div_ceil(NDV));
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(BASE_ROWS),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(NDV))],
	);

	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Eq(
			Box::new(customer_column("region")),
			Box::new(utf8_literal("hot")),
		),
	)
	.await
	.expect("bind skewed hot-key equality filter");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(BASE_ROWS)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(uniform_estimate));
	assert!(
		ACTUAL_HOT_ROWS >= uniform_estimate.get() * MISS_RATIO_THRESHOLD,
		"expected hot-key actual rows {ACTUAL_HOT_ROWS} to materially exceed uniform estimate {:?}",
		uniform_estimate
	);
}

#[tokio::test]
async fn uniform_ndv_hot_key_in_list_miss_is_measured() {
	const BASE_ROWS: u64 = 10_000;
	const NDV: u64 = 100;
	const ACTUAL_HOT_ROWS: u64 = 5_000;
	const ACTUAL_TAIL0_ROWS: u64 = 51;
	const MISS_RATIO_THRESHOLD: u64 = 20;
	let rows_per_value = BASE_ROWS.div_ceil(NDV);
	let expected_estimate = RowCount::new(rows_per_value * 2);
	let actual_rows = ACTUAL_HOT_ROWS + ACTUAL_TAIL0_ROWS;
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(BASE_ROWS),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(NDV))],
	);

	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![utf8_literal("hot"), utf8_literal("tail_0")],
			false,
		),
	)
	.await
	.expect("bind skewed hot-key IN-list filter");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(BASE_ROWS)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(expected_estimate));
	assert!(
		actual_rows >= expected_estimate.get() * MISS_RATIO_THRESHOLD,
		"expected IN-list actual rows {actual_rows} to materially exceed uniform estimate {:?}",
		expected_estimate
	);
}

#[tokio::test]
async fn skew_miss_measurement_does_not_change_estimated_row_count() {
	const BASE_ROWS: u64 = 10_000;
	const NDV: u64 = 100;
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(BASE_ROWS),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(NDV))],
	);

	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Eq(
			Box::new(customer_column("region")),
			Box::new(utf8_literal("hot")),
		),
	)
	.await
	.expect("bind skew evidence filter");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(BASE_ROWS.div_ceil(NDV)))
	);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(BASE_ROWS)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert!(scoped.facts.estimated_byte_size.is_some());
}

#[tokio::test]
async fn hot_key_equality_uses_heavy_hitter_frequency() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_and_heavy_hitters(
		Some(64 * 1024 * 1024),
		None,
		Some(10_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		Vec::new(),
		vec![(ColumnOrdinal(1), utf8_heavy_hitter_fact(&[("hot", 5_000)]))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Eq(
			Box::new(customer_column("region")),
			Box::new(utf8_literal("hot")),
		),
	)
	.await
	.expect("bind heavy-hitter equality");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(5_000)));
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(10_000)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
}

#[tokio::test]
async fn untracked_literal_uses_uniform_tail_not_global_uniform() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_and_heavy_hitters(
		Some(64 * 1024 * 1024),
		None,
		Some(10_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		Vec::new(),
		vec![(ColumnOrdinal(1), utf8_heavy_hitter_fact(&[("hot", 5_000)]))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Eq(
			Box::new(customer_column("region")),
			Box::new(utf8_literal("tail_0")),
		),
	)
	.await
	.expect("bind heavy-hitter tail equality");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(51)));
	assert_ne!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
}

#[tokio::test]
async fn hot_key_in_list_sums_tracked_frequencies_and_uniform_tail() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_and_heavy_hitters(
		Some(64 * 1024 * 1024),
		None,
		Some(10_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		Vec::new(),
		vec![(ColumnOrdinal(1), utf8_heavy_hitter_fact(&[("hot", 5_000)]))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![utf8_literal("hot"), utf8_literal("tail_0")],
			false,
		),
	)
	.await
	.expect("bind heavy-hitter IN-list");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(5_051)));
	assert_ne!(scoped.facts.estimated_row_count, Some(RowCount::new(200)));
}

#[tokio::test]
async fn missing_heavy_hitter_fact_preserves_141_148_uniform_ndv_behavior() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![utf8_literal("hot"), utf8_literal("tail_0")],
			false,
		),
	)
	.await
	.expect("bind IN-list without heavy hitters");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(200)));
}

#[tokio::test]
async fn invalid_heavy_hitter_fact_zero_freq_or_over_base_is_ignored() {
	for fact in [
		utf8_heavy_hitter_fact(&[("hot", 0)]),
		utf8_heavy_hitter_fact(&[("hot", 10_001)]),
	] {
		let catalog =
			descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_and_heavy_hitters(
				Some(64 * 1024 * 1024),
				None,
				Some(10_000),
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("region", DataType::Utf8, true),
				]),
				vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
				Vec::new(),
				vec![(ColumnOrdinal(1), fact)],
			);
		let scoped = bind_filtered_customers(
			&catalog,
			Expr::Eq(
				Box::new(customer_column("region")),
				Box::new(utf8_literal("hot")),
			),
		)
		.await
		.expect("bind invalid heavy-hitter equality");

		assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
	}
}

#[tokio::test]
async fn non_finite_or_unsupported_literal_fails_open_to_existing_behavior() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_and_heavy_hitters(
		Some(64 * 1024 * 1024),
		None,
		Some(1_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("score", DataType::Float64, true),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		Vec::new(),
		vec![(
			ColumnOrdinal(1),
			heavy_hitter_fact(vec![(ScalarLiteralKey::Float64(0.5_f64.to_bits()), 500)]),
		)],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"score",
			vec![
				Expr::Literal(ScalarValue::Float64(Some(0.5))),
				Expr::Literal(ScalarValue::Float64(Some(f64::INFINITY))),
			],
			false,
		),
	)
	.await
	.expect("bind unsupported heavy-hitter IN-list");

	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn heavy_hitter_estimate_stays_estimate_channel_not_hard_bound() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_and_heavy_hitters(
		Some(64 * 1024 * 1024),
		None,
		Some(10_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		Vec::new(),
		vec![(ColumnOrdinal(1), utf8_heavy_hitter_fact(&[("hot", 5_000)]))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Eq(
			Box::new(customer_column("region")),
			Box::new(utf8_literal("hot")),
		),
	)
	.await
	.expect("bind estimate-channel heavy-hitter filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(5_000)));
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(10_000)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[test]
fn heavy_hitter_fact_projection_remaps_with_scan_projection() {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int64, true),
		Field::new("region", DataType::Utf8, true),
	]));
	let descriptor = named_descriptor(41, "customers", schema.as_ref().clone());
	let plan = ExecutionPlan::Scan {
		local_table_id: TableId(41),
		table: descriptor.table_object_id,
		fingerprint: descriptor.physical_descriptor_fingerprint,
		schema,
		projection: None,
		pbc_program: None,
		row_limit: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: Vec::new(),
		column_ndv: vec![(ColumnOrdinal(2), DistinctCountFact::Exact(100))],
		column_range: Vec::new(),
		column_range_distributions: Vec::new(),
		column_heavy_hitters: vec![(ColumnOrdinal(2), utf8_heavy_hitter_fact(&[("hot", 5_000)]))],
		column_group_top_k: Default::default(),
		column_group_ndv: Default::default(),
	};

	let logical_types = LogicalTypeRegistry::default();
	let (optimized, _) = optimize_for_required(
		plan,
		Some(BTreeSet::from([ColumnOrdinal(2)])),
		&logical_types,
	)
	.expect("optimize scan projection");
	let ExecutionPlan::Scan {
		projection,
		schema,
		column_ndv,
		column_heavy_hitters,
		..
	} = optimized
	else {
		panic!("expected optimized scan");
	};

	assert_eq!(projection, Some(vec![ColumnOrdinal(2)]));
	assert_eq!(schema.field(0).name(), "region");
	assert_eq!(
		column_ndv,
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))]
	);
	assert_eq!(column_heavy_hitters.len(), 1);
	assert_eq!(column_heavy_hitters[0].0, ColumnOrdinal(0));
	assert_eq!(
		column_heavy_hitters[0]
			.1
			.entries
			.get(&ScalarLiteralKey::Utf8("hot".to_string())),
		Some(&5_000)
	);
}

#[test]
fn range_distribution_fact_projection_remaps_with_scan_projection() {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::Utf8, true),
		Field::new("amount", DataType::Int64, true),
	]));
	let descriptor = named_descriptor(42, "customers", schema.as_ref().clone());
	let plan = ExecutionPlan::Scan {
		local_table_id: TableId(42),
		table: descriptor.table_object_id,
		fingerprint: descriptor.physical_descriptor_fingerprint,
		schema,
		projection: None,
		pbc_program: None,
		row_limit: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: Vec::new(),
		column_ndv: Vec::new(),
		column_range: vec![(ColumnOrdinal(2), range_fact_i64(0, 10_000, 10_000))],
		column_range_distributions: vec![(ColumnOrdinal(2), range_distribution_fact_i64(17))],
		column_heavy_hitters: Vec::new(),
		column_group_top_k: Default::default(),
		column_group_ndv: Default::default(),
	};

	let logical_types = LogicalTypeRegistry::default();
	let (optimized, _) = optimize_for_required(
		plan,
		Some(BTreeSet::from([ColumnOrdinal(2)])),
		&logical_types,
	)
	.expect("optimize scan projection");
	let ExecutionPlan::Scan {
		projection,
		schema,
		column_range,
		column_range_distributions,
		..
	} = optimized
	else {
		panic!("expected optimized scan");
	};

	assert_eq!(projection, Some(vec![ColumnOrdinal(2)]));
	assert_eq!(schema.field(0).name(), "amount");
	assert_eq!(column_range[0].0, ColumnOrdinal(0));
	assert_eq!(column_range_distributions.len(), 1);
	assert_eq!(column_range_distributions[0].0, ColumnOrdinal(0));
	assert_eq!(column_range_distributions[0].1.rank_error_ppm, 17);
}

#[tokio::test]
async fn non_ndv_filters_do_not_emit_skew_evidence() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(10_000));

	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Eq(
			Box::new(customer_column("region")),
			Box::new(utf8_literal("hot")),
		),
	)
	.await
	.expect("bind non-NDV equality filter");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(10_000)));
	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn not_in_does_not_use_ndv_selectivity() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![utf8_literal("north"), utf8_literal("south")],
			true,
		),
	)
	.await
	.expect("bind NOT IN-list");

	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn in_list_without_ndv_or_row_bound_fails_open() {
	let missing_ndv = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(20_000));
	let missing_ndv_scoped = bind_filtered_customers(
		&missing_ndv,
		customer_in_list(
			"region",
			vec![utf8_literal("north"), utf8_literal("south")],
			false,
		),
	)
	.await
	.expect("bind IN-list without NDV");
	assert_eq!(missing_ndv_scoped.facts.estimated_row_count, None);

	let missing_row_bound = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		None,
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let missing_row_bound_scoped = bind_filtered_customers(
		&missing_row_bound,
		customer_in_list(
			"region",
			vec![utf8_literal("north"), utf8_literal("south")],
			false,
		),
	)
	.await
	.expect("bind IN-list without row bound");
	assert_eq!(missing_row_bound_scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn in_list_and_equality_keep_tightest_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(1_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
			Field::new("segment", DataType::Utf8, true),
		]),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(10)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(1_000)),
		],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::And(
			Box::new(customer_in_list(
				"region",
				vec![utf8_literal("north"), utf8_literal("south")],
				false,
			)),
			Box::new(Expr::Eq(
				Box::new(customer_column("segment")),
				Box::new(utf8_literal("retail")),
			)),
		),
	)
	.await
	.expect("bind IN-list and equality filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(1)));
	assert_ne!(scoped.facts.estimated_row_count, Some(RowCount::new(200)));
}

#[tokio::test]
async fn in_list_does_not_write_hard_bounds_or_empty_relation() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		customer_in_list(
			"region",
			vec![utf8_literal("north"), utf8_literal("south")],
			false,
		),
	)
	.await
	.expect("bind IN-list estimate-channel filter");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(4)));
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn equality_ndv_existing_behavior_unchanged() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Eq(
			Box::new(customer_column("region")),
			Box::new(utf8_literal("north")),
		),
	)
	.await
	.expect("bind equality NDV-filtered dimension");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(2)));
}

#[tokio::test]
async fn rune_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(
			ColumnOrdinal(1),
			DistinctCountFact::Approx {
				estimate: 10_000,
				rel_err_ppm: 26_000,
			},
		)],
	);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("customers").alias("c")
               .filter(qcol("c", "region").eq(lit_str("north")))
               .auto_join(
                   ctx.table("orders").alias("o"),
                   qcol("c", "id").eq(qcol("o", "customer_id"))
               )
        }
    "#,
	)
	.await;

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune NDV-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Left,
			..
		}
	));
}

#[tokio::test]
async fn rune_equality_filtered_dim_broadcasts_over_wendelon_ndv_no_seed() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(
			ColumnOrdinal(1),
			DistinctCountFact::Approx {
				estimate: 10_000,
				rel_err_ppm: 26_000,
			},
		)],
	);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("customers").alias("c")
               .filter(qcol("c", "region").eq(lit_str("north")))
               .auto_join(
                   ctx.table("orders").alias("o"),
                   qcol("c", "id").eq(qcol("o", "customer_id"))
               )
        }
    "#,
	)
	.await;

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind Rune NDV-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Left,
			..
		}
	));
}

#[tokio::test]
async fn sql_equality_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(
			ColumnOrdinal(1),
			DistinctCountFact::Approx {
				estimate: 10_000,
				rel_err_ppm: 26_000,
			},
		)],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.region = 'north'",
		)
		.expect("plan SQL NDV-filtered dimension join");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL NDV-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn sql_inner_join_pushes_leftmost_local_conjunct_to_left_input() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(
			ColumnOrdinal(1),
			DistinctCountFact::Approx {
				estimate: 10_000,
				rel_err_ppm: 26_000,
			},
		)],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM customers c \
				 INNER JOIN orders o ON c.id = o.customer_id \
				 WHERE c.region = 'north'",
		)
		.expect("plan SQL leftmost-filtered dimension join");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL leftmost NDV-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Left,
			..
		}
	));
}

#[tokio::test]
async fn sql_multi_dim_star_each_filtered_dim_broadcasts() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(
			ColumnOrdinal(1),
			DistinctCountFact::Approx {
				estimate: 10_000,
				rel_err_ppm: 26_000,
			},
		)],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id \
				 WHERE c1.region = 'west' AND c2.region = 'east'",
		)
		.expect("plan SQL filtered two-dimension star");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL filtered two-dimension star");

	assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 2);
	assert_eq!(trace.join_decisions.len(), 2);
	assert!(trace.join_decisions.iter().all(|decision| matches!(
		decision.reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	)));
}

#[tokio::test]
async fn missing_ndv_keeps_137_138_behavior() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(20_000));
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
			Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
		),
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered_customers)
		.await
		.expect("bind filtered dimension without NDV");
	assert_eq!(scoped.facts.estimated_row_count, None);

	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(filtered_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind missing-NDV dimension join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn ndv_selectivity_writes_estimated_row_count_not_hard_bound() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(
			ColumnOrdinal(1),
			DistinctCountFact::Approx {
				estimate: 10_000,
				rel_err_ppm: 26_000,
			},
		)],
	);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
			Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
		),
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered_customers)
		.await
		.expect("bind NDV-filtered dimension");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(2)));
	assert!(scoped.facts.estimated_byte_size.is_some());
}

#[tokio::test]
async fn exact_ndv_still_uses_estimate_channel() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
			Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
		),
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered_customers)
		.await
		.expect("bind exact-NDV filtered dimension");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(2)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
}

#[tokio::test]
async fn range_filter_uses_min_max_range_selectivity_not_ndv() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int64, true),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1))],
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(89)))),
		),
	)
	.await
	.expect("bind range-filtered dimension with NDV present");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
	assert_ne!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(20_000))
	);
}

#[tokio::test]
async fn range_filter_estimates_without_row_upper_bound() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Lte(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(9)))),
		),
	)
	.await
	.expect("bind row-bound-free range filter");

	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
	assert!(scoped.facts.estimated_byte_size.is_some());
}

#[tokio::test]
async fn date32_comparisons_coerce_bare_string_literals_only_against_date32() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(10_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("signup_date", DataType::Date32, true),
			Field::new("region", DataType::Utf8, true),
		]),
		Vec::new(),
	);

	for sql in [
		"SELECT * FROM customers WHERE signup_date = '2024-01-01'",
		"SELECT * FROM customers WHERE signup_date IN ('2024-01-01', DATE '2024-01-02', NULL)",
	] {
		let logical = SqlPlanner::new().plan(sql).expect("plan Date32 SQL");
		Binder::new(&catalog, &PlannerConfig::default())
			.bind_plan(&logical)
			.await
			.expect("bind Date32 string coercion");
	}

	for (sql, diagnostic) in [
		(
			"SELECT * FROM customers WHERE signup_date = '2024-02-30'",
			"invalid Date32 literal",
		),
		(
			"SELECT * FROM customers WHERE signup_date = 19723",
			"comparison operands have different types",
		),
		(
			"SELECT * FROM customers WHERE region = DATE '2024-01-01'",
			"comparison operands have different types",
		),
	] {
		let logical = SqlPlanner::new().plan(sql).expect("plan Date32 SQL");
		let err = Binder::new(&catalog, &PlannerConfig::default())
			.bind_plan(&logical)
			.await;
		let Err(err) = err else {
			panic!("invalid Date32 comparison should reject");
		};
		assert!(
			err.to_string().contains(diagnostic),
			"expected diagnostic {diagnostic:?}, got {err}"
		);
	}
}

#[tokio::test]
async fn range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.amount BETWEEN 10 AND 19",
		)
		.expect("plan SQL range-filtered dimension join");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL range-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn date32_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_date_range(range_fact_date32(
		19_723, 19_822, 1_000,
	));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
					 INNER JOIN customers c ON o.customer_id = c.id \
					 WHERE c.signup_date BETWEEN DATE '2024-01-10' AND DATE '2024-01-19'",
		)
		.expect("plan SQL Date32 range-filtered dimension join");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL Date32 range-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn timestamp_session_zone_range_filtered_dim_infers_broadcast_by_estimate() {
	let catalog =
		descriptor_sidecar_join_catalog_with_customer_timestamp_range(range_fact_timestamp_ms(
			timestamp_ms("2025-12-01T06:00:00Z"),
			timestamp_ms("2026-02-01T06:00:00Z"),
			20_000,
		));
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.observed_at >= '2026-01-01'",
		)
		.expect("plan SQL timestamp range-filtered dimension join");
	let config = PlannerConfig::default().with_session_timezone("America/Chicago");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &config)
		.await
		.expect("bind SQL timestamp range-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn float_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("score", DataType::Float64, true),
		]),
		Vec::new(),
		vec![(ColumnOrdinal(1), range_fact_f64(0.0, 1.0, 1_000))],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.score >= 0.8",
		)
		.expect("plan SQL float range-filtered dimension join");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind SQL float range-estimated dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn sql_join_projected_derived_dim_broadcasts_where_full_column_hashes() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		wide_customer_sql_schema(),
		Vec::new(),
		vec![(ColumnOrdinal(1), range_fact_i32(0, 99, 1_000))],
	);
	let config = auto_broadcast_config(32 * 1024);
	let full_column = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.amount BETWEEN 10 AND 19",
		)
		.expect("plan full-column range-filtered join");
	let projected_derived = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN (SELECT id, amount FROM customers WHERE amount BETWEEN 10 AND 19) c \
				 ON o.customer_id = c.id",
		)
		.expect("plan projected derived range-filtered join");

	let (full_plan, full_trace) = bind_with_trace(&full_column, &catalog, &config)
		.await
		.expect("bind full-column filtered join");
	let (projected_plan, projected_trace) = bind_with_trace(&projected_derived, &catalog, &config)
		.await
		.expect("bind projected derived filtered join");

	assert_join_strategy(&full_plan, JoinStrategy::HashPartitioned);
	assert!(matches!(
		full_trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::BuildByteUpperBoundAboveThreshold
		}
	));
	match first_join_plan(&projected_plan) {
		ExecutionPlan::Join { strategy, .. } => assert_eq!(
			*strategy,
			JoinStrategy::Broadcast,
			"projected derived join trace: {:?}",
			projected_trace.join_decisions
		),
		other => panic!("expected join, got {other:?}"),
	}
	match projected_trace.join_decisions[0].reason {
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side,
			estimated_byte_size,
			threshold,
			..
		} => {
			assert_eq!(build_side, BroadcastBuildSide::Right);
			assert!(estimated_byte_size < threshold);
		}
		other => panic!("expected estimated broadcast, got {other:?}"),
	}
}

#[tokio::test]
async fn sql_join_right_derived_range_filtered_dim_broadcasts_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN (SELECT id, amount FROM customers WHERE amount BETWEEN 10 AND 19) c \
				 ON o.customer_id = c.id",
		)
		.expect("plan right derived range-filtered join");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind right derived range-filtered join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn sql_join_left_derived_range_filtered_dim_broadcasts_by_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM (SELECT id, amount FROM customers WHERE amount BETWEEN 10 AND 19) c \
				 INNER JOIN orders o ON c.id = o.customer_id",
		)
		.expect("plan left derived range-filtered join");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind left derived range-filtered join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Left,
			..
		}
	));
}

#[tokio::test]
async fn sql_join_derived_alias_column_list_resolves_join_keys() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT c.cid, c.label \
				 FROM orders o \
				 INNER JOIN (SELECT id, region FROM customers) c(cid, label) \
				 ON o.customer_id = c.cid",
		)
		.expect("plan derived alias-column join");

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind derived alias-column join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn sql_broadcast_hint_targets_derived_alias() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * \
				 FROM (SELECT id, region FROM customers) c \
				 INNER JOIN orders o ON c.id = o.customer_id",
		)
		.expect("plan broadcast hint targeting derived alias");

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind broadcast-hinted derived join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn comma_join_keys_projected_aggregate_cte_group_key() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, None);
	let logical = SqlPlanner::new()
		.plan(
			"WITH revenue AS ( \
			   SELECT customer_id AS supplier_no, SUM(id) AS total_revenue \
			   FROM orders \
			   GROUP BY customer_id \
			 ) \
			 SELECT id, total_revenue \
			 FROM customers, revenue \
			 WHERE id = supplier_no",
		)
		.expect("plan aggregate CTE comma join");

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("aggregate CTE group key must key the comma join");

	let ExecutionPlan::Join {
		kind,
		left_keys,
		right_keys,
		output_schema,
		..
	} = first_join_plan(&plan)
	else {
		panic!("expected keyed join, got {plan:?}");
	};
	assert_eq!(*kind, JoinKind::Inner);
	assert_eq!(left_keys.len(), 1);
	assert_eq!(right_keys.len(), 1);
	assert!(output_schema.field_with_name("id").is_ok());
	assert!(output_schema.field_with_name("supplier_no").is_ok());
	assert!(output_schema.field_with_name("total_revenue").is_ok());
}

#[tokio::test]
async fn range_lt_and_gt_estimate_clipped_fraction() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::And(
			Box::new(Expr::Gt(
				Box::new(customer_column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(-10)))),
			)),
			Box::new(Expr::Lt(
				Box::new(customer_column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(10)))),
			)),
		),
	)
	.await
	.expect("bind clipped range filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
}

#[tokio::test]
async fn range_between_intersects_same_column_bounds() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let scoped = bind_filtered_customers(&catalog, customer_amount_between(10, 19))
		.await
		.expect("bind intersected range filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
	assert_ne!(scoped.facts.estimated_row_count, Some(RowCount::new(200)));
}

#[tokio::test]
async fn range_skew_measurement_does_not_change_estimated_rows_for_range() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 10_000, 10_000))],
	);
	let scoped = bind_filtered_customers(&catalog, customer_amount_between(0, 100))
		.await
		.expect("bind range-skew evidence filter");
	let estimated_rows = RowCount::new(101);

	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(estimated_rows));
	assert_eq!(
		scoped.facts.estimated_byte_size,
		estimate_materialized_byte_size(estimated_rows, scoped.plan.schema().as_ref())
	);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn absent_range_distribution_falls_back_to_min_max_uniform() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 10_000, 10_000))],
	);
	let scoped = bind_filtered_customers(&catalog, customer_amount_between(0, 100))
		.await
		.expect("bind range filter without distribution");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(101)));
}

#[tokio::test]
async fn distribution_refines_in_envelope_min_max_still_zeroes_out_of_range() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range_distribution(
		vec![(ColumnOrdinal(1), range_fact_i64(0, 10_000, 10_000))],
		vec![(ColumnOrdinal(1), range_distribution_fact_i64(17))],
	);
	let clustered = bind_filtered_customers(&catalog, customer_amount_between(0, 100))
		.await
		.expect("bind clustered range filter with distribution");
	let outside = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(20_000)))),
		),
	)
	.await
	.expect("bind out-of-envelope range filter with distribution");

	assert_eq!(
		clustered.facts.estimated_row_count,
		Some(RowCount::new(9_000))
	);
	assert_eq!(outside.facts.estimated_row_count, Some(RowCount::new(0)));
	assert!(!matches!(outside.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn malformed_or_mismatched_range_distribution_fails_open() {
	let uniform = Some(RowCount::new(101));
	for fact in [
		ColumnRangeDistributionFact {
			arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT64,
			non_null_count: 10_000,
			rank_error_ppm: 17,
			buckets: Vec::new(),
		},
		range_distribution_fact_i32(17),
	] {
		let catalog = descriptor_sidecar_join_catalog_with_customer_range_distribution(
			vec![(ColumnOrdinal(1), range_fact_i64(0, 10_000, 10_000))],
			vec![(ColumnOrdinal(1), fact)],
		);
		let scoped = bind_filtered_customers(&catalog, customer_amount_between(0, 100))
			.await
			.expect("bind malformed/mismatched range-distribution filter");

		assert_eq!(scoped.facts.estimated_row_count, uniform);
	}
}

#[tokio::test]
async fn range_distribution_estimate_stays_estimate_channel_not_hard_bound() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range_distribution(
		vec![(ColumnOrdinal(1), range_fact_i64(0, 10_000, 10_000))],
		vec![(ColumnOrdinal(1), range_distribution_fact_i64(17))],
	);
	let scoped = bind_filtered_customers(&catalog, customer_amount_between(0, 100))
		.await
		.expect("bind clustered range filter with distribution");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(9_000)));
	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn range_distribution_rank_error_is_visible_on_fact() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range_distribution(
		vec![(ColumnOrdinal(1), range_fact_i64(0, 10_000, 10_000))],
		vec![(ColumnOrdinal(1), range_distribution_fact_i64(26_000))],
	);
	let plan = bind(
		&LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		},
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind scan with range distribution");
	let ExecutionPlan::Scan {
		column_range_distributions,
		..
	} = plan
	else {
		panic!("expected scan");
	};

	assert_eq!(column_range_distributions.len(), 1);
	assert_eq!(column_range_distributions[0].1.rank_error_ppm, 26_000);
}

#[tokio::test]
async fn min_max_envelope_pruning_still_correct_out_of_range_is_zero() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 10_000, 10_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(20_000)))),
		),
	)
	.await
	.expect("bind out-of-envelope range-skew filter");

	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
	assert_eq!(
		scoped.facts.estimated_byte_size,
		estimate_materialized_byte_size(RowCount::new(0), scoped.plan.schema().as_ref())
	);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn float_range_continuous_midpoint_estimates_half_non_null_rows() {
	let catalog =
		descriptor_sidecar_join_catalog_with_customer_score_range(range_fact_f64(0.0, 1.0, 1_000));
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Lte(
			Box::new(customer_column("score")),
			Box::new(Expr::Literal(ScalarValue::Float64(Some(0.5)))),
		),
	)
	.await
	.expect("bind midpoint float range filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(500)));
}

#[tokio::test]
async fn float_range_endpoint_inclusive_ge_max_estimates_one_row_but_gt_max_zero() {
	let catalog =
		descriptor_sidecar_join_catalog_with_customer_score_range(range_fact_f64(0.0, 1.0, 1_000));
	let inclusive = bind_filtered_customers(
		&catalog,
		Expr::Gte(
			Box::new(customer_column("score")),
			Box::new(Expr::Literal(ScalarValue::Float64(Some(1.0)))),
		),
	)
	.await
	.expect("bind inclusive endpoint float range filter");
	let exclusive = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("score")),
			Box::new(Expr::Literal(ScalarValue::Float64(Some(1.0)))),
		),
	)
	.await
	.expect("bind exclusive endpoint float range filter");

	assert_eq!(inclusive.facts.estimated_row_count, Some(RowCount::new(1)));
	assert_eq!(exclusive.facts.estimated_row_count, Some(RowCount::new(0)));
}

#[tokio::test]
async fn float_range_constant_column_min_equals_max_no_div_by_zero() {
	let catalog =
		descriptor_sidecar_join_catalog_with_customer_score_range(range_fact_f64(7.0, 7.0, 123));
	let at_value = bind_filtered_customers(
		&catalog,
		Expr::Gte(
			Box::new(customer_column("score")),
			Box::new(Expr::Literal(ScalarValue::Float64(Some(7.0)))),
		),
	)
	.await
	.expect("bind constant float range matching value");
	let above_value = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("score")),
			Box::new(Expr::Literal(ScalarValue::Float64(Some(7.0)))),
		),
	)
	.await
	.expect("bind constant float range above value");

	assert_eq!(at_value.facts.estimated_row_count, Some(RowCount::new(123)));
	assert_eq!(
		above_value.facts.estimated_row_count,
		Some(RowCount::new(0))
	);
}

#[tokio::test]
async fn pure_subset_project_shrinks_estimated_byte_size() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		wide_customer_schema(),
		Vec::new(),
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: customer_amount_between(10, 19),
	};
	let filtered = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered_customers)
		.await
		.expect("bind full-width filtered customers");
	let projected_customers = LogicalPlan::Project {
		input: Box::new(filtered_customers),
		exprs: vec![customer_column("id"), customer_column("amount")],
		output_names: vec![Some("id".to_string()), Some("amount".to_string())],
	};
	let projected = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&projected_customers)
		.await
		.expect("bind projected filtered customers");

	assert_eq!(filtered.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(projected.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(filtered.facts.byte_upper_bound, None);
	assert_eq!(projected.facts.byte_upper_bound, None);
	assert_eq!(filtered.facts.estimated_row_count, Some(RowCount::new(100)));
	assert_eq!(
		projected.facts.estimated_row_count,
		Some(RowCount::new(100))
	);
	let filtered_bytes = filtered
		.facts
		.estimated_byte_size
		.expect("full-width filter should derive estimated bytes");
	let projected_bytes = projected
		.facts
		.estimated_byte_size
		.expect("pure subset project should re-derive estimated bytes");
	assert!(
		projected_bytes < filtered_bytes,
		"projected bytes {projected_bytes:?} should shrink from full-width {filtered_bytes:?}"
	);
	let expected_projected = estimate_materialized_byte_size(
		RowCount::new(100),
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int64, true),
		]),
	)
	.expect("projected fixed-width estimate");
	assert_eq!(
		projected.facts.estimated_byte_size,
		Some(expected_projected)
	);
}

#[tokio::test]
async fn expression_project_keeps_byte_estimate_none() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		wide_customer_schema(),
		Vec::new(),
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let projected_customers = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: customer_amount_between(10, 19),
		}),
		exprs: vec![
			customer_column("id"),
			Expr::Add(
				Box::new(customer_column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		],
		output_names: vec![Some("id".to_string()), Some("next_amount".to_string())],
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&projected_customers)
		.await
		.expect("bind expression project over filtered customers");

	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_byte_size, None);
}

#[tokio::test]
async fn range_multiple_bounds_keep_tightest_interval() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::And(
			Box::new(Expr::And(
				Box::new(Expr::Gte(
					Box::new(customer_column("amount")),
					Box::new(Expr::Literal(ScalarValue::Int64(Some(10)))),
				)),
				Box::new(Expr::Gte(
					Box::new(customer_column("amount")),
					Box::new(Expr::Literal(ScalarValue::Int64(Some(50)))),
				)),
			)),
			Box::new(Expr::And(
				Box::new(Expr::Lte(
					Box::new(customer_column("amount")),
					Box::new(Expr::Literal(ScalarValue::Int64(Some(90)))),
				)),
				Box::new(Expr::Lte(
					Box::new(customer_column("amount")),
					Box::new(Expr::Literal(ScalarValue::Int64(Some(70)))),
				)),
			)),
		),
	)
	.await
	.expect("bind multiple-bound range filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(210)));
}

#[tokio::test]
async fn range_constant_column_min_equals_max_no_div_by_zero() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(7, 7, 123))],
	);
	let at_value = bind_filtered_customers(&catalog, customer_amount_between(7, 7))
		.await
		.expect("bind constant-column matching range");
	let above_value = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
		),
	)
	.await
	.expect("bind constant-column disjoint range");

	assert_eq!(at_value.facts.estimated_row_count, Some(RowCount::new(123)));
	assert_eq!(
		above_value.facts.estimated_row_count,
		Some(RowCount::new(0))
	);
}

#[tokio::test]
async fn timestamp_string_range_uses_session_timezone_for_estimate() {
	let instant = timestamp_ms("2024-03-10T08:00:00Z");
	let catalog = descriptor_sidecar_join_catalog_with_customer_timestamp_range(
		range_fact_timestamp_ms(instant, instant, 10),
	);
	let scoped = bind_filtered_customers_with_config(
		&catalog,
		customer_timestamp_between_strings("2024-03-10 03:00:00", "2024-03-10 03:00:00"),
		PlannerConfig::default().with_session_timezone("America/Chicago"),
	)
	.await
	.expect("bind timestamp range with session timezone");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(10)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
}

#[tokio::test]
async fn bare_string_timestamp_interpreted_in_session_zone() {
	let central_midnight = timestamp_ms("2024-01-01T06:00:00Z");
	let catalog = descriptor_sidecar_join_catalog_with_customer_timestamp_range(
		range_fact_timestamp_ms(central_midnight, central_midnight, 10),
	);

	let central = bind_filtered_customers_with_config(
		&catalog,
		customer_timestamp_between_strings("2024-01-01 00:00:00", "2024-01-01 00:00:00"),
		PlannerConfig::default().with_session_timezone("America/Chicago"),
	)
	.await
	.expect("bind Central timestamp range");
	let eastern = bind_filtered_customers_with_config(
		&catalog,
		customer_timestamp_between_strings("2024-01-01 00:00:00", "2024-01-01 00:00:00"),
		PlannerConfig::default().with_session_timezone("America/New_York"),
	)
	.await
	.expect("bind Eastern timestamp range");

	assert_eq!(central.facts.estimated_row_count, Some(RowCount::new(10)));
	assert_eq!(eastern.facts.estimated_row_count, Some(RowCount::new(0)));
}

#[tokio::test]
async fn bare_string_timestamp_with_explicit_offset_normalizes_to_utc() {
	let utc_midnight = timestamp_ms("2024-01-01T00:00:00Z");
	let catalog = descriptor_sidecar_join_catalog_with_customer_timestamp_range(
		range_fact_timestamp_ms(utc_midnight, utc_midnight, 10),
	);

	let explicit_z = bind_filtered_customers_with_config(
		&catalog,
		customer_timestamp_between_strings("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
		PlannerConfig::default().with_session_timezone("America/Chicago"),
	)
	.await
	.expect("bind explicit-offset timestamp string range");
	let explicit_offset = bind_filtered_customers_with_config(
		&catalog,
		customer_timestamp_between_strings(
			"2023-12-31T18:00:00-06:00",
			"2023-12-31T18:00:00-06:00",
		),
		PlannerConfig::default().with_session_timezone("America/New_York"),
	)
	.await
	.expect("bind explicit numeric-offset timestamp string range");

	assert_eq!(
		explicit_z.facts.estimated_row_count,
		Some(RowCount::new(10))
	);
	assert_eq!(
		explicit_offset.facts.estimated_row_count,
		Some(RowCount::new(10))
	);
}

#[test]
fn insert_timestamp_bare_string_with_explicit_offset_normalizes_to_utc() {
	let field = Field::new(
		"observed_at",
		crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond),
		true,
	);
	let value = coerce_insert_literal(
		&InsertLiteral::Utf8("2024-01-01T00:00:00Z".to_string()),
		&field,
		"America/Chicago".parse::<Tz>().expect("session timezone"),
	)
	.expect("coerce explicit-offset timestamp string");

	assert_eq!(
		value,
		ScalarValue::Timestamp {
			value: Some(timestamp_ms("2024-01-01T00:00:00Z")),
			unit: TimeUnit::Millisecond,
		}
	);
}

#[tokio::test]
async fn timestamp_string_dst_gap_is_binding_error() {
	let instant = timestamp_ms("2024-03-10T08:00:00Z");
	let catalog = descriptor_sidecar_join_catalog_with_customer_timestamp_range(
		range_fact_timestamp_ms(instant, instant, 10),
	);
	let err = match bind_filtered_customers_with_config(
		&catalog,
		Expr::Gte(
			Box::new(customer_column("observed_at")),
			Box::new(utf8_literal("2024-03-10 02:30:00")),
		),
		PlannerConfig::default().with_session_timezone("America/Chicago"),
	)
	.await
	{
		Ok(_) => panic!("DST gap should reject"),
		Err(error) => error,
	};

	assert!(err.to_string().contains("does not exist"));
}

#[tokio::test]
async fn timestamp_string_dst_fold_chooses_earlier_instant() {
	let earlier_fold = timestamp_ms("2024-11-03T06:30:00Z");
	let catalog = descriptor_sidecar_join_catalog_with_customer_timestamp_range(
		range_fact_timestamp_ms(earlier_fold, earlier_fold, 10),
	);
	let scoped = bind_filtered_customers_with_config(
		&catalog,
		customer_timestamp_between_strings("2024-11-03 01:30:00", "2024-11-03 01:30:00"),
		PlannerConfig::default().with_session_timezone("America/Chicago"),
	)
	.await
	.expect("bind ambiguous timestamp fold");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(10)));
}

#[test]
fn extract_hour_from_timestamp_at_time_zone_uses_named_zone() {
	let program = bind_temporal_expr(
		Expr::Extract(
			TemporalExtractField::Hour,
			Box::new(Expr::AtTimeZone(
				Box::new(Expr::column("event_time")),
				"America/Chicago".to_string(),
			)),
		),
		"America/New_York",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-01T06:30:00Z"), 0),
	)
	.expect("evaluate extract");
	let hours = outputs[0]
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("hour output");

	assert_eq!(hours.value(0), 0);
}

#[test]
fn extract_utc_column_without_at_time_zone_uses_session_zone() {
	let central = bind_temporal_expr(
		Expr::Extract(
			TemporalExtractField::Hour,
			Box::new(Expr::column("event_time")),
		),
		"America/Chicago",
	);
	let eastern = bind_temporal_expr(
		Expr::Extract(
			TemporalExtractField::Hour,
			Box::new(Expr::column("event_time")),
		),
		"America/New_York",
	);
	let batch = temporal_batch(timestamp_ms("2024-01-01T06:30:00Z"), 0);
	let central = Executor::evaluate_bound_program_without_storage(&central, &batch)
		.expect("evaluate Central extract");
	let eastern = Executor::evaluate_bound_program_without_storage(&eastern, &batch)
		.expect("evaluate Eastern extract");

	assert_eq!(
		central[0]
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("Central hour")
			.value(0),
		0
	);
	assert_eq!(
		eastern[0]
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("Eastern hour")
			.value(0),
		1
	);
}

#[test]
fn extract_at_time_zone_utc_uses_utc_not_session_zone() {
	let program = bind_temporal_expr(
		Expr::Extract(
			TemporalExtractField::Hour,
			Box::new(Expr::AtTimeZone(
				Box::new(Expr::column("event_time")),
				"UTC".to_string(),
			)),
		),
		"America/Chicago",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-01T06:30:00Z"), 0),
	)
	.expect("evaluate UTC extract");

	assert_eq!(
		outputs[0]
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("UTC hour")
			.value(0),
		6
	);
}

#[test]
fn date_trunc_day_returns_utc_instant_for_session_local_midnight() {
	let program = bind_temporal_expr(
		Expr::DateTrunc(
			TemporalTruncField::Day,
			Box::new(Expr::column("event_time")),
		),
		"America/Chicago",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-01T12:34:56Z"), 0),
	)
	.expect("evaluate date_trunc");
	let truncated = outputs[0]
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
		.expect("truncated timestamp");

	assert_eq!(truncated.value(0), timestamp_ms("2024-01-01T06:00:00Z"));
	assert_eq!(
		truncated.data_type(),
		&DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
	);
}

#[test]
fn date_trunc_day_at_time_zone_returns_utc_instant_for_local_midnight() {
	let program = bind_temporal_expr(
		Expr::DateTrunc(
			TemporalTruncField::Day,
			Box::new(Expr::AtTimeZone(
				Box::new(Expr::column("event_time")),
				"America/New_York".to_string(),
			)),
		),
		"America/Chicago",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-01T12:34:56Z"), 0),
	)
	.expect("evaluate explicit-zone date_trunc");
	let truncated = outputs[0]
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
		.expect("truncated timestamp");

	assert_eq!(truncated.value(0), timestamp_ms("2024-01-01T05:00:00Z"));
}

#[test]
fn date_trunc_day_across_dst_reuses_adr150_gap_fold_policy() {
	let program = bind_temporal_expr(
		Expr::DateTrunc(
			TemporalTruncField::Day,
			Box::new(Expr::column("event_time")),
		),
		"America/Chicago",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-03-10T12:34:56Z"), 0),
	)
	.expect("evaluate DST date_trunc");
	let truncated = outputs[0]
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
		.expect("truncated timestamp");

	assert_eq!(truncated.value(0), timestamp_ms("2024-03-10T06:00:00Z"));
	assert_ne!(truncated.value(0), timestamp_ms("2024-03-10T00:00:00Z"));
}

#[test]
fn date32_extract_is_civil_and_zone_free() {
	let days = parse_date_ymd("2024-02-29").expect("date");
	let program = bind_temporal_expr(
		Expr::Extract(
			TemporalExtractField::DayOfYear,
			Box::new(Expr::column("signup_date")),
		),
		"America/Chicago",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-01T00:00:00Z"), days),
	)
	.expect("evaluate Date32 extract");
	let doy = outputs[0]
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("doy output");

	assert_eq!(doy.value(0), 60);
	assert_eq!(program.node_type(program.roots[0]), Some(&DataType::Int32));
}

#[test]
fn at_time_zone_rejects_date32_operand() {
	let schema = temporal_schema();
	let config = PlannerConfig::default().with_session_timezone("UTC");
	let logical_casts = HashMap::new();
	let logical_type_registry = LogicalTypeRegistry::default();
	let ctx = ExprBindContext {
		config: &config,
		session_timezone: config
			.resolved_session_timezone()
			.expect("session timezone"),
		logical_type_specs: &logical_casts,
		logical_type_registry: &logical_type_registry,
		relation_scope: None,
	};
	let err = bind_expr_program(
		&[Expr::Extract(
			TemporalExtractField::Day,
			Box::new(Expr::AtTimeZone(
				Box::new(Expr::column("signup_date")),
				"UTC".to_string(),
			)),
		)],
		&schema,
		&ctx,
	)
	.expect_err("Date32 AT TIME ZONE should reject");

	assert!(err.to_string().contains("AT TIME ZONE requires Timestamp"));
}

#[tokio::test]
async fn rune_date_trunc_day_groups_event_time_in_session_zone() {
	let logical = rune_plan(
		r#"
            pub fn main(ctx) {
                ctx.table("customers")
                   .project([date_trunc("day", col("observed_at"))])
            }
        "#,
	)
	.await;
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(10),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(
				"observed_at",
				crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond),
				true,
			),
		]),
		Vec::new(),
	);
	let plan = bind(
		&logical,
		&catalog,
		&PlannerConfig::default().with_session_timezone("America/Chicago"),
	)
	.await
	.expect("bind Rune temporal plan");
	let ExecutionPlan::Project { exprs, .. } = plan else {
		panic!("expected project");
	};
	let root = exprs.single_root().expect("single root");

	match exprs.node(root) {
		Some(BoundExprNode::DateTrunc { field, zone, .. }) => {
			assert_eq!(*field, TemporalTruncField::Day);
			assert_eq!(zone, "America/Chicago");
		}
		other => panic!("expected DateTrunc node, got {other:?}"),
	}
}

#[test]
fn timestamp_plus_interval_preserves_utc_instant_type() {
	let program = bind_temporal_expr(
		Expr::Add(
			Box::new(Expr::column("event_time")),
			Box::new(interval_literal(DAY_NANOS)),
		),
		"UTC",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-01T00:00:00Z"), 0),
	)
	.expect("evaluate timestamp + interval");
	let values = outputs[0]
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
		.expect("timestamp output");

	assert_eq!(values.value(0), timestamp_ms("2024-01-02T00:00:00Z"));
	assert_eq!(
		values.data_type(),
		&DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
	);
}

#[test]
fn timestamp_minus_interval_preserves_utc_instant_type() {
	let program = bind_temporal_expr(
		Expr::Sub(
			Box::new(Expr::column("event_time")),
			Box::new(interval_literal(DAY_NANOS)),
		),
		"UTC",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-02T00:00:00Z"), 0),
	)
	.expect("evaluate timestamp - interval");
	let values = outputs[0]
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
		.expect("timestamp output");

	assert_eq!(values.value(0), timestamp_ms("2024-01-01T00:00:00Z"));
	assert_eq!(
		values.data_type(),
		&DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
	);
}

#[test]
fn timestamp_minus_timestamp_returns_interval_in_shared_unit() {
	let program = bind_temporal_expr(
		Expr::Sub(
			Box::new(Expr::column("event_time")),
			Box::new(Expr::Literal(ScalarValue::Timestamp {
				value: Some(timestamp_ms("2024-01-01T00:00:00Z")),
				unit: TimeUnit::Millisecond,
			})),
		),
		"UTC",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-02T00:00:00Z"), 0),
	)
	.expect("evaluate timestamp - timestamp");
	let values = outputs[0]
		.as_any()
		.downcast_ref::<DurationNanosecondArray>()
		.expect("duration output");

	assert_eq!(values.value(0), DAY_NANOS);
	assert_eq!(
		program.node_type(program.roots[0]),
		Some(&DataType::Duration(TimeUnit::Nanosecond))
	);
}

#[test]
fn mixed_unit_timestamp_difference_rejects_in_v1() {
	let err = try_bind_temporal_expr(
		Expr::Sub(
			Box::new(Expr::column("event_time")),
			Box::new(Expr::Literal(ScalarValue::Timestamp {
				value: Some(timestamp_ns("2024-01-01T00:00:00Z")),
				unit: TimeUnit::Nanosecond,
			})),
		),
		"UTC",
	)
	.expect_err("mixed timestamp units should reject");

	assert!(
		err.to_string()
			.contains("mixed-unit Timestamp - Timestamp is deferred")
	);
}

#[test]
fn interval_plus_interval_returns_interval() {
	let program = bind_temporal_expr(
		Expr::Add(
			Box::new(interval_literal(1_000_000_000)),
			Box::new(interval_literal(2_000_000_000)),
		),
		"UTC",
	);
	let root = program.single_root().expect("single root");
	let Some(BoundExprNode::Literal(slot)) = program.node(root) else {
		panic!("expected folded interval literal");
	};

	assert_eq!(
		program.literals[*slot as usize],
		ScalarValue::Interval(Some(3_000_000_000))
	);
	assert_eq!(
		program.node_type(root),
		Some(&DataType::Duration(TimeUnit::Nanosecond))
	);
}

#[test]
fn interval_comparison_orders_fixed_durations() {
	let program = bind_temporal_expr(
		Expr::Gt(
			Box::new(interval_literal(2 * DAY_NANOS)),
			Box::new(interval_literal(DAY_NANOS)),
		),
		"UTC",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-01T00:00:00Z"), 0),
	)
	.expect("evaluate interval comparison");
	let values = outputs[0]
		.as_any()
		.downcast_ref::<BooleanArray>()
		.expect("boolean output");

	assert!(values.value(0));
}

#[test]
fn timestamp_plus_timestamp_rejects() {
	let err = try_bind_temporal_expr(
		Expr::Add(
			Box::new(Expr::column("event_time")),
			Box::new(Expr::Literal(ScalarValue::Timestamp {
				value: Some(timestamp_ms("2024-01-01T00:00:00Z")),
				unit: TimeUnit::Millisecond,
			})),
		),
		"UTC",
	)
	.expect_err("timestamp + timestamp should reject");

	assert!(err.to_string().contains("Timestamp + Timestamp"));
}

#[test]
fn interval_day_is_fixed_86400s_not_civil() {
	let plan = SqlPlanner::new()
		.plan("SELECT INTERVAL '1 day' FROM sales")
		.expect("plan interval day");
	let LogicalPlan::Project { exprs, .. } = plan else {
		panic!("expected project");
	};

	assert!(matches!(
		&exprs[0],
		Expr::Literal(ScalarValue::Interval(Some(value))) if *value == DAY_NANOS
	));
}

#[test]
fn non_representable_interval_precision_rejects() {
	let err = try_bind_temporal_expr(
		Expr::Add(
			Box::new(Expr::Literal(ScalarValue::Timestamp {
				value: Some(timestamp_ms("2024-01-01T00:00:00Z")),
				unit: TimeUnit::Millisecond,
			})),
			Box::new(interval_literal(1_000)),
		),
		"UTC",
	)
	.expect_err("literal microsecond interval should not fold against millisecond timestamp");

	assert!(
		err.to_string()
			.contains("not exactly representable at millisecond precision")
	);
}

#[test]
fn non_representable_interval_precision_runtime_rejects() {
	let program = bind_temporal_expr(
		Expr::Add(
			Box::new(Expr::column("event_time")),
			Box::new(interval_literal(1_000)),
		),
		"UTC",
	);
	let err = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-01T00:00:00Z"), 0),
	)
	.expect_err("runtime microsecond interval should reject against millisecond timestamp");

	assert!(
		err.to_string()
			.contains("not exactly representable at millisecond precision")
	);
}

#[test]
fn calendar_month_year_interval_literals_are_year_month() {
	for (sql, expected) in [
		("SELECT INTERVAL '1 month' FROM sales", 1),
		("SELECT INTERVAL '12 months' FROM sales", 12),
		("SELECT INTERVAL '1 year' FROM sales", 12),
		("SELECT INTERVAL '2 years' FROM sales", 24),
		("SELECT INTERVAL '1' MONTH FROM sales", 1),
		("SELECT INTERVAL '1' YEAR FROM sales", 12),
	] {
		let plan = SqlPlanner::new().plan(sql).expect("plan calendar interval");
		let LogicalPlan::Project { exprs, .. } = plan else {
			panic!("expected project");
		};
		assert!(matches!(
			&exprs[0],
			Expr::Literal(ScalarValue::CalendarMonthInterval(Some(value))) if *value == expected
		));
	}
}

#[test]
fn timestamp_plus_calendar_interval_uses_session_zone_and_eom_clamp() {
	let program = bind_temporal_expr(
		Expr::Add(
			Box::new(Expr::column("event_time")),
			Box::new(calendar_month_literal(1)),
		),
		"America/Chicago",
	);
	let root = program.single_root().expect("single root");
	let Some(BoundExprNode::Binary { left, right, .. }) = program.node(root) else {
		panic!("expected calendar arithmetic binary");
	};
	match program.node(*left) {
		Some(BoundExprNode::AtTimeZone { zone, .. }) => assert_eq!(zone, "America/Chicago"),
		other => panic!("expected session-zone timestamp operand, got {other:?}"),
	}
	assert_eq!(
		program.node_type(*right),
		Some(&DataType::Interval(IntervalUnit::YearMonth))
	);

	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-31T06:00:00Z"), 0),
	)
	.expect("evaluate timestamp + calendar interval");
	let values = outputs[0]
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
		.expect("timestamp output");

	assert_eq!(values.value(0), timestamp_ms("2024-02-29T06:00:00Z"));
	assert_eq!(
		values.data_type(),
		&DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
	);
}

#[test]
fn timestamp_calendar_literal_arithmetic_folds_in_bind() {
	let program = bind_temporal_expr(
		Expr::Sub(
			Box::new(Expr::Literal(ScalarValue::Timestamp {
				value: Some(timestamp_ms("2026-01-31T00:00:00Z")),
				unit: TimeUnit::Millisecond,
			})),
			Box::new(calendar_month_literal(1)),
		),
		"UTC",
	);
	let root = program.single_root().expect("single root");
	let Some(BoundExprNode::Literal(slot)) = program.node(root) else {
		panic!("expected folded timestamp literal");
	};

	assert_eq!(
		program.literals[*slot as usize],
		ScalarValue::Timestamp {
			value: Some(timestamp_ms("2025-12-31T00:00:00Z")),
			unit: TimeUnit::Millisecond,
		}
	);
}

#[test]
fn date32_calendar_arithmetic_clamps_end_of_month() {
	let program = bind_temporal_expr(
		Expr::Sub(
			Box::new(Expr::column("signup_date")),
			Box::new(calendar_month_literal(12)),
		),
		"UTC",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(
			timestamp_ms("2024-01-01T00:00:00Z"),
			parse_date_ymd("2024-02-29").expect("date"),
		),
	)
	.expect("evaluate Date32 calendar arithmetic");
	let values = outputs[0]
		.as_any()
		.downcast_ref::<Date32Array>()
		.expect("Date32 output");

	assert_eq!(values.value(0), parse_date_ymd("2023-02-28").expect("date"));
	assert_eq!(program.node_type(program.roots[0]), Some(&DataType::Date32));
}

#[test]
fn calendar_interval_plus_interval_returns_year_month() {
	let program = bind_temporal_expr(
		Expr::Add(
			Box::new(calendar_month_literal(1)),
			Box::new(calendar_month_literal(12)),
		),
		"UTC",
	);
	let root = program.single_root().expect("single root");
	let Some(BoundExprNode::Literal(slot)) = program.node(root) else {
		panic!("expected folded calendar interval literal");
	};

	assert_eq!(
		program.literals[*slot as usize],
		ScalarValue::CalendarMonthInterval(Some(13))
	);
	assert_eq!(
		program.node_type(root),
		Some(&DataType::Interval(IntervalUnit::YearMonth))
	);
}

#[test]
fn calendar_interval_runtime_array_arithmetic_uses_year_month() {
	let program = bind_temporal_expr(
		Expr::Add(
			Box::new(Expr::column("event_time")),
			Box::new(calendar_month_literal(1)),
		),
		"UTC",
	);
	let outputs = Executor::evaluate_bound_program_without_storage(
		&program,
		&temporal_batch(timestamp_ms("2024-01-31T00:00:00Z"), 0),
	)
	.expect("evaluate calendar interval array arithmetic");
	let values = outputs[0]
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
		.expect("timestamp output");

	assert_eq!(values.value(0), timestamp_ms("2024-02-29T00:00:00Z"));
}

#[test]
fn calendar_interval_comparison_rejects() {
	let err = try_bind_temporal_expr(
		Expr::Gt(
			Box::new(calendar_month_literal(2)),
			Box::new(calendar_month_literal(1)),
		),
		"UTC",
	)
	.expect_err("calendar interval comparison should reject");

	assert!(
		err.to_string()
			.contains("CalendarMonthInterval comparison is deferred")
	);
}

#[test]
fn calendar_fixed_interval_mix_rejects() {
	let err = try_bind_temporal_expr(
		Expr::Add(
			Box::new(calendar_month_literal(1)),
			Box::new(interval_literal(DAY_NANOS)),
		),
		"UTC",
	)
	.expect_err("calendar/fixed interval mix should reject");

	assert!(
		err.to_string()
			.contains("fixed-duration Interval arithmetic is not supported")
	);
}

#[test]
fn date32_plus_interval_rejects_in_v1() {
	let err = try_bind_temporal_expr(
		Expr::Add(
			Box::new(Expr::column("signup_date")),
			Box::new(interval_literal(DAY_NANOS)),
		),
		"UTC",
	)
	.expect_err("Date32 + interval should reject");

	assert!(err.to_string().contains("Date32 +/- Interval is deferred"));
}

#[tokio::test]
async fn rune_interval_arithmetic_matches_sql() {
	let sql = SqlPlanner::new()
		.plan("SELECT observed_at - INTERVAL '7 days' FROM customers")
		.expect("plan SQL interval arithmetic");
	let rune = rune_plan(
		r#"
            pub fn main(ctx) {
                ctx.table("customers").project([col("observed_at").sub(days(7))])
            }
        "#,
	)
	.await;
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(10),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(
				"observed_at",
				crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond),
				true,
			),
		]),
		Vec::new(),
	);

	for logical in [sql, rune] {
		let ExecutionPlan::Project { exprs, .. } =
			bind(&logical, &catalog, &PlannerConfig::default())
				.await
				.expect("bind interval project")
		else {
			panic!("expected project");
		};
		let root = exprs.single_root().expect("single root");
		assert_eq!(
			exprs.node_type(root),
			Some(&DataType::Timestamp(
				TimeUnit::Millisecond,
				Some("UTC".into())
			))
		);
		let Some(BoundExprNode::Binary { op, right, .. }) = exprs.node(root) else {
			panic!("expected timestamp - interval binary");
		};
		assert_eq!(*op, BoundBinaryOp::Sub);
		assert_eq!(
			exprs.node_type(*right),
			Some(&DataType::Duration(TimeUnit::Nanosecond))
		);
	}
}

#[tokio::test]
async fn rune_calendar_interval_arithmetic_matches_sql() {
	let sql = SqlPlanner::new()
		.plan("SELECT observed_at + INTERVAL '1 month' FROM customers")
		.expect("plan SQL calendar interval arithmetic");
	let rune = rune_plan(
		r#"
            pub fn main(ctx) {
                ctx.table("customers").project([col("observed_at").add(months(1))])
            }
        "#,
	)
	.await;
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(10),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(
				"observed_at",
				crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond),
				true,
			),
		]),
		Vec::new(),
	);

	for logical in [sql, rune] {
		let ExecutionPlan::Project { exprs, .. } = bind(
			&logical,
			&catalog,
			&PlannerConfig::default().with_session_timezone("America/Chicago"),
		)
		.await
		.expect("bind calendar interval project") else {
			panic!("expected project");
		};
		let root = exprs.single_root().expect("single root");
		assert_eq!(
			exprs.node_type(root),
			Some(&DataType::Timestamp(
				TimeUnit::Millisecond,
				Some("UTC".into())
			))
		);
		let Some(BoundExprNode::Binary { left, right, .. }) = exprs.node(root) else {
			panic!("expected timestamp + calendar interval binary");
		};
		assert_eq!(
			exprs.node_type(*right),
			Some(&DataType::Interval(IntervalUnit::YearMonth))
		);
		match exprs.node(*left) {
			Some(BoundExprNode::AtTimeZone { zone, .. }) => {
				assert_eq!(zone, "America/Chicago")
			}
			other => panic!("expected session-zone timestamp operand, got {other:?}"),
		}
	}
}

#[tokio::test]
async fn relative_interval_filter_spends_timestamp_range_fact() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(
				"observed_at",
				crate::timestamp::utc_timestamp_data_type(TimeUnit::Nanosecond),
				true,
			),
		]),
		Vec::new(),
		vec![(
			ColumnOrdinal(1),
			range_fact_timestamp_ns(
				timestamp_ns("2025-12-01T00:00:00Z"),
				timestamp_ns("2026-02-01T00:00:00Z"),
				20_000,
			),
		)],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.observed_at > TIMESTAMPTZ '2026-01-01T00:00:00Z' - INTERVAL '7 days'",
		)
		.expect("plan relative interval filter");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind relative interval range-estimated join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn relative_calendar_month_filter_spends_timestamp_range_fact() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(
				"observed_at",
				crate::timestamp::utc_timestamp_data_type(TimeUnit::Nanosecond),
				true,
			),
		]),
		Vec::new(),
		vec![(
			ColumnOrdinal(1),
			range_fact_timestamp_ns(
				timestamp_ns("2025-12-01T00:00:00Z"),
				timestamp_ns("2026-02-01T00:00:00Z"),
				20_000,
			),
		)],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 WHERE c.observed_at > TIMESTAMPTZ '2026-01-31T00:00:00Z' - INTERVAL '1 month'",
		)
		.expect("plan relative calendar month filter");

	let (plan, trace) = bind_with_trace(
		&logical,
		&catalog,
		&PlannerConfig::default().with_session_timezone("UTC"),
	)
	.await
	.expect("bind relative calendar month range-estimated join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
	assert!(matches!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Right,
			..
		}
	));
}

#[tokio::test]
async fn timestamp_precision_loss_in_target_unit_is_bind_error() {
	let instant = timestamp_ms("2024-01-01T00:00:00Z");
	let catalog = descriptor_sidecar_join_catalog_with_customer_timestamp_range(
		range_fact_timestamp_ms(instant, instant, 10),
	);
	let err = match bind_filtered_customers_with_config(
		&catalog,
		Expr::Gte(
			Box::new(customer_column("observed_at")),
			Box::new(utf8_literal("2024-01-01 00:00:00.000001")),
		),
		PlannerConfig::default().with_session_timezone("UTC"),
	)
	.await
	{
		Ok(_) => panic!("precision-losing timestamp should reject"),
		Err(error) => error,
	};

	assert!(
		err.to_string()
			.contains("not exactly representable at millisecond precision")
	);
}

#[tokio::test]
async fn timestamp_in_list_uses_existing_ndv_fact_and_writes_no_hard_bounds() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(20_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(
				"observed_at",
				crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond),
				true,
			),
		]),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	let scoped = bind_filtered_customers_with_config(
		&catalog,
		customer_in_list(
			"observed_at",
			vec![
				utf8_literal("2024-01-01 00:00:00"),
				utf8_literal("2024-01-02 00:00:00"),
				utf8_literal("2024-01-01 00:00:00"),
			],
			false,
		),
		PlannerConfig::default().with_session_timezone("UTC"),
	)
	.await
	.expect("bind timestamp IN-list");

	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(4)));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[test]
fn timestamp_range_fact_requires_timestamp_column_and_matching_unit() {
	let range = range_fact_timestamp_ms(10, 20, 100);
	let timestamp_type = crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond);
	let naive_timestamp_type = DataType::Timestamp(TimeUnit::Millisecond, None);
	let non_utc_timestamp_type =
		DataType::Timestamp(TimeUnit::Millisecond, Some("America/Chicago".into()));

	assert_eq!(
		range_scalar_for_fact(
			&range,
			&timestamp_type,
			&ScalarValue::Timestamp {
				value: Some(12),
				unit: TimeUnit::Millisecond
			}
		),
		Some(RangeScalar::Integral(12))
	);
	assert_eq!(
		range_scalar_for_fact(&range, &DataType::Int64, &ScalarValue::Int64(Some(12))),
		None
	);
	assert_eq!(
		range_scalar_for_fact(
			&range,
			&timestamp_type,
			&ScalarValue::Timestamp {
				value: Some(12),
				unit: TimeUnit::Microsecond
			}
		),
		None
	);
	assert_eq!(
		range_scalar_for_fact(
			&range,
			&naive_timestamp_type,
			&ScalarValue::Timestamp {
				value: Some(12),
				unit: TimeUnit::Millisecond
			}
		),
		None
	);
	assert_eq!(
		range_scalar_for_fact(
			&range,
			&non_utc_timestamp_type,
			&ScalarValue::Timestamp {
				value: Some(12),
				unit: TimeUnit::Millisecond
			}
		),
		None
	);
}

#[test]
fn date32_stays_civil_not_timestamp_day_boundary_and_date64_fact_remains_dark() {
	let date32_range = range_fact_date32(19_723, 19_724, 2);
	let date64_range = RangeFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_DATE64,
		null_count: 0,
		non_null_count: 2,
		min: Some(wendelon_client::encode_signed_optimizer_fact_scalar(0, 8).unwrap()),
		max: Some(wendelon_client::encode_signed_optimizer_fact_scalar(86_400_000, 8).unwrap()),
	};
	let timestamp = ScalarValue::Timestamp {
		value: Some(timestamp_ms("2024-01-01T00:00:00Z")),
		unit: TimeUnit::Millisecond,
	};

	assert_eq!(
		range_scalar_for_fact(&date32_range, &DataType::Date32, &timestamp),
		None
	);
	assert_eq!(
		range_scalar_for_fact(&date64_range, &DataType::Date64, &timestamp),
		None
	);
}

#[tokio::test]
async fn range_outside_envelope_estimates_zero_without_hard_bound() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(200)))),
		),
	)
	.await
	.expect("bind outside-envelope range filter");

	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn float_range_outside_envelope_estimates_zero_without_hard_bound() {
	let catalog =
		descriptor_sidecar_join_catalog_with_customer_score_range(range_fact_f64(0.0, 1.0, 1_000));
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("score")),
			Box::new(Expr::Literal(ScalarValue::Float64(Some(2.0)))),
		),
	)
	.await
	.expect("bind outside-envelope float range filter");

	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn float_range_does_not_write_hard_bounds_or_empty_relation() {
	let catalog =
		descriptor_sidecar_join_catalog_with_customer_score_range(range_fact_f64(0.0, 1.0, 1_000));
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Lt(
			Box::new(customer_column("score")),
			Box::new(Expr::Literal(ScalarValue::Float64(Some(-1.0)))),
		),
	)
	.await
	.expect("bind disjoint float range filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[test]
fn int_literal_against_float_fact_consumed_as_f64() {
	let range = range_fact_f64(0.0, 10.0, 100);

	assert_eq!(
		range_scalar_for_fact(&range, &DataType::Float64, &ScalarValue::Int32(Some(7))),
		Some(RangeScalar::Real(7.0))
	);
	assert_eq!(
		range_scalar_for_fact(&range, &DataType::Float64, &ScalarValue::Int64(Some(7))),
		Some(RangeScalar::Real(7.0))
	);
	assert_eq!(
		range_scalar_for_fact(&range, &DataType::Float64, &ScalarValue::Float32(Some(2.5))),
		Some(RangeScalar::Real(2.5))
	);
	assert_eq!(
		range_scalar_for_fact(&range, &DataType::Float64, &ScalarValue::Float64(Some(2.5))),
		Some(RangeScalar::Real(2.5))
	);
	assert_eq!(
		range_scalar_for_fact(
			&range,
			&DataType::Float64,
			&ScalarValue::Int64(Some(i64::MAX))
		),
		None
	);
	assert_eq!(
		range_scalar_for_fact(
			&range,
			&DataType::Float64,
			&ScalarValue::Float64(Some(f64::NAN))
		),
		None
	);
	assert_eq!(
		range_scalar_for_fact(&range, &DataType::Float64, &ScalarValue::Float64(None)),
		None
	);
}

#[tokio::test]
async fn integer_range_facts_keep_discrete_interpolation() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		None,
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(89)))),
		),
	)
	.await
	.expect("bind integer discrete range filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
}

#[tokio::test]
async fn date32_range_facts_keep_discrete_interpolation() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_date_range(range_fact_date32(
		19_723, 19_822, 1_000,
	));
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("signup_date")),
			Box::new(date32_literal(19_812)),
		),
	)
	.await
	.expect("bind Date32 discrete range filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
}

#[tokio::test]
async fn utf8_range_facts_still_fail_open() {
	let utf8_catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("label", DataType::Utf8, true),
		]),
		Vec::new(),
		vec![(
			ColumnOrdinal(1),
			RangeFact {
				arrow_type_tag: wendelon_client::ARROW_STATS_TAG_UTF8,
				null_count: 0,
				non_null_count: 100,
				min: Some(b"a".to_vec()),
				max: Some(b"z".to_vec()),
			},
		)],
	);
	let utf8_scoped = bind_filtered_customers(
		&utf8_catalog,
		Expr::Gt(
			Box::new(customer_column("label")),
			Box::new(Expr::Literal(ScalarValue::Utf8(Some("m".to_string())))),
		),
	)
	.await
	.expect("bind UTF8 range filter");

	assert_eq!(utf8_scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn opaque_float_range_facts_still_fail_open() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("score", DataType::Float64, true),
		]),
		Vec::new(),
		vec![(
			ColumnOrdinal(1),
			RangeFact {
				arrow_type_tag: wendelon_client::ARROW_STATS_TAG_OPAQUE_UNSUPPORTED,
				null_count: 0,
				non_null_count: 100,
				min: None,
				max: None,
			},
		)],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::Gt(
			Box::new(customer_column("score")),
			Box::new(Expr::Literal(ScalarValue::Float64(Some(0.5)))),
		),
	)
	.await
	.expect("bind opaque float range filter");

	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn range_and_equality_take_single_tightest_estimate() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		Some(1_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int64, true),
			Field::new("region", DataType::Utf8, true),
		]),
		vec![(ColumnOrdinal(2), DistinctCountFact::Exact(10))],
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let scoped = bind_filtered_customers(
		&catalog,
		Expr::And(
			Box::new(customer_amount_between(10, 19)),
			Box::new(Expr::Eq(
				Box::new(customer_column("region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			)),
		),
	)
	.await
	.expect("bind range and equality filter");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
	assert_ne!(scoped.facts.estimated_row_count, Some(RowCount::new(10)));
}

#[tokio::test]
async fn and_filter_uses_single_equality_not_independence_product() {
	let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		Some(64 * 1024 * 1024),
		None,
		Some(10_000),
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
			Field::new("segment", DataType::Utf8, true),
		]),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
	);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::And(
			Box::new(Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			)),
			Box::new(Expr::Eq(
				Box::new(Expr::qualified_column(
					TableRef::new("customers"),
					"segment",
				)),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some(
					"enterprise".to_string(),
				)))),
			)),
		),
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered_customers)
		.await
		.expect("bind AND-filtered dimension");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
	assert_ne!(scoped.facts.estimated_row_count, Some(RowCount::new(1)));
}

#[test]
fn estimated_size_over_counts_encoded_payload() {
	for rows in [1_usize, 8, 1_000] {
		for batch in representative_estimate_batches(rows) {
			let estimate = estimate_materialized_byte_size(
				RowCount::new(u64::try_from(rows).expect("test rows fit u64")),
				batch.schema().as_ref(),
			)
			.expect("representative schema is estimate-supported");
			let encoded = encode_record_batch(&batch).expect("representative batch encodes to IPC");
			assert!(
				estimate.get() >= u64::try_from(encoded.len()).expect("encoded length fits u64"),
				"estimate {estimate:?} under-counted encoded IPC bytes {} for schema {:?} rows {rows}",
				encoded.len(),
				batch.schema()
			);
		}
	}
}

#[tokio::test]
async fn join_output_containment_estimate_matches_formula() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan containment-estimated join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind containment-estimated join");

	let expected_rows = RowCount::new(50_000);
	let expected_bytes = estimate_materialized_byte_size(expected_rows, scoped.schema().as_ref())
		.expect("join schema is byte-estimable");
	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.estimated_row_count, Some(expected_rows));
	assert_eq!(scoped.facts.estimated_byte_size, Some(expected_bytes));
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.row_upper_bound, None);
}

#[tokio::test]
async fn join_output_containment_zero_rows_estimates_zero_without_empty_relation() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(0),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan zero-row containment join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind zero-row containment join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn join_output_containment_positive_inputs_floor_to_one() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(1),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(1),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(10_000))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan positive-floor containment join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind positive-floor containment join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(1)));
}

#[tokio::test]
async fn join_output_containment_fails_open_without_ndv() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(&mut catalog, "customers", None, Some(5_000), Vec::new());
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan missing-NDV containment join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind missing-NDV containment join");

	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.estimated_row_count, None);
	assert_eq!(scoped.facts.estimated_byte_size, None);
}

#[tokio::test]
async fn join_output_containment_rejects_zero_ndv() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(0))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan zero-NDV containment join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind zero-NDV containment join");

	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn join_output_containment_skips_filtered_input_ndv() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(500)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(50)),
		],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o INNER JOIN customers c \
				 ON o.customer_id = c.id WHERE c.region = 'north'",
		)
		.expect("plan filtered-input containment join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind filtered-input containment join");

	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[test]
fn join_output_containment_maps_scan_projection_ndv() {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int64, true),
		Field::new("customer_id", DataType::Int64, false),
	]));
	let descriptor = named_descriptor(43, "projected_orders", schema.as_ref().clone());
	let scan = ExecutionPlan::Scan {
		local_table_id: TableId(43),
		table: descriptor.table_object_id,
		fingerprint: descriptor.physical_descriptor_fingerprint,
		schema,
		projection: Some(vec![ColumnOrdinal(2)]),
		pbc_program: None,
		row_limit: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: Vec::new(),
		column_ndv: vec![(ColumnOrdinal(2), DistinctCountFact::Exact(777))],
		column_range: Vec::new(),
		column_range_distributions: Vec::new(),
		column_heavy_hitters: Vec::new(),
		column_group_top_k: Default::default(),
		column_group_ndv: Default::default(),
	};

	assert_eq!(
		clean_base_column_ndv_for_join_input(&scan, ColumnOrdinal(0)),
		Some(DistinctCountFact::Exact(777))
	);
}

#[test]
fn clean_base_join_range_maps_scan_projection() {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int64, true),
		Field::new("customer_id", DataType::Int64, false),
	]));
	let descriptor = named_descriptor(43, "projected_orders", schema.as_ref().clone());
	let range = range_fact_i64(10, 20, 11);
	let scan = ExecutionPlan::Scan {
		local_table_id: TableId(43),
		table: descriptor.table_object_id,
		fingerprint: descriptor.physical_descriptor_fingerprint,
		schema,
		projection: Some(vec![ColumnOrdinal(2)]),
		pbc_program: None,
		row_limit: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: Vec::new(),
		column_ndv: Vec::new(),
		column_range: vec![(ColumnOrdinal(2), range.clone())],
		column_range_distributions: Vec::new(),
		column_heavy_hitters: Vec::new(),
		column_group_top_k: Default::default(),
		column_group_ndv: Default::default(),
	};

	assert_eq!(
		clean_base_column_range_for_join_input(&scan, ColumnOrdinal(0)),
		Some(range)
	);
}

#[test]
fn clean_base_join_heavy_hitter_maps_scan_projection() {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int64, true),
		Field::new("customer_id", DataType::Int64, false),
	]));
	let descriptor = named_descriptor(43, "projected_orders", schema.as_ref().clone());
	let fact = int64_heavy_hitter_fact(&[(7, 500)]);
	let scan = ExecutionPlan::Scan {
		local_table_id: TableId(43),
		table: descriptor.table_object_id,
		fingerprint: descriptor.physical_descriptor_fingerprint,
		schema,
		projection: Some(vec![ColumnOrdinal(2)]),
		pbc_program: None,
		row_limit: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: Vec::new(),
		column_ndv: Vec::new(),
		column_range: Vec::new(),
		column_range_distributions: Vec::new(),
		column_heavy_hitters: vec![(ColumnOrdinal(2), fact.clone())],
		column_group_top_k: Default::default(),
		column_group_ndv: Default::default(),
	};

	assert_eq!(
		clean_base_column_heavy_hitter_for_join_input(&scan, ColumnOrdinal(0)),
		Some(&fact)
	);
}

#[tokio::test]
async fn disjoint_join_key_ranges_estimate_empty_not_containment_product() {
	let sql = "SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id";
	let mut ranged_catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges(
		&mut ranged_catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
		vec![(ColumnOrdinal(1), range_fact_i64(1, 5, 10_000))],
	);
	seed_table_optimizer_facts_with_ranges(
		&mut ranged_catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
		vec![(ColumnOrdinal(0), range_fact_i64(100, 200, 5_000))],
	);
	let ranged = Binder::new(&ranged_catalog, &PlannerConfig::default())
		.bind_plan(&SqlPlanner::new().plan(sql).expect("plan ranged join"))
		.await
		.expect("bind ranged join");

	let mut base_catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut base_catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut base_catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
	);
	let base = Binder::new(&base_catalog, &PlannerConfig::default())
		.bind_plan(&SqlPlanner::new().plan(sql).expect("plan base join"))
		.await
		.expect("bind base join");

	assert_eq!(base.facts.estimated_row_count, Some(RowCount::new(50_000)));
	assert_eq!(ranged.facts.estimated_row_count, Some(RowCount::new(0)));
	assert_eq!(ranged.facts.row_upper_bound, None);
	assert_eq!(ranged.facts.byte_upper_bound, None);
	assert!(!matches!(ranged.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn partial_overlap_join_scales_containment_down() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"orders",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"customers",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
		vec![(ColumnOrdinal(0), range_fact_i64(50, 149, 1_000))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan partial-overlap join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind partial-overlap join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(5_000)));
}

#[test]
fn range_overlap_refinement_never_exceeds_containment() {
	let int64 = DataType::Int64;
	let base = RowCount::new(10_000);
	for (left, right) in [
		(range_fact_i64(0, 99, 100), range_fact_i64(50, 149, 100)),
		(range_fact_i64(0, 9, 10), range_fact_i64(0, 99, 100)),
		(range_fact_i64(0, 99, 100), range_fact_i64(200, 299, 100)),
		(range_fact_i64(-50, 50, 101), range_fact_i64(0, 200, 201)),
	] {
		let refined = range_refined_containment_join_rows(base, &left, &int64, &right, &int64)
			.expect("range refinement");
		assert!(refined <= base, "{refined:?} exceeded {base:?}");
	}
}

#[test]
fn contained_range_scores_full_overlap() {
	let int64 = DataType::Int64;
	let base = RowCount::new(10_000);
	let refined = range_refined_containment_join_rows(
		base,
		&range_fact_i64(0, 9, 10),
		&int64,
		&range_fact_i64(0, 99, 100),
		&int64,
	)
	.expect("contained range refinement");

	assert_eq!(refined, base);
}

#[tokio::test]
async fn join_range_overlap_fails_open_without_range_fact() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
		vec![(ColumnOrdinal(1), range_fact_i64(1, 5, 10_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan one-sided range join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind one-sided range join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(50_000))
	);
}

#[tokio::test]
async fn join_range_overlap_fails_open_on_fact_tag_type_mismatch() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
		vec![(ColumnOrdinal(1), range_fact_i32(1, 5, 10_000))],
	);
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
		vec![(ColumnOrdinal(0), range_fact_i64(100, 200, 5_000))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan mismatched range-tag join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind mismatched range-tag join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(50_000))
	);
}

#[test]
fn range_overlap_real_lane_fails_open_in_helper() {
	assert_eq!(
		join_key_range_overlap_fraction(
			&range_fact_f64(0.0, 10.0, 10),
			&DataType::Float64,
			&range_fact_f64(5.0, 15.0, 10),
			&DataType::Float64,
		),
		None
	);
}

#[test]
fn integral_lane_covers_date32_and_timestamp_join_keys() {
	assert_eq!(
		join_key_range_overlap_fraction(
			&range_fact_date32(10, 20, 11),
			&DataType::Date32,
			&range_fact_date32(15, 25, 11),
			&DataType::Date32,
		),
		Some((6, 11))
	);
	let timestamp_ms_type = crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond);
	assert_eq!(
		join_key_range_overlap_fraction(
			&range_fact_timestamp_ms(10, 20, 11),
			&timestamp_ms_type,
			&range_fact_timestamp_ms(15, 25, 11),
			&timestamp_ms_type,
		),
		Some((6, 11))
	);
	assert_eq!(
		join_key_range_overlap_fraction(
			&range_fact_timestamp_ns(10, 20, 11),
			&timestamp_ms_type,
			&range_fact_timestamp_ns(15, 25, 11),
			&timestamp_ms_type,
		),
		None
	);
}

#[tokio::test]
async fn join_range_overlap_skips_filtered_input() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
		vec![(ColumnOrdinal(1), range_fact_i64(1, 5, 10_000))],
	);
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(500)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(50)),
		],
		vec![(ColumnOrdinal(0), range_fact_i64(100, 200, 5_000))],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o INNER JOIN customers c \
				 ON o.customer_id = c.id WHERE c.region = 'north'",
		)
		.expect("plan filtered-input range-overlap join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind filtered-input range-overlap join");

	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn join_range_overlap_is_estimate_channel_not_emptyrelation() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
		vec![(ColumnOrdinal(1), range_fact_i64(1, 5, 10_000))],
	);
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
		vec![(ColumnOrdinal(0), range_fact_i64(100, 200, 5_000))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan disjoint estimate-channel join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind disjoint estimate-channel join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[test]
fn hot_both_contribution_is_exact_product_of_frequencies() {
	let left = int64_heavy_hitter_fact(&[(7, 5), (9, 3)]);
	let right = int64_heavy_hitter_fact(&[(7, 11), (10, 13)]);
	let contribution = hot_both_join_contribution(
		&left,
		RowCount::new(8),
		&DataType::Int64,
		&right,
		RowCount::new(24),
		&DataType::Int64,
	)
	.expect("valid hot-both contribution");

	assert_eq!(contribution.rows, 55);
	assert_eq!(contribution.count, 1);
	assert_eq!(contribution.left_rows, 5);
	assert_eq!(contribution.right_rows, 11);
}

#[tokio::test]
async fn co_hot_join_recovers_skew_and_hashes_without_replay() {
	let sql = "SELECT * FROM orders o \
			INNER JOIN customers c ON o.customer_id = c.id \
			INNER JOIN regions r ON c.region_id = r.id";
	let mut uniform_catalog = join_cardinality_catalog();
	seed_table_optimizer_facts(
		&mut uniform_catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	seed_table_optimizer_facts(
		&mut uniform_catalog,
		"customers",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(10_000))],
	);
	let logical = SqlPlanner::new().plan(sql).expect("plan co-hot join");
	let (_uniform_plan, uniform_trace) = bind_with_trace(
		&logical,
		&uniform_catalog,
		&auto_broadcast_config(512 * 1024),
	)
	.await
	.expect("bind uniform co-hot baseline");
	assert_eq!(uniform_trace.join_decisions.len(), 2);
	assert!(matches!(
		uniform_trace.join_decisions[1].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Left,
			..
		}
	));

	let mut skew_catalog = join_cardinality_catalog();
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut skew_catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
		Vec::new(),
		vec![(ColumnOrdinal(1), int64_heavy_hitter_fact(&[(7, 5_000)]))],
	);
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut skew_catalog,
		"customers",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(10_000))],
		Vec::new(),
		vec![(ColumnOrdinal(0), int64_heavy_hitter_fact(&[(7, 5_000)]))],
	);
	let (skew_plan, skew_trace) =
		bind_with_trace(&logical, &skew_catalog, &auto_broadcast_config(512 * 1024))
			.await
			.expect("bind co-hot join");

	assert_eq!(skew_trace.join_decisions.len(), 2);
	assert_eq!(
		skew_trace.join_decisions[1].strategy,
		JoinStrategy::HashPartitioned
	);
	assert!(
		!matches!(
			skew_trace.join_decisions[1].reason,
			JoinStrategyDecisionReason::BroadcastEstimated { .. }
		),
		"co-hot child estimate should prevent parent estimated broadcast: {:?}",
		skew_trace.join_decisions[1].reason
	);
	assert_join_strategy_count(&skew_plan, JoinStrategy::HashPartitioned, 1);
}

#[tokio::test]
async fn no_shared_hot_keys_falls_back_to_161_containment() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
		Vec::new(),
		vec![(ColumnOrdinal(1), int64_heavy_hitter_fact(&[(7, 5_000)]))],
	);
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"customers",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(10_000))],
		Vec::new(),
		vec![(ColumnOrdinal(0), int64_heavy_hitter_fact(&[(8, 5_000)]))],
	);
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(
			&SqlPlanner::new()
				.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
				.expect("plan no-shared-hot join"),
		)
		.await
		.expect("bind no-shared-hot join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_000))
	);
}

#[tokio::test]
async fn residual_excludes_hot_both_rows_and_keys_no_double_count() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"orders",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
		vec![(ColumnOrdinal(1), int64_heavy_hitter_fact(&[(50, 100)]))],
	);
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"customers",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
		vec![(ColumnOrdinal(0), range_fact_i64(50, 149, 1_000))],
		vec![(ColumnOrdinal(0), int64_heavy_hitter_fact(&[(50, 100)]))],
	);
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(
			&SqlPlanner::new()
				.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
				.expect("plan residual hot-both join"),
		)
		.await
		.expect("bind residual hot-both join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(14_091))
	);
}

#[tokio::test]
async fn single_key_join_unchanged_byte_identical_to_162() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"orders",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
		vec![(ColumnOrdinal(1), int64_heavy_hitter_fact(&[(50, 100)]))],
	);
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"customers",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
		vec![(ColumnOrdinal(0), range_fact_i64(50, 149, 1_000))],
		vec![(ColumnOrdinal(0), int64_heavy_hitter_fact(&[(50, 100)]))],
	);
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(
			&SqlPlanner::new()
				.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
				.expect("plan single-key 162 regression join"),
		)
		.await
		.expect("bind single-key 162 regression join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(14_091))
	);
}

#[tokio::test]
async fn disjoint_range_zero_dominates_stale_shared_hot_key() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"orders",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		vec![(ColumnOrdinal(1), range_fact_i64(0, 10, 1_000))],
		vec![(ColumnOrdinal(1), int64_heavy_hitter_fact(&[(7, 500)]))],
	);
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"customers",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
		vec![(ColumnOrdinal(0), range_fact_i64(100, 200, 1_000))],
		vec![(ColumnOrdinal(0), int64_heavy_hitter_fact(&[(7, 500)]))],
	);
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(
			&SqlPlanner::new()
				.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
				.expect("plan disjoint stale-HH join"),
		)
		.await
		.expect("bind disjoint stale-HH join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
}

#[tokio::test]
async fn join_skew_never_lowers_161_prior() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"orders",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
		Vec::new(),
		vec![(ColumnOrdinal(1), int64_heavy_hitter_fact(&[(7, 1)]))],
	);
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"customers",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
		Vec::new(),
		vec![(ColumnOrdinal(0), int64_heavy_hitter_fact(&[(7, 100)]))],
	);
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(
			&SqlPlanner::new()
				.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
				.expect("plan prior-floor join"),
		)
		.await
		.expect("bind prior-floor join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_000))
	);
}

#[tokio::test]
async fn join_skew_fails_open_on_key_type_mismatch_or_invalid_frequency() {
	for invalid_fact in [
		utf8_heavy_hitter_fact(&[("7", 500)]),
		int64_heavy_hitter_fact(&[(7, 0)]),
		int64_heavy_hitter_fact(&[(7, 1_001)]),
	] {
		let mut catalog = join_catalog();
		seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
			&mut catalog,
			"orders",
			None,
			Some(1_000),
			vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
			Vec::new(),
			vec![(ColumnOrdinal(1), invalid_fact)],
		);
		seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
			&mut catalog,
			"customers",
			None,
			Some(1_000),
			vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
			Vec::new(),
			vec![(ColumnOrdinal(0), int64_heavy_hitter_fact(&[(7, 500)]))],
		);
		let scoped = Binder::new(&catalog, &PlannerConfig::default())
			.bind_plan(
				&SqlPlanner::new()
					.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
					.expect("plan invalid-HH join"),
			)
			.await
			.expect("bind invalid-HH join");

		assert_eq!(
			scoped.facts.estimated_row_count,
			Some(RowCount::new(10_000))
		);
	}
}

#[tokio::test]
async fn join_skew_skips_filtered_input() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
		Vec::new(),
		vec![(ColumnOrdinal(1), int64_heavy_hitter_fact(&[(7, 5_000)]))],
	);
	seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(500)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(50)),
		],
		Vec::new(),
		vec![(ColumnOrdinal(0), int64_heavy_hitter_fact(&[(7, 5_000)]))],
	);
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(
			&SqlPlanner::new()
				.plan(
					"SELECT * FROM orders o INNER JOIN customers c \
					 ON o.customer_id = c.id WHERE c.region = 'north'",
				)
				.expect("plan filtered HH join"),
		)
		.await
		.expect("bind filtered HH join");

	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[tokio::test]
async fn multi_key_denominator_is_product_of_per_key_max_ndv() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"order_items",
		None,
		Some(10_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"line_prices",
		None,
		Some(5_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(500)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(5)),
		],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind multi-key product denominator join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(5_000)));
	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.byte_upper_bound, None);
}

#[tokio::test]
async fn multi_key_group_ndv_caps_product_denominator() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_ndv(
		&mut catalog,
		"order_items",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
		],
		vec![(
			vec![ColumnOrdinal(2), ColumnOrdinal(1)],
			DistinctCountFact::Exact(200),
		)],
	);
	seed_table_optimizer_facts_with_column_group_ndv(
		&mut catalog,
		"line_prices",
		5_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(500)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(5)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind multi-key group NDV join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(250_000))
	);
	assert_eq!(scoped.facts.row_upper_bound, None);
}

#[tokio::test]
async fn multi_key_group_ndv_requires_both_sides() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_ndv(
		&mut catalog,
		"order_items",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(200),
		)],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"line_prices",
		None,
		Some(5_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(500)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(5)),
		],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind one-sided group NDV join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(5_000)));
}

#[tokio::test]
async fn multi_key_group_ndv_never_lowers_163_prior() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_column_group_ndv(
			&mut catalog,
			table,
			match table {
				"order_items" => 10_000,
				"line_prices" => 5_000,
				_ => unreachable!(),
			},
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
			],
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				DistinctCountFact::Exact(20_000),
			)],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind oversized group NDV join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(5_000)));
}

#[tokio::test]
async fn multi_key_fails_open_when_any_key_lacks_ndv() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"order_items",
		None,
		Some(10_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"line_prices",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(500))],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind missing multi-key NDV join");

	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.estimated_row_count, None);
	assert_eq!(scoped.facts.estimated_byte_size, None);
}

#[tokio::test]
async fn multi_key_range_overlap_folds_per_key() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_ranges(
			&mut catalog,
			table,
			None,
			Some(10_000),
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			],
			match table {
				"order_items" => vec![
					(ColumnOrdinal(1), range_fact_i64(0, 99, 10_000)),
					(ColumnOrdinal(2), range_fact_i64(0, 99, 10_000)),
				],
				"line_prices" => vec![
					(ColumnOrdinal(1), range_fact_i64(50, 149, 10_000)),
					(ColumnOrdinal(2), range_fact_i64(90, 189, 10_000)),
				],
				_ => unreachable!(),
			},
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind multi-key range fold join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(500)));
}

#[tokio::test]
async fn multi_key_range_malformed_key_keeps_multi_key_ndv_estimate() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"order_items",
		None,
		Some(10_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![
			(ColumnOrdinal(1), range_fact_i32(0, 99, 10_000)),
			(ColumnOrdinal(2), range_fact_i64(0, 99, 10_000)),
		],
	);
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"line_prices",
		None,
		Some(10_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![
			(ColumnOrdinal(1), range_fact_i64(50, 149, 10_000)),
			(ColumnOrdinal(2), range_fact_i64(50, 149, 10_000)),
		],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind multi-key malformed range join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(5_000)));
}

#[tokio::test]
async fn multi_key_disjoint_on_any_key_estimates_empty() {
	let scoped = bind_multi_key_disjoint_join().await;

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
}

#[tokio::test]
async fn multi_key_is_estimate_channel_not_emptyrelation() {
	let scoped = bind_multi_key_disjoint_join().await;

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
	assert_eq!(scoped.facts.row_upper_bound, None);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn multi_key_three_key_join_does_not_spuriously_zero() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts(
			&mut catalog,
			table,
			None,
			Some(1),
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(u64::MAX)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(u64::MAX)),
				(ColumnOrdinal(3), DistinctCountFact::Exact(u64::MAX)),
			],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&[
			"order_id", "line_no", "shard_id",
		]))
		.await
		.expect("bind three-key no-spurious-zero join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(1)));
}

#[tokio::test]
async fn multi_key_three_key_disjoint_estimates_zero() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_ranges(
			&mut catalog,
			table,
			None,
			Some(1),
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(u64::MAX)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(u64::MAX)),
				(ColumnOrdinal(3), DistinctCountFact::Exact(u64::MAX)),
			],
			match table {
				"order_items" => vec![(ColumnOrdinal(2), range_fact_i64(0, 0, 1))],
				"line_prices" => vec![(ColumnOrdinal(2), range_fact_i64(1, 1, 1))],
				_ => unreachable!(),
			},
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&[
			"order_id", "line_no", "shard_id",
		]))
		.await
		.expect("bind three-key disjoint join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
}

#[tokio::test]
async fn multi_key_does_not_apply_heavy_hitter() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
			&mut catalog,
			table,
			None,
			Some(10_000),
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			],
			Vec::new(),
			vec![(ColumnOrdinal(1), int64_heavy_hitter_fact(&[(7, 5_000)]))],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind multi-key HH-deferred join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_000))
	);
}

#[test]
fn hot_tuple_contribution_is_exact_product_of_frequencies() {
	let left = int64_group_top_k_fact(&[([7, 2], 100), ([8, 2], 3)]);
	let right = int64_group_top_k_fact(&[([7, 2], 50), ([7, 3], 5)]);
	let contribution = hot_tuple_join_contribution(
		&left,
		RowCount::new(103),
		&[DataType::Int64, DataType::Int64],
		&right,
		RowCount::new(55),
		&[DataType::Int64, DataType::Int64],
	)
	.expect("valid hot tuple contribution");

	assert_eq!(contribution.rows, 5_000);
	assert_eq!(contribution.count, 1);
	assert_eq!(contribution.left_rows, 100);
	assert_eq!(contribution.right_rows, 50);
}

#[tokio::test]
async fn absent_column_group_top_k_falls_back_to_163_byte_identical() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts(
			&mut catalog,
			table,
			None,
			Some(10_000),
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind absent group top-k join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_000))
	);
}

#[tokio::test]
async fn absent_column_group_ndv_falls_back_to_164_byte_identical() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_column_group_top_k(
			&mut catalog,
			table,
			10_000,
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(10_000)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(10_000)),
			],
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				int64_group_top_k_fact(&[([7, 2], 5_000)]),
			)],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind absent group NDV join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(25_000_000))
	);
}

#[tokio::test]
async fn consumer_uses_exact_hot_floor_not_fake_residual() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_column_group_top_k(
			&mut catalog,
			table,
			10_000,
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(10_000)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(10_000)),
			],
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				int64_group_top_k_fact(&[([7, 2], 5_000)]),
			)],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind group top-k exact floor join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(25_000_000))
	);
}

#[tokio::test]
async fn multi_key_skew_never_lowers_163_prior() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_top_k(
		&mut catalog,
		"order_items",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 1)]),
		)],
	);
	seed_table_optimizer_facts_with_column_group_top_k(
		&mut catalog,
		"line_prices",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 100)]),
		)],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind group top-k prior floor join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_000))
	);
}

#[tokio::test]
async fn hot_plus_cold_no_double_counts_hot_tuples() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
		&mut catalog,
		"order_items",
		1_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(1_000)),
		],
		Vec::new(),
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 20)]),
		)],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);
	seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
		&mut catalog,
		"line_prices",
		1_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(1_000)),
		],
		Vec::new(),
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 30)]),
		)],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind hot+cold no-double-count join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_203))
	);
}

#[tokio::test]
async fn cold_ndv_uses_effective_composite_not_independence_proxy_or_raw_overcount() {
	let mut catalog = multi_key_join_catalog();
	for (table, frequency) in [("order_items", 100), ("line_prices", 100)] {
		seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
			&mut catalog,
			table,
			1_000,
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(10)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
			],
			Vec::new(),
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				int64_group_top_k_fact(&[([7, 2], frequency)]),
			)],
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				DistinctCountFact::Approx {
					estimate: 1_000,
					rel_err_ppm: 100_000,
				},
			)],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind effective composite denominator join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(18_182))
	);
}

#[tokio::test]
async fn hot_plus_cold_never_lowers_post_165() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
		&mut catalog,
		"order_items",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(10)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
		],
		Vec::new(),
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 200)]),
		)],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);
	seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
		&mut catalog,
		"line_prices",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(10)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
		],
		Vec::new(),
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 50)]),
		)],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind hot+cold prior floor join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(1_000_000))
	);
}

#[tokio::test]
async fn only_one_group_fact_present_is_byte_identical_to_post_165() {
	let mut top_k_only = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_column_group_top_k(
			&mut top_k_only,
			table,
			10_000,
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(10_000)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(10_000)),
			],
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				int64_group_top_k_fact(&[([7, 2], 5_000)]),
			)],
		);
	}
	let top_k_scoped = Binder::new(&top_k_only, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind top-k-only post-165 join");

	assert_eq!(
		top_k_scoped.facts.estimated_row_count,
		Some(RowCount::new(25_000_000))
	);

	let mut group_ndv_only = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_ndv(
		&mut group_ndv_only,
		"order_items",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(200),
		)],
	);
	seed_table_optimizer_facts_with_column_group_ndv(
		&mut group_ndv_only,
		"line_prices",
		5_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(500)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(5)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);
	let group_ndv_scoped = Binder::new(&group_ndv_only, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind group-NDV-only post-165 join");

	assert_eq!(
		group_ndv_scoped.facts.estimated_row_count,
		Some(RowCount::new(250_000))
	);
}

#[tokio::test]
async fn hot_plus_cold_fails_open_without_either_group_fact() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts(
			&mut catalog,
			table,
			None,
			Some(10_000),
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind no-group-facts join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_000))
	);
}

#[tokio::test]
async fn cold_residual_zero_when_all_tuples_hot() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
			&mut catalog,
			table,
			100,
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(10)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(10)),
			],
			Vec::new(),
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				int64_group_top_k_fact(&[([7, 2], 100)]),
			)],
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				DistinctCountFact::Exact(1),
			)],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind all-hot cold residual join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_000))
	);
}

#[tokio::test]
async fn cold_ndv_zero_with_cold_rows_declines_to_post_165() {
	let mut catalog = multi_key_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
			&mut catalog,
			table,
			1_000,
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			],
			Vec::new(),
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				int64_group_top_k_fact(&[([7, 2], 10), ([8, 3], 10)]),
			)],
			vec![(
				vec![ColumnOrdinal(1), ColumnOrdinal(2)],
				DistinctCountFact::Exact(2),
			)],
		);
	}

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind zero cold NDV with cold rows join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(500_000))
	);
}

#[tokio::test]
async fn cold_residual_applies_range_overlap() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
		&mut catalog,
		"order_items",
		1_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![
			(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000)),
			(ColumnOrdinal(2), range_fact_i64(0, 99, 1_000)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([75, 2], 100)]),
		)],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);
	seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
		&mut catalog,
		"line_prices",
		1_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![
			(ColumnOrdinal(1), range_fact_i64(50, 149, 1_000)),
			(ColumnOrdinal(2), range_fact_i64(0, 99, 1_000)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([75, 2], 100)]),
		)],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind cold range-overlap join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(14_091))
	);
}

#[tokio::test]
async fn disjoint_join_short_circuits_before_hot_plus_cold() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
		&mut catalog,
		"order_items",
		1_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![
			(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000)),
			(ColumnOrdinal(2), range_fact_i64(0, 99, 1_000)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 100)]),
		)],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);
	seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
		&mut catalog,
		"line_prices",
		1_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![
			(ColumnOrdinal(1), range_fact_i64(1_000, 1_099, 1_000)),
			(ColumnOrdinal(2), range_fact_i64(0, 99, 1_000)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 100)]),
		)],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			DistinctCountFact::Exact(100),
		)],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind disjoint stale hot+cold join");

	assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(0)));
}

#[tokio::test]
async fn column_group_top_k_normalizes_group_order_to_join_key_order() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_top_k(
		&mut catalog,
		"order_items",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(10_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10_000)),
		],
		vec![(
			vec![ColumnOrdinal(2), ColumnOrdinal(1)],
			int64_group_top_k_fact(&[([2, 7], 5_000)]),
		)],
	);
	seed_table_optimizer_facts_with_column_group_top_k(
		&mut catalog,
		"line_prices",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(10_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(10_000)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2)],
			int64_group_top_k_fact(&[([7, 2], 5_000)]),
		)],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind reordered group top-k join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(25_000_000))
	);
}

#[tokio::test]
async fn group_must_match_full_join_key_set() {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_column_group_top_k(
		&mut catalog,
		"order_items",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![(
			vec![ColumnOrdinal(1)],
			int64_group_top_k_fact(&[([7, 2], 5_000)]),
		)],
	);
	seed_table_optimizer_facts_with_column_group_top_k(
		&mut catalog,
		"line_prices",
		10_000,
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![(
			vec![ColumnOrdinal(1), ColumnOrdinal(2), ColumnOrdinal(3)],
			int64_group_top_k_fact(&[([7, 2], 5_000)]),
		)],
	);

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind partial/overgrouped top-k join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(10_000))
	);
}

#[tokio::test]
async fn multi_key_join_estimates_from_product_denominator_not_composite_unknown() {
	let mut catalog = multi_key_parent_join_catalog();
	for table in ["order_items", "line_prices"] {
		seed_table_optimizer_facts(
			&mut catalog,
			table,
			None,
			Some(10_000),
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(10_000)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(10_000)),
			],
		);
	}

	let (_plan, trace) = bind_with_trace(
		&multi_key_parent_join_logical(),
		&catalog,
		&auto_broadcast_config(512 * 1024),
	)
	.await
	.expect("bind headline multi-key join");

	assert_eq!(trace.join_decisions.len(), 2);
	assert!(matches!(
		trace.join_decisions[1].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Left,
			..
		}
	));
}

#[tokio::test]
async fn join_output_containment_skips_non_inner_join() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
	);
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		kind: JoinKind::LeftOuter,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind non-inner containment join");

	assert_eq!(scoped.facts.estimated_row_count, None);
}

#[test]
fn composite_hard_bound_build_side_still_rejected() {
	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::Composite,
			byte_upper_bound: Some(ByteCount::new(1_024)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: Some(RowCount::new(1)),
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(8_192)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&auto_broadcast_config(4_096),
	)
	.expect("choose join strategy");

	assert_eq!(decision.strategy, JoinStrategy::HashPartitioned);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::LeftInputNotSingleBaseRelation,
		}
	);
}

#[test]
fn composite_estimate_channel_broadcasts_without_hard_bound_authority() {
	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::Composite,
			byte_upper_bound: Some(ByteCount::new(1)),
			estimated_byte_size: Some(ByteCount::new(1_024)),
			estimated_row_count: Some(RowCount::new(10)),
			row_upper_bound: None,
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(8_192)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&auto_broadcast_config(4_096),
	)
	.expect("choose join strategy");

	assert_eq!(decision.strategy, JoinStrategy::Broadcast);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Left,
			estimated_byte_size: ByteCount::new(1_024),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: None,
		}
	);
}

#[tokio::test]
async fn take_limited_composite_still_does_not_broadcast_without_join_estimated_bytes() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(12));
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::Take {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			limit: 1,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind take-limited composite join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn join_output_containment_estimate_channel_not_hard_bound() {
	let mut catalog = join_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(500))],
	);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan estimate-channel containment join");

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind estimate-channel containment join");

	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(50_000))
	);
	assert!(scoped.facts.estimated_byte_size.is_some());
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.row_upper_bound, None);
	assert!(!matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
}

#[tokio::test]
async fn two_join_plan_broadcasts_small_inner_join_output_by_containment_without_replay() {
	let mut catalog = join_cardinality_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(10_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(10_000))],
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 INNER JOIN regions r ON c.region_id = r.id",
		)
		.expect("plan containment-broadcast two-join SQL");

	let (plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(512 * 1024))
		.await
		.expect("bind containment-broadcast two-join SQL");

	assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 2);
	assert_eq!(trace.join_decisions.len(), 2);
	assert!(matches!(
		trace.join_decisions[1].reason,
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side: BroadcastBuildSide::Left,
			..
		}
	));
}

#[tokio::test]
async fn estimated_broadcast_receipt_is_labeled_estimated() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(12));
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan estimated EXPLAIN join");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0][3],
		ScalarValue::Utf8(Some("swapped_right_build:broadcast_estimated".to_string()))
	);
	assert_eq!(rows[0][11], ScalarValue::Utf8(Some("right".to_string())));
	assert!(matches!(rows[0][12], ScalarValue::Int64(Some(bytes)) if bytes > 0));
}

#[tokio::test]
async fn hard_row_bound_anchors_scan_byte_estimate() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(12));
	let logical = LogicalPlan::ScanNamed {
		table: TableRef::new("customers"),
		projection: None,
	};

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind hard-row scan");

	assert!(matches!(&scoped.plan, ExecutionPlan::Scan { .. }));
	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(12)));
	let expected =
		estimate_materialized_byte_size(RowCount::new(12), scoped.plan.schema().as_ref())
			.expect("fixed scan estimate");
	assert_eq!(scoped.facts.estimated_byte_size, Some(expected));
}

#[tokio::test]
async fn row_zero_fact_does_not_elide_scan() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(0));
	let logical = LogicalPlan::ScanNamed {
		table: TableRef::new("customers"),
		projection: None,
	};

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&logical)
		.await
		.expect("bind zero-row scan");

	assert!(matches!(&scoped.plan, ExecutionPlan::Scan { .. }));
	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.byte_upper_bound, Some(ByteCount::ZERO));
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(0)));
}

#[tokio::test]
async fn known_empty_table_infers_broadcast() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(0));
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan known-empty join");

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind known-empty broadcast join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn filtered_tiny_dim_infers_broadcast() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
			Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
		),
	};
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(filtered_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind filtered tiny dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn narrowing_project_tiny_dim_infers_broadcast() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
	let projected_customers = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		exprs: vec![
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("customers"), "region"),
		],
		output_names: vec![Some("id".to_string()), Some("region".to_string())],
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&projected_customers)
		.await
		.expect("bind projected tiny dimension");
	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.byte_upper_bound, Some(ByteCount::new(1_024)));

	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(projected_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind narrowing projected tiny dimension join");

	assert_join_strategy(&plan, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn expression_project_drops_byte_bound_and_hashes() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), Some(12));
	let expression_customers = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		exprs: vec![
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::Add(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		],
		output_names: vec![Some("id".to_string()), Some("next_id".to_string())],
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&expression_customers)
		.await
		.expect("bind expression projected tiny dimension");
	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(12)));

	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(expression_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind expression projected tiny dimension join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn duplicate_project_drops_byte_bound() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), Some(12));
	let duplicate_customers = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		exprs: vec![
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		],
		output_names: vec![Some("id".to_string()), Some("id_again".to_string())],
	};

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&duplicate_customers)
		.await
		.expect("bind duplicate projected tiny dimension");

	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(12)));
}

#[tokio::test]
async fn pk_filtered_large_table_caps_rows_but_still_hashes() {
	let catalog =
		descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(64 * 1024 * 1024), None);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
		),
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered_customers)
		.await
		.expect("bind PK-filtered large table");
	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(
		scoped.facts.byte_upper_bound,
		Some(ByteCount::new(64 * 1024 * 1024))
	);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(1)));

	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(filtered_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind PK-filtered large table join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn composite_pk_single_column_filter_does_not_cap_rows() {
	let mut catalog = MockCatalog::new();
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("part", DataType::Int64, false),
	]));
	let descriptor = named_descriptor(31, "composite_pk", schema.as_ref().clone());
	let mut binding = TableBinding::new_with_primary_key_columns(
		TableId(31),
		descriptor.table_object_id,
		descriptor.schema_generation,
		descriptor.physical_descriptor_fingerprint,
		schema,
		HashSet::from([ColumnOrdinal(0), ColumnOrdinal(1)]),
	);
	binding.optimizer_facts = TableOptimizerFacts {
		byte_upper_bound: Some(ByteCount::new(1_024)),
		row_upper_bound: None,
		column_ndv: Default::default(),
		column_range: Default::default(),
		column_range_distributions: Default::default(),
		column_heavy_hitters: Default::default(),
		column_group_top_k: Default::default(),
		column_group_ndv: Default::default(),
	};
	catalog
		.register_table(TableRef::new("composite_pk"), binding)
		.expect("register composite-PK binding");
	let filtered = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("composite_pk"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("composite_pk"), "id")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
		),
	};

	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&filtered)
		.await
		.expect("bind composite-PK component filter");

	assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
	assert_eq!(scoped.facts.byte_upper_bound, Some(ByteCount::new(1_024)));
	assert_eq!(scoped.facts.row_upper_bound, None);
}

#[tokio::test]
async fn aggregate_or_expanding_project_stays_composite() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), Some(12));
	let aggregate_customers = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		group_expr: vec![Expr::qualified_column(TableRef::new("customers"), "region")],
		aggr_expr: Vec::new(),
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&aggregate_customers)
		.await
		.expect("bind aggregate tiny dimension");
	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.row_upper_bound, None);

	let project_over_aggregate = LogicalPlan::Project {
		input: Box::new(aggregate_customers),
		exprs: vec![Expr::column("region")],
		output_names: vec![Some("region".to_string())],
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&project_over_aggregate)
		.await
		.expect("bind project over aggregate tiny dimension");
	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.byte_upper_bound, None);
}

#[tokio::test]
async fn filtered_dim_without_facts_still_hashes() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, None);
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
			Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
		),
	};
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(filtered_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind filtered dimension without facts join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn left_tiny_with_filtered_probe_infers_broadcast() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	let filtered_orders = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		predicate: Expr::Eq(
			Box::new(Expr::qualified_column(
				TableRef::new("orders"),
				"customer_id",
			)),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(2)))),
		),
	};
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		right: Box::new(filtered_orders),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind left-tiny filtered-probe join");

	let ExecutionPlan::Join {
		left,
		right,
		strategy,
		..
	} = plan
	else {
		panic!("expected natural broadcast join");
	};
	assert_eq!(strategy, JoinStrategy::Broadcast);
	assert!(matches!(terminal_exchange(&left), Some((1, _))));
	assert!(terminal_exchange(&right).is_none());
}

#[tokio::test]
async fn right_tiny_with_projected_probe_infers_broadcast() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let projected_orders = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		exprs: vec![
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("orders"), "id"),
		],
		output_names: vec![Some("customer_id".to_string()), Some("id".to_string())],
	};
	let logical = LogicalPlan::Join {
		left: Box::new(projected_orders),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::column("customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind right-tiny projected-probe join");

	let ExecutionPlan::Project {
		input,
		output_schema,
		..
	} = plan
	else {
		panic!("right-build broadcast must restore declared order with a project");
	};
	assert_eq!(
		field_names(&output_schema),
		vec!["customer_id", "id", "id", "region"]
	);
	let ExecutionPlan::Join {
		left,
		right,
		strategy,
		..
	} = input.as_ref()
	else {
		panic!("restore project must wrap the physical join");
	};
	assert_eq!(*strategy, JoinStrategy::Broadcast);
	assert!(matches!(terminal_exchange(left), Some((1, _))));
	assert!(terminal_exchange(right).is_none());
}

#[tokio::test]
async fn nested_hash_probe_still_stays_hash() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = nested_hash_probe_with_tiny_right();

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind nested-probe join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
	assert_join_strategy_count(&plan, JoinStrategy::HashPartitioned, 2);
}

#[tokio::test]
async fn trace_records_hash_reason_for_non_scan_linear_probe() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = nested_hash_probe_with_tiny_right();

	let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind with trace");

	assert_eq!(trace.join_decisions.len(), 2);
	assert_eq!(
		trace
			.join_decisions
			.iter()
			.map(|receipt| receipt.target.join_index)
			.collect::<Vec<_>>(),
		vec![0, 1]
	);
	let receipt = trace.join_decisions[1];
	assert_eq!(receipt.strategy, JoinStrategy::HashPartitioned);
	assert_eq!(receipt.placement, JoinPhysicalPlacement::Natural);
	assert_eq!(
		receipt.reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::ProbeNotScanLinear(ProbeLinearityFence::NonBroadcastJoin),
		}
	);
}

#[tokio::test]
async fn probe_linearity_project_and_filter_are_transparent() {
	let logical = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(
					TableRef::new("orders"),
					"customer_id",
				)),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
			),
		}),
		exprs: vec![Expr::qualified_column(
			TableRef::new("orders"),
			"customer_id",
		)],
		output_names: vec![Some("customer_id".to_string())],
	};
	let plan = bind(&logical, &join_catalog(), &PlannerConfig::default())
		.await
		.expect("bind transparent probe");

	assert_eq!(broadcast_probe_linearity(&plan), ProbeLinearity::Linear);
}

#[tokio::test]
async fn probe_linearity_limit_sort_aggregate_exchange_are_distinct_causes() {
	let catalog = join_catalog();
	let scan = bind(
		&LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		},
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind scan");
	let mut limited_scan = scan.clone();
	let ExecutionPlan::Scan { row_limit, .. } = &mut limited_scan else {
		panic!("expected scan, got {limited_scan:?}");
	};
	*row_limit = Some(1);

	let take = bind(
		&LogicalPlan::Take {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			limit: 1,
		},
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind take");
	let sort = bind(
		&LogicalPlan::Sort {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			order_by: vec![SortExpr {
				expr: Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				descending: false,
				nulls_first: false,
			}],
			limit: None,
			offset: 0,
		},
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind sort");
	let aggregate = bind(
		&LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			group_expr: vec![Expr::qualified_column(
				TableRef::new("orders"),
				"customer_id",
			)],
			aggr_expr: Vec::new(),
		},
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind aggregate");
	let exchange = bind(
		&LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			partition_count: 2,
			key: vec![Expr::qualified_column(
				TableRef::new("orders"),
				"customer_id",
			)],
		},
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind exchange");

	assert_eq!(
		broadcast_probe_linearity(&limited_scan).blocked_fence(),
		Some(ProbeLinearityFence::ScanRowLimit)
	);
	assert_eq!(
		broadcast_probe_linearity(&take).blocked_fence(),
		Some(ProbeLinearityFence::Take)
	);
	assert_eq!(
		broadcast_probe_linearity(&sort).blocked_fence(),
		Some(ProbeLinearityFence::Sort)
	);
	assert_eq!(
		broadcast_probe_linearity(&aggregate).blocked_fence(),
		Some(ProbeLinearityFence::Aggregate)
	);
	assert_eq!(
		broadcast_probe_linearity(&exchange).blocked_fence(),
		Some(ProbeLinearityFence::Exchange)
	);
}

#[tokio::test]
async fn probe_linearity_nested_broadcast_propagates_inner_cause() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	let broadcast = bind(
		&LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
		},
		&catalog,
		&auto_broadcast_config(4_096),
	)
	.await
	.expect("bind broadcast join");
	let sorted_probe = bind(
		&LogicalPlan::Sort {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			order_by: vec![SortExpr {
				expr: Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				descending: false,
				nulls_first: false,
			}],
			limit: None,
			offset: 0,
		},
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("bind sorted probe");
	let ExecutionPlan::Join {
		replay_id,
		replay_eligibility,
		left,
		kind,
		left_keys,
		right_keys,
		match_filter,
		strategy,
		output_schema,
		..
	} = broadcast
	else {
		panic!("expected broadcast join");
	};
	let nested = ExecutionPlan::Join {
		replay_id,
		replay_eligibility,
		left,
		right: Box::new(sorted_probe),
		kind,
		left_keys,
		right_keys,
		match_filter,
		null_safe_keys: false,
		strategy,
		output_schema,
	};

	assert_eq!(
		broadcast_probe_linearity(&nested).blocked_fence(),
		Some(ProbeLinearityFence::Sort)
	);
}

#[tokio::test]
async fn probe_linearity_receipts_do_not_change_bound_plan() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = nested_hash_probe_with_tiny_right();
	let config = auto_broadcast_config(4_096);

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind without trace");
	let (traced_plan, trace) = bind_with_trace(&logical, &catalog, &config)
		.await
		.expect("bind with trace");

	assert!(trace.join_decisions.iter().any(|receipt| {
		matches!(
			receipt.reason,
			JoinStrategyDecisionReason::HashByDefault {
				reason: HashByDefaultReason::ProbeNotScanLinear(
					ProbeLinearityFence::NonBroadcastJoin
				),
			}
		)
	}));
	assert_eq!(format!("{plan:?}"), format!("{traced_plan:?}"));
}

#[tokio::test]
async fn probe_with_aggregate_stays_hash() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let aggregate_orders = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		group_expr: vec![Expr::qualified_column(
			TableRef::new("orders"),
			"customer_id",
		)],
		aggr_expr: Vec::new(),
	};
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		right: Box::new(aggregate_orders),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::column("customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind aggregate-probe join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn probe_with_take_or_limited_scan_stays_hash() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let taken_orders = LogicalPlan::Take {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		limit: 1,
	};
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		right: Box::new(taken_orders.clone()),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind take-probe join");
	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);

	let limited_scan = bind(&taken_orders, &catalog, &PlannerConfig::default())
		.await
		.expect("bind limited scan");
	assert!(!broadcast_probe_is_scan_linear(&limited_scan));
}

#[tokio::test]
async fn sql_broadcast_hint_binds_pinned_by_author() {
	let logical = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
		)
		.expect("plan hinted broadcast");
	assert!(matches!(
		logical,
		LogicalPlan::Join {
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::SqlHint,
				build_side: Some(BroadcastBuildSide::Left),
			},
			..
		}
	));

	let plan = bind(&logical, &join_catalog(), &PlannerConfig::default())
		.await
		.expect("pinned broadcast binds without auto config");
	assert_join_strategy(&plan, JoinStrategy::Broadcast);

	let preserved_side_pin = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM customers c LEFT JOIN orders o ON c.id = o.customer_id",
		)
		.expect("frontend records preserved-side pin");
	let err = bind(
		&preserved_side_pin,
		&join_catalog(),
		&PlannerConfig::default(),
	)
	.await
	.expect_err("preserved-side broadcast pin must fail");
	assert!(
		err.to_string()
			.contains("target the null-supplied side or drop the hint"),
		"{err}"
	);
}

#[tokio::test]
async fn sql_left_outer_broadcast_hint_on_null_supplied_side_binds_preserved_probe_broadcast() {
	let logical = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id",
		)
		.expect("plan left outer safe-side broadcast hint");
	let (_plan, trace) = bind_with_trace(&logical, &join_catalog(), &PlannerConfig::default())
		.await
		.expect("bind left outer safe-side broadcast");

	assert_eq!(trace.join_decisions.len(), 1);
	assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
	assert_eq!(
		trace.join_decisions[0].placement,
		JoinPhysicalPlacement::SwappedRightBuild
	);
	assert_eq!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::PinnedByAuthor
	);
}

#[tokio::test]
async fn sql_right_outer_broadcast_hint_on_null_supplied_side_binds_preserved_probe_broadcast() {
	let logical = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(o) */ * FROM orders o RIGHT JOIN customers c ON o.customer_id = c.id",
		)
		.expect("plan right outer safe-side broadcast hint");
	let (_plan, trace) = bind_with_trace(&logical, &join_catalog(), &PlannerConfig::default())
		.await
		.expect("bind right outer safe-side broadcast");

	assert_eq!(trace.join_decisions.len(), 1);
	assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
	assert_eq!(
		trace.join_decisions[0].placement,
		JoinPhysicalPlacement::Natural
	);
	assert_eq!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::PinnedByAuthor
	);
}

#[tokio::test]
async fn sql_full_outer_broadcast_hint_rejects_without_demotion() {
	let logical = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM orders o FULL OUTER JOIN customers c ON o.customer_id = c.id",
		)
		.expect("plan full outer broadcast hint");
	let err = bind(&logical, &join_catalog(), &PlannerConfig::default())
		.await
		.expect_err("true full outer broadcast pin must fail");
	assert!(
		err.to_string()
			.contains("FullOuter requires demotion before broadcast can be pinned"),
		"{err}"
	);
}

#[tokio::test]
async fn sql_full_outer_broadcast_hint_is_honored_after_both_sides_null_rejecting_demote() {
	let logical = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM orders o FULL OUTER JOIN customers c \
			 ON o.customer_id = c.id WHERE o.id IS NOT NULL AND c.id IS NOT NULL",
		)
		.expect("plan demoted full outer broadcast hint");
	let (_plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &join_catalog(), &PlannerConfig::default())
			.await
			.expect("bind demoted full outer broadcast hint");

	assert_eq!(render_trace.outer_join_demotions.len(), 1);
	assert_eq!(
		render_trace.outer_join_demotions[0].from_kind,
		JoinKind::FullOuter
	);
	assert_eq!(
		render_trace.outer_join_demotions[0].to_kind,
		JoinKind::Inner
	);
	assert_eq!(trace.join_decisions.len(), 1);
	assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
	assert_eq!(
		trace.join_decisions[0].placement,
		JoinPhysicalPlacement::SwappedRightBuild
	);
	assert_eq!(
		trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::PinnedByAuthor
	);
}

fn direct_left_outer_broadcast_pin(build_side: BroadcastBuildSide) -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		kind: JoinKind::LeftOuter,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Direct,
			build_side: Some(build_side),
		},
	}
}

#[tokio::test]
async fn direct_pinned_non_inner_safe_side_binds() {
	let catalog = join_catalog();
	let (_plan, trace) = bind_with_trace(
		&direct_left_outer_broadcast_pin(BroadcastBuildSide::Right),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect("safe-side direct pin binds");
	assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
	assert_eq!(
		trace.join_decisions[0].placement,
		JoinPhysicalPlacement::SwappedRightBuild
	);
}

#[tokio::test]
async fn direct_pinned_non_inner_wrong_side_rejects() {
	let catalog = join_catalog();
	let err = bind(
		&direct_left_outer_broadcast_pin(BroadcastBuildSide::Left),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.expect_err("preserved-side direct pin rejects");
	assert!(
		err.to_string()
			.contains("target the null-supplied side or drop the hint"),
		"{err}"
	);
}

#[tokio::test]
async fn rune_post_join_broadcast_pin_matches_sql_hint_and_rejects_preserved_side() {
	let catalog = join_catalog();
	let sql = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id",
		)
		.expect("plan SQL safe-side hint");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o")
                .left_join(
                    ctx.table("customers").alias("c"),
                    qcol("o", "customer_id").eq(qcol("c", "id"))
                )
                .broadcast_build("right")
        }
    "#,
	)
	.await;

	for (source, logical) in [("sql", sql), ("rune", rune)] {
		let (_plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.unwrap_or_else(|error| panic!("{source} safe-side broadcast should bind: {error}"));
		assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
		assert_eq!(
			trace.join_decisions[0].placement,
			JoinPhysicalPlacement::SwappedRightBuild,
			"{source}"
		);
		assert_eq!(
			trace.join_decisions[0].reason,
			JoinStrategyDecisionReason::PinnedByAuthor,
			"{source}"
		);
	}

	let bad_rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o")
                .left_join(
                    ctx.table("customers").alias("c"),
                    qcol("o", "customer_id").eq(qcol("c", "id"))
                )
                .broadcast_build("left")
        }
    "#,
	)
	.await;
	let err = bind(&bad_rune, &catalog, &PlannerConfig::default())
		.await
		.expect_err("Rune preserved-side broadcast pin rejects like SQL");
	assert!(
		err.to_string()
			.contains("target the null-supplied side or drop the hint"),
		"{err}"
	);
}

#[tokio::test]
async fn force_hash_join_ids_demotes_auto_and_sql_broadcast_only() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let force_hash = PlannerConfig {
		force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
		..PlannerConfig::default()
	};

	let auto_logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan auto join");
	let (auto_plan, auto_trace) = bind_with_trace(&auto_logical, &catalog, &force_hash)
		.await
		.expect("bind force-hash auto join");
	assert_join_strategy(&auto_plan, JoinStrategy::HashPartitioned);
	assert!(matches!(
		auto_trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::HashAfterBroadcastCap
		}
	));

	let sql_hint = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
		)
		.expect("plan SQL hinted broadcast");
	let (sql_plan, sql_trace) = bind_with_trace(&sql_hint, &catalog, &force_hash)
		.await
		.expect("bind force-hash SQL hint");
	let ExecutionPlan::Join {
		left,
		right,
		strategy,
		..
	} = sql_plan
	else {
		panic!("force-hash SQL hint should bind as a hash join");
	};
	assert_eq!(strategy, JoinStrategy::HashPartitioned);
	assert!(matches!(
		terminal_exchange(&left),
		Some((DEFAULT_AUTO_JOIN_PARTITION_COUNT, _))
	));
	assert!(matches!(
		terminal_exchange(&right),
		Some((DEFAULT_AUTO_JOIN_PARTITION_COUNT, _))
	));
	assert!(matches!(
		sql_trace.join_decisions[0].reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::HashAfterBroadcastCap
		}
	));

	let direct = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("customers", "id", 1)),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Direct,
			build_side: Some(BroadcastBuildSide::Left),
		},
	};
	let direct_plan = bind(&direct, &catalog, &force_hash)
		.await
		.expect("bind direct pinned broadcast under force hash");
	assert_join_strategy(&direct_plan, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn mixed_sql_and_rune_broadcast_plan_replays_sql_preserves_rune() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let sql_broadcast = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
		)
		.expect("plan SQL hinted broadcast");
	let rune_build = LogicalPlan::Alias {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		alias: TableRef::new("c2"),
		columns: Vec::new(),
	};
	let mixed = LogicalPlan::Join {
		left: Box::new(LogicalPlan::Exchange {
			partition_count: 1,
			key: vec![Expr::qualified_column(TableRef::new("c2"), "id")],
			input: Box::new(rune_build),
		}),
		right: Box::new(sql_broadcast),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("c2"), "id"),
			Expr::qualified_column(TableRef::new("c"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Rune,
			build_side: Some(BroadcastBuildSide::Left),
		},
	};
	let config = PlannerConfig {
		force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
		..PlannerConfig::default()
	};

	let (plan, trace) = bind_with_trace(&mixed, &catalog, &config)
		.await
		.expect("bind mixed SQL/Rune targeted replay");
	let mut metadata = Vec::new();
	collect_join_replay_metadata(&plan, &mut metadata);

	assert_eq!(
		trace.join_decisions[0].strategy,
		JoinStrategy::HashPartitioned
	);
	assert_eq!(trace.join_decisions[1].strategy, JoinStrategy::Broadcast);
	assert!(metadata.contains(&(
		JoinReplayId(0),
		BroadcastReplayEligibility::Replayable,
		JoinStrategy::HashPartitioned,
	)));
	assert!(metadata.contains(&(
		JoinReplayId(1),
		BroadcastReplayEligibility::Terminal,
		JoinStrategy::Broadcast,
	)));
}

#[tokio::test]
async fn pinned_author_choice_not_downgraded_or_upgraded_by_inference() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);

	let pinned_hash = LogicalPlan::Join {
		left: Box::new(exchanged_named_scan("customers", "id", 2)),
		right: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	};
	let plan = bind(&pinned_hash, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind pinned hash");
	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);

	let pinned_broadcast = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
		)
		.expect("plan pinned broadcast");
	let plan = bind(&pinned_broadcast, &catalog, &PlannerConfig::default())
		.await
		.expect("bind pinned broadcast");
	assert_join_strategy(&plan, JoinStrategy::Broadcast);
}

#[tokio::test]
async fn trace_records_pinned_strategy_intent() {
	let logical = SqlPlanner::new()
		.plan(
			"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
		)
		.expect("plan hinted broadcast");

	let (_plan, trace) = bind_with_trace(&logical, &join_catalog(), &PlannerConfig::default())
		.await
		.expect("bind with trace");

	assert_eq!(trace.join_decisions.len(), 1);
	let receipt = trace.join_decisions[0];
	assert_eq!(receipt.target.join_index, 0);
	assert_eq!(receipt.strategy, JoinStrategy::Broadcast);
	assert_eq!(receipt.placement, JoinPhysicalPlacement::Natural);
	assert_eq!(receipt.reason, JoinStrategyDecisionReason::PinnedByAuthor);
}

#[tokio::test]
async fn trace_indexes_each_join_deterministically() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
		)
		.expect("plan two-join SQL");

	let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind with trace");

	assert_eq!(
		trace
			.join_decisions
			.iter()
			.map(|receipt| receipt.target.join_index)
			.collect::<Vec<_>>(),
		vec![0, 1]
	);
}

#[tokio::test]
async fn bind_remains_structurally_identical_with_and_without_trace() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 3_072);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan SQL join");

	let plain = bind(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind without trace");
	let (traced, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind with trace");

	assert!(!trace.join_decisions.is_empty());
	assert_eq!(format!("{plain:?}"), format!("{traced:?}"));
}

#[tokio::test]
async fn explain_plans_unchanged_under_render_capture() {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM customers WHERE amount > 49")
		.expect("plan filtered query");
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		Some(1_000),
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);

	let (plain_plan, plain_trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind normal trace");
	let (explain_plan, explain_trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind explain trace");

	assert_eq!(plain_trace, explain_trace);
	assert_eq!(format!("{plain_plan:?}"), format!("{explain_plan:?}"));
	assert_eq!(render_trace.scan_filters.len(), 1);
}

#[tokio::test]
async fn normal_bind_does_not_materialize_explain_render_strings() {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM customers WHERE amount > 49")
		.expect("plan filtered query");
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		Some(1_000),
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);

	let (_plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind normal trace");
	let trace_debug = format!("{trace:?}");

	assert_eq!(trace.scan_filter_decisions.len(), 1);
	assert!(!trace_debug.contains("predicate_text"));
	assert!(!trace_debug.contains("root["));
	assert!(!trace_debug.contains("amount#"));
}

#[test]
fn spent_column_name_declines_when_source_ordinal_is_ambiguous() {
	let mut builder = BoundExprProgramBuilder::new();
	let left = builder.append_column(test_bool_column("amount", 1, true));
	let right = builder.append_column(test_bool_column("renamed_amount", 1, true));
	let root = builder.append_binary(BoundBinaryOp::And, left, right, DataType::Boolean);
	builder.push_root(root);
	let predicate = builder.build();

	assert_eq!(
		spent_column_name_for_predicate(&predicate, ColumnOrdinal(1)),
		None
	);
}

#[tokio::test]
async fn sql_explain_broadcast_inference_renders_receipt_rows() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 3_072);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan EXPLAIN join");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

	assert_eq!(
		rows,
		vec![vec![
			ScalarValue::Utf8(Some("join".to_string())),
			ScalarValue::Utf8(Some("join[0]".to_string())),
			ScalarValue::Utf8(Some("broadcast".to_string())),
			ScalarValue::Utf8(Some("swapped_right_build:broadcast_inferred".to_string())),
			ScalarValue::Utf8(None),
			ScalarValue::Utf8(None),
			ScalarValue::Utf8(None),
			ScalarValue::Int64(None),
			ScalarValue::Utf8(None),
			ScalarValue::Utf8(None),
			ScalarValue::Utf8(None),
			ScalarValue::Utf8(Some("right".to_string())),
			ScalarValue::Int64(Some(1_024)),
			ScalarValue::Int64(Some(4_096)),
			ScalarValue::Int64(Some(3_072)),
		]]
	);
}

#[tokio::test]
async fn join_rows_keep_scan_readability_columns_null() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 3_072);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan EXPLAIN join");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0][0], ScalarValue::Utf8(Some("join".to_string())));
	assert_eq!(rows[0][4], ScalarValue::Utf8(None));
	assert_eq!(rows[0][5], ScalarValue::Utf8(None));
	assert_eq!(rows[0][6], ScalarValue::Utf8(None));
	assert_eq!(rows[0][10], ScalarValue::Utf8(None));
	assert_eq!(rows[0][11], ScalarValue::Utf8(Some("right".to_string())));
}

#[tokio::test]
async fn hash_join_probe_not_scan_linear_renders_typed_cause() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = LogicalPlan::Explain {
		input: Box::new(nested_hash_probe_with_tiny_right()),
	};

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let join_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join".to_string())))
		.collect::<Vec<_>>();

	assert_eq!(join_rows.len(), 2);
	assert_eq!(
		join_rows[1][1],
		ScalarValue::Utf8(Some("join[1]".to_string()))
	);
	assert_eq!(
		join_rows[1][2],
		ScalarValue::Utf8(Some("hash_partitioned".to_string()))
	);
	assert_eq!(
		join_rows[1][3],
		ScalarValue::Utf8(Some(
			"natural:hash_by_default:probe_not_scan_linear:non_broadcast_join".to_string()
		))
	);
}

#[tokio::test]
async fn explain_broadcast_star_emits_one_receipt_per_link() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
		)
		.expect("plan EXPLAIN broadcast-star join");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let join_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join".to_string())))
		.collect::<Vec<_>>();

	assert_eq!(join_rows.len(), 2);
	assert_eq!(
		join_rows
			.iter()
			.map(|row| row[1].clone())
			.collect::<Vec<_>>(),
		vec![
			ScalarValue::Utf8(Some("join[0]".to_string())),
			ScalarValue::Utf8(Some("join[1]".to_string()))
		]
	);
	for row in join_rows {
		assert_eq!(row[2], ScalarValue::Utf8(Some("broadcast".to_string())));
		assert_eq!(
			row[3],
			ScalarValue::Utf8(Some("swapped_right_build:broadcast_inferred".to_string()))
		);
		assert_eq!(row[11], ScalarValue::Utf8(Some("right".to_string())));
	}
}

#[tokio::test]
async fn explain_pinned_broadcast_reports_pinned_by_author() {
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT /*+ BROADCAST(c) */ * \
				 FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
		)
		.expect("plan EXPLAIN hinted broadcast");

	let (_schema, rows) =
		bind_explain_rows(&logical, &join_catalog(), &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0][0], ScalarValue::Utf8(Some("join".to_string())));
	assert_eq!(rows[0][1], ScalarValue::Utf8(Some("join[0]".to_string())));
	assert_eq!(rows[0][2], ScalarValue::Utf8(Some("broadcast".to_string())));
	assert_eq!(
		rows[0][3],
		ScalarValue::Utf8(Some("natural:pinned_by_author".to_string()))
	);
	assert_eq!(rows[0][7], ScalarValue::Int64(None));
	assert_eq!(rows[0][11], ScalarValue::Utf8(Some("left".to_string())));
}

#[tokio::test]
async fn hinted_safe_side_outer_join_explain_reports_pinned_by_author() {
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT /*+ BROADCAST(c) */ * \
				 FROM orders o LEFT JOIN customers c ON o.customer_id = c.id",
		)
		.expect("plan EXPLAIN hinted safe-side broadcast");

	let (_schema, rows) =
		bind_explain_rows(&logical, &join_catalog(), &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0][0], ScalarValue::Utf8(Some("join".to_string())));
	assert_eq!(rows[0][1], ScalarValue::Utf8(Some("join[0]".to_string())));
	assert_eq!(rows[0][2], ScalarValue::Utf8(Some("broadcast".to_string())));
	assert_eq!(
		rows[0][3],
		ScalarValue::Utf8(Some("swapped_right_build:pinned_by_author".to_string()))
	);
	assert_eq!(rows[0][11], ScalarValue::Utf8(Some("right".to_string())));
}

#[tokio::test]
async fn explain_emits_one_row_per_join_in_index_order() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
		)
		.expect("plan EXPLAIN two-join SQL");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let join_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join".to_string())))
		.collect::<Vec<_>>();

	assert_eq!(
		join_rows
			.iter()
			.map(|row| row[1].clone())
			.collect::<Vec<_>>(),
		vec![
			ScalarValue::Utf8(Some("join[0]".to_string())),
			ScalarValue::Utf8(Some("join[1]".to_string()))
		]
	);
}

#[tokio::test]
async fn star_order_evidence_reports_declared_vs_preferred() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_star_order_sql()))
		.expect("plan non-optimal star EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let row = join_order_row(&rows);

	assert_eq!(row[0], ScalarValue::Utf8(Some("join_order".to_string())));
	assert_eq!(row[1], ScalarValue::Utf8(Some("star[probe=o]".to_string())));
	assert_eq!(row[2], ScalarValue::Utf8(Some("reordered".to_string())));
	assert_utf8_contains(&row[3], "declared_order=[c,p,s]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[s,p,c]");
	assert_utf8_contains(&row[3], "applied_order=[s,p,c]");
	assert_utf8_contains(&row[3], "confidence=hard");
	assert_utf8_contains(&row[3], "c:5000000:single_key_ndv_containment:hard");
	assert_utf8_contains(&row[3], "p:1000000:single_key_ndv_containment:hard");
	assert_utf8_contains(&row[3], "s:100000:single_key_ndv_containment:hard");
}

#[tokio::test]
async fn star_order_actuation_reorders_non_optimal_broadcast_star() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");

	let (plain_plan, plain_trace) =
		bind_with_trace(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind without render trace");
	let (explain_plan, explain_trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind with render trace");

	assert_eq!(plain_trace, explain_trace);
	assert_eq!(format!("{plain_plan:?}"), format!("{explain_plan:?}"));
	assert!(matches!(plain_plan, ExecutionPlan::Project { .. }));
	assert_eq!(
		broadcast_build_scan_ids_in_applied_order(&plain_plan),
		vec![TableId(24), TableId(23), TableId(22)]
	);
	let JoinOrderRenderState::StarReceipts(receipts) = &render_trace.join_order else {
		panic!("expected star-order receipt");
	};
	assert!(matches!(
		receipts.as_slice(),
		[StarOrderRenderReceipt {
			outcome: StarOrderOutcome::Recognized(StarOrderEvidence {
				action: StarOrderAction::Reordered,
				strategy: StarOrderStrategy::Broadcast,
				..
			}),
			..
		}]
	));
}

#[tokio::test]
async fn optimizer_decision_accrual_is_always_on() {
	let catalog = star_join_order_catalog();
	let config = auto_broadcast_config(64 * 1024 * 1024);
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");

	let _plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind normal star query");
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(&rows, "flat_star", "reordered", None),
		1
	);
}

#[tokio::test]
async fn optimizer_decisions_accrue_across_queries() {
	let config = auto_broadcast_config(4_096);
	let star_catalog = star_join_order_catalog();
	let mixed_snowflake_catalog = mixed_strategy_snowflake_join_order_catalog();
	let reordered = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan reordered star");
	let kept = SqlPlanner::new()
		.plan(preferred_star_order_sql())
		.expect("plan kept star");
	let actuated = SqlPlanner::new()
		.plan(mixed_strategy_snowflake_order_sql())
		.expect("plan actuated snowflake");

	let _ = bind(&reordered, &star_catalog, &config)
		.await
		.expect("bind reordered star");
	let _ = bind(&kept, &star_catalog, &config)
		.await
		.expect("bind kept star");
	let _ = bind(&actuated, &mixed_snowflake_catalog, &config)
		.await
		.expect("bind actuated snowflake");
	let rows = optimizer_decision_rows(&star_catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(&rows, "flat_star", "reordered", None),
		1
	);
	assert_eq!(
		optimizer_decision_count(&rows, "flat_star", "kept_optimal", None),
		1
	);
	// ADR 193: the mixed-strategy snowflake root now actuates (it no longer declines as
	// `mixed_strategy_star`). The broadcast-first declared order is already optimal at the root, and
	// the bushy `customers` branch reorders — both decisions accrue alongside the flat-star ones.
	assert_eq!(
		optimizer_decision_count(&rows, "snowflake_root", "kept_optimal", None),
		1
	);
	assert_eq!(
		optimizer_decision_count(&rows, "snowflake_branch", "reordered", None),
		1
	);
}

#[tokio::test]
async fn optimizer_decisions_report_aggregates_by_kind_and_outcome() {
	let catalog = star_join_order_catalog();
	let config = auto_broadcast_config(64 * 1024 * 1024);
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");

	for _ in 0..2 {
		let _ = bind(&logical, &catalog, &config)
			.await
			.expect("bind repeated star");
	}
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		rows.iter()
			.filter(|row| {
				row[0] == ScalarValue::Utf8(Some("flat_star".to_string()))
					&& row[1] == ScalarValue::Utf8(Some("reordered".to_string()))
			})
			.count(),
		1
	);
	assert_eq!(
		optimizer_decision_count(&rows, "flat_star", "reordered", None),
		2
	);
}

#[tokio::test]
async fn optimizer_decisions_records_decline_reasons() {
	let catalog = mixed_strategy_star_join_order_catalog();
	let config = auto_broadcast_config(4_096);
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan mixed strategy star");

	let _ = bind(&logical, &catalog, &config)
		.await
		.expect("bind mixed strategy star");
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(&rows, "flat_star", "declined", Some("mixed_strategy_star")),
		1
	);
}

#[tokio::test]
async fn uniform_hash_star_reorders_on_robust_win() {
	let catalog = hash_star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_star_order_sql()))
		.expect("plan non-optimal hash star EXPLAIN");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &auto_broadcast_config(1)).await;
	let row = join_order_row(&rows);

	assert_eq!(row[0], ScalarValue::Utf8(Some("join_order".to_string())));
	assert_eq!(row[2], ScalarValue::Utf8(Some("reordered".to_string())));
	assert_utf8_contains(&row[3], "strategy=hash_partitioned");
	assert_utf8_contains(&row[3], "declared_order=[c,p,s]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[s,p,c]");
	assert_utf8_contains(&row[3], "applied_order=[s,p,c]");
}

#[tokio::test]
async fn hash_star_reorder_preserves_hash_strategy() {
	let catalog = hash_star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal hash star");

	let (plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(1))
			.await
			.expect("bind hash star with trace");

	assert_eq!(trace.join_decisions.len(), 3);
	assert!(trace.join_decisions.iter().all(|receipt| {
		receipt.strategy == JoinStrategy::HashPartitioned
			&& receipt.placement == JoinPhysicalPlacement::Natural
	}));
	assert_join_strategy_count(&plan, JoinStrategy::HashPartitioned, 3);
	let JoinOrderRenderState::StarReceipts(receipts) = &render_trace.join_order else {
		panic!("expected star-order receipt");
	};
	assert!(matches!(
		receipts.as_slice(),
		[StarOrderRenderReceipt {
			outcome: StarOrderOutcome::Recognized(StarOrderEvidence {
				action: StarOrderAction::Reordered,
				strategy: StarOrderStrategy::HashPartitioned,
				..
			}),
			..
		}]
	));
}

#[tokio::test]
async fn hash_star_reorder_is_results_identical() {
	let (catalog, storage) = star_order_execution_fixture().await;
	let config = auto_broadcast_config(1);
	let (reordered_schema, reordered_rows) = execute_star_order_query(
		&catalog,
		storage.clone(),
		non_optimal_star_order_project_sql(),
		&config,
	)
	.await;
	let (declared_schema, declared_rows) = execute_star_order_query(
		&catalog,
		storage,
		preferred_star_order_project_sql(),
		&config,
	)
	.await;

	assert_eq!(
		schema_signature(&reordered_schema),
		schema_signature(&declared_schema)
	);
	assert_eq!(reordered_rows, declared_rows);
}

#[tokio::test]
async fn mixed_strategy_star_declines() {
	let catalog = mixed_strategy_star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_star_order_sql()))
		.expect("plan mixed strategy star EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let row = join_order_row(&rows);

	assert_eq!(
		row[2],
		ScalarValue::Utf8(Some("declined_mixed_strategy_star".to_string()))
	);
	assert_utf8_contains(&row[3], "reason=mixed_strategy_star");
}

#[tokio::test]
async fn declared_and_selected_preflights_share_uniform_strategy() {
	let catalog = mixed_strategy_star_join_order_catalog();
	let non_optimal = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_star_order_sql()))
		.expect("plan mixed declared star EXPLAIN");
	let preferred = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", preferred_star_order_sql()))
		.expect("plan hash preferred star EXPLAIN");

	let (_schema, non_optimal_rows) =
		bind_explain_rows(&non_optimal, &catalog, &auto_broadcast_config(4_096)).await;
	let (_schema, preferred_rows) =
		bind_explain_rows(&preferred, &catalog, &auto_broadcast_config(4_096)).await;
	let non_optimal_row = join_order_row(&non_optimal_rows);
	let preferred_row = join_order_row(&preferred_rows);

	assert_eq!(
		non_optimal_row[2],
		ScalarValue::Utf8(Some("declined_mixed_strategy_star".to_string()))
	);
	assert_eq!(
		preferred_row[2],
		ScalarValue::Utf8(Some("kept_already_optimal".to_string()))
	);
	assert_utf8_contains(&preferred_row[3], "strategy=hash_partitioned");
	assert_utf8_contains(&preferred_row[3], "declared_order=[s,p,c]");
}

#[tokio::test]
async fn broadcast_star_order_unchanged_from_178() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_star_order_sql()))
		.expect("plan non-optimal broadcast star EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let row = join_order_row(&rows);

	assert_eq!(row[2], ScalarValue::Utf8(Some("reordered".to_string())));
	assert_utf8_contains(&row[3], "strategy=broadcast");
	assert_utf8_contains(&row[3], "applied_order=[s,p,c]");
}

#[tokio::test]
async fn star_probe_not_scan_linear_renders_same_cause_via_shared_helper() {
	let mut catalog = star_join_order_catalog();
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = LogicalPlan::Explain {
		input: Box::new(nested_hash_probe_with_tiny_right()),
	};

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let row = join_order_row(&rows);

	assert_eq!(
		row[2],
		ScalarValue::Utf8(Some("declined_probe_not_scan_linear".to_string()))
	);
	assert_utf8_contains(&row[3], "reason=probe_not_scan_linear:exchange");
}

#[tokio::test]
async fn snowflake_order_evidence_reports_nested_declared_vs_preferred() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", observed_snowflake_order_sql()))
		.expect("plan snowflake EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let row = join_order_snowflake_row(&rows);

	assert_eq!(
		row[0],
		ScalarValue::Utf8(Some("join_order_snowflake".to_string()))
	);
	assert_eq!(
		row[1],
		ScalarValue::Utf8(Some("snowflake[probe=o]".to_string()))
	);
	assert_eq!(row[2], ScalarValue::Utf8(Some("would_reorder".to_string())));
	assert_utf8_contains(&row[3], "root=o");
	assert_utf8_contains(&row[3], "declared_order=[c,s]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[s,c]");
	assert_utf8_contains(&row[3], "root=c");
	assert_utf8_contains(&row[3], "declared_order=[r,g]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[g,r]");
	assert_utf8_contains(&row[3], "c:5000000:single_key_ndv_containment:soft");
	assert_utf8_contains(&row[3], "s:100000:single_key_ndv_containment:soft");
	assert_utf8_contains(&row[3], "r:100000:single_key_ndv_containment:soft");
	assert_utf8_contains(&row[3], "g:5000:single_key_ndv_containment:soft");
}

#[tokio::test]
async fn hash_snowflake_root_reorders_on_robust_win() {
	let catalog = hash_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_snowflake_order_sql()))
		.expect("plan hash snowflake EXPLAIN");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &auto_broadcast_config(1)).await;
	let row = join_order_row(&rows);

	assert_eq!(row[0], ScalarValue::Utf8(Some("join_order".to_string())));
	assert_eq!(row[1], ScalarValue::Utf8(Some("star[probe=o]".to_string())));
	assert_eq!(row[2], ScalarValue::Utf8(Some("reordered".to_string())));
	assert_utf8_contains(&row[3], "strategy=hash_partitioned");
	assert_utf8_contains(&row[3], "declared_order=[c,s]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[s,c]");
	assert_utf8_contains(&row[3], "applied_order=[s,c]");
	assert_utf8_contains(&row[3], "c:5000000:single_key_ndv_containment:hard");
	assert_utf8_contains(&row[3], "s:100000:single_key_ndv_containment:hard");
	assert!(
		!rows
			.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string())) })
	);
}

#[tokio::test]
async fn hash_snowflake_reorder_is_results_identical() {
	let (catalog, storage) = snowflake_order_execution_fixture().await;
	let config = auto_broadcast_config(1);
	let (reordered_schema, reordered_rows) = execute_star_order_query(
		&catalog,
		storage.clone(),
		non_optimal_snowflake_order_project_sql(),
		&config,
	)
	.await;
	let (declared_schema, declared_rows) = execute_star_order_query(
		&catalog,
		storage,
		preferred_hash_snowflake_order_project_sql(),
		&config,
	)
	.await;

	assert_eq!(
		schema_signature(&reordered_schema),
		schema_signature(&declared_schema)
	);
	assert_eq!(reordered_rows, declared_rows);
}

#[tokio::test]
async fn hash_snowflake_branch_emits_edge_owned_star_receipt() {
	let catalog = hash_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(non_optimal_snowflake_order_sql())
		.expect("plan hash snowflake SQL");

	let (_plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(1))
			.await
			.expect("bind hash snowflake");

	assert_eq!(trace.join_decisions.len(), 4);
	assert_eq!(render_trace.join_order_edges.len(), 4);
	let JoinOrderRenderState::StarReceipts(receipts) = &render_trace.join_order else {
		panic!("expected recursive star receipts");
	};
	assert_eq!(receipts.len(), 2);

	let root_receipt = &receipts[0];
	let branch_receipt = &receipts[1];
	assert_eq!(root_receipt.probe_label, "o");
	assert_eq!(branch_receipt.probe_label, "c");
	assert_eq!(
		root_receipt
			.edge_targets
			.iter()
			.map(|target| target.join_index)
			.collect::<Vec<_>>(),
		vec![2, 3]
	);
	assert_eq!(
		branch_receipt
			.edge_targets
			.iter()
			.map(|target| target.join_index)
			.collect::<Vec<_>>(),
		vec![0, 1]
	);

	let StarOrderOutcome::Recognized(root_evidence) = &root_receipt.outcome else {
		panic!("expected root receipt");
	};
	assert_eq!(root_evidence.applied_order, vec!["s", "c"]);
	assert_eq!(root_evidence.strategy, StarOrderStrategy::HashPartitioned);

	let StarOrderOutcome::Recognized(branch_evidence) = &branch_receipt.outcome else {
		panic!("expected branch receipt");
	};
	assert_eq!(branch_evidence.declared_order, vec!["r", "g"]);
	assert_eq!(branch_evidence.applied_order, vec!["g", "r"]);
	assert_eq!(branch_evidence.strategy, StarOrderStrategy::HashPartitioned);
}

#[tokio::test]
async fn optimizer_decisions_records_per_level_for_recursive_snowflake() {
	let catalog = hash_snowflake_join_order_catalog();
	let config = auto_broadcast_config(1);
	let logical = SqlPlanner::new()
		.plan(non_optimal_snowflake_order_sql())
		.expect("plan hash snowflake");

	let _ = bind(&logical, &catalog, &config)
		.await
		.expect("bind hash snowflake");
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(&rows, "snowflake_root", "reordered", None),
		1
	);
	assert_eq!(
		optimizer_decision_count(&rows, "snowflake_branch", "reordered", None),
		1
	);
}

#[tokio::test]
async fn scratch_bind_decisions_do_not_reach_durable_lane() {
	let committed_catalog = hash_snowflake_join_order_catalog();
	let committed_config = auto_broadcast_config(1);
	let committed = SqlPlanner::new()
		.plan(non_optimal_snowflake_order_sql())
		.expect("plan committed snowflake");
	let _ = bind(&committed, &committed_catalog, &committed_config)
		.await
		.expect("bind committed snowflake");
	let committed_rows = optimizer_decision_rows(&committed_catalog, &committed_config).await;
	assert_eq!(
		optimizer_decision_count(&committed_rows, "snowflake_root", "reordered", None),
		1
	);
	assert_eq!(
		optimizer_decision_count(&committed_rows, "snowflake_branch", "reordered", None),
		1
	);

	// ADR 193: the mixed-strategy root now actuates, so the declined-root case is a non-scan-linear
	// probe (outside the actuator's authorized domain). It declines before any branch is bound, so the
	// decision log carries no snowflake_branch decisions — the same commit-discard guarantee at the
	// decision-log level that `root_decline_keeps_observed_snowflake_state` proves at the render level.
	let declined_catalog = mixed_strategy_snowflake_join_order_catalog();
	let declined_config = auto_broadcast_config(4_096);
	let declined = SqlPlanner::new()
		.plan(observed_snowflake_order_sql())
		.expect("plan declined snowflake");
	let _ = bind(&declined, &declined_catalog, &declined_config)
		.await
		.expect("bind declined snowflake");
	let declined_rows = optimizer_decision_rows(&declined_catalog, &declined_config).await;

	assert!(
		!declined_rows
			.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("snowflake_branch".to_string()))),
		"declined root must discard scratch branch decisions: {declined_rows:?}"
	);
	assert!(
		!declined_rows.iter().any(|row| row[0]
			== ScalarValue::Utf8(Some("snowflake_root".to_string()))
			&& row[1] == ScalarValue::Utf8(Some("reordered".to_string()))),
		"non-actuating snowflake commits no snowflake_root reorder: {declined_rows:?}"
	);
}

// ADR 193: the mixed-strategy snowflake root (stores broadcast, customers-composite hash) now
// ACTUATES — the selected order is authoritative. The receipt is honestly mixed: a `strategy=mixed`
// summary plus a per-edge `strategies=[...]` detail with both a broadcast and a hash edge.
#[tokio::test]
async fn mixed_strategy_snowflake_root_actuates_with_mixed_receipt() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_snowflake_order_sql()))
		.expect("plan mixed-strategy snowflake EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let mixed_row = rows
		.iter()
		.find(|row| {
			row[0] == ScalarValue::Utf8(Some("join_order".to_string()))
				&& matches!(&row[3], ScalarValue::Utf8(Some(detail)) if detail.contains("strategy=mixed"))
		})
		.expect("mixed snowflake root actuates with a strategy=mixed receipt");
	assert_utf8_contains(&mixed_row[3], "strategy=mixed");
	assert_utf8_contains(&mixed_row[3], "broadcast");
	assert_utf8_contains(&mixed_row[3], "hash_partitioned");
	// The snowflake no longer reports an observed-only verdict once its root actuates.
	assert!(
		!rows
			.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string()))),
		"actuated mixed snowflake emits no observed-only verdict"
	);
}

// ADR 192 headline: a canonical multi-child broadcast snowflake (customers is a multi-child branch
// with regions + segments) actuates a broadcast root and executes correctly. On pre-192 code the
// composite branch build's `(Broadcast, Natural)` placement fails ADR-181 uniformity, so the root
// declines (observed only). The fenced snowflake uniformity + broadcast upgrade unlock it. Execution
// correctness is locked against the hash baseline.
#[tokio::test]
async fn broadcast_snowflake_root_actuates_and_executes_distributed() {
	let (catalog, storage) = snowflake_order_execution_fixture().await;

	// Actuation: EXPLAIN shows an actuated broadcast `join_order` root receipt (not observed-only).
	let explain = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_snowflake_order_sql()))
		.expect("plan broadcast snowflake EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&explain, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let join_order_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order".to_string())))
		.collect::<Vec<_>>();
	assert_eq!(
		join_order_rows.len(),
		1,
		"broadcast snowflake root must actuate a join_order receipt"
	);
	assert_utf8_contains(&join_order_rows[0][3], "strategy=broadcast");

	// Correctness: the actuated broadcast plan executes and matches the hash baseline (cap=1).
	let sql = non_optimal_snowflake_order_project_sql();
	let (hash_schema, mut hash_rows) =
		execute_star_order_query(&catalog, storage.clone(), sql, &auto_broadcast_config(1)).await;
	let (broadcast_schema, mut broadcast_rows) = execute_star_order_query(
		&catalog,
		storage,
		sql,
		&auto_broadcast_config(64 * 1024 * 1024),
	)
	.await;
	assert_eq!(broadcast_schema, hash_schema);
	assert!(
		!broadcast_rows.is_empty(),
		"broadcast snowflake produced rows"
	);
	hash_rows.sort();
	broadcast_rows.sort();
	assert_eq!(
		broadcast_rows, hash_rows,
		"broadcast snowflake result must match the hash baseline"
	);
}

#[tokio::test]
async fn deep_snowflake_actuates_root_and_keeps_unary_branches_receiptless() {
	let catalog = hash_snowflake_join_order_catalog();
	let sql = "SELECT * FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN regions r ON c.region_id = r.id \
			   INNER JOIN segments g ON r.id = g.id \
			   INNER JOIN stores s ON o.store_id = s.id";
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {sql}"))
		.expect("plan deep snowflake EXPLAIN");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &auto_broadcast_config(1)).await;

	let join_order_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order".to_string())))
		.collect::<Vec<_>>();
	assert_eq!(join_order_rows.len(), 1);
	assert_eq!(
		join_order_rows[0][1],
		ScalarValue::Utf8(Some("star[probe=o]".to_string()))
	);
	assert_utf8_contains(&join_order_rows[0][3], "applied_order=[s,c]");
	assert!(
		!rows
			.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string())) })
	);
}

#[tokio::test]
async fn actuated_hash_snowflake_root_uses_star_verdict() {
	let catalog = hash_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(non_optimal_snowflake_order_sql())
		.expect("plan hash snowflake SQL");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(1))
		.await
		.expect("bind hash snowflake analyze");
	assert_eq!(binding.replay_ids.len(), 4);
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(10),
			probe_rows: RowCount::new(20),
			output_rows: RowCount::new(30),
		})
		.collect::<Vec<_>>();

	let plan = render_explain_analyze(binding, &observations).expect("render hash snowflake");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};

	assert_eq!(
		rows.iter()
			.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order_runtime".to_string())))
			.count(),
		4
	);
	let verdict_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order_verdict".to_string())))
		.collect::<Vec<_>>();
	assert_eq!(verdict_rows.len(), 2);
	let verdict_details = verdict_rows
		.iter()
		.map(|row| match &row[3] {
			ScalarValue::Utf8(Some(detail)) => detail.as_str(),
			_ => "",
		})
		.collect::<Vec<_>>();
	assert!(
		verdict_details
			.iter()
			.any(|detail| detail.contains("applied_order=[s,c]"))
	);
	assert!(
		verdict_details
			.iter()
			.any(|detail| detail.contains("applied_order=[g,r]"))
	);
	assert!(
		!rows
			.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string())) })
	);
	assert!(!rows.iter().any(|row| {
		row[0] == ScalarValue::Utf8(Some("join_order_snowflake_verdict".to_string()))
	}));
}

#[tokio::test]
async fn snowflake_order_evidence_is_observational() {
	let (catalog, storage) = mixed_strategy_snowflake_execution_fixture().await;
	let config = auto_broadcast_config(4_096);
	let logical = SqlPlanner::new()
		.plan(observed_snowflake_order_project_sql())
		.expect("plan snowflake SQL");

	let (plain_plan, plain_trace) = bind_with_trace(&logical, &catalog, &config)
		.await
		.expect("bind snowflake without render trace");
	let (explain_plan, explain_trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &config)
			.await
			.expect("bind snowflake with render trace");

	assert_eq!(plain_trace, explain_trace);
	assert_eq!(format!("{plain_plan:?}"), format!("{explain_plan:?}"));
	assert!(matches!(
		render_trace.join_order,
		JoinOrderRenderState::ObservedSnowflake(_)
	));

	let (_schema, rows) = execute_star_order_query(
		&catalog,
		storage,
		observed_snowflake_order_project_sql(),
		&config,
	)
	.await;
	assert_eq!(
		rows,
		vec![
			vec![Some(1), Some(10), Some(100), Some(10_000), Some(1_000)],
			vec![Some(2), Some(20), Some(200), Some(20_000), Some(2_000)],
			vec![Some(3), Some(10), Some(100), Some(10_000), Some(1_000)],
		]
	);
}

#[tokio::test]
async fn snowflake_recognized_supersedes_flat_missing_relation_decline() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", observed_snowflake_order_sql()))
		.expect("plan snowflake EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

	assert!(
		rows.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string())) })
	);
	assert!(!rows.iter().any(|row| {
		row[0] == ScalarValue::Utf8(Some("join_order".to_string()))
			&& row[2] == ScalarValue::Utf8(Some("declined_missing_relation_label".to_string()))
	}));
}

#[tokio::test]
async fn snowflake_branch_links_outer_dimension_to_inner_probe() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(observed_snowflake_order_sql())
		.expect("plan snowflake SQL");

	let (_plan, _trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind snowflake render trace");
	let JoinOrderRenderState::ObservedSnowflake(evidence) = &render_trace.join_order else {
		panic!("snowflake evidence");
	};

	assert_eq!(evidence.ranking.probe, "o");
	assert_eq!(evidence.branches.len(), 1);
	assert_eq!(evidence.branches[0].dimension, "c");
	assert_eq!(evidence.branches[0].evidence.ranking.probe, "c");
}

#[tokio::test]
async fn flat_star_emits_no_snowflake_row() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_star_order_sql()))
		.expect("plan flat star EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;

	assert!(
		rows.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order".to_string())) })
	);
	assert!(
		!rows
			.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string())) })
	);
}

// ADR 194 headline: the reachable conformed two-fact query emits galaxy evidence from the
// role-neutral substrate, additive to the snowflake-shaped tree receipt it still records.
#[tokio::test]
async fn conformed_two_fact_query_emits_galaxy_evidence() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", galaxy_conformed_order_sql()))
		.expect("plan conformed galaxy EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

	let galaxy_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("conformed two-fact query emits a join_order_galaxy row");
	assert_utf8_contains(&galaxy_row[1], "conformed=product");
	assert_utf8_contains(&galaxy_row[1], "relations=[returns,sales]");
	// ADR 198: this galaxy's cost-optimal root differs from FROM, so it actuates (outcome `reordered`).
	assert_eq!(
		galaxy_row[2],
		ScalarValue::Utf8(Some("reordered".to_string()))
	);
	assert_utf8_contains(&galaxy_row[3], "product.id");
	// Additive: the tree receipt (snowflake or actuated star) still renders alongside the galaxy row.
	assert!(rows.iter().any(|row| {
		matches!(&row[0], ScalarValue::Utf8(Some(kind))
			if kind == "join_order" || kind == "join_order_snowflake")
	}));
}

// ADR 198: the galaxy ACTUATES — the FROM order roots `sales` (sales→product = 100000), but
// `returns→product` = 5000 is cheaper, so the binder re-roots to `returns` and the EXPLAIN receipt
// shows the applied re-root (`action=reordered; applied_root=returns`).
#[tokio::test]
async fn galaxy_actuates_to_cheaper_root() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", galaxy_conformed_order_sql()))
		.expect("plan galaxy EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let galaxy_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("galaxy row");
	assert_eq!(
		galaxy_row[2],
		ScalarValue::Utf8(Some("reordered".to_string()))
	);
	assert_utf8_contains(&galaxy_row[3], "action=reordered");
	assert_utf8_contains(&galaxy_row[3], "applied_root=returns");
	assert_utf8_contains(&galaxy_row[3], "costs=[");
}

// ADR 197: the model is not a constant — when the FROM root IS the smallest fact-to-dimension join,
// it keeps the declared order.
#[tokio::test]
async fn galaxy_already_optimal_order_kept() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", galaxy_optimal_order_sql()))
		.expect("plan optimal galaxy EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let galaxy_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("galaxy row");
	assert_utf8_contains(&galaxy_row[3], "declared_root=returns");
	assert_utf8_contains(&galaxy_row[3], "preferred_root=returns");
	assert_utf8_contains(&galaxy_row[3], "action=kept_already_optimal");
}

// ADR 200: a reordering galaxy and bridge each accrue exactly one committed graph decision
// (`galaxy_root`/`bridge_root`, `reordered`) into the shared decision log — invisible on `main`. The
// re-rooted re-bind runs on the no-op recorder, so no phantom `kept_optimal` is double-recorded.
#[tokio::test]
async fn graph_actuation_accrues_committed_decision() {
	let catalog = galaxy_join_order_catalog();
	let config = auto_broadcast_config(4_096);
	let _ = bind(
		&SqlPlanner::new()
			.plan(galaxy_conformed_order_sql())
			.expect("plan galaxy"),
		&catalog,
		&config,
	)
	.await
	.expect("bind galaxy");
	let rows = optimizer_decision_rows(&catalog, &config).await;
	assert_eq!(
		optimizer_decision_count(&rows, "galaxy_root", "reordered", None),
		1
	);
	assert!(
		!rows.iter().any(
			|row| row[0] == ScalarValue::Utf8(Some("galaxy_root".to_string()))
				&& row[1] == ScalarValue::Utf8(Some("kept_optimal".to_string()))
		),
		"actuated galaxy must not double-record a phantom kept_optimal: {rows:?}"
	);

	let bridge_catalog = selective_bridge_join_order_catalog();
	let bridge_config = auto_broadcast_config(64 * 1024 * 1024);
	let _ = bind(
		&SqlPlanner::new()
			.plan(selective_bridge_order_sql())
			.expect("plan bridge"),
		&bridge_catalog,
		&bridge_config,
	)
	.await
	.expect("bind bridge");
	let bridge_rows = optimizer_decision_rows(&bridge_catalog, &bridge_config).await;
	assert_eq!(
		optimizer_decision_count(&bridge_rows, "bridge_root", "reordered", None),
		1
	);
}

// ADR 200 discriminator: an already-optimal galaxy and a clean-FK (non-selective) bridge accrue a
// *kept* decision, not reordered — proving accrual records the committed decision, not actuation-fired.
#[tokio::test]
async fn kept_graph_accrues_kept_not_reordered() {
	let catalog = galaxy_join_order_catalog();
	let config = auto_broadcast_config(4_096);
	let _ = bind(
		&SqlPlanner::new()
			.plan(galaxy_optimal_order_sql())
			.expect("plan optimal galaxy"),
		&catalog,
		&config,
	)
	.await
	.expect("bind optimal galaxy");
	let rows = optimizer_decision_rows(&catalog, &config).await;
	assert_eq!(
		optimizer_decision_count(&rows, "galaxy_root", "kept_optimal", None),
		1
	);
	assert!(
		!rows.iter().any(
			|row| row[0] == ScalarValue::Utf8(Some("galaxy_root".to_string()))
				&& row[1] == ScalarValue::Utf8(Some("reordered".to_string()))
		),
		"already-optimal galaxy must not record reordered: {rows:?}"
	);

	let bridge_catalog = bridge_join_order_catalog();
	let bridge_config = auto_broadcast_config(64 * 1024 * 1024);
	let _ = bind(
		&SqlPlanner::new()
			.plan(bridge_chain_order_sql())
			.expect("plan clean bridge"),
		&bridge_catalog,
		&bridge_config,
	)
	.await
	.expect("bind clean bridge");
	let bridge_rows = optimizer_decision_rows(&bridge_catalog, &bridge_config).await;
	assert_eq!(
		optimizer_decision_count(&bridge_rows, "bridge_root", "kept_optimal", None),
		1
	);
}

fn uniform_observations(binding: &ExplainAnalyzeBinding) -> Vec<JoinRuntimeObservation> {
	binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(3),
			output_rows: RowCount::new(3),
		})
		.collect()
}

fn descending_observations(binding: &ExplainAnalyzeBinding) -> Vec<JoinRuntimeObservation> {
	let count = u64::try_from(binding.replay_ids.len()).expect("test replay id count fits in u64");
	binding
		.replay_ids
		.iter()
		.enumerate()
		.map(|(index, replay_id)| {
			let rank = u64::try_from(index).expect("test replay id index fits in u64");
			JoinRuntimeObservation {
				replay_id: *replay_id,
				strategy: JoinStrategy::HashPartitioned,
				build_rows: RowCount::new(3),
				probe_rows: RowCount::new(3),
				output_rows: RowCount::new((count - rank) * 10),
			}
		})
		.collect()
}

fn verdict_kinds(verdicts: &[MeasuredLevelVerdict]) -> Vec<OptimizerDecisionKind> {
	verdicts.iter().map(|verdict| verdict.kind).collect()
}

fn measured_verdict_signature(
	verdicts: &[MeasuredLevelVerdict],
) -> Vec<(
	OptimizerDecisionKind,
	OptimizerRuntimeVerdictClass,
	OptimizerRuntimeVerdictProvenance,
)> {
	verdicts
		.iter()
		.map(|verdict| (verdict.kind, verdict.verdict, verdict.provenance))
		.collect()
}

fn has_tree_verdict(kinds: &[OptimizerDecisionKind]) -> bool {
	kinds.iter().any(|kind| {
		matches!(
			kind,
			OptimizerDecisionKind::SnowflakeRoot
				| OptimizerDecisionKind::SnowflakeBranch
				| OptimizerDecisionKind::FlatStar
		)
	})
}

// ADR 201 headline: under the sampled verdict path, a reordered galaxy and a reordered bridge each accrue
// their graph-root runtime verdict, and the executed re-rooted plan emits NO orphan tree verdict (its
// re-rooted tree decisions were never accrued by ADR 200).
#[tokio::test]
async fn graph_actuation_accrues_runtime_verdict() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_conformed_order_sql())
		.expect("plan galaxy");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind galaxy analyze");
	let observations = uniform_observations(&binding);
	let verdicts = measured_explain_analyze_verdicts(&binding, &observations)
		.expect("measure galaxy verdicts");
	let kinds = verdict_kinds(&verdicts);
	assert!(
		kinds.contains(&OptimizerDecisionKind::GalaxyRoot),
		"actuated galaxy accrues a galaxy_root verdict: {kinds:?}"
	);
	assert!(
		!has_tree_verdict(&kinds),
		"actuated galaxy emits no orphan tree verdict: {kinds:?}"
	);

	let bridge_catalog = selective_bridge_join_order_catalog();
	let bridge_logical = SqlPlanner::new()
		.plan(selective_bridge_order_sql())
		.expect("plan bridge");
	let bridge_binding = bind_explain_analyze(
		&bridge_logical,
		&bridge_catalog,
		&auto_broadcast_config(64 * 1024 * 1024),
	)
	.await
	.expect("bind bridge analyze");
	let bridge_observations = uniform_observations(&bridge_binding);
	let bridge_verdicts = measured_explain_analyze_verdicts(&bridge_binding, &bridge_observations)
		.expect("measure bridge verdicts");
	let bridge_kinds = verdict_kinds(&bridge_verdicts);
	assert!(
		bridge_kinds.contains(&OptimizerDecisionKind::BridgeRoot),
		"actuated bridge accrues a bridge_root verdict: {bridge_kinds:?}"
	);
	assert!(
		!has_tree_verdict(&bridge_kinds),
		"actuated bridge emits no orphan tree verdict: {bridge_kinds:?}"
	);
}

// ADR 201 discriminator: a kept (already-optimal) galaxy emits BOTH its graph verdict AND its FROM tree's
// verdict — both have matching decisions. Fails under a blanket suppression; passes only with the
// actuated-vs-kept asymmetry.
#[tokio::test]
async fn kept_graph_emits_both_graph_and_tree_verdict() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_optimal_order_sql())
		.expect("plan optimal galaxy");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind optimal galaxy analyze");
	let observations = uniform_observations(&binding);
	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure kept verdicts");
	let kinds = verdict_kinds(&verdicts);
	assert!(
		kinds.contains(&OptimizerDecisionKind::GalaxyRoot),
		"kept galaxy still accrues a graph verdict: {kinds:?}"
	);
	assert!(
		has_tree_verdict(&kinds),
		"kept galaxy's FROM tree verdict coexists: {kinds:?}"
	);
}

// ADR 201: an accrued graph verdict with no runtime measurement is Indeterminate (no verdict without
// measurement) — the accrual path shares the render classifier.
#[tokio::test]
async fn graph_runtime_verdict_indeterminate_without_measurement() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_conformed_order_sql())
		.expect("plan galaxy");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind galaxy analyze");
	// No observations → no measured costs → Indeterminate.
	let verdicts = measured_explain_analyze_verdicts(&binding, &[]).expect("measure verdicts");
	let galaxy_verdict = verdicts
		.iter()
		.find(|verdict| verdict.kind == OptimizerDecisionKind::GalaxyRoot)
		.expect("galaxy verdict present");
	assert_eq!(
		galaxy_verdict.verdict,
		OptimizerRuntimeVerdictClass::Indeterminate
	);
}

// ADR 198: after actuation the measured verdict judges the APPLIED (returns-rooted) order. With
// `returns` measured cheaper than `sales`, the applied re-root is vindicated.
#[tokio::test]
async fn galaxy_actuated_verdict_vindicates_applied_root() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_conformed_order_sql())
		.expect("plan galaxy");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind galaxy analyze");
	// After re-root: replay 0 = returns→product, replay 1 = product→sales. Make returns cheaper.
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(5),
			output_rows: if replay_id.0 == 0 {
				RowCount::new(10)
			} else {
				RowCount::new(100)
			},
		})
		.collect::<Vec<_>>();
	let plan = render_explain_analyze(binding, &observations).expect("render galaxy analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};
	let verdict = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_graph_verdict".to_string())))
		.expect("graph verdict row");
	assert_eq!(
		verdict[2],
		ScalarValue::Utf8(Some("vindicated".to_string()))
	);
}

// ADR 197: no verdict without measurement — absent runtime rows yield Indeterminate, never a
// fabricated judgment.
#[tokio::test]
async fn graph_verdict_indeterminate_on_missing_measurement() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_conformed_order_sql())
		.expect("plan galaxy");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind galaxy analyze");
	let plan = render_explain_analyze(binding, &[]).expect("render galaxy analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};
	let verdict = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_graph_verdict".to_string())))
		.expect("graph verdict row");
	assert_eq!(
		verdict[2],
		ScalarValue::Utf8(Some("indeterminate".to_string()))
	);
}

// ADR 199 discriminator: the clean FK bridge fans out (`bridge_rows 10000 > fact_rows 1000`, so the
// pre-199 endpoint-row proxy would have rerooted to `filter_first`), but with no predicate on the
// dimension both incident edges estimate to the bridge's row count (`F⋈B ≈ D⋈B ≈ 10000`), so the
// relation-rooted cost correctly KEEPS the FROM order — rerooting a clean FK bridge buys nothing. This
// fails under the old fan-out proxy and passes only with the edge-estimate cost, locking D1.
#[tokio::test]
async fn bridge_fans_out_but_dimension_not_selective_is_kept() {
	let catalog = bridge_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", bridge_chain_order_sql()))
		.expect("plan bridge EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let bridge_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge".to_string())))
		.expect("bridge row");
	assert_utf8_contains(&bridge_row[3], "action=kept_already_optimal");
	assert_utf8_contains(&bridge_row[3], "preferred_root=orders");
	assert_utf8_contains(&bridge_row[3], "costs=[orders:10000,products:10000]");

	let analyze_logical = SqlPlanner::new()
		.plan(bridge_chain_order_sql())
		.expect("plan bridge");
	let binding = bind_explain_analyze(
		&analyze_logical,
		&catalog,
		&auto_broadcast_config(64 * 1024 * 1024),
	)
	.await
	.expect("bind bridge analyze");
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(1_000),
			output_rows: RowCount::new(5_000),
		})
		.collect::<Vec<_>>();
	let plan = render_explain_analyze(binding, &observations).expect("render bridge analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};
	let verdict = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_graph_verdict".to_string())))
		.expect("graph verdict row");
	// The kept FROM order (orders root) measured cheaper than the products-root estimate → vindicated.
	assert_eq!(
		verdict[2],
		ScalarValue::Utf8(Some("vindicated".to_string()))
	);
}

// ADR 194 / D6 — the truthful boundary (was `galaxy_or_shared_dimension_declines_snowflake`): the
// synthetic multi-root edge stream is a cartesian the binder cannot emit and is a galaxy of neither
// recognizer, while the *reachable* conformed two-fact query is recognized by the role-neutral
// substrate.
#[tokio::test]
async fn role_neutral_substrate_recognizes_reachable_galaxy_not_synthetic() {
	// (a) The hand-built disjoint multi-root stream shares no conformed dimension — a cartesian, not a
	//     galaxy. Both the snowflake recognizer and the galaxy recognizer decline it.
	let synthetic = vec![
		snowflake_test_edge(0, "orders", "customers", 0, 1_000, true),
		snowflake_test_edge(1, "invoices", "regions", 1, 100, true),
	];
	let decisions = vec![
		snowflake_test_decision(
			0,
			JoinStrategy::HashPartitioned,
			JoinPhysicalPlacement::Natural,
		),
		snowflake_test_decision(
			1,
			JoinStrategy::HashPartitioned,
			JoinPhysicalPlacement::Natural,
		),
	];
	assert!(
		analyze_snowflake(&synthetic, &decisions, CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP).is_none()
	);
	assert!(analyze_galaxy(&synthetic, &decisions).is_none());

	// (b) ADR 239: the frontend now emits a multi-root cartesian via CROSS JOIN / comma joins —
	//     both lower to a keyless `JoinKind::Cross` (broadcast-only), which is a cartesian, not a
	//     conformed galaxy; the role-neutral recognizers still decline it (covered in (a)).
	assert!(
		SqlPlanner::new()
			.plan("SELECT * FROM sales s CROSS JOIN returns r")
			.is_ok()
	);
	assert!(
		SqlPlanner::new()
			.plan("SELECT * FROM sales s, returns r")
			.is_ok()
	);

	// (c) The reachable conformed two-fact query binds and the role-neutral substrate recognizes
	//     `product` as the conformed dimension shared by both facts.
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_conformed_order_sql())
		.expect("plan conformed galaxy");
	let (_plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind conformed galaxy");
	let galaxy = analyze_galaxy(&render_trace.join_order_edges, &trace.join_decisions)
		.expect("conformed query is recognized as a galaxy");
	assert_eq!(galaxy.conformed.len(), 1);
	assert_eq!(galaxy.conformed[0].label, "product");
	assert_eq!(
		galaxy.conformed[0].key_signature,
		vec!["product.id".to_string()]
	);
	assert_eq!(
		galaxy.conformed[0].incident_relations,
		vec!["returns".to_string(), "sales".to_string()]
	);
	// The edge list carries role-neutral endpoints (labels + key signatures), the substrate the
	// directed trace role alone could not.
	assert!(galaxy.edges.iter().any(|edge| edge.left_key_signature
		== vec!["product.id".to_string()]
		|| edge.right_key_signature == vec!["product.id".to_string()]));
}

// ADR 194: Rune is the primary interface — the conformed two-fact graph authored in Rune emits the
// same galaxy evidence as the SQL path.
#[tokio::test]
async fn rune_conformed_two_fact_matches_sql() {
	let catalog = galaxy_join_order_catalog();
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .auto_join(
                   ctx.table("product"),
                   qcol("sales", "product_id").eq(qcol("product", "id"))
               )
               .auto_join(
                   ctx.table("returns"),
                   qcol("returns", "product_id").eq(qcol("product", "id"))
               )
               .explain()
        }
    "#,
	)
	.await;

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let galaxy_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("Rune conformed two-fact query emits a join_order_galaxy row");
	assert_utf8_contains(&galaxy_row[1], "conformed=product");
	assert_utf8_contains(&galaxy_row[1], "relations=[returns,sales]");
}

// ADR 194 anti-poaching: a normal snowflake fans out on the branch's *own* foreign keys
// (`customers.region_id`, `customers.segment_id`), never conforming on a shared key signature — so it
// produces no galaxy row, and the tree receipts stay byte-identical.
#[tokio::test]
async fn galaxy_recognizer_declines_single_fact_snowflake_and_star() {
	for (catalog, sql) in [
		(
			mixed_strategy_snowflake_join_order_catalog(),
			non_optimal_snowflake_order_sql().to_string(),
		),
		(star_join_order_catalog(), non_optimal_star_order_sql()),
	] {
		let logical = SqlPlanner::new()
			.plan(&format!("EXPLAIN {sql}"))
			.expect("plan single-fact EXPLAIN");
		let (_schema, rows) =
			bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
		assert!(
			!rows
				.iter()
				.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string()))),
			"single-fact star/snowflake must not emit a galaxy row: {rows:?}"
		);
	}
}

// ADR 194 (F1 regression): a single fact joined to two aliases of the same dimension on its OWN
// foreign key is a duplicate-dimension star, not a galaxy. `orders.customer_id` is incident to both
// `c1` and `c2` on the same signature, so the candidate detector flags `orders` — but `orders`
// (100_000 rows) is larger than its `customers` aliases (5_000), so the dimension-like row gate
// rejects it. No galaxy row.
#[tokio::test]
async fn galaxy_recognizer_declines_single_fact_same_key_alias_star() {
	let catalog = snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT o.id, c1.id, c2.id FROM orders o \
			 INNER JOIN customers c1 ON o.customer_id = c1.id \
			 INNER JOIN customers c2 ON o.customer_id = c2.id",
		)
		.expect("plan alias-star EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	assert!(
		!rows
			.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string()))),
		"duplicate-dimension alias star must not emit a galaxy row: {rows:?}"
	);
}

// ADR 195 headline: EXPLAIN ANALYZE surfaces measured fan-out drift on the conformed galaxy graph —
// per-edge `join_order_galaxy_runtime` rows whose actual rows differ from the estimate, with a ratio
// gauge. No order verdict (galaxy has no chosen order to judge).
#[tokio::test]
async fn conformed_galaxy_runtime_surfaces_measured_fanout_drift() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_conformed_order_sql())
		.expect("plan galaxy ANALYZE");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind galaxy analyze");
	// Every galaxy edge is observed (the replay-id union gathers galaxy targets explicitly).
	assert_eq!(binding.replay_ids.len(), 2);
	// Observations whose output_rows deliberately contradict the estimate → drift.
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(7),
			probe_rows: RowCount::new(11),
			output_rows: RowCount::new(1),
		})
		.collect::<Vec<_>>();

	let plan = render_explain_analyze(binding, &observations).expect("render galaxy analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};

	let runtime_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy_runtime".to_string())))
		.collect::<Vec<_>>();
	assert_eq!(runtime_rows.len(), 2, "one runtime row per galaxy edge");
	// Drift visible: estimated (col 4) != actual (col 5) on at least one edge; actual is the seeded 1.
	let drift = runtime_rows.iter().any(|row| {
		matches!((&row[4], &row[5]),
			(ScalarValue::Int64(Some(estimated)), ScalarValue::Int64(Some(actual))) if estimated != actual)
	});
	assert!(drift, "measured fan-out drift surfaces: {runtime_rows:?}");
	assert!(
		runtime_rows
			.iter()
			.all(|row| row[5] == ScalarValue::Int64(Some(1)))
	);
	assert_utf8_contains(&runtime_rows[0][3], "ratio=");
	assert_utf8_contains(&runtime_rows[0][3], "conformed=");
	// No order verdict for galaxies in this cut.
	assert!(
		!rows
			.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy_verdict".to_string()))),
		"galaxy runtime emits no order verdict"
	);
	// Additive: the conformed query still emits its snowflake runtime rows.
	assert!(rows.iter().any(|row| {
		row[0] == ScalarValue::Utf8(Some("join_order_snowflake_runtime".to_string()))
	}));
}

// ADR 195: galaxy runtime rows correlate to galaxy edges by replay id (the `join[N]` target), and a
// single-fact query emits none — the galaxy path never pollutes a non-galaxy ANALYZE.
#[tokio::test]
async fn galaxy_runtime_keys_by_replay_id_and_declines_non_galaxy() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_conformed_order_sql())
		.expect("plan galaxy ANALYZE");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind galaxy analyze");
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(5),
			output_rows: RowCount::new(2),
		})
		.collect::<Vec<_>>();
	let plan = render_explain_analyze(binding, &observations).expect("render galaxy analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};
	for row in rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy_runtime".to_string())))
	{
		let ScalarValue::Utf8(Some(target)) = &row[1] else {
			panic!("galaxy runtime target");
		};
		assert!(
			target.starts_with("join["),
			"runtime row keyed by replay id: {target}"
		);
	}

	// A single-fact snowflake ANALYZE emits no galaxy runtime rows.
	let snow_catalog = mixed_strategy_snowflake_join_order_catalog();
	let snow_logical = SqlPlanner::new()
		.plan(observed_snowflake_order_sql())
		.expect("plan snowflake ANALYZE");
	let snow_binding =
		bind_explain_analyze(&snow_logical, &snow_catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind snowflake analyze");
	let snow_observations = snow_binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(5),
			output_rows: RowCount::new(2),
		})
		.collect::<Vec<_>>();
	let snow_plan =
		render_explain_analyze(snow_binding, &snow_observations).expect("render snowflake analyze");
	let ExecutionPlan::SystemTable {
		rows: snow_rows, ..
	} = snow_plan
	else {
		panic!("expected analyze system table");
	};
	assert!(
		!snow_rows.iter().any(|row| {
			matches!(&row[0], ScalarValue::Utf8(Some(kind)) if kind.starts_with("join_order_galaxy"))
		}),
		"non-galaxy ANALYZE emits no galaxy rows"
	);
}

// ADR 196 headline: a many-to-many bridge emits a `join_order_bridge` plan row, and under
// EXPLAIN ANALYZE a `join_order_bridge_runtime` row whose measured fan-out ratio (output/probe) > 1 —
// the M:N explosion made real. Additive: the snowflake tree receipt still renders.
#[tokio::test]
async fn bridge_evidence_measures_many_to_many_fanout() {
	let catalog = bridge_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", bridge_chain_order_sql()))
		.expect("plan bridge EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let bridge_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge".to_string())))
		.expect("bridge plan row");
	assert_utf8_contains(&bridge_row[1], "orders->order_items->products");
	assert_utf8_contains(&bridge_row[3], "fan_out=");
	// Additive: a tree receipt (snowflake or actuated star) renders alongside the bridge row.
	assert!(rows.iter().any(|row| {
		matches!(&row[0], ScalarValue::Utf8(Some(kind))
			if kind == "join_order" || kind == "join_order_snowflake")
	}));

	let analyze_logical = SqlPlanner::new()
		.plan(bridge_chain_order_sql())
		.expect("plan bridge");
	let binding = bind_explain_analyze(
		&analyze_logical,
		&catalog,
		&auto_broadcast_config(64 * 1024 * 1024),
	)
	.await
	.expect("bind bridge analyze");
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(1_000),
			output_rows: RowCount::new(2_500),
		})
		.collect::<Vec<_>>();
	let plan = render_explain_analyze(binding, &observations).expect("render bridge analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};
	let runtime_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge_runtime".to_string())))
		.collect::<Vec<_>>();
	assert!(!runtime_rows.is_empty(), "bridge runtime rows present");
	assert!(
		runtime_rows
			.iter()
			.any(|row| matches!(&row[6], ScalarValue::Float64(Some(ratio)) if *ratio > 1.0)),
		"measured many-to-many fan-out > 1: {runtime_rows:?}"
	);
}

// ADR 196 / the load-bearing partner: cardinality, not structure, decides. The bridge fixture
// (junction NOT dimension-like) is recognized; the structurally identical snowflake branch
// (dimension-like intermediary) is not.
#[tokio::test]
async fn bridge_recognizer_distinguishes_bridge_from_snowflake_branch() {
	let bridge_catalog = bridge_join_order_catalog();
	let bridge_logical = SqlPlanner::new()
		.plan(bridge_chain_order_sql())
		.expect("plan bridge");
	let (_p, bridge_trace, bridge_rt) = bind_with_explain_trace(
		&bridge_logical,
		&bridge_catalog,
		&auto_broadcast_config(64 * 1024 * 1024),
	)
	.await
	.expect("bind bridge");
	let bridge = analyze_bridge(&bridge_rt.join_order_edges, &bridge_trace.join_decisions)
		.expect("junction not dimension-like → bridge");
	assert_eq!(bridge.links.len(), 1);
	assert_eq!(bridge.links[0].bridge, "order_items");
	assert_eq!(bridge.links[0].fact, "orders");
	assert_eq!(bridge.links[0].dim, "products");

	let branch_catalog = snowflake_join_order_catalog();
	let branch_logical = SqlPlanner::new()
		.plan("SELECT o.id, c.id, r.id FROM orders o INNER JOIN customers c ON o.customer_id = c.id INNER JOIN regions r ON c.region_id = r.id")
		.expect("plan branch");
	let (_p2, branch_trace, branch_rt) = bind_with_explain_trace(
		&branch_logical,
		&branch_catalog,
		&auto_broadcast_config(64 * 1024 * 1024),
	)
	.await
	.expect("bind branch");
	assert!(
		analyze_bridge(&branch_rt.join_order_edges, &branch_trace.join_decisions).is_none(),
		"dimension-like snowflake branch is not a bridge"
	);
}

// ADR 196: a conformed dimension (equal signatures) routes to galaxy, never bridge.
#[tokio::test]
async fn bridge_recognizer_declines_conformed_dimension() {
	let catalog = galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(galaxy_conformed_order_sql())
		.expect("plan galaxy");
	let (_p, trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind galaxy");
	assert!(
		analyze_bridge(&render_trace.join_order_edges, &trace.join_decisions).is_none(),
		"conformed dimension is not a bridge"
	);
}

// ADR 196: Rune is the primary interface — the bridge graph authored in Rune emits the same evidence.
#[tokio::test]
async fn rune_bridge_matches_sql() {
	let catalog = bridge_join_order_catalog();
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders")
               .auto_join(
                   ctx.table("order_items"),
                   qcol("orders", "id").eq(qcol("order_items", "order_id"))
               )
               .auto_join(
                   ctx.table("products"),
                   qcol("order_items", "product_id").eq(qcol("products", "id"))
               )
               .explain()
        }
    "#,
	)
	.await;
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let bridge_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge".to_string())))
		.expect("Rune bridge query emits a join_order_bridge row");
	assert_utf8_contains(&bridge_row[1], "orders->order_items->products");
}

// ADR 196: a degenerate junction whose fact and dimension collapse to the same relation is a cycle,
// not a bridge — the recognizer rejects it.
#[test]
fn bridge_recognizer_rejects_cyclic_joins() {
	let edges = vec![
		bridge_test_edge(
			0,
			"orders",
			"order_items",
			"orders.id",
			"order_items.order_id",
			1_000,
			10_000,
		),
		bridge_test_edge(
			1,
			"order_items",
			"orders",
			"order_items.ref_id",
			"orders.id",
			10_000,
			1_000,
		),
	];
	let decisions = vec![
		snowflake_test_decision(
			0,
			JoinStrategy::HashPartitioned,
			JoinPhysicalPlacement::Natural,
		),
		snowflake_test_decision(
			1,
			JoinStrategy::HashPartitioned,
			JoinPhysicalPlacement::Natural,
		),
	];
	assert!(
		analyze_bridge(&edges, &decisions).is_none(),
		"a fact==dim cycle is not a bridge"
	);
}

#[test]
fn snowflake_uniformity_accepts_only_authenticated_composite_natural_broadcast() {
	let decisions = vec![
		snowflake_test_decision(0, JoinStrategy::Broadcast, JoinPhysicalPlacement::Natural),
		snowflake_test_decision(
			1,
			JoinStrategy::Broadcast,
			JoinPhysicalPlacement::SwappedRightBuild,
		),
	];
	let composite_build_positions = std::collections::BTreeSet::from([0usize]);
	let no_composite_build_positions = std::collections::BTreeSet::new();

	assert_eq!(
		uniform_snowflake_order_strategy(&decisions, &composite_build_positions),
		Some(StarOrderStrategy::Broadcast)
	);
	assert_eq!(
		uniform_snowflake_order_strategy(&decisions, &no_composite_build_positions),
		None
	);
	assert_eq!(uniform_star_order_strategy(&decisions), None);
}

// ADR 193: the observed snowflake evidence still records each edge's resolved strategy/placement,
// but those strategies do not gate anything on the observe-only path — they are reported, not
// authorized against. A non-scan-linear probe forces every edge to hash (broadcast needs a
// scan-linear probe), and the evidence records that honestly with `scan_linear=false`. The broadcast
// edge in a *mixed* receipt is proven on the actuated path by
// `mixed_strategy_snowflake_root_actuates_with_mixed_receipt`.
#[tokio::test]
async fn snowflake_evidence_records_per_edge_strategy_without_gating() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", observed_snowflake_order_sql()))
		.expect("plan mixed-strategy snowflake EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let row = join_order_snowflake_row(&rows);

	assert_utf8_contains(&row[3], "c:hash_partitioned/natural");
	assert_utf8_contains(&row[3], "s:hash_partitioned/natural");
	assert_utf8_contains(&row[3], "scan_linear=false");
}

#[tokio::test]
async fn root_decline_keeps_observed_snowflake_state() {
	// ADR 193: commit-discard atomicity. The root declines actuation because its probe is
	// non-scan-linear (outside the actuator's authorized domain), not because it is mixed — mixed
	// roots now actuate. Even though the `customers -> {regions, segments}` branch is bushy and
	// could reorder locally, the root-gate throws the branch receipts away with the root and falls
	// back to the 182-184 observe-only path. The render state must be ObservedSnowflake, never a
	// committed StarReceipts tree.
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(observed_snowflake_order_sql())
		.expect("plan mixed-strategy snowflake");

	let (_plan, _trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind mixed-strategy snowflake explain trace");

	let JoinOrderRenderState::ObservedSnowflake(evidence) = &render_trace.join_order else {
		panic!("root decline must keep observed snowflake state, not commit star receipts");
	};
	// The discarded branch was bushy (>=2 children) — a would-be local actuation thrown away
	// atomically with the declining root, not committed independently.
	let bushy_branch = evidence
		.branches
		.iter()
		.find(|branch| branch.evidence.ranking.dimensions.len() >= 2)
		.expect("mixed-strategy snowflake has a bushy customers branch");
	assert_eq!(bushy_branch.dimension, "c");
}

#[tokio::test]
async fn snowflake_order_evidence_is_explain_gated() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(observed_snowflake_order_sql())
		.expect("plan snowflake SQL");

	let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind normal snowflake trace");
	let trace_debug = format!("{trace:?}");
	assert!(!trace_debug.contains("JoinOrderEdgeEvidence"));
	assert!(!trace_debug.contains("SnowflakeOrderEvidence"));

	let (_plan, _trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind snowflake explain trace");
	assert!(matches!(
		render_trace.join_order,
		JoinOrderRenderState::ObservedSnowflake(_)
	));
}

#[tokio::test]
async fn snowflake_explain_analyze_emits_plan_runtime_and_verdict() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(observed_snowflake_order_sql())
		.expect("plan snowflake SQL");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind snowflake analyze");
	assert_eq!(binding.replay_ids.len(), 4);
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(u64::from(replay_id.0) + 10),
			probe_rows: RowCount::new(u64::from(replay_id.0) + 20),
			output_rows: RowCount::new(u64::from(replay_id.0) + 30),
		})
		.collect::<Vec<_>>();

	let plan = render_explain_analyze(binding, &observations).expect("render snowflake analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};

	assert!(
		rows.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string())) })
	);
	let runtime_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order_snowflake_runtime".to_string())))
		.collect::<Vec<_>>();
	assert_eq!(runtime_rows.len(), 4);
	let verdict_rows = rows
		.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some("join_order_snowflake_verdict".to_string())))
		.collect::<Vec<_>>();
	assert_eq!(verdict_rows.len(), 3);
	assert!(
		verdict_rows
			.iter()
			.any(|row| { row[1] == ScalarValue::Utf8(Some("level=o".to_string())) })
	);
	assert!(
		verdict_rows
			.iter()
			.any(|row| { row[1] == ScalarValue::Utf8(Some("level=c".to_string())) })
	);
	assert!(
		verdict_rows
			.iter()
			.any(|row| { row[1] == ScalarValue::Utf8(Some("snowflake".to_string())) })
	);
	assert!(runtime_rows.iter().any(|row| {
		row[1] == ScalarValue::Utf8(Some("join[0] level=o dimension=c".to_string()))
	}));
	assert!(runtime_rows.iter().any(|row| {
		row[1] == ScalarValue::Utf8(Some("join[1] level=c dimension=r".to_string()))
	}));
	for row in runtime_rows {
		let ScalarValue::Utf8(Some(target)) = &row[1] else {
			panic!("runtime row target")
		};
		let close = target.find(']').expect("target carries join id");
		let replay_id = target[5..close].parse::<u32>().expect("replay id");
		assert_eq!(row[5], ScalarValue::Int64(Some(i64::from(replay_id) + 30)));
		assert_eq!(row[8], ScalarValue::Int64(Some(i64::from(replay_id) + 10)));
		assert_eq!(row[9], ScalarValue::Int64(Some(i64::from(replay_id) + 20)));
		assert_utf8_contains(&row[3], "source=single_key_ndv_containment");
	}
	assert!(
		!rows
			.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order_runtime".to_string())) })
	);
	assert!(
		!rows
			.iter()
			.any(|row| { row[0] == ScalarValue::Utf8(Some("join_order_verdict".to_string())) })
	);
}

#[tokio::test]
async fn snowflake_runtime_replay_ids_collected_from_edges() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(observed_snowflake_order_sql())
		.expect("plan snowflake SQL");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind snowflake analyze");
	let expected = binding
		.render_trace
		.join_order_edges
		.iter()
		.map(|edge| JoinReplayId(edge.target.join_index as u32))
		.collect::<BTreeSet<_>>();

	assert!(matches!(
		binding.render_trace.join_order,
		JoinOrderRenderState::ObservedSnowflake(_)
	));
	assert_eq!(binding.replay_ids, expected);
	assert_eq!(binding.replay_ids.len(), 4);

	let non_star = SqlPlanner::new()
		.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
		.expect("plan non-star SQL");
	let binding = bind_explain_analyze(&non_star, &catalog, &auto_broadcast_config(1))
		.await
		.expect("bind non-star analyze");

	assert!(binding.replay_ids.is_empty());
}

#[tokio::test]
async fn star_order_evidence_agrees_when_declared_is_optimal() {
	let catalog = star_join_order_catalog();
	let sql = "SELECT * FROM orders o \
			   INNER JOIN stores s ON o.store_id = s.id \
			   INNER JOIN products p ON o.product_id = p.id \
			   INNER JOIN customers c ON o.customer_id = c.id";
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {sql}"))
		.expect("plan optimal star EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let row = join_order_row(&rows);

	assert_eq!(
		row[2],
		ScalarValue::Utf8(Some("kept_already_optimal".to_string()))
	);
	assert_utf8_contains(&row[3], "declared_order=[s,p,c]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[s,p,c]");
	assert_utf8_contains(&row[3], "applied_order=[s,p,c]");
}

#[tokio::test]
async fn star_order_keeps_declared_when_soft_margin_is_thin() {
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_100, 1_000);
	let sql = "SELECT * FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN products p ON o.product_id = p.id";
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {sql}"))
		.expect("plan thin-margin star EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let row = join_order_row(&rows);

	assert_eq!(
		row[2],
		ScalarValue::Utf8(Some("kept_insufficient_margin".to_string()))
	);
	assert_utf8_contains(&row[3], "declared_order=[c,p]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[p,c]");
	assert_utf8_contains(&row[3], "applied_order=[c,p]");
	assert_utf8_contains(&row[3], "confidence=soft");
}

fn margin_config(step: SoftMarginLadderStep) -> PlannerConfig {
	PlannerConfig {
		join_order_soft_margin_step: step,
		..auto_broadcast_config(64 * 1024 * 1024)
	}
}

#[test]
fn operator_set_margin_changes_reorder_threshold() {
	// declared=100, preferred=85 (a 15% soft win): kept under the default 4/5 and the tightest
	// 3/5, reordered only once the operator loosens to 9/10. The configured margin is the gate.
	let declared = 100;
	let preferred = 85;
	let classify = |step: SoftMarginLadderStep| {
		classify_star_order_action(
			declared,
			preferred,
			false,
			JoinOrderEstimateConfidence::Soft,
			step.margin(),
		)
	};
	assert_eq!(
		classify(SoftMarginLadderStep::Tightest),
		StarOrderAction::KeptInsufficientMargin
	);
	assert_eq!(
		classify(SoftMarginLadderStep::Baseline),
		StarOrderAction::KeptInsufficientMargin
	);
	assert_eq!(
		classify(SoftMarginLadderStep::Loose),
		StarOrderAction::Reordered
	);
}

#[tokio::test]
async fn operator_margin_changes_bound_plan() {
	// End-to-end proof: the same query produces a different *bound* ExecutionPlan under a tight
	// vs a loose operator margin — the configured threshold actually reorders the plan, not just
	// the receipt. The thin soft win is kept under the tightest step and reordered under the
	// loosest.
	let mut catalog = star_join_order_catalog();
	// A soft win that the tightest step keeps and the loosest step reorders (a clean hash-star
	// reorder, not a strategy decline).
	seed_soft_star_join_order_facts(&mut catalog, 1_000, 850);
	let sql = "SELECT * FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN products p ON o.product_id = p.id";
	let logical = SqlPlanner::new().plan(sql).expect("plan star");

	let (tight_plan, _) = bind_with_trace(
		&logical,
		&catalog,
		&margin_config(SoftMarginLadderStep::Tightest),
	)
	.await
	.expect("bind under tightest margin");
	let (loose_plan, _) = bind_with_trace(
		&logical,
		&catalog,
		&margin_config(SoftMarginLadderStep::Loosest),
	)
	.await
	.expect("bind under loosest margin");

	assert_ne!(
		format!("{tight_plan:?}"),
		format!("{loose_plan:?}"),
		"operator margin must change the bound plan: tight keeps declared, loose reorders"
	);
}

// ADR 208: an opt-in ApplyAdvisory config seeded with verdicts that drive the tree-scoped ADR 189
// advisory. `Manual` ignores the verdicts (base step is effective); `ApplyAdvisory` resolves the tree
// step from `recommend_soft_margin(...).recommended_step` once at bind entry.
fn auto_applied_margin_config(
	base_step: SoftMarginLadderStep,
	verdicts: &[(OptimizerDecisionKind, OptimizerRuntimeVerdictClass, u64)],
) -> PlannerConfig {
	let config = PlannerConfig {
		join_order_soft_margin_step: base_step,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	for (kind, verdict, count) in verdicts {
		record_verdicts(config.runtime_evidence.as_ref(), *kind, *verdict, *count);
	}
	config
}

// A clean "loosen" signal from the ADR 189 policy: pure MissedWin at the min sample loosens one
// rung from any base step.
const LOOSEN_VERDICTS: &[(OptimizerDecisionKind, OptimizerRuntimeVerdictClass, u64)] = &[(
	OptimizerDecisionKind::FlatStar,
	OptimizerRuntimeVerdictClass::MissedWin,
	25,
)];

const GRAPH_LOOSEN_VERDICTS: &[(OptimizerDecisionKind, OptimizerRuntimeVerdictClass, u64)] = &[(
	OptimizerDecisionKind::GalaxyRoot,
	OptimizerRuntimeVerdictClass::MissedWin,
	JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
)];

const GRAPH_DIVERGENT_VERDICTS: &[(OptimizerDecisionKind, OptimizerRuntimeVerdictClass, u64)] = &[
	(
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	),
	(
		OptimizerDecisionKind::BridgeChainRoot,
		OptimizerRuntimeVerdictClass::OverEager,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	),
];

fn preserved_outer_actionable_config(base_step: SoftMarginLadderStep) -> PlannerConfig {
	auto_applied_margin_config(
		base_step,
		&[(
			OptimizerDecisionKind::PreservedOuterOrder,
			OptimizerRuntimeVerdictClass::MissedWin,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
		)],
	)
}

#[tokio::test]
async fn auto_applied_margin_uses_advisory_recommendation() {
	// Base Baseline (4/5) + a loosen signal: the ADR 189 advisory recommends 9/10, and under
	// ApplyAdvisory that recommended step is the margin the star receipt renders. The same query,
	// same verdicts, under Manual keeps the configured base (4/5) — the application mode, not the
	// verdict log alone, is what changes the applied margin.
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_100, 1_000);
	let sql = "SELECT * FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN products p ON o.product_id = p.id";
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {sql}"))
		.expect("plan star EXPLAIN");

	let auto = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);
	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &auto).await;
	assert_utf8_contains(&join_order_row(&rows)[3], "soft_margin=9/10");

	let (_schema, manual_rows) = bind_explain_rows(
		&logical,
		&catalog,
		&margin_config(SoftMarginLadderStep::Baseline),
	)
	.await;
	assert_utf8_contains(&join_order_row(&manual_rows)[3], "soft_margin=4/5");
}

#[tokio::test]
async fn auto_application_reorders_bound_plan_one_step() {
	// End-to-end plan diff. The soft star win is ~0.92, so it only reorders at the loosest rung
	// (19/20); estimates are NDV-dominated, so no fixture in this family flips at an intermediate
	// rung (see the 190 plan-diff which also used Tightest-vs-Loosest). Base Loose + a loosen
	// signal resolves one rung up to Loosest, which reorders the bound plan; Manual at the same
	// base keeps it. ApplyAdvisory moves exactly one rung — it equals Manual(Loosest), never beyond.
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_000, 850);
	let sql = "SELECT * FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN products p ON o.product_id = p.id";
	let logical = SqlPlanner::new().plan(sql).expect("plan star");

	let auto = auto_applied_margin_config(SoftMarginLadderStep::Loose, LOOSEN_VERDICTS);
	let (auto_plan, _) = bind_with_trace(&logical, &catalog, &auto)
		.await
		.expect("bind under auto-applied margin");
	let (manual_loose, _) = bind_with_trace(
		&logical,
		&catalog,
		&margin_config(SoftMarginLadderStep::Loose),
	)
	.await
	.expect("bind under manual loose");
	let (manual_loosest, _) = bind_with_trace(
		&logical,
		&catalog,
		&margin_config(SoftMarginLadderStep::Loosest),
	)
	.await
	.expect("bind under manual loosest");

	assert_ne!(
		format!("{auto_plan:?}"),
		format!("{manual_loose:?}"),
		"auto application must move off the configured base and reorder the bound plan"
	);
	assert_eq!(
		format!("{auto_plan:?}"),
		format!("{manual_loosest:?}"),
		"auto application applies exactly the one-rung recommendation, not a double step"
	);
}

#[tokio::test]
async fn manual_application_mode_is_190_byte_identical() {
	// Manual ignores seeded verdicts: a Manual bind with a loosen-signalling log produces the same
	// bound plan as the plain ADR 190 configured-margin path with an empty log.
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_000, 850);
	let sql = "SELECT * FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN products p ON o.product_id = p.id";
	let logical = SqlPlanner::new().plan(sql).expect("plan star");

	let manual_with_verdicts = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	record_verdicts(
		manual_with_verdicts.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		25,
	);
	let (with_verdicts, _) = bind_with_trace(&logical, &catalog, &manual_with_verdicts)
		.await
		.expect("bind manual with verdicts");
	let (baseline_190, _) = bind_with_trace(
		&logical,
		&catalog,
		&margin_config(SoftMarginLadderStep::Baseline),
	)
	.await
	.expect("bind 190 baseline");

	assert_eq!(
		format!("{with_verdicts:?}"),
		format!("{baseline_190:?}"),
		"manual mode must be byte-identical to ADR 190 regardless of verdict accrual"
	);
}

#[tokio::test]
async fn advisory_report_does_not_double_step_auto_application() {
	// The report keeps the configured base and the recommendation-from-base distinct from the
	// applied step. With base 4/5 and a loosen signal: current=4/5, recommended=9/10, applied=9/10
	// (NOT 19/20 — the advisory is never recomputed from the already-applied margin).
	let catalog = star_join_order_catalog();
	let config = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);
	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;
	assert_margin_advisory_scope_order(&rows);
	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	assert_eq!(margin_advisory_utf8(tree, 0), "4/5");
	assert_eq!(margin_advisory_utf8(tree, 1), "9/10");
	assert_eq!(margin_advisory_utf8(tree, 2), "loosen");
	assert_eq!(margin_advisory_utf8(tree, 9), "apply_advisory");
	assert_eq!(margin_advisory_utf8(tree, 10), "9/10");
	assert_eq!(margin_advisory_utf8(tree, 11), "tree_join_order");
	assert_eq!(margin_advisory_i64(tree, 12), 0);
	assert_eq!(margin_advisory_utf8(tree, 13), "knob_advisory");
}

#[tokio::test]
async fn tree_row_selected_by_scope_matches_208_contract() {
	let catalog = star_join_order_catalog();
	let config = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);
	let report = margin_advisory_report(&config);
	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;
	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");

	assert_eq!(
		margin_advisory_utf8(tree, 0),
		report.advisory.current.label()
	);
	assert_eq!(
		margin_advisory_utf8(tree, 1),
		report.advisory.recommended.label()
	);
	assert_eq!(
		margin_advisory_utf8(tree, 2),
		margin_recommendation_label(report.advisory.recommendation)
	);
	assert_eq!(
		margin_advisory_i64(tree, 3),
		i64::try_from(report.advisory.missed_win).unwrap()
	);
	assert_eq!(
		margin_advisory_i64(tree, 4),
		i64::try_from(report.advisory.over_eager).unwrap()
	);
	assert_eq!(
		margin_advisory_i64(tree, 5),
		i64::try_from(report.advisory.vindicated).unwrap()
	);
	assert_eq!(
		margin_advisory_i64(tree, 6),
		i64::try_from(report.advisory.mis_ranked).unwrap()
	);
	assert_eq!(
		margin_advisory_i64(tree, 7),
		i64::try_from(report.advisory.indeterminate).unwrap()
	);
	assert_eq!(
		margin_advisory_i64(tree, 8),
		i64::try_from(report.advisory.sample_size).unwrap()
	);
	assert_eq!(
		margin_advisory_utf8(tree, 9),
		join_order_soft_margin_application_label(report.application)
	);
	assert_eq!(
		margin_advisory_utf8(tree, 10),
		report.applied_step.margin().label()
	);
	assert_eq!(margin_advisory_utf8(tree, 11), report.evidence_scope);
	assert_eq!(
		margin_advisory_i64(tree, 12),
		i64::try_from(report.excluded_sample_size).unwrap()
	);
}

#[test]
fn auto_application_insufficient_evidence_uses_configured_margin() {
	// MisRanked never enters the sample, so the advisory has insufficient evidence and the applied
	// step stays at the configured base.
	let config = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[(
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictClass::MisRanked,
			25,
		)],
	);
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Baseline
	);
	let report = margin_advisory_report(&config);
	assert_eq!(report.applied_step, SoftMarginLadderStep::Baseline);
	assert_eq!(
		report.advisory.recommendation,
		MarginRecommendation::InsufficientEvidence
	);
}

#[test]
fn auto_application_hysteresis_keep_uses_configured_margin() {
	// Net signal inside the hysteresis band recommends Keep, so the applied step stays at the base.
	let config = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[
			(
				OptimizerDecisionKind::FlatStar,
				OptimizerRuntimeVerdictClass::MissedWin,
				11,
			),
			(
				OptimizerDecisionKind::FlatStar,
				OptimizerRuntimeVerdictClass::OverEager,
				9,
			),
		],
	);
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		margin_advisory_report(&config).applied_step,
		SoftMarginLadderStep::Baseline
	);
}

#[test]
fn auto_application_tightens_on_overeager() {
	// A clean over-eager signal tightens one rung: Baseline (4/5) -> Tight (7/10).
	let config = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[(
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictClass::OverEager,
			25,
		)],
	);
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Tight
	);
	assert_eq!(
		margin_advisory_report(&config)
			.applied_step
			.margin()
			.label(),
		"7/10"
	);
}

#[test]
fn auto_application_is_bounded_to_one_step() {
	// Extreme signals still move exactly one rung from the base, never two.
	let loosen = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[(
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictClass::MissedWin,
			10_000,
		)],
	);
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&loosen),
		SoftMarginLadderStep::Loose
	);
	let tighten = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[(
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictClass::OverEager,
			10_000,
		)],
	);
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&tighten),
		SoftMarginLadderStep::Tight
	);
}

#[test]
fn tree_margin_advisory_scope_accepts_only_tree_kinds() {
	assert!(is_tree_margin_advisory_kind(
		OptimizerDecisionKind::StarCandidate
	));
	assert!(is_tree_margin_advisory_kind(
		OptimizerDecisionKind::FlatStar
	));
	assert!(is_tree_margin_advisory_kind(
		OptimizerDecisionKind::SnowflakeRoot
	));
	assert!(is_tree_margin_advisory_kind(
		OptimizerDecisionKind::SnowflakeBranch
	));
	assert!(!is_tree_margin_advisory_kind(
		OptimizerDecisionKind::GalaxyRoot
	));
	assert!(!is_tree_margin_advisory_kind(
		OptimizerDecisionKind::BridgeRoot
	));
	assert!(!is_tree_margin_advisory_kind(
		OptimizerDecisionKind::BridgeChainRoot
	));
	assert!(!is_tree_margin_advisory_kind(
		OptimizerDecisionKind::PreservedOuterOrder
	));
}

#[test]
fn graph_margin_advisory_scope_accepts_only_graph_kinds() {
	for kind in [
		OptimizerDecisionKind::StarCandidate,
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerDecisionKind::SnowflakeBranch,
	] {
		assert!(is_tree_margin_advisory_kind(kind));
		assert!(!is_graph_margin_advisory_kind(kind));
	}
	for kind in [
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionKind::BridgeChainRoot,
	] {
		assert!(!is_tree_margin_advisory_kind(kind));
		assert!(is_graph_margin_advisory_kind(kind));
	}
	assert!(!is_tree_margin_advisory_kind(
		OptimizerDecisionKind::PreservedOuterOrder
	));
	assert!(!is_graph_margin_advisory_kind(
		OptimizerDecisionKind::PreservedOuterOrder
	));
}

#[test]
fn tree_verdicts_still_drive_soft_margin_advisory() {
	let config = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);

	assert_eq!(
		effective_tree_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Loose
	);
	assert_eq!(
		margin_advisory_report(&config).advisory.recommendation,
		MarginRecommendation::Loosen
	);
}

#[test]
fn tree_and_graph_evidence_do_not_cross_steer() {
	let tree = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&tree),
		SoftMarginLadderStep::Loose
	);
	assert_eq!(
		effective_graph_join_order_soft_margin_step(&tree),
		SoftMarginLadderStep::Baseline
	);

	let graph = auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_LOOSEN_VERDICTS);
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&graph),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		effective_graph_join_order_soft_margin_step(&graph),
		SoftMarginLadderStep::Loose
	);
}

#[test]
fn per_kind_graph_margin_inert_until_gate_actionable() {
	let cold = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	for kind in [
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionKind::BridgeChainRoot,
	] {
		assert_eq!(
			effective_graph_kind_join_order_soft_margin_step(&cold, kind),
			SoftMarginLadderStep::Baseline
		);
	}

	let one_ready =
		auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_LOOSEN_VERDICTS);
	assert_eq!(
		effective_graph_join_order_soft_margin_step(&one_ready),
		SoftMarginLadderStep::Loose
	);
	for kind in [
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionKind::BridgeChainRoot,
	] {
		assert_eq!(
			effective_graph_kind_join_order_soft_margin_step(&one_ready, kind),
			SoftMarginLadderStep::Loose,
			"a non-actionable graph gate must keep every graph kind on the family step"
		);
	}

	let converged = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[
			(
				OptimizerDecisionKind::GalaxyRoot,
				OptimizerRuntimeVerdictClass::MissedWin,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			),
			(
				OptimizerDecisionKind::BridgeChainRoot,
				OptimizerRuntimeVerdictClass::MissedWin,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			),
		],
	);
	for kind in [
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionKind::BridgeChainRoot,
	] {
		assert_eq!(
			effective_graph_kind_join_order_soft_margin_step(&converged, kind),
			SoftMarginLadderStep::Loose,
			"converged ready kinds must not arm per-kind divergence"
		);
	}
}

#[test]
fn actionable_graph_divergence_resolves_kind_specific_steps() {
	let config =
		auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_DIVERGENT_VERDICTS);
	let gate =
		graph_margin_divergence_gate(&config.runtime_evidence.receipt_gate_snapshot(), &config);

	assert_eq!(gate.status, OptimizerReceiptGateStatus::Actionable);
	assert_eq!(gate.ready.len(), 2);
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		effective_graph_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		effective_graph_kind_join_order_soft_margin_step(
			&config,
			OptimizerDecisionKind::GalaxyRoot
		),
		SoftMarginLadderStep::Loose
	);
	assert_eq!(
		effective_graph_kind_join_order_soft_margin_step(
			&config,
			OptimizerDecisionKind::BridgeRoot
		),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		effective_graph_kind_join_order_soft_margin_step(
			&config,
			OptimizerDecisionKind::BridgeChainRoot,
		),
		SoftMarginLadderStep::Tight
	);
}

#[test]
fn graph_kind_without_ready_evidence_uses_family_step() {
	let config =
		auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_DIVERGENT_VERDICTS);

	assert_eq!(
		effective_graph_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		effective_graph_kind_join_order_soft_margin_step(
			&config,
			OptimizerDecisionKind::BridgeRoot
		),
		SoftMarginLadderStep::Baseline
	);
}

#[test]
fn manual_ignores_graph_kind_divergence() {
	let manual = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	for (kind, class, count) in GRAPH_DIVERGENT_VERDICTS {
		record_verdicts(manual.runtime_evidence.as_ref(), *kind, *class, *count);
	}

	assert_eq!(
		effective_tree_join_order_soft_margin_step(&manual),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		effective_graph_join_order_soft_margin_step(&manual),
		SoftMarginLadderStep::Baseline
	);
	for kind in [
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionKind::BridgeChainRoot,
	] {
		assert_eq!(
			effective_graph_kind_join_order_soft_margin_step(&manual, kind),
			SoftMarginLadderStep::Baseline
		);
	}
}

#[test]
fn graph_or_chain_verdicts_do_not_drive_soft_margin_advisory() {
	let config = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[
			(
				OptimizerDecisionKind::GalaxyRoot,
				OptimizerRuntimeVerdictClass::MissedWin,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			),
			(
				OptimizerDecisionKind::BridgeChainRoot,
				OptimizerRuntimeVerdictClass::MissedWin,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			),
		],
	);

	assert_eq!(
		effective_tree_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Baseline
	);
	let report = margin_advisory_report(&config);
	assert_eq!(
		report.advisory.recommendation,
		MarginRecommendation::InsufficientEvidence
	);
	assert_eq!(report.advisory.sample_size, 0);
	assert_eq!(
		report.excluded_sample_size,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE.saturating_mul(2)
	);
}

#[tokio::test]
async fn preserved_outer_evidence_steers_neither_family() {
	let config = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[(
			OptimizerDecisionKind::PreservedOuterOrder,
			OptimizerRuntimeVerdictClass::MissedWin,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
		)],
	);

	assert_eq!(
		effective_tree_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		effective_graph_join_order_soft_margin_step(&config),
		SoftMarginLadderStep::Baseline
	);
	let report = margin_advisory_report(&config);
	assert_eq!(
		report.advisory.recommendation,
		MarginRecommendation::InsufficientEvidence
	);
	assert_eq!(report.advisory.sample_size, 0);
	assert_eq!(
		report.excluded_sample_size,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE
	);

	let rows = optimizer_margin_advisory_rows(&MockCatalog::new(), &config).await;
	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	let graph = margin_advisory_row_by_scope(&rows, "graph_join_order");
	assert_eq!(
		margin_advisory_i64(tree, 12),
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE as i64
	);
	assert_eq!(
		margin_advisory_i64(graph, 12),
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE as i64
	);
}

#[tokio::test]
async fn graph_knob_row_renders_row_effective_application() {
	let catalog = MockCatalog::new();
	let config = auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_LOOSEN_VERDICTS);

	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;

	assert_margin_advisory_scope_order(&rows);
	let graph = margin_advisory_row_by_scope(&rows, "graph_join_order");
	assert_eq!(margin_advisory_utf8(graph, 0), "4/5");
	assert_eq!(margin_advisory_utf8(graph, 1), "9/10");
	assert_eq!(margin_advisory_utf8(graph, 2), "loosen");
	assert_eq!(
		margin_advisory_i64(graph, 8),
		i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE).unwrap()
	);
	assert_eq!(margin_advisory_utf8(graph, 9), "apply_advisory");
	assert_eq!(margin_advisory_utf8(graph, 10), "9/10");
	assert_eq!(margin_advisory_utf8(graph, 11), "graph_join_order");
	assert_eq!(margin_advisory_i64(graph, 12), 0);
	assert_eq!(margin_advisory_utf8(graph, 13), "knob_advisory");

	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	assert_eq!(
		margin_advisory_utf8(tree, 2),
		"insufficient_evidence",
		"graph verdicts must not enter the tree advisory row"
	);
	assert_eq!(margin_advisory_i64(tree, 8), 0);
	assert_eq!(
		margin_advisory_i64(tree, 12),
		i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE).unwrap()
	);
}

#[tokio::test]
async fn per_kind_detail_rows_aggregate_independently() {
	let catalog = MockCatalog::new();
	let config = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[
			(
				OptimizerDecisionKind::GalaxyRoot,
				OptimizerRuntimeVerdictClass::MissedWin,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			),
			(
				OptimizerDecisionKind::BridgeChainRoot,
				OptimizerRuntimeVerdictClass::OverEager,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			),
		],
	);

	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;

	let graph = margin_advisory_row_by_scope(&rows, "graph_join_order");
	assert_eq!(margin_advisory_utf8(graph, 2), "keep");
	assert_eq!(
		margin_advisory_i64(graph, 8),
		i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 2).unwrap()
	);

	let galaxy = margin_advisory_row_by_scope(&rows, "galaxy_root");
	assert_eq!(margin_advisory_utf8(galaxy, 2), "loosen");
	assert_eq!(margin_advisory_utf8(galaxy, 13), "kind_detail");
	assert_eq!(
		margin_advisory_i64(galaxy, 12),
		i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE).unwrap()
	);

	let bridge = margin_advisory_row_by_scope(&rows, "bridge_root");
	assert_eq!(margin_advisory_utf8(bridge, 2), "insufficient_evidence");
	assert_eq!(margin_advisory_i64(bridge, 8), 0);
	assert_eq!(
		margin_advisory_i64(bridge, 12),
		i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 2).unwrap()
	);

	let chain = margin_advisory_row_by_scope(&rows, "bridge_chain_root");
	assert_eq!(margin_advisory_utf8(chain, 2), "tighten");
	assert_eq!(margin_advisory_utf8(chain, 13), "kind_detail");
}

#[tokio::test]
async fn graph_margin_advisory_ignores_tree_verdicts() {
	let catalog = MockCatalog::new();
	let config = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);

	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;

	let graph = margin_advisory_row_by_scope(&rows, "graph_join_order");
	assert_eq!(margin_advisory_utf8(graph, 2), "insufficient_evidence");
	assert_eq!(margin_advisory_i64(graph, 8), 0);
	assert_eq!(
		margin_advisory_i64(graph, 12),
		i64::try_from(LOOSEN_VERDICTS[0].2).unwrap()
	);
	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	assert_eq!(margin_advisory_utf8(tree, 2), "loosen");
}

#[tokio::test]
async fn kind_detail_rows_are_not_actionable() {
	let catalog = MockCatalog::new();
	let config = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[(
			OptimizerDecisionKind::GalaxyRoot,
			OptimizerRuntimeVerdictClass::MissedWin,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
		)],
	);

	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;

	for scope in ["galaxy_root", "bridge_root", "bridge_chain_root"] {
		let row = margin_advisory_row_by_scope(&rows, scope);
		assert_eq!(margin_advisory_utf8(row, 9), "manual");
		assert_eq!(margin_advisory_utf8(row, 10), "4/5");
		assert_eq!(margin_advisory_utf8(row, 13), "kind_detail");
	}
	let galaxy = margin_advisory_row_by_scope(&rows, "galaxy_root");
	assert_eq!(margin_advisory_utf8(galaxy, 2), "loosen");
	assert_eq!(margin_advisory_utf8(galaxy, 1), "9/10");
}

#[tokio::test]
async fn insufficient_evidence_renders_per_scope() {
	let catalog = MockCatalog::new();
	let config = auto_applied_margin_config(
		SoftMarginLadderStep::Baseline,
		&[
			(
				OptimizerDecisionKind::FlatStar,
				OptimizerRuntimeVerdictClass::MissedWin,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			),
			(
				OptimizerDecisionKind::GalaxyRoot,
				OptimizerRuntimeVerdictClass::MissedWin,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE - 1,
			),
		],
	);

	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;

	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	assert_eq!(margin_advisory_utf8(tree, 2), "loosen");
	let graph = margin_advisory_row_by_scope(&rows, "graph_join_order");
	assert_eq!(margin_advisory_utf8(graph, 2), "insufficient_evidence");
	let galaxy = margin_advisory_row_by_scope(&rows, "galaxy_root");
	assert_eq!(margin_advisory_utf8(galaxy, 2), "insufficient_evidence");
	assert_eq!(margin_advisory_i64(galaxy, 8), 19);
}

#[tokio::test]
async fn graph_evidence_changes_near_margin_graph_decision_while_tree_evidence_does_not() {
	let catalog = margin_boundary_galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", galaxy_conformed_order_sql()))
		.expect("plan margin-boundary galaxy EXPLAIN");
	let tree_config = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);
	let graph_config =
		auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_LOOSEN_VERDICTS);

	let (_schema, tree_rows) = bind_explain_rows(&logical, &catalog, &tree_config).await;
	let tree_galaxy = tree_rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("galaxy row");
	assert_eq!(
		effective_tree_join_order_soft_margin_step(&tree_config),
		SoftMarginLadderStep::Loose
	);
	assert_eq!(
		effective_graph_join_order_soft_margin_step(&tree_config),
		SoftMarginLadderStep::Baseline
	);
	assert_eq!(
		tree_galaxy[2],
		ScalarValue::Utf8(Some("observed".to_string()))
	);
	if let ScalarValue::Utf8(Some(detail)) = &tree_galaxy[3] {
		assert!(!detail.contains("applied_root="));
	}

	assert_eq!(
		effective_graph_join_order_soft_margin_step(&graph_config),
		SoftMarginLadderStep::Loose
	);
	let (_schema, graph_rows) = bind_explain_rows(&logical, &catalog, &graph_config).await;
	let graph_galaxy = graph_rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("galaxy row");
	assert_eq!(
		graph_galaxy[2],
		ScalarValue::Utf8(Some("reordered".to_string()))
	);
	assert_utf8_contains(&graph_galaxy[3], "action=reordered");
	assert_utf8_contains(&graph_galaxy[3], "applied_root=returns");
	assert_utf8_contains(&graph_galaxy[3], "costs=[returns:85000,sales:100000]");
}

#[tokio::test]
async fn graph_kind_margin_reaches_graph_actuation_decision() {
	let catalog = margin_boundary_galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", galaxy_conformed_order_sql()))
		.expect("plan margin-boundary galaxy EXPLAIN");
	let family_only = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	let armed =
		auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_DIVERGENT_VERDICTS);

	assert_eq!(
		effective_graph_join_order_soft_margin_step(&armed),
		SoftMarginLadderStep::Baseline,
		"family graph step must stay baseline so this test proves the kind-specific seam"
	);
	assert_eq!(
		effective_graph_kind_join_order_soft_margin_step(&armed, OptimizerDecisionKind::GalaxyRoot),
		SoftMarginLadderStep::Loose
	);

	let (_schema, family_rows) = bind_explain_rows(&logical, &catalog, &family_only).await;
	let family_galaxy = family_rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("family galaxy row");
	assert_eq!(
		family_galaxy[2],
		ScalarValue::Utf8(Some("observed".to_string()))
	);

	let (_schema, armed_rows) = bind_explain_rows(&logical, &catalog, &armed).await;
	let armed_galaxy = armed_rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("armed galaxy row");
	assert_eq!(
		armed_galaxy[2],
		ScalarValue::Utf8(Some("reordered".to_string()))
	);
	assert_utf8_contains(&armed_galaxy[3], "action=reordered");
	assert_utf8_contains(&armed_galaxy[3], "applied_root=returns");
}

#[tokio::test]
async fn tree_margin_application_byte_identical_under_graph_kind_divergence() {
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_000, 850);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
			 INNER JOIN customers c ON o.customer_id = c.id \
			 INNER JOIN products p ON o.product_id = p.id",
		)
		.expect("plan star");
	let baseline = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	let graph_divergent =
		auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_DIVERGENT_VERDICTS);

	let (baseline_plan, baseline_trace) = bind_with_trace(&logical, &catalog, &baseline)
		.await
		.expect("bind baseline star");
	let (graph_plan, graph_trace) = bind_with_trace(&logical, &catalog, &graph_divergent)
		.await
		.expect("bind graph-divergent star");

	assert_eq!(format!("{baseline_plan:?}"), format!("{graph_plan:?}"));
	assert_eq!(baseline_trace.join_decisions, graph_trace.join_decisions);
}

#[tokio::test]
async fn apply_advisory_with_insufficient_graph_evidence_binds_byte_identical() {
	let catalog = margin_boundary_galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&galaxy_conformed_order_sql())
		.expect("plan margin-boundary galaxy");

	for verdicts in [
		&[(
			OptimizerDecisionKind::GalaxyRoot,
			OptimizerRuntimeVerdictClass::MissedWin,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE - 1,
		)][..],
		&[
			(
				OptimizerDecisionKind::GalaxyRoot,
				OptimizerRuntimeVerdictClass::MissedWin,
				11,
			),
			(
				OptimizerDecisionKind::GalaxyRoot,
				OptimizerRuntimeVerdictClass::OverEager,
				9,
			),
		][..],
	] {
		let manual = PlannerConfig {
			join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
			join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual,
			..auto_broadcast_config(64 * 1024 * 1024)
		};
		let auto = PlannerConfig {
			join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
			join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
			..auto_broadcast_config(64 * 1024 * 1024)
		};
		for (kind, verdict, count) in verdicts {
			record_verdicts(manual.runtime_evidence.as_ref(), *kind, *verdict, *count);
			record_verdicts(auto.runtime_evidence.as_ref(), *kind, *verdict, *count);
		}

		let (manual_plan, manual_trace) = bind_with_trace(&logical, &catalog, &manual)
			.await
			.expect("bind manual graph plan");
		let (auto_plan, auto_trace) = bind_with_trace(&logical, &catalog, &auto)
			.await
			.expect("bind auto graph plan with insufficient evidence");

		assert_eq!(
			format!("{manual_plan:?}"),
			format!("{auto_plan:?}"),
			"insufficient graph evidence must not alter graph planning: {verdicts:?}"
		);
		assert_eq!(manual_trace.join_decisions, auto_trace.join_decisions);
		assert_eq!(
			effective_graph_join_order_soft_margin_step(&auto),
			SoftMarginLadderStep::Baseline
		);
	}
}

#[tokio::test]
async fn manual_mode_byte_identical_for_both_families() {
	let catalog = margin_boundary_galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&galaxy_conformed_order_sql())
		.expect("plan margin-boundary galaxy");

	let manual = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	record_verdicts(
		manual.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_verdicts(
		manual.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_verdicts(
		manual.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeChainRoot,
		OptimizerRuntimeVerdictClass::OverEager,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	let baseline = margin_config(SoftMarginLadderStep::Baseline);

	let (manual_plan, manual_trace) = bind_with_trace(&logical, &catalog, &manual)
		.await
		.expect("bind manual graph plan");
	let (baseline_plan, baseline_trace) = bind_with_trace(&logical, &catalog, &baseline)
		.await
		.expect("bind baseline graph plan");

	assert_eq!(format!("{manual_plan:?}"), format!("{baseline_plan:?}"));
	assert_eq!(manual_trace.join_decisions, baseline_trace.join_decisions);
}

#[test]
fn auto_application_does_not_mutate_config_or_log() {
	// Resolving the effective step and building the report are pure: the configured base step is
	// unchanged and the verdict snapshot is identical before and after.
	let config = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);
	let before = config.runtime_evidence.verdict_snapshot();
	let _ = effective_tree_join_order_soft_margin_step(&config);
	let _ = margin_advisory_report(&config);
	assert_eq!(
		config.join_order_soft_margin_step,
		SoftMarginLadderStep::Baseline
	);
	let after = config.runtime_evidence.verdict_snapshot();
	let total =
		|snapshot: &[OptimizerRuntimeVerdictRow]| snapshot.iter().map(|row| row.count).sum::<u64>();
	assert_eq!(before.len(), after.len());
	assert_eq!(total(&before), total(&after));
}

#[tokio::test]
async fn observed_snowflake_uses_auto_applied_margin() {
	// EXPLAIN-only snowflake observation renders on the auto-applied step: base Baseline + a loosen
	// signal makes the observed snowflake receipt show soft_margin=9/10.
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", observed_snowflake_order_sql()))
		.expect("plan mixed-strategy snowflake EXPLAIN");

	let config = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
		..auto_broadcast_config(4_096)
	};
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		25,
	);
	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &config).await;
	let row = join_order_snowflake_row(&rows);
	assert_utf8_contains(&row[3], "soft_margin=9/10");
}

#[tokio::test]
async fn rune_respects_auto_applied_margin() {
	// Rune is the primary interface: a Rune-authored EXPLAIN sees the same auto-applied effective
	// margin (9/10) as the SQL path.
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_100, 1_000);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("EXPLAIN SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id INNER JOIN products p ON o.product_id = p.id")
        }
    "#,
	)
	.await;

	let config = auto_applied_margin_config(SoftMarginLadderStep::Baseline, LOOSEN_VERDICTS);
	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &config).await;
	assert_utf8_contains(&join_order_row(&rows)[3], "soft_margin=9/10");
}

#[tokio::test]
async fn rune_graph_kind_margin_matches_sql() {
	let catalog = margin_boundary_galaxy_join_order_catalog();
	let config =
		auto_applied_margin_config(SoftMarginLadderStep::Baseline, GRAPH_DIVERGENT_VERDICTS);
	let sql_logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", galaxy_conformed_order_sql()))
		.expect("plan SQL graph-kind margin EXPLAIN");
	let rune_source = format!(
		r#"
        pub fn main(ctx) {{
            ctx.sql("EXPLAIN {}")
        }}
    "#,
		galaxy_conformed_order_sql()
	);
	let rune_logical = rune_plan(&rune_source).await;

	let (_schema, sql_rows) = bind_explain_rows(&sql_logical, &catalog, &config).await;
	let (_schema, rune_rows) = bind_explain_rows(&rune_logical, &catalog, &config).await;
	let sql_galaxy = sql_rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("SQL galaxy row");
	let rune_galaxy = rune_rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("Rune galaxy row");

	assert_eq!(sql_galaxy[2], rune_galaxy[2]);
	assert_eq!(sql_galaxy[3], rune_galaxy[3]);
	assert_utf8_contains(&rune_galaxy[3], "applied_root=returns");
}

#[test]
fn default_margin_is_behavior_neutral() {
	// The default config step resolves to the pre-190 const margin (4/5), so default plans are
	// byte-identical (the 177-189 suites prove plan identity end-to-end).
	assert_eq!(
		PlannerConfig::default().join_order_soft_margin_step,
		CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP
	);
	assert_eq!(CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP.margin().label(), "4/5");
}

#[tokio::test]
async fn applied_margin_recorded_and_displayed() {
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_100, 1_000);
	let sql = "SELECT * FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN products p ON o.product_id = p.id";
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {sql}"))
		.expect("plan star EXPLAIN");

	let (_schema, rows) = bind_explain_rows(
		&logical,
		&catalog,
		&margin_config(SoftMarginLadderStep::Loose),
	)
	.await;
	let row = join_order_row(&rows);
	assert_utf8_contains(&row[3], "soft_margin=9/10");
}

#[tokio::test]
async fn advisory_current_is_applied_margin() {
	let catalog = star_join_order_catalog();
	let rows =
		optimizer_margin_advisory_rows(&catalog, &margin_config(SoftMarginLadderStep::Loose)).await;
	assert_margin_advisory_scope_order(&rows);
	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	assert_eq!(margin_advisory_utf8(tree, 0), "9/10");
}

#[tokio::test]
async fn observed_snowflake_uses_configured_margin() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", observed_snowflake_order_sql()))
		.expect("plan mixed-strategy snowflake EXPLAIN");

	let config = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Loose,
		..auto_broadcast_config(4_096)
	};
	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &config).await;
	let row = join_order_snowflake_row(&rows);
	assert_utf8_contains(&row[3], "soft_margin=9/10");
}

#[tokio::test]
async fn star_order_declines_missing_facts() {
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_100, 1_000);
	seed_table_optimizer_facts(&mut catalog, "products", None, Some(1_000), Vec::new());
	let sql = "SELECT * FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN products p ON o.product_id = p.id";
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {sql}"))
		.expect("plan missing-fact star EXPLAIN");

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let row = join_order_row(&rows);

	assert_eq!(
		row[2],
		ScalarValue::Utf8(Some("declined_missing_estimate".to_string()))
	);
	assert_utf8_contains(&row[3], "reason=missing_estimate");
}

#[tokio::test]
async fn star_order_evidence_estimates_match_join_estimates() {
	let catalog = star_join_order_catalog();
	let single_edge = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
			 INNER JOIN customers c ON o.customer_id = c.id",
		)
		.expect("plan first star edge");
	let scoped = Binder::new(&catalog, &auto_broadcast_config(64 * 1024 * 1024))
		.bind_plan(&single_edge)
		.await
		.expect("bind first star edge");
	assert_eq!(
		scoped.facts.estimated_row_count,
		Some(RowCount::new(5_000_000))
	);

	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", non_optimal_star_order_sql()))
		.expect("plan star EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let row = join_order_row(&rows);
	assert_utf8_contains(&row[3], "c:5000000:single_key_ndv_containment:hard");
	assert_utf8_contains(&row[3], "applied_order=[s,p,c]");
}

#[tokio::test]
async fn star_recognition_respects_scan_linear_probe_fence() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let logical = LogicalPlan::Explain {
		input: Box::new(nested_hash_probe_with_tiny_right()),
	};

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

	assert_utf8_contains(
		&join_order_row(&rows)[3],
		"reason=probe_not_scan_linear:exchange",
	);
}

#[tokio::test]
async fn optimizer_decisions_records_unclassified_candidate_declines() {
	let catalog = star_join_order_catalog();
	let config = auto_broadcast_config(4_096);
	let limited_orders = LogicalPlan::Take {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		limit: 1,
	};
	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::Join {
			left: Box::new(limited_orders),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("products"),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "product_id"),
			Expr::qualified_column(TableRef::new("products"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let _plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind non-scan-linear probe star candidate");
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(
			&rows,
			"star_candidate",
			"declined",
			Some("probe_not_scan_linear")
		),
		1
	);
	assert!(
		!rows.iter().any(|row| {
			(row[0] == ScalarValue::Utf8(Some("flat_star".to_string()))
				|| row[0] == ScalarValue::Utf8(Some("snowflake_root".to_string())))
				&& row[1] == ScalarValue::Utf8(Some("declined".to_string()))
		}),
		"pre-tree decline must not invent a flat/root classification: {rows:?}"
	);
}

#[tokio::test]
async fn manual_planning_byte_identical_with_durable_decision_telemetry() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let quiet_config = auto_broadcast_config(64 * 1024 * 1024);
	let noisy_config = auto_broadcast_config(64 * 1024 * 1024);
	noisy_config.runtime_evidence.record_decision(
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::MixedStrategyStar),
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let (quiet_plan, quiet_trace) = bind_with_trace(&logical, &catalog, &quiet_config)
		.await
		.expect("bind quiet plan");
	let (noisy_plan, noisy_trace) = bind_with_trace(&logical, &catalog, &noisy_config)
		.await
		.expect("bind noisy plan");

	assert_eq!(format!("{quiet_plan:?}"), format!("{noisy_plan:?}"));
	assert_eq!(quiet_trace, noisy_trace);
}

#[tokio::test]
async fn optimizer_decisions_durable_matrix_excludes_overflow() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	config.runtime_evidence.record_decision(
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	config.runtime_evidence.record_decision(
		OptimizerDecisionKind::PreservedOuterOrder,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(&rows, "flat_star", "reordered", None),
		1
	);
	assert_eq!(
		rows.iter()
			.filter(|row| row[0] == ScalarValue::Utf8(Some("overflow".to_string())))
			.count(),
		0
	);
	assert_eq!(
		rows.iter()
			.filter(|row| {
				row[0] == ScalarValue::Utf8(Some("preserved_outer_order".to_string()))
			})
			.count(),
		0
	);
	assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn optimizer_decisions_report_projects_like_information_schema() {
	let catalog = star_join_order_catalog();
	let config = auto_broadcast_config(64 * 1024 * 1024);
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let _ = bind(&logical, &catalog, &config)
		.await
		.expect("bind star query");

	let report = LogicalPlan::ScanNamed {
		table: TableRef::with_schema("information_schema", "optimizer_decisions"),
		projection: Some(vec!["decision_kind".to_string(), "count".to_string()]),
	};
	let plan = bind(&report, &catalog, &config)
		.await
		.expect("bind projected optimizer decisions");
	let ExecutionPlan::SystemTable { schema, rows, .. } = plan else {
		panic!("expected projected system table");
	};

	assert_eq!(field_names(&schema), vec!["decision_kind", "count"]);
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0][0], ScalarValue::Utf8(Some("flat_star".to_string())));
	assert_eq!(rows[0][1], ScalarValue::Int64(Some(1)));
}

#[tokio::test]
async fn optimizer_decisions_table_appends_provenance_only() {
	let catalog = star_join_order_catalog();
	let config = auto_broadcast_config(64 * 1024 * 1024);
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let _ = bind(&logical, &catalog, &config)
		.await
		.expect("bind star query");
	let report = LogicalPlan::ScanNamed {
		table: TableRef::with_schema("information_schema", "optimizer_decisions"),
		projection: None,
	};
	let plan = bind(&report, &catalog, &config)
		.await
		.expect("bind optimizer decisions report");
	let ExecutionPlan::SystemTable { schema, rows, .. } = plan else {
		panic!("expected optimizer decisions system table");
	};

	assert_eq!(
		field_names(&schema),
		vec![
			"decision_kind",
			"outcome",
			"decline_reason",
			"count",
			"provenance"
		]
	);
	assert_eq!(rows[0][4], ScalarValue::Utf8(Some("native".to_string())));
}

#[tokio::test]
async fn rune_queries_optimizer_decisions() {
	let catalog = star_join_order_catalog();
	let config = auto_broadcast_config(64 * 1024 * 1024);
	config.runtime_evidence.record_decision(
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_decisions")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer decisions report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer decisions system table");
	};

	assert_eq!(kind, SystemTableKind::InformationSchemaOptimizerDecisions);
	assert_eq!(
		optimizer_decision_count(&rows, "flat_star", "reordered", None),
		1
	);
}

#[tokio::test]
async fn demoted_decision_dual_writes_total_and_demoted_sibling() {
	let catalog = star_join_order_catalog();
	let config = auto_broadcast_config(64 * 1024 * 1024);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT * FROM orders o \
			 LEFT JOIN customers c ON o.customer_id = c.id \
			 INNER JOIN products p ON o.product_id = p.id \
			 INNER JOIN stores s ON o.store_id = s.id \
			 WHERE c.id IS NOT NULL",
		)
		.expect("plan demoted outer star");

	let _ = bind(&logical, &catalog, &config)
		.await
		.expect("bind demoted star query");
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count_by_provenance(&rows, "flat_star", "reordered", None, "demoted"),
		1
	);
	assert_eq!(
		optimizer_decision_count_by_provenance(&rows, "flat_star", "reordered", None, "native"),
		0
	);
}

#[tokio::test]
async fn measured_verdicts_accrue_for_sampled_queries() {
	let (catalog, storage) = star_order_execution_fixture().await;
	let catalog = Arc::new(catalog);
	let config = PlannerConfig {
		measured_accrual_sample_rate: 1,
		..auto_broadcast_config(1)
	};

	let (_schema, rows) = execute_star_order_query_with_coordinator(
		catalog.clone(),
		storage.clone(),
		non_optimal_star_order_project_sql(),
		config.clone(),
	)
	.await;
	assert!(!rows.is_empty());
	let verdict_rows = optimizer_runtime_verdict_rows(catalog.as_ref(), &config).await;
	assert_eq!(
		optimizer_runtime_verdict_count(&verdict_rows, "flat_star", "over_eager"),
		1
	);

	let (_schema, rows) = execute_star_order_query_with_coordinator(
		catalog.clone(),
		storage,
		preferred_star_order_project_sql(),
		config.clone(),
	)
	.await;
	assert!(!rows.is_empty());
	let verdict_rows = optimizer_runtime_verdict_rows(catalog.as_ref(), &config).await;
	assert_eq!(
		optimizer_runtime_verdict_count(&verdict_rows, "flat_star", "vindicated"),
		1
	);
}

#[tokio::test]
async fn measured_accrual_off_by_default_is_zero_overhead() {
	let (catalog, storage) = star_order_execution_fixture().await;
	let catalog = Arc::new(catalog);
	let config = auto_broadcast_config(1);
	let (expected_schema, expected_rows) = execute_star_order_query(
		catalog.as_ref(),
		storage.clone(),
		non_optimal_star_order_project_sql(),
		&config,
	)
	.await;
	let (actual_schema, actual_rows) = execute_star_order_query_with_coordinator(
		catalog.clone(),
		storage,
		non_optimal_star_order_project_sql(),
		config.clone(),
	)
	.await;

	assert_eq!(
		schema_signature(&actual_schema),
		schema_signature(&expected_schema)
	);
	assert_eq!(actual_rows, expected_rows);
	assert!(
		optimizer_runtime_verdict_rows(catalog.as_ref(), &config)
			.await
			.is_empty()
	);
}

#[tokio::test]
async fn sampled_query_returns_user_data_not_analyze_table() {
	let (catalog, storage) = star_order_execution_fixture().await;
	let catalog = Arc::new(catalog);
	let expected_config = auto_broadcast_config(1);
	let sampled_config = PlannerConfig {
		measured_accrual_sample_rate: 1,
		..auto_broadcast_config(1)
	};
	let (expected_schema, expected_rows) = execute_star_order_query(
		catalog.as_ref(),
		storage.clone(),
		non_optimal_star_order_project_sql(),
		&expected_config,
	)
	.await;
	let (sampled_schema, sampled_rows) = execute_star_order_query_with_coordinator(
		catalog,
		storage,
		non_optimal_star_order_project_sql(),
		sampled_config,
	)
	.await;

	assert_eq!(
		schema_signature(&sampled_schema),
		schema_signature(&expected_schema)
	);
	assert_eq!(sampled_rows, expected_rows);
	assert_eq!(
		field_names(&sampled_schema),
		vec!["id", "id", "id", "id"],
		"sampled query must return user rows, not optimizer analyze rows"
	);
}

#[tokio::test]
async fn measured_accrual_samples_every_nth_query() {
	let (catalog, storage) = star_order_execution_fixture().await;
	let catalog = Arc::new(catalog);
	let config = PlannerConfig {
		measured_accrual_sample_rate: 3,
		..auto_broadcast_config(1)
	};

	for _ in 0..4 {
		let (_schema, rows) = execute_star_order_query_with_coordinator(
			catalog.clone(),
			storage.clone(),
			non_optimal_star_order_project_sql(),
			config.clone(),
		)
		.await;
		assert!(!rows.is_empty());
	}

	let verdict_rows = optimizer_runtime_verdict_rows(catalog.as_ref(), &config).await;
	assert_eq!(
		optimizer_runtime_verdict_count(&verdict_rows, "flat_star", "over_eager"),
		1
	);
}

#[tokio::test]
async fn measured_verdict_reuses_180_184_computation() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let binding =
		bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind star analyze");
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::Broadcast,
			build_rows: RowCount::new(4),
			probe_rows: RowCount::new(4),
			output_rows: RowCount::new(4),
		})
		.collect::<Vec<_>>();

	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure verdicts");
	let plan = render_explain_analyze(binding, &observations).expect("render analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze rows");
	};
	let rendered_verdict = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_verdict".to_string())))
		.expect("rendered verdict");

	assert_eq!(verdicts.len(), 1);
	assert_eq!(verdicts[0].kind, OptimizerDecisionKind::FlatStar);
	assert_eq!(verdicts[0].verdict, OptimizerRuntimeVerdictClass::OverEager);
	assert_eq!(
		rendered_verdict[2],
		ScalarValue::Utf8(Some("over_eager".to_string()))
	);
}

#[tokio::test]
async fn outer_ordering_receipts_distinguish_demoted_inner_from_true_outer() {
	let catalog = star_join_order_catalog();
	let demoted_sql = "SELECT * FROM orders o \
		LEFT JOIN customers c ON o.customer_id = c.id \
		INNER JOIN products p ON o.product_id = p.id \
		INNER JOIN stores s ON o.store_id = s.id \
		WHERE c.id IS NOT NULL";
	let demoted = SqlPlanner::new()
		.plan(demoted_sql)
		.expect("plan demoted outer star");
	let demoted_binding =
		bind_explain_analyze(&demoted, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind demoted outer star analyze");
	let demoted_verdicts = measured_explain_analyze_verdicts(
		&demoted_binding,
		&uniform_observations(&demoted_binding),
	)
	.expect("measure demoted outer star verdicts");
	assert!(
		demoted_verdicts.iter().any(|verdict| {
			verdict.kind == OptimizerDecisionKind::FlatStar
				&& verdict.provenance == OptimizerRuntimeVerdictProvenance::Demoted
		}),
		"fully demoted outer joins keep their tree kind and carry demoted provenance: {demoted_verdicts:?}"
	);
	assert!(
		!demoted_verdicts
			.iter()
			.any(|verdict| verdict.kind == OptimizerDecisionKind::PreservedOuterOrder),
		"fully demoted joins must not enter the preserved-outer lane: {demoted_verdicts:?}"
	);

	let preserved = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan preserved outer star");
	let preserved_binding = bind_explain_analyze(
		&preserved,
		&catalog,
		&auto_broadcast_config(64 * 1024 * 1024),
	)
	.await
	.expect("bind preserved outer star analyze");
	assert_eq!(
		preserved_binding.replay_ids.len(),
		3,
		"preserved outer counterfactual targets must map back to real join replay ids"
	);
	let preserved_verdicts = measured_explain_analyze_verdicts(
		&preserved_binding,
		&descending_observations(&preserved_binding),
	)
	.expect("measure preserved outer star verdicts");
	assert!(
		preserved_verdicts.iter().any(|verdict| {
			verdict.kind == OptimizerDecisionKind::PreservedOuterOrder
				&& verdict.provenance == OptimizerRuntimeVerdictProvenance::Native
				&& verdict.verdict == OptimizerRuntimeVerdictClass::MissedWin
		}),
		"true preserved outers accrue the outer opportunity lane: {preserved_verdicts:?}"
	);
	assert!(
		!has_tree_verdict(&verdict_kinds(&preserved_verdicts)),
		"true preserved outers must not masquerade as tree verdicts: {preserved_verdicts:?}"
	);
}

#[tokio::test]
async fn rune_outer_ordering_receipts_match_sql() {
	let catalog = star_join_order_catalog();
	let sql = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan SQL preserved outer star");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id LEFT JOIN products p ON o.product_id = p.id LEFT JOIN stores s ON o.store_id = s.id")
        }
    "#,
	)
	.await;

	let (_sql_plan, _sql_trace, sql_render) =
		bind_with_explain_trace(&sql, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind SQL preserved outer trace");
	let (_rune_plan, _rune_trace, rune_render) =
		bind_with_explain_trace(&rune, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind Rune preserved outer trace");

	assert_eq!(
		sql_render.preserved_outer_order, rune_render.preserved_outer_order,
		"Rune and SQL must expose the same preserved-outer receipt"
	);

	let sql_binding =
		bind_explain_analyze(&sql, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind SQL preserved outer analyze");
	let rune_binding =
		bind_explain_analyze(&rune, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind Rune preserved outer analyze");
	assert_eq!(sql_binding.replay_ids, rune_binding.replay_ids);

	let sql_verdicts =
		measured_explain_analyze_verdicts(&sql_binding, &descending_observations(&sql_binding))
			.expect("measure SQL preserved outer verdicts");
	let rune_verdicts =
		measured_explain_analyze_verdicts(&rune_binding, &descending_observations(&rune_binding))
			.expect("measure Rune preserved outer verdicts");
	assert_eq!(
		measured_verdict_signature(&sql_verdicts),
		measured_verdict_signature(&rune_verdicts)
	);
}

#[tokio::test]
async fn safe_side_left_outer_star_preserves_rows_and_schema() {
	let (catalog, storage) = outer_star_order_execution_fixture().await;
	let declared_config = auto_broadcast_config(64 * 1024 * 1024);
	let armed_config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let (declared_schema, declared_rows) = execute_star_order_query(
		&catalog,
		storage.clone(),
		preserved_outer_star_project_sql(),
		&declared_config,
	)
	.await;
	let (armed_schema, armed_rows) = execute_star_order_query(
		&catalog,
		storage,
		preserved_outer_star_project_sql(),
		&armed_config,
	)
	.await;

	assert_eq!(
		schema_signature(&armed_schema),
		schema_signature(&declared_schema)
	);
	assert_eq!(armed_rows, declared_rows);
	assert!(
		armed_rows.iter().any(|row| row.iter().any(Option::is_none)),
		"fixture must include null-supplied misses: {armed_rows:?}"
	);
	assert!(
		armed_rows.len() > 4,
		"fixture must include multi-match dimensions, got {armed_rows:?}"
	);
}

#[tokio::test]
async fn safe_side_right_outer_star_preserves_rows_and_schema() {
	let (catalog, storage) = outer_star_order_execution_fixture().await;
	let declared_config = auto_broadcast_config(64 * 1024 * 1024);
	let armed_config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);
	let logical = preserved_outer_right_star_project_plan();

	let (declared_schema, declared_rows) =
		execute_star_order_plan(&catalog, storage.clone(), &logical, &declared_config).await;
	let (armed_schema, armed_rows) =
		execute_star_order_plan(&catalog, storage, &logical, &armed_config).await;

	assert_eq!(
		schema_signature(&armed_schema),
		schema_signature(&declared_schema)
	);
	assert_eq!(armed_rows, declared_rows);
	assert!(
		armed_rows.iter().any(|row| row.iter().any(Option::is_none)),
		"right-outer fixture must include null-supplied dimension misses: {armed_rows:?}"
	);
}

#[tokio::test]
async fn safe_side_left_outer_snowflake_preserves_rows_and_schema() {
	let (catalog, storage) = snowflake_order_execution_fixture().await;
	let declared_config = auto_broadcast_config(64 * 1024 * 1024);
	let armed_config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);
	let logical = preserved_outer_snowflake_project_plan();

	let (declared_schema, declared_rows) =
		execute_star_order_plan(&catalog, storage.clone(), &logical, &declared_config).await;
	let (armed_schema, armed_rows) =
		execute_star_order_plan(&catalog, storage, &logical, &armed_config).await;

	assert_eq!(
		schema_signature(&armed_schema),
		schema_signature(&declared_schema)
	);
	assert_eq!(armed_rows, declared_rows);
	assert!(
		armed_rows.iter().any(|row| row.iter().any(Option::is_none)),
		"snowflake fixture must include null-supplied misses: {armed_rows:?}"
	);
}

#[tokio::test]
async fn safe_side_left_outer_star_reorders_when_gate_actionable() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan preserved outer star");
	let config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let (_plan, _trace, render) = bind_with_explain_trace(&logical, &catalog, &config)
		.await
		.expect("bind actionable preserved outer star");

	assert_eq!(
		join_order_edge_dimensions(&render),
		vec!["s".to_string(), "p".to_string(), "c".to_string()]
	);
	assert!(
		render
			.join_order_edges
			.iter()
			.all(|edge| edge.join_kind == JoinKind::LeftOuter),
		"outer actuation must build real left-outer edges: {:?}",
		render.join_order_edges
	);
}

#[tokio::test]
async fn right_outer_and_snowflake_reorder_only_when_gate_actionable() {
	let baseline = auto_broadcast_config(64 * 1024 * 1024);
	let armed = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let right_catalog = star_join_order_catalog();
	let right = preserved_outer_right_star_plan();
	let (right_baseline_plan, right_baseline_trace, right_baseline_render) =
		bind_with_explain_trace(&right, &right_catalog, &baseline)
			.await
			.expect("bind cold right outer star");
	let (right_armed_plan, _right_armed_trace, right_armed_render) =
		bind_with_explain_trace(&right, &right_catalog, &armed)
			.await
			.expect("bind actionable right outer star");
	assert_eq!(
		join_order_edge_dimensions(&right_baseline_render),
		vec!["o".to_string(), "o".to_string(), "o".to_string()]
	);
	assert_eq!(
		join_order_edge_dimensions(&right_armed_render),
		vec!["s".to_string(), "p".to_string(), "c".to_string()]
	);
	assert_ne!(
		format!("{right_baseline_plan:?}"),
		format!("{right_armed_plan:?}"),
		"actionable gate must change only the armed right-outer bind"
	);
	assert!(!right_baseline_trace.join_decisions.is_empty());

	let snowflake_catalog = hash_snowflake_join_order_catalog();
	let snowflake = preserved_outer_snowflake_plan();
	let (snowflake_baseline_plan, snowflake_baseline_trace, snowflake_baseline_render) =
		bind_with_explain_trace(&snowflake, &snowflake_catalog, &baseline)
			.await
			.expect("bind cold left outer snowflake");
	let (snowflake_armed_plan, _snowflake_armed_trace, snowflake_armed_render) =
		bind_with_explain_trace(&snowflake, &snowflake_catalog, &armed)
			.await
			.expect("bind actionable left outer snowflake");
	let baseline_dimensions = join_order_edge_dimensions(&snowflake_baseline_render);
	let armed_dimensions = join_order_edge_dimensions(&snowflake_armed_render);
	assert!(
		baseline_dimensions.ends_with(&["c".to_string(), "s".to_string()]),
		"cold snowflake keeps declared top-level order after internal edges: {baseline_dimensions:?}"
	);
	assert!(
		armed_dimensions.ends_with(&["s".to_string(), "c".to_string()]),
		"actionable snowflake reorders top-level dimensions after internal edges: {armed_dimensions:?}"
	);
	assert_ne!(
		format!("{snowflake_baseline_plan:?}"),
		format!("{snowflake_armed_plan:?}"),
		"actionable gate must change only the armed snowflake bind"
	);
	assert!(!snowflake_baseline_trace.join_decisions.is_empty());
}

#[tokio::test]
async fn preserved_outer_ordering_inert_until_gate_actionable() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan preserved outer star");
	let baseline = auto_broadcast_config(64 * 1024 * 1024);
	let cases = [
		PlannerConfig {
			join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
			join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
			..auto_broadcast_config(64 * 1024 * 1024)
		},
		auto_applied_margin_config(
			SoftMarginLadderStep::Baseline,
			&[(
				OptimizerDecisionKind::PreservedOuterOrder,
				OptimizerRuntimeVerdictClass::MissedWin,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE - 1,
			)],
		),
		auto_applied_margin_config(
			SoftMarginLadderStep::Baseline,
			&[
				(
					OptimizerDecisionKind::PreservedOuterOrder,
					OptimizerRuntimeVerdictClass::MissedWin,
					11,
				),
				(
					OptimizerDecisionKind::PreservedOuterOrder,
					OptimizerRuntimeVerdictClass::OverEager,
					9,
				),
			],
		),
		auto_applied_margin_config(
			SoftMarginLadderStep::Baseline,
			&[(
				OptimizerDecisionKind::PreservedOuterOrder,
				OptimizerRuntimeVerdictClass::OverEager,
				JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			)],
		),
	];
	let (baseline_plan, baseline_trace, baseline_render) =
		bind_with_explain_trace(&logical, &catalog, &baseline)
			.await
			.expect("bind baseline preserved outer star");

	for config in cases {
		let (plan, trace, render) = bind_with_explain_trace(&logical, &catalog, &config)
			.await
			.expect("bind inert preserved outer star");
		assert_eq!(format!("{plan:?}"), format!("{baseline_plan:?}"));
		assert_eq!(trace.join_decisions, baseline_trace.join_decisions);
		assert_eq!(
			join_order_edge_dimensions(&render),
			join_order_edge_dimensions(&baseline_render)
		);
	}
}

#[tokio::test]
async fn manual_ignores_preserved_outer_ordering_for_new_shapes() {
	let cases = [
		(star_join_order_catalog(), preserved_outer_right_star_plan()),
		(
			hash_snowflake_join_order_catalog(),
			preserved_outer_snowflake_plan(),
		),
	];

	for (catalog, logical) in cases {
		let manual = PlannerConfig {
			join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
			join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual,
			..auto_broadcast_config(64 * 1024 * 1024)
		};
		record_verdicts(
			manual.runtime_evidence.as_ref(),
			OptimizerDecisionKind::PreservedOuterOrder,
			OptimizerRuntimeVerdictClass::MissedWin,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
		);
		let baseline = auto_broadcast_config(64 * 1024 * 1024);

		let (manual_plan, manual_trace, manual_render) =
			bind_with_explain_trace(&logical, &catalog, &manual)
				.await
				.expect("bind manual new outer shape");
		let (baseline_plan, baseline_trace, baseline_render) =
			bind_with_explain_trace(&logical, &catalog, &baseline)
				.await
				.expect("bind baseline new outer shape");

		assert_eq!(format!("{manual_plan:?}"), format!("{baseline_plan:?}"));
		assert_eq!(manual_trace.join_decisions, baseline_trace.join_decisions);
		assert_eq!(
			join_order_edge_dimensions(&manual_render),
			join_order_edge_dimensions(&baseline_render)
		);
	}
}

#[tokio::test]
async fn manual_ignores_preserved_outer_ordering_gate() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan preserved outer star");
	let manual = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	record_verdicts(
		manual.runtime_evidence.as_ref(),
		OptimizerDecisionKind::PreservedOuterOrder,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	let baseline = auto_broadcast_config(64 * 1024 * 1024);

	let (manual_plan, manual_trace, manual_render) =
		bind_with_explain_trace(&logical, &catalog, &manual)
			.await
			.expect("bind manual preserved outer star");
	let (baseline_plan, baseline_trace, baseline_render) =
		bind_with_explain_trace(&logical, &catalog, &baseline)
			.await
			.expect("bind baseline preserved outer star");

	assert_eq!(format!("{manual_plan:?}"), format!("{baseline_plan:?}"));
	assert_eq!(manual_trace.join_decisions, baseline_trace.join_decisions);
	assert_eq!(
		join_order_edge_dimensions(&manual_render),
		join_order_edge_dimensions(&baseline_render)
	);
}

#[tokio::test]
async fn selected_order_uses_outer_preflight_not_inner_receipt_targets() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan preserved outer star");
	let config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let (plan, _trace, render) = bind_with_explain_trace(&logical, &catalog, &config)
		.await
		.expect("bind actionable preserved outer star");

	assert_eq!(
		join_order_edge_dimensions(&render),
		vec!["s".to_string(), "p".to_string(), "c".to_string()]
	);
	assert!(
		!execution_join_kinds(&plan).contains(&JoinKind::Inner),
		"live transform must not actuate the inner counterfactual targets: {plan:?}"
	);
	assert!(
		render
			.preserved_outer_order
			.iter()
			.flat_map(|receipt| receipt.receipt.edge_targets.iter())
			.all(
				|target| render.join_order_edges[target.join_index as usize].join_kind
					== JoinKind::LeftOuter
			)
	);
}

#[tokio::test]
async fn existing_left_outer_base_leaf_stars_bind_byte_identical() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan preserved outer star");
	let config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let (plan, _trace, render) = bind_with_explain_trace(&logical, &catalog, &config)
		.await
		.expect("bind existing preserved outer star");

	assert_eq!(
		join_order_edge_dimensions(&render),
		vec!["s".to_string(), "p".to_string(), "c".to_string()]
	);
	let join_kinds = execution_join_kinds(&plan);
	assert_eq!(join_kinds.len(), 3);
	assert!(
		join_kinds
			.iter()
			.all(|kind| matches!(kind, JoinKind::LeftOuter | JoinKind::RightOuter)),
		"existing preserved-outer star must stay on outer physical joins: {join_kinds:?}"
	);
	assert_eq!(
		root_project_column_ordinals(&plan),
		vec![
			ColumnOrdinal(0),
			ColumnOrdinal(1),
			ColumnOrdinal(2),
			ColumnOrdinal(3),
			ColumnOrdinal(6),
			ColumnOrdinal(5),
			ColumnOrdinal(4)
		]
	);
}

#[tokio::test]
async fn within_subtree_reorder_stays_fenced() {
	let catalog = hash_snowflake_join_order_catalog();
	let logical = preserved_outer_deep_snowflake_plan();
	let config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let (_plan, _trace, render) = bind_with_explain_trace(&logical, &catalog, &config)
		.await
		.expect("bind deep preserved outer snowflake");
	let dimensions = join_order_edge_dimensions(&render);
	let region_pos = dimensions
		.iter()
		.position(|dimension| dimension == "r")
		.expect("region edge rendered");
	let segment_pos = dimensions
		.iter()
		.position(|dimension| dimension == "g")
		.expect("segment edge rendered");
	assert!(
		region_pos < segment_pos,
		"opaque dimension subtree must keep declared internal order: {dimensions:?}"
	);
	assert!(
		dimensions.ends_with(&["s".to_string(), "c".to_string()]),
		"top-level dimensions may reorder, but subtree internals stay opaque: {dimensions:?}"
	);
}

#[tokio::test]
async fn full_outer_and_cross_into_subtree_predicate_shapes_stay_fenced() {
	let catalog = star_join_order_catalog();
	let config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);
	let baseline = auto_broadcast_config(64 * 1024 * 1024);
	for sql in [
		"SELECT * FROM orders o \
		 FULL OUTER JOIN customers c ON o.customer_id = c.id \
		 FULL OUTER JOIN products p ON o.product_id = p.id",
		"SELECT * FROM orders o \
		 LEFT JOIN customers c ON o.customer_id = c.id \
		 LEFT JOIN products p ON c.id = p.id",
	] {
		let logical = SqlPlanner::new().plan(sql).expect("plan fenced outer SQL");
		let (plan, trace) = bind_with_trace(&logical, &catalog, &config)
			.await
			.expect("bind armed fenced outer SQL");
		let (baseline_plan, baseline_trace) = bind_with_trace(&logical, &catalog, &baseline)
			.await
			.expect("bind baseline fenced outer SQL");
		assert_eq!(format!("{plan:?}"), format!("{baseline_plan:?}"), "{sql}");
		assert_eq!(trace.join_decisions, baseline_trace.join_decisions, "{sql}");
	}

	let snowflake_catalog = hash_snowflake_join_order_catalog();
	for logical in [
		preserved_outer_cross_into_subtree_plan(),
		right_outer_opaque_snowflake_plan(),
	] {
		let (plan, trace) = bind_with_trace(&logical, &snowflake_catalog, &config)
			.await
			.expect("bind armed fenced snowflake SQL");
		let (baseline_plan, baseline_trace) =
			bind_with_trace(&logical, &snowflake_catalog, &baseline)
				.await
				.expect("bind baseline fenced snowflake SQL");
		assert_eq!(
			format!("{plan:?}"),
			format!("{baseline_plan:?}"),
			"{logical:?}"
		);
		assert_eq!(
			trace.join_decisions, baseline_trace.join_decisions,
			"{logical:?}"
		);
	}

	let residual = projected_dimension_preserved_outer_plan();
	let (plan, trace) = bind_with_trace(&residual, &catalog, &config)
		.await
		.expect("bind armed projected-dimension outer plan");
	let (baseline_plan, baseline_trace) = bind_with_trace(&residual, &catalog, &baseline)
		.await
		.expect("bind baseline projected-dimension outer plan");
	assert_eq!(format!("{plan:?}"), format!("{baseline_plan:?}"));
	assert_eq!(trace.join_decisions, baseline_trace.join_decisions);
}

#[tokio::test]
async fn rune_safe_side_outer_surface_matches_sql() {
	let catalog = star_join_order_catalog();
	let sql = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan SQL preserved outer star");
	let rune = rune_plan(&format!(
		r#"
        pub fn main(ctx) {{
            ctx.sql({:?})
        }}
    "#,
		preserved_outer_star_sql()
	))
	.await;
	let config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let (sql_bound, sql_trace, sql_render) = bind_with_explain_trace(&sql, &catalog, &config)
		.await
		.expect("bind SQL outer surface");
	let (rune_bound, rune_trace, rune_render) = bind_with_explain_trace(&rune, &catalog, &config)
		.await
		.expect("bind Rune outer surface");

	assert_eq!(format!("{sql_bound:?}"), format!("{rune_bound:?}"));
	assert_eq!(sql_trace.join_decisions, rune_trace.join_decisions);
	assert_eq!(
		join_order_edge_dimensions(&sql_render),
		join_order_edge_dimensions(&rune_render)
	);
}

#[tokio::test]
async fn preserved_outer_actuator_and_receipt_table_share_gate_result() {
	let catalog = star_join_order_catalog();
	let config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);
	let snapshot = config.runtime_evidence.receipt_gate_snapshot();
	let gate = preserved_outer_ordering_gate(&snapshot, &config);
	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let row = receipt_gate_row_by_name(&rows, "preserved_outer_ordering");

	assert_eq!(gate.status, OptimizerReceiptGateStatus::Actionable);
	assert_eq!(receipt_gate_utf8(row, 2), "actionable");
	assert_eq!(
		receipt_gate_i64(row, 3),
		i64::try_from(gate.advisory.sample_size).unwrap()
	);
	assert_eq!(
		effective_join_order_policy(&config).preserved_outer_gate,
		gate
	);
}

#[tokio::test]
async fn preserved_outer_policy_uses_gate_recommended_step() {
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 140_000, 100_000);
	let logical = SqlPlanner::new()
		.plan(preserved_outer_two_dim_sql())
		.expect("plan margin-boundary preserved outer star");
	let baseline = PlannerConfig {
		join_order_soft_margin_step: SoftMarginLadderStep::Baseline,
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	let armed = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let (_baseline_plan, _baseline_trace, baseline_render) =
		bind_with_explain_trace(&logical, &catalog, &baseline)
			.await
			.expect("bind baseline boundary preserved outer");
	let (_armed_plan, _armed_trace, armed_render) =
		bind_with_explain_trace(&logical, &catalog, &armed)
			.await
			.expect("bind armed boundary preserved outer");

	assert_eq!(
		effective_join_order_policy(&armed)
			.preserved_outer_gate
			.advisory
			.recommended_step,
		SoftMarginLadderStep::Loose
	);
	assert_eq!(
		join_order_edge_dimensions(&baseline_render),
		vec!["c".to_string(), "p".to_string()]
	);
	assert_eq!(
		join_order_edge_dimensions(&armed_render),
		vec!["p".to_string(), "c".to_string()],
		"using the base Baseline margin would keep declared; the gate recommended Loose step reorders"
	);
}

#[tokio::test]
async fn rune_preserved_outer_ordering_matches_sql() {
	let catalog = star_join_order_catalog();
	let sql = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan SQL preserved outer star");
	let rune = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id LEFT JOIN products p ON o.product_id = p.id LEFT JOIN stores s ON o.store_id = s.id")
        }
    "#,
	)
	.await;
	let config = preserved_outer_actionable_config(SoftMarginLadderStep::Baseline);

	let (sql_plan, sql_trace, sql_render) = bind_with_explain_trace(&sql, &catalog, &config)
		.await
		.expect("bind SQL actionable preserved outer");
	let (rune_plan, rune_trace, rune_render) = bind_with_explain_trace(&rune, &catalog, &config)
		.await
		.expect("bind Rune actionable preserved outer");

	assert_eq!(format!("{sql_plan:?}"), format!("{rune_plan:?}"));
	assert_eq!(sql_trace.join_decisions, rune_trace.join_decisions);
	assert_eq!(
		join_order_edge_dimensions(&sql_render),
		join_order_edge_dimensions(&rune_render)
	);
}

#[tokio::test]
async fn counterfactual_bind_is_scratch_and_byte_identical() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan preserved outer star");
	let config = auto_broadcast_config(64 * 1024 * 1024);
	let before_decisions = config.runtime_evidence.decision_snapshot();

	let (plain_plan, plain_trace) = bind_with_trace(&logical, &catalog, &config)
		.await
		.expect("bind preserved outer without render trace");
	let (explain_plan, explain_trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &config)
			.await
			.expect("bind preserved outer with render trace");

	assert_eq!(plain_trace, explain_trace);
	assert_eq!(format!("{plain_plan:?}"), format!("{explain_plan:?}"));
	assert_eq!(
		before_decisions,
		config.runtime_evidence.decision_snapshot()
	);
	assert_eq!(
		render_trace.preserved_outer_order.len(),
		1,
		"the scratch counterfactual receipt exists without mutating the live bind"
	);
}

#[tokio::test]
async fn mixed_binding_provenance_is_binding_level_and_documented() {
	let catalog = hash_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(mixed_demoted_snowflake_order_sql())
		.expect("plan mixed demoted snowflake");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(1))
		.await
		.expect("bind mixed demoted snowflake analyze");
	let verdicts = measured_explain_analyze_verdicts(&binding, &uniform_observations(&binding))
		.expect("measure mixed demoted snowflake verdicts");

	assert!(
		verdicts
			.iter()
			.any(|verdict| verdict.kind == OptimizerDecisionKind::SnowflakeRoot),
		"fixture must include the root star verdict: {verdicts:?}"
	);
	assert!(
		verdicts
			.iter()
			.any(|verdict| verdict.kind == OptimizerDecisionKind::SnowflakeBranch),
		"fixture must include the demoted branch verdict: {verdicts:?}"
	);
	assert!(
		verdicts
			.iter()
			.all(|verdict| verdict.provenance == OptimizerRuntimeVerdictProvenance::Demoted),
		"ADR214 D3 is binding-level: one demotion marks all measured verdicts in the binding: {verdicts:?}"
	);
}

#[tokio::test]
async fn preserved_outer_verdicts_are_opportunity_classes_only() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(preserved_outer_star_sql())
		.expect("plan preserved outer star");
	let binding =
		bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind preserved outer star analyze");
	let verdicts = measured_explain_analyze_verdicts(&binding, &descending_observations(&binding))
		.expect("measure preserved outer verdicts");
	let preserved = verdicts
		.iter()
		.find(|verdict| verdict.kind == OptimizerDecisionKind::PreservedOuterOrder)
		.expect("preserved outer verdict");

	assert_eq!(preserved.verdict, OptimizerRuntimeVerdictClass::MissedWin);
	assert!(
		verdicts
			.iter()
			.filter(|verdict| verdict.kind == OptimizerDecisionKind::PreservedOuterOrder)
			.all(|verdict| matches!(
				verdict.verdict,
				OptimizerRuntimeVerdictClass::MissedWin
					| OptimizerRuntimeVerdictClass::Vindicated
					| OptimizerRuntimeVerdictClass::Indeterminate
			)),
		"preserved outer verdicts expose opportunity classes only: {verdicts:?}"
	);
}

#[tokio::test]
async fn measured_verdicts_record_per_level_for_recursive_snowflake() {
	let catalog = hash_snowflake_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(non_optimal_snowflake_order_sql())
		.expect("plan hash snowflake");
	let binding = bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(1))
		.await
		.expect("bind hash snowflake analyze");
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(10),
			probe_rows: RowCount::new(20),
			output_rows: RowCount::new(30),
		})
		.collect::<Vec<_>>();

	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure verdicts");

	assert_eq!(
		verdicts
			.iter()
			.map(|verdict| verdict.kind)
			.collect::<Vec<_>>(),
		vec![
			OptimizerDecisionKind::SnowflakeRoot,
			OptimizerDecisionKind::SnowflakeBranch
		]
	);
}

#[tokio::test]
async fn sampled_runtime_verdicts_record_per_level_for_recursive_snowflake() {
	let (catalog, storage) = snowflake_order_execution_fixture().await;
	let catalog = Arc::new(catalog);
	let config = PlannerConfig {
		measured_accrual_sample_rate: 1,
		..auto_broadcast_config(1)
	};

	let (_schema, rows) = execute_star_order_query_with_coordinator(
		catalog.clone(),
		storage,
		non_optimal_snowflake_order_project_sql(),
		config.clone(),
	)
	.await;
	assert!(!rows.is_empty());
	let verdict_rows = optimizer_runtime_verdict_rows(catalog.as_ref(), &config).await;

	assert_eq!(
		optimizer_runtime_verdict_kind_count(&verdict_rows, "snowflake_root"),
		1
	);
	assert_eq!(
		optimizer_runtime_verdict_kind_count(&verdict_rows, "snowflake_branch"),
		1
	);
}

#[tokio::test]
async fn optimizer_runtime_verdicts_report_aggregates_by_kind_and_class() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	config.runtime_evidence.record_verdict(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	config.runtime_evidence.record_verdict(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_runtime_verdict_rows(&catalog, &config).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(
		optimizer_runtime_verdict_count(&rows, "flat_star", "missed_win"),
		2
	);
}

#[tokio::test]
async fn optimizer_runtime_verdicts_split_native_and_demoted_provenance() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	config.runtime_evidence.record_verdict(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	config.runtime_evidence.record_verdict(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		OptimizerRuntimeVerdictProvenance::Demoted,
	);

	let rows = optimizer_runtime_verdict_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_runtime_verdict_count_by_provenance(&rows, "flat_star", "missed_win", "native",),
		1
	);
	assert_eq!(
		optimizer_runtime_verdict_count_by_provenance(&rows, "flat_star", "missed_win", "demoted",),
		1
	);
}

#[tokio::test]
async fn rune_queries_optimizer_runtime_verdicts() {
	let catalog = star_join_order_catalog();
	let config = PlannerConfig::default();
	config.runtime_evidence.record_verdict(
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerRuntimeVerdictClass::Vindicated,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_runtime_verdicts")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer runtime verdict report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer runtime verdict system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerRuntimeVerdicts
	);
	assert_eq!(
		optimizer_runtime_verdict_count(&rows, "snowflake_root", "vindicated"),
		1
	);
}

#[tokio::test]
async fn optimizer_evidence_status_reports_local_pending() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	config.runtime_evidence.record_verdict(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_evidence_status_rows(&catalog, &config).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0][0],
		ScalarValue::Utf8(Some("local_only".to_string()))
	);
	assert_eq!(rows[0][1], ScalarValue::Utf8(Some("ok".to_string())));
	assert_eq!(rows[0][2], ScalarValue::Int64(Some(2)));
	assert_eq!(rows[0][3], ScalarValue::Int64(Some(0)));
	assert_eq!(rows[0][4], ScalarValue::Int64(Some(0)));
	assert_eq!(rows[0][5], ScalarValue::Int64(Some(0)));
	assert_eq!(rows[0][6], ScalarValue::Utf8(None));
}

#[tokio::test]
async fn optimizer_evidence_status_query_is_read_only() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();

	let _ = optimizer_evidence_status_rows(&catalog, &config).await;

	assert!(config.runtime_evidence.decision_snapshot().is_empty());
	assert!(config.runtime_evidence.verdict_snapshot().is_empty());
}

#[tokio::test]
async fn rune_queries_optimizer_evidence_status() {
	let catalog = star_join_order_catalog();
	let config = PlannerConfig::default();
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_evidence_status")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer evidence status report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer evidence status system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerEvidenceStatus
	);
	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0][0],
		ScalarValue::Utf8(Some("local_only".to_string()))
	);
}

#[test]
fn margin_advisory_recommends_looser_on_missed_wins() {
	let snapshot = verdict_snapshot(&[(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	)]);

	let advisory = recommend_soft_margin(&snapshot, CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP);

	assert_eq!(advisory.recommendation, MarginRecommendation::Loosen);
	assert_eq!(advisory.current.label(), "4/5");
	assert_eq!(advisory.recommended.label(), "9/10");
	assert_eq!(advisory.sample_size, JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE);
}

#[test]
fn margin_advisory_recommends_tighter_on_overeager() {
	let snapshot = verdict_snapshot(&[(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::OverEager,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	)]);

	let advisory = recommend_soft_margin(&snapshot, CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP);

	assert_eq!(advisory.recommendation, MarginRecommendation::Tighten);
	assert_eq!(advisory.current.label(), "4/5");
	assert_eq!(advisory.recommended.label(), "7/10");
}

#[test]
fn margin_advisory_keeps_when_well_calibrated() {
	let snapshot = verdict_snapshot(&[
		(
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictClass::MissedWin,
			10,
		),
		(
			OptimizerDecisionKind::SnowflakeRoot,
			OptimizerRuntimeVerdictClass::OverEager,
			10,
		),
		(
			OptimizerDecisionKind::SnowflakeBranch,
			OptimizerRuntimeVerdictClass::Vindicated,
			20,
		),
	]);

	let advisory = recommend_soft_margin(&snapshot, CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP);

	assert_eq!(advisory.recommendation, MarginRecommendation::Keep);
	assert_eq!(advisory.current.label(), "4/5");
	assert_eq!(advisory.recommended.label(), "4/5");
	assert_eq!(advisory.sample_size, 40);
}

#[test]
fn margin_advisory_insufficient_evidence_below_min_sample() {
	let snapshot = verdict_snapshot(&[(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE - 1,
	)]);

	let advisory = recommend_soft_margin(&snapshot, CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP);

	assert_eq!(
		advisory.recommendation,
		MarginRecommendation::InsufficientEvidence
	);
	assert_eq!(advisory.current.label(), "4/5");
	assert_eq!(advisory.recommended.label(), "4/5");
}

#[tokio::test]
async fn margin_advisory_excludes_misranked_and_indeterminate() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MisRanked,
		25,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerRuntimeVerdictClass::Indeterminate,
		30,
	);

	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;

	assert_margin_advisory_scope_order(&rows);
	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	assert_eq!(margin_advisory_utf8(tree, 2), "insufficient_evidence");
	assert_eq!(margin_advisory_i64(tree, 6), 25);
	assert_eq!(margin_advisory_i64(tree, 7), 30);
	assert_eq!(margin_advisory_i64(tree, 8), 0);
}

#[test]
fn margin_advisory_clamps_to_safe_range() {
	let missed = verdict_snapshot(&[(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	)]);
	let over_eager = verdict_snapshot(&[(
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::OverEager,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	)]);

	let loosest = recommend_soft_margin(&missed, SoftMarginLadderStep::Loosest);
	let tightest = recommend_soft_margin(&over_eager, SoftMarginLadderStep::Tightest);

	assert_eq!(loosest.recommendation, MarginRecommendation::Loosen);
	assert_eq!(loosest.current.label(), "19/20");
	assert_eq!(loosest.recommended.label(), "19/20");
	assert_eq!(tightest.recommendation, MarginRecommendation::Tighten);
	assert_eq!(tightest.current.label(), "3/5");
	assert_eq!(tightest.recommended.label(), "3/5");
}

#[test]
fn margin_advisory_has_hysteresis() {
	let snapshot = verdict_snapshot(&[
		(
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictClass::MissedWin,
			11,
		),
		(
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictClass::OverEager,
			9,
		),
	]);

	let advisory = recommend_soft_margin(&snapshot, CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP);

	assert_eq!(advisory.recommendation, MarginRecommendation::Keep);
	assert_eq!(advisory.current.label(), "4/5");
	assert_eq!(advisory.recommended.label(), "4/5");
}

#[tokio::test]
async fn margin_advisory_is_read_only_no_plan_change() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let quiet_config = auto_broadcast_config(64 * 1024 * 1024);
	let noisy_config = PlannerConfig {
		decision_log: Arc::new(OptimizerDecisionLog::default()),
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	record_verdicts(
		noisy_config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let (quiet_plan, quiet_trace) = bind_with_trace(&logical, &catalog, &quiet_config)
		.await
		.expect("bind quiet plan");
	let (noisy_plan, noisy_trace) = bind_with_trace(&logical, &catalog, &noisy_config)
		.await
		.expect("bind noisy plan");

	assert_eq!(format!("{quiet_plan:?}"), format!("{noisy_plan:?}"));
	assert_eq!(quiet_trace.join_decisions, noisy_trace.join_decisions);
	assert_eq!(
		quiet_trace.scan_filter_decisions,
		noisy_trace.scan_filter_decisions
	);
}

#[tokio::test]
async fn margin_advisory_report_projects_like_information_schema() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let report = LogicalPlan::ScanNamed {
		table: TableRef::with_schema("information_schema", "optimizer_margin_advisory"),
		projection: Some(vec![
			"recommendation".to_string(),
			"recommended_margin".to_string(),
			"sample_size".to_string(),
			"evidence_scope".to_string(),
			"excluded_sample_size".to_string(),
		]),
	};
	let plan = bind(&report, &catalog, &config)
		.await
		.expect("bind projected optimizer margin advisory");
	let ExecutionPlan::SystemTable { kind, schema, rows } = plan else {
		panic!("expected projected system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerMarginAdvisory
	);
	assert_eq!(
		field_names(&schema),
		vec![
			"recommendation",
			"recommended_margin",
			"sample_size",
			"evidence_scope",
			"excluded_sample_size"
		]
	);
	assert_eq!(rows.len(), 5);
	let tree = rows
		.iter()
		.find(|row| row[3] == ScalarValue::Utf8(Some("tree_join_order".to_string())))
		.expect("tree margin advisory row");
	assert_eq!(tree[0], ScalarValue::Utf8(Some("loosen".to_string())));
	assert_eq!(tree[1], ScalarValue::Utf8(Some("9/10".to_string())));
	assert_eq!(
		tree[2],
		ScalarValue::Int64(Some(
			i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE).unwrap()
		))
	);
	assert_eq!(
		tree[3],
		ScalarValue::Utf8(Some("tree_join_order".to_string()))
	);
	assert_eq!(tree[4], ScalarValue::Int64(Some(0)));
}

#[tokio::test]
async fn optimizer_margin_advisory_reports_scope_and_exclusions() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		7,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictClass::OverEager,
		5,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeChainRoot,
		OptimizerRuntimeVerdictClass::MisRanked,
		13,
	);

	let rows = optimizer_margin_advisory_rows(&catalog, &config).await;

	assert_margin_advisory_scope_order(&rows);
	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	assert_eq!(margin_advisory_utf8(tree, 2), "loosen");
	assert_eq!(
		margin_advisory_i64(tree, 8),
		i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE).unwrap()
	);
	assert_eq!(margin_advisory_utf8(tree, 11), "tree_join_order");
	assert_eq!(margin_advisory_i64(tree, 12), 12);
	assert_eq!(margin_advisory_utf8(tree, 13), "knob_advisory");

	let graph = margin_advisory_row_by_scope(&rows, "graph_join_order");
	assert_eq!(margin_advisory_utf8(graph, 2), "insufficient_evidence");
	assert_eq!(margin_advisory_i64(graph, 3), 7);
	assert_eq!(margin_advisory_i64(graph, 4), 5);
	assert_eq!(margin_advisory_i64(graph, 6), 13);
	assert_eq!(margin_advisory_i64(graph, 8), 12);
	assert_eq!(margin_advisory_i64(graph, 12), 20);
	assert_eq!(margin_advisory_utf8(graph, 13), "knob_advisory");

	let galaxy = margin_advisory_row_by_scope(&rows, "galaxy_root");
	assert_eq!(margin_advisory_i64(galaxy, 3), 7);
	assert_eq!(margin_advisory_i64(galaxy, 12), 25);
	assert_eq!(margin_advisory_utf8(galaxy, 13), "kind_detail");
}

#[tokio::test]
async fn optimizer_receipt_gates_projects_six_static_rows() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;

	assert_receipt_gate_order(&rows);
	assert_eq!(rows.len(), 6);

	let projected = LogicalPlan::ScanNamed {
		table: TableRef::with_schema("information_schema", "optimizer_receipt_gates"),
		projection: Some(vec![
			"gate_name".to_string(),
			"status".to_string(),
			"next_cut".to_string(),
		]),
	};
	let plan = bind(&projected, &catalog, &config)
		.await
		.expect("bind projected optimizer receipt gates");
	let ExecutionPlan::SystemTable { kind, schema, rows } = plan else {
		panic!("expected projected optimizer receipt gates system table");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerReceiptGates
	);
	assert_eq!(
		field_names(&schema),
		vec!["gate_name", "status", "next_cut"]
	);
	assert_eq!(rows.len(), 6);
}

#[tokio::test]
async fn receipt_gates_render_insufficient_evidence_on_cold_ledger() {
	let rows =
		optimizer_receipt_gate_table_rows(&MockCatalog::new(), &PlannerConfig::default()).await;

	let graph = receipt_gate_row_by_name(&rows, "graph_margin_divergence");
	assert_eq!(receipt_gate_utf8(graph, 2), "insufficient_evidence");
	assert_eq!(receipt_gate_i64(graph, 3), 0);
	assert_eq!(
		receipt_gate_utf8(graph, 6),
		"requires_two_ready_graph_kind_rows"
	);

	let preserved = receipt_gate_row_by_name(&rows, "preserved_outer_ordering");
	assert_eq!(receipt_gate_utf8(preserved, 2), "insufficient_evidence");
	assert_eq!(receipt_gate_i64(preserved, 3), 0);

	let conflict = receipt_gate_row_by_name(&rows, "decision_conflict_pressure");
	assert_eq!(receipt_gate_utf8(conflict, 2), "insufficient_evidence");
	assert_eq!(receipt_gate_utf8(conflict, 4), "no_decision_rows");

	assert_eq!(
		receipt_gate_utf8(
			receipt_gate_row_by_name(&rows, "windowing_decay_pressure"),
			2
		),
		"insufficient_evidence"
	);
	assert_eq!(
		receipt_gate_utf8(
			receipt_gate_row_by_name(&rows, "windowing_decay_pressure"),
			6
		),
		"requires_lifetime_evidence_floor"
	);
	assert_eq!(
		receipt_gate_utf8(
			receipt_gate_row_by_name(&rows, "counter_handoff_readiness"),
			2
		),
		"blocked_external_signal"
	);

	let probe = receipt_gate_row_by_name(&rows, "probe_linearity_fence_dominance");
	assert_eq!(receipt_gate_utf8(probe, 1), "probe_linearity");
	assert_eq!(receipt_gate_utf8(probe, 2), "insufficient_evidence");
	assert_eq!(receipt_gate_i64(probe, 3), 0);
	assert_eq!(
		receipt_gate_utf8(probe, 4),
		"no_dominant_probe_linearity_fence"
	);
	assert_eq!(
		receipt_gate_utf8(probe, 5),
		"strongest_consumer_recent_total=0,evidence_health=local_only/ok"
	);
	assert_eq!(
		receipt_gate_utf8(probe, 6),
		"requires_recent_probe_linearity_floor"
	);
}

#[tokio::test]
async fn graph_kind_divergence_row_requires_two_ready_distinct_recommendations() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeChainRoot,
		OptimizerRuntimeVerdictClass::OverEager,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let graph = receipt_gate_row_by_name(&rows, "graph_margin_divergence");

	assert_eq!(receipt_gate_utf8(graph, 2), "actionable");
	assert_eq!(
		receipt_gate_i64(graph, 3),
		i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 2).unwrap()
	);
	assert_eq!(receipt_gate_utf8(graph, 4), "ready_kind_count=2");
	assert!(receipt_gate_utf8(graph, 5).contains("galaxy_root=loosen:9/10"));
	assert!(receipt_gate_utf8(graph, 5).contains("bridge_chain_root=tighten:7/10"));
}

#[tokio::test]
async fn preserved_outer_missed_win_row_is_separate_from_margin_families() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::PreservedOuterOrder,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let receipt_rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let preserved = receipt_gate_row_by_name(&receipt_rows, "preserved_outer_ordering");
	assert_eq!(
		receipt_gate_utf8(preserved, 1),
		"preserved_outer_join_order"
	);
	assert_eq!(receipt_gate_utf8(preserved, 2), "actionable");
	assert_eq!(receipt_gate_utf8(preserved, 4), "recommendation=loosen");
	assert_eq!(
		receipt_gate_i64(preserved, 3),
		i64::try_from(JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE).unwrap()
	);

	let advisory_rows = optimizer_margin_advisory_rows(&catalog, &config).await;
	assert_eq!(
		margin_advisory_utf8(
			margin_advisory_row_by_scope(&advisory_rows, "tree_join_order"),
			2
		),
		"insufficient_evidence"
	);
	assert_eq!(
		margin_advisory_utf8(
			margin_advisory_row_by_scope(&advisory_rows, "graph_join_order"),
			2
		),
		"insufficient_evidence"
	);
}

#[tokio::test]
async fn decision_conflict_pressure_uses_durable_decision_rows() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE / 2,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::MissingEstimate),
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE / 2,
	);

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let conflict = receipt_gate_row_by_name(&rows, "decision_conflict_pressure");

	assert_eq!(receipt_gate_utf8(conflict, 2), "actionable");
	assert_eq!(receipt_gate_i64(conflict, 3), 40);
	assert_eq!(
		receipt_gate_utf8(conflict, 4),
		"kind=flat_star,provenance=native"
	);
	assert!(receipt_gate_utf8(conflict, 5).contains("reordered=20,blocked_or_near_miss=20"));
}

#[tokio::test]
async fn optimizer_decision_conflicts_projects_static_14_rows() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	let rows = optimizer_decision_conflict_table_rows(&catalog, &config).await;

	assert_decision_conflict_order(&rows);
	assert_eq!(rows.len(), 14);
	assert!(
		!rows
			.iter()
			.any(|row| decision_conflict_utf8(row, 0) == "preserved_outer_order")
	);
	assert!(
		rows.iter()
			.all(|row| decision_conflict_utf8(row, 2) == "cold")
	);
	assert!(rows.iter().all(|row| {
		(3..=11)
			.filter(|index| *index != 10)
			.all(|index| decision_conflict_i64(row, index) == 0)
	}));

	let projected = LogicalPlan::ScanNamed {
		table: TableRef::with_schema("information_schema", "optimizer_decision_conflicts"),
		projection: Some(vec![
			"decision_kind".to_string(),
			"provenance".to_string(),
			"status".to_string(),
			"next_cut".to_string(),
		]),
	};
	let plan = bind(&projected, &catalog, &config)
		.await
		.expect("bind projected optimizer decision conflicts");
	let ExecutionPlan::SystemTable { kind, schema, rows } = plan else {
		panic!("expected projected optimizer decision conflicts system table");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerDecisionConflicts
	);
	assert_eq!(
		field_names(&schema),
		vec!["decision_kind", "provenance", "status", "next_cut"]
	);
	assert_eq!(rows.len(), 14);
}

#[tokio::test]
async fn decision_conflict_detail_is_interpretation_not_decision_count_reprojection() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_decision_conflicts")
		.expect("plan optimizer decision conflicts");
	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind optimizer decision conflicts");
	let ExecutionPlan::SystemTable { schema, rows, .. } = plan else {
		panic!("expected optimizer decision conflicts system table");
	};
	assert_eq!(
		field_names(&schema),
		vec![
			"decision_kind",
			"provenance",
			"status",
			"reordered_count",
			"kept_optimal_count",
			"kept_thin_margin_count",
			"declined_count",
			"blocked_or_near_miss_count",
			"total_count",
			"conflict_strength",
			"top_decline_reason",
			"top_decline_count",
			"evidence_health",
			"reason",
			"next_cut",
		]
	);
	assert_eq!(rows[0].len(), 15);
}

#[tokio::test]
async fn decision_conflict_detail_marks_conflicted_only_when_both_sides_ready() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::StarCandidate,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::StarCandidate,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE - 1,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_decision_conflict_table_rows(&catalog, &config).await;
	let one_sided = decision_conflict_row(&rows, "star_candidate", "native");
	assert_eq!(decision_conflict_utf8(one_sided, 2), "one_sided_reordered");
	assert_eq!(decision_conflict_i64(one_sided, 7), 19);
	assert_eq!(decision_conflict_i64(one_sided, 9), 19);

	let conflicted = decision_conflict_row(&rows, "flat_star", "native");
	assert_eq!(decision_conflict_utf8(conflicted, 2), "conflicted");
	assert_eq!(decision_conflict_i64(conflicted, 3), 20);
	assert_eq!(decision_conflict_i64(conflicted, 7), 20);
	assert_eq!(decision_conflict_i64(conflicted, 9), 20);
}

#[tokio::test]
async fn decision_conflict_keptoptimal_is_neutral_not_blocked() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::KeptOptimal,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_decision_conflict_table_rows(&catalog, &config).await;
	let row = decision_conflict_row(&rows, "flat_star", "native");
	assert_eq!(decision_conflict_utf8(row, 2), "one_sided_reordered");
	assert_eq!(decision_conflict_i64(row, 4), 20);
	assert_eq!(decision_conflict_i64(row, 7), 0);
	assert_eq!(decision_conflict_i64(row, 9), 0);
}

#[tokio::test]
async fn decision_conflict_detail_preserves_top_decline_reason() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Demoted,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::MissingEstimate),
		OptimizerRuntimeVerdictProvenance::Demoted,
		7,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::DuplicateDimension),
		OptimizerRuntimeVerdictProvenance::Demoted,
		11,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Demoted,
		2,
	);

	let rows = optimizer_decision_conflict_table_rows(&catalog, &config).await;
	let row = decision_conflict_row(&rows, "snowflake_root", "demoted");
	assert_eq!(decision_conflict_utf8(row, 2), "conflicted");
	assert_eq!(decision_conflict_i64(row, 6), 18);
	assert_eq!(decision_conflict_i64(row, 7), 20);
	assert_eq!(
		decision_conflict_nullable_utf8(row, 10),
		Some("duplicate_dimension")
	);
	assert_eq!(decision_conflict_i64(row, 11), 11);
}

#[tokio::test]
async fn decision_conflict_gate_and_detail_share_strongest_report() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		20,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		20,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Demoted,
		30,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Demoted,
		25,
	);

	let detail_rows = optimizer_decision_conflict_table_rows(&catalog, &config).await;
	let strongest = decision_conflict_row(&detail_rows, "bridge_root", "demoted");
	assert_eq!(decision_conflict_utf8(strongest, 2), "conflicted");
	assert_eq!(decision_conflict_i64(strongest, 9), 25);

	let gate_rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let conflict = receipt_gate_row_by_name(&gate_rows, "decision_conflict_pressure");
	assert_eq!(receipt_gate_utf8(conflict, 2), "actionable");
	assert_eq!(receipt_gate_i64(conflict, 3), 55);
	assert_eq!(
		receipt_gate_utf8(conflict, 4),
		"kind=bridge_root,provenance=demoted"
	);
	assert!(receipt_gate_utf8(conflict, 5).contains("reordered=30,blocked_or_near_miss=25"));
}

#[tokio::test]
async fn decision_conflict_detail_ignores_verdict_rows() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictClass::OverEager,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_decision_conflict_table_rows(&catalog, &config).await;
	assert!(
		rows.iter()
			.all(|row| decision_conflict_utf8(row, 2) == "cold")
	);
	assert!(rows.iter().all(|row| decision_conflict_i64(row, 8) == 0));
}

#[tokio::test]
async fn preserved_outer_order_absent_from_decision_conflict_detail() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::PreservedOuterOrder,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_decision_conflict_table_rows(&catalog, &config).await;
	assert_eq!(rows.len(), 14);
	assert!(
		!rows
			.iter()
			.any(|row| decision_conflict_utf8(row, 0) == "preserved_outer_order")
	);
	assert!(
		rows.iter()
			.all(|row| decision_conflict_utf8(row, 2) == "cold")
	);
}

#[tokio::test]
async fn decision_conflict_readout_is_read_only_no_plan_change() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let quiet_config = auto_broadcast_config(64 * 1024 * 1024);
	let noisy_config = PlannerConfig {
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	record_decisions(
		noisy_config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		noisy_config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	let before = noisy_config.runtime_evidence.receipt_gate_snapshot();
	let _ = optimizer_decision_conflict_table_rows(&catalog, &noisy_config).await;
	let after = noisy_config.runtime_evidence.receipt_gate_snapshot();
	assert_eq!(before, after);

	let (quiet_plan, quiet_trace) = bind_with_trace(&logical, &catalog, &quiet_config)
		.await
		.expect("bind quiet plan");
	let (noisy_plan, noisy_trace) = bind_with_trace(&logical, &catalog, &noisy_config)
		.await
		.expect("bind noisy plan");

	assert_eq!(format!("{quiet_plan:?}"), format!("{noisy_plan:?}"));
	assert_eq!(quiet_trace.join_decisions, noisy_trace.join_decisions);
	assert_eq!(
		quiet_trace.scan_filter_decisions,
		noisy_trace.scan_filter_decisions
	);
}

#[tokio::test]
async fn rune_queries_optimizer_decision_conflicts() {
	let catalog = star_join_order_catalog();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	let sql_rows = optimizer_decision_conflict_table_rows(&catalog, &config).await;
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_decision_conflicts")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer decision conflict report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer decision conflicts system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerDecisionConflicts
	);
	assert_eq!(rows, sql_rows);
}

#[tokio::test]
async fn optimizer_memo_admission_projects_static_14_rows() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;

	assert_memo_admission_order(&rows);
	assert_eq!(rows.len(), 14);
	assert!(
		!rows
			.iter()
			.any(|row| memo_admission_utf8(row, 0) == "preserved_outer_order")
	);
	assert!(rows.iter().all(|row| memo_admission_utf8(row, 2) == "cold"));
	assert!(rows.iter().all(|row| memo_admission_i64(row, 7) == 0));
	assert!(
		rows.iter()
			.all(|row| memo_admission_utf8(row, 11) == "not_conflicted")
	);
	assert!(rows.iter().all(|row| {
		[12, 13, 14, 16]
			.into_iter()
			.all(|index| memo_admission_i64(row, index) == 0)
	}));
	assert!(
		rows.iter()
			.all(|row| memo_admission_utf8(row, 15).is_empty())
	);
	assert!(
		rows.iter()
			.all(|row| memo_admission_utf8(row, 10) == "decision_conflict_pressure")
	);

	let projected = LogicalPlan::ScanNamed {
		table: TableRef::with_schema("information_schema", "optimizer_memo_admission"),
		projection: Some(vec![
			"decision_kind".to_string(),
			"provenance".to_string(),
			"admission_status".to_string(),
			"next_cut".to_string(),
		]),
	};
	let plan = bind(&projected, &catalog, &config)
		.await
		.expect("bind projected optimizer memo admission");
	let ExecutionPlan::SystemTable { kind, schema, rows } = plan else {
		panic!("expected projected optimizer memo admission system table");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerMemoAdmission
	);
	assert_eq!(
		field_names(&schema),
		vec![
			"decision_kind",
			"provenance",
			"admission_status",
			"next_cut"
		]
	);
	assert_eq!(rows.len(), 14);
}

#[tokio::test]
async fn memo_admission_cold_when_conflict_not_conflicted() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "cold");
	assert_eq!(
		memo_admission_utf8(row, 9),
		"requires_conflicted_decision_row"
	);
	assert_eq!(memo_admission_utf8(row, 10), "decision_conflict_pressure");
}

#[tokio::test]
async fn memo_admission_stale_when_conflict_recent_window_missing() {
	let catalog = MockCatalog::new();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	runtime_evidence.set_window_id_for_test(12);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "stale_evidence");
	assert_eq!(memo_admission_i64(row, 3), 20);
	assert_eq!(memo_admission_i64(row, 4), 0);
	assert_eq!(memo_admission_i64(row, 5), 40);
	assert_eq!(memo_admission_i64(row, 6), 40);
	assert_eq!(
		memo_admission_utf8(row, 9),
		"requires_recent_conflicted_decision_row"
	);
	assert_eq!(memo_admission_utf8(row, 10), "per_group_window_recency");
}

#[tokio::test]
async fn memo_admission_stale_when_group_conflict_is_outside_recent_window() {
	let catalog = MockCatalog::new();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	runtime_evidence.set_window_id_for_test(12);
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let gate_rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let windowing = receipt_gate_row_by_name(&gate_rows, "windowing_decay_pressure");
	assert_eq!(receipt_gate_utf8(windowing, 2), "inactive");
	assert_eq!(
		receipt_gate_utf8(windowing, 4),
		"recent_window=40,stale_lifetime=40,lifetime_total=80"
	);
	assert_eq!(
		receipt_gate_utf8(windowing, 6),
		"recent_window_matches_lifetime"
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let stale = memo_admission_row(&rows, "flat_star", "native");
	assert_eq!(memo_admission_utf8(stale, 2), "stale_evidence");
	assert_eq!(memo_admission_i64(stale, 4), 0);
	assert_eq!(memo_admission_i64(stale, 5), 40);
	assert_eq!(memo_admission_i64(stale, 6), 40);

	let fresh = memo_admission_row(&rows, "bridge_root", "native");
	assert_eq!(memo_admission_utf8(fresh, 2), "blocked_shape");
	assert_eq!(memo_admission_i64(fresh, 4), 40);
	assert_eq!(memo_admission_i64(fresh, 5), 0);
	assert_eq!(memo_admission_i64(fresh, 6), 40);
}

#[tokio::test]
async fn memo_admission_group_recency_requires_recent_conflict_not_one_sided_activity() {
	let catalog = MockCatalog::new();
	for (outcome, expected_recent, expected_lifetime) in [
		(
			OptimizerDecisionOutcome::Reordered,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 3,
		),
		(
			OptimizerDecisionOutcome::KeptThinMargin,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 3,
		),
		(
			OptimizerDecisionOutcome::KeptOptimal,
			0,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 2,
		),
	] {
		let runtime_evidence =
			std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
		let config = PlannerConfig {
			runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
			..PlannerConfig::default()
		};
		record_conflicted_decisions(
			config.runtime_evidence.as_ref(),
			OptimizerDecisionKind::FlatStar,
			OptimizerRuntimeVerdictProvenance::Native,
		);
		runtime_evidence.set_window_id_for_test(12);
		record_decisions(
			config.runtime_evidence.as_ref(),
			OptimizerDecisionKind::FlatStar,
			outcome,
			OptimizerRuntimeVerdictProvenance::Native,
			JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
		);

		let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
		let row = memo_admission_row(&rows, "flat_star", "native");
		assert_eq!(memo_admission_utf8(row, 2), "stale_evidence");
		assert_eq!(
			memo_admission_utf8(row, 9),
			"requires_recent_conflicted_decision_row"
		);
		assert_eq!(
			memo_admission_i64(row, 4),
			i64::try_from(expected_recent).unwrap()
		);
		assert_eq!(
			memo_admission_i64(row, 5),
			i64::try_from(expected_lifetime.saturating_sub(expected_recent)).unwrap()
		);
		assert_eq!(
			memo_admission_i64(row, 6),
			i64::try_from(expected_lifetime).unwrap()
		);
	}
}

#[tokio::test]
async fn memo_admission_group_recency_is_lane_scoped() {
	let catalog = MockCatalog::new();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Demoted,
	);
	runtime_evidence.set_window_id_for_test(12);
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let demoted = memo_admission_row(&rows, "flat_star", "demoted");
	assert_eq!(memo_admission_utf8(demoted, 2), "stale_evidence");
	assert_eq!(memo_admission_i64(demoted, 4), 0);
	assert_eq!(memo_admission_i64(demoted, 5), 40);
	assert_eq!(memo_admission_i64(demoted, 6), 40);
	let native = memo_admission_row(&rows, "flat_star", "native");
	assert_eq!(memo_admission_utf8(native, 2), "blocked_shape");
	assert_eq!(memo_admission_i64(native, 4), 40);
	assert_eq!(memo_admission_i64(native, 5), 0);
	assert_eq!(memo_admission_i64(native, 6), 40);

	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	runtime_evidence.set_window_id_for_test(12);
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Demoted,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let native = memo_admission_row(&rows, "bridge_root", "native");
	assert_eq!(memo_admission_utf8(native, 2), "stale_evidence");
	assert_eq!(memo_admission_i64(native, 4), 0);
	assert_eq!(memo_admission_i64(native, 5), 40);
	assert_eq!(memo_admission_i64(native, 6), 40);
	let demoted = memo_admission_row(&rows, "bridge_root", "demoted");
	assert_eq!(memo_admission_utf8(demoted, 2), "blocked_shape");
	assert_eq!(memo_admission_i64(demoted, 4), 40);
	assert_eq!(memo_admission_i64(demoted, 5), 0);
	assert_eq!(memo_admission_i64(demoted, 6), 40);
}

#[tokio::test]
async fn memo_admission_window_columns_are_group_local_conflict_samples() {
	let catalog = MockCatalog::new();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	runtime_evidence.set_window_id_for_test(12);
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let flat = memo_admission_row(&rows, "flat_star", "native");
	assert_eq!(memo_admission_i64(flat, 4), 0);
	assert_eq!(memo_admission_i64(flat, 5), 40);
	assert_eq!(memo_admission_i64(flat, 6), 40);
	let bridge = memo_admission_row(&rows, "bridge_root", "native");
	assert_eq!(memo_admission_i64(bridge, 4), 40);
	assert_eq!(memo_admission_i64(bridge, 5), 0);
	assert_eq!(memo_admission_i64(bridge, 6), 40);
}

#[test]
fn memo_admission_degraded_when_evidence_health_not_ok() {
	use super::runtime_evidence::{
		OptimizerEvidenceMode, OptimizerEvidenceState, OptimizerEvidenceWindowId,
		OptimizerEvidenceWindowSnapshot, OptimizerReceiptGateSnapshot,
	};

	let snapshot = OptimizerReceiptGateSnapshot {
		verdicts: Vec::new(),
		decisions: vec![
			OptimizerDecisionRow {
				kind: OptimizerDecisionKind::FlatStar,
				outcome: OptimizerDecisionOutcome::Reordered,
				provenance: OptimizerRuntimeVerdictProvenance::Native,
				count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			},
			OptimizerDecisionRow {
				kind: OptimizerDecisionKind::FlatStar,
				outcome: OptimizerDecisionOutcome::KeptThinMargin,
				provenance: OptimizerRuntimeVerdictProvenance::Native,
				count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			},
		],
		recent_decisions: vec![
			OptimizerDecisionRow {
				kind: OptimizerDecisionKind::FlatStar,
				outcome: OptimizerDecisionOutcome::Reordered,
				provenance: OptimizerRuntimeVerdictProvenance::Native,
				count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			},
			OptimizerDecisionRow {
				kind: OptimizerDecisionKind::FlatStar,
				outcome: OptimizerDecisionOutcome::KeptThinMargin,
				provenance: OptimizerRuntimeVerdictProvenance::Native,
				count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
			},
		],
		probe_linearity_fences: Vec::new(),
		health: OptimizerEvidenceStatusSnapshot {
			mode: OptimizerEvidenceMode::Degraded,
			state: OptimizerEvidenceState::Stale,
			pending_deltas: 0,
			dropped_deltas: 0,
			ambiguous_deltas: 0,
			refresh_failures: 1,
			last_error: Some("fixture".to_string()),
		},
		window: OptimizerEvidenceWindowSnapshot {
			current_window: OptimizerEvidenceWindowId(10),
			previous_window: OptimizerEvidenceWindowId(9),
			recent_count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 2,
			stale_lifetime_count: 0,
			lifetime_count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 2,
		},
	};

	let rows = information_schema_optimizer_memo_admission_rows(
		&optimizer_memo_admission_rows_from_snapshot(&snapshot),
	)
	.expect("render optimizer memo admission rows");
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "degraded_evidence");
	assert_eq!(memo_admission_i64(row, 3), 20);
	assert_eq!(memo_admission_utf8(row, 8), "degraded/stale");
	assert_eq!(memo_admission_utf8(row, 9), "requires_ok_evidence_health");
	assert_eq!(
		memo_admission_utf8(row, 10),
		"durable_optimizer_evidence_health"
	);
}

#[tokio::test]
async fn memo_admission_blocked_shape_when_conflicted_recent_and_healthy() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "blocked_shape");
	assert_eq!(memo_admission_i64(row, 3), 20);
	assert_eq!(memo_admission_i64(row, 4), 40);
	assert_eq!(memo_admission_i64(row, 5), 0);
	assert_eq!(memo_admission_i64(row, 6), 40);
	assert_eq!(memo_admission_i64(row, 7), 0);
	assert_eq!(memo_admission_utf8(row, 8), "local_only/ok");
	assert_eq!(memo_admission_utf8(row, 9), "mixed_blocker_class");
	assert_eq!(memo_admission_utf8(row, 10), "blocker_class_detail");
	assert_eq!(memo_admission_utf8(row, 11), "mixed_blocker");
	assert_eq!(memo_admission_i64(row, 12), 20);
	assert_eq!(memo_admission_i64(row, 13), 0);
	assert_eq!(memo_admission_i64(row, 14), 0);
	assert!(
		rows.iter()
			.all(|row| memo_admission_utf8(row, 2) != "admissible")
	);
}

#[tokio::test]
async fn memo_admission_blocked_shape_when_same_group_conflict_is_recent() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "snowflake_root", "native");

	assert_eq!(memo_admission_utf8(row, 2), "blocked_shape");
	assert_eq!(memo_admission_i64(row, 3), 20);
	assert_eq!(memo_admission_i64(row, 4), 40);
	assert_eq!(memo_admission_i64(row, 5), 0);
	assert_eq!(memo_admission_i64(row, 6), 40);
	assert_eq!(memo_admission_i64(row, 7), 0);
	assert_eq!(memo_admission_utf8(row, 11), "mixed_blocker");
}

#[tokio::test]
async fn memo_admission_margin_limited_conflict_points_to_margin_tuning() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "galaxy_root", "native");

	assert_eq!(memo_admission_utf8(row, 2), "blocked_shape");
	assert_eq!(memo_admission_i64(row, 7), 0);
	assert_eq!(memo_admission_utf8(row, 9), "margin_limited_blocker");
	assert_eq!(memo_admission_utf8(row, 10), "margin_policy_tuning");
	assert_eq!(memo_admission_utf8(row, 11), "margin_limited");
	assert_eq!(memo_admission_i64(row, 12), 20);
	assert_eq!(memo_admission_i64(row, 13), 0);
	assert_eq!(memo_admission_i64(row, 14), 0);
	assert_eq!(memo_admission_utf8(row, 15), "");
	assert_eq!(memo_admission_i64(row, 16), 0);
}

#[tokio::test]
async fn memo_admission_hard_limited_conflict_points_to_hard_blocker_resolution() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_declined_conflict_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionDeclineReason::MissingEstimate,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "blocked_shape");
	assert_eq!(memo_admission_utf8(row, 9), "hard_limited_blocker");
	assert_eq!(memo_admission_utf8(row, 10), "hard_blocker_resolution");
	assert_eq!(memo_admission_utf8(row, 11), "hard_limited");
	assert_eq!(memo_admission_i64(row, 12), 0);
	assert_eq!(memo_admission_i64(row, 13), 20);
	assert_eq!(memo_admission_i64(row, 14), 0);
	assert_eq!(memo_admission_utf8(row, 15), "missing_estimate");
	assert_eq!(memo_admission_i64(row, 16), 20);
}

#[tokio::test]
async fn memo_admission_capability_limited_conflict_points_to_contract_investigation() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_declined_conflict_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionDeclineReason::MultipleProbeRelations,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "blocked_shape");
	assert_eq!(memo_admission_i64(row, 7), 0);
	assert_eq!(memo_admission_utf8(row, 9), "capability_limited_blocker");
	assert_eq!(
		memo_admission_utf8(row, 10),
		"bounded_candidate_contract_investigation"
	);
	assert_eq!(memo_admission_utf8(row, 11), "capability_limited");
	assert_eq!(memo_admission_i64(row, 12), 0);
	assert_eq!(memo_admission_i64(row, 13), 0);
	assert_eq!(memo_admission_i64(row, 14), 20);
	assert_eq!(memo_admission_utf8(row, 15), "multiple_probe_relations");
	assert_eq!(memo_admission_i64(row, 16), 20);
}

#[tokio::test]
async fn memo_admission_mixed_blocker_does_not_overclaim_capability() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerDecisionOutcome::Declined(OptimizerDecisionDeclineReason::MultipleProbeRelations),
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "galaxy_root", "native");

	assert_eq!(memo_admission_utf8(row, 2), "blocked_shape");
	assert_eq!(memo_admission_utf8(row, 9), "mixed_blocker_class");
	assert_eq!(memo_admission_utf8(row, 10), "blocker_class_detail");
	assert_eq!(memo_admission_utf8(row, 11), "mixed_blocker");
	assert_eq!(memo_admission_i64(row, 12), 20);
	assert_eq!(memo_admission_i64(row, 13), 0);
	assert_eq!(memo_admission_i64(row, 14), 20);
}

#[tokio::test]
async fn memo_admission_star_family_is_never_margin_limited() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::KeptThinMargin,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "blocked_shape");
	assert_eq!(memo_admission_utf8(row, 11), "mixed_blocker");
	assert_ne!(memo_admission_utf8(row, 11), "margin_limited");
	assert_eq!(memo_admission_i64(row, 12), 20);
}

#[tokio::test]
async fn memo_admission_blocker_class_does_not_bypass_stale_recency() {
	let catalog = MockCatalog::new();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_declined_conflict_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionDeclineReason::MultipleProbeRelations,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	runtime_evidence.set_window_id_for_test(12);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "stale_evidence");
	assert_eq!(memo_admission_utf8(row, 10), "per_group_window_recency");
	assert_eq!(memo_admission_utf8(row, 11), "capability_limited");
	assert_eq!(memo_admission_i64(row, 14), 20);
}

#[test]
fn memo_admission_blocker_class_does_not_bypass_degraded_health() {
	use super::runtime_evidence::{
		OptimizerEvidenceMode, OptimizerEvidenceState, OptimizerEvidenceWindowId,
		OptimizerEvidenceWindowSnapshot, OptimizerReceiptGateSnapshot,
	};

	let decisions = vec![
		OptimizerDecisionRow {
			kind: OptimizerDecisionKind::FlatStar,
			outcome: OptimizerDecisionOutcome::Reordered,
			provenance: OptimizerRuntimeVerdictProvenance::Native,
			count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
		},
		OptimizerDecisionRow {
			kind: OptimizerDecisionKind::FlatStar,
			outcome: OptimizerDecisionOutcome::Declined(
				OptimizerDecisionDeclineReason::MultipleProbeRelations,
			),
			provenance: OptimizerRuntimeVerdictProvenance::Native,
			count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
		},
	];
	let snapshot = OptimizerReceiptGateSnapshot {
		verdicts: Vec::new(),
		decisions: decisions.clone(),
		recent_decisions: decisions,
		probe_linearity_fences: Vec::new(),
		health: OptimizerEvidenceStatusSnapshot {
			mode: OptimizerEvidenceMode::Degraded,
			state: OptimizerEvidenceState::Stale,
			pending_deltas: 0,
			dropped_deltas: 0,
			ambiguous_deltas: 0,
			refresh_failures: 1,
			last_error: Some("fixture".to_string()),
		},
		window: OptimizerEvidenceWindowSnapshot {
			current_window: OptimizerEvidenceWindowId(10),
			previous_window: OptimizerEvidenceWindowId(9),
			recent_count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 2,
			stale_lifetime_count: 0,
			lifetime_count: JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE * 2,
		},
	};

	let rows = information_schema_optimizer_memo_admission_rows(
		&optimizer_memo_admission_rows_from_snapshot(&snapshot),
	)
	.expect("render optimizer memo admission rows");
	let row = memo_admission_row(&rows, "flat_star", "native");

	assert_eq!(memo_admission_utf8(row, 2), "degraded_evidence");
	assert_eq!(
		memo_admission_utf8(row, 10),
		"durable_optimizer_evidence_health"
	);
	assert_eq!(memo_admission_utf8(row, 11), "capability_limited");
	assert_eq!(memo_admission_i64(row, 14), 20);
}

#[tokio::test]
async fn memo_admission_blocker_columns_are_additive_to_existing_schema() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_memo_admission")
		.expect("plan optimizer memo admission");
	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind optimizer memo admission");
	let ExecutionPlan::SystemTable { schema, rows, .. } = plan else {
		panic!("expected optimizer memo admission system table");
	};
	assert_eq!(
		field_names(&schema),
		vec![
			"decision_kind",
			"provenance",
			"admission_status",
			"conflict_strength",
			"recent_window",
			"stale_lifetime",
			"lifetime_total",
			"bounded_candidate_count",
			"evidence_health",
			"reason",
			"next_cut",
			"blocker_class",
			"margin_blocked_count",
			"hard_decline_count",
			"capability_decline_count",
			"top_decline_reason",
			"top_decline_count",
		]
	);
	assert_eq!(rows[0].len(), 17);
}

#[tokio::test]
async fn optimizer_decision_conflicts_220_schema_unchanged() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_decision_conflicts")
		.expect("plan optimizer decision conflicts");
	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind optimizer decision conflicts");
	let ExecutionPlan::SystemTable { schema, rows, .. } = plan else {
		panic!("expected optimizer decision conflicts system table");
	};
	assert_eq!(
		field_names(&schema),
		vec![
			"decision_kind",
			"provenance",
			"status",
			"reordered_count",
			"kept_optimal_count",
			"kept_thin_margin_count",
			"declined_count",
			"blocked_or_near_miss_count",
			"total_count",
			"conflict_strength",
			"top_decline_reason",
			"top_decline_count",
			"evidence_health",
			"reason",
			"next_cut",
		]
	);
	assert_eq!(rows[0].len(), 15);
}

#[test]
fn memo_admission_uses_same_snapshot_as_conflict_and_window_health() {
	let runtime_evidence = OptimizerRuntimeEvidence::local_only_with_window_id(10);
	record_conflicted_decisions(
		&runtime_evidence,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Demoted,
	);
	let snapshot = runtime_evidence.receipt_gate_snapshot();
	let conflict = decision_conflict_reports(&snapshot)
		.into_iter()
		.find(|report| {
			report.kind == OptimizerDecisionKind::BridgeRoot
				&& report.provenance == OptimizerRuntimeVerdictProvenance::Demoted
		})
		.expect("bridge demoted conflict report");
	let admission = optimizer_memo_admission_rows_from_snapshot(&snapshot)
		.into_iter()
		.find(|row| {
			row.decision_kind == OptimizerDecisionKind::BridgeRoot
				&& row.provenance == OptimizerRuntimeVerdictProvenance::Demoted
		})
		.expect("bridge demoted admission row");

	assert_eq!(admission.conflict_strength, conflict.conflict_strength);
	assert_eq!(admission.recent_window, 40);
	assert_eq!(admission.stale_lifetime, 0);
	assert_eq!(admission.lifetime_total, 40);
	assert_eq!(admission.evidence_health, conflict.evidence_health);
}

#[test]
fn memo_admission_group_recency_uses_same_receipt_snapshot() {
	let runtime_evidence = OptimizerRuntimeEvidence::local_only_with_window_id(10);
	record_conflicted_decisions(
		&runtime_evidence,
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let snapshot = runtime_evidence.receipt_gate_snapshot();
	let rows = optimizer_memo_admission_rows_from_snapshot(&snapshot);
	let row = rows
		.iter()
		.find(|row| {
			row.decision_kind == OptimizerDecisionKind::BridgeRoot
				&& row.provenance == OptimizerRuntimeVerdictProvenance::Native
		})
		.expect("bridge native admission row");
	let recent_conflict_relevant = snapshot
		.recent_decisions
		.iter()
		.filter(|row| {
			row.kind == OptimizerDecisionKind::BridgeRoot
				&& row.provenance == OptimizerRuntimeVerdictProvenance::Native
				&& matches!(
					row.outcome,
					OptimizerDecisionOutcome::Reordered | OptimizerDecisionOutcome::KeptThinMargin
				)
		})
		.fold(0_u64, |sum, row| sum.saturating_add(row.count));

	assert_eq!(row.recent_window, recent_conflict_relevant);
	assert_eq!(row.lifetime_total, 40);
	assert_eq!(row.stale_lifetime, 0);
	assert_eq!(snapshot, runtime_evidence.receipt_gate_snapshot());
}

#[tokio::test]
async fn memo_admission_still_renders_zero_admissible() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Demoted,
	);

	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	assert!(
		rows.iter()
			.all(|row| memo_admission_utf8(row, 2) != "admissible")
	);
	assert!(rows.iter().all(|row| memo_admission_i64(row, 7) == 0));
}

#[tokio::test]
async fn bounded_admission_candidate_contract_returns_zero_for_all_inputs() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	let rows = optimizer_memo_admission_table_rows(&catalog, &config).await;

	for kind in decision_recording_kinds() {
		for provenance in [
			OptimizerRuntimeVerdictProvenance::Native,
			OptimizerRuntimeVerdictProvenance::Demoted,
		] {
			let row = memo_admission_row(
				&rows,
				optimizer_decision_kind_label(kind),
				optimizer_runtime_verdict_provenance_label(provenance),
			);
			assert_eq!(
				memo_admission_i64(row, 7),
				0,
				"bounded candidate contract must stay unbuilt for {kind:?}/{provenance:?}"
			);
			assert_ne!(memo_admission_utf8(row, 2), "admissible");
		}
	}
}

#[tokio::test]
async fn memo_audit_multiple_probe_relations_commits_graph_decision_not_star_decline() {
	let catalog = three_fact_galaxy_join_order_catalog();
	let config = auto_broadcast_config(4_096);
	let logical = SqlPlanner::new()
		.plan(three_fact_galaxy_conformed_order_sql())
		.expect("plan conformed galaxy");

	let _plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind conformed galaxy through production path");
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(&rows, "galaxy_root", "reordered", None),
		1
	);
	assert!(
		!rows
			.iter()
			.any(|row| row[2] == ScalarValue::Utf8(Some("multiple_probe_relations".to_string()))),
		"real conformed galaxy must not commit a star multiple-probe decline: {rows:?}"
	);
}

#[tokio::test]
async fn memo_audit_mixed_strategy_star_is_authorization_fence_not_missing_candidate() {
	let catalog = mixed_strategy_star_join_order_catalog();
	let config = auto_broadcast_config(4_096);
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan mixed-strategy star");

	let _plan = bind(&logical, &catalog, &config)
		.await
		.expect("mixed-strategy star decline must still bind a valid fallback plan");
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(&rows, "flat_star", "declined", Some("mixed_strategy_star")),
		1
	);

	let legal_catalog = mixed_strategy_snowflake_join_order_catalog();
	let legal_config = auto_broadcast_config(4_096);
	let legal = SqlPlanner::new()
		.plan(mixed_strategy_snowflake_order_sql())
		.expect("plan legal mixed-strategy snowflake");
	let _legal_plan = bind(&legal, &legal_catalog, &legal_config)
		.await
		.expect("authorized mixed-strategy snowflake binds");
	let legal_rows = optimizer_decision_rows(&legal_catalog, &legal_config).await;
	assert_eq!(
		optimizer_decision_count(&legal_rows, "snowflake_root", "kept_optimal", None),
		1
	);
	assert!(
		!legal_rows.iter().any(|row| {
			row[1] == ScalarValue::Utf8(Some("declined".to_string()))
				&& row[2] == ScalarValue::Utf8(Some("mixed_strategy_star".to_string()))
		}),
		"authorized mixed strategy must actuate instead of committing a missing-candidate decline: {legal_rows:?}"
	);
}

#[tokio::test]
async fn memo_audit_probe_not_scan_linear_is_physical_fence_not_missing_candidate() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let config = auto_broadcast_config(4_096);
	let logical = SqlPlanner::new()
		.plan(observed_snowflake_order_sql())
		.expect("plan non-scan-linear snowflake");

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("non-scan-linear probe decline must still bind a valid fallback plan");
	assert_join_strategy_count(&plan, JoinStrategy::HashPartitioned, 4);
	let rows = optimizer_decision_rows(&catalog, &config).await;

	assert_eq!(
		optimizer_decision_count(
			&rows,
			"star_candidate",
			"declined",
			Some("probe_not_scan_linear")
		),
		1
	);
	assert!(
		!rows.iter().any(|row| {
			row[0] == ScalarValue::Utf8(Some("snowflake_root".to_string()))
				&& row[1] == ScalarValue::Utf8(Some("reordered".to_string()))
		}),
		"physical-fence fallback must not claim a generated snowflake candidate: {rows:?}"
	);
}

#[tokio::test]
async fn optimizer_receipt_gates_windowing_manifest_stays_global_six_rows() {
	let catalog = MockCatalog::new();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	runtime_evidence.set_window_id_for_test(12);
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Native,
	);

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	assert_receipt_gate_order(&rows);
	assert_eq!(rows.len(), 6);
	let windowing = receipt_gate_row_by_name(&rows, "windowing_decay_pressure");
	assert_eq!(receipt_gate_utf8(windowing, 2), "inactive");
	assert_eq!(receipt_gate_i64(windowing, 3), 80);
	assert_eq!(
		receipt_gate_utf8(windowing, 4),
		"recent_window=40,stale_lifetime=40,lifetime_total=80"
	);
}

#[tokio::test]
async fn memo_admission_readout_is_read_only_no_plan_change() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let quiet_config = auto_broadcast_config(64 * 1024 * 1024);
	let noisy_config = PlannerConfig {
		join_order_soft_margin_application: JoinOrderSoftMarginApplication::ApplyAdvisory,
		..auto_broadcast_config(64 * 1024 * 1024)
	};
	record_conflicted_decisions(
		noisy_config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	let before = noisy_config.runtime_evidence.receipt_gate_snapshot();
	let _ = optimizer_memo_admission_table_rows(&catalog, &noisy_config).await;
	let after = noisy_config.runtime_evidence.receipt_gate_snapshot();
	assert_eq!(before, after);

	let (quiet_plan, quiet_trace) = bind_with_trace(&logical, &catalog, &quiet_config)
		.await
		.expect("bind quiet plan");
	let (noisy_plan, noisy_trace) = bind_with_trace(&logical, &catalog, &noisy_config)
		.await
		.expect("bind noisy plan");

	assert_eq!(format!("{quiet_plan:?}"), format!("{noisy_plan:?}"));
	assert_eq!(quiet_trace.join_decisions, noisy_trace.join_decisions);
	assert_eq!(
		quiet_trace.scan_filter_decisions,
		noisy_trace.scan_filter_decisions
	);
}

#[tokio::test]
async fn rune_queries_optimizer_memo_admission() {
	let catalog = star_join_order_catalog();
	let config = PlannerConfig::default();
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	let sql_rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_memo_admission")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer memo admission report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer memo admission system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerMemoAdmission
	);
	assert_eq!(rows, sql_rows);
}

#[tokio::test]
async fn rune_queries_optimizer_memo_admission_with_group_recency() {
	let catalog = star_join_order_catalog();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	runtime_evidence.set_window_id_for_test(12);
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	let sql_rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let stale = memo_admission_row(&sql_rows, "flat_star", "native");
	assert_eq!(memo_admission_utf8(stale, 2), "stale_evidence");
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_memo_admission")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer memo admission group recency report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer memo admission system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerMemoAdmission
	);
	assert_eq!(rows, sql_rows);
}

#[tokio::test]
async fn rune_queries_optimizer_memo_admission_with_blocker_class() {
	let catalog = star_join_order_catalog();
	let config = PlannerConfig::default();
	record_declined_conflict_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionDeclineReason::MultipleProbeRelations,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	let sql_rows = optimizer_memo_admission_table_rows(&catalog, &config).await;
	let row = memo_admission_row(&sql_rows, "flat_star", "native");
	assert_eq!(memo_admission_utf8(row, 11), "capability_limited");
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_memo_admission")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer memo admission blocker class report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer memo admission system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerMemoAdmission
	);
	assert_eq!(rows, sql_rows);
}

#[tokio::test]
async fn optimizer_probe_linearity_fences_projects_static_14_rows() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	let rows = optimizer_probe_linearity_fence_table_rows(&catalog, &config).await;

	assert_probe_linearity_fence_order(&rows);
	assert_eq!(rows.len(), 14);
	assert!(rows.iter().all(|row| {
		(2..=6).all(|index| probe_linearity_fence_i64(row, index) == 0)
			&& probe_linearity_fence_utf8(row, 7) == "absent"
			&& probe_linearity_fence_utf8(row, 8) == "no_recent_evidence"
			&& probe_linearity_fence_utf8(row, 9) == "await_dominant_probe_linearity_fence"
	}));

	let projected = LogicalPlan::ScanNamed {
		table: TableRef::with_schema("information_schema", "optimizer_probe_linearity_fences"),
		projection: Some(vec![
			"consumer".to_string(),
			"fence".to_string(),
			"status".to_string(),
			"next_cut".to_string(),
		]),
	};
	let plan = bind(&projected, &catalog, &config)
		.await
		.expect("bind projected optimizer probe-linearity fences");
	let ExecutionPlan::SystemTable { kind, schema, rows } = plan else {
		panic!("expected projected optimizer probe-linearity system table");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerProbeLinearityFences
	);
	assert_eq!(
		field_names(&schema),
		vec!["consumer", "fence", "status", "next_cut"]
	);
	assert_eq!(rows.len(), 14);
}

#[tokio::test]
async fn hash_fallback_probe_linearity_fence_rows_are_production_fed() {
	let mut catalog = join_catalog();
	seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
	seed_byte_upper_bound(&mut catalog, "customers", 1_024);
	let config = auto_broadcast_config(4_096);
	let logical = nested_hash_probe_with_tiny_right();

	let _plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind nested hash probe");
	let rows = optimizer_probe_linearity_fence_table_rows(&catalog, &config).await;
	let row = probe_linearity_fence_row(&rows, "hash_fallback", "non_broadcast_join");

	assert_eq!(probe_linearity_fence_i64(row, 2), 1);
	assert_eq!(probe_linearity_fence_i64(row, 3), 1);
	assert_eq!(probe_linearity_fence_i64(row, 4), 1);
	assert_eq!(probe_linearity_fence_i64(row, 5), 1);
	assert_eq!(probe_linearity_fence_i64(row, 6), 1000);
	assert_eq!(probe_linearity_fence_utf8(row, 7), "insufficient_evidence");
	assert_eq!(
		probe_linearity_fence_utf8(row, 8),
		"consumer_recent_total_below_floor"
	);
	assert_eq!(
		probe_linearity_fence_i64(
			probe_linearity_fence_row(&rows, "star_decline", "non_broadcast_join"),
			2,
		),
		0
	);
}

#[tokio::test]
async fn star_decline_probe_linearity_fence_rows_are_production_fed() {
	let catalog = star_join_order_catalog();
	let config = auto_broadcast_config(64 * 1024 * 1024);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o").sort([qcol("o", "id").asc()])
               .auto_join(
                   ctx.table("customers").alias("c"),
                   qcol("o", "customer_id").eq(qcol("c", "id"))
               )
               .auto_join(
                   ctx.table("products").alias("p"),
                   qcol("o", "product_id").eq(qcol("p", "id"))
               )
               .auto_join(
                   ctx.table("stores").alias("s"),
                   qcol("o", "store_id").eq(qcol("s", "id"))
               )
        }
    "#,
	)
	.await;

	let _plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind sorted-probe star");
	let rows = optimizer_probe_linearity_fence_table_rows(&catalog, &config).await;
	let row = probe_linearity_fence_row(&rows, "star_decline", "sort");

	assert_eq!(probe_linearity_fence_i64(row, 2), 1);
	assert_eq!(probe_linearity_fence_i64(row, 3), 1);
	assert_eq!(probe_linearity_fence_i64(row, 4), 1);
	assert_eq!(probe_linearity_fence_i64(row, 5), 1);
	assert_eq!(probe_linearity_fence_i64(row, 6), 1000);
	assert_eq!(probe_linearity_fence_utf8(row, 7), "insufficient_evidence");

	let decision_rows = optimizer_decision_rows(&catalog, &config).await;
	assert_eq!(
		optimizer_decision_count(
			&decision_rows,
			"star_candidate",
			"declined",
			Some("probe_not_scan_linear")
		),
		1
	);
}

#[tokio::test]
async fn probe_linearity_fence_statuses_use_consumer_window_share() {
	let catalog = MockCatalog::new();
	let insufficient = PlannerConfig::default();
	for _ in 0..19 {
		insufficient.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::Sort,
		);
	}
	let rows = optimizer_probe_linearity_fence_table_rows(&catalog, &insufficient).await;
	let sort = probe_linearity_fence_row(&rows, "hash_fallback", "sort");
	assert_eq!(probe_linearity_fence_i64(sort, 2), 19);
	assert_eq!(probe_linearity_fence_i64(sort, 4), 19);
	assert_eq!(probe_linearity_fence_utf8(sort, 7), "insufficient_evidence");

	let dominant = PlannerConfig::default();
	for _ in 0..20 {
		dominant.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::Sort,
		);
	}
	let rows = optimizer_probe_linearity_fence_table_rows(&catalog, &dominant).await;
	let sort = probe_linearity_fence_row(&rows, "hash_fallback", "sort");
	assert_eq!(probe_linearity_fence_i64(sort, 2), 20);
	assert_eq!(probe_linearity_fence_i64(sort, 4), 20);
	assert_eq!(probe_linearity_fence_i64(sort, 6), 1000);
	assert_eq!(probe_linearity_fence_utf8(sort, 7), "dominant");
	assert_eq!(
		probe_linearity_fence_utf8(sort, 9),
		"targeted_probe_linearity_relaxation"
	);

	let split = PlannerConfig::default();
	for _ in 0..20 {
		split.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::Sort,
		);
		split.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::Take,
		);
	}
	let rows = optimizer_probe_linearity_fence_table_rows(&catalog, &split).await;
	let sort = probe_linearity_fence_row(&rows, "hash_fallback", "sort");
	let take = probe_linearity_fence_row(&rows, "hash_fallback", "take");
	assert_eq!(probe_linearity_fence_i64(sort, 4), 40);
	assert_eq!(probe_linearity_fence_i64(sort, 6), 500);
	assert_eq!(probe_linearity_fence_utf8(sort, 7), "observed");
	assert_eq!(probe_linearity_fence_utf8(take, 7), "observed");
}

#[tokio::test]
async fn probe_linearity_gate_requires_dominant_real_lane() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	for _ in 0..19 {
		config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::Sort,
		);
	}

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let gate = receipt_gate_row_by_name(&rows, "probe_linearity_fence_dominance");

	assert_eq!(receipt_gate_utf8(gate, 2), "insufficient_evidence");
	assert_eq!(receipt_gate_i64(gate, 3), 19);
	assert_eq!(
		receipt_gate_utf8(gate, 4),
		"no_dominant_probe_linearity_fence"
	);
	assert_eq!(
		receipt_gate_utf8(gate, 5),
		"strongest_consumer_recent_total=19,evidence_health=local_only/ok"
	);
	assert_eq!(
		receipt_gate_utf8(gate, 6),
		"requires_recent_probe_linearity_floor"
	);
}

#[tokio::test]
async fn probe_linearity_gate_is_cold_without_evidence() {
	let rows =
		optimizer_receipt_gate_table_rows(&MockCatalog::new(), &PlannerConfig::default()).await;
	let gate = receipt_gate_row_by_name(&rows, "probe_linearity_fence_dominance");

	assert_eq!(receipt_gate_utf8(gate, 2), "insufficient_evidence");
	assert_eq!(receipt_gate_i64(gate, 3), 0);
	assert_eq!(
		receipt_gate_utf8(gate, 4),
		"no_dominant_probe_linearity_fence"
	);
	assert_eq!(
		receipt_gate_utf8(gate, 6),
		"requires_recent_probe_linearity_floor"
	);
}

#[tokio::test]
async fn probe_linearity_gate_selects_strongest_dominant_fence() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	for _ in 0..20 {
		config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::Sort,
		);
	}
	for _ in 0..25 {
		config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::StarDecline,
			ProbeLinearityFence::Exchange,
		);
	}

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let gate = receipt_gate_row_by_name(&rows, "probe_linearity_fence_dominance");

	assert_eq!(receipt_gate_utf8(gate, 2), "actionable");
	assert_eq!(receipt_gate_i64(gate, 3), 25);
	assert_eq!(
		receipt_gate_utf8(gate, 4),
		"consumer=star_decline,fence=exchange"
	);
	assert_eq!(
		receipt_gate_utf8(gate, 5),
		"recent_count=25,consumer_recent_total=25,recent_share_per_mille=1000,evidence_health=local_only/ok"
	);
	assert_eq!(
		receipt_gate_utf8(gate, 6),
		"dominant_probe_linearity_fence_ready"
	);
	assert_eq!(
		receipt_gate_utf8(gate, 7),
		"targeted_probe_linearity_fence_relaxation"
	);
}

#[tokio::test]
async fn probe_linearity_gate_uses_same_locked_snapshot_as_229_detail() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	for _ in 0..20 {
		config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::StarDecline,
			ProbeLinearityFence::Sort,
		);
	}

	let snapshot = config.runtime_evidence.receipt_gate_snapshot();
	let detail = snapshot
		.probe_linearity_fences
		.iter()
		.find(|row| {
			row.consumer == OptimizerProbeLinearityConsumer::StarDecline
				&& row.fence == ProbeLinearityFence::Sort
		})
		.expect("star decline sort detail row");
	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let gate = receipt_gate_row_by_name(&rows, "probe_linearity_fence_dominance");

	assert_eq!(detail.status, OptimizerProbeLinearityFenceStatus::Dominant);
	assert_eq!(
		receipt_gate_utf8(gate, 4),
		"consumer=star_decline,fence=sort"
	);
	assert_eq!(
		receipt_gate_utf8(gate, 5),
		format!(
			"recent_count={},consumer_recent_total={},recent_share_per_mille={},evidence_health=local_only/ok",
			detail.recent_count, detail.consumer_recent_total, detail.recent_share_per_mille
		)
	);
	assert_eq!(snapshot, config.runtime_evidence.receipt_gate_snapshot());
}

#[tokio::test]
async fn probe_linearity_gate_rejects_split_pressure() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	for _ in 0..20 {
		config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::Sort,
		);
		config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::Take,
		);
	}

	let detail_rows = optimizer_probe_linearity_fence_table_rows(&catalog, &config).await;
	assert_eq!(
		probe_linearity_fence_utf8(
			probe_linearity_fence_row(&detail_rows, "hash_fallback", "sort"),
			7
		),
		"observed"
	);
	assert_eq!(
		probe_linearity_fence_utf8(
			probe_linearity_fence_row(&detail_rows, "hash_fallback", "take"),
			7
		),
		"observed"
	);

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let gate = receipt_gate_row_by_name(&rows, "probe_linearity_fence_dominance");

	assert_eq!(receipt_gate_utf8(gate, 2), "inactive");
	assert_eq!(receipt_gate_i64(gate, 3), 40);
	assert_eq!(
		receipt_gate_utf8(gate, 4),
		"no_dominant_probe_linearity_fence"
	);
	assert_eq!(
		receipt_gate_utf8(gate, 5),
		"strongest_consumer_recent_total=40,evidence_health=local_only/ok"
	);
	assert_eq!(receipt_gate_utf8(gate, 6), "probe_linearity_pressure_split");
}

#[tokio::test]
async fn probe_linearity_gate_is_read_only_no_plan_change() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let quiet_config = auto_broadcast_config(64 * 1024 * 1024);
	let noisy_config = auto_broadcast_config(64 * 1024 * 1024);
	for _ in 0..20 {
		noisy_config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::NonBroadcastJoin,
		);
	}

	let before = noisy_config.runtime_evidence.receipt_gate_snapshot();
	let rows = optimizer_receipt_gate_table_rows(&catalog, &noisy_config).await;
	let after = noisy_config.runtime_evidence.receipt_gate_snapshot();
	assert_eq!(before, after);
	assert_eq!(
		receipt_gate_utf8(
			receipt_gate_row_by_name(&rows, "probe_linearity_fence_dominance"),
			2
		),
		"actionable"
	);

	let (quiet_plan, quiet_trace) = bind_with_trace(&logical, &catalog, &quiet_config)
		.await
		.expect("bind quiet plan");
	let (noisy_plan, noisy_trace) = bind_with_trace(&logical, &catalog, &noisy_config)
		.await
		.expect("bind noisy plan");

	assert_eq!(format!("{quiet_plan:?}"), format!("{noisy_plan:?}"));
	assert_eq!(quiet_trace.join_decisions, noisy_trace.join_decisions);
	assert_eq!(
		quiet_trace.scan_filter_decisions,
		noisy_trace.scan_filter_decisions
	);
}

#[tokio::test]
async fn probe_linearity_fence_readout_uses_recent_window_but_preserves_lifetime() {
	let catalog = MockCatalog::new();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	for _ in 0..20 {
		config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::StarDecline,
			ProbeLinearityFence::Sort,
		);
	}
	runtime_evidence.set_window_id_for_test(12);
	let rows = optimizer_probe_linearity_fence_table_rows(&catalog, &config).await;
	let row = probe_linearity_fence_row(&rows, "star_decline", "sort");

	assert_eq!(probe_linearity_fence_i64(row, 2), 0);
	assert_eq!(probe_linearity_fence_i64(row, 3), 20);
	assert_eq!(probe_linearity_fence_i64(row, 4), 0);
	assert_eq!(probe_linearity_fence_i64(row, 5), 20);
	assert_eq!(probe_linearity_fence_utf8(row, 7), "absent");
	assert_eq!(probe_linearity_fence_utf8(row, 8), "no_recent_evidence");
}

#[tokio::test]
async fn probe_linearity_fence_readout_is_read_only_no_plan_change() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let quiet_config = auto_broadcast_config(64 * 1024 * 1024);
	let noisy_config = auto_broadcast_config(64 * 1024 * 1024);
	for _ in 0..20 {
		noisy_config.runtime_evidence.record_probe_linearity_fence(
			OptimizerProbeLinearityConsumer::HashFallback,
			ProbeLinearityFence::NonBroadcastJoin,
		);
	}
	let before = noisy_config
		.runtime_evidence
		.probe_linearity_fence_snapshot();
	let _ = optimizer_probe_linearity_fence_table_rows(&catalog, &noisy_config).await;
	let after = noisy_config
		.runtime_evidence
		.probe_linearity_fence_snapshot();
	assert_eq!(before, after);

	let (quiet_plan, quiet_trace) = bind_with_trace(&logical, &catalog, &quiet_config)
		.await
		.expect("bind quiet plan");
	let (noisy_plan, noisy_trace) = bind_with_trace(&logical, &catalog, &noisy_config)
		.await
		.expect("bind noisy plan");

	assert_eq!(format!("{quiet_plan:?}"), format!("{noisy_plan:?}"));
	assert_eq!(quiet_trace.join_decisions, noisy_trace.join_decisions);
	assert_eq!(
		quiet_trace.scan_filter_decisions,
		noisy_trace.scan_filter_decisions
	);
}

#[tokio::test]
async fn rune_queries_optimizer_probe_linearity_fences() {
	let catalog = star_join_order_catalog();
	let config = PlannerConfig::default();
	config.runtime_evidence.record_probe_linearity_fence(
		OptimizerProbeLinearityConsumer::HashFallback,
		ProbeLinearityFence::NonBroadcastJoin,
	);
	let sql_rows = optimizer_probe_linearity_fence_table_rows(&catalog, &config).await;
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.optimizer_probe_linearity_fences()
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer probe-linearity fence report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer probe-linearity system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerProbeLinearityFences
	);
	assert_eq!(rows, sql_rows);
}

#[tokio::test]
async fn windowing_gate_inactive_when_recent_window_matches_lifetime() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let windowing = receipt_gate_row_by_name(&rows, "windowing_decay_pressure");

	assert_eq!(receipt_gate_utf8(windowing, 2), "inactive");
	assert_eq!(receipt_gate_i64(windowing, 3), 40);
	assert_eq!(
		receipt_gate_utf8(windowing, 4),
		"recent_window=40,stale_lifetime=0,lifetime_total=40"
	);
	assert_eq!(
		receipt_gate_utf8(windowing, 6),
		"recent_window_matches_lifetime"
	);
}

#[tokio::test]
async fn windowing_gate_actionable_when_lifetime_dominated_by_stale_windows() {
	let catalog = MockCatalog::new();
	let runtime_evidence =
		std::sync::Arc::new(OptimizerRuntimeEvidence::local_only_with_window_id(10));
	let config = PlannerConfig {
		runtime_evidence: std::sync::Arc::clone(&runtime_evidence),
		..PlannerConfig::default()
	};
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	runtime_evidence.set_window_id_for_test(12);

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let windowing = receipt_gate_row_by_name(&rows, "windowing_decay_pressure");

	assert_eq!(receipt_gate_utf8(windowing, 2), "actionable");
	assert_eq!(receipt_gate_i64(windowing, 3), 40);
	assert_eq!(
		receipt_gate_utf8(windowing, 4),
		"recent_window=0,stale_lifetime=40,lifetime_total=40"
	);
	assert_eq!(
		receipt_gate_utf8(windowing, 6),
		"stale_window_evidence_dominates"
	);
}

#[tokio::test]
async fn windowing_gate_insufficient_on_cold_ledger() {
	let rows =
		optimizer_receipt_gate_table_rows(&MockCatalog::new(), &PlannerConfig::default()).await;
	let windowing = receipt_gate_row_by_name(&rows, "windowing_decay_pressure");

	assert_eq!(receipt_gate_utf8(windowing, 2), "insufficient_evidence");
	assert_eq!(receipt_gate_i64(windowing, 3), 0);
	assert_eq!(
		receipt_gate_utf8(windowing, 4),
		"recent_window=0,stale_lifetime=0,lifetime_total=0"
	);
	assert_eq!(
		receipt_gate_utf8(windowing, 6),
		"requires_lifetime_evidence_floor"
	);
}

#[tokio::test]
async fn counter_handoff_readiness_stays_blocked_external_signal() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);

	let rows = optimizer_receipt_gate_table_rows(&catalog, &config).await;
	let counter = receipt_gate_row_by_name(&rows, "counter_handoff_readiness");

	assert_eq!(receipt_gate_utf8(counter, 2), "blocked_external_signal");
	assert_eq!(receipt_gate_i64(counter, 3), 0);
	assert_eq!(receipt_gate_utf8(counter, 6), "elasticity_signal_unbuilt");
}

#[tokio::test]
async fn windowing_readout_is_read_only_no_plan_change() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");
	let quiet_config = auto_broadcast_config(64 * 1024 * 1024);
	let noisy_config = auto_broadcast_config(64 * 1024 * 1024);
	record_verdicts(
		noisy_config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		noisy_config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerDecisionOutcome::Reordered,
		OptimizerRuntimeVerdictProvenance::Native,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	let before = noisy_config.runtime_evidence.receipt_gate_snapshot();
	let _ = optimizer_receipt_gate_table_rows(&catalog, &noisy_config).await;
	let after = noisy_config.runtime_evidence.receipt_gate_snapshot();
	assert_eq!(before, after);

	let (quiet_plan, quiet_trace) = bind_with_trace(&logical, &catalog, &quiet_config)
		.await
		.expect("bind quiet plan");
	let (noisy_plan, noisy_trace) = bind_with_trace(&logical, &catalog, &noisy_config)
		.await
		.expect("bind noisy plan");

	assert_eq!(format!("{quiet_plan:?}"), format!("{noisy_plan:?}"));
	assert_eq!(quiet_trace.join_decisions, noisy_trace.join_decisions);
	assert_eq!(
		quiet_trace.scan_filter_decisions,
		noisy_trace.scan_filter_decisions
	);
}

#[tokio::test]
async fn rune_queries_optimizer_receipt_gates_with_windowing() {
	let catalog = star_join_order_catalog();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeChainRoot,
		OptimizerRuntimeVerdictClass::OverEager,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_receipt_gates")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer receipt gate report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer receipt gates system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerReceiptGates
	);
	assert_receipt_gate_order(&rows);
	assert_eq!(
		receipt_gate_utf8(
			receipt_gate_row_by_name(&rows, "graph_margin_divergence"),
			2
		),
		"actionable"
	);
	let windowing = receipt_gate_row_by_name(&rows, "windowing_decay_pressure");
	assert_eq!(receipt_gate_utf8(windowing, 2), "inactive");
	assert_eq!(
		receipt_gate_utf8(windowing, 6),
		"recent_window_matches_lifetime"
	);
	let probe = receipt_gate_row_by_name(&rows, "probe_linearity_fence_dominance");
	assert_eq!(receipt_gate_utf8(probe, 2), "insufficient_evidence");
}

#[tokio::test]
async fn rune_queries_graph_margin_advisory() {
	let catalog = star_join_order_catalog();
	let config = PlannerConfig::default();
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerRuntimeVerdictClass::OverEager,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM information_schema.optimizer_margin_advisory")
        }
    "#,
	)
	.await;

	let plan = bind(&logical, &catalog, &config)
		.await
		.expect("bind Rune optimizer margin advisory report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected Rune optimizer margin advisory system table");
	};

	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerMarginAdvisory
	);
	assert_margin_advisory_scope_order(&rows);
	let tree = margin_advisory_row_by_scope(&rows, "tree_join_order");
	assert_eq!(margin_advisory_utf8(tree, 2), "tighten");
	assert_eq!(margin_advisory_utf8(tree, 1), "7/10");
	let graph = margin_advisory_row_by_scope(&rows, "graph_join_order");
	assert_eq!(margin_advisory_utf8(graph, 2), "loosen");
	assert_eq!(margin_advisory_utf8(graph, 1), "9/10");
}

#[tokio::test]
async fn rune_optimizer_readout_handles_match_ctx_sql_plan() {
	for (handle, table, _) in RUNE_OPTIMIZER_READOUT_HANDLES {
		let rune_logical = rune_plan(&optimizer_readout_handle_script(handle)).await;
		let sql_logical = SqlPlanner::new()
			.plan(&optimizer_readout_sql(table))
			.unwrap_or_else(|error| panic!("plan SQL optimizer readout {table}: {error:?}"));

		assert_optimizer_readout_scan_plan(&rune_logical, table);
		assert_optimizer_readout_scan_plan(&sql_logical, table);
		assert_eq!(format!("{rune_logical:?}"), format!("{sql_logical:?}"));
	}
}

#[tokio::test]
async fn rune_optimizer_readout_handles_match_ctx_sql_rows() {
	let catalog = star_join_order_catalog();
	let config = optimizer_readout_seeded_config();

	for (handle, table, expected_kind) in RUNE_OPTIMIZER_READOUT_HANDLES {
		let rune_logical = rune_plan(&optimizer_readout_handle_script(handle)).await;
		let sql_logical = SqlPlanner::new()
			.plan(&optimizer_readout_sql(table))
			.unwrap_or_else(|error| panic!("plan SQL optimizer readout {table}: {error:?}"));
		let (rune_kind, rune_rows) = bind_optimizer_readout(&rune_logical, &catalog, &config).await;
		let (sql_kind, sql_rows) = bind_optimizer_readout(&sql_logical, &catalog, &config).await;

		assert_eq!(rune_kind, expected_kind);
		assert_eq!(sql_kind, expected_kind);
		assert_eq!(rune_rows, sql_rows, "row parity failed for {handle}");
	}
}

#[tokio::test]
async fn rune_optimizer_readout_handles_bind_to_existing_system_tables() {
	let catalog = MockCatalog::new();
	let config = PlannerConfig::default();

	for (handle, table, expected_kind) in RUNE_OPTIMIZER_READOUT_HANDLES {
		let logical = rune_plan(&optimizer_readout_handle_script(handle)).await;
		let (kind, _) = bind_optimizer_readout(&logical, &catalog, &config).await;

		assert_eq!(kind, expected_kind, "wrong system table kind for {table}");
	}
}

#[tokio::test]
async fn rune_optimizer_readout_handles_are_composable() {
	let catalog = MockCatalog::new();
	let config = optimizer_readout_seeded_config();
	let rune_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.optimizer_receipt_gates()
               .filter(col("status").eq(lit_str("actionable")))
               .project([col("gate_name"), col("status")])
        }
    "#,
	)
	.await;
	let sql_logical = SqlPlanner::new()
		.plan(
			"SELECT gate_name, status \
			 FROM information_schema.optimizer_receipt_gates \
			 WHERE status = 'actionable'",
		)
		.expect("plan composable SQL optimizer receipt gates");

	let rune_plan = bind(&rune_logical, &catalog, &config)
		.await
		.expect("bind composable Rune optimizer receipt gates");
	let sql_plan = bind(&sql_logical, &catalog, &config)
		.await
		.expect("bind composable SQL optimizer receipt gates");

	assert_eq!(format!("{rune_plan:?}"), format!("{sql_plan:?}"));
}

#[tokio::test]
async fn rune_optimizer_readout_handles_are_lazy_logical_plans() {
	for (handle, table, _) in RUNE_OPTIMIZER_READOUT_HANDLES {
		let logical = rune_plan(&optimizer_readout_handle_script(handle)).await;

		assert_optimizer_readout_scan_plan(&logical, table);
	}
}

#[test]
fn rune_optimizer_readout_handles_cover_exactly_the_eight_readout_tables() {
	let actual = RUNE_OPTIMIZER_READOUT_HANDLES
		.iter()
		.map(|(handle, table, _)| (*handle, *table))
		.collect::<Vec<_>>();

	assert_eq!(
		actual,
		vec![
			("optimizer_decisions", "optimizer_decisions"),
			("optimizer_runtime_verdicts", "optimizer_runtime_verdicts"),
			("optimizer_evidence_status", "optimizer_evidence_status"),
			("optimizer_margin_advisory", "optimizer_margin_advisory"),
			("optimizer_receipt_gates", "optimizer_receipt_gates"),
			(
				"optimizer_decision_conflicts",
				"optimizer_decision_conflicts"
			),
			("optimizer_memo_admission", "optimizer_memo_admission"),
			(
				"optimizer_probe_linearity_fences",
				"optimizer_probe_linearity_fences"
			),
		]
	);
}

#[tokio::test]
async fn rune_optimizer_readout_handles_are_read_only() {
	let catalog = MockCatalog::new();
	let config = optimizer_readout_seeded_config();
	let before = config.runtime_evidence.receipt_gate_snapshot();

	for (handle, _, _) in RUNE_OPTIMIZER_READOUT_HANDLES {
		let logical = rune_plan(&optimizer_readout_handle_script(handle)).await;
		let _ = bind_optimizer_readout(&logical, &catalog, &config).await;
	}

	assert_eq!(before, config.runtime_evidence.receipt_gate_snapshot());
}

#[tokio::test]
async fn rune_optimizer_readout_handles_do_not_change_user_query_plans() {
	let catalog = star_join_order_catalog();
	let config = optimizer_readout_seeded_config();
	let user_logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders")
               .filter(col("id").gt(lit_i64(10)))
               .project([col("id"), col("customer_id")])
        }
    "#,
	)
	.await;
	let (before_plan, before_trace) = bind_with_trace(&user_logical, &catalog, &config)
		.await
		.expect("bind user Rune query before readouts");

	for (handle, _, _) in RUNE_OPTIMIZER_READOUT_HANDLES {
		let logical = rune_plan(&optimizer_readout_handle_script(handle)).await;
		let _ = bind_optimizer_readout(&logical, &catalog, &config).await;
	}

	let (after_plan, after_trace) = bind_with_trace(&user_logical, &catalog, &config)
		.await
		.expect("bind user Rune query after readouts");
	assert_eq!(format!("{before_plan:?}"), format!("{after_plan:?}"));
	assert_eq!(before_trace.join_decisions, after_trace.join_decisions);
	assert_eq!(
		before_trace.scan_filter_decisions,
		after_trace.scan_filter_decisions
	);
}

#[tokio::test]
async fn ctx_table_remains_one_part_user_table_handle() {
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("information_schema.optimizer_decisions")
        }
    "#,
	)
	.await;
	let LogicalPlan::ScanNamed { table, projection } = logical else {
		panic!("expected one-part ctx.table ScanNamed");
	};

	assert_eq!(table.schema, None);
	assert_eq!(table.table, "information_schema.optimizer_decisions");
	assert_eq!(projection, None);
}

#[tokio::test]
async fn rune_respects_operator_margin() {
	let mut catalog = star_join_order_catalog();
	seed_soft_star_join_order_facts(&mut catalog, 1_100, 1_000);
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.sql("EXPLAIN SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id INNER JOIN products p ON o.product_id = p.id")
        }
    "#,
	)
	.await;

	let (_schema, rows) = bind_explain_rows(
		&logical,
		&catalog,
		&margin_config(SoftMarginLadderStep::Loose),
	)
	.await;
	let row = join_order_row(&rows);
	assert_utf8_contains(&row[3], "soft_margin=9/10");
}

#[tokio::test]
async fn star_order_replay_ids_and_receipts_follow_applied_sequence() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan non-optimal star");

	let (plan, trace) =
		bind_with_trace(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind non-optimal star");

	assert_eq!(
		trace
			.join_decisions
			.iter()
			.map(|receipt| receipt.target.join_index)
			.collect::<Vec<_>>(),
		vec![0, 1, 2]
	);
	assert!(trace.join_decisions.iter().all(|receipt| {
		receipt.strategy == JoinStrategy::Broadcast
			&& receipt.placement == JoinPhysicalPlacement::SwappedRightBuild
	}));
	assert_eq!(
		broadcast_build_scan_ids_in_applied_order(&plan),
		vec![TableId(24), TableId(23), TableId(22)]
	);
}

#[tokio::test]
async fn projected_star_reorder_restores_downstream_ordinals() {
	let catalog = star_join_order_catalog();
	let sql = "SELECT c.id, p.id, s.id FROM orders o \
			   INNER JOIN customers c ON o.customer_id = c.id \
			   INNER JOIN products p ON o.product_id = p.id \
			   INNER JOIN stores s ON o.store_id = s.id";
	let logical = SqlPlanner::new()
		.plan(sql)
		.expect("plan projected non-optimal star");

	let scoped = Binder::new(&catalog, &auto_broadcast_config(64 * 1024 * 1024))
		.bind_plan(&logical)
		.await
		.expect("bind projected star");

	assert_eq!(
		root_project_column_ordinals(&scoped.plan),
		vec![ColumnOrdinal(4), ColumnOrdinal(5), ColumnOrdinal(6)]
	);
}

#[tokio::test]
async fn star_order_evidence_is_explain_gated() {
	let catalog = star_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&non_optimal_star_order_sql())
		.expect("plan star SQL");

	let (_plan, trace) =
		bind_with_trace(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind normal trace");
	let trace_debug = format!("{trace:?}");
	assert!(!trace_debug.contains("JoinOrderEdgeEvidence"));
	assert!(!trace_debug.contains("StarOrderOutcome"));

	let (_plan, _trace, render_trace) =
		bind_with_explain_trace(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind explain trace");
	assert_eq!(render_trace.join_order_edges.len(), 3);
	assert!(matches!(
		render_trace.join_order,
		JoinOrderRenderState::StarReceipts(_)
	));
}

#[test]
fn verdict_indeterminate_on_missing_actuals() {
	let evidence = recognized_star_order_outcome(
		"o".to_string(),
		vec![
			JoinOrderDimEvidence {
				dimension: "c".to_string(),
				declared_position: 0,
				estimated_rows: RowCount::new(1_000),
				estimate_source: JoinOrderEstimateSource::SingleKeyNdvContainment,
				confidence: JoinOrderEstimateConfidence::Hard,
			},
			JoinOrderDimEvidence {
				dimension: "p".to_string(),
				declared_position: 1,
				estimated_rows: RowCount::new(100),
				estimate_source: JoinOrderEstimateSource::SingleKeyNdvContainment,
				confidence: JoinOrderEstimateConfidence::Hard,
			},
		],
		Some(vec!["p".to_string(), "c".to_string()]),
		StarOrderStrategy::Broadcast,
		Vec::new(),
		JoinOrderEstimateConfidence::Hard,
		CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP.margin(),
	);
	let StarOrderOutcome::Recognized(evidence) = evidence else {
		panic!("recognized fixture");
	};

	let verdict = measured_star_order_verdict(
		&evidence,
		&BTreeMap::from([("c".to_string(), RowCount::new(10))]),
	);

	assert_eq!(verdict.verdict, StarOrderVerdict::Indeterminate);
	assert_eq!(
		verdict.recommendation,
		StarOrderVerdictRecommendation::NoRecommendation
	);
	assert!(verdict.measured_evidence.is_none());
	assert!(verdict.measured_applied_order.is_empty());
	assert_eq!(
		verdict
			.accuracy
			.iter()
			.filter(|accuracy| accuracy.actual_rows.is_none())
			.map(|accuracy| accuracy.dimension.as_str())
			.collect::<Vec<_>>(),
		vec!["p"]
	);
}

#[test]
fn measured_order_rerank_preserves_180_measured_order_semantics() {
	let ranking = rank_star_order(
		"o".to_string(),
		vec![
			snowflake_verdict_dim("c", 0, 100),
			snowflake_verdict_dim("s", 1, 10),
		],
		JoinOrderEstimateConfidence::Hard,
		CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP.margin(),
	);
	let rerank = measured_order_rerank(
		&ranking,
		&BTreeMap::from([
			("c".to_string(), RowCount::new(100)),
			("s".to_string(), RowCount::new(1)),
		]),
	);

	let measured = rerank.measured_ranking.expect("complete measurement");
	assert_eq!(measured.action, StarOrderAction::Reordered);
	assert_eq!(rerank.measured_applied_order, vec!["s", "c"]);
	assert_eq!(
		rerank
			.accuracy
			.iter()
			.map(|accuracy| (
				accuracy.dimension.as_str(),
				accuracy.actual_rows.map(RowCount::get)
			))
			.collect::<Vec<_>>(),
		vec![("c", Some(100)), ("s", Some(1))]
	);
}

#[test]
fn measured_snowflake_order_verdict_aggregates_worst_case() {
	let evidence = snowflake_verdict_fixture();
	let missed_branch = measured_snowflake_order_verdict(
		&evidence,
		&BTreeMap::from([
			("c".to_string(), RowCount::new(1)),
			("s".to_string(), RowCount::new(100)),
			("r".to_string(), RowCount::new(100)),
			("g".to_string(), RowCount::new(1)),
		]),
	);

	assert_eq!(missed_branch.levels.len(), 2);
	assert_eq!(missed_branch.levels[0].probe, "o");
	assert_eq!(
		missed_branch.levels[0].verdict,
		SnowflakeOrderVerdictKind::Vindicated
	);
	assert_eq!(missed_branch.levels[1].probe, "c");
	assert_eq!(
		missed_branch.levels[1].verdict,
		SnowflakeOrderVerdictKind::MissedWin
	);
	assert_eq!(
		missed_branch.aggregate,
		SnowflakeOrderVerdictKind::MissedWin
	);
	assert_eq!(
		missed_branch.recommendation,
		SnowflakeOrderRecommendation::WouldReorder
	);

	let all_vindicated = measured_snowflake_order_verdict(
		&evidence,
		&BTreeMap::from([
			("c".to_string(), RowCount::new(1)),
			("s".to_string(), RowCount::new(100)),
			("r".to_string(), RowCount::new(1)),
			("g".to_string(), RowCount::new(100)),
		]),
	);
	assert_eq!(
		all_vindicated.aggregate,
		SnowflakeOrderVerdictKind::Vindicated
	);
	assert_eq!(
		all_vindicated.recommendation,
		SnowflakeOrderRecommendation::KeepDeclared
	);

	let missing = measured_snowflake_order_verdict(
		&evidence,
		&BTreeMap::from([
			("c".to_string(), RowCount::new(1)),
			("s".to_string(), RowCount::new(100)),
			("r".to_string(), RowCount::new(1)),
		]),
	);
	assert_eq!(missing.aggregate, SnowflakeOrderVerdictKind::Indeterminate);
	assert_eq!(
		missing.recommendation,
		SnowflakeOrderRecommendation::NoRecommendation
	);
}

#[test]
fn snowflake_verdict_type_excludes_star_actuation_only_outcomes() {
	let labels = [
		SnowflakeOrderVerdictKind::Vindicated,
		SnowflakeOrderVerdictKind::MissedWin,
		SnowflakeOrderVerdictKind::Indeterminate,
	]
	.iter()
	.map(|verdict| format!("{verdict:?}"))
	.collect::<Vec<_>>();

	assert_eq!(labels, vec!["Vindicated", "MissedWin", "Indeterminate"]);
	assert!(!labels.iter().any(|label| label == "MisRanked"));
	assert!(!labels.iter().any(|label| label == "OverEager"));
}

#[tokio::test]
async fn rune_explain_shows_star_order_evidence() {
	let catalog = star_join_order_catalog();
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o")
               .auto_join(
                   ctx.table("customers").alias("c"),
                   qcol("o", "customer_id").eq(qcol("c", "id"))
               )
               .auto_join(
                   ctx.table("products").alias("p"),
                   qcol("o", "product_id").eq(qcol("p", "id"))
               )
               .auto_join(
                   ctx.table("stores").alias("s"),
                   qcol("o", "store_id").eq(qcol("s", "id"))
               )
               .explain()
        }
    "#,
	)
	.await;

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let row = join_order_row(&rows);

	assert_eq!(row[0], ScalarValue::Utf8(Some("join_order".to_string())));
	assert_eq!(row[2], ScalarValue::Utf8(Some("reordered".to_string())));
	assert_utf8_contains(&row[3], "cost_preferred_order=[s,p,c]");
	assert_utf8_contains(&row[3], "applied_order=[s,p,c]");
}

#[tokio::test]
async fn rune_explain_shows_snowflake_order_evidence() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	// ADR 193: a non-scan-linear Rune probe (`.sort(...)` on the fact table) keeps the snowflake on
	// the observe-only path — Rune is the primary surface for the observed-snowflake evidence.
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o").sort([qcol("o", "id").asc()])
               .auto_join(
                   ctx.table("customers").alias("c"),
                   qcol("o", "customer_id").eq(qcol("c", "id"))
               )
               .auto_join(
                   ctx.table("regions").alias("r"),
                   qcol("c", "region_id").eq(qcol("r", "id"))
               )
               .auto_join(
                   ctx.table("segments").alias("g"),
                   qcol("c", "segment_id").eq(qcol("g", "id"))
               )
               .auto_join(
                   ctx.table("stores").alias("s"),
                   qcol("o", "store_id").eq(qcol("s", "id"))
               )
               .explain()
        }
    "#,
	)
	.await;

	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let row = join_order_snowflake_row(&rows);

	assert_eq!(
		row[0],
		ScalarValue::Utf8(Some("join_order_snowflake".to_string()))
	);
	assert_utf8_contains(&row[3], "root=o");
	assert_utf8_contains(&row[3], "root=c");
	assert_utf8_contains(&row[3], "cost_preferred_order=[s,c]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[g,r]");
}

#[tokio::test]
async fn rune_hash_snowflake_root_reorders_same_as_sql() {
	let catalog = hash_snowflake_join_order_catalog();
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o")
               .auto_join(
                   ctx.table("customers").alias("c"),
                   qcol("o", "customer_id").eq(qcol("c", "id"))
               )
               .auto_join(
                   ctx.table("regions").alias("r"),
                   qcol("c", "region_id").eq(qcol("r", "id"))
               )
               .auto_join(
                   ctx.table("segments").alias("g"),
                   qcol("c", "segment_id").eq(qcol("g", "id"))
               )
               .auto_join(
                   ctx.table("stores").alias("s"),
                   qcol("o", "store_id").eq(qcol("s", "id"))
               )
               .explain()
        }
    "#,
	)
	.await;

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &auto_broadcast_config(1)).await;
	let row = join_order_row(&rows);

	assert_eq!(row[0], ScalarValue::Utf8(Some("join_order".to_string())));
	assert_eq!(row[2], ScalarValue::Utf8(Some("reordered".to_string())));
	assert_utf8_contains(&row[3], "strategy=hash_partitioned");
	assert_utf8_contains(&row[3], "declared_order=[c,s]");
	assert_utf8_contains(&row[3], "cost_preferred_order=[s,c]");
	assert_utf8_contains(&row[3], "applied_order=[s,c]");
}

#[tokio::test]
async fn rune_analyze_renders_star_order_verdict() {
	let catalog = star_join_order_catalog();
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o")
               .auto_join(
                   ctx.table("customers").alias("c"),
                   qcol("o", "customer_id").eq(qcol("c", "id"))
               )
               .auto_join(
                   ctx.table("products").alias("p"),
                   qcol("o", "product_id").eq(qcol("p", "id"))
               )
               .auto_join(
                   ctx.table("stores").alias("s"),
                   qcol("o", "store_id").eq(qcol("s", "id"))
               )
               .analyze()
        }
	"#,
	)
	.await;
	let LogicalPlan::ExplainAnalyze { input } = logical else {
		panic!("expected Rune analyze logical plan");
	};
	let binding = bind_explain_analyze(&input, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
		.await
		.expect("bind Rune analyze star");
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::Broadcast,
			build_rows: RowCount::new(4),
			probe_rows: RowCount::new(4),
			output_rows: RowCount::new(4),
		})
		.collect::<Vec<_>>();

	let plan = render_explain_analyze(binding, &observations).expect("render Rune analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};
	let verdict = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_verdict".to_string())))
		.expect("Rune analyze verdict row");

	assert_eq!(
		verdict[2],
		ScalarValue::Utf8(Some("over_eager".to_string()))
	);
	assert_utf8_contains(&verdict[3], "recommendation=WOULD_KEEP_DECLARED");
}

#[tokio::test]
async fn rune_analyze_shows_snowflake_verdict() {
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	// ADR 193: a non-scan-linear Rune probe (`.sort(...)`) keeps the snowflake observed, not actuated.
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders").alias("o").sort([qcol("o", "id").asc()])
               .auto_join(
                   ctx.table("customers").alias("c"),
                   qcol("o", "customer_id").eq(qcol("c", "id"))
               )
               .auto_join(
                   ctx.table("regions").alias("r"),
                   qcol("c", "region_id").eq(qcol("r", "id"))
               )
               .auto_join(
                   ctx.table("segments").alias("g"),
                   qcol("c", "segment_id").eq(qcol("g", "id"))
               )
               .auto_join(
                   ctx.table("stores").alias("s"),
                   qcol("o", "store_id").eq(qcol("s", "id"))
               )
               .analyze()
        }
	"#,
	)
	.await;
	let LogicalPlan::ExplainAnalyze { input } = logical else {
		panic!("expected Rune analyze logical plan");
	};
	let binding = bind_explain_analyze(&input, &catalog, &auto_broadcast_config(4_096))
		.await
		.expect("bind Rune analyze snowflake");
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(5),
			output_rows: RowCount::new(7),
		})
		.collect::<Vec<_>>();

	let plan = render_explain_analyze(binding, &observations).expect("render Rune analyze");
	let ExecutionPlan::SystemTable { rows, .. } = plan else {
		panic!("expected analyze system table");
	};

	assert!(
		rows.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string())))
	);
	assert_eq!(
		rows.iter()
			.filter(
				|row| row[0] == ScalarValue::Utf8(Some("join_order_snowflake_runtime".to_string()))
			)
			.count(),
		4
	);
	assert_eq!(
		rows.iter()
			.filter(
				|row| row[0] == ScalarValue::Utf8(Some("join_order_snowflake_verdict".to_string()))
			)
			.count(),
		3
	);
	assert!(
		!rows
			.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_verdict".to_string())))
	);
}

#[tokio::test]
async fn explain_result_schema_is_stable() {
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM orders")
		.expect("plan EXPLAIN scan");

	let (schema, rows) =
		bind_explain_rows(&logical, &join_catalog(), &PlannerConfig::default()).await;

	assert!(rows.is_empty());
	assert_eq!(
		schema
			.fields()
			.iter()
			.map(|field| (
				field.name().as_str(),
				field.data_type(),
				field.is_nullable()
			))
			.collect::<Vec<_>>(),
		vec![
			("decision_kind", &DataType::Utf8, false),
			("target", &DataType::Utf8, false),
			("outcome", &DataType::Utf8, false),
			("detail", &DataType::Utf8, true),
			("predicate_text", &DataType::Utf8, true),
			("pushed_predicate_text", &DataType::Utf8, true),
			("residual_predicate_text", &DataType::Utf8, true),
			("estimated_rows", &DataType::Int64, true),
			("spent_fact", &DataType::Utf8, true),
			("spent_column", &DataType::Utf8, true),
			("spent_column_name", &DataType::Utf8, true),
			("build_side", &DataType::Utf8, true),
			("build_bytes", &DataType::Int64, true),
			("threshold_bytes", &DataType::Int64, true),
			("rejected_side_bytes", &DataType::Int64, true),
		]
	);
}

#[tokio::test]
async fn explain_reports_null_rejected_outer_join_demotion() {
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT * FROM orders o LEFT JOIN customers c \
			 ON o.customer_id = c.id WHERE c.region = 'north'",
		)
		.expect("plan EXPLAIN demoted outer join");

	let (_schema, rows) =
		bind_explain_rows(&logical, &join_catalog(), &PlannerConfig::default()).await;
	let row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("outer_join_demotion".to_string())))
		.expect("outer join demotion receipt row");

	assert_eq!(row[1], ScalarValue::Utf8(Some("prebind".to_string())));
	assert_eq!(
		row[2],
		ScalarValue::Utf8(Some("left_outer->inner".to_string()))
	);
	assert_utf8_contains(&row[3], "null_rejecting_filter:");
	assert_utf8_contains(&row[3], "region");
	assert_utf8_contains(&row[4], "region");
	assert_eq!(row[5], ScalarValue::Utf8(None));
	assert_eq!(row[6], ScalarValue::Utf8(None));
}

#[tokio::test]
async fn scan_filter_receipt_records_pushdown_outcome() {
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM customers WHERE region = 'north'")
		.expect("plan EXPLAIN pushed scan");

	let (_schema, rows) =
		bind_explain_rows(&logical, &join_catalog(), &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0][0],
		ScalarValue::Utf8(Some("scan_filter".to_string()))
	);
	assert_eq!(
		rows[0][1],
		ScalarValue::Utf8(Some("scan[0] table_id=2".to_string()))
	);
	assert_eq!(
		rows[0][2],
		ScalarValue::Utf8(Some("fully_pushed".to_string()))
	);
	assert_eq!(rows[0][3], ScalarValue::Utf8(Some("scan".to_string())));
	assert_utf8_contains(&rows[0][4], "region");
	assert_utf8_contains(&rows[0][5], "region");
	assert_eq!(rows[0][6], ScalarValue::Utf8(None));
}

#[tokio::test]
async fn explain_scan_filter_renders_original_pushed_and_residual_predicates() {
	let catalog = converted_unit_catalog(false);
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT * FROM customers \
				 WHERE CONVERT(distance_mile, Kilometer) > CAST(10.0 AS Kilometer)",
		)
		.expect("plan EXPLAIN converted pushed scan");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0][1],
		ScalarValue::Utf8(Some("scan[0] table_id=44".to_string()))
	);
	assert_eq!(
		rows[0][2],
		ScalarValue::Utf8(Some("partially_pushed:converted".to_string()))
	);
	assert_utf8_contains(&rows[0][4], "distance_mile");
	assert_utf8_contains(&rows[0][5], "distance_mile");
	assert_utf8_contains(&rows[0][6], "distance_mile");
}

#[tokio::test]
async fn converted_pushdown_predicate_text_shows_source_column() {
	let catalog = converted_unit_catalog(false);
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT * FROM customers \
				 WHERE CONVERT(distance_mile, Kilometer) > CAST(10.0 AS Kilometer)",
		)
		.expect("plan EXPLAIN converted pushed scan");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_utf8_contains(&rows[0][4], "distance_mile");
	assert_utf8_contains(&rows[0][5], "distance_mile");
	assert_utf8_contains(&rows[0][6], "distance_mile");
}

#[tokio::test]
async fn explain_scan_filter_renders_spent_column_name() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(
		Some(1_000),
		vec![(ColumnOrdinal(1), range_fact_i64(0, 99, 1_000))],
	);
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM customers WHERE amount > 49")
		.expect("plan EXPLAIN range scan");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0][7], ScalarValue::Int64(Some(500)));
	assert_eq!(rows[0][8], ScalarValue::Utf8(Some("range".to_string())));
	assert_eq!(rows[0][9], ScalarValue::Utf8(Some("col[1]".to_string())));
	assert_eq!(rows[0][10], ScalarValue::Utf8(Some("amount".to_string())));
}

#[tokio::test]
async fn scan_filter_receipt_records_converted_selectivity_estimate() {
	let catalog = converted_unit_catalog(true);
	let logical = SqlPlanner::new()
		.plan(
			"EXPLAIN SELECT * FROM customers \
				 WHERE CONVERT(distance_mile, Kilometer) > CAST(10.0 AS Kilometer)",
		)
		.expect("plan EXPLAIN converted range scan");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0][7], ScalarValue::Int64(Some(938)));
	assert_eq!(
		rows[0][8],
		ScalarValue::Utf8(Some("converted_range".to_string()))
	);
	assert_eq!(rows[0][9], ScalarValue::Utf8(Some("col[1]".to_string())));
	assert_eq!(
		rows[0][10],
		ScalarValue::Utf8(Some("distance_mile".to_string()))
	);
}

#[tokio::test]
async fn scan_filter_receipt_records_residual_only() {
	let catalog = descriptor_sidecar_join_catalog_with_customer_range(None, Vec::new());
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM customers WHERE amount + 1 > 5")
		.expect("plan EXPLAIN residual scan");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog, &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0][2],
		ScalarValue::Utf8(Some("residual_only".to_string()))
	);
	assert_eq!(rows[0][7], ScalarValue::Int64(None));
	assert_utf8_contains(&rows[0][4], "amount");
	assert_eq!(rows[0][5], ScalarValue::Utf8(None));
	assert_utf8_contains(&rows[0][6], "amount");
}

#[tokio::test]
async fn explain_scan_filter_static_elision_renders_original_predicate() {
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM customers WHERE FALSE")
		.expect("plan EXPLAIN static-elided scan");

	let (_schema, rows) =
		bind_explain_rows(&logical, &join_catalog(), &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0][2],
		ScalarValue::Utf8(Some("static_elided".to_string()))
	);
	assert_eq!(rows[0][3], ScalarValue::Utf8(Some("scan".to_string())));
	assert_utf8_contains(&rows[0][4], "Boolean(Some(false))");
	assert_eq!(rows[0][5], ScalarValue::Utf8(None));
	assert_eq!(rows[0][6], ScalarValue::Utf8(None));
}

#[tokio::test]
async fn scan_filter_receipt_records_point_read() {
	let logical = SqlPlanner::new()
		.plan("EXPLAIN SELECT * FROM sales WHERE id = 7")
		.expect("plan EXPLAIN point-read scan");

	let (_schema, rows) = bind_explain_rows(&logical, &catalog(), &PlannerConfig::default()).await;

	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0][1],
		ScalarValue::Utf8(Some("scan[0] table_id=1".to_string()))
	);
	assert_eq!(
		rows[0][2],
		ScalarValue::Utf8(Some("point_read".to_string()))
	);
	assert_eq!(
		rows[0][3],
		ScalarValue::Utf8(Some("point_read".to_string()))
	);
}

#[test]
fn ctx_sql_explain_is_read_only_relation() {
	let logical = SqlPlanner::new()
		.plan_relation("EXPLAIN SELECT * FROM orders")
		.expect("ctx.sql accepts read-only EXPLAIN");

	assert!(matches!(logical, LogicalPlan::Explain { .. }));
}

#[test]
fn ctx_sql_explain_analyze_is_read_only_relation() {
	let logical = SqlPlanner::new()
		.plan_relation("EXPLAIN ANALYZE SELECT * FROM orders")
		.expect("ctx.sql accepts read-only EXPLAIN ANALYZE");

	assert!(matches!(logical, LogicalPlan::ExplainAnalyze { .. }));
}

#[test]
fn ctx_sql_explain_rejects_mutation_explain() {
	let error = SqlPlanner::new()
		.plan_relation("EXPLAIN INSERT INTO orders (id, customer_id) VALUES (1, 2)")
		.expect_err("ctx.sql rejects EXPLAIN over mutation");

	assert!(
		error
			.to_string()
			.contains("EXPLAIN supports SELECT queries only"),
		"{error}"
	);
}

#[test]
fn sql_explain_accepts_only_default_query_form() {
	let planner = SqlPlanner::new();
	for sql in [
		"DESC SELECT * FROM orders",
		"DESCRIBE SELECT * FROM orders",
		"EXPLAIN VERBOSE SELECT * FROM orders",
		"EXPLAIN QUERY PLAN SELECT * FROM orders",
		"EXPLAIN ESTIMATE SELECT * FROM orders",
		"EXPLAIN (ANALYZE TRUE) SELECT * FROM orders",
		"EXPLAIN FORMAT JSON SELECT * FROM orders",
		"EXPLAIN TABLE orders",
		"EXPLAIN EXPLAIN SELECT * FROM orders",
		"EXPLAIN INSERT INTO orders (id, customer_id) VALUES (1, 2)",
	] {
		assert!(planner.plan(sql).is_err(), "{sql} must be rejected");
	}
	assert!(planner.plan("EXPLAIN SELECT * FROM orders").is_ok());
	assert!(matches!(
		planner.plan("EXPLAIN ANALYZE SELECT * FROM orders"),
		Ok(LogicalPlan::ExplainAnalyze { .. })
	));
}

#[test]
fn inferred_broadcast_carries_reason_receipt() {
	let config = auto_broadcast_config(4_096);
	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(1_024)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: None,
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&config,
	)
	.expect("choose join strategy");

	assert_eq!(decision.strategy, JoinStrategy::Broadcast);
	assert_eq!(decision.placement, JoinPhysicalPlacement::Natural);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side: BroadcastBuildSide::Left,
			build_bytes: ByteCount::new(1_024),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: None,
		}
	);

	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: None,
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(1_024)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&config,
	)
	.expect("choose join strategy");
	assert_eq!(decision.strategy, JoinStrategy::Broadcast);
	assert_eq!(decision.placement, JoinPhysicalPlacement::SwappedRightBuild);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side: BroadcastBuildSide::Right,
			build_bytes: ByteCount::new(1_024),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: None,
		}
	);
}

#[test]
fn sole_eligible_side_reports_no_rejected_bytes() {
	let config = auto_broadcast_config(4_096);
	for (left_bytes, right_bytes, expected_side, expected_placement) in [
		(
			Some(1_024),
			None,
			BroadcastBuildSide::Left,
			JoinPhysicalPlacement::Natural,
		),
		(
			None,
			Some(1_024),
			BroadcastBuildSide::Right,
			JoinPhysicalPlacement::SwappedRightBuild,
		),
	] {
		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: left_bytes.map(ByteCount::new),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: right_bytes.map(ByteCount::new),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			&config,
		)
		.expect("choose join strategy");

		assert_eq!(decision.strategy, JoinStrategy::Broadcast);
		assert_eq!(decision.placement, expected_placement);
		match decision.reason {
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side,
				build_bytes,
				threshold,
				rejected_side_bytes,
			} => {
				assert_eq!(build_side, expected_side);
				assert_eq!(build_bytes, ByteCount::new(1_024));
				assert_eq!(threshold, ByteCount::new(4_096));
				assert_eq!(rejected_side_bytes, None);
			}
			other => panic!("expected inferred broadcast reason, got {other:?}"),
		}
	}
}

#[test]
fn hard_byte_bound_still_wins_over_estimate() {
	let config = auto_broadcast_config(4_096);
	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(8_192)),
			estimated_byte_size: Some(ByteCount::new(1_024)),
			estimated_row_count: None,
			row_upper_bound: Some(RowCount::new(1)),
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(8_192)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&config,
	)
	.expect("choose join strategy");

	assert_eq!(decision.strategy, JoinStrategy::HashPartitioned);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::BuildByteUpperBoundAboveThreshold,
		}
	);

	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(2_048)),
			estimated_byte_size: Some(ByteCount::new(512)),
			estimated_row_count: None,
			row_upper_bound: Some(RowCount::new(1)),
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(8_192)),
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}),
		&config,
	)
	.expect("choose join strategy");
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side: BroadcastBuildSide::Left,
			build_bytes: ByteCount::new(2_048),
			threshold: ByteCount::new(4_096),
			rejected_side_bytes: None,
		}
	);
}

#[test]
fn estimate_above_effective_threshold_hashes() {
	let config = auto_broadcast_config(4_096);
	let decision = choose_join_strategy(
		JoinReplayId::SYNTHETIC,
		JoinStrategyIntent::Auto,
		JoinKind::Inner,
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: None,
			estimated_byte_size: Some(ByteCount::new(8_192)),
			estimated_row_count: None,
			row_upper_bound: Some(RowCount::new(100)),
		}),
		join_input(PlanOptimizerFacts {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: None,
			estimated_byte_size: Some(ByteCount::new(8_192)),
			estimated_row_count: None,
			row_upper_bound: Some(RowCount::new(100)),
		}),
		&config,
	)
	.expect("choose join strategy");

	assert_eq!(decision.strategy, JoinStrategy::HashPartitioned);
	assert_eq!(
		decision.reason,
		JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::BuildByteUpperBoundAboveThreshold,
		}
	);
}

#[tokio::test]
async fn take_limited_dim_still_hashes() {
	let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(12));
	let taken_customers = LogicalPlan::Take {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		limit: 1,
	};
	let scoped = Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&taken_customers)
		.await
		.expect("bind limited dimension");
	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.byte_upper_bound, None);
	assert_eq!(scoped.facts.estimated_byte_size, None);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(1)));

	let logical = LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		right: Box::new(taken_customers),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let plan = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind take-limited dimension join");

	assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
}

#[tokio::test]
async fn take_or_empty_relation_seeds_row_bound_without_dispatch_change() {
	let catalog = join_catalog();
	let config = PlannerConfig::default();
	let take = LogicalPlan::Take {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		limit: 0,
	};
	let scoped = Binder::new(&catalog, &config)
		.bind_plan(&take)
		.await
		.expect("bind take");
	assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(0)));

	let empty = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate: Expr::Literal(ScalarValue::Boolean(Some(false))),
	};
	let scoped = Binder::new(&catalog, &config)
		.bind_plan(&empty)
		.await
		.expect("bind empty");
	assert!(matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
	assert_eq!(scoped.facts.byte_upper_bound, Some(ByteCount::ZERO));
	assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(0)));
}

fn catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	catalog
}

async fn bind_sql_scoped(catalog: &dyn Catalog, sql: &str) -> ScopedExecutionPlan {
	let logical = SqlPlanner::new().plan(sql).expect("plan SQL");
	let config = PlannerConfig::default();
	Binder::new(catalog, &config)
		.bind_plan(&logical)
		.await
		.expect(sql)
}

fn converted_unit_catalog(seed_range: bool) -> MockCatalog {
	let mut catalog = MockCatalog::new();
	register_logical_type(&catalog, mile_definition());
	register_logical_type(&catalog, kilometer_definition());
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		field_with_logical_type_metadata(
			"distance_mile",
			DataType::Float64,
			true,
			mile_logical_type(),
		),
	]);
	catalog
		.register_descriptor(TableId(44), named_descriptor(44, "customers", schema))
		.expect("register converted-unit descriptor");
	if seed_range {
		catalog
			.seed_optimizer_facts(
				&TableRef::new("customers"),
				TableOptimizerFacts {
					byte_upper_bound: None,
					row_upper_bound: Some(RowCount::new(1_000)),
					column_ndv: Default::default(),
					column_range: [(ColumnOrdinal(1), range_fact_f64(0.0, 100.0, 1_000))]
						.into_iter()
						.collect(),
					column_range_distributions: Default::default(),
					column_heavy_hitters: Default::default(),
					column_group_top_k: Default::default(),
					column_group_ndv: Default::default(),
				},
			)
			.expect("seed converted-unit range fact");
	}
	catalog
}

fn point_unit_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	register_logical_type(&catalog, celsius_definition());
	register_logical_type(&catalog, fahrenheit_definition());
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		field_with_logical_type_metadata("temp_c", DataType::Float64, true, celsius_logical_type()),
	]);
	catalog
		.register_descriptor(TableId(45), named_descriptor(45, "temperatures", schema))
		.expect("register point-unit descriptor");
	catalog
}

fn register_logical_type(catalog: &MockCatalog, definition: LogicalTypeDefinition) {
	catalog
		.register_logical_type_for_tenant(crate::storage::TenantId(1), definition)
		.expect("register logical type");
}

fn mile_logical_type() -> LogicalTypeId {
	LogicalTypeId(crate::ir::DURABLE_LOGICAL_TYPE_MIN_ID)
}

fn kilometer_logical_type() -> LogicalTypeId {
	LogicalTypeId(crate::ir::DURABLE_LOGICAL_TYPE_MIN_ID + 1)
}

fn celsius_logical_type() -> LogicalTypeId {
	LogicalTypeId(crate::ir::DURABLE_LOGICAL_TYPE_MIN_ID + 10)
}

fn fahrenheit_logical_type() -> LogicalTypeId {
	LogicalTypeId(crate::ir::DURABLE_LOGICAL_TYPE_MIN_ID + 11)
}

fn mile_definition() -> LogicalTypeDefinition {
	delta_unit_definition(
		mile_logical_type(),
		"Mile",
		UnitScale {
			numerator: 201_168,
			denominator: 125,
		},
	)
}

fn kilometer_definition() -> LogicalTypeDefinition {
	delta_unit_definition(
		kilometer_logical_type(),
		"Kilometer",
		UnitScale {
			numerator: 1_000,
			denominator: 1,
		},
	)
}

fn delta_unit_definition(
	id: LogicalTypeId,
	name: &str,
	scale_to_base: UnitScale,
) -> LogicalTypeDefinition {
	LogicalTypeDefinition {
		logical_type_id: id.0,
		name: name.to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Length".to_string(),
			scale_to_base,
			quantity_kind: UnitQuantityKind::Delta,
			offset_to_base: UnitOffset::ZERO,
			delta_unit: None,
		}),
	}
}

fn celsius_definition() -> LogicalTypeDefinition {
	point_unit_definition(
		celsius_logical_type(),
		"Celsius",
		UnitScale {
			numerator: 1,
			denominator: 1,
		},
		UnitOffset {
			numerator: 5_463,
			denominator: 20,
		},
	)
}

fn fahrenheit_definition() -> LogicalTypeDefinition {
	point_unit_definition(
		fahrenheit_logical_type(),
		"Fahrenheit",
		UnitScale {
			numerator: 5,
			denominator: 9,
		},
		UnitOffset {
			numerator: 45_967,
			denominator: 180,
		},
	)
}

fn point_unit_definition(
	id: LogicalTypeId,
	name: &str,
	scale_to_base: UnitScale,
	offset_to_base: UnitOffset,
) -> LogicalTypeDefinition {
	LogicalTypeDefinition {
		logical_type_id: id.0,
		name: name.to_string(),
		physical: LogicalTypePhysical::Float64,
		refinement: None,
		unit: Some(LogicalTypeUnit {
			dimension: "Temperature".to_string(),
			scale_to_base,
			quantity_kind: UnitQuantityKind::Point,
			offset_to_base,
			delta_unit: None,
		}),
	}
}

fn first_filter_scan_state(plan: &ExecutionPlan) -> Option<(FilterDisposition, bool)> {
	match plan {
		ExecutionPlan::Filter {
			input, disposition, ..
		} => Some((*disposition, scan_has_pbc(input))),
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => first_filter_scan_state(input),
		ExecutionPlan::Join { left, right, .. } => {
			first_filter_scan_state(left).or_else(|| first_filter_scan_state(right))
		}
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => first_filter_scan_state(source),
		_ => None,
	}
}

fn scan_has_pbc(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Scan { pbc_program, .. } => pbc_program.is_some(),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => scan_has_pbc(input),
		ExecutionPlan::Join { left, right, .. } => scan_has_pbc(left) || scan_has_pbc(right),
		ExecutionPlan::CreateTableAsSelect { source, .. }
		| ExecutionPlan::Update { source, .. }
		| ExecutionPlan::InsertSelect { source, .. } => scan_has_pbc(source),
		_ => false,
	}
}

fn auto_broadcast_config(max_build_bytes: u64) -> PlannerConfig {
	PlannerConfig {
		auto_broadcast_max_build_bytes: Some(ByteCount::new(max_build_bytes)),
		..PlannerConfig::default()
	}
}

fn seed_byte_upper_bound(catalog: &mut MockCatalog, table: &str, bytes: u64) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: Some(ByteCount::new(bytes)),
				row_upper_bound: None,
				column_ndv: Default::default(),
				column_range: Default::default(),
				column_range_distributions: Default::default(),
				column_heavy_hitters: Default::default(),
				column_group_top_k: Default::default(),
				column_group_ndv: Default::default(),
			},
		)
		.expect("seed optimizer facts");
}

fn seed_table_optimizer_facts(
	catalog: &mut MockCatalog,
	table: &str,
	byte_upper_bound: Option<u64>,
	row_upper_bound: Option<u64>,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: byte_upper_bound.map(ByteCount::new),
				row_upper_bound: row_upper_bound.map(RowCount::new),
				column_ndv: column_ndv.into_iter().collect(),
				column_range: Default::default(),
				column_range_distributions: Default::default(),
				column_heavy_hitters: Default::default(),
				column_group_top_k: Default::default(),
				column_group_ndv: Default::default(),
			},
		)
		.expect("seed optimizer facts");
}

fn seed_table_optimizer_facts_with_ranges(
	catalog: &mut MockCatalog,
	table: &str,
	byte_upper_bound: Option<u64>,
	row_upper_bound: Option<u64>,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: byte_upper_bound.map(ByteCount::new),
				row_upper_bound: row_upper_bound.map(RowCount::new),
				column_ndv: column_ndv.into_iter().collect(),
				column_range: column_range.into_iter().collect(),
				column_range_distributions: Default::default(),
				column_heavy_hitters: Default::default(),
				column_group_top_k: Default::default(),
				column_group_ndv: Default::default(),
			},
		)
		.expect("seed optimizer facts");
}

fn seed_table_optimizer_facts_with_ranges_and_heavy_hitters(
	catalog: &mut MockCatalog,
	table: &str,
	byte_upper_bound: Option<u64>,
	row_upper_bound: Option<u64>,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
	column_heavy_hitters: Vec<(ColumnOrdinal, ColumnHeavyHitterFact)>,
) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: byte_upper_bound.map(ByteCount::new),
				row_upper_bound: row_upper_bound.map(RowCount::new),
				column_ndv: column_ndv.into_iter().collect(),
				column_range: column_range.into_iter().collect(),
				column_range_distributions: Default::default(),
				column_heavy_hitters: column_heavy_hitters.into_iter().collect(),
				column_group_top_k: Default::default(),
				column_group_ndv: Default::default(),
			},
		)
		.expect("seed optimizer facts");
}

fn seed_table_optimizer_facts_with_column_group_top_k(
	catalog: &mut MockCatalog,
	table: &str,
	row_upper_bound: u64,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_group_top_k: Vec<(Vec<ColumnOrdinal>, ColumnGroupTopKFact)>,
) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: None,
				row_upper_bound: Some(RowCount::new(row_upper_bound)),
				column_ndv: column_ndv.into_iter().collect(),
				column_range: Default::default(),
				column_range_distributions: Default::default(),
				column_heavy_hitters: Default::default(),
				column_group_top_k: column_group_top_k.into_iter().collect(),
				column_group_ndv: Default::default(),
			},
		)
		.expect("seed optimizer facts");
}

fn seed_table_optimizer_facts_with_column_group_ndv(
	catalog: &mut MockCatalog,
	table: &str,
	row_upper_bound: u64,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_group_ndv: Vec<(Vec<ColumnOrdinal>, DistinctCountFact)>,
) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: None,
				row_upper_bound: Some(RowCount::new(row_upper_bound)),
				column_ndv: column_ndv.into_iter().collect(),
				column_range: Default::default(),
				column_range_distributions: Default::default(),
				column_heavy_hitters: Default::default(),
				column_group_top_k: Default::default(),
				column_group_ndv: column_group_ndv.into_iter().collect(),
			},
		)
		.expect("seed optimizer facts");
}

fn seed_table_optimizer_facts_with_column_group_top_k_ndv_and_ranges(
	catalog: &mut MockCatalog,
	table: &str,
	row_upper_bound: u64,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
	column_group_top_k: Vec<(Vec<ColumnOrdinal>, ColumnGroupTopKFact)>,
	column_group_ndv: Vec<(Vec<ColumnOrdinal>, DistinctCountFact)>,
) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: None,
				row_upper_bound: Some(RowCount::new(row_upper_bound)),
				column_ndv: column_ndv.into_iter().collect(),
				column_range: column_range.into_iter().collect(),
				column_range_distributions: Default::default(),
				column_heavy_hitters: Default::default(),
				column_group_top_k: column_group_top_k.into_iter().collect(),
				column_group_ndv: column_group_ndv.into_iter().collect(),
			},
		)
		.expect("seed optimizer facts");
}

fn descriptor_sidecar_join_catalog(
	orders_byte_upper_bound: Option<u64>,
	customers_byte_upper_bound: Option<u64>,
	customers_row_upper_bound: Option<u64>,
) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customer_ndv(
		orders_byte_upper_bound,
		customers_byte_upper_bound,
		customers_row_upper_bound,
		Vec::new(),
	)
}

fn descriptor_sidecar_join_catalog_with_customer_ndv(
	orders_byte_upper_bound: Option<u64>,
	customers_byte_upper_bound: Option<u64>,
	customers_row_upper_bound: Option<u64>,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		orders_byte_upper_bound,
		customers_byte_upper_bound,
		customers_row_upper_bound,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]),
		column_ndv,
	)
}

fn descriptor_sidecar_join_catalog_with_customer_range(
	customers_row_upper_bound: Option<u64>,
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		customers_row_upper_bound,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int64, true),
		]),
		Vec::new(),
		column_range,
	)
}

fn descriptor_sidecar_join_catalog_with_customer_range_distribution(
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
	column_range_distributions: Vec<(ColumnOrdinal, ColumnRangeDistributionFact)>,
) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_distributions_and_heavy_hitters(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int64, true),
		]),
		Vec::new(),
		column_range,
		column_range_distributions,
		Vec::new(),
	)
}

fn descriptor_sidecar_join_catalog_with_customer_score_range(range: RangeFact) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("score", DataType::Float64, true),
		]),
		Vec::new(),
		vec![(ColumnOrdinal(1), range)],
	)
}

fn descriptor_sidecar_join_catalog_with_customer_date_range(range: RangeFact) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("signup_date", DataType::Date32, true),
		]),
		Vec::new(),
		vec![(ColumnOrdinal(1), range)],
	)
}

fn descriptor_sidecar_join_catalog_with_customer_timestamp_range(
	range: RangeFact,
) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		Some(64 * 1024 * 1024),
		None,
		None,
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(
				"observed_at",
				crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond),
				true,
			),
		]),
		Vec::new(),
		vec![(ColumnOrdinal(1), range)],
	)
}

fn wide_customer_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int64, true),
		Field::new("region", DataType::Utf8, true),
		Field::new("segment", DataType::Utf8, true),
		Field::new("status", DataType::Utf8, true),
		Field::new("tier", DataType::Utf8, true),
		Field::new("notes", DataType::Utf8, true),
		Field::new("profile", DataType::LargeUtf8, true),
	])
}

fn wide_customer_sql_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("region", DataType::Utf8, true),
		Field::new("segment", DataType::Utf8, true),
		Field::new("status", DataType::Utf8, true),
		Field::new("tier", DataType::Utf8, true),
		Field::new("notes", DataType::Utf8, true),
		Field::new("profile", DataType::LargeUtf8, true),
	])
}

fn descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
	orders_byte_upper_bound: Option<u64>,
	customers_byte_upper_bound: Option<u64>,
	customers_row_upper_bound: Option<u64>,
	customers_schema: Schema,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
		orders_byte_upper_bound,
		customers_byte_upper_bound,
		customers_row_upper_bound,
		customers_schema,
		column_ndv,
		Vec::new(),
	)
}

fn descriptor_sidecar_join_catalog_with_customers_schema_ndv_and_range(
	orders_byte_upper_bound: Option<u64>,
	customers_byte_upper_bound: Option<u64>,
	customers_row_upper_bound: Option<u64>,
	customers_schema: Schema,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_and_heavy_hitters(
		orders_byte_upper_bound,
		customers_byte_upper_bound,
		customers_row_upper_bound,
		customers_schema,
		column_ndv,
		column_range,
		Vec::new(),
	)
}

fn descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_and_heavy_hitters(
	orders_byte_upper_bound: Option<u64>,
	customers_byte_upper_bound: Option<u64>,
	customers_row_upper_bound: Option<u64>,
	customers_schema: Schema,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
	column_heavy_hitters: Vec<(ColumnOrdinal, ColumnHeavyHitterFact)>,
) -> WendelonCatalog {
	descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_distributions_and_heavy_hitters(
		orders_byte_upper_bound,
		customers_byte_upper_bound,
		customers_row_upper_bound,
		customers_schema,
		column_ndv,
		column_range,
		Vec::new(),
		column_heavy_hitters,
	)
}

fn descriptor_sidecar_join_catalog_with_customers_schema_ndv_range_distributions_and_heavy_hitters(
	orders_byte_upper_bound: Option<u64>,
	customers_byte_upper_bound: Option<u64>,
	customers_row_upper_bound: Option<u64>,
	customers_schema: Schema,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
	column_range_distributions: Vec<(ColumnOrdinal, ColumnRangeDistributionFact)>,
	column_heavy_hitters: Vec<(ColumnOrdinal, ColumnHeavyHitterFact)>,
) -> WendelonCatalog {
	let orders = named_descriptor(
		1,
		"orders",
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]),
	);
	let customers = named_descriptor(2, "customers", customers_schema);
	WendelonCatalog::from_descriptors_with_optimizer_facts([
		(
			TableId(1),
			orders.clone(),
			orders_byte_upper_bound.map(|bytes| {
				descriptor_sidecar_facts_with_range_distributions(
					&orders,
					Some(bytes),
					None,
					Vec::new(),
					Vec::new(),
					Vec::new(),
					Vec::new(),
				)
			}),
		),
		(
			TableId(2),
			customers.clone(),
			Some(descriptor_sidecar_facts_with_range_distributions(
				&customers,
				customers_byte_upper_bound,
				customers_row_upper_bound,
				column_ndv,
				column_range,
				column_range_distributions,
				column_heavy_hitters,
			)),
		),
	])
	.expect("descriptor-sidecar catalog")
}

fn descriptor_sidecar_facts_with_range_distributions(
	descriptor: &TableSchemaDescriptor,
	materialized_byte_upper_bound: Option<u64>,
	row_upper_bound: Option<u64>,
	column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	column_range: Vec<(ColumnOrdinal, RangeFact)>,
	column_range_distributions: Vec<(ColumnOrdinal, ColumnRangeDistributionFact)>,
	column_heavy_hitters: Vec<(ColumnOrdinal, ColumnHeavyHitterFact)>,
) -> DescriptorOptimizerFacts {
	DescriptorOptimizerFacts {
		schema_generation: descriptor.schema_generation,
		physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
		manifest_epoch: 7,
		materialized_byte_upper_bound: materialized_byte_upper_bound.map(ByteCount::new),
		row_upper_bound: row_upper_bound.map(RowCount::new),
		column_ndv: column_ndv
			.into_iter()
			.map(|(column_ordinal, distinct_count)| DescriptorColumnNdvFact {
				column_ordinal,
				distinct_count,
			})
			.collect(),
		column_range: column_range
			.into_iter()
			.map(|(column_ordinal, range)| DescriptorColumnRangeFact {
				column_ordinal,
				range,
			})
			.collect(),
		column_range_distributions: column_range_distributions
			.into_iter()
			.map(
				|(column_ordinal, fact)| DescriptorColumnRangeDistributionFact {
					column_ordinal,
					fact,
				},
			)
			.collect(),
		column_heavy_hitters: column_heavy_hitters
			.into_iter()
			.map(
				|(column_ordinal, fact)| crate::catalog::DescriptorColumnHeavyHitterFact {
					column_ordinal,
					fact,
				},
			)
			.collect(),
		column_group_top_k: Default::default(),
		column_group_ndv: Default::default(),
	}
}

fn heavy_hitter_fact(entries: Vec<(ScalarLiteralKey, u64)>) -> ColumnHeavyHitterFact {
	ColumnHeavyHitterFact {
		entries: entries.into_iter().collect(),
	}
}

fn utf8_heavy_hitter_fact(entries: &[(&str, u64)]) -> ColumnHeavyHitterFact {
	heavy_hitter_fact(
		entries
			.iter()
			.map(|(value, frequency)| (ScalarLiteralKey::Utf8((*value).to_string()), *frequency))
			.collect(),
	)
}

fn int64_heavy_hitter_fact(entries: &[(i64, u64)]) -> ColumnHeavyHitterFact {
	heavy_hitter_fact(
		entries
			.iter()
			.map(|(value, frequency)| (ScalarLiteralKey::Int64(*value), *frequency))
			.collect(),
	)
}

fn int64_group_top_k_fact(entries: &[([i64; 2], u64)]) -> ColumnGroupTopKFact {
	ColumnGroupTopKFact {
		arrow_type_tags: vec![
			wendelon_client::ARROW_STATS_TAG_INT64,
			wendelon_client::ARROW_STATS_TAG_INT64,
		],
		entries: entries
			.iter()
			.map(|(values, frequency)| {
				(
					values
						.iter()
						.map(|value| ScalarLiteralKey::Int64(*value))
						.collect::<Vec<_>>(),
					*frequency,
				)
			})
			.collect(),
	}
}

fn range_fact_i64(min: i64, max: i64, non_null_count: u64) -> RangeFact {
	RangeFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT64,
		null_count: 0,
		non_null_count,
		min: Some(wendelon_client::encode_signed_optimizer_fact_scalar(min, 8).unwrap()),
		max: Some(wendelon_client::encode_signed_optimizer_fact_scalar(max, 8).unwrap()),
	}
}

fn range_distribution_fact_i64(rank_error_ppm: u32) -> ColumnRangeDistributionFact {
	ColumnRangeDistributionFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT64,
		non_null_count: 10_000,
		rank_error_ppm,
		buckets: vec![
			range_distribution_bucket_i64(0, 100, 9_000),
			range_distribution_bucket_i64(101, 10_000, 1_000),
		],
	}
}

fn range_distribution_fact_i32(rank_error_ppm: u32) -> ColumnRangeDistributionFact {
	ColumnRangeDistributionFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT32,
		non_null_count: 10_000,
		rank_error_ppm,
		buckets: vec![
			range_distribution_bucket_i32(0, 100, 9_000),
			range_distribution_bucket_i32(101, 10_000, 1_000),
		],
	}
}

fn range_distribution_bucket_i64(lower: i64, upper: i64, count: u64) -> RangeDistributionBucket {
	RangeDistributionBucket {
		lower: wendelon_client::encode_signed_optimizer_fact_scalar(lower, 8).unwrap(),
		upper: wendelon_client::encode_signed_optimizer_fact_scalar(upper, 8).unwrap(),
		count,
	}
}

fn range_distribution_bucket_i32(lower: i32, upper: i32, count: u64) -> RangeDistributionBucket {
	RangeDistributionBucket {
		lower: wendelon_client::encode_signed_optimizer_fact_scalar(i64::from(lower), 4).unwrap(),
		upper: wendelon_client::encode_signed_optimizer_fact_scalar(i64::from(upper), 4).unwrap(),
		count,
	}
}

fn range_fact_i32(min: i32, max: i32, non_null_count: u64) -> RangeFact {
	RangeFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT32,
		null_count: 0,
		non_null_count,
		min: Some(wendelon_client::encode_signed_optimizer_fact_scalar(i64::from(min), 4).unwrap()),
		max: Some(wendelon_client::encode_signed_optimizer_fact_scalar(i64::from(max), 4).unwrap()),
	}
}

fn range_fact_date32(min: i32, max: i32, non_null_count: u64) -> RangeFact {
	RangeFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_DATE32,
		null_count: 0,
		non_null_count,
		min: Some(wendelon_client::encode_signed_optimizer_fact_scalar(i64::from(min), 4).unwrap()),
		max: Some(wendelon_client::encode_signed_optimizer_fact_scalar(i64::from(max), 4).unwrap()),
	}
}

fn range_fact_timestamp_ms(min: i64, max: i64, non_null_count: u64) -> RangeFact {
	RangeFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_TIMESTAMP_MILLISECOND,
		null_count: 0,
		non_null_count,
		min: Some(wendelon_client::encode_signed_optimizer_fact_scalar(min, 8).unwrap()),
		max: Some(wendelon_client::encode_signed_optimizer_fact_scalar(max, 8).unwrap()),
	}
}

fn range_fact_timestamp_ns(min: i64, max: i64, non_null_count: u64) -> RangeFact {
	RangeFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_TIMESTAMP_NANOSECOND,
		null_count: 0,
		non_null_count,
		min: Some(wendelon_client::encode_signed_optimizer_fact_scalar(min, 8).unwrap()),
		max: Some(wendelon_client::encode_signed_optimizer_fact_scalar(max, 8).unwrap()),
	}
}

fn timestamp_ms(value: &str) -> i64 {
	crate::timestamp::parse_explicit_timestamptz(value, TimeUnit::Millisecond)
		.expect("timestamp literal")
}

fn timestamp_ns(value: &str) -> i64 {
	crate::timestamp::parse_explicit_timestamptz(value, TimeUnit::Nanosecond)
		.expect("timestamp literal")
}

fn interval_literal(nanos: i64) -> Expr {
	Expr::Literal(ScalarValue::Interval(Some(nanos)))
}

fn calendar_month_literal(months: i32) -> Expr {
	Expr::Literal(ScalarValue::CalendarMonthInterval(Some(months)))
}

fn temporal_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new(
			"event_time",
			crate::timestamp::utc_timestamp_data_type(TimeUnit::Millisecond),
			true,
		),
		Field::new("signup_date", DataType::Date32, true),
	]))
}

fn temporal_batch(event_time: i64, signup_date: i32) -> RecordBatch {
	RecordBatch::try_new(
		temporal_schema(),
		vec![
			Arc::new(TimestampMillisecondArray::from(vec![Some(event_time)]).with_timezone("UTC"))
				as ArrayRef,
			Arc::new(Date32Array::from(vec![Some(signup_date)])) as ArrayRef,
		],
	)
	.expect("temporal batch")
}

fn substring_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("name", DataType::Utf8, true),
		Field::new("amount", DataType::Int32, true),
	]))
}

fn substring_batch() -> RecordBatch {
	RecordBatch::try_new(
		substring_schema(),
		vec![
			Arc::new(StringArray::from(vec![Some("abcde"), Some("aé日z"), None])) as ArrayRef,
			Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
		],
	)
	.expect("substring batch")
}

fn bind_substring_expr(expr: Expr) -> Result<BoundExprProgram, BindingError> {
	let config = PlannerConfig::default();
	let logical_casts = HashMap::new();
	let logical_type_registry = LogicalTypeRegistry::default();
	let ctx = ExprBindContext {
		config: &config,
		session_timezone: config
			.resolved_session_timezone()
			.expect("session timezone"),
		logical_type_specs: &logical_casts,
		logical_type_registry: &logical_type_registry,
		relation_scope: None,
	};
	bind_expr_program(&[expr], &substring_schema(), &ctx)
}

fn substring_expr(from: i64, length: Option<u64>) -> Expr {
	Expr::Substring {
		input: Box::new(Expr::column("name")),
		from,
		length,
	}
}

fn bind_temporal_expr(expr: Expr, session_timezone: &str) -> BoundExprProgram {
	try_bind_temporal_expr(expr, session_timezone).expect("bind temporal expr")
}

fn try_bind_temporal_expr(
	expr: Expr,
	session_timezone: &str,
) -> Result<BoundExprProgram, BindingError> {
	let config = PlannerConfig::default().with_session_timezone(session_timezone);
	let logical_casts = HashMap::new();
	let logical_type_registry = LogicalTypeRegistry::default();
	let ctx = ExprBindContext {
		config: &config,
		session_timezone: config
			.resolved_session_timezone()
			.expect("session timezone"),
		logical_type_specs: &logical_casts,
		logical_type_registry: &logical_type_registry,
		relation_scope: None,
	};
	bind_expr_program(&[expr], &temporal_schema(), &ctx)
}

#[test]
fn substring_sql_lowers_from_for_and_comma_forms() {
	for (sql, expected) in [
		(
			"SELECT SUBSTRING(c_phone FROM 1 FOR 2) FROM customer",
			(1, Some(2)),
		),
		(
			"SELECT SUBSTRING(c_phone, 3, 4) FROM customer",
			(3, Some(4)),
		),
		("SELECT SUBSTRING(c_phone FROM 5) FROM customer", (5, None)),
	] {
		let plan = SqlPlanner::new().plan(sql).expect("plan substring");
		let LogicalPlan::Project { exprs, .. } = plan else {
			panic!("expected project for {sql}");
		};
		let Expr::Substring {
			input,
			from,
			length,
		} = &exprs[0]
		else {
			panic!("expected substring expression for {sql}: {:?}", exprs[0]);
		};
		assert!(matches!(input.as_ref(), Expr::Column(column) if column.name.0 == "c_phone"));
		assert_eq!((*from, *length), expected, "{sql}");
	}
}

#[test]
fn substring_sql_rejects_unsupported_bounds() {
	for (sql, expected) in [
		(
			"SELECT SUBSTRING(c_phone FROM 0 FOR 2) FROM customer",
			"SUBSTRING FROM must be >= 1",
		),
		(
			"SELECT SUBSTRING(c_phone FROM 1 FOR -1) FROM customer",
			"SUBSTRING FOR must be >= 0",
		),
		(
			"SELECT SUBSTRING(c_phone FROM c_acctbal FOR 2) FROM customer",
			"SUBSTRING FROM expects an integer literal",
		),
		(
			"SELECT SUBSTRING(c_phone FROM 1 FOR c_acctbal) FROM customer",
			"SUBSTRING FOR expects an integer literal",
		),
		(
			"SELECT SUBSTR(c_phone FROM 1 FOR 2) FROM customer",
			"SUBSTR shorthand is not supported",
		),
	] {
		let error = SqlPlanner::new()
			.plan(sql)
			.expect_err("substring shape should reject");
		assert!(
			error.to_string().contains(expected),
			"{sql}: expected {expected:?}, got {error}"
		);
	}
}

#[test]
fn substring_exec_uses_one_based_character_offsets() {
	let program = bind_substring_expr(substring_expr(2, Some(2))).expect("bind substring");
	let outputs = Executor::evaluate_bound_program_without_storage(&program, &substring_batch())
		.expect("evaluate substring");
	let values = outputs[0]
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("substring output");

	assert_eq!(values.value(0), "bc");
	assert_eq!(values.value(1), "é日");
	assert!(values.is_null(2));
	assert_eq!(program.node_type(program.roots[0]), Some(&DataType::Utf8));
}

#[test]
fn substring_without_for_returns_rest_and_for_zero_returns_empty() {
	let rest = bind_substring_expr(substring_expr(3, None)).expect("bind substring rest");
	let empty = bind_substring_expr(substring_expr(3, Some(0))).expect("bind substring empty");
	let batch = substring_batch();
	let rest_outputs = Executor::evaluate_bound_program_without_storage(&rest, &batch)
		.expect("evaluate substring rest");
	let empty_outputs = Executor::evaluate_bound_program_without_storage(&empty, &batch)
		.expect("evaluate substring empty");
	let rest_values = rest_outputs[0]
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("substring rest output");
	let empty_values = empty_outputs[0]
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("substring empty output");

	assert_eq!(rest_values.value(0), "cde");
	assert_eq!(rest_values.value(1), "日z");
	assert_eq!(empty_values.value(0), "");
	assert_eq!(empty_values.value(1), "");
}

#[test]
fn substring_bind_rejects_non_utf8_input() {
	let error = bind_substring_expr(Expr::Substring {
		input: Box::new(Expr::column("amount")),
		from: 1,
		length: Some(2),
	})
	.expect_err("non-Utf8 substring input should reject");

	assert!(error.to_string().contains("SUBSTRING expects Utf8 input"));
}

fn range_fact_f64(min: f64, max: f64, non_null_count: u64) -> RangeFact {
	RangeFact {
		arrow_type_tag: wendelon_client::ARROW_STATS_TAG_FLOAT64,
		null_count: 0,
		non_null_count,
		min: Some(wendelon_client::encode_float64_optimizer_fact_scalar(min).unwrap()),
		max: Some(wendelon_client::encode_float64_optimizer_fact_scalar(max).unwrap()),
	}
}

fn customer_column(name: &str) -> Expr {
	Expr::qualified_column(TableRef::new("customers"), name)
}

fn utf8_literal(value: &str) -> Expr {
	Expr::Literal(ScalarValue::Utf8(Some(value.to_string())))
}

fn int64_literal(value: i64) -> Expr {
	Expr::Literal(ScalarValue::Int64(Some(value)))
}

fn date32_literal(value: i32) -> Expr {
	Expr::Literal(ScalarValue::Date32(Some(value)))
}

fn customer_in_list(column: &str, list: Vec<Expr>, negated: bool) -> Expr {
	Expr::InList {
		expr: Box::new(customer_column(column)),
		list,
		negated,
	}
}

fn customer_amount_between(low: i64, high: i64) -> Expr {
	Expr::And(
		Box::new(Expr::Gte(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(low)))),
		)),
		Box::new(Expr::Lte(
			Box::new(customer_column("amount")),
			Box::new(Expr::Literal(ScalarValue::Int64(Some(high)))),
		)),
	)
}

fn customer_timestamp_between_strings(low: &str, high: &str) -> Expr {
	Expr::And(
		Box::new(Expr::Gte(
			Box::new(customer_column("observed_at")),
			Box::new(utf8_literal(low)),
		)),
		Box::new(Expr::Lte(
			Box::new(customer_column("observed_at")),
			Box::new(utf8_literal(high)),
		)),
	)
}

async fn bind_filtered_customers(
	catalog: &dyn Catalog,
	predicate: Expr,
) -> Result<ScopedExecutionPlan, BindingError> {
	bind_filtered_customers_with_config(catalog, predicate, PlannerConfig::default()).await
}

async fn bind_filtered_customers_with_config(
	catalog: &dyn Catalog,
	predicate: Expr,
	config: PlannerConfig,
) -> Result<ScopedExecutionPlan, BindingError> {
	let filtered_customers = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		predicate,
	};
	Binder::new(catalog, &config)
		.bind_plan(&filtered_customers)
		.await
}

async fn rune_plan(script: &str) -> LogicalPlan {
	let config = RuneSandboxConfig::v1_default();
	let engine = RuneEngine::with_config(config).expect("Rune engine");
	engine
		.execute_script(script, &config)
		.await
		.expect("Rune-authored plan")
}

const RUNE_OPTIMIZER_READOUT_HANDLES: [(&str, &str, SystemTableKind); 8] = [
	(
		"optimizer_decisions",
		"optimizer_decisions",
		SystemTableKind::InformationSchemaOptimizerDecisions,
	),
	(
		"optimizer_runtime_verdicts",
		"optimizer_runtime_verdicts",
		SystemTableKind::InformationSchemaOptimizerRuntimeVerdicts,
	),
	(
		"optimizer_evidence_status",
		"optimizer_evidence_status",
		SystemTableKind::InformationSchemaOptimizerEvidenceStatus,
	),
	(
		"optimizer_margin_advisory",
		"optimizer_margin_advisory",
		SystemTableKind::InformationSchemaOptimizerMarginAdvisory,
	),
	(
		"optimizer_receipt_gates",
		"optimizer_receipt_gates",
		SystemTableKind::InformationSchemaOptimizerReceiptGates,
	),
	(
		"optimizer_decision_conflicts",
		"optimizer_decision_conflicts",
		SystemTableKind::InformationSchemaOptimizerDecisionConflicts,
	),
	(
		"optimizer_memo_admission",
		"optimizer_memo_admission",
		SystemTableKind::InformationSchemaOptimizerMemoAdmission,
	),
	(
		"optimizer_probe_linearity_fences",
		"optimizer_probe_linearity_fences",
		SystemTableKind::InformationSchemaOptimizerProbeLinearityFences,
	),
];

fn optimizer_readout_handle_script(handle: &str) -> String {
	format!("pub fn main(ctx) {{ ctx.{handle}() }}")
}

fn optimizer_readout_sql(table: &str) -> String {
	format!("SELECT * FROM information_schema.{table}")
}

fn assert_optimizer_readout_scan_plan(logical: &LogicalPlan, table: &str) {
	let LogicalPlan::ScanNamed {
		table: actual,
		projection,
	} = logical
	else {
		panic!("expected optimizer readout ScanNamed, got {logical:?}");
	};

	assert_eq!(actual, &TableRef::with_schema("information_schema", table));
	assert_eq!(projection, &None);
}

async fn bind_optimizer_readout(
	logical: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> (SystemTableKind, Vec<Vec<ScalarValue>>) {
	let plan = bind(logical, catalog, config)
		.await
		.expect("bind optimizer readout");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer readout system table, got {plan:?}");
	};
	(kind, rows)
}

fn optimizer_readout_seeded_config() -> PlannerConfig {
	let config = PlannerConfig::default();
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::FlatStar,
		OptimizerRuntimeVerdictProvenance::Native,
	);
	record_conflicted_decisions(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::BridgeRoot,
		OptimizerRuntimeVerdictProvenance::Demoted,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::GalaxyRoot,
		OptimizerRuntimeVerdictClass::MissedWin,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_verdicts(
		config.runtime_evidence.as_ref(),
		OptimizerDecisionKind::SnowflakeRoot,
		OptimizerRuntimeVerdictClass::Vindicated,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	config
}

fn representative_estimate_batches(rows: usize) -> Vec<RecordBatch> {
	let fixed_schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
	]));
	let string_schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("label", DataType::Utf8, true),
	]));
	let binary_schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("payload", DataType::Binary, true),
	]));
	let large_string_schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("description", DataType::LargeUtf8, true),
	]));

	vec![
		RecordBatch::try_new(
			fixed_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0_i64..rows as i64)) as ArrayRef,
				Arc::new(Int32Array::from_iter_values(
					(0..rows).map(|value| value as i32),
				)) as ArrayRef,
			],
		)
		.expect("fixed-width representative batch"),
		RecordBatch::try_new(
			string_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0_i64..rows as i64)) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("north"); rows])) as ArrayRef,
			],
		)
		.expect("Utf8 representative batch"),
		RecordBatch::try_new(
			binary_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0_i64..rows as i64)) as ArrayRef,
				Arc::new(BinaryArray::from_iter_values(vec![b"abc".as_slice(); rows])) as ArrayRef,
			],
		)
		.expect("Binary representative batch"),
		RecordBatch::try_new(
			large_string_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0_i64..rows as i64)) as ArrayRef,
				Arc::new(LargeStringArray::from(vec![Some("north"); rows])) as ArrayRef,
			],
		)
		.expect("LargeUtf8 representative batch"),
	]
}

async fn bind_explain_rows(
	logical: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> (SchemaRef, Vec<Vec<ScalarValue>>) {
	let plan = bind(logical, catalog, config).await.expect("bind EXPLAIN");
	let ExecutionPlan::SystemTable { kind, schema, rows } = plan else {
		panic!("expected optimizer explain system table");
	};
	assert_eq!(kind, SystemTableKind::OptimizerExplain);
	(schema, rows)
}

async fn demote_outer_join_plan(
	logical: &LogicalPlan,
	catalog: &dyn Catalog,
) -> (LogicalPlan, Vec<OuterJoinDemotionReceipt>) {
	let config = PlannerConfig::default();
	let binder = Binder::new(catalog, &config);
	binder
		.demote_null_rejected_outer_joins(logical)
		.await
		.expect("demote outer joins")
}

async fn optimizer_decision_rows(
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Vec<Vec<ScalarValue>> {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_decisions")
		.expect("plan optimizer decisions report");
	let plan = bind(&logical, catalog, config)
		.await
		.expect("bind optimizer decisions report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer decisions system table, got {plan:?}");
	};
	assert_eq!(kind, SystemTableKind::InformationSchemaOptimizerDecisions);
	rows
}

async fn optimizer_runtime_verdict_rows(
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Vec<Vec<ScalarValue>> {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_runtime_verdicts")
		.expect("plan optimizer runtime verdict report");
	let plan = bind(&logical, catalog, config)
		.await
		.expect("bind optimizer runtime verdict report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer runtime verdict system table, got {plan:?}");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerRuntimeVerdicts
	);
	rows
}

async fn optimizer_evidence_status_rows(
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Vec<Vec<ScalarValue>> {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_evidence_status")
		.expect("plan optimizer evidence status report");
	let plan = bind(&logical, catalog, config)
		.await
		.expect("bind optimizer evidence status report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer evidence status system table, got {plan:?}");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerEvidenceStatus
	);
	rows
}

async fn optimizer_margin_advisory_rows(
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Vec<Vec<ScalarValue>> {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_margin_advisory")
		.expect("plan optimizer margin advisory report");
	let plan = bind(&logical, catalog, config)
		.await
		.expect("bind optimizer margin advisory report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer margin advisory system table, got {plan:?}");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerMarginAdvisory
	);
	rows
}

async fn optimizer_receipt_gate_table_rows(
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Vec<Vec<ScalarValue>> {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_receipt_gates")
		.expect("plan optimizer receipt gates report");
	let plan = bind(&logical, catalog, config)
		.await
		.expect("bind optimizer receipt gates report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer receipt gates system table, got {plan:?}");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerReceiptGates
	);
	rows
}

async fn optimizer_decision_conflict_table_rows(
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Vec<Vec<ScalarValue>> {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_decision_conflicts")
		.expect("plan optimizer decision conflicts report");
	let plan = bind(&logical, catalog, config)
		.await
		.expect("bind optimizer decision conflicts report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer decision conflicts system table, got {plan:?}");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerDecisionConflicts
	);
	rows
}

async fn optimizer_memo_admission_table_rows(
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Vec<Vec<ScalarValue>> {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_memo_admission")
		.expect("plan optimizer memo admission report");
	let plan = bind(&logical, catalog, config)
		.await
		.expect("bind optimizer memo admission report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer memo admission system table, got {plan:?}");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerMemoAdmission
	);
	rows
}

async fn optimizer_probe_linearity_fence_table_rows(
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Vec<Vec<ScalarValue>> {
	let logical = SqlPlanner::new()
		.plan("SELECT * FROM information_schema.optimizer_probe_linearity_fences")
		.expect("plan optimizer probe-linearity fence report");
	let plan = bind(&logical, catalog, config)
		.await
		.expect("bind optimizer probe-linearity fence report");
	let ExecutionPlan::SystemTable { kind, rows, .. } = plan else {
		panic!("expected optimizer probe-linearity fence system table, got {plan:?}");
	};
	assert_eq!(
		kind,
		SystemTableKind::InformationSchemaOptimizerProbeLinearityFences
	);
	rows
}

fn optimizer_decision_count(
	rows: &[Vec<ScalarValue>],
	kind: &str,
	outcome: &str,
	decline_reason: Option<&str>,
) -> i64 {
	let row = rows
		.iter()
		.find(|row| {
			row[0] == ScalarValue::Utf8(Some(kind.to_string()))
				&& row[1] == ScalarValue::Utf8(Some(outcome.to_string()))
				&& row[2]
					== ScalarValue::Utf8(decline_reason.map(std::string::ToString::to_string))
		})
		.unwrap_or_else(|| {
			panic!(
				"missing optimizer decision row kind={kind} outcome={outcome} reason={decline_reason:?}: {rows:?}"
			)
		});
	let ScalarValue::Int64(Some(count)) = row[3] else {
		panic!("expected optimizer decision count, got {:?}", row[3]);
	};
	count
}

fn optimizer_decision_count_by_provenance(
	rows: &[Vec<ScalarValue>],
	kind: &str,
	outcome: &str,
	decline_reason: Option<&str>,
	provenance: &str,
) -> i64 {
	rows.iter()
		.find_map(|row| {
			(row[0] == ScalarValue::Utf8(Some(kind.to_string()))
				&& row[1] == ScalarValue::Utf8(Some(outcome.to_string()))
				&& row[2]
					== ScalarValue::Utf8(decline_reason.map(std::string::ToString::to_string))
				&& row[4] == ScalarValue::Utf8(Some(provenance.to_string())))
			.then(|| {
				let ScalarValue::Int64(Some(count)) = row[3] else {
					panic!("expected optimizer decision count, got {:?}", row[3]);
				};
				count
			})
		})
		.unwrap_or(0)
}

fn optimizer_runtime_verdict_count(rows: &[Vec<ScalarValue>], kind: &str, verdict: &str) -> i64 {
	let row = rows
		.iter()
		.find(|row| {
			row[0] == ScalarValue::Utf8(Some(kind.to_string()))
				&& row[1] == ScalarValue::Utf8(Some(verdict.to_string()))
		})
		.unwrap_or_else(|| {
			panic!("missing optimizer runtime verdict row kind={kind} verdict={verdict}: {rows:?}")
		});
	let ScalarValue::Int64(Some(count)) = row[2] else {
		panic!("expected optimizer runtime verdict count, got {:?}", row[2]);
	};
	count
}

fn optimizer_runtime_verdict_count_by_provenance(
	rows: &[Vec<ScalarValue>],
	kind: &str,
	verdict: &str,
	provenance: &str,
) -> i64 {
	let row = rows
		.iter()
		.find(|row| {
			row[0] == ScalarValue::Utf8(Some(kind.to_string()))
				&& row[1] == ScalarValue::Utf8(Some(verdict.to_string()))
				&& row[3] == ScalarValue::Utf8(Some(provenance.to_string()))
		})
		.unwrap_or_else(|| {
			panic!(
				"missing optimizer runtime verdict row kind={kind} verdict={verdict} provenance={provenance}: {rows:?}"
			)
		});
	let ScalarValue::Int64(Some(count)) = row[2] else {
		panic!("expected optimizer runtime verdict count, got {:?}", row[2]);
	};
	count
}

fn optimizer_runtime_verdict_kind_count(rows: &[Vec<ScalarValue>], kind: &str) -> i64 {
	rows.iter()
		.filter(|row| row[0] == ScalarValue::Utf8(Some(kind.to_string())))
		.map(|row| {
			let ScalarValue::Int64(Some(count)) = row[2] else {
				panic!("expected optimizer runtime verdict count, got {:?}", row[2]);
			};
			count
		})
		.sum()
}

fn verdict_snapshot(
	rows: &[(OptimizerDecisionKind, OptimizerRuntimeVerdictClass, u64)],
) -> Vec<OptimizerRuntimeVerdictRow> {
	rows.iter()
		.map(|(kind, verdict, count)| OptimizerRuntimeVerdictRow {
			kind: *kind,
			verdict: *verdict,
			provenance: OptimizerRuntimeVerdictProvenance::Native,
			count: *count,
		})
		.collect()
}

fn record_verdicts(
	evidence: &OptimizerRuntimeEvidence,
	kind: OptimizerDecisionKind,
	verdict: OptimizerRuntimeVerdictClass,
	count: u64,
) {
	for _ in 0..count {
		evidence.record_verdict(kind, verdict, OptimizerRuntimeVerdictProvenance::Native);
	}
}

fn record_decisions(
	evidence: &OptimizerRuntimeEvidence,
	kind: OptimizerDecisionKind,
	outcome: OptimizerDecisionOutcome,
	provenance: OptimizerRuntimeVerdictProvenance,
	count: u64,
) {
	for _ in 0..count {
		evidence.record_decision(kind, outcome, provenance);
	}
}

fn record_conflicted_decisions(
	evidence: &OptimizerRuntimeEvidence,
	kind: OptimizerDecisionKind,
	provenance: OptimizerRuntimeVerdictProvenance,
) {
	record_decisions(
		evidence,
		kind,
		OptimizerDecisionOutcome::Reordered,
		provenance,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		evidence,
		kind,
		OptimizerDecisionOutcome::KeptThinMargin,
		provenance,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
}

fn record_declined_conflict_decisions(
	evidence: &OptimizerRuntimeEvidence,
	kind: OptimizerDecisionKind,
	reason: OptimizerDecisionDeclineReason,
	provenance: OptimizerRuntimeVerdictProvenance,
) {
	record_decisions(
		evidence,
		kind,
		OptimizerDecisionOutcome::Reordered,
		provenance,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
	record_decisions(
		evidence,
		kind,
		OptimizerDecisionOutcome::Declined(reason),
		provenance,
		JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE,
	);
}

fn margin_advisory_row_by_scope<'a>(
	rows: &'a [Vec<ScalarValue>],
	scope: &str,
) -> &'a [ScalarValue] {
	rows.iter()
		.find(|row| row[11] == ScalarValue::Utf8(Some(scope.to_string())))
		.unwrap_or_else(|| panic!("missing optimizer margin advisory scope={scope}: {rows:?}"))
}

fn assert_margin_advisory_scope_order(rows: &[Vec<ScalarValue>]) {
	let scopes = rows
		.iter()
		.map(|row| margin_advisory_utf8(row, 11))
		.collect::<Vec<_>>();
	assert_eq!(
		scopes,
		vec![
			"tree_join_order",
			"graph_join_order",
			"galaxy_root",
			"bridge_root",
			"bridge_chain_root",
		]
	);
}

fn receipt_gate_row_by_name<'a>(
	rows: &'a [Vec<ScalarValue>],
	gate_name: &str,
) -> &'a [ScalarValue] {
	rows.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some(gate_name.to_string())))
		.unwrap_or_else(|| panic!("missing optimizer receipt gate name={gate_name}: {rows:?}"))
}

fn assert_receipt_gate_order(rows: &[Vec<ScalarValue>]) {
	let gates = rows
		.iter()
		.map(|row| receipt_gate_utf8(row, 0))
		.collect::<Vec<_>>();
	assert_eq!(
		gates,
		vec![
			"graph_margin_divergence",
			"preserved_outer_ordering",
			"decision_conflict_pressure",
			"windowing_decay_pressure",
			"counter_handoff_readiness",
			"probe_linearity_fence_dominance",
		]
	);
}

fn decision_conflict_row<'a>(
	rows: &'a [Vec<ScalarValue>],
	kind: &str,
	provenance: &str,
) -> &'a [ScalarValue] {
	rows.iter()
		.find(|row| {
			row[0] == ScalarValue::Utf8(Some(kind.to_string()))
				&& row[1] == ScalarValue::Utf8(Some(provenance.to_string()))
		})
		.unwrap_or_else(|| {
			panic!(
				"missing optimizer decision conflict kind={kind} provenance={provenance}: {rows:?}"
			)
		})
}

fn assert_decision_conflict_order(rows: &[Vec<ScalarValue>]) {
	let expected = decision_recording_kinds()
		.into_iter()
		.flat_map(|kind| {
			["native", "demoted"].into_iter().map(move |provenance| {
				(
					optimizer_decision_kind_label(kind).to_string(),
					provenance.to_string(),
				)
			})
		})
		.collect::<Vec<_>>();
	let actual = rows
		.iter()
		.map(|row| {
			(
				decision_conflict_utf8(row, 0).to_string(),
				decision_conflict_utf8(row, 1).to_string(),
			)
		})
		.collect::<Vec<_>>();
	assert_eq!(actual, expected);
}

fn memo_admission_row<'a>(
	rows: &'a [Vec<ScalarValue>],
	kind: &str,
	provenance: &str,
) -> &'a [ScalarValue] {
	rows.iter()
		.find(|row| {
			row[0] == ScalarValue::Utf8(Some(kind.to_string()))
				&& row[1] == ScalarValue::Utf8(Some(provenance.to_string()))
		})
		.unwrap_or_else(|| {
			panic!("missing optimizer memo admission kind={kind} provenance={provenance}: {rows:?}")
		})
}

fn assert_memo_admission_order(rows: &[Vec<ScalarValue>]) {
	let expected = decision_recording_kinds()
		.into_iter()
		.flat_map(|kind| {
			["native", "demoted"].into_iter().map(move |provenance| {
				(
					optimizer_decision_kind_label(kind).to_string(),
					provenance.to_string(),
				)
			})
		})
		.collect::<Vec<_>>();
	let actual = rows
		.iter()
		.map(|row| {
			(
				memo_admission_utf8(row, 0).to_string(),
				memo_admission_utf8(row, 1).to_string(),
			)
		})
		.collect::<Vec<_>>();
	assert_eq!(actual, expected);
}

fn probe_linearity_fence_row<'a>(
	rows: &'a [Vec<ScalarValue>],
	consumer: &str,
	fence: &str,
) -> &'a [ScalarValue] {
	rows.iter()
		.find(|row| {
			row[0] == ScalarValue::Utf8(Some(consumer.to_string()))
				&& row[1] == ScalarValue::Utf8(Some(fence.to_string()))
		})
		.unwrap_or_else(|| {
			panic!("missing optimizer probe-linearity row {consumer}/{fence}: {rows:?}")
		})
}

fn assert_probe_linearity_fence_order(rows: &[Vec<ScalarValue>]) {
	let expected = ["hash_fallback", "star_decline"]
		.into_iter()
		.flat_map(|consumer| {
			[
				"scan_row_limit",
				"take",
				"sort",
				"aggregate",
				"exchange",
				"non_broadcast_join",
				"non_scan_leaf",
			]
			.into_iter()
			.map(move |fence| (consumer.to_string(), fence.to_string()))
		})
		.collect::<Vec<_>>();
	let actual = rows
		.iter()
		.map(|row| {
			(
				probe_linearity_fence_utf8(row, 0).to_string(),
				probe_linearity_fence_utf8(row, 1).to_string(),
			)
		})
		.collect::<Vec<_>>();
	assert_eq!(actual, expected);
}

fn margin_advisory_utf8(row: &[ScalarValue], index: usize) -> &str {
	let ScalarValue::Utf8(Some(value)) = &row[index] else {
		panic!(
			"expected margin advisory Utf8 at {index}, got {:?}",
			row[index]
		);
	};
	value.as_str()
}

fn margin_advisory_i64(row: &[ScalarValue], index: usize) -> i64 {
	let ScalarValue::Int64(Some(value)) = row[index] else {
		panic!(
			"expected margin advisory Int64 at {index}, got {:?}",
			row[index]
		);
	};
	value
}

fn receipt_gate_utf8(row: &[ScalarValue], index: usize) -> &str {
	let ScalarValue::Utf8(Some(value)) = &row[index] else {
		panic!(
			"expected receipt gate Utf8 at {index}, got {:?}",
			row[index]
		);
	};
	value.as_str()
}

fn receipt_gate_i64(row: &[ScalarValue], index: usize) -> i64 {
	let ScalarValue::Int64(Some(value)) = row[index] else {
		panic!(
			"expected receipt gate Int64 at {index}, got {:?}",
			row[index]
		);
	};
	value
}

fn decision_conflict_utf8(row: &[ScalarValue], index: usize) -> &str {
	let ScalarValue::Utf8(Some(value)) = &row[index] else {
		panic!(
			"expected decision conflict Utf8 at {index}, got {:?}",
			row[index]
		);
	};
	value.as_str()
}

fn decision_conflict_nullable_utf8(row: &[ScalarValue], index: usize) -> Option<&str> {
	let ScalarValue::Utf8(value) = &row[index] else {
		panic!(
			"expected decision conflict nullable Utf8 at {index}, got {:?}",
			row[index]
		);
	};
	value.as_deref()
}

fn decision_conflict_i64(row: &[ScalarValue], index: usize) -> i64 {
	let ScalarValue::Int64(Some(value)) = row[index] else {
		panic!(
			"expected decision conflict Int64 at {index}, got {:?}",
			row[index]
		);
	};
	value
}

fn memo_admission_utf8(row: &[ScalarValue], index: usize) -> &str {
	let ScalarValue::Utf8(Some(value)) = &row[index] else {
		panic!(
			"expected optimizer memo admission Utf8 at {index}, got {:?}",
			row[index]
		);
	};
	value.as_str()
}

fn memo_admission_i64(row: &[ScalarValue], index: usize) -> i64 {
	let ScalarValue::Int64(Some(value)) = row[index] else {
		panic!(
			"expected optimizer memo admission Int64 at {index}, got {:?}",
			row[index]
		);
	};
	value
}

fn probe_linearity_fence_utf8(row: &[ScalarValue], index: usize) -> &str {
	let ScalarValue::Utf8(Some(value)) = &row[index] else {
		panic!(
			"expected optimizer probe-linearity Utf8 at {index}, got {:?}",
			row[index]
		);
	};
	value.as_str()
}

fn probe_linearity_fence_i64(row: &[ScalarValue], index: usize) -> i64 {
	let ScalarValue::Int64(Some(value)) = row[index] else {
		panic!(
			"expected optimizer probe-linearity Int64 at {index}, got {:?}",
			row[index]
		);
	};
	value
}

fn assert_utf8_contains(value: &ScalarValue, expected: &str) {
	let ScalarValue::Utf8(Some(text)) = value else {
		panic!("expected Utf8 containing {expected:?}, got {value:?}");
	};
	assert!(
		text.contains(expected),
		"expected {text:?} to contain {expected:?}"
	);
}

fn join_order_row(rows: &[Vec<ScalarValue>]) -> &[ScalarValue] {
	rows.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order".to_string())))
		.map(Vec::as_slice)
		.expect("join_order evidence row")
}

fn join_order_snowflake_row(rows: &[Vec<ScalarValue>]) -> &[ScalarValue] {
	rows.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_snowflake".to_string())))
		.map(Vec::as_slice)
		.expect("join_order_snowflake evidence row")
}

fn join_order_edge_dimensions(render: &OptimizerExplainRenderTrace) -> Vec<String> {
	render
		.join_order_edges
		.iter()
		.filter_map(|edge| edge.dimension.clone())
		.collect()
}

fn join_decision_summary(trace: &OptimizerTrace) -> String {
	trace
		.join_decisions
		.iter()
		.map(|receipt| {
			format!(
				"#{} {} {} {}",
				receipt.target.join_index,
				join_strategy_label(receipt.strategy),
				join_physical_placement_label(receipt.placement),
				join_strategy_decision_reason_label(&receipt.reason)
			)
		})
		.collect::<Vec<_>>()
		.join(", ")
}

fn join_order_edge_summary(render: &OptimizerExplainRenderTrace) -> String {
	render
		.join_order_edges
		.iter()
		.map(|edge| {
			format!(
				"#{} {:?} probe={:?} dim={:?} left_keys={:?} right_keys={:?} rows={:?} linearity={:?}",
				edge.target.join_index,
				edge.join_kind,
				edge.probe,
				edge.dimension,
				edge.left_key_signature,
				edge.right_key_signature,
				edge.estimated_rows,
				edge.probe_linearity
			)
		})
		.collect::<Vec<_>>()
		.join(" | ")
}

fn broadcast_build_scan_ids_in_applied_order(plan: &ExecutionPlan) -> Vec<TableId> {
	let mut top_down_build_ids = Vec::new();
	collect_broadcast_build_scan_ids_top_down(plan, &mut top_down_build_ids);
	top_down_build_ids.reverse();
	top_down_build_ids
}

fn collect_broadcast_build_scan_ids_top_down(plan: &ExecutionPlan, build_ids: &mut Vec<TableId>) {
	match plan {
		ExecutionPlan::Project { input, .. } => {
			collect_broadcast_build_scan_ids_top_down(input, build_ids);
		}
		ExecutionPlan::Join {
			left,
			right,
			kind,
			strategy,
			..
		} if *kind == JoinKind::Inner && *strategy == JoinStrategy::Broadcast => {
			if let Some(table_id) = single_scan_table_id(left) {
				build_ids.push(table_id);
			}
			collect_broadcast_build_scan_ids_top_down(right, build_ids);
		}
		_ => {}
	}
}

fn single_scan_table_id(plan: &ExecutionPlan) -> Option<TableId> {
	match plan {
		ExecutionPlan::Scan { local_table_id, .. } => Some(*local_table_id),
		ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. } => single_scan_table_id(input),
		_ => None,
	}
}

fn root_project_column_ordinals(plan: &ExecutionPlan) -> Vec<ColumnOrdinal> {
	let ExecutionPlan::Project { exprs, .. } = plan else {
		panic!("expected root project, got {plan:?}");
	};
	exprs
		.roots
		.iter()
		.map(|root| match exprs.node(*root) {
			Some(BoundExprNode::Column(column)) => column.ordinal,
			other => panic!("expected projected column, got {other:?}"),
		})
		.collect()
}

fn join_input(facts: PlanOptimizerFacts) -> JoinInputOptimization {
	join_input_with_linearity(facts, ProbeLinearity::Linear)
}

fn join_input_with_linearity(
	facts: PlanOptimizerFacts,
	probe_linearity: ProbeLinearity,
) -> JoinInputOptimization {
	JoinInputOptimization {
		facts,
		probe_linearity,
	}
}

fn assert_join_strategy(plan: &ExecutionPlan, expected: JoinStrategy) {
	match first_join_plan(plan) {
		ExecutionPlan::Join { strategy, .. } => assert_eq!(*strategy, expected),
		other => panic!("expected join, got {other:?}"),
	}
}

fn assert_join_strategy_count(plan: &ExecutionPlan, expected: JoinStrategy, expected_count: usize) {
	let mut observed = Vec::new();
	collect_join_strategies(plan, &mut observed);
	assert_eq!(
		observed
			.into_iter()
			.filter(|strategy| *strategy == expected)
			.count(),
		expected_count
	);
}

fn collect_join_strategies(plan: &ExecutionPlan, strategies: &mut Vec<JoinStrategy>) {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} => {
			strategies.push(*strategy);
			collect_join_strategies(left, strategies);
			collect_join_strategies(right, strategies);
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => collect_join_strategies(input, strategies),
		ExecutionPlan::Union { left, right, .. } => {
			collect_join_strategies(left, strategies);
			collect_join_strategies(right, strategies);
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE has no joins in its body (first-cut shape is scan/filter/project),
		// so it is an opaque leaf to these join collectors.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => {}
	}
}

fn execution_join_kinds(plan: &ExecutionPlan) -> Vec<JoinKind> {
	let mut kinds = Vec::new();
	collect_execution_join_kinds(plan, &mut kinds);
	kinds
}

fn collect_execution_join_kinds(plan: &ExecutionPlan, kinds: &mut Vec<JoinKind>) {
	match plan {
		ExecutionPlan::Join {
			left, right, kind, ..
		} => {
			kinds.push(*kind);
			collect_execution_join_kinds(left, kinds);
			collect_execution_join_kinds(right, kinds);
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => collect_execution_join_kinds(input, kinds),
		ExecutionPlan::Union { left, right, .. } => {
			collect_execution_join_kinds(left, kinds);
			collect_execution_join_kinds(right, kinds);
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE has no joins in its body (first-cut shape is scan/filter/project),
		// so it is an opaque leaf to these join collectors.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => {}
	}
}

fn collect_join_replay_ids(plan: &ExecutionPlan, ids: &mut Vec<JoinReplayId>) {
	match plan {
		ExecutionPlan::Join {
			replay_id,
			left,
			right,
			..
		} => {
			ids.push(*replay_id);
			collect_join_replay_ids(left, ids);
			collect_join_replay_ids(right, ids);
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => collect_join_replay_ids(input, ids),
		ExecutionPlan::Union { left, right, .. } => {
			collect_join_replay_ids(left, ids);
			collect_join_replay_ids(right, ids);
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE has no joins in its body (first-cut shape is scan/filter/project),
		// so it is an opaque leaf to these join collectors.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => {}
	}
}

fn collect_join_replay_metadata(
	plan: &ExecutionPlan,
	metadata: &mut Vec<(JoinReplayId, BroadcastReplayEligibility, JoinStrategy)>,
) {
	match plan {
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			strategy,
			left,
			right,
			..
		} => {
			metadata.push((*replay_id, *replay_eligibility, *strategy));
			collect_join_replay_metadata(left, metadata);
			collect_join_replay_metadata(right, metadata);
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => collect_join_replay_metadata(input, metadata),
		ExecutionPlan::Union { left, right, .. } => {
			collect_join_replay_metadata(left, metadata);
			collect_join_replay_metadata(right, metadata);
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE has no joins in its body (first-cut shape is scan/filter/project),
		// so it is an opaque leaf to these join collectors.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => {}
	}
}

fn first_join_plan(plan: &ExecutionPlan) -> &ExecutionPlan {
	match plan {
		ExecutionPlan::Join { .. } => plan,
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => first_join_plan(input),
		ExecutionPlan::Union { left, right, .. } => {
			let left = first_join_plan(left);
			if matches!(left, ExecutionPlan::Join { .. }) {
				left
			} else {
				first_join_plan(right)
			}
		}
		other => other,
	}
}

fn logical_join_kinds(plan: &LogicalPlan) -> Vec<JoinKind> {
	let mut kinds = Vec::new();
	collect_logical_join_kinds(plan, &mut kinds);
	kinds
}

fn collect_logical_join_kinds(plan: &LogicalPlan, kinds: &mut Vec<JoinKind>) {
	match plan {
		LogicalPlan::Join {
			left, right, kind, ..
		} => {
			kinds.push(*kind);
			collect_logical_join_kinds(left, kinds);
			collect_logical_join_kinds(right, kinds);
		}
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Distinct { input }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Window { input, .. }
		| LogicalPlan::Take { input, .. }
		| LogicalPlan::Repeat { input, .. }
		| LogicalPlan::Explain { input }
		| LogicalPlan::ExplainAnalyze { input } => collect_logical_join_kinds(input, kinds),
		LogicalPlan::CreateTableAsSelect { source, .. }
		| LogicalPlan::InsertSelect { source, .. } => collect_logical_join_kinds(source, kinds),
		LogicalPlan::Union { left, right } => {
			collect_logical_join_kinds(left, kinds);
			collect_logical_join_kinds(right, kinds);
		}
		LogicalPlan::SetOpAll { left, right, .. } => {
			collect_logical_join_kinds(left, kinds);
			collect_logical_join_kinds(right, kinds);
		}
		// ADR 279: a deferred EXISTS resolves to a semi-join later; recurse both sides for inner joins.
		LogicalPlan::DeferredExists { outer, inner, .. } => {
			collect_logical_join_kinds(outer, kinds);
			collect_logical_join_kinds(inner, kinds);
		}
		LogicalPlan::DeferredScalarSubquery { outer, inner, .. } => {
			collect_logical_join_kinds(outer, kinds);
			collect_logical_join_kinds(inner, kinds);
		}
		LogicalPlan::Scan { .. }
		| LogicalPlan::ScanNamed { .. }
		| LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateLogicalType { .. }
		| LogicalPlan::DropTable { .. }
		| LogicalPlan::Delete { .. }
		| LogicalPlan::Update { .. }
		// ADR 255: the first-cut recursive CTE body has no joins — opaque leaf to this join collector.
		| LogicalPlan::RecursiveCte { .. }
		| LogicalPlan::RecursiveRef { .. }
		| LogicalPlan::Insert { .. } => {}
	}
}

fn field_names(schema: &SchemaRef) -> Vec<&str> {
	schema
		.fields()
		.iter()
		.map(|field| field.name().as_str())
		.collect()
}

fn join_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			named_descriptor(
				1,
				"orders",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register orders");
	catalog
		.register_descriptor(
			TableId(2),
			named_descriptor(
				2,
				"customers",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("region", DataType::Utf8, true),
				]),
			),
		)
		.expect("register customers");
	catalog
}

fn mixed_outer_join_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(11),
			named_descriptor(
				11,
				"orders",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
					Field::new("product_id", DataType::Int64, false),
					Field::new("store_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register mixed orders");
	catalog
		.register_descriptor(
			TableId(12),
			named_descriptor(
				12,
				"customers",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("region", DataType::Utf8, true),
				]),
			),
		)
		.expect("register mixed customers");
	catalog
		.register_descriptor(
			TableId(13),
			named_descriptor(
				13,
				"products",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register mixed products");
	catalog
		.register_descriptor(
			TableId(14),
			named_descriptor(
				14,
				"stores",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register mixed stores");
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(100_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(3), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"products",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"stores",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	catalog
}

fn named_scan(table: &str) -> LogicalPlan {
	LogicalPlan::ScanNamed {
		table: TableRef::new(table),
		projection: None,
	}
}

fn qcol(table: &str, column: &str) -> Expr {
	Expr::qualified_column(TableRef::new(table), column)
}

fn orders_customers_left_join() -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(named_scan("orders")),
		right: Box::new(named_scan("customers")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("orders", "customer_id"), qcol("customers", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn orders_customers_full_join() -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(named_scan("orders")),
		right: Box::new(named_scan("customers")),
		kind: JoinKind::FullOuter,
		on: vec![(qcol("orders", "customer_id"), qcol("customers", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn products_stores_left_join() -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(named_scan("products")),
		right: Box::new(named_scan("stores")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("products", "id"), qcol("stores", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn mixed_chain_one_inner_hop() -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(orders_customers_left_join()),
		right: Box::new(named_scan("products")),
		kind: JoinKind::Inner,
		on: vec![(qcol("orders", "product_id"), qcol("products", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn customers_region_is_north() -> Expr {
	Expr::Eq(
		Box::new(qcol("customers", "region")),
		Box::new(utf8_literal("north")),
	)
}

fn orders_id_sort_key() -> SortExpr {
	SortExpr {
		expr: qcol("orders", "id"),
		descending: false,
		nulls_first: false,
	}
}

fn star_join_order_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(21),
			named_descriptor(
				21,
				"orders",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
					Field::new("product_id", DataType::Int64, false),
					Field::new("store_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register star orders");
	catalog
		.register_descriptor(
			TableId(22),
			named_descriptor(
				22,
				"customers",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register star customers");
	catalog
		.register_descriptor(
			TableId(23),
			named_descriptor(
				23,
				"products",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register star products");
	catalog
		.register_descriptor(
			TableId(24),
			named_descriptor(
				24,
				"stores",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register star stores");

	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		None,
		Some(100_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(3), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"customers",
		None,
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"products",
		None,
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"stores",
		None,
		Some(100),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	catalog
}

fn hash_star_join_order_catalog() -> MockCatalog {
	let mut catalog = star_join_order_catalog();
	seed_star_join_order_facts_with_bytes(&mut catalog, 8_192, 8_192, 8_192);
	catalog
}

fn mixed_strategy_star_join_order_catalog() -> MockCatalog {
	let mut catalog = star_join_order_catalog();
	seed_star_join_order_facts_with_bytes(&mut catalog, 1_024, 8_192, 8_192);
	catalog
}

fn snowflake_join_order_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	register_snowflake_join_order_tables(&mut catalog);
	seed_snowflake_join_order_facts(&mut catalog, None, None, None, None, None);
	catalog
}

fn hash_snowflake_join_order_catalog() -> MockCatalog {
	let mut catalog = snowflake_join_order_catalog();
	seed_snowflake_join_order_facts(
		&mut catalog,
		Some(64 * 1024 * 1024),
		Some(8_192),
		Some(8_192),
		Some(8_192),
		Some(8_192),
	);
	catalog
}

// ADR 194: two fact tables (`sales`, `returns`) sharing a conformed dimension `product` on its
// primary key. A reachable single-query join graph that binds as a single-root snowflake but whose
// role-neutral key signatures reveal `product.id` referenced by both facts.
fn galaxy_join_order_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(41),
			named_descriptor(
				41,
				"product",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register galaxy product");
	catalog
		.register_descriptor(
			TableId(42),
			named_descriptor(
				42,
				"sales",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("product_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register galaxy sales");
	catalog
		.register_descriptor(
			TableId(43),
			named_descriptor(
				43,
				"returns",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("product_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register galaxy returns");
	seed_table_optimizer_facts(
		&mut catalog,
		"product",
		Some(512),
		Some(100),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"sales",
		Some(64 * 1024 * 1024),
		Some(100_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"returns",
		Some(8_192),
		Some(5_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
	);
	catalog
}

fn margin_boundary_galaxy_join_order_catalog() -> MockCatalog {
	let mut catalog = galaxy_join_order_catalog();
	seed_table_optimizer_facts(
		&mut catalog,
		"returns",
		Some(8_192),
		Some(85_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
	);
	catalog
}

fn galaxy_conformed_order_sql() -> &'static str {
	"SELECT sales.id, product.id, returns.id FROM sales \
	 JOIN product ON sales.product_id = product.id \
	 JOIN returns ON returns.product_id = product.id"
}

// ADR 202: a 3-fact galaxy — sales / returns / shipments all conformed on `product`. `shipments` (1k) is
// the smallest fact (cost-optimal root); the FROM root `sales` (100k) is the largest.
fn three_fact_galaxy_join_order_catalog() -> MockCatalog {
	let mut catalog = galaxy_join_order_catalog();
	catalog
		.register_descriptor(
			TableId(44),
			named_descriptor(
				44,
				"shipments",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("product_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register galaxy shipments");
	seed_table_optimizer_facts(
		&mut catalog,
		"shipments",
		Some(2_048),
		Some(1_000),
		vec![(ColumnOrdinal(1), DistinctCountFact::Exact(100))],
	);
	catalog
}

fn three_fact_galaxy_conformed_order_sql() -> &'static str {
	"SELECT sales.id, product.id, returns.id, shipments.id FROM sales \
	 JOIN product ON sales.product_id = product.id \
	 JOIN returns ON returns.product_id = product.id \
	 JOIN shipments ON shipments.product_id = product.id"
}

fn three_fact_galaxy_optimal_order_sql() -> &'static str {
	"SELECT sales.id, product.id, returns.id, shipments.id FROM shipments \
	 JOIN product ON shipments.product_id = product.id \
	 JOIN sales ON sales.product_id = product.id \
	 JOIN returns ON returns.product_id = product.id"
}

// ADR 202 headline (binder): the 3-fact galaxy actuates — the actuator re-roots the FROM-`sales` order to
// the cost-optimal `shipments`, emitting `action=reordered; applied_root=shipments`. Fails on `main`
// (the 2-fact-only re-root declines an n>2 chain → observe-only).
#[tokio::test]
async fn three_fact_galaxy_reroots_to_cheapest_root() {
	let catalog = three_fact_galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!(
			"EXPLAIN {}",
			three_fact_galaxy_conformed_order_sql()
		))
		.expect("plan 3-fact galaxy EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let galaxy_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("galaxy row");
	assert_eq!(
		galaxy_row[2],
		ScalarValue::Utf8(Some("reordered".to_string()))
	);
	assert_utf8_contains(&galaxy_row[3], "action=reordered");
	assert_utf8_contains(&galaxy_row[3], "applied_root=shipments");
}

// ADR 202 discriminator: the same 3-fact galaxy written FROM the cheapest fact (`shipments`) is kept —
// proving the generalized re-root decides from cost, not a constant, and the already-optimal path holds.
#[tokio::test]
async fn three_fact_galaxy_already_optimal_kept() {
	let catalog = three_fact_galaxy_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!(
			"EXPLAIN {}",
			three_fact_galaxy_optimal_order_sql()
		))
		.expect("plan optimal 3-fact galaxy EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;
	let galaxy_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_galaxy".to_string())))
		.expect("galaxy row");
	assert_utf8_contains(&galaxy_row[3], "action=kept_already_optimal");
}

// ADR 202: an n-fact galaxy reorder accrues a `galaxy_root` reordered decision through the unchanged
// 200/201 telemetry path — no new decision kind.
#[tokio::test]
async fn three_fact_galaxy_accrues_galaxy_root_reorder() {
	let catalog = three_fact_galaxy_join_order_catalog();
	let config = auto_broadcast_config(4_096);
	let _ = bind(
		&SqlPlanner::new()
			.plan(three_fact_galaxy_conformed_order_sql())
			.expect("plan 3-fact galaxy"),
		&catalog,
		&config,
	)
	.await
	.expect("bind 3-fact galaxy");
	let rows = optimizer_decision_rows(&catalog, &config).await;
	assert_eq!(
		optimizer_decision_count(&rows, "galaxy_root", "reordered", None),
		1
	);
}

// ADR 202 (F1 regression): the generalized re-root must carry each fact's join `strategy_intent` with
// the fact, not the rebuilt position. Here the `returns` join is pinned Broadcast; the rest are Auto.
// Re-rooting to `shipments` (a non-leftmost fact) must leave the pin on `returns`'s edge — under a
// position-indexed rebuild it shifts onto `sales`. (`choose_join_strategy` / broadcast eligibility
// branch on `strategy_intent`, so a shifted pin replans a different join physically.)
#[test]
fn n_fact_galaxy_reroot_carries_strategy_intent_by_fact() {
	let scan = |name: &str| LogicalPlan::ScanNamed {
		table: TableRef::new(name),
		projection: None,
	};
	let pinned = JoinStrategyIntent::Pinned {
		strategy: JoinStrategy::Broadcast,
		source: JoinStrategyPinSource::Direct,
		build_side: Some(BroadcastBuildSide::Left),
	};
	// `((sales ⋈ product) ⋈ returns[pinned]) ⋈ shipments`, all conformed on `product`.
	let inner = LogicalPlan::Join {
		left: Box::new(scan("sales")),
		right: Box::new(scan("product")),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("sales"), "product_id"),
			Expr::qualified_column(TableRef::new("product"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let with_returns = LogicalPlan::Join {
		left: Box::new(inner),
		right: Box::new(scan("returns")),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("product"), "id"),
			Expr::qualified_column(TableRef::new("returns"), "product_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: pinned.clone(),
	};
	let galaxy = LogicalPlan::Join {
		left: Box::new(with_returns),
		right: Box::new(scan("shipments")),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("product"), "id"),
			Expr::qualified_column(TableRef::new("shipments"), "product_id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};

	let rerooted =
		reroot_galaxy_join_tree(&galaxy, "product", "shipments").expect("3-fact galaxy re-roots");

	// Collect each fact's edge intent: the right leaf of every non-base join, and the left leaf of the
	// base join (`Join(Fk, D)`).
	let leaf_name = |plan: &LogicalPlan| match plan {
		LogicalPlan::ScanNamed { table, .. } => table.table.clone(),
		_ => panic!("expected a leaf scan"),
	};
	let mut by_fact: std::collections::BTreeMap<String, JoinStrategyIntent> = Default::default();
	let mut node = &rerooted;
	loop {
		let LogicalPlan::Join {
			left,
			right,
			strategy_intent,
			..
		} = node
		else {
			panic!("expected a join chain");
		};
		if matches!(left.as_ref(), LogicalPlan::Join { .. }) {
			by_fact.insert(leaf_name(right), strategy_intent.clone());
			node = left.as_ref();
		} else {
			// base `Join(Fk, D)` — the fact is the left leaf.
			by_fact.insert(leaf_name(left), strategy_intent.clone());
			break;
		}
	}

	assert_eq!(
		by_fact.get("returns"),
		Some(&pinned),
		"the pin must stay on the returns edge: {by_fact:?}"
	);
	assert_eq!(by_fact.get("sales"), Some(&JoinStrategyIntent::Auto));
	assert_eq!(by_fact.get("shipments"), Some(&JoinStrategyIntent::Auto));
}

// ADR 197: the same galaxy graph with `returns` written first in FROM. Now the FROM root (`returns`,
// the smaller fact-to-`product` join) IS the cost-optimal root → `kept_already_optimal`.
fn galaxy_optimal_order_sql() -> &'static str {
	"SELECT returns.id, product.id, sales.id FROM returns \
	 JOIN product ON returns.product_id = product.id \
	 JOIN sales ON sales.product_id = product.id"
}

// ADR 203: a 2-hop bridge chain — `orders`(F, 1_000) ⋈ `order_lines`(B1, 5_000) ⋈
// `line_promotions`(B2, 20_000) ⋈ `promotions`(D, 100). Each junction fans out vs its fact-side
// predecessor (inverse-F1: 5_000 >= 1_000 at B1, 20_000 >= 5_000 at B2), so `analyze_bridge` yields two
// links that `analyze_bridge_chain` stitches into one path.
fn bridge_chain_join_order_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(61),
			named_descriptor(
				61,
				"orders",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register chain orders");
	catalog
		.register_descriptor(
			TableId(62),
			named_descriptor(
				62,
				"order_lines",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("order_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register chain order_lines");
	catalog
		.register_descriptor(
			TableId(63),
			named_descriptor(
				63,
				"line_promotions",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("line_id", DataType::Int64, false),
					Field::new("promo_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register chain line_promotions");
	catalog
		.register_descriptor(
			TableId(64),
			named_descriptor(
				64,
				"promotions",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("tier", DataType::Int64, false),
				]),
			),
		)
		.expect("register chain promotions");
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		Some(8_192),
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"order_lines",
		Some(64 * 1024),
		Some(5_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(5_000)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"line_promotions",
		Some(64 * 1024 * 1024),
		Some(20_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(20_000)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(5_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"promotions",
		Some(512),
		Some(100),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
		],
	);
	catalog
}

fn two_hop_bridge_chain_sql() -> &'static str {
	"SELECT orders.id, promotions.id FROM orders \
	 JOIN order_lines ON orders.id = order_lines.order_id \
	 JOIN line_promotions ON order_lines.id = line_promotions.line_id \
	 JOIN promotions ON line_promotions.promo_id = promotions.id"
}

// ADR 204: a reachable 3-hop chain `orders ⋈ order_lines ⋈ line_promotions ⋈ promo_tiers ⋈ promotions`
// (5 relations, 3 junctions). Cardinalities strictly increasing along `F→B1→B2→B3` so the inverse-F1 gate
// (`bridge_rows >= fact_rows`) recognizes each junction as a bridge; `analyze_bridge` yields 3 stitching
// links. Each junction has distinct in/out keys (in/out signatures differ), mirroring the 2-hop fixture.
fn three_hop_bridge_chain_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(71),
			named_descriptor(
				71,
				"orders",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register 3hop orders");
	catalog
		.register_descriptor(
			TableId(72),
			named_descriptor(
				72,
				"order_lines",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("order_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register 3hop order_lines");
	catalog
		.register_descriptor(
			TableId(73),
			named_descriptor(
				73,
				"line_promotions",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("line_id", DataType::Int64, false),
					Field::new("tier_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register 3hop line_promotions");
	catalog
		.register_descriptor(
			TableId(74),
			named_descriptor(
				74,
				"promo_tiers",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("promotion_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register 3hop promo_tiers");
	catalog
		.register_descriptor(
			TableId(75),
			named_descriptor(
				75,
				"promotions",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register 3hop promotions");
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		Some(8_192),
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"order_lines",
		Some(64 * 1024),
		Some(5_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(5_000)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"line_promotions",
		Some(64 * 1024 * 1024),
		Some(20_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(20_000)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(5_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(20_000)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"promo_tiers",
		Some(256 * 1024 * 1024),
		Some(80_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(80_000)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"promotions",
		Some(512),
		Some(100),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	catalog
}

fn three_hop_bridge_chain_sql() -> &'static str {
	"SELECT orders.id, promotions.id FROM orders \
	 JOIN order_lines ON orders.id = order_lines.order_id \
	 JOIN line_promotions ON order_lines.id = line_promotions.line_id \
	 JOIN promo_tiers ON line_promotions.tier_id = promo_tiers.id \
	 JOIN promotions ON promo_tiers.promotion_id = promotions.id"
}

// ADR 204: a generated linear bridge chain of `hops` junctions (`hops + 2` relations
// `rel0 ⋈ rel1 ⋈ … ⋈ rel{hops+1}`). Each junction `rel_i` has a `back` FK (its in-signature) and its `id`
// PK (its out-signature) — distinct — and geometric-growing cardinality (`1000 << i`) so the inverse-F1
// gate recognizes every junction as a bridge. Used to exercise the supported-breadth cap at the boundary.
fn linear_bridge_chain(hops: usize) -> (MockCatalog, String) {
	let mut catalog = MockCatalog::new();
	let n = hops + 2;
	for i in 0..n {
		let id: u8 = 80 + i as u8;
		let name = format!("rel{i}");
		let fields = if i == 0 {
			vec![Field::new("id", DataType::Int64, false)]
		} else {
			vec![
				Field::new("id", DataType::Int64, false),
				Field::new("back", DataType::Int64, false),
			]
		};
		catalog
			.register_descriptor(
				TableId(u64::from(id)),
				named_descriptor(id, &name, Schema::new(fields)),
			)
			.expect("register chain rel");
		let rows = 1_000u64 << i;
		let mut facts = vec![(ColumnOrdinal(0), DistinctCountFact::Exact(rows))];
		if i > 0 {
			// `back` FK → the previous relation's rows (a clean FK).
			facts.push((
				ColumnOrdinal(1),
				DistinctCountFact::Exact(1_000u64 << (i - 1)),
			));
		}
		seed_table_optimizer_facts(&mut catalog, &name, Some(8_192u64 << i), Some(rows), facts);
	}
	let mut sql = String::from("SELECT rel0.id FROM rel0");
	for i in 1..n {
		sql.push_str(&format!(
			" JOIN rel{i} ON rel{prev}.id = rel{i}.back",
			prev = i - 1
		));
	}
	(catalog, sql)
}

// Same chain, but a highly selective predicate on the `D`-end (`promotions.tier`, ndv 100) shrinks the
// `B2⋈D` entry hop below the `F⋈B1` entry hop — so the cheaper driver-end flips to `promotions`.
fn selective_two_hop_bridge_chain_sql() -> &'static str {
	"SELECT orders.id, promotions.id FROM orders \
	 JOIN order_lines ON orders.id = order_lines.order_id \
	 JOIN line_promotions ON order_lines.id = line_promotions.line_id \
	 JOIN promotions ON line_promotions.promo_id = promotions.id \
	 WHERE promotions.tier = 7"
}

// ADR 203: a reachable 2-hop chain is recognized as a single path and emits an observe-only
// `join_order_bridge_chain` cockpit row naming both hops. Must fail on `main` (no chain recognizer).
#[tokio::test]
async fn two_hop_bridge_chain_is_recognized() {
	let catalog = bridge_chain_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", two_hop_bridge_chain_sql()))
		.expect("plan chain EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let chain_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge_chain".to_string())))
		.expect("bridge chain row");
	assert_utf8_contains(
		&chain_row[1],
		"chain[orders->order_lines->line_promotions->promotions]",
	);
	assert_eq!(
		chain_row[2],
		ScalarValue::Utf8(Some("observed".to_string()))
	);
	// Clean FK chain: the F-end `orders` (entry 5_000) is the cheaper driver vs the D-end
	// (`line_promotions⋈promotions`, 20_000) — the path cost was evaluated and the AST end kept.
	assert_utf8_contains(&chain_row[3], "action=kept_already_optimal");
	assert_utf8_contains(&chain_row[3], "preferred_root=orders");
}

// ADR 205: a reachable chain whose AST driver-end is not the cheaper one now ACTUATES (204 only observed
// it). A selective predicate on the `D`-end (`promotions.tier = 7`) makes `B2⋈D` the cheaper entry hop, so
// the chain reverses to drive from `promotions`: the `join_order_bridge_chain` row flips to `reordered`
// with `applied_root=promotions`. Must fail on `main` (204 is observe-only — no chain in the actuation loop).
#[tokio::test]
async fn two_hop_bridge_chain_reroots_to_cheaper_driver_end() {
	let catalog = bridge_chain_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", selective_two_hop_bridge_chain_sql()))
		.expect("plan selective chain EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let chain_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge_chain".to_string())))
		.expect("bridge chain row");
	assert_utf8_contains(
		&chain_row[1],
		"chain[orders->order_lines->line_promotions->promotions]",
	);
	assert_eq!(
		chain_row[2],
		ScalarValue::Utf8(Some("reordered".to_string()))
	);
	assert_utf8_contains(&chain_row[3], "applied_root=promotions");
}

// ADR 205 load-bearing discriminator (the 3-way partition): a multi-junction chain actuates as a CHAIN,
// never as a partial single-bridge re-root around its first junction. The selective chain accrues a
// `bridge_chain_root, reordered` decision and NO `bridge_root, reordered` decision — proving the Bridge
// actuation arm is gated to one junction. Fails under the ungated Bridge arm (`links.first()`).
#[tokio::test]
async fn chain_query_does_not_trigger_single_bridge_actuation() {
	let catalog = bridge_chain_join_order_catalog();
	let config = auto_broadcast_config(64 * 1024 * 1024);
	let _ = bind(
		&SqlPlanner::new()
			.plan(selective_two_hop_bridge_chain_sql())
			.expect("plan selective chain"),
		&catalog,
		&config,
	)
	.await
	.expect("bind selective chain");
	let rows = optimizer_decision_rows(&catalog, &config).await;
	assert_eq!(
		optimizer_decision_count(&rows, "bridge_chain_root", "reordered", None),
		1,
		"a multi-junction chain actuates as a chain: {rows:?}"
	);
	assert!(
		!rows
			.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("bridge_root".to_string()))),
		"the single-bridge arm must NOT fire on a chain: {rows:?}"
	);
}

// ADR 203 D3 proof (ADR 205: an OBSERVED chain): under the sampled verdict path, a recognized non-actuated
// chain accrues a `bridge_chain_root` runtime verdict — the measured per-hop fan-out feeds the path scorer
// through the existing 201 `measured_verdicts_from_summaries` seam, with a determinate verdict once both
// ends are measured. (The clean FK chain keeps its AST order, so it stays observed; an actuated chain's
// verdict is deferred — 205.)
#[tokio::test]
async fn chain_verdict_vindicates_or_faults_recommended_order() {
	let catalog = bridge_chain_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(two_hop_bridge_chain_sql())
		.expect("plan clean chain");
	let binding =
		bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind chain analyze");
	let observations = uniform_observations(&binding);
	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure chain verdicts");
	let chain_verdict = verdicts
		.iter()
		.find(|verdict| verdict.kind == OptimizerDecisionKind::BridgeChainRoot)
		.expect("chain accrues a bridge_chain_root verdict");
	// Both ends measured → a determinate verdict (vindicated or a fault), never Indeterminate.
	assert_ne!(
		chain_verdict.verdict,
		OptimizerRuntimeVerdictClass::Indeterminate,
		"a measured chain yields a determinate verdict: {verdicts:?}"
	);
}

// ADR 205 F1: the 3-way partition holds on the VERDICT log. A chain query also carries per-junction bridge
// evidence (`bridge_order`), but the sampled verdict log emits only the authoritative `BridgeChainRoot` —
// never a partial first-junction `BridgeRoot` (judged at `links.first()` alone). Fails under an ungated
// bridge verdict arm (which would double-count both kinds for one chain query).
#[tokio::test]
async fn chain_verdict_log_has_no_partial_bridge_verdict() {
	let catalog = bridge_chain_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(two_hop_bridge_chain_sql())
		.expect("plan clean chain");
	let binding =
		bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind chain analyze");
	let observations = uniform_observations(&binding);
	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure chain verdicts");
	let kinds = verdict_kinds(&verdicts);
	assert!(
		kinds.contains(&OptimizerDecisionKind::BridgeChainRoot),
		"a chain emits its authoritative chain verdict: {kinds:?}"
	);
	assert!(
		!kinds.contains(&OptimizerDecisionKind::BridgeRoot),
		"a chain must NOT emit a partial first-junction bridge verdict: {kinds:?}"
	);
}

// ADR 206: an ACTUATED chain is now judged (205 deferred this). The selective chain reverses to drive from
// `promotions`; under EXPLAIN ANALYZE its `BridgeChainRoot` verdict is emitted, keyed to the REVERSED plan's
// measured costs (via `reverse_chain_evidence`), and is determinate. Must fail on `main` (205 suppresses it).
#[tokio::test]
async fn actuated_chain_verdict_vindicates_applied_root() {
	let catalog = bridge_chain_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(selective_two_hop_bridge_chain_sql())
		.expect("plan selective chain");
	let binding =
		bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind selective chain analyze");
	let observations = uniform_observations(&binding);
	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure chain verdicts");
	let chain_verdict = verdicts
		.iter()
		.find(|verdict| verdict.kind == OptimizerDecisionKind::BridgeChainRoot)
		.expect("actuated chain accrues a bridge_chain_root verdict");
	assert_ne!(
		chain_verdict.verdict,
		OptimizerRuntimeVerdictClass::Indeterminate,
		"the actuated chain verdict keys to the reversed plan's measured costs: {verdicts:?}"
	);
}

// ADR 206 D1: the partition-complete `graph_actuated` gate. An actuated chain suppresses its orphan tree
// verdicts (its re-rooted FROM tree never accrued), emitting only its authoritative `BridgeChainRoot` — no
// `SnowflakeRoot`/`SnowflakeBranch`, no partial `BridgeRoot`. Fails under the stale galaxy/bridge-only gate.
#[tokio::test]
async fn actuated_chain_emits_no_orphan_tree_verdict() {
	let catalog = bridge_chain_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(selective_two_hop_bridge_chain_sql())
		.expect("plan selective chain");
	let binding =
		bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind selective chain analyze");
	let observations = uniform_observations(&binding);
	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure chain verdicts");
	let kinds = verdict_kinds(&verdicts);
	assert!(
		kinds.contains(&OptimizerDecisionKind::BridgeChainRoot),
		"an actuated chain emits its own verdict: {kinds:?}"
	);
	assert!(
		!has_tree_verdict(&kinds),
		"an actuated chain emits no orphan tree verdict: {kinds:?}"
	);
	assert!(
		!kinds.contains(&OptimizerDecisionKind::BridgeRoot),
		"an actuated chain emits no partial bridge verdict: {kinds:?}"
	);
}

// ADR 206: the actuated chain verdict is built from REVERSED evidence whose targets resolve against the
// REVERSED plan's executed joins (not the AST evidence's). Two checks: (1) every reversed-evidence target
// is a real executed replay id (the re-matching is sound, not garbage), and (2) exploding the reversed
// declared-end's entry faults the reversal — the verdict reads the reversed declared cost, so a broken
// `reverse_chain_evidence` (unresolved targets → Indeterminate) fails this.
#[tokio::test]
async fn actuated_chain_verdict_uses_reversed_plan_targets() {
	let catalog = bridge_chain_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(selective_two_hop_bridge_chain_sql())
		.expect("plan selective chain");
	let binding =
		bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind selective chain analyze");
	let ast_chain = binding
		.render_trace
		.bridge_chain_order
		.as_ref()
		.expect("chain recognized");
	let reversed = reverse_chain_evidence(ast_chain, &binding.render_trace.join_order_edges)
		.expect("reversed evidence resolves against the reversed plan's edges");
	// (1) every reversed target is a real executed join.
	for hop in &reversed.hops {
		for target in [hop.incoming_target, hop.outgoing_target] {
			assert!(
				binding
					.replay_ids
					.iter()
					.any(|id| id.0 as usize == target.join_index),
				"reversed target {target:?} must be an executed reversed-plan join: {:?}",
				binding.replay_ids
			);
		}
	}
	// (2) exploding the reversed declared-end's entry faults the reversal (reads reversed declared cost).
	let reversed_entry = reversed
		.hops
		.first()
		.expect("reversed head")
		.incoming_target
		.join_index as u32;
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(3),
			output_rows: if replay_id.0 == reversed_entry {
				RowCount::new(10_000_000)
			} else {
				RowCount::new(3)
			},
		})
		.collect::<Vec<_>>();
	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure chain verdicts");
	let chain_verdict = verdicts
		.iter()
		.find(|verdict| verdict.kind == OptimizerDecisionKind::BridgeChainRoot)
		.expect("actuated chain verdict");
	assert!(
		matches!(
			chain_verdict.verdict,
			OptimizerRuntimeVerdictClass::MissedWin | OptimizerRuntimeVerdictClass::OverEager
		),
		"exploding the reversed declared-end faults the reversal: {:?}",
		chain_verdict.verdict
	);
}

// ADR 203 F1 lock: the middle hop is FOLDED into the compounded chain verdict, not ignored. Only the
// middle hop `B1⋈B2` (`head.outgoing_target`) explodes (10M rows) while both entry hops stay small (10).
// The declared `orders`-end's compounded cost is the PEAK over its measured path = the 10M middle, which
// exceeds the `D`-end estimate → `MissedWin`. The rejected endcap-only model (declared cost = entry hop =
// 10) would call this `Vindicated`, so this test FAILS under the two-endcap alternative — locking the
// fold. The middle hop is the recommendation heuristic's blind spot; this is the verdict that catches it.
#[tokio::test]
async fn chain_verdict_folds_middle_hop_explosion() {
	let catalog = bridge_chain_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(two_hop_bridge_chain_sql())
		.expect("plan chain");
	let binding =
		bind_explain_analyze(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024))
			.await
			.expect("bind chain analyze");
	let chain = binding
		.render_trace
		.bridge_chain_order
		.as_ref()
		.expect("chain recognized at bind time");
	// The middle hop is the head's outgoing edge (`B1⋈B2`); make ONLY it explode.
	let middle_replay = chain.hops[0].outgoing_target.join_index as u32;
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(3),
			output_rows: if replay_id.0 == middle_replay {
				RowCount::new(10_000_000)
			} else {
				RowCount::new(10)
			},
		})
		.collect::<Vec<_>>();
	let verdicts =
		measured_explain_analyze_verdicts(&binding, &observations).expect("measure chain verdicts");
	let chain_verdict = verdicts
		.iter()
		.find(|verdict| verdict.kind == OptimizerDecisionKind::BridgeChainRoot)
		.expect("chain accrues a bridge_chain_root verdict");
	assert_eq!(
		chain_verdict.verdict,
		OptimizerRuntimeVerdictClass::MissedWin,
		"an exploding middle hop must fold into the compounded verdict: {verdicts:?}"
	);
}

// ADR 204: a reachable 3-hop chain is recognized as one path and emits an observe-only
// `join_order_bridge_chain` row naming all five relations. Must FAIL under the 203 recognizer (`len()!=2` →
// no chain row for a 3-hop query). The clean FK chain drives cheapest from the `orders` end (kept).
#[tokio::test]
async fn three_hop_chain_is_recognized() {
	let catalog = three_hop_bridge_chain_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", three_hop_bridge_chain_sql()))
		.expect("plan 3-hop chain EXPLAIN");
	let (_schema, rows) = bind_explain_rows(
		&logical,
		&catalog,
		&auto_broadcast_config(256 * 1024 * 1024),
	)
	.await;
	let chain_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge_chain".to_string())))
		.expect("bridge chain row");
	assert_utf8_contains(
		&chain_row[1],
		"chain[orders->order_lines->line_promotions->promo_tiers->promotions]",
	);
	assert_eq!(
		chain_row[2],
		ScalarValue::Utf8(Some("observed".to_string()))
	);
	assert_utf8_contains(&chain_row[3], "action=kept_already_optimal");
	assert_utf8_contains(&chain_row[3], "preferred_root=orders");
}

// ADR 204 headline: the compounded verdict folds EVERY middle, not just the head's. Only the *interior*
// middle `line_promotions⋈promo_tiers` (`hops[1].outgoing`) explodes; both entry hops + the head middle
// stay small. The declared `orders`-end's peak is lifted by the 10M interior middle above the `D`-end
// estimate → `MissedWin`. Fails two ways on the rejected alternatives: the 2-hop recognizer (no chain row
// for a 3-hop query) AND the 2-hop cost model (peaks only `head.outgoing` → misses the interior →
// `Vindicated`).
#[tokio::test]
async fn three_hop_chain_interior_middle_explosion_is_missed_win() {
	let catalog = three_hop_bridge_chain_catalog();
	let logical = SqlPlanner::new()
		.plan(three_hop_bridge_chain_sql())
		.expect("plan 3-hop chain");
	let binding = bind_explain_analyze(
		&logical,
		&catalog,
		&auto_broadcast_config(256 * 1024 * 1024),
	)
	.await
	.expect("bind 3-hop chain analyze");
	let chain = binding
		.render_trace
		.bridge_chain_order
		.as_ref()
		.expect("3-hop chain recognized at bind time");
	assert_eq!(chain.hops.len(), 3, "a 3-hop chain has three links");
	// The interior middle is the second hop's outgoing edge (`line_promotions⋈promo_tiers`), NOT the head's.
	let interior_replay = chain.hops[1].outgoing_target.join_index as u32;
	let observations = binding
		.replay_ids
		.iter()
		.map(|replay_id| JoinRuntimeObservation {
			replay_id: *replay_id,
			strategy: JoinStrategy::HashPartitioned,
			build_rows: RowCount::new(3),
			probe_rows: RowCount::new(3),
			output_rows: if replay_id.0 == interior_replay {
				RowCount::new(10_000_000)
			} else {
				RowCount::new(10)
			},
		})
		.collect::<Vec<_>>();
	let verdicts = measured_explain_analyze_verdicts(&binding, &observations)
		.expect("measure 3-hop chain verdicts");
	let chain_verdict = verdicts
		.iter()
		.find(|verdict| verdict.kind == OptimizerDecisionKind::BridgeChainRoot)
		.expect("chain accrues a bridge_chain_root verdict");
	assert_eq!(
		chain_verdict.verdict,
		OptimizerRuntimeVerdictClass::MissedWin,
		"an exploding INTERIOR middle must fold into the compounded verdict: {verdicts:?}"
	);
}

// ADR 204: the supported-breadth cap. A chain of exactly `MAX_BRIDGE_CHAIN_HOPS` (4) junctions is
// recognized; one junction deeper declines (no chain row) rather than emitting an unvalidated verdict.
// Locks the cap boundary (off-by-one) — fails under an uncapped walk (which would emit a row at k=5).
#[tokio::test]
async fn chain_supported_breadth_cap_declines_past_boundary() {
	let chain_row = |rows: &[Vec<ScalarValue>]| {
		rows.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge_chain".to_string())))
	};
	// k = 4 (== cap): recognized.
	let (at_cap_catalog, at_cap_sql) = linear_bridge_chain(4);
	let at_cap = SqlPlanner::new()
		.plan(&format!("EXPLAIN {at_cap_sql}"))
		.expect("plan at-cap chain");
	let (_schema, at_cap_rows) =
		bind_explain_rows(&at_cap, &at_cap_catalog, &auto_broadcast_config(1 << 30)).await;
	assert!(
		chain_row(&at_cap_rows),
		"a chain at the supported breadth (k=4) is recognized: {at_cap_rows:?}"
	);
	// k = 5 (> cap): declined.
	let (over_catalog, over_sql) = linear_bridge_chain(5);
	let over = SqlPlanner::new()
		.plan(&format!("EXPLAIN {over_sql}"))
		.expect("plan over-cap chain");
	let (_schema, over_rows) =
		bind_explain_rows(&over, &over_catalog, &auto_broadcast_config(1 << 30)).await;
	assert!(
		!chain_row(&over_rows),
		"a chain past the supported breadth (k=5) declines: {over_rows:?}"
	);
}

// ADR 204: a branch declines. `rel0` is the fact of TWO bridges (`rel1→rel3` and `rel2→rel4`), so the two
// links share a `fact` — the recognizer's `by_fact` collision rejects it. Two links are present, so a mere
// links-count recognizer would wrongly emit a chain row; the structural walk declines (`None`).
#[tokio::test]
async fn branching_bridge_chain_is_declined() {
	let mut catalog = MockCatalog::new();
	for (id, name, fields, rows) in [
		(90u8, "rel0", vec!["id"], 1_000u64),
		(91, "rel1", vec!["id", "back"], 5_000),
		(92, "rel2", vec!["id", "back"], 5_000),
		(93, "rel3", vec!["id", "back"], 100),
		(94, "rel4", vec!["id", "back"], 100),
	] {
		let schema = Schema::new(
			fields
				.iter()
				.map(|f| Field::new(*f, DataType::Int64, false))
				.collect::<Vec<_>>(),
		);
		catalog
			.register_descriptor(TableId(u64::from(id)), named_descriptor(id, name, schema))
			.expect("register branch rel");
		let mut facts = vec![(ColumnOrdinal(0), DistinctCountFact::Exact(rows))];
		if fields.len() > 1 {
			facts.push((ColumnOrdinal(1), DistinctCountFact::Exact(1_000)));
		}
		seed_table_optimizer_facts(&mut catalog, name, Some(8_192), Some(rows), facts);
	}
	// rel0 forks into two bridges: rel1→rel3 and rel2→rel4.
	let sql = "SELECT rel0.id FROM rel0 \
		 JOIN rel1 ON rel0.id = rel1.back \
		 JOIN rel3 ON rel1.id = rel3.back \
		 JOIN rel2 ON rel0.id = rel2.back \
		 JOIN rel4 ON rel2.id = rel4.back";
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {sql}"))
		.expect("plan branching shape");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(1 << 30)).await;
	assert!(
		!rows
			.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge_chain".to_string()))),
		"a branch (shared fact) must decline, not be mis-stitched into a chain: {rows:?}"
	);
}

// ADR 203 load-bearing discriminator: a plain single bridge `orders ⋈ order_items ⋈ products` emits the
// 199 `join_order_bridge` row but NO chain row — proving the path-stitch decides, not "two links present."
#[tokio::test]
async fn bridge_chain_recognizer_distinguishes_chain_from_single_junction() {
	let catalog = bridge_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", bridge_chain_order_sql()))
		.expect("plan single bridge EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	assert!(
		rows.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge".to_string()))),
		"single bridge still emits its 199 bridge row"
	);
	assert!(
		!rows
			.iter()
			.any(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge_chain".to_string()))),
		"a single junction must not be recognized as a chain: {rows:?}"
	);
}

// ADR 196: a many-to-many bridge — `orders`(fact, 1_000) ⋈ `order_items`(junction, 10_000) ⋈
// `products`(dim, 100). The junction is NOT dimension-like relative to its fact-side probe
// (10_000 >= 1_000), so the inverse-F1 gate recognizes a bridge. Contrast the snowflake branch
// `customers`(5_000) < `orders`(100_000), which stays dimension-like.
fn bridge_join_order_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(51),
			named_descriptor(
				51,
				"orders",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register bridge orders");
	catalog
		.register_descriptor(
			TableId(52),
			named_descriptor(
				52,
				"order_items",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("order_id", DataType::Int64, false),
					Field::new("product_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register bridge order_items");
	catalog
		.register_descriptor(
			TableId(53),
			named_descriptor(
				53,
				"products",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register bridge products");
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		Some(8_192),
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"order_items",
		Some(64 * 1024 * 1024),
		Some(10_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"products",
		Some(512),
		Some(100),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	catalog
}

fn bridge_chain_order_sql() -> &'static str {
	"SELECT orders.id, order_items.order_id, products.id FROM orders \
	 JOIN order_items ON orders.id = order_items.order_id \
	 JOIN products ON order_items.product_id = products.id"
}

// ADR 199: a bridge whose dimension carries a filterable `category` column (ndv 10). A selective
// `WHERE products.category = N` shrinks the products source rows ~10x, so the `B⋈D` outgoing edge
// estimate drops below the `F⋈B` incoming edge — the relation-rooted cost then prefers rooting the
// dimension (filter-first), unlike the clean FK bridge above.
fn selective_bridge_join_order_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(51),
			named_descriptor(
				51,
				"orders",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register bridge orders");
	catalog
		.register_descriptor(
			TableId(52),
			named_descriptor(
				52,
				"order_items",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("order_id", DataType::Int64, false),
					Field::new("product_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register bridge order_items");
	catalog
		.register_descriptor(
			TableId(53),
			named_descriptor(
				53,
				"products",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("category", DataType::Int64, false),
				]),
			),
		)
		.expect("register bridge products");
	seed_table_optimizer_facts(
		&mut catalog,
		"orders",
		Some(8_192),
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(1_000))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"order_items",
		Some(64 * 1024 * 1024),
		Some(10_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(1_000)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"products",
		Some(512),
		Some(100),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(10)),
		],
	);
	catalog
}

fn selective_bridge_order_sql() -> &'static str {
	"SELECT orders.id, order_items.order_id, products.id FROM orders \
	 JOIN order_items ON orders.id = order_items.order_id \
	 JOIN products ON order_items.product_id = products.id \
	 WHERE products.category = 5"
}

fn mediated_non_key_cross_join_catalog(
	a_byte_upper_bound: u64,
	fact_byte_upper_bound: u64,
	b_byte_upper_bound: u64,
) -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(81),
			named_descriptor(
				81,
				"a",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("a_payload", DataType::Int64, false),
				]),
			),
		)
		.expect("register non-key a");
	catalog
		.register_descriptor(
			TableId(82),
			named_descriptor(
				82,
				"fact",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("a_id", DataType::Int64, false),
					Field::new("b_id", DataType::Int64, false),
					Field::new("fact_payload", DataType::Int64, false),
				]),
			),
		)
		.expect("register non-key fact");
	catalog
		.register_descriptor(
			TableId(83),
			named_descriptor(
				83,
				"b",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("b_payload", DataType::Int64, false),
				]),
			),
		)
		.expect("register non-key b");
	seed_table_optimizer_facts(
		&mut catalog,
		"a",
		Some(a_byte_upper_bound),
		Some(2_500),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(2_500))],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"fact",
		Some(fact_byte_upper_bound),
		Some(100_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(100_000)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(2_500)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(2_500)),
		],
	);
	seed_table_optimizer_facts(
		&mut catalog,
		"b",
		Some(b_byte_upper_bound),
		Some(2_500),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(2_500))],
	);
	catalog
}

fn non_key_cross_join_sql() -> &'static str {
	"SELECT a.id AS a_id, b.id AS b_id, fact.id AS fact_id FROM a, b, fact \
	 WHERE fact.a_id = a.id \
	   AND fact.b_id = b.id"
}

fn non_key_cross_join_star_sql() -> &'static str {
	"SELECT * FROM a, b, fact \
	 WHERE fact.a_id = a.id \
	   AND fact.b_id = b.id"
}

// ADR 199 positive gate (binder level): a bridge with a selective predicate on the dimension reroots
// to the dimension. The EXPLAIN bridge receipt flips to `action=reordered; applied_root=products`, and
// the recursion terminates (the re-rooted bridge is not re-rooted again).
#[tokio::test]
async fn bridge_reroots_to_selective_dimension() {
	let catalog = selective_bridge_join_order_catalog();
	let logical = SqlPlanner::new()
		.plan(&format!("EXPLAIN {}", selective_bridge_order_sql()))
		.expect("plan selective bridge EXPLAIN");
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let bridge_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge".to_string())))
		.expect("bridge row");
	assert_eq!(
		bridge_row[2],
		ScalarValue::Utf8(Some("reordered".to_string()))
	);
	assert_utf8_contains(&bridge_row[3], "action=reordered");
	assert_utf8_contains(&bridge_row[3], "applied_root=products");
}

// ADR 331 Phase 1b: the smaller repro. Keep the fixture to three relations, use the mediated
// join shape, and capture only a compact join-decision summary. This remains a diagnostic surrogate,
// not the official q08/q09 shape.
#[tokio::test]
async fn mediated_join_stays_keyed_at_scale() {
	let keyed_catalog =
		mediated_non_key_cross_join_catalog(4 * 1024 * 1024, 4 * 1024 * 1024, 4 * 1024 * 1024);
	let scaled_catalog =
		mediated_non_key_cross_join_catalog(12 * 1024 * 1024, 4 * 1024 * 1024, 12 * 1024 * 1024);
	let logical = SqlPlanner::new()
		.plan(non_key_cross_join_sql())
		.expect("plan mediated comma cross");
	let default_cap = auto_broadcast_config(8 * 1024 * 1024);
	let (plan, trace, render_trace) =
		bind_with_explain_trace(&logical, &keyed_catalog, &default_cap)
			.await
			.expect("bind default-cap mediated comma cross explain trace");

	assert_eq!(trace.join_decisions.len(), 2);
	assert_eq!(render_trace.join_order_edges.len(), 2);
	assert!(
		render_trace.join_order_edges.iter().all(|edge| {
			edge.join_kind == JoinKind::Inner
				&& edge.inner_equi
				&& !edge.left_key_signature.is_empty()
				&& !edge.right_key_signature.is_empty()
		}),
		"small mediated join must stay keyed, got: {}",
		join_order_edge_summary(&render_trace)
	);
	let summary = join_decision_summary(&trace);
	assert!(
		summary.contains("broadcast"),
		"expected a compact join summary, got: {summary}"
	);
	assert_eq!(
		field_names(&plan.schema()),
		vec!["a_id", "b_id", "fact_id"],
		"projected mediated join must preserve SQL projection order"
	);

	let (scaled_plan, scaled_trace, scaled_render_trace) =
		bind_with_explain_trace(&logical, &scaled_catalog, &default_cap)
			.await
			.expect("scaled mediated join should stay keyed under the default cap");
	assert_eq!(scaled_trace.join_decisions.len(), 2);
	assert_eq!(scaled_render_trace.join_order_edges.len(), 2);
	assert!(
		scaled_render_trace.join_order_edges.iter().all(|edge| {
			edge.join_kind == JoinKind::Inner
				&& edge.inner_equi
				&& !edge.left_key_signature.is_empty()
				&& !edge.right_key_signature.is_empty()
		}),
		"scaled mediated join must stay keyed, got: {}",
		join_order_edge_summary(&scaled_render_trace)
	);
	let scaled_summary = join_decision_summary(&scaled_trace);
	assert!(
		scaled_summary.contains("broadcast"),
		"expected a compact scaled join summary, got: {scaled_summary}"
	);
	assert_eq!(
		field_names(&scaled_plan.schema()),
		vec!["a_id", "b_id", "fact_id"],
		"scaled projected mediated join must preserve SQL projection order"
	);

	let star_logical = SqlPlanner::new()
		.plan(non_key_cross_join_star_sql())
		.expect("plan mediated comma cross star");
	let star_plan = bind(&star_logical, &scaled_catalog, &default_cap)
		.await
		.expect("scaled mediated star join should bind under the default cap");
	assert_eq!(
		field_names(&star_plan.schema()),
		vec![
			"id",
			"a_payload",
			"id",
			"b_payload",
			"id",
			"a_id",
			"b_id",
			"fact_payload"
		],
		"reassociated mediated star join must preserve FROM column order"
	);
}

// ADR 199: the Rune frontend (primary interface) actuates the selective bridge identically to SQL — the
// same `action=reordered; applied_root=products` receipt from a Rune-planned filtered bridge.
#[tokio::test]
async fn rune_bridge_reorder_matches_sql() {
	let catalog = selective_bridge_join_order_catalog();
	let logical = rune_plan(
		r#"
        pub fn main(ctx) {
            ctx.table("orders")
               .auto_join(
                   ctx.table("order_items"),
                   qcol("orders", "id").eq(qcol("order_items", "order_id"))
               )
               .auto_join(
                   ctx.table("products").filter(qcol("products", "category").eq(lit_i64(5))),
                   qcol("order_items", "product_id").eq(qcol("products", "id"))
               )
               .explain()
        }
    "#,
	)
	.await;
	let (_schema, rows) =
		bind_explain_rows(&logical, &catalog, &auto_broadcast_config(64 * 1024 * 1024)).await;
	let bridge_row = rows
		.iter()
		.find(|row| row[0] == ScalarValue::Utf8(Some("join_order_bridge".to_string())))
		.expect("Rune bridge query emits a join_order_bridge row");
	assert_eq!(
		bridge_row[2],
		ScalarValue::Utf8(Some("reordered".to_string()))
	);
	assert_utf8_contains(&bridge_row[3], "applied_root=products");
}

fn mixed_strategy_snowflake_join_order_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	register_snowflake_join_order_tables(&mut catalog);
	seed_snowflake_join_order_facts(
		&mut catalog,
		Some(64 * 1024 * 1024),
		Some(8_192),
		Some(8_192),
		Some(8_192),
		Some(1_024),
	);
	catalog
}

fn register_snowflake_join_order_tables(catalog: &mut MockCatalog) {
	catalog
		.register_descriptor(
			TableId(31),
			named_descriptor(
				31,
				"orders",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
					Field::new("store_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register snowflake orders");
	catalog
		.register_descriptor(
			TableId(32),
			named_descriptor(
				32,
				"customers",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("region_id", DataType::Int64, false),
					Field::new("segment_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register snowflake customers");
	catalog
		.register_descriptor(
			TableId(33),
			named_descriptor(
				33,
				"regions",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register snowflake regions");
	catalog
		.register_descriptor(
			TableId(34),
			named_descriptor(
				34,
				"segments",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register snowflake segments");
	catalog
		.register_descriptor(
			TableId(35),
			named_descriptor(
				35,
				"stores",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register snowflake stores");
}

fn seed_snowflake_join_order_facts(
	catalog: &mut MockCatalog,
	order_bytes: Option<u64>,
	customer_bytes: Option<u64>,
	region_bytes: Option<u64>,
	segment_bytes: Option<u64>,
	store_bytes: Option<u64>,
) {
	seed_table_optimizer_facts(
		catalog,
		"orders",
		order_bytes,
		Some(100_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		catalog,
		"customers",
		customer_bytes,
		Some(5_000),
		vec![
			(ColumnOrdinal(0), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		catalog,
		"regions",
		region_bytes,
		Some(2_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		catalog,
		"segments",
		segment_bytes,
		Some(100),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		catalog,
		"stores",
		store_bytes,
		Some(100),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
}

fn seed_star_join_order_facts_with_bytes(
	catalog: &mut MockCatalog,
	customer_bytes: u64,
	product_bytes: u64,
	store_bytes: u64,
) {
	seed_table_optimizer_facts(
		catalog,
		"orders",
		Some(64 * 1024 * 1024),
		Some(100_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(3), DistinctCountFact::Exact(100)),
		],
	);
	seed_table_optimizer_facts(
		catalog,
		"customers",
		Some(customer_bytes),
		Some(5_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		catalog,
		"products",
		Some(product_bytes),
		Some(1_000),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
	seed_table_optimizer_facts(
		catalog,
		"stores",
		Some(store_bytes),
		Some(100),
		vec![(ColumnOrdinal(0), DistinctCountFact::Exact(100))],
	);
}

async fn star_order_execution_fixture() -> (MockCatalog, Arc<InMemoryTableStorage>) {
	let catalog = hash_star_join_order_catalog();
	let storage = Arc::new(InMemoryTableStorage::new());
	for (table_id, descriptor, batch) in star_order_storage_batches() {
		write_star_order_table(storage.as_ref(), table_id, &descriptor, batch).await;
	}
	(catalog, storage)
}

async fn outer_star_order_execution_fixture() -> (MockCatalog, Arc<InMemoryTableStorage>) {
	let catalog = hash_star_join_order_catalog();
	let storage = Arc::new(InMemoryTableStorage::new());
	for (table_id, descriptor, batch) in outer_star_order_storage_batches() {
		write_star_order_table(storage.as_ref(), table_id, &descriptor, batch).await;
	}
	(catalog, storage)
}

async fn snowflake_order_execution_fixture() -> (MockCatalog, Arc<InMemoryTableStorage>) {
	let mut catalog = snowflake_join_order_catalog();
	seed_snowflake_join_order_facts(
		&mut catalog,
		Some(64 * 1024 * 1024),
		Some(8_192),
		Some(8_192),
		Some(8_192),
		Some(8_192),
	);
	let storage = Arc::new(InMemoryTableStorage::new());
	for (table_id, descriptor, batch) in snowflake_order_storage_batches() {
		write_star_order_table(storage.as_ref(), table_id, &descriptor, batch).await;
	}
	(catalog, storage)
}

async fn mixed_strategy_snowflake_execution_fixture() -> (MockCatalog, Arc<InMemoryTableStorage>) {
	// ADR 192: a genuinely mixed snowflake (segments broadcast-eligible, the rest over the 4_096
	// cap) so the root stays observed (mixed strategy), exercising 182-186 observation on a
	// non-actuating snowflake. Same storage/data as the all-broadcast fixture.
	let catalog = mixed_strategy_snowflake_join_order_catalog();
	let storage = Arc::new(InMemoryTableStorage::new());
	for (table_id, descriptor, batch) in snowflake_order_storage_batches() {
		write_star_order_table(storage.as_ref(), table_id, &descriptor, batch).await;
	}
	(catalog, storage)
}

fn star_order_storage_batches() -> Vec<(TableId, TableSchemaDescriptor, RecordBatch)> {
	let orders = named_descriptor(
		21,
		"orders",
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
			Field::new("product_id", DataType::Int64, false),
			Field::new("store_id", DataType::Int64, false),
		]),
	);
	let customers = named_descriptor(
		22,
		"customers",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	let products = named_descriptor(
		23,
		"products",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	let stores = named_descriptor(
		24,
		"stores",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	vec![
		(
			TableId(21),
			orders.clone(),
			record_batch_i64(
				orders.arrow_schema().expect("orders schema").into(),
				vec![
					vec![1, 2, 3, 4],
					vec![10, 20, 10, 30],
					vec![100, 100, 200, 300],
					vec![1000, 2000, 1000, 3000],
				],
			),
		),
		(
			TableId(22),
			customers.clone(),
			record_batch_i64(
				customers.arrow_schema().expect("customers schema").into(),
				vec![vec![10, 20, 30]],
			),
		),
		(
			TableId(23),
			products.clone(),
			record_batch_i64(
				products.arrow_schema().expect("products schema").into(),
				vec![vec![100, 200, 300]],
			),
		),
		(
			TableId(24),
			stores.clone(),
			record_batch_i64(
				stores.arrow_schema().expect("stores schema").into(),
				vec![vec![1000, 2000, 3000]],
			),
		),
	]
}

fn outer_star_order_storage_batches() -> Vec<(TableId, TableSchemaDescriptor, RecordBatch)> {
	let orders = named_descriptor(
		21,
		"orders",
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
			Field::new("product_id", DataType::Int64, false),
			Field::new("store_id", DataType::Int64, false),
		]),
	);
	let customers = named_descriptor(
		22,
		"customers",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	let products = named_descriptor(
		23,
		"products",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	let stores = named_descriptor(
		24,
		"stores",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	vec![
		(
			TableId(21),
			orders.clone(),
			record_batch_i64(
				orders.arrow_schema().expect("orders schema").into(),
				vec![
					vec![1, 2, 3, 4],
					vec![10, 99, 10, 30],
					vec![100, 100, 999, 100],
					vec![1000, 1000, 1000, 9999],
				],
			),
		),
		(
			TableId(22),
			customers.clone(),
			record_batch_i64(
				customers.arrow_schema().expect("customers schema").into(),
				vec![vec![10, 10, 30]],
			),
		),
		(
			TableId(23),
			products.clone(),
			record_batch_i64(
				products.arrow_schema().expect("products schema").into(),
				vec![vec![100]],
			),
		),
		(
			TableId(24),
			stores.clone(),
			record_batch_i64(
				stores.arrow_schema().expect("stores schema").into(),
				vec![vec![1000, 1000]],
			),
		),
	]
}

fn snowflake_order_storage_batches() -> Vec<(TableId, TableSchemaDescriptor, RecordBatch)> {
	let orders = named_descriptor(
		31,
		"orders",
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
			Field::new("store_id", DataType::Int64, false),
		]),
	);
	let customers = named_descriptor(
		32,
		"customers",
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region_id", DataType::Int64, false),
			Field::new("segment_id", DataType::Int64, false),
		]),
	);
	let regions = named_descriptor(
		33,
		"regions",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	let segments = named_descriptor(
		34,
		"segments",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	let stores = named_descriptor(
		35,
		"stores",
		Schema::new(vec![Field::new("id", DataType::Int64, false)]),
	);
	vec![
		(
			TableId(31),
			orders.clone(),
			record_batch_i64(
				orders.arrow_schema().expect("orders schema").into(),
				vec![
					vec![1, 2, 3, 4],
					vec![10, 20, 10, 30],
					vec![1000, 2000, 1000, 1000],
				],
			),
		),
		(
			TableId(32),
			customers.clone(),
			record_batch_i64(
				customers.arrow_schema().expect("customers schema").into(),
				vec![
					vec![10, 20, 30],
					vec![100, 200, 999],
					vec![10_000, 20_000, 30_000],
				],
			),
		),
		(
			TableId(33),
			regions.clone(),
			record_batch_i64(
				regions.arrow_schema().expect("regions schema").into(),
				vec![vec![100, 200]],
			),
		),
		(
			TableId(34),
			segments.clone(),
			record_batch_i64(
				segments.arrow_schema().expect("segments schema").into(),
				vec![vec![10_000, 20_000]],
			),
		),
		(
			TableId(35),
			stores.clone(),
			record_batch_i64(
				stores.arrow_schema().expect("stores schema").into(),
				vec![vec![1000, 2000]],
			),
		),
	]
}

fn record_batch_i64(schema: SchemaRef, columns: Vec<Vec<i64>>) -> RecordBatch {
	RecordBatch::try_new(
		schema,
		columns
			.into_iter()
			.map(|values| Arc::new(Int64Array::from(values)) as ArrayRef)
			.collect(),
	)
	.expect("record batch")
}

async fn write_star_order_table(
	storage: &InMemoryTableStorage,
	table_id: TableId,
	descriptor: &TableSchemaDescriptor,
	payload: RecordBatch,
) {
	storage
		.write(WriteRequest {
			session_id: SessionId(181),
			query_id: QueryId::from_u128(181),
			tenant_id: TenantId(1),
			table_id,
			table_object_id: descriptor.table_object_id,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			payload,
			profile_hint: Some(QueryProfileHint::Scan),
			trace_context: None,
		})
		.await
		.expect("write star order fixture");
}

async fn execute_star_order_query(
	catalog: &MockCatalog,
	storage: Arc<InMemoryTableStorage>,
	sql: &str,
	config: &PlannerConfig,
) -> (SchemaRef, Vec<Vec<Option<i64>>>) {
	let logical = SqlPlanner::new().plan(sql).expect("plan star order SQL");
	let bound = Binder::new(catalog, config)
		.bind_plan(&logical)
		.await
		.expect("bind star order SQL");
	let batches = Executor::new(storage)
		.execute_bound(&bound.plan, None)
		.await
		.expect("execute star order SQL");
	let schema = batches
		.first()
		.map(RecordBatch::schema)
		.unwrap_or_else(|| bound.plan.schema());
	(schema, normalized_i64_rows(&batches))
}

async fn execute_star_order_plan(
	catalog: &MockCatalog,
	storage: Arc<InMemoryTableStorage>,
	logical: &LogicalPlan,
	config: &PlannerConfig,
) -> (SchemaRef, Vec<Vec<Option<i64>>>) {
	let bound = Binder::new(catalog, config)
		.bind_plan(logical)
		.await
		.expect("bind star order plan");
	let batches = Executor::new(storage)
		.execute_bound(&bound.plan, None)
		.await
		.expect("execute star order plan");
	let schema = batches
		.first()
		.map(RecordBatch::schema)
		.unwrap_or_else(|| bound.plan.schema());
	(schema, normalized_i64_rows(&batches))
}

async fn execute_star_order_query_with_coordinator(
	catalog: Arc<MockCatalog>,
	storage: Arc<InMemoryTableStorage>,
	sql: &str,
	config: PlannerConfig,
) -> (SchemaRef, Vec<Vec<Option<i64>>>) {
	let logical = SqlPlanner::new().plan(sql).expect("plan star order SQL");
	let coordinator = QueryCoordinator::new(catalog, Executor::new(storage), config);
	let batches = coordinator
		.execute_query(&logical, None)
		.await
		.expect("execute star order SQL through coordinator");
	let schema = batches
		.first()
		.map(RecordBatch::schema)
		.expect("star order query returns at least one batch");
	(schema, normalized_i64_rows(&batches))
}

fn normalized_i64_rows(batches: &[RecordBatch]) -> Vec<Vec<Option<i64>>> {
	let mut rows = Vec::new();
	for batch in batches {
		let columns = (0..batch.num_columns())
			.map(|idx| {
				batch
					.column(idx)
					.as_any()
					.downcast_ref::<Int64Array>()
					.unwrap_or_else(|| panic!("expected Int64 column {idx}"))
			})
			.collect::<Vec<_>>();
		for row_idx in 0..batch.num_rows() {
			rows.push(
				columns
					.iter()
					.map(|column| {
						if column.is_null(row_idx) {
							None
						} else {
							Some(column.value(row_idx))
						}
					})
					.collect::<Vec<_>>(),
			);
		}
	}
	rows.sort();
	rows
}

fn schema_signature(schema: &SchemaRef) -> Vec<(String, DataType)> {
	schema
		.fields()
		.iter()
		.map(|field| (field.name().clone(), field.data_type().clone()))
		.collect()
}

fn seed_soft_star_join_order_facts(
	catalog: &mut MockCatalog,
	customer_rows: u64,
	product_rows: u64,
) {
	seed_table_optimizer_facts(
		catalog,
		"orders",
		None,
		Some(100_000),
		vec![
			(
				ColumnOrdinal(1),
				DistinctCountFact::Approx {
					estimate: 100_000,
					rel_err_ppm: 10_000,
				},
			),
			(
				ColumnOrdinal(2),
				DistinctCountFact::Approx {
					estimate: 100_000,
					rel_err_ppm: 10_000,
				},
			),
		],
	);
	seed_table_optimizer_facts(
		catalog,
		"customers",
		None,
		Some(customer_rows),
		vec![(
			ColumnOrdinal(0),
			DistinctCountFact::Approx {
				estimate: 100_000,
				rel_err_ppm: 10_000,
			},
		)],
	);
	seed_table_optimizer_facts(
		catalog,
		"products",
		None,
		Some(product_rows),
		vec![(
			ColumnOrdinal(0),
			DistinctCountFact::Approx {
				estimate: 100_000,
				rel_err_ppm: 10_000,
			},
		)],
	);
}

fn non_optimal_star_order_sql() -> String {
	"SELECT * FROM orders o \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 INNER JOIN products p ON o.product_id = p.id \
	 INNER JOIN stores s ON o.store_id = s.id"
		.to_string()
}

fn non_optimal_star_order_project_sql() -> &'static str {
	"SELECT o.id, c.id, p.id, s.id FROM orders o \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 INNER JOIN products p ON o.product_id = p.id \
	 INNER JOIN stores s ON o.store_id = s.id"
}

fn non_optimal_snowflake_order_sql() -> &'static str {
	"SELECT * FROM orders o \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 INNER JOIN regions r ON c.region_id = r.id \
	 INNER JOIN segments g ON c.segment_id = g.id \
	 INNER JOIN stores s ON o.store_id = s.id"
}

fn mixed_demoted_snowflake_order_sql() -> &'static str {
	"SELECT * FROM orders o \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 LEFT JOIN regions r ON c.region_id = r.id \
	 INNER JOIN segments g ON c.segment_id = g.id \
	 INNER JOIN stores s ON o.store_id = s.id \
	 WHERE r.id IS NOT NULL"
}

fn preserved_outer_star_sql() -> &'static str {
	"SELECT * FROM orders o \
	 LEFT JOIN customers c ON o.customer_id = c.id \
	 LEFT JOIN products p ON o.product_id = p.id \
	 LEFT JOIN stores s ON o.store_id = s.id"
}

fn preserved_outer_star_project_sql() -> &'static str {
	"SELECT o.id, c.id, p.id, s.id FROM orders o \
	 LEFT JOIN customers c ON o.customer_id = c.id \
	 LEFT JOIN products p ON o.product_id = p.id \
	 LEFT JOIN stores s ON o.store_id = s.id"
}

fn preserved_outer_two_dim_sql() -> &'static str {
	"SELECT * FROM orders o \
	 LEFT JOIN customers c ON o.customer_id = c.id \
	 LEFT JOIN products p ON o.product_id = p.id"
}

fn preserved_outer_right_star_plan() -> LogicalPlan {
	let stores_orders = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("stores", "s")),
		right: Box::new(aliased_named_scan("orders", "o")),
		kind: JoinKind::RightOuter,
		on: vec![(qcol("s", "id"), qcol("o", "store_id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let products_stores_orders = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("products", "p")),
		right: Box::new(stores_orders),
		kind: JoinKind::RightOuter,
		on: vec![(qcol("p", "id"), qcol("o", "product_id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	LogicalPlan::Join {
		left: Box::new(aliased_named_scan("customers", "c")),
		right: Box::new(products_stores_orders),
		kind: JoinKind::RightOuter,
		on: vec![(qcol("c", "id"), qcol("o", "customer_id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn preserved_outer_right_star_project_plan() -> LogicalPlan {
	project_ids(preserved_outer_right_star_plan(), &["c", "p", "s", "o"])
}

fn preserved_outer_snowflake_plan() -> LogicalPlan {
	let customers_regions = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("customers", "c")),
		right: Box::new(aliased_named_scan("regions", "r")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("c", "region_id"), qcol("r", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let orders_customers = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("orders", "o")),
		right: Box::new(customers_regions),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("o", "customer_id"), qcol("c", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	LogicalPlan::Join {
		left: Box::new(orders_customers),
		right: Box::new(aliased_named_scan("stores", "s")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("o", "store_id"), qcol("s", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn preserved_outer_snowflake_project_plan() -> LogicalPlan {
	project_ids(preserved_outer_snowflake_plan(), &["o", "c", "r", "s"])
}

fn preserved_outer_deep_snowflake_plan() -> LogicalPlan {
	let customers_regions = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("customers", "c")),
		right: Box::new(aliased_named_scan("regions", "r")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("c", "region_id"), qcol("r", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let customers_regions_segments = LogicalPlan::Join {
		left: Box::new(customers_regions),
		right: Box::new(aliased_named_scan("segments", "g")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("c", "segment_id"), qcol("g", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let orders_customers = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("orders", "o")),
		right: Box::new(customers_regions_segments),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("o", "customer_id"), qcol("c", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	LogicalPlan::Join {
		left: Box::new(orders_customers),
		right: Box::new(aliased_named_scan("stores", "s")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("o", "store_id"), qcol("s", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn preserved_outer_cross_into_subtree_plan() -> LogicalPlan {
	let customers_regions = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("customers", "c")),
		right: Box::new(aliased_named_scan("regions", "r")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("c", "region_id"), qcol("r", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let orders_customers = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("orders", "o")),
		right: Box::new(customers_regions),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("o", "customer_id"), qcol("r", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	LogicalPlan::Join {
		left: Box::new(orders_customers),
		right: Box::new(aliased_named_scan("stores", "s")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("o", "store_id"), qcol("s", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn right_outer_opaque_snowflake_plan() -> LogicalPlan {
	let customers_regions = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("customers", "c")),
		right: Box::new(aliased_named_scan("regions", "r")),
		kind: JoinKind::LeftOuter,
		on: vec![(qcol("c", "region_id"), qcol("r", "id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	let stores_orders = LogicalPlan::Join {
		left: Box::new(aliased_named_scan("stores", "s")),
		right: Box::new(aliased_named_scan("orders", "o")),
		kind: JoinKind::RightOuter,
		on: vec![(qcol("s", "id"), qcol("o", "store_id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	LogicalPlan::Join {
		left: Box::new(customers_regions),
		right: Box::new(stores_orders),
		kind: JoinKind::RightOuter,
		on: vec![(qcol("c", "id"), qcol("o", "customer_id"))],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn project_ids(input: LogicalPlan, aliases: &[&str]) -> LogicalPlan {
	LogicalPlan::Project {
		input: Box::new(input),
		exprs: aliases.iter().map(|alias| qcol(alias, "id")).collect(),
		output_names: vec![None; aliases.len()],
	}
}

fn projected_dimension_preserved_outer_plan() -> LogicalPlan {
	let orders = LogicalPlan::ScanNamed {
		table: TableRef::new("orders"),
		projection: None,
	};
	let projected_customers = LogicalPlan::Project {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("customers"),
			projection: None,
		}),
		exprs: vec![Expr::column("id")],
		output_names: vec![None],
	};
	let products = LogicalPlan::ScanNamed {
		table: TableRef::new("products"),
		projection: None,
	};
	let orders_customers = LogicalPlan::Join {
		left: Box::new(orders),
		right: Box::new(projected_customers),
		kind: JoinKind::LeftOuter,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	};
	LogicalPlan::Join {
		left: Box::new(orders_customers),
		right: Box::new(products),
		kind: JoinKind::LeftOuter,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "product_id"),
			Expr::qualified_column(TableRef::new("products"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn mixed_strategy_snowflake_order_sql() -> &'static str {
	"SELECT * FROM orders o \
	 INNER JOIN stores s ON o.store_id = s.id \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 INNER JOIN regions r ON c.region_id = r.id \
	 INNER JOIN segments g ON c.segment_id = g.id"
}

fn non_optimal_snowflake_order_project_sql() -> &'static str {
	"SELECT o.id, c.id, r.id, g.id, s.id FROM orders o \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 INNER JOIN regions r ON c.region_id = r.id \
	 INNER JOIN segments g ON c.segment_id = g.id \
	 INNER JOIN stores s ON o.store_id = s.id"
}

// ADR 193: after the mixed-strategy root actuates, a recognized snowflake reaches the
// observe-only path only when its shape is outside the root actuator's authorized domain. A
// non-scan-linear probe (the fact table arrives via a `LIMIT` subplan) is such a shape: the root
// actuator requires a scan-linear probe to guarantee a fragmentable left-deep plan, so it declines
// with `ProbeNotScanLinear`, while `analyze_snowflake` still recognizes the full
// orders -> {customers -> {regions, segments}, stores} tree. This is the post-193 evidence path the
// observation machinery is exercised against. The `LIMIT` is wider than the fixture's row count, so
// rows and order are preserved.
fn observed_snowflake_order_sql() -> &'static str {
	"SELECT * FROM (SELECT * FROM orders LIMIT 1000000) o \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 INNER JOIN regions r ON c.region_id = r.id \
	 INNER JOIN segments g ON c.segment_id = g.id \
	 INNER JOIN stores s ON o.store_id = s.id"
}

fn observed_snowflake_order_project_sql() -> &'static str {
	"SELECT o.id, c.id, r.id, g.id, s.id FROM (SELECT * FROM orders LIMIT 1000000) o \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 INNER JOIN regions r ON c.region_id = r.id \
	 INNER JOIN segments g ON c.segment_id = g.id \
	 INNER JOIN stores s ON o.store_id = s.id"
}

fn preferred_hash_snowflake_order_project_sql() -> &'static str {
	"SELECT o.id, c.id, r.id, g.id, s.id FROM orders o \
	 INNER JOIN stores s ON o.store_id = s.id \
	 INNER JOIN customers c ON o.customer_id = c.id \
	 INNER JOIN regions r ON c.region_id = r.id \
	 INNER JOIN segments g ON c.segment_id = g.id"
}

fn preferred_star_order_sql() -> &'static str {
	"SELECT * FROM orders o \
	 INNER JOIN stores s ON o.store_id = s.id \
	 INNER JOIN products p ON o.product_id = p.id \
	 INNER JOIN customers c ON o.customer_id = c.id"
}

fn preferred_star_order_project_sql() -> &'static str {
	"SELECT o.id, c.id, p.id, s.id FROM orders o \
	 INNER JOIN stores s ON o.store_id = s.id \
	 INNER JOIN products p ON o.product_id = p.id \
	 INNER JOIN customers c ON o.customer_id = c.id"
}

fn snowflake_test_edge(
	join_index: usize,
	probe: &str,
	dimension: &str,
	declared_position: usize,
	estimated_rows: u64,
	probe_scan_linear: bool,
) -> JoinOrderEdgeEvidence {
	JoinOrderEdgeEvidence {
		target: JoinTraceTarget { join_index },
		join_kind: JoinKind::Inner,
		probe: Some(probe.to_string()),
		dimension: Some(dimension.to_string()),
		declared_position,
		estimated_rows: Some(RowCount::new(estimated_rows)),
		estimate_source: JoinOrderEstimateSource::SingleKeyNdvContainment,
		inner_equi: true,
		probe_linearity: if probe_scan_linear {
			ProbeLinearity::Linear
		} else {
			ProbeLinearity::Blocked(ProbeLinearityFence::NonBroadcastJoin)
		},
		left_key_signature: vec![format!("{probe}.k")],
		right_key_signature: vec![format!("{dimension}.id")],
		left_endpoint_rows: None,
		right_endpoint_rows: None,
	}
}

// ADR 196: a synthetic edge with explicit key signatures and per-side endpoint rows, for unit-testing
// `analyze_bridge` decline paths (cycles, etc.) without a full catalog.
#[allow(clippy::too_many_arguments)]
fn bridge_test_edge(
	join_index: usize,
	probe: &str,
	dimension: &str,
	left_signature: &str,
	right_signature: &str,
	left_rows: u64,
	right_rows: u64,
) -> JoinOrderEdgeEvidence {
	JoinOrderEdgeEvidence {
		target: JoinTraceTarget { join_index },
		join_kind: JoinKind::Inner,
		probe: Some(probe.to_string()),
		dimension: Some(dimension.to_string()),
		declared_position: join_index,
		estimated_rows: Some(RowCount::new(right_rows)),
		estimate_source: JoinOrderEstimateSource::SingleKeyNdvContainment,
		inner_equi: true,
		probe_linearity: ProbeLinearity::Linear,
		left_key_signature: vec![left_signature.to_string()],
		right_key_signature: vec![right_signature.to_string()],
		left_endpoint_rows: Some(RowCount::new(left_rows)),
		right_endpoint_rows: Some(RowCount::new(right_rows)),
	}
}

fn snowflake_test_decision(
	join_index: usize,
	strategy: JoinStrategy,
	placement: JoinPhysicalPlacement,
) -> DecisionReceipt {
	DecisionReceipt {
		target: JoinTraceTarget { join_index },
		strategy,
		placement,
		reason: JoinStrategyDecisionReason::PinnedByAuthor,
	}
}

fn snowflake_verdict_fixture() -> SnowflakeOrderEvidence {
	SnowflakeOrderEvidence {
		ranking: rank_star_order(
			"o".to_string(),
			vec![
				snowflake_verdict_dim("c", 0, 100),
				snowflake_verdict_dim("s", 1, 10),
			],
			JoinOrderEstimateConfidence::Hard,
			CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP.margin(),
		),
		edge_strategies: vec![
			snowflake_verdict_strategy("c"),
			snowflake_verdict_strategy("s"),
		],
		branches: vec![SnowflakeBranch {
			dimension: "c".to_string(),
			evidence: SnowflakeOrderEvidence {
				ranking: rank_star_order(
					"c".to_string(),
					vec![
						snowflake_verdict_dim("r", 0, 100),
						snowflake_verdict_dim("g", 1, 10),
					],
					JoinOrderEstimateConfidence::Hard,
					CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP.margin(),
				),
				edge_strategies: vec![
					snowflake_verdict_strategy("r"),
					snowflake_verdict_strategy("g"),
				],
				branches: Vec::new(),
			},
		}],
	}
}

fn snowflake_verdict_dim(
	dimension: &str,
	declared_position: usize,
	estimated_rows: u64,
) -> JoinOrderDimEvidence {
	JoinOrderDimEvidence {
		dimension: dimension.to_string(),
		declared_position,
		estimated_rows: RowCount::new(estimated_rows),
		estimate_source: JoinOrderEstimateSource::SingleKeyNdvContainment,
		confidence: JoinOrderEstimateConfidence::Hard,
	}
}

fn snowflake_verdict_strategy(dimension: &str) -> SnowflakeEdgeStrategy {
	SnowflakeEdgeStrategy {
		dimension: dimension.to_string(),
		strategy: JoinStrategy::HashPartitioned,
		placement: JoinPhysicalPlacement::Natural,
		probe_linearity: ProbeLinearity::Linear,
	}
}

fn join_cardinality_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(1),
			named_descriptor(
				1,
				"orders",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register orders");
	catalog
		.register_descriptor(
			TableId(2),
			named_descriptor(
				2,
				"customers",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("region_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register customers");
	catalog
		.register_descriptor(
			TableId(3),
			named_descriptor(
				3,
				"regions",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register regions");
	catalog
}

fn multi_key_join_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	register_multi_key_tables(&mut catalog);
	catalog
}

fn multi_key_parent_join_catalog() -> MockCatalog {
	let mut catalog = multi_key_join_catalog();
	catalog
		.register_descriptor(
			TableId(12),
			named_descriptor(
				12,
				"regions",
				Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			),
		)
		.expect("register multi-key regions");
	catalog
}

fn register_multi_key_tables(catalog: &mut MockCatalog) {
	catalog
		.register_descriptor(
			TableId(10),
			named_descriptor(
				10,
				"order_items",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("order_id", DataType::Int64, false),
					Field::new("line_no", DataType::Int64, false),
					Field::new("shard_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register order_items");
	catalog
		.register_descriptor(
			TableId(11),
			named_descriptor(
				11,
				"line_prices",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("order_id", DataType::Int64, false),
					Field::new("line_no", DataType::Int64, false),
					Field::new("shard_id", DataType::Int64, false),
					Field::new("region_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register line_prices");
}

fn multi_key_join_logical(keys: &[&str]) -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("order_items"),
			projection: None,
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("line_prices"),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: keys
			.iter()
			.map(|key| {
				(
					Expr::qualified_column(TableRef::new("order_items"), *key),
					Expr::qualified_column(TableRef::new("line_prices"), *key),
				)
			})
			.collect(),
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

fn multi_key_parent_join_logical() -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(multi_key_join_logical(&["order_id", "line_no"])),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("regions"),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("line_prices"), "region_id"),
			Expr::qualified_column(TableRef::new("regions"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

async fn bind_multi_key_disjoint_join() -> ScopedExecutionPlan {
	let mut catalog = multi_key_join_catalog();
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"order_items",
		None,
		Some(10_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![
			(ColumnOrdinal(1), range_fact_i64(0, 99, 10_000)),
			(ColumnOrdinal(2), range_fact_i64(0, 99, 10_000)),
		],
	);
	seed_table_optimizer_facts_with_ranges(
		&mut catalog,
		"line_prices",
		None,
		Some(10_000),
		vec![
			(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
			(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
		],
		vec![
			(ColumnOrdinal(1), range_fact_i64(1_000, 1_099, 10_000)),
			(ColumnOrdinal(2), range_fact_i64(50, 149, 10_000)),
		],
	);

	Binder::new(&catalog, &PlannerConfig::default())
		.bind_plan(&multi_key_join_logical(&["order_id", "line_no"]))
		.await
		.expect("bind multi-key disjoint range join")
}

fn float_join_catalog() -> MockCatalog {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(11),
			named_descriptor(
				11,
				"left_floats",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("f", DataType::Float64, false),
				]),
			),
		)
		.expect("register left floats");
	catalog
		.register_descriptor(
			TableId(12),
			named_descriptor(
				12,
				"right_floats",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("f", DataType::Float64, false),
				]),
			),
		)
		.expect("register right floats");
	catalog
}

fn exchanged_named_scan(table: &str, key: &str, partition_count: u32) -> LogicalPlan {
	LogicalPlan::Exchange {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new(table),
			projection: None,
		}),
		partition_count,
		key: vec![Expr::qualified_column(TableRef::new(table), key)],
	}
}

fn aliased_named_scan(table: &str, alias: &str) -> LogicalPlan {
	LogicalPlan::Alias {
		input: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new(table),
			projection: None,
		}),
		alias: TableRef::new(alias),
		columns: Vec::new(),
	}
}

fn exchanged_aliased_named_scan(
	table: &str,
	alias: &str,
	key: &str,
	partition_count: u32,
) -> LogicalPlan {
	LogicalPlan::Exchange {
		input: Box::new(aliased_named_scan(table, alias)),
		partition_count,
		key: vec![Expr::qualified_column(TableRef::new(alias), key)],
	}
}

fn nested_hash_probe_with_tiny_right() -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::Join {
			left: Box::new(exchanged_aliased_named_scan(
				"orders",
				"o",
				"customer_id",
				2,
			)),
			right: Box::new(exchanged_aliased_named_scan("customers", "c1", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("o"), "customer_id"),
				Expr::qualified_column(TableRef::new("c1"), "id"),
			)],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
				build_side: None,
			},
		}),
		right: Box::new(aliased_named_scan("customers", "c2")),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("o"), "customer_id"),
			Expr::qualified_column(TableRef::new("c2"), "id"),
		)],
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Auto,
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestK3 {
	True,
	False,
	Null,
}

impl TestK3 {
	const VALUES: [Self; 3] = [Self::True, Self::False, Self::Null];
}

fn property_common_or_factor_program() -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let p_left = builder.append_column(test_bool_column("p", 0, true));
	let r1 = builder.append_column(test_bool_column("r1", 1, false));
	let left = builder.append_binary(BoundBinaryOp::And, p_left, r1, DataType::Boolean);
	let p_right = builder.append_column(test_bool_column("p", 0, true));
	let r2 = builder.append_column(test_bool_column("r2", 2, false));
	let right = builder.append_binary(BoundBinaryOp::And, p_right, r2, DataType::Boolean);
	let root = builder.append_binary(BoundBinaryOp::Or, left, right, DataType::Boolean);
	builder.push_root(root);
	builder.build()
}

fn property_common_or_three_branch_program() -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let p_left = builder.append_column(test_bool_column("p", 0, true));
	let r1_left = builder.append_column(test_bool_column("r1", 1, false));
	let left = builder.append_binary(BoundBinaryOp::And, p_left, r1_left, DataType::Boolean);
	let p_middle = builder.append_column(test_bool_column("p", 0, true));
	let r2 = builder.append_column(test_bool_column("r2", 2, false));
	let middle = builder.append_binary(BoundBinaryOp::And, p_middle, r2, DataType::Boolean);
	let p_right = builder.append_column(test_bool_column("p", 0, true));
	let r1_right = builder.append_column(test_bool_column("r1", 1, false));
	let right = builder.append_binary(BoundBinaryOp::And, p_right, r1_right, DataType::Boolean);
	let left_or_middle = builder.append_binary(BoundBinaryOp::Or, left, middle, DataType::Boolean);
	let root = builder.append_binary(BoundBinaryOp::Or, left_or_middle, right, DataType::Boolean);
	builder.push_root(root);
	builder.build()
}

fn property_or_branch_strips_to_true_program() -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let p_left = builder.append_column(test_bool_column("p", 0, true));
	let r1 = builder.append_column(test_bool_column("r1", 1, false));
	let left = builder.append_binary(BoundBinaryOp::And, p_left, r1, DataType::Boolean);
	let right = builder.append_column(test_bool_column("p", 0, true));
	let root = builder.append_binary(BoundBinaryOp::Or, left, right, DataType::Boolean);
	builder.push_root(root);
	builder.build()
}

fn property_not_or_program() -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let p = builder.append_column(test_bool_column("p", 0, true));
	let r1 = builder.append_column(test_bool_column("r1", 1, false));
	let disjunction = builder.append_binary(BoundBinaryOp::Or, p, r1, DataType::Boolean);
	let root = builder.append_unary(BoundUnaryOp::Not, disjunction, DataType::Boolean);
	builder.push_root(root);
	builder.build()
}

fn property_not_or_partial_push_program() -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let p = builder.append_column(test_bool_column("p", 0, true));
	let r1 = builder.append_column(test_bool_column("r1", 1, true));
	let r2 = builder.append_column(test_bool_column("r2", 2, false));
	let left = builder.append_binary(BoundBinaryOp::Or, p, r1, DataType::Boolean);
	let disjunction = builder.append_binary(BoundBinaryOp::Or, left, r2, DataType::Boolean);
	let root = builder.append_unary(BoundUnaryOp::Not, disjunction, DataType::Boolean);
	builder.push_root(root);
	builder.build()
}

fn property_top_level_and_program() -> BoundExprProgram {
	let mut builder = BoundExprProgramBuilder::new();
	let p = builder.append_column(test_bool_column("p", 0, true));
	let r1 = builder.append_column(test_bool_column("r1", 1, false));
	let root = builder.append_binary(BoundBinaryOp::And, p, r1, DataType::Boolean);
	builder.push_root(root);
	builder.build()
}

fn test_bool_column(name: &str, ordinal: u32, pbc_eligible: bool) -> ResolvedColumn {
	ResolvedColumn {
		name: ColumnName(name.to_string()),
		ordinal: ColumnOrdinal(ordinal),
		data_type: DataType::Boolean,
		nullable: true,
		pbc_eligible,
	}
}

fn eval_test_program(program: &BoundExprProgram, row: &[TestK3; 3]) -> TestK3 {
	let root = program.single_root().expect("single root");
	eval_test_node(program, root, row)
}

fn eval_test_node(program: &BoundExprProgram, id: BoundExprId, row: &[TestK3; 3]) -> TestK3 {
	match program.node(id).expect("node") {
		BoundExprNode::Column(column) => row[column.ordinal.0 as usize],
		BoundExprNode::Literal(literal) => match program.literals[*literal as usize] {
			ScalarValue::Boolean(Some(true)) => TestK3::True,
			ScalarValue::Boolean(Some(false)) => TestK3::False,
			ScalarValue::Boolean(None) => TestK3::Null,
			_ => panic!("property evaluator only supports Boolean literals"),
		},
		BoundExprNode::Binary { op, left, right } => {
			let left = eval_test_node(program, *left, row);
			let right = eval_test_node(program, *right, row);
			match op {
				BoundBinaryOp::And => k3_and(left, right),
				BoundBinaryOp::Or => k3_or(left, right),
				_ => panic!("property evaluator only supports Boolean binary ops"),
			}
		}
		BoundExprNode::Unary { op, input } => match op {
			BoundUnaryOp::Not => k3_not(eval_test_node(program, *input, row)),
			_ => panic!("property evaluator only supports NOT"),
		},
		_ => panic!("property evaluator only supports Boolean test nodes"),
	}
}

fn k3_not(value: TestK3) -> TestK3 {
	match value {
		TestK3::True => TestK3::False,
		TestK3::False => TestK3::True,
		TestK3::Null => TestK3::Null,
	}
}

fn k3_and(left: TestK3, right: TestK3) -> TestK3 {
	match (left, right) {
		(TestK3::False, _) | (_, TestK3::False) => TestK3::False,
		(TestK3::True, TestK3::True) => TestK3::True,
		_ => TestK3::Null,
	}
}

fn k3_or(left: TestK3, right: TestK3) -> TestK3 {
	match (left, right) {
		(TestK3::True, _) | (_, TestK3::True) => TestK3::True,
		(TestK3::False, TestK3::False) => TestK3::False,
		_ => TestK3::Null,
	}
}

fn descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("region", DataType::Utf8, true),
		Field::new("ts", DataType::Int64, true),
	]);

	TableSchemaDescriptor::new(
		TableObjectId::new([1; 16]),
		1,
		"default",
		"sales",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn named_descriptor(id_byte: u8, table: &str, schema: Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"default",
		table,
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}
