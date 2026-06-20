#![allow(unused_imports)]

use super::support::*;
use super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, rune_frontend::*,
	storage_wendelon_boundary::*,
};

#[tokio::test]
pub(crate) async fn experimental_kernels_are_gated() {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Project {
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

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("default planner must reject experimental kernels");
	assert!(matches!(err, BindingError::ExperimentalKernel { .. }));
}

#[test]
pub(crate) fn lower_query_and_lower_select_have_no_rest_pattern() {
	let source = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	for function in ["fn lower_query", "fn lower_select"] {
		let body = source
			.split(function)
			.nth(1)
			.and_then(|rest| rest.split("\n\tfn ").next())
			.expect(function);
		assert!(
			!body.contains(".."),
			"{function} contains a rest pattern and may silently drop parser fields"
		);
	}
}

#[test]
pub(crate) fn lower_select_does_not_return_input_from_wildcard_projection_loop() {
	let source = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_select = source
		.split("fn lower_select")
		.nth(1)
		.and_then(|rest| rest.split("\n\tfn lower_order_by").next())
		.expect("lower_select body");
	let projection_loop = lower_select
		.split("for item in projection")
		.nth(1)
		.and_then(|rest| rest.split("Ok(LogicalPlan::Project").next())
		.expect("non-aggregate projection loop");

	assert!(lower_select.contains("matches!(projection, [SelectItem::Wildcard(_)])"));
	assert!(
		!projection_loop.contains("return Ok(plan)"),
		"wildcard projection loop still returns the input plan"
	);
	assert!(projection_loop.contains("mixed wildcard projections are not supported"));
}

#[test]
pub(crate) fn adr015_grouped_aggregate_ordering_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("GROUP BY with ORDER BY is not supported"));

	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let recursive_split_index = fragmenter
		.find("if let Some(barrier) = global_sort_barrier(&plan)")
		.expect("ADR 117 recursive sort barrier split");
	let reject_index = fragmenter
		.find("FragmentError::MultipleExchangeBoundaries")
		.expect("multi-exchange reject");
	assert!(
		recursive_split_index < reject_index,
		"global sort barriers must split recursively before the generic multi-exchange reject"
	);

	let recursive_split = adr115_source_block(&fragmenter, "fn fragment_execution_plan");
	assert!(recursive_split.contains("global_sort_barrier(&plan)"));
	assert!(recursive_split.contains("FragmentTerminalOutput::ShuffleProducer"));
	assert!(recursive_split.contains("partition_count: 1"));
	assert!(recursive_split.contains("BoundExprProgram::empty()"));
	assert!(fragmenter.contains("if exchange_count > 1"));
}

#[test]
pub(crate) fn aggregate_kernel_catalog_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("unsupported(\"HAVING is not supported\")"));
	assert!(planner.contains("HAVING aggregate-call must appear in SELECT list"));
	assert!(planner.contains("ambiguous HAVING aggregate-call"));
	assert!(planner.contains("unsupported aggregate function"));

	let lib = std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source");
	assert!(lib.contains("mod aggregate_kernel;"));
	assert!(!lib.contains("pub mod aggregate_kernel"));

	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	assert!(catalog.contains("debug_assert_eq!(spec.kind, kernel);"));
	let stable_kernel_enum = catalog
		.split("pub(crate) enum StableAggregateKernel")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("stable aggregate kernel enum");
	for variant in ["CountAll", "Count", "Sum", "Min", "Max", "Avg"] {
		assert!(
			stable_kernel_enum.contains(variant),
			"stable aggregate kernel missing {variant}"
		);
	}
	assert_eq!(enum_variant_count(stable_kernel_enum), 6);

	let stable_rows = catalog
		.split("STABLE_AGGREGATE_KERNELS")
		.nth(1)
		.and_then(|rest| rest.split("];").next())
		.expect("stable aggregate kernel rows");
	assert_eq!(stable_rows.matches("AggregateKernelSpec {").count(), 6);
	for marker in [
		"kind: StableAggregateKernel::CountAll",
		"kind: StableAggregateKernel::Count",
		"kind: StableAggregateKernel::Sum",
		"kind: StableAggregateKernel::Min",
		"kind: StableAggregateKernel::Max",
		"kind: StableAggregateKernel::Avg",
		"arity: AggregateArity::Zero",
		"arity: AggregateArity::One",
		"input_domain: AggregateInputDomain::Any",
		"input_domain: AggregateInputDomain::SumNumeric",
		"input_domain: AggregateInputDomain::AvgNumeric",
		"input_domain: AggregateInputDomain::OrderedMinMax",
		"output_rule: AggregateOutputRule::Int64",
		"output_rule: AggregateOutputRule::AvgNumeric",
		"output_rule: AggregateOutputRule::SameAsInput",
		"output_rule: AggregateOutputRule::SumNumeric",
		"nullable: false",
		"nullable: true",
		"grouped_supported: true",
	] {
		assert!(stable_rows.contains(marker), "catalog missing {marker}");
	}
	assert_eq!(stable_rows.matches("sql_name: \"COUNT\"").count(), 2);
	for sql_name in ["COUNT", "SUM", "MIN", "MAX", "AVG"] {
		assert!(
			stable_rows.contains(&format!("sql_name: \"{sql_name}\"")),
			"catalog missing SQL name {sql_name}"
		);
	}
	for output_name in ["count", "sum", "min", "max", "avg"] {
		assert!(
			stable_rows.contains(&format!("output_name: \"{output_name}\"")),
			"catalog missing output name {output_name}"
		);
	}
	assert!(!stable_rows.contains("LinearRegress"));
	assert!(!stable_rows.contains("ApproxQuantile"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let filter_disposition = execution_source
		.split("pub enum FilterDisposition")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ExecutionSortKey").next())
		.expect("FilterDisposition enum");
	assert_eq!(enum_variant_count(filter_disposition), 2);
	assert!(filter_disposition.contains("EvaluateInToron"));

	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let global_barrier = adr115_source_block(&fragmenter, "fn global_sort_barrier");
	let base_fragment = adr115_source_block(&fragmenter, "fn fragment_base_plan");
	assert!(global_barrier.contains("UnaryPlanTail::Filter"));
	assert!(base_fragment.contains("terminal_output.wrap_plan"));
	assert!(base_fragment.contains("replace_exchange_with_scan"));

	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let agg_state_enum = exec
		.split("enum AggState")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("AggState enum");
	// ADR 268 added `AvgDecimal`; ADR 284 added Decimal MIN/MAX for strict q02.
	assert_eq!(enum_variant_count(agg_state_enum), 16);
	let before_agg_state = exec.split("enum AggState").next().unwrap_or("");
	let derives_above = before_agg_state.rsplit("#[derive").next().unwrap_or("");
	assert!(derives_above.contains("Copy"));
}

#[test]
pub(crate) fn bind_expr_remains_aggregate_free() {
	let binder = binder_source();
	let bind_expr = brace_blocks_after_marker(&binder, "fn bind_expr_into_program")
		.into_iter()
		.next()
		.expect("bind_expr_into_program body")
		.1;
	assert!(!bind_expr.contains("AggregateCall"));
	assert!(!bind_expr.contains("AggregateFunction"));
}

#[test]
pub(crate) fn adr017_order_by_aggregate_call_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(
		planner.contains(
			"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"
		)
	);
	assert!(planner.contains("ambiguous ORDER BY aggregate-call"));
	assert!(planner.contains("struct LoweredSelect"));
	assert!(planner.contains("aggregate_output_catalog: Option<AggregateOutputCatalog>"));
	assert!(planner.contains("fn rewrite_grouped_aggregate_calls"));
	assert!(planner.contains("struct AggregateOutputCatalog"));
	assert!(!planner.contains("HavingAggregateScope"));
	assert!(!planner.contains("rewrite_having_aggregate_calls"));

	let binder = binder_source();
	let bind_expr = brace_blocks_after_marker(&binder, "fn bind_expr_into_program")
		.into_iter()
		.next()
		.expect("bind_expr_into_program body")
		.1;
	assert!(!bind_expr.contains("AggregateCall"));
	assert!(!bind_expr.contains("AggregateFunction"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let filter_disposition = execution_source
		.split("pub enum FilterDisposition")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ExecutionSortKey").next())
		.expect("FilterDisposition enum");
	assert_eq!(enum_variant_count(filter_disposition), 2);
}

#[test]
pub(crate) fn adr018_scalar_aggregate_having_order_by_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(planner.contains("HAVING requires GROUP BY or an aggregate projection"));
	assert!(!planner.contains("HAVING without GROUP BY is not supported"));
	assert!(planner.contains("HAVING aggregate-call must appear in SELECT list"));
	assert!(
		planner.contains("HAVING does not resolve SELECT aliases; spell the aggregate explicitly")
	);
	assert!(planner.contains("ambiguous HAVING aggregate-call"));
	assert!(planner.contains("ambiguous ORDER BY aggregate-call"));
	assert!(
		planner.contains(
			"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"
		)
	);

	let binder = binder_source();
	let bind_expr = brace_blocks_after_marker(&binder, "fn bind_expr_into_program")
		.into_iter()
		.next()
		.expect("bind_expr_into_program body")
		.1;
	assert!(!bind_expr.contains("AggregateCall"));
	assert!(!bind_expr.contains("AggregateFunction"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let filter_disposition = execution_source
		.split("pub enum FilterDisposition")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ExecutionSortKey").next())
		.expect("FilterDisposition enum");
	assert_eq!(enum_variant_count(filter_disposition), 2);
}

#[test]
pub(crate) fn adr023_post_aggregate_projection_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("GroupedAggregateScope"));
	assert!(planner.contains("struct AggregateOutputCatalog"));
	assert!(!planner.contains("GROUP BY projection must list all keys in order before aggregates"));
	assert!(
		planner.contains("HAVING does not resolve SELECT aliases; spell the aggregate explicitly")
	);
	assert!(
		planner.contains(
			"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"
		)
	);

	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let project_variant = logical_source
		.split("Project {")
		.nth(1)
		.and_then(|rest| rest.split("Sort {").next())
		.expect("LogicalPlan::Project variant");
	assert!(project_variant.contains("output_names: Vec<Option<String>>"));

	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let barrier = adr115_source_block(&fragmenter, "fn global_sort_barrier");
	let barrier_stage = adr115_source_block(&fragmenter, "fn global_sort_barrier_stage");
	assert!(barrier.contains("UnaryPlanTail::Project"));
	assert!(barrier.contains("UnaryPlanTail::Filter"));
	assert!(
		barrier_stage.contains(".rev()"),
		"terminal sort tail must be reapplied in original order"
	);
}

#[test]
pub(crate) fn adr024_scalar_arithmetic_guards_are_narrow() {
	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	for variant in [
		"Cast(Box<Expr>, DataType)",
		"TryCast(Box<Expr>, DataType)",
		"Add(Box<Expr>, Box<Expr>)",
		"Sub(Box<Expr>, Box<Expr>)",
		"Mul(Box<Expr>, Box<Expr>)",
		"Div(Box<Expr>, Box<Expr>)",
	] {
		assert!(logical_source.contains(variant), "missing Expr::{variant}");
	}

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let binary_op = execution_source
		.split("pub enum BoundBinaryOp")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("BoundBinaryOp enum");
	for variant in ["Add", "Sub", "Mul", "Div"] {
		assert!(
			binary_op.contains(variant),
			"missing BoundBinaryOp::{variant}"
		);
	}
	let node = execution_source
		.split("pub enum BoundExprNode")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("BoundExprNode enum");
	assert!(node.contains("Cast {"));
	assert!(node.contains("TryCast {"));
	assert!(node.contains("Binary {"));
	assert!(node.contains("op: BoundBinaryOp"));
	assert!(node.contains("left: BoundExprId"));
	assert!(node.contains("right: BoundExprId"));
	assert!(execution_source.contains("pub fn arithmetic_output_type"));
	assert!(execution_source.contains("(Int32, Int32) => Some(Int32)"));
	assert!(execution_source.contains("(Int32, Int64) | (Int64, Int32) | (Int64, Int64)"));
	assert!(execution_source.contains("(Float32, Float32) => Some(Float32)"));

	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	for operator in [
		"BinaryOperator::Plus => Ok(Expr::Add",
		"BinaryOperator::Minus => Ok(Expr::Sub",
		"BinaryOperator::Multiply => Ok(Expr::Mul",
		"BinaryOperator::Divide => Ok(Expr::Div",
	] {
		assert!(
			planner.contains(operator),
			"missing SQL lowerer for {operator}"
		);
	}
	assert!(!planner.contains("aggregate arguments must be simple column references"));
	assert!(planner.contains("aggregate arguments must not contain aggregate calls"));

	let binder = binder_source();
	assert!(binder.contains("fn bind_arithmetic"));
	assert!(binder.contains("fn is_widening_cast"));
	for conversion in [
		"(DataType::Int32, DataType::Int64)",
		"(DataType::Int32, DataType::Float64)",
		"(DataType::Int64, DataType::Float64)",
		"(DataType::Float32, DataType::Float64)",
	] {
		assert!(binder.contains(conversion), "missing cast {conversion}");
	}

	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read PBC source");
	assert!(pbc.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
	assert!(!pbc.contains("OP_ADD"));
	assert!(!pbc.contains("OP_SUB"));
	assert!(!pbc.contains("OP_MUL"));
	assert!(!pbc.contains("OP_DIV"));

	let pbc_helper = brace_blocks_after_marker(&logical_source, "fn is_pbc_pushdown_eligible")
		.into_iter()
		.next()
		.expect("is_pbc_pushdown_eligible body")
		.1;
	assert!(pbc_helper.contains("Self::TryCast(_, _)"));
	assert!(pbc_helper.contains("false"));
}

#[test]
pub(crate) fn adr025_sql_cast_lowers_via_planner() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
		.into_iter()
		.next()
		.expect("lower_expr body")
		.1;

	assert!(lower_expr.contains("SqlExpr::Cast {"));
	assert!(lower_expr.contains("lower_cast_target(data_type)?"));
	assert!(lower_expr.contains("(CastKind::Cast, LoweredCastTarget::Physical(data_type))"));
	assert!(lower_expr.contains("Ok(Expr::Cast"));
	assert!(lower_expr.contains("(CastKind::Cast, LoweredCastTarget::LogicalName(logical_name))"));
	assert!(lower_expr.contains("Ok(Expr::SqlLogicalCast"));
	assert!(lower_expr.contains("if format.is_some()"));
	assert!(lower_expr.contains("CAST FORMAT clause is not supported"));

	let rewrite_grouped_aggregates =
		brace_blocks_after_marker(&planner, "fn rewrite_grouped_aggregate_calls")
			.into_iter()
			.next()
			.expect("rewrite_grouped_aggregate_calls body")
			.1;
	assert!(rewrite_grouped_aggregates.contains("SqlExpr::Cast {"));
	assert!(rewrite_grouped_aggregates.contains("self.rewrite_grouped_aggregate_calls(expr"));

	for (marker, guard) in [
		(
			"fn reject_having_alias_references(\n\texpr",
			"reject_having_alias_references",
		),
		(
			"fn expr_contains_aggregate_call",
			"expr_contains_aggregate_call",
		),
		(
			"fn expr_contains_nonaggregate_column_reference",
			"expr_contains_nonaggregate_column_reference",
		),
		("fn collect_scope_refs", "collect_scope_refs"),
	] {
		let body = brace_blocks_after_marker(&planner, marker)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{guard} body"))
			.1;
		assert!(
			body.contains("SqlExpr::Cast"),
			"{guard} must recurse into CAST"
		);
	}
}

#[test]
pub(crate) fn adr025_order_by_cast_uses_hidden_sort_projection() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let tail = brace_blocks_after_marker(&planner, "fn apply_order_by_limit_tail")
		.into_iter()
		.next()
		.expect("apply_order_by_limit_tail body")
		.1;
	assert!(tail.contains("sort_needs_hidden_projection(&order_by)"));
	assert!(tail.contains("add_hidden_sort_projection"));
	assert!(tail.contains("final_projection"));

	let hidden_gate = brace_blocks_after_marker(&planner, "fn sort_needs_hidden_projection")
		.into_iter()
		.next()
		.expect("sort_needs_hidden_projection body")
		.1;
	assert!(hidden_gate.contains("!sort_key_is_direct_output_reference(&key.expr)"));

	let validator = brace_blocks_after_marker(&planner, "fn validate_hidden_sort_key_refs")
		.into_iter()
		.next()
		.expect("validate_hidden_sort_key_refs body")
		.1;
	assert!(validator.contains("Expr::SqlLogicalCast { expr: inner, .. }"));
	assert!(validator.contains("| Expr::Cast(inner, _)"));
	assert!(validator.contains("| Expr::TryCast(inner, _)"));
	assert!(validator.contains("| Expr::AtTimeZone(inner, _)"));
	assert!(validator.contains("| Expr::Extract(_, inner)"));
	assert!(validator.contains("| Expr::DateTrunc(_, inner)"));
	assert!(!planner.contains("fn expr_contains_cast_family"));

	let hidden_projection = brace_blocks_after_marker(&planner, "fn add_hidden_sort_projection")
		.into_iter()
		.next()
		.expect("add_hidden_sort_projection body")
		.1;
	assert!(hidden_projection.contains("LogicalPlan::Project"));
	assert!(
		hidden_projection
			.contains("ORDER BY computed aggregate expression requires unique output names")
	);
	assert!(hidden_projection.contains("unique_hidden_sort_name"));

	let visible_names = brace_blocks_after_marker(&planner, "fn visible_output_names")
		.into_iter()
		.next()
		.expect("visible_output_names body")
		.1;
	assert!(visible_names.contains("AggregateOutputCatalog::natural_output_names"));

	let hidden_name = brace_blocks_after_marker(&planner, "fn unique_hidden_sort_name")
		.into_iter()
		.next()
		.expect("unique_hidden_sort_name body")
		.1;
	assert!(hidden_name.contains("__toron_order_key_{idx}"));
}

#[test]
pub(crate) fn adr025_sql_cast_target_type_normalization() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let target_normalizer = brace_blocks_after_marker(&planner, "fn lower_cast_target_type")
		.into_iter()
		.next()
		.expect("lower_cast_target_type body")
		.1;

	for mapping in [
		"SqlDataType::Int32 => Ok(DataType::Int32)",
		"SqlDataType::BigInt(_) | SqlDataType::Int64 => Ok(DataType::Int64)",
		"SqlDataType::Real | SqlDataType::Float4 | SqlDataType::Float32",
		"SqlDataType::Float64 => Ok(DataType::Float64)",
	] {
		assert!(
			target_normalizer.contains(mapping),
			"missing CAST target mapping {mapping}"
		);
	}
	assert!(target_normalizer.contains("SqlDataType::Float(_)"));
	assert!(target_normalizer.contains("unsupported CAST target type: FLOAT"));
	assert!(target_normalizer.contains("unsupported CAST target type: {other}"));
}

#[test]
pub(crate) fn adr025_explicit_cast_policy_is_separate_from_implicit_widening() {
	let binder = binder_source();
	let explicit_cast = brace_blocks_after_marker(&binder, "fn bind_explicit_cast")
		.into_iter()
		.next()
		.expect("bind_explicit_cast body")
		.1;
	let implicit_cast = brace_blocks_after_marker(&binder, "fn cast_if_needed")
		.into_iter()
		.next()
		.expect("cast_if_needed body")
		.1;
	let widening = brace_blocks_after_marker(&binder, "fn is_widening_cast")
		.into_iter()
		.next()
		.expect("is_widening_cast body")
		.1;
	let explicit_policy =
		brace_blocks_after_marker(&binder, "fn is_supported_explicit_numeric_cast")
			.into_iter()
			.next()
			.expect("is_supported_explicit_numeric_cast body")
			.1;

	assert!(explicit_cast.contains("is_supported_explicit_numeric_cast"));
	assert!(explicit_cast.contains("builder.append_cast(inner, target.clone(), target.clone())"));
	assert!(!explicit_cast.contains("source == *target"));
	assert!(!explicit_cast.contains("source == target"));
	assert!(implicit_cast.contains("if source == *target"));
	assert!(implicit_cast.contains("is_widening_cast"));

	for conversion in [
		"(DataType::Int32, DataType::Int64)",
		"(DataType::Int32, DataType::Float64)",
		"(DataType::Int64, DataType::Float64)",
		"(DataType::Float32, DataType::Float64)",
	] {
		assert!(
			widening.contains(conversion),
			"implicit widening missing {conversion}"
		);
	}
	assert!(!widening.contains("(DataType::Int64, DataType::Int32)"));
	assert!(!widening.contains("(DataType::Float64, DataType::Float32)"));

	for conversion in [
		"(DataType::Int32, DataType::Int32)",
		"(DataType::Int32, DataType::Int64)",
		"(DataType::Int32, DataType::Float32)",
		"(DataType::Int32, DataType::Float64)",
		"(DataType::Int64, DataType::Int32)",
		"(DataType::Int64, DataType::Int64)",
		"(DataType::Int64, DataType::Float32)",
		"(DataType::Int64, DataType::Float64)",
		"(DataType::Float32, DataType::Int32)",
		"(DataType::Float32, DataType::Int64)",
		"(DataType::Float32, DataType::Float32)",
		"(DataType::Float32, DataType::Float64)",
		"(DataType::Float64, DataType::Int32)",
		"(DataType::Float64, DataType::Int64)",
		"(DataType::Float64, DataType::Float32)",
		"(DataType::Float64, DataType::Float64)",
	] {
		assert!(
			explicit_policy.contains(conversion),
			"explicit CAST policy missing {conversion}"
		);
	}
}

#[test]
pub(crate) fn adr025_cast_supports_numeric_primitive_lattice() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let evaluate_cast = brace_blocks_after_marker(&exec, "fn evaluate_cast")
		.into_iter()
		.next()
		.expect("evaluate_cast body")
		.1;

	assert!(evaluate_cast.contains("if array.data_type() == target"));
	for arm in [
		"(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array)",
		"(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array)",
		"(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array)",
		"(DataType::Int64, DataType::Int32) => cast_i64_to_i32(array)",
		"(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array)",
		"(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array)",
		"(DataType::Float32, DataType::Int32) => cast_f32_to_i32(array)",
		"(DataType::Float32, DataType::Int64) => cast_f32_to_i64(array)",
		"(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array)",
		"(DataType::Float64, DataType::Int32) => cast_f64_to_i32(array)",
		"(DataType::Float64, DataType::Int64) => cast_f64_to_i64(array)",
		"(DataType::Float64, DataType::Float32) => cast_f64_to_f32(array)",
	] {
		assert!(
			evaluate_cast.contains(arm),
			"missing evaluate_cast arm {arm}"
		);
	}

	let int64_to_float32 = brace_blocks_after_marker(&exec, "fn cast_i64_to_f32")
		.into_iter()
		.next()
		.expect("cast_i64_to_f32 body")
		.1;
	assert!(int64_to_float32.contains("v as f32"));
}

#[test]
pub(crate) fn adr025_cast_failure_returns_kernel_error_with_prefix() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let cast_failed = brace_blocks_after_marker(&exec, "fn cast_failed")
		.into_iter()
		.next()
		.expect("cast_failed body")
		.1;

	assert!(cast_failed.contains("ExecutionError::Kernel"));
	assert!(cast_failed.contains("cast failed: {source:?} to {target:?}: {cause}"));
}

#[test]
pub(crate) fn adr026_cast_syntax_doublecolon_and_format_remain_rejected() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
		.into_iter()
		.next()
		.expect("lower_expr body")
		.1;

	for rejected in [
		"CastKind::DoubleColon",
		"unsupported cast syntax: ::; only CAST(... AS ...) is supported",
		"CAST FORMAT clause is not supported",
	] {
		assert!(
			lower_expr.contains(rejected),
			"missing syntax guard {rejected}"
		);
	}
	assert!(lower_expr.contains("CastKind::TryCast | CastKind::SafeCast"));
	assert!(lower_expr.contains("Ok(Expr::TryCast"));
	assert!(!lower_expr.contains("unsupported cast syntax: TRY_CAST"));
	assert!(!lower_expr.contains("unsupported cast syntax: SAFE_CAST"));
}

#[test]
pub(crate) fn adr025_float_to_int_uses_exclusive_upper_bound() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("const F64_I32_LOWER: f64 = i32::MIN as f64"));
	assert!(exec.contains("const F64_I32_UPPER: f64 = 2147483648.0"));
	assert!(exec.contains("const F64_I64_LOWER: f64 = i64::MIN as f64"));
	assert!(exec.contains("const F64_I64_UPPER: f64 = 9223372036854775808.0"));

	for (marker, bound, range_bound) in [
		(
			"fn cast_f32_to_i32",
			"trunc < F64_I32_UPPER",
			"(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc)",
		),
		(
			"fn cast_f32_to_i64",
			"trunc < F64_I64_UPPER",
			"(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc)",
		),
		(
			"fn cast_f64_to_i32",
			"trunc < F64_I32_UPPER",
			"(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc)",
		),
		(
			"fn cast_f64_to_i64",
			"trunc < F64_I64_UPPER",
			"(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc)",
		),
	] {
		let body = brace_blocks_after_marker(&exec, marker)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{marker} body"))
			.1;
		assert!(
			body.contains(bound) || body.contains(range_bound),
			"{marker} missing exclusive upper bound"
		);
		assert!(
			!body.contains("trunc <= F64_"),
			"{marker} must not use inclusive upper bound"
		);
		assert!(body.contains("NaN has no integer image"));
		assert!(body.contains("Inf has no integer image"));
	}

	assert!(
		brace_blocks_after_marker(&exec, "fn cast_f32_to_i32")
			.into_iter()
			.next()
			.expect("cast_f32_to_i32 body")
			.1
			.contains("f64::from(value).trunc()")
	);
	assert!(
		brace_blocks_after_marker(&exec, "fn cast_f32_to_i64")
			.into_iter()
			.next()
			.expect("cast_f32_to_i64 body")
			.1
			.contains("f64::from(value).trunc()")
	);
}

#[test]
pub(crate) fn adr026_try_cast_ir_surface_is_narrow() {
	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	assert!(logical_source.contains("TryCast(Box<Expr>, DataType)"));
	let logical_pbc = brace_blocks_after_marker(&logical_source, "fn is_pbc_pushdown_eligible")
		.into_iter()
		.next()
		.expect("logical pbc eligibility body")
		.1;
	assert!(logical_pbc.contains("Self::TryCast(_, _)"));
	assert!(logical_pbc.contains("false"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	assert!(execution_source.contains("TryCast {"));
	assert!(execution_source.contains("target: DataType"));
	let nullable = brace_blocks_after_marker(&execution_source, "fn nullable")
		.into_iter()
		.next()
		.or_else(|| {
			brace_blocks_after_marker(&execution_source, "fn node_nullable")
				.into_iter()
				.next()
		})
		.expect("nullable body")
		.1;
	assert!(nullable.contains("BoundExprNode::TryCast { .. } => Some(true)"));
}

#[test]
pub(crate) fn adr026_try_cast_lowers_via_planner() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
		.into_iter()
		.next()
		.expect("lower_expr body")
		.1;
	assert!(lower_expr.contains("CastKind::TryCast | CastKind::SafeCast"));
	assert!(lower_expr.contains("Ok(Expr::TryCast"));
	assert!(lower_expr.contains("lower_cast_target(data_type)?"));
	assert!(lower_expr.contains("LoweredCastTarget::LogicalName(logical_name)"));
	assert!(lower_expr.contains("CastKind::DoubleColon"));
	assert!(lower_expr.contains("CAST FORMAT clause is not supported"));
}

#[test]
pub(crate) fn adr026_try_cast_policy_is_separate() {
	let binder = binder_source();
	let bind_try_cast = brace_blocks_after_marker(&binder, "fn bind_try_cast")
		.into_iter()
		.next()
		.expect("bind_try_cast body")
		.1;
	assert!(bind_try_cast.contains("is_supported_explicit_numeric_cast"));
	assert!(bind_try_cast.contains("unsupported TRY_CAST/SAFE_CAST"));
	assert!(
		bind_try_cast.contains("builder.append_try_cast(inner, target.clone(), target.clone())")
	);
	assert!(
		!bind_try_cast.contains("if source == *target"),
		"bind_try_cast must not elide identity TryCast"
	);
	assert!(
		!bind_try_cast.contains("if source == target"),
		"bind_try_cast must not elide identity TryCast"
	);
	assert!(binder.contains("BoundExprNode::TryCast { input, .. }"));
	assert!(binder.contains("BoundExprNode::TryCast { input, target }"));
}

#[test]
pub(crate) fn adr026_try_cast_supports_numeric_primitive_lattice() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let evaluate_try_cast = brace_blocks_after_marker(&exec, "fn evaluate_try_cast")
		.into_iter()
		.next()
		.expect("evaluate_try_cast body")
		.1;
	assert!(evaluate_try_cast.contains("if array.data_type() == target"));
	assert!(!evaluate_try_cast.contains("evaluate_cast("));

	for arm in [
		"(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array)",
		"(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array)",
		"(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array)",
		"(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array)",
		"(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array)",
		"(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array)",
		"(DataType::Int64, DataType::Int32) => try_cast_i64_to_i32(array)",
		"(DataType::Float64, DataType::Float32) => try_cast_f64_to_f32(array)",
		"(DataType::Float32, DataType::Int32) => try_cast_f32_to_i32(array)",
		"(DataType::Float32, DataType::Int64) => try_cast_f32_to_i64(array)",
		"(DataType::Float64, DataType::Int32) => try_cast_f64_to_i32(array)",
		"(DataType::Float64, DataType::Int64) => try_cast_f64_to_i64(array)",
	] {
		assert!(
			evaluate_try_cast.contains(arm),
			"missing evaluate_try_cast arm {arm}"
		);
	}
}

#[test]
pub(crate) fn adr026_try_cast_nulls_only_data_domain_failures() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(!exec.contains("catch_unwind"));
	for kernel in [
		"fn try_cast_i64_to_i32",
		"fn try_cast_f64_to_f32",
		"fn try_cast_f32_to_i32",
		"fn try_cast_f32_to_i64",
		"fn try_cast_f64_to_i32",
		"fn try_cast_f64_to_i64",
	] {
		let body = brace_blocks_after_marker(&exec, kernel)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{kernel} body"))
			.1;
		assert!(
			!body.contains("cast_failed("),
			"{kernel} must not emit cast_failed errors"
		);
		assert!(
			!body.contains("ExecutionError::Kernel"),
			"{kernel} must not construct Kernel errors directly"
		);
		assert!(body.contains("return None"), "{kernel} must emit None");
	}

	for (kernel, bound, range_bound) in [
		(
			"fn try_cast_f32_to_i32",
			"trunc < F64_I32_UPPER",
			"(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc)",
		),
		(
			"fn try_cast_f32_to_i64",
			"trunc < F64_I64_UPPER",
			"(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc)",
		),
		(
			"fn try_cast_f64_to_i32",
			"trunc < F64_I32_UPPER",
			"(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc)",
		),
		(
			"fn try_cast_f64_to_i64",
			"trunc < F64_I64_UPPER",
			"(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc)",
		),
	] {
		let body = brace_blocks_after_marker(&exec, kernel)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{kernel} body"))
			.1;
		assert!(
			body.contains(bound) || body.contains(range_bound),
			"{kernel} missing bound {bound}"
		);
	}
	assert!(
		brace_blocks_after_marker(&exec, "fn try_cast_f64_to_f32")
			.into_iter()
			.next()
			.expect("try_cast_f64_to_f32 body")
			.1
			.contains("value.is_finite() && value.abs() > f32::MAX as f64")
	);
}

#[test]
pub(crate) fn adr026_try_cast_pbc_declines() {
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read PBC source");
	assert!(pbc.contains("BoundExprNode::TryCast { .. }"));
	assert!(pbc.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
	assert!(!pbc.contains("OP_CAST"));
	assert!(!pbc.contains("OP_TRY_CAST"));
}

#[test]
pub(crate) fn adr026_try_cast_order_by_uses_hidden_sort_projection() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("fn expr_contains_cast_family"));

	let sort_gate = brace_blocks_after_marker(&planner, "fn sort_needs_hidden_projection")
		.into_iter()
		.next()
		.expect("sort_needs_hidden_projection body")
		.1;
	assert!(sort_gate.contains("!sort_key_is_direct_output_reference(&key.expr)"));

	let validator = brace_blocks_after_marker(&planner, "fn validate_hidden_sort_key_refs")
		.into_iter()
		.next()
		.expect("validate_hidden_sort_key_refs body")
		.1;
	assert!(validator.contains("Expr::SqlLogicalCast { expr: inner, .. }"));
	assert!(validator.contains("| Expr::Cast(inner, _)"));
	assert!(validator.contains("| Expr::TryCast(inner, _)"));
}

#[test]
pub(crate) fn adr027_hidden_sort_projection_gate_is_generalized() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	let gate = brace_blocks_after_marker(&planner, "fn sort_needs_hidden_projection")
		.into_iter()
		.next()
		.expect("sort_needs_hidden_projection body")
		.1;
	assert!(gate.contains("!sort_key_is_direct_output_reference(&key.expr)"));
	assert!(!gate.contains("expr_contains_cast_family"));
	assert!(!planner.contains("fn expr_contains_cast_family"));
}

#[test]
pub(crate) fn adr027_hidden_sort_key_refs_are_visible_outputs() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	let validator = brace_blocks_after_marker(&planner, "fn validate_hidden_sort_key_refs")
		.into_iter()
		.next()
		.expect("validate_hidden_sort_key_refs body")
		.1;

	assert!(validator.contains("visible_names.contains(&column.name.0)"));
	assert!(
		validator.contains(
			"ORDER BY computed aggregate expression must reference visible output columns"
		)
	);
	assert!(validator.contains("Expr::Literal(_) | Expr::UntypedNull => Ok(())"));
	assert!(validator.contains("Expr::SqlLogicalCast { expr: inner, .. }"));
	assert!(validator.contains("| Expr::Cast(inner, _)"));
	assert!(validator.contains("| Expr::TryCast(inner, _)"));
	for op in ["Add", "Sub", "Mul", "Div"] {
		assert!(validator.contains(&format!("Expr::{op}(left, right)")));
	}
	for op in ["Eq", "Neq", "Lt", "Lte", "Gt", "Gte", "And", "Or"] {
		assert!(validator.contains(&format!("Expr::{op}(left, right)")));
	}
	for op in ["Not", "IsNull", "IsNotNull"] {
		assert!(validator.contains(&format!("Expr::{op}(inner)")));
	}
	assert!(validator.contains("Expr::TimeBucket(_, inner)"));
	assert!(validator.contains("| Expr::AtTimeZone(inner, _)"));
	assert!(validator.contains("| Expr::Extract(_, inner)"));
	assert!(validator.contains("| Expr::DateTrunc(_, inner)"));
}

#[test]
pub(crate) fn adr027_hidden_sort_projection_contract_is_pinned() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(planner.contains("fn visible_output_names"));
	assert!(planner.contains("fn has_duplicate_names"));
	assert!(planner.contains("fn unique_hidden_sort_name"));

	let mechanism = brace_blocks_after_marker(&planner, "fn add_hidden_sort_projection")
		.into_iter()
		.next()
		.expect("add_hidden_sort_projection body")
		.1;
	assert!(mechanism.contains("validate_hidden_sort_key_refs"));
	assert!(planner.contains("__toron_order_key_"));
	assert!(
		planner.contains("ORDER BY computed aggregate expression requires unique output names")
	);
	assert!(mechanism.contains("FinalProjection"));
}

#[test]
pub(crate) fn adr028_aggregate_arguments_accept_computed_expressions() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(!planner.contains("aggregate arguments must be simple column references"));
	assert!(planner.contains("aggregate arguments must not contain aggregate calls"));
	let validator = brace_blocks_after_marker(&planner, "fn validate_aggregate_argument")
		.into_iter()
		.next()
		.expect("validate aggregate argument")
		.1;
	assert!(validator.contains("reject_nested_aggregate"));
}

#[test]
pub(crate) fn adr028_aggregate_signature_uses_planner_private_visitors() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let signature = brace_blocks_after_marker(&planner, "struct GroupedAggregateSignature")
		.into_iter()
		.next()
		.expect("GroupedAggregateSignature body")
		.1;
	assert!(signature.contains("kernel: aggregate_kernel::StableAggregateKernel"));
	assert!(signature.contains("args: Vec<Expr>"));
	assert!(planner.contains("impl PartialEq for GroupedAggregateSignature"));
	assert!(planner.contains("impl Hash for GroupedAggregateSignature"));
	let hash_impl = brace_blocks_after_marker(&planner, "impl Hash for GroupedAggregateSignature")
		.into_iter()
		.next()
		.expect("GroupedAggregateSignature Hash impl")
		.1;
	assert!(hash_impl.contains("self.args.len().hash(state)"));
	for helper in [
		"fn hash_expr_structural",
		"fn expr_structural_eq",
		"fn hash_scalar_structural",
		"fn scalar_structural_eq",
		"fn hash_data_type_structural",
		"fn data_type_structural_eq",
	] {
		assert!(planner.contains(helper), "missing {helper}");
	}
	for forbidden in ["AggregateArgKey", "ScalarKey", "DataTypeKey", "DurationKey"] {
		assert!(!planner.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
pub(crate) fn adr028_aggregate_signature_visitors_are_exhaustive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let hash_body = brace_blocks_after_marker(&planner, "fn hash_expr_structural")
		.into_iter()
		.next()
		.expect("hash expr structural")
		.1;
	let eq_body = brace_blocks_after_marker(&planner, "fn expr_structural_eq")
		.into_iter()
		.next()
		.expect("expr structural eq")
		.1;
	for variant in [
		"Expr::Column",
		"Expr::Literal",
		"Expr::Cast",
		"Expr::TryCast",
		"Expr::Add",
		"Expr::Sub",
		"Expr::Mul",
		"Expr::Div",
		"Expr::Eq",
		"Expr::Neq",
		"Expr::Lt",
		"Expr::Lte",
		"Expr::Gt",
		"Expr::Gte",
		"Expr::And",
		"Expr::Or",
		"Expr::Not",
		"Expr::IsNull",
		"Expr::IsNotNull",
		"Expr::TimeBucket",
		"Expr::AtTimeZone",
		"Expr::Extract",
		"Expr::DateTrunc",
	] {
		assert!(
			hash_body.contains(variant),
			"hash visitor missing {variant}"
		);
		assert!(eq_body.contains(variant), "eq visitor missing {variant}");
	}
}

#[test]
pub(crate) fn adr028_aggregate_signature_visitors_preserve_float_bits() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let hash_body = brace_blocks_after_marker(&planner, "fn hash_scalar_structural")
		.into_iter()
		.next()
		.expect("hash scalar structural")
		.1;
	let eq_body = brace_blocks_after_marker(&planner, "fn scalar_structural_eq")
		.into_iter()
		.next()
		.expect("scalar structural eq")
		.1;
	assert!(hash_body.contains("f32::to_bits"));
	assert!(hash_body.contains("f64::to_bits"));
	assert!(eq_body.contains("f32::to_bits"));
	assert!(eq_body.contains("f64::to_bits"));
	for variant in [
		"ScalarValue::Boolean",
		"ScalarValue::Int32",
		"ScalarValue::Int64",
		"ScalarValue::Float32",
		"ScalarValue::Float64",
		"ScalarValue::Utf8",
	] {
		assert!(hash_body.contains(variant), "hash scalar missing {variant}");
		assert!(eq_body.contains(variant), "eq scalar missing {variant}");
	}
}

#[test]
pub(crate) fn adr028_data_type_structural_eq_is_reflexive() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let eq_body = brace_blocks_after_marker(&planner, "fn data_type_structural_eq")
		.into_iter()
		.next()
		.expect("data type structural eq")
		.1;
	assert!(eq_body.contains("left == right"));
	assert!(!eq_body.contains("matches!"));
	let hash_body = brace_blocks_after_marker(&planner, "fn hash_data_type_structural")
		.into_iter()
		.next()
		.expect("hash data type structural")
		.1;
	for data_type in [
		"DataType::Int32",
		"DataType::Int64",
		"DataType::Float32",
		"DataType::Float64",
	] {
		assert!(
			hash_body.contains(data_type),
			"hash data type missing {data_type}"
		);
	}
	assert!(hash_body.contains("format!"));
}

#[test]
pub(crate) fn adr028_aggregate_signature_count_all_is_zero_arg() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let signature = brace_blocks_after_marker(&planner, "fn grouped_aggregate_signature")
		.into_iter()
		.next()
		.expect("grouped aggregate signature")
		.1;
	assert!(signature.contains("aggregate_kernel::AggregateArity::Zero"));
	assert!(signature.contains("args: Vec::new()"));
	let lower_count = brace_blocks_after_marker(&planner, "fn lower_count_aggregate")
		.into_iter()
		.next()
		.expect("lower count aggregate")
		.1;
	assert!(lower_count.contains("Expr::column(\"count\")"));
}

#[test]
pub(crate) fn adr028_nested_aggregate_rejects_at_sql_walk() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let walker = brace_blocks_after_marker(&planner, "fn reject_nested_aggregate")
		.into_iter()
		.next()
		.expect("reject nested aggregate")
		.1;
	for arm in [
		"SqlExpr::Function",
		"SqlExpr::BinaryOp",
		"SqlExpr::UnaryOp",
		"SqlExpr::Cast",
		"SqlExpr::IsNull",
		"SqlExpr::IsNotNull",
		"SqlExpr::Extract",
		"SqlExpr::AtTimeZone",
		"SqlExpr::Nested",
	] {
		assert!(walker.contains(arm), "walker missing {arm}");
	}
	assert!(walker.contains("aggregate_function_name(&function.name).is_some()"));
	assert!(planner.contains("aggregate arguments must not contain aggregate calls"));
}

#[test]
pub(crate) fn adr028_experimental_kernels_excluded_from_grouped_signatures() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	let mapping = brace_blocks_after_marker(&catalog, "fn stable_kernel_for_function")
		.into_iter()
		.next()
		.expect("stable kernel for function")
		.1;
	assert!(mapping.contains(
		"AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => None"
	));
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let signature = brace_blocks_after_marker(&planner, "fn grouped_aggregate_signature")
		.into_iter()
		.next()
		.expect("grouped aggregate signature")
		.1;
	assert!(signature.contains("aggregate_kernel::stable_kernel_for_function(function)?"));
}

#[test]
pub(crate) fn adr028_aggregate_output_names_stay_kernel_names() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let output_names = brace_blocks_after_marker(&planner, "fn aggregate_output_name")
		.into_iter()
		.next()
		.expect("aggregate output name")
		.1;
	assert!(!output_names.contains("format!"));
	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	for name in [
		"output_name: \"sum\"",
		"output_name: \"count\"",
		"output_name: \"min\"",
		"output_name: \"max\"",
		"output_name: \"avg\"",
	] {
		assert!(catalog.contains(name), "missing output name {name}");
	}
}

#[test]
pub(crate) fn adr028_binder_executor_aggregate_args_preserved() {
	let binder = binder_source();
	let stable_bind = brace_blocks_after_marker(&binder, "fn bind_stable_aggregate_call")
		.into_iter()
		.next()
		.expect("bind stable aggregate call")
		.1;
	assert!(stable_bind.contains("bind_expr_program(args"));
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("self.evaluate_expr(&args[0]"));
	assert!(exec.contains("self.evaluate_bound_single_root(&call.args"));
}

#[test]
pub(crate) fn adr028_public_ir_remains_non_eq_non_hash() {
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source");
	for type_name in ["AggregateFunction", "Expr", "ScalarValue"] {
		let before_type = ir
			.split(&format!("pub enum {type_name}"))
			.next()
			.expect("type marker");
		let derives = before_type.rsplit("#[derive").next().unwrap_or("");
		let derive_items = derives
			.split(')')
			.next()
			.and_then(|line| line.split('(').nth(1))
			.unwrap_or("");
		let has_forbidden_derive = derive_items
			.split(',')
			.map(str::trim)
			.any(|item| item == "Eq" || item == "Hash");
		assert!(
			!has_forbidden_derive,
			"ADR 028 D2: {type_name} must not derive Eq or Hash"
		);
		assert!(
			!ir.contains(&format!("impl Hash for {type_name}")),
			"ADR 028 D2: {type_name} must not implement Hash"
		);
		assert!(
			!ir.contains(&format!("impl Eq for {type_name}")),
			"ADR 028 D2: {type_name} must not implement Eq"
		);
	}
}

#[test]
pub(crate) fn adr019_min_max_aggregate_kernels_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(planner.contains("\"MIN\" => self.lower_min_aggregate"));
	assert!(planner.contains("\"MAX\" => self.lower_max_aggregate"));
	assert!(planner.contains("MIN requires one argument"));
	assert!(planner.contains("MAX requires one argument"));
	assert!(planner.contains("unsupported aggregate function"));

	let binder = binder_source();
	assert!(binder.contains("expects Int32, Int64, Float32, or Float64 input"));

	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	assert!(catalog.contains("kind: StableAggregateKernel::Min"));
	assert!(catalog.contains("kind: StableAggregateKernel::Max"));
	assert!(catalog.contains("input_domain: AggregateInputDomain::OrderedMinMax"));
	assert!(catalog.contains("output_rule: AggregateOutputRule::SameAsInput"));

	let aggregate_function_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source");
	let aggregate_function = aggregate_function_source
		.split("pub enum AggregateFunction")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("AggregateFunction enum");
	assert!(aggregate_function.contains("Min,"));
	assert!(aggregate_function.contains("Max,"));

	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("AggState::MinInt32"));
	assert!(exec.contains("AggState::MinInt64"));
	assert!(exec.contains("AggState::MinFloat32"));
	assert!(exec.contains("AggState::MinFloat64"));
	assert!(exec.contains("AggState::MinDecimal128"));
	assert!(exec.contains("AggState::MaxInt32"));
	assert!(exec.contains("AggState::MaxInt64"));
	assert!(exec.contains("AggState::MaxFloat32"));
	assert!(exec.contains("AggState::MaxFloat64"));
	assert!(exec.contains("AggState::MaxDecimal128"));
	let before_agg_state = exec.split("enum AggState").next().unwrap_or("");
	let derives_above = before_agg_state.rsplit("#[derive").next().unwrap_or("");
	assert!(derives_above.contains("Copy"));

	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	assert!(!storage.contains("AggregateExpression"));
	assert!(!storage.contains("AggregateFunctionTag"));
}

#[test]
pub(crate) fn adr020_avg_aggregate_kernel_guards_are_narrow() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	assert!(planner.contains("\"AVG\" => self.lower_avg_aggregate"));
	assert!(planner.contains("AVG requires one argument"));
	assert!(planner.contains("unsupported aggregate function"));

	let binder = binder_source();
	assert!(binder.contains("AVG expects numeric input"));

	let catalog = std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate catalog source");
	assert!(catalog.contains("kind: StableAggregateKernel::Avg"));
	assert!(catalog.contains("kind: StableAggregateKernel::Avg"));
	assert!(catalog.contains("output_name: \"avg\""));
	assert!(catalog.contains("input_domain: AggregateInputDomain::AvgNumeric"));
	assert!(catalog.contains("output_rule: AggregateOutputRule::AvgNumeric"));

	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	assert!(exec.contains("AggState::AvgInt"));
	assert!(exec.contains("AggState::AvgFloat"));
	assert!(exec.contains("fn avg_state_for_data_type"));
	assert!(exec.contains("fn add_avg_value"));
	assert!(exec.contains("fn avg_finalize"));
	let before_agg_state = exec.split("enum AggState").next().unwrap_or("");
	let derives_above = before_agg_state.rsplit("#[derive").next().unwrap_or("");
	assert!(derives_above.contains("Copy"));

	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	assert!(!storage.contains("AggregateExpression"));
	assert!(!storage.contains("AggregateFunctionTag"));
}

#[test]
pub(crate) fn adr070_update_logical_predicate_is_optional() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let update = brace_blocks_after_marker(&logical, "Update {")
		.into_iter()
		.next()
		.expect("logical update")
		.1;

	assert!(update.contains("predicate: Option<Expr>"));
	assert!(!update.contains("predicate: Expr,"));
}

#[test]
pub(crate) fn adr070_planner_accepts_update_without_where() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("UPDATE people SET note = 'new'")
		.expect("no-WHERE UPDATE should plan");

	let LogicalPlan::Update { predicate, .. } = plan else {
		panic!("expected update plan");
	};
	assert!(predicate.is_none());
}

#[test]
pub(crate) fn adr070_planner_lowers_missing_update_selection_to_none() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_update = brace_blocks_after_marker(&planner, "fn lower_update(&self")
		.into_iter()
		.next()
		.expect("lower_update")
		.1;

	assert!(lower_update.contains("let predicate = selection"));
	assert!(lower_update.contains(".map(|expr| self.lower_expr(expr))"));
	assert!(lower_update.contains(".transpose()?"));
	assert!(!lower_update.contains("UPDATE requires a WHERE clause"));
}

#[test]
pub(crate) fn adr070_update_without_where_is_not_lowered_as_true_literal() {
	let binder = binder_source();
	let all_rows = brace_blocks_after_marker(&binder, "fn bind_update_source_for_all_rows")
		.into_iter()
		.next()
		.expect("bind_update_source_for_all_rows")
		.1;

	for forbidden in [
		"Expr::Literal",
		"ScalarValue::Boolean",
		"compile_pbc",
		"attach_pbc_program",
		"ExecutionPlan::Filter",
		"FilterDisposition",
	] {
		assert!(
			!all_rows.contains(forbidden),
			"no-WHERE UPDATE helper gained {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr070_update_without_where_builds_project_over_scan_without_filter() {
	let binder = binder_source();
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let all_rows = brace_blocks_after_marker(&binder, "fn bind_update_source_for_all_rows")
		.into_iter()
		.next()
		.expect("bind_update_source_for_all_rows")
		.1;
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(bind_update.contains("bind_update_source_for_all_rows(scan)"));
	assert!(bind_update.contains("ExecutionPlan::Project"));
	assert!(bind_update.contains("input: Box::new(project_input)"));
	assert!(!all_rows.contains("ExecutionPlan::Filter"));
	assert!(update_test.contains("binder_update_without_where_uses_full_scan_profile_no_filter"));
	assert!(update_test.contains("update_all_rows_source_scan"));
}

#[test]
pub(crate) fn adr070_update_without_where_scan_uses_no_append_identity() {
	let binder = binder_source();
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let scan_plan = brace_blocks_after_marker(&binder, "fn scan_plan")
		.into_iter()
		.next()
		.expect("scan_plan")
		.1;

	assert!(bind_update.contains("scan_plan(binding.clone(), None)"));
	assert!(scan_plan.contains("RowIdentityProjection::None"));
	assert!(!bind_update.contains("RowIdentityProjection::Append"));
}

#[test]
pub(crate) fn adr070_update_without_where_profile_hint_is_scan() {
	let binder = binder_source();
	let all_rows = brace_blocks_after_marker(&binder, "fn bind_update_source_for_all_rows")
		.into_iter()
		.next()
		.expect("bind_update_source_for_all_rows")
		.1;
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");

	assert!(all_rows.contains("QueryProfileHint::Scan"));
	assert!(!ir.contains("FullTable"));
}

#[test]
pub(crate) fn adr070_update_where_pk_equality_point_read_profile_still_works() {
	let binder = binder_source();
	let predicate_helper =
		brace_blocks_after_marker(&binder, "fn bind_update_source_for_predicate")
			.into_iter()
			.next()
			.expect("bind_update_source_for_predicate")
			.1;
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(predicate_helper.contains("plan_filter_with_predicate_pushdown("));
	assert!(predicate_helper.contains("scan,"));
	assert!(predicate_helper.contains("bound_predicate,"));
	assert!(predicate_helper.contains("&logical_types"));
	assert!(binder.contains("scan_profile_hint(&pushed_predicate, input)"));
	assert!(binder.contains("apply_scan_profile_hint(&mut input, Some(profile_hint))"));
	assert!(update_test.contains("binder_update_pk_equality_uses_point_read_profile"));
}

#[test]
pub(crate) fn adr070_update_without_where_still_requires_pk_table() {
	let binder = binder_source();
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(bind_update.contains("TableRowIdentity::PrimaryKey"));
	assert!(bind_update.contains("primary_key_columns.contains(primary_key_ordinal)"));
	assert!(bind_update.contains("UPDATE requires a PRIMARY KEY table"));
	assert!(update_test.contains("binder_rejects_update_without_where_on_append_only_table"));
}

#[test]
pub(crate) fn adr070_update_without_where_preserves_pk_immutability() {
	let binder = binder_source();
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(bind_update.contains("UPDATE cannot modify the PRIMARY KEY column"));
	assert!(update_test.contains("binder_rejects_update_without_where_primary_key_assignment"));
}

#[test]
pub(crate) fn adr070_update_returning_composes_with_no_where() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(
		coordinator.contains("write::materialize_update_select_batch(schema, &source_batches)")
	);
	assert!(coordinator.contains("project_returning_batches(returning.as_ref()"));
	assert!(coordinator.contains("verify_write_returning_receipt(\"UPDATE\""));
	assert!(update_test.contains("planner_accepts_update_without_where_returning"));
	assert!(update_test.contains("update_all_rows_returning_emits_post_update_rows"));
}

#[test]
pub(crate) fn adr070_update_remains_coordinator_owned_source_bearing_write() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update = brace_blocks_after_marker(&execution, "Update {")
		.into_iter()
		.next()
		.expect("execution update")
		.1;
	let update_start = coordinator
		.find("ExecutionPlan::Update {\n\t\t\tlocal")
		.expect("coordinator update arm start");
	let update_end = update_start
		+ coordinator[update_start..]
			.find("ExecutionPlan::CreateTableAsSelect")
			.expect("coordinator update arm end");
	let update_arm = &coordinator[update_start..update_end];

	assert!(update.contains("source: Box<ExecutionPlan>"));
	assert!(update_arm.contains("source_executor.execute_read_plan(source).await?"));
	assert!(update_arm.contains("write(WriteRequest"));
	assert!(!update_arm.contains("delete_rows"));
	assert!(!update_arm.contains("StorageDeleteRows"));
}

#[test]
pub(crate) fn adr070_update_remains_non_fragmentable_and_worker_rejected() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(fragmenter.contains("UpdateNotFragmentable"));
	assert!(fragmenter.contains("matches!(plan, ExecutionPlan::Update { .. })"));
	assert!(server.contains("ExecutionPlan::Update { .. } => \"update\""));
	assert!(exec.contains("UPDATE execution is coordinator-only"));
	assert!(exec.contains("unbound UPDATE execution"));
	assert!(update_test.contains("fragmenter_rejects_direct_update_without_where_fragmentation"));
}

#[test]
pub(crate) fn adr070_delete_no_where_semantics_unchanged() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_delete = brace_blocks_after_marker(&planner, "fn lower_delete")
		.into_iter()
		.next()
		.expect("lower_delete")
		.1;
	let delete_test = std::fs::read_to_string(manifest_dir().join("tests/delete_test.rs"))
		.expect("read delete tests");

	assert!(lower_delete.contains("let predicate = selection"));
	assert!(lower_delete.contains(".map(|expr| self.lower_expr(expr))"));
	assert!(lower_delete.contains(".transpose()?"));
	assert!(!lower_delete.contains("DELETE requires a WHERE clause"));
	assert!(delete_test.contains("planner_lowers_delete_without_where"));
}

#[test]
pub(crate) fn adr070_no_transaction_lock_cas_retry_or_safety_knob() {
	let binder = binder_source();
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;
	let update_start = coordinator
		.find("ExecutionPlan::Update {\n\t\t\tlocal")
		.expect("coordinator update arm start");
	let update_end = update_start
		+ coordinator[update_start..]
			.find("ExecutionPlan::CreateTableAsSelect")
			.expect("coordinator update arm end");
	let update_arm = &coordinator[update_start..update_end];

	for forbidden in [
		"CompareSwap",
		"compare_swap",
		"lost_update",
		"transaction",
		"rollback",
		"StorageDeleteRows",
		"delete_rows",
	] {
		assert!(
			!bind_update.contains(forbidden),
			"bind UPDATE gained {forbidden}"
		);
		assert!(
			!update_arm.contains(forbidden),
			"coordinator UPDATE gained {forbidden}"
		);
	}
	for forbidden in [
		"update_without_where",
		"full_table_update",
		"FullTable",
		"confirm_update",
	] {
		assert!(
			!config.contains(forbidden),
			"config gained no-WHERE UPDATE knob {forbidden}"
		);
		assert!(
			!budget.contains(forbidden),
			"budget gained no-WHERE UPDATE knob {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr070_no_wendelon_wire_storage_or_descriptor_change() {
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"LogicalPlan::Update",
		"ExecutionPlan::Update",
		"Statement::Update",
		"UpdateNotFragmentable",
		"StorageUpdate",
		"TableUpdateRequest",
		"TableUpdateResponse",
	] {
		assert!(
			!wendelon_source.contains(forbidden),
			"ADR 070 leaked UPDATE surface into Wendelon: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr070_prior_adr062_to_064_guards_continue_to_pass_except_no_where_retarget() {
	let architecture_test = architecture_corpus_source();

	assert!(
		architecture_test
			.contains("adr062_update_surface_is_present_under_pk_update_contract_after_adr063")
	);
	assert!(
		architecture_test.contains("adr063_update_where_requirement_is_retargeted_after_adr070")
	);
	assert!(architecture_test.contains("adr063_logical_update_shape_is_name_based"));
	assert!(
		architecture_test.contains("adr063_no_transaction_lock_compare_swap_surface_after_adr070")
	);
	assert!(architecture_test.contains("adr064_update_returning_inherits_pk_update_contract"));
	let stale_update_reject = ["planner_rejects_update", "_without_where"].concat();
	assert!(!architecture_test.contains(&stale_update_reject));
}

#[test]
pub(crate) fn adr070_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "clap", "tracing", "arrow"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr070_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"LogicalPlan::Update",
		"ExecutionPlan::Update",
		"UPDATE",
		"UpdateNotFragmentable",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained UPDATE surface {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr070_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"UpdateWithoutWhereV1",
		"UpdateAllRowsV1",
		"FullTableUpdateV1",
		"BulkUpdateV1",
		"FullTable",
	]);
}

#[test]
pub(crate) fn adr073_sqlparser_is_pinned_to_062() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let cargo_lock =
		std::fs::read_to_string(workspace_root_dir().join("Cargo.lock")).expect("read lockfile");
	let lock_entry = cargo_lock
		.split("[[package]]")
		.find(|entry| entry.contains("name = \"sqlparser\""))
		.expect("sqlparser lockfile entry");

	assert!(manifest.contains("sqlparser = \"0.62\""));
	assert!(lock_entry.contains("version = \"0.62.0\""));
	assert!(!manifest.contains("sqlparser = \"0.54\""));
	assert!(!lock_entry.contains("version = \"0.54.0\""));
}

#[test]
pub(crate) fn adr073_sqlparser_ast_is_quarantined_to_sql_planner() {
	let toron_manifest = manifest_dir();
	let mut offenders = source_lines(&toron_manifest.join("src"))
		.into_iter()
		.filter(|(path, _, _)| {
			path.file_name().and_then(|name| name.to_str()) != Some("sql_planner.rs")
		})
		.filter(|(_, _, line)| line.contains("sqlparser"))
		.collect::<Vec<_>>();

	for root in [
		wendelon_manifest_dir().join("src"),
		wendelon_manifest_dir().join("crates/wendelon-client/src"),
	] {
		offenders.extend(
			source_lines(&root)
				.into_iter()
				.filter(|(_, _, line)| line.contains("sqlparser")),
		);
	}

	assert!(
		offenders.is_empty(),
		"sqlparser leaked outside Toron SQL planner:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr073_no_wendelon_wire_storage_or_ir_leakage() {
	for path in [
		manifest_dir().join("src/ir.rs"),
		manifest_dir().join("src/ir/execution.rs"),
		manifest_dir().join("src/sql_wire.rs"),
		manifest_dir().join("src/storage.rs"),
	] {
		let source = std::fs::read_to_string(&path).expect("read source");
		assert!(
			!source.contains("sqlparser"),
			"{} leaked sqlparser",
			path.display()
		);
	}
}

#[test]
pub(crate) fn adr073_lowered_external_ast_structs_do_not_use_rest_patterns() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for marker in [
		"fn lower_update(&self",
		"fn lower_delete",
		"fn lower_insert(",
		"fn lower_create_table(",
		"fn lower_create_table_as_select",
		"fn lower_drop_table",
		"fn lower_query",
		"fn lower_select",
		"fn lower_table_factor",
		"fn lower_update_target_table",
	] {
		let body = brace_blocks_after_marker(&planner, marker)
			.into_iter()
			.next()
			.unwrap_or_else(|| panic!("{marker} body"))
			.1;
		assert!(!body.contains("{ .."), "{marker} uses a rest pattern");
		assert!(!body.contains("..,"), "{marker} uses a rest pattern");
	}
}

#[test]
pub(crate) fn adr073_semantic_wildcard_arms_only_reject() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_limit = brace_blocks_after_marker(&planner, "fn lower_limit_clause")
		.into_iter()
		.next()
		.expect("lower_limit_clause")
		.1;
	let lower_order_by = brace_blocks_after_marker(&planner, "fn lower_order_by")
		.into_iter()
		.next()
		.expect("lower_order_by")
		.1;
	let lower_select = brace_blocks_after_marker(&planner, "fn lower_select")
		.into_iter()
		.next()
		.expect("lower_select")
		.1;

	assert!(lower_limit.contains("LimitClause::OffsetCommaLimit { .. }"));
	assert!(lower_limit.contains("LIMIT offset, count syntax is not supported"));
	assert!(lower_order_by.contains("OrderByKind::All(_)"));
	assert!(lower_order_by.contains("ORDER BY ALL is not supported"));
	assert!(lower_select.contains("SelectItem::ExprWithAliases"));
	assert!(lower_select.contains("multi-alias projections are not supported"));
	assert!(!lower_select.contains("_ => Ok("));
}

#[test]
pub(crate) fn adr073_query_limit_clause_preserves_ordered_limit_offset_else_rejects() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_limit = brace_blocks_after_marker(&planner, "fn lower_limit_clause")
		.into_iter()
		.next()
		.expect("lower_limit_clause")
		.1;
	// ADR 254: the order-by/limit tail moved into `lower_query_scoped` (the WITH-aware lowering body).
	let lower_query = brace_blocks_after_marker(&planner, "fn lower_query_scoped")
		.into_iter()
		.next()
		.expect("lower_query_scoped")
		.1;
	let tail = brace_blocks_after_marker(&planner, "fn apply_order_by_limit_tail")
		.into_iter()
		.next()
		.expect("apply_order_by_limit_tail")
		.1;
	let lower_order_by = brace_blocks_after_marker(&planner, "fn lower_order_by")
		.into_iter()
		.next()
		.expect("lower_order_by")
		.1;

	for marker in [
		"LimitClause::LimitOffset",
		"limit_by",
		"LIMIT BY is not supported",
		"LimitClause::OffsetCommaLimit",
		"LIMIT offset, count syntax is not supported",
	] {
		assert!(
			lower_limit.contains(marker),
			"limit lowering missing {marker}"
		);
	}
	assert!(lower_query.contains("self.apply_order_by_limit_tail("));
	assert!(tail.contains("LIMIT/OFFSET without ORDER BY is not supported"));
	assert!(tail.contains("let limit = limit_offset"));
	assert!(tail.contains("let offset = limit_offset"));
	assert!(lower_order_by.contains("OrderByKind::Expressions"));
	assert!(lower_order_by.contains("OrderByKind::All(_)"));
	assert!(lower_order_by.contains("ORDER BY ALL is not supported"));
}

#[test]
pub(crate) fn adr073_insert_values_source_rejects_query_decorations() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower_insert = brace_blocks_after_marker(&planner, "fn lower_insert(")
		.into_iter()
		.next()
		.expect("lower_insert")
		.1;
	let lower_source = brace_blocks_after_marker(&planner, "fn lower_insert_source")
		.into_iter()
		.next()
		.expect("lower_insert_source")
		.1;

	assert!(lower_insert.contains("lower_insert_column_list(columns)?"));
	assert!(planner.contains("TableObject::TableQuery"));
	for marker in [
		"SetExpr::Values",
		"query.order_by.is_some()",
		"query.limit_clause.is_some()",
		"query.fetch.is_some()",
		"!query.locks.is_empty()",
		"query.settings.is_some()",
		"query.format_clause.is_some()",
		"!query.pipe_operators.is_empty()",
		"unsupported INSERT source: must be VALUES only",
	] {
		assert!(
			lower_source.contains(marker),
			"INSERT source guard missing {marker}"
		);
	}
}

#[test]
pub(crate) fn adr073_new_062_select_item_and_expr_variants_are_explicitly_rejected() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for marker in [
		"ObjectNamePart",
		"object_name_part_ident",
		"ExprWithAliases",
		"multi-alias projections are not supported",
		"qualified wildcard projections are not supported",
		"ColumnOption::Srid",
		"ColumnOption::Invisible",
		"CAST AS ARRAY is not supported",
		"SELECT EXCLUDE is not supported",
		"FROM-first SELECT is not supported",
		"unsupported INSERT clause: multi-table INSERT",
		"TableObject::TableQuery",
	] {
		assert!(
			planner.contains(marker),
			"missing explicit 0.62 guard {marker}"
		);
	}
}

#[test]
pub(crate) fn adr073_no_new_sql_capability_surface() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let plan_body = brace_blocks_after_marker(&planner, "pub fn plan")
		.into_iter()
		.next()
		.expect("plan body")
		.1;
	let lower_statement = brace_blocks_after_marker(&planner, "fn lower_statement")
		.into_iter()
		.next()
		.expect("lower_statement body")
		.1;
	let dispatch_body = format!("{plan_body}\n{lower_statement}");

	for accepted in [
		"Statement::Query(query)",
		"Statement::CreateTable(create_table)",
		"Statement::Insert(insert)",
		"Statement::Delete(delete)",
		"Statement::Update(update)",
		"Statement::Drop { .. }",
	] {
		assert!(
			dispatch_body.contains(accepted),
			"missing existing surface {accepted}"
		);
	}
	for forbidden in [
		"Statement::AlterTable",
		"Statement::CreateIndex",
		"Statement::CreateView",
		"Statement::Copy",
		"Statement::Merge",
	] {
		assert!(
			!dispatch_body.contains(forbidden),
			"planner gained new SQL surface {forbidden}"
		);
	}
	assert!(dispatch_body.contains(
		"unsupported SQL statement; only SELECT, EXPLAIN, CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE are supported"
	));
}

#[test]
pub(crate) fn adr073_retargets_adr063_064_sqlparser_054_pins() {
	let architecture_test = architecture_corpus_source();

	assert!(
		architecture_test
			.contains("adr063_sql_planner_destructures_sqlparser_062_update_fields_after_adr073")
	);
	assert!(architecture_test.contains("adr063_sqlparser_upgrade_is_now_owned_by_adr073"));
	assert!(architecture_test.contains("adr064_sqlparser_upgrade_is_now_owned_by_adr073"));

	for stale_guard in [
		[
			"fn ",
			"adr063_sql_planner_destructures_sqlparser_054",
			"_update_fields",
		]
		.concat(),
		[
			"fn ",
			"adr063_sqlparser_062_upgrade",
			"_is_not_part_of_update_surface",
		]
		.concat(),
		["fn ", "adr064_sqlparser", "_stays_054"].concat(),
	] {
		assert!(!architecture_test.contains(&stale_guard));
	}
}

#[test]
pub(crate) fn adr073_prior_028_029_030_034_036_exhaustive_destructure_guards_pass_on_062() {
	let architecture_test = architecture_corpus_source();
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");

	for guard in [
		"adr028_aggregate_signature_visitors_are_exhaustive",
		"adr029_create_table_destructure_is_exhaustive",
		"adr030_insert_destructure_is_exhaustive",
		"adr034_drop_table_destructure_is_exhaustive",
		"adr036_delete_destructure_is_exhaustive",
	] {
		assert!(
			architecture_test.contains(guard),
			"missing prior guard {guard}"
		);
	}
	assert!(manifest.contains("sqlparser = \"0.62\""));
}

#[test]
pub(crate) fn adr115_derived_query_tail_fence_lifted_only() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let derived = adr115_source_block(&planner, "fn validate_derived_query_shape");
	let exists = adr115_source_block(&planner, "fn validate_exists_query_shape");
	let derived_select = adr115_source_block(&planner, "fn validate_derived_select_shape");

	assert!(!derived.contains("query.order_by.is_some()"));
	assert!(!derived.contains("query_has_limit_or_offset"));
	for retained in [
		"query.with.is_some()",
		"query.fetch.is_some()",
		"!query.locks.is_empty()",
		"query.for_clause.is_some()",
		"query.settings.is_some()",
		"query.format_clause.is_some()",
		"!query.pipe_operators.is_empty()",
		"unsupported derived table query body",
	] {
		assert!(
			derived.contains(retained),
			"derived query gate lost {retained}"
		);
	}
	assert!(exists.contains("query.order_by.is_some()"));
	assert!(exists.contains("query_has_limit_or_offset"));
	assert!(!derived_select.contains("select.distinct.is_some()"));
	assert!(derived_select.contains("!select.sort_by.is_empty()"));
}

#[test]
pub(crate) fn adr115_derived_lowering_does_not_delegate_to_lower_query() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let derived = adr115_source_block(&planner, "fn lower_derived_table");

	for required in [
		"validate_derived_query_shape(subquery)?",
		"validate_derived_select_shape(select)?",
		"self.lower_select(select, cte)?",
		"self.apply_order_by_limit_tail(",
		"alias_plan(plan, Some(alias))",
	] {
		assert!(
			derived.contains(required),
			"derived lowering lost {required}"
		);
	}
	assert!(!derived.contains("lower_query("));
}

#[test]
pub(crate) fn adr115_sort_reuses_existing_ir_and_executor() {
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source");
	let execution_ir = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(ir.contains("Sort {\n\t\tinput: Box<LogicalPlan>"));
	assert!(execution_ir.contains("Sort {\n\t\tinput: Box<ExecutionPlan>"));
	assert!(!ir.contains("Limit {\n"));
	assert!(!execution_ir.contains("Limit {\n"));
	assert!(exec.contains("fn execute_sort"));
	assert!(!exec.contains("fn execute_limit"));
	assert!(planner.contains("LogicalPlan::Sort"));
}

#[test]
pub(crate) fn adr115_sort_builders_use_single_partition_exchange() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let rune = std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read Rune source");
	let tail = adr115_source_block(&planner, "fn apply_order_by_limit_tail");
	let rune_sort = adr115_source_block(&rune, "pub fn sort");

	for block in [tail, rune_sort] {
		assert!(block.contains("LogicalPlan::Sort"));
		assert!(block.contains("LogicalPlan::Exchange"));
		assert!(block.contains("partition_count: 1"));
		assert!(block.contains("key: Vec::new()"));
	}
}

#[test]
pub(crate) fn adr115_global_sort_barrier_is_a_named_primitive() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let recursive_split = adr115_source_block(&fragment, "fn fragment_execution_plan");
	let barrier_stage = adr115_source_block(&fragment, "fn global_sort_barrier_stage");

	assert!(fragment.contains("fn global_sort_barrier("));
	assert!(fragment.contains("fn global_sort_barrier_count("));
	assert!(recursive_split.contains("if let Some(barrier) = global_sort_barrier(&plan)"));
	assert!(recursive_split.contains("fragment_execution_plan("));
	assert!(recursive_split.contains("global_sort_barrier_stage("));
	assert!(barrier_stage.contains(".tail"));
	assert!(fragment.contains("tail: Vec<UnaryPlanTail>"));
}

#[test]
pub(crate) fn adr117_multiple_global_sorts_split_recursively() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let sort_tests = std::fs::read_to_string(manifest_dir().join("tests/sort_ordering_test.rs"))
		.expect("read sort tests");
	let rune_tests =
		std::fs::read_to_string(manifest_dir().join("tests/rune_relation_handles_test.rs"))
			.expect("read Rune tests");
	let distributed_tests =
		std::fs::read_to_string(manifest_dir().join("tests/distributed_process_test.rs"))
			.expect("read distributed process tests");
	let recursive_split = adr115_source_block(&fragment, "fn fragment_execution_plan");

	assert!(!fragment.contains("multiple global sort barriers are not supported in V1"));
	assert!(!fragment.contains("FragmentError::MultipleGlobalSortBarriers"));
	assert!(recursive_split.contains("if let Some(barrier) = global_sort_barrier(&plan)"));
	assert!(recursive_split.contains("FragmentTerminalOutput::ShuffleProducer"));
	assert!(
		sort_tests.contains("three_global_sort_barriers_allocate_unique_stage_and_exchange_ids")
	);
	assert!(rune_tests.contains("rune_top_k_of_top_k_fragments_and_executes"));
	assert!(
		distributed_tests.contains("multiple_ordered_top_k_process_query_preserves_inner_barrier")
	);
	assert!(!sort_tests.contains("MultipleGlobalSortBarriers"));
	assert!(!rune_tests.contains("MultipleGlobalSortBarriers"));
}

#[test]
pub(crate) fn adr117_terminal_output_is_explicit() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let terminal_output = adr115_source_block(&fragment, "enum FragmentTerminalOutput");
	let wrap_plan = adr115_source_block(&fragment, "fn wrap_plan");
	let barrier_stage = adr115_source_block(&fragment, "fn global_sort_barrier_stage");

	assert!(terminal_output.contains("FinalResult"));
	assert!(terminal_output.contains("ShuffleProducer"));
	assert!(terminal_output.contains("exchange_id: ExchangeId"));
	assert!(terminal_output.contains("partition_key: BoundExprProgram"));
	assert!(wrap_plan.contains("ExecutionPlan::Exchange"));
	assert!(barrier_stage.contains("terminal_output.wrap_plan"));
	assert!(barrier_stage.contains("terminal_output.task_output"));
}

#[test]
pub(crate) fn adr117_fragment_ids_are_allocated_not_fixed() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let production = fragment
		.split("#[cfg(test)]")
		.next()
		.expect("fragment production source");

	assert!(production.contains("struct FragmentIdAllocator"));
	assert!(production.contains("fn alloc_stage"));
	assert!(production.contains("fn alloc_exchange"));
	assert!(production.contains("allocator.alloc_stage()"));
	assert!(production.contains("allocator.alloc_exchange()"));
	assert!(!production.contains("StageId::new(0)"));
	assert!(!production.contains("ExchangeId::new(0)"));
}

#[test]
pub(crate) fn adr117_no_barrier_inside_join_inputs() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let join_fragment = adr115_source_block(&fragment, "fn fragment_hash_join_plan");

	assert!(fragment.contains("SortBarrierInsideJoinInput"));
	assert!(fragment.contains("fn reject_sort_barrier_inside_join_inputs"));
	assert!(join_fragment.contains("reject_sort_barrier_inside_join_inputs(&join)?"));
	assert!(join_fragment.contains("extract_exchange(*join.left)?"));
	assert!(join_fragment.contains("extract_exchange(*join.right)?"));
	assert!(!join_fragment.contains("global_sort_barrier"));
}

#[test]
pub(crate) fn adr117_take_is_not_a_sort_barrier() {
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let barrier = adr115_source_block(&fragment, "fn global_sort_barrier");
	let count = adr115_source_block(&fragment, "fn global_sort_barrier_count");

	assert!(!barrier.contains("ExecutionPlan::Take"));
	assert!(
		count.contains("ExecutionPlan::Take { input, .. } => global_sort_barrier_count(input)")
	);
}

#[test]
pub(crate) fn adr115_no_merge_sort_or_topk_pushdown() {
	for path in [
		"src/distributed/fragment.rs",
		"src/exec.rs",
		"src/pbc.rs",
		"src/storage.rs",
	] {
		let source =
			std::fs::read_to_string(manifest_dir().join(path)).unwrap_or_else(|_| panic!("{path}"));
		let source = source.replace("column_group_top_k", "");
		assert!(!source.contains("merge_sort"), "{path} added merge sort");
		assert!(!source.contains("topk"), "{path} added top-k pushdown");
		assert!(!source.contains("top_k"), "{path} added top-k pushdown");
	}
}

#[test]
pub(crate) fn adr116_unordered_limit_is_take_not_sort() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let rune = std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read Rune source");
	let tests = std::fs::read_to_string(manifest_dir().join("tests/sort_ordering_test.rs"))
		.expect("read sort tests");
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source");
	let execution_ir = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let binder = binder_source();

	assert!(planner.contains("LIMIT/OFFSET without ORDER BY is not supported"));
	assert!(rune.contains("LIMIT/OFFSET without ORDER BY is not supported"));
	assert!(tests.contains("limit_without_order_by_lowers_to_unordered_take"));
	assert!(tests.contains("offset_without_order_by_still_rejects"));
	assert!(ir.contains("Take {\n\t\tinput: Box<LogicalPlan>"));
	assert!(execution_ir.contains("Take {\n\t\tinput: Box<ExecutionPlan>"));
	assert!(planner.contains("LogicalPlan::Take"));
	assert!(binder.contains("ExecutionPlan::Take"));
	assert!(binder.contains("apply_safe_row_limit_to_scan"));
	assert!(rune.contains("pub fn take"));
}

#[test]
pub(crate) fn adr116_row_limit_pushes_through_scan_request_to_wendelon_meta() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read Toron storage source");
	let client =
		std::fs::read_to_string(wendelon_manifest_dir().join("crates/wendelon-client/src/lib.rs"))
			.expect("read Wendelon client source");
	let schema = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema");
	let data_server =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/network/data_server.rs"))
			.expect("read Wendelon data server");

	assert!(storage.contains("pub row_limit: Option<usize>"));
	assert!(storage.contains("options.row_limit = request"));
	assert!(storage.contains("std::num::NonZeroU64::new(limit as u64)"));
	assert!(client.contains("pub row_limit: Option<NonZeroU64>"));
	assert!(client.contains("row_limit_rows: options.row_limit.map(NonZeroU64::get).unwrap_or(0)"));
	assert!(schema.contains("pub row_limit_rows: u64"));
	assert!(schema.contains("offset_of!(TableQueryRequestMeta, row_limit_rows)"));
	assert!(schema.contains("120"));
	assert!(data_server.contains("fn table_query_row_limit"));
	assert!(data_server.contains("fn take_output_batch"));
}

fn adr115_source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}
