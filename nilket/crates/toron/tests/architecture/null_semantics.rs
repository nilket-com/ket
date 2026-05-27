use super::support::*;

#[test]
pub(crate) fn adr109_conditional_sugar_desugars_to_case() {
	let manifest_dir = manifest_dir();
	let ir = std::fs::read_to_string(manifest_dir.join("src/ir.rs")).expect("read ir");
	let execution_ir =
		std::fs::read_to_string(manifest_dir.join("src/ir/execution.rs")).expect("read exec ir");
	let planner =
		std::fs::read_to_string(manifest_dir.join("src/sql_planner.rs")).expect("read planner");
	let binder = std::fs::read_to_string(manifest_dir.join("src/binder.rs")).expect("read binder");
	let pbc = std::fs::read_to_string(manifest_dir.join("src/pbc.rs")).expect("read pbc");
	let exec = std::fs::read_to_string(manifest_dir.join("src/exec.rs")).expect("read exec");

	let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
		.into_iter()
		.next()
		.expect("lower_expr body")
		.1;
	let classifier = brace_blocks_after_marker(&planner, "fn conditional_sugar_function_name")
		.into_iter()
		.next()
		.expect("conditional sugar classifier")
		.1;
	let lower_coalesce = brace_blocks_after_marker(&planner, "fn lower_coalesce")
		.into_iter()
		.next()
		.expect("lower_coalesce body")
		.1;
	let lower_if = brace_blocks_after_marker(&planner, "fn lower_if")
		.into_iter()
		.next()
		.expect("lower_if body")
		.1;
	let lower_nullif = brace_blocks_after_marker(&planner, "fn lower_nullif")
		.into_iter()
		.next()
		.expect("lower_nullif body")
		.1;
	let lower_ifnull_nvl = brace_blocks_after_marker(&planner, "fn lower_ifnull_nvl")
		.into_iter()
		.next()
		.expect("lower_ifnull_nvl body")
		.1;
	let lower_nvl2 = brace_blocks_after_marker(&planner, "fn lower_nvl2")
		.into_iter()
		.next()
		.expect("lower_nvl2 body")
		.1;
	let lower_statement = brace_blocks_after_marker(&planner, "fn lower_statement")
		.into_iter()
		.next()
		.expect("lower_statement body")
		.1;

	assert!(planner.contains("enum ConditionalSugarFunction"));
	for name in ["COALESCE", "NULLIF", "IF", "IIF", "IFNULL", "NVL", "NVL2"] {
		assert!(classifier.contains(name), "classifier missing {name}");
	}
	assert!(lower_expr.contains("SqlExpr::Function(f) => match conditional_sugar_function_name"));
	for variant in [
		"ConditionalSugarFunction::Coalesce",
		"ConditionalSugarFunction::If",
		"ConditionalSugarFunction::Iif",
		"ConditionalSugarFunction::Nullif",
		"ConditionalSugarFunction::Ifnull",
		"ConditionalSugarFunction::Nvl",
		"ConditionalSugarFunction::Nvl2",
	] {
		assert!(lower_expr.contains(variant), "lower_expr missing {variant}");
	}
	assert!(planner.contains("fn function_positional_args"));
	assert!(!planner.contains("fn lower_conditional_value_arg"));
	assert!(!planner.contains("fn conditional_null_placeholder"));
	assert!(lower_coalesce.contains("Expr::Case"));
	assert!(lower_coalesce.contains("Expr::IsNotNull"));
	assert!(lower_coalesce.contains("COALESCE requires at least one argument"));
	assert!(lower_if.contains("Expr::Case"));
	assert!(lower_if.contains("self.lower_expr(condition)?"));
	assert!(lower_if.contains("self.lower_expr(truthy)?"));
	assert!(lower_nullif.contains("Expr::Case"));
	assert!(lower_nullif.contains("Expr::Eq"));
	assert!(lower_nullif.contains("Expr::UntypedNull"));
	assert!(lower_ifnull_nvl.contains("Expr::Case"));
	assert!(lower_ifnull_nvl.contains("Expr::IsNotNull"));
	assert!(lower_ifnull_nvl.contains("IFNULL/NVL requires exactly two arguments"));
	assert!(lower_nvl2.contains("Expr::Case"));
	assert!(lower_nvl2.contains("Expr::IsNotNull"));
	assert!(lower_nvl2.contains("NVL2 requires exactly three arguments"));
	assert!(planner.contains("rewrite_function_argument_exprs"));
	assert!(!lower_statement.contains("Statement::If"));
	for forbidden in ["Coalesce", "NullIf", "Nullif"] {
		assert!(
			!ir.contains(&format!("{forbidden} {{")),
			"Expr must not grow {forbidden} variant"
		);
		assert!(
			!execution_ir.contains(&format!("{forbidden} {{")),
			"BoundExprNode must not grow {forbidden} variant"
		);
	}
	assert!(!pbc.contains("OP_COALESCE"));
	assert!(!pbc.contains("OP_NULLIF"));
	assert!(!pbc.contains("OP_NVL2"));
	assert!(!exec.contains("evaluate_coalesce"));
	assert!(!exec.contains("evaluate_nullif"));
	assert!(!exec.contains("evaluate_nvl2"));
	assert!(binder.contains("Expr::Case"));
}

#[test]
pub(crate) fn adr112_null_sentinel_retired() {
	let planner = source("src/sql_planner.rs");
	let binder = source("src/binder.rs");
	let bind_case = brace_blocks_after_marker(&binder, "fn bind_case")
		.into_iter()
		.next()
		.expect("bind_case body")
		.1;
	let bind_case_result_input = brace_blocks_after_marker(&binder, "fn bind_case_result_input")
		.into_iter()
		.next()
		.expect("bind_case_result_input body")
		.1;
	let type_case_result = brace_blocks_after_marker(&binder, "fn type_case_result")
		.into_iter()
		.next()
		.expect("type_case_result body")
		.1;
	let update_null = brace_blocks_after_marker(&binder, "fn typed_update_null_for_target")
		.into_iter()
		.next()
		.expect("typed_update_null_for_target body")
		.1;

	assert!(!planner.contains("fn conditional_null_placeholder"));
	assert!(!planner.contains("fn update_null_placeholder"));
	assert!(!binder.contains("fn is_case_null_placeholder"));
	assert!(!binder.contains("fn typed_null_for_case"));
	assert!(binder.contains("fn typed_null_for_data_type"));
	assert!(binder.contains("fn resolve_untyped_null"));
	assert!(bind_case_result_input.contains("is_untyped_null(expr)"));
	assert!(bind_case.contains("CaseResultInput::Bound(result_id)"));
	assert!(
		bind_case.contains("case_output_logical_type(builder, &typed_result_ids, &output_type)?")
	);
	assert!(bind_case.contains("(DataType::Null, LogicalTypeId::NAKED)"));
	assert!(bind_case.contains("materialize_null_for_output_type(builder, &output_type)?"));
	assert!(type_case_result.contains("CaseResultInput::Bound(id)"));
	assert!(type_case_result.contains("CaseResultInput::UntypedNull"));
	assert!(type_case_result.contains("materialize_null_for_output_type(builder, output_type)"));
	assert!(update_null.contains("is_untyped_null(expr)"));
	assert!(update_null.contains("typed_null_for_data_type(field.data_type())?"));
}

#[test]
pub(crate) fn adr112_explicit_null_is_transient_binder_consumed() {
	let ir = source("src/ir.rs");
	let execution_ir = source("src/ir/execution.rs");
	let exec = source("src/exec.rs");
	let planner = source("src/sql_planner.rs");
	let binder = source("src/binder.rs");
	let pbc = source("src/pbc.rs");
	let expr_enum = ir
		.split("pub enum Expr")
		.nth(1)
		.and_then(|rest| rest.split("impl Expr").next())
		.expect("Expr enum");
	let scalar_enum = ir
		.split("pub enum ScalarValue")
		.nth(1)
		.and_then(|rest| rest.split("impl ScalarValue").next())
		.expect("ScalarValue enum");
	let bound_node = execution_ir
		.split("pub enum BoundExprNode")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("BoundExprNode enum");
	let lower_expr = brace_blocks_after_marker(&planner, "fn lower_expr")
		.into_iter()
		.next()
		.expect("lower_expr body")
		.1;
	let bind_expr = brace_blocks_after_marker(&binder, "fn bind_expr_into_program")
		.into_iter()
		.next()
		.expect("bind_expr_into_program body")
		.1;

	assert!(expr_enum.contains("UntypedNull"));
	assert!(!scalar_enum.contains("Null"));
	assert!(!bound_node.contains("UntypedNull"));
	assert!(!pbc.contains("UntypedNull"));
	assert!(!exec.contains("BoundExprNode::UntypedNull"));
	assert!(lower_expr.contains("Value::Null => Ok(Expr::UntypedNull)"));
	assert!(bind_expr.contains("Expr::UntypedNull => Err"));
	assert!(bind_expr.contains("NULL literal has no type in this position"));
	assert!(bind_expr.contains("CAST(NULL AS <type>)"));
}

#[test]
pub(crate) fn adr112_null_predicates_stay_residual() {
	let binder = source("src/binder.rs");
	let pbc = source("src/pbc.rs");
	let comparison = brace_blocks_after_marker(&binder, "fn bind_comparison")
		.into_iter()
		.next()
		.expect("bind_comparison body")
		.1;
	let in_list = brace_blocks_after_marker(&binder, "fn bind_in_list")
		.into_iter()
		.next()
		.expect("bind_in_list body")
		.1;

	assert!(comparison.contains("resolve_untyped_null(builder, &right_type)?"));
	assert!(comparison.contains("resolve_untyped_null(builder, &left_type)?"));
	assert!(in_list.contains("resolve_untyped_null(builder, &expr_type)?"));
	assert!(pbc.contains("NullLiteralUnrepresentable"));
	assert!(pbc.contains("ScalarValue::Int32(None)"));
	assert!(pbc.contains("PBC doesn't have OP_PUSH_NULL"));
}

#[test]
pub(crate) fn adr114_contextless_null_projection_is_result_position_only() {
	let execution_ir = source("src/ir/execution.rs");
	let binder = source("src/binder.rs");
	let exec = source("src/exec.rs");
	let pbc = source("src/pbc.rs");
	let bound_node = execution_ir
		.split("pub enum BoundExprNode")
		.nth(1)
		.and_then(|rest| rest.split("pub struct BoundExprProgram").next())
		.expect("BoundExprNode enum");
	let projection_bind =
		brace_blocks_after_marker(&binder, "async fn bind_projection_expr_program")
			.into_iter()
			.next()
			.expect("bind_projection_expr_program body")
			.1;
	let returning_bind = brace_blocks_after_marker(&binder, "async fn bind_returning_projection")
		.into_iter()
		.next()
		.expect("bind_returning_projection body")
		.1;
	let bind_program = brace_blocks_after_marker(&binder, "fn bind_expr_program_with_null_policy")
		.into_iter()
		.next()
		.expect("bind_expr_program_with_null_policy body")
		.1;
	let bind_expr = brace_blocks_after_marker(&binder, "fn bind_expr_into_program")
		.into_iter()
		.next()
		.expect("bind_expr_into_program body")
		.1;
	let bind_case = brace_blocks_after_marker(&binder, "fn bind_case")
		.into_iter()
		.next()
		.expect("bind_case body")
		.1;
	let materialize_null =
		brace_blocks_after_marker(&binder, "fn materialize_null_for_output_type")
			.into_iter()
			.next()
			.expect("materialize_null_for_output_type body")
			.1;
	let update_bind = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update body")
		.1;
	let ctas_types = brace_blocks_after_marker(&binder, "fn is_supported_ctas_column_type")
		.into_iter()
		.next()
		.expect("is_supported_ctas_column_type body")
		.1;

	assert!(bound_node.contains("NullLiteral"));
	assert!(execution_ir.contains("pub fn append_null"));
	assert!(execution_ir.contains("BoundExprNode::NullLiteral => Some(true)"));
	assert!(execution_ir.contains("BoundExprNode::NullLiteral => \"NULL\".to_string()"));
	assert!(exec.contains("BoundExprNode::NullLiteral"));
	assert!(exec.contains("new_null_array(&DataType::Null, batch.num_rows())"));
	assert!(bind_expr.contains("Expr::UntypedNull => Err"));
	assert!(bind_expr.contains("NULL literal has no type in this position"));
	assert!(projection_bind.contains("NullRootPolicy::MaterializeResultNull"));
	assert!(returning_bind.contains("self.bind_projection_expr_program"));
	assert!(binder.contains("NullRootPolicy::Reject"));
	assert!(bind_program.contains("NullRootPolicy::MaterializeResultNull"));
	assert!(bind_program.contains("is_untyped_null(expr)"));
	assert!(bind_program.contains("builder.append_null()"));
	assert!(bind_case.contains("(DataType::Null, LogicalTypeId::NAKED)"));
	assert!(materialize_null.contains("output_type == &DataType::Null"));
	assert!(materialize_null.contains("builder.append_null()"));
	assert!(update_bind.contains("typed_update_null_for_target(expr, field)?"));
	assert!(update_bind.contains("bind_expr_program(&source_exprs"));
	assert!(!update_bind.contains("bind_projection_expr_program(&source_exprs"));
	assert!(pbc.contains("NULL literal is not PBC-eligible"));
	assert!(pbc.contains("NULL literal reached PBC emitter"));
	assert!(!ctas_types.contains("DataType::Null"));
}

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative)).expect("read source")
}
