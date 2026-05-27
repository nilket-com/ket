#![allow(unused_imports)]

use super::sql_planner_surface::*;
use super::support::*;

fn rune_engine_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read rune engine source")
}

fn sql_planner_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source")
}

fn ir_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source")
}

fn binder_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source")
}

fn exec_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source")
}

fn pbc_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read PBC source")
}

fn rune_relation_handles_test_source() -> String {
	std::fs::read_to_string(manifest_dir().join("tests/rune_relation_handles_test.rs"))
		.expect("read Rune relation handles tests")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

fn registered_rune_function_names(module: &str) -> Vec<String> {
	let mut names = Vec::new();
	for line in module.lines() {
		if let Some(rest) = line.split("m.associated_function(\"").nth(1)
			&& let Some(name) = rest.split('"').next()
		{
			names.push(name.to_string());
		}
		if let Some(rest) = line.split("m.function([\"").nth(1)
			&& let Some(name) = rest.split('"').next()
		{
			names.push(name.to_string());
		}
	}
	names
}

#[test]
pub(crate) fn adr079_rune_dataframe_remains_lazy_logical_plan_handle() {
	let source = rune_engine_source();
	let dataframe = source
		.split("pub struct RuneDataFrame")
		.nth(1)
		.and_then(|rest| rest.split("pub struct RuneExpr").next())
		.expect("RuneDataFrame block");

	assert!(dataframe.contains("plan: LogicalPlan"));
	assert!(!dataframe.contains("RecordBatch"));
	assert!(!dataframe.contains("ScanStream"));
	assert!(!dataframe.contains("Iterator"));
	assert!(source.contains(") -> Result<LogicalPlan, FrontendError>"));
}

#[test]
pub(crate) fn adr079_toron_context_exposes_sql_relation_source() {
	let rune = rune_engine_source();
	let planner = sql_planner_source();
	let sql_method = source_block(&rune, "pub fn sql");

	assert!(sql_method.contains("query: String"));
	assert!(sql_method.contains("VmResult<RuneDataFrame>"));
	assert!(sql_method.contains("SqlPlanner::new().plan_relation(&query)"));
	assert!(sql_method.contains("RuneDataFrame { plan }"));
	assert!(rune.contains("m.associated_function(\"sql\", ToronContext::sql)?;"));
	assert!(planner.contains("pub fn plan_relation(&self, sql: &str)"));
}

#[test]
pub(crate) fn adr079_ctx_sql_rejects_multi_statement_input() {
	let planner = sql_planner_source();
	let parse_one = source_block(&planner, "fn parse_one_statement");
	let plan_relation = source_block(&planner, "pub fn plan_relation");

	assert!(plan_relation.contains("self.parse_one_statement(sql)?"));
	assert!(parse_one.contains("Parser::parse_sql"));
	assert!(parse_one.contains("ast.is_empty()"));
	assert!(parse_one.contains("ast.len() != 1"));
	assert!(parse_one.contains("exactly one SQL statement is required"));
}

#[test]
pub(crate) fn adr079_ctx_sql_rejects_ddl_and_dml() {
	let planner = sql_planner_source();
	let plan_relation = source_block(&planner, "pub fn plan_relation");
	let terminal = source_block(&planner, "fn relation_plan_is_terminal");

	assert!(plan_relation.contains("relation_plan_is_terminal(&plan)"));
	for variant in [
		"LogicalPlan::CreateTable",
		"LogicalPlan::CreateTableAsSelect",
		"LogicalPlan::DropTable",
		"LogicalPlan::Delete",
		"LogicalPlan::Update",
		"LogicalPlan::Insert",
		"LogicalPlan::InsertSelect",
	] {
		assert!(
			terminal.contains(variant),
			"terminal reject missing {variant}"
		);
	}
	assert!(plan_relation.contains("ctx.sql accepts single query relations only"));
}

#[test]
pub(crate) fn adr079_ctx_sql_does_not_execute_or_materialize() {
	let rune = rune_engine_source();
	let sql_method = source_block(&rune, "pub fn sql");

	for forbidden in [
		"Executor",
		"execute_bound",
		".execute(",
		"RecordBatch",
		"ScanStream",
		"collect",
		"persist",
		"write",
	] {
		assert!(
			!sql_method.contains(forbidden),
			"ctx.sql materialization/action marker found: {forbidden}"
		);
	}
	assert!(sql_method.contains("VmResult::Ok(RuneDataFrame { plan })"));
}

#[test]
pub(crate) fn adr079_sql_derived_relation_can_be_wrapped_by_rune_aggregate() {
	let rune = rune_engine_source();
	let sql_method = source_block(&rune, "pub fn sql");
	let aggregate_method = source_block(&rune, "pub fn aggregate");

	assert!(sql_method.contains("RuneDataFrame { plan }"));
	assert!(aggregate_method.contains("LogicalPlan::Aggregate"));
	assert!(aggregate_method.contains("input: Box::new(self.plan)"));
	assert!(aggregate_method.contains("aggr_expr.into_iter()"));
	assert!(rune.contains("pub fn sum(expr: RuneExpr) -> RuneAggr"));
	assert!(rune.contains("func: AggregateFunction::Sum"));
	assert!(rune.contains("m.function([\"sum\"], sum).build()?;"));
}

#[test]
pub(crate) fn adr079_sql_alias_can_feed_rune_column_expression() {
	let rune = rune_engine_source();
	let tests = std::fs::read_to_string(manifest_dir().join("tests/rune_relation_handles_test.rs"))
		.expect("read ADR 079 integration test");
	let col_function = source_block(&rune, "pub fn col");

	assert!(col_function.contains("Expr::column(name)"));
	assert!(tests.contains("sum(col(\"cnt\"))"));
	assert!(tests.contains("SELECT region, COUNT(1) AS cnt FROM sales GROUP BY region"));
	assert!(tests.contains("rune_sql_group_by_alias_can_feed_outer_sum_and_bind"));
}

#[test]
pub(crate) fn adr079_nested_sql_aggregate_continuation_binds_inner_alias() {
	let tests = std::fs::read_to_string(manifest_dir().join("tests/rune_relation_handles_test.rs"))
		.expect("read ADR 079 integration test");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");

	assert!(tests.contains("rune_sql_group_by_alias_can_feed_outer_sum_and_bind"));
	assert!(tests.contains("bind(&logical, &catalog(), &PlannerConfig::default())"));
	assert!(tests.contains("field(0).name(), \"sum\""));
	assert!(binder.contains("fn project_program_roots"));
	assert!(binder.contains("schema_for_projected_exprs_with_existing_names"));
	assert!(binder.contains("let output_remap = ordinal_remap(\n\t\t\t\t&output_columns"));
}

#[test]
pub(crate) fn adr079_composed_rune_sql_plan_enters_normal_binder_path() {
	let rune = rune_engine_source();
	let tests = std::fs::read_to_string(manifest_dir().join("tests/rune_relation_handles_test.rs"))
		.expect("read ADR 079 integration test");

	assert!(!rune.contains("PlannerConfig"));
	assert!(!rune.contains("bind("));
	assert!(!rune.contains("ExecutionPlan"));
	assert!(tests.contains("rune_sql_composition_preserves_existing_pushdown_path"));
	assert!(tests.contains("FilterDisposition::FullyPushed"));
	assert!(tests.contains("scan_pbc(scan).is_some()"));
}

#[test]
pub(crate) fn adr079_no_rune_local_optimizer_binder_or_executor() {
	let rune = rune_engine_source();

	for forbidden in [
		"Executor",
		"ExecutionPlan",
		"PlannerConfig",
		"PbcCompiler",
		"optimize",
		"pushdown",
		"push_down",
	] {
		assert!(
			!rune.contains(forbidden),
			"Rune engine gained local planner/executor marker: {forbidden}"
		);
	}
	assert!(rune.contains("execute_script("));
	assert!(rune.contains("Ok(df.plan)"));
}

#[test]
pub(crate) fn adr079_no_worker_side_rune_udf_surface() {
	let manifest_dir = manifest_dir();
	let worker_roots = [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
	];
	let offenders = worker_roots
		.iter()
		.flat_map(|root| {
			if root.is_dir() {
				source_lines(root)
			} else {
				let source = std::fs::read_to_string(root).expect("read worker source");
				source
					.lines()
					.enumerate()
					.map(|(idx, line)| (root.clone(), idx + 1, line.to_string()))
					.collect::<Vec<_>>()
			}
		})
		.filter(|(_, _, line)| {
			[
				"RuneEngine",
				"RuneDataFrame",
				"RuneExpr",
				"RuneAggr",
				"execute_script",
				"rune::",
			]
			.iter()
			.any(|marker| line.contains(marker))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"worker-side Rune surface found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr079_no_new_wendelon_wire_storage_pbc_opcode_or_dependency() {
	let rune = rune_engine_source();
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");

	for forbidden in ["wendelon", "Storage", "ScanStream", "Pbc", "PBC"] {
		assert!(
			!rune.contains(forbidden),
			"ctx.sql/Rune implementation touched cross-ring or storage marker: {forbidden}"
		);
	}
	assert!(manifest.contains("rune ="));
	assert!(!manifest.contains("syn ="));

	for source_file in ["src/wire.rs", "src/storage.rs", "src/pbc.rs"] {
		let source = std::fs::read_to_string(manifest_dir().join(source_file))
			.unwrap_or_else(|_| panic!("read {source_file}"));
		assert!(!source.contains("ctx.sql"));
		assert!(!source.contains("plan_relation"));
		assert!(!source.contains("RuneDataFrame"));
	}
}

#[test]
pub(crate) fn adr079_sqlparser_ast_remains_quarantined_to_sql_planner() {
	adr073_sqlparser_ast_is_quarantined_to_sql_planner();
	let rune = rune_engine_source();

	assert!(!rune.contains("sqlparser"));
	assert!(sql_planner_source().contains("fn parse_one_statement"));
}

#[test]
pub(crate) fn adr079_rune_sandbox_allow_list_remains_in_force() {
	let rune = rune_engine_source();
	let with_config = source_block(&rune, "pub fn with_config");
	let module = source_block(&rune, "pub fn create_toron_module");
	let default_modules_marker = ["with", "_", "default", "_", "modules"].concat();

	assert!(with_config.contains("Context::new()"));
	assert!(with_config.contains("install_minimal_primitives"));
	assert!(!rune.contains(&default_modules_marker));
	assert!(module.contains("m.associated_function(\"sql\", ToronContext::sql)?;"));
	assert!(module.contains("m.function([\"sum\"], sum).build()?;"));
}

#[test]
pub(crate) fn adr080_rune_named_expr_is_host_surface_only() {
	let rune = rune_engine_source();
	let named = source_block(&rune, "pub struct RuneNamedExpr");
	let manifest_dir = manifest_dir();
	let rune_engine = manifest_dir.join("src/rune_engine.rs");
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| path != &rune_engine && line.contains("RuneNamedExpr"))
		.collect::<Vec<_>>();

	assert!(named.contains("expr: Expr"));
	assert!(named.contains("name: String"));
	assert!(
		offenders.is_empty(),
		"RuneNamedExpr leaked outside rune_engine.rs:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr080_rune_expr_does_not_carry_alias_state() {
	let rune = rune_engine_source();
	let expr = source_block(&rune, "pub struct RuneExpr");

	assert!(expr.contains("expr: Expr"));
	for forbidden in ["alias", "output_name", "Option<String>"] {
		assert!(
			!expr.contains(forbidden),
			"RuneExpr gained forbidden naming state: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr080_named_projection_lowers_to_existing_project_output_names() {
	let rune = rune_engine_source();
	let project_named = source_block(&rune, "pub fn project_named");

	assert!(project_named.contains("exprs: Vec<RuneNamedExpr>"));
	assert!(project_named.contains("LogicalPlan::Project"));
	assert!(project_named.contains("output_names.push(Some(named.name))"));
	assert!(project_named.contains("plan_exprs.push(named.expr)"));
	assert!(!project_named.contains("LogicalPlan::Join"));
	assert!(!project_named.contains("Aggregate"));
	assert!(!project_named.contains("normalize_"));
}

#[test]
pub(crate) fn adr080_anonymous_project_surface_remains_available() {
	let rune = rune_engine_source();
	let project = source_block(&rune, "pub fn project");

	assert!(project.contains("exprs: Vec<RuneExpr>"));
	assert!(project.contains("LogicalPlan::Project"));
	assert!(project.contains("output_names: vec![None; exprs.len()]"));
}

#[test]
pub(crate) fn adr080_aggregate_helper_pack_uses_existing_stable_functions() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");

	for (function, variant) in [
		("pub fn count_all", "AggregateFunction::CountAll"),
		("pub fn count(expr: RuneExpr)", "AggregateFunction::Count"),
		("pub fn sum(expr: RuneExpr)", "AggregateFunction::Sum"),
		("pub fn avg(expr: RuneExpr)", "AggregateFunction::Avg"),
		("pub fn min(expr: RuneExpr)", "AggregateFunction::Min"),
		("pub fn max(expr: RuneExpr)", "AggregateFunction::Max"),
	] {
		let body = source_block(&rune, function);
		assert!(body.contains("RuneAggr"));
		assert!(body.contains(variant), "{function} missing {variant}");
	}
	for registration in [
		"m.function([\"count_all\"], count_all).build()?;",
		"m.function([\"count\"], count).build()?;",
		"m.function([\"sum\"], sum).build()?;",
		"m.function([\"avg\"], avg).build()?;",
		"m.function([\"min\"], min).build()?;",
		"m.function([\"max\"], max).build()?;",
	] {
		assert!(module.contains(registration), "missing {registration}");
	}
}

#[test]
pub(crate) fn adr080_count_all_helper_uses_placeholder_count_arg() {
	let rune = rune_engine_source();
	let count_all = source_block(&rune, "pub fn count_all");

	assert!(count_all.contains("AggregateFunction::CountAll"));
	assert!(count_all.contains("Expr::column(\"count\")"));
}

#[test]
pub(crate) fn adr080_dataframe_count_shortcut_remains_separate() {
	let rune = rune_engine_source();
	let dataframe_count = source_block(&rune, "pub fn count(self)");
	let count_all = source_block(&rune, "pub fn count_all");

	assert!(dataframe_count.contains("LogicalPlan::Aggregate"));
	assert!(dataframe_count.contains("AggregateFunction::CountAll"));
	assert!(count_all.contains("-> RuneAggr"));
	assert!(!count_all.contains("LogicalPlan::Aggregate"));
}

#[test]
pub(crate) fn adr080_no_logical_aggregate_output_names_or_alias_field() {
	let ir = ir_source();
	let rune = rune_engine_source();
	let logical_plan = source_block(&ir, "pub enum LogicalPlan");
	let aggregate_variant = logical_plan
		.split("Aggregate {")
		.nth(1)
		.and_then(|rest| rest.split("Exchange {").next())
		.expect("LogicalPlan Aggregate variant");
	let rune_aggr = source_block(&rune, "pub struct RuneAggr");

	assert!(aggregate_variant.contains("input: Box<LogicalPlan>"));
	assert!(aggregate_variant.contains("group_expr: Vec<Expr>"));
	assert!(aggregate_variant.contains("aggr_expr: Vec<(AggregateFunction, Vec<Expr>)>"));
	for forbidden in ["output_names", "alias", "name:"] {
		assert!(
			!aggregate_variant.contains(forbidden),
			"LogicalPlan::Aggregate gained forbidden naming field: {forbidden}"
		);
		assert!(
			!rune_aggr.contains(forbidden),
			"RuneAggr gained forbidden naming field: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr080_duplicate_aggregate_output_names_use_existing_ambiguous_error() {
	let tests = rune_relation_handles_test_source();

	assert!(tests.contains("rune_duplicate_aggregate_output_name_is_ambiguous"));
	assert!(tests.contains("BindingError::AmbiguousColumn"));
	assert!(tests.contains("column == \"sum\""));
	assert!(tests.contains("[sum(col(\"amount\")), sum(col(\"id\"))]"));
}

#[test]
pub(crate) fn adr080_named_continuation_binds_through_normal_binder() {
	let rune = rune_engine_source();
	let tests = rune_relation_handles_test_source();

	assert!(tests.contains("rune_project_named_sql_multi_hop_continuation_binds_and_executes"));
	assert!(tests.contains("bind(&logical, &catalog(), &PlannerConfig::default())"));
	assert!(
		tests.contains(
			"assert_schema_names(bound.schema().as_ref(), &[\"bucket\", \"gross\", \"n\"])"
		)
	);
	assert!(tests.contains("string_column_rows(&batches, 0)"));
	assert!(!rune.contains("PlannerConfig"));
	assert!(!rune.contains("bind("));
	assert!(!rune.contains("BindingError"));
}

#[test]
pub(crate) fn adr080_no_rune_local_schema_binder_optimizer_or_executor() {
	adr079_no_rune_local_optimizer_binder_or_executor();
	let rune = rune_engine_source();

	for forbidden in ["schema_for", "resolve_column", "BindingError", "BoundExpr"] {
		assert!(
			!rune.contains(forbidden),
			"Rune engine gained local schema/binder marker: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr080_no_join_surface_in_named_projection_cut() {
	let rune = rune_engine_source();

	assert!(!rune.contains("LogicalPlan::Join"));
	assert!(!rune.contains("JoinKind"));
	assert!(!rune.contains("JoinStrategy"));
	assert!(!rune.contains("pub fn join"));
}

#[test]
pub(crate) fn adr080_no_sql_planner_grammar_or_subquery_expansion() {
	adr079_sqlparser_ast_remains_quarantined_to_sql_planner();
	let rune = rune_engine_source();
	let planner = sql_planner_source();

	assert!(!rune.contains("sqlparser"));
	assert!(!rune.contains("Subquery"));
	assert!(!planner.contains("RuneNamedExpr"));
	assert!(!planner.contains("pub fn named"));
}

#[test]
pub(crate) fn adr080_no_new_wendelon_wire_worker_storage_pbc_opcode_or_dependency() {
	adr079_no_new_wendelon_wire_storage_pbc_opcode_or_dependency();
	let rune = rune_engine_source();

	for forbidden in ["wendelon", "Storage", "ScanStream", "Pbc", "PBC"] {
		assert!(
			!rune.contains(forbidden),
			"ADR080 Rune implementation touched cross-ring/storage marker: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr080_rune_sandbox_allow_list_remains_toron_authoring_only() {
	adr079_rune_sandbox_allow_list_remains_in_force();
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");

	assert!(module.contains("m.ty::<RuneNamedExpr>()?;"));
	assert!(
		module.contains("m.associated_function(\"project_named\", RuneDataFrame::project_named)?;")
	);
	assert!(!module.contains("m.associated_function(\"select\","));
	assert!(!module.contains("m.associated_function(\"select_named\","));
	assert!(module.contains("m.associated_function(\"named\", RuneExpr::named)?;"));
	assert!(module.contains("m.function([\"count_all\"], count_all).build()?;"));
	assert!(!rune.contains("Context::with_default_modules"));
	assert!(rune.contains("install_minimal_primitives"));
}

#[test]
pub(crate) fn adr081_rune_frontend_does_not_rewrite_user_source() {
	let rune = rune_engine_source();
	let compile_script = source_block(&rune, "fn compile_script");

	assert!(!rune.contains("fn normalize_rune_named_select_surface"));
	assert!(!rune.contains("fn skip_rune_quoted"));
	assert!(!rune.contains("fn skip_rune_line_comment"));
	assert!(!rune.contains("fn skip_rune_block_comment"));
	assert!(!rune.contains("fn is_ascii_ident_continue"));
	assert!(!compile_script.contains("normalize_"));
	assert!(compile_script.contains("Source::new(\"script\", script)"));
}

#[test]
pub(crate) fn adr081_authored_rune_methods_avoid_reserved_keywords() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);

	for keyword in ["select", "match", "if", "else", "as", "is", "in", "not"] {
		assert!(
			!names.iter().any(|name| name == keyword),
			"Rune authoring surface registered keyword `{keyword}`: {names:?}"
		);
	}
	assert!(
		!names.iter().any(|name| name == "select_named"),
		"Rune authoring surface kept compatibility shim: {names:?}"
	);
	for expected in [
		"table",
		"sql",
		"filter",
		"project",
		"project_named",
		"aggregate",
		"count",
		"named",
		"gt",
		"eq",
		"when",
		"case",
	] {
		assert!(
			names.iter().any(|name| name == expected),
			"Rune authoring surface missing `{expected}`: {names:?}"
		);
	}
}

#[test]
pub(crate) fn adr082_rune_typed_literal_helpers_lower_to_existing_scalar_values() {
	let rune = rune_engine_source();

	for (function, scalar) in [
		("pub fn lit(val: i32)", "ScalarValue::Int32(Some(val))"),
		(
			"pub fn lit_str(value: String)",
			"ScalarValue::Utf8(Some(value))",
		),
		(
			"pub fn lit_f64(value: f64)",
			"ScalarValue::Float64(Some(value))",
		),
		(
			"pub fn lit_bool(value: bool)",
			"ScalarValue::Boolean(Some(value))",
		),
		(
			"pub fn lit_i64(value: i64)",
			"ScalarValue::Int64(Some(value))",
		),
	] {
		let body = source_block(&rune, function);
		assert!(
			body.contains("Expr::Literal"),
			"{function} must build Expr::Literal"
		);
		assert!(body.contains(scalar), "{function} missing {scalar}");
	}
}

#[test]
pub(crate) fn adr082_rune_typed_literal_helpers_are_registered_in_toron_module() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");

	for registration in [
		"m.function([\"lit\"], lit).build()?;",
		"m.function([\"lit_str\"], lit_str).build()?;",
		"m.function([\"lit_f64\"], lit_f64).build()?;",
		"m.function([\"lit_bool\"], lit_bool).build()?;",
		"m.function([\"lit_i64\"], lit_i64).build()?;",
	] {
		assert!(module.contains(registration), "missing {registration}");
	}
}

#[test]
pub(crate) fn adr082_typed_literal_names_avoid_rune_keywords() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);

	for literal_name in ["lit_str", "lit_f64", "lit_bool", "lit_i64"] {
		assert!(
			names.iter().any(|name| name == literal_name),
			"Rune typed literal surface missing `{literal_name}`: {names:?}"
		);
	}
	for keyword in ["as", "is", "not", "select", "match", "if", "else", "in"] {
		assert!(
			!names.iter().any(|name| name == keyword),
			"Rune authoring surface registered keyword `{keyword}`: {names:?}"
		);
	}
}

#[test]
pub(crate) fn adr082_typed_literal_cut_does_not_expand_operator_surface() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);

	for retained in [
		"eq",
		"gt",
		"lt",
		"lte",
		"gte",
		"ne",
		"and",
		"or",
		"logical_not",
		"is_null",
		"is_not_null",
		"add",
		"sub",
		"mul",
		"div",
		"cast",
		"try_cast",
		"in_list",
		"not_in_list",
		"like",
		"not_like",
		"ilike",
		"not_ilike",
	] {
		assert!(
			names.iter().any(|name| name == retained),
			"post-ADR086 Rune expression method `{retained}` missing: {names:?}"
		);
	}
	for deferred in ["not", "safe_cast"] {
		assert!(
			!names.iter().any(|name| name == deferred),
			"post-ADR086 Rune surface must keep deferred operator `{deferred}` out: {names:?}"
		);
	}
}

#[test]
pub(crate) fn adr082_typed_literals_do_not_change_ir_binder_executor_or_sandbox() {
	adr081_rune_frontend_does_not_rewrite_user_source();
	let rune = rune_engine_source();
	let ir = ir_source();
	let binder = binder_source();
	let exec = exec_source();

	for forbidden in [
		"BindingError",
		"BoundExpr",
		"ExecutionPlan",
		"PlannerConfig",
	] {
		assert!(
			!rune.contains(forbidden),
			"Rune typed literal surface gained planner/binder marker {forbidden}"
		);
	}
	assert!(!rune.contains("normalize_"));
	assert!(!rune.contains("evaluate_expr"));
	assert!(ir.contains("Utf8(Option<String>)"));
	assert!(ir.contains("Float64(Option<f64>)"));
	assert!(ir.contains("Boolean(Option<bool>)"));
	assert!(ir.contains("Int64(Option<i64>)"));
	assert!(binder.contains("Expr::Literal(value) => Ok(builder.append_literal(value.clone()))"));
	assert!(exec.contains("ScalarValue::Boolean(v)"));
	assert!(exec.contains("ScalarValue::Float64(v)"));
	assert!(exec.contains("ScalarValue::Int64(v)"));
}

#[test]
pub(crate) fn adr083_rune_predicate_operators_lower_to_existing_expr_variants() {
	let rune = rune_engine_source();

	for (function, variant) in [
		("pub fn lt", "Expr::Lt"),
		("pub fn lte", "Expr::Lte"),
		("pub fn gte", "Expr::Gte"),
		("pub fn ne(self", "Expr::Neq"),
		("pub fn and", "Expr::And"),
		("pub fn or", "Expr::Or"),
		("pub fn logical_not", "Expr::Not"),
		("pub fn is_null", "Expr::IsNull"),
		("pub fn is_not_null", "Expr::IsNotNull"),
	] {
		let body = source_block(&rune, function);
		assert!(body.contains(variant), "{function} missing {variant}");
		assert!(
			!body.contains("Expr::Add")
				&& !body.contains("Expr::Sub")
				&& !body.contains("Expr::Mul")
				&& !body.contains("Expr::Div"),
			"{function} must not introduce arithmetic"
		);
	}
}

#[test]
pub(crate) fn adr084_rune_arithmetic_methods_lower_to_existing_expr_variants() {
	let rune = rune_engine_source();

	for (function, variant) in [
		("pub fn add", "Expr::Add"),
		("pub fn sub", "Expr::Sub"),
		("pub fn mul", "Expr::Mul"),
		("pub fn div", "Expr::Div"),
	] {
		let body = source_block(&rune, function);
		assert!(body.contains(variant), "{function} missing {variant}");
		assert!(
			body.contains("Box::new(self.expr)") && body.contains("Box::new(other.expr)"),
			"{function} must lower both operands into existing Expr variants"
		);
		for forbidden in ["Expr::Cast", "Expr::TryCast", "BindingError", "BoundExpr"] {
			assert!(
				!body.contains(forbidden),
				"{function} must not introduce substrate marker {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr084_rune_arithmetic_methods_are_registered() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");

	for registration in [
		"m.associated_function(\"add\", RuneExpr::add)?;",
		"m.associated_function(\"sub\", RuneExpr::sub)?;",
		"m.associated_function(\"mul\", RuneExpr::mul)?;",
		"m.associated_function(\"div\", RuneExpr::div)?;",
	] {
		assert!(module.contains(registration), "missing {registration}");
	}
	for forbidden in [
		"m.associated_function(\"mod\"",
		"m.associated_function(\"neg\"",
	] {
		assert!(
			!module.contains(forbidden),
			"ADR084 registered deferred arithmetic-adjacent surface {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr085_rune_datatype_is_host_surface_only() {
	let manifest_dir = manifest_dir();
	let rune = rune_engine_source();
	assert!(rune.contains("pub struct RuneDataType"));
	assert!(rune.contains("data_type: DataType"));

	let offenders = source_files(&manifest_dir.join("src"))
		.into_iter()
		.filter(|path| path.file_name().and_then(|name| name.to_str()) != Some("rune_engine.rs"))
		.filter_map(|path| {
			let source = std::fs::read_to_string(&path).expect("read production source");
			source.contains("RuneDataType").then(|| {
				path.strip_prefix(&manifest_dir)
					.unwrap_or(&path)
					.display()
					.to_string()
			})
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"RuneDataType leaked outside rune_engine.rs: {offenders:?}"
	);
}

#[test]
pub(crate) fn adr085_rune_cast_target_helpers_are_numeric_only() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);

	for (helper, data_type) in [
		("type_i32", "DataType::Int32"),
		("type_i64", "DataType::Int64"),
		("type_f32", "DataType::Float32"),
		("type_f64", "DataType::Float64"),
	] {
		assert!(
			names.iter().any(|name| name == helper),
			"Rune cast target helper `{helper}` missing: {names:?}"
		);
		let body = source_block(&rune, &format!("pub fn {helper}"));
		assert!(body.contains(data_type), "{helper} missing {data_type}");
	}
	for forbidden in [
		"type_utf8",
		"type_bool",
		"type_decimal",
		"type_date",
		"safe_cast",
		"rune_parse_data_type",
		"FromStr",
		"type_str",
	] {
		assert!(
			!rune.contains(forbidden),
			"Rune cast target surface gained forbidden marker {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr085_rune_cast_methods_lower_to_existing_expr_variants() {
	let rune = rune_engine_source();
	let cast = source_block(&rune, "pub fn cast");
	let try_cast = source_block(&rune, "pub fn try_cast");

	assert!(cast.contains("target: RuneDataType"));
	assert!(cast.contains("Expr::Cast(Box::new(self.expr), target.data_type)"));
	assert!(try_cast.contains("target: RuneDataType"));
	assert!(try_cast.contains("Expr::TryCast(Box::new(self.expr), target.data_type)"));
	for forbidden in [
		"BindingError",
		"BoundExpr",
		"ExecutionPlan",
		"PlannerConfig",
		"safe_cast",
		"rune_parse_data_type",
	] {
		assert!(
			!cast.contains(forbidden) && !try_cast.contains(forbidden),
			"Rune cast methods gained forbidden marker {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr085_rune_cast_methods_are_registered() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");

	for registration in [
		"m.ty::<RuneDataType>()?;",
		"m.function([\"type_i32\"], type_i32).build()?;",
		"m.function([\"type_i64\"], type_i64).build()?;",
		"m.function([\"type_f32\"], type_f32).build()?;",
		"m.function([\"type_f64\"], type_f64).build()?;",
		"m.associated_function(\"cast\", RuneExpr::cast)?;",
		"m.associated_function(\"try_cast\", RuneExpr::try_cast)?;",
	] {
		assert!(module.contains(registration), "missing {registration}");
	}
	assert!(!module.contains("safe_cast"));
}

#[test]
pub(crate) fn adr085_rune_cast_reuses_existing_substrate() {
	let rune = rune_engine_source();
	let ir = ir_source();
	let binder = binder_source();
	let exec = exec_source();

	for forbidden in [
		"BindingError",
		"BoundExpr",
		"ExecutionPlan",
		"PlannerConfig",
		"normalize_",
		"evaluate_expr",
	] {
		assert!(
			!rune.contains(forbidden),
			"Rune cast surface gained substrate marker {forbidden}"
		);
	}
	for variant in ["Cast(Box<Expr>, DataType)", "TryCast(Box<Expr>, DataType)"] {
		assert!(ir.contains(variant), "IR missing existing {variant}");
	}
	assert!(binder.contains("Expr::Cast(inner, target) =>"));
	assert!(binder.contains("bind_explicit_cast"));
	assert!(binder.contains("Expr::TryCast(inner, target) =>"));
	assert!(binder.contains("bind_try_cast"));
	assert!(exec.contains("Expr::Cast(inner, target)"));
	assert!(exec.contains("Expr::TryCast(inner, target)"));
}

#[test]
pub(crate) fn adr085_rune_cast_remains_pbc_ineligible() {
	let ir = ir_source();
	let pbc = pbc_source();
	let eligible_body = source_block(&ir, "pub fn is_pbc_pushdown_eligible");

	for cast_variant in ["Self::Cast(_, _)", "Self::TryCast(_, _)"] {
		assert!(
			eligible_body.contains(cast_variant),
			"is_pbc_pushdown_eligible missing cast decline {cast_variant}"
		);
	}
	assert!(eligible_body.contains("=> false"));
	assert!(pbc.contains("BoundExprNode::Cast { .. }"));
	assert!(pbc.contains("BoundExprNode::TryCast { .. }"));
	assert!(pbc.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
}

#[test]
pub(crate) fn adr086_rune_in_list_methods_lower_to_existing_expr_variant() {
	let rune = rune_engine_source();
	let in_list = source_block(&rune, "pub fn in_list");
	let not_in_list = source_block(&rune, "pub fn not_in_list");
	let build = source_block(&rune, "fn build_in_list");

	assert!(in_list.contains("list: Vec<RuneExpr>"));
	assert!(in_list.contains("VmResult<RuneExpr>"));
	assert!(in_list.contains("build_in_list(self.expr, list, false)"));
	assert!(not_in_list.contains("list: Vec<RuneExpr>"));
	assert!(not_in_list.contains("VmResult<RuneExpr>"));
	assert!(not_in_list.contains("build_in_list(self.expr, list, true)"));
	assert!(build.contains("Expr::InList"));
	assert!(build.contains("expr: Box::new(expr)"));
	assert!(build.contains("list.into_iter().map(|item| item.expr).collect()"));
	assert!(build.contains("negated"));
	for forbidden in [
		"BindingError",
		"BoundExpr",
		"ExecutionPlan",
		"PlannerConfig",
	] {
		assert!(
			!in_list.contains(forbidden)
				&& !not_in_list.contains(forbidden)
				&& !build.contains(forbidden),
			"Rune InList methods gained forbidden marker {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr086_rune_in_list_methods_validate_frontend_bounds() {
	let rune = rune_engine_source();
	let build = source_block(&rune, "fn build_in_list");

	assert!(rune.contains("const RUNE_MAX_IN_LIST_ITEMS: usize = 32;"));
	assert!(build.contains("if list.is_empty()"));
	assert!(build.contains("VmResult::panic(\"IN list must not be empty\")"));
	assert!(build.contains("if list.len() > RUNE_MAX_IN_LIST_ITEMS"));
	assert!(build.contains("VmResult::panic(format!"));
	assert!(build.contains("IN list exceeds {RUNE_MAX_IN_LIST_ITEMS} items"));
}

#[test]
pub(crate) fn adr086_rune_in_list_authoring_bounds_match_sql_planner() {
	fn const_usize(source: &str, name: &str) -> usize {
		let marker = format!("const {name}: usize = ");
		let rest = source
			.split(&marker)
			.nth(1)
			.unwrap_or_else(|| panic!("missing {name}"));
		rest.split(';')
			.next()
			.expect("constant terminator")
			.trim()
			.parse()
			.unwrap_or_else(|error| panic!("parse {name}: {error}"))
	}

	let rune = rune_engine_source();
	let sql_planner = sql_planner_source();
	let rune_max = const_usize(&rune, "RUNE_MAX_IN_LIST_ITEMS");
	let sql_max = const_usize(&sql_planner, "MAX_IN_LIST_ITEMS");

	assert_eq!(rune_max, 32);
	assert_eq!(rune_max, sql_max);
}

#[test]
pub(crate) fn adr086_rune_in_list_methods_are_registered() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);

	for registration in [
		"m.associated_function(\"in_list\", RuneExpr::in_list)?;",
		"m.associated_function(\"not_in_list\", RuneExpr::not_in_list)?;",
	] {
		assert!(module.contains(registration), "missing {registration}");
	}
	for forbidden in ["in", "not", "safe_cast"] {
		assert!(
			!names.iter().any(|name| name == forbidden),
			"Rune InList surface registered forbidden name `{forbidden}`: {names:?}"
		);
	}
}

#[test]
pub(crate) fn adr086_rune_in_list_pushes_when_pbc_eligible() {
	let tests = rune_relation_handles_test_source();

	assert!(tests.contains("rune_in_list_pushes_to_scan_pbc"));
	assert!(tests.contains("rune_not_in_list_pushes_to_scan_pbc"));
	assert!(tests.contains("FilterDisposition::FullyPushed"));
	assert!(tests.contains("scan_pbc(scan).expect(\"in_list must reach scan PBC\")"));
	assert!(tests.contains("scan_pbc(scan).expect(\"not_in_list must reach scan PBC\")"));
	assert!(tests.contains("OP_IN_LIST"));
	assert!(tests.contains("OP_NOT"));
}

#[test]
pub(crate) fn adr086_rune_in_list_reuses_existing_substrate() {
	let rune = rune_engine_source();
	let ir = ir_source();
	let binder = binder_source();
	let exec = exec_source();
	let pbc = pbc_source();

	for forbidden in [
		"BindingError",
		"BoundExpr",
		"ExecutionPlan",
		"PlannerConfig",
		"normalize_",
		"evaluate_expr",
		"OP_IN_LIST",
		"OP_NOT",
	] {
		assert!(
			!rune.contains(forbidden),
			"Rune InList surface gained substrate marker {forbidden}"
		);
	}
	assert!(ir.contains("InList {\n\t\texpr: Box<Expr>,"));
	assert!(ir.contains("list: Vec<Expr>"));
	assert!(ir.contains("negated: bool"));
	assert!(binder.contains("Expr::InList { expr, list, .. }"));
	assert!(binder.contains("fn bind_in_list"));
	assert!(exec.contains("Expr::InList"));
	assert!(exec.contains("fn evaluate_in_list"));
	assert!(pbc.contains("BoundExprNode::InList"));
	assert!(pbc.contains("bytecode.push(OP_IN_LIST)"));
	assert!(pbc.contains("bytecode.push(OP_NOT)"));
}

#[test]
pub(crate) fn adr101_rune_like_ilike_methods_are_registered() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);
	let build = source_block(&rune, "fn build_like");

	for (function, negated, case_insensitive) in [
		("pub fn like", "false", "false"),
		("pub fn not_like", "true", "false"),
		("pub fn ilike", "false", "true"),
		("pub fn not_ilike", "true", "true"),
	] {
		let body = source_block(&rune, function);
		assert!(body.contains("pattern: String"));
		assert!(body.contains("-> RuneExpr"));
		assert!(
			body.contains(&format!(
				"build_like(self.expr, pattern, {negated}, {case_insensitive})"
			)),
			"{function} must delegate to build_like with the expected flags"
		);
	}
	assert!(build.contains("Expr::Like"));
	assert!(build.contains("expr: Box::new(expr)"));
	assert!(build.contains("pattern"));
	assert!(build.contains("negated"));
	assert!(build.contains("case_insensitive"));

	for registration in [
		"m.associated_function(\"like\", RuneExpr::like)?;",
		"m.associated_function(\"not_like\", RuneExpr::not_like)?;",
		"m.associated_function(\"ilike\", RuneExpr::ilike)?;",
		"m.associated_function(\"not_ilike\", RuneExpr::not_ilike)?;",
	] {
		assert!(module.contains(registration), "missing {registration}");
	}
	for method in ["like", "not_like", "ilike", "not_ilike"] {
		assert!(
			names.iter().any(|name| name == method),
			"ADR101 Rune method `{method}` missing: {names:?}"
		);
	}
}

#[test]
pub(crate) fn adr101_rune_like_methods_are_infallible() {
	let rune = rune_engine_source();
	let build_like = source_block(&rune, "fn build_like");

	for function in [
		"pub fn like",
		"pub fn not_like",
		"pub fn ilike",
		"pub fn not_ilike",
	] {
		let body = source_block(&rune, function);
		assert!(!body.contains("VmResult"));
		assert!(!body.contains("panic"));
		assert!(!body.contains("is_empty"));
	}
	assert!(!build_like.contains("VmResult"));
	assert!(!build_like.contains("panic"));
	assert!(!build_like.contains("is_empty"));
	assert!(source_block(&rune, "fn build_in_list").contains("VmResult::panic"));
}

#[test]
pub(crate) fn adr101_stale_surface_guards_are_retargeted() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);

	for admitted in ["like", "not_like", "ilike", "not_ilike", "when", "case"] {
		assert!(
			names.iter().any(|name| name == admitted),
			"ADR101 admitted method `{admitted}` missing: {names:?}"
		);
	}
	for deferred in ["not", "safe_cast", "match", "if", "else", "as", "is", "in"] {
		assert!(
			!names.iter().any(|name| name == deferred),
			"ADR101 must keep deferred method `{deferred}` out: {names:?}"
		);
	}
}

#[test]
pub(crate) fn adr101_reuses_075_like_policy() {
	let rune = rune_engine_source();
	let planner = sql_planner_source();
	let binder = binder_source();
	let exec = exec_source();

	assert!(planner.contains("fn lower_like"));
	assert!(planner.contains("LIKE ANY"));
	assert!(planner.contains("LIKE ESCAPE is not supported"));
	assert!(binder.contains("fn bind_like"));
	assert!(binder.contains("LIKE expects Utf8 input, got {expr_type:?}"));
	assert!(exec.contains("fn evaluate_like"));
	for forbidden in [
		"LIKE ANY",
		"ESCAPE",
		"SIMILAR TO",
		"RLIKE",
		"REGEXP",
		"regex",
	] {
		assert!(
			!rune.contains(forbidden),
			"Rune LIKE surface introduced forbidden policy marker {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr101_like_remains_residual_only() {
	let pbc = pbc_source();

	assert!(pbc.contains("BoundExprNode::Like { .. }"));
	assert!(pbc.contains("ExpressionShapeUnsupported"));
	assert!(!pbc.contains("OP_LIKE"));
	assert!(!pbc.contains("OP_ILIKE"));
	assert!(!pbc.contains("bytecode.push(OP_LIKE)"));
}

#[test]
pub(crate) fn adr101_no_cross_ring_surface() {
	let manifest_dir = manifest_dir();
	let rune = rune_engine_source();

	for forbidden in ["wendelon", "Storage", "ScanStream", "Pbc", "PBC"] {
		assert!(
			!rune.contains(forbidden),
			"ADR101 Rune LIKE surface touched cross-ring/storage marker: {forbidden}"
		);
	}
	for source_file in ["src/storage.rs", "src/wire.rs", "src/sql_wire.rs"] {
		let source = std::fs::read_to_string(manifest_dir.join(source_file))
			.unwrap_or_else(|_| panic!("read {source_file}"));
		for marker in ["not_like", "not_ilike", "RuneExpr"] {
			assert!(
				!source.contains(marker),
				"ADR101 marker {marker} leaked to {source_file}"
			);
		}
	}
	let wendelon_src = manifest_dir
		.parent()
		.expect("workspace crates dir")
		.join("wendelon/src");
	let offenders = source_files(&wendelon_src)
		.into_iter()
		.filter_map(|path| {
			let source = std::fs::read_to_string(&path).expect("read Wendelon source");
			(source.contains("not_like")
				|| source.contains("not_ilike")
				|| source.contains("RuneExpr"))
			.then(|| {
				path.strip_prefix(manifest_dir.parent().expect("workspace crates dir"))
					.unwrap_or(&path)
					.display()
					.to_string()
			})
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR101 Rune LIKE surface leaked to Wendelon: {offenders:?}"
	);
}

#[test]
pub(crate) fn adr111_rune_case_lowers_to_existing_expr_case() {
	let rune = rune_engine_source();
	let ir = ir_source();
	let binder = binder_source();
	let exec = exec_source();
	let pbc = pbc_source();
	let tests = rune_relation_handles_test_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);
	let carrier = source_block(&rune, "pub struct RuneWhenThen");
	let when = source_block(&rune, "pub fn when");
	let case = source_block(&rune, "pub fn case");
	let case_surface = format!("{carrier}\n{when}\n{case}");

	assert!(rune.contains("#[derive(Any, Debug, Clone)]\npub struct RuneWhenThen"));
	assert!(carrier.contains("condition: Expr"));
	assert!(carrier.contains("result: Expr"));

	assert!(when.contains("condition: RuneExpr"));
	assert!(when.contains("result: RuneExpr"));
	assert!(when.contains("-> RuneWhenThen"));
	assert!(when.contains("condition: condition.expr"));
	assert!(when.contains("result: result.expr"));

	assert!(case.contains("branches: Vec<RuneWhenThen>"));
	assert!(case.contains("else_expr: RuneExpr"));
	assert!(case.contains("-> VmResult<RuneExpr>"));
	assert!(case.contains("branches.is_empty()"));
	assert!(case.contains("VmResult::panic(\"CASE requires at least one WHEN branch\")"));
	assert!(case.contains("Expr::Case"));
	assert!(case.contains("when_then"));
	assert!(case.contains("else_expr: Some(Box::new(else_expr.expr))"));

	for registration in [
		"m.ty::<RuneWhenThen>()?;",
		"m.function([\"when\"], when).build()?;",
		"m.function([\"case\"], case).build()?;",
	] {
		assert!(module.contains(registration), "missing {registration}");
	}
	for admitted in ["when", "case"] {
		assert!(
			names.iter().any(|name| name == admitted),
			"ADR111 Rune function `{admitted}` missing: {names:?}"
		);
	}
	for forbidden in [
		"select",
		"match",
		"if",
		"else",
		"as",
		"is",
		"in",
		"not",
		"safe_cast",
	] {
		assert!(
			!names.iter().any(|name| name == forbidden),
			"ADR111 Rune CASE surface registered forbidden name `{forbidden}`: {names:?}"
		);
	}

	for forbidden in [
		"BindingError",
		"BoundExpr",
		"ExecutionPlan",
		"PushdownDeclineReason",
		"OP_CASE",
		"PBC",
		"CASE is not PBC-eligible",
	] {
		assert!(
			!case_surface.contains(forbidden),
			"Rune CASE authoring surface gained substrate marker {forbidden}"
		);
	}

	assert!(ir.contains("Case {\n\t\twhen_then: Vec<(Box<Expr>, Box<Expr>)>"));
	assert!(binder.contains("fn bind_case"));
	assert!(binder.contains("require_boolean(builder, \"CASE WHEN\", condition)"));
	assert!(exec.contains("fn evaluate_case"));
	assert!(pbc.contains("BoundExprNode::Case"));
	assert!(!pbc.contains("OP_CASE"));
	assert!(pbc.contains("CASE is not PBC-eligible"));

	for behavior in [
		"rune_case_projection_executes",
		"rune_case_numeric_widening_stays_naked",
		"rune_case_empty_branches_reject",
		"rune_case_type_errors_inherited",
		"rune_case_filter_is_residual_and_keeps_pushable_conjunct",
		"rune_case_eager_eval_masked_error_fails",
		"rune_case_carries_logical_identity",
		"rune_case_mixed_identities_reject",
		"rune_case_identity_widening_rejects",
	] {
		assert!(tests.contains(behavior), "missing behavior gate {behavior}");
	}
}

#[test]
pub(crate) fn adr084_rune_arithmetic_reuses_adr024_substrate() {
	let rune = rune_engine_source();
	let ir = ir_source();
	let binder = binder_source();
	let exec = exec_source();
	let pbc = pbc_source();

	for forbidden in [
		"BindingError",
		"BoundExpr",
		"ExecutionPlan",
		"PlannerConfig",
		"normalize_",
		"evaluate_expr",
	] {
		assert!(
			!rune.contains(forbidden),
			"Rune arithmetic surface gained substrate marker {forbidden}"
		);
	}
	for variant in [
		"Add(Box<Expr>, Box<Expr>)",
		"Sub(Box<Expr>, Box<Expr>)",
		"Mul(Box<Expr>, Box<Expr>)",
		"Div(Box<Expr>, Box<Expr>)",
	] {
		assert!(ir.contains(variant), "IR missing existing {variant}");
	}
	for (variant, op) in [
		("Expr::Add(left, right)", "BoundBinaryOp::Add"),
		("Expr::Sub(left, right)", "BoundBinaryOp::Sub"),
		("Expr::Mul(left, right)", "BoundBinaryOp::Mul"),
		("Expr::Div(left, right)", "BoundBinaryOp::Div"),
	] {
		assert!(binder.contains(variant), "binder missing {variant}");
		assert!(binder.contains(op), "binder missing {op}");
	}
	assert!(binder.contains("fn bind_arithmetic"));
	assert!(binder.contains("arithmetic_output_type(&left_type, &right_type)"));
	for exec_arm in [
		"Expr::Add(left, right)",
		"Expr::Sub(left, right)",
		"Expr::Mul(left, right)",
		"Expr::Div(left, right)",
	] {
		assert!(exec.contains(exec_arm), "executor missing {exec_arm}");
	}
	assert!(exec.contains("fn evaluate_unbound_arithmetic"));
	assert!(exec.contains("arithmetic_output_type(left.data_type(), right.data_type())"));
	assert!(pbc.contains("BoundBinaryOp::Add"));
}

#[test]
pub(crate) fn adr084_rune_arithmetic_remains_pbc_ineligible() {
	let ir = ir_source();
	let pbc = pbc_source();
	let eligible_body = source_block(&ir, "pub fn is_pbc_pushdown_eligible");

	for arithmetic in [
		"Self::Add(_, _)",
		"Self::Sub(_, _)",
		"Self::Mul(_, _)",
		"Self::Div(_, _)",
	] {
		assert!(
			eligible_body.contains(arithmetic),
			"is_pbc_pushdown_eligible missing arithmetic decline {arithmetic}"
		);
	}
	assert!(eligible_body.contains("=> false"));
	assert!(pbc.contains("BoundBinaryOp::Add"));
	assert!(pbc.contains("BoundBinaryOp::Sub"));
	assert!(pbc.contains("BoundBinaryOp::Mul"));
	assert!(pbc.contains("BoundBinaryOp::Div"));
	assert!(pbc.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
}

#[test]
pub(crate) fn adr083_rune_predicate_operators_are_registered() {
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");

	for registration in [
		"m.associated_function(\"lt\", RuneExpr::lt)?;",
		"m.associated_function(\"lte\", RuneExpr::lte)?;",
		"m.associated_function(\"gte\", RuneExpr::gte)?;",
		"m.associated_function(\"ne\", RuneExpr::ne)?;",
		"m.associated_function(\"and\", RuneExpr::and)?;",
		"m.associated_function(\"or\", RuneExpr::or)?;",
		"m.associated_function(\"logical_not\", RuneExpr::logical_not)?;",
		"m.associated_function(\"is_null\", RuneExpr::is_null)?;",
		"m.associated_function(\"is_not_null\", RuneExpr::is_not_null)?;",
	] {
		assert!(module.contains(registration), "missing {registration}");
	}
	assert!(!module.contains("m.associated_function(\"not\","));
}

#[test]
pub(crate) fn adr083_logical_not_avoids_keyword_collision_with_not() {
	adr081_authored_rune_methods_avoid_reserved_keywords();
	let rune = rune_engine_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let names = registered_rune_function_names(&module);

	assert!(
		names.iter().any(|name| name == "logical_not"),
		"Rune negation method must be logical_not: {names:?}"
	);
	for keyword in ["not", "is", "select", "as"] {
		assert!(
			!names.iter().any(|name| name == keyword),
			"Rune authoring surface registered keyword `{keyword}`: {names:?}"
		);
	}
}

#[test]
pub(crate) fn adr083_predicate_operator_cut_does_not_change_engine_substrate() {
	let rune = rune_engine_source();
	let ir = ir_source();
	let binder = binder_source();
	let exec = exec_source();
	let pbc = pbc_source();

	for forbidden in [
		"BindingError",
		"BoundExpr",
		"ExecutionPlan",
		"PlannerConfig",
		"normalize_",
		"evaluate_expr",
	] {
		assert!(
			!rune.contains(forbidden),
			"Rune predicate operator surface gained substrate marker {forbidden}"
		);
	}
	for variant in [
		"Neq(Box<Expr>, Box<Expr>)",
		"Lt(Box<Expr>, Box<Expr>)",
		"Lte(Box<Expr>, Box<Expr>)",
		"Gte(Box<Expr>, Box<Expr>)",
		"And(Box<Expr>, Box<Expr>)",
		"Or(Box<Expr>, Box<Expr>)",
		"Not(Box<Expr>)",
		"IsNull(Box<Expr>)",
		"IsNotNull(Box<Expr>)",
	] {
		assert!(ir.contains(variant), "IR missing existing {variant}");
	}
	for (variant, op) in [
		("Expr::Neq(left, right)", "BoundBinaryOp::Neq"),
		("Expr::Lt(left, right)", "BoundBinaryOp::Lt"),
		("Expr::Lte(left, right)", "BoundBinaryOp::Lte"),
		("Expr::Gte(left, right)", "BoundBinaryOp::Gte"),
		("Expr::And(left, right)", "BoundBinaryOp::And"),
		("Expr::Or(left, right)", "BoundBinaryOp::Or"),
	] {
		assert!(binder.contains(variant), "binder missing {variant}");
		assert!(binder.contains(op), "binder missing {op}");
	}
	for binder_arm in [
		"Expr::Not(inner) =>",
		"Expr::IsNull(inner) =>",
		"Expr::IsNotNull(inner) =>",
	] {
		assert!(binder.contains(binder_arm), "binder missing {binder_arm}");
	}
	for exec_arm in [
		"Expr::Neq(left, right)",
		"Expr::Lt(left, right)",
		"Expr::Lte(left, right)",
		"Expr::Gte(left, right)",
		"Expr::And(left, right)",
		"Expr::Or(left, right)",
		"Expr::Not(inner)",
		"Expr::IsNull(inner)",
		"Expr::IsNotNull(inner)",
	] {
		assert!(exec.contains(exec_arm), "executor missing {exec_arm}");
	}
	for pbc_mapping in [
		"BoundBinaryOp::Neq => Ok(OP_NEQ)",
		"BoundBinaryOp::Lt => Ok(OP_LT)",
		"BoundBinaryOp::Lte => Ok(OP_LTE)",
		"BoundBinaryOp::Gte => Ok(OP_GTE)",
		"BoundBinaryOp::And => Ok(OP_AND)",
		"BoundBinaryOp::Or => Ok(OP_OR)",
		"BoundUnaryOp::Not => OP_NOT",
		"BoundUnaryOp::IsNull => OP_IS_NULL",
		"BoundUnaryOp::IsNotNull => OP_IS_NOT_NULL",
	] {
		assert!(
			pbc.contains(pbc_mapping),
			"PBC compiler missing {pbc_mapping}"
		);
	}
}
