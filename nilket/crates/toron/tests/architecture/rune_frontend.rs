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
pub(crate) fn adr080_named_select_lowers_to_existing_project_output_names() {
	let rune = rune_engine_source();
	let select = source_block(&rune, "pub fn select");
	let normalize = source_block(&rune, "fn normalize_rune_named_select_surface");
	let compile_script = source_block(&rune, "fn compile_script");

	assert!(select.contains("exprs: Vec<RuneNamedExpr>"));
	assert!(select.contains("LogicalPlan::Project"));
	assert!(select.contains("output_names.push(Some(named.name))"));
	assert!(select.contains("plan_exprs.push(named.expr)"));
	assert!(!select.contains("LogicalPlan::Join"));
	assert!(!select.contains("Aggregate"));
	assert!(normalize.contains("const FROM: &str = \".select\";"));
	assert!(normalize.contains("const TO: &str = \".select_named\";"));
	assert!(compile_script.contains("normalize_rune_named_select_surface(script)"));
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

	assert!(tests.contains("rune_named_sql_multi_hop_continuation_binds_and_executes"));
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
	assert!(module.contains("m.associated_function(\"select\", RuneDataFrame::select)?;"));
	assert!(module.contains("m.associated_function(\"select_named\", RuneDataFrame::select)?;"));
	assert!(module.contains("m.associated_function(\"named\", RuneExpr::named)?;"));
	assert!(module.contains("m.function([\"count_all\"], count_all).build()?;"));
	assert!(!rune.contains("Context::with_default_modules"));
	assert!(rune.contains("install_minimal_primitives"));
}
