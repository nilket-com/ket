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
