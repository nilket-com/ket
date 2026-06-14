use super::support::*;

const ADR228_OPTIMIZER_READOUTS: [(&str, &str); 7] = [
	("optimizer_decisions", "optimizer_decisions"),
	("optimizer_runtime_verdicts", "optimizer_runtime_verdicts"),
	("optimizer_evidence_status", "optimizer_evidence_status"),
	("optimizer_margin_advisory", "optimizer_margin_advisory"),
	("optimizer_receipt_gates", "optimizer_receipt_gates"),
	(
		"optimizer_decision_conflicts",
		"optimizer_decision_conflicts",
	),
	("optimizer_memo_admission", "optimizer_memo_admission"),
];

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative)).expect("read Toron source")
}

fn rune_engine_source() -> String {
	source("src/rune_engine.rs")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

fn quoted_const_value(line: &str) -> Option<String> {
	line.split('=')
		.nth(1)
		.and_then(|value| value.split('"').nth(1))
		.map(ToOwned::to_owned)
}

fn binder_optimizer_readout_table_constants() -> Vec<String> {
	let mut tables = binder_root_source()
		.lines()
		.filter(|line| {
			line.contains("const INFORMATION_SCHEMA_OPTIMIZER_") && line.contains("_TABLE: &str")
		})
		.filter_map(quoted_const_value)
		.collect::<Vec<_>>();
	tables.sort();
	tables
}

fn rune_optimizer_readout_table_constants() -> Vec<String> {
	let mut tables = rune_engine_source()
		.lines()
		.filter(|line| line.contains("const RUNE_OPTIMIZER_") && line.contains("_TABLE: &str"))
		.filter_map(quoted_const_value)
		.collect::<Vec<_>>();
	tables.sort();
	tables
}

#[test]
pub(crate) fn adr228_rune_optimizer_readout_handles_are_finite_seven() {
	let rune = rune_engine_source();
	let context = source_block(&rune, "impl ToronContext");
	let module = source_block(&rune, "pub fn create_toron_module");

	assert_eq!(context.matches("pub fn optimizer_").count(), 7);
	assert_eq!(module.matches("\"optimizer_").count(), 7);
	for (handle, _) in ADR228_OPTIMIZER_READOUTS {
		assert!(
			context.contains(&format!("pub fn {handle}(&self) -> RuneDataFrame")),
			"missing ToronContext::{handle}"
		);
		assert!(
			module.contains(&format!(
				"m.associated_function(\"{handle}\", ToronContext::{handle})?;"
			)) || module.contains(&format!("\"{handle}\",\n\t\tToronContext::{handle},")),
			"missing Rune module registration for {handle}"
		);
	}
}

#[test]
pub(crate) fn adr228_rune_optimizer_handles_lower_to_schema_qualified_scan_named() {
	let rune = rune_engine_source();
	let helper = source_block(&rune, "fn optimizer_readout");

	assert!(helper.contains("LogicalPlan::ScanNamed"));
	assert!(helper.contains("TableRef::with_schema(RUNE_INFORMATION_SCHEMA_NAMESPACE, table)"));
	assert!(helper.contains("projection: None"));
	assert!(
		rune.contains("const RUNE_INFORMATION_SCHEMA_NAMESPACE: &str = \"information_schema\"")
	);
	for (_, table) in ADR228_OPTIMIZER_READOUTS {
		assert!(
			helper.contains("RUNE_OPTIMIZER_READOUT_TABLES.contains(&table)")
				&& rune.contains(&format!("\"{table}\"")),
			"optimizer readout table {table} is not lowered through the finite manifest"
		);
	}
}

#[test]
pub(crate) fn adr228_rune_optimizer_handles_do_not_use_sql_parser() {
	let rune = rune_engine_source();
	let helper = source_block(&rune, "fn optimizer_readout");

	for forbidden in [
		"SqlPlanner",
		"plan_relation",
		"Parser::parse_sql",
		"parse_one_statement",
		"bind(",
		"ExecutionPlan",
		"Catalog",
	] {
		assert!(
			!helper.contains(forbidden),
			"optimizer readout helper used forbidden parser/binder marker {forbidden}"
		);
	}
	for (handle, _) in ADR228_OPTIMIZER_READOUTS {
		let method = source_block(&rune, &format!("pub fn {handle}"));
		assert!(
			method.contains("Self::optimizer_readout("),
			"{handle} must only delegate to optimizer_readout"
		);
		for forbidden in ["SqlPlanner", "plan_relation", "bind(", "ExecutionPlan"] {
			assert!(
				!method.contains(forbidden),
				"{handle} used forbidden parser/binder marker {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr228_rune_engine_stays_lazy_no_binder_executor_config() {
	super::rune_frontend::adr079_rune_dataframe_remains_lazy_logical_plan_handle();
	super::rune_frontend::adr079_no_rune_local_optimizer_binder_or_executor();
	let rune = rune_engine_source();
	let helper = source_block(&rune, "fn optimizer_readout");

	for forbidden in [
		"PlannerConfig",
		"Executor",
		"ExecutionPlan",
		"RecordBatch",
		"ScanStream",
	] {
		assert!(
			!helper.contains(forbidden),
			"optimizer readout helper materialized or configured planning: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr228_rune_optimizer_handles_mirror_table_constants_no_drift() {
	let expected = ADR228_OPTIMIZER_READOUTS
		.iter()
		.map(|(_, table)| (*table).to_string())
		.collect::<Vec<_>>();
	let mut expected_sorted = expected;
	expected_sorted.sort();

	assert_eq!(rune_optimizer_readout_table_constants(), expected_sorted);
	assert_eq!(binder_optimizer_readout_table_constants(), expected_sorted);
}

#[test]
pub(crate) fn adr228_no_new_information_schema_table_or_system_table_kind() {
	let execution = source("src/ir/execution.rs");
	let system_kind = source_block(&execution, "pub enum SystemTableKind");
	let binder_tables = binder_optimizer_readout_table_constants();

	assert_eq!(enum_variant_count(&system_kind), 12);
	assert_eq!(binder_tables.len(), 7);
	for (_, table) in ADR228_OPTIMIZER_READOUTS {
		assert!(
			binder_tables.iter().any(|actual| actual == table),
			"binder readout table constants missing {table}"
		);
	}
}

#[test]
pub(crate) fn adr228_no_runtime_evidence_or_policy_surface_change() {
	let rune = rune_engine_source();
	let helper = source_block(&rune, "fn optimizer_readout");

	for forbidden in [
		"runtime_evidence",
		"OptimizerRuntimeEvidence",
		"OptimizerDecisionKind",
		"OptimizerRuntimeVerdict",
		"JoinOrderSoftMarginApplication",
		"receipt_gate_snapshot",
		"record_decision",
		"record_verdict",
	] {
		assert!(
			!helper.contains(forbidden),
			"optimizer readout helper touched evidence/policy marker {forbidden}"
		);
	}
	for source_file in [
		"src/binder/runtime_evidence.rs",
		"src/network/observability.rs",
		"src/coordinator.rs",
		"src/daemon.rs",
		"src/distributed/fragment.rs",
		"src/distributed/scheduler.rs",
		"src/exec.rs",
		"src/wire.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(source_file))
			.unwrap_or_else(|_| panic!("read {source_file}"));
		assert!(
			!source.contains("optimizer_readout"),
			"ADR228 readout handle leaked into {source_file}"
		);
	}
}

#[test]
pub(crate) fn adr228_ctx_table_remains_one_part_and_ctx_sql_path_retained() {
	let rune = rune_engine_source();
	let table = source_block(&rune, "pub fn table");
	let sql = source_block(&rune, "pub fn sql");
	let tests = binder_source();

	assert!(table.contains("name: String"));
	assert!(table.contains("RuneDataFrame::new(name)"));
	assert!(!table.contains("with_schema"));
	assert!(sql.contains("SqlPlanner::new().plan_relation(&query)"));
	assert!(rune.contains("m.associated_function(\"sql\", ToronContext::sql)?;"));
	assert!(tests.contains("ctx_table_remains_one_part_user_table_handle"));
}
