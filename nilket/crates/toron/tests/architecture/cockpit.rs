use super::support::*;

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative)).expect("read Toron source")
}

fn first_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing block for {marker}"))
		.1
}

fn filter_arm(source: &str) -> String {
	source
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Project").next())
		.expect("Filter bind arm")
		.to_string()
}

fn optimizer_explain_plan_region(source: &str) -> String {
	source
		.split("fn optimizer_explain_plan")
		.nth(1)
		.and_then(|rest| rest.split("fn optimizer_explain_row").next())
		.expect("optimizer_explain_plan region")
		.to_string()
}

fn source_lines_for_path(path: &Path) -> Vec<(PathBuf, usize, String)> {
	if path.is_dir() {
		return source_lines(path);
	}
	std::fs::read_to_string(path)
		.expect("read source")
		.lines()
		.enumerate()
		.map(|(idx, line)| (path.to_path_buf(), idx + 1, line.to_string()))
		.collect()
}

#[test]
pub(crate) fn adr174_optimizer_trace_carries_scan_filter_receipts() {
	let binder = source("src/binder.rs");
	let trace = first_block(&binder, "struct OptimizerTrace");
	let filter = filter_arm(&binder);

	assert!(binder.contains("pub(crate) struct ScanFilterReceipt"));
	assert!(binder.contains("pub(crate) enum ScanFilterOutcome"));
	assert!(binder.contains("pub(crate) enum FilterEstimateSource"));
	assert!(trace.contains("scan_filter_decisions: Vec<ScanFilterReceipt>"));
	assert!(filter.contains("filter_equality_ndv_estimate_evidence"));
	assert!(filter.contains("filter_range_selectivity_estimate_evidence"));
	assert!(filter.contains("plan_filter_with_predicate_pushdown_evidence("));
	assert!(filter.contains("trace.scan_filter_decisions.push(ScanFilterReceipt"));
	assert!(filter.contains("scan_index = trace.scan_filter_decisions.len()"));
}

#[test]
pub(crate) fn adr174_scan_filter_receipt_is_observational() {
	let binder = source("src/binder.rs");
	for marker in [
		"fn predicate_pushdown_plan",
		"fn plan_filter_with_predicate_pushdown_evidence",
		"fn filter_equality_ndv_estimate_evidence",
		"fn filter_range_selectivity_estimate_evidence",
		"fn choose_join_strategy",
	] {
		let block = first_block(&binder, marker);
		assert!(
			!block.contains("scan_filter_decisions"),
			"{marker} must not read scan/filter receipts"
		);
	}
	let explain = optimizer_explain_plan_region(&binder);
	assert!(explain.contains("trace"));
	assert!(explain.contains(".scan_filter_decisions"));
}

#[test]
pub(crate) fn adr174_pushdown_helper_does_not_mutate_trace() {
	let binder = source("src/binder.rs");
	let helper = first_block(&binder, "fn plan_filter_with_predicate_pushdown_evidence");

	assert!(helper.contains("ScanFilterPushdownEvidence"));
	assert!(!helper.contains("OptimizerTrace"));
	assert!(!helper.contains("trace."));
	assert!(helper.contains("static_filter_class(&input, &original_predicate, logical_types)"));
	assert!(helper.contains("predicate_pushdown_plan(predicate, &input, logical_types)"));
}

#[test]
pub(crate) fn adr174_scan_filter_receipt_is_cheap() {
	let binder = source("src/binder.rs");
	let receipt = first_block(&binder, "struct ScanFilterReceipt");
	let target = first_block(&binder, "struct ScanTraceTarget");
	let source = first_block(&binder, "enum FilterEstimateSource");
	let outcome = first_block(&binder, "enum ScanFilterOutcome");

	for block in [&receipt, &target, &source, &outcome] {
		assert!(!block.contains("String"));
		assert!(!block.contains("BoundExprProgram"));
		assert!(!block.contains("PbcProgram"));
	}
	assert!(binder.contains("#[derive(Debug, Clone, Copy, PartialEq, Eq)]\npub(crate) struct ScanFilterReceipt"));
	assert!(receipt.contains("estimated_rows: Option<RowCount>"));
	assert!(receipt.contains("spent_column: Option<ColumnOrdinal>"));
}

#[test]
pub(crate) fn adr174_explain_schema_is_unified_decision_kind() {
	let binder = source("src/binder.rs");
	let schema = first_block(&binder, "fn optimizer_explain_schema");
	let plan = optimizer_explain_plan_region(&binder);
	let join_row = first_block(&binder, "fn optimizer_explain_row");
	let scan_row = first_block(&binder, "fn optimizer_explain_scan_filter_row");

	for column in [
		"decision_kind",
		"target",
		"outcome",
		"detail",
		"estimated_rows",
		"spent_fact",
		"spent_column",
		"build_side",
		"build_bytes",
		"threshold_bytes",
		"rejected_side_bytes",
	] {
		assert!(schema.contains(column), "missing EXPLAIN column {column}");
	}
	assert!(plan.contains("trace"));
	assert!(plan.contains(".join_decisions"));
	assert!(plan.contains(".scan_filter_decisions"));
	assert!(join_row.contains("\"join\""));
	assert!(scan_row.contains("\"scan_filter\""));
}

#[test]
pub(crate) fn adr174_join_receipts_preserved() {
	let binder = source("src/binder.rs");
	let join_row = first_block(&binder, "fn optimizer_explain_row");
	let detail = first_block(&binder, "fn join_strategy_decision_detail_label");
	let build = first_block(&binder, "fn optimizer_explain_build_columns");

	assert!(join_row.contains("join_strategy_label"));
	assert!(join_row.contains("join_strategy_decision_detail_label"));
	assert!(join_row.contains("optimizer_explain_build_columns"));
	assert!(detail.contains("join_physical_placement_label"));
	assert!(detail.contains("join_strategy_decision_reason_label"));
	assert!(detail.contains("join_strategy_decision_reason_detail"));
	assert!(build.contains("BroadcastInferred"));
	assert!(build.contains("BroadcastEstimated"));
}

#[test]
pub(crate) fn adr174_no_runtime_storage_or_producer_surface() {
	let manifest = manifest_dir();
	let markers = [
		"ScanFilterReceipt",
		"ScanFilterOutcome",
		"FilterEstimateSource",
		"ScanFilterPushdownEvidence",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/ir.rs"),
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/catalog.rs"),
		manifest.join("src/distributed"),
	] {
		offenders.extend(
			source_lines_for_path(&path)
				.into_iter()
				.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
		);
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);
	assert!(
		offenders.is_empty(),
		"ADR174 cockpit receipt leaked outside binder/system-table surface:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr174_cockpit_is_frontend_shared() {
	let binder = source("src/binder.rs");
	let rune = source("src/rune_engine.rs");
	let bind_explain = first_block(&binder, "async fn bind_explain");
	let rune_explain = first_block(&rune, "pub fn explain");

	assert!(bind_explain.contains("bind_with_trace(input, self.catalog, self.config).await?"));
	assert!(bind_explain.contains("optimizer_explain_plan(trace)"));
	assert!(rune_explain.contains("LogicalPlan::Explain"));
	assert!(!rune.contains("ScanFilterReceipt"));
	assert!(!rune.contains("scan_filter_decisions"));
}
