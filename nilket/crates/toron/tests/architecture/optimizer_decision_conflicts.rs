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

#[test]
pub(crate) fn adr220_decision_conflict_table_is_explicit_information_schema_surface() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let explain = source("src/binder/explain.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_decision_conflicts_schema",
	);
	let rows = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_decision_conflict_rows",
	);

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_DECISION_CONFLICTS_TABLE"));
	assert!(binder.contains("information_schema.optimizer_decision_conflicts"));
	assert!(execution.contains("InformationSchemaOptimizerDecisionConflicts"));
	assert!(bind_relation.contains("SystemTableKind::InformationSchemaOptimizerDecisionConflicts"));
	assert!(bind_relation.contains("optimizer_decision_conflict_rows(self.config)"));
	assert!(bind_relation.contains("information_schema_optimizer_decision_conflicts_schema"));
	assert!(bind_relation.contains("information_schema_optimizer_decision_conflict_rows"));
	for column in [
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
	] {
		assert!(schema.contains(column), "schema is missing {column}");
		assert!(rows.contains(column), "row projection is missing {column}");
	}
}

#[test]
pub(crate) fn adr220_decision_conflict_rows_are_static_14_row_manifest() {
	let decision_log = source("src/binder/decision_log.rs");
	let join_order = source("src/binder/join_order.rs");
	let recording = first_block(&decision_log, "pub(crate) fn decision_recording_kinds");
	let reports = first_block(&join_order, "pub(crate) fn decision_conflict_reports");

	assert_eq!(recording.matches("OptimizerDecisionKind::").count(), 7);
	assert!(!recording.contains("PreservedOuterOrder"));
	assert!(reports.contains("for kind in decision_recording_kinds()"));
	assert!(reports.contains("OptimizerRuntimeVerdictProvenance::Native"));
	assert!(reports.contains("OptimizerRuntimeVerdictProvenance::Demoted"));
	assert!(reports.contains("debug_assert_eq!(grouped.len(), 14)"));
}

#[test]
pub(crate) fn adr220_conflict_model_matches_217_gate() {
	let join_order = source("src/binder/join_order.rs");
	let reports = first_block(&join_order, "pub(crate) fn decision_conflict_reports");

	assert!(reports.contains("OptimizerDecisionOutcome::Reordered"));
	assert!(reports.contains("OptimizerDecisionOutcome::KeptThinMargin"));
	assert!(reports.contains("OptimizerDecisionOutcome::Declined(reason)"));
	assert!(reports.contains("OptimizerDecisionOutcome::KeptOptimal"));
	assert!(reports.contains("kept_optimal_count"));
	assert!(reports.contains("blocked_or_near_miss_count = accumulator"));
	assert!(reports.contains("kept_thin_margin_count"));
	assert!(reports.contains("saturating_add(declined_count)"));
	let status = first_block(&join_order, "fn decision_conflict_status(");
	assert!(status.contains("JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE"));
	assert!(status.contains("DecisionConflictStatus::Conflicted"));
	assert!(!status.contains("> 0"));
}

#[test]
pub(crate) fn adr220_conflict_gate_and_detail_share_one_aggregation() {
	let join_order = source("src/binder/join_order.rs");
	let gate = first_block(&join_order, "fn decision_conflict_pressure_gate");
	let reports = first_block(&join_order, "pub(crate) fn decision_conflict_reports");

	assert!(gate.contains("decision_conflict_reports(snapshot)"));
	assert!(gate.contains("strongest_decision_conflict_report(&reports)"));
	assert!(reports.contains("DecisionConflictReport"));
	assert!(!join_order.contains("fn strongest_decision_conflict_signal"));
	assert!(!join_order.contains("fn compare_decision_conflict_signal"));
}

#[test]
pub(crate) fn adr220_conflict_detail_uses_decision_rows_not_verdicts() {
	let join_order = source("src/binder/join_order.rs");
	let rows = first_block(
		&join_order,
		"pub(crate) fn optimizer_decision_conflict_rows",
	);
	let reports = first_block(&join_order, "pub(crate) fn decision_conflict_reports");

	assert!(rows.contains("receipt_gate_snapshot()"));
	assert!(reports.contains("for row in &snapshot.decisions"));
	assert!(!reports.contains("snapshot.verdicts"));
	assert!(!reports.contains("verdict_snapshot()"));
	assert!(!reports.contains("decision_snapshot()"));
}

#[test]
pub(crate) fn adr220_no_decision_conflict_durable_key_counter_flush_or_evidence_matrix() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let decision_keys = first_block(&runtime_evidence, "pub(crate) fn all_decision_counter_keys");
	let verdict_keys = first_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");
	let flush = first_block(&runtime_evidence, "pub async fn flush_decision_deltas");
	let window_keys = first_block(&runtime_evidence, "pub(crate) fn all_window_counter_keys");
	let tests = source("src/binder/runtime_evidence.rs");

	assert!(!runtime_evidence.contains("optimizer_decision_conflicts"));
	assert!(!runtime_evidence.contains("decision_conflict/"));
	assert!(!decision_keys.contains("conflict"));
	assert!(!verdict_keys.contains("conflict"));
	assert!(!window_keys.contains("conflict"));
	assert_eq!(flush.matches("counter_fetch_add").count(), 1);
	assert!(tests.contains("assert_eq!(decision_keys.len(), 140)"));
	assert!(tests.contains("assert_eq!(decision_keys.len() + verdict_keys.len(), 220)"));
}

#[test]
pub(crate) fn adr220_windowing_retargeted_by_adr222_counter_handoff_stays_blocked() {
	super::optimizer_accrual::adr217_windowing_placeholder_retargeted_by_adr222_counter_handoff_stays_blocked();
	let join_order = source("src/binder/join_order.rs");
	let windowing = first_block(&join_order, "fn windowing_decay_pressure_gate");
	let counter = first_block(&join_order, "fn counter_handoff_readiness_gate");
	let reports = first_block(&join_order, "pub(crate) fn decision_conflict_reports");

	assert!(windowing.contains("stale_window_evidence_dominates"));
	assert!(!windowing.contains("DecisionConflictReport"));
	assert!(!counter.contains("DecisionConflictReport"));
	assert!(!reports.contains("windowing_decay_pressure"));
	assert!(!reports.contains("counter_handoff_readiness"));
}
