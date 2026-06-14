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
pub(crate) fn adr222_window_keys_are_separate_namespace_not_in_220_matrix() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let verdict_keys = first_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");
	let decision_keys = first_block(&runtime_evidence, "pub(crate) fn all_decision_counter_keys");
	let window_key = first_block(&runtime_evidence, "pub(crate) fn window_counter_key");
	let window_keys = first_block(&runtime_evidence, "pub(crate) fn all_window_counter_keys");
	let tests = first_block(
		&runtime_evidence,
		"fn adr222_window_keys_are_separate_namespace_not_in_220_matrix",
	);

	assert!(runtime_evidence.contains(
		"pub const OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX: &str = \"optimizer_evidence/window/\""
	));
	assert!(!verdict_keys.contains("OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX"));
	assert!(!decision_keys.contains("OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX"));
	assert!(window_key.contains("OPTIMIZER_EVIDENCE_WINDOW_KEY_PREFIX"));
	assert!(window_key.contains("verdict_counter_suffix"));
	assert!(window_key.contains("decision_counter_suffix"));
	assert!(window_keys.contains("all_verdict_counter_keys"));
	assert!(window_keys.contains("all_decision_counter_keys"));
	assert!(tests.contains("assert_eq!(window_keys.len(), 440)"));
	assert!(tests.contains("assert_eq!(all_verdict_counter_keys().len(), 80)"));
	assert!(tests.contains("assert_eq!(all_decision_counter_keys().len(), 140)"));
}

#[test]
pub(crate) fn adr222_windowing_gate_is_computed_not_placeholder() {
	let join_order = source("src/binder/join_order.rs");
	let windowing = first_block(&join_order, "fn windowing_decay_pressure");

	assert!(windowing.contains("let window = &snapshot.window"));
	assert!(windowing.contains("window.lifetime_count < JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE"));
	assert!(
		windowing.contains("window.stale_lifetime_count >= JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE")
	);
	assert!(windowing.contains("window.stale_lifetime_count > window.recent_count"));
	assert!(windowing.contains("OptimizerReceiptGateStatus::Actionable"));
	assert!(windowing.contains("OptimizerReceiptGateStatus::Inactive"));
	assert!(windowing.contains("OptimizerReceiptGateStatus::InsufficientEvidence"));
	assert!(!windowing.contains("status: OptimizerReceiptGateStatus::BlockedExternalSignal"));
	assert!(!windowing.contains("windowing_decay_substrate_unbuilt"));
}

#[test]
pub(crate) fn adr222_windowing_gate_uses_window_split_not_lifetime_totals() {
	let join_order = source("src/binder/join_order.rs");
	let windowing = first_block(&join_order, "fn windowing_decay_pressure");
	let gate = first_block(&join_order, "fn windowing_decay_pressure_gate");

	assert!(windowing.contains("lifetime_total: window.lifetime_count"));
	assert!(windowing.contains("recent_window: window.recent_count"));
	assert!(windowing.contains("stale_lifetime: window.stale_lifetime_count"));
	assert!(gate.contains("recent_window={}"));
	assert!(gate.contains("stale_lifetime={}"));
	assert!(gate.contains("lifetime_total={}"));
	assert!(windowing.contains("requires_lifetime_evidence_floor"));
	assert!(windowing.contains("recent_window_matches_lifetime"));
	assert!(windowing.contains("stale_window_evidence_dominates"));
	assert!(!windowing.contains("snapshot.verdicts"));
	assert!(!windowing.contains("snapshot.decisions"));
}

#[test]
pub(crate) fn adr222_window_read_is_bounded_current_previous_manifest() {
	let binder = binder_root_source();
	let coordinator = source("src/coordinator.rs");
	let daemon = source("src/daemon.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let refresh = first_block(&runtime_evidence, "pub async fn refresh_durable_cache");
	let manifest = first_block(&runtime_evidence, "pub(crate) fn all_window_counter_keys");
	let snapshot = first_block(&runtime_evidence, "fn window_snapshot_from_state");

	assert!(refresh.contains("let current_window = self.window_id_source.current()"));
	assert!(refresh.contains("let previous_window = current_window.previous()"));
	assert!(refresh.contains("all_window_counter_keys(current_window, previous_window)"));
	assert!(manifest.contains("for window in [current_window, previous_window]"));
	assert!(snapshot.contains("current_window.previous()"));
	assert!(!refresh.contains("prefix"));
	assert!(!manifest.contains("prefix"));
	assert!(!refresh.contains("scan"));
	assert!(daemon.contains("flush_window_deltas().await"));
	assert!(!binder.contains("flush_window_deltas"));
	assert!(!coordinator.contains("flush_window_deltas"));
}

#[test]
pub(crate) fn adr222_window_snapshot_is_single_locked_snapshot() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let snapshot = first_block(&runtime_evidence, "pub(crate) fn receipt_gate_snapshot");
	let window_snapshot = first_block(&runtime_evidence, "fn window_snapshot_from_state");

	assert_eq!(snapshot.matches("self.state.lock()").count(), 1);
	assert!(snapshot.contains("let current_window = self.window_id_source.current()"));
	assert!(snapshot.contains("recent_decision_rows_from_state(&state, current_window)"));
	assert!(snapshot.contains("window_snapshot_from_state(&state, current_window)"));
	assert!(snapshot.contains("OptimizerReceiptGateSnapshot"));
	assert!(runtime_evidence.contains("window: OptimizerEvidenceWindowSnapshot"));
	assert!(runtime_evidence.contains("recent_decisions: Vec<OptimizerDecisionRow>"));
	assert!(window_snapshot.contains("lifetime_total_count_from_state(state)"));
	assert!(window_snapshot.contains("window_total_count_from_state(state, current_window)"));
	assert!(window_snapshot.contains("window_total_count_from_state(state, previous_window)"));
}

#[test]
pub(crate) fn adr222_windowing_readout_has_no_policy_consumers() {
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let graph = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");
	let outer = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);
	let windowing = first_block(&join_order, "fn windowing_decay_pressure_gate");

	for policy in [resolver, graph, outer] {
		assert!(!policy.contains("windowing_decay_pressure_gate"));
		assert!(!policy.contains("windowing_decay_pressure"));
		assert!(!policy.contains("OptimizerEvidenceWindowSnapshot"));
	}
	assert!(!windowing.contains("effective_join_order_policy"));
	assert!(!windowing.contains("try_bind_graph_actuation"));
	assert!(!windowing.contains("try_bind_preserved_outer_star_actuation"));
}

#[test]
pub(crate) fn adr222_counter_handoff_stays_blocked_external_signal() {
	let join_order = source("src/binder/join_order.rs");
	let counter = first_block(&join_order, "fn counter_handoff_readiness_gate");

	assert!(counter.contains("OptimizerReceiptGateStatus::BlockedExternalSignal"));
	assert!(counter.contains("sample_count: 0"));
	assert!(counter.contains("elasticity_signal=missing"));
	assert!(counter.contains("elasticity_signal_unbuilt"));
	assert!(!counter.contains("OptimizerReceiptGateStatus::Actionable"));
	assert!(!counter.contains("window"));
}

#[test]
pub(crate) fn adr222_no_new_information_schema_or_system_table_kind() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let explain = source("src/binder/explain.rs");
	let system_table_kind = first_block(&execution, "pub enum SystemTableKind");

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_RECEIPT_GATES_TABLE"));
	assert!(!binder.contains("OPTIMIZER_EVIDENCE_WINDOWS"));
	assert!(!binder.contains("optimizer_evidence_windows"));
	assert!(!execution.contains("OptimizerEvidenceWindow"));
	assert!(!explain.contains("optimizer_evidence_window"));
	assert_eq!(enum_variant_count(&system_table_kind), 12);
	assert!(system_table_kind.contains("InformationSchemaOptimizerMemoAdmission"));
}
