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
pub(crate) fn adr223_memo_admission_table_is_explicit_information_schema_surface() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let explain = source("src/binder/explain.rs");
	let bind_relation = first_block(&binder, "async fn bind_information_schema_relation");
	let schema = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_memo_admission_schema",
	);
	let rows = first_block(
		&explain,
		"pub(super) fn information_schema_optimizer_memo_admission_rows",
	);

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_MEMO_ADMISSION_TABLE"));
	assert!(binder.contains("information_schema.optimizer_memo_admission"));
	assert!(execution.contains("InformationSchemaOptimizerMemoAdmission"));
	assert!(bind_relation.contains("SystemTableKind::InformationSchemaOptimizerMemoAdmission"));
	assert!(bind_relation.contains("optimizer_memo_admission_rows(self.config)"));
	assert!(bind_relation.contains("information_schema_optimizer_memo_admission_schema"));
	assert!(bind_relation.contains("information_schema_optimizer_memo_admission_rows"));
	for column in [
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
	] {
		assert!(schema.contains(column), "schema is missing {column}");
		assert!(rows.contains(column), "row projection is missing {column}");
	}
}

#[test]
pub(crate) fn adr223_memo_admission_rows_are_static_14_group_manifest() {
	let decision_log = source("src/binder/decision_log.rs");
	let join_order = source("src/binder/join_order.rs");
	let recording = first_block(&decision_log, "pub(crate) fn decision_recording_kinds");
	let from_snapshot = first_block(
		&join_order,
		"pub(crate) fn optimizer_memo_admission_rows_from_snapshot",
	);
	let status = first_block(&join_order, "pub(crate) enum OptimizerAdmissionStatus");
	let label = first_block(
		&join_order,
		"pub(crate) fn optimizer_memo_admission_status_label",
	);

	assert_eq!(recording.matches("OptimizerDecisionKind::").count(), 7);
	assert!(!recording.contains("PreservedOuterOrder"));
	assert!(from_snapshot.contains("decision_conflict_reports(snapshot)"));
	assert!(from_snapshot.contains("windowing_decay_pressure(snapshot)"));
	assert!(from_snapshot.contains(".iter()"));
	assert_eq!(enum_variant_count(&status), 5);
	for token in [
		"Cold",
		"StaleEvidence",
		"DegradedEvidence",
		"BlockedShape",
		"Admissible",
		"cold",
		"stale_evidence",
		"degraded_evidence",
		"blocked_shape",
		"admissible",
	] {
		assert!(
			label.contains(token),
			"missing admission status token {token}"
		);
	}
}

#[test]
pub(crate) fn adr223_admission_uses_decision_conflict_reports_and_window_snapshot() {
	let join_order = source("src/binder/join_order.rs");
	let rows = first_block(&join_order, "pub(crate) fn optimizer_memo_admission_rows");
	let from_snapshot = first_block(
		&join_order,
		"pub(crate) fn optimizer_memo_admission_rows_from_snapshot",
	);
	let row = first_block(&join_order, "fn optimizer_memo_admission_row(");

	assert_eq!(rows.matches("receipt_gate_snapshot()").count(), 1);
	assert!(!rows.contains("decision_snapshot()"));
	assert!(!rows.contains("verdict_snapshot()"));
	assert!(!rows.contains("status_snapshot()"));
	assert!(rows.contains("optimizer_memo_admission_rows_from_snapshot(&snapshot)"));
	assert!(!from_snapshot.contains("receipt_gate_snapshot()"));
	assert!(from_snapshot.contains("decision_conflict_reports(snapshot)"));
	assert!(from_snapshot.contains("windowing_decay_pressure(snapshot)"));
	assert!(row.contains("report.conflict_strength"));
	assert!(row.contains("window.recent_window"));
	assert!(row.contains("window.stale_lifetime"));
	assert!(row.contains("window.lifetime_total"));
	assert!(row.contains("evidence_health_label(&snapshot.health)"));
}

#[test]
pub(crate) fn adr223_recent_evidence_requires_222_window_inactive_recent_reason() {
	let join_order = source("src/binder/join_order.rs");
	let row = first_block(&join_order, "fn optimizer_memo_admission_row(");
	let window = first_block(&join_order, "fn windowing_decay_pressure");

	assert!(row.contains("report.status != DecisionConflictStatus::Conflicted"));
	assert!(row.contains("window.status != OptimizerReceiptGateStatus::Inactive"));
	assert!(row.contains("window.reason != \"recent_window_matches_lifetime\""));
	assert!(row.contains("OptimizerAdmissionStatus::StaleEvidence"));
	assert!(row.contains("per_group_window_recency"));
	assert!(window.contains("recent_window_matches_lifetime"));
	assert!(window.contains("stale_window_evidence_dominates"));
}

#[test]
pub(crate) fn adr223_blocked_shape_is_not_admissible_without_bounded_contract() {
	let join_order = source("src/binder/join_order.rs");
	let row = first_block(&join_order, "fn optimizer_memo_admission_row(");
	let contract = first_block(&join_order, "fn bounded_admission_candidate_contract");

	assert!(row.contains("snapshot.health.state != OptimizerEvidenceState::Ok"));
	assert!(row.contains("contract.bounded_candidate_count >= 2"));
	assert!(row.contains("OptimizerAdmissionStatus::Admissible"));
	assert!(row.contains("OptimizerAdmissionStatus::BlockedShape"));
	assert!(contract.contains("bounded_candidate_count: 0"));
	assert!(contract.contains("bounded_candidate_contract_unbuilt"));
	assert!(contract.contains("bounded_conflict_candidate_contract"));
}

#[test]
pub(crate) fn adr223_no_memo_engine_or_policy_consumer() {
	let manifest = manifest_dir();
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let graph = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");
	let outer = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);

	assert!(!manifest.join("src/optimizer.rs").exists());
	assert!(!manifest.join("src/optimizer").exists());
	assert!(!join_order.contains("Cascades"));
	assert!(!join_order.contains("fn memo"));
	assert!(!join_order.contains("Memo"));
	for policy in [resolver, graph, outer] {
		assert!(!policy.contains("optimizer_memo_admission"));
		assert!(!policy.contains("OptimizerAdmission"));
		assert!(!policy.contains("bounded_admission_candidate_contract"));
		assert!(!policy.contains("memo"));
		assert!(!policy.contains("Cascades"));
	}
}

#[test]
pub(crate) fn adr223_no_new_durable_key_counter_flush_or_wendelon_surface() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let decision_keys = first_block(&runtime_evidence, "pub(crate) fn all_decision_counter_keys");
	let verdict_keys = first_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");
	let window_keys = first_block(&runtime_evidence, "pub(crate) fn all_window_counter_keys");
	let flush_decisions = first_block(&runtime_evidence, "pub async fn flush_decision_deltas");
	let flush_windows = first_block(&runtime_evidence, "pub async fn flush_window_deltas");
	let wendelon_hits = source_files(&manifest_dir().join("../wendelon/src"))
		.into_iter()
		.filter_map(|path| {
			std::fs::read_to_string(&path)
				.ok()
				.and_then(|text| text.contains("optimizer_memo_admission").then_some(path))
		})
		.collect::<Vec<_>>();

	assert!(!runtime_evidence.contains("optimizer_memo_admission"));
	assert!(!decision_keys.contains("memo"));
	assert!(!verdict_keys.contains("memo"));
	assert!(!window_keys.contains("memo"));
	assert!(!flush_decisions.contains("optimizer_memo_admission"));
	assert!(!flush_windows.contains("optimizer_memo_admission"));
	assert!(
		wendelon_hits.is_empty(),
		"ADR223 leaked optimizer memo admission into Wendelon: {wendelon_hits:?}"
	);
}

#[test]
pub(crate) fn adr223_no_receipt_gate_manifest_change() {
	let join_order = source("src/binder/join_order.rs");
	let rows = first_block(&join_order, "pub(crate) fn optimizer_receipt_gate_rows");
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let system_table_kind = first_block(&execution, "pub enum SystemTableKind");

	assert!(rows.contains("graph_margin_divergence_gate_row(&snapshot, config)"));
	assert!(rows.contains("preserved_outer_ordering_gate_row(&snapshot, config)"));
	assert!(rows.contains("decision_conflict_pressure_gate(&snapshot)"));
	assert!(rows.contains("windowing_decay_pressure_gate(&snapshot)"));
	assert!(rows.contains("counter_handoff_readiness_gate(&snapshot)"));
	for gate in [
		"graph_margin_divergence_gate_row(&snapshot, config)",
		"preserved_outer_ordering_gate_row(&snapshot, config)",
		"decision_conflict_pressure_gate(&snapshot)",
		"windowing_decay_pressure_gate(&snapshot)",
		"counter_handoff_readiness_gate(&snapshot)",
	] {
		assert_eq!(rows.matches(gate).count(), 1, "receipt gate {gate}");
	}
	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_MEMO_ADMISSION_TABLE"));
	assert_eq!(enum_variant_count(&system_table_kind), 12);
}

#[test]
pub(crate) fn memo_admission_adds_no_durable_key_counter_or_flush() {
	adr223_no_new_durable_key_counter_flush_or_wendelon_surface();
}
