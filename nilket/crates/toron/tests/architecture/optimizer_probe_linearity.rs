use super::support::*;

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative)).expect("read Toron source")
}

fn execution_source() -> String {
	source("src/ir/execution.rs")
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
pub(crate) fn adr227_probe_linearity_is_typed_exhaustive_no_wildcard() {
	let binder = binder_source();
	let execution = execution_source();
	let execution_plan = source_block(&execution, "pub enum ExecutionPlan");
	let helper = source_block(&binder, "fn broadcast_probe_linearity");
	let wrapper = source_block(&binder, "fn broadcast_probe_is_scan_linear");

	assert!(binder.contains("enum ProbeLinearity"));
	assert!(binder.contains("Blocked(ProbeLinearityFence)"));
	assert!(binder.contains("enum ProbeLinearityFence"));
	for variant in [
		"EmptyRelation",
		"SystemTable",
		"Scan",
		"Filter",
		"Project",
		"Aggregate",
		"Exchange",
		"Sort",
		"Take",
		"Join",
		"CreateTable",
		"CreateLogicalType",
		"CreateTableAsSelect",
		"DropTable",
		"Delete",
		"Update",
		"Insert",
		"InsertSelect",
	] {
		assert!(
			execution_plan.contains(&format!("{variant} {{")),
			"ExecutionPlan variant list changed; update ADR227 guard for {variant}"
		);
		assert!(
			helper.contains(&format!("ExecutionPlan::{variant}")),
			"broadcast_probe_linearity missing ExecutionPlan::{variant}"
		);
	}
	assert!(helper.contains("ProbeLinearityFence::ScanRowLimit"));
	assert!(helper.contains("ProbeLinearityFence::NonBroadcastJoin"));
	assert!(helper.contains("ProbeLinearityFence::NonScanLeaf"));
	assert!(!helper.contains("_ =>"));
	assert!(wrapper.contains("broadcast_probe_linearity(plan).is_linear()"));
}

#[test]
pub(crate) fn adr227_probe_linearity_cause_is_not_stringly_typed() {
	let binder = binder_source();
	let join = source_block(&binder, "pub(crate) enum HashByDefaultReason");
	let decline = source_block(&binder, "pub(super) enum StarOrderDeclineReason");

	assert!(binder.contains("pub(crate) enum ProbeLinearityFence"));
	assert!(binder.contains("Blocked(ProbeLinearityFence)"));
	assert!(join.contains("ProbeNotScanLinear(ProbeLinearityFence)"));
	assert!(decline.contains("ProbeNotScanLinear(ProbeLinearityFence)"));
	assert!(!binder.contains("Blocked(&'static str)"));
	assert!(!binder.contains("ProbeNotScanLinear(&'static str)"));
}

#[test]
pub(crate) fn adr227_both_consumers_share_single_linearity_classifier() {
	let binder = binder_source();
	let bind_scoped_join = source_block(&binder, "fn bind_scoped_join");
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");
	let join_input = source_block(&binder, "pub(super) struct JoinInputOptimization");
	let edge = source_block(&binder, "pub(super) struct JoinOrderEdgeEvidence");
	let actuator = source_block(&binder, "async fn try_bind_star_order_actuation");

	assert!(bind_scoped_join.contains("broadcast_probe_linearity(&left_plan.plan)"));
	assert!(bind_scoped_join.contains("broadcast_probe_linearity(&right_plan.plan)"));
	assert!(bind_scoped_join.contains("probe_linearity: left_probe_linearity"));
	assert!(bind_scoped_join.contains("probe_linearity: right_probe_linearity"));
	assert!(join_input.contains("probe_linearity: ProbeLinearity"));
	assert!(edge.contains("probe_linearity: ProbeLinearity"));
	assert!(choose_auto.contains("right_input.probe_linearity.is_linear()"));
	assert!(choose_auto.contains("left_input.probe_linearity.is_linear()"));
	assert!(choose_auto.contains("ProbeNotScanLinear(fence)"));
	assert!(actuator.contains("broadcast_probe_linearity(&probe_plan.plan)"));
	assert!(actuator.contains("StarOrderDeclineReason::ProbeNotScanLinear(fence)"));
}

#[test]
pub(crate) fn adr227_plan_fragmenter_stays_probe_linearity_free_after_adr229_readout() {
	let manifest = manifest_dir();
	for relative in [
		"src/distributed/fragment.rs",
		"src/distributed/scheduler.rs",
		"src/exec.rs",
		"src/wire.rs",
	] {
		let source = std::fs::read_to_string(manifest.join(relative)).expect("read source");
		assert!(
			!source.contains("ProbeLinearity") && !source.contains("ProbeLinearityFence"),
			"ADR227/229 probe-linearity cause leaked into execution surface {relative}"
		);
		assert!(
			!source.contains("probe_not_scan_linear:"),
			"ADR227 rendered receipt detail leaked into execution surface {relative}"
		);
	}
}

#[test]
pub(crate) fn adr227_no_new_decision_reason_matrix_stays_140_220() {
	super::optimizer_accrual::adr216_decision_matrix_is_140_keys_and_evidence_matrix_220();
	let decision_log = std::fs::read_to_string(manifest_dir().join("src/binder/decision_log.rs"))
		.expect("read decision log");
	for fence in [
		"ScanRowLimit",
		"Take",
		"Sort",
		"Aggregate",
		"Exchange",
		"NonBroadcastJoin",
		"NonScanLeaf",
	] {
		assert!(
			!decision_log.contains(fence),
			"ADR227 fence {fence} must not become a durable decision reason"
		);
	}
}

#[test]
pub(crate) fn adr229_probe_linearity_fences_are_explicit_information_schema_surface() {
	let binder = binder_root_source();
	let execution = execution_source();
	let explain = std::fs::read_to_string(manifest_dir().join("src/binder/explain.rs"))
		.expect("read explain source");
	let bind_relation = source_block(&binder, "async fn bind_information_schema_relation");
	let schema = source_block(
		&explain,
		"pub(super) fn information_schema_optimizer_probe_linearity_fences_schema",
	);
	let rows = source_block(
		&explain,
		"pub(super) fn information_schema_optimizer_probe_linearity_fence_rows",
	);

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_PROBE_LINEARITY_FENCES_TABLE"));
	assert!(binder.contains("information_schema.optimizer_probe_linearity_fences"));
	assert!(execution.contains("InformationSchemaOptimizerProbeLinearityFences"));
	assert!(
		bind_relation.contains("SystemTableKind::InformationSchemaOptimizerProbeLinearityFences")
	);
	assert!(bind_relation.contains("runtime_evidence"));
	assert!(bind_relation.contains("probe_linearity_fence_snapshot()"));
	assert!(bind_relation.contains("information_schema_optimizer_probe_linearity_fences_schema"));
	assert!(bind_relation.contains("information_schema_optimizer_probe_linearity_fence_rows"));
	for column in [
		"consumer",
		"fence",
		"recent_count",
		"lifetime_count",
		"consumer_recent_total",
		"consumer_lifetime_total",
		"recent_share_per_mille",
		"status",
		"reason",
		"next_cut",
	] {
		assert!(schema.contains(column), "schema is missing {column}");
		assert!(rows.contains(column), "row projection is missing {column}");
	}
}

#[test]
pub(crate) fn adr229_probe_linearity_fence_manifest_is_fixed_two_by_seven() {
	let binder = binder_source();
	let decision_log = std::fs::read_to_string(manifest_dir().join("src/binder/decision_log.rs"))
		.expect("read decision log");
	let runtime_evidence =
		std::fs::read_to_string(manifest_dir().join("src/binder/runtime_evidence.rs"))
			.expect("read runtime evidence");
	let consumers = source_block(
		&decision_log,
		"pub(crate) fn optimizer_probe_linearity_consumers",
	);
	let fences = source_block(&binder, "pub(crate) fn probe_linearity_fences");
	let all_keys = source_block(
		&runtime_evidence,
		"pub(crate) fn all_probe_linearity_counter_keys",
	);
	let tests = source_block(
		&runtime_evidence,
		"fn adr229_probe_linearity_keys_are_fixed_14_key_detail_matrix",
	);

	assert_eq!(
		consumers
			.matches("OptimizerProbeLinearityConsumer::")
			.count(),
		2
	);
	assert_eq!(fences.matches("ProbeLinearityFence::").count(), 7);
	assert!(all_keys.contains("optimizer_probe_linearity_consumers()"));
	assert!(all_keys.contains("probe_linearity_fences()"));
	assert!(tests.contains("assert_eq!(keys.len(), 14)"));
	assert!(tests.contains("hash_fallback/non_broadcast_join"));
	assert!(tests.contains("star_decline/sort"));
}

#[test]
pub(crate) fn adr229_probe_linearity_durable_namespace_is_separate_from_220_matrix() {
	super::optimizer_accrual::adr216_decision_matrix_is_140_keys_and_evidence_matrix_220();
	let runtime_evidence =
		std::fs::read_to_string(manifest_dir().join("src/binder/runtime_evidence.rs"))
			.expect("read runtime evidence");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let decision_keys = source_block(&runtime_evidence, "pub(crate) fn all_decision_counter_keys");
	let verdict_keys = source_block(&runtime_evidence, "pub(crate) fn all_verdict_counter_keys");
	let probe_keys = source_block(
		&runtime_evidence,
		"pub(crate) fn all_probe_linearity_counter_keys",
	);
	let window_keys = source_block(&runtime_evidence, "pub(crate) fn all_window_counter_keys");
	let flush = source_block(
		&runtime_evidence,
		"pub async fn flush_probe_linearity_deltas",
	);

	assert!(runtime_evidence.contains("OPTIMIZER_PROBE_LINEARITY_EVIDENCE_KEY_PREFIX"));
	assert!(runtime_evidence.contains("optimizer_evidence/probe_linearity/"));
	assert!(!decision_keys.contains("probe_linearity"));
	assert!(!verdict_keys.contains("probe_linearity"));
	assert!(probe_keys.contains("probe_linearity_counter_key"));
	assert!(window_keys.contains("OptimizerEvidenceWindowCounterKey::ProbeLinearityFence"));
	assert!(window_keys.contains("all_probe_linearity_counter_keys"));
	assert_eq!(flush.matches("counter_fetch_add").count(), 1);
	assert!(daemon.contains("flush_probe_linearity_deltas().await"));
}

#[test]
pub(crate) fn adr229_probe_linearity_recording_is_committed_consumer_side_only() {
	let binder = binder_root_source();
	let join = binder_source();
	let join_order = source("src/binder/join_order.rs");
	let bind_scoped_join = source_block(&binder, "fn bind_scoped_join");
	let choose_auto = source_block(&join, "fn choose_auto_join_strategy");
	let star_decline = source_block(&join_order, "fn accrue_star_order_decline");
	let analyze = source_block(&join_order, "pub(super) fn analyze");

	let receipt_pos = bind_scoped_join
		.find("trace.join_decisions.push(DecisionReceipt")
		.expect("bind_scoped_join should commit the join receipt");
	let detail_pos = bind_scoped_join
		.find("OptimizerProbeLinearityConsumer::HashFallback")
		.expect("bind_scoped_join should record hash-fallback detail");
	assert!(receipt_pos < detail_pos);
	assert!(bind_scoped_join.contains("OptimizerProbeLinearityConsumer::HashFallback"));
	assert!(bind_scoped_join.contains("record_probe_linearity_fence"));
	assert!(!choose_auto.contains("record_probe_linearity_fence"));
	assert!(star_decline.contains("OptimizerProbeLinearityConsumer::StarDecline"));
	assert!(star_decline.contains("record_probe_linearity_fence"));
	assert!(!analyze.contains("record_probe_linearity_fence"));
}

#[test]
pub(crate) fn adr230_dominance_gate_is_sixth_receipt_gate_row_reusing_cockpit() {
	let join_order = source("src/binder/join_order.rs");
	let rows = source_block(&join_order, "pub(crate) fn optimizer_receipt_gate_rows");
	let gate = source_block(&join_order, "fn probe_linearity_fence_dominance_gate");

	for row in [
		"graph_margin_divergence_gate_row(&snapshot, config)",
		"preserved_outer_ordering_gate_row(&snapshot, config)",
		"decision_conflict_pressure_gate(&snapshot)",
		"windowing_decay_pressure_gate(&snapshot)",
		"counter_handoff_readiness_gate(&snapshot)",
		"probe_linearity_fence_dominance_gate(&snapshot)",
	] {
		assert_eq!(rows.matches(row).count(), 1, "receipt gate row {row}");
	}
	let counter = rows
		.find("counter_handoff_readiness_gate(&snapshot)")
		.expect("counter handoff row");
	let dominance = rows
		.find("probe_linearity_fence_dominance_gate(&snapshot)")
		.expect("probe-linearity dominance row");
	assert!(counter < dominance);
	assert!(gate.contains("gate_name: \"probe_linearity_fence_dominance\""));
	assert!(gate.contains("gate_family: \"probe_linearity\""));
	assert!(gate.contains("targeted_probe_linearity_fence_relaxation"));
}

#[test]
pub(crate) fn adr230_dominance_gate_status_from_real_229_evidence_only() {
	let join_order = source("src/binder/join_order.rs");
	let dominance = source_block(&join_order, "fn probe_linearity_fence_dominance");
	let gate = source_block(&join_order, "fn probe_linearity_fence_dominance_gate");

	assert!(dominance.contains("snapshot.probe_linearity_fences"));
	assert!(dominance.contains("OptimizerProbeLinearityFenceStatus::Dominant"));
	assert!(dominance.contains("OptimizerProbeLinearityFenceStatus::Observed"));
	assert!(dominance.contains("row.consumer_recent_total"));
	assert!(dominance.contains("selected.recent_count"));
	assert!(!dominance.contains("PROBE_LINEARITY_FENCE_MIN_SAMPLE"));
	assert!(!dominance.contains("OptimizerProbeLinearityConsumer::HashFallback"));
	assert!(!dominance.contains("OptimizerProbeLinearityConsumer::StarDecline"));
	assert!(dominance.contains("dominant_probe_linearity_fence_ready"));
	assert!(dominance.contains("probe_linearity_pressure_split"));
	assert!(dominance.contains("requires_recent_probe_linearity_floor"));
	assert!(gate.contains("optimizer_probe_linearity_consumer_label(row.consumer)"));
	assert!(gate.contains("row.fence.label()"));
}

#[test]
pub(crate) fn adr230_dominance_gate_uses_single_locked_receipt_snapshot() {
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let join_order = source("src/binder/join_order.rs");
	let snapshot_struct = source_block(
		&runtime_evidence,
		"pub(crate) struct OptimizerReceiptGateSnapshot",
	);
	let snapshot = source_block(&runtime_evidence, "pub(crate) fn receipt_gate_snapshot");
	let rows = source_block(&join_order, "pub(crate) fn optimizer_receipt_gate_rows");

	assert!(
		snapshot_struct.contains("probe_linearity_fences: Vec<OptimizerProbeLinearityFenceRow>")
	);
	assert_eq!(snapshot.matches("self.state.lock()").count(), 1);
	assert!(snapshot.contains("let current_window = self.window_id_source.current()"));
	assert!(snapshot.contains("probe_linearity_fence_rows_from_state(&state, current_window)"));
	assert!(snapshot.contains("probe_linearity_fences,"));
	assert_eq!(rows.matches("receipt_gate_snapshot()").count(), 1);
	assert!(rows.contains("probe_linearity_fence_dominance_gate(&snapshot)"));
	assert!(!rows.contains("probe_linearity_fence_snapshot()"));
}

#[test]
pub(crate) fn adr230_dominance_gate_has_no_policy_consumer() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let resolver = source_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let graph = source_block(&join_order, "pub(super) async fn try_bind_graph_actuation");
	let outer = source_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);
	let star = source_block(&join_order, "async fn try_bind_star_order_actuation");
	let bind_scoped_join = source_block(&binder, "fn bind_scoped_join");

	for policy in [resolver, graph, outer, star, bind_scoped_join] {
		assert!(!policy.contains("probe_linearity_fence_dominance"));
		assert!(!policy.contains("targeted_probe_linearity_fence_relaxation"));
		assert!(!policy.contains("OptimizerProbeLinearityFenceStatus::Dominant"));
	}
}

#[test]
pub(crate) fn adr230_no_optimizer_actuation_without_actionable_dominance_gate() {
	adr230_dominance_gate_has_no_policy_consumer();
}

#[test]
pub(crate) fn adr230_no_new_system_table_rune_handle_durable_key_or_wendelon_surface() {
	super::rune_optimizer_readouts::adr228_no_new_information_schema_table_or_system_table_kind();
	let manifest = manifest_dir();
	let execution = execution_source();
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let rune = source("src/rune_engine.rs");
	let daemon = source("src/daemon.rs");
	let system_kind = source_block(&execution, "pub enum SystemTableKind");

	assert_eq!(enum_variant_count(&system_kind), 13);
	assert!(!system_kind.contains("InformationSchemaOptimizerProbeLinearityDominance"));
	assert!(rune.contains("const RUNE_OPTIMIZER_READOUT_TABLES: [&str; 8]"));
	assert!(!rune.contains("optimizer_probe_linearity_dominance"));
	assert!(!rune.contains("RUNE_OPTIMIZER_PROBE_LINEARITY_DOMINANCE"));
	assert!(!runtime_evidence.contains("optimizer_evidence/probe_linearity_dominance"));
	assert!(!runtime_evidence.contains("all_probe_linearity_dominance_counter_keys"));
	assert!(!daemon.contains("flush_probe_linearity_dominance"));

	let markers = [
		"probe_linearity_fence_dominance",
		"targeted_probe_linearity_fence_relaxation",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/coordinator.rs"),
		manifest.join("src/network/observability.rs"),
		manifest.join("src/distributed"),
		manifest.join("src/exec.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/wire.rs"),
		manifest.join("../wendelon/src"),
	] {
		offenders.extend(
			source_lines_for_path(&path)
				.into_iter()
				.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
		);
	}
	assert!(
		offenders.is_empty(),
		"ADR230 dominance gate leaked into policy/execution/Wendelon surfaces:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr227_226_zero_admissible_and_no_generic_search_intact() {
	super::optimizer_memo_admission::adr226_no_capability_decline_is_currently_contractable();
	super::optimizer_memo_admission::adr226_memo_admission_still_renders_zero_admissible();
	super::optimizer_memo_admission::adr226_future_admissible_row_requires_production_home_proof();
	super::optimizer_memo_admission::adr226_capability_audit_makes_no_source_logic_or_matrix_change(
	);
}
