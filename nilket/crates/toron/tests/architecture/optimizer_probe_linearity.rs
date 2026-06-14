use super::support::*;

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

fn execution_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source")
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
	let join_order = std::fs::read_to_string(manifest_dir().join("src/binder/join_order.rs"))
		.expect("read join_order");
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
pub(crate) fn adr227_226_zero_admissible_and_no_generic_search_intact() {
	super::optimizer_memo_admission::adr226_no_capability_decline_is_currently_contractable();
	super::optimizer_memo_admission::adr226_memo_admission_still_renders_zero_admissible();
	super::optimizer_memo_admission::adr226_future_admissible_row_requires_production_home_proof();
	super::optimizer_memo_admission::adr226_capability_audit_makes_no_source_logic_or_matrix_change(
	);
}
