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
pub(crate) fn adr227_no_plan_fragmenter_or_durable_change() {
	let manifest = manifest_dir();
	for relative in [
		"src/distributed/fragment.rs",
		"src/binder/runtime_evidence.rs",
		"src/binder/decision_log.rs",
		"src/ir/execution.rs",
	] {
		let source = std::fs::read_to_string(manifest.join(relative)).expect("read source");
		assert!(
			!source.contains("ProbeLinearity") && !source.contains("ProbeLinearityFence"),
			"ADR227 probe-linearity cause leaked into {relative}"
		);
		assert!(
			!source.contains("probe_not_scan_linear:"),
			"ADR227 rendered receipt detail leaked into durable/runtime surface {relative}"
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
pub(crate) fn adr227_226_zero_admissible_and_no_generic_search_intact() {
	super::optimizer_memo_admission::adr226_no_capability_decline_is_currently_contractable();
	super::optimizer_memo_admission::adr226_memo_admission_still_renders_zero_admissible();
	super::optimizer_memo_admission::adr226_future_admissible_row_requires_production_home_proof();
	super::optimizer_memo_admission::adr226_capability_audit_makes_no_source_logic_or_matrix_change(
	);
}
