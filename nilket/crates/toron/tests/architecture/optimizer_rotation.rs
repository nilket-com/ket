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
pub(crate) fn adr218_graph_actuator_and_receipt_table_share_gate_result() {
	let join_order = source("src/binder/join_order.rs");
	let gate = first_block(&join_order, "pub(crate) fn graph_margin_divergence_gate");
	let row = first_block(&join_order, "fn graph_margin_divergence_gate_row");
	let resolver = first_block(
		&join_order,
		"pub(crate) fn effective_join_order_soft_margin_steps",
	);
	let steps = first_block(&join_order, "fn graph_kind_steps_from_gate");

	assert!(join_order.contains("pub(crate) struct GraphMarginDivergenceGate"));
	assert!(join_order.contains("ready: Vec<GraphKindGateSignal>"));
	assert!(gate.contains("GraphMarginDivergenceGate { status, ready }"));
	assert!(row.contains("let gate = graph_margin_divergence_gate(snapshot, config)"));
	assert!(row.contains("gate.sample_count()"));
	assert!(row.contains("gate.ready"));
	assert!(resolver.contains("let graph_gate = graph_margin_divergence_gate(&snapshot, config)"));
	assert!(resolver.contains("graph_kind_steps_from_gate(graph_family, &graph_gate)"));
	assert!(steps.contains("gate.status != OptimizerReceiptGateStatus::Actionable"));
	assert!(steps.contains("for signal in &gate.ready"));
	assert_eq!(resolver.matches("graph_margin_divergence_gate(").count(), 1);
	assert_eq!(row.matches("graph_margin_divergence_gate(").count(), 1);
}

#[test]
pub(crate) fn adr218_graph_actuation_uses_step_for_shape_decision_kind() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let binder_struct = first_block(&binder, "struct Binder<'a>");
	let no_recording = first_block(&binder, "fn without_decision_recording");
	let graph_steps = first_block(&join_order, "impl EffectiveGraphJoinOrderSoftMarginSteps");
	let shape = first_block(&join_order, "impl GraphShape");
	let actuation = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");

	assert!(
		binder_struct
			.contains("graph_join_order_soft_margin_steps: EffectiveGraphJoinOrderSoftMarginSteps")
	);
	assert!(
		no_recording.contains(
			"graph_join_order_soft_margin_steps: self.graph_join_order_soft_margin_steps"
		)
	);
	assert!(graph_steps.contains("pub(crate) fn step_for"));
	assert!(graph_steps.contains("OptimizerDecisionKind::GalaxyRoot => self.galaxy"));
	assert!(graph_steps.contains("OptimizerDecisionKind::BridgeRoot => self.bridge"));
	assert!(graph_steps.contains("OptimizerDecisionKind::BridgeChainRoot => self.bridge_chain"));
	assert!(graph_steps.contains("unreachable!"));
	assert!(shape.contains("fn decision_kind"));
	assert!(shape.contains("GraphShape::Galaxy => OptimizerDecisionKind::GalaxyRoot"));
	assert!(shape.contains("GraphShape::Bridge => OptimizerDecisionKind::BridgeRoot"));
	assert!(shape.contains("GraphShape::Chain => OptimizerDecisionKind::BridgeChainRoot"));
	assert!(actuation.contains("graph_join_order_soft_margin_steps"));
	assert!(actuation.contains("step_for(shape.decision_kind())"));
	assert!(!actuation.contains("graph_join_order_soft_margin_step.margin()"));
}

#[test]
pub(crate) fn adr218_graph_actuation_armed_only_by_graph_margin_divergence() {
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(
		&join_order,
		"pub(crate) fn effective_join_order_soft_margin_steps",
	);
	let steps = first_block(&join_order, "fn graph_kind_steps_from_gate");

	assert!(resolver.contains("graph_margin_divergence_gate(&snapshot, config)"));
	assert!(steps.contains("gate.status != OptimizerReceiptGateStatus::Actionable"));
	assert!(steps.contains("EffectiveGraphJoinOrderSoftMarginSteps::uniform(family)"));
	assert!(steps.contains("OptimizerDecisionKind::GalaxyRoot => steps.galaxy"));
	assert!(steps.contains("OptimizerDecisionKind::BridgeRoot => steps.bridge"));
	assert!(steps.contains("OptimizerDecisionKind::BridgeChainRoot => steps.bridge_chain"));
	for forbidden in [
		"preserved_outer_ordering_gate",
		"decision_conflict_pressure_gate",
		"windowing_decay_pressure_gate",
		"counter_handoff_readiness_gate",
	] {
		assert!(
			!resolver.contains(forbidden),
			"ADR218 resolver consumed non-graph receipt gate {forbidden}"
		);
		assert!(
			!steps.contains(forbidden),
			"ADR218 graph step resolver consumed non-graph receipt gate {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr218_tree_and_manual_margin_byte_identical_under_graph_divergence() {
	let tests = source("src/binder/tests.rs");
	let tree = first_block(
		&tests,
		"async fn tree_margin_application_byte_identical_under_graph_kind_divergence",
	);
	let manual = first_block(
		&tests,
		"async fn manual_mode_byte_identical_for_both_families",
	);
	let resolver = first_block(
		&source("src/binder/join_order.rs"),
		"pub(crate) fn effective_join_order_soft_margin_steps",
	);

	assert!(tests.contains("const GRAPH_DIVERGENT_VERDICTS"));
	assert!(tests.contains("fn graph_kind_without_ready_evidence_uses_family_step"));
	assert!(tests.contains("fn manual_ignores_graph_kind_divergence"));
	assert!(tree.contains("GRAPH_DIVERGENT_VERDICTS"));
	assert!(tree.contains("format!(\"{baseline_plan:?}\")"));
	assert!(tree.contains("format!(\"{graph_plan:?}\")"));
	assert!(tree.contains("baseline_trace.join_decisions"));
	assert!(manual.contains("OptimizerDecisionKind::BridgeChainRoot"));
	assert!(manual.contains("OptimizerRuntimeVerdictClass::OverEager"));
	assert!(manual.contains("format!(\"{manual_plan:?}\")"));
	assert!(manual.contains("format!(\"{baseline_plan:?}\")"));
	assert!(resolver.contains("JoinOrderSoftMarginApplication::Manual"));
	assert!(resolver.contains("EffectiveGraphJoinOrderSoftMarginSteps::uniform"));
}

#[test]
pub(crate) fn adr218_no_new_durable_key_counter_table_or_system_table_kind() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let system_table_kind = first_block(&execution, "pub enum SystemTableKind");
	let config = first_block(&binder, "pub struct PlannerConfig");

	assert!(!runtime_evidence.contains("GraphMarginDivergenceGate"));
	assert!(!runtime_evidence.contains("graph_kind_margin"));
	assert!(!runtime_evidence.contains("optimizer_evidence/graph"));
	assert!(!runtime_evidence.contains("optimizer_graph"));
	assert_eq!(
		system_table_kind
			.matches("InformationSchemaOptimizerReceiptGates")
			.count(),
		1
	);
	assert!(!system_table_kind.contains("GraphKind"));
	assert!(!config.contains("graph_join_order_soft_margin_application"));
	assert!(!config.contains("graph_kind"));
	assert!(!config.contains("receipt_armed"));
}

#[test]
pub(crate) fn adr218_other_four_receipt_gates_stay_non_consumers() {
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(
		&join_order,
		"pub(crate) fn effective_join_order_soft_margin_steps",
	);
	let actuation = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");

	assert!(resolver.contains("graph_margin_divergence_gate"));
	for forbidden in [
		"preserved_outer_ordering_gate",
		"decision_conflict_pressure_gate",
		"windowing_decay_pressure_gate",
		"counter_handoff_readiness_gate",
		"PreservedOuterOrder",
		"decision_conflict_pressure",
		"windowing_decay_pressure",
		"counter_handoff_readiness",
	] {
		assert!(
			!resolver.contains(forbidden),
			"non-graph receipt gate became resolver policy: {forbidden}"
		);
		assert!(
			!actuation.contains(forbidden),
			"non-graph receipt gate became graph actuation policy: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr218_graph_kind_domain_is_galaxy_bridge_bridgechain_only() {
	let join_order = source("src/binder/join_order.rs");
	let graph_steps = first_block(&join_order, "impl EffectiveGraphJoinOrderSoftMarginSteps");
	let signals = first_block(&join_order, "fn graph_kind_gate_signals");
	let tests = source("src/binder/tests.rs");

	for kind in ["GalaxyRoot", "BridgeRoot", "BridgeChainRoot"] {
		assert!(graph_steps.contains(kind), "step_for missing {kind}");
		assert!(signals.contains(kind), "gate signals missing {kind}");
	}
	for kind in [
		"StarCandidate",
		"FlatStar",
		"SnowflakeRoot",
		"SnowflakeBranch",
		"PreservedOuterOrder",
	] {
		assert!(graph_steps.contains(kind), "step_for must reject {kind}");
	}
	assert!(tests.contains("effective_graph_kind_join_order_soft_margin_step"));
	assert!(tests.contains("OptimizerDecisionKind::GalaxyRoot"));
	assert!(tests.contains("OptimizerDecisionKind::BridgeRoot"));
	assert!(tests.contains("OptimizerDecisionKind::BridgeChainRoot"));
}
