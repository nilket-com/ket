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
pub(crate) fn adr218_graph_actuator_and_receipt_table_share_gate_result() {
	let join_order = source("src/binder/join_order.rs");
	let gate = first_block(&join_order, "pub(crate) fn graph_margin_divergence_gate");
	let row = first_block(&join_order, "fn graph_margin_divergence_gate_row");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
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

	assert!(binder_struct.contains("join_order_policy: EffectiveJoinOrderPolicy"));
	assert!(no_recording.contains("join_order_policy: self.join_order_policy"));
	assert!(graph_steps.contains("pub(crate) fn step_for"));
	assert!(graph_steps.contains("OptimizerDecisionKind::GalaxyRoot => self.galaxy"));
	assert!(graph_steps.contains("OptimizerDecisionKind::BridgeRoot => self.bridge"));
	assert!(graph_steps.contains("OptimizerDecisionKind::BridgeChainRoot => self.bridge_chain"));
	assert!(graph_steps.contains("unreachable!"));
	assert!(shape.contains("fn decision_kind"));
	assert!(shape.contains("GraphShape::Galaxy => OptimizerDecisionKind::GalaxyRoot"));
	assert!(shape.contains("GraphShape::Bridge => OptimizerDecisionKind::BridgeRoot"));
	assert!(shape.contains("GraphShape::Chain => OptimizerDecisionKind::BridgeChainRoot"));
	assert!(actuation.contains("join_order_policy"));
	assert!(actuation.contains("graph_steps"));
	assert!(actuation.contains("step_for(shape.decision_kind())"));
	assert!(!actuation.contains("graph_join_order_soft_margin_step.margin()"));
}

#[test]
pub(crate) fn adr218_graph_actuation_armed_only_by_graph_margin_divergence() {
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let steps = first_block(&join_order, "fn graph_kind_steps_from_gate");

	assert!(resolver.contains("graph_margin_divergence_gate(&snapshot, config)"));
	assert!(steps.contains("gate.status != OptimizerReceiptGateStatus::Actionable"));
	assert!(steps.contains("EffectiveGraphJoinOrderSoftMarginSteps::uniform(family)"));
	assert!(steps.contains("OptimizerDecisionKind::GalaxyRoot => steps.galaxy"));
	assert!(steps.contains("OptimizerDecisionKind::BridgeRoot => steps.bridge"));
	assert!(steps.contains("OptimizerDecisionKind::BridgeChainRoot => steps.bridge_chain"));
	assert!(resolver.contains("preserved_outer_ordering_gate(&snapshot, config)"));
	for forbidden in [
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
		"pub(crate) fn effective_join_order_policy",
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
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let actuation = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");

	assert!(resolver.contains("graph_margin_divergence_gate"));
	assert!(resolver.contains("preserved_outer_ordering_gate"));
	for forbidden in [
		"decision_conflict_pressure_gate",
		"windowing_decay_pressure_gate",
		"counter_handoff_readiness_gate",
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

#[test]
pub(crate) fn adr219_outer_actuation_is_left_outer_star_only() {
	let join_order = source("src/binder/join_order.rs");
	let extractor = first_block(&join_order, "fn extract_left_outer_star_join_candidate");
	let leaf = first_block(&join_order, "fn left_outer_star_base_leaf_label");
	let actuation = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);
	let tests = source("src/binder/tests.rs");

	assert!(extractor.contains("*kind == JoinKind::LeftOuter"));
	assert!(extractor.contains("*strategy_intent == JoinStrategyIntent::Auto"));
	assert!(extractor.contains("left_outer_star_base_leaf_label(right).is_some()"));
	assert!(extractor.contains("dimensions.len() < 2"));
	assert!(extractor.contains("left_outer_star_base_leaf_label(current).is_none()"));
	assert!(leaf.contains("LogicalPlan::ScanNamed"));
	assert!(leaf.contains("LogicalPlan::Alias"));
	assert!(!leaf.contains("LogicalPlan::Filter"));
	assert!(!leaf.contains("LogicalPlan::Project"));
	assert!(actuation.contains("gate.status != OptimizerReceiptGateStatus::Actionable"));
	assert!(tests.contains("preserved_outer_full_right_cross_and_residual_shapes_stay_fenced"));
}

#[test]
pub(crate) fn adr219_outer_actuation_uses_left_outer_sequence_not_inner_targets() {
	let join_order = source("src/binder/join_order.rs");
	let actuation = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);
	let sequence = first_block(&join_order, "fn bind_left_outer_star_order_sequence");
	let preflight = first_block(&join_order, "fn preflight_left_outer_star_order_strategy");
	let inner_sequence = first_block(&join_order, "fn bind_star_order_sequence");

	assert!(actuation.contains("bind_left_outer_star_order_sequence"));
	assert!(actuation.contains("bound_left_outer_star_dimension"));
	assert!(actuation.contains("restore_declared_star_order_project"));
	assert!(sequence.contains("JoinKind::LeftOuter"));
	assert!(preflight.contains("bind_left_outer_star_order_sequence"));
	assert!(inner_sequence.contains("JoinKind::Inner"));
	assert!(!actuation.contains("preserved_outer_order_receipt"));
	assert!(!actuation.contains("edge_targets"));
	assert!(!actuation.contains("inner_equi"));
}

#[test]
pub(crate) fn adr219_preserved_outer_gate_result_shared_by_readout_and_policy() {
	let join_order = source("src/binder/join_order.rs");
	let gate = first_block(&join_order, "pub(crate) fn preserved_outer_ordering_gate");
	let row = first_block(&join_order, "fn preserved_outer_ordering_gate_row");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let tests = source("src/binder/tests.rs");

	assert!(join_order.contains("pub(crate) struct PreservedOuterOrderingGate"));
	assert!(join_order.contains("pub(crate) preserved_outer_gate: PreservedOuterOrderingGate"));
	assert!(gate.contains("PreservedOuterOrderingGate"));
	assert!(gate.contains("status"));
	assert!(gate.contains("advisory: report.advisory"));
	assert!(row.contains("let gate = preserved_outer_ordering_gate(snapshot, config)"));
	assert!(row.contains("gate.status"));
	assert!(row.contains("gate.advisory.sample_size"));
	assert!(
		resolver.contains(
			"let preserved_outer_gate = preserved_outer_ordering_gate(&snapshot, config)"
		)
	);
	assert!(resolver.contains("preserved_outer_gate,"));
	assert!(tests.contains("preserved_outer_actuator_and_receipt_table_share_gate_result"));
}

#[test]
pub(crate) fn adr219_join_order_policy_uses_one_receipt_snapshot() {
	let binder = binder_root_source();
	let join_order = source("src/binder/join_order.rs");
	let binder_new = first_block(&binder, "fn new_with_decision_provenance");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let actuation = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);

	assert_eq!(
		binder_new
			.matches("effective_join_order_policy(config)")
			.count(),
		1
	);
	assert_eq!(resolver.matches("receipt_gate_snapshot()").count(), 1);
	assert!(resolver.contains("JoinOrderSoftMarginApplication::Manual"));
	assert!(resolver.contains("JoinOrderSoftMarginApplication::ApplyAdvisory"));
	assert!(!actuation.contains("receipt_gate_snapshot"));
	assert!(!actuation.contains("optimizer_receipt_gate_rows"));
	assert!(!actuation.contains("information_schema"));
}

#[test]
pub(crate) fn adr219_preserved_outer_policy_uses_gate_recommended_step() {
	let join_order = source("src/binder/join_order.rs");
	let actuation = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);
	let tests = source("src/binder/tests.rs");

	assert!(actuation.contains("gate.advisory.recommended_step.margin()"));
	assert!(!actuation.contains("self.config.join_order_soft_margin_step.margin()"));
	assert!(tests.contains("async fn preserved_outer_policy_uses_gate_recommended_step"));
}

#[test]
pub(crate) fn adr219_manual_and_cold_ledger_are_byte_identical() {
	let tests = source("src/binder/tests.rs");
	let manual = first_block(
		&tests,
		"async fn manual_ignores_preserved_outer_ordering_gate",
	);
	let inert = first_block(
		&tests,
		"async fn preserved_outer_ordering_inert_until_gate_actionable",
	);

	assert!(manual.contains("JoinOrderSoftMarginApplication::Manual"));
	assert!(manual.contains("OptimizerDecisionKind::PreservedOuterOrder"));
	assert!(manual.contains("format!(\"{manual_plan:?}\")"));
	assert!(manual.contains("baseline_trace.join_decisions"));
	assert!(inert.contains("JOIN_ORDER_MARGIN_ADVISORY_MIN_SAMPLE - 1"));
	assert!(inert.contains("OptimizerRuntimeVerdictClass::OverEager"));
	assert!(inert.contains("format!(\"{plan:?}\")"));
	assert!(inert.contains("baseline_trace.join_decisions"));
}

#[test]
pub(crate) fn adr219_outer_actuation_does_not_record_inner_decisions() {
	let join_order = source("src/binder/join_order.rs");
	let actuation = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);
	let binder = binder_root_source();

	assert!(!actuation.contains("accrue_star_order_receipt"));
	assert!(!actuation.contains("accrue_star_order_decline"));
	assert!(!actuation.contains("accrue_outer_order_decision"));
	assert!(!binder.contains("accrue_outer_order_decision"));
}

#[test]
pub(crate) fn adr219_no_new_durable_key_counter_table_kind_or_decision_matrix() {
	let binder = binder_root_source();
	let execution = source("src/ir/execution.rs");
	let decision_log = source("src/binder/decision_log.rs");
	let runtime_evidence = source("src/binder/runtime_evidence.rs");
	let accrual = source("tests/architecture/optimizer_accrual.rs");
	let system_table_kind = first_block(&execution, "pub enum SystemTableKind");
	let recording = first_block(&decision_log, "pub(crate) fn decision_recording_kinds");

	assert!(!recording.contains("PreservedOuterOrder"));
	assert!(!runtime_evidence.contains("preserved_outer_ordering/"));
	assert!(!runtime_evidence.contains("outer_order_decision"));
	assert!(!binder.contains("INFORMATION_SCHEMA_OPTIMIZER_PRESERVED_OUTER"));
	assert_eq!(
		system_table_kind
			.matches("InformationSchemaOptimizerReceiptGates")
			.count(),
		1
	);
	assert!(accrual.contains("adr216_decision_matrix_is_140_keys_and_evidence_matrix_220"));
	assert!(accrual.contains("decision_keys.len(), 140"));
	assert!(accrual.contains("decision_keys.len() + verdict_keys.len(), 220"));
}

#[test]
pub(crate) fn adr219_no_executor_fragmenter_wire_or_wendelon_surface() {
	let manifest = manifest_dir();
	let offenders = [
		manifest.join("src/exec.rs"),
		manifest.join("src/distributed"),
		manifest.join("../wendelon/src"),
	]
	.into_iter()
	.flat_map(|path| source_lines_for_path(&path))
	.filter(|(_, _, line)| {
		line.contains("try_bind_preserved_outer_star_actuation")
			|| line.contains("PreservedOuterOrderingGate")
			|| line.contains("preserved_outer_ordering")
	})
	.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR219 leaked outside binder policy surfaces:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr219_decision_conflict_gate_remains_non_consumer() {
	let join_order = source("src/binder/join_order.rs");
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let graph = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");
	let outer = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);

	assert!(!resolver.contains("decision_conflict_pressure_gate"));
	assert!(!resolver.contains("decision_conflict_pressure"));
	assert!(!graph.contains("decision_conflict"));
	assert!(!outer.contains("decision_conflict"));
}

#[test]
pub(crate) fn adr220_decision_conflict_readout_is_read_only_no_policy_consumers() {
	let join_order = source("src/binder/join_order.rs");
	let binder = binder_root_source();
	let resolver = first_block(&join_order, "pub(crate) fn effective_join_order_policy");
	let graph = first_block(&join_order, "pub(super) async fn try_bind_graph_actuation");
	let outer = first_block(
		&join_order,
		"pub(super) async fn try_bind_preserved_outer_star_actuation",
	);
	let readout = first_block(
		&join_order,
		"pub(crate) fn optimizer_decision_conflict_rows",
	);
	let gate = first_block(&join_order, "fn decision_conflict_pressure_gate");

	assert!(binder.contains("INFORMATION_SCHEMA_OPTIMIZER_DECISION_CONFLICTS_TABLE"));
	assert!(readout.contains("decision_conflict_reports(&snapshot)"));
	assert!(gate.contains("decision_conflict_reports(snapshot)"));
	for policy in [resolver, graph, outer] {
		assert!(!policy.contains("optimizer_decision_conflict_rows"));
		assert!(!policy.contains("decision_conflict_reports"));
		assert!(!policy.contains("decision_conflict_pressure"));
		assert!(!policy.contains("DecisionConflictReport"));
		assert!(!policy.contains("memo"));
		assert!(!policy.contains("Cascades"));
	}
}
