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
pub(crate) fn adr178_star_order_actuation_is_planning_time_not_explain_rendered() {
	let binder = binder_source();
	let bind_with_explain_trace = first_block(&binder, "async fn bind_with_explain_trace");
	let bind_plan = first_block(&binder, "fn bind_plan_with_trace");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let join_order = source("src/binder/join_order.rs");

	assert!(bind_plan.contains("try_bind_star_order_actuation(plan, trace, render_trace)"));
	assert!(actuator.contains("extract_star_join_candidate(plan)"));
	assert!(!actuator.contains("analyze("));
	assert!(join_order.contains("pub(super) fn extract_star_join_candidate"));
	assert!(bind_with_explain_trace.contains("if render_trace.join_order.is_none()"));
	assert!(!join_order.contains("ExecutionPlan::"));
	assert!(!join_order.contains("restore_declared_star_order_project"));
}

#[test]
pub(crate) fn adr178_join_order_scoring_stays_in_join_order_model() {
	let binder = binder_source();
	let bind_scoped_join = first_block(&binder, "fn bind_scoped_join");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let join_order = source("src/binder/join_order.rs");

	assert!(join_order.contains("pub(super) fn analyze"));
	assert!(join_order.contains("pub(super) fn order_cost"));
	assert!(join_order.contains("pub(super) fn classify_star_order_action"));
	assert!(join_order.contains("STAR_ORDER_SOFT_MARGIN_NUMERATOR"));
	assert!(join_order.contains("STAR_ORDER_SOFT_MARGIN_DENOMINATOR"));
	assert!(join_order.contains("StarOrderOutcome::Recognized"));
	assert!(join_order.contains("StarOrderOutcome::Declined"));
	assert!(actuator.contains("recognized_star_order_outcome("));
	assert!(!bind_scoped_join.contains("order_cost("));
	assert!(!bind_scoped_join.contains("cost_preferred_order"));
}

#[test]
pub(crate) fn adr178_actuation_reuses_existing_join_estimate_channels() {
	let binder = binder_source();
	let bind_scoped_join = first_block(&binder, "fn bind_scoped_join");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let estimate_helper = first_block(&binder, "fn join_estimated_row_count_for_keys");
	let join_order = source("src/binder/join_order.rs");

	assert!(bind_scoped_join.contains("Self::join_estimated_row_count_for_keys("));
	assert!(actuator.contains("Self::join_estimated_row_count_for_keys("));
	assert!(actuator.contains("join_order_edge_estimate("));
	assert!(estimate_helper.contains("join_output_ndv_containment_estimate("));
	assert!(estimate_helper.contains("multi_key_join_cardinality("));
	assert!(join_order.contains("actual_join_estimate"));
	assert!(join_order.contains("return (\n\t\t\tactual_join_estimate,"));
}

#[test]
pub(crate) fn adr178_broadcast_star_only_respects_scan_linear_probe_fence() {
	let binder = binder_source();
	let bind_scoped_join = first_block(&binder, "fn bind_scoped_join");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let join_order = source("src/binder/join_order.rs");

	assert!(
		bind_scoped_join.contains(
			"let left_probe_scan_linear = broadcast_probe_is_scan_linear(&left_plan.plan);"
		)
	);
	assert!(bind_scoped_join.contains("probe_scan_linear: left_probe_scan_linear"));
	assert!(actuator.contains("broadcast_probe_is_scan_linear(&probe_plan.plan)"));
	assert!(join_order.contains("JoinPhysicalPlacement::SwappedRightBuild"));
	assert!(join_order.contains("MixedStrategyStar"));
	assert!(join_order.contains("edge.probe_scan_linear"));
	assert!(join_order.contains("StarOrderDeclineReason::ProbeNotScanLinear"));
}

#[test]
pub(crate) fn adr178_source_capture_is_split_from_explain_rendering() {
	let binder = binder_source();
	let bind_with_trace = first_block(&binder, "pub(crate) async fn bind_with_trace");
	let bind_with_explain_trace = first_block(&binder, "async fn bind_with_explain_trace");
	let bind_scoped_join = first_block(&binder, "fn bind_scoped_join");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");

	assert!(bind_with_trace.contains("let mut render_trace = None;"));
	assert!(bind_with_trace.contains("JoinOrderSourceCapture::Off"));
	assert!(bind_with_explain_trace.contains("Some(OptimizerExplainRenderTrace::default())"));
	assert!(bind_with_explain_trace.contains("JoinOrderSourceCapture::On"));
	assert!(binder.contains("join_order_source_capture.enabled().then(||"));
	assert!(binder.contains("JoinOrderSourceCapture::On"));
	assert!(bind_scoped_join.contains("if let Some(render_trace) = render_trace.as_mut()"));
	assert!(bind_scoped_join.contains("render_trace.join_order_edges.push(JoinOrderEdgeEvidence"));
	assert!(actuator.contains("render_trace.join_order = Some("));
}

#[test]
pub(crate) fn adr178_reuses_join_constructor_and_restores_declared_output_shape() {
	let binder = binder_source();
	let join = source("src/binder/join.rs");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let sequence = first_block(&binder, "fn bind_star_order_sequence");
	let bind_join = first_block(&binder, "async fn bind_join");
	let bind_scoped_join = first_block(&binder, "fn bind_scoped_join");

	assert!(bind_join.contains("self.bind_scoped_join("));
	assert!(sequence.contains("self.bind_scoped_join("));
	assert!(actuator.contains("restore_declared_star_order_project("));
	assert!(actuator.contains("star_output_segments_for_order("));
	assert!(join.contains("pub(super) struct StarOutputSegment"));
	assert!(join.contains("pub(super) fn restore_declared_star_order_project"));
	assert!(join.contains("decode_column_logical_type(field)"));
	assert!(bind_scoped_join.contains("restore_declared_join_order_project("));
}

#[test]
pub(crate) fn adr178_no_optimizer_shell_or_planproperties_contract() {
	let manifest = manifest_dir();
	assert!(!manifest.join("src/optimizer.rs").exists());
	assert!(!manifest.join("src/optimizer").exists());

	let src = source_lines(&manifest.join("src"));
	let offenders = src
		.into_iter()
		.filter(|(_, _, line)| {
			let trimmed = line.trim_start();
			trimmed.starts_with("struct Optimizer ")
				|| trimmed.starts_with("pub struct Optimizer ")
				|| trimmed.starts_with("enum Optimizer ")
				|| trimmed.starts_with("pub enum Optimizer ")
				|| line.contains("PlanProperties")
				|| line.contains("Cascades")
				|| trimmed.starts_with("struct Memo ")
				|| trimmed.starts_with("struct Memo{")
				|| trimmed.starts_with("pub struct Memo ")
				|| trimmed.starts_with("pub struct Memo{")
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR177 introduced an optimizer shell/property contract:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr178_no_executor_fragment_storage_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = [
		"JoinOrderEdgeEvidence",
		"StarOrderOutcome",
		"StarOrderEvidence",
		"join_order_edges",
		"join_order::",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/distributed/fragment.rs"),
		manifest.join("src/catalog.rs"),
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
		"ADR177 join-order evidence leaked outside binder/system-table surface:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr178_cockpit_is_frontend_shared() {
	let binder = binder_source();
	let rune = source("src/rune_engine.rs");
	let explain_plan = first_block(&binder, "fn optimizer_explain_plan");
	let bind_explain = first_block(&binder, "async fn bind_explain(&self");
	let rune_explain = first_block(&rune, "pub fn explain");

	assert!(explain_plan.contains("optimizer_explain_join_order_row"));
	assert!(bind_explain.contains("optimizer_explain_plan(trace, Some(render_trace))"));
	assert!(rune_explain.contains("LogicalPlan::Explain"));
	assert!(!rune.contains("JoinOrderEdgeEvidence"));
	assert!(!rune.contains("StarOrderOutcome"));
	assert!(!rune.contains("join_order_edges"));
}

#[test]
pub(crate) fn adr179_explain_analyze_is_implemented_not_rejected() {
	let planner = source("src/sql_planner.rs");
	let coordinator = source("src/coordinator.rs");
	let exec = source("src/exec.rs");
	let lower_explain = first_block(&planner, "fn lower_explain_statement");
	let local = first_block(&coordinator, "async fn execute_explain_analyze(");
	let distributed = first_block(&coordinator, "async fn execute_explain_analyze_distributed");

	assert!(lower_explain.contains("LogicalPlan::ExplainAnalyze"));
	assert!(!planner.contains("EXPLAIN ANALYZE is not supported"));
	assert!(local.contains("bind_explain_analyze(input, catalog, &self.config).await?"));
	assert!(distributed.contains("bind_explain_analyze(input, catalog, planner_config).await?"));
	assert!(exec.contains("unbound EXPLAIN ANALYZE execution"));
}

#[test]
pub(crate) fn adr179_runtime_evidence_is_observational() {
	let exec = source("src/exec.rs");
	let coordinator = source("src/coordinator.rs");
	let recorder = first_block(&exec, "fn record_join_runtime_observation");
	let local = first_block(&coordinator, "async fn execute_explain_analyze(");

	assert!(recorder.contains("if !mode.measures(observation.replay_id)"));
	assert!(recorder.contains("task.join_observations.push(observation)"));
	assert!(!recorder.contains("JoinStrategy::"));
	assert!(!recorder.contains("ExecutionPlan::"));
	assert!(local.contains("let _results = executor"));
	assert!(local.contains("render_explain_analyze(binding, &observations)?"));
}

#[test]
pub(crate) fn adr179_measures_does_not_act() {
	let binder = binder_source();
	let exec = source("src/exec.rs");
	let chooser = first_block(&binder, "fn choose_join_strategy");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");

	assert!(!binder.contains("JoinRuntimeObservationMode"));
	assert!(!chooser.contains("JoinRuntimeObservation"));
	assert!(!actuator.contains("JoinRuntimeObservation"));
	assert!(exec.contains("JoinRuntimeObservation"));
	assert!(!exec.contains("choose_join_strategy"));
	assert!(!exec.contains("try_bind_star_order_actuation"));
}

#[test]
pub(crate) fn adr179_actual_correlates_via_replay_id() {
	let binder = binder_source();
	let task = source("src/distributed/task.rs");
	let exec = source("src/exec.rs");
	let render = first_block(&binder, "fn optimizer_analyze_join_order_row");
	let measurement = first_block(&binder, "fn join_order_runtime_measurement");
	let replay_ids = first_block(&binder, "fn star_order_runtime_replay_ids");
	let execute_hash_join = first_block(&exec, "async fn execute_hash_join");

	assert!(task.contains("pub replay_id: JoinReplayId"));
	assert!(replay_ids.contains("edge.target.join_index"));
	assert!(replay_ids.contains(".map(JoinReplayId)"));
	assert!(render.contains("join_order_runtime_measurement(edge"));
	assert!(measurement.contains("join_replay_id_for_trace_target(edge.target)?"));
	assert!(measurement.contains("summaries.get(&replay_id)"));
	assert!(execute_hash_join.contains("JoinRuntimeObservation {\n\t\t\t\t\treplay_id,"));
	assert!(execute_hash_join.contains("output_rows: RowCount::new(record_batch_row_count"));
}

#[test]
pub(crate) fn adr179_reuses_178_estimate_no_second_estimator() {
	let binder = binder_source();
	let analyze_plan = first_block(&binder, "fn optimizer_analyze_plan");
	let analyze_rows = first_block(&binder, "fn optimizer_analyze_rows");
	let analyze_row = first_block(&binder, "fn optimizer_analyze_join_order_row");

	assert!(analyze_row.contains("let estimated_rows = dimension.estimated_rows;"));
	assert!(analyze_row.contains("dimension.estimate_source"));
	for forbidden in [
		"join_output_ndv_containment_estimate",
		"multi_key_join_cardinality",
		"join_estimated_row_count_for_keys",
		"estimate_join_order_edge_from_sources",
		"join_order_edge_estimate",
	] {
		assert!(!analyze_plan.contains(forbidden));
		assert!(!analyze_rows.contains(forbidden));
		assert!(!analyze_row.contains(forbidden));
	}
}

#[test]
pub(crate) fn adr179_capture_is_cheap_and_analyze_gated() {
	let exec = source("src/exec.rs");
	let coordinator = source("src/coordinator.rs");
	let context_new = first_block(&exec, "pub fn new(");
	let recorder = first_block(&exec, "fn record_join_runtime_observation");
	let local = first_block(&coordinator, "async fn execute_explain_analyze(");
	let distributed = first_block(&coordinator, "async fn execute_explain_analyze_distributed");

	assert!(context_new.contains("JoinRuntimeObservationMode::Disabled"));
	assert!(
		recorder.find("mode.measures").expect("measure gate")
			< recorder
				.find("record_task_telemetry")
				.expect("telemetry lock path")
	);
	assert!(local.contains(".with_join_runtime_observation_mode(analyze_observation_mode("));
	assert!(distributed.contains(".with_join_runtime_observation_mode(analyze_observation_mode("));
}

#[test]
pub(crate) fn adr179_capture_allowlist_is_replay_id_only() {
	let task = source("src/distributed/task.rs");
	let mode = first_block(&task, "pub enum JoinRuntimeObservationMode");
	let measures = first_block(&task, "impl JoinRuntimeObservationMode");

	assert!(mode.contains("MeasureJoins"));
	assert!(mode.contains("replay_ids: BTreeSet<JoinReplayId>"));
	assert!(measures.contains("replay_ids.contains(&replay_id)"));
	for forbidden in ["table", "dimension", "predicate", "column", "relation"] {
		assert!(
			!mode.contains(forbidden),
			"runtime capture allowlist must remain replay-id-only: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr179_no_star_order_vocabulary_in_exec() {
	let manifest = manifest_dir();
	let markers = [
		"JoinOrderEdgeEvidence",
		"StarOrderOutcome",
		"StarOrderEvidence",
		"StarOrderAction",
		"join_order_runtime",
		"dimension=",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/distributed/task.rs"),
		manifest.join("src/distributed/fragment.rs"),
		manifest.join("src/distributed/scheduler.rs"),
		manifest.join("src/server/mod.rs"),
	] {
		offenders.extend(
			source_lines_for_path(&path)
				.into_iter()
				.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
		);
	}
	assert!(
		offenders.is_empty(),
		"ADR179 leaked star-order vocabulary below binder/explain:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr179_no_fragment_or_wendelon_storage_pbc_change() {
	let manifest = manifest_dir();
	let fragment = source("src/distributed/fragment.rs");
	let markers = [
		"JoinRuntimeObservation",
		"JoinRuntimeObservationMode",
		"join_runtime_observation",
		"join_observations",
		"join_order_runtime",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/catalog.rs"),
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
	assert!(fragment.contains("join_runtime_observation_mode"));
	assert!(!fragment.contains("JoinOrderEdgeEvidence"));
	assert!(!fragment.contains("StarOrderOutcome"));
	assert!(
		offenders.is_empty(),
		"ADR179 runtime evidence changed storage/PBC/Wendelon surfaces:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr180_verdict_reuses_178_order_model_no_second_estimator() {
	let join_order = source("src/binder/join_order.rs");
	let helper = first_block(&join_order, "pub(super) fn measured_star_order_verdict");

	assert!(join_order.contains("pub(super) fn order_cost"));
	assert!(helper.contains("recognized_star_order_outcome("));
	assert!(helper.contains("evidence.confidence"));
	for forbidden in [
		"join_output_ndv_containment_estimate",
		"multi_key_join_cardinality",
		"join_estimated_row_count_for_keys",
		"estimate_join_order_edge_from_sources",
		"join_order_edge_estimate",
		"filter_range_selectivity_estimated_row_count",
	] {
		assert!(
			!helper.contains(forbidden),
			"ADR180 measured verdict must not call estimator {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr180_measured_helper_sets_applied_order_override() {
	let join_order = source("src/binder/join_order.rs");
	let helper = first_block(&join_order, "pub(super) fn measured_star_order_verdict");
	let recognized = first_block(&join_order, "pub(super) fn recognized_star_order_outcome");

	assert!(recognized.contains("unwrap_or_else(|| ranking.declared_order.clone())"));
	assert!(helper.contains("let measured_applied_order = applied_order_for_action(&measured);"));
	assert!(helper.contains("Some(measured_applied_order.clone())"));
	assert!(helper.contains("measured_applied_order,"));
}

#[test]
pub(crate) fn adr180_verdict_compares_star_evidence_applied_order() {
	let join_order = source("src/binder/join_order.rs");
	let helper = first_block(&join_order, "pub(super) fn measured_star_order_verdict");

	assert!(helper.contains("evidence.applied_order == measured.applied_order"));
	assert!(helper.contains("StarOrderVerdict::Vindicated"));
	assert!(helper.contains("StarOrderVerdict::MisRanked"));
	assert!(helper.contains("StarOrderVerdict::MissedWin"));
	assert!(helper.contains("StarOrderVerdict::OverEager"));
}

#[test]
pub(crate) fn adr180_verdict_uses_runtime_output_rows() {
	let explain = source("src/binder/explain.rs");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");
	let verdict_row = first_block(&explain, "fn optimizer_analyze_join_order_verdict_row");

	assert!(analyze_rows.contains("summary.output_rows"));
	assert!(!analyze_rows.contains("summary.build_rows"));
	assert!(!analyze_rows.contains("summary.probe_rows"));
	assert!(
		verdict_row.contains("measured_star_order_verdict(evidence, measured_rows_by_dimension)")
	);
}

#[test]
pub(crate) fn adr180_verdict_is_interpretation_only() {
	let explain = source("src/binder/explain.rs");
	let join_order = source("src/binder/join_order.rs");
	let verdict_row = first_block(&explain, "fn optimizer_analyze_join_order_verdict_row");
	let helper = first_block(&join_order, "pub(super) fn measured_star_order_verdict");

	assert!(verdict_row.contains("join_order_verdict"));
	assert!(verdict_row.contains("ScalarValue::Int64(None)"));
	assert!(!verdict_row.contains("ExecutionPlan::"));
	assert!(!verdict_row.contains("PlannerConfig"));
	assert!(!helper.contains("ExecutionPlan::"));
	assert!(!helper.contains("PlannerConfig"));
	assert!(!helper.contains("JoinRuntimeObservationMode"));
}

#[test]
pub(crate) fn adr180_verdict_does_not_act() {
	let coordinator = source("src/coordinator.rs");
	let explain = source("src/binder/explain.rs");
	let join_order = source("src/binder/join_order.rs");
	let render = first_block(&explain, "fn optimizer_analyze_join_order_verdict_row");
	let helper = first_block(&join_order, "pub(super) fn measured_star_order_verdict");

	for forbidden in [
		"force_hash_join_ids",
		"PlannerConfig",
		"broadcast_cap_replay_trigger",
		"force_declared_join_order",
		"spill_replay_trigger",
	] {
		assert!(!render.contains(forbidden), "render acted via {forbidden}");
		assert!(!helper.contains(forbidden), "helper acted via {forbidden}");
	}
	assert!(coordinator.contains("broadcast_cap_replay_trigger"));
	assert!(!explain.contains("broadcast_cap_replay_trigger"));
}

#[test]
pub(crate) fn adr180_runtime_correlation_remains_replay_id_keyed() {
	let explain = source("src/binder/explain.rs");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");
	let runtime_row = first_block(&explain, "fn optimizer_analyze_join_order_row");
	let measurement = first_block(&explain, "fn join_order_runtime_measurement");

	assert!(analyze_rows.contains("join_replay_id_for_trace_target(edge.target)?"));
	assert!(analyze_rows.contains("summaries.get(&replay_id)"));
	assert!(runtime_row.contains("join_order_runtime_measurement(edge"));
	assert!(measurement.contains("join_replay_id_for_trace_target(edge.target)?"));
	assert!(measurement.contains("summaries.get(&replay_id)"));
}

#[test]
pub(crate) fn adr180_no_exec_or_fragment_change() {
	let manifest = manifest_dir();
	let markers = [
		"join_order_verdict",
		"StarOrderVerdict",
		"StarOrderVerdictReceipt",
		"StarOrderDimAccuracy",
		"MeasuredOrderDiffers",
		"MissedWin",
		"OverEager",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/distributed/task.rs"),
		manifest.join("src/distributed/fragment.rs"),
		manifest.join("src/distributed/scheduler.rs"),
		manifest.join("src/server/mod.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/catalog.rs"),
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
		"ADR180 verdict vocabulary leaked below binder/explain or across storage boundary:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr180_rune_and_sql_share_analyze_renderer() {
	let binder = binder_source();
	let rune = source("src/rune_engine.rs");
	let render_explain_analyze = first_block(&binder, "pub(crate) fn render_explain_analyze");

	assert!(render_explain_analyze.contains("optimizer_analyze_plan"));
	assert!(rune.contains("LogicalPlan::ExplainAnalyze"));
	assert!(rune.contains("pub fn analyze"));
	assert!(!rune.contains("join_order_verdict"));
	assert!(!rune.contains("StarOrderVerdict"));
}

#[test]
pub(crate) fn adr180_no_optimizer_shell() {
	adr179_no_optimizer_shell();
}

#[test]
pub(crate) fn adr181_uniform_strategy_recognizer() {
	let join_order = source("src/binder/join_order.rs");
	let strategy = first_block(&join_order, "pub(super) enum StarOrderStrategy");
	let decline = first_block(&join_order, "pub(super) enum StarOrderDeclineReason");
	let evidence = first_block(&join_order, "pub(super) struct StarOrderEvidence");
	let uniform = first_block(&join_order, "pub(super) fn uniform_star_order_strategy");

	assert_eq!(enum_variant_count(&strategy), 2);
	assert!(strategy.contains("Broadcast"));
	assert!(strategy.contains("HashPartitioned"));
	assert!(evidence.contains("pub(super) strategy: StarOrderStrategy"));
	assert!(decline.contains("MixedStrategyStar"));
	assert!(!decline.contains("NotBroadcastStar"));
	assert!(uniform.contains("decisions.is_empty()"));
	assert!(uniform.contains("JoinStrategy::Broadcast"));
	assert!(uniform.contains("JoinPhysicalPlacement::SwappedRightBuild"));
	assert!(uniform.contains("StarOrderStrategy::Broadcast"));
	assert!(uniform.contains("JoinStrategy::HashPartitioned"));
	assert!(uniform.contains("JoinPhysicalPlacement::Natural"));
	assert!(uniform.contains("StarOrderStrategy::HashPartitioned"));
}

#[test]
pub(crate) fn adr181_uniform_strategy_preflights_declared_and_selected() {
	let binder = binder_source();
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let preflight = first_block(&binder, "fn preflight_star_order_strategy");

	assert!(actuator.contains("let Some(declared_strategy)"));
	assert!(actuator.contains("preflight_star_order_strategy("));
	assert!(actuator.contains("&declared_indices"));
	assert!(actuator.contains("let Some(selected_strategy)"));
	assert!(actuator.contains("&selected_indices"));
	assert!(actuator.contains("if declared_strategy != selected_strategy"));
	assert!(actuator.contains("StarOrderDeclineReason::MixedStrategyStar"));
	assert!(actuator.contains("let star_order_strategy = selected_strategy;"));
	assert!(preflight.contains("let mut preflight_trace = base_trace.clone();"));
	assert!(preflight.contains("let preflight_join_start = preflight_trace.join_decisions.len();"));
	assert!(preflight.contains("let mut no_render_trace = None;"));
	assert!(preflight.contains("self.bind_star_order_sequence("));
	assert!(preflight.contains("if preflight_decisions.len() != order.len()"));
	assert!(preflight.contains("uniform_star_order_strategy(preflight_decisions)"));
}

#[test]
pub(crate) fn adr181_reorder_is_strategy_invariant() {
	let binder = binder_source();
	let join_order = source("src/binder/join_order.rs");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let recognized = first_block(&join_order, "pub(super) fn recognized_star_order_outcome");
	let ranking = first_block(&join_order, "pub(super) fn rank_star_order");
	let order_cost = first_block(&join_order, "pub(super) fn order_cost");
	let classify = first_block(&join_order, "pub(super) fn classify_star_order_action");

	assert!(actuator.contains("preferred_indices.sort_by"));
	assert!(actuator.contains("restore_declared_star_order_project("));
	assert!(actuator.contains("star_output_segments_for_order("));
	assert!(actuator.contains("recognized_star_order_outcome("));
	assert!(actuator.contains("star_order_strategy,"));
	assert!(recognized.contains("strategy: StarOrderStrategy"));
	assert!(recognized.contains("strategy,"));
	assert!(recognized.contains("rank_star_order("));
	assert!(ranking.contains("classify_star_order_action("));
	assert!(!ranking.contains("StarOrderStrategy"));
	assert!(!ranking.contains("JoinStrategy::"));
	assert!(!order_cost.contains("StarOrderStrategy"));
	assert!(!order_cost.contains("JoinStrategy::"));
	assert!(!classify.contains("StarOrderStrategy"));
	assert!(!classify.contains("JoinStrategy::"));
}

#[test]
pub(crate) fn adr181_hash_star_order_is_binder_only() {
	let manifest = manifest_dir();
	let markers = [
		"StarOrderStrategy",
		"MixedStrategyStar",
		"uniform_star_order_strategy",
		"preflight_star_order_strategy",
		"strategy=hash_partitioned",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/catalog.rs"),
		manifest.join("src/coordinator.rs"),
		manifest.join("src/sql_planner.rs"),
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
		"ADR181 hash-star order vocabulary leaked outside binder/explain:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr181_reuses_178_restore_and_179_180_net() {
	let binder = binder_source();
	let explain = source("src/binder/explain.rs");
	let join = source("src/binder/join.rs");
	let join_order = source("src/binder/join_order.rs");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");
	let runtime_row = first_block(&explain, "fn optimizer_analyze_join_order_row");
	let measurement = first_block(&explain, "fn join_order_runtime_measurement");
	let verdict_row = first_block(&explain, "fn optimizer_analyze_join_order_verdict_row");
	let verdict = first_block(&join_order, "pub(super) fn measured_star_order_verdict");

	assert!(actuator.contains("restore_declared_star_order_project("));
	assert!(join.contains("pub(super) fn restore_declared_star_order_project"));
	assert!(analyze_rows.contains("join_replay_id_for_trace_target(edge.target)?"));
	assert!(runtime_row.contains("join_order_runtime_measurement(edge"));
	assert!(measurement.contains("join_replay_id_for_trace_target(edge.target)?"));
	assert!(
		verdict_row.contains("measured_star_order_verdict(evidence, measured_rows_by_dimension)")
	);
	assert!(verdict.contains("evidence.strategy"));
	assert!(explain.contains("strategy={}; applied_order"));
	assert!(explain.contains("strategy={}; dimensions"));
	assert!(!binder.contains("HashStarRuntimeObservation"));
	assert!(!join_order.contains("HashStarRuntimeObservation"));
	for forbidden in [
		"join_output_ndv_containment_estimate",
		"multi_key_join_cardinality",
		"join_estimated_row_count_for_keys",
		"estimate_join_order_edge_from_sources",
		"join_order_edge_estimate",
	] {
		assert!(!verdict.contains(forbidden));
		assert!(!verdict_row.contains(forbidden));
	}
}

#[test]
pub(crate) fn adr181_strict_star_leaf_shape_still_defers_snowflake_and_bushy_ordering() {
	let join_order = source("src/binder/join_order.rs");
	let extract = first_block(&join_order, "pub(super) fn extract_star_join_candidate");

	assert!(extract.contains("&& !logical_plan_contains_join(right)"));
	assert!(extract.contains("dimensions.len() < 2 || logical_plan_contains_join(current)"));
	assert!(join_order.contains("fn logical_plan_contains_join"));
}

#[test]
pub(crate) fn adr182_snowflake_recognition_is_observational() {
	let binder = binder_source();
	let join_order = source("src/binder/join_order.rs");
	let bind_with_explain_trace = first_block(&binder, "async fn bind_with_explain_trace");
	let bind_join = first_block(&binder, "async fn bind_join");
	let bind_scoped_join = first_block(&binder, "fn bind_scoped_join");
	let choose = first_block(&binder, "fn choose_join_strategy");
	let analyzer = first_block(&join_order, "pub(super) fn analyze_snowflake");
	let extract = first_block(&join_order, "pub(super) fn extract_star_join_candidate");

	assert!(
		bind_with_explain_trace
			.contains("analyze_snowflake(&render_trace.join_order_edges, &trace.join_decisions)")
	);
	assert!(bind_with_explain_trace.contains("render_trace.join_order = None"));
	assert!(!bind_join.contains("snowflake_order"));
	assert!(!bind_scoped_join.contains("snowflake_order"));
	assert!(!choose.contains("snowflake_order"));
	assert!(!extract.contains("snowflake_order"));
	for forbidden in [
		"ExecutionPlan::",
		"LogicalPlan::",
		"bind_scoped_join(",
		"choose_join_strategy(",
		"restore_declared_star_order_project(",
	] {
		assert!(
			!analyzer.contains(forbidden),
			"snowflake recognizer must be read-only; found {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr182_snowflake_reconstructs_from_existing_edges() {
	let join_order = source("src/binder/join_order.rs");
	let edge = first_block(&join_order, "pub(super) struct JoinOrderEdgeEvidence");
	let analyzer = first_block(&join_order, "pub(super) fn analyze_snowflake");
	let level = first_block(&join_order, "fn snowflake_order_level");

	for field in [
		"target: JoinTraceTarget",
		"probe: Option<String>",
		"dimension: Option<String>",
		"declared_position: usize",
		"estimated_rows: Option<RowCount>",
		"estimate_source: JoinOrderEstimateSource",
		"inner_equi: bool",
		"probe_scan_linear: bool",
	] {
		assert!(edge.contains(field));
	}
	assert!(!edge.contains("snowflake"));
	assert!(edge.contains("probe: Option<String>"));
	assert!(edge.contains("dimension: Option<String>"));
	assert!(edge.contains("target: JoinTraceTarget"));
	assert!(analyzer.contains("edge.probe.as_ref()?"));
	assert!(analyzer.contains("edge.dimension.as_ref()?"));
	assert!(analyzer.contains("adjacency.entry(probe.clone()).or_default().push(edge_index)"));
	assert!(analyzer.contains(".any(|dimension| probes.contains(dimension))"));
	assert!(level.contains("adjacency.contains_key(dimension)"));
}

#[test]
pub(crate) fn adr182_snowflake_uses_strategy_free_ranking_not_fake_strategy() {
	let join_order = source("src/binder/join_order.rs");
	let snowflake = first_block(&join_order, "pub(super) struct SnowflakeOrderEvidence");
	let level = first_block(&join_order, "fn snowflake_order_level");
	let recognized = first_block(&join_order, "pub(super) fn recognized_star_order_outcome");

	assert!(join_order.contains("pub(super) struct StarOrderRanking"));
	assert!(join_order.contains("pub(super) fn rank_star_order"));
	assert!(snowflake.contains("ranking: StarOrderRanking"));
	assert!(!snowflake.contains("StarOrderEvidence"));
	assert!(level.contains("rank_star_order("));
	assert!(!level.contains("recognized_star_order_outcome("));
	assert!(!level.contains("StarOrderStrategy::Broadcast"));
	assert!(recognized.contains("rank_star_order("));
}

#[test]
pub(crate) fn adr182_snowflake_records_strategy_as_evidence_not_gate() {
	let join_order = source("src/binder/join_order.rs");
	let edge_strategy = first_block(&join_order, "pub(super) struct SnowflakeEdgeStrategy");
	let analyzer = first_block(&join_order, "pub(super) fn analyze_snowflake");
	let level = first_block(&join_order, "fn snowflake_order_level");

	assert!(edge_strategy.contains("strategy: JoinStrategy"));
	assert!(edge_strategy.contains("placement: JoinPhysicalPlacement"));
	assert!(edge_strategy.contains("probe_scan_linear: bool"));
	assert!(analyzer.contains("decisions.get(edge.target.join_index)?"));
	assert!(level.contains("decisions.get(edge.target.join_index)?"));
	assert!(level.contains("strategy: decision.strategy"));
	assert!(level.contains("placement: decision.placement"));
	assert!(level.contains("probe_scan_linear: edge.probe_scan_linear"));
	assert!(!analyzer.contains("uniform_star_order_strategy"));
	assert!(!level.contains("uniform_star_order_strategy"));
	assert!(!analyzer.contains("MixedStrategyStar"));
	assert!(!level.contains("MixedStrategyStar"));
}

#[test]
pub(crate) fn adr182_snowflake_supersedes_decline_without_gating_runtime() {
	let binder = binder_source();
	let explain = source("src/binder/explain.rs");
	let bind_with_explain_trace = first_block(&binder, "async fn bind_with_explain_trace");
	let runtime = first_block(&binder, "fn star_order_runtime_replay_ids");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");

	assert!(bind_with_explain_trace.contains("render_trace.snowflake_order ="));
	assert!(bind_with_explain_trace.contains("render_trace.join_order = None"));
	assert!(runtime.contains("render_trace.join_order"));
	assert!(runtime.contains("render_trace.snowflake_order.is_none()"));
	assert!(analyze_rows.contains("render_trace.snowflake_order.as_ref()"));
	assert!(analyze_rows.contains("optimizer_analyze_snowflake_order_row(evidence)"));
	assert!(analyze_rows.contains("optimizer_analyze_snowflake_runtime_row"));
	assert!(explain.contains("\"join_order_snowflake\""));
}

#[test]
pub(crate) fn adr183_snowflake_runtime_reuses_179_measurement() {
	let explain = source("src/binder/explain.rs");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");
	let runtime_row = first_block(&explain, "fn optimizer_analyze_snowflake_runtime_row");
	let measurement = first_block(&explain, "fn join_order_runtime_measurement");

	assert!(analyze_rows.contains("optimizer_analyze_snowflake_runtime_row"));
	assert!(runtime_row.contains("join_order_runtime_measurement(edge"));
	assert!(runtime_row.contains("JoinRuntimeObservationSummary"));
	assert!(measurement.contains("join_replay_id_for_trace_target(edge.target)?"));
	assert!(measurement.contains("summary.output_rows"));
	assert!(!explain.contains("SnowflakeRuntimeObservation"));
	assert!(!explain.contains("SnowflakeOptimizerFact"));
}

#[test]
pub(crate) fn adr183_snowflake_runtime_replay_ids_from_edges() {
	let binder = binder_source();
	let join_order = source("src/binder/join_order.rs");
	let runtime = first_block(&binder, "fn star_order_runtime_replay_ids");
	let edge_strategy = first_block(&join_order, "pub(super) struct SnowflakeEdgeStrategy");

	assert!(runtime.contains("render_trace.snowflake_order.is_none()"));
	assert!(runtime.contains("render_trace.join_order"));
	assert!(runtime.contains("render_trace\n\t\t.join_order_edges"));
	assert!(runtime.contains("edge.target.join_index"));
	assert!(!edge_strategy.contains("target:"));
}

#[test]
pub(crate) fn adr183_snowflake_runtime_emits_no_verdict() {
	let explain = source("src/binder/explain.rs");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");
	let runtime_row = first_block(&explain, "fn optimizer_analyze_snowflake_runtime_row");
	let snowflake_branch = analyze_rows
		.split("let Some(StarOrderOutcome::Recognized")
		.next()
		.expect("snowflake analyze branch");

	assert!(snowflake_branch.contains("optimizer_analyze_snowflake_order_row(evidence)"));
	assert!(snowflake_branch.contains("optimizer_analyze_snowflake_runtime_row"));
	assert!(!snowflake_branch.contains("optimizer_analyze_join_order_verdict_row"));
	assert!(!snowflake_branch.contains("measured_star_order_verdict"));
	assert!(!runtime_row.contains("join_order_verdict"));
	assert!(!runtime_row.contains("measured_star_order_verdict"));
}

#[test]
pub(crate) fn adr183_snowflake_runtime_is_observational() {
	let binder = binder_source();
	let explain = source("src/binder/explain.rs");
	let bind_join = first_block(&binder, "async fn bind_join");
	let choose = first_block(&binder, "fn choose_join_strategy");
	let actuator = first_block(&binder, "async fn try_bind_star_order_actuation");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");
	let runtime_row = first_block(&explain, "fn optimizer_analyze_snowflake_runtime_row");

	assert!(!bind_join.contains("join_order_snowflake_runtime"));
	assert!(!choose.contains("join_order_snowflake_runtime"));
	assert!(!actuator.contains("join_order_snowflake_runtime"));
	for forbidden in [
		"choose_join_strategy",
		"try_bind_star_order_actuation",
		"bind_star_order_sequence",
		"broadcast_cap_replay_trigger",
		"STAR_ORDER_SOFT_MARGIN",
		"seed_optimizer_facts",
	] {
		assert!(!analyze_rows.contains(forbidden));
		assert!(!runtime_row.contains(forbidden));
	}
}

#[test]
pub(crate) fn adr183_no_executor_fragment_costmodel_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = [
		"join_order_snowflake_runtime",
		"optimizer_analyze_snowflake_runtime",
		"flatten_snowflake_dimensions",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/catalog.rs"),
		manifest.join("src/coordinator.rs"),
		manifest.join("src/sql_planner.rs"),
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
		"ADR183 snowflake runtime evidence leaked below binder/explain:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr183_no_optimizer_shell_or_planproperties_contract() {
	adr179_no_optimizer_shell();
}

#[test]
pub(crate) fn adr183_cockpit_is_frontend_shared() {
	let binder = binder_source();
	let explain = source("src/binder/explain.rs");
	let rune = source("src/rune_engine.rs");
	let render_explain_analyze = first_block(&binder, "pub(crate) fn render_explain_analyze");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");

	assert!(render_explain_analyze.contains("optimizer_analyze_plan"));
	assert!(analyze_rows.contains("optimizer_analyze_snowflake_runtime_row"));
	assert!(explain.contains("join_order_snowflake_runtime"));
	assert!(rune.contains("LogicalPlan::ExplainAnalyze"));
	assert!(rune.contains("pub fn analyze"));
	assert!(!rune.contains("join_order_snowflake_runtime"));
	assert!(!rune.contains("SnowflakeOrderEvidence"));
}

#[test]
pub(crate) fn adr182_no_executor_fragment_costmodel_or_wendelon_change() {
	let manifest = manifest_dir();
	let markers = [
		"SnowflakeOrder",
		"analyze_snowflake",
		"join_order_snowflake",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest.join("src/exec.rs"),
		manifest.join("src/pbc.rs"),
		manifest.join("src/storage.rs"),
		manifest.join("src/catalog.rs"),
		manifest.join("src/coordinator.rs"),
		manifest.join("src/sql_planner.rs"),
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
		"ADR182 snowflake evidence leaked outside binder/explain:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr182_no_optimizer_shell_or_planproperties_contract() {
	adr179_no_optimizer_shell();
}

#[test]
pub(crate) fn adr182_cockpit_is_frontend_shared() {
	let binder = binder_source();
	let explain = source("src/binder/explain.rs");
	let rune = source("src/rune_engine.rs");
	let explain_plan = first_block(&explain, "pub(super) fn optimizer_explain_plan");
	let analyze_rows = first_block(&explain, "fn optimizer_analyze_rows");
	let bind_explain = first_block(&binder, "async fn bind_explain(&self");
	let rune_explain = first_block(&rune, "pub fn explain");

	assert!(explain_plan.contains("optimizer_explain_snowflake_order_row"));
	assert!(analyze_rows.contains("optimizer_analyze_snowflake_order_row"));
	assert!(bind_explain.contains("optimizer_explain_plan(trace, Some(render_trace))"));
	assert!(rune_explain.contains("LogicalPlan::Explain"));
	assert!(!rune.contains("SnowflakeOrderEvidence"));
	assert!(!rune.contains("join_order_snowflake"));
	assert!(!rune.contains("analyze_snowflake"));
}

#[test]
pub(crate) fn adr181_no_optimizer_shell() {
	adr179_no_optimizer_shell();
}

#[test]
pub(crate) fn adr179_no_optimizer_shell() {
	let manifest = manifest_dir();
	assert!(!manifest.join("src/optimizer.rs").exists());
	assert!(!manifest.join("src/optimizer").exists());

	let src = source_lines(&manifest.join("src"));
	let offenders = src
		.into_iter()
		.filter(|(_, _, line)| {
			let trimmed = line.trim_start();
			trimmed.starts_with("struct Optimizer ")
				|| trimmed.starts_with("pub struct Optimizer ")
				|| trimmed.starts_with("enum Optimizer ")
				|| trimmed.starts_with("pub enum Optimizer ")
				|| line.contains("PlanProperties")
				|| line.contains("Cascades")
				|| trimmed.starts_with("struct Memo ")
				|| trimmed.starts_with("struct Memo{")
				|| trimmed.starts_with("pub struct Memo ")
				|| trimmed.starts_with("pub struct Memo{")
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR179 introduced an optimizer shell/property contract:\n{}",
		format_offenders(&manifest, &offenders)
	);
}
