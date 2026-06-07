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
	assert!(actuator.contains("JoinPhysicalPlacement::SwappedRightBuild"));
	assert!(join_order.contains("NotBroadcastStar"));
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
	let bind_explain = first_block(&binder, "async fn bind_explain");
	let rune_explain = first_block(&rune, "pub fn explain");

	assert!(explain_plan.contains("optimizer_explain_join_order_row"));
	assert!(bind_explain.contains("optimizer_explain_plan(trace, Some(render_trace))"));
	assert!(rune_explain.contains("LogicalPlan::Explain"));
	assert!(!rune.contains("JoinOrderEdgeEvidence"));
	assert!(!rune.contains("StarOrderOutcome"));
	assert!(!rune.contains("join_order_edges"));
}
