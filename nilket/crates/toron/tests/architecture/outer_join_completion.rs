use super::support::*;

fn ir_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source")
}

fn planner_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source")
}

fn rune_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read Rune engine source")
}

fn lib_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative))
		.unwrap_or_else(|err| panic!("read {relative}: {err}"))
}

fn outer_join_route_source(demotion: &str) -> String {
	source_block(demotion, "fn route_filter_routed_conjuncts")
}

#[test]
pub(crate) fn adr434_decoded_page_pin_stays_inside_projected_grouped_left_outer_shape() {
	let streaming = source("src/exec/join/streaming.rs");
	let grouped = source_block(
		&streaming,
		"pub(in crate::exec) async fn try_streaming_left_outer_grouped_aggregate",
	);
	assert_eq!(grouped.matches(".try_pin_decoded_pages(").count(), 1);
	let shape_fence = grouped
		.find("if match_filter.is_none()")
		.expect("projected grouped shape fence");
	let probe_started = grouped
		.find("let probe_started = Instant::now()")
		.expect("projected grouped probe boundary");
	let pin = grouped
		.find(".try_pin_decoded_pages(")
		.expect("projected grouped decoded-page pin");
	let probe = grouped
		.find(".visit_streaming_spilled_left_outer_scan_probe_chunks(")
		.expect("projected grouped probe call");
	let off_shape_fallback = grouped
		.find("let left_batches = self.execute_bound_node(left, morsel).await?")
		.expect("off-shape materialized fallback");
	assert!(shape_fence < probe_started);
	assert!(probe_started < pin);
	assert!(pin < probe);
	assert!(probe < off_shape_fallback);
}

#[test]
pub(crate) fn adr209_null_rejection_classifier_is_default_deny_allowlist() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let classifier = source_block(&demotion, "fn null_rejecting_sides");
	let tests = binder_source();

	for allowed in [
		"Expr::IsNotNull(inner)",
		"| Expr::Gte(left, right)",
		"Expr::Like {\n\t\t\texpr,\n\t\t\tnegated: false",
		"Expr::InList {\n\t\t\texpr,\n\t\t\tlist,\n\t\t\tnegated: false",
		"list.iter().all(non_null_literal)",
		"Expr::Or(left, right)",
		".intersect(null_rejecting_sides(right, left_scope, right_scope))",
	] {
		assert!(
			classifier.contains(allowed),
			"missing null-rejection allowlist marker {allowed}"
		);
	}
	for denied in [
		"Expr::Not(_)",
		"Expr::Cast(_, _)",
		"Expr::TryCast(_, _)",
		"Expr::Case { .. }",
		"Expr::IsNull(_)",
		"Expr::InList { .. }",
		"Expr::Like { .. }",
		"=> NullRejectedSides::default()",
	] {
		assert!(
			classifier.contains(denied),
			"missing null-rejection default-deny marker {denied}"
		);
	}
	for gate in [
		"is_null_on_null_supplied_side_does_not_demote",
		"positive_in_demotes_negated_in_does_not",
		"positive_like_demotes_negated_like_does_not",
		"or_both_branches_null_rejecting_demotes_left_join",
		"or_one_branch_is_null_does_not_demote",
		"or_branches_reject_different_sides_do_not_demote",
	] {
		assert!(
			tests.contains(gate),
			"missing ADR209 classifier gate {gate}"
		);
	}
}

#[test]
pub(crate) fn adr209_outer_demotion_runs_before_join_strategy_choice() {
	let binder_root = binder_root_source();
	let binder = binder_source();
	let bind_with_trace = source_block(&binder_root, "async fn bind_with_trace_at");
	let bind_with_explain = source_block(&binder_root, "async fn bind_with_explain_trace_at");
	let bind_scoped_join = source_block(&binder_root, "fn bind_scoped_join");

	assert!(
		bind_with_trace
			.find("demote_null_rejected_outer_joins(plan)")
			.unwrap() < bind_with_trace.find("bind_plan_with_trace").unwrap()
	);
	assert!(
		bind_with_explain
			.find("demote_null_rejected_outer_joins(plan)")
			.unwrap() < bind_with_explain.find("bind_plan_with_trace").unwrap()
	);
	assert!(
		bind_with_explain
			.find("outer_join_demotions\n\t\t\t.extend")
			.unwrap() < bind_with_explain.find("bind_plan_with_trace").unwrap()
	);
	assert!(bind_scoped_join.contains("choose_join_strategy("));
	assert!(bind_scoped_join.contains("mirrored_broadcast_physical_kind("));
	assert!(binder.contains(
		"left_outer_null_rejecting_right_filter_binds_inner_and_broadcasts_filtered_dim"
	));
}

#[test]
pub(crate) fn adr209_demotion_is_frontend_neutral_prebind_normalization() {
	let binder_root = binder_root_source();
	let planner = planner_source();
	let rune = rune_source();
	let demotion = source("src/binder/outer_join_demotion.rs");

	assert!(binder_root.contains("mod outer_join_demotion;"));
	assert!(binder_root.contains("use self::outer_join_demotion::*;"));
	assert!(binder_root.contains("LogicalPlan::Explain { .. } => (plan.clone(), Vec::new())"));
	assert!(demotion.contains("pub(super) fn demote_null_rejected_outer_joins"));
	assert!(!planner.contains("outer_join_demotion"));
	assert!(!planner.contains("demote_null_rejected_outer_joins"));
	assert!(!rune.contains("outer_join_demotion"));
	assert!(!rune.contains("demote_null_rejected_outer_joins"));
	assert!(binder_source().contains("rune_filter_after_left_join_gets_same_demotion_as_sql"));
}

#[test]
pub(crate) fn adr209_demoted_predicates_push_to_owning_side_only() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let filter_arm = demotion
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Alias").next())
		.expect("outer demotion filter arm");
	let route = outer_join_route_source(&demotion);

	assert!(filter_arm.contains("split_logical_and_conjuncts(predicate)"));
	assert!(filter_arm.contains("route_filter_conjuncts(input, conjuncts, Vec::new())"));
	assert!(filter_arm.contains("wrap_residual_filter(routed.input, routed.residual)"));
	assert!(route.contains("proof_sides.push(sides)"));
	assert!(route.contains("ownership_sides.push(conjunct_column_sides("));
	assert!(route.contains("demotion_owned_sides(kind, to_kind, rejected)"));
	assert!(route.contains("demotion_sides.left"));
	assert!(route.contains("proof.left"));
	assert!(route.contains("ownership.left"));
	assert!(route.contains("!ownership.right"));
	assert!(route.contains("!ownership.ambiguous"));
	assert!(route.contains("demotion_sides.right"));
	assert!(route.contains("proof.right"));
	assert!(route.contains("ownership.right"));
	assert!(route.contains("!ownership.left"));
	assert!(route.contains("residual.push(conjunct.original)"));
	assert!(route.contains("push_logical_filter_conjuncts(*left, left_push)"));
	assert!(route.contains("push_logical_filter_conjuncts(*right, right_push)"));
	assert!(binder_source().contains(
		"left_outer_null_rejecting_right_filter_binds_inner_and_broadcasts_filtered_dim"
	));
}

#[test]
pub(crate) fn adr209_auto_broadcast_uses_preserved_probe_matrix() {
	let binder = binder_source();
	let placement = source_block(&binder, "fn broadcast_physical_placement");
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");

	assert!(placement.contains("JoinKind::Inner => BroadcastPlacementRule::SmallerBuild"));
	assert!(placement.contains("JoinKind::RightOuter | JoinKind::RightSemi | JoinKind::RightAnti"));
	assert!(placement.contains("BroadcastPlacementRule::NaturalLeftBuild"));
	assert!(placement.contains("JoinKind::LeftOuter | JoinKind::LeftSemi | JoinKind::LeftAnti"));
	assert!(placement.contains("BroadcastPlacementRule::SwappedRightBuild"));
	assert!(placement.contains("JoinKind::FullOuter => BroadcastPlacementRule::Never"));
	assert!(choose_auto.contains("let placement_rule = broadcast_physical_placement(kind)"));
	assert!(choose_auto.contains("BroadcastPlacementRule::Never"));
	assert!(choose_auto.contains("HashByDefaultReason::NonInnerJoin"));
	for gate in [
		"left_outer_right_build_auto_broadcast_executes_as_physical_right_outer",
		"right_outer_natural_left_build_auto_broadcast_keeps_physical_right_outer",
		"full_outer_without_demote_stays_hash_non_inner",
	] {
		assert!(
			binder.contains(gate),
			"missing ADR209 placement gate {gate}"
		);
	}
}

#[test]
pub(crate) fn adr209_swapped_sided_broadcast_uses_mirrored_physical_kind() {
	let binder = binder_source();
	let mirror = source_block(&binder, "fn mirrored_broadcast_physical_kind");
	let bind_scoped_join = source_block(&binder, "fn bind_scoped_join");

	assert!(mirror.contains(
		"(JoinKind::LeftOuter, JoinPhysicalPlacement::SwappedRightBuild) => JoinKind::RightOuter"
	));
	assert!(mirror.contains(
		"(JoinKind::LeftSemi, JoinPhysicalPlacement::SwappedRightBuild) => JoinKind::RightSemi"
	));
	assert!(mirror.contains(
		"(JoinKind::LeftAnti, JoinPhysicalPlacement::SwappedRightBuild) => JoinKind::RightAnti"
	));
	assert!(bind_scoped_join.contains(
		"validate_strategy_shape(\n\t\t\tmirrored_broadcast_physical_kind(kind, decision.placement)"
	));
	assert!(bind_scoped_join.contains("kind: physical_kind"));
	assert!(
		bind_scoped_join.contains("if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti)")
	);
	assert!(
		binder.contains(
			"left_semi_and_left_anti_auto_broadcast_build_right_without_restore_projection"
		)
	);
}

#[test]
pub(crate) fn adr211_binder_validates_author_pins_through_209_placement_table() {
	let binder_root = binder_root_source();
	let binder = binder_source();
	let planner = planner_source();
	let bind_scoped_join = source_block(&binder_root, "fn bind_scoped_join");
	let placement = source_block(&binder, "fn pinned_broadcast_placement");
	let build_join_plan = source_block(&planner, "pub(crate) fn build_join_plan");

	assert!(bind_scoped_join.contains("choose_join_strategy("));
	assert!(bind_scoped_join.contains("binder_physicalizes_join"));
	assert!(bind_scoped_join.contains("strategy: JoinStrategy::Broadcast"));
	assert!(!bind_scoped_join.contains("&& kind != JoinKind::Inner"));
	assert!(!build_join_plan.contains("kind != JoinKind::Inner"));
	assert!(placement.contains("broadcast_physical_placement(kind)"));
	assert!(
		placement.contains("(BroadcastPlacementRule::NaturalLeftBuild, BroadcastBuildSide::Left)")
	);
	assert!(
		placement
			.contains("(BroadcastPlacementRule::SwappedRightBuild, BroadcastBuildSide::Right)")
	);
	assert!(
		placement.contains("(BroadcastPlacementRule::NaturalLeftBuild, BroadcastBuildSide::Right)")
	);
	assert!(
		placement.contains("(BroadcastPlacementRule::SwappedRightBuild, BroadcastBuildSide::Left)")
	);
	assert!(placement.contains("BroadcastPlacementRule::Never"));
	assert!(binder.contains("direct_pinned_non_inner_safe_side_binds"));
	assert!(binder.contains("direct_pinned_non_inner_wrong_side_rejects"));
}

#[test]
pub(crate) fn adr209_fragmenter_accepts_only_safe_non_inner_broadcast_shapes() {
	let fragment = source("src/distributed/fragment.rs");
	let root = source_block(&fragment, "fn fragment_broadcast_join_plan");
	let rewrite = source_block(&fragment, "fn rewrite_broadcast_star_probe_inner");
	let safe = source_block(&fragment, "fn safe_broadcast_physical_join_kind");

	assert!(root.contains("safe_broadcast_physical_join_kind(join.kind)"));
	assert!(rewrite.contains("root: bool"));
	assert!(rewrite.contains("if root"));
	assert!(rewrite.contains("safe_broadcast_physical_join_kind(kind)"));
	// ADR 239: a left-deep comma fold nests CROSS in the broadcast star, so the non-root star kind is
	// now "inner or cross" (CROSS composes like INNER — match-all of build x probe, no null-fill).
	assert!(rewrite.contains("} else if !matches!(kind, JoinKind::Inner | JoinKind::Cross)"));
	for kind in [
		"JoinKind::Inner",
		"JoinKind::Cross",
		"JoinKind::RightOuter",
		"JoinKind::RightSemi",
		"JoinKind::RightAnti",
	] {
		assert!(
			safe.contains(kind),
			"safe_broadcast_physical_join_kind must accept {kind}"
		);
	}
	assert!(fragment.contains("broadcast_safe_non_inner_physical_roots_fragment"));
	assert!(fragment.contains("broadcast_non_inner_star_child_remains_rejected_by_fragmenter"));
	assert!(fragment.contains("broadcast_non_inner_join_is_rejected_by_fragmenter"));
}

#[test]
pub(crate) fn adr209_runtime_filters_remain_actual_inner_only() {
	let fragment = source("src/distributed/fragment.rs");
	let runtime_filter = source_block(&fragment, "fn runtime_filter_join_eligible");

	assert!(runtime_filter.contains("!matches!(join.kind, JoinKind::Inner)"));
	assert!(runtime_filter.contains("return false"));
	assert!(!runtime_filter.contains("safe_broadcast_physical_join_kind"));
	assert!(fragment.contains("runtime_filter_eligible_false_for_every_non_inner_kind"));
}

#[test]
pub(crate) fn adr209_join_order_recognizers_remain_actual_inner_only() {
	let join_order = source("src/binder/join_order.rs");
	let star = source_block(&join_order, "pub(super) fn extract_star_join_candidate");
	let analyze = source_block(&join_order, "pub(super) fn analyze(");
	let reroot = source_block(&join_order, "pub(super) fn reroot_galaxy_join_tree");

	assert!(join_order.contains("StarOrderDeclineReason::NonInnerEquiJoin"));
	assert!(star.contains("JoinKind::Inner"));
	assert!(analyze.contains("NonInnerEquiJoin"));
	assert!(reroot.contains("JoinKind::Inner"));
	assert!(!join_order.contains("safe_broadcast_physical_join_kind"));
}

#[test]
pub(crate) fn adr209_explain_receipts_report_outer_demotion() {
	let explain = source("src/binder/explain.rs");
	let trace = source_block(&explain, "pub(super) struct OptimizerExplainRenderTrace");
	let row = source_block(
		&explain,
		"pub(super) fn optimizer_explain_outer_join_demotion_row",
	);

	assert!(trace.contains("outer_join_demotions: Vec<OuterJoinDemotionReceipt>"));
	assert!(explain.contains("render_trace\n\t\t\t\t.outer_join_demotions"));
	assert!(row.contains("\"outer_join_demotion\""));
	assert!(row.contains("\"prebind\""));
	assert!(row.contains("join_kind_label(receipt.from_kind)"));
	assert!(row.contains("join_kind_label(receipt.to_kind)"));
	assert!(row.contains("null_rejecting_filter:"));
	assert!(binder_source().contains("explain_reports_null_rejected_outer_join_demotion"));
}

#[test]
pub(crate) fn adr209_demoted_join_reaches_durable_verdict_lane() {
	let wendelon = source("tests/wendelon_storage_execution_test.rs");

	assert!(wendelon.contains("demoted_join_verdict_reaches_durable_lane"));
	assert!(wendelon.contains("execute_demoted_star_query"));
	assert!(wendelon.contains("optimizer_runtime_flat_star_verdict_count"));
	assert!(wendelon.contains("JoinKind::LeftOuter"));
	assert!(wendelon.contains("JoinKind::Inner"));
	assert!(wendelon.contains("durable_optimizer_runtime_evidence"));
	assert!(wendelon.contains("measured_accrual_sample_rate"));
}

#[test]
pub(crate) fn adr210_descent_reuses_209_classifier_and_demotion_table() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let tests = binder_source();

	assert_eq!(demotion.matches("fn null_rejecting_sides").count(), 1);
	assert_eq!(demotion.matches("fn demoted_join_kind").count(), 1);
	assert_eq!(demotion.matches("fn demotion_owned_sides").count(), 1);
	assert!(route.contains("null_rejecting_sides(&conjunct.routed, &left_scope, &right_scope)"));
	assert!(route.contains("demoted_join_kind(kind, rejected)"));
	assert!(route.contains("demotion_owned_sides(kind, to_kind, rejected)"));
	assert!(route.contains("conjunct_column_sides(&conjunct.routed, &left_scope, &right_scope)"));
	assert!(!demotion.contains("path_null_reject"));
	assert!(!demotion.contains("propagated_null_reject"));
	assert!(tests.contains("is_null_and_negated_in_remain_non_demoting_inside_mixed_chain"));
	assert!(tests.contains("full_outer_stepwise_demotes_via_propagation"));
}

#[test]
pub(crate) fn adr210_filter_router_subsumes_depth_zero_demotion() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let filter_arm = demotion
		.split("LogicalPlan::Filter { input, predicate } => {")
		.nth(1)
		.and_then(|rest| rest.split("LogicalPlan::Alias").next())
		.expect("outer demotion filter arm");
	let route = outer_join_route_source(&demotion);
	let tests = binder_source();

	assert!(filter_arm.contains("split_logical_and_conjuncts(predicate)"));
	assert!(filter_arm.contains("route_filter_conjuncts(input, conjuncts, Vec::new())"));
	assert!(filter_arm.contains("receipts.extend(routed.receipts)"));
	assert!(filter_arm.contains("wrap_residual_filter(routed.input, routed.residual)"));
	assert!(route.contains("LogicalPlan::Join {"));
	assert!(route.contains("kind: JoinKind::Inner"));
	assert!(route.contains("let Some(to_kind) = demoted_join_kind(kind, rejected)"));
	assert!(tests.contains(
		"left_outer_null_rejecting_right_filter_binds_inner_and_broadcasts_filtered_dim"
	));
	assert!(tests.contains("path_scoped_filter_demotes_nested_left_join_through_inner_chain"));
}

#[test]
pub(crate) fn adr210_motion_set_is_inner_exchange_and_pure_sort_only() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let alias_router = source_block(&demotion, "fn route_conjuncts_through_alias");
	let ownership = source_block(&demotion, "fn conjunct_column_sides");
	let tests = binder_source();

	assert!(route.contains("kind: JoinKind::Inner"));
	assert!(route.contains("sides.left && !sides.right && !sides.ambiguous"));
	assert!(route.contains("sides.right && !sides.left && !sides.ambiguous"));
	assert!(route.contains("LogicalPlan::Exchange"));
	assert!(route.contains("LogicalPlan::Sort"));
	assert!(route.contains("limit: None"));
	assert!(route.contains("offset: 0"));
	assert!(route.contains("LogicalPlan::Alias {"));
	assert!(route.contains("route_conjuncts_through_alias("));
	assert!(alias_router.contains("remap_expr_through_alias("));
	assert!(route.contains("LogicalPlan::Project {"));
	assert!(route.contains("project_passthrough_map"));
	assert!(route.contains("LogicalPlan::Aggregate { .. }"));
	assert!(ownership.contains("Expr::Column(column_ref)"));
	assert!(ownership.contains("column_side(column_ref, left_scope, right_scope)"));
	assert!(tests.contains("exchange_allows_path_scoped_demote"));
	assert!(tests.contains("pure_sort_allows_path_scoped_demote_with_results_match"));
}

#[test]
pub(crate) fn adr210_topn_sort_and_take_are_motion_boundaries() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let tests = binder_source();

	assert!(route.contains("limit: None"));
	assert!(route.contains("offset: 0"));
	assert!(route.contains("LogicalPlan::Sort { .. }"));
	assert!(route.contains("LogicalPlan::Take { .. }"));
	assert!(route.contains("residual: routed_originals(conjuncts)"));
	assert!(tests.contains("filter_does_not_move_below_take_limit"));
	assert!(tests.contains("filter_does_not_move_below_sort_limit_or_offset"));
}

#[test]
pub(crate) fn adr210_conjuncts_move_only_on_demotion_success() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let tests = binder_source();
	let wendelon = source("tests/wendelon_storage_execution_test.rs");

	assert!(route.contains("let Some(to_kind) = demoted_join_kind(kind, rejected) else"));
	assert!(route.contains("residual: routed_originals(conjuncts)"));
	assert!(route.contains("residual.push(conjunct.original)"));
	assert!(route.contains("residual.extend(left.residual)"));
	assert!(route.contains("residual.extend(right.residual)"));
	assert!(route.contains("push_logical_filter_conjuncts(*left, left_push)"));
	assert!(route.contains("push_logical_filter_conjuncts(*right, right_push)"));
	assert!(tests.contains("cross_side_conjunct_stays_residual_above_original_filter"));
	assert!(tests.contains("predicate_does_not_cross_outer_boundary_to_reach_deeper_edge"));
	assert!(tests.contains("multi_conjuncts_can_demote_two_edges_in_one_pass"));
	assert!(wendelon.contains("mixed_chain_demoted_results_match_undemoted"));
}

#[test]
pub(crate) fn adr210_propagation_extends_existing_outer_join_demotion_receipt() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let receipt = source_block(&demotion, "pub(super) struct OuterJoinDemotionReceipt");
	let explain = source("src/binder/explain.rs");
	let row = source_block(
		&explain,
		"pub(super) fn optimizer_explain_outer_join_demotion_row",
	);
	let tests = binder_source();

	assert!(receipt.contains("from_kind: JoinKind"));
	assert!(receipt.contains("to_kind: JoinKind"));
	assert!(receipt.contains("conjunct_text: String"));
	assert!(receipt.contains("propagation_path: Vec<JoinSideStep>"));
	assert!(demotion.contains("pub(super) enum JoinSideStep"));
	assert!(demotion.contains("fn label"));
	assert!(row.contains("\"outer_join_demotion\""));
	assert!(row.contains("\"prebind\""));
	assert!(row.contains("propagated_path:"));
	assert!(tests.contains("propagation_receipt_reports_descent_path"));
}

#[test]
pub(crate) fn adr210_no_recognizer_pushdown_strategy_executor_or_wire_widening() {
	let manifest_dir = manifest_dir();
	let markers = [
		"route_filter_conjuncts",
		"JoinSideStep",
		"propagation_path",
		"propagated_path",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/sql_planner.rs"),
		manifest_dir.join("src/rune_engine.rs"),
		manifest_dir.join("src/binder/join_order.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/ir/execution.rs"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("../toron-client/src/protocol.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}

	assert!(
		offenders.is_empty(),
		"ADR210 propagation leaked outside binder/explain surfaces:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
	assert!(
		binder_source()
			.contains("recognizers_still_decline_true_outer_join_after_non_demoting_filter")
	);
}

#[test]
pub(crate) fn adr211_pinned_broadcast_intent_carries_build_side() {
	let ir = ir_source();
	let planner = planner_source();
	let binder = binder_source();
	let join_intent = source_block(&ir, "pub enum JoinStrategyIntent");
	let build_join_plan = source_block(&planner, "pub(crate) fn build_join_plan_with_filter");
	let choose = source_block(&binder, "fn choose_join_strategy");

	assert!(ir.contains("pub enum BroadcastBuildSide"));
	assert!(ir.contains("Left"));
	assert!(ir.contains("Right"));
	assert!(join_intent.contains("build_side: Option<BroadcastBuildSide>"));
	assert!(build_join_plan.contains("HashPartitioned if build_side.is_some()"));
	assert!(build_join_plan.contains("build_side: None"));
	assert!(build_join_plan.contains("let Some(build_side) = build_side"));
	assert!(build_join_plan.contains("build_side: Some(build_side)"));
	assert!(choose.contains("build_side,"));
	assert!(choose.contains("build_side.ok_or_else"));
	assert!(choose.contains("pinned_broadcast_placement(kind, build_side)?"));
}

#[test]
pub(crate) fn adr211_frontends_do_not_preshape_pinned_broadcast() {
	let planner = planner_source();
	let binder_root = binder_root_source();
	let build_join_plan = source_block(&planner, "pub(crate) fn build_join_plan_with_filter");
	let broadcast_arm = build_join_plan
		.split("JoinStrategy::Broadcast =>")
		.nth(1)
		.expect("Broadcast arm");
	let bind_scoped_join = source_block(&binder_root, "fn bind_scoped_join");

	assert!(!broadcast_arm.contains("LogicalPlan::Exchange"));
	assert!(!broadcast_arm.contains("partition_count: 1"));
	assert!(broadcast_arm.contains("LogicalPlan::Join"));
	assert!(bind_scoped_join.contains("binder_physicalizes_join"));
	assert!(bind_scoped_join.contains("JoinStrategyIntent::Auto"));
	assert!(bind_scoped_join.contains("strategy: JoinStrategy::Broadcast"));
	assert!(bind_scoped_join.contains("exchange_for_join_keys"));
	assert!(!bind_scoped_join.contains("force_demoted_sql_broadcast"));
	assert!(!bind_scoped_join.contains("strip_root_exchange"));
}

#[test]
pub(crate) fn adr211_sql_hint_resolves_immediate_safe_side() {
	let planner = planner_source();
	let binder = binder_source();
	let hint = source_block(&planner, "fn join_strategy_intent_for_hint");
	let lower_select = source_block(&planner, "fn lower_select");

	assert!(!hint.contains("kind: JoinKind"));
	assert!(hint.contains("plan_qualifiers(left)"));
	assert!(hint.contains("plan_qualifiers(right)"));
	assert!(hint.contains("left_matches + right_matches > 1"));
	assert!(hint.contains("composite left join input"));
	assert!(hint.contains("composite right join input"));
	assert!(hint.contains("build_side: Some(BroadcastBuildSide::Left)"));
	assert!(hint.contains("build_side: Some(BroadcastBuildSide::Right)"));
	assert!(
		lower_select.contains("join_strategy_intent_for_hint(&mut broadcast_hint, &plan, &right)?")
	);
	assert!(binder.contains(
		"sql_left_outer_broadcast_hint_on_null_supplied_side_binds_preserved_probe_broadcast"
	));
	assert!(binder.contains(
		"sql_right_outer_broadcast_hint_on_null_supplied_side_binds_preserved_probe_broadcast"
	));
}

#[test]
pub(crate) fn adr211_preserved_side_and_full_outer_pins_reject_loudly() {
	let binder = binder_source();
	let placement = source_block(&binder, "fn pinned_broadcast_placement");
	let unsupported = source_block(&binder, "fn unsupported_pinned_broadcast_side");

	assert!(placement.contains("BroadcastPlacementRule::Never"));
	assert!(placement.contains("FullOuter requires demotion before broadcast can be pinned"));
	assert!(unsupported.contains("target the null-supplied side or drop the hint"));
	assert!(binder.contains("sql_full_outer_broadcast_hint_rejects_without_demotion"));
	assert!(binder.contains(
		"sql_full_outer_broadcast_hint_is_honored_after_both_sides_null_rejecting_demote"
	));
	assert!(binder.contains("direct_pinned_non_inner_wrong_side_rejects"));
}

#[test]
pub(crate) fn adr211_rune_post_join_pin_shares_sql_admission_path() {
	let rune = rune_source();
	let binder = binder_source();
	let module = source_block(&rune, "pub fn create_toron_module");
	let pin = source_block(&rune, "fn pin_broadcast_build");

	assert!(rune.contains("pub fn broadcast_build(self, side: String)"));
	assert!(rune.contains("fn parse_broadcast_build_side"));
	assert!(module.contains("associated_function(\"broadcast_build\""));
	assert!(pin.contains("LogicalPlan::Join"));
	assert!(pin.contains("JoinStrategyIntent::Auto"));
	assert!(pin.contains("strategy: JoinStrategy::Broadcast"));
	assert!(pin.contains("source: JoinStrategyPinSource::Rune"));
	assert!(pin.contains("build_side: Some(build_side)"));
	assert!(
		binder.contains("rune_post_join_broadcast_pin_matches_sql_hint_and_rejects_preserved_side")
	);
}

#[test]
pub(crate) fn adr211_no_fragmenter_executor_wire_runtime_filter_or_recognizer_widening() {
	let manifest_dir = manifest_dir();
	let markers = [
		"BroadcastBuildSide",
		"pinned_broadcast_placement",
		"unsupported_pinned_broadcast_side",
		"pub fn broadcast_build",
		"associated_function(\"broadcast_build\"",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/ir/execution.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("../toron-client/src/protocol.rs"),
		manifest_dir.join("src/binder/join_order.rs"),
		manifest_dir.join("src/binder/selectivity.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);

	assert!(
		offenders.is_empty(),
		"ADR211 broadcast authoring leaked into execution/wire/recognizer/Wendelon source:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr212_or_null_rejection_uses_intersection() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let classifier = source_block(&demotion, "fn null_rejecting_sides");
	let sides = source_block(&demotion, "impl NullRejectedSides");
	let tests = binder_source();

	assert!(sides.contains("fn intersect"));
	assert!(sides.contains("left: self.left && other.left"));
	assert!(sides.contains("right: self.right && other.right"));
	assert!(classifier.contains("Expr::Or(left, right)"));
	assert!(classifier.contains(
		"null_rejecting_sides(left, left_scope, right_scope)\n\t\t\t.intersect(null_rejecting_sides(right, left_scope, right_scope))"
	));
	assert!(!classifier.contains("Expr::Or(_, _)"));
	for gate in [
		"or_both_branches_null_rejecting_demotes_left_join",
		"or_one_branch_is_null_does_not_demote",
		"or_branches_reject_different_sides_do_not_demote",
	] {
		assert!(tests.contains(gate), "missing ADR212 OR gate {gate}");
	}
}

#[test]
pub(crate) fn adr212_demoting_cross_side_or_keeps_conjunct_residual() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let tests = binder_source();

	assert!(route.contains("let mut proof_sides = Vec::with_capacity(conjuncts.len())"));
	assert!(route.contains("let mut ownership_sides = Vec::with_capacity(conjuncts.len())"));
	assert!(route.contains("proof_sides.push(sides)"));
	assert!(route.contains("ownership_sides.push(conjunct_column_sides("));
	assert!(route.contains("proof.intersect(demotion_sides)"));
	assert!(route.contains("left_push.push(conjunct.routed)"));
	assert!(route.contains("right_push.push(conjunct.routed)"));
	assert!(route.contains("residual.push(conjunct.original)"));
	assert!(tests.contains("or_cross_side_conjunct_demotes_without_moving"));
}

#[test]
pub(crate) fn adr212_alias_descent_requires_identity_remap_unique_resolution() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let alias_router = source_block(&demotion, "fn route_conjuncts_through_alias");
	let alias = source_block(&demotion, "fn remap_expr_through_alias");
	let ordinal = source_block(&demotion, "fn alias_output_ordinal");
	let tests = binder_source();

	assert!(route.contains("LogicalPlan::Alias {"));
	assert!(route.contains("alias_schema_for_outer_demotion(&input_scope.schema, &columns)?"));
	assert!(route.contains("RelationScope::alias(alias.clone(), &alias_schema)"));
	assert!(route.contains("route_conjuncts_through_alias("));
	assert!(alias_router.contains("remap_expr_through_alias("));
	assert!(route.contains("JoinSideStep::Alias(alias.clone())"));
	assert!(alias.contains("resolve_join_column(&remapped, \"alias input\")"));
	assert!(ordinal.contains("unique_schema_ordinal("));
	assert!(ordinal.contains("&alias_scope.schema"));
	assert!(ordinal.contains("qualifier != alias"));
	for gate in [
		"alias_wrapped_left_join_filter_reaches_demotion",
		"alias_empty_columns_qualifier_only_remap_reaches_demotion",
		"alias_remap_ambiguous_resolution_stops",
	] {
		assert!(tests.contains(gate), "missing ADR212 alias gate {gate}");
	}
}

#[test]
pub(crate) fn adr212_project_descent_requires_direct_column_passthrough() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let map = source_block(&demotion, "fn project_passthrough_map");
	let project_router = source_block(&demotion, "fn route_conjuncts_through_project");
	let remap = source_block(&demotion, "fn remap_expr_through_project");
	let tests = binder_source();

	assert!(route.contains("LogicalPlan::Project {"));
	assert!(route.contains("project_passthrough_map(&exprs, &output_names, &input_scope)?"));
	assert!(route.contains("route_conjuncts_through_project("));
	assert!(project_router.contains("remap_expr_through_project("));
	assert!(route.contains("JoinSideStep::Project"));
	assert!(map.contains("Some(Expr::Column(column_ref))"));
	assert!(map.contains("output_name.clone()"));
	assert!(map.contains("map.insert(output_name.clone(), None)"));
	assert!(remap.contains("if column_ref.qualifier.is_some()"));
	assert!(remap.contains("let Some(Some(remapped)) = map.get(&column_ref.name.0)"));
	for gate in [
		"project_passthrough_filter_reaches_demotion",
		"computed_project_column_stops_motion",
		"duplicate_or_missing_project_output_name_stops_motion",
	] {
		assert!(tests.contains(gate), "missing ADR212 project gate {gate}");
	}
}

#[test]
pub(crate) fn adr212_residuals_preserve_original_form() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let alias_router = source_block(&demotion, "fn route_conjuncts_through_alias");
	let project_router = source_block(&demotion, "fn route_conjuncts_through_project");
	let shared_router = source_block(&demotion, "fn route_conjuncts_through<F>");
	let routed = source_block(&demotion, "struct RoutedExpr");
	let originals = source_block(&demotion, "fn routed_originals");
	let tests = binder_source();

	assert!(routed.contains("original: Expr"));
	assert!(routed.contains("routed: Expr"));
	assert!(alias_router.contains("route_conjuncts_through(conjuncts"));
	assert!(project_router.contains("route_conjuncts_through(conjuncts"));
	assert!(shared_router.contains("original: conjunct.original"));
	assert!(shared_router.contains("routed,"));
	assert!(route.contains("residual.push(conjunct.original)"));
	assert!(shared_router.contains("residual.push(conjunct.original)"));
	assert!(originals.contains("map(|conjunct| conjunct.original)"));
	assert!(tests.contains("alias_project_residuals_preserve_original_filter_shape"));
	assert!(tests.contains("remapped_conjunct_results_match_unwrapped_query"));
}

#[test]
pub(crate) fn adr212_receipt_path_names_wrapper_steps() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let path = source_block(&demotion, "pub(super) enum JoinSideStep");
	let label = source_block(&demotion, "impl JoinSideStep");
	let explain = source("src/binder/explain.rs");
	let row = source_block(
		&explain,
		"pub(super) fn optimizer_explain_outer_join_demotion_row",
	);
	let tests = binder_source();

	assert!(path.contains("Alias(TableRef)"));
	assert!(path.contains("Project"));
	assert!(label.contains("format!(\"alias({})\", relation_key_label(alias))"));
	assert!(label.contains("\"project\".to_string()"));
	assert!(row.contains(".map(JoinSideStep::label)"));
	assert!(tests.contains("propagation_receipt_reports_alias_and_project_steps"));
}

#[test]
pub(crate) fn adr212_motion_fences_remain_closed() {
	let demotion = source("src/binder/outer_join_demotion.rs");
	let route = outer_join_route_source(&demotion);
	let tests = binder_source();

	assert!(route.contains("limit: None"));
	assert!(route.contains("offset: 0"));
	assert!(route.contains("LogicalPlan::Sort { .. }"));
	assert!(route.contains("LogicalPlan::Take { .. }"));
	assert!(route.contains("LogicalPlan::Aggregate { .. }"));
	assert!(route.contains("residual: routed_originals(conjuncts)"));
	assert!(tests.contains("filter_does_not_move_below_take_limit"));
	assert!(tests.contains("filter_does_not_move_below_sort_limit_or_offset"));
	assert!(tests.contains("computed_project_column_stops_motion"));
}

#[test]
pub(crate) fn adr212_rune_and_sql_share_outer_join_demotion_entry() {
	let binder_root = binder_root_source();
	let planner = planner_source();
	let rune = rune_source();
	let tests = binder_source();

	assert!(binder_root.contains("mod outer_join_demotion;"));
	assert!(binder_root.contains("demote_null_rejected_outer_joins(plan)"));
	assert!(!planner.contains("demote_null_rejected_outer_joins"));
	assert!(!rune.contains("demote_null_rejected_outer_joins"));
	assert!(tests.contains("rune_or_filter_and_alias_descent_match_sql"));
}

#[test]
pub(crate) fn adr212_no_executor_fragmenter_wire_runtime_filter_strategy_or_wendelon_change() {
	let manifest_dir = manifest_dir();
	let markers = [
		"remap_expr_through_alias",
		"remap_expr_through_project",
		"project_passthrough_map",
		"JoinSideStep::Alias",
		"JoinSideStep::Project",
		"or_cross_side_conjunct_demotes_without_moving",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/ir/execution.rs"),
		manifest_dir.join("src/distributed"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("../toron-client/src/protocol.rs"),
		manifest_dir.join("src/binder/join_order.rs"),
		manifest_dir.join("src/binder/selectivity.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);

	assert!(
		offenders.is_empty(),
		"ADR212 demotion reach leaked outside binder/explain surfaces:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr209_no_wendelon_wire_executor_semantics_or_strategy_variant() {
	let manifest_dir = manifest_dir();
	let ir = ir_source();
	let join_strategy = source_block(&ir, "pub enum JoinStrategy");
	let markers = [
		"OuterJoinDemotionReceipt",
		"outer_join_demotion",
		"demote_null_rejected_outer_joins",
		"BroadcastPlacementRule",
		"broadcast_physical_placement",
		"mirrored_broadcast_physical_kind",
		"safe_broadcast_physical_kind",
	];
	let mut offenders = Vec::new();
	for path in [
		manifest_dir.join("src/exec.rs"),
		manifest_dir.join("src/ir/execution.rs"),
		manifest_dir.join("src/server"),
		manifest_dir.join("src/wire.rs"),
		manifest_dir.join("../toron-client/src/protocol.rs"),
	] {
		if path.is_dir() {
			offenders.extend(
				source_lines(&path)
					.into_iter()
					.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
			);
		} else {
			let source = std::fs::read_to_string(&path).expect("read Toron source");
			offenders.extend(
				source
					.lines()
					.enumerate()
					.filter(|(_, line)| markers.iter().any(|marker| line.contains(marker)))
					.map(|(idx, line)| (path.clone(), idx + 1, line.to_string())),
			);
		}
	}
	offenders.extend(
		source_lines(&wendelon_manifest_dir().join("src"))
			.into_iter()
			.filter(|(_, _, line)| markers.iter().any(|marker| line.contains(marker))),
	);

	assert!(join_strategy.contains("HashPartitioned"));
	assert!(join_strategy.contains("Broadcast"));
	assert_eq!(enum_variant_count(&join_strategy), 2);
	assert!(!ir.contains("PreservedProbeBroadcast"));
	assert!(!ir.contains("RightBroadcast"));
	assert!(
		offenders.is_empty(),
		"ADR209 optimizer-only markers leaked into executor/wire/Wendelon source:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
	assert!(
		lib_source().contains("non_inner_broadcast_distributed_result_matches_hash_for_safe_kinds")
	);
}
