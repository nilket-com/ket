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
	] {
		assert!(
			classifier.contains(allowed),
			"missing null-rejection allowlist marker {allowed}"
		);
	}
	for denied in [
		"Expr::Or(_, _)",
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
	let bind_with_trace = source_block(&binder_root, "pub(crate) async fn bind_with_trace");
	let bind_with_explain = source_block(&binder_root, "async fn bind_with_explain_trace");
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
	assert!(binder_root.contains("LogicalPlan::Explain { .. } => plan.clone()"));
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

	assert!(filter_arm.contains("split_logical_and_conjuncts(predicate)"));
	assert!(filter_arm.contains("conjunct_sides.push(sides)"));
	assert!(filter_arm.contains("demotion_owned_sides(kind, to_kind, rejected)"));
	assert!(filter_arm.contains("demotion_sides.left && sides.left && !sides.right"));
	assert!(filter_arm.contains("demotion_sides.right && sides.right && !sides.left"));
	assert!(filter_arm.contains("residual.push(conjunct)"));
	assert!(filter_arm.contains("push_logical_filter_conjuncts(*left, left_push)"));
	assert!(filter_arm.contains("push_logical_filter_conjuncts(*right, right_push)"));
	assert!(filter_arm.contains("wrap_residual_filter(join, residual)"));
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
pub(crate) fn adr209_author_pinned_non_inner_broadcast_remains_rejected() {
	let binder_root = binder_root_source();
	let binder = binder_source();
	let planner = planner_source();
	let bind_scoped_join = source_block(&binder_root, "fn bind_scoped_join");
	let build_join_plan = source_block(&planner, "pub(crate) fn build_join_plan");

	assert!(bind_scoped_join.contains("JoinStrategyIntent::Pinned"));
	assert!(bind_scoped_join.contains("strategy: JoinStrategy::Broadcast"));
	assert!(bind_scoped_join.contains("&& kind != JoinKind::Inner"));
	assert!(bind_scoped_join.contains("broadcast join is only supported for INNER joins"));
	assert!(build_join_plan.contains("kind != JoinKind::Inner"));
	assert!(binder.contains("direct_non_inner_broadcast_pin_stays_rejected"));
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
	assert!(rewrite.contains("} else if kind != JoinKind::Inner"));
	assert!(safe.contains(
		"JoinKind::Inner | JoinKind::RightOuter | JoinKind::RightSemi | JoinKind::RightAnti"
	));
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
		manifest_dir.join("src/sql_wire.rs"),
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
