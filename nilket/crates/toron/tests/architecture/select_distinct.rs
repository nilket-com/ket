use super::support::*;

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative))
		.unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr231_distinct_marker_lowers_to_empty_aggregate_in_binder() {
	let ir = source("src/ir.rs");
	let binder = binder_source();

	assert!(ir.contains("Distinct {\n\t\tinput: Box<LogicalPlan>,\n\t}"));
	assert!(binder.contains("LogicalPlan::Distinct { input } =>"));
	assert!(binder.contains("all_output_columns_program(&input.schema())"));
	assert!(binder.contains("ExecutionPlan::Exchange"));
	assert!(binder.contains("DEFAULT_DISTINCT_PARTITION_COUNT"));
	assert!(binder.contains("self.bind_bound_aggregate(exchange, group_expr, Vec::new())"));
	assert!(binder.contains("fn bind_bound_aggregate("));
	assert!(binder.contains("ExecutionPlan::Aggregate"));
}

#[test]
pub(crate) fn adr231_distinct_binder_group_keys_are_ordinal_safe() {
	let binder = binder_source();
	let helper = source_block(&binder, "fn all_output_columns_program");

	assert!(helper.contains("for (idx, field) in schema.fields().iter().enumerate()"));
	assert!(helper.contains("ColumnOrdinal(u32::try_from(idx)"));
	assert!(helper.contains("name: ColumnName(field.name().clone())"));
	assert!(helper.contains("builder.push_root(node)"));
	assert!(
		!helper.contains("resolve_column("),
		"DISTINCT group keys must be built by ordinal, not output-name lookup"
	);
}

#[test]
pub(crate) fn adr231_distinct_is_frontend_neutral_sql_and_rune_share_the_marker() {
	let planner = source("src/sql_planner.rs");
	let rune = source("src/rune_engine.rs");
	let lower_select = source_block(&planner, "fn lower_select");
	let rune_distinct = source_block(&rune, "pub fn distinct");

	assert!(lower_select.contains("wrap_distinct_if_needed(plan, distinct_requested)"));
	assert!(planner.contains("fn wrap_distinct_if_needed"));
	assert!(planner.contains("LogicalPlan::Distinct"));
	assert!(rune_distinct.contains("LogicalPlan::Distinct"));
	assert!(rune.contains("m.associated_function(\"distinct\", RuneDataFrame::distinct)?;"));
}

#[test]
pub(crate) fn adr231_distinct_adds_no_new_execution_plan_or_executor_op() {
	let ir_execution = source("src/ir/execution.rs");
	let exec = source("src/exec.rs");
	let fragment = source("src/distributed/fragment.rs");
	let execution_plan = source_block(&ir_execution, "pub enum ExecutionPlan");
	let execute_bound = source_block(&exec, "pub fn execute_bound");

	assert!(!execution_plan.contains("\n\tDistinct {"));
	assert!(!ir_execution.contains("ExecutionPlan::Distinct"));
	assert!(exec.contains("unbound logical DISTINCT execution"));
	assert!(!execute_bound.contains("Distinct"));
	assert!(!fragment.contains("Distinct"));
}

#[test]
pub(crate) fn adr231_distinct_first_cut_retains_top_level_aggregate_fences() {
	let planner = source("src/sql_planner.rs");
	let binder = binder_source();
	let lower_select = source_block(&planner, "fn lower_select");
	let distinct_fence = source_block(&binder, "fn distinct_input_unsupported_message");

	for required in [
		"Some(Distinct::On(_)) => return Err(unsupported(\"DISTINCT ON is not supported\"))",
		"Some(Distinct::All) => return Err(unsupported(\"SELECT ALL is not supported\"))",
		"unsupported(\"DISTINCT with GROUP BY is not supported\")",
		"unsupported(\"DISTINCT with HAVING is not supported\")",
		"\"DISTINCT with aggregate projections is not supported\"",
		"\"DISTINCT with window functions is not supported\"",
	] {
		assert!(
			lower_select.contains(required),
			"lower_select lost {required}"
		);
	}
	assert!(distinct_fence.contains("LogicalPlan::Aggregate { .. }"));
	assert!(distinct_fence.contains("DISTINCT over aggregate inputs is not supported"));
}

#[test]
pub(crate) fn adr231_user_group_by_without_aggregate_still_rejected() {
	let planner = source("src/sql_planner.rs");
	let lower_select = source_block(&planner, "fn lower_select");

	assert!(lower_select.contains("if aggr_expr.is_empty()"));
	assert!(lower_select.contains("GROUP BY requires at least one aggregate call"));
	assert!(lower_select.contains("LogicalPlan::Aggregate"));
	assert!(lower_select.contains("wrap_distinct_if_needed(plan, distinct_requested)"));
}

#[test]
pub(crate) fn adr231_distinct_leaves_optimizer_evidence_surfaces_unchanged() {
	for relative in [
		"src/binder/runtime_evidence.rs",
		"src/binder/decision_log.rs",
		"src/daemon.rs",
		"src/distributed/fragment.rs",
		"src/wire.rs",
	] {
		let source = source(relative);
		assert!(
			!source.contains("LogicalPlan::Distinct")
				&& !source.contains("ExecutionPlan::Distinct"),
			"{relative} should not learn DISTINCT policy or execution"
		);
	}
	let wendelon = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<String>();
	assert!(!wendelon.contains("LogicalPlan::Distinct"));
	assert!(!wendelon.contains("ExecutionPlan::Distinct"));
}

#[test]
pub(crate) fn adr232_distinct_over_joins_lifts_sql_and_binder_fences_together() {
	let planner = source("src/sql_planner.rs");
	let binder = binder_source();
	let lower_select = source_block(&planner, "fn lower_select");
	let distinct_fence = source_block(&binder, "fn distinct_input_unsupported_message");

	assert!(!lower_select.contains("DISTINCT over joins is not supported"));
	assert!(!distinct_fence.contains("DISTINCT over joins is not supported"));
	assert!(distinct_fence.contains("LogicalPlan::Join { left, right, .. }"));
	assert!(distinct_fence.contains("distinct_input_unsupported_message(left)"));
	assert!(distinct_fence.contains("distinct_input_unsupported_message(right)"));
	assert!(lower_select.contains("wrap_distinct_if_needed(plan, distinct_requested)"));
}

#[test]
pub(crate) fn adr232_distinct_join_binder_lift_preserves_nested_aggregate_fence() {
	let binder = binder_source();
	let tests = source("tests/group_by_test.rs");
	let distinct_fence = source_block(&binder, "fn distinct_input_unsupported_message");

	assert!(distinct_fence.contains("LogicalPlan::Aggregate { .. }"));
	assert!(distinct_fence.contains("DISTINCT over aggregate inputs is not supported"));
	assert!(distinct_fence.contains("LogicalPlan::Join { left, right, .. }"));
	assert!(distinct_fence.contains("distinct_input_unsupported_message(left)"));
	assert!(distinct_fence.contains("distinct_input_unsupported_message(right)"));
	assert!(tests.contains("distinct_over_joined_aggregate_input_still_rejects"));
}

#[test]
pub(crate) fn adr232_distinct_join_remains_bind_eliminated_empty_aggregate() {
	let binder = binder_source();
	let tests = source("tests/group_by_test.rs");

	assert!(binder.contains("LogicalPlan::Distinct { input } =>"));
	assert!(binder.contains("all_output_columns_program(&input.schema())"));
	assert!(binder.contains("ExecutionPlan::Exchange"));
	assert!(binder.contains("self.bind_bound_aggregate(exchange, group_expr, Vec::new())"));
	assert!(!source("src/ir/execution.rs").contains("ExecutionPlan::Distinct"));
	assert!(tests.contains("distinct_join_lowers_to_existing_empty_aggregate_no_new_execution_op"));
}

#[test]
pub(crate) fn adr232_distinct_join_dedup_is_post_join_via_all_column_exchange() {
	let binder = binder_source();
	let fragment = source("src/distributed/fragment.rs");
	let tests = source("tests/group_by_test.rs");
	let distributed = source("tests/distributed_process_test.rs");

	assert!(
		binder.contains(
			"bind_plan_with_trace(input, trace, render_trace, join_order_source_capture)"
		)
	);
	assert!(binder.contains("let group_expr = all_output_columns_program(&input.schema())?;"));
	assert!(binder.contains("input: Box::new(input.plan)"));
	assert!(binder.contains("key: group_expr.clone()"));
	assert!(fragment.contains("fn leading_exchange_before_join"));
	// ADR 247 renamed fragment_post_join_exchange_plan -> fragment_leading_exchange_plan (the shared
	// outermost-exchange peel, now reused for stacked exchanges); the post-join split is unchanged.
	assert!(fragment.contains("fn fragment_leading_exchange_plan"));
	assert!(fragment.contains("FragmentTerminalOutput::ShuffleProducer"));
	assert!(tests.contains("select_distinct_left_outer_join_dedups_after_null_padding"));
	assert!(tests.contains("select_distinct_right_outer_join_dedups_after_null_padding"));
	assert!(tests.contains("select_distinct_full_outer_join_dedups_after_null_padding"));
	assert!(tests.contains("select_distinct_join_star_dedups_full_join_output"));
	assert!(distributed.contains("select_distinct_join_dedups_across_partitions"));
	assert!(distributed.contains("SELECT DISTINCT 1 AS one"));
}

#[test]
pub(crate) fn adr232_distinct_join_ordinal_safe() {
	let binder = binder_source();
	let tests = source("tests/group_by_test.rs");
	let all_columns = source_block(&binder, "fn all_output_columns_program");

	assert!(all_columns.contains("ColumnOrdinal(u32::try_from(idx)"));
	assert!(all_columns.contains("name: ColumnName(field.name().clone())"));
	assert!(
		!all_columns.contains("resolve_column("),
		"DISTINCT join group keys must not resolve duplicate output names by name"
	);
	assert!(tests.contains("select_distinct_join_duplicate_output_names_group_by_position"));
	assert!(tests.contains("Some(ColumnOrdinal(0))"));
	assert!(tests.contains("Some(ColumnOrdinal(1))"));
}

#[test]
pub(crate) fn adr232_distinct_retains_top_level_aggregate_fences() {
	let planner = source("src/sql_planner.rs");
	let tests = source("tests/group_by_test.rs");
	let lower_select = source_block(&planner, "fn lower_select");

	for required in [
		"unsupported(\"DISTINCT with GROUP BY is not supported\")",
		"unsupported(\"DISTINCT with HAVING is not supported\")",
		"\"DISTINCT with aggregate projections is not supported\"",
		"\"DISTINCT with window functions is not supported\"",
	] {
		assert!(
			lower_select.contains(required),
			"lower_select lost {required}"
		);
	}
	assert!(tests.contains("distinct_over_aggregate_inputs_still_rejects"));
	assert!(tests.contains("public_group_by_without_aggregate_still_rejects"));
}

#[test]
pub(crate) fn adr232_distinct_join_adds_no_physical_or_optimizer_surface() {
	adr231_distinct_adds_no_new_execution_plan_or_executor_op();
	adr231_distinct_leaves_optimizer_evidence_surfaces_unchanged();

	for relative in [
		"src/daemon.rs",
		"src/distributed/fragment.rs",
		"src/wire.rs",
		"src/binder/runtime_evidence.rs",
		"src/binder/decision_log.rs",
	] {
		let source = source(relative);
		assert!(
			!source.contains("DISTINCT over joins")
				&& !source.contains("select_distinct_join")
				&& !source.contains("ExecutionPlan::Distinct"),
			"{relative} must not grow a join-DISTINCT runtime surface"
		);
	}
}

#[test]
pub(crate) fn adr234_distinct_aggregate_rewrites_to_project_distinct_then_existing_aggregate() {
	let planner = source("src/sql_planner.rs");
	let lower_select = source_block(&planner, "fn lower_select");
	let rewrite = source_block(&planner, "fn build_distinct_aggregate_rewrite");
	let wrapper = source_block(&planner, "impl DistinctAggregateRewrite");
	let tests = source("tests/group_by_test.rs");

	// ADR 240 generalized the single-dedup rewrite into a fan-out builder; the single-distinct case
	// is subsumed as one dedup branch, still Project -> Distinct -> Aggregate.
	assert!(planner.contains("struct DistinctAggregateRewrite"));
	assert!(lower_select.contains("distinct_rewrite"));
	assert!(lower_select.contains("into_aggregated_plan"));
	assert!(wrapper.contains("LogicalPlan::Distinct"));
	assert!(wrapper.contains("LogicalPlan::Project"));
	assert!(wrapper.contains("LogicalPlan::Aggregate"));
	assert!(rewrite.contains("RecombineBranch"));
	assert!(tests.contains("count_distinct_ignores_null_and_rewrites_to_count_arg"));
	assert!(tests.contains("count_distinct_over_join_uses_distinct_below_group_exchange"));
}

#[test]
pub(crate) fn adr234_distinct_aggregate_keeps_aggregate_function_enum_flagless() {
	let ir = source("src/ir.rs");
	let aggregate_function = ir
		.split("pub enum AggregateFunction")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("AggregateFunction enum");
	let aggregate_plan = source_block(&ir, "Aggregate {");

	assert!(!aggregate_function.contains("Distinct"));
	assert!(!aggregate_function.contains("distinct"));
	assert!(aggregate_plan.contains("aggr_expr: Vec<(AggregateFunction, Vec<Expr>)>"));
	assert!(!aggregate_plan.contains("distinct"));
}

#[test]
pub(crate) fn adr234_count_distinct_rewrites_to_count_arg_not_count_all() {
	let planner = source("src/sql_planner.rs");
	let tests = source("tests/group_by_test.rs");
	let distinct_arg = source_block(&planner, "fn distinct_aggregate_arg");

	assert!(distinct_arg.contains("AggregateFunction::Count"));
	assert!(!distinct_arg.contains("AggregateFunction::CountAll"));
	assert!(tests.contains("assert!(matches!(aggr_expr[0].0, AggregateFunction::Count));"));
	assert!(tests.contains("assert_eq!(scalar_int64_column_rows(&results, 0), vec![Some(4)])"));
}

#[test]
pub(crate) fn adr234_distinct_aggregate_multi_dedup_fences_lifted_by_adr240() {
	// ADR 240 lifted the four ADR-234 single-dedup fences; the rewrite no longer rejects these forms.
	let planner = source("src/sql_planner.rs");
	let tests = source("tests/group_by_test.rs");

	for lifted in [
		"mixed DISTINCT and non-DISTINCT aggregates require multi-dedup fan-out",
		"multiple DISTINCT aggregate arguments require multi-dedup fan-out",
		"AVG(DISTINCT) requires multi-dedup fan-out",
		"computed DISTINCT aggregate arguments require multi-dedup fan-out",
	] {
		assert!(
			!planner.contains(lifted),
			"ADR 240 must lift the fence: {lifted}"
		);
	}
	assert!(tests.contains("distinct_aggregate_multi_dedup_forms_now_plan"));
}

#[test]
pub(crate) fn adr234_derived_distinct_reuses_232_ordinal_safe_binder() {
	let planner = source("src/sql_planner.rs");
	let binder = binder_source();
	let lower_select = source_block(&planner, "fn lower_select");
	let derived_select = source_block(&planner, "fn validate_derived_select_shape");
	let all_columns = source_block(&binder, "fn all_output_columns_program");
	let tests = source("tests/group_by_test.rs");

	assert!(!lower_select.contains("DISTINCT over derived tables is not supported"));
	assert!(!derived_select.contains("select.distinct.is_some()"));
	assert!(lower_select.contains("wrap_distinct_if_needed(plan, distinct_requested)"));
	assert!(all_columns.contains("ColumnOrdinal(u32::try_from(idx)"));
	assert!(!all_columns.contains("resolve_column("));
	assert!(tests.contains("distinct_over_and_inside_derived_tables_is_supported"));
}

#[test]
pub(crate) fn adr234_exists_distinct_is_inert_and_decorrelates_without_distinct_plan() {
	let planner = source("src/sql_planner.rs");
	let exists_select = source_block(&planner, "fn validate_exists_select_shape");
	let exists_join = source_block(&planner, "fn lower_exists_join");
	let tests = source("tests/group_by_test.rs");

	assert!(!exists_select.contains("select.distinct.is_some()"));
	assert!(exists_join.contains("validate_exists_select_shape(select)?"));
	assert!(!exists_join.contains("LogicalPlan::Distinct"));
	assert!(exists_join.contains("JoinKind::LeftSemi"));
	assert!(tests.contains("distinct_inside_exists_is_accepted_and_stripped"));
}

#[test]
pub(crate) fn adr234_adds_no_physical_fragmenter_optimizer_or_wendelon_surface() {
	let execution_plan = source_block(&source("src/ir/execution.rs"), "pub enum ExecutionPlan");
	let fragment = source("src/distributed/fragment.rs");

	assert!(!execution_plan.contains("\n\tDistinct {"));
	assert!(!fragment.contains("ExecutionPlan::Distinct"));
	for relative in [
		"src/binder/runtime_evidence.rs",
		"src/binder/decision_log.rs",
		"src/daemon.rs",
		"src/wire.rs",
	] {
		let source = source(relative);
		assert!(!source.contains("DistinctAggregateRewrite"));
	}
	let wendelon = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<String>();
	assert!(!wendelon.contains("DistinctAggregateRewrite"));
	assert!(!wendelon.contains("ExecutionPlan::Distinct"));
}

#[test]
pub(crate) fn adr233_group_by_over_joins_lifts_both_grouped_fences() {
	let planner = source("src/sql_planner.rs");
	let tests = source("tests/group_by_test.rs");
	let lower_select = source_block(&planner, "fn lower_select");

	assert!(!lower_select.contains("GROUP BY over joins is not supported"));
	assert!(!planner.contains("GROUP BY over joins is not supported"));
	assert!(!planner.contains("fn plan_contains_join"));
	assert!(lower_select.contains("LogicalPlan::Aggregate"));
	assert!(lower_select.contains("AggregateSqlMode::Grouped"));
	assert!(tests.contains("group_by_inner_join_counts_matched_rows"));
	assert!(tests.contains("group_by_decorrelated_exists_aggregates_after_semi_join"));
}

#[test]
pub(crate) fn adr233_group_by_join_reuses_232_post_join_exchange_split() {
	let planner = source("src/sql_planner.rs");
	let fragment = source("src/distributed/fragment.rs");
	let tests = source("tests/group_by_test.rs");
	let distributed = source("tests/distributed_process_test.rs");
	let lower_select = source_block(&planner, "fn lower_select");
	let leading_exchange = source_block(&fragment, "fn leading_exchange_before_join");

	assert!(lower_select.contains("LogicalPlan::Exchange"));
	assert!(lower_select.contains("key: group_expr.clone()"));
	assert!(lower_select.contains("partition_count: DEFAULT_AGGREGATE_PARTITION_COUNT"));
	// ADR 247 renamed fragment_post_join_exchange_plan -> fragment_leading_exchange_plan (the shared
	// outermost-exchange peel, now reused for stacked exchanges); the post-join split is unchanged.
	assert!(fragment.contains("fn fragment_leading_exchange_plan"));
	assert!(leading_exchange.contains("ExecutionPlan::Aggregate { input, .. }"));
	assert!(leading_exchange.contains("ExecutionPlan::Join { .. }"));
	assert!(tests.contains("group_by_join_composes_with_existing_filter_pushdown"));
	assert!(tests.contains("group_by_join_having_filters_after_aggregate"));
	assert!(distributed.contains("group_by_join_real_aggregate_merges_across_partitions"));
}

#[test]
pub(crate) fn adr233_group_by_join_lowers_through_existing_aggregate_no_new_op() {
	let ir_execution = source("src/ir/execution.rs");
	let exec = source("src/exec.rs");
	let fragment = source("src/distributed/fragment.rs");
	let tests = source("tests/group_by_test.rs");
	let execution_plan = source_block(&ir_execution, "pub enum ExecutionPlan");

	assert!(execution_plan.contains("Aggregate {"));
	assert!(!execution_plan.contains("GroupByJoin"));
	assert!(!ir_execution.contains("ExecutionPlan::GroupByJoin"));
	assert!(!exec.contains("execute_group_by_join"));
	assert!(!fragment.contains("GroupByJoin"));
	assert!(tests.contains("global_aggregate_over_join_still_works"));
	assert!(tests.contains("rune_group_by_over_join_matches_sql_plan_and_rows"));
}

#[test]
pub(crate) fn adr233_group_by_join_retains_top_level_distinct_and_derived_shape_fences() {
	let planner = source("src/sql_planner.rs");
	let tests = source("tests/group_by_test.rs");
	let lower_select = source_block(&planner, "fn lower_select");
	let derived_select = source_block(&planner, "fn validate_derived_select_shape");
	let exists_select = source_block(&planner, "fn validate_exists_select_shape");

	for required in [
		"unsupported(\"DISTINCT with GROUP BY is not supported\")",
		"unsupported(\"DISTINCT with HAVING is not supported\")",
		"\"DISTINCT with aggregate projections is not supported\"",
		"GROUP BY requires at least one aggregate call",
	] {
		assert!(
			lower_select.contains(required),
			"lower_select lost {required}"
		);
	}
	// ADR 254 lifts the joins-inside-derived fence (the binder presents a join-bodied subplan as a named
	// relation); the nested-derived-table fence remains the derived-shape boundary.
	assert!(derived_select.contains("nested derived tables are not supported"));
	assert!(exists_select.contains("!group_by_is_empty(&select.group_by)"));
	assert!(tests.contains("public_group_by_without_aggregate_still_rejects"));
	assert!(tests.contains("distinct_with_aggregate_group_by_or_having_is_explicit_unsupported"));
	assert!(tests.contains("derived_table_join_body_executes"));
	assert!(tests.contains("group_by_inside_exists_subquery_still_rejects"));
}

#[test]
pub(crate) fn adr233_join_scoped_aggregation_adds_no_optimizer_surface() {
	adr231_distinct_leaves_optimizer_evidence_surfaces_unchanged();

	for relative in [
		"src/binder/runtime_evidence.rs",
		"src/binder/decision_log.rs",
		"src/daemon.rs",
		"src/distributed/fragment.rs",
		"src/wire.rs",
	] {
		let source = source(relative);
		assert!(
			!source.contains("join_scoped_aggregation")
				&& !source.contains("GroupByJoin")
				&& !source.contains("GROUP BY over joins"),
			"{relative} must not grow an ADR 233 optimizer/runtime surface"
		);
	}
}
