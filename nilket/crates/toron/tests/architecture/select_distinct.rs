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
pub(crate) fn adr231_distinct_first_cut_fences_joins_subqueries_and_aggregates() {
	let planner = source("src/sql_planner.rs");
	let binder = binder_source();
	let lower_select = source_block(&planner, "fn lower_select");
	let derived_select = source_block(&planner, "fn validate_derived_select_shape");
	let exists_select = source_block(&planner, "fn validate_exists_select_shape");
	let distinct_fence = source_block(&binder, "fn distinct_input_unsupported_message");

	for required in [
		"Some(Distinct::On(_)) => return Err(unsupported(\"DISTINCT ON is not supported\"))",
		"Some(Distinct::All) => return Err(unsupported(\"SELECT ALL is not supported\"))",
		"unsupported(\"DISTINCT with GROUP BY is not supported\")",
		"unsupported(\"DISTINCT with HAVING is not supported\")",
		"\"DISTINCT with aggregate projections is not supported\"",
		"\"DISTINCT with window functions is not supported\"",
		"unsupported(\"DISTINCT over joins is not supported\")",
		"unsupported(\"DISTINCT over derived tables is not supported\")",
	] {
		assert!(
			lower_select.contains(required),
			"lower_select lost {required}"
		);
	}
	assert!(derived_select.contains("select.distinct.is_some()"));
	assert!(exists_select.contains("select.distinct.is_some()"));
	assert!(distinct_fence.contains("LogicalPlan::Join { .. }"));
	assert!(distinct_fence.contains("DISTINCT over joins is not supported"));
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
