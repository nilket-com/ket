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
pub(crate) fn adr235_set_ops_reuse_union_distinct_and_membership_joins() {
	let planner = source("src/sql_planner.rs");
	let lower_set_operation = source_block(&planner, "fn lower_set_operation");

	assert!(lower_set_operation.contains("SetOperator::Union"));
	assert!(lower_set_operation.contains("LogicalPlan::Union"));
	assert!(lower_set_operation.contains("LogicalPlan::Distinct"));
	assert!(lower_set_operation.contains("SetOperator::Intersect"));
	assert!(lower_set_operation.contains("JoinKind::LeftSemi"));
	assert!(lower_set_operation.contains("SetOperator::Except | SetOperator::Minus"));
	assert!(lower_set_operation.contains("JoinKind::LeftAnti"));
	assert!(planner.contains("fn lower_distinct_membership_set_op"));
	assert!(planner.contains("null_safe_keys: true"));
	assert!(lower_set_operation.contains("INTERSECT ALL is not supported"));
	assert!(lower_set_operation.contains("EXCEPT ALL"));
	assert!(lower_set_operation.contains("MINUS ALL"));
}

#[test]
pub(crate) fn adr235_union_is_only_concat_execution_state() {
	let ir = source("src/ir.rs");
	let execution = source("src/ir/execution.rs");
	let exec = source("src/exec.rs");
	let fragment = source("src/distributed/fragment.rs");

	assert!(ir.contains("Union {\n\t\tleft: Box<LogicalPlan>,\n\t\tright: Box<LogicalPlan>,\n\t}"));
	assert!(execution.contains("Union {\n\t\tleft: Box<ExecutionPlan>,"));
	assert!(exec.contains(
		"ExecutionPlan::Union {\n\t\t\t\t\tleft,\n\t\t\t\t\tright,\n\t\t\t\t\toutput_schema,"
	));
	assert!(exec.contains("LogicalPlan::Union { left, right }"));
	assert!(fragment.contains("fn fragment_union_plan"));
	assert!(fragment.contains("fn union_producer_tasks"));
	assert!(fragment.contains("share: Some(ScanShare"));
	assert!(!execution.contains("Intersect {"));
	assert!(!execution.contains("Except {"));
	assert!(!execution.contains("Minus {"));
}

#[test]
pub(crate) fn adr235_set_membership_join_path_is_private_and_null_safe() {
	let binder = binder_source();
	let bind_scoped_join = source_block(&binder, "fn bind_scoped_join");
	let exec = source("src/exec.rs");

	assert!(bind_scoped_join.contains("if null_safe_keys && on.is_empty()"));
	assert!(bind_scoped_join.contains("set_op_output_schema"));
	assert!(bind_scoped_join.contains("normalize_set_op_arm"));
	assert!(bind_scoped_join.contains("ColumnOrdinal"));
	assert!(exec.contains("fn join_key_bytes_null_safe"));
	assert!(exec.contains("encode_partition_key_value"));
}

#[test]
pub(crate) fn adr235_behavior_tests_cover_core_sql_surface() {
	let tests = source("tests/group_by_test.rs");
	let distributed = source("tests/distributed_process_test.rs");

	for marker in [
		"sql_union_all_preserves_duplicates_and_left_schema",
		"sql_union_distinct_deduplicates_union_all_output",
		"sql_intersect_and_except_use_null_equal_set_membership",
		"ordinary_join_stays_null_unequal",
		"sql_rejects_deferred_set_operation_forms",
	] {
		assert!(tests.contains(marker), "missing {marker}");
	}
	for marker in [
		"union_all_scans_both_arms_across_workers",
		"union_distinct_dedups_across_partitions",
		"intersect_null_safe_matching_across_partitions",
	] {
		assert!(distributed.contains(marker), "missing {marker}");
	}
}
