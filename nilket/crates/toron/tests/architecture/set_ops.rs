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
	assert!(lower_set_operation.contains("LogicalPlan::SetOpAll"));
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

#[test]
pub(crate) fn adr237_set_op_all_desugars_to_count_join_repeat() {
	let planner = source("src/sql_planner.rs");
	let lower_set_operation = source_block(&planner, "fn lower_set_operation");
	let binder = binder_source();
	let bind_set_op_all = source_block(&binder, "async fn bind_set_op_all");
	let bind_set_op_all_count_arm = source_block(&binder, "fn bind_set_op_all_count_arm");

	assert!(lower_set_operation.contains("SetOperator::Intersect"));
	assert!(lower_set_operation.contains("SetOpAllKind::Intersect"));
	assert!(lower_set_operation.contains("SetOperator::Except | SetOperator::Minus"));
	assert!(lower_set_operation.contains("SetOpAllKind::Except"));
	assert!(bind_set_op_all.contains("bind_set_op_all_count_arm"));
	assert!(bind_set_op_all_count_arm.contains("AggregateFunction::CountAll"));
	assert!(bind_set_op_all.contains("JoinKind::Inner"));
	assert!(bind_set_op_all.contains("JoinKind::LeftOuter"));
	assert!(bind_set_op_all.contains("bind_scoped_join"));
	assert!(bind_set_op_all.contains("\n\t\t\ttrue,"));
	assert!(bind_set_op_all.contains("ExecutionPlan::Repeat"));
}

#[test]
pub(crate) fn adr237_repeat_is_the_only_new_physical_multiplicity_primitive() {
	let ir = source("src/ir.rs");
	let execution = source("src/ir/execution.rs");
	let exec = source("src/exec.rs");

	assert!(ir.contains("SetOpAll"));
	assert!(execution.contains("Repeat {"));
	assert!(exec.contains("ExecutionPlan::Repeat"));
	assert!(!execution.contains("IntersectAll {"));
	assert!(!execution.contains("ExceptAll {"));
	assert!(!execution.contains("MinusAll {"));
}

#[test]
pub(crate) fn adr237_repeat_consumes_integer_count_column_and_rejects_invalid_counts() {
	let binder = binder_source();
	let repeat_count_column = source_block(&binder, "fn repeat_count_column");
	let repeat_output_schema = source_block(&binder, "fn repeat_output_schema");
	let exec = source("src/exec.rs");
	let execute_repeat = source_block(&exec, "fn execute_repeat");

	assert!(repeat_count_column.contains("DataType::Int64"));
	assert!(repeat_count_column.contains("REPEAT count expression must resolve to a column"));
	assert!(repeat_count_column.contains("REPEAT count column must be Int64"));
	assert!(repeat_output_schema.contains(".enumerate()"));
	assert!(execute_repeat.contains("downcast_ref::<Int64Array>()"));
	assert!(execute_repeat.contains("REPEAT count must not be NULL"));
	assert!(execute_repeat.contains("REPEAT count must not be negative"));
	assert!(execute_repeat.contains("task_budget.memory"));
}

#[test]
pub(crate) fn adr237_behavior_tests_cover_multiplicity_surface() {
	let tests = source("tests/group_by_test.rs");
	let distributed = source("tests/distributed_process_test.rs");

	for marker in [
		"intersect_all_is_min_not_distinct",
		"except_all_subtracts_exactly",
		"intersect_except_all_are_null_equal",
		"except_all_zero_count_emits_no_row",
		"repeat_zero_count_emits_no_row",
		"repeat_rejects_null_and_negative_counts",
		"minus_all_aliases_except_all",
		"INTERSECT ALL BY NAME",
	] {
		assert!(tests.contains(marker), "missing {marker}");
	}
	assert!(
		distributed.contains("set_op_all_distributed_multiplicity_across_producers"),
		"missing distributed set-op ALL multiplicity test"
	);
	assert!(
		distributed.contains("EXCEPT ALL result [NULL, 7]"),
		"distributed set-op ALL gate must document the cross-producer discriminator"
	);
}
