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
pub(crate) fn adr236_fixture_worker_honors_projected_columns() {
	let worker = source("src/bin/toron_fixture_worker.rs");
	let scan = source_block(&worker, "async fn scan(&self, request: ScanRequest)");

	// Behavioral-leaning (ADR 236 D1 / Codex risk note): the real proof is the distributed cardinal
	// `distributed_projection_fidelity_selects_nonzero_column`, which fails if the worker returns the
	// wrong ordinal. Here we only pin that the worker reads the projection field and projects the
	// batch — not the exact call shape or a project/morsel ordering (column projection and row-morsel
	// slicing commute, so the order is not behaviorally load-bearing).
	assert!(
		scan.contains("request.projected_columns"),
		"fixture worker must read projected_columns"
	);
	assert!(
		scan.contains(".project("),
		"fixture worker must project the batch to the requested columns"
	);
}

#[test]
pub(crate) fn adr236_distributed_projection_fidelity_tests_exist() {
	let distributed = source("tests/distributed_process_test.rs");

	for marker in [
		"distributed_projection_fidelity_selects_nonzero_column",
		"distributed_projection_fidelity_preserves_nullable_column",
		"distributed_cross_producer_cardinal_is_value_discriminating",
	] {
		assert!(distributed.contains(marker), "missing {marker}");
	}
}

#[test]
pub(crate) fn adr236_cross_producer_cardinal_rule_documented() {
	let distributed = source("tests/distributed_process_test.rs");
	let helper = source_block(
		&distributed,
		"fn assert_cross_producer_cardinal_is_value_discriminating",
	);

	for marker in [
		"ADR 236 cross-producer cardinal rule",
		"expected value must be unreachable from a single producer",
		"unique_selected_worker_count > 1",
		"supporting evidence, not the proof",
		"discriminating key or row group",
	] {
		assert!(
			distributed.contains(marker),
			"missing checklist marker {marker}"
		);
	}

	assert!(helper.contains("discriminator"));
	assert!(helper.contains("unique_selected_worker_count(selected_workers)"));
	assert!(distributed.contains("intersect_null_safe_matching_across_partitions"));
	assert!(distributed.contains("INTERSECT result [NULL, 9]"));
}

#[test]
pub(crate) fn adr236_no_new_sql_ir_or_operator_surface() {
	let ir = source("src/ir.rs");
	let execution = source("src/ir/execution.rs");
	let exec = source("src/exec.rs");
	let planner = source("src/sql_planner.rs");
	let lower_set_operation = source_block(&planner, "fn lower_set_operation");

	assert!(!ir.contains("Repeat {"));
	assert!(!execution.contains("Repeat {"));
	assert!(!exec.contains("ExecutionPlan::Repeat"));
	assert!(lower_set_operation.contains("INTERSECT ALL is not supported"));
	assert!(lower_set_operation.contains("EXCEPT ALL"));
	assert!(lower_set_operation.contains("MINUS ALL"));
	assert!(!lower_set_operation.contains("Repeat"));
}
