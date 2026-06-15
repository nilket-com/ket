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
pub(crate) fn adr236_projection_fidelity_substrate_stays_behavior_only() {
	let distributed = source("tests/distributed_process_test.rs");
	let worker = source("src/bin/toron_fixture_worker.rs");
	let scan = source_block(&worker, "async fn scan(&self, request: ScanRequest)");

	assert!(scan.contains("request.projected_columns"));
	assert!(scan.contains(".project("));
	assert!(distributed.contains("distributed_projection_fidelity_selects_nonzero_column"));
	assert!(distributed.contains("distributed_projection_fidelity_preserves_nullable_column"));
}

#[test]
pub(crate) fn adr238_fixture_worker_applies_pbc_before_projection() {
	let worker = source("src/bin/toron_fixture_worker.rs");
	let scan = source_block(&worker, "async fn scan(&self, request: ScanRequest)");

	// Behavioral-leaning (the 236 F1 lesson): only pin that the worker reads pbc_predicate and calls
	// the PBC engine. The distributed_predicate_fidelity_* value-discriminating tests are the real
	// ordering/correctness proof — not a brittle `find(pbc) < find(project)` source-ordering pin.
	assert!(scan.contains("request.pbc_predicate"));
	assert!(scan.contains("pbc::execute"));
}

#[test]
pub(crate) fn adr238_reuses_production_pbc_evaluator() {
	let worker = source("src/bin/toron_fixture_worker.rs");
	let storage = source("src/storage.rs");

	// The worker reuses the *production* evaluator (the same call the real in-memory scan makes),
	// not a fixture-local predicate interpreter.
	assert!(worker.contains("wendelon::schema::pbc::execute"));
	assert!(storage.contains("wendelon::schema::pbc::execute"));
}

#[test]
pub(crate) fn adr238_predicate_fidelity_tests_are_value_discriminating_and_assert_pushed() {
	let distributed = source("tests/distributed_process_test.rs");

	for marker in [
		"distributed_predicate_fidelity_does_not_ignore_pbc_predicate",
		"distributed_predicate_fidelity_filters_before_projection",
		"distributed_predicate_fidelity_preserves_null_semantics",
		"distributed_predicate_fidelity_composes_with_scan_share",
	] {
		assert!(distributed.contains(marker), "missing {marker}");
	}
	// Each gate confirms the predicate genuinely fully-pushes (pbc on the scan, no residual Toron
	// re-check) — else a perfectly-honoring fixture proves nothing over an un-pushed query (D5).
	assert!(distributed.contains("fn assert_predicate_fully_pushes"));
	assert!(distributed.contains("collect_scan_pbc_states"));
	assert!(distributed.contains("plan_has_residual_recheck"));
}

#[test]
pub(crate) fn adr238_no_new_sql_ir_operator_or_wendelon_production_surface() {
	// Test-only: the worker reuses the pre-existing `ScanRequest.pbc_predicate` field and the
	// production PBC evaluator; no new IR/operator/predicate surface is introduced for fidelity.
	let storage = source("src/storage.rs");
	assert!(storage.contains("pub pbc_predicate"));
	assert!(!source("src/ir/execution.rs").contains("PredicateFidelity"));
}

#[test]
pub(crate) fn adr238_projection_fidelity_guards_remain_intact() {
	// 238 composes with 236; the projection guard + tests must still exist.
	let guards = source("tests/architecture/distributed_fixture.rs");
	let distributed = source("tests/distributed_process_test.rs");
	assert!(guards.contains("fn adr236_fixture_worker_honors_projected_columns"));
	assert!(distributed.contains("distributed_projection_fidelity_selects_nonzero_column"));
}
