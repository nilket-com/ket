use super::support::*;

fn binder_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source")
}

fn catalog_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog source")
}

fn coordinator_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source")
}

fn lib_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source")
}

fn wendelon_optimizer_facts_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/optimizer_facts.rs"))
		.expect("read Wendelon optimizer facts source")
}

fn wendelon_manager_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon storage manager source")
}

fn wendelon_sketch_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/sketch.rs"))
		.expect("read Wendelon sketch source")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr157_range_selectivity_is_min_max_uniform_no_quantile_fact() {
	let binder = binder_source();
	let lib = lib_source();
	let range = source_block(&binder, "fn filter_range_selectivity_estimated_row_count");
	let estimate = source_block(&binder, "fn estimated_rows_for_range");
	let integral = source_block(&binder, "fn estimated_rows_for_integral_range");
	let real = source_block(&binder, "fn estimated_rows_for_real_range");

	assert!(range.contains("column_range_for_input"));
	assert!(estimate.contains("range.non_null_count"));
	assert!(integral.contains("domain_width"));
	assert!(integral.contains("overlap_width"));
	assert!(integral.contains("div_ceil"));
	assert!(real.contains("clip_real_lower"));
	assert!(real.contains("ceil().max(1.0)"));
	for forbidden in [
		"ColumnQuantileFact",
		"RangeDistributionPayload",
		"column_quantiles",
		"KllSketch",
		"histogram",
		"quantile",
	] {
		assert!(
			!range.contains(forbidden)
				&& !estimate.contains(forbidden)
				&& !integral.contains(forbidden)
				&& !real.contains(forbidden),
			"range selectivity gained distribution marker {forbidden}"
		);
	}
	assert!(binder.contains("range_skew_measurement_does_not_change_estimated_rows_for_range"));
	assert!(lib.contains("range_skew_filter_records_min_max_uniform_miss"));
}

#[test]
pub(crate) fn adr157_no_live_range_distribution_optimizer_fact_or_producer() {
	let catalog = catalog_source();
	let binder = binder_source();
	let schema_facts = wendelon_optimizer_facts_source();
	let manager = wendelon_manager_source();
	let sketch = wendelon_sketch_source();
	let descriptor_sync = source_block(&catalog, "fn descriptor_optimizer_facts_from_storage");
	let table_optimizer_facts = source_block(&manager, "pub async fn table_optimizer_facts");

	assert!(schema_facts.contains("pub struct ColumnRangeFact"));
	assert!(sketch.contains("pub struct KllSketch"));
	assert!(catalog.contains("pub struct ColumnRangeDistributionFact"));
	assert!(binder.contains("fn column_range_distribution_for_input"));
	assert!(descriptor_sync.contains("column_range_distributions: Vec::new()"));
	for forbidden in [
		"ColumnQuantileFact",
		"RangeDistributionPayload",
		"column_quantiles",
		"KllSketch",
	] {
		assert!(
			!schema_facts.contains(forbidden),
			"optimizer facts schema gained range-distribution marker {forbidden}"
		);
		assert!(
			!descriptor_sync.contains(forbidden),
			"descriptor sync gained producer/sketch marker {forbidden}"
		);
		assert!(
			!table_optimizer_facts.contains(forbidden),
			"table optimizer fact producer gained range-distribution marker {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr157_public_explain_surface_stays_closed() {
	let binder = binder_source();
	let explain_schema = source_block(&binder, "fn optimizer_explain_schema");
	let explain_plan = source_block(&binder, "fn optimizer_explain_plan");

	assert!(explain_schema.contains("\"join_index\""));
	assert!(explain_schema.contains("\"strategy\""));
	assert!(explain_schema.contains("\"reason\""));
	for forbidden in [
		"filter_index",
		"estimated_rows",
		"actual_rows",
		"range_skew",
		"uniform_estimate",
		"min_max",
		"quantile",
		"distribution",
	] {
		assert!(
			!explain_schema.contains(forbidden),
			"optimizer EXPLAIN schema gained range-skew field {forbidden}"
		);
		assert!(
			!explain_plan.contains(forbidden),
			"optimizer EXPLAIN plan gained range-skew marker {forbidden}"
		);
	}
	assert!(explain_plan.contains("join_decisions"));
	assert!(!explain_plan.contains("filter_estimates"));
}

#[test]
pub(crate) fn adr157_estimate_channel_and_replay_fences_stay_private() {
	let binder = binder_source();
	let coordinator = coordinator_source();
	let lib = lib_source();
	let with_estimate = source_block(&binder, "fn with_estimated_row_count");
	let build_candidate = source_block(&binder, "fn build_byte_candidate");
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let choose = source_block(&binder, "fn choose_join_strategy");

	assert!(with_estimate.contains("estimated_row_count: Some(estimate)"));
	assert!(!with_estimate.contains("byte_upper_bound: Some"));
	assert!(
		build_candidate.find("facts.byte_upper_bound").unwrap()
			< build_candidate.find("estimated_byte_size").unwrap()
	);
	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(!trigger.contains("ShuffleBytes"));
	assert!(choose.contains("HashAfterBroadcastCap"));
	assert!(lib.contains("range_skew_miss_triggers_broadcast_replay"));
	assert!(lib.contains("range_skew_miss_within_headroom_is_absorbed_no_replay"));
	assert!(lib.contains("run_distributed_attempts_with_replay_signal"));
}

#[test]
pub(crate) fn adr157_min_max_envelope_pruning_unchanged() {
	let binder = binder_source();
	let range = source_block(&binder, "fn filter_range_selectivity_estimated_row_count");
	let integral = source_block(&binder, "fn estimated_rows_for_integral_range");
	let real = source_block(&binder, "fn estimated_rows_for_real_range");

	assert!(integral.contains("if low > high"));
	assert!(integral.contains("Some(RowCount::new(0))"));
	assert!(real.contains("if low.value > high.value"));
	assert!(real.contains("Some(RowCount::new(0))"));
	assert!(!range.contains("EmptyRelation"));
	assert!(binder.contains("range_outside_envelope_estimates_zero_without_hard_bound"));
	assert!(binder.contains("min_max_envelope_pruning_still_correct_out_of_range_is_zero"));
}
