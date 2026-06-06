use super::support::*;

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
pub(crate) fn adr157_range_distribution_activation_is_later_adr158_159_work() {
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
	assert!(schema_facts.contains("pub struct ColumnRangeDistributionFact"));
	assert!(descriptor_sync.contains("descriptor_column_range_distributions_from_storage"));
	assert!(table_optimizer_facts.contains("column_range_distributions"));
	for forbidden in [
		"ColumnQuantileFact",
		"RangeDistributionPayload",
		"column_quantiles",
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
	assert!(
		!schema_facts.contains("KllSketch") && !descriptor_sync.contains("KllSketch"),
		"KLL sketch state must stay out of the optimizer fact wire and Toron descriptor sync"
	);
}

#[test]
pub(crate) fn adr157_public_explain_surface_stays_closed() {
	let binder = binder_source();
	let explain_schema = source_block(&binder, "fn optimizer_explain_schema");
	let explain_plan = source_block(&binder, "fn optimizer_explain_plan");

	assert!(explain_schema.contains("\"decision_kind\""));
	assert!(explain_schema.contains("\"outcome\""));
	assert!(explain_schema.contains("\"detail\""));
	for forbidden in [
		"filter_index",
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
	assert!(explain_plan.contains("scan_filter_decisions"));
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

#[test]
pub(crate) fn adr141_139_140_replay_fences_untouched() {
	let coordinator = coordinator_source();
	let binder = binder_source();
	let lib = lib_source();
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let config = source_block(&binder, "pub struct PlannerConfig");

	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(!trigger.contains("ShuffleBytes"));
	assert!(coordinator.contains("let mut retried_broadcast_replay = false"));
	assert!(config.contains("force_hash_join_ids: BTreeSet<JoinReplayId>"));
	assert!(lib.contains("ndv_underestimate_over_cap_replays_as_hash"));
}

#[test]
pub(crate) fn adr154_uniform_ndv_math_stays_current_behavior() {
	let binder = binder_source();
	let equality = source_block(&binder, "fn equality_ndv_estimated_row_count");
	let in_list = source_block(&binder, "fn in_list_ndv_estimated_row_count");
	let estimator = source_block(&binder, "fn estimated_rows_for_ndv");
	let lib = lib_source();

	assert!(equality.contains("estimated_rows_for_ndv"));
	assert!(equality.contains("distinct_count.point_estimate()"));
	assert!(in_list.contains("estimated_rows_for_ndv"));
	assert!(in_list.contains("distinct_literal_count"));
	assert!(estimator.contains("let divisor = u128::from(ndv.max(1))"));
	assert!(estimator.contains("/ divisor"));
	assert!(binder.contains("skewed_hot_key_filter_records_uniform_ndv_miss"));
	assert!(binder.contains("uniform_ndv_hot_key_in_list_miss_is_measured"));
	assert!(lib.contains("uniform_ndv_hot_key_miss_triggers_broadcast_replay"));
	assert!(lib.contains("uniform_ndv_miss_within_headroom_is_absorbed_no_replay"));
	assert!(binder.contains("missing_heavy_hitter_fact_preserves_141_148_uniform_ndv_behavior"));
}

#[test]
pub(crate) fn adr154_no_public_filter_explain_surface() {
	let binder = binder_source();
	let explain_schema = source_block(&binder, "fn optimizer_explain_schema");
	let explain_plan = source_block(&binder, "fn optimizer_explain_plan");

	assert!(explain_schema.contains("\"decision_kind\""));
	assert!(explain_schema.contains("\"outcome\""));
	assert!(explain_schema.contains("\"detail\""));
	for forbidden in ["filter_index", "actual_rows", "hot_key", "skew"] {
		assert!(
			!explain_schema.contains(forbidden),
			"optimizer EXPLAIN schema gained filter/skew column {forbidden}"
		);
	}
	assert!(explain_plan.contains("join_decisions"));
	assert!(explain_plan.contains("scan_filter_decisions"));
	assert!(!explain_plan.contains("filter_estimates"));
}

#[test]
pub(crate) fn adr154_estimate_channel_and_replay_fences_untouched() {
	let binder = binder_source();
	let coordinator = coordinator_source();
	let with_estimate = source_block(&binder, "fn with_estimated_row_count");
	let build_candidate = source_block(&binder, "fn build_byte_candidate");
	let trigger = source_block(&coordinator, "fn broadcast_cap_replay_trigger");
	let lib = lib_source();

	assert!(with_estimate.contains("estimated_row_count: Some(estimate)"));
	assert!(!with_estimate.contains("byte_upper_bound: Some"));
	assert!(
		build_candidate.find("facts.byte_upper_bound").unwrap()
			< build_candidate.find("estimated_byte_size").unwrap()
	);
	assert!(trigger.contains("BudgetDimension::BroadcastReplication"));
	assert!(!trigger.contains("ShuffleBytes"));
	assert!(binder.contains("skew_miss_measurement_does_not_change_estimated_row_count"));
	assert!(lib.contains("run_distributed_attempts_with_replay_signal"));
}
