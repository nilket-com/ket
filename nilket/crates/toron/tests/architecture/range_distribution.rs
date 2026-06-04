use super::support::*;

fn binder_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source")
}

fn catalog_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog source")
}

fn execution_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source")
}

fn wendelon_optimizer_facts_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/optimizer_facts.rs"))
		.expect("read Wendelon optimizer facts source")
}

fn wendelon_manager_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read Wendelon storage manager source")
}

fn wendelon_stats_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/stats.rs"))
		.expect("read Wendelon stats source")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr158_range_distribution_consumer_is_storage_activated_by_159() {
	let binder = binder_source();
	let catalog = catalog_source();
	let execution = execution_source();
	let sync = source_block(&catalog, "fn descriptor_optimizer_facts_from_storage");

	assert!(catalog.contains("pub struct ColumnRangeDistributionFact"));
	assert!(catalog.contains("pub struct RangeDistributionBucket"));
	assert!(catalog.contains("fn range_distribution_tag_matches_data_type"));
	assert!(catalog.contains("fn valid_range_distribution_fact"));
	assert!(catalog.contains(
		"pub column_range_distributions: BTreeMap<ColumnOrdinal, ColumnRangeDistributionFact>"
	));
	assert!(
		catalog
			.contains("pub column_range_distributions: Vec<DescriptorColumnRangeDistributionFact>")
	);
	assert!(
		execution.contains(
			"column_range_distributions: Vec<(ColumnOrdinal, ColumnRangeDistributionFact)>"
		)
	);
	assert!(binder.contains("fn column_range_distribution_for_input"));
	assert!(sync.contains("descriptor_column_range_distributions_from_storage"));
	assert!(catalog.contains("fn descriptor_column_range_distributions_from_storage"));
}

#[test]
pub(crate) fn adr159_wendelon_range_distribution_producer_is_live_and_bucketed() {
	let facts = wendelon_optimizer_facts_source();
	let manager = wendelon_manager_source();
	let stats = wendelon_stats_source();
	let table_optimizer_facts = source_block(&manager, "pub async fn table_optimizer_facts");

	assert!(facts.contains("pub struct ColumnRangeDistributionFact"));
	assert!(facts.contains("pub struct RangeDistributionBucket"));
	assert!(facts.contains("pub column_range_distributions"));
	assert!(stats.contains("COLUMN_STAT_HAS_RANGE_DISTRIBUTION"));
	assert!(stats.contains("fn read_kll_range_distribution"));
	assert!(manager.contains("fn range_distribution_fact_for_column"));
	assert!(manager.contains("fn range_distribution_column_candidates"));
	assert!(table_optimizer_facts.contains("column_range_distributions.push(fact)"));
	assert!(!facts.contains("KllSketch"));
	assert!(!facts.contains("RangeDistributionPayload"));
}

#[test]
pub(crate) fn adr158_distribution_refines_in_envelope_only() {
	let binder = binder_source();
	let range = source_block(&binder, "fn filter_range_selectivity_estimated_row_count");
	let integral = source_block(&binder, "fn estimated_rows_for_integral_range");
	let real = source_block(&binder, "fn estimated_rows_for_real_range");

	assert!(range.contains("column_range_for_input"));
	assert!(range.contains("column_range_distribution_for_input"));
	assert!(
		integral.find("if low > high").unwrap()
			< integral
				.find("range_distribution_integral_estimate")
				.unwrap()
	);
	assert!(
		real.find("if low.value > high.value").unwrap()
			< real.find("range_distribution_real_estimate").unwrap()
	);
	assert!(binder.contains("distribution_refines_in_envelope_min_max_still_zeroes_out_of_range"));
}

#[test]
pub(crate) fn adr158_range_distribution_is_estimate_channel_only() {
	let binder = binder_source();
	let range = source_block(&binder, "fn filter_range_selectivity_estimated_row_count");
	let integral_estimate = source_block(&binder, "fn range_distribution_integral_estimate");
	let real_estimate = source_block(&binder, "fn range_distribution_real_estimate");

	assert!(range.contains("estimated_rows_for_range"));
	assert!(!range.contains("row_upper_bound"));
	assert!(!range.contains("byte_upper_bound"));
	assert!(!range.contains("EmptyRelation"));
	assert!(!integral_estimate.contains("row_upper_bound"));
	assert!(!integral_estimate.contains("byte_upper_bound"));
	assert!(!real_estimate.contains("row_upper_bound"));
	assert!(!real_estimate.contains("byte_upper_bound"));
	assert!(binder.contains("range_distribution_estimate_stays_estimate_channel_not_hard_bound"));
}

#[test]
pub(crate) fn adr158_range_distribution_names_are_brand_clean() {
	let mut lines = source_lines(&manifest_dir().join("src"));
	lines.extend(source_lines(&wendelon_manifest_dir().join("src")));
	let offenders = lines
		.into_iter()
		.filter(|(_, _, line)| {
			line.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
				.any(|token| token.contains("RangeDistribution") && token.contains("V1"))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"range-distribution names must not introduce fresh V1 suffixes: {offenders:?}"
	);
}

#[test]
pub(crate) fn adr158_kll_aggregate_state_not_toron_fact_contract() {
	let catalog = catalog_source();
	let fact = source_block(&catalog, "pub struct ColumnRangeDistributionFact");

	assert!(fact.contains("rank_error_ppm"));
	assert!(fact.contains("buckets: Vec<RangeDistributionBucket>"));
	for forbidden in ["Kll", "Sketch", "sketch"] {
		assert!(
			!fact.contains(forbidden),
			"Toron range-distribution fact leaked producer aggregate state marker {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr158_public_explain_surface_stays_closed() {
	let binder = binder_source();
	let explain_schema = source_block(&binder, "fn optimizer_explain_schema");
	let explain_plan = source_block(&binder, "fn optimizer_explain_plan");

	for forbidden in [
		"range_distribution",
		"rank_error",
		"bucket",
		"histogram",
		"quantile",
	] {
		assert!(
			!explain_schema.contains(forbidden),
			"optimizer EXPLAIN schema gained range-distribution field {forbidden}"
		);
		assert!(
			!explain_plan.contains(forbidden),
			"optimizer EXPLAIN plan gained range-distribution marker {forbidden}"
		);
	}
}
