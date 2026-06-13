use super::support::*;

fn catalog_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog source")
}

fn lib_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source")
}

fn wendelon_schema_table_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema source")
}

fn wendelon_optimizer_facts_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/optimizer_facts.rs"))
		.expect("read Wendelon optimizer facts source")
}

fn wendelon_manager_source() -> String {
	wendelon_storage_manager_source()
}

fn wendelon_stats_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/stats.rs"))
		.expect("read Wendelon stats source")
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
pub(crate) fn adr156_heavy_hitter_producer_is_opt_in_and_default_empty() {
	let table = wendelon_schema_table_source();
	let manager = wendelon_manager_source();

	assert!(table.contains("pub heavy_hitters: Vec<ColumnHeavyHitterSpec>"));
	assert!(
		table.contains("#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]")
	);
	assert!(table.contains("pub struct ColumnHeavyHitterSpec"));
	assert!(!table.contains("ColumnHeavyHitterSpecV1"));
	assert!(table.contains("validate_heavy_hitter_stats_policy"));
	assert!(manager.contains("descriptor.stats_policy.heavy_hitters"));
	assert!(manager.contains("fn heavy_hitter_column_candidates"));
}

#[test]
pub(crate) fn adr156_heavy_hitter_csb_payload_is_stats_not_request_scan() {
	let stats = wendelon_stats_source();
	let manager = wendelon_manager_source();
	let table_optimizer_facts = source_block(&manager, "pub async fn table_optimizer_facts");

	assert!(stats.contains("COLUMN_STAT_HAS_HEAVY_HITTERS"));
	assert!(stats.contains("ColumnStatsHeavyHitterPayload"));
	assert!(stats.contains("build_column_stats_block_internal"));
	assert!(manager.contains("build_tiered_column_stats_block"));
	assert!(!table_optimizer_facts.contains("RecordBatch"));
	assert!(!table_optimizer_facts.contains("decode_record_batch"));
}

#[test]
pub(crate) fn adr156_heavy_hitter_frequencies_are_exact_for_emitted_keys() {
	let stats = wendelon_stats_source();
	let sketch = wendelon_sketch_source();

	assert!(sketch.contains("pub struct SpaceSaving"));
	assert!(stats.contains("frequencies: BTreeMap<Vec<u8>, u64>"));
	assert!(stats.contains("frequency.checked_add(1)"));
	assert!(stats.contains("heavy_hitter_stats_build_counts_emitted_keys_exactly"));
	assert!(stats.contains("heavy_hitter_complete_flag_requires_exact_distinct_and_byte_fit"));
}

#[test]
pub(crate) fn adr156_incomplete_absence_is_unknown_not_zero() {
	let manager = wendelon_manager_source();
	let merge = source_block(&manager, "fn heavy_hitter_fact_for_column");

	assert!(merge.contains("Err(_) if contributor.complete => {}"));
	assert!(merge.contains("Err(_) =>"));
	assert!(merge.contains("known = false"));
	assert!(manager.contains("incomplete_contributor_topk_absence_is_unknown_not_zero"));
}

#[test]
pub(crate) fn adr156_no_count_min_or_approx_frequency_consumer() {
	let offenders = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.chain(source_lines(&wendelon_manifest_dir().join("src")))
		.filter(|(_, _, line)| {
			["CountMin", "Count-Min"]
				.iter()
				.any(|marker| line.contains(marker))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR156 must not introduce Count-Min: {offenders:?}"
	);
}

#[test]
pub(crate) fn adr156_storage_wire_does_not_use_toron_scalar_literal_key() {
	let facts = wendelon_optimizer_facts_source();
	let client =
		std::fs::read_to_string(wendelon_manifest_dir().join("crates/wendelon-client/src/lib.rs"))
			.expect("read Wendelon client source");

	assert!(facts.contains("pub struct ColumnHeavyHitterFact"));
	assert!(facts.contains("pub struct ColumnHeavyHitterEntry"));
	assert!(facts.contains("value_key: Vec<u8>"));
	assert!(facts.contains("frequency: u64"));
	assert!(!facts.contains("ScalarLiteralKey"));
	assert!(client.contains("ColumnHeavyHitterFact"));
}

#[test]
pub(crate) fn adr156_descriptor_sync_maps_heavy_hitters_fail_open() {
	let catalog = catalog_source();
	let lib = lib_source();
	let sync = source_block(&catalog, "fn descriptor_optimizer_facts_from_storage");
	let mapper = source_block(&catalog, "fn descriptor_column_heavy_hitters_from_storage");

	assert!(sync.contains("descriptor_column_heavy_hitters_from_storage"));
	assert!(mapper.contains("frequency == 0"));
	assert!(mapper.contains("entries.insert"));
	assert!(mapper.contains("frequency_sum > row_upper_bound"));
	assert!(catalog.contains("fn scalar_literal_key_from_storage_fact"));
	assert!(catalog.contains("descriptor_sync_maps_supported_heavy_hitter_storage_facts"));
	assert!(
		catalog.contains(
			"descriptor_sync_drops_invalid_zero_duplicate_or_over_base_heavy_hitter_fact"
		)
	);
	assert!(lib.contains("produced_heavy_hitter_fact_makes_hot_key_filter_hash_without_seeding"));
}

#[test]
pub(crate) fn adr155_heavy_hitter_is_estimate_only_consumer() {
	let binder = binder_source();
	let equality = source_block(&binder, "fn heavy_hitter_equality_estimate");
	let in_list = source_block(&binder, "fn heavy_hitter_in_list_estimate");
	let tail = source_block(&binder, "fn heavy_hitter_uniform_tail_from_total");

	assert!(binder.contains("fn column_heavy_hitter_for_input"));
	assert!(binder.contains("fn heavy_hitter_total_frequency"));
	assert!(binder.contains("hot_key_equality_uses_heavy_hitter_frequency"));
	assert!(binder.contains("heavy_hitter_estimate_stays_estimate_channel_not_hard_bound"));
	for block in [equality, in_list, tail] {
		for forbidden in ["row_upper_bound", "byte_upper_bound", "EmptyRelation"] {
			assert!(
				!block.contains(forbidden),
				"heavy-hitter consumer must not write hard-bound surface {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr155_no_wendelon_heavy_hitter_producer_or_count_min() {
	let count_min_offenders = source_lines(&manifest_dir().join("src"))
		.into_iter()
		.chain(source_lines(&wendelon_manifest_dir().join("src")))
		.filter(|(_, _, line)| {
			["CountMin", "Count-Min"]
				.iter()
				.any(|marker| line.contains(marker))
		})
		.collect::<Vec<_>>();

	assert!(
		count_min_offenders.is_empty(),
		"ADR155 must not introduce Count-Min sketch code: {count_min_offenders:?}"
	);
}

#[test]
pub(crate) fn adr155_descriptor_sync_emits_empty_heavy_hitter_facts() {
	let catalog = catalog_source();
	let sync = source_block(&catalog, "fn descriptor_optimizer_facts_from_storage");

	assert!(catalog.contains("ColumnHeavyHitterFact"));
	assert!(catalog.contains("DescriptorColumnHeavyHitterFact"));
	assert!(sync.contains("descriptor_column_heavy_hitters_from_storage"));
	assert!(catalog.contains("descriptor_sync_maps_supported_heavy_hitter_storage_facts"));
	assert!(
		catalog.contains(
			"descriptor_sync_drops_invalid_zero_duplicate_or_over_base_heavy_hitter_fact"
		)
	);
}
