use super::support::*;

fn binder_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source")
}

fn catalog_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog source")
}

fn fragment_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragment source")
}

fn lib_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source")
}

fn wendelon_optimizer_facts_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/optimizer_facts.rs"))
		.expect("read wendelon optimizer facts source")
}

fn wendelon_schema_table_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read wendelon table schema source")
}

fn wendelon_storage_manager_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/manager.rs"))
		.expect("read wendelon storage manager source")
}

fn wendelon_storage_stats_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/stats.rs"))
		.expect("read wendelon storage stats source")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr160_join_cardinality_is_estimate_channel_only() {
	let binder = binder_source();
	let bind_join = source_block(&binder, "async fn bind_join");
	let facts = source_block(&binder, "fn composite_with_join_estimate");

	assert!(bind_join.contains("join_output_ndv_containment_estimate"));
	assert!(bind_join.contains("PlanOptimizerFacts::composite_with_join_estimate"));
	assert!(facts.contains("origin: PlanOrigin::Composite"));
	assert!(facts.contains("byte_upper_bound: None"));
	assert!(facts.contains("row_upper_bound: None"));
	assert!(facts.contains("estimated_row_count"));
	assert!(facts.contains("estimate_materialized_byte_size"));
	assert!(!facts.contains("composite_with_bounds"));
	assert!(binder.contains("join_output_containment_estimate_channel_not_hard_bound"));
}

#[test]
pub(crate) fn adr160_join_containment_requires_clean_base_ndv() {
	let binder = binder_source();
	let lookup = source_block(&binder, "fn clean_base_column_ndv_for_join_input");
	let estimate = source_block(&binder, "fn join_output_ndv_containment_estimate");
	let formula = source_block(&binder, "fn ndv_containment_join_rows");
	let point_formula = source_block(&binder, "fn ndv_containment_join_rows_from_points");
	let denominator_formula =
		source_block(&binder, "fn ndv_containment_join_rows_from_denominator");

	assert!(lookup.contains("ExecutionPlan::Scan"));
	assert!(lookup.contains("projection"));
	assert!(lookup.contains("column_ndv"));
	assert!(!lookup.contains("ExecutionPlan::Filter"));
	assert!(estimate.contains("kind != JoinKind::Inner"));
	assert!(estimate.contains("PlanOrigin::SingleBaseRelation"));
	assert!(estimate.contains("estimate_row_source"));
	assert!(estimate.contains("clean_base_column_ndv_for_join_input"));
	assert!(!estimate.contains("column_ndv_for_input"));
	assert!(formula.contains("point_estimate"));
	assert!(formula.contains("ndv_containment_join_rows_from_points"));
	assert!(point_formula.contains("left_ndv == 0 || right_ndv == 0"));
	assert!(point_formula.contains("ndv_containment_join_rows_from_denominator"));
	assert!(denominator_formula.contains("u128::from(left_rows.get())"));
	assert!(denominator_formula.contains("u64::try_from(estimated).unwrap_or(u64::MAX)"));
	assert!(binder.contains("join_output_containment_skips_filtered_input_ndv"));
	assert!(binder.contains("join_output_containment_skips_non_inner_join"));
}

#[test]
pub(crate) fn adr161_range_overlap_requires_clean_base_range() {
	let binder = binder_source();
	let lookup = source_block(&binder, "fn clean_base_column_range_for_join_input");
	let estimate = source_block(&binder, "fn join_output_ndv_containment_estimate");

	assert!(lookup.contains("ExecutionPlan::Scan"));
	assert!(lookup.contains("projection"));
	assert!(lookup.contains("column_range"));
	assert!(!lookup.contains("ExecutionPlan::Filter"));
	assert!(estimate.contains("clean_base_column_range_for_join_input"));
	assert!(!estimate.contains("column_range_for_input"));
	assert!(binder.contains("clean_base_join_range_maps_scan_projection"));
	assert!(binder.contains("join_range_overlap_skips_filtered_input"));
}

#[test]
pub(crate) fn adr161_range_overlap_only_tightens_containment() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn join_output_ndv_containment_estimate");
	let refined = source_block(&binder, "fn range_refined_containment_join_rows");
	let overlap = source_block(&binder, "fn join_key_range_overlap_fraction");

	assert!(estimate.contains("let base = ndv_containment_join_rows"));
	assert!(estimate.contains("let prior ="));
	assert!(estimate.contains("range_refined_containment_join_rows"));
	assert!(estimate.contains(".unwrap_or(base)"));
	assert!(refined.contains("join_key_range_overlap_fraction"));
	assert!(refined.contains("return Some(RowCount::new(0))"));
	assert!(refined.contains("checked_mul(overlap_width)"));
	assert!(refined.contains("/ min_width"));
	assert!(overlap.contains("let min_width = left_width.min(right_width);"));
	assert!(overlap.contains("let overlap_low = left_min.max(right_min);"));
	assert!(overlap.contains("let overlap_high = left_max.min(right_max);"));
	assert!(overlap.contains("return Some((0, 1));"));
	assert!(binder.contains("range_overlap_refinement_never_exceeds_containment"));
	assert!(binder.contains("contained_range_scores_full_overlap"));
}

#[test]
pub(crate) fn adr161_range_overlap_validates_fact_tag_against_key_type() {
	let binder = binder_source();
	let overlap = source_block(&binder, "fn join_key_range_overlap_fraction");
	let tag_match = source_block(&binder, "fn range_tag_matches_join_key_type");

	assert!(
		overlap
			.contains("range_tag_matches_join_key_type(left_range.arrow_type_tag, left_key_type)")
	);
	assert!(
		overlap.contains(
			"range_tag_matches_join_key_type(right_range.arrow_type_tag, right_key_type)"
		)
	);
	assert!(overlap.contains("left_range.arrow_type_tag != right_range.arrow_type_tag"));
	assert!(overlap.contains("RangeScalar::Integral"));
	assert!(!overlap.contains("RangeScalar::Real"));
	assert!(tag_match.contains("(ARROW_STATS_TAG_INT32, DataType::Int32)"));
	assert!(tag_match.contains("(ARROW_STATS_TAG_INT64, DataType::Int64)"));
	assert!(tag_match.contains("(ARROW_STATS_TAG_DATE32, DataType::Date32)"));
	assert!(tag_match.contains("DataType::Timestamp(unit, Some(timezone))"));
	assert!(tag_match.contains("timezone.as_ref() == \"UTC\""));
	assert!(tag_match.contains("timestamp_fact_unit(arrow_type_tag) == Some(*unit)"));
	assert!(binder.contains("join_range_overlap_fails_open_on_fact_tag_type_mismatch"));
	assert!(binder.contains("range_overlap_real_lane_fails_open_in_helper"));
	assert!(binder.contains("integral_lane_covers_date32_and_timestamp_join_keys"));
}

#[test]
pub(crate) fn adr161_range_overlap_spends_existing_composite_estimated_wire() {
	let binder = binder_source();
	let lib = lib_source();
	let bind_join = source_block(&binder, "async fn bind_join");
	let facts = source_block(&binder, "fn composite_with_join_estimate");

	assert!(bind_join.contains("join_output_ndv_containment_estimate"));
	assert!(bind_join.contains("PlanOptimizerFacts::composite_with_join_estimate"));
	assert!(facts.contains("estimated_row_count"));
	assert!(facts.contains("estimated_byte_size"));
	assert!(facts.contains("estimate_materialized_byte_size"));
	assert!(facts.contains("row_upper_bound: None"));
	assert!(facts.contains("byte_upper_bound: None"));
	assert!(lib.contains("range_disjoint_child_join_broadcasts_zero_estimate_without_replay"));
	assert!(
		lib.contains("stale_disjoint_join_range_underestimate_replays_as_hash_and_matches_digest")
	);
}

#[test]
pub(crate) fn adr161_no_emptyrelation_from_disjoint_join() {
	let binder = binder_source();
	let refined = source_block(&binder, "fn range_refined_containment_join_rows");

	assert!(refined.contains("RowCount::new(0)"));
	assert!(!refined.contains("EmptyRelation"));
	assert!(binder.contains("join_range_overlap_is_estimate_channel_not_emptyrelation"));
}

#[test]
pub(crate) fn adr162_join_skew_is_estimate_channel_only() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn join_output_ndv_containment_estimate");
	let facts = source_block(&binder, "fn composite_with_join_estimate");

	assert!(estimate.contains("hot_both_join_contribution"));
	assert!(estimate.contains("prior.get().max(candidate.get())"));
	assert!(!estimate.contains("row_upper_bound:"));
	assert!(!estimate.contains("byte_upper_bound:"));
	assert!(!estimate.contains("EmptyRelation"));
	assert!(facts.contains("estimated_row_count"));
	assert!(facts.contains("estimated_byte_size"));
	assert!(facts.contains("row_upper_bound: None"));
	assert!(facts.contains("byte_upper_bound: None"));
	assert!(binder.contains("co_hot_join_recovers_skew_and_hashes_without_replay"));
}

#[test]
pub(crate) fn adr162_hot_both_removed_from_residual() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn join_output_ndv_containment_estimate");
	let hot_both = source_block(&binder, "fn hot_both_join_contribution");
	let hot_both_add = source_block(&binder, "impl HotBothJoinContribution");

	assert!(hot_both.contains("left.entries.len() <= right.entries.len()"));
	assert!(hot_both.contains("right.entries.get(key)"));
	assert!(hot_both_add.contains("checked_mul"));
	assert!(hot_both_add.contains("checked_add"));
	assert!(estimate.contains("saturating_sub(hot_both.left_rows)"));
	assert!(estimate.contains("saturating_sub(hot_both.right_rows)"));
	assert!(estimate.contains("saturating_sub(hot_both.count)"));
	assert!(estimate.contains("hot_both.rows.checked_add(residual)"));
	assert!(binder.contains("hot_both_contribution_is_exact_product_of_frequencies"));
	assert!(binder.contains("residual_excludes_hot_both_rows_and_keys_no_double_count"));
}

#[test]
pub(crate) fn adr162_requires_clean_base_heavy_hitter() {
	let binder = binder_source();
	let lookup = source_block(&binder, "fn clean_base_column_heavy_hitter_for_join_input");
	let estimate = source_block(&binder, "fn join_output_ndv_containment_estimate");
	let key_type = source_block(&binder, "fn scalar_literal_key_matches_join_key_type");

	assert!(lookup.contains("ExecutionPlan::Scan"));
	assert!(lookup.contains("projection"));
	assert!(lookup.contains("column_heavy_hitters"));
	assert!(!lookup.contains("ExecutionPlan::Filter"));
	assert!(estimate.contains("clean_base_column_heavy_hitter_for_join_input"));
	assert!(!estimate.contains("column_heavy_hitter_for_input"));
	assert!(key_type.contains("ScalarLiteralKey::Int64"));
	assert!(key_type.contains("DataType::Timestamp(key_unit, Some(timezone))"));
	assert!(key_type.contains("timezone.as_ref() == \"UTC\""));
	assert!(binder.contains("clean_base_join_heavy_hitter_maps_scan_projection"));
	assert!(binder.contains("join_skew_skips_filtered_input"));
}

#[test]
pub(crate) fn adr162_join_skew_keeps_160_161_prior_floor() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn join_output_ndv_containment_estimate");
	let hot_both = source_block(&binder, "fn hot_both_join_contribution");

	assert!(estimate.contains("let prior ="));
	assert!(estimate.contains("return Some(prior)"));
	assert!(estimate.contains("matches!(range_overlap, Some((0, _)))"));
	assert!(estimate.contains("prior.get().max(candidate.get())"));
	assert!(hot_both.contains("heavy_hitter_total_frequency(left, left_rows)?"));
	assert!(hot_both.contains("heavy_hitter_fact_matches_join_key_type"));
	assert!(binder.contains("join_skew_never_lowers_161_prior"));
	assert!(binder.contains("join_skew_fails_open_on_key_type_mismatch_or_invalid_frequency"));
	assert!(binder.contains("disjoint_range_zero_dominates_stale_shared_hot_key"));
}

#[test]
pub(crate) fn adr163_multi_key_denominator_is_product() {
	let binder = binder_source();
	let bind_join = source_block(&binder, "async fn bind_join");
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");
	let denominator = source_block(&binder, "fn ndv_containment_join_rows_from_denominator");
	let multiply = source_block(&binder, "fn multiply_denominator_capped_at_numerator");

	assert!(bind_join.contains("left_keys.len() == right_keys.len() && left_keys.len() >= 2"));
	assert!(bind_join.contains("multi_key_join_cardinality"));
	assert!(estimate.contains("left_keys.len() < 2"));
	assert!(estimate.contains("left_ndv.max(right_ndv)"));
	assert!(estimate.contains("multiply_denominator_capped_at_numerator"));
	assert!(denominator.contains("((numerator - 1) / denominator) + 1"));
	assert!(multiply.contains("checked_mul"));
	assert!(multiply.contains("product.min(numerator)"));
	assert!(binder.contains("multi_key_denominator_is_product_of_per_key_max_ndv"));
	assert!(binder.contains("multi_key_three_key_join_does_not_spuriously_zero"));
	assert!(binder.contains("single_key_join_unchanged_byte_identical_to_162"));
}

#[test]
pub(crate) fn adr163_multi_key_requires_clean_base_ndv() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");

	assert!(estimate.contains("kind != JoinKind::Inner"));
	assert!(estimate.contains("PlanOrigin::SingleBaseRelation"));
	assert!(estimate.contains("estimate_row_source"));
	assert!(estimate.contains("clean_base_column_ndv_for_join_input"));
	assert!(!estimate.contains("column_ndv_for_input"));
	assert!(binder.contains("multi_key_fails_open_when_any_key_lacks_ndv"));
}

#[test]
pub(crate) fn adr163_multi_key_range_fold_reuses_161_fail_open() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");

	assert!(estimate.contains("clean_base_column_range_for_join_input"));
	assert!(estimate.contains("range_refined_containment_join_rows"));
	assert!(estimate.contains(".unwrap_or(refined)"));
	assert!(binder.contains("multi_key_range_overlap_folds_per_key"));
	assert!(binder.contains("multi_key_range_malformed_key_keeps_multi_key_ndv_estimate"));
	assert!(binder.contains("multi_key_disjoint_on_any_key_estimates_empty"));
	assert!(binder.contains("multi_key_three_key_disjoint_estimates_zero"));
}

#[test]
pub(crate) fn adr163_multi_key_defers_heavy_hitter() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");

	assert!(!estimate.contains("column_heavy_hitters"));
	assert!(!estimate.contains("clean_base_column_heavy_hitter_for_join_input"));
	assert!(binder.contains("multi_key_does_not_apply_heavy_hitter"));
}

#[test]
pub(crate) fn adr163_no_wendelon_tuple_top_k_surface() {
	let manifest_dir = manifest_dir();
	let mut offenders = source_lines(&manifest_dir.join("src"));
	offenders.extend(source_lines(&wendelon_manifest_dir().join("src")));
	let offenders = offenders
		.into_iter()
		.filter(|(_, _, line)| line.contains("TupleTopK") || line.contains("tuple_top_k"))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR163 deferred tuple top-k naming; ADR164 owns ColumnGroupTopK, but tuple_top_k remains forbidden:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr164_group_top_k_is_estimate_channel_only() {
	let binder = binder_source();
	let lib = lib_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");
	let facts = source_block(&binder, "fn composite_with_join_estimate");

	assert!(estimate.contains("clean_base_column_group_top_k_for_join_input"));
	assert!(estimate.contains("hot_tuple_join_contribution"));
	assert!(estimate.contains("let hot_floor ="));
	assert!(estimate.contains(".max(u64::try_from(hot_tuple.rows).unwrap_or(u64::MAX))"));
	assert!(!estimate.contains("row_upper_bound:"));
	assert!(!estimate.contains("byte_upper_bound:"));
	assert!(!estimate.contains("EmptyRelation"));
	assert!(facts.contains("estimated_row_count"));
	assert!(facts.contains("estimated_byte_size"));
	assert!(facts.contains("row_upper_bound: None"));
	assert!(facts.contains("byte_upper_bound: None"));
	assert!(binder.contains("multi_key_skew_never_lowers_163_prior"));
	assert!(
		lib.contains("live_column_group_top_k_recovers_multi_key_skew_and_hashes_without_seeding")
	);
}

#[test]
pub(crate) fn adr164_consumer_dark_without_group_top_k() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");

	assert!(estimate.contains("_ => return Some(refined)"));
	assert!(estimate.contains("None => return Some(refined)"));
	assert!(binder.contains("absent_column_group_top_k_falls_back_to_163_byte_identical"));
}

#[test]
pub(crate) fn adr164_consumer_uses_exact_hot_floor_not_fake_residual() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");
	let hot_tuple = source_block(&binder, "fn hot_tuple_join_contribution");
	let hot_add = source_block(&binder, "impl HotBothJoinContribution");

	assert!(hot_tuple.contains("left.entries.len() <= right.entries.len()"));
	assert!(hot_tuple.contains("right.entries.get(key)"));
	assert!(hot_tuple.contains("column_group_top_k_total_frequency(left, left_rows)?"));
	assert!(hot_add.contains("checked_mul"));
	assert!(hot_add.contains("checked_add"));
	assert!(estimate.contains("let hot_floor ="));
	assert!(estimate.contains("return Some(hot_floor);"));
	assert!(estimate.contains("let Some(effective_group_denominator)"));
	assert!(binder.contains("hot_tuple_contribution_is_exact_product_of_frequencies"));
	assert!(binder.contains("consumer_uses_exact_hot_floor_not_fake_residual"));
	assert!(binder.contains("only_one_group_fact_present_is_byte_identical_to_post_165"));
}

#[test]
pub(crate) fn adr164_exact_or_decline_not_approximate_merge() {
	let manager = wendelon_storage_manager_source();
	let stats = wendelon_storage_stats_source();
	let merge = source_block(&manager, "fn column_group_top_k_fact_for_group");
	let collector = source_block(&stats, "impl GroupTopKCollector");

	assert!(merge.contains("payload.group_top_k"));
	assert!(merge.contains("candidate_keys.insert(entry.value_keys.clone())"));
	assert!(merge.contains("binary_search_by(|entry| entry.value_keys.cmp(&key))"));
	assert!(merge.contains("Err(_) if contributor.complete => {}"));
	assert!(merge.contains("Err(_) =>"));
	assert!(merge.contains("known = false"));
	assert!(merge.contains("return Ok(None)"));
	assert!(stats.contains("SpaceSaving<Vec<Vec<u8>>>"));
	assert!(collector.contains("complete: true"));
	assert!(collector.contains("complete: false"));
	assert!(!merge.contains("estimate"));
	assert!(!merge.contains("rank_error"));
	assert!(manager.contains("column_group_top_k_declines_on_partial_coverage"));
}

#[test]
pub(crate) fn adr164_group_top_k_sketch_is_wendelon_private() {
	let manifest_dir = manifest_dir();
	let toron_offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("SpaceSaving<Vec<Vec<u8>>>")
				|| line.contains("ColumnStatsGroupTopKPayload")
				|| line.contains("ColumnStatsGroupTopKEntry")
		})
		.collect::<Vec<_>>();
	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("ScalarLiteralKey"))
		.collect::<Vec<_>>();

	assert!(
		toron_offenders.is_empty(),
		"ADR164 Toron must consume mapped facts, not Wendelon group top-k sketch payloads:\n{}",
		format_offenders(&manifest_dir, &toron_offenders)
	);
	assert!(
		wendelon_offenders.is_empty(),
		"ADR164 Wendelon wire must stay byte-keyed and must not depend on Toron ScalarLiteralKey:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr164_group_top_k_wire_does_not_use_toron_scalar_literal_key() {
	let optimizer_facts = wendelon_optimizer_facts_source();
	let catalog = catalog_source();
	let storage_sync = source_block(&catalog, "fn descriptor_column_group_top_k_from_storage");
	let descriptor_sync = source_block(&catalog, "fn descriptor_column_group_top_k");

	assert!(optimizer_facts.contains("pub struct ColumnGroupTopKFact"));
	assert!(optimizer_facts.contains("pub column_ordinals: Vec<u16>"));
	assert!(optimizer_facts.contains("pub arrow_type_tags: Vec<u8>"));
	assert!(optimizer_facts.contains("pub value_keys: Vec<Vec<u8>>"));
	assert!(!optimizer_facts.contains("ScalarLiteralKey"));
	assert!(storage_sync.contains("scalar_literal_key_from_storage_fact"));
	assert!(storage_sync.contains("scalar_literal_key_matches_data_type"));
	assert!(descriptor_sync.contains("scalar_literal_key_matches_data_type"));
	assert!(catalog.contains("descriptor_sync_maps_supported_column_group_top_k_storage_facts"));
}

#[test]
pub(crate) fn adr164_group_top_k_csb_payload_is_anchored_not_format_bumped() {
	let stats = wendelon_storage_stats_source();
	let attach = source_block(&stats, "fn attach_group_top_k_payloads");
	let pending_finish = source_block(&stats, "fn finish");

	assert!(stats.contains("COLUMN_STATS_BLOCK_FORMAT_VERSION: u32 = 1"));
	assert!(stats.contains("COLUMN_STAT_HAS_GROUP_TOP_K"));
	assert!(stats.contains("COLUMN_STAT_HAS_GROUP_TOP_K: u16 = 1 << 11"));
	assert!(attach.contains("descriptor.stats_policy.column_group_top_k"));
	assert!(attach.contains("let anchor_ordinal = resolved"));
	assert!(attach.contains(".min()"));
	assert!(attach.contains("pending_entry.group_top_k = Some(bytes)"));
	assert!(pending_finish.contains("self.entry.stat_flags |= COLUMN_STAT_HAS_GROUP_TOP_K"));
	assert!(stats.contains("column_group_top_k_builds_tuple_payload_and_skips_null_components"));
}

#[test]
pub(crate) fn adr165_group_ndv_is_estimate_channel_only() {
	let binder = binder_source();
	let lib = lib_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");
	let facts = source_block(&binder, "fn composite_with_join_estimate");

	assert!(estimate.contains("clean_base_column_group_ndv_for_join_input"));
	assert!(estimate.contains("composite_denominator"));
	assert!(estimate.contains("denominator = denominator.min(u128::from(composite_denominator));"));
	assert!(!estimate.contains("row_upper_bound:"));
	assert!(!estimate.contains("byte_upper_bound:"));
	assert!(!estimate.contains("EmptyRelation"));
	assert!(facts.contains("estimated_row_count"));
	assert!(facts.contains("estimated_byte_size"));
	assert!(facts.contains("row_upper_bound: None"));
	assert!(facts.contains("byte_upper_bound: None"));
	assert!(binder.contains("multi_key_group_ndv_caps_product_denominator"));
	assert!(binder.contains("multi_key_group_ndv_never_lowers_163_prior"));
	assert!(lib.contains(
		"live_column_group_ndv_recovers_multi_key_containment_and_hashes_without_seeding"
	));
}

#[test]
pub(crate) fn adr165_consumer_dark_without_group_ndv() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");
	let lookup = source_block(&binder, "fn clean_base_column_group_ndv_for_join_input");

	assert!(lookup.contains("ExecutionPlan::Scan"));
	assert!(lookup.contains("projection"));
	assert!(lookup.contains("column_group_ndv"));
	assert!(!lookup.contains("ExecutionPlan::Filter"));
	assert!(estimate.contains("if let (Some(left_group_ndv), Some(right_group_ndv))"));
	assert!(binder.contains("absent_column_group_ndv_falls_back_to_164_byte_identical"));
	assert!(binder.contains("multi_key_group_ndv_requires_both_sides"));
}

#[test]
pub(crate) fn adr165_group_ndv_cap_is_raising_only() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");

	assert!(estimate.contains("left_group_ndv"));
	assert!(estimate.contains("right_group_ndv"));
	assert!(estimate.contains(".max(right_group_ndv.point_estimate())"));
	assert!(estimate.contains("denominator = denominator.min(u128::from(composite_denominator));"));
	assert!(!estimate.contains("denominator = u128::from(composite_denominator);"));
	assert!(binder.contains("multi_key_group_ndv_caps_product_denominator"));
	assert!(binder.contains("multi_key_group_ndv_never_lowers_163_prior"));
}

#[test]
pub(crate) fn adr165_group_ndv_registers_are_wendelon_private() {
	let manifest_dir = manifest_dir();
	let toron_offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("ColumnStatsGroupNdvPayload")
				|| line.contains("GroupNdvCollector")
				|| line.contains("HllRegisters")
				|| line.contains("hll_distinct.registers")
		})
		.collect::<Vec<_>>();
	let manager = wendelon_storage_manager_source();
	let stats = wendelon_storage_stats_source();
	let merge = source_block(&manager, "fn column_group_ndv_fact_for_group");
	let collector = source_block(&stats, "impl GroupNdvCollector");

	assert!(
		toron_offenders.is_empty(),
		"ADR165 Toron must consume mapped group NDV facts, not Wendelon HLL/register payloads:\n{}",
		format_offenders(&manifest_dir, &toron_offenders)
	);
	assert!(merge.contains("HllRegisters::from_registers"));
	assert!(merge.contains("merged.merge(&registers)"));
	assert!(merge.contains("NdvFact::Exact"));
	assert!(merge.contains("NdvFact::Approx"));
	assert!(collector.contains("hll_insert_encoded_value"));
	assert!(manager.contains("column_group_ndv_merges_hll_when_exact_sets_are_absent"));
}

#[test]
pub(crate) fn adr165_group_ndv_wire_does_not_use_toron_scalar_literal_key() {
	let optimizer_facts = wendelon_optimizer_facts_source();
	let catalog = catalog_source();
	let fact = source_block(&optimizer_facts, "pub struct ColumnGroupNdvFact");
	let storage_sync = source_block(&catalog, "fn descriptor_column_group_ndv_from_storage");
	let descriptor_sync = source_block(&catalog, "fn descriptor_column_group_ndv");

	assert!(fact.contains("pub column_ordinals: Vec<u16>"));
	assert!(fact.contains("pub distinct: NdvFact"));
	assert!(!fact.contains("arrow_type_tags"));
	assert!(!fact.contains("value_keys"));
	assert!(!fact.contains("ScalarLiteralKey"));
	assert!(!fact.contains("registers"));
	assert!(!optimizer_facts.contains("ScalarLiteralKey"));
	assert!(storage_sync.contains("distinct_count_from_storage"));
	assert!(!storage_sync.contains("ScalarLiteralKey"));
	assert!(!descriptor_sync.contains("ScalarLiteralKey"));
	assert!(catalog.contains("descriptor_sync_maps_supported_column_group_ndv_storage_facts"));
	assert!(catalog.contains("descriptor_sync_drops_invalid_column_group_ndv_storage_facts"));
}

#[test]
pub(crate) fn adr165_group_ndv_names_are_brand_clean_and_unsuffixed() {
	let table = wendelon_schema_table_source();
	let optimizer_facts = wendelon_optimizer_facts_source();
	let manifest_dir = manifest_dir();
	let toron_offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("GroupNdvV1")
				|| line.contains("GroupNdvSpecV1")
				|| line.contains("ColumnGroupNdvFactV1")
				|| line.contains("column_group_ndv_v1")
		})
		.collect::<Vec<_>>();
	let wendelon_manifest_dir = wendelon_manifest_dir();
	let wendelon_offenders = source_lines(&wendelon_manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("GroupNdvV1")
				|| line.contains("GroupNdvSpecV1")
				|| line.contains("ColumnGroupNdvFactV1")
				|| line.contains("column_group_ndv_v1")
		})
		.collect::<Vec<_>>();

	assert!(table.contains("pub struct ColumnGroupNdvSpec"));
	assert!(optimizer_facts.contains("pub struct ColumnGroupNdvFact"));
	assert!(
		toron_offenders.is_empty(),
		"ADR165 Toron group NDV names must stay unsuffixed:\n{}",
		format_offenders(&manifest_dir, &toron_offenders)
	);
	assert!(
		wendelon_offenders.is_empty(),
		"ADR165 Wendelon group NDV names must stay unsuffixed:\n{}",
		format_offenders(&wendelon_manifest_dir, &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr166_hot_plus_cold_is_estimate_channel_only() {
	let binder = binder_source();
	let lib = lib_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");
	let facts = source_block(&binder, "fn composite_with_join_estimate");

	assert!(estimate.contains("hot_tuple.rows.checked_add(cold_residual)"));
	assert!(estimate.contains("Some(RowCount::new("));
	assert!(estimate.contains("hot_floor"));
	assert!(estimate.contains(".max(u64::try_from(candidate).unwrap_or(u64::MAX))"));
	assert!(!estimate.contains("row_upper_bound:"));
	assert!(!estimate.contains("byte_upper_bound:"));
	assert!(!estimate.contains("EmptyRelation"));
	assert!(facts.contains("estimated_row_count"));
	assert!(facts.contains("estimated_byte_size"));
	assert!(facts.contains("row_upper_bound: None"));
	assert!(facts.contains("byte_upper_bound: None"));
	assert!(binder.contains("hot_plus_cold_never_lowers_post_165"));
	assert!(lib.contains(
		"live_column_group_hot_cold_recovers_multi_key_skew_and_correlation_without_seeding"
	));
}

#[test]
pub(crate) fn adr166_requires_both_group_facts() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");

	assert!(estimate.contains("effective_group_denominator"));
	assert!(estimate.contains("clean_base_column_group_ndv_for_join_input"));
	assert!(estimate.contains("clean_base_column_group_top_k_for_join_input"));
	assert!(estimate.contains("let Some(mut cold_refined)"));
	assert!(estimate.contains("return Some(hot_floor);"));
	assert!(binder.contains("only_one_group_fact_present_is_byte_identical_to_post_165"));
	assert!(binder.contains("hot_plus_cold_fails_open_without_either_group_fact"));
}

#[test]
pub(crate) fn adr166_hot_plus_cold_removes_hot_from_cold() {
	let binder = binder_source();
	let estimate = source_block(&binder, "fn multi_key_join_cardinality");

	assert!(estimate.contains("saturating_sub(hot_tuple.left_rows)"));
	assert!(estimate.contains("saturating_sub(hot_tuple.right_rows)"));
	assert!(estimate.contains("saturating_sub(u128::from(hot_tuple.count))"));
	assert!(estimate.contains("hot_tuple.rows.checked_add(cold_residual)"));
	assert!(binder.contains("hot_plus_cold_no_double_counts_hot_tuples"));
	assert!(
		binder
			.contains("cold_ndv_uses_effective_composite_not_independence_proxy_or_raw_overcount")
	);
	assert!(binder.contains("cold_ndv_zero_with_cold_rows_declines_to_post_165"));
}

#[test]
pub(crate) fn adr166_no_new_producer_or_wire() {
	let wendelon_manifest_dir = wendelon_manifest_dir();
	let offenders = source_lines(&wendelon_manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("HotCold")
				|| line.contains("hot_cold")
				|| line.contains("ColumnGroupResidual")
				|| line.contains("GroupResidual")
				|| line.contains("group_residual")
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR166 is a Toron-only consumer and must not add Wendelon producer/wire surfaces:\n{}",
		format_offenders(&wendelon_manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr161_no_wendelon_or_descriptor_sidecar_range_overlap_join_surface() {
	let catalog = catalog_source();
	let descriptor_facts = source_block(&catalog, "struct DescriptorOptimizerFacts");
	let storage_sync = source_block(&catalog, "fn descriptor_optimizer_facts_from_storage");
	let offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("join_key_range")
				|| line.contains("JoinRangeOverlap")
				|| line.contains("range-overlap join")
				|| line.contains("range_overlap_join")
		})
		.collect::<Vec<_>>();

	assert!(!descriptor_facts.contains("range_overlap"));
	assert!(!descriptor_facts.contains("join_key_range"));
	assert!(!storage_sync.contains("range_overlap"));
	assert!(!storage_sync.contains("join_key_range"));
	assert!(
		offenders.is_empty(),
		"ADR161 range-overlap join refinement must not alter Wendelon fact surfaces:\n{}",
		format_offenders(&manifest_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr160_composite_broadcast_is_estimate_channel_only() {
	let binder = binder_source();
	let fragment = fragment_source();
	let candidate = source_block(&binder, "fn build_byte_candidate");
	let star_rewrite = source_block(&fragment, "fn rewrite_broadcast_star_probe");

	let single_base = candidate
		.find("PlanOrigin::SingleBaseRelation")
		.expect("single-base build candidate arm");
	let hard = candidate
		.find("BuildByteSource::Hard")
		.expect("hard build candidate source");
	let composite = candidate
		.find("PlanOrigin::Composite")
		.expect("composite build candidate arm");
	let composite_tail = &candidate[composite..];

	assert!(single_base < hard);
	assert!(hard < composite);
	assert!(composite_tail.contains("BuildByteSource::Estimated"));
	assert!(!composite_tail.contains("BuildByteSource::Hard"));
	assert!(binder.contains("composite_hard_bound_build_side_still_rejected"));
	assert!(binder.contains("composite_estimate_channel_broadcasts_without_hard_bound_authority"));
	assert!(fragment.contains("broadcast_composite_build_fragments_to_single_build_producer"));
	assert!(!star_rewrite.contains("contains_join(&build_boundary.input)"));
}

#[test]
pub(crate) fn adr131_build_side_still_requires_single_base_hard_bytes() {
	let binder = binder_source();
	let choose_auto = source_block(&binder, "fn choose_auto_join_strategy");
	let candidate = source_block(&binder, "fn build_byte_candidate");
	let single_base = candidate
		.find("PlanOrigin::SingleBaseRelation")
		.expect("single-base candidate arm");
	let hard = candidate
		.find("BuildByteSource::Hard")
		.expect("hard candidate source");
	let composite = candidate
		.find("PlanOrigin::Composite")
		.expect("composite estimate arm");

	assert!(single_base < hard && hard < composite);
	assert!(
		!candidate[composite..].contains("BuildByteSource::Hard")
			&& candidate[composite..].contains("BuildByteSource::Estimated")
			&& candidate.contains("facts.byte_upper_bound")
			&& candidate.contains("estimated_byte_size")
			&& candidate.contains("build_bytes < threshold")
	);
	assert!(
		choose_auto.contains("left_input.facts.origin != PlanOrigin::SingleBaseRelation")
			&& choose_auto.contains("right_input.facts.origin != PlanOrigin::SingleBaseRelation")
			&& choose_auto.contains("MissingBuildByteUpperBound")
			&& choose_auto.contains("BuildByteUpperBoundAboveThreshold")
	);
}

#[test]
pub(crate) fn adr160_no_theta_minhash_or_join_intersection_fact() {
	let manifest_dir = manifest_dir();
	let mut offenders = source_lines(&manifest_dir.join("src"));
	offenders.extend(source_lines(&wendelon_manifest_dir().join("src")));
	let offenders = offenders
		.into_iter()
		.filter(|(_, _, line)| {
			[
				"Theta",
				"MinHash",
				"JoinIntersection",
				"JoinKeyIntersection",
				"IntersectionFact",
			]
			.iter()
			.any(|forbidden| line.contains(forbidden))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR160 must stay a simple NDV-containment estimate, not a join-intersection fact family:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr160_no_wendelon_or_descriptor_sidecar_join_cardinality_surface() {
	let catalog = catalog_source();
	let descriptor_facts = source_block(&catalog, "struct DescriptorOptimizerFacts");
	let storage_sync = source_block(&catalog, "fn descriptor_optimizer_facts_from_storage");
	let offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("JoinCardinality")
				|| line.contains("join_cardinality")
				|| line.contains("join containment")
				|| line.contains("containment_join")
		})
		.collect::<Vec<_>>();

	assert!(!descriptor_facts.contains("join"));
	assert!(!descriptor_facts.contains("containment"));
	assert!(!storage_sync.contains("join"));
	assert!(
		offenders.is_empty(),
		"ADR160 join cardinality must remain Toron-local and not alter ADR136 sidecar surfaces:\n{}",
		format_offenders(&manifest_dir(), &offenders)
	);
}
