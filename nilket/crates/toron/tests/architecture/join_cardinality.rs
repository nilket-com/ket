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
		.filter(|(_, _, line)| {
			[
				"ColumnGroupTopK",
				"TupleTopK",
				"column_group_top_k",
				"tuple_top_k",
			]
			.iter()
			.any(|forbidden| line.contains(forbidden))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR163 defers Wendelon tuple top-k production; no source surface should be added yet:\n{}",
		format_offenders(&manifest_dir, &offenders)
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
