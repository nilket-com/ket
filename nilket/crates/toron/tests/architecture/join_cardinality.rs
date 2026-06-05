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
	assert!(formula.contains("left_ndv == 0 || right_ndv == 0"));
	assert!(formula.contains("u128::from(left_rows.get())"));
	assert!(formula.contains("u64::try_from(estimated).unwrap_or(u64::MAX)"));
	assert!(binder.contains("join_output_containment_skips_filtered_input_ndv"));
	assert!(binder.contains("join_output_containment_skips_multi_key_join"));
	assert!(binder.contains("join_output_containment_skips_non_inner_join"));
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
