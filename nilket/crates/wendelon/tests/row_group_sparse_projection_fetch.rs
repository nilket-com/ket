use std::path::Path;

fn read_module_concat(path: impl AsRef<std::path::Path>, include_tests: bool) -> String {
	let path = path.as_ref();
	let mut source = std::fs::read_to_string(path).unwrap();
	// ADR 078: storage/manager.rs is now a module dir. Production guards exclude
	// tests.rs; behavior-coverage guards opt into it explicitly.
	if path.file_name().and_then(|s| s.to_str()) == Some("manager.rs")
		&& path
			.parent()
			.and_then(|p| p.file_name())
			.and_then(|s| s.to_str())
			== Some("storage")
	{
		let dir = path.with_extension("");
		if dir.is_dir() {
			let mut subs: Vec<_> = std::fs::read_dir(&dir)
				.unwrap()
				.flatten()
				.map(|e| e.path())
				.filter(|p| {
					p.extension().and_then(|s| s.to_str()) == Some("rs")
						&& (include_tests
							|| p.file_name().and_then(|s| s.to_str()) != Some("tests.rs"))
				})
				.collect();
			subs.sort();
			for p in subs {
				source.push('\n');
				source.push_str(&std::fs::read_to_string(p).unwrap());
			}
		}
	}
	source
}

fn source(relative: &str) -> String {
	read_module_concat(Path::new(env!("CARGO_MANIFEST_DIR")).join(relative), false)
}

fn source_with_tests(relative: &str) -> String {
	read_module_concat(Path::new(env!("CARGO_MANIFEST_DIR")).join(relative), true)
}

#[test]
fn sparse_projection_fetch_target_binds_planner_unit_matrix() {
	let sparse = source("src/storage/encoding/sparse_projection_fetch.rs");
	for required in [
		"pub(crate) const MAX_SPARSE_RANGES_PER_BUFFER",
		"pub(crate) fn decide_sparse_projection_fetch",
		"pub(crate) enum SparseProjectionFetchDecline",
		"MissingExactCocdMask",
		"CascadeNotPageEmitting",
		"CascadeNotProjectionEligible",
		"EncodedValidityBufferUnsupported",
		"ValidityBufferRangeMissing",
		"NullableTypeNotInCatalog",
		"VariableWidthProjectedColumn",
		"PageGeometryMismatchAcrossColumns",
		"ByteEstimateNotSparse",
		"SimdPageCapExceeded",
		"RangeCapExceeded",
		"PerBufferFragmentationCapExceeded",
		"SelectedByteFractionExceeded",
		"Disabled",
		"none_mask_short_circuits_before_projection_column_eligibility",
		"clustered_mask_selects_only_touched_projection_pages",
		"nullable_projection_declines_when_validity_range_is_missing",
		"encoded_validity_detection_for_one_row_group_does_not_poison_another",
		"fragmented_mask_declines_before_tiered_range_planning",
	] {
		assert!(
			sparse.contains(required),
			"sparse planner source is missing ADR 060 token {required}"
		);
	}
}

#[test]
fn sparse_projection_fetch_target_binds_runtime_dense_oracle() {
	let manager = source("src/storage/manager.rs");
	let manager_with_tests = source_with_tests("src/storage/manager.rs");
	for required in ["read_projected_record_from_l2"] {
		assert!(
			manager.contains(required),
			"manager runtime oracle is missing ADR 060 token {required}"
		);
	}
	for required in [
		"sparse_projection_fetch_materializes_exact_cocd_clustered_pages",
		"OP_LT",
		"execute_program(&predicate, &batch)",
		"phase_c_fetch.selected_plaintext_ranges",
		"sparse projection should not fetch the whole projected payload buffer",
	] {
		assert!(
			manager_with_tests.contains(required),
			"manager runtime oracle is missing ADR 060 test token {required}"
		);
	}
}

#[test]
fn adr066_fsst_case_a_binds_sparse_projection_composition() {
	let manager = source("src/storage/manager.rs");
	let manager_with_tests = source_with_tests("src/storage/manager.rs");
	let bench = source("benches/row_group_fetch.rs");
	for required in ["read_projected_record_from_l2"] {
		assert!(
			manager.contains(required),
			"manager FSST composition coverage is missing ADR 066 token {required}"
		);
	}
	for required in [
		"fsst_phase_b_l2_read_filters_a1_and_declines_a2_overlap",
		"phase_b_fetch.is_some()",
		"FSST A1 must not carry the predicate column into Phase C projection",
		"execute_program(&predicate, &batch)",
	] {
		assert!(
			manager_with_tests.contains(required),
			"manager FSST composition coverage is missing ADR 066 test token {required}"
		);
	}
	for required in [
		"bench_adr066_fsst_row_group_fetch_matrix",
		"scenario=adr066_fsst_row_group_fetch",
		"route=case_a_after_fsst",
		"route=dense_case_b_overlap_a2",
		"kernel_engaged=false",
		"fsst_kernel_vs_dense_cpu_micros",
	] {
		assert!(
			bench.contains(required),
			"row_group_fetch bench is missing ADR 066 token {required}"
		);
	}
}

#[test]
fn sparse_projection_fetch_target_binds_helper_split() {
	let mod_rs = source("src/storage/encoding/mod.rs");
	let helper = source("src/storage/encoding/page_materialize.rs");
	let sparse = source("src/storage/encoding/sparse_projection_fetch.rs");
	assert!(mod_rs.contains("pub mod page_materialize;"));
	assert!(mod_rs.contains("pub mod sparse_projection_fetch;"));
	for required in [
		"decode_page_fetch",
		"projected_page_batch_from_decoded_buffers",
		"required_projected_physical_ordinals",
		"fixed_width_sparse_projection_column",
		"fixed_width_encoding_indexed_value_column",
		"validity_bitmap_body_range_for_page",
		"rebase_validity_bitmap",
	] {
		assert!(
			helper.contains(required),
			"page materializer helper is missing ADR 060 token {required}"
		);
		assert!(
			!sparse.contains(&format!("fn {required}")),
			"sparse planner must not duplicate helper function {required}"
		);
	}
}
