use std::fs;
use std::path::Path;

fn source(relative: &str) -> String {
	fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)).unwrap()
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
	for required in [
		"sparse_projection_fetch_materializes_exact_cocd_clustered_pages",
		"read_projected_record_from_l2",
		"OP_LT",
		"execute_program(&predicate, &batch)",
		"phase_c_fetch.selected_plaintext_ranges",
		"sparse projection should not fetch the whole projected payload buffer",
	] {
		assert!(
			manager.contains(required),
			"manager runtime oracle is missing ADR 060 token {required}"
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
