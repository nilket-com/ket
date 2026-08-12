#[test]
fn product_0088_heatmap_cells_excludes_unvalidated_external_construction() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/heatmap_cells_construct.rs");
	cases.compile_fail("tests/ui/heatmap_cells_private.rs");
}
