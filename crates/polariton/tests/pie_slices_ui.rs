#[test]
fn product_0102_pie_slices_exclude_unvalidated_external_construction() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/pie_slices_construct.rs");
	cases.compile_fail("tests/ui/pie_slices_private.rs");
}
