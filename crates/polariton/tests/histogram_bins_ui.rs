#[test]
fn product_0078_histogram_bins_excludes_unvalidated_mutation() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/histogram_bins_construct.rs");
	cases.compile_fail("tests/ui/histogram_bins_private.rs");
}
