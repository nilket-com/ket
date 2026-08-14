#[test]
fn product_0104_multivariate_rows_exclude_unvalidated_external_construction() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/multivariate_rows_construct.rs");
	cases.compile_fail("tests/ui/multivariate_rows_private.rs");
}
