#[test]
fn product_0143_hexbin_observations_excludes_unvalidated_external_construction() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/hexbin_observations_construct.rs");
	cases.compile_fail("tests/ui/hexbin_observations_private.rs");
}
