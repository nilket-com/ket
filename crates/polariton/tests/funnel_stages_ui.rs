#[test]
fn product_0096_funnel_stages_excludes_unvalidated_external_construction() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/funnel_stages_construct.rs");
	cases.compile_fail("tests/ui/funnel_stages_private.rs");
}
