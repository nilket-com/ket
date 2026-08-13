#[test]
fn product_0090_bubble_observations_excludes_unvalidated_external_construction() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/bubble_observations_construct.rs");
	cases.compile_fail("tests/ui/bubble_observations_private.rs");
}
