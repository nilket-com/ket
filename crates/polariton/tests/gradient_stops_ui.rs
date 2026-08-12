#[test]
fn product_0087_gradient_stops_exclude_unvalidated_external_access() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/gradient_stops_construct.rs");
	cases.compile_fail("tests/ui/gradient_stops_private_literal.rs");
	cases.compile_fail("tests/ui/gradient_stops_private_read.rs");
}
