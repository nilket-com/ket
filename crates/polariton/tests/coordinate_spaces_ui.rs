#[test]
fn coordinate_spaces_reject_cross_space_geometry() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/coordinate_same_space.rs");
	cases.compile_fail("tests/ui/coordinate_cross_space.rs");
}
