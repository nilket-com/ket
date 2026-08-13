#[test]
fn product_0099_radial_dial_readings_exclude_unvalidated_external_construction() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/radial_dial_readings_construct.rs");
	cases.compile_fail("tests/ui/radial_dial_readings_private.rs");
}
