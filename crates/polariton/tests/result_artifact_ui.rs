#[test]
fn product_0084_arrow_backing_has_one_complete_ipc_authority() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/result_artifact_construct.rs");
	cases.compile_fail("tests/ui/result_artifact_private.rs");
}
