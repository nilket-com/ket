#[test]
fn saved_session_capabilities_are_closed_by_typestate() {
	let cases = trybuild::TestCases::new();
	for operation in [
		"mint_document",
		"update_document_path",
		"remove_document",
		"set_owner_payload",
		"commit",
		"cleanup_unreferenced",
	] {
		cases.pass(format!("tests/ui/session_writable_{operation}.rs"));
		cases.compile_fail(format!("tests/ui/session_read_only_{operation}.rs"));
	}
	cases.pass("tests/ui/session_writable_spawn_saved.rs");
	cases.compile_fail("tests/ui/session_read_only_spawn_saved.rs");
}
