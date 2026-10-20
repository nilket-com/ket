//! T-PASS (hardening): the tool's startup leaves the process non-dumpable with a zero core limit.
//! Its own test binary, so no other test's process is changed.

#[test]
fn startup_makes_the_process_non_dumpable_with_no_core_file() {
	// Control: before startup the test process is dumpable.
	assert!(key_guard::custody::is_dumpable().unwrap());
	wendelon_key::startup().unwrap();
	assert!(!key_guard::custody::is_dumpable().unwrap());
	assert_eq!(key_guard::custody::core_dump_limit().unwrap(), (0, 0));
}
