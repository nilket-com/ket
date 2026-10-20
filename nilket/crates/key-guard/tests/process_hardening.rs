//! T-HARD, in its own test process: hardening is process-wide, so it never runs beside the unit
//! tests.
#![cfg(target_os = "linux")]

use key_guard::custody::{core_dump_limit, harden_process, is_dumpable};

#[test]
fn hardening_makes_the_process_non_dumpable_with_no_core_dumps() {
	// Controls: a test process starts dumpable with a non-zero hard core limit (the soft limit
	// may already be 0), so the probe can see both change.
	assert!(is_dumpable().unwrap());
	let before = core_dump_limit().unwrap();
	assert_ne!(before.1, 0, "the hard core limit is already 0: {before:?}");
	harden_process().unwrap();
	assert!(!is_dumpable().unwrap());
	assert_eq!(core_dump_limit().unwrap(), (0, 0), "before: {before:?}");
}
