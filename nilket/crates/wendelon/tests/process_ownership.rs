//! Data-directory ownership and shutdown behaviour that needs real child
//! processes. These tests fork, so they live in their own test binary: a fork
//! duplicates every open descriptor until the child execs, which briefly keeps
//! another test's released directory lock held. Keeping process-spawning tests
//! out of the library test binary keeps that window away from the lock
//! release/reacquire tests there.

use std::path::{Path, PathBuf};

use wendelon::storage::ownership::{
	DataDirectoryBusy, DataDirectoryOwner, UNCLEAN_SHUTDOWN_EXIT_CODE,
};

struct TestDir(PathBuf);

impl TestDir {
	fn new() -> Self {
		let path =
			std::env::temp_dir().join(format!("wendelon-process-owner-{}", uuid::Uuid::new_v4()));
		std::fs::create_dir(&path).unwrap();
		Self(path)
	}
	fn path(&self) -> &Path {
		&self.0
	}
}

impl Drop for TestDir {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.0);
	}
}

fn run_child(name: &str, env: &[(&str, &std::ffi::OsStr)]) -> std::process::Output {
	let mut command = std::process::Command::new(std::env::current_exe().unwrap());
	command.arg("--exact").arg(name).arg("--nocapture");
	for (key, value) in env {
		command.env(key, value);
	}
	command.output().unwrap()
}

#[test]
fn a_second_process_cannot_claim_the_owned_directory() {
	let dir = TestDir::new();
	let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
	let output = run_child(
		"second_process_reports_busy",
		&[("WENDELON_OWNER_TEST_PATH", dir.path().as_os_str())],
	);
	assert!(
		output.status.success(),
		"child lock check failed: {}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	drop(owner);
}

#[test]
fn second_process_reports_busy() {
	let Some(path) = std::env::var_os("WENDELON_OWNER_TEST_PATH") else {
		return;
	};
	let error = DataDirectoryOwner::acquire_existing(path).unwrap_err();
	assert!(
		error
			.get_ref()
			.and_then(|source| source.downcast_ref::<DataDirectoryBusy>())
			.is_some()
	);
}

#[test]
fn a_wedged_shutdown_self_terminates_with_the_unclean_exit_code() {
	let started = std::time::Instant::now();
	let output = run_child(
		"wedged_shutdown_child",
		&[("WENDELON_WATCHDOG_TEST_CHILD", std::ffi::OsStr::new("1"))],
	);
	assert_eq!(
		output.status.code(),
		Some(UNCLEAN_SHUTDOWN_EXIT_CODE),
		"child status {:?}: {}",
		output.status,
		String::from_utf8_lossy(&output.stderr)
	);
	assert!(started.elapsed() < std::time::Duration::from_secs(30));
	assert!(
		String::from_utf8_lossy(&output.stderr).contains("terminating after incomplete shutdown")
	);
}

#[test]
fn wedged_shutdown_child() {
	if std::env::var_os("WENDELON_WATCHDOG_TEST_CHILD").is_none() {
		return;
	}
	let _watchdog =
		wendelon::shutdown_watchdog::ShutdownWatchdog::arm(std::time::Duration::from_millis(200));
	// A shutdown step that never returns.
	loop {
		std::thread::park();
	}
}

#[test]
fn a_disarmed_watchdog_never_fires() {
	let watchdog =
		wendelon::shutdown_watchdog::ShutdownWatchdog::arm(std::time::Duration::from_secs(5));
	watchdog.disarm();
}
