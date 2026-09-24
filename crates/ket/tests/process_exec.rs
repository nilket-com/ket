#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);
static PROCESS_TEST_LOCK: Mutex<()> = Mutex::new(());

fn temp_dir() -> PathBuf {
	let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
	let path = std::env::temp_dir().join(format!(
		"ket-process-test-{}-{sequence}",
		std::process::id()
	));
	fs::create_dir(&path).expect("create process test directory");
	// getcwd reports the resolved path; macOS temp dirs sit behind `/var` → `/private/var`.
	fs::canonicalize(&path).expect("resolve process test directory")
}

fn copy_executable(source: &Path, target: &Path) {
	fs::copy(source, target).expect("copy executable fixture");
}

#[test]
fn leaf_helper() {
	if std::env::var_os("KET_LEAF_HELPER").is_none() {
		return;
	}
	println!("LEAF_PID={}", std::process::id());
	println!("LEAF_CWD={}", std::env::current_dir().unwrap().display());
	println!("LEAF_ENV={}", std::env::var("KET_LEAF_VALUE").unwrap());
	eprintln!("LEAF_STDERR=visible");
}

#[test]
fn toron_leaf_helper() {
	if std::env::var_os("KET_ROLE_HELPER").is_some() {
		println!("ROLE=toron");
	}
}

#[test]
fn wendelon_leaf_helper() {
	if std::env::var_os("KET_ROLE_HELPER").is_some() {
		println!("ROLE=wendelon");
	}
}

#[test]
fn router_process_is_replaced_and_process_context_survives() {
	let _serial = PROCESS_TEST_LOCK.lock().unwrap();
	let root = temp_dir();
	let ket = root.join("ket");
	let baryon = root.join("baryon");
	copy_executable(Path::new(env!("CARGO_BIN_EXE_ket")), &ket);
	copy_executable(&std::env::current_exe().unwrap(), &baryon);

	let mut child = Command::new(&ket);
	child
		.args(["baryon", "--exact", "leaf_helper", "--nocapture"])
		.current_dir(&root)
		.env("KET_LEAF_HELPER", "1")
		.env("KET_LEAF_VALUE", "preserved")
		.stdout(Stdio::piped())
		.stderr(Stdio::piped());
	let child = child.spawn().expect("launch copied ket");
	let launched_pid = child.id();
	let output = child
		.wait_with_output()
		.expect("wait for process-replaced leaf helper");
	assert!(
		output.status.success(),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);
	let stdout = String::from_utf8(output.stdout).unwrap();
	let stderr = String::from_utf8(output.stderr).unwrap();
	assert!(
		stdout.contains(&format!("LEAF_PID={launched_pid}")),
		"{stdout}"
	);
	assert!(
		stdout.contains(&format!("LEAF_CWD={}", root.display())),
		"{stdout}"
	);
	assert!(stdout.contains("LEAF_ENV=preserved"), "{stdout}");
	assert!(stderr.contains("LEAF_STDERR=visible"), "{stderr}");
	fs::remove_dir_all(root).unwrap();
}

#[test]
fn symlinked_ket_resolves_siblings_beside_its_target() {
	let _serial = PROCESS_TEST_LOCK.lock().unwrap();
	let root = temp_dir();
	let install = root.join("install");
	let links = root.join("links");
	fs::create_dir(&install).expect("create install directory");
	fs::create_dir(&links).expect("create link directory");
	let ket = install.join("ket");
	copy_executable(Path::new(env!("CARGO_BIN_EXE_ket")), &ket);
	copy_executable(&std::env::current_exe().unwrap(), &install.join("baryon"));
	let link = links.join("ket");
	std::os::unix::fs::symlink(&ket, &link).expect("link ket");

	let output = Command::new(&link)
		.args(["baryon", "--exact", "leaf_helper", "--nocapture"])
		.current_dir(&root)
		.env("KET_LEAF_HELPER", "1")
		.env("KET_LEAF_VALUE", "preserved")
		.env("PATH", "")
		.output()
		.expect("launch ket through its symlink");
	assert!(
		output.status.success(),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);
	assert!(
		String::from_utf8_lossy(&output.stdout).contains("LEAF_ENV=preserved"),
		"{}",
		String::from_utf8_lossy(&output.stdout)
	);
	fs::remove_dir_all(root).unwrap();
}

#[test]
fn role_prefixes_reach_nilket_through_real_exec() {
	let _serial = PROCESS_TEST_LOCK.lock().unwrap();
	let root = temp_dir();
	let ket = root.join("ket");
	copy_executable(Path::new(env!("CARGO_BIN_EXE_ket")), &ket);
	copy_executable(&std::env::current_exe().unwrap(), &root.join("nilket"));

	for role in ["toron", "wendelon"] {
		let output = Command::new(&ket)
			.args([role, "--nocapture"])
			.env("KET_ROLE_HELPER", "1")
			.output()
			.unwrap();
		assert!(
			output.status.success(),
			"{}",
			String::from_utf8_lossy(&output.stderr)
		);
		assert!(
			String::from_utf8_lossy(&output.stdout).contains(&format!("ROLE={role}")),
			"{}",
			String::from_utf8_lossy(&output.stdout)
		);
	}
	fs::remove_dir_all(root).unwrap();
}

#[test]
fn corrupt_default_and_invalid_sibling_never_launch_a_fallback() {
	let _serial = PROCESS_TEST_LOCK.lock().unwrap();
	let root = temp_dir();
	let ket = root.join("ket");
	copy_executable(Path::new(env!("CARGO_BIN_EXE_ket")), &ket);
	let path_dir = root.join("path");
	let config_root = root.join("config");
	fs::create_dir(&path_dir).unwrap();
	fs::create_dir_all(config_root.join("ket")).unwrap();
	copy_executable(&std::env::current_exe().unwrap(), &path_dir.join("baryon"));
	fs::write(
		config_root.join("ket/router.conf"),
		b"version=2\ndefault=baryon\n",
	)
	.unwrap();
	let marker = root.join("must-not-exist");

	let output = Command::new(&ket)
		.arg(&marker)
		.env("XDG_CONFIG_HOME", &config_root)
		.env("PATH", &path_dir)
		.output()
		.unwrap();
	assert_eq!(output.status.code(), Some(2));
	assert!(!marker.exists());
	assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported"));

	fs::remove_file(config_root.join("ket/router.conf")).unwrap();
	fs::write(root.join("baryon"), b"not executable").unwrap();
	let output = Command::new(&ket)
		.arg("baryon")
		.arg(&marker)
		.env("PATH", &path_dir)
		.output()
		.unwrap();
	assert_eq!(output.status.code(), Some(2));
	assert!(!marker.exists());
	assert!(String::from_utf8_lossy(&output.stderr).contains("refusing PATH fallback"));
	fs::remove_dir_all(root).unwrap();
}
