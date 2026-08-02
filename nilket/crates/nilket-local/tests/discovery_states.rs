use nilket_local::{
	FORMAT_VERSION, LocalWorkspace, ProfileMarker, ProfilePaths, RuntimeManifest, SQL_ENDPOINT,
	StaleCause, WENDELON_ENDPOINT, blocking_discover, process_start_token,
};
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

struct HeldFlock(File);

impl HeldFlock {
	fn acquire(path: &std::path::Path) -> Self {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(path)
			.expect("open fixture lock");
		// Test-only positive control: production discovery is forbidden from
		// calling flock and observes this ownership through /proc/locks.
		let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
		assert_eq!(result, 0, "acquire fixture flock");
		Self(file)
	}
}

impl Drop for HeldFlock {
	fn drop(&mut self) {
		let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
	}
}

fn fresh_root(label: &str) -> PathBuf {
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("clock")
		.as_nanos();
	std::env::temp_dir().join(format!(
		"nilket-local-integration-{label}-{}-{nonce}",
		std::process::id()
	))
}

fn write_marker(paths: &ProfilePaths) -> ProfileMarker {
	fs::create_dir_all(&paths.root).expect("create profile root");
	let marker = ProfileMarker {
		format_version: FORMAT_VERSION,
		profile_uuid: Uuid::new_v4(),
	};
	fs::write(
		&paths.marker,
		toml::to_string_pretty(&marker).expect("serialize marker"),
	)
	.expect("write marker");
	marker
}

fn write_runtime(paths: &ProfilePaths, marker: &ProfileMarker, pid: u32, endpoint: &str) {
	let runtime = RuntimeManifest {
		version: FORMAT_VERSION,
		profile_uuid: marker.profile_uuid,
		session_nonce: Uuid::new_v4(),
		pid,
		process_start_token: process_start_token(pid).unwrap_or_else(|_| "absent".to_owned()),
		started_unix_ms: 0,
		wendelon_endpoint: WENDELON_ENDPOINT.to_owned(),
		sql_endpoint: endpoint.to_owned(),
	};
	fs::write(
		&paths.runtime,
		serde_json::to_vec_pretty(&runtime).expect("serialize runtime"),
	)
	.expect("write runtime");
}

#[test]
fn held_lock_without_any_identity_is_starting() {
	let root = fresh_root("first-boot");
	fs::create_dir_all(&root).expect("create root");
	let paths = ProfilePaths::new(root.clone());
	let _lock = HeldFlock::acquire(&paths.lock);
	assert!(matches!(
		blocking_discover(Some(&root)),
		LocalWorkspace::Starting { owner_pid, .. } if owner_pid == std::process::id()
	));
	drop(_lock);
	fs::remove_dir_all(root).expect("remove root");
}

#[test]
fn held_lock_with_marker_but_no_manifest_is_starting() {
	let root = fresh_root("pre-publication");
	let paths = ProfilePaths::new(root.clone());
	write_marker(&paths);
	let _lock = HeldFlock::acquire(&paths.lock);
	assert!(matches!(
		blocking_discover(Some(&root)),
		LocalWorkspace::Starting { owner_pid, .. } if owner_pid == std::process::id()
	));
	drop(_lock);
	fs::remove_dir_all(root).expect("remove root");
}

#[test]
fn released_lock_makes_a_complete_manifest_stale() {
	let root = fresh_root("stale");
	let paths = ProfilePaths::new(root.clone());
	let marker = write_marker(&paths);
	write_runtime(&paths, &marker, std::process::id(), SQL_ENDPOINT);
	assert!(matches!(
		blocking_discover(Some(&root)),
		LocalWorkspace::Stale {
			cause: StaleCause::LockFree,
			..
		}
	));
	fs::remove_dir_all(root).expect("remove root");
}

#[test]
fn lock_holder_must_match_the_manifest_owner() {
	let root = fresh_root("owner-mismatch");
	let paths = ProfilePaths::new(root.clone());
	let marker = write_marker(&paths);
	write_runtime(
		&paths,
		&marker,
		std::process::id().saturating_add(1),
		SQL_ENDPOINT,
	);
	let _lock = HeldFlock::acquire(&paths.lock);
	assert!(matches!(
		blocking_discover(Some(&root)),
		LocalWorkspace::Stale {
			cause: StaleCause::OwnerMismatch { lock_owner_pid },
			..
		} if lock_owner_pid == std::process::id()
	));
	drop(_lock);
	fs::remove_dir_all(root).expect("remove root");
}

#[test]
fn verified_identity_with_failed_health_is_unhealthy() {
	let root = fresh_root("unhealthy");
	let paths = ProfilePaths::new(root.clone());
	let marker = write_marker(&paths);
	write_runtime(&paths, &marker, std::process::id(), "127.0.0.1:9");
	let _lock = HeldFlock::acquire(&paths.lock);
	assert!(matches!(
		blocking_discover(Some(&root)),
		LocalWorkspace::Unhealthy { .. }
	));
	drop(_lock);
	fs::remove_dir_all(root).expect("remove root");
}
