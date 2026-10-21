//! Owner directories are format 2 only (D-O1): an unmarked directory is refused as
//! `Unformatted` and never upgraded. Every other owner-log row runs on a marked directory in
//! `owner_log_three.rs`.
use super::*;
use crate::storage::mutation_visibility::root2::provisioning::owner_log::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

// A fresh 0700 directory unique to this process and call; removed on drop. `marked` initializes it
// as a format-2 owner directory.
struct Scratch(PathBuf);
impl Scratch {
	fn bare() -> Self {
		let n = NEXT.fetch_add(1, Ordering::SeqCst);
		let p = std::env::temp_dir().join(format!("owner-log-{}-{n}", std::process::id()));
		let _ = fs::remove_dir_all(&p);
		fs::create_dir_all(&p).unwrap();
		fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
		Self(p)
	}
	fn marked() -> Self {
		let s = Self::bare();
		drop(OwnerDir2::create_v2(&s.0).unwrap());
		s
	}
	fn at(&self, rel: &str) -> PathBuf {
		self.0.join(rel)
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}
#[test]
fn a_forked_child_cannot_extend_a_dropped_owner_lock() {
	let s = Scratch::marked();
	let owner = OwnerDir2::open(&s.0).unwrap();
	let marker = std::env::temp_dir().join(format!(
		"wendelon-owner-fork-marker-{}",
		uuid::Uuid::new_v4()
	));
	let child = crate::test_support::hold_child_in_fork_window(
		marker.clone(),
		std::time::Duration::from_secs(1),
	);
	// The child shares LOCK's open file description; dropping the owner must
	// still free it immediately.
	drop(owner);
	assert!(
		OwnerDir2::open(&s.0).is_ok(),
		"a dropped owner's lock is free while a forked child holds the descriptor"
	);
	child.join().unwrap();
	let _ = std::fs::remove_file(marker);
}
/// O1: no marker means `Unformatted`, whatever log the directory holds, and nothing is written.
/// The accepted twin is the same directory once initialized by `create_v2`.
#[test]
fn an_unmarked_directory_is_unformatted_and_never_written() {
	let mut visoev02 = vec![0u8; 512];
	visoev02[..8].copy_from_slice(b"VISOEV02");
	for log in [
		None,
		Some(visiev02_bytes()),
		Some(Vec::new()),
		Some(visoev02),
	] {
		let s = Scratch::bare();
		if let Some(log) = &log {
			fs::write(s.at("inventory.log"), log).unwrap();
		}
		assert_eq!(
			OwnerDir2::open(&s.0).err(),
			Some(OwnerIoError2::Unformatted),
			"{:?}",
			log.as_ref().map(|l| l.len())
		);
		assert!(!s.at("OWNER_FORMAT").exists());
		if let Some(log) = &log {
			assert_eq!(&fs::read(s.at("inventory.log")).unwrap(), log);
		}
	}
	let s = Scratch::bare();
	assert_eq!(
		OwnerDir2::open(&s.0).err(),
		Some(OwnerIoError2::Unformatted)
	);
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	assert!(OwnerDir2::open(&s.0).is_ok());
}
