//! T-K2: each defect is refused with its typed reason, and the same fixture with that one defect
//! fixed is accepted.
use crate::custody::dir::{
	CustodyDir, Facts, Kind, Stop, TEMP_PREFIX, judge_ancestor, judge_dir, judge_file,
	open_dir_path, walk,
};
use crate::custody::test_dir::TestDir;
use crate::custody::{CustodyError, Refusal, sys};
use std::fs::{self, File, Permissions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// A custody directory `keys` (0700) under an ancestor `anc` (0755) under a fresh `TestDir`, the
/// walk's anchor. Ancestors above the anchor belong to the host (see `the_production_walk_*`).
struct Fixture {
	root: TestDir,
	anc: PathBuf,
	keys: PathBuf,
}

fn chmod(path: &Path, mode: u32) {
	fs::set_permissions(path, Permissions::from_mode(mode)).unwrap();
}

fn fixture() -> Fixture {
	let root = TestDir::new("kg-custody");
	let anc = root.join("anc");
	let keys = anc.join("keys");
	fs::DirBuilder::new().mode(0o755).create(&anc).unwrap();
	fs::DirBuilder::new().mode(0o700).create(&keys).unwrap();
	chmod(&anc, 0o755);
	chmod(&keys, 0o700);
	Fixture { root, anc, keys }
}

fn identity(path: &Path) -> (u64, u64) {
	let meta = fs::metadata(path).unwrap();
	(meta.dev(), meta.ino())
}

impl Fixture {
	fn open_path(&self, path: &Path) -> Result<CustodyDir, CustodyError> {
		open_dir_path(path, Stop::at(identity(&self.root)))
	}
	fn open(&self) -> Result<CustodyDir, CustodyError> {
		self.open_path(&self.keys)
	}
}

fn reason<T: std::fmt::Debug>(result: Result<T, CustodyError>) -> Refusal {
	match result {
		Err(CustodyError::Refused { reason, .. }) => reason,
		other => panic!("expected a refusal, got {other:?}"),
	}
}

fn facts(kind: Kind, uid: u32, mode: u32) -> Facts {
	Facts {
		kind,
		uid,
		mode,
		nlink: 1,
		size: 10,
		dev: 1,
		ino: 2,
	}
}

#[test]
fn the_fixture_directory_is_admitted() {
	let fx = fixture();
	let dir = fx.open().unwrap();
	assert_eq!(dir.identity(), identity(&fx.keys));
}

#[test]
fn a_directory_mode_other_than_0700_is_refused_and_0700_is_admitted() {
	let fx = fixture();
	for mode in [0o750, 0o701, 0o2700, 0o1700, 0o500] {
		chmod(&fx.keys, mode);
		assert_eq!(
			reason(fx.open()),
			Refusal::DirectoryMode { mode },
			"mode {mode:o}"
		);
	}
	chmod(&fx.keys, 0o700);
	fx.open().unwrap();
}

#[test]
fn a_symlinked_directory_is_refused_and_the_directory_itself_is_admitted() {
	let fx = fixture();
	let link = fx.anc.join("link");
	std::os::unix::fs::symlink(&fx.keys, &link).unwrap();
	assert_eq!(reason(fx.open_path(&link)), Refusal::Symlink);
	let dir = fx.open().unwrap();
	fs::DirBuilder::new()
		.mode(0o700)
		.create(fx.keys.join("real"))
		.unwrap();
	chmod(&fx.keys.join("real"), 0o700);
	std::os::unix::fs::symlink("real", fx.keys.join("alias")).unwrap();
	assert_eq!(reason(dir.open_subdir("alias")), Refusal::Symlink);
	dir.open_subdir("real").unwrap();
}

#[test]
fn a_file_where_the_directory_should_be_is_refused() {
	let fx = fixture();
	let file = fx.anc.join("plain");
	fs::write(&file, b"x").unwrap();
	assert_eq!(reason(fx.open_path(&file)), Refusal::NotDirectory);
}

#[test]
fn a_group_or_other_writable_ancestor_is_refused_and_0755_is_admitted() {
	let fx = fixture();
	let (dev, ino) = identity(&fx.anc);
	for mode in [0o775, 0o757, 0o1777] {
		chmod(&fx.anc, mode);
		assert_eq!(
			reason(fx.open()),
			Refusal::AncestorWritable { dev, ino, mode },
			"mode {mode:o}"
		);
	}
	chmod(&fx.anc, 0o755);
	fx.open().unwrap();
}

#[test]
fn the_ownership_rules_refuse_a_foreign_uid_and_admit_the_owner() {
	let euid = sys::euid();
	let other = euid.wrapping_add(1).max(1);
	assert_eq!(
		judge_dir(&facts(Kind::Directory, other, 0o700), euid),
		Err(Refusal::Owner { uid: other })
	);
	assert_eq!(
		judge_dir(&facts(Kind::Directory, euid, 0o700), euid),
		Ok(())
	);
	assert_eq!(
		judge_file(&facts(Kind::Regular, other, 0o600), euid, Some(&[10])),
		Err(Refusal::Owner { uid: other })
	);
	assert_eq!(
		judge_file(&facts(Kind::Regular, euid, 0o600), euid, Some(&[10])),
		Ok(())
	);
	assert_eq!(
		judge_ancestor(&facts(Kind::Directory, other, 0o755), euid),
		Err(Refusal::AncestorOwner {
			dev: 1,
			ino: 2,
			uid: other
		})
	);
	assert_eq!(
		judge_ancestor(&facts(Kind::Directory, euid, 0o755), euid),
		Ok(())
	);
	assert_eq!(
		judge_ancestor(&facts(Kind::Directory, 0, 0o755), euid),
		Ok(())
	);
}

/// The path-based reference: canonicalise, then judge each parent up to `/` from `stat`.
fn oracle(path: &Path) -> (Result<(), Refusal>, usize) {
	let euid = sys::euid();
	let real = fs::canonicalize(path).unwrap();
	let mut judged = 0;
	let mut parent = real.parent();
	while let Some(ancestor) = parent {
		let facts = Facts::of(&fs::metadata(ancestor).unwrap());
		if let Err(refusal) = judge_ancestor(&facts, euid) {
			return (Err(refusal), judged);
		}
		judged += 1;
		parent = ancestor.parent();
	}
	(Ok(()), judged)
}

fn handle_walk(path: &Path) -> (Result<(), Refusal>, Option<usize>) {
	let dir = File::open(path).unwrap();
	match walk(&dir, Stop::ROOT, "walk") {
		Ok(judged) => (Ok(()), Some(judged)),
		Err(CustodyError::Refused { reason, .. }) => (Err(reason), None),
		Err(other) => panic!("{other}"),
	}
}

#[test]
fn the_production_walk_agrees_with_the_path_oracle_all_the_way_to_root() {
	let fx = fixture();
	let mut reached_root = false;
	for path in [
		fx.keys.clone(),
		fx.anc.clone(),
		std::env::temp_dir(),
		PathBuf::from("/usr/share"),
		PathBuf::from("/usr"),
		PathBuf::from("/"),
	] {
		if !path.exists() {
			continue;
		}
		let (expected, oracle_judged) = oracle(&path);
		let (got, judged) = handle_walk(&path);
		assert_eq!(got, expected, "{}", path.display());
		if let Some(judged) = judged {
			assert_eq!(judged, oracle_judged, "{}", path.display());
			reached_root = true;
		}
	}
	assert!(reached_root, "no walk ran to / on this host");
	assert_eq!(handle_walk(Path::new("/")), (Ok(()), Some(0)));
}

#[test]
fn the_production_walk_judges_ancestors_above_the_fixture_anchor() {
	// The anchored walk ends below the host's ancestors; the production walk does not. Each
	// agrees with its own reference, so the anchor is the only difference.
	let fx = fixture();
	chmod(&fx.anc, 0o755);
	let anchored = walk(
		&File::open(&fx.keys).unwrap(),
		Stop::at(identity(&fx.root)),
		"walk",
	)
	.unwrap();
	assert_eq!(anchored, 1, "only `anc` lies between `keys` and the anchor");
	let (expected, _) = oracle(&fx.keys);
	assert_eq!(handle_walk(&fx.keys).0, expected);
	match fx.root.ancestors().find(|p| {
		let mode = fs::metadata(p).unwrap().mode();
		mode & 0o022 != 0
	}) {
		Some(writable) => {
			let (dev, ino) = identity(writable);
			assert!(matches!(
				handle_walk(&fx.keys).0,
				Err(Refusal::AncestorWritable { dev: d, ino: i, .. }) if (d, i) == (dev, ino)
			));
		}
		None => assert_eq!(handle_walk(&fx.keys).0, expected),
	}
}

fn published(fx: &Fixture, name: &str, bytes: &[u8]) -> CustodyDir {
	let dir = fx.open().unwrap();
	dir.lock().unwrap().create_file(name, bytes).unwrap();
	dir
}

#[test]
fn a_created_file_is_0600_and_opens_at_its_admitted_length() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let meta = fs::metadata(fx.keys.join("key")).unwrap();
	assert_eq!(meta.mode() & 0o7777, 0o600);
	let mut got = Vec::new();
	dir.open_file("key", &[3, 10])
		.unwrap()
		.read_to_end(&mut got)
		.unwrap();
	assert_eq!(got, vec![7; 10]);
	assert_eq!(
		reason(dir.open_file("key", &[11])),
		Refusal::Size { len: 10 }
	);
	assert_eq!(reason(dir.open_file("key", &[])), Refusal::Size { len: 10 });
}

#[test]
fn a_file_mode_other_than_0600_is_refused_and_0600_is_admitted() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let path = fx.keys.join("key");
	for mode in [0o644, 0o640, 0o604, 0o4600, 0o2600, 0o400, 0o700] {
		chmod(&path, mode);
		assert_eq!(
			reason(dir.open_file("key", &[10])),
			Refusal::FileMode { mode },
			"mode {mode:o}"
		);
	}
	chmod(&path, 0o600);
	dir.open_file("key", &[10]).unwrap();
}

#[test]
fn a_second_link_is_refused_and_one_link_is_admitted() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let other = fx.keys.join("alias");
	fs::hard_link(fx.keys.join("key"), &other).unwrap();
	assert_eq!(
		reason(dir.open_file("key", &[10])),
		Refusal::LinkCount { nlink: 2 }
	);
	fs::remove_file(&other).unwrap();
	dir.open_file("key", &[10]).unwrap();
}

#[test]
fn a_symlinked_file_is_refused_and_its_target_is_admitted() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	std::os::unix::fs::symlink("key", fx.keys.join("link")).unwrap();
	assert_eq!(reason(dir.open_file("link", &[10])), Refusal::Symlink);
	dir.open_file("key", &[10]).unwrap();
}

#[test]
fn a_fifo_is_refused_without_blocking_and_a_regular_file_is_admitted() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let status = std::process::Command::new("mkfifo")
		.arg("-m")
		.arg("600")
		.arg(fx.keys.join("pipe"))
		.status()
		.unwrap();
	assert!(status.success());
	assert_eq!(reason(dir.open_file("pipe", &[0, 10])), Refusal::NotRegular);
	dir.open_file("key", &[10]).unwrap();
}

#[test]
fn names_that_are_not_one_plain_component_are_refused() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let temp = format!("{TEMP_PREFIX}1.2");
	for name in [
		"",
		".",
		"..",
		"a/b",
		"../keys/key",
		"/key",
		temp.as_str(),
		"k\0ey",
	] {
		assert_eq!(
			reason(dir.open_file(name, &[10])),
			Refusal::Name,
			"{name:?}"
		);
		assert_eq!(
			reason(dir.lock().unwrap().create_file(name, b"x")),
			Refusal::Name,
			"{name:?}"
		);
	}
	dir.open_file("key", &[10]).unwrap();
}

fn temp_files(dir: &Path) -> Vec<String> {
	fs::read_dir(dir)
		.unwrap()
		.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
		.filter(|n| n.starts_with(TEMP_PREFIX))
		.collect()
}

#[test]
fn an_existing_name_is_never_overwritten_and_a_fresh_name_is_created() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let lock = dir.lock().unwrap();
	assert!(matches!(
		lock.create_file("key", &[9; 10]),
		Err(CustodyError::Exists { .. })
	));
	assert_eq!(fs::read(fx.keys.join("key")).unwrap(), vec![7; 10]);
	lock.create_file("key2", &[9; 4]).unwrap();
	assert_eq!(fs::read(fx.keys.join("key2")).unwrap(), vec![9; 4]);
	assert!(
		temp_files(&fx.keys).is_empty(),
		"{:?}",
		temp_files(&fx.keys)
	);
}

#[test]
fn a_second_lock_is_busy_until_the_first_is_dropped() {
	let fx = fixture();
	let first = fx.open().unwrap();
	let second = fx.open().unwrap();
	let held = first.lock().unwrap();
	assert!(matches!(second.lock(), Err(CustodyError::Busy)));
	drop(held);
	second.lock().unwrap();
}

#[test]
fn the_log_is_created_empty_appends_and_is_held_to_the_file_rules() {
	let fx = fixture();
	let dir = fx.open().unwrap();
	let lock = dir.lock().unwrap();
	lock.open_log("custody.log")
		.unwrap()
		.write_all(b"one\n")
		.unwrap();
	let mut log = lock.open_log("custody.log").unwrap();
	log.write_all(b"two\n").unwrap();
	assert_eq!(
		fs::read(fx.keys.join("custody.log")).unwrap(),
		b"one\ntwo\n"
	);
	chmod(&fx.keys.join("custody.log"), 0o644);
	assert_eq!(
		reason(lock.open_log("custody.log")),
		Refusal::FileMode { mode: 0o644 }
	);
	chmod(&fx.keys.join("custody.log"), 0o600);
	lock.open_log("custody.log").unwrap();
}

#[test]
fn a_subdirectory_is_created_0700_once_and_held_to_the_directory_rules() {
	let fx = fixture();
	let dir = fx.open().unwrap();
	let lock = dir.lock().unwrap();
	let retired = lock.create_subdir("retired").unwrap();
	assert_eq!(retired.identity(), identity(&fx.keys.join("retired")));
	assert!(matches!(
		lock.create_subdir("retired"),
		Err(CustodyError::Exists { .. })
	));
	chmod(&fx.keys.join("retired"), 0o750);
	assert_eq!(
		reason(dir.open_subdir("retired")),
		Refusal::DirectoryMode { mode: 0o750 }
	);
	chmod(&fx.keys.join("retired"), 0o700);
	dir.open_subdir("retired").unwrap();
}

#[test]
fn a_move_never_replaces_and_a_fresh_target_receives_the_file() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let lock = dir.lock().unwrap();
	let retired = lock.create_subdir("retired").unwrap();
	lock.create_file("taken", &[1]).unwrap();
	assert!(matches!(
		lock.move_file("key", &dir, "taken"),
		Err(CustodyError::Exists { .. })
	));
	assert_eq!(fs::read(fx.keys.join("key")).unwrap(), vec![7; 10]);
	assert_eq!(fs::read(fx.keys.join("taken")).unwrap(), vec![1]);
	lock.move_file("key", &retired, "key").unwrap();
	assert!(!fx.keys.join("key").exists());
	retired.open_file("key", &[10]).unwrap();
}

#[test]
fn destroy_refuses_an_unadmitted_file_and_removes_an_admitted_one() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let lock = dir.lock().unwrap();
	assert_eq!(
		reason(lock.destroy_file("key", &[11])),
		Refusal::Size { len: 10 }
	);
	assert_eq!(fs::read(fx.keys.join("key")).unwrap(), vec![7; 10]);
	lock.destroy_file("key", &[10]).unwrap();
	assert!(!fx.keys.join("key").exists());
}

#[test]
fn a_second_lock_on_the_same_handle_is_busy_until_the_first_is_dropped() {
	let fx = fixture();
	let dir = fx.open().unwrap();
	let other = fx.open().unwrap();
	let held = dir.lock().unwrap();
	assert!(matches!(dir.lock(), Err(CustodyError::Busy)));
	// The refused attempt left the held lock in force for every other handle too.
	assert!(matches!(other.lock(), Err(CustodyError::Busy)));
	drop(held);
	// Control: once the guard drops, the same handle locks again.
	let again = dir.lock().unwrap();
	assert!(matches!(other.lock(), Err(CustodyError::Busy)));
	drop(again);
	other.lock().unwrap();
}

#[test]
fn an_execute_only_ancestor_is_admitted_and_a_writable_one_is_not() {
	let fx = fixture();
	let (dev, ino) = identity(&fx.anc);
	// Search permission only: an O_RDONLY open of this ancestor fails, an O_PATH open does not.
	chmod(&fx.anc, 0o311);
	assert!(
		File::open(&fx.anc).is_err(),
		"the ancestor is still readable"
	);
	let admitted = fx.open().map(|d| d.identity());
	chmod(&fx.anc, 0o331);
	let refused = fx.open().map(|d| d.identity());
	chmod(&fx.anc, 0o755);
	assert_eq!(admitted.unwrap(), identity(&fx.keys));
	assert_eq!(
		reason(refused),
		Refusal::AncestorWritable {
			dev,
			ino,
			mode: 0o331
		}
	);
}

fn mkfifo(path: &Path) {
	let status = std::process::Command::new("mkfifo")
		.arg("-m")
		.arg("600")
		.arg(path)
		.status()
		.unwrap();
	assert!(status.success());
}

fn os(names: &[&str]) -> Vec<std::ffi::OsString> {
	names.iter().map(std::ffi::OsString::from).collect()
}

#[test]
fn names_list_every_entry_through_the_handle_without_opening_one() {
	use std::os::unix::ffi::OsStrExt;
	let fx = fixture();
	let dir = fx.open().unwrap();
	// Control: an empty directory lists nothing (not even `.` or `..`).
	assert!(dir.names().unwrap().is_empty());
	let lock = dir.lock().unwrap();
	lock.create_file("key", &[7; 10]).unwrap();
	lock.create_subdir("retired").unwrap();
	drop(lock);
	mkfifo(&fx.keys.join("pipe"));
	let raw = std::ffi::OsStr::from_bytes(b"not-utf8-\xff");
	File::create(fx.keys.join(raw)).unwrap();
	let mut expected = os(&["key", "pipe", "retired"]);
	expected.push(raw.to_os_string());
	expected.sort();
	assert_eq!(dir.names().unwrap(), expected);
	// The listing changed nothing: the key still opens at its admitted length.
	dir.open_file("key", &[10]).unwrap();
}

#[test]
fn a_leftover_temp_is_reported_and_kept_and_a_clean_creation_leaves_none() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	// Control: creation alone leaves no temp file.
	assert!(dir.lock().unwrap().leftover_temps().unwrap().is_empty());
	let planted = format!("{TEMP_PREFIX}4242.0");
	fs::write(fx.keys.join(&planted), [1, 2, 3]).unwrap();
	let lock = dir.lock().unwrap();
	assert_eq!(lock.leftover_temps().unwrap(), os(&[&planted]));
	assert_eq!(lock.leftover_temps().unwrap(), os(&[&planted]));
	assert_eq!(fs::read(fx.keys.join(&planted)).unwrap(), vec![1, 2, 3]);
	// A leftover is never a custody name.
	assert_eq!(reason(dir.open_file(&planted, &[3])), Refusal::Name);
}

#[test]
fn a_move_outside_the_lock_is_refused_and_one_within_it_succeeds() {
	let fx = fixture();
	let dir = published(&fx, "key", &[7; 10]);
	let lock = dir.lock().unwrap();
	let retired = lock.create_subdir("retired").unwrap();
	let deeper = retired.lock().unwrap().create_subdir("deeper").unwrap();
	fs::DirBuilder::new()
		.mode(0o700)
		.create(fx.anc.join("other"))
		.unwrap();
	chmod(&fx.anc.join("other"), 0o700);
	let sibling = fx.open_path(&fx.anc.join("other")).unwrap();
	for (what, to) in [("grandchild", &deeper), ("sibling", &sibling)] {
		assert_eq!(
			reason(lock.move_file("key", to, "key")),
			Refusal::OutsideLock,
			"{what}"
		);
		dir.open_file("key", &[10]).unwrap();
	}
	// The parent of a locked directory is outside its lock too.
	let retired_lock = retired.lock().unwrap();
	retired_lock.create_file("old", &[1]).unwrap();
	assert_eq!(
		reason(retired_lock.move_file("old", &dir, "old")),
		Refusal::OutsideLock
	);
	retired.open_file("old", &[1]).unwrap();
	drop(retired_lock);
	// Controls: the locked directory itself and a direct child.
	lock.move_file("key", &dir, "key2").unwrap();
	lock.move_file("key2", &retired, "key").unwrap();
	retired.open_file("key", &[10]).unwrap();
	assert!(!fx.keys.join("key").exists() && !fx.keys.join("key2").exists());
}

#[test]
fn the_anchored_open_admits_what_lies_below_its_anchor_and_production_refuses_it() {
	let root = TestDir::new("kg-anchor");
	let writable = root.join("writable");
	let anc = writable.join("anc");
	let keys = anc.join("keys");
	fs::DirBuilder::new().mode(0o775).create(&writable).unwrap();
	fs::DirBuilder::new().mode(0o755).create(&anc).unwrap();
	fs::DirBuilder::new().mode(0o700).create(&keys).unwrap();
	chmod(&writable, 0o775);
	chmod(&anc, 0o755);
	chmod(&keys, 0o700);
	let dir = super::open_custody_dir_anchored(&keys, &writable).unwrap();
	assert_eq!(dir.identity(), identity(&keys));
	// Control: the production open walks past the anchor and refuses the writable ancestor.
	assert!(matches!(
		reason(super::open_custody_dir(&keys)),
		Refusal::AncestorWritable { .. }
	));
	// Below the anchor every check still holds.
	chmod(&anc, 0o775);
	assert!(matches!(
		reason(super::open_custody_dir_anchored(&keys, &writable)),
		Refusal::AncestorWritable { .. }
	));
	// An anchor that is not a directory is refused.
	fs::write(root.join("file"), b"x").unwrap();
	assert_eq!(
		reason(super::open_custody_dir_anchored(&keys, &root.join("file"))),
		Refusal::NotDirectory
	);
}
