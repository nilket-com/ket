use super::*;
use crate::test_support::TestDir;
use std::sync::{
	Arc,
	atomic::{AtomicUsize, Ordering},
	mpsc,
};
use std::time::Duration;

const TENANT: &str = "0000000000000001";
const TABLE: &str = "11111111111111111111111111111111";
const INDEX: &str = "22222222222222222222222222222222";
fn leaf_name() -> String {
	format!("0000000000000003.idx.{}.tmp", uuid::Uuid::new_v4().simple())
}
fn leaf_dir(root: &Path) -> PathBuf {
	root.join("secondary_index")
		.join(TENANT)
		.join(TABLE)
		.join(INDEX)
}
fn staged(root: &Path) -> PathBuf {
	let dir = leaf_dir(root);
	std::fs::create_dir_all(&dir).unwrap();
	let path = dir.join(leaf_name());
	std::fs::write(&path, b"interrupted-stage").unwrap();
	path
}

#[test]
fn recognizes_only_exact_generated_uuid_v4_staging_names() {
	let name = leaf_name();
	assert!(is_sidecar_tmp(name.as_bytes()));
	for bad in [
		"0000000000000003.idx.tmp".to_owned(),
		"0000000000000003.idx".to_owned(),
		name.to_uppercase(),
		format!("other-{name}"),
		format!("{name}.extra"),
		name.replacen(".idx.", ".data.", 1),
		name.replacen(".idx.", ".idx..", 1),
	] {
		assert!(!is_sidecar_tmp(bad.as_bytes()), "{bad}");
	}
	let uuid_start = 16 + ".idx.".len();
	for (offset, byte) in [(12, b'1'), (16, b'7'), (5, b'G')] {
		let mut bad = name.as_bytes().to_vec();
		bad[uuid_start + offset] = byte;
		assert!(!is_sidecar_tmp(&bad));
	}
	assert!(!is_sidecar_tmp(&[0xff; 57]));
}

struct CountSync(Arc<AtomicUsize>);
impl CleanupHooks for CountSync {
	fn before_fsync(&mut self, _: &File) -> io::Result<()> {
		self.0.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}
}
#[tokio::test]
async fn sweep_is_narrow_parent_synced_and_idempotent() {
	let dir = TestDir::new("sidecar-sweep-matrix");
	let orphan = staged(&dir);
	let published = orphan.parent().unwrap().join("0000000000000003.idx");
	let legacy = orphan.parent().unwrap().join("0000000000000003.idx.tmp");
	let unrelated = orphan.parent().unwrap().join("notes.tmp");
	let malformed = dir
		.join("secondary_index/not-a-tenant")
		.join(TABLE)
		.join(INDEX)
		.join(leaf_name());
	std::fs::create_dir_all(malformed.parent().unwrap()).unwrap();
	for path in [&published, &legacy, &unrelated, &malformed] {
		std::fs::write(path, b"preserve").unwrap();
	}
	let outside_depth = leaf_dir(&dir).join("extra").join(leaf_name());
	std::fs::create_dir_all(outside_depth.parent().unwrap()).unwrap();
	std::fs::write(&outside_depth, b"preserve").unwrap();
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	let syncs = Arc::new(AtomicUsize::new(0));
	assert_eq!(
		run_owned_cleanup(
			dir.to_path_buf(),
			Some(owner.clone()),
			CountSync(syncs.clone())
		)
		.await
		.unwrap(),
		1
	);
	assert!(!orphan.exists());
	assert_eq!(syncs.load(Ordering::SeqCst), 1);
	for path in [&published, &legacy, &unrelated, &malformed, &outside_depth] {
		assert_eq!(std::fs::read(path).unwrap(), b"preserve");
	}
	assert_eq!(
		run_owned_cleanup(dir.to_path_buf(), Some(owner.clone()), NoHooks)
			.await
			.unwrap(),
		0
	);
	owner.close(Duration::from_secs(10)).await.unwrap();
}

struct SwapDirectory {
	depth: usize,
	path: PathBuf,
	outside: PathBuf,
	swapped: bool,
}
impl CleanupHooks for SwapDirectory {
	fn before_open(&mut self, _: &File, _: &CStr, depth: usize) -> io::Result<()> {
		if depth == self.depth && !self.swapped {
			std::fs::rename(&self.path, self.path.with_extension("saved"))?;
			std::os::unix::fs::symlink(&self.outside, &self.path)?;
			self.swapped = true;
		}
		Ok(())
	}
}
#[tokio::test]
async fn never_follows_child_directory_symlinks_even_after_enumeration() {
	for depth in 0..=3 {
		let dir = TestDir::new("sidecar-sweep-link");
		let external = TestDir::new("sidecar-sweep-link-target");
		staged(&dir);
		let outside_file = staged(&external);
		let parts = ["secondary_index", TENANT, TABLE, INDEX];
		let path: PathBuf = parts[..=depth]
			.iter()
			.fold(dir.to_path_buf(), |p, s| p.join(s));
		let outside: PathBuf = parts[..=depth]
			.iter()
			.fold(external.to_path_buf(), |p, s| p.join(s));
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		assert_eq!(
			run_owned_cleanup(
				dir.to_path_buf(),
				Some(owner.clone()),
				SwapDirectory {
					depth,
					path: path.clone(),
					outside,
					swapped: false
				}
			)
			.await
			.unwrap(),
			0
		);
		assert!(
			std::fs::symlink_metadata(path)
				.unwrap()
				.file_type()
				.is_symlink()
		);
		assert_eq!(std::fs::read(outside_file).unwrap(), b"interrupted-stage");
		owner.close(Duration::from_secs(10)).await.unwrap();
	}
}

struct SwapLeaf {
	path: PathBuf,
	target: PathBuf,
}
impl CleanupHooks for SwapLeaf {
	fn before_unlink(&mut self, _: &File, _: &CStr) -> io::Result<()> {
		std::fs::rename(&self.path, self.path.with_extension("saved"))?;
		std::os::unix::fs::symlink(&self.target, &self.path)
	}
}
#[tokio::test]
async fn leaf_identity_recheck_preserves_replaced_symlink_and_target() {
	let dir = TestDir::new("sidecar-sweep-leaf-swap");
	let external = TestDir::new("sidecar-sweep-leaf-target");
	let target = external.join("untouched");
	std::fs::write(&target, b"outside").unwrap();
	let path = staged(&dir);
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	assert_eq!(
		run_owned_cleanup(
			dir.to_path_buf(),
			Some(owner.clone()),
			SwapLeaf {
				path: path.clone(),
				target: target.clone()
			}
		)
		.await
		.unwrap(),
		0
	);
	assert!(
		std::fs::symlink_metadata(path)
			.unwrap()
			.file_type()
			.is_symlink()
	);
	assert_eq!(std::fs::read(target).unwrap(), b"outside");
	owner.close(Duration::from_secs(10)).await.unwrap();
}

struct FailAt {
	unlink: bool,
}
impl CleanupHooks for FailAt {
	fn before_unlink(&mut self, _: &File, _: &CStr) -> io::Result<()> {
		if self.unlink {
			Err(io::Error::from_raw_os_error(libc::EACCES))
		} else {
			Ok(())
		}
	}
	fn before_fsync(&mut self, _: &File) -> io::Result<()> {
		Err(io::Error::from_raw_os_error(libc::EIO))
	}
}
#[tokio::test]
async fn unlink_and_fsync_failures_surface_without_false_success() {
	for unlink in [true, false] {
		let dir = TestDir::new("sidecar-sweep-error");
		let path = staged(&dir);
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let error = run_owned_cleanup(dir.to_path_buf(), Some(owner.clone()), FailAt { unlink })
			.await
			.unwrap_err();
		assert_eq!(
			error.raw_os_error(),
			Some(if unlink { libc::EACCES } else { libc::EIO })
		);
		assert_eq!(path.exists(), unlink);
		assert_eq!(
			run_owned_cleanup(dir.to_path_buf(), Some(owner.clone()), NoHooks)
				.await
				.unwrap(),
			usize::from(unlink)
		);
		owner.close(Duration::from_secs(10)).await.unwrap();
	}
}

struct RemoveBeforeUnlink(PathBuf);
impl CleanupHooks for RemoveBeforeUnlink {
	fn before_unlink(&mut self, _: &File, _: &CStr) -> io::Result<()> {
		std::fs::remove_file(&self.0)
	}
}
#[tokio::test]
async fn already_removed_leaf_and_missing_root_are_harmless() {
	let dir = TestDir::new("sidecar-sweep-missing");
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	assert_eq!(
		run_owned_cleanup(dir.to_path_buf(), Some(owner.clone()), NoHooks)
			.await
			.unwrap(),
		0
	);
	let path = staged(&dir);
	assert_eq!(
		run_owned_cleanup(
			dir.to_path_buf(),
			Some(owner.clone()),
			RemoveBeforeUnlink(path)
		)
		.await
		.unwrap(),
		0
	);
	owner.close(Duration::from_secs(10)).await.unwrap();
}

struct BlockUnlink {
	entered: mpsc::SyncSender<()>,
	release: mpsc::Receiver<()>,
}
impl CleanupHooks for BlockUnlink {
	fn before_unlink(&mut self, _: &File, _: &CStr) -> io::Result<()> {
		self.entered.send(()).unwrap();
		self.release.recv_timeout(Duration::from_secs(30)).unwrap();
		Ok(())
	}
}
#[tokio::test]
async fn cancelled_cleanup_keeps_owner_and_permit_until_job_finishes() {
	let dir = TestDir::new("sidecar-sweep-cancel");
	let path = staged(&dir);
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	let (entered, ready) = mpsc::sync_channel(1);
	let (release, released) = mpsc::sync_channel(1);
	let job = {
		let owner = owner.clone();
		let path = dir.to_path_buf();
		tokio::spawn(async move {
			run_owned_cleanup(
				path,
				Some(owner),
				BlockUnlink {
					entered,
					release: released,
				},
			)
			.await
		})
	};
	tokio::task::spawn_blocking(move || ready.recv_timeout(Duration::from_secs(30)).unwrap())
		.await
		.unwrap();
	job.abort();
	assert!(job.await.unwrap_err().is_cancelled());
	let error = owner.close(Duration::from_millis(10)).await.unwrap_err();
	assert_eq!(
		error
			.get_ref()
			.unwrap()
			.downcast_ref::<crate::storage::ownership::ShutdownIncomplete>()
			.unwrap()
			.outstanding,
		1
	);
	assert!(DataDirectoryOwner::acquire_existing(&dir).is_err());
	assert!(path.exists());
	release.send(()).unwrap();
	owner.close(Duration::from_secs(30)).await.unwrap();
	assert!(!path.exists());
	DataDirectoryOwner::acquire_existing(&dir).unwrap();
}

#[tokio::test]
async fn interrupted_staging_is_removed_by_actual_owned_manager_reopen() {
	const CHILD: &str = "WENDELON_SIDECAR_SWEEP_CHILD_DIR";
	if let Some(path) = std::env::var_os(CHILD) {
		let path = PathBuf::from(path);
		let _owner = DataDirectoryOwner::acquire_existing(&path).unwrap();
		staged(&path);
		// Model exit after stage bytes exist but before rename/cleanup. No
		// destructors or clean-shutdown record may mask interrupted recovery.
		std::process::exit(0);
	}
	let dir = TestDir::new("sidecar-sweep-reopen");
	// Reopen cleanup is permitted only with durable index authority.
	drop(
		crate::storage::index::PersistentLocalIndex::initialize_new(
			crate::storage::index::persistent_index_path(&dir),
			None,
		)
		.await
		.unwrap(),
	);
	let published = leaf_dir(&dir).join("0000000000000003.idx");
	std::fs::create_dir_all(published.parent().unwrap()).unwrap();
	std::fs::write(&published, b"published-preserved").unwrap();
	let name = concat!(
		module_path!(),
		"::interrupted_staging_is_removed_by_actual_owned_manager_reopen"
	)
	.trim_start_matches("wendelon::");
	let mut command = std::process::Command::new(std::env::current_exe().unwrap());
	command
		.args([name, "--exact", "--nocapture"])
		.env(CHILD, dir.as_os_str());
	let output = crate::test_support::run_test_child(&mut command, Duration::from_secs(120))
		.await
		.unwrap();
	assert!(
		output.status.success(),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);
	assert_eq!(std::fs::read_dir(leaf_dir(&dir)).unwrap().count(), 2);
	for _ in 0..2 {
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let crypto = Arc::new(crate::crypto::CryptoManager::new_dev());
		crypto
			.install_local_only_for_tests(crate::schema::TenantId::ZERO)
			.unwrap();
		let manager = super::super::SlabManager::open_with_owner(
			super::super::SlabManagerConfig {
				tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
				dir: dir.to_path_buf(),
				max_slab_bytes: 1024 * 1024,
				io: crate::storage::io_uring::spawn_io_thread(64).unwrap(),
				l2: None,
				rehydration: Default::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: Default::default(),
				crypto,
			},
			owner.clone(),
		)
		.await
		.unwrap();
		assert_eq!(std::fs::read_dir(leaf_dir(&dir)).unwrap().count(), 1);
		assert_eq!(std::fs::read(&published).unwrap(), b"published-preserved");
		owner.close(Duration::from_secs(30)).await.unwrap();
		drop(manager);
	}
}

struct FailSecondUnlink {
	calls: usize,
	syncs: Arc<AtomicUsize>,
}
impl CleanupHooks for FailSecondUnlink {
	fn before_unlink(&mut self, _: &File, _: &CStr) -> io::Result<()> {
		self.calls += 1;
		if self.calls == 2 {
			Err(io::Error::from_raw_os_error(libc::EACCES))
		} else {
			Ok(())
		}
	}
	fn before_fsync(&mut self, _: &File) -> io::Result<()> {
		self.syncs.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}
}
#[tokio::test]
async fn changed_parent_is_synced_when_a_later_unlink_fails() {
	let dir = TestDir::new("sidecar-sweep-partial-error");
	let first = staged(&dir);
	let second = staged(&dir);
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	let syncs = Arc::new(AtomicUsize::new(0));
	let error = run_owned_cleanup(
		dir.to_path_buf(),
		Some(owner.clone()),
		FailSecondUnlink {
			calls: 0,
			syncs: syncs.clone(),
		},
	)
	.await
	.unwrap_err();
	assert_eq!(error.raw_os_error(), Some(libc::EACCES));
	// Select by hook call order, not filesystem enumeration order: precisely
	// one actual unlink succeeded before the injected second-operation error.
	assert_eq!(
		usize::from(first.exists()) + usize::from(second.exists()),
		1
	);
	assert_eq!(
		syncs.load(Ordering::SeqCst),
		1,
		"changed parent must be synced on the error path"
	);
	for path in [&first, &second] {
		if path.exists() {
			assert_eq!(std::fs::read(path).unwrap(), b"interrupted-stage");
		}
	}
	assert_eq!(
		run_owned_cleanup(
			dir.to_path_buf(),
			Some(owner.clone()),
			CountSync(syncs.clone())
		)
		.await
		.unwrap(),
		1
	);
	assert!(!first.exists() && !second.exists());
	assert_eq!(syncs.load(Ordering::SeqCst), 2);
	assert_eq!(
		run_owned_cleanup(dir.to_path_buf(), Some(owner.clone()), NoHooks)
			.await
			.unwrap(),
		0
	);
	owner.close(Duration::from_secs(10)).await.unwrap();
}

#[test]
fn pd22_cleanup_accepts_exact_new_fingerprint_depth_and_preserves_other_shapes() {
	let root = TestDir::new("pd22-cleanup-shape");
	let base = leaf_dir(&root);
	let fingerprint = "33".repeat(32);
	let new_leaf = base.join(&fingerprint);
	std::fs::create_dir_all(&new_leaf).unwrap();
	let valid = new_leaf.join(leaf_name());
	std::fs::write(&valid, b"owned-temp").unwrap();
	let regular = new_leaf.join("0000000000000003.idx");
	std::fs::write(&regular, b"published").unwrap();
	let arbitrary = new_leaf.join("unrelated.tmp");
	std::fs::write(&arbitrary, b"user").unwrap();
	let wrong = base.join("44".repeat(31));
	std::fs::create_dir_all(&wrong).unwrap();
	let wrong_temp = wrong.join(leaf_name());
	std::fs::write(&wrong_temp, b"wrong-depth").unwrap();
	let extra = new_leaf.join("55".repeat(32));
	std::fs::create_dir_all(&extra).unwrap();
	let nested_temp = extra.join(leaf_name());
	std::fs::write(&nested_temp, b"too-deep").unwrap();
	let removed = walk(
		&OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
			.open(root.join("secondary_index"))
			.unwrap(),
		0,
		&mut NoHooks,
	)
	.unwrap();
	assert_eq!(removed, 1);
	assert!(!valid.exists());
	for path in [regular, arbitrary, wrong_temp, nested_temp] {
		assert!(
			path.exists(),
			"cleanup crossed its exact namespace: {path:?}"
		);
	}
}

#[tokio::test]
async fn unique_sidecar_finals_survive_cleanup_and_inventory_counts_unreferenced_bytes() {
	let dir = TestDir::new("unique-sidecar-inventory");
	let leaf =
		leaf_dir(&dir).join("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
	std::fs::create_dir_all(&leaf).unwrap();
	let current = leaf.join(format!(
		"0000000000000003.idx.{}",
		uuid::Uuid::new_v4().simple()
	));
	let previous = leaf.join(format!(
		"0000000000000003.idx.{}",
		uuid::Uuid::new_v4().simple()
	));
	let orphan = leaf.join(format!(
		"0000000000000003.idx.{}",
		uuid::Uuid::new_v4().simple()
	));
	let tmp = leaf.join(format!(
		"{}.{}.tmp",
		current.file_name().unwrap().to_str().unwrap(),
		uuid::Uuid::new_v4().simple()
	));
	for (path, bytes) in [
		(&current, b"current".as_slice()),
		(&previous, b"old".as_slice()),
		(&orphan, b"orphan".as_slice()),
		(&tmp, b"temporary".as_slice()),
	] {
		std::fs::write(path, bytes).unwrap();
	}
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	assert_eq!(
		run_owned_cleanup(dir.to_path_buf(), Some(owner.clone()), NoHooks)
			.await
			.unwrap(),
		1
	);
	assert!(!tmp.exists());
	let referenced = [current.strip_prefix(&*dir).unwrap().to_path_buf()]
		.into_iter()
		.collect();
	let mut files = scan_retained_secondary_files(&dir, Some(owner.clone()), referenced)
		.await
		.unwrap();
	files.sort();
	let mut expected = vec![
		(previous.strip_prefix(&*dir).unwrap().to_path_buf(), 3),
		(orphan.strip_prefix(&*dir).unwrap().to_path_buf(), 6),
	];
	expected.sort();
	assert_eq!(files, expected);
	for path in [&current, &previous, &orphan] {
		assert!(path.exists());
	}
	owner.close(Duration::from_secs(10)).await.unwrap();
}
