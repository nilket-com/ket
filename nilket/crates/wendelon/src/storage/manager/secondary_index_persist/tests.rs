//! Two writers persisting the same secondary-index sidecar at once: an active
//! buffer seal and a lifecycle backfill of the same source slab can both reach
//! `persist_secondary_index_segment` for one path.
use super::*;
use crate::storage::file_ops::{DirectoryEntry, FileHandle};
use crate::storage::io_uring::AlignedBuf;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::task::Poll;
use tokio::sync::Notify;

/// Delegates every operation, but parks the first writer that opens a sidecar
/// temp file until `release` fires, so a second writer can run in that window.
struct ParkFirstSidecarTmpOpen {
	inner: Arc<dyn AsyncFileOps>,
	armed: AtomicBool,
	parked: Notify,
	release: Notify,
	// Synchronous boundaries make a single poll discriminate lock ordering,
	// independently of filesystem executor scheduling.
	synchronous_boundaries: bool,
	park_unlink: AtomicBool,
	staging_started: AtomicUsize,
	unlinks_started: AtomicUsize,
}

impl ParkFirstSidecarTmpOpen {
	fn is_sidecar_tmp(path: &Path) -> bool {
		path.to_string_lossy().contains(".tmp")
			&& path
				.components()
				.any(|component| component.as_os_str() == "secondary_index")
	}
}

#[async_trait::async_trait]
impl AsyncFileOps for ParkFirstSidecarTmpOpen {
	async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
		if Self::is_sidecar_tmp(path) && self.armed.swap(false, Ordering::AcqRel) {
			self.parked.notify_one();
			self.release.notified().await;
		}
		self.inner.open(path, flags).await
	}
	async fn read_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		len: usize,
	) -> std::io::Result<Vec<u8>> {
		self.inner.read_plain_at(handle, offset, len).await
	}
	async fn write_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		bytes: Vec<u8>,
	) -> std::io::Result<usize> {
		self.inner.write_plain_at(handle, offset, bytes).await
	}
	async fn read_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		self.inner.read_at(handle, offset, buf).await
	}
	async fn write_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		self.inner.write_at(handle, offset, buf).await
	}
	async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
		self.inner.fsync(handle).await
	}
	async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
		self.inner.fdatasync(handle).await
	}
	async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		self.inner.read_all(path).await
	}
	async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
		if Self::is_sidecar_tmp(path) {
			self.staging_started.fetch_add(1, Ordering::AcqRel);
			if self.synchronous_boundaries {
				return std::fs::write(path, bytes);
			}
		}
		self.inner.write_all(path, bytes).await
	}
	async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
		self.inner.file_len(path).await
	}
	async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
		self.inner.set_len(handle, len).await
	}
	async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
		if self.synchronous_boundaries {
			return std::fs::create_dir_all(path);
		}
		self.inner.create_dir_all(path).await
	}
	async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<DirectoryEntry>> {
		self.inner.read_dir(path).await
	}
	async fn fsync_dir(&self, path: &Path) -> std::io::Result<()> {
		self.inner.fsync_dir(path).await
	}
	async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		self.inner.rename(from, to).await
	}
	async fn link_no_replace(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		self.inner.link_no_replace(from, to).await
	}
	async fn unlink(&self, path: &Path) -> std::io::Result<()> {
		if self.synchronous_boundaries {
			self.unlinks_started.fetch_add(1, Ordering::AcqRel);
			if self.park_unlink.swap(false, Ordering::AcqRel) {
				self.parked.notify_one();
				self.release.notified().await;
			}
			return std::fs::remove_file(path);
		}
		self.inner.unlink(path).await
	}
}

fn segment_with_key(source_slab_id: SlabId, key: i64) -> SecondaryIndexSegment {
	let mut segment = secondary_index_segment_for_test(source_slab_id);
	let mut entries = segment.entries.clone();
	entries[0].encoded_key = ((key as u64) ^ 0x8000_0000_0000_0000)
		.to_be_bytes()
		.to_vec();
	segment = SecondaryIndexSegment::build(
		segment.identity(),
		source_slab_id,
		segment.header.table_schema_generation,
		segment.header.arrow_type_tag,
		entries,
	)
	.unwrap();
	segment
}

/// The first writer is parked after writing its temp file and before opening
/// it; a second writer for the same sidecar runs in that window. Neither may
/// fail, and the published file must be the one the catalog row describes.
#[tokio::test]
async fn concurrent_persists_of_one_sidecar_neither_fail_nor_clobber() {
	let dir = fresh_dir("secondary-index-concurrent-persist");
	let io = spawn_io_thread(64).unwrap();
	let mut manager = open_manager_at_dir(dir.to_path_buf(), io, test_crypto()).await;
	let gate = Arc::new(ParkFirstSidecarTmpOpen {
		inner: Arc::clone(&manager.file_ops),
		armed: AtomicBool::new(true),
		parked: Notify::new(),
		release: Notify::new(),
		synchronous_boundaries: false,
		park_unlink: AtomicBool::new(false),
		staging_started: AtomicUsize::new(0),
		unlinks_started: AtomicUsize::new(0),
	});
	Arc::get_mut(&mut manager)
		.expect("the fresh manager is not shared")
		.file_ops = gate.clone();

	let slab = SlabId::new(77);
	let first = {
		let manager = Arc::clone(&manager);
		tokio::spawn(async move {
			manager
				.persist_secondary_index_segment(&segment_with_key(slab, 1))
				.await
		})
	};
	tokio::time::timeout(Duration::from_secs(60), gate.parked.notified())
		.await
		.expect("the first writer reached its temp-file open");
	let second = {
		let manager = Arc::clone(&manager);
		tokio::spawn(async move {
			manager
				.persist_secondary_index_segment(&segment_with_key(slab, 2))
				.await
		})
	};
	// Let an unserialized second writer run to completion inside the window. A
	// serialized one waits for the first; the bound only keeps the test live.
	let _ = tokio::time::timeout(Duration::from_secs(2), async {
		while !second.is_finished() {
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
	})
	.await;
	gate.release.notify_one();
	let first = first.await.unwrap();
	let second = second.await.unwrap();
	let first = first.expect("the first persist must not fail");
	let second = second.expect("the second persist must not fail");
	assert_eq!(first.segment_path, second.segment_path);

	let listed = manager.list_secondary_index_segments(
		first.tenant_id,
		first.table_object_id,
		first.index_object_id,
		first.logical_schema_fingerprint,
	);
	assert_eq!(listed.len(), 1, "one catalog row for one source slab");
	let published = manager
		.load_secondary_index_segment(&listed[0])
		.await
		.expect("the published sidecar matches its catalog row")
		.expect("the published sidecar exists");
	assert_eq!(published.header.payload_hash, listed[0].payload_hash);
	let leftovers = std::fs::read_dir(dir.join(&first.segment_path).parent().unwrap())
		.unwrap()
		.filter_map(Result::ok)
		.filter(|entry| entry.file_name().to_string_lossy().contains(".tmp"))
		.count();
	assert_eq!(leftovers, 0, "no temp file survives a completed persist");
}

async fn poll_pending_once<F: Future>(mut future: Pin<&mut F>) {
	std::future::poll_fn(|cx| {
		assert!(
			future.as_mut().poll(cx).is_pending(),
			"operation must wait for the sidecar stripe"
		);
		Poll::Ready(())
	})
	.await;
}

async fn remove_sidecar(
	manager: &SlabManager,
	info: &SecondaryIndexSegmentInfo,
	retire: bool,
) -> std::io::Result<bool> {
	if retire {
		manager
			.retire_secondary_index_segment_for_replaced_slab(info)
			.await
	} else {
		manager.delete_secondary_index_segment(info).await
	}
}

async fn assert_sidecar_state(
	manager: &SlabManager,
	info: &SecondaryIndexSegmentInfo,
	present: bool,
) {
	let row = manager
		.secondary_index_segments
		.get(&PersistentSecondaryIndexSegmentKey {
			tenant_id: info.tenant_id,
			table_object_id: info.table_object_id,
			index_object_id: info.index_object_id,
			logical_schema_fingerprint: info.logical_schema_fingerprint,
			source_slab_id: info.source_slab_id,
		})
		.map(|row| row.value().clone());
	assert_eq!(row.is_some(), present);
	let path = manager.config.dir.join(&info.segment_path);
	assert_eq!(path.exists(), present);
	assert_eq!(
		manager
			.secondary_index_table_references_by_slab
			.contains_key(&info.source_slab_id),
		present
	);
	let coverage = manager
		.secondary_index_coverage_snapshot(info.coverage_key())
		.unwrap();
	assert_eq!(
		coverage.covered_slabs.contains(&info.source_slab_id),
		present
	);
	if present {
		let published = manager
			.load_secondary_index_segment(info)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(
			published.header.payload_hash,
			row.as_ref().unwrap().payload_hash
		);
		assert_eq!(published.header.payload_hash, info.payload_hash);
	}
	if let Some(index) = &manager.persistent_index {
		let rows = index
			.load_snapshot()
			.await
			.unwrap()
			.secondary_index_segments;
		assert_eq!(rows.len(), usize::from(present));
		if present {
			assert_eq!(rows[0].payload_hash, info.payload_hash);
		}
	}
}

/// Single-poll controls use immediate filesystem boundaries and no optional redb
/// catalog, so unrelated executor scheduling cannot masquerade as the stripe.
/// Repeat with real redb enabled to check durable and in-memory end states too.
#[tokio::test]
async fn delete_and_retire_wait_for_same_path_publication() {
	for retire in [false, true] {
		for durable in [false, true] {
			let dir = fresh_dir("sidecar-publish-before-delete");
			let mut manager = open_manager_at_dir(
				dir.to_path_buf(),
				spawn_io_thread(64).unwrap(),
				test_crypto(),
			)
			.await;
			if !durable {
				Arc::get_mut(&mut manager).unwrap().persistent_index = None;
			}
			let original = manager
				.persist_secondary_index_segment(&segment_with_key(SlabId::new(81), 1))
				.await
				.unwrap();
			let gate = Arc::new(ParkFirstSidecarTmpOpen {
				inner: Arc::clone(&manager.file_ops),
				armed: AtomicBool::new(true),
				parked: Notify::new(),
				release: Notify::new(),
				synchronous_boundaries: true,
				park_unlink: AtomicBool::new(false),
				staging_started: AtomicUsize::new(0),
				unlinks_started: AtomicUsize::new(0),
			});
			Arc::get_mut(&mut manager).unwrap().file_ops = gate.clone();
			let publisher = {
				let manager = Arc::clone(&manager);
				tokio::spawn(async move {
					manager
						.persist_secondary_index_segment(&segment_with_key(SlabId::new(81), 2))
						.await
				})
			};
			tokio::time::timeout(Duration::from_secs(60), gate.parked.notified())
				.await
				.unwrap();
			let deletion = remove_sidecar(&manager, &original, retire);
			tokio::pin!(deletion);
			poll_pending_once(deletion.as_mut()).await;
			assert_eq!(gate.unlinks_started.load(Ordering::Acquire), 0);
			assert_sidecar_state(&manager, &original, true).await;
			gate.release.notify_one();
			let replacement = tokio::time::timeout(Duration::from_secs(60), publisher)
				.await
				.unwrap()
				.unwrap()
				.unwrap();
			assert!(
				tokio::time::timeout(Duration::from_secs(60), deletion)
					.await
					.unwrap()
					.unwrap()
			);
			assert_sidecar_state(&manager, &replacement, false).await;
		}
	}
}

#[tokio::test]
async fn publication_waits_for_same_path_delete_and_retire() {
	for retire in [false, true] {
		for durable in [false, true] {
			let dir = fresh_dir("sidecar-delete-before-publish");
			let mut manager = open_manager_at_dir(
				dir.to_path_buf(),
				spawn_io_thread(64).unwrap(),
				test_crypto(),
			)
			.await;
			if !durable {
				Arc::get_mut(&mut manager).unwrap().persistent_index = None;
			}
			let original = manager
				.persist_secondary_index_segment(&segment_with_key(SlabId::new(82), 1))
				.await
				.unwrap();
			let gate = Arc::new(ParkFirstSidecarTmpOpen {
				inner: Arc::clone(&manager.file_ops),
				armed: AtomicBool::new(false),
				parked: Notify::new(),
				release: Notify::new(),
				synchronous_boundaries: true,
				park_unlink: AtomicBool::new(true),
				staging_started: AtomicUsize::new(0),
				unlinks_started: AtomicUsize::new(0),
			});
			Arc::get_mut(&mut manager).unwrap().file_ops = gate.clone();
			let deletion = {
				let manager = Arc::clone(&manager);
				let original = original.clone();
				tokio::spawn(async move { remove_sidecar(&manager, &original, retire).await })
			};
			tokio::time::timeout(Duration::from_secs(60), gate.parked.notified())
				.await
				.unwrap();
			if let Some(index) = &manager.persistent_index {
				assert!(
					index
						.load_snapshot()
						.await
						.unwrap()
						.secondary_index_segments
						.is_empty()
				);
			}
			let replacement_segment = segment_with_key(SlabId::new(82), 2);
			let publication = manager.persist_secondary_index_segment(&replacement_segment);
			tokio::pin!(publication);
			poll_pending_once(publication.as_mut()).await;
			assert_eq!(
				gate.staging_started.load(Ordering::Acquire),
				0,
				"publication must not stage during deletion"
			);
			gate.release.notify_one();
			assert!(
				tokio::time::timeout(Duration::from_secs(60), deletion)
					.await
					.unwrap()
					.unwrap()
					.unwrap()
			);
			let replacement = tokio::time::timeout(Duration::from_secs(60), publication)
				.await
				.unwrap()
				.unwrap();
			assert_sidecar_state(&manager, &replacement, true).await;
		}
	}
}

#[tokio::test]
async fn absent_row_delete_and_retire_still_wait_for_publication_stripe() {
	let dir = fresh_dir("sidecar-no-row-stripe");
	let manager = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let info = manager
		.persist_secondary_index_segment(&segment_with_key(SlabId::new(83), 1))
		.await
		.unwrap();
	assert!(manager.delete_secondary_index_segment(&info).await.unwrap());
	let path = dir.join(&info.segment_path);
	for retire in [false, true] {
		// A source may have coverage state even when its durable row is absent.
		manager.mark_secondary_index_covered(info.coverage_key(), info.source_slab_id);
		let guard = manager.secondary_index_persist_stripes[secondary_index_persist_stripe(&path)]
			.lock()
			.await;
		let deletion = remove_sidecar(&manager, &info, retire);
		tokio::pin!(deletion);
		poll_pending_once(deletion.as_mut()).await;
		assert!(
			manager
				.secondary_index_coverage_snapshot(info.coverage_key())
				.unwrap()
				.covered_slabs
				.contains(&info.source_slab_id)
		);
		drop(guard);
		assert!(!deletion.await.unwrap());
		assert_eq!(
			manager
				.secondary_index_coverage_snapshot(info.coverage_key())
				.unwrap()
				.covered_slabs
				.contains(&info.source_slab_id),
			!retire
		);
	}
}

#[tokio::test]
async fn restored_noncanonical_sidecar_paths_remain_deletable() {
	for retire in [false, true] {
		let dir = fresh_dir("sidecar-restored-path");
		let manager = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			test_crypto(),
		)
		.await;
		let mut info = manager
			.persist_secondary_index_segment(&segment_with_key(SlabId::new(84), 1))
			.await
			.unwrap();
		let canonical = dir.join(&info.segment_path);
		info.segment_path = "restored-sidecar.idx".to_owned();
		std::fs::rename(&canonical, dir.join(&info.segment_path)).unwrap();
		manager
			.persistent_index
			.as_ref()
			.unwrap()
			.upsert_secondary_index_segment(info.clone().into())
			.await
			.unwrap();
		drop(manager);
		let reopened = open_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			test_crypto(),
		)
		.await;
		assert_sidecar_state(&reopened, &info, true).await;
		assert!(remove_sidecar(&reopened, &info, retire).await.unwrap());
		assert_sidecar_state(&reopened, &info, false).await;
	}
}

async fn open_owned_manager_for_sidecar_recovery(
	dir: &Path,
	owner: &crate::storage::ownership::DataDirectoryOwner,
) -> Arc<SlabManager> {
	Arc::new(
		SlabManager::open_with_owner(
			SlabManagerConfig {
				dir: dir.to_path_buf(),
				max_slab_bytes: 1024 * 1024,
				io: spawn_io_thread(64).unwrap(),
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
				crypto: test_crypto(),
			},
			owner.clone(),
		)
		.await
		.unwrap(),
	)
}

#[tokio::test]
async fn interrupted_real_sidecar_publisher_is_cleaned_on_owned_reopen() {
	use crate::storage::ownership::DataDirectoryOwner;
	const CHILD: &str = "WENDELON_REAL_SIDECAR_PUBLISH_CHILD_DIR";
	let slab = SlabId::new(88);
	if let Some(path) = std::env::var_os(CHILD) {
		let dir = PathBuf::from(path);
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let mut manager = open_owned_manager_for_sidecar_recovery(&dir, &owner).await;
		assert!(
			manager.persistent_index.is_some(),
			"real publisher control requires its durable catalog"
		);
		let original = manager
			.persist_secondary_index_segment(&segment_with_key(slab, 1))
			.await
			.unwrap();
		let published_bytes = std::fs::read(dir.join(&original.segment_path)).unwrap();
		let gate = Arc::new(ParkFirstSidecarTmpOpen {
			inner: Arc::clone(&manager.file_ops),
			armed: AtomicBool::new(true),
			parked: Notify::new(),
			release: Notify::new(),
			synchronous_boundaries: false,
			park_unlink: AtomicBool::new(false),
			staging_started: AtomicUsize::new(0),
			unlinks_started: AtomicUsize::new(0),
		});
		Arc::get_mut(&mut manager).unwrap().file_ops = gate.clone();
		let _publisher = {
			let manager = Arc::clone(&manager);
			tokio::spawn(async move {
				manager
					.persist_secondary_index_segment(&segment_with_key(slab, 2))
					.await
			})
		};
		tokio::time::timeout(Duration::from_secs(60), gate.parked.notified())
			.await
			.expect("real publisher wrote staging bytes and reached pre-open gate");
		let parent = dir
			.join(&original.segment_path)
			.parent()
			.unwrap()
			.to_path_buf();
		let staged: Vec<_> = std::fs::read_dir(parent)
			.unwrap()
			.map(Result::unwrap)
			.filter(|entry| ParkFirstSidecarTmpOpen::is_sidecar_tmp(&entry.path()))
			.collect();
		assert_eq!(staged.len(), 1);
		assert_eq!(
			std::fs::read(staged[0].path()).unwrap(),
			segment_with_key(slab, 2).encode().unwrap()
		);
		assert_eq!(
			std::fs::read(dir.join(&original.segment_path)).unwrap(),
			published_bytes
		);
		assert_sidecar_state(&manager, &original, true).await;
		println!("real publisher staged replacement before open; original catalog unchanged");
		std::io::Write::flush(&mut std::io::stdout()).unwrap();
		// Exit without dropping the parked publish future: its normal error
		// cleanup cannot run, and no clean-shutdown record can hide the crash.
		std::process::exit(0);
	}

	let dir = fresh_dir("sidecar-real-publisher-reopen");
	let test = concat!(
		module_path!(),
		"::interrupted_real_sidecar_publisher_is_cleaned_on_owned_reopen"
	)
	.trim_start_matches("wendelon::");
	let mut command = std::process::Command::new(std::env::current_exe().unwrap());
	command
		.args([test, "--exact", "--nocapture"])
		.env(CHILD, dir.as_os_str());
	let output = crate::test_support::run_test_child(&mut command, Duration::from_secs(120))
		.await
		.unwrap();
	assert!(
		output.status.success(),
		"{}\n{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	assert!(
		String::from_utf8_lossy(&output.stdout)
			.contains("real publisher staged replacement before open; original catalog unchanged")
	);
	let original_segment = segment_with_key(slab, 1);
	let identity = original_segment.identity();
	let relative = secondary_index_segment_storage_path(
		identity.tenant_id,
		identity.table_object_id,
		identity.index_object_id,
		slab,
	);
	let published_path = dir.join(&relative);
	let original_bytes = std::fs::read(&published_path).unwrap();
	assert_eq!(original_bytes, original_segment.encode().unwrap());
	let parent = published_path.parent().unwrap();
	assert_eq!(
		std::fs::read_dir(parent).unwrap().count(),
		2,
		"real orphan must exist before reopening"
	);
	for _ in 0..2 {
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let manager = open_owned_manager_for_sidecar_recovery(&dir, &owner).await;
		assert!(
			manager.persistent_index.is_some(),
			"reopen must validate the durable catalog, not a fallback"
		);
		let listed = manager.list_secondary_index_segments(
			identity.tenant_id,
			identity.table_object_id,
			identity.index_object_id,
			identity.logical_schema_fingerprint,
		);
		assert_eq!(
			listed.len(),
			1,
			"reopen must not invent or drop a published catalog row"
		);
		assert_sidecar_state(&manager, &listed[0], true).await;
		assert_eq!(
			manager
				.load_secondary_index_segment(&listed[0])
				.await
				.unwrap()
				.unwrap(),
			original_segment
		);
		assert_eq!(std::fs::read(&published_path).unwrap(), original_bytes);
		assert_eq!(
			std::fs::read_dir(parent).unwrap().count(),
			1,
			"only the staged replacement may be removed"
		);
		owner.close(Duration::from_secs(30)).await.unwrap();
		drop(manager);
	}
}
