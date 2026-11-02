//! Two writers persisting the same secondary-index sidecar at once: an active
//! buffer seal and a lifecycle backfill of the same source slab can both reach
//! `persist_secondary_index_segment` for one path.
use super::*;
use crate::storage::file_ops::{DirectoryEntry, FileHandle};
use crate::storage::io_uring::AlignedBuf;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize};
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
	deny_final_unlink: AtomicBool,
	staging_started: AtomicUsize,
	tmp_fd: AtomicI32,
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
	fn bind_directory_owner(
		self: Arc<Self>,
		owner: DataDirectoryOwner,
	) -> std::io::Result<Arc<dyn AsyncFileOps>> {
		// Validate the underlying implementation supports the same owner; the
		// fixture installs an already owner-bound delegate before opening.
		self.inner.clone().bind_directory_owner(owner)?;
		Ok(self)
	}

	fn write_permit(&self) -> std::io::Result<Option<crate::storage::ownership::WritePermit>> {
		self.inner.write_permit()
	}

	async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
		let handle = self.inner.open(path, flags).await?;
		if Self::is_sidecar_tmp(path) {
			self.staging_started.fetch_add(1, Ordering::AcqRel);
			self.tmp_fd.store(handle.raw_fd(), Ordering::Release);
		}
		Ok(handle)
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
		self.inner.fsync(handle).await?;
		if self.tmp_fd.load(Ordering::Acquire) == handle.raw_fd()
			&& self.armed.swap(false, Ordering::AcqRel)
		{
			self.parked.notify_one();
			self.release.notified().await;
		}
		Ok(())
	}
	async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
		self.inner.fdatasync(handle).await
	}
	async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		self.inner.read_all(path).await
	}
	async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
		if Self::is_sidecar_tmp(path) {
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
		if !Self::is_sidecar_tmp(path)
			&& path
				.components()
				.any(|part| part.as_os_str() == "secondary_index")
			&& self.deny_final_unlink.swap(false, Ordering::AcqRel)
		{
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				"injected final unlink refusal",
			));
		}
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
	let gate = Arc::new(ParkFirstSidecarTmpOpen {
		inner: Arc::new(BlockingFileOps),
		armed: AtomicBool::new(false),
		parked: Notify::new(),
		release: Notify::new(),
		synchronous_boundaries: false,
		park_unlink: AtomicBool::new(false),
		deny_final_unlink: AtomicBool::new(false),
		staging_started: AtomicUsize::new(0),
		tmp_fd: AtomicI32::new(-1),
		unlinks_started: AtomicUsize::new(0),
	});
	let manager = open_fresh_sidecar_manager_with_ops(&dir, gate.clone()).await;
	gate.armed.store(true, Ordering::Release);

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
	assert_ne!(first.segment_path, second.segment_path);

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
	assert_sidecar_state_inner(manager, info, present, true).await;
}
async fn assert_sidecar_state_while_writer_is_parked(
	manager: &SlabManager,
	info: &SecondaryIndexSegmentInfo,
) {
	// The public loader now shares the publication stripe and must wait.
	// Cancel its real pending future before the writer is released.
	let mut read = Box::pin(manager.load_secondary_index_segment(info));
	poll_pending_once(read.as_mut()).await;
	drop(read);
	// The gate parks before temp open/rename. Preserve the original old-file,
	// row, coverage and durable-catalog oracle using the physical preimage.
	assert_sidecar_state_inner(manager, info, true, false).await;
}
async fn assert_sidecar_state_inner(
	manager: &SlabManager,
	info: &SecondaryIndexSegmentInfo,
	present: bool,
	through_api: bool,
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
		let published = if through_api {
			manager
				.load_secondary_index_segment(info)
				.await
				.unwrap()
				.unwrap()
		} else {
			SecondaryIndexSegment::decode(&std::fs::read(&path).unwrap()).unwrap()
		};
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

/// A real redb catalog and a parked filesystem boundary show that publication
/// and deletion serialize for both retirement and ordinary deletion.
#[tokio::test]
async fn delete_and_retire_wait_for_same_path_publication() {
	for retire in [false, true] {
		let dir = fresh_dir("sidecar-publish-before-delete");
		let gate = Arc::new(ParkFirstSidecarTmpOpen {
			inner: Arc::new(BlockingFileOps),
			armed: AtomicBool::new(false),
			parked: Notify::new(),
			release: Notify::new(),
			synchronous_boundaries: true,
			park_unlink: AtomicBool::new(false),
			deny_final_unlink: AtomicBool::new(false),
			staging_started: AtomicUsize::new(0),
			tmp_fd: AtomicI32::new(-1),
			unlinks_started: AtomicUsize::new(0),
		});
		let manager = open_fresh_sidecar_manager_with_ops(&dir, gate.clone()).await;
		let original = manager
			.persist_secondary_index_segment(&segment_with_key(SlabId::new(81), 1))
			.await
			.unwrap();
		gate.unlinks_started.store(0, Ordering::Release);
		gate.armed.store(true, Ordering::Release);
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
		assert_sidecar_state_while_writer_is_parked(&manager, &original).await;
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

#[tokio::test]
async fn publication_waits_for_same_path_delete_and_retire() {
	for retire in [false, true] {
		let dir = fresh_dir("sidecar-delete-before-publish");
		let gate = Arc::new(ParkFirstSidecarTmpOpen {
			inner: Arc::new(BlockingFileOps),
			armed: AtomicBool::new(false),
			parked: Notify::new(),
			release: Notify::new(),
			synchronous_boundaries: true,
			park_unlink: AtomicBool::new(false),
			deny_final_unlink: AtomicBool::new(false),
			staging_started: AtomicUsize::new(0),
			tmp_fd: AtomicI32::new(-1),
			unlinks_started: AtomicUsize::new(0),
		});
		let manager = open_fresh_sidecar_manager_with_ops(&dir, gate.clone()).await;
		let original = manager
			.persist_secondary_index_segment(&segment_with_key(SlabId::new(82), 1))
			.await
			.unwrap();
		gate.staging_started.store(0, Ordering::Release);
		gate.park_unlink.store(true, Ordering::Release);
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

#[tokio::test]
async fn absent_row_delete_and_retire_still_wait_for_publication_stripe() {
	let dir = fresh_dir("sidecar-no-row-stripe");
	let manager = initialize_manager_at_dir(
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
	let key = PersistentSecondaryIndexSegmentRow::from(info.clone()).key;
	let path = dir.join(secondary_index_segment_storage_path(&key));
	for retire in [false, true] {
		// A source may have coverage state even when its durable row is absent.
		manager.mark_secondary_index_covered(info.coverage_key(), info.source_slab_id);
		let guard = manager.secondary_index_persist_stripes[secondary_index_persist_stripe(&path)]
			.write()
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
		let manager = initialize_manager_at_dir(
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
	initialize: bool,
	file_ops: Option<Arc<dyn AsyncFileOps>>,
) -> Arc<SlabManager> {
	let config = SlabManagerConfig {
		tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
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
	};
	match (initialize, file_ops) {
		(true, Some(ops)) => {
			SlabManager::initialize_new_store_with_file_ops_and_owner(config, ops, owner.clone())
				.await
		}
		(false, Some(ops)) => {
			SlabManager::open_with_file_ops_and_owner(config, ops, owner.clone()).await
		}
		(true, None) => SlabManager::initialize_new_store_with_owner(config, owner.clone()).await,
		(false, None) => SlabManager::open_with_owner(config, owner.clone()).await,
	}
	.unwrap()
}

#[tokio::test]
async fn interrupted_real_sidecar_publisher_is_cleaned_on_owned_reopen() {
	use crate::storage::ownership::DataDirectoryOwner;
	const CHILD: &str = "WENDELON_REAL_SIDECAR_PUBLISH_CHILD_DIR";
	let slab = SlabId::new(88);
	if let Some(path) = std::env::var_os(CHILD) {
		let dir = PathBuf::from(path);
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let gate = Arc::new(ParkFirstSidecarTmpOpen {
			inner: Arc::new(BlockingFileOps::with_owner(owner.clone())),
			armed: AtomicBool::new(false),
			parked: Notify::new(),
			release: Notify::new(),
			synchronous_boundaries: false,
			park_unlink: AtomicBool::new(false),
			deny_final_unlink: AtomicBool::new(false),
			staging_started: AtomicUsize::new(0),
			tmp_fd: AtomicI32::new(-1),
			unlinks_started: AtomicUsize::new(0),
		});
		let manager =
			open_owned_manager_for_sidecar_recovery(&dir, &owner, true, Some(gate.clone())).await;
		assert!(
			manager.persistent_index.is_some(),
			"real publisher control requires its durable catalog"
		);
		let original = manager
			.persist_secondary_index_segment(&segment_with_key(slab, 1))
			.await
			.unwrap();
		let published_bytes = std::fs::read(dir.join(&original.segment_path)).unwrap();
		gate.armed.store(true, Ordering::Release);
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
		assert_sidecar_state_while_writer_is_parked(&manager, &original).await;
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
	let canonical = dir.join(secondary_index_segment_storage_path(
		&original_segment.catalog_key(),
	));
	let parent = canonical.parent().unwrap();
	let published_path = std::fs::read_dir(parent)
		.unwrap()
		.map(Result::unwrap)
		.map(|entry| entry.path())
		.find(|path| !ParkFirstSidecarTmpOpen::is_sidecar_tmp(path))
		.expect("published final survives the interrupted replacement");
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
		let manager = open_owned_manager_for_sidecar_recovery(&dir, &owner, false, None).await;
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

fn pd22_segment(source: SlabId, fingerprint: u8, alternate_index: bool) -> SecondaryIndexSegment {
	let base = segment_with_key(source, i64::from(fingerprint));
	let mut identity = base.identity();
	identity.logical_schema_fingerprint =
		crate::schema::LogicalSchemaFingerprint::from_bytes([fingerprint; 32]);
	if alternate_index {
		identity.index_object_id = crate::schema::IndexObjectId::new([0xf2; 16]);
	}
	SecondaryIndexSegment::build(
		identity,
		source,
		base.header.table_schema_generation,
		base.header.arrow_type_tag,
		base.entries,
	)
	.unwrap()
}

#[tokio::test]
async fn pd22_fingerprint_rows_load_their_own_payloads() {
	let dir = fresh_dir("pd22-load-fingerprint");
	let manager = initialize_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let old = pd22_segment(SlabId::new(91), 1, false);
	let new = pd22_segment(SlabId::new(91), 2, false);
	let old_info = manager.persist_secondary_index_segment(&old).await.unwrap();
	let new_info = manager.persist_secondary_index_segment(&new).await.unwrap();
	assert_ne!(
		PersistentSecondaryIndexSegmentRow::from(old_info.clone()).key,
		PersistentSecondaryIndexSegmentRow::from(new_info.clone()).key
	);
	assert_eq!(
		manager
			.load_secondary_index_segment(&new_info)
			.await
			.unwrap(),
		Some(new)
	);
	assert_eq!(
		manager
			.load_secondary_index_segment(&old_info)
			.await
			.unwrap(),
		Some(old),
		"a catalog row loaded another fingerprint's payload"
	);
}

async fn pd22_old_retirement_preserves_new(retire: bool, alternate_index: bool) {
	let dir = fresh_dir("pd22-retire-fingerprint");
	let manager = initialize_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let old_info = manager
		.persist_secondary_index_segment(&pd22_segment(SlabId::new(92), 1, false))
		.await
		.unwrap();
	let new = pd22_segment(SlabId::new(92), 2, alternate_index);
	let new_info = manager.persist_secondary_index_segment(&new).await.unwrap();
	assert_ne!(
		PersistentSecondaryIndexSegmentRow::from(old_info.clone()).key,
		PersistentSecondaryIndexSegmentRow::from(new_info.clone()).key
	);
	assert_eq!(
		manager
			.load_secondary_index_segment(&new_info)
			.await
			.unwrap(),
		Some(new.clone())
	);
	assert!(remove_sidecar(&manager, &old_info, retire).await.unwrap());
	assert!(
		manager
			.secondary_index_segments
			.contains_key(&PersistentSecondaryIndexSegmentRow::from(new_info.clone()).key)
	);
	assert_eq!(
		manager
			.load_secondary_index_segment(&new_info)
			.await
			.unwrap(),
		Some(new),
		"retiring an old identity silently unlinked the still-cataloged live file"
	);
}

#[tokio::test]
async fn pd22_delete_old_fingerprint_preserves_new_sidecar() {
	pd22_old_retirement_preserves_new(false, false).await;
}
#[tokio::test]
async fn pd22_retire_old_fingerprint_preserves_new_sidecar() {
	pd22_old_retirement_preserves_new(true, false).await;
}
#[tokio::test]
async fn pd22_different_index_ids_are_an_independent_retirement_control() {
	pd22_old_retirement_preserves_new(false, true).await;
	pd22_old_retirement_preserves_new(true, true).await;
}

fn pd22_reason(
	error: &std::io::Error,
) -> super::super::secondary_index_segments::SidecarDataErrorReason {
	assert_eq!(error.kind(), ErrorKind::InvalidData);
	error
		.get_ref()
		.unwrap()
		.downcast_ref::<super::super::secondary_index_segments::SidecarDataError>()
		.expect("typed sidecar error")
		.reason
}

#[tokio::test]
async fn pd22_loader_binds_header_and_payload_hash_to_the_row_and_downgrades_coverage() {
	use super::super::secondary_index_segments::SidecarDataErrorReason as Reason;
	for (fingerprint, key, reason) in [
		(2, 1, Reason::IdentityMismatch),
		(1, 2, Reason::PayloadMismatch),
	] {
		let dir = fresh_dir("pd22-loader-binding");
		let manager = initialize_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			test_crypto(),
		)
		.await;
		let segment = pd22_segment(SlabId::new(94), 1, false);
		let info = manager
			.persist_secondary_index_segment(&segment)
			.await
			.unwrap();
		let base = segment_with_key(SlabId::new(94), key);
		let mut identity = base.identity();
		identity.logical_schema_fingerprint =
			crate::schema::LogicalSchemaFingerprint::from_bytes([fingerprint; 32]);
		let wrong = SecondaryIndexSegment::build(
			identity,
			base.header.source_slab_id,
			base.header.table_schema_generation,
			base.header.arrow_type_tag,
			base.entries,
		)
		.unwrap();
		std::fs::write(dir.join(&info.segment_path), wrong.encode().unwrap()).unwrap();
		assert_eq!(
			pd22_reason(
				&manager
					.load_secondary_index_segment(&info)
					.await
					.unwrap_err()
			),
			reason
		);
		let coverage = manager
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert!(!coverage.covered_slabs.contains(&info.source_slab_id));
		assert!(
			coverage
				.uncovered_slabs
				.iter()
				.any(|(slab, _)| *slab == info.source_slab_id)
		);
	}
}

#[tokio::test]
async fn pd22_covered_missing_file_or_row_is_typed_and_uncovered() {
	use super::super::secondary_index_segments::SidecarDataErrorReason as Reason;
	for missing_row in [false, true] {
		let dir = fresh_dir("pd22-covered-missing");
		let manager = initialize_manager_at_dir(
			dir.to_path_buf(),
			spawn_io_thread(64).unwrap(),
			test_crypto(),
		)
		.await;
		let info = manager
			.persist_secondary_index_segment(&pd22_segment(SlabId::new(95), 1, false))
			.await
			.unwrap();
		if missing_row {
			manager
				.secondary_index_segments
				.remove(&PersistentSecondaryIndexSegmentRow::from(info.clone()).key);
		} else {
			std::fs::remove_file(dir.join(&info.segment_path)).unwrap();
		}
		assert_eq!(
			pd22_reason(
				&manager
					.load_secondary_index_segment(&info)
					.await
					.unwrap_err()
			),
			if missing_row {
				Reason::MissingCoveredRow
			} else {
				Reason::MissingFile
			}
		);
		let coverage = manager
			.secondary_index_coverage_snapshot(info.coverage_key())
			.unwrap();
		assert!(!coverage.covered_slabs.contains(&info.source_slab_id));
		assert!(
			coverage
				.uncovered_slabs
				.iter()
				.any(|(slab, _)| *slab == info.source_slab_id)
		);
	}
}

async fn pd22_legacy_alias_pair(
	manager: &SlabManager,
	dir: &Path,
	winner: usize,
) -> (Vec<SecondaryIndexSegmentInfo>, Vec<SecondaryIndexSegment>) {
	let segments = vec![
		pd22_segment(SlabId::new(96), 1, false),
		pd22_segment(SlabId::new(96), 2, false),
	];
	let mut infos = Vec::new();
	for segment in &segments {
		infos.push(
			manager
				.persist_secondary_index_segment(segment)
				.await
				.unwrap(),
		);
	}
	let mut parts = infos[0].segment_path.split('/').collect::<Vec<_>>();
	assert_eq!(parts.len(), 6);
	parts.remove(4);
	let legacy = parts.join("/");
	std::fs::write(dir.join(&legacy), segments[winner].encode().unwrap()).unwrap();
	for info in &mut infos {
		std::fs::remove_file(dir.join(&info.segment_path)).unwrap();
		info.segment_path = legacy.clone();
		let row = PersistentSecondaryIndexSegmentRow::from(info.clone());
		manager
			.persistent_index
			.as_ref()
			.unwrap()
			.upsert_secondary_index_segment(row.clone())
			.await
			.unwrap();
		manager.secondary_index_segments.insert(row.key, row);
	}
	assert_eq!(
		secondary_index_persist_stripe(&dir.join(&infos[0].segment_path)),
		secondary_index_persist_stripe(&dir.join(&infos[1].segment_path))
	);
	(infos, segments)
}

#[tokio::test]
async fn pd22_legacy_alias_deletion_preserves_a_referenced_file() {
	for winner in 0..2 {
		for removed in 0..2 {
			for retire in [false, true] {
				let dir = fresh_dir("pd22-legacy-alias");
				let manager = initialize_manager_at_dir(
					dir.to_path_buf(),
					spawn_io_thread(64).unwrap(),
					test_crypto(),
				)
				.await;
				let (infos, segments) = pd22_legacy_alias_pair(&manager, &dir, winner).await;
				assert!(
					remove_sidecar(&manager, &infos[removed], retire)
						.await
						.unwrap()
				);
				let remains = 1 - removed;
				assert!(
					dir.join(&infos[remains].segment_path).exists(),
					"a live legacy alias lost its file"
				);
				if remains == winner {
					assert_eq!(
						manager
							.load_secondary_index_segment(&infos[remains])
							.await
							.unwrap(),
						Some(segments[winner].clone())
					);
				} else {
					assert_eq!(pd22_reason(&manager.load_secondary_index_segment(&infos[remains]).await.unwrap_err()), super::super::secondary_index_segments::SidecarDataErrorReason::IdentityMismatch);
				}
			}
		}
	}
}

#[tokio::test]
async fn pd22_legacy_physical_stripe_excludes_replacement_and_other_row_delete() {
	let dir = fresh_dir("pd22-legacy-physical-lock");
	let manager = initialize_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let (infos, segments) = pd22_legacy_alias_pair(&manager, &dir, 1).await;
	let physical = secondary_index_persist_stripe(&dir.join(&infos[0].segment_path));
	let held = manager.secondary_index_persist_stripes[physical]
		.write()
		.await;
	let mut publish = Box::pin(manager.persist_secondary_index_segment(&segments[0]));
	let mut delete = Box::pin(manager.delete_secondary_index_segment(&infos[1]));
	poll_pending_once(publish.as_mut()).await;
	poll_pending_once(delete.as_mut()).await;
	drop(held);
	let (published, deleted) = tokio::time::timeout(Duration::from_secs(60), async {
		tokio::join!(publish, delete)
	})
	.await
	.unwrap();
	let published = published.unwrap();
	assert!(deleted.unwrap());
	assert_eq!(
		manager
			.load_secondary_index_segment(&published)
			.await
			.unwrap(),
		Some(segments[0].clone())
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pd22_hot_sidecar_loads_share_the_read_stripe() {
	let dir = fresh_dir("pd22-shared-loads");
	let manager = initialize_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let segment = pd22_segment(SlabId::new(97), 1, false);
	let info = manager
		.persist_secondary_index_segment(&segment)
		.await
		.unwrap();
	let shared = manager.secondary_index_persist_stripes
		[secondary_index_persist_stripe(&dir.join(&info.segment_path))]
	.read()
	.await;
	let mut tasks = tokio::task::JoinSet::new();
	for _ in 0..16 {
		let manager = manager.clone();
		let info = info.clone();
		let expected = segment.clone();
		tasks.spawn(async move {
			assert_eq!(
				manager.load_secondary_index_segment(&info).await.unwrap(),
				Some(expected)
			);
		});
	}
	tokio::time::timeout(Duration::from_secs(60), async {
		while let Some(result) = tasks.join_next().await {
			result.unwrap();
		}
	})
	.await
	.expect("loads must finish while another reader retains the stripe");
	drop(shared);
}

#[tokio::test]
async fn pd22_equivalent_noncanonical_alias_deletion_preserves_a_referenced_file() {
	for winner in 0..2 {
		for removed in 0..2 {
			for retire in [false, true] {
				let dir = fresh_dir("pd22-legacy-alias");
				let manager = initialize_manager_at_dir(
					dir.to_path_buf(),
					spawn_io_thread(64).unwrap(),
					test_crypto(),
				)
				.await;
				let (mut infos, segments) = pd22_legacy_alias_pair(&manager, &dir, winner).await;
				let (parent, filename) = infos[1].segment_path.rsplit_once('/').unwrap();
				infos[1].segment_path = format!("{parent}/./{filename}");
				let row = PersistentSecondaryIndexSegmentRow::from(infos[1].clone());
				manager
					.persistent_index
					.as_ref()
					.unwrap()
					.upsert_secondary_index_segment(row.clone())
					.await
					.unwrap();
				manager.secondary_index_segments.insert(row.key, row);

				assert!(
					remove_sidecar(&manager, &infos[removed], retire)
						.await
						.unwrap()
				);
				let remains = 1 - removed;
				assert!(
					dir.join(&infos[remains].segment_path).exists(),
					"a live legacy alias lost its file"
				);
				if remains == winner {
					assert_eq!(
						manager
							.load_secondary_index_segment(&infos[remains])
							.await
							.unwrap(),
						Some(segments[winner].clone())
					);
				} else {
					assert_eq!(pd22_reason(&manager.load_secondary_index_segment(&infos[remains]).await.unwrap_err()), super::super::secondary_index_segments::SidecarDataErrorReason::IdentityMismatch);
				}
			}
		}
	}
}

#[tokio::test]
async fn pd22_republish_legacy_row_reclaims_only_its_unreferenced_old_path() {
	let dir = fresh_dir("pd22-legacy-republish");
	let manager = initialize_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	let (infos, segments) = pd22_legacy_alias_pair(&manager, &dir, 0).await;
	let legacy_path = dir.join(&infos[0].segment_path);
	let legacy_bytes = std::fs::read(&legacy_path).unwrap();
	let retained_before = metrics().storage_retained_secondary_sidecar_files.get();
	let first = manager
		.persist_secondary_index_segment(&segments[0])
		.await
		.unwrap();
	assert!(
		legacy_path.exists(),
		"the other live alias still references the old path"
	);
	let second = manager
		.persist_secondary_index_segment(&segments[1])
		.await
		.unwrap();
	assert_eq!(std::fs::read(&legacy_path).unwrap(), legacy_bytes);
	assert_eq!(
		metrics().storage_retained_secondary_sidecar_files.get(),
		retained_before + 1
	);
	assert_eq!(
		manager.load_secondary_index_segment(&first).await.unwrap(),
		Some(segments[0].clone())
	);
	assert_eq!(
		manager.load_secondary_index_segment(&second).await.unwrap(),
		Some(segments[1].clone())
	);
}

async fn open_fresh_sidecar_manager_with_ops(
	dir: &Path,
	file_ops: Arc<dyn AsyncFileOps>,
) -> Arc<SlabManager> {
	let io = spawn_io_thread(64).unwrap();
	let crypto = test_crypto();
	SlabManager::initialize_new_store_with_file_ops(
		SlabManagerConfig {
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			dir: dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io,
			l2: None,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: GroupCommitConfig::default(),
			crypto,
		},
		file_ops,
	)
	.await
	.unwrap()
}

#[tokio::test]
async fn publication_refuses_when_persistent_index_is_unavailable() {
	let dir = fresh_dir("sidecar-index-unavailable");
	let manager = open_manager_at_dir(
		dir.to_path_buf(),
		spawn_io_thread(64).unwrap(),
		test_crypto(),
	)
	.await;
	assert!(
		manager.persistent_index.is_none(),
		"premise: missing redb was not initialized"
	);
	let error = manager
		.persist_secondary_index_segment(&segment_with_key(SlabId::new(86), 1))
		.await
		.unwrap_err();
	assert_eq!(error.kind(), ErrorKind::WouldBlock);
	assert!(manager.secondary_index_segments.is_empty());
	assert!(
		!dir.join("secondary_index").exists(),
		"refused publication must not stage a file"
	);
}

#[tokio::test]
async fn failed_sidecar_cleanup_counts_and_exports_retained_new_final() {
	let dir = fresh_dir("sidecar-failed-cleanup-gauge");
	let gate = Arc::new(ParkFirstSidecarTmpOpen {
		inner: Arc::new(BlockingFileOps),
		armed: AtomicBool::new(false),
		parked: Notify::new(),
		release: Notify::new(),
		synchronous_boundaries: false,
		park_unlink: AtomicBool::new(false),
		deny_final_unlink: AtomicBool::new(false),
		staging_started: AtomicUsize::new(0),
		tmp_fd: AtomicI32::new(-1),
		unlinks_started: AtomicUsize::new(0),
	});
	let manager = open_fresh_sidecar_manager_with_ops(&dir, gate.clone()).await;
	let segment = segment_with_key(SlabId::new(96), 1);
	let original = manager
		.persist_secondary_index_segment(&segment)
		.await
		.unwrap();
	let original_path = dir.join(&original.segment_path);
	let original_bytes = std::fs::read(&original_path).unwrap();
	let before_files = metrics().storage_retained_secondary_sidecar_files.get();
	let before_bytes = metrics().storage_retained_secondary_sidecar_bytes.get();
	gate.deny_final_unlink.store(true, Ordering::Release);
	manager
		.persistent_index
		.as_ref()
		.unwrap()
		.fail_next_delta
		.store(true, Ordering::Release);
	assert!(
		manager
			.persist_secondary_index_segment(&segment)
			.await
			.is_err()
	);
	assert!(manager.has_orphaned_index_publications());
	assert_eq!(std::fs::read(&original_path).unwrap(), original_bytes);
	let parent = original_path.parent().unwrap();
	let retained = std::fs::read_dir(parent)
		.unwrap()
		.map(Result::unwrap)
		.map(|entry| entry.path())
		.find(|path| path != &original_path && !ParkFirstSidecarTmpOpen::is_sidecar_tmp(path))
		.expect("new final remains after the injected unlink failure");
	let bytes = std::fs::metadata(&retained).unwrap().len();
	assert_eq!(
		metrics().storage_retained_secondary_sidecar_files.get(),
		before_files + 1
	);
	assert_eq!(
		metrics().storage_retained_secondary_sidecar_bytes.get(),
		before_bytes + bytes
	);
	let rendered = metrics().render_prometheus();
	assert!(rendered.lines().any(|line| line
		== format!(
			"wendelon_storage_retained_secondary_sidecar_files {}",
			before_files + 1
		)));
	assert!(rendered.lines().any(|line| line
		== format!(
			"wendelon_storage_retained_secondary_sidecar_bytes {}",
			before_bytes + bytes
		)));
}
