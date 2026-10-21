//! Two writers persisting the same secondary-index sidecar at once: an active
//! buffer seal and a lifecycle backfill of the same source slab can both reach
//! `persist_secondary_index_segment` for one path.
use super::*;
use crate::storage::file_ops::{DirectoryEntry, FileHandle};
use crate::storage::io_uring::AlignedBuf;
use std::sync::atomic::AtomicBool;
use tokio::sync::Notify;

/// Delegates every operation, but parks the first writer that opens a sidecar
/// temp file until `release` fires, so a second writer can run in that window.
struct ParkFirstSidecarTmpOpen {
	inner: Arc<dyn AsyncFileOps>,
	armed: AtomicBool,
	parked: Notify,
	release: Notify,
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
		self.inner.write_all(path, bytes).await
	}
	async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
		self.inner.file_len(path).await
	}
	async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
		self.inner.set_len(handle, len).await
	}
	async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
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
