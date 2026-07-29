use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BackingIdentity {
	pub length: u64,
	pub modified_nanos: u128,
	pub file_id: Option<u128>,
}

impl BackingIdentity {
	pub(crate) fn for_path(path: &std::path::Path) -> io::Result<Self> {
		let metadata = std::fs::metadata(path)?;
		Ok(Self::from_metadata(&metadata))
	}

	fn from_metadata(metadata: &std::fs::Metadata) -> Self {
		let modified_nanos = metadata
			.modified()
			.ok()
			.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
			.map_or(0, |duration| duration.as_nanos());
		#[cfg(unix)]
		let file_id = {
			use std::os::unix::fs::MetadataExt;
			Some((u128::from(metadata.dev()) << 64) | u128::from(metadata.ino()))
		};
		#[cfg(not(unix))]
		let file_id = None;
		Self {
			length: metadata.len(),
			modified_nanos,
			file_id,
		}
	}
}

pub trait BackingStore: Send + Sync {
	fn len(&self) -> u64;
	fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> io::Result<()>;
	fn identity(&self) -> BackingIdentity;

	fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

pub struct FileBackingStore {
	file: Mutex<File>,
	identity: BackingIdentity,
}

impl FileBackingStore {
	pub fn open(path: &std::path::Path) -> io::Result<Self> {
		let file = File::open(path)?;
		let identity = BackingIdentity::from_metadata(&file.metadata()?);
		Ok(Self {
			file: Mutex::new(file),
			identity,
		})
	}
}

impl BackingStore for FileBackingStore {
	fn len(&self) -> u64 {
		self.identity.length
	}

	fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> io::Result<()> {
		let mut file = self.file.lock().unwrap();
		file.seek(SeekFrom::Start(offset))?;
		file.read_exact(dst)
	}

	fn identity(&self) -> BackingIdentity {
		self.identity
	}
}

/// A zero-allocation logical store for huge-address tests.
pub struct GeneratedBackingStore {
	length: u64,
	identity: BackingIdentity,
	generator: Arc<dyn Fn(u64) -> u8 + Send + Sync>,
	bytes_read: AtomicU64,
}

impl GeneratedBackingStore {
	pub fn new(length: u64, generator: impl Fn(u64) -> u8 + Send + Sync + 'static) -> Self {
		Self {
			length,
			identity: BackingIdentity {
				length,
				modified_nanos: 0,
				file_id: None,
			},
			generator: Arc::new(generator),
			bytes_read: AtomicU64::new(0),
		}
	}

	pub fn bytes_read(&self) -> u64 {
		self.bytes_read.load(Ordering::Acquire)
	}
}

impl BackingStore for GeneratedBackingStore {
	fn len(&self) -> u64 {
		self.length
	}

	fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> io::Result<()> {
		let requested = u64::try_from(dst.len()).expect("buffer length fits u64");
		if offset
			.checked_add(requested)
			.is_none_or(|end| end > self.length)
		{
			return Err(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"generated read exceeds logical store",
			));
		}
		for (index, byte) in dst.iter_mut().enumerate() {
			*byte = (self.generator)(offset + index as u64);
		}
		self.bytes_read.fetch_add(requested, Ordering::Relaxed);
		Ok(())
	}

	fn identity(&self) -> BackingIdentity {
		self.identity
	}
}

#[cfg(test)]
mod tests {
	use super::{BackingStore, FileBackingStore};

	#[test]
	fn sparse_200_gib_fixture_marker_reads() {
		let Ok(path) = std::env::var("BARYON_SPARSE_FIXTURE") else {
			return;
		};
		const GIB: u64 = 1024 * 1024 * 1024;
		let store =
			FileBackingStore::open(std::path::Path::new(&path)).expect("open sparse fixture");
		assert_eq!(store.len(), 200 * GIB);
		for (offset, expected) in [
			(0, &b"HEAD"[..]),
			(4 * GIB - 4, &b"PRE4"[..]),
			(4 * GIB, &b"POST"[..]),
			(100 * GIB, &b"MIDDLE"[..]),
			(200 * GIB - 8, &b"TAILMARK"[..]),
		] {
			let mut actual = vec![0; expected.len()];
			store
				.read_exact_at(offset, &mut actual)
				.expect("marker read");
			assert_eq!(actual, expected);
		}
	}

	#[test]
	fn sparse_200_gib_warm_index_open_stays_extent_bounded() {
		use crate::core::DocByte;
		use crate::ecs::UastRegistry;
		use crate::svp::ingest::{COLD_INGEST_EXTENT_SIZE, ingest_svp_file};
		use crate::svp::line_index::{SparseLineIndex, cache_path_for_backing};
		use crate::svp::resolver::SvpResolver;
		use crate::uast::{UastMutation, UastProjection};
		use std::sync::{Arc, mpsc};

		let Ok(path) = std::env::var("BARYON_SPARSE_FIXTURE") else {
			return;
		};
		let path = std::path::PathBuf::from(path);
		let store = FileBackingStore::open(&path).expect("open sparse fixture");
		let identity = store.identity();
		let extent_count = identity.length.div_ceil(COLD_INGEST_EXTENT_SIZE as u64);
		let index = SparseLineIndex::new(
			identity,
			COLD_INGEST_EXTENT_SIZE as u32,
			vec![0; extent_count as usize],
			vec![false; extent_count as usize],
		)
		.expect("build warm index");
		let sidecar = cache_path_for_backing(&path);
		std::fs::create_dir_all(sidecar.parent().expect("cache parent")).expect("cache directory");
		index.save_atomic(&sidecar).expect("save warm index");

		let registry = Arc::new(UastRegistry::new(1_000_000));
		let (tx, _rx) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), tx);
		let root = ingest_svp_file(
			&resolver,
			&registry,
			identity.length,
			501,
			path.to_string_lossy().into_owned(),
		)
		.expect("warm sparse ingest");

		assert_eq!(registry.exact_line_count(root), Some(1));
		assert_eq!(registry.stats().allocated_nodes, extent_count + 1);
		assert_eq!(registry.stats().pending_scan_blocks, 0);
		assert_eq!(registry.stats().resident_bytes, 0);
		let near_eof = registry.find_node_at_doc_byte(
			root,
			&crate::uast::NoFolds,
			DocByte::new(identity.length - 1),
		);
		assert_eq!(
			registry.doc_byte_for_node_offset(root, near_eof.node_id, near_eof.node_byte),
			DocByte::new(identity.length - 1)
		);
		let (inserted, inserted_offset) =
			registry.insert_text(near_eof.node_id, near_eof.node_byte.get(), b"Z");
		assert_eq!(inserted_offset, 1);
		assert_eq!(registry.get_total_bytes(root), identity.length + 1);
		assert_eq!(
			registry.doc_byte_for_node_offset(root, inserted, crate::core::NodeByteOffset::ZERO,),
			DocByte::new(identity.length - 1)
		);
		assert!(registry.stats().resident_bytes <= 64 * 1024 * 1024);

		let _ = std::fs::remove_file(sidecar);
	}
}
