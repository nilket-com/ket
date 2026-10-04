use crate::metrics::global as metrics;
use crate::storage::io_uring::{AlignedBuf, DIRECT_IO_ALIGN, IoOp, IoRingHandle};
use async_trait::async_trait;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Error, ErrorKind};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Linux `O_DIRECT` flag for the target architecture. Shared across production
/// storage paths so callers can request direct I/O without reaching into `libc`.
/// Its value differs by architecture: x86_64's `0o40000` is `O_DIRECTORY` on
/// aarch64, where opening a slab with it fails with `EINVAL`.
pub const O_DIRECT: i32 = libc::O_DIRECT;

#[derive(Clone)]
pub struct FileHandle {
	file: Arc<File>,
}

impl FileHandle {
	pub fn new(file: File) -> Self {
		Self {
			file: Arc::new(file),
		}
	}

	pub fn raw_fd(&self) -> RawFd {
		self.file.as_raw_fd()
	}

	pub fn try_clone(&self) -> std::io::Result<File> {
		self.file.try_clone()
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenFlags {
	pub read: bool,
	pub write: bool,
	pub create: bool,
	pub create_new: bool,
	pub truncate: bool,
	pub direct: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
	pub file_name: OsString,
	pub is_file: bool,
}

#[async_trait]
pub trait AsyncFileOps: Send + Sync {
	async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle>;

	async fn read_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		len: usize,
	) -> std::io::Result<Vec<u8>>;

	async fn write_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		bytes: Vec<u8>,
	) -> std::io::Result<usize>;

	async fn read_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)>;

	async fn write_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)>;

	async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()>;

	async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()>;

	async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>>;

	async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()>;

	async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>>;

	async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()>;

	async fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;

	async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<DirectoryEntry>>;

	async fn fsync_dir(&self, path: &Path) -> std::io::Result<()>;

	async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;

	async fn unlink(&self, path: &Path) -> std::io::Result<()>;
}

#[derive(Clone)]
pub struct IoUringFileOps {
	io: IoRingHandle,
	blocking: Option<BlockingIoExecutor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockingIoClass {
	WalMetadata,
	SlabMetadata,
}

impl IoUringFileOps {
	pub fn new(io: IoRingHandle) -> Self {
		Self { io, blocking: None }
	}

	pub fn with_isolated_blocking(
		io: IoRingHandle,
		thread_name: impl Into<String>,
		class: BlockingIoClass,
	) -> std::io::Result<Self> {
		Ok(Self {
			io,
			blocking: Some(BlockingIoExecutor::spawn(thread_name, class)?),
		})
	}

	async fn submit_sync(&self, handle: &FileHandle, op: IoOp) -> std::io::Result<()> {
		let reply = match op {
			IoOp::Fsync => self.io.submit_fsync(handle.raw_fd()).await?,
			IoOp::Fdatasync => self.io.submit_fdatasync(handle.raw_fd()).await?,
			IoOp::Read | IoOp::Write => {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"submit_sync only supports fsync-style operations",
				));
			}
		};
		let completion = reply
			.await
			.map_err(|_| Error::new(ErrorKind::BrokenPipe, "io_uring reply channel closed"))?;
		if completion.result < 0 {
			return Err(Error::from_raw_os_error(-completion.result));
		}
		Ok(())
	}

	async fn run_blocking<T, F>(&self, f: F) -> std::io::Result<T>
	where
		T: Send + 'static,
		F: FnOnce() -> std::io::Result<T> + Send + 'static,
	{
		if let Some(blocking) = &self.blocking {
			blocking.run(f).await
		} else {
			spawn_blocking_io(f).await
		}
	}
}

#[async_trait]
impl AsyncFileOps for IoUringFileOps {
	async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
		let path = path.to_path_buf();
		self.run_blocking(move || {
			let mut options = std::fs::OpenOptions::new();
			options
				.read(flags.read)
				.write(flags.write)
				.create(flags.create)
				.create_new(flags.create_new)
				.truncate(flags.truncate);
			if flags.direct {
				options.custom_flags(O_DIRECT);
			}
			options.open(path).map(FileHandle::new)
		})
		.await
	}

	async fn read_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		len: usize,
	) -> std::io::Result<Vec<u8>> {
		let file = handle.try_clone()?;
		self.run_blocking(move || {
			let mut buf = vec![0u8; len];
			let read = file.read_at(&mut buf, offset)?;
			buf.truncate(read);
			Ok(buf)
		})
		.await
	}

	async fn write_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		bytes: Vec<u8>,
	) -> std::io::Result<usize> {
		let file = handle.try_clone()?;
		self.run_blocking(move || file.write_at(&bytes, offset))
			.await
	}

	async fn read_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		let reply = self
			.io
			.submit(IoOp::Read, handle.raw_fd(), buf, offset)
			.await?;
		let completion = reply
			.await
			.map_err(|_| Error::new(ErrorKind::BrokenPipe, "io_uring reply channel closed"))?;
		if completion.result < 0 {
			return Err(Error::from_raw_os_error(-completion.result));
		}
		let buf = completion.buf.ok_or_else(|| {
			Error::other("io_uring read completion missing aligned buffer payload")
		})?;
		Ok((completion.result as usize, buf))
	}

	async fn write_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		let reply = self
			.io
			.submit(IoOp::Write, handle.raw_fd(), buf, offset)
			.await?;
		let completion = reply
			.await
			.map_err(|_| Error::new(ErrorKind::BrokenPipe, "io_uring reply channel closed"))?;
		if completion.result < 0 {
			return Err(Error::from_raw_os_error(-completion.result));
		}
		let buf = completion.buf.ok_or_else(|| {
			Error::other("io_uring write completion missing aligned buffer payload")
		})?;
		Ok((completion.result as usize, buf))
	}

	async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
		self.submit_sync(handle, IoOp::Fsync).await
	}

	async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
		self.submit_sync(handle, IoOp::Fdatasync).await
	}

	async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		let path = path.to_path_buf();
		self.run_blocking(move || std::fs::read(path)).await
	}

	async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
		let path = path.to_path_buf();
		let bytes = bytes.to_vec();
		self.run_blocking(move || std::fs::write(path, bytes)).await
	}

	async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
		let path = path.to_path_buf();
		self.run_blocking(move || match std::fs::metadata(path) {
			Ok(meta) => Ok(Some(meta.len())),
			Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
			Err(err) => Err(err),
		})
		.await
	}

	async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
		let file = handle.try_clone()?;
		self.run_blocking(move || file.set_len(len)).await
	}

	async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
		let path = path.to_path_buf();
		self.run_blocking(move || std::fs::create_dir_all(path))
			.await
	}

	async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<DirectoryEntry>> {
		let path = path.to_path_buf();
		self.run_blocking(move || {
			let mut entries = Vec::new();
			for entry in std::fs::read_dir(path)? {
				let entry = entry?;
				entries.push(DirectoryEntry {
					file_name: entry.file_name(),
					is_file: entry.file_type()?.is_file(),
				});
			}
			Ok(entries)
		})
		.await
	}

	async fn fsync_dir(&self, path: &Path) -> std::io::Result<()> {
		let path = path.to_path_buf();
		self.run_blocking(move || {
			let dir = File::open(path)?;
			dir.sync_all()
		})
		.await
	}

	async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		let from = from.to_path_buf();
		let to = to.to_path_buf();
		self.run_blocking(move || std::fs::rename(from, to)).await
	}

	async fn unlink(&self, path: &Path) -> std::io::Result<()> {
		let path = path.to_path_buf();
		self.run_blocking(move || std::fs::remove_file(path)).await
	}
}

#[derive(Clone, Default)]
pub struct BlockingFileOps;

#[async_trait]
impl AsyncFileOps for BlockingFileOps {
	async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
		let path = path.to_path_buf();
		spawn_blocking_io(move || {
			let mut options = std::fs::OpenOptions::new();
			options
				.read(flags.read)
				.write(flags.write)
				.create(flags.create)
				.create_new(flags.create_new)
				.truncate(flags.truncate);
			if flags.direct {
				options.custom_flags(O_DIRECT);
			}
			options.open(path).map(FileHandle::new)
		})
		.await
	}

	async fn read_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		len: usize,
	) -> std::io::Result<Vec<u8>> {
		let file = handle.try_clone()?;
		spawn_blocking_io(move || {
			let mut buf = vec![0u8; len];
			let read = file.read_at(&mut buf, offset)?;
			buf.truncate(read);
			Ok(buf)
		})
		.await
	}

	async fn write_plain_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		bytes: Vec<u8>,
	) -> std::io::Result<usize> {
		let file = handle.try_clone()?;
		spawn_blocking_io(move || file.write_at(&bytes, offset)).await
	}

	async fn read_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		let file = handle.try_clone()?;
		spawn_blocking_io(move || {
			let mut buf = buf;
			let read = file.read_at(buf.as_mut_slice(), offset)?;
			Ok((read, buf))
		})
		.await
	}

	async fn write_at(
		&self,
		handle: &FileHandle,
		offset: u64,
		buf: AlignedBuf,
	) -> std::io::Result<(usize, AlignedBuf)> {
		let file = handle.try_clone()?;
		spawn_blocking_io(move || {
			let written = file.write_at(buf.as_slice(), offset)?;
			Ok((written, buf))
		})
		.await
	}

	async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
		let file = handle.try_clone()?;
		spawn_blocking_io(move || file.sync_all()).await
	}

	async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
		let file = handle.try_clone()?;
		spawn_blocking_io(move || file.sync_data()).await
	}

	async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		let path = path.to_path_buf();
		spawn_blocking_io(move || std::fs::read(path)).await
	}

	async fn write_all(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
		let path = path.to_path_buf();
		let bytes = bytes.to_vec();
		spawn_blocking_io(move || std::fs::write(path, bytes)).await
	}

	async fn file_len(&self, path: &Path) -> std::io::Result<Option<u64>> {
		let path = path.to_path_buf();
		spawn_blocking_io(move || match std::fs::metadata(path) {
			Ok(meta) => Ok(Some(meta.len())),
			Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
			Err(err) => Err(err),
		})
		.await
	}

	async fn set_len(&self, handle: &FileHandle, len: u64) -> std::io::Result<()> {
		let file = handle.try_clone()?;
		spawn_blocking_io(move || file.set_len(len)).await
	}

	async fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
		let path = path.to_path_buf();
		spawn_blocking_io(move || std::fs::create_dir_all(path)).await
	}

	async fn read_dir(&self, path: &Path) -> std::io::Result<Vec<DirectoryEntry>> {
		let path = path.to_path_buf();
		spawn_blocking_io(move || {
			let mut entries = Vec::new();
			for entry in std::fs::read_dir(path)? {
				let entry = entry?;
				entries.push(DirectoryEntry {
					file_name: entry.file_name(),
					is_file: entry.file_type()?.is_file(),
				});
			}
			Ok(entries)
		})
		.await
	}

	async fn fsync_dir(&self, path: &Path) -> std::io::Result<()> {
		let path = path.to_path_buf();
		spawn_blocking_io(move || {
			let dir = File::open(path)?;
			dir.sync_all()
		})
		.await
	}

	async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		let from = from.to_path_buf();
		let to = to.to_path_buf();
		spawn_blocking_io(move || std::fs::rename(from, to)).await
	}

	async fn unlink(&self, path: &Path) -> std::io::Result<()> {
		let path = path.to_path_buf();
		spawn_blocking_io(move || std::fs::remove_file(path)).await
	}
}

pub async fn write_bytes(
	file_ops: &dyn AsyncFileOps,
	handle: &FileHandle,
	offset: u64,
	bytes: &[u8],
) -> std::io::Result<()> {
	let mut written_total = 0usize;
	while written_total < bytes.len() {
		let chunk = &bytes[written_total..];
		let remainder = chunk.len() % DIRECT_IO_ALIGN;
		let aligned_len = if remainder == 0 {
			chunk.len()
		} else {
			chunk.len() + (DIRECT_IO_ALIGN - remainder)
		};
		let mut buf = AlignedBuf::new(aligned_len);
		buf.as_mut_slice()[..chunk.len()].copy_from_slice(chunk);
		let (written, _buf) = file_ops
			.write_at(handle, offset + written_total as u64, buf)
			.await?;
		if written == 0 {
			return Err(Error::new(
				ErrorKind::WriteZero,
				"file write completed with zero bytes written",
			));
		}
		written_total += written.min(chunk.len());
	}
	Ok(())
}

pub async fn write_file(
	file_ops: &dyn AsyncFileOps,
	path: &Path,
	flags: OpenFlags,
	bytes: &[u8],
) -> std::io::Result<()> {
	let handle = file_ops.open(path, flags).await?;
	write_bytes(file_ops, &handle, 0, bytes).await?;
	file_ops.set_len(&handle, bytes.len() as u64).await?;
	file_ops.fsync(&handle).await
}

pub async fn fsync_parent_dir(file_ops: &dyn AsyncFileOps, path: &Path) -> std::io::Result<()> {
	let parent = path.parent().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("path has no parent directory: {}", path.display()),
		)
	})?;
	file_ops.fsync_dir(parent).await
}

async fn spawn_blocking_io<T, F>(f: F) -> std::io::Result<T>
where
	T: Send + 'static,
	F: FnOnce() -> std::io::Result<T> + Send + 'static,
{
	let observation = crate::storage::finality_observation::current_work();
	tokio::task::spawn_blocking(move || {
		let _observation = observation;
		f()
	})
	.await
	.map_err(|err| Error::other(format!("blocking file op task failed: {err}")))?
}

type BlockingTask = Box<dyn FnOnce() + Send + 'static>;

const ISOLATED_BLOCKING_IO_QUEUE_DEPTH: usize = 1024;
const ISOLATED_BLOCKING_IO_WORKERS: usize = 4;

#[derive(Clone)]
struct BlockingIoExecutor {
	tx: tokio::sync::mpsc::Sender<BlockingTask>,
	class: BlockingIoClass,
	queued_tasks: Arc<AtomicU64>,
}

impl BlockingIoExecutor {
	fn spawn(thread_name: impl Into<String>, class: BlockingIoClass) -> std::io::Result<Self> {
		let thread_name = thread_name.into();
		let (tx, rx) = tokio::sync::mpsc::channel(ISOLATED_BLOCKING_IO_QUEUE_DEPTH);
		let queued_tasks = Arc::new(AtomicU64::new(0));
		let rx = Arc::new(std::sync::Mutex::new(rx));
		for worker_idx in 0..ISOLATED_BLOCKING_IO_WORKERS {
			let worker_name = format!("{thread_name}-{worker_idx}");
			let rx = rx.clone();
			let queued_tasks = queued_tasks.clone();
			std::thread::Builder::new()
				.name(worker_name.clone())
				.spawn(move || {
					loop {
						let task = {
							let mut guard = rx.lock().unwrap();
							guard.blocking_recv()
						};
						let Some(task) = task else {
							break;
						};
						let depth = queued_tasks.fetch_sub(1, Ordering::Relaxed) - 1;
						update_blocking_io_queue_depth(class, depth);
						let _ = catch_unwind(AssertUnwindSafe(task));
					}
				})
				.map_err(|err| {
					Error::other(format!(
						"failed to spawn isolated blocking io thread {worker_name}: {err}"
					))
				})?;
		}
		update_blocking_io_queue_depth(class, 0);
		Ok(Self {
			tx,
			class,
			queued_tasks,
		})
	}

	async fn run<T, F>(&self, f: F) -> std::io::Result<T>
	where
		T: Send + 'static,
		F: FnOnce() -> std::io::Result<T> + Send + 'static,
	{
		let (tx, rx) = tokio::sync::oneshot::channel();
		let observation = crate::storage::finality_observation::current_work();
		let task: BlockingTask = Box::new(move || {
			let _observation = observation;
			let _ = tx.send(f());
		});
		let depth = self.queued_tasks.fetch_add(1, Ordering::Relaxed) + 1;
		update_blocking_io_queue_depth(self.class, depth);
		self.tx.send(task).await.map_err(|_| {
			let depth = self.queued_tasks.fetch_sub(1, Ordering::Relaxed) - 1;
			update_blocking_io_queue_depth(self.class, depth);
			Error::other("isolated blocking io thread stopped before accepting file op")
		})?;
		rx.await
			.map_err(|_| Error::other("isolated blocking io task was cancelled"))?
	}
}

fn update_blocking_io_queue_depth(class: BlockingIoClass, depth: u64) {
	match class {
		BlockingIoClass::WalMetadata => metrics().storage_wal_blocking_queue_depth.set(depth),
		BlockingIoClass::SlabMetadata => metrics().storage_slab_blocking_queue_depth.set(depth),
	}
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultOp {
	WriteAt,
	ReadAll,
	Fsync,
	Fdatasync,
	FsyncDir,
	Rename,
	Unlink,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub enum FaultAction {
	Error {
		kind: ErrorKind,
		message: String,
	},
	PartialWriteError {
		written_bytes: usize,
		persist_partial: bool,
		kind: ErrorKind,
		message: String,
	},
	CorruptReadAll {
		offset: usize,
		xor_mask: u8,
	},
	Skip,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct InjectedFault {
	op: FaultOp,
	path_suffix: Option<std::path::PathBuf>,
	remaining_matches: usize,
	action: FaultAction,
}

#[cfg(test)]
impl InjectedFault {
	pub fn on(op: FaultOp, action: FaultAction) -> Self {
		Self {
			op,
			path_suffix: None,
			remaining_matches: 0,
			action,
		}
	}

	pub fn for_path(mut self, path_suffix: impl Into<std::path::PathBuf>) -> Self {
		self.path_suffix = Some(path_suffix.into());
		self
	}

	pub fn after_matches(mut self, remaining_matches: usize) -> Self {
		self.remaining_matches = remaining_matches;
		self
	}

	fn matches(&self, op: FaultOp, path: &Path) -> bool {
		self.op == op
			&& self
				.path_suffix
				.as_ref()
				.map(|suffix| path.ends_with(suffix))
				.unwrap_or(true)
	}
}

#[cfg(test)]
#[derive(Default)]
struct FaultyFileOpsState {
	handle_paths: std::collections::HashMap<RawFd, std::path::PathBuf>,
	durable_dirs: std::collections::HashSet<std::path::PathBuf>,
	durable_paths: std::collections::HashSet<std::path::PathBuf>,
	durable_contents: std::collections::HashMap<std::path::PathBuf, Vec<u8>>,
	faults: std::collections::VecDeque<InjectedFault>,
}

#[cfg(test)]
#[derive(Clone)]
pub struct FaultyFileOps {
	root: std::path::PathBuf,
	inner: BlockingFileOps,
	state: Arc<std::sync::Mutex<FaultyFileOpsState>>,
}

#[cfg(test)]
impl FaultyFileOps {
	pub fn new(root: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
		let root = root.into();
		std::fs::create_dir_all(&root)?;
		let mut state = FaultyFileOpsState::default();
		snapshot_durable_tree(&root, &root, &mut state)?;
		Ok(Self {
			root,
			inner: BlockingFileOps,
			state: Arc::new(std::sync::Mutex::new(state)),
		})
	}

	pub fn inject(&self, fault: InjectedFault) {
		self.state.lock().unwrap().faults.push_back(fault);
	}

	pub fn crash(&self) -> std::io::Result<()> {
		let (durable_dirs, durable_files) = {
			let mut state = self.state.lock().unwrap();
			state.handle_paths.clear();
			let mut dirs = state.durable_dirs.iter().cloned().collect::<Vec<_>>();
			dirs.sort();
			let mut files = state
				.durable_paths
				.iter()
				.cloned()
				.map(|path| {
					let bytes = state
						.durable_contents
						.get(&path)
						.cloned()
						.unwrap_or_default();
					(path, bytes)
				})
				.collect::<Vec<_>>();
			files.sort_by(|a, b| a.0.cmp(&b.0));
			(dirs, files)
		};

		match std::fs::remove_dir_all(&self.root) {
			Ok(()) => {}
			Err(err) if err.kind() == ErrorKind::NotFound => {}
			Err(err) => return Err(err),
		}
		std::fs::create_dir_all(&self.root)?;
		for dir in durable_dirs {
			std::fs::create_dir_all(&dir)?;
		}
		for (path, bytes) in durable_files {
			if let Some(parent) = path.parent() {
				std::fs::create_dir_all(parent)?;
			}
			std::fs::write(path, bytes)?;
		}
		Ok(())
	}

	fn current_path(&self, handle: &FileHandle) -> std::io::Result<std::path::PathBuf> {
		self.state
			.lock()
			.unwrap()
			.handle_paths
			.get(&handle.raw_fd())
			.cloned()
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"faulty file-ops lost handle mapping for fd {}",
						handle.raw_fd()
					),
				)
			})
	}

	fn take_fault(&self, op: FaultOp, path: &Path) -> Option<FaultAction> {
		let mut state = self.state.lock().unwrap();
		let idx = state
			.faults
			.iter()
			.position(|fault| fault.matches(op, path))?;
		let fault = state.faults.get_mut(idx).unwrap();
		if fault.remaining_matches > 0 {
			fault.remaining_matches -= 1;
			return None;
		}
		Some(state.faults.remove(idx).unwrap().action)
	}

	fn persist_file_now(&self, path: &Path) -> std::io::Result<()> {
		let bytes = std::fs::read(path)?;
		self.state
			.lock()
			.unwrap()
			.durable_contents
			.insert(path.to_path_buf(), bytes);
		Ok(())
	}

	fn snapshot_dir_now(&self, dir: &Path) -> std::io::Result<()> {
		let mut state = self.state.lock().unwrap();
		snapshot_durable_tree(&self.root, dir, &mut state)
	}

	fn apply_fault_result(&self, action: FaultAction) -> std::io::Result<()> {
		match action {
			FaultAction::Error { kind, message } => Err(Error::new(kind, message)),
			FaultAction::Skip => Ok(()),
			other => Err(Error::new(
				ErrorKind::InvalidInput,
				format!("fault action {other:?} is not valid for this operation"),
			)),
		}
	}
}

#[cfg(test)]
#[async_trait]
impl AsyncFileOps for FaultyFileOps {
	async fn open(&self, path: &Path, flags: OpenFlags) -> std::io::Result<FileHandle> {
		let handle = self.inner.open(path, flags).await?;
		self.state
			.lock()
			.unwrap()
			.handle_paths
			.insert(handle.raw_fd(), path.to_path_buf());
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
		let path = self.current_path(handle)?;
		if let Some(action) = self.take_fault(FaultOp::WriteAt, &path) {
			match action {
				FaultAction::PartialWriteError {
					written_bytes,
					persist_partial,
					kind,
					message,
				} => {
					let file = handle.try_clone()?;
					let partial = bytes[..written_bytes.min(bytes.len())].to_vec();
					let written =
						spawn_blocking_io(move || file.write_at(&partial, offset)).await?;
					if persist_partial && self.state.lock().unwrap().durable_paths.contains(&path) {
						self.persist_file_now(&path)?;
					}
					if written > 0 {
						return Err(Error::new(kind, message));
					}
				}
				other => {
					self.apply_fault_result(other)?;
					return Ok(0);
				}
			}
		}
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
		let path = self.current_path(handle)?;
		if let Some(action) = self.take_fault(FaultOp::WriteAt, &path) {
			match action {
				FaultAction::PartialWriteError {
					written_bytes,
					persist_partial,
					kind,
					message,
				} => {
					let bytes = buf.as_slice()[..written_bytes.min(buf.len())].to_vec();
					let file = handle.try_clone()?;
					spawn_blocking_io(move || {
						let written = file.write_at(&bytes, offset)?;
						if written != bytes.len() {
							return Err(Error::new(
								ErrorKind::WriteZero,
								format!(
									"faulty partial write wrote {} of {} bytes",
									written,
									bytes.len()
								),
							));
						}
						Ok(())
					})
					.await?;
					if persist_partial && self.state.lock().unwrap().durable_paths.contains(&path) {
						self.persist_file_now(&path)?;
					}
					return Err(Error::new(kind, message));
				}
				other => {
					self.apply_fault_result(other)?;
					return Ok((0, buf));
				}
			}
		}
		self.inner.write_at(handle, offset, buf).await
	}

	async fn fsync(&self, handle: &FileHandle) -> std::io::Result<()> {
		let path = self.current_path(handle)?;
		if let Some(action) = self.take_fault(FaultOp::Fsync, &path) {
			self.apply_fault_result(action)?;
			return Ok(());
		}
		self.inner.fsync(handle).await?;
		self.persist_file_now(&path)
	}

	async fn fdatasync(&self, handle: &FileHandle) -> std::io::Result<()> {
		let path = self.current_path(handle)?;
		if let Some(action) = self.take_fault(FaultOp::Fdatasync, &path) {
			self.apply_fault_result(action)?;
			return Ok(());
		}
		self.inner.fdatasync(handle).await?;
		self.persist_file_now(&path)
	}

	async fn read_all(&self, path: &Path) -> std::io::Result<Vec<u8>> {
		let mut bytes = self.inner.read_all(path).await?;
		if let Some(action) = self.take_fault(FaultOp::ReadAll, path) {
			match action {
				FaultAction::CorruptReadAll { offset, xor_mask } => {
					if let Some(byte) = bytes.get_mut(offset) {
						*byte ^= xor_mask;
					}
				}
				other => {
					self.apply_fault_result(other)?;
					return Ok(bytes);
				}
			}
		}
		Ok(bytes)
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
		if let Some(action) = self.take_fault(FaultOp::FsyncDir, path) {
			match action {
				FaultAction::Skip => return Ok(()),
				other => {
					self.apply_fault_result(other)?;
					return Ok(());
				}
			}
		}
		self.inner.fsync_dir(path).await?;
		self.snapshot_dir_now(path)
	}

	async fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
		if let Some(action) = self.take_fault(FaultOp::Rename, to) {
			self.apply_fault_result(action)?;
			return Ok(());
		}
		self.inner.rename(from, to).await?;
		let mut state = self.state.lock().unwrap();
		for mapped_path in state.handle_paths.values_mut() {
			if *mapped_path == from {
				*mapped_path = to.to_path_buf();
			}
		}
		Ok(())
	}

	async fn unlink(&self, path: &Path) -> std::io::Result<()> {
		if let Some(action) = self.take_fault(FaultOp::Unlink, path) {
			self.apply_fault_result(action)?;
			return Ok(());
		}
		self.inner.unlink(path).await
	}
}

#[cfg(test)]
fn snapshot_durable_tree(
	root: &Path,
	dir: &Path,
	state: &mut FaultyFileOpsState,
) -> std::io::Result<()> {
	if !dir.exists() {
		return Ok(());
	}
	state
		.durable_dirs
		.retain(|existing| existing == root || (!existing.starts_with(dir) || existing == dir));
	state.durable_paths.retain(|path| !path.starts_with(dir));
	state
		.durable_contents
		.retain(|path, _| !path.starts_with(dir));
	state.durable_dirs.insert(dir.to_path_buf());
	for entry in std::fs::read_dir(dir)? {
		let entry = entry?;
		let path = entry.path();
		let ty = entry.file_type()?;
		if ty.is_dir() {
			snapshot_durable_tree(root, &path, state)?;
		} else if ty.is_file() {
			state.durable_paths.insert(path.clone());
			state
				.durable_contents
				.insert(path.clone(), std::fs::read(&path)?);
		}
	}
	if dir == root {
		return Ok(());
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::{BlockingIoClass, BlockingIoExecutor, ISOLATED_BLOCKING_IO_WORKERS};
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::{Arc, Barrier};
	use std::time::Duration;

	#[test]
	fn direct_io_flag_is_never_another_architectures_value() {
		// x86_64's O_DIRECT is aarch64's O_DIRECTORY; a hard-coded value opened
		// every slab as a directory on arm64 Linux.
		assert_ne!(super::O_DIRECT, libc::O_DIRECTORY);
		assert_ne!(super::O_DIRECT, 0);
	}

	#[tokio::test]
	async fn isolated_blocking_executor_runs_tasks_in_parallel() {
		let executor = BlockingIoExecutor::spawn(
			"wendelon-test-isolated-blocking",
			BlockingIoClass::SlabMetadata,
		)
		.unwrap();
		let barrier = Arc::new(Barrier::new(ISOLATED_BLOCKING_IO_WORKERS));
		let active = Arc::new(AtomicUsize::new(0));
		let max_active = Arc::new(AtomicUsize::new(0));
		let mut tasks = Vec::new();
		for _ in 0..ISOLATED_BLOCKING_IO_WORKERS {
			let executor = executor.clone();
			let barrier = barrier.clone();
			let active = active.clone();
			let max_active = max_active.clone();
			tasks.push(tokio::spawn(async move {
				executor
					.run(move || {
						let now = active.fetch_add(1, Ordering::SeqCst) + 1;
						max_active.fetch_max(now, Ordering::SeqCst);
						barrier.wait();
						active.fetch_sub(1, Ordering::SeqCst);
						Ok::<_, std::io::Error>(())
					})
					.await
					.unwrap();
			}));
		}
		tokio::time::timeout(Duration::from_secs(1), async {
			for task in tasks {
				task.await.unwrap();
			}
		})
		.await
		.unwrap();
		assert_eq!(
			max_active.load(Ordering::SeqCst),
			ISOLATED_BLOCKING_IO_WORKERS
		);
	}
}

#[cfg(test)]
mod adr629_p2b_observed_io_tests {
	use super::*;
	use crate::storage::finality_observation::{
		FinalityObservationRegistry, WorkKind, WorkScope, with_work,
	};

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn adr629_p2b_both_blocking_executors_keep_owners_before_and_after_real_file_write() {
		for isolated in [false, true] {
			for after_write in [false, true] {
				let registry = FinalityObservationRegistry::new();
				let owner = registry.begin(WorkScope::Node, WorkKind::StoragePublication);
				let path =
					std::env::temp_dir().join(format!("adr629-p2b-io-{}", uuid::Uuid::new_v4()));
				let output = path.clone();
				let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
				let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
				let work = move || {
					if after_write {
						std::fs::write(&output, b"counted publication")?;
					}
					ready_tx.send(()).unwrap();
					release_rx.recv().unwrap();
					if !after_write {
						std::fs::write(&output, b"counted publication")?;
					}
					Ok(())
				};
				let caller = tokio::spawn(with_work(owner, async move {
					if isolated {
						let executor = BlockingIoExecutor::spawn(
							"adr629-p2b-observed",
							BlockingIoClass::SlabMetadata,
						)
						.unwrap();
						executor.run(work).await
					} else {
						spawn_blocking_io(work).await
					}
				}));
				tokio::time::timeout(
					std::time::Duration::from_secs(10),
					tokio::task::spawn_blocking(move || ready_rx.recv().unwrap()),
				)
				.await
				.unwrap()
				.unwrap();
				assert_eq!(path.exists(), after_write);
				caller.abort();
				let _ = caller.await;
				assert_eq!(registry.test_state(), (1, 0, None));
				release_tx.send(()).unwrap();
				tokio::time::timeout(std::time::Duration::from_secs(10), async {
					while registry.test_state().0 != 0 {
						tokio::task::yield_now().await;
					}
				})
				.await
				.unwrap();
				assert_eq!(std::fs::read(&path).unwrap(), b"counted publication");
				std::fs::remove_file(path).unwrap();
			}
		}
	}
}
