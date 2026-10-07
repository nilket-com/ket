use super::*;
use async_trait::async_trait;
use std::{ffi::OsStr, fs::OpenOptions, os::unix::fs::OpenOptionsExt};
#[derive(Clone, Debug)]
pub(super) struct Entry {
	pub(super) name: String,
	pub(super) directory: bool,
}
#[derive(Clone, Copy)]
pub(super) struct Space {
	pub(super) bytes: u64,
	pub(super) inodes: u64,
}
/// Bounded namespace/handle seam. File bytes and barriers stay on AsyncFileOps.
#[async_trait]
pub(super) trait LocalAdapter: Send + Sync {
	async fn list(&self, path: &Path, limit: usize) -> std::io::Result<Vec<Entry>>;
	async fn mkdir(&self, path: &Path) -> std::io::Result<()>;
	async fn open_regular(
		&self,
		path: &Path,
		create: bool,
		write: bool,
	) -> std::io::Result<FileHandle>;
	async fn length(&self, handle: &FileHandle) -> std::io::Result<u64>;
	async fn lock(&self, handle: &FileHandle) -> std::io::Result<()>;
	async fn space(&self, path: &Path) -> std::io::Result<Space>;
}
pub(super) struct LinuxAdapter;
pub(super) fn invalid() -> std::io::Error {
	std::io::Error::new(
		std::io::ErrorKind::InvalidData,
		"R2 bounded namespace unavailable",
	)
}
pub(super) async fn blocking<T: Send + 'static>(
	job: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
	tokio::task::spawn_blocking(job)
		.await
		.map_err(|_| invalid())?
}
#[async_trait]
impl LocalAdapter for LinuxAdapter {
	async fn list(&self, path: &Path, limit: usize) -> std::io::Result<Vec<Entry>> {
		let path = path.to_path_buf();
		blocking(move || {
			if !std::fs::symlink_metadata(&path)?.file_type().is_dir() {
				return Err(invalid());
			}
			let mut out = Vec::new();
			for entry in std::fs::read_dir(path)? {
				let entry = entry?;
				// Stop before retaining limit+1, never collecting an unrestricted iterator.
				if out.len() == limit {
					return Err(invalid());
				}
				let ty = entry.file_type()?;
				if !(ty.is_file() || ty.is_dir()) {
					return Err(invalid());
				}
				let name = entry.file_name().into_string().map_err(|_| invalid())?;
				if name == "." || name == ".." || OsStr::new(&name).as_encoded_bytes().contains(&0)
				{
					return Err(invalid());
				}
				out.push(Entry {
					name,
					directory: ty.is_dir(),
				});
			}
			Ok(out)
		})
		.await
	}
	async fn mkdir(&self, path: &Path) -> std::io::Result<()> {
		let p = path.to_path_buf();
		blocking(move || std::fs::create_dir(p)).await
	}
	async fn open_regular(
		&self,
		path: &Path,
		create: bool,
		write: bool,
	) -> std::io::Result<FileHandle> {
		let p = path.to_path_buf();
		blocking(move || {
			let f = OpenOptions::new()
				.read(true)
				.write(write)
				.create_new(create)
				.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
				.open(p)?;
			if !f.metadata()?.is_file() {
				return Err(invalid());
			}
			Ok(FileHandle::new(f))
		})
		.await
	}
	async fn length(&self, handle: &FileHandle) -> std::io::Result<u64> {
		let handle = handle.clone();
		blocking(move || {
			let mut s = std::mem::MaybeUninit::<libc::stat>::uninit();
			// fstat keeps size/type tied to this descriptor, without a descriptor clone.
			if unsafe { libc::fstat(handle.raw_fd(), s.as_mut_ptr()) } != 0 {
				return Err(std::io::Error::last_os_error());
			}
			let s = unsafe { s.assume_init() };
			if s.st_mode & libc::S_IFMT != libc::S_IFREG || s.st_size < 0 {
				return Err(invalid());
			}
			Ok(s.st_size as u64)
		})
		.await
	}
	async fn lock(&self, handle: &FileHandle) -> std::io::Result<()> {
		let handle = handle.clone();
		blocking(move || {
			if unsafe { libc::flock(handle.raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		})
		.await
	}
	async fn space(&self, path: &Path) -> std::io::Result<Space> {
		use std::os::unix::ffi::OsStrExt;
		let p = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| invalid())?;
		blocking(move || {
			let mut s = std::mem::MaybeUninit::<libc::statvfs>::uninit();
			if unsafe { libc::statvfs(p.as_ptr(), s.as_mut_ptr()) } != 0 {
				return Err(std::io::Error::last_os_error());
			}
			let s = unsafe { s.assume_init() };
			let bytes = s.f_bavail.checked_mul(s.f_frsize).ok_or_else(invalid)?;
			Ok(Space {
				bytes,
				inodes: s.f_favail,
			})
		})
		.await
	}
}
pub(super) struct OwnedFile {
	pub(super) handle: FileHandle,
	pub(super) _permit: OwnedSemaphorePermit,
}
pub(super) struct Io {
	pub(super) files: Arc<dyn AsyncFileOps>,
	pub(super) adapter: Arc<dyn LocalAdapter>,
	pub(super) handles: Arc<Semaphore>,
	pub(super) buffers: Arc<Semaphore>,
}
impl Io {
	pub(super) fn new(files: Arc<dyn AsyncFileOps>, adapter: Arc<dyn LocalAdapter>) -> Self {
		Self {
			files,
			adapter,
			handles: Arc::new(Semaphore::new(64)),
			buffers: Arc::new(Semaphore::new(BUFFER_BYTES as usize)),
		}
	}
	pub(super) async fn list(&self, path: &Path, limit: usize) -> StoreResult<Vec<Entry>> {
		let _directory = self
			.handles
			.clone()
			.try_acquire_owned()
			.map_err(|_| StoreError::Limit)?;
		Ok(self.adapter.list(path, limit).await?)
	}
	pub(super) async fn open(&self, p: &Path, create: bool, write: bool) -> StoreResult<OwnedFile> {
		let permit = self
			.handles
			.clone()
			.try_acquire_owned()
			.map_err(|_| StoreError::Limit)?;
		let handle = self.adapter.open_regular(p, create, write).await?;
		Ok(OwnedFile {
			handle,
			_permit: permit,
		})
	}
	pub(super) async fn directory_sync(&self, p: &Path) -> StoreResult<()> {
		// Count AsyncFileOps' transient directory descriptor before submission.
		let _permit = self
			.handles
			.clone()
			.try_acquire_owned()
			.map_err(|_| StoreError::Limit)?;
		self.files.fsync_dir(p).await?;
		Ok(())
	}
	pub(super) async fn write(&self, h: &OwnedFile, mut offset: u64, b: &[u8]) -> StoreResult<()> {
		offset
			.checked_add(b.len() as u64)
			.ok_or(StoreError::Limit)?;
		let mut pos = 0;
		while pos < b.len() {
			let end = (pos + IO_CHUNK).min(b.len());
			let n = end - pos;
			let _buffer = self
				.buffers
				.clone()
				.try_acquire_many_owned(n as u32)
				.map_err(|_| StoreError::Limit)?;
			// Existing implementations clone the OS descriptor for a blocking plain operation.
			let _clone = self
				.handles
				.clone()
				.try_acquire_owned()
				.map_err(|_| StoreError::Limit)?;
			let result = self
				.files
				.write_plain_at(&h.handle, offset, b[pos..end].to_vec())
				.await;
			match result {
				Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
				Err(e) => return Err(e.into()),
				Ok(0) => {
					return Err(std::io::Error::new(
						std::io::ErrorKind::WriteZero,
						"R2 exact write stalled",
					)
					.into());
				}
				Ok(written) => {
					require(written <= n)?;
					offset = offset
						.checked_add(written as u64)
						.ok_or(StoreError::Limit)?;
					pos += written;
				}
			}
		}
		Ok(())
	}
	pub(super) async fn read(
		&self,
		h: &OwnedFile,
		offset: u64,
		n: usize,
		work: &mut u64,
	) -> StoreResult<Vec<u8>> {
		offset.checked_add(n as u64).ok_or(StoreError::Limit)?;
		*work = work.checked_add(n as u64).ok_or(StoreError::Limit)?;
		if *work > READ_WORK {
			return Err(StoreError::Limit);
		}
		let _buffer = self
			.buffers
			.clone()
			.try_acquire_many_owned(u32::try_from(n).map_err(|_| StoreError::Limit)?)
			.map_err(|_| StoreError::Limit)?;
		let mut out = Vec::with_capacity(n);
		while out.len() < n {
			let at = offset
				.checked_add(out.len() as u64)
				.ok_or(StoreError::Limit)?;
			let extent = (n - out.len()).min(IO_CHUNK);
			let _clone = self
				.handles
				.clone()
				.try_acquire_owned()
				.map_err(|_| StoreError::Limit)?;
			match self.files.read_plain_at(&h.handle, at, extent).await {
				Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
				Err(e) => return Err(e.into()),
				Ok(b) => {
					require(!b.is_empty() && b.len() <= extent)?;
					out.extend(b)
				}
			}
		}
		Ok(out)
	}
	pub(super) async fn sync(&self, h: &OwnedFile) -> StoreResult<()> {
		let _clone = self
			.handles
			.clone()
			.try_acquire_owned()
			.map_err(|_| StoreError::Limit)?;
		self.files.fsync(&h.handle).await?;
		Ok(())
	}
	pub(super) async fn immutable(&self, path: &Path, b: &[u8]) -> StoreResult<()> {
		let h = match self.open(path, true, true).await {
			Ok(h) => {
				self.write(&h, 0, b).await?;
				h
			}
			Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::AlreadyExists => {
				let h = self.open(path, false, true).await?;
				require(self.adapter.length(&h.handle).await? == b.len() as u64)?;
				let mut work = 0;
				require(self.read(&h, 0, b.len(), &mut work).await? == b)?;
				h
			}
			Err(e) => return Err(e),
		};
		self.sync(&h).await?;
		self.directory_sync(path.parent().ok_or(StoreError::Unavailable)?)
			.await
	}
	pub(super) async fn bounded_file(
		&self,
		path: &Path,
		cap: usize,
		work: &mut u64,
	) -> StoreResult<Vec<u8>> {
		let h = self.open(path, false, false).await?;
		let n = self.adapter.length(&h.handle).await?;
		if n > cap as u64 {
			return Err(StoreError::Limit);
		}
		self.read(&h, 0, n as usize, work).await
	}
}
