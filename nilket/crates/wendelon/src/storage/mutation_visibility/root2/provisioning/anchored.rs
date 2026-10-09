use std::ffi::CStr;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

/// Directory everything is resolved beneath. Its own path and ancestors are trusted to the
/// operator, as the configuration file itself is.
pub(in crate::storage::mutation_visibility::root2) struct AnchorDir2 {
	dir: File,
}
/// A regular file opened beneath an anchor; the descriptor is owned here and closed on drop.
pub(in crate::storage::mutation_visibility::root2) struct AnchoredFile2 {
	file: File,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum AnchoredError2 {
	Unsupported,
	Escape,
	Symlink,
	NotFound,
	Permission,
	NotRegular,
	TooLarge,
	InvalidLimit,
	Io(i32),
}

// Mirror of the kernel's `struct open_how` at its first published size (linux/openat2.h,
// OPEN_HOW_SIZE_VER0 = 24): three u64 fields. libc's own type is #[non_exhaustive] and cannot be
// built without unsafe zeroing; the layout is fixed by the kernel ABI and pinned below.
#[repr(C)]
struct OpenHow {
	flags: u64,
	mode: u64,
	resolve: u64,
}
const _: () = assert!(std::mem::size_of::<OpenHow>() == 24 && std::mem::align_of::<OpenHow>() == 8);
const _: () = assert!(std::mem::size_of::<OpenHow>() == std::mem::size_of::<libc::open_how>());

fn classify(errno: i32) -> AnchoredError2 {
	match errno {
		libc::ENOSYS | libc::EINVAL | libc::E2BIG => AnchoredError2::Unsupported,
		libc::EXDEV => AnchoredError2::Escape,
		libc::ELOOP => AnchoredError2::Symlink,
		libc::ENOENT | libc::ENOTDIR => AnchoredError2::NotFound,
		libc::EACCES | libc::EPERM => AnchoredError2::Permission,
		other => AnchoredError2::Io(other),
	}
}
fn io(e: std::io::Error) -> AnchoredError2 {
	classify(e.raw_os_error().unwrap_or(0))
}

#[cfg(test)]
thread_local! {
	// Test-only: forces the next open to fail with this errno before any system call is made.
	pub(super) static INJECTED_OPEN_ERRNO: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

// Test-only: consumes a forced errno, failing before any system call; never yields a descriptor.
#[cfg(test)]
fn injected_open_error() -> Result<(), AnchoredError2> {
	match INJECTED_OPEN_ERRNO.with(|c| c.take()) {
		Some(errno) => Err(classify(errno)),
		None => Ok(()),
	}
}
impl AnchorDir2 {
	pub(in crate::storage::mutation_visibility::root2) fn open(
		path: &Path,
	) -> Result<Self, AnchoredError2> {
		let dir = OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC)
			.open(path)
			.map_err(io)?;
		Ok(Self { dir })
	}
}
impl AnchoredFile2 {
	/// Opens `relative` beneath `dir`. The kernel refuses any symlink, magic link, mount crossing
	/// or escape during resolution; anything but a regular file is then refused.
	pub(in crate::storage::mutation_visibility::root2) fn open(
		dir: &AnchorDir2,
		relative: &CStr,
	) -> Result<Self, AnchoredError2> {
		#[cfg(test)]
		injected_open_error()?;
		// O_NONBLOCK keeps a FIFO from blocking the open; it has no effect on the regular files
		// that are the only ones admitted below.
		let how = OpenHow {
			flags: (libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW) as u64,
			mode: 0,
			resolve: libc::RESOLVE_BENEATH
				| libc::RESOLVE_NO_SYMLINKS
				| libc::RESOLVE_NO_MAGICLINKS
				| libc::RESOLVE_NO_XDEV,
		};
		// SAFETY: every argument is valid for the duration of the call: the directory descriptor
		// is owned by `dir`, borrowed for this call; `relative` is a NUL-terminated string borrowed
		// for this call; `how` is a fully initialised kernel-layout `open_how` on this stack frame,
		// and its exact size is passed. The kernel keeps none of these pointers after returning.
		let r = unsafe {
			libc::syscall(
				libc::SYS_openat2,
				dir.dir.as_raw_fd(),
				relative.as_ptr(),
				&how as *const OpenHow,
				std::mem::size_of::<OpenHow>(),
			)
		};
		if r < 0 {
			return Err(io(std::io::Error::last_os_error()));
		}
		// A successful openat2 returns a descriptor no larger than INT_MAX on supported Linux ABIs.
		let fd: RawFd = libc::c_int::try_from(r).map_err(|_| AnchoredError2::Io(0))?;
		// SAFETY: `fd` was returned by the successful openat2 just above, on this thread, and has
		// not been stored, copied or handed anywhere; this transfer makes `OwnedFd` its only owner,
		// which closes it exactly once on drop.
		let owned = unsafe { OwnedFd::from_raw_fd(fd) };
		let file = File::from(owned);
		if !file.metadata().map_err(io)?.is_file() {
			return Err(AnchoredError2::NotRegular);
		}
		Ok(Self { file })
	}
	/// Reads the whole file if it holds at most `max` bytes. The buffer is one exact allocation
	/// of `max + 1` bytes and never grows; at most `max + 1` bytes are read. Allocator overhead
	/// is not covered.
	pub(in crate::storage::mutation_visibility::root2) fn read_bounded(
		self,
		max: usize,
	) -> Result<Vec<u8>, AnchoredError2> {
		let limit = max
			.checked_add(1)
			.filter(|&l| l <= isize::MAX as usize)
			.ok_or(AnchoredError2::InvalidLimit)?;
		let mut buf = Vec::new();
		buf.try_reserve_exact(limit)
			.map_err(|_| AnchoredError2::Io(libc::ENOMEM))?;
		buf.resize(limit, 0);
		let mut n = 0;
		while n < limit {
			match (&self.file).read(&mut buf[n..]) {
				Ok(0) => break,
				Ok(k) => n += k,
				Err(e) if e.kind() == ErrorKind::Interrupted => continue,
				Err(e) => return Err(AnchoredError2::Io(e.raw_os_error().unwrap_or(0))),
			}
		}
		if n > max {
			return Err(AnchoredError2::TooLarge);
		}
		buf.truncate(n);
		Ok(buf)
	}
}
