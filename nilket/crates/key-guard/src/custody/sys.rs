//! Every system call of the custody modules that std does not wrap. This is the only file in
//! `custody` with `unsafe`, and each block states why it is sound (pinned in `custody::tests`).
use std::ffi::CStr;
use std::fs::File;
use std::io::{Error, Result};
use std::os::fd::{AsRawFd, FromRawFd};

pub(crate) fn euid() -> u32 {
	// SAFETY: geteuid takes no arguments, cannot fail and touches no memory of ours.
	unsafe { libc::geteuid() }
}

fn check(rc: libc::c_int) -> Result<()> {
	if rc == 0 {
		Ok(())
	} else {
		Err(Error::last_os_error())
	}
}

/// `openat` relative to an open directory; the result is close-on-exec.
pub(crate) fn openat(
	dir: &File,
	name: &CStr,
	flags: libc::c_int,
	mode: libc::mode_t,
) -> Result<File> {
	// SAFETY: `dir` is an open descriptor and `name` a NUL-terminated string, both alive for the
	// call; the variadic mode is the `c_uint` openat reads only with `O_CREAT`.
	let fd = unsafe {
		libc::openat(
			dir.as_raw_fd(),
			name.as_ptr(),
			flags | libc::O_CLOEXEC,
			libc::c_uint::from(mode),
		)
	};
	if fd < 0 {
		return Err(Error::last_os_error());
	}
	// SAFETY: `fd` was just returned by openat and is owned by nothing else.
	Ok(unsafe { File::from_raw_fd(fd) })
}

pub(crate) fn mkdirat(dir: &File, name: &CStr, mode: libc::mode_t) -> Result<()> {
	// SAFETY: `dir` is an open descriptor and `name` a NUL-terminated string, alive for the call.
	check(unsafe { libc::mkdirat(dir.as_raw_fd(), name.as_ptr(), mode) })
}

pub(crate) fn unlinkat(dir: &File, name: &CStr) -> Result<()> {
	// SAFETY: `dir` is an open descriptor and `name` a NUL-terminated string, alive for the call.
	check(unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) })
}

/// `renameat2(RENAME_NOREPLACE)`. EINVAL (unsupported by the file system) is returned as is; the
/// caller refuses and never falls back to a plain rename.
pub(crate) fn rename_noreplace(
	from_dir: &File,
	from: &CStr,
	to_dir: &File,
	to: &CStr,
) -> Result<()> {
	// SAFETY: two open directory descriptors and two NUL-terminated names, alive for the call.
	check(unsafe {
		libc::renameat2(
			from_dir.as_raw_fd(),
			from.as_ptr(),
			to_dir.as_raw_fd(),
			to.as_ptr(),
			libc::RENAME_NOREPLACE,
		)
	})
}

pub(crate) fn mlock(bytes: &[u8]) -> Result<()> {
	// SAFETY: the range is a live borrowed slice; mlock only changes its pages' residency.
	check(unsafe { libc::mlock(bytes.as_ptr().cast(), bytes.len()) })
}

pub(crate) fn munlock(bytes: &[u8]) {
	// SAFETY: the range is a live borrowed slice; munlock only changes its pages' residency.
	// Best effort: a range that was never locked is not an error worth reporting.
	let _ = unsafe { libc::munlock(bytes.as_ptr().cast(), bytes.len()) };
}

pub(crate) fn isatty(file: &File) -> bool {
	// SAFETY: isatty reads only the descriptor number of an open file.
	unsafe { libc::isatty(file.as_raw_fd()) == 1 }
}

pub(crate) fn tcgetattr(file: &File) -> Result<libc::termios> {
	let mut termios = std::mem::MaybeUninit::<libc::termios>::uninit();
	// SAFETY: the pointer is to writable storage of the exact type tcgetattr fills.
	check(unsafe { libc::tcgetattr(file.as_raw_fd(), termios.as_mut_ptr()) })?;
	// SAFETY: tcgetattr returned 0, so it initialised every field.
	Ok(unsafe { termios.assume_init() })
}

/// Sets the terminal, discarding unread input (`TCSAFLUSH`).
pub(crate) fn tcsetattr(file: &File, termios: &libc::termios) -> Result<()> {
	// SAFETY: the pointer is to an initialised termios, alive and only read for the call.
	check(unsafe { libc::tcsetattr(file.as_raw_fd(), libc::TCSAFLUSH, termios) })
}

pub(crate) fn set_not_dumpable() -> Result<()> {
	// SAFETY: PR_SET_DUMPABLE takes one integer argument and no pointers.
	check(unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0 as libc::c_ulong) })
}

pub(crate) fn dumpable() -> Result<bool> {
	// SAFETY: PR_GET_DUMPABLE takes no further arguments and returns the flag or -1.
	let rc = unsafe { libc::prctl(libc::PR_GET_DUMPABLE) };
	if rc < 0 {
		return Err(Error::last_os_error());
	}
	Ok(rc != 0)
}

pub(crate) fn set_core_limit_zero() -> Result<()> {
	let zero = libc::rlimit {
		rlim_cur: 0,
		rlim_max: 0,
	};
	// SAFETY: the pointer is to an initialised rlimit, alive and only read for the call.
	check(unsafe { libc::setrlimit(libc::RLIMIT_CORE, &zero) })
}

pub(crate) fn core_limit() -> Result<(u64, u64)> {
	let mut limit = libc::rlimit {
		rlim_cur: 0,
		rlim_max: 0,
	};
	// SAFETY: the pointer is to writable storage of the exact type getrlimit fills.
	check(unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limit) })?;
	Ok((limit.rlim_cur, limit.rlim_max))
}

/// Every `getifaddrs` entry as `(interface name, flags)`; names repeat once per address.
pub(crate) fn interfaces() -> Result<Vec<(String, u32)>> {
	let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
	// SAFETY: the pointer is to writable storage for the list head getifaddrs allocates.
	check(unsafe { libc::getifaddrs(&mut head) })?;
	let mut out = Vec::new();
	let mut cursor = head;
	while !cursor.is_null() {
		// SAFETY: `cursor` is a node of the list getifaddrs returned, live until freeifaddrs.
		let entry = unsafe { &*cursor };
		if !entry.ifa_name.is_null() {
			// SAFETY: a non-null ifa_name is a NUL-terminated string owned by the same list.
			let name = unsafe { CStr::from_ptr(entry.ifa_name) };
			out.push((name.to_string_lossy().into_owned(), entry.ifa_flags));
		}
		cursor = entry.ifa_next;
	}
	// SAFETY: `head` came from a successful getifaddrs, is freed once, and no reference into the
	// list outlives this call (names were copied).
	unsafe { libc::freeifaddrs(head) };
	Ok(out)
}

/// A new pseudo-terminal pair `(master, slave)` for the terminal tests.
#[cfg(test)]
pub(crate) fn open_pty() -> Result<(File, File)> {
	use std::os::unix::ffi::OsStrExt;
	use std::os::unix::fs::OpenOptionsExt;
	// SAFETY: posix_openpt takes flags only and returns a new descriptor or -1.
	let fd = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) };
	if fd < 0 {
		return Err(Error::last_os_error());
	}
	// SAFETY: `fd` was just returned by posix_openpt and is owned by nothing else.
	let master = unsafe { File::from_raw_fd(fd) };
	// SAFETY: grantpt takes the open master descriptor only.
	check(unsafe { libc::grantpt(master.as_raw_fd()) })?;
	// SAFETY: unlockpt takes the open master descriptor only.
	check(unsafe { libc::unlockpt(master.as_raw_fd()) })?;
	let mut name = [0 as libc::c_char; 128];
	// SAFETY: the buffer is writable for the length passed.
	let rc = unsafe { libc::ptsname_r(master.as_raw_fd(), name.as_mut_ptr(), name.len()) };
	if rc != 0 {
		return Err(Error::from_raw_os_error(rc));
	}
	// SAFETY: ptsname_r succeeded, so `name` holds a NUL-terminated string within the buffer.
	let path = unsafe { CStr::from_ptr(name.as_ptr()) };
	let slave = std::fs::OpenOptions::new()
		.read(true)
		.write(true)
		.custom_flags(libc::O_NOCTTY)
		.open(std::ffi::OsStr::from_bytes(path.to_bytes()))?;
	Ok((master, slave))
}
