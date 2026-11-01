//! Startup-only cleanup of interrupted sidecar staging, beneath opened directory
//! descriptors. Never follows a child symlink or treats an arbitrary .tmp as ours.
use crate::storage::file_ops::spawn_blocking_io_owned;
use crate::storage::ownership::DataDirectoryOwner;
use std::ffi::{CStr, CString, OsString};
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;

pub(super) async fn cleanup_secondary_index_tmp_files(
	dir: &Path,
	owner: Option<DataDirectoryOwner>,
) -> io::Result<()> {
	let removed = run_owned_cleanup(dir.to_path_buf(), owner, NoHooks).await?;
	if removed > 0 {
		tracing::warn!(dir = %dir.display(), removed_tmp_files = removed,
			"removed interrupted secondary index staging files during open");
	}
	Ok(())
}

async fn run_owned_cleanup<H: CleanupHooks + Send + 'static>(
	dir: PathBuf,
	owner: Option<DataDirectoryOwner>,
	mut hooks: H,
) -> io::Result<usize> {
	// The existing executor moves both owner and write permit into its job;
	// cancellation of this future cannot release LOCK while unlink still runs.
	spawn_blocking_io_owned(owner, move || {
		// The configured root is operator-trusted (and may itself be an alias).
		// All child traversal after this one root open is descriptor-relative.
		let root = OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
			.open(&dir)?;
		let Some(sidecars) = open_child_dir(&root, c"secondary_index", 0, &mut hooks)? else {
			return Ok(0);
		};
		walk(&sidecars, 0, &mut hooks)
	})
	.await
}

trait CleanupHooks {
	fn before_open(&mut self, _parent: &File, _name: &CStr, _depth: usize) -> io::Result<()> {
		Ok(())
	}
	fn before_unlink(&mut self, _parent: &File, _name: &CStr) -> io::Result<()> {
		Ok(())
	}
	fn before_fsync(&mut self, _parent: &File) -> io::Result<()> {
		Ok(())
	}
}
struct NoHooks;
impl CleanupHooks for NoHooks {}

fn lower_hex(bytes: &[u8], width: usize) -> bool {
	bytes.len() == width
		&& bytes
			.iter()
			.all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
}

fn is_uuid_v4(uuid: &[u8]) -> bool {
	lower_hex(uuid, 32) && uuid[12] == b'4' && matches!(uuid[16], b'8' | b'9' | b'a' | b'b')
}

fn is_sidecar_tmp(name: &[u8]) -> bool {
	let Some(stem) = name.strip_suffix(b".tmp") else {
		return false;
	};
	let Some(dot) = stem.iter().position(|b| *b == b'.') else {
		return false;
	};
	let (slab, rest) = stem.split_at(dot);
	let Some(rest) = rest[1..].strip_prefix(b"idx.") else {
		return false;
	};
	if !lower_hex(slab, 16) {
		return false;
	}
	let pieces = rest.split(|byte| *byte == b'.').collect::<Vec<_>>();
	match pieces.as_slice() {
		[staging] => is_uuid_v4(staging),
		[final_id, staging] => is_uuid_v4(final_id) && is_uuid_v4(staging),
		_ => false,
	}
}

/// Recognize both the legacy final spelling and unique-attempt final names.
fn is_sidecar_final(name: &[u8]) -> bool {
	let Some(dot) = name.iter().position(|byte| *byte == b'.') else {
		return false;
	};
	if !lower_hex(&name[..dot], 16) {
		return false;
	}
	let rest = &name[dot + 1..];
	if rest == b"idx" {
		return true;
	}
	rest.strip_prefix(b"idx.").is_some_and(is_uuid_v4)
}

/// Inventory only recognized, unreferenced finals without following child
/// symlinks. Files are retained; this path does not perform garbage collection.
pub(super) async fn scan_retained_secondary_files(
	dir: &Path,
	owner: Option<DataDirectoryOwner>,
	referenced: std::collections::HashSet<PathBuf>,
) -> io::Result<Vec<(PathBuf, u64)>> {
	let root_path = dir.to_path_buf();
	spawn_blocking_io_owned(owner, move || {
		let root = OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
			.open(root_path)?;
		let Some(sidecars) = open_child_dir(&root, c"secondary_index", 0, &mut NoHooks)? else {
			return Ok(Vec::new());
		};
		let mut files = Vec::new();
		scan_finals(
			&sidecars,
			0,
			&PathBuf::from("secondary_index"),
			&referenced,
			&mut files,
		)?;
		Ok(files)
	})
	.await
}

fn scan_finals(
	dir: &File,
	depth: usize,
	prefix: &Path,
	referenced: &std::collections::HashSet<PathBuf>,
	files: &mut Vec<(PathBuf, u64)>,
) -> io::Result<()> {
	let mut names = DirectoryStream::open(dir)?;
	while let Some(name) = names.next_name()? {
		let bytes = name.as_bytes();
		if depth < 3 || (depth == 3 && lower_hex(bytes, 64)) {
			let width = if depth == 0 {
				16
			} else if depth == 3 {
				64
			} else {
				32
			};
			if !lower_hex(bytes, width) {
				continue;
			}
			let cname = CString::new(bytes).expect("directory entry has no embedded NUL");
			if let Some(child) = open_child_dir(dir, &cname, depth + 1, &mut NoHooks)? {
				scan_finals(&child, depth + 1, &prefix.join(&name), referenced, files)?;
			}
		} else if (depth == 3 || depth == 4) && is_sidecar_final(bytes) {
			let relative = prefix.join(&name);
			if referenced.contains(&relative) {
				continue;
			}
			let cname = CString::new(bytes).expect("directory entry has no embedded NUL");
			let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
			// SAFETY: dir owns its fd and cname is a valid single component.
			if unsafe {
				libc::fstatat(
					dir.as_raw_fd(),
					cname.as_ptr(),
					stat.as_mut_ptr(),
					libc::AT_SYMLINK_NOFOLLOW,
				)
			} < 0
			{
				let error = io::Error::last_os_error();
				if error.kind() == ErrorKind::NotFound {
					continue;
				}
				return Err(error);
			}
			// SAFETY: fstatat succeeded and initialized stat.
			let stat = unsafe { stat.assume_init() };
			if stat.st_mode & libc::S_IFMT == libc::S_IFREG && stat.st_size >= 0 {
				files.push((relative, stat.st_size as u64));
			}
		}
	}
	Ok(())
}

fn ignored_child_error(error: &io::Error) -> bool {
	matches!(
		error.raw_os_error(),
		Some(libc::ENOENT | libc::ENOTDIR | libc::ELOOP)
	)
}

fn open_child_dir<H: CleanupHooks>(
	parent: &File,
	name: &CStr,
	depth: usize,
	hooks: &mut H,
) -> io::Result<Option<File>> {
	hooks.before_open(parent, name, depth)?;
	// SAFETY: parent owns its live fd and name is a single NUL-terminated
	// component; openat retains neither pointer. O_NOFOLLOW forbids aliases.
	let fd = unsafe {
		libc::openat(
			parent.as_raw_fd(),
			name.as_ptr(),
			libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
		)
	};
	if fd < 0 {
		let error = io::Error::last_os_error();
		return if ignored_child_error(&error) {
			Ok(None)
		} else {
			Err(error)
		};
	}
	// SAFETY: successful openat returned a fresh owned fd, transferred once.
	Ok(Some(unsafe { File::from_raw_fd(fd) }))
}

struct DirectoryStream(NonNull<libc::DIR>);
impl DirectoryStream {
	fn open(dir: &File) -> io::Result<Self> {
		let fd = dir.try_clone()?.into_raw_fd();
		// SAFETY: fd is a fresh descriptor with ownership transferred on success.
		let stream = unsafe { libc::fdopendir(fd) };
		match NonNull::new(stream) {
			Some(stream) => Ok(Self(stream)),
			None => {
				let error = io::Error::last_os_error();
				// SAFETY: failed fdopendir did not consume fd; reclaim exactly once.
				drop(unsafe { File::from_raw_fd(fd) });
				Err(error)
			}
		}
	}
	fn next_name(&mut self) -> io::Result<Option<OsString>> {
		// SAFETY: errno is thread-local. Reset immediately before readdir so a
		// null result distinguishes end-of-directory from an actual read error.
		#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
		unsafe {
			*libc::__error() = 0;
		}
		#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
		unsafe {
			*libc::__errno_location() = 0;
		}
		// SAFETY: this owns a live DIR and no other caller accesses that stream.
		let entry = unsafe { libc::readdir(self.0.as_ptr()) };
		if entry.is_null() {
			let error = io::Error::last_os_error();
			return if error.raw_os_error() == Some(0) {
				Ok(None)
			} else {
				Err(error)
			};
		}
		// SAFETY: readdir supplies a NUL-terminated name valid until the next
		// readdir call. Copy before advancing; preserve non-UTF8 without parsing.
		let bytes = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
		Ok(Some(OsString::from_vec(bytes.to_vec())))
	}
}
impl Drop for DirectoryStream {
	fn drop(&mut self) {
		// SAFETY: this is the unique owner; closedir also closes the consumed fd.
		unsafe {
			libc::closedir(self.0.as_ptr());
		}
	}
}

fn regular_identity(parent: &File, name: &CStr) -> io::Result<Option<(libc::dev_t, libc::ino_t)>> {
	let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
	// SAFETY: valid owned parent fd, NUL name and writable stat output; the
	// output is read only after success. No symlink target is resolved.
	if unsafe {
		libc::fstatat(
			parent.as_raw_fd(),
			name.as_ptr(),
			stat.as_mut_ptr(),
			libc::AT_SYMLINK_NOFOLLOW,
		)
	} < 0
	{
		let error = io::Error::last_os_error();
		return if error.kind() == ErrorKind::NotFound {
			Ok(None)
		} else {
			Err(error)
		};
	}
	// SAFETY: successful fstatat fully initialized this output.
	let stat = unsafe { stat.assume_init() };
	Ok((stat.st_mode & libc::S_IFMT == libc::S_IFREG).then_some((stat.st_dev, stat.st_ino)))
}

fn walk<H: CleanupHooks>(dir: &File, depth: usize, hooks: &mut H) -> io::Result<usize> {
	let mut names = DirectoryStream::open(dir)?;
	let mut removed = 0;
	let mut local_removed = 0usize;
	let result = (|| {
		while let Some(name) = names.next_name()? {
			let bytes = name.as_bytes();
			if depth < 3 || (depth == 3 && lower_hex(bytes, 64)) {
				let width = if depth == 0 {
					16
				} else if depth == 3 {
					64
				} else {
					32
				};
				if !lower_hex(bytes, width) {
					continue;
				}
				let name = CString::new(bytes).expect("directory entry has no embedded NUL");
				if let Some(child) = open_child_dir(dir, &name, depth + 1, hooks)? {
					removed += walk(&child, depth + 1, hooks)?;
				}
			} else if (depth == 3 || depth == 4) && is_sidecar_tmp(bytes) {
				let name = CString::new(bytes).expect("directory entry has no embedded NUL");
				let Some(identity) = regular_identity(dir, &name)? else {
					continue;
				};
				hooks.before_unlink(dir, &name)?;
				// A replacement during a gated/tested interruption must not be
				// mistaken for the regular file we recognized. unlinkat itself
				// never follows a last-moment symlink to its target.
				if regular_identity(dir, &name)? != Some(identity) {
					continue;
				}
				// SAFETY: owned parent and single-component NUL name; flags=0
				// unlinks a leaf and cannot recurse or follow symlinks.
				if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) } < 0 {
					let error = io::Error::last_os_error();
					if error.kind() != ErrorKind::NotFound {
						return Err(error);
					}
				} else {
					removed += 1;
					local_removed += 1;
				}
			}
		}
		Ok(())
	})();
	// Sync this leaf directory even if a later unlink/readdir fails after a
	// partial sweep. Ancestors are untouched and need no namespace sync.
	if local_removed > 0 {
		hooks.before_fsync(dir)?;
		dir.sync_all()?;
	}
	result?;
	Ok(removed)
}

#[cfg(test)]
mod tests;
