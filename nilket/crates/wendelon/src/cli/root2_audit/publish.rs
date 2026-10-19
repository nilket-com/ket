//! The context export's file: beside, never inside, the audited data directory, and published
//! whole or not at all.
//!
//! Before the fold, the data directory and the output's parent are opened as directories and the
//! parent's ancestry is walked by `..` from its descriptor up to `/`; any `(st_dev, st_ino)` equal
//! to the data directory's refuses, so a symlinked, aliased or bind-mounted parent that resolves
//! into the data directory is caught by identity, not by spelling. The output name is one path
//! component, resolved only through the parent descriptor, and must not exist yet. After a complete
//! fold the bytes go to a fresh `.wrctxexp.tmp.` file created `O_EXCL|O_NOFOLLOW` with mode 0600
//! in that directory, are fsynced, published by `renameat2(RENAME_NOREPLACE)` (never a plain
//! rename), and the directory is fsynced. Any failure removes what this run created.
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::{Error, ErrorKind, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A checked output: the open parent directory, the single-component name, and the data
/// directory's path and identity, both re-checked when publishing.
pub(super) struct Target {
	parent: File,
	name: CString,
	out: PathBuf,
	data_dir: PathBuf,
	data: (u64, u64),
}
/// The prefix of this tool's temp files; never a published name.
const TEMP_PREFIX: &str = ".wrctxexp.tmp.";

/// Ancestry walks deeper than this refuse rather than loop.
const MAX_DEPTH: usize = 4096;
static TEMPS: AtomicU64 = AtomicU64::new(0);

fn open_dir(path: &Path) -> Result<File, Error> {
	std::fs::OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
		.open(path)
}
fn identity(dir: &File) -> Result<(u64, u64), Error> {
	let meta = dir.metadata()?;
	Ok((meta.dev(), meta.ino()))
}
fn openat(dir: &File, name: &CStr, flags: libc::c_int, mode: libc::mode_t) -> Result<File, Error> {
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
/// The `(st_dev, st_ino)` of `name` in `dir` itself, never following a symlink: the name is
/// opened `O_PATH|O_NOFOLLOW` (a symlink opens as itself) and fstatted through std.
fn identity_at(dir: &File, name: &CStr) -> Result<(u64, u64), Error> {
	identity(&openat(dir, name, libc::O_PATH | libc::O_NOFOLLOW, 0)?)
}
fn unlinkat(dir: &File, name: &CStr) {
	// SAFETY: `dir` is an open descriptor and `name` a NUL-terminated string. Best effort: the
	// caller is already failing and reports that failure.
	let _ = unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) };
}
fn rename_noreplace(dir: &File, from: &CStr, to: &CStr) -> Result<(), Error> {
	// SAFETY: one open directory descriptor and two NUL-terminated names, alive for the call.
	let rc = unsafe {
		libc::renameat2(
			dir.as_raw_fd(),
			from.as_ptr(),
			dir.as_raw_fd(),
			to.as_ptr(),
			libc::RENAME_NOREPLACE,
		)
	};
	if rc != 0 {
		// EINVAL (no RENAME_NOREPLACE on this file system) refuses too; there is no fallback.
		return Err(Error::last_os_error());
	}
	Ok(())
}

/// Refuses when `dir` or any ancestor reached through `..` is the data directory.
fn walk(dir: &File, data: (u64, u64)) -> Result<(), String> {
	let mut current = dir
		.try_clone()
		.map_err(|e| format!("output directory: {e}"))?;
	for _ in 0..MAX_DEPTH {
		let here = identity(&current).map_err(|e| format!("output directory: {e}"))?;
		if here == data {
			return Err("the output directory is inside the data directory".into());
		}
		let up = openat(&current, c"..", libc::O_DIRECTORY | libc::O_RDONLY, 0)
			.map_err(|e| format!("output directory ancestry: {e}"))?;
		if identity(&up).map_err(|e| format!("output directory ancestry: {e}"))? == here {
			return Ok(());
		}
		current = up;
	}
	Err("the output directory's ancestry is too deep".into())
}

/// Checks `out` against `data_dir` before anything is read: the path rule, and that nothing
/// exists at `out` (a dangling symlink included).
pub(super) fn target(data_dir: &Path, out: &Path) -> Result<Target, String> {
	let data = open_dir(data_dir)
		.and_then(|d| identity(&d))
		.map_err(|e| format!("data directory {}: {e}", data_dir.display()))?;
	let raw = out.as_os_str().as_bytes();
	let (parent, name) = match raw.iter().rposition(|b| *b == b'/') {
		Some(0) => (&b"/"[..], &raw[1..]),
		Some(at) => (&raw[..at], &raw[at + 1..]),
		None => (&b"."[..], raw),
	};
	if name.is_empty() || name == b"." || name == b".." {
		return Err(format!(
			"{}: the output name must be one file name component",
			out.display()
		));
	}
	if name.starts_with(TEMP_PREFIX.as_bytes()) {
		return Err(format!(
			"{}: {TEMP_PREFIX} names are the tool's temp files",
			out.display()
		));
	}
	let name = CString::new(name).map_err(|_| "the output name contains NUL".to_string())?;
	let parent_path = Path::new(std::ffi::OsStr::from_bytes(parent));
	let parent = open_dir(parent_path)
		.map_err(|e| format!("output directory {}: {e}", parent_path.display()))?;
	walk(&parent, data)?;
	match openat(&parent, &name, libc::O_PATH | libc::O_NOFOLLOW, 0) {
		Ok(_) => return Err(format!("{}: already exists", out.display())),
		Err(e) if e.kind() == ErrorKind::NotFound => {}
		Err(e) => return Err(format!("{}: {e}", out.display())),
	}
	Ok(Target {
		parent,
		name,
		out: out.to_path_buf(),
		data_dir: data_dir.to_path_buf(),
		data,
	})
}

#[cfg(test)]
thread_local! {
	/// Test-only fault injection: the publish step that fails on this thread.
	static FAIL_AT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}
#[cfg(test)]
pub(super) fn fail_at(step: Option<&'static str>) {
	FAIL_AT.with(|f| f.set(step));
}
fn injected(step: &'static str) -> Result<(), Error> {
	#[cfg(test)]
	if FAIL_AT.with(|f| f.get()) == Some(step) {
		return Err(Error::other(format!("injected {step} failure")));
	}
	let _ = step;
	Ok(())
}

/// The identity of the file one publish created, so a withdrawal removes only that file.
#[derive(Debug)]
pub(super) struct Published((u64, u64));

/// Publishes `bytes` at the checked target, or leaves nothing there and nothing of this run's.
pub(super) fn publish(target: &Target, bytes: &[u8]) -> Result<Published, String> {
	// The audited directory is still the one checked: a swap since the check refuses.
	let now = open_dir(&target.data_dir)
		.and_then(|d| identity(&d))
		.map_err(|e| format!("data directory {}: {e}", target.data_dir.display()))?;
	if now != target.data {
		return Err("the data directory changed since the path check".into());
	}
	walk(&target.parent, target.data)?;
	let temp = CString::new(format!(
		"{TEMP_PREFIX}{}.{}",
		std::process::id(),
		TEMPS.fetch_add(1, Ordering::Relaxed)
	))
	.expect("no NUL in a formatted name");
	let mut file = openat(
		&target.parent,
		&temp,
		libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW,
		0o600,
	)
	.map_err(|e| format!("export temp file: {e}"))?;
	// The temp file's own identity, from its descriptor before the rename publishes it.
	let mut created = None;
	let written = injected("write")
		.and_then(|()| file.write_all(bytes))
		.and_then(|()| injected("sync"))
		.and_then(|()| file.sync_all())
		.and_then(|()| file.metadata())
		.map(|meta| created = Some(Published((meta.dev(), meta.ino()))))
		.and_then(|()| injected("rename"))
		.and_then(|()| rename_noreplace(&target.parent, &temp, &target.name));
	drop(file);
	let (Ok(()), Some(published)) = (written.as_ref(), created) else {
		unlinkat(&target.parent, &temp);
		return Err(format!(
			"export write: {}",
			written
				.err()
				.map_or("no file identity".to_string(), |e| e.to_string())
		));
	};
	// Published; its name is durable only once the directory is.
	if let Err(e) = injected("directory").and_then(|()| target.parent.sync_all()) {
		let left = withdraw(target, &published).err();
		return Err(format!(
			"export directory sync: {e}{}",
			left.map_or(String::new(), |l| format!("; {l}"))
		));
	}
	Ok(published)
}
/// Removes a published export when the run fails after publishing, only if the name still holds
/// the file this run created: its identity is checked first, then the name is unlinked and the
/// directory fsynced (best effort). Anything else there is left in place and named. The caller
/// is already failing and reports that failure either way.
pub(super) fn withdraw(target: &Target, published: &Published) -> Result<(), String> {
	// A swap between this check and the unlink stays possible; the check narrows that window to
	// two calls on the held parent descriptor and does not close it.
	match identity_at(&target.parent, &target.name) {
		Ok(found) if found == published.0 => {
			unlinkat(&target.parent, &target.name);
			let _ = target.parent.sync_all();
			Ok(())
		}
		Ok(_) => Err(format!(
			"{} is no longer the published export; left in place",
			target.out.display()
		)),
		Err(e) => Err(format!(
			"{}: the published export is gone ({e}); nothing withdrawn",
			target.out.display()
		)),
	}
}

#[cfg(test)]
mod tests {
	use super::{publish, target};

	/// The data directory is re-identified at publish: one swapped in since the check refuses,
	/// and nothing is published.
	#[test]
	fn a_data_directory_swapped_after_the_check_refuses_the_publish() {
		let base = crate::test_support::TestDir::new("root2-export-swap");
		let (data, outside) = (base.join("data"), base.join("outside"));
		std::fs::create_dir(&data).unwrap();
		std::fs::create_dir(&outside).unwrap();
		let out = outside.join("context.wrctxexp");
		let checked = target(&data, &out).unwrap();
		// Held open, the removed directory's inode cannot be reused by its replacement.
		let _held = std::fs::File::open(&data).unwrap();
		std::fs::remove_dir(&data).unwrap();
		std::fs::create_dir(&data).unwrap();
		let refused = publish(&checked, b"x").unwrap_err();
		assert!(refused.contains("changed"), "{refused}");
		assert!(std::fs::symlink_metadata(&out).is_err());
		assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0, "no temp");
		// Control: an unchanged directory publishes.
		let same = target(&data, &out).unwrap();
		publish(&same, b"x").unwrap();
		assert_eq!(std::fs::read(&out).unwrap(), b"x");
	}
}
