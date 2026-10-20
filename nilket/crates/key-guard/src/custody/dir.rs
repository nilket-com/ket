//! K2: the custody directory and its files.
//!
//! The directory is opened `O_DIRECTORY|O_NOFOLLOW`, judged on its handle (a directory owned by
//! the effective user, mode exactly 0700), and its ancestry is walked by `openat(fd, "..")` up to
//! `/`: every ancestor must be owned by root or the effective user and be neither group- nor
//! other-writable. Files are one name component resolved through the directory handle, opened
//! `O_NOFOLLOW|O_NONBLOCK` (a FIFO never blocks) and judged on their handle: a regular file owned
//! by the effective user, mode exactly 0600, one link, and a length the caller admits. Nothing is
//! ever repaired (G1).
//!
//! Mutations exist only on [`CustodyLock`], an exclusive `flock` on the directory handle. A file
//! is created as an `O_EXCL` 0600 temp file, fsynced, published by `renameat2(RENAME_NOREPLACE)`
//! and the directory fsynced, so a name holds at most one file for its life and is never
//! overwritten (G2). EINVAL from `renameat2` refuses; there is no plain-rename fallback.
use crate::custody::sys;
use crate::custody::{CustodyError, Refusal};
use std::ffi::CString;
use std::fs::{File, Metadata, OpenOptions, Permissions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(test)]
mod tests;

/// The prefix of this module's temp files; never an admitted name.
const TEMP_PREFIX: &str = ".wrolekey.tmp.";
/// Ancestry walks deeper than this refuse rather than loop.
const MAX_DEPTH: usize = 4096;
static TEMPS: AtomicU64 = AtomicU64::new(0);

/// Where the ancestor walk ends. Production code can only name `/`; a fixture anchor exists only
/// under `cfg(test)` and is unreachable from the public API (pinned in `custody::tests`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stop(Option<(u64, u64)>);

impl Stop {
	const ROOT: Stop = Stop(None);

	#[cfg(test)]
	fn at(anchor: (u64, u64)) -> Stop {
		Stop(Some(anchor))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
	Directory,
	Regular,
	Other,
}

/// What the checks read from one `fstat`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Facts {
	pub(crate) kind: Kind,
	pub(crate) uid: u32,
	pub(crate) mode: u32,
	pub(crate) nlink: u64,
	pub(crate) size: u64,
	pub(crate) dev: u64,
	pub(crate) ino: u64,
}

impl Facts {
	pub(crate) fn of(meta: &Metadata) -> Self {
		let file_type = meta.file_type();
		let kind = if file_type.is_dir() {
			Kind::Directory
		} else if file_type.is_file() {
			Kind::Regular
		} else {
			Kind::Other
		};
		Self {
			kind,
			uid: meta.uid(),
			mode: meta.mode() & 0o7777,
			nlink: meta.nlink(),
			size: meta.size(),
			dev: meta.dev(),
			ino: meta.ino(),
		}
	}
	fn identity(&self) -> (u64, u64) {
		(self.dev, self.ino)
	}
}

pub(crate) fn judge_dir(facts: &Facts, euid: u32) -> Result<(), Refusal> {
	if facts.kind != Kind::Directory {
		return Err(Refusal::NotDirectory);
	}
	if facts.uid != euid {
		return Err(Refusal::Owner { uid: facts.uid });
	}
	if facts.mode != 0o700 {
		return Err(Refusal::DirectoryMode { mode: facts.mode });
	}
	Ok(())
}

pub(crate) fn judge_ancestor(facts: &Facts, euid: u32) -> Result<(), Refusal> {
	if facts.uid != 0 && facts.uid != euid {
		return Err(Refusal::AncestorOwner {
			dev: facts.dev,
			ino: facts.ino,
			uid: facts.uid,
		});
	}
	if facts.mode & 0o022 != 0 {
		return Err(Refusal::AncestorWritable {
			dev: facts.dev,
			ino: facts.ino,
			mode: facts.mode,
		});
	}
	Ok(())
}

/// `admitted` of `None` is the log's exemption from the exact-length rule, and nothing else.
pub(crate) fn judge_file(
	facts: &Facts,
	euid: u32,
	admitted: Option<&[u64]>,
) -> Result<(), Refusal> {
	if facts.kind != Kind::Regular {
		return Err(Refusal::NotRegular);
	}
	if facts.uid != euid {
		return Err(Refusal::Owner { uid: facts.uid });
	}
	if facts.mode != 0o600 {
		return Err(Refusal::FileMode { mode: facts.mode });
	}
	if facts.nlink != 1 {
		return Err(Refusal::LinkCount { nlink: facts.nlink });
	}
	if let Some(admitted) = admitted
		&& !admitted.contains(&facts.size)
	{
		return Err(Refusal::Size { len: facts.size });
	}
	Ok(())
}

fn refused(what: &str, reason: Refusal) -> CustodyError {
	CustodyError::Refused {
		what: what.to_string(),
		reason,
	}
}

/// Maps an open failure: a symlink under `O_NOFOLLOW` and a non-directory under `O_DIRECTORY`
/// are refusals; anything else is I/O.
fn classify(what: &str, error: std::io::Error) -> CustodyError {
	match error.raw_os_error() {
		Some(libc::ELOOP) => refused(what, Refusal::Symlink),
		Some(libc::ENOTDIR) => refused(what, Refusal::NotDirectory),
		_ => CustodyError::io(what)(error),
	}
}

/// Types an ENOTDIR from an `O_DIRECTORY|O_NOFOLLOW` open: Linux reports a symlink that way too.
/// The name is re-probed `O_PATH|O_NOFOLLOW` only to label the refusal; nothing is admitted by it.
fn not_a_directory(what: &str, error: std::io::Error, link: std::io::Result<File>) -> CustodyError {
	if error.raw_os_error() != Some(libc::ENOTDIR) {
		return classify(what, error);
	}
	let symlink = link
		.and_then(|l| l.metadata())
		.is_ok_and(|m| m.file_type().is_symlink());
	refused(
		what,
		if symlink {
			Refusal::Symlink
		} else {
			Refusal::NotDirectory
		},
	)
}

fn facts(file: &File, what: &str) -> Result<Facts, CustodyError> {
	file.metadata()
		.map(|meta| Facts::of(&meta))
		.map_err(CustodyError::io(format!("{what}: fstat")))
}

/// One plain component: not empty, not `.` or `..`, no `/` or NUL, not a temp name.
fn component(name: &str, what: &str) -> Result<CString, CustodyError> {
	let plain = !name.is_empty()
		&& name != "."
		&& name != ".."
		&& name.len() <= 255
		&& !name.contains('/')
		&& !name.starts_with(TEMP_PREFIX);
	match CString::new(name) {
		Ok(c) if plain => Ok(c),
		_ => Err(refused(what, Refusal::Name)),
	}
}

/// Walks from `dir` to `/` (or the test anchor) by `..`, judging every ancestor. Returns how many
/// ancestors were judged.
fn walk(dir: &File, stop: Stop, what: &str) -> Result<usize, CustodyError> {
	let euid = sys::euid();
	let mut current = dir
		.try_clone()
		.map_err(CustodyError::io(format!("{what}: ancestry")))?;
	let mut here = facts(&current, what)?.identity();
	for judged in 0..MAX_DEPTH {
		let up = sys::openat(
			&current,
			c"..",
			// O_PATH needs only search permission, so an execute-only ancestor (a 0711 /home) is
			// judged on its fstat instead of failing the open; fstat works on an O_PATH handle.
			libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW,
			0,
		)
		.map_err(CustodyError::io(format!("{what}: ancestry")))?;
		let up_facts = facts(&up, what)?;
		if up_facts.identity() == here || stop.0 == Some(up_facts.identity()) {
			return Ok(judged);
		}
		judge_ancestor(&up_facts, euid).map_err(|reason| refused(what, reason))?;
		here = up_facts.identity();
		current = up;
	}
	Err(refused(what, Refusal::AncestryTooDeep))
}

/// Opens and admits the custody directory at `path` (K2), or refuses.
pub fn open_custody_dir(path: &Path) -> Result<CustodyDir, CustodyError> {
	open_dir_path(path, Stop::ROOT)
}

fn open_dir_path(path: &Path, stop: Stop) -> Result<CustodyDir, CustodyError> {
	let what = path.display().to_string();
	let dir = OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
		.open(path)
		.map_err(|e| {
			let link = OpenOptions::new()
				.read(true)
				.custom_flags(libc::O_PATH | libc::O_NOFOLLOW)
				.open(path);
			not_a_directory(&what, e, link)
		})?;
	CustodyDir::admit(dir, what, stop)
}

/// An admitted custody directory, held open; everything in it is resolved through this handle.
#[derive(Debug)]
pub struct CustodyDir {
	dir: File,
	what: String,
	identity: (u64, u64),
	stop: Stop,
	/// Whether this handle holds its lock. flock is per open file description, so a second
	/// `lock` on the same handle would otherwise succeed and either guard would unlock both.
	held: AtomicBool,
}

impl CustodyDir {
	fn admit(dir: File, what: String, stop: Stop) -> Result<Self, CustodyError> {
		let own = facts(&dir, &what)?;
		judge_dir(&own, sys::euid()).map_err(|reason| refused(&what, reason))?;
		walk(&dir, stop, &what)?;
		Ok(Self {
			dir,
			what,
			identity: own.identity(),
			stop,
			held: AtomicBool::new(false),
		})
	}

	/// The directory's `(st_dev, st_ino)`, from the handle that was checked.
	pub fn identity(&self) -> (u64, u64) {
		self.identity
	}

	fn child(&self, name: &str) -> String {
		format!("{}/{name}", self.what)
	}

	fn open_checked(
		&self,
		name: &str,
		flags: libc::c_int,
		admitted: Option<&[u64]>,
	) -> Result<File, CustodyError> {
		let what = self.child(name);
		let c_name = component(name, &what)?;
		let file = sys::openat(
			&self.dir,
			&c_name,
			flags | libc::O_NOFOLLOW | libc::O_NONBLOCK,
			0,
		)
		.map_err(|e| classify(&what, e))?;
		judge_file(&facts(&file, &what)?, sys::euid(), admitted)
			.map_err(|reason| refused(&what, reason))?;
		Ok(file)
	}

	/// Opens `name` read-only if it is an admitted custody file whose length is one of
	/// `admitted_len` (G3).
	pub fn open_file(&self, name: &str, admitted_len: &[u64]) -> Result<File, CustodyError> {
		self.open_checked(name, libc::O_RDONLY, Some(admitted_len))
	}

	/// Opens and admits the subdirectory `name` (for example `retired` or `pending`) with the
	/// same directory checks and a fresh ancestor walk.
	pub fn open_subdir(&self, name: &str) -> Result<CustodyDir, CustodyError> {
		let what = self.child(name);
		let c_name = component(name, &what)?;
		let dir = sys::openat(
			&self.dir,
			&c_name,
			libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
			0,
		)
		.map_err(|e| {
			let link = sys::openat(&self.dir, &c_name, libc::O_PATH | libc::O_NOFOLLOW, 0);
			not_a_directory(&what, e, link)
		})?;
		Self::admit(dir, what, self.stop)
	}

	/// Takes the exclusive lock every mutation needs. Another holder, including an earlier guard on
	/// this same handle, makes this `Busy`; it never waits.
	pub fn lock(&self) -> Result<CustodyLock<'_>, CustodyError> {
		if self.held.swap(true, Ordering::AcqRel) {
			return Err(CustodyError::Busy);
		}
		let taken = match self.dir.try_lock() {
			Ok(()) => return Ok(CustodyLock { dir: self }),
			Err(std::fs::TryLockError::WouldBlock) => CustodyError::Busy,
			Err(std::fs::TryLockError::Error(e)) => {
				CustodyError::io(format!("{}: lock", self.what))(e)
			}
		};
		self.held.store(false, Ordering::Release);
		Err(taken)
	}
}

/// The exclusive lock on a custody directory. Every mutating function lives here.
#[derive(Debug)]
pub struct CustodyLock<'a> {
	dir: &'a CustodyDir,
}

impl Drop for CustodyLock<'_> {
	fn drop(&mut self) {
		let _ = self.dir.dir.unlock();
		self.dir.held.store(false, Ordering::Release);
	}
}

impl CustodyLock<'_> {
	fn sync_dir(&self, dir: &CustodyDir) -> Result<(), CustodyError> {
		dir.dir
			.sync_all()
			.map_err(CustodyError::io(format!("{}: directory fsync", dir.what)))
	}

	/// Publishes `bytes` under the new name `name`, whole or not at all (G2).
	pub fn create_file(&self, name: &str, bytes: &[u8]) -> Result<(), CustodyError> {
		let dir = self.dir;
		let what = dir.child(name);
		let target = component(name, &what)?;
		let temp = CString::new(format!(
			"{TEMP_PREFIX}{}.{}",
			std::process::id(),
			TEMPS.fetch_add(1, Ordering::Relaxed)
		))
		.expect("no NUL in a formatted name");
		let mut file = sys::openat(
			&dir.dir,
			&temp,
			libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW,
			0o600,
		)
		.map_err(CustodyError::io(format!("{what}: temp file")))?;
		// The temp file is this call's own new file: setting its mode is creation, not repair.
		let written = file
			.set_permissions(Permissions::from_mode(0o600))
			.and_then(|()| file.write_all(bytes))
			.and_then(|()| file.sync_all());
		drop(file);
		if let Err(e) = written {
			let _ = sys::unlinkat(&dir.dir, &temp);
			return Err(CustodyError::io(format!("{what}: write"))(e));
		}
		if let Err(e) = sys::rename_noreplace(&dir.dir, &temp, &dir.dir, &target) {
			let _ = sys::unlinkat(&dir.dir, &temp);
			return Err(match e.raw_os_error() {
				Some(libc::EEXIST) => CustodyError::Exists { name: what },
				Some(libc::EINVAL) => CustodyError::NoReplaceUnsupported,
				_ => CustodyError::io(format!("{what}: publish"))(e),
			});
		}
		self.sync_dir(dir)
	}

	/// Creates the subdirectory `name` with mode 0700 and admits it. A umask that leaves another
	/// mode refuses (G1).
	pub fn create_subdir(&self, name: &str) -> Result<CustodyDir, CustodyError> {
		let what = self.dir.child(name);
		let c_name = component(name, &what)?;
		sys::mkdirat(&self.dir.dir, &c_name, 0o700).map_err(|e| match e.kind() {
			ErrorKind::AlreadyExists => CustodyError::Exists { name: what.clone() },
			_ => CustodyError::io(format!("{what}: mkdir"))(e),
		})?;
		self.sync_dir(self.dir)?;
		self.dir.open_subdir(name)
	}

	/// Opens the append-only log `name`, creating it empty first if it is absent. The same checks
	/// as any custody file apply, except the exact length (K2).
	pub fn open_log(&self, name: &str) -> Result<File, CustodyError> {
		let flags = libc::O_RDWR | libc::O_APPEND;
		match self.dir.open_checked(name, flags, None) {
			Err(CustodyError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {
				self.create_file(name, &[])?;
				self.dir.open_checked(name, flags, None)
			}
			other => other,
		}
	}

	/// Moves the custody file `name` to `to_name` in `to` (this directory or one of its
	/// subdirectories), never replacing anything there.
	pub fn move_file(
		&self,
		name: &str,
		to: &CustodyDir,
		to_name: &str,
	) -> Result<(), CustodyError> {
		let from = self.dir;
		self.dir.open_checked(name, libc::O_RDONLY, None)?;
		let what = from.child(name);
		let to_what = to.child(to_name);
		let source = component(name, &what)?;
		let target = component(to_name, &to_what)?;
		sys::rename_noreplace(&from.dir, &source, &to.dir, &target).map_err(|e| {
			match e.raw_os_error() {
				Some(libc::EEXIST) => CustodyError::Exists { name: to_what },
				Some(libc::EINVAL) => CustodyError::NoReplaceUnsupported,
				_ => CustodyError::io(format!("{what}: move"))(e),
			}
		})?;
		self.sync_dir(to)?;
		self.sync_dir(from)
	}

	/// Overwrites the custody file `name` with zeros, fsyncs it, and unlinks it if the name still
	/// holds the file that was overwritten. Hygiene only, not secure erasure (K6).
	pub fn destroy_file(&self, name: &str, admitted_len: &[u64]) -> Result<(), CustodyError> {
		let dir = self.dir;
		let what = dir.child(name);
		let mut file = dir.open_checked(name, libc::O_WRONLY, Some(admitted_len))?;
		let overwritten = facts(&file, &what)?;
		let zeros = vec![0_u8; usize::try_from(overwritten.size).unwrap_or(usize::MAX)];
		file.write_all(&zeros)
			.and_then(|()| file.sync_all())
			.map_err(CustodyError::io(format!("{what}: overwrite")))?;
		drop(file);
		let c_name = component(name, &what)?;
		let now = sys::openat(&dir.dir, &c_name, libc::O_PATH | libc::O_NOFOLLOW, 0)
			.map_err(|e| classify(&what, e))?;
		if facts(&now, &what)?.identity() != overwritten.identity() {
			return Err(CustodyError::io(format!("{what}: unlink"))(
				std::io::Error::other("the name no longer holds the overwritten file"),
			));
		}
		sys::unlinkat(&dir.dir, &c_name).map_err(CustodyError::io(format!("{what}: unlink")))?;
		self.sync_dir(dir)
	}
}
