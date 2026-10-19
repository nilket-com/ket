//! Dormant replica-local store for one recovery mark, encoded only by the G1e-1 codec. A mark is
//! safety state: the highest baseline this replica has durably confirmed. It authenticates no
//! history, target, deployment or administrator, and grants no serving by itself.
//!
//! A baseline is durably confirmed by a store instance only when (a) `open`, holding the lock,
//! read and decoded the mark and then synced the file and its directory, or (b) this instance's own
//! write returned `Durable`. Reads, matching bytes, a reopen, `EEXIST` and elapsed time are not
//! evidence. Any failed or uncertain write poisons the instance: `durable()` is then `None` and
//! every further write is refused, so a poisoned handle can never authorize serving.
//!
//! Path model, as for the owner log: Linux, a local filesystem, one host, one writer per
//! directory; the directory and its ancestors are operator-controlled and not replaced while an
//! operation runs. `O_NOFOLLOW` on a leaf is not anchored resolution. The directory identity
//! captured at `open` is rechecked before every write or retry. Old-or-new outcomes after an
//! interruption assume atomic rename and link and honoured file and directory fsync.
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::recovery::{
	Baseline2, Deployment2, FieldError2, RECOVERY_MARK_BYTES2, RecoveryError2, RecoveryMark2,
	decode_recovery_mark2, encode_recovery_mark2,
};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const LOCK: &str = "LOCK";
const MARK: &str = "RECOVERY_MARK";
const TMP: &str = "RECOVERY_MARK.tmp";
type Bytes = [u8; RECOVERY_MARK_BYTES2];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum MarkStep2 {
	DirOpen,
	LockOpen,
	LockCreate,
	LockCreateDirSync,
	Lock,
	MarkRead,
	MarkTmpStat,
	ConfirmFileSync,
	ConfirmDirSync,
	DirCheck,
	ReRead,
	TmpUnlink,
	TmpUnlinkDirSync,
	TmpCreate,
	TmpWrite,
	TmpSync,
	Link,
	LinkTmpUnlink,
	Rename,
	DirSync,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum MarkOpen2 {
	Confirmed(Baseline2),
	Absent,
	TmpOnly,
	Corrupt(RecoveryError2),
}
redacted_debug!(MarkOpen2, "MarkOpen2");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum MarkWrite2 {
	Durable(Baseline2),
	Unchanged(Baseline2),
}
redacted_debug!(MarkWrite2, "MarkWrite2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum MarkIoError2 {
	Busy,
	NotDirectory,
	InsecureDir,
	NotRegular { step: MarkStep2 },
	Io { step: MarkStep2, errno: Option<i32> },
	Uncertain { step: MarkStep2 },
	Poisoned { step: MarkStep2 },
	DirectoryChanged,
	Interference,
	AlreadyProvisioned,
	NotProvisioned,
	Corrupt,
	Invalid(FieldError2),
	Identity,
	Regressed,
	Conflict,
}
type Owned<T> = Result<T, MarkIoError2>;

/// One locked mark directory. Dropping it releases the lock.
pub(in crate::storage::mutation_visibility::root2) struct MarkStore2 {
	path: PathBuf,
	dir: File,
	id: (u64, u64),
	_lock: File,
	confirmed: Option<(Baseline2, Bytes)>,
	corrupt_at_open: bool,
	poisoned: Option<MarkStep2>,
}

impl Drop for MarkStore2 {
	/// Unlock before the descriptor closes: a forked child shares the open file
	/// description until it execs, so closing alone could keep the lock held and
	/// make an immediate reopen report Busy.
	fn drop(&mut self) {
		let _ = self._lock.unlock();
	}
}
redacted_debug!(MarkStore2, "MarkStore2");

enum Disk {
	Absent,
	Present(Vec<u8>),
}

fn errno(e: &io::Error) -> Option<i32> {
	e.raw_os_error()
}
fn io_err(step: MarkStep2) -> impl Fn(io::Error) -> MarkIoError2 {
	move |e| MarkIoError2::Io {
		step,
		errno: errno(&e),
	}
}
fn run<T>(step: MarkStep2, f: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
	// The step names the operation for the test seam; production code only runs it.
	let _ = step;
	#[cfg(test)]
	seam::before(step)?;
	let value = f()?;
	#[cfg(test)]
	seam::after(step)?;
	Ok(value)
}
// No symlink leaf (ELOOP), no blocking open of a FIFO; callers then require a regular file.
fn open_file(path: &Path, create: bool) -> io::Result<File> {
	let mut o = OpenOptions::new();
	if create {
		o.read(true).write(true).create_new(true).mode(0o600);
	} else {
		o.read(true);
	}
	o.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
	o.open(path)
}
fn regular(file: File, step: MarkStep2) -> Owned<File> {
	if !file.metadata().map_err(io_err(step))?.file_type().is_file() {
		return Err(MarkIoError2::NotRegular { step });
	}
	Ok(file)
}
// One exact allocation of limit + 1 bytes; the reported length is never trusted.
fn bounded(mut file: File, step: MarkStep2) -> Owned<Vec<u8>> {
	let limit = RECOVERY_MARK_BYTES2 + 1;
	let mut buf = vec![0u8; limit];
	let mut n = 0;
	while n < limit {
		match file.read(&mut buf[n..]) {
			Ok(0) => break,
			Ok(k) => n += k,
			Err(e) if e.kind() == ErrorKind::Interrupted => {}
			Err(e) => return Err(io_err(step)(e)),
		}
	}
	buf.truncate(n);
	Ok(buf)
}
fn read_mark(dir: &Path, step: MarkStep2) -> Owned<(Disk, Option<File>)> {
	match run(step, || open_file(&dir.join(MARK), false)) {
		Ok(file) => {
			let file = regular(file, step)?;
			let handle = file.try_clone().map_err(io_err(step))?;
			Ok((Disk::Present(bounded(file, step)?), Some(handle)))
		}
		Err(e) if e.kind() == ErrorKind::NotFound => Ok((Disk::Absent, None)),
		Err(e) => Err(io_err(step)(e)),
	}
}
fn encode(baseline: &Baseline2) -> Owned<Bytes> {
	encode_recovery_mark2(baseline).map_err(|e| match e {
		RecoveryError2::Field(f) => MarkIoError2::Invalid(f),
		_ => MarkIoError2::Corrupt,
	})
}

impl MarkStore2 {
	/// Locks `dir`, classifies its mark and, for a valid mark, confirms it durable by syncing the
	/// file and the directory. A confirmation failure returns no store. Nothing is repaired.
	pub(in crate::storage::mutation_visibility::root2) fn open(
		dir: &Path,
	) -> Owned<(Self, MarkOpen2)> {
		use MarkStep2 as S;
		let path = run(S::DirOpen, || fs::canonicalize(dir)).map_err(io_err(S::DirOpen))?;
		let handle = run(S::DirOpen, || {
			OpenOptions::new()
				.read(true)
				.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
				.open(&path)
		})
		.map_err(|e| match e.raw_os_error() {
			Some(libc::ENOTDIR) => MarkIoError2::NotDirectory,
			_ => io_err(S::DirOpen)(e),
		})?;
		let meta = handle.metadata().map_err(io_err(S::DirOpen))?;
		if !meta.is_dir() {
			return Err(MarkIoError2::NotDirectory);
		}
		if meta.mode() & 0o022 != 0 {
			return Err(MarkIoError2::InsecureDir);
		}
		let lock_path = path.join(LOCK);
		let lock = match run(S::LockOpen, || {
			let mut o = OpenOptions::new();
			o.read(true)
				.write(true)
				.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
			o.open(&lock_path)
		}) {
			Ok(f) => regular(f, S::LockOpen)?,
			Err(e) if e.kind() == ErrorKind::NotFound => {
				let f = run(S::LockCreate, || open_file(&lock_path, true))
					.map_err(io_err(S::LockCreate))?;
				let f = regular(f, S::LockCreate)?;
				if run(S::LockCreateDirSync, || handle.sync_all()).is_err() {
					return Err(MarkIoError2::Uncertain {
						step: S::LockCreateDirSync,
					});
				}
				f
			}
			Err(e) => return Err(io_err(S::LockOpen)(e)),
		};
		run(S::Lock, || Ok(())).map_err(io_err(S::Lock))?;
		match lock.try_lock() {
			Ok(()) => {}
			Err(TryLockError::WouldBlock) => return Err(MarkIoError2::Busy),
			Err(TryLockError::Error(e)) => return Err(io_err(S::Lock)(e)),
		}
		let mut store = Self {
			path,
			dir: handle,
			id: (meta.dev(), meta.ino()),
			_lock: lock,
			confirmed: None,
			corrupt_at_open: false,
			poisoned: None,
		};
		let opened = match read_mark(&store.path, S::MarkRead)? {
			(Disk::Absent, _) => {
				match run(S::MarkTmpStat, || {
					fs::symlink_metadata(store.path.join(TMP))
				}) {
					Ok(_) => MarkOpen2::TmpOnly,
					Err(e) if e.kind() == ErrorKind::NotFound => MarkOpen2::Absent,
					Err(e) => return Err(io_err(S::MarkTmpStat)(e)),
				}
			}
			(Disk::Present(bytes), file) => match decode_recovery_mark2(&bytes) {
				Err(e) => {
					store.corrupt_at_open = true;
					MarkOpen2::Corrupt(e)
				}
				Ok(baseline) => {
					let file = file.expect("a present mark has a handle");
					if run(S::ConfirmFileSync, || file.sync_all()).is_err() {
						return Err(MarkIoError2::Uncertain {
							step: S::ConfirmFileSync,
						});
					}
					if run(S::ConfirmDirSync, || store.dir.sync_all()).is_err() {
						return Err(MarkIoError2::Uncertain {
							step: S::ConfirmDirSync,
						});
					}
					let canonical = bytes.as_slice().try_into().expect("decoded mark length");
					store.confirmed = Some((baseline, canonical));
					MarkOpen2::Confirmed(baseline)
				}
			},
		};
		Ok((store, opened))
	}

	/// The baseline durably confirmed by this instance; `None` once poisoned.
	pub(in crate::storage::mutation_visibility::root2) fn durable(&self) -> Option<Baseline2> {
		match self.poisoned {
			Some(_) => None,
			None => self.confirmed.map(|(b, _)| b),
		}
	}

	/// Owner-provisioned first boot: create-only, never over an existing mark.
	pub(in crate::storage::mutation_visibility::root2) fn provision(
		&mut self,
	) -> Owned<MarkWrite2> {
		if let Some(step) = self.poisoned {
			return Err(MarkIoError2::Poisoned { step });
		}
		match self.confirmed {
			Some((Baseline2::Mark(_), _)) => return Err(MarkIoError2::AlreadyProvisioned),
			None if self.corrupt_at_open => return Err(MarkIoError2::AlreadyProvisioned),
			_ => {}
		}
		let bytes = encode(&Baseline2::Provisioned)?;
		self.check_disk()?;
		if self.confirmed.is_some() {
			return Ok(MarkWrite2::Unchanged(Baseline2::Provisioned));
		}
		self.write_scratch(&bytes)?;
		let (tmp, mark) = (self.path.join(TMP), self.path.join(MARK));
		self.uncertain(
			MarkStep2::Link,
			run(MarkStep2::Link, || fs::hard_link(&tmp, &mark)),
		)?;
		let r = run(MarkStep2::LinkTmpUnlink, || fs::remove_file(&tmp));
		self.uncertain(MarkStep2::LinkTmpUnlink, r)?;
		let r = run(MarkStep2::DirSync, || self.dir.sync_all());
		self.uncertain(MarkStep2::DirSync, r)?;
		self.confirmed = Some((Baseline2::Provisioned, bytes));
		Ok(MarkWrite2::Durable(Baseline2::Provisioned))
	}

	/// Monotone replacement of the confirmed baseline by `new`, for this `deployment`.
	pub(in crate::storage::mutation_visibility::root2) fn advance(
		&mut self,
		deployment: &Deployment2,
		new: &RecoveryMark2,
	) -> Owned<MarkWrite2> {
		if let Some(step) = self.poisoned {
			return Err(MarkIoError2::Poisoned { step });
		}
		let Some((current, _)) = self.confirmed else {
			return Err(if self.corrupt_at_open {
				MarkIoError2::Corrupt
			} else {
				MarkIoError2::NotProvisioned
			});
		};
		let bytes = encode(&Baseline2::Mark(*new))?;
		if (new.generation, new.cluster) != (deployment.generation, deployment.cluster) {
			return Err(MarkIoError2::Identity);
		}
		let mut retry = false;
		if let Baseline2::Mark(m) = current {
			if (m.generation, m.cluster) != (new.generation, new.cluster) {
				return Err(MarkIoError2::Identity);
			}
			if new.cut.index < m.cut.index {
				return Err(MarkIoError2::Regressed);
			}
			if new.cut.index == m.cut.index {
				if *new != m {
					return Err(MarkIoError2::Conflict);
				}
				retry = true;
			}
		}
		self.check_disk()?;
		if retry {
			return Ok(MarkWrite2::Unchanged(Baseline2::Mark(*new)));
		}
		self.write_scratch(&bytes)?;
		let (tmp, mark) = (self.path.join(TMP), self.path.join(MARK));
		self.uncertain(
			MarkStep2::Rename,
			run(MarkStep2::Rename, || fs::rename(&tmp, &mark)),
		)?;
		let r = run(MarkStep2::DirSync, || self.dir.sync_all());
		self.uncertain(MarkStep2::DirSync, r)?;
		self.confirmed = Some((Baseline2::Mark(*new), bytes));
		Ok(MarkWrite2::Durable(Baseline2::Mark(*new)))
	}

	fn poison(&mut self, step: MarkStep2, error: MarkIoError2) -> MarkIoError2 {
		self.poisoned = Some(step);
		error
	}
	fn uncertain<T>(&mut self, step: MarkStep2, r: io::Result<T>) -> Owned<T> {
		r.map_err(|_| self.poison(step, MarkIoError2::Uncertain { step }))
	}
	// Directory identity, then the on-disk mark against this instance's confirmed state.
	fn check_disk(&mut self) -> Owned<()> {
		use MarkStep2 as S;
		let now = run(S::DirCheck, || fs::metadata(&self.path))
			.and_then(|path| Ok((path, self.dir.metadata()?)));
		match now {
			Ok((p, h)) if (p.dev(), p.ino()) == self.id && (h.dev(), h.ino()) == self.id => {}
			Ok(_) => return Err(self.poison(S::DirCheck, MarkIoError2::DirectoryChanged)),
			Err(e) => return Err(self.poison(S::DirCheck, io_err(S::DirCheck)(e))),
		}
		let disk = match read_mark(&self.path, S::ReRead) {
			Ok((disk, _)) => disk,
			Err(e) => return Err(self.poison(S::ReRead, e)),
		};
		let matches = match (&disk, &self.confirmed) {
			(Disk::Absent, None) => true,
			(Disk::Present(bytes), Some((_, confirmed))) => bytes.as_slice() == &confirmed[..],
			_ => false,
		};
		if !matches {
			return Err(self.poison(S::ReRead, MarkIoError2::Interference));
		}
		Ok(())
	}
	// Clears a stale scratch file, then writes and syncs the new bytes to a fresh one.
	fn write_scratch(&mut self, bytes: &Bytes) -> Owned<()> {
		use MarkStep2 as S;
		let tmp = self.path.join(TMP);
		match fs::symlink_metadata(&tmp) {
			Ok(_) => {
				if let Err(e) = run(S::TmpUnlink, || fs::remove_file(&tmp)) {
					return Err(self.poison(S::TmpUnlink, io_err(S::TmpUnlink)(e)));
				}
				let r = run(S::TmpUnlinkDirSync, || self.dir.sync_all());
				self.uncertain(S::TmpUnlinkDirSync, r)?;
			}
			Err(e) if e.kind() == ErrorKind::NotFound => {}
			Err(e) => return Err(self.poison(S::TmpUnlink, io_err(S::TmpUnlink)(e))),
		}
		let file = match run(S::TmpCreate, || open_file(&tmp, true)) {
			Ok(f) => f,
			Err(e) => return Err(self.poison(S::TmpCreate, io_err(S::TmpCreate)(e))),
		};
		let file = match regular(file, S::TmpCreate) {
			Ok(f) => f,
			Err(e) => return Err(self.poison(S::TmpCreate, e)),
		};
		if let Err(e) = run(S::TmpWrite, || (&file).write_all(bytes)) {
			return Err(self.poison(S::TmpWrite, io_err(S::TmpWrite)(e)));
		}
		// After a failed fsync the file's contents are unknown: nothing is published from it.
		if run(S::TmpSync, || file.sync_data()).is_err() {
			return Err(self.poison(S::TmpSync, MarkIoError2::Poisoned { step: S::TmpSync }));
		}
		Ok(())
	}
}

/// Test-only fault and stop plan, per thread. `Before` fails a step before its system call;
/// `After` lets the call succeed and then reports `EIO`; a stop panics after the call succeeds.
/// A stop followed by a reopen models process interruption with the same kernel page cache, not
/// power loss.
#[cfg(test)]
pub(in crate::storage::mutation_visibility::root2) mod seam {
	use super::MarkStep2;
	use std::cell::RefCell;

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub(in crate::storage::mutation_visibility::root2) enum Fault {
		Before,
		After,
	}
	type Hook = Box<dyn FnMut()>;
	thread_local! {
		static FAULTS: RefCell<Vec<(MarkStep2, u32, Fault)>> = const { RefCell::new(Vec::new()) };
		static STOP: RefCell<Option<(MarkStep2, u32)>> = const { RefCell::new(None) };
		static HOOK: RefCell<Option<(MarkStep2, Hook)>> = const { RefCell::new(None) };
		static SEEN: RefCell<Vec<MarkStep2>> = const { RefCell::new(Vec::new()) };
	}
	pub(in crate::storage::mutation_visibility::root2) struct Plan;
	impl Drop for Plan {
		fn drop(&mut self) {
			FAULTS.with(|f| f.borrow_mut().clear());
			STOP.with(|s| *s.borrow_mut() = None);
			HOOK.with(|h| *h.borrow_mut() = None);
		}
	}
	/// Fail the nth (1-based) occurrence of each listed step; stop after the nth of `stop`.
	pub(in crate::storage::mutation_visibility::root2) fn plan(
		faults: &[(MarkStep2, u32, Fault)],
		stop: Option<(MarkStep2, u32)>,
	) -> Plan {
		FAULTS.with(|f| *f.borrow_mut() = faults.to_vec());
		STOP.with(|s| *s.borrow_mut() = stop);
		SEEN.with(|s| s.borrow_mut().clear());
		Plan
	}
	/// Runs `hook` once, just before the first occurrence of `step` performs its system call.
	pub(in crate::storage::mutation_visibility::root2) fn hook(
		step: MarkStep2,
		hook: impl FnMut() + 'static,
	) {
		HOOK.with(|h| *h.borrow_mut() = Some((step, Box::new(hook))));
	}
	pub(in crate::storage::mutation_visibility::root2) fn seen() -> Vec<MarkStep2> {
		SEEN.with(|s| s.borrow().clone())
	}
	fn count(step: MarkStep2) -> u32 {
		SEEN.with(|s| s.borrow().iter().filter(|x| **x == step).count() as u32)
	}
	fn faulted(step: MarkStep2, kind: Fault) -> bool {
		FAULTS.with(|f| f.borrow().contains(&(step, count(step), kind)))
	}
	pub(super) fn before(step: MarkStep2) -> std::io::Result<()> {
		SEEN.with(|s| s.borrow_mut().push(step));
		let hook = HOOK.with(|h| {
			let mut h = h.borrow_mut();
			match h.as_ref() {
				Some((s, _)) if *s == step => h.take().map(|(_, f)| f),
				_ => None,
			}
		});
		if let Some(mut f) = hook {
			f();
		}
		if faulted(step, Fault::Before) {
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		Ok(())
	}
	pub(super) fn after(step: MarkStep2) -> std::io::Result<()> {
		if STOP.with(|s| *s.borrow()) == Some((step, count(step))) {
			panic!("crash stop after {step:?}");
		}
		if faulted(step, Fault::After) {
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		Ok(())
	}
}
