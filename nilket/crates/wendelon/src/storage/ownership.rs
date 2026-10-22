//! Exclusive process ownership for a Wendelon node data directory.

use std::fs::{self, File, OpenOptions};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// Process exit status for a shutdown that could not prove every writer stopped.
/// Supervisors treat it as "restart into recovery", never as a configuration error.
pub const UNCLEAN_SHUTDOWN_EXIT_CODE: i32 = 86;

/// A retained storage handle attempted to write after shutdown admission closed.
#[derive(Debug)]
pub struct Closed;

impl std::fmt::Display for Closed {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("storage write admission is closed")
	}
}
impl std::error::Error for Closed {}

pub(crate) fn closed_error() -> Error {
	Error::new(ErrorKind::BrokenPipe, Closed)
}

/// Shutdown could not prove every writer stopped. The lock remains held.
#[derive(Debug)]
pub struct ShutdownIncomplete {
	pub outstanding: usize,
	cause: Error,
}
impl std::fmt::Display for ShutdownIncomplete {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"storage shutdown incomplete ({} outstanding writes)",
			self.outstanding
		)
	}
}
impl std::error::Error for ShutdownIncomplete {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		Some(&self.cause)
	}
}

#[async_trait::async_trait]
pub(crate) trait StorageWorker: Send + Sync {
	async fn close_and_join(&self) -> std::io::Result<()>;
}

/// Join results survive cancellation/timeouts of an async shutdown caller.
#[derive(Default)]
pub(crate) struct WorkerJoins {
	handles: Mutex<Option<Vec<std::thread::JoinHandle<()>>>>,
	result: Mutex<Option<Result<(), String>>>,
	finished: Notify,
}

impl WorkerJoins {
	pub(crate) fn new(handles: Vec<std::thread::JoinHandle<()>>) -> Arc<Self> {
		Arc::new(Self {
			handles: Mutex::new(Some(handles)),
			..Self::default()
		})
	}

	pub(crate) async fn join(self: &Arc<Self>) -> std::io::Result<()> {
		let handles = self.handles.lock().unwrap().take();
		if let Some(handles) = handles {
			let state = self.clone();
			tokio::task::spawn_blocking(move || {
				let mut result = Ok(());
				for handle in handles {
					if handle.join().is_err() {
						result = Err("storage worker panicked".to_string());
					}
				}
				*state.result.lock().unwrap() = Some(result);
				state.finished.notify_waiters();
			});
		}
		loop {
			let notified = self.finished.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			if let Some(result) = self.result.lock().unwrap().clone() {
				return result.map_err(Error::other);
			}
			notified.await;
		}
	}
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OwnerPhase {
	Active,
	Closing,
	Closed,
}

struct OwnerState {
	phase: OwnerPhase,
	outstanding: usize,
	startup_barriers: usize,
	// Fields drop in declaration order. Workers (whose closers may still hold a
	// redb database, which writes on drop) must go before the lock descriptor,
	// so an owner dropped without close never releases the lock first.
	workers: Vec<Arc<dyn StorageWorker>>,
	// A failed/cancelled close must retain LOCK even if the caller drops all
	// external handles. Cleared only after drain and joins actually finish.
	closing_pin: Option<Arc<OwnerInner>>,
	lock: Option<HeldFlock>,
}

/// The flock'd sentinel. Dropping it unlocks explicitly before closing: a lock
/// belongs to the open file description, which a concurrently forked child
/// shares until it execs, so closing alone could leave the lock held briefly.
struct HeldFlock(File);

impl Drop for HeldFlock {
	fn drop(&mut self) {
		#[cfg(unix)]
		unsafe {
			libc::flock(std::os::fd::AsRawFd::as_raw_fd(&self.0), libc::LOCK_UN);
		}
	}
}

/// Admission for exactly one write operation. Move it into queued work and keep
/// it until the blocking operation or kernel completion finishes, even if its
/// caller is cancelled. Never hold it while waiting for another permit, a
/// channel reservation/receive, or a network event. Reads require no permit.
pub struct WritePermit {
	inner: Arc<OwnerInner>,
}

impl Drop for WritePermit {
	fn drop(&mut self) {
		let mut state = self.inner.state.lock().unwrap();
		state.outstanding -= 1;
		if state.outstanding == 0 {
			self.inner.drained.notify_waiters();
		}
	}
}

/// Returned when another process already owns the canonical data directory.
#[derive(Debug)]
pub struct DataDirectoryBusy {
	/// Canonical data-directory path used to identify the contended resource.
	pub path: PathBuf,
}

impl std::fmt::Display for DataDirectoryBusy {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"Wendelon data directory is already owned: {}",
			self.path.display()
		)
	}
}

impl std::error::Error for DataDirectoryBusy {}

/// Cloneable proof that this process owns one canonical Wendelon data directory.
/// The sentinel file is intentionally persistent; dropping the final clone only
/// releases the advisory lock and never unlinks the sentinel.
#[derive(Clone)]
pub struct DataDirectoryOwner {
	inner: Arc<OwnerInner>,
}

struct OwnerInner {
	closed_signal: Arc<std::sync::atomic::AtomicBool>,
	startup_failed: std::sync::atomic::AtomicBool,
	path: PathBuf,
	previous_shutdown_clean: bool,
	// Set when a run that started after an unclean shutdown finished its
	// recovery validation. Only then may that run leave a clean record.
	recovery_complete: std::sync::atomic::AtomicBool,
	state: Mutex<OwnerState>,
	drained: Notify,
	close_gate: tokio::sync::Mutex<()>,
	startup_cleanups: Mutex<Vec<Arc<StartupCleanupSlot>>>,
	#[cfg(test)]
	startup_cleanup_pause: Mutex<Option<Arc<Notify>>>,
}

impl std::fmt::Debug for DataDirectoryOwner {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DataDirectoryOwner")
			.field("path", &self.inner.path)
			.finish_non_exhaustive()
	}
}

/// A reserved startup drain is distinct from an individual write permit.
/// Unexpected Drop fences admission and deliberately retains the reservation.
pub(crate) struct StartupDrainBarrier {
	owner: DataDirectoryOwner,
	released: bool,
}
impl StartupDrainBarrier {
	fn release(mut self) {
		let mut state = self.owner.inner.state.lock().unwrap();
		state.startup_barriers = state
			.startup_barriers
			.checked_sub(1)
			.expect("startup barrier released twice");
		self.released = true;
		self.owner.inner.drained.notify_waiters();
	}
	pub(crate) fn release_after_handoff(self) {
		self.release();
	}
	pub(crate) fn complete_cleanup(self) {
		self.release();
	}
}
impl Drop for StartupDrainBarrier {
	fn drop(&mut self) {
		if !self.released {
			self.owner.begin_close();
		}
	}
}
#[derive(Clone, Debug)]
pub enum StartupCleanupStatus {
	Running,
	PendingIncomplete(String),
	Complete,
	Failed {
		kind: ErrorKind,
		message: String,
		incomplete: bool,
		outstanding: Option<usize>,
	},
}
pub(crate) struct StartupCleanupSlot {
	context: String,
	progress: Arc<Mutex<&'static str>>,
	status: tokio::sync::watch::Sender<StartupCleanupStatus>,
	handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl StartupCleanupSlot {
	#[cfg(test)]
	pub(crate) fn current_stage(&self) -> &'static str {
		*self
			.progress
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}
	pub(crate) async fn wait(&self) -> StartupCleanupStatus {
		let mut status = self.status.subscribe();
		let mut handle = self.handle.lock().await;
		loop {
			let value = status.borrow_and_update().clone();
			if !matches!(value, StartupCleanupStatus::Running) {
				let finished_pending = matches!(value, StartupCleanupStatus::PendingIncomplete(_))
					&& handle.as_ref().is_some_and(|task| task.is_finished());
				if !finished_pending {
					return value;
				}
			}
			let Some(task) = handle.as_mut() else {
				return StartupCleanupStatus::PendingIncomplete(
					"cleanup handle missing report".into(),
				);
			};
			tokio::select! {
				result = task => {
					handle.take();
					if let Err(error) = result {
						self.status.send_replace(StartupCleanupStatus::Failed { kind: ErrorKind::Other, message: format!("{}: startup cleanup task failed: {error}", self.context), incomplete: true, outstanding: None });
					}
				},
				changed = status.changed() => if changed.is_err() { return StartupCleanupStatus::PendingIncomplete("cleanup status closed".into()); }
			}
		}
	}

	pub(crate) async fn join_finished(&self) {
		// Borrow the registered handle: cancelling an observer never drops it.
		let mut handle = self.handle.lock().await;
		if let Some(task) = handle.as_mut() {
			let _ = task.await;
		}
		handle.take();
	}
}

impl DataDirectoryOwner {
	pub(crate) fn reserve_startup_drain(&self) -> std::io::Result<StartupDrainBarrier> {
		let mut state = self.inner.state.lock().unwrap();
		if state.phase != OwnerPhase::Active {
			return Err(closed_error());
		}
		state.startup_barriers = state
			.startup_barriers
			.checked_add(1)
			.expect("startup barrier count overflow");
		Ok(StartupDrainBarrier {
			owner: self.clone(),
			released: false,
		})
	}
	pub(crate) fn closed_signal(&self) -> Arc<std::sync::atomic::AtomicBool> {
		self.inner.closed_signal.clone()
	}
	pub(crate) fn begin_close(&self) {
		let mut state = self
			.inner
			.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		if state.phase != OwnerPhase::Closed {
			state.phase = OwnerPhase::Closing;
			state.closing_pin = Some(self.inner.clone());
			self.inner.drained.notify_waiters();
		}
	}
	pub(crate) fn startup_incomplete(&self, message: String) -> Error {
		self.startup_incomplete_from(ErrorKind::TimedOut, message, None)
	}
	pub(crate) fn startup_incomplete_from(
		&self,
		kind: ErrorKind,
		message: String,
		outstanding: Option<usize>,
	) -> Error {
		Error::new(
			kind,
			ShutdownIncomplete {
				outstanding: outstanding.unwrap_or_else(|| {
					self.inner
						.state
						.lock()
						.unwrap_or_else(std::sync::PoisonError::into_inner)
						.outstanding
				}),
				cause: Error::new(kind, message),
			},
		)
	}
	pub(crate) fn track_startup_cleanup<F>(
		&self,
		future: F,
		bound: Duration,
		context: String,
		progress: Arc<Mutex<&'static str>>,
		runtime: tokio::runtime::Handle,
	) -> Arc<StartupCleanupSlot>
	where
		F: std::future::Future<Output = std::io::Result<()>> + Send + 'static,
	{
		let (status, _) = tokio::sync::watch::channel(StartupCleanupStatus::Running);
		let (start, ready) = tokio::sync::oneshot::channel();
		let slot = Arc::new(StartupCleanupSlot {
			context,
			progress: progress.clone(),
			status,
			handle: tokio::sync::Mutex::new(None),
		});
		let worker_slot = slot.clone();
		#[cfg(test)]
		let pause = self.inner.startup_cleanup_pause.lock().unwrap().take();
		let handle = runtime.spawn(async move {
			let _ = ready.await;
			#[cfg(test)]
			if let Some(pause) = pause {
				pause.notified().await;
			}
			tokio::pin!(future);
			let result = match tokio::time::timeout(bound, future.as_mut()).await {
				Ok(result) => result,
				Err(_) => {
					tracing::error!(context = %worker_slot.context, stage = *progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner), "startup cleanup incomplete at bound; resources retained");
					worker_slot
						.status
						.send_replace(StartupCleanupStatus::PendingIncomplete(format!(
							"{}: startup shutdown still running at {}",
							worker_slot.context,
							*progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
						)));
					future.await
				}
			};
			let status = match result {
				Ok(()) => StartupCleanupStatus::Complete,
				Err(error) => {
					let incomplete = error.get_ref().and_then(|source| source.downcast_ref::<ShutdownIncomplete>());
					let outstanding = incomplete.map(|error| error.outstanding);
					let mut message = format!("{} at {}: {error}", worker_slot.context, *worker_slot.progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
					let mut cause = std::error::Error::source(&error);
					while let Some(source) = cause { message.push_str(": "); message.push_str(&source.to_string()); cause = source.source(); }
					StartupCleanupStatus::Failed { kind: error.kind(), message, incomplete: incomplete.is_some(), outstanding }
				},
			};
			if let StartupCleanupStatus::Failed { message, .. } = &status {
				tracing::error!(context = %worker_slot.context, error = %message, "startup cleanup failed");
			}
			worker_slot.status.send_replace(status);
		});
		*slot.handle.try_lock().expect("unpublished startup slot") = Some(handle);
		self.inner
			.startup_cleanups
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.push(slot.clone());
		let _ = start.send(());
		slot
	}
	/// Retained startup cleanup outcomes, including cancellation with no caller.
	/// Reports own no storage resources; incomplete cleanup still retains LOCK.
	pub fn startup_cleanup_reports(&self) -> Vec<StartupCleanupStatus> {
		self.inner
			.startup_cleanups
			.lock()
			.unwrap()
			.iter()
			.map(|slot| {
				let value = slot.status.borrow().clone();
				if matches!(
					value,
					StartupCleanupStatus::Running | StartupCleanupStatus::PendingIncomplete(_)
				) && slot
					.handle
					.try_lock()
					.ok()
					.is_some_and(|handle| handle.as_ref().is_some_and(|task| task.is_finished()))
				{
					{
						let current = slot.status.borrow().clone();
						if matches!(
							current,
							StartupCleanupStatus::Running
								| StartupCleanupStatus::PendingIncomplete(_)
						) {
							StartupCleanupStatus::Failed {
								kind: ErrorKind::Other,
								message: format!(
									"{}: cleanup task ended without terminal report; wait joins exact failure",
									slot.context
								),
								incomplete: true,
								outstanding: None,
							}
						} else {
							current
						}
					}
				} else {
					value
				}
			})
			.collect()
	}
	#[cfg(test)]
	pub(crate) fn pause_startup_cleanup(&self, pause: Arc<Notify>) {
		*self.inner.startup_cleanup_pause.lock().unwrap() = Some(pause);
	}
	#[cfg(test)]
	pub(crate) fn latest_startup_cleanup(&self) -> Option<Arc<StartupCleanupSlot>> {
		self.inner.startup_cleanups.lock().unwrap().last().cloned()
	}

	/// Record that this run, which started after an unclean shutdown, finished
	/// its recovery validation, so a complete close may mark the directory clean.
	pub fn mark_recovery_complete(&self) {
		self.inner
			.recovery_complete
			.store(true, std::sync::atomic::Ordering::Release);
	}

	pub(crate) fn mark_startup_failed(&self) {
		self.inner
			.startup_failed
			.store(true, std::sync::atomic::Ordering::Release);
	}
	fn may_record_clean_shutdown(&self) -> bool {
		if self
			.inner
			.startup_failed
			.load(std::sync::atomic::Ordering::Acquire)
		{
			return false;
		}
		self.inner.previous_shutdown_clean
			|| self
				.inner
				.recovery_complete
				.load(std::sync::atomic::Ordering::Acquire)
	}

	/// Whether the previous owner of this directory finished a complete shutdown
	/// (or the directory was never owned). False after a crash, kill or an
	/// incomplete shutdown: the node must boot in recovery mode.
	pub fn previous_shutdown_clean(&self) -> bool {
		self.inner.previous_shutdown_clean
	}

	pub(crate) fn same_owner(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.inner, &other.inner)
	}
	/// A permit when owned; `None` for an unowned (test or tool) store.
	pub(crate) fn optional_permit(owner: Option<&Self>) -> std::io::Result<Option<WritePermit>> {
		owner.map(Self::write_permit).transpose()
	}

	pub(crate) fn write_permit(&self) -> std::io::Result<WritePermit> {
		let mut state = self.inner.state.lock().unwrap();
		if state.phase != OwnerPhase::Active {
			return Err(closed_error());
		}
		state.outstanding += 1;
		Ok(WritePermit {
			inner: self.inner.clone(),
		})
	}

	#[cfg(test)]
	pub(crate) async fn wait_until_closing(&self) {
		loop {
			let changed = self.inner.drained.notified();
			tokio::pin!(changed);
			changed.as_mut().enable();
			if self.inner.state.lock().unwrap().phase != OwnerPhase::Active {
				return;
			}
			changed.await;
		}
	}

	pub(crate) fn register_worker(&self, worker: Arc<dyn StorageWorker>) -> std::io::Result<()> {
		let mut state = self.inner.state.lock().unwrap();
		if state.phase != OwnerPhase::Active {
			return Err(closed_error());
		}
		if !state
			.workers
			.iter()
			.any(|existing| Arc::ptr_eq(existing, &worker))
		{
			state.workers.push(worker);
		}
		Ok(())
	}

	/// Reject new writes, drain admitted operations and join registered workers.
	/// Timeout/failure leaves the descriptor held and admission closed. Repeating
	/// close resumes the same joins; cancellation never treats a live join as done.
	pub async fn close(&self, bound: Duration) -> std::io::Result<()> {
		self.begin_close();
		let _close = self.inner.close_gate.lock().await;
		let result = tokio::time::timeout(bound, async {
			{
				let mut state = self.inner.state.lock().unwrap();
				if state.phase == OwnerPhase::Closed {
					return Ok(());
				}
				state.phase = OwnerPhase::Closing;
				state.closing_pin = Some(self.inner.clone());
				self.inner.drained.notify_waiters();
			}
			loop {
				let notified = self.inner.drained.notified();
				tokio::pin!(notified);
				notified.as_mut().enable();
				if { let state = self.inner.state.lock().unwrap(); state.outstanding == 0 && state.startup_barriers == 0 } {
					break;
				}
				notified.await;
			}
			let workers = self.inner.state.lock().unwrap().workers.clone();
			let mut first_error = None;
			for worker in workers {
				if let Err(error) = worker.close_and_join().await {
					first_error.get_or_insert(error);
				}
			}
			if let Some(error) = first_error {
				return Err(error);
			}
			let mut state = self.inner.state.lock().unwrap();
			assert_eq!(state.startup_barriers, 0, "startup drain bypassed");
			// Every writer has stopped. Record that before releasing the lock so
			// the next owner can tell a complete shutdown from a crash or kill.
			// Failure only costs the next boot a recovery pass.
			if !self.may_record_clean_shutdown() {
				tracing::warn!(
					path = %self.path().display(),
					"recovery did not complete during this run; leaving no clean-shutdown record"
				);
			} else if let Err(error) = write_clean_shutdown_record(self.path()) {
				tracing::warn!(%error, path = %self.path().display(), "clean-shutdown record not written; next start will recover");
			}
			state.lock.take();
			state.phase = OwnerPhase::Closed;
			state.closing_pin.take();
			state.workers.clear();
			self.inner.closed_signal.store(true, std::sync::atomic::Ordering::Release);
			Ok(())
		})
		.await;
		match result {
			Ok(Ok(())) => Ok(()),
			result => {
				let cause = match result {
					Ok(Err(error)) => error,
					Err(error) => Error::new(ErrorKind::TimedOut, error),
					Ok(Ok(())) => unreachable!(),
				};
				Err(Error::new(
					cause.kind(),
					ShutdownIncomplete {
						outstanding: self.inner.state.lock().unwrap().outstanding,
						cause,
					},
				))
			}
		}
	}
	/// Acquire an existing data directory. Configuration validation should run
	/// before this call so ordinary startup does not create operator paths.
	pub fn acquire_existing(data_dir: impl AsRef<Path>) -> std::io::Result<Self> {
		Self::acquire(data_dir.as_ref(), false)
	}

	/// Acquire a directory for an authorized restore or profile reset entry point.
	/// This is the only variant that creates the data-directory path.
	pub fn acquire_or_create(data_dir: impl AsRef<Path>) -> std::io::Result<Self> {
		Self::acquire(data_dir.as_ref(), true)
	}

	/// Canonical path held by this owner.
	pub fn path(&self) -> &Path {
		&self.inner.path
	}

	pub fn ensure_path(&self, data_dir: impl AsRef<Path>) -> std::io::Result<()> {
		let canonical = fs::canonicalize(data_dir)?;
		if canonical != self.path() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"Wendelon owner token is for {}, not {}",
					self.path().display(),
					canonical.display()
				),
			));
		}
		Ok(())
	}

	/// Verify an existing root or prospective file is within this owner's
	/// canonical directory. Resolve existing symlinks before comparing paths;
	/// for new files, resolve the nearest existing ancestor before any write.
	pub fn ensure_contains(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
		let path = path.as_ref();
		let absolute = if path.is_absolute() {
			path.to_path_buf()
		} else {
			std::env::current_dir()?.join(path)
		};
		let mut ancestor = absolute.as_path();
		let mut missing = Vec::new();
		let canonical = loop {
			match fs::symlink_metadata(ancestor) {
				Ok(_) => break fs::canonicalize(ancestor)?,
				Err(error) if error.kind() == ErrorKind::NotFound => {
					let name = ancestor.file_name().ok_or_else(|| {
						Error::new(ErrorKind::InvalidInput, "invalid owned storage path")
					})?;
					missing.push(name.to_os_string());
					ancestor = ancestor.parent().ok_or_else(|| {
						Error::new(ErrorKind::InvalidInput, "owned storage path has no parent")
					})?;
				}
				Err(error) => return Err(error),
			}
		};
		let resolved = missing
			.iter()
			.rev()
			.fold(canonical, |root, name| root.join(name));
		if !resolved.starts_with(self.path()) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"storage path {} is outside owned directory {}",
					path.display(),
					self.path().display()
				),
			));
		}
		Ok(())
	}

	fn acquire(data_dir: &Path, create: bool) -> std::io::Result<Self> {
		if create {
			fs::create_dir_all(data_dir)?;
		}
		let canonical = fs::canonicalize(data_dir)?;
		let metadata = fs::metadata(&canonical)?;
		if !metadata.is_dir() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"Wendelon data path is not a directory: {}",
					canonical.display()
				),
			));
		}
		acquire_lock(canonical)
	}
}

#[cfg(unix)]
fn acquire_lock(canonical: PathBuf) -> std::io::Result<DataDirectoryOwner> {
	use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

	let lock_path = canonical.join("LOCK");
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.mode(0o600)
		.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
		.open(&lock_path)?;
	let metadata = file.metadata()?;
	if !metadata.file_type().is_file() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"Wendelon ownership sentinel is not a regular file: {}",
				lock_path.display()
			),
		));
	}
	if metadata.permissions().mode() & 0o077 != 0 {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			format!(
				"Wendelon ownership sentinel has group/other permissions: {}",
				lock_path.display()
			),
		));
	}
	if metadata.nlink() != 1 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"Wendelon ownership sentinel has unexpected hard links: {}",
				lock_path.display()
			),
		));
	}
	let rc = unsafe {
		libc::flock(
			std::os::fd::AsRawFd::as_raw_fd(&file),
			libc::LOCK_EX | libc::LOCK_NB,
		)
	};
	if rc != 0 {
		let error = Error::last_os_error();
		match error.raw_os_error() {
			Some(libc::EWOULDBLOCK) => {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					DataDirectoryBusy { path: canonical },
				));
			}
			Some(libc::ENOSYS) | Some(libc::ENOLCK) | Some(libc::EOPNOTSUPP) => {
				return Err(Error::new(
					ErrorKind::Unsupported,
					format!(
						"Wendelon data-directory locking is unavailable for {}: {error}",
						canonical.display()
					),
				));
			}
			_ => return Err(error),
		}
	}
	// Decided under the lock. A directory is fresh only if it holds nothing but
	// the sentinel (and a leftover record temp file): a pre-existing data dir
	// without a LOCK (an older build, or a lock removed by hand) is not fresh.
	// Held from here: an error below must still unlock explicitly, not by close.
	let file = HeldFlock(file);
	let previous_shutdown_clean =
		consume_clean_shutdown_record(&canonical)? || directory_is_fresh(&canonical)?;
	Ok(DataDirectoryOwner {
		inner: Arc::new(OwnerInner {
			closed_signal: Arc::new(std::sync::atomic::AtomicBool::new(false)),
			startup_failed: std::sync::atomic::AtomicBool::new(false),
			path: canonical,
			previous_shutdown_clean,
			recovery_complete: std::sync::atomic::AtomicBool::new(false),
			state: Mutex::new(OwnerState {
				phase: OwnerPhase::Active,
				lock: Some(file),
				outstanding: 0,
				startup_barriers: 0,
				workers: Vec::new(),
				closing_pin: None,
			}),
			drained: Notify::new(),
			close_gate: tokio::sync::Mutex::new(()),
			startup_cleanups: Mutex::new(Vec::new()),
			#[cfg(test)]
			startup_cleanup_pause: Mutex::new(None),
		}),
	})
}

const CLEAN_SHUTDOWN_RECORD: &str = "CLEAN_SHUTDOWN";

fn write_clean_shutdown_record(dir: &Path) -> std::io::Result<()> {
	let tmp = dir.join(".CLEAN_SHUTDOWN.tmp");
	let record = dir.join(CLEAN_SHUTDOWN_RECORD);
	{
		use std::os::unix::fs::OpenOptionsExt;
		let file = OpenOptions::new()
			.write(true)
			.create(true)
			.truncate(true)
			.mode(0o600)
			.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
			.open(&tmp)?;
		file.sync_all()?;
	}
	fs::rename(&tmp, &record)?;
	File::open(dir)?.sync_all()
}

fn directory_is_fresh(dir: &Path) -> std::io::Result<bool> {
	for entry in fs::read_dir(dir)? {
		let name = entry?.file_name();
		if name != "LOCK" && name != ".CLEAN_SHUTDOWN.tmp" {
			return Ok(false);
		}
	}
	Ok(true)
}

/// Remove the clean-shutdown record durably before any other write by this
/// owner, so a crash during this run is never mistaken for a clean stop.
fn consume_clean_shutdown_record(dir: &Path) -> std::io::Result<bool> {
	let record = dir.join(CLEAN_SHUTDOWN_RECORD);
	match fs::remove_file(&record) {
		Ok(()) => {
			File::open(dir)?.sync_all()?;
			Ok(true)
		}
		Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
		Err(error) => Err(error),
	}
}

#[cfg(not(unix))]
fn acquire_lock(canonical: PathBuf) -> std::io::Result<DataDirectoryOwner> {
	Err(Error::new(
		ErrorKind::Unsupported,
		format!(
			"Wendelon data-directory ownership requires a supported exclusive file lock: {}",
			canonical.display()
		),
	))
}

#[cfg(all(test, unix))]
mod tests {
	use super::*;
	use std::os::unix::fs::MetadataExt;
	use std::path::PathBuf;
	use tokio::sync::oneshot;
	use uuid::Uuid;

	struct TestDir(PathBuf);

	impl TestDir {
		fn new() -> Self {
			let path = std::env::temp_dir().join(format!("wendelon-owner-{}", Uuid::new_v4()));
			fs::create_dir(&path).unwrap();
			Self(path)
		}

		fn path(&self) -> &Path {
			&self.0
		}
	}

	impl Drop for TestDir {
		fn drop(&mut self) {
			let _ = fs::remove_dir_all(&self.0);
		}
	}

	#[test]
	fn independent_open_in_same_process_is_busy_and_sentinel_survives_drop() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let sentinel = dir.path().join("LOCK");
		let inode = fs::metadata(&sentinel).unwrap();
		let err = DataDirectoryOwner::acquire_existing(dir.path()).unwrap_err();
		assert!(
			err.get_ref()
				.and_then(|e| e.downcast_ref::<DataDirectoryBusy>())
				.is_some()
		);
		drop(owner);
		assert_eq!(fs::metadata(&sentinel).unwrap().ino(), inode.ino());
		let reacquired = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		drop(reacquired);
	}

	#[tokio::test(start_paused = true)]
	async fn close_timeout_retains_lock_until_the_last_permit_finishes() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let permit = owner.write_permit().unwrap();
		let error = owner.close(Duration::from_secs(1)).await.unwrap_err();
		assert_eq!(
			error
				.get_ref()
				.unwrap()
				.downcast_ref::<ShutdownIncomplete>()
				.unwrap()
				.outstanding,
			1
		);
		assert!(DataDirectoryOwner::acquire_existing(dir.path()).is_err());
		assert!(
			owner
				.write_permit()
				.err()
				.unwrap()
				.get_ref()
				.unwrap()
				.is::<Closed>()
		);
		drop(permit);
		owner.close(Duration::from_secs(1)).await.unwrap();
		DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn racing_admission_is_granted_before_closing_or_refused() {
		for _ in 0..32 {
			let dir = TestDir::new();
			let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
			let barrier = Arc::new(tokio::sync::Barrier::new(3));
			let (release, released) = oneshot::channel();
			let writer = {
				let owner = owner.clone();
				let barrier = barrier.clone();
				tokio::spawn(async move {
					barrier.wait().await;
					let permit = owner.write_permit();
					released.await.unwrap();
					if let Err(error) = &permit {
						assert!(error.get_ref().unwrap().is::<Closed>());
					}
					drop(permit);
				})
			};
			let closer = {
				let owner = owner.clone();
				let barrier = barrier.clone();
				tokio::spawn(async move {
					barrier.wait().await;
					owner.close(Duration::from_secs(5)).await
				})
			};
			barrier.wait().await;
			loop {
				let notified = owner.inner.drained.notified();
				tokio::pin!(notified);
				notified.as_mut().enable();
				if owner.inner.state.lock().unwrap().phase != OwnerPhase::Active {
					break;
				}
				notified.await;
			}
			assert!(
				owner
					.write_permit()
					.err()
					.unwrap()
					.get_ref()
					.unwrap()
					.is::<Closed>()
			);
			release.send(()).unwrap();
			writer.await.unwrap();
			closer.await.unwrap().unwrap();
			DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		}
	}

	struct TestWorker {
		fail: bool,
		joined: Arc<std::sync::atomic::AtomicUsize>,
	}
	#[async_trait::async_trait]
	impl StorageWorker for TestWorker {
		async fn close_and_join(&self) -> std::io::Result<()> {
			self.joined
				.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			if self.fail {
				Err(Error::other("injected worker join failure"))
			} else {
				Ok(())
			}
		}
	}

	#[tokio::test]
	async fn failing_worker_does_not_skip_later_joins_or_release_lock() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let joined = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		owner
			.register_worker(Arc::new(TestWorker {
				fail: true,
				joined: joined.clone(),
			}))
			.unwrap();
		owner
			.register_worker(Arc::new(TestWorker {
				fail: false,
				joined: joined.clone(),
			}))
			.unwrap();
		assert!(owner.close(Duration::from_secs(5)).await.is_err());
		assert_eq!(joined.load(std::sync::atomic::Ordering::SeqCst), 2);
		assert!(DataDirectoryOwner::acquire_existing(dir.path()).is_err());
		owner.inner.state.lock().unwrap().workers.clear();
		owner.close(Duration::from_secs(5)).await.unwrap();
	}

	#[tokio::test]
	async fn owner_for_one_directory_cannot_open_a_store_in_another() {
		let owned = TestDir::new();
		let other = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(owned.path()).unwrap();
		let result =
			crate::consensus::raft_storage::WendelonLogStore::open_with_file_ops_and_owner(
				other.path().join("control.wal"),
				other.path().join("vote.wnd"),
				other.path().join("snapshot.wnd"),
				Arc::new(crate::storage::BlockingFileOps),
				owner,
			)
			.await;
		let error = result.err().expect("foreign directory must be rejected");
		assert_eq!(error.kind(), ErrorKind::InvalidInput);
		assert_eq!(fs::read_dir(other.path()).unwrap().count(), 0);
	}

	#[test]
	fn containment_resolves_aliases_and_rejects_escaping_symlinks() {
		let owned = TestDir::new();
		let other = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(owned.path()).unwrap();
		owner.ensure_contains(owned.path()).unwrap();
		owner
			.ensure_contains(owned.path().join("new/subdir/file"))
			.unwrap();
		std::os::unix::fs::symlink(owned.path(), other.path().join("alias")).unwrap();
		owner
			.ensure_contains(other.path().join("alias/new-file"))
			.unwrap();
		assert_eq!(
			owner
				.ensure_contains(owned.path().join("missing/../../B/x"))
				.unwrap_err()
				.kind(),
			ErrorKind::InvalidInput
		);
		std::os::unix::fs::symlink(other.path(), owned.path().join("escape")).unwrap();
		assert_eq!(
			owner
				.ensure_contains(owned.path().join("escape/file"))
				.unwrap_err()
				.kind(),
			ErrorKind::InvalidInput
		);
		std::os::unix::fs::symlink(other.path().join("missing"), owned.path().join("dangling"))
			.unwrap();
		assert!(
			owner
				.ensure_contains(owned.path().join("dangling/file"))
				.is_err()
		);
	}

	#[tokio::test]
	async fn shared_owner_is_held_until_every_aborted_worker_terminates() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let (started_a_tx, started_a_rx) = oneshot::channel();
		let (started_b_tx, started_b_rx) = oneshot::channel();
		let (release_a_tx, release_a_rx) = std::sync::mpsc::channel();
		let (release_b_tx, release_b_rx) = std::sync::mpsc::channel();
		let owner_a = owner.clone();
		let owner_b = owner.clone();
		let worker_a = tokio::task::spawn_blocking(move || {
			let _owner = owner_a;
			let _ = started_a_tx.send(());
			release_a_rx.recv().unwrap();
		});
		let worker_b = tokio::task::spawn_blocking(move || {
			let _owner = owner_b;
			let _ = started_b_tx.send(());
			release_b_rx.recv().unwrap();
		});
		started_a_rx.await.unwrap();
		started_b_rx.await.unwrap();
		worker_a.abort();
		worker_b.abort();
		assert!(DataDirectoryOwner::acquire_existing(dir.path()).is_err());
		release_a_tx.send(()).unwrap();
		let _ = worker_a.await;
		assert!(DataDirectoryOwner::acquire_existing(dir.path()).is_err());
		release_b_tx.send(()).unwrap();
		let _ = worker_b.await;
		drop(owner);
		DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
	}

	#[tokio::test(flavor = "current_thread")]
	async fn async_worker_retains_owner_until_abort_is_joined() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let (started_tx, started_rx) = oneshot::channel();
		let worker = tokio::spawn(async move {
			let _owner = owner;
			started_tx.send(()).unwrap();
			std::future::pending::<()>().await;
		});
		started_rx.await.unwrap();
		worker.abort();
		// On this `current_thread` runtime cancellation cannot be polled between
		// abort and this independent open: the task still owns its future.
		let error = DataDirectoryOwner::acquire_existing(dir.path()).unwrap_err();
		assert!(
			error
				.get_ref()
				.and_then(|e| e.downcast_ref::<DataDirectoryBusy>())
				.is_some()
		);
		assert!(worker.await.unwrap_err().is_cancelled());
		DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
	}

	#[test]
	fn an_existing_data_directory_without_a_lock_is_not_treated_as_fresh() {
		// An older build or a hand-removed LOCK: data is present, so the previous
		// run's shutdown cannot be assumed clean.
		let dir = TestDir::new();
		fs::write(dir.path().join("control.wal"), b"history").unwrap();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		assert!(!owner.previous_shutdown_clean());
	}

	#[tokio::test]
	async fn a_recovering_run_leaves_a_clean_record_only_after_recovery_completes() {
		let dir = TestDir::new();
		fs::write(dir.path().join("control.wal"), b"history").unwrap();
		let recovering = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		assert!(!recovering.previous_shutdown_clean());
		recovering.close(Duration::from_secs(5)).await.unwrap();
		drop(recovering);
		assert!(
			!dir.path().join(CLEAN_SHUTDOWN_RECORD).exists(),
			"an unrecovered run must not certify a clean shutdown"
		);

		let recovered = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		recovered.mark_recovery_complete();
		recovered.close(Duration::from_secs(5)).await.unwrap();
		drop(recovered);
		assert!(dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());
	}

	#[test]
	fn dropping_an_owner_without_close_releases_the_lock_after_its_workers() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let path = dir.path().join("store.redb");
		let store = crate::storage::owned_redb::OwnedRedb::new(
			redb::Database::create(&path).unwrap(),
			"test store",
		);
		store.bind_owner(owner.clone()).unwrap();
		drop(store);
		drop(owner);
		// Both the directory lock and redb's own file lock are free again.
		let again = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		drop(redb::Database::create(&path).unwrap());
		drop(again);
	}

	#[tokio::test]
	async fn clean_shutdown_record_is_written_on_close_and_consumed_once() {
		let dir = TestDir::new();
		let fresh = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		assert!(
			fresh.previous_shutdown_clean(),
			"a never-owned directory is clean"
		);
		fresh.close(Duration::from_secs(5)).await.unwrap();
		drop(fresh);
		assert!(dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());

		let after_clean = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		assert!(after_clean.previous_shutdown_clean());
		assert!(
			!dir.path().join(CLEAN_SHUTDOWN_RECORD).exists(),
			"the record is consumed before any other write"
		);
		// The run writes data, then ends without close: a crash or kill.
		fs::write(dir.path().join("data.bin"), b"written").unwrap();
		drop(after_clean);

		let after_crash = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		assert!(!after_crash.previous_shutdown_clean());
	}
}

#[cfg(test)]
mod startup_drain_tests {
	use super::*;
	fn directory() -> crate::test_support::TestDir {
		crate::test_support::TestDir::new("startup-drain")
	}
	fn incomplete(error: &Error) -> bool {
		error
			.get_ref()
			.is_some_and(|e| e.is::<ShutdownIncomplete>())
	}
	#[tokio::test]
	async fn reserved_startup_drain_excludes_external_close_until_actual_completion() {
		let dir = directory();
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let token = owner.reserve_startup_drain().unwrap();
		assert!(owner.write_permit().is_ok());
		let error = owner.close(Duration::from_millis(20)).await.unwrap_err();
		assert!(incomplete(&error));
		assert!(owner.write_permit().is_err());
		assert!(DataDirectoryOwner::acquire_existing(&dir).is_err());
		assert!(
			owner.reserve_startup_drain().is_err(),
			"Closing excludes new reservations"
		);
		token.complete_cleanup();
		owner.close(Duration::from_secs(5)).await.unwrap();
		let reopened = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		reopened.close(Duration::from_secs(5)).await.unwrap();
	}
	#[tokio::test]
	async fn successful_startup_handoff_preserves_admission_and_normal_close() {
		let dir = directory();
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		owner
			.reserve_startup_drain()
			.unwrap()
			.release_after_handoff();
		assert!(owner.write_permit().is_ok());
		owner.close(Duration::from_secs(5)).await.unwrap();
		assert!(
			owner.reserve_startup_drain().is_err(),
			"Closed excludes late reservations"
		);
	}
	#[tokio::test]
	async fn cancelled_cleanup_observer_retains_worker_and_reserved_drain() {
		let dir = directory();
		let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let token = owner.reserve_startup_drain().unwrap();
		owner.begin_close();
		let (release, wait) = tokio::sync::oneshot::channel();
		let cleanup_owner = owner.clone();
		let slot = owner.track_startup_cleanup(
			async move {
				let _ = wait.await;
				token.complete_cleanup();
				cleanup_owner.close(Duration::from_secs(5)).await
			},
			Duration::from_millis(30),
			"cancelled observer control".into(),
			Arc::new(Mutex::new("parked fixture joins")),
			tokio::runtime::Handle::current(),
		);
		let mut observer = Box::pin(slot.wait());
		use std::future::Future;
		assert!(
			observer
				.as_mut()
				.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
				.is_pending()
		);
		drop(observer);
		assert!(incomplete(
			&owner.close(Duration::from_millis(20)).await.unwrap_err()
		));
		assert!(matches!(
			slot.wait().await,
			StartupCleanupStatus::PendingIncomplete(_)
		));
		assert!(DataDirectoryOwner::acquire_existing(&dir).is_err());
		release.send(()).unwrap();
		// A prior PendingIncomplete is a report, not loss of the running worker.
		tokio::time::timeout(Duration::from_secs(5), async {
			loop {
				if matches!(*slot.status.borrow(), StartupCleanupStatus::Complete) {
					break;
				}
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		assert!(matches!(slot.wait().await, StartupCleanupStatus::Complete));
		slot.join_finished().await;
		let reopened = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		reopened.close(Duration::from_secs(5)).await.unwrap();
	}
	#[test]
	fn abandoned_startup_drain_fails_closed_in_disposable_process() {
		const CHILD: &str = "NILKET_STARTUP_DRAIN_CHILD";
		if std::env::var_os(CHILD).is_some() {
			let dir = directory();
			let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
			drop(owner.reserve_startup_drain().unwrap());
			assert!(owner.write_permit().is_err());
			assert!(DataDirectoryOwner::acquire_existing(&dir).is_err());
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.unwrap();
			assert!(incomplete(
				&runtime
					.block_on(owner.close(Duration::from_millis(20)))
					.unwrap_err()
			));
			runtime.block_on(async {
				let (release, wait) = tokio::sync::oneshot::channel();
				let slot = owner.track_startup_cleanup(async move { let _ = wait.await; panic!("cleanup panic after incomplete report"); }, Duration::from_millis(20), "panic report control".into(), Arc::new(Mutex::new("parked panic control")), tokio::runtime::Handle::current());
				assert!(matches!(slot.wait().await, StartupCleanupStatus::PendingIncomplete(_)));
				release.send(()).unwrap();
				tokio::time::timeout(Duration::from_secs(5), async { loop {
					if slot.handle.try_lock().ok().is_some_and(|h| h.as_ref().is_some_and(|task| task.is_finished())) { break; }
					tokio::task::yield_now().await;
				}}).await.unwrap();
				assert!(matches!(owner.startup_cleanup_reports().last(), Some(StartupCleanupStatus::Failed { incomplete: true, .. })));
				assert!(matches!(slot.wait().await, StartupCleanupStatus::Failed { incomplete: true, ref message, .. } if message.contains("cleanup panic after incomplete report")));
				assert!(incomplete(&owner.close(Duration::from_millis(20)).await.unwrap_err()));
			});
			// Intentional retained pin belongs solely to this process, never parent.
			return;
		}
		let output = std::process::Command::new(std::env::current_exe().unwrap()).env(CHILD, "1").arg("storage::ownership::startup_drain_tests::abandoned_startup_drain_fails_closed_in_disposable_process").arg("--exact").arg("--nocapture").output().unwrap();
		assert!(
			output.status.success(),
			"child failed: {}",
			String::from_utf8_lossy(&output.stderr)
		);
	}
}
