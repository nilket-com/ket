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
	// ADR 0132 D2: what a clean close settles after its first drain, and
	// whether a close already settled it, followed by its second drain.
	settlers: Vec<Arc<dyn CloseSettler>>,
	settled: bool,
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

/// ADR 0699 S5a: admission for one lifecycle operation that creates trusted
/// durable state (a staged base, its report, an activation). It shares the
/// drain accounting of writes, so close waits for it, and it is a separate type
/// from [`WritePermit`], whose contract is unchanged. Move it into the work
/// and keep it until the work finishes, even across cancellation. Dropped
/// without [`LifecyclePermit::complete`], it revokes this run's clean-shutdown
/// record for good: the next start is unclean and validates.
pub struct LifecyclePermit {
	inner: Arc<OwnerInner>,
	completed: bool,
}

impl LifecyclePermit {
	pub(crate) fn complete(mut self) {
		self.completed = true;
	}
}

impl Drop for LifecyclePermit {
	fn drop(&mut self) {
		if !self.completed {
			self.inner
				.clean_record_revoked
				.store(true, std::sync::atomic::Ordering::Release);
		}
		let mut state = self.inner.state.lock().unwrap();
		state.outstanding -= 1;
		if state.outstanding == 0 {
			self.inner.drained.notify_waiters();
		}
	}
}

/// ADR 0131 D2: admission for one manager-owned operation that issues many file
/// writes in sequence (a rehydration installer, a released-file settler). Like
/// [`LifecyclePermit`] it counts in the drain, so close waits for it; it is
/// refused once close began; and dropped without completing, it revokes this
/// run's clean-shutdown record. Its body runs through [`AdmittedOperation::run`]:
/// inside that scope, [`DataDirectoryOwner::write_permit`] for the same owner
/// mints a child permit in the submitting task even while the owner closes, so
/// the operation finishes what it started. The child is an ordinary counted
/// [`WritePermit`]: it moves with its I/O and outlives the parent if it must.
/// Nothing outside the scope gains anything.
pub(crate) struct AdmittedOperation {
	inner: Arc<OwnerInner>,
	completed: bool,
}

tokio::task_local! {
	/// The owner whose admitted operation the current task's body runs under.
	static ADMITTED_OPERATION: AdmittedScope;
}

/// The scope's view of its parent: which owner it belongs to. Built only by
/// [`AdmittedOperation::run`], which holds the parent for the scope's whole life,
/// so a scope exists only while its parent is outstanding.
struct AdmittedScope {
	owner: std::sync::Weak<OwnerInner>,
}

impl AdmittedOperation {
	/// Runs `body` inside this operation's scope, then completes the operation.
	/// Cancelled before `body` ends, the operation is dropped uncompleted.
	pub(crate) async fn run<F: std::future::Future>(self, body: F) -> F::Output {
		let scope = AdmittedScope {
			owner: Arc::downgrade(&self.inner),
		};
		let output = ADMITTED_OPERATION.scope(scope, body).await;
		self.complete();
		output
	}

	/// Completes the operation. A close settler (ADR 0132), which issues no
	/// write of its own and only mints children, completes its operation here
	/// once it has its result, whatever that result is.
	pub(crate) fn complete(mut self) {
		self.completed = true;
	}

	/// ADR 0132 D2: a counted operation of the same owner, minted from this one
	/// while it is outstanding (as a child write permit is), and admitted while
	/// the owner closes. Every new ordinary admission stays fenced. The child
	/// is counted on its own, so the drain waits for it even after its parent
	/// completed; its body runs through [`AdmittedOperation::run`] like any other.
	pub(crate) fn child_operation(&self) -> AdmittedOperation {
		let mut state = self
			.inner
			.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		state.outstanding += 1;
		AdmittedOperation {
			inner: self.inner.clone(),
			completed: false,
		}
	}
}

/// ADR 0132 D2: what a clean close owes after its first drain, settled by one
/// party the owner keeps for the whole run. `close` calls `settle` once per
/// close attempt that reaches the settle phase, in its own task, with an
/// admitted operation that `close` alone mints and a transient backend whose
/// writes are charged to this owner; a settler never stores that backend.
/// `Ok` means nothing the settler owes is left, so a repeated settle finds
/// nothing to do. A settler holds no owner and no owner-bound backend of its
/// own, so an owner dropped without close is still freed with its lock.
pub(crate) trait CloseSettler: Send + Sync {
	fn settle(
		self: Arc<Self>,
		operation: AdmittedOperation,
		io: Arc<dyn crate::storage::file_ops::AsyncFileOps>,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>>;
}

impl Drop for AdmittedOperation {
	fn drop(&mut self) {
		if !self.completed {
			self.inner
				.clean_record_revoked
				.store(true, std::sync::atomic::Ordering::Release);
		}
		let mut state = self
			.inner
			.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
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
	// ADR 0699 S5a: a lifecycle operation was interrupted. Sticky for the run:
	// no later completion clears it, so this run leaves no clean record.
	clean_record_revoked: std::sync::atomic::AtomicBool,
	// ADR 0699 S5a: an unclean start owes the certified base's physical proof;
	// until it is discharged no close records the directory clean, whatever
	// other recovery finished.
	base_recovery_pending: std::sync::atomic::AtomicBool,
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

	/// ADR 0699 S5a: this run must prove its certified base before a close may
	/// record the directory clean.
	pub(crate) fn require_base_recovery(&self) {
		self.inner
			.base_recovery_pending
			.store(true, std::sync::atomic::Ordering::Release);
	}

	/// The certified base was proved for this run. Only a successful base
	/// validation calls this; it never clears a sticky revocation.
	pub(crate) fn complete_base_recovery(&self) {
		self.inner
			.base_recovery_pending
			.store(false, std::sync::atomic::Ordering::Release);
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
			|| self
				.inner
				.clean_record_revoked
				.load(std::sync::atomic::Ordering::Acquire)
			|| self
				.inner
				.base_recovery_pending
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

	/// ADR 0699 S5a: a lifecycle permit; refused once close began.
	pub(crate) fn lifecycle_permit(&self) -> std::io::Result<LifecyclePermit> {
		let mut state = self.inner.state.lock().unwrap();
		if state.phase != OwnerPhase::Active {
			return Err(closed_error());
		}
		state.outstanding += 1;
		Ok(LifecyclePermit {
			inner: self.inner.clone(),
			completed: false,
		})
	}

	/// Whether a lifecycle operation was interrupted during this run.
	#[cfg(test)]
	pub(crate) fn clean_record_revoked(&self) -> bool {
		self.inner
			.clean_record_revoked
			.load(std::sync::atomic::Ordering::Acquire)
	}

	/// ADR 0131 D2: an admitted operation; refused once close began.
	pub(crate) fn admitted_operation(&self) -> std::io::Result<AdmittedOperation> {
		let mut state = self.inner.state.lock().unwrap();
		if state.phase != OwnerPhase::Active {
			return Err(closed_error());
		}
		state.outstanding += 1;
		Ok(AdmittedOperation {
			inner: self.inner.clone(),
			completed: false,
		})
	}

	/// Whether the current task runs inside an admitted operation of this owner.
	fn in_admitted_scope(&self) -> bool {
		ADMITTED_OPERATION
			.try_with(|scope| std::ptr::eq(scope.owner.as_ptr(), Arc::as_ptr(&self.inner)))
			.unwrap_or(false)
	}

	/// The single admission every write reaches, in the submitting task. While
	/// the owner closes, only a child of an admitted operation of this owner is
	/// admitted (ADR 0131 D2); the drain counts it like any other permit.
	pub(crate) fn write_permit(&self) -> std::io::Result<WritePermit> {
		let mut state = self.inner.state.lock().unwrap();
		let admitted = match state.phase {
			OwnerPhase::Active => true,
			OwnerPhase::Closing => self.in_admitted_scope(),
			OwnerPhase::Closed => false,
		};
		if !admitted {
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

	/// ADR 0132 D2: `settler` settles what a clean close of this owner owes,
	/// after the close's first drain. Refused once close began.
	pub(crate) fn register_close_settler(
		&self,
		settler: Arc<dyn CloseSettler>,
	) -> std::io::Result<()> {
		let mut state = self.inner.state.lock().unwrap();
		if state.phase != OwnerPhase::Active {
			return Err(closed_error());
		}
		state.settlers.push(settler);
		Ok(())
	}

	/// ADR 0132 D2: the one admitted operation of one settler's settle. Minted
	/// only by the settle phase of `close`, after its first drain, so it is
	/// admitted while the owner closes; it counts in the second drain.
	fn settle_operation(&self) -> AdmittedOperation {
		let mut state = self.inner.state.lock().unwrap();
		state.outstanding += 1;
		AdmittedOperation {
			inner: self.inner.clone(),
			completed: false,
		}
	}

	/// Waits until nothing admitted is outstanding: no write or lifecycle
	/// permit, no admitted operation or child, and no startup drain barrier.
	async fn drain(&self) {
		loop {
			let notified = self.inner.drained.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			{
				let state = self.inner.state.lock().unwrap();
				if state.outstanding == 0 && state.startup_barriers == 0 {
					return;
				}
			}
			notified.await;
		}
	}

	/// ADR 0132 D2: the settle phase of a close, between its two drains. Each
	/// registered settler, one at a time, gets one admitted operation and a
	/// transient backend charged to this owner, which no settler keeps, and
	/// settles in its own task: a close that times out or is cancelled leaves
	/// it running with its operation, which the next drain waits for. Any
	/// settler's error fails this close attempt; a repeat settles again.
	async fn settle_registered(&self) -> std::io::Result<()> {
		let settlers = self.inner.state.lock().unwrap().settlers.clone();
		for settler in settlers {
			let operation = self.settle_operation();
			let io: Arc<dyn crate::storage::file_ops::AsyncFileOps> = Arc::new(
				crate::storage::file_ops::BlockingFileOps::with_owner(self.clone()),
			);
			tokio::spawn(settler.settle(operation, io))
				.await
				.map_err(|error| {
					Error::other(format!("a close settler ended without a result: {error}"))
				})??;
		}
		Ok(())
	}

	/// Whether a close of this owner recorded its settle as complete.
	#[cfg(test)]
	pub(crate) fn close_settled_for_test(&self) -> bool {
		self.inner.state.lock().unwrap().settled
	}

	// Deterministic timeout controls must reach their held worker before any
	// filesystem closer can consume the deliberately tiny observation budget.
	#[cfg(test)]
	pub(crate) fn register_first_test_worker(&self, worker: Arc<dyn StorageWorker>) {
		let mut state = self.inner.state.lock().unwrap();
		assert!(state.phase == OwnerPhase::Active);
		state.workers.insert(0, worker);
	}

	/// Reject new writes, drain admitted operations, settle what the close owes
	/// (ADR 0132), drain again and join registered workers. One deadline, taken
	/// on entry, bounds the whole close, its wait for the close gate included,
	/// so overlapping callers each wait at most their own bound. Timeout/failure
	/// leaves the descriptor held and admission closed. Repeating close drains
	/// again and settles again until a close recorded its settle, then resumes
	/// the same joins; cancellation never treats a live join as done.
	pub async fn close(&self, bound: Duration) -> std::io::Result<()> {
		// A bound past the clock's range (`Duration::MAX`, say) is no deadline.
		let deadline = tokio::time::Instant::now().checked_add(bound);
		self.begin_close();
		// The gate is taken inside the bounded close, so a caller waiting for an
		// overlapping close never waits past its own deadline.
		let closing = async {
			let _close = self.inner.close_gate.lock().await;
			{
				let mut state = self.inner.state.lock().unwrap();
				if state.phase == OwnerPhase::Closed {
					return Ok(());
				}
				state.phase = OwnerPhase::Closing;
				state.closing_pin = Some(self.inner.clone());
				self.inner.drained.notify_waiters();
			}
			self.drain().await;
			let settled = self.inner.state.lock().unwrap().settled;
			if !settled {
				// After the first drain no installer or handoff can add an
				// obligation; the settle's own operations count in the second.
				self.settle_registered().await?;
				self.drain().await;
				self.inner.state.lock().unwrap().settled = true;
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
			// Every writer has stopped and every settler settled. Record that
			// before releasing the lock so the next owner can tell a complete
			// shutdown from a crash or kill. Failure only costs the next boot a
			// recovery pass.
			if !self.may_record_clean_shutdown() || !state.settled {
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
			state.settlers.clear();
			self.inner
				.closed_signal
				.store(true, std::sync::atomic::Ordering::Release);
			Ok(())
		};
		let result = match deadline {
			Some(deadline) => tokio::time::timeout_at(deadline, closing).await,
			None => Ok(closing.await),
		};
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
		Self::acquire(data_dir.as_ref(), false, &|_| Ok(()))
	}

	/// Acquire a directory for an authorized restore or profile reset entry point.
	/// This is the only variant that creates the data-directory path.
	pub fn acquire_or_create(data_dir: impl AsRef<Path>) -> std::io::Result<Self> {
		Self::acquire(data_dir.as_ref(), true, &|_| Ok(()))
	}

	/// `acquire_existing` for a daemon start: `admit` judges the directory before
	/// its sentinel is opened (so a refused directory gains no `LOCK`) and again
	/// under the exclusive lock, before the clean-shutdown record is consumed. A
	/// refusal therefore changes no byte of the store, and its own binary still
	/// starts it clean.
	pub fn acquire_existing_admitted(
		data_dir: impl AsRef<Path>,
		admit: impl Fn(&Path) -> std::io::Result<()>,
	) -> std::io::Result<Self> {
		Self::acquire(data_dir.as_ref(), false, &admit)
	}

	/// `acquire_or_create` with the admission of [`Self::acquire_existing_admitted`].
	pub fn acquire_or_create_admitted(
		data_dir: impl AsRef<Path>,
		admit: impl Fn(&Path) -> std::io::Result<()>,
	) -> std::io::Result<Self> {
		Self::acquire(data_dir.as_ref(), true, &admit)
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

	fn acquire(
		data_dir: &Path,
		create: bool,
		admit: &dyn Fn(&Path) -> std::io::Result<()>,
	) -> std::io::Result<Self> {
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
		admit(&canonical)?;
		acquire_lock(canonical, admit)
	}
}

#[cfg(unix)]
fn acquire_lock(
	canonical: PathBuf,
	admit: &dyn Fn(&Path) -> std::io::Result<()>,
) -> std::io::Result<DataDirectoryOwner> {
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
	// The admission again, now that no other owner can change the directory: a
	// refusal returns here, unlocking, before the clean-shutdown record is read.
	admit(&canonical)?;
	let previous_shutdown_clean =
		consume_clean_shutdown_record(&canonical)? || directory_is_fresh(&canonical)?;
	Ok(DataDirectoryOwner {
		inner: Arc::new(OwnerInner {
			clean_record_revoked: std::sync::atomic::AtomicBool::new(false),
			base_recovery_pending: std::sync::atomic::AtomicBool::new(false),
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
				settlers: Vec::new(),
				settled: false,
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

/// Wendelon ADR 0104 provisioning: the formation intent and the replacement token are written
/// beside a never-owned store before its first start. Their content, admission and identity are
/// the startup paths' to judge; here they are only not evidence that the store was used.
const PROVISIONING_FILES: [&str; 2] = ["FORMATION_INTENT", "REPLACEMENT_TOKEN"];

/// A provisioning file by its exact name AND as a regular file itself: a directory or a symlink
/// of that name is not one, so it cannot hide other state.
fn is_provisioning_file(entry: &fs::DirEntry) -> std::io::Result<bool> {
	let name = entry.file_name();
	Ok(PROVISIONING_FILES
		.iter()
		.any(|provisioned| name == *provisioned)
		&& entry.file_type()?.is_file())
}

fn directory_is_fresh(dir: &Path) -> std::io::Result<bool> {
	for entry in fs::read_dir(dir)? {
		let entry = entry?;
		let name = entry.file_name();
		if name != "LOCK"
			&& name != ".CLEAN_SHUTDOWN.tmp"
			&& !is_provisioning_file(&entry)?
			// ADR 0726: an enrolled root KEK directory is setup, not a store.
			&& !crate::crypto::is_root_kek_artifact_dir(&entry.path())?
		{
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
fn acquire_lock(
	canonical: PathBuf,
	_admit: &dyn Fn(&Path) -> std::io::Result<()>,
) -> std::io::Result<DataDirectoryOwner> {
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

	/// A refused admission leaves the directory as it was, its clean-shutdown record
	/// included, and releases the lock; a later admitted start finds the stop clean.
	/// A refused directory that never held a sentinel gains none.
	#[tokio::test]
	async fn a_refused_admission_keeps_the_clean_record_and_the_lock_is_released() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		owner.close(Duration::from_secs(5)).await.unwrap();
		let record = dir.path().join(CLEAN_SHUTDOWN_RECORD);
		assert!(record.exists(), "a clean close");
		let refuse = |_: &Path| Err(Error::new(ErrorKind::InvalidData, "foreign"));
		let err = DataDirectoryOwner::acquire_existing_admitted(dir.path(), refuse).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(record.exists(), "the refusal kept the clean record");
		let admitted =
			DataDirectoryOwner::acquire_existing_admitted(dir.path(), |_| Ok(())).unwrap();
		assert!(
			admitted.previous_shutdown_clean(),
			"the stop is still clean"
		);
		assert!(!record.exists(), "an admitted start consumes it");
		drop(admitted);

		// A directory that changes between the unlocked and the locked judgement is
		// refused by the locked one, before the record is read.
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		owner.close(Duration::from_secs(5)).await.unwrap();
		let calls = std::sync::atomic::AtomicUsize::new(0);
		let changed = |_: &Path| {
			if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
				Ok(())
			} else {
				Err(Error::new(ErrorKind::InvalidData, "changed under the lock"))
			}
		};
		assert!(DataDirectoryOwner::acquire_existing_admitted(dir.path(), changed).is_err());
		assert_eq!(
			calls.load(std::sync::atomic::Ordering::SeqCst),
			2,
			"judged twice"
		);
		assert!(record.exists(), "the locked refusal kept the clean record");

		let bare = TestDir::new();
		assert!(DataDirectoryOwner::acquire_existing_admitted(bare.path(), refuse).is_err());
		assert!(
			!bare.path().join("LOCK").exists(),
			"no sentinel for a refused directory"
		);
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

	/// DC-282: provisioning beside a never-owned store is not use of the store, by its exact
	/// name and only as a regular file; any evidence of use beside it, with no clean record,
	/// still starts unclean, and a directory or a symlink of a provisioning name hides nothing.
	#[tokio::test]
	async fn provisioning_is_not_store_use_and_store_evidence_beside_it_still_starts_unclean() {
		let previous_clean = |prepare: &dyn Fn(&Path)| {
			let dir = TestDir::new();
			prepare(dir.path());
			let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
			owner.previous_shutdown_clean()
		};
		let intent = |dir: &Path| fs::write(dir.join("FORMATION_INTENT"), b"").unwrap();
		let token = |dir: &Path| fs::write(dir.join("REPLACEMENT_TOKEN"), b"00\n").unwrap();
		// Each provisioning file alone, as a regular file: a never-owned store.
		assert!(previous_clean(&intent), "a formation intent alone");
		assert!(previous_clean(&token), "a replacement token alone");
		// Evidence of use beside either, and no clean record: unclean, as before.
		for evidence in [
			"PARENT_DECISIONS",
			".PARENT_DECISIONS.tmp",
			"control.wal",
			"control.wal.00000000000000000000.seg",
			"vote.wnd",
			"snapshot.wnd",
			"storage_index.redb",
			"slab_0000000000000000.wnd",
			"audit.ndjson",
			"FORMATION_INTENT.old",
			"formation_intent",
		] {
			for provision in [&intent as &dyn Fn(&Path), &token] {
				assert!(
					!previous_clean(&|dir: &Path| {
						provision(dir);
						fs::write(dir.join(evidence), b"x").unwrap();
					}),
					"{evidence} beside provisioning is store use"
				);
			}
		}
		// A directory or a symlink of a provisioning name is not a provisioning file.
		for name in ["FORMATION_INTENT", "REPLACEMENT_TOKEN"] {
			assert!(
				!previous_clean(&|dir: &Path| fs::create_dir(dir.join(name)).unwrap()),
				"an empty directory named {name}"
			);
			assert!(
				!previous_clean(&|dir: &Path| {
					fs::create_dir(dir.join(name)).unwrap();
					fs::write(dir.join(name).join("storage_index.redb"), b"x").unwrap();
				}),
				"a directory named {name} holding state"
			);
			assert!(
				!previous_clean(&|dir: &Path| {
					std::os::unix::fs::symlink("/dev/null", dir.join(name)).unwrap()
				}),
				"a symlink named {name}"
			);
		}
		// A clean record still decides for a used store, provisioning or not.
		let dir = TestDir::new();
		fs::write(dir.path().join("control.wal"), b"history").unwrap();
		intent(dir.path());
		fs::write(dir.path().join(CLEAN_SHUTDOWN_RECORD), b"").unwrap();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		assert!(
			owner.previous_shutdown_clean(),
			"a clean record beside a used store"
		);
		drop(owner);
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		assert!(
			!owner.previous_shutdown_clean(),
			"consumed: the next start is unclean"
		);
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

	/// ADR 0699 S5a: an interrupted lifecycle operation revokes the clean
	/// record for good; a later successful one does not restore it.
	#[tokio::test]
	async fn an_interrupted_lifecycle_permit_revokes_the_clean_record_for_good() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		// A store with data, not a never-owned directory.
		fs::write(dir.path().join("data.bin"), b"written").unwrap();
		let interrupted = owner.lifecycle_permit().unwrap();
		drop(interrupted);
		assert!(owner.clean_record_revoked());
		owner.lifecycle_permit().unwrap().complete();
		assert!(owner.clean_record_revoked(), "completion never clears it");
		owner.close(Duration::from_secs(5)).await.unwrap();
		drop(owner);
		assert!(!dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());
		let next = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		assert!(!next.previous_shutdown_clean(), "the next start is unclean");
	}

	/// A completed lifecycle permit leaves a clean close clean, and none is
	/// granted once close began; close waits for one in flight.
	#[tokio::test]
	async fn a_lifecycle_permit_is_drained_by_close_and_refused_after_it() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let held = owner.lifecycle_permit().unwrap();
		let closing = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.close(Duration::from_secs(5)).await })
		};
		tokio::task::yield_now().await;
		assert!(owner.lifecycle_permit().is_err(), "none after close began");
		assert!(!closing.is_finished(), "close waits for the permit");
		held.complete();
		closing.await.unwrap().unwrap();
		drop(owner);
		assert!(dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());
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

	/// ADR 0131 D2: close waits for an admitted operation; while it drains, the
	/// operation's own scope still gets child write permits, which close counts
	/// on their own, while every other admission is refused. Once the last
	/// child goes, close completes and records the directory clean.
	#[tokio::test]
	async fn an_admitted_operation_finishes_its_writes_while_its_owner_closes() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let operation = owner.admitted_operation().unwrap();
		let (entered, reached) = oneshot::channel();
		let (release, released) = oneshot::channel::<()>();
		let body_owner = owner.clone();
		let task = tokio::spawn(operation.run(async move {
			let _ = entered.send(());
			let _ = released.await;
			body_owner.write_permit()
		}));
		reached.await.unwrap();
		let closing = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.close(Duration::from_secs(5)).await })
		};
		owner.wait_until_closing().await;
		assert!(
			owner.write_permit().is_err(),
			"a write outside the scope is refused"
		);
		assert!(owner.admitted_operation().is_err(), "no new operation");
		assert!(owner.lifecycle_permit().is_err(), "no new lifecycle permit");
		let outside = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.write_permit().is_err() })
		};
		assert!(
			outside.await.unwrap(),
			"another task of the owner is refused"
		);
		assert!(!closing.is_finished(), "close waits for the operation");
		release.send(()).unwrap();
		let child = task
			.await
			.unwrap()
			.expect("the operation's scope mints a child permit while its owner closes");
		for _ in 0..4 {
			tokio::task::yield_now().await;
		}
		assert!(
			!closing.is_finished(),
			"the child is counted on its own after its parent completed"
		);
		drop(child);
		closing.await.unwrap().unwrap();
		assert!(!owner.clean_record_revoked());
		drop(owner);
		assert!(dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());
	}

	/// ADR 0131 D2: a scope admits only its own owner's children; a task it
	/// spawns gains nothing; and an operation dropped before completing revokes
	/// the clean-shutdown record, as an interrupted lifecycle permit does.
	#[tokio::test]
	async fn an_admitted_operation_scope_is_narrow_and_its_interruption_revokes() {
		let closing_dir = TestDir::new();
		let closing = DataDirectoryOwner::acquire_existing(closing_dir.path()).unwrap();
		let other_dir = TestDir::new();
		let other = DataDirectoryOwner::acquire_existing(other_dir.path()).unwrap();
		let held = closing.admitted_operation().unwrap();
		closing.begin_close();
		let foreign = other.admitted_operation().unwrap();
		let refused = {
			let closing = closing.clone();
			foreign
				.run(async move { closing.write_permit().is_err() })
				.await
		};
		assert!(refused, "another owner's scope admits nothing here");
		let spawned = {
			let closing = closing.clone();
			held.run(async move {
				let own = closing.write_permit().map(drop).is_ok();
				let spawned = tokio::spawn(async move { closing.write_permit().is_err() })
					.await
					.unwrap();
				own && spawned
			})
			.await
		};
		assert!(
			spawned,
			"the scope's own child is admitted; a task it spawns is not"
		);
		closing.close(Duration::from_secs(5)).await.unwrap();
		assert!(closing_dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());

		// A store with data, not a never-owned directory.
		fs::write(other_dir.path().join("data.bin"), b"written").unwrap();
		drop(other.admitted_operation().unwrap());
		assert!(
			other.clean_record_revoked(),
			"an interrupted operation revokes"
		);
		other.close(Duration::from_secs(5)).await.unwrap();
		drop(other);
		assert!(!other_dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());
	}

	type SettleFuture =
		std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>>;
	/// The transient backend a close passes to its settlers.
	type Io = Arc<dyn crate::storage::AsyncFileOps>;
	type SettleFn = dyn Fn(AdmittedOperation, Io) -> SettleFuture + Send + Sync;

	/// A close settler for the owner-level controls (ADR 0132): its settle runs
	/// the closure with the operation and the transient backend close passed.
	struct FnSettler(Box<SettleFn>);

	impl CloseSettler for FnSettler {
		fn settle(self: Arc<Self>, operation: AdmittedOperation, io: Io) -> SettleFuture {
			(self.0)(operation, io)
		}
	}

	fn settler<F, Fut>(settle: F) -> Arc<dyn CloseSettler>
	where
		F: Fn(AdmittedOperation, Io) -> Fut + Send + Sync + 'static,
		Fut: std::future::Future<Output = std::io::Result<()>> + Send + 'static,
	{
		Arc::new(FnSettler(Box::new(move |operation, io| {
			Box::pin(settle(operation, io)) as SettleFuture
		})))
	}

	/// A settler that counts its settles and completes its operation, failing
	/// while `failures` remain.
	fn counting_settler(
		settles: Arc<std::sync::atomic::AtomicUsize>,
		failures: Arc<std::sync::atomic::AtomicUsize>,
	) -> Arc<dyn CloseSettler> {
		settler(move |operation, _io| {
			let settles = settles.clone();
			let failures = failures.clone();
			async move {
				settles.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
				operation.complete();
				let fail = failures
					.fetch_update(
						std::sync::atomic::Ordering::SeqCst,
						std::sync::atomic::Ordering::SeqCst,
						|left| left.checked_sub(1),
					)
					.is_ok();
				if fail {
					return Err(Error::other("injected settle failure"));
				}
				Ok(())
			}
		})
	}

	/// A worker whose join runs `observe`, waits for one `release` if it has
	/// a hold (signalling `entered` first), then fails while `failures` remain.
	struct ObservingWorker {
		observe: Box<dyn Fn() + Send + Sync>,
		hold: Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>,
		failures: std::sync::atomic::AtomicUsize,
	}

	#[async_trait::async_trait]
	impl StorageWorker for ObservingWorker {
		async fn close_and_join(&self) -> std::io::Result<()> {
			(self.observe)();
			if let Some((entered, release)) = &self.hold {
				entered.add_permits(1);
				release.acquire().await.unwrap().forget();
			}
			let fail = self
				.failures
				.fetch_update(
					std::sync::atomic::Ordering::SeqCst,
					std::sync::atomic::Ordering::SeqCst,
					|left| left.checked_sub(1),
				)
				.is_ok();
			if fail {
				return Err(Error::other("injected worker join failure"));
			}
			Ok(())
		}
	}

	fn counted(counter: &Arc<std::sync::atomic::AtomicUsize>) -> usize {
		counter.load(std::sync::atomic::Ordering::SeqCst)
	}

	fn shutdown_incomplete(error: &Error) -> bool {
		error
			.get_ref()
			.and_then(|source| source.downcast_ref::<ShutdownIncomplete>())
			.is_some()
	}

	/// ADR 0132 D2: the settle runs after the first drain (a held permit delays
	/// it), once, and before the joins and the clean-shutdown record; a closed
	/// owner settles nothing again.
	#[tokio::test]
	async fn a_close_settles_after_its_first_drain_and_before_its_joins_and_record() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let record = dir.path().join(CLEAN_SHUTDOWN_RECORD);
		let settles = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		{
			let settles = settles.clone();
			let record = record.clone();
			owner
				.register_close_settler(settler(move |operation, _io| {
					let settles = settles.clone();
					let unrecorded = !record.exists();
					async move {
						assert!(unrecorded, "no clean-shutdown record before the settle");
						settles.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
						operation.complete();
						Ok(())
					}
				}))
				.unwrap();
		}
		let seen_at_join = Arc::new(Mutex::new(None));
		{
			let seen = seen_at_join.clone();
			let settles = settles.clone();
			owner
				.register_worker(Arc::new(ObservingWorker {
					observe: Box::new(move || *seen.lock().unwrap() = Some(counted(&settles))),
					hold: None,
					failures: std::sync::atomic::AtomicUsize::new(0),
				}))
				.unwrap();
		}
		let permit = owner.write_permit().unwrap();
		let closing = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.close(Duration::from_secs(5)).await })
		};
		owner.wait_until_closing().await;
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
		assert_eq!(counted(&settles), 0, "the settle waits for the first drain");
		assert!(!closing.is_finished());
		drop(permit);
		closing.await.unwrap().unwrap();
		assert_eq!(counted(&settles), 1, "one settle");
		assert_eq!(
			*seen_at_join.lock().unwrap(),
			Some(1),
			"the joins follow the settle"
		);
		assert!(owner.close_settled_for_test());
		assert!(record.exists());
		owner.close(Duration::from_secs(5)).await.unwrap();
		assert_eq!(counted(&settles), 1, "a closed owner settles nothing again");
	}

	/// ADR 0132 D2: a settler's child operation is admitted while the owner
	/// closes, and so are its scope's write permits. The second drain waits for
	/// a child that outlives its settle: neither the clean-shutdown record nor
	/// the lock's release comes before the child's last permit goes.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn the_second_drain_waits_for_a_settle_child_that_outlives_its_settle() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let record = dir.path().join(CLEAN_SHUTDOWN_RECORD);
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		{
			let (entered, release) = (entered.clone(), release.clone());
			owner
				.register_close_settler(settler(move |operation, io| {
					let (entered, release) = (entered.clone(), release.clone());
					async move {
						let child = operation.child_operation();
						tokio::spawn(child.run(async move {
							let permit = io
								.write_permit()
								.expect("the child's scope is admitted while the owner closes")
								.expect("the transient backend is owned");
							entered.add_permits(1);
							release.acquire().await.unwrap().forget();
							drop(permit);
						}));
						operation.complete();
						Ok(())
					}
				}))
				.unwrap();
		}
		let closing = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.close(Duration::from_secs(10)).await })
		};
		tokio::time::timeout(Duration::from_secs(5), entered.acquire())
			.await
			.expect("the child runs")
			.unwrap()
			.forget();
		tokio::time::sleep(Duration::from_millis(100)).await;
		assert!(
			!closing.is_finished(),
			"the second drain waits for the child"
		);
		assert!(!record.exists(), "no record before the child's last permit");
		assert!(
			DataDirectoryOwner::acquire_existing(dir.path()).is_err(),
			"the lock is held"
		);
		release.add_permits(1);
		closing.await.unwrap().unwrap();
		assert!(record.exists());
		assert!(!owner.clean_record_revoked());
		drop(owner);
		DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
	}

	/// ADR 0132 K11: during the settle phase every new ordinary admission is
	/// refused: a write outside a scope (directly and through the transient
	/// backend), an admitted operation, a lifecycle permit, a startup drain and
	/// a new settler. Only the settle's own child scope is admitted.
	#[tokio::test]
	async fn the_settle_phase_admits_only_its_own_children() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		// Taken by the settle, so the settler keeps no owner after it.
		let probe = Arc::new(Mutex::new(Some(owner.clone())));
		let seen = Arc::new(Mutex::new(Vec::new()));
		{
			let (probe, seen) = (probe.clone(), seen.clone());
			owner
				.register_close_settler(settler(move |operation, io| {
					let (probe, seen) = (probe.clone(), seen.clone());
					async move {
						let owner = probe.lock().unwrap().take().expect("one settle");
						let late = counting_settler(Default::default(), Default::default());
						let mut checks = vec![
							("a write outside a scope", owner.write_permit().is_err()),
							(
								"a backend write outside a scope",
								io.write_permit().is_err(),
							),
							("an admitted operation", owner.admitted_operation().is_err()),
							("a lifecycle permit", owner.lifecycle_permit().is_err()),
							("a startup drain", owner.reserve_startup_drain().is_err()),
							("a settler", owner.register_close_settler(late).is_err()),
						];
						let admitted = operation
							.child_operation()
							.run(async { owner.write_permit().map(drop).is_ok() })
							.await;
						checks.push(("the settle's own child is admitted", admitted));
						*seen.lock().unwrap() = checks;
						operation.complete();
						Ok(())
					}
				}))
				.unwrap();
		}
		owner.close(Duration::from_secs(5)).await.unwrap();
		let seen = seen.lock().unwrap().clone();
		assert_eq!(seen.len(), 7, "{seen:?}");
		for (what, held) in seen {
			assert!(held, "{what}");
		}
		assert!(!owner.clean_record_revoked());
		assert!(dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());
	}

	/// ADR 0132 D2: a settler's error fails the close before any join and
	/// leaves no record, with the lock held; the repeat drains, settles again
	/// and completes. The failed settle completed its operation, so the run
	/// still records a clean shutdown.
	#[tokio::test]
	async fn a_failed_settle_joins_nothing_and_its_repeat_settles_again() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let record = dir.path().join(CLEAN_SHUTDOWN_RECORD);
		let settles = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let failures = Arc::new(std::sync::atomic::AtomicUsize::new(1));
		owner
			.register_close_settler(counting_settler(settles.clone(), failures))
			.unwrap();
		let joins = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		{
			let joins = joins.clone();
			owner
				.register_worker(Arc::new(ObservingWorker {
					observe: Box::new(move || {
						joins.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
					}),
					hold: None,
					failures: std::sync::atomic::AtomicUsize::new(0),
				}))
				.unwrap();
		}
		let error = owner.close(Duration::from_secs(5)).await.unwrap_err();
		assert!(shutdown_incomplete(&error), "{error}");
		assert_eq!(counted(&settles), 1);
		assert_eq!(counted(&joins), 0, "no join after a failed settle");
		assert!(!owner.close_settled_for_test());
		assert!(!record.exists());
		assert!(DataDirectoryOwner::acquire_existing(dir.path()).is_err());
		owner.close(Duration::from_secs(5)).await.unwrap();
		assert_eq!(counted(&settles), 2, "the repeat settles again");
		assert_eq!(counted(&joins), 1);
		assert!(!owner.clean_record_revoked());
		assert!(record.exists());
	}

	/// ADR 0132 K6: a join that fails after the settle recorded, then a close
	/// timed out in its joins: each repeat skips the settle and resumes the
	/// joins, and the clean-shutdown record follows the one that completes.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn a_repeat_after_the_settle_skips_it_and_resumes_the_joins() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let record = dir.path().join(CLEAN_SHUTDOWN_RECORD);
		let settles = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		owner
			.register_close_settler(counting_settler(settles.clone(), Default::default()))
			.unwrap();
		owner
			.register_worker(Arc::new(ObservingWorker {
				observe: Box::new(|| {}),
				hold: None,
				failures: std::sync::atomic::AtomicUsize::new(1),
			}))
			.unwrap();
		let (entered, release) = (
			Arc::new(tokio::sync::Semaphore::new(0)),
			Arc::new(tokio::sync::Semaphore::new(0)),
		);
		owner
			.register_worker(Arc::new(ObservingWorker {
				observe: Box::new(|| {}),
				hold: Some((entered.clone(), release.clone())),
				failures: std::sync::atomic::AtomicUsize::new(0),
			}))
			.unwrap();
		// The first close settles, then its first join fails.
		let first = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.close(Duration::from_secs(10)).await })
		};
		tokio::time::timeout(Duration::from_secs(5), entered.acquire())
			.await
			.expect("the joins run")
			.unwrap()
			.forget();
		release.add_permits(1);
		let error = first.await.unwrap().unwrap_err();
		assert!(shutdown_incomplete(&error), "{error}");
		assert_eq!(counted(&settles), 1);
		assert!(owner.close_settled_for_test(), "the settle stays recorded");
		assert!(!record.exists());
		// The second times out in the held join: cancelled there.
		let error = owner.close(Duration::from_millis(300)).await.unwrap_err();
		assert!(shutdown_incomplete(&error), "{error}");
		assert_eq!(error.kind(), ErrorKind::TimedOut);
		entered.acquire().await.unwrap().forget();
		assert_eq!(counted(&settles), 1, "the repeat skipped the settle");
		assert!(!record.exists());
		assert!(DataDirectoryOwner::acquire_existing(dir.path()).is_err());
		// The third resumes the joins and completes.
		let third = {
			let owner = owner.clone();
			tokio::spawn(async move { owner.close(Duration::from_secs(10)).await })
		};
		tokio::time::timeout(Duration::from_secs(5), entered.acquire())
			.await
			.expect("the joins resume")
			.unwrap()
			.forget();
		release.add_permits(1);
		third.await.unwrap().unwrap();
		assert_eq!(counted(&settles), 1, "one settle in all");
		assert!(record.exists());
		drop(owner);
		DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
	}

	/// ADR 0132 K7: two closes overlap while the settle is held. Each returns
	/// within its own bound, the one waiting at the close gate included; the
	/// held settle keeps running, and a later close completes with exactly one
	/// disposal of the one obligation.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn overlapping_closes_each_keep_their_own_deadline_and_dispose_once() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		let record = dir.path().join(CLEAN_SHUTDOWN_RECORD);
		let owed = Arc::new(std::sync::atomic::AtomicUsize::new(1));
		let disposals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let (entered, release) = (
			Arc::new(tokio::sync::Semaphore::new(0)),
			Arc::new(tokio::sync::Semaphore::new(0)),
		);
		{
			let (owed, disposals) = (owed.clone(), disposals.clone());
			let (entered, release) = (entered.clone(), release.clone());
			owner
				.register_close_settler(settler(move |operation, _io| {
					let (owed, disposals) = (owed.clone(), disposals.clone());
					let (entered, release) = (entered.clone(), release.clone());
					async move {
						if counted(&owed) > 0 {
							entered.add_permits(1);
							release.acquire().await.unwrap().forget();
						}
						if owed.swap(0, std::sync::atomic::Ordering::SeqCst) == 1 {
							disposals.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
						}
						operation.complete();
						Ok(())
					}
				}))
				.unwrap();
		}
		let bound = Duration::from_millis(300);
		let timed_close = move |owner: DataDirectoryOwner| {
			tokio::spawn(async move {
				let started = tokio::time::Instant::now();
				let result = owner.close(bound).await;
				(result, started.elapsed())
			})
		};
		let first = timed_close(owner.clone());
		tokio::time::timeout(Duration::from_secs(5), entered.acquire())
			.await
			.expect("the first close reaches its settle")
			.unwrap()
			.forget();
		let second = timed_close(owner.clone());
		for (what, close) in [
			("the settling close", first),
			("the close at the gate", second),
		] {
			let (result, elapsed) = close.await.unwrap();
			let error = result.expect_err(what);
			assert!(shutdown_incomplete(&error), "{what}: {error}");
			assert!(
				elapsed < bound + Duration::from_secs(2),
				"{what} waited {elapsed:?}, past its own bound"
			);
		}
		assert_eq!(
			counted(&disposals),
			0,
			"the held settle has not disposed yet"
		);
		assert!(!record.exists());
		release.add_permits(1);
		owner.close(Duration::from_secs(5)).await.unwrap();
		assert_eq!(counted(&disposals), 1, "one disposal");
		assert!(record.exists());
	}

	/// ADR 0132 D1: an owner holding a settler and dropped without close is
	/// freed with its lock, as before: no settler holds its owner. Registration
	/// is refused once close began.
	#[tokio::test]
	async fn an_owner_dropped_with_a_settler_releases_its_lock_and_a_closing_one_refuses_settlers()
	{
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		owner
			.register_close_settler(counting_settler(Default::default(), Default::default()))
			.unwrap();
		drop(owner);
		let owner = DataDirectoryOwner::acquire_existing(dir.path())
			.expect("the dropped owner released its lock");
		owner.begin_close();
		assert!(
			owner
				.register_close_settler(counting_settler(Default::default(), Default::default()))
				.is_err()
		);
		owner.close(Duration::from_secs(5)).await.unwrap();
	}

	/// ADR 0132: a bound past the clock's range is no deadline rather than a
	/// panic; an idle owner closed with it completes and records a clean shutdown.
	#[tokio::test]
	async fn a_close_bounded_by_the_largest_duration_completes_clean() {
		let dir = TestDir::new();
		let owner = DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
		owner.close(Duration::MAX).await.unwrap();
		assert!(dir.path().join(CLEAN_SHUTDOWN_RECORD).exists());
		drop(owner);
		DataDirectoryOwner::acquire_existing(dir.path()).unwrap();
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
