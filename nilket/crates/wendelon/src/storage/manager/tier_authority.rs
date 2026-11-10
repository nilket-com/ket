//! Process-local verification of committed tier authority before any answer.
use super::*;
use crate::storage::ownership::StorageWorker;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{OnceLock, Weak};
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant as TokioInstant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PendingTierAuthority {
	pub(super) pointer: TieredSlabPointer,
	pub(super) last_control_log_index: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct TierAuthorityIndexConflict {
	pub(super) slab_id: SlabId,
	pub(super) index: u64,
}
impl std::fmt::Display for TierAuthorityIndexConflict {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"TierAuthorityIndexConflict(slab={}, index={}, RestartRequired)",
			self.slab_id, self.index
		)
	}
}
impl std::error::Error for TierAuthorityIndexConflict {}

/// Only this positively identified local refusal may be acknowledged to Raft.
#[derive(Debug)]
struct SnapshotAppendNotQuiescent(SlabId);
impl std::fmt::Display for SnapshotAppendNotQuiescent {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "snapshot append not quiescent in slab {}", self.0)
	}
}
impl std::error::Error for SnapshotAppendNotQuiescent {}
fn snapshot_append_not_quiescent(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|cause| cause.is::<SnapshotAppendNotQuiescent>())
}

/// Epoch identity makes releasing an obsolete retry's fence idempotent. It
/// cannot decrement the fence belonging to a newer snapshot installation.
#[derive(Default)]
struct SnapshotFences(std::sync::Mutex<std::collections::BTreeSet<u64>>);
impl SnapshotFences {
	fn load(&self, _order: Ordering) -> usize {
		self.0.lock().unwrap_or_else(|e| e.into_inner()).len()
	}
	fn insert(&self, epoch: u64) {
		self.0
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.insert(epoch);
	}
	fn release(&self, epoch: u64) {
		self.0
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.remove(&epoch);
	}
}

#[derive(Clone)]
struct SnapshotReconciliation {
	epoch: u64,
	source: crate::consensus::TierSnapshotSource,
	phase: SnapshotReconciliationPhase,
}
#[derive(Clone, Copy)]
enum SnapshotReconciliationPhase {
	Compare,
	Verify { revision: u64 },
	Failed,
}

#[derive(Clone, Debug)]
enum AuthorityStatus {
	Pending,
	Verifying,
	Retry { at: TokioInstant, kind: ErrorKind },
	Failed(ErrorKind),
	Ready,
	Closed,
}

/// The worker owns a weak manager reference. Its join handle stays in this
/// worker across cancellation of a bounded owner close.
pub(super) struct TierAuthority {
	manager: OnceLock<Weak<SlabManager>>,
	pub(super) pending: DashMap<SlabId, PendingTierAuthority>,
	integrity_failures: DashMap<SlabId, u64>,
	epoch: AtomicU64,
	ready: AtomicBool,
	retirement_fence: AtomicBool,
	snapshot_fences: SnapshotFences,
	/// The newest control index a begun comparison has read: delivered owner
	/// evidence at or below it is covered, whatever that comparison's outcome,
	/// because its readers hold at least that view. Only rises.
	comparison_read_index: std::sync::RwLock<Option<u64>>,
	/// A reopen found tier or retire operations with no durable decision. Only
	/// their receipts can resolve them, so nothing is served until they do.
	pub(super) unresolved_op_fence: AtomicBool,
	/// A follower holding unresolved operations (ADR 0697 §6 step 4).
	pub(super) awaiting_leadership: AtomicBool,
	snapshot_reconciliation: std::sync::Mutex<Option<SnapshotReconciliation>>,
	retirements: DashMap<SlabId, Option<u64>>,
	snapshot_retirements: DashMap<SlabId, (u64, Option<u64>)>,
	/// Decided retirements held for an install, by slab, with their log index. Reads of the slab
	/// refuse, and nothing else does: unlike a retirement fence, this can stand as long as the
	/// install takes, and it does not stop the install's own private completion or undo.
	deferred_retirements: DashMap<SlabId, u64>,
	closed: AtomicBool,
	exposed: AtomicBool,
	staging: AtomicUsize,
	completed_attempts: AtomicU64,
	/// This store's open ran under an unresolved intent, and what it skipped is not done
	/// yet. Set before the manager exists; cleared only by finishing that work.
	pub(super) deferred_open: AtomicBool,
	/// A recovered install's publication whose outcome could not be read, by job.
	pub(super) held_install_publications:
		std::sync::Mutex<HashMap<CompactionJobId, super::index_publication::IndexPublication>>,
	status: std::sync::Mutex<AuthorityStatus>,
	changed: Notify,
	task: Mutex<Option<JoinHandle<()>>>,
	abort: std::sync::Mutex<Option<AbortHandle>>,
	reconcile_task: Mutex<Option<JoinHandle<()>>>,
	reconcile_state: std::sync::Mutex<ReconcileState>,
	#[cfg(test)]
	pub(super) reconcile_before_mutation_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	#[cfg(test)]
	pub(super) reconcile_task_starts: AtomicUsize,
	#[cfg(test)]
	pub(super) snapshot_retirement_quarantine_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// Every verification flight `start()` spawned, so a test can count them
	/// and abort any that a lost single-flight check would leave detached.
	#[cfg(test)]
	pub(super) verification_flights_for_test: std::sync::Mutex<Vec<AbortHandle>>,
	/// Readers that reached and parked in the verification wait.
	#[cfg(test)]
	pub(super) verification_waits_for_test: AtomicUsize,
	/// Pauses a readiness wait between its ready check and its attempt check.
	#[cfg(test)]
	pub(super) wait_before_attempt_check_hold:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
}

#[derive(Default)]
struct ReconcileState {
	running: bool,
	rerun: bool,
	abort: Option<AbortHandle>,
}

struct BootstrapStaging<'a> {
	authority: &'a TierAuthority,
	finished: bool,
}

impl BootstrapStaging<'_> {
	fn finish(mut self) {
		self.finished = true;
	}
}

impl Drop for BootstrapStaging<'_> {
	fn drop(&mut self) {
		let mut status = self
			.authority
			.status
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		self.authority.staging.fetch_sub(1, Ordering::AcqRel);
		if self
			.authority
			.epoch
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
				epoch.checked_add(1)
			})
			.is_err()
		{
			self.authority.closed.store(true, Ordering::Release);
		}
		self.authority.ready.store(false, Ordering::Release);
		*status = if self.authority.closed.load(Ordering::Acquire) {
			AuthorityStatus::Closed
		} else if self.finished {
			AuthorityStatus::Pending
		} else {
			AuthorityStatus::Failed(ErrorKind::Other)
		};
		self.authority.changed.notify_waiters();
	}
}

impl TierAuthority {
	pub(super) fn new() -> Arc<Self> {
		Arc::new(Self {
			manager: OnceLock::new(),
			pending: DashMap::new(),
			integrity_failures: DashMap::new(),
			epoch: AtomicU64::new(1),
			ready: AtomicBool::new(false),
			retirement_fence: AtomicBool::new(false),
			snapshot_fences: SnapshotFences::default(),
			comparison_read_index: std::sync::RwLock::new(None),
			unresolved_op_fence: AtomicBool::new(false),
			awaiting_leadership: AtomicBool::new(false),
			snapshot_reconciliation: std::sync::Mutex::new(None),
			retirements: DashMap::new(),
			snapshot_retirements: DashMap::new(),
			deferred_retirements: DashMap::new(),
			closed: AtomicBool::new(false),
			exposed: AtomicBool::new(false),
			staging: AtomicUsize::new(0),
			completed_attempts: AtomicU64::new(0),
			deferred_open: AtomicBool::new(false),
			held_install_publications: std::sync::Mutex::new(HashMap::new()),
			status: std::sync::Mutex::new(AuthorityStatus::Pending),
			changed: Notify::new(),
			task: Mutex::new(None),
			abort: std::sync::Mutex::new(None),
			reconcile_task: Mutex::new(None),
			reconcile_state: std::sync::Mutex::new(ReconcileState::default()),
			#[cfg(test)]
			reconcile_before_mutation_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			reconcile_task_starts: AtomicUsize::new(0),
			#[cfg(test)]
			snapshot_retirement_quarantine_hold: std::sync::Mutex::new(None),
			#[cfg(test)]
			verification_flights_for_test: std::sync::Mutex::new(Vec::new()),
			#[cfg(test)]
			verification_waits_for_test: AtomicUsize::new(0),
			#[cfg(test)]
			wait_before_attempt_check_hold: std::sync::Mutex::new(None),
		})
	}

	#[cfg(test)]
	pub(super) fn retirement_pending_for_test(&self, id: SlabId, index: Option<u64>) -> bool {
		self.retirements
			.get(&id)
			.is_some_and(|entry| *entry.value() == index)
	}
	/// Wake the worker: local tier state it waits on changed.
	pub(super) fn notify_changed(&self) {
		self.changed.notify_waiters();
	}

	/// Whether a snapshot's tier reconciliation is still pending or fenced.
	pub(super) fn snapshot_reconciliation_pending(&self) -> bool {
		self.snapshot_fences.load(Ordering::Acquire) != 0
			|| self
				.snapshot_reconciliation
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.is_some()
	}
	#[cfg(test)]
	pub(super) fn snapshot_retry_pending_for_test(&self) -> bool {
		self.snapshot_reconciliation.lock().unwrap().is_some()
	}
	#[cfg(test)]
	pub(super) fn snapshot_fence_count_for_test(&self) -> usize {
		self.snapshot_fences.load(Ordering::Acquire)
	}
	#[cfg(test)]
	pub(super) fn release_snapshot_fence_for_test(&self, epoch: u64) {
		self.snapshot_fences.release(epoch);
	}
	#[cfg(test)]
	pub(super) fn closed_for_test(&self) -> bool {
		self.closed.load(Ordering::Acquire)
	}
	#[cfg(test)]
	pub(super) async fn joined_for_test(&self) -> bool {
		self.task.lock().await.is_none()
	}
	#[cfg(test)]
	pub(super) fn conflict_for_test(&self, slab_id: SlabId, index: u64) {
		let _ = self.note_index_conflict(slab_id, index);
	}

	pub(super) fn integrity_error(&self) -> Option<Error> {
		self.integrity_failures
			.iter()
			.map(|entry| (*entry.key(), *entry.value()))
			.min_by_key(|(slab, _)| *slab)
			.map(|(slab_id, index)| {
				Error::new(
					ErrorKind::InvalidData,
					TierAuthorityIndexConflict { slab_id, index },
				)
			})
	}

	fn note_index_conflict(&self, slab_id: SlabId, index: u64) -> Error {
		let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		self.integrity_failures.entry(slab_id).or_insert(index);
		self.invalidate_locked(&mut status);
		if !self.closed.load(Ordering::Acquire) {
			*status = AuthorityStatus::Failed(ErrorKind::InvalidData);
		}
		tracing::error!(
			slab_id = slab_id.get(),
			index,
			"conflicting tier pointers at one control index; restart required"
		);
		self.integrity_error().expect("latched conflict")
	}

	#[cfg(test)]
	pub(super) fn task_running_for_test(&self) -> Option<bool> {
		self.task
			.try_lock()
			.ok()
			.map(|task| task.as_ref().is_some_and(|task| !task.is_finished()))
	}

	#[cfg(test)]
	pub(super) fn completed_attempts_for_test(&self) -> u64 {
		self.completed_attempts.load(Ordering::Acquire)
	}

	#[cfg(test)]
	pub(super) fn status_for_test(&self) -> String {
		format!("{:?}", self.status())
	}

	fn status(&self) -> AuthorityStatus {
		self.status
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.clone()
	}

	fn supersede_snapshot_reconciliation(&self) {
		let old = self
			.snapshot_reconciliation
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.take();
		if let Some(old) = old {
			self.snapshot_fences.release(old.epoch);
			metrics()
				.storage_tier_snapshot_reconciliation_pending
				.sub(1);
		}
	}

	fn snapshot_reconciliation_source(&self) -> Option<crate::consensus::TierSnapshotSource> {
		self.snapshot_reconciliation
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.as_ref()
			.map(|retry| retry.source.clone())
	}

	fn snapshot_revision_matches(&self, revision: u64) -> bool {
		self.snapshot_reconciliation.lock().unwrap_or_else(|e|e.into_inner()).as_ref().is_none_or(|retry|matches!(retry.phase,SnapshotReconciliationPhase::Verify { revision: expected } if expected==revision))
	}

	fn clear_completed_snapshot_reconciliation(&self) {
		let mut current = self
			.snapshot_reconciliation
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if current
			.as_ref()
			.is_some_and(|retry| matches!(retry.phase, SnapshotReconciliationPhase::Verify { .. }))
		{
			current.take();
			metrics()
				.storage_tier_snapshot_reconciliation_pending
				.sub(1);
		}
	}

	async fn retry_snapshot_comparison(
		&self,
		retry: &SnapshotReconciliation,
	) -> std::io::Result<u64> {
		// No manager, owner or SM lease crosses backoff. The read lease excludes
		// canonical apply/install only for the local comparison, never L2 reads.
		let canonical = retry.source.read().await?;
		let manager = self
			.manager
			.get()
			.and_then(Weak::upgrade)
			.ok_or_else(crate::storage::ownership::closed_error)?;
		if self.closed.load(Ordering::Acquire) || manager.capture_snapshot_epoch() != retry.epoch {
			return Err(obsolete_verification());
		}
		// ADR 0697: only this store's own committed pointers; a view that is
		// not authoritative for this store never verifies.
		let mut pointers = manager
			.own_tier_view(canonical.tiers().iter(), canonical.incarnations())
			.ok_or_else(obsolete_verification)?;
		// MigrationComplete may arrive before this snapshot has registered any
		// local pending pointer for its metadata observer. Without a verified
		// row census, derive the same conservative summary from canonical
		// tombstones rather than restoring the older snapshot's None shortcut.
		if canonical.has_migrated_ranges() {
			for pointer in &mut pointers {
				pointer.tombstone_metadata = TombstoneMetadata::Untracked;
			}
		}

		let pending = self
			.pending
			.iter()
			.map(|entry| *entry.key())
			.collect::<Vec<_>>();
		let floor = pointers
			.iter()
			.map(|p| p.slab_id)
			.chain(pending.iter().copied())
			.max()
			.unwrap_or(SlabId::ZERO);
		for id in pending {
			manager
				.quarantine_pending_snapshot_collision(id, floor, retry.epoch)
				.await?;
		}
		let deferred = self
			.retirements
			.iter()
			.filter_map(|entry| {
				let index = (*entry.value())?;
				(canonical.index().is_some_and(|applied| applied >= index)
					&& !pointers.iter().any(|p| p.slab_id == *entry.key()))
				.then_some((*entry.key(), index))
			})
			.collect::<Vec<_>>();
		manager
			.finish_tier_snapshot_install_at_epoch(
				pointers,
				canonical.index(),
				retry.epoch,
				Some(&canonical),
			)
			.await?;
		#[cfg(test)]
		if !deferred.is_empty() {
			let hold = self
				.snapshot_retirement_quarantine_hold
				.lock()
				.unwrap()
				.take();
			if let Some((entered, release)) = hold {
				entered.add_permits(1);
				if let Ok(permit) = release.acquire().await {
					permit.forget();
				}
			}
		}
		// B8 completed the checked retirement and its durable accounting. Only
		// the matching log pre-hook fence remains; never clear another index.
		for (id, index) in deferred {
			let lock = manager.tier_publication_lock(id);
			let _publication = lock.lock().await;
			let _gate = manager.persistent_index_gate.clone().read_owned().await;
			let durable = manager
				.persistent_index
				.as_ref()
				.ok_or_else(|| Error::new(ErrorKind::WouldBlock, "persistent index unavailable"))?
				.load_snapshot()
				.await?;
			if manager.state.lock().await.tiered.contains_key(&id)
				|| self.pending.contains_key(&id)
				|| durable.tiered_slabs.iter().any(|p| p.slab_id == id)
			{
				return Err(Error::new(
					ErrorKind::InvalidData,
					"snapshot retirement remains unresolved",
				));
			}
			manager.clear_unapplied_tier_retirement(id, Some(index));
		}
		Ok(canonical.revision())
	}

	fn set_status(&self, status: AuthorityStatus) {
		*self.status.lock().unwrap_or_else(|e| e.into_inner()) = status;
		self.changed.notify_waiters();
	}

	pub(super) fn initialize(self: &Arc<Self>, manager: &Arc<SlabManager>) -> std::io::Result<()> {
		self.manager
			.set(Arc::downgrade(manager))
			.map_err(|_| Error::other("tier authority worker was already bound to a manager"))?;
		if let Some(owner) = &manager._directory_owner {
			owner.register_worker(self.clone())?;
		}
		Ok(())
	}

	fn is_ready(&self) -> bool {
		// An admission that sees the clear flag precedes the pre-mutation hook.
		// Its operation may finish against that earlier control state.
		self.integrity_failures.is_empty()
			&& !self.retirement_fence.load(Ordering::Acquire)
			&& self.snapshot_fences.load(Ordering::Acquire) == 0
			&& self.ready.load(Ordering::Acquire)
			&& !self.closed.load(Ordering::Acquire)
	}

	pub(super) fn mark_retirement(&self, slab_id: SlabId, index: Option<u64>) {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		self.retirement_fence.store(true, Ordering::Release);
		match self.retirements.entry(slab_id) {
			dashmap::mapref::entry::Entry::Vacant(entry) => {
				entry.insert(index);
			}
			dashmap::mapref::entry::Entry::Occupied(mut entry) => {
				let prior = *entry.get();
				if index.is_none() || matches!((prior, index), (Some(old), Some(new)) if new > old)
				{
					entry.insert(index);
				}
			}
		}
		self.changed.notify_waiters();
	}

	pub(super) fn clear_retirement(&self, slab_id: SlabId, index: Option<u64>) {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		if let dashmap::mapref::entry::Entry::Occupied(entry) = self.retirements.entry(slab_id) {
			if index.is_some() && *entry.get() == index {
				entry.remove();
			}
		}
		if self.retirements.is_empty() && self.snapshot_retirements.is_empty() {
			self.retirement_fence.store(false, Ordering::Release);
		}
		self.changed.notify_waiters();
	}

	pub(super) fn mark_snapshot_retirement(&self, slab_id: SlabId, epoch: u64, index: Option<u64>) {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		self.retirement_fence.store(true, Ordering::Release);
		self.snapshot_retirements
			.entry(slab_id)
			.and_modify(|value| {
				if (epoch, index) > *value {
					*value = (epoch, index);
				}
			})
			.or_insert((epoch, index));
		self.changed.notify_waiters();
	}

	pub(super) fn clear_snapshot_retirement(&self, slab_id: SlabId, epoch: u64) {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		self.snapshot_retirements
			.remove_if(&slab_id, |_, value| value.0 == epoch);
		if self.retirements.is_empty() && self.snapshot_retirements.is_empty() {
			self.retirement_fence.store(false, Ordering::Release);
		}
		self.changed.notify_waiters();
	}

	pub(super) fn cancel_snapshot_retirement_before_control(&self, slab_id: SlabId, index: u64) {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		self.snapshot_retirements
			.remove_if(&slab_id, |_, value| value.1.is_none_or(|old| index > old));
		if self.retirements.is_empty() && self.snapshot_retirements.is_empty() {
			self.retirement_fence.store(false, Ordering::Release);
		}
		self.changed.notify_waiters();
	}

	fn cancel_older_snapshot_retirement(&self, slab_id: SlabId, epoch: u64) {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		self.snapshot_retirements
			.remove_if(&slab_id, |_, value| value.0 < epoch);
		if self.retirements.is_empty() && self.snapshot_retirements.is_empty() {
			self.retirement_fence.store(false, Ordering::Release);
		}
		self.changed.notify_waiters();
	}

	fn expose_read_scope_if_ready(&self, slabs: &[SlabId]) -> bool {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		if !self.ready.load(Ordering::Acquire)
			|| self.closed.load(Ordering::Acquire)
			|| self.snapshot_fences.load(Ordering::Acquire) != 0
			|| slabs.iter().any(|slab| {
				self.retirements.contains_key(slab)
					|| self.snapshot_retirements.contains_key(slab)
					|| self.deferred_retirements.contains_key(slab)
			}) {
			return false;
		}
		self.exposed.store(true, Ordering::Release);
		true
	}

	#[cfg(test)]
	pub(super) fn retirement_fence_stands(&self) -> bool {
		self.retirement_fence.load(Ordering::Acquire)
	}

	pub(super) fn note_deferred_retirement(&self, slab_id: SlabId, index: u64) {
		self.deferred_retirements.insert(slab_id, index);
		self.changed.notify_waiters();
	}

	pub(super) fn clear_deferred_retirement(&self, slab_id: SlabId) {
		if self.deferred_retirements.remove(&slab_id).is_some() {
			self.changed.notify_waiters();
		}
	}

	pub(super) fn deferred_retirements(&self) -> Vec<(SlabId, u64)> {
		self.deferred_retirements
			.iter()
			.map(|entry| (*entry.key(), *entry.value()))
			.collect()
	}

	fn expose_if_ready(&self) -> bool {
		if self.exposed.load(Ordering::Acquire) {
			return self.is_ready();
		}
		let _status = self
			.status
			.lock()
			.unwrap_or_else(|error| error.into_inner());
		if !self.is_ready() {
			return false;
		}
		self.exposed.store(true, Ordering::Release);
		true
	}

	fn begin_staging(
		&self,
		slab_id: SlabId,
		snapshot: bool,
	) -> std::io::Result<BootstrapStaging<'_>> {
		let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		if (!snapshot && self.exposed.load(Ordering::Acquire))
			|| self.closed.load(Ordering::Acquire)
		{
			return Err(stale_location_error(slab_id, StaleLocationReason::Fenced));
		}
		let epoch = self
			.epoch
			.load(Ordering::Acquire)
			.checked_add(1)
			.ok_or_else(|| Error::other("tier authority epoch exhausted"))?;
		self.epoch.store(epoch, Ordering::Release);
		self.ready.store(false, Ordering::Release);
		self.staging.fetch_add(1, Ordering::AcqRel);
		*status = AuthorityStatus::Pending;
		Ok(BootstrapStaging {
			authority: self,
			finished: false,
		})
	}

	pub(super) fn notify_publication_change(&self) {
		self.changed.notify_waiters();
	}

	/// Reconciliation is one registered worker flight. Concurrent triggers
	/// request a further pass rather than spawning detached manager owners.
	pub(super) fn schedule_index_reconciliation(self: &Arc<Self>) {
		let mut state = self
			.reconcile_state
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if self.closed.load(Ordering::Acquire) {
			return;
		}
		if state.running {
			state.rerun = true;
			return;
		}
		let Some(manager) = self.manager.get().cloned() else {
			return;
		};
		let Ok(mut task) = self.reconcile_task.try_lock() else {
			return;
		};
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let worker = self.clone();
		state.running = true;
		state.rerun = false;
		#[cfg(test)]
		self.reconcile_task_starts.fetch_add(1, Ordering::Relaxed);
		let handle = runtime.spawn(async move {
			loop {
				let epoch = worker.epoch.load(Ordering::Acquire);
				for attempt in 0..4 {
					if !worker.current(epoch) {
						break;
					}
					match SlabManager::reconcile_single_append_publications_once(
						&manager, &worker, epoch,
					)
					.await
					{
						Ok((_, false)) => break,
						Ok((_, true)) => {}
						Err(error) => {
							tracing::warn!(%error, "single append reconciliation remains fenced");
							break;
						}
					}
					tokio::time::sleep(Duration::from_millis(20 << attempt)).await;
				}
				let rerun = {
					let mut state = worker
						.reconcile_state
						.lock()
						.unwrap_or_else(|e| e.into_inner());
					if (state.rerun || worker.epoch.load(Ordering::Acquire) != epoch)
						&& !worker.closed.load(Ordering::Acquire)
					{
						state.rerun = false;
						true
					} else {
						state.running = false;
						state.abort = None;
						false
					}
				};
				if !rerun {
					break;
				}
			}
		});
		state.abort = Some(handle.abort_handle());
		*task = Some(handle);
	}

	/// Serialize close with the final in-memory repair and orphan-mark removal.
	pub(super) fn reconcile_if_current<T>(&self, epoch: u64, f: impl FnOnce() -> T) -> Option<T> {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		self.current(epoch).then(f)
	}

	pub(super) fn local_ready(&self) {
		self.ready.store(true, Ordering::Release);
		self.set_status(AuthorityStatus::Ready);
	}

	fn invalidate_locked(&self, status: &mut AuthorityStatus) {
		self.ready.store(false, Ordering::Release);
		if self.closed.load(Ordering::Acquire) {
			*status = AuthorityStatus::Closed;
			self.changed.notify_waiters();
			return;
		}
		if self
			.epoch
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
				epoch.checked_add(1)
			})
			.is_err()
		{
			self.closed.store(true, Ordering::Release);
			*status = AuthorityStatus::Closed;
		} else {
			*status = AuthorityStatus::Pending;
		}
		self.changed.notify_waiters();
	}

	#[cfg(test)]
	pub(super) fn retirement_marker_for_test(&self, slab: SlabId) -> Option<Option<u64>> {
		self.retirements.get(&slab).map(|marker| *marker)
	}

	#[cfg(test)]
	pub(super) fn manager_weak(&self) -> Weak<SlabManager> {
		self.manager.get().cloned().unwrap_or_default()
	}

	#[cfg(test)]
	pub(super) fn advance_reconcile_epoch_for_test(&self) {
		let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		self.epoch.fetch_add(1, Ordering::AcqRel);
	}

	#[cfg(test)]
	pub(super) fn reconcile_epoch(&self) -> u64 {
		self.epoch.load(Ordering::Acquire)
	}

	pub(super) fn current(&self, epoch: u64) -> bool {
		self.integrity_failures.is_empty()
			&& !self.closed.load(Ordering::Acquire)
			&& self.snapshot_fences.load(Ordering::Acquire) == 0
			&& self.epoch.load(Ordering::Acquire) == epoch
	}

	fn upgrade(&self, epoch: u64) -> Option<Arc<SlabManager>> {
		if !self.current(epoch) {
			return None;
		}
		self.manager
			.get()?
			.upgrade()
			.filter(|_| self.current(epoch))
	}

	pub(super) fn wake_completed_metadata(self: &Arc<Self>) {
		self.start();
		self.changed.notify_waiters();
	}

	pub(super) fn start(self: &Arc<Self>) {
		if self.closed.load(Ordering::Acquire) {
			return;
		}
		let Ok(mut task) = self.task.try_lock() else {
			return;
		};
		if task.as_ref().is_some_and(|task| !task.is_finished()) {
			return;
		}
		let mut abort = self.abort.lock().unwrap_or_else(|e| e.into_inner());
		if self.closed.load(Ordering::Acquire) {
			return;
		}
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let worker = self.clone();
		let handle = runtime.spawn(async move {
			let _exit = AuthorityExit(worker.clone());
			worker.run().await;
		});
		#[cfg(test)]
		self.verification_flights_for_test
			.lock()
			.unwrap()
			.push(handle.abort_handle());
		*abort = Some(handle.abort_handle());
		*task = Some(handle);
	}

	pub(super) fn stop(&self) {
		let abort = self.abort.lock().unwrap_or_else(|e| e.into_inner());
		{
			let _status = self.status.lock().unwrap_or_else(|e| e.into_inner());
			self.closed.store(true, Ordering::Release);
		}
		self.ready.store(false, Ordering::Release);
		self.supersede_snapshot_reconciliation();
		let _ = self
			.epoch
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
				epoch.checked_add(1)
			});
		self.set_status(AuthorityStatus::Closed);
		if let Some(abort) = abort.as_ref() {
			abort.abort();
		}
		let state = self
			.reconcile_state
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if let Some(abort) = state.abort.as_ref() {
			abort.abort();
		}
	}
}

struct AuthorityExit(Arc<TierAuthority>);
impl Drop for AuthorityExit {
	fn drop(&mut self) {
		if !self.0.closed.load(Ordering::Acquire) {
			self.0.ready.store(false, Ordering::Release);
			self.0.set_status(AuthorityStatus::Failed(ErrorKind::Other));
		}
	}
}

#[async_trait::async_trait]
impl StorageWorker for TierAuthority {
	async fn close_and_join(&self) -> std::io::Result<()> {
		self.stop();
		let mut task = self.task.lock().await;
		if let Some(handle) = task.as_mut() {
			match handle.await {
				Ok(()) => {}
				Err(error) if error.is_cancelled() => {}
				Err(error) => {
					return Err(Error::other(format!(
						"tier authority worker failed: {error}"
					)));
				}
			}
		}
		task.take();
		drop(task);
		let mut reconcile = self.reconcile_task.lock().await;
		if let Some(handle) = reconcile.as_mut() {
			match handle.await {
				Ok(()) => {}
				Err(error) if error.is_cancelled() => {}
				Err(error) => {
					return Err(Error::other(format!(
						"index reconciliation worker failed: {error}"
					)));
				}
			}
		}
		reconcile.take();
		Ok(())
	}
}

/// Only transient transport/availability failures retry. Integrity errors and
/// unknown failures stay fenced; messages never decide the classification.
pub(crate) fn next_transient_retry(delay: &mut Duration) -> Duration {
	let next = delay.mul_f64(1.0 + (rand::random::<u16>() as f64 / u16::MAX as f64) * 0.25);
	*delay = (*delay * 2).min(Duration::from_secs(30));
	next
}

pub(crate) fn transient_tier_error(kind: ErrorKind) -> bool {
	matches!(
		kind,
		ErrorKind::NotFound
			| ErrorKind::TimedOut
			| ErrorKind::WouldBlock
			| ErrorKind::Interrupted
			| ErrorKind::ConnectionAborted
			| ErrorKind::ConnectionReset
			| ErrorKind::ConnectionRefused
			| ErrorKind::NotConnected
			| ErrorKind::BrokenPipe
			| ErrorKind::StorageFull
			| ErrorKind::OutOfMemory
			| ErrorKind::ResourceBusy
	)
}

pub(crate) fn observe_tier_authority_retry(error: &Error) {
	metrics().observe_storage_tier_authority_retry(tier_retry_class(error));
}

fn tier_retry_class(error: &Error) -> crate::metrics::TierAuthorityRetryClass {
	use crate::metrics::TierAuthorityRetryClass as Class;
	use crate::storage::l2::{RemoteAvailabilityFailure, remote_availability_failure};
	match remote_availability_failure(error) {
		Some(RemoteAvailabilityFailure::Unexpected) => Class::RemoteUnexpected,
		Some(RemoteAvailabilityFailure::RateLimited) => Class::RateLimited,
		None => match error.kind() {
			ErrorKind::NotFound => Class::ObjectNotFound,
			ErrorKind::TimedOut => Class::Timeout,
			_ => Class::IoUnavailable,
		},
	}
}

/// Two objects bound recovery's plaintext cache. A conflict group can fetch
/// an evicted object again without retaining a manager over that remote read.
#[derive(Default)]
/// Each object keeps the record offsets it was decoded with, so a later walk
/// of the cached bytes never rebuilds evidence from the index.
struct RecoveryObjects(VecDeque<(TieredSlabPointer, Arc<TieredPlaintext>)>);
impl RecoveryObjects {
	fn get(&mut self, pointer: &TieredSlabPointer) -> Option<Arc<TieredPlaintext>> {
		let index = self.0.iter().position(|(cached, _)| cached == pointer)?;
		let entry = self.0.remove(index)?;
		let bytes = entry.1.clone();
		self.0.push_back(entry);
		Some(bytes)
	}
	fn insert(&mut self, pointer: TieredSlabPointer, bytes: Arc<TieredPlaintext>) {
		self.0
			.retain(|(cached, _)| cached.slab_id != pointer.slab_id);
		while self.0.len() >= 2 {
			self.0.pop_front();
		}
		self.0.push_back((pointer, bytes));
	}
}

#[derive(Debug)]
struct ObsoleteVerification;

impl std::fmt::Display for ObsoleteVerification {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("tier verification snapshot was superseded")
	}
}

impl std::error::Error for ObsoleteVerification {}

/// ADR 0704: a marked store's verification with no canonical tier state bound: it
/// waits, retryably, and publishes nothing.
#[derive(Debug)]
pub(crate) struct VerificationAwaitsCanonicalState;

impl std::fmt::Display for VerificationAwaitsCanonicalState {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("tier verification in a marked store awaits the canonical tier state")
	}
}

impl std::error::Error for VerificationAwaitsCanonicalState {}

pub(crate) fn obsolete_verification() -> Error {
	Error::new(ErrorKind::Interrupted, ObsoleteVerification)
}

impl TierAuthority {
	/// This store's own pointers in a canonical tier snapshot (ADR 0697). A
	/// view that is not authoritative for this store never verifies.
	fn own_canonical_pointers(
		&self,
		canonical: &crate::consensus::TierSnapshotLease,
	) -> std::io::Result<Vec<TieredSlabPointer>> {
		let manager = self
			.manager
			.get()
			.and_then(Weak::upgrade)
			.ok_or_else(obsolete_verification)?;
		manager
			.own_tier_view(canonical.tiers().iter(), canonical.incarnations())
			.ok_or_else(obsolete_verification)
	}

	/// ADR 0704: the lease a verification judges by. With none, a marked
	/// (ParentDecisions) store waits, retryably, for its canonical state, whatever
	/// op intents it holds; Today verifies as before.
	fn verification_authority<'a>(
		&self,
		manager: &SlabManager,
		lease: Option<&'a crate::consensus::TierSnapshotLease>,
	) -> std::io::Result<Option<&'a crate::consensus::TierSnapshotLease>> {
		if lease.is_none() && manager.versions_contained() {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				VerificationAwaitsCanonicalState,
			));
		}
		Ok(lease)
	}

	async fn run(self: &Arc<Self>) {
		let mut delay = Duration::from_millis(200);
		let mut replay_schedule = std::collections::HashMap::new();
		loop {
			let changed = self.changed.notified();
			tokio::pin!(changed);
			changed.as_mut().enable();
			if self.closed.load(Ordering::Acquire) {
				return;
			}
			let retry = self
				.snapshot_reconciliation
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.clone();
			if let Some(retry) = retry {
				match retry.phase {
					SnapshotReconciliationPhase::Failed => {
						changed.await;
						continue;
					}
					SnapshotReconciliationPhase::Verify { revision } => {
						let canonical = retry.source.read().await;
						if canonical
							.as_ref()
							.is_ok_and(|canonical| canonical.revision() != revision)
						{
							let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
							let mut current = self
								.snapshot_reconciliation
								.lock()
								.unwrap_or_else(|e| e.into_inner());
							if let Some(current) = current.as_mut().filter(|current| {
								current.epoch == retry.epoch && !self.closed.load(Ordering::Acquire)
							}) {
								current.phase = SnapshotReconciliationPhase::Compare;
								self.snapshot_fences.insert(retry.epoch);
								self.invalidate_locked(&mut status);
							}
							continue;
						}
					}
					SnapshotReconciliationPhase::Compare => {
						let result = self.retry_snapshot_comparison(&retry).await;
						let mut retry_again = false;
						{
							let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
							let mut current = self
								.snapshot_reconciliation
								.lock()
								.unwrap_or_else(|e| e.into_inner());
							if let Some(current) = current.as_mut().filter(|current| {
								current.epoch == retry.epoch && !self.closed.load(Ordering::Acquire)
							}) {
								match result {
									Ok(revision) => {
										current.phase =
											SnapshotReconciliationPhase::Verify { revision };
										*status = AuthorityStatus::Pending;
										delay = Duration::from_millis(200);
									}
									Err(ref error) if snapshot_append_not_quiescent(error) => {
										retry_again = true;
										observe_tier_authority_retry(error);
									}
									Err(error) => {
										tracing::error!(%error, "snapshot reconciliation remains fenced after unidentified failure");
										current.phase = SnapshotReconciliationPhase::Failed;
										self.snapshot_fences.insert(retry.epoch);
										*status = AuthorityStatus::Failed(error.kind());
									}
								}
							}
						}
						if retry_again {
							tokio::select! { _ = tokio::time::sleep(next_transient_retry(&mut delay)) => {}, _ = self.changed.notified() => {} }
						}
						continue;
					}
				}
			}
			if !self.integrity_failures.is_empty()
				|| self.retirement_fence.load(Ordering::Acquire)
				|| self.snapshot_fences.load(Ordering::Acquire) != 0
			{
				changed.await;
				continue;
			}
			if self.staging.load(Ordering::Acquire) != 0 {
				changed.await;
				continue;
			}
			let metadata_retry_pending = match self.manager.get().and_then(Weak::upgrade) {
				Some(manager) => manager.retry_completed_metadata_operations().await,
				None => false,
			};
			let (manager_blocked, replay) = match self.manager.get().and_then(Weak::upgrade) {
				Some(manager) => {
					let awaiting = manager.awaiting_cluster.load(Ordering::Acquire);
					let blocked = awaiting || !manager.unapplied_tier_pointers.is_empty();
					// Replay deferrals the worker is meant to clear instead of
					// parking on them (ADR 0696 r3 E).
					let replay = if blocked && !awaiting {
						let round = manager.replay_tier_deferrals(&mut replay_schedule).await;
						#[cfg(test)]
						let hold = manager
							.replay_probe
							.before_wait
							.lock()
							.unwrap()
							.clone()
							.filter(|_| round.attempted == 0 && !round.progressed);
						#[cfg(test)]
						if let Some((entered, release)) = hold {
							entered.add_permits(1);
							if let Ok(permit) = release.acquire().await {
								permit.forget();
							}
						}
						Some((round, manager.tier_budget.clone()))
					} else {
						None
					};
					(blocked, replay)
				}
				None => (false, None),
			};
			if manager_blocked {
				match replay {
					Some((round, _)) if round.progressed => continue,
					Some((round, budget)) => {
						let released = budget.released().notified();
						tokio::pin!(released);
						released.as_mut().enable();
						// A release after a budget-fenced attempt ended, including one
						// before this waiter was registered, is never missed.
						if round
							.budget_watermark
							.is_some_and(|mark| budget.release_epoch() > mark)
						{
							continue;
						}
						let wake = round.next.unwrap_or_else(|| {
							tokio::time::Instant::now() + Duration::from_secs(3600)
						});
						tokio::select! {
							_ = &mut changed => {}
							_ = tokio::time::sleep_until(wake), if round.next.is_some() => {}
							_ = &mut released, if round.budget_watermark.is_some() => {}
						}
					}
					None => changed.await,
				}
				continue;
			}
			match self.status() {
				AuthorityStatus::Ready | AuthorityStatus::Failed(_) => {
					if metadata_retry_pending {
						tokio::select! { _ = tokio::time::sleep(Duration::from_millis(200)) => {}, _ = &mut changed => {} }
					} else {
						changed.await;
					}
					continue;
				}
				AuthorityStatus::Closed => return,
				AuthorityStatus::Retry { at, .. } => {
					tokio::select! {
						_ = tokio::time::sleep_until(at) => {}
						_ = &mut changed => { continue; }
					}
				}
				AuthorityStatus::Pending | AuthorityStatus::Verifying => {}
			}
			let epoch = {
				let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
				if self.closed.load(Ordering::Acquire) {
					return;
				}
				if self.staging.load(Ordering::Acquire) != 0 {
					continue;
				}
				let epoch = self.epoch.load(Ordering::Acquire);
				*status = AuthorityStatus::Verifying;
				epoch
			};
			let result = self.verify(epoch).await;
			{
				let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
				if !self.current(epoch) {
					continue;
				}
				match result {
					Ok(()) => {
						delay = Duration::from_millis(200);
					}
					Err(error)
						if error
							.get_ref()
							.is_some_and(|cause| cause.is::<ObsoleteVerification>()) =>
					{
						*status = AuthorityStatus::Pending;
					}
					Err(error) => {
						tracing::warn!(kind = ?error.kind(), %error, "tier authority verification refused");
						*status = if transient_tier_error(error.kind()) {
							metrics()
								.observe_storage_tier_authority_retry(tier_retry_class(&error));
							let next = AuthorityStatus::Retry {
								at: TokioInstant::now() + next_transient_retry(&mut delay),
								kind: error.kind(),
							};
							next
						} else {
							AuthorityStatus::Failed(error.kind())
						};
					}
				}
				self.completed_attempts.fetch_add(1, Ordering::AcqRel);
			}

			self.changed.notify_waiters();
		}
	}

	async fn plaintext(
		&self,
		epoch: u64,
		pointer: &TieredSlabPointer,
		cache: &mut RecoveryObjects,
	) -> std::io::Result<Arc<TieredPlaintext>> {
		if let Some(bytes) = cache.get(pointer) {
			return Ok(bytes);
		}
		let (store, acknowledged) = {
			let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
			let store = manager.config.l2.clone().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"committed tier authority has no object store",
				)
			})?;
			(store, manager.acknowledged_offsets_in_slab(pointer.slab_id))
		};
		// Only a read-only L2 handle and owned metadata cross this await. In
		// particular, detached non-owned filesystem reads must never acquire a
		// manager, directory owner, or authority to install their late result.
		let blob = store.get_bytes(&pointer.l2_uri).await?;
		let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
		validate_tiered_blob(pointer, &blob)?;
		// Everything verified here is staged committed authority (bootstrap or
		// an accepted snapshot). The recovered index describes the object only
		// when that object is the published generation (advisory tombstone or
		// supersession metadata aside) or has its verified payload hash, so its
		// bytes are identical at another location. Otherwise the committed object's own
		// verified layout is the evidence: a replacement whose publication was
		// interrupted, or committed authority staged over local evidence (a
		// quarantined collision) that the index was rebuilt from. Those local
		// rows stay unresolved evidence; this choice does not certify them.
		let published = manager.tier_pointer(pointer.slab_id).await;
		let mut evidence =
			tiered_io::tier_object_evidence(published.as_ref(), pointer, &acknowledged, true);
		// Plaintext staged over a different published generation (a snapshot
		// naming an object that a rewrap has since replaced) has no layout of
		// its own. Its verified bytes may still be exactly the published
		// generation's authenticated plaintext, whose layout the index
		// describes; nothing weaker counts.
		let incoming_plaintext = matches!(evidence, TierObjectEvidence::Replacement)
			&& manager
				.config
				.crypto
				.encrypted_footer_summary(&blob)?
				.is_none();
		if let (true, Some(published)) = (incoming_plaintext, published) {
			drop(manager);
			let published_blob = store.get_bytes(&published.l2_uri).await?;
			let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
			if manager.tier_pointer(pointer.slab_id).await.as_ref() != Some(&published)
				|| manager.acknowledged_offsets_in_slab(pointer.slab_id) != acknowledged
			{
				return Err(obsolete_verification());
			}
			validate_tiered_blob(&published, &published_blob)?;
			let published_plaintext = decode_tiered_blob(
				manager.config.crypto.as_ref(),
				&published,
				&published_blob,
				TierObjectEvidence::Indexed(&acknowledged),
			)?;
			if published_plaintext.bytes.as_slice() == blob.as_ref() {
				evidence = TierObjectEvidence::Indexed(&acknowledged);
			}
			let bytes = Arc::new(decode_tiered_blob(
				manager.config.crypto.as_ref(),
				pointer,
				&blob,
				evidence,
			)?);
			cache.insert(pointer.clone(), bytes.clone());
			return Ok(bytes);
		}
		let bytes = Arc::new(decode_tiered_blob(
			manager.config.crypto.as_ref(),
			pointer,
			&blob,
			evidence,
		)?);
		cache.insert(pointer.clone(), bytes.clone());
		Ok(bytes)
	}

	async fn frame(
		&self,
		epoch: u64,
		location: RecordLocation,
		cache: &mut RecoveryObjects,
	) -> std::io::Result<(SlabRecordHeader, Vec<u8>)> {
		let (backing, target, _pin) = {
			let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
			manager.recovery_read_target(location).await?
		};
		match backing {
			SlabBacking::Local(slab) => slab.read_with_header(target).await,
			// ADR 0705: the copy converts through its object's layout, and its
			// frame must name the version as an L2 frame must.
			SlabBacking::Rehydrated(copy) => {
				let record = copy.read_with_header(target).await?;
				lsm_driver::verify_frame_names_version(
					&record.0,
					&record.1,
					target,
					lsm_driver::ExpectedVersion {
						tenant_id: copy.generation.tenant_id,
						key_identity: None,
					},
				)?;
				Ok(record)
			}
			SlabBacking::L2(pointer) => {
				let object = self.plaintext(epoch, &pointer, cache).await?;
				let record = tiered_io::extract_record_and_header_from_blob(
					object.view()?,
					crate::storage::core::FrameLocation::of_point_read(target),
				)?;
				lsm_driver::verify_frame_names_version(
					&record.0,
					&record.1,
					target,
					lsm_driver::ExpectedVersion {
						tenant_id: pointer.tenant_id,
						key_identity: None,
					},
				)?;
				Ok(record)
			}
			SlabBacking::Missing => Err(Error::new(
				ErrorKind::InvalidData,
				"a published recovery location has no backing",
			)),
		}
	}

	async fn verify(&self, epoch: u64) -> std::io::Result<()> {
		let snapshot = {
			let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
			manager.tier_authority_snapshot().await
		};
		let mut cache = RecoveryObjects::default();
		for pending in &snapshot {
			// ADR 0705 §5: where this store holds a published local source of
			// another object, the pending generation is classified against it
			// (below, under the publication lock); both objects are fetched
			// here, with no lease or lock held.
			let classified_source = {
				let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
				manager.tier_pointer(pending.pointer.slab_id).await
			};
			let source_object = match classified_source
				.as_ref()
				.filter(|source| source.payload_hash != pending.pointer.payload_hash)
			{
				Some(source) => Some((source, self.plaintext(epoch, source, &mut cache).await?)),
				None => None,
			};
			let plaintext = self.plaintext(epoch, &pending.pointer, &mut cache).await?;
			let canonical = self.snapshot_reconciliation_source();
			let _canonical = match canonical {
				Some(source) => Some(source.read().await?),
				None => None,
			};
			if let Some(canonical) = &_canonical
				&& !self.snapshot_revision_matches(canonical.revision())
			{
				return Err(obsolete_verification());
			}
			let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
			// ADR 0704: with no deferred-snapshot retry, the manager's bound canonical state;
			// held, as the retry's lease is, before the slab lock and through the
			// publication. No object bytes are fetched under it.
			let bound = match &_canonical {
				Some(_) => None,
				None => manager.canonical_tier_lease().await?,
			};
			let lease =
				self.verification_authority(&manager, _canonical.as_ref().or(bound.as_ref()))?;
			// Whichever lease is held, the pending generation must be this store's current
			// committed one: absent or another generation never publishes.
			if let Some(lease) = lease
				&& !self
					.own_canonical_pointers(lease)?
					.iter()
					.any(|pointer| pointer.same_object_generation(&pending.pointer))
			{
				return Err(obsolete_verification());
			}
			let lock = manager.tier_publication_lock(pending.pointer.slab_id);
			let _publication = lock.lock().await;
			if !self.current(epoch)
				|| manager.tier_pointer(pending.pointer.slab_id).await != classified_source
			{
				return Err(obsolete_verification());
			}
			if let Some((source, source_object)) = &source_object {
				super::tier_transition::classify_tier_transition(
					super::tier_transition::TierTransitionObject {
						plaintext: source_object.view()?,
						payload_hash: Some(&source.payload_hash),
					},
					super::tier_transition::TierTransitionObject {
						plaintext: plaintext.view()?,
						payload_hash: Some(&pending.pointer.payload_hash),
					},
				)
				.map_err(|error| {
					manager.observed_publication_error(pending.pointer.slab_id, error)
				})?;
			}
			if manager
				.retired_tier_slabs
				.contains(&pending.pointer.slab_id)
			{
				let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
				self.invalidate_locked(&mut status);
				return Err(obsolete_verification());
			}
			if self
				.pending
				.get(&pending.pointer.slab_id)
				.is_some_and(|current| current.value() != pending)
			{
				return Err(obsolete_verification());
			}
			let plan = manager
				.build_tiered_row_refresh_plan_from_plaintext(&pending.pointer, &plaintext)
				.map_err(|error| {
					manager.observed_publication_error(pending.pointer.slab_id, error)
				})?;
			// ADR 0704: a pending certified generation publishes only its proved rows.
			if let Some(lease) = lease {
				super::certified_delivery::verify_certified_generation(
					&manager,
					lease,
					&pending.pointer,
					&plan.old_rows,
					&plan.new_rows,
				)
				.await?;
			}
			if plan.old_rows != plan.new_rows {
				metrics().observe_storage_tier_rows_repaired_at_first_serve();
			}
			manager
				.publish_tier_generation(
					&pending.pointer,
					&plan,
					pending.last_control_log_index,
					epoch,
				)
				.await?;
		}
		let groups = {
			let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
			manager.conflicted_locations.clear();
			manager.version_conflict_groups_for_recovery()
		};
		for group in groups {
			let mut payloads = Vec::with_capacity(group.len());
			for location in &group {
				let (header, bytes) = self.frame(epoch, *location, &mut cache).await?;
				payloads.push((*location, header, bytes));
			}
			let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
			manager.record_recovered_version_conflicts(&group, payloads)?;
		}
		let canonical = self.snapshot_reconciliation_source();
		let _canonical = match canonical {
			Some(source) => Some(source.read().await?),
			None => None,
		};
		if let Some(canonical) = &_canonical {
			if !self.snapshot_revision_matches(canonical.revision()) {
				return Err(obsolete_verification());
			}
			let own = self.own_canonical_pointers(canonical)?;
			if own.len() != snapshot.len()
				|| snapshot.iter().any(|pending| {
					!own.iter()
						.any(|pointer| pointer.same_object_generation(&pending.pointer))
				}) {
				return Err(obsolete_verification());
			}
		}
		let manager = self.upgrade(epoch).ok_or_else(obsolete_verification)?;
		// ADR 0704: with no retry, the bound canonical state, held through the Ready
		// transition below: every verified generation is still this store's current one,
		// so a stale pending set is never certified Ready.
		let bound = match &_canonical {
			Some(_) => None,
			None => manager.canonical_tier_lease().await?,
		};
		// Whichever lease is held (the retry's, or the bound one), both kept alive
		// through Ready.
		let held = match snapshot.is_empty() {
			// Nothing to certify: no generation can be stale.
			true => None,
			false => {
				self.verification_authority(&manager, _canonical.as_ref().or(bound.as_ref()))?
			}
		};
		if let Some(held) = held {
			let own = self.own_canonical_pointers(held)?;
			if snapshot.iter().any(|pending| {
				!own.iter()
					.any(|pointer| pointer.same_object_generation(&pending.pointer))
			}) {
				return Err(obsolete_verification());
			}
		}
		let state = manager.state.lock().await;
		let mut status = self.status.lock().unwrap_or_else(|e| e.into_inner());
		if !self.current(epoch)
			|| self.staging.load(Ordering::Acquire) != 0
			|| self.retirement_fence.load(Ordering::Acquire)
			|| self.snapshot_fences.load(Ordering::Acquire) != 0
		{
			return Err(obsolete_verification());
		}
		if manager.tier_authority_snapshot_locked(&state) != snapshot {
			self.invalidate_locked(&mut status);
			return Err(obsolete_verification());
		}
		self.pending.clear();
		manager.unverified_tier_slabs.clear();
		*status = AuthorityStatus::Ready;
		self.ready.store(true, Ordering::Release);
		self.clear_completed_snapshot_reconciliation();
		self.changed.notify_waiters();
		Ok(())
	}
}

/// A retryable readiness refusal at the head of an append, which therefore wrote nothing. This
/// says nothing about any other child of the same mutation, or its remote legs.
#[derive(Debug)]
pub(crate) struct AppendRefusedBeforeEffects(Error);
impl AppendRefusedBeforeEffects {
	pub(crate) fn cause(&self) -> &Error {
		&self.0
	}
}
impl std::fmt::Display for AppendRefusedBeforeEffects {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		self.0.fmt(f)
	}
}
impl std::error::Error for AppendRefusedBeforeEffects {}

/// Whether `error` is that refusal, and so proves this append itself did nothing.
pub(crate) fn append_refused_before_effects(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|cause| cause.is::<AppendRefusedBeforeEffects>())
}

/// The cause alone, for a caller whose earlier work means the refusal no longer proves that.
pub(crate) fn without_before_effects_proof(error: Error) -> Error {
	if !append_refused_before_effects(&error) {
		return error;
	}
	let kind = error.kind();
	Error::new(kind, error.to_string())
}

#[derive(Debug)]
struct LsmInstallUnresolved;
impl std::fmt::Display for LsmInstallUnresolved {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("unresolved durable LSM installation")
	}
}
impl std::error::Error for LsmInstallUnresolved {}

#[derive(Debug)]
struct IndexAuthorityUnavailable;

impl std::fmt::Display for IndexAuthorityUnavailable {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("persistent index authority unavailable; restart required")
	}
}
impl std::error::Error for IndexAuthorityUnavailable {}

impl SlabManager {
	pub(crate) fn is_snapshot_append_deferral(&self, error: &Error) -> bool {
		snapshot_append_not_quiescent(error)
	}

	pub(crate) fn defer_tier_snapshot_reconciliation(
		&self,
		epoch: u64,
		source: crate::consensus::TierSnapshotSource,
		error: &Error,
	) -> std::io::Result<bool> {
		if !snapshot_append_not_quiescent(error) {
			return Ok(false);
		}
		{
			let mut status = self
				.tier_authority
				.status
				.lock()
				.unwrap_or_else(|e| e.into_inner());
			if self.tier_authority.closed.load(Ordering::Acquire) {
				return Err(crate::storage::ownership::closed_error());
			}
			if self.capture_snapshot_epoch() != epoch {
				return Err(obsolete_verification());
			}
			let mut current = self
				.tier_authority
				.snapshot_reconciliation
				.lock()
				.unwrap_or_else(|e| e.into_inner());
			if current.is_none() {
				metrics()
					.storage_tier_snapshot_reconciliation_pending
					.add(1);
			}
			*current = Some(SnapshotReconciliation {
				epoch,
				source,
				phase: SnapshotReconciliationPhase::Compare,
			});
			self.tier_authority.ready.store(false, Ordering::Release);
			*status = AuthorityStatus::Pending;
		}
		self.tier_authority.start();
		self.tier_authority.changed.notify_waiters();
		Ok(true)
	}

	pub(crate) fn fence_unapplied_tier_retirement(&self, slab_id: SlabId, index: Option<u64>) {
		self.tier_authority.mark_retirement(slab_id, index);
	}

	pub(crate) fn clear_unapplied_tier_retirement(&self, slab_id: SlabId, index: Option<u64>) {
		self.tier_authority.clear_retirement(slab_id, index);
	}

	/// The published pointers plus bootstrap's newer committed authority. This
	/// snapshot never derives authority or scope from a local row list.
	pub(super) fn tier_authority_snapshot_locked(
		&self,
		state: &ManagerState,
	) -> Vec<PendingTierAuthority> {
		let mut pointers = state
			.tiered
			.iter()
			.map(|entry| {
				(
					*entry.key(),
					PendingTierAuthority {
						pointer: entry.value().clone(),
						last_control_log_index: None,
					},
				)
			})
			.collect::<BTreeMap<_, _>>();
		for entry in &self.tier_authority.pending {
			pointers.insert(*entry.key(), entry.value().clone());
		}
		pointers
			.into_iter()
			.filter(|(id, _)| !self.retired_tier_slabs.contains(id))
			.map(|(_, pending)| pending)
			.collect()
	}

	pub(super) async fn tier_authority_snapshot(&self) -> Vec<PendingTierAuthority> {
		let state = self.state.lock().await;
		self.tier_authority_snapshot_locked(&state)
	}

	fn tier_authority_refusal(&self) -> Error {
		if let Some(reason) = self.owner_authority_fence.get() {
			return Error::new(ErrorKind::WouldBlock, *reason);
		}
		if self
			.tier_authority
			.unresolved_op_fence
			.load(Ordering::Acquire)
		{
			return Error::new(ErrorKind::WouldBlock, "UnresolvedOpIntent");
		}
		if let Some(error) = self.tier_authority.integrity_error() {
			return error;
		}
		if self.awaiting_cluster.load(Ordering::Acquire) {
			return Error::new(ErrorKind::WouldBlock, "TierAuthorityAwaitingCluster");
		}
		if self.index_authority_unavailable.load(Ordering::Acquire) {
			return Error::new(ErrorKind::WouldBlock, IndexAuthorityUnavailable);
		}
		if self.has_orphaned_index_publications() {
			return Error::new(
				ErrorKind::WouldBlock,
				"TierAuthorityPublicationPending(RestartRequired)",
			);
		}
		if self.has_orphaned_lsm_intents() {
			return Error::new(ErrorKind::WouldBlock, LsmInstallUnresolved);
		}
		if self.tier_authority.closed.load(Ordering::Acquire) {
			return crate::storage::ownership::closed_error();
		}
		let slab_id = self
			.tier_authority
			.pending
			.iter()
			.map(|entry| *entry.key())
			.min()
			.or_else(|| {
				self.unapplied_tier_pointers
					.iter()
					.map(|entry| *entry.key())
					.min()
			})
			.unwrap_or(SlabId::ZERO);
		stale_location_error(slab_id, StaleLocationReason::Fenced)
	}

	pub(super) fn tier_authority_is_ready(&self) -> bool {
		!self.awaiting_cluster.load(Ordering::Acquire)
			&& !self.index_authority_unavailable.load(Ordering::Acquire)
			&& !self.has_orphaned_lsm_intents()
			&& !self.has_orphaned_index_publications()
			&& self.tier_authority.is_ready()
	}

	/// Caller holds this slab's publication lock. Committed log retirement
	/// invalidates its generation even while read verification is pending.
	/// Local initiative still requires a verified private row snapshot.
	pub(super) fn require_private_retirement_authority(
		&self,
		slab_id: SlabId,
		committed_log: bool,
	) -> std::io::Result<()> {
		let own_dirty = self
			.pending_index_publications
			.iter()
			.any(|entry| entry.key().0 == slab_id);
		if self.tier_authority.integrity_failures.is_empty()
			&& !self.awaiting_cluster.load(Ordering::Acquire)
			&& !self.index_authority_unavailable.load(Ordering::Acquire)
			&& self.tier_authority.snapshot_fences.load(Ordering::Acquire) == 0
			&& !own_dirty
			&& !self.tier_authority.closed.load(Ordering::Acquire)
			&& (committed_log
				|| (self.tier_authority.ready.load(Ordering::Acquire)
					&& !self.tier_authority.pending.contains_key(&slab_id)
					&& !self.unverified_tier_slabs.contains(&slab_id)))
			// ADR 0704: a fence on an unprovable committed standing is no authority against a
			// retirement. It exists only under a canonical lease, which lets a retirement through
			// only while one is in force, and that retirement is what clears it.
			&& self
				.unapplied_tier_pointers
				.get(&slab_id)
				.is_none_or(|entry| {
					entry.value().outcome
						== super::tier_deferral::DeferralOutcome::Fenced(
							super::tier_deferral::FenceCause::CommittedStanding,
						)
				}) {
			return Ok(());
		}
		Err(stale_location_error(slab_id, StaleLocationReason::Fenced))
	}

	/// Private tier apply holds X's publication lock. Unrelated LSM intents
	/// and other slabs' dirty marks do not change X's established authority.
	pub(super) fn require_private_tier_apply_authority(
		&self,
		slab_id: SlabId,
	) -> std::io::Result<()> {
		let own_dirty = self
			.pending_index_publications
			.iter()
			.any(|entry| entry.key().0 == slab_id);
		if self.tier_authority.integrity_failures.is_empty()
			&& !self.awaiting_cluster.load(Ordering::Acquire)
			&& !self.index_authority_unavailable.load(Ordering::Acquire)
			&& self.tier_authority.snapshot_fences.load(Ordering::Acquire) == 0
			&& !own_dirty
			&& !self.lsm_intent_names_slab(slab_id)
			&& !self.lsm_intent_keys_unknown()
			&& !self.tier_authority.closed.load(Ordering::Acquire)
			&& self.tier_authority.ready.load(Ordering::Acquire)
			&& !self.tier_authority.pending.contains_key(&slab_id)
			&& !self.unverified_tier_slabs.contains(&slab_id)
		{
			return Ok(());
		}
		Err(stale_location_error(slab_id, StaleLocationReason::Fenced))
	}

	/// The head of an append: refused here, it has done nothing. Only a refusal the store clears
	/// by itself says so. An integrity, owner, restart-required or closed refusal stays what it
	/// was, for the caller to treat as it always has.
	pub(crate) fn require_append_admission(&self) -> std::io::Result<()> {
		#[cfg(test)]
		let refused = self.take_append_refusal_for_test();
		#[cfg(not(test))]
		let refused = false;
		let answer = if refused {
			Err(stale_location_error(
				SlabId::ZERO,
				StaleLocationReason::Fenced,
			))
		} else {
			self.require_tier_authority_ready()
		};
		answer.map_err(|cause| self.refused_before_effects(cause))
	}

	/// An authority refusal met before this append has done anything: it carries that proof if
	/// it will clear by itself. Each check ahead of an append's first effect answers through
	/// this, so a fence raised after the head of the append is waited out the same way.
	pub(super) fn refused_before_effects(&self, cause: Error) -> Error {
		if self.append_refusal_clears_by_itself() {
			#[cfg(test)]
			self.note_append_refusal_for_test();
			Error::new(cause.kind(), AppendRefusedBeforeEffects(cause))
		} else {
			cause
		}
	}

	/// Until the authority next changes, or `max` passes: never a claim that it is ready.
	pub(crate) async fn tier_authority_change_within(&self, max: Duration) {
		let _ = tokio::time::timeout(max, self.tier_authority.changed.notified()).await;
	}

	fn append_refusal_clears_by_itself(&self) -> bool {
		self.owner_authority_fence.get().is_none()
			&& !self
				.tier_authority
				.unresolved_op_fence
				.load(Ordering::Acquire)
			&& self.tier_authority.integrity_error().is_none()
			&& !self.awaiting_cluster.load(Ordering::Acquire)
			&& !self.index_authority_unavailable.load(Ordering::Acquire)
			&& !self.has_orphaned_index_publications()
			&& !self.tier_authority.closed.load(Ordering::Acquire)
			&& !matches!(
				self.tier_authority.status(),
				AuthorityStatus::Failed(_) | AuthorityStatus::Closed
			)
	}

	/// Synchronous answer paths refuse rather than turn unverified authority
	/// into an empty candidate list or an authoritative negative answer.
	pub(crate) fn require_tier_authority_ready(&self) -> std::io::Result<()> {
		if !self.awaiting_cluster.load(Ordering::Acquire)
			&& !self.index_authority_unavailable.load(Ordering::Acquire)
			&& !self.has_orphaned_lsm_intents()
			&& !self.tier_authority.deferred_open.load(Ordering::Acquire)
			&& !self.has_orphaned_index_publications()
			&& self.unapplied_tier_pointers.is_empty()
			&& self.tier_authority.expose_if_ready()
		{
			return Ok(());
		}
		self.tier_authority.start();
		Err(self.tier_authority_refusal())
	}

	pub(crate) fn require_tier_read_scope(&self, slabs: &[SlabId]) -> std::io::Result<()> {
		if !self.awaiting_cluster.load(Ordering::Acquire)
			&& !self.index_authority_unavailable.load(Ordering::Acquire)
			&& !self.has_orphaned_lsm_intents()
			&& !self.tier_authority.deferred_open.load(Ordering::Acquire)
			&& !self.has_orphaned_index_publications()
			&& self.unapplied_tier_pointers.is_empty()
			&& self.tier_authority.expose_read_scope_if_ready(slabs)
		{
			return Ok(());
		}
		Err(self.tier_authority_refusal())
	}

	pub(super) async fn wait_tier_read_scope(&self, slabs: &[SlabId]) -> std::io::Result<()> {
		if self.require_tier_read_scope(slabs).is_ok() {
			return Ok(());
		}
		self.wait_tier_authority_ready().await?;
		self.require_tier_read_scope(slabs)
	}

	/// Async reads join one attempt. A read made during backoff joins the next
	/// attempt; it never recursively joins from the verifier's private reads.
	pub(crate) async fn wait_tier_authority_ready(&self) -> std::io::Result<()> {
		let initial = self
			.tier_authority
			.completed_attempts
			.load(Ordering::Acquire);
		loop {
			if let Some(error) = self.tier_authority.integrity_error() {
				return Err(error);
			}
			let changed = self.tier_authority.changed.notified();
			tokio::pin!(changed);
			changed.as_mut().enable();
			if self.awaiting_cluster.load(Ordering::Acquire)
				|| self.tier_authority.closed.load(Ordering::Acquire)
				|| self.index_authority_unavailable.load(Ordering::Acquire)
				|| self.has_orphaned_lsm_intents()
				|| self.tier_authority.deferred_open.load(Ordering::Acquire)
				|| self.has_orphaned_index_publications()
			{
				return Err(self.tier_authority_refusal());
			}
			if self.tier_authority.retirement_fence.load(Ordering::Acquire)
				|| self.tier_authority.snapshot_fences.load(Ordering::Acquire) != 0
			{
				return Err(self.tier_authority_refusal());
			}
			if self.tier_authority.is_ready() {
				return self.require_tier_authority_ready();
			}
			#[cfg(test)]
			{
				let hold = self
					.tier_authority
					.wait_before_attempt_check_hold
					.lock()
					.unwrap()
					.take();
				if let Some((entered, release)) = hold {
					entered.add_permits(1);
					let _ = release.acquire().await;
				}
			}
			if matches!(
				self.tier_authority.status(),
				AuthorityStatus::Failed(_) | AuthorityStatus::Closed
			) || self
				.tier_authority
				.completed_attempts
				.load(Ordering::Acquire)
				> initial
			{
				// That attempt can have succeeded since the check above, and its verdict is
				// what this waited for: a ready store is not refused for finishing just now.
				if self.tier_authority.is_ready() {
					return self.require_tier_authority_ready();
				}
				return Err(self.tier_authority_refusal());
			}
			self.tier_authority.start();
			#[cfg(test)]
			self.tier_authority
				.verification_waits_for_test
				.fetch_add(1, Ordering::AcqRel);
			changed.await;
		}
	}

	pub(crate) fn tier_authority_failure_kind(&self) -> Option<ErrorKind> {
		if self.tier_authority.closed.load(Ordering::Acquire) {
			return Some(ErrorKind::BrokenPipe);
		}
		match self.tier_authority.status() {
			AuthorityStatus::Failed(kind) => Some(kind),
			AuthorityStatus::Closed => Some(ErrorKind::BrokenPipe),
			_ => None,
		}
	}

	/// Kept separate from persistence distrust: waiting for L2 must not shut
	/// down Raft participation during node bootstrap.
	pub(crate) fn tier_authority_readiness_reason(&self) -> Option<String> {
		if let Some(reason) = self.owner_authority_fence.get() {
			return Some((*reason).into());
		}
		// Below the sticky reasons, and only a follower with something to resolve.
		if self
			.tier_authority
			.awaiting_leadership
			.load(Ordering::Acquire)
			&& self.tier_authority.integrity_error().is_none()
			&& !self.index_authority_unavailable.load(Ordering::Acquire)
			&& self.require_cleanup_authority().is_ok()
		{
			return Some("AwaitingLeadership".into());
		}
		if self
			.tier_authority
			.unresolved_op_fence
			.load(Ordering::Acquire)
		{
			return Some("UnresolvedOpIntent".into());
		}
		if let Some(error) = self.tier_authority.integrity_error() {
			return Some(error.to_string());
		}
		if self.awaiting_cluster.load(Ordering::Acquire) {
			return Some("TierAuthorityAwaitingCluster".into());
		}
		if self.index_authority_unavailable.load(Ordering::Acquire) {
			return Some("TierAuthorityIndexUnavailable(RestartRequired)".into());
		}
		if self.require_cleanup_authority().is_err() {
			return Some("TierCleanupAuthorityMissing(RestartRequired)".into());
		}
		if self.has_orphaned_lsm_intents() {
			return Some("TierAuthorityLsmUnresolved".into());
		}
		if self.tier_authority.deferred_open.load(Ordering::Acquire) {
			return Some("DeferredOpenIncomplete".into());
		}
		if self.has_orphaned_index_publications() {
			return Some("TierAuthorityPublicationPending(RestartRequired)".into());
		}
		if self.tier_authority.closed.load(Ordering::Acquire) {
			return Some("TierAuthorityClosed".into());
		}
		if self.tier_authority.retirement_fence.load(Ordering::Acquire)
			|| self.tier_authority.snapshot_fences.load(Ordering::Acquire) != 0
		{
			return Some("TierAuthorityFenced".into());
		}
		if !self.index_authority_unavailable.load(Ordering::Acquire)
			&& !self.has_orphaned_lsm_intents()
			&& !self.has_orphaned_index_publications()
			&& self.unapplied_tier_pointers.is_empty()
			&& self.tier_authority.expose_if_ready()
		{
			return None;
		}
		self.tier_authority.start();
		Some(match self.tier_authority.status() {
			AuthorityStatus::Retry { at, kind } => format!(
				"TierAuthorityRetry({kind:?}, backoff_ms={})",
				at.saturating_duration_since(TokioInstant::now())
					.as_millis()
			),
			AuthorityStatus::Failed(kind) => format!("TierAuthorityFailed({kind:?})"),
			AuthorityStatus::Closed => "TierAuthorityClosed".into(),
			AuthorityStatus::Ready => "TierAuthorityFenced".into(),
			AuthorityStatus::Pending | AuthorityStatus::Verifying => "TierAuthorityPending".into(),
		})
	}

	/// Reconcile durable retirement marks against the complete recovered control set.
	pub(crate) async fn fence_local_tiers_absent_from_committed(
		&self,
		committed: &std::collections::HashSet<SlabId>,
		index: Option<u64>,
	) -> std::io::Result<()> {
		if self.persistent_index.is_none() {
			for pointer in self.tiered_pointers_snapshot().await {
				if !committed.contains(&pointer.slab_id) {
					self.fence_unapplied_tier_retirement(pointer.slab_id, None);
				}
			}
			return Ok(());
		}
		let epoch = self.begin_tier_snapshot_install()?;
		let gate = self.persistent_index_gate.clone().write_owned().await;
		self.clear_disposal_evidence().await;
		let local = self.tiered_pointers_snapshot().await;
		let marks = local
			.into_iter()
			.filter(|pointer| !committed.contains(&pointer.slab_id))
			.map(|pointer| crate::storage::index::SnapshotRetirementMark {
				epoch,
				snapshot_index: index,
				pointer,
			})
			.collect::<Vec<_>>();
		let result = self
			.persistent_index
			.as_ref()
			.ok_or_else(|| {
				Error::new(
					ErrorKind::WouldBlock,
					"persistent index unavailable for bootstrap retirement",
				)
			})?
			.stage_snapshot_retirement_marks(
				marks.clone(),
				committed.clone(),
				epoch,
				self.snapshot_epoch.clone(),
				self.snapshot_epoch_gate.clone(),
			)
			.await;
		if let Err(error) = result {
			if error.kind() == ErrorKind::Interrupted {
				self.tier_authority.snapshot_fences.release(epoch);
				self.tier_authority.changed.notify_waiters();
			}
			return Err(error);
		}
		for mark in &marks {
			self.tier_authority
				.mark_snapshot_retirement(mark.pointer.slab_id, epoch, index);
		}
		for slab_id in committed {
			self.tier_authority
				.cancel_older_snapshot_retirement(*slab_id, epoch);
		}
		drop(gate);
		self.tier_authority.snapshot_fences.release(epoch);
		self.tier_authority.changed.notify_waiters();
		// Bootstrap: no installed state is held here; a marked store fences a held
		// preimage rather than retire it (ADR 0704).
		for mark in marks {
			self.apply_snapshot_retirement(mark, None).await?;
		}
		self.tier_authority.start();
		Ok(())
	}

	/// Fence admissions before snapshot publication. A failed or cancelled
	/// installation deliberately retains its own fence until recovery.
	pub(crate) fn begin_tier_snapshot_install(&self) -> std::io::Result<u64> {
		self.begin_tier_comparison(false, None)
	}

	/// Begin installing the snapshot at `index`, whose decoded `registries` are
	/// committed state newer than this store has applied. They are classified
	/// before the snapshot's index covers any delivery, so if publication then
	/// fails, what the covered deliveries said has still been consumed.
	pub(crate) fn begin_tier_snapshot_install_at(
		&self,
		index: Option<u64>,
		registries: &std::collections::BTreeMap<u32, crate::consensus::IncarnationRegistry>,
	) -> std::io::Result<u64> {
		self.begin_tier_comparison(false, index.map(|index| (index, Some(registries))))
	}

	/// Whether a begun comparison read a view at or past `index`.
	pub(super) fn comparison_covers_index(&self, index: u64) -> bool {
		self.tier_authority
			.comparison_read_index
			.read()
			.unwrap_or_else(|e| e.into_inner())
			.is_some_and(|read| index <= read)
	}

	/// `reads` is the index this comparison's view reached, with its registries
	/// when they have not been classified yet.
	fn begin_tier_comparison(
		&self,
		replay: bool,
		reads: Option<(
			u64,
			Option<&std::collections::BTreeMap<u32, crate::consensus::IncarnationRegistry>>,
		)>,
	) -> std::io::Result<u64> {
		if let Some(error) = self.tier_authority.integrity_error() {
			return Err(error);
		}
		let _status = self
			.tier_authority
			.status
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		let _epoch_guard = self
			.snapshot_epoch_gate
			.write()
			.unwrap_or_else(|e| e.into_inner());
		if replay && self.tier_authority.snapshot_fences.load(Ordering::Acquire) != 0 {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"replacement comparison already in flight",
			));
		}
		let epoch = self
			.snapshot_epoch
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
			.map_err(|_| Error::other("snapshot epoch exhausted"))?
			+ 1;
		self.tier_authority.snapshot_fences.insert(epoch);
		if let Some((reads, registries)) = reads {
			let superseded =
				self.comparison_covers_index(reads) || self.observer_is_superseded(Some(reads));
			if let Some(registries) = registries.filter(|_| !superseded) {
				self.classify_incoming_registries(registries);
			}
			let mut read = self
				.tier_authority
				.comparison_read_index
				.write()
				.unwrap_or_else(|e| e.into_inner());
			*read = Some(read.map_or(reads, |old| old.max(reads)));
		}
		if self
			.tier_authority
			.epoch
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
			.is_err()
		{
			return Err(Error::other("tier authority epoch exhausted"));
		}
		self.tier_authority.supersede_snapshot_reconciliation();
		self.tier_authority.changed.notify_waiters();
		Ok(epoch)
	}

	pub(crate) fn seed_installed_snapshot_index(&self, index: u64) {
		let mut installed = self
			.installed_snapshot_index
			.write()
			.unwrap_or_else(|e| e.into_inner());
		*installed = Some(installed.map_or(index, |old| old.max(index)));
	}

	/// Drain earlier index publications before comparing the complete tier set.
	#[cfg(test)]
	pub(crate) async fn finish_tier_snapshot_install(
		&self,
		pointers: Vec<TieredSlabPointer>,
		index: Option<u64>,
	) -> std::io::Result<()> {
		self.finish_tier_snapshot_install_at_epoch(
			pointers,
			index,
			self.capture_snapshot_epoch(),
			None,
		)
		.await
	}

	/// `lease`: the installed state, held by the caller (ADR 0704: it classifies every
	/// retirement mark this comparison applies).
	pub(crate) async fn finish_tier_snapshot_install_at_epoch(
		&self,
		pointers: Vec<TieredSlabPointer>,
		index: Option<u64>,
		snapshot_epoch: u64,
		lease: Option<&crate::consensus::TierSnapshotLease>,
	) -> std::io::Result<()> {
		self.finish_tier_comparison_at_epoch(pointers, index, snapshot_epoch, None, lease)
			.await
	}

	pub(crate) async fn finish_replacement_log_replay(
		&self,
		pointers: Vec<TieredSlabPointer>,
		applied: u64,
		confirmed: u64,
	) -> std::io::Result<()> {
		if !self.is_awaiting_cluster() {
			return Ok(());
		}
		if applied < confirmed {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"replacement is below leader confirmation",
			));
		}
		// `raft_server` classified this view's registry before starting replay.
		let epoch = self.begin_tier_comparison(true, Some((applied, None)))?;
		struct ReplayFence<'a>(&'a SlabManager, u64);
		impl Drop for ReplayFence<'_> {
			fn drop(&mut self) {
				self.0.tier_authority.snapshot_fences.release(self.1);
				self.0.tier_authority.changed.notify_waiters();
			}
		}
		let _fence = ReplayFence(self, epoch);
		self.finish_tier_comparison_at_epoch(pointers, Some(applied), epoch, Some(confirmed), None)
			.await
	}

	async fn finish_tier_comparison_at_epoch(
		&self,
		pointers: Vec<TieredSlabPointer>,
		index: Option<u64>,
		snapshot_epoch: u64,
		replay_confirmation: Option<u64>,
		lease: Option<&crate::consensus::TierSnapshotLease>,
	) -> std::io::Result<()> {
		let _gate = self.persistent_index_gate.clone().write_owned().await;
		if self.tier_authority.closed.load(Ordering::Acquire) {
			return Err(crate::storage::ownership::closed_error());
		}
		if self.capture_snapshot_epoch() != snapshot_epoch {
			return Err(obsolete_verification());
		}
		// A snapshot comparison is a bulk authority event.
		self.clear_disposal_evidence().await;
		if replay_confirmation.is_some()
			&& index
				.zip(
					*self
						.installed_snapshot_index
						.read()
						.unwrap_or_else(|e| e.into_inner()),
				)
				.is_some_and(|(applied, installed)| applied < installed)
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"replacement proof precedes installed snapshot",
			));
		}
		let committed: std::collections::HashMap<_, _> = pointers
			.into_iter()
			.map(|pointer| (pointer.slab_id, pointer))
			.collect();
		let local = self.tiered_pointers_snapshot().await;
		let pending = self
			.tier_authority
			.pending
			.iter()
			.map(|entry| entry.value().pointer.clone())
			.collect::<Vec<_>>();
		if let Some(index) = index {
			for pointer in committed.values() {
				let current = self
					.tier_authority
					.pending
					.get(&pointer.slab_id)
					.map(|entry| entry.value().clone());
				if let Some(current) = current
					&& current.last_control_log_index == Some(index)
					&& current.pointer != *pointer
				{
					return Err(self
						.tier_authority
						.note_index_conflict(pointer.slab_id, index));
				}
			}
		}
		let retire_ids = local
			.iter()
			.chain(&pending)
			.filter(|pointer| !committed.contains_key(&pointer.slab_id))
			.map(|pointer| pointer.slab_id)
			.collect::<std::collections::BTreeSet<_>>();
		if replay_confirmation.is_some()
			&& (local.iter().chain(&pending).any(|pointer| {
				committed.get(&pointer.slab_id).is_none_or(|expected| {
					let mut normalized = pointer.clone();
					normalized.tombstone_metadata = expected.tombstone_metadata;
					normalized.supersession_metadata = expected.supersession_metadata;
					&normalized != expected
				})
			}) || committed.keys().any(|id| {
				self.retired_tier_slabs.contains(id)
					|| self.unapplied_tier_pointers.get(id).is_some_and(|entry| {
						// Complete replay may migrate only its exact canonical
						// transient deferral into the existing verification flight.
						// Structural failures and pointer discrepancies remain evidence.
						!entry.value().fences(&committed[id])
					})
			})) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"replacement committed-pointer discrepancy",
			));
		}
		let mut displaced = Vec::new();
		for old in &local {
			let replacement = committed.get(&old.slab_id);
			if replacement.is_none_or(|pointer| pointer.l2_uri != old.l2_uri) {
				displaced.push((old.clone(), replacement.cloned(), true));
			}
		}
		for old in &pending {
			let replacement = committed.get(&old.slab_id);
			if !local
				.iter()
				.any(|live| live.slab_id == old.slab_id && live.l2_uri == old.l2_uri)
				&& replacement.is_none_or(|pointer| pointer.l2_uri != old.l2_uri)
			{
				displaced.push((old.clone(), replacement.cloned(), false));
			}
		}
		let committed_id_floor = committed.keys().copied().max().unwrap_or(SlabId::ZERO);
		let mut pending_retirements = Vec::new();
		if replay_confirmation.is_none() {
			for pointer in pending.iter().filter(|pointer| {
				!committed.contains_key(&pointer.slab_id)
					&& !local.iter().any(|old| old.slab_id == pointer.slab_id)
			}) {
				let registered = self.tier_authority.pending.get(&pointer.slab_id);
				let old = registered
					.as_ref()
					.filter(|entry| entry.pointer == *pointer)
					.and_then(|entry| entry.last_control_log_index);
				let Some((old, snapshot)) = old.zip(index).filter(|(old, snapshot)| old < snapshot)
				else {
					return Err(Error::new(
						ErrorKind::WouldBlock,
						"pending-only retirement requires an exact older indexed flight",
					));
				};
				debug_assert!(old < snapshot);
				pending_retirements.push((pointer.clone(), old));
			}
		}
		let mut retirement_marks: Vec<crate::storage::index::SnapshotRetirementMark> = local
			.iter()
			.filter(|pointer| !committed.contains_key(&pointer.slab_id))
			.map(|pointer| crate::storage::index::SnapshotRetirementMark {
				epoch: snapshot_epoch,
				snapshot_index: index,
				pointer: pointer.clone(),
			})
			.collect();
		retirement_marks.extend(pending_retirements.iter().map(|(pointer, _)| {
			crate::storage::index::SnapshotRetirementMark {
				epoch: snapshot_epoch,
				snapshot_index: index,
				pointer: pointer.clone(),
			}
		}));
		let post_retirement_marks = retirement_marks.clone();
		let committed_ids: std::collections::HashSet<_> = committed.keys().copied().collect();
		let committed_for_cancel = committed_ids.clone();
		let replacement_completion = replay_confirmation.map(|_| {
			(
				index.expect("replay applied index"),
				committed.values().cloned().collect::<Vec<_>>(),
			)
		});
		// This is an input only to confirmed replacement replay. A missing
		// durable row needs exact callback provenance, never just membership
		// in the caller's expected set. Published local tiers cannot be repaired
		// through this path: a missing old row remains an A11 discrepancy.
		let replay_staged = if replay_confirmation.is_some() {
			committed
				.values()
				.filter(|pointer| {
					!local.iter().any(|old| old.slab_id == pointer.slab_id)
						&& (self
							.tier_authority
							.pending
							.get(&pointer.slab_id)
							.is_some_and(|entry| {
								entry.pointer == **pointer
									&& entry
										.last_control_log_index
										.zip(index)
										.is_some_and(|(callback, applied)| callback <= applied)
							}) || self
							.unapplied_tier_pointers
							.get(&pointer.slab_id)
							.is_some_and(|entry| {
								entry.value().fences(pointer)
									&& entry
										.value()
										.provenance
										.zip(index)
										.is_some_and(|(callback, applied)| callback <= applied)
							}))
				})
				.cloned()
				.collect()
		} else {
			Vec::new()
		};
		let mut to_stage = Vec::new();
		let mut conflicts = Vec::new();
		for (id, pointer) in committed {
			let pending_matches = pending.iter().any(|old| old == &pointer);
			let local_matches = local.iter().any(|old| old == &pointer);
			let known = self
				.tier_authority
				.pending
				.get(&id)
				.map(|entry| entry.value().clone());
			let conflicting_pending = known.as_ref().is_some_and(|old| {
				old.pointer != pointer
					&& !index.is_some_and(|incoming| {
						old.last_control_log_index
							.is_none_or(|known| incoming > known)
					})
			});
			let matching_deferral = index.is_some()
				&& self
					.unapplied_tier_pointers
					.get(&id)
					.is_some_and(|entry| entry.value().fences(&pointer));
			let superseded_deferral = index.is_some_and(|incoming| {
				known.as_ref().is_some_and(|old| {
					old.pointer != pointer
						&& old
							.last_control_log_index
							.is_none_or(|index| incoming > index)
						&& self
							.unapplied_tier_pointers
							.get(&id)
							.is_some_and(|entry| entry.value().fences(&old.pointer))
				})
			});
			if conflicting_pending || self.retired_tier_slabs.contains(&id) {
				conflicts.push((id, pointer));
			} else if matching_deferral
				|| superseded_deferral
				|| (!pending_matches
					&& !local_matches
					&& !self.unapplied_tier_pointers.contains_key(&id))
			{
				to_stage.push(pointer);
			}
		}
		let persistent = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for snapshot retirement",
			)
		})?;
		let mark_result = if let Some(completion) = replacement_completion {
			persistent
				.stage_replacement_tier_comparison(
					completion,
					replay_staged,
					retirement_marks,
					committed_ids,
					snapshot_epoch,
					self.snapshot_epoch.clone(),
					self.snapshot_epoch_gate.clone(),
				)
				.await
		} else if !pending_retirements.is_empty() {
			persistent
				.stage_snapshot_tier_comparison_with_pending_retirements(
					index.expect("checked pending retirement index"),
					pending_retirements,
					retirement_marks,
					committed_ids,
					snapshot_epoch,
					self.snapshot_epoch.clone(),
					self.snapshot_epoch_gate.clone(),
				)
				.await
		} else {
			persistent
				.stage_tier_comparison(
					None,
					retirement_marks,
					committed_ids,
					snapshot_epoch,
					self.snapshot_epoch.clone(),
					self.snapshot_epoch_gate.clone(),
				)
				.await
		};
		let degraded_pointers = match mark_result {
			Ok(pointers) => pointers,
			Err(error) => {
				if error.kind() == ErrorKind::Interrupted && replay_confirmation.is_none() {
					self.tier_authority.snapshot_fences.release(snapshot_epoch);
					self.tier_authority.changed.notify_waiters();
				}
				return Err(error);
			}
		};
		{
			let state = self.state.lock().await;
			for pointer in degraded_pointers {
				if let Some(mut live) = state.tiered.get_mut(&pointer.slab_id) {
					if live.l2_uri == pointer.l2_uri {
						live.supersession_metadata = pointer.supersession_metadata;
						live.tombstone_metadata = pointer.tombstone_metadata;
					}
				}
			}
		}
		if let Some(index) = index {
			self.seed_installed_snapshot_index(index);
		}
		for id in retire_ids {
			self.tier_authority
				.mark_snapshot_retirement(id, snapshot_epoch, index);
		}
		for id in committed_for_cancel {
			self.tier_authority
				.cancel_older_snapshot_retirement(id, snapshot_epoch);
		}
		for (id, pointer) in conflicts {
			self.unapplied_tier_pointers.entry(id).or_insert(
				super::tier_deferral::TierDeferral::fenced(
					pointer,
					super::tier_deferral::FenceCause::Staged,
					None,
				),
			);
		}
		#[cfg(test)]
		self.snapshot_after_retirement_mark_pause_point().await;
		drop(_gate);
		for (old, replacement, locally_published) in displaced {
			if replacement.is_none() && locally_published {
				continue;
			}
			self.record_retained_pointer_transition(&old, replacement.as_ref(), locally_published)
				.await?;
		}
		for mut pointer in to_stage {
			if let Some(tokens) = self.pending_metadata_tokens.get(&pointer.slab_id) {
				tokens.degrade(&mut pointer);
			}
			if self.tier_authority.closed.load(Ordering::Acquire)
				|| self.capture_snapshot_epoch() != snapshot_epoch
			{
				return Err(obsolete_verification());
			}
			self.stage_tier_authority(pointer, index, committed_id_floor, true)
				.await?;
		}
		if self.tier_authority.closed.load(Ordering::Acquire)
			|| self.capture_snapshot_epoch() != snapshot_epoch
		{
			return Err(obsolete_verification());
		}
		if self.awaiting_cluster.load(Ordering::Acquire) {
			#[cfg(test)]
			self.snapshot_before_marker_clear_pause_point().await;
			let marker = self.config.dir.join(AWAITING_CLUSTER_MARKER);
			unlink_local_file(self.file_ops.as_ref(), &marker).await?;
			self.awaiting_cluster.store(false, Ordering::Release);
		}
		if replay_confirmation.is_none() {
			self.tier_authority.snapshot_fences.release(snapshot_epoch);
			self.tier_authority.changed.notify_waiters();
		}
		#[cfg(test)]
		self.snapshot_before_retirement_apply_pause_point().await;
		for mark in post_retirement_marks {
			self.apply_snapshot_retirement(mark, lease).await?;
		}
		self.tier_authority.start();
		Ok(())
	}

	/// Test-only: holds or releases this store's tier authority as a start
	/// awaiting the cluster does.
	#[cfg(test)]
	pub(crate) fn set_awaiting_cluster_for_test(&self, awaiting: bool) {
		self.awaiting_cluster.store(awaiting, Ordering::Release);
	}

	pub(crate) fn is_awaiting_cluster(&self) -> bool {
		self.awaiting_cluster.load(Ordering::Acquire)
	}

	pub(crate) fn capture_snapshot_epoch(&self) -> u64 {
		self.snapshot_epoch.load(Ordering::Acquire)
	}

	pub(super) fn snapshot_epoch_is_current(&self, epoch: u64) -> bool {
		!self.snapshot_install_in_flight() && self.snapshot_epoch.load(Ordering::Acquire) == epoch
	}

	pub(super) fn snapshot_install_in_flight(&self) -> bool {
		self.tier_authority.snapshot_fences.load(Ordering::Acquire) != 0
	}

	pub(crate) fn observer_is_superseded(&self, index: Option<u64>) -> bool {
		let installed = *self
			.installed_snapshot_index
			.read()
			.unwrap_or_else(|e| e.into_inner());
		index
			.zip(installed)
			.is_some_and(|(index, installed)| index <= installed)
	}

	/// Bootstrap knows committed authority before any data answer is exposed.
	/// A collision is evidence, never a cache or a source of live rows.
	pub(crate) async fn stage_bootstrap_tier_authority(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		committed_id_floor: SlabId,
	) -> std::io::Result<()> {
		self.stage_tier_authority(pointer, last_control_log_index, committed_id_floor, false)
			.await
	}

	async fn stage_tier_authority(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		committed_id_floor: SlabId,
		snapshot: bool,
	) -> std::io::Result<()> {
		let lock = self.tier_publication_lock(pointer.slab_id);
		let _publication = lock.lock().await;
		self.stage_tier_authority_under_publication_lock(
			pointer,
			last_control_log_index,
			committed_id_floor,
			snapshot,
		)
		.await
	}

	/// Caller owns this slab's publication lock; log callbacks also retain the index read gate.
	pub(super) async fn stage_tier_authority_under_publication_lock(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
		committed_id_floor: SlabId,
		snapshot: bool,
	) -> std::io::Result<()> {
		if let Some(error) = self.tier_authority.integrity_error() {
			return Err(error);
		}
		// Authority staging (bootstrap, replay, snapshot) is a bulk event.
		self.clear_disposal_evidence().await;
		let known = self
			.tier_authority
			.pending
			.get(&pointer.slab_id)
			.map(|entry| entry.value().clone());
		if let Some(known) = &known
			&& let Some(index) = last_control_log_index
			&& known.last_control_log_index == Some(index)
			&& known.pointer != pointer
		{
			return Err(self
				.tier_authority
				.note_index_conflict(pointer.slab_id, index));
		}

		let matching_deferral = last_control_log_index.is_some()
			&& self
				.unapplied_tier_pointers
				.get(&pointer.slab_id)
				.is_some_and(|entry| entry.value().fences(&pointer));
		let superseded_deferral = last_control_log_index.is_some_and(|incoming| {
			known.as_ref().is_some_and(|old| {
				old.pointer != pointer
					&& old
						.last_control_log_index
						.is_none_or(|index| incoming > index)
					&& self
						.unapplied_tier_pointers
						.get(&pointer.slab_id)
						.is_some_and(|entry| entry.value().fences(&old.pointer))
			})
		});
		if snapshot
			&& self.unapplied_tier_pointers.contains_key(&pointer.slab_id)
			&& !matching_deferral
			&& !superseded_deferral
		{
			return Err(stale_location_error(
				pointer.slab_id,
				StaleLocationReason::Fenced,
			));
		}
		let staging = self
			.tier_authority
			.begin_staging(pointer.slab_id, snapshot)?;
		let mut state = self.state.lock().await;
		let id = pointer.slab_id;
		self.quarantine_bootstrap_collision_locked(&mut state, id, committed_id_floor, snapshot)
			.await?;
		let next = if state.next_slab_id > id {
			state.next_slab_id
		} else {
			self.cover_slab_ids_through(&mut state, id).await?
		};
		state.advance_slab_allocator(next)?;
		self.update_pending_tier_authority(pointer.clone(), last_control_log_index)?;
		self.resume_matching_tier_deferral(&pointer);
		drop(state);
		staging.finish();
		self.tier_authority.start();
		Ok(())
	}

	/// The DC-87 collision operation, shared by bootstrap and accepted-snapshot
	/// retries. Caller holds publication, index and state gates; every backing file is retained.
	async fn quarantine_bootstrap_collision_locked(
		&self,
		state: &mut ManagerState,
		id: SlabId,
		committed_id_floor: SlabId,
		snapshot: bool,
	) -> std::io::Result<()> {
		let active_collision = state.active.slab_id() == id;
		if snapshot
			&& ((active_collision && !state.active.manager_appends_quiescent())
				|| state
					.sealed
					.get(&id)
					.is_some_and(|slab| !slab.manager_appends_quiescent()))
		{
			return Err(Error::new(
				ErrorKind::WouldBlock,
				SnapshotAppendNotQuiescent(id),
			));
		}
		let local_collision = active_collision
			|| state.sealed.contains_key(&id)
			|| state.rehydrated.contains_key(&id);
		if local_collision {
			let bytes = self
				.file_ops
				.file_len(&slab_path(&self.config.dir, id))
				.await?
				.ok_or_else(|| {
					Error::new(ErrorKind::NotFound, "bootstrap collision file disappeared")
				})?;
			// Allocate before removing the old active backing. On failure the node
			// stays fenced and its original file remains available as evidence.
			let replacement = if active_collision {
				let staged_floor = self
					.tier_authority
					.pending
					.iter()
					.map(|entry| entry.key().saturating_add(1))
					.max()
					.unwrap_or(state.next_slab_id);
				let next = state
					.next_slab_id
					.max(id.saturating_add(1))
					.max(committed_id_floor.saturating_add(1))
					.max(staged_floor);
				if next <= id || next <= committed_id_floor {
					return Err(Error::other("slab allocator exhausted"));
				}
				let next_end = self.cover_slab_ids_through(state, next).await?;
				let slab = Arc::new(
					LocalSlabStore::open(
						slab_path(&self.config.dir, next),
						next,
						self.file_ops.clone(),
						self.config.group_commit.clone(),
					)
					.await?,
				);
				state.advance_slab_allocator(next_end)?;
				Some(slab)
			} else {
				None
			};
			self.quarantined_slab_files
				.insert_bootstrap(QuarantinedSlabFile {
					slab_id: id,
					bytes,
					reason: DisagreeingSlabFileReason::TierPointer,
				});
			state.sealed.remove(&id);
			state.rehydrated.remove(&id);
			if let Some(replacement) = replacement {
				state.active = replacement;
			}
		}
		Ok(())
	}

	/// Pending authority already has a durable canonical row. Quarantine its
	/// colliding local file before either verification or B8 retirement runs.
	async fn quarantine_pending_snapshot_collision(
		&self,
		id: SlabId,
		floor: SlabId,
		epoch: u64,
	) -> std::io::Result<()> {
		let lock = self.tier_publication_lock(id);
		let _publication = lock.lock().await;
		let _gate = self.persistent_index_gate.clone().read_owned().await;
		if self.capture_snapshot_epoch() != epoch
			|| self.tier_authority.closed.load(Ordering::Acquire)
		{
			return Err(obsolete_verification());
		}
		if let Some(error) = self.tier_authority.integrity_error() {
			return Err(error);
		}
		let mut state = self.state.lock().await;
		self.quarantine_bootstrap_collision_locked(&mut state, id, floor, true)
			.await
	}

	/// Called only by the typed accepted-snapshot deferral. The fresh lease
	/// binds these exact pointers to the durable canonical state at this index.
	pub(crate) async fn stage_deferred_snapshot_tiers(
		&self,
		canonical: &crate::consensus::TierSnapshotLease,
		index: Option<u64>,
		epoch: u64,
	) -> std::io::Result<()> {
		let index = index
			.filter(|index| Some(*index) == canonical.index())
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"snapshot deferral requires exact canonical index",
				)
			})?;
		let mut pointers = self
			.own_tier_view(canonical.tiers().iter(), canonical.incarnations())
			.ok_or_else(obsolete_verification)?;
		pointers.sort_by_key(|pointer| pointer.slab_id);
		let mut locks = Vec::new();
		for pointer in &pointers {
			locks.push(
				self.tier_publication_lock(pointer.slab_id)
					.lock_owned()
					.await,
			);
		}
		let gate = Arc::new(self.persistent_index_gate.clone().write_owned().await);
		if self.tier_authority.closed.load(Ordering::Acquire) {
			return Err(crate::storage::ownership::closed_error());
		}
		let persistent = self.persistent_index.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::WouldBlock,
				"persistent index unavailable for snapshot staging",
			)
		})?;
		let durable = persistent.load_snapshot().await?;
		let staged = pointers
			.iter()
			.filter(|pointer| {
				!durable
					.tiered_slabs
					.iter()
					.any(|stored| stored.slab_id == pointer.slab_id)
			})
			.cloned()
			.collect();
		persistent
			.stage_snapshot_pending_tiers(
				staged,
				pointers.clone(),
				epoch,
				self.snapshot_epoch.clone(),
				self.snapshot_epoch_gate.clone(),
				gate.clone(),
			)
			.await?;
		for pointer in pointers {
			let staging = self.tier_authority.begin_staging(pointer.slab_id, true)?;
			self.update_pending_tier_authority(pointer, Some(index))?;
			staging.finish();
		}
		drop(locks);
		Ok(())
	}

	pub(super) fn resume_matching_tier_deferral(&self, pointer: &TieredSlabPointer) {
		let registered = self
			.tier_authority
			.pending
			.get(&pointer.slab_id)
			.is_some_and(|entry| {
				entry.pointer == *pointer && entry.last_control_log_index.is_some()
			});
		if registered
			&& self
				.unapplied_tier_pointers
				.remove_if(&pointer.slab_id, |_, entry| entry.fences(pointer))
				.is_some()
		{
			self.tier_authority.changed.notify_waiters();
		}
	}

	/// Caller holds the per-slab publication lock. c5 will reuse this same
	/// pending flight for the remaining normal observer resume cases.
	pub(super) fn update_pending_tier_authority(
		&self,
		pointer: TieredSlabPointer,
		last_control_log_index: Option<u64>,
	) -> std::io::Result<()> {
		if let Some(error) = self.tier_authority.integrity_error() {
			return Err(error);
		}
		let current = self
			.tier_authority
			.pending
			.get(&pointer.slab_id)
			.map(|entry| entry.value().clone());
		if let Some(current) = &current {
			if let Some(index) = last_control_log_index
				&& current.last_control_log_index == Some(index)
				&& current.pointer != pointer
			{
				return Err(self
					.tier_authority
					.note_index_conflict(pointer.slab_id, index));
			}
			if last_control_log_index
				.zip(current.last_control_log_index)
				.is_some_and(|(incoming, known)| incoming < known)
			{
				return Ok(());
			}
			if last_control_log_index.is_none() && pointer != current.pointer {
				return Err(self.tier_authority_refusal());
			}
		}
		let superseded_deferral = current.as_ref().and_then(|old| {
			if old.pointer != pointer
				&& last_control_log_index.is_some_and(|incoming| {
					old.last_control_log_index
						.is_none_or(|known| incoming > known)
				}) {
				self.unapplied_tier_pointers
					.get(&pointer.slab_id)
					.filter(|entry| entry.value().fences(&old.pointer))
					.map(|entry| entry.value().clone())
			} else {
				None
			}
		});
		let last_control_log_index = last_control_log_index.or_else(|| {
			current
				.as_ref()
				.and_then(|current| current.last_control_log_index)
		});
		let next = PendingTierAuthority {
			pointer: pointer.clone(),
			last_control_log_index,
		};
		if current.as_ref() == Some(&next) {
			return Ok(());
		}
		{
			// Final Ready publication clears the pending map under this same
			// lock. It must never clear a newly staged pointer before seeing
			// that pointer's epoch invalidation.
			let mut status = self
				.tier_authority
				.status
				.lock()
				.unwrap_or_else(|e| e.into_inner());
			self.tier_authority.pending.insert(pointer.slab_id, next);
			self.unverified_tier_slabs.insert(pointer.slab_id);
			self.tier_authority.invalidate_locked(&mut status);
			if let Some(expected) = superseded_deferral {
				self.unapplied_tier_pointers
					.remove_if(&pointer.slab_id, |_, value| *value == expected);
			}
		}
		self.tier_authority.start();
		Ok(())
	}
}

impl Drop for SlabManager {
	fn drop(&mut self) {
		self.tier_authority.stop();
	}
}
