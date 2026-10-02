use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::distributed::identity::QueryId;
use crate::error::PartitionMissingReason;

pub const DEFAULT_RECEIPT_CAPACITY: usize = 4_096;
pub const DEFAULT_RECEIPT_HORIZON: Duration = Duration::from_secs(15 * 60);
pub const DEFAULT_QUERY_LEASE_RENEW_CADENCE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShuffleLifecycleConfig {
	receipt_capacity: NonZeroUsize,
	receipt_horizon: Duration,
	lease_renew_cadence: Duration,
	lease_term: Duration,
}

impl ShuffleLifecycleConfig {
	pub fn from_worker_loss_window(
		worker_lost_after: Duration,
	) -> Result<Self, LifecycleConfigError> {
		Self::new(
			NonZeroUsize::new(DEFAULT_RECEIPT_CAPACITY).expect("receipt capacity is non-zero"),
			DEFAULT_RECEIPT_HORIZON,
			DEFAULT_QUERY_LEASE_RENEW_CADENCE,
			worker_lost_after
				.checked_add(DEFAULT_QUERY_LEASE_RENEW_CADENCE)
				.and_then(|minimum| minimum.checked_add(Duration::from_millis(1)))
				.ok_or(LifecycleConfigError::DurationOverflow)?,
			worker_lost_after,
		)
	}

	pub fn new(
		receipt_capacity: NonZeroUsize,
		receipt_horizon: Duration,
		lease_renew_cadence: Duration,
		lease_term: Duration,
		worker_lost_after: Duration,
	) -> Result<Self, LifecycleConfigError> {
		let minimum = worker_lost_after
			.checked_add(lease_renew_cadence)
			.ok_or(LifecycleConfigError::DurationOverflow)?;
		if lease_term <= minimum {
			return Err(LifecycleConfigError::LeaseTermTooShort {
				lease_term,
				worker_lost_after,
				lease_renew_cadence,
			});
		}
		Ok(Self {
			receipt_capacity,
			receipt_horizon,
			lease_renew_cadence,
			lease_term,
		})
	}

	pub const fn receipt_capacity(self) -> NonZeroUsize {
		self.receipt_capacity
	}

	pub const fn receipt_horizon(self) -> Duration {
		self.receipt_horizon
	}

	pub const fn lease_renew_cadence(self) -> Duration {
		self.lease_renew_cadence
	}

	pub const fn lease_term(self) -> Duration {
		self.lease_term
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleConfigError {
	DurationOverflow,
	LeaseTermTooShort {
		lease_term: Duration,
		worker_lost_after: Duration,
		lease_renew_cadence: Duration,
	},
}

impl std::fmt::Display for LifecycleConfigError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::DurationOverflow => formatter.write_str("shuffle lifecycle duration overflow"),
			Self::LeaseTermTooShort {
				lease_term,
				worker_lost_after,
				lease_renew_cadence,
			} => write!(
				formatter,
				"query lease term {lease_term:?} must be greater than worker lost-after {worker_lost_after:?} plus renewal cadence {lease_renew_cadence:?}"
			),
		}
	}
}

impl std::error::Error for LifecycleConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryTerminalCause {
	Success,
	Cancelled,
	Failed,
	Abandoned,
}

impl QueryTerminalCause {
	pub const fn late_pull_reason(self) -> PartitionMissingReason {
		match self {
			Self::Success => PartitionMissingReason::NamespaceRetiring,
			Self::Cancelled => PartitionMissingReason::CancelledTaskCleanup,
			Self::Failed => PartitionMissingReason::QueryFailedCleanup,
			Self::Abandoned => PartitionMissingReason::Abandoned,
		}
	}

	const fn receipt_pull_reason(self) -> PartitionMissingReason {
		match self {
			Self::Success => PartitionMissingReason::Reclaimed,
			other => other.late_pull_reason(),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteLeaseError {
	Missing,
	Expired,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShuffleLifecycleSnapshot {
	pub active_leases: usize,
	pub retiring_namespaces: usize,
	pub receipts: usize,
	pub resident_namespaces: usize,
	pub retained_bytes: u64,
	pub physical_spill_files: usize,
	pub cleanup_debt: usize,
	pub cancelled_tombstones: usize,
	pub live_partition_records: usize,
	pub startup_swept_files: u64,
	pub oldest_resident_age_millis: u64,
	pub capacity_evictions: u64,
	pub horizon_evictions: u64,
	pub quiesced_reclaims: u64,
	pub cancelled_reclaims: u64,
	pub failed_reclaims: u64,
	pub abandoned_reclaims: u64,
}

/// One physical store holding a query's shuffle state under a lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShuffleBackend {
	Memory,
	Spill,
}

/// The backends a lifecycle governs. Every retirement waits for each of them; a backend's
/// completion is recorded at most once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShuffleBackends {
	memory: bool,
	spill: bool,
}

impl ShuffleBackends {
	pub const MEMORY: Self = Self {
		memory: true,
		spill: false,
	};
	pub const SPILL: Self = Self {
		memory: false,
		spill: true,
	};
	pub const MEMORY_AND_SPILL: Self = Self {
		memory: true,
		spill: true,
	};

	pub const fn contains(self, backend: ShuffleBackend) -> bool {
		match backend {
			ShuffleBackend::Memory => self.memory,
			ShuffleBackend::Spill => self.spill,
		}
	}

	const fn without(self, backend: ShuffleBackend) -> Self {
		match backend {
			ShuffleBackend::Memory => Self {
				memory: false,
				..self
			},
			ShuffleBackend::Spill => Self {
				spill: false,
				..self
			},
		}
	}

	const fn is_empty(self) -> bool {
		!self.memory && !self.spill
	}
}

/// Why a lease issuance or renewal was not applied. The reason is the state the request observed
/// at the lifecycle lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseIgnored {
	/// This request found the lease at or past its deadline and performed the expiry.
	Expired,
	/// The query is retiring (including an `Abandoned` retirement from an earlier expiry).
	Retiring { cause: QueryTerminalCause },
	/// The query's cleanup finished and its receipt is held.
	ReceiptHeld { cause: QueryTerminalCause },
}

/// The outcome of one issuance or renewal at the worker's linearization point (the lifecycle lock).
/// `Applied` certifies only that: no later retirement, write completion or computation
/// cancellation is implied, and it does not identify the namespace incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseRenewal {
	Applied,
	Ignored(LeaseIgnored),
}

#[derive(Debug, Clone)]
pub struct ShuffleNamespaceLifecycle {
	inner: Arc<Mutex<LifecycleState>>,
	config: ShuffleLifecycleConfig,
	enforce_leases: bool,
	backends: ShuffleBackends,
}

#[derive(Debug, Default)]
struct LifecycleState {
	leases: HashMap<QueryId, Lease>,
	namespaces_seen: HashSet<QueryId>,
	retiring: HashMap<QueryId, Retirement>,
	receipts: HashMap<QueryId, Receipt>,
	receipt_order: VecDeque<QueryId>,
	capacity_evictions: u64,
	horizon_evictions: u64,
	quiesced_reclaims: u64,
	cancelled_reclaims: u64,
	failed_reclaims: u64,
	abandoned_reclaims: u64,
}

#[derive(Debug, Clone, Copy)]
struct Receipt {
	cause: QueryTerminalCause,
	created_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct Lease {
	deadline: Instant,
	first_seen: Instant,
}

/// A logically terminal query whose physical cleanup is pending on `pending` backends. It stays
/// discoverable (`cleanup_work`) until every pending backend completes.
#[derive(Debug, Clone, Copy)]
struct Retirement {
	cause: QueryTerminalCause,
	pending: ShuffleBackends,
	receipt_licensed: bool,
}

impl ShuffleNamespaceLifecycle {
	/// A managed lifecycle governing the in-memory backend; stores spanning other backends declare
	/// them with [`Self::with_backends`] before the lifecycle is shared.
	pub fn managed(config: ShuffleLifecycleConfig) -> Self {
		Self {
			inner: Arc::new(Mutex::new(LifecycleState::default())),
			config,
			enforce_leases: true,
			backends: ShuffleBackends::MEMORY,
		}
	}

	/// Compatibility substrate for direct store fixtures. Production worker construction
	/// must use [`Self::managed`]; this mode deliberately has no lease admission policy.
	pub fn unmanaged_fixture(config: ShuffleLifecycleConfig) -> Self {
		Self {
			inner: Arc::new(Mutex::new(LifecycleState::default())),
			config,
			enforce_leases: false,
			backends: ShuffleBackends::MEMORY,
		}
	}

	/// Declares the backends every retirement waits for. Called once, at store construction.
	#[must_use]
	pub const fn with_backends(mut self, backends: ShuffleBackends) -> Self {
		self.backends = backends;
		self
	}

	pub const fn config(&self) -> ShuffleLifecycleConfig {
		self.config
	}

	pub const fn enforces_leases(&self) -> bool {
		self.enforce_leases
	}

	pub const fn backends(&self) -> ShuffleBackends {
		self.backends
	}

	/// Logical expiry: under the lifecycle lock, an elapsed lease (`now >= deadline`) is removed
	/// and an `Abandoned` retirement pending on every declared backend takes its place. Physical
	/// cleanup is left to the store (`cleanup_work`), never done here.
	fn expire_locked(&self, state: &mut LifecycleState, query_id: QueryId) {
		state.leases.remove(&query_id);
		let receipt_licensed = state.namespaces_seen.remove(&query_id);
		state.retiring.entry(query_id).or_insert(Retirement {
			cause: QueryTerminalCause::Abandoned,
			pending: self.backends,
			receipt_licensed,
		});
	}

	/// Expires `query_id` if its lease has elapsed; true when it did.
	fn expire_if_elapsed_locked(
		&self,
		state: &mut LifecycleState,
		query_id: QueryId,
		now: Instant,
	) -> bool {
		if state
			.leases
			.get(&query_id)
			.is_some_and(|lease| lease.deadline <= now)
		{
			self.expire_locked(state, query_id);
			return true;
		}
		false
	}

	// ADR 628 B0: the deadline is judged AT THE LIFECYCLE LOCK. Production callers use the `*_now`
	// forms, which sample the clock after acquiring the lock, so a call that waits for the lock
	// across the deadline sees it expired. The explicit-`now` forms are for deterministic tests.

	pub fn issue_or_renew(&self, query_id: QueryId, now: Instant) -> LeaseRenewal {
		self.issue_or_renew_with(query_id, || now)
	}

	pub fn issue_or_renew_now(&self, query_id: QueryId) -> LeaseRenewal {
		self.issue_or_renew_with(query_id, Instant::now)
	}

	pub fn authorize_write(&self, query_id: QueryId, now: Instant) -> Result<(), WriteLeaseError> {
		self.authorize_write_with(query_id, || now)
	}

	pub fn authorize_write_now(&self, query_id: QueryId) -> Result<(), WriteLeaseError> {
		self.authorize_write_with(query_id, Instant::now)
	}

	pub fn has_live_lease(&self, query_id: QueryId, now: Instant) -> bool {
		self.has_live_lease_with(query_id, || now)
	}

	pub fn has_live_lease_now(&self, query_id: QueryId) -> bool {
		self.has_live_lease_with(query_id, Instant::now)
	}

	pub fn cleanup_work(&self, now: Instant) -> Vec<(QueryId, QueryTerminalCause)> {
		self.cleanup_work_with(|| now)
	}

	pub fn cleanup_work_now(&self) -> Vec<(QueryId, QueryTerminalCause)> {
		self.cleanup_work_with(Instant::now)
	}

	fn issue_or_renew_with(
		&self,
		query_id: QueryId,
		now: impl FnOnce() -> Instant,
	) -> LeaseRenewal {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		let now = now();
		state.trim_receipts(now, self.config);
		if !self.enforce_leases {
			// The unmanaged fixture substrate has no lease admission policy: it keeps its pre-B0
			// bookkeeping (terminal evidence makes the renewal a no-op; an elapsed lease is
			// extended) and always answers `Applied`. B0's semantics govern managed lifecycles.
			if !state.retiring.contains_key(&query_id) && !state.receipts.contains_key(&query_id) {
				state
					.leases
					.entry(query_id)
					.and_modify(|lease| lease.deadline = now + self.config.lease_term)
					.or_insert(Lease {
						deadline: now + self.config.lease_term,
						first_seen: now,
					});
			}
			return LeaseRenewal::Applied;
		}
		// Terminal evidence dominates a delayed or late renewal: reopening here would authorize a
		// delayed writer to resurrect the namespace. Guaranteed only while the retirement or
		// receipt guard exists; after eviction a delayed message carrying the same id can lease
		// it again (ADR 628 B0.3).
		if let Some(receipt) = state.receipts.get(&query_id) {
			return LeaseRenewal::Ignored(LeaseIgnored::ReceiptHeld {
				cause: receipt.cause,
			});
		}
		if let Some(retirement) = state.retiring.get(&query_id) {
			return LeaseRenewal::Ignored(LeaseIgnored::Retiring {
				cause: retirement.cause,
			});
		}
		if self.expire_if_elapsed_locked(&mut state, query_id, now) {
			return LeaseRenewal::Ignored(LeaseIgnored::Expired);
		}
		state
			.leases
			.entry(query_id)
			.and_modify(|lease| lease.deadline = now + self.config.lease_term)
			.or_insert(Lease {
				deadline: now + self.config.lease_term,
				first_seen: now,
			});
		LeaseRenewal::Applied
	}

	fn authorize_write_with(
		&self,
		query_id: QueryId,
		now: impl FnOnce() -> Instant,
	) -> Result<(), WriteLeaseError> {
		if !self.enforce_leases {
			return Ok(());
		}
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		let now = now();
		state.trim_receipts(now, self.config);
		if self.expire_if_elapsed_locked(&mut state, query_id, now) {
			return Err(WriteLeaseError::Expired);
		}
		if state.leases.contains_key(&query_id) {
			Ok(())
		} else {
			Err(WriteLeaseError::Missing)
		}
	}

	/// Record that this worker actually created query-scoped shuffle state. A lease
	/// alone is not namespace evidence: final-result-only workers receive leases too.
	pub fn mark_namespace_present(&self, query_id: QueryId) {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		if let Some(retirement) = state.retiring.get_mut(&query_id) {
			// A write authorized immediately before retirement (or expiry) may commit while
			// retirement waits on the store lock. Preserve that namespace evidence in the
			// already-created retirement record.
			retirement.receipt_licensed = true;
		} else if !state.receipts.contains_key(&query_id) {
			state.namespaces_seen.insert(query_id);
		}
	}

	fn has_live_lease_with(&self, query_id: QueryId, now: impl FnOnce() -> Instant) -> bool {
		if !self.enforce_leases {
			return true;
		}
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		let now = now();
		if self.expire_if_elapsed_locked(&mut state, query_id, now) {
			return false;
		}
		state.leases.contains_key(&query_id)
	}

	/// Begins (or joins) retirement and returns the EFFECTIVE cause: an existing retirement or
	/// receipt keeps its cause, which then governs physical cleanup and missing reasons for every
	/// later request. A new retirement waits on every declared backend.
	pub fn begin_retirement(
		&self,
		query_id: QueryId,
		cause: QueryTerminalCause,
	) -> QueryTerminalCause {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		if let Some(receipt) = state.receipts.get(&query_id) {
			return receipt.cause;
		}
		if let Some(retirement) = state.retiring.get(&query_id) {
			return retirement.cause;
		}
		state.leases.remove(&query_id);
		let receipt_licensed = state.namespaces_seen.remove(&query_id);
		state.retiring.insert(
			query_id,
			Retirement {
				cause,
				pending: self.backends,
				receipt_licensed,
			},
		);
		cause
	}

	pub fn retirement_cause(&self, query_id: QueryId) -> Option<QueryTerminalCause> {
		self.inner
			.lock()
			.expect("shuffle lifecycle poisoned")
			.retiring
			.get(&query_id)
			.map(|retirement| retirement.cause)
	}

	/// Records that `backend` finished removing `query_id`. Idempotent per backend; the receipt
	/// (when licensed) is written only once every pending backend has completed.
	pub fn finish_removal(&self, query_id: QueryId, backend: ShuffleBackend, now: Instant) {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		let Some(retirement) = state.retiring.get_mut(&query_id) else {
			return;
		};
		if !retirement.pending.contains(backend) {
			return;
		}
		retirement.pending = retirement.pending.without(backend);
		if !retirement.pending.is_empty() {
			return;
		}
		let retirement = state
			.retiring
			.remove(&query_id)
			.expect("retirement checked above");
		if !retirement.receipt_licensed {
			return;
		}
		let cause = retirement.cause;
		state.insert_receipt(query_id, cause, now, self.config);
		match cause {
			QueryTerminalCause::Success => state.quiesced_reclaims += 1,
			QueryTerminalCause::Cancelled => state.cancelled_reclaims += 1,
			QueryTerminalCause::Failed => state.failed_reclaims += 1,
			QueryTerminalCause::Abandoned => state.abandoned_reclaims += 1,
		}
	}

	pub fn missing_reason(&self, query_id: QueryId, now: Instant) -> PartitionMissingReason {
		if !self.enforce_leases {
			return PartitionMissingReason::StoreEvicted;
		}
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		state.trim_receipts(now, self.config);
		state
			.receipts
			.get(&query_id)
			.map_or(PartitionMissingReason::NamespaceUnknown, |receipt| {
				receipt.cause.receipt_pull_reason()
			})
	}

	/// The reaper's work, with each query's effective cause: every elapsed lease is expired here
	/// (`now >= deadline`), and EVERY retirement still pending on some backend is returned, so an
	/// expiry observed by a write or renewal, or a pass that could not finish (a pinned reader,
	/// spill deletion debt, an interruption), is picked up again by the next pass. The caller does
	/// the physical cleanup without holding this lock.
	fn cleanup_work_with(
		&self,
		now: impl FnOnce() -> Instant,
	) -> Vec<(QueryId, QueryTerminalCause)> {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		let now = now();
		state.trim_receipts(now, self.config);
		let elapsed = state
			.leases
			.iter()
			.filter_map(|(query_id, lease)| (lease.deadline <= now).then_some(*query_id))
			.collect::<Vec<_>>();
		for query_id in elapsed {
			self.expire_locked(&mut state, query_id);
		}
		let mut work = state
			.retiring
			.iter()
			.map(|(query_id, retirement)| (*query_id, retirement.cause))
			.collect::<Vec<_>>();
		work.sort_by_key(|(query_id, _)| *query_id);
		work
	}

	pub fn snapshot(&self, now: Instant) -> ShuffleLifecycleSnapshot {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		state.trim_receipts(now, self.config);
		ShuffleLifecycleSnapshot {
			active_leases: state.leases.len(),
			retiring_namespaces: state.retiring.len(),
			receipts: state.receipts.len(),
			capacity_evictions: state.capacity_evictions,
			horizon_evictions: state.horizon_evictions,
			quiesced_reclaims: state.quiesced_reclaims,
			cancelled_reclaims: state.cancelled_reclaims,
			failed_reclaims: state.failed_reclaims,
			abandoned_reclaims: state.abandoned_reclaims,
			oldest_resident_age_millis: state
				.leases
				.values()
				.map(|lease| now.saturating_duration_since(lease.first_seen))
				.max()
				.map(|age| u64::try_from(age.as_millis()).unwrap_or(u64::MAX))
				.unwrap_or(0),
			..ShuffleLifecycleSnapshot::default()
		}
	}
}

impl LifecycleState {
	fn insert_receipt(
		&mut self,
		query_id: QueryId,
		cause: QueryTerminalCause,
		now: Instant,
		config: ShuffleLifecycleConfig,
	) {
		self.remove_receipt(query_id);
		self.receipts.insert(
			query_id,
			Receipt {
				cause,
				created_at: now,
			},
		);
		self.receipt_order.push_back(query_id);
		while self.receipts.len() > config.receipt_capacity.get() {
			let Some(oldest) = self.receipt_order.pop_front() else {
				break;
			};
			if self.receipts.remove(&oldest).is_some() {
				self.capacity_evictions += 1;
			}
		}
	}

	fn remove_receipt(&mut self, query_id: QueryId) {
		self.receipts.remove(&query_id);
		self.receipt_order
			.retain(|candidate| *candidate != query_id);
	}

	fn trim_receipts(&mut self, now: Instant, config: ShuffleLifecycleConfig) {
		while let Some(query_id) = self.receipt_order.front().copied() {
			let Some(receipt) = self.receipts.get(&query_id).copied() else {
				self.receipt_order.pop_front();
				continue;
			};
			if now.saturating_duration_since(receipt.created_at) < config.receipt_horizon {
				break;
			}
			self.receipt_order.pop_front();
			self.receipts.remove(&query_id);
			self.horizon_evictions += 1;
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn config(capacity: usize, horizon: Duration) -> ShuffleLifecycleConfig {
		ShuffleLifecycleConfig::new(
			NonZeroUsize::new(capacity).expect("capacity"),
			horizon,
			Duration::from_secs(5),
			Duration::from_secs(16),
			Duration::from_secs(10),
		)
		.expect("config")
	}

	#[test]
	fn lease_term_must_exceed_loss_window_plus_renewal() {
		let error = ShuffleLifecycleConfig::new(
			NonZeroUsize::new(1).unwrap(),
			Duration::from_secs(1),
			Duration::from_secs(5),
			Duration::from_secs(15),
			Duration::from_secs(10),
		)
		.expect_err("equal is not greater");
		assert!(matches!(
			error,
			LifecycleConfigError::LeaseTermTooShort { .. }
		));
	}

	#[test]
	fn receipts_are_capacity_first_then_horizon_bounded() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(2, Duration::from_secs(10)));
		let now = Instant::now();
		for value in 1..=3 {
			let query_id = QueryId::from_u128(value);
			lifecycle.issue_or_renew(query_id, now);
			lifecycle.mark_namespace_present(query_id);
			lifecycle.begin_retirement(query_id, QueryTerminalCause::Success);
			lifecycle.finish_removal(query_id, ShuffleBackend::Memory, now);
		}
		let snapshot = lifecycle.snapshot(now);
		assert_eq!(snapshot.receipts, 2);
		assert_eq!(snapshot.capacity_evictions, 1);
		assert_eq!(
			lifecycle.missing_reason(QueryId::from_u128(1), now),
			PartitionMissingReason::NamespaceUnknown
		);
		assert_eq!(
			lifecycle.missing_reason(QueryId::from_u128(3), now),
			PartitionMissingReason::Reclaimed
		);

		let later = now + Duration::from_secs(11);
		let snapshot = lifecycle.snapshot(later);
		assert_eq!(snapshot.receipts, 0);
		assert_eq!(snapshot.horizon_evictions, 2);
	}

	#[test]
	fn write_admission_is_positive_and_expiry_is_abandonment_evidence() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(2, Duration::from_secs(10)));
		let now = Instant::now();
		let query_id = QueryId::from_u128(7);
		assert_eq!(
			lifecycle.authorize_write(query_id, now),
			Err(WriteLeaseError::Missing)
		);
		lifecycle.issue_or_renew(query_id, now);
		assert_eq!(lifecycle.authorize_write(query_id, now), Ok(()));
		let expired_at = now + Duration::from_secs(17);
		assert_eq!(
			lifecycle.cleanup_work(expired_at),
			vec![(query_id, QueryTerminalCause::Abandoned)]
		);
		assert_eq!(
			lifecycle.retirement_cause(query_id),
			Some(QueryTerminalCause::Abandoned)
		);
	}

	#[test]
	fn terminal_state_dominates_an_inflight_or_late_lease_renewal() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(2, Duration::from_secs(10)));
		let now = Instant::now();
		let query_id = QueryId::from_u128(8);
		lifecycle.issue_or_renew(query_id, now);
		lifecycle.mark_namespace_present(query_id);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Success);

		lifecycle.issue_or_renew(query_id, now + Duration::from_millis(1));
		assert_eq!(
			lifecycle.authorize_write(query_id, now + Duration::from_millis(1)),
			Err(WriteLeaseError::Missing),
			"an in-flight renewal must not reopen a retiring namespace"
		);

		lifecycle.finish_removal(
			query_id,
			ShuffleBackend::Memory,
			now + Duration::from_millis(2),
		);
		lifecycle.issue_or_renew(query_id, now + Duration::from_millis(3));
		assert_eq!(
			lifecycle.authorize_write(query_id, now + Duration::from_millis(3)),
			Err(WriteLeaseError::Missing),
			"a late renewal must not erase terminal receipt evidence"
		);
		assert_eq!(
			lifecycle.missing_reason(query_id, now + Duration::from_millis(3)),
			PartitionMissingReason::Reclaimed
		);
	}

	#[test]
	fn lease_without_namespace_never_licenses_reclaimed() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(2, Duration::from_secs(10)));
		let now = Instant::now();
		let query_id = QueryId::from_u128(9);
		lifecycle.issue_or_renew(query_id, now);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Success);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, now);

		assert_eq!(lifecycle.snapshot(now).receipts, 0);
		assert_eq!(
			lifecycle.missing_reason(query_id, now),
			PartitionMissingReason::NamespaceUnknown
		);
	}

	// ---- ADR 628 Stage A race (c) pins: the documented residual re-lease once the guard is gone
	// (B0.3). The test config's lease term is 16 s; every instant is explicit. ----

	/// Race candidate (c), capacity: once a receipt is evicted, the retired query id can be leased
	/// and written again.
	#[test]
	fn adr628_retired_query_can_be_leased_again_after_capacity_eviction() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(1, Duration::from_secs(600)));
		let now = Instant::now();
		let (old, newer) = (QueryId::from_u128(23), QueryId::from_u128(24));
		for query_id in [old, newer] {
			lifecycle.issue_or_renew(query_id, now);
			lifecycle.mark_namespace_present(query_id);
			lifecycle.begin_retirement(query_id, QueryTerminalCause::Success);
			lifecycle.finish_removal(query_id, ShuffleBackend::Memory, now);
		}
		assert_eq!(lifecycle.snapshot(now).receipts, 1);
		lifecycle.issue_or_renew(old, now);
		assert_eq!(lifecycle.authorize_write(old, now), Ok(()));
		lifecycle.issue_or_renew(newer, now);
		assert_eq!(
			lifecycle.authorize_write(newer, now),
			Err(WriteLeaseError::Missing),
			"the held receipt still guards"
		);
	}

	/// Race candidate (c), horizon: past the receipt horizon, the retired query id can be leased
	/// and written again.
	#[test]
	fn adr628_retired_query_can_be_leased_again_after_the_receipt_horizon() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(4, Duration::from_secs(10)));
		let t0 = Instant::now();
		let query_id = QueryId::from_u128(25);
		lifecycle.issue_or_renew(query_id, t0);
		lifecycle.mark_namespace_present(query_id);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Success);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, t0);
		lifecycle.issue_or_renew(query_id, t0 + Duration::from_secs(9));
		assert_eq!(
			lifecycle.authorize_write(query_id, t0 + Duration::from_secs(9)),
			Err(WriteLeaseError::Missing)
		);
		let past = t0 + Duration::from_secs(11);
		lifecycle.issue_or_renew(query_id, past);
		assert_eq!(lifecycle.authorize_write(query_id, past), Ok(()));
	}

	/// Race candidate (c), no receipt: a retirement with no namespace licenses no receipt, so the
	/// same query id can be leased and written again at once.
	#[test]
	fn adr628_retirement_without_a_namespace_leaves_the_id_leasable() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(4, Duration::from_secs(600)));
		let now = Instant::now();
		let query_id = QueryId::from_u128(26);
		lifecycle.issue_or_renew(query_id, now);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Failed);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, now);
		assert_eq!(lifecycle.snapshot(now).receipts, 0);
		lifecycle.issue_or_renew(query_id, now);
		assert_eq!(lifecycle.authorize_write(query_id, now), Ok(()));
	}

	#[test]
	fn namespace_commit_racing_retirement_preserves_receipt_evidence() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(2, Duration::from_secs(10)));
		let now = Instant::now();
		let query_id = QueryId::from_u128(10);
		lifecycle.issue_or_renew(query_id, now);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Success);
		lifecycle.mark_namespace_present(query_id);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, now);

		assert_eq!(lifecycle.snapshot(now).receipts, 1);
		assert_eq!(
			lifecycle.missing_reason(query_id, now),
			PartitionMissingReason::Reclaimed
		);
	}

	// ---- ADR 628 B0 invariants (they replace the Stage A pins
	// adr628_renewal_of_an_expired_lease_before_retirement_extends_it,
	// adr628_expired_write_then_reaper_starts_no_retirement and
	// adr628_renewal_between_reaper_enumeration_and_retirement_is_applied_then_reclaimed).
	// Lease term 16 s; every instant is explicit. ----

	const TERM: Duration = Duration::from_secs(16);

	fn leased(
		backends: ShuffleBackends,
		value: u128,
	) -> (ShuffleNamespaceLifecycle, QueryId, Instant) {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(4, Duration::from_secs(600)))
			.with_backends(backends);
		let t0 = Instant::now();
		let query_id = QueryId::from_u128(value);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, t0),
			LeaseRenewal::Applied
		);
		lifecycle.mark_namespace_present(query_id);
		(lifecycle, query_id, t0)
	}

	#[test]
	fn the_deadline_itself_is_expired_for_every_observer() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 40);
		assert_eq!(
			lifecycle.authorize_write(query_id, t0 + TERM),
			Err(WriteLeaseError::Expired)
		);
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 41);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, t0 + TERM),
			LeaseRenewal::Ignored(LeaseIgnored::Expired)
		);
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 42);
		assert!(!lifecycle.has_live_lease(query_id, t0 + TERM));
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 43);
		assert_eq!(
			lifecycle.cleanup_work(t0 + TERM),
			vec![(query_id, QueryTerminalCause::Abandoned)]
		);
		// Each observer above left the query retiring as Abandoned.
		assert_eq!(
			lifecycle.retirement_cause(query_id),
			Some(QueryTerminalCause::Abandoned)
		);
	}

	#[test]
	fn a_valid_renewal_before_the_deadline_extends_it() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 44);
		let just_before = t0 + TERM - Duration::from_nanos(1);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, just_before),
			LeaseRenewal::Applied
		);
		assert_eq!(lifecycle.authorize_write(query_id, t0 + TERM), Ok(()));
		assert_eq!(
			lifecycle.authorize_write(query_id, just_before + TERM),
			Err(WriteLeaseError::Expired),
			"the renewed deadline is the current one"
		);
	}

	#[test]
	fn an_elapsed_lease_is_not_renewable_before_the_reaper() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 45);
		let late = t0 + TERM + Duration::from_secs(1);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, late),
			LeaseRenewal::Ignored(LeaseIgnored::Expired)
		);
		assert_eq!(
			lifecycle.authorize_write(query_id, late),
			Err(WriteLeaseError::Missing)
		);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, late),
			LeaseRenewal::Ignored(LeaseIgnored::Retiring {
				cause: QueryTerminalCause::Abandoned
			}),
			"a later renewal reports the state it found"
		);
		assert_eq!(
			lifecycle.cleanup_work(late),
			vec![(query_id, QueryTerminalCause::Abandoned)]
		);
	}

	#[test]
	fn an_expired_write_retires_the_query_for_the_reaper() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 46);
		let expired = t0 + TERM + Duration::from_secs(1);
		assert_eq!(
			lifecycle.authorize_write(query_id, expired),
			Err(WriteLeaseError::Expired)
		);
		assert_eq!(
			lifecycle.retirement_cause(query_id),
			Some(QueryTerminalCause::Abandoned)
		);
		assert_eq!(
			lifecycle.cleanup_work(expired),
			vec![(query_id, QueryTerminalCause::Abandoned)],
			"the reaper discovers an expiry observed by a write"
		);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, expired);
		assert_eq!(lifecycle.snapshot(expired).abandoned_reclaims, 1);
		assert_eq!(
			lifecycle.missing_reason(query_id, expired),
			PartitionMissingReason::Abandoned
		);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, expired),
			LeaseRenewal::Ignored(LeaseIgnored::ReceiptHeld {
				cause: QueryTerminalCause::Abandoned
			})
		);
	}

	#[test]
	fn no_renewal_window_between_reaper_enumeration_and_retirement() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 47);
		let reap = t0 + TERM + Duration::from_secs(1);
		assert_eq!(
			lifecycle.cleanup_work(reap),
			vec![(query_id, QueryTerminalCause::Abandoned)]
		);
		// The renewal lands after enumeration, before the store's physical cleanup.
		assert_eq!(
			lifecycle.issue_or_renew(query_id, reap),
			LeaseRenewal::Ignored(LeaseIgnored::Retiring {
				cause: QueryTerminalCause::Abandoned
			})
		);
		assert_eq!(
			lifecycle.authorize_write(query_id, reap),
			Err(WriteLeaseError::Missing)
		);
		assert_eq!(
			lifecycle.begin_retirement(query_id, QueryTerminalCause::Abandoned),
			QueryTerminalCause::Abandoned
		);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, reap);
		assert_eq!(
			lifecycle.missing_reason(query_id, reap),
			PartitionMissingReason::Abandoned
		);
	}

	#[test]
	fn an_existing_terminal_cause_is_retained_in_either_order() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 48);
		assert_eq!(
			lifecycle.begin_retirement(query_id, QueryTerminalCause::Success),
			QueryTerminalCause::Success
		);
		let late = t0 + TERM + Duration::from_secs(1);
		assert_eq!(
			lifecycle.cleanup_work(late),
			vec![(query_id, QueryTerminalCause::Success)]
		);
		assert_eq!(
			lifecycle.begin_retirement(query_id, QueryTerminalCause::Abandoned),
			QueryTerminalCause::Success,
			"a later request gets the retained cause"
		);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, late);
		assert_eq!(
			lifecycle.missing_reason(query_id, late),
			PartitionMissingReason::Reclaimed
		);
		assert_eq!(
			lifecycle.begin_retirement(query_id, QueryTerminalCause::Cancelled),
			QueryTerminalCause::Success,
			"the receipt keeps it too"
		);
		let snapshot = lifecycle.snapshot(late);
		assert_eq!(
			(
				snapshot.quiesced_reclaims,
				snapshot.abandoned_reclaims,
				snapshot.cancelled_reclaims
			),
			(1, 0, 0)
		);

		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 49);
		let late = t0 + TERM;
		assert_eq!(
			lifecycle.authorize_write(query_id, late),
			Err(WriteLeaseError::Expired)
		);
		assert_eq!(
			lifecycle.begin_retirement(query_id, QueryTerminalCause::Success),
			QueryTerminalCause::Abandoned,
			"an expiry's Abandoned is retained against a later Success"
		);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, late);
		assert_eq!(
			lifecycle.missing_reason(query_id, late),
			PartitionMissingReason::Abandoned
		);
	}

	#[test]
	fn completion_waits_for_every_declared_backend_and_counts_each_once() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY_AND_SPILL, 50);
		let late = t0 + TERM;
		assert_eq!(
			lifecycle.authorize_write(query_id, late),
			Err(WriteLeaseError::Expired)
		);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, late);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, late);
		assert_eq!(
			lifecycle.snapshot(late).receipts,
			0,
			"no receipt until both backends finish"
		);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, late),
			LeaseRenewal::Ignored(LeaseIgnored::Retiring {
				cause: QueryTerminalCause::Abandoned
			}),
			"no reopening between backends"
		);
		assert_eq!(
			lifecycle.cleanup_work(late),
			vec![(query_id, QueryTerminalCause::Abandoned)],
			"still discoverable with one backend pending"
		);
		lifecycle.finish_removal(query_id, ShuffleBackend::Spill, late);
		lifecycle.finish_removal(query_id, ShuffleBackend::Spill, late);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, late);
		let snapshot = lifecycle.snapshot(late);
		assert_eq!((snapshot.receipts, snapshot.abandoned_reclaims), (1, 1));
		assert!(lifecycle.cleanup_work(late).is_empty());
	}

	#[test]
	fn interrupted_cleanup_stays_discoverable_until_complete() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY_AND_SPILL, 51);
		let reap = t0 + TERM;
		for _ in 0..3 {
			// A pass that collected the work but could not (or did not) finish.
			assert_eq!(
				lifecycle.cleanup_work(reap),
				vec![(query_id, QueryTerminalCause::Abandoned)]
			);
		}
		lifecycle.finish_removal(query_id, ShuffleBackend::Spill, reap);
		assert_eq!(
			lifecycle.cleanup_work(reap),
			vec![(query_id, QueryTerminalCause::Abandoned)]
		);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, reap);
		assert!(lifecycle.cleanup_work(reap).is_empty());
	}

	#[test]
	fn a_write_authorized_before_expiry_keeps_its_namespace_licensed() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(4, Duration::from_secs(600)));
		let t0 = Instant::now();
		let query_id = QueryId::from_u128(52);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, t0),
			LeaseRenewal::Applied
		);
		assert_eq!(
			lifecycle.authorize_write(query_id, t0 + TERM - Duration::from_millis(1)),
			Ok(())
		);
		assert_eq!(
			lifecycle.cleanup_work(t0 + TERM),
			vec![(query_id, QueryTerminalCause::Abandoned)]
		);
		// The earlier-authorized write commits after the expiry.
		lifecycle.mark_namespace_present(query_id);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, t0 + TERM);
		assert_eq!(lifecycle.snapshot(t0 + TERM).receipts, 1);
		assert_eq!(
			lifecycle.missing_reason(query_id, t0 + TERM),
			PartitionMissingReason::Abandoned
		);
	}

	/// ADR 628 B0, supplemental: a seeded, bounded SERIAL randomized state-machine sequence (one
	/// thread, no barriers; not a concurrency test) of renew, write (+ namespace commit), clock
	/// advance, reaper pass, retire and per-backend finish over four queries on a two-backend
	/// lifecycle. After every step:
	/// (1) every query with committed namespace state and no receipt has a discoverable lease
	///     entry (possibly elapsed, pending observation) or a retirement record;
	/// (2) a write is authorized only strictly before the query's CURRENT deadline, tracked from
	///     the `Applied` renewals the model has seen.
	fn short_lifecycle() -> ShuffleNamespaceLifecycle {
		ShuffleNamespaceLifecycle::managed(
			ShuffleLifecycleConfig::new(
				NonZeroUsize::new(4).expect("capacity"),
				Duration::from_secs(600),
				Duration::from_millis(1),
				Duration::from_millis(40),
				Duration::from_millis(2),
			)
			.expect("config"),
		)
	}

	/// ADR 628 B0: the deadline is judged at the lifecycle lock (actual clock). A write and a
	/// renewal that start before the deadline but acquire the lock after it are denied/ignored as
	/// expired; a lock released before the deadline lets them through.
	#[test]
	fn calls_waiting_for_the_lock_across_the_deadline_see_it_expired() {
		for hold in [Duration::from_millis(80), Duration::from_millis(1)] {
			let lifecycle = short_lifecycle();
			let (writer, renewer) = (QueryId::from_u128(70), QueryId::from_u128(71));
			assert_eq!(lifecycle.issue_or_renew_now(writer), LeaseRenewal::Applied);
			assert_eq!(lifecycle.issue_or_renew_now(renewer), LeaseRenewal::Applied);
			let held = lifecycle.inner.lock().expect("lifecycle");
			let write = std::thread::spawn({
				let lifecycle = lifecycle.clone();
				move || lifecycle.authorize_write_now(writer)
			});
			let renew = std::thread::spawn({
				let lifecycle = lifecycle.clone();
				move || lifecycle.issue_or_renew_now(renewer)
			});
			std::thread::sleep(hold);
			drop(held);
			let (write, renew) = (
				write.join().expect("writer"),
				renew.join().expect("renewer"),
			);
			if hold > Duration::from_millis(40) {
				assert_eq!(write, Err(WriteLeaseError::Expired));
				assert_eq!(renew, LeaseRenewal::Ignored(LeaseIgnored::Expired));
			} else {
				assert_eq!(write, Ok(()), "acquired before the deadline");
				assert_eq!(renew, LeaseRenewal::Applied, "acquired before the deadline");
			}
		}
	}

	/// ADR 628 B0: the unmanaged fixture substrate keeps its pre-B0 answer (always `Applied`, no
	/// admission policy), even for a receipted query or an elapsed lease.
	#[test]
	fn the_unmanaged_fixture_substrate_always_answers_applied() {
		let lifecycle =
			ShuffleNamespaceLifecycle::unmanaged_fixture(config(4, Duration::from_secs(600)));
		let t0 = Instant::now();
		let query_id = QueryId::from_u128(60);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, t0),
			LeaseRenewal::Applied
		);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, t0 + TERM),
			LeaseRenewal::Applied
		);
		lifecycle.mark_namespace_present(query_id);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Success);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, t0 + TERM);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, t0 + TERM),
			LeaseRenewal::Applied
		);
		assert_eq!(lifecycle.authorize_write(query_id, t0 + TERM), Ok(()));
	}

	#[test]
	fn seeded_serial_sequences_keep_state_discoverable_and_writes_before_the_deadline() {
		for seed in 1..=64u64 {
			let lifecycle =
				ShuffleNamespaceLifecycle::managed(config(64, Duration::from_secs(600)))
					.with_backends(ShuffleBackends::MEMORY_AND_SPILL);
			let t0 = Instant::now();
			let mut now = t0;
			let mut state = seed;
			let mut next = move |bound: u64| {
				state = state
					.wrapping_mul(6364136223846793005)
					.wrapping_add(1442695040888963407);
				(state >> 33) % bound
			};
			let queries = (0..4u128)
				.map(|value| QueryId::from_u128(900 + value))
				.collect::<Vec<_>>();
			let mut deadline: HashMap<QueryId, Instant> = HashMap::new();
			let mut committed: HashSet<QueryId> = HashSet::new();
			for step in 0..400 {
				let query_id = queries[next(4) as usize];
				match next(7) {
					0 => {
						if lifecycle.issue_or_renew(query_id, now) == LeaseRenewal::Applied {
							deadline.insert(query_id, now + TERM);
						}
					}
					1 => {
						if lifecycle.authorize_write(query_id, now).is_ok() {
							let current = deadline.get(&query_id).copied();
							assert!(
								current.is_some_and(|deadline| now < deadline),
								"seed {seed} step {step}: write authorized at/after the deadline"
							);
							lifecycle.mark_namespace_present(query_id);
							committed.insert(query_id);
						}
					}
					2 => now += Duration::from_millis(next(9_000)),
					3 => {
						let _ = lifecycle.cleanup_work(now);
					}
					4 => {
						let cause = [
							QueryTerminalCause::Success,
							QueryTerminalCause::Cancelled,
							QueryTerminalCause::Failed,
							QueryTerminalCause::Abandoned,
						][next(4) as usize];
						let _ = lifecycle.begin_retirement(query_id, cause);
					}
					5 => lifecycle.finish_removal(query_id, ShuffleBackend::Memory, now),
					_ => lifecycle.finish_removal(query_id, ShuffleBackend::Spill, now),
				}
				let guard = lifecycle.inner.lock().expect("lifecycle");
				for query_id in &committed {
					let held = guard.leases.contains_key(query_id)
						|| guard.retiring.contains_key(query_id)
						|| guard.receipts.contains_key(query_id);
					assert!(
						held,
						"seed {seed} step {step}: committed state undiscoverable"
					);
				}
				// A query whose retirement finished with a receipt is no longer resident.
				committed.retain(|query_id| !guard.receipts.contains_key(query_id));
				for query_id in guard.retiring.keys() {
					assert!(
						!guard.leases.contains_key(query_id),
						"seed {seed} step {step}: a retiring query also holds a lease"
					);
				}
			}
		}
	}

	#[test]
	fn a_retiring_or_receipted_renewal_reports_its_cause() {
		let (lifecycle, query_id, t0) = leased(ShuffleBackends::MEMORY, 53);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Cancelled);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, t0),
			LeaseRenewal::Ignored(LeaseIgnored::Retiring {
				cause: QueryTerminalCause::Cancelled
			})
		);
		lifecycle.finish_removal(query_id, ShuffleBackend::Memory, t0);
		assert_eq!(
			lifecycle.issue_or_renew(query_id, t0),
			LeaseRenewal::Ignored(LeaseIgnored::ReceiptHeld {
				cause: QueryTerminalCause::Cancelled
			})
		);
	}
}
