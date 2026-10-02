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

#[derive(Debug, Clone)]
pub struct ShuffleNamespaceLifecycle {
	inner: Arc<Mutex<LifecycleState>>,
	config: ShuffleLifecycleConfig,
	enforce_leases: bool,
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

#[derive(Debug, Clone, Copy)]
struct Retirement {
	cause: QueryTerminalCause,
	remaining_backends: usize,
	receipt_licensed: bool,
}

impl ShuffleNamespaceLifecycle {
	pub fn managed(config: ShuffleLifecycleConfig) -> Self {
		Self {
			inner: Arc::new(Mutex::new(LifecycleState::default())),
			config,
			enforce_leases: true,
		}
	}

	/// Compatibility substrate for direct store fixtures. Production worker construction
	/// must use [`Self::managed`]; this mode deliberately has no lease admission policy.
	pub fn unmanaged_fixture(config: ShuffleLifecycleConfig) -> Self {
		Self {
			inner: Arc::new(Mutex::new(LifecycleState::default())),
			config,
			enforce_leases: false,
		}
	}

	pub const fn config(&self) -> ShuffleLifecycleConfig {
		self.config
	}

	pub const fn enforces_leases(&self) -> bool {
		self.enforce_leases
	}

	pub fn issue_or_renew(&self, query_id: QueryId, now: Instant) {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		state.trim_receipts(now, self.config);
		// A renewal already in flight when the coordinator declares terminal may
		// arrive after retirement. Terminal evidence dominates that stale renewal:
		// reopening here would authorize a delayed writer to resurrect the namespace.
		if state.retiring.contains_key(&query_id) || state.receipts.contains_key(&query_id) {
			return;
		}
		state
			.leases
			.entry(query_id)
			.and_modify(|lease| lease.deadline = now + self.config.lease_term)
			.or_insert(Lease {
				deadline: now + self.config.lease_term,
				first_seen: now,
			});
	}

	pub fn authorize_write(&self, query_id: QueryId, now: Instant) -> Result<(), WriteLeaseError> {
		if !self.enforce_leases {
			return Ok(());
		}
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		state.trim_receipts(now, self.config);
		match state.leases.get(&query_id).copied() {
			Some(lease) if lease.deadline > now => Ok(()),
			Some(_) => {
				state.leases.remove(&query_id);
				Err(WriteLeaseError::Expired)
			}
			None => Err(WriteLeaseError::Missing),
		}
	}

	/// Record that this worker actually created query-scoped shuffle state. A lease
	/// alone is not namespace evidence: final-result-only workers receive leases too.
	pub fn mark_namespace_present(&self, query_id: QueryId) {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		if let Some(retirement) = state.retiring.get_mut(&query_id) {
			// A write authorized immediately before retirement may commit while
			// retirement waits on the store lock. Preserve that namespace evidence
			// in the already-created retirement record.
			retirement.receipt_licensed = true;
		} else if !state.receipts.contains_key(&query_id) {
			state.namespaces_seen.insert(query_id);
		}
	}

	pub fn has_live_lease(&self, query_id: QueryId, now: Instant) -> bool {
		if !self.enforce_leases {
			return true;
		}
		self.inner
			.lock()
			.expect("shuffle lifecycle poisoned")
			.leases
			.get(&query_id)
			.is_some_and(|lease| lease.deadline > now)
	}

	pub fn begin_retirement(&self, query_id: QueryId, cause: QueryTerminalCause) {
		self.begin_retirement_with_participants(query_id, cause, 1);
	}

	pub fn begin_retirement_with_participants(
		&self,
		query_id: QueryId,
		cause: QueryTerminalCause,
		participants: usize,
	) {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		if state.receipts.contains_key(&query_id) {
			return;
		}
		state.leases.remove(&query_id);
		let receipt_licensed = state.namespaces_seen.remove(&query_id);
		state.retiring.entry(query_id).or_insert(Retirement {
			cause,
			remaining_backends: participants.max(1),
			receipt_licensed,
		});
	}

	pub fn retirement_cause(&self, query_id: QueryId) -> Option<QueryTerminalCause> {
		self.inner
			.lock()
			.expect("shuffle lifecycle poisoned")
			.retiring
			.get(&query_id)
			.map(|retirement| retirement.cause)
	}

	pub fn finish_removal(&self, query_id: QueryId, now: Instant) {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		let Some(retirement) = state.retiring.get_mut(&query_id) else {
			return;
		};
		retirement.remaining_backends = retirement.remaining_backends.saturating_sub(1);
		if retirement.remaining_backends != 0 {
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

	pub fn expired_queries(&self, now: Instant) -> Vec<QueryId> {
		let mut state = self.inner.lock().expect("shuffle lifecycle poisoned");
		state.trim_receipts(now, self.config);
		let expired = state
			.leases
			.iter()
			.filter_map(|(query_id, lease)| (lease.deadline <= now).then_some(*query_id))
			.collect::<Vec<_>>();
		for query_id in &expired {
			state.leases.remove(query_id);
		}
		expired
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
			lifecycle.finish_removal(query_id, now);
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
		assert_eq!(lifecycle.expired_queries(expired_at), vec![query_id]);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Abandoned);
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

		lifecycle.finish_removal(query_id, now + Duration::from_millis(2));
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
		lifecycle.finish_removal(query_id, now);

		assert_eq!(lifecycle.snapshot(now).receipts, 0);
		assert_eq!(
			lifecycle.missing_reason(query_id, now),
			PartitionMissingReason::NamespaceUnknown
		);
	}

	// ---- ADR 628 Stage A: current-behaviour pins (diagnostic assertions, not invariants) ----
	// The test config's lease term is 16 s. Every instant is explicit, so each schedule is exact.

	/// Deadline expiry alone is not terminal: a renewal that arrives after the deadline but before
	/// retirement begins extends the lease again.
	#[test]
	fn adr628_renewal_of_an_expired_lease_before_retirement_extends_it() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(2, Duration::from_secs(10)));
		let t0 = Instant::now();
		let query_id = QueryId::from_u128(20);
		lifecycle.issue_or_renew(query_id, t0);
		let late = t0 + Duration::from_secs(17);
		assert!(!lifecycle.has_live_lease(query_id, late));
		lifecycle.issue_or_renew(query_id, late);
		assert_eq!(lifecycle.authorize_write(query_id, late), Ok(()));
	}

	/// Race candidate (a): an expired write removes the lease, so the reaper no longer finds the
	/// query. No retirement starts, no receipt is written, and a later lease reopens writes.
	#[test]
	fn adr628_expired_write_then_reaper_starts_no_retirement() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(2, Duration::from_secs(10)));
		let t0 = Instant::now();
		let query_id = QueryId::from_u128(21);
		lifecycle.issue_or_renew(query_id, t0);
		lifecycle.mark_namespace_present(query_id);
		let expired = t0 + Duration::from_secs(17);
		assert_eq!(
			lifecycle.authorize_write(query_id, expired),
			Err(WriteLeaseError::Expired)
		);
		assert_eq!(lifecycle.expired_queries(expired), Vec::<QueryId>::new());
		assert_eq!(lifecycle.retirement_cause(query_id), None);
		let snapshot = lifecycle.snapshot(expired);
		assert_eq!((snapshot.active_leases, snapshot.receipts), (0, 0));
		lifecycle.issue_or_renew(query_id, expired + Duration::from_millis(1));
		assert_eq!(
			lifecycle.authorize_write(query_id, expired + Duration::from_millis(1)),
			Ok(()),
			"current behaviour: the namespace's query id can be leased again"
		);
	}

	/// Race candidate (b), lifecycle interleaving only: the reaper enumerates and removes an expired
	/// lease, a renewal is applied before retirement begins (writes are authorized), then retirement
	/// reclaims the namespace as abandoned. An applied renewal alone does not prevent an already
	/// selected abandonment reclamation (apply-before-retire). The server's ACK is not exercised here.
	#[test]
	fn adr628_renewal_between_reaper_enumeration_and_retirement_is_applied_then_reclaimed() {
		let lifecycle = ShuffleNamespaceLifecycle::managed(config(2, Duration::from_secs(10)));
		let t0 = Instant::now();
		let query_id = QueryId::from_u128(22);
		lifecycle.issue_or_renew(query_id, t0);
		lifecycle.mark_namespace_present(query_id);
		let reap = t0 + Duration::from_secs(17);
		assert_eq!(lifecycle.expired_queries(reap), vec![query_id]);
		lifecycle.issue_or_renew(query_id, reap);
		assert_eq!(
			lifecycle.authorize_write(query_id, reap),
			Ok(()),
			"the renewal is applied: retirement has not begun"
		);
		lifecycle.begin_retirement(query_id, QueryTerminalCause::Abandoned);
		assert_eq!(
			lifecycle.authorize_write(query_id, reap),
			Err(WriteLeaseError::Missing)
		);
		lifecycle.finish_removal(query_id, reap);
		assert_eq!(
			lifecycle.missing_reason(query_id, reap),
			PartitionMissingReason::Abandoned
		);
	}

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
			lifecycle.finish_removal(query_id, now);
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
		lifecycle.finish_removal(query_id, t0);
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
		lifecycle.finish_removal(query_id, now);
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
		lifecycle.finish_removal(query_id, now);

		assert_eq!(lifecycle.snapshot(now).receipts, 1);
		assert_eq!(
			lifecycle.missing_reason(query_id, now),
			PartitionMissingReason::Reclaimed
		);
	}
}
