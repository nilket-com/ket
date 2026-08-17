use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::distributed::budget::{ByteCount, DurationMillis};
use crate::distributed::identity::{LeaseEpoch, WorkerId};

/// Single worker-loss window used to derive query-namespace lease safety.
/// The registry expiry API accepts the same concept explicitly; keeping the
/// production default here prevents the shuffle store from inventing a second clock.
pub const DEFAULT_WORKER_LOST_AFTER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerEndpoint(pub String);

impl WorkerEndpoint {
	pub fn new(value: impl Into<String>) -> Self {
		Self(value.into())
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}

	pub fn into_inner(self) -> String {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct SimdLaneCount(pub u16);

impl SimdLaneCount {
	pub const fn new(value: u16) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u16 {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct ActiveTaskCount(pub u32);

impl ActiveTaskCount {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct UnixMillis(pub u64);

impl UnixMillis {
	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapabilities {
	pub advertised_memory: ByteCount,
	pub simd_width: SimdLaneCount,
	pub spill_capacity_reserved: ByteCount,
}

/// An owned, exactly-once claim on one worker's task slot (ADR 555 §3).
///
/// Acquired only through `select_and_reserve` or `reserve`, both of which
/// increment `active_tasks` under the registry write lock so the reservation is
/// visible to the *next* selector. Released by drop, so normal completion,
/// failure, cancellation, and early `?` exits are all covered by ownership
/// rather than by four manually synchronised decrements.
///
/// Deliberately neither `Clone` nor `Copy`: a double release would underflow the
/// counter, after which a worker looks permanently idle and attracts every task
/// — a balancer worse than the inert one it replaces, and silent. The type makes
/// that unrepresentable rather than merely tested for.
#[must_use = "a worker reservation must be held until its attempt reaches a terminal outcome"]
pub struct WorkerReservation {
	worker_id: WorkerId,
	release: Option<Box<dyn FnOnce(WorkerId) + Send + Sync>>,
}

impl WorkerReservation {
	fn new(worker_id: WorkerId, release: impl FnOnce(WorkerId) + Send + Sync + 'static) -> Self {
		Self {
			worker_id,
			release: Some(Box::new(release)),
		}
	}

	/// A token that tracks nothing, for registries whose placement does not
	/// depend on observed load. Public so an implementor can select it
	/// **explicitly**, with a reason, rather than inheriting it from a default.
	pub fn unreserved(worker_id: WorkerId) -> Self {
		Self {
			worker_id,
			release: None,
		}
	}

	pub fn worker_id(&self) -> WorkerId {
		self.worker_id
	}
}

impl std::fmt::Debug for WorkerReservation {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("WorkerReservation")
			.field("worker_id", &self.worker_id)
			.finish_non_exhaustive()
	}
}

impl Drop for WorkerReservation {
	fn drop(&mut self) {
		if let Some(release) = self.release.take() {
			release(self.worker_id);
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WorkerResourceState {
	pub in_flight: ByteCount,
	pub active_tasks: ActiveTaskCount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLease {
	pub last_heartbeat: UnixMillis,
	pub epoch: LeaseEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerStatus {
	Live,
	Suspect,
	Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerEntry {
	pub worker_id: WorkerId,
	pub endpoint: WorkerEndpoint,
	pub lease: WorkerLease,
	pub capabilities: WorkerCapabilities,
	pub resources: WorkerResourceState,
	pub status: WorkerStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlacementHints;

pub trait WorkerRegistry: Send + Sync {
	fn list(&self) -> Vec<WorkerEntry>;
	fn resolve(&self, worker_id: WorkerId) -> Option<WorkerEntry>;
	fn select_for_attempt(&self, hints: &PlacementHints) -> Option<WorkerId>;
	/// Select the least-loaded live worker **and reserve it under one write
	/// lock**, so the reservation is visible to the next selector (ADR 555 §3).
	/// Reserving after dispatch would leave a window in which two selectors
	/// observe identical load and recreate the tie.
	///
	/// **Deliberately has no default.** An implementor must choose either an
	/// observed-load reservation or [`WorkerReservation::unreserved`] with its own
	/// stated reason. A default would let a future load-sensitive registry
	/// silently inherit the inert behaviour ADR 555 exists to remove, and the
	/// compile error is what forces the decision.
	fn select_and_reserve(&self, hints: &PlacementHints) -> Option<WorkerReservation>;

	/// Reserve a specific worker chosen by shuffle affinity. Returns `None` when
	/// that worker is no longer live, so the caller falls back to selection.
	///
	/// No default, on the same terms as above.
	fn reserve(&self, worker_id: WorkerId) -> Option<WorkerReservation>;
	fn mark_lost(&self, worker_id: WorkerId);
	fn heartbeat(&self, worker_id: WorkerId, lease: WorkerLease);
	fn expire_heartbeats(
		&self,
		now: UnixMillis,
		suspect_after: DurationMillis,
		lost_after: DurationMillis,
	) -> Vec<WorkerId>;
}

impl<Registry> WorkerRegistry for Arc<Registry>
where
	Registry: WorkerRegistry + ?Sized,
{
	fn list(&self) -> Vec<WorkerEntry> {
		self.as_ref().list()
	}

	fn resolve(&self, worker_id: WorkerId) -> Option<WorkerEntry> {
		self.as_ref().resolve(worker_id)
	}

	fn select_for_attempt(&self, hints: &PlacementHints) -> Option<WorkerId> {
		self.as_ref().select_for_attempt(hints)
	}

	fn select_and_reserve(&self, hints: &PlacementHints) -> Option<WorkerReservation> {
		self.as_ref().select_and_reserve(hints)
	}

	fn reserve(&self, worker_id: WorkerId) -> Option<WorkerReservation> {
		self.as_ref().reserve(worker_id)
	}

	fn mark_lost(&self, worker_id: WorkerId) {
		self.as_ref().mark_lost(worker_id);
	}

	fn heartbeat(&self, worker_id: WorkerId, lease: WorkerLease) {
		self.as_ref().heartbeat(worker_id, lease);
	}

	fn expire_heartbeats(
		&self,
		now: UnixMillis,
		suspect_after: DurationMillis,
		lost_after: DurationMillis,
	) -> Vec<WorkerId> {
		self.as_ref()
			.expire_heartbeats(now, suspect_after, lost_after)
	}
}

/// How a registry places tasks that shuffle affinity declined (ADR 555 §7).
///
/// Chosen at **construction**, never from the environment: this is a harness
/// instrument for the paired locality-versus-parallelism comparison, not a
/// production runtime knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FallbackPlacementPolicy {
	/// Least-loaded by `(active_tasks, worker_id)` — the landed behaviour.
	#[default]
	LoadObserving,
	/// Send every fallback-eligible task to one stable worker, chosen by the same
	/// deterministic `worker_id` tie-break.
	///
	/// A **controlled serialization baseline**, not a revival of the old
	/// nondeterminism: the pre-ADR-555 engine resolved ties by `HashMap`
	/// iteration order, which varied per construction. This arm is reproducible,
	/// so a paired comparison measures serialization rather than hash noise.
	AffinityOnly,
}

#[derive(Debug)]
pub struct StaticWorkerRegistry {
	workers: Arc<RwLock<HashMap<WorkerId, WorkerEntry>>>,
	fallback: FallbackPlacementPolicy,
}

impl StaticWorkerRegistry {
	/// ADR 555 §2: `(active_tasks, worker_id)`. Byte `in_flight` is deliberately
	/// **not** consulted — it has no producer, and a permanently-zero term would
	/// claim byte load is considered when it is not. `worker_id` is the final,
	/// deterministic tie-break, replacing `HashMap` iteration order.
	fn placement_key(worker: &&WorkerEntry) -> (ActiveTaskCount, WorkerId) {
		(worker.resources.active_tasks, worker.worker_id)
	}

	fn acquire_locked(
		workers: &mut HashMap<WorkerId, WorkerEntry>,
		worker_id: WorkerId,
	) -> Option<()> {
		let worker = workers.get_mut(&worker_id)?;
		if worker.status != WorkerStatus::Live {
			return None;
		}
		worker.resources.active_tasks =
			ActiveTaskCount::new(worker.resources.active_tasks.get().saturating_add(1));
		Some(())
	}

	fn reservation_for(&self, worker_id: WorkerId) -> WorkerReservation {
		let workers = Arc::clone(&self.workers);
		WorkerReservation::new(worker_id, move |worker_id| {
			let Ok(mut workers) = workers.write() else {
				return;
			};
			let Some(worker) = workers.get_mut(&worker_id) else {
				return;
			};
			// Never saturate: a zero count here is an ownership bug, not a
			// tolerable edge, and hiding it would leave the balancer quietly wrong.
			match worker.resources.active_tasks.get().checked_sub(1) {
				Some(next) => worker.resources.active_tasks = ActiveTaskCount::new(next),
				None => {
					debug_assert!(false, "worker reservation released more than once");
					tracing::error!(
						worker_id = ?worker_id,
						"worker reservation released below zero; load balancing is unreliable"
					);
				}
			}
		})
	}

	/// Total outstanding reservations across every worker.
	///
	/// ADR 555 §7 seals that an arm finishes with **zero** live reservations
	/// before its paired peer begins: a leaked reservation would carry phantom
	/// load into the next arm and bias its placement, which is exactly the
	/// comparison the pairing exists to protect.
	pub fn live_reservations(&self) -> u32 {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.values()
			.map(|worker| worker.resources.active_tasks.get())
			.sum()
	}

	/// Select the fallback policy explicitly. Construction-time only, so a
	/// production coordinator cannot acquire the baseline arm by configuration.
	pub fn with_fallback_placement_policy(mut self, fallback: FallbackPlacementPolicy) -> Self {
		self.fallback = fallback;
		self
	}

	pub fn new(workers: impl IntoIterator<Item = WorkerEntry>) -> Self {
		Self {
			fallback: FallbackPlacementPolicy::default(),
			workers: Arc::new(RwLock::new(
				workers
					.into_iter()
					.map(|worker| (worker.worker_id, worker))
					.collect(),
			)),
		}
	}
}

impl WorkerRegistry for StaticWorkerRegistry {
	fn list(&self) -> Vec<WorkerEntry> {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.values()
			.cloned()
			.collect()
	}

	fn resolve(&self, worker_id: WorkerId) -> Option<WorkerEntry> {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.get(&worker_id)
			.cloned()
	}

	fn select_for_attempt(&self, _hints: &PlacementHints) -> Option<WorkerId> {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.values()
			.filter(|worker| worker.status == WorkerStatus::Live)
			.min_by_key(Self::placement_key)
			.map(|worker| worker.worker_id)
	}

	fn select_and_reserve(&self, _hints: &PlacementHints) -> Option<WorkerReservation> {
		// Selection and reservation share one write lock: the increment must be
		// visible to the next selector, or two selectors observe identical load
		// and recreate the tie this record exists to remove (ADR 555 §3).
		let mut workers = self.workers.write().expect("worker registry poisoned");
		let live = workers
			.values()
			.filter(|worker| worker.status == WorkerStatus::Live);
		// Both arms reserve, so the token lifecycle is identical and the only
		// difference between them is the ordering key.
		let worker_id = match self.fallback {
			FallbackPlacementPolicy::LoadObserving => live.min_by_key(Self::placement_key),
			FallbackPlacementPolicy::AffinityOnly => live.min_by_key(|worker| worker.worker_id),
		}
		.map(|worker| worker.worker_id)?;
		Self::acquire_locked(&mut workers, worker_id)?;
		drop(workers);
		Some(self.reservation_for(worker_id))
	}

	fn reserve(&self, worker_id: WorkerId) -> Option<WorkerReservation> {
		let mut workers = self.workers.write().expect("worker registry poisoned");
		Self::acquire_locked(&mut workers, worker_id)?;
		drop(workers);
		Some(self.reservation_for(worker_id))
	}

	fn mark_lost(&self, worker_id: WorkerId) {
		if let Some(worker) = self
			.workers
			.write()
			.expect("worker registry poisoned")
			.get_mut(&worker_id)
		{
			worker.status = WorkerStatus::Lost;
			worker.lease.epoch = worker.lease.epoch.next();
		}
	}

	fn heartbeat(&self, worker_id: WorkerId, lease: WorkerLease) {
		if let Some(worker) = self
			.workers
			.write()
			.expect("worker registry poisoned")
			.get_mut(&worker_id)
		{
			worker.lease = lease;
			if worker.status != WorkerStatus::Lost {
				worker.status = WorkerStatus::Live;
			}
		}
	}

	fn expire_heartbeats(
		&self,
		now: UnixMillis,
		suspect_after: DurationMillis,
		lost_after: DurationMillis,
	) -> Vec<WorkerId> {
		let mut lost_workers = Vec::new();
		for worker in self
			.workers
			.write()
			.expect("worker registry poisoned")
			.values_mut()
		{
			if worker.status == WorkerStatus::Lost {
				continue;
			}

			let age = now.get().saturating_sub(worker.lease.last_heartbeat.get());
			if age >= u64::from(lost_after.get()) {
				worker.status = WorkerStatus::Lost;
				worker.lease.epoch = worker.lease.epoch.next();
				lost_workers.push(worker.worker_id);
			} else if age >= u64::from(suspect_after.get()) {
				worker.status = WorkerStatus::Suspect;
			}
		}

		lost_workers
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn worker(
		worker_id: WorkerId,
		active_tasks: ActiveTaskCount,
		in_flight: ByteCount,
		status: WorkerStatus,
	) -> WorkerEntry {
		WorkerEntry {
			worker_id,
			endpoint: WorkerEndpoint::new("127.0.0.1:9000"),
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(10),
				epoch: LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: ByteCount::new(1024),
				simd_width: SimdLaneCount::new(8),
				spill_capacity_reserved: ByteCount::new(4096),
			},
			resources: WorkerResourceState {
				in_flight,
				active_tasks,
			},
			status,
		}
	}

	fn two_equal_live_workers() -> StaticWorkerRegistry {
		StaticWorkerRegistry::new([
			worker(
				WorkerId::from_u128(2),
				ActiveTaskCount::new(0),
				ByteCount::ZERO,
				WorkerStatus::Live,
			),
			worker(
				WorkerId::from_u128(1),
				ActiveTaskCount::new(0),
				ByteCount::ZERO,
				WorkerStatus::Live,
			),
		])
	}

	fn two_equal_live_workers_with(policy: FallbackPlacementPolicy) -> StaticWorkerRegistry {
		two_equal_live_workers().with_fallback_placement_policy(policy)
	}

	/// ADR 555 §7 baseline arm: fallback-eligible tasks all land on one worker,
	/// so the arm serializes them exactly as pre-555 placement did.
	#[test]
	fn adr555_affinity_only_arm_sends_every_fallback_task_to_one_worker() {
		let registry = two_equal_live_workers_with(FallbackPlacementPolicy::AffinityOnly);
		let picked: Vec<_> = (0..8)
			.map(|_| {
				registry
					.select_and_reserve(&PlacementHints)
					.expect("selection")
			})
			.collect();
		let distinct: std::collections::BTreeSet<_> = picked
			.iter()
			.map(|reservation| reservation.worker_id())
			.collect();
		assert_eq!(
			distinct.len(),
			1,
			"the baseline arm must serialize onto one worker even as load accumulates"
		);
	}

	/// And it is a **controlled** baseline: deterministic across constructions,
	/// not the `HashMap` iteration order the pre-555 engine actually used. A
	/// paired comparison against hash noise would measure the noise.
	#[test]
	fn adr555_affinity_only_arm_is_deterministic_across_constructions() {
		let picked: Vec<u128> = (0..16)
			.map(|_| {
				two_equal_live_workers_with(FallbackPlacementPolicy::AffinityOnly)
					.select_and_reserve(&PlacementHints)
					.expect("selection")
					.worker_id()
					.get()
					.as_u128()
			})
			.collect();
		assert!(
			picked.windows(2).all(|pair| pair[0] == pair[1]),
			"the baseline arm must be reproducible, not hash-ordered: {picked:?}"
		);
	}

	/// The two arms must actually differ, or the comparison measures nothing.
	#[test]
	fn adr555_the_two_arms_place_differently_under_load() {
		let baseline = two_equal_live_workers_with(FallbackPlacementPolicy::AffinityOnly);
		let first = baseline.select_and_reserve(&PlacementHints).expect("first");
		let second = baseline
			.select_and_reserve(&PlacementHints)
			.expect("second");
		assert_eq!(first.worker_id(), second.worker_id(), "baseline serializes");

		let observing = two_equal_live_workers_with(FallbackPlacementPolicy::LoadObserving);
		let first = observing
			.select_and_reserve(&PlacementHints)
			.expect("first");
		let second = observing
			.select_and_reserve(&PlacementHints)
			.expect("second");
		assert_ne!(
			first.worker_id(),
			second.worker_id(),
			"load-observing spreads"
		);
	}

	/// ADR 555 §3: the reservation must be visible to the **next** selector.
	/// Reserving after dispatch would leave both selections seeing zero load.
	#[test]
	fn adr555_selection_and_reservation_are_atomic_for_the_next_selector() {
		let registry = two_equal_live_workers();
		let first = registry
			.select_and_reserve(&PlacementHints)
			.expect("first selection");
		let second = registry
			.select_and_reserve(&PlacementHints)
			.expect("second selection");
		assert_ne!(
			first.worker_id(),
			second.worker_id(),
			"the second selector must observe the first reservation"
		);
	}

	/// Releasing returns the slot, so a third selection reuses the freed worker.
	#[test]
	fn adr555_dropping_a_reservation_returns_the_slot() {
		let registry = two_equal_live_workers();
		let first = registry.select_and_reserve(&PlacementHints).expect("first");
		let freed = first.worker_id();
		drop(first);

		let again = registry.select_and_reserve(&PlacementHints).expect("again");
		assert_eq!(
			again.worker_id(),
			freed,
			"a released slot is available to the next selection"
		);
	}

	/// ADR 555 §2: byte `in_flight` is not consulted. Two workers differing only
	/// in that field must select identically, or the key is claiming to consider
	/// byte load that nothing produces.
	#[test]
	fn adr555_byte_in_flight_does_not_influence_selection() {
		let loaded_bytes = StaticWorkerRegistry::new([
			worker(
				WorkerId::from_u128(1),
				ActiveTaskCount::new(0),
				ByteCount::new(1 << 30),
				WorkerStatus::Live,
			),
			worker(
				WorkerId::from_u128(2),
				ActiveTaskCount::new(0),
				ByteCount::ZERO,
				WorkerStatus::Live,
			),
		]);
		let picked = loaded_bytes
			.select_and_reserve(&PlacementHints)
			.expect("selection");
		assert_eq!(
			picked.worker_id(),
			WorkerId::from_u128(1),
			"selection follows (active_tasks, worker_id); bytes are ignored"
		);
	}

	/// §6: equal load must not be resolved by map iteration order. Repeated
	/// registry constructions over the same set must select identically.
	#[test]
	fn adr555_equal_load_selection_is_stable_across_constructions() {
		let picked: Vec<u128> = (0..16)
			.map(|_| {
				two_equal_live_workers()
					.select_and_reserve(&PlacementHints)
					.expect("selection")
					.worker_id()
					.get()
					.as_u128()
			})
			.collect();
		assert!(
			picked.windows(2).all(|pair| pair[0] == pair[1]),
			"iteration order must not decide a tie: {picked:?}"
		);
	}

	#[test]
	fn static_registry_selects_least_loaded_live_worker() {
		let busy = worker(
			WorkerId::from_u128(1),
			ActiveTaskCount::new(4),
			ByteCount::new(1),
			WorkerStatus::Live,
		);
		let idle = worker(
			WorkerId::from_u128(2),
			ActiveTaskCount::new(1),
			ByteCount::new(64),
			WorkerStatus::Live,
		);
		let lost = worker(
			WorkerId::from_u128(3),
			ActiveTaskCount::new(0),
			ByteCount::ZERO,
			WorkerStatus::Lost,
		);
		let registry = StaticWorkerRegistry::new([busy, idle.clone(), lost]);

		assert_eq!(
			registry.select_for_attempt(&PlacementHints),
			Some(idle.worker_id)
		);
	}

	#[test]
	fn mark_lost_bumps_lease_epoch() {
		let worker_id = WorkerId::from_u128(1);
		let registry = StaticWorkerRegistry::new([worker(
			worker_id,
			ActiveTaskCount::default(),
			ByteCount::ZERO,
			WorkerStatus::Live,
		)]);

		registry.mark_lost(worker_id);

		let entry = registry.resolve(worker_id).expect("worker exists");
		assert_eq!(entry.status, WorkerStatus::Lost);
		assert_eq!(entry.lease.epoch, LeaseEpoch::new(2));
	}

	#[test]
	fn heartbeat_expiry_marks_suspect_then_lost_without_selecting_lost_workers() {
		let worker_id = WorkerId::from_u128(1);
		let registry = StaticWorkerRegistry::new([worker(
			worker_id,
			ActiveTaskCount::default(),
			ByteCount::ZERO,
			WorkerStatus::Live,
		)]);

		assert!(
			registry
				.expire_heartbeats(
					UnixMillis::new(50),
					DurationMillis::new(100),
					DurationMillis::new(500),
				)
				.is_empty()
		);
		assert_eq!(
			registry.resolve(worker_id).expect("worker").status,
			WorkerStatus::Live
		);

		assert!(
			registry
				.expire_heartbeats(
					UnixMillis::new(150),
					DurationMillis::new(100),
					DurationMillis::new(500),
				)
				.is_empty()
		);
		assert_eq!(
			registry.resolve(worker_id).expect("worker").status,
			WorkerStatus::Suspect
		);

		let lost = registry.expire_heartbeats(
			UnixMillis::new(600),
			DurationMillis::new(100),
			DurationMillis::new(500),
		);
		assert_eq!(lost, vec![worker_id]);
		let entry = registry.resolve(worker_id).expect("worker");
		assert_eq!(entry.status, WorkerStatus::Lost);
		assert_eq!(entry.lease.epoch, LeaseEpoch::new(2));
		assert_eq!(registry.select_for_attempt(&PlacementHints), None);
	}
}
