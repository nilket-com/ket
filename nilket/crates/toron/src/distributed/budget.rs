use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::distributed::identity::FrameByteLen;
use crate::storage::TenantId;

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct ByteCount(pub u64);

impl ByteCount {
	pub const ZERO: Self = Self(0);

	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub fn from_usize(value: usize) -> Self {
		Self(u64::try_from(value).expect("usize byte count fits u64"))
	}

	pub const fn get(self) -> u64 {
		self.0
	}

	pub const fn saturating_sub(self, other: Self) -> Self {
		Self(self.0.saturating_sub(other.0))
	}
}

#[derive(
	Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct DurationMillis(pub u32);

impl DurationMillis {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BudgetDimension {
	Memory,
	ShuffleBytes,
	SpillBytes,
	BroadcastReplication,
	BytesInFlightPerQuery,
	BytesInFlightPerWorkerStream,
	FrameSize,
}

impl BudgetDimension {
	pub const fn retryable(self) -> bool {
		matches!(self, Self::Memory | Self::ShuffleBytes)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBudget {
	pub bytes_in_flight_per_query: ByteCount,
	pub bytes_in_flight_per_worker_stream: ByteCount,
	pub memory_per_task: ByteCount,
	pub shuffle_bytes_published_per_task: ByteCount,
	pub spill_bytes_reserved_per_query: ByteCount,
	pub broadcast_build_bytes_per_task: ByteCount,
	pub broadcast_replicated_bytes_per_query: ByteCount,
	pub max_arrow_ipc_frame: FrameByteLen,
	pub cancellation_grace: DurationMillis,
}

impl QueryBudget {
	pub const fn test_default() -> Self {
		Self {
			bytes_in_flight_per_query: ByteCount::new(64 * 1024 * 1024),
			bytes_in_flight_per_worker_stream: ByteCount::new(16 * 1024 * 1024),
			memory_per_task: ByteCount::new(64 * 1024 * 1024),
			shuffle_bytes_published_per_task: ByteCount::new(64 * 1024 * 1024),
			spill_bytes_reserved_per_query: ByteCount::new(512 * 1024 * 1024),
			broadcast_build_bytes_per_task: ByteCount::new(64 * 1024 * 1024),
			broadcast_replicated_bytes_per_query: ByteCount::new(1024 * 1024 * 1024),
			max_arrow_ipc_frame: FrameByteLen::from_u32(16 * 1024 * 1024),
			cancellation_grace: DurationMillis::new(1_000),
		}
	}
}

impl Default for QueryBudget {
	fn default() -> Self {
		Self::test_default()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantBudget {
	pub memory_per_task: ByteCount,
	pub aggregate_memory: ByteCount,
}

impl TenantBudget {
	pub const fn new(memory_per_task: ByteCount, aggregate_memory: ByteCount) -> Self {
		Self {
			memory_per_task,
			aggregate_memory,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantBudgetPolicy {
	pub default: TenantBudget,
	pub per_tenant: HashMap<TenantId, TenantBudget>,
}

impl TenantBudgetPolicy {
	pub fn omitted_config(worker_memory_budget: ByteCount) -> Self {
		let base = QueryBudget::default();
		let aggregate_memory = worker_memory_budget.max(base.memory_per_task);
		Self {
			default: TenantBudget::new(base.memory_per_task, aggregate_memory),
			per_tenant: HashMap::new(),
		}
	}

	/// Builds the conservative bare-coordinator fallback policy.
	///
	/// This is intentionally single-query-per-tenant because callers without
	/// node-local `[tenant_budgets]` config have no independent aggregate cap.
	/// Daemon construction must override this with the normalized config policy.
	pub fn from_query_budget(query_budget: QueryBudget) -> Self {
		Self {
			default: TenantBudget::new(query_budget.memory_per_task, query_budget.memory_per_task),
			per_tenant: HashMap::new(),
		}
	}

	pub fn new(default: TenantBudget, per_tenant: HashMap<TenantId, TenantBudget>) -> Self {
		Self {
			default,
			per_tenant,
		}
	}

	pub fn budget_for(&self, tenant_id: TenantId) -> TenantBudget {
		self.per_tenant
			.get(&tenant_id)
			.copied()
			.unwrap_or(self.default)
	}

	pub fn query_budget_for(&self, tenant_id: TenantId) -> QueryBudget {
		self.query_budget_for_base(tenant_id, QueryBudget::default())
	}

	pub fn query_budget_for_base(&self, tenant_id: TenantId, mut base: QueryBudget) -> QueryBudget {
		base.memory_per_task = self.budget_for(tenant_id).memory_per_task;
		base
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskBudget {
	pub memory: ByteCount,
	pub shuffle_bytes: ByteCount,
	pub broadcast_build_bytes: ByteCount,
	pub broadcast_replicated_bytes: ByteCount,
	pub spill_bytes: ByteCount,
	pub max_arrow_ipc_frame: FrameByteLen,
}

impl TaskBudget {
	pub const fn from_query_budget(query: QueryBudget) -> Self {
		Self {
			memory: query.memory_per_task,
			shuffle_bytes: query.shuffle_bytes_published_per_task,
			broadcast_build_bytes: query.broadcast_build_bytes_per_task,
			broadcast_replicated_bytes: query.broadcast_replicated_bytes_per_query,
			spill_bytes: query.spill_bytes_reserved_per_query,
			max_arrow_ipc_frame: query.max_arrow_ipc_frame,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetRejection {
	pub dimension: BudgetDimension,
	pub requested: ByteCount,
	pub available: ByteCount,
}

#[derive(Debug, Clone)]
pub struct BudgetTracker {
	inner: Arc<Mutex<BudgetState>>,
}

#[derive(Debug)]
struct BudgetState {
	budget: QueryBudget,
	memory_used: ByteCount,
	shuffle_used: ByteCount,
	spill_used: ByteCount,
	broadcast_replication_used: ByteCount,
	bytes_in_flight_per_query_used: ByteCount,
	bytes_in_flight_per_worker_stream_used: ByteCount,
}

impl BudgetTracker {
	pub fn new(budget: QueryBudget) -> Self {
		Self {
			inner: Arc::new(Mutex::new(BudgetState {
				budget,
				memory_used: ByteCount::ZERO,
				shuffle_used: ByteCount::ZERO,
				spill_used: ByteCount::ZERO,
				broadcast_replication_used: ByteCount::ZERO,
				bytes_in_flight_per_query_used: ByteCount::ZERO,
				bytes_in_flight_per_worker_stream_used: ByteCount::ZERO,
			})),
		}
	}

	pub fn reserve(
		&self,
		dimension: BudgetDimension,
		amount: ByteCount,
	) -> Result<BudgetReservation, BudgetRejection> {
		let mut state = self.inner.lock().expect("budget tracker poisoned");
		let used = state.used_for(dimension);
		let limit = state.limit_for(dimension);
		let available = limit.saturating_sub(used);
		if amount > available {
			return Err(BudgetRejection {
				dimension,
				requested: amount,
				available,
			});
		}

		state.add_used(dimension, amount);
		Ok(BudgetReservation {
			tracker: self.clone(),
			dimension,
			amount,
			released: false,
		})
	}

	pub fn used(&self, dimension: BudgetDimension) -> ByteCount {
		self.inner
			.lock()
			.expect("budget tracker poisoned")
			.used_for(dimension)
	}

	fn release(&self, dimension: BudgetDimension, amount: ByteCount) {
		self.inner
			.lock()
			.expect("budget tracker poisoned")
			.sub_used(dimension, amount);
	}
}

#[derive(Debug)]
pub struct BudgetReservation {
	tracker: BudgetTracker,
	dimension: BudgetDimension,
	amount: ByteCount,
	released: bool,
}

impl BudgetReservation {
	pub fn release(mut self) {
		self.release_once();
	}

	fn release_once(&mut self) {
		if !self.released {
			self.tracker.release(self.dimension, self.amount);
			self.released = true;
		}
	}
}

impl Drop for BudgetReservation {
	fn drop(&mut self) {
		self.release_once();
	}
}

#[derive(Debug, Clone)]
pub struct TenantMemoryAccountant {
	policy: TenantBudgetPolicy,
	used: Arc<Mutex<HashMap<TenantId, ByteCount>>>,
}

impl TenantMemoryAccountant {
	pub fn new(policy: TenantBudgetPolicy) -> Self {
		Self {
			policy,
			used: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn try_reserve(
		&self,
		tenant_id: TenantId,
		amount: ByteCount,
	) -> Result<TenantMemoryReservation, BudgetRejection> {
		let mut used = self.used.lock().expect("tenant memory accountant poisoned");
		let current = used.get(&tenant_id).copied().unwrap_or(ByteCount::ZERO);
		let limit = self.policy.budget_for(tenant_id).aggregate_memory;
		let available = limit.saturating_sub(current);
		if amount > available {
			return Err(BudgetRejection {
				dimension: BudgetDimension::Memory,
				requested: amount,
				available,
			});
		}
		used.insert(tenant_id, ByteCount::new(current.get() + amount.get()));
		Ok(TenantMemoryReservation {
			used: Arc::clone(&self.used),
			tenant_id,
			amount,
			released: false,
		})
	}

	pub fn used(&self, tenant_id: TenantId) -> ByteCount {
		self.used
			.lock()
			.expect("tenant memory accountant poisoned")
			.get(&tenant_id)
			.copied()
			.unwrap_or(ByteCount::ZERO)
	}
}

#[derive(Debug)]
pub struct TenantMemoryReservation {
	used: Arc<Mutex<HashMap<TenantId, ByteCount>>>,
	tenant_id: TenantId,
	amount: ByteCount,
	released: bool,
}

impl TenantMemoryReservation {
	pub fn release(mut self) {
		self.release_once();
	}

	fn release_once(&mut self) {
		if self.released {
			return;
		}
		let mut used = self.used.lock().expect("tenant memory accountant poisoned");
		let current = used
			.get(&self.tenant_id)
			.copied()
			.unwrap_or(ByteCount::ZERO);
		let updated = current.saturating_sub(self.amount);
		if updated == ByteCount::ZERO {
			used.remove(&self.tenant_id);
		} else {
			used.insert(self.tenant_id, updated);
		}
		self.released = true;
	}
}

impl Drop for TenantMemoryReservation {
	fn drop(&mut self) {
		self.release_once();
	}
}

impl BudgetState {
	fn limit_for(&self, dimension: BudgetDimension) -> ByteCount {
		match dimension {
			BudgetDimension::Memory => self.budget.memory_per_task,
			BudgetDimension::ShuffleBytes => self.budget.shuffle_bytes_published_per_task,
			BudgetDimension::SpillBytes => self.budget.spill_bytes_reserved_per_query,
			BudgetDimension::BroadcastReplication => {
				self.budget.broadcast_replicated_bytes_per_query
			}
			BudgetDimension::BytesInFlightPerQuery => self.budget.bytes_in_flight_per_query,
			BudgetDimension::BytesInFlightPerWorkerStream => {
				self.budget.bytes_in_flight_per_worker_stream
			}
			BudgetDimension::FrameSize => {
				ByteCount::new(u64::from(self.budget.max_arrow_ipc_frame.get()))
			}
		}
	}

	fn used_for(&self, dimension: BudgetDimension) -> ByteCount {
		match dimension {
			BudgetDimension::Memory => self.memory_used,
			BudgetDimension::ShuffleBytes => self.shuffle_used,
			BudgetDimension::SpillBytes => self.spill_used,
			BudgetDimension::BroadcastReplication => self.broadcast_replication_used,
			BudgetDimension::BytesInFlightPerQuery => self.bytes_in_flight_per_query_used,
			BudgetDimension::BytesInFlightPerWorkerStream => {
				self.bytes_in_flight_per_worker_stream_used
			}
			BudgetDimension::FrameSize => ByteCount::ZERO,
		}
	}

	fn add_used(&mut self, dimension: BudgetDimension, amount: ByteCount) {
		if dimension == BudgetDimension::FrameSize {
			return;
		}
		let target = self.used_mut(dimension);
		*target = ByteCount::new(target.get() + amount.get());
	}

	fn sub_used(&mut self, dimension: BudgetDimension, amount: ByteCount) {
		if dimension == BudgetDimension::FrameSize {
			return;
		}
		let target = self.used_mut(dimension);
		*target = target.saturating_sub(amount);
	}

	fn used_mut(&mut self, dimension: BudgetDimension) -> &mut ByteCount {
		match dimension {
			BudgetDimension::Memory => &mut self.memory_used,
			BudgetDimension::ShuffleBytes => &mut self.shuffle_used,
			BudgetDimension::SpillBytes => &mut self.spill_used,
			BudgetDimension::BroadcastReplication => &mut self.broadcast_replication_used,
			BudgetDimension::BytesInFlightPerQuery => &mut self.bytes_in_flight_per_query_used,
			BudgetDimension::BytesInFlightPerWorkerStream => {
				&mut self.bytes_in_flight_per_worker_stream_used
			}
			BudgetDimension::FrameSize => unreachable!("FrameSize is not a retained reservation"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reservation_releases_on_drop() {
		let tracker = BudgetTracker::new(QueryBudget {
			memory_per_task: ByteCount::new(10),
			..QueryBudget::test_default()
		});

		let reservation = tracker
			.reserve(BudgetDimension::Memory, ByteCount::new(7))
			.expect("reserve");
		assert_eq!(tracker.used(BudgetDimension::Memory), ByteCount::new(7));

		drop(reservation);

		assert_eq!(tracker.used(BudgetDimension::Memory), ByteCount::ZERO);
	}

	#[test]
	fn reservation_rejects_over_limit() {
		let tracker = BudgetTracker::new(QueryBudget {
			shuffle_bytes_published_per_task: ByteCount::new(5),
			..QueryBudget::test_default()
		});

		let rejection = tracker
			.reserve(BudgetDimension::ShuffleBytes, ByteCount::new(6))
			.expect_err("over limit");

		assert_eq!(
			rejection,
			BudgetRejection {
				dimension: BudgetDimension::ShuffleBytes,
				requested: ByteCount::new(6),
				available: ByteCount::new(5),
			}
		);
	}

	#[test]
	fn tenant_budget_policy_preserves_non_memory_budget_dimensions() {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(
			TenantId(7),
			TenantBudget::new(ByteCount::new(8), ByteCount::new(32)),
		);
		let policy = TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(16), ByteCount::new(64)),
			per_tenant,
		);
		let mut base = QueryBudget::test_default();
		base.shuffle_bytes_published_per_task = ByteCount::new(99);

		let budget = policy.query_budget_for_base(TenantId(7), base);

		assert_eq!(budget.memory_per_task, ByteCount::new(8));
		assert_eq!(budget.shuffle_bytes_published_per_task, ByteCount::new(99));
	}

	#[test]
	fn tenant_memory_accountant_releases_on_drop() {
		let policy = TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(8), ByteCount::new(10)),
			HashMap::new(),
		);
		let accountant = TenantMemoryAccountant::new(policy);

		let reservation = accountant
			.try_reserve(TenantId(7), ByteCount::new(7))
			.expect("reserve");
		assert_eq!(accountant.used(TenantId(7)), ByteCount::new(7));

		drop(reservation);

		assert_eq!(accountant.used(TenantId(7)), ByteCount::ZERO);
	}

	#[test]
	fn tenant_memory_accountant_isolated_by_tenant() {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(
			TenantId(7),
			TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
		);
		per_tenant.insert(
			TenantId(8),
			TenantBudget::new(ByteCount::new(8), ByteCount::new(16)),
		);
		let policy = TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
			per_tenant,
		);
		let accountant = TenantMemoryAccountant::new(policy);

		let _tenant_7 = accountant
			.try_reserve(TenantId(7), ByteCount::new(8))
			.expect("tenant 7 reserve");
		assert!(matches!(
			accountant.try_reserve(TenantId(7), ByteCount::new(1)),
			Err(BudgetRejection {
				dimension: BudgetDimension::Memory,
				requested: ByteCount(1),
				available: ByteCount(0),
			})
		));

		let _tenant_8 = accountant
			.try_reserve(TenantId(8), ByteCount::new(8))
			.expect("tenant 8 still has budget");
		assert_eq!(accountant.used(TenantId(8)), ByteCount::new(8));
	}
}
