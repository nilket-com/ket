use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

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

	pub const fn saturating_add(self, other: Self) -> Self {
		Self(self.0.saturating_add(other.0))
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
	pub max_concurrent_queries: NonZeroU32,
	pub max_concurrent_descriptor_syncs: NonZeroU32,
	pub max_concurrent_connections: Option<NonZeroU32>,
	pub admission_weight: NonZeroU32,
	pub max_queued_queries: u32,
	pub max_borrowed_memory: ByteCount,
	pub max_borrowed_concurrent_queries: u32,
	pub max_queries_per_second: u32,
	pub query_burst_capacity: u32,
}

impl TenantBudget {
	pub fn new(memory_per_task: ByteCount, aggregate_memory: ByteCount) -> Self {
		Self::with_max_concurrent_queries(
			memory_per_task,
			aggregate_memory,
			derive_max_concurrent_queries(memory_per_task, aggregate_memory),
		)
	}

	pub const fn with_max_concurrent_queries(
		memory_per_task: ByteCount,
		aggregate_memory: ByteCount,
		max_concurrent_queries: NonZeroU32,
	) -> Self {
		Self::with_query_and_descriptor_sync_limits(
			memory_per_task,
			aggregate_memory,
			max_concurrent_queries,
			derive_max_concurrent_descriptor_syncs(max_concurrent_queries),
		)
	}

	pub const fn with_query_and_descriptor_sync_limits(
		memory_per_task: ByteCount,
		aggregate_memory: ByteCount,
		max_concurrent_queries: NonZeroU32,
		max_concurrent_descriptor_syncs: NonZeroU32,
	) -> Self {
		Self::with_admission_limits(
			memory_per_task,
			aggregate_memory,
			max_concurrent_queries,
			max_concurrent_descriptor_syncs,
			None,
			match NonZeroU32::new(1) {
				Some(value) => value,
				None => unreachable!(),
			},
			0,
			ByteCount::ZERO,
			0,
			0,
			0,
		)
	}

	#[allow(clippy::too_many_arguments)]
	pub const fn with_admission_limits(
		memory_per_task: ByteCount,
		aggregate_memory: ByteCount,
		max_concurrent_queries: NonZeroU32,
		max_concurrent_descriptor_syncs: NonZeroU32,
		max_concurrent_connections: Option<NonZeroU32>,
		admission_weight: NonZeroU32,
		max_queued_queries: u32,
		max_borrowed_memory: ByteCount,
		max_borrowed_concurrent_queries: u32,
		max_queries_per_second: u32,
		query_burst_capacity: u32,
	) -> Self {
		Self {
			memory_per_task,
			aggregate_memory,
			max_concurrent_queries,
			max_concurrent_descriptor_syncs,
			max_concurrent_connections,
			admission_weight,
			max_queued_queries,
			max_borrowed_memory,
			max_borrowed_concurrent_queries,
			max_queries_per_second,
			query_burst_capacity,
		}
	}

	pub const fn rate_enabled(self) -> bool {
		self.max_queries_per_second != 0
	}

	pub const fn effective_burst(self) -> u32 {
		if self.max_queries_per_second == 0 {
			0
		} else if self.query_burst_capacity == 0 {
			self.max_queries_per_second
		} else {
			self.query_burst_capacity
		}
	}
}

pub fn derive_max_concurrent_queries(
	memory_per_task: ByteCount,
	aggregate_memory: ByteCount,
) -> NonZeroU32 {
	let per_task = memory_per_task.get().max(1);
	let slots = (aggregate_memory.get() / per_task).max(1);
	let slots = u32::try_from(slots).unwrap_or(u32::MAX);
	NonZeroU32::new(slots).expect("derived concurrency is nonzero")
}

pub const fn derive_max_concurrent_descriptor_syncs(
	max_concurrent_queries: NonZeroU32,
) -> NonZeroU32 {
	let slots = if max_concurrent_queries.get() < 2 {
		max_concurrent_queries.get()
	} else {
		2
	};
	match NonZeroU32::new(slots) {
		Some(slots) => slots,
		None => unreachable!(),
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
			default: TenantBudget::with_query_and_descriptor_sync_limits(
				query_budget.memory_per_task,
				query_budget.memory_per_task,
				NonZeroU32::new(1).expect("one is nonzero"),
				NonZeroU32::new(1).expect("one is nonzero"),
			),
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

	pub fn is_configured_tenant(&self, tenant_id: TenantId) -> bool {
		self.per_tenant.contains_key(&tenant_id)
	}

	pub fn configured_tenant_ids(&self) -> Vec<TenantId> {
		let mut tenant_ids = self.per_tenant.keys().copied().collect::<Vec<_>>();
		tenant_ids.sort_by_key(|tenant_id| tenant_id.0);
		tenant_ids
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantConcurrencyRejection {
	pub tenant_id: TenantId,
	pub requested: u32,
	pub available: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantDescriptorSyncRejection {
	pub tenant_id: TenantId,
	pub requested: u32,
	pub available: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionResource {
	Memory,
	Concurrency,
	Rate,
}

#[derive(Debug, Clone)]
pub struct TenantAdmissionRejectionCounters {
	inner: Arc<TenantAdmissionRejectionCounterState>,
}

#[derive(Debug)]
struct TenantAdmissionRejectionCounterState {
	configured: HashMap<TenantId, TenantResourceRejectionCounters>,
	other: TenantResourceRejectionCounters,
}

#[derive(Debug, Default)]
struct TenantResourceRejectionCounters {
	memory: AtomicU64,
	concurrency: AtomicU64,
	rate: AtomicU64,
}

impl TenantAdmissionRejectionCounters {
	pub fn new(policy: &TenantBudgetPolicy) -> Self {
		let configured = policy
			.configured_tenant_ids()
			.into_iter()
			.map(|tenant_id| (tenant_id, TenantResourceRejectionCounters::default()))
			.collect();
		Self {
			inner: Arc::new(TenantAdmissionRejectionCounterState {
				configured,
				other: TenantResourceRejectionCounters::default(),
			}),
		}
	}

	pub fn increment(&self, tenant_id: TenantId, resource: AdmissionResource) {
		let counters = self
			.inner
			.configured
			.get(&tenant_id)
			.unwrap_or(&self.inner.other);
		counters.increment(resource);
	}

	pub fn get(&self, tenant_id: Option<TenantId>, resource: AdmissionResource) -> u64 {
		let counters = tenant_id
			.and_then(|tenant_id| self.inner.configured.get(&tenant_id))
			.unwrap_or(&self.inner.other);
		counters.get(resource)
	}
}

impl TenantResourceRejectionCounters {
	fn increment(&self, resource: AdmissionResource) {
		self.counter(resource).fetch_add(1, Ordering::Relaxed);
	}

	fn get(&self, resource: AdmissionResource) -> u64 {
		self.counter(resource).load(Ordering::Relaxed)
	}

	fn counter(&self, resource: AdmissionResource) -> &AtomicU64 {
		match resource {
			AdmissionResource::Memory => &self.memory,
			AdmissionResource::Concurrency => &self.concurrency,
			AdmissionResource::Rate => &self.rate,
		}
	}
}

trait AdmissionClock: Debug + Send + Sync {
	fn now_millis(&self) -> u64;
}

#[derive(Debug)]
struct SystemAdmissionClock {
	start: Instant,
}

impl SystemAdmissionClock {
	fn new() -> Self {
		Self {
			start: Instant::now(),
		}
	}
}

impl AdmissionClock for SystemAdmissionClock {
	fn now_millis(&self) -> u64 {
		u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionPolicy {
	pub max_queued_queries: u32,
	pub max_queue_wait_ms: DurationMillis,
	pub shared_memory: ByteCount,
	pub shared_concurrent_queries: u32,
}

impl AdmissionPolicy {
	pub const fn disabled() -> Self {
		Self {
			max_queued_queries: 0,
			max_queue_wait_ms: DurationMillis::new(0),
			shared_memory: ByteCount::ZERO,
			shared_concurrent_queries: 0,
		}
	}
}

#[derive(Debug, Clone)]
pub struct TenantAdmissionController {
	inner: Arc<TenantAdmissionControllerInner>,
}

#[derive(Debug)]
struct TenantAdmissionControllerInner {
	policy: TenantBudgetPolicy,
	admission: Option<AdmissionPolicy>,
	clock: Arc<dyn AdmissionClock>,
	state: Mutex<AdmissionState>,
}

#[derive(Debug, Default)]
struct AdmissionState {
	tenant_memory: HashMap<TenantId, ByteCount>,
	tenant_concurrency: HashMap<TenantId, u32>,
	borrowed_memory: HashMap<TenantId, ByteCount>,
	borrowed_concurrency: HashMap<TenantId, u32>,
	queues: HashMap<TenantId, VecDeque<Waiter>>,
	rate_buckets: HashMap<RateBucketKey, TokenBucket>,
	deficits: HashMap<TenantId, i64>,
	next_waiter_id: u64,
	next_sequence: u64,
	shared_memory_used: ByteCount,
	shared_concurrency_used: u32,
	queue_full_rejections: HashMap<TenantMetricKey, u64>,
	queue_timeouts: HashMap<TenantMetricKey, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TenantMetricKey {
	Configured(TenantId),
	Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RateBucketKey {
	Configured(TenantId),
	Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TokenBucket {
	tokens: u32,
	last_refill_millis: u64,
}

#[derive(Debug)]
struct Waiter {
	id: WaiterId,
	tenant_id: TenantId,
	memory: ByteCount,
	sequence: u64,
	deadline_ms: u32,
	grant: oneshot::Sender<AdmissionReservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct WaiterId(u64);

#[derive(Debug)]
pub enum AdmissionAttempt {
	Granted(AdmissionReservation),
	Queued(QueuedAdmission),
	Rejected(TenantAdmissionRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantAdmissionRejection {
	Memory {
		tenant_id: TenantId,
		rejection: BudgetRejection,
	},
	Concurrency(TenantConcurrencyRejection),
	QueueDisabled {
		tenant_id: TenantId,
	},
	QueueFull {
		tenant_id: TenantId,
	},
	QueueTimeout {
		tenant_id: TenantId,
	},
	RequestTooLarge {
		tenant_id: TenantId,
	},
	GrantCanceled {
		tenant_id: TenantId,
	},
	RateLimited {
		tenant_id: TenantId,
	},
}

#[derive(Debug)]
pub struct QueuedAdmission {
	inner: Arc<TenantAdmissionControllerInner>,
	tenant_id: TenantId,
	id: WaiterId,
	grant: Option<oneshot::Receiver<AdmissionReservation>>,
	cleanup: bool,
}

#[derive(Debug)]
pub struct AdmissionReservation {
	inner: Arc<TenantAdmissionControllerInner>,
	tenant_id: TenantId,
	memory: ByteCount,
	owned_concurrency: u32,
	borrowed_memory: ByteCount,
	borrowed_concurrency: u32,
	released: bool,
}

#[derive(Debug)]
struct GrantDelivery {
	id: WaiterId,
	grant: oneshot::Sender<AdmissionReservation>,
	reservation: AdmissionReservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantAdmissionMetricSlot {
	pub tenant_id: Option<TenantId>,
	pub label: String,
	pub queue_depth: u64,
	pub borrowed_memory: ByteCount,
	pub borrowed_concurrency: u64,
	pub queue_timeouts: u64,
	pub queue_full_rejections: u64,
}

#[derive(Debug, Clone, Copy)]
struct GrantResources {
	owned_memory: ByteCount,
	borrowed_memory: ByteCount,
	owned_concurrency: u32,
	borrowed_concurrency: u32,
}

impl TenantAdmissionController {
	pub fn new(policy: TenantBudgetPolicy, admission: Option<AdmissionPolicy>) -> Self {
		Self {
			inner: Arc::new(TenantAdmissionControllerInner {
				policy,
				admission,
				clock: Arc::new(SystemAdmissionClock::new()),
				state: Mutex::new(AdmissionState::default()),
			}),
		}
	}

	#[cfg(test)]
	fn with_clock(
		policy: TenantBudgetPolicy,
		admission: Option<AdmissionPolicy>,
		clock: Arc<dyn AdmissionClock>,
	) -> Self {
		Self {
			inner: Arc::new(TenantAdmissionControllerInner {
				policy,
				admission,
				clock,
				state: Mutex::new(AdmissionState::default()),
			}),
		}
	}

	pub fn try_admit(&self, tenant_id: TenantId, memory: ByteCount) -> AdmissionAttempt {
		self.inner.try_admit(tenant_id, memory)
	}

	pub fn used_memory(&self, tenant_id: TenantId) -> ByteCount {
		self.inner
			.state
			.lock()
			.expect("tenant admission controller poisoned")
			.tenant_memory
			.get(&tenant_id)
			.copied()
			.unwrap_or(ByteCount::ZERO)
	}

	pub fn active_concurrency(&self, tenant_id: TenantId) -> u32 {
		self.inner
			.state
			.lock()
			.expect("tenant admission controller poisoned")
			.tenant_concurrency
			.get(&tenant_id)
			.copied()
			.unwrap_or(0)
	}

	pub fn memory_snapshot(&self) -> HashMap<TenantId, ByteCount> {
		self.inner
			.state
			.lock()
			.expect("tenant admission controller poisoned")
			.tenant_memory
			.clone()
	}

	pub fn concurrency_snapshot(&self) -> HashMap<TenantId, u32> {
		self.inner
			.state
			.lock()
			.expect("tenant admission controller poisoned")
			.tenant_concurrency
			.clone()
	}

	pub fn metric_slots(&self) -> Vec<TenantAdmissionMetricSlot> {
		let state = self
			.inner
			.state
			.lock()
			.expect("tenant admission controller poisoned");
		let mut slots = Vec::new();
		for tenant_id in self.inner.policy.configured_tenant_ids() {
			let key = TenantMetricKey::Configured(tenant_id);
			slots.push(TenantAdmissionMetricSlot {
				tenant_id: Some(tenant_id),
				label: tenant_id.0.to_string(),
				queue_depth: state
					.queues
					.get(&tenant_id)
					.map(|queue| u64::try_from(queue.len()).unwrap_or(u64::MAX))
					.unwrap_or(0),
				borrowed_memory: state
					.borrowed_memory
					.get(&tenant_id)
					.copied()
					.unwrap_or(ByteCount::ZERO),
				borrowed_concurrency: u64::from(
					state
						.borrowed_concurrency
						.get(&tenant_id)
						.copied()
						.unwrap_or(0),
				),
				queue_timeouts: state.queue_timeouts.get(&key).copied().unwrap_or(0),
				queue_full_rejections: state.queue_full_rejections.get(&key).copied().unwrap_or(0),
			});
		}

		let other_depth = state
			.queues
			.iter()
			.filter(|(tenant_id, _)| !self.inner.policy.is_configured_tenant(**tenant_id))
			.fold(0_u64, |total, (_, queue)| {
				total.saturating_add(u64::try_from(queue.len()).unwrap_or(u64::MAX))
			});
		let other_borrowed_memory = state
			.borrowed_memory
			.iter()
			.filter(|(tenant_id, _)| !self.inner.policy.is_configured_tenant(**tenant_id))
			.fold(ByteCount::ZERO, |total, (_, used)| {
				total.saturating_add(*used)
			});
		let other_borrowed_concurrency = state
			.borrowed_concurrency
			.iter()
			.filter(|(tenant_id, _)| !self.inner.policy.is_configured_tenant(**tenant_id))
			.fold(0_u64, |total, (_, active)| {
				total.saturating_add(u64::from(*active))
			});
		slots.push(TenantAdmissionMetricSlot {
			tenant_id: None,
			label: "other".to_string(),
			queue_depth: other_depth,
			borrowed_memory: other_borrowed_memory,
			borrowed_concurrency: other_borrowed_concurrency,
			queue_timeouts: state
				.queue_timeouts
				.get(&TenantMetricKey::Other)
				.copied()
				.unwrap_or(0),
			queue_full_rejections: state
				.queue_full_rejections
				.get(&TenantMetricKey::Other)
				.copied()
				.unwrap_or(0),
		});
		slots
	}
}

impl TenantAdmissionControllerInner {
	fn try_admit(self: &Arc<Self>, tenant_id: TenantId, memory: ByteCount) -> AdmissionAttempt {
		let budget = self.policy.budget_for(tenant_id);
		let mut state = self
			.state
			.lock()
			.expect("tenant admission controller poisoned");
		if let Err(rejection) = self.try_consume_rate(&mut state, tenant_id, budget) {
			return AdmissionAttempt::Rejected(rejection);
		}
		let Some(admission) = self.admission else {
			return match self.try_grant_owned_legacy_order(&mut state, tenant_id, memory, budget) {
				Ok(reservation) => AdmissionAttempt::Granted(reservation),
				Err(rejection) => AdmissionAttempt::Rejected(rejection),
			};
		};

		if !state.has_waiters(tenant_id)
			&& let Ok(reservation) =
				self.try_grant_owned_legacy_order(&mut state, tenant_id, memory, budget)
		{
			return AdmissionAttempt::Granted(reservation);
		}
		if budget.max_queued_queries == 0 || admission.max_queued_queries == 0 {
			return AdmissionAttempt::Rejected(TenantAdmissionRejection::QueueDisabled {
				tenant_id,
			});
		}
		if !request_can_ever_fit(memory, budget) {
			return AdmissionAttempt::Rejected(TenantAdmissionRejection::RequestTooLarge {
				tenant_id,
			});
		}
		if admission.max_queue_wait_ms.get() == 0 {
			return AdmissionAttempt::Rejected(TenantAdmissionRejection::QueueTimeout {
				tenant_id,
			});
		}
		if state.total_queue_depth() >= admission.max_queued_queries
			|| state.tenant_queue_depth(tenant_id) >= budget.max_queued_queries
		{
			state.increment_queue_full(self.metric_key(tenant_id));
			return AdmissionAttempt::Rejected(TenantAdmissionRejection::QueueFull { tenant_id });
		}

		let (grant_tx, grant_rx) = oneshot::channel();
		let id = state.next_waiter_id();
		let waiter = Waiter {
			id,
			tenant_id,
			memory,
			sequence: state.next_sequence(),
			deadline_ms: admission.max_queue_wait_ms.get(),
			grant: grant_tx,
		};
		state.queues.entry(tenant_id).or_default().push_back(waiter);
		let mut deliveries = self.pump_locked(&mut state);
		let own_delivery = deliveries
			.iter()
			.position(|delivery| delivery.id == id)
			.map(|index| deliveries.remove(index));
		drop(state);
		send_grants(deliveries);
		if let Some(delivery) = own_delivery {
			AdmissionAttempt::Granted(delivery.reservation)
		} else {
			AdmissionAttempt::Queued(QueuedAdmission {
				inner: Arc::clone(self),
				tenant_id,
				id,
				grant: Some(grant_rx),
				cleanup: true,
			})
		}
	}

	fn try_consume_rate(
		&self,
		state: &mut AdmissionState,
		tenant_id: TenantId,
		budget: TenantBudget,
	) -> Result<(), TenantAdmissionRejection> {
		if !budget.rate_enabled() {
			return Ok(());
		}
		let rate = budget.max_queries_per_second;
		let burst = budget.effective_burst();
		if burst == 0 {
			return Ok(());
		}
		let now = self.clock.now_millis();
		let key = self.rate_bucket_key(tenant_id);
		let bucket = state.rate_buckets.entry(key).or_insert(TokenBucket {
			tokens: burst,
			last_refill_millis: now,
		});
		refill_rate_bucket(bucket, now, rate, burst);
		if bucket.tokens == 0 {
			return Err(TenantAdmissionRejection::RateLimited { tenant_id });
		}
		bucket.tokens = bucket.tokens.saturating_sub(1);
		Ok(())
	}

	fn try_grant_owned_legacy_order(
		self: &Arc<Self>,
		state: &mut AdmissionState,
		tenant_id: TenantId,
		memory: ByteCount,
		budget: TenantBudget,
	) -> Result<AdmissionReservation, TenantAdmissionRejection> {
		let active = state
			.tenant_concurrency
			.get(&tenant_id)
			.copied()
			.unwrap_or(0);
		let concurrency_limit = budget.max_concurrent_queries.get();
		if active >= concurrency_limit {
			return Err(TenantAdmissionRejection::Concurrency(
				TenantConcurrencyRejection {
					tenant_id,
					requested: 1,
					available: concurrency_limit.saturating_sub(active),
				},
			));
		}
		let current_memory = state
			.tenant_memory
			.get(&tenant_id)
			.copied()
			.unwrap_or(ByteCount::ZERO);
		let available_memory = budget.aggregate_memory.saturating_sub(current_memory);
		if memory > available_memory {
			return Err(TenantAdmissionRejection::Memory {
				tenant_id,
				rejection: BudgetRejection {
					dimension: BudgetDimension::Memory,
					requested: memory,
					available: available_memory,
				},
			});
		}
		apply_grant(
			state,
			tenant_id,
			GrantResources {
				owned_memory: memory,
				borrowed_memory: ByteCount::ZERO,
				owned_concurrency: 1,
				borrowed_concurrency: 0,
			},
		);
		Ok(AdmissionReservation {
			inner: Arc::clone(self),
			tenant_id,
			memory,
			owned_concurrency: 1,
			borrowed_memory: ByteCount::ZERO,
			borrowed_concurrency: 0,
			released: false,
		})
	}

	fn pump_locked(self: &Arc<Self>, state: &mut AdmissionState) -> Vec<GrantDelivery> {
		let mut deliveries = Vec::new();
		loop {
			let Some((tenant_id, resources)) = self.pick_grantable_waiter(state) else {
				break;
			};
			let Some(waiter) = state
				.queues
				.get_mut(&tenant_id)
				.and_then(VecDeque::pop_front)
			else {
				break;
			};
			debug_assert_eq!(waiter.tenant_id, tenant_id);
			let _deadline_ms = waiter.deadline_ms;
			state.remove_queue_if_empty(tenant_id);
			apply_grant(state, tenant_id, resources);
			let reservation = AdmissionReservation {
				inner: Arc::clone(self),
				tenant_id,
				memory: resources.owned_memory,
				owned_concurrency: resources.owned_concurrency,
				borrowed_memory: resources.borrowed_memory,
				borrowed_concurrency: resources.borrowed_concurrency,
				released: false,
			};
			deliveries.push(GrantDelivery {
				id: waiter.id,
				grant: waiter.grant,
				reservation,
			});
		}
		deliveries
	}

	fn pick_grantable_waiter(
		&self,
		state: &mut AdmissionState,
	) -> Option<(TenantId, GrantResources)> {
		let mut tenants = state.queued_tenants();
		if tenants.is_empty() {
			return None;
		}
		let total_weight = tenants
			.iter()
			.map(|tenant_id| i64::from(self.policy.budget_for(*tenant_id).admission_weight.get()))
			.sum::<i64>()
			.max(1);
		for tenant_id in &tenants {
			let weight = i64::from(self.policy.budget_for(*tenant_id).admission_weight.get());
			*state.deficits.entry(*tenant_id).or_default() += weight;
		}
		tenants.sort_by_key(|tenant_id| tenant_id.0);
		let mut best: Option<(TenantId, GrantResources, i64, u64)> = None;
		for tenant_id in tenants {
			let Some(waiter) = state.queues.get(&tenant_id).and_then(|queue| queue.front()) else {
				continue;
			};
			let budget = self.policy.budget_for(tenant_id);
			let Some(resources) =
				compute_grant(state, self.admission, tenant_id, waiter.memory, budget)
			else {
				continue;
			};
			let deficit = state.deficits.get(&tenant_id).copied().unwrap_or(0);
			let sequence = waiter.sequence;
			let replace = best
				.as_ref()
				.is_none_or(|(_, _, best_deficit, best_sequence)| {
					deficit > *best_deficit
						|| (deficit == *best_deficit && sequence < *best_sequence)
				});
			if replace {
				best = Some((tenant_id, resources, deficit, sequence));
			}
		}
		let (tenant_id, resources, _, _) = best?;
		*state.deficits.entry(tenant_id).or_default() -= total_weight;
		Some((tenant_id, resources))
	}

	fn cancel_waiter(
		self: &Arc<Self>,
		tenant_id: TenantId,
		id: WaiterId,
		reason: WaiterCancelReason,
	) -> bool {
		let mut state = self
			.state
			.lock()
			.expect("tenant admission controller poisoned");
		let removed = state.remove_waiter(tenant_id, id);
		if removed && reason == WaiterCancelReason::Timeout {
			state.increment_queue_timeout(self.metric_key(tenant_id));
		}
		let deliveries = self.pump_locked(&mut state);
		drop(state);
		send_grants(deliveries);
		removed
	}

	fn release_reservation(
		self: &Arc<Self>,
		tenant_id: TenantId,
		memory: ByteCount,
		owned_concurrency: u32,
		borrowed_memory: ByteCount,
		borrowed_concurrency: u32,
	) {
		let mut state = self
			.state
			.lock()
			.expect("tenant admission controller poisoned");
		sub_memory(&mut state.tenant_memory, tenant_id, memory);
		sub_u32(&mut state.tenant_concurrency, tenant_id, owned_concurrency);
		sub_memory(&mut state.borrowed_memory, tenant_id, borrowed_memory);
		sub_u32(
			&mut state.borrowed_concurrency,
			tenant_id,
			borrowed_concurrency,
		);
		state.shared_memory_used = state.shared_memory_used.saturating_sub(borrowed_memory);
		state.shared_concurrency_used = state
			.shared_concurrency_used
			.saturating_sub(borrowed_concurrency);
		let deliveries = self.pump_locked(&mut state);
		drop(state);
		send_grants(deliveries);
	}

	fn metric_key(&self, tenant_id: TenantId) -> TenantMetricKey {
		if self.policy.is_configured_tenant(tenant_id) {
			TenantMetricKey::Configured(tenant_id)
		} else {
			TenantMetricKey::Other
		}
	}

	fn rate_bucket_key(&self, tenant_id: TenantId) -> RateBucketKey {
		if self.policy.is_configured_tenant(tenant_id) {
			RateBucketKey::Configured(tenant_id)
		} else {
			RateBucketKey::Other
		}
	}
}

fn refill_rate_bucket(bucket: &mut TokenBucket, now: u64, rate: u32, burst: u32) {
	let elapsed = now.saturating_sub(bucket.last_refill_millis);
	let added = u128::from(elapsed) * u128::from(rate) / 1_000;
	if added == 0 {
		return;
	}
	let added_tokens = u32::try_from(added).unwrap_or(u32::MAX);
	bucket.tokens = bucket.tokens.saturating_add(added_tokens).min(burst);
	let consumed_millis = added.saturating_mul(1_000) / u128::from(rate);
	let advance = u64::try_from(consumed_millis).unwrap_or(u64::MAX);
	bucket.last_refill_millis = bucket.last_refill_millis.saturating_add(advance);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaiterCancelReason {
	Cancelled,
	Timeout,
}

impl QueuedAdmission {
	pub async fn recv(&mut self) -> Result<AdmissionReservation, TenantAdmissionRejection> {
		let grant = self
			.grant
			.as_mut()
			.expect("queued admission grant receiver missing");
		match grant.await {
			Ok(reservation) => {
				self.cleanup = false;
				self.grant = None;
				Ok(reservation)
			}
			Err(_) => {
				self.cleanup = false;
				self.grant = None;
				Err(TenantAdmissionRejection::GrantCanceled {
					tenant_id: self.tenant_id,
				})
			}
		}
	}

	pub fn cancel_due_to_timeout(&mut self) {
		if self.cleanup {
			self.inner
				.cancel_waiter(self.tenant_id, self.id, WaiterCancelReason::Timeout);
			self.cleanup = false;
			self.grant = None;
		}
	}
}

impl Drop for QueuedAdmission {
	fn drop(&mut self) {
		if self.cleanup {
			self.inner
				.cancel_waiter(self.tenant_id, self.id, WaiterCancelReason::Cancelled);
			self.cleanup = false;
		}
	}
}

impl AdmissionReservation {
	pub fn release(mut self) {
		self.release_once();
	}

	fn release_once(&mut self) {
		if self.released {
			return;
		}
		self.inner.release_reservation(
			self.tenant_id,
			self.memory,
			self.owned_concurrency,
			self.borrowed_memory,
			self.borrowed_concurrency,
		);
		self.released = true;
	}
}

impl Drop for AdmissionReservation {
	fn drop(&mut self) {
		self.release_once();
	}
}

impl AdmissionState {
	fn has_waiters(&self, tenant_id: TenantId) -> bool {
		self.queues
			.get(&tenant_id)
			.is_some_and(|queue| !queue.is_empty())
	}

	fn tenant_queue_depth(&self, tenant_id: TenantId) -> u32 {
		self.queues
			.get(&tenant_id)
			.map(|queue| u32::try_from(queue.len()).unwrap_or(u32::MAX))
			.unwrap_or(0)
	}

	fn total_queue_depth(&self) -> u32 {
		self.queues.values().fold(0_u32, |total, queue| {
			total.saturating_add(u32::try_from(queue.len()).unwrap_or(u32::MAX))
		})
	}

	fn next_waiter_id(&mut self) -> WaiterId {
		let id = WaiterId(self.next_waiter_id);
		self.next_waiter_id = self.next_waiter_id.saturating_add(1);
		id
	}

	fn next_sequence(&mut self) -> u64 {
		let sequence = self.next_sequence;
		self.next_sequence = self.next_sequence.saturating_add(1);
		sequence
	}

	fn queued_tenants(&self) -> Vec<TenantId> {
		self.queues
			.iter()
			.filter(|(_, queue)| !queue.is_empty())
			.map(|(tenant_id, _)| *tenant_id)
			.collect()
	}

	fn remove_queue_if_empty(&mut self, tenant_id: TenantId) {
		if self.queues.get(&tenant_id).is_some_and(VecDeque::is_empty) {
			self.queues.remove(&tenant_id);
			self.deficits.remove(&tenant_id);
		}
	}

	fn remove_waiter(&mut self, tenant_id: TenantId, id: WaiterId) -> bool {
		let Some(queue) = self.queues.get_mut(&tenant_id) else {
			return false;
		};
		let Some(index) = queue.iter().position(|waiter| waiter.id == id) else {
			return false;
		};
		queue.remove(index);
		self.remove_queue_if_empty(tenant_id);
		true
	}

	fn increment_queue_full(&mut self, key: TenantMetricKey) {
		*self.queue_full_rejections.entry(key).or_default() += 1;
	}

	fn increment_queue_timeout(&mut self, key: TenantMetricKey) {
		*self.queue_timeouts.entry(key).or_default() += 1;
	}
}

fn request_can_ever_fit(memory: ByteCount, budget: TenantBudget) -> bool {
	memory
		<= budget
			.aggregate_memory
			.saturating_add(budget.max_borrowed_memory)
		&& budget
			.max_concurrent_queries
			.get()
			.saturating_add(budget.max_borrowed_concurrent_queries)
			>= 1
}

fn compute_grant(
	state: &AdmissionState,
	admission: Option<AdmissionPolicy>,
	tenant_id: TenantId,
	memory: ByteCount,
	budget: TenantBudget,
) -> Option<GrantResources> {
	let admission = admission.unwrap_or_else(AdmissionPolicy::disabled);
	let current_memory = state
		.tenant_memory
		.get(&tenant_id)
		.copied()
		.unwrap_or(ByteCount::ZERO);
	let owned_memory_available = budget.aggregate_memory.saturating_sub(current_memory);
	let owned_memory = memory.min(owned_memory_available);
	let borrowed_memory = memory.saturating_sub(owned_memory);
	let tenant_borrowed_memory = state
		.borrowed_memory
		.get(&tenant_id)
		.copied()
		.unwrap_or(ByteCount::ZERO);
	let tenant_borrowed_memory_available = budget
		.max_borrowed_memory
		.saturating_sub(tenant_borrowed_memory);
	let shared_memory_available = admission
		.shared_memory
		.saturating_sub(state.shared_memory_used);
	if borrowed_memory > tenant_borrowed_memory_available
		|| borrowed_memory > shared_memory_available
	{
		return None;
	}

	let active = state
		.tenant_concurrency
		.get(&tenant_id)
		.copied()
		.unwrap_or(0);
	let owned_concurrency_limit = budget.max_concurrent_queries.get();
	let (owned_concurrency, borrowed_concurrency) = if active < owned_concurrency_limit {
		(1, 0)
	} else {
		let tenant_borrowed = state
			.borrowed_concurrency
			.get(&tenant_id)
			.copied()
			.unwrap_or(0);
		if tenant_borrowed >= budget.max_borrowed_concurrent_queries {
			return None;
		}
		if state.shared_concurrency_used >= admission.shared_concurrent_queries {
			return None;
		}
		(0, 1)
	};

	Some(GrantResources {
		owned_memory,
		borrowed_memory,
		owned_concurrency,
		borrowed_concurrency,
	})
}

fn apply_grant(state: &mut AdmissionState, tenant_id: TenantId, resources: GrantResources) {
	add_memory(&mut state.tenant_memory, tenant_id, resources.owned_memory);
	add_u32(
		&mut state.tenant_concurrency,
		tenant_id,
		resources.owned_concurrency,
	);
	add_memory(
		&mut state.borrowed_memory,
		tenant_id,
		resources.borrowed_memory,
	);
	add_u32(
		&mut state.borrowed_concurrency,
		tenant_id,
		resources.borrowed_concurrency,
	);
	state.shared_memory_used = state
		.shared_memory_used
		.saturating_add(resources.borrowed_memory);
	state.shared_concurrency_used = state
		.shared_concurrency_used
		.saturating_add(resources.borrowed_concurrency);
}

fn add_memory(map: &mut HashMap<TenantId, ByteCount>, tenant_id: TenantId, amount: ByteCount) {
	if amount == ByteCount::ZERO {
		return;
	}
	let current = map.get(&tenant_id).copied().unwrap_or(ByteCount::ZERO);
	map.insert(tenant_id, current.saturating_add(amount));
}

fn sub_memory(map: &mut HashMap<TenantId, ByteCount>, tenant_id: TenantId, amount: ByteCount) {
	if amount == ByteCount::ZERO {
		return;
	}
	let current = map.get(&tenant_id).copied().unwrap_or(ByteCount::ZERO);
	let updated = current.saturating_sub(amount);
	if updated == ByteCount::ZERO {
		map.remove(&tenant_id);
	} else {
		map.insert(tenant_id, updated);
	}
}

fn add_u32(map: &mut HashMap<TenantId, u32>, tenant_id: TenantId, amount: u32) {
	if amount == 0 {
		return;
	}
	let current = map.get(&tenant_id).copied().unwrap_or(0);
	map.insert(tenant_id, current.saturating_add(amount));
}

fn sub_u32(map: &mut HashMap<TenantId, u32>, tenant_id: TenantId, amount: u32) {
	if amount == 0 {
		return;
	}
	let current = map.get(&tenant_id).copied().unwrap_or(0);
	let updated = current.saturating_sub(amount);
	if updated == 0 {
		map.remove(&tenant_id);
	} else {
		map.insert(tenant_id, updated);
	}
}

fn send_grants(deliveries: Vec<GrantDelivery>) {
	for delivery in deliveries {
		if let Err(reservation) = delivery.grant.send(delivery.reservation) {
			drop(reservation);
		}
	}
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

	pub fn snapshot(&self) -> HashMap<TenantId, ByteCount> {
		self.used
			.lock()
			.expect("tenant memory accountant poisoned")
			.clone()
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

#[derive(Debug, Clone)]
pub struct TenantConcurrencyAccountant {
	policy: TenantBudgetPolicy,
	active: Arc<Mutex<HashMap<TenantId, u32>>>,
}

impl TenantConcurrencyAccountant {
	pub fn new(policy: TenantBudgetPolicy) -> Self {
		Self {
			policy,
			active: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn try_reserve(
		&self,
		tenant_id: TenantId,
	) -> Result<TenantConcurrencyReservation, TenantConcurrencyRejection> {
		let mut active = self
			.active
			.lock()
			.expect("tenant concurrency accountant poisoned");
		let current = active.get(&tenant_id).copied().unwrap_or(0);
		let limit = self
			.policy
			.budget_for(tenant_id)
			.max_concurrent_queries
			.get();
		let available = limit.saturating_sub(current);
		if available == 0 {
			return Err(TenantConcurrencyRejection {
				tenant_id,
				requested: 1,
				available,
			});
		}
		active.insert(tenant_id, current + 1);
		Ok(TenantConcurrencyReservation {
			active: Arc::clone(&self.active),
			tenant_id,
			released: false,
		})
	}

	pub fn active(&self, tenant_id: TenantId) -> u32 {
		self.active
			.lock()
			.expect("tenant concurrency accountant poisoned")
			.get(&tenant_id)
			.copied()
			.unwrap_or(0)
	}

	pub fn snapshot(&self) -> HashMap<TenantId, u32> {
		self.active
			.lock()
			.expect("tenant concurrency accountant poisoned")
			.clone()
	}
}

#[derive(Debug)]
pub struct TenantConcurrencyReservation {
	active: Arc<Mutex<HashMap<TenantId, u32>>>,
	tenant_id: TenantId,
	released: bool,
}

impl TenantConcurrencyReservation {
	pub fn release(mut self) {
		self.release_once();
	}

	fn release_once(&mut self) {
		if self.released {
			return;
		}
		let mut active = self
			.active
			.lock()
			.expect("tenant concurrency accountant poisoned");
		let current = active.get(&self.tenant_id).copied().unwrap_or(0);
		let updated = current.saturating_sub(1);
		if updated == 0 {
			active.remove(&self.tenant_id);
		} else {
			active.insert(self.tenant_id, updated);
		}
		self.released = true;
	}
}

impl Drop for TenantConcurrencyReservation {
	fn drop(&mut self) {
		self.release_once();
	}
}

#[derive(Debug, Clone)]
pub struct TenantDescriptorSyncAccountant {
	policy: TenantBudgetPolicy,
	active: Arc<Mutex<HashMap<TenantId, u32>>>,
}

impl TenantDescriptorSyncAccountant {
	pub fn new(policy: TenantBudgetPolicy) -> Self {
		Self {
			policy,
			active: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn try_reserve(
		&self,
		tenant_id: TenantId,
	) -> Result<TenantDescriptorSyncReservation, TenantDescriptorSyncRejection> {
		let mut active = self
			.active
			.lock()
			.expect("tenant descriptor sync accountant poisoned");
		let current = active.get(&tenant_id).copied().unwrap_or(0);
		let limit = self
			.policy
			.budget_for(tenant_id)
			.max_concurrent_descriptor_syncs
			.get();
		let available = limit.saturating_sub(current);
		if available == 0 {
			return Err(TenantDescriptorSyncRejection {
				tenant_id,
				requested: 1,
				available,
			});
		}
		active.insert(tenant_id, current + 1);
		Ok(TenantDescriptorSyncReservation {
			active: Arc::clone(&self.active),
			tenant_id,
			released: false,
		})
	}

	pub fn active(&self, tenant_id: TenantId) -> u32 {
		self.active
			.lock()
			.expect("tenant descriptor sync accountant poisoned")
			.get(&tenant_id)
			.copied()
			.unwrap_or(0)
	}

	pub fn snapshot(&self) -> HashMap<TenantId, u32> {
		self.active
			.lock()
			.expect("tenant descriptor sync accountant poisoned")
			.clone()
	}
}

#[derive(Debug)]
pub struct TenantDescriptorSyncReservation {
	active: Arc<Mutex<HashMap<TenantId, u32>>>,
	tenant_id: TenantId,
	released: bool,
}

impl TenantDescriptorSyncReservation {
	pub fn release(mut self) {
		self.release_once();
	}

	fn release_once(&mut self) {
		if self.released {
			return;
		}
		let mut active = self
			.active
			.lock()
			.expect("tenant descriptor sync accountant poisoned");
		let current = active.get(&self.tenant_id).copied().unwrap_or(0);
		let updated = current.saturating_sub(1);
		if updated == 0 {
			active.remove(&self.tenant_id);
		} else {
			active.insert(self.tenant_id, updated);
		}
		self.released = true;
	}
}

impl Drop for TenantDescriptorSyncReservation {
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

	#[derive(Debug, Default)]
	struct ManualAdmissionClock {
		now_millis: AtomicU64,
	}

	impl ManualAdmissionClock {
		fn advance_millis(&self, millis: u64) {
			self.now_millis.fetch_add(millis, Ordering::Relaxed);
		}
	}

	impl AdmissionClock for ManualAdmissionClock {
		fn now_millis(&self) -> u64 {
			self.now_millis.load(Ordering::Relaxed)
		}
	}

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
	fn tenant_budget_derives_concurrency_from_memory_ratio() {
		let budget = TenantBudget::new(ByteCount::new(8), ByteCount::new(32));
		assert_eq!(budget.max_concurrent_queries.get(), 4);
		assert_eq!(budget.max_concurrent_descriptor_syncs.get(), 2);

		let fallback = TenantBudgetPolicy::from_query_budget(QueryBudget {
			memory_per_task: ByteCount::new(8),
			..QueryBudget::test_default()
		});
		assert_eq!(fallback.default.max_concurrent_queries.get(), 1);
		assert_eq!(fallback.default.max_concurrent_descriptor_syncs.get(), 1);
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

	#[test]
	fn tenant_concurrency_accountant_releases_on_drop() {
		let policy = TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(8), ByteCount::new(16)),
			HashMap::new(),
		);
		let accountant = TenantConcurrencyAccountant::new(policy);

		let reservation = accountant.try_reserve(TenantId(7)).expect("reserve");
		assert_eq!(accountant.active(TenantId(7)), 1);

		drop(reservation);

		assert_eq!(accountant.active(TenantId(7)), 0);
	}

	#[test]
	fn tenant_concurrency_accountant_isolated_by_tenant() {
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
		let accountant = TenantConcurrencyAccountant::new(policy);

		let _tenant_7 = accountant
			.try_reserve(TenantId(7))
			.expect("tenant 7 reserve");
		assert!(matches!(
			accountant.try_reserve(TenantId(7)),
			Err(TenantConcurrencyRejection {
				tenant_id: TenantId(7),
				requested: 1,
				available: 0,
			})
		));

		let _tenant_8_a = accountant
			.try_reserve(TenantId(8))
			.expect("tenant 8 first reserve");
		let _tenant_8_b = accountant
			.try_reserve(TenantId(8))
			.expect("tenant 8 second reserve");
		assert_eq!(accountant.active(TenantId(8)), 2);
	}

	#[test]
	fn tenant_descriptor_sync_accountant_releases_on_drop() {
		let policy = TenantBudgetPolicy::new(
			TenantBudget::with_query_and_descriptor_sync_limits(
				ByteCount::new(8),
				ByteCount::new(16),
				NonZeroU32::new(2).expect("nonzero"),
				NonZeroU32::new(1).expect("nonzero"),
			),
			HashMap::new(),
		);
		let accountant = TenantDescriptorSyncAccountant::new(policy);

		let reservation = accountant.try_reserve(TenantId(7)).expect("reserve");
		assert_eq!(accountant.active(TenantId(7)), 1);
		assert!(accountant.try_reserve(TenantId(7)).is_err());

		drop(reservation);

		assert_eq!(accountant.active(TenantId(7)), 0);
		assert!(accountant.try_reserve(TenantId(7)).is_ok());
	}

	#[test]
	fn tenant_descriptor_sync_accountant_isolated_by_tenant() {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(
			TenantId(7),
			TenantBudget::with_query_and_descriptor_sync_limits(
				ByteCount::new(8),
				ByteCount::new(16),
				NonZeroU32::new(2).expect("nonzero"),
				NonZeroU32::new(1).expect("nonzero"),
			),
		);
		per_tenant.insert(
			TenantId(8),
			TenantBudget::with_query_and_descriptor_sync_limits(
				ByteCount::new(8),
				ByteCount::new(16),
				NonZeroU32::new(2).expect("nonzero"),
				NonZeroU32::new(2).expect("nonzero"),
			),
		);
		let policy = TenantBudgetPolicy::new(
			TenantBudget::with_query_and_descriptor_sync_limits(
				ByteCount::new(8),
				ByteCount::new(16),
				NonZeroU32::new(2).expect("nonzero"),
				NonZeroU32::new(1).expect("nonzero"),
			),
			per_tenant,
		);
		let accountant = TenantDescriptorSyncAccountant::new(policy);

		let _tenant_7 = accountant.try_reserve(TenantId(7)).expect("tenant 7");
		let _tenant_8_a = accountant.try_reserve(TenantId(8)).expect("tenant 8 a");
		let _tenant_8_b = accountant.try_reserve(TenantId(8)).expect("tenant 8 b");

		assert!(accountant.try_reserve(TenantId(7)).is_err());
		assert!(accountant.try_reserve(TenantId(8)).is_err());
		assert_eq!(accountant.active(TenantId(7)), 1);
		assert_eq!(accountant.active(TenantId(8)), 2);
	}

	#[test]
	fn omitted_admission_config_preserves_memory_and_concurrency_hard_reject() {
		let controller = TenantAdmissionController::new(single_tenant_policy(), None);
		let _held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::Concurrency(
				TenantConcurrencyRejection {
					tenant_id: TenantId(7),
					requested: 1,
					available: 0,
				}
			))
		));
	}

	#[test]
	fn queue_disabled_for_tenant_rejects_immediately() {
		let controller =
			TenantAdmissionController::new(single_tenant_policy(), Some(admission_policy()));
		let _held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::QueueDisabled {
				tenant_id: TenantId(7),
			})
		));
	}

	#[test]
	fn default_zero_rate_preserves_current_admission_behavior() {
		let controller = TenantAdmissionController::new(single_tenant_policy(), None);
		let _held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::Concurrency(_))
		));
	}

	#[test]
	fn burst_capacity_admits_then_rejects() {
		let controller = TenantAdmissionController::new(
			TenantBudgetPolicy::new(rate_budget(8, 128, 8, 60, 2), HashMap::new()),
			None,
		);
		let _first = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _second = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited {
				tenant_id: TenantId(7),
			})
		));
	}

	#[test]
	fn tokens_refill_with_injected_clock() {
		let clock = Arc::new(ManualAdmissionClock::default());
		let controller = TenantAdmissionController::with_clock(
			TenantBudgetPolicy::new(rate_budget(8, 128, 8, 2, 1), HashMap::new()),
			None,
			clock.clone(),
		);
		let _first = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited { .. })
		));

		clock.advance_millis(500);

		let _second = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
	}

	#[test]
	fn slow_subgranularity_rate_still_accrues_over_time() {
		let clock = Arc::new(ManualAdmissionClock::default());
		let controller = TenantAdmissionController::with_clock(
			TenantBudgetPolicy::new(rate_budget(8, 128, 8, 1, 1), HashMap::new()),
			None,
			clock.clone(),
		);
		let _first = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		clock.advance_millis(400);
		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited { .. })
		));
		clock.advance_millis(400);
		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited { .. })
		));
		clock.advance_millis(200);

		let _second = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
	}

	#[test]
	fn rate_rejected_query_never_enters_admission_queue() {
		let controller = TenantAdmissionController::new(
			TenantBudgetPolicy::new(rate_budget(8, 8, 1, 1, 1), HashMap::new()),
			Some(admission_policy()),
		);
		let _held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited {
				tenant_id: TenantId(7),
			})
		));
		assert_eq!(controller.metric_slots()[0].queue_depth, 0);
	}

	#[test]
	fn token_consumed_on_pass_not_refunded_on_queue_timeout() {
		let mut budget = admission_budget(8, 8, 1, 0, 0, 1);
		budget.max_queries_per_second = 1;
		budget.query_burst_capacity = 2;
		let clock = Arc::new(ManualAdmissionClock::default());
		let controller = TenantAdmissionController::with_clock(
			TenantBudgetPolicy::new(budget, HashMap::new()),
			Some(admission_policy()),
			clock,
		);
		let held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let mut queued = expect_queued(controller.try_admit(TenantId(7), ByteCount::new(8)));

		queued.cancel_due_to_timeout();
		drop(held);

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited {
				tenant_id: TenantId(7),
			})
		));
	}

	#[test]
	fn unconfigured_tenants_share_other_rate_bucket() {
		let controller = TenantAdmissionController::new(
			TenantBudgetPolicy::new(rate_budget(8, 128, 8, 100, 2), HashMap::new()),
			None,
		);
		let _tenant_7 = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _tenant_8 = expect_granted(controller.try_admit(TenantId(8), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(9), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited {
				tenant_id: TenantId(9),
			})
		));
	}

	#[test]
	fn configured_tenant_rate_bucket_isolated_from_other() {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(TenantId(7), rate_budget(8, 128, 8, 100, 2));
		let controller = TenantAdmissionController::new(
			TenantBudgetPolicy::new(rate_budget(8, 128, 8, 100, 1), per_tenant),
			None,
		);
		let _other = expect_granted(controller.try_admit(TenantId(8), ByteCount::new(8)));
		assert!(matches!(
			controller.try_admit(TenantId(9), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited {
				tenant_id: TenantId(9),
			})
		));

		let _configured_a = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _configured_b = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RateLimited {
				tenant_id: TenantId(7),
			})
		));
	}

	#[test]
	fn queue_full_rejects_immediately() {
		let controller = TenantAdmissionController::new(
			queued_policy(1, ByteCount::ZERO, 0, 1),
			Some(admission_policy()),
		);
		let _held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _queued = expect_queued(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::QueueFull {
				tenant_id: TenantId(7),
			})
		));
		let slot = metric_slot(&controller, TenantId(7));
		assert_eq!(slot.queue_full_rejections, 1);
	}

	#[test]
	fn request_that_can_never_fit_owned_plus_borrow_rejects_immediately() {
		let controller = TenantAdmissionController::new(
			queued_policy(1, ByteCount::new(4), 1, 1),
			Some(admission_policy()),
		);
		let _held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(13)),
			AdmissionAttempt::Rejected(TenantAdmissionRejection::RequestTooLarge {
				tenant_id: TenantId(7),
			})
		));
	}

	#[test]
	fn queue_timeout_removes_waiter_and_returns_resource_exhausted() {
		let controller = TenantAdmissionController::new(
			queued_policy(2, ByteCount::ZERO, 0, 1),
			Some(AdmissionPolicy {
				max_queue_wait_ms: DurationMillis::new(1),
				..admission_policy()
			}),
		);
		let _held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let mut queued = expect_queued(controller.try_admit(TenantId(7), ByteCount::new(8)));

		queued.cancel_due_to_timeout();

		let slot = metric_slot(&controller, TenantId(7));
		assert_eq!(slot.queue_depth, 0);
		assert_eq!(slot.queue_timeouts, 1);
	}

	#[test]
	fn cancelled_queued_request_cannot_receive_a_stale_grant() {
		let controller = TenantAdmissionController::new(
			queued_policy(2, ByteCount::ZERO, 0, 1),
			Some(admission_policy()),
		);
		let held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let queued = expect_queued(controller.try_admit(TenantId(7), ByteCount::new(8)));

		drop(queued);
		drop(held);

		let slot = metric_slot(&controller, TenantId(7));
		assert_eq!(slot.queue_depth, 0);
		assert_eq!(controller.active_concurrency(TenantId(7)), 0);
		assert_eq!(controller.used_memory(TenantId(7)), ByteCount::ZERO);
	}

	#[tokio::test]
	async fn release_grants_next_waiter_without_leaking_owned_or_borrowed() {
		let controller = TenantAdmissionController::new(
			queued_policy(2, ByteCount::ZERO, 0, 1),
			Some(admission_policy()),
		);
		let held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let mut queued = expect_queued(controller.try_admit(TenantId(7), ByteCount::new(8)));

		drop(held);
		let granted = queued.recv().await.expect("queued grant");

		assert_eq!(controller.active_concurrency(TenantId(7)), 1);
		assert_eq!(controller.used_memory(TenantId(7)), ByteCount::new(8));
		drop(granted);
		assert_eq!(controller.active_concurrency(TenantId(7)), 0);
		assert_eq!(controller.used_memory(TenantId(7)), ByteCount::ZERO);
	}

	#[tokio::test]
	async fn same_tenant_fifo_holds_new_request_cannot_bypass_existing_waiter() {
		let controller = TenantAdmissionController::new(
			queued_policy(3, ByteCount::ZERO, 0, 1),
			Some(admission_policy()),
		);
		let held = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let mut first = expect_queued(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let mut second = expect_queued(controller.try_admit(TenantId(7), ByteCount::new(8)));

		drop(held);
		let first_grant = first.recv().await.expect("first waiter granted");
		assert!(
			tokio::time::timeout(std::time::Duration::from_millis(5), second.recv())
				.await
				.is_err(),
			"second waiter must not bypass first"
		);
		drop(first_grant);
		let second_grant = second.recv().await.expect("second waiter granted");
		drop(second_grant);
	}

	#[tokio::test]
	async fn weighted_tenants_receive_deterministic_integer_deficit_grant_order() {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(TenantId(7), admission_budget(8, 8, 1, 8, 1, 2));
		per_tenant.insert(TenantId(8), admission_budget(8, 8, 1, 8, 1, 1));
		let controller = TenantAdmissionController::new(
			TenantBudgetPolicy::new(
				TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
				per_tenant,
			),
			Some(admission_policy()),
		);
		let _tenant_7_owned = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let borrowed = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _tenant_8_owned = expect_granted(controller.try_admit(TenantId(8), ByteCount::new(8)));
		let mut tenant_7_waiter =
			expect_queued(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let mut tenant_8_waiter =
			expect_queued(controller.try_admit(TenantId(8), ByteCount::new(8)));

		drop(borrowed);
		let first = tenant_7_waiter.recv().await.expect("weighted first grant");
		assert_eq!(controller.active_concurrency(TenantId(7)), 1);
		drop(first);
		let second = tenant_8_waiter.recv().await.expect("weighted second grant");
		drop(second);
	}

	#[test]
	fn borrowed_memory_and_concurrency_never_exceed_shared_pool_caps() {
		let controller = TenantAdmissionController::new(
			queued_policy(1, ByteCount::new(8), 1, 1),
			Some(admission_policy()),
		);
		let _owned = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _borrowed = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let queued = controller.try_admit(TenantId(7), ByteCount::new(8));

		assert!(matches!(queued, AdmissionAttempt::Queued(_)));
		let slot = metric_slot(&controller, TenantId(7));
		assert_eq!(slot.borrowed_memory, ByteCount::new(8));
		assert_eq!(slot.borrowed_concurrency, 1);
	}

	#[test]
	fn dropping_borrowed_memory_reservation_preserves_owned_memory_accounting() {
		let controller = TenantAdmissionController::new(
			queued_policy(1, ByteCount::new(8), 1, 1),
			Some(admission_policy()),
		);
		let owned = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let borrowed = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert_eq!(controller.used_memory(TenantId(7)), ByteCount::new(8));
		let slot = metric_slot(&controller, TenantId(7));
		assert_eq!(slot.borrowed_memory, ByteCount::new(8));
		assert_eq!(slot.borrowed_concurrency, 1);

		drop(borrowed);

		assert_eq!(controller.used_memory(TenantId(7)), ByteCount::new(8));
		let slot = metric_slot(&controller, TenantId(7));
		assert_eq!(slot.borrowed_memory, ByteCount::ZERO);
		assert_eq!(slot.borrowed_concurrency, 0);

		drop(owned);

		assert_eq!(controller.used_memory(TenantId(7)), ByteCount::ZERO);
	}

	#[test]
	fn drained_unconfigured_queue_prunes_scheduler_deficit() {
		let controller = TenantAdmissionController::new(
			TenantBudgetPolicy::new(admission_budget(8, 8, 1, 0, 0, 1), HashMap::new()),
			Some(admission_policy()),
		);
		let blocked = expect_granted(controller.try_admit(TenantId(8), ByteCount::new(8)));
		let queued = expect_queued(controller.try_admit(TenantId(8), ByteCount::new(8)));
		let unrelated = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		drop(unrelated);
		{
			let state = controller
				.inner
				.state
				.lock()
				.expect("tenant admission controller poisoned");
			assert!(state.queues.contains_key(&TenantId(8)));
			assert!(state.deficits.contains_key(&TenantId(8)));
		}

		drop(queued);
		{
			let state = controller
				.inner
				.state
				.lock()
				.expect("tenant admission controller poisoned");
			assert!(!state.queues.contains_key(&TenantId(8)));
			assert!(!state.deficits.contains_key(&TenantId(8)));
		}

		drop(blocked);
	}

	#[test]
	fn per_tenant_borrow_never_exceeds_tenant_borrow_caps() {
		let controller = TenantAdmissionController::new(
			queued_policy(2, ByteCount::new(8), 1, 1),
			Some(AdmissionPolicy {
				shared_memory: ByteCount::new(64),
				shared_concurrent_queries: 4,
				..admission_policy()
			}),
		);
		let _owned = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _borrowed = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Queued(_)
		));
		let slot = metric_slot(&controller, TenantId(7));
		assert_eq!(slot.borrowed_memory, ByteCount::new(8));
		assert_eq!(slot.borrowed_concurrency, 1);
	}

	#[test]
	fn tenant_a_cannot_consume_tenant_b_owned_budget() {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(TenantId(7), admission_budget(8, 8, 1, 0, 0, 1));
		per_tenant.insert(TenantId(8), admission_budget(8, 8, 1, 0, 0, 1));
		let controller = TenantAdmissionController::new(
			TenantBudgetPolicy::new(
				TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
				per_tenant,
			),
			Some(admission_policy()),
		);
		let _tenant_7 = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(7), ByteCount::new(8)),
			AdmissionAttempt::Queued(_)
		));
		assert_eq!(controller.used_memory(TenantId(8)), ByteCount::ZERO);
	}

	#[test]
	fn borrowed_in_flight_query_not_preempted_when_other_tenant_arrives() {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(TenantId(7), admission_budget(8, 8, 1, 8, 1, 1));
		per_tenant.insert(TenantId(8), admission_budget(8, 8, 1, 8, 1, 1));
		let controller = TenantAdmissionController::new(
			TenantBudgetPolicy::new(
				TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
				per_tenant,
			),
			Some(admission_policy()),
		);
		let _tenant_7_owned = expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _tenant_7_borrowed =
			expect_granted(controller.try_admit(TenantId(7), ByteCount::new(8)));
		let _tenant_8_owned = expect_granted(controller.try_admit(TenantId(8), ByteCount::new(8)));

		assert!(matches!(
			controller.try_admit(TenantId(8), ByteCount::new(8)),
			AdmissionAttempt::Queued(_)
		));
		let slot = metric_slot(&controller, TenantId(7));
		assert_eq!(slot.borrowed_memory, ByteCount::new(8));
		assert_eq!(slot.borrowed_concurrency, 1);
		assert_eq!(controller.active_concurrency(TenantId(7)), 1);
	}

	fn admission_policy() -> AdmissionPolicy {
		AdmissionPolicy {
			max_queued_queries: 8,
			max_queue_wait_ms: DurationMillis::new(100),
			shared_memory: ByteCount::new(8),
			shared_concurrent_queries: 1,
		}
	}

	fn single_tenant_policy() -> TenantBudgetPolicy {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(
			TenantId(7),
			TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
		);
		TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
			per_tenant,
		)
	}

	fn queued_policy(
		max_queued_queries: u32,
		max_borrowed_memory: ByteCount,
		max_borrowed_concurrent_queries: u32,
		admission_weight: u32,
	) -> TenantBudgetPolicy {
		let mut per_tenant = HashMap::new();
		per_tenant.insert(
			TenantId(7),
			admission_budget(
				8,
				8,
				max_queued_queries,
				max_borrowed_memory.get(),
				max_borrowed_concurrent_queries,
				admission_weight,
			),
		);
		TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
			per_tenant,
		)
	}

	fn admission_budget(
		memory_per_task: u64,
		aggregate_memory: u64,
		max_queued_queries: u32,
		max_borrowed_memory: u64,
		max_borrowed_concurrent_queries: u32,
		admission_weight: u32,
	) -> TenantBudget {
		TenantBudget::with_admission_limits(
			ByteCount::new(memory_per_task),
			ByteCount::new(aggregate_memory),
			NonZeroU32::new(1).expect("nonzero"),
			NonZeroU32::new(1).expect("nonzero"),
			None,
			NonZeroU32::new(admission_weight).expect("nonzero"),
			max_queued_queries,
			ByteCount::new(max_borrowed_memory),
			max_borrowed_concurrent_queries,
			0,
			0,
		)
	}

	fn rate_budget(
		memory_per_task: u64,
		aggregate_memory: u64,
		max_concurrent_queries: u32,
		max_queries_per_second: u32,
		query_burst_capacity: u32,
	) -> TenantBudget {
		let mut budget = TenantBudget::with_max_concurrent_queries(
			ByteCount::new(memory_per_task),
			ByteCount::new(aggregate_memory),
			NonZeroU32::new(max_concurrent_queries).expect("nonzero"),
		);
		budget.max_queries_per_second = max_queries_per_second;
		budget.query_burst_capacity = query_burst_capacity;
		budget
	}

	fn expect_granted(attempt: AdmissionAttempt) -> AdmissionReservation {
		match attempt {
			AdmissionAttempt::Granted(reservation) => reservation,
			other => panic!("expected admission grant, got {other:?}"),
		}
	}

	fn expect_queued(attempt: AdmissionAttempt) -> QueuedAdmission {
		match attempt {
			AdmissionAttempt::Queued(queued) => queued,
			other => panic!("expected queued admission, got {other:?}"),
		}
	}

	fn metric_slot(
		controller: &TenantAdmissionController,
		tenant_id: TenantId,
	) -> TenantAdmissionMetricSlot {
		controller
			.metric_slots()
			.into_iter()
			.find(|slot| slot.tenant_id == Some(tenant_id))
			.expect("metric slot")
	}
}
