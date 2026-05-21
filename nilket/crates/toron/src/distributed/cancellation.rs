use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::distributed::identity::{QueryId, TaskIdentity};
use crate::distributed::worker_registry::WorkerEntry;
use crate::storage::TenantId;
use tokio::sync::Notify;

const MAX_PENDING_TASK_CANCELLATIONS: usize = 1024;
const PENDING_TASK_CANCELLATION_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancellationTrigger {
	StatementTimeout,
	ClientDisconnect,
	SqlCancel,
}

impl CancellationTrigger {
	pub const ALL: [Self; 3] = [
		Self::StatementTimeout,
		Self::ClientDisconnect,
		Self::SqlCancel,
	];

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::StatementTimeout => "statement_timeout",
			Self::ClientDisconnect => "client_disconnect",
			Self::SqlCancel => "sql_cancel",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancellationOutcome {
	Requested,
	AlreadyTerminal,
	RemoteAck,
	RemoteTimeout,
	RemoteError,
}

impl CancellationOutcome {
	pub const ALL: [Self; 5] = [
		Self::Requested,
		Self::AlreadyTerminal,
		Self::RemoteAck,
		Self::RemoteTimeout,
		Self::RemoteError,
	];

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Requested => "requested",
			Self::AlreadyTerminal => "already_terminal",
			Self::RemoteAck => "remote_ack",
			Self::RemoteTimeout => "remote_timeout",
			Self::RemoteError => "remote_error",
		}
	}
}

#[derive(Debug, Default)]
pub struct CancellationMetrics {
	statement_timeout: CancellationOutcomeCounters,
	client_disconnect: CancellationOutcomeCounters,
	sql_cancel: CancellationOutcomeCounters,
}

impl CancellationMetrics {
	pub fn record(&self, trigger: CancellationTrigger, outcome: CancellationOutcome) {
		self.counters(trigger).increment(outcome);
	}

	pub fn get(&self, trigger: CancellationTrigger, outcome: CancellationOutcome) -> u64 {
		self.counters(trigger).get(outcome)
	}

	fn counters(&self, trigger: CancellationTrigger) -> &CancellationOutcomeCounters {
		match trigger {
			CancellationTrigger::StatementTimeout => &self.statement_timeout,
			CancellationTrigger::ClientDisconnect => &self.client_disconnect,
			CancellationTrigger::SqlCancel => &self.sql_cancel,
		}
	}
}

#[derive(Debug, Default)]
struct CancellationOutcomeCounters {
	requested: AtomicU64,
	already_terminal: AtomicU64,
	remote_ack: AtomicU64,
	remote_timeout: AtomicU64,
	remote_error: AtomicU64,
}

impl CancellationOutcomeCounters {
	fn increment(&self, outcome: CancellationOutcome) {
		self.counter(outcome).fetch_add(1, Ordering::Relaxed);
	}

	fn get(&self, outcome: CancellationOutcome) -> u64 {
		self.counter(outcome).load(Ordering::Relaxed)
	}

	fn counter(&self, outcome: CancellationOutcome) -> &AtomicU64 {
		match outcome {
			CancellationOutcome::Requested => &self.requested,
			CancellationOutcome::AlreadyTerminal => &self.already_terminal,
			CancellationOutcome::RemoteAck => &self.remote_ack,
			CancellationOutcome::RemoteTimeout => &self.remote_timeout,
			CancellationOutcome::RemoteError => &self.remote_error,
		}
	}
}

#[derive(Debug, Clone)]
pub struct TaskCancellationSignal {
	inner: Arc<CancellationSignalInner>,
}

#[derive(Debug)]
struct CancellationSignalInner {
	cancelled: AtomicBool,
	notify: Notify,
}

impl TaskCancellationSignal {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(CancellationSignalInner {
				cancelled: AtomicBool::new(false),
				notify: Notify::new(),
			}),
		}
	}

	pub fn cancel(&self) {
		if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
			self.inner.notify.notify_waiters();
		}
	}

	pub fn is_cancelled(&self) -> bool {
		self.inner.cancelled.load(Ordering::SeqCst)
	}

	pub async fn cancelled(&self) {
		loop {
			let notified = self.inner.notify.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			if self.is_cancelled() {
				return;
			}
			notified.await;
		}
	}
}

impl Default for TaskCancellationSignal {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Debug, Clone)]
pub struct ActiveTaskRegistry {
	inner: Arc<Mutex<ActiveTaskRegistryInner>>,
}

#[derive(Debug, Default)]
struct ActiveTaskRegistryInner {
	active: HashMap<TaskIdentity, TaskCancellationSignal>,
	pending: HashMap<TaskIdentity, Instant>,
	pending_order: VecDeque<TaskIdentity>,
}

#[derive(Debug)]
pub struct ActiveTaskRegistration {
	pub signal: TaskCancellationSignal,
	pub guard: ActiveTaskGuard,
	pub cancelled_before_start: bool,
}

#[derive(Debug)]
pub struct ActiveTaskGuard {
	registry: Weak<Mutex<ActiveTaskRegistryInner>>,
	identity: TaskIdentity,
}

impl ActiveTaskRegistry {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(Mutex::new(ActiveTaskRegistryInner::default())),
		}
	}

	pub fn register(&self, identity: TaskIdentity) -> ActiveTaskRegistration {
		let signal = TaskCancellationSignal::new();
		let mut inner = self.inner.lock().expect("active task registry poisoned");
		inner.prune_pending(Instant::now());
		let cancelled_before_start = inner.pending.remove(&identity).is_some();
		if cancelled_before_start {
			signal.cancel();
		}
		inner.active.insert(identity, signal.clone());
		ActiveTaskRegistration {
			signal,
			guard: ActiveTaskGuard {
				registry: Arc::downgrade(&self.inner),
				identity,
			},
			cancelled_before_start,
		}
	}

	pub fn cancel(&self, identity: TaskIdentity) -> bool {
		let mut inner = self.inner.lock().expect("active task registry poisoned");
		inner.prune_pending(Instant::now());
		if let Some(signal) = inner.active.get(&identity) {
			signal.cancel();
			return true;
		}
		inner.insert_pending(identity);
		false
	}

	pub fn active_len(&self) -> usize {
		self.inner
			.lock()
			.expect("active task registry poisoned")
			.active
			.len()
	}

	pub fn pending_len(&self) -> usize {
		self.inner
			.lock()
			.expect("active task registry poisoned")
			.pending
			.len()
	}
}

impl Default for ActiveTaskRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl ActiveTaskRegistryInner {
	fn insert_pending(&mut self, identity: TaskIdentity) {
		let now = Instant::now();
		if self.pending.insert(identity, now).is_none() {
			self.pending_order.push_back(identity);
		}
		while self.pending.len() > MAX_PENDING_TASK_CANCELLATIONS {
			let Some(stale) = self.pending_order.pop_front() else {
				break;
			};
			self.pending.remove(&stale);
		}
	}

	fn prune_pending(&mut self, now: Instant) {
		while let Some(identity) = self.pending_order.front().copied() {
			let Some(inserted) = self.pending.get(&identity).copied() else {
				self.pending_order.pop_front();
				continue;
			};
			if now.duration_since(inserted) < PENDING_TASK_CANCELLATION_TTL {
				break;
			}
			self.pending_order.pop_front();
			self.pending.remove(&identity);
		}
	}
}

impl Drop for ActiveTaskGuard {
	fn drop(&mut self) {
		let Some(registry) = self.registry.upgrade() else {
			return;
		};
		if let Ok(mut inner) = registry.lock() {
			inner.active.remove(&self.identity);
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActiveQueryKey {
	pub tenant_id: TenantId,
	pub query_id: QueryId,
}

impl ActiveQueryKey {
	pub const fn new(tenant_id: TenantId, query_id: QueryId) -> Self {
		Self {
			tenant_id,
			query_id,
		}
	}
}

#[derive(Debug, Clone)]
pub struct RemoteTaskCancellation {
	pub identity: TaskIdentity,
	pub worker: WorkerEntry,
}

#[derive(Debug, Clone)]
pub struct ActiveQueryHandle {
	inner: Arc<ActiveQueryState>,
}

#[derive(Debug)]
struct ActiveQueryState {
	key: ActiveQueryKey,
	cancelled: AtomicBool,
	notify: Notify,
	remote_tasks: Mutex<Vec<RemoteTaskCancellation>>,
}

#[derive(Debug, Clone)]
pub struct ActiveQueryRegistry {
	inner: Arc<Mutex<HashMap<ActiveQueryKey, ActiveQueryHandle>>>,
}

#[derive(Debug)]
pub struct ActiveQueryGuard {
	registry: ActiveQueryRegistry,
	key: ActiveQueryKey,
	handle: ActiveQueryHandle,
}

#[derive(Debug)]
pub struct QueryCancellation {
	pub was_active: bool,
	pub first_request: bool,
	pub remote_tasks: Vec<RemoteTaskCancellation>,
}

impl ActiveQueryRegistry {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub fn register(&self, tenant_id: TenantId, query_id: QueryId) -> ActiveQueryGuard {
		let key = ActiveQueryKey::new(tenant_id, query_id);
		let handle = ActiveQueryHandle::new(key);
		let mut inner = self.inner.lock().expect("active query registry poisoned");
		inner.insert(key, handle.clone());
		ActiveQueryGuard {
			registry: self.clone(),
			key,
			handle,
		}
	}

	pub fn cancel(
		&self,
		tenant_id: TenantId,
		query_id: QueryId,
		_trigger: CancellationTrigger,
	) -> QueryCancellation {
		let key = ActiveQueryKey::new(tenant_id, query_id);
		let handle = self
			.inner
			.lock()
			.expect("active query registry poisoned")
			.get(&key)
			.cloned();
		let Some(handle) = handle else {
			return QueryCancellation {
				was_active: false,
				first_request: false,
				remote_tasks: Vec::new(),
			};
		};
		let (first_request, remote_tasks) = handle.cancel();
		QueryCancellation {
			was_active: true,
			first_request,
			remote_tasks,
		}
	}

	pub fn active_len(&self) -> usize {
		self.inner
			.lock()
			.expect("active query registry poisoned")
			.len()
	}

	fn remove_if_same(&self, key: ActiveQueryKey, handle: &ActiveQueryHandle) {
		let mut inner = self.inner.lock().expect("active query registry poisoned");
		if inner
			.get(&key)
			.is_some_and(|current| current.ptr_eq(handle))
		{
			inner.remove(&key);
		}
	}
}

impl Default for ActiveQueryRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl ActiveQueryGuard {
	pub fn handle(&self) -> ActiveQueryHandle {
		self.handle.clone()
	}
}

impl Drop for ActiveQueryGuard {
	fn drop(&mut self) {
		self.registry.remove_if_same(self.key, &self.handle);
	}
}

impl ActiveQueryHandle {
	fn new(key: ActiveQueryKey) -> Self {
		Self {
			inner: Arc::new(ActiveQueryState {
				key,
				cancelled: AtomicBool::new(false),
				notify: Notify::new(),
				remote_tasks: Mutex::new(Vec::new()),
			}),
		}
	}

	pub fn key(&self) -> ActiveQueryKey {
		self.inner.key
	}

	pub fn is_cancelled(&self) -> bool {
		self.inner.cancelled.load(Ordering::SeqCst)
	}

	pub async fn cancelled(&self) {
		loop {
			let notified = self.inner.notify.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			if self.is_cancelled() {
				return;
			}
			notified.await;
		}
	}

	pub fn cancel(&self) -> (bool, Vec<RemoteTaskCancellation>) {
		let first_request = !self.inner.cancelled.swap(true, Ordering::SeqCst);
		if first_request {
			self.inner.notify.notify_waiters();
		}
		let remote_tasks = self
			.inner
			.remote_tasks
			.lock()
			.expect("active query tasks poisoned")
			.clone();
		(first_request, remote_tasks)
	}

	pub fn record_remote_task(&self, worker: WorkerEntry, identity: TaskIdentity) {
		self.inner
			.remote_tasks
			.lock()
			.expect("active query tasks poisoned")
			.push(RemoteTaskCancellation { identity, worker });
	}

	fn ptr_eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.inner, &other.inner)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::identity::{AttemptId, StageId, TaskId};

	#[test]
	fn active_task_cancel_before_registration_is_bounded_and_consumed() {
		let registry = ActiveTaskRegistry::new();
		let first_identity = identity(0);

		for index in 0..(MAX_PENDING_TASK_CANCELLATIONS + 5) {
			registry.cancel(identity(index as u128));
		}

		assert_eq!(registry.pending_len(), MAX_PENDING_TASK_CANCELLATIONS);
		let first = registry.register(first_identity);
		assert!(!first.cancelled_before_start);

		let newest_identity = identity((MAX_PENDING_TASK_CANCELLATIONS + 4) as u128);
		let newest = registry.register(newest_identity);
		assert!(newest.cancelled_before_start);
		assert!(newest.signal.is_cancelled());
		assert_eq!(registry.pending_len(), MAX_PENDING_TASK_CANCELLATIONS - 1);
	}

	#[test]
	fn active_task_cancel_signals_and_raii_deregisters() {
		let registry = ActiveTaskRegistry::new();
		let registration = registry.register(identity(7));

		assert_eq!(registry.active_len(), 1);
		assert!(registry.cancel(identity(7)));
		assert!(registration.signal.is_cancelled());
		drop(registration.guard);
		assert_eq!(registry.active_len(), 0);
	}

	#[tokio::test]
	async fn task_cancellation_signal_observes_cancel_before_wait() {
		let signal = TaskCancellationSignal::new();
		signal.cancel();

		tokio::time::timeout(Duration::from_millis(50), signal.cancelled())
			.await
			.expect("pre-cancelled signal must not park");
	}

	#[test]
	fn active_query_registry_is_tenant_scoped_idempotent_and_raii_removed() {
		let registry = ActiveQueryRegistry::new();
		let guard = registry.register(TenantId(7), QueryId::from_u128(9));
		let handle = guard.handle();

		assert_eq!(registry.active_len(), 1);
		assert!(
			!registry
				.cancel(
					TenantId(8),
					QueryId::from_u128(9),
					CancellationTrigger::SqlCancel
				)
				.was_active
		);
		let first = registry.cancel(
			TenantId(7),
			QueryId::from_u128(9),
			CancellationTrigger::SqlCancel,
		);
		assert!(first.was_active);
		assert!(first.first_request);
		assert!(handle.is_cancelled());
		let second = registry.cancel(
			TenantId(7),
			QueryId::from_u128(9),
			CancellationTrigger::SqlCancel,
		);
		assert!(second.was_active);
		assert!(!second.first_request);
		drop(guard);
		assert_eq!(registry.active_len(), 0);
	}

	#[tokio::test]
	async fn active_query_handle_observes_cancel_before_wait() {
		let registry = ActiveQueryRegistry::new();
		let guard = registry.register(TenantId(7), QueryId::from_u128(10));
		let handle = guard.handle();
		handle.cancel();

		tokio::time::timeout(Duration::from_millis(50), handle.cancelled())
			.await
			.expect("pre-cancelled query handle must not park");
	}

	fn identity(value: u128) -> TaskIdentity {
		TaskIdentity::new(
			QueryId::from_u128(1),
			StageId::new(1),
			TaskId::new(u32::try_from(value).expect("test task id fits u32")),
			AttemptId::first(),
		)
	}
}
