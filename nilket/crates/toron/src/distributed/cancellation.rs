use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::distributed::identity::{QueryId, TaskIdentity};
use crate::distributed::worker_registry::WorkerEntry;
use crate::storage::TenantId;
use tokio::sync::{Notify, Semaphore};
use wendelon::schema::{
	NodeId, TableQueryCancelResponseMeta, TableQueryCancelStatus, TableQueryRequestId,
	TenantId as WendelonTenantId,
};

const MAX_PENDING_TASK_CANCELLATIONS: usize = 1024;
const PENDING_TASK_CANCELLATION_TTL: Duration = Duration::from_secs(60);
pub(crate) const DOWNSTREAM_CANCEL_ACK_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_DOWNSTREAM_CANCEL_FANOUTS: usize = 64;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DownstreamCancellationTrigger {
	StatementTimeout,
	ClientDisconnect,
	SqlCancel,
	WorkerCancelTask,
}

impl DownstreamCancellationTrigger {
	pub const ALL: [Self; 4] = [
		Self::StatementTimeout,
		Self::ClientDisconnect,
		Self::SqlCancel,
		Self::WorkerCancelTask,
	];

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::StatementTimeout => "statement_timeout",
			Self::ClientDisconnect => "client_disconnect",
			Self::SqlCancel => "sql_cancel",
			Self::WorkerCancelTask => "worker_cancel_task",
		}
	}
}

impl From<CancellationTrigger> for DownstreamCancellationTrigger {
	fn from(trigger: CancellationTrigger) -> Self {
		match trigger {
			CancellationTrigger::StatementTimeout => Self::StatementTimeout,
			CancellationTrigger::ClientDisconnect => Self::ClientDisconnect,
			CancellationTrigger::SqlCancel => Self::SqlCancel,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DownstreamCancellationOutcome {
	Accepted,
	AlreadyTerminal,
	RemoteTimeout,
	RemoteError,
}

impl DownstreamCancellationOutcome {
	pub const ALL: [Self; 4] = [
		Self::Accepted,
		Self::AlreadyTerminal,
		Self::RemoteTimeout,
		Self::RemoteError,
	];

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Accepted => "accepted",
			Self::AlreadyTerminal => "already_terminal",
			Self::RemoteTimeout => "remote_timeout",
			Self::RemoteError => "remote_error",
		}
	}
}

#[derive(Debug, Default)]
pub struct DownstreamCancellationMetrics {
	statement_timeout: DownstreamCancellationOutcomeCounters,
	client_disconnect: DownstreamCancellationOutcomeCounters,
	sql_cancel: DownstreamCancellationOutcomeCounters,
	worker_cancel_task: DownstreamCancellationOutcomeCounters,
}

impl DownstreamCancellationMetrics {
	pub fn record(
		&self,
		trigger: DownstreamCancellationTrigger,
		outcome: DownstreamCancellationOutcome,
	) {
		self.counters(trigger).increment(outcome);
	}

	pub fn get(
		&self,
		trigger: DownstreamCancellationTrigger,
		outcome: DownstreamCancellationOutcome,
	) -> u64 {
		self.counters(trigger).get(outcome)
	}

	fn counters(
		&self,
		trigger: DownstreamCancellationTrigger,
	) -> &DownstreamCancellationOutcomeCounters {
		match trigger {
			DownstreamCancellationTrigger::StatementTimeout => &self.statement_timeout,
			DownstreamCancellationTrigger::ClientDisconnect => &self.client_disconnect,
			DownstreamCancellationTrigger::SqlCancel => &self.sql_cancel,
			DownstreamCancellationTrigger::WorkerCancelTask => &self.worker_cancel_task,
		}
	}
}

#[derive(Debug, Default)]
struct DownstreamCancellationOutcomeCounters {
	accepted: AtomicU64,
	already_terminal: AtomicU64,
	remote_timeout: AtomicU64,
	remote_error: AtomicU64,
}

impl DownstreamCancellationOutcomeCounters {
	fn increment(&self, outcome: DownstreamCancellationOutcome) {
		self.counter(outcome).fetch_add(1, Ordering::Relaxed);
	}

	fn get(&self, outcome: DownstreamCancellationOutcome) -> u64 {
		self.counter(outcome).load(Ordering::Relaxed)
	}

	fn counter(&self, outcome: DownstreamCancellationOutcome) -> &AtomicU64 {
		match outcome {
			DownstreamCancellationOutcome::Accepted => &self.accepted,
			DownstreamCancellationOutcome::AlreadyTerminal => &self.already_terminal,
			DownstreamCancellationOutcome::RemoteTimeout => &self.remote_timeout,
			DownstreamCancellationOutcome::RemoteError => &self.remote_error,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WendelonTableQueryAttempt {
	pub target: NodeId,
	pub tenant_id: WendelonTenantId,
	pub request_id: TableQueryRequestId,
}

#[async_trait::async_trait]
pub trait WendelonTableQueryCancelSender: Send + Sync {
	async fn cancel_table_query_attempt(
		&self,
		target: NodeId,
		tenant_id: WendelonTenantId,
		request_id: TableQueryRequestId,
	) -> Result<TableQueryCancelResponseMeta, wendelon_client::ClientError>;
}

#[async_trait::async_trait]
impl WendelonTableQueryCancelSender for wendelon_client::Client {
	async fn cancel_table_query_attempt(
		&self,
		target: NodeId,
		tenant_id: WendelonTenantId,
		request_id: TableQueryRequestId,
	) -> Result<TableQueryCancelResponseMeta, wendelon_client::ClientError> {
		self.cancel_table_query_attempt(target, tenant_id, request_id)
			.await
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownstreamRegistrationError {
	AlreadyCancelled,
}

impl fmt::Display for DownstreamRegistrationError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::AlreadyCancelled => {
				f.write_str("downstream table-query attempt already cancelled")
			}
		}
	}
}

impl std::error::Error for DownstreamRegistrationError {}

#[derive(Clone)]
pub struct DownstreamCancellationContext {
	registry: DownstreamAttemptRegistry,
}

impl DownstreamCancellationContext {
	pub fn is_cancelled(&self) -> bool {
		self.registry.is_cancelled()
	}

	pub async fn cancelled(&self) {
		self.registry.cancelled().await;
	}

	pub fn register_wendelon_table_query_attempt(
		&self,
		sender: Arc<dyn WendelonTableQueryCancelSender>,
		attempt: WendelonTableQueryAttempt,
	) -> Result<DownstreamAttemptGuard, DownstreamRegistrationError> {
		self.registry.register_table_query_attempt(sender, attempt)
	}

	pub fn cancel_registered_attempts(&self, trigger: DownstreamCancellationTrigger) {
		self.registry.cancel_registered_attempts(trigger);
	}
}

impl fmt::Debug for DownstreamCancellationContext {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("DownstreamCancellationContext")
			.field("cancelled", &self.is_cancelled())
			.finish_non_exhaustive()
	}
}

#[derive(Clone)]
pub struct DownstreamAttemptRegistry {
	inner: Arc<Mutex<DownstreamAttemptRegistryInner>>,
	fanout_permits: Arc<Semaphore>,
	metrics: Option<Arc<DownstreamCancellationMetrics>>,
	notify: Arc<Notify>,
}

#[derive(Default)]
struct DownstreamAttemptRegistryInner {
	cancelled: bool,
	next_registration_id: u64,
	attempts: HashMap<u64, RegisteredDownstreamAttempt>,
}

#[derive(Clone)]
struct RegisteredDownstreamAttempt {
	sender: Arc<dyn WendelonTableQueryCancelSender>,
	attempt: WendelonTableQueryAttempt,
}

pub struct DownstreamAttemptGuard {
	registry: Weak<Mutex<DownstreamAttemptRegistryInner>>,
	registration_id: u64,
}

impl DownstreamAttemptRegistry {
	pub fn new(
		fanout_permits: Arc<Semaphore>,
		metrics: Option<Arc<DownstreamCancellationMetrics>>,
	) -> Self {
		Self {
			inner: Arc::new(Mutex::new(DownstreamAttemptRegistryInner::default())),
			fanout_permits,
			metrics,
			notify: Arc::new(Notify::new()),
		}
	}

	pub fn with_default_limits(metrics: Option<Arc<DownstreamCancellationMetrics>>) -> Self {
		Self::new(
			Arc::new(Semaphore::new(MAX_DOWNSTREAM_CANCEL_FANOUTS)),
			metrics,
		)
	}

	pub fn context(&self) -> DownstreamCancellationContext {
		DownstreamCancellationContext {
			registry: self.clone(),
		}
	}

	pub fn is_cancelled(&self) -> bool {
		self.inner
			.lock()
			.expect("downstream attempt registry poisoned")
			.cancelled
	}

	pub async fn cancelled(&self) {
		loop {
			let notified = self.notify.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			if self.is_cancelled() {
				return;
			}
			notified.await;
		}
	}

	pub fn active_len(&self) -> usize {
		self.inner
			.lock()
			.expect("downstream attempt registry poisoned")
			.attempts
			.len()
	}

	pub fn register_table_query_attempt(
		&self,
		sender: Arc<dyn WendelonTableQueryCancelSender>,
		attempt: WendelonTableQueryAttempt,
	) -> Result<DownstreamAttemptGuard, DownstreamRegistrationError> {
		let mut inner = self
			.inner
			.lock()
			.expect("downstream attempt registry poisoned");
		if inner.cancelled {
			return Err(DownstreamRegistrationError::AlreadyCancelled);
		}
		let registration_id = inner.next_registration_id;
		inner.next_registration_id = inner.next_registration_id.wrapping_add(1);
		inner.attempts.insert(
			registration_id,
			RegisteredDownstreamAttempt { sender, attempt },
		);
		Ok(DownstreamAttemptGuard {
			registry: Arc::downgrade(&self.inner),
			registration_id,
		})
	}

	pub fn cancel_registered_attempts(&self, trigger: DownstreamCancellationTrigger) {
		let attempts = {
			let mut inner = self
				.inner
				.lock()
				.expect("downstream attempt registry poisoned");
			if inner.cancelled {
				return;
			}
			inner.cancelled = true;
			inner.attempts.values().cloned().collect::<Vec<_>>()
		};
		self.notify.notify_waiters();
		if attempts.is_empty() {
			return;
		}
		let Ok(permit) = self.fanout_permits.clone().try_acquire_owned() else {
			if let Some(metrics) = &self.metrics {
				for _ in &attempts {
					metrics.record(trigger, DownstreamCancellationOutcome::RemoteError);
				}
			}
			tracing::warn!("downstream Wendelon table-query cancel fan-out limit exhausted");
			return;
		};
		let metrics = self.metrics.clone();
		tokio::spawn(async move {
			let _permit = permit;
			for registered in attempts {
				let result = tokio::time::timeout(
					DOWNSTREAM_CANCEL_ACK_TIMEOUT,
					registered.sender.cancel_table_query_attempt(
						registered.attempt.target,
						registered.attempt.tenant_id,
						registered.attempt.request_id,
					),
				)
				.await;
				let outcome = match result {
					Ok(Ok(response)) if response.status == TableQueryCancelStatus::ACCEPTED => {
						DownstreamCancellationOutcome::Accepted
					}
					Ok(Ok(response)) if response.status == TableQueryCancelStatus::TERMINAL => {
						DownstreamCancellationOutcome::AlreadyTerminal
					}
					Ok(Ok(response)) => {
						tracing::warn!(
							status = response.status.0,
							"downstream Wendelon table-query cancel returned unknown status"
						);
						DownstreamCancellationOutcome::RemoteError
					}
					Ok(Err(error)) => {
						tracing::warn!(
							error = %error,
							"downstream Wendelon table-query cancel failed"
						);
						DownstreamCancellationOutcome::RemoteError
					}
					Err(_) => {
						tracing::warn!("downstream Wendelon table-query cancel timed out");
						DownstreamCancellationOutcome::RemoteTimeout
					}
				};
				if let Some(metrics) = &metrics {
					metrics.record(trigger, outcome);
				}
			}
		});
	}
}

impl fmt::Debug for DownstreamAttemptRegistry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("DownstreamAttemptRegistry")
			.field("cancelled", &self.is_cancelled())
			.field("active_len", &self.active_len())
			.finish_non_exhaustive()
	}
}

impl fmt::Debug for DownstreamAttemptGuard {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("DownstreamAttemptGuard")
			.field("registration_id", &self.registration_id)
			.finish_non_exhaustive()
	}
}

impl Drop for DownstreamAttemptGuard {
	fn drop(&mut self) {
		let Some(registry) = self.registry.upgrade() else {
			return;
		};
		if let Ok(mut inner) = registry.lock() {
			inner.attempts.remove(&self.registration_id);
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
	downstream_fanout_permits: Arc<Semaphore>,
	downstream_metrics: Option<Arc<DownstreamCancellationMetrics>>,
}

#[derive(Debug, Default)]
struct ActiveTaskRegistryInner {
	active: HashMap<TaskIdentity, ActiveTaskState>,
	pending: HashMap<TaskIdentity, Instant>,
	pending_order: VecDeque<TaskIdentity>,
}

#[derive(Debug, Clone)]
struct ActiveTaskState {
	signal: TaskCancellationSignal,
	downstream: DownstreamAttemptRegistry,
}

#[derive(Debug)]
pub struct ActiveTaskRegistration {
	pub signal: TaskCancellationSignal,
	pub downstream: DownstreamAttemptRegistry,
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
			downstream_fanout_permits: Arc::new(Semaphore::new(MAX_DOWNSTREAM_CANCEL_FANOUTS)),
			downstream_metrics: None,
		}
	}

	pub fn with_downstream_cancellation_metrics(
		metrics: Option<Arc<DownstreamCancellationMetrics>>,
	) -> Self {
		Self {
			inner: Arc::new(Mutex::new(ActiveTaskRegistryInner::default())),
			downstream_fanout_permits: Arc::new(Semaphore::new(MAX_DOWNSTREAM_CANCEL_FANOUTS)),
			downstream_metrics: metrics,
		}
	}

	pub fn register(&self, identity: TaskIdentity) -> ActiveTaskRegistration {
		let signal = TaskCancellationSignal::new();
		let downstream = DownstreamAttemptRegistry::new(
			self.downstream_fanout_permits.clone(),
			self.downstream_metrics.clone(),
		);
		let mut inner = self.inner.lock().expect("active task registry poisoned");
		inner.prune_pending(Instant::now());
		let cancelled_before_start = inner.pending.remove(&identity).is_some();
		if cancelled_before_start {
			signal.cancel();
		}
		inner.active.insert(
			identity,
			ActiveTaskState {
				signal: signal.clone(),
				downstream: downstream.clone(),
			},
		);
		ActiveTaskRegistration {
			signal,
			downstream,
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
		if let Some(state) = inner.active.get(&identity) {
			state
				.downstream
				.cancel_registered_attempts(DownstreamCancellationTrigger::WorkerCancelTask);
			state.signal.cancel();
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
	/// ADR 628 B1.C: the coordinator's per-worker lease reliance for this query.
	reliance: Arc<crate::distributed::query_reliance::QueryReliance>,
	cancelled: AtomicBool,
	terminal: AtomicBool,
	notify: Notify,
	lease_failure_worker: Mutex<Option<crate::distributed::identity::WorkerId>>,
	remote_tasks: Mutex<Vec<RemoteTaskCancellation>>,
	downstream: DownstreamAttemptRegistry,
}

#[derive(Debug, Clone)]
pub struct ActiveQueryRegistry {
	inner: Arc<Mutex<HashMap<ActiveQueryKey, ActiveQueryHandle>>>,
	reliance_params: crate::distributed::query_reliance::RelianceParams,
	reliance_clock: Arc<dyn crate::distributed::query_reliance::RelianceClock>,
	reliance_observer: ObserverSlot,
}

/// ADR 628 B1.C (instrument seam S2): the observer installed on every query registered from now on.
#[derive(Clone, Default)]
struct ObserverSlot(Option<Arc<dyn crate::distributed::query_reliance::RelianceObserver>>);

impl std::fmt::Debug for ObserverSlot {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(if self.0.is_some() {
			"ObserverSlot(installed)"
		} else {
			"ObserverSlot(none)"
		})
	}
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
		let term = crate::distributed::namespace_lifecycle::ShuffleLifecycleConfig::from_worker_loss_window(
			crate::distributed::partition_store::DEFAULT_WORKER_LOST_AFTER,
		)
		.expect("default shuffle lifecycle config")
		.lease_term();
		Self {
			inner: Arc::new(Mutex::new(HashMap::new())),
			reliance_params: crate::distributed::query_reliance::RelianceParams::production(term),
			reliance_clock: Arc::new(crate::distributed::query_reliance::BootClock),
			reliance_observer: ObserverSlot::default(),
		}
	}

	/// ADR 628 B1.C: the reliance assumptions, budgets and clock for queries registered from now on.
	/// Production keeps the defaults (declared values, `CLOCK_BOOTTIME`). Tests inject a clock.
	pub fn with_reliance(
		mut self,
		params: crate::distributed::query_reliance::RelianceParams,
		clock: Arc<dyn crate::distributed::query_reliance::RelianceClock>,
	) -> Self {
		self.reliance_params = params;
		self.reliance_clock = clock;
		self
	}

	/// ADR 628 B1.C (instrument seam S2): records the reliance events of every query registered from
	/// now on. Production installs none.
	pub fn with_reliance_observer(
		mut self,
		observer: Arc<dyn crate::distributed::query_reliance::RelianceObserver>,
	) -> Self {
		self.reliance_observer = ObserverSlot(Some(observer));
		self
	}

	pub fn register(&self, tenant_id: TenantId, query_id: QueryId) -> ActiveQueryGuard {
		self.register_with_downstream(
			tenant_id,
			query_id,
			DownstreamAttemptRegistry::with_default_limits(None),
		)
	}

	pub fn register_with_downstream(
		&self,
		tenant_id: TenantId,
		query_id: QueryId,
		downstream: DownstreamAttemptRegistry,
	) -> ActiveQueryGuard {
		let key = ActiveQueryKey::new(tenant_id, query_id);
		let reliance = Arc::new(crate::distributed::query_reliance::QueryReliance::new(
			self.reliance_params,
			Arc::clone(&self.reliance_clock),
		));
		reliance.set_query(query_id);
		if let Some(observer) = &self.reliance_observer.0 {
			reliance.set_observer(Arc::clone(observer));
		}
		let handle = ActiveQueryHandle::new(key, downstream, reliance);
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
		let (first_request, remote_tasks) = handle.cancel(_trigger);
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
		// ADR 628 B1.C: an owner dropped before an outcome ends the query's reliance (no-op after a
		// commit or failure).
		self.handle
			.reliance()
			.seal_ended(crate::distributed::query_reliance::QueryEnd::Dropped);
		self.registry.remove_if_same(self.key, &self.handle);
	}
}

impl ActiveQueryHandle {
	fn new(
		key: ActiveQueryKey,
		downstream: DownstreamAttemptRegistry,
		reliance: Arc<crate::distributed::query_reliance::QueryReliance>,
	) -> Self {
		let handle = Self {
			inner: Arc::new(ActiveQueryState {
				key,
				reliance,
				cancelled: AtomicBool::new(false),
				terminal: AtomicBool::new(false),
				notify: Notify::new(),
				lease_failure_worker: Mutex::new(None),
				remote_tasks: Mutex::new(Vec::new()),
				downstream,
			}),
		};
		// A reliance failure (lapse or fatal reply) stops every dispatcher with the worker's
		// identity; a local clock or arithmetic fault names none. The hook holds only a weak
		// reference: no state -> ledger -> state cycle.
		let weak = Arc::downgrade(&handle.inner);
		handle
			.inner
			.reliance
			.set_failure_hook(Arc::new(move |worker| {
				if let Some(inner) = weak.upgrade() {
					let handle = ActiveQueryHandle { inner };
					match worker {
						Some(worker_id) => {
							handle.fail_query_lease(worker_id);
						}
						None => handle.stop_dispatchers(),
					}
				}
			}));
		handle
	}

	pub fn key(&self) -> ActiveQueryKey {
		self.inner.key
	}

	/// ADR 628 B1.C: this query's per-worker lease reliance.
	pub fn reliance(&self) -> &Arc<crate::distributed::query_reliance::QueryReliance> {
		&self.inner.reliance
	}

	pub fn is_cancelled(&self) -> bool {
		self.inner.cancelled.load(Ordering::SeqCst)
	}

	pub fn is_terminal(&self) -> bool {
		self.inner.terminal.load(Ordering::SeqCst)
	}

	pub fn lease_failure_worker(&self) -> Option<crate::distributed::identity::WorkerId> {
		*self
			.inner
			.lease_failure_worker
			.lock()
			.expect("active query lease failure poisoned")
	}

	pub fn mark_terminal(&self) {
		// ADR 628 B1.C: a backstop seal; the owner seals its outcome before retiring.
		self.inner
			.reliance
			.seal_ended(crate::distributed::query_reliance::QueryEnd::Retired);
		if !self.inner.terminal.swap(true, Ordering::SeqCst) {
			self.inner.notify.notify_waiters();
		}
	}

	pub async fn stopped(&self) {
		loop {
			let notified = self.inner.notify.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			if self.is_cancelled() || self.is_terminal() {
				return;
			}
			notified.await;
		}
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

	pub fn downstream_cancellation_context(&self) -> DownstreamCancellationContext {
		self.inner.downstream.context()
	}

	pub fn cancel(&self, trigger: CancellationTrigger) -> (bool, Vec<RemoteTaskCancellation>) {
		// ADR 628 B1.C: cancellation is sealed into the reliance ledger first, so a success that
		// arrives later cannot commit and no new action is authorized.
		self.inner
			.reliance
			.seal_ended(crate::distributed::query_reliance::QueryEnd::Cancelled);
		let first_request = !self.inner.cancelled.swap(true, Ordering::SeqCst);
		if first_request {
			self.inner.notify.notify_waiters();
			self.inner
				.downstream
				.cancel_registered_attempts(trigger.into());
		}
		let remote_tasks = self
			.inner
			.remote_tasks
			.lock()
			.expect("active query tasks poisoned")
			.clone();
		(first_request, remote_tasks)
	}

	/// Fail the query when its owner can no longer renew a worker's namespace lease.
	/// This is not a client cancellation: preserving the worker id lets dispatch surface
	/// `WorkerLost` rather than flattening an ownership failure into `Cancelled`.
	pub fn fail_query_lease(&self, worker_id: crate::distributed::identity::WorkerId) -> bool {
		{
			let mut failed = self
				.inner
				.lease_failure_worker
				.lock()
				.expect("active query lease failure poisoned");
			failed.get_or_insert(worker_id);
		}
		let first_request = !self.inner.cancelled.swap(true, Ordering::SeqCst);
		if first_request {
			self.inner.notify.notify_waiters();
			self.inner
				.downstream
				.cancel_registered_attempts(DownstreamCancellationTrigger::WorkerCancelTask);
		}
		first_request
	}

	/// ADR 628 B1.C: a local reliance fault (clock or arithmetic) names no worker. The ledger is
	/// already failed; this stops every dispatcher as a lease failure would, without a worker.
	fn stop_dispatchers(&self) {
		let first_request = !self.inner.cancelled.swap(true, Ordering::SeqCst);
		if first_request {
			self.inner.notify.notify_waiters();
			self.inner
				.downstream
				.cancel_registered_attempts(DownstreamCancellationTrigger::WorkerCancelTask);
		}
	}

	pub fn record_remote_task(&self, worker: WorkerEntry, identity: TaskIdentity) {
		self.inner
			.remote_tasks
			.lock()
			.expect("active query tasks poisoned")
			.push(RemoteTaskCancellation { identity, worker });
	}

	/// Growing per-query worker census. Task attempts may repeat on the same worker;
	/// retirement addresses the worker namespace once, not each attempt.
	pub fn remote_workers(&self) -> Vec<WorkerEntry> {
		let tasks = self
			.inner
			.remote_tasks
			.lock()
			.expect("active query tasks poisoned");
		let mut workers = HashMap::new();
		for task in tasks.iter() {
			workers
				.entry(task.worker.worker_id)
				.or_insert_with(|| task.worker.clone());
		}
		workers.into_values().collect()
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
		handle.cancel(CancellationTrigger::SqlCancel);

		tokio::time::timeout(Duration::from_millis(50), handle.cancelled())
			.await
			.expect("pre-cancelled query handle must not park");
	}

	#[tokio::test]
	async fn statement_timeout_cancels_dispatched_wendelon_table_query_attempt() {
		let metrics = Arc::new(DownstreamCancellationMetrics::default());
		let registry = DownstreamAttemptRegistry::with_default_limits(Some(metrics.clone()));
		let sender = Arc::new(FakeWendelonCancelSender::new(
			TableQueryCancelStatus::ACCEPTED,
		));
		let sender_dyn: Arc<dyn WendelonTableQueryCancelSender> = sender.clone();
		let attempt = table_query_attempt(11);
		let _guard = registry
			.register_table_query_attempt(sender_dyn, attempt)
			.expect("register attempt");

		registry.cancel_registered_attempts(DownstreamCancellationTrigger::StatementTimeout);
		sender.wait_for_calls(1).await;

		assert_eq!(sender.calls(), vec![attempt]);
		assert_eq!(
			metrics.get(
				DownstreamCancellationTrigger::StatementTimeout,
				DownstreamCancellationOutcome::Accepted
			),
			1
		);
	}

	#[tokio::test]
	async fn sql_cancel_cancels_same_tenant_wendelon_table_query_attempt() {
		let metrics = Arc::new(DownstreamCancellationMetrics::default());
		let downstream = DownstreamAttemptRegistry::with_default_limits(Some(metrics));
		let registry = ActiveQueryRegistry::new();
		let guard =
			registry.register_with_downstream(TenantId(7), QueryId::from_u128(70), downstream);
		let handle = guard.handle();
		let sender = Arc::new(FakeWendelonCancelSender::new(
			TableQueryCancelStatus::ACCEPTED,
		));
		let sender_dyn: Arc<dyn WendelonTableQueryCancelSender> = sender.clone();
		let attempt = table_query_attempt(70);
		let _attempt_guard = handle
			.downstream_cancellation_context()
			.register_wendelon_table_query_attempt(sender_dyn, attempt)
			.expect("register downstream attempt");

		let wrong_tenant = registry.cancel(
			TenantId(8),
			QueryId::from_u128(70),
			CancellationTrigger::SqlCancel,
		);
		assert!(!wrong_tenant.was_active);
		let cancellation = registry.cancel(
			TenantId(7),
			QueryId::from_u128(70),
			CancellationTrigger::SqlCancel,
		);
		assert!(cancellation.was_active);
		assert!(cancellation.first_request);
		sender.wait_for_calls(1).await;

		assert_eq!(sender.calls(), vec![attempt]);
	}

	#[tokio::test]
	async fn worker_cancel_task_cancels_inflight_wendelon_table_query_before_future_drop() {
		let metrics = Arc::new(DownstreamCancellationMetrics::default());
		let registry = ActiveTaskRegistry::with_downstream_cancellation_metrics(Some(metrics));
		let registration = registry.register(identity(77));
		let sender = Arc::new(FakeWendelonCancelSender::new(
			TableQueryCancelStatus::ACCEPTED,
		));
		let sender_dyn: Arc<dyn WendelonTableQueryCancelSender> = sender.clone();
		let attempt = table_query_attempt(77);
		let _attempt_guard = registration
			.downstream
			.context()
			.register_wendelon_table_query_attempt(sender_dyn, attempt)
			.expect("register downstream attempt");

		assert!(registry.cancel(identity(77)));
		assert!(registration.signal.is_cancelled());
		sender.wait_for_calls(1).await;
		assert_eq!(sender.calls(), vec![attempt]);
	}

	#[test]
	fn cancel_before_wendelon_scan_start_sends_no_table_query_req() {
		let registry = DownstreamAttemptRegistry::with_default_limits(None);
		registry.cancel_registered_attempts(DownstreamCancellationTrigger::SqlCancel);
		let sender = Arc::new(FakeWendelonCancelSender::new(
			TableQueryCancelStatus::ACCEPTED,
		));
		let sender_dyn: Arc<dyn WendelonTableQueryCancelSender> = sender;

		let error = registry
			.context()
			.register_wendelon_table_query_attempt(sender_dyn, table_query_attempt(1))
			.expect_err("cancelled context rejects registration");

		assert_eq!(error, DownstreamRegistrationError::AlreadyCancelled);
		assert_eq!(registry.active_len(), 0);
	}

	#[tokio::test]
	async fn wendelon_redirect_retry_replaces_attempt_registration() {
		let registry = DownstreamAttemptRegistry::with_default_limits(None);
		let sender = Arc::new(FakeWendelonCancelSender::new(
			TableQueryCancelStatus::ACCEPTED,
		));
		let sender_dyn: Arc<dyn WendelonTableQueryCancelSender> = sender.clone();
		let first = table_query_attempt(1);
		let second = table_query_attempt(2);
		let first_guard = registry
			.register_table_query_attempt(sender_dyn.clone(), first)
			.expect("register first attempt");
		drop(first_guard);
		let _second_guard = registry
			.register_table_query_attempt(sender_dyn, second)
			.expect("register replacement attempt");

		registry.cancel_registered_attempts(DownstreamCancellationTrigger::StatementTimeout);
		sender.wait_for_calls(1).await;

		assert_eq!(sender.calls(), vec![second]);
	}

	#[test]
	fn downstream_attempt_raii_removes_on_success_error_redirect_and_drop() {
		let registry = DownstreamAttemptRegistry::with_default_limits(None);
		let sender = Arc::new(FakeWendelonCancelSender::new(
			TableQueryCancelStatus::ACCEPTED,
		));
		let sender_dyn: Arc<dyn WendelonTableQueryCancelSender> = sender;

		let success_guard = registry
			.register_table_query_attempt(sender_dyn.clone(), table_query_attempt(1))
			.expect("register success attempt");
		let error_guard = registry
			.register_table_query_attempt(sender_dyn.clone(), table_query_attempt(2))
			.expect("register error attempt");
		let redirect_guard = registry
			.register_table_query_attempt(sender_dyn, table_query_attempt(3))
			.expect("register redirect attempt");
		assert_eq!(registry.active_len(), 3);

		drop(success_guard);
		drop(error_guard);
		drop(redirect_guard);

		assert_eq!(registry.active_len(), 0);
	}

	#[tokio::test]
	async fn downstream_cancel_racing_completion_is_terminal_or_ack_without_error() {
		let metrics = Arc::new(DownstreamCancellationMetrics::default());
		let registry = DownstreamAttemptRegistry::with_default_limits(Some(metrics.clone()));
		let sender = Arc::new(FakeWendelonCancelSender::new(
			TableQueryCancelStatus::TERMINAL,
		));
		let sender_dyn: Arc<dyn WendelonTableQueryCancelSender> = sender.clone();
		let _guard = registry
			.register_table_query_attempt(sender_dyn, table_query_attempt(99))
			.expect("register attempt");

		registry.cancel_registered_attempts(DownstreamCancellationTrigger::ClientDisconnect);
		sender.wait_for_calls(1).await;

		assert_eq!(
			metrics.get(
				DownstreamCancellationTrigger::ClientDisconnect,
				DownstreamCancellationOutcome::AlreadyTerminal
			),
			1
		);
	}

	#[tokio::test]
	async fn downstream_cancel_fanout_timeout_does_not_delay_sql_cancel_response() {
		let metrics = Arc::new(DownstreamCancellationMetrics::default());
		let registry = DownstreamAttemptRegistry::with_default_limits(Some(metrics.clone()));
		let sender = Arc::new(
			FakeWendelonCancelSender::new(TableQueryCancelStatus::ACCEPTED).with_hung_ack(),
		);
		let sender_dyn: Arc<dyn WendelonTableQueryCancelSender> = sender;
		let _guard = registry
			.register_table_query_attempt(sender_dyn, table_query_attempt(101))
			.expect("register attempt");
		let started = Instant::now();

		registry.cancel_registered_attempts(DownstreamCancellationTrigger::SqlCancel);

		assert!(started.elapsed() < Duration::from_millis(20));
		wait_for_downstream_metric(
			&metrics,
			DownstreamCancellationTrigger::SqlCancel,
			DownstreamCancellationOutcome::RemoteTimeout,
			1,
		)
		.await;
		assert_eq!(
			metrics.get(
				DownstreamCancellationTrigger::SqlCancel,
				DownstreamCancellationOutcome::RemoteTimeout
			),
			1
		);
	}

	struct FakeWendelonCancelSender {
		status: TableQueryCancelStatus,
		hang_ack: bool,
		calls: Mutex<Vec<WendelonTableQueryAttempt>>,
		notify: Notify,
	}

	impl FakeWendelonCancelSender {
		fn new(status: TableQueryCancelStatus) -> Self {
			Self {
				status,
				hang_ack: false,
				calls: Mutex::new(Vec::new()),
				notify: Notify::new(),
			}
		}

		fn with_hung_ack(mut self) -> Self {
			self.hang_ack = true;
			self
		}

		fn calls(&self) -> Vec<WendelonTableQueryAttempt> {
			self.calls.lock().expect("calls poisoned").clone()
		}

		async fn wait_for_calls(&self, expected: usize) {
			tokio::time::timeout(Duration::from_secs(1), async {
				loop {
					if self.calls.lock().expect("calls poisoned").len() >= expected {
						return;
					}
					self.notify.notified().await;
				}
			})
			.await
			.expect("timed out waiting for downstream cancel calls");
		}
	}

	async fn wait_for_downstream_metric(
		metrics: &DownstreamCancellationMetrics,
		trigger: DownstreamCancellationTrigger,
		outcome: DownstreamCancellationOutcome,
		expected: u64,
	) {
		tokio::time::timeout(Duration::from_secs(1), async {
			loop {
				if metrics.get(trigger, outcome) >= expected {
					return;
				}
				tokio::task::yield_now().await;
			}
		})
		.await
		.expect("timed out waiting for downstream cancellation metric");
	}

	#[async_trait::async_trait]
	impl WendelonTableQueryCancelSender for FakeWendelonCancelSender {
		async fn cancel_table_query_attempt(
			&self,
			target: NodeId,
			tenant_id: WendelonTenantId,
			request_id: TableQueryRequestId,
		) -> Result<TableQueryCancelResponseMeta, wendelon_client::ClientError> {
			if self.hang_ack {
				std::future::pending::<()>().await;
			}
			self.calls
				.lock()
				.expect("calls poisoned")
				.push(WendelonTableQueryAttempt {
					target,
					tenant_id,
					request_id,
				});
			self.notify.notify_waiters();
			Ok(TableQueryCancelResponseMeta {
				status: self.status,
				_pad: [0; 7],
			})
		}
	}

	fn table_query_attempt(request_id: u128) -> WendelonTableQueryAttempt {
		WendelonTableQueryAttempt {
			target: NodeId {
				id: 42,
				_reserved: 0,
			},
			tenant_id: WendelonTenantId::new(7),
			request_id: TableQueryRequestId::from_u128(request_id),
		}
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
