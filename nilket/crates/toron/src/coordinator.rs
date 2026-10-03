use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt};

mod delete;
mod write;

use crate::binder::{
	PlannerConfig, bind_explain_analyze_with_statement_timestamp, bind_with_statement_timestamp,
	measured_explain_analyze_verdicts, render_explain_analyze,
};
use crate::catalog::{Catalog, CtasProvisionRequest, InvalidatedDescriptor};
use crate::distributed::budget::{
	AdmissionAttempt, AdmissionPolicy, AdmissionReservation, AdmissionResource, BudgetDimension,
	BudgetRejection, QueryBudget, TaskBudget, TenantAdmissionController, TenantAdmissionRejection,
	TenantAdmissionRejectionCounters, TenantBudgetPolicy, TenantConcurrencyRejection,
};
use crate::distributed::cancellation::{
	ActiveQueryHandle, ActiveQueryRegistry, CancellationMetrics, CancellationOutcome,
	CancellationTrigger, DownstreamAttemptRegistry, DownstreamCancellationMetrics,
	QueryCancellation, RemoteTaskCancellation,
};
use crate::distributed::context::TaskExecutionContext;
use crate::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use crate::distributed::identity::QueryId;
use crate::distributed::identity::StageId;
use crate::distributed::namespace_lifecycle::QueryTerminalCause;
use crate::distributed::scheduler::{
	DistributedScheduler, SchedulerError, StagePlan, TaskTelemetryRecord, duration_micros,
};
use crate::distributed::task::{JoinRuntimeObservation, JoinRuntimeObservationMode, TaskTelemetry};
use crate::distributed::task_state::TaskKey;
use crate::distributed::worker_registry::{WorkerEntry, WorkerRegistry};
use crate::error::{
	CatalogError, ExecutionError, QueryError, RuntimeError, StorageError, TaskFailureKind,
};
use crate::exec::{
	ExecutionContext, Executor, StatementClock, SystemStatementClock, materialize_scalar_rows,
};
use crate::ir::{
	BoundReturningProjection, BroadcastReplayEligibility, ColumnOrdinal, CreateTableRowIdentity,
	ExecutionPlan, FilterDisposition, JoinReplayId, JoinStrategy, LogicalPlan, MorselId, TableId,
	data_type_to_logical_type_physical,
};
use crate::server::{
	NetworkTaskDispatcher, ToronCoordinator, memory::MemoryManager, transport::ToronTransport,
};
use crate::storage::{
	CreateLogicalTypeRequest, DeleteRowsOutcome, DeleteRowsRequest, DropTableStorageRequest,
	MutationResolution, ResolvedMutationOutcome, RowIdentityProjection, StorageAdmin,
	StorageDeleteRows, StorageWrite, TenantId, UnsupportedStorageAdmin, WriteOutcome, WriteRequest,
};
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

const REMOTE_CANCEL_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);
const REMOTE_CANCEL_ACK_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_REMOTE_CANCEL_FANOUTS: usize = 64;
const DISTRIBUTED_RESULT_STREAM_DEPTH: usize = 16;
const CTAS_ROW_WRITE_SEQUENCE_NO: u64 = 0x4354_4153_0000_0001;
const CTAS_OWNER_LEASE_MS: u64 = 60_000;

type RemoteCancelSpawner =
	Arc<dyn Fn(CancellationTrigger, Vec<RemoteTaskCancellation>) + Send + Sync>;
type RemoteRetireSpawner = Arc<dyn Fn(QueryId, QueryTerminalCause, Vec<WorkerEntry>) + Send + Sync>;

struct DistributedAttemptOutput {
	final_results: Vec<RecordBatch>,
	task_telemetry: Vec<TaskTelemetryRecord>,
}

/// Everything one distributed query's attribution needs, retained only when a caller opted
/// in: per-task telemetry keyed by the task that produced it, plus the stage graph's
/// upstream edges so a per-cell figure can be derived along a real path rather than summed
/// across concurrent tasks.
#[derive(Debug, Clone)]
pub struct QueryTaskTelemetryReport {
	pub query_id: QueryId,
	/// ADR 554 Branch B2: coordinator-owned lifecycle spans, reported separately.
	/// Neither is an ADR 538 component, and they are deliberately not summed here
	/// — a single lifecycle number is the opaque bucket that record exists to
	/// avoid.
	pub lifecycle: QueryLifecycleSpans,
	/// `(stage, its upstream stages)` for every stage in the fragmented plan, control
	/// dependencies included — any edge that orders execution belongs in a wall-clock path.
	pub stage_upstreams: Vec<(StageId, Vec<StageId>)>,
	pub tasks: Vec<TaskTelemetryRecord>,
}

/// Coordinator-owned lifecycle spans around the dispatch window (ADR 554 §3).
///
/// `pre_dispatch` covers bind, fragment, and scheduler construction — everything
/// between the driver receiving a logical plan and the first dispatcher call.
/// None of these include SQL planning, which happens before the coordinator is
/// called, nor consumer drain, which continues after the producer finishes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLifecycleSpans {
	pub pre_dispatch_micros: u64,
	/// Wall duration of `dispatch_loop` itself. **Contains** every task's dispatch
	/// window, so it is an envelope over them, not a term beside them. Subtracting
	/// the path's dispatch sum from it yields in-loop coordination only when those
	/// windows are contained and non-overlapping — which the consumer must check
	/// rather than assume.
	pub dispatch_loop_wall_micros: u64,
	/// From `dispatch_loop` returning to the end of the driver's post-loop work.
	///
	/// **This is producer finalization, not drain.** It ends when the coordinator
	/// stops producing; buffered batches may still be consumed afterwards, so a
	/// consumer-exhaustion boundary cannot live here and is measured by the caller.
	pub post_dispatch_finalization_micros: u64,
}

/// Opt-in retention slot for [`QueryTaskTelemetryReport`]s. Reports accumulate keyed by
/// query until taken, so the holder must drain what it asked for; production coordinators
/// that never opt in retain nothing.
pub type TaskTelemetryRetention = Arc<Mutex<BTreeMap<QueryId, QueryTaskTelemetryReport>>>;

fn retain_task_telemetry_report(
	retention: &Option<TaskTelemetryRetention>,
	query_id: QueryId,
	stages: &[StagePlan],
	tasks: &[TaskTelemetryRecord],
	lifecycle: QueryLifecycleSpans,
) {
	let Some(retention) = retention.as_ref() else {
		return;
	};
	let report = QueryTaskTelemetryReport {
		query_id,
		lifecycle,
		stage_upstreams: stages
			.iter()
			.map(|stage| (stage.stage_id, stage.upstream_stages.clone()))
			.collect(),
		tasks: tasks.to_vec(),
	};
	if let Ok(mut retained) = retention.lock() {
		retained.insert(query_id, report);
	}
}

pub(crate) async fn materialize_scan_stream(
	stream: crate::scan_metering::MeteredScanStream,
) -> Result<Vec<RecordBatch>, StorageError> {
	stream.try_collect().await
}

/// ADR 628 B1.C: seals the query's outcome in its reliance ledger, before any error delivery or
/// cleanup is awaited. Success commits only if every required worker is still relied on; a refusal
/// (a lapse, a fatal reply, a local fault, or a cancellation that won first) becomes the outward
/// error. An ordinary failure is sealed with its own cause and keeps its own outward error, unless a
/// local reliance fault (which names no worker) is what stopped the query.
fn commit_query_success<T>(
	active_query: &ActiveQueryHandle,
	result: Result<T, QueryError>,
) -> Result<T, QueryError> {
	use crate::distributed::query_reliance::{QueryEnd, QueryOutcome, Refusal, RelianceCause};
	let reliance = active_query.reliance();
	match result {
		Ok(value) => match reliance.commit_success() {
			Ok(()) => Ok(value),
			Err(refusal) => Err(refusal.query_error()),
		},
		Err(error) => {
			reliance.seal_ended(QueryEnd::Error(error.to_string()));
			match reliance.outcome() {
				QueryOutcome::Failed {
					worker: None,
					cause: cause @ (RelianceCause::Clock(_) | RelianceCause::Arithmetic),
				} => Err(Refusal {
					worker: None,
					cause,
				}
				.query_error()),
				_ => Err(error),
			}
		}
	}
}

pub struct DistributedResultStream {
	receiver: mpsc::Receiver<Result<RecordBatch, QueryError>>,
	active_query: ActiveQueryHandle,
	cancel_remote: RemoteCancelSpawner,
	finished: bool,
	/// Buffered batches offered to the delivery gate so far.
	offered: u64,
}

impl DistributedResultStream {
	fn new(
		receiver: mpsc::Receiver<Result<RecordBatch, QueryError>>,
		active_query: ActiveQueryHandle,
		cancel_remote: RemoteCancelSpawner,
	) -> Self {
		Self {
			receiver,
			active_query,
			cancel_remote,
			finished: false,
			offered: 0,
		}
	}
}

impl futures::Stream for DistributedResultStream {
	type Item = Result<RecordBatch, QueryError>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		use crate::distributed::query_reliance::{QueryEnd, QueryOutcome, Refusal, RelianceCause};
		if self.finished {
			return Poll::Ready(None);
		}
		loop {
			match self.receiver.poll_recv(cx) {
				Poll::Ready(Some(Ok(batch))) => {
					// ADR 628 B1.C: each buffered batch passes the delivery gate at the moment it is
					// handed out. Committed: delivered against the committed local state. Open: every
					// relied-on bound must still hold, sampled now (a starved or late poll sees the
					// lapse itself). Failed: suppressed (partial-result-then-error).
					self.offered += 1;
					let offered = self.offered;
					match self.active_query.reliance().delivery_gate(offered) {
						Ok(()) => return Poll::Ready(Some(Ok(batch))),
						// The query's own error follows from its driver: discard the batch and
						// report that error, not a substitute.
						Err(Refusal {
							cause: RelianceCause::QueryEnded(QueryEnd::Error(_)),
							..
						}) => continue,
						Err(refusal) => {
							self.finished = true;
							return Poll::Ready(Some(Err(refusal.query_error())));
						}
					}
				}
				Poll::Ready(Some(Err(error))) => {
					self.finished = true;
					return Poll::Ready(Some(Err(error)));
				}
				Poll::Ready(None) => {
					// End of stream certifies success only once committed. A failure without its
					// error, or a driver that disappeared while the query was open, is an error.
					self.finished = true;
					let reliance = self.active_query.reliance();
					return match reliance.outcome() {
						QueryOutcome::Committed => Poll::Ready(None),
						QueryOutcome::Failed { worker, cause } => {
							Poll::Ready(Some(Err(Refusal { worker, cause }.query_error())))
						}
						QueryOutcome::Open => {
							reliance.seal_ended(QueryEnd::DriverLost);
							Poll::Ready(Some(Err(Refusal {
								worker: None,
								cause: RelianceCause::QueryEnded(QueryEnd::DriverLost),
							}
							.query_error())))
						}
					};
				}
				Poll::Pending => return Poll::Pending,
			}
		}
	}
}

impl Drop for DistributedResultStream {
	fn drop(&mut self) {
		if self.finished {
			return;
		}
		let (first_request, remote_tasks) = self
			.active_query
			.cancel(CancellationTrigger::ClientDisconnect);
		if first_request && !remote_tasks.is_empty() {
			(self.cancel_remote)(CancellationTrigger::ClientDisconnect, remote_tasks);
		}
	}
}

pub struct DistributedQueryCoordinator<Registry, Transport>
where
	Registry: WorkerRegistry + ?Sized,
	Transport: ToronTransport + ?Sized,
{
	catalog: Arc<dyn Catalog>,
	registry: Arc<Registry>,
	memory: Arc<MemoryManager>,
	transport: Arc<Transport>,
	config: PlannerConfig,
	query_budget: QueryBudget,
	tenant_budgets: TenantBudgetPolicy,
	admission_policy: Option<AdmissionPolicy>,
	tenant_admission: TenantAdmissionController,
	admission_rejections: TenantAdmissionRejectionCounters,
	active_queries: ActiveQueryRegistry,
	cancellation_metrics: Option<Arc<CancellationMetrics>>,
	downstream_cancellation_metrics: Option<Arc<DownstreamCancellationMetrics>>,
	cancel_fanout_permits: Arc<Semaphore>,
	downstream_cancel_fanout_permits: Arc<Semaphore>,
	context: ExecutionContext,
	writer: Arc<dyn StorageWrite>,
	deleter: Arc<dyn StorageDeleteRows>,
	admin: Arc<dyn StorageAdmin>,
	task_telemetry_retention: Option<TaskTelemetryRetention>,
	/// ADR 628 B1.C (instrument seam S1): the transport query lease RENEWAL exchanges use. `None`
	/// (production) uses `transport`, as dispatch, retirement and cancellation always do.
	renewal_transport: Option<Arc<dyn ToronTransport>>,
	/// Test-only: holds or breaks each query's renewal cleanup, to pin its ordering.
	#[cfg(test)]
	renewal_cleanup_hook: RenewalCleanupHook,
}

/// Test-only control of a renewal owner's shutdown: `hold` blocks it until a permit is added; with
/// `lose_supervisor` the supervisor is aborted before it is awaited (an unverified cleanup).
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct RenewalCleanupHook {
	pub(crate) hold: Option<Arc<Semaphore>>,
	pub(crate) lose_supervisor: bool,
}

impl<Registry, Transport> DistributedQueryCoordinator<Registry, Transport>
where
	Registry: WorkerRegistry + ?Sized,
	Transport: ToronTransport + ?Sized,
{
	pub fn new(
		catalog: Arc<dyn Catalog>,
		registry: Arc<Registry>,
		memory: Arc<MemoryManager>,
		transport: Arc<Transport>,
		config: PlannerConfig,
		query_budget: QueryBudget,
	) -> Self {
		Self::with_context_writer_deleter_and_admin(
			catalog,
			registry,
			memory,
			transport,
			config,
			query_budget,
			default_distributed_execution_context(),
			Arc::new(UnsupportedStorageWrite),
			Arc::new(UnsupportedStorageDeleteRows),
			Arc::new(UnsupportedStorageAdmin),
		)
	}

	pub fn with_context(
		catalog: Arc<dyn Catalog>,
		registry: Arc<Registry>,
		memory: Arc<MemoryManager>,
		transport: Arc<Transport>,
		config: PlannerConfig,
		query_budget: QueryBudget,
		context: ExecutionContext,
	) -> Self {
		Self::with_context_writer_deleter_and_admin(
			catalog,
			registry,
			memory,
			transport,
			config,
			query_budget,
			context,
			Arc::new(UnsupportedStorageWrite),
			Arc::new(UnsupportedStorageDeleteRows),
			Arc::new(UnsupportedStorageAdmin),
		)
	}

	pub fn with_writer(
		catalog: Arc<dyn Catalog>,
		registry: Arc<Registry>,
		memory: Arc<MemoryManager>,
		transport: Arc<Transport>,
		config: PlannerConfig,
		query_budget: QueryBudget,
		writer: Arc<dyn StorageWrite>,
	) -> Self {
		Self::with_context_writer_deleter_and_admin(
			catalog,
			registry,
			memory,
			transport,
			config,
			query_budget,
			default_distributed_execution_context(),
			writer,
			Arc::new(UnsupportedStorageDeleteRows),
			Arc::new(UnsupportedStorageAdmin),
		)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn with_writer_and_deleter(
		catalog: Arc<dyn Catalog>,
		registry: Arc<Registry>,
		memory: Arc<MemoryManager>,
		transport: Arc<Transport>,
		config: PlannerConfig,
		query_budget: QueryBudget,
		writer: Arc<dyn StorageWrite>,
		deleter: Arc<dyn StorageDeleteRows>,
	) -> Self {
		Self::with_context_writer_deleter_and_admin(
			catalog,
			registry,
			memory,
			transport,
			config,
			query_budget,
			default_distributed_execution_context(),
			writer,
			deleter,
			Arc::new(UnsupportedStorageAdmin),
		)
	}

	pub fn with_storage_admin(mut self, admin: Arc<dyn StorageAdmin>) -> Self {
		self.admin = admin;
		self
	}

	#[allow(clippy::too_many_arguments)]
	pub fn with_writer_deleter_and_admin(
		catalog: Arc<dyn Catalog>,
		registry: Arc<Registry>,
		memory: Arc<MemoryManager>,
		transport: Arc<Transport>,
		config: PlannerConfig,
		query_budget: QueryBudget,
		writer: Arc<dyn StorageWrite>,
		deleter: Arc<dyn StorageDeleteRows>,
		admin: Arc<dyn StorageAdmin>,
	) -> Self {
		Self::with_context_writer_deleter_and_admin(
			catalog,
			registry,
			memory,
			transport,
			config,
			query_budget,
			default_distributed_execution_context(),
			writer,
			deleter,
			admin,
		)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn with_context_writer_and_deleter(
		catalog: Arc<dyn Catalog>,
		registry: Arc<Registry>,
		memory: Arc<MemoryManager>,
		transport: Arc<Transport>,
		config: PlannerConfig,
		query_budget: QueryBudget,
		context: ExecutionContext,
		writer: Arc<dyn StorageWrite>,
		deleter: Arc<dyn StorageDeleteRows>,
	) -> Self {
		Self::with_context_writer_deleter_and_admin(
			catalog,
			registry,
			memory,
			transport,
			config,
			query_budget,
			context,
			writer,
			deleter,
			Arc::new(UnsupportedStorageAdmin),
		)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn with_context_writer_deleter_and_admin(
		catalog: Arc<dyn Catalog>,
		registry: Arc<Registry>,
		memory: Arc<MemoryManager>,
		transport: Arc<Transport>,
		config: PlannerConfig,
		query_budget: QueryBudget,
		context: ExecutionContext,
		writer: Arc<dyn StorageWrite>,
		deleter: Arc<dyn StorageDeleteRows>,
		admin: Arc<dyn StorageAdmin>,
	) -> Self {
		let tenant_budgets = TenantBudgetPolicy::from_query_budget(query_budget);
		Self {
			catalog,
			registry,
			memory,
			transport,
			config,
			query_budget,
			admission_policy: None,
			tenant_admission: TenantAdmissionController::new(tenant_budgets.clone(), None),
			admission_rejections: TenantAdmissionRejectionCounters::new(&tenant_budgets),
			active_queries: ActiveQueryRegistry::new(),
			cancellation_metrics: None,
			downstream_cancellation_metrics: None,
			cancel_fanout_permits: Arc::new(Semaphore::new(MAX_REMOTE_CANCEL_FANOUTS)),
			downstream_cancel_fanout_permits: Arc::new(Semaphore::new(MAX_REMOTE_CANCEL_FANOUTS)),
			tenant_budgets,
			context,
			writer,
			deleter,
			admin,
			task_telemetry_retention: None,
			renewal_transport: None,
			#[cfg(test)]
			renewal_cleanup_hook: RenewalCleanupHook::default(),
		}
	}

	#[cfg(test)]
	pub(crate) fn with_renewal_cleanup_hook(mut self, hook: RenewalCleanupHook) -> Self {
		self.renewal_cleanup_hook = hook;
		self
	}

	/// ADR 628 B1.C (instrument seam S1): renewal exchanges use `transport` instead of the dispatch
	/// transport. Dispatch (with its inline leases), retirement, cancellation and peer pulls are
	/// unchanged. Only renewal-path isolation, never a network partition.
	pub fn with_renewal_transport(mut self, transport: Arc<dyn ToronTransport>) -> Self {
		self.renewal_transport = Some(transport);
		self
	}

	/// ADR 628 B1.C (instrument seam S2): records every query's reliance events. Observational only.
	pub fn with_reliance_observer(
		mut self,
		observer: Arc<dyn crate::distributed::query_reliance::RelianceObserver>,
	) -> Self {
		self.active_queries = self.active_queries.with_reliance_observer(observer);
		self
	}

	/// ADR 628 B1.C: the reliance assumptions, budgets and clock (tests and instruments).
	pub fn with_reliance(
		mut self,
		params: crate::distributed::query_reliance::RelianceParams,
		clock: Arc<dyn crate::distributed::query_reliance::RelianceClock>,
	) -> Self {
		self.active_queries = self.active_queries.with_reliance(params, clock);
		self
	}

	/// Opt in to retaining each query's per-task telemetry report in `retention`. Reports
	/// stay keyed by query until the holder takes them, so only callers that drain what
	/// they request should opt in.
	pub fn with_task_telemetry_retention(mut self, retention: TaskTelemetryRetention) -> Self {
		self.task_telemetry_retention = Some(retention);
		self
	}

	pub fn with_tenant_budget_policy(mut self, tenant_budgets: TenantBudgetPolicy) -> Self {
		self.tenant_admission =
			TenantAdmissionController::new(tenant_budgets.clone(), self.admission_policy);
		self.admission_rejections = TenantAdmissionRejectionCounters::new(&tenant_budgets);
		self.tenant_budgets = tenant_budgets;
		self
	}

	pub fn with_admission_policy(mut self, admission: Option<AdmissionPolicy>) -> Self {
		self.admission_policy = admission;
		self.tenant_admission =
			TenantAdmissionController::new(self.tenant_budgets.clone(), admission);
		self
	}

	pub fn with_cancellation_metrics(mut self, metrics: Arc<CancellationMetrics>) -> Self {
		self.cancellation_metrics = Some(metrics);
		self
	}

	pub fn with_downstream_cancellation_metrics(
		mut self,
		metrics: Arc<DownstreamCancellationMetrics>,
	) -> Self {
		self.downstream_cancellation_metrics = Some(metrics);
		self
	}

	pub fn tenant_memory_used(&self, tenant_id: TenantId) -> crate::distributed::budget::ByteCount {
		self.tenant_admission.used_memory(tenant_id)
	}

	pub fn tenant_concurrency_active(&self, tenant_id: TenantId) -> u32 {
		self.tenant_admission.active_concurrency(tenant_id)
	}

	pub fn tenant_admission_controller(&self) -> TenantAdmissionController {
		self.tenant_admission.clone()
	}

	pub fn planner_config(&self) -> PlannerConfig {
		self.config.clone()
	}

	pub fn admission_rejection_counters(&self) -> TenantAdmissionRejectionCounters {
		self.admission_rejections.clone()
	}

	pub async fn execute_query(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>, QueryError>
	where
		Registry: 'static,
		Transport: 'static,
	{
		self.execute_query_with_context(plan, self.context.clone())
			.await
	}

	pub async fn execute_query_stream(
		&self,
		plan: &LogicalPlan,
	) -> Result<DistributedResultStream, QueryError>
	where
		Registry: 'static,
		Transport: 'static,
	{
		self.execute_query_stream_with_context(plan, self.context.clone())
			.await
	}

	pub async fn execute_query_stream_with_context(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
	) -> Result<DistributedResultStream, QueryError>
	where
		Registry: 'static,
		Transport: 'static,
	{
		if !logical_plan_is_distributed_read(plan) {
			return Err(QueryError::Internal {
				message: "execute_query_stream supports read queries only in ADR 120 V1"
					.to_string(),
			});
		}
		let admitted = self.admit_query(&context).await?;
		let query_budget = admitted.query_budget;
		let downstream = DownstreamAttemptRegistry::new(
			self.downstream_cancel_fanout_permits.clone(),
			self.downstream_cancellation_metrics.clone(),
		);
		let active_query = self.active_queries.register_with_downstream(
			context.tenant_id,
			context.query_id,
			downstream,
		);
		let active_query_handle = active_query.handle();
		let renewal = self.spawn_query_lease_renewal(active_query_handle.clone());
		let retire_remote = self.remote_retire_spawner();
		let query_id = context.query_id;
		let context = context.with_downstream_cancellation(Some(
			active_query_handle.downstream_cancellation_context(),
		));
		let (sender, receiver) = mpsc::channel(DISTRIBUTED_RESULT_STREAM_DEPTH);
		let batch_sent = Arc::new(AtomicBool::new(false));
		let cancel_remote = self.remote_cancel_spawner();
		let driver_cancel_remote = cancel_remote.clone();
		let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
		let registry = Arc::clone(&self.registry);
		let memory = Arc::clone(&self.memory);
		let transport = Arc::clone(&self.transport);
		let config = self.config.clone();
		let plan = plan.clone();
		let stream_active_query = active_query_handle.clone();
		let telemetry_retention = self.task_telemetry_retention.clone();
		tokio::spawn(async move {
			let _admitted = admitted;
			let _active_query = active_query;
			let result = run_streaming_read_driver(
				plan.clone(),
				catalog,
				registry,
				memory,
				transport,
				config,
				context,
				query_budget,
				active_query_handle.clone(),
				sender.clone(),
				batch_sent,
				telemetry_retention,
			)
			.await;
			// ADR 628 B1.C: the atomic successful-completion gate. Results are fully local now.
			let result = commit_query_success(&active_query_handle, result);
			let cause = if active_query_handle.lease_failure_worker().is_some() {
				QueryTerminalCause::Failed
			} else if active_query_handle.is_cancelled() {
				QueryTerminalCause::Cancelled
			} else if result.is_ok() {
				QueryTerminalCause::Success
			} else {
				QueryTerminalCause::Failed
			};
			if let Err(error) = result {
				let _ = sender.send(Err(error)).await;
			}
			if active_query_handle.is_cancelled() {
				let (first_request, remote_tasks) =
					active_query_handle.cancel(CancellationTrigger::ClientDisconnect);
				if first_request && !remote_tasks.is_empty() {
					driver_cancel_remote(CancellationTrigger::ClientDisconnect, remote_tasks);
				}
			}
			active_query_handle.mark_terminal();
			drop(_active_query);
			drop(_admitted);
			retire_remote(query_id, cause, active_query_handle.remote_workers());
			// ADR 628 B1.C: renewal admission is sealed and the owned renewal shutdown is awaited and
			// reported BEFORE the final sender is released, so a normal end of stream follows the cleanup
			// receipt. A failure was already published above (prompt); its cleanup completes here.
			report_renewal_cleanup(query_id, renewal.shutdown().await);
			drop(sender);
		});
		Ok(DistributedResultStream::new(
			receiver,
			stream_active_query,
			cancel_remote,
		))
	}

	/// ADR 574: execute a certificate-carrying CTAS and return its receipt.
	///
	/// The receipt is optional inside the CTAS path, because plain SQL CTAS
	/// legitimately has none. It stops being optional here: a materialization
	/// that published and produced no receipt is a broken invariant, not a
	/// success the caller should have to interpret.
	pub async fn execute_materialization_with_context(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
	) -> Result<MaterializationReceipt, QueryError>
	where
		Registry: 'static,
		Transport: 'static,
	{
		if !matches!(
			plan,
			LogicalPlan::CreateTableAsSelect {
				materialization_certificate: Some(_),
				..
			}
		) {
			return Err(QueryError::Internal {
				message: "execute_materialization_with_context requires a certificate-carrying \
				          CTAS"
					.to_string(),
			});
		}
		let admitted = self.admit_query(&context).await?;
		let query_budget = admitted.query_budget;
		let mut receipt = None;
		self.execute_query_with_admission(plan, context, query_budget, None, &mut receipt)
			.await?;
		receipt.ok_or_else(|| QueryError::Internal {
			message: "materialization published without producing a receipt".to_string(),
		})
	}

	pub async fn execute_query_with_context(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
	) -> Result<Vec<RecordBatch>, QueryError>
	where
		Registry: 'static,
		Transport: 'static,
	{
		if logical_plan_is_distributed_read(plan) {
			let mut stream = self
				.execute_query_stream_with_context(plan, context)
				.await?;
			let mut batches = Vec::new();
			while let Some(batch) = stream.next().await {
				batches.push(batch?);
			}
			return Ok(batches);
		}
		let admitted = self.admit_query(&context).await?;
		let query_budget = admitted.query_budget;
		let downstream = DownstreamAttemptRegistry::new(
			self.downstream_cancel_fanout_permits.clone(),
			self.downstream_cancellation_metrics.clone(),
		);
		let active_query = self.active_queries.register_with_downstream(
			context.tenant_id,
			context.query_id,
			downstream,
		);
		let active_query_handle = active_query.handle();
		let renewal = self.spawn_query_lease_renewal(active_query_handle.clone());
		let retire_remote = self.remote_retire_spawner();
		let query_id = context.query_id;
		let context = context.with_downstream_cancellation(Some(
			active_query_handle.downstream_cancellation_context(),
		));
		let result = self
			.execute_query_with_admission(
				plan,
				context,
				query_budget,
				Some(active_query_handle.clone()),
				&mut None,
			)
			.await;
		// ADR 628 B1.C: the atomic successful-completion gate.
		let result = commit_query_success(&active_query_handle, result);
		let cause = if active_query_handle.lease_failure_worker().is_some() {
			QueryTerminalCause::Failed
		} else if active_query_handle.is_cancelled() {
			QueryTerminalCause::Cancelled
		} else if result.is_ok() {
			QueryTerminalCause::Success
		} else {
			QueryTerminalCause::Failed
		};
		active_query_handle.mark_terminal();
		retire_remote(query_id, cause, active_query_handle.remote_workers());
		report_renewal_cleanup(query_id, renewal.shutdown().await);
		drop(active_query);
		drop(admitted);
		result
	}

	pub fn cancel_query_for_tenant(
		&self,
		tenant_id: TenantId,
		query_id: QueryId,
		trigger: CancellationTrigger,
	) -> QueryCancellation
	where
		Transport: 'static,
	{
		let cancellation = self.active_queries.cancel(tenant_id, query_id, trigger);
		if cancellation.first_request && !cancellation.remote_tasks.is_empty() {
			self.spawn_remote_cancel_fanout(trigger, cancellation.remote_tasks.clone());
		}
		cancellation
	}

	/// Resolve a caller-retained SQL/Rune mutation id without executing its
	/// statement again. Tenant scope comes from the authenticated connection.
	pub async fn resolve_mutation_outcome(
		&self,
		tenant_id: TenantId,
		operation_query_id: QueryId,
	) -> Result<ResolvedMutationOutcome, QueryError> {
		self.writer
			.resolve_mutation(
				tenant_id,
				mutation_operation_id(*operation_query_id.get().as_bytes(), 0),
			)
			.await
			.map_err(QueryError::Storage)
	}

	pub async fn resolve_ctas_outcome(
		&self,
		tenant_id: TenantId,
		operation_query_id: QueryId,
	) -> Result<Option<wendelon_client::CtasSagaEntry>, QueryError> {
		self.catalog
			.clone()
			.scoped(tenant_id)
			.resolve_ctas_saga(wendelon_client::CtasOperationId::new(
				*operation_query_id.get().as_bytes(),
			))
			.await
			.map_err(QueryError::Catalog)
	}

	pub async fn execute_query_with_context_and_config(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
		planner_config: PlannerConfig,
	) -> Result<Vec<RecordBatch>, QueryError>
	where
		Registry: 'static,
		Transport: 'static,
	{
		if logical_plan_is_distributed_read(plan) {
			let mut stream = self
				.execute_query_stream_with_context_and_config(plan, context, planner_config)
				.await?;
			let mut batches = Vec::new();
			while let Some(batch) = stream.next().await {
				batches.push(batch?);
			}
			return Ok(batches);
		}
		let admitted = self.admit_query(&context).await?;
		let query_budget = admitted.query_budget;
		let downstream = DownstreamAttemptRegistry::new(
			self.downstream_cancel_fanout_permits.clone(),
			self.downstream_cancellation_metrics.clone(),
		);
		let active_query = self.active_queries.register_with_downstream(
			context.tenant_id,
			context.query_id,
			downstream,
		);
		let active_query_handle = active_query.handle();
		let renewal = self.spawn_query_lease_renewal(active_query_handle.clone());
		let retire_remote = self.remote_retire_spawner();
		let query_id = context.query_id;
		let context = context.with_downstream_cancellation(Some(
			active_query_handle.downstream_cancellation_context(),
		));
		let result = self
			.execute_query_with_config_admission(
				plan,
				context,
				query_budget,
				Some(active_query_handle.clone()),
				planner_config,
				&mut None,
			)
			.await;
		// ADR 628 B1.C: the atomic successful-completion gate.
		let result = commit_query_success(&active_query_handle, result);
		let cause = if active_query_handle.lease_failure_worker().is_some() {
			QueryTerminalCause::Failed
		} else if active_query_handle.is_cancelled() {
			QueryTerminalCause::Cancelled
		} else if result.is_ok() {
			QueryTerminalCause::Success
		} else {
			QueryTerminalCause::Failed
		};
		active_query_handle.mark_terminal();
		retire_remote(query_id, cause, active_query_handle.remote_workers());
		report_renewal_cleanup(query_id, renewal.shutdown().await);
		drop(active_query);
		drop(admitted);
		result
	}

	pub async fn execute_query_stream_with_context_and_config(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
		planner_config: PlannerConfig,
	) -> Result<DistributedResultStream, QueryError>
	where
		Registry: 'static,
		Transport: 'static,
	{
		if !logical_plan_is_distributed_read(plan) {
			return Err(QueryError::Internal {
				message: "execute_query_stream supports read queries only in ADR 120 V1"
					.to_string(),
			});
		}
		let admitted = self.admit_query(&context).await?;
		let query_budget = admitted.query_budget;
		let downstream = DownstreamAttemptRegistry::new(
			self.downstream_cancel_fanout_permits.clone(),
			self.downstream_cancellation_metrics.clone(),
		);
		let active_query = self.active_queries.register_with_downstream(
			context.tenant_id,
			context.query_id,
			downstream,
		);
		let active_query_handle = active_query.handle();
		let renewal = self.spawn_query_lease_renewal(active_query_handle.clone());
		let retire_remote = self.remote_retire_spawner();
		let query_id = context.query_id;
		let context = context.with_downstream_cancellation(Some(
			active_query_handle.downstream_cancellation_context(),
		));
		let (sender, receiver) = mpsc::channel(DISTRIBUTED_RESULT_STREAM_DEPTH);
		let batch_sent = Arc::new(AtomicBool::new(false));
		let cancel_remote = self.remote_cancel_spawner();
		let driver_cancel_remote = cancel_remote.clone();
		let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
		let registry = Arc::clone(&self.registry);
		let memory = Arc::clone(&self.memory);
		let transport = Arc::clone(&self.transport);
		let config = planner_config;
		let plan = plan.clone();
		let stream_active_query = active_query_handle.clone();
		let telemetry_retention = self.task_telemetry_retention.clone();
		tokio::spawn(async move {
			let _admitted = admitted;
			let _active_query = active_query;
			let result = run_streaming_read_driver(
				plan.clone(),
				catalog,
				registry,
				memory,
				transport,
				config,
				context,
				query_budget,
				active_query_handle.clone(),
				sender.clone(),
				batch_sent,
				telemetry_retention,
			)
			.await;
			// ADR 628 B1.C: the atomic successful-completion gate. Results are fully local now.
			let result = commit_query_success(&active_query_handle, result);
			let cause = if active_query_handle.lease_failure_worker().is_some() {
				QueryTerminalCause::Failed
			} else if active_query_handle.is_cancelled() {
				QueryTerminalCause::Cancelled
			} else if result.is_ok() {
				QueryTerminalCause::Success
			} else {
				QueryTerminalCause::Failed
			};
			if let Err(error) = result {
				let _ = sender.send(Err(error)).await;
			}
			if active_query_handle.is_cancelled() {
				let (first_request, remote_tasks) =
					active_query_handle.cancel(CancellationTrigger::ClientDisconnect);
				if first_request && !remote_tasks.is_empty() {
					driver_cancel_remote(CancellationTrigger::ClientDisconnect, remote_tasks);
				}
			}
			active_query_handle.mark_terminal();
			drop(_active_query);
			drop(_admitted);
			retire_remote(query_id, cause, active_query_handle.remote_workers());
			// ADR 628 B1.C: renewal admission is sealed and the owned renewal shutdown is awaited and
			// reported BEFORE the final sender is released, so a normal end of stream follows the cleanup
			// receipt. A failure was already published above (prompt); its cleanup completes here.
			report_renewal_cleanup(query_id, renewal.shutdown().await);
			drop(sender);
		});
		Ok(DistributedResultStream::new(
			receiver,
			stream_active_query,
			cancel_remote,
		))
	}

	fn remote_cancel_spawner(&self) -> RemoteCancelSpawner
	where
		Transport: 'static,
	{
		let transport = Arc::clone(&self.transport);
		let memory = Arc::clone(&self.memory);
		let metrics = self.cancellation_metrics.clone();
		let permits = Arc::clone(&self.cancel_fanout_permits);
		Arc::new(move |trigger, remote_tasks| {
			spawn_remote_cancel_fanout_task(
				Arc::clone(&transport),
				Arc::clone(&memory),
				metrics.clone(),
				Arc::clone(&permits),
				trigger,
				remote_tasks,
			);
		})
	}

	fn spawn_query_lease_renewal(&self, active_query: ActiveQueryHandle) -> RenewalOwner
	where
		Transport: 'static,
	{
		// The only use of the renewal seam (S1).
		#[allow(unused_mut)]
		let mut owner = match &self.renewal_transport {
			Some(renewal) => spawn_query_lease_renewal_task(
				Arc::clone(renewal),
				Arc::clone(&self.memory),
				active_query,
			),
			None => spawn_query_lease_renewal_task(
				Arc::clone(&self.transport),
				Arc::clone(&self.memory),
				active_query,
			),
		};
		#[cfg(test)]
		{
			owner.hook = self.renewal_cleanup_hook.clone();
		}
		owner
	}

	fn remote_retire_spawner(&self) -> RemoteRetireSpawner
	where
		Transport: 'static,
	{
		let transport = Arc::clone(&self.transport);
		let memory = Arc::clone(&self.memory);
		Arc::new(move |query_id, cause, workers| {
			spawn_remote_retire_fanout_task(
				Arc::clone(&transport),
				Arc::clone(&memory),
				query_id,
				cause,
				workers,
			);
		})
	}

	async fn execute_explain_analyze_distributed(
		&self,
		catalog: &dyn Catalog,
		input: &LogicalPlan,
		context: &ExecutionContext,
		query_budget: QueryBudget,
		active_query: Option<ActiveQueryHandle>,
		planner_config: &PlannerConfig,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let planner_config = distributed_planner_config(planner_config.clone());
		let binding = bind_explain_analyze_with_statement_timestamp(
			input,
			catalog,
			&planner_config,
			context.statement_timestamp_nanos,
		)
		.await?;
		let context = context
			.clone()
			.with_join_runtime_observation_mode(analyze_observation_mode(
				binding.replay_ids.clone(),
			));
		let output = self
			.execute_bound_distributed_attempt_with_telemetry(
				binding.plan.clone(),
				&context,
				query_budget,
				active_query,
				0,
			)
			.await
			.map_err(DistributedAttemptError::into_query_error)?;
		let observations = join_observations_from_task_telemetry(&output.task_telemetry);
		let analyze_plan = render_explain_analyze(binding, &observations)?;
		materialize_system_table_plan(analyze_plan)
	}

	async fn execute_query_with_admission(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
		query_budget: QueryBudget,
		active_query: Option<ActiveQueryHandle>,
		receipt: &mut Option<MaterializationReceipt>,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let planner_config = distributed_planner_config(self.config.clone());
		let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
		if let LogicalPlan::ExplainAnalyze { input } = plan {
			return self
				.execute_explain_analyze_distributed(
					catalog.as_ref(),
					input,
					&context,
					query_budget,
					active_query,
					&planner_config,
				)
				.await;
		}
		if matches!(
			plan,
			LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::DropTable { .. }
		) {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&planner_config,
				context.statement_timestamp_nanos,
			)
			.await?;
			return execute_catalog_plan(catalog.as_ref(), self.admin.as_ref(), &context, &bound)
				.await;
		}
		if matches!(plan, LogicalPlan::Insert { .. }) {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&planner_config,
				context.statement_timestamp_nanos,
			)
			.await?;
			let source_executor = DistributedMutationSourceExecutor {
				coordinator: self,
				context: &context,
				query_budget,
				active_query: active_query.clone(),
			};
			let result = execute_write_plan_with_source(
				catalog.as_ref(),
				self.writer.as_ref(),
				self.deleter.as_ref(),
				self.admin.as_ref(),
				&context,
				mutation_operation_id(*context.query_id.get().as_bytes(), 0),
				&source_executor,
				&bound,
				receipt,
			)
			.await;
			if let Err(error) = &result {
				invalidate_descriptor_evolution_on_error(catalog.as_ref(), error).await?;
			}
			return result;
		}
		if matches!(
			plan,
			LogicalPlan::InsertSelect { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
		) {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&planner_config,
				context.statement_timestamp_nanos,
			)
			.await?;
			let source_executor = DistributedMutationSourceExecutor {
				coordinator: self,
				context: &context,
				query_budget,
				active_query: active_query.clone(),
			};
			let result = execute_write_plan_with_source(
				catalog.as_ref(),
				self.writer.as_ref(),
				self.deleter.as_ref(),
				self.admin.as_ref(),
				&context,
				mutation_operation_id(*context.query_id.get().as_bytes(), 0),
				&source_executor,
				&bound,
				receipt,
			)
			.await;
			if let Err(error) = &result {
				invalidate_descriptor_evolution_on_error(catalog.as_ref(), error).await?;
			}
			return result;
		}

		let mut planner_config = planner_config;
		let mut retried_broadcast_replay = false;
		let mut fragment_id_namespace = 0;
		let mut retried_descriptor_evolution = false;
		let mut invalidated = Vec::new();
		loop {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&planner_config,
				context.statement_timestamp_nanos,
			)
			.await?;
			if !invalidated.is_empty() {
				reject_replacement_object(catalog.as_ref(), &invalidated).await?;
			}
			match self
				.execute_bound_distributed_attempt(
					bound,
					&context,
					query_budget,
					active_query.clone(),
					fragment_id_namespace,
				)
				.await
			{
				Ok(results) => return Ok(results),
				Err(DistributedAttemptError::Scheduler { error, stages }) => {
					if !retried_broadcast_replay {
						if let Some(force_hash_join_ids) =
							broadcast_cap_replay_trigger(&error, &stages)
						{
							planner_config.force_hash_join_ids = force_hash_join_ids;
							fragment_id_namespace =
								next_fragment_id_namespace(fragment_id_namespace)?;
							retried_broadcast_replay = true;
							continue;
						}
					}
					let error =
						DistributedAttemptError::Scheduler { error, stages }.into_query_error();
					if descriptor_evolved_table_id(&error).is_some()
						&& !retried_descriptor_evolution
					{
						if let Some(stale) =
							invalidate_descriptor_evolution(catalog.as_ref(), &error).await?
						{
							invalidated.push(stale);
						}
						retried_descriptor_evolution = true;
						continue;
					}
					if descriptor_evolved_table_id(&error).is_some() {
						return Err(QueryError::SchemaInstability);
					}
					return Err(error);
				}
				Err(error) => {
					let error = error.into_query_error();
					if descriptor_evolved_table_id(&error).is_some()
						&& !retried_descriptor_evolution
					{
						if let Some(stale) =
							invalidate_descriptor_evolution(catalog.as_ref(), &error).await?
						{
							invalidated.push(stale);
						}
						retried_descriptor_evolution = true;
						continue;
					}
					if descriptor_evolved_table_id(&error).is_some() {
						return Err(QueryError::SchemaInstability);
					}
					return Err(error);
				}
			}
		}
	}

	async fn execute_query_with_config_admission(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
		query_budget: QueryBudget,
		active_query: Option<ActiveQueryHandle>,
		planner_config: PlannerConfig,
		receipt: &mut Option<MaterializationReceipt>,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let planner_config = distributed_planner_config(planner_config);
		let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
		if let LogicalPlan::ExplainAnalyze { input } = plan {
			return self
				.execute_explain_analyze_distributed(
					catalog.as_ref(),
					input,
					&context,
					query_budget,
					active_query,
					&planner_config,
				)
				.await;
		}
		if matches!(
			plan,
			LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::DropTable { .. }
		) {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&planner_config,
				context.statement_timestamp_nanos,
			)
			.await?;
			return execute_catalog_plan(catalog.as_ref(), self.admin.as_ref(), &context, &bound)
				.await;
		}
		if matches!(plan, LogicalPlan::Insert { .. }) {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&planner_config,
				context.statement_timestamp_nanos,
			)
			.await?;
			let source_executor = DistributedMutationSourceExecutor {
				coordinator: self,
				context: &context,
				query_budget,
				active_query: active_query.clone(),
			};
			let result = execute_write_plan_with_source(
				catalog.as_ref(),
				self.writer.as_ref(),
				self.deleter.as_ref(),
				self.admin.as_ref(),
				&context,
				mutation_operation_id(*context.query_id.get().as_bytes(), 0),
				&source_executor,
				&bound,
				receipt,
			)
			.await;
			if let Err(error) = &result {
				invalidate_descriptor_evolution_on_error(catalog.as_ref(), error).await?;
			}
			return result;
		}
		if matches!(
			plan,
			LogicalPlan::InsertSelect { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
		) {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&planner_config,
				context.statement_timestamp_nanos,
			)
			.await?;
			let source_executor = DistributedMutationSourceExecutor {
				coordinator: self,
				context: &context,
				query_budget,
				active_query: active_query.clone(),
			};
			let result = execute_write_plan_with_source(
				catalog.as_ref(),
				self.writer.as_ref(),
				self.deleter.as_ref(),
				self.admin.as_ref(),
				&context,
				mutation_operation_id(*context.query_id.get().as_bytes(), 0),
				&source_executor,
				&bound,
				receipt,
			)
			.await;
			if let Err(error) = &result {
				invalidate_descriptor_evolution_on_error(catalog.as_ref(), error).await?;
			}
			return result;
		}

		let mut planner_config = planner_config;
		let mut retried_broadcast_replay = false;
		let mut fragment_id_namespace = 0;
		let mut retried_descriptor_evolution = false;
		let mut invalidated = Vec::new();
		loop {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&planner_config,
				context.statement_timestamp_nanos,
			)
			.await?;
			if !invalidated.is_empty() {
				reject_replacement_object(catalog.as_ref(), &invalidated).await?;
			}
			match self
				.execute_bound_distributed_attempt(
					bound,
					&context,
					query_budget,
					active_query.clone(),
					fragment_id_namespace,
				)
				.await
			{
				Ok(results) => return Ok(results),
				Err(DistributedAttemptError::Scheduler { error, stages }) => {
					if !retried_broadcast_replay {
						if let Some(force_hash_join_ids) =
							broadcast_cap_replay_trigger(&error, &stages)
						{
							planner_config.force_hash_join_ids = force_hash_join_ids;
							fragment_id_namespace =
								next_fragment_id_namespace(fragment_id_namespace)?;
							retried_broadcast_replay = true;
							continue;
						}
					}
					let error =
						DistributedAttemptError::Scheduler { error, stages }.into_query_error();
					if descriptor_evolved_table_id(&error).is_some()
						&& !retried_descriptor_evolution
					{
						if let Some(stale) =
							invalidate_descriptor_evolution(catalog.as_ref(), &error).await?
						{
							invalidated.push(stale);
						}
						retried_descriptor_evolution = true;
						continue;
					}
					if descriptor_evolved_table_id(&error).is_some() {
						return Err(QueryError::SchemaInstability);
					}
					return Err(error);
				}
				Err(error) => {
					let error = error.into_query_error();
					if descriptor_evolved_table_id(&error).is_some()
						&& !retried_descriptor_evolution
					{
						if let Some(stale) =
							invalidate_descriptor_evolution(catalog.as_ref(), &error).await?
						{
							invalidated.push(stale);
						}
						retried_descriptor_evolution = true;
						continue;
					}
					if descriptor_evolved_table_id(&error).is_some() {
						return Err(QueryError::SchemaInstability);
					}
					return Err(error);
				}
			}
		}
	}

	async fn admit_query(&self, context: &ExecutionContext) -> Result<AdmittedQuery, QueryError> {
		let query_budget = self
			.tenant_budgets
			.query_budget_for_base(context.tenant_id, self.query_budget);
		let reservation = match self
			.tenant_admission
			.try_admit(context.tenant_id, query_budget.memory_per_task)
		{
			AdmissionAttempt::Granted(reservation) => reservation,
			AdmissionAttempt::Queued(mut queued) => {
				let wait = self
					.admission_policy
					.map(|policy| Duration::from_millis(u64::from(policy.max_queue_wait_ms.get())))
					.unwrap_or(Duration::ZERO);
				match tokio::time::timeout(wait, queued.recv()).await {
					Ok(Ok(reservation)) => reservation,
					Ok(Err(rejection)) => {
						return Err(self.admission_rejection_query_error(rejection));
					}
					Err(_) => {
						queued.cancel_due_to_timeout();
						return Err(self.admission_rejection_query_error(
							TenantAdmissionRejection::QueueTimeout {
								tenant_id: context.tenant_id,
							},
						));
					}
				}
			}
			AdmissionAttempt::Rejected(rejection) => {
				return Err(self.admission_rejection_query_error(rejection));
			}
		};
		Ok(AdmittedQuery {
			query_budget,
			_reservation: reservation,
		})
	}

	fn admission_rejection_query_error(&self, rejection: TenantAdmissionRejection) -> QueryError {
		match rejection {
			TenantAdmissionRejection::Memory {
				tenant_id,
				rejection,
			} => {
				self.admission_rejections
					.increment(tenant_id, AdmissionResource::Memory);
				budget_rejection_query_error(rejection)
			}
			TenantAdmissionRejection::Concurrency(rejection) => {
				self.admission_rejections
					.increment(rejection.tenant_id, AdmissionResource::Concurrency);
				concurrency_rejection_query_error(rejection)
			}
			TenantAdmissionRejection::RateLimited { tenant_id } => {
				self.admission_rejections
					.increment(tenant_id, AdmissionResource::Rate);
				resource_exhausted_query_error(format!(
					"tenant query rate limit exhausted for tenant {}",
					tenant_id.0
				))
			}
			TenantAdmissionRejection::QueueDisabled { tenant_id } => {
				resource_exhausted_query_error(format!(
					"tenant admission queue disabled for tenant {}",
					tenant_id.0
				))
			}
			TenantAdmissionRejection::QueueFull { tenant_id } => resource_exhausted_query_error(
				format!("tenant admission queue full for tenant {}", tenant_id.0),
			),
			TenantAdmissionRejection::QueueTimeout { tenant_id } => resource_exhausted_query_error(
				format!("tenant admission queue timeout for tenant {}", tenant_id.0),
			),
			TenantAdmissionRejection::RequestTooLarge { tenant_id } => {
				resource_exhausted_query_error(format!(
					"tenant admission request cannot fit owned plus borrowed capacity for tenant {}",
					tenant_id.0
				))
			}
			TenantAdmissionRejection::GrantCanceled { tenant_id } => {
				resource_exhausted_query_error(format!(
					"tenant admission grant canceled for tenant {}",
					tenant_id.0
				))
			}
		}
	}

	async fn execute_bound_distributed_with_context(
		&self,
		bound: ExecutionPlan,
		context: &ExecutionContext,
		query_budget: QueryBudget,
		active_query: Option<ActiveQueryHandle>,
	) -> Result<Vec<RecordBatch>, QueryError> {
		self.execute_bound_distributed_attempt(bound, context, query_budget, active_query, 0)
			.await
			.map_err(DistributedAttemptError::into_query_error)
	}

	async fn execute_bound_distributed_attempt(
		&self,
		bound: ExecutionPlan,
		context: &ExecutionContext,
		query_budget: QueryBudget,
		active_query: Option<ActiveQueryHandle>,
		fragment_id_namespace: u32,
	) -> Result<Vec<RecordBatch>, DistributedAttemptError> {
		Ok(self
			.execute_bound_distributed_attempt_with_telemetry(
				bound,
				context,
				query_budget,
				active_query,
				fragment_id_namespace,
			)
			.await?
			.final_results)
	}

	async fn execute_bound_distributed_attempt_with_telemetry(
		&self,
		bound: ExecutionPlan,
		context: &ExecutionContext,
		query_budget: QueryBudget,
		active_query: Option<ActiveQueryHandle>,
		fragment_id_namespace: u32,
	) -> Result<DistributedAttemptOutput, DistributedAttemptError> {
		// This attempt receives an already-bound plan, so its pre-dispatch span covers
		// fragment and scheduler construction only — narrower than the streaming
		// driver's, which also owns binding. The receipt names the owner, so a
		// consumer can tell the two apart rather than assuming one boundary.
		let pre_dispatch_started = std::time::Instant::now();
		let stages = fragment_plan(
			bound,
			self.fragment_config(context, query_budget)
				.with_id_namespace(fragment_id_namespace),
		)
		.map_err(DistributedAttemptError::Fragment)?;
		let mut scheduler = DistributedScheduler::new(self.registry.clone())
			.with_max_in_flight_tasks(query_budget.max_in_flight_tasks);
		let mut dispatcher =
			NetworkTaskDispatcher::new(self.memory.clone(), self.transport.clone())
				.with_active_query(active_query.clone());
		let pre_dispatch_micros = duration_micros(pre_dispatch_started);
		let dispatch_loop_started = std::time::Instant::now();
		let run = match scheduler.dispatch_loop(&stages, &mut dispatcher).await {
			Ok(run) => run,
			Err(error) => {
				if active_query
					.as_ref()
					.is_some_and(ActiveQueryHandle::is_cancelled)
				{
					scheduler.cancel_query(context.query_id);
				}
				return Err(DistributedAttemptError::Scheduler { error, stages });
			}
		};
		// Opened after the loop returns — not before the await. A clock started before
		// `dispatch_loop` measures the loop itself, which is the whole query.
		let dispatch_loop_wall_micros = duration_micros(dispatch_loop_started);
		let dispatch_returned = std::time::Instant::now();
		retain_task_telemetry_report(
			&self.task_telemetry_retention,
			context.query_id,
			&stages,
			&run.telemetry.task_telemetry,
			QueryLifecycleSpans {
				pre_dispatch_micros,
				dispatch_loop_wall_micros,
				post_dispatch_finalization_micros: duration_micros(dispatch_returned),
			},
		);
		Ok(DistributedAttemptOutput {
			final_results: run.final_results,
			task_telemetry: run.telemetry.task_telemetry,
		})
	}

	fn spawn_remote_cancel_fanout(
		&self,
		trigger: CancellationTrigger,
		remote_tasks: Vec<RemoteTaskCancellation>,
	) where
		Transport: 'static,
	{
		spawn_remote_cancel_fanout_task(
			Arc::clone(&self.transport),
			Arc::clone(&self.memory),
			self.cancellation_metrics.clone(),
			Arc::clone(&self.cancel_fanout_permits),
			trigger,
			remote_tasks,
		);
	}

	fn fragment_config(
		&self,
		context: &ExecutionContext,
		query_budget: QueryBudget,
	) -> FragmentPlanConfig {
		FragmentPlanConfig::new(
			context.query_id,
			TaskExecutionContext::from_execution_context(context),
			TaskBudget::from_query_budget(query_budget),
		)
		.with_join_runtime_observation_mode(context.join_runtime_observation_mode.clone())
		.with_max_in_flight_tasks(query_budget.max_in_flight_tasks)
		// ADR 537: one source for the runtime-filter mode. Both fragmentation paths and
		// the census preflight read it from the planner config resolved at query entry.
		.with_runtime_filters_enabled(self.config.runtime_filters_enabled)
	}
}

async fn run_streaming_read_driver<Registry, Transport>(
	plan: LogicalPlan,
	catalog: Arc<dyn Catalog>,
	registry: Arc<Registry>,
	memory: Arc<MemoryManager>,
	transport: Arc<Transport>,
	config: PlannerConfig,
	context: ExecutionContext,
	query_budget: QueryBudget,
	active_query: ActiveQueryHandle,
	sender: mpsc::Sender<Result<RecordBatch, QueryError>>,
	batch_sent: Arc<AtomicBool>,
	telemetry_retention: Option<TaskTelemetryRetention>,
) -> Result<(), QueryError>
where
	Registry: WorkerRegistry + ?Sized + 'static,
	Transport: ToronTransport + ?Sized + 'static,
{
	// ADR 489: this entry always fragments the bound plan. A caller may tune planner
	// policy here, but it may not assert the local-only admission signal across the
	// distributed boundary.
	let mut config = distributed_planner_config(config);
	let measure_runtime_verdict = should_measure_runtime_verdict(
		config.measured_accrual_sample_rate,
		&config.measured_accrual_counter,
	);
	let mut retried_broadcast_replay = false;
	let mut fragment_id_namespace = 0;
	let mut retried_descriptor_evolution = false;
	let mut invalidated = Vec::new();
	loop {
		// ADR 554 §3 B2: the pre-dispatch span opens at the start of the attempt and
		// closes at the dispatcher call below. A retry re-binds and re-fragments, so
		// the clock is per-attempt rather than per-query — the figure describes the
		// work that actually preceded the dispatch that ran.
		let pre_dispatch_started = std::time::Instant::now();
		let measured_binding = if measure_runtime_verdict {
			Some(
				bind_explain_analyze_with_statement_timestamp(
					&plan,
					catalog.as_ref(),
					&config,
					context.statement_timestamp_nanos,
				)
				.await
				.map_err(QueryError::Binding)?,
			)
		} else {
			None
		};
		let bound = match &measured_binding {
			Some(binding) => binding.plan.clone(),
			None => bind_with_statement_timestamp(
				&plan,
				catalog.as_ref(),
				&config,
				context.statement_timestamp_nanos,
			)
			.await
			.map_err(QueryError::Binding)?,
		};
		if !invalidated.is_empty() {
			reject_replacement_object(catalog.as_ref(), &invalidated).await?;
		}
		let execution_context = match &measured_binding {
			Some(binding) => {
				context
					.clone()
					.with_join_runtime_observation_mode(analyze_observation_mode(
						binding.replay_ids.clone(),
					))
			}
			None => context.clone(),
		};
		let stages = fragment_plan(
			bound,
			FragmentPlanConfig::new(
				execution_context.query_id,
				TaskExecutionContext::from_execution_context(&execution_context),
				TaskBudget::from_query_budget(query_budget),
			)
			.with_join_runtime_observation_mode(
				execution_context.join_runtime_observation_mode.clone(),
			)
			.with_max_in_flight_tasks(query_budget.max_in_flight_tasks)
			.with_runtime_filters_enabled(config.runtime_filters_enabled)
			.with_id_namespace(fragment_id_namespace),
		)
		.map_err(fragment_error)?;
		let mut scheduler = DistributedScheduler::new(registry.clone())
			.with_max_in_flight_tasks(query_budget.max_in_flight_tasks);
		let dispatcher = NetworkTaskDispatcher::new(memory.clone(), transport.clone())
			.with_active_query(Some(active_query.clone()))
			.with_final_result_sender(Some(sender.clone()), Some(batch_sent.clone()));
		let pre_dispatch_micros = duration_micros(pre_dispatch_started);
		let dispatch_loop_started = std::time::Instant::now();
		match scheduler.dispatch_loop(&stages, &dispatcher).await {
			Ok(run) => {
				let dispatch_loop_wall_micros = duration_micros(dispatch_loop_started);
				let dispatch_returned = std::time::Instant::now();
				let retain_report = |post_dispatch_finalization_micros: u64| {
					retain_task_telemetry_report(
						&telemetry_retention,
						execution_context.query_id,
						&stages,
						&run.telemetry.task_telemetry,
						QueryLifecycleSpans {
							pre_dispatch_micros,
							dispatch_loop_wall_micros,
							post_dispatch_finalization_micros,
						},
					);
				};
				if let Some(binding) = measured_binding.as_ref() {
					let observations =
						join_observations_from_task_telemetry(&run.telemetry.task_telemetry);
					let mut touched = Vec::new();
					for verdict in measured_explain_analyze_verdicts(binding, &observations)
						.map_err(QueryError::Binding)?
					{
						touched.extend(config.runtime_evidence.record_verdict(
							verdict.kind,
							verdict.verdict,
							verdict.provenance,
						));
					}
					config.runtime_evidence.flush_keys(touched).await;
				}
				// Retained after the post-loop work, so the finalization span covers it.
				// A verdict error above now short-circuits before retention; the query
				// fails in that case regardless, so the caller sees the error rather
				// than a missing report.
				retain_report(duration_micros(dispatch_returned));
				return Ok(());
			}
			Err(error) => {
				if active_query.is_cancelled() {
					scheduler.cancel_query(execution_context.query_id);
				}
				if !retried_broadcast_replay && !batch_sent.load(Ordering::SeqCst) {
					if let Some(force_hash_join_ids) = broadcast_cap_replay_trigger(&error, &stages)
					{
						config.force_hash_join_ids = force_hash_join_ids;
						fragment_id_namespace = next_fragment_id_namespace(fragment_id_namespace)?;
						retried_broadcast_replay = true;
						continue;
					}
				}
				let error = scheduler_error(error);
				if descriptor_evolved_table_id(&error).is_some()
					&& !retried_descriptor_evolution
					&& !batch_sent.load(Ordering::SeqCst)
				{
					if let Some(stale) =
						invalidate_descriptor_evolution(catalog.as_ref(), &error).await?
					{
						invalidated.push(stale);
					}
					retried_descriptor_evolution = true;
					continue;
				}
				if descriptor_evolved_table_id(&error).is_some() {
					return Err(QueryError::SchemaInstability);
				}
				return Err(error);
			}
		}
	}
}

/// The distributed coordinator is never evidence that a join will execute in one
/// partition. Keep the local-only admission signal false even for callers that supply
/// an otherwise-custom planner configuration.
fn distributed_planner_config(mut config: PlannerConfig) -> PlannerConfig {
	config.single_partition_execution = false;
	config
}

fn spawn_remote_cancel_fanout_task<Transport>(
	transport: Arc<Transport>,
	memory: Arc<MemoryManager>,
	metrics: Option<Arc<CancellationMetrics>>,
	permits: Arc<Semaphore>,
	trigger: CancellationTrigger,
	remote_tasks: Vec<RemoteTaskCancellation>,
) where
	Transport: ToronTransport + ?Sized + 'static,
{
	let Ok(permit) = permits.try_acquire_owned() else {
		if let Some(metrics) = &metrics {
			for _ in &remote_tasks {
				metrics.record(trigger, CancellationOutcome::RemoteError);
			}
		}
		tracing::warn!("remote cancel fan-out limit exhausted");
		return;
	};
	tokio::spawn(async move {
		let _permit = permit;
		let coordinator = ToronCoordinator::new(memory);
		for task in remote_tasks {
			let stream = match tokio::time::timeout(
				REMOTE_CANCEL_CONNECT_TIMEOUT,
				transport.connect_worker(&task.worker),
			)
			.await
			{
				Ok(Ok(stream)) => stream,
				Ok(Err(error)) => {
					if let Some(metrics) = &metrics {
						metrics.record(trigger, CancellationOutcome::RemoteError);
					}
					tracing::warn!(
						error = %error,
						identity = ?task.identity,
						"remote cancel connect failed"
					);
					continue;
				}
				Err(_) => {
					if let Some(metrics) = &metrics {
						metrics.record(trigger, CancellationOutcome::RemoteTimeout);
					}
					tracing::warn!(
						identity = ?task.identity,
						"remote cancel connect timed out"
					);
					continue;
				}
			};
			match tokio::time::timeout(
				REMOTE_CANCEL_ACK_TIMEOUT,
				coordinator.cancel_task_on_worker(stream, task.identity),
			)
			.await
			{
				Ok(Ok(())) => {
					if let Some(metrics) = &metrics {
						metrics.record(trigger, CancellationOutcome::RemoteAck);
					}
				}
				Ok(Err(error)) => {
					if let Some(metrics) = &metrics {
						metrics.record(trigger, CancellationOutcome::RemoteError);
					}
					tracing::warn!(
						error = %error,
						identity = ?task.identity,
						"remote cancel acknowledgement failed"
					);
				}
				Err(_) => {
					if let Some(metrics) = &metrics {
						metrics.record(trigger, CancellationOutcome::RemoteTimeout);
					}
					tracing::warn!(
						identity = ?task.identity,
						"remote cancel acknowledgement timed out"
					);
				}
			}
		}
	});
}

/// ADR 628 B1.C: owns a query's renewal supervisor. The query's owner keeps it outside the renewal
/// tasks, so there is no task -> handle -> task ownership cycle. `Drop` is the synchronous stop
/// boundary: it closes renewal admission in the ledger (no renewal lease write is admitted after it
/// returns, even by a loop that is already runnable), seals a still-open query as dropped, and
/// signals the supervisor. The supervisor then aborts and joins every per-worker loop and completes
/// the [`RenewalCleanup`] receipt, which can be awaited after the owner is gone. `shutdown` does the
/// same and awaits it. This is local cleanup only, not remote cancellation: an exchange already on
/// the wire may still complete, and its reply refreshes nothing.
pub(crate) struct RenewalOwner {
	reliance: Arc<crate::distributed::query_reliance::QueryReliance>,
	stop: Option<tokio::sync::watch::Sender<bool>>,
	task: Option<tokio::task::JoinHandle<()>>,
	cleanup: RenewalCleanup,
	#[cfg(test)]
	hook: RenewalCleanupHook,
}

/// The renewal supervisor's cleanup receipt. Only the supervisor's explicit `done`, sent AFTER it has
/// aborted and joined every per-worker loop, certifies cleanup.
#[derive(Clone)]
pub(crate) struct RenewalCleanup(tokio::sync::watch::Receiver<bool>);

/// Renewal cleanup could not be verified: the supervisor ended (panicked, was aborted, or its
/// runtime is shutting down) without joining its loops. Dropping its `JoinSet` requests their abort
/// but does not await it, so a loop or its stream may still be live. A runtime that is going away
/// cannot provide an asynchronous cleanup guarantee at all; this reports that limit instead of
/// confirming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RenewalCleanupError {
	SupervisorLost,
	SupervisorPanicked,
}

impl RenewalCleanup {
	pub(crate) async fn finished(mut self) -> Result<(), RenewalCleanupError> {
		match self.0.wait_for(|done| *done).await {
			Ok(_) => Ok(()),
			// The sender closed without `done`: no evidence that the loops were joined.
			Err(_) => Err(RenewalCleanupError::SupervisorLost),
		}
	}
}

impl RenewalOwner {
	/// The cleanup receipt, to await after dropping the owner (the owner tests await it).
	#[cfg(test)]
	pub(crate) fn cleanup(&self) -> RenewalCleanup {
		self.cleanup.clone()
	}

	fn close(&mut self) {
		self.reliance.close_renewal();
		self.reliance
			.seal_ended(crate::distributed::query_reliance::QueryEnd::Dropped);
		if let Some(stop) = self.stop.take() {
			let _ = stop.send(true);
		}
	}

	/// Closes renewal and awaits its cleanup (every per-worker loop aborted and joined). An error
	/// means the cleanup could not be verified.
	pub(crate) async fn shutdown(mut self) -> Result<(), RenewalCleanupError> {
		self.close();
		#[cfg(test)]
		{
			if let Some(hold) = self.hook.hold.clone() {
				let _ = hold.acquire().await.map(|permit| permit.forget());
			}
			if self.hook.lose_supervisor
				&& let Some(task) = self.task.as_ref()
			{
				task.abort();
			}
		}
		let result = match self.task.take() {
			Some(task) => match task.await {
				Ok(()) => self.cleanup.clone().finished().await,
				Err(error) if error.is_panic() => Err(RenewalCleanupError::SupervisorPanicked),
				Err(_) => Err(RenewalCleanupError::SupervisorLost),
			},
			None => self.cleanup.clone().finished().await,
		};
		self.reliance.record_cleanup(match result {
			Ok(()) => "verified",
			Err(RenewalCleanupError::SupervisorLost) => "supervisor_lost",
			Err(RenewalCleanupError::SupervisorPanicked) => "supervisor_panicked",
		});
		result
	}
}

impl Drop for RenewalOwner {
	fn drop(&mut self) {
		self.close();
	}
}

/// ADR 628 B1.C: the query's result is already sealed when its renewal is cleaned up, so an
/// unverified cleanup is reported, never turned into a different query outcome.
fn report_renewal_cleanup(query_id: QueryId, cleanup: Result<(), RenewalCleanupError>) {
	if let Err(error) = cleanup {
		tracing::error!(
			?query_id,
			?error,
			"query lease renewal cleanup could not be verified"
		);
	}
}

/// ADR 628 B1.C: spawns the query's renewal supervisor. Each worker gets its own loop the first time
/// it becomes `Reliable` (enrolled through the ledger). One worker's slow or failed attempts never
/// delay or fail another's.
fn spawn_query_lease_renewal_task<Transport>(
	transport: Arc<Transport>,
	memory: Arc<MemoryManager>,
	active_query: ActiveQueryHandle,
) -> RenewalOwner
where
	Transport: ToronTransport + ?Sized + 'static,
{
	let (stop, mut stopped) = tokio::sync::watch::channel(false);
	let (done, cleanup) = tokio::sync::watch::channel(false);
	let (enroll, mut enrolled) = mpsc::unbounded_channel();
	let reliance = Arc::clone(active_query.reliance());
	reliance.set_enroller(enroll);
	let supervised = Arc::clone(&reliance);
	let task = tokio::spawn(async move {
		let reliance = supervised;
		let coordinator = Arc::new(ToronCoordinator::new(memory));
		let query_id = active_query.key().query_id;
		let mut loops = tokio::task::JoinSet::new();
		loop {
			tokio::select! {
				_ = stopped.changed() => break,
				_ = active_query.stopped() => break,
				worker = enrolled.recv() => match worker {
					Some(worker) => {
						loops.spawn(worker_renewal_loop(
							Arc::clone(&transport),
							Arc::clone(&coordinator),
							Arc::clone(&reliance),
							query_id,
							worker,
						));
					}
					None => break,
				},
			}
		}
		loops.abort_all();
		while loops.join_next().await.is_some() {}
		let _ = done.send(true);
	});
	RenewalOwner {
		reliance,
		stop: Some(stop),
		task: Some(task),
		cleanup: RenewalCleanup(cleanup),
		#[cfg(test)]
		hook: RenewalCleanupHook::default(),
	}
}

/// One renewal attempt's outcome, classified by the B1.C failure table.
enum RenewalAttempt {
	/// A validated `Applied` whose whole exchange (close included) completed, with its send marker
	/// and the whole budget's deadline it must have completed by.
	Applied {
		marker: crate::distributed::query_reliance::BootInstant,
		by: crate::distributed::query_reliance::BootInstant,
	},
	/// Transport or budget failure (including a reply or close completed after its budget): retry
	/// while the admission rule holds.
	Transient(String),
	/// Terminal for this worker.
	Fatal(crate::distributed::query_reliance::RelianceCause),
	/// A gate refused (the query is sealed, renewal is closed, the worker lapsed, or the clock
	/// faulted): the ledger already holds the outcome.
	Refused,
}

/// ADR 628 B1.C: one renewal exchange with its budgets. The budgets are boot-clock instants; tokio
/// timers only bound the waiting, and every completion is re-checked against its instant, so a
/// starved runtime that polls a ready future late cannot turn it into a refresh. Every exit after
/// the whole budget is recorded as an overrun.
async fn renewal_attempt<Transport>(
	transport: &Transport,
	coordinator: &ToronCoordinator,
	reliance: &crate::distributed::query_reliance::QueryReliance,
	query_id: QueryId,
	worker: &WorkerEntry,
) -> (Option<u64>, RenewalAttempt)
where
	Transport: ToronTransport + ?Sized,
{
	use crate::distributed::query_reliance::{ExchangeId, Phase};
	// The exact start of the exchange (the connect) on the boot clock, its ordinal and its connect
	// and whole deadlines, admitted by the ledger.
	let Ok(start) = reliance.begin_renewal_attempt(worker.worker_id) else {
		return (None, RenewalAttempt::Refused);
	};
	let exchange = ExchangeId::Renewal {
		attempt: start.attempt,
	};
	// Records how the attempt ended, including when its future is dropped (renewal cleanup aborts
	// loops mid-exchange).
	struct AttemptEnd<'a> {
		reliance: &'a crate::distributed::query_reliance::QueryReliance,
		worker: crate::distributed::identity::WorkerId,
		attempt: u64,
		recorded: bool,
	}
	impl Drop for AttemptEnd<'_> {
		fn drop(&mut self) {
			if !self.recorded {
				self.reliance
					.end_renewal_attempt(self.worker, self.attempt, "aborted", None);
			}
		}
	}
	let mut end = AttemptEnd {
		reliance,
		worker: worker.worker_id,
		attempt: start.attempt,
		recorded: false,
	};
	let attempt = renewal_exchange(
		transport,
		coordinator,
		reliance,
		query_id,
		worker,
		exchange,
		start.connect_by,
		start.whole_by,
	)
	.await;
	// Admission is not a guarantee: a starved future can finish late. Recorded on every exit.
	let attempt = match reliance.within(worker.worker_id, exchange, Phase::Whole, start.whole_by) {
		Ok(_) => attempt,
		Err(_) => RenewalAttempt::Refused,
	};
	let (outcome, reason) = match &attempt {
		RenewalAttempt::Applied { .. } => ("applied", None),
		RenewalAttempt::Transient(reason) => ("transient", Some(reason.clone())),
		RenewalAttempt::Fatal(cause) => ("fatal", Some(cause.to_string())),
		RenewalAttempt::Refused => ("refused", None),
	};
	end.recorded = true;
	reliance.end_renewal_attempt(worker.worker_id, start.attempt, outcome, reason);
	(Some(start.attempt), attempt)
}

#[allow(clippy::too_many_arguments)]
async fn renewal_exchange<Transport>(
	transport: &Transport,
	coordinator: &ToronCoordinator,
	reliance: &crate::distributed::query_reliance::QueryReliance,
	query_id: QueryId,
	worker: &WorkerEntry,
	exchange: crate::distributed::query_reliance::ExchangeId,
	connect_by: crate::distributed::query_reliance::BootInstant,
	whole_by: crate::distributed::query_reliance::BootInstant,
) -> RenewalAttempt
where
	Transport: ToronTransport + ?Sized,
{
	use crate::distributed::query_reliance::{Phase, RelianceCause};
	let params = *reliance.params();
	let started = tokio::time::Instant::now();
	let whole_deadline = started + params.whole_budget;
	let connect_deadline = (started + params.connect_budget).min(whole_deadline);
	let tag = reliance.tag(worker.worker_id, exchange);
	let mut stream = match tokio::time::timeout_at(
		connect_deadline,
		transport.connect_worker_tagged(worker, &tag),
	)
	.await
	{
		Ok(Ok(stream)) => stream,
		Ok(Err(error)) => {
			return match crate::quic::establishment_failure(&error) {
				Some(failure) if !failure.is_liveness() => {
					RenewalAttempt::Fatal(RelianceCause::Auth(failure.to_string()))
				}
				_ => RenewalAttempt::Transient(format!("connect failed: {error}")),
			};
		}
		Err(_) => return RenewalAttempt::Transient("connect budget expired".to_string()),
	};
	// A connect the runtime observed after its budget is late even if tokio returned it: no lease is
	// written, nothing is refreshed, and the overrun is recorded (even with the whole budget left).
	match reliance.within(worker.worker_id, exchange, Phase::Connect, connect_by) {
		Ok(true) => {}
		Ok(false) => {
			return RenewalAttempt::Transient("connect completed after its budget".to_string());
		}
		Err(_) => return RenewalAttempt::Refused,
	}
	// The lease-write gate: no renewal is written after closure, a seal or a lapse.
	let Ok(marker) = reliance.send_marker(worker.worker_id, exchange) else {
		return RenewalAttempt::Refused;
	};
	let Some(ack_by) = marker.checked_add(params.ack_budget) else {
		return RenewalAttempt::Fatal(RelianceCause::Arithmetic);
	};
	let ack_deadline = (tokio::time::Instant::now() + params.ack_budget).min(whole_deadline);
	match tokio::time::timeout_at(
		ack_deadline,
		coordinator.query_lease_handshake(&mut stream, query_id),
	)
	.await
	{
		Err(_) => RenewalAttempt::Transient("ACK budget expired".to_string()),
		Ok(Err(QueryError::Io(error))) => RenewalAttempt::Transient(format!("handshake: {error}")),
		Ok(Err(QueryError::Runtime(RuntimeError::QueryLeaseIgnored { reason, .. }))) => {
			// B0: a validated Ignored is terminal at the reply, late or not; the close is not
			// awaited.
			RenewalAttempt::Fatal(RelianceCause::Ignored(reason))
		}
		Ok(Err(error)) => RenewalAttempt::Fatal(RelianceCause::Protocol(error.to_string())),
		Ok(Ok(())) => {
			match reliance.within(worker.worker_id, exchange, Phase::Ack, ack_by.min(whole_by)) {
				Ok(true) => {}
				Ok(false) => {
					return RenewalAttempt::Transient("reply after the ACK budget".to_string());
				}
				Err(_) => return RenewalAttempt::Refused,
			}
			// B0: a renewal's Applied counts only once the whole exchange, close included, completes.
			match tokio::time::timeout_at(
				whole_deadline,
				ToronCoordinator::finish_lease_exchange(&mut stream),
			)
			.await
			{
				Ok(Ok(())) => RenewalAttempt::Applied {
					marker,
					by: whole_by,
				},
				Ok(Err(error)) => RenewalAttempt::Transient(format!("close failed: {error}")),
				Err(_) => RenewalAttempt::Transient("close: whole budget expired".to_string()),
			}
		}
	}
}

/// ADR 628 B1.C: one worker's renewal loop. It renews at the supporting marker + cadence, retries
/// transient failures with backoff while `now + whole_budget < bound`, and PAUSES (never exits)
/// when no attempt fits. It recomputes whenever the ledger changes (an inline dispatch `Applied`
/// may extend the bound), and stops when the worker is no longer `Reliable`, renewal is closed, the
/// query is sealed, or a clock read fails (which fails the query). Exactly one periodic exchange is
/// in flight at a time.
async fn worker_renewal_loop<Transport>(
	transport: Arc<Transport>,
	coordinator: Arc<ToronCoordinator>,
	reliance: Arc<crate::distributed::query_reliance::QueryReliance>,
	query_id: QueryId,
	worker: WorkerEntry,
) where
	Transport: ToronTransport + ?Sized,
{
	use crate::distributed::query_reliance::{Applied, ExchangeId, RenewalPlan};
	let params = *reliance.params();
	let mut backoff = params.backoff_initial;
	let mut not_before = None;
	loop {
		let changed = reliance.changed_notified();
		tokio::pin!(changed);
		changed.as_mut().enable();
		match reliance.renewal_plan(worker.worker_id, not_before) {
			RenewalPlan::Stop => return,
			RenewalPlan::SleepUntil(instant) => {
				let Ok(wait) = reliance.until(instant) else {
					return;
				};
				tokio::select! {
					_ = tokio::time::sleep(wait) => {}
					_ = changed => {}
				}
				continue;
			}
			RenewalPlan::Attempt => {}
		}
		let (attempt, outcome) = renewal_attempt(
			transport.as_ref(),
			coordinator.as_ref(),
			reliance.as_ref(),
			query_id,
			&worker,
		)
		.await;
		let Some(attempt) = attempt else {
			return;
		};
		let reason = match outcome {
			RenewalAttempt::Applied { marker, by } => {
				match reliance.on_applied(
					worker.worker_id,
					marker,
					ExchangeId::Renewal { attempt },
					by,
				) {
					Ok(Applied::Bound(_)) => {
						not_before = None;
						backoff = params.backoff_initial;
						continue;
					}
					Ok(Applied::Late) => "Applied completed after its whole budget".to_string(),
					// Sealed or lapsed: the next plan stops.
					Err(_) => continue,
				}
			}
			RenewalAttempt::Transient(reason) => reason,
			RenewalAttempt::Fatal(cause) => {
				tracing::warn!(
					cause = %cause,
					?query_id,
					worker_id = ?worker.worker_id,
					"query lease renewal refused"
				);
				reliance.on_fatal(worker.worker_id, cause);
				return;
			}
			RenewalAttempt::Refused => return,
		};
		tracing::warn!(
			reason = %reason,
			?query_id,
			worker_id = ?worker.worker_id,
			"query lease renewal attempt failed; retrying within the reliance bound"
		);
		let Ok(next) = reliance.backoff_until(worker.worker_id, attempt, backoff) else {
			return;
		};
		not_before = Some(next);
		backoff = (backoff * 2).min(params.backoff_max);
	}
}

fn spawn_remote_retire_fanout_task<Transport>(
	transport: Arc<Transport>,
	memory: Arc<MemoryManager>,
	query_id: QueryId,
	cause: QueryTerminalCause,
	workers: Vec<WorkerEntry>,
) where
	Transport: ToronTransport + ?Sized + 'static,
{
	if workers.is_empty() {
		return;
	}
	let config =
		crate::distributed::namespace_lifecycle::ShuffleLifecycleConfig::from_worker_loss_window(
			crate::distributed::partition_store::DEFAULT_WORKER_LOST_AFTER,
		)
		.expect("default shuffle lifecycle config");
	tokio::spawn(async move {
		let coordinator = ToronCoordinator::new(memory);
		let deadline = tokio::time::Instant::now() + config.lease_term();
		let mut pending = workers;
		while !pending.is_empty() && tokio::time::Instant::now() < deadline {
			let mut unacked = Vec::new();
			for worker in pending {
				let stream = tokio::time::timeout(
					REMOTE_CANCEL_CONNECT_TIMEOUT,
					transport.connect_worker(&worker),
				)
				.await;
				let acknowledged = match stream {
					Ok(Ok(stream)) => tokio::time::timeout(
						REMOTE_CANCEL_ACK_TIMEOUT,
						coordinator.retire_query_on_worker(stream, query_id, cause),
					)
					.await
					.is_ok_and(|result| result.is_ok()),
					Ok(Err(error)) => {
						tracing::warn!(
							error = %error,
							?query_id,
							worker_id = ?worker.worker_id,
							"query retirement connect failed"
						);
						false
					}
					Err(_) => false,
				};
				if !acknowledged {
					unacked.push(worker);
				}
			}
			pending = unacked;
			if !pending.is_empty() {
				tokio::time::sleep_until(
					(tokio::time::Instant::now() + Duration::from_secs(1)).min(deadline),
				)
				.await;
			}
		}
		if !pending.is_empty() {
			tracing::warn!(
				?query_id,
				unacknowledged_workers = pending.len(),
				"query retirement lease expired before every worker acknowledged"
			);
		}
	});
}

fn logical_plan_is_distributed_read(plan: &LogicalPlan) -> bool {
	!matches!(
		plan,
		LogicalPlan::CreateTable { .. }
			| LogicalPlan::CreateLogicalType { .. }
			| LogicalPlan::DropTable { .. }
			| LogicalPlan::Insert { .. }
			| LogicalPlan::InsertSelect { .. }
			| LogicalPlan::CreateTableAsSelect { .. }
			| LogicalPlan::Delete { .. }
			| LogicalPlan::Update { .. }
			| LogicalPlan::ExplainAnalyze { .. }
	)
}

fn analyze_observation_mode(replay_ids: BTreeSet<JoinReplayId>) -> JoinRuntimeObservationMode {
	JoinRuntimeObservationMode::MeasureJoins { replay_ids }
}

fn should_measure_runtime_verdict(rate: u32, counter: &AtomicU64) -> bool {
	if rate == 0 {
		return false;
	}
	let ordinal = counter.fetch_add(1, Ordering::Relaxed).saturating_add(1);
	ordinal % u64::from(rate) == 0
}

fn join_observations_from_task_telemetry(
	task_telemetry: &[TaskTelemetryRecord],
) -> Vec<JoinRuntimeObservation> {
	task_telemetry
		.iter()
		.flat_map(|record| record.telemetry.join_observations.iter().cloned())
		.collect()
}

fn join_observations_from_local_telemetry(
	telemetry: &Arc<Mutex<TaskTelemetry>>,
) -> Vec<JoinRuntimeObservation> {
	telemetry
		.lock()
		.map(|telemetry| telemetry.join_observations.clone())
		.unwrap_or_default()
}

fn materialize_system_table_plan(plan: ExecutionPlan) -> Result<Vec<RecordBatch>, QueryError> {
	let ExecutionPlan::SystemTable { schema, rows, .. } = plan else {
		return Err(QueryError::Internal {
			message: format!("expected EXPLAIN ANALYZE system table, got {plan:?}"),
		});
	};
	Ok(vec![
		materialize_scalar_rows(&schema, &rows).map_err(QueryError::Execution)?,
	])
}

pub(crate) fn broadcast_cap_replay_trigger(
	error: &SchedulerError,
	stages: &[StagePlan],
) -> Option<BTreeSet<JoinReplayId>> {
	let SchedulerError::TaskFailed {
		key,
		kind:
			TaskFailureKind::BudgetExhausted {
				dimension: BudgetDimension::BroadcastReplication,
				..
			},
	} = error
	else {
		return None;
	};
	let ids = task_key_replayable_broadcast_join_ids(stages, *key);
	(!ids.is_empty()).then_some(ids)
}

fn task_key_replayable_broadcast_join_ids(
	stages: &[StagePlan],
	key: TaskKey,
) -> BTreeSet<JoinReplayId> {
	let mut ids = BTreeSet::new();
	for stage in stages.iter().filter(|stage| stage.stage_id == key.stage_id) {
		for task in &stage.tasks {
			if TaskKey::from_identity(task.spec.identity) == key {
				collect_replayable_broadcast_join_ids(&task.spec.plan, &mut ids);
			}
		}
	}
	ids
}

fn collect_replayable_broadcast_join_ids(plan: &ExecutionPlan, ids: &mut BTreeSet<JoinReplayId>) {
	match plan {
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left,
			right,
			strategy,
			..
		} => {
			if *strategy == JoinStrategy::Broadcast
				&& *replay_eligibility == BroadcastReplayEligibility::Replayable
			{
				ids.insert(*replay_id);
			}
			collect_replayable_broadcast_join_ids(left, ids);
			collect_replayable_broadcast_join_ids(right, ids);
		}
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::KernelDensity { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => collect_replayable_broadcast_join_ids(input, ids),
		ExecutionPlan::Union { left, right, .. } => {
			collect_replayable_broadcast_join_ids(left, ids);
			collect_replayable_broadcast_join_ids(right, ids);
		}
		// ADR 255: opaque-local — broadcast-cap replay IDs only apply to cross-stage broadcast
		// joins; a recursive CTE runs as a self-contained single-task local fixpoint (and its
		// self-reference is a leaf), so its interior holds no cross-stage replay candidates.
		ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. }
		| ExecutionPlan::Range { .. }
		| ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => {}
	}
}

fn next_fragment_id_namespace(current: u32) -> Result<u32, QueryError> {
	current.checked_add(1).ok_or_else(|| QueryError::Internal {
		message: "fragment id namespace overflow during broadcast-cap replay".to_string(),
	})
}

struct AdmittedQuery {
	query_budget: QueryBudget,
	_reservation: AdmissionReservation,
}

enum DistributedAttemptError {
	Fragment(FragmentError),
	Scheduler {
		error: SchedulerError,
		stages: Vec<StagePlan>,
	},
}

impl DistributedAttemptError {
	fn into_query_error(self) -> QueryError {
		match self {
			Self::Fragment(error) => fragment_error(error),
			Self::Scheduler { error, .. } => scheduler_error(error),
		}
	}
}

fn budget_rejection_query_error(rejection: BudgetRejection) -> QueryError {
	QueryError::Execution(ExecutionError::BudgetExhausted {
		dimension: rejection.dimension,
		producer: "tenant budget admission",
		requested: rejection.requested,
		available: rejection.available,
		detail: Default::default(),
	})
}

fn concurrency_rejection_query_error(rejection: TenantConcurrencyRejection) -> QueryError {
	QueryError::Execution(ExecutionError::ConcurrencyExhausted {
		tenant_id: rejection.tenant_id,
		requested: rejection.requested,
		available: rejection.available,
	})
}

fn resource_exhausted_query_error(message: String) -> QueryError {
	QueryError::Catalog(CatalogError::ResourceExhausted { message })
}

fn fragment_error(error: FragmentError) -> QueryError {
	QueryError::Fragment(error)
}

fn scheduler_error(error: SchedulerError) -> QueryError {
	match error {
		SchedulerError::TaskFailed { kind, .. } => {
			QueryError::Runtime(crate::error::RuntimeError::TaskFailure { kind })
		}
		SchedulerError::NoLiveWorker => QueryError::Internal {
			message: "no live worker available for distributed query".to_string(),
		},
		SchedulerError::NoProgress { blocked_stages } => QueryError::Internal {
			message: format!(
				"distributed scheduler made no progress; blocked stages: {blocked_stages:?}"
			),
		},
		SchedulerError::PartitionManifestMismatch { message } => QueryError::Internal { message },
		SchedulerError::Acceptance(message) => QueryError::Internal { message },
		SchedulerError::ProtocolViolation { key, reason } => QueryError::Internal {
			message: format!("protocol violation for task {key:?}: {reason}"),
		},
		SchedulerError::NoEligibleWorker { query_id, excluded } => QueryError::Internal {
			message: format!("no eligible worker for query {query_id:?}; excluded {excluded:?}"),
		},
		SchedulerError::ReplacementLimit { key } => QueryError::Internal {
			message: format!("task {key:?} exhausted its replacement attempts"),
		},
		SchedulerError::ExclusionLimit { query_id, worker } => QueryError::Internal {
			message: format!(
				"excluding worker {worker:?} would leave query {query_id:?} no worker"
			),
		},
		SchedulerError::ConfirmationRefused { worker, reason } => QueryError::Internal {
			message: format!("confirmation of worker {worker:?} refused: {reason}"),
		},
	}
}

fn descriptor_evolved_table_id(error: &QueryError) -> Option<TableId> {
	match error {
		QueryError::Execution(ExecutionError::Storage(StorageError::DescriptorEvolved {
			table_id,
		}))
		| QueryError::Storage(StorageError::DescriptorEvolved { table_id }) => Some(*table_id),
		QueryError::Runtime(RuntimeError::TaskFailure {
			kind: TaskFailureKind::DescriptorEvolved { table_id, .. },
		}) => Some(*table_id),
		_ => None,
	}
}

async fn invalidate_descriptor_evolution(
	catalog: &dyn Catalog,
	error: &QueryError,
) -> Result<Option<InvalidatedDescriptor>, QueryError> {
	let Some(table_id) = descriptor_evolved_table_id(error) else {
		return Ok(None);
	};
	catalog
		.invalidate_table_descriptor(table_id)
		.await
		.map_err(QueryError::Catalog)
}

async fn invalidate_descriptor_evolution_on_error(
	catalog: &dyn Catalog,
	error: &QueryError,
) -> Result<(), QueryError> {
	if descriptor_evolved_table_id(error).is_some() {
		invalidate_descriptor_evolution(catalog, error).await?;
	}
	Ok(())
}

async fn reject_replacement_object(
	catalog: &dyn Catalog,
	invalidated: &[InvalidatedDescriptor],
) -> Result<(), QueryError> {
	for stale in invalidated {
		let mut same_object_seen = false;
		let mut replacement_seen = false;
		for table_ref in &stale.refs {
			match catalog.resolve_table_local(table_ref).await {
				Ok(current) if current.table == stale.table_object_id => {
					same_object_seen = true;
				}
				Ok(_) => {
					replacement_seen = true;
				}
				Err(CatalogError::TableNotFound { .. }) => {}
				Err(error) => return Err(QueryError::Catalog(error)),
			}
		}
		if !same_object_seen && replacement_seen {
			catalog.descriptor_refresh_metrics().record_replacement();
			return Err(QueryError::SchemaInstability);
		}
	}
	Ok(())
}

async fn execute_catalog_plan(
	catalog: &dyn Catalog,
	admin: &dyn StorageAdmin,
	context: &ExecutionContext,
	bound: &ExecutionPlan,
) -> Result<Vec<RecordBatch>, QueryError> {
	match bound {
		ExecutionPlan::CreateTable {
			name,
			schema,
			row_identity,
			if_not_exists,
			or_replace,
		} => {
			if *or_replace {
				// Absence is confirmed, and then nothing is dropped. Calling the drop
				// helper here would reopen the hazard from the other side: a table
				// created between this check and that call would be resolved and
				// destroyed, because the helper looks the name up again. A concurrent
				// create instead surfaces from `create_table_with_outcome` as an
				// existence error, leaving the other actor's table intact.
				refuse_destructive_or_replace(catalog, name).await?;
			}
			catalog
				.create_table_with_outcome(
					name,
					schema.clone(),
					row_identity.clone(),
					*if_not_exists,
				)
				.await?;
			Ok(Vec::new())
		}
		ExecutionPlan::CreateLogicalType {
			name,
			physical,
			refinement,
			unit,
		} => {
			let physical = data_type_to_logical_type_physical(physical).ok_or_else(|| {
				QueryError::Internal {
					message: format!(
						"unsupported CREATE LOGICAL TYPE physical carrier: {physical:?}"
					),
				}
			})?;
			admin
				.create_logical_type(CreateLogicalTypeRequest {
					tenant_id: context.tenant_id,
					name: name.clone(),
					physical,
					refinement: refinement.clone(),
					unit: unit.clone(),
				})
				.await?;
			Ok(Vec::new())
		}
		ExecutionPlan::DropTable { name, if_exists } => {
			drop_existing_table_with_storage_admin(catalog, admin, context, name, *if_exists)
				.await?;
			Ok(Vec::new())
		}
		other => Err(QueryError::Internal {
			message: format!("execute_catalog_plan called with non-DDL plan: {other:?}"),
		}),
	}
}

/// Refuse a replace that would destroy an existing table.
///
/// `drop_existing_table_with_storage_admin` drops storage and then the catalog
/// entry, both durably, before any replacement is created. Every failure after
/// it therefore destroys the original and leaves nothing in its place. CTAS now
/// uses ADR 548 staged publication and never reaches this guard; plain CREATE
/// still lacks a provisional-object lifecycle, so it remains refused before
/// mutation.
///
/// Replacing a table that does not exist is not destructive and stays allowed.
async fn refuse_destructive_or_replace(
	catalog: &dyn Catalog,
	name: &crate::catalog::TableRef,
) -> Result<(), QueryError> {
	match catalog.resolve_table_local(name).await {
		Err(CatalogError::TableNotFound { .. }) => Ok(()),
		Err(error) => Err(QueryError::Catalog(error)),
		Ok(_) => Err(QueryError::Frontend(
			crate::error::FrontendError::Unsupported {
				message: format!(
					"OR REPLACE on existing table {} is refused: the table would be \
					 dropped before its replacement is created and populated, so a \
					 later failure would destroy it with no replacement. DROP the \
					 table explicitly if that is intended.",
					query_table_name(name)
				),
			},
		)),
	}
}

async fn drop_existing_table_with_storage_admin(
	catalog: &dyn Catalog,
	admin: &dyn StorageAdmin,
	context: &ExecutionContext,
	name: &crate::catalog::TableRef,
	if_exists: bool,
) -> Result<bool, QueryError> {
	let binding = match catalog.resolve_table_local(name).await {
		Ok(binding) => binding,
		Err(CatalogError::TableNotFound { .. }) if if_exists => return Ok(false),
		Err(error) => return Err(QueryError::Catalog(error)),
	};
	match admin
		.drop_table_storage(DropTableStorageRequest {
			session_id: context.session_id,
			query_id: context.query_id,
			tenant_id: context.tenant_id,
			table_id: binding.local_table_id,
			table_object_id: binding.table,
			schema_generation: binding.schema_generation,
			physical_descriptor_fingerprint: binding.fingerprint,
			profile_hint: context.profile_hint,
			trace_context: context.trace_context,
		})
		.await
	{
		Ok(_) | Err(StorageError::TableNotFound { .. }) => {}
		Err(error) => return Err(QueryError::Storage(error)),
	}
	catalog.drop_table(name, false).await?;
	Ok(true)
}

fn query_table_name(table: &crate::catalog::TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
	}
}

fn default_distributed_execution_context() -> ExecutionContext {
	ExecutionContext::new(
		crate::ir::SessionId(1),
		QueryId::new(),
		crate::storage::TenantId(1),
		crate::storage::HlcBound(1),
	)
}

#[async_trait::async_trait]
trait MutationSourceExecutor: Sync {
	async fn execute_read_plan(&self, plan: &ExecutionPlan)
	-> Result<Vec<RecordBatch>, QueryError>;

	async fn execute_delete_candidate_plan(
		&self,
		plan: &ExecutionPlan,
	) -> Result<Vec<RecordBatch>, QueryError> {
		self.execute_read_plan(plan).await
	}
}

struct LocalMutationSourceExecutor<'a> {
	executor: &'a Executor,
}

#[async_trait::async_trait]
impl MutationSourceExecutor for LocalMutationSourceExecutor<'_> {
	async fn execute_read_plan(
		&self,
		plan: &ExecutionPlan,
	) -> Result<Vec<RecordBatch>, QueryError> {
		self.executor
			.execute_bound(plan, None)
			.await
			.map_err(QueryError::Execution)
	}

	async fn execute_delete_candidate_plan(
		&self,
		plan: &ExecutionPlan,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let context = self.executor.context().clone().with_runtime_filter(None);
		let executor = Executor::with_context(self.executor.storage_arc(), context);
		executor
			.execute_bound(plan, None)
			.await
			.map_err(QueryError::Execution)
	}
}

struct DistributedMutationSourceExecutor<'a, Registry, Transport>
where
	Registry: WorkerRegistry + ?Sized,
	Transport: ToronTransport + ?Sized,
{
	coordinator: &'a DistributedQueryCoordinator<Registry, Transport>,
	context: &'a ExecutionContext,
	query_budget: QueryBudget,
	active_query: Option<ActiveQueryHandle>,
}

#[async_trait::async_trait]
impl<Registry, Transport> MutationSourceExecutor
	for DistributedMutationSourceExecutor<'_, Registry, Transport>
where
	Registry: WorkerRegistry + ?Sized,
	Transport: ToronTransport + ?Sized,
{
	async fn execute_read_plan(
		&self,
		plan: &ExecutionPlan,
	) -> Result<Vec<RecordBatch>, QueryError> {
		self.coordinator
			.execute_bound_distributed_with_context(
				plan.clone(),
				self.context,
				self.query_budget,
				self.active_query.clone(),
			)
			.await
	}
}

fn ctas_pending_error(entry: &wendelon_client::CtasSagaEntry) -> QueryError {
	QueryError::Internal {
		message: format!(
			"CTAS operation is durably pending in phase {:?} (owner epoch {}, retry debt {}); resolve or resume it by query id",
			entry.phase, entry.owner_epoch, entry.retry_debt
		),
	}
}

async fn drop_ctas_object_storage(
	admin: &dyn StorageAdmin,
	context: &ExecutionContext,
	binding: &crate::catalog::TableBinding,
) -> Result<(), QueryError> {
	match admin
		.drop_table_storage(DropTableStorageRequest {
			session_id: context.session_id,
			query_id: context.query_id,
			tenant_id: context.tenant_id,
			table_id: binding.local_table_id,
			table_object_id: binding.table,
			schema_generation: binding.schema_generation,
			physical_descriptor_fingerprint: binding.fingerprint,
			profile_hint: context.profile_hint,
			trace_context: context.trace_context,
		})
		.await
	{
		Ok(_) | Err(StorageError::TableNotFound { .. }) => Ok(()),
		Err(error) => Err(QueryError::Storage(error)),
	}
}

async fn ctas_publication_conflict_is_proven(
	catalog: &dyn Catalog,
	entry: &wendelon_client::CtasSagaEntry,
) -> Result<bool, QueryError> {
	let name = crate::catalog::TableRef::with_schema(
		&entry.descriptor.namespace,
		&entry.descriptor.table_name,
	);
	let mut observed = catalog.resolve_table_local(&name).await;
	if matches!(observed, Err(CatalogError::TableNotFound { .. }))
		&& entry.descriptor.namespace == "default"
	{
		observed = catalog
			.resolve_table_local(&crate::catalog::TableRef::new(&entry.descriptor.table_name))
			.await;
	}
	match (entry.expected_binding, observed) {
		(wendelon_client::CtasExpectedBinding::Absent, Ok(_)) => Ok(true),
		(wendelon_client::CtasExpectedBinding::Absent, Err(CatalogError::TableNotFound { .. })) => {
			Ok(false)
		}
		(
			wendelon_client::CtasExpectedBinding::Bound {
				table_object_id,
				schema_generation,
			},
			Ok(binding),
		) => Ok(binding.table != table_object_id
			|| binding.schema_generation != schema_generation.get()),
		(
			wendelon_client::CtasExpectedBinding::Bound { .. },
			Err(CatalogError::TableNotFound { .. }),
		) => Ok(true),
		(_, Err(error)) => Err(QueryError::Catalog(error)),
	}
}

/// ADR 574: the facts a successful `materialize` reports, taken from the
/// publish result. They cannot be re-read later: `prune_terminal_detail`
/// removes a terminal saga entry whole, taking the certificate bytes, the row
/// count, and the descriptor with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationReceipt {
	pub namespace: String,
	pub table_name: String,
	pub certificate_format_version: u16,
	pub rows_written: u64,
}

/// ADR 574: build the receipt from a published saga entry. Called on both the
/// fresh publish and the resumed resolution, so a crash between `RowsCommitted`
/// and the reply does not change what the caller is told.
fn derive_materialization_receipt(
	entry: &wendelon_client::CtasSagaEntry,
	receipt: &mut Option<MaterializationReceipt>,
) -> Result<(), QueryError> {
	let Some(certificate) = entry.certificate.as_deref() else {
		return Ok(());
	};
	let decoded = boson::CanonicalRelationExpression::decode_v1(certificate).map_err(|error| {
		QueryError::Internal {
			message: format!("published materialization certificate does not decode: {error}"),
		}
	})?;
	let witnesses = crate::canonical_expression::seed_witnesses(&decoded);
	tracing::info!(
		table = %entry.descriptor.table_name,
		certificate_bytes = certificate.len(),
		seed_witnesses = ?witnesses.literal_seeds,
		seed_witnesses_complete = witnesses.complete,
		"materialization certificate published with its table"
	);
	*receipt = Some(MaterializationReceipt {
		namespace: entry.descriptor.namespace.clone(),
		table_name: entry.descriptor.table_name.clone(),
		certificate_format_version: decoded.format_version,
		rows_written: entry.row_count.unwrap_or_default(),
	});
	Ok(())
}

fn ctas_chain_write_ids(
	entry: &wendelon_client::CtasSagaEntry,
) -> Result<Vec<wendelon::schema::ClientWriteId>, QueryError> {
	let chunk_count = entry.chunk_count.ok_or_else(|| QueryError::Internal {
		message: "CTAS saga is missing its durable chunk count".to_string(),
	})?;
	(0..chunk_count)
		.map(|index| derive_ctas_chunk_write_id(entry.row_write_id, index))
		.collect()
}

fn derive_ctas_chunk_write_id(
	base: wendelon::schema::ClientWriteId,
	index: u16,
) -> Result<wendelon::schema::ClientWriteId, QueryError> {
	let sequence_no = base
		.sequence_no
		.checked_add(u64::from(index))
		.ok_or_else(|| QueryError::Internal {
			message: format!(
				"CTAS write-chain sequence range overflows at base {} + chunk {index}",
				base.sequence_no
			),
		})?;
	Ok(wendelon::schema::ClientWriteId {
		client_uuid: base.client_uuid,
		sequence_no,
		idempotency_ttl_ms: base.idempotency_ttl_ms,
		_pad: base._pad,
	})
}

async fn release_ctas_chain_pins(
	catalog: &dyn Catalog,
	writer: &dyn StorageWrite,
	context: &ExecutionContext,
	entry: &wendelon_client::CtasSagaEntry,
) -> Result<wendelon_client::CtasSagaEntry, QueryError> {
	if entry.chain_pins_released {
		return Ok(entry.clone());
	}
	for write_id in ctas_chain_write_ids(entry)? {
		writer
			.acknowledge_mutation_outcome(context.tenant_id, write_id)
			.await?;
	}
	Ok(catalog
		.mutate_ctas_saga(wendelon_client::CtasSagaMutation::MarkChainPinsReleased {
			tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
			operation_id: entry.operation_id,
			owner_epoch: entry.owner_epoch,
		})
		.await?)
}

async fn finish_ctas_after_rows_committed(
	catalog: &dyn Catalog,
	writer: &dyn StorageWrite,
	admin: &dyn StorageAdmin,
	context: &ExecutionContext,
	mut entry: wendelon_client::CtasSagaEntry,
	if_not_exists: bool,
	result_schema: &arrow::datatypes::SchemaRef,
	receipt: &mut Option<MaterializationReceipt>,
) -> Result<Vec<RecordBatch>, QueryError> {
	let tenant_id = wendelon::schema::TenantId::new(context.tenant_id.0);
	let operation_id = entry.operation_id;
	let owner_epoch = entry.owner_epoch;
	entry = release_ctas_chain_pins(catalog, writer, context, &entry).await?;

	if matches!(
		entry.phase,
		wendelon_client::CtasSagaPhase::RowsCommitted { .. }
	) {
		match catalog
			.mutate_ctas_saga(wendelon_client::CtasSagaMutation::Publish {
				tenant_id,
				operation_id,
				owner_epoch,
			})
			.await
		{
			Ok(published) => {
				// ADR 574: the receipt is derived here, from the publish result,
				// because nothing downstream can reconstruct it — the entry is
				// removed whole once terminal and past its retention.
				derive_materialization_receipt(&published, receipt)?;
				entry = published;
			}
			Err(publish_error) => {
				let resolved = catalog.resolve_ctas_saga(operation_id).await?;
				if let Some(resolved) = resolved
					&& matches!(
						resolved.phase,
						wendelon_client::CtasSagaPhase::Published
							| wendelon_client::CtasSagaPhase::RetiringOld
							| wendelon_client::CtasSagaPhase::Complete
					) {
					// The resumed publish must report the same facts the
					// original would have; the entry still carries them until
					// its terminal retention elapses.
					derive_materialization_receipt(&resolved, receipt)?;
					entry = resolved;
				} else if ctas_publication_conflict_is_proven(catalog, &entry).await? {
					let provisional = catalog
						.resolve_ctas_object(entry.descriptor.table_object_id)
						.await?;
					drop_ctas_object_storage(admin, context, &provisional).await?;
					entry = catalog
						.mutate_ctas_saga(
							wendelon_client::CtasSagaMutation::RecordPublicationConflict {
								tenant_id,
								operation_id,
								owner_epoch,
								completed_at_ms: wall_clock_millis(),
							},
						)
						.await?;
					if if_not_exists
						&& let Ok(winner) = catalog
							.resolve_table(&crate::catalog::TableRef::with_schema(
								&entry.descriptor.namespace,
								&entry.descriptor.table_name,
							))
							.await && winner.schema.as_ref() == result_schema.as_ref()
					{
						return Ok(Vec::new());
					}
					return Err(QueryError::Catalog(publish_error));
				} else {
					let _ = catalog
						.mutate_ctas_saga(wendelon_client::CtasSagaMutation::RecordRetryDebt {
							tenant_id,
							operation_id,
							owner_epoch,
						})
						.await;
					return Err(QueryError::Catalog(publish_error));
				}
			}
		}
	}

	if entry.phase == wendelon_client::CtasSagaPhase::Published {
		if let wendelon_client::CtasExpectedBinding::Bound { .. } = entry.expected_binding {
			entry = catalog
				.mutate_ctas_saga(wendelon_client::CtasSagaMutation::MarkRetiringOld {
					tenant_id,
					operation_id,
					owner_epoch,
				})
				.await?;
		} else {
			catalog
				.mutate_ctas_saga(wendelon_client::CtasSagaMutation::Complete {
					tenant_id,
					operation_id,
					owner_epoch,
					completed_at_ms: wall_clock_millis(),
				})
				.await?;
			return Ok(Vec::new());
		}
	}

	if entry.phase == wendelon_client::CtasSagaPhase::RetiringOld {
		let wendelon_client::CtasExpectedBinding::Bound {
			table_object_id, ..
		} = entry.expected_binding
		else {
			return Err(ctas_pending_error(&entry));
		};
		let old = catalog.resolve_ctas_object(table_object_id).await?;
		drop_ctas_object_storage(admin, context, &old).await?;
		catalog
			.mutate_ctas_saga(wendelon_client::CtasSagaMutation::Complete {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms: wall_clock_millis(),
			})
			.await?;
		return Ok(Vec::new());
	}

	if entry.phase == wendelon_client::CtasSagaPhase::Complete {
		// ADR 574: a completed saga is a successful materialization the caller
		// may be asking about again. Its entry still carries the published
		// facts until terminal retention removes it, so the receipt is the
		// same one the original publish produced.
		derive_materialization_receipt(&entry, receipt)?;
		return Ok(Vec::new());
	}
	Err(ctas_pending_error(&entry))
}

async fn resume_ctas_saga(
	catalog: &dyn Catalog,
	writer: &dyn StorageWrite,
	admin: &dyn StorageAdmin,
	context: &ExecutionContext,
	mut entry: wendelon_client::CtasSagaEntry,
	if_not_exists: bool,
	result_schema: &arrow::datatypes::SchemaRef,
	receipt: &mut Option<MaterializationReceipt>,
) -> Result<Vec<RecordBatch>, QueryError> {
	if entry.chunk_count.is_some() && !entry.chain_pins_released && entry.phase.is_terminal() {
		entry = release_ctas_chain_pins(catalog, writer, context, &entry).await?;
	}
	if entry.phase == wendelon_client::CtasSagaPhase::Complete {
		// ADR 574: a completed saga is a successful materialization being asked
		// about again. Its entry still carries the published facts until
		// terminal retention removes it, so the receipt is the one the original
		// publish produced.
		derive_materialization_receipt(&entry, receipt)?;
		return Ok(Vec::new());
	}
	if entry.phase == wendelon_client::CtasSagaPhase::Conflict && if_not_exists {
		let name = crate::catalog::TableRef::with_schema(
			&entry.descriptor.namespace,
			&entry.descriptor.table_name,
		);
		if let Ok(winner) = catalog.resolve_table(&name).await
			&& winner.schema.as_ref() == result_schema.as_ref()
		{
			return Ok(Vec::new());
		}
	}
	if entry.phase.is_terminal() {
		return Err(QueryError::Internal {
			message: format!(
				"CTAS operation terminated in phase {:?} with cause {:?}",
				entry.phase, entry.terminal_cause
			),
		});
	}
	let now = wall_clock_millis();
	if now < entry.owner_lease_expires_at_ms {
		return Err(ctas_pending_error(&entry));
	}
	let prior_epoch = entry.owner_epoch;
	entry = catalog
		.mutate_ctas_saga(wendelon_client::CtasSagaMutation::Claim {
			tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
			operation_id: entry.operation_id,
			expected_owner_epoch: prior_epoch,
			new_owner_epoch: prior_epoch.saturating_add(1),
			observed_now_ms: now,
			lease_expires_at_ms: now.saturating_add(CTAS_OWNER_LEASE_MS),
		})
		.await?;

	match entry.phase {
		wendelon_client::CtasSagaPhase::Provisioned => {
			if entry.chunk_count.is_some() {
				entry = release_ctas_chain_pins(catalog, writer, context, &entry).await?;
			} else {
				// Pre-0094 crash window: the old flow could pin its one root
				// immediately before MarkRowDispatched without durable declaration.
				writer
					.acknowledge_mutation_outcome(context.tenant_id, entry.row_write_id)
					.await?;
			}
			catalog
				.mutate_ctas_saga(wendelon_client::CtasSagaMutation::Reclaim {
					tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
					operation_id: entry.operation_id,
					owner_epoch: entry.owner_epoch,
					cause: wendelon_client::CtasTerminalCause::OwnerAbandonedBeforeRowDispatch,
					completed_at_ms: wall_clock_millis(),
				})
				.await?;
			Err(QueryError::Internal {
				message: "CTAS owner disappeared before row dispatch; provisional object reclaimed"
					.to_string(),
			})
		}
		wendelon_client::CtasSagaPhase::RowDispatched => {
			let mut all_committed = true;
			let mut any_known_not_committed = false;
			for write_id in ctas_chain_write_ids(&entry)? {
				match writer.resolve_mutation(context.tenant_id, write_id).await {
					Ok(resolved) => {
						let committed = resolved.resolution == MutationResolution::Present
							&& resolved.commit_knowledge
								== wendelon::schema::MutationCommitKnowledge::COMMITTED.0;
						let known_not_committed = resolved.resolution
							== MutationResolution::Present
							&& resolved.commit_knowledge
								== wendelon::schema::MutationCommitKnowledge::KNOWN_NOT_COMMITTED.0;
						all_committed &= committed;
						any_known_not_committed |= known_not_committed;
					}
					// Outcome lookup failure is indeterminate evidence, never proof that
					// a child did not commit. Keep every pin and accrue retry debt.
					Err(_) => all_committed = false,
				}
			}
			if all_committed {
				entry = catalog
					.mutate_ctas_saga(wendelon_client::CtasSagaMutation::AttestRowsCommitted {
						tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
						operation_id: entry.operation_id,
						owner_epoch: entry.owner_epoch,
						row_count: entry.row_count.ok_or_else(|| QueryError::Internal {
							message: "dispatched CTAS saga is missing its durable row count"
								.to_string(),
						})?,
					})
					.await?;
				finish_ctas_after_rows_committed(
					catalog,
					writer,
					admin,
					context,
					entry,
					if_not_exists,
					result_schema,
					receipt,
				)
				.await
			} else if any_known_not_committed {
				entry = catalog
					.mutate_ctas_saga(wendelon_client::CtasSagaMutation::Reclaim {
						tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
						operation_id: entry.operation_id,
						owner_epoch: entry.owner_epoch,
						cause: wendelon_client::CtasTerminalCause::RowKnownNotCommitted,
						completed_at_ms: wall_clock_millis(),
					})
					.await?;
				let _ = release_ctas_chain_pins(catalog, writer, context, &entry).await?;
				Err(QueryError::Internal {
					message:
						"CTAS row mutation is known not committed; provisional object reclaimed"
							.to_string(),
				})
			} else {
				entry = catalog
					.mutate_ctas_saga(wendelon_client::CtasSagaMutation::RecordRetryDebt {
						tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
						operation_id: entry.operation_id,
						owner_epoch: entry.owner_epoch,
					})
					.await?;
				Err(ctas_pending_error(&entry))
			}
		}
		wendelon_client::CtasSagaPhase::RowsCommitted { .. }
		| wendelon_client::CtasSagaPhase::Published
		| wendelon_client::CtasSagaPhase::RetiringOld => {
			finish_ctas_after_rows_committed(
				catalog,
				writer,
				admin,
				context,
				entry,
				if_not_exists,
				result_schema,
				receipt,
			)
			.await
		}
		wendelon_client::CtasSagaPhase::Complete
		| wendelon_client::CtasSagaPhase::Conflict
		| wendelon_client::CtasSagaPhase::Reclaimed => unreachable!("terminal checked above"),
	}
}

async fn execute_write_plan_with_source(
	catalog: &dyn Catalog,
	writer: &dyn StorageWrite,
	deleter: &dyn StorageDeleteRows,
	admin: &dyn StorageAdmin,
	context: &ExecutionContext,
	operation_id: wendelon::schema::ClientWriteId,
	source_executor: &dyn MutationSourceExecutor,
	bound: &ExecutionPlan,
	receipt: &mut Option<MaterializationReceipt>,
) -> Result<Vec<RecordBatch>, QueryError> {
	let logical_types = catalog.logical_type_registry();
	match bound {
		ExecutionPlan::Insert {
			local_table_id,
			table,
			fingerprint,
			schema,
			rows,
			returning,
		} => {
			let payload = write::materialize_insert_batch(schema, rows)?;
			write::validate_refined_logical_columns("INSERT", &logical_types, &payload)?;
			let returning_batches =
				project_returning_batches(returning.as_ref(), std::slice::from_ref(&payload))?;
			let returning_row_count = returning_batches
				.as_ref()
				.map(|batches| batch_row_count(batches))
				.unwrap_or(0);
			let outcome = writer
				.write(WriteRequest {
					session_id: context.session_id,
					query_id: context.query_id,
					tenant_id: context.tenant_id,
					table_id: *local_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					payload,
					profile_hint: context.profile_hint,
					trace_context: context.trace_context,
					operation_id: Some(operation_id),
					deadline: context.deadline,
				})
				.await?;
			if let Some(batches) = returning_batches {
				verify_write_returning_receipt("INSERT", outcome.rows, returning_row_count)?;
				Ok(batches)
			} else {
				Ok(vec![write::affected_count_batch(outcome.rows)?])
			}
		}
		ExecutionPlan::InsertSelect {
			local_table_id,
			table,
			fingerprint,
			schema,
			target_columns,
			source,
			returning,
		} => {
			let source_batches = source_executor.execute_read_plan(source).await?;
			let Some(payload) =
				write::materialize_insert_select_batch(schema, target_columns, &source_batches)?
			else {
				return if returning.is_some() {
					Ok(Vec::new())
				} else {
					Ok(vec![write::affected_count_batch(0)?])
				};
			};
			write::validate_refined_logical_columns("INSERT SELECT", &logical_types, &payload)?;
			let returning_batches =
				project_returning_batches(returning.as_ref(), std::slice::from_ref(&payload))?;
			let returning_row_count = returning_batches
				.as_ref()
				.map(|batches| batch_row_count(batches))
				.unwrap_or(0);
			let outcome = writer
				.write(WriteRequest {
					session_id: context.session_id,
					query_id: context.query_id,
					tenant_id: context.tenant_id,
					table_id: *local_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					payload,
					profile_hint: context.profile_hint,
					trace_context: context.trace_context,
					operation_id: Some(operation_id),
					deadline: context.deadline,
				})
				.await?;
			if let Some(batches) = returning_batches {
				verify_write_returning_receipt("INSERT SELECT", outcome.rows, returning_row_count)?;
				Ok(batches)
			} else {
				Ok(vec![write::affected_count_batch(outcome.rows)?])
			}
		}
		ExecutionPlan::Update {
			local_table_id,
			table,
			fingerprint,
			schema,
			source,
			profile_hint,
			returning,
		} => {
			let source_batches = source_executor.execute_read_plan(source).await?;
			let Some(payload) = write::materialize_update_select_batch(schema, &source_batches)?
			else {
				return if returning.is_some() {
					Ok(Vec::new())
				} else {
					Ok(vec![write::affected_count_batch(0)?])
				};
			};
			write::validate_refined_logical_columns("UPDATE", &logical_types, &payload)?;
			let returning_batches =
				project_returning_batches(returning.as_ref(), std::slice::from_ref(&payload))?;
			let returning_row_count = returning_batches
				.as_ref()
				.map(|batches| batch_row_count(batches))
				.unwrap_or(0);
			let outcome = writer
				.write(WriteRequest {
					session_id: context.session_id,
					query_id: context.query_id,
					tenant_id: context.tenant_id,
					table_id: *local_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					payload,
					profile_hint: Some(*profile_hint),
					trace_context: context.trace_context,
					operation_id: Some(operation_id),
					deadline: context.deadline,
				})
				.await?;
			if let Some(batches) = returning_batches {
				verify_write_returning_receipt("UPDATE", outcome.rows, returning_row_count)?;
				Ok(batches)
			} else {
				Ok(vec![write::affected_count_batch(outcome.rows)?])
			}
		}
		ExecutionPlan::CreateTableAsSelect {
			table,
			schema,
			intent_fingerprint,
			if_not_exists,
			or_replace,
			source,
			materialization_certificate,
		} => {
			// A certificate binds to exactly one freshly created table object. With
			// both flags false, any pre-existing binding is rejected below, so the
			// provision always carries CtasExpectedBinding::Absent.
			if materialization_certificate.is_some() && (*if_not_exists || *or_replace) {
				return Err(QueryError::Internal {
					message: "a materialization certificate requires a fresh table binding"
						.to_string(),
				});
			}
			let composite_id = wendelon_client::CtasOperationId::new(operation_id.client_uuid);
			if let Some(existing_saga) = catalog.resolve_ctas_saga(composite_id).await? {
				if existing_saga.intent_fingerprint != *intent_fingerprint {
					return Err(QueryError::Internal {
						message: "query id was reused for a different CTAS intent".to_string(),
					});
				}
				return resume_ctas_saga(
					catalog,
					writer,
					admin,
					context,
					existing_saga,
					*if_not_exists,
					schema,
					receipt,
				)
				.await;
			}
			let issued_at_ms =
				composite_id
					.issued_at_unix_millis()
					.ok_or_else(|| QueryError::Internal {
						message: "CTAS requires a caller UUIDv7 operation id".to_string(),
					})?;
			if issued_at_ms.saturating_add(wendelon_client::CTAS_SAGA_DETAIL_RETENTION_MS)
				<= wall_clock_millis()
			{
				return Err(QueryError::Internal {
					message: "CTAS operation id expired before first durable provisioning"
						.to_string(),
				});
			}

			let prior = match catalog.resolve_table(table).await {
				Ok(existing) if *if_not_exists => {
					if existing.schema.as_ref() == schema.as_ref() {
						return Ok(Vec::new());
					}
					return Err(CatalogError::SchemaMismatch {
						table: query_table_name(table),
					}
					.into());
				}
				Ok(existing) if *or_replace => Some(existing),
				Ok(_) => {
					return Err(CatalogError::TableAlreadyExists {
						table: query_table_name(table),
					}
					.into());
				}
				Err(CatalogError::TableNotFound { .. }) => None,
				Err(error) => return Err(error.into()),
			};

			// Source execution, materialization, and logical-value validation all
			// precede provisioning. A failure here leaves no saga and no object.
			let source_batches = source_executor.execute_read_plan(source).await?;
			let target_columns = (0..schema.fields().len())
				.map(|index| ColumnOrdinal(u32::try_from(index).expect("column ordinal overflow")))
				.collect::<Vec<_>>();
			let payload = match write::materialize_ctas_select_batch(
				schema,
				&target_columns,
				&source_batches,
			)? {
				Some(payload) => payload,
				None => write::materialize_empty_record_batch(schema)?,
			};
			write::validate_refined_logical_columns("CTAS", &logical_types, &payload)?;
			let row_count = u64::try_from(payload.num_rows()).unwrap_or(u64::MAX);
			let chunks = write::plan_materialization_write_chunks(&payload)?;
			let chunk_count = u16::try_from(chunks.len()).map_err(|_| QueryError::Internal {
				message: "MATERIALIZE write-chain length exceeds u16".to_string(),
			})?;
			let row_write_id =
				mutation_operation_id(operation_id.client_uuid, CTAS_ROW_WRITE_SEQUENCE_NO);
			let chunk_write_ids = (0..chunk_count)
				.map(|index| derive_ctas_chunk_write_id(row_write_id, index))
				.collect::<Result<Vec<_>, _>>()?;
			let expected_binding =
				prior
					.as_ref()
					.map_or(wendelon_client::CtasExpectedBinding::Absent, |binding| {
						wendelon_client::CtasExpectedBinding::Bound {
							table_object_id: binding.table,
							schema_generation: wendelon_client::SchemaGeneration::new(
								binding.schema_generation,
							),
						}
					});
			let prior_descriptor = prior
				.as_ref()
				.map(|binding| catalog.descriptor_for_ctas_binding(binding))
				.transpose()?;
			if context
				.deadline
				.is_some_and(|deadline| std::time::Instant::now() >= deadline)
			{
				return Err(QueryError::Internal {
					message: "CTAS deadline elapsed before provisional catalog dispatch"
						.to_string(),
				});
			}
			let owner_epoch = 1;
			let observed_now_ms = wall_clock_millis();
			let provision = match catalog
				.provision_ctas(CtasProvisionRequest {
					operation_id: composite_id,
					intent_fingerprint: *intent_fingerprint,
					name: table.clone(),
					schema: schema.clone(),
					row_identity: CreateTableRowIdentity::AppendOnly,
					expected_binding,
					prior_descriptor,
					row_write_id,
					owner_epoch,
					observed_now_ms,
					owner_lease_expires_at_ms: observed_now_ms.saturating_add(CTAS_OWNER_LEASE_MS),
					certificate: materialization_certificate.clone(),
				})
				.await
			{
				Ok(provision) => provision,
				Err(_error) if *if_not_exists => {
					if let Ok(winner) = catalog.resolve_table(table).await {
						if winner.schema.as_ref() == schema.as_ref() {
							return Ok(Vec::new());
						}
						return Err(CatalogError::SchemaMismatch {
							table: query_table_name(table),
						}
						.into());
					}
					return Err(_error.into());
				}
				Err(error) => return Err(error.into()),
			};
			let binding = provision.binding;
			let declared = catalog
				.mutate_ctas_saga(wendelon_client::CtasSagaMutation::DeclareChain {
					tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
					operation_id: composite_id,
					owner_epoch,
					row_count,
					chunk_count,
				})
				.await?;

			if context
				.deadline
				.is_some_and(|deadline| std::time::Instant::now() >= deadline)
			{
				let reclaimed = catalog
					.mutate_ctas_saga(wendelon_client::CtasSagaMutation::Reclaim {
						tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
						operation_id: composite_id,
						owner_epoch,
						cause: wendelon_client::CtasTerminalCause::DeadlineBeforeRowDispatch,
						completed_at_ms: wall_clock_millis(),
					})
					.await?;
				let _ = release_ctas_chain_pins(catalog, writer, context, &reclaimed).await?;
				return Err(QueryError::Internal {
					message: "CTAS deadline elapsed before row dispatch".to_string(),
				});
			}

			for write_id in &chunk_write_ids {
				if let Err(error) = writer
					.pin_mutation_outcome(context.tenant_id, *write_id)
					.await
				{
					// A failed pin call may still have installed its outcome before the
					// response was lost. Acknowledge the entire declared chain, not only
					// calls observed as successful, before recording cleanup complete.
					if let Err(cleanup_error) =
						release_ctas_chain_pins(catalog, writer, context, &declared).await
					{
						return Err(cleanup_error);
					}
					return Err(error.into());
				}
			}
			if let Err(error) = catalog
				.mutate_ctas_saga(wendelon_client::CtasSagaMutation::MarkRowDispatched {
					tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
					operation_id: composite_id,
					owner_epoch,
					row_count,
				})
				.await
			{
				if let Err(cleanup_error) =
					release_ctas_chain_pins(catalog, writer, context, &declared).await
				{
					return Err(cleanup_error);
				}
				return Err(error.into());
			}
			let mut written_rows = 0usize;
			for (payload, chunk_write_id) in chunks.into_iter().zip(chunk_write_ids.iter().copied())
			{
				let write_result = writer
					.write(WriteRequest {
						session_id: context.session_id,
						query_id: context.query_id,
						tenant_id: context.tenant_id,
						table_id: binding.local_table_id,
						table_object_id: binding.table,
						physical_descriptor_fingerprint: binding.fingerprint,
						payload,
						profile_hint: context.profile_hint,
						trace_context: context.trace_context,
						operation_id: Some(chunk_write_id),
						// The caller deadline belongs to this originating activation.
						// Recovery activations use durable attempt budgets instead of
						// reconstructing an Instant across restart.
						deadline: context.deadline,
					})
					.await;
				let write_outcome = match write_result {
					Ok(outcome) => outcome,
					Err(error) => {
						let mut all_committed = true;
						let mut any_known_not_committed = false;
						for write_id in &chunk_write_ids {
							match writer.resolve_mutation(context.tenant_id, *write_id).await {
								Ok(resolved) => {
									let committed = resolved.resolution
										== MutationResolution::Present && resolved
										.commit_knowledge
										== wendelon::schema::MutationCommitKnowledge::COMMITTED.0;
									let known_negative = resolved.resolution == MutationResolution::Present
										&& resolved.commit_knowledge
											== wendelon::schema::MutationCommitKnowledge::KNOWN_NOT_COMMITTED.0;
									all_committed &= committed;
									any_known_not_committed |= known_negative;
								}
								Err(_) => all_committed = false,
							}
						}
						if all_committed {
							let entry = catalog
								.mutate_ctas_saga(
									wendelon_client::CtasSagaMutation::AttestRowsCommitted {
										tenant_id: wendelon::schema::TenantId::new(
											context.tenant_id.0,
										),
										operation_id: composite_id,
										owner_epoch,
										row_count,
									},
								)
								.await?;
							return finish_ctas_after_rows_committed(
								catalog,
								writer,
								admin,
								context,
								entry,
								*if_not_exists,
								schema,
								receipt,
							)
							.await;
						}
						if any_known_not_committed {
							let reclaimed = catalog
								.mutate_ctas_saga(wendelon_client::CtasSagaMutation::Reclaim {
									tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
									operation_id: composite_id,
									owner_epoch,
									cause: wendelon_client::CtasTerminalCause::RowKnownNotCommitted,
									completed_at_ms: wall_clock_millis(),
								})
								.await?;
							let _ = release_ctas_chain_pins(catalog, writer, context, &reclaimed)
								.await?;
						} else {
							let _ = catalog
								.mutate_ctas_saga(
									wendelon_client::CtasSagaMutation::RecordRetryDebt {
										tenant_id: wendelon::schema::TenantId::new(
											context.tenant_id.0,
										),
										operation_id: composite_id,
										owner_epoch,
									},
								)
								.await;
						}
						return Err(error.into());
					}
				};
				written_rows = written_rows.saturating_add(write_outcome.rows);
			}
			let entry = catalog
				.mutate_ctas_saga(wendelon_client::CtasSagaMutation::AttestRowsCommitted {
					tenant_id: wendelon::schema::TenantId::new(context.tenant_id.0),
					operation_id: composite_id,
					owner_epoch,
					row_count,
				})
				.await?;
			debug_assert_eq!(u64::try_from(written_rows).unwrap_or(u64::MAX), row_count);
			finish_ctas_after_rows_committed(
				catalog,
				writer,
				admin,
				context,
				entry,
				*if_not_exists,
				schema,
				receipt,
			)
			.await
		}
		ExecutionPlan::Delete {
			local_table_id,
			table,
			fingerprint,
			schema,
			profile_hint,
			returning,
			..
		} => {
			let candidate_plan = derive_delete_candidate_plan(bound)?;
			let candidate_batches = source_executor
				.execute_delete_candidate_plan(&candidate_plan)
				.await?;
			let row_key_ids = delete::extract_delete_row_key_ids(&candidate_batches)?;
			let candidate_count = row_key_ids.len();
			let returning_batches = if let Some(returning) = returning.as_ref() {
				let user_batches =
					delete::delete_candidate_user_batches(&candidate_batches, schema)?;
				project_returning_batches(Some(returning), &user_batches)?
			} else {
				None
			};
			let returning_row_count = returning_batches
				.as_ref()
				.map(|batches| batch_row_count(batches))
				.unwrap_or(0);
			let outcome = delete::execute_delete_with_candidates(
				deleter,
				DeleteRowsRequest {
					session_id: context.session_id,
					query_id: context.query_id,
					tenant_id: context.tenant_id,
					table_id: *local_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					row_key_ids,
					profile_hint: Some(*profile_hint),
					trace_context: context.trace_context,
					operation_id,
					deadline: context.deadline,
				},
			)
			.await?;
			if let Some(batches) = returning_batches {
				verify_delete_returning_receipt(&outcome, candidate_count, returning_row_count)?;
				Ok(batches)
			} else {
				Ok(vec![delete::delete_count_batch(outcome.deleted)?])
			}
		}
		other => Err(QueryError::Internal {
			message: format!("execute_write_plan called with non-DML plan: {other:?}"),
		}),
	}
}

fn mutation_operation_id(
	client_uuid: [u8; 16],
	sequence_no: u64,
) -> wendelon::schema::ClientWriteId {
	wendelon::schema::ClientWriteId {
		client_uuid,
		sequence_no,
		idempotency_ttl_ms: 300_000,
		_pad: 0,
	}
}

fn wall_clock_millis() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}

fn project_returning_batches(
	returning: Option<&BoundReturningProjection>,
	input_batches: &[RecordBatch],
) -> Result<Option<Vec<RecordBatch>>, QueryError> {
	let Some(returning) = returning else {
		return Ok(None);
	};
	let mut output = Vec::new();
	for batch in input_batches {
		if batch.num_rows() == 0 {
			continue;
		}
		let columns = Executor::evaluate_bound_program_without_storage(&returning.exprs, batch)?;
		let projected = RecordBatch::try_new(returning.output_schema.clone(), columns)?;
		if projected.num_rows() != batch.num_rows() {
			return Err(QueryError::Internal {
				message: format!(
					"RETURNING projection produced {} rows from {} input rows",
					projected.num_rows(),
					batch.num_rows()
				),
			});
		}
		output.push(projected);
	}
	Ok(Some(output))
}

fn batch_row_count(batches: &[RecordBatch]) -> usize {
	batches.iter().map(RecordBatch::num_rows).sum()
}

fn verify_write_returning_receipt(
	verb: &'static str,
	storage_rows: usize,
	returned_rows: usize,
) -> Result<(), QueryError> {
	if storage_rows == returned_rows {
		return Ok(());
	}
	Err(QueryError::Internal {
		message: format!(
			"{verb} RETURNING row count {returned_rows} does not match storage receipt {storage_rows}"
		),
	})
}

fn verify_delete_returning_receipt(
	outcome: &DeleteRowsOutcome,
	candidate_count: usize,
	returned_rows: usize,
) -> Result<(), QueryError> {
	if outcome.requested == candidate_count
		&& outcome.deleted == candidate_count
		&& outcome.already_deleted == 0
		&& outcome.not_found == 0
		&& returned_rows == candidate_count
	{
		return Ok(());
	}
	Err(QueryError::Internal {
		message: format!(
			"DELETE RETURNING receipt mismatch: candidates={candidate_count} returned={returned_rows} requested={} deleted={} already_deleted={} not_found={}",
			outcome.requested, outcome.deleted, outcome.already_deleted, outcome.not_found
		),
	})
}

fn derive_delete_candidate_plan(bound: &ExecutionPlan) -> Result<ExecutionPlan, QueryError> {
	let ExecutionPlan::Delete {
		local_table_id,
		table,
		fingerprint,
		schema,
		predicate,
		pbc_program,
		profile_hint,
		returning: _,
	} = bound
	else {
		return Err(QueryError::Internal {
			message: format!("derive_delete_candidate_plan called with non-DELETE plan: {bound:?}"),
		});
	};

	let scan = ExecutionPlan::Scan {
		local_table_id: *local_table_id,
		table: *table,
		fingerprint: *fingerprint,
		schema: schema.clone(),
		projection: None,
		pbc_program: pbc_program.clone(),
		row_identity: RowIdentityProjection::Append,
		row_limit: None,
		profile_hint: *profile_hint,
		estimated_byte_size: None,
		primary_key_columns: Vec::new(),
		column_ndv: Vec::new(),
		column_range: Vec::new(),
		column_range_distributions: Vec::new(),
		column_heavy_hitters: Vec::new(),
		column_group_top_k: Vec::new(),
		column_group_ndv: Vec::new(),
	};

	if let Some(predicate) = predicate {
		Ok(ExecutionPlan::Filter {
			input: Box::new(scan),
			predicate: predicate.clone(),
			disposition: FilterDisposition::EvaluateInToron,
		})
	} else {
		Ok(scan)
	}
}

pub struct QueryCoordinator {
	catalog: Arc<dyn Catalog>,
	executor: Executor,
	config: PlannerConfig,
	writer: Arc<dyn StorageWrite>,
	deleter: Arc<dyn StorageDeleteRows>,
	admin: Arc<dyn StorageAdmin>,
	statement_clock: Arc<dyn StatementClock>,
}

impl QueryCoordinator {
	pub fn new(catalog: Arc<dyn Catalog>, executor: Executor, config: PlannerConfig) -> Self {
		Self::with_writer_deleter_and_admin(
			catalog,
			executor,
			config,
			Arc::new(UnsupportedStorageWrite),
			Arc::new(UnsupportedStorageDeleteRows),
			Arc::new(UnsupportedStorageAdmin),
		)
	}

	pub fn with_writer(
		catalog: Arc<dyn Catalog>,
		executor: Executor,
		config: PlannerConfig,
		writer: Arc<dyn StorageWrite>,
	) -> Self {
		Self::with_writer_deleter_and_admin(
			catalog,
			executor,
			config,
			writer,
			Arc::new(UnsupportedStorageDeleteRows),
			Arc::new(UnsupportedStorageAdmin),
		)
	}

	pub fn with_writer_and_deleter(
		catalog: Arc<dyn Catalog>,
		executor: Executor,
		config: PlannerConfig,
		writer: Arc<dyn StorageWrite>,
		deleter: Arc<dyn StorageDeleteRows>,
	) -> Self {
		Self::with_writer_deleter_and_admin(
			catalog,
			executor,
			config,
			writer,
			deleter,
			Arc::new(UnsupportedStorageAdmin),
		)
	}

	pub fn with_storage_admin(mut self, admin: Arc<dyn StorageAdmin>) -> Self {
		self.admin = admin;
		self
	}

	#[cfg(test)]
	fn with_statement_clock(mut self, statement_clock: Arc<dyn StatementClock>) -> Self {
		self.statement_clock = statement_clock;
		self
	}

	pub fn with_writer_deleter_and_admin(
		catalog: Arc<dyn Catalog>,
		executor: Executor,
		config: PlannerConfig,
		writer: Arc<dyn StorageWrite>,
		deleter: Arc<dyn StorageDeleteRows>,
		admin: Arc<dyn StorageAdmin>,
	) -> Self {
		Self {
			catalog,
			executor,
			config,
			writer,
			deleter,
			admin,
			statement_clock: Arc::new(SystemStatementClock),
		}
	}

	/// ADR 574: the single-node mirror of the receipt seam. Same rule as the
	/// distributed entry — the receipt is optional inside the CTAS path and
	/// non-optional here.
	pub async fn execute_materialization_with_context(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
	) -> Result<MaterializationReceipt, QueryError> {
		if !matches!(
			plan,
			LogicalPlan::CreateTableAsSelect {
				materialization_certificate: Some(_),
				..
			}
		) {
			return Err(QueryError::Internal {
				message: "execute_materialization_with_context requires a certificate-carrying \
				          CTAS"
					.to_string(),
			});
		}
		let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
		let bound = bind_with_statement_timestamp(
			plan,
			catalog.as_ref(),
			&self.config,
			context.statement_timestamp_nanos,
		)
		.await?;
		let source_executor = LocalMutationSourceExecutor {
			executor: &self.executor,
		};
		let mut receipt = None;
		execute_write_plan_with_source(
			catalog.as_ref(),
			self.writer.as_ref(),
			self.deleter.as_ref(),
			self.admin.as_ref(),
			&context,
			mutation_operation_id(*context.query_id.get().as_bytes(), 0),
			&source_executor,
			&bound,
			&mut receipt,
		)
		.await?;
		receipt.ok_or_else(|| QueryError::Internal {
			message: "materialization published without producing a receipt".to_string(),
		})
	}

	pub async fn execute_query(
		&self,
		plan: &LogicalPlan,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let statement_context = self
			.executor
			.context()
			.for_statement_with_clock(self.statement_clock.as_ref());
		let statement_timestamp_nanos = statement_context.statement_timestamp_nanos;
		let catalog = Arc::clone(&self.catalog).scoped(statement_context.tenant_id);
		if let LogicalPlan::ExplainAnalyze { input } = plan {
			return self
				.execute_explain_analyze(catalog.as_ref(), input, morsel, &statement_context)
				.await;
		}
		if matches!(
			plan,
			LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::DropTable { .. }
		) {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&self.config,
				statement_timestamp_nanos,
			)
			.await?;
			return execute_catalog_plan(
				catalog.as_ref(),
				self.admin.as_ref(),
				&statement_context,
				&bound,
			)
			.await;
		}
		if matches!(
			plan,
			LogicalPlan::Insert { .. }
				| LogicalPlan::InsertSelect { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
		) {
			let bound = bind_with_statement_timestamp(
				plan,
				catalog.as_ref(),
				&self.config,
				statement_timestamp_nanos,
			)
			.await?;
			return self.execute_write_plan(catalog.as_ref(), &bound).await;
		}

		let measure_runtime_verdict = should_measure_runtime_verdict(
			self.config.measured_accrual_sample_rate,
			&self.config.measured_accrual_counter,
		);
		let mut retried_descriptor_evolution = false;
		let mut invalidated = Vec::new();
		loop {
			let measured_binding = if measure_runtime_verdict {
				Some(
					bind_explain_analyze_with_statement_timestamp(
						plan,
						catalog.as_ref(),
						&self.config,
						statement_timestamp_nanos,
					)
					.await?,
				)
			} else {
				None
			};
			let bound = match &measured_binding {
				Some(binding) => binding.plan.clone(),
				None => {
					bind_with_statement_timestamp(
						plan,
						catalog.as_ref(),
						&self.config,
						statement_timestamp_nanos,
					)
					.await?
				}
			};
			if !invalidated.is_empty() {
				reject_replacement_object(catalog.as_ref(), &invalidated).await?;
			}
			let telemetry = measured_binding
				.as_ref()
				.map(|_| Arc::new(Mutex::new(TaskTelemetry::default())));
			let result = if let Some(binding) = measured_binding.as_ref() {
				let context = statement_context
					.clone()
					.with_task_telemetry(telemetry.as_ref().expect("telemetry").clone())
					.with_join_runtime_observation_mode(analyze_observation_mode(
						binding.replay_ids.clone(),
					));
				Executor::with_context(self.executor.storage_arc(), context)
					.execute_bound(&bound, morsel)
					.await
			} else {
				Executor::with_context(self.executor.storage_arc(), statement_context.clone())
					.execute_bound(&bound, morsel)
					.await
			};
			match result {
				Ok(results) => {
					if let Some(binding) = measured_binding.as_ref() {
						let observations = join_observations_from_local_telemetry(
							telemetry.as_ref().expect("telemetry"),
						);
						let mut touched = Vec::new();
						for verdict in measured_explain_analyze_verdicts(binding, &observations)? {
							touched.extend(self.config.runtime_evidence.record_verdict(
								verdict.kind,
								verdict.verdict,
								verdict.provenance,
							));
						}
						self.config.runtime_evidence.flush_keys(touched).await;
					}
					return Ok(results);
				}
				Err(ExecutionError::Storage(StorageError::DescriptorEvolved { table_id }))
					if !retried_descriptor_evolution =>
				{
					let error = QueryError::Execution(ExecutionError::Storage(
						StorageError::DescriptorEvolved { table_id },
					));
					if let Some(stale) =
						invalidate_descriptor_evolution(catalog.as_ref(), &error).await?
					{
						invalidated.push(stale);
					}
					retried_descriptor_evolution = true;
				}
				Err(ExecutionError::Storage(StorageError::DescriptorEvolved { .. })) => {
					return Err(QueryError::SchemaInstability);
				}
				Err(error) => return Err(QueryError::Execution(error)),
			}
		}
	}

	/// Execute a local mutation under a caller-retained query identity. The SQL
	/// daemon uses the distributed coordinator's context path; this companion
	/// keeps embedded/local callers capable of resolving ADR 542/548 outcomes
	/// without changing `execute_query`, whose fixed executor context serves many
	/// independent statements.
	pub async fn execute_mutation_with_query_id(
		&self,
		plan: &LogicalPlan,
		query_id: QueryId,
	) -> Result<Vec<RecordBatch>, QueryError> {
		if !matches!(
			plan,
			LogicalPlan::Insert { .. }
				| LogicalPlan::InsertSelect { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
		) {
			return Err(QueryError::Internal {
				message: "execute_mutation_with_query_id requires a mutation plan".to_string(),
			});
		}
		let statement_context = self
			.executor
			.context()
			.for_statement_with_clock(self.statement_clock.as_ref());
		let catalog = Arc::clone(&self.catalog).scoped(statement_context.tenant_id);
		let statement_timestamp_nanos = statement_context.statement_timestamp_nanos;
		let bound = bind_with_statement_timestamp(
			plan,
			catalog.as_ref(),
			&self.config,
			statement_timestamp_nanos,
		)
		.await?;
		self.execute_write_plan_with_operation_id(
			catalog.as_ref(),
			&bound,
			mutation_operation_id(*query_id.get().as_bytes(), 0),
		)
		.await
	}

	async fn execute_write_plan(
		&self,
		catalog: &dyn Catalog,
		bound: &ExecutionPlan,
	) -> Result<Vec<RecordBatch>, QueryError> {
		// The public local coordinator can execute multiple logical statements through
		// one fixed ExecutionContext. It is therefore the caller on this rail and mints
		// a fresh statement identity before each dispatch; reusing context.query_id here
		// would collapse the second mutation into a replay of the first.
		let operation_id = mutation_operation_id(*QueryId::new().get().as_bytes(), 0);
		self.execute_write_plan_with_operation_id(catalog, bound, operation_id)
			.await
	}

	async fn execute_write_plan_with_operation_id(
		&self,
		catalog: &dyn Catalog,
		bound: &ExecutionPlan,
		operation_id: wendelon::schema::ClientWriteId,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let source_executor = LocalMutationSourceExecutor {
			executor: &self.executor,
		};
		let result = execute_write_plan_with_source(
			catalog,
			self.writer.as_ref(),
			self.deleter.as_ref(),
			self.admin.as_ref(),
			self.executor.context(),
			operation_id,
			&source_executor,
			bound,
			&mut None,
		)
		.await;
		if let Err(error) = &result {
			invalidate_descriptor_evolution_on_error(catalog, error).await?;
		}
		result
	}

	async fn execute_explain_analyze(
		&self,
		catalog: &dyn Catalog,
		input: &LogicalPlan,
		morsel: Option<(MorselId, u32)>,
		statement_context: &ExecutionContext,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let binding = bind_explain_analyze_with_statement_timestamp(
			input,
			catalog,
			&self.config,
			statement_context.statement_timestamp_nanos,
		)
		.await?;
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let context = statement_context
			.clone()
			.with_task_telemetry(telemetry.clone())
			.with_join_runtime_observation_mode(analyze_observation_mode(
				binding.replay_ids.clone(),
			));
		let executor = Executor::with_context(self.executor.storage_arc(), context);
		let _results = executor
			.execute_bound(&binding.plan, morsel)
			.await
			.map_err(QueryError::Execution)?;
		let observations = join_observations_from_local_telemetry(&telemetry);
		let analyze_plan = render_explain_analyze(binding, &observations)?;
		materialize_system_table_plan(analyze_plan)
	}
}

struct UnsupportedStorageWrite;

#[async_trait::async_trait]
impl StorageWrite for UnsupportedStorageWrite {
	async fn write(&self, _request: WriteRequest) -> Result<WriteOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "INSERT execution requires a configured StorageWrite".to_string(),
		})
	}
}

struct UnsupportedStorageDeleteRows;

#[async_trait::async_trait]
impl StorageDeleteRows for UnsupportedStorageDeleteRows {
	async fn delete_rows(
		&self,
		_request: DeleteRowsRequest,
	) -> Result<DeleteRowsOutcome, StorageError> {
		Err(StorageError::Internal {
			message: "DELETE execution requires a configured StorageDeleteRows".to_string(),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn adr0094_c6_chunk_ids_preserve_structure_and_refuse_sequence_overflow() {
		let base = wendelon::schema::ClientWriteId {
			client_uuid: [0x94; 16],
			sequence_no: 41,
			idempotency_ttl_ms: 234_567,
			_pad: 0xABCD,
		};
		assert_eq!(derive_ctas_chunk_write_id(base, 0).unwrap(), base);
		let child = derive_ctas_chunk_write_id(base, 7).unwrap();
		assert_eq!(child.client_uuid, base.client_uuid);
		assert_eq!(child.sequence_no, 48);
		assert_eq!(child.idempotency_ttl_ms, base.idempotency_ttl_ms);
		assert_eq!(child._pad, base._pad);
		assert_ne!(
			child.canonical_idempotency_bytes(),
			base.canonical_idempotency_bytes()
		);

		let overflow = wendelon::schema::ClientWriteId {
			sequence_no: u64::MAX,
			..base
		};
		let error = derive_ctas_chunk_write_id(overflow, 1).unwrap_err();
		assert!(error.to_string().contains("overflows"), "{error}");
	}

	#[test]
	fn adr489_distributed_planner_config_refuses_local_only_admission() {
		let config = PlannerConfig {
			single_partition_execution: true,
			..PlannerConfig::default()
		};

		assert!(
			config.single_partition_execution,
			"fixture arms the local-only signal"
		);
		assert!(
			!distributed_planner_config(config).single_partition_execution,
			"the distributed entry must clear the local-only signal before binding"
		);
	}

	use crate::catalog::{DescriptorRefreshMetrics, MockCatalog, TableBinding, TableRef};
	use crate::distributed::budget::{BudgetDimension, ByteCount, DurationMillis, TenantBudget};
	use crate::distributed::identity::{
		AttemptId, LeaseEpoch, StageId, StreamSeq, TaskId, TaskIdentity, WorkerId,
	};
	use crate::distributed::namespace_lifecycle::LeaseIgnored;
	use crate::distributed::task::TaskCompletionOutput;
	use crate::distributed::worker_registry::StaticWorkerRegistry;
	use crate::distributed::worker_registry::{
		ActiveTaskCount, SimdLaneCount, UnixMillis, WorkerCapabilities, WorkerEndpoint,
		WorkerEntry, WorkerLease, WorkerResourceState, WorkerStatus,
	};
	use crate::error::BindingError;
	use crate::ir::{
		AggregateFunction, CreateTableSchema, Expr, InsertLiteral, QueryId, SessionId, TableId,
	};
	use crate::server::transport::{TcpTransport, ToronStream, ToronTransport};
	use crate::storage::{
		HlcBound, ScanRequest, ScanStream, StorageScan, StorageWrite, TenantId, WriteOutcome,
		WriteRequest,
	};
	use crate::wire::{
		ArrowIpcBytes, CancelTaskPayload, EnvelopeIdentity, MessageKind, QueryLeaseAcceptedPayload,
		QueryLeaseOutcome, QueryLeasePayload, QueryRetiredPayload, RetireQueryPayload,
		StartTaskPayload, TaskBatchPayload, TaskCancelledPayload, TaskCompletePayload,
		TaskFailedPayload, TaskStartedPayload, WireEnvelope, decode_payload, encode_payload,
	};
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
	use arrow::ipc::writer::StreamWriter;
	use std::num::NonZeroU32;
	use std::sync::Mutex as StdMutex;
	use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
	use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex as test_duplex};
	use tokio::sync::Notify;
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy,
		TableObjectId, TableSchemaDescriptor,
	};

	struct AdvancingStatementClock {
		value: AtomicI64,
		captures: AtomicUsize,
	}

	impl AdvancingStatementClock {
		fn new(value: i64) -> Self {
			Self {
				value: AtomicI64::new(value),
				captures: AtomicUsize::new(0),
			}
		}
	}

	impl StatementClock for AdvancingStatementClock {
		fn now_nanos(&self) -> i64 {
			self.captures.fetch_add(1, Ordering::Relaxed);
			self.value.fetch_add(1, Ordering::Relaxed)
		}
	}

	#[tokio::test]
	async fn coordinator_retries_once_on_descriptor_evolved() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.expect("batch");
		let clock = Arc::new(AdvancingStatementClock::new(100));
		let coordinator = coordinator(batch, 1).with_statement_clock(clock.clone());
		let plan = named_count_plan();

		let results = coordinator
			.execute_query(&plan, None)
			.await
			.expect("retry succeeds");

		assert_eq!(
			results[0]
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("count")
				.value(0),
			3
		);
		assert_eq!(
			clock.captures.load(Ordering::Relaxed),
			1,
			"descriptor retry must reuse the original statement capture"
		);
	}

	#[tokio::test]
	async fn coordinator_fails_schema_instability_after_second_descriptor_evolved() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.expect("batch");
		let coordinator = coordinator(batch, 2);
		let plan = named_count_plan();

		let err = coordinator
			.execute_query(&plan, None)
			.await
			.expect_err("second descriptor evolution fails");

		assert!(matches!(err, QueryError::SchemaInstability));
	}

	#[tokio::test]
	async fn coordinator_rejects_replacement_object_after_descriptor_evolved() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.expect("batch");
		let coordinator = coordinator_with_refresh_object(batch, 1, TableObjectId::new([2; 16]));

		let err = coordinator
			.execute_query(&named_count_plan(), None)
			.await
			.expect_err("replacement object fails closed");

		assert!(matches!(err, QueryError::SchemaInstability));
	}

	#[tokio::test]
	async fn coordinator_returns_table_not_found_after_descriptor_evolved_drop() {
		let coordinator = coordinator_with_refresh_outcome(batch(), 1, RefreshOutcome::Missing);

		let err = coordinator
			.execute_query(&named_count_plan(), None)
			.await
			.expect_err("missing replacement surfaces table-not-found");

		assert!(matches!(
			err,
			QueryError::Binding(BindingError::Catalog(CatalogError::TableNotFound { .. }))
		));
	}

	#[tokio::test]
	async fn coordinator_returns_resource_exhausted_when_refresh_sync_is_over_budget() {
		let coordinator =
			coordinator_with_refresh_outcome(batch(), 1, RefreshOutcome::ResourceExhausted);

		let err = coordinator
			.execute_query(&named_count_plan(), None)
			.await
			.expect_err("descriptor refresh bind-miss budget exhausted");

		assert!(matches!(
			err,
			QueryError::Binding(BindingError::Catalog(
				CatalogError::ResourceExhausted { .. }
			))
		));
	}

	#[tokio::test]
	async fn bind_miss_after_grant_still_resource_exhausted_and_releases_admission() {
		let coordinator = DistributedQueryCoordinator::new(
			Arc::new(RefreshingCatalog::missing(
				RefreshOutcome::ResourceExhausted,
			)),
			Arc::new(StaticWorkerRegistry::new(Vec::new())),
			Arc::new(MemoryManager::new(1, 1024)),
			Arc::new(TcpTransport::new()),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_tenant_budget_policy(tenant_policy())
		.with_admission_policy(Some(coordinator_admission_policy()));
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);

		let err = coordinator
			.execute_query_with_context(&named_scan_plan(), context)
			.await
			.expect_err("descriptor sync exhaustion surfaces after admission grant");

		assert!(matches!(
			err,
			QueryError::Binding(BindingError::Catalog(
				CatalogError::ResourceExhausted { .. }
			))
		));
		assert_eq!(coordinator.tenant_memory_used(TenantId(7)), ByteCount::ZERO);
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 0);
	}

	#[tokio::test]
	async fn rate_flood_of_cheap_queries_rejects_without_concurrency_saturation() {
		let coordinator = distributed_coordinator_with_policy(rate_policy(1, 1));
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let admitted = coordinator
			.admit_query(&context)
			.await
			.expect("first cheap query admitted");
		drop(admitted);

		let err = coordinator
			.execute_query_with_context(&count_plan(), context)
			.await
			.expect_err("second cheap query hits rate gate");

		assert_rate_limited(err, TenantId(7));
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 0);
		assert_eq!(coordinator.tenant_memory_used(TenantId(7)), ByteCount::ZERO);
	}

	#[tokio::test]
	async fn token_consumed_on_pass_not_refunded_on_bind_error() {
		let coordinator = distributed_coordinator_with_policy(rate_policy(1, 1));
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);

		let first = coordinator
			.execute_query_with_context(&named_scan_plan(), context.clone())
			.await
			.expect_err("first query consumes rate then fails bind");
		assert!(matches!(first, QueryError::Binding(_)));
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 0);

		let second = coordinator
			.execute_query_with_context(&named_scan_plan(), context)
			.await
			.expect_err("rate token was not refunded after bind error");

		assert_rate_limited(second, TenantId(7));
	}

	#[tokio::test]
	async fn token_consumed_on_pass_not_refunded_on_execution_error() {
		let coordinator = distributed_coordinator_with_policy(rate_policy(1, 1));
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);

		let first = coordinator
			.execute_query_with_context(&count_plan(), context.clone())
			.await
			.expect_err("first query consumes rate then fails execution");
		assert!(!is_rate_limited(&first));
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 0);

		let second = coordinator
			.execute_query_with_context(&count_plan(), context)
			.await
			.expect_err("rate token was not refunded after execution error");

		assert_rate_limited(second, TenantId(7));
	}

	#[tokio::test]
	async fn adr054_evolved_retry_stays_inside_one_admission_reservation() {
		let attempts = Arc::new(AtomicUsize::new(0));
		let coordinator = DistributedQueryCoordinator::new(
			Arc::new(RefreshingCatalog::new(
				descriptor_with_identity(TableId(1), TableObjectId::new([1; 16]), 1),
				RefreshOutcome::Binding(descriptor_with_identity(
					TableId(2),
					TableObjectId::new([1; 16]),
					2,
				)),
			)),
			Arc::new(StaticWorkerRegistry::new([scripted_worker()])),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			Arc::new(ScriptedDescriptorTransport {
				attempts: Arc::clone(&attempts),
				success_batch: batch(),
				completion: None,
			}),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_tenant_budget_policy(rate_policy(1, 1))
		.with_admission_policy(Some(coordinator_admission_policy()));
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);

		let results = coordinator
			.execute_query_with_context(&named_scan_plan(), context)
			.await
			.expect("descriptor evolved retry succeeds under one admission reservation");

		// Two task attempts plus the coordinator-issued terminal retirement. The
		// retirement is a separate protocol exchange and does not reacquire admission.
		assert_eq!(attempts.load(Ordering::SeqCst), 3);
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].num_rows(), 3);
		assert_eq!(coordinator.tenant_memory_used(TenantId(7)), ByteCount::ZERO);
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 0);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn distributed_stream_eof_releases_admission_before_the_next_query() {
		let attempts = Arc::new(AtomicUsize::new(1));
		let completion = Arc::new(ScriptedWorkerCompletion::default());
		let coordinator = DistributedQueryCoordinator::new(
			Arc::new(RefreshingCatalog::new(
				descriptor_with_identity(TableId(1), TableObjectId::new([1; 16]), 1),
				RefreshOutcome::Binding(descriptor_with_identity(
					TableId(1),
					TableObjectId::new([1; 16]),
					1,
				)),
			)),
			Arc::new(StaticWorkerRegistry::new([scripted_worker()])),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			Arc::new(ScriptedDescriptorTransport {
				attempts: Arc::clone(&attempts),
				success_batch: batch(),
				completion: Some(Arc::clone(&completion)),
			}),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_tenant_budget_policy(one_query_tenant_policy());
		let first = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(70),
			TenantId(7),
			HlcBound(1),
		);
		let second = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(71),
			TenantId(7),
			HlcBound(1),
		);

		let mut stream = coordinator
			.execute_query_stream_with_context(&named_scan_plan(), first)
			.await
			.expect("first distributed query starts");
		while let Some(batch) = stream.next().await {
			batch.expect("first distributed query batch");
		}

		assert_eq!(
			coordinator.tenant_concurrency_active(TenantId(7)),
			0,
			"EOF must publish only after the first query releases its tenant slot"
		);
		let admitted = coordinator
			.admit_query(&second)
			.await
			.expect("the next query is admitted immediately after EOF");
		drop(admitted);
		wait_for_scripted_workers(&completion, 2).await;
		assert!(
			!completion.failed.load(Ordering::SeqCst),
			"the query and its retirement exchange both complete cleanly"
		);
	}

	#[test]
	fn caller_query_id_is_the_stable_parent_mutation_identity() {
		let query_id = QueryId::new();
		let first = mutation_operation_id(*query_id.get().as_bytes(), 0);
		let replay = mutation_operation_id(*query_id.get().as_bytes(), 0);
		assert_eq!(
			first.canonical_idempotency_bytes(),
			replay.canonical_idempotency_bytes()
		);
		assert_eq!(first.client_uuid, *query_id.get().as_bytes());
		assert_eq!(
			first.client_uuid[6] >> 4,
			7,
			"production QueryId must remain UUIDv7"
		);
	}

	#[test]
	fn local_statement_identity_distinguishes_mutations_in_one_execution_context() {
		let first = mutation_operation_id(*QueryId::new().get().as_bytes(), 0);
		let second = mutation_operation_id(*QueryId::new().get().as_bytes(), 0);
		assert_ne!(
			first.canonical_idempotency_bytes(),
			second.canonical_idempotency_bytes(),
			"two logical statements must never collapse into one replay identity"
		);
	}

	#[tokio::test]
	async fn remote_cancel_fanout_uses_existing_wire_and_records_ack_metric() {
		let identity = TaskIdentity::new(
			QueryId::from_u128(77),
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let metrics = Arc::new(CancellationMetrics::default());
		let seen = Arc::new(StdMutex::new(Vec::new()));
		let acked = Arc::new(Notify::new());
		let coordinator = DistributedQueryCoordinator::new(
			Arc::new(MockCatalog::new()),
			Arc::new(StaticWorkerRegistry::new([scripted_worker()])),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			Arc::new(CancelRecordingTransport {
				seen: Arc::clone(&seen),
				acked: Arc::clone(&acked),
			}),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_cancellation_metrics(Arc::clone(&metrics));

		coordinator.spawn_remote_cancel_fanout(
			CancellationTrigger::SqlCancel,
			vec![RemoteTaskCancellation {
				identity,
				worker: scripted_worker(),
			}],
		);

		tokio::time::timeout(std::time::Duration::from_secs(1), acked.notified())
			.await
			.expect("remote cancel ack observed");
		wait_for_metric(
			&metrics,
			CancellationTrigger::SqlCancel,
			CancellationOutcome::RemoteAck,
		)
		.await;
		assert_eq!(seen.lock().expect("seen lock").as_slice(), &[identity]);
	}

	/// ADR 628 B1.C: the reliance span for L = 35.001 s, ρ 500 ppm each, M = 1 s.
	const RELIANCE_SPAN: Duration = Duration::from_nanos(33_966_016_491);

	fn reliance_registry() -> ActiveQueryRegistry {
		ActiveQueryRegistry::new().with_reliance(
			crate::distributed::query_reliance::RelianceParams::production(Duration::from_millis(
				35_001,
			)),
			Arc::new(crate::distributed::query_reliance::TokioRelianceClock::new()),
		)
	}

	/// Issues `worker` in the query's ledger, as a dispatch's validated handshake would, now.
	fn issue_worker(handle: &ActiveQueryHandle, worker: &WorkerEntry) {
		let reliance = handle.reliance();
		let ticket = reliance.begin_dispatch(worker).expect("dispatch");
		let marker = reliance
			.send_marker(worker.worker_id, ticket.exchange())
			.expect("marker");
		reliance
			.on_applied(
				worker.worker_id,
				marker,
				ticket.exchange(),
				crate::distributed::query_reliance::BootInstant::from_nanos(u128::MAX / 2),
			)
			.expect("applied");
		drop(ticket);
		handle.record_remote_task(worker.clone(), lease_test_identity());
	}

	/// Asserts a lapse at exactly the bound (tokio timers round up to the millisecond).
	fn assert_lapsed_at_bound(stopped: Option<Duration>) {
		let stopped = stopped.expect("reliance must lapse");
		assert!(
			stopped >= RELIANCE_SPAN && stopped <= RELIANCE_SPAN + Duration::from_millis(2),
			"lapsed at {stopped:?}, bound {RELIANCE_SPAN:?}"
		);
	}

	#[tokio::test(start_paused = true)]
	async fn query_lifetime_renews_worker_lease_until_terminal() {
		let registry = reliance_registry();
		let guard = registry.register(TenantId(7), QueryId::from_u128(1));
		let handle = guard.handle();
		let calls = Arc::new(AtomicUsize::new(0));
		let owner = spawn_query_lease_renewal_task(
			Arc::new(LeaseRecordingTransport {
				connect_calls: Arc::clone(&calls),
				fail_at: None,
				fail_first: 0,
			}),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			handle.clone(),
		);
		issue_worker(&handle, &scripted_worker());
		// Far beyond the initial reliance horizon: renewals keep it alive.
		tokio::time::sleep(Duration::from_secs(120)).await;
		assert!(!handle.is_cancelled());
		assert_eq!(handle.lease_failure_worker(), None);
		let renewed = calls.load(Ordering::SeqCst);
		assert!(
			(22..=24).contains(&renewed),
			"one renewal per 5 s, no bursts: {renewed}"
		);

		handle.mark_terminal();
		owner.shutdown().await.expect("renewal cleanup verified");
		let after_stop = calls.load(Ordering::SeqCst);
		tokio::time::sleep(Duration::from_secs(30)).await;
		assert_eq!(
			calls.load(Ordering::SeqCst),
			after_stop,
			"no exchange after cleanup"
		);
	}

	#[tokio::test(start_paused = true)]
	async fn persistent_renewal_failure_lapses_at_the_bound_with_worker_identity() {
		let registry = reliance_registry();
		let guard = registry.register(TenantId(7), QueryId::from_u128(1));
		let handle = guard.handle();
		let worker = scripted_worker();
		let owner = spawn_query_lease_renewal_task(
			Arc::new(LeaseRecordingTransport {
				connect_calls: Arc::new(AtomicUsize::new(0)),
				fail_at: Some(0),
				fail_first: 0,
			}),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			handle.clone(),
		);
		let started = tokio::time::Instant::now();
		issue_worker(&handle, &worker);
		let stopped = tokio::time::timeout(Duration::from_secs(120), handle.stopped())
			.await
			.ok()
			.map(|()| started.elapsed());
		// One transient attempt never fails the query; only the lapse at the bound does.
		assert_lapsed_at_bound(stopped);
		assert!(handle.is_cancelled());
		assert_eq!(handle.lease_failure_worker(), Some(worker.worker_id));
		assert!(matches!(
			handle.reliance().state(worker.worker_id),
			Some(crate::distributed::query_reliance::WorkerReliance::Lapsed { .. })
		));
		owner.shutdown().await.expect("renewal cleanup verified");
	}

	#[tokio::test(start_paused = true)]
	async fn transient_failures_inside_the_boundary_are_survived() {
		let registry = reliance_registry();
		let guard = registry.register(TenantId(7), QueryId::from_u128(1));
		let handle = guard.handle();
		let calls = Arc::new(AtomicUsize::new(0));
		let owner = spawn_query_lease_renewal_task(
			Arc::new(LeaseRecordingTransport {
				connect_calls: Arc::clone(&calls),
				fail_at: None,
				fail_first: 6, // several seconds of failed attempts, well inside ~34 s
			}),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			handle.clone(),
		);
		issue_worker(&handle, &scripted_worker());
		tokio::time::sleep(Duration::from_secs(120)).await;
		assert!(
			!handle.is_cancelled(),
			"a delay inside the boundary is survived"
		);
		assert!(calls.load(Ordering::SeqCst) > 6);
		handle.mark_terminal();
		owner.shutdown().await.expect("renewal cleanup verified");
	}

	/// ADR 628 B1.C: B1.B measured fresh-connection connects at p50 ≈ 125–164 ms. The old 100 ms caps
	/// failed such renewals; the provisional budgets (2000/2000/3000 ms) do not.
	async fn adr628_renewal_run(
		connect_delay: Duration,
		ack_delay: Duration,
	) -> (Option<Duration>, usize, bool) {
		lease_reply_run(connect_delay, ack_delay, None, None, false).await
	}

	async fn lease_reply_run(
		connect_delay: Duration,
		ack_delay: Duration,
		ignore: Option<LeaseIgnored>,
		term_override: Option<u64>,
		stall_shutdown: bool,
	) -> (Option<Duration>, usize, bool) {
		let registry = reliance_registry();
		let guard = registry.register(TenantId(7), QueryId::from_u128(1));
		let handle = guard.handle();
		let worker = scripted_worker();
		let renewals = Arc::new(AtomicUsize::new(0));
		let owner = spawn_query_lease_renewal_task(
			Arc::new(PhasedLeaseTransport {
				connect_delay,
				ack_delay,
				renewals: renewals.clone(),
				ignore,
				term_override,
				stall_shutdown,
			}),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			handle.clone(),
		);
		let started = tokio::time::Instant::now();
		issue_worker(&handle, &worker);
		let stopped = tokio::time::timeout(Duration::from_secs(120), handle.stopped())
			.await
			.ok()
			.map(|()| started.elapsed());
		let failed = handle.lease_failure_worker() == Some(worker.worker_id);
		guard.handle().mark_terminal();
		owner.shutdown().await.expect("renewal cleanup verified");
		(stopped, renewals.load(Ordering::SeqCst), failed)
	}

	#[tokio::test(start_paused = true)]
	async fn renewals_slower_than_the_old_100ms_caps_keep_the_query_alive() {
		for (connect, ack) in [
			(Duration::from_millis(150), Duration::ZERO),
			(Duration::ZERO, Duration::from_millis(150)),
			(Duration::from_millis(1500), Duration::from_millis(1400)),
		] {
			let (stopped, renewals, failed) = adr628_renewal_run(connect, ack).await;
			assert!(!failed && stopped.is_none(), "{connect:?}/{ack:?}");
			// The next renewal is due 5 s after the previous send marker, which follows the connect.
			let period = Duration::from_secs(5) + connect;
			let expected = (Duration::from_secs(120).as_millis() / period.as_millis()) as usize;
			assert!(
				renewals + 1 >= expected,
				"{connect:?}/{ack:?}: {renewals} < {expected}"
			);
		}
	}

	#[tokio::test(start_paused = true)]
	async fn renewals_beyond_the_budgets_lapse_at_the_bound() {
		for (connect, ack) in [
			(Duration::from_millis(2500), Duration::ZERO),
			(Duration::ZERO, Duration::from_millis(2500)),
		] {
			let (stopped, renewals, failed) = adr628_renewal_run(connect, ack).await;
			assert!(failed, "{connect:?}/{ack:?}");
			assert_eq!(renewals, 0, "{connect:?}/{ack:?}");
			assert_lapsed_at_bound(stopped);
		}
	}

	/// ADR 628 B0 (kept): an `Ignored` renewal ends the query's reliance on that worker at the reply,
	/// which is the first renewal (issuance + 5 s), naming it, for every reason.
	#[tokio::test(start_paused = true)]
	async fn an_ignored_renewal_ends_reliance_at_the_reply() {
		for reason in [
			LeaseIgnored::Expired,
			LeaseIgnored::Retiring {
				cause: QueryTerminalCause::Abandoned,
			},
			LeaseIgnored::ReceiptHeld {
				cause: QueryTerminalCause::Success,
			},
		] {
			let (stopped, _, failed) =
				lease_reply_run(Duration::ZERO, Duration::ZERO, Some(reason), None, false).await;
			assert!(failed, "{reason:?}");
			assert_eq!(stopped, Some(Duration::from_secs(5)), "{reason:?}");
			// A stalled close cannot delay it: the reply alone ends reliance.
			let (stopped, _, failed) =
				lease_reply_run(Duration::ZERO, Duration::ZERO, Some(reason), None, true).await;
			assert!(failed, "{reason:?} with a stalled close");
			assert_eq!(
				stopped,
				Some(Duration::from_secs(5)),
				"{reason:?} with a stalled close"
			);
		}
	}

	/// ADR 628 B0 (kept): an `Applied` reply carrying a term other than the requested one is a
	/// protocol error, terminal at the reply, never a renewal.
	#[tokio::test(start_paused = true)]
	async fn an_applied_reply_with_another_term_is_not_a_renewal() {
		let (stopped, _, failed) =
			lease_reply_run(Duration::ZERO, Duration::ZERO, None, Some(1), false).await;
		assert!(failed);
		assert_eq!(stopped, Some(Duration::from_secs(5)));
	}

	/// ADR 628 B0/B1.C: `Applied` still requires the whole exchange. A close that never completes
	/// refreshes nothing; the attempts are transient, and reliance lapses at the bound.
	#[tokio::test(start_paused = true)]
	async fn an_applied_renewal_with_a_stalled_close_refreshes_nothing() {
		let (stopped, _, failed) =
			lease_reply_run(Duration::ZERO, Duration::ZERO, None, None, true).await;
		assert!(failed);
		assert_lapsed_at_bound(stopped);
	}

	#[tokio::test(start_paused = true)]
	async fn one_workers_failures_never_delay_or_fail_another() {
		let registry = reliance_registry();
		let guard = registry.register(TenantId(7), QueryId::from_u128(1));
		let handle = guard.handle();
		let healthy = scripted_worker();
		let mut flaky = scripted_worker();
		flaky.worker_id = WorkerId::from_u128(8);
		flaky.endpoint = WorkerEndpoint::new("scripted://flaky-worker");
		let transport = Arc::new(AddressLeaseTransport::new([(
			"scripted://flaky-worker".to_string(),
			5usize,
		)]));
		let owner = spawn_query_lease_renewal_task(
			Arc::clone(&transport),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			handle.clone(),
		);
		issue_worker(&handle, &healthy);
		issue_worker(&handle, &flaky);
		tokio::time::sleep(Duration::from_secs(60)).await;
		assert!(!handle.is_cancelled());
		let healthy_connects = transport.connects("scripted://descriptor-worker");
		assert!(
			(11..=12).contains(&healthy_connects),
			"the healthy worker renews at its own cadence: {healthy_connects}"
		);
		assert!(transport.connects("scripted://flaky-worker") > 5);
		handle.mark_terminal();
		owner.shutdown().await.expect("renewal cleanup verified");
	}

	fn one_row_batch() -> RecordBatch {
		use arrow::array::Int64Array;
		use arrow::datatypes::{DataType, Field, Schema};
		RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
			vec![Arc::new(Int64Array::from(vec![1_i64]))],
		)
		.expect("batch")
	}

	/// A query with one issued worker on a hand-set clock; returns the handle, clock and bound.
	fn settable_query() -> (
		crate::distributed::cancellation::ActiveQueryGuard,
		ActiveQueryHandle,
		Arc<crate::distributed::query_reliance::SettableClock>,
		u128,
	) {
		let clock = Arc::new(crate::distributed::query_reliance::SettableClock::at(
			1_000_000_000,
		));
		let registry = ActiveQueryRegistry::new().with_reliance(
			crate::distributed::query_reliance::RelianceParams::production(Duration::from_millis(
				35_001,
			)),
			clock.clone(),
		);
		let guard = registry.register(TenantId(7), QueryId::from_u128(1));
		let handle = guard.handle();
		issue_worker(&handle, &scripted_worker());
		(
			guard,
			handle,
			clock,
			1_000_000_000 + RELIANCE_SPAN.as_nanos(),
		)
	}

	fn no_remote_cancel() -> RemoteCancelSpawner {
		Arc::new(|_, _| {})
	}

	/// ADR 628 B1.C (amendment A): after the success commit, buffered final batches are local
	/// results. They are delivered even when the consumer reads them after the old bound.
	#[tokio::test]
	async fn buffered_results_after_a_committed_success_are_delivered_past_the_old_bound() {
		let (_guard, handle, clock, bound) = settable_query();
		let (sender, receiver) = mpsc::channel(4);
		sender.send(Ok(one_row_batch())).await.unwrap();
		sender.send(Ok(one_row_batch())).await.unwrap();
		drop(sender);
		assert!(commit_query_success(&handle, Ok(())).is_ok());
		clock.set(bound + 60_000_000_000); // the slow consumer reads long after the bound
		let mut stream = DistributedResultStream::new(receiver, handle.clone(), no_remote_cancel());
		assert!(stream.next().await.unwrap().is_ok());
		assert!(stream.next().await.unwrap().is_ok());
		assert!(stream.next().await.is_none());
	}

	/// ADR 628 B1.C: a lapse BEFORE the commit suppresses every later buffered batch. The stream ends
	/// with the lapse error (partial-result-then-error), and a late Ok cannot commit.
	#[tokio::test]
	async fn a_lapse_before_commit_suppresses_buffered_batches() {
		let (_guard, handle, clock, bound) = settable_query();
		let (sender, receiver) = mpsc::channel(4);
		sender.send(Ok(one_row_batch())).await.unwrap();
		sender.send(Ok(one_row_batch())).await.unwrap();
		drop(sender);
		let mut stream = DistributedResultStream::new(receiver, handle.clone(), no_remote_cancel());
		assert!(
			stream.next().await.unwrap().is_ok(),
			"delivered while relied on"
		);
		clock.set(bound);
		let late = commit_query_success(&handle, Ok(()));
		assert!(
			matches!(&late, Err(QueryError::Runtime(RuntimeError::TaskFailure { kind: TaskFailureKind::WorkerLost { worker_id } })) if *worker_id == scripted_worker().worker_id),
			"{late:?}"
		);
		let suppressed = stream.next().await.unwrap();
		assert!(
			suppressed.is_err(),
			"the buffered batch after the lapse is suppressed"
		);
		assert!(stream.next().await.is_none());
		assert_eq!(
			handle.lease_failure_worker(),
			Some(scripted_worker().worker_id)
		);
	}

	/// ADR 628 B1.C: a late fatal callback after the commit is context only.
	#[tokio::test]
	async fn a_late_fatal_after_commit_cannot_fail_the_query() {
		let (_guard, handle, _clock, _) = settable_query();
		assert!(commit_query_success(&handle, Ok(())).is_ok());
		handle.reliance().on_fatal(
			scripted_worker().worker_id,
			crate::distributed::query_reliance::RelianceCause::Ignored(LeaseIgnored::Expired),
		);
		assert_eq!(
			handle.reliance().outcome(),
			crate::distributed::query_reliance::QueryOutcome::Committed
		);
		assert!(!handle.is_cancelled());
		assert_eq!(handle.lease_failure_worker(), None);
	}

	/// ADR 628 B1.C: a coordinator clock fault is a local internal error, not `WorkerLost`.
	#[tokio::test]
	async fn a_clock_fault_at_commit_is_an_internal_error() {
		let (_guard, handle, clock, _) = settable_query();
		clock.fail(crate::distributed::query_reliance::ClockFault::ReadFailed(
			5,
		));
		let result = commit_query_success(&handle, Ok(()));
		assert!(
			matches!(result, Err(QueryError::Internal { .. })),
			"{result:?}"
		);
	}

	#[tokio::test(start_paused = true)]
	async fn dropping_the_owner_stops_new_exchanges() {
		let registry = reliance_registry();
		let guard = registry.register(TenantId(7), QueryId::from_u128(1));
		let handle = guard.handle();
		let calls = Arc::new(AtomicUsize::new(0));
		let owner = spawn_query_lease_renewal_task(
			Arc::new(LeaseRecordingTransport {
				connect_calls: Arc::clone(&calls),
				fail_at: None,
				fail_first: 0,
			}),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			handle.clone(),
		);
		issue_worker(&handle, &scripted_worker());
		tokio::time::sleep(Duration::from_secs(12)).await;
		let cleanup = owner.cleanup();
		drop(owner);
		let after = calls.load(Ordering::SeqCst);
		// The cleanup receipt completes once every loop is aborted and joined.
		cleanup.finished().await.expect("renewal cleanup verified");
		tokio::time::sleep(Duration::from_secs(30)).await;
		assert_eq!(calls.load(Ordering::SeqCst), after);
	}

	/// ADR 628 B1.C: `Drop` is a synchronous stop boundary. A loop that is due and RUNNABLE at the
	/// drop (woken by the drop itself) admits no exchange. The control shows the same wake-up
	/// without the drop does start an exchange.
	#[tokio::test]
	async fn dropping_the_owner_admits_no_exchange_from_a_runnable_loop() {
		for drop_owner in [true, false] {
			let (_guard, handle, clock, _) = settable_query();
			let transport = Arc::new(AddressLeaseTransport::new([]));
			let owner = spawn_query_lease_renewal_task(
				Arc::clone(&transport),
				Arc::new(MemoryManager::new(1, 1024 * 1024)),
				handle.clone(),
			);
			// The first worker was issued before the owner existed; issue another now so the
			// supervisor runs its loop. Its renewal is due 5 s after its marker.
			let mut worker = scripted_worker();
			worker.worker_id = WorkerId::from_u128(8);
			worker.endpoint = WorkerEndpoint::new("scripted://runnable-worker");
			issue_worker(&handle, &worker);
			tokio::task::yield_now().await;
			tokio::task::yield_now().await;
			clock.advance(Duration::from_secs(6)); // due; the loop still sleeps on tokio's timer
			let cleanup = owner.cleanup();
			if drop_owner {
				// Closes admission, then wakes the loop through the ledger's change notification.
				drop(owner);
				cleanup.finished().await.expect("renewal cleanup verified");
				assert_eq!(transport.connects("scripted://runnable-worker"), 0);
				assert_eq!(
					handle.reliance().renewal_plan(worker.worker_id, None),
					crate::distributed::query_reliance::RenewalPlan::Stop
				);
			} else {
				// The same wake-up with admission open: a third issuance notifies the ledger.
				let mut other = scripted_worker();
				other.worker_id = WorkerId::from_u128(9);
				other.endpoint = WorkerEndpoint::new("scripted://other-worker");
				issue_worker(&handle, &other);
				tokio::time::timeout(Duration::from_secs(5), async {
					while transport.connects("scripted://runnable-worker") == 0 {
						tokio::task::yield_now().await;
					}
				})
				.await
				.expect("the runnable loop starts an exchange");
				owner.shutdown().await.expect("renewal cleanup verified");
			}
		}
	}

	/// ADR 628 B1.C: a buffered batch is delivered only through the delivery gate. With the query
	/// still open and NOTHING else running (no renewal, no commit, no dispatch gate), a poll after
	/// the bound observes the lapse itself.
	#[tokio::test]
	async fn an_open_query_delivers_no_buffered_batch_after_a_bound() {
		let (_guard, handle, clock, bound) = settable_query();
		let (sender, receiver) = mpsc::channel(4);
		sender.send(Ok(one_row_batch())).await.unwrap();
		sender.send(Ok(one_row_batch())).await.unwrap();
		let mut stream = DistributedResultStream::new(receiver, handle.clone(), no_remote_cancel());
		assert!(
			stream.next().await.unwrap().is_ok(),
			"delivered while relied on"
		);
		clock.set(bound);
		let suppressed = stream.next().await.unwrap();
		assert!(
			matches!(&suppressed, Err(QueryError::Runtime(RuntimeError::TaskFailure { kind: TaskFailureKind::WorkerLost { worker_id } })) if *worker_id == scripted_worker().worker_id),
			"{suppressed:?}"
		);
		assert!(stream.next().await.is_none());
		assert!(matches!(
			handle.reliance().outcome(),
			crate::distributed::query_reliance::QueryOutcome::Failed { .. }
		));
		assert!(commit_query_success(&handle, Ok(())).is_err());
	}

	/// ADR 628 B1.C: a clock fault at delivery fails closed as a local internal error.
	#[tokio::test]
	async fn a_clock_fault_at_delivery_is_an_internal_error() {
		let (_guard, handle, clock, _) = settable_query();
		let (sender, receiver) = mpsc::channel(4);
		sender.send(Ok(one_row_batch())).await.unwrap();
		clock.fail(crate::distributed::query_reliance::ClockFault::ReadFailed(
			5,
		));
		let mut stream = DistributedResultStream::new(receiver, handle.clone(), no_remote_cancel());
		let item = stream.next().await.unwrap();
		assert!(matches!(item, Err(QueryError::Internal { .. })), "{item:?}");
	}

	/// ADR 628 B1.C: a real blocked runtime (not a hand-set clock). The current-thread runtime is
	/// blocked past the bound between buffering and delivery; the delivery gate sees it.
	#[tokio::test]
	async fn a_starved_consumer_receives_no_batch_after_a_bound() {
		// L = 1.3 s: D − M = 298_700_649 ns.
		let registry = ActiveQueryRegistry::new().with_reliance(
			crate::distributed::query_reliance::RelianceParams::production(Duration::from_millis(
				1_300,
			)),
			Arc::new(crate::distributed::query_reliance::BootClock),
		);
		let guard = registry.register(TenantId(7), QueryId::from_u128(1));
		let handle = guard.handle();
		issue_worker(&handle, &scripted_worker());
		let (sender, receiver) = mpsc::channel(4);
		sender.send(Ok(one_row_batch())).await.unwrap();
		let mut stream = DistributedResultStream::new(receiver, handle.clone(), no_remote_cancel());
		std::thread::sleep(Duration::from_millis(400));
		let item = stream.next().await.unwrap();
		assert!(
			matches!(
				&item,
				Err(QueryError::Runtime(RuntimeError::TaskFailure {
					kind: TaskFailureKind::WorkerLost { .. }
				}))
			),
			"{item:?}"
		);
	}

	/// ADR 628 B1.C: end of stream certifies success only once committed. A driver that disappeared
	/// while the query was open is an error, and the query is sealed.
	#[tokio::test]
	async fn end_of_stream_while_open_is_not_success() {
		let (_guard, handle, _clock, _) = settable_query();
		let (sender, receiver) = mpsc::channel::<Result<RecordBatch, QueryError>>(4);
		drop(sender);
		let mut stream = DistributedResultStream::new(receiver, handle.clone(), no_remote_cancel());
		let item = stream.next().await.unwrap();
		assert!(matches!(item, Err(QueryError::Internal { .. })), "{item:?}");
		assert!(stream.next().await.is_none());
		assert!(commit_query_success(&handle, Ok(())).is_err());
	}

	/// ADR 628 B1.C: an ordinary failure is sealed before its error is delivered. Buffered batches
	/// behind it are suppressed, and the client receives the ORIGINAL error, not a substitute.
	#[tokio::test]
	async fn an_ordinary_failure_suppresses_buffered_batches_and_keeps_its_error() {
		let (_guard, handle, _clock, _) = settable_query();
		let (sender, receiver) = mpsc::channel(4);
		sender.send(Ok(one_row_batch())).await.unwrap();
		sender.send(Ok(one_row_batch())).await.unwrap();
		let result = commit_query_success::<()>(
			&handle,
			Err(QueryError::Internal {
				message: "scan failed".to_string(),
			}),
		);
		assert!(
			matches!(&result, Err(QueryError::Internal { message }) if message == "scan failed"),
			"{result:?}"
		);
		sender.send(Err(result.unwrap_err())).await.unwrap();
		drop(sender);
		let mut stream = DistributedResultStream::new(receiver, handle.clone(), no_remote_cancel());
		let item = stream.next().await.unwrap();
		assert!(
			matches!(&item, Err(QueryError::Internal { message }) if message == "scan failed"),
			"{item:?}"
		);
		assert!(stream.next().await.is_none());
		assert!(matches!(
			handle.reliance().outcome(),
			crate::distributed::query_reliance::QueryOutcome::Failed {
				cause: crate::distributed::query_reliance::RelianceCause::QueryEnded(
					crate::distributed::query_reliance::QueryEnd::Error(_)
				),
				..
			}
		));
		// Nothing after the seal refreshes or authorizes.
		assert!(
			handle
				.reliance()
				.gate(
					scripted_worker().worker_id,
					crate::distributed::query_reliance::UseSite::new("start_task")
				)
				.is_err()
		);
	}

	/// ADR 628 B1.C: when a cancellation wins, a later success cannot: the late Ok is refused, a late
	/// Applied refreshes nothing, and no dispatch is authorized. In the other order the commit wins
	/// and the cancellation is context only.
	#[tokio::test]
	async fn a_cancellation_beats_a_late_success_and_a_late_applied() {
		let (_guard, handle, _clock, _) = settable_query();
		let worker = scripted_worker();
		let marker = handle
			.reliance()
			.send_marker(
				worker.worker_id,
				crate::distributed::query_reliance::ExchangeId::Renewal { attempt: 1 },
			)
			.expect("marker");
		handle.cancel(CancellationTrigger::SqlCancel);
		let late = commit_query_success(&handle, Ok(()));
		assert!(
			matches!(
				late,
				Err(QueryError::Runtime(RuntimeError::TaskFailure {
					kind: TaskFailureKind::Cancelled
				}))
			),
			"{late:?}"
		);
		assert!(
			handle
				.reliance()
				.on_applied(
					worker.worker_id,
					marker,
					crate::distributed::query_reliance::ExchangeId::Renewal { attempt: 1 },
					crate::distributed::query_reliance::BootInstant::from_nanos(u128::MAX / 2),
				)
				.is_err()
		);
		assert!(handle.reliance().begin_dispatch(&worker).is_err());

		let (_guard, handle, _clock, _) = settable_query();
		assert!(commit_query_success(&handle, Ok(())).is_ok());
		handle.cancel(CancellationTrigger::SqlCancel);
		assert_eq!(
			handle.reliance().outcome(),
			crate::distributed::query_reliance::QueryOutcome::Committed
		);
	}

	/// ADR 628 B1.C: a renewal transport for real blocked-runtime controls. Its worker's `Applied`
	/// reply is written before the coordinator even sends its lease, so it is READY when polled; the
	/// coordinator's runtime thread is then blocked in the chosen phase. Because tokio's timeout
	/// polls the ready inner future first, only the boot-clock re-check can refuse the refresh.
	struct StarvingLeaseTransport {
		phase: &'static str,
		block: Duration,
		query_id: QueryId,
		term_millis: u64,
	}

	struct StarvedStream {
		inner: DuplexStream,
		_worker: DuplexStream,
		block_read: Option<Duration>,
		block_close: Option<Duration>,
	}

	impl tokio::io::AsyncRead for StarvedStream {
		fn poll_read(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
			buf: &mut tokio::io::ReadBuf<'_>,
		) -> Poll<std::io::Result<()>> {
			if let Some(block) = self.block_read.take() {
				std::thread::sleep(block);
			}
			Pin::new(&mut self.inner).poll_read(cx, buf)
		}
	}

	impl tokio::io::AsyncWrite for StarvedStream {
		fn poll_write(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
			buf: &[u8],
		) -> Poll<std::io::Result<usize>> {
			Pin::new(&mut self.inner).poll_write(cx, buf)
		}

		fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Pin::new(&mut self.inner).poll_flush(cx)
		}

		fn poll_shutdown(
			mut self: Pin<&mut Self>,
			cx: &mut Context<'_>,
		) -> Poll<std::io::Result<()>> {
			if let Some(block) = self.block_close.take() {
				std::thread::sleep(block);
			}
			Pin::new(&mut self.inner).poll_shutdown(cx)
		}
	}

	#[async_trait::async_trait]
	impl ToronTransport for StarvingLeaseTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			if self.phase == "connect" {
				std::thread::sleep(self.block);
			}
			let (client, mut worker) = test_duplex(1024 * 1024);
			send_test_payload(
				&mut worker,
				MessageKind::QueryLeaseAccepted,
				EnvelopeIdentity::query(self.query_id),
				None,
				&QueryLeaseAcceptedPayload {
					query_id: self.query_id,
					outcome: QueryLeaseOutcome::Applied {
						term_ms: self.term_millis,
					},
				},
			)
			.await
			.expect("pre-written reply");
			Ok(Box::new(StarvedStream {
				inner: client,
				_worker: worker,
				block_read: (self.phase == "reply").then_some(self.block),
				block_close: (self.phase == "close").then_some(self.block),
			}))
		}
	}

	/// ADR 628 B1.C: ACK, close and whole-budget overruns caused by a really blocked runtime, each
	/// ending long before the old bound: no refresh, an overrun recorded, the query still open, and
	/// the loop's retry policy (transient) applies within the existing bound. The control (no block)
	/// refreshes.
	#[tokio::test]
	async fn blocked_runtime_overruns_refresh_nothing() {
		use crate::distributed::query_reliance::{Applied, ExchangeId, QueryOutcome};
		let term = Duration::from_millis(35_001);
		let mut params = crate::distributed::query_reliance::RelianceParams::production(term);
		params.connect_budget = Duration::from_millis(100);
		params.ack_budget = Duration::from_millis(100);
		let lease_term_millis = u64::try_from(
			crate::distributed::namespace_lifecycle::ShuffleLifecycleConfig::from_worker_loss_window(
				crate::distributed::partition_store::DEFAULT_WORKER_LOST_AFTER,
			)
			.expect("default lifecycle")
			.lease_term()
			.as_millis(),
		)
		.expect("term");
		// (whole budget, blocked phase, block). The last two are the connect-only counterexample:
		// connect 150 ms against its 100 ms budget, with the 1000 ms whole budget still left, and its
		// on-time twin.
		for (whole_ms, phase, block_ms) in [
			(150, "connect", 250),
			(150, "reply", 250),
			(150, "close", 250),
			(150, "none", 0),
			(1000, "connect", 150),
			(1000, "none", 0),
		] {
			params.whole_budget = Duration::from_millis(whole_ms);
			let label = format!("{phase} (whole {whole_ms} ms)");
			let registry = ActiveQueryRegistry::new().with_reliance(
				params,
				Arc::new(crate::distributed::query_reliance::BootClock),
			);
			let query_id = QueryId::from_u128(1);
			let guard = registry.register(TenantId(7), query_id);
			let handle = guard.handle();
			let worker = scripted_worker();
			issue_worker(&handle, &worker);
			let reliance = Arc::clone(handle.reliance());
			let before = match reliance.state(worker.worker_id) {
				Some(crate::distributed::query_reliance::WorkerReliance::Reliable {
					bound,
					..
				}) => bound,
				other => panic!("{other:?}"),
			};
			let transport = StarvingLeaseTransport {
				phase,
				block: Duration::from_millis(block_ms),
				query_id,
				term_millis: lease_term_millis,
			};
			let coordinator = ToronCoordinator::new(Arc::new(MemoryManager::new(1, 1024 * 1024)));
			let (attempt_no, attempt) =
				renewal_attempt(&transport, &coordinator, &reliance, query_id, &worker).await;
			let attempt_no = attempt_no.expect("admitted");
			// The loop's handling of the attempt.
			let refreshed = match attempt {
				RenewalAttempt::Applied { marker, by } => {
					match reliance.on_applied(
						worker.worker_id,
						marker,
						ExchangeId::Renewal {
							attempt: attempt_no,
						},
						by,
					) {
						Ok(Applied::Bound(bound)) => bound > before,
						Ok(Applied::Late) => false,
						Err(refusal) => panic!("{label}: {refusal:?}"),
					}
				}
				RenewalAttempt::Transient(reason) => {
					if whole_ms == 1000 {
						assert_eq!(reason, "connect completed after its budget", "{label}");
					}
					false
				}
				RenewalAttempt::Fatal(cause) => panic!("{label}: fatal {cause}"),
				RenewalAttempt::Refused => panic!("{label}: refused"),
			};
			assert_eq!(reliance.outcome(), QueryOutcome::Open, "{label}");
			if phase == "none" {
				assert!(refreshed, "{label}: the control refreshes");
				assert_eq!(reliance.budget_overruns(), 0, "{label}");
			} else {
				assert!(!refreshed, "{label}: an overrun refreshes nothing");
				assert!(reliance.budget_overruns() >= 1, "{label}: overrun recorded");
				assert!(
					matches!(
						reliance.state(worker.worker_id),
						Some(crate::distributed::query_reliance::WorkerReliance::Reliable { bound, .. }) if bound == before
					),
					"{label}: the existing bound is kept"
				);
			}
		}
	}

	/// ADR 628 B1.C: a scripted worker that grants the lease and reads StartTask, then waits for a
	/// permit before it sends its one batch and completes. Retirement connections end at once.
	struct GatedScriptTransport {
		go: Arc<Semaphore>,
		batch: RecordBatch,
	}

	#[async_trait::async_trait]
	impl ToronTransport for GatedScriptTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			let (client, mut server) = test_duplex(1024 * 1024);
			let (go, batch) = (Arc::clone(&self.go), self.batch.clone());
			tokio::spawn(async move {
				let Ok(Some(start)) = read_start_after_lifecycle(&mut server).await else {
					return;
				};
				let payload: StartTaskPayload = decode_payload(&start.payload).expect("start");
				let identity = payload.spec.identity;
				let _ = go.acquire().await.map(|permit| permit.forget());
				let seq = StreamSeq::new(0);
				let _ = send_test_payload(
					&mut server,
					MessageKind::TaskStarted,
					EnvelopeIdentity::task(identity),
					None,
					&TaskStartedPayload { identity },
				)
				.await;
				let _ = send_test_payload(
					&mut server,
					MessageKind::TaskBatch,
					EnvelopeIdentity::task(identity),
					Some(seq),
					&TaskBatchPayload {
						identity,
						stream_seq: seq,
						payload: ArrowIpcBytes::new(
							encode_record_batch_for_test(&batch).expect("encode"),
						),
					},
				)
				.await;
				let _ = send_test_payload(
					&mut server,
					MessageKind::TaskComplete,
					EnvelopeIdentity::task(identity),
					None,
					&TaskCompletePayload {
						identity,
						output: TaskCompletionOutput::FinalResult {
							output_digest: None,
						},
						telemetry: None,
					},
				)
				.await;
				std::future::pending::<()>().await;
			});
			Ok(Box::new(client))
		}
	}

	/// A transport whose every connect is refused at once.
	struct RefusingTransport;

	#[async_trait::async_trait]
	impl ToronTransport for RefusingTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			Err(QueryError::Io(std::io::Error::new(
				std::io::ErrorKind::ConnectionRefused,
				"refused",
			)))
		}
	}

	fn hooked_stream_coordinator(
		transport: Arc<dyn ToronTransport>,
		collect: Arc<CollectReliance>,
		hook: RenewalCleanupHook,
	) -> DistributedQueryCoordinator<StaticWorkerRegistry, dyn ToronTransport> {
		DistributedQueryCoordinator::new(
			Arc::new(RefreshingCatalog::new(
				descriptor_with_identity(TableId(1), TableObjectId::new([1; 16]), 1),
				RefreshOutcome::Binding(descriptor_with_identity(
					TableId(1),
					TableObjectId::new([1; 16]),
					1,
				)),
			)),
			Arc::new(StaticWorkerRegistry::new([scripted_worker()])),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			transport,
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_reliance_observer(collect)
		.with_renewal_cleanup_hook(hook)
	}

	async fn open_stream(
		coordinator: &DistributedQueryCoordinator<StaticWorkerRegistry, dyn ToronTransport>,
		config_path: bool,
		query: u128,
	) -> DistributedResultStream {
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(query),
			TenantId(7),
			HlcBound(1),
		);
		if config_path {
			coordinator
				.execute_query_stream_with_context_and_config(
					&named_scan_plan(),
					context,
					PlannerConfig::default(),
				)
				.await
				.expect("the stream starts")
		} else {
			coordinator
				.execute_query_stream_with_context(&named_scan_plan(), context)
				.await
				.expect("the stream starts")
		}
	}

	fn kinds_of(
		collect: &CollectReliance,
		kind: &str,
	) -> Vec<crate::distributed::query_reliance::RelianceEvent> {
		let mut events: Vec<_> = collect
			.0
			.lock()
			.unwrap()
			.iter()
			.filter(|e| e.kind == kind)
			.cloned()
			.collect();
		events.sort_by_key(|e| e.seq);
		events
	}

	/// ADR 628 B1.C: on BOTH streaming entry paths, a committed stream delivers every batch but no normal
	/// end while its renewal cleanup is held; releasing it records the verified receipt, then the end.
	#[tokio::test]
	async fn a_committed_stream_ends_only_after_its_verified_cleanup_receipt() {
		for config_path in [false, true] {
			let hold = Arc::new(Semaphore::new(0));
			let collect = Arc::new(CollectReliance::default());
			let coordinator = hooked_stream_coordinator(
				Arc::new(ScriptedDescriptorTransport {
					attempts: Arc::new(AtomicUsize::new(1)),
					success_batch: batch(),
					completion: None,
				}),
				Arc::clone(&collect),
				RenewalCleanupHook {
					hold: Some(Arc::clone(&hold)),
					lose_supervisor: false,
				},
			);
			let mut stream = open_stream(&coordinator, config_path, 80).await;
			let mut batches = 0;
			loop {
				match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
					Ok(Some(item)) => {
						item.expect("a batch");
						batches += 1;
					}
					Ok(None) => {
						panic!("config_path={config_path}: a normal end while the cleanup is held")
					}
					Err(_) => break,
				}
			}
			assert_eq!(batches, 1, "config_path={config_path}");
			assert_eq!(
				kinds_of(&collect, "commit").last().unwrap().fields["result"],
				"committed"
			);
			assert!(
				kinds_of(&collect, "renewal_cleanup").is_empty(),
				"config_path={config_path}"
			);
			hold.add_permits(1);
			let end = tokio::time::timeout(Duration::from_secs(5), stream.next())
				.await
				.expect("the end after release");
			assert!(end.is_none(), "config_path={config_path}");
			let cleanup = kinds_of(&collect, "renewal_cleanup");
			assert_eq!(cleanup.len(), 1, "config_path={config_path}");
			assert_eq!(
				cleanup[0].fields["result"], "verified",
				"config_path={config_path}"
			);
		}
	}

	/// ADR 628 B1.C: a lost supervisor still ends the committed stream, and its receipt is recorded
	/// unverified (`supervisor_lost`), never as success. The committed result is unchanged.
	#[tokio::test]
	async fn a_lost_supervisor_ends_the_stream_with_an_unverified_receipt() {
		for config_path in [false, true] {
			let collect = Arc::new(CollectReliance::default());
			let coordinator = hooked_stream_coordinator(
				Arc::new(ScriptedDescriptorTransport {
					attempts: Arc::new(AtomicUsize::new(1)),
					success_batch: batch(),
					completion: None,
				}),
				Arc::clone(&collect),
				RenewalCleanupHook {
					hold: None,
					lose_supervisor: true,
				},
			);
			let mut stream = open_stream(&coordinator, config_path, 81).await;
			let mut items = Vec::new();
			while let Some(item) = tokio::time::timeout(Duration::from_secs(5), stream.next())
				.await
				.expect("progress")
			{
				items.push(item);
			}
			assert_eq!(items.len(), 1);
			assert!(
				items[0].is_ok(),
				"the committed result stands: {:?}",
				items[0]
			);
			let cleanup = kinds_of(&collect, "renewal_cleanup");
			assert_eq!(cleanup.len(), 1, "config_path={config_path}");
			assert_eq!(
				cleanup[0].fields["result"], "supervisor_lost",
				"config_path={config_path}"
			);
		}
	}

	/// ADR 628 B1.C: a failed query publishes its error promptly, while the cleanup is still held; the
	/// cleanup completes afterwards (observable as its own record), never before the failure.
	#[tokio::test]
	async fn a_failure_is_published_promptly_and_its_cleanup_completes_afterwards() {
		for config_path in [false, true] {
			let hold = Arc::new(Semaphore::new(0));
			let collect = Arc::new(CollectReliance::default());
			let coordinator = hooked_stream_coordinator(
				Arc::new(RefusingTransport),
				Arc::clone(&collect),
				RenewalCleanupHook {
					hold: Some(Arc::clone(&hold)),
					lose_supervisor: false,
				},
			);
			let mut stream = open_stream(&coordinator, config_path, 82).await;
			let item = tokio::time::timeout(Duration::from_secs(5), stream.next())
				.await
				.expect("the failure is published while the cleanup is held")
				.expect("an item");
			assert!(item.is_err(), "config_path={config_path}: {item:?}");
			assert!(
				kinds_of(&collect, "renewal_cleanup").is_empty(),
				"config_path={config_path}"
			);
			hold.add_permits(1);
			tokio::time::timeout(Duration::from_secs(5), async {
				while kinds_of(&collect, "renewal_cleanup").is_empty() {
					tokio::task::yield_now().await;
				}
			})
			.await
			.expect("the cleanup completes after the failure");
			assert_eq!(
				kinds_of(&collect, "renewal_cleanup")[0].fields["result"],
				"verified"
			);
		}
	}

	/// ADR 628 B1.C: a renewal attempt in flight when the query ends is aborted (recorded `aborted`)
	/// and drained before the verified receipt, which precedes the end of stream.
	#[tokio::test(start_paused = true)]
	async fn an_in_flight_renewal_is_aborted_and_drained_before_the_end() {
		let go = Arc::new(Semaphore::new(0));
		let collect = Arc::new(CollectReliance::default());
		let connects = Arc::new(AtomicUsize::new(0));
		let coordinator = hooked_stream_coordinator(
			Arc::new(GatedScriptTransport {
				go: Arc::clone(&go),
				batch: batch(),
			}),
			Arc::clone(&collect),
			RenewalCleanupHook::default(),
		)
		.with_reliance(
			crate::distributed::query_reliance::RelianceParams::production(Duration::from_millis(
				35_001,
			)),
			Arc::new(crate::distributed::query_reliance::TokioRelianceClock::new()),
		)
		.with_renewal_transport(Arc::new(PendingConnectTransport {
			connects: Arc::clone(&connects),
		}));
		let mut stream = open_stream(&coordinator, false, 83).await;
		// The renewal comes due 5 s after the issuance and stays inside its (never completing) connect.
		tokio::time::timeout(Duration::from_secs(30), async {
			while connects.load(Ordering::SeqCst) == 0 {
				tokio::time::sleep(Duration::from_millis(100)).await;
			}
		})
		.await
		.expect("a renewal attempt is in flight");
		go.add_permits(1);
		let mut items = 0;
		while let Some(item) = tokio::time::timeout(Duration::from_secs(30), stream.next())
			.await
			.expect("progress")
		{
			item.expect("a batch");
			items += 1;
		}
		assert_eq!(items, 1);
		let ends = kinds_of(&collect, "renewal_attempt_end");
		assert!(
			ends.iter().any(|e| e.fields["outcome"] == "aborted"),
			"{ends:?}"
		);
		let cleanup = kinds_of(&collect, "renewal_cleanup");
		assert_eq!(cleanup.len(), 1);
		assert_eq!(cleanup[0].fields["result"], "verified");
		assert!(
			ends.iter().all(|e| e.seq < cleanup[0].seq),
			"every attempt ended before the receipt"
		);
	}

	/// A transport whose connects never complete: a renewal loop stays live inside its exchange.
	struct PendingConnectTransport {
		connects: Arc<AtomicUsize>,
	}

	#[async_trait::async_trait]
	impl ToronTransport for PendingConnectTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			self.connects.fetch_add(1, Ordering::SeqCst);
			std::future::pending().await
		}
	}

	#[derive(Default)]
	struct CollectReliance(StdMutex<Vec<crate::distributed::query_reliance::RelianceEvent>>);

	impl crate::distributed::query_reliance::RelianceObserver for CollectReliance {
		fn record(&self, event: &crate::distributed::query_reliance::RelianceEvent) {
			self.0.lock().unwrap().push(event.clone());
		}
	}

	/// ADR 628 B1.C (instrument): an attempt aborted by renewal cleanup records an `aborted` end, and
	/// the cleanup result is recorded: `verified` after a normal shutdown, `supervisor_lost` after
	/// the supervisor is aborted.
	#[tokio::test]
	async fn aborted_attempts_and_cleanup_results_are_recorded() {
		for lose_supervisor in [false, true] {
			let clock = Arc::new(crate::distributed::query_reliance::SettableClock::at(
				1_000_000_000,
			));
			let collect = Arc::new(CollectReliance::default());
			let registry = ActiveQueryRegistry::new()
				.with_reliance(
					crate::distributed::query_reliance::RelianceParams::production(
						Duration::from_millis(35_001),
					),
					clock.clone(),
				)
				.with_reliance_observer(collect.clone());
			let guard = registry.register(TenantId(7), QueryId::from_u128(1));
			let handle = guard.handle();
			let connects = Arc::new(AtomicUsize::new(0));
			let owner = spawn_query_lease_renewal_task(
				Arc::new(PendingConnectTransport {
					connects: Arc::clone(&connects),
				}),
				Arc::new(MemoryManager::new(1, 1024 * 1024)),
				handle.clone(),
			);
			issue_worker(&handle, &scripted_worker());
			clock.advance(Duration::from_secs(6));
			let mut other = scripted_worker();
			other.worker_id = WorkerId::from_u128(9);
			issue_worker(&handle, &other);
			tokio::time::timeout(Duration::from_secs(5), async {
				while connects.load(Ordering::SeqCst) == 0 {
					tokio::task::yield_now().await;
				}
			})
			.await
			.expect("a loop is live inside its exchange");
			if lose_supervisor {
				owner.task.as_ref().expect("supervisor").abort();
			}
			let result = owner.shutdown().await;
			assert_eq!(result.is_err(), lose_supervisor);
			let events = collect.0.lock().unwrap().clone();
			let cleanup: Vec<_> = events
				.iter()
				.filter(|e| e.kind == "renewal_cleanup")
				.collect();
			assert_eq!(cleanup.len(), 1);
			assert_eq!(
				cleanup[0].fields["result"],
				if lose_supervisor {
					"supervisor_lost"
				} else {
					"verified"
				}
			);
			let ends: Vec<_> = events
				.iter()
				.filter(|e| e.kind == "renewal_attempt_end")
				.collect();
			assert_eq!(ends.len(), 1, "{ends:?}");
			assert_eq!(ends[0].fields["outcome"], "aborted");
			assert_eq!(
				events.iter().filter(|e| e.kind == "renewal_closed").count(),
				1,
				"closure is recorded once"
			);
		}
	}

	/// ADR 628 B1.C: only the supervisor's explicit `done`, sent after it has aborted and joined its
	/// loops, certifies cleanup. With a loop live inside an exchange, an aborted supervisor (its
	/// `JoinSet` dropped, its `done` sender closed) cannot make the receipt or `shutdown` report a
	/// verified cleanup.
	#[tokio::test]
	async fn a_lost_supervisor_cannot_certify_cleanup() {
		let (_guard, handle, clock, _) = settable_query();
		let connects = Arc::new(AtomicUsize::new(0));
		let owner = spawn_query_lease_renewal_task(
			Arc::new(PendingConnectTransport {
				connects: Arc::clone(&connects),
			}),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			handle.clone(),
		);
		let mut worker = scripted_worker();
		worker.worker_id = WorkerId::from_u128(8);
		issue_worker(&handle, &worker);
		clock.advance(Duration::from_secs(6));
		// Wake the due loop: it enters its exchange and stays live in the connect.
		let mut other = scripted_worker();
		other.worker_id = WorkerId::from_u128(9);
		issue_worker(&handle, &other);
		tokio::time::timeout(Duration::from_secs(5), async {
			while connects.load(Ordering::SeqCst) == 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.expect("a loop is live inside its exchange");
		let cleanup = owner.cleanup();
		owner.task.as_ref().expect("supervisor").abort();
		assert_eq!(
			cleanup.finished().await,
			Err(RenewalCleanupError::SupervisorLost)
		);
		assert_eq!(
			owner.shutdown().await,
			Err(RenewalCleanupError::SupervisorLost)
		);
	}

	#[tokio::test]
	async fn remote_cancel_fanout_records_error_when_connect_fails() {
		let metrics = Arc::new(CancellationMetrics::default());
		let coordinator = DistributedQueryCoordinator::new(
			Arc::new(MockCatalog::new()),
			Arc::new(StaticWorkerRegistry::new([scripted_worker()])),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			Arc::new(FailingCancelTransport),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_cancellation_metrics(Arc::clone(&metrics));

		coordinator.spawn_remote_cancel_fanout(
			CancellationTrigger::StatementTimeout,
			vec![RemoteTaskCancellation {
				identity: TaskIdentity::new(
					QueryId::from_u128(77),
					StageId::new(0),
					TaskId::new(0),
					AttemptId::first(),
				),
				worker: scripted_worker(),
			}],
		);

		wait_for_metric(
			&metrics,
			CancellationTrigger::StatementTimeout,
			CancellationOutcome::RemoteError,
		)
		.await;
	}

	#[tokio::test]
	async fn pre_admission_rejects_emit_no_cancel_task() {
		let connect_calls = Arc::new(AtomicUsize::new(0));
		let coordinator = DistributedQueryCoordinator::new(
			Arc::new(MockCatalog::new()),
			Arc::new(StaticWorkerRegistry::new([scripted_worker()])),
			Arc::new(MemoryManager::new(1, 1024)),
			Arc::new(CountingTransport {
				connect_calls: Arc::clone(&connect_calls),
			}),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_tenant_budget_policy(memory_only_rejection_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let _held = coordinator
			.admit_query(&context)
			.await
			.expect("saturate tenant admission before execution");

		let err = coordinator
			.execute_query_with_context(&count_plan(), context.clone())
			.await
			.expect_err("pre-admission rejection");

		assert!(matches!(
			err,
			QueryError::Execution(ExecutionError::BudgetExhausted { .. })
		));
		assert_eq!(connect_calls.load(Ordering::SeqCst), 0);
		assert!(
			!coordinator
				.cancel_query_for_tenant(
					context.tenant_id,
					context.query_id,
					CancellationTrigger::StatementTimeout
				)
				.was_active
		);
	}

	#[tokio::test]
	async fn admission_and_memory_release_after_cancellation() {
		let coordinator = DistributedQueryCoordinator::new(
			Arc::new(RefreshingCatalog::new(
				descriptor_with_identity(TableId(1), TableObjectId::new([1; 16]), 1),
				RefreshOutcome::Missing,
			)),
			Arc::new(StaticWorkerRegistry::new([scripted_worker()])),
			Arc::new(MemoryManager::new(1, 1024 * 1024)),
			Arc::new(CancelledTaskTransport),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_tenant_budget_policy(tenant_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);

		let err = coordinator
			.execute_query_with_context(&named_scan_plan(), context)
			.await
			.expect_err("cancelled task surfaces");

		assert!(matches!(
			err,
			QueryError::Runtime(RuntimeError::TaskFailure {
				kind: TaskFailureKind::Cancelled
			})
		));
		assert_eq!(coordinator.tenant_memory_used(TenantId(7)), ByteCount::ZERO);
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 0);
	}

	#[tokio::test]
	async fn reads_and_writes_each_spend_one_top_level_token() {
		let read_coordinator = distributed_coordinator_with_policy(rate_policy(1, 1));
		let read_context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let read = read_coordinator
			.execute_query_with_context(&named_scan_plan(), read_context.clone())
			.await
			.expect_err("read spends one token then fails bind");
		assert!(matches!(read, QueryError::Binding(_)));
		assert_rate_limited(
			read_coordinator
				.execute_query_with_context(&named_scan_plan(), read_context)
				.await
				.expect_err("second read has no token"),
			TenantId(7),
		);

		let write_coordinator = distributed_coordinator_with_policy(rate_policy(1, 1));
		let write_context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(8),
			TenantId(7),
			HlcBound(1),
		);
		let create = LogicalPlan::CreateTable {
			name: TableRef::new("rate_write"),
			schema: empty_create_table_schema(),
			row_identity: CreateTableRowIdentity::AppendOnly,
			if_not_exists: false,
			or_replace: false,
		};
		write_coordinator
			.execute_query_with_context(&create, write_context.clone())
			.await
			.expect("write spends one token and succeeds");
		assert_rate_limited(
			write_coordinator
				.execute_query_with_context(&create, write_context)
				.await
				.expect_err("second write has no token"),
			TenantId(7),
		);
	}

	#[tokio::test]
	async fn write_descriptor_evolved_invalidates_without_retrying() {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(
				TableId(1),
				descriptor_with_object(TableObjectId::new([1; 16]), 1),
			)
			.expect("register descriptor");
		let catalog = Arc::new(catalog);
		let writer = Arc::new(DescriptorEvolutionWriter {
			calls: AtomicUsize::new(0),
		});
		let executor = Executor::with_context(
			Arc::new(DescriptorEvolutionStorage {
				table_data: batch(),
				remaining_failures: AtomicUsize::new(0),
			}),
			ExecutionContext::new(
				SessionId(1),
				QueryId::from_u128(1),
				TenantId(1),
				HlcBound(1),
			),
		);
		let coordinator = QueryCoordinator::with_writer(
			catalog.clone(),
			executor,
			PlannerConfig::default(),
			writer.clone(),
		);
		let plan = LogicalPlan::Insert {
			table: TableRef::new("retry_table"),
			columns: None,
			rows: vec![vec![InsertLiteral::Int64(4)]],
			returning: None,
		};

		let err = coordinator
			.execute_query(&plan, None)
			.await
			.expect_err("write returns descriptor evolution");

		assert!(matches!(
			err,
			QueryError::Storage(StorageError::DescriptorEvolved {
				table_id: TableId(1)
			})
		));
		assert_eq!(writer.calls.load(Ordering::SeqCst), 1);
		assert!(
			catalog
				.resolve_table(&TableRef::new("retry_table"))
				.await
				.is_err()
		);
	}

	#[tokio::test]
	async fn distributed_coordinator_selects_query_budget_by_tenant() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());

		let tenant_7 = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let tenant_8 = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(8),
			TenantId(8),
			HlcBound(1),
		);

		let admitted_7 = coordinator
			.admit_query(&tenant_7)
			.await
			.expect("tenant 7 admitted");
		let admitted_8 = coordinator
			.admit_query(&tenant_8)
			.await
			.expect("tenant 8 admitted");

		assert_eq!(admitted_7.query_budget.memory_per_task, ByteCount::new(8));
		assert_eq!(admitted_8.query_budget.memory_per_task, ByteCount::new(16));
	}

	#[tokio::test]
	async fn aggregate_admission_rejects_before_bind_or_execution() {
		let coordinator = distributed_coordinator_with_policy(memory_only_rejection_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let _held = coordinator
			.admit_query(&context)
			.await
			.expect("saturate tenant 7");

		let err = coordinator
			.execute_query_with_context(&count_plan(), context)
			.await
			.expect_err("tenant aggregate should reject before catalog bind");

		assert!(matches!(
			err,
			QueryError::Execution(ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: _,
				requested: ByteCount(8),
				available: ByteCount(0),
				..
			})
		));
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 1);
	}

	#[tokio::test]
	async fn tenant_concurrency_limit_rejects_before_bind() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let held = coordinator
			.admit_query(&context)
			.await
			.expect("saturate tenant 7 concurrency");

		let err = coordinator
			.execute_query_with_context(&count_plan(), context)
			.await
			.expect_err("tenant concurrency should reject before catalog bind");

		assert!(matches!(
			err,
			QueryError::Execution(ExecutionError::ConcurrencyExhausted {
				tenant_id: TenantId(7),
				requested: 1,
				available: 0,
			})
		));
		assert_eq!(
			coordinator.tenant_memory_used(TenantId(7)),
			ByteCount::new(8)
		);
		drop(held);
		assert_eq!(coordinator.tenant_memory_used(TenantId(7)), ByteCount::ZERO);
	}

	#[tokio::test]
	async fn memory_rejection_releases_concurrency_reservation() {
		let coordinator = distributed_coordinator_with_policy(memory_only_rejection_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let held_memory = coordinator
			.admit_query(&context)
			.await
			.expect("saturate tenant 7 memory");

		let err = coordinator
			.execute_query_with_context(&count_plan(), context)
			.await
			.expect_err("tenant memory should reject after concurrency admission");

		assert!(matches!(
			err,
			QueryError::Execution(ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: _,
				requested: ByteCount(8),
				available: ByteCount(0),
				..
			})
		));
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 1);
		drop(held_memory);
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 0);
	}

	#[tokio::test]
	async fn tenant_concurrency_reservation_releases_on_error() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(8),
			TenantId(8),
			HlcBound(1),
		);

		let err = coordinator
			.execute_query_with_context(&count_plan(), context)
			.await
			.expect_err("missing descriptor should fail after admission");

		assert!(matches!(err, QueryError::Binding(_)));
		assert_eq!(coordinator.tenant_memory_used(TenantId(8)), ByteCount::ZERO);
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(8)), 0);
	}

	#[tokio::test]
	async fn tenant_concurrency_reservation_releases_on_success() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(9),
			TenantId(8),
			HlcBound(1),
		);
		let plan = LogicalPlan::CreateTable {
			name: TableRef::new("budget_success"),
			schema: empty_create_table_schema(),
			row_identity: CreateTableRowIdentity::AppendOnly,
			if_not_exists: false,
			or_replace: false,
		};

		let result = coordinator
			.execute_query_with_context(&plan, context)
			.await
			.expect("DDL succeeds after admission");

		assert!(result.is_empty());
		assert_eq!(coordinator.tenant_memory_used(TenantId(8)), ByteCount::ZERO);
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(8)), 0);
	}

	#[tokio::test]
	async fn one_tenant_saturated_does_not_block_another() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let tenant_7 = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let _tenant_7 = coordinator
			.admit_query(&tenant_7)
			.await
			.expect("saturate tenant 7");
		let tenant_8 = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(8),
			TenantId(8),
			HlcBound(1),
		);

		let admitted_8 = coordinator
			.admit_query(&tenant_8)
			.await
			.expect("tenant 8 admitted");

		assert_eq!(admitted_8.query_budget.memory_per_task, ByteCount::new(16));
		assert_eq!(
			coordinator.tenant_memory_used(TenantId(8)),
			ByteCount::new(16)
		);
	}

	#[tokio::test]
	async fn one_tenant_concurrency_saturation_does_not_block_another() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let tenant_7 = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let _tenant_7 = coordinator
			.admit_query(&tenant_7)
			.await
			.expect("saturate tenant 7 concurrency");
		let tenant_8 = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(8),
			TenantId(8),
			HlcBound(1),
		);

		let admitted_8 = coordinator
			.admit_query(&tenant_8)
			.await
			.expect("tenant 8 admitted");

		assert_eq!(admitted_8.query_budget.memory_per_task, ByteCount::new(16));
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(8)), 1);
	}

	fn coordinator(batch: RecordBatch, descriptor_failures: usize) -> QueryCoordinator {
		coordinator_with_refresh_object(batch, descriptor_failures, TableObjectId::new([1; 16]))
	}

	fn batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
			.expect("batch")
	}

	fn coordinator_with_refresh_object(
		batch: RecordBatch,
		descriptor_failures: usize,
		refresh_object: TableObjectId,
	) -> QueryCoordinator {
		coordinator_with_refresh_outcome(
			batch,
			descriptor_failures,
			RefreshOutcome::Binding(descriptor_with_identity(TableId(2), refresh_object, 2)),
		)
	}

	fn coordinator_with_refresh_outcome(
		batch: RecordBatch,
		descriptor_failures: usize,
		refresh: RefreshOutcome,
	) -> QueryCoordinator {
		let catalog = RefreshingCatalog::new(
			descriptor_with_identity(TableId(1), TableObjectId::new([1; 16]), 1),
			refresh,
		);
		let executor = Executor::with_context(
			Arc::new(DescriptorEvolutionStorage {
				table_data: batch,
				remaining_failures: AtomicUsize::new(descriptor_failures),
			}),
			crate::exec::ExecutionContext::new(
				SessionId(1),
				QueryId::from_u128(1),
				TenantId(1),
				HlcBound(1),
			),
		);
		QueryCoordinator::new(Arc::new(catalog), executor, PlannerConfig::default())
	}

	enum RefreshOutcome {
		Binding(TableBinding),
		Missing,
		ResourceExhausted,
	}

	fn distributed_coordinator_with_policy(
		policy: TenantBudgetPolicy,
	) -> DistributedQueryCoordinator<StaticWorkerRegistry, TcpTransport> {
		DistributedQueryCoordinator::new(
			Arc::new(MockCatalog::new()),
			Arc::new(StaticWorkerRegistry::new(Vec::new())),
			Arc::new(MemoryManager::new(1, 1024)),
			Arc::new(TcpTransport::new()),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		)
		.with_tenant_budget_policy(policy)
	}

	fn tenant_policy() -> TenantBudgetPolicy {
		let mut per_tenant = std::collections::HashMap::new();
		per_tenant.insert(
			TenantId(7),
			TenantBudget::new(ByteCount::new(8), ByteCount::new(8)),
		);
		per_tenant.insert(
			TenantId(8),
			TenantBudget::new(ByteCount::new(16), ByteCount::new(32)),
		);
		TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(64), ByteCount::new(64)),
			per_tenant,
		)
	}

	fn memory_only_rejection_policy() -> TenantBudgetPolicy {
		let mut per_tenant = std::collections::HashMap::new();
		per_tenant.insert(
			TenantId(7),
			TenantBudget::with_max_concurrent_queries(
				ByteCount::new(8),
				ByteCount::new(8),
				NonZeroU32::new(2).expect("nonzero"),
			),
		);
		TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(64), ByteCount::new(64)),
			per_tenant,
		)
	}

	fn one_query_tenant_policy() -> TenantBudgetPolicy {
		let mut per_tenant = std::collections::HashMap::new();
		per_tenant.insert(
			TenantId(7),
			TenantBudget::with_max_concurrent_queries(
				ByteCount::new(64),
				ByteCount::new(64),
				NonZeroU32::new(1).expect("one is nonzero"),
			),
		);
		TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(64), ByteCount::new(64)),
			per_tenant,
		)
	}

	fn rate_policy(max_queries_per_second: u32, query_burst_capacity: u32) -> TenantBudgetPolicy {
		let mut per_tenant = std::collections::HashMap::new();
		per_tenant.insert(
			TenantId(7),
			rate_budget(8, 64, 8, max_queries_per_second, query_burst_capacity),
		);
		TenantBudgetPolicy::new(
			TenantBudget::new(ByteCount::new(64), ByteCount::new(64)),
			per_tenant,
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

	fn is_rate_limited(error: &QueryError) -> bool {
		matches!(
			error,
			QueryError::Catalog(CatalogError::ResourceExhausted { message })
				if message.starts_with("tenant query rate limit exhausted for tenant ")
		)
	}

	fn assert_rate_limited(error: QueryError, tenant_id: TenantId) {
		match error {
			QueryError::Catalog(CatalogError::ResourceExhausted { message }) => {
				assert_eq!(
					message,
					format!(
						"tenant query rate limit exhausted for tenant {}",
						tenant_id.0
					)
				);
			}
			other => panic!("expected rate-limit ResourceExhausted, got {other:?}"),
		}
	}

	fn count_plan() -> LogicalPlan {
		LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("id")])],
		}
	}

	fn named_count_plan() -> LogicalPlan {
		LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("retry_table"),
				projection: None,
			}),
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("id")])],
		}
	}

	fn named_scan_plan() -> LogicalPlan {
		LogicalPlan::ScanNamed {
			table: TableRef::new("retry_table"),
			projection: None,
		}
	}

	fn empty_create_table_schema() -> CreateTableSchema {
		CreateTableSchema {
			columns: Vec::new(),
		}
	}

	fn coordinator_admission_policy() -> AdmissionPolicy {
		AdmissionPolicy {
			max_queued_queries: 1,
			max_queue_wait_ms: DurationMillis::new(50),
			shared_memory: ByteCount::ZERO,
			shared_concurrent_queries: 0,
		}
	}

	fn scripted_worker() -> WorkerEntry {
		WorkerEntry {
			worker_id: WorkerId::from_u128(7),
			endpoint: WorkerEndpoint::new("scripted://descriptor-worker"),
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(0),
				epoch: LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: ByteCount::new(1024 * 1024),
				simd_width: SimdLaneCount::new(1),
				spill_capacity_reserved: ByteCount::ZERO,
			},
			resources: WorkerResourceState {
				in_flight: ByteCount::ZERO,
				active_tasks: ActiveTaskCount::new(0),
			},
			status: WorkerStatus::Live,
		}
	}

	fn lease_test_identity() -> TaskIdentity {
		TaskIdentity::new(
			QueryId::from_u128(1),
			StageId::new(1),
			TaskId::new(1),
			AttemptId::first(),
		)
	}

	struct CancelRecordingTransport {
		seen: Arc<StdMutex<Vec<TaskIdentity>>>,
		acked: Arc<Notify>,
	}

	#[async_trait::async_trait]
	impl ToronTransport for CancelRecordingTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			let (client, mut server) = test_duplex(1024 * 1024);
			let seen = Arc::clone(&self.seen);
			let acked = Arc::clone(&self.acked);
			tokio::spawn(async move {
				let cancel = read_test_envelope(&mut server)
					.await
					.expect("read cancel task");
				assert_eq!(cancel.kind, MessageKind::CancelTask);
				let payload: CancelTaskPayload =
					decode_payload(&cancel.payload).expect("decode cancel task");
				seen.lock().expect("seen lock").push(payload.identity);
				send_test_payload(
					&mut server,
					MessageKind::TaskCancelled,
					EnvelopeIdentity::task(payload.identity),
					None,
					&TaskCancelledPayload {
						identity: payload.identity,
					},
				)
				.await
				.expect("send task cancelled");
				acked.notify_waiters();
			});
			Ok(Box::new(client))
		}
	}

	struct FailingCancelTransport;

	#[async_trait::async_trait]
	impl ToronTransport for FailingCancelTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			Err(QueryError::Internal {
				message: "cancel connect failed".to_string(),
			})
		}
	}

	struct CountingTransport {
		connect_calls: Arc<AtomicUsize>,
	}

	#[async_trait::async_trait]
	impl ToronTransport for CountingTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			self.connect_calls.fetch_add(1, Ordering::SeqCst);
			Err(QueryError::Internal {
				message: "unexpected dispatch".to_string(),
			})
		}
	}

	struct LeaseRecordingTransport {
		connect_calls: Arc<AtomicUsize>,
		fail_at: Option<usize>,
		/// ADR 628 B1.C: the first this many connects fail, then connects succeed.
		fail_first: usize,
	}

	/// ADR 628 B1.C: per-endpoint behaviour. The first `fail_first` connects to an endpoint fail; every
	/// lease is then `Applied`.
	struct AddressLeaseTransport {
		fail_first: std::collections::HashMap<String, usize>,
		connects: std::sync::Mutex<std::collections::HashMap<String, usize>>,
	}

	impl AddressLeaseTransport {
		fn new(fail_first: impl IntoIterator<Item = (String, usize)>) -> Self {
			Self {
				fail_first: fail_first.into_iter().collect(),
				connects: std::sync::Mutex::new(std::collections::HashMap::new()),
			}
		}

		fn connects(&self, address: &str) -> usize {
			self.connects
				.lock()
				.unwrap()
				.get(address)
				.copied()
				.unwrap_or(0)
		}
	}

	#[async_trait::async_trait]
	impl ToronTransport for AddressLeaseTransport {
		async fn connect(&self, address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			let ordinal = {
				let mut connects = self.connects.lock().unwrap();
				let n = connects.entry(address.to_string()).or_insert(0);
				*n += 1;
				*n - 1
			};
			if ordinal < self.fail_first.get(address).copied().unwrap_or(0) {
				return Err(QueryError::Internal {
					message: "injected lease renewal failure".to_string(),
				});
			}
			LeaseRecordingTransport {
				connect_calls: Arc::new(AtomicUsize::new(0)),
				fail_at: None,
				fail_first: 0,
			}
			.connect(address)
			.await
		}
	}

	#[async_trait::async_trait]
	impl ToronTransport for LeaseRecordingTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			let ordinal = self.connect_calls.fetch_add(1, Ordering::SeqCst);
			if self.fail_at.is_some_and(|fail_at| ordinal >= fail_at) || ordinal < self.fail_first {
				return Err(QueryError::Internal {
					message: "injected lease renewal failure".to_string(),
				});
			}
			let (client, mut server) = test_duplex(1024 * 1024);
			tokio::spawn(async move {
				let lease = read_test_envelope(&mut server)
					.await
					.expect("read query lease renewal");
				assert_eq!(lease.kind, MessageKind::QueryLease);
				let payload: QueryLeasePayload =
					decode_payload(&lease.payload).expect("decode query lease renewal");
				send_test_payload(
					&mut server,
					MessageKind::QueryLeaseAccepted,
					EnvelopeIdentity::query(payload.query_id),
					None,
					&QueryLeaseAcceptedPayload {
						query_id: payload.query_id,
						outcome: QueryLeaseOutcome::Applied {
							term_ms: payload.lease_term_millis,
						},
					},
				)
				.await
				.expect("ack query lease renewal");
			});
			Ok(Box::new(client))
		}
	}

	/// ADR 628 Stage A: a renewal transport whose connect resolves after `connect_delay` and whose
	/// worker acknowledges each lease after `ack_delay` (both on tokio's clock, paused in the pins).
	struct PhasedLeaseTransport {
		connect_delay: Duration,
		ack_delay: Duration,
		renewals: Arc<AtomicUsize>,
		/// ADR 628 B0: reply `Ignored` with this reason instead of `Applied`.
		ignore: Option<LeaseIgnored>,
		/// ADR 628 B0: reply `Applied` with this term instead of the requested one.
		term_override: Option<u64>,
		/// ADR 628 B0: the client stream's shutdown stalls forever.
		stall_shutdown: bool,
	}

	/// ADR 628 B0: a client stream whose shutdown never completes.
	struct StallingShutdown(DuplexStream);

	impl tokio::io::AsyncRead for StallingShutdown {
		fn poll_read(
			mut self: std::pin::Pin<&mut Self>,
			cx: &mut std::task::Context<'_>,
			buf: &mut tokio::io::ReadBuf<'_>,
		) -> std::task::Poll<std::io::Result<()>> {
			std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
		}
	}

	impl tokio::io::AsyncWrite for StallingShutdown {
		fn poll_write(
			mut self: std::pin::Pin<&mut Self>,
			cx: &mut std::task::Context<'_>,
			buf: &[u8],
		) -> std::task::Poll<std::io::Result<usize>> {
			std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
		}

		fn poll_flush(
			mut self: std::pin::Pin<&mut Self>,
			cx: &mut std::task::Context<'_>,
		) -> std::task::Poll<std::io::Result<()>> {
			std::pin::Pin::new(&mut self.0).poll_flush(cx)
		}

		fn poll_shutdown(
			self: std::pin::Pin<&mut Self>,
			_cx: &mut std::task::Context<'_>,
		) -> std::task::Poll<std::io::Result<()>> {
			std::task::Poll::Pending
		}
	}

	#[async_trait::async_trait]
	impl ToronTransport for PhasedLeaseTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			tokio::time::sleep(self.connect_delay).await;
			let (client, mut server) = test_duplex(1024 * 1024);
			let (ack_delay, renewals) = (self.ack_delay, self.renewals.clone());
			let (ignore, term_override) = (self.ignore, self.term_override);
			let stall_shutdown = self.stall_shutdown;
			tokio::spawn(async move {
				let Ok(lease) = read_test_envelope(&mut server).await else {
					return;
				};
				assert_eq!(lease.kind, MessageKind::QueryLease);
				let payload: QueryLeasePayload =
					decode_payload(&lease.payload).expect("decode query lease renewal");
				tokio::time::sleep(ack_delay).await;
				if send_test_payload(
					&mut server,
					MessageKind::QueryLeaseAccepted,
					EnvelopeIdentity::query(payload.query_id),
					None,
					&QueryLeaseAcceptedPayload {
						query_id: payload.query_id,
						outcome: match ignore {
							Some(reason) => QueryLeaseOutcome::Ignored { reason },
							None => QueryLeaseOutcome::Applied {
								term_ms: term_override.unwrap_or(payload.lease_term_millis),
							},
						},
					},
				)
				.await
				.is_ok()
				{
					renewals.fetch_add(1, Ordering::SeqCst);
				}
			});
			if stall_shutdown {
				return Ok(Box::new(StallingShutdown(client)));
			}
			Ok(Box::new(client))
		}
	}

	struct CancelledTaskTransport;

	#[async_trait::async_trait]
	impl ToronTransport for CancelledTaskTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			let (client, server) = test_duplex(1024 * 1024);
			tokio::spawn(async move {
				if let Err(error) = run_scripted_cancelled_worker(server).await {
					panic!("scripted cancelled worker failed: {error:?}");
				}
			});
			Ok(Box::new(client))
		}
	}

	async fn read_start_after_lifecycle(
		stream: &mut DuplexStream,
	) -> Result<Option<WireEnvelope>, QueryError> {
		let first = read_test_envelope(stream).await?;
		match first.kind {
			MessageKind::QueryLease => {
				let lease: QueryLeasePayload = decode_payload(&first.payload)?;
				send_test_payload(
					stream,
					MessageKind::QueryLeaseAccepted,
					EnvelopeIdentity::query(lease.query_id),
					None,
					&QueryLeaseAcceptedPayload {
						query_id: lease.query_id,
						outcome: QueryLeaseOutcome::Applied {
							term_ms: lease.lease_term_millis,
						},
					},
				)
				.await?;
				let start = read_test_envelope(stream).await?;
				if start.kind != MessageKind::StartTask {
					return Err(QueryError::UnexpectedMessage {
						expected: "StartTask after QueryLeaseAccepted",
					});
				}
				Ok(Some(start))
			}
			MessageKind::RetireQuery => {
				let retirement: RetireQueryPayload = decode_payload(&first.payload)?;
				send_test_payload(
					stream,
					MessageKind::QueryRetired,
					EnvelopeIdentity::query(retirement.query_id),
					None,
					&QueryRetiredPayload {
						query_id: retirement.query_id,
					},
				)
				.await?;
				Ok(None)
			}
			_ => Err(QueryError::UnexpectedMessage {
				expected: "QueryLease or RetireQuery",
			}),
		}
	}

	async fn wait_for_metric(
		metrics: &CancellationMetrics,
		trigger: CancellationTrigger,
		outcome: CancellationOutcome,
	) {
		let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
		loop {
			if metrics.get(trigger, outcome) == 1 {
				return;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"metric {trigger:?}/{outcome:?} was not recorded"
			);
			tokio::time::sleep(std::time::Duration::from_millis(5)).await;
		}
	}

	async fn run_scripted_cancelled_worker(mut stream: DuplexStream) -> Result<(), QueryError> {
		let Some(start) = read_start_after_lifecycle(&mut stream).await? else {
			return Ok(());
		};
		let payload: StartTaskPayload = decode_payload(&start.payload)?;
		let identity = payload.spec.identity;
		send_test_payload(
			&mut stream,
			MessageKind::TaskStarted,
			EnvelopeIdentity::task(identity),
			None,
			&TaskStartedPayload { identity },
		)
		.await?;
		send_test_payload(
			&mut stream,
			MessageKind::TaskFailed,
			EnvelopeIdentity::task(identity),
			None,
			&TaskFailedPayload {
				identity,
				kind: TaskFailureKind::Cancelled,
			},
		)
		.await
	}

	struct ScriptedDescriptorTransport {
		attempts: Arc<AtomicUsize>,
		success_batch: RecordBatch,
		completion: Option<Arc<ScriptedWorkerCompletion>>,
	}

	#[derive(Default)]
	struct ScriptedWorkerCompletion {
		completed: AtomicUsize,
		failed: AtomicBool,
		notify: Notify,
	}

	#[async_trait::async_trait]
	impl ToronTransport for ScriptedDescriptorTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
			let success_batch = self.success_batch.clone();
			let completion = self.completion.clone();
			let (client, server) = test_duplex(1024 * 1024);
			tokio::spawn(async move {
				let result = run_scripted_descriptor_worker(server, attempt, success_batch).await;
				if let Some(completion) = completion {
					completion.failed.store(result.is_err(), Ordering::SeqCst);
					completion.completed.fetch_add(1, Ordering::SeqCst);
					completion.notify.notify_waiters();
				} else if let Err(error) = result {
					panic!("scripted descriptor worker failed: {error:?}");
				}
			});
			Ok(Box::new(client))
		}
	}

	async fn wait_for_scripted_workers(completion: &ScriptedWorkerCompletion, expected: usize) {
		tokio::time::timeout(Duration::from_secs(1), async {
			while completion.completed.load(Ordering::SeqCst) < expected {
				completion.notify.notified().await;
			}
		})
		.await
		.expect("scripted query and retirement workers complete");
	}

	async fn run_scripted_descriptor_worker(
		mut stream: DuplexStream,
		attempt: usize,
		success_batch: RecordBatch,
	) -> Result<(), QueryError> {
		let Some(start) = read_start_after_lifecycle(&mut stream).await? else {
			return Ok(());
		};
		let payload: StartTaskPayload = decode_payload(&start.payload)?;
		let identity = payload.spec.identity;
		send_test_payload(
			&mut stream,
			MessageKind::TaskStarted,
			EnvelopeIdentity::task(identity),
			None,
			&TaskStartedPayload { identity },
		)
		.await?;

		if attempt == 0 {
			send_test_payload(
				&mut stream,
				MessageKind::TaskFailed,
				EnvelopeIdentity::task(identity),
				None,
				&TaskFailedPayload {
					identity,
					kind: TaskFailureKind::DescriptorEvolved {
						table_id: TableId(1),
						expected: None,
						current: None,
					},
				},
			)
			.await?;
			return Ok(());
		}

		let stream_seq = StreamSeq::new(0);
		send_test_payload(
			&mut stream,
			MessageKind::TaskBatch,
			EnvelopeIdentity::task(identity),
			Some(stream_seq),
			&TaskBatchPayload {
				identity,
				stream_seq,
				payload: ArrowIpcBytes::new(encode_record_batch_for_test(&success_batch)?),
			},
		)
		.await?;
		send_test_payload(
			&mut stream,
			MessageKind::TaskComplete,
			EnvelopeIdentity::task(identity),
			None,
			&TaskCompletePayload {
				identity,
				output: TaskCompletionOutput::FinalResult {
					output_digest: None,
				},
				telemetry: None,
			},
		)
		.await
	}

	async fn read_test_envelope(stream: &mut DuplexStream) -> Result<WireEnvelope, QueryError> {
		let mut len_buf = [0_u8; 4];
		stream.read_exact(&mut len_buf).await?;
		let len = usize::try_from(u32::from_le_bytes(len_buf)).expect("u32 fits usize");
		let mut payload = vec![0_u8; len];
		stream.read_exact(&mut payload).await?;
		Ok(WireEnvelope::decode(&payload)?)
	}

	async fn send_test_payload<Payload>(
		stream: &mut DuplexStream,
		kind: MessageKind,
		identity: EnvelopeIdentity,
		stream_seq: Option<StreamSeq>,
		payload: &Payload,
	) -> Result<(), QueryError>
	where
		Payload: serde::Serialize,
	{
		let envelope =
			WireEnvelope::try_new(kind, identity, stream_seq, None, encode_payload(payload))?;
		let encoded = envelope.encode();
		let len = u32::try_from(encoded.len())
			.map_err(|_| QueryError::Internal {
				message: "test envelope too large".to_string(),
			})?
			.to_le_bytes();
		stream.write_all(&len).await?;
		stream.write_all(&encoded).await?;
		Ok(())
	}

	fn encode_record_batch_for_test(batch: &RecordBatch) -> Result<Vec<u8>, QueryError> {
		let mut buffer = Vec::new();
		{
			let mut writer =
				StreamWriter::try_new(&mut buffer, &batch.schema()).map_err(StorageError::from)?;
			writer.write(batch).map_err(StorageError::from)?;
			writer.finish().map_err(StorageError::from)?;
		}
		Ok(buffer)
	}

	struct RefreshingCatalog {
		inner: std::sync::Mutex<RefreshingCatalogState>,
		metrics: Arc<DescriptorRefreshMetrics>,
	}

	struct RefreshingCatalogState {
		current: Option<TableBinding>,
		refresh: Option<RefreshOutcome>,
		refs: Vec<TableRef>,
	}

	impl RefreshingCatalog {
		fn new(stale: TableBinding, refresh: RefreshOutcome) -> Self {
			Self {
				inner: std::sync::Mutex::new(RefreshingCatalogState {
					current: Some(stale),
					refresh: Some(refresh),
					refs: vec![
						TableRef::new("retry_table"),
						TableRef::with_schema("default", "retry_table"),
					],
				}),
				metrics: Arc::new(DescriptorRefreshMetrics::default()),
			}
		}

		fn missing(refresh: RefreshOutcome) -> Self {
			Self {
				inner: std::sync::Mutex::new(RefreshingCatalogState {
					current: None,
					refresh: Some(refresh),
					refs: vec![
						TableRef::new("retry_table"),
						TableRef::with_schema("default", "retry_table"),
					],
				}),
				metrics: Arc::new(DescriptorRefreshMetrics::default()),
			}
		}
	}

	#[async_trait::async_trait]
	impl Catalog for RefreshingCatalog {
		fn scoped(self: Arc<Self>, _tenant_id: TenantId) -> Arc<dyn Catalog> {
			self
		}

		fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
			Arc::clone(&self.metrics)
		}

		async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
			let mut state = self.inner.lock().expect("RefreshingCatalog lock poisoned");
			if !state.refs.contains(table) {
				return Err(CatalogError::TableNotFound {
					table: table.table.clone(),
				});
			}
			if state.current.is_none() {
				match state.refresh.take() {
					Some(RefreshOutcome::Binding(binding)) => state.current = Some(binding),
					Some(RefreshOutcome::Missing) | None => {}
					Some(RefreshOutcome::ResourceExhausted) => {
						return Err(CatalogError::ResourceExhausted {
							message: "tenant descriptor sync limit exhausted".to_string(),
						});
					}
				}
			}
			state
				.current
				.clone()
				.ok_or_else(|| CatalogError::TableNotFound {
					table: table.table.clone(),
				})
		}

		async fn resolve_table_local(
			&self,
			table: &TableRef,
		) -> Result<TableBinding, CatalogError> {
			let state = self.inner.lock().expect("RefreshingCatalog lock poisoned");
			if !state.refs.contains(table) {
				return Err(CatalogError::TableNotFound {
					table: table.table.clone(),
				});
			}
			state
				.current
				.clone()
				.ok_or_else(|| CatalogError::TableNotFound {
					table: table.table.clone(),
				})
		}

		async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
			let state = self.inner.lock().expect("RefreshingCatalog lock poisoned");
			state
				.current
				.as_ref()
				.filter(|binding| binding.local_table_id == table)
				.cloned()
				.ok_or_else(|| CatalogError::TableNotFound {
					table: format!("{table:?}"),
				})
		}

		async fn resolve_durable_logical_type_by_name(
			&self,
			name: &str,
		) -> Result<crate::ir::LogicalTypeSpec, CatalogError> {
			Err(CatalogError::LogicalTypeNotFound {
				name: name.to_string(),
			})
		}

		async fn invalidate_table_descriptor(
			&self,
			table: TableId,
		) -> Result<Option<InvalidatedDescriptor>, CatalogError> {
			let mut state = self.inner.lock().expect("RefreshingCatalog lock poisoned");
			let Some(binding) = state
				.current
				.take()
				.filter(|binding| binding.local_table_id == table)
			else {
				self.metrics.record_missing();
				return Ok(None);
			};
			self.metrics.record_invalidated();
			Ok(Some(InvalidatedDescriptor {
				table_id: table,
				table_object_id: binding.table,
				fingerprint: binding.fingerprint,
				refs: state.refs.clone(),
			}))
		}

		async fn create_table_with_outcome(
			&self,
			_name: &TableRef,
			_schema: SchemaRef,
			_row_identity: CreateTableRowIdentity,
			_if_not_exists: bool,
		) -> Result<crate::catalog::CreateTableOutcome, CatalogError> {
			Err(CatalogError::AdminFailed {
				message: "RefreshingCatalog does not create tables".to_string(),
			})
		}

		async fn drop_table(
			&self,
			_name: &TableRef,
			_if_exists: bool,
		) -> Result<Option<TableBinding>, CatalogError> {
			Err(CatalogError::AdminFailed {
				message: "RefreshingCatalog does not drop tables".to_string(),
			})
		}
	}

	struct DescriptorEvolutionStorage {
		table_data: RecordBatch,
		remaining_failures: AtomicUsize,
	}

	#[async_trait::async_trait]
	impl StorageScan for DescriptorEvolutionStorage {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			if self
				.remaining_failures
				.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
					if remaining > 0 {
						Some(remaining - 1)
					} else {
						None
					}
				})
				.is_ok()
			{
				return Err(StorageError::DescriptorEvolved {
					table_id: request.table_id,
				});
			}
			Ok(ScanStream::from_batches(
				self.table_data.schema(),
				request.session_id,
				request.query_id,
				crate::storage::ScanOrigin::BaseRelation,
				vec![self.table_data.clone()],
			))
		}
	}

	struct DescriptorEvolutionWriter {
		calls: AtomicUsize,
	}

	#[async_trait::async_trait]
	impl StorageWrite for DescriptorEvolutionWriter {
		async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError> {
			self.calls.fetch_add(1, Ordering::SeqCst);
			Err(StorageError::DescriptorEvolved {
				table_id: request.table_id,
			})
		}
	}

	fn descriptor_with_identity(
		table_id: TableId,
		table_object_id: TableObjectId,
		schema_generation: u64,
	) -> TableBinding {
		TableBinding::from_descriptor(
			table_id,
			&descriptor_with_object(table_object_id, schema_generation),
		)
		.expect("table binding")
	}

	fn descriptor_with_object(
		table_object_id: TableObjectId,
		schema_generation: u64,
	) -> TableSchemaDescriptor {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		TableSchemaDescriptor::new(
			table_object_id,
			schema_generation,
			"default",
			"retry_table",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
