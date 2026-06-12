use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt};

mod delete;
mod write;

use crate::binder::{
	PlannerConfig, bind, bind_explain_analyze, measured_explain_analyze_verdicts,
	render_explain_analyze,
};
use crate::catalog::{Catalog, InvalidatedDescriptor};
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
use crate::distributed::scheduler::{DistributedScheduler, SchedulerError, StagePlan};
use crate::distributed::task::{JoinRuntimeObservation, JoinRuntimeObservationMode, TaskTelemetry};
use crate::distributed::task_state::TaskKey;
use crate::distributed::worker_registry::WorkerRegistry;
use crate::error::{
	CatalogError, ExecutionError, QueryError, RuntimeError, StorageError, TaskFailureKind,
};
use crate::exec::{ExecutionContext, Executor, materialize_scalar_rows};
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
	RowIdentityProjection, ScanStream, StorageAdmin, StorageDeleteRows, StorageWrite, TenantId,
	UnsupportedStorageAdmin, WriteOutcome, WriteRequest,
};
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

const REMOTE_CANCEL_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);
const REMOTE_CANCEL_ACK_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_REMOTE_CANCEL_FANOUTS: usize = 64;
const DISTRIBUTED_RESULT_STREAM_DEPTH: usize = 16;

type RemoteCancelSpawner =
	Arc<dyn Fn(CancellationTrigger, Vec<RemoteTaskCancellation>) + Send + Sync>;

struct DistributedAttemptOutput {
	final_results: Vec<RecordBatch>,
	task_telemetry: Vec<TaskTelemetry>,
}

pub(crate) async fn materialize_scan_stream(
	stream: ScanStream,
) -> Result<Vec<RecordBatch>, StorageError> {
	stream.into_batches().try_collect().await
}

pub(crate) async fn materialize_scan_stream_bounded(
	stream: ScanStream,
	limit: usize,
) -> Result<Vec<RecordBatch>, StorageError> {
	let mut stream = stream.into_batches();
	let mut rows = 0usize;
	let mut batches = Vec::new();
	while rows < limit {
		let Some(batch) = stream.next().await else {
			break;
		};
		let batch = batch?;
		if batch.num_rows() == 0 {
			continue;
		}
		let remaining = limit - rows;
		if batch.num_rows() <= remaining {
			rows += batch.num_rows();
			batches.push(batch);
		} else {
			batches.push(batch.slice(0, remaining));
			break;
		}
	}
	Ok(batches)
}

pub struct DistributedResultStream {
	receiver: mpsc::Receiver<Result<RecordBatch, QueryError>>,
	active_query: ActiveQueryHandle,
	cancel_remote: RemoteCancelSpawner,
	finished: bool,
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
		}
	}
}

impl futures::Stream for DistributedResultStream {
	type Item = Result<RecordBatch, QueryError>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		match self.receiver.poll_recv(cx) {
			Poll::Ready(Some(Err(error))) => {
				self.finished = true;
				Poll::Ready(Some(Err(error)))
			}
			Poll::Ready(None) => {
				self.finished = true;
				Poll::Ready(None)
			}
			other => other,
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
		}
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
			)
			.await;
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
		});
		Ok(DistributedResultStream::new(
			receiver,
			stream_active_query,
			cancel_remote,
		))
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
		let context = context.with_downstream_cancellation(Some(
			active_query_handle.downstream_cancellation_context(),
		));
		let result = self
			.execute_query_with_admission(plan, context, query_budget, Some(active_query_handle))
			.await;
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
		let context = context.with_downstream_cancellation(Some(
			active_query_handle.downstream_cancellation_context(),
		));
		let result = self
			.execute_query_with_config_admission(
				plan,
				context,
				query_budget,
				Some(active_query_handle),
				planner_config,
			)
			.await;
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
			)
			.await;
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

	async fn execute_explain_analyze_distributed(
		&self,
		catalog: &dyn Catalog,
		input: &LogicalPlan,
		context: &ExecutionContext,
		query_budget: QueryBudget,
		active_query: Option<ActiveQueryHandle>,
		planner_config: &PlannerConfig,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let binding = bind_explain_analyze(input, catalog, planner_config).await?;
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
	) -> Result<Vec<RecordBatch>, QueryError> {
		let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
		if let LogicalPlan::ExplainAnalyze { input } = plan {
			return self
				.execute_explain_analyze_distributed(
					catalog.as_ref(),
					input,
					&context,
					query_budget,
					active_query,
					&self.config,
				)
				.await;
		}
		if matches!(
			plan,
			LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::DropTable { .. }
		) {
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
			return execute_catalog_plan(catalog.as_ref(), self.admin.as_ref(), &context, &bound)
				.await;
		}
		if matches!(plan, LogicalPlan::Insert { .. }) {
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
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
				&source_executor,
				&bound,
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
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
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
				&source_executor,
				&bound,
			)
			.await;
			if let Err(error) = &result {
				invalidate_descriptor_evolution_on_error(catalog.as_ref(), error).await?;
			}
			return result;
		}

		let mut planner_config = self.config.clone();
		let mut retried_broadcast_replay = false;
		let mut fragment_id_namespace = 0;
		let mut retried_descriptor_evolution = false;
		let mut invalidated = Vec::new();
		loop {
			let bound = bind(plan, catalog.as_ref(), &planner_config).await?;
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
	) -> Result<Vec<RecordBatch>, QueryError> {
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
			let bound = bind(plan, catalog.as_ref(), &planner_config).await?;
			return execute_catalog_plan(catalog.as_ref(), self.admin.as_ref(), &context, &bound)
				.await;
		}
		if matches!(plan, LogicalPlan::Insert { .. }) {
			let bound = bind(plan, catalog.as_ref(), &planner_config).await?;
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
				&source_executor,
				&bound,
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
			let bound = bind(plan, catalog.as_ref(), &planner_config).await?;
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
				&source_executor,
				&bound,
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
			let bound = bind(plan, catalog.as_ref(), &planner_config).await?;
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
			TaskBudget::from_query_budget(query_budget),
		)
		.with_task_context(TaskExecutionContext::from_execution_context(context))
		.with_join_runtime_observation_mode(context.join_runtime_observation_mode.clone())
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
) -> Result<(), QueryError>
where
	Registry: WorkerRegistry + ?Sized + 'static,
	Transport: ToronTransport + ?Sized + 'static,
{
	let mut config = config;
	let measure_runtime_verdict = should_measure_runtime_verdict(
		config.measured_accrual_sample_rate,
		&config.measured_accrual_counter,
	);
	let mut retried_broadcast_replay = false;
	let mut fragment_id_namespace = 0;
	let mut retried_descriptor_evolution = false;
	let mut invalidated = Vec::new();
	loop {
		let measured_binding = if measure_runtime_verdict {
			Some(
				bind_explain_analyze(&plan, catalog.as_ref(), &config)
					.await
					.map_err(QueryError::Binding)?,
			)
		} else {
			None
		};
		let bound = match &measured_binding {
			Some(binding) => binding.plan.clone(),
			None => bind(&plan, catalog.as_ref(), &config)
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
				TaskBudget::from_query_budget(query_budget),
			)
			.with_task_context(TaskExecutionContext::from_execution_context(
				&execution_context,
			))
			.with_join_runtime_observation_mode(
				execution_context.join_runtime_observation_mode.clone(),
			)
			.with_id_namespace(fragment_id_namespace),
		)
		.map_err(fragment_error)?;
		let mut scheduler = DistributedScheduler::new(registry.clone())
			.with_max_in_flight_tasks(query_budget.max_in_flight_tasks);
		let dispatcher = NetworkTaskDispatcher::new(memory.clone(), transport.clone())
			.with_active_query(Some(active_query.clone()))
			.with_final_result_sender(Some(sender.clone()), Some(batch_sent.clone()));
		match scheduler.dispatch_loop(&stages, &dispatcher).await {
			Ok(run) => {
				if let Some(binding) = measured_binding.as_ref() {
					let observations =
						join_observations_from_task_telemetry(&run.telemetry.task_telemetry);
					let mut touched = Vec::new();
					for verdict in measured_explain_analyze_verdicts(binding, &observations)
						.map_err(QueryError::Binding)?
					{
						touched.push(
							config
								.runtime_evidence
								.record_verdict(verdict.kind, verdict.verdict),
						);
					}
					config.runtime_evidence.flush_keys(touched).await;
				}
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
	task_telemetry: &[TaskTelemetry],
) -> Vec<JoinRuntimeObservation> {
	task_telemetry
		.iter()
		.flat_map(|telemetry| telemetry.join_observations.iter().cloned())
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
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => collect_replayable_broadcast_join_ids(input, ids),
		ExecutionPlan::EmptyRelation { .. }
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
		requested: rejection.requested,
		available: rejection.available,
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
	QueryError::Internal {
		message: error.to_string(),
	}
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
				drop_existing_table_with_storage_admin(catalog, admin, context, name, true).await?;
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
			table_id: binding.legacy_table_id,
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

async fn execute_write_plan_with_source(
	catalog: &dyn Catalog,
	writer: &dyn StorageWrite,
	deleter: &dyn StorageDeleteRows,
	admin: &dyn StorageAdmin,
	context: &ExecutionContext,
	source_executor: &dyn MutationSourceExecutor,
	bound: &ExecutionPlan,
) -> Result<Vec<RecordBatch>, QueryError> {
	let logical_types = catalog.logical_type_registry();
	match bound {
		ExecutionPlan::Insert {
			legacy_table_id,
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
					table_id: *legacy_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					payload,
					profile_hint: context.profile_hint,
					trace_context: context.trace_context,
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
			legacy_table_id,
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
					table_id: *legacy_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					payload,
					profile_hint: context.profile_hint,
					trace_context: context.trace_context,
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
			legacy_table_id,
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
					table_id: *legacy_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					payload,
					profile_hint: Some(*profile_hint),
					trace_context: context.trace_context,
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
			if_not_exists,
			or_replace,
			source,
		} => {
			if *if_not_exists {
				match catalog.resolve_table(table).await {
					Ok(existing) => {
						if existing.schema.as_ref() == schema.as_ref() {
							return Ok(Vec::new());
						}
						return Err(CatalogError::SchemaMismatch {
							table: query_table_name(table),
						}
						.into());
					}
					Err(CatalogError::TableNotFound { .. }) => {}
					Err(error) => return Err(error.into()),
				}
			}
			let source_batches = source_executor.execute_read_plan(source).await?;
			if *or_replace {
				drop_existing_table_with_storage_admin(catalog, admin, context, table, true)
					.await?;
			}
			let outcome = catalog
				.create_table_with_outcome(
					table,
					schema.clone(),
					CreateTableRowIdentity::AppendOnly,
					*if_not_exists,
				)
				.await?;
			if !outcome.created {
				return Ok(Vec::new());
			}
			let binding = outcome.binding;
			let target_columns = (0..schema.fields().len())
				.map(|index| ColumnOrdinal(u32::try_from(index).expect("column ordinal overflow")))
				.collect::<Vec<_>>();
			let payload = match write::materialize_insert_select_batch(
				schema,
				&target_columns,
				&source_batches,
			)? {
				Some(payload) => payload,
				None => write::materialize_empty_record_batch(schema)?,
			};
			write::validate_refined_logical_columns("CTAS", &logical_types, &payload)?;
			writer
				.write(WriteRequest {
					session_id: context.session_id,
					query_id: context.query_id,
					tenant_id: context.tenant_id,
					table_id: binding.legacy_table_id,
					table_object_id: binding.table,
					physical_descriptor_fingerprint: binding.fingerprint,
					payload,
					profile_hint: context.profile_hint,
					trace_context: context.trace_context,
				})
				.await?;
			Ok(Vec::new())
		}
		ExecutionPlan::Delete {
			legacy_table_id,
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
					table_id: *legacy_table_id,
					table_object_id: *table,
					physical_descriptor_fingerprint: *fingerprint,
					row_key_ids,
					profile_hint: Some(*profile_hint),
					trace_context: context.trace_context,
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
		legacy_table_id,
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
		legacy_table_id: *legacy_table_id,
		table: *table,
		fingerprint: *fingerprint,
		schema: schema.clone(),
		projection: None,
		pbc_program: pbc_program.clone(),
		row_identity: RowIdentityProjection::Append,
		row_limit: None,
		profile_hint: *profile_hint,
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
		Self {
			catalog,
			executor,
			config,
			writer,
			deleter,
			admin: Arc::new(UnsupportedStorageAdmin),
		}
	}

	pub fn with_storage_admin(mut self, admin: Arc<dyn StorageAdmin>) -> Self {
		self.admin = admin;
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
		}
	}

	pub async fn execute_query(
		&self,
		plan: &LogicalPlan,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let catalog = Arc::clone(&self.catalog).scoped(self.executor.context().tenant_id);
		if let LogicalPlan::ExplainAnalyze { input } = plan {
			return self
				.execute_explain_analyze(catalog.as_ref(), input, morsel)
				.await;
		}
		if matches!(
			plan,
			LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::DropTable { .. }
		) {
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
			return execute_catalog_plan(
				catalog.as_ref(),
				self.admin.as_ref(),
				self.executor.context(),
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
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
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
				Some(bind_explain_analyze(plan, catalog.as_ref(), &self.config).await?)
			} else {
				None
			};
			let bound = match &measured_binding {
				Some(binding) => binding.plan.clone(),
				None => bind(plan, catalog.as_ref(), &self.config).await?,
			};
			if !invalidated.is_empty() {
				reject_replacement_object(catalog.as_ref(), &invalidated).await?;
			}
			let telemetry = measured_binding
				.as_ref()
				.map(|_| Arc::new(Mutex::new(TaskTelemetry::default())));
			let result = if let Some(binding) = measured_binding.as_ref() {
				let context = self
					.executor
					.context()
					.clone()
					.with_task_telemetry(telemetry.as_ref().expect("telemetry").clone())
					.with_join_runtime_observation_mode(analyze_observation_mode(
						binding.replay_ids.clone(),
					));
				Executor::with_context(self.executor.storage_arc(), context)
					.execute_bound(&bound, morsel)
					.await
			} else {
				self.executor.execute_bound(&bound, morsel).await
			};
			match result {
				Ok(results) => {
					if let Some(binding) = measured_binding.as_ref() {
						let observations = join_observations_from_local_telemetry(
							telemetry.as_ref().expect("telemetry"),
						);
						let mut touched = Vec::new();
						for verdict in measured_explain_analyze_verdicts(binding, &observations)? {
							touched.push(
								self.config
									.runtime_evidence
									.record_verdict(verdict.kind, verdict.verdict),
							);
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

	async fn execute_write_plan(
		&self,
		catalog: &dyn Catalog,
		bound: &ExecutionPlan,
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
			&source_executor,
			bound,
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
	) -> Result<Vec<RecordBatch>, QueryError> {
		let binding = bind_explain_analyze(input, catalog, &self.config).await?;
		let telemetry = Arc::new(Mutex::new(TaskTelemetry::default()));
		let context = self
			.executor
			.context()
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
	use crate::catalog::{DescriptorRefreshMetrics, MockCatalog, TableBinding, TableRef};
	use crate::distributed::budget::{BudgetDimension, ByteCount, DurationMillis, TenantBudget};
	use crate::distributed::identity::{
		AttemptId, LeaseEpoch, StageId, StreamSeq, TaskId, TaskIdentity, WorkerId,
	};
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
		ArrowIpcBytes, CancelTaskPayload, EnvelopeIdentity, MessageKind, StartTaskPayload,
		TaskBatchPayload, TaskCancelledPayload, TaskCompletePayload, TaskFailedPayload,
		TaskStartedPayload, WireEnvelope, decode_payload, encode_payload,
	};
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
	use arrow::ipc::writer::StreamWriter;
	use std::num::NonZeroU32;
	use std::sync::Mutex as StdMutex;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex as test_duplex};
	use tokio::sync::Notify;
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy,
		TableObjectId, TableSchemaDescriptor,
	};

	#[tokio::test]
	async fn coordinator_retries_once_on_descriptor_evolved() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
		)
		.expect("batch");
		let coordinator = coordinator(batch, 1);
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

		assert_eq!(attempts.load(Ordering::SeqCst), 2);
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].num_rows(), 3);
		assert_eq!(coordinator.tenant_memory_used(TenantId(7)), ByteCount::ZERO);
		assert_eq!(coordinator.tenant_concurrency_active(TenantId(7)), 0);
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
				requested: ByteCount(8),
				available: ByteCount(0),
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
				requested: ByteCount(8),
				available: ByteCount(0),
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
		let start = read_test_envelope(&mut stream).await?;
		if start.kind != MessageKind::StartTask {
			return Err(QueryError::UnexpectedMessage {
				expected: "StartTask",
			});
		}
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
	}

	#[async_trait::async_trait]
	impl ToronTransport for ScriptedDescriptorTransport {
		async fn connect(&self, _address: &str) -> Result<Box<dyn ToronStream>, QueryError> {
			let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
			let success_batch = self.success_batch.clone();
			let (client, server) = test_duplex(1024 * 1024);
			tokio::spawn(async move {
				if let Err(error) =
					run_scripted_descriptor_worker(server, attempt, success_batch).await
				{
					panic!("scripted descriptor worker failed: {error:?}");
				}
			});
			Ok(Box::new(client))
		}
	}

	async fn run_scripted_descriptor_worker(
		mut stream: DuplexStream,
		attempt: usize,
		success_batch: RecordBatch,
	) -> Result<(), QueryError> {
		let start = read_test_envelope(&mut stream).await?;
		if start.kind != MessageKind::StartTask {
			return Err(QueryError::UnexpectedMessage {
				expected: "StartTask",
			});
		}
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
				.filter(|binding| binding.legacy_table_id == table)
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
				.filter(|binding| binding.legacy_table_id == table)
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
