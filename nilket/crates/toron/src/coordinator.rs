use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

mod delete;
mod write;

use crate::binder::{PlannerConfig, bind};
use crate::catalog::{Catalog, InvalidatedDescriptor};
use crate::distributed::budget::{
	AdmissionAttempt, AdmissionPolicy, AdmissionReservation, AdmissionResource, BudgetRejection,
	QueryBudget, TaskBudget, TenantAdmissionController, TenantAdmissionRejection,
	TenantAdmissionRejectionCounters, TenantBudgetPolicy, TenantConcurrencyRejection,
};
use crate::distributed::context::TaskExecutionContext;
use crate::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use crate::distributed::identity::QueryId;
use crate::distributed::scheduler::{DistributedScheduler, SchedulerError};
use crate::distributed::worker_registry::WorkerRegistry;
use crate::error::{
	CatalogError, ExecutionError, QueryError, RuntimeError, StorageError, TaskFailureKind,
};
use crate::exec::{ExecutionContext, Executor};
use crate::ir::{ColumnOrdinal, ExecutionPlan, FilterDisposition, LogicalPlan, MorselId, TableId};
use crate::server::{NetworkTaskDispatcher, memory::MemoryManager, transport::ToronTransport};
use crate::storage::{
	DeleteRowsOutcome, DeleteRowsRequest, DropTableStorageRequest, RowIdentityProjection,
	ScanStream, StorageAdmin, StorageDeleteRows, StorageWrite, TenantId, UnsupportedStorageAdmin,
	WriteOutcome, WriteRequest,
};

pub(crate) async fn materialize_scan_stream(
	stream: ScanStream,
) -> Result<Vec<RecordBatch>, StorageError> {
	stream.into_batches().try_collect().await
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

	pub fn tenant_memory_used(&self, tenant_id: TenantId) -> crate::distributed::budget::ByteCount {
		self.tenant_admission.used_memory(tenant_id)
	}

	pub fn tenant_concurrency_active(&self, tenant_id: TenantId) -> u32 {
		self.tenant_admission.active_concurrency(tenant_id)
	}

	pub fn tenant_admission_controller(&self) -> TenantAdmissionController {
		self.tenant_admission.clone()
	}

	pub fn admission_rejection_counters(&self) -> TenantAdmissionRejectionCounters {
		self.admission_rejections.clone()
	}

	pub async fn execute_query(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>, QueryError> {
		self.execute_query_with_context(plan, self.context.clone())
			.await
	}

	pub async fn execute_query_with_context(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let admitted = self.admit_query(&context).await?;
		let query_budget = admitted.query_budget;
		let result = self
			.execute_query_with_admission(plan, context, query_budget)
			.await;
		drop(admitted);
		result
	}

	async fn execute_query_with_admission(
		&self,
		plan: &LogicalPlan,
		context: ExecutionContext,
		query_budget: QueryBudget,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
		if matches!(
			plan,
			LogicalPlan::CreateTable { .. } | LogicalPlan::DropTable { .. }
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
		) {
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
			let source_executor = DistributedMutationSourceExecutor {
				coordinator: self,
				context: &context,
				query_budget,
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

		let mut retried_descriptor_evolution = false;
		let mut invalidated = Vec::new();
		loop {
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
			if !invalidated.is_empty() {
				reject_replacement_object(catalog.as_ref(), &invalidated).await?;
			}
			match self
				.execute_bound_distributed_with_context(bound, &context, query_budget)
				.await
			{
				Ok(results) => return Ok(results),
				Err(error)
					if descriptor_evolved_table_id(&error).is_some()
						&& !retried_descriptor_evolution =>
				{
					if let Some(stale) =
						invalidate_descriptor_evolution(catalog.as_ref(), &error).await?
					{
						invalidated.push(stale);
					}
					retried_descriptor_evolution = true;
				}
				Err(error) if descriptor_evolved_table_id(&error).is_some() => {
					return Err(QueryError::SchemaInstability);
				}
				Err(error) => return Err(error),
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
	) -> Result<Vec<RecordBatch>, QueryError> {
		let stages = fragment_plan(bound, self.fragment_config(context, query_budget))
			.map_err(fragment_error)?;
		let mut scheduler = DistributedScheduler::new(self.registry.clone());
		let mut dispatcher =
			NetworkTaskDispatcher::new(self.memory.clone(), self.transport.clone());
		let run = scheduler
			.dispatch_loop(&stages, &mut dispatcher)
			.await
			.map_err(scheduler_error)?;
		Ok(run.final_results)
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
	}
}

struct AdmittedQuery {
	query_budget: QueryBudget,
	_reservation: AdmissionReservation,
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
			if_not_exists,
			or_replace,
		} => {
			if *or_replace {
				drop_existing_table_with_storage_admin(catalog, admin, context, name, true).await?;
			}
			catalog
				.create_table(name, schema.clone(), *if_not_exists)
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
			.execute_bound_distributed_with_context(plan.clone(), self.context, self.query_budget)
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
	match bound {
		ExecutionPlan::Insert {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			rows,
		} => {
			let payload = write::materialize_insert_batch(schema, rows)?;
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
			Ok(vec![write::affected_count_batch(outcome.rows)?])
		}
		ExecutionPlan::InsertSelect {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			target_columns,
			source,
		} => {
			let source_batches = source_executor.execute_read_plan(source).await?;
			let Some(payload) =
				write::materialize_insert_select_batch(schema, target_columns, &source_batches)?
			else {
				return Ok(vec![write::affected_count_batch(0)?]);
			};
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
			Ok(vec![write::affected_count_batch(outcome.rows)?])
		}
		ExecutionPlan::CreateTableAsSelect {
			table,
			schema,
			or_replace,
			source,
		} => {
			let source_batches = source_executor.execute_read_plan(source).await?;
			if *or_replace {
				drop_existing_table_with_storage_admin(catalog, admin, context, table, true)
					.await?;
			}
			let binding = catalog.create_table(table, schema.clone(), false).await?;
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
			profile_hint,
			..
		} => {
			let candidate_plan = derive_delete_candidate_plan(bound)?;
			let candidate_batches = source_executor
				.execute_delete_candidate_plan(&candidate_plan)
				.await?;
			let row_key_ids = delete::extract_delete_row_key_ids(&candidate_batches)?;
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
			Ok(vec![delete::delete_count_batch(outcome.deleted)?])
		}
		other => Err(QueryError::Internal {
			message: format!("execute_write_plan called with non-DML plan: {other:?}"),
		}),
	}
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
		profile_hint: *profile_hint,
		primary_key_columns: Vec::new(),
	};

	if let (Some(predicate), None) = (predicate, pbc_program) {
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
		if matches!(
			plan,
			LogicalPlan::CreateTable { .. } | LogicalPlan::DropTable { .. }
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
		) {
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
			return self.execute_write_plan(catalog.as_ref(), &bound).await;
		}

		let mut retried_descriptor_evolution = false;
		let mut invalidated = Vec::new();
		loop {
			let bound = bind(plan, catalog.as_ref(), &self.config).await?;
			if !invalidated.is_empty() {
				reject_replacement_object(catalog.as_ref(), &invalidated).await?;
			}
			match self.executor.execute_bound(&bound, morsel).await {
				Ok(results) => return Ok(results),
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
	use crate::distributed::identity::{LeaseEpoch, StreamSeq, WorkerId};
	use crate::distributed::task::TaskCompletionOutput;
	use crate::distributed::worker_registry::StaticWorkerRegistry;
	use crate::distributed::worker_registry::{
		ActiveTaskCount, SimdLaneCount, UnixMillis, WorkerCapabilities, WorkerEndpoint,
		WorkerEntry, WorkerLease, WorkerResourceState, WorkerStatus,
	};
	use crate::error::BindingError;
	use crate::ir::{AggregateFunction, Expr, InsertLiteral, QueryId, SessionId, TableId};
	use crate::server::transport::{TcpTransport, ToronStream, ToronTransport};
	use crate::storage::{
		HlcBound, ScanRequest, ScanStream, StorageScan, StorageWrite, TenantId, WriteOutcome,
		WriteRequest,
	};
	use crate::wire::{
		ArrowIpcBytes, EnvelopeIdentity, MessageKind, StartTaskPayload, TaskBatchPayload,
		TaskCompletePayload, TaskFailedPayload, TaskStartedPayload, WireEnvelope, decode_payload,
		encode_payload,
	};
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
	use arrow::ipc::writer::StreamWriter;
	use std::num::NonZeroU32;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex as test_duplex};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
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
			schema: Arc::new(Schema::empty()),
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
			schema: Arc::new(Schema::empty()),
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

		async fn create_table(
			&self,
			_name: &TableRef,
			_schema: SchemaRef,
			_if_not_exists: bool,
		) -> Result<TableBinding, CatalogError> {
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
