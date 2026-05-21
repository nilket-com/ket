use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

mod delete;
mod write;

use crate::binder::{PlannerConfig, bind};
use crate::catalog::{Catalog, InvalidatedDescriptor};
use crate::distributed::budget::{
	AdmissionResource, BudgetRejection, QueryBudget, TaskBudget, TenantAdmissionRejectionCounters,
	TenantBudgetPolicy, TenantConcurrencyAccountant, TenantConcurrencyRejection,
	TenantConcurrencyReservation, TenantMemoryAccountant, TenantMemoryReservation,
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
	tenant_memory: TenantMemoryAccountant,
	tenant_concurrency: TenantConcurrencyAccountant,
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
			tenant_memory: TenantMemoryAccountant::new(tenant_budgets.clone()),
			tenant_concurrency: TenantConcurrencyAccountant::new(tenant_budgets.clone()),
			admission_rejections: TenantAdmissionRejectionCounters::new(&tenant_budgets),
			tenant_budgets,
			context,
			writer,
			deleter,
			admin,
		}
	}

	pub fn with_tenant_budget_policy(mut self, tenant_budgets: TenantBudgetPolicy) -> Self {
		self.tenant_memory = TenantMemoryAccountant::new(tenant_budgets.clone());
		self.tenant_concurrency = TenantConcurrencyAccountant::new(tenant_budgets.clone());
		self.admission_rejections = TenantAdmissionRejectionCounters::new(&tenant_budgets);
		self.tenant_budgets = tenant_budgets;
		self
	}

	pub fn tenant_memory_used(&self, tenant_id: TenantId) -> crate::distributed::budget::ByteCount {
		self.tenant_memory.used(tenant_id)
	}

	pub fn tenant_concurrency_active(&self, tenant_id: TenantId) -> u32 {
		self.tenant_concurrency.active(tenant_id)
	}

	pub fn tenant_memory_accountant(&self) -> TenantMemoryAccountant {
		self.tenant_memory.clone()
	}

	pub fn tenant_concurrency_accountant(&self) -> TenantConcurrencyAccountant {
		self.tenant_concurrency.clone()
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
		let admitted = self.admit_query(&context)?;
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

	fn admit_query(&self, context: &ExecutionContext) -> Result<AdmittedQuery, QueryError> {
		let query_budget = self
			.tenant_budgets
			.query_budget_for_base(context.tenant_id, self.query_budget);
		let concurrency_reservation = self
			.tenant_concurrency
			.try_reserve(context.tenant_id)
			.map_err(|rejection| {
				self.admission_rejections
					.increment(context.tenant_id, AdmissionResource::Concurrency);
				concurrency_rejection_query_error(rejection)
			})?;
		let reservation = self
			.tenant_memory
			.try_reserve(context.tenant_id, query_budget.memory_per_task)
			.map_err(|rejection| {
				self.admission_rejections
					.increment(context.tenant_id, AdmissionResource::Memory);
				budget_rejection_query_error(rejection)
			})?;
		Ok(AdmittedQuery {
			query_budget,
			_concurrency_reservation: concurrency_reservation,
			_reservation: reservation,
		})
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
	_concurrency_reservation: TenantConcurrencyReservation,
	_reservation: TenantMemoryReservation,
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
	use crate::distributed::budget::{BudgetDimension, ByteCount, TenantBudget};
	use crate::distributed::worker_registry::StaticWorkerRegistry;
	use crate::error::BindingError;
	use crate::ir::{AggregateFunction, Expr, InsertLiteral, QueryId, SessionId, TableId};
	use crate::server::transport::TcpTransport;
	use crate::storage::{
		HlcBound, ScanRequest, ScanStream, StorageScan, StorageWrite, TenantId, WriteOutcome,
		WriteRequest,
	};
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
	use std::sync::atomic::{AtomicUsize, Ordering};
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

	#[test]
	fn distributed_coordinator_selects_query_budget_by_tenant() {
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
			.expect("tenant 7 admitted");
		let admitted_8 = coordinator
			.admit_query(&tenant_8)
			.expect("tenant 8 admitted");

		assert_eq!(admitted_7.query_budget.memory_per_task, ByteCount::new(8));
		assert_eq!(admitted_8.query_budget.memory_per_task, ByteCount::new(16));
	}

	#[tokio::test]
	async fn aggregate_admission_rejects_before_bind_or_execution() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let _held = coordinator
			.tenant_memory
			.try_reserve(TenantId(7), ByteCount::new(8))
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
		let _held = coordinator
			.tenant_concurrency
			.try_reserve(TenantId(7))
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
		assert_eq!(coordinator.tenant_memory_used(TenantId(7)), ByteCount::ZERO);
	}

	#[tokio::test]
	async fn memory_rejection_releases_concurrency_reservation() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let context = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(7),
			TenantId(7),
			HlcBound(1),
		);
		let _held_memory = coordinator
			.tenant_memory
			.try_reserve(TenantId(7), ByteCount::new(8))
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

	#[test]
	fn one_tenant_saturated_does_not_block_another() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let _tenant_7 = coordinator
			.tenant_memory
			.try_reserve(TenantId(7), ByteCount::new(8))
			.expect("saturate tenant 7");
		let tenant_8 = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(8),
			TenantId(8),
			HlcBound(1),
		);

		let admitted_8 = coordinator
			.admit_query(&tenant_8)
			.expect("tenant 8 admitted");

		assert_eq!(admitted_8.query_budget.memory_per_task, ByteCount::new(16));
		assert_eq!(
			coordinator.tenant_memory_used(TenantId(8)),
			ByteCount::new(16)
		);
	}

	#[test]
	fn one_tenant_concurrency_saturation_does_not_block_another() {
		let coordinator = distributed_coordinator_with_policy(tenant_policy());
		let _tenant_7 = coordinator
			.tenant_concurrency
			.try_reserve(TenantId(7))
			.expect("saturate tenant 7 concurrency");
		let tenant_8 = ExecutionContext::new(
			SessionId(1),
			QueryId::from_u128(8),
			TenantId(8),
			HlcBound(1),
		);

		let admitted_8 = coordinator
			.admit_query(&tenant_8)
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
