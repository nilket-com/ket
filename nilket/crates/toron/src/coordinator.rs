use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

mod write;

use crate::binder::{PlannerConfig, bind};
use crate::catalog::Catalog;
use crate::distributed::budget::{QueryBudget, TaskBudget};
use crate::distributed::fragment::{FragmentError, FragmentPlanConfig, fragment_plan};
use crate::distributed::identity::QueryId;
use crate::distributed::scheduler::{DistributedScheduler, SchedulerError};
use crate::distributed::worker_registry::WorkerRegistry;
use crate::error::{ExecutionError, QueryError, StorageError};
use crate::exec::Executor;
use crate::ir::{ColumnOrdinal, ExecutionPlan, LogicalPlan, MorselId};
use crate::server::{NetworkTaskDispatcher, memory::MemoryManager, transport::ToronTransport};
use crate::storage::{ScanStream, StorageWrite, WriteOutcome, WriteRequest};

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
		Self {
			catalog,
			registry,
			memory,
			transport,
			config,
			query_budget,
		}
	}

	pub async fn execute_query(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>, QueryError> {
		let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
		if matches!(bound, ExecutionPlan::CreateTable { .. }) {
			return execute_catalog_plan(self.catalog.as_ref(), &bound).await;
		}
		if matches!(bound, ExecutionPlan::Insert { .. }) {
			return Err(QueryError::Internal {
				message: "distributed INSERT execution is not wired to a write-context-bearing coordinator"
					.to_string(),
			});
		}
		if matches!(bound, ExecutionPlan::InsertSelect { .. }) {
			return Err(QueryError::Internal {
				message: "distributed INSERT INTO ... SELECT execution is not wired to a write-context-bearing coordinator"
					.to_string(),
			});
		}
		if matches!(bound, ExecutionPlan::CreateTableAsSelect { .. }) {
			return Err(QueryError::Internal {
				message: "distributed CREATE TABLE AS SELECT execution is not wired to a write-context-bearing coordinator"
					.to_string(),
			});
		}

		let stages = fragment_plan(
			bound,
			FragmentPlanConfig::new(
				QueryId::new(),
				TaskBudget::from_query_budget(self.query_budget),
			),
		)
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

async fn execute_catalog_plan(
	catalog: &dyn Catalog,
	bound: &ExecutionPlan,
) -> Result<Vec<RecordBatch>, QueryError> {
	match bound {
		ExecutionPlan::CreateTable {
			name,
			schema,
			if_not_exists,
		} => {
			catalog
				.create_table(name, schema.clone(), *if_not_exists)
				.await?;
			Ok(Vec::new())
		}
		other => Err(QueryError::Internal {
			message: format!("execute_catalog_plan called with non-DDL plan: {other:?}"),
		}),
	}
}

pub struct QueryCoordinator {
	catalog: Arc<dyn Catalog>,
	executor: Executor,
	config: PlannerConfig,
	writer: Arc<dyn StorageWrite>,
}

impl QueryCoordinator {
	pub fn new(catalog: Arc<dyn Catalog>, executor: Executor, config: PlannerConfig) -> Self {
		Self::with_writer(catalog, executor, config, Arc::new(UnsupportedStorageWrite))
	}

	pub fn with_writer(
		catalog: Arc<dyn Catalog>,
		executor: Executor,
		config: PlannerConfig,
		writer: Arc<dyn StorageWrite>,
	) -> Self {
		Self {
			catalog,
			executor,
			config,
			writer,
		}
	}

	pub async fn execute_query(
		&self,
		plan: &LogicalPlan,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, QueryError> {
		if matches!(plan, LogicalPlan::CreateTable { .. }) {
			let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
			return execute_catalog_plan(self.catalog.as_ref(), &bound).await;
		}
		if matches!(
			plan,
			LogicalPlan::Insert { .. }
				| LogicalPlan::InsertSelect { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
		) {
			let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
			return self.execute_write_plan(&bound).await;
		}

		let mut retried_descriptor_evolution = false;
		loop {
			let bound = bind(plan, self.catalog.as_ref(), &self.config).await?;
			match self.executor.execute_bound(&bound, morsel).await {
				Ok(results) => return Ok(results),
				Err(ExecutionError::Storage(StorageError::DescriptorEvolved))
					if !retried_descriptor_evolution =>
				{
					retried_descriptor_evolution = true;
				}
				Err(ExecutionError::Storage(StorageError::DescriptorEvolved)) => {
					return Err(QueryError::SchemaInstability);
				}
				Err(error) => return Err(QueryError::Execution(error)),
			}
		}
	}

	async fn execute_write_plan(
		&self,
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
				let context = self.executor.context();
				self.writer
					.write(WriteRequest {
						session_id: context.session_id,
						query_id: context.query_id,
						tenant_id: context.tenant_id,
						table_id: *legacy_table_id,
						table_object_id: *table,
						physical_descriptor_fingerprint: *fingerprint,
						payload,
						profile_hint: context.profile_hint,
						trace_context: context.trace_context.clone(),
					})
					.await?;
				Ok(Vec::new())
			}
			ExecutionPlan::InsertSelect {
				legacy_table_id,
				table,
				fingerprint,
				schema,
				target_columns,
				source,
			} => {
				let source_batches = self.executor.execute_bound(source, None).await?;
				let Some(payload) = write::materialize_insert_select_batch(
					schema,
					target_columns,
					&source_batches,
				)?
				else {
					return Ok(Vec::new());
				};
				let context = self.executor.context();
				self.writer
					.write(WriteRequest {
						session_id: context.session_id,
						query_id: context.query_id,
						tenant_id: context.tenant_id,
						table_id: *legacy_table_id,
						table_object_id: *table,
						physical_descriptor_fingerprint: *fingerprint,
						payload,
						profile_hint: context.profile_hint,
						trace_context: context.trace_context.clone(),
					})
					.await?;
				Ok(Vec::new())
			}
			ExecutionPlan::CreateTableAsSelect {
				table,
				schema,
				source,
			} => {
				let binding = self
					.catalog
					.create_table(table, schema.clone(), false)
					.await?;
				let source_batches = self.executor.execute_bound(source, None).await?;
				let target_columns = (0..schema.fields().len())
					.map(|index| {
						ColumnOrdinal(u32::try_from(index).expect("column ordinal overflow"))
					})
					.collect::<Vec<_>>();
				let payload = match write::materialize_insert_select_batch(
					schema,
					&target_columns,
					&source_batches,
				)? {
					Some(payload) => payload,
					None => write::materialize_empty_record_batch(schema)?,
				};
				let context = self.executor.context();
				self.writer
					.write(WriteRequest {
						session_id: context.session_id,
						query_id: context.query_id,
						tenant_id: context.tenant_id,
						table_id: binding.legacy_table_id,
						table_object_id: binding.table,
						physical_descriptor_fingerprint: binding.fingerprint,
						payload,
						profile_hint: context.profile_hint,
						trace_context: context.trace_context.clone(),
					})
					.await?;
				Ok(Vec::new())
			}
			other => Err(QueryError::Internal {
				message: format!("execute_write_plan called with non-DML plan: {other:?}"),
			}),
		}
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::MockCatalog;
	use crate::ir::{AggregateFunction, Expr, QueryId, SessionId, TableId};
	use crate::storage::{HlcBound, ScanRequest, ScanStream, StorageScan, TenantId};
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
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
		let plan = count_plan();

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
		let plan = count_plan();

		let err = coordinator
			.execute_query(&plan, None)
			.await
			.expect_err("second descriptor evolution fails");

		assert!(matches!(err, QueryError::SchemaInstability));
	}

	fn coordinator(batch: RecordBatch, descriptor_failures: usize) -> QueryCoordinator {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), descriptor())
			.expect("register descriptor");
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
				return Err(StorageError::DescriptorEvolved);
			}
			Ok(ScanStream::from_batches(
				self.table_data.schema(),
				request.session_id,
				request.query_id,
				vec![self.table_data.clone()],
			))
		}
	}

	fn descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		TableSchemaDescriptor::new(
			TableObjectId::new([1; 16]),
			1,
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
