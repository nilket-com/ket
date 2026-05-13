use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

use crate::binder::{PlannerConfig, bind};
use crate::catalog::Catalog;
use crate::error::{ExecutionError, QueryError, StorageError};
use crate::exec::Executor;
use crate::ir::{LogicalPlan, MorselId};
use crate::storage::ScanStream;

pub(crate) async fn materialize_scan_stream(
	stream: ScanStream,
) -> Result<Vec<RecordBatch>, StorageError> {
	stream.into_batches().try_collect().await
}

pub struct QueryCoordinator {
	catalog: Arc<dyn Catalog>,
	executor: Executor,
	config: PlannerConfig,
}

impl QueryCoordinator {
	pub fn new(catalog: Arc<dyn Catalog>, executor: Executor, config: PlannerConfig) -> Self {
		Self {
			catalog,
			executor,
			config,
		}
	}

	pub async fn execute_query(
		&self,
		plan: &LogicalPlan,
		morsel: Option<(MorselId, u32)>,
	) -> Result<Vec<RecordBatch>, QueryError> {
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
		LocalityPolicyV1, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptorV1,
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
			crate::exec::ExecutionContext::new(SessionId(1), QueryId(1), TenantId(1), HlcBound(1)),
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
			aggr_expr: vec![(
				AggregateFunction::CountAll,
				vec![Expr::Column("id".to_string())],
			)],
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

	fn descriptor() -> TableSchemaDescriptorV1 {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		TableSchemaDescriptorV1::new(
			TableObjectId::new([1; 16]),
			1,
			"default",
			"retry_table",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
