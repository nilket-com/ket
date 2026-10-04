//! C1: deterministic coordinator candidate-to-write orderings, unit layer only.
//! The gate is a cfg(test) MutationSourceExecutor decorator: it holds real Executor
//! output after candidate materialization and before the mutation writer is called.
//! Writers capture requests, not Wendelon durability or final table state.
use super::*;
use crate::binder::bind;
use crate::catalog::{MockCatalog, TableRef};
use crate::ir::{CreateTableRowIdentity, SessionId};
use crate::sql_planner::SqlPlanner;
use crate::storage::{HlcBound, InMemoryTableStorage};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Mutex;
use tokio::sync::{Notify, Semaphore};

struct CandidateGate<'a> {
	inner: LocalMutationSourceExecutor<'a>,
	arrived: Notify,
	release: Semaphore,
}

#[async_trait::async_trait]
impl MutationSourceExecutor for CandidateGate<'_> {
	async fn execute_read_plan(&self, p: &ExecutionPlan) -> Result<Vec<RecordBatch>, QueryError> {
		let result = self.inner.execute_read_plan(p).await?;
		self.arrived.notify_one();
		self.release.acquire().await.unwrap().forget();
		Ok(result)
	}
	async fn execute_delete_candidate_plan(
		&self,
		p: &ExecutionPlan,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let result = self.inner.execute_delete_candidate_plan(p).await?;
		self.arrived.notify_one();
		self.release.acquire().await.unwrap().forget();
		Ok(result)
	}
}

#[derive(Default)]
struct CaptureMutations {
	sequence: Mutex<Vec<(QueryId, &'static str, usize, Option<i64>)>>,
}

#[async_trait::async_trait]
impl StorageWrite for CaptureMutations {
	async fn write(&self, r: WriteRequest) -> Result<WriteOutcome, StorageError> {
		let amount = r
			.payload
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap()
			.value(0);
		assert_eq!(r.payload.num_rows(), 1);
		assert_eq!(
			r.operation_id.unwrap().client_uuid,
			*r.query_id.get().as_bytes()
		);
		self.sequence.lock().unwrap().push((
			r.query_id,
			"update",
			r.payload.num_rows(),
			Some(amount),
		));
		Ok(WriteOutcome {
			rows: r.payload.num_rows(),
		})
	}
}

#[async_trait::async_trait]
impl StorageDeleteRows for CaptureMutations {
	async fn delete_rows(&self, r: DeleteRowsRequest) -> Result<DeleteRowsOutcome, StorageError> {
		assert_eq!(r.row_key_ids.len(), 1);
		assert_eq!(r.operation_id.client_uuid, *r.query_id.get().as_bytes());
		self.sequence
			.lock()
			.unwrap()
			.push((r.query_id, "delete", r.row_key_ids.len(), None));
		Ok(DeleteRowsOutcome {
			requested: 1,
			deleted: 1,
			already_deleted: 0,
			not_found: 0,
		})
	}
}

async fn bound_interleaving(left_delete: bool, left_first: bool) {
	let catalog = MockCatalog::new();
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int64, false),
	]));
	let binding = catalog
		.create_table_with_outcome(
			&TableRef::new("c1"),
			schema.clone(),
			CreateTableRowIdentity::PrimaryKey {
				column: "id".into(),
			},
			false,
		)
		.await
		.unwrap()
		.binding;
	let storage = Arc::new(InMemoryTableStorage::new());
	storage
		.write(WriteRequest {
			session_id: SessionId(1),
			query_id: QueryId::new(),
			tenant_id: TenantId(1),
			table_id: binding.local_table_id,
			table_object_id: binding.table,
			physical_descriptor_fingerprint: binding.fingerprint,
			payload: RecordBatch::try_new(
				schema,
				vec![
					Arc::new(Int64Array::from(vec![1])),
					Arc::new(Int64Array::from(vec![10])),
				],
			)
			.unwrap(),
			profile_hint: None,
			trace_context: None,
			operation_id: None,
			deadline: None,
		})
		.await
		.unwrap();
	let left_context =
		ExecutionContext::new(SessionId(1), QueryId::new(), TenantId(1), HlcBound(0));
	let right_context =
		ExecutionContext::new(SessionId(1), QueryId::new(), TenantId(1), HlcBound(0));
	let left_executor = Executor::with_context(storage.clone(), left_context.clone());
	let right_executor = Executor::with_context(storage, right_context.clone());
	let gate = |executor| CandidateGate {
		inner: LocalMutationSourceExecutor { executor },
		arrived: Notify::new(),
		release: Semaphore::new(0),
	};
	let left_gate = gate(&left_executor);
	let right_gate = gate(&right_executor);
	let planner = SqlPlanner::new();
	let left = bind(
		&planner
			.plan(if left_delete {
				"DELETE FROM c1 WHERE id = 1"
			} else {
				"UPDATE c1 SET amount = amount + 1 WHERE id = 1"
			})
			.unwrap(),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.unwrap();
	let right = bind(
		&planner
			.plan("UPDATE c1 SET amount = amount + 1 WHERE id = 1")
			.unwrap(),
		&catalog,
		&PlannerConfig::default(),
	)
	.await
	.unwrap();
	let mutations = CaptureMutations::default();
	let left_done = Notify::new();
	let right_done = Notify::new();
	let admin = UnsupportedStorageAdmin;
	let run_left = async {
		let r = execute_write_plan_with_source(
			&catalog,
			&mutations,
			&mutations,
			&admin,
			&left_context,
			mutation_operation_id(*left_context.query_id.get().as_bytes(), 0),
			&left_gate,
			&left,
			&mut None,
		)
		.await;
		left_done.notify_one();
		r
	};
	let run_right = async {
		let r = execute_write_plan_with_source(
			&catalog,
			&mutations,
			&mutations,
			&admin,
			&right_context,
			mutation_operation_id(*right_context.query_id.get().as_bytes(), 0),
			&right_gate,
			&right,
			&mut None,
		)
		.await;
		right_done.notify_one();
		r
	};
	let control = async {
		left_gate.arrived.notified().await;
		right_gate.arrived.notified().await;
		assert!(
			mutations.sequence.lock().unwrap().is_empty(),
			"both candidate sets held before either mutation"
		);
		if left_first {
			left_gate.release.add_permits(1);
			left_done.notified().await;
			right_gate.release.add_permits(1);
		} else {
			right_gate.release.add_permits(1);
			right_done.notified().await;
			left_gate.release.add_permits(1);
		}
	};
	let (a, b, ()) = tokio::time::timeout(Duration::from_secs(5), async {
		tokio::join!(run_left, run_right, control)
	})
	.await
	.expect("causal gates finish");
	a.unwrap();
	b.unwrap();
	let sequence = mutations.sequence.lock().unwrap();
	let left_record = (
		left_context.query_id,
		if left_delete { "delete" } else { "update" },
		1,
		if left_delete { None } else { Some(11) },
	);
	let right_record = (right_context.query_id, "update", 1, Some(11));
	assert_eq!(
		*sequence,
		if left_first {
			vec![left_record, right_record]
		} else {
			vec![right_record, left_record]
		}
	);
	println!(
		"ADR629_C1 left_delete={left_delete} left_first={left_first} both_candidates_before_mutation=true sequence={sequence:?} storage_effect_unavailable=true"
	);
}

#[tokio::test]
async fn adr629_c1_two_updates_submit_the_same_stale_increment_in_both_orders() {
	for left_first in [true, false] {
		bound_interleaving(false, left_first).await;
	}
}

#[tokio::test]
async fn adr629_c1_delete_and_update_keep_pre_mutation_candidates_in_both_orders() {
	for left_first in [true, false] {
		bound_interleaving(true, left_first).await;
	}
}
