mod aggregate_kernel;

pub mod binder;
pub mod catalog;
pub mod coordinator;
pub mod distributed;
pub mod error;
pub mod exec;
pub mod ir;
pub mod pbc;
pub mod planner;
pub mod rune_engine;
pub mod server;
pub mod sql_planner;
pub mod storage;
pub mod wire;

#[cfg(test)]
mod tests {
	use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;
	use std::collections::HashMap;
	use std::sync::Arc;

	use crate::binder::{PlannerConfig, bind};
	use crate::catalog::MockCatalog;
	use crate::coordinator::DistributedQueryCoordinator;
	use crate::distributed::budget::{BudgetDimension, ByteCount, QueryBudget, TaskBudget};
	use crate::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
	use crate::distributed::identity::{
		AttemptId, ExchangeId, LeaseEpoch, PartitionId, QueryId, TaskId, TaskIdentity, WorkerId,
	};
	use crate::distributed::partition_store::{
		HybridPartitionStore, InMemoryPartitionStore, PartitionPayload, PartitionStore,
	};
	use crate::distributed::scheduler::{DistributedScheduler, StagePlan, TaskPlan};
	use crate::distributed::task::{
		ArrowIpcDigest, PartitionAddress, PartitionBackend, PartitionCount, PartitionManifest,
		ProducerPartitionRef, RowCount, TaskCompletionOutput, TaskInputBinding, TaskOutputBinding,
		TaskSpec,
	};
	use crate::distributed::task_state::{AcceptanceState, TaskKey};
	use crate::distributed::worker_registry::{
		ActiveTaskCount, SimdLaneCount, StaticWorkerRegistry, UnixMillis, WorkerCapabilities,
		WorkerEndpoint, WorkerEntry, WorkerLease, WorkerResourceState, WorkerStatus,
	};
	use crate::error::{QueryError, StorageError, TaskFailureKind};
	use crate::exec::Executor;
	use crate::ir::{
		AggregateFunction, BoundExprProgram, ExecutionPlan, Expr, JoinKind, JoinStrategy,
		LogicalPlan, MorselId, ScalarValue, StageId, TableId,
	};
	use crate::server::{
		NetworkTaskDispatcher, ToronCoordinator, ToronWorker, memory::MemoryManager,
		spill::SpillManager, transport::TcpTransport,
	};
	use crate::storage::{InMemoryStorageScan, ScanRequest, ScanStream, StorageScan};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};
	use wendelon::storage::file_ops::BlockingFileOps;

	fn test_memory() -> Arc<MemoryManager> {
		Arc::new(MemoryManager::new(10, 1024 * 1024))
	}

	#[tokio::test]
	async fn single_process_ir_execution_counts_filtered_rows() {
		// Build a RecordBatch fixture
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int32, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("status", DataType::Utf8, true),
		]));

		let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
		let amount_array = Int32Array::from(vec![Some(50), Some(150), None, Some(200), Some(80)]);
		let status_array = StringArray::from(vec![
			Some("active"),
			Some("active"),
			Some("inactive"),
			Some("active"),
			Some("pending"),
		]);

		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(id_array),
				Arc::new(amount_array),
				Arc::new(status_array),
			],
		)
		.unwrap();

		let storage = Arc::new(InMemoryStorageScan { table_data: batch });
		let executor = Executor::new(storage);

		// Construct IR manually: SELECT COUNT(*) WHERE amount > 100 AND status = 'active'
		// Scan -> Filter -> Aggregate

		let plan = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("id")])],
			input: Box::new(LogicalPlan::Filter {
				predicate: Expr::And(
					Box::new(Expr::Gt(
						Box::new(Expr::column("amount")),
						Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
					)),
					Box::new(Expr::Eq(
						Box::new(Expr::column("status")),
						Box::new(Expr::Literal(ScalarValue::Utf8(Some("active".to_string())))),
					)),
				),
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
			}),
		};

		// Execute
		let results = executor
			.execute(&plan, None)
			.await
			.expect("Execution failed");

		// Assert output
		assert_eq!(results.len(), 1);
		let result_batch = &results[0];

		// Result should be a single row with 'count'
		assert_eq!(result_batch.num_rows(), 1);
		assert_eq!(result_batch.schema().field(0).name(), "count");

		let count_array = result_batch
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Int64Array>()
			.unwrap();

		// amount > 100 AND status == 'active':
		// row 2: amount 150, status active -> matches
		// row 4: amount 200, status active -> matches
		// So expected count is 2.
		assert_eq!(count_array.value(0), 2);
	}

	#[tokio::test]
	async fn filter_null_semantics_count_expected_rows() {
		// Assert that IsNull and IsNotNull work
		let schema = Arc::new(Schema::new(vec![Field::new(
			"amount",
			DataType::Int32,
			true,
		)]));

		let amount_array = Int32Array::from(vec![Some(50), None, Some(100)]);
		let batch = RecordBatch::try_new(schema, vec![Arc::new(amount_array)]).unwrap();

		let storage = Arc::new(InMemoryStorageScan { table_data: batch });
		let executor = Executor::new(storage);

		// Filter: IsNull(amount) -> should yield 1 row
		let plan_null = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("amount")])],
			input: Box::new(LogicalPlan::Filter {
				predicate: Expr::IsNull(Box::new(Expr::column("amount"))),
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
			}),
		};

		let results_null = executor.execute(&plan_null, None).await.unwrap();
		let count_null = results_null[0]
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Int64Array>()
			.unwrap()
			.value(0);
		assert_eq!(count_null, 1);

		// Filter: IsNotNull(amount) -> should yield 2 rows
		let plan_not_null = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("amount")])],
			input: Box::new(LogicalPlan::Filter {
				predicate: Expr::IsNotNull(Box::new(Expr::column("amount"))),
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
			}),
		};

		let results_not_null = executor.execute(&plan_not_null, None).await.unwrap();
		let count_not_null = results_not_null[0]
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Int64Array>()
			.unwrap()
			.value(0);
		assert_eq!(count_not_null, 2);
	}

	#[tokio::test]
	async fn morsel_execution_is_deterministic() {
		let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));

		// 100 rows
		let val_array = Int32Array::from((0..100).collect::<Vec<i32>>());
		let batch = RecordBatch::try_new(schema, vec![Arc::new(val_array)]).unwrap();

		let storage = Arc::new(InMemoryStorageScan { table_data: batch });
		let executor = Arc::new(Executor::new(storage));

		let plan = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("val")])],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};

		// 1. Single execution (no morsels)
		let single_results = executor.execute(&plan, None).await.unwrap();
		let single_count = single_results[0]
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Int64Array>()
			.unwrap()
			.value(0);
		assert_eq!(single_count, 100);

		// 2. Morsel splitting (4 morsels) - run sequentially
		let total_morsels = 4;
		let mut sequential_sum = 0;
		for i in 0..total_morsels {
			let res = executor
				.execute(&plan, Some((MorselId(i), total_morsels)))
				.await
				.unwrap();
			sequential_sum += res[0]
				.column(0)
				.as_any()
				.downcast_ref::<arrow::array::Int64Array>()
				.unwrap()
				.value(0);
		}
		assert_eq!(sequential_sum, 100);

		// 3. Concurrent morsel execution (multi-thread determinism)
		let mut tasks = Vec::new();
		for i in 0..total_morsels {
			let exec_clone = executor.clone();
			let plan_clone = plan.clone();
			tasks.push(tokio::spawn(async move {
				let res = exec_clone
					.execute(&plan_clone, Some((MorselId(i), total_morsels)))
					.await
					.unwrap();
				res[0]
					.column(0)
					.as_any()
					.downcast_ref::<arrow::array::Int64Array>()
					.unwrap()
					.value(0)
			}));
		}
		let mut concurrent_sum = 0;
		for task in tasks {
			concurrent_sum += task.await.unwrap();
		}
		assert_eq!(concurrent_sum, 100);

		// 4. Idempotent morsel retry
		let res1 = executor
			.execute(&plan, Some((MorselId(2), total_morsels)))
			.await
			.unwrap();
		let val1 = res1[0]
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Int64Array>()
			.unwrap()
			.value(0);

		let res2 = executor
			.execute(&plan, Some((MorselId(2), total_morsels)))
			.await
			.unwrap();
		let val2 = res2[0]
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Int64Array>()
			.unwrap()
			.value(0);

		assert_eq!(val1, val2);
		assert_eq!(val1, 25); // 100 rows / 4 morsels = 25
	}

	#[tokio::test]
	async fn coordinator_worker_boundary_executes_task_spec_over_tcp() {
		let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
		let val_array = Int32Array::from((0..100).collect::<Vec<i32>>());
		let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(val_array)]).unwrap();

		let storage = Arc::new(InMemoryStorageScan { table_data: batch });
		let memory = test_memory();
		let catalog = test_catalog(schema.as_ref());
		let worker = Arc::new(ToronWorker::new(storage, memory.clone(), catalog.clone()));
		let coordinator = ToronCoordinator::new(memory);

		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();

		let worker_handle = tokio::spawn(async move {
			let (server_stream, _) = listener.accept().await.unwrap();
			worker.handle_connection(server_stream).await.unwrap();
		});

		let plan = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("val")])],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};

		let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
			.await
			.unwrap();
		let spec = test_task_spec(bound, TaskId::new(0));
		let client_stream = tokio::net::TcpStream::connect(address).await.unwrap();

		let results = coordinator
			.execute_task_on_worker(client_stream, spec)
			.await
			.unwrap();

		assert_eq!(results.len(), 1);
		let count = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Int64Array>()
			.unwrap()
			.value(0);
		assert_eq!(count, 100);

		worker_handle.await.unwrap();
	}

	#[tokio::test]
	async fn distributed_scheduler_dispatches_task_spec_over_tcp() {
		let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int32Array::from((0..64).collect::<Vec<i32>>()))],
		)
		.unwrap();
		let memory = test_memory();
		let catalog = test_catalog(schema.as_ref());
		let worker = Arc::new(ToronWorker::new(
			Arc::new(InMemoryStorageScan { table_data: batch }),
			memory.clone(),
			catalog.clone(),
		));
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		let worker_handle = tokio::spawn(async move {
			let (server_stream, _) = listener.accept().await.unwrap();
			worker.handle_connection(server_stream).await.unwrap();
		});

		let plan = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("val")])],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};
		let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
			.await
			.unwrap();
		let spec = test_task_spec(bound, TaskId::new(0));
		let key = TaskKey::from_identity(spec.identity);
		let worker_id = WorkerId::from_u128(77);
		let registry = StaticWorkerRegistry::new([WorkerEntry {
			worker_id,
			endpoint: WorkerEndpoint::new(address.to_string()),
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(1),
				epoch: LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: ByteCount::new(1024 * 1024),
				simd_width: SimdLaneCount::new(8),
				spill_capacity_reserved: ByteCount::new(1024 * 1024),
			},
			resources: WorkerResourceState {
				in_flight: ByteCount::ZERO,
				active_tasks: ActiveTaskCount::default(),
			},
			status: WorkerStatus::Live,
		}]);
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![TaskPlan::new(spec)],
		};
		let mut scheduler = DistributedScheduler::new(registry);
		let mut dispatcher = NetworkTaskDispatcher::new(memory, TcpTransport::new());

		let run = scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect("scheduler dispatch over tcp");

		assert_eq!(run.dispatched.len(), 1);
		assert_eq!(run.final_results.len(), 1);
		assert_eq!(
			run.final_results[0]
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap()
				.value(0),
			64
		);
		assert_eq!(
			scheduler.state().acceptance_state(key),
			AcceptanceState::LiveAccepted {
				attempt_id: AttemptId::first(),
				worker_id,
				drained: false,
			}
		);
		worker_handle.await.unwrap();
	}

	#[tokio::test]
	async fn distributed_query_coordinator_executes_fragmented_exchange_over_tcp() {
		let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int32Array::from((0..16).collect::<Vec<i32>>()))],
		)
		.unwrap();
		let memory = test_memory();
		let catalog = test_catalog(schema.as_ref());
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let endpoint = WorkerEndpoint::new(listener.local_addr().unwrap().to_string());
		let worker_id = WorkerId::from_u128(99);
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(InMemoryStorageScan { table_data: batch }),
			memory.clone(),
			Arc::new(InMemoryPartitionStore::new()),
			worker_id,
			endpoint.clone(),
		));
		let server_handle = tokio::spawn(async move {
			let mut handles = Vec::new();
			for _ in 0..3 {
				let (server_stream, _) = listener.accept().await.unwrap();
				let worker = worker.clone();
				handles.push(tokio::spawn(async move {
					worker.handle_connection(server_stream).await.unwrap();
				}));
			}
			for handle in handles {
				handle.await.unwrap();
			}
		});
		let registry = Arc::new(StaticWorkerRegistry::new([WorkerEntry {
			worker_id,
			endpoint,
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(1),
				epoch: LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: ByteCount::new(1024 * 1024),
				simd_width: SimdLaneCount::new(8),
				spill_capacity_reserved: ByteCount::new(1024 * 1024),
			},
			resources: WorkerResourceState {
				in_flight: ByteCount::ZERO,
				active_tasks: ActiveTaskCount::default(),
			},
			status: WorkerStatus::Live,
		}]));
		let coordinator = DistributedQueryCoordinator::new(
			catalog,
			registry,
			memory,
			Arc::new(TcpTransport::new()),
			PlannerConfig::default(),
			QueryBudget::test_default(),
		);
		let plan = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("val")])],
			input: Box::new(LogicalPlan::Exchange {
				partition_count: 1,
				key: vec![Expr::column("val")],
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
			}),
		};

		let results = coordinator.execute_query(&plan).await.unwrap();

		assert_eq!(results.len(), 1);
		assert_eq!(
			results[0]
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap()
				.value(0),
			16
		);
		server_handle.await.unwrap();
	}

	#[tokio::test]
	async fn distributed_query_coordinator_executes_hash_join_over_tcp() {
		let plan = LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				partition_count: 2,
				key: vec![Expr::qualified_column(
					crate::catalog::TableRef::new("orders"),
					"customer_id",
				)],
				input: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("orders"),
					projection: None,
				}),
			}),
			right: Box::new(LogicalPlan::Exchange {
				partition_count: 2,
				key: vec![Expr::qualified_column(
					crate::catalog::TableRef::new("customers"),
					"id",
				)],
				input: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("customers"),
					projection: None,
				}),
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(crate::catalog::TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(crate::catalog::TableRef::new("customers"), "id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};

		let results = run_distributed_join_query(plan, 8).await;

		assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
	}

	#[tokio::test]
	async fn distributed_query_coordinator_executes_broadcast_join_over_tcp() {
		let plan = broadcast_join_plan();

		let results = run_distributed_join_query(plan, 3).await;

		assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
	}

	#[tokio::test]
	async fn distributed_broadcast_join_digest_matches_hash_join_over_tcp() {
		let hash_results = run_distributed_join_query(hash_join_customers_left_plan(), 8).await;
		let broadcast_results = run_distributed_join_query(broadcast_join_plan(), 3).await;

		assert_eq!(
			canonical_arrow_ipc_digest(&broadcast_results, SortPolicy::FullRowLex)
				.expect("broadcast digest"),
			canonical_arrow_ipc_digest(&hash_results, SortPolicy::FullRowLex).expect("hash digest")
		);
	}

	#[tokio::test]
	async fn distributed_broadcast_join_over_cap_fails_terminally_over_tcp() {
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);

		let err = run_distributed_join_query_with_budget(broadcast_join_plan(), 2, query_budget)
			.await
			.expect_err("broadcast cap must fail");

		assert!(matches!(
			err,
			QueryError::Runtime(crate::error::RuntimeError::TaskFailure {
				kind: TaskFailureKind::BudgetExhausted {
					dimension: BudgetDimension::BroadcastReplication,
					..
				},
				..
			})
		));
	}

	#[tokio::test]
	async fn shuffle_task_complete_publishes_partition_manifest() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("key", DataType::Int32, false),
			Field::new("val", DataType::Int32, false),
		]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![
				Arc::new(Int32Array::from((0..20).collect::<Vec<i32>>())),
				Arc::new(Int32Array::from((0..20).collect::<Vec<i32>>())),
			],
		)
		.unwrap();
		let storage = Arc::new(InMemoryStorageScan { table_data: batch });
		let memory = test_memory();
		let catalog = test_catalog(schema.as_ref());
		let spill_dir =
			std::env::temp_dir().join(format!("toron_shuffle_manifest_{}", rand::random::<u64>()));
		let store = Arc::new(
			HybridPartitionStore::new(spill_dir.clone(), ByteCount::ZERO)
				.expect("hybrid partition store"),
		);
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let endpoint = WorkerEndpoint::new(listener.local_addr().unwrap().to_string());
		let worker_id = WorkerId::from_u128(88);
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			storage,
			memory.clone(),
			store.clone(),
			worker_id,
			endpoint.clone(),
		));
		let coordinator = ToronCoordinator::new(memory.clone());
		let plan = LogicalPlan::Exchange {
			partition_count: 2,
			key: vec![Expr::column("key")],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};
		let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
			.await
			.unwrap();
		let spec = TaskSpec {
			output: TaskOutputBinding::ShuffleProducer {
				exchange_id: ExchangeId::new(55),
				partition_count: PartitionCount::new(2),
				partition_key: BoundExprProgram::empty(),
			},
			..test_task_spec(bound, TaskId::new(55))
		};
		let socket_address = endpoint.as_str().to_string();
		let worker_handle = tokio::spawn(async move {
			let (server_stream, _) = listener.accept().await.unwrap();
			worker.handle_connection(server_stream).await.unwrap();
		});

		let (_batches, completion) = coordinator
			.execute_task_on_worker_with_completion(
				tokio::net::TcpStream::connect(socket_address)
					.await
					.unwrap(),
				spec,
			)
			.await
			.unwrap();
		let TaskCompletionOutput::ShuffleProducer { manifest } = completion else {
			panic!("expected shuffle manifest");
		};

		assert_eq!(manifest.exchange_id, ExchangeId::new(55));
		assert_eq!(manifest.producer_worker_id, worker_id);
		assert_eq!(manifest.producer_endpoint, endpoint);
		assert!(matches!(
			manifest.backend,
			PartitionBackend::EncryptedSpill { .. }
		));
		assert_eq!(manifest.partitions.len(), 2);
		let first_address = PartitionAddress {
			query_id: manifest.query_id,
			exchange_id: manifest.exchange_id,
			producer_stage_id: manifest.stage_id,
			producer_task_id: manifest.task_id,
			attempt_id: manifest.attempt_id,
			partition_id: manifest.partitions[0].partition_id,
		};
		assert!(store.pull(first_address, manifest.attempt_id).is_ok());
		worker_handle.await.unwrap();
		std::fs::remove_dir_all(spill_dir).expect("remove spill dir");
	}

	#[tokio::test]
	async fn distributed_shuffle_exchange_accepts_partition_payload() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("key", DataType::Int32, false),
			Field::new("val", DataType::Int32, false),
		]));

		// Worker 1 Data: keys 0..50
		let w1_keys = Int32Array::from((0..50).collect::<Vec<i32>>());
		let w1_vals = Int32Array::from((0..50).collect::<Vec<i32>>());
		let w1_batch =
			RecordBatch::try_new(schema.clone(), vec![Arc::new(w1_keys), Arc::new(w1_vals)])
				.unwrap();

		// Worker 2 Data: keys 50..100
		let w2_keys = Int32Array::from((50..100).collect::<Vec<i32>>());
		let w2_vals = Int32Array::from((50..100).collect::<Vec<i32>>());
		let w2_batch =
			RecordBatch::try_new(schema.clone(), vec![Arc::new(w2_keys), Arc::new(w2_vals)])
				.unwrap();

		let memory = test_memory();
		let w1 = Arc::new(ToronWorker::new(
			Arc::new(InMemoryStorageScan {
				table_data: w1_batch,
			}),
			memory.clone(),
			test_catalog(schema.as_ref()),
		));
		let w2 = Arc::new(ToronWorker::new(
			Arc::new(InMemoryStorageScan {
				table_data: w2_batch,
			}),
			memory.clone(),
			test_catalog(schema.as_ref()),
		));

		let plan = LogicalPlan::Exchange {
			partition_count: 2,
			key: vec![Expr::column("key")],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};

		let catalog = test_catalog(schema.as_ref());
		let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
			.await
			.unwrap();

		// 1. Stage 1: partitioning on workers from an already-bound task spec.
		let w1_partitions = w1
			.execute_task_local(&test_task_spec(bound.clone(), TaskId::new(1)))
			.await
			.unwrap();
		let w2_partitions = w2
			.execute_task_local(&test_task_spec(bound, TaskId::new(2)))
			.await
			.unwrap();

		// Verify w1 correctly partitioned its 50 rows into 2 buckets
		let w1_total: usize = w1_partitions.iter().map(|b| b.num_rows()).sum();
		assert_eq!(w1_total, 50);

		// 2. Stage 2: pull-addressed partition-store exchange.
		let store = Arc::new(InMemoryPartitionStore::new());
		let address = PartitionAddress {
			query_id: QueryId::from_u128(10),
			exchange_id: ExchangeId::new(1),
			producer_stage_id: StageId::new(1),
			producer_task_id: TaskId::new(1),
			attempt_id: AttemptId::first(),
			partition_id: PartitionId::new(1),
		};
		let payload = PartitionPayload::new(
			address,
			encode_test_batch(&w1_partitions[1]),
			RowCount::new(u64::try_from(w1_partitions[1].num_rows()).expect("row count fits u64")),
			ArrowIpcDigest::new([1; 32]),
		);
		let manifest = PartitionManifest {
			query_id: address.query_id,
			stage_id: address.producer_stage_id,
			task_id: address.producer_task_id,
			attempt_id: address.attempt_id,
			exchange_id: address.exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("in-memory-worker-1"),
			partitions: vec![payload.entry()],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		store.publish(manifest, vec![payload]).unwrap();

		let partition_worker = Arc::new(ToronWorker::with_partition_store(
			Arc::new(InMemoryStorageScan {
				table_data: w2_partitions[0].clone(),
			}),
			memory.clone(),
			store,
		));
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let endpoint = listener.local_addr().unwrap();
		let worker_handle = tokio::spawn(async move {
			let (server_stream, _) = listener.accept().await.unwrap();
			partition_worker
				.handle_connection(server_stream)
				.await
				.unwrap();
		});
		let client_stream = tokio::net::TcpStream::connect(endpoint).await.unwrap();
		let pulled = ToronCoordinator::new(memory.clone())
			.pull_partition_from_worker(client_stream, address, AttemptId::first())
			.await
			.unwrap();
		worker_handle.await.unwrap();

		let mut reader =
			arrow::ipc::reader::StreamReader::try_new(pulled.payload.as_slice(), None).unwrap();
		let pulled_batch = reader.next().transpose().unwrap().unwrap();
		assert_eq!(pulled_batch.num_rows(), w1_partitions[1].num_rows());

		// 3. Verification
		assert!(w1_partitions.len() >= 1);
		assert!(w2_partitions.len() >= 1);
	}

	#[tokio::test]
	async fn partition_pull_stale_attempt_returns_partition_store_mismatch() {
		let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
		)
		.unwrap();
		let memory = test_memory();
		let store = Arc::new(InMemoryPartitionStore::new());
		let accepted_address = PartitionAddress {
			query_id: QueryId::from_u128(11),
			exchange_id: ExchangeId::new(1),
			producer_stage_id: StageId::new(1),
			producer_task_id: TaskId::new(1),
			attempt_id: AttemptId::new(2),
			partition_id: PartitionId::new(0),
		};
		let payload = PartitionPayload::new(
			accepted_address,
			encode_test_batch(&batch),
			RowCount::new(3),
			ArrowIpcDigest::new([2; 32]),
		);
		store
			.publish(
				PartitionManifest {
					query_id: accepted_address.query_id,
					stage_id: accepted_address.producer_stage_id,
					task_id: accepted_address.producer_task_id,
					attempt_id: accepted_address.attempt_id,
					exchange_id: accepted_address.exchange_id,
					producer_worker_id: WorkerId::from_u128(2),
					producer_endpoint: WorkerEndpoint::new("in-memory-worker-2"),
					partitions: vec![payload.entry()],
					runtime_filter_fragment: None,
					backend: PartitionBackend::InMemory,
				},
				vec![payload],
			)
			.unwrap();
		let worker = Arc::new(ToronWorker::with_partition_store(
			Arc::new(InMemoryStorageScan {
				table_data: batch.clone(),
			}),
			memory.clone(),
			store,
		));
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let endpoint = listener.local_addr().unwrap();
		let worker_handle = tokio::spawn(async move {
			let (server_stream, _) = listener.accept().await.unwrap();
			worker.handle_connection(server_stream).await.unwrap();
		});

		let stale_address = PartitionAddress {
			attempt_id: AttemptId::first(),
			..accepted_address
		};
		let err = ToronCoordinator::new(memory)
			.pull_partition_from_worker(
				tokio::net::TcpStream::connect(endpoint).await.unwrap(),
				stale_address,
				AttemptId::first(),
			)
			.await
			.expect_err("stale attempt should surface as partition-store mismatch");

		assert!(matches!(
			err,
			crate::error::QueryError::Runtime(
				crate::error::RuntimeError::PartitionStoreMismatch { ref message }
			) if message.contains("AttemptStale")
		));
		worker_handle.await.unwrap();
	}

	#[tokio::test]
	async fn shuffle_consumer_task_pulls_partition_refs_over_tcp() {
		let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
		let partition_batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int32Array::from((0..7).collect::<Vec<i32>>()))],
		)
		.unwrap();
		let partition_bytes = encode_test_batch(&partition_batch);
		let address = PartitionAddress {
			query_id: QueryId::from_u128(22),
			exchange_id: ExchangeId::new(3),
			producer_stage_id: StageId::new(0),
			producer_task_id: TaskId::new(4),
			attempt_id: AttemptId::first(),
			partition_id: PartitionId::new(0),
		};
		let payload = PartitionPayload::new(
			address,
			partition_bytes.clone(),
			RowCount::new(7),
			ArrowIpcDigest::new(wendelon::hash::blake3::digest(&partition_bytes)),
		);
		let producer_store = Arc::new(InMemoryPartitionStore::new());
		let producer_worker_id = WorkerId::from_u128(42);
		let memory = test_memory();
		let producer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let producer_endpoint =
			WorkerEndpoint::new(producer_listener.local_addr().unwrap().to_string());
		let manifest = PartitionManifest {
			query_id: address.query_id,
			stage_id: address.producer_stage_id,
			task_id: address.producer_task_id,
			attempt_id: address.attempt_id,
			exchange_id: address.exchange_id,
			producer_worker_id,
			producer_endpoint: producer_endpoint.clone(),
			partitions: vec![payload.entry()],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		producer_store
			.publish(manifest, vec![payload.clone()])
			.unwrap();
		let producer = Arc::new(ToronWorker::with_partition_store(
			Arc::new(InMemoryStorageScan {
				table_data: partition_batch.clone(),
			}),
			memory.clone(),
			producer_store,
		));
		let producer_handle = tokio::spawn(async move {
			let (server_stream, _) = producer_listener.accept().await.unwrap();
			producer.handle_connection(server_stream).await.unwrap();
		});

		let consumer = Arc::new(ToronWorker::new(
			Arc::new(InMemoryStorageScan {
				table_data: RecordBatch::new_empty(schema.clone()),
			}),
			memory.clone(),
			test_catalog(schema.as_ref()),
		));
		let consumer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let consumer_endpoint = consumer_listener.local_addr().unwrap();
		let consumer_handle = tokio::spawn(async move {
			let (server_stream, _) = consumer_listener.accept().await.unwrap();
			consumer.handle_connection(server_stream).await.unwrap();
		});

		let plan = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("val")])],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};
		let catalog = test_catalog(schema.as_ref());
		let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
			.await
			.unwrap();
		let mut spec = test_task_spec(bound, TaskId::new(9));
		spec.identity = TaskIdentity::new(
			address.query_id,
			StageId::new(1),
			TaskId::new(9),
			AttemptId::first(),
		);
		spec.inputs = vec![TaskInputBinding::ShuffleConsumer {
			exchange_id: address.exchange_id,
			producer_stage_id: address.producer_stage_id,
			partition_id: address.partition_id,
			producer_partitions: vec![ProducerPartitionRef {
				address,
				worker_id: producer_worker_id,
				endpoint: producer_endpoint,
				size_bytes: ByteCount::new(
					u64::try_from(partition_bytes.len()).expect("payload length fits u64"),
				),
				digest: ArrowIpcDigest::new(wendelon::hash::blake3::digest(&partition_bytes)),
			}],
		}];

		let results = ToronCoordinator::new(memory)
			.execute_task_on_worker(
				tokio::net::TcpStream::connect(consumer_endpoint)
					.await
					.unwrap(),
				spec,
			)
			.await
			.unwrap();

		let count = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap()
			.value(0);
		assert_eq!(count, 7);
		consumer_handle.await.unwrap();
		producer_handle.await.unwrap();
	}

	#[tokio::test]
	async fn coordinator_cancel_task_round_trips_over_tcp() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
		)
		.unwrap();
		let memory = test_memory();
		let worker = Arc::new(ToronWorker::new(
			Arc::new(InMemoryStorageScan { table_data: batch }),
			memory.clone(),
			test_catalog(schema.as_ref()),
		));
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let endpoint = listener.local_addr().unwrap();
		let worker_handle = tokio::spawn(async move {
			let (server_stream, _) = listener.accept().await.unwrap();
			worker.handle_connection(server_stream).await.unwrap();
		});
		let identity = TaskIdentity::new(
			QueryId::from_u128(14),
			StageId::new(1),
			TaskId::new(2),
			AttemptId::first(),
		);

		ToronCoordinator::new(memory)
			.cancel_task_on_worker(
				tokio::net::TcpStream::connect(endpoint).await.unwrap(),
				identity,
			)
			.await
			.unwrap();

		worker_handle.await.unwrap();
	}

	#[tokio::test]
	async fn rune_authoring_surface_builds_count_plan() {
		let config = crate::rune_engine::RuneSandboxConfig::v1_default();
		let engine = crate::rune_engine::RuneEngine::with_config(config).unwrap();

		let script = r#"
            pub fn main(ctx) {
                ctx.table("sales")
                   .filter(col("amount").gt(lit(100)))
                   .count()
            }
        "#;

		let plan = engine
			.execute_script(script, &config)
			.await
			.expect("Rune execution failed");

		// Verify the plan structure
		match &plan {
			crate::ir::LogicalPlan::Aggregate {
				input,
				group_expr,
				aggr_expr,
			} => {
				assert!(group_expr.is_empty());
				assert_eq!(aggr_expr.len(), 1);
				assert!(matches!(
					aggr_expr[0].0,
					crate::ir::AggregateFunction::CountAll
				));

				if let crate::ir::LogicalPlan::Filter {
					input: _,
					predicate,
				} = &**input
				{
					if let crate::ir::Expr::Gt(left, right) = predicate {
						assert!(
							matches!(&**left, crate::ir::Expr::Column(c) if c.name.0 == "amount")
						);
						assert!(matches!(
							&**right,
							crate::ir::Expr::Literal(crate::ir::ScalarValue::Int32(Some(100)))
						));
					} else {
						panic!("Expected Gt predicate, got {:?}", predicate);
					}
				} else {
					panic!("Expected Filter plan, got {:?}", input);
				}
			}
			_ => panic!("Expected Aggregate plan, got {:?}", plan),
		}
	}

	#[tokio::test]
	async fn sql_compatibility_surface_builds_and_executes_count_plan() {
		let planner = crate::sql_planner::SqlPlanner::new();

		let sql = "SELECT count(*) FROM sales WHERE amount > 100";
		let plan = planner.plan(sql).expect("SQL planning failed");

		// Verify the plan structure
		match &plan {
			crate::ir::LogicalPlan::Aggregate {
				input,
				group_expr,
				aggr_expr,
			} => {
				assert!(group_expr.is_empty());
				assert_eq!(aggr_expr.len(), 1);
				assert!(matches!(
					aggr_expr[0].0,
					crate::ir::AggregateFunction::CountAll
				));

				if let crate::ir::LogicalPlan::Filter {
					input: _,
					predicate,
				} = &**input
				{
					if let crate::ir::Expr::Gt(left, right) = predicate {
						assert!(
							matches!(&**left, crate::ir::Expr::Column(c) if c.name.0 == "amount")
						);
						assert!(matches!(
							&**right,
							crate::ir::Expr::Literal(crate::ir::ScalarValue::Int32(Some(100)))
						));
					} else {
						panic!("Expected Gt predicate, got {:?}", predicate);
					}
				} else {
					panic!("Expected Filter plan, got {:?}", input);
				}
			}
			_ => panic!("Expected Aggregate plan, got {:?}", plan),
		}

		// End-to-end execution verification
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
		]));
		let id_array = Int64Array::from(vec![1, 2, 3]);
		let amount_array = Int32Array::from(vec![Some(50), Some(150), Some(200)]);
		let batch = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(id_array), Arc::new(amount_array)],
		)
		.unwrap();

		let storage = Arc::new(InMemoryStorageScan { table_data: batch });
		let executor = Executor::new(storage);
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), test_descriptor(&schema))
			.unwrap();

		let bound = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.unwrap();
		let results = executor.execute_bound(&bound, None).await.unwrap();
		let count = results[0]
			.column(0)
			.as_any()
			.downcast_ref::<arrow::array::Int64Array>()
			.unwrap()
			.value(0);
		assert_eq!(count, 2);
	}

	fn test_descriptor(schema: &Schema) -> TableSchemaDescriptor {
		let primary_key = schema
			.fields()
			.first()
			.expect("test schema must have a primary key column")
			.name()
			.clone();
		TableSchemaDescriptor::new(
			TableObjectId::new([1; 16]),
			1,
			"default",
			"sales",
			schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(primary_key)),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn test_catalog(schema: &Schema) -> Arc<MockCatalog> {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), test_descriptor(schema))
			.unwrap();
		Arc::new(catalog)
	}

	fn join_catalog() -> Arc<MockCatalog> {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(
				TableId(1),
				named_descriptor(
					1,
					"orders",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("customer_id", DataType::Int64, false),
					]),
				),
			)
			.unwrap();
		catalog
			.register_descriptor(
				TableId(2),
				named_descriptor(
					2,
					"customers",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("region", DataType::Utf8, true),
					]),
				),
			)
			.unwrap();
		Arc::new(catalog)
	}

	fn named_descriptor(id_byte: u8, table: &str, schema: Schema) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([id_byte; 16]),
			1,
			"default",
			table,
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn join_storage() -> Arc<MultiTableStorageScan> {
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]));
		let customers_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]));
		let orders = RecordBatch::try_new(
			orders_schema,
			vec![
				Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
				Arc::new(Int64Array::from(vec![1, 2, 2, 3])),
			],
		)
		.unwrap();
		let customers = RecordBatch::try_new(
			customers_schema,
			vec![
				Arc::new(Int64Array::from(vec![2, 3])),
				Arc::new(StringArray::from(vec![Some("west"), Some("east")])),
			],
		)
		.unwrap();
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([(TableId(1), orders), (TableId(2), customers)]),
		})
	}

	fn broadcast_join_plan() -> LogicalPlan {
		LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				partition_count: 1,
				key: vec![Expr::qualified_column(
					crate::catalog::TableRef::new("customers"),
					"id",
				)],
				input: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("customers"),
					projection: None,
				}),
			}),
			right: Box::new(LogicalPlan::ScanNamed {
				table: crate::catalog::TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(crate::catalog::TableRef::new("customers"), "id"),
				Expr::qualified_column(crate::catalog::TableRef::new("orders"), "customer_id"),
			)],
			strategy: JoinStrategy::Broadcast,
		}
	}

	fn hash_join_customers_left_plan() -> LogicalPlan {
		LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				partition_count: 2,
				key: vec![Expr::qualified_column(
					crate::catalog::TableRef::new("customers"),
					"id",
				)],
				input: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("customers"),
					projection: None,
				}),
			}),
			right: Box::new(LogicalPlan::Exchange {
				partition_count: 2,
				key: vec![Expr::qualified_column(
					crate::catalog::TableRef::new("orders"),
					"customer_id",
				)],
				input: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("orders"),
					projection: None,
				}),
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(crate::catalog::TableRef::new("customers"), "id"),
				Expr::qualified_column(crate::catalog::TableRef::new("orders"), "customer_id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		}
	}

	async fn run_distributed_join_query(plan: LogicalPlan, connections: usize) -> Vec<RecordBatch> {
		run_distributed_join_query_with_budget(plan, connections, QueryBudget::test_default())
			.await
			.unwrap()
	}

	async fn run_distributed_join_query_with_budget(
		plan: LogicalPlan,
		connections: usize,
		query_budget: QueryBudget,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let memory = test_memory();
		let catalog = join_catalog();
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let endpoint = WorkerEndpoint::new(listener.local_addr().unwrap().to_string());
		let worker_id = WorkerId::from_u128(101);
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			join_storage(),
			memory.clone(),
			Arc::new(InMemoryPartitionStore::new()),
			worker_id,
			endpoint.clone(),
		));
		let server_handle = tokio::spawn(async move {
			let mut handles = Vec::new();
			for _ in 0..connections {
				let (server_stream, _) = listener.accept().await.unwrap();
				let worker = worker.clone();
				handles.push(tokio::spawn(async move {
					worker.handle_connection(server_stream).await.unwrap();
				}));
			}
			for handle in handles {
				handle.await.unwrap();
			}
		});
		let registry = Arc::new(StaticWorkerRegistry::new([WorkerEntry {
			worker_id,
			endpoint,
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(1),
				epoch: LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: ByteCount::new(1024 * 1024),
				simd_width: SimdLaneCount::new(8),
				spill_capacity_reserved: ByteCount::new(1024 * 1024),
			},
			resources: WorkerResourceState {
				in_flight: ByteCount::ZERO,
				active_tasks: ActiveTaskCount::default(),
			},
			status: WorkerStatus::Live,
		}]));
		let coordinator = DistributedQueryCoordinator::new(
			catalog,
			registry,
			memory,
			Arc::new(TcpTransport::new()),
			PlannerConfig::default(),
			query_budget,
		);

		let results = coordinator.execute_query(&plan).await;
		server_handle.await.unwrap();
		results
	}

	struct MultiTableStorageScan {
		tables: HashMap<TableId, RecordBatch>,
	}

	#[async_trait::async_trait]
	impl StorageScan for MultiTableStorageScan {
		async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
			let mut batch =
				self.tables
					.get(&request.table_id)
					.cloned()
					.ok_or(StorageError::TableNotFound {
						table: request.table_id,
					})?;
			if let Some((morsel_id, total_morsels)) = request.morsel {
				let total_rows = batch.num_rows();
				let rows_per_morsel =
					(total_rows + total_morsels as usize - 1) / total_morsels as usize;
				let start = morsel_id.0 as usize * rows_per_morsel;
				let end = std::cmp::min(start + rows_per_morsel, total_rows);
				batch = if start < total_rows {
					batch.slice(start, end - start)
				} else {
					batch.slice(0, 0)
				};
			}
			Ok(ScanStream::from_batches(
				batch.schema(),
				request.session_id,
				request.query_id,
				vec![batch],
			))
		}
	}

	fn test_task_spec(plan: ExecutionPlan, task_id: TaskId) -> TaskSpec {
		TaskSpec {
			identity: TaskIdentity::new(
				QueryId::from_u128(100),
				StageId::new(0),
				task_id,
				AttemptId::first(),
			),
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			plan,
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		}
	}

	fn encode_test_batch(batch: &RecordBatch) -> Vec<u8> {
		let mut buffer = Vec::new();
		{
			let mut writer =
				arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
			writer.write(batch).unwrap();
			writer.finish().unwrap();
		}
		buffer
	}

	#[tokio::test]
	async fn memory_pool_reuses_released_buffers() {
		let memory = Arc::new(MemoryManager::new(2, 1024));

		{
			let buf1 = memory.acquire_at_least(100);
			assert_eq!(memory.available(), 0);
			drop(buf1);
		}
		assert_eq!(memory.available(), 1);
	}

	#[tokio::test]
	async fn encrypted_spill_round_trips_without_plain_ipc() {
		let temp_dir = std::env::temp_dir().join(format!("toron_spill_{}", rand::random::<u64>()));
		std::fs::create_dir_all(&temp_dir).unwrap();

		let file_ops = Arc::new(BlockingFileOps::default());
		let spill_manager = SpillManager::new(file_ops.clone(), temp_dir.clone()).unwrap();

		let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
		let val_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
		let batch = RecordBatch::try_new(schema, vec![Arc::new(val_array)]).unwrap();

		let id = spill_manager.spill_batch(&batch).await.unwrap();
		let path = temp_dir.join(format!("spill_{}.bin", id.0));
		let ciphertext = std::fs::read(&path).unwrap();

		let reader_res = arrow::ipc::reader::StreamReader::try_new(&ciphertext[..], None);
		assert!(reader_res.is_err());

		let read_batch = spill_manager.read_batch(id).await.unwrap();
		assert_eq!(batch, read_batch);

		spill_manager.cleanup(id).await.unwrap();
		std::fs::remove_dir_all(&temp_dir).unwrap();
	}

	#[tokio::test]
	async fn experimental_linear_regression_kernel_executes_under_legacy_path() {
		let schema = Arc::new(Schema::new(vec![
			Field::new("y", DataType::Int32, true),
			Field::new("x", DataType::Int32, true),
		]));

		// y = 2x + 10
		let y_array = Int32Array::from(vec![12, 14, 16, 18]);
		let x_array = Int32Array::from(vec![1, 2, 3, 4]);
		let batch =
			RecordBatch::try_new(schema, vec![Arc::new(y_array), Arc::new(x_array)]).unwrap();

		let storage = Arc::new(InMemoryStorageScan { table_data: batch });
		let executor = Executor::new(storage);

		let plan = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(
				AggregateFunction::LinearRegress,
				vec![Expr::column("y"), Expr::column("x")],
			)],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};

		let results = executor.execute(&plan, None).await.unwrap();
		let result_batch = &results[0];
		let linreg_array = result_batch
			.column(0)
			.as_any()
			.downcast_ref::<Float64Array>()
			.unwrap();

		assert_eq!(linreg_array.value(0), 2.0);
		assert_eq!(linreg_array.value(1), 10.0);
	}

	#[tokio::test]
	async fn experimental_quantile_kernel_executes_under_legacy_path() {
		let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, true)]));

		let val_array = Int32Array::from((1..=100).collect::<Vec<i32>>());
		let batch = RecordBatch::try_new(schema, vec![Arc::new(val_array)]).unwrap();

		let storage = Arc::new(InMemoryStorageScan { table_data: batch });
		let executor = Executor::new(storage);

		let plan = LogicalPlan::Aggregate {
			group_expr: vec![],
			aggr_expr: vec![(
				AggregateFunction::ApproxQuantile(0.5),
				vec![Expr::column("val")],
			)],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};

		let results = executor.execute(&plan, None).await.unwrap();
		let result_batch = &results[0];
		let q_array = result_batch
			.column(0)
			.as_any()
			.downcast_ref::<Float64Array>()
			.unwrap();

		assert!((q_array.value(0) - 50.0).abs() < 5.0);
	}

	#[tokio::test]
	async fn experimental_time_bucket_kernel_executes_under_legacy_path() {
		let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));

		// Align base to an hour boundary (3600 * 1000 ms)
		let hour_ms = 3600 * 1000;
		let base = (1600000000000i64 / hour_ms) * hour_ms;
		let ts_array = Int64Array::from(vec![
			base + 5 * 60 * 1000,
			base + 55 * 60 * 1000,
			base + 75 * 60 * 1000,
		]);
		let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_array)]).unwrap();

		let storage = Arc::new(InMemoryStorageScan {
			table_data: batch.clone(),
		});
		let executor = Executor::new(storage);

		let expr = Expr::TimeBucket(
			std::time::Duration::from_secs(3600),
			Box::new(Expr::column("ts")),
		);
		let result_arr = executor.evaluate_expr(&expr, &batch).unwrap();
		let bucketed = result_arr.as_any().downcast_ref::<Int64Array>().unwrap();

		assert_eq!(bucketed.value(0), bucketed.value(1));
		assert_ne!(bucketed.value(0), bucketed.value(2));
		assert_eq!(bucketed.value(2) - bucketed.value(0), 3600 * 1000);
	}
}
