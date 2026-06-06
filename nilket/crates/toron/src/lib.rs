mod aggregate_kernel;

pub mod audit;
pub mod binder;
pub mod catalog;
pub mod cli;
pub mod config;
pub mod coordinator;
pub mod daemon;
mod date32;
pub mod distributed;
pub mod error;
pub mod exec;
pub mod ir;
pub mod network;
pub mod pbc;
pub mod planner;
pub mod quic;
pub mod rune_engine;
pub mod server;
pub mod sql_planner;
pub mod sql_wire;
pub mod storage;
pub mod telemetry;
mod timestamp;
pub mod wire;

#[cfg(test)]
mod tests {
	use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;
	use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
	use std::sync::Arc;

	use crate::binder::{
		BroadcastBuildSide, JoinStrategyDecisionReason, PlannerConfig, bind, bind_with_trace,
	};
	use crate::catalog::{
		Catalog, ColumnHeavyHitterFact, ColumnRangeDistributionFact, DistinctCountFact,
		MockCatalog, RangeDistributionBucket, RangeFact, TableOptimizerFacts, WendelonCatalog,
	};
	use crate::coordinator::DistributedQueryCoordinator;
	use crate::distributed::budget::{BudgetDimension, ByteCount, QueryBudget, TaskBudget};
	use crate::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
	use crate::distributed::fragment::{FragmentPlanConfig, fragment_plan};
	use crate::distributed::identity::{
		AttemptId, ExchangeId, LeaseEpoch, PartitionId, QueryId, TaskId, TaskIdentity, WorkerId,
	};
	use crate::distributed::partition_store::{
		HybridPartitionStore, InMemoryPartitionStore, PartitionPayload, PartitionStore,
	};
	use crate::distributed::scheduler::{
		DistributedScheduler, SchedulerError, StagePlan, TaskPlan,
	};
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
	use crate::error::{QueryError, RuntimeError, StorageError, TaskFailureKind};
	use crate::exec::Executor;
	use crate::ir::{
		AggregateFunction, BoundExprProgram, ColumnOrdinal, ExecutionPlan, Expr, JoinKind,
		JoinReplayId, JoinStrategy, JoinStrategyIntent, JoinStrategyPinSource, LogicalPlan,
		MorselId, ScalarLiteralKey, ScalarValue, SortExpr, StageId, TableId,
	};
	use crate::rune_engine::{RuneEngine, RuneSandboxConfig};
	use crate::server::{
		NetworkTaskDispatcher, ToronCoordinator, ToronWorker, memory::MemoryManager,
		spill::SpillManager, transport::TcpTransport,
	};
	use crate::storage::{InMemoryStorageScan, ScanRequest, ScanStream, StorageScan};
	use wendelon::schema::{
		KllK, LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};
	use wendelon::storage::file_ops::BlockingFileOps;
	use wendelon::storage::sketch::KllSketch;

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
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
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
	async fn direct_pinned_broadcast_over_cap_stays_terminal() {
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);

		let err = run_distributed_join_query_with_budget(broadcast_join_plan(), 2, query_budget)
			.await
			.expect_err("direct pinned broadcast cap must fail");

		assert_broadcast_cap_error(err);
	}

	#[tokio::test]
	async fn rune_authored_broadcast_join_digest_matches_hash_join_over_tcp() {
		let hash_results = run_distributed_join_query(rune_hash_join_plan().await, 16).await;
		let broadcast_results =
			run_distributed_join_query(rune_broadcast_join_plan().await, 3).await;

		assert_eq!(
			canonical_arrow_ipc_digest(&broadcast_results, SortPolicy::FullRowLex)
				.expect("broadcast digest"),
			canonical_arrow_ipc_digest(&hash_results, SortPolicy::FullRowLex).expect("hash digest")
		);
	}

	#[tokio::test]
	async fn rune_authored_broadcast_join_respects_cap_terminally() {
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);

		let err = run_distributed_join_query_with_budget(
			rune_broadcast_join_plan().await,
			2,
			query_budget,
		)
		.await
		.expect_err("Rune-authored broadcast cap must fail");

		assert_broadcast_cap_error(err);
	}

	#[tokio::test]
	async fn sql_broadcast_hint_digest_matches_hash_join_over_tcp() {
		let hash_results = run_distributed_join_query(sql_hash_join_plan(), 16).await;
		let broadcast_results = run_distributed_join_query(sql_broadcast_join_plan(), 3).await;
		let rune_broadcast_results =
			run_distributed_join_query(rune_broadcast_join_plan().await, 3).await;

		let hash_digest =
			canonical_arrow_ipc_digest(&hash_results, SortPolicy::FullRowLex).expect("hash digest");
		let broadcast_digest =
			canonical_arrow_ipc_digest(&broadcast_results, SortPolicy::FullRowLex)
				.expect("SQL broadcast digest");
		let rune_digest =
			canonical_arrow_ipc_digest(&rune_broadcast_results, SortPolicy::FullRowLex)
				.expect("Rune broadcast digest");

		assert_eq!(broadcast_digest, hash_digest);
		assert_eq!(broadcast_digest, rune_digest);
	}

	#[tokio::test]
	async fn sql_and_rune_broadcast_pins_have_distinct_sources() {
		assert_broadcast_pin_source(&sql_broadcast_join_plan(), JoinStrategyPinSource::SqlHint);
		assert_broadcast_pin_source(
			&rune_broadcast_join_plan().await,
			JoinStrategyPinSource::Rune,
		);
		assert_broadcast_pin_source(&broadcast_join_plan(), JoinStrategyPinSource::Direct);
	}

	#[tokio::test]
	async fn right_build_broadcast_preserves_output_schema_and_values() {
		let hash_results = run_distributed_join_query(sql_fact_dim_join_plan(), 16).await;
		let broadcast_results = run_distributed_join_query_with_catalog_config(
			sql_fact_dim_join_plan(),
			3,
			join_catalog_with_optimizer_facts(64 * 1024 * 1024, 1_024),
			PlannerConfig {
				auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
				..PlannerConfig::default()
			},
		)
		.await;

		assert_fact_dim_join_matches_hash(&hash_results, &broadcast_results);
	}

	#[tokio::test]
	async fn right_build_broadcast_byte_identical_to_hash() {
		let local_hash = run_local_join_query_with_catalog_config(
			sql_fact_dim_join_plan(),
			join_catalog(),
			PlannerConfig::default(),
		)
		.await;
		let local_broadcast = run_local_join_query_with_catalog_config(
			sql_fact_dim_join_plan(),
			join_catalog_with_optimizer_facts(64 * 1024 * 1024, 1_024),
			PlannerConfig {
				auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
				..PlannerConfig::default()
			},
		)
		.await;
		assert_fact_dim_join_matches_hash(&local_hash, &local_broadcast);

		let distributed_hash = run_distributed_join_query(sql_fact_dim_join_plan(), 16).await;
		let distributed_broadcast = run_distributed_join_query_with_catalog_config(
			sql_fact_dim_join_plan(),
			3,
			join_catalog_with_optimizer_facts(64 * 1024 * 1024, 1_024),
			PlannerConfig {
				auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
				..PlannerConfig::default()
			},
		)
		.await;
		assert_fact_dim_join_matches_hash(&distributed_hash, &distributed_broadcast);
	}

	#[tokio::test]
	async fn dominance_right_build_byte_identical_to_hash_over_tcp() {
		let hash_results = run_distributed_join_query(sql_fact_dim_join_plan(), 16).await;
		let broadcast_results = run_distributed_join_query_with_catalog_config(
			sql_fact_dim_join_plan(),
			3,
			join_catalog_with_optimizer_facts(3_072, 1_024),
			PlannerConfig {
				auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
				..PlannerConfig::default()
			},
		)
		.await;

		assert_fact_dim_join_matches_hash(&hash_results, &broadcast_results);
	}

	#[tokio::test]
	async fn three_way_broadcast_star_digest_matches_hash_over_tcp() {
		let logical = sql_fact_two_dim_join_plan();
		let hash_results = run_distributed_join_query(logical.clone(), 16).await;
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(64 * 1024);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(512 * 1024);
		let broadcast_results = run_distributed_join_query_with_catalog_config_and_budget(
			logical,
			3,
			join_catalog_with_optimizer_facts(64 * 1024 * 1024, 1_024),
			PlannerConfig {
				auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
				..PlannerConfig::default()
			},
			query_budget,
		)
		.await
		.expect("execute broadcast-star join");

		assert_eq!(
			canonical_arrow_ipc_digest(&broadcast_results, SortPolicy::FullRowLex)
				.expect("broadcast-star digest"),
			canonical_arrow_ipc_digest(&hash_results, SortPolicy::FullRowLex).expect("hash digest")
		);
	}

	#[tokio::test]
	async fn broadcast_star_over_cap_replays_all_task_local_dims_and_succeeds() {
		let logical = sql_fact_two_dim_join_plan();
		let hash_results = run_distributed_join_query(logical.clone(), 16).await;
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);

		let replay_results = run_distributed_join_query_with_catalog_config_and_budget(
			logical,
			3,
			join_catalog_with_optimizer_facts(64 * 1024 * 1024, 1_024),
			PlannerConfig {
				auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
				..PlannerConfig::default()
			},
			query_budget,
		)
		.await
		.expect("broadcast star should replay as hash");

		assert_eq!(
			canonical_arrow_ipc_digest(&replay_results, SortPolicy::FullRowLex)
				.expect("broadcast-star replay digest"),
			canonical_arrow_ipc_digest(&hash_results, SortPolicy::FullRowLex).expect("hash digest")
		);
	}

	#[tokio::test]
	async fn filtered_probe_broadcast_byte_identical_to_hash_over_tcp() {
		let hash_results =
			run_distributed_join_query(filtered_fact_probe_dim_left_join_plan(), 16).await;
		let broadcast_results = run_distributed_join_query_with_catalog_config(
			filtered_fact_probe_dim_left_join_plan(),
			3,
			join_catalog_with_optimizer_facts(64 * 1024 * 1024, 1_024),
			PlannerConfig {
				auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
				..PlannerConfig::default()
			},
		)
		.await;

		assert!(!hash_results.is_empty());
		assert!(!broadcast_results.is_empty());
		assert_eq!(broadcast_results[0].schema(), hash_results[0].schema());
		assert_eq!(
			canonical_arrow_ipc_digest(&broadcast_results, SortPolicy::FullRowLex)
				.expect("filtered-probe broadcast digest"),
			canonical_arrow_ipc_digest(&hash_results, SortPolicy::FullRowLex).expect("hash digest")
		);
	}

	#[tokio::test]
	async fn sql_filter_pushdown_digest_matches_above_join_filter() {
		let pushed = crate::sql_planner::SqlPlanner::new()
			.plan(
				"SELECT * FROM orders \
				 INNER JOIN customers ON orders.customer_id = customers.id \
				 WHERE customers.region = 'west'",
			)
			.expect("plan pushed SQL filter");
		let above_join = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Join {
				left: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("orders"),
					projection: None,
				}),
				right: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("customers"),
					projection: None,
				}),
				kind: JoinKind::Inner,
				on: vec![(
					Expr::qualified_column(crate::catalog::TableRef::new("orders"), "customer_id"),
					Expr::qualified_column(crate::catalog::TableRef::new("customers"), "id"),
				)],
				strategy_intent: JoinStrategyIntent::Auto,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(
					crate::catalog::TableRef::new("customers"),
					"region",
				)),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("west".to_string())))),
			),
		};
		let catalog = join_catalog_with_customers_row_upper_bound_and_ndv(
			20_000,
			BTreeMap::from([(
				ColumnOrdinal(1),
				DistinctCountFact::Approx {
					estimate: 10_000,
					rel_err_ppm: 26_000,
				},
			)]),
		);

		let pushed_results = run_local_join_query_with_catalog_config(
			pushed,
			catalog.clone(),
			PlannerConfig::default(),
		)
		.await;
		let above_join_results =
			run_local_join_query_with_catalog_config(above_join, catalog, PlannerConfig::default())
				.await;

		assert_eq!(
			canonical_arrow_ipc_digest(&pushed_results, SortPolicy::FullRowLex)
				.expect("pushed digest"),
			canonical_arrow_ipc_digest(&above_join_results, SortPolicy::FullRowLex)
				.expect("above-join digest")
		);
	}

	#[tokio::test]
	async fn sql_outer_join_chain_filter_stays_above_and_returns_empty_counterexample() {
		let plan = crate::sql_planner::SqlPlanner::new()
			.plan(
				"SELECT c.id, b.x \
				 FROM a INNER JOIN b ON a.k = b.k \
				 RIGHT OUTER JOIN c ON c.j = b.j \
				 WHERE b.x = 1",
			)
			.expect("plan outer-chain counterexample");
		let catalog = outer_join_counterexample_catalog();
		let bound = bind(&plan, catalog.as_ref(), &PlannerConfig::default())
			.await
			.expect("bind outer-chain counterexample");
		let results = Executor::new(outer_join_counterexample_storage())
			.execute_bound(&bound, None)
			.await
			.expect("execute outer-chain counterexample");

		assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
	}

	#[tokio::test]
	async fn estimated_broadcast_over_cap_replays_as_hash_and_matches_digest() {
		let hash_results = run_distributed_join_query(sql_fact_dim_join_plan(), 16).await;
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);

		let replay_results = run_distributed_join_query_with_catalog_config_and_budget(
			sql_fact_dim_join_plan(),
			3,
			join_catalog_with_customers_row_upper_bound(12),
			PlannerConfig::default(),
			query_budget,
		)
		.await
		.expect("estimated broadcast should replay as hash");

		assert_fact_dim_join_matches_hash(&hash_results, &replay_results);
	}

	#[tokio::test]
	async fn ndv_underestimate_over_cap_replays_as_hash() {
		let hash_results = run_distributed_join_query(ndv_filtered_fact_dim_join_plan(), 16).await;
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);

		let replay_results = run_distributed_join_query_with_catalog_config_and_budget(
			ndv_filtered_fact_dim_join_plan(),
			3,
			join_catalog_with_customers_row_upper_bound_and_ndv(
				20_000,
				BTreeMap::from([(
					ColumnOrdinal(1),
					DistinctCountFact::Approx {
						estimate: 20_000,
						rel_err_ppm: 26_000,
					},
				)]),
			),
			PlannerConfig::default(),
			query_budget,
		)
		.await
		.expect("NDV-estimated broadcast should replay as hash");

		assert_fact_dim_join_matches_hash(&hash_results, &replay_results);
	}

	#[tokio::test]
	async fn overestimated_inner_join_broadcast_over_cap_replays_as_hash_and_matches_digest() {
		let plan = join_cardinality_three_table_plan();
		let catalog = join_cardinality_catalog();
		let storage = join_cardinality_storage();

		let hash = run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
			plan.clone(),
			catalog.clone(),
			PlannerConfig {
				force_hash_join_ids: BTreeSet::from([JoinReplayId(0), JoinReplayId(1)]),
				..PlannerConfig::default()
			},
			QueryBudget::test_default(),
			storage.clone(),
		)
		.await
		.expect("execute forced hash containment join");
		assert!(!hash.replayed, "forced hash baseline should not replay");

		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
		let replay =
			run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
				plan,
				catalog,
				PlannerConfig::default(),
				query_budget,
				storage,
			)
			.await
			.expect("containment-estimated broadcast should replay as hash");

		assert!(
			replay.replayed,
			"expected broadcast-cap replay for containment-estimated parent join"
		);
		assert_eq!(
			canonical_arrow_ipc_digest(&replay.results, SortPolicy::FullRowLex)
				.expect("containment replay digest"),
			canonical_arrow_ipc_digest(&hash.results, SortPolicy::FullRowLex)
				.expect("forced hash digest")
		);
	}

	#[tokio::test]
	async fn co_hot_join_recovers_skew_and_hashes_without_replay() {
		let plan = join_cardinality_three_table_plan();
		let uniform_catalog = join_cardinality_catalog_with_join_key_skew_facts(false);
		let skew_catalog = join_cardinality_catalog_with_join_key_skew_facts(true);
		let storage = join_cardinality_storage();
		let planner_config = || PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(512 * 1024)),
			..PlannerConfig::default()
		};

		let (_uniform_bound, uniform_trace) =
			bind_with_trace(&plan, uniform_catalog.as_ref(), &planner_config())
				.await
				.expect("bind stale-uniform join-skew baseline");
		assert_eq!(uniform_trace.join_decisions.len(), 2);
		assert!(
			matches!(
				uniform_trace.join_decisions[1].reason,
				JoinStrategyDecisionReason::BroadcastEstimated {
					build_side: BroadcastBuildSide::Left,
					..
				}
			),
			"stale uniform facts should leave the parent join as estimated broadcast: {:?}",
			uniform_trace.join_decisions[1].reason
		);

		let mut replay_budget = QueryBudget::test_default();
		replay_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		replay_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
		let replay =
			run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
				plan.clone(),
				uniform_catalog,
				planner_config(),
				replay_budget,
				storage.clone(),
			)
			.await
			.expect("stale-uniform parent broadcast should replay");
		assert!(
			replay.replayed,
			"same plan without co-hot facts should replay under a tiny broadcast cap"
		);

		let (_skew_bound, skew_trace) =
			bind_with_trace(&plan, skew_catalog.as_ref(), &planner_config())
				.await
				.expect("bind co-hot join-skew estimate");
		assert_eq!(skew_trace.join_decisions.len(), 2);
		assert_eq!(
			skew_trace.join_decisions[1].strategy,
			JoinStrategy::HashPartitioned
		);
		assert!(
			!matches!(
				skew_trace.join_decisions[1].reason,
				JoinStrategyDecisionReason::BroadcastEstimated { .. }
			),
			"co-hot child estimate should prevent parent estimated broadcast: {:?}",
			skew_trace.join_decisions[1].reason
		);

		let hash = run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
			plan.clone(),
			skew_catalog.clone(),
			PlannerConfig {
				force_hash_join_ids: BTreeSet::from([JoinReplayId(0), JoinReplayId(1)]),
				..planner_config()
			},
			QueryBudget::test_default(),
			storage.clone(),
		)
		.await
		.expect("execute forced hash co-hot join");
		assert!(!hash.replayed, "forced hash baseline should not replay");

		let mut hash_budget = QueryBudget::test_default();
		hash_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		hash_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
		let run = run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
			plan,
			skew_catalog,
			planner_config(),
			hash_budget,
			storage,
		)
		.await
		.expect("co-hot join-skew estimate should execute as hash without replay");

		assert!(
			!run.replayed,
			"co-hot join-skew estimate should hash up front without broadcast replay"
		);
		assert_eq!(
			canonical_arrow_ipc_digest(&run.results, SortPolicy::FullRowLex)
				.expect("co-hot no-replay digest"),
			canonical_arrow_ipc_digest(&hash.results, SortPolicy::FullRowLex)
				.expect("forced hash digest")
		);
	}

	#[tokio::test]
	async fn range_disjoint_child_join_broadcasts_zero_estimate_without_replay() {
		let plan = join_cardinality_three_table_plan();
		let catalog = join_cardinality_catalog_with_join_key_ranges(
			Some(range_fact_i64(1, 5, 10_000)),
			Some(range_fact_i64(100, 200, 10_000)),
		);
		assert_range_disjoint_child_join_binds_parent_as_estimated_broadcast(
			&plan,
			catalog.as_ref(),
		)
		.await;
		let storage = join_cardinality_disjoint_storage();

		let hash = run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
			plan.clone(),
			catalog.clone(),
			PlannerConfig {
				force_hash_join_ids: BTreeSet::from([JoinReplayId(0), JoinReplayId(1)]),
				..PlannerConfig::default()
			},
			QueryBudget::test_default(),
			storage.clone(),
		)
		.await
		.expect("execute forced hash disjoint range join");
		assert!(!hash.replayed, "forced hash baseline should not replay");

		let run = run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
			plan,
			catalog,
			PlannerConfig::default(),
			QueryBudget::test_default(),
			storage,
		)
		.await
		.expect("range-disjoint estimated broadcast should complete");

		assert!(
			!run.replayed,
			"clean disjoint range estimate should complete without replay"
		);
		assert_eq!(
			hash.results
				.iter()
				.map(RecordBatch::num_rows)
				.sum::<usize>(),
			0
		);
		assert_eq!(
			run.results.iter().map(RecordBatch::num_rows).sum::<usize>(),
			0
		);
	}

	#[tokio::test]
	async fn stale_disjoint_join_range_underestimate_replays_as_hash_and_matches_digest() {
		let plan = join_cardinality_three_table_plan();
		let catalog = join_cardinality_catalog_with_join_key_ranges(
			Some(range_fact_i64(1, 5, 10_000)),
			Some(range_fact_i64(100, 200, 10_000)),
		);
		assert_range_disjoint_child_join_binds_parent_as_estimated_broadcast(
			&plan,
			catalog.as_ref(),
		)
		.await;
		let storage = join_cardinality_storage();

		let hash = run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
			plan.clone(),
			catalog.clone(),
			PlannerConfig {
				force_hash_join_ids: BTreeSet::from([JoinReplayId(0), JoinReplayId(1)]),
				..PlannerConfig::default()
			},
			QueryBudget::test_default(),
			storage.clone(),
		)
		.await
		.expect("execute forced hash stale-disjoint range join");
		assert!(!hash.replayed, "forced hash baseline should not replay");

		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
		let replay =
			run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
				plan,
				catalog,
				PlannerConfig::default(),
				query_budget,
				storage,
			)
			.await
			.expect("stale range-underestimated broadcast should replay as hash");

		assert!(
			replay.replayed,
			"stale disjoint range estimate should replay when runtime bytes exceed cap"
		);
		assert_eq!(
			canonical_arrow_ipc_digest(&replay.results, SortPolicy::FullRowLex)
				.expect("stale range replay digest"),
			canonical_arrow_ipc_digest(&hash.results, SortPolicy::FullRowLex)
				.expect("forced hash digest")
		);
	}

	mod skew_evidence {
		use super::*;

		#[tokio::test]
		async fn uniform_ndv_hot_key_miss_triggers_broadcast_replay() {
			let fixture = skewed_join_fixture();
			let auto_plan = skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_skewed_hot_key_binds_as_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;
			let hash_plan = skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto);
			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					hash_plan,
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
						..PlannerConfig::default()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash skewed join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let replay =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					PlannerConfig::default(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("skewed NDV-estimated broadcast should replay as hash");

			assert!(
				replay.replayed,
				"expected broadcast-cap replay for hot-key miss"
			);
			assert_skewed_join_matches_hash(&hash.results, &replay.results);
		}

		#[tokio::test]
		async fn uniform_ndv_miss_within_headroom_is_absorbed_no_replay() {
			let fixture = skewed_join_fixture();
			let auto_plan = skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_skewed_hot_key_binds_as_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;
			let actual_hot_rows = fixture.actual_hot_rows;

			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					PlannerConfig::default(),
					QueryBudget::test_default(),
					fixture.storage,
				)
				.await
				.expect("absorbed skew miss should complete as broadcast");

			assert!(
				!run.replayed,
				"hot-key miss under the runtime cap should not need hash replay"
			);
			assert_eq!(
				run.results.iter().map(RecordBatch::num_rows).sum::<usize>(),
				actual_hot_rows
			);
		}

		#[tokio::test]
		async fn range_skew_filter_records_min_max_uniform_miss() {
			let fixture = range_skew_join_fixture();
			let filtered = range_skew_filtered_customers_plan(range_skew_amount_between(
				RANGE_SKEW_FILTER_LOW,
				RANGE_SKEW_FILTER_HIGH,
			));
			let bound = bind(
				&filtered,
				fixture.catalog.as_ref(),
				&PlannerConfig::default(),
			)
			.await
			.expect("bind range-skew filtered customers");
			let results = Executor::new(fixture.storage.clone())
				.execute_bound(&bound, None)
				.await
				.expect("execute range-skew filtered customers");
			let actual_rows = results.iter().map(RecordBatch::num_rows).sum::<usize>();

			assert_eq!(actual_rows, fixture.actual_range_rows);
			assert_eq!(
				RANGE_SKEW_UNIFORM_ESTIMATE,
				((RANGE_SKEW_BASE_ROWS as u64)
					* ((RANGE_SKEW_FILTER_HIGH - RANGE_SKEW_FILTER_LOW + 1) as u64))
					.div_ceil((RANGE_SKEW_ENVELOPE_HIGH - RANGE_SKEW_FILTER_LOW + 1) as u64)
			);
			assert!(
				fixture.actual_range_rows as u64 > RANGE_SKEW_UNIFORM_ESTIMATE * 80,
				"clustered actual rows should materially exceed the min/max uniform estimate"
			);
			assert_range_skew_binds_as_estimated_broadcast(
				&range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto),
				fixture.catalog.as_ref(),
			)
			.await;
		}

		#[tokio::test]
		async fn range_skew_miss_triggers_broadcast_replay() {
			let fixture = range_skew_join_fixture();
			let auto_plan = range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_range_skew_binds_as_estimated_broadcast(&auto_plan, fixture.catalog.as_ref())
				.await;
			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
						..PlannerConfig::default()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash range-skew join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let replay =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					PlannerConfig::default(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("range-skew estimated broadcast should replay as hash");

			assert!(
				replay.replayed,
				"expected broadcast-cap replay for range skew miss"
			);
			assert_range_skew_join_matches_hash(&hash.results, &replay.results);
		}

		#[tokio::test]
		async fn range_skew_miss_within_headroom_is_absorbed_no_replay() {
			let fixture = range_skew_join_fixture();
			let auto_plan = range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_range_skew_binds_as_estimated_broadcast(&auto_plan, fixture.catalog.as_ref())
				.await;
			let actual_range_rows = fixture.actual_range_rows;

			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					PlannerConfig::default(),
					QueryBudget::test_default(),
					fixture.storage,
				)
				.await
				.expect("absorbed range-skew miss should complete as broadcast");

			assert!(
				!run.replayed,
				"range-skew miss under the runtime cap should not need hash replay"
			);
			assert_eq!(
				run.results.iter().map(RecordBatch::num_rows).sum::<usize>(),
				actual_range_rows
			);
		}

		#[tokio::test]
		async fn range_distribution_fact_makes_clustered_range_filter_hash_without_replay() {
			let fixture = range_skew_join_fixture_with_range_distribution();
			let auto_plan = range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_range_distribution_binds_without_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;
			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
						..range_distribution_hash_config()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash range-distribution join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					range_distribution_hash_config(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("range-distribution hash join should not replay");

			assert!(
				!run.replayed,
				"distribution estimate should bind hash directly"
			);
			assert_range_skew_join_matches_hash(&hash.results, &run.results);
		}

		#[tokio::test]
		async fn produced_range_distribution_fact_makes_clustered_filter_hash_without_seeding() {
			let fixture = range_skew_join_fixture_with_produced_storage_range_distribution_fact();
			let auto_plan = range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_range_distribution_binds_without_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;
			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
						..range_distribution_hash_config()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash produced range-distribution join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					range_distribution_hash_config(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("produced range-distribution fact should execute as hash without replay");

			assert!(
				!run.replayed,
				"produced range-distribution fact should hash up front without broadcast replay"
			);
			assert_range_skew_join_matches_hash(&hash.results, &run.results);
		}

		#[tokio::test]
		async fn range_skew_evidence_uses_range_fact_not_heavy_hitter_or_ndv() {
			let facts = range_skew_customers_optimizer_facts();
			assert!(facts.column_ndv.is_empty());
			assert_eq!(facts.column_range.len(), 1);
			assert!(facts.column_heavy_hitters.is_empty());

			let fixture = range_skew_join_fixture();
			assert_range_skew_binds_as_estimated_broadcast(
				&range_skew_fact_dim_join_plan(JoinStrategyIntent::Auto),
				fixture.catalog.as_ref(),
			)
			.await;
		}

		#[tokio::test]
		async fn hot_key_filter_with_heavy_hitter_fact_estimates_frequency_and_hashes() {
			let fixture = skewed_join_fixture_with_heavy_hitter_hot_key();
			let auto_plan = skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_skewed_hot_key_binds_without_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;

			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
						..PlannerConfig::default()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash heavy-hitter skewed join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					PlannerConfig::default(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("heavy-hitter-estimated hash should execute without broadcast replay");

			assert!(
				!run.replayed,
				"heavy-hitter fact should hash up front without broadcast replay"
			);
			assert_skewed_join_matches_hash(&hash.results, &run.results);
		}

		#[tokio::test]
		async fn produced_heavy_hitter_fact_makes_hot_key_filter_hash_without_seeding() {
			let fixture = skewed_join_fixture_with_produced_storage_heavy_hitter_fact();
			let auto_plan = skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_skewed_hot_key_binds_without_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;

			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
						..PlannerConfig::default()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash produced-heavy-hitter skewed join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					PlannerConfig::default(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("produced heavy-hitter fact should execute as hash without replay");

			assert!(
				!run.replayed,
				"produced heavy-hitter fact should hash up front without broadcast replay"
			);
			assert_skewed_join_matches_hash(&hash.results, &run.results);
		}

		#[tokio::test]
		async fn live_column_group_top_k_recovers_multi_key_skew_and_hashes_without_seeding() {
			let fixture = multi_key_join_fixture_with_produced_storage_group_top_k_fact();
			let actual_rows = fixture.actual_rows;
			let auto_plan = multi_key_group_top_k_parent_join_plan();
			assert_multi_key_group_top_k_binds_without_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;

			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					multi_key_group_top_k_parent_join_plan(),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0), JoinReplayId(1)]),
						..multi_key_group_top_k_hash_config()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash produced group top-k join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					multi_key_group_top_k_hash_config(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("produced group top-k fact should execute as hash without replay");

			assert!(
				!run.replayed,
				"produced column-group top-k fact should hash parent join up front"
			);
			assert_eq!(
				run.results.iter().map(RecordBatch::num_rows).sum::<usize>(),
				actual_rows
			);
			assert_multi_key_group_top_k_join_matches_hash(&hash.results, &run.results);
		}

		#[tokio::test]
		async fn live_column_group_ndv_recovers_multi_key_containment_and_hashes_without_seeding() {
			let fixture = multi_key_join_fixture_with_produced_storage_group_ndv_fact();
			let actual_rows = fixture.actual_rows;
			let auto_plan = multi_key_group_ndv_parent_join_plan();
			assert_multi_key_group_ndv_binds_without_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;

			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					multi_key_group_ndv_parent_join_plan(),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0), JoinReplayId(1)]),
						..multi_key_group_ndv_hash_config()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash produced group NDV join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					multi_key_group_ndv_hash_config(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("produced group NDV fact should execute as hash without replay");

			assert!(
				!run.replayed,
				"produced column-group NDV fact should hash parent join up front"
			);
			assert_eq!(
				run.results.iter().map(RecordBatch::num_rows).sum::<usize>(),
				actual_rows
			);
			assert_multi_key_group_top_k_join_matches_hash(&hash.results, &run.results);
		}

		#[tokio::test]
		async fn live_column_group_hot_cold_recovers_multi_key_skew_and_correlation_without_seeding()
		 {
			let fixture = multi_key_join_fixture_with_produced_storage_group_hot_cold_facts();
			let actual_rows = fixture.actual_rows;
			let auto_plan = multi_key_group_top_k_parent_join_plan();
			assert_multi_key_group_hot_cold_binds_without_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;

			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					multi_key_group_top_k_parent_join_plan(),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0), JoinReplayId(1)]),
						..multi_key_group_hot_cold_hash_config()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash produced group hot+cold join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let run =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					multi_key_group_hot_cold_hash_config(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("produced group hot+cold facts should execute as hash without replay");

			assert!(
				!run.replayed,
				"produced column-group hot+cold facts should hash parent join up front"
			);
			assert_eq!(
				run.results.iter().map(RecordBatch::num_rows).sum::<usize>(),
				actual_rows
			);
			assert_multi_key_group_top_k_join_matches_hash(&hash.results, &run.results);
		}

		#[tokio::test]
		async fn stale_low_heavy_hitter_frequency_still_replays_as_hash() {
			let fixture = skewed_join_fixture_with_heavy_hitter_frequency(100);
			let auto_plan = skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto);
			assert_skewed_hot_key_binds_as_estimated_broadcast(
				&auto_plan,
				fixture.catalog.as_ref(),
			)
			.await;

			let hash =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					skewed_hot_key_fact_dim_join_plan(JoinStrategyIntent::Auto),
					fixture.catalog.clone(),
					PlannerConfig {
						force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
						..PlannerConfig::default()
					},
					QueryBudget::test_default(),
					fixture.storage.clone(),
				)
				.await
				.expect("execute forced hash stale-low skewed join");
			assert!(!hash.replayed, "forced hash baseline should not replay");

			let mut query_budget = QueryBudget::test_default();
			query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
			query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);
			let replay =
				run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
					auto_plan,
					fixture.catalog,
					PlannerConfig::default(),
					query_budget,
					fixture.storage,
				)
				.await
				.expect("stale-low heavy-hitter broadcast should replay as hash");

			assert!(
				replay.replayed,
				"stale-low heavy-hitter fact should remain broadcast-replay safe"
			);
			assert_skewed_join_matches_hash(&hash.results, &replay.results);
		}
	}

	#[tokio::test]
	async fn hard_bound_broadcast_over_cap_replays_as_hash_and_matches_digest() {
		let hash_results = run_distributed_join_query(sql_fact_dim_join_plan(), 16).await;
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);

		let replay_results = run_distributed_join_query_with_catalog_config_and_budget(
			sql_fact_dim_join_plan(),
			3,
			join_catalog_with_optimizer_facts(64 * 1024 * 1024, 1_024),
			PlannerConfig::default(),
			query_budget,
		)
		.await
		.expect("hard-bound broadcast should replay as hash");

		assert_fact_dim_join_matches_hash(&hash_results, &replay_results);
	}

	#[tokio::test]
	async fn sql_broadcast_hint_over_cap_replays_as_hash() {
		let hash_results = run_distributed_join_query(sql_hash_join_plan(), 16).await;
		let mut query_budget = QueryBudget::test_default();
		query_budget.broadcast_build_bytes_per_task = ByteCount::new(1);
		query_budget.broadcast_replicated_bytes_per_query = ByteCount::new(1);

		let replay_results =
			run_distributed_join_query_with_budget(sql_broadcast_join_plan(), 2, query_budget)
				.await
				.expect("SQL broadcast hint should replay as hash");

		assert_eq!(
			canonical_arrow_ipc_digest(&replay_results, SortPolicy::FullRowLex)
				.expect("SQL replay digest"),
			canonical_arrow_ipc_digest(&hash_results, SortPolicy::FullRowLex).expect("hash digest")
		);
	}

	#[tokio::test]
	async fn replay_uses_fresh_fragment_namespace() {
		let bound = bind(
			&hash_join_customers_left_plan(),
			join_catalog().as_ref(),
			&PlannerConfig::default(),
		)
		.await
		.expect("bind hash join");
		let query_id = QueryId::from_u128(139);
		let config = FragmentPlanConfig::new(
			query_id,
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		);
		let stages0 = fragment_plan(bound.clone(), config).expect("fragment attempt 0");
		let stages1 = fragment_plan(bound, config.with_id_namespace(1)).expect("fragment replay");

		assert!(
			stage_ids(&stages0).is_disjoint(&stage_ids(&stages1)),
			"replay stages must not reuse attempt-0 stage ids"
		);
		assert!(
			exchange_ids(&stages0).is_disjoint(&exchange_ids(&stages1)),
			"replay exchanges must not reuse attempt-0 exchange ids"
		);
		assert!(
			stages1
				.iter()
				.flat_map(|stage| &stage.tasks)
				.all(|task| task.spec.identity.query_id == query_id
					&& task.spec.identity.attempt_id == AttemptId::first())
		);
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
			input: Box::new(LogicalPlan::Sort {
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
				order_by: vec![SortExpr {
					expr: Expr::column("key"),
					descending: false,
					nulls_first: false,
				}],
				limit: None,
				offset: 0,
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
		assert!(!w1_partitions.is_empty());
		assert!(!w2_partitions.is_empty());
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
			producer_partitions: vec![crate::distributed::task::ShufflePartitionRef::Sealed(
				ProducerPartitionRef {
					address,
					worker_id: producer_worker_id,
					endpoint: producer_endpoint,
					size_bytes: ByteCount::new(
						u64::try_from(partition_bytes.len()).expect("payload length fits u64"),
					),
					digest: ArrowIpcDigest::new(wendelon::hash::blake3::digest(&partition_bytes)),
				},
			)],
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
		Arc::new(join_catalog_inner())
	}

	fn join_catalog_with_optimizer_facts(
		orders_byte_upper_bound: u64,
		customers_byte_upper_bound: u64,
	) -> Arc<MockCatalog> {
		let mut catalog = join_catalog_inner();
		for (table, bytes) in [
			("orders", orders_byte_upper_bound),
			("customers", customers_byte_upper_bound),
		] {
			catalog
				.seed_optimizer_facts(
					&crate::catalog::TableRef::new(table),
					TableOptimizerFacts {
						byte_upper_bound: Some(ByteCount::new(bytes)),
						row_upper_bound: None,
						column_ndv: Default::default(),
						column_range: Default::default(),
						column_range_distributions: Default::default(),
						column_heavy_hitters: Default::default(),
						column_group_top_k: Default::default(),
						column_group_ndv: Default::default(),
					},
				)
				.expect("seed optimizer facts");
		}
		Arc::new(catalog)
	}

	fn join_catalog_with_customers_row_upper_bound(
		customers_row_upper_bound: u64,
	) -> Arc<MockCatalog> {
		join_catalog_with_customers_row_upper_bound_and_ndv(
			customers_row_upper_bound,
			BTreeMap::new(),
		)
	}

	fn join_catalog_with_customers_row_upper_bound_and_ndv(
		customers_row_upper_bound: u64,
		column_ndv: BTreeMap<ColumnOrdinal, DistinctCountFact>,
	) -> Arc<MockCatalog> {
		let mut catalog = join_catalog_inner();
		catalog
			.seed_optimizer_facts(
				&crate::catalog::TableRef::new("customers"),
				TableOptimizerFacts {
					byte_upper_bound: None,
					row_upper_bound: Some(RowCount::new(customers_row_upper_bound)),
					column_ndv,
					column_range: Default::default(),
					column_range_distributions: Default::default(),
					column_heavy_hitters: Default::default(),
					column_group_top_k: Default::default(),
					column_group_ndv: Default::default(),
				},
			)
			.expect("seed customers row optimizer facts");
		Arc::new(catalog)
	}

	fn join_catalog_inner() -> MockCatalog {
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
		catalog
	}

	fn join_cardinality_three_table_plan() -> LogicalPlan {
		crate::sql_planner::SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c ON o.customer_id = c.id \
				 INNER JOIN regions r ON c.region_id = r.id",
			)
			.expect("join-cardinality SQL plan")
	}

	fn join_cardinality_catalog() -> Arc<MockCatalog> {
		join_cardinality_catalog_with_join_key_ranges(None, None)
	}

	fn join_cardinality_catalog_with_join_key_skew_facts(
		include_heavy_hitters: bool,
	) -> Arc<MockCatalog> {
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
						Field::new("region_id", DataType::Int64, false),
					]),
				),
			)
			.unwrap();
		catalog
			.register_descriptor(
				TableId(3),
				named_descriptor(
					3,
					"regions",
					Schema::new(vec![Field::new("id", DataType::Int64, false)]),
				),
			)
			.unwrap();
		for (table, join_key) in [
			("orders", ColumnOrdinal(1)),
			("customers", ColumnOrdinal(0)),
		] {
			let column_heavy_hitters = include_heavy_hitters
				.then(|| {
					BTreeMap::from([(
						join_key,
						ColumnHeavyHitterFact {
							entries: BTreeMap::from([(ScalarLiteralKey::Int64(2), 5_000)]),
						},
					)])
				})
				.unwrap_or_default();
			catalog
				.seed_optimizer_facts(
					&crate::catalog::TableRef::new(table),
					TableOptimizerFacts {
						byte_upper_bound: Some(ByteCount::new(2 * 1024 * 1024)),
						row_upper_bound: Some(RowCount::new(10_000)),
						column_ndv: BTreeMap::from([(join_key, DistinctCountFact::Exact(10_000))]),
						column_range: Default::default(),
						column_range_distributions: Default::default(),
						column_heavy_hitters,
						column_group_top_k: Default::default(),
						column_group_ndv: Default::default(),
					},
				)
				.expect("seed join-skew cardinality facts");
		}
		Arc::new(catalog)
	}

	fn join_cardinality_catalog_with_join_key_ranges(
		orders_range: Option<RangeFact>,
		customers_range: Option<RangeFact>,
	) -> Arc<MockCatalog> {
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
						Field::new("region_id", DataType::Int64, false),
					]),
				),
			)
			.unwrap();
		catalog
			.register_descriptor(
				TableId(3),
				named_descriptor(
					3,
					"regions",
					Schema::new(vec![Field::new("id", DataType::Int64, false)]),
				),
			)
			.unwrap();
		for (table, join_key, range) in [
			("orders", ColumnOrdinal(1), orders_range),
			("customers", ColumnOrdinal(0), customers_range),
		] {
			catalog
				.seed_optimizer_facts(
					&crate::catalog::TableRef::new(table),
					TableOptimizerFacts {
						byte_upper_bound: None,
						row_upper_bound: Some(RowCount::new(1)),
						column_ndv: BTreeMap::from([(join_key, DistinctCountFact::Exact(1_000))]),
						column_range: range
							.map(|range| BTreeMap::from([(join_key, range)]))
							.unwrap_or_default(),
						column_range_distributions: Default::default(),
						column_heavy_hitters: Default::default(),
						column_group_top_k: Default::default(),
						column_group_ndv: Default::default(),
					},
				)
				.expect("seed stale-low join cardinality facts");
		}
		Arc::new(catalog)
	}

	fn range_fact_i64(min: i64, max: i64, non_null_count: u64) -> RangeFact {
		RangeFact {
			arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT64,
			null_count: 0,
			non_null_count,
			min: Some(wendelon_client::encode_signed_optimizer_fact_scalar(min, 8).unwrap()),
			max: Some(wendelon_client::encode_signed_optimizer_fact_scalar(max, 8).unwrap()),
		}
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

	fn join_cardinality_storage() -> Arc<MultiTableStorageScan> {
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]));
		let customers_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region_id", DataType::Int64, false),
		]));
		let regions_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let orders = RecordBatch::try_new(
			orders_schema,
			vec![
				Arc::new(Int64Array::from(vec![100, 101, 102, 103, 104, 105])),
				Arc::new(Int64Array::from(vec![1, 2, 2, 3, 4, 5])),
			],
		)
		.unwrap();
		let customers = RecordBatch::try_new(
			customers_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
				Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
			],
		)
		.unwrap();
		let regions = RecordBatch::try_new(
			regions_schema,
			vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]))],
		)
		.unwrap();
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([
				(TableId(1), orders),
				(TableId(2), customers),
				(TableId(3), regions),
			]),
		})
	}

	fn join_cardinality_disjoint_storage() -> Arc<MultiTableStorageScan> {
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]));
		let customers_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region_id", DataType::Int64, false),
		]));
		let regions_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let orders = RecordBatch::try_new(
			orders_schema,
			vec![
				Arc::new(Int64Array::from(vec![100, 101, 102])),
				Arc::new(Int64Array::from(vec![1, 2, 3])),
			],
		)
		.unwrap();
		let customers = RecordBatch::try_new(
			customers_schema,
			vec![
				Arc::new(Int64Array::from(vec![100, 101])),
				Arc::new(Int64Array::from(vec![10, 20])),
			],
		)
		.unwrap();
		let regions = RecordBatch::try_new(
			regions_schema,
			vec![Arc::new(Int64Array::from(vec![10, 20]))],
		)
		.unwrap();
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([
				(TableId(1), orders),
				(TableId(2), customers),
				(TableId(3), regions),
			]),
		})
	}

	struct SkewedJoinFixture {
		catalog: Arc<dyn Catalog>,
		storage: Arc<dyn StorageScan>,
		actual_hot_rows: usize,
	}

	fn skewed_join_fixture() -> SkewedJoinFixture {
		skewed_join_fixture_with_heavy_hitters(BTreeMap::new())
	}

	fn skewed_join_fixture_with_heavy_hitter_hot_key() -> SkewedJoinFixture {
		skewed_join_fixture_with_heavy_hitter_frequency(5_000)
	}

	fn skewed_join_fixture_with_heavy_hitter_frequency(frequency: u64) -> SkewedJoinFixture {
		skewed_join_fixture_with_heavy_hitters(BTreeMap::from([(
			ColumnOrdinal(1),
			ColumnHeavyHitterFact {
				entries: BTreeMap::from([(ScalarLiteralKey::Utf8("hot".to_string()), frequency)]),
			},
		)]))
	}

	fn skewed_join_fixture_with_produced_storage_heavy_hitter_fact() -> SkewedJoinFixture {
		const BASE_ROWS: usize = 10_000;
		const HOT_ROWS: usize = 5_000;
		const NDV: u64 = 100;

		let orders_descriptor = named_descriptor(
			1,
			"orders",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("customer_id", DataType::Int64, false),
			]),
		);
		let customers_descriptor = named_descriptor(
			2,
			"customers",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("region", DataType::Utf8, true),
			]),
		);
		let storage_facts = wendelon_client::TableOptimizerFacts {
			table_object_id: customers_descriptor.table_object_id,
			schema_generation: customers_descriptor.schema_generation,
			physical_descriptor_fingerprint: customers_descriptor.physical_descriptor_fingerprint,
			producer_epoch: 156,
			row_upper_bound: Some(BASE_ROWS as u64),
			column_ndv: vec![wendelon_client::ColumnNdvFact {
				column_ordinal: 1,
				distinct: wendelon_client::NdvFact::Exact { count: NDV },
			}],
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: vec![wendelon_client::ColumnHeavyHitterFact {
				column_ordinal: 1,
				arrow_type_tag: wendelon_client::ARROW_STATS_TAG_UTF8,
				entries: vec![wendelon_client::ColumnHeavyHitterEntry {
					value_key: b"hot".to_vec(),
					frequency: HOT_ROWS as u64,
				}],
			}],
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		};
		let descriptor_facts = crate::catalog::descriptor_optimizer_facts_from_storage(
			storage_facts,
			&customers_descriptor,
		)
		.expect("storage optimizer facts map through descriptor sync");
		assert!(
			!descriptor_facts.column_heavy_hitters.is_empty(),
			"storage-mapped heavy-hitter fact should be non-empty"
		);
		let catalog = WendelonCatalog::from_descriptors_with_optimizer_facts([
			(TableId(1), orders_descriptor, None),
			(TableId(2), customers_descriptor, Some(descriptor_facts)),
		])
		.expect("descriptor registry with produced heavy-hitter facts");

		SkewedJoinFixture {
			catalog: Arc::new(catalog),
			storage: skewed_join_storage(BASE_ROWS, HOT_ROWS, NDV),
			actual_hot_rows: HOT_ROWS,
		}
	}

	fn skewed_join_fixture_with_heavy_hitters(
		column_heavy_hitters: BTreeMap<ColumnOrdinal, ColumnHeavyHitterFact>,
	) -> SkewedJoinFixture {
		const BASE_ROWS: usize = 10_000;
		const HOT_ROWS: usize = 5_000;
		const NDV: u64 = 100;

		let mut catalog = join_catalog_inner();
		catalog
			.seed_optimizer_facts(
				&crate::catalog::TableRef::new("customers"),
				TableOptimizerFacts {
					byte_upper_bound: None,
					row_upper_bound: Some(RowCount::new(BASE_ROWS as u64)),
					column_ndv: BTreeMap::from([(ColumnOrdinal(1), DistinctCountFact::Exact(NDV))]),
					column_range: Default::default(),
					column_range_distributions: Default::default(),
					column_heavy_hitters,
					column_group_top_k: Default::default(),
					column_group_ndv: Default::default(),
				},
			)
			.expect("seed skewed customers optimizer facts");

		SkewedJoinFixture {
			catalog: Arc::new(catalog),
			storage: skewed_join_storage(BASE_ROWS, HOT_ROWS, NDV),
			actual_hot_rows: HOT_ROWS,
		}
	}

	fn skewed_join_storage(base_rows: usize, hot_rows: usize, ndv: u64) -> Arc<dyn StorageScan> {
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
				Arc::new(Int64Array::from_iter_values(0..base_rows as i64)),
				Arc::new(Int64Array::from_iter_values(0..base_rows as i64)),
			],
		)
		.expect("skewed orders");
		let mut regions = Vec::with_capacity(base_rows);
		for row in 0..base_rows {
			if row < hot_rows {
				regions.push(Some("hot".to_string()));
			} else {
				let tail = (row - hot_rows) % (ndv as usize - 1);
				regions.push(Some(format!("tail_{tail}")));
			}
		}
		let customers = RecordBatch::try_new(
			customers_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0..base_rows as i64)),
				Arc::new(StringArray::from(regions)),
			],
		)
		.expect("skewed customers");
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([(TableId(1), orders), (TableId(2), customers)]),
		})
	}

	const GROUP_TOP_K_BASE_ROWS: usize = 200;
	const GROUP_TOP_K_HOT_ROWS: usize = 100;
	const GROUP_TOP_K_NDV: u64 = 101;

	struct MultiKeyGroupTopKFixture {
		catalog: Arc<dyn Catalog>,
		storage: Arc<dyn StorageScan>,
		actual_rows: usize,
	}

	fn multi_key_join_fixture_with_produced_storage_group_top_k_fact() -> MultiKeyGroupTopKFixture {
		let order_items_descriptor = named_descriptor(
			10,
			"order_items",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("order_id", DataType::Int64, false),
				Field::new("line_no", DataType::Int64, false),
				Field::new("shard_id", DataType::Int64, false),
			]),
		);
		let line_prices_descriptor = named_descriptor(
			11,
			"line_prices",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("order_id", DataType::Int64, false),
				Field::new("line_no", DataType::Int64, false),
				Field::new("shard_id", DataType::Int64, false),
				Field::new("region_id", DataType::Int64, false),
			]),
		);
		let regions_descriptor = named_descriptor(
			12,
			"regions",
			Schema::new(vec![Field::new("id", DataType::Int64, false)]),
		);

		let order_items_facts = multi_key_group_top_k_descriptor_facts(&order_items_descriptor);
		let line_prices_facts = multi_key_group_top_k_descriptor_facts(&line_prices_descriptor);
		let catalog = WendelonCatalog::from_descriptors_with_optimizer_facts([
			(TableId(10), order_items_descriptor, Some(order_items_facts)),
			(TableId(11), line_prices_descriptor, Some(line_prices_facts)),
			(TableId(12), regions_descriptor, None),
		])
		.expect("descriptor registry with produced group top-k facts");

		MultiKeyGroupTopKFixture {
			catalog: Arc::new(catalog),
			storage: multi_key_group_top_k_storage(),
			actual_rows: GROUP_TOP_K_HOT_ROWS * GROUP_TOP_K_HOT_ROWS
				+ (GROUP_TOP_K_BASE_ROWS - GROUP_TOP_K_HOT_ROWS),
		}
	}

	fn multi_key_group_top_k_descriptor_facts(
		descriptor: &TableSchemaDescriptor,
	) -> crate::catalog::DescriptorOptimizerFacts {
		let storage_facts = wendelon_client::TableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			producer_epoch: 164,
			row_upper_bound: Some(GROUP_TOP_K_BASE_ROWS as u64),
			column_ndv: vec![
				wendelon_client::ColumnNdvFact {
					column_ordinal: 1,
					distinct: wendelon_client::NdvFact::Exact {
						count: GROUP_TOP_K_NDV,
					},
				},
				wendelon_client::ColumnNdvFact {
					column_ordinal: 2,
					distinct: wendelon_client::NdvFact::Exact {
						count: GROUP_TOP_K_NDV,
					},
				},
			],
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: vec![wendelon_client::ColumnGroupTopKFact {
				column_ordinals: vec![1, 2],
				arrow_type_tags: vec![
					wendelon_client::ARROW_STATS_TAG_INT64,
					wendelon_client::ARROW_STATS_TAG_INT64,
				],
				entries: vec![wendelon_client::ColumnGroupTopKEntry {
					value_keys: vec![
						wendelon_client::encode_signed_optimizer_fact_scalar(7, 8).unwrap(),
						wendelon_client::encode_signed_optimizer_fact_scalar(2, 8).unwrap(),
					],
					frequency: GROUP_TOP_K_HOT_ROWS as u64,
				}],
			}],
			column_group_ndv: Default::default(),
		};
		let descriptor_facts =
			crate::catalog::descriptor_optimizer_facts_from_storage(storage_facts, descriptor)
				.expect("storage optimizer facts map through descriptor sync");
		assert!(
			!descriptor_facts.column_group_top_k.is_empty(),
			"storage-mapped group top-k fact should be non-empty"
		);
		descriptor_facts
	}

	fn multi_key_group_top_k_storage() -> Arc<dyn StorageScan> {
		let order_items_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("order_id", DataType::Int64, false),
			Field::new("line_no", DataType::Int64, false),
			Field::new("shard_id", DataType::Int64, false),
		]));
		let line_prices_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("order_id", DataType::Int64, false),
			Field::new("line_no", DataType::Int64, false),
			Field::new("shard_id", DataType::Int64, false),
			Field::new("region_id", DataType::Int64, false),
		]));
		let regions_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

		let (order_ids, line_nos) = multi_key_group_top_k_keys();
		let order_items = RecordBatch::try_new(
			order_items_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(
					0..GROUP_TOP_K_BASE_ROWS as i64,
				)),
				Arc::new(Int64Array::from(order_ids.clone())),
				Arc::new(Int64Array::from(line_nos.clone())),
				Arc::new(Int64Array::from_iter_values(
					0..GROUP_TOP_K_BASE_ROWS as i64,
				)),
			],
		)
		.expect("group top-k order_items");
		let line_prices = RecordBatch::try_new(
			line_prices_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(
					0..GROUP_TOP_K_BASE_ROWS as i64,
				)),
				Arc::new(Int64Array::from(order_ids)),
				Arc::new(Int64Array::from(line_nos)),
				Arc::new(Int64Array::from_iter_values(
					0..GROUP_TOP_K_BASE_ROWS as i64,
				)),
				Arc::new(Int64Array::from(vec![1_i64; GROUP_TOP_K_BASE_ROWS])),
			],
		)
		.expect("group top-k line_prices");
		let regions = RecordBatch::try_new(
			regions_schema,
			vec![Arc::new(Int64Array::from(vec![1_i64]))],
		)
		.expect("group top-k regions");
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([
				(TableId(10), order_items),
				(TableId(11), line_prices),
				(TableId(12), regions),
			]),
		})
	}

	fn multi_key_group_top_k_keys() -> (Vec<i64>, Vec<i64>) {
		let mut order_ids = Vec::with_capacity(GROUP_TOP_K_BASE_ROWS);
		let mut line_nos = Vec::with_capacity(GROUP_TOP_K_BASE_ROWS);
		for row in 0..GROUP_TOP_K_BASE_ROWS {
			if row < GROUP_TOP_K_HOT_ROWS {
				order_ids.push(7);
				line_nos.push(2);
			} else {
				let value = 1_000 + row as i64;
				order_ids.push(value);
				line_nos.push(value);
			}
		}
		(order_ids, line_nos)
	}

	const GROUP_NDV_BASE_ROWS: usize = 400;
	const GROUP_NDV_GROUPS: usize = 4;
	const GROUP_NDV_ROWS_PER_GROUP: usize = GROUP_NDV_BASE_ROWS / GROUP_NDV_GROUPS;

	fn multi_key_join_fixture_with_produced_storage_group_ndv_fact() -> MultiKeyGroupTopKFixture {
		let order_items_descriptor = named_descriptor(
			10,
			"order_items",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("order_id", DataType::Int64, false),
				Field::new("line_no", DataType::Int64, false),
				Field::new("shard_id", DataType::Int64, false),
			]),
		);
		let line_prices_descriptor = named_descriptor(
			11,
			"line_prices",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("order_id", DataType::Int64, false),
				Field::new("line_no", DataType::Int64, false),
				Field::new("shard_id", DataType::Int64, false),
				Field::new("region_id", DataType::Int64, false),
			]),
		);
		let regions_descriptor = named_descriptor(
			12,
			"regions",
			Schema::new(vec![Field::new("id", DataType::Int64, false)]),
		);

		let order_items_facts = multi_key_group_ndv_descriptor_facts(&order_items_descriptor);
		let line_prices_facts = multi_key_group_ndv_descriptor_facts(&line_prices_descriptor);
		let catalog = WendelonCatalog::from_descriptors_with_optimizer_facts([
			(TableId(10), order_items_descriptor, Some(order_items_facts)),
			(TableId(11), line_prices_descriptor, Some(line_prices_facts)),
			(TableId(12), regions_descriptor, None),
		])
		.expect("descriptor registry with produced group NDV facts");

		MultiKeyGroupTopKFixture {
			catalog: Arc::new(catalog),
			storage: multi_key_group_ndv_storage(),
			actual_rows: GROUP_NDV_GROUPS * GROUP_NDV_ROWS_PER_GROUP * GROUP_NDV_ROWS_PER_GROUP,
		}
	}

	fn multi_key_group_ndv_descriptor_facts(
		descriptor: &TableSchemaDescriptor,
	) -> crate::catalog::DescriptorOptimizerFacts {
		let storage_facts = wendelon_client::TableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			producer_epoch: 165,
			row_upper_bound: Some(GROUP_NDV_BASE_ROWS as u64),
			column_ndv: vec![
				wendelon_client::ColumnNdvFact {
					column_ordinal: 1,
					distinct: wendelon_client::NdvFact::Exact {
						count: GROUP_NDV_GROUPS as u64,
					},
				},
				wendelon_client::ColumnNdvFact {
					column_ordinal: 2,
					distinct: wendelon_client::NdvFact::Exact {
						count: GROUP_NDV_GROUPS as u64,
					},
				},
			],
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: vec![wendelon_client::ColumnGroupNdvFact {
				column_ordinals: vec![1, 2],
				distinct: wendelon_client::NdvFact::Exact {
					count: GROUP_NDV_GROUPS as u64,
				},
			}],
		};
		let descriptor_facts =
			crate::catalog::descriptor_optimizer_facts_from_storage(storage_facts, descriptor)
				.expect("storage optimizer facts map through descriptor sync");
		assert!(
			!descriptor_facts.column_group_ndv.is_empty(),
			"storage-mapped group NDV fact should be non-empty"
		);
		descriptor_facts
	}

	fn multi_key_group_ndv_storage() -> Arc<dyn StorageScan> {
		let order_items_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("order_id", DataType::Int64, false),
			Field::new("line_no", DataType::Int64, false),
			Field::new("shard_id", DataType::Int64, false),
		]));
		let line_prices_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("order_id", DataType::Int64, false),
			Field::new("line_no", DataType::Int64, false),
			Field::new("shard_id", DataType::Int64, false),
			Field::new("region_id", DataType::Int64, false),
		]));
		let regions_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

		let (order_ids, line_nos) = multi_key_group_ndv_keys();
		let order_items = RecordBatch::try_new(
			order_items_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0..GROUP_NDV_BASE_ROWS as i64)),
				Arc::new(Int64Array::from(order_ids.clone())),
				Arc::new(Int64Array::from(line_nos.clone())),
				Arc::new(Int64Array::from_iter_values(0..GROUP_NDV_BASE_ROWS as i64)),
			],
		)
		.expect("group NDV order_items");
		let line_prices = RecordBatch::try_new(
			line_prices_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0..GROUP_NDV_BASE_ROWS as i64)),
				Arc::new(Int64Array::from(order_ids)),
				Arc::new(Int64Array::from(line_nos)),
				Arc::new(Int64Array::from_iter_values(0..GROUP_NDV_BASE_ROWS as i64)),
				Arc::new(Int64Array::from(vec![1_i64; GROUP_NDV_BASE_ROWS])),
			],
		)
		.expect("group NDV line_prices");
		let regions = RecordBatch::try_new(
			regions_schema,
			vec![Arc::new(Int64Array::from(vec![1_i64]))],
		)
		.expect("group NDV regions");
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([
				(TableId(10), order_items),
				(TableId(11), line_prices),
				(TableId(12), regions),
			]),
		})
	}

	fn multi_key_group_ndv_keys() -> (Vec<i64>, Vec<i64>) {
		let mut order_ids = Vec::with_capacity(GROUP_NDV_BASE_ROWS);
		let mut line_nos = Vec::with_capacity(GROUP_NDV_BASE_ROWS);
		for row in 0..GROUP_NDV_BASE_ROWS {
			let group = (row / GROUP_NDV_ROWS_PER_GROUP) as i64;
			order_ids.push(group);
			line_nos.push(group);
		}
		(order_ids, line_nos)
	}

	const GROUP_HOT_COLD_BASE_ROWS: usize = 1_000;
	const GROUP_HOT_COLD_HOT_ROWS: usize = 100;
	const GROUP_HOT_COLD_GROUPS: u64 = 100;

	fn multi_key_join_fixture_with_produced_storage_group_hot_cold_facts()
	-> MultiKeyGroupTopKFixture {
		let order_items_descriptor = named_descriptor(
			10,
			"order_items",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("order_id", DataType::Int64, false),
				Field::new("line_no", DataType::Int64, false),
				Field::new("shard_id", DataType::Int64, false),
			]),
		);
		let line_prices_descriptor = named_descriptor(
			11,
			"line_prices",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("order_id", DataType::Int64, false),
				Field::new("line_no", DataType::Int64, false),
				Field::new("shard_id", DataType::Int64, false),
				Field::new("region_id", DataType::Int64, false),
			]),
		);
		let regions_descriptor = named_descriptor(
			12,
			"regions",
			Schema::new(vec![Field::new("id", DataType::Int64, false)]),
		);

		let order_items_facts = multi_key_group_hot_cold_descriptor_facts(&order_items_descriptor);
		let line_prices_facts = multi_key_group_hot_cold_descriptor_facts(&line_prices_descriptor);
		let catalog = WendelonCatalog::from_descriptors_with_optimizer_facts([
			(TableId(10), order_items_descriptor, Some(order_items_facts)),
			(TableId(11), line_prices_descriptor, Some(line_prices_facts)),
			(TableId(12), regions_descriptor, None),
		])
		.expect("descriptor registry with produced group hot+cold facts");

		MultiKeyGroupTopKFixture {
			catalog: Arc::new(catalog),
			storage: multi_key_group_hot_cold_storage(),
			actual_rows: multi_key_group_hot_cold_actual_rows(),
		}
	}

	fn multi_key_group_hot_cold_descriptor_facts(
		descriptor: &TableSchemaDescriptor,
	) -> crate::catalog::DescriptorOptimizerFacts {
		let storage_facts = wendelon_client::TableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			producer_epoch: 166,
			row_upper_bound: Some(GROUP_HOT_COLD_BASE_ROWS as u64),
			column_ndv: vec![
				wendelon_client::ColumnNdvFact {
					column_ordinal: 1,
					distinct: wendelon_client::NdvFact::Exact {
						count: GROUP_HOT_COLD_GROUPS,
					},
				},
				wendelon_client::ColumnNdvFact {
					column_ordinal: 2,
					distinct: wendelon_client::NdvFact::Exact {
						count: GROUP_HOT_COLD_GROUPS,
					},
				},
			],
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: vec![wendelon_client::ColumnGroupTopKFact {
				column_ordinals: vec![1, 2],
				arrow_type_tags: vec![
					wendelon_client::ARROW_STATS_TAG_INT64,
					wendelon_client::ARROW_STATS_TAG_INT64,
				],
				entries: vec![wendelon_client::ColumnGroupTopKEntry {
					value_keys: vec![
						wendelon_client::encode_signed_optimizer_fact_scalar(7, 8).unwrap(),
						wendelon_client::encode_signed_optimizer_fact_scalar(2, 8).unwrap(),
					],
					frequency: GROUP_HOT_COLD_HOT_ROWS as u64,
				}],
			}],
			column_group_ndv: vec![wendelon_client::ColumnGroupNdvFact {
				column_ordinals: vec![1, 2],
				distinct: wendelon_client::NdvFact::Exact {
					count: GROUP_HOT_COLD_GROUPS,
				},
			}],
		};
		let descriptor_facts =
			crate::catalog::descriptor_optimizer_facts_from_storage(storage_facts, descriptor)
				.expect("storage optimizer facts map through descriptor sync");
		assert!(
			!descriptor_facts.column_group_top_k.is_empty(),
			"storage-mapped group top-k fact should be non-empty"
		);
		assert!(
			!descriptor_facts.column_group_ndv.is_empty(),
			"storage-mapped group NDV fact should be non-empty"
		);
		descriptor_facts
	}

	fn multi_key_group_hot_cold_storage() -> Arc<dyn StorageScan> {
		let order_items_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("order_id", DataType::Int64, false),
			Field::new("line_no", DataType::Int64, false),
			Field::new("shard_id", DataType::Int64, false),
		]));
		let line_prices_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("order_id", DataType::Int64, false),
			Field::new("line_no", DataType::Int64, false),
			Field::new("shard_id", DataType::Int64, false),
			Field::new("region_id", DataType::Int64, false),
		]));
		let regions_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

		let (order_ids, line_nos) = multi_key_group_hot_cold_keys();
		let order_items = RecordBatch::try_new(
			order_items_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(
					0..GROUP_HOT_COLD_BASE_ROWS as i64,
				)),
				Arc::new(Int64Array::from(order_ids.clone())),
				Arc::new(Int64Array::from(line_nos.clone())),
				Arc::new(Int64Array::from_iter_values(
					0..GROUP_HOT_COLD_BASE_ROWS as i64,
				)),
			],
		)
		.expect("group hot+cold order_items");
		let line_prices = RecordBatch::try_new(
			line_prices_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(
					0..GROUP_HOT_COLD_BASE_ROWS as i64,
				)),
				Arc::new(Int64Array::from(order_ids)),
				Arc::new(Int64Array::from(line_nos)),
				Arc::new(Int64Array::from_iter_values(
					0..GROUP_HOT_COLD_BASE_ROWS as i64,
				)),
				Arc::new(Int64Array::from(vec![1_i64; GROUP_HOT_COLD_BASE_ROWS])),
			],
		)
		.expect("group hot+cold line_prices");
		let regions = RecordBatch::try_new(
			regions_schema,
			vec![Arc::new(Int64Array::from(vec![1_i64]))],
		)
		.expect("group hot+cold regions");
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([
				(TableId(10), order_items),
				(TableId(11), line_prices),
				(TableId(12), regions),
			]),
		})
	}

	fn multi_key_group_hot_cold_keys() -> (Vec<i64>, Vec<i64>) {
		let mut order_ids = Vec::with_capacity(GROUP_HOT_COLD_BASE_ROWS);
		let mut line_nos = Vec::with_capacity(GROUP_HOT_COLD_BASE_ROWS);
		for row in 0..GROUP_HOT_COLD_BASE_ROWS {
			if row < GROUP_HOT_COLD_HOT_ROWS {
				order_ids.push(7);
				line_nos.push(2);
			} else {
				let group =
					((row - GROUP_HOT_COLD_HOT_ROWS) % (GROUP_HOT_COLD_GROUPS as usize - 1)) as i64;
				let value = 1_000 + group;
				order_ids.push(value);
				line_nos.push(value);
			}
		}
		(order_ids, line_nos)
	}

	fn multi_key_group_hot_cold_actual_rows() -> usize {
		let cold_rows = GROUP_HOT_COLD_BASE_ROWS - GROUP_HOT_COLD_HOT_ROWS;
		let cold_groups = GROUP_HOT_COLD_GROUPS as usize - 1;
		let base = cold_rows / cold_groups;
		let extra = cold_rows % cold_groups;
		let cold_matches = extra * (base + 1) * (base + 1) + (cold_groups - extra) * base * base;
		GROUP_HOT_COLD_HOT_ROWS * GROUP_HOT_COLD_HOT_ROWS + cold_matches
	}

	fn multi_key_group_ndv_parent_join_plan() -> LogicalPlan {
		multi_key_group_top_k_parent_join_plan()
	}

	fn multi_key_group_top_k_parent_join_plan() -> LogicalPlan {
		LogicalPlan::Join {
			left: Box::new(LogicalPlan::Join {
				left: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("order_items"),
					projection: None,
				}),
				right: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("line_prices"),
					projection: None,
				}),
				kind: JoinKind::Inner,
				on: vec![
					(
						Expr::qualified_column(
							crate::catalog::TableRef::new("order_items"),
							"order_id",
						),
						Expr::qualified_column(
							crate::catalog::TableRef::new("line_prices"),
							"order_id",
						),
					),
					(
						Expr::qualified_column(
							crate::catalog::TableRef::new("order_items"),
							"line_no",
						),
						Expr::qualified_column(
							crate::catalog::TableRef::new("line_prices"),
							"line_no",
						),
					),
				],
				strategy_intent: JoinStrategyIntent::Auto,
			}),
			right: Box::new(LogicalPlan::ScanNamed {
				table: crate::catalog::TableRef::new("regions"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(crate::catalog::TableRef::new("line_prices"), "region_id"),
				Expr::qualified_column(crate::catalog::TableRef::new("regions"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		}
	}

	fn multi_key_group_top_k_hash_config() -> PlannerConfig {
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(512 * 1024)),
			..PlannerConfig::default()
		}
	}

	fn multi_key_group_ndv_hash_config() -> PlannerConfig {
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(512 * 1024)),
			..PlannerConfig::default()
		}
	}

	fn multi_key_group_hot_cold_hash_config() -> PlannerConfig {
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(1024 * 1024)),
			..PlannerConfig::default()
		}
	}

	const RANGE_SKEW_BASE_ROWS: usize = 10_000;
	const RANGE_SKEW_CLUSTER_ROWS: usize = 9_000;
	const RANGE_SKEW_FILTER_LOW: i64 = 0;
	const RANGE_SKEW_FILTER_HIGH: i64 = 100;
	const RANGE_SKEW_ENVELOPE_HIGH: i64 = 10_000;
	const RANGE_SKEW_UNIFORM_ESTIMATE: u64 = 101;

	struct RangeSkewJoinFixture {
		catalog: Arc<dyn Catalog>,
		storage: Arc<dyn StorageScan>,
		actual_range_rows: usize,
	}

	fn range_skew_join_fixture() -> RangeSkewJoinFixture {
		RangeSkewJoinFixture {
			catalog: Arc::new(range_skew_join_catalog()),
			storage: range_skew_join_storage(),
			actual_range_rows: RANGE_SKEW_CLUSTER_ROWS,
		}
	}

	fn range_skew_join_fixture_with_range_distribution() -> RangeSkewJoinFixture {
		RangeSkewJoinFixture {
			catalog: Arc::new(range_skew_join_catalog_with_customers_optimizer_facts(
				range_skew_customers_optimizer_facts_with_range_distribution(),
			)),
			storage: range_skew_join_storage(),
			actual_range_rows: RANGE_SKEW_CLUSTER_ROWS,
		}
	}

	fn range_skew_join_fixture_with_produced_storage_range_distribution_fact()
	-> RangeSkewJoinFixture {
		let orders_descriptor = named_descriptor(
			1,
			"orders",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("customer_id", DataType::Int64, false),
			]),
		);
		let customers_descriptor = named_descriptor(
			2,
			"customers",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("amount", DataType::Int64, true),
			]),
		);
		let storage_facts = wendelon_client::TableOptimizerFacts {
			table_object_id: customers_descriptor.table_object_id,
			schema_generation: customers_descriptor.schema_generation,
			physical_descriptor_fingerprint: customers_descriptor.physical_descriptor_fingerprint,
			producer_epoch: 159,
			row_upper_bound: Some(RANGE_SKEW_BASE_ROWS as u64),
			column_ndv: Vec::new(),
			column_range: vec![wendelon_client::ColumnRangeFact {
				column_ordinal: 1,
				arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT64,
				null_count: 0,
				non_null_count: RANGE_SKEW_BASE_ROWS as u64,
				min: Some(
					wendelon_client::encode_signed_optimizer_fact_scalar(RANGE_SKEW_FILTER_LOW, 8)
						.unwrap(),
				),
				max: Some(
					wendelon_client::encode_signed_optimizer_fact_scalar(
						RANGE_SKEW_ENVELOPE_HIGH,
						8,
					)
					.unwrap(),
				),
			}],
			column_range_distributions: vec![range_skew_produced_storage_distribution_fact()],
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		};
		let descriptor_facts = crate::catalog::descriptor_optimizer_facts_from_storage(
			storage_facts,
			&customers_descriptor,
		)
		.expect("storage optimizer facts map through descriptor sync");
		assert!(
			!descriptor_facts.column_range_distributions.is_empty(),
			"storage-mapped range-distribution fact should be non-empty"
		);
		let catalog = WendelonCatalog::from_descriptors_with_optimizer_facts([
			(TableId(1), orders_descriptor, None),
			(TableId(2), customers_descriptor, Some(descriptor_facts)),
		])
		.expect("descriptor registry with produced range-distribution facts");

		RangeSkewJoinFixture {
			catalog: Arc::new(catalog),
			storage: range_skew_join_storage(),
			actual_range_rows: RANGE_SKEW_CLUSTER_ROWS,
		}
	}

	fn range_skew_join_catalog() -> MockCatalog {
		range_skew_join_catalog_with_customers_optimizer_facts(
			range_skew_customers_optimizer_facts(),
		)
	}

	fn range_skew_join_catalog_with_customers_optimizer_facts(
		customers_facts: TableOptimizerFacts,
	) -> MockCatalog {
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
						Field::new("amount", DataType::Int64, true),
					]),
				),
			)
			.unwrap();
		catalog
			.seed_optimizer_facts(&crate::catalog::TableRef::new("customers"), customers_facts)
			.expect("seed range-skew customers optimizer facts");
		catalog
	}

	fn range_skew_customers_optimizer_facts() -> TableOptimizerFacts {
		TableOptimizerFacts {
			byte_upper_bound: None,
			row_upper_bound: None,
			column_ndv: Default::default(),
			column_range: BTreeMap::from([(ColumnOrdinal(1), range_skew_amount_range_fact())]),
			column_range_distributions: Default::default(),
			column_heavy_hitters: Default::default(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		}
	}

	fn range_skew_customers_optimizer_facts_with_range_distribution() -> TableOptimizerFacts {
		TableOptimizerFacts {
			byte_upper_bound: None,
			row_upper_bound: None,
			column_ndv: Default::default(),
			column_range: BTreeMap::from([(ColumnOrdinal(1), range_skew_amount_range_fact())]),
			column_range_distributions: BTreeMap::from([(
				ColumnOrdinal(1),
				range_skew_amount_distribution_fact(),
			)]),
			column_heavy_hitters: Default::default(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		}
	}

	fn range_skew_amount_range_fact() -> RangeFact {
		RangeFact {
			arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT64,
			null_count: 0,
			non_null_count: RANGE_SKEW_BASE_ROWS as u64,
			min: Some(
				wendelon_client::encode_signed_optimizer_fact_scalar(RANGE_SKEW_FILTER_LOW, 8)
					.unwrap(),
			),
			max: Some(
				wendelon_client::encode_signed_optimizer_fact_scalar(RANGE_SKEW_ENVELOPE_HIGH, 8)
					.unwrap(),
			),
		}
	}

	fn range_skew_amount_distribution_fact() -> ColumnRangeDistributionFact {
		ColumnRangeDistributionFact {
			arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT64,
			non_null_count: RANGE_SKEW_BASE_ROWS as u64,
			rank_error_ppm: 17,
			buckets: vec![
				range_skew_distribution_bucket(
					RANGE_SKEW_FILTER_LOW,
					RANGE_SKEW_FILTER_HIGH,
					9_000,
				),
				range_skew_distribution_bucket(
					RANGE_SKEW_FILTER_HIGH + 1,
					RANGE_SKEW_ENVELOPE_HIGH,
					1_000,
				),
			],
		}
	}

	fn range_skew_produced_storage_distribution_fact()
	-> wendelon_client::ColumnRangeDistributionFact {
		let mut sketch = KllSketch::new(
			wendelon_client::ARROW_STATS_TAG_INT64,
			KllK::new(2048),
			0x5A17_1590,
		)
		.expect("range-skew KLL sketch");
		for row in 0..RANGE_SKEW_BASE_ROWS {
			sketch.insert(
				wendelon_client::encode_signed_optimizer_fact_scalar(range_skew_amount(row), 8)
					.unwrap(),
			);
		}
		let buckets = sketch.query_buckets(64).expect("range-skew buckets");
		assert!(!buckets.is_empty());
		wendelon_client::ColumnRangeDistributionFact {
			column_ordinal: 1,
			arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT64,
			non_null_count: sketch.total_item_count(),
			rank_error_ppm: sketch.rank_error_ppm(),
			buckets,
		}
	}

	fn range_skew_distribution_bucket(
		lower: i64,
		upper: i64,
		count: u64,
	) -> RangeDistributionBucket {
		RangeDistributionBucket {
			lower: wendelon_client::encode_signed_optimizer_fact_scalar(lower, 8).unwrap(),
			upper: wendelon_client::encode_signed_optimizer_fact_scalar(upper, 8).unwrap(),
			count,
		}
	}

	fn range_skew_join_storage() -> Arc<dyn StorageScan> {
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]));
		let customers_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int64, true),
		]));
		let orders = RecordBatch::try_new(
			orders_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0..RANGE_SKEW_BASE_ROWS as i64)),
				Arc::new(Int64Array::from_iter_values(0..RANGE_SKEW_BASE_ROWS as i64)),
			],
		)
		.expect("range-skew orders");
		let customers = RecordBatch::try_new(
			customers_schema,
			vec![
				Arc::new(Int64Array::from_iter_values(0..RANGE_SKEW_BASE_ROWS as i64)),
				Arc::new(Int64Array::from_iter_values(
					(0..RANGE_SKEW_BASE_ROWS).map(range_skew_amount),
				)),
			],
		)
		.expect("range-skew customers");
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([(TableId(1), orders), (TableId(2), customers)]),
		})
	}

	fn range_skew_amount(row: usize) -> i64 {
		if row < RANGE_SKEW_CLUSTER_ROWS {
			(row % ((RANGE_SKEW_FILTER_HIGH - RANGE_SKEW_FILTER_LOW + 1) as usize)) as i64
		} else if row == RANGE_SKEW_CLUSTER_ROWS {
			RANGE_SKEW_ENVELOPE_HIGH
		} else {
			RANGE_SKEW_FILTER_HIGH + 1 + (row - RANGE_SKEW_CLUSTER_ROWS - 1) as i64
		}
	}

	fn outer_join_counterexample_catalog() -> Arc<MockCatalog> {
		let mut catalog = MockCatalog::new();
		for (table_id, id_byte, table, fields) in [
			(
				TableId(1),
				11,
				"a",
				vec![
					Field::new("id", DataType::Int64, false),
					Field::new("k", DataType::Int64, false),
				],
			),
			(
				TableId(2),
				12,
				"b",
				vec![
					Field::new("id", DataType::Int64, false),
					Field::new("k", DataType::Int64, false),
					Field::new("j", DataType::Int64, false),
					Field::new("x", DataType::Int64, true),
				],
			),
			(
				TableId(3),
				13,
				"c",
				vec![
					Field::new("id", DataType::Int64, false),
					Field::new("j", DataType::Int64, false),
				],
			),
		] {
			catalog
				.register_descriptor(
					table_id,
					named_descriptor(id_byte, table, Schema::new(fields)),
				)
				.unwrap();
		}
		Arc::new(catalog)
	}

	fn outer_join_counterexample_storage() -> Arc<MultiTableStorageScan> {
		let a_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("k", DataType::Int64, false),
		]));
		let b_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("k", DataType::Int64, false),
			Field::new("j", DataType::Int64, false),
			Field::new("x", DataType::Int64, true),
		]));
		let c_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("j", DataType::Int64, false),
		]));
		let a = RecordBatch::try_new(
			a_schema,
			vec![
				Arc::new(Int64Array::from(vec![1])),
				Arc::new(Int64Array::from(vec![1])),
			],
		)
		.unwrap();
		let b = RecordBatch::try_new(
			b_schema,
			vec![
				Arc::new(Int64Array::from(vec![2])),
				Arc::new(Int64Array::from(vec![1])),
				Arc::new(Int64Array::from(vec![9])),
				Arc::new(Int64Array::from(vec![Some(2_i64)])),
			],
		)
		.unwrap();
		let c = RecordBatch::try_new(
			c_schema,
			vec![
				Arc::new(Int64Array::from(vec![100])),
				Arc::new(Int64Array::from(vec![9])),
			],
		)
		.unwrap();
		Arc::new(MultiTableStorageScan {
			tables: HashMap::from([(TableId(1), a), (TableId(2), b), (TableId(3), c)]),
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
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Direct,
			},
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
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		}
	}

	fn sql_hash_join_plan() -> LogicalPlan {
		crate::sql_planner::SqlPlanner::new()
			.plan("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
			.expect("SQL hash join plan")
	}

	fn sql_fact_dim_join_plan() -> LogicalPlan {
		crate::sql_planner::SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("SQL fact-dim join plan")
	}

	fn ndv_filtered_fact_dim_join_plan() -> LogicalPlan {
		LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: crate::catalog::TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("customers"),
					projection: None,
				}),
				predicate: Expr::Eq(
					Box::new(Expr::qualified_column(
						crate::catalog::TableRef::new("customers"),
						"region",
					)),
					Box::new(Expr::Literal(ScalarValue::Utf8(Some("west".to_string())))),
				),
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(crate::catalog::TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(crate::catalog::TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		}
	}

	fn skewed_hot_key_fact_dim_join_plan(strategy_intent: JoinStrategyIntent) -> LogicalPlan {
		LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: crate::catalog::TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("customers"),
					projection: None,
				}),
				predicate: Expr::Eq(
					Box::new(Expr::qualified_column(
						crate::catalog::TableRef::new("customers"),
						"region",
					)),
					Box::new(Expr::Literal(ScalarValue::Utf8(Some("hot".to_string())))),
				),
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(crate::catalog::TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(crate::catalog::TableRef::new("customers"), "id"),
			)],
			strategy_intent,
		}
	}

	fn range_skew_fact_dim_join_plan(strategy_intent: JoinStrategyIntent) -> LogicalPlan {
		LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: crate::catalog::TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(range_skew_filtered_customers_plan(
				range_skew_amount_between(RANGE_SKEW_FILTER_LOW, RANGE_SKEW_FILTER_HIGH),
			)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(crate::catalog::TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(crate::catalog::TableRef::new("customers"), "id"),
			)],
			strategy_intent,
		}
	}

	fn range_skew_filtered_customers_plan(predicate: Expr) -> LogicalPlan {
		LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: crate::catalog::TableRef::new("customers"),
				projection: None,
			}),
			predicate,
		}
	}

	fn range_skew_amount_between(low: i64, high: i64) -> Expr {
		Expr::And(
			Box::new(Expr::Gte(
				Box::new(range_skew_amount_column()),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(low)))),
			)),
			Box::new(Expr::Lte(
				Box::new(range_skew_amount_column()),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(high)))),
			)),
		)
	}

	fn range_skew_amount_column() -> Expr {
		Expr::qualified_column(crate::catalog::TableRef::new("customers"), "amount")
	}

	fn sql_fact_two_dim_join_plan() -> LogicalPlan {
		crate::sql_planner::SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
			)
			.expect("SQL fact-two-dim join plan")
	}

	fn filtered_fact_probe_dim_left_join_plan() -> LogicalPlan {
		LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: crate::catalog::TableRef::new("customers"),
				projection: None,
			}),
			right: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::ScanNamed {
					table: crate::catalog::TableRef::new("orders"),
					projection: None,
				}),
				predicate: Expr::Eq(
					Box::new(Expr::qualified_column(
						crate::catalog::TableRef::new("orders"),
						"customer_id",
					)),
					Box::new(Expr::Literal(ScalarValue::Int64(Some(2)))),
				),
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(crate::catalog::TableRef::new("customers"), "id"),
				Expr::qualified_column(crate::catalog::TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		}
	}

	fn sql_broadcast_join_plan() -> LogicalPlan {
		crate::sql_planner::SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("SQL broadcast join plan")
	}

	async fn rune_hash_join_plan() -> LogicalPlan {
		rune_join_plan(
			r#"
        pub fn main(ctx) {
            ctx.table("customers")
               .inner_join(
                   ctx.table("orders"),
                   qcol("customers", "id").eq(qcol("orders", "customer_id"))
               )
        }
    "#,
		)
		.await
	}

	async fn rune_broadcast_join_plan() -> LogicalPlan {
		rune_join_plan(
			r#"
        pub fn main(ctx) {
            ctx.table("customers")
               .broadcast_join(
                   ctx.table("orders"),
                   qcol("customers", "id").eq(qcol("orders", "customer_id"))
               )
        }
    "#,
		)
		.await
	}

	async fn rune_join_plan(script: &str) -> LogicalPlan {
		let config = RuneSandboxConfig::v1_default();
		let engine = RuneEngine::with_config(config).expect("Rune engine");
		engine
			.execute_script(script, &config)
			.await
			.expect("Rune-authored join plan")
	}

	async fn run_distributed_join_query(plan: LogicalPlan, connections: usize) -> Vec<RecordBatch> {
		run_distributed_join_query_with_budget(plan, connections, QueryBudget::test_default())
			.await
			.unwrap()
	}

	async fn run_local_join_query_with_catalog_config(
		plan: LogicalPlan,
		catalog: Arc<dyn Catalog>,
		planner_config: PlannerConfig,
	) -> Vec<RecordBatch> {
		let bound = bind(&plan, catalog.as_ref(), &planner_config)
			.await
			.expect("bind local join query");
		Executor::new(join_storage())
			.execute_bound(&bound, None)
			.await
			.expect("execute local join query")
	}

	async fn run_distributed_join_query_with_catalog_config(
		plan: LogicalPlan,
		connections: usize,
		catalog: Arc<dyn Catalog>,
		planner_config: PlannerConfig,
	) -> Vec<RecordBatch> {
		run_distributed_join_query_with_catalog_config_and_budget(
			plan,
			connections,
			catalog,
			planner_config,
			QueryBudget::test_default(),
		)
		.await
		.unwrap()
	}

	async fn run_distributed_join_query_with_budget(
		plan: LogicalPlan,
		connections: usize,
		query_budget: QueryBudget,
	) -> Result<Vec<RecordBatch>, QueryError> {
		run_distributed_join_query_with_catalog_config_and_budget(
			plan,
			connections,
			join_catalog(),
			PlannerConfig::default(),
			query_budget,
		)
		.await
	}

	async fn run_distributed_join_query_with_catalog_config_and_budget(
		plan: LogicalPlan,
		_connections: usize,
		catalog: Arc<dyn Catalog>,
		planner_config: PlannerConfig,
		query_budget: QueryBudget,
	) -> Result<Vec<RecordBatch>, QueryError> {
		let memory = test_memory();
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
		let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
		let server_handle = tokio::spawn(async move {
			let mut handles = Vec::new();
			loop {
				tokio::select! {
					accepted = listener.accept() => {
						let (server_stream, _) = accepted.unwrap();
						let worker = worker.clone();
						handles.push(tokio::spawn(async move {
							worker.handle_connection(server_stream).await.unwrap();
						}));
					}
					_ = shutdown_rx.changed() => {
						break;
					}
				}
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
			planner_config,
			query_budget,
		);

		let results = coordinator.execute_query(&plan).await;
		shutdown_tx.send(true).unwrap();
		server_handle.await.unwrap();
		results
	}

	struct ReplaySignalRun {
		results: Vec<RecordBatch>,
		replayed: bool,
	}

	async fn run_distributed_join_query_with_catalog_config_budget_storage_and_replay_signal(
		plan: LogicalPlan,
		catalog: Arc<dyn Catalog>,
		planner_config: PlannerConfig,
		query_budget: QueryBudget,
		storage: Arc<dyn StorageScan>,
	) -> Result<ReplaySignalRun, QueryError> {
		let memory = test_memory();
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
		let endpoint = WorkerEndpoint::new(listener.local_addr()?.to_string());
		let worker_id = WorkerId::from_u128(101);
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			storage,
			memory.clone(),
			Arc::new(InMemoryPartitionStore::new()),
			worker_id,
			endpoint.clone(),
		));
		let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
		let server_handle = tokio::spawn(async move {
			let mut handles = Vec::new();
			loop {
				tokio::select! {
					accepted = listener.accept() => {
						let (server_stream, _) = accepted.unwrap();
						let worker = worker.clone();
						handles.push(tokio::spawn(async move {
							worker.handle_connection(server_stream).await.unwrap();
						}));
					}
					_ = shutdown_rx.changed() => {
						break;
					}
				}
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

		let result = run_distributed_attempts_with_replay_signal(
			plan,
			catalog,
			registry,
			memory,
			planner_config,
			query_budget,
		)
		.await;
		shutdown_tx.send(true).unwrap();
		server_handle.await.unwrap();
		result
	}

	async fn run_distributed_attempts_with_replay_signal(
		plan: LogicalPlan,
		catalog: Arc<dyn Catalog>,
		registry: Arc<StaticWorkerRegistry>,
		memory: Arc<MemoryManager>,
		mut planner_config: PlannerConfig,
		query_budget: QueryBudget,
	) -> Result<ReplaySignalRun, QueryError> {
		let query_id = QueryId::new();
		let mut replayed = false;
		let mut fragment_id_namespace = 0_u32;
		loop {
			let bound = bind(&plan, catalog.as_ref(), &planner_config).await?;
			let stages = fragment_plan(
				bound,
				FragmentPlanConfig::new(query_id, TaskBudget::from_query_budget(query_budget))
					.with_id_namespace(fragment_id_namespace),
			)
			.map_err(|error| QueryError::Remote {
				message: format!("{error:?}"),
			})?;
			let mut scheduler = DistributedScheduler::new(registry.clone())
				.with_max_in_flight_tasks(query_budget.max_in_flight_tasks);
			let dispatcher = NetworkTaskDispatcher::new(memory.clone(), TcpTransport::new());
			match scheduler.dispatch_loop(&stages, &dispatcher).await {
				Ok(run) => {
					return Ok(ReplaySignalRun {
						results: run.final_results,
						replayed,
					});
				}
				Err(error) if !replayed => {
					if let Some(force_hash_join_ids) =
						crate::coordinator::broadcast_cap_replay_trigger(&error, &stages)
					{
						planner_config.force_hash_join_ids = force_hash_join_ids;
						fragment_id_namespace = fragment_id_namespace
							.checked_add(1)
							.expect("test fragment namespace overflow");
						replayed = true;
					} else {
						return Err(scheduler_error_for_test(error));
					}
				}
				Err(error) => return Err(scheduler_error_for_test(error)),
			}
		}
	}

	fn scheduler_error_for_test(error: SchedulerError) -> QueryError {
		match error {
			SchedulerError::TaskFailed { kind, .. } => {
				QueryError::Runtime(RuntimeError::TaskFailure { kind })
			}
			other => QueryError::Remote {
				message: format!("{other:?}"),
			},
		}
	}

	fn assert_fact_dim_join_matches_hash(
		hash_results: &[RecordBatch],
		broadcast_results: &[RecordBatch],
	) {
		assert!(!hash_results.is_empty());
		assert!(!broadcast_results.is_empty());
		assert_eq!(
			field_names(hash_results[0].schema().as_ref()),
			vec!["id", "customer_id", "id", "region"]
		);
		assert_eq!(
			field_names(broadcast_results[0].schema().as_ref()),
			vec!["id", "customer_id", "id", "region"]
		);
		assert_eq!(broadcast_results[0].schema(), hash_results[0].schema());
		assert_eq!(
			canonical_arrow_ipc_digest(broadcast_results, SortPolicy::FullRowLex)
				.expect("right-build broadcast digest"),
			canonical_arrow_ipc_digest(hash_results, SortPolicy::FullRowLex).expect("hash digest")
		);
	}

	async fn assert_range_disjoint_child_join_binds_parent_as_estimated_broadcast(
		plan: &LogicalPlan,
		catalog: &dyn Catalog,
	) {
		let (_bound, trace) = bind_with_trace(plan, catalog, &PlannerConfig::default())
			.await
			.expect("bind range-disjoint join with trace");
		assert_eq!(trace.join_decisions.len(), 2);
		match trace.join_decisions[1].reason {
			JoinStrategyDecisionReason::BroadcastEstimated {
				build_side,
				estimated_byte_size,
				threshold,
				..
			} => {
				assert_eq!(build_side, BroadcastBuildSide::Left);
				assert!(estimated_byte_size < threshold);
			}
			other => panic!(
				"expected parent estimated broadcast for range-disjoint child join, got {other:?}"
			),
		}
	}

	async fn assert_skewed_hot_key_binds_as_estimated_broadcast(
		plan: &LogicalPlan,
		catalog: &dyn Catalog,
	) {
		let (_bound, trace) = bind_with_trace(plan, catalog, &PlannerConfig::default())
			.await
			.expect("bind skewed hot-key join with trace");
		assert_eq!(trace.join_decisions.len(), 1);
		match trace.join_decisions[0].reason {
			JoinStrategyDecisionReason::BroadcastEstimated {
				build_side,
				estimated_byte_size,
				threshold,
				..
			} => {
				assert_eq!(build_side, BroadcastBuildSide::Right);
				assert!(estimated_byte_size < threshold);
			}
			other => panic!("expected estimated broadcast for skewed hot key, got {other:?}"),
		}
	}

	async fn assert_skewed_hot_key_binds_without_estimated_broadcast(
		plan: &LogicalPlan,
		catalog: &dyn Catalog,
	) {
		let (_bound, trace) = bind_with_trace(plan, catalog, &PlannerConfig::default())
			.await
			.expect("bind heavy-hitter skewed hot-key join with trace");
		assert_eq!(trace.join_decisions.len(), 1);
		assert_eq!(
			trace.join_decisions[0].strategy,
			JoinStrategy::HashPartitioned
		);
		assert!(
			!matches!(
				trace.join_decisions[0].reason,
				JoinStrategyDecisionReason::BroadcastEstimated { .. }
			),
			"heavy-hitter hot key should not bind as BroadcastEstimated: {:?}",
			trace.join_decisions[0].reason
		);
	}

	async fn assert_range_skew_binds_as_estimated_broadcast(
		plan: &LogicalPlan,
		catalog: &dyn Catalog,
	) {
		let (_bound, trace) = bind_with_trace(plan, catalog, &PlannerConfig::default())
			.await
			.expect("bind range-skew join with trace");
		assert_eq!(trace.join_decisions.len(), 1);
		match trace.join_decisions[0].reason {
			JoinStrategyDecisionReason::BroadcastEstimated {
				build_side,
				estimated_byte_size,
				threshold,
				..
			} => {
				assert_eq!(build_side, BroadcastBuildSide::Right);
				assert!(estimated_byte_size < threshold);
			}
			other => panic!("expected estimated broadcast for range skew, got {other:?}"),
		}
	}

	async fn assert_range_distribution_binds_without_estimated_broadcast(
		plan: &LogicalPlan,
		catalog: &dyn Catalog,
	) {
		let (_bound, trace) = bind_with_trace(plan, catalog, &range_distribution_hash_config())
			.await
			.expect("bind range-distribution join with trace");
		assert_eq!(trace.join_decisions.len(), 1);
		assert_eq!(
			trace.join_decisions[0].strategy,
			JoinStrategy::HashPartitioned
		);
		assert!(
			!matches!(
				trace.join_decisions[0].reason,
				JoinStrategyDecisionReason::BroadcastEstimated { .. }
			),
			"range distribution should not bind as BroadcastEstimated: {:?}",
			trace.join_decisions[0].reason
		);
	}

	async fn assert_multi_key_group_top_k_binds_without_estimated_broadcast(
		plan: &LogicalPlan,
		catalog: &dyn Catalog,
	) {
		let (_bound, trace) = bind_with_trace(plan, catalog, &multi_key_group_top_k_hash_config())
			.await
			.expect("bind multi-key group top-k join with trace");
		assert_eq!(trace.join_decisions.len(), 2);
		assert_eq!(
			trace.join_decisions[1].strategy,
			JoinStrategy::HashPartitioned
		);
		assert!(
			!matches!(
				trace.join_decisions[1].reason,
				JoinStrategyDecisionReason::BroadcastEstimated { .. }
			),
			"group top-k child estimate should prevent parent estimated broadcast: {:?}",
			trace.join_decisions[1].reason
		);
	}

	async fn assert_multi_key_group_ndv_binds_without_estimated_broadcast(
		plan: &LogicalPlan,
		catalog: &dyn Catalog,
	) {
		let (_bound, trace) = bind_with_trace(plan, catalog, &multi_key_group_ndv_hash_config())
			.await
			.expect("bind multi-key group NDV join with trace");
		assert_eq!(trace.join_decisions.len(), 2);
		assert_eq!(
			trace.join_decisions[1].strategy,
			JoinStrategy::HashPartitioned
		);
		assert!(
			!matches!(
				trace.join_decisions[1].reason,
				JoinStrategyDecisionReason::BroadcastEstimated { .. }
			),
			"group NDV child estimate should prevent parent estimated broadcast: {:?}",
			trace.join_decisions[1].reason
		);
	}

	async fn assert_multi_key_group_hot_cold_binds_without_estimated_broadcast(
		plan: &LogicalPlan,
		catalog: &dyn Catalog,
	) {
		let (_bound, trace) =
			bind_with_trace(plan, catalog, &multi_key_group_hot_cold_hash_config())
				.await
				.expect("bind multi-key group hot+cold join with trace");
		assert_eq!(trace.join_decisions.len(), 2);
		assert_eq!(
			trace.join_decisions[1].strategy,
			JoinStrategy::HashPartitioned
		);
		assert!(
			!matches!(
				trace.join_decisions[1].reason,
				JoinStrategyDecisionReason::BroadcastEstimated { .. }
			),
			"group hot+cold child estimate should prevent parent estimated broadcast: {:?}",
			trace.join_decisions[1].reason
		);
	}

	fn range_distribution_hash_config() -> PlannerConfig {
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(32 * 1024)),
			..PlannerConfig::default()
		}
	}

	fn assert_skewed_join_matches_hash(
		hash_results: &[RecordBatch],
		broadcast_results: &[RecordBatch],
	) {
		assert!(!hash_results.is_empty());
		assert!(!broadcast_results.is_empty());
		assert_eq!(
			field_names(hash_results[0].schema().as_ref()),
			vec!["id", "customer_id", "id", "region"]
		);
		assert_eq!(
			field_names(broadcast_results[0].schema().as_ref()),
			vec!["id", "customer_id", "id", "region"]
		);
		assert_eq!(
			canonical_arrow_ipc_digest(broadcast_results, SortPolicy::FullRowLex)
				.expect("skew replay digest"),
			canonical_arrow_ipc_digest(hash_results, SortPolicy::FullRowLex)
				.expect("skew hash digest")
		);
	}

	fn assert_range_skew_join_matches_hash(
		hash_results: &[RecordBatch],
		broadcast_results: &[RecordBatch],
	) {
		assert!(!hash_results.is_empty());
		assert!(!broadcast_results.is_empty());
		assert_eq!(
			field_names(hash_results[0].schema().as_ref()),
			vec!["id", "customer_id", "id", "amount"]
		);
		assert_eq!(
			field_names(broadcast_results[0].schema().as_ref()),
			vec!["id", "customer_id", "id", "amount"]
		);
		assert_eq!(
			canonical_arrow_ipc_digest(broadcast_results, SortPolicy::FullRowLex)
				.expect("range skew replay digest"),
			canonical_arrow_ipc_digest(hash_results, SortPolicy::FullRowLex)
				.expect("range skew hash digest")
		);
	}

	fn assert_multi_key_group_top_k_join_matches_hash(
		hash_results: &[RecordBatch],
		broadcast_results: &[RecordBatch],
	) {
		assert!(!hash_results.is_empty());
		assert!(!broadcast_results.is_empty());
		assert_eq!(
			field_names(hash_results[0].schema().as_ref()),
			vec![
				"id",
				"order_id",
				"line_no",
				"shard_id",
				"id",
				"order_id",
				"line_no",
				"shard_id",
				"region_id",
				"id",
			]
		);
		assert_eq!(
			field_names(broadcast_results[0].schema().as_ref()),
			vec![
				"id",
				"order_id",
				"line_no",
				"shard_id",
				"id",
				"order_id",
				"line_no",
				"shard_id",
				"region_id",
				"id",
			]
		);
		assert_eq!(
			canonical_arrow_ipc_digest(broadcast_results, SortPolicy::FullRowLex)
				.expect("group top-k replay digest"),
			canonical_arrow_ipc_digest(hash_results, SortPolicy::FullRowLex)
				.expect("group top-k hash digest")
		);
	}

	fn assert_broadcast_cap_error(error: QueryError) {
		assert!(matches!(
			error,
			QueryError::Runtime(crate::error::RuntimeError::TaskFailure {
				kind: TaskFailureKind::BudgetExhausted {
					dimension: BudgetDimension::BroadcastReplication,
					..
				},
				..
			})
		));
	}

	fn assert_broadcast_pin_source(plan: &LogicalPlan, expected: JoinStrategyPinSource) {
		match plan {
			LogicalPlan::Join {
				strategy_intent:
					JoinStrategyIntent::Pinned {
						strategy: JoinStrategy::Broadcast,
						source,
					},
				..
			} => assert_eq!(*source, expected),
			other => panic!("expected pinned broadcast join, got {other:?}"),
		}
	}

	fn stage_ids(stages: &[StagePlan]) -> HashSet<crate::distributed::identity::StageId> {
		stages.iter().map(|stage| stage.stage_id).collect()
	}

	fn exchange_ids(stages: &[StagePlan]) -> HashSet<ExchangeId> {
		let mut ids = HashSet::new();
		for task in stages.iter().flat_map(|stage| &stage.tasks) {
			for input in &task.spec.inputs {
				if let TaskInputBinding::ShuffleConsumer { exchange_id, .. } = input {
					ids.insert(*exchange_id);
				}
			}
			if let TaskOutputBinding::ShuffleProducer { exchange_id, .. } = &task.spec.output {
				ids.insert(*exchange_id);
			}
		}
		ids
	}

	fn field_names(schema: &Schema) -> Vec<&str> {
		schema
			.fields()
			.iter()
			.map(|field| field.name().as_str())
			.collect()
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
			if let Some(pbc) = request.pbc_predicate {
				batch = wendelon::schema::pbc::execute(&pbc, &batch).map_err(|error| {
					StorageError::InvalidPbcProgram {
						message: format!("{error:?}"),
					}
				})?;
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

		let file_ops = Arc::new(BlockingFileOps);
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
