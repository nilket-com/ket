use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::coordinator::DistributedQueryCoordinator;
use toron::distributed::budget::{ByteCount, DurationMillis, QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{AttemptId, LeaseEpoch, QueryId, WorkerId};
use toron::distributed::scheduler::{DistributedScheduler, SchedulerRun};
use toron::distributed::task::{PartitionCount, TaskInputBinding, TaskOutputBinding};
use toron::distributed::worker_registry::{
	ActiveTaskCount, PlacementHints, SimdLaneCount, UnixMillis, WorkerCapabilities, WorkerEndpoint,
	WorkerEntry, WorkerLease, WorkerRegistry, WorkerResourceState, WorkerStatus,
};
use toron::ir::{
	AggregateFunction, BroadcastBuildSide, ExecutionPlan, Expr, JoinKind, JoinStrategy,
	JoinStrategyIntent, JoinStrategyPinSource, LogicalPlan, SortExpr, TableId,
};
use toron::server::transport::TcpTransport;
use toron::server::{NetworkTaskDispatcher, memory::MemoryManager};
use toron::sql_planner::SqlPlanner;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

struct FixtureWorkerProcess {
	child: Child,
	endpoint: WorkerEndpoint,
	worker_id: WorkerId,
}

impl FixtureWorkerProcess {
	async fn spawn(worker_id: WorkerId) -> Self {
		let mut child = Command::new(env!("CARGO_BIN_EXE_toron_fixture_worker"))
			.arg("--worker-u128")
			.arg(worker_id.get().as_u128().to_string())
			.stdout(Stdio::piped())
			.stderr(Stdio::inherit())
			.spawn()
			.expect("spawn fixture worker");
		let stdout = child.stdout.take().expect("fixture worker stdout");
		let mut line = String::new();
		let mut reader = BufReader::new(stdout);
		reader
			.read_line(&mut line)
			.await
			.expect("read fixture worker readiness");
		let endpoint = line
			.trim()
			.strip_prefix("READY ")
			.expect("fixture worker readiness marker");

		Self {
			child,
			endpoint: WorkerEndpoint::new(endpoint),
			worker_id,
		}
	}

	async fn shutdown(&mut self) {
		let _ = self.child.kill().await;
	}
}

impl Drop for FixtureWorkerProcess {
	fn drop(&mut self) {
		let _ = self.child.start_kill();
	}
}

struct RoundRobinRegistry {
	workers: RwLock<HashMap<WorkerId, WorkerEntry>>,
	order: Vec<WorkerId>,
	next: AtomicUsize,
	selected: Mutex<Vec<WorkerId>>,
}

impl RoundRobinRegistry {
	fn new(workers: impl IntoIterator<Item = WorkerEntry>, start_offset: usize) -> Self {
		let workers = workers.into_iter().collect::<Vec<_>>();
		let order = workers
			.iter()
			.map(|worker| worker.worker_id)
			.collect::<Vec<_>>();
		Self {
			workers: RwLock::new(
				workers
					.into_iter()
					.map(|worker| (worker.worker_id, worker))
					.collect(),
			),
			order,
			next: AtomicUsize::new(start_offset),
			selected: Mutex::new(Vec::new()),
		}
	}

	fn selected_workers(&self) -> Vec<WorkerId> {
		self.selected
			.lock()
			.expect("selected workers poisoned")
			.clone()
	}
}

impl WorkerRegistry for RoundRobinRegistry {
	fn list(&self) -> Vec<WorkerEntry> {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.values()
			.cloned()
			.collect()
	}

	fn resolve(&self, worker_id: WorkerId) -> Option<WorkerEntry> {
		self.workers
			.read()
			.expect("worker registry poisoned")
			.get(&worker_id)
			.cloned()
	}

	fn select_for_attempt(&self, _hints: &PlacementHints) -> Option<WorkerId> {
		let workers = self.workers.read().expect("worker registry poisoned");
		for _attempt in 0..self.order.len() {
			let offset = self.next.fetch_add(1, Ordering::SeqCst);
			let worker_id = self.order[offset % self.order.len()];
			let Some(worker) = workers.get(&worker_id) else {
				continue;
			};
			if worker.status == WorkerStatus::Live {
				self.selected
					.lock()
					.expect("selected workers poisoned")
					.push(worker_id);
				return Some(worker_id);
			}
		}
		None
	}

	fn mark_lost(&self, worker_id: WorkerId) {
		if let Some(worker) = self
			.workers
			.write()
			.expect("worker registry poisoned")
			.get_mut(&worker_id)
		{
			worker.status = WorkerStatus::Lost;
			worker.lease.epoch = worker.lease.epoch.next();
		}
	}

	fn heartbeat(&self, worker_id: WorkerId, lease: WorkerLease) {
		if let Some(worker) = self
			.workers
			.write()
			.expect("worker registry poisoned")
			.get_mut(&worker_id)
		{
			worker.lease = lease;
			worker.status = WorkerStatus::Live;
		}
	}

	fn expire_heartbeats(
		&self,
		_now: UnixMillis,
		_suspect_after: DurationMillis,
		_lost_after: DurationMillis,
	) -> Vec<WorkerId> {
		Vec::new()
	}
}

#[derive(Debug)]
struct ProcessRun {
	digest: toron::distributed::task::ArrowIpcDigest,
	total_count: i64,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct OrderedGroupProcessRun {
	digest: toron::distributed::task::ArrowIpcDigest,
	row_count: usize,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct JoinProcessRun {
	digest: toron::distributed::task::ArrowIpcDigest,
	row_count: usize,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct JoinAggregateProcessRun {
	rows: HashMap<Option<String>, (i64, Option<i64>)>,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct JoinDistinctAggregateProcessRun {
	rows: HashMap<Option<String>, i64>,
	selected_workers: Vec<WorkerId>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_process_distributed_query_digest_is_canonical() {
	let first = run_process_query(100, 0).await;
	let second = run_process_query(200, 1).await;

	assert_eq!(first.total_count, 16);
	assert_eq!(second.total_count, 16);
	assert_eq!(first.digest, second.digest);
	assert_eq!(
		first
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
	assert_eq!(
		second
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_process_kill_replays_attempt_and_matches_baseline_digest() {
	let baseline = run_process_query(300, 0).await;
	let killed = run_producer_kill_query(400).await;

	assert_eq!(killed.total_count, 16);
	assert_eq!(killed.digest, baseline.digest);
	assert_eq!(killed.selected_workers[0], WorkerId::from_u128(400));
	assert_eq!(
		killed
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_join_producer_process_kill_replays_attempt_and_matches_baseline_digest() {
	let baseline = run_hash_join_process_query(500, 0).await;
	let left_killed = run_hash_join_producer_kill_query(600, 0).await;
	let right_killed = run_hash_join_producer_kill_query(700, 1).await;

	assert_eq!(baseline.row_count, 3);
	assert_eq!(left_killed.row_count, 3);
	assert_eq!(right_killed.row_count, 3);
	assert_eq!(left_killed.digest, baseline.digest);
	assert_eq!(right_killed.digest, baseline.digest);
	assert_eq!(
		left_killed
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
	assert_eq!(
		right_killed
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_partitioned_promoted_join_kinds_match_single_partition_digest() {
	for (index, kind) in [
		JoinKind::LeftOuter,
		JoinKind::FullOuter,
		JoinKind::LeftSemi,
		JoinKind::LeftAnti,
	]
	.into_iter()
	.enumerate()
	{
		let base = 1_200 + (index as u128 * 10);
		let baseline = run_hash_join_kind_process_query(base, kind, 1, 0).await;
		let distributed = run_hash_join_kind_process_query(base + 4, kind, 2, 1).await;

		assert_eq!(distributed.row_count, baseline.row_count, "{kind:?}");
		assert_eq!(distributed.digest, baseline.digest, "{kind:?}");
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn correlated_exists_decorrelation_matches_explicit_left_semi_join_digest() {
	let decorrelated = run_hash_join_logical_process_query(
		1_300,
		0,
		SqlPlanner::new()
			.plan(
				"SELECT * FROM orders \
				 WHERE EXISTS (SELECT 1 FROM customers \
				 WHERE customers.id = orders.customer_id)",
			)
			.expect("plan correlated exists"),
	)
	.await;
	let explicit = run_hash_join_kind_process_query(1_304, JoinKind::LeftSemi, 2, 1).await;

	assert_eq!(decorrelated.row_count, explicit.row_count);
	assert_eq!(decorrelated.digest, explicit.digest);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_distinct_join_dedups_across_partitions() {
	let run = run_hash_join_logical_process_query(
		1_320,
		0,
		SqlPlanner::new()
			.plan(
				"SELECT DISTINCT 1 AS one \
				 FROM orders JOIN customers ON orders.customer_id = customers.id",
			)
			.expect("plan distinct over distributed join"),
	)
	.await;

	assert_eq!(
		run.row_count, 1,
		"post-join DISTINCT must collapse rows across hash partitions"
	);
	assert_eq!(
		unique_selected_worker_count(&run.selected_workers),
		2,
		"fixture must exercise more than one worker"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_join_real_aggregate_merges_across_partitions() {
	let run = run_group_by_join_real_aggregate_process_query(1_340, 0).await;

	// "west" spans customers 2 (partition 0) and 3 (partition 3): its three matched orders
	// (101,102 from cust 2; 103 from cust 3) are produced by two different join workers, so a
	// correct group-key shuffle co-locates them at one consumer -> COUNT=3, SUM(orders.id)=306.
	// A broken cross-producer merge would emit two partial "west" rows (collapsing wrong here).
	assert_eq!(
		run.rows,
		HashMap::from([(Some("west".to_string()), (3, Some(306)))])
	);
	assert_eq!(
		unique_selected_worker_count(&run.selected_workers),
		2,
		"fixture must exercise more than one worker"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_distinct_over_join_merges_across_partitions() {
	let run = run_count_distinct_join_process_query(1_360, 0).await;

	// The west group has orders for distinct customers 2 and 3. Customer 2 has duplicate
	// orders, and customers 2/3 are produced by different join workers, so this proves
	// cross-producer dedup before Count(customer_id).
	assert_eq!(run.rows, HashMap::from([(Some("west".to_string()), 2)]));
	assert_eq!(
		unique_selected_worker_count(&run.selected_workers),
		2,
		"fixture must exercise more than one worker"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grouped_aggregate_sql_matches_hand_built_aggregate_over_processes() {
	let sql = SqlPlanner::new()
		.plan("SELECT val, COUNT(*) FROM fixture GROUP BY val")
		.expect("plan group by sql");
	let grouped_sql = run_test_logical_process_query(1_400, 0, sql).await;
	let hand_built = run_process_query(1_404, 1).await;

	assert_eq!(grouped_sql.total_count, 16);
	assert_eq!(grouped_sql.total_count, hand_built.total_count);
	assert_eq!(grouped_sql.digest, hand_built.digest);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_ordered_top_k_process_query_preserves_inner_barrier() {
	let mut workers = [
		FixtureWorkerProcess::spawn(WorkerId::from_u128(1_410)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(1_411)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(workers.iter().map(worker_entry), 0));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry,
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = logical_ordered_top_k(
		logical_ordered_top_k(fixture_scan_plan(), true, Some(10)),
		false,
		Some(3),
	);

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("nested ordered top-k query");
	for worker in &mut workers {
		worker.shutdown().await;
	}

	assert_eq!(results.len(), 1);
	let values = results[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("val column");
	assert_eq!(
		(0..values.len())
			.map(|row| values.value(row))
			.collect::<Vec<_>>(),
		vec![6, 7, 8]
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_query_stream_yields_bare_limit_first_rows() {
	let mut worker = FixtureWorkerProcess::spawn(WorkerId::from_u128(1_420)).await;
	let registry = Arc::new(RoundRobinRegistry::new([worker_entry(&worker)], 0));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry,
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan("SELECT val FROM fixture LIMIT 3")
		.expect("plan bare limit");

	let mut stream = coordinator
		.execute_query_stream(&logical)
		.await
		.expect("open result stream");
	let first = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
		.await
		.expect("first result batch")
		.expect("stream item")
		.expect("first batch ok");
	drop(stream);
	worker.shutdown().await;

	assert_eq!(first.num_rows(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collecting_execute_query_remains_behavior_compatible_for_bare_limit() {
	let mut worker = FixtureWorkerProcess::spawn(WorkerId::from_u128(1_421)).await;
	let registry = Arc::new(RoundRobinRegistry::new([worker_entry(&worker)], 0));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry,
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan("SELECT val FROM fixture LIMIT 3")
		.expect("plan bare limit");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("collecting query");
	worker.shutdown().await;

	assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[tokio::test]
async fn distributed_group_by_has_single_group_keyed_exchange() {
	let logical = SqlPlanner::new()
		.plan("SELECT val, COUNT(*) FROM fixture GROUP BY val")
		.expect("plan group by sql");
	let bound = bind(&logical, test_catalog().as_ref(), &PlannerConfig::default())
		.await
		.expect("bind group by sql");
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment group by sql");

	assert_eq!(stages.len(), 2);
	assert_eq!(stages[0].tasks.len(), 1);
	assert_eq!(stages[1].tasks.len(), 4);
	assert!(matches!(
		&stages[0].tasks[0].spec.output,
		TaskOutputBinding::ShuffleProducer {
			partition_count,
			partition_key,
			..
		} if *partition_count == PartitionCount::new(4) && partition_key.root_count() == 1
	));
	for task in &stages[1].tasks {
		assert!(matches!(
			task.spec.inputs[0],
			TaskInputBinding::ShuffleConsumer { .. }
		));
		assert!(matches!(task.spec.output, TaskOutputBinding::FinalResult));
		assert!(matches!(task.spec.plan, ExecutionPlan::Aggregate { .. }));
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_grouped_aggregate_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_ordered_grouped_aggregate_process_query(1_500, 2, 0).await;
	let four_workers = run_ordered_grouped_aggregate_process_query(1_600, 4, 1).await;

	assert_eq!(two_workers.row_count, 5);
	assert_eq!(four_workers.row_count, 5);
	assert_eq!(two_workers.digest, four_workers.digest);
	assert_eq!(
		two_workers
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
	assert_eq!(
		four_workers
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		4
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_grouped_min_max_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_ordered_grouped_min_max_process_query(2_500, 2, 0).await;
	let four_workers = run_ordered_grouped_min_max_process_query(2_600, 4, 1).await;

	assert_eq!(two_workers.row_count, 5);
	assert_eq!(four_workers.row_count, 5);
	assert_eq!(two_workers.digest, four_workers.digest);
	assert_eq!(
		unique_selected_worker_count(&two_workers.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&four_workers.selected_workers),
		4
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_grouped_float_min_max_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_ordered_grouped_float_min_max_process_query(2_650, 2, 0).await;
	let four_workers = run_ordered_grouped_float_min_max_process_query(2_660, 4, 1).await;

	assert_eq!(two_workers.row_count, 5);
	assert_eq!(four_workers.row_count, 5);
	assert_eq!(two_workers.digest, four_workers.digest);
	assert_eq!(
		unique_selected_worker_count(&two_workers.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&four_workers.selected_workers),
		4
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_grouped_avg_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_ordered_grouped_avg_process_query(2_700, 2, 0).await;
	let four_workers = run_ordered_grouped_avg_process_query(2_800, 4, 1).await;

	assert_eq!(two_workers.row_count, 5);
	assert_eq!(four_workers.row_count, 5);
	assert_eq!(two_workers.digest, four_workers.digest);
	assert_eq!(
		unique_selected_worker_count(&two_workers.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&four_workers.selected_workers),
		4
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn having_grouped_aggregate_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_having_grouped_aggregate_process_query(1_700, 2, 0).await;
	let four_workers = run_having_grouped_aggregate_process_query(1_800, 4, 1).await;

	assert_eq!(two_workers.row_count, 10);
	assert_eq!(four_workers.row_count, 10);
	assert_eq!(two_workers.digest, four_workers.digest);
	assert_eq!(
		two_workers
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
	assert_eq!(
		four_workers
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		4
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn having_ordered_grouped_aggregate_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_ordered_having_grouped_aggregate_process_query(1_900, 2, 0).await;
	let four_workers = run_ordered_having_grouped_aggregate_process_query(2_000, 4, 1).await;

	assert_eq!(two_workers.row_count, 5);
	assert_eq!(four_workers.row_count, 5);
	assert_eq!(two_workers.digest, four_workers.digest);
	assert_eq!(
		two_workers
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
	assert_eq!(
		four_workers
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		4
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adr023_post_aggregate_projection_composition_is_stable_across_worker_counts() {
	let two_workers = run_adr023_composition_process_query(2_900, 2, 0).await;
	let four_workers = run_adr023_composition_process_query(3_000, 4, 1).await;

	assert_eq!(two_workers.row_count, 5);
	assert_eq!(four_workers.row_count, 5);
	assert_eq!(two_workers.digest, four_workers.digest);
	assert_eq!(
		unique_selected_worker_count(&two_workers.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&four_workers.selected_workers),
		4
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_aggregate_ordered_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_ordered_scalar_aggregate_process_query(2_100, 2, 0).await;
	let four_workers = run_ordered_scalar_aggregate_process_query(2_200, 4, 1).await;

	assert_eq!(two_workers.row_count, 1);
	assert_eq!(four_workers.row_count, 1);
	assert_eq!(two_workers.digest, four_workers.digest);
	// Scalar ordered aggregate has only producer + consumer task slots; a larger pool
	// must not imply grouped-style fan-out.
	assert_eq!(
		unique_selected_worker_count(&two_workers.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&four_workers.selected_workers),
		2
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_aggregate_having_ordered_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_ordered_having_scalar_aggregate_process_query(2_300, 2, 0).await;
	let four_workers = run_ordered_having_scalar_aggregate_process_query(2_400, 4, 1).await;

	assert_eq!(two_workers.row_count, 1);
	assert_eq!(four_workers.row_count, 1);
	assert_eq!(two_workers.digest, four_workers.digest);
	// Scalar ordered aggregate has only producer + consumer task slots; a larger pool
	// must not imply grouped-style fan-out.
	assert_eq!(
		unique_selected_worker_count(&two_workers.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&four_workers.selected_workers),
		2
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_join_build_process_kill_replays_attempt_and_matches_baseline_digest() {
	let baseline = run_broadcast_join_process_query(800).await;
	let killed = run_broadcast_join_build_kill_query(900).await;

	assert_eq!(baseline.row_count, 3);
	assert_eq!(killed.row_count, 3);
	assert_eq!(killed.digest, baseline.digest);
	assert_eq!(
		killed
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		2
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_join_worker_process_loss_retries_join_task_and_matches_baseline_digest() {
	let baseline = run_hash_join_single_partition_process_query(1000).await;
	let killed = run_hash_join_worker_loss_query(1100).await;

	assert_eq!(baseline.row_count, 3);
	assert_eq!(killed.row_count, 3);
	assert_eq!(killed.digest, baseline.digest);
	assert_eq!(
		killed
			.selected_workers
			.iter()
			.copied()
			.collect::<HashSet<_>>()
			.len(),
		3
	);
}

async fn run_process_query(worker_id_base: u128, start_offset: usize) -> ProcessRun {
	run_test_logical_process_query(
		worker_id_base,
		start_offset,
		group_count_after_exchange_plan(),
	)
	.await
}

async fn run_test_logical_process_query(
	worker_id_base: u128,
	start_offset: usize,
	plan: LogicalPlan,
) -> ProcessRun {
	let mut workers = [
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);

	let results = coordinator.execute_query(&plan).await.expect("query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::FullRowLex).expect("digest");
	let total_count = sum_counts(&results);
	for worker in &mut workers {
		worker.shutdown().await;
	}

	ProcessRun {
		digest,
		total_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_ordered_grouped_aggregate_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan("SELECT val, SUM(val) FROM fixture GROUP BY val ORDER BY SUM(val) DESC LIMIT 5")
		.expect("plan ordered grouped aggregate");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("ordered grouped aggregate query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_ordered_grouped_min_max_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT val, MIN(val), MAX(val) FROM fixture \
			 GROUP BY val ORDER BY MAX(val) DESC LIMIT 5",
		)
		.expect("plan ordered grouped MIN/MAX aggregate");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("ordered grouped MIN/MAX aggregate query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_ordered_grouped_float_min_max_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT val, MIN(float_val), MAX(float_val) FROM fixture \
			 GROUP BY val ORDER BY MAX(float_val) DESC LIMIT 5",
		)
		.expect("plan ordered grouped Float64 MIN/MAX aggregate");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("ordered grouped Float64 MIN/MAX aggregate query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_ordered_grouped_avg_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT val, AVG(val) FROM fixture \
			 GROUP BY val ORDER BY AVG(val) DESC LIMIT 5",
		)
		.expect("plan ordered grouped AVG aggregate");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("ordered grouped AVG aggregate query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_having_grouped_aggregate_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan("SELECT val, SUM(val) FROM fixture GROUP BY val HAVING SUM(val) > 5")
		.expect("plan grouped aggregate HAVING");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("grouped aggregate HAVING query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::FullRowLex).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_ordered_having_grouped_aggregate_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT val, SUM(val) FROM fixture GROUP BY val \
			 HAVING SUM(val) > 3 ORDER BY SUM(val) DESC LIMIT 5 OFFSET 1",
		)
		.expect("plan ordered grouped aggregate HAVING");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("ordered grouped aggregate HAVING query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_adr023_composition_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT SUM(val) AS total, val AS k FROM fixture \
			 GROUP BY val HAVING COUNT(*) > 0 ORDER BY total DESC LIMIT 5",
		)
		.expect("plan ADR 023 post-aggregate projection composition");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("ADR 023 post-aggregate projection composition query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_ordered_scalar_aggregate_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan("SELECT SUM(val) FROM fixture ORDER BY SUM(val) DESC LIMIT 1")
		.expect("plan ordered scalar aggregate");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("ordered scalar aggregate query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_ordered_having_scalar_aggregate_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
) -> OrderedGroupProcessRun {
	let mut workers = Vec::new();
	for index in 0..worker_count {
		workers.push(
			FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + index as u128)).await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		test_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT SUM(val) FROM fixture \
			 HAVING SUM(val) > 0 ORDER BY SUM(val) DESC LIMIT 1",
		)
		.expect("plan ordered scalar aggregate HAVING");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("ordered scalar aggregate HAVING query");
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("digest");
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	OrderedGroupProcessRun {
		digest,
		row_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn run_hash_join_process_query(worker_id_base: u128, start_offset: usize) -> JoinProcessRun {
	run_hash_join_kind_process_query(worker_id_base, JoinKind::Inner, 2, start_offset).await
}

async fn run_hash_join_kind_process_query(
	worker_id_base: u128,
	kind: JoinKind,
	partition_count: u32,
	start_offset: usize,
) -> JoinProcessRun {
	let mut workers = [
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		join_process_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);

	let results = coordinator
		.execute_query(&hash_join_plan_with_partitions_and_kind(
			partition_count,
			kind,
		))
		.await
		.expect("hash join query");
	let run = join_process_run(results, registry.selected_workers());
	for worker in &mut workers {
		worker.shutdown().await;
	}
	run
}

async fn run_hash_join_logical_process_query(
	worker_id_base: u128,
	start_offset: usize,
	plan: LogicalPlan,
) -> JoinProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		join_process_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);

	let results = coordinator
		.execute_query(&plan)
		.await
		.expect("hash join query");
	let run = join_process_run(results, registry.selected_workers());
	for worker in &mut workers {
		worker.shutdown().await;
	}
	run
}

async fn run_group_by_join_real_aggregate_process_query(
	worker_id_base: u128,
	start_offset: usize,
) -> JoinAggregateProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		join_process_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT customers.region, COUNT(*), SUM(orders.id) \
			 FROM orders JOIN customers ON orders.customer_id = customers.id \
			 GROUP BY customers.region",
		)
		.expect("plan group-by over distributed join");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("group-by join aggregate query");
	let rows = grouped_join_rows(&results);
	let selected_workers = registry.selected_workers();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	JoinAggregateProcessRun {
		rows,
		selected_workers,
	}
}

async fn run_count_distinct_join_process_query(
	worker_id_base: u128,
	start_offset: usize,
) -> JoinDistinctAggregateProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		join_process_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new()
		.plan(
			"SELECT customers.region, COUNT(DISTINCT orders.customer_id) \
			 FROM orders JOIN customers ON orders.customer_id = customers.id \
			 GROUP BY customers.region",
		)
		.expect("plan count-distinct over distributed join");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("count-distinct join aggregate query");
	let rows = grouped_join_count_rows(&results);
	let selected_workers = registry.selected_workers();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	JoinDistinctAggregateProcessRun {
		rows,
		selected_workers,
	}
}

async fn run_hash_join_producer_kill_query(
	worker_id_base: u128,
	killed_stage_index: usize,
) -> JoinProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(workers.iter().map(worker_entry), 0));
	let memory = Arc::new(MemoryManager::new(64, 1024 * 1024));
	let bound = bind(
		&hash_join_plan(),
		join_process_catalog().as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind hash join");
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment hash join");
	let mut scheduler = DistributedScheduler::new(registry.clone());
	let mut dispatcher = NetworkTaskDispatcher::new(memory, TcpTransport::new());

	scheduler
		.dispatch_loop(&stages[..=killed_stage_index], &mut dispatcher)
		.await
		.expect("producer stages");

	let selected = registry.selected_workers();
	let killed_worker_id = selected[killed_stage_index];
	let killed_worker = workers
		.iter_mut()
		.find(|worker| worker.worker_id == killed_worker_id)
		.expect("killed worker");
	killed_worker.shutdown().await;
	let invalidated = scheduler.mark_worker_lost(killed_worker_id);
	assert_eq!(invalidated.producers.len(), 1);
	assert_eq!(
		invalidated.producers[0].invalidated_attempt,
		AttemptId::first()
	);
	assert_eq!(
		invalidated.producers[0].replacement_attempt,
		AttemptId::new(1)
	);

	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("replacement dispatch");
	let result = join_process_run(run.final_results, registry.selected_workers());
	for worker in &mut workers {
		if worker.worker_id != killed_worker_id {
			worker.shutdown().await;
		}
	}
	result
}

async fn run_broadcast_join_process_query(worker_id_base: u128) -> JoinProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(workers.iter().map(worker_entry), 0));
	let stages = broadcast_join_stages().await;
	let mut scheduler = DistributedScheduler::new(registry.clone());
	let mut dispatcher = NetworkTaskDispatcher::new(
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		TcpTransport::new(),
	);

	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("broadcast join");
	let result = join_process_run(run.final_results, registry.selected_workers());
	for worker in &mut workers {
		worker.shutdown().await;
	}
	result
}

async fn run_broadcast_join_build_kill_query(worker_id_base: u128) -> JoinProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(workers.iter().map(worker_entry), 0));
	let stages = broadcast_join_stages().await;
	let mut scheduler = DistributedScheduler::new(registry.clone());
	let mut dispatcher = NetworkTaskDispatcher::new(
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		TcpTransport::new(),
	);

	scheduler
		.dispatch_loop(&stages[..1], &mut dispatcher)
		.await
		.expect("broadcast build stage");
	let killed_worker_id = registry.selected_workers()[0];
	let killed_worker = workers
		.iter_mut()
		.find(|worker| worker.worker_id == killed_worker_id)
		.expect("killed worker");
	killed_worker.shutdown().await;
	let invalidated = scheduler.mark_worker_lost(killed_worker_id);
	assert_eq!(invalidated.producers.len(), 1);
	assert_eq!(
		invalidated.producers[0].invalidated_attempt,
		AttemptId::first()
	);
	assert_eq!(
		invalidated.producers[0].replacement_attempt,
		AttemptId::new(1)
	);

	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("broadcast replacement dispatch");
	let result = join_process_run(run.final_results, registry.selected_workers());
	for worker in &mut workers {
		if worker.worker_id != killed_worker_id {
			worker.shutdown().await;
		}
	}
	result
}

async fn run_hash_join_single_partition_process_query(worker_id_base: u128) -> JoinProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 2)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(workers.iter().map(worker_entry), 0));
	let stages = hash_join_stages(hash_join_plan_with_partitions(1)).await;
	let mut scheduler = DistributedScheduler::new(registry.clone());
	let mut dispatcher = NetworkTaskDispatcher::new(
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		TcpTransport::new(),
	);

	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("single-partition hash join");
	let result = join_process_run(run.final_results, registry.selected_workers());
	for worker in &mut workers {
		worker.shutdown().await;
	}
	result
}

async fn run_hash_join_worker_loss_query(worker_id_base: u128) -> JoinProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 2)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(workers.iter().map(worker_entry), 0));
	let stages = hash_join_stages(hash_join_plan_with_partitions(1)).await;
	let mut scheduler = DistributedScheduler::new(registry.clone());
	let mut dispatcher = NetworkTaskDispatcher::new(
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		TcpTransport::new(),
	);

	scheduler
		.dispatch_loop(&stages[..2], &mut dispatcher)
		.await
		.expect("hash join producers");
	let first_join_run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("first join dispatch");
	assert_eq!(
		first_join_run
			.final_results
			.iter()
			.map(RecordBatch::num_rows)
			.sum::<usize>(),
		3
	);
	let join_worker_id = registry
		.selected_workers()
		.last()
		.copied()
		.expect("join worker selected");
	let join_worker = workers
		.iter_mut()
		.find(|worker| worker.worker_id == join_worker_id)
		.expect("join worker");
	join_worker.shutdown().await;
	let invalidated = scheduler.mark_worker_lost(join_worker_id);
	assert_eq!(invalidated.producers.len(), 1);
	assert_eq!(
		invalidated.producers[0].invalidated_attempt,
		AttemptId::first()
	);
	assert_eq!(
		invalidated.producers[0].replacement_attempt,
		AttemptId::new(1)
	);

	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("replacement join dispatch");
	let result = join_process_run(run.final_results, registry.selected_workers());
	for worker in &mut workers {
		if worker.worker_id != join_worker_id {
			worker.shutdown().await;
		}
	}
	result
}

async fn run_producer_kill_query(worker_id_base: u128) -> ProcessRun {
	let mut workers = [
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(workers.iter().map(worker_entry), 0));
	let memory = Arc::new(MemoryManager::new(64, 1024 * 1024));
	let bound = bind(
		&group_count_after_exchange_plan(),
		test_catalog().as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind");
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment");
	let mut scheduler = DistributedScheduler::new(registry.clone());
	let mut dispatcher = NetworkTaskDispatcher::new(memory, TcpTransport::new());

	scheduler
		.dispatch_loop(&stages[..1], &mut dispatcher)
		.await
		.expect("producer stage");
	workers[0].shutdown().await;
	let invalidated = scheduler.mark_worker_lost(workers[0].worker_id);
	assert_eq!(invalidated.producers.len(), 1);
	assert_eq!(
		invalidated.producers[0].invalidated_attempt,
		AttemptId::first()
	);
	assert_eq!(
		invalidated.producers[0].replacement_attempt,
		AttemptId::new(1)
	);

	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("replacement dispatch");
	let digest = run_digest(&run);
	let total_count = sum_counts(&run.final_results);
	for worker in workers.iter_mut().skip(1) {
		worker.shutdown().await;
	}

	ProcessRun {
		digest,
		total_count,
		selected_workers: registry.selected_workers(),
	}
}

async fn broadcast_join_stages() -> Vec<toron::distributed::scheduler::StagePlan> {
	let bound = bind(
		&broadcast_join_plan(),
		join_process_catalog().as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind broadcast join");
	fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		)
		.with_worker_count(2),
	)
	.expect("fragment broadcast join")
}

async fn hash_join_stages(plan: LogicalPlan) -> Vec<toron::distributed::scheduler::StagePlan> {
	let bound = bind(
		&plan,
		join_process_catalog().as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind hash join");
	fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		)
		.with_worker_count(2),
	)
	.expect("fragment hash join")
}

fn hash_join_plan() -> LogicalPlan {
	hash_join_plan_with_partitions(2)
}

fn hash_join_plan_with_partitions(partition_count: u32) -> LogicalPlan {
	hash_join_plan_with_partitions_and_kind(partition_count, JoinKind::Inner)
}

fn hash_join_plan_with_partitions_and_kind(partition_count: u32, kind: JoinKind) -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::Exchange {
			partition_count,
			key: vec![Expr::qualified_column(
				TableRef::new("orders"),
				"customer_id",
			)],
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
		}),
		right: Box::new(LogicalPlan::Exchange {
			partition_count,
			key: vec![Expr::qualified_column(TableRef::new("customers"), "id")],
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
		}),
		kind,
		on: vec![(
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		)],
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Direct,
			build_side: None,
		},
	}
}

fn broadcast_join_plan() -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::Exchange {
			partition_count: 1,
			key: vec![Expr::qualified_column(TableRef::new("customers"), "id")],
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
		}),
		right: Box::new(LogicalPlan::ScanNamed {
			table: TableRef::new("orders"),
			projection: None,
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::Direct,
			build_side: Some(BroadcastBuildSide::Left),
		},
	}
}

fn group_count_after_exchange_plan() -> LogicalPlan {
	LogicalPlan::Aggregate {
		group_expr: vec![Expr::column("val")],
		aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("val")])],
		input: Box::new(LogicalPlan::Exchange {
			partition_count: 2,
			key: vec![Expr::column("val")],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		}),
	}
}

fn fixture_scan_plan() -> LogicalPlan {
	LogicalPlan::Scan {
		table: TableId(1),
		projection: None,
	}
}

fn logical_ordered_top_k(
	input: LogicalPlan,
	descending: bool,
	limit: Option<usize>,
) -> LogicalPlan {
	LogicalPlan::Sort {
		input: Box::new(LogicalPlan::Exchange {
			partition_count: 1,
			key: Vec::new(),
			input: Box::new(input),
		}),
		order_by: vec![SortExpr {
			expr: Expr::column("val"),
			descending,
			nulls_first: false,
		}],
		limit,
		offset: 0,
	}
}

fn run_digest(run: &SchedulerRun) -> toron::distributed::task::ArrowIpcDigest {
	canonical_arrow_ipc_digest(&run.final_results, SortPolicy::FullRowLex).expect("digest")
}

fn join_process_run(results: Vec<RecordBatch>, selected_workers: Vec<WorkerId>) -> JoinProcessRun {
	let row_count = results.iter().map(RecordBatch::num_rows).sum();
	let digest = canonical_arrow_ipc_digest(&results, SortPolicy::FullRowLex).expect("digest");
	JoinProcessRun {
		digest,
		row_count,
		selected_workers,
	}
}

fn sum_counts(results: &[arrow::record_batch::RecordBatch]) -> i64 {
	results
		.iter()
		.map(|batch| {
			let counts = batch
				.column(1)
				.as_any()
				.downcast_ref::<Int64Array>()
				.expect("count column");
			(0..counts.len()).map(|row| counts.value(row)).sum::<i64>()
		})
		.sum()
}

fn grouped_join_rows(results: &[RecordBatch]) -> HashMap<Option<String>, (i64, Option<i64>)> {
	let mut rows = HashMap::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count column");
		let sums = batch
			.column(2)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			let sum = if sums.is_null(row) {
				None
			} else {
				Some(sums.value(row))
			};
			rows.insert(region, (counts.value(row), sum));
		}
	}
	rows
}

fn grouped_join_count_rows(results: &[RecordBatch]) -> HashMap<Option<String>, i64> {
	let mut rows = HashMap::new();
	for batch in results {
		let regions = batch
			.column(0)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		let counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count column");
		for row in 0..batch.num_rows() {
			let region = if regions.is_null(row) {
				None
			} else {
				Some(regions.value(row).to_string())
			};
			rows.insert(region, counts.value(row));
		}
	}
	rows
}

fn unique_selected_worker_count(workers: &[WorkerId]) -> usize {
	workers.iter().copied().collect::<HashSet<_>>().len()
}

fn worker_entry(worker: &FixtureWorkerProcess) -> WorkerEntry {
	WorkerEntry {
		worker_id: worker.worker_id,
		endpoint: worker.endpoint.clone(),
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
	}
}

fn test_catalog() -> Arc<MockCatalog> {
	let schema = Schema::new(vec![
		Field::new("val", DataType::Int32, false),
		Field::new("float_val", DataType::Float64, true),
	]);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), test_descriptor(&schema))
		.expect("register descriptor");
	Arc::new(catalog)
}

fn join_process_catalog() -> Arc<MockCatalog> {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(
			TableId(2),
			named_descriptor(
				2,
				"orders",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
				]),
			),
		)
		.expect("register orders");
	catalog
		.register_descriptor(
			TableId(3),
			named_descriptor(
				3,
				"customers",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("region", DataType::Utf8, true),
				]),
			),
		)
		.expect("register customers");
	Arc::new(catalog)
}

fn test_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([1; 16]),
		1,
		"default",
		"fixture",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("val")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn named_descriptor(id_byte: u8, table: &str, schema: Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"default",
		table,
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}
