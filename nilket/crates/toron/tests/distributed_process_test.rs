use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
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
use toron::catalog::{MockCatalog, TableOptimizerFacts, TableRef};
use toron::coordinator::DistributedQueryCoordinator;
use toron::distributed::budget::{ByteCount, DurationMillis, QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{AttemptId, LeaseEpoch, QueryId, WorkerId};
use toron::distributed::scheduler::{DistributedScheduler, SchedulerRun};
use toron::distributed::task::{PartitionCount, TaskInputBinding, TaskOutputBinding};
use toron::distributed::worker_registry::{
	ActiveTaskCount, PlacementHints, SimdLaneCount, UnixMillis, WorkerCapabilities, WorkerEndpoint,
	WorkerEntry, WorkerLease, WorkerRegistry, WorkerReservation, WorkerResourceState, WorkerStatus,
};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	AggregateFunction, BroadcastBuildSide, ExecutionPlan, Expr, FilterDisposition, JoinKind,
	JoinStrategy, JoinStrategyIntent, JoinStrategyPinSource, LogicalPlan, SessionId, SortExpr,
	TableId,
};
use toron::server::transport::TcpTransport;
use toron::server::{NetworkTaskDispatcher, memory::MemoryManager};
use toron::sql_planner::SqlPlanner;
use toron::storage::{HlcBound, InMemoryStorageScan, TenantId};
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

	/// ADR 555: this registry rotates by construction, so its placement does not
	/// depend on observed load. The choice is stated explicitly rather than
	/// inherited — the trait has no default precisely so a load-sensitive
	/// registry cannot acquire inert behaviour by omission.
	fn select_and_reserve(&self, hints: &PlacementHints) -> Option<WorkerReservation> {
		self.select_for_attempt(hints)
			.map(WorkerReservation::unreserved)
	}

	fn reserve(&self, worker_id: WorkerId) -> Option<WorkerReservation> {
		self.resolve(worker_id)
			.map(|worker| WorkerReservation::unreserved(worker.worker_id))
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
struct QuantileProcessRun {
	global_digest: toron::distributed::task::ArrowIpcDigest,
	grouped_digest: toron::distributed::task::ArrowIpcDigest,
	global_median: f64,
	grouped_rows: usize,
	selected_workers: Vec<WorkerId>,
}

const ADR547_TUKEY_SQL: &str = "WITH samples AS ( \
       SELECT category, sample AS value FROM fixture \
       WHERE category IS NOT NULL AND sample IS NOT NULL), \
     quartiles AS ( \
       SELECT category, APPROX_QUANTILE(value, 0.25) AS q1, \
              APPROX_QUANTILE(value, 0.50) AS median, \
              APPROX_QUANTILE(value, 0.75) AS q3 \
       FROM samples GROUP BY category), \
     classified AS ( \
       SELECT s.category, s.value, q.q1, q.median, q.q3, \
              q.q1 - 1.5 * (q.q3 - q.q1) AS lower_fence, \
              q.q3 + 1.5 * (q.q3 - q.q1) AS upper_fence \
       FROM samples s JOIN quartiles q ON s.category = q.category), \
     summaries AS ( \
       SELECT category, MIN(CASE WHEN value >= lower_fence THEN value END) AS whisker_low, \
              MIN(q1) AS q1, MIN(median) AS median, MIN(q3) AS q3, \
              MAX(CASE WHEN value <= upper_fence THEN value END) AS whisker_high \
       FROM classified GROUP BY category), \
     outliers AS ( \
       SELECT category, value AS outlier FROM classified \
       WHERE value < lower_fence OR value > upper_fence) \
   SELECT s.category, s.whisker_low, s.q1, s.median, s.q3, s.whisker_high, o.outlier \
   FROM summaries s LEFT JOIN outliers o ON s.category = o.category \
   ORDER BY s.category, o.outlier";

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
struct GlobalJoinAggregateProcessRun {
	/// `(sum, count)` rows — a correct global aggregate yields exactly one.
	rows: Vec<(Option<i64>, i64)>,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct JoinDistinctAggregateProcessRun {
	rows: HashMap<Option<String>, i64>,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct SetOpProcessRun {
	rows: Vec<Option<i64>>,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct CrossJoinProcessRun {
	rows: Vec<(i64, Option<i64>)>,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct WindowProcessRun {
	/// `(partition key value, rank)` rows, sorted.
	rows: Vec<(Option<i64>, i64)>,
	selected_workers: Vec<WorkerId>,
}

#[derive(Debug)]
struct FloorProcessRun {
	rows: Vec<f64>,
	selected_workers: Vec<WorkerId>,
}

/// ADR 236 cross-producer cardinal rule:
/// - the expected value must be unreachable from a single producer / non-merging consumer;
/// - `unique_selected_worker_count > 1` is supporting evidence, not the proof;
/// - the fixture must place a discriminating key or row group on more than one producer.
fn assert_cross_producer_cardinal_is_value_discriminating(
	selected_workers: &[WorkerId],
	discriminator: &str,
) {
	assert!(
		!discriminator.trim().is_empty(),
		"cross-producer cardinal must document why the expected value is the discriminator"
	);
	assert_eq!(
		unique_selected_worker_count(selected_workers),
		2,
		"fixture must exercise more than one worker; value discriminator: {discriminator}"
	);
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
async fn adr531_global_aggregate_over_join_gathers_across_producers() {
	let run = run_global_join_aggregate_process_query(3_100, 0).await;

	// The three matched orders (101,102 from cust 2 -> join partition 0; 103 from cust 3 ->
	// partition 3) are produced by two different join workers. A global SUM/COUNT with no
	// group keys is exact only once the binder's gather funnels both producers' rows to a
	// single terminal aggregate task: one row, SUM=306, COUNT=3. Without the gather each
	// partition folds locally and the coordinator concatenates the partials (203/2, 103/1)
	// — the ADR 529 census's q17 divergence.
	assert_eq!(run.rows, vec![(Some(306), 3)]);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the single 306/3 row requires the gather exchange to co-locate join output produced \
		 on both workers; a single producer reaches only 203/2 or 103/1, and a non-gathering \
		 plan emits two rows",
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
async fn union_all_scans_both_arms_across_workers() {
	let run = run_set_op_process_query(
		1_380,
		0,
		"SELECT set_left.set_key FROM set_left UNION ALL SELECT set_right.set_key FROM set_right",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![None, None, None, Some(7), Some(9), Some(9), Some(10)]
	);
	assert_eq!(
		unique_selected_worker_count(&run.selected_workers),
		2,
		"UNION ALL must not collapse to a single scan worker"
	);
}

// ADR 238: a predicate-fidelity gate is only value-discriminating if the predicate is *fully pushed*
// to the scan (no residual Toron Filter) — otherwise the Filter node does the work regardless of
// whether the fixture worker honors pbc_predicate. These helpers assert the pushdown, so the
// filtered distributed result genuinely proves the worker applied the pushed predicate.
fn collect_scan_pbc_states(plan: &ExecutionPlan, states: &mut Vec<bool>) {
	match plan {
		ExecutionPlan::Scan { pbc_program, .. } => states.push(pbc_program.is_some()),
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => collect_scan_pbc_states(input, states),
		ExecutionPlan::Join { left, right, .. } | ExecutionPlan::Union { left, right, .. } => {
			collect_scan_pbc_states(left, states);
			collect_scan_pbc_states(right, states);
		}
		_ => {}
	}
}

// A `Filter` with `FullyPushed` disposition is a no-op tag — the scan does the filtering. Only a
// residual (non-`FullyPushed`) Filter re-checks in Toron and would mask whether the scan honored PBC.
fn plan_has_residual_recheck(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Filter {
			input, disposition, ..
		} => {
			!matches!(disposition, FilterDisposition::FullyPushed)
				|| plan_has_residual_recheck(input)
		}
		ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. }
		| ExecutionPlan::Repeat { input, .. } => plan_has_residual_recheck(input),
		ExecutionPlan::Join { left, right, .. } | ExecutionPlan::Union { left, right, .. } => {
			plan_has_residual_recheck(left) || plan_has_residual_recheck(right)
		}
		_ => false,
	}
}

async fn assert_predicate_fully_pushes(sql: &str) {
	let logical = SqlPlanner::new().plan(sql).expect("plan predicate query");
	let bound = bind(
		&logical,
		join_process_catalog().as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind predicate query");
	let mut scan_pbc_states = Vec::new();
	collect_scan_pbc_states(&bound, &mut scan_pbc_states);
	assert_eq!(
		scan_pbc_states,
		vec![true; scan_pbc_states.len()],
		"every scan in the predicate-fidelity gate must carry pbc_program, else the gate is hollow: {sql}"
	);
	assert!(
		!scan_pbc_states.is_empty(),
		"predicate-fidelity gate must contain at least one scan: {sql}"
	);
	assert!(
		!plan_has_residual_recheck(&bound),
		"predicate must be FullyPushed (no residual Toron re-check), so the scan must honor it for a \
		 correct result: {sql}"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_predicate_fidelity_does_not_ignore_pbc_predicate() {
	let sql = "SELECT set_left.set_key FROM set_left WHERE set_left.set_key IN (7, 9)";
	assert_predicate_fully_pushes(sql).await;
	let run = run_set_op_process_query(1_440, 0, sql).await;
	// Ignoring pbc_predicate would return the unfiltered set_key column [NULL, NULL, 7, 9].
	assert_eq!(run.rows, vec![Some(7), Some(9)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_predicate_fidelity_filters_before_projection() {
	// Predicate is on `id` (source ordinal 0); projection keeps only `set_key` (ordinal 1). A worker
	// that projected before filtering could not evaluate `id` at all, and one that ignores the
	// predicate returns the unfiltered set_key column.
	let sql = "SELECT set_left.set_key FROM set_left WHERE set_left.id IN (2, 3, 4)";
	assert_predicate_fully_pushes(sql).await;
	let run = run_set_op_process_query(1_450, 0, sql).await;
	assert_eq!(run.rows, vec![None, None, Some(9)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_predicate_fidelity_preserves_null_semantics() {
	// `set_key IN (9)` is unknown for NULL set_key rows (ids 2, 4), so they must be dropped — only
	// id 3 (set_key = 9) survives. Collapsing NULL to a match would wrongly include ids 2 and 4.
	let sql = "SELECT set_left.id FROM set_left WHERE set_left.set_key IN (9)";
	assert_predicate_fully_pushes(sql).await;
	let run = run_set_op_process_query(1_460, 0, sql).await;
	assert_eq!(run.rows, vec![Some(3)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_predicate_fidelity_composes_with_scan_share() {
	// Both UNION ALL arms push a predicate and scan-share across workers (the 236 union fragment
	// path). The result requires filtered rows from both producers.
	let sql = "SELECT set_left.set_key FROM set_left WHERE set_left.id IN (2, 3, 4) \
	           UNION ALL SELECT set_right.set_key FROM set_right WHERE set_right.id IN (12)";
	assert_predicate_fully_pushes(sql).await;
	let run = run_set_op_process_query(1_470, 0, sql).await;
	assert_eq!(run.rows, vec![None, None, Some(9), Some(10)]);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"filtered set_key rows come from both UNION ALL arms (set_left id>1 and set_right id>11), \
		 split across fixture producers; an unfiltered or single-producer scan cannot produce \
		 [NULL, NULL, 9, 10]",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_projection_fidelity_selects_nonzero_column() {
	let run = run_set_op_process_query(
		1_386,
		0,
		"SELECT set_left.set_key FROM set_left UNION ALL SELECT set_right.set_key FROM set_right",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![None, None, None, Some(7), Some(9), Some(9), Some(10)],
		"projecting ordinal 1 must return set_key values, not id values"
	);
	assert_ne!(
		run.rows,
		vec![
			Some(1),
			Some(2),
			Some(3),
			Some(4),
			Some(10),
			Some(11),
			Some(12)
		],
		"ignoring projected_columns would expose the ordinal-0 id values"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"UNION ALL over set_left/set_right uses ScanShare producers; ordinal-1 NULL and 7/9/10 \
		 values cannot be produced by returning ordinal-0 ids",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_projection_fidelity_preserves_nullable_column() {
	let run = run_set_op_process_query(
		1_388,
		0,
		"SELECT set_left.set_key FROM set_left UNION SELECT set_right.set_key FROM set_right",
	)
	.await;

	assert_eq!(run.rows, vec![None, Some(7), Some(9), Some(10)]);
	assert!(
		run.rows.iter().any(Option::is_none),
		"projecting nullable set_key must preserve NULLs"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"UNION DISTINCT shuffles projected nullable set_key values from both fixture producers; \
		 the NULL row is lost if projection nullability is not preserved",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_cross_producer_cardinal_is_value_discriminating() {
	let run = run_set_op_process_query(
		1_389,
		0,
		"SELECT set_left.set_key FROM set_left INTERSECT SELECT set_right.set_key FROM set_right",
	)
	.await;

	assert_eq!(run.rows, vec![None, Some(9)]);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"INTERSECT result [NULL, 9] requires null-safe set membership over projected set_key values \
		 contributed by set_left and set_right fixture producers; worker count alone is not the proof",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_cross_join_cartesian_is_value_discriminating() {
	// ADR 239 D4/D5: `customers CROSS JOIN set_right` is broadcast-only — `set_right` (3 rows) is the
	// broadcast build replicated to every worker, while `customers` (ids 2 and 3) is the scan-shared
	// probe whose two rows land on different workers. The full cartesian therefore requires the
	// broadcast build AND both probe-worker shards; a single producer could only emit one customer id.
	let run = run_cross_join_process_query(
		1_410,
		0,
		"SELECT customers.id, set_right.set_key FROM customers CROSS JOIN set_right",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![
			(2, None),
			(2, Some(9)),
			(2, Some(10)),
			(3, None),
			(3, Some(9)),
			(3, Some(10)),
		],
		"cartesian customers.id {{2,3}} x set_right.set_key {{NULL,9,10}}"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the cartesian customers.id {2,3} x set_right.set_key {NULL,9,10} requires the broadcast \
		 set_right build AND both customers probe-worker shards (id 2 and id 3 scan-share to different \
		 workers); worker count alone is not the proof",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_dedup_distinct_aggregate_distributed_merges_across_producers() {
	// ADR 240 distributed gate (236/238 discipline). `orders JOIN customers` matches customer 2
	// (orders 101, 102) and customer 3 (order 103), both region "west". customer 2 and 3 hash to
	// different join workers, so the result requires merging BOTH the distinct dedup and the plain
	// count across the two producers: a single producer sees only one customer id.
	let run = run_multi_dedup_distinct_join_process_query(1_420, 0).await;

	assert_eq!(
		run.rows.get(&Some("west".to_string())),
		Some(&(2, Some(3))),
		"COUNT(DISTINCT customer_id) over {{2,3}} = 2; COUNT(*) over the 3 matched rows = 3"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"west's COUNT(DISTINCT orders.customer_id)=2 (over {2,3}) and COUNT(*)=3 require merging the \
		 distinct dedup AND the plain count across both join producers (cust 2 and cust 3 land on \
		 different workers); a single producer reaches neither value",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_window_rank_is_value_discriminating() {
	// ADR 241 distributed gate (236/238 discipline). set_left: id=[1,2,3,4], set_key=[7,NULL,9,NULL].
	// The NULL partition holds ids 2 and 4, which scan-share to different workers; ROW_NUMBER within the
	// NULL partition is correct only once the partition exchange co-locates and orders those rows. A
	// single producer sees one of them -> it could only reach rn=1.
	let run = run_window_process_query(
		1_430,
		0,
		"SELECT set_left.set_key, \
		        ROW_NUMBER() OVER (PARTITION BY set_left.set_key ORDER BY set_left.id) AS rn \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 1), (None, 2), (Some(7), 1), (Some(9), 1)],
		"the NULL partition (ids 2,4) ranks 1,2 by id; singletons 7 and 9 rank 1"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the NULL partition's ROW_NUMBER 1,2 over ids {2,4} requires the partition exchange to \
		 co-locate rows scan-shared across both workers; a single producer reaches only rn=1",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_window_aggregate_is_value_discriminating() {
	// ADR 242 distributed gate (236/238 discipline). set_left: id=[1,2,3,4], set_key=[7,NULL,9,NULL].
	// The NULL partition holds ids 2 and 4, which scan-share to different workers; a running SUM(id) over
	// the NULL partition is the cumulative 2 then 6 only once the partition exchange co-locates and
	// orders those rows. A single producer sees one of them -> it could only reach SUM 2 or 4.
	let run = run_window_process_query(
		1_432,
		0,
		"SELECT set_left.set_key, \
		        SUM(set_left.id) OVER (PARTITION BY set_left.set_key ORDER BY set_left.id) AS s \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 2), (None, 6), (Some(7), 1), (Some(9), 3)],
		"the NULL partition (ids 2,4) running SUM is 2 then 6; singletons 7 and 9 sum to their own id"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the NULL partition's running SUM 2,6 over ids {2,4} requires the partition exchange to \
		 co-locate rows scan-shared across both workers; a single producer reaches only 2 or 4",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_frame_distributed_gate_is_value_discriminating() {
	// ADR 243 distributed gate (236/238). set_left: id=[1,2,3,4], set_key=[7,NULL,9,NULL]. The NULL
	// partition holds ids 2 and 4 (scan-shared across workers). A ROWS BETWEEN 1 PRECEDING AND CURRENT
	// ROW sum over the ordered NULL partition is 2 then 6 only once the exchange co-locates and orders
	// both rows; a single producer sees one and reaches only 2 or 4.
	let run = run_window_process_query(
		1_434,
		0,
		"SELECT set_left.set_key, \
		        SUM(set_left.id) OVER (PARTITION BY set_left.set_key ORDER BY set_left.id \
		        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 2), (None, 6), (Some(7), 1), (Some(9), 3)],
		"the NULL partition (ids 2,4) trailing-frame sum is 2 then 6; singletons sum to their own id"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the NULL partition's ROWS-frame sum 2,6 over ids {2,4} requires the partition exchange to \
		 co-locate rows scan-shared across both workers; a single producer reaches only 2 or 4",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_frame_distributed_gate_is_value_discriminating() {
	// ADR 245 distributed gate (236/238). set_left: id=[1,2,3,4], set_key=[7,NULL,9,NULL]. The NULL
	// partition holds ids 2 and 4 (scan-shared across workers); ordered by id they are two peer groups.
	// GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW: id 2 (group 0) sees {2} = 2, id 4 (group 1) sees both
	// groups {2,4} = 6 — only once the exchange co-locates and orders both rows; a single producer reaches
	// only 2 or 4.
	let run = run_window_process_query(
		1_438,
		0,
		"SELECT set_left.set_key, \
		        SUM(set_left.id) OVER (PARTITION BY set_left.set_key ORDER BY set_left.id \
		        GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 2), (None, 6), (Some(7), 1), (Some(9), 3)],
		"the NULL partition (ids 2,4) GROUPS-window sum is 2 then 6; singletons sum to their own id"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the NULL partition's GROUPS-frame sum 2,6 over ids {2,4} requires the partition exchange to \
		 co-locate rows scan-shared across both workers; a single producer reaches only 2 or 4",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_frame_distributed_gate_is_value_discriminating() {
	// ADR 244 distributed gate (236/238). set_left: id=[1,2,3,4], set_key=[7,NULL,9,NULL]. The NULL
	// partition holds ids 2 and 4 (scan-shared across workers). A RANGE BETWEEN 5 PRECEDING AND CURRENT
	// ROW sum over id is the value window [id-5, id]: id 2 sees {2} = 2, id 4 sees {2,4} = 6 — only once
	// the exchange co-locates and orders both rows; a single producer sees one and reaches only 2 or 4.
	let run = run_window_process_query(
		1_436,
		0,
		"SELECT set_left.set_key, \
		        SUM(set_left.id) OVER (PARTITION BY set_left.set_key ORDER BY set_left.id \
		        RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 2), (None, 6), (Some(7), 1), (Some(9), 3)],
		"the NULL partition (ids 2,4) RANGE-window sum is 2 then 6; singletons sum to their own id"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the NULL partition's RANGE-frame sum 2,6 over ids {2,4} requires the partition exchange to \
		 co-locate rows scan-shared across both workers; a single producer reaches only 2 or 4",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn value_functions_distributed_gate_is_value_discriminating() {
	// ADR 246 distributed gate (236/238). set_left: id=[1,2,3,4], set_key=[7,NULL,9,NULL]. The NULL
	// partition holds ids 2 and 4 (scan-shared across workers). LAG(id, 1, 0) over the ordered NULL
	// partition gives id 2 -> 0 (default, no prior) and id 4 -> 2 (the prior row) — only once the exchange
	// co-locates and orders both rows; a single producer sees one and reaches only 0.
	let run = run_window_process_query(
		1_440,
		0,
		"SELECT set_left.set_key, \
		        LAG(set_left.id, 1, 0) OVER (PARTITION BY set_left.set_key ORDER BY set_left.id) AS l \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 0), (None, 2), (Some(7), 0), (Some(9), 0)],
		"the NULL partition (ids 2,4) LAG is 0 then 2; singletons have no prior row -> 0"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the NULL partition's LAG value 2 for id 4 requires the partition exchange to co-locate id 2 \
		 (scan-shared on the other worker) and order it before id 4; a single producer reaches only 0",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_specs_distributed_gate_is_value_discriminating() {
	// ADR 248 distributed gate (236/238), riding on the ADR 247 stacked-exchange substrate. set_left:
	// id=[1,2,3,4], set_key=[7,NULL,9,NULL], spread across both workers. Two distinct OVER specs stack
	// into two Window-over-Exchange nodes: a global `OVER ()` total and a per-set_key sequence. The
	// total column (col1) is SUM(id) over every row = 1+2+3+4 = 10, which holds only once the empty-spec
	// exchange gathers all four rows to one partition across both producers; a single producer reaching
	// only its shard sums to less than 10. The per-set_key ROW_NUMBER (col2, unread) forces the second
	// distinct spec so the query exercises the multi-spec lowering rather than a single funnel.
	let run = run_window_process_query(
		1_442,
		0,
		"SELECT set_left.set_key, \
		        SUM(set_left.id) OVER () AS total, \
		        ROW_NUMBER() OVER (PARTITION BY set_left.set_key ORDER BY set_left.id) AS seq \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 10), (None, 10), (Some(7), 10), (Some(9), 10)],
		"the global OVER () total is 10 on every row only if all four rows gather cross-producer"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"the global OVER () total 10 requires the empty-spec exchange to gather every row across both \
		 workers; alongside a distinct per-set_key spec this proves both specs fragment independently \
		 over the ADR 247 stacked-exchange substrate",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ntile_distributed_gate_is_value_discriminating() {
	// ADR 249 distributed gate (236/238). set_left: id=[1,2,3,4], set_key=[7,NULL,9,NULL]. The NULL
	// partition holds ids 2,4 (scan-shared across workers); ordered by id, NTILE(2) gives id 2 -> bucket
	// 1 and id 4 -> bucket 2, which holds only once the exchange co-locates and orders both rows. A single
	// producer sees one row (a 1-row partition -> bucket 1) and never reaches bucket 2.
	let run = run_window_process_query(
		1_444,
		0,
		"SELECT set_left.set_key, \
		        NTILE(2) OVER (PARTITION BY set_left.set_key ORDER BY set_left.id) AS bucket \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 1), (None, 2), (Some(7), 1), (Some(9), 1)],
		"the NULL partition (ids 2,4) buckets to 1 then 2; the singleton partitions are bucket 1"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"id 4's NTILE bucket 2 requires the partition exchange to co-locate id 2 (scan-shared on the \
		 other worker) and order it first; a single producer sees a 1-row partition and reaches only bucket 1",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ignore_nulls_distributed_gate_is_value_discriminating() {
	// ADR 252 distributed gate (236/238). set_left: id=[1,2,3,4], set_key=[7,NULL,9,NULL], spread across
	// both workers. LAG(set_key, 1, 0) IGNORE NULLS over the whole input (empty PARTITION BY -> gather to
	// one partition): id 3 (set_key 9) skips id 2's NULL and returns id 1's 7 — which holds only once the
	// gather co-locates ids 1,2,3 across both producers. A single producer reaching only its shard finds
	// no prior non-NULL and returns the default 0.
	let run = run_window_process_query(
		1_446,
		0,
		"SELECT set_left.set_key, \
		        LAG(set_left.set_key, 1, 0) IGNORE NULLS OVER (ORDER BY set_left.id) AS l \
		 FROM set_left",
	)
	.await;

	assert_eq!(
		run.rows,
		vec![(None, 7), (None, 9), (Some(7), 0), (Some(9), 7)],
		"id 3 (set_key 9) skips id 2's NULL to id 1's 7; id 1 has no prior non-NULL -> default 0"
	);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"id 3's IGNORE-NULLS LAG value 7 requires the gather to co-locate ids 1,2,3 (scan-shared across \
		 both workers) and skip id 2's NULL; a single producer finds no prior non-NULL and returns 0",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn union_distinct_dedups_across_partitions() {
	let run = run_set_op_process_query(
		1_390,
		0,
		"SELECT set_left.set_key FROM set_left UNION SELECT set_right.set_key FROM set_right",
	)
	.await;

	assert_eq!(run.rows, vec![None, Some(7), Some(9), Some(10)]);
	assert_eq!(
		unique_selected_worker_count(&run.selected_workers),
		2,
		"UNION DISTINCT fixture must exercise more than one worker"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn intersect_null_safe_matching_across_partitions() {
	let run = run_set_op_process_query(
		1_392,
		0,
		"SELECT set_left.set_key FROM set_left INTERSECT SELECT set_right.set_key FROM set_right",
	)
	.await;

	assert_eq!(run.rows, vec![None, Some(9)]);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"INTERSECT result [NULL, 9] requires null-safe set membership over projected set_key values \
		 contributed by set_left and set_right fixture producers",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_op_all_distributed_multiplicity_across_producers() {
	let run = run_set_op_process_query(
		1_394,
		0,
		"SELECT set_left.set_key FROM set_left EXCEPT ALL SELECT set_right.set_key FROM set_right",
	)
	.await;

	assert_eq!(run.rows, vec![None, Some(7)]);
	assert_cross_producer_cardinal_is_value_discriminating(
		&run.selected_workers,
		"EXCEPT ALL result [NULL, 7] requires the two set_left NULL copies, split across fixture \
		 producers, to merge into a left count before subtracting the one right-side NULL and repeating",
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
async fn floor_distributed_execution_matches_the_exact_numeric_result() {
	let run = run_floor_process_query(1_408, 0).await;
	assert_eq!(
		run.rows,
		(0..16).map(|value| value as f64).collect::<Vec<_>>()
	);
	assert_eq!(
		unique_selected_worker_count(&run.selected_workers),
		2,
		"FLOOR must cross the serialized worker boundary, not execute on one local producer"
	);
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
		FragmentPlanConfig::new_for_test(
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
async fn adr546_local_and_multiworker_quantiles_correspond_for_one_execution_seed() {
	let seed = [0x46; 32];
	let local_global =
		run_local_quantile_query("SELECT APPROX_QUANTILE(val, 0.5) FROM fixture", seed).await;
	let local_grouped = run_local_quantile_query(
		"SELECT val, APPROX_QUANTILE(float_val, 0.5) FROM fixture GROUP BY val ORDER BY val",
		seed,
	)
	.await;
	let two_workers = run_quantile_process_query(8_460, 2, 0, seed).await;
	let four_workers = run_quantile_process_query(8_470, 4, 1, seed).await;

	assert_eq!(two_workers.global_median, 8.0);
	assert_eq!(four_workers.global_median, 8.0);
	assert_eq!(two_workers.grouped_rows, 16);
	assert_eq!(four_workers.grouped_rows, 16);
	assert_eq!(two_workers.global_digest, four_workers.global_digest);
	assert_eq!(two_workers.grouped_digest, four_workers.grouped_digest);
	assert_eq!(
		canonical_arrow_ipc_digest(&local_global, SortPolicy::FullRowLex)
			.expect("local global digest"),
		two_workers.global_digest
	);
	assert_eq!(
		canonical_arrow_ipc_digest(&local_grouped, SortPolicy::FullRowLex)
			.expect("local grouped digest"),
		two_workers.grouped_digest
	);
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

#[test]
fn adr547_tukey_relation_corresponds_locally_and_across_worker_counts() {
	// The canonical nested CTE relation needs an enlarged harness stack. Keeping the bind and
	// coordinator on this thread prevents a default-stack overflow from masquerading as lineage.
	std::thread::Builder::new()
		.name("adr547-distributed-tukey".to_string())
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("ADR 547 runtime");
			runtime.block_on(async {
				let seed = [0x47; 32];
				let local = run_local_quantile_query(ADR547_TUKEY_SQL, seed).await;
				let local_digest = canonical_arrow_ipc_digest(&local, SortPolicy::None)
					.expect("local Tukey digest");
				assert_eq!(local.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

				let two_workers = run_tukey_process_query(8_480, 2, 0, seed).await;
				let four_workers = run_tukey_process_query(8_490, 4, 1, seed).await;
				// The same explicit test seed enters all three execution contexts, and every group is
				// below the first compaction threshold. Digest equality therefore proves distributed
				// relation correspondence for the exact population; it is not evidence that independent
				// seeds or post-compaction sketches are byte-identical across executions.
				assert_eq!(two_workers.row_count, 3);
				assert_eq!(four_workers.row_count, 3);
				assert_eq!(two_workers.digest, local_digest);
				assert_eq!(four_workers.digest, local_digest);
				assert_eq!(two_workers.digest, four_workers.digest);
				assert_eq!(
					unique_selected_worker_count(&two_workers.selected_workers),
					2
				);
				assert_eq!(
					unique_selected_worker_count(&four_workers.selected_workers),
					4
				);
			});
		})
		.expect("spawn ADR 547 distributed harness thread")
		.join()
		.expect("ADR 547 distributed harness thread");
}

async fn run_local_quantile_query(sql: &str, seed: [u8; 32]) -> Vec<RecordBatch> {
	let schema = Arc::new(Schema::new(vec![
		Field::new("val", DataType::Int32, false),
		Field::new("float_val", DataType::Float64, true),
		Field::new("category", DataType::Utf8, true),
		Field::new("sample", DataType::Float64, true),
	]));
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from((0..16).collect::<Vec<i32>>())),
			Arc::new(Float64Array::from(
				(0..16)
					.map(|row| Some(row as f64 + 0.25))
					.collect::<Vec<_>>(),
			)),
			Arc::new(StringArray::from(
				(0..16)
					.map(|row| Some(if row < 9 { "fence" } else { "zero" }))
					.collect::<Vec<_>>(),
			)),
			Arc::new(Float64Array::from(vec![
				Some(-6.0),
				Some(-1.0),
				Some(0.0),
				Some(1.0),
				Some(2.0),
				Some(3.0),
				Some(4.0),
				Some(5.0),
				Some(10.0),
				Some(0.0),
				Some(5.0),
				Some(5.0),
				Some(5.0),
				Some(5.0),
				Some(5.0),
				Some(10.0),
			])),
		],
	)
	.expect("local quantile fixture");
	let logical = SqlPlanner::new().plan(sql).expect("plan local quantile");
	let catalog = test_catalog();
	let bound = bind(&logical, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect("bind local quantile");
	Executor::with_context(
		Arc::new(InMemoryStorageScan { table_data: batch }),
		ExecutionContext::local_test().with_quantile_execution(seed, 1_024),
	)
	.execute_bound(&bound, None)
	.await
	.expect("execute local quantile")
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
	// The gathered scalar chain has scan-producer, fold, and sort task slots — three
	// single-task stages; a larger pool must not imply grouped-style fan-out.
	assert_eq!(
		unique_selected_worker_count(&two_workers.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&four_workers.selected_workers),
		3
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_aggregate_having_ordered_process_digest_is_stable_across_worker_counts() {
	let two_workers = run_ordered_having_scalar_aggregate_process_query(2_300, 2, 0).await;
	let four_workers = run_ordered_having_scalar_aggregate_process_query(2_400, 4, 1).await;

	assert_eq!(two_workers.row_count, 1);
	assert_eq!(four_workers.row_count, 1);
	assert_eq!(two_workers.digest, four_workers.digest);
	// The gathered scalar chain has scan-producer, fold, and sort task slots — three
	// single-task stages; a larger pool must not imply grouped-style fan-out.
	assert_eq!(
		unique_selected_worker_count(&two_workers.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&four_workers.selected_workers),
		3
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

async fn run_floor_process_query(worker_id_base: u128, start_offset: usize) -> FloorProcessRun {
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
	let logical = SqlPlanner::new()
		.plan("SELECT FLOOR(float_val) AS floored FROM fixture ORDER BY floored")
		.expect("plan distributed FLOOR query");
	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("distributed FLOOR query");
	let mut rows = Vec::new();
	for batch in &results {
		let values = batch
			.column(0)
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("FLOOR Float64 output");
		rows.extend(values.iter().map(|value| value.expect("fixture value")));
	}
	rows.sort_by(f64::total_cmp);
	for worker in &mut workers {
		worker.shutdown().await;
	}

	FloorProcessRun {
		rows,
		selected_workers: registry.selected_workers(),
	}
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

async fn run_quantile_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
	seed: [u8; 32],
) -> QuantileProcessRun {
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
	let global = SqlPlanner::new()
		.plan("SELECT APPROX_QUANTILE(val, 0.5) FROM fixture")
		.expect("plan global quantile");
	let global_context = ExecutionContext::new(
		SessionId(1),
		QueryId::from_u128(worker_id_base + 100),
		TenantId(1),
		HlcBound(1),
	)
	.with_quantile_execution(seed, 1_024);
	let global_results = coordinator
		.execute_query_with_context(&global, global_context)
		.await
		.expect("global distributed quantile");
	let global_median = global_results[0]
		.column(0)
		.as_any()
		.downcast_ref::<Float64Array>()
		.expect("global quantile output")
		.value(0);

	let grouped = SqlPlanner::new()
		.plan(
			"SELECT val, APPROX_QUANTILE(float_val, 0.5) FROM fixture \
			 GROUP BY val ORDER BY val",
		)
		.expect("plan grouped quantile");
	let grouped_context = ExecutionContext::new(
		SessionId(1),
		QueryId::from_u128(worker_id_base + 101),
		TenantId(1),
		HlcBound(1),
	)
	.with_quantile_execution(seed, 1_024);
	let grouped_results = coordinator
		.execute_query_with_context(&grouped, grouped_context)
		.await
		.expect("grouped distributed quantile");
	let run = QuantileProcessRun {
		global_digest: canonical_arrow_ipc_digest(&global_results, SortPolicy::FullRowLex)
			.expect("global digest"),
		grouped_digest: canonical_arrow_ipc_digest(&grouped_results, SortPolicy::FullRowLex)
			.expect("grouped digest"),
		global_median,
		grouped_rows: grouped_results.iter().map(RecordBatch::num_rows).sum(),
		selected_workers: registry.selected_workers(),
	};
	for worker in &mut workers {
		worker.shutdown().await;
	}
	run
}

async fn run_tukey_process_query(
	worker_id_base: u128,
	worker_count: usize,
	start_offset: usize,
	seed: [u8; 32],
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
		.plan(ADR547_TUKEY_SQL)
		.expect("plan distributed Tukey relation");
	let context = ExecutionContext::new(
		SessionId(1),
		QueryId::from_u128(worker_id_base + 102),
		TenantId(1),
		HlcBound(1),
	)
	.with_quantile_execution(seed, 1_024);
	let results = coordinator
		.execute_query_with_context(&logical, context)
		.await
		.expect("distributed Tukey relation");
	let run = OrderedGroupProcessRun {
		digest: canonical_arrow_ipc_digest(&results, SortPolicy::None).expect("Tukey digest"),
		row_count: results.iter().map(RecordBatch::num_rows).sum(),
		selected_workers: registry.selected_workers(),
	};
	for worker in &mut workers {
		worker.shutdown().await;
	}
	run
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

async fn run_window_process_query(
	worker_id_base: u128,
	start_offset: usize,
	sql: &str,
) -> WindowProcessRun {
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
	let logical = SqlPlanner::new().plan(sql).expect("plan window query");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("window query");
	let rows = window_set_key_rank_rows(&results);
	let selected_workers = registry.selected_workers();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	WindowProcessRun {
		rows,
		selected_workers,
	}
}

async fn run_set_op_process_query(
	worker_id_base: u128,
	start_offset: usize,
	sql: &str,
) -> SetOpProcessRun {
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
	let logical = SqlPlanner::new().plan(sql).expect("plan set operation");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("set operation query");
	let rows = sorted_optional_i64_column_rows(&results, 0);
	let selected_workers = registry.selected_workers();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	SetOpProcessRun {
		rows,
		selected_workers,
	}
}

async fn run_cross_join_process_query(
	worker_id_base: u128,
	start_offset: usize,
	sql: &str,
) -> CrossJoinProcessRun {
	let mut workers = vec![
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base)).await,
		FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + 1)).await,
	];
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		cross_join_process_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		Arc::new(TcpTransport::new()),
		auto_broadcast_config(1024 * 1024),
		QueryBudget::test_default(),
	);
	let logical = SqlPlanner::new().plan(sql).expect("plan cross join");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("cross join query");
	let rows = sorted_i64_optional_i64_pair_rows(&results);
	let selected_workers = registry.selected_workers();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	CrossJoinProcessRun {
		rows,
		selected_workers,
	}
}

async fn run_multi_dedup_distinct_join_process_query(
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
			"SELECT customers.region, COUNT(DISTINCT orders.customer_id), COUNT(*) \
			 FROM orders JOIN customers ON orders.customer_id = customers.id \
			 GROUP BY customers.region",
		)
		.expect("plan multi-dedup over distributed join");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("multi-dedup distinct join query");
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

async fn run_global_join_aggregate_process_query(
	worker_id_base: u128,
	start_offset: usize,
) -> GlobalJoinAggregateProcessRun {
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
			"SELECT SUM(orders.id), COUNT(*) \
			 FROM orders JOIN customers ON orders.customer_id = customers.id",
		)
		.expect("plan global aggregate over distributed join");

	let results = coordinator
		.execute_query(&logical)
		.await
		.expect("global join aggregate query");
	let rows = global_aggregate_rows(&results);
	let selected_workers = registry.selected_workers();
	for worker in &mut workers {
		worker.shutdown().await;
	}

	GlobalJoinAggregateProcessRun {
		rows,
		selected_workers,
	}
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
		FragmentPlanConfig::new_for_test(
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
		FragmentPlanConfig::new_for_test(
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
		FragmentPlanConfig::new_for_test(
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
		FragmentPlanConfig::new_for_test(
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
		on_filter: None,
		null_safe_keys: false,
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
		on_filter: None,
		null_safe_keys: false,
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

fn global_aggregate_rows(results: &[RecordBatch]) -> Vec<(Option<i64>, i64)> {
	let mut rows = Vec::new();
	for batch in results {
		let sums = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("sum column");
		let counts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("count column");
		for row in 0..batch.num_rows() {
			let sum = if sums.is_null(row) {
				None
			} else {
				Some(sums.value(row))
			};
			rows.push((sum, counts.value(row)));
		}
	}
	rows
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

fn sorted_optional_i64_column_rows(results: &[RecordBatch], column_idx: usize) -> Vec<Option<i64>> {
	let mut rows = Vec::new();
	for batch in results {
		let values = batch
			.column(column_idx)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 set-op column");
		for row in 0..batch.num_rows() {
			rows.push(if values.is_null(row) {
				None
			} else {
				Some(values.value(row))
			});
		}
	}
	rows.sort();
	rows
}

fn sorted_i64_optional_i64_pair_rows(results: &[RecordBatch]) -> Vec<(i64, Option<i64>)> {
	let mut rows = Vec::new();
	for batch in results {
		let left = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 left column");
		let right = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 right column");
		for row in 0..batch.num_rows() {
			let right_value = if right.is_null(row) {
				None
			} else {
				Some(right.value(row))
			};
			rows.push((left.value(row), right_value));
		}
	}
	rows.sort();
	rows
}

fn window_set_key_rank_rows(results: &[RecordBatch]) -> Vec<(Option<i64>, i64)> {
	let mut rows = Vec::new();
	for batch in results {
		let key = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 partition key column");
		let rank = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 rank column");
		for row in 0..batch.num_rows() {
			let key = if key.is_null(row) {
				None
			} else {
				Some(key.value(row))
			};
			rows.push((key, rank.value(row)));
		}
	}
	rows.sort();
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
		Field::new("category", DataType::Utf8, true),
		Field::new("sample", DataType::Float64, true),
	]);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), test_descriptor(&schema))
		.expect("register descriptor");
	Arc::new(catalog)
}

fn join_process_catalog() -> Arc<MockCatalog> {
	Arc::new(join_process_catalog_owned())
}

// ADR 239: a CROSS join is broadcast-only, so the distributed gate needs a broadcast-eligible build
// side and an auto-broadcast config. `set_right` (the deterministic SQL right-build) is seeded small;
// `customers` (the scan-shared probe) is seeded large so it never becomes the build.
fn cross_join_process_catalog() -> Arc<MockCatalog> {
	let mut catalog = join_process_catalog_owned();
	seed_byte_upper_bound(&mut catalog, "set_right", 1_024);
	seed_byte_upper_bound(&mut catalog, "customers", 64 * 1024 * 1024);
	Arc::new(catalog)
}

fn seed_byte_upper_bound(catalog: &mut MockCatalog, table: &str, bytes: u64) {
	catalog
		.seed_optimizer_facts(
			&TableRef::new(table),
			TableOptimizerFacts {
				byte_upper_bound: Some(ByteCount::new(bytes)),
				..TableOptimizerFacts::default()
			},
		)
		.expect("seed optimizer facts");
}

fn auto_broadcast_config(max_build_bytes: u64) -> PlannerConfig {
	PlannerConfig {
		auto_broadcast_max_build_bytes: Some(ByteCount::new(max_build_bytes)),
		..PlannerConfig::default()
	}
}

fn join_process_catalog_owned() -> MockCatalog {
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
	catalog
		.register_descriptor(
			TableId(4),
			named_descriptor(
				4,
				"set_left",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("set_key", DataType::Int64, true),
				]),
			),
		)
		.expect("register set_left");
	catalog
		.register_descriptor(
			TableId(5),
			named_descriptor(
				5,
				"set_right",
				Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("set_key", DataType::Int64, true),
				]),
			),
		)
		.expect("register set_right");
	catalog
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

// ADR 260 verify-first distribution gate.
// A correlated aggregate scalar `(SELECT MAX(orders.id) FROM orders WHERE orders.customer_id =
// customers.id)` decorrelates to a `LeftOuter` over a grouped aggregate, recombined on the
// correlation key. This pins that the decorrelated *plan shape* binds and fragments on the hash
// path — the load-bearing "frontend-only, no fragmenter change" claim. We express the shape with a
// CTE (a named table factor) rather than an inline `LEFT JOIN (subquery)`, because the SQL surface
// rejects a derived table in a non-inner join chain (`lower_select`: "derived tables in joins are
// only supported in all-inner chains"); 260's lowering sidesteps that guard by building the
// `LeftOuter` node directly via `build_join_plan`, so the CTE plan is the faithful proxy.
fn adr260_probe_find_join(plan: &ExecutionPlan) -> Option<(JoinKind, JoinStrategy)> {
	match plan {
		ExecutionPlan::Join {
			kind,
			strategy,
			left,
			right,
			..
		} => Some(
			adr260_probe_find_join(left)
				.or_else(|| adr260_probe_find_join(right))
				.unwrap_or((*kind, *strategy)),
		),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => adr260_probe_find_join(input),
		_ => None,
	}
}

#[tokio::test]
async fn adr260_verify_probe_left_outer_over_grouped_aggregate_fragments() {
	let logical = SqlPlanner::new()
		.plan(
			"WITH s AS (SELECT orders.customer_id, MAX(orders.id) AS v \
			            FROM orders GROUP BY orders.customer_id) \
			 SELECT customers.id \
			 FROM customers \
			 LEFT JOIN s ON customers.id = s.customer_id",
		)
		.expect("plan manual correlated-scalar shape (CTE form)");
	let bound = bind(
		&logical,
		join_process_catalog().as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind LeftOuter over grouped aggregate");

	let (kind, strategy) =
		adr260_probe_find_join(&bound).expect("a join is present in the bound plan");
	assert!(
		matches!(kind, JoinKind::LeftOuter),
		"the decorrelated recombine is a LeftOuter, got {kind:?}"
	);
	assert!(
		matches!(strategy, JoinStrategy::HashPartitioned),
		"a LeftOuter cannot broadcast, so it fragments on the hash path, got {strategy:?}"
	);

	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new_for_test(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("LeftOuter over a grouped aggregate fragments");
	// Hash join over two producers: left producer + right (grouped-aggregate) producer + join consumer.
	assert_eq!(
		stages.len(),
		3,
		"hash LeftOuter over grouped aggregate is a 3-stage plan"
	);
}

/// ADR 580: a join's output batches are never mistaken for shuffle partitions.
///
/// The batch-local route returns one output batch per matching probe batch where
/// the concatenating route returned one in total. That is safe only because a
/// `ShuffleProducer` task never publishes a join's batches directly: the
/// fragmenter puts an `Exchange` at the task's root, and the exchange decides
/// partitions from the partition key over ALL input batches. This pins that
/// structure — a producer task whose root stopped being an Exchange would start
/// interpreting batch count as partition count, silently.
#[tokio::test]
async fn a_join_shuffle_producer_repartitions_through_an_exchange() {
	let logical = SqlPlanner::new()
		.plan(
			"SELECT set_left.set_key, set_right.set_key FROM set_left \
			 INNER JOIN set_right ON set_left.set_key = set_right.set_key",
		)
		.expect("plan join sql");
	let bound = bind(
		&logical,
		join_process_catalog().as_ref(),
		&PlannerConfig::default(),
	)
	.await
	.expect("bind join sql");
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new_for_test(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment join sql");

	let mut producers = 0;
	for stage in &stages {
		for task in &stage.tasks {
			let TaskOutputBinding::ShuffleProducer {
				partition_count, ..
			} = &task.spec.output
			else {
				continue;
			};
			producers += 1;
			let ExecutionPlan::Exchange {
				partition_count: plan_partitions,
				..
			} = &task.spec.plan
			else {
				panic!(
					"a shuffle producer's plan root must be an Exchange, not {:?} — \
					 without it the task's returned batches would be published as \
					 partitions",
					task.spec.plan
				);
			};
			assert_eq!(
				PartitionCount::new(*plan_partitions),
				*partition_count,
				"the exchange must produce exactly the partitions the binding declares"
			);
		}
	}
	assert!(
		producers > 0,
		"this fixture must fragment into at least one shuffle producer, or the \
		 test proves nothing"
	);
}
