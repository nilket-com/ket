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
use toron::server::transport::{QuicTransport, TcpTransport, ToronTransport};
use toron::server::{NetworkTaskDispatcher, memory::MemoryManager};
use toron::sql_planner::SqlPlanner;
use toron::storage::{HlcBound, InMemoryStorageScan, TenantId};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

/// ADR 626 Stage A: passive wait events, shared with the fixture worker binary.
#[path = "support/wait_events.rs"]
mod wait_events;

/// Which transport the fixture workers serve and the coordinator dials. TCP unless a test scopes
/// QUIC: QUIC-only workers have no TCP listener, so a sealed shuffle pull completes only if the
/// worker pulls through its transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessTransport {
	Tcp,
	Quic,
}

tokio::task_local! {
	static PROCESS_TRANSPORT: ProcessTransport;
}

fn current_process_transport() -> ProcessTransport {
	PROCESS_TRANSPORT
		.try_with(|transport| *transport)
		.unwrap_or(ProcessTransport::Tcp)
}

fn process_transport() -> Arc<dyn ToronTransport> {
	match current_process_transport() {
		ProcessTransport::Tcp => Arc::new(TcpTransport::new()),
		ProcessTransport::Quic => Arc::new(QuicTransport::new().expect("QUIC transport")),
	}
}

async fn with_quic<T>(run: impl std::future::Future<Output = T>) -> T {
	PROCESS_TRANSPORT.scope(ProcessTransport::Quic, run).await
}

struct FixtureWorkerProcess {
	child: Child,
	endpoint: WorkerEndpoint,
	worker_id: WorkerId,
}

/// A worker that has not announced readiness within this bound is a failed spawn. The child is
/// `kill_on_drop`, so a panic before the guard exists still reaps it.
const FIXTURE_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl FixtureWorkerProcess {
	async fn spawn(worker_id: WorkerId) -> Self {
		Self::spawn_with(worker_id, None).await
	}

	/// `record_peer_dials`: the worker appends one line per sealed-pull dial to this file.
	async fn spawn_with(worker_id: WorkerId, record_peer_dials: Option<&std::path::Path>) -> Self {
		Self::spawn_recorded(worker_id, record_peer_dials, None).await
	}

	/// ADR 626 Stage A: `record_wait_events` makes the worker write passive wait events there.
	async fn spawn_recorded(
		worker_id: WorkerId,
		record_peer_dials: Option<&std::path::Path>,
		record_wait_events: Option<&std::path::Path>,
	) -> Self {
		// ADR 626 Stage A: a diagnostic run may name a staged, hashed fixture worker instead of
		// this build's; unset, every test uses the build's own binary as before.
		let exe = std::env::var_os("TORON_ADR626_FIXTURE_WORKER_EXE")
			.map(std::path::PathBuf::from)
			.unwrap_or_else(|| env!("CARGO_BIN_EXE_toron_fixture_worker").into());
		let mut command = Command::new(exe);
		command
			.arg("--worker-u128")
			.arg(worker_id.get().as_u128().to_string())
			.arg("--transport")
			.arg(match current_process_transport() {
				ProcessTransport::Tcp => "tcp",
				ProcessTransport::Quic => "quic",
			});
		if let Some(path) = record_peer_dials {
			command.arg("--record-peer-dials").arg(path);
		}
		if let Some(path) = record_wait_events {
			command.arg("--record-wait-events").arg(path);
		}
		let mut child = command
			.stdout(Stdio::piped())
			.stderr(Stdio::inherit())
			.kill_on_drop(true)
			.spawn()
			.expect("spawn fixture worker");
		let stdout = child.stdout.take().expect("fixture worker stdout");
		let mut line = String::new();
		let mut reader = BufReader::new(stdout);
		tokio::time::timeout(FIXTURE_READY_TIMEOUT, reader.read_line(&mut line))
			.await
			.expect("fixture worker readiness within its bound")
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

// ---- Sealed shuffle pulls over QUIC-only workers ----
//
// A QUIC-only fixture worker serves tasks and partition pulls on a loopback QUIC endpoint and has
// no TCP listener, so a sealed shuffle completes only if the consumer worker pulls through its
// transport. Each case runs the identical query, data and worker count over TCP for the parity.

const QUIC_CASE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

async fn bounded<T>(run: impl std::future::Future<Output = T>) -> T {
	tokio::time::timeout(QUIC_CASE_TIMEOUT, run)
		.await
		.expect("process case exceeded its bound")
}

/// Evidence of one scheduled run's sealed pulls: the workers' recorded dials, the accepted sealed
/// manifests and the per-task telemetry.
struct SealedPullEvidence {
	/// `(pulling worker, producer worker, producer endpoint)` per sealed-pull dial.
	dials: Vec<(u128, u128, String)>,
	/// `(producer worker, producer endpoint)` of every accepted sealed manifest.
	sealed_producers: Vec<(u128, String)>,
	/// Workers whose tasks pulled a nonzero number of sealed bytes.
	pulling_workers: HashSet<u128>,
}

/// The number of proven cross-worker sealed pulls: a dial by worker C to a producer P != C, whose
/// worker id and endpoint match an accepted sealed manifest, where C's tasks pulled sealed bytes.
fn cross_worker_sealed_pulls(evidence: &SealedPullEvidence) -> Result<usize, String> {
	let proven = evidence
		.dials
		.iter()
		.filter(|(puller, producer, endpoint)| {
			puller != producer
				&& evidence.pulling_workers.contains(puller)
				&& evidence
					.sealed_producers
					.iter()
					.any(|(worker, sealed_endpoint)| {
						worker == producer && sealed_endpoint == endpoint
					})
		})
		.count();
	if proven == 0 {
		return Err(format!(
			"no cross-worker sealed pull among {} dial(s)",
			evidence.dials.len()
		));
	}
	Ok(proven)
}

#[test]
fn cross_worker_sealed_pull_proof_refuses_self_pulls_and_unsealed_producers() {
	let evidence = |dials: Vec<(u128, u128, &str)>, pulling: &[u128]| SealedPullEvidence {
		dials: dials
			.into_iter()
			.map(|(c, p, e)| (c, p, e.to_string()))
			.collect(),
		sealed_producers: vec![(1, "a".to_string()), (2, "b".to_string())],
		pulling_workers: pulling.iter().copied().collect(),
	};
	assert_eq!(
		cross_worker_sealed_pulls(&evidence(vec![(1, 2, "b")], &[1])),
		Ok(1)
	);
	// Both workers pull, but each only from itself: no edge.
	assert!(cross_worker_sealed_pulls(&evidence(vec![(1, 1, "a"), (2, 2, "b")], &[1, 2])).is_err());
	// A dial to an endpoint no accepted manifest sealed at: no edge.
	assert!(cross_worker_sealed_pulls(&evidence(vec![(1, 2, "elsewhere")], &[1])).is_err());
	// A dial from a worker whose tasks pulled nothing: no edge.
	assert!(cross_worker_sealed_pulls(&evidence(vec![(1, 2, "b")], &[2])).is_err());
}

/// The hash join through the scheduler, with every worker recording its sealed-pull dials.
async fn run_hash_join_scheduled(
	worker_id_base: u128,
	worker_count: u128,
) -> (JoinProcessRun, SealedPullEvidence) {
	let record_dir = std::env::temp_dir().join(format!(
		"toron-sealed-dials-{}-{}",
		std::process::id(),
		worker_id_base
	));
	let _ = std::fs::remove_dir_all(&record_dir);
	std::fs::create_dir_all(&record_dir).expect("dial record directory");
	let mut workers = Vec::new();
	for offset in 0..worker_count {
		let worker_id = worker_id_base + offset;
		workers.push(
			FixtureWorkerProcess::spawn_with(
				WorkerId::from_u128(worker_id),
				Some(&record_dir.join(format!("{worker_id}.dials"))),
			)
			.await,
		);
	}
	let registry = Arc::new(RoundRobinRegistry::new(workers.iter().map(worker_entry), 0));
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
	let mut dispatcher = NetworkTaskDispatcher::new(
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		process_transport(),
	);
	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("hash join");
	let result = join_process_run(run.final_results, registry.selected_workers());
	for worker in &mut workers {
		worker.shutdown().await;
	}
	let mut dials = Vec::new();
	for entry in std::fs::read_dir(&record_dir).expect("dial records") {
		let text =
			std::fs::read_to_string(entry.expect("dial record").path()).expect("dial record");
		for line in text.lines() {
			let fields = line.split(' ').collect::<Vec<_>>();
			assert_eq!(fields.len(), 3, "dial record line {line:?}");
			dials.push((
				fields[0].parse().expect("pulling worker"),
				fields[1].parse().expect("producer worker"),
				fields[2].to_string(),
			));
		}
	}
	let _ = std::fs::remove_dir_all(&record_dir);
	let evidence = SealedPullEvidence {
		dials,
		sealed_producers: run
			.accepted_manifests
			.values()
			.map(|manifest| {
				(
					manifest.producer_worker_id.get().as_u128(),
					manifest.producer_endpoint.as_str().to_string(),
				)
			})
			.collect(),
		pulling_workers: run
			.telemetry
			.task_telemetry
			.iter()
			.filter(|record| record.telemetry.counters.pulled_shuffle_bytes > 0)
			.map(|record| record.worker_id.get().as_u128())
			.collect(),
	};
	(result, evidence)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_only_fixture_workers_have_no_tcp_listener() {
	bounded(async {
		let mut quic = with_quic(FixtureWorkerProcess::spawn(WorkerId::from_u128(5_000))).await;
		assert!(
			tokio::net::TcpStream::connect(quic.endpoint.as_str())
				.await
				.is_err(),
			"a QUIC-only worker must not accept TCP at its announced endpoint"
		);
		quic.shutdown().await;
		let mut tcp = FixtureWorkerProcess::spawn(WorkerId::from_u128(5_001)).await;
		assert!(
			tokio::net::TcpStream::connect(tcp.endpoint.as_str())
				.await
				.is_ok(),
			"control: a TCP worker accepts TCP at its announced endpoint"
		);
		tcp.shutdown().await;
	})
	.await;
}

/// The failure path before a worker guard exists: a worker that cannot start exits without
/// announcing readiness, and the spawned child is reaped (`kill_on_drop`) rather than left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_worker_that_cannot_start_exits_without_readiness() {
	bounded(async {
		let mut child = Command::new(env!("CARGO_BIN_EXE_toron_fixture_worker"))
			.arg("--worker-u128")
			.arg("5002")
			.arg("--transport")
			.arg("carrier-pigeon")
			.stdout(Stdio::piped())
			.stderr(Stdio::null())
			.kill_on_drop(true)
			.spawn()
			.expect("spawn fixture worker");
		let mut stdout = String::new();
		tokio::io::AsyncReadExt::read_to_string(
			child.stdout.as_mut().expect("fixture worker stdout"),
			&mut stdout,
		)
		.await
		.expect("read fixture worker stdout");
		let status = child.wait().await.expect("fixture worker exit");
		assert!(!status.success(), "an unstartable worker must fail");
		assert!(
			!stdout.contains("READY"),
			"an unstartable worker must not announce readiness"
		);
	})
	.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_shuffle_pulls_cross_quic_only_workers_with_tcp_digest_parity() {
	for worker_count in [2u128, 3] {
		let (tcp, tcp_evidence) = bounded(run_hash_join_scheduled(
			5_100 + worker_count * 10,
			worker_count,
		))
		.await;
		let (quic, quic_evidence) = bounded(with_quic(run_hash_join_scheduled(
			5_200 + worker_count * 10,
			worker_count,
		)))
		.await;
		let tcp_edges =
			cross_worker_sealed_pulls(&tcp_evidence).expect("TCP cross-worker sealed pull");
		let quic_edges =
			cross_worker_sealed_pulls(&quic_evidence).expect("QUIC cross-worker sealed pull");
		println!(
			"{worker_count} workers: cross-worker sealed pulls tcp={tcp_edges} quic={quic_edges}"
		);
		assert_eq!(tcp.row_count, 3, "{worker_count} workers");
		assert_eq!(quic.row_count, 3, "{worker_count} workers");
		assert_eq!(
			quic.digest, tcp.digest,
			"{worker_count} workers: QUIC result equals TCP"
		);
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_grouped_aggregate_over_quic_only_workers_matches_tcp() {
	for worker_count in [2usize, 3] {
		let base = 5_300 + (worker_count as u128) * 10;
		let tcp = bounded(run_ordered_grouped_aggregate_process_query(
			base,
			worker_count,
			0,
		))
		.await;
		let quic = bounded(with_quic(run_ordered_grouped_aggregate_process_query(
			base + 100,
			worker_count,
			0,
		)))
		.await;
		assert_eq!(tcp.row_count, 5);
		assert_eq!(quic.row_count, 5);
		assert_eq!(
			quic.digest, tcp.digest,
			"{worker_count} workers: QUIC result equals TCP"
		);
		assert_eq!(
			unique_selected_worker_count(&quic.selected_workers),
			worker_count
		);
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_op_and_window_over_quic_only_workers_match_tcp() {
	let except_all =
		"SELECT set_left.set_key FROM set_left EXCEPT ALL SELECT set_right.set_key FROM set_right";
	let row_number = "SELECT set_left.set_key, \
	        ROW_NUMBER() OVER (PARTITION BY set_left.set_key ORDER BY set_left.id) AS rn \
	 FROM set_left";
	for worker_count in [2u128, 3] {
		let base = 5_500 + worker_count * 100;
		let tcp = bounded(run_set_op_process_query_on(
			base,
			0,
			except_all,
			worker_count,
		))
		.await;
		let quic = bounded(with_quic(run_set_op_process_query_on(
			base + 10,
			0,
			except_all,
			worker_count,
		)))
		.await;
		assert_eq!(tcp.rows, vec![None, Some(7)], "{worker_count} workers");
		assert_eq!(quic.rows, tcp.rows, "{worker_count} workers");
		assert!(unique_selected_worker_count(&quic.selected_workers) >= 2);

		let tcp = bounded(run_window_process_query_on(
			base + 20,
			0,
			row_number,
			worker_count,
		))
		.await;
		let quic = bounded(with_quic(run_window_process_query_on(
			base + 30,
			0,
			row_number,
			worker_count,
		)))
		.await;
		assert_eq!(
			tcp.rows,
			vec![(None, 1), (None, 2), (Some(7), 1), (Some(9), 1)],
			"{worker_count} workers"
		);
		assert_eq!(quic.rows, tcp.rows, "{worker_count} workers");
		assert!(unique_selected_worker_count(&quic.selected_workers) >= 2);
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_join_producer_kill_over_quic_only_workers_replays_and_matches_baseline() {
	let baseline = bounded(run_hash_join_process_query(5_600, 0)).await;
	let left_killed = bounded(with_quic(run_hash_join_producer_kill_query(5_610, 0))).await;
	let right_killed = bounded(with_quic(run_hash_join_producer_kill_query(5_620, 1))).await;
	assert_eq!(baseline.row_count, 3);
	assert_eq!(left_killed.digest, baseline.digest);
	assert_eq!(right_killed.digest, baseline.digest);
	assert_eq!(
		unique_selected_worker_count(&left_killed.selected_workers),
		2
	);
	assert_eq!(
		unique_selected_worker_count(&right_killed.selected_workers),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
	run_window_process_query_on(worker_id_base, start_offset, sql, 2).await
}

async fn run_window_process_query_on(
	worker_id_base: u128,
	start_offset: usize,
	sql: &str,
	worker_count: u128,
) -> WindowProcessRun {
	let mut workers = Vec::new();
	for offset in 0..worker_count {
		workers
			.push(FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + offset)).await);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		join_process_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		process_transport(),
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
	run_set_op_process_query_on(worker_id_base, start_offset, sql, 2).await
}

async fn run_set_op_process_query_on(
	worker_id_base: u128,
	start_offset: usize,
	sql: &str,
	worker_count: u128,
) -> SetOpProcessRun {
	let mut workers = Vec::new();
	for offset in 0..worker_count {
		workers
			.push(FixtureWorkerProcess::spawn(WorkerId::from_u128(worker_id_base + offset)).await);
	}
	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(worker_entry),
		start_offset,
	));
	let coordinator = DistributedQueryCoordinator::new(
		join_process_catalog(),
		registry.clone(),
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
	let mut dispatcher = NetworkTaskDispatcher::new(memory, process_transport());

	let producers_run = scheduler
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

	// The invalidated producer had sealed its output on the killed worker before the kill.
	let lost = &invalidated.producers[0];
	let sealed = producers_run
		.accepted_manifests
		.get(&lost.key)
		.expect("the invalidated producer's manifest was accepted before the kill");
	assert_eq!(sealed.attempt_id, lost.invalidated_attempt);
	assert_eq!(sealed.producer_worker_id, killed_worker_id);

	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.expect("replacement dispatch");
	assert!(
		run.dispatched
			.contains(&toron::distributed::identity::TaskIdentity::new(
				lost.key.query_id,
				lost.key.stage_id,
				lost.key.task_id,
				lost.replacement_attempt,
			)),
		"the invalidated producer itself must be re-dispatched at its replacement attempt"
	);
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
		process_transport(),
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
	let mut dispatcher = NetworkTaskDispatcher::new(memory, process_transport());

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

/// ADR 626 Stage A: the join catalog plus the fixture worker's sized payload tables (ids 6–8).
fn wait_survey_catalog() -> Arc<MockCatalog> {
	let mut catalog = join_process_catalog_owned();
	for (id, name) in [(6u8, "payload_16"), (7, "payload_16k"), (8, "payload_192k")] {
		catalog
			.register_descriptor(
				TableId(u64::from(id)),
				named_descriptor(
					id,
					name,
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("pad", DataType::Utf8, false),
					]),
				),
			)
			.expect("register sized payload");
	}
	Arc::new(catalog)
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

// ---- ADR 625: the multinode probe ----
//
// Ignored tests driven by a spec file (`TORON_MULTINODE_SPEC`): they dial already-running host-mode
// fixture workers over authenticated QUIC and write one evidence JSON per attempt into the spec's
// attempt directory, never overwriting. Intervals are measured on this process's steady (`Instant`) clock;
// wall times are provenance only. Evidence is written on every outcome, including errors and
// deadlines; the harness's readers, not these tests' exit codes, decide acceptance.

struct ProbeSpec {
	raw: serde_json::Value,
	attempt_dir: std::path::PathBuf,
	workers: Vec<(WorkerId, WorkerEndpoint)>,
	transport: Arc<dyn ToronTransport>,
	deadline: std::time::Duration,
	/// ADR 626 Stage A: `probe_events.jsonl` in the attempt directory when the spec asks for it.
	wait_log: Option<Arc<wait_events::EventLog>>,
}

fn unix_ms() -> u128 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|elapsed| elapsed.as_millis())
		.unwrap_or(0)
}

/// A task identity as exact typed fields (never a debug rendering), so readers compare identities.
fn task_fields(query: QueryId, stage: u32, task: u32, attempt: AttemptId) -> serde_json::Value {
	serde_json::json!({
		"query": query.get().to_string(),
		"stage": stage,
		"task": task,
		"attempt": attempt.get(),
	})
}

fn spec_str(raw: &serde_json::Value, name: &str) -> String {
	raw[name]
		.as_str()
		.unwrap_or_else(|| panic!("spec field {name}"))
		.to_string()
}

fn load_probe_spec() -> ProbeSpec {
	let path = std::env::var("TORON_MULTINODE_SPEC").expect("TORON_MULTINODE_SPEC");
	let raw: serde_json::Value =
		serde_json::from_str(&std::fs::read_to_string(&path).expect("read spec"))
			.expect("parse spec");
	let workers = raw["workers"]
		.as_array()
		.expect("spec workers")
		.iter()
		.map(|worker| {
			(
				WorkerId::from_u128(
					spec_str(worker, "worker_u128")
						.parse()
						.expect("worker_u128"),
				),
				WorkerEndpoint::new(spec_str(worker, "endpoint")),
			)
		})
		.collect::<Vec<_>>();
	assert!(
		!workers.is_empty(),
		"a probe spec must name at least one worker"
	);
	let client = toron::network::tls::client_config_from_paths(
		std::path::Path::new(&spec_str(&raw, "tls_cert")),
		std::path::Path::new(&spec_str(&raw, "tls_key")),
		std::path::Path::new(&spec_str(&raw, "tls_ca")),
	)
	.expect("probe client TLS material");
	let transport: Arc<dyn ToronTransport> = Arc::new(
		QuicTransport::authenticated(client, spec_str(&raw, "cluster_domain"))
			.expect("authenticated transport"),
	);
	let attempt_dir = std::path::PathBuf::from(spec_str(&raw, "attempt_dir"));
	let wait_log = (raw["wait_events"].as_bool() == Some(true)).then(|| {
		wait_events::EventLog::create(&attempt_dir.join("probe_events.jsonl"))
			.expect("probe wait events must be a fresh file")
	});
	let transport: Arc<dyn ToronTransport> = match &wait_log {
		Some(log) => Arc::new(wait_events::ObservedTransport {
			inner: transport,
			log: log.clone(),
			role: "dispatch",
		}),
		None => transport,
	};
	ProbeSpec {
		attempt_dir,
		workers,
		transport,
		deadline: std::time::Duration::from_secs(
			raw["deadline_secs"].as_u64().expect("deadline_secs"),
		),
		wait_log,
		raw,
	}
}

/// Write the attempt's evidence exactly once; an existing file refuses.
fn write_probe_evidence(dir: &std::path::Path, evidence: &serde_json::Value) {
	use std::io::Write as _;
	let mut file = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(dir.join("probe.json"))
		.expect("probe evidence must be a fresh file");
	file.write_all(
		serde_json::to_string_pretty(evidence)
			.expect("evidence json")
			.as_bytes(),
	)
	.expect("write probe evidence");
}

fn probe_registry(spec: &ProbeSpec) -> Arc<RoundRobinRegistry> {
	Arc::new(RoundRobinRegistry::new(
		spec.workers
			.iter()
			.map(|(worker_id, endpoint)| worker_entry_for(*worker_id, endpoint.clone())),
		0,
	))
}

fn worker_entry_for(worker_id: WorkerId, endpoint: WorkerEndpoint) -> WorkerEntry {
	WorkerEntry {
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
	}
}

/// The named cases: SQL over the process fixtures, planned and bound the same way for every
/// attempt and every placement.
async fn probe_case_plan(case: &str) -> ExecutionPlan {
	let (catalog, logical) = match case {
		"hash_join" => (join_process_catalog(), hash_join_plan()),
		"grouped_aggregate" => (
			test_catalog(),
			SqlPlanner::new()
				.plan("SELECT val, SUM(val) FROM fixture GROUP BY val ORDER BY SUM(val) DESC LIMIT 5")
				.expect("plan grouped aggregate"),
		),
		"except_all" => (
			join_process_catalog(),
			SqlPlanner::new()
				.plan("SELECT set_left.set_key FROM set_left EXCEPT ALL SELECT set_right.set_key FROM set_right")
				.expect("plan except all"),
		),
		"row_number" => (
			join_process_catalog(),
			SqlPlanner::new()
				.plan(
					"SELECT set_left.set_key, ROW_NUMBER() OVER (PARTITION BY set_left.set_key ORDER BY set_left.id) AS rn FROM set_left",
				)
				.expect("plan row number"),
		),
		"sized_16" | "sized_16k" | "sized_192k" => {
			let table = format!("payload_{}", case.trim_start_matches("sized_"));
			(
				wait_survey_catalog(),
				SqlPlanner::new()
					.plan(&format!(
						"SELECT {table}.pad, customers.region FROM {table} JOIN customers ON {table}.id = customers.id"
					))
					.expect("plan sized payload join"),
			)
		}
		other => panic!("unknown probe case {other}"),
	};
	bind(&logical, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect("bind probe case")
}

fn manifest_evidence(manifest: &toron::distributed::task::PartitionManifest) -> serde_json::Value {
	serde_json::json!({
		"task": task_fields(manifest.query_id, manifest.stage_id.get(), manifest.task_id.get(), manifest.attempt_id),
		"producer_worker": manifest.producer_worker_id.get().as_u128().to_string(),
		"producer_endpoint": manifest.producer_endpoint.as_str(),
	})
}

fn run_evidence(run: &SchedulerRun) -> serde_json::Value {
	let digest =
		canonical_arrow_ipc_digest(&run.final_results, SortPolicy::FullRowLex).expect("digest");
	let rows = run
		.final_results
		.iter()
		.map(RecordBatch::num_rows)
		.sum::<usize>();
	// Every row as text, sorted, so equal results render equally regardless of batch order.
	let mut rendered = Vec::new();
	for batch in &run.final_results {
		for row in 0..batch.num_rows() {
			rendered.push(
				batch
					.columns()
					.iter()
					.map(|column| {
						arrow::util::display::array_value_to_string(column.as_ref(), row)
							.unwrap_or_else(|error| format!("<{error}>"))
					})
					.collect::<Vec<_>>()
					.join("|"),
			);
		}
	}
	rendered.sort();
	serde_json::json!({
		"digest": format!("{digest:?}"),
		"row_count": rows,
		"rows_rendered": rendered,
		"retry_count": run.telemetry.retry_count,
		"dispatched": run.dispatched.iter().map(|identity| task_fields(identity.query_id, identity.stage_id.get(), identity.task_id.get(), identity.attempt_id)).collect::<Vec<_>>(),
		"sealed_manifests": run.accepted_manifests.values().map(manifest_evidence).collect::<Vec<_>>(),
		"task_telemetry": run.telemetry.task_telemetry.iter().map(|record| serde_json::json!({
			"task": task_fields(record.identity.query_id, record.identity.stage_id.get(), record.identity.task_id.get(), record.identity.attempt_id),
			"worker": record.worker_id.get().as_u128().to_string(),
			"pulled_shuffle_bytes": record.telemetry.counters.pulled_shuffle_bytes,
			"published_shuffle_bytes": record.telemetry.counters.published_shuffle_bytes,
		})).collect::<Vec<_>>(),
	})
}

fn probe_base_evidence(spec: &ProbeSpec, mode: &str) -> serde_json::Value {
	let toron_env = std::env::vars()
		.filter(|(name, _)| name.starts_with("TORON_"))
		.map(|(name, value)| format!("{name}={value}"))
		.collect::<Vec<_>>();
	serde_json::json!({
		"mode": mode,
		"spec": spec.raw.clone(),
		"probe_executable": std::env::current_exe().map(|path| path.display().to_string()).unwrap_or_default(),
		"toron_environment": toron_env,
		"started_unix_ms": unix_ms() as u64,
	})
}

/// ADR 626 Stage A: records each dispatch's start and return per task attempt, and tags the
/// transport's dial events with the dispatch they belong to. Passive: the inner dispatcher's
/// outcome is returned unchanged.
struct ObservedDispatcher<D> {
	inner: D,
	log: Option<Arc<wait_events::EventLog>>,
}

fn dispatch_outcome_text(outcome: &toron::distributed::scheduler::DispatchOutcome) -> String {
	use toron::distributed::scheduler::DispatchOutcome;
	match outcome {
		DispatchOutcome::Completed(_)
		| DispatchOutcome::CompletedWithBatches { .. }
		| DispatchOutcome::CompletedWithTelemetry { .. } => "completed".to_string(),
		DispatchOutcome::Failed(kind) | DispatchOutcome::FailedWithTelemetry { kind, .. } => {
			format!("failed: {kind:?}")
		}
	}
}

#[async_trait::async_trait]
impl<D> toron::distributed::scheduler::TaskDispatcher for ObservedDispatcher<D>
where
	D: toron::distributed::scheduler::TaskDispatcher + Send + Sync,
{
	async fn dispatch(
		&self,
		worker: &WorkerEntry,
		spec: &toron::distributed::task::TaskSpec,
	) -> toron::distributed::scheduler::DispatchOutcome {
		let Some(log) = &self.log else {
			return self.inner.dispatch(worker, spec).await;
		};
		let identity = spec.identity;
		let label = format!(
			"{}/{}/{}/{}",
			identity.query_id.get(),
			identity.stage_id.get(),
			identity.task_id.get(),
			identity.attempt_id.get()
		);
		let mut fields = serde_json::json!({
			"task": label,
			"identity": task_fields(identity.query_id, identity.stage_id.get(), identity.task_id.get(), identity.attempt_id),
			"worker": worker.worker_id.get().as_u128().to_string(),
			"endpoint": worker.endpoint.as_str(),
		});
		// Captured instants on the log's clock, so `elapsed_us` is exactly the interval the events span.
		let started = log.now_us();
		log.emit_at(started, "dispatch_start", "dispatch", fields.clone());
		let outcome = wait_events::DISPATCH_TASK
			.scope(label, self.inner.dispatch(worker, spec))
			.await;
		let ended = log.now_us();
		fields["elapsed_us"] = ended.saturating_sub(started).into();
		fields["outcome"] = dispatch_outcome_text(&outcome).into();
		log.emit_at(ended, "dispatch_return", "dispatch", fields);
		outcome
	}
}

fn fresh_scheduler(
	spec: &ProbeSpec,
) -> (
	Arc<RoundRobinRegistry>,
	DistributedScheduler<Arc<RoundRobinRegistry>>,
	ObservedDispatcher<NetworkTaskDispatcher<Arc<dyn ToronTransport>>>,
) {
	let registry = probe_registry(spec);
	let scheduler = DistributedScheduler::new(registry.clone());
	let dispatcher = ObservedDispatcher {
		inner: NetworkTaskDispatcher::new(
			Arc::new(MemoryManager::new(64, 1024 * 1024)),
			spec.transport.clone(),
		),
		log: spec.wait_log.clone(),
	};
	(registry, scheduler, dispatcher)
}

/// Authenticated reachability, per worker independently, with an application-level answer that
/// runs no task: a pull for a partition that cannot exist must come back `PartitionMissing`, which
/// only a worker that accepted this client's certificate and served the stream can send.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "multinode probe: needs TORON_MULTINODE_SPEC and running host-mode workers"]
async fn multinode_probe_handshake() {
	let spec = load_probe_spec();
	let mut evidence = probe_base_evidence(&spec, "handshake");
	let coordinator =
		toron::server::ToronCoordinator::new(Arc::new(MemoryManager::new(4, 1024 * 1024)));
	let client = || {
		toron::network::tls::client_config_from_paths(
			std::path::Path::new(&spec_str(&spec.raw, "tls_cert")),
			std::path::Path::new(&spec_str(&spec.raw, "tls_key")),
			std::path::Path::new(&spec_str(&spec.raw, "tls_ca")),
		)
		.expect("probe client TLS material")
	};
	let domain = spec_str(&spec.raw, "cluster_domain");
	let mut endpoints = Vec::new();
	for (worker_id, endpoint) in &spec.workers {
		// One client endpoint per worker: the exact local port identifies this connection in the
		// worker's own log. The same authenticated dial as `QuicTransport::connect_peer`.
		let client_endpoint =
			toron::quic::QuicEndpoint::client_authenticated(client()).expect("client endpoint");
		let client_port = client_endpoint.local_addr().expect("client port").port();
		let server_name = toron::network::tls::canonical_daemon_dns_name(worker_id, &domain);
		let window_start = unix_ms() as u64;
		let started = std::time::Instant::now();
		// ADR 626 Stage A: establishment (connect plus stream open) timed apart from the answer.
		let mut connect_elapsed_us: Option<u64> = None;
		let address = toron::distributed::task::PartitionAddress {
			query_id: QueryId::new(),
			exchange_id: toron::distributed::identity::ExchangeId::new(0),
			producer_stage_id: toron::distributed::identity::StageId::new(0),
			producer_task_id: toron::distributed::identity::TaskId::new(0),
			attempt_id: AttemptId::first(),
			partition_id: toron::distributed::identity::PartitionId::new(0),
		};
		let outcome = tokio::time::timeout(spec.deadline, async {
			let remote: std::net::SocketAddr = endpoint
				.as_str()
				.parse()
				.map_err(|error| ("handshake_error", format!("endpoint {endpoint:?}: {error}")))?;
			let stream = client_endpoint
				.connect_stream_with_server_name(remote, &server_name)
				.await
				.map_err(|error| ("handshake_error", format!("dial: {error}")))?;
			connect_elapsed_us = Some(started.elapsed().as_micros() as u64);
			match coordinator
				.pull_partition_from_worker(stream, address, AttemptId::first())
				.await
			{
				Err(toron::error::QueryError::Runtime(
					toron::error::RuntimeError::PartitionStoreMismatch { message },
				)) => Ok(message),
				Err(error) => Err(("ack_error", error.to_string())),
				Ok(_) => Err(("ack_error", "a fabricated partition was served".to_string())),
			}
		})
		.await;
		let (status, detail) = match outcome {
			Ok(Ok(missing)) => ("acknowledged", format!("PartitionMissing: {missing}")),
			Ok(Err((status, detail))) => (status, detail),
			Err(_) => ("deadline", format!("no outcome within {:?}", spec.deadline)),
		};
		endpoints.push(serde_json::json!({
			"worker": worker_id.get().as_u128().to_string(),
			"endpoint": endpoint.as_str(),
			"status": status,
			"detail": detail,
			"elapsed_ms": started.elapsed().as_millis() as u64,
			"connect_elapsed_us": connect_elapsed_us,
			"client_port": client_port,
			"window_start_unix_ms": window_start,
			"ended_unix_ms": unix_ms() as u64,
		}));
	}
	evidence["endpoints"] = serde_json::Value::Array(endpoints);
	evidence["ended_unix_ms"] = (unix_ms() as u64).into();
	write_probe_evidence(&spec.attempt_dir, &evidence);
}

/// One case through the scheduler against the declared workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "multinode probe: needs TORON_MULTINODE_SPEC and running host-mode workers"]
async fn multinode_probe_query() {
	let spec = load_probe_spec();
	let case = spec_str(&spec.raw, "case");
	let mut evidence = probe_base_evidence(&spec, "query");
	let stages = fragment_plan(
		probe_case_plan(&case).await,
		FragmentPlanConfig::new_for_test(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment probe case");
	let (_registry, mut scheduler, mut dispatcher) = fresh_scheduler(&spec);
	let started = std::time::Instant::now();
	let outcome = tokio::time::timeout(
		spec.deadline,
		scheduler.dispatch_loop(&stages, &mut dispatcher),
	)
	.await;
	evidence["dispatch_elapsed_ms"] = (started.elapsed().as_millis() as u64).into();
	match outcome {
		Ok(Ok(run)) => {
			evidence["outcome"] = "completed".into();
			evidence["run"] = run_evidence(&run);
		}
		Ok(Err(error)) => {
			evidence["outcome"] = "error".into();
			evidence["error"] = format!("{error:?}").into();
		}
		Err(_) => evidence["outcome"] = "deadline".into(),
	}
	evidence["ended_unix_ms"] = (unix_ms() as u64).into();
	write_probe_evidence(&spec.attempt_dir, &evidence);
}

/// Producer loss after its stage sealed, on a worker remote from this probe. The victim is bound
/// before the kill: one accepted sealed manifest (task key, attempt, worker) from a remote worker.
/// `announced` marks it lost and requires the invalidation and the replacement dispatch to name that
/// exact task (ADR 624's binding), all inside one whole-attempt deadline on the steady clock. `unannounced`
/// tells the scheduler nothing and gives the final dispatch the declared 30 s bound from the
/// confirmed kill. `dispatch_loop` exposes no internal events, so the first error or retry inside
/// it is not observable here: detection timing is reported as unavailable, and the outcome is the
/// loop's terminal return (or none) within the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "multinode probe: needs TORON_MULTINODE_SPEC and running host-mode workers"]
async fn multinode_probe_worker_loss() {
	let spec = load_probe_spec();
	let attempt_started = std::time::Instant::now();
	// ADR 626 Stage A adds `unannounced_dispatch_target`: the victim is a remote worker holding no
	// accepted sealed manifest, killed before the remaining stages are dispatched.
	let (announced, dispatch_target) = match spec_str(&spec.raw, "loss").as_str() {
		"announced" => (true, false),
		"unannounced" => (false, false),
		"unannounced_dispatch_target" => (false, true),
		other => panic!("unknown loss mode {other}"),
	};
	let mut evidence = probe_base_evidence(
		&spec,
		if announced {
			"announced_loss"
		} else if dispatch_target {
			"unannounced_dispatch_target_loss"
		} else {
			"unannounced_loss"
		},
	);
	evidence["detection_timing"] =
		"unavailable: dispatch_loop exposes no internal error or retry events; terminal return time is reported".into();
	let remote = spec.raw["remote_workers"]
		.as_array()
		.expect("remote_workers")
		.iter()
		.map(|worker| worker.as_str().expect("remote worker").to_string())
		.collect::<HashSet<_>>();
	let stages = fragment_plan(
		probe_case_plan("hash_join").await,
		FragmentPlanConfig::new_for_test(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment probe case");
	let (_registry, mut scheduler, mut dispatcher) = fresh_scheduler(&spec);
	let finish = |evidence: &mut serde_json::Value, outcome: &str| {
		if let Some(log) = &spec.wait_log {
			log.emit(
				"observation_end",
				"dispatch",
				serde_json::json!({ "outcome": outcome }),
			);
		}
		evidence["outcome"] = outcome.into();
		evidence["attempt_elapsed_ms"] = (attempt_started.elapsed().as_millis() as u64).into();
		evidence["ended_unix_ms"] = (unix_ms() as u64).into();
		write_probe_evidence(&spec.attempt_dir, evidence);
	};
	let remaining =
		|deadline: std::time::Duration| deadline.saturating_sub(attempt_started.elapsed());

	// 1. The producer stage, inside the attempt's deadline.
	let producers_run = match tokio::time::timeout(
		remaining(spec.deadline),
		scheduler.dispatch_loop(&stages[..1], &mut dispatcher),
	)
	.await
	{
		Ok(Ok(run)) => run,
		Ok(Err(error)) => {
			evidence["error"] = format!("{error:?}").into();
			return finish(&mut evidence, "producer_stage_error");
		}
		Err(_) => return finish(&mut evidence, "producer_stage_deadline"),
	};
	evidence["producer_manifests_before_kill"] = producers_run
		.accepted_manifests
		.values()
		.map(manifest_evidence)
		.collect::<Vec<_>>()
		.into();

	// 2b. ADR 626 Stage A: a dispatch-target victim, bound before the kill.
	let dispatch_victim = if dispatch_target {
		let producers = producers_run
			.accepted_manifests
			.values()
			.map(|manifest| manifest.producer_worker_id)
			.collect::<HashSet<_>>();
		let mut idle_remote = spec
			.workers
			.iter()
			.filter(|(worker_id, _)| {
				remote.contains(&worker_id.get().as_u128().to_string())
					&& !producers.contains(worker_id)
			})
			.cloned()
			.collect::<Vec<_>>();
		idle_remote.sort_by_key(|(worker_id, _)| worker_id.get().as_u128());
		let Some((worker_id, endpoint)) = idle_remote.first().cloned() else {
			return finish(&mut evidence, "no_remote_idle_worker");
		};
		evidence["victim"] = serde_json::json!({
			"role": "dispatch_target",
			"worker": worker_id.get().as_u128().to_string(),
			"endpoint": endpoint.as_str(),
			"accepted_manifests_held": 0,
		});
		Some(worker_id)
	} else {
		None
	};

	// 2. Bind the victim: one accepted sealed manifest from a remote worker.
	let mut candidates = producers_run
		.accepted_manifests
		.iter()
		.filter(|(_, manifest)| {
			remote.contains(&manifest.producer_worker_id.get().as_u128().to_string())
		})
		.collect::<Vec<_>>();
	candidates.sort_by_key(|(key, _)| format!("{key:?}"));
	let producer_victim = candidates
		.first()
		.map(|(key, manifest)| ((*key).clone(), (*manifest).clone()));
	let victim_worker = match (dispatch_victim, &producer_victim) {
		(Some(worker_id), _) => worker_id,
		(None, Some((victim_key, victim_manifest))) => {
			evidence["victim"] = serde_json::json!({
				"task": task_fields(victim_key.query_id, victim_key.stage_id.get(), victim_key.task_id.get(), victim_manifest.attempt_id),
				"worker": victim_manifest.producer_worker_id.get().as_u128().to_string(),
				"endpoint": victim_manifest.producer_endpoint.as_str(),
			});
			victim_manifest.producer_worker_id
		}
		(None, None) => return finish(&mut evidence, "no_remote_sealed_producer"),
	};

	// 3. The bounded kill: the harness's command for exactly this worker, output preserved.
	let victim_name = victim_worker.get().as_u128().to_string();
	let kill_args = spec.raw["kill_commands"][victim_name.as_str()]
		.as_array()
		.unwrap_or_else(|| panic!("no kill command for victim worker {victim_name}"))
		.iter()
		.map(|arg| arg.as_str().expect("kill arg").to_string())
		.collect::<Vec<_>>();
	evidence["kill_command"] = kill_args.clone().into();
	let kill_started = std::time::Instant::now();
	let kill = Command::new(&kill_args[0])
		.args(&kill_args[1..])
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.kill_on_drop(true)
		.spawn()
		.expect("spawn kill command");
	let kill_output =
		tokio::time::timeout(std::time::Duration::from_secs(20), kill.wait_with_output()).await;
	evidence["kill_elapsed_ms"] = (kill_started.elapsed().as_millis() as u64).into();
	match kill_output {
		Ok(Ok(output)) => {
			evidence["kill_status"] = output.status.code().map(i64::from).into();
			evidence["kill_stdout"] = String::from_utf8_lossy(&output.stdout).to_string().into();
			evidence["kill_stderr"] = String::from_utf8_lossy(&output.stderr).to_string().into();
			// The host primitive exits 0 only once the group is gone, and says so in its JSON line.
			let confirmed = serde_json::from_slice::<serde_json::Value>(output.stdout.trim_ascii())
				.ok()
				.and_then(|line| line["gone"].as_bool())
				== Some(true);
			if !output.status.success() || !confirmed {
				return finish(&mut evidence, "kill_not_confirmed");
			}
		}
		Ok(Err(error)) => {
			evidence["kill_error"] = error.to_string().into();
			return finish(&mut evidence, "kill_not_confirmed");
		}
		Err(_) => return finish(&mut evidence, "kill_deadline"),
	}
	let confirmed = std::time::Instant::now();
	evidence["kill_confirmed_unix_ms"] = (unix_ms() as u64).into();
	if let Some(log) = &spec.wait_log {
		log.emit(
			"victim_killed",
			"dispatch",
			serde_json::json!({ "worker": victim_worker.get().as_u128().to_string() }),
		);
	}

	// 4. Recovery.
	let bound = if announced {
		remaining(spec.deadline)
	} else {
		std::time::Duration::from_secs(
			spec.raw["unannounced_bound_secs"]
				.as_u64()
				.expect("unannounced_bound_secs"),
		)
	};
	if announced {
		let Some((victim_key, victim_manifest)) = producer_victim else {
			return finish(&mut evidence, "no_remote_sealed_producer");
		};
		let invalidated = scheduler.mark_worker_lost(victim_worker);
		let bound_task = invalidated.producers.iter().find(|task| {
			task.key == victim_key && task.invalidated_attempt == victim_manifest.attempt_id
		});
		evidence["invalidated"] = invalidated
			.producers
			.iter()
			.map(|task| serde_json::json!({
				"task": task_fields(task.key.query_id, task.key.stage_id.get(), task.key.task_id.get(), task.invalidated_attempt),
				"replacement_attempt": task.replacement_attempt.get(),
				"worker": task.worker_id.get().as_u128().to_string(),
			}))
			.collect::<Vec<_>>()
			.into();
		let Some(bound_task) = bound_task.cloned() else {
			return finish(&mut evidence, "invalidation_does_not_name_the_victim");
		};
		evidence["replacement_attempt"] = bound_task.replacement_attempt.get().into();
		match tokio::time::timeout(bound, scheduler.dispatch_loop(&stages, &mut dispatcher)).await {
			Ok(Ok(run)) => {
				evidence["since_confirmed_kill_ms"] =
					(confirmed.elapsed().as_millis() as u64).into();
				let replaced =
					run.dispatched
						.contains(&toron::distributed::identity::TaskIdentity::new(
							victim_key.query_id,
							victim_key.stage_id,
							victim_key.task_id,
							bound_task.replacement_attempt,
						));
				evidence["replacement_dispatched"] = replaced.into();
				evidence["run"] = run_evidence(&run);
				finish(
					&mut evidence,
					if replaced {
						"completed"
					} else {
						"completed_without_bound_replacement"
					},
				);
			}
			Ok(Err(error)) => {
				evidence["since_confirmed_kill_ms"] =
					(confirmed.elapsed().as_millis() as u64).into();
				evidence["error"] = format!("{error:?}").into();
				finish(&mut evidence, "error");
			}
			Err(_) => finish(&mut evidence, "deadline"),
		}
	} else {
		match tokio::time::timeout(bound, scheduler.dispatch_loop(&stages, &mut dispatcher)).await {
			Ok(Ok(run)) => {
				evidence["since_confirmed_kill_ms"] =
					(confirmed.elapsed().as_millis() as u64).into();
				evidence["run"] = run_evidence(&run);
				finish(&mut evidence, "completed_within_bound");
			}
			Ok(Err(error)) => {
				evidence["since_confirmed_kill_ms"] =
					(confirmed.elapsed().as_millis() as u64).into();
				evidence["error"] = format!("{error:?}").into();
				finish(&mut evidence, "error_within_bound");
			}
			Err(_) => finish(&mut evidence, "no_terminal_return_within_bound"),
		}
	}
}

// ---- ADR 626 Stage A: sized-payload shuffle and the local loss-wait diagnostic ----

/// The sized payload join moves the payload across QUIC workers (at least its raw `pad` bytes are
/// pulled) while matching nothing, so a survey pull measures transfer, not result volume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sized_payload_join_shuffles_the_payload_over_quic() {
	with_quic(async {
		let mut workers = Vec::new();
		for id in 62_611..=62_613u128 {
			workers.push(FixtureWorkerProcess::spawn(WorkerId::from_u128(id)).await);
		}
		let registry = Arc::new(RoundRobinRegistry::new(
			workers
				.iter()
				.map(|worker| worker_entry_for(worker.worker_id, worker.endpoint.clone())),
			0,
		));
		let mut scheduler = DistributedScheduler::new(registry);
		let dispatcher = NetworkTaskDispatcher::new(
			Arc::new(MemoryManager::new(64, 1024 * 1024)),
			process_transport(),
		);
		let stages = fragment_plan(
			probe_case_plan("sized_16k").await,
			FragmentPlanConfig::new_for_test(
				QueryId::new(),
				TaskBudget::from_query_budget(QueryBudget::test_default()),
			),
		)
		.expect("fragment sized payload join");
		let run = bounded(scheduler.dispatch_loop(&stages, &dispatcher))
			.await
			.expect("sized payload join completes");
		let rows = run
			.final_results
			.iter()
			.map(RecordBatch::num_rows)
			.sum::<usize>();
		assert_eq!(rows, 0, "payload keys never match a customer");
		let pulled = run
			.telemetry
			.task_telemetry
			.iter()
			.map(|record| record.telemetry.counters.pulled_shuffle_bytes)
			.sum::<u64>();
		assert!(
			pulled >= 16 * 1024 * 64,
			"the payload's pad bytes must cross the shuffle: pulled {pulled}"
		);
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

/// Unannounced loss on loopback, observed passively: three QUIC fixture workers write wait events,
/// stage 0 seals, the victim is bound and `SIGKILL`ed, and the full plan is then dispatched for at
/// most the declared window. Nothing is asserted about recovery: the evidence (`local.json`, the
/// probe's and each worker's events) is the result. `TORON_ADR626_LOCAL_DIR` must not exist yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ADR 626 Stage A diagnostic: needs TORON_ADR626_LOCAL_DIR; observes for up to its window"]
async fn unannounced_loss_waits_are_observed_on_loopback() {
	let dir = std::path::PathBuf::from(
		std::env::var("TORON_ADR626_LOCAL_DIR").expect("TORON_ADR626_LOCAL_DIR"),
	);
	std::fs::create_dir(&dir).expect("a fresh diagnostic directory");
	let mode = std::env::var("TORON_ADR626_LOCAL_MODE").unwrap_or_else(|_| "producer".to_string());
	assert!(
		mode == "producer" || mode == "dispatch_target",
		"TORON_ADR626_LOCAL_MODE must be producer or dispatch_target, got {mode}"
	);
	let window = std::time::Duration::from_secs(
		std::env::var("TORON_ADR626_LOCAL_WINDOW_SECS")
			.map(|value| value.parse().expect("window seconds"))
			.unwrap_or(360),
	);
	with_quic(async {
		let mut workers = Vec::new();
		for id in 62_601..=62_603u128 {
			workers.push(
				FixtureWorkerProcess::spawn_recorded(
					WorkerId::from_u128(id),
					Some(&dir.join(format!("dials-{id}.txt"))),
					Some(&dir.join(format!("worker-{id}.jsonl"))),
				)
				.await,
			);
		}
		let log = wait_events::EventLog::create(&dir.join("probe_events.jsonl"))
			.expect("fresh probe events");
		let transport: Arc<dyn ToronTransport> = Arc::new(wait_events::ObservedTransport {
			inner: process_transport(),
			log: log.clone(),
			role: "dispatch",
		});
		let dispatcher = ObservedDispatcher {
			inner: NetworkTaskDispatcher::new(
				Arc::new(MemoryManager::new(64, 1024 * 1024)),
				transport,
			),
			log: Some(log.clone()),
		};
		let registry = Arc::new(RoundRobinRegistry::new(
			workers
				.iter()
				.map(|worker| worker_entry_for(worker.worker_id, worker.endpoint.clone())),
			0,
		));
		let mut scheduler = DistributedScheduler::new(registry);
		let stages = fragment_plan(
			probe_case_plan("hash_join").await,
			FragmentPlanConfig::new_for_test(
				QueryId::new(),
				TaskBudget::from_query_budget(QueryBudget::test_default()),
			),
		)
		.expect("fragment hash join");
		let mut evidence = serde_json::json!({
			"mode": mode,
			"window_secs": window.as_secs(),
			"workers": workers.iter().map(|worker| serde_json::json!({
				"worker": worker.worker_id.get().as_u128().to_string(),
				"endpoint": worker.endpoint.as_str(),
			})).collect::<Vec<_>>(),
			"started_unix_ms": unix_ms() as u64,
		});
		let write = |evidence: &serde_json::Value| {
			use std::io::Write as _;
			std::fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(dir.join("local.json"))
				.expect("fresh local evidence")
				.write_all(
					serde_json::to_string_pretty(evidence)
						.expect("json")
						.as_bytes(),
				)
				.expect("write local evidence");
		};

		let producers_run = match tokio::time::timeout(
			std::time::Duration::from_secs(60),
			scheduler.dispatch_loop(&stages[..1], &dispatcher),
		)
		.await
		{
			Ok(Ok(run)) => run,
			other => {
				evidence["outcome"] = "producer_stage_failed".into();
				evidence["detail"] = format!("{:?}", other.map(|result| result.map(|_| ()))).into();
				return write(&evidence);
			}
		};
		let producers = producers_run
			.accepted_manifests
			.values()
			.map(|manifest| manifest.producer_worker_id)
			.collect::<HashSet<_>>();
		evidence["producer_manifests_before_kill"] = producers_run
			.accepted_manifests
			.values()
			.map(manifest_evidence)
			.collect::<Vec<_>>()
			.into();
		let mut candidates = workers
			.iter()
			.map(|worker| worker.worker_id)
			.filter(|worker_id| producers.contains(worker_id) == (mode == "producer"))
			.collect::<Vec<_>>();
		candidates.sort_by_key(|worker_id| worker_id.get().as_u128());
		let Some(victim) = candidates.first().copied() else {
			evidence["outcome"] = "no_victim_for_mode".into();
			return write(&evidence);
		};
		evidence["victim"] = victim.get().as_u128().to_string().into();
		let victim_process = workers
			.iter_mut()
			.find(|worker| worker.worker_id == victim)
			.expect("victim process");
		evidence["victim_endpoint"] = victim_process.endpoint.as_str().into();
		// `Child::kill` sends SIGKILL and waits; the exit status confirms the process is gone.
		let kill = victim_process.child.kill().await;
		let status = victim_process.child.try_wait();
		evidence["kill"] = serde_json::json!({
			"result": match &kill { Ok(()) => "ok".to_string(), Err(error) => error.to_string() },
			"exit_status": match &status { Ok(Some(status)) => format!("{status}"), other => format!("{other:?}") },
			"confirmed": kill.is_ok() && matches!(status, Ok(Some(_))),
		});
		if !(kill.is_ok() && matches!(status, Ok(Some(_)))) {
			evidence["outcome"] = "kill_not_confirmed".into();
			return write(&evidence);
		}
		let killed = std::time::Instant::now();
		evidence["kill_confirmed_unix_ms"] = (unix_ms() as u64).into();
		log.emit(
			"victim_killed",
			"dispatch",
			serde_json::json!({ "worker": victim.get().as_u128().to_string() }),
		);

		let outcome =
			tokio::time::timeout(window, scheduler.dispatch_loop(&stages, &dispatcher)).await;
		evidence["since_kill_ms"] = (killed.elapsed().as_millis() as u64).into();
		match outcome {
			Ok(Ok(run)) => {
				evidence["outcome"] = "completed".into();
				evidence["run"] = run_evidence(&run);
			}
			Ok(Err(error)) => {
				evidence["outcome"] = "error".into();
				evidence["error"] = format!("{error:?}").into();
			}
			Err(_) => evidence["outcome"] = "window_elapsed".into(),
		}
		log.emit(
			"observation_end",
			"dispatch",
			serde_json::json!({ "outcome": evidence["outcome"].clone() }),
		);
		evidence["ended_unix_ms"] = (unix_ms() as u64).into();
		for worker in &mut workers {
			worker.shutdown().await;
		}
		write(&evidence);
	})
	.await;
}

// ---- ADR 625: host-mode fixtures (local, no network peers) ----

/// Starts the fixture worker with `args` and returns (exit success, stdout, stderr), bounded.
async fn fixture_worker_exit(args: &[&str]) -> (bool, String, String) {
	bounded(async {
		let output = Command::new(env!("CARGO_BIN_EXE_toron_fixture_worker"))
			.args(args)
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.kill_on_drop(true)
			.output()
			.await
			.expect("run fixture worker");
		(
			output.status.success(),
			String::from_utf8_lossy(&output.stdout).to_string(),
			String::from_utf8_lossy(&output.stderr).to_string(),
		)
	})
	.await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_mode_requires_every_host_flag_together() {
	let (ok, stdout, stderr) =
		fixture_worker_exit(&["--worker-u128", "9000", "--bind", "0.0.0.0:7701"]).await;
	assert!(
		!ok && !stdout.contains("READY"),
		"a partial host mode must not start"
	);
	assert!(stderr.contains("host mode needs"), "{stderr}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_mode_refuses_a_loopback_or_unspecified_advertised_address() {
	for advertise in ["127.0.0.1:7701", "0.0.0.0:7701", "192.0.2.1:0"] {
		let (ok, stdout, stderr) = fixture_worker_exit(&[
			"--worker-u128",
			"9000",
			"--bind",
			"0.0.0.0:0",
			"--advertise",
			advertise,
			"--tls-cert",
			"/nonexistent/c",
			"--tls-key",
			"/nonexistent/k",
			"--tls-ca",
			"/nonexistent/ca",
			"--cluster-domain",
			"cluster.local",
		])
		.await;
		assert!(
			!ok && !stdout.contains("READY"),
			"{advertise} must be refused"
		);
		assert!(
			stderr.contains("--advertise must be a routable"),
			"{advertise}: {stderr}"
		);
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_mode_refuses_a_listener_certificate_for_another_worker_id() {
	let dir = std::env::temp_dir().join(format!("toron-adr625-pki-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&dir);
	std::fs::create_dir_all(&dir).expect("pki dir");
	let ca = tls_pki::issue::generate_ca("adr625 test ca").expect("ca");
	let ca_cert =
		tls_pki::issue::load_ca(&ca.cert, ca.key.as_deref().expect("ca key")).expect("load ca");
	let other = WorkerId::from_u128(9_001);
	let leaf = tls_pki::issue::issue_listener(&ca_cert, &other.get().to_string(), "cluster.local")
		.expect("listener for another worker");
	write_new_with_mode(&dir.join("ca.crt"), &ca.cert, 0o644);
	write_new_with_mode(&dir.join("listener.crt"), &leaf.cert, 0o644);
	write_new_with_mode(
		&dir.join("listener.key"),
		leaf.key.as_deref().expect("listener key"),
		0o600,
	);
	let (ok, stdout, stderr) = fixture_worker_exit(&[
		"--worker-u128",
		"9002",
		"--bind",
		"0.0.0.0:0",
		"--advertise",
		"192.0.2.1:7701",
		"--tls-cert",
		dir.join("listener.crt").to_str().unwrap(),
		"--tls-key",
		dir.join("listener.key").to_str().unwrap(),
		"--tls-ca",
		dir.join("ca.crt").to_str().unwrap(),
		"--cluster-domain",
		"cluster.local",
	])
	.await;
	let _ = std::fs::remove_dir_all(&dir);
	assert!(
		!ok && !stdout.contains("READY"),
		"the worker must not start or listen"
	);
	assert!(stderr.contains("STARTUP_REJECTED"), "{stderr}");
	assert!(!stderr.contains("LISTENING"), "{stderr}");
}

#[test]
#[should_panic(expected = "probe evidence must be a fresh file")]
fn probe_evidence_is_never_overwritten() {
	let dir = std::env::temp_dir().join(format!("toron-adr625-evidence-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&dir);
	std::fs::create_dir_all(&dir).expect("evidence dir");
	write_probe_evidence(&dir, &serde_json::json!({"attempt": 1}));
	write_probe_evidence(&dir, &serde_json::json!({"attempt": 2}));
}

/// Create `path` new, with `mode` applied at creation (never widened then narrowed).
fn write_new_with_mode(path: &std::path::Path, text: &str, mode: u32) {
	use std::io::Write as _;
	use std::os::unix::fs::OpenOptionsExt as _;
	let mut file = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(mode)
		.open(path)
		.unwrap_or_else(|error| panic!("{}: {error}", path.display()));
	file.write_all(text.as_bytes()).expect("write pki file");
}

fn create_private_dir(path: &std::path::Path) {
	use std::os::unix::fs::DirBuilderExt as _;
	std::fs::DirBuilder::new()
		.mode(0o700)
		.create(path)
		.unwrap_or_else(|error| panic!("{}: {error}", path.display()));
}

/// The campaign's identities: (directory, presenting CA, trusted root CA, worker id in the SAN).
/// The presented certificate and the trusted root are independent, so a wrong-CA case is rejected
/// by exactly one side: `probe-b` presents CA-B but trusts CA-A (a wrong client against CA-A
/// workers); `strix-b` presents CA-B but trusts CA-A (a wrong server that still accepts CA-A
/// clients).
const MULTINODE_IDENTITIES: [(&str, usize, usize, u128); 11] = [
	("ref1", 0, 0, 70_101),
	("ref2", 0, 0, 70_102),
	("ref3", 0, 0, 70_103),
	("bigfoot", 0, 0, 70_201),
	("strix", 0, 0, 70_202),
	("optiplex", 0, 0, 70_203),
	("probe", 0, 0, 70_900),
	("wrong-id", 0, 0, 70_299),
	("probe-b", 1, 0, 70_901),
	("strix-b", 1, 0, 70_202),
	// The positive control for `strix-b`: presents CA-A (which `strix-b` accepts) and trusts CA-B
	// (which `strix-b` presents), so `strix-b` must serve it.
	("probe-trusts-b", 0, 1, 70_902),
];

/// Issue the campaign's PKI under `root` (which must not exist). Each identity directory (0700)
/// holds its certificate, its key (created 0600) and exactly one trust root; CA keys stay under
/// `ca-a/` and `ca-b/`. Returns the SHA-256 of every certificate and root.
fn issue_multinode_pki(root: &std::path::Path) -> serde_json::Map<String, serde_json::Value> {
	let domain = "cluster.local";
	create_private_dir(root);
	let mut fingerprints = serde_json::Map::new();
	let mut cas = Vec::new();
	for name in ["ca-a", "ca-b"] {
		let ca = tls_pki::generate_ca(&format!("adr625 {name}")).expect("ca");
		create_private_dir(&root.join(name));
		write_new_with_mode(&root.join(name).join("ca.crt"), &ca.cert, 0o644);
		write_new_with_mode(
			&root.join(name).join("ca.key"),
			ca.key.as_deref().expect("ca key"),
			0o600,
		);
		fingerprints.insert(
			format!("{name}/ca.crt"),
			sha256_hex(ca.cert.as_bytes()).into(),
		);
		let signer =
			tls_pki::load_ca(&ca.cert, ca.key.as_deref().expect("ca key")).expect("load ca");
		cas.push((name, ca.cert, signer));
	}
	for (dir, presenting, trusting, worker_u128) in MULTINODE_IDENTITIES {
		let issued = tls_pki::issue_listener(
			&cas[presenting].2,
			&WorkerId::from_u128(worker_u128).get().to_string(),
			domain,
		)
		.expect("listener");
		create_private_dir(&root.join(dir));
		write_new_with_mode(&root.join(dir).join("listener.crt"), &issued.cert, 0o644);
		write_new_with_mode(
			&root.join(dir).join("listener.key"),
			issued.key.as_deref().expect("key"),
			0o600,
		);
		write_new_with_mode(&root.join(dir).join("ca.crt"), &cas[trusting].1, 0o644);
		fingerprints.insert(
			format!(
				"{dir}/listener.crt (presents {}, worker {worker_u128})",
				cas[presenting].0
			),
			sha256_hex(issued.cert.as_bytes()).into(),
		);
		fingerprints.insert(
			format!("{dir}/ca.crt (trusts {})", cas[trusting].0),
			sha256_hex(cas[trusting].1.as_bytes()).into(),
		);
	}
	// A CA-A `client-<tenant>` identity (an SQL client, not `toron-*`) that trusts CA-A.
	let csr_key = rcgen::Certificate::from_params(rcgen::CertificateParams::new(Vec::new()))
		.expect("csr key");
	let csr = csr_key.serialize_request_pem().expect("csr");
	let client = tls_pki::sign_client_csr(&cas[0].2, &csr, "7", domain).expect("client cert");
	create_private_dir(&root.join("client-tenant"));
	write_new_with_mode(
		&root.join("client-tenant").join("listener.crt"),
		&client.cert,
		0o644,
	);
	write_new_with_mode(
		&root.join("client-tenant").join("listener.key"),
		&csr_key.serialize_private_key_pem(),
		0o600,
	);
	write_new_with_mode(&root.join("client-tenant").join("ca.crt"), &cas[0].1, 0o644);
	fingerprints.insert(
		"client-tenant/listener.crt (presents ca-a, client-7)".into(),
		sha256_hex(client.cert.as_bytes()).into(),
	);
	fingerprints.insert(
		"client-tenant/ca.crt (trusts ca-a)".into(),
		sha256_hex(cas[0].1.as_bytes()).into(),
	);
	write_new_with_mode(
		&root.join("fingerprints.json"),
		&serde_json::to_string_pretty(&fingerprints).expect("fingerprints"),
		0o644,
	);
	fingerprints
}

/// ADR 625: issue the campaign's PKI into `TORON_MULTINODE_PKI_DIR` (which must not exist).
#[test]
#[ignore = "multinode PKI issuance: needs TORON_MULTINODE_PKI_DIR"]
fn multinode_issue_pki() {
	let root = std::path::PathBuf::from(
		std::env::var("TORON_MULTINODE_PKI_DIR").expect("TORON_MULTINODE_PKI_DIR"),
	);
	issue_multinode_pki(&root);
}

fn sha256_hex(bytes: &[u8]) -> String {
	aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes)
		.as_ref()
		.iter()
		.map(|byte| format!("{byte:02x}"))
		.collect()
}

/// ADR 625, before any host use: the decoupled wrong-CA arrangements behave locally as intended.
/// A host-mode worker binds loopback (advertising a placeholder routable address) and is dialled
/// through the real mTLS path (an authenticated endpoint with the worker's canonical server name),
/// not through `QuicTransport`'s unauthenticated loopback shortcut. Each rejection is observed on
/// its intended side, and a rejected client never reaches an application stream on the worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decoupled_wrong_ca_arrangements_are_rejected_by_the_intended_side() {
	let root = std::env::temp_dir().join(format!("toron-adr625-mtls-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&root);
	issue_multinode_pki(&root);
	let pki = |identity: &str, file: &str| root.join(identity).join(file);

	// (worker identity, worker id, client identity, expected outcome)
	let cases = [
		("strix", 70_202u128, "probe", "acknowledged"),
		("strix", 70_202, "probe-b", "rejected"),
		("strix-b", 70_202, "probe", "rejected"),
		("strix", 70_202, "client-tenant", "rejected"),
		("strix-b", 70_202, "probe-trusts-b", "acknowledged"),
	];
	for (worker_identity, worker_u128, client_identity, expected) in cases {
		let port = std::net::UdpSocket::bind("127.0.0.1:0")
			.expect("free port")
			.local_addr()
			.expect("port")
			.port();
		let stderr_path = root.join(format!("{worker_identity}-{client_identity}.stderr"));
		let mut worker = Command::new(env!("CARGO_BIN_EXE_toron_fixture_worker"))
			.args(["--worker-u128", &worker_u128.to_string()])
			.args([
				"--bind",
				&format!("127.0.0.1:{port}"),
				"--advertise",
				&format!("192.0.2.1:{port}"),
			])
			.args([
				"--tls-cert",
				pki(worker_identity, "listener.crt").to_str().unwrap(),
			])
			.args([
				"--tls-key",
				pki(worker_identity, "listener.key").to_str().unwrap(),
			])
			.args(["--tls-ca", pki(worker_identity, "ca.crt").to_str().unwrap()])
			.args(["--cluster-domain", "cluster.local"])
			.stdout(Stdio::piped())
			.stderr(std::fs::File::create(&stderr_path).expect("stderr file"))
			.kill_on_drop(true)
			.spawn()
			.expect("spawn host-mode worker");
		let mut line = String::new();
		bounded(BufReader::new(worker.stdout.take().unwrap()).read_line(&mut line))
			.await
			.expect("readiness");
		assert!(line.starts_with("READY "), "{worker_identity}: {line}");

		let client = toron::network::tls::client_config_from_paths(
			&pki(client_identity, "listener.crt"),
			&pki(client_identity, "listener.key"),
			&pki(client_identity, "ca.crt"),
		)
		.expect("client material");
		let endpoint =
			toron::quic::QuicEndpoint::client_authenticated(client).expect("client endpoint");
		let server_name = toron::network::tls::canonical_daemon_dns_name(
			&WorkerId::from_u128(worker_u128),
			"cluster.local",
		);
		let address = toron::distributed::task::PartitionAddress {
			query_id: QueryId::new(),
			exchange_id: toron::distributed::identity::ExchangeId::new(0),
			producer_stage_id: toron::distributed::identity::StageId::new(0),
			producer_task_id: toron::distributed::identity::TaskId::new(0),
			attempt_id: AttemptId::first(),
			partition_id: toron::distributed::identity::PartitionId::new(0),
		};
		let client_port = endpoint.local_addr().expect("client port").port();
		let dial_and_pull = |endpoint: toron::quic::QuicEndpoint| {
			let server_name = server_name.clone();
			async move {
				let stream = endpoint
					.connect_stream_with_server_name(
						format!("127.0.0.1:{port}").parse().unwrap(),
						&server_name,
					)
					.await
					.map_err(|error| format!("dial: {error}"))?;
				toron::server::ToronCoordinator::new(Arc::new(MemoryManager::new(4, 1024 * 1024)))
					.pull_partition_from_worker(stream, address, AttemptId::first())
					.await
					.map(|_| "served".to_string())
					.map_err(|error| format!("pull: {error}"))
			}
		};
		let outcome = bounded(dial_and_pull(endpoint.clone())).await;
		// A valid client right after: the worker must still serve (a rejection does not wedge it).
		let valid = toron::network::tls::client_config_from_paths(
			&pki("probe", "listener.crt"),
			&pki("probe", "listener.key"),
			&pki("probe", "ca.crt"),
		)
		.expect("valid client material");
		let valid_endpoint =
			toron::quic::QuicEndpoint::client_authenticated(valid).expect("valid endpoint");
		let valid_port = valid_endpoint.local_addr().expect("valid port").port();
		let follow_up = bounded(dial_and_pull(valid_endpoint)).await;
		tokio::time::sleep(std::time::Duration::from_millis(300)).await;
		let _ = worker.kill().await;
		let _ = worker.wait().await;
		let log = std::fs::read_to_string(&stderr_path).expect("worker stderr");
		let accepted_from = |port: u16| {
			log.lines().any(|line| {
				line.starts_with("STREAM_ACCEPTED") && line.ends_with(&format!(":{port}"))
			})
		};
		let partition_missing = |result: &Result<String, String>| matches!(result, Err(error) if error.starts_with("pull: partition store mismatch"));
		let tag = format!("{worker_identity} <- {client_identity}");
		if expected == "acknowledged" {
			assert!(partition_missing(&outcome), "{tag}: {outcome:?}");
			assert!(
				accepted_from(client_port),
				"{tag}: no STREAM_ACCEPTED from :{client_port}\n{log}"
			);
		} else {
			assert!(
				!partition_missing(&outcome),
				"{tag} reached the application: {outcome:?}"
			);
			assert!(outcome.is_err(), "{tag}: {outcome:?}");
			assert!(
				!accepted_from(client_port),
				"{tag}: the rejected client got a stream\n{log}"
			);
			let side = match &outcome {
				Err(error) if error.starts_with("dial:") => "client refused the server at the dial",
				_ => "server ended the connection after the client's dial",
			};
			println!("{tag}: {side}: {outcome:?}");
		}
		// Where the worker itself judged the client (it presents CA-A), a valid client right after must
		// still be served. A worker presenting CA-B is refused by every CA-A client, by design.
		if worker_identity != "strix-b" {
			assert!(
				partition_missing(&follow_up),
				"{tag}: the valid follow-up was not served: {follow_up:?}"
			);
			assert!(
				accepted_from(valid_port),
				"{tag}: no STREAM_ACCEPTED for the valid follow-up\n{log}"
			);
		}
	}
	let _ = std::fs::remove_dir_all(&root);
}
