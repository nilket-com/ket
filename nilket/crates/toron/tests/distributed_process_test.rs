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

#[path = "support/lease_faults.rs"]
mod lease_faults;
#[path = "support/reliance_faults.rs"]
mod reliance_faults;
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
	/// ADR 626 Stage C: a relayed worker's loopback bind and advertised addresses.
	static LOOPBACK_ADDRESSES: (std::net::SocketAddr, std::net::SocketAddr);
	/// ADR 628: workers spawned in this scope run the managed (production) store lifecycle with
	/// its one-second reaper.
	static MANAGED_LIFECYCLE: ();
	/// ADR 628 B1.C: workers spawned in this scope run the long table's scan for this many ms
	/// (`--long-scan-ms`).
	static LONG_SCAN_MS: u64;
	/// ADR 628 B1.C: the instrument installed on the production coordinator path in `adr627_run`.
	static RELIANCE_INSTRUMENT: RelianceInstrument;
}

/// ADR 628 B1.C: the coordinator-side instrument: the reliance observer (seam S2) and, for a
/// renewal-path control, the renewal transport (seam S1).
#[derive(Clone)]
struct RelianceInstrument {
	observer: Arc<dyn toron::distributed::query_reliance::RelianceObserver>,
	renewal_transport: Option<Arc<dyn ToronTransport>>,
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
		Self::spawn_configured(worker_id, record_peer_dials, record_wait_events, None).await
	}

	/// ADR 626 Stage C: a QUIC loopback worker bound at `bind` and advertising `advertise` (a relay).
	async fn spawn_loopback_relayed(
		worker_id: WorkerId,
		bind: std::net::SocketAddr,
		advertise: std::net::SocketAddr,
		recovery: (u64, u64),
	) -> Self {
		LOOPBACK_ADDRESSES
			.scope(
				(bind, advertise),
				Self::spawn_configured(worker_id, None, None, Some(recovery)),
			)
			.await
	}

	/// ADR 626 Stage B: `recovery` (`H` and `C` in ms) enables the recovery experiment.
	async fn spawn_configured(
		worker_id: WorkerId,
		record_peer_dials: Option<&std::path::Path>,
		record_wait_events: Option<&std::path::Path>,
		recovery: Option<(u64, u64)>,
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
		if let Ok((bind, advertise)) = LOOPBACK_ADDRESSES.try_with(|addresses| *addresses) {
			command
				.arg("--loopback-bind")
				.arg(bind.to_string())
				.arg("--loopback-advertise")
				.arg(advertise.to_string());
		}
		if MANAGED_LIFECYCLE.try_with(|()| ()).is_ok() {
			command.arg("--managed-lifecycle");
		}
		if let Ok(ms) = LONG_SCAN_MS.try_with(|ms| *ms) {
			command.arg("--long-scan-ms").arg(ms.to_string());
		}
		if let Some((establishment, confirmation)) = recovery {
			command
				.arg("--recovery-establishment-ms")
				.arg(establishment.to_string())
				.arg("--recovery-confirmation-ms")
				.arg(confirmation.to_string());
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
	for (id, name) in [
		(6u8, "payload_16"),
		(7, "payload_16k"),
		(8, "payload_192k"),
		(9, "slow"),
		(10, "long"),
	] {
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
	/// ADR 626 Stage B: the recovery experiment, when the spec declares one.
	recovery: Option<toron::distributed::recovery::RecoveryExperiment>,
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

/// The spec's transport, unobserved: loopback QUIC for a loopback spec, otherwise authenticated QUIC
/// with the spec's identity. ADR 628 B1.B's survey dials through this directly.
fn spec_transport(raw: &serde_json::Value) -> Arc<dyn ToronTransport> {
	// ADR 628 Stage A: a loopback spec drives loopback fixture workers (the child coordinator of the
	// owner-disappearance control); every host spec stays authenticated.
	if raw["loopback"].as_bool() == Some(true) {
		Arc::new(QuicTransport::new().expect("loopback QUIC transport"))
	} else {
		let client = toron::network::tls::client_config_from_paths(
			std::path::Path::new(&spec_str(raw, "tls_cert")),
			std::path::Path::new(&spec_str(raw, "tls_key")),
			std::path::Path::new(&spec_str(raw, "tls_ca")),
		)
		.expect("probe client TLS material");
		Arc::new(
			QuicTransport::authenticated(client, spec_str(raw, "cluster_domain"))
				.expect("authenticated transport"),
		)
	}
}

fn load_probe_spec() -> ProbeSpec {
	load_probe_spec_from(&std::env::var("TORON_MULTINODE_SPEC").expect("TORON_MULTINODE_SPEC"))
}

fn load_probe_spec_from(path: &str) -> ProbeSpec {
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
	let transport = spec_transport(&raw);
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
	let recovery = raw.get("recovery").map(|recovery| {
		toron::distributed::recovery::RecoveryExperiment::from_millis(
			recovery["establishment_ms"]
				.as_u64()
				.expect("recovery.establishment_ms"),
			recovery["confirmation_ms"]
				.as_u64()
				.expect("recovery.confirmation_ms"),
		)
		.expect("a valid recovery experiment")
	});
	ProbeSpec {
		attempt_dir,
		workers,
		transport,
		deadline: std::time::Duration::from_secs(
			raw["deadline_secs"].as_u64().expect("deadline_secs"),
		),
		wait_log,
		recovery,
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
	let (catalog, logical) = probe_case_logical(case);
	bind(&logical, catalog.as_ref(), &PlannerConfig::default())
		.await
		.expect("bind probe case")
}

/// A named case's catalog and logical plan, unbound: the scheduler path binds it here, and the
/// ADR 627 coordinator path hands it to the production coordinator, which binds it itself.
fn probe_case_logical(case: &str) -> (Arc<MockCatalog>, LogicalPlan) {
	match case {
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
		"slow_scan" => (
			wait_survey_catalog(),
			SqlPlanner::new()
				.plan("SELECT slow.id FROM slow")
				.expect("plan slow scan"),
		),
		// ADR 627: a stage-0 producer that scans the slow table, joined by four consumers.
		"slow_join" => (
			wait_survey_catalog(),
			SqlPlanner::new()
				.plan("SELECT slow.id, customers.region FROM slow JOIN customers ON slow.id = customers.id")
				.expect("plan slow join"),
		),
		// ADR 627: the long healthy coordinator-path case. Its fast `customers` producer and its 30 s
		// `long` producer are dispatched together, so two workers are in the lease census through the
		// long scan (observed on loopback; the third joins with the consumers afterwards).
		"long_join_last" => (
			wait_survey_catalog(),
			SqlPlanner::new()
				.plan("SELECT customers.region, long.id FROM customers JOIN long ON customers.id = long.id")
				.expect("plan long join last"),
		),
		// ADR 628 B1.C: the same query; its workers run the long scan at `--long-scan-ms`.
		"horizon_join_last" => (
			wait_survey_catalog(),
			SqlPlanner::new()
				.plan("SELECT customers.region, long.id FROM customers JOIN long ON customers.id = long.id")
				.expect("plan horizon join last"),
		),
		other => panic!("unknown probe case {other}"),
	}
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
		"recovery_events": recovery_events_evidence(&run.telemetry.recovery_events),
		"task_telemetry": run.telemetry.task_telemetry.iter().map(|record| serde_json::json!({
			"task": task_fields(record.identity.query_id, record.identity.stage_id.get(), record.identity.task_id.get(), record.identity.attempt_id),
			"worker": record.worker_id.get().as_u128().to_string(),
			"pulled_shuffle_bytes": record.telemetry.counters.pulled_shuffle_bytes,
			"published_shuffle_bytes": record.telemetry.counters.published_shuffle_bytes,
		})).collect::<Vec<_>>(),
	})
}

/// ADR 626 Stage B: the scheduler's recovery events as typed evidence (its own clock).
fn recovery_events_evidence(
	events: &[toron::distributed::scheduler::RecoveryEvent],
) -> serde_json::Value {
	use toron::distributed::scheduler::RecoveryEventKind as K;
	let id = |identity: &toron::distributed::identity::TaskIdentity| {
		task_fields(
			identity.query_id,
			identity.stage_id.get(),
			identity.task_id.get(),
			identity.attempt_id,
		)
	};
	let worker = |worker: &WorkerId| worker.get().as_u128().to_string();
	events
		.iter()
		.map(|event| {
			let (name, fields) = match &event.kind {
				K::EstablishmentTimedOut {
					worker: w,
					task,
					failure,
				} => (
					"establishment_timed_out",
					serde_json::json!({ "worker": worker(w), "task": id(task), "failure": failure.to_string() }),
				),
				K::ReportAccepted {
					consumer,
					producer,
					worker: w,
				} => (
					"report_accepted",
					serde_json::json!({ "consumer": id(consumer), "producer": id(producer), "worker": worker(w) }),
				),
				K::ConfirmationStarted { query, worker: w } => (
					"confirmation_started",
					serde_json::json!({ "query": query.get().to_string(), "worker": worker(w) }),
				),
				K::ConfirmationEnded {
					query,
					worker: w,
					outcome,
				} => (
					"confirmation_ended",
					serde_json::json!({ "query": query.get().to_string(), "worker": worker(w), "outcome": confirmation_outcome_evidence(outcome) }),
				),
				K::ConfirmationDiscarded { query, worker: w } => (
					"confirmation_discarded",
					serde_json::json!({ "query": query.get().to_string(), "worker": worker(w) }),
				),
				K::Excluded { query, worker: w } => (
					"excluded",
					serde_json::json!({ "query": query.get().to_string(), "worker": worker(w) }),
				),
				K::LinkRetry { consumer, avoided } => (
					"link_retry",
					serde_json::json!({ "consumer": id(consumer), "avoided": worker(avoided) }),
				),
				K::Superseded { task } => ("superseded", serde_json::json!({ "task": id(task) })),
				K::ReplayDispatched { task, worker: w } => (
					"replay_dispatched",
					serde_json::json!({ "task": id(task), "worker": worker(w) }),
				),
				K::StaleOutcome { task } => {
					("stale_outcome", serde_json::json!({ "task": id(task) }))
				}
			};
			serde_json::json!({ "event": name, "at_micros": event.at_micros, "fields": fields })
		})
		.collect::<Vec<_>>()
		.into()
}

/// ADR 626 Stage C: a confirmation outcome as a typed kind plus its debug text.
fn confirmation_outcome_evidence(
	outcome: &toron::distributed::scheduler::ConfirmationOutcome,
) -> serde_json::Value {
	use toron::distributed::scheduler::ConfirmationOutcome as O;
	let kind = match outcome {
		O::Acknowledged { .. } => "acknowledged",
		O::Liveness { .. } => "liveness",
		O::Refused { .. } => "refused",
		O::Unsupported => "unsupported",
	};
	serde_json::json!({ "kind": kind, "debug": format!("{outcome:?}") })
}

/// ADR 626 Stage C: a scheduler error's variant, so a terminal failure is typed by the record, not
/// by parsing its debug text. Exhaustive: a new variant must be named here.
fn scheduler_error_kind(error: &toron::distributed::scheduler::SchedulerError) -> &'static str {
	use toron::distributed::scheduler::SchedulerError;
	match error {
		SchedulerError::NoLiveWorker => "no_live_worker",
		SchedulerError::TaskFailed { .. } => "task_failed",
		SchedulerError::NoProgress { .. } => "no_progress",
		SchedulerError::PartitionManifestMismatch { .. } => "partition_manifest_mismatch",
		SchedulerError::Acceptance(_) => "acceptance",
		SchedulerError::ProtocolViolation { .. } => "protocol_violation",
		SchedulerError::NoEligibleWorker { .. } => "no_eligible_worker",
		SchedulerError::ReplacementLimit { .. } => "replacement_limit",
		SchedulerError::ExclusionLimit { .. } => "exclusion_limit",
		SchedulerError::ConfirmationRefused { .. } => "confirmation_refused",
	}
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
		DispatchOutcome::WorkerUnreachable {
			failure,
			elapsed_micros,
		} => format!("worker unreachable after {elapsed_micros} us: {failure}"),
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
		// ADR 628 Stage A: a sealed producer's typed manifest, as this dispatcher received it, so a
		// later inspection can bind its exact published addresses, worker and digests.
		use toron::distributed::scheduler::DispatchOutcome as D;
		use toron::distributed::task::TaskCompletionOutput as O;
		let produced = match &outcome {
			D::Completed(O::ShuffleProducer { manifest })
			| D::CompletedWithBatches {
				output: O::ShuffleProducer { manifest },
				..
			}
			| D::CompletedWithTelemetry {
				output: O::ShuffleProducer { manifest },
				..
			} => Some(manifest),
			_ => None,
		};
		if let Some(manifest) = produced {
			log.emit(
				"manifest_received",
				"dispatch",
				serde_json::json!({
					"task": task_fields(manifest.query_id, manifest.stage_id.get(), manifest.task_id.get(), manifest.attempt_id),
					"exchange": manifest.exchange_id.get(),
					"producer_worker": manifest.producer_worker_id.get().as_u128().to_string(),
					"producer_endpoint": manifest.producer_endpoint.as_str(),
					"partitions": manifest.partitions.iter().map(|entry| serde_json::json!({
						"partition": entry.partition_id.get(),
						"rows": entry.row_count.get(),
						"bytes": entry.size_bytes.get(),
						"digest": format!("{:?}", entry.digest),
					})).collect::<Vec<_>>(),
				}),
			);
		}
		outcome
	}

	/// ADR 626: forwarded (never the default), so confirmations reach the real dispatcher.
	async fn confirm_worker(
		&self,
		worker: &WorkerEntry,
		deadline: std::time::Duration,
	) -> toron::distributed::scheduler::ConfirmationOutcome {
		let Some(log) = &self.log else {
			return self.inner.confirm_worker(worker, deadline).await;
		};
		let fields = serde_json::json!({
			"task": format!("confirm/{}", worker.worker_id.get().as_u128()),
			"worker": worker.worker_id.get().as_u128().to_string(),
		});
		let started = log.now_us();
		log.emit_at(started, "dispatch_start", "dispatch", fields.clone());
		let outcome = self.inner.confirm_worker(worker, deadline).await;
		let ended = log.now_us();
		let mut fields = fields;
		fields["elapsed_us"] = ended.saturating_sub(started).into();
		fields["outcome"] = format!("{outcome:?}").into();
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
	let mut scheduler = DistributedScheduler::new(registry.clone());
	let mut network = NetworkTaskDispatcher::new(
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		spec.transport.clone(),
	);
	if let Some(experiment) = spec.recovery {
		scheduler = scheduler.with_recovery_experiment(experiment);
		network = network.with_recovery_experiment(experiment);
	}
	let dispatcher = ObservedDispatcher {
		inner: network,
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
	// ADR 626 Stage C: the scheduler's recovery events, whatever the outcome.
	evidence["recovery_events"] = recovery_events_evidence(scheduler.recovery_events());
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
		if spec.recovery.is_some() {
			evidence["recovery_experiment"] = spec.raw["recovery"].clone();
		}
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
	// ADR 626 Stage C: the scheduler clock at the confirmed kill, so recovery events can be stated
	// relative to it on one clock.
	evidence["scheduler_clock_at_kill_micros"] = scheduler.recovery_clock_micros().into();
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
		let outcome =
			tokio::time::timeout(bound, scheduler.dispatch_loop(&stages, &mut dispatcher)).await;
		// ADR 626 Stage C: the scheduler's recovery events, whatever the outcome.
		evidence["recovery_events"] = recovery_events_evidence(scheduler.recovery_events());
		match outcome {
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
		let outcome =
			tokio::time::timeout(bound, scheduler.dispatch_loop(&stages, &mut dispatcher)).await;
		// ADR 626 Stage C: the scheduler's recovery events, whatever the outcome.
		evidence["recovery_events"] = recovery_events_evidence(scheduler.recovery_events());
		match outcome {
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
				evidence["error_kind"] = scheduler_error_kind(&error).into();
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

// ---- ADR 626 Stage B: query-local recovery over loopback QUIC ----

/// Fast experiment values for the local tests (the campaign uses the declared 5 s and 12 s).
const ADR626_LOCAL_RECOVERY: (u64, u64) = (1_000, 3_000);
/// The fixture's outcome deadline from the confirmed kill.
const ADR626_R: std::time::Duration = std::time::Duration::from_secs(30);

async fn adr626_workers(first_id: u128, recovery: Option<(u64, u64)>) -> Vec<FixtureWorkerProcess> {
	let mut workers = Vec::new();
	for id in first_id..first_id + 3 {
		workers.push(
			FixtureWorkerProcess::spawn_configured(WorkerId::from_u128(id), None, None, recovery)
				.await,
		);
	}
	workers
}

fn adr626_registry(workers: &[FixtureWorkerProcess]) -> Arc<RoundRobinRegistry> {
	Arc::new(RoundRobinRegistry::new(
		workers
			.iter()
			.map(|worker| worker_entry_for(worker.worker_id, worker.endpoint.clone())),
		0,
	))
}

async fn adr626_hash_join_stages() -> Vec<toron::distributed::scheduler::StagePlan> {
	fragment_plan(
		probe_case_plan("hash_join").await,
		FragmentPlanConfig::new_for_test(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)
	.expect("fragment hash join")
}

fn adr626_digest(run: &SchedulerRun) -> String {
	format!(
		"{:?}",
		canonical_arrow_ipc_digest(&run.final_results, SortPolicy::FullRowLex).expect("digest")
	)
}

/// The healthy reference digest for the hash-join fixture, on its own fresh workers.
async fn adr626_reference_digest() -> String {
	let mut workers = adr626_workers(62_650, None).await;
	let mut scheduler = DistributedScheduler::new(adr626_registry(&workers));
	let dispatcher = NetworkTaskDispatcher::new(
		Arc::new(MemoryManager::new(64, 1024 * 1024)),
		process_transport(),
	);
	let run = bounded(scheduler.dispatch_loop(&adr626_hash_join_stages().await, &dispatcher))
		.await
		.expect("reference");
	for worker in &mut workers {
		worker.shutdown().await;
	}
	adr626_digest(&run)
}

fn adr626_event_names(run: &SchedulerRun) -> Vec<String> {
	recovery_events_evidence(&run.telemetry.recovery_events)
		.as_array()
		.expect("events")
		.iter()
		.map(|event| event["event"].as_str().expect("name").to_string())
		.collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unannounced_producer_loss_recovers_on_loopback() {
	with_quic(async {
		let reference = adr626_reference_digest().await;
		let (establishment, confirmation) = ADR626_LOCAL_RECOVERY;
		let experiment = toron::distributed::recovery::RecoveryExperiment::from_millis(
			establishment,
			confirmation,
		)
		.expect("experiment");
		let mut workers = adr626_workers(62_660, Some(ADR626_LOCAL_RECOVERY)).await;
		let mut scheduler = DistributedScheduler::new(adr626_registry(&workers))
			.with_recovery_experiment(experiment);
		let dispatcher = NetworkTaskDispatcher::new(
			Arc::new(MemoryManager::new(64, 1024 * 1024)),
			process_transport(),
		)
		.with_recovery_experiment(experiment);
		let stages = adr626_hash_join_stages().await;
		let producers = bounded(scheduler.dispatch_loop(&stages[..1], &dispatcher))
			.await
			.expect("producer stage");
		let victim = producers
			.accepted_manifests
			.values()
			.map(|manifest| manifest.producer_worker_id)
			.min_by_key(|worker| worker.get().as_u128())
			.expect("a sealed producer");
		let victim_process = workers
			.iter_mut()
			.find(|worker| worker.worker_id == victim)
			.expect("victim");
		victim_process.child.kill().await.expect("SIGKILL");
		let killed = std::time::Instant::now();
		let run = tokio::time::timeout(ADR626_R, scheduler.dispatch_loop(&stages, &dispatcher))
			.await
			.expect("a terminal return within R")
			.expect("recovered");
		let elapsed = killed.elapsed();
		assert_eq!(adr626_digest(&run), reference, "recovery is correct");
		let events = adr626_event_names(&run);
		for expected in [
			"report_accepted",
			"confirmation_started",
			"confirmation_ended",
			"excluded",
			"replay_dispatched",
		] {
			assert!(
				events.iter().any(|event| event == expected),
				"{expected} missing: {events:?}"
			);
		}
		assert!(
			run.accepted_manifests
				.values()
				.all(|manifest| manifest.producer_worker_id != victim),
			"no accepted output remains on the excluded worker"
		);
		assert!(elapsed < ADR626_R, "recovered in {elapsed:?}");
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_dispatch_target_is_excluded_on_loopback() {
	with_quic(async {
		let reference = adr626_reference_digest().await;
		let (establishment, confirmation) = ADR626_LOCAL_RECOVERY;
		let experiment = toron::distributed::recovery::RecoveryExperiment::from_millis(
			establishment,
			confirmation,
		)
		.expect("experiment");
		let mut workers = adr626_workers(62_670, Some(ADR626_LOCAL_RECOVERY)).await;
		let mut scheduler = DistributedScheduler::new(adr626_registry(&workers))
			.with_recovery_experiment(experiment);
		let dispatcher = NetworkTaskDispatcher::new(
			Arc::new(MemoryManager::new(64, 1024 * 1024)),
			process_transport(),
		)
		.with_recovery_experiment(experiment);
		let stages = adr626_hash_join_stages().await;
		let producers = bounded(scheduler.dispatch_loop(&stages[..1], &dispatcher))
			.await
			.expect("producer stage");
		let holders = producers
			.accepted_manifests
			.values()
			.map(|manifest| manifest.producer_worker_id)
			.collect::<HashSet<_>>();
		let victim = workers
			.iter()
			.map(|worker| worker.worker_id)
			.filter(|worker| !holders.contains(worker))
			.min_by_key(|worker| worker.get().as_u128())
			.expect("a worker holding no manifest");
		let victim_process = workers
			.iter_mut()
			.find(|worker| worker.worker_id == victim)
			.expect("victim");
		victim_process.child.kill().await.expect("SIGKILL");
		let run = tokio::time::timeout(ADR626_R, scheduler.dispatch_loop(&stages, &dispatcher))
			.await
			.expect("a terminal return within R")
			.expect("completed elsewhere");
		assert_eq!(adr626_digest(&run), reference);
		let events = adr626_event_names(&run);
		assert!(
			events
				.iter()
				.any(|event| event == "establishment_timed_out"),
			"{events:?}"
		);
		assert!(events.iter().any(|event| event == "excluded"), "{events:?}");
		assert!(
			!events.iter().any(|event| event == "confirmation_started"),
			"the dispatch path never confirms: {events:?}"
		);
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

#[tokio::test]
async fn an_invalid_recovery_experiment_refuses_to_start() {
	for flags in [
		[
			"--recovery-establishment-ms",
			"0",
			"--recovery-confirmation-ms",
			"100",
		],
		[
			"--recovery-establishment-ms",
			"500",
			"--recovery-confirmation-ms",
			"100",
		],
		[
			"--recovery-establishment-ms",
			"500",
			"--recovery-confirmation-ms",
			"300000",
		],
	] {
		let mut args = vec!["--worker-u128", "62690", "--transport", "quic"];
		args.extend(flags);
		let (ok, stdout, _) = fixture_worker_exit(&args).await;
		assert!(
			!ok && !stdout.contains("READY"),
			"{flags:?} must refuse to start"
		);
	}
	let (ok, _, _) = fixture_worker_exit(&[
		"--worker-u128",
		"62691",
		"--transport",
		"quic",
		"--recovery-establishment-ms",
		"500",
	])
	.await;
	assert!(!ok, "the two values go together");
}

// ---- ADR 626 Stage C: local controls (relay, long silence) ----

/// The declared experiment values the controls run at.
const ADR626_DECLARED: (u64, u64) = (5_000, 12_000);

/// A test-only, unprivileged UDP relay in front of one worker. Every datagram in either
/// direction is delayed by `delay` and dropped with probability `loss_per_mille / 1000` from a
/// seeded generator; `blackhole` drops everything. The seed fixes the generator, not a packet-by-
/// packet schedule (arrival order is concurrent), so the realized counts per direction are kept.
///
/// Every task the relay runs (the listener loop, one reader per client, one delayed sender per
/// direction) is owned by one supervisor's `JoinSet`. `shutdown` aborts and drains all of them,
/// so no forwarding outlives it and its sockets, the listener's port included, are released;
/// `live` counts the tasks still running, for the lifecycle fixture.
struct UdpRelay {
	addr: std::net::SocketAddr,
	blackhole: Arc<std::sync::atomic::AtomicBool>,
	counts: Arc<RelayCounts>,
	live: Arc<std::sync::atomic::AtomicUsize>,
	stop: Option<tokio::sync::oneshot::Sender<()>>,
	supervisor: Option<tokio::task::JoinHandle<()>>,
}

/// Per direction: `admitted` datagrams passed the loss draw and entered the delay queue; `dropped`
/// did not; `sent` left the relay's socket (`send` returned Ok). Queued work aborted at shutdown is
/// admitted but never sent. A send is not proof of delivery.
#[derive(Default)]
struct RelayCounts {
	to_worker_admitted: std::sync::atomic::AtomicU64,
	to_worker_dropped: std::sync::atomic::AtomicU64,
	to_worker_sent: std::sync::atomic::AtomicU64,
	to_client_admitted: std::sync::atomic::AtomicU64,
	to_client_dropped: std::sync::atomic::AtomicU64,
	to_client_sent: std::sync::atomic::AtomicU64,
}

impl RelayCounts {
	fn evidence(&self) -> serde_json::Value {
		use std::sync::atomic::Ordering::SeqCst;
		serde_json::json!({
			"to_worker": { "admitted": self.to_worker_admitted.load(SeqCst), "dropped": self.to_worker_dropped.load(SeqCst), "sent": self.to_worker_sent.load(SeqCst) },
			"to_client": { "admitted": self.to_client_admitted.load(SeqCst), "dropped": self.to_client_dropped.load(SeqCst), "sent": self.to_client_sent.load(SeqCst) },
		})
	}
}

/// Decrements the relay's live-task count when its task ends or is aborted.
struct LiveTask(Arc<std::sync::atomic::AtomicUsize>);

impl LiveTask {
	fn enter(live: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
		live.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		Self(live.clone())
	}
}

impl Drop for LiveTask {
	fn drop(&mut self) {
		self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
	}
}

impl UdpRelay {
	async fn start(
		target: std::net::SocketAddr,
		delay: std::time::Duration,
		loss_per_mille: u64,
		seed: u64,
	) -> Self {
		use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
		let listener = Arc::new(
			tokio::net::UdpSocket::bind("127.0.0.1:0")
				.await
				.expect("relay bind"),
		);
		let addr = listener.local_addr().expect("relay addr");
		let blackhole = Arc::new(AtomicBool::new(false));
		let counts = Arc::new(RelayCounts::default());
		let live = Arc::new(AtomicUsize::new(0));
		let rng = Arc::new(Mutex::new(seed.max(1)));
		let pass = {
			let (blackhole, rng) = (blackhole.clone(), rng.clone());
			move |admitted: &AtomicU64, dropped: &AtomicU64| {
				if blackhole.load(Ordering::SeqCst) {
					dropped.fetch_add(1, Ordering::SeqCst);
					return false;
				}
				// xorshift64: deterministic for a seed.
				let mut state = rng.lock().expect("rng");
				*state ^= *state << 13;
				*state ^= *state >> 7;
				*state ^= *state << 17;
				if *state % 1000 < loss_per_mille {
					dropped.fetch_add(1, Ordering::SeqCst);
					false
				} else {
					admitted.fetch_add(1, Ordering::SeqCst);
					true
				}
			}
		};
		let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
		let supervisor = {
			let (counts, live) = (counts.clone(), live.clone());
			tokio::spawn(async move {
				let _live = LiveTask::enter(&live);
				let mut tasks = tokio::task::JoinSet::new();
				// One delayed sender per direction; the delay is constant, so FIFO keeps deadlines.
				let (to_worker, mut to_worker_queue) = tokio::sync::mpsc::unbounded_channel::<(
					tokio::time::Instant,
					Vec<u8>,
					Arc<tokio::net::UdpSocket>,
				)>();
				let (to_client, mut to_client_queue) = tokio::sync::mpsc::unbounded_channel::<(
					tokio::time::Instant,
					Vec<u8>,
					std::net::SocketAddr,
				)>();
				{
					let (live, counts) = (live.clone(), counts.clone());
					tasks.spawn(async move {
						let _live = LiveTask::enter(&live);
						while let Some((due, bytes, upstream)) = to_worker_queue.recv().await {
							tokio::time::sleep_until(due).await;
							if upstream.send(&bytes).await.is_ok() {
								counts
									.to_worker_sent
									.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
							}
						}
					});
				}
				{
					let (live, listener, counts) = (live.clone(), listener.clone(), counts.clone());
					tasks.spawn(async move {
						let _live = LiveTask::enter(&live);
						while let Some((due, bytes, client)) = to_client_queue.recv().await {
							tokio::time::sleep_until(due).await;
							if listener.send_to(&bytes, client).await.is_ok() {
								counts
									.to_client_sent
									.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
							}
						}
					});
				}
				let mut upstreams =
					HashMap::<std::net::SocketAddr, Arc<tokio::net::UdpSocket>>::new();
				let mut buf = vec![0u8; 65_535];
				loop {
					let (len, client) = tokio::select! {
						_ = &mut stopped => break,
						received = listener.recv_from(&mut buf) => match received {
							Ok(received) => received,
							Err(_) => break,
						},
					};
					let upstream = match upstreams.get(&client) {
						Some(upstream) => upstream.clone(),
						None => {
							let upstream = Arc::new(
								tokio::net::UdpSocket::bind("127.0.0.1:0")
									.await
									.expect("upstream bind"),
							);
							upstream.connect(target).await.expect("upstream connect");
							upstreams.insert(client, upstream.clone());
							let (live, reader, to_client, counts, pass) = (
								live.clone(),
								upstream.clone(),
								to_client.clone(),
								counts.clone(),
								pass.clone(),
							);
							tasks.spawn(async move {
								let _live = LiveTask::enter(&live);
								let mut reply = vec![0u8; 65_535];
								while let Ok(len) = reader.recv(&mut reply).await {
									if pass(&counts.to_client_admitted, &counts.to_client_dropped) {
										let _ = to_client.send((
											tokio::time::Instant::now() + delay,
											reply[..len].to_vec(),
											client,
										));
									}
								}
							});
							upstream
						}
					};
					if pass(&counts.to_worker_admitted, &counts.to_worker_dropped) {
						let _ = to_worker.send((
							tokio::time::Instant::now() + delay,
							buf[..len].to_vec(),
							upstream,
						));
					}
				}
				// Retire every child before returning; their sockets drop with them.
				tasks.abort_all();
				while tasks.join_next().await.is_some() {}
			})
		};
		Self {
			addr,
			blackhole,
			counts,
			live,
			stop: Some(stop),
			supervisor: Some(supervisor),
		}
	}

	fn blackhole(&self) {
		self.blackhole
			.store(true, std::sync::atomic::Ordering::SeqCst);
	}

	/// ADR 628: end a blackout; datagrams are forwarded (subject to the loss draw) again.
	fn restore(&self) {
		self.blackhole
			.store(false, std::sync::atomic::Ordering::SeqCst);
	}

	/// Stops the relay and waits until every task it owns has ended; returns the realized counts.
	async fn shutdown(mut self) -> serde_json::Value {
		if let Some(stop) = self.stop.take() {
			let _ = stop.send(());
		}
		if let Some(supervisor) = self.supervisor.take() {
			supervisor.await.expect("relay supervisor");
		}
		self.counts.evidence()
	}
}

impl Drop for UdpRelay {
	/// A relay dropped without `shutdown` (a failed assertion) is still aborted: the supervisor's
	/// `JoinSet` aborts every child when it drops.
	fn drop(&mut self) {
		if let Some(supervisor) = self.supervisor.take() {
			supervisor.abort();
		}
	}
}

/// Test-support lifecycle: after `shutdown`, no relay task remains, nothing queued is delivered
/// late, the listener's port can be bound again, and the test's own endpoints stay open. Repeated
/// to catch accumulation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shut_down_relay_retires_every_task_and_releases_its_port() {
	for round in 0..3 {
		let target = tokio::net::UdpSocket::bind("127.0.0.1:0")
			.await
			.expect("target");
		let client = tokio::net::UdpSocket::bind("127.0.0.1:0")
			.await
			.expect("client");
		let relay = UdpRelay::start(
			target.local_addr().expect("target addr"),
			std::time::Duration::from_millis(200),
			0,
			626,
		)
		.await;
		let (addr, live) = (relay.addr, relay.live.clone());
		client.send_to(b"first", addr).await.expect("send");
		let mut buf = [0u8; 64];
		let (len, relay_side) = tokio::time::timeout(
			std::time::Duration::from_secs(5),
			target.recv_from(&mut buf),
		)
		.await
		.expect("forwarded within 5 s")
		.expect("recv");
		assert_eq!(&buf[..len], b"first");
		target.send_to(b"reply", relay_side).await.expect("reply");
		let (len, _) = tokio::time::timeout(
			std::time::Duration::from_secs(5),
			client.recv_from(&mut buf),
		)
		.await
		.expect("reply within 5 s")
		.expect("recv");
		assert_eq!(&buf[..len], b"reply");
		// Queue work that is still delayed when the relay stops, in both directions.
		client.send_to(b"late", addr).await.expect("send late");
		target
			.send_to(b"late reply", relay_side)
			.await
			.expect("late reply");
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		assert!(
			live.load(std::sync::atomic::Ordering::SeqCst) >= 4,
			"round {round}: supervisor, two senders, one reader"
		);
		let counts = relay.shutdown().await;
		assert_eq!(
			live.load(std::sync::atomic::Ordering::SeqCst),
			0,
			"round {round}: a relay task outlived shutdown"
		);
		// Two admitted per direction; only the first was sent, the queued one was retired unsent.
		for direction in ["to_worker", "to_client"] {
			assert_eq!(counts[direction]["admitted"], 2, "{counts}");
			assert_eq!(counts[direction]["sent"], 1, "{counts}");
		}
		let quiet = std::time::Duration::from_millis(500);
		assert!(
			tokio::time::timeout(quiet, target.recv_from(&mut buf))
				.await
				.is_err(),
			"round {round}: a queued datagram reached the worker after shutdown"
		);
		assert!(
			tokio::time::timeout(quiet, client.recv_from(&mut buf))
				.await
				.is_err(),
			"round {round}: a queued reply reached the client after shutdown"
		);
		drop(std::net::UdpSocket::bind(addr).expect("the relay's port is released"));
		// The test's own endpoints were not closed by the relay.
		client
			.send_to(b"still open", target.local_addr().expect("addr"))
			.await
			.expect("client open");
		let (len, _) = target.recv_from(&mut buf).await.expect("target open");
		assert_eq!(&buf[..len], b"still open");
	}
}

fn free_loopback_port() -> std::net::SocketAddr {
	let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("probe a free port");
	socket.local_addr().expect("free port")
}

/// Three QUIC loopback workers with the declared experiment; the first sits behind `relay`.
async fn relayed_cluster(
	first_id: u128,
	delay: std::time::Duration,
	loss_per_mille: u64,
	seed: u64,
) -> (Vec<FixtureWorkerProcess>, UdpRelay, Vec<WorkerEntry>) {
	let behind = free_loopback_port();
	let relay = UdpRelay::start(behind, delay, loss_per_mille, seed).await;
	let mut workers = vec![
		FixtureWorkerProcess::spawn_loopback_relayed(
			WorkerId::from_u128(first_id),
			behind,
			relay.addr,
			ADR626_DECLARED,
		)
		.await,
	];
	for id in first_id + 1..first_id + 3 {
		workers.push(
			FixtureWorkerProcess::spawn_configured(
				WorkerId::from_u128(id),
				None,
				None,
				Some(ADR626_DECLARED),
			)
			.await,
		);
	}
	let entries = workers
		.iter()
		.map(|worker| worker_entry_for(worker.worker_id, worker.endpoint.clone()))
		.collect();
	(workers, relay, entries)
}

fn declared_experiment() -> toron::distributed::recovery::RecoveryExperiment {
	toron::distributed::recovery::RecoveryExperiment::from_millis(
		ADR626_DECLARED.0,
		ADR626_DECLARED.1,
	)
	.expect("declared")
}

fn write_control_evidence(name: &str, evidence: &serde_json::Value) {
	let Ok(dir) = std::env::var("TORON_ADR626_CONTROL_DIR") else {
		return;
	};
	use std::io::Write as _;
	std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(std::path::Path::new(&dir).join(format!("{name}.json")))
		.expect("fresh control evidence")
		.write_all(
			serde_json::to_string_pretty(evidence)
				.expect("json")
				.as_bytes(),
		)
		.expect("write control evidence");
}

/// Confirmation intervals per (query, worker), from the scheduler's ordered events: each
/// `confirmation_started` opens one, its `confirmation_ended` closes it. A start while one is open
/// for the same pair is an overlap. Each interval lists the distinct consumer reports bound to it:
/// `report_accepted` naming its worker since the previous interval closed. A coordinator dispatch
/// timeout (`establishment_timed_out`) excludes its target directly, without a confirmation, so it
/// never binds to one. Coalescing is "observed" only if an interval has two reports.
fn confirmation_intervals(events: &serde_json::Value) -> serde_json::Value {
	let mut open = HashMap::<(String, String), serde_json::Value>::new();
	let mut pending = HashMap::<(String, String), Vec<serde_json::Value>>::new();
	let (mut closed, mut overlapping, mut unclosed) = (Vec::new(), Vec::new(), Vec::new());
	for (index, event) in events.as_array().expect("events").iter().enumerate() {
		let fields = &event["fields"];
		let text = |value: &serde_json::Value| value.as_str().unwrap_or_default().to_string();
		match event["event"].as_str().unwrap_or_default() {
			"report_accepted" => pending
				.entry((text(&fields["consumer"]["query"]), text(&fields["worker"])))
				.or_default()
				.push(serde_json::json!({ "index": index, "kind": "report_accepted", "consumer": fields["consumer"], "producer": fields["producer"] })),
			"confirmation_started" => {
				let pair = (text(&fields["query"]), text(&fields["worker"]));
				if open.contains_key(&pair) {
					overlapping.push(serde_json::json!({ "query": pair.0, "worker": pair.1, "index": index }));
				} else {
					open.insert(pair, serde_json::json!({ "started_index": index, "started_at_micros": event["at_micros"] }));
				}
			}
			"confirmation_ended" => {
				let pair = (text(&fields["query"]), text(&fields["worker"]));
				if let Some(mut interval) = open.remove(&pair) {
					let reports = pending.remove(&pair).unwrap_or_default();
					interval["query"] = pair.0.into();
					interval["worker"] = pair.1.into();
					interval["ended_index"] = index.into();
					interval["ended_at_micros"] = event["at_micros"].clone();
					interval["outcome"] = fields["outcome"].clone();
					interval["report_count"] = reports.len().into();
					interval["reports"] = reports.into();
					closed.push(interval);
				}
			}
			_ => {}
		}
	}
	for ((query, worker), interval) in open {
		unclosed
			.push(serde_json::json!({ "query": query, "worker": worker, "interval": interval }));
	}
	let coalescing = if closed
		.iter()
		.any(|interval| interval["report_count"].as_u64() >= Some(2))
	{
		"observed"
	} else {
		"single-report coverage; coalescing rests on the unit fixture simultaneous_reports_of_one_worker_share_one_confirmation"
	};
	serde_json::json!({ "intervals": closed, "overlapping": overlapping, "unclosed": unclosed, "coalescing": coalescing })
}

/// Below `H`: a relay adding 300 ms each way and 5% seeded loss in front of one worker. Every
/// establishment and pull through it is slower but healthy: no suspicion, parity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ADR 626 Stage C control: run explicitly; TORON_ADR626_CONTROL_DIR keeps its evidence"]
async fn relay_control_below_h_raises_no_suspicion() {
	with_quic(async {
		let reference = adr626_reference_digest().await;
		let (delay, loss, seed) = (std::time::Duration::from_millis(300), 50, 626);
		let (mut workers, relay, entries) = relayed_cluster(62_700, delay, loss, seed).await;
		let relayed = entries[0].worker_id;
		let entry_ids = entries
			.iter()
			.map(|entry| entry.worker_id.get().as_u128().to_string())
			.collect::<Vec<_>>();
		// The actual establishment through the relay, measured.
		let probe = toron::quic::QuicEndpoint::client_loopback().expect("client");
		let started = std::time::Instant::now();
		let established = tokio::time::timeout(
			std::time::Duration::from_secs(5),
			probe.connect_stream(relay.addr),
		)
		.await;
		let establishment_ms = started.elapsed().as_millis() as u64;
		assert!(
			matches!(established, Ok(Ok(_))),
			"establishment through the relay below H"
		);
		let registry = Arc::new(RoundRobinRegistry::new(entries, 0));
		let mut scheduler =
			DistributedScheduler::new(registry).with_recovery_experiment(declared_experiment());
		let dispatcher = NetworkTaskDispatcher::new(
			Arc::new(MemoryManager::new(64, 1024 * 1024)),
			process_transport(),
		)
		.with_recovery_experiment(declared_experiment());
		let started = std::time::Instant::now();
		let run = tokio::time::timeout(
			ADR626_R,
			scheduler.dispatch_loop(&adr626_hash_join_stages().await, &dispatcher),
		)
		.await
		.expect("within R")
		.expect("healthy through the relay");
		let elapsed_ms = started.elapsed().as_millis() as u64;
		let counts = relay.shutdown().await;
		write_control_evidence(
			"relay-below-h",
			&serde_json::json!({
				"relay": {"delay_ms_each_way": 300, "loss_per_mille": loss, "seed": seed, "realized": counts},
				"relayed_worker": relayed.get().as_u128().to_string(),
				"workers": entry_ids,
				"experiment": {"establishment_ms": ADR626_DECLARED.0, "confirmation_ms": ADR626_DECLARED.1},
				"establishment_through_relay_ms": establishment_ms,
				"query_elapsed_ms": elapsed_ms,
				"digest_equals_reference": adr626_digest(&run) == reference,
				"recovery_events": recovery_events_evidence(&run.telemetry.recovery_events),
			}),
		);
		assert_eq!(adr626_digest(&run), reference);
		assert!(
			run.telemetry.recovery_events.is_empty(),
			"no suspicion below H: {:?}",
			run.telemetry.recovery_events
		);
		let realized =
			|direction: &str, what: &str| counts[direction][what].as_u64().expect("count");
		assert!(
			["to_worker", "to_client"]
				.iter()
				.all(|d| realized(d, "sent") > 0)
				&& realized("to_worker", "dropped") + realized("to_client", "dropped") > 0,
			"the relay delayed and dropped: {counts}"
		);
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

/// Past `H` on both paths: once the producer stage has sealed on the relayed worker, the relay
/// drops everything. Consumers' pulls and the coordinator's confirmation both go through it.
/// The one declared outcome: the query recovers within `R` at parity with the reference, every
/// suspicion and the one exclusion name the relayed worker, and no two confirmations of one
/// (query, worker) overlap. Coalescing is claimed only if two reports actually joined one
/// confirmation; otherwise the evidence says single-report coverage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ADR 626 Stage C control: run explicitly; TORON_ADR626_CONTROL_DIR keeps its evidence"]
async fn relay_control_past_h_on_both_paths_excludes_only_the_relayed_worker() {
	with_quic(async {
		let reference = adr626_reference_digest().await;
		let (delay, loss, seed) = (std::time::Duration::from_millis(50), 0, 626);
		let (mut workers, relay, entries) = relayed_cluster(62_710, delay, loss, seed).await;
		let relayed = entries[0].worker_id;
		let entry_ids = entries.iter().map(|entry| entry.worker_id.get().as_u128().to_string()).collect::<Vec<_>>();
		let registry = Arc::new(RoundRobinRegistry::new(entries, 0));
		let mut scheduler = DistributedScheduler::new(registry).with_recovery_experiment(declared_experiment());
		let dispatcher = NetworkTaskDispatcher::new(Arc::new(MemoryManager::new(64, 1024 * 1024)), process_transport())
			.with_recovery_experiment(declared_experiment());
		let stages = adr626_hash_join_stages().await;
		let producers = bounded(scheduler.dispatch_loop(&stages[..1], &dispatcher)).await.expect("producer stage");
		assert!(
			producers.accepted_manifests.values().any(|manifest| manifest.producer_worker_id == relayed),
			"a producer sealed behind the relay"
		);
		relay.blackhole();
		let cut = scheduler.recovery_clock_micros();
		let started = std::time::Instant::now();
		let outcome = tokio::time::timeout(ADR626_R, scheduler.dispatch_loop(&stages, &dispatcher)).await;
		let elapsed_ms = started.elapsed().as_millis() as u64;
		let events = recovery_events_evidence(scheduler.recovery_events());
		let names = events.as_array().expect("events").iter().map(|e| e["event"].as_str().expect("name").to_string()).collect::<Vec<_>>();
		let counts = relay.shutdown().await;
		let intervals = confirmation_intervals(&events);
		let digest_matches = matches!(&outcome, Ok(Ok(run)) if adr626_digest(run) == reference);
		write_control_evidence("relay-past-h", &serde_json::json!({
			"relay": {"delay_ms_each_way": 50, "loss_per_mille": loss, "seed": seed, "blackholed_after_producer_stage": true, "realized": counts},
			"relayed_worker": relayed.get().as_u128().to_string(),
			"workers": entry_ids,
			"experiment": {"establishment_ms": ADR626_DECLARED.0, "confirmation_ms": ADR626_DECLARED.1},
			"declared_outcome": "recovered within R at reference parity",
			"outcome": match &outcome { Ok(Ok(_)) => "recovered".to_string(), Ok(Err(error)) => format!("error: {error:?}"), Err(_) => "no terminal return within R".to_string() },
			"scheduler_clock_at_cut_micros": cut,
			"elapsed_from_cut_ms": elapsed_ms,
			"digest_equals_reference": digest_matches,
			"confirmation_intervals": intervals,
			"recovery_events": events,
		}));
		assert!(digest_matches, "the declared outcome: recovered within R at parity: {:?}", outcome.as_ref().map(|r| r.as_ref().err()));
		assert_eq!(intervals["overlapping"], serde_json::json!([]), "{intervals}");
		// Part 5: "exactly one suspicion" is not asserted under retransmission and concurrency.
		// Every suspicion and the one exclusion name the relayed worker.
		let relayed_name = relayed.get().as_u128().to_string();
		let named = |event: &str| {
			events.as_array().expect("events").iter().filter(|e| e["event"] == event).map(|e| e["fields"]["worker"].as_str().expect("worker").to_string()).collect::<Vec<_>>()
		};
		let started_for = named("confirmation_started");
		assert!(!started_for.is_empty() && started_for.iter().all(|w| *w == relayed_name), "{names:?}");
		assert_eq!(named("excluded"), vec![relayed_name.clone()], "only the relayed worker is excluded: {names:?}");
		for expected in ["report_accepted", "confirmation_ended", "excluded", "replay_dispatched"] {
			assert!(names.iter().any(|name| name == expected), "{expected}: {names:?}");
		}
		assert!(elapsed_ms < ADR626_R.as_millis() as u64);
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

/// A healthy execution that stays silent longer than `H`: `H` bounds establishment only, so the
/// query completes with no suspicion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_silent_healthy_execution_outlasts_h() {
	with_quic(async {
		let mut workers = adr626_workers(62_720, Some(ADR626_LOCAL_RECOVERY)).await;
		let experiment = toron::distributed::recovery::RecoveryExperiment::from_millis(
			ADR626_LOCAL_RECOVERY.0,
			ADR626_LOCAL_RECOVERY.1,
		)
		.expect("experiment");
		let mut scheduler = DistributedScheduler::new(adr626_registry(&workers))
			.with_recovery_experiment(experiment);
		let dispatcher = NetworkTaskDispatcher::new(
			Arc::new(MemoryManager::new(64, 1024 * 1024)),
			process_transport(),
		)
		.with_recovery_experiment(experiment);
		let stages = fragment_plan(
			probe_case_plan("slow_scan").await,
			FragmentPlanConfig::new_for_test(
				QueryId::new(),
				TaskBudget::from_query_budget(QueryBudget::test_default()),
			),
		)
		.expect("fragment slow scan");
		let started = std::time::Instant::now();
		let run = tokio::time::timeout(ADR626_R, scheduler.dispatch_loop(&stages, &dispatcher))
			.await
			.expect("within R")
			.expect("completes");
		let elapsed = started.elapsed();
		let rows = run
			.final_results
			.iter()
			.map(RecordBatch::num_rows)
			.sum::<usize>();
		write_control_evidence(
			"long-silence",
			&serde_json::json!({
				"experiment": {"establishment_ms": ADR626_LOCAL_RECOVERY.0, "confirmation_ms": ADR626_LOCAL_RECOVERY.1},
				"case": "slow_scan",
				"query_elapsed_ms": elapsed.as_millis() as u64,
				"row_count": rows,
				"recovery_events": recovery_events_evidence(&run.telemetry.recovery_events),
			}),
		);
		assert!(
			elapsed > std::time::Duration::from_millis(ADR626_LOCAL_RECOVERY.0),
			"silent for longer than H: {elapsed:?}"
		);
		assert!(
			run.telemetry.recovery_events.is_empty(),
			"{:?}",
			run.telemetry.recovery_events
		);
		assert!(rows > 0);
		for worker in &mut workers {
			worker.shutdown().await;
		}
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

// ---- ADR 627: mid-task worker loss (diagnosis only; no behaviour change) ----

/// The frozen delay from the victim's `StartTask` payload being written (on this process's clock)
/// to the signal. It is not measured from the scan's start; whether the scan had entered and not
/// returned at the signal is settled from the victim's own log.
const ADR627_SIGNAL_DELAY: std::time::Duration = std::time::Duration::from_secs(3);
/// The frozen observation window after the signal.
const ADR627_OBSERVATION: std::time::Duration = std::time::Duration::from_secs(360);

/// Which coordinator drives the query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Adr627Path {
	/// `DistributedScheduler::dispatch_loop` directly, as the ADR 625/626 probes do.
	Scheduler,
	/// The production `DistributedQueryCoordinator::execute_query`: admission, active query and
	/// lease renewal. ADR 626's recovery experiment cannot be attached on this path.
	Coordinator,
}

impl Adr627Path {
	fn parse(text: &str) -> Self {
		match text {
			"scheduler" => Self::Scheduler,
			"coordinator" => Self::Coordinator,
			other => panic!("unknown ADR 627 path {other}"),
		}
	}

	fn name(self) -> &'static str {
		match self {
			Self::Scheduler => "scheduler",
			Self::Coordinator => "coordinator",
		}
	}
}

/// The fault to inject into one running task: the target worker and the signal that acts on it.
/// The signal returns its own evidence, including whether its effect was confirmed (gone for a
/// kill, stopped for a stop).
struct Adr627Fault {
	target: WorkerId,
	/// The frozen stage whose task scans the slow table on this path and case, declared before the
	/// run (from the same-path reference) and verified afterwards; never chosen from what landed.
	victim_stage: u32,
	fault: &'static str,
	signal: Box<dyn FnOnce() -> futures::future::BoxFuture<'static, serde_json::Value> + Send>,
}

/// ADR 627: the task failure a query error carries, typed by its taxonomy label, with the lost
/// worker's id when it names one.
fn adr627_task_failure(kind: &toron::error::TaskFailureKind) -> serde_json::Value {
	match kind {
		toron::error::TaskFailureKind::WorkerLost { worker_id } => serde_json::json!({
			"kind": kind.taxonomy_label(),
			"worker": worker_id.get().as_u128().to_string(),
		}),
		other => serde_json::json!({ "kind": other.taxonomy_label() }),
	}
}

/// ADR 627: copies WARN-and-above tracing events (the coordinator's existing "query lease renewal
/// connect failed", among others) into the probe log, so lease operations can be correlated with
/// them. Passive: it records, and changes nothing the code does.
struct Adr627TraceLayer {
	log: Arc<wait_events::EventLog>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Adr627TraceLayer {
	fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
		if *event.metadata().level() > tracing::Level::WARN {
			return;
		}
		struct Fields(serde_json::Map<String, serde_json::Value>);
		impl tracing::field::Visit for Fields {
			fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
				self.0
					.insert(field.name().to_string(), format!("{value:?}").into());
			}
			fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
				self.0.insert(field.name().to_string(), value.into());
			}
		}
		let mut fields = Fields(serde_json::Map::new());
		event.record(&mut fields);
		self.log.emit(
			"trace_event",
			"dispatch",
			serde_json::json!({
				"level": event.metadata().level().to_string(),
				"target": event.metadata().target(),
				"fields": fields.0,
			}),
		);
	}
}

/// Installs the trace layer as this process's global subscriber (spawned tasks such as the lease
/// renewal run on other threads, so a thread-local default would miss them). Returns whether it was
/// installed; a process that already has a global subscriber records that the capture is unavailable.
fn adr627_capture_traces(log: &Arc<wait_events::EventLog>) -> bool {
	use tracing_subscriber::layer::SubscriberExt as _;
	let subscriber = tracing_subscriber::registry().with(Adr627TraceLayer { log: log.clone() });
	tracing::subscriber::set_global_default(subscriber).is_ok()
}

/// One mid-task run. Every observation is on this process's clock (the probe log's `now_us`);
/// nothing here subtracts another host's timestamps. Whether the victim's task had entered
/// execution is settled later, from the victim's own log.
async fn adr627_run(
	path: Adr627Path,
	case: &str,
	workers: &[(WorkerId, WorkerEndpoint)],
	transport: Arc<dyn ToronTransport>,
	log: Arc<wait_events::EventLog>,
	recovery: Option<toron::distributed::recovery::RecoveryExperiment>,
	fault: Option<Adr627Fault>,
	healthy_bound: std::time::Duration,
) -> serde_json::Value {
	let mut events = log.subscribe();
	let trace_capture = adr627_capture_traces(&log);
	let mut evidence = serde_json::json!({
		"trace_capture": if trace_capture { "installed" } else { "unavailable: a global subscriber was already set" },
		"mode": "mid_task",
		"path": path.name(),
		"case": case,
		"fault": fault.as_ref().map(|fault| fault.fault),
		"target_worker": fault.as_ref().map(|fault| fault.target.get().as_u128().to_string()),
		"signal_delay_after_start_task_write_ms": ADR627_SIGNAL_DELAY.as_millis() as u64,
		"victim_stage": fault.as_ref().map(|fault| fault.victim_stage),
		"observation_secs": ADR627_OBSERVATION.as_secs(),
		"recovery_experiment": match (path, recovery) {
			(Adr627Path::Coordinator, _) => serde_json::json!("off: not configurable on the production coordinator path"),
			(Adr627Path::Scheduler, Some(experiment)) => serde_json::json!({
				"establishment_ms": experiment.establishment_deadline_millis(),
				"confirmation_ms": u64::try_from(experiment.confirmation_deadline().as_millis()).unwrap_or(u64::MAX),
			}),
			(Adr627Path::Scheduler, None) => serde_json::json!("off"),
		},
		"workers": workers.iter().map(|(worker, endpoint)| serde_json::json!({
			"worker": worker.get().as_u128().to_string(), "endpoint": endpoint.as_str(),
		})).collect::<Vec<_>>(),
		"started_us": log.now_us(),
		"started_unix_ms": unix_ms() as u64,
	});
	let entries = || {
		workers
			.iter()
			.map(|(worker, endpoint)| worker_entry_for(*worker, endpoint.clone()))
			.collect::<Vec<_>>()
	};
	let target_endpoint = fault.as_ref().and_then(|fault| {
		workers
			.iter()
			.find(|(worker, _)| *worker == fault.target)
			.map(|(_, endpoint)| endpoint.as_str().to_string())
	});
	let (done_tx, mut done_rx) = tokio::sync::watch::channel(false);
	let signalled_at: Arc<Mutex<Option<std::time::Instant>>> = Arc::default();

	// The watcher: bind the victim from this process's own transport events, wait the frozen
	// delay, check nothing has come back on the victim's stream, then signal.
	let watcher = {
		let log = log.clone();
		let signalled_at = signalled_at.clone();
		async move {
			let Some(fault) = fault else {
				return serde_json::json!(null);
			};
			let target = fault.target.get().as_u128().to_string();
			let victim_stage = fault.victim_stage;
			// Unbounded, installed before dispatch: no event is dropped (no lag); a closed log ends
			// the watch without a victim.
			let mut stream_target = HashMap::<u64, String>::new();
			let victim = loop {
				let event = tokio::select! {
					event = events.recv() => match event { Some(event) => event, None => return serde_json::json!({ "outcome": "event log closed" }) },
					_ = done_rx.changed() => return serde_json::json!({ "outcome": "no_running_victim: the query ended before a StartTask to the target was written" }),
				};
				let name = event["event"].as_str().unwrap_or_default();
				if name == "dial_resolved" {
					if let (Some(stream), Some(worker)) =
						(event["stream"].as_u64(), event["target"]["worker"].as_str())
					{
						stream_target.insert(stream, worker.to_string());
					}
				}
				if name == "frame_envelope_written"
					&& event["kind"] == "StartTask"
					&& event["identity"]["task"]["stage"] == victim_stage
				{
					let stream = event["stream"].as_u64().unwrap_or(u64::MAX);
					let landed = stream_target.get(&stream).cloned();
					if landed.as_deref() == Some(target.as_str()) {
						break serde_json::json!({
							"task": event["identity"]["task"].clone(),
							"worker": target,
							"endpoint": target_endpoint,
							"stream": stream,
							"start_task_written_us": event["at_us"].clone(),
						});
					}
					// The declared stage's task went elsewhere: no victim on the declared host, never retargeted.
					return serde_json::json!({
						"outcome": "no_running_victim_on_declared_host",
						"landed_on": landed,
						"task": event["identity"]["task"].clone(),
					});
				}
			};
			let stream = victim["stream"].as_u64().expect("victim stream");
			tokio::select! {
				_ = tokio::time::sleep(ADR627_SIGNAL_DELAY) => {}
				_ = done_rx.changed() => {
					return serde_json::json!({ "outcome": "query_returned_before_signal", "victim": victim });
				}
			}
			if log.failed() {
				return serde_json::json!({ "outcome": "instrument_failure: an event write failed; not signalled", "victim": victim });
			}
			// Everything this process has observed on the victim's stream up to now. Task output or
			// a terminal frame means the task was not demonstrably unresolved at the signal; an
			// acknowledgement (`TaskStarted`, `Progress`, `Heartbeat`, a lease accept) does not.
			let mut read_back = Vec::new();
			let mut output_before_signal = Vec::new();
			while let Ok(event) = events.try_recv() {
				if event["stream"].as_u64() == Some(stream)
					&& event["event"] == "frame_envelope_read"
				{
					if matches!(
						event["kind"].as_str(),
						Some("TaskBatch" | "TaskComplete" | "TaskFailed" | "TaskCancelled")
					) {
						output_before_signal.push(event.clone());
					}
					read_back.push(event);
				}
			}
			let started_ack = read_back.iter().any(|event| event["kind"] == "TaskStarted");
			// Each instant is captured once and both recorded and emitted with that exact value, so the
			// saved record and the raw event agree to the microsecond.
			let signal_us = log.now_us();
			log.emit_at(
				signal_us,
				"signal_sent",
				"dispatch",
				serde_json::json!({ "worker": target, "fault": fault.fault }),
			);
			*signalled_at.lock().expect("signal time") = Some(std::time::Instant::now());
			let signal = (fault.signal)().await;
			let returned_us = log.now_us();
			log.emit_at(
				returned_us,
				"signal_returned",
				"dispatch",
				serde_json::json!({ "worker": target, "result": signal.clone() }),
			);
			serde_json::json!({
				"outcome": if output_before_signal.is_empty() { "signalled" } else { "signalled_after_task_output" },
				"victim": victim,
				"envelopes_read_before_signal": read_back,
				"task_output_before_signal": output_before_signal,
				"task_started_ack_before_signal": started_ack,
				"signal_sent_us": signal_us,
				"signal_returned_us": returned_us,
				"signal": signal,
			})
		}
	};

	// The query, raced against the observation window measured from the signal (or the healthy
	// bound when nothing is signalled).
	let started = std::time::Instant::now();
	let window_open = |signalled_at: &Arc<Mutex<Option<std::time::Instant>>>| match *signalled_at
		.lock()
		.expect("signal time")
	{
		Some(at) => at.elapsed() < ADR627_OBSERVATION,
		None => started.elapsed() < healthy_bound.max(ADR627_OBSERVATION),
	};
	let mut recovery_events = serde_json::json!(null);
	let outcome = match path {
		Adr627Path::Scheduler => {
			let stages = fragment_plan(
				probe_case_plan(case).await,
				FragmentPlanConfig::new_for_test(
					QueryId::new(),
					TaskBudget::from_query_budget(QueryBudget::test_default()),
				),
			)
			.expect("fragment ADR 627 case");
			// Evidence only: the stages whose tasks scan the slow or long table, as this path fragments them.
			evidence["scheduler_scan_stages"] = stages
				.iter()
				.filter(|stage| {
					stage.tasks.iter().any(|task| {
						let text = format!("{:?}", task.spec);
						text.contains("TableId(9)") || text.contains("TableId(10)")
					})
				})
				.map(|stage| stage.stage_id.get())
				.collect::<Vec<_>>()
				.into();
			let mut scheduler =
				DistributedScheduler::new(Arc::new(RoundRobinRegistry::new(entries(), 0)));
			let mut network = NetworkTaskDispatcher::new(
				Arc::new(MemoryManager::new(64, 1024 * 1024)),
				transport.clone(),
			);
			if let Some(experiment) = recovery {
				scheduler = scheduler.with_recovery_experiment(experiment);
				network = network.with_recovery_experiment(experiment);
			}
			let dispatcher = ObservedDispatcher {
				inner: network,
				log: Some(log.clone()),
			};
			let query = async {
				let run = scheduler.dispatch_loop(&stages, &dispatcher);
				tokio::pin!(run);
				loop {
					tokio::select! {
						result = &mut run => {
							log.emit("query_returned", "dispatch", serde_json::json!({ "ok": result.is_ok() }));
							break Some(result);
						}
						_ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {
							if !window_open(&signalled_at) {
								log.emit("observation_expired", "dispatch", serde_json::json!({}));
								break None;
							}
						}
					}
				}
			};
			let (signal, result) = {
				let query = async {
					let result = query.await;
					let _ = done_tx.send(true);
					result
				};
				tokio::join!(watcher, query)
			};
			evidence["signal"] = signal;
			recovery_events = recovery_events_evidence(scheduler.recovery_events());
			match result {
				Some(Ok(run)) => {
					serde_json::json!({ "outcome": "completed", "run": run_evidence(&run) })
				}
				Some(Err(error)) => serde_json::json!({
					"outcome": "error", "error": format!("{error:?}"), "error_kind": scheduler_error_kind(&error),
					"error_task_failure": match &error {
						toron::distributed::scheduler::SchedulerError::TaskFailed { kind, .. } => adr627_task_failure(kind),
						_ => serde_json::Value::Null,
					},
				}),
				None => serde_json::json!({ "outcome": "no_terminal_return_within_observation" }),
			}
		}
		Adr627Path::Coordinator => {
			let (catalog, logical) = probe_case_logical(case);
			let coordinator = toron::coordinator::DistributedQueryCoordinator::new(
				catalog,
				Arc::new(RoundRobinRegistry::new(entries(), 0)),
				Arc::new(MemoryManager::new(64, 1024 * 1024)),
				transport.clone(),
				PlannerConfig::default(),
				QueryBudget::test_default(),
			);
			// ADR 628 B1.C: the declared instrument, when this run has one.
			let coordinator = match RELIANCE_INSTRUMENT.try_with(|instrument| instrument.clone()) {
				Ok(instrument) => {
					evidence["reliance_instrument"] = serde_json::json!({
						"observer": "probe_events.jsonl reliance events",
						"renewal_transport": instrument.renewal_transport.is_some(),
					});
					let coordinator = coordinator.with_reliance_observer(instrument.observer);
					match instrument.renewal_transport {
						Some(renewal) => coordinator.with_renewal_transport(renewal),
						None => coordinator,
					}
				}
				Err(_) => coordinator,
			};
			// ADR 628 B1.C: with the instrument, the query's own renewal cleanup record is awaited (bounded)
			// AFTER the raw `query_returned` stamp; subscribed now so no record is missed.
			let cleanup_events = RELIANCE_INSTRUMENT.try_with(|_| log.subscribe()).ok();
			let query = async {
				let run = coordinator.execute_query(&logical);
				tokio::pin!(run);
				let result = loop {
					tokio::select! {
						result = &mut run => {
							log.emit("query_returned", "dispatch", serde_json::json!({ "ok": result.is_ok() }));
							break Some(result);
						}
						_ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {
							if !window_open(&signalled_at) {
								log.emit("observation_expired", "dispatch", serde_json::json!({}));
								break None;
							}
						}
					}
				};
				let _ = done_tx.send(true);
				result
			};
			let (signal, result) = tokio::join!(watcher, query);
			evidence["signal"] = signal;
			if let Some(mut events) = cleanup_events {
				let waited = tokio::time::timeout(ADR628_CLEANUP_WAIT, async {
					while let Some(event) = events.recv().await {
						if event["event"] == "reliance" && event["kind"] == "renewal_cleanup" {
							return Some(event["result"].clone());
						}
					}
					None
				})
				.await;
				let outcome = match waited {
					Ok(Some(result)) => serde_json::json!({ "recorded": result }),
					Ok(None) => {
						serde_json::json!({ "recorded": null, "reason": "the event log closed" })
					}
					Err(_) => {
						serde_json::json!({ "recorded": null, "reason": "no cleanup record within the bound" })
					}
				};
				log.emit(
					"renewal_cleanup_wait",
					"dispatch",
					serde_json::json!({
						"bound_ms": ADR628_CLEANUP_WAIT.as_millis() as u64, "outcome": outcome.clone(),
					}),
				);
				evidence["renewal_cleanup_wait"] = outcome;
			}
			match result {
				Some(Ok(batches)) => {
					let digest = canonical_arrow_ipc_digest(&batches, SortPolicy::FullRowLex)
						.expect("digest");
					let mut rendered = Vec::new();
					for batch in &batches {
						for row in 0..batch.num_rows() {
							rendered.push(
								batch
									.columns()
									.iter()
									.map(|column| {
										arrow::util::display::array_value_to_string(
											column.as_ref(),
											row,
										)
										.unwrap_or_else(|error| format!("<{error}>"))
									})
									.collect::<Vec<_>>()
									.join("|"),
							);
						}
					}
					rendered.sort();
					serde_json::json!({ "outcome": "completed", "result": {
						"digest": format!("{digest:?}"), "row_count": rendered.len(), "rows_rendered": rendered,
					}})
				}
				Some(Err(error)) => serde_json::json!({
					"outcome": "error",
					"error": format!("{error:?}"),
					"error_task_failure": match &error {
						toron::error::QueryError::Runtime(toron::error::RuntimeError::TaskFailure { kind }) => adr627_task_failure(kind),
						_ => serde_json::Value::Null,
					},
				}),
				None => serde_json::json!({ "outcome": "no_terminal_return_within_observation" }),
			}
		}
	};
	let ended_us = log.now_us();
	log.emit(
		"observation_end",
		"dispatch",
		serde_json::json!({ "outcome": outcome["outcome"].clone() }),
	);
	// Terminal timing is read by the reader from the raw `query_returned` / `observation_expired`
	// and `signal_sent` events on this log's clock, never stamped after the watcher completes.
	evidence["instrument_failed"] = log.failed().into();
	evidence["result"] = outcome;
	evidence["recovery_events"] = recovery_events;
	evidence["elapsed_ms"] = (started.elapsed().as_millis() as u64).into();
	evidence["ended_us"] = ended_us.into();
	evidence["ended_unix_ms"] = (unix_ms() as u64).into();
	evidence
}

/// The host-mode mid-task probe: the declared workers, path, case and fault from the spec; the
/// signal is the harness's own command for exactly the target worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "multinode probe: needs TORON_MULTINODE_SPEC and running host-mode workers"]
async fn multinode_probe_mid_task() {
	let spec = load_probe_spec();
	let log = spec
		.wait_log
		.clone()
		.expect("the mid-task probe needs wait_events");
	let path = Adr627Path::parse(&spec_str(&spec.raw, "path"));
	let case = spec_str(&spec.raw, "case");
	let fault = match spec.raw["fault"].as_str() {
		None | Some("none") => None,
		Some(kind) => {
			let fault: &'static str = match kind {
				"kill" => "kill",
				"stop" => "stop",
				other => panic!("unknown fault {other}"),
			};
			let target_text = spec_str(&spec.raw, "target_worker");
			let target = WorkerId::from_u128(target_text.parse().expect("target_worker"));
			let argv = spec.raw["signal_commands"][target_text.as_str()]
				.as_array()
				.unwrap_or_else(|| panic!("no signal command for {target_text}"))
				.iter()
				.map(|arg| arg.as_str().expect("signal arg").to_string())
				.collect::<Vec<_>>();
			Some(Adr627Fault {
				target,
				victim_stage: u32::try_from(
					spec.raw["victim_stage"].as_u64().expect("victim_stage"),
				)
				.expect("stage"),
				fault,
				signal: Box::new(move || {
					Box::pin(async move {
						let started = std::time::Instant::now();
						let child = Command::new(&argv[0])
							.args(&argv[1..])
							.stdout(Stdio::piped())
							.stderr(Stdio::piped())
							.kill_on_drop(true)
							.spawn()
							.expect("spawn signal command");
						let output = tokio::time::timeout(
							std::time::Duration::from_secs(20),
							child.wait_with_output(),
						)
						.await;
						match output {
							Ok(Ok(output)) => {
								let line = serde_json::from_slice::<serde_json::Value>(
									output.stdout.trim_ascii(),
								)
								.unwrap_or(serde_json::Value::Null);
								let marker = if fault == "kill" { "gone" } else { "stopped" };
								serde_json::json!({
									"command": argv,
									"status": output.status.code(),
									"stdout": String::from_utf8_lossy(&output.stdout),
									"stderr": String::from_utf8_lossy(&output.stderr),
									"elapsed_ms": started.elapsed().as_millis() as u64,
									"confirmed": output.status.success() && line[marker].as_bool() == Some(true),
								})
							}
							Ok(Err(error)) => {
								serde_json::json!({ "command": argv, "error": error.to_string(), "confirmed": false })
							}
							Err(_) => {
								serde_json::json!({ "command": argv, "error": "signal command exceeded 20 s", "confirmed": false })
							}
						}
					})
				}),
			})
		}
	};
	let mut evidence = probe_base_evidence(&spec, "mid_task");
	// ADR 628 B1.C: a spec may declare the reliance observer (Wi-Fi campaign, production path).
	let instrument =
		(spec.raw["reliance_observer"].as_bool() == Some(true)).then(|| RelianceInstrument {
			observer: Arc::new(reliance_faults::EventLogRelianceObserver { log: log.clone() }),
			renewal_transport: None,
		});
	let run = adr627_run(
		path,
		&case,
		&spec.workers,
		spec.transport.clone(),
		log,
		spec.recovery,
		fault,
		spec.deadline,
	);
	let run = match instrument {
		Some(instrument) => RELIANCE_INSTRUMENT.scope(instrument, run).await,
		None => run.await,
	};
	if let serde_json::Value::Object(map) = run {
		for (key, value) in map {
			evidence[key] = value;
		}
	}
	write_probe_evidence(&spec.attempt_dir, &evidence);
}

/// The local mid-task reproduction: three QUIC loopback fixture workers that record wait events,
/// one combination of path, case and fault from the environment, the victim signalled through its
/// own child process. Its evidence and every log are written to `TORON_ADR627_LOCAL_DIR`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ADR 627 local diagnostic: run explicitly with TORON_ADR627_LOCAL_DIR"]
async fn mid_task_loss_is_observed_on_loopback() {
	let dir = std::path::PathBuf::from(
		std::env::var("TORON_ADR627_LOCAL_DIR").expect("TORON_ADR627_LOCAL_DIR"),
	);
	std::fs::create_dir(&dir).expect("a fresh diagnostic directory");
	let path = Adr627Path::parse(
		&std::env::var("TORON_ADR627_LOCAL_PATH").expect("TORON_ADR627_LOCAL_PATH"),
	);
	let case = std::env::var("TORON_ADR627_LOCAL_CASE").expect("TORON_ADR627_LOCAL_CASE");
	let fault_kind = std::env::var("TORON_ADR627_LOCAL_FAULT").expect("TORON_ADR627_LOCAL_FAULT");
	assert!(
		matches!(case.as_str(), "slow_scan" | "slow_join" | "long_join_last"),
		"case {case}"
	);
	assert!(
		matches!(fault_kind.as_str(), "kill" | "stop" | "none"),
		"fault {fault_kind}"
	);
	with_quic(async {
		let mut workers = Vec::new();
		for id in 62_701..=62_703u128 {
			workers.push(
				FixtureWorkerProcess::spawn_recorded(
					WorkerId::from_u128(id),
					Some(&dir.join(format!("dials-{id}.txt"))),
					Some(&dir.join(format!("worker-{id}.jsonl"))),
				)
				.await,
			);
		}
		let log = wait_events::EventLog::create(&dir.join("probe_events.jsonl")).expect("fresh probe events");
		let transport: Arc<dyn ToronTransport> = Arc::new(wait_events::ObservedTransport {
			inner: process_transport(),
			log: log.clone(),
			role: "dispatch",
		});
		let declared = workers.iter().map(|worker| (worker.worker_id, worker.endpoint.clone())).collect::<Vec<_>>();
		// The first registry entry takes the first dispatch: the target is declared, then verified.
		let target = declared[0].0;
		let pid = workers[0].child.id().expect("victim pid");
		let fault: &'static str = if fault_kind == "kill" { "kill" } else { "stop" };
		let recovery = (path == Adr627Path::Scheduler).then(|| {
			toron::distributed::recovery::RecoveryExperiment::from_millis(ADR626_DECLARED.0, ADR626_DECLARED.1).expect("declared")
		});
		let victim_stage = std::env::var("TORON_ADR627_LOCAL_STAGE")
			.ok()
			.map(|stage| stage.parse().expect("stage"))
			.unwrap_or(0);
		let signal = (fault_kind != "none").then(|| Adr627Fault {
			target,
			victim_stage,
			fault,
			signal: Box::new(move || {
				Box::pin(async move {
					let signal = if fault == "kill" { "-KILL" } else { "-STOP" };
					let status = Command::new("kill").arg(signal).arg(pid.to_string()).status().await;
					// Confirm the effect from /proc: gone for a kill, state T for a stop.
					let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
					let mut state = None;
					while std::time::Instant::now() < deadline {
						state = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok().and_then(|stat| {
							stat.rsplit_once(") ").and_then(|(_, rest)| rest.split_whitespace().next().map(str::to_string))
						});
						let confirmed = match fault {
							"kill" => state.is_none() || state.as_deref() == Some("Z"),
							_ => state.as_deref() == Some("T"),
						};
						if confirmed {
							break;
						}
						tokio::time::sleep(std::time::Duration::from_millis(20)).await;
					}
					let confirmed = match fault {
						"kill" => state.is_none() || state.as_deref() == Some("Z"),
						_ => state.as_deref() == Some("T"),
					};
					serde_json::json!({
						"pid": pid,
						"kill_status": status.map(|status| status.code()).map_err(|error| error.to_string()),
						"proc_state": state,
						"confirmed": confirmed,
					})
				})
			}),
		});
		let recovery_text = recovery.map(|_| "declared");
		let mut evidence = adr627_run(path, &case, &declared, transport, log, recovery, signal, std::time::Duration::from_secs(60)).await;
		evidence["local"] = serde_json::json!({ "recovery": recovery_text, "victim_pid": pid });
		// Cleanup: a stopped victim is killed after the window; its stream endings from here on are
		// cleanup-induced, recorded after observation_end.
		let cleanup_kill = Command::new("kill").arg("-KILL").arg(pid.to_string()).status().await;
		evidence["cleanup"] = serde_json::json!({
			"victim_kill_status": cleanup_kill.map(|status| status.code()).map_err(|error| error.to_string()),
			"after_observation": true,
		});
		use std::io::Write as _;
		std::fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(dir.join("local.json"))
			.expect("fresh local evidence")
			.write_all(serde_json::to_string_pretty(&evidence).expect("json").as_bytes())
			.expect("write local evidence");
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

// ---- ADR 628 Stage A: lease fault controls on loopback (current behaviour; no mechanism change) ----

/// The frozen post-terminal window in which every worker's served-stream log is kept open, so any
/// `CancelTask` / `RetireQuery` fanout that follows the terminal return is captured.
const ADR628_POST_TERMINAL_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);
/// ADR 628 B1.C: the bounded wait, after the raw `query_returned`, for the query's own renewal cleanup
/// record. A missing or unverified receipt still refuses at the reader.
const ADR628_CLEANUP_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
/// ADR 628: workers stay up this long after an absence window ends, so every managed worker's log
/// records a reaper pass past it (the reader withholds absence otherwise).
const ADR628_COVERAGE_TAIL: std::time::Duration = std::time::Duration::from_millis(1500);

/// One declared lease fault control. `delays` act on the coordinator's dials to the target (synthetic
/// caller delays); `route` puts a relay on the coordinator's route to the target only (the target
/// still advertises its direct endpoint, so peer pulls bypass the relay).
struct Adr628Control {
	name: &'static str,
	delays: lease_faults::PhaseDelays,
	route: Option<Adr628Route>,
	kill_target: bool,
}

struct Adr628Route {
	delay: std::time::Duration,
	loss_per_mille: u64,
	seed: u64,
	/// A blackout starting when the coordinator reads the target's first sealed producer manifest
	/// (decoded from a whole `TaskComplete` frame, on the owner's clock), lasting `Some(d)` or
	/// permanently.
	blackout: Option<Option<std::time::Duration>>,
}

fn adr628_control(name: &str) -> Adr628Control {
	use lease_faults::PhaseDelays;
	use std::time::Duration;
	let ms = Duration::from_millis;
	let delays = |connect, write, read, close| PhaseDelays {
		connect: ms(connect),
		write: ms(write),
		read: ms(read),
		close: ms(close),
	};
	let route = |delay, loss_per_mille, blackout| {
		Some(Adr628Route {
			delay: ms(delay),
			loss_per_mille,
			seed: 628,
			blackout,
		})
	};
	let (name, delays, route, kill_target): (&'static str, _, _, _) = match name {
		"none" => ("none", PhaseDelays::default(), None, false),
		"connect_150" => ("connect_150", delays(150, 0, 0, 0), None, false),
		"connect_50" => ("connect_50", delays(50, 0, 0, 0), None, false),
		"write_150" => ("write_150", delays(0, 150, 0, 0), None, false),
		"read_150" => ("read_150", delays(0, 0, 150, 0), None, false),
		"close_150" => ("close_150", delays(0, 0, 0, 150), None, false),
		// Inside-bound twins of the operation-phase delays.
		"write_50" => ("write_50", delays(0, 50, 0, 0), None, false),
		"read_50" => ("read_50", delays(0, 0, 50, 0), None, false),
		"close_50" => ("close_50", delays(0, 0, 0, 50), None, false),
		"route_none" => (
			"route_none",
			PhaseDelays::default(),
			route(0, 0, None),
			false,
		),
		"route_delay_40" => (
			"route_delay_40",
			PhaseDelays::default(),
			route(40, 0, None),
			false,
		),
		"route_loss_100" => (
			"route_loss_100",
			PhaseDelays::default(),
			route(0, 100, None),
			false,
		),
		"route_blackout_8s" => (
			"route_blackout_8s",
			PhaseDelays::default(),
			route(0, 0, Some(Some(Duration::from_secs(8)))),
			false,
		),
		"route_unreachable" => (
			"route_unreachable",
			PhaseDelays::default(),
			route(0, 0, Some(None)),
			false,
		),
		"census_worker_kill" => ("census_worker_kill", PhaseDelays::default(), None, true),
		other => panic!("unknown ADR 628 control {other}"),
	};
	Adr628Control {
		name,
		delays,
		route,
		kill_target,
	}
}

/// A declared lease fault control on loopback: three recorded fixture workers, the production
/// coordinator path, `long_join_last` (its 30 s producer scan spans several renewal ticks), the target
/// being the first registry entry. Evidence and every log go to `TORON_ADR628_DIR`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ADR 628 Stage A control: run explicitly with TORON_ADR628_DIR and TORON_ADR628_CONTROL"]
async fn lease_fault_control_on_loopback() {
	let dir =
		std::path::PathBuf::from(std::env::var("TORON_ADR628_DIR").expect("TORON_ADR628_DIR"));
	std::fs::create_dir(&dir).expect("a fresh control directory");
	let control =
		adr628_control(&std::env::var("TORON_ADR628_CONTROL").expect("TORON_ADR628_CONTROL"));
	with_quic(async {
		// ADR 628: lifecycle controls need the managed store and its reaper on every worker.
		let mut workers = Vec::new();
		for id in 62_801..=62_803u128 {
			workers.push(
				MANAGED_LIFECYCLE
					.scope(
						(),
						FixtureWorkerProcess::spawn_recorded(
							WorkerId::from_u128(id),
							Some(&dir.join(format!("dials-{id}.txt"))),
							Some(&dir.join(format!("worker-{id}.jsonl"))),
						),
					)
					.await,
			);
		}
		let log = wait_events::EventLog::create(&dir.join("probe_events.jsonl")).expect("fresh probe events");
		let observed: Arc<dyn ToronTransport> = Arc::new(wait_events::ObservedTransport {
			inner: process_transport(),
			log: log.clone(),
			role: "dispatch",
		});
		let target = workers[0].worker_id;
		let transport: Arc<dyn ToronTransport> =
			Arc::new(lease_faults::PhaseDelayTransport::new(observed, target, control.delays, log.clone()));
		// The coordinator's route to the target, through a relay when the control declares one.
		let relay = match &control.route {
			Some(route) => {
				let direct: std::net::SocketAddr = workers[0].endpoint.as_str().parse().expect("loopback endpoint");
				Some(UdpRelay::start(direct, route.delay, route.loss_per_mille, route.seed).await)
			}
			None => None,
		};
		let mut declared = workers
			.iter()
			.map(|worker| (worker.worker_id, worker.endpoint.clone()))
			.collect::<Vec<_>>();
		if let Some(relay) = &relay {
			declared[0].1 = WorkerEndpoint::new(relay.addr.to_string());
		}
		let blackout = control.route.as_ref().and_then(|route| route.blackout);
		let relay_ref = relay.as_ref();
		// Installed before dispatch, so the trigger event cannot be missed.
		let mut trigger_events = log.subscribe();
		let target_text = target.get().as_u128().to_string();
		let schedule = async {
			let Some(lasting) = blackout else { return serde_json::json!(null) };
			let relay = relay_ref.expect("a blackout needs a relay");
			// Trigger: the coordinator reads the target's first sealed producer manifest, positively
			// decoded from a whole `TaskComplete` frame (`task_complete_manifest_read`).
			let trigger = loop {
				let Some(event) = trigger_events.recv().await else {
					return serde_json::json!({ "outcome": "event log closed before the trigger" });
				};
				if event["event"] == "observation_end" {
					return serde_json::json!({ "outcome": "the query ended before the trigger; no blackout applied" });
				}
				if event["event"] == "task_complete_manifest_read"
					&& event["producer_worker"] == target_text.as_str()
				{
					break event;
				}
			};
			relay.blackhole();
			let started = log.now_us();
			log.emit_at(started, "fault_blackout_start", "dispatch", serde_json::json!({ "trigger_task": trigger["task"].clone(), "trigger_exchange": trigger["exchange"].clone(), "trigger_stream": trigger["stream"].clone() }));
			if let Some(lasting) = lasting {
				tokio::time::sleep(lasting).await;
				relay.restore();
				log.emit("fault_blackout_end", "dispatch", serde_json::json!({ "configured_ms": lasting.as_millis() as u64 }));
			}
			serde_json::json!({
				"trigger": "the coordinator read the target's first sealed producer manifest",
				"trigger_task": trigger["task"].clone(),
				"trigger_exchange": trigger["exchange"].clone(),
				"trigger_stream": trigger["stream"].clone(),
				"started_us": started,
				"configured_lasting_ms": lasting.map(|d| d.as_millis() as u64),
			})
		};
		let pid = workers[0].child.id().expect("target pid");
		// Census worker kill: the SIGKILL fires 3 s after the target's stage-0 `StartTask` is written
		// (ADR 627's trigger), usually after the fast producer's output, while the target stays in the
		// lease census. It measures losing a census worker; it is NOT a mid-scan loss, and its
		// mid-scan binding is recorded as missed coverage.
		let fault = control.kill_target.then(|| Adr627Fault {
			target,
			victim_stage: 0,
			fault: "kill",
			signal: Box::new(move || {
				Box::pin(async move {
					let status = Command::new("kill").arg("-KILL").arg(pid.to_string()).status().await;
					tokio::time::sleep(std::time::Duration::from_millis(200)).await;
					let state = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok().and_then(|stat| {
						stat.rsplit_once(") ").and_then(|(_, rest)| rest.split_whitespace().next().map(str::to_string))
					});
					serde_json::json!({
						"pid": pid,
						"kill_status": status.map(|status| status.code()).map_err(|error| error.to_string()),
						"proc_state": state,
						"confirmed": state.is_none() || state.as_deref() == Some("Z"),
					})
				})
			}),
		});
		let run = adr627_run(
			Adr627Path::Coordinator,
			"long_join_last",
			&declared,
			transport,
			log.clone(),
			None,
			fault,
			std::time::Duration::from_secs(120),
		);
		let (mut evidence, blackout_record) = tokio::join!(run, schedule);
		// The frozen post-terminal window: workers stay up so any fanout lands in their logs.
		log.emit("post_terminal_window_start", "dispatch", serde_json::json!({ "window_ms": ADR628_POST_TERMINAL_WINDOW.as_millis() as u64 }));
		tokio::time::sleep(ADR628_POST_TERMINAL_WINDOW).await;
		log.emit("post_terminal_window_end", "dispatch", serde_json::json!({}));
		// The coverage tail: each managed worker records at least one reaper pass after the window end.
		tokio::time::sleep(ADR628_COVERAGE_TAIL).await;
		evidence["adr628"] = serde_json::json!({
			"control": control.name,
			"delays": control.delays.evidence(),
			"route": control.route.as_ref().map(|route| serde_json::json!({
				"delay_ms_each_way": route.delay.as_millis() as u64,
				"loss_per_mille": route.loss_per_mille,
				"seed": route.seed,
			})),
			"blackout": blackout_record,
			"target": target.get().as_u128().to_string(),
			"target_direct_endpoint": workers[0].endpoint.as_str(),
			"coordinator_endpoint_for_target": declared[0].1.as_str(),
			"post_terminal_window_ms": ADR628_POST_TERMINAL_WINDOW.as_millis() as u64,
		});
		if let Some(relay) = relay {
			evidence["adr628"]["relay_realized"] = relay.shutdown().await;
		}
		use std::io::Write as _;
		std::fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(dir.join("control.json"))
			.expect("fresh control evidence")
			.write_all(serde_json::to_string_pretty(&evidence).expect("json").as_bytes())
			.expect("write control evidence");
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

/// ADR 628 B1.C: one declared reliance control. `case` is `long_join_last` (30 s) or
/// `horizon_join_last` (the same query with the long scan at `long_scan_ms`).
struct RelianceControl {
	name: &'static str,
	case: &'static str,
	long_scan_ms: Option<u64>,
	renewal: Option<(reliance_faults::RenewalFault, reliance_faults::Firing)>,
	route: Option<Adr628Route>,
	/// The relay blackout starts at the target's first renewal refresh (a recorded `applied` /
	/// `installed` reliance event for a renewal exchange of the target), lasting `Some(d)` or
	/// permanently.
	kill_target: bool,
}

/// ADR 628 B1.C: the 110 s horizon case (about 3.2 initial horizons of 33.966 s).
const RELIANCE_HORIZON_SCAN_MS: u64 = 110_000;

fn reliance_control(name: &str) -> RelianceControl {
	use lease_faults::PhaseDelays;
	use reliance_faults::{Firing, RenewalFault};
	use std::time::Duration;
	let ms = Duration::from_millis;
	let delays = |connect, read, close| {
		RenewalFault::Delays(PhaseDelays {
			connect: ms(connect),
			write: ms(0),
			read: ms(read),
			close: ms(close),
		})
	};
	let route = |delay, loss_per_mille, blackout| {
		Some(Adr628Route {
			delay: ms(delay),
			loss_per_mille,
			seed: 628,
			blackout,
		})
	};
	let short = ("long_join_last", None);
	let horizon = ("horizon_join_last", Some(RELIANCE_HORIZON_SCAN_MS));
	let (name, (case, long_scan_ms), renewal, route, kill_target): (&'static str, _, _, _, _) =
		match name {
			"none" => ("none", short, None, None, false),
			"ren_connect_1500" => (
				"ren_connect_1500",
				short,
				Some((delays(1500, 0, 0), Firing::Every)),
				None,
				false,
			),
			"ren_read_1500" => (
				"ren_read_1500",
				short,
				Some((delays(0, 1500, 0), Firing::Every)),
				None,
				false,
			),
			"ren_close_1500" => (
				"ren_close_1500",
				short,
				Some((delays(0, 0, 1500), Firing::Every)),
				None,
				false,
			),
			"ren_connect_2500" => (
				"ren_connect_2500",
				horizon,
				Some((delays(2500, 0, 0), Firing::Every)),
				None,
				false,
			),
			"ren_read_2500" => (
				"ren_read_2500",
				horizon,
				Some((delays(0, 2500, 0), Firing::Every)),
				None,
				false,
			),
			"ren_close_3500" => (
				"ren_close_3500",
				horizon,
				Some((delays(0, 0, 3500), Firing::Every)),
				None,
				false,
			),
			"ren_withhold_once" => (
				"ren_withhold_once",
				horizon,
				Some((RenewalFault::WithholdReply, Firing::Only(2))),
				None,
				false,
			),
			"ren_withhold_all" => (
				"ren_withhold_all",
				horizon,
				Some((RenewalFault::WithholdReply, Firing::From(2))),
				None,
				false,
			),
			"ren_refuse" => (
				"ren_refuse",
				horizon,
				Some((RenewalFault::Refuse, Firing::Only(2))),
				None,
				false,
			),
			"route_delay_40" => ("route_delay_40", short, None, route(40, 0, None), false),
			"route_loss_100" => ("route_loss_100", short, None, route(0, 100, None), false),
			"route_blackout_8s" => (
				"route_blackout_8s",
				horizon,
				None,
				route(0, 0, Some(Some(Duration::from_secs(8)))),
				false,
			),
			"route_blackout_40s" => (
				"route_blackout_40s",
				horizon,
				None,
				route(0, 0, Some(Some(Duration::from_secs(40)))),
				false,
			),
			"route_unreachable" => (
				"route_unreachable",
				horizon,
				None,
				route(0, 0, Some(None)),
				false,
			),
			"census_worker_kill" => ("census_worker_kill", short, None, None, true),
			"horizon_110" => ("horizon_110", horizon, None, None, false),
			other => panic!("unknown ADR 628 B1.C control {other}"),
		};
	RelianceControl {
		name,
		case,
		long_scan_ms,
		renewal,
		route,
		kill_target,
	}
}

/// ADR 628 B1.C: a declared reliance control on loopback. Three managed fixture workers (the long
/// scan configured for every worker when the case declares it), the production coordinator path
/// with the reliance observer on the probe log (seam S2), and, for a renewal-path control, the
/// renewal transport carrying the fault (seam S1, renewal dials only). The target is the first
/// registry entry. Evidence and every log go to `TORON_ADR628_DIR`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ADR 628 B1.C control: run explicitly with TORON_ADR628_DIR and TORON_ADR628_CONTROL"]
async fn reliance_control_on_loopback() {
	let dir =
		std::path::PathBuf::from(std::env::var("TORON_ADR628_DIR").expect("TORON_ADR628_DIR"));
	std::fs::create_dir(&dir).expect("a fresh control directory");
	let control =
		reliance_control(&std::env::var("TORON_ADR628_CONTROL").expect("TORON_ADR628_CONTROL"));
	with_quic(async {
		let mut workers = Vec::new();
		for id in 62_801..=62_803u128 {
			let dials = dir.join(format!("dials-{id}.txt"));
			let events = dir.join(format!("worker-{id}.jsonl"));
			let spawn = MANAGED_LIFECYCLE.scope(
				(),
				FixtureWorkerProcess::spawn_recorded(
					WorkerId::from_u128(id),
					Some(&dials),
					Some(&events),
				),
			);
			workers.push(match control.long_scan_ms {
				Some(ms) => LONG_SCAN_MS.scope(ms, spawn).await,
				None => spawn.await,
			});
		}
		let log = wait_events::EventLog::create(&dir.join("probe_events.jsonl")).expect("fresh probe events");
		let observed: Arc<dyn ToronTransport> = Arc::new(wait_events::ObservedTransport {
			inner: process_transport(),
			log: log.clone(),
			role: "dispatch",
		});
		let target = workers[0].worker_id;
		// The renewal transport (S1) records its dials in the same log, through its own observed layer.
		let renewal = control.renewal.map(|(fault, firing)| {
			let observed_renewal: Arc<dyn ToronTransport> = Arc::new(wait_events::ObservedTransport {
				inner: process_transport(),
				log: log.clone(),
				role: "renewal",
			});
			Arc::new(reliance_faults::RenewalFaultTransport::new(
				observed_renewal,
				target,
				fault,
				firing,
				log.clone(),
			))
		});
		let relay = match &control.route {
			Some(route) => {
				let direct: std::net::SocketAddr = workers[0].endpoint.as_str().parse().expect("loopback endpoint");
				Some(UdpRelay::start(direct, route.delay, route.loss_per_mille, route.seed).await)
			}
			None => None,
		};
		let mut declared = workers
			.iter()
			.map(|worker| (worker.worker_id, worker.endpoint.clone()))
			.collect::<Vec<_>>();
		if let Some(relay) = &relay {
			// Coordinator traffic to the target (dispatch, renewal, retirement) takes the relay; peer
			// pulls use the workers' direct advertised endpoints.
			declared[0].1 = WorkerEndpoint::new(relay.addr.to_string());
		}
		let blackout = control.route.as_ref().and_then(|route| route.blackout);
		let relay_ref = relay.as_ref();
		let mut trigger_events = log.subscribe();
		let target_text = target.get().as_u128().to_string();
		let schedule = async {
			let Some(lasting) = blackout else { return serde_json::json!(null) };
			let relay = relay_ref.expect("a blackout needs a relay");
			// Trigger: the target's first renewal refresh recorded by the ledger. The cut's installed
			// bound is that event's `new_bound_ns`.
			let trigger = loop {
				let Some(event) = trigger_events.recv().await else {
					return serde_json::json!({ "outcome": "event log closed before the trigger" });
				};
				if event["event"] == "observation_end" {
					return serde_json::json!({ "outcome": "the query ended before the trigger; no blackout applied" });
				}
				if event["event"] == "reliance"
					&& event["kind"] == "applied"
					&& event["result"] == "installed"
					&& event["worker"] == target_text.as_str()
					&& event["exchange"]["kind"] == "renewal"
				{
					break event;
				}
			};
			// The cut and the restoration are sampled on the coordinator's boot clock immediately after
			// each takes effect (the ledger's clock domain); an unavailable sample is null.
			let boot_ns = || {
				use toron::distributed::query_reliance::RelianceClock as _;
				toron::distributed::query_reliance::BootClock
					.now()
					.ok()
					.and_then(|instant| u64::try_from(instant.as_nanos()).ok())
			};
			relay.blackhole();
			let cut_boot_ns = boot_ns();
			let started = log.now_us();
			log.emit_at(started, "fault_blackout_start", "dispatch", serde_json::json!({
				"trigger_seq": trigger["seq"].clone(), "trigger_attempt": trigger["exchange"]["attempt"].clone(),
				"bound_at_cut_ns": trigger["new_bound_ns"].clone(), "trigger_at_ns": trigger["at_ns"].clone(),
				"cut_boot_ns": cut_boot_ns,
			}));
			let mut restore_boot_ns = None;
			if let Some(lasting) = lasting {
				tokio::time::sleep(lasting).await;
				relay.restore();
				restore_boot_ns = boot_ns();
				log.emit("fault_blackout_end", "dispatch", serde_json::json!({
					"configured_ms": lasting.as_millis() as u64, "restore_boot_ns": restore_boot_ns,
				}));
			}
			serde_json::json!({
				"trigger": "the target's first renewal refresh (reliance applied/installed, renewal exchange)",
				"trigger_seq": trigger["seq"].clone(),
				"trigger_attempt": trigger["exchange"]["attempt"].clone(),
				"bound_at_cut_ns": trigger["new_bound_ns"].clone(),
				"started_us": started,
				"cut_boot_ns": cut_boot_ns,
				"restore_boot_ns": restore_boot_ns,
				"configured_lasting_ms": lasting.map(|d| d.as_millis() as u64),
			})
		};
		let pid = workers[0].child.id().expect("target pid");
		// Census worker kill: Stage A's trigger (3 s after the target's stage-0 `StartTask` write). It
		// measures losing a census worker, not a mid-scan loss; permitted failure paths are read from
		// the actual events.
		let fault = control.kill_target.then(|| Adr627Fault {
			target,
			victim_stage: 0,
			fault: "kill",
			signal: Box::new(move || {
				Box::pin(async move {
					let status = Command::new("kill").arg("-KILL").arg(pid.to_string()).status().await;
					tokio::time::sleep(std::time::Duration::from_millis(200)).await;
					let state = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok().and_then(|stat| {
						stat.rsplit_once(") ").and_then(|(_, rest)| rest.split_whitespace().next().map(str::to_string))
					});
					serde_json::json!({
						"pid": pid,
						"kill_status": status.map(|status| status.code()).map_err(|error| error.to_string()),
						"proc_state": state,
						"confirmed": state.is_none() || state.as_deref() == Some("Z"),
					})
				})
			}),
		});
		let instrument = RelianceInstrument {
			observer: Arc::new(reliance_faults::EventLogRelianceObserver { log: log.clone() }),
			renewal_transport: renewal
				.clone()
				.map(|transport| transport as Arc<dyn ToronTransport>),
		};
		// The query bound covers the horizon case's 110 s scan.
		let run = RELIANCE_INSTRUMENT.scope(
			instrument,
			adr627_run(
				Adr627Path::Coordinator,
				control.case,
				&declared,
				observed,
				log.clone(),
				None,
				fault,
				std::time::Duration::from_secs(240),
			),
		);
		let (mut evidence, blackout_record) = tokio::join!(run, schedule);
		log.emit("post_terminal_window_start", "dispatch", serde_json::json!({ "window_ms": ADR628_POST_TERMINAL_WINDOW.as_millis() as u64 }));
		tokio::time::sleep(ADR628_POST_TERMINAL_WINDOW).await;
		log.emit("post_terminal_window_end", "dispatch", serde_json::json!({}));
		tokio::time::sleep(ADR628_COVERAGE_TAIL).await;
		evidence["adr628_b1c"] = serde_json::json!({
			"control": control.name,
			"case": control.case,
			"long_scan_ms": control.long_scan_ms,
			"renewal_fault": control.renewal.map(|(fault, firing)| serde_json::json!({
				"fault": fault.evidence(), "firing": firing.evidence(),
			})),
			"renewal_dials": renewal.as_ref().map(|t| t.dials.load(std::sync::atomic::Ordering::SeqCst)),
			"renewal_faults_applied": renewal.as_ref().map(|t| t.applied.load(std::sync::atomic::Ordering::SeqCst)),
			"route": control.route.as_ref().map(|route| serde_json::json!({
				"delay_ms_each_way": route.delay.as_millis() as u64,
				"loss_per_mille_each_direction": route.loss_per_mille,
				"seed": route.seed,
				"applies_to": "coordinator traffic to the target (dispatch, renewal, retirement); peer pulls direct",
			})),
			"blackout": blackout_record,
			"target": target_text,
			"target_direct_endpoint": workers[0].endpoint.as_str(),
			"coordinator_endpoint_for_target": declared[0].1.as_str(),
			"post_terminal_window_ms": ADR628_POST_TERMINAL_WINDOW.as_millis() as u64,
			"instrument_failed": log.failed(),
		});
		if let Some(relay) = relay {
			evidence["adr628_b1c"]["relay_realized"] = relay.shutdown().await;
		}
		use std::io::Write as _;
		std::fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(dir.join("control.json"))
			.expect("fresh control evidence")
			.write_all(serde_json::to_string_pretty(&evidence).expect("json").as_bytes())
			.expect("write control evidence");
		for worker in &mut workers {
			worker.shutdown().await;
		}
		// toron_fixture_worker LONG_SCAN default (30 s); a change there must fail here.
		let expected_long_scan_ms = control.long_scan_ms.unwrap_or(30_000);
		for worker in &workers {
			let path = dir.join(format!("worker-{}.jsonl", worker.worker_id.get().as_u128()));
			let contents = std::fs::read_to_string(&path).expect("worker fixture event log");
			let fixture_config = contents
				.lines()
				.filter(|line| !line.trim().is_empty())
				.map(|line| {
					serde_json::from_str::<serde_json::Value>(line)
						.expect("worker event log line is JSON")
				})
				.filter(|event| event["event"] == "fixture_config")
				.collect::<Vec<_>>();
			assert_eq!(
				fixture_config.len(),
				1,
				"one fixture_config event for {}",
				worker.worker_id.get().as_u128()
			);
			assert_eq!(
				fixture_config[0]["long_scan_ms"].as_u64(),
				Some(expected_long_scan_ms),
				"configured long scan for {}",
				worker.worker_id.get().as_u128()
			);
		}
	})
	.await;
}

/// ADR 628 Stage A (owner disappearance, primary): the owner is a child process on the production
/// coordinator path (`DistributedQueryCoordinator::execute_query`: admission, active query, periodic
/// renewal), running `long_join_last`. It is SIGKILLed once the fast producer's sealed manifest is
/// observed at the production boundary: decoded from the `TaskComplete` frame the coordinator read
/// (`task_complete_manifest_read`). See `owner_disappearance_control`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ADR 628 Stage A control: run explicitly with TORON_ADR628_DIR"]
async fn owner_disappearance_on_coordinator_path() {
	owner_disappearance_control("coordinator", "task_complete_manifest_read", 62_811).await;
}

/// ADR 628 Stage A (scheduler owner disappearance, secondary): the same control with the owner on
/// the scheduler path (per-dispatch lease issuance, no renewals), bound by the scheduler probe's
/// `manifest_received`. It is not the production-coordinator case and is reported separately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "ADR 628 Stage A control: run explicitly with TORON_ADR628_DIR"]
async fn scheduler_owner_disappearance_on_loopback() {
	owner_disappearance_control("scheduler", "manifest_received", 62_821).await;
}

/// The owner (a child test process on `path`) is SIGKILLed after the first sealed producer manifest
/// (`bind_event`); the managed workers stay alive. On the producer worker's own clock the control
/// then waits conservatively: the last `lease_apply_end` for the bound query + the term (any later
/// application refreshes it), then the first completed reaper pass after that. The last `QueryLease`
/// read + term is recorded only as a candidate lower bound. It issues ONLY `PullPartition` for the
/// exact received address and records the typed result as observed. Every outcome is recorded;
/// nothing here forces race (a), which the deterministic store pin covers separately.
async fn owner_disappearance_control(
	path: &'static str,
	bind_event: &'static str,
	first_worker: u128,
) {
	let dir =
		std::path::PathBuf::from(std::env::var("TORON_ADR628_DIR").expect("TORON_ADR628_DIR"));
	std::fs::create_dir(&dir).expect("a fresh control directory");
	let term_us = u64::try_from(
		toron::distributed::namespace_lifecycle::ShuffleLifecycleConfig::from_worker_loss_window(
			toron::distributed::partition_store::DEFAULT_WORKER_LOST_AFTER,
		)
		.expect("config")
		.lease_term()
		.as_micros(),
	)
	.expect("term");
	let harness_limit = std::time::Duration::from_secs(90);
	with_quic(async {
		let mut workers = Vec::new();
		for id in first_worker..=first_worker + 2 {
			workers.push(
				MANAGED_LIFECYCLE
					.scope(
						(),
						FixtureWorkerProcess::spawn_recorded(
							WorkerId::from_u128(id),
							Some(&dir.join(format!("dials-{id}.txt"))),
							Some(&dir.join(format!("worker-{id}.jsonl"))),
						),
					)
					.await,
			);
		}
		let owner_dir = dir.join("owner");
		let spec = serde_json::json!({
			"workers": workers.iter().map(|w| serde_json::json!({ "worker_u128": w.worker_id.get().as_u128().to_string(), "endpoint": w.endpoint.as_str() })).collect::<Vec<_>>(),
			"loopback": true,
			"cluster_domain": "loopback",
			"deadline_secs": 120,
			"wait_events": true,
			"path": path,
			"case": "long_join_last",
			"fault": "none",
			"attempt_dir": owner_dir.display().to_string(),
		});
		std::fs::create_dir(&owner_dir).expect("owner dir");
		let spec_path = dir.join("owner.spec.json");
		std::fs::write(&spec_path, serde_json::to_string_pretty(&spec).expect("spec")).expect("write spec");
		let mut evidence = serde_json::json!({
			"mode": if path == "coordinator" { "owner_disappearance" } else { "scheduler_owner_disappearance" },
			"bind_event": bind_event,
			"spec": spec,
			"term_us": term_us,
		});
		// The harness's own raw log: owner lifecycle, the wait it chose, the observed inspection
		// exchange, and the absence window. `owner.json` is a summary; the reader recomputes it.
		let harness = wait_events::EventLog::create(&dir.join("harness_events.jsonl")).expect("fresh harness events");
		let mut owner = Command::new(std::env::current_exe().expect("test executable"))
			.args(["--ignored", "--exact", "multinode_probe_mid_task", "--nocapture"])
			.env("TORON_MULTINODE_SPEC", &spec_path)
			.stdout(std::fs::File::create(dir.join("owner.stdout")).expect("owner stdout"))
			.stderr(std::fs::File::create(dir.join("owner.stderr")).expect("owner stderr"))
			.kill_on_drop(true)
			.spawn()
			.expect("spawn the owner");
		let owner_pid = owner.id().expect("owner pid");
		harness.emit("owner_spawned", "harness", serde_json::json!({ "pid": owner_pid }));
		let read_log = |path: &std::path::Path| -> Vec<serde_json::Value> {
			std::fs::read_to_string(path)
				.unwrap_or_default()
				.lines()
				.filter_map(|line| serde_json::from_str(line).ok())
				.collect()
		};
		let finish = |evidence: &mut serde_json::Value, outcome: &str| {
			evidence["outcome"] = outcome.into();
			harness.emit("owner_control_end", "harness", serde_json::json!({ "outcome": outcome }));
		};
		// 1. Bind the fast producer's exact received manifest before the kill.
		let started = std::time::Instant::now();
		let manifest = loop {
			let events = read_log(&owner_dir.join("probe_events.jsonl"));
			if let Some(manifest) = events.into_iter().find(|e| e["event"] == bind_event) {
				break Some(manifest);
			}
			if started.elapsed() > std::time::Duration::from_secs(30) {
				break None;
			}
			tokio::time::sleep(std::time::Duration::from_millis(20)).await;
		};
		// 2. SIGKILL the owner and confirm, from its reaped exit status, that signal 9 ended it.
		let kill_owner = async |owner: &mut tokio::process::Child| {
			use std::os::unix::process::ExitStatusExt as _;
			harness.emit("owner_kill_sent", "harness", serde_json::json!({ "pid": owner_pid }));
			let kill_status = Command::new("kill").arg("-KILL").arg(owner_pid.to_string()).status().await;
			let reaped = tokio::time::timeout(std::time::Duration::from_secs(10), owner.wait()).await;
			let (signal, code, error) = match &reaped {
				Ok(Ok(status)) => (status.signal(), status.code(), None),
				Ok(Err(error)) => (None, None, Some(error.to_string())),
				Err(_) => (None, None, Some("not reaped within 10 s".to_string())),
			};
			harness.emit(
				"owner_reaped",
				"harness",
				serde_json::json!({
					"pid": owner_pid,
					"kill_status": kill_status.as_ref().ok().and_then(|status| status.code()),
					"signal": signal,
					"code": code,
					"error": error,
				}),
			);
			signal == Some(9)
		};
		let Some(manifest) = manifest else {
			kill_owner(&mut owner).await;
			finish(&mut evidence, "no manifest bound within the harness limit; the owner was killed only for cleanup");
			write_owner_evidence(&dir, &evidence);
			for worker in &mut workers {
				worker.shutdown().await;
			}
			return;
		};
		harness.emit("owner_bound", "harness", serde_json::json!({ "bind_event": bind_event, "manifest": manifest.clone() }));
		evidence["bound_manifest"] = manifest.clone();
		if !kill_owner(&mut owner).await {
			finish(&mut evidence, "owner kill unconfirmed: owner-loss coverage unavailable");
			write_owner_evidence(&dir, &evidence);
			for worker in &mut workers {
				worker.shutdown().await;
			}
			return;
		}
		// 3. On the producer worker's own clock: the conservative wait from the last completed lease
		//    application call for the bound query (every call closed; refreshed by any later call),
		//    and the first completed reaper pass after it.
		let producer = manifest["producer_worker"].as_str().expect("producer worker").to_string();
		let producer_log = dir.join(format!("worker-{producer}.jsonl"));
		let query = manifest["task"]["query"].as_str().expect("query").to_string();
		let waited = std::time::Instant::now();
		let observed = loop {
			let events = read_log(&producer_log);
			let calls = |name: &str| {
				events
					.iter()
					.filter(|e| e["event"] == name && e["query"].as_str() == Some(query.as_str()))
					.filter_map(|e| e["call"].as_u64())
					.collect::<HashSet<_>>()
			};
			let closed = calls("lease_apply_start") == calls("lease_apply_end");
			let last_apply_end = events
				.iter()
				.filter(|e| e["event"] == "lease_apply_end" && e["query"].as_str() == Some(query.as_str()))
				.filter_map(|e| e["at_us"].as_u64())
				.max();
			let pass_after = last_apply_end.filter(|_| closed).and_then(|end| {
				events
					.iter()
					.find(|e| e["event"] == "reaper_pass_end" && e["at_us"].as_u64().is_some_and(|at| at > end + term_us))
					.cloned()
			});
			if let (Some(end), Some(pass)) = (last_apply_end, pass_after) {
				break Some((end, pass));
			}
			if waited.elapsed() > harness_limit {
				break None;
			}
			tokio::time::sleep(std::time::Duration::from_millis(250)).await;
		};
		let Some((end, pass)) = observed else {
			harness.emit("owner_wait_unavailable", "harness", serde_json::json!({ "producer_worker": producer }));
			evidence["worker_clock"] = serde_json::json!({ "producer_worker": producer });
			finish(&mut evidence, "no completed reaper pass after the conservative wait within the harness limit");
			write_owner_evidence(&dir, &evidence);
			for worker in &mut workers {
				worker.shutdown().await;
			}
			return;
		};
		harness.emit(
			"owner_wait_satisfied",
			"harness",
			serde_json::json!({
				"producer_worker": producer,
				"last_lease_apply_end_us": end,
				"conservative_wait_until_us": end + term_us,
				"pass": pass["pass"].clone(),
				"pass_end_us": pass["at_us"].clone(),
			}),
		);
		evidence["worker_clock"] = serde_json::json!({
			"producer_worker": producer,
			"last_lease_apply_end_us": end,
			"conservative_wait_until_us": end + term_us,
			"first_completed_pass_after_wait": pass,
		});
		// 4. Inspection: PullPartition ONLY, for the exact received address, on an observed stream:
		//    the request and the typed reply are raw events in the harness log.
		let entry = &manifest["partitions"][0];
		let address = toron::distributed::task::PartitionAddress {
			query_id: QueryId::from_uuid(uuid::Uuid::parse_str(&query).expect("query uuid")),
			exchange_id: toron::distributed::identity::ExchangeId::new(
				u32::try_from(manifest["exchange"].as_u64().expect("exchange")).expect("exchange"),
			),
			producer_stage_id: toron::distributed::identity::StageId::new(u32::try_from(manifest["task"]["stage"].as_u64().expect("stage")).expect("stage")),
			producer_task_id: toron::distributed::identity::TaskId::new(u32::try_from(manifest["task"]["task"].as_u64().expect("task")).expect("task")),
			attempt_id: AttemptId::new(u32::try_from(manifest["task"]["attempt"].as_u64().expect("attempt")).expect("attempt")),
			partition_id: toron::distributed::identity::PartitionId::new(
				u32::try_from(entry["partition"].as_u64().expect("partition")).expect("partition"),
			),
		};
		let endpoint = manifest["producer_endpoint"].as_str().expect("endpoint").to_string();
		let inspector = wait_events::ObservedTransport {
			inner: process_transport(),
			log: harness.clone(),
			role: "inspection",
		};
		harness.emit("inspection_start", "harness", serde_json::json!({ "producer_worker": producer, "endpoint": endpoint }));
		let inspection = match inspector.connect(&endpoint).await {
			Ok(stream) => match toron::server::ToronCoordinator::new(Arc::new(MemoryManager::new(4, 1024 * 1024)))
				.pull_partition_from_worker(stream, address, address.attempt_id)
				.await
			{
				Ok(payload) => serde_json::json!({ "result": "partition data", "bytes": payload.payload.0.len() }),
				Err(error) => serde_json::json!({ "result": "error", "error": format!("{error:?}") }),
			},
			Err(error) => serde_json::json!({ "result": "connect failed", "error": format!("{error:?}") }),
		};
		harness.emit("inspection_returned", "harness", inspection.clone());
		evidence["inspection"] = inspection;
		// The recorded window for absence claims ends here; every worker stays up through the
		// coverage tail, so each managed worker records a reaper pass after it.
		harness.emit("absence_window_end", "harness", serde_json::json!({}));
		tokio::time::sleep(ADR628_COVERAGE_TAIL).await;
		finish(&mut evidence, "inspected");
		write_owner_evidence(&dir, &evidence);
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

fn write_owner_evidence(dir: &std::path::Path, evidence: &serde_json::Value) {
	use std::io::Write as _;
	std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(dir.join("owner.json"))
		.expect("fresh owner evidence")
		.write_all(
			serde_json::to_string_pretty(evidence)
				.expect("json")
				.as_bytes(),
		)
		.expect("write owner evidence");
}

// ---- ADR 628 B1.B: extended-budget renewal timing survey (test-only; no production change) ----
//
// Nominal diagnostic boundary. The survey decides whether a synthetic id may still be renewed with
// `send + term` on the coordinator's steady clock: ρ = 0, no suspend, no restart. It is a
// diagnostic schedule rule only. It is NOT B1.A's conservative reliance boundary and certifies nothing.

/// The largest lease reply the survey reads; a longer frame is a protocol failure.
const SURVEY_MAX_REPLY_BYTES: usize = 64 * 1024;

/// The survey's frozen per-phase budgets and whole-exchange bound.
#[derive(Clone, Copy, Debug)]
struct SurveyBudgets {
	connect: std::time::Duration,
	write: std::time::Duration,
	ack: std::time::Duration,
	close: std::time::Duration,
	whole: std::time::Duration,
}

/// The survey's frozen parameters. `term` is the declared lease term (production: the worker's
/// configured term); tests pass a short one.
#[derive(Clone, Copy, Debug)]
struct SurveyParams {
	cadence: std::time::Duration,
	duration: std::time::Duration,
	term: std::time::Duration,
	budgets: SurveyBudgets,
	/// Renewal ATTEMPTS per id (any outcome); successful refreshes are counted separately.
	renewal_attempts_per_id: u64,
	/// An id is retired once this long has passed since its issuance send marker.
	id_max_age: std::time::Duration,
}

impl SurveyParams {
	fn from_spec(raw: &serde_json::Value, term: std::time::Duration) -> Self {
		let ms = |v: &serde_json::Value, name: &str| {
			std::time::Duration::from_millis(v[name].as_u64().unwrap_or_else(|| panic!("{name}")))
		};
		let b = &raw["budgets_ms"];
		Self {
			cadence: ms(raw, "cadence_ms"),
			duration: std::time::Duration::from_secs(
				raw["duration_secs"].as_u64().expect("duration_secs"),
			),
			term,
			budgets: SurveyBudgets {
				connect: ms(b, "connect"),
				write: ms(b, "write"),
				ack: ms(b, "ack"),
				close: ms(b, "close"),
				whole: ms(b, "whole"),
			},
			renewal_attempts_per_id: raw["renewal_attempts_per_id"]
				.as_u64()
				.expect("renewal_attempts_per_id"),
			id_max_age: ms(raw, "id_max_age_ms"),
		}
	}

	/// The frozen consistency rules: phases fit the whole bound; a whole exchange plus the cadence fit
	/// the term; an id's age limit fits the term.
	fn check(&self) {
		let b = self.budgets;
		assert!(
			b.connect + b.write + b.ack + b.close <= b.whole,
			"phase budgets exceed the whole bound"
		);
		assert!(
			b.whole + self.cadence <= self.term,
			"whole bound plus cadence must fit the term"
		);
		assert!(
			self.id_max_age + b.whole <= self.term,
			"the id age limit plus a whole exchange must fit the term"
		);
		assert!(self.cadence > std::time::Duration::ZERO && self.duration >= self.cadence);
	}

	fn evidence(&self) -> serde_json::Value {
		let ms = |d: std::time::Duration| d.as_millis() as u64;
		serde_json::json!({
			"cadence_ms": ms(self.cadence),
			"duration_ms": ms(self.duration),
			"term_ms": ms(self.term),
			"renewal_attempts_per_id": self.renewal_attempts_per_id,
			"id_max_age_ms": ms(self.id_max_age),
			"budgets_ms": {
				"connect": ms(self.budgets.connect), "write": ms(self.budgets.write), "ack": ms(self.budgets.ack),
				"close": ms(self.budgets.close), "whole": ms(self.budgets.whole),
			},
			"max_reply_bytes": SURVEY_MAX_REPLY_BYTES,
			"boundary": "nominal diagnostic: send + term on the coordinator's steady clock (rho = 0, no suspend, no restart); not B1.A's reliance boundary",
		})
	}
}

/// The phases of one exchange, each with an explicit start and exactly one terminal mark; the whole
/// bound's cut is recorded on the phase that was active.
#[derive(Default)]
struct PhaseTrace {
	phases: Vec<serde_json::Value>,
}

impl PhaseTrace {
	fn begin(&mut self, name: &str, at: u64) {
		self.phases
			.push(serde_json::json!({ "phase": name, "start_us": at }));
	}

	fn mark(&mut self, key: &str, at: u64, error: Option<String>) {
		let last = self.phases.last_mut().expect("an active phase");
		last[key] = at.into();
		if let Some(error) = error {
			last["error"] = error.into();
		}
	}

	fn active(&self) -> bool {
		self.phases.last().is_some_and(|p| {
			!["end_us", "budget_expired_us", "error_us", "whole_cut_us"]
				.iter()
				.any(|k| p.get(*k).is_some())
		})
	}
}

/// Runs one budgeted phase: its start is recorded; it ends, expires its budget (censored) or fails.
async fn survey_phase<T, F>(
	trace: &std::sync::Mutex<PhaseTrace>,
	log: &wait_events::EventLog,
	name: &str,
	budget: Option<std::time::Duration>,
	future: F,
) -> Result<T, (&'static str, String)>
where
	F: std::future::Future<Output = Result<T, (&'static str, String)>>,
{
	trace.lock().expect("trace").begin(name, log.now_us());
	let outcome = match budget {
		Some(budget) => match tokio::time::timeout(budget, future).await {
			Ok(result) => result,
			Err(_) => {
				trace
					.lock()
					.expect("trace")
					.mark("budget_expired_us", log.now_us(), None);
				return Err(("budget_expired", format!("{name} budget expired")));
			}
		},
		None => future.await,
	};
	match outcome {
		Ok(value) => {
			trace
				.lock()
				.expect("trace")
				.mark("end_us", log.now_us(), None);
			Ok(value)
		}
		Err((kind, error)) => {
			trace
				.lock()
				.expect("trace")
				.mark("error_us", log.now_us(), Some(error.clone()));
			Err((kind, error))
		}
	}
}

/// One lease exchange (issuance or renewal): connect, write (`send_us` = the exact send marker, the
/// write phase's start), ACK read (length-checked), validate (decode, protocol version and payload
/// length via `validate_runtime`, kind, identity, query, outcome, term) and close, each budgeted
/// except validate (CPU only), all under the whole bound. Every outcome is data: a decoded outcome is
/// kept even when a later phase fails.
async fn survey_exchange(
	transport: &Arc<dyn ToronTransport>,
	worker: &(WorkerId, WorkerEndpoint),
	query_id: QueryId,
	term_ms: u64,
	budgets: SurveyBudgets,
	log: &wait_events::EventLog,
) -> serde_json::Value {
	use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
	use toron::distributed::identity::FrameByteLen;
	use toron::wire::{
		EnvelopeIdentity, MessageKind, ProtocolVersion, QueryLeaseAcceptedPayload,
		QueryLeaseOutcome, QueryLeasePayload, WireEnvelope, decode_payload, encode_payload,
	};
	let trace = Arc::new(std::sync::Mutex::new(PhaseTrace::default()));
	let decoded: Arc<std::sync::Mutex<serde_json::Value>> = Arc::default();
	let start_us = log.now_us();
	let exchange = {
		let (trace, decoded) = (trace.clone(), decoded.clone());
		async move {
			let mut stream = survey_phase(&trace, log, "connect", Some(budgets.connect), async {
				transport
					.connect_peer(&worker.0, &worker.1)
					.await
					.map_err(|e| ("transport_error", format!("{e:?}")))
			})
			.await?;
			let frame = WireEnvelope::try_new(
				MessageKind::QueryLease,
				EnvelopeIdentity::query(query_id),
				None,
				None,
				encode_payload(&QueryLeasePayload {
					query_id,
					lease_term_millis: term_ms,
				}),
			)
			.expect("lease envelope")
			.encode();
			survey_phase(&trace, log, "write", Some(budgets.write), async {
				let io = |e: std::io::Error| ("transport_error", e.to_string());
				stream
					.write_all(&(frame.len() as u32).to_le_bytes())
					.await
					.map_err(io)?;
				stream.write_all(&frame).await.map_err(io)?;
				stream.flush().await.map_err(io)
			})
			.await?;
			let reply = survey_phase(&trace, log, "ack", Some(budgets.ack), async {
				let io = |e: std::io::Error| ("transport_error", e.to_string());
				let mut len = [0u8; 4];
				stream.read_exact(&mut len).await.map_err(io)?;
				let len = u32::from_le_bytes(len) as usize;
				if len > SURVEY_MAX_REPLY_BYTES {
					return Err((
						"protocol_error",
						format!(
							"reply frame of {len} bytes over the {SURVEY_MAX_REPLY_BYTES} limit"
						),
					));
				}
				let mut body = vec![0u8; len];
				stream.read_exact(&mut body).await.map_err(io)?;
				Ok(body)
			})
			.await?;
			survey_phase(&trace, log, "validate", None, async {
				let protocol = |e: String| ("protocol_error", e);
				let envelope =
					WireEnvelope::decode(&reply).map_err(|e| protocol(format!("{e:?}")))?;
				envelope
					.validate_runtime(
						ProtocolVersion::CURRENT,
						FrameByteLen(SURVEY_MAX_REPLY_BYTES as u32),
					)
					.map_err(|e| protocol(format!("{e:?}")))?;
				if envelope.kind != MessageKind::QueryLeaseAccepted
					|| envelope.identity != EnvelopeIdentity::query(query_id)
				{
					return Err(protocol(format!(
						"reply {:?} for {:?}",
						envelope.kind, envelope.identity
					)));
				}
				let payload: QueryLeaseAcceptedPayload =
					decode_payload(&envelope.payload).map_err(|e| protocol(format!("{e:?}")))?;
				if payload.query_id != query_id {
					return Err(protocol("reply for another query".to_string()));
				}
				let mut out = decoded.lock().expect("decoded");
				match payload.outcome {
					QueryLeaseOutcome::Applied { term_ms: applied } if applied == term_ms => {
						*out = serde_json::json!({ "outcome": "Applied", "term_ms": applied });
						Ok(())
					}
					QueryLeaseOutcome::Applied { term_ms: applied } => {
						Err(protocol(format!("Applied with term {applied}")))
					}
					QueryLeaseOutcome::Ignored { reason } => {
						*out = serde_json::json!({ "outcome": "Ignored", "reason": format!("{reason:?}") });
						Ok(())
					}
				}
			})
			.await?;
			survey_phase(&trace, log, "close", Some(budgets.close), async {
				stream
					.shutdown()
					.await
					.map_err(|e| ("transport_error", e.to_string()))
			})
			.await?;
			Ok::<(), (&'static str, String)>(())
		}
	};
	let result = tokio::time::timeout(budgets.whole, exchange).await;
	let end_us = log.now_us();
	let mut trace = std::mem::take(&mut *trace.lock().expect("trace"));
	let mut record = serde_json::json!({ "start_us": start_us });
	match result {
		Ok(Ok(())) => {}
		Ok(Err((kind, error))) => {
			record["failure"] = kind.into();
			record["error"] = error.into();
		}
		Err(_) => {
			record["failure"] = "whole_budget_expired".into();
			if trace.active() {
				trace.mark("whole_cut_us", end_us, None);
			}
		}
	}
	if let Some(write) = trace.phases.iter().find(|p| p["phase"] == "write") {
		record["send_us"] = write["start_us"].clone();
	}
	for (key, value) in decoded
		.lock()
		.expect("decoded")
		.as_object()
		.cloned()
		.unwrap_or_default()
	{
		record[key] = value;
	}
	record["phases"] = serde_json::Value::Array(trace.phases);
	record["end_us"] = end_us.into();
	record
}

/// A refresh is accepted only when the whole exchange completed `Applied`, before the OLD nominal
/// boundary (renewals), with its NEW send-derived boundary still in the future at completion.
fn survey_refresh_decision(
	end_us: u64,
	send_us: u64,
	old_boundary_us: Option<u64>,
	term_us: u64,
) -> Result<(), &'static str> {
	if old_boundary_us.is_some_and(|old| end_us >= old) {
		return Err("rejected: completed at or after the old nominal boundary");
	}
	if end_us >= send_us + term_us {
		return Err("rejected: the new nominal boundary was already unusable at completion");
	}
	Ok(())
}

/// One synthetic id in the survey's ledger.
#[derive(Clone, Copy)]
struct SurveyId {
	query: QueryId,
	generation: u64,
	issued_send_us: u64,
	last_applied_send_us: u64,
	attempts: u64,
	refreshes: u64,
}

/// Retires `query_id` once (any outcome is recorded, never relabelled reclamation).
async fn survey_retire(
	transport: &Arc<dyn ToronTransport>,
	worker: &(WorkerId, WorkerEndpoint),
	query_id: QueryId,
	budget: std::time::Duration,
	log: &wait_events::EventLog,
	reason: &str,
	at_end: bool,
) {
	let start_us = log.now_us();
	let retired = tokio::time::timeout(budget, async {
		let stream = transport.connect_peer(&worker.0, &worker.1).await?;
		toron::server::ToronCoordinator::new(Arc::new(MemoryManager::new(4, 1024 * 1024)))
			.retire_query_on_worker(
				stream,
				query_id,
				toron::distributed::namespace_lifecycle::QueryTerminalCause::Success,
			)
			.await
	})
	.await;
	log.emit("survey_retired", "survey", serde_json::json!({
		"worker": worker.0.get().as_u128().to_string(),
		"query": query_id.get().to_string(),
		"reason": reason,
		"at_end": at_end,
		"start_us": start_us,
		"end_us": log.now_us(),
		"result": match retired { Ok(Ok(())) => "retire acknowledged".to_string(), Ok(Err(e)) => format!("error: {e:?}"), Err(_) => "budget expired".to_string() },
	}));
}

/// The per-worker survey state machine: one action per tick (issue, renew or retire), fixed-schedule
/// ticks with `MissedTickBehavior::Skip` (a delayed tick fires once, later ones keep their schedule;
/// missed ticks are recorded, never caught up). Every attempted id enters the ledger and is retired
/// exactly once with a declared reason: age, attempts, invalid (nominal validity), ignored, unknown
/// application (a failed issuance the worker may have applied), refresh rejected, or survey end.
async fn survey_worker_loop(
	transport: Arc<dyn ToronTransport>,
	worker: (WorkerId, WorkerEndpoint),
	params: SurveyParams,
	log: Arc<wait_events::EventLog>,
	started: tokio::time::Instant,
	started_us: u64,
) {
	let worker_text = worker.0.get().as_u128().to_string();
	let (term_us, whole_us) = (
		params.term.as_micros() as u64,
		params.budgets.whole.as_micros() as u64,
	);
	let term_ms = params.term.as_millis() as u64;
	let mut ticks = tokio::time::interval_at(started + params.cadence, params.cadence);
	ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	let ends_at = started + params.duration;
	let mut live: Option<SurveyId> = None;
	let mut to_retire: Option<(QueryId, &'static str)> = None;
	let mut generation = 0u64;
	let mut last_tick = 0u64;
	// The last scheduled tick inside the observation window: k * cadence < duration.
	let final_tick = ((params.duration.as_nanos() - 1) / params.cadence.as_nanos().max(1)) as u64;
	loop {
		let scheduled = ticks.tick().await;
		if scheduled >= ends_at {
			// Ticks skipped just before the end are recorded too, so every scheduled tick is
			// accounted for.
			if last_tick < final_tick {
				log.emit("survey_tick_skipped", "survey", serde_json::json!({ "worker": worker_text, "from_tick": last_tick + 1, "to_tick": final_tick, "at_end": true }));
			}
			break;
		}
		let actual_us = log.now_us();
		let tick = ((scheduled - started).as_nanos() / params.cadence.as_nanos().max(1)) as u64;
		let scheduled_us = started_us + (scheduled - started).as_micros() as u64;
		if tick > last_tick + 1 {
			log.emit(
				"survey_tick_skipped",
				"survey",
				serde_json::json!({ "worker": worker_text, "from_tick": last_tick + 1, "to_tick": tick - 1 }),
			);
		}
		last_tick = tick;
		// One decision per tick, made now.
		let decided_us = log.now_us();
		let action = if let Some((query, reason)) = to_retire.take() {
			("retire", Some(query), reason)
		} else if let Some(id) = live {
			if decided_us >= id.issued_send_us + params.id_max_age.as_micros() as u64 {
				("retire", Some(id.query), "age")
			} else if id.attempts >= params.renewal_attempts_per_id {
				("retire", Some(id.query), "attempts")
			} else if decided_us + whole_us >= id.last_applied_send_us + term_us {
				("retire", Some(id.query), "invalid")
			} else {
				("renewal", Some(id.query), "")
			}
		} else {
			("issuance", None, "")
		};
		log.emit(
			"survey_tick",
			"survey",
			serde_json::json!({
				"worker": worker_text, "tick": tick, "scheduled_us": scheduled_us, "actual_us": actual_us,
				"decided_us": decided_us, "action": action.0, "reason": action.2,
			}),
		);
		match action {
			("retire", Some(query), reason) => {
				if live.is_some_and(|id| id.query == query) {
					live = None;
				}
				survey_retire(
					&transport,
					&worker,
					query,
					params.budgets.whole,
					&log,
					reason,
					false,
				)
				.await;
			}
			(kind, query, _) => {
				let query = query.unwrap_or_else(|| {
					generation += 1;
					QueryId::new()
				});
				let mut record =
					survey_exchange(&transport, &worker, query, term_ms, params.budgets, &log)
						.await;
				record["worker"] = worker_text.clone().into();
				record["endpoint"] = worker.1.as_str().into();
				record["query"] = query.get().to_string().into();
				record["generation"] = generation.into();
				record["kind"] = kind.into();
				record["tick"] = tick.into();
				record["decided_us"] = decided_us.into();
				let applied = record["outcome"] == "Applied" && record.get("failure").is_none();
				let ignored = record["outcome"] == "Ignored";
				let send_us = record["send_us"].as_u64();
				let end_us = record["end_us"].as_u64().expect("end_us");
				if kind == "renewal" {
					let id = live.as_mut().expect("a renewal of the live id");
					id.attempts += 1;
				}
				let old_boundary = live
					.filter(|_| kind == "renewal")
					.map(|id| id.last_applied_send_us + term_us);
				let refresh = match (applied, send_us) {
					(true, Some(send_us)) => Some(
						survey_refresh_decision(end_us, send_us, old_boundary, term_us)
							.map(|()| send_us),
					),
					_ => None,
				};
				record["refresh"] = match &refresh {
					Some(Ok(_)) => "accepted".into(),
					Some(Err(why)) => (*why).into(),
					None => serde_json::Value::Null,
				};
				log.emit("survey_exchange", "survey", record);
				match (kind, refresh) {
					("issuance", Some(Ok(send_us))) => {
						live = Some(SurveyId {
							query,
							generation,
							issued_send_us: send_us,
							last_applied_send_us: send_us,
							attempts: 0,
							refreshes: 0,
						});
					}
					("issuance", _) => {
						to_retire = Some((
							query,
							if ignored {
								"ignored"
							} else if refresh.is_some() {
								"refresh rejected"
							} else {
								"unknown application"
							},
						));
					}
					(_, Some(Ok(send_us))) => {
						let id = live.as_mut().expect("live id");
						id.last_applied_send_us = send_us;
						id.refreshes += 1;
					}
					(_, Some(Err(_))) => {
						live = None;
						to_retire = Some((query, "refresh rejected"));
					}
					_ if ignored => {
						live = None;
						to_retire = Some((query, "ignored"));
					}
					// Any other renewal failure keeps the id; its validity is checked at the next tick.
					_ => {}
				}
				if let Some(id) = live {
					log.emit(
						"survey_ledger",
						"survey",
						serde_json::json!({
							"worker": worker_text, "query": id.query.get().to_string(), "generation": id.generation,
							"attempts": id.attempts, "refreshes": id.refreshes,
						}),
					);
				}
			}
		}
	}
	// Survey end: every id still open is retired once.
	if let Some((query, reason)) = to_retire.take() {
		survey_retire(
			&transport,
			&worker,
			query,
			params.budgets.whole,
			&log,
			reason,
			true,
		)
		.await;
	}
	if let Some(id) = live.take() {
		survey_retire(
			&transport,
			&worker,
			id.query,
			params.budgets.whole,
			&log,
			"survey end",
			true,
		)
		.await;
	}
}

/// ADR 628 B1.B: the extended-budget renewal timing survey (spec via `TORON_MULTINODE_SPEC`; see
/// `run_renewal_survey`).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "ADR 628 B1.B survey: needs TORON_MULTINODE_SPEC and running host-mode workers"]
async fn renewal_timing_survey() {
	run_renewal_survey(load_probe_spec()).await;
}

/// ADR 628 B1.B development smoke: the same survey against three managed loopback fixture workers
/// (no hosts, no campaign). `TORON_ADR628_DIR` receives the spec, the worker logs and the survey log;
/// `TORON_ADR628_SURVEY` (optional) is a JSON object overriding spec fields (condition, duration...).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "ADR 628 B1.B development smoke: run explicitly with TORON_ADR628_DIR"]
async fn renewal_timing_survey_on_loopback() {
	let dir =
		std::path::PathBuf::from(std::env::var("TORON_ADR628_DIR").expect("TORON_ADR628_DIR"));
	std::fs::create_dir(&dir).expect("a fresh survey directory");
	with_quic(async {
		let mut workers = Vec::new();
		for id in 62_831..=62_833u128 {
			workers.push(
				MANAGED_LIFECYCLE
					.scope(
						(),
						FixtureWorkerProcess::spawn_recorded(
							WorkerId::from_u128(id),
							Some(&dir.join(format!("dials-{id}.txt"))),
							Some(&dir.join(format!("worker-{id}.jsonl"))),
						),
					)
					.await,
			);
		}
		let mut spec = serde_json::json!({
			"workers": workers.iter().map(|w| serde_json::json!({ "worker_u128": w.worker_id.get().as_u128().to_string(), "endpoint": w.endpoint.as_str() })).collect::<Vec<_>>(),
			"loopback": true,
			"cluster_domain": "loopback",
			"deadline_secs": 120,
			"wait_events": false,
			"attempt_dir": dir.display().to_string(),
			"condition": "idle",
			"duration_secs": 30,
			"cadence_ms": 1000,
			"renewal_attempts_per_id": 3,
			"id_max_age_ms": 8000,
			"budgets_ms": { "connect": 2000, "write": 2000, "ack": 2000, "close": 2000, "whole": 8000 },
		});
		if let Ok(overrides) = std::env::var("TORON_ADR628_SURVEY") {
			let overrides: serde_json::Value = serde_json::from_str(&overrides).expect("survey overrides");
			for (key, value) in overrides.as_object().expect("an object") {
				spec[key] = value.clone();
			}
		}
		let path = dir.join("survey.spec.json");
		std::fs::write(&path, serde_json::to_string_pretty(&spec).expect("spec")).expect("write spec");
		run_renewal_survey(load_probe_spec_from(path.to_str().expect("utf-8 path"))).await;
		// The coverage tail: every managed worker records a reaper pass after the survey ends.
		tokio::time::sleep(ADR628_COVERAGE_TAIL).await;
		for worker in &mut workers {
			worker.shutdown().await;
		}
	})
	.await;
}

/// The survey. Spec: the ADR 627 host fields plus `condition` ("idle" | "load"), `duration_secs`,
/// `cadence_ms`, `renewal_attempts_per_id`, `id_max_age_ms` and `budgets_ms` {connect, write, ack,
/// close, whole}. The term is the worker's configured lease term. One state-machine task per worker
/// (concurrency = the worker count; one exchange at a time per worker). Under "load",
/// `long_join_last` runs back to back on the coordinator path; its events go to the SAME log as the
/// survey (one clock), bracketed by `load_window_start` / `load_window_end`.
async fn run_renewal_survey(spec: ProbeSpec) {
	let raw = spec.raw.clone();
	let condition = spec_str(&raw, "condition");
	assert!(
		matches!(condition.as_str(), "idle" | "load"),
		"condition {condition}"
	);
	let term =
		toron::distributed::namespace_lifecycle::ShuffleLifecycleConfig::from_worker_loss_window(
			toron::distributed::partition_store::DEFAULT_WORKER_LOST_AFTER,
		)
		.expect("config")
		.lease_term();
	let params = SurveyParams::from_spec(&raw, term);
	params.check();
	let log = wait_events::EventLog::create(&spec.attempt_dir.join("survey_events.jsonl"))
		.expect("fresh survey events");
	let transport = spec_transport(&raw);
	let started = tokio::time::Instant::now();
	let started_us = log.now_us();
	log.emit_at(
		started_us,
		"survey_start",
		"survey",
		serde_json::json!({
			"condition": condition,
			"started_us": started_us,
			"params": params.evidence(),
			"workers": spec.workers.iter().map(|(id, endpoint)| serde_json::json!({ "worker": id.get().as_u128().to_string(), "endpoint": endpoint.as_str() })).collect::<Vec<_>>(),
		}),
	);
	let mut tasks = Vec::new();
	for worker in spec.workers.clone() {
		tasks.push(tokio::spawn(survey_worker_loop(
			transport.clone(),
			worker,
			params,
			log.clone(),
			started,
			started_us,
		)));
	}
	let load = (condition == "load").then(|| {
		let log = log.clone();
		let workers = spec.workers.clone();
		let observed: Arc<dyn ToronTransport> = Arc::new(wait_events::ObservedTransport {
			inner: spec_transport(&raw),
			log: log.clone(),
			role: "dispatch",
		});
		let ends_at = started + params.duration;
		tokio::spawn(async move {
			let mut index = 0u64;
			while tokio::time::Instant::now() < ends_at {
				log.emit(
					"load_window_start",
					"survey",
					serde_json::json!({ "index": index }),
				);
				let evidence = adr627_run(
					Adr627Path::Coordinator,
					"long_join_last",
					&workers,
					observed.clone(),
					log.clone(),
					None,
					None,
					std::time::Duration::from_secs(120),
				)
				.await;
				log.emit(
					"load_window_end",
					"survey",
					serde_json::json!({
						"index": index,
						"outcome": evidence["result"]["outcome"].clone(),
						"error_task_failure": evidence["result"]["error_task_failure"].clone(),
					}),
				);
				index += 1;
			}
		})
	});
	for task in tasks {
		task.await.expect("survey worker");
	}
	if let Some(load) = load {
		load.await.expect("load loop");
	}
	log.emit(
		"survey_end",
		"survey",
		serde_json::json!({ "instrument_failed": log.failed() }),
	);
	assert!(!log.failed(), "the survey log must be complete");
}

// ---- ADR 628 B1.B instrument controls (scripted workers; no hosts) ----

/// How a scripted lease worker misbehaves on one exchange.
#[derive(Clone, Copy, Default)]
struct ScriptedLease {
	connect_delay_ms: u64,
	/// The worker never reads, and the client stream's buffer is tiny: the write blocks.
	block_write: bool,
	ack_delay_ms: u64,
	/// Blocks the worker THREAD (not the task) before replying, starving a current-thread runtime.
	starve_ms: u64,
	ignore: bool,
	wrong_version: bool,
	oversized: bool,
	wrong_term: bool,
	stall_close: bool,
}

struct ScriptedLeaseTransport {
	script: Arc<std::sync::Mutex<std::collections::VecDeque<ScriptedLease>>>,
}

#[async_trait::async_trait]
impl ToronTransport for ScriptedLeaseTransport {
	async fn connect(
		&self,
		_address: &str,
	) -> Result<Box<dyn toron::server::transport::ToronStream>, toron::error::QueryError> {
		use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
		use toron::wire::{
			EnvelopeIdentity, MessageKind, ProtocolVersion, QueryLeaseAcceptedPayload,
			QueryLeaseOutcome, QueryLeasePayload, WireEnvelope, decode_payload, encode_payload,
		};
		let step = self
			.script
			.lock()
			.expect("script")
			.pop_front()
			.unwrap_or_default();
		tokio::time::sleep(std::time::Duration::from_millis(step.connect_delay_ms)).await;
		let (client, mut server) = tokio::io::duplex(if step.block_write { 4 } else { 1 << 16 });
		tokio::spawn(async move {
			if step.block_write {
				std::future::pending::<()>().await;
			}
			let mut len = [0u8; 4];
			if server.read_exact(&mut len).await.is_err() {
				return;
			}
			let mut body = vec![0u8; u32::from_le_bytes(len) as usize];
			if server.read_exact(&mut body).await.is_err() {
				return;
			}
			if RetireRequest::is_retire(&body) {
				return RetireRequest::answer(server, &body).await;
			}
			let request: QueryLeasePayload =
				decode_payload(&WireEnvelope::decode(&body).expect("lease").payload)
					.expect("payload");
			tokio::time::sleep(std::time::Duration::from_millis(step.ack_delay_ms)).await;
			if step.starve_ms > 0 {
				std::thread::sleep(std::time::Duration::from_millis(step.starve_ms));
			}
			if step.oversized {
				let _ = server.write_all(&(1u32 << 30).to_le_bytes()).await;
				return;
			}
			let outcome = if step.ignore {
				QueryLeaseOutcome::Ignored {
					reason: toron::distributed::namespace_lifecycle::LeaseIgnored::Expired,
				}
			} else {
				QueryLeaseOutcome::Applied {
					term_ms: request.lease_term_millis + u64::from(step.wrong_term),
				}
			};
			let mut envelope = WireEnvelope::try_new(
				MessageKind::QueryLeaseAccepted,
				EnvelopeIdentity::query(request.query_id),
				None,
				None,
				encode_payload(&QueryLeaseAcceptedPayload {
					query_id: request.query_id,
					outcome,
				}),
			)
			.expect("reply");
			if step.wrong_version {
				envelope.protocol_version =
					ProtocolVersion::new(ProtocolVersion::CURRENT.get() + 1);
			}
			let frame = envelope.encode();
			let _ = server.write_all(&(frame.len() as u32).to_le_bytes()).await;
			let _ = server.write_all(&frame).await;
			let _ = server.flush().await;
			if step.stall_close {
				std::future::pending::<()>().await;
			}
			let mut rest = Vec::new();
			let _ = server.read_to_end(&mut rest).await;
		});
		if step.stall_close {
			return Ok(Box::new(StallingClose(client)));
		}
		Ok(Box::new(client))
	}
}

/// Answers a scripted `RetireQuery` with `QueryRetired`.
struct RetireRequest;

impl RetireRequest {
	fn is_retire(body: &[u8]) -> bool {
		toron::wire::WireEnvelope::decode(body)
			.is_ok_and(|e| e.kind == toron::wire::MessageKind::RetireQuery)
	}

	async fn answer(mut server: tokio::io::DuplexStream, body: &[u8]) {
		use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
		let envelope = toron::wire::WireEnvelope::decode(body).expect("retire");
		let reply = toron::wire::WireEnvelope::try_new(
			toron::wire::MessageKind::QueryRetired,
			envelope.identity,
			None,
			None,
			envelope.payload.clone(),
		)
		.expect("retired")
		.encode();
		let _ = server.write_all(&(reply.len() as u32).to_le_bytes()).await;
		let _ = server.write_all(&reply).await;
		let mut rest = Vec::new();
		let _ = server.read_to_end(&mut rest).await;
	}
}

/// A client stream whose shutdown never completes.
struct StallingClose(tokio::io::DuplexStream);

impl tokio::io::AsyncRead for StallingClose {
	fn poll_read(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
	}
}

impl tokio::io::AsyncWrite for StallingClose {
	fn poll_write(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<std::io::Result<usize>> {
		std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
	}
	fn poll_flush(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::Pin::new(&mut self.0).poll_flush(cx)
	}
	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::task::Poll::Pending
	}
}

fn scripted(steps: Vec<ScriptedLease>) -> Arc<dyn ToronTransport> {
	Arc::new(ScriptedLeaseTransport {
		script: Arc::new(std::sync::Mutex::new(steps.into())),
	})
}

fn survey_test_log() -> (tempfile_dir::Dir, Arc<wait_events::EventLog>) {
	let dir = tempfile_dir::Dir::new();
	let log = wait_events::EventLog::create(&dir.path().join("survey_events.jsonl")).expect("log");
	(dir, log)
}

/// Minimal temp-dir helper for the survey controls.
mod tempfile_dir {
	pub struct Dir(std::path::PathBuf);

	impl Dir {
		pub fn new() -> Self {
			let path = std::env::temp_dir().join(format!("adr628-survey-{}", uuid::Uuid::now_v7()));
			std::fs::create_dir(&path).expect("temp dir");
			Self(path)
		}

		pub fn path(&self) -> &std::path::Path {
			&self.0
		}
	}

	impl Drop for Dir {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.0);
		}
	}
}

fn test_budgets(ms: u64) -> SurveyBudgets {
	let d = std::time::Duration::from_millis(ms);
	SurveyBudgets {
		connect: d,
		write: d,
		ack: d,
		close: d,
		whole: d * 3,
	}
}

fn test_worker() -> (WorkerId, WorkerEndpoint) {
	(
		WorkerId::from_u128(62_899),
		WorkerEndpoint::new("scripted://survey"),
	)
}

/// ADR 628 B1.B control: every failure class is classified and timed. A wrong version or wrong term is
/// a protocol error in `validate`; an oversized frame a protocol error in `ack` (not transport); the
/// whole bound cutting the connect, write, ack or close phase marks that phase `whole_cut_us`; an
/// Ignored reply is an outcome, not a failure.
#[tokio::test]
async fn survey_exchange_classifies_every_failure() {
	let (_dir, log) = survey_test_log();
	let query = QueryId::new();
	let run = |step: ScriptedLease, budgets: SurveyBudgets| {
		let log = log.clone();
		async move {
			survey_exchange(
				&scripted(vec![step]),
				&test_worker(),
				query,
				35_001,
				budgets,
				&log,
			)
			.await
		}
	};
	let phase = |r: &serde_json::Value, name: &str| {
		r["phases"]
			.as_array()
			.unwrap()
			.iter()
			.find(|p| p["phase"] == name)
			.cloned()
	};
	let ok = run(ScriptedLease::default(), test_budgets(500)).await;
	assert_eq!(
		(ok["outcome"].as_str(), ok.get("failure")),
		(Some("Applied"), None),
		"{ok}"
	);
	assert_eq!(
		ok["phases"]
			.as_array()
			.unwrap()
			.iter()
			.map(|p| p["phase"].as_str().unwrap())
			.collect::<Vec<_>>(),
		["connect", "write", "ack", "validate", "close"]
	);
	assert_eq!(
		ok["send_us"],
		phase(&ok, "write").unwrap()["start_us"],
		"the send marker is the write's start"
	);
	let ignored = run(
		ScriptedLease {
			ignore: true,
			..Default::default()
		},
		test_budgets(500),
	)
	.await;
	assert_eq!(
		(ignored["outcome"].as_str(), ignored.get("failure")),
		(Some("Ignored"), None)
	);
	let version = run(
		ScriptedLease {
			wrong_version: true,
			..Default::default()
		},
		test_budgets(500),
	)
	.await;
	assert_eq!(version["failure"], "protocol_error");
	assert!(
		phase(&version, "validate")
			.unwrap()
			.get("error_us")
			.is_some(),
		"{version}"
	);
	let term = run(
		ScriptedLease {
			wrong_term: true,
			..Default::default()
		},
		test_budgets(500),
	)
	.await;
	assert_eq!(term["failure"], "protocol_error");
	let oversized = run(
		ScriptedLease {
			oversized: true,
			..Default::default()
		},
		test_budgets(500),
	)
	.await;
	assert_eq!(oversized["failure"], "protocol_error");
	assert!(
		phase(&oversized, "ack").unwrap().get("error_us").is_some(),
		"{oversized}"
	);
	// The whole bound (here smaller than one phase budget) cuts each phase in turn.
	let tight = SurveyBudgets {
		whole: std::time::Duration::from_millis(100),
		..test_budgets(1000)
	};
	for (step, name) in [
		(
			ScriptedLease {
				connect_delay_ms: 400,
				..Default::default()
			},
			"connect",
		),
		(
			ScriptedLease {
				block_write: true,
				..Default::default()
			},
			"write",
		),
		(
			ScriptedLease {
				ack_delay_ms: 400,
				..Default::default()
			},
			"ack",
		),
		(
			ScriptedLease {
				stall_close: true,
				..Default::default()
			},
			"close",
		),
	] {
		let cut = run(step, tight).await;
		assert_eq!(cut["failure"], "whole_budget_expired", "{name}: {cut}");
		let last = cut["phases"].as_array().unwrap().last().unwrap().clone();
		assert_eq!(
			(last["phase"].as_str(), last.get("whole_cut_us").is_some()),
			(Some(name), true),
			"{cut}"
		);
	}
	// A phase's own budget expiring is censored on that phase.
	let censored = run(
		ScriptedLease {
			ack_delay_ms: 300,
			..Default::default()
		},
		SurveyBudgets {
			ack: std::time::Duration::from_millis(50),
			..test_budgets(1000)
		},
	)
	.await;
	assert_eq!(censored["failure"], "budget_expired");
	assert!(
		phase(&censored, "ack")
			.unwrap()
			.get("budget_expired_us")
			.is_some()
	);
}

/// ADR 628 B1.B control: the refresh rule rejects completion at/after the old boundary and an
/// already-unusable new boundary.
#[test]
fn survey_refresh_decision_rejects_late_completions() {
	assert_eq!(
		survey_refresh_decision(900, 100, Some(1_000), 5_000),
		Ok(())
	);
	assert!(
		survey_refresh_decision(1_000, 100, Some(1_000), 5_000).is_err(),
		"at the old boundary"
	);
	assert!(
		survey_refresh_decision(6_000, 900, None, 5_000).is_err(),
		"new boundary unusable"
	);
	assert_eq!(survey_refresh_decision(4_000, 100, None, 5_000), Ok(()));
}

fn survey_test_params(
	term_ms: u64,
	cadence_ms: u64,
	whole_ms: u64,
	attempts: u64,
	age_ms: u64,
	duration_ms: u64,
) -> SurveyParams {
	let ms = std::time::Duration::from_millis;
	let phase = ms(whole_ms / 4);
	SurveyParams {
		cadence: ms(cadence_ms),
		duration: ms(duration_ms),
		term: ms(term_ms),
		budgets: SurveyBudgets {
			connect: phase,
			write: phase,
			ack: phase,
			close: phase,
			whole: ms(whole_ms),
		},
		renewal_attempts_per_id: attempts,
		id_max_age: ms(age_ms),
	}
}

fn survey_events(log_dir: &std::path::Path) -> Vec<serde_json::Value> {
	std::fs::read_to_string(log_dir.join("survey_events.jsonl"))
		.expect("log")
		.lines()
		.map(|l| serde_json::from_str(l).expect("json"))
		.collect()
}

/// ADR 628 B1.B control: a failed renewal attempt keeps the id and a later attempt refreshes it; a
/// late close (Applied decoded, close budget expired) does not refresh; an issuance whose ACK is lost
/// is retired as unknown application; every id is retired exactly once; attempts and refreshes are
/// counted separately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn survey_loop_accounts_for_retries_late_close_and_unknown_application() {
	let (dir, log) = survey_test_log();
	let params = survey_test_params(2_000, 100, 400, 10, 1_500, 1_000);
	params.check();
	let steps = vec![
		ScriptedLease::default(), // issuance Applied
		ScriptedLease {
			ack_delay_ms: 300,
			..Default::default()
		}, // renewal: ack budget (100) expires
		ScriptedLease::default(), // renewal Applied (refresh)
		ScriptedLease {
			stall_close: true,
			..Default::default()
		}, // renewal Applied, close budget expires
		ScriptedLease::default(), // renewal Applied (refresh)
	];
	let started = tokio::time::Instant::now();
	let started_us = log.now_us();
	survey_worker_loop(
		scripted(steps),
		test_worker(),
		params,
		log.clone(),
		started,
		started_us,
	)
	.await;
	let events = survey_events(dir.path());
	let exchanges = events
		.iter()
		.filter(|e| e["event"] == "survey_exchange")
		.collect::<Vec<_>>();
	assert_eq!(exchanges[0]["refresh"], "accepted");
	assert_eq!(exchanges[1]["failure"], "budget_expired");
	assert_eq!(exchanges[2]["refresh"], "accepted");
	assert_eq!(exchanges[3]["failure"], "budget_expired");
	assert!(
		exchanges[3]["refresh"].is_null(),
		"a late close never refreshes"
	);
	assert_eq!(exchanges[4]["refresh"], "accepted");
	// The ledger after the scripted fifth exchange: four renewal attempts, two accepted refreshes.
	let ledger = events
		.iter()
		.filter(|e| e["event"] == "survey_ledger")
		.nth(4)
		.expect("ledger");
	assert_eq!(
		(ledger["attempts"].as_u64(), ledger["refreshes"].as_u64()),
		(Some(4), Some(2))
	);
	// Every query that had an exchange is retired exactly once.
	let queries = exchanges
		.iter()
		.map(|e| e["query"].as_str().unwrap())
		.collect::<std::collections::BTreeSet<_>>();
	for q in queries {
		assert_eq!(
			events
				.iter()
				.filter(|e| e["event"] == "survey_retired" && e["query"] == q)
				.count(),
			1,
			"{q}"
		);
	}
	// An issuance whose ACK never arrives is retired as unknown application.
	let (dir, log) = survey_test_log();
	let steps = vec![
		ScriptedLease {
			ack_delay_ms: 300,
			..Default::default()
		},
		ScriptedLease::default(),
	];
	survey_worker_loop(
		scripted(steps),
		test_worker(),
		survey_test_params(2_000, 100, 400, 10, 1_500, 350),
		log.clone(),
		tokio::time::Instant::now(),
		log.now_us(),
	)
	.await;
	let events = survey_events(dir.path());
	let retired = events
		.iter()
		.find(|e| e["event"] == "survey_retired")
		.expect("retired");
	assert_eq!(retired["reason"], "unknown application");
}

/// ADR 628 B1.B control (delayed poll): on a current-thread runtime the scripted worker blocks the
/// THREAD past the old nominal boundary before replying, so the client's timeout cannot fire first;
/// the Applied that then completes is rejected as a refresh (kept as data) and the id is retired.
#[tokio::test(flavor = "current_thread")]
async fn survey_loop_rejects_a_refresh_completed_after_the_old_boundary() {
	let (dir, log) = survey_test_log();
	// term 600 ms, cadence 100, whole 400: the renewal at ~100 ms is valid (100 + 400 < 600); the
	// worker thread then blocks 700 ms, past the issuance's boundary (~600 ms).
	let params = survey_test_params(600, 100, 400, 10, 150, 450);
	params.check();
	let steps = vec![
		ScriptedLease::default(),
		ScriptedLease {
			starve_ms: 700,
			..Default::default()
		},
	];
	survey_worker_loop(
		scripted(steps),
		test_worker(),
		params,
		log.clone(),
		tokio::time::Instant::now(),
		log.now_us(),
	)
	.await;
	let events = survey_events(dir.path());
	let renewal = events
		.iter()
		.find(|e| e["event"] == "survey_exchange" && e["kind"] == "renewal")
		.expect("renewal");
	assert_eq!(renewal["outcome"], "Applied", "{renewal}");
	assert_eq!(
		renewal["refresh"], "rejected: completed at or after the old nominal boundary",
		"{renewal}"
	);
	let retired = events
		.iter()
		.find(|e| e["event"] == "survey_retired")
		.expect("retired");
	assert_eq!(retired["reason"], "refresh rejected");
}
