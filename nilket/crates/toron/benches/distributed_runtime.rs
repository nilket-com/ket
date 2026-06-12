//! ADR 003 distributed-runtime benchmark harness.
//!
//! Smoke check:
//! `cargo bench --bench distributed_runtime -- --quick --distributed-runtime-only`.

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::distributed::budget::{ByteCount, DurationMillis, QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{LeaseEpoch, QueryId, WorkerId};
use toron::distributed::scheduler::DistributedScheduler;
use toron::distributed::worker_registry::{
	ActiveTaskCount, PlacementHints, SimdLaneCount, UnixMillis, WorkerCapabilities, WorkerEndpoint,
	WorkerEntry, WorkerLease, WorkerRegistry, WorkerResourceState, WorkerStatus,
};
use toron::ir::{AggregateFunction, Expr, LogicalPlan, TableId};
use toron::server::{
	NetworkTaskDispatcher, ToronWorker, memory::MemoryManager, transport::TcpTransport,
};
use toron::storage::InMemoryStorageScan;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

const DEFAULT_WORKERS: &[usize] = &[2, 4, 8];
const QUICK_WORKERS: &[usize] = &[2];
const DEFAULT_ROWS: usize = 65_536;
const QUICK_ROWS: usize = 8_192;

fn main() -> Result<(), Box<dyn Error>> {
	let config = BenchConfig::parse();
	let runtime = Runtime::new()?;
	for worker_count in &config.workers {
		let row = runtime.block_on(run_case(*worker_count, config.rows))?;
		print_row(&row, config.quick);
	}
	Ok(())
}

#[derive(Debug)]
struct BenchConfig {
	workers: Vec<usize>,
	rows: usize,
	quick: bool,
}

impl BenchConfig {
	fn parse() -> Self {
		let mut workers = DEFAULT_WORKERS.to_vec();
		let mut rows = DEFAULT_ROWS;
		let mut quick = false;
		for arg in std::env::args().skip(1) {
			if arg == "--quick" {
				workers = QUICK_WORKERS.to_vec();
				rows = QUICK_ROWS;
				quick = true;
			} else if let Some(value) = arg.strip_prefix("--workers=") {
				workers = value
					.split(',')
					.map(|part| {
						part.parse()
							.expect("--workers entries must be positive integers")
					})
					.collect();
			} else if let Some(value) = arg.strip_prefix("--rows=") {
				rows = value.parse().expect("--rows must be a positive integer");
			} else if arg == "--distributed-runtime-only" || arg == "--bench" {
			} else if arg == "--kill-test" {
				eprintln!(
					"distributed_runtime kill-test evidence lives in distributed_process_test"
				);
			} else {
				eprintln!("distributed_runtime ignoring unrecognized arg: {arg}");
			}
		}
		Self {
			workers,
			rows,
			quick,
		}
	}
}

#[derive(Debug)]
struct BenchRow {
	worker_count: usize,
	rows: usize,
	stage_count: usize,
	task_count: usize,
	exchange_count: usize,
	shuffle_bytes_published: u64,
	result_rows: usize,
	total_count: i64,
	result_digest: String,
	wall_ms: f64,
}

async fn run_case(worker_count: usize, rows: usize) -> Result<BenchRow, Box<dyn Error>> {
	let memory = Arc::new(MemoryManager::new(128, 1024 * 1024));
	let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
	let fixture = RecordBatch::try_new(
		schema.clone(),
		vec![Arc::new(Int32Array::from(
			(0..i32::try_from(rows)?).collect::<Vec<_>>(),
		))],
	)?;
	let mut workers = Vec::new();
	for worker_index in 0..worker_count {
		workers.push(spawn_worker(worker_index, fixture.clone(), memory.clone()).await?);
	}

	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(|worker| worker.entry.clone()),
	));
	let mut catalog = MockCatalog::new();
	catalog.register_descriptor(TableId(1), test_descriptor(schema.as_ref()))?;
	let plan = group_count_after_exchange_plan(u32::try_from(worker_count)?);
	let bound = bind(&plan, &catalog, &PlannerConfig::default()).await?;
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(QueryBudget::test_default()),
		),
	)?;
	let stage_count = stages.len();
	let task_count = stages.iter().map(|stage| stage.tasks.len()).sum();
	let started = Instant::now();
	let mut scheduler = DistributedScheduler::new(registry);
	let mut dispatcher = NetworkTaskDispatcher::new(memory, TcpTransport::new());
	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.map_err(|error| std::io::Error::other(format!("{error:?}")))?;
	let wall_ms = started.elapsed().as_secs_f64() * 1_000.0;
	let digest = canonical_arrow_ipc_digest(&run.final_results, SortPolicy::FullRowLex)?;
	let shuffle_bytes_published = run
		.accepted_manifests
		.values()
		.flat_map(|manifest| manifest.partitions.iter())
		.map(|entry| entry.size_bytes.get())
		.sum();
	let result_rows = run
		.final_results
		.iter()
		.map(RecordBatch::num_rows)
		.sum::<usize>();
	let total_count = sum_counts(&run.final_results);

	for worker in workers {
		worker.shutdown.send(()).ok();
		worker.handle.abort();
	}

	Ok(BenchRow {
		worker_count,
		rows,
		stage_count,
		task_count,
		exchange_count: 1,
		shuffle_bytes_published,
		result_rows,
		total_count,
		result_digest: hex_digest(digest.get()),
		wall_ms,
	})
}

fn print_row(row: &BenchRow, quick: bool) {
	println!(
		"adr003_distributed_runtime,quick={},workers={},rows={},stages={},tasks={},exchanges={},shuffle_bytes_published={},result_rows={},total_count={},digest={},wall_ms={:.3}",
		quick,
		row.worker_count,
		row.rows,
		row.stage_count,
		row.task_count,
		row.exchange_count,
		row.shuffle_bytes_published,
		row.result_rows,
		row.total_count,
		row.result_digest,
		row.wall_ms,
	);
}

struct WorkerHarness {
	entry: WorkerEntry,
	shutdown: oneshot::Sender<()>,
	handle: tokio::task::JoinHandle<()>,
}

async fn spawn_worker(
	worker_index: usize,
	fixture: RecordBatch,
	memory: Arc<MemoryManager>,
) -> Result<WorkerHarness, Box<dyn Error>> {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
	let endpoint = WorkerEndpoint::new(listener.local_addr()?.to_string());
	let worker_id = WorkerId::from_u128(u128::try_from(worker_index + 1)?);
	let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
		Arc::new(InMemoryStorageScan {
			table_data: fixture,
		}),
		memory,
		Arc::new(toron::distributed::partition_store::InMemoryPartitionStore::new()),
		worker_id,
		endpoint.clone(),
	));
	let (shutdown, mut shutdown_rx) = oneshot::channel();
	let handle = tokio::spawn(async move {
		loop {
			tokio::select! {
				_ = &mut shutdown_rx => break,
				accepted = listener.accept() => {
					let Ok((stream, _)) = accepted else {
						break;
					};
					let worker = worker.clone();
					tokio::spawn(async move {
						let _ = worker.handle_connection(stream).await;
					});
				}
			}
		}
	});

	Ok(WorkerHarness {
		entry: worker_entry(worker_id, endpoint),
		shutdown,
		handle,
	})
}

struct RoundRobinRegistry {
	workers: RwLock<HashMap<WorkerId, WorkerEntry>>,
	order: Vec<WorkerId>,
	next: AtomicUsize,
}

impl RoundRobinRegistry {
	fn new(workers: impl IntoIterator<Item = WorkerEntry>) -> Self {
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
			next: AtomicUsize::new(0),
		}
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

fn group_count_after_exchange_plan(partition_count: u32) -> LogicalPlan {
	LogicalPlan::Aggregate {
		group_expr: vec![Expr::column("val")],
		aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("val")])],
		input: Box::new(LogicalPlan::Exchange {
			partition_count,
			key: vec![Expr::column("val")],
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		}),
	}
}

fn sum_counts(results: &[RecordBatch]) -> i64 {
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

fn worker_entry(worker_id: WorkerId, endpoint: WorkerEndpoint) -> WorkerEntry {
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

fn test_descriptor(schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([1; 16]),
		1,
		"default",
		"distributed_runtime",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("val")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn hex_digest(bytes: [u8; 32]) -> String {
	let mut out = String::with_capacity(64);
	for byte in bytes {
		out.push_str(&format!("{byte:02x}"));
	}
	out
}
