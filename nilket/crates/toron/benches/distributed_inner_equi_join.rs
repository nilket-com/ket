//! ADR 004 distributed inner equi-join benchmark harness.
//!
//! Smoke check:
//! `cargo bench --bench distributed_inner_equi_join -- --quick --distributed-inner-equi-join-only`.

use arrow::array::Int64Array;
use arrow::compute::filter_record_batch;
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
use toron::catalog::{MockCatalog, TableRef};
use toron::distributed::budget::{ByteCount, DurationMillis, QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{FrameByteLen, LeaseEpoch, QueryId, WorkerId};
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::scheduler::DistributedScheduler;
use toron::distributed::worker_registry::{
	ActiveTaskCount, PlacementHints, SimdLaneCount, UnixMillis, WorkerCapabilities, WorkerEndpoint,
	WorkerEntry, WorkerLease, WorkerRegistry, WorkerResourceState, WorkerStatus,
};
use toron::error::StorageError;
use toron::ir::{
	BroadcastBuildSide, Expr, JoinKind, JoinStrategy, JoinStrategyIntent, JoinStrategyPinSource,
	LogicalPlan, TableId,
};
use toron::server::{
	NetworkTaskDispatcher, ToronWorker, memory::MemoryManager, transport::TcpTransport,
};
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, RuntimeFilterDescriptor,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
	build_runtime_filter_mask_for_batch,
};

const DEFAULT_WORKERS: &[usize] = &[2, 4, 8];
const QUICK_WORKERS: &[usize] = &[2];
const DEFAULT_BUILD_ROWS: usize = 65_536;
const DEFAULT_PROBE_ROWS: usize = 262_144;
const QUICK_BUILD_ROWS: usize = 2_048;
const QUICK_PROBE_ROWS: usize = 8_192;

fn main() -> Result<(), Box<dyn Error>> {
	let config = BenchConfig::parse();
	let runtime = Runtime::new()?;
	for worker_count in &config.workers {
		for join_kind in &config.join_kinds {
			for strategy in &config.strategies {
				for runtime_filters_enabled in &config.runtime_filter_modes {
					for wendelon_runtime_filters_enabled in &config.wendelon_runtime_filter_modes {
						let row = runtime.block_on(run_case(
							*join_kind,
							*strategy,
							*worker_count,
							config.build_rows,
							config.probe_rows,
							config.probe_key_domain,
							config.key_shape,
							*runtime_filters_enabled,
							*wendelon_runtime_filters_enabled,
						))?;
						print_row(&row, config.quick);
					}
				}
			}
		}
	}
	Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchStrategy {
	HashPartitioned,
	Broadcast,
}

impl BenchStrategy {
	fn as_str(self) -> &'static str {
		match self {
			Self::HashPartitioned => "hash_partitioned",
			Self::Broadcast => "broadcast",
		}
	}

	fn join_strategy(self) -> JoinStrategy {
		match self {
			Self::HashPartitioned => JoinStrategy::HashPartitioned,
			Self::Broadcast => JoinStrategy::Broadcast,
		}
	}

	fn build_side(self) -> Option<BroadcastBuildSide> {
		match self {
			Self::HashPartitioned => None,
			Self::Broadcast => Some(BroadcastBuildSide::Left),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchKeyShape {
	Single,
	Composite,
}

impl BenchKeyShape {
	fn as_str(self) -> &'static str {
		match self {
			Self::Single => "single",
			Self::Composite => "composite",
		}
	}
}

#[derive(Debug)]
struct BenchConfig {
	workers: Vec<usize>,
	build_rows: usize,
	probe_rows: usize,
	probe_key_domain: Option<usize>,
	join_kinds: Vec<JoinKind>,
	strategies: Vec<BenchStrategy>,
	key_shape: BenchKeyShape,
	runtime_filter_modes: Vec<bool>,
	wendelon_runtime_filter_modes: Vec<bool>,
	quick: bool,
}

impl BenchConfig {
	fn parse() -> Self {
		let mut workers = DEFAULT_WORKERS.to_vec();
		let mut build_rows = DEFAULT_BUILD_ROWS;
		let mut probe_rows = DEFAULT_PROBE_ROWS;
		let mut probe_key_domain = None;
		let mut join_kinds = Vec::new();
		let mut strategies = vec![BenchStrategy::HashPartitioned, BenchStrategy::Broadcast];
		let mut key_shape = BenchKeyShape::Single;
		let mut runtime_filter_modes = vec![true];
		let mut wendelon_runtime_filter_modes = vec![false];
		let mut quick = false;
		let mut strategy_specified = false;
		for arg in std::env::args().skip(1) {
			if arg == "--quick" {
				workers = QUICK_WORKERS.to_vec();
				build_rows = QUICK_BUILD_ROWS;
				probe_rows = QUICK_PROBE_ROWS;
				quick = true;
			} else if let Some(value) = arg.strip_prefix("--workers=") {
				workers = value
					.split(',')
					.map(|part| {
						part.parse()
							.expect("--workers entries must be positive integers")
					})
					.collect();
			} else if let Some(value) = arg.strip_prefix("--build-rows=") {
				build_rows = value.parse().expect("--build-rows must be positive");
			} else if let Some(value) = arg.strip_prefix("--probe-rows=") {
				probe_rows = value.parse().expect("--probe-rows must be positive");
			} else if let Some(value) = arg.strip_prefix("--probe-key-domain=") {
				probe_key_domain =
					Some(value.parse().expect("--probe-key-domain must be positive"));
			} else if let Some(value) = arg.strip_prefix("--strategy=") {
				strategy_specified = true;
				strategies = value
					.split(',')
					.map(|part| match part {
						"hash" | "hash_partitioned" => BenchStrategy::HashPartitioned,
						"broadcast" => BenchStrategy::Broadcast,
						other => panic!("unknown --strategy entry {other}"),
					})
					.collect();
			} else if let Some(value) = arg.strip_prefix("--kind=") {
				join_kinds.extend(value.split(',').map(parse_join_kind));
			} else if arg == "--composite-key" {
				key_shape = BenchKeyShape::Composite;
			} else if let Some(value) = arg.strip_prefix("--runtime-filter=") {
				runtime_filter_modes = value
					.split(',')
					.map(|part| match part {
						"enabled" | "on" | "true" => true,
						"disabled" | "off" | "false" => false,
						other => panic!("unknown --runtime-filter entry {other}"),
					})
					.collect();
			} else if let Some(value) = arg.strip_prefix("--wendelon-runtime-filter=") {
				wendelon_runtime_filter_modes = value
					.split(',')
					.map(|part| match part {
						"enabled" | "on" | "true" => true,
						"disabled" | "off" | "false" => false,
						other => panic!("unknown --wendelon-runtime-filter entry {other}"),
					})
					.collect();
			} else if arg == "--distributed-inner-equi-join-only" || arg == "--bench" {
			} else if arg == "--kill-test" {
				eprintln!("ADR004 kill-test evidence lives in distributed_process_test");
			} else {
				eprintln!("distributed_inner_equi_join ignoring unrecognized arg: {arg}");
			}
		}
		if join_kinds.is_empty() {
			join_kinds.push(JoinKind::Inner);
		} else if !strategy_specified && join_kinds.iter().any(|kind| *kind != JoinKind::Inner) {
			strategies = vec![BenchStrategy::HashPartitioned];
		}
		Self {
			workers,
			build_rows,
			probe_rows,
			probe_key_domain,
			join_kinds,
			strategies,
			key_shape,
			runtime_filter_modes,
			wendelon_runtime_filter_modes,
			quick,
		}
	}
}

fn parse_join_kind(value: &str) -> JoinKind {
	match value {
		"inner" => JoinKind::Inner,
		"left-outer" | "left_outer" => JoinKind::LeftOuter,
		"right-outer" | "right_outer" => JoinKind::RightOuter,
		"full-outer" | "full_outer" => JoinKind::FullOuter,
		"left-semi" | "left_semi" | "semi" => JoinKind::LeftSemi,
		"right-semi" | "right_semi" => JoinKind::RightSemi,
		"left-anti" | "left_anti" | "anti" => JoinKind::LeftAnti,
		"right-anti" | "right_anti" => JoinKind::RightAnti,
		other => panic!("unknown --kind entry {other}"),
	}
}

fn join_kind_as_str(kind: JoinKind) -> &'static str {
	match kind {
		JoinKind::Inner => "inner",
		JoinKind::LeftOuter => "left_outer",
		JoinKind::RightOuter => "right_outer",
		JoinKind::FullOuter => "full_outer",
		JoinKind::LeftSemi => "left_semi",
		JoinKind::RightSemi => "right_semi",
		JoinKind::LeftAnti => "left_anti",
		JoinKind::RightAnti => "right_anti",
		JoinKind::Cross => "cross",
	}
}

#[derive(Debug)]
struct BenchRow {
	join_kind: JoinKind,
	strategy: BenchStrategy,
	key_shape: BenchKeyShape,
	worker_count: usize,
	build_rows: usize,
	probe_rows: usize,
	probe_key_domain: usize,
	stage_count: usize,
	task_count: usize,
	runtime_filters_enabled: bool,
	wendelon_runtime_filters_enabled: bool,
	shuffle_bytes_published: u64,
	broadcast_replicated_bytes: u64,
	pulled_shuffle_bytes: u64,
	left_shuffle_bytes: u64,
	right_shuffle_bytes: u64,
	published_shuffle_bytes: u64,
	task_wall_ms: f64,
	shuffle_pull_decode_ms: f64,
	build_ms: f64,
	probe_ms: f64,
	spill_ms: f64,
	output_publish_ms: f64,
	task_setup_ms: f64,
	batch_encode_ms: f64,
	batch_send_ms: f64,
	batch_decode_ms: f64,
	dispatch_wait_ms: f64,
	telemetry_lock_ms: f64,
	framework_overhead_ms: f64,
	telemetry_build_rows: u64,
	telemetry_probe_rows: u64,
	matched_rows: u64,
	emitted_rows: u64,
	emitted_batches: u64,
	spill_bytes: u64,
	retry_count: u64,
	stale_rejection_count: u64,
	cancellation_cleanup_count: u64,
	result_rows: usize,
	result_digest: String,
	wall_ms: f64,
}

#[allow(clippy::too_many_arguments)]
async fn run_case(
	join_kind: JoinKind,
	strategy: BenchStrategy,
	worker_count: usize,
	build_rows: usize,
	probe_rows: usize,
	probe_key_domain: Option<usize>,
	key_shape: BenchKeyShape,
	runtime_filters_enabled: bool,
	wendelon_runtime_filters_enabled: bool,
) -> Result<BenchRow, Box<dyn Error>> {
	let memory = Arc::new(MemoryManager::new(128, 1024 * 1024));
	let budget = bench_budget(build_rows, probe_rows, worker_count);
	let fixture = Fixture::new(
		build_rows,
		probe_rows,
		probe_key_domain,
		key_shape,
		wendelon_runtime_filters_enabled,
	)?;
	let probe_key_domain = fixture.probe_key_domain;
	let mut workers = Vec::new();
	for worker_index in 0..worker_count {
		workers.push(spawn_worker(worker_index, fixture.clone(), memory.clone(), budget).await?);
	}

	let registry = Arc::new(RoundRobinRegistry::new(
		workers.iter().map(|worker| worker.entry.clone()),
	));
	let mut catalog = MockCatalog::new();
	catalog.register_descriptor(TableId(1), fixture.customers_descriptor.clone())?;
	catalog.register_descriptor(TableId(2), fixture.orders_descriptor.clone())?;
	let plan = join_plan(join_kind, strategy, u32::try_from(worker_count)?, key_shape);
	let bound = bind(&plan, &catalog, &PlannerConfig::default()).await?;
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new_for_test(QueryId::new(), TaskBudget::from_query_budget(budget))
			.with_worker_count(u32::try_from(worker_count)?)
			.with_runtime_filters_enabled(runtime_filters_enabled),
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
	let build_bytes = run
		.accepted_manifests
		.values()
		.flat_map(|manifest| manifest.partitions.iter())
		.map(|entry| entry.size_bytes.get())
		.min()
		.unwrap_or(0);
	let broadcast_replicated_bytes = if strategy == BenchStrategy::Broadcast {
		build_bytes.saturating_mul(worker_count as u64)
	} else {
		0
	};
	let result_rows = run
		.final_results
		.iter()
		.map(RecordBatch::num_rows)
		.sum::<usize>();
	let telemetry = aggregate_telemetry(&run.telemetry);

	for worker in workers {
		worker.shutdown.send(()).ok();
		worker.handle.abort();
	}

	Ok(BenchRow {
		join_kind,
		strategy,
		key_shape,
		worker_count,
		build_rows,
		probe_rows,
		probe_key_domain,
		stage_count,
		task_count,
		runtime_filters_enabled,
		wendelon_runtime_filters_enabled,
		shuffle_bytes_published,
		broadcast_replicated_bytes,
		pulled_shuffle_bytes: telemetry.pulled_shuffle_bytes,
		left_shuffle_bytes: telemetry.left_shuffle_bytes,
		right_shuffle_bytes: telemetry.right_shuffle_bytes,
		published_shuffle_bytes: telemetry.published_shuffle_bytes,
		task_wall_ms: micros_to_ms(telemetry.task_wall_micros),
		shuffle_pull_decode_ms: micros_to_ms(telemetry.shuffle_pull_decode_micros),
		build_ms: micros_to_ms(telemetry.build_micros),
		probe_ms: micros_to_ms(telemetry.probe_micros),
		spill_ms: micros_to_ms(telemetry.spill_micros),
		output_publish_ms: micros_to_ms(telemetry.output_publish_micros),
		task_setup_ms: micros_to_ms(telemetry.task_setup_micros),
		batch_encode_ms: micros_to_ms(telemetry.batch_encode_micros),
		batch_send_ms: micros_to_ms(telemetry.batch_send_micros),
		batch_decode_ms: micros_to_ms(telemetry.batch_decode_micros),
		dispatch_wait_ms: micros_to_ms(telemetry.dispatch_wait_micros),
		telemetry_lock_ms: micros_to_ms(telemetry.telemetry_lock_micros),
		framework_overhead_ms: micros_to_ms(telemetry.framework_overhead_micros),
		telemetry_build_rows: telemetry.build_rows,
		telemetry_probe_rows: telemetry.probe_rows,
		matched_rows: telemetry.matched_rows,
		emitted_rows: telemetry.emitted_rows,
		emitted_batches: telemetry.emitted_batches,
		spill_bytes: telemetry.spill_bytes,
		retry_count: telemetry.retry_count,
		stale_rejection_count: telemetry.stale_rejection_count,
		cancellation_cleanup_count: telemetry.cancellation_cleanup_count,
		result_rows,
		result_digest: hex_digest(digest.get()),
		wall_ms,
	})
}

fn print_row(row: &BenchRow, quick: bool) {
	println!(
		"adr004_distributed_inner_equi_join,quick={},kind={},strategy={},key={},runtime_filter={},wendelon_runtime_filter={},workers={},build_rows={},probe_rows={},probe_key_domain={},stages={},tasks={},shuffle_bytes_published={},broadcast_replicated_bytes={},pulled_shuffle_bytes={},left_shuffle_bytes={},right_shuffle_bytes={},published_shuffle_bytes={},task_wall_ms={:.3},shuffle_pull_decode_ms={:.3},build_ms={:.3},probe_ms={:.3},spill_ms={:.3},output_publish_ms={:.3},task_setup_ms={:.3},batch_encode_ms={:.3},batch_send_ms={:.3},batch_decode_ms={:.3},dispatch_wait_ms={:.3},telemetry_lock_ms={:.3},framework_overhead_ms={:.3},telemetry_build_rows={},telemetry_probe_rows={},matched_rows={},emitted_rows={},emitted_batches={},spill_bytes={},retry_count={},stale_rejection_count={},cancellation_cleanup_count={},result_rows={},digest={},wall_ms={:.3}",
		quick,
		join_kind_as_str(row.join_kind),
		row.strategy.as_str(),
		row.key_shape.as_str(),
		if row.runtime_filters_enabled {
			"enabled"
		} else {
			"disabled"
		},
		if row.wendelon_runtime_filters_enabled {
			"enabled"
		} else {
			"disabled"
		},
		row.worker_count,
		row.build_rows,
		row.probe_rows,
		row.probe_key_domain,
		row.stage_count,
		row.task_count,
		row.shuffle_bytes_published,
		row.broadcast_replicated_bytes,
		row.pulled_shuffle_bytes,
		row.left_shuffle_bytes,
		row.right_shuffle_bytes,
		row.published_shuffle_bytes,
		row.task_wall_ms,
		row.shuffle_pull_decode_ms,
		row.build_ms,
		row.probe_ms,
		row.spill_ms,
		row.output_publish_ms,
		row.task_setup_ms,
		row.batch_encode_ms,
		row.batch_send_ms,
		row.batch_decode_ms,
		row.dispatch_wait_ms,
		row.telemetry_lock_ms,
		row.framework_overhead_ms,
		row.telemetry_build_rows,
		row.telemetry_probe_rows,
		row.matched_rows,
		row.emitted_rows,
		row.emitted_batches,
		row.spill_bytes,
		row.retry_count,
		row.stale_rejection_count,
		row.cancellation_cleanup_count,
		row.result_rows,
		row.result_digest,
		row.wall_ms,
	);
}

#[derive(Debug, Default)]
struct AggregatedTelemetry {
	pulled_shuffle_bytes: u64,
	left_shuffle_bytes: u64,
	right_shuffle_bytes: u64,
	published_shuffle_bytes: u64,
	build_rows: u64,
	probe_rows: u64,
	matched_rows: u64,
	emitted_rows: u64,
	emitted_batches: u64,
	spill_bytes: u64,
	task_wall_micros: u64,
	shuffle_pull_decode_micros: u64,
	build_micros: u64,
	probe_micros: u64,
	spill_micros: u64,
	output_publish_micros: u64,
	task_setup_micros: u64,
	batch_encode_micros: u64,
	batch_send_micros: u64,
	batch_decode_micros: u64,
	dispatch_wait_micros: u64,
	telemetry_lock_micros: u64,
	framework_overhead_micros: u64,
	retry_count: u64,
	stale_rejection_count: u64,
	cancellation_cleanup_count: u64,
}

fn aggregate_telemetry(
	telemetry: &toron::distributed::scheduler::SchedulerTelemetry,
) -> AggregatedTelemetry {
	let mut aggregated = AggregatedTelemetry {
		retry_count: telemetry.retry_count,
		stale_rejection_count: telemetry.stale_rejection_count,
		cancellation_cleanup_count: telemetry.cancellation_cleanup_count,
		..AggregatedTelemetry::default()
	};
	for task in telemetry
		.task_telemetry
		.iter()
		.map(|record| &record.telemetry)
	{
		aggregated.pulled_shuffle_bytes = aggregated
			.pulled_shuffle_bytes
			.saturating_add(task.counters.pulled_shuffle_bytes);
		aggregated.left_shuffle_bytes = aggregated
			.left_shuffle_bytes
			.saturating_add(task.counters.left_shuffle_bytes);
		aggregated.right_shuffle_bytes = aggregated
			.right_shuffle_bytes
			.saturating_add(task.counters.right_shuffle_bytes);
		aggregated.published_shuffle_bytes = aggregated
			.published_shuffle_bytes
			.saturating_add(task.counters.published_shuffle_bytes);
		aggregated.build_rows = aggregated
			.build_rows
			.saturating_add(task.counters.build_rows);
		aggregated.probe_rows = aggregated
			.probe_rows
			.saturating_add(task.counters.probe_rows);
		aggregated.matched_rows = aggregated
			.matched_rows
			.saturating_add(task.counters.matched_rows);
		aggregated.emitted_rows = aggregated
			.emitted_rows
			.saturating_add(task.counters.emitted_rows);
		aggregated.emitted_batches = aggregated
			.emitted_batches
			.saturating_add(task.counters.emitted_batches);
		aggregated.spill_bytes = aggregated
			.spill_bytes
			.saturating_add(task.counters.spill_bytes);
		aggregated.task_wall_micros = aggregated
			.task_wall_micros
			.saturating_add(task.phases.task_wall_micros);
		aggregated.shuffle_pull_decode_micros = aggregated
			.shuffle_pull_decode_micros
			.saturating_add(task.phases.shuffle_pull_decode_micros);
		aggregated.build_micros = aggregated
			.build_micros
			.saturating_add(task.phases.build_micros);
		aggregated.probe_micros = aggregated
			.probe_micros
			.saturating_add(task.phases.probe_micros);
		aggregated.spill_micros = aggregated
			.spill_micros
			.saturating_add(task.phases.spill_micros);
		aggregated.output_publish_micros = aggregated
			.output_publish_micros
			.saturating_add(task.phases.output_publish_micros);
		aggregated.task_setup_micros = aggregated
			.task_setup_micros
			.saturating_add(task.phases.task_setup_micros);
		aggregated.batch_encode_micros = aggregated
			.batch_encode_micros
			.saturating_add(task.phases.batch_encode_micros);
		aggregated.batch_send_micros = aggregated
			.batch_send_micros
			.saturating_add(task.phases.batch_send_micros);
		aggregated.batch_decode_micros = aggregated
			.batch_decode_micros
			.saturating_add(task.phases.batch_decode_micros);
		aggregated.dispatch_wait_micros = aggregated
			.dispatch_wait_micros
			.saturating_add(task.phases.dispatch_wait_micros);
		aggregated.telemetry_lock_micros = aggregated
			.telemetry_lock_micros
			.saturating_add(task.phases.telemetry_lock_micros);
		aggregated.framework_overhead_micros = aggregated
			.framework_overhead_micros
			.saturating_add(task.phases.framework_overhead_micros);
	}
	aggregated
}

fn micros_to_ms(micros: u64) -> f64 {
	micros as f64 / 1_000.0
}

fn bench_budget(build_rows: usize, probe_rows: usize, worker_count: usize) -> QueryBudget {
	const GIB: u64 = 1024 * 1024 * 1024;
	let largest_rows = build_rows.max(probe_rows) as u64;
	let scale_cap = largest_rows.saturating_mul(256).clamp(GIB, 8 * GIB);
	QueryBudget {
		bytes_in_flight_per_query: ByteCount::new(scale_cap),
		bytes_in_flight_per_worker_stream: ByteCount::new(scale_cap),
		memory_per_task: ByteCount::new(scale_cap),
		shuffle_bytes_published_per_task: ByteCount::new(scale_cap),
		spill_bytes_reserved_per_query: ByteCount::new(scale_cap),
		broadcast_build_bytes_per_task: ByteCount::new(scale_cap),
		broadcast_replicated_bytes_per_query: ByteCount::new(
			scale_cap.saturating_mul(worker_count as u64),
		),
		max_arrow_ipc_frame: FrameByteLen::from_u32(u32::MAX),
		cancellation_grace: DurationMillis::new(1_000),
		max_in_flight_tasks: None,
	}
}

#[derive(Clone)]
struct Fixture {
	tables: HashMap<TableId, RecordBatch>,
	customers_descriptor: TableSchemaDescriptor,
	orders_descriptor: TableSchemaDescriptor,
	probe_key_domain: usize,
	apply_runtime_filters: bool,
}

impl Fixture {
	fn new(
		build_rows: usize,
		probe_rows: usize,
		probe_key_domain: Option<usize>,
		key_shape: BenchKeyShape,
		apply_runtime_filters: bool,
	) -> Result<Self, Box<dyn Error>> {
		let build_rows = build_rows.max(1);
		let probe_key_domain = probe_key_domain.unwrap_or(build_rows).max(1);
		let build_rows_i64 = i64::try_from(build_rows)?;
		let probe_rows_i64 = i64::try_from(probe_rows)?;
		let probe_key_domain_i64 = i64::try_from(probe_key_domain)?;
		let (customers_schema, customers_columns, orders_schema, orders_columns) = match key_shape {
			BenchKeyShape::Single => (
				Arc::new(Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("segment", DataType::Int64, false),
				])),
				vec![
					Arc::new(Int64Array::from_iter_values(0..build_rows_i64))
						as Arc<dyn arrow::array::Array>,
					Arc::new(Int64Array::from_iter_values(
						(0..build_rows_i64).map(|row| row % 16),
					)),
				],
				Arc::new(Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
				])),
				vec![
					Arc::new(Int64Array::from_iter_values(0..probe_rows_i64))
						as Arc<dyn arrow::array::Array>,
					Arc::new(Int64Array::from_iter_values(
						(0..probe_rows_i64).map(|row| row % probe_key_domain_i64),
					)),
				],
			),
			BenchKeyShape::Composite => (
				Arc::new(Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("region", DataType::Int64, false),
					Field::new("segment", DataType::Int64, false),
				])),
				vec![
					Arc::new(Int64Array::from_iter_values(0..build_rows_i64))
						as Arc<dyn arrow::array::Array>,
					Arc::new(Int64Array::from_iter_values(
						(0..build_rows_i64).map(|row| row % 32),
					)),
					Arc::new(Int64Array::from_iter_values(
						(0..build_rows_i64).map(|row| row % 16),
					)),
				],
				Arc::new(Schema::new(vec![
					Field::new("id", DataType::Int64, false),
					Field::new("customer_id", DataType::Int64, false),
					Field::new("region", DataType::Int64, false),
				])),
				vec![
					Arc::new(Int64Array::from_iter_values(0..probe_rows_i64))
						as Arc<dyn arrow::array::Array>,
					Arc::new(Int64Array::from_iter_values(
						(0..probe_rows_i64).map(|row| row % probe_key_domain_i64),
					)),
					Arc::new(Int64Array::from_iter_values(
						(0..probe_rows_i64).map(|row| (row % probe_key_domain_i64) % 32),
					)),
				],
			),
		};
		let customers = RecordBatch::try_new(customers_schema.clone(), customers_columns)?;
		let orders = RecordBatch::try_new(orders_schema.clone(), orders_columns)?;
		Ok(Self {
			tables: HashMap::from([(TableId(1), customers), (TableId(2), orders)]),
			customers_descriptor: descriptor(1, "customers", customers_schema.as_ref()),
			orders_descriptor: descriptor(2, "orders", orders_schema.as_ref()),
			probe_key_domain,
			apply_runtime_filters,
		})
	}
}

#[async_trait::async_trait]
impl StorageScan for Fixture {
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
		if self.apply_runtime_filters
			&& let Some(runtime_filter) = &request.runtime_filter
			&& runtime_filter.probe_descriptor_fingerprint
				== request.physical_descriptor_fingerprint
		{
			batch = apply_runtime_filter(&batch, runtime_filter)?;
		}
		if let Some(columns) = &request.projected_columns {
			let indices = columns
				.iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![batch],
		))
	}
}

fn apply_runtime_filter(
	batch: &RecordBatch,
	runtime_filter: &RuntimeFilterDescriptor,
) -> Result<RecordBatch, StorageError> {
	// Route through the shared kind-aware mask helper so the bench
	// exercises the same path as production (`InMemoryStorageScan` and the
	// Wendelon scan engine). Falls back to passing the batch through
	// unfiltered if the descriptor is unusable — same advisory-drop
	// semantics as production.
	match build_runtime_filter_mask_for_batch(batch, runtime_filter) {
		Ok(mask) => filter_record_batch(batch, &mask).map_err(StorageError::from),
		Err(_) => Ok(batch.clone()),
	}
}

struct WorkerHarness {
	entry: WorkerEntry,
	shutdown: oneshot::Sender<()>,
	handle: tokio::task::JoinHandle<()>,
}

async fn spawn_worker(
	worker_index: usize,
	fixture: Fixture,
	memory: Arc<MemoryManager>,
	budget: QueryBudget,
) -> Result<WorkerHarness, Box<dyn Error>> {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
	let endpoint = WorkerEndpoint::new(listener.local_addr()?.to_string());
	let worker_id = WorkerId::from_u128(u128::try_from(worker_index + 1)?);
	let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
		Arc::new(fixture),
		memory,
		Arc::new(InMemoryPartitionStore::with_budget(budget)),
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

fn join_plan(
	kind: JoinKind,
	strategy: BenchStrategy,
	partition_count: u32,
	key_shape: BenchKeyShape,
) -> LogicalPlan {
	let left_scan = LogicalPlan::ScanNamed {
		table: TableRef::new("customers"),
		projection: None,
	};
	let right_scan = LogicalPlan::ScanNamed {
		table: TableRef::new("orders"),
		projection: None,
	};
	let (left, right) = match strategy {
		BenchStrategy::HashPartitioned => (
			LogicalPlan::Exchange {
				partition_count,
				key: join_left_keys(key_shape),
				input: Box::new(left_scan),
			},
			LogicalPlan::Exchange {
				partition_count,
				key: join_right_keys(key_shape),
				input: Box::new(right_scan),
			},
		),
		BenchStrategy::Broadcast => (
			LogicalPlan::Exchange {
				partition_count: 1,
				key: join_left_keys(key_shape),
				input: Box::new(left_scan),
			},
			right_scan,
		),
	};

	LogicalPlan::Join {
		left: Box::new(left),
		right: Box::new(right),
		kind,
		on: join_on(key_shape),
		on_filter: None,
		null_safe_keys: false,
		strategy_intent: JoinStrategyIntent::Pinned {
			strategy: strategy.join_strategy(),
			source: JoinStrategyPinSource::Direct,
			build_side: strategy.build_side(),
		},
	}
}

fn join_left_keys(key_shape: BenchKeyShape) -> Vec<Expr> {
	match key_shape {
		BenchKeyShape::Single => vec![Expr::qualified_column(TableRef::new("customers"), "id")],
		BenchKeyShape::Composite => vec![
			Expr::qualified_column(TableRef::new("customers"), "region"),
			Expr::qualified_column(TableRef::new("customers"), "id"),
		],
	}
}

fn join_right_keys(key_shape: BenchKeyShape) -> Vec<Expr> {
	match key_shape {
		BenchKeyShape::Single => {
			vec![Expr::qualified_column(
				TableRef::new("orders"),
				"customer_id",
			)]
		}
		BenchKeyShape::Composite => vec![
			Expr::qualified_column(TableRef::new("orders"), "region"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		],
	}
}

fn join_on(key_shape: BenchKeyShape) -> Vec<(Expr, Expr)> {
	match key_shape {
		BenchKeyShape::Single => vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		BenchKeyShape::Composite => vec![
			(
				Expr::qualified_column(TableRef::new("customers"), "region"),
				Expr::qualified_column(TableRef::new("orders"), "region"),
			),
			(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			),
		],
	}
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

fn descriptor(id_byte: u8, table: &str, schema: &Schema) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"default",
		table,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
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
