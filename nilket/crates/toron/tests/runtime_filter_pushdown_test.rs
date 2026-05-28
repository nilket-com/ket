use std::error::Error;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::sync::oneshot;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableRef};
use toron::distributed::budget::{ByteCount, QueryBudget, TaskBudget};
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::distributed::fragment::{FragmentPlanConfig, fragment_plan};
use toron::distributed::identity::{FrameByteLen, LeaseEpoch, QueryId, WorkerId};
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::scheduler::DistributedScheduler;
use toron::distributed::worker_registry::{
	ActiveTaskCount, SimdLaneCount, StaticWorkerRegistry, UnixMillis, WorkerCapabilities,
	WorkerEndpoint, WorkerEntry, WorkerLease, WorkerResourceState, WorkerStatus,
};
use toron::error::StorageError;
use toron::ir::{Expr, JoinKind, JoinStrategy, LogicalPlan, QueryProfileHint, SessionId, TableId};
use toron::server::{
	NetworkTaskDispatcher, ToronWorker, memory::MemoryManager, transport::TcpTransport,
};
use toron::storage::{
	HlcBound, RowIdentityProjection, ScanRequest, ScanStream, StorageScan, TenantId,
};
use twox_hash::XxHash3_64;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, RuntimeFilterBloom,
	RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor,
};

const RUNTIME_FILTER_HASH_SEED_2: u64 = 0x9e37_79b9_7f4a_7c15;

#[tokio::test]
async fn end_to_end_selective_join_emits_byte_identical_results_with_filter_enabled()
-> Result<(), Box<dyn Error>> {
	let baseline = run_selective_join(false, false).await?;
	let filtered = run_selective_join(true, true).await?;

	assert_eq!(filtered.digest, baseline.digest);
	assert!(filtered.counters.runtime_filter_requests > 0);
	assert!(filtered.counters.filtered_rows_after < filtered.counters.filtered_rows_before);
	Ok(())
}

#[tokio::test]
async fn end_to_end_wendelon_disabled_byte_identical_to_filter_disabled()
-> Result<(), Box<dyn Error>> {
	let filter_disabled = run_selective_join(false, false).await?;
	let wendelon_disabled = run_selective_join(true, false).await?;

	assert_eq!(wendelon_disabled.digest, filter_disabled.digest);
	assert_eq!(
		wendelon_disabled.counters.filtered_rows_after,
		wendelon_disabled.counters.filtered_rows_before
	);
	Ok(())
}

#[tokio::test]
async fn mock_wendelon_silently_drops_on_fingerprint_mismatch() -> Result<(), Box<dyn Error>> {
	let fixture = RuntimeFilterFixture::new(true)?;
	let orders = fixture.orders.clone();
	let request = scan_request(
		TableId(2),
		fixture.orders_descriptor.table_object_id,
		fixture.orders_descriptor.physical_descriptor_fingerprint,
		Some(runtime_filter_descriptor(
			fixture.customers_descriptor.physical_descriptor_fingerprint,
			1,
			vec![0],
		)),
	);

	let batch = fixture.scan_batch(&request)?;

	assert_eq!(batch.num_rows(), orders.num_rows());
	assert_eq!(
		fixture.counters.lock().unwrap().fingerprint_mismatch_drops,
		1
	);
	Ok(())
}

#[tokio::test]
async fn end_to_end_filter_dropped_on_fingerprint_mismatch_is_byte_identical()
-> Result<(), Box<dyn Error>> {
	let fixture = RuntimeFilterFixture::new(true)?;
	let baseline = canonical_arrow_ipc_digest(
		std::slice::from_ref(&fixture.orders),
		SortPolicy::FullRowLex,
	)?;
	let request = scan_request(
		TableId(2),
		fixture.orders_descriptor.table_object_id,
		fixture.orders_descriptor.physical_descriptor_fingerprint,
		Some(runtime_filter_descriptor(
			fixture.customers_descriptor.physical_descriptor_fingerprint,
			1,
			vec![0],
		)),
	);

	let batch = fixture.scan_batch(&request)?;
	let observed = canonical_arrow_ipc_digest(&[batch], SortPolicy::FullRowLex)?;

	assert_eq!(observed, baseline);
	Ok(())
}

#[tokio::test]
async fn end_to_end_saturated_filter_emits_byte_identical_results() -> Result<(), Box<dyn Error>> {
	let fixture = RuntimeFilterFixture::new(true)?;
	let orders = fixture.orders.clone();
	let request = scan_request(
		TableId(2),
		fixture.orders_descriptor.table_object_id,
		fixture.orders_descriptor.physical_descriptor_fingerprint,
		Some(runtime_filter_descriptor(
			fixture.orders_descriptor.physical_descriptor_fingerprint,
			8,
			vec![0xFF],
		)),
	);

	let batch = fixture.scan_batch(&request)?;

	assert_eq!(batch.num_rows(), orders.num_rows());
	Ok(())
}

#[tokio::test]
async fn end_to_end_no_false_negatives_on_random_keys() -> Result<(), Box<dyn Error>> {
	let schema = Arc::new(Schema::new(vec![
		Field::new("order_id", DataType::Int64, false),
		Field::new("customer_id", DataType::Int64, false),
	]));
	let values = (0_i64..257)
		.map(|idx| (idx * 48_271 + 17) % 1_000_003)
		.collect::<Vec<_>>();
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from_iter_values(
				0_i64..i64::try_from(values.len())?,
			)) as ArrayRef,
			Arc::new(Int64Array::from(values.clone())) as ArrayRef,
		],
	)?;
	let descriptor = runtime_filter_descriptor_for_values(
		RuntimeFilterFixture::new(true)?
			.orders_descriptor
			.physical_descriptor_fingerprint,
		&values,
	);

	let filtered = apply_runtime_filter(&batch, &descriptor)?;

	assert_eq!(filtered.num_rows(), batch.num_rows());
	Ok(())
}

#[tokio::test]
async fn end_to_end_two_wave_barrier_enforced() -> Result<(), Box<dyn Error>> {
	let fixture = RuntimeFilterFixture::new(false)?;
	let mut catalog = MockCatalog::new();
	catalog.register_descriptor(TableId(1), fixture.customers_descriptor.clone())?;
	catalog.register_descriptor(TableId(2), fixture.orders_descriptor.clone())?;
	let bound = bind(&join_plan(), &catalog, &PlannerConfig::default()).await?;
	let enabled = fragment_plan(
		bound.clone(),
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(query_budget()),
		)
		.with_worker_count(2)
		.with_runtime_filters_enabled(true),
	)?;
	let disabled = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(query_budget()),
		)
		.with_worker_count(2)
		.with_runtime_filters_enabled(false),
	)?;

	assert_eq!(enabled[1].upstream_stages, vec![enabled[0].stage_id]);
	assert!(disabled[1].upstream_stages.is_empty());
	Ok(())
}

async fn run_selective_join(
	runtime_filters_enabled: bool,
	apply_runtime_filters: bool,
) -> Result<RunOutput, Box<dyn Error>> {
	let memory = Arc::new(MemoryManager::new(16, 1024 * 1024));
	let fixture = RuntimeFilterFixture::new(apply_runtime_filters)?;
	let mut workers = Vec::new();
	for worker_index in 0..2 {
		workers.push(spawn_worker(worker_index, fixture.clone(), memory.clone()).await?);
	}
	let registry = StaticWorkerRegistry::new(workers.iter().map(|worker| worker.entry.clone()));
	let mut catalog = MockCatalog::new();
	catalog.register_descriptor(TableId(1), fixture.customers_descriptor.clone())?;
	catalog.register_descriptor(TableId(2), fixture.orders_descriptor.clone())?;
	let bound = bind(&join_plan(), &catalog, &PlannerConfig::default()).await?;
	let stages = fragment_plan(
		bound,
		FragmentPlanConfig::new(
			QueryId::new(),
			TaskBudget::from_query_budget(query_budget()),
		)
		.with_worker_count(2)
		.with_runtime_filters_enabled(runtime_filters_enabled),
	)?;
	let mut scheduler = DistributedScheduler::new(registry);
	let mut dispatcher = NetworkTaskDispatcher::new(memory, TcpTransport::new());
	let run = scheduler
		.dispatch_loop(&stages, &mut dispatcher)
		.await
		.map_err(|error| std::io::Error::other(format!("{error:?}")))?;
	let digest = canonical_arrow_ipc_digest(&run.final_results, SortPolicy::FullRowLex)?;
	let counters = fixture.counters.lock().unwrap().clone();

	for worker in workers {
		worker.shutdown.send(()).ok();
		worker.handle.abort();
	}

	Ok(RunOutput {
		digest: digest.get(),
		counters,
	})
}

fn join_plan() -> LogicalPlan {
	LogicalPlan::Join {
		left: Box::new(LogicalPlan::Exchange {
			partition_count: 2,
			key: vec![Expr::qualified_column(TableRef::new("customers"), "id")],
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
		}),
		right: Box::new(LogicalPlan::Exchange {
			partition_count: 2,
			key: vec![Expr::qualified_column(
				TableRef::new("orders"),
				"customer_id",
			)],
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
		}),
		kind: JoinKind::Inner,
		on: vec![(
			Expr::qualified_column(TableRef::new("customers"), "id"),
			Expr::qualified_column(TableRef::new("orders"), "customer_id"),
		)],
		strategy: JoinStrategy::HashPartitioned,
	}
}

#[derive(Debug)]
struct RunOutput {
	digest: [u8; 32],
	counters: RuntimeFilterCounters,
}

#[derive(Clone)]
struct RuntimeFilterFixture {
	customers: RecordBatch,
	orders: RecordBatch,
	customers_descriptor: TableSchemaDescriptor,
	orders_descriptor: TableSchemaDescriptor,
	apply_runtime_filters: bool,
	counters: Arc<Mutex<RuntimeFilterCounters>>,
}

#[derive(Debug, Default, Clone)]
struct RuntimeFilterCounters {
	runtime_filter_requests: u64,
	filtered_rows_before: usize,
	filtered_rows_after: usize,
	fingerprint_mismatch_drops: u64,
}

impl RuntimeFilterFixture {
	fn new(apply_runtime_filters: bool) -> Result<Self, Box<dyn Error>> {
		let customers_schema =
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("order_id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]));
		let customers = RecordBatch::try_new(
			customers_schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef],
		)?;
		let orders = RecordBatch::try_new(
			orders_schema.clone(),
			vec![
				Arc::new(Int64Array::from_iter_values(0_i64..12)) as ArrayRef,
				Arc::new(Int64Array::from(vec![
					1_i64, 10, 2, 11, 3, 12, 13, 14, 15, 16, 17, 18,
				])) as ArrayRef,
			],
		)?;
		Ok(Self {
			customers,
			orders,
			customers_descriptor: descriptor(1, "customers", customers_schema.as_ref(), "id"),
			orders_descriptor: descriptor(2, "orders", orders_schema.as_ref(), "order_id"),
			apply_runtime_filters,
			counters: Arc::new(Mutex::new(RuntimeFilterCounters::default())),
		})
	}

	fn scan_batch(&self, request: &ScanRequest) -> Result<RecordBatch, StorageError> {
		let mut batch = match request.table_id {
			TableId(1) => self.customers.clone(),
			TableId(2) => self.orders.clone(),
			table => return Err(StorageError::TableNotFound { table }),
		};
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
		if let Some(runtime_filter) = &request.runtime_filter {
			let before = batch.num_rows();
			if self.apply_runtime_filters {
				if runtime_filter.probe_descriptor_fingerprint
					== request.physical_descriptor_fingerprint
				{
					batch = apply_runtime_filter(&batch, runtime_filter)?;
				} else {
					self.counters.lock().unwrap().fingerprint_mismatch_drops += 1;
				}
			}
			let mut counters = self.counters.lock().unwrap();
			counters.runtime_filter_requests += 1;
			counters.filtered_rows_before += before;
			counters.filtered_rows_after += batch.num_rows();
		}
		if let Some(columns) = &request.projected_columns {
			let indices = columns
				.iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}
		Ok(batch)
	}
}

#[async_trait::async_trait]
impl StorageScan for RuntimeFilterFixture {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let batch = self.scan_batch(&request)?;
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			vec![batch],
		))
	}
}

fn apply_runtime_filter(
	batch: &RecordBatch,
	runtime_filter: &RuntimeFilterDescriptor,
) -> Result<RecordBatch, StorageError> {
	if runtime_filter.key_types.as_slice() != [RuntimeFilterKeyType::Int64]
		|| runtime_filter.probe_key_columns.len() != 1
	{
		return Ok(batch.clone());
	}
	let mask = match wendelon::schema::build_runtime_filter_mask_for_batch(batch, runtime_filter) {
		Ok(mask) => mask,
		Err(_) => return Ok(batch.clone()),
	};
	filter_record_batch(batch, &mask).map_err(StorageError::from)
}

fn runtime_filter_descriptor(
	fingerprint: wendelon::schema::PhysicalDescriptorFingerprint,
	bit_count: u32,
	payload: Vec<u8>,
) -> RuntimeFilterDescriptor {
	RuntimeFilterDescriptor {
		key_encoding_version: 1,
		key_types: vec![RuntimeFilterKeyType::Int64],
		probe_key_columns: vec![1],
		kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
			hash_seed: 7,
			bit_count,
			hash_count: 1,
			byte_budget: payload.len() as u32,
			target_fp_rate_bp: 100,
			payload,
		}),
		probe_descriptor_fingerprint: fingerprint,
	}
}

fn runtime_filter_descriptor_for_values(
	fingerprint: wendelon::schema::PhysicalDescriptorFingerprint,
	values: &[i64],
) -> RuntimeFilterDescriptor {
	let bit_count = 16 * 1024 * 8;
	let hash_count = 7;
	let mut payload = vec![0u8; bit_count / 8];
	for value in values {
		let mut encoded = Vec::with_capacity(9);
		encoded.push(5);
		encoded.extend_from_slice(&value.to_le_bytes());
		set_bloom_bits(&mut payload, bit_count as u32, hash_count, 7, &encoded);
	}
	RuntimeFilterDescriptor {
		key_encoding_version: 1,
		key_types: vec![RuntimeFilterKeyType::Int64],
		probe_key_columns: vec![1],
		kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
			hash_seed: 7,
			bit_count: bit_count as u32,
			hash_count,
			byte_budget: payload.len() as u32,
			target_fp_rate_bp: 100,
			payload,
		}),
		probe_descriptor_fingerprint: fingerprint,
	}
}

fn set_bloom_bits(
	payload: &mut [u8],
	bit_count: u32,
	hash_count: u8,
	hash_seed: u64,
	encoded: &[u8],
) {
	let h1 = XxHash3_64::oneshot_with_seed(hash_seed, encoded);
	let mut h2 = XxHash3_64::oneshot_with_seed(hash_seed ^ RUNTIME_FILTER_HASH_SEED_2, encoded);
	if h2 == 0 {
		h2 = RUNTIME_FILTER_HASH_SEED_2;
	}
	for idx in 0..u64::from(hash_count) {
		let bit = h1.wrapping_add(idx.wrapping_mul(h2)) % u64::from(bit_count);
		let byte = (bit / 8) as usize;
		let mask = 1u8 << (bit % 8);
		payload[byte] |= mask;
	}
}

fn scan_request(
	table_id: TableId,
	table_object_id: TableObjectId,
	physical_descriptor_fingerprint: wendelon::schema::PhysicalDescriptorFingerprint,
	runtime_filter: Option<RuntimeFilterDescriptor>,
) -> ScanRequest {
	ScanRequest {
		session_id: SessionId(1),
		query_id: QueryId::from_u128(1),
		tenant_id: TenantId(1),
		table_id,
		table_object_id,
		physical_descriptor_fingerprint,
		read_bound: HlcBound(1),
		projected_columns: None,
		pbc_predicate: None,
		runtime_filter,
		row_identity: RowIdentityProjection::None,
		profile_hint: Some(QueryProfileHint::Scan),
		trace_context: None,
		morsel: None,
		row_limit: None,
		downstream_cancellation: None,
	}
}

struct WorkerHarness {
	entry: WorkerEntry,
	shutdown: oneshot::Sender<()>,
	handle: tokio::task::JoinHandle<()>,
}

async fn spawn_worker(
	worker_index: usize,
	fixture: RuntimeFilterFixture,
	memory: Arc<MemoryManager>,
) -> Result<WorkerHarness, Box<dyn Error>> {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
	let endpoint = WorkerEndpoint::new(listener.local_addr()?.to_string());
	let worker_id = WorkerId::from_u128(u128::try_from(worker_index + 1)?);
	let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
		Arc::new(fixture),
		memory,
		Arc::new(InMemoryPartitionStore::with_budget(query_budget())),
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

fn query_budget() -> QueryBudget {
	QueryBudget {
		bytes_in_flight_per_query: ByteCount::new(64 * 1024 * 1024),
		bytes_in_flight_per_worker_stream: ByteCount::new(64 * 1024 * 1024),
		memory_per_task: ByteCount::new(64 * 1024 * 1024),
		shuffle_bytes_published_per_task: ByteCount::new(64 * 1024 * 1024),
		spill_bytes_reserved_per_query: ByteCount::new(64 * 1024 * 1024),
		broadcast_build_bytes_per_task: ByteCount::new(64 * 1024 * 1024),
		broadcast_replicated_bytes_per_query: ByteCount::new(64 * 1024 * 1024),
		max_arrow_ipc_frame: FrameByteLen::from_u32(u32::MAX),
		cancellation_grace: toron::distributed::budget::DurationMillis::new(1_000),
		max_in_flight_tasks: None,
	}
}

fn descriptor(
	id_byte: u8,
	table: &str,
	schema: &Schema,
	primary_key: &str,
) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"default",
		table,
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single(primary_key)),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}
