use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::task::JoinHandle;

use toron::binder::PlannerConfig;
use toron::catalog::{Catalog, MockCatalog, TableRef};
use toron::coordinator::{DistributedQueryCoordinator, QueryCoordinator};
use toron::distributed::budget::{ByteCount, QueryBudget};
use toron::distributed::identity::{LeaseEpoch, QueryId, WorkerId};
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::worker_registry::{
	ActiveTaskCount, SimdLaneCount, StaticWorkerRegistry, UnixMillis, WorkerCapabilities,
	WorkerEndpoint, WorkerEntry, WorkerLease, WorkerResourceState, WorkerStatus,
};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{SessionId, TableId};
use toron::server::ToronWorker;
use toron::server::memory::MemoryManager;
use toron::server::transport::{ToronStream, ToronTransport};
use toron::sql_planner::SqlPlanner;
use toron::storage::{HlcBound, InMemoryTableStorage, StorageWrite, TenantId, WriteRequest};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

#[tokio::test]
async fn distributed_insert_values_writes_without_dispatch() {
	let catalog = Arc::new(catalog_with_tables([table_descriptor(
		1,
		"people",
		people_schema(),
	)]));
	let storage = Arc::new(InMemoryTableStorage::new());
	let coordinator = DistributedQueryCoordinator::with_writer(
		catalog.clone(),
		Arc::new(StaticWorkerRegistry::new([])),
		Arc::new(MemoryManager::new(1, 1024 * 1024)),
		Arc::new(FailingTransport),
		PlannerConfig::default(),
		QueryBudget::test_default(),
		storage.clone(),
	);
	let plan = SqlPlanner::new()
		.plan("INSERT INTO people (id, name) VALUES (1, 'Ada'), (2, 'Grace')")
		.expect("plan insert");

	coordinator
		.execute_query(&plan)
		.await
		.expect("distributed insert values");

	let rows =
		people_rows(query(catalog, storage, "SELECT id, name FROM people ORDER BY id").await);
	assert_eq!(rows, vec![(1, "Ada".to_string()), (2, "Grace".to_string())]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_insert_select_fragments_source_and_writes_once() {
	let catalog = Arc::new(catalog_with_tables([
		table_descriptor(1, "src", people_schema()),
		table_descriptor(2, "dst", people_schema()),
	]));
	let storage = Arc::new(InMemoryTableStorage::new());
	seed_people(catalog.as_ref(), storage.as_ref(), "src").await;
	let worker = spawn_worker(storage.clone(), WorkerId::from_u128(10)).await;
	let coordinator = DistributedQueryCoordinator::with_writer(
		catalog.clone(),
		Arc::new(StaticWorkerRegistry::new([worker.entry.clone()])),
		Arc::new(MemoryManager::new(16, 1024 * 1024)),
		Arc::new(toron::server::transport::TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
		storage.clone(),
	);
	let plan = SqlPlanner::new()
		.plan("INSERT INTO dst (id, name) SELECT id, name FROM src WHERE id > 1")
		.expect("plan insert select");

	coordinator
		.execute_query(&plan)
		.await
		.expect("distributed insert select");

	let rows = people_rows(query(catalog, storage, "SELECT id, name FROM dst ORDER BY id").await);
	assert_eq!(rows, vec![(2, "Grace".to_string()), (3, "Lin".to_string())]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_ctas_fragments_source_then_creates_and_writes() {
	let catalog = Arc::new(catalog_with_tables([table_descriptor(
		1,
		"src",
		people_schema(),
	)]));
	let storage = Arc::new(InMemoryTableStorage::new());
	seed_people(catalog.as_ref(), storage.as_ref(), "src").await;
	let worker = spawn_worker(storage.clone(), WorkerId::from_u128(20)).await;
	let coordinator = DistributedQueryCoordinator::with_writer(
		catalog.clone(),
		Arc::new(StaticWorkerRegistry::new([worker.entry.clone()])),
		Arc::new(MemoryManager::new(16, 1024 * 1024)),
		Arc::new(toron::server::transport::TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
		storage.clone(),
	);
	let plan = SqlPlanner::new()
		.plan("CREATE TABLE copied AS SELECT id, name FROM src WHERE id <> 2")
		.expect("plan ctas");

	coordinator
		.execute_query(&plan)
		.await
		.expect("distributed ctas");

	let rows =
		people_rows(query(catalog, storage, "SELECT id, name FROM copied ORDER BY id").await);
	assert_eq!(rows, vec![(1, "Ada".to_string()), (3, "Lin".to_string())]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_delete_fragments_candidate_read_and_deletes_once() {
	let catalog = Arc::new(catalog_with_tables([table_descriptor(
		1,
		"people",
		people_schema(),
	)]));
	let storage = Arc::new(InMemoryTableStorage::new());
	seed_people(catalog.as_ref(), storage.as_ref(), "people").await;
	let worker = spawn_worker(storage.clone(), WorkerId::from_u128(30)).await;
	let coordinator = DistributedQueryCoordinator::with_writer_and_deleter(
		catalog.clone(),
		Arc::new(StaticWorkerRegistry::new([worker.entry.clone()])),
		Arc::new(MemoryManager::new(16, 1024 * 1024)),
		Arc::new(toron::server::transport::TcpTransport::new()),
		PlannerConfig::default(),
		QueryBudget::test_default(),
		storage.clone(),
		storage.clone(),
	);
	let plan = SqlPlanner::new()
		.plan("DELETE FROM people WHERE id >= 2")
		.expect("plan delete");

	let result = coordinator
		.execute_query(&plan)
		.await
		.expect("distributed delete");

	assert_eq!(deleted_count(&result), 2);
	let rows =
		people_rows(query(catalog, storage, "SELECT id, name FROM people ORDER BY id").await);
	assert_eq!(rows, vec![(1, "Ada".to_string())]);
}

struct FailingTransport;

#[async_trait::async_trait]
impl ToronTransport for FailingTransport {
	async fn connect(
		&self,
		_address: &str,
	) -> Result<Box<dyn ToronStream>, toron::error::QueryError> {
		Err(toron::error::QueryError::Internal {
			message: "transport should not be used for INSERT VALUES".to_string(),
		})
	}
}

struct WorkerHarness {
	entry: WorkerEntry,
	handle: JoinHandle<()>,
}

impl Drop for WorkerHarness {
	fn drop(&mut self) {
		self.handle.abort();
	}
}

async fn spawn_worker(storage: Arc<InMemoryTableStorage>, worker_id: WorkerId) -> WorkerHarness {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bind worker");
	let endpoint = WorkerEndpoint::new(listener.local_addr().expect("worker addr").to_string());
	let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
		storage,
		Arc::new(MemoryManager::new(32, 1024 * 1024)),
		Arc::new(InMemoryPartitionStore::new()),
		worker_id,
		endpoint.clone(),
	));
	let handle = tokio::spawn(async move {
		loop {
			let Ok((stream, _)) = listener.accept().await else {
				break;
			};
			let worker = worker.clone();
			tokio::spawn(async move {
				let _ = worker.handle_connection(stream).await;
			});
		}
	});

	WorkerHarness {
		entry: worker_entry(worker_id, endpoint),
		handle,
	}
}

async fn query(
	catalog: Arc<MockCatalog>,
	storage: Arc<InMemoryTableStorage>,
	sql: &str,
) -> Vec<RecordBatch> {
	let coordinator = QueryCoordinator::new(
		catalog,
		Executor::with_context(
			storage,
			ExecutionContext::new(
				SessionId(1),
				QueryId::from_u128(1),
				TenantId(1),
				HlcBound(1),
			),
		),
		PlannerConfig::default(),
	);
	let plan = SqlPlanner::new().plan(sql).expect("plan query");
	coordinator
		.execute_query(&plan, None)
		.await
		.expect("query target")
}

async fn seed_people(catalog: &MockCatalog, storage: &InMemoryTableStorage, table: &str) {
	let binding = catalog
		.resolve_table(&TableRef::new(table))
		.await
		.expect("resolve seed table");
	storage
		.write(WriteRequest {
			session_id: SessionId(1),
			query_id: QueryId::from_u128(1),
			tenant_id: TenantId(1),
			table_id: binding.local_table_id,
			table_object_id: binding.table,
			physical_descriptor_fingerprint: binding.fingerprint,
			payload: people_batch(),
			profile_hint: None,
			trace_context: None,
		})
		.await
		.expect("seed people");
}

fn people_rows(results: Vec<RecordBatch>) -> Vec<(i32, String)> {
	let batch = concat(results);
	let ids = batch
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("id column");
	let names = batch
		.column(1)
		.as_any()
		.downcast_ref::<StringArray>()
		.expect("name column");
	(0..batch.num_rows())
		.map(|row| (ids.value(row), names.value(row).to_string()))
		.collect()
}

fn deleted_count(results: &[RecordBatch]) -> i64 {
	let batch = concat(results.to_vec());
	batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("deleted column")
		.value(0)
}

fn concat(results: Vec<RecordBatch>) -> RecordBatch {
	let schema = results.first().expect("at least one batch").schema();
	arrow::compute::concat_batches(&schema, results.iter()).expect("concat batches")
}

fn catalog_with_tables(
	descriptors: impl IntoIterator<Item = (TableId, TableSchemaDescriptor)>,
) -> MockCatalog {
	let mut catalog = MockCatalog::new();
	for (table_id, descriptor) in descriptors {
		catalog
			.register_descriptor(table_id, descriptor)
			.expect("register descriptor");
	}
	catalog
}

fn table_descriptor(id: u64, table: &str, schema: Schema) -> (TableId, TableSchemaDescriptor) {
	let table_id = TableId(id);
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([id as u8; 16]),
		1,
		"default",
		table,
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor");
	(table_id, descriptor)
}

fn people_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int32, false),
		Field::new("name", DataType::Utf8, false),
	])
}

fn people_batch() -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(people_schema()),
		vec![
			Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
			Arc::new(StringArray::from(vec!["Ada", "Grace", "Lin"])) as ArrayRef,
		],
	)
	.expect("people batch")
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
