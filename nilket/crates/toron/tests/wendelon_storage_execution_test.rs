use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::WendelonCatalog;
use toron::distributed::determinism::{SortPolicy, canonical_arrow_ipc_digest};
use toron::exec::{ExecutionContext, Executor};
use toron::ir::{
	AggregateFunction, ExecutionPlan, Expr, FilterDisposition, LogicalPlan, QueryId, ScalarValue,
	SessionId, TableId,
};
use toron::storage::{
	HlcBound, InMemoryStorageScan, TenantId as ToronTenantId, WendelonStorageScan,
};
use wendelon::client::{ClusterPeerWire, ClusterStatusWire, StaticTopologySource};
use wendelon::consensus::WendelonRequest;
use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKindV1, SchemaObjectKind, SchemaObjectRefV1, SchemaUpdateEnvelopeV1,
	encode_schema_update_envelope_v1,
};
use wendelon::network::pool::BufPool;
use wendelon::network::transport::{
	NodeRole, NodeRoleSet, TransportProvider, TransportTier, TransportTierSet,
};
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RUNTIME_FILTER_HASH_SEED,
	RUNTIME_FILTER_KEY_ENCODING_VERSION, RecordBodyCompressionPolicy, RowIdentityPolicy,
	RuntimeFilterBloom, RuntimeFilterDescriptor, RuntimeFilterKeyType, RuntimeFilterKind,
	SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptor, TenantId as WendelonTenantId,
	encode_runtime_filter_key, encode_table_schema_descriptor_v1,
	insert_runtime_filter_encoded_key,
	pbc::{OP_GT, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT},
};
use wendelon::simulator::network::{SimulatedTransportNetwork, SimulatedTransportProvider};
use wendelon_client::{Client, TableQueryOptions, WriteOptions};

const LIVE_TENANT: WendelonTenantId = WendelonTenantId::new(1);
const OTHER_TENANT: WendelonTenantId = WendelonTenantId::new(2);
const HOUR_MS: i64 = 60 * 60 * 1000;
const BASE_TS: i64 = 1_599_998_400_000;

#[tokio::test]
async fn toron_executes_pbc_projection_against_live_wendelon_table_query() {
	let fixture = LiveWendelonFixture::start(31).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let ids = query_ids_gt(&fixture, &descriptor, LIVE_TENANT, 100).await;
	assert_eq!(ids, vec![2, 4]);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_storage_scan_keeps_live_tenants_isolated() {
	let fixture = LiveWendelonFixture::start(33).await;
	create_live_tenant(&fixture.node, OTHER_TENANT).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	install_schema(&fixture.node, OTHER_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;
	write_rows(
		&fixture.client,
		OTHER_TENANT,
		&descriptor,
		batch_from_rows(&[
			(10, Some(75), Some("north"), BASE_TS),
			(11, Some(250), Some("south"), BASE_TS + HOUR_MS),
		]),
	)
	.await;

	let live_ids = query_ids_gt(&fixture, &descriptor, LIVE_TENANT, 100).await;
	let other_ids = query_ids_gt(&fixture, &descriptor, OTHER_TENANT, 100).await;

	assert_eq!(live_ids, vec![2, 4]);
	assert_eq!(other_ids, vec![11]);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_declined_time_bucket_filter_matches_in_memory_baseline() {
	let fixture = LiveWendelonFixture::start(34).await;
	let descriptor = descriptor();
	let input = batch();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, input.clone()).await;

	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::TimeBucket(
					Duration::from_millis(HOUR_MS as u64),
					Box::new(Expr::column("ts")),
				)),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(BASE_TS)))),
			),
		}),
		output_names: vec![None],
		exprs: vec![Expr::column("id")],
	};
	let config = PlannerConfig {
		allow_experimental_kernels: true,
	};
	let bound = bind_plan(&descriptor, &plan, &config).await;
	assert_filter_declined_without_pbc(&bound);

	let live = execute_wendelon_plan(&fixture, &descriptor, LIVE_TENANT, &bound).await;
	let baseline = execute_in_memory_plan(LIVE_TENANT, input, &bound).await;
	assert_batches_equal(&baseline, &live);
	assert_eq!(ids_from_single_column(&live), vec![1, 2, 4]);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_table_query_runtime_filter_fetches_omitted_probe_key() {
	let fixture = LiveWendelonFixture::start(36).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let mut options = TableQueryOptions::default();
	options.physical_descriptor_fingerprint = Some(descriptor.physical_descriptor_fingerprint);
	let pbc = pbc_project_amounts_gt_100();
	let filtered = fixture
		.client
		.table_query_with_runtime_filter(
			LIVE_TENANT,
			&descriptor,
			pbc.clone(),
			Some(runtime_filter_for_i64_values(&descriptor, &[2, 4])),
			options,
		)
		.await
		.expect("runtime-filtered table query");
	let unfiltered = fixture
		.client
		.table_query(LIVE_TENANT, &descriptor, pbc, options)
		.await
		.expect("unfiltered table query");

	assert_batches_equal(&unfiltered.batches, &filtered.batches);
	assert_eq!(
		amounts_from_single_column(&filtered.batches),
		vec![150, 200]
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_table_query_runtime_filter_reduces_boundary_rows() {
	let fixture = LiveWendelonFixture::start(37).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(
		&fixture.client,
		LIVE_TENANT,
		&descriptor,
		selective_batch(1024),
	)
	.await;

	let filter = runtime_filter_for_i64_values(&descriptor, &[2, 4]);
	let unfiltered = table_query_id_amounts(&fixture, &descriptor, None).await;
	let filtered = table_query_id_amounts(&fixture, &descriptor, Some(filter)).await;
	let unfiltered_rows = row_count(&unfiltered);
	let filtered_rows = row_count(&filtered);
	let unfiltered_bytes = encode_batches(&unfiltered).len();
	let filtered_bytes = encode_batches(&filtered).len();

	assert!(filtered_rows < unfiltered_rows);
	assert!(filtered_bytes < unfiltered_bytes);

	let unfiltered_joined = joined_amounts_for_ids(&unfiltered, &[2, 4]);
	let filtered_joined = joined_amounts_for_ids(&filtered, &[2, 4]);
	let unfiltered_digest =
		canonical_arrow_ipc_digest(&[unfiltered_joined], SortPolicy::FullRowLex)
			.expect("unfiltered digest");
	let filtered_digest = canonical_arrow_ipc_digest(&[filtered_joined], SortPolicy::FullRowLex)
		.expect("filtered digest");
	assert_eq!(unfiltered_digest, filtered_digest);
	println!(
		"adr013_production_wendelon_runtime_filter rows_off={unfiltered_rows} rows_on={filtered_rows} bytes_off={unfiltered_bytes} bytes_on={filtered_bytes} digest={}",
		hex32(unfiltered_digest.get()),
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_table_query_exact_set_runtime_filter_matches_bloom_and_reduces_boundary_rows() {
	let fixture = LiveWendelonFixture::start(38).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(
		&fixture.client,
		LIVE_TENANT,
		&descriptor,
		selective_batch(1024),
	)
	.await;

	let build_ids = [2, 4];
	let unfiltered = table_query_id_amounts(&fixture, &descriptor, None).await;
	let bloom = table_query_id_amounts(
		&fixture,
		&descriptor,
		Some(runtime_filter_for_i64_values(&descriptor, &build_ids)),
	)
	.await;
	let exact = table_query_id_amounts(
		&fixture,
		&descriptor,
		Some(runtime_filter_exact_for_i64_values(&descriptor, &build_ids)),
	)
	.await;

	let unfiltered_rows = row_count(&unfiltered);
	let bloom_rows = row_count(&bloom);
	let exact_rows = row_count(&exact);
	let unfiltered_bytes = encode_batches(&unfiltered).len();
	let bloom_bytes = encode_batches(&bloom).len();
	let exact_bytes = encode_batches(&exact).len();
	assert!(bloom_rows < unfiltered_rows);
	assert!(exact_rows <= bloom_rows);
	assert!(exact_bytes <= bloom_bytes);
	let exact_fp = false_positive_rows(&exact, &build_ids);
	assert_eq!(exact_fp, 0);

	let unfiltered_digest = canonical_arrow_ipc_digest(
		&[joined_amounts_for_ids(&unfiltered, &build_ids)],
		SortPolicy::FullRowLex,
	)
	.expect("unfiltered digest");
	let bloom_digest = canonical_arrow_ipc_digest(
		&[joined_amounts_for_ids(&bloom, &build_ids)],
		SortPolicy::FullRowLex,
	)
	.expect("bloom digest");
	let exact_digest = canonical_arrow_ipc_digest(
		&[joined_amounts_for_ids(&exact, &build_ids)],
		SortPolicy::FullRowLex,
	)
	.expect("exact digest");
	assert_eq!(bloom_digest, unfiltered_digest);
	assert_eq!(exact_digest, unfiltered_digest);
	println!(
		"adr014_exact_small_set_runtime_filter rows_off={unfiltered_rows} rows_bloom={bloom_rows} rows_exact={exact_rows} bytes_off={unfiltered_bytes} bytes_bloom={bloom_bytes} bytes_exact={exact_bytes} exact_false_positive_rows=0 digest={}",
		hex32(exact_digest.get()),
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

#[tokio::test]
async fn wendelon_grouped_aggregate_matches_in_memory_baseline() {
	let fixture = LiveWendelonFixture::start(35).await;
	let descriptor = descriptor();
	let input = batch();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, input.clone()).await;

	let plan = LogicalPlan::Aggregate {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		group_expr: vec![Expr::column("region")],
		aggr_expr: vec![
			(AggregateFunction::Sum, vec![Expr::column("amount")]),
			(AggregateFunction::Count, vec![Expr::column("amount")]),
			(AggregateFunction::CountAll, vec![]),
		],
	};
	let bound = bind_plan(&descriptor, &plan, &PlannerConfig::default()).await;

	let live = execute_wendelon_plan(&fixture, &descriptor, LIVE_TENANT, &bound).await;
	let baseline = execute_in_memory_plan(LIVE_TENANT, input, &bound).await;
	assert_batches_equal(&baseline, &live);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

async fn query_ids_gt(
	fixture: &LiveWendelonFixture,
	descriptor: &TableSchemaDescriptor,
	tenant: WendelonTenantId,
	threshold: i32,
) -> Vec<i64> {
	let catalog =
		WendelonCatalog::from_descriptors([(TableId(1), descriptor.clone())]).expect("catalog");
	let mut descriptors = HashMap::new();
	descriptors.insert(TableId(1), descriptor.clone());
	let storage = Arc::new(WendelonStorageScan::new(
		fixture.client.clone(),
		descriptors,
	));
	let executor = Executor::with_context(
		storage,
		ExecutionContext::new(
			SessionId(10),
			QueryId::from_u128(20),
			ToronTenantId(tenant.get()),
			HlcBound(0),
		),
	);
	let plan = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Gt(
				Box::new(Expr::column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(threshold)))),
			),
		}),
		output_names: vec![None],
		exprs: vec![Expr::column("id")],
	};
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	let results = executor
		.execute_bound(&bound, None)
		.await
		.expect("execute live Wendelon-backed query");

	assert_eq!(results.len(), 1);
	let ids = results[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("id column");
	ids.values().to_vec()
}

async fn bind_plan(
	descriptor: &TableSchemaDescriptor,
	plan: &LogicalPlan,
	config: &PlannerConfig,
) -> ExecutionPlan {
	let catalog =
		WendelonCatalog::from_descriptors([(TableId(1), descriptor.clone())]).expect("catalog");
	bind(plan, &catalog, config).await.expect("bind")
}

async fn execute_wendelon_plan(
	fixture: &LiveWendelonFixture,
	descriptor: &TableSchemaDescriptor,
	tenant: WendelonTenantId,
	bound: &ExecutionPlan,
) -> Vec<RecordBatch> {
	let mut descriptors = HashMap::new();
	descriptors.insert(TableId(1), descriptor.clone());
	let storage = Arc::new(WendelonStorageScan::new(
		fixture.client.clone(),
		descriptors,
	));
	let executor = Executor::with_context(storage, execution_context(tenant));
	executor
		.execute_bound(bound, None)
		.await
		.expect("execute live Wendelon-backed query")
}

async fn execute_in_memory_plan(
	tenant: WendelonTenantId,
	batch: RecordBatch,
	bound: &ExecutionPlan,
) -> Vec<RecordBatch> {
	let storage = Arc::new(InMemoryStorageScan { table_data: batch });
	let executor = Executor::with_context(storage, execution_context(tenant));
	executor
		.execute_bound(bound, None)
		.await
		.expect("execute in-memory baseline")
}

async fn table_query_id_amounts(
	fixture: &LiveWendelonFixture,
	descriptor: &TableSchemaDescriptor,
	runtime_filter: Option<RuntimeFilterDescriptor>,
) -> Vec<RecordBatch> {
	let mut options = TableQueryOptions::default();
	options.physical_descriptor_fingerprint = Some(descriptor.physical_descriptor_fingerprint);
	fixture
		.client
		.table_query_with_runtime_filter(
			LIVE_TENANT,
			descriptor,
			pbc_project_id_amounts(),
			runtime_filter,
			options,
		)
		.await
		.expect("table query id/amounts")
		.batches
}

fn execution_context(tenant: WendelonTenantId) -> ExecutionContext {
	ExecutionContext::new(
		SessionId(10),
		QueryId::from_u128(20),
		ToronTenantId(tenant.get()),
		HlcBound(0),
	)
}

fn assert_filter_declined_without_pbc(plan: &ExecutionPlan) {
	match plan {
		ExecutionPlan::Project { input, .. } => assert_filter_declined_without_pbc(input),
		ExecutionPlan::Filter {
			input, disposition, ..
		} => {
			assert_eq!(*disposition, FilterDisposition::EvaluateInToron);
			assert_scan_has_no_pbc(input);
		}
		other => panic!("expected project/filter plan, got {other:?}"),
	}
}

fn assert_scan_has_no_pbc(plan: &ExecutionPlan) {
	match plan {
		ExecutionPlan::Scan { pbc_program, .. } => assert!(
			pbc_program.is_none(),
			"declined filter must not attach a PBC program"
		),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Sort { input, .. } => assert_scan_has_no_pbc(input),
		ExecutionPlan::Join { left, right, .. } => {
			assert_scan_has_no_pbc(left);
			assert_scan_has_no_pbc(right);
		}
	}
}

fn assert_batches_equal(expected: &[RecordBatch], actual: &[RecordBatch]) {
	assert_eq!(actual.len(), expected.len(), "batch count");
	assert_eq!(
		encode_batches(expected),
		encode_batches(actual),
		"record batch IPC payloads"
	);
}

fn encode_batches(batches: &[RecordBatch]) -> Vec<u8> {
	let schema = batches.first().expect("at least one batch").schema();
	let mut bytes = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("IPC writer");
		for batch in batches {
			writer.write(batch).expect("write IPC batch");
		}
		writer.finish().expect("finish IPC stream");
	}
	bytes
}

fn ids_from_single_column(batches: &[RecordBatch]) -> Vec<i64> {
	assert_eq!(batches.len(), 1);
	let ids = batches[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.expect("id column");
	ids.values().to_vec()
}

fn amounts_from_single_column(batches: &[RecordBatch]) -> Vec<i32> {
	assert_eq!(batches.len(), 1);
	assert_eq!(batches[0].num_columns(), 1);
	let amounts = batches[0]
		.column(0)
		.as_any()
		.downcast_ref::<Int32Array>()
		.expect("amount column");
	amounts.values().to_vec()
}

fn row_count(batches: &[RecordBatch]) -> usize {
	batches.iter().map(RecordBatch::num_rows).sum()
}

fn false_positive_rows(batches: &[RecordBatch], build_ids: &[i64]) -> usize {
	let mut rows = 0usize;
	for batch in batches {
		assert_eq!(batch.num_columns(), 2);
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");
		for row in 0..batch.num_rows() {
			if !build_ids.contains(&ids.value(row)) {
				rows += 1;
			}
		}
	}
	rows
}

fn joined_amounts_for_ids(batches: &[RecordBatch], build_ids: &[i64]) -> RecordBatch {
	let mut amounts = Vec::new();
	for batch in batches {
		assert_eq!(batch.num_columns(), 2);
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");
		let batch_amounts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("amount column");
		for row in 0..batch.num_rows() {
			if build_ids.contains(&ids.value(row)) {
				amounts.push(if batch_amounts.is_null(row) {
					None
				} else {
					Some(batch_amounts.value(row))
				});
			}
		}
	}
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![Field::new(
			"amount",
			DataType::Int32,
			true,
		)])),
		vec![Arc::new(Int32Array::from(amounts)) as ArrayRef],
	)
	.expect("joined amounts batch")
}

fn hex32(bytes: [u8; 32]) -> String {
	bytes
		.iter()
		.map(|byte| format!("{byte:02x}"))
		.collect::<Vec<_>>()
		.join("")
}

#[tokio::test]
async fn wendelon_storage_scan_maps_server_descriptor_evolution() {
	let fixture = LiveWendelonFixture::start(32).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let mut evolved = descriptor.clone();
	evolved.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
	evolved
		.refresh_descriptor_fingerprints()
		.expect("refresh evolved fingerprint");
	assert_ne!(
		evolved.physical_descriptor_fingerprint,
		descriptor.physical_descriptor_fingerprint
	);
	let pbc = Vec::new();
	let mut options = TableQueryOptions::default();
	options.physical_descriptor_fingerprint = Some(evolved.physical_descriptor_fingerprint);
	let err = fixture
		.client
		.table_query(LIVE_TENANT, &evolved, pbc, options)
		.await
		.expect_err("server detects stale physical descriptor pin");

	assert!(
		matches!(
			&err,
			wendelon_client::ClientError::TableDescriptorEvolved { .. }
		),
		"expected TableDescriptorEvolved, got {err:?}"
	);

	fixture.node.handles.abort();
	let _ = std::fs::remove_dir_all(fixture.data_dir);
}

struct LiveWendelonFixture {
	node: WendelonNode,
	client: Arc<Client>,
	data_dir: std::path::PathBuf,
}

impl LiveWendelonFixture {
	async fn start(node_id: u32) -> Self {
		let data_dir = std::env::temp_dir().join(format!(
			"toron-wendelon-live-{node_id}-{}",
			rand::random::<u64>()
		));
		std::fs::create_dir_all(&data_dir).expect("create Wendelon data dir");
		let address = format!("sim://node-{node_id}");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			node_id,
			data_dir.clone(),
			address.clone(),
		))
		.await
		.expect("bootstrap Wendelon node");
		wait_for_single_node_leader(&node).await;
		create_live_tenant(&node, LIVE_TENANT).await;
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;

		let pool = BufPool::new(64, 8 * 1024 * 1024);
		let network = Arc::new(SimulatedTransportNetwork::default());
		let client_node = wendelon::schema::NodeId {
			id: node_id + 10_000,
			_reserved: 0,
		};
		let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
			client_node,
			network,
			pool,
			TransportTier::QuicIoUring,
		));
		provider
			.publish_local(node.local_node, &address, node.service.clone())
			.expect("publish simulated Wendelon service");
		let snapshot = single_node_snapshot(node_id, address);
		let mut providers = HashMap::new();
		providers.insert(TransportTier::QuicIoUring, provider);
		let inner = wendelon::client::WendelonClient::new(wendelon::client::WendelonClientConfig {
			local_tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
			providers,
			bootstrap: snapshot.clone(),
			topology: Arc::new(StaticTopologySource::new(snapshot)),
			pool_max_retained: 64,
			pool_max_cap_bytes: 8 * 1024 * 1024,
		});
		let client = Arc::new(Client::with_client_uuid(inner, [node_id as u8; 16]));

		Self {
			node,
			client,
			data_dir,
		}
	}
}

async fn create_live_tenant(node: &WendelonNode, tenant: WendelonTenantId) {
	node.create_tenant(tenant, u64::MAX, 1)
		.await
		.expect("create live Toron test tenant");
	node.data_server.sync_tenant_policy_cache_once().await;
}

async fn wait_for_single_node_leader(node: &WendelonNode) {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node Wendelon leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

fn single_node_snapshot(node_id: u32, address: String) -> ClusterStatusWire {
	ClusterStatusWire {
		epoch: 1,
		leader: Some(node_id),
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id,
			address,
			tier_bits: TransportTierSet::empty()
				.with(TransportTier::QuicIoUring)
				.as_u8(),
			role_bits: NodeRoleSet::empty()
				.with(NodeRole::Data)
				.with(NodeRole::Control)
				.as_u8(),
			capability_bits: 0,
		}],
	}
}

async fn install_schema(
	node: &WendelonNode,
	tenant: WendelonTenantId,
	descriptor: &TableSchemaDescriptor,
) {
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(schema_update(
			tenant, descriptor,
		)))
		.await
		.expect("install schema");
}

fn schema_update(tenant: WendelonTenantId, descriptor: &TableSchemaDescriptor) -> Vec<u8> {
	let payload = encode_table_schema_descriptor_v1(descriptor).expect("encode descriptor");
	let intent_byte = tenant.get() as u8;
	let envelope = SchemaUpdateEnvelopeV1 {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: tenant,
		object: SchemaObjectRefV1 {
			kind: SchemaObjectKind::Table,
			namespace: descriptor.namespace.clone(),
			name: descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(descriptor.schema_generation),
		intent_id: SchemaIntentId::new([intent_byte; 16]),
		mutation: SchemaMutationKindV1::Create,
		payload,
	};
	encode_schema_update_envelope_v1(&envelope).expect("encode schema update")
}

async fn write_rows(
	client: &Client,
	tenant: WendelonTenantId,
	descriptor: &TableSchemaDescriptor,
	batch: RecordBatch,
) {
	let row_count = batch.num_rows();
	let receipt = client
		.table_write(
			tenant,
			descriptor,
			batch,
			WriteOptions {
				await_finalization: true,
				..WriteOptions::default()
			},
		)
		.await
		.expect("write typed table rows");
	assert_eq!(receipt.rows as usize, row_count);
}

fn descriptor() -> TableSchemaDescriptor {
	let schema = schema();
	TableSchemaDescriptor::new(
		TableObjectId::new([0x91; 16]),
		1,
		"bench",
		"storage_execution",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::IdentityProjection,
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

fn schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("region", DataType::Utf8, true),
		Field::new("ts", DataType::Int64, false),
	])
}

fn batch() -> RecordBatch {
	batch_from_rows(&[
		(1, Some(50), Some("north"), BASE_TS + 5 * 60 * 1000),
		(2, Some(150), Some("east"), BASE_TS + 55 * 60 * 1000),
		(3, None, Some("north"), BASE_TS + HOUR_MS + 15 * 60 * 1000),
		(4, Some(200), Some("west"), BASE_TS + 30 * 60 * 1000),
	])
}

fn batch_from_rows(rows: &[(i64, Option<i32>, Option<&str>, i64)]) -> RecordBatch {
	let schema = Arc::new(schema());
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
			)) as ArrayRef,
			Arc::new(Int32Array::from(
				rows.iter()
					.map(|(_, amount, _, _)| *amount)
					.collect::<Vec<_>>(),
			)) as ArrayRef,
			Arc::new(StringArray::from(
				rows.iter()
					.map(|(_, _, region, _)| *region)
					.collect::<Vec<_>>(),
			)) as ArrayRef,
			Arc::new(Int64Array::from(
				rows.iter()
					.map(|(_, _, _, timestamp)| *timestamp)
					.collect::<Vec<_>>(),
			)) as ArrayRef,
		],
	)
	.expect("batch")
}

fn selective_batch(rows: i64) -> RecordBatch {
	let schema = Arc::new(schema());
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
			Arc::new(Int32Array::from_iter_values(
				(0..rows).map(|row| i32::try_from(row * 10).expect("amount fits i32")),
			)) as ArrayRef,
			Arc::new(StringArray::from(vec![Some("bulk"); rows as usize])) as ArrayRef,
			Arc::new(Int64Array::from_iter_values(
				(0..rows).map(|row| BASE_TS + row * 1000),
			)) as ArrayRef,
		],
	)
	.expect("selective batch")
}

fn pbc_project_amounts_gt_100() -> Vec<u8> {
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&100_i64.to_le_bytes());
	bytecode.push(OP_GT);
	bytecode.push(OP_PROJECT);
	bytecode.extend_from_slice(&1_u16.to_le_bytes());
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode
}

fn pbc_project_id_amounts() -> Vec<u8> {
	let mut bytecode = Vec::new();
	bytecode.push(OP_PROJECT);
	bytecode.extend_from_slice(&2_u16.to_le_bytes());
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode
}

fn runtime_filter_for_i64_values(
	descriptor: &TableSchemaDescriptor,
	values: &[i64],
) -> RuntimeFilterDescriptor {
	let bit_count = 4096;
	let hash_count = 3;
	let byte_budget = bit_count / 8;
	let mut payload = vec![0; byte_budget as usize];
	let array = Int64Array::from(values.to_vec());
	for row in 0..array.len() {
		let encoded = encode_runtime_filter_key(&array, row)
			.expect("encode runtime filter key")
			.expect("non-null key");
		insert_runtime_filter_encoded_key(
			&mut payload,
			bit_count,
			hash_count,
			RUNTIME_FILTER_HASH_SEED,
			&encoded,
		);
	}
	RuntimeFilterDescriptor {
		key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
		key_types: vec![RuntimeFilterKeyType::Int64],
		probe_key_columns: vec![0],
		kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
			hash_seed: RUNTIME_FILTER_HASH_SEED,
			bit_count,
			hash_count,
			byte_budget,
			target_fp_rate_bp: 100,
			payload,
		}),
		probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
	}
}

fn runtime_filter_exact_for_i64_values(
	descriptor: &TableSchemaDescriptor,
	values: &[i64],
) -> RuntimeFilterDescriptor {
	let array = Int64Array::from(values.to_vec());
	let mut keys = Vec::new();
	for row in 0..array.len() {
		keys.push(
			encode_runtime_filter_key(&array, row)
				.expect("encode runtime filter key")
				.expect("non-null key"),
		);
	}
	RuntimeFilterDescriptor {
		key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
		key_types: vec![RuntimeFilterKeyType::Int64],
		probe_key_columns: vec![0],
		kind: RuntimeFilterKind::ExactSet(
			wendelon::schema::runtime_filter_exact_set_from_encoded_keys(keys, 16 * 1024)
				.expect("exact runtime filter"),
		),
		probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
	}
}
