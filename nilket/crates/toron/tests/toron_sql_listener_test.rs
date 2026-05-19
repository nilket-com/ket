use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use rcgen::{
	BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
	KeyUsagePurpose,
};
use toron::config::{
	CoordinatorConfig, SqlConfig, SqlTransport, TlsConfig, ToronBootstrapConfig,
	ToronCatalogConfig, ToronDescriptorBootstrap, ToronDescriptorBootstrapTable, ToronNodeConfig,
	ToronObservabilityConfig, ToronStorageConfig, ToronTracingFormat, ToronWendelonBootstrapPeer,
	ToronWendelonStorageConfig, ToronWorkerConfig,
};
use toron::daemon::ToronDaemon;
use toron::distributed::identity::{QueryId, WorkerId};
use toron::network::tls::{
	DEFAULT_CLUSTER_DOMAIN, canonical_client_dns_name, canonical_daemon_dns_name,
	client_config_from_paths,
};
use toron::quic::QuicEndpoint;
use toron::sql_wire::{
	Complete, SqlError, SqlErrorCode, SqlFrame, SqlMessage, read_frame as read_sql_frame,
	write_frame as write_sql_frame,
};
use toron::storage::TenantId as ToronTenantId;
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
	LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor, TenantId as WendelonTenantId,
	encode_table_schema_descriptor_v1,
};
use wendelon::simulator::network::{SimulatedTransportNetwork, SimulatedTransportProvider};
use wendelon_client::{Client, WriteOptions};

const LIVE_TENANT: WendelonTenantId = WendelonTenantId::new(1);
const BASE_TS: i64 = 1_599_998_400_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toron_sql_select_from_bootstrapped_descriptor_executes_over_wire() {
	let fixture = LiveWendelonFixture::start(43).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let config = toron_config_with_descriptor(descriptor.clone(), true);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let response = execute_sql(
		endpoint,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await
	.expect("SQL select response");

	assert_eq!(response.complete.row_count, 2);
	let mut rows = projected_rows(&response.batches);
	rows.sort();
	assert_eq!(
		rows,
		vec![
			(2, Some(150), Some("east".to_string())),
			(4, Some(200), Some("west".to_string())),
		]
	);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toron_sql_select_executes_over_authenticated_remote_listener() {
	let fixture = LiveWendelonFixture::start(45).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let worker_id = WorkerId::from_u128(45);
	let pki = TestPki::generate(worker_id, "alice");
	let config = toron_config_with_descriptor_tls(descriptor.clone(), worker_id, &pki);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = connectable_loopback_addr(daemon.handle().sql_endpoint.expect("sql endpoint"));
	let server_name = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);

	let response = execute_sql_authenticated(
		endpoint,
		&server_name,
		&pki.client_cert,
		&pki.client_key,
		&pki.ca_cert,
		"SELECT id, amount, region FROM storage_execution WHERE amount > 100",
	)
	.await
	.expect("SQL select response over mTLS");

	assert_eq!(response.complete.row_count, 2);
	let mut rows = projected_rows(&response.batches);
	rows.sort();
	assert_eq!(
		rows,
		vec![
			(2, Some(150), Some("east".to_string())),
			(4, Some(200), Some("west".to_string())),
		]
	);

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toron_sql_uses_self_worker_registry_entry() {
	let descriptor = descriptor();
	let config = toron_config_with_descriptor(descriptor, false);
	let daemon = ToronDaemon::start(config).await.expect("start daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let error = execute_sql(endpoint, "SELECT id FROM storage_execution")
		.await
		.expect_err("self-worker disabled should leave no live worker");

	assert_eq!(error.code, SqlErrorCode::NoLiveWorker);

	daemon.shutdown().await.expect("shutdown daemon");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toron_sql_complete_frame_carries_stable_row_count() {
	let fixture = LiveWendelonFixture::start(44).await;
	let descriptor = descriptor();
	install_schema(&fixture.node, LIVE_TENANT, &descriptor).await;
	write_rows(&fixture.client, LIVE_TENANT, &descriptor, batch()).await;

	let config = toron_config_with_descriptor(descriptor, true);
	let daemon =
		ToronDaemon::start_with_wendelon_client_for_harness(config, Arc::clone(&fixture.client))
			.await
			.expect("start Toron SQL daemon");
	let endpoint = daemon.handle().sql_endpoint.expect("sql endpoint");

	let create_existing = execute_sql(
		endpoint,
		"CREATE TABLE IF NOT EXISTS storage_execution \
		 (id BIGINT NOT NULL, amount INT, region TEXT, ts BIGINT NOT NULL)",
	)
	.await
	.expect("create-if-not-exists response");
	assert_eq!(create_existing.complete.row_count, 0);
	assert!(create_existing.batches.is_empty());

	let insert = execute_sql(
		endpoint,
		"INSERT INTO storage_execution (id, amount, region, ts) \
		 VALUES (5, 250, 'south', 1599998400000)",
	)
	.await
	.expect("insert response");
	assert_eq!(insert.complete.row_count, 1);
	assert!(insert.batches.is_empty());

	let delete = execute_sql(endpoint, "DELETE FROM storage_execution WHERE id = 5")
		.await
		.expect("delete response");
	assert_eq!(delete.complete.row_count, 1);
	assert!(delete.batches.is_empty());

	let drop_missing = execute_sql(endpoint, "DROP TABLE IF EXISTS missing_table")
		.await
		.expect("drop-if-exists response");
	assert_eq!(drop_missing.complete.row_count, 0);
	assert!(drop_missing.batches.is_empty());

	daemon.shutdown().await.expect("shutdown daemon");
	fixture.shutdown();
}

#[tokio::test]
async fn toron_daemon_without_sql_section_preserves_worker_only_start() {
	let mut config = toron_config_with_descriptor(descriptor(), true);
	config.sql = None;

	let daemon = ToronDaemon::start(config).await.expect("start daemon");

	assert!(daemon.handle().worker_endpoint.ip().is_loopback());
	assert!(daemon.handle().sql_endpoint.is_none());

	daemon.shutdown().await.expect("shutdown daemon");
}

#[derive(Debug)]
struct SqlResponse {
	batches: Vec<RecordBatch>,
	complete: Complete,
}

async fn execute_sql(
	endpoint: std::net::SocketAddr,
	statement: &str,
) -> Result<SqlResponse, SqlError> {
	let client = toron::quic::QuicEndpoint::client().expect("sql client");
	let mut stream = client.connect_stream(endpoint).await.expect("connect sql");
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame::execute(query_id, statement.to_string(), None),
	)
	.await
	.expect("write sql request");
	stream.finish().await.expect("finish sql request");

	let mut batches = Vec::new();
	loop {
		let frame = read_sql_frame(&mut stream)
			.await
			.expect("read sql response");
		match frame.message {
			SqlMessage::ResultBatch(batch) => {
				let reader = StreamReader::try_new(batch.arrow_ipc_bytes.as_slice(), None)
					.expect("open Arrow IPC stream");
				for batch in reader {
					batches.push(batch.expect("read Arrow IPC batch"));
				}
			}
			SqlMessage::Complete(complete) => return Ok(SqlResponse { batches, complete }),
			SqlMessage::Error(error) => return Err(error),
			other => panic!("unexpected SQL response frame: {other:?}"),
		}
	}
}

async fn execute_sql_authenticated(
	endpoint: SocketAddr,
	server_name: &str,
	cert: &Path,
	key: &Path,
	ca: &Path,
	statement: &str,
) -> Result<SqlResponse, SqlError> {
	let client_config = client_config_from_paths(cert, key, ca).expect("client TLS config");
	let client = QuicEndpoint::client_authenticated(client_config).expect("authenticated client");
	let mut stream = client
		.connect_stream_with_server_name(endpoint, server_name)
		.await
		.expect("connect authenticated sql");
	let query_id = QueryId::new();
	write_sql_frame(
		&mut stream,
		&SqlFrame::execute(query_id, statement.to_string(), None),
	)
	.await
	.expect("write sql request");
	stream.finish().await.expect("finish sql request");

	let mut batches = Vec::new();
	loop {
		let frame = read_sql_frame(&mut stream)
			.await
			.expect("read sql response");
		match frame.message {
			SqlMessage::ResultBatch(batch) => {
				let reader = StreamReader::try_new(batch.arrow_ipc_bytes.as_slice(), None)
					.expect("open Arrow IPC stream");
				for batch in reader {
					batches.push(batch.expect("read Arrow IPC batch"));
				}
			}
			SqlMessage::Complete(complete) => return Ok(SqlResponse { batches, complete }),
			SqlMessage::Error(error) => return Err(error),
			other => panic!("unexpected SQL response frame: {other:?}"),
		}
	}
}

fn projected_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<i32>, Option<String>)> {
	let mut rows = Vec::new();
	for batch in batches {
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("id column");
		let amounts = batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("amount column");
		let regions = batch
			.column(2)
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("region column");
		for row in 0..batch.num_rows() {
			rows.push((
				ids.value(row),
				(!amounts.is_null(row)).then(|| amounts.value(row)),
				(!regions.is_null(row)).then(|| regions.value(row).to_string()),
			));
		}
	}
	rows
}

struct TestPki {
	ca_cert: PathBuf,
	daemon_cert: PathBuf,
	daemon_key: PathBuf,
	client_cert: PathBuf,
	client_key: PathBuf,
}

impl TestPki {
	fn generate(worker_id: WorkerId, principal: &str) -> Self {
		let dir = fresh_dir("tls-sql");
		let ca = ca_cert();
		let daemon_dns = canonical_daemon_dns_name(&worker_id, DEFAULT_CLUSTER_DOMAIN);
		let daemon = leaf_cert(
			&daemon_dns,
			&[
				ExtendedKeyUsagePurpose::ServerAuth,
				ExtendedKeyUsagePurpose::ClientAuth,
			],
		);
		let client_dns = canonical_client_dns_name(principal, DEFAULT_CLUSTER_DOMAIN);
		let client = leaf_cert(&client_dns, &[ExtendedKeyUsagePurpose::ClientAuth]);
		let pki = Self {
			ca_cert: dir.join("ca.pem"),
			daemon_cert: dir.join("daemon.pem"),
			daemon_key: dir.join("daemon.key"),
			client_cert: dir.join("client.pem"),
			client_key: dir.join("client.key"),
		};
		std::fs::write(&pki.ca_cert, ca.serialize_pem().expect("CA PEM")).expect("write CA");
		write_signed_cert(&pki.daemon_cert, &pki.daemon_key, &daemon, &ca);
		write_signed_cert(&pki.client_cert, &pki.client_key, &client, &ca);
		pki
	}
}

fn toron_config_with_descriptor_tls(
	descriptor: TableSchemaDescriptor,
	worker_id: WorkerId,
	pki: &TestPki,
) -> ToronBootstrapConfig {
	let mut config = toron_config_with_descriptor(descriptor, true);
	config.node.worker_id = worker_id;
	config.sql.as_mut().expect("sql config").listener_bind = "0.0.0.0:0".parse().expect("sql bind");
	config.tls = Some(TlsConfig {
		listener_cert: pki.daemon_cert.clone(),
		listener_key: pki.daemon_key.clone(),
		peer_ca: pki.ca_cert.clone(),
		cluster_domain: DEFAULT_CLUSTER_DOMAIN.to_string(),
	});
	config
}

fn toron_config_with_descriptor(
	descriptor: TableSchemaDescriptor,
	include_self_worker: bool,
) -> ToronBootstrapConfig {
	let descriptor_path = write_descriptor_bootstrap(descriptor);
	ToronBootstrapConfig {
		node: ToronNodeConfig {
			worker_id: WorkerId::new(),
			tenant_id: ToronTenantId(1),
			drain_timeout: Duration::from_secs(1),
		},
		worker: ToronWorkerConfig {
			task_listener_bind: "127.0.0.1:0".parse().expect("task bind"),
			advertised_endpoint: None,
			memory_budget_bytes: 64 * 1024 * 1024,
		},
		storage: ToronStorageConfig {
			wendelon: ToronWendelonStorageConfig {
				bootstrap_peers: vec![ToronWendelonBootstrapPeer {
					node_id: 1,
					address: "127.0.0.1:7001".to_string(),
				}],
				client_bind: "127.0.0.1:0".parse().expect("client bind"),
				tls: None,
				cluster_domain: wendelon::network::tls::DEFAULT_CLUSTER_DOMAIN.to_string(),
				pool_max_retained: 64,
				pool_max_cap_bytes: 8 * 1024 * 1024,
			},
		},
		catalog: ToronCatalogConfig {
			descriptor_bootstrap_path: Some(descriptor_path),
		},
		sql: Some(SqlConfig {
			transport: SqlTransport::Quic,
			listener_bind: "127.0.0.1:0".parse().expect("sql bind"),
			statement_timeout: Duration::from_secs(3),
		}),
		coordinator: CoordinatorConfig {
			include_self_worker,
			static_workers: Vec::new(),
		},
		tls: None,
		observability: ToronObservabilityConfig {
			level: "info".to_string(),
			format: ToronTracingFormat::Pretty,
		},
	}
}

fn ca_cert() -> Certificate {
	let mut params = CertificateParams::new(Vec::new());
	params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
	params.key_usages = vec![
		KeyUsagePurpose::DigitalSignature,
		KeyUsagePurpose::KeyCertSign,
	];
	params
		.distinguished_name
		.push(DnType::CommonName, "toron-sql-listener-test-ca");
	Certificate::from_params(params).expect("CA cert")
}

fn leaf_cert(dns_name: &str, extended_key_usages: &[ExtendedKeyUsagePurpose]) -> Certificate {
	let mut params = CertificateParams::new(vec![dns_name.to_string()]);
	params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
	params.extended_key_usages = extended_key_usages.to_vec();
	params.distinguished_name.push(DnType::CommonName, dns_name);
	Certificate::from_params(params).expect("leaf cert")
}

fn write_signed_cert(cert_path: &Path, key_path: &Path, cert: &Certificate, ca: &Certificate) {
	std::fs::write(
		cert_path,
		cert.serialize_pem_with_signer(ca).expect("signed cert PEM"),
	)
	.expect("write cert");
	std::fs::write(key_path, cert.serialize_private_key_pem()).expect("write key");
}

fn connectable_loopback_addr(addr: SocketAddr) -> SocketAddr {
	SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
}

fn write_descriptor_bootstrap(descriptor: TableSchemaDescriptor) -> PathBuf {
	let dir = fresh_dir("descriptor-bootstrap");
	let path = dir.join("descriptors.json");
	let bootstrap = ToronDescriptorBootstrap {
		tables: vec![ToronDescriptorBootstrapTable {
			table_id: 1,
			descriptor,
		}],
	};
	std::fs::write(&path, serde_json::to_string_pretty(&bootstrap).unwrap())
		.expect("write descriptor bootstrap");
	path
}

struct LiveWendelonFixture {
	node: WendelonNode,
	client: Arc<Client>,
	data_dir: PathBuf,
}

impl LiveWendelonFixture {
	async fn start(node_id: u32) -> Self {
		let data_dir = fresh_dir(&format!("wendelon-live-{node_id}"));
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

	fn shutdown(self) {
		self.node.handles.abort();
		let _ = std::fs::remove_dir_all(self.data_dir);
	}
}

async fn create_live_tenant(node: &WendelonNode, tenant: WendelonTenantId) {
	node.create_tenant(tenant, u64::MAX, 1)
		.await
		.expect("create live Toron SQL test tenant");
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
		(
			3,
			None,
			Some("north"),
			BASE_TS + 60 * 60 * 1000 + 15 * 60 * 1000,
		),
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

fn fresh_dir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"toron-sql-listener-{tag}-{}-{}",
		std::process::id(),
		rand::random::<u64>()
	));
	std::fs::create_dir_all(&dir).expect("create temp dir");
	dir
}
