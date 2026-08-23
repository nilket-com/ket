#[path = "support/tls_pki.rs"]
mod tls_pki;

use arrow::array::LargeBinaryBuilder;
#[cfg(feature = "test-support")]
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use uuid::Uuid;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, TopologySource, WendelonClient, WendelonClientConfig,
	WriteRequest,
};
#[cfg(feature = "test-support")]
use wendelon::consensus::WendelonRequest;
#[cfg(feature = "test-support")]
use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, DropOperationId, SchemaGeneration, SchemaIntentId,
	SchemaIntentOrigin, SchemaMutationKind, SchemaObjectKind, SchemaObjectRef,
	SchemaUpdateEnvelope, TableSchemaDropFields, TableSchemaDropRequest,
	encode_schema_update_envelope_v1,
};
use wendelon::network::core::{HRFT_MAGIC, HrftFrame};
use wendelon::network::pool::PooledBuf;
use wendelon::network::ring::{RingRouter, RingState};
use wendelon::network::server::{HrftService, RequestContext};
use wendelon::network::{
	ActorRole, BufPool, EpochCapabilityResolver, MembershipEpoch, NodeCapabilitySet, NodeRole,
	NodeRoleSet, PeerCapabilities, QuicTransportProvider, ServerAdmissionConfig, ServerLifecycle,
	TcpTlsTransportProvider, TlsMaterial, TransportConnection, TransportProvider,
	TransportProviderFactory, TransportTier, TransportTierSet, UnroutedService,
	canonical_node_dns_name, canonical_server_name_resolver, client_config, endpoint_from_tls,
	read_frame, rustls_client_config, serve_with_admission, server_config, verified_transport_peer,
	write_frame,
};
use wendelon::node::{
	ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, TransportTlsConfig, WendelonNode,
};
use wendelon::schema::{
	ClientWriteId, DurabilityLevel, FrameHeader, HybridTimestamp, Identity, Locality, MsgType,
	NodeId, TenantId, WriteRequestMeta, WriteResponseMeta, WriteStatus, write_payload_schema,
};
#[cfg(feature = "test-support")]
use wendelon::schema::{
	PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor, encode_table_schema_descriptor_v1,
};

fn fresh_dir(tag: &str) -> std::path::PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-quic-transport-{tag}-{}-{}",
		std::process::id(),
		Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

fn reserve_socket_addr() -> String {
	let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
	let addr = listener.local_addr().unwrap();
	drop(listener);
	addr.to_string()
}

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn tier_bits() -> u8 {
	TransportTierSet::empty()
		.with(TransportTier::QuicIoUring)
		.as_u8()
}

fn role_bits() -> u8 {
	NodeRoleSet::empty()
		.with(NodeRole::Data)
		.with(NodeRole::Control)
		.as_u8()
}

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

fn snapshot(
	epoch: u64,
	leader: Option<u32>,
	replication_factor: u32,
	peers: &[(u32, &str)],
) -> ClusterStatusWire {
	ClusterStatusWire {
		epoch,
		leader,
		replication_factor,
		peers: peers
			.iter()
			.map(|(node_id, address)| ClusterPeerWire {
				node_id: *node_id,
				address: (*address).to_string(),
				tier_bits: tier_bits(),
				role_bits: role_bits(),
				capability_bits: 0,
			})
			.collect(),
	}
}

fn write_meta(epoch: u64, key: &[u8], value: &[u8], sequence_no: u64) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [7u8; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(5_000 + sequence_no, 0, 42),
		expected_epoch: epoch,
		tenant_id: tenant(),
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: node(0),
		key_identity: Identity::blake3_tenant(tenant(), key),
		key_len: key.len() as u32,
		value_len: value.len() as u32,
	}
}

fn kv_batch(key: &[u8], value: &[u8]) -> RecordBatch {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	let mut values = LargeBinaryBuilder::new();
	values.append_value(value);
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap()
}

fn find_key_for_replica(snapshot: &ClusterStatusWire, target: NodeId) -> Vec<u8> {
	let ring = RingRouter::new(RingState::build(
		&snapshot.to_membership(),
		snapshot.replication_factor as usize,
	));
	for i in 0..100_000u64 {
		let candidate = format!("quic-retry-key-{i}");
		let locality = Locality::from(Identity::blake3_tenant(tenant(), candidate.as_bytes()));
		if ring
			.replicas_for_locality(locality)
			.into_iter()
			.any(|replica| replica == target)
		{
			return candidate.into_bytes();
		}
	}
	panic!("failed to find key routed to replica {}", target.id);
}

struct CountingTopologySource {
	snapshot: Mutex<ClusterStatusWire>,
	fetches: AtomicUsize,
}

impl CountingTopologySource {
	fn new(snapshot: ClusterStatusWire) -> Self {
		Self {
			snapshot: Mutex::new(snapshot),
			fetches: AtomicUsize::new(0),
		}
	}

	fn fetches(&self) -> usize {
		self.fetches.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl TopologySource for CountingTopologySource {
	async fn fetch(&self) -> std::io::Result<ClusterStatusWire> {
		self.fetches.fetch_add(1, Ordering::SeqCst);
		Ok(self.snapshot.lock().unwrap().clone())
	}
}

struct WriteResponseService {
	status: WriteStatus,
	leader_hint: NodeId,
	epoch: u64,
	commit_hlc: HybridTimestamp,
	calls: AtomicUsize,
}

impl WriteResponseService {
	fn new(
		status: WriteStatus,
		leader_hint: NodeId,
		epoch: u64,
		commit_hlc: HybridTimestamp,
	) -> Self {
		Self {
			status,
			leader_hint,
			epoch,
			commit_hlc,
			calls: AtomicUsize::new(0),
		}
	}

	fn calls(&self) -> usize {
		self.calls.load(Ordering::SeqCst)
	}
}

#[async_trait]
impl HrftService for WriteResponseService {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		match MsgType(frame.header.msg_type) {
			MsgType::WRITE_REQ => {
				let req: &WriteRequestMeta =
					bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<WriteRequestMeta>()]);
				let resp = WriteResponseMeta {
					status: self.status,
					_pad1: [0; 7],
					log_index: 1,
					term: 1,
					epoch: self.epoch.max(req.expected_epoch),
					commit_hlc: self.commit_hlc,
					leader_hint: self.leader_hint,
					echoed_write_id: req.write_id,
					has_finalization: 0,
					_pad2: [0; 7],
				};
				Ok(Some(HrftFrame {
					header: FrameHeader {
						magic: HRFT_MAGIC,
						version: 1,
						msg_type: MsgType::WRITE_RESP.0,
						flags: 0,
						stream_id: frame.header.stream_id,
						meta_len: std::mem::size_of::<WriteResponseMeta>() as u32,
						arrow_len: 0,
						trailing_len: 0,
					},
					meta: PooledBuf::orphan(bytemuck::bytes_of(&resp).to_vec()),
					arrow_payload: None,
					trailing: None,
				}))
			}
			other => Err(Error::new(
				ErrorKind::Unsupported,
				format!(
					"write response service does not handle msg_type=0x{:02x}",
					other.0
				),
			)),
		}
	}
}

struct RefusingProvider;

#[async_trait]
impl TransportProvider for RefusingProvider {
	async fn connect(
		&self,
		target: NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		Err(Error::new(
			ErrorKind::ConnectionRefused,
			format!("forced QUIC failure for node {} at {address}", target.id),
		))
	}
}

#[allow(clippy::too_many_arguments)]
fn bootstrap_cfg(
	tag: &str,
	node_id: u32,
	advertise_address: String,
	rf: usize,
	trusted_nodes: &[u32],
	cluster: ClusterBootstrapConfig,
	cert_path: &Path,
	key_path: &Path,
	node_ca_path: &Path,
	operator_ca_path: &Path,
) -> NodeBootstrapConfig {
	let lease_signing_secret = wendelon::network::dev_lease_signing_secret(node_id);
	let transfer_signing_secret = wendelon::network::dev_transfer_signing_secret(node_id);
	let mut trusted_lease_signers = BTreeMap::new();
	let mut trusted_transfer_signers = BTreeMap::new();
	for trusted_node in trusted_nodes {
		let lease_secret = wendelon::network::dev_lease_signing_secret(*trusted_node);
		let transfer_secret = wendelon::network::dev_transfer_signing_secret(*trusted_node);
		trusted_lease_signers.insert(
			*trusted_node,
			wendelon::network::lease_verifying_key_bytes(lease_secret),
		);
		trusted_transfer_signers.insert(
			*trusted_node,
			wendelon::network::transfer_verifying_key_bytes(transfer_secret),
		);
	}
	let mut cfg = NodeBootstrapConfig::dev(node_id, fresh_dir(tag), advertise_address);
	cfg.rf = rf;
	cfg.lease_signing_secret = lease_signing_secret;
	cfg.trusted_lease_signers = trusted_lease_signers;
	cfg.transfer_signing_secret = transfer_signing_secret;
	cfg.trusted_transfer_signers = trusted_transfer_signers;
	cfg.cluster = cluster;
	cfg.transport_tls = Some(TransportTlsConfig {
		cert_path: cert_path.to_path_buf(),
		key_path: key_path.to_path_buf(),
		node_ca_path: node_ca_path.to_path_buf(),
		operator_ca_path: operator_ca_path.to_path_buf(),
		cluster_domain: tls_pki::CLUSTER_DOMAIN.into(),
	});
	cfg
}

async fn wait_for_cluster(node: &WendelonNode, expected_epoch: u64, expected_nodes: usize) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		let snapshot = node.ring.snapshot();
		if node.ring.current_epoch() >= expected_epoch
			&& snapshot.data_capable_nodes() == expected_nodes
		{
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for epoch >= {expected_epoch} and {expected_nodes} nodes; got epoch={} nodes={}",
			node.ring.current_epoch(),
			snapshot.data_capable_nodes()
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn wait_for_voters(node: &WendelonNode, expected: &[u32]) {
	let expected: std::collections::BTreeSet<u32> = expected.iter().copied().collect();
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		let voters: std::collections::BTreeSet<u32> =
			snapshot.last_membership.voter_ids().collect();
		if voters == expected {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for voters {:?}; got {:?}",
			expected,
			voters
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn adr0093_connect_and_idle(
	keep_alive: Option<std::time::Duration>,
	silence: std::time::Duration,
) -> Result<(), quinn::ConnectionError> {
	let root = fresh_dir("adr0093-compressed-liveness");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let client_material =
		TlsMaterial::load(&pki.data_client_cert, &pki.data_client_key, &pki.ca_cert).unwrap();
	let idle = std::time::Duration::from_millis(300);
	let mut transport = quinn::TransportConfig::default();
	transport.max_idle_timeout(Some(idle.try_into().expect("representable idle timeout")));
	transport.keep_alive_interval(keep_alive);
	let transport = Arc::new(transport);

	let mut server_cfg = server_config(&server_material).unwrap();
	server_cfg.transport_config(Arc::clone(&transport));
	let server = quinn::Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
	let address = server.local_addr().unwrap();
	let mut client_cfg = client_config(&client_material).unwrap();
	client_cfg.transport_config(transport);
	let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
	client.set_default_client_config(client_cfg);

	let accepting = tokio::spawn(async move {
		let connection = server
			.accept()
			.await
			.expect("incoming")
			.await
			.expect("accept");
		tokio::time::sleep(silence).await;
		connection
	});
	let connection = client
		.connect(
			address,
			&canonical_node_dns_name(1, tls_pki::CLUSTER_DOMAIN),
		)
		.expect("connect")
		.await
		.expect("handshake");
	tokio::time::sleep(silence).await;
	let opened = connection.open_bi().await;
	let _ = accepting.await;
	opened.map(|_| ())
}

#[tokio::test]
async fn adr0093_keep_alive_holds_a_silent_wendelon_connection_past_idle() {
	adr0093_connect_and_idle(
		Some(std::time::Duration::from_millis(50)),
		std::time::Duration::from_millis(900),
	)
	.await
	.expect("keep-alive must preserve a connection through three idle periods");
}

#[tokio::test]
async fn adr0093_without_keep_alive_the_same_wendelon_connection_times_out() {
	let error = adr0093_connect_and_idle(None, std::time::Duration::from_millis(900))
		.await
		.expect_err("the same silence without keep-alive must lose the connection");
	assert!(
		matches!(error, quinn::ConnectionError::TimedOut),
		"{error:?}"
	);
}

#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "evidence exhibit: holds a real Wendelon DROP beyond Quinn's former 30s idle timeout"]
async fn adr0093_live_quic_drop_survives_more_than_thirty_seconds_of_reclaim() {
	let root = fresh_dir("adr0093-live-drop");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let address = reserve_socket_addr();
	let node = WendelonNode::bootstrap(bootstrap_cfg(
		"adr0093-live-drop-node",
		1,
		address.clone(),
		1,
		&[1],
		ClusterBootstrapConfig::Bootstrap,
		&pki.server_cert,
		&pki.server_key,
		&pki.ca_cert,
		&pki.ops_ca_cert,
	))
	.await
	.expect("bootstrap live Wendelon node");
	wait_for_cluster(&node, 1, 1).await;
	let tenant_id = TenantId::new(93);
	node.create_tenant(tenant_id, u64::MAX, 1)
		.await
		.expect("create test tenant");

	let table_object_id = TableObjectId::new([0x93; 16]);
	let descriptor = TableSchemaDescriptor::new(
		table_object_id,
		1,
		"adr0093",
		"slow_drop",
		&Schema::new(vec![Field::new("id", DataType::Int64, false)]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		wendelon::schema::LocalityPolicy::IdentityProjection,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let envelope = SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id,
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: descriptor.namespace.clone(),
			name: descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(1),
		intent_id: SchemaIntentId::new([0x93; 16]),
		mutation: SchemaMutationKind::Create,
		payload: encode_table_schema_descriptor_v1(&descriptor).unwrap(),
	};
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			encode_schema_update_envelope_v1(&envelope).unwrap(),
		))
		.await
		.expect("install table descriptor");

	let client_material =
		TlsMaterial::load(&pki.operator_cert, &pki.operator_key, &pki.ca_cert).unwrap();
	let client_endpoint = endpoint_from_tls(&client_material, "127.0.0.1:0".parse().unwrap())
		.expect("build operator QUIC endpoint");
	let provider: Arc<dyn TransportProvider> =
		Arc::new(QuicTransportProvider::with_cluster_domain(
			Arc::new(client_endpoint.clone()),
			canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
			tls_pki::CLUSTER_DOMAIN,
		));
	let topology = snapshot(1, Some(1), 1, &[(1, &address)]);
	let inner = WendelonClient::new(WendelonClientConfig {
		local_tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
		providers: HashMap::from([(TransportTier::QuicIoUring, provider)]),
		bootstrap: topology.clone(),
		topology: Arc::new(wendelon::client::StaticTopologySource::new(topology)),
		pool_max_retained: 8,
		pool_max_cap_bytes: 1024 * 1024,
	});
	let client = wendelon_client::Client::with_client_uuid(inner, [0x93; 16]);
	let operation_id = DropOperationId {
		tenant_id,
		table_object_id,
		schema_generation: SchemaGeneration::new(1),
	};
	let _delay = wendelon::network::adr0093_test_support::delay_next_drop_reclaim(
		table_object_id,
		std::time::Duration::from_secs(31),
	);
	let started = std::time::Instant::now();
	client
		.drop_table_schema(TableSchemaDropRequest::current(TableSchemaDropFields {
			tenant_id,
			table_object_id,
			expected_generation: SchemaGeneration::new(1),
			operation_id,
		}))
		.await
		.expect("DROP must survive the old 30-second idle boundary");
	let elapsed = started.elapsed();
	assert!(
		elapsed > std::time::Duration::from_secs(30),
		"the exhibit must genuinely cross the old idle boundary: {elapsed:?}"
	);
	assert_eq!(
		node.state_machine
			.state_snapshot()
			.await
			.table_drop_outcomes
			.get(tenant_id, table_object_id)
			.map(|outcome| outcome.operation_id),
		Some(operation_id)
	);

	client_endpoint.close(0u32.into(), b"done");
	node.shutdown().await.unwrap();
}

#[tokio::test]
async fn quic_provider_dials_and_verifies_peer_identity() {
	let root = fresh_dir("provider");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let client_material =
		TlsMaterial::load(&pki.data_client_cert, &pki.data_client_key, &pki.ca_cert).unwrap();
	let server_endpoint =
		endpoint_from_tls(&server_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let client_endpoint =
		endpoint_from_tls(&client_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let server_addr = server_endpoint.local_addr().unwrap();

	let server = tokio::spawn({
		let server_endpoint = server_endpoint.clone();
		async move {
			let incoming = server_endpoint
				.accept()
				.await
				.expect("incoming QUIC connect");
			let connection = incoming.await.unwrap();
			let peer = verified_transport_peer(&connection, tls_pki::CLUSTER_DOMAIN).unwrap();
			let (_send, mut recv) = connection.accept_bi().await.unwrap();
			let mut body = Vec::new();
			AsyncReadExt::read_to_end(&mut recv, &mut body)
				.await
				.unwrap();
			(peer.node_id.id, body)
		}
	});

	let provider = QuicTransportProvider::with_cluster_domain(
		Arc::new(client_endpoint.clone()),
		canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
		tls_pki::CLUSTER_DOMAIN,
	);
	let target = NodeId {
		id: 1,
		_reserved: 0,
	};
	let connection = provider
		.connect(target, &server_addr.to_string())
		.await
		.unwrap();
	assert_eq!(connection.remote_id(), Some(target));
	assert_eq!(connection.transport_type(), TransportTier::QuicIoUring);
	assert_eq!(connection.actor_identity().role, ActorRole::DataNode);
	assert_eq!(
		connection.actor_identity().node_id.map(|node| node.id),
		Some(1)
	);

	let mut stream = connection.open_bi().await.unwrap();
	stream.write_all(b"ping").await.unwrap();
	stream.finish().await.unwrap();

	let (peer_id, body) = server.await.unwrap();
	assert_eq!(peer_id, 2);
	assert_eq!(body, b"ping");

	client_endpoint.close(0u32.into(), b"done");
	server_endpoint.close(0u32.into(), b"done");
	client_endpoint.wait_idle().await;
	server_endpoint.wait_idle().await;
}

#[tokio::test]
async fn quic_accept_loop_refuses_connections_over_limit() {
	let root = fresh_dir("accept-loop-connection-limit");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let client_material =
		TlsMaterial::load(&pki.data_client_cert, &pki.data_client_key, &pki.ca_cert).unwrap();
	let server_endpoint =
		endpoint_from_tls(&server_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let client_endpoint =
		endpoint_from_tls(&client_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let server_addr = server_endpoint.local_addr().unwrap();
	let lifecycle = ServerLifecycle::new();
	let server = tokio::spawn({
		let server_endpoint = server_endpoint.clone();
		let lifecycle = lifecycle.clone();
		async move {
			serve_with_admission(
				server_endpoint,
				Arc::new(UnroutedService),
				BufPool::new(8, 1024),
				lifecycle,
				ServerAdmissionConfig {
					max_inbound_connections: 1,
					max_inbound_connections_per_ip: 4,
					max_inbound_streams_per_connection: 8,
					max_concurrent_stream_tasks: 16,
				},
				tls_pki::CLUSTER_DOMAIN,
				None,
				None,
			)
			.await
			.unwrap();
		}
	});

	let server_name = canonical_node_dns_name(1, tls_pki::CLUSTER_DOMAIN);
	let first = client_endpoint
		.connect(server_addr, &server_name)
		.unwrap()
		.await
		.unwrap();
	let second = client_endpoint
		.connect(server_addr, &server_name)
		.unwrap()
		.await;
	assert!(
		second.is_err(),
		"second connection should be refused once the listener-level connection cap is full"
	);

	first.close(0u32.into(), b"done");
	lifecycle.begin_drain();
	client_endpoint.close(0u32.into(), b"done");
	server_endpoint.close(0u32.into(), b"done");
	server.await.unwrap();
	client_endpoint.wait_idle().await;
	server_endpoint.wait_idle().await;
}

#[tokio::test]
async fn listener_overload_sheds_excess_handshakes_while_retry_redirect_succeeds() {
	let root = fresh_dir("listener-overload-retry");
	let pki = tls_pki::write_test_pki(&root.join("pki"));

	let node1_server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let node2_server_material =
		TlsMaterial::load(&pki.data_client_cert, &pki.data_client_key, &pki.ca_cert).unwrap();
	let client_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();

	let node1_endpoint =
		endpoint_from_tls(&node1_server_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let node2_endpoint =
		endpoint_from_tls(&node2_server_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let client_endpoint =
		endpoint_from_tls(&client_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let raw_endpoint = endpoint_from_tls(&client_material, "127.0.0.1:0".parse().unwrap()).unwrap();

	let node1_addr = node1_endpoint.local_addr().unwrap().to_string();
	let node2_addr = node2_endpoint.local_addr().unwrap().to_string();

	let lifecycle1 = ServerLifecycle::new();
	let lifecycle2 = ServerLifecycle::new();
	let leader_service = Arc::new(WriteResponseService::new(
		WriteStatus::OK,
		node(1),
		11,
		hlc(11_000, 0, 1),
	));
	let redirect_service = Arc::new(WriteResponseService::new(
		WriteStatus::NOT_LEADER,
		node(1),
		10,
		hlc(10_000, 0, 2),
	));

	let server1 = tokio::spawn({
		let endpoint = node1_endpoint.clone();
		let lifecycle = lifecycle1.clone();
		let service = leader_service.clone();
		async move {
			serve_with_admission(
				endpoint,
				service,
				BufPool::new(8, 1024),
				lifecycle,
				ServerAdmissionConfig {
					max_inbound_connections: 8,
					max_inbound_connections_per_ip: 8,
					max_inbound_streams_per_connection: 8,
					max_concurrent_stream_tasks: 16,
				},
				tls_pki::CLUSTER_DOMAIN,
				None,
				None,
			)
			.await
			.unwrap();
		}
	});
	let server2 = tokio::spawn({
		let endpoint = node2_endpoint.clone();
		let lifecycle = lifecycle2.clone();
		let service = redirect_service.clone();
		async move {
			serve_with_admission(
				endpoint,
				service,
				BufPool::new(8, 1024),
				lifecycle,
				ServerAdmissionConfig {
					max_inbound_connections: 1,
					max_inbound_connections_per_ip: 4,
					max_inbound_streams_per_connection: 8,
					max_concurrent_stream_tasks: 16,
				},
				tls_pki::CLUSTER_DOMAIN,
				None,
				None,
			)
			.await
			.unwrap();
		}
	});

	let stale_snapshot = snapshot(10, Some(2), 2, &[(1, &node1_addr), (2, &node2_addr)]);
	let fresh_snapshot = snapshot(11, Some(1), 2, &[(1, &node1_addr), (2, &node2_addr)]);
	let topology = Arc::new(CountingTopologySource::new(fresh_snapshot));
	let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
	providers.insert(
		TransportTier::QuicIoUring,
		Arc::new(QuicTransportProvider::with_cluster_domain(
			Arc::new(client_endpoint.clone()),
			canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
			tls_pki::CLUSTER_DOMAIN,
		)),
	);
	let client = WendelonClient::new(WendelonClientConfig {
		local_tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
		providers,
		bootstrap: stale_snapshot.clone(),
		topology: topology.clone(),
		pool_max_retained: 64,
		pool_max_cap_bytes: 1024 * 1024,
	});

	let key = find_key_for_replica(&stale_snapshot, node(2));
	let value = b"retry-survives-listener-overload";
	let write = client
		.write(WriteRequest {
			meta: write_meta(10, &key, value, 1),
			payload: kv_batch(&key, value),
		})
		.await
		.unwrap();
	assert_eq!(write.response.status, WriteStatus::OK);
	assert_eq!(redirect_service.calls(), 1);
	assert_eq!(leader_service.calls(), 1);
	assert_eq!(topology.fetches(), 1);
	assert_eq!(client.leader(), Some(node(1)));

	tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	let overload_err = raw_endpoint
		.connect(
			node2_addr.parse().unwrap(),
			&canonical_node_dns_name(2, tls_pki::CLUSTER_DOMAIN),
		)
		.unwrap()
		.await;
	assert!(
		overload_err.is_err(),
		"excess handshake should be refused while retry traffic still holds the listener slot"
	);

	raw_endpoint.close(0u32.into(), b"done");
	client_endpoint.close(0u32.into(), b"done");
	lifecycle1.begin_drain();
	lifecycle2.begin_drain();
	node1_endpoint.close(0u32.into(), b"done");
	node2_endpoint.close(0u32.into(), b"done");
	server1.await.unwrap();
	server2.await.unwrap();
	raw_endpoint.wait_idle().await;
	client_endpoint.wait_idle().await;
	node1_endpoint.wait_idle().await;
	node2_endpoint.wait_idle().await;
}

#[tokio::test]
async fn transport_tls_bootstrap_uses_real_quic_join_path() {
	let root = fresh_dir("bootstrap");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let trusted_nodes = [1u32, 2];
	let node1_addr = reserve_socket_addr();
	let node2_addr = reserve_socket_addr();

	let node1 = WendelonNode::bootstrap(bootstrap_cfg(
		"node1",
		1,
		node1_addr.clone(),
		2,
		&trusted_nodes,
		ClusterBootstrapConfig::Bootstrap,
		&pki.server_cert,
		&pki.server_key,
		&pki.ca_cert,
		&pki.ops_ca_cert,
	))
	.await
	.unwrap();
	let node2 = WendelonNode::bootstrap(bootstrap_cfg(
		"node2",
		2,
		node2_addr.clone(),
		2,
		&trusted_nodes,
		ClusterBootstrapConfig::Join {
			seeds: vec![SeedNodeConfig {
				node_id: 1,
				address: node1_addr.clone(),
			}],
		},
		&pki.data_client_cert,
		&pki.data_client_key,
		&pki.ca_cert,
		&pki.ops_ca_cert,
	))
	.await
	.unwrap();

	let node1_epoch = node1.epoch_tx.borrow().clone();
	let node1_caps = node1_epoch.peers.get(&node1.local_node).unwrap();
	assert!(node1_caps.tiers.contains(TransportTier::QuicIoUring));
	assert!(node1_caps.tiers.contains(TransportTier::TcpTls));
	wait_for_cluster(&node1, 2, 2).await;
	wait_for_cluster(&node2, 2, 2).await;
	wait_for_voters(&node1, &[1, 2]).await;

	node2.shutdown().await.unwrap();
	node1.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_factory_falls_back_to_tcp_tls_against_dual_stack_node() {
	let root = fresh_dir("factory-fallback");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let trusted_nodes = [1u32];
	let node_addr = reserve_socket_addr();
	let node = WendelonNode::bootstrap(bootstrap_cfg(
		"fallback-node",
		1,
		node_addr.clone(),
		1,
		&trusted_nodes,
		ClusterBootstrapConfig::Bootstrap,
		&pki.server_cert,
		&pki.server_key,
		&pki.ca_cert,
		&pki.ops_ca_cert,
	))
	.await
	.unwrap();

	let local_epoch = node.epoch_tx.borrow().clone();
	let local_caps = local_epoch.peers.get(&node.local_node).unwrap();
	assert!(local_caps.tiers.contains(TransportTier::QuicIoUring));
	assert!(local_caps.tiers.contains(TransportTier::TcpTls));

	let client_material =
		TlsMaterial::load(&pki.data_client_cert, &pki.data_client_key, &pki.ca_cert).unwrap();
	let mut providers: std::collections::HashMap<TransportTier, Arc<dyn TransportProvider>> =
		std::collections::HashMap::new();
	providers.insert(TransportTier::QuicIoUring, Arc::new(RefusingProvider));
	providers.insert(
		TransportTier::TcpTls,
		Arc::new(TcpTlsTransportProvider::with_cluster_domain(
			rustls_client_config(&client_material).unwrap(),
			canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
			tls_pki::CLUSTER_DOMAIN,
		)),
	);
	let target = NodeId {
		id: 1,
		_reserved: 0,
	};
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		target,
		PeerCapabilities {
			address: node_addr.clone(),
			tiers: TransportTierSet::empty()
				.with(TransportTier::QuicIoUring)
				.with(TransportTier::TcpTls),
			roles: NodeRoleSet::empty()
				.with(NodeRole::Control)
				.with(NodeRole::Data),
			capabilities: NodeCapabilitySet::empty(),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_tx, rx) = watch::channel(Arc::new(epoch));
	let resolver = Arc::new(EpochCapabilityResolver::new(rx.clone()));
	let factory = TransportProviderFactory::new(
		TransportTierSet::empty()
			.with(TransportTier::QuicIoUring)
			.with(TransportTier::TcpTls),
		providers,
		resolver,
		rx,
	);
	let dialer: Arc<dyn TransportProvider> = factory.clone();
	let connection = dialer.connect(target, &node_addr).await.unwrap();
	assert_eq!(connection.transport_type(), TransportTier::TcpTls);
	assert_eq!(connection.remote_id(), Some(target));

	let pool = BufPool::new(8, 64 * 1024);
	let mut stream = connection.open_bi().await.unwrap();
	let request = FrameHeader {
		magic: wendelon::network::HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::STATUS_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: 0,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &request, &[], None, None)
		.await
		.unwrap();
	stream.finish().await.unwrap();
	let response = read_frame(&mut *stream, &pool).await.unwrap();
	assert_eq!(response.header.msg_type, MsgType::STATUS_RESP.0);

	node.shutdown().await.unwrap();
}

async fn send_empty_request(
	connection: &Arc<dyn TransportConnection>,
	msg_type: MsgType,
) -> std::io::Result<HrftFrame> {
	let pool = BufPool::new(8, 64 * 1024);
	let mut stream = connection.open_bi().await?;
	let request = FrameHeader {
		magic: wendelon::network::HRFT_MAGIC,
		version: 1,
		msg_type: msg_type.0,
		flags: 0,
		stream_id: 0,
		meta_len: 0,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &request, &[], None, None).await?;
	stream.finish().await?;
	read_frame(&mut *stream, &pool).await
}

#[tokio::test]
async fn operator_reaches_only_control_ring_over_both_production_transports() {
	let root = fresh_dir("operator-shared-ingress");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let node_addr = reserve_socket_addr();
	let deployment = WendelonNode::bootstrap(bootstrap_cfg(
		"operator-shared-ingress-node",
		1,
		node_addr.clone(),
		1,
		&[1],
		ClusterBootstrapConfig::Bootstrap,
		&pki.server_cert,
		&pki.server_key,
		&pki.ca_cert,
		&pki.ops_ca_cert,
	))
	.await
	.unwrap();

	let operator_material =
		TlsMaterial::load(&pki.operator_cert, &pki.operator_key, &pki.ca_cert).unwrap();
	let operator_quic_endpoint =
		Arc::new(endpoint_from_tls(&operator_material, "127.0.0.1:0".parse().unwrap()).unwrap());
	let quic_provider: Arc<dyn TransportProvider> =
		Arc::new(QuicTransportProvider::with_cluster_domain(
			operator_quic_endpoint.clone(),
			canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
			tls_pki::CLUSTER_DOMAIN,
		));
	let tcp_provider: Arc<dyn TransportProvider> =
		Arc::new(TcpTlsTransportProvider::with_cluster_domain(
			rustls_client_config(&operator_material).unwrap(),
			canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
			tls_pki::CLUSTER_DOMAIN,
		));
	let target = node(1);

	for (name, provider) in [("quic", quic_provider), ("tcp_tls", tcp_provider)] {
		let connection = provider.connect(target, &node_addr).await.unwrap();
		let status = send_empty_request(&connection, MsgType::STATUS_REQ)
			.await
			.unwrap_or_else(|error| panic!("{name} operator status failed: {error}"));
		assert_eq!(status.header.msg_type, MsgType::STATUS_RESP.0, "{name}");

		for denied in [
			MsgType::READ_REQ,
			MsgType::WRITE_REQ,
			MsgType::APPEND_ENTRIES_REQ,
		] {
			assert!(
				send_empty_request(&connection, denied).await.is_err(),
				"{name} admitted operator msg_type=0x{:02x} outside Control Ring",
				denied.0
			);
		}
	}

	operator_quic_endpoint.close(0u32.into(), b"done");
	deployment.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_listener_rejects_cross_domain_roles_on_both_transports() {
	let root = fresh_dir("cross-domain-shared-ingress");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let node_addr = reserve_socket_addr();
	let deployment = WendelonNode::bootstrap(bootstrap_cfg(
		"cross-domain-shared-ingress-node",
		1,
		node_addr.clone(),
		1,
		&[1],
		ClusterBootstrapConfig::Bootstrap,
		&pki.server_cert,
		&pki.server_key,
		&pki.ca_cert,
		&pki.ops_ca_cert,
	))
	.await
	.unwrap();
	let target = node(1);

	for (identity, cert, key) in [
		(
			"operator_on_node_ca",
			&pki.operator_on_node_ca_cert,
			&pki.operator_on_node_ca_key,
		),
		(
			"data_node_on_ops_ca",
			&pki.data_node_on_ops_ca_cert,
			&pki.data_node_on_ops_ca_key,
		),
	] {
		let material = TlsMaterial::load(cert, key, &pki.ca_cert).unwrap();
		let endpoint =
			Arc::new(endpoint_from_tls(&material, "127.0.0.1:0".parse().unwrap()).unwrap());
		let quic: Arc<dyn TransportProvider> =
			Arc::new(QuicTransportProvider::with_cluster_domain(
				endpoint.clone(),
				canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
				tls_pki::CLUSTER_DOMAIN,
			));
		let tcp: Arc<dyn TransportProvider> =
			Arc::new(TcpTlsTransportProvider::with_cluster_domain(
				rustls_client_config(&material).unwrap(),
				canonical_server_name_resolver(tls_pki::CLUSTER_DOMAIN),
				tls_pki::CLUSTER_DOMAIN,
			));

		for (transport, provider) in [("quic", quic), ("tcp_tls", tcp)] {
			let rejected = match provider.connect(target, &node_addr).await {
				Err(_) => true,
				Ok(connection) => send_empty_request(&connection, MsgType::STATUS_REQ)
					.await
					.is_err(),
			};
			assert!(
				rejected,
				"{transport} admitted cross-domain identity {identity}"
			);
		}
		endpoint.close(0u32.into(), b"done");
	}

	deployment.shutdown().await.unwrap();
}

#[tokio::test]
async fn transport_peer_verification_rejects_operator_client_over_real_quic() {
	let root = fresh_dir("operator-auth");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let server_material =
		TlsMaterial::load(&pki.server_cert, &pki.server_key, &pki.ca_cert).unwrap();
	let operator_material =
		TlsMaterial::load(&pki.operator_cert, &pki.operator_key, &pki.ca_cert).unwrap();
	let server_endpoint =
		endpoint_from_tls(&server_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let operator_endpoint =
		endpoint_from_tls(&operator_material, "127.0.0.1:0".parse().unwrap()).unwrap();
	let server_name = canonical_node_dns_name(1, tls_pki::CLUSTER_DOMAIN);
	let connecting = operator_endpoint
		.connect(server_endpoint.local_addr().unwrap(), &server_name)
		.unwrap();
	let incoming = server_endpoint
		.accept()
		.await
		.expect("incoming QUIC connect");
	let err = incoming.await.unwrap_err();
	assert!(
		err.to_string().contains("UnknownIssuer")
			|| err.to_string().contains("invalid peer certificate"),
		"unexpected error: {err}"
	);
	drop(connecting);

	operator_endpoint.close(0u32.into(), b"done");
	server_endpoint.close(0u32.into(), b"done");
	operator_endpoint.wait_idle().await;
	server_endpoint.wait_idle().await;
}
