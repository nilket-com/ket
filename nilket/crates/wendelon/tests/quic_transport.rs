#[path = "support/tls_pki.rs"]
mod tls_pki;

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use uuid::Uuid;
use wendelon::audit::AuditSinksConfig;
use wendelon::network::{
	ActorRole, BufPool, EpochCapabilityResolver, MembershipEpoch, NodeRole, NodeRoleSet,
	PeerCapabilities, QuicTransportProvider, ServerAdmissionConfig, ServerLifecycle,
	TcpTlsTransportProvider, TlsMaterial, TransportConnection, TransportProvider,
	TransportProviderFactory, TransportTier, TransportTierSet, UnroutedService,
	canonical_node_dns_name, canonical_server_name_resolver, endpoint_from_tls, read_frame,
	rustls_client_config, serve_with_admission, verified_transport_peer, write_frame,
};
use wendelon::node::{
	ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, TransportTlsConfig, WendelonNode,
};
use wendelon::schema::{FrameHeader, MsgType, NodeId};

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
	NodeBootstrapConfig {
		node_id,
		data_dir: fresh_dir(tag),
		slab_max_bytes: 64 * 1024 * 1024,
		group_commit: wendelon::storage::GroupCommitConfig::default(),
		rf,
		wal_io_depth: 64,
		slab_io_depth: 64,
		advertise_address,
		lease_signing_secret,
		trusted_lease_signers,
		transfer_signing_secret,
		trusted_transfer_signers,
		cluster,
		l2: None,
		tiering: wendelon::storage::SweeperConfig::default(),
		rehydration: wendelon::storage::RehydrationPolicy::default(),
		root_kek_provider: wendelon::crypto::RootKekProviderConfig::DevInsecure,
		crypto_cache: wendelon::crypto::CryptoCacheConfig::default(),
		protocol: wendelon::schema::ProtocolVersionWindow::default(),
		transport_tls: Some(TransportTlsConfig {
			cert_path: cert_path.to_path_buf(),
			key_path: key_path.to_path_buf(),
			node_ca_path: node_ca_path.to_path_buf(),
			operator_ca_path: operator_ca_path.to_path_buf(),
			cluster_domain: tls_pki::CLUSTER_DOMAIN.into(),
		}),
		transport_admission: wendelon::network::ServerAdmissionConfig::default(),
		drift_max_offset_ms: wendelon::consensus::MAX_OFFSET_MS,
		mvcc_retention_policy: wendelon::storage::MvccRetentionPolicy::Disabled,
		observability: None,
		audit: AuditSinksConfig::default(),
	}
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
	assert_eq!(connection.remote_id(), target);
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
	assert_eq!(connection.remote_id(), target);

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
