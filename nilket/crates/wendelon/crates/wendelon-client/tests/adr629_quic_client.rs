#[path = "../../../tests/support/tls_pki.rs"]
mod tls_pki;

use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
	time::Duration,
};
use wendelon::node::{
	ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, TransportTlsConfig, WendelonNode,
};
use wendelon::schema::{Locality, NodeId, TenantId};
use wendelon_client::{BootstrapPeer, Client, ClientConfig, ReadOptions, TlsPaths};

fn fresh_dir(tag: &str) -> PathBuf {
	let path = std::env::temp_dir().join(format!("adr629-p1-{tag}-{}", uuid::Uuid::new_v4()));
	std::fs::create_dir(&path).unwrap();
	path
}
fn reserve_socket_addr() -> String {
	let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
	socket.local_addr().unwrap().to_string()
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

#[tokio::test]
async fn adr629_p1_production_client_authenticates_a_discovered_data_route() {
	let root = fresh_dir("production-client");
	let pki = tls_pki::write_test_pki(&root.join("pki"));
	let a = reserve_socket_addr();
	let b = reserve_socket_addr();
	let node1 = WendelonNode::bootstrap(bootstrap_cfg(
		"node1",
		1,
		a.clone(),
		2,
		&[1, 2],
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
		b,
		2,
		&[1, 2],
		ClusterBootstrapConfig::Join {
			seeds: vec![SeedNodeConfig {
				node_id: 1,
				address: a.clone(),
			}],
		},
		&pki.data_client_cert,
		&pki.data_client_key,
		&pki.ca_cert,
		&pki.ops_ca_cert,
	))
	.await
	.unwrap();
	let mut config = ClientConfig::new(
		"0.0.0.0:0".parse().unwrap(),
		vec![BootstrapPeer {
			node_id: 1,
			address: a,
		}],
		TlsPaths {
			cert_path: pki.data_client_cert.clone(),
			key_path: pki.data_client_key.clone(),
			ca_path: pki.ca_cert.clone(),
		},
	);
	config.cluster_domain = tls_pki::CLUSTER_DOMAIN.into();
	let client = tokio::time::timeout(Duration::from_secs(5), Client::connect_quic(config.clone()))
		.await
		.unwrap()
		.unwrap();
	assert!(
		client.epoch() >= 2,
		"topology must come from the real control path"
	);
	let key = (0..10000)
		.map(|i| format!("adr629-p1-missing-{i}").into_bytes())
		.find(|key| {
			client
				.inner()
				.replicas_for_locality(Locality::from(Client::identity_for_key(
					TenantId::ZERO,
					key,
				)))
				.first()
				.map(|node| node.id)
				== Some(2)
		})
		.expect("key owned by the discovered, non-bootstrap node");
	// This is a routing/authentication pin, not a placement/finalization oracle.
	// The new range is not leased here: keep its typed NOT_LEADER response rather
	// than claim that a topology reply made it ready for customer writes.
	let result = tokio::time::timeout(
		Duration::from_secs(5),
		client.read(TenantId::ZERO, &key, ReadOptions::default()),
	)
	.await
	.unwrap();
	assert!(
		matches!(result, Err(wendelon_client::ClientError::ReadFailed {status}) if status == wendelon::schema::ReadStatus::NOT_LEADER)
	);
	let connection = client
		.inner()
		.factory()
		.get_connection(NodeId {
			id: 2,
			_reserved: 0,
		})
		.await
		.unwrap();
	assert_eq!(
		connection.remote_id(),
		Some(NodeId {
			id: 2,
			_reserved: 0
		})
	);
	println!(
		"ADR629_P1 production_client bind=wildcard outbound=true discovered_owner=2 data_reply=NOT_LEADER placement_readiness=not_claimed"
	);
	for field in 0..3 {
		let mut invalid = config.clone();
		let missing = root.join("missing");
		match field {
			0 => invalid.tls.cert_path = missing,
			1 => invalid.tls.key_path = missing,
			_ => invalid.tls.ca_path = missing,
		}
		assert!(
			matches!(
				Client::connect_quic(invalid).await,
				Err(wendelon_client::ClientError::Io(_))
			),
			"missing TLS input must fail without fallback"
		);
	}
	drop(connection);
	drop(client);
	// This pin owns both nodes inside the test runtime. Stop their producers,
	// then let runtime teardown retire the owned transport tasks. Graceful
	// daemon shutdown is not claimed by this routing/authentication test.
	node2.lifecycle.begin_drain();
	node1.lifecycle.begin_drain();
	node2.handles.abort();
	node1.handles.abort();
	drop(node2);
	drop(node1);
}
