#[path = "../src/test_support/test_dir.rs"]
mod test_dir;
use std::sync::Arc;
use wendelon::network::pool::BufPool;
use wendelon::network::transport::{TransportProvider, TransportTier};
use wendelon::network::{
	dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use wendelon::node::{ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, WendelonNode};
use wendelon::schema::NodeId;
use wendelon::simulator::network::{
	SimulatedTransportEventKind, SimulatedTransportNetwork, SimulatedTransportProvider,
};

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

fn fresh_dir(tag: &str) -> test_dir::TestDir {
	test_dir::TestDir::new(&format!("wendelon-simulator-transport-{tag}"))
}

fn trusted_cfg(
	tag: &str,
	node_id: u32,
	advertise_address: &str,
	rf: usize,
	all_nodes: &[u32],
	join_seeds: &[(u32, &str)],
) -> (test_dir::TestDir, NodeBootstrapConfig) {
	let scratch = fresh_dir(tag);
	let mut cfg = NodeBootstrapConfig::dev(
		node_id,
		scratch.to_path_buf(),
		advertise_address.to_string(),
	);
	cfg.rf = rf;
	cfg.cluster = if join_seeds.is_empty() {
		ClusterBootstrapConfig::Bootstrap
	} else {
		ClusterBootstrapConfig::Join {
			seeds: join_seeds
				.iter()
				.map(|(seed_id, address)| SeedNodeConfig {
					node_id: *seed_id,
					address: (*address).to_string(),
				})
				.collect(),
		}
	};
	for trusted_node_id in all_nodes {
		let lease = dev_lease_signing_secret(*trusted_node_id);
		let transfer = dev_transfer_signing_secret(*trusted_node_id);
		cfg.trusted_lease_signers
			.insert(*trusted_node_id, lease_verifying_key_bytes(lease));
		cfg.trusted_transfer_signers
			.insert(*trusted_node_id, transfer_verifying_key_bytes(transfer));
	}
	(scratch, cfg)
}

fn provider(local: u32, network: Arc<SimulatedTransportNetwork>) -> Arc<dyn TransportProvider> {
	Arc::new(SimulatedTransportProvider::new(
		node(local),
		network,
		BufPool::new(64, 1024 * 1024),
		TransportTier::QuicIoUring,
	))
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

#[tokio::test]
async fn two_node_cluster_forms_over_simulated_transport() {
	let network = Arc::new(SimulatedTransportNetwork::default());
	let node1_addr = "sim://node-1";
	let node2_addr = "sim://node-2";
	let (_scratch_1, cfg_1) = trusted_cfg("sim-transport-node-1", 1, node1_addr, 2, &[1, 2], &[]);
	let node1 = WendelonNode::bootstrap_with_provider(cfg_1, provider(1, network.clone()))
		.await
		.unwrap();
	let (_scratch_2, cfg_2) = trusted_cfg(
		"sim-transport-node-2",
		2,
		node2_addr,
		2,
		&[1, 2],
		&[(1, node1_addr)],
	);
	// ADR 0726 enrollment: the joiner is given the forming member's root KEK.
	wendelon::crypto::test_support::enroll_root_artifact(
		&_scratch_1.to_path_buf(),
		&cfg_2.data_dir,
	)
	.unwrap();
	let node2 = WendelonNode::bootstrap_with_provider(cfg_2, provider(2, network.clone()))
		.await
		.unwrap();

	wait_for_cluster(&node1, 2, 2).await;
	wait_for_cluster(&node2, 2, 2).await;

	let events = network.events();
	assert!(
		events
			.iter()
			.any(|event| matches!(event.kind, SimulatedTransportEventKind::Connect))
	);
	assert!(
		events
			.iter()
			.any(|event| matches!(event.kind, SimulatedTransportEventKind::Published { .. }))
	);

	node2.shutdown().await.unwrap();
	node1.shutdown().await.unwrap();
}
