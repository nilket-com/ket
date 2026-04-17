#[path = "support/control_ring.rs"]
mod support;

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wendelon::config::load_node_bootstrap_config;
use wendelon::network::control_server::{
	CONTROL_REASON_GRANTED, JoinClusterRequestWire, JoinClusterResponseWire,
};
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::pool::BufPool;
use wendelon::network::transport::TransportProvider;
use wendelon::network::wire;
use wendelon::network::{
	dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use wendelon::node::WendelonNode;
use wendelon::schema::{FrameHeader, MsgType, NodeId};

fn fresh_dir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-multinode-bootstrap-{tag}-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

fn hex(bytes: [u8; 32]) -> String {
	let mut out = String::with_capacity(64);
	for byte in bytes {
		write!(&mut out, "{byte:02x}").unwrap();
	}
	out
}

fn write_node_config(
	root: &Path,
	node_id: u32,
	advertise_address: &str,
	rf: usize,
	join_seeds: &[(u32, &str)],
	trusted_node_ids: &[u32],
) -> PathBuf {
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();

	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);

	let mut trusted_lease = String::new();
	let mut trusted_transfer = String::new();
	for trusted_node_id in trusted_node_ids {
		let lease = dev_lease_signing_secret(*trusted_node_id);
		let transfer = dev_transfer_signing_secret(*trusted_node_id);
		writeln!(
			&mut trusted_lease,
			"\"{trusted_node_id}\" = \"{}\"",
			hex(lease_verifying_key_bytes(lease))
		)
		.unwrap();
		writeln!(
			&mut trusted_transfer,
			"\"{trusted_node_id}\" = \"{}\"",
			hex(transfer_verifying_key_bytes(transfer))
		)
		.unwrap();
	}

	let mut cluster = String::from("[cluster]\n");
	if join_seeds.is_empty() {
		cluster.push_str("mode = \"bootstrap\"\n");
	} else {
		cluster.push_str("mode = \"join\"\n");
		for (seed_id, seed_addr) in join_seeds {
			writeln!(&mut cluster, "\n[[cluster.seed_nodes]]").unwrap();
			writeln!(&mut cluster, "node_id = {seed_id}").unwrap();
			writeln!(&mut cluster, "address = \"{seed_addr}\"").unwrap();
		}
	}

	let body = format!(
		r#"
[node]
node_id = {node_id}
advertise_address = "{advertise_address}"

{cluster}
[storage]
data_dir = "data"

[tuning]
rf = {rf}
io_depth = 64

[identity]
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
{trusted_lease}
[identity.trusted_transfer_signers]
{trusted_transfer}
"#,
		node_id = node_id,
		advertise_address = advertise_address,
		cluster = cluster,
		rf = rf,
		lease_secret = hex(lease_secret),
		transfer_secret = hex(transfer_secret),
		trusted_lease = trusted_lease,
		trusted_transfer = trusted_transfer,
	);

	let path = root.join("wendelon.toml");
	std::fs::write(&path, body).unwrap();
	path
}

fn provider_for(
	node_id: u32,
	registry: Arc<support::LoopbackRegistry>,
) -> Arc<dyn TransportProvider> {
	support::loopback_provider(
		NodeId {
			id: node_id,
			_reserved: 0,
		},
		registry,
	)
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

async fn wait_for_leader(node: &WendelonNode, expected_leader: u32) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		if node.raft.current_leader().await == Some(expected_leader) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader {expected_leader}; current leader = {:?}",
			node.raft.current_leader().await
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn wait_for_voters(node: &WendelonNode, expected: &[u32]) {
	let expected: BTreeSet<u32> = expected.iter().copied().collect();
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		let voters: BTreeSet<u32> = snapshot.last_membership.voter_ids().collect();
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

async fn send_join_request(
	provider: Arc<dyn TransportProvider>,
	target_id: u32,
	target_address: &str,
	joining_id: u32,
	joining_address: &str,
) -> JoinClusterResponseWire {
	let pool = BufPool::new(8, 64 * 1024);
	let conn = provider
		.connect(
			NodeId {
				id: target_id,
				_reserved: 0,
			},
			target_address,
		)
		.await
		.unwrap();
	let mut stream = conn.open_bi().await.unwrap();
	let req = JoinClusterRequestWire {
		node_id: joining_id,
		advertise_address: joining_address.into(),
		tier_bits: wendelon::network::transport::TransportTierSet::empty()
			.with(wendelon::network::transport::TransportTier::QuicIoUring)
			.as_u8(),
		role_bits: wendelon::network::transport::NodeRoleSet::empty()
			.with(wendelon::network::transport::NodeRole::Control)
			.with(wendelon::network::transport::NodeRole::Data)
			.as_u8(),
		protocol: wendelon::schema::ProtocolVersionWindow::default(),
	};
	let meta = wire::encode(&req).unwrap();
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::JOIN_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: meta.len() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &header, &meta, None, None)
		.await
		.unwrap();
	stream.finish().await.unwrap();
	let resp = read_frame(&mut *stream, &pool).await.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::JOIN_RESP.0);
	wire::decode(&resp.meta).unwrap()
}

#[tokio::test]
async fn cluster_forms_from_config_and_join_seed_can_be_a_peer() {
	let trusted = [1u32, 2, 3];
	let node1_root = fresh_dir("node1");
	let node2_root = fresh_dir("node2");
	let node3_root = fresh_dir("node3");
	let node1_addr = "loopback://node-1";
	let node2_addr = "loopback://node-2";
	let node3_addr = "loopback://node-3";

	let node1_cfg_path = write_node_config(&node1_root, 1, node1_addr, 3, &[], &trusted);
	let node2_cfg_path =
		write_node_config(&node2_root, 2, node2_addr, 3, &[(1, node1_addr)], &trusted);
	// Seed through node 2 only to prove bootstrap-from-peer works
	// even when the seed is not the leader.
	let node3_cfg_path =
		write_node_config(&node3_root, 3, node3_addr, 3, &[(2, node2_addr)], &trusted);

	let registry = support::shared_loopback_registry();
	let node1 = WendelonNode::bootstrap_with_provider(
		load_node_bootstrap_config(&node1_cfg_path).unwrap(),
		provider_for(1, registry.clone()),
	)
	.await
	.unwrap();
	let node2 = WendelonNode::bootstrap_with_provider(
		load_node_bootstrap_config(&node2_cfg_path).unwrap(),
		provider_for(2, registry.clone()),
	)
	.await
	.unwrap();
	let node3 = WendelonNode::bootstrap_with_provider(
		load_node_bootstrap_config(&node3_cfg_path).unwrap(),
		provider_for(3, registry.clone()),
	)
	.await
	.unwrap();

	wait_for_cluster(&node1, 3, 3).await;
	wait_for_cluster(&node2, 3, 3).await;
	wait_for_cluster(&node3, 3, 3).await;
	wait_for_leader(&node2, 1).await;
	wait_for_leader(&node3, 1).await;
	wait_for_voters(&node1, &[1, 2, 3]).await;

	support::abort_node(&node3);
	support::abort_node(&node2);
	support::abort_node(&node1);
}

#[tokio::test]
async fn explicit_join_is_idempotent_and_recovery_rejoins_cleanly() {
	let trusted = [1u32, 2];
	let node1_root = fresh_dir("rejoin-node1");
	let node2_root = fresh_dir("rejoin-node2");
	let node1_addr = "loopback://rejoin-node-1";
	let node2_addr = "loopback://rejoin-node-2";

	let node1_cfg_path = write_node_config(&node1_root, 1, node1_addr, 2, &[], &trusted);
	let node2_cfg_path =
		write_node_config(&node2_root, 2, node2_addr, 2, &[(1, node1_addr)], &trusted);

	let registry = support::shared_loopback_registry();
	let node1 = WendelonNode::bootstrap_with_provider(
		load_node_bootstrap_config(&node1_cfg_path).unwrap(),
		provider_for(1, registry.clone()),
	)
	.await
	.unwrap();
	let node2_provider = provider_for(2, registry.clone());
	let node2 = WendelonNode::bootstrap_with_provider(
		load_node_bootstrap_config(&node2_cfg_path).unwrap(),
		node2_provider.clone(),
	)
	.await
	.unwrap();

	wait_for_cluster(&node1, 2, 2).await;
	wait_for_cluster(&node2, 2, 2).await;
	wait_for_voters(&node1, &[1, 2]).await;

	let epoch_before = node1.ring.current_epoch();
	let resp = send_join_request(node2_provider.clone(), 1, node1_addr, 2, node2_addr).await;
	assert!(resp.accepted);
	assert_eq!(resp.reason_code, CONTROL_REASON_GRANTED);
	tokio::time::sleep(std::time::Duration::from_millis(200)).await;
	assert_eq!(node1.ring.current_epoch(), epoch_before);
	wait_for_voters(&node1, &[1, 2]).await;

	support::abort_node(&node2);
	drop(node2);

	let node2_restarted = WendelonNode::bootstrap_with_provider(
		load_node_bootstrap_config(&node2_cfg_path).unwrap(),
		provider_for(2, registry.clone()),
	)
	.await
	.unwrap();

	wait_for_cluster(&node2_restarted, 2, 2).await;
	wait_for_leader(&node2_restarted, 1).await;
	assert_eq!(node1.ring.current_epoch(), epoch_before);

	support::abort_node(&node2_restarted);
	support::abort_node(&node1);
}
