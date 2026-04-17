#[path = "support/control_ring.rs"]
mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use wendelon::network::control_server::{
	CONTROL_REASON_INCOMPATIBLE_VERSION, JoinClusterRequestWire, JoinClusterResponseWire,
};
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::pool::BufPool;
use wendelon::network::transport::TransportProvider;
use wendelon::network::wire;
use wendelon::network::{
	dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use wendelon::node::{ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, WendelonNode};
use wendelon::schema::{FrameHeader, MsgType, NodeId, ProtocolVersionWindow};

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

fn trusted_cfg(
	tag: &str,
	node_id: u32,
	advertise_address: &str,
	all_nodes: &[u32],
	current_protocol: u16,
	min_compatible_protocol: u16,
) -> NodeBootstrapConfig {
	let mut cfg = NodeBootstrapConfig::dev(
		node_id,
		support::tempdir(tag),
		advertise_address.to_string(),
	)
	.with_protocol_window(current_protocol, min_compatible_protocol);
	for trusted_node_id in all_nodes {
		let lease = dev_lease_signing_secret(*trusted_node_id);
		let transfer = dev_transfer_signing_secret(*trusted_node_id);
		cfg.trusted_lease_signers
			.insert(*trusted_node_id, lease_verifying_key_bytes(lease));
		cfg.trusted_transfer_signers
			.insert(*trusted_node_id, transfer_verifying_key_bytes(transfer));
	}
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
	protocol: ProtocolVersionWindow,
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
		protocol,
	};
	let meta = wire::encode(&req).unwrap();
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: wendelon::schema::HRFT_ENVELOPE_VERSION,
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
async fn supported_protocol_window_negotiates_and_joined_node_communicates() {
	let registry = support::shared_loopback_registry();
	let leader = WendelonNode::bootstrap_with_provider(
		trusted_cfg("upgrade-leader", 1, "loopback://node-1", &[1, 2], 2, 1),
		provider_for(1, registry.clone()),
	)
	.await
	.unwrap();
	support::wait_for_local_leader(&leader).await;

	let mut join_cfg = trusted_cfg("upgrade-joiner", 2, "loopback://node-2", &[1, 2], 1, 1);
	join_cfg.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: 1,
			address: "loopback://node-1".into(),
		}],
	};
	let joiner = WendelonNode::bootstrap_with_provider(join_cfg, provider_for(2, registry.clone()))
		.await
		.unwrap();

	wait_for_cluster(&leader, 2, 2).await;
	wait_for_cluster(&joiner, 2, 2).await;
	wait_for_voters(&leader, &[1, 2]).await;
	assert_eq!(
		wire::negotiate_protocol(
			ProtocolVersionWindow::new(2, 1),
			ProtocolVersionWindow::new(1, 1),
		)
		.unwrap(),
		1
	);

	tokio::time::timeout(std::time::Duration::from_secs(2), joiner.shutdown())
		.await
		.unwrap()
		.unwrap();
	tokio::time::timeout(std::time::Duration::from_secs(2), leader.shutdown())
		.await
		.unwrap()
		.unwrap();
}

#[tokio::test]
async fn incompatible_protocol_window_fails_fast_and_explicitly() {
	let registry = support::shared_loopback_registry();
	let leader = WendelonNode::bootstrap_with_provider(
		trusted_cfg(
			"upgrade-incompat-leader",
			1,
			"loopback://node-1",
			&[1],
			3,
			2,
		),
		provider_for(1, registry.clone()),
	)
	.await
	.unwrap();
	support::wait_for_local_leader(&leader).await;

	let resp = send_join_request(
		provider_for(2, registry.clone()),
		1,
		"loopback://node-1",
		2,
		"loopback://node-2",
		ProtocolVersionWindow::new(1, 1),
	)
	.await;
	assert!(!resp.accepted);
	assert_eq!(resp.reason_code, CONTROL_REASON_INCOMPATIBLE_VERSION);
	assert_eq!(resp.leader_hint, Some(1));
	assert_eq!(resp.local_protocol, ProtocolVersionWindow::new(3, 2));
	assert_eq!(resp.negotiated_protocol, None);

	let mut join_cfg = trusted_cfg(
		"upgrade-incompat-joiner",
		2,
		"loopback://node-2",
		&[1, 2],
		1,
		1,
	);
	join_cfg.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: 1,
			address: "loopback://node-1".into(),
		}],
	};
	let err =
		match WendelonNode::bootstrap_with_provider(join_cfg, provider_for(2, registry.clone()))
			.await
		{
			Ok(_) => panic!("incompatible join should fail"),
			Err(err) => err,
		};
	assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
	assert!(err.to_string().contains("local_protocol=2..=3"));

	leader.shutdown().await.unwrap();
}
