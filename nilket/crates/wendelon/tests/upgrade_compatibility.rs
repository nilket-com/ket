#[path = "support/control_ring.rs"]
mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use wendelon::consensus::{TenantFinalization, WendelonRequest};
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
use wendelon::schema::{
	DurabilityLevel, FinalizationMarker, FrameHeader, HybridTimestamp, Identity, MsgType, NodeId,
	ProtocolVersionWindow, TenantId,
};

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
) -> (support::TestDir, NodeBootstrapConfig) {
	let scratch = support::tempdir(tag);
	let mut cfg = NodeBootstrapConfig::dev(
		node_id,
		scratch.to_path_buf(),
		advertise_address.to_string(),
	)
	.with_protocol_window(current_protocol, min_compatible_protocol);
	cfg.drift_max_offset_ms = 5_000;
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

async fn bootstrap_node(
	cfg: NodeBootstrapConfig,
	provider: Arc<dyn TransportProvider>,
) -> std::io::Result<WendelonNode> {
	let mut attempts = 0;
	loop {
		match WendelonNode::bootstrap_with_provider(cfg.clone(), provider.clone()).await {
			Ok(node) => return Ok(node),
			Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied && attempts < 2 => {
				attempts += 1;
				tokio::time::sleep(Duration::from_millis(100)).await;
			}
			Err(err) => return Err(err),
		}
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

async fn wait_for_leader(node: &WendelonNode, expected_leader: u32) {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		if node.raft.current_leader().await == Some(expected_leader) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader {expected_leader}; current leader = {:?}",
			node.raft.current_leader().await
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
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

async fn wait_for_failover_leader(left: &WendelonNode, right: &WendelonNode, previous: u32) -> u32 {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		let left_leader = left.raft.current_leader().await;
		let right_leader = right.raft.current_leader().await;
		if let (Some(left_leader), Some(right_leader)) = (left_leader, right_leader)
			&& left_leader == right_leader
			&& left_leader != previous
		{
			return left_leader;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for failover leader after {previous}; left={left_leader:?} right={right_leader:?}",
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

fn wall_clock_ms(offset_ms: u64) -> u64 {
	let now = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	now.saturating_add(offset_ms % 50)
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn id(locality: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality.to_le_bytes());
	Identity::new(bytes)
}

fn finalize_marker(epoch: u64, physical_ms: u64, node_id: u32) -> FinalizationMarker {
	FinalizationMarker {
		log_index: 0,
		term: 0,
		epoch,
		commit_hlc: HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id,
		},
		payload_hash: [0; 32],
		e_safe_index: 0,
		durability: DurabilityLevel::QUORUM,
		_pad: [0; 7],
	}
}

fn tenant_finalization(marker: FinalizationMarker) -> TenantFinalization {
	TenantFinalization {
		marker,
		tenant_id: tenant(),
		key_identity: id(marker.epoch),
		charged_bytes: marker.epoch,
		record_count: 1,
	}
}

async fn wait_for_finalization(node: &WendelonNode, marker: FinalizationMarker) {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		if snapshot
			.e_safe_watermarks
			.get(&marker.epoch)
			.copied()
			.is_some_and(|hlc| hlc >= marker.commit_hlc)
		{
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for finalization epoch {} on node {}",
			marker.epoch,
			node.local_node.id
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

async fn commit_control_write(
	leader: &WendelonNode,
	replicas: &[&WendelonNode],
	epoch: u64,
	physical_ms: u64,
) -> FinalizationMarker {
	let marker = finalize_marker(epoch, wall_clock_ms(physical_ms), leader.local_node.id);
	leader
		.raft
		.client_write(WendelonRequest::Finalize(tenant_finalization(marker)))
		.await
		.unwrap();
	for replica in replicas {
		wait_for_finalization(replica, marker).await;
	}
	marker
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
		capability_bits: 0,
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
	let (_scratch_1, cfg_1) = trusted_cfg("upgrade-leader", 1, "loopback://node-1", &[1, 2], 2, 1);
	let leader = WendelonNode::bootstrap_with_provider(cfg_1, provider_for(1, registry.clone()))
		.await
		.unwrap();
	support::wait_for_local_leader(&leader).await;

	let (_join_cfg_dir, mut join_cfg) =
		trusted_cfg("upgrade-joiner", 2, "loopback://node-2", &[1, 2], 1, 1);
	join_cfg.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: 1,
			address: "loopback://node-1".into(),
		}],
	};
	// ADR 0726 enrollment: the joiner is given the forming member's root KEK.
	wendelon::crypto::test_support::enroll_root_artifact(
		&_scratch_1.to_path_buf(),
		&join_cfg.data_dir,
	)
	.unwrap();
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
	let (_scratch_2, cfg_2) = trusted_cfg(
		"upgrade-incompat-leader",
		1,
		"loopback://node-1",
		&[1],
		3,
		2,
	);
	let leader = WendelonNode::bootstrap_with_provider(cfg_2, provider_for(1, registry.clone()))
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

	let (_join_cfg_dir, mut join_cfg) = trusted_cfg(
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
	// ADR 0726 enrollment: the joiner is given the forming member's root KEK.
	wendelon::crypto::test_support::enroll_root_artifact(
		&_scratch_2.to_path_buf(),
		&join_cfg.data_dir,
	)
	.unwrap();
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

#[tokio::test]
async fn rolling_restart_with_compatible_protocol_window_preserves_control_writes() {
	let registry = support::shared_loopback_registry();
	let node1_addr = "loopback://rolling-upgrade-node-1";
	let node2_addr = "loopback://rolling-upgrade-node-2";
	let node3_addr = "loopback://rolling-upgrade-node-3";
	let all_nodes = [1u32, 2, 3];

	let (_node1_v1_dir, node1_v1) =
		trusted_cfg("rolling-upgrade-node1", 1, node1_addr, &all_nodes, 1, 1);
	let (_node2_v1_dir, mut node2_v1) =
		trusted_cfg("rolling-upgrade-node2", 2, node2_addr, &all_nodes, 1, 1);
	node2_v1.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: 1,
			address: node1_addr.into(),
		}],
	};
	let (_node3_v1_dir, mut node3_v1) =
		trusted_cfg("rolling-upgrade-node3", 3, node3_addr, &all_nodes, 1, 1);
	node3_v1.cluster = ClusterBootstrapConfig::Join {
		seeds: vec![SeedNodeConfig {
			node_id: 1,
			address: node1_addr.into(),
		}],
	};
	let node1_v2 = node1_v1.clone().with_protocol_window(2, 1);
	let node2_v2 = node2_v1.clone().with_protocol_window(2, 1);
	let node3_v2 = node3_v1.clone().with_protocol_window(2, 1);

	let node1 = match bootstrap_node(node1_v1, provider_for(1, registry.clone())).await {
		Ok(node) => node,
		Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
			eprintln!(
				"skipping rolling_restart_with_compatible_protocol_window_preserves_control_writes in this environment: {err}"
			);
			return;
		}
		Err(err) => panic!("bootstrap node1 failed: {err}"),
	};
	// ADR 0726 enrollment: each joiner is given the forming member's root KEK.
	for joiner in [&node2_v1, &node3_v1] {
		wendelon::crypto::test_support::enroll_root_artifact(&node1_v2.data_dir, &joiner.data_dir)
			.unwrap();
	}
	let node2 = bootstrap_node(node2_v1, provider_for(2, registry.clone()))
		.await
		.unwrap();
	let node3 = bootstrap_node(node3_v1, provider_for(3, registry.clone()))
		.await
		.unwrap();

	wait_for_cluster(&node1, 3, 3).await;
	wait_for_cluster(&node2, 3, 3).await;
	wait_for_cluster(&node3, 3, 3).await;
	wait_for_voters(&node1, &[1, 2, 3]).await;
	wait_for_leader(&node2, 1).await;
	wait_for_leader(&node3, 1).await;
	commit_control_write(&node1, &[&node1, &node2, &node3], 101, 10_100).await;

	node2.begin_drain();
	node2.wait_for_drain().await;
	registry.unpublish(node2.local_node);
	tokio::time::timeout(Duration::from_secs(2), node2.shutdown())
		.await
		.unwrap()
		.unwrap();
	let node2 = bootstrap_node(node2_v2, provider_for(2, registry.clone()))
		.await
		.unwrap();
	wait_for_cluster(&node2, 3, 3).await;
	wait_for_leader(&node2, 1).await;
	commit_control_write(&node1, &[&node1, &node2, &node3], 102, 10_200).await;

	node3.begin_drain();
	node3.wait_for_drain().await;
	registry.unpublish(node3.local_node);
	tokio::time::timeout(Duration::from_secs(2), node3.shutdown())
		.await
		.unwrap()
		.unwrap();
	let node3 = bootstrap_node(node3_v2, provider_for(3, registry.clone()))
		.await
		.unwrap();
	wait_for_cluster(&node3, 3, 3).await;
	wait_for_leader(&node3, 1).await;
	commit_control_write(&node1, &[&node1, &node2, &node3], 103, 10_300).await;

	node1.begin_drain();
	node1.wait_for_drain().await;
	registry.unpublish(node1.local_node);
	tokio::time::timeout(Duration::from_secs(2), node1.shutdown())
		.await
		.unwrap()
		.unwrap();

	let new_leader = wait_for_failover_leader(&node2, &node3, 1).await;
	let leader = if new_leader == 2 { &node2 } else { &node3 };
	commit_control_write(leader, &[&node2, &node3], 104, 10_400).await;

	let node1 = bootstrap_node(node1_v2, provider_for(1, registry.clone()))
		.await
		.unwrap();
	wait_for_cluster(&node1, 3, 3).await;
	wait_for_leader(&node1, new_leader).await;
	wait_for_leader(&node2, new_leader).await;
	wait_for_leader(&node3, new_leader).await;
	wait_for_voters(&node1, &[1, 2, 3]).await;
	let leader = if new_leader == 2 { &node2 } else { &node3 };
	commit_control_write(leader, &[&node1, &node2, &node3], 105, 10_500).await;

	registry.unpublish(node1.local_node);
	node1.handles.abort();
	drop(node1);
	registry.unpublish(node2.local_node);
	node2.handles.abort();
	drop(node2);
	registry.unpublish(node3.local_node);
	node3.handles.abort();
	drop(node3);
	tokio::time::sleep(Duration::from_millis(25)).await;
}
