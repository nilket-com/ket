#[path = "support/control_ring.rs"]
mod support;

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wendelon::config::load_node_bootstrap_config;
use wendelon::metrics::observability::ReadinessReporter;
use wendelon::network::control_server::{
	CONTROL_REASON_GRANTED, JoinClusterRequestWire, JoinClusterResponseWire,
};
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::pool::BufPool;
use wendelon::network::server::HrftService;
use wendelon::network::transport::TransportProvider;
use wendelon::network::wire;
use wendelon::network::{
	dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use wendelon::node::{ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, WendelonNode};
use wendelon::schema::{
	AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, FrameHeader, HybridTimestamp, LeaseStatus,
	MsgType, NodeId, ReleaseLeaseRequestMeta, ReleaseLeaseResponseMeta, TenantId,
};

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
replication_factor = {rf}
wal_io_depth = 64
slab_io_depth = 64

[identity]
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
{trusted_lease}
[identity.trusted_transfer_signers]
{trusted_transfer}

[crypto]
root_kek_provider = "dev-insecure"
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

fn trusted_cfg(
	tag: &str,
	node_id: u32,
	advertise_address: &str,
	rf: usize,
	all_nodes: &[u32],
	join_seeds: &[(u32, &str)],
) -> NodeBootstrapConfig {
	let mut cfg = NodeBootstrapConfig::dev(node_id, fresh_dir(tag), advertise_address.to_string());
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
	cfg
}

async fn wait_for_cluster(node: &WendelonNode, expected_epoch: u64, expected_nodes: usize) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
	loop {
		let snapshot = node.ring.snapshot();
		if node.ring.current_epoch() >= expected_epoch
			&& snapshot.data_capable_nodes() == expected_nodes
		{
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for node {} to observe epoch >= {expected_epoch} and {expected_nodes} nodes; got epoch={} nodes={}",
			node.local_node.id,
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

async fn wait_for_ready(node: &WendelonNode) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		let snapshot = node.readiness_reporter().snapshot().await;
		if snapshot.is_ready() {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for readiness; snapshot={snapshot:?}",
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn wait_for_tenant_quota(node: &WendelonNode, tenant_id: TenantId, quota_bytes: u64) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		let actual = snapshot
			.tenants
			.get(&tenant_id)
			.map(|tenant| tenant.quota_bytes);
		if actual == Some(quota_bytes) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for tenant {} quota {}; got {:?}",
			tenant_id.get(),
			quota_bytes,
			actual,
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn wait_for_failover_leader(left: &WendelonNode, right: &WendelonNode, previous: u32) -> u32 {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
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
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn acquire_local_primary_lease(node: &WendelonNode) -> (u64, u64) {
	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory_from(node.local_node, node.local_node, service);
	let (range_start, range_end) = node
		.ring
		.primary_ranges_for(node.local_node)
		.into_iter()
		.next()
		.expect("leader must own at least one primary range");
	let req = AcquireLeaseRequestMeta {
		range_start,
		range_end,
		epoch: node.ring.current_epoch(),
		holder: node.local_node,
		requested_hlc: HybridTimestamp {
			physical_ms: 50_000,
			logical: 0,
			node_id: node.local_node.id,
		},
		ttl_ms: 250,
		_pad: 0,
	};

	let resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::LEASE_REQ,
		bytemuck::bytes_of(&req),
		None,
		None,
	)
	.await
	.unwrap();
	let meta: AcquireLeaseResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<AcquireLeaseResponseMeta>()]);
	assert_eq!(meta.status, LeaseStatus::GRANTED);
	(range_start, range_end)
}

async fn release_local_primary_lease(node: &WendelonNode, range_start: u64, range_end: u64) {
	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory_from(node.local_node, node.local_node, service);
	let req = ReleaseLeaseRequestMeta {
		range_start,
		range_end,
		epoch: node.ring.current_epoch(),
		holder: node.local_node,
	};

	let resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::LEASE_RELEASE_REQ,
		bytemuck::bytes_of(&req),
		None,
		None,
	)
	.await
	.unwrap();
	let meta: ReleaseLeaseResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReleaseLeaseResponseMeta>()]);
	assert_eq!(meta.status, LeaseStatus::GRANTED);
}

async fn wait_for_lease_state(node: &WendelonNode, range_start: u64, expected: Option<(u32, u64)>) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		let actual = snapshot
			.leases
			.get(&range_start)
			.map(|lease| (lease.holder.id, lease.range_end));
		if actual == expected {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for lease {:?} on node {}; observed {:?}",
			expected,
			node.local_node.id,
			actual,
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
		capability_bits: 0,
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

struct BootstrapOutcome {
	final_epoch: u64,
	final_config: wendelon::consensus::EpochConfig,
	ring_signature: Vec<(u64, u64, Vec<u32>)>,
}

fn capture_ring_signature(node: &WendelonNode) -> Vec<(u64, u64, Vec<u32>)> {
	node.ring
		.snapshot()
		.replica_segments()
		.into_iter()
		.map(|segment| {
			(
				segment.start,
				segment.end,
				segment.replicas.into_iter().map(|node| node.id).collect(),
			)
		})
		.collect()
}

async fn bootstrap_partial_sequence(tag: &str, join_order: [u32; 2]) -> BootstrapOutcome {
	let registry = support::shared_loopback_registry();
	let all_nodes = [1u32, 2, 3];
	let node1_addr = "loopback://deterministic-node-1".to_string();
	let node2_addr = "loopback://deterministic-node-2".to_string();
	let node3_addr = "loopback://deterministic-node-3".to_string();

	let node1 = WendelonNode::bootstrap_with_provider(
		trusted_cfg(&format!("{tag}-node1"), 1, &node1_addr, 3, &all_nodes, &[]),
		provider_for(1, registry.clone()),
	)
	.await
	.unwrap();

	let mut node2 = None;
	let mut node3 = None;
	for joiner in join_order {
		match joiner {
			2 => {
				let cfg = trusted_cfg(
					&format!("{tag}-node2"),
					2,
					&node2_addr,
					3,
					&all_nodes,
					&[(1, node1_addr.as_str())],
				);
				node2 = Some(
					WendelonNode::bootstrap_with_provider(cfg, provider_for(2, registry.clone()))
						.await
						.unwrap(),
				);
			}
			3 => {
				let cfg = trusted_cfg(
					&format!("{tag}-node3"),
					3,
					&node3_addr,
					3,
					&all_nodes,
					&[(1, node1_addr.as_str())],
				);
				node3 = Some(
					WendelonNode::bootstrap_with_provider(cfg, provider_for(3, registry.clone()))
						.await
						.unwrap(),
				);
			}
			other => panic!("unexpected joiner id {other}"),
		}
	}

	wait_for_cluster(&node1, 3, 3).await;
	wait_for_voters(&node1, &[1, 2, 3]).await;
	wait_for_cluster(node2.as_ref().unwrap(), 3, 3).await;
	wait_for_cluster(node3.as_ref().unwrap(), 3, 3).await;

	let final_epoch = node1.ring.current_epoch();
	let final_config = node1
		.state_machine
		.state_snapshot()
		.await
		.epoch_history
		.get(&final_epoch)
		.cloned()
		.expect("final epoch should be committed");
	let ring_signature = capture_ring_signature(&node1);
	assert_eq!(
		capture_ring_signature(node2.as_ref().unwrap()),
		ring_signature
	);
	assert_eq!(
		capture_ring_signature(node3.as_ref().unwrap()),
		ring_signature
	);

	support::abort_node(node3.as_ref().unwrap());
	support::abort_node(node2.as_ref().unwrap());
	support::abort_node(&node1);

	BootstrapOutcome {
		final_epoch,
		final_config,
		ring_signature,
	}
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

#[tokio::test]
async fn restored_replacement_node_rejoins_live_cluster_and_catches_up() {
	let trusted = [1u32, 2, 3, 4];
	let node1_addr = "loopback://replacement-node-1".to_string();
	let node2_addr = "loopback://replacement-node-2".to_string();
	let node3_addr = "loopback://replacement-node-3".to_string();
	let node4_addr = "loopback://replacement-node-4".to_string();

	let node1_cfg = trusted_cfg("replacement-node1", 1, &node1_addr, 3, &trusted, &[]);
	let node2_cfg = trusted_cfg(
		"replacement-node2",
		2,
		&node2_addr,
		3,
		&trusted,
		&[(1, node1_addr.as_str())],
	);
	let node3_cfg = trusted_cfg(
		"replacement-node3",
		3,
		&node3_addr,
		3,
		&trusted,
		&[(1, node1_addr.as_str())],
	);

	let registry = support::shared_loopback_registry();
	let node1 =
		WendelonNode::bootstrap_with_provider(node1_cfg.clone(), provider_for(1, registry.clone()))
			.await
			.unwrap();
	let node2 =
		WendelonNode::bootstrap_with_provider(node2_cfg.clone(), provider_for(2, registry.clone()))
			.await
			.unwrap();
	let node3 =
		WendelonNode::bootstrap_with_provider(node3_cfg.clone(), provider_for(3, registry.clone()))
			.await
			.unwrap();

	wait_for_cluster(&node1, 3, 3).await;
	wait_for_cluster(&node2, 3, 3).await;
	wait_for_cluster(&node3, 3, 3).await;
	wait_for_voters(&node1, &[1, 2, 3]).await;
	wait_for_ready(&node3).await;
	node1.handles.lease_manager.abort();
	node2.handles.lease_manager.abort();
	node3.handles.lease_manager.abort();

	let baseline_quota = 128 * 1024 * 1024;
	node1
		.update_tenant_quota(TenantId::ZERO, baseline_quota)
		.await
		.unwrap();
	wait_for_tenant_quota(&node1, TenantId::ZERO, baseline_quota).await;
	wait_for_tenant_quota(&node2, TenantId::ZERO, baseline_quota).await;
	wait_for_tenant_quota(&node3, TenantId::ZERO, baseline_quota).await;

	let backup = node3.export_cluster_backup().await.unwrap();

	support::abort_node(&node3);
	drop(node3);

	let epoch_before_replacement = node1.ring.current_epoch();
	node1.remove_member_for_replacement(3).await.unwrap();

	wait_for_cluster(&node1, epoch_before_replacement + 1, 2).await;
	wait_for_cluster(&node2, epoch_before_replacement + 1, 2).await;
	wait_for_voters(&node1, &[1, 2]).await;

	let replacement_cfg = trusted_cfg(
		"replacement-node4-restored",
		4,
		&node4_addr,
		3,
		&trusted,
		&[(1, node1_addr.as_str())],
	);
	let restored = WendelonNode::restore_replacement_from_cluster_backup_with_provider(
		&backup,
		3,
		replacement_cfg,
		provider_for(4, registry.clone()),
	)
	.await
	.unwrap();
	restored.handles.lease_manager.abort();

	wait_for_cluster(&node1, epoch_before_replacement + 2, 3).await;
	wait_for_cluster(&node2, epoch_before_replacement + 2, 3).await;
	wait_for_cluster(&restored, epoch_before_replacement + 2, 3).await;
	wait_for_leader(&restored, 1).await;
	wait_for_voters(&node1, &[1, 2, 4]).await;
	wait_for_voters(&restored, &[1, 2, 4]).await;
	wait_for_tenant_quota(&restored, TenantId::ZERO, baseline_quota).await;
	wait_for_ready(&restored).await;

	let post_replacement_quota = baseline_quota * 2;
	node1
		.update_tenant_quota(TenantId::ZERO, post_replacement_quota)
		.await
		.unwrap();
	wait_for_tenant_quota(&node1, TenantId::ZERO, post_replacement_quota).await;
	wait_for_tenant_quota(&node2, TenantId::ZERO, post_replacement_quota).await;
	wait_for_tenant_quota(&restored, TenantId::ZERO, post_replacement_quota).await;

	support::abort_node(&restored);
	support::abort_node(&node2);
	support::abort_node(&node1);
}

#[tokio::test]
async fn leader_failover_recovery_replays_post_failover_commits() {
	let registry = support::shared_loopback_registry();
	let all_nodes = [1u32, 2, 3];
	let node1_addr = "loopback://failover-node-1".to_string();
	let node2_addr = "loopback://failover-node-2".to_string();
	let node3_addr = "loopback://failover-node-3".to_string();

	let node1_cfg = trusted_cfg("failover-node1", 1, &node1_addr, 3, &all_nodes, &[]);
	let node2_cfg = trusted_cfg(
		"failover-node2",
		2,
		&node2_addr,
		3,
		&all_nodes,
		&[(1, node1_addr.as_str())],
	);
	let node3_cfg = trusted_cfg(
		"failover-node3",
		3,
		&node3_addr,
		3,
		&all_nodes,
		&[(1, node1_addr.as_str())],
	);

	let node1 =
		WendelonNode::bootstrap_with_provider(node1_cfg.clone(), provider_for(1, registry.clone()))
			.await
			.unwrap();
	let node2 = WendelonNode::bootstrap_with_provider(node2_cfg, provider_for(2, registry.clone()))
		.await
		.unwrap();
	let node3 = WendelonNode::bootstrap_with_provider(node3_cfg, provider_for(3, registry.clone()))
		.await
		.unwrap();

	wait_for_cluster(&node1, 3, 3).await;
	wait_for_cluster(&node2, 3, 3).await;
	wait_for_cluster(&node3, 3, 3).await;
	wait_for_leader(&node2, 1).await;
	wait_for_leader(&node3, 1).await;
	wait_for_voters(&node1, &[1, 2, 3]).await;

	node1.handles.lease_manager.abort();
	node2.handles.lease_manager.abort();
	node3.handles.lease_manager.abort();

	registry.unpublish(node1.local_node);
	tokio::time::timeout(std::time::Duration::from_secs(2), node1.shutdown())
		.await
		.expect("leader shutdown should finish")
		.unwrap();

	let new_leader = wait_for_failover_leader(&node2, &node3, 1).await;
	let (leader, follower) = if node2.local_node.id == new_leader {
		(&node2, &node3)
	} else {
		(&node3, &node2)
	};

	let (range_start, range_end) = acquire_local_primary_lease(leader).await;
	wait_for_lease_state(leader, range_start, Some((new_leader, range_end))).await;
	wait_for_lease_state(follower, range_start, Some((new_leader, range_end))).await;

	let recovered =
		WendelonNode::bootstrap_with_provider(node1_cfg, provider_for(1, registry.clone()))
			.await
			.unwrap();
	recovered.handles.lease_manager.abort();
	wait_for_cluster(&recovered, 3, 3).await;
	wait_for_leader(&recovered, new_leader).await;
	wait_for_lease_state(&recovered, range_start, Some((new_leader, range_end))).await;

	wait_for_leader(leader, new_leader).await;
	release_local_primary_lease(leader, range_start, range_end).await;
	wait_for_lease_state(leader, range_start, None).await;
	wait_for_lease_state(follower, range_start, None).await;
	wait_for_lease_state(&recovered, range_start, None).await;

	support::abort_node(&recovered);
	support::abort_node(&node3);
	support::abort_node(&node2);
}

#[tokio::test]
async fn partial_bring_up_orders_converge_to_same_final_ring() {
	let two_then_three = bootstrap_partial_sequence("partial-23", [2, 3]).await;
	let three_then_two = bootstrap_partial_sequence("partial-32", [3, 2]).await;

	assert_eq!(two_then_three.final_epoch, 3);
	assert_eq!(three_then_two.final_epoch, 3);
	assert_eq!(two_then_three.final_config, three_then_two.final_config);
	assert_eq!(two_then_three.ring_signature, three_then_two.ring_signature);
}
