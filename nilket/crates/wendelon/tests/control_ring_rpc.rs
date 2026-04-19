#[path = "support/control_ring.rs"]
mod support;

use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wendelon::client::ClusterStatusWire;
use wendelon::network::control_server::{
	AdvanceEpochResponseWire, CONTROL_REASON_GRANTED, WendelonControlServer,
};
use wendelon::network::router::MsgRouterBuilder;
use wendelon::network::server::HrftService;
use wendelon::network::traits::{TransferLeadershipRequestMeta, TransferLeadershipResponseMeta};
use wendelon::network::transport::{
	MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportTier, TransportTierSet,
};
use wendelon::network::wire;
use wendelon::network::{
	TransferAuthority, dev_transfer_signing_secret, transfer_verifying_key_bytes,
};
use wendelon::node::{LEASE_TTL_MS, NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{
	AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, HybridTimestamp, LeaseDescriptor,
	LeaseStatus, MsgType, NodeId, ReleaseLeaseRequestMeta, ReleaseLeaseResponseMeta, TransferToken,
};

fn decode_status_response(
	resp: &wendelon::network::core::HrftFrame,
) -> (ClusterStatusWire, Vec<LeaseDescriptor>) {
	let status: ClusterStatusWire = wire::decode(&resp.meta).unwrap();
	let leases = resp
		.trailing
		.as_ref()
		.map(|bytes| wire::decode::<Vec<LeaseDescriptor>>(bytes).unwrap())
		.unwrap_or_default();
	(status, leases)
}

fn read_audit_log(path: &Path) -> String {
	std::fs::read_to_string(path).unwrap_or_default()
}

async fn wait_for_audit_event(path: &Path, event_type: &str) -> String {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
	let needle = format!("\"event_type\":\"{event_type}\"");
	loop {
		let audit = read_audit_log(path);
		if audit.contains(&needle) {
			return audit;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for audit event {event_type}; current log: {audit}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

fn epoch_config_batch() -> RecordBatch {
	let tier_bits = TransportTierSet::empty()
		.with(TransportTier::QuicIoUring)
		.as_u8();
	let role_bits = NodeRoleSet::empty()
		.with(NodeRole::Data)
		.with(NodeRole::Control)
		.as_u8();
	support::epoch_config_batch(
		&[
			(1, "127.0.0.1:0", tier_bits, role_bits),
			(2, "127.0.0.1:1", tier_bits, role_bits),
		],
		&[1],
		&[2],
	)
}

#[tokio::test]
async fn lease_req_commits_through_control_ring_handler() {
	let node = support::bootstrap_node("lease-rpc", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory_from(node.local_node, node.local_node, service);
	let (range_start, range_end) = node
		.ring
		.primary_ranges_for(node.local_node)
		.into_iter()
		.next()
		.unwrap();
	let req = AcquireLeaseRequestMeta {
		range_start,
		range_end,
		epoch: node.ring.current_epoch(),
		holder: node.local_node,
		requested_hlc: HybridTimestamp {
			physical_ms: 10_000,
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
	assert_eq!(resp.header.msg_type, MsgType::LEASE_RESP.0);

	let meta: AcquireLeaseResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<AcquireLeaseResponseMeta>()]);
	assert_eq!(meta.status, LeaseStatus::GRANTED);

	let lease: LeaseDescriptor = *bytemuck::from_bytes(
		&resp.trailing.as_ref().unwrap()[..std::mem::size_of::<LeaseDescriptor>()],
	);
	assert_eq!(lease.range_start, range_start);
	assert_eq!(lease.range_end, range_end);
	assert_eq!(lease.holder, node.local_node);

	let snapshot = node.state_machine.state_snapshot().await;
	assert!(snapshot.leases.contains_key(&range_start));

	support::abort_node(&node);
}

#[tokio::test]
async fn lease_req_rejects_invalid_target_before_commit() {
	let node = support::bootstrap_node("lease-rpc-invalid-target", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory_from(node.local_node, node.local_node, service);
	let (range_start, range_end) = node
		.ring
		.primary_ranges_for(node.local_node)
		.into_iter()
		.next()
		.unwrap();

	let req = AcquireLeaseRequestMeta {
		range_start,
		range_end,
		epoch: node.ring.current_epoch(),
		holder: NodeId {
			id: 2,
			_reserved: 0,
		},
		requested_hlc: HybridTimestamp {
			physical_ms: 20_000,
			logical: 0,
			node_id: 2,
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
	assert_eq!(meta.status, LeaseStatus::DENIED_INVALID_TARGET);
	assert!(resp.trailing.is_none());
	let snapshot = node.state_machine.state_snapshot().await;
	assert!(snapshot.leases.is_empty());

	support::abort_node(&node);
}

#[tokio::test]
async fn lease_req_rejects_non_canonical_range_for_owner() {
	let node = support::bootstrap_node("lease-rpc-non-canonical", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory_from(node.local_node, node.local_node, service);
	let (range_start, range_end) = node
		.ring
		.primary_ranges_for(node.local_node)
		.into_iter()
		.next()
		.unwrap();
	let req = AcquireLeaseRequestMeta {
		range_start: range_start.saturating_add(1),
		range_end,
		epoch: node.ring.current_epoch(),
		holder: node.local_node,
		requested_hlc: HybridTimestamp {
			physical_ms: 25_000,
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
	assert_eq!(meta.status, LeaseStatus::DENIED_INVALID_TARGET);
	assert!(resp.trailing.is_none());
	assert!(node.state_machine.state_snapshot().await.leases.is_empty());

	support::abort_node(&node);
}

#[tokio::test]
async fn status_req_includes_topology_and_committed_lease_metadata() {
	let node = support::bootstrap_node("status-with-lease", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory_from(node.local_node, node.local_node, service);
	let (range_start, range_end) = node
		.ring
		.primary_ranges_for(node.local_node)
		.into_iter()
		.next()
		.unwrap();
	let req = AcquireLeaseRequestMeta {
		range_start,
		range_end,
		epoch: node.ring.current_epoch(),
		holder: node.local_node,
		requested_hlc: HybridTimestamp {
			physical_ms: 30_000,
			logical: 0,
			node_id: node.local_node.id,
		},
		ttl_ms: 250,
		_pad: 0,
	};

	let lease_resp = support::call_frame(
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
	let lease_meta: AcquireLeaseResponseMeta =
		*bytemuck::from_bytes(&lease_resp.meta[..std::mem::size_of::<AcquireLeaseResponseMeta>()]);
	assert_eq!(lease_meta.status, LeaseStatus::GRANTED);

	let status_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::STATUS_REQ,
		&[],
		None,
		None,
	)
	.await
	.unwrap();
	let (status, leases) = decode_status_response(&status_resp);
	assert_eq!(status.epoch, node.ring.current_epoch());
	assert_eq!(status.leader, Some(node.local_node.id));
	assert!(!status.peers.is_empty());
	assert_eq!(leases.len(), 1);
	assert_eq!(leases[0].holder, node.local_node);
	assert_eq!(leases[0].range_start, range_start);
	assert_eq!(leases[0].range_end, range_end);

	support::abort_node(&node);
}

#[tokio::test]
async fn lease_release_req_removes_committed_lease() {
	let node = support::bootstrap_node("lease-release-rpc", 1).await;
	support::wait_for_local_leader(&node).await;
	node.handles.lease_manager.abort();

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory_from(node.local_node, node.local_node, service);
	let (range_start, range_end) = node
		.ring
		.primary_ranges_for(node.local_node)
		.into_iter()
		.next()
		.unwrap();
	let acquire = AcquireLeaseRequestMeta {
		range_start,
		range_end,
		epoch: node.ring.current_epoch(),
		holder: node.local_node,
		requested_hlc: HybridTimestamp {
			physical_ms: 35_000,
			logical: 0,
			node_id: node.local_node.id,
		},
		ttl_ms: 250,
		_pad: 0,
	};

	let acquire_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::LEASE_REQ,
		bytemuck::bytes_of(&acquire),
		None,
		None,
	)
	.await
	.unwrap();
	let acquire_meta: AcquireLeaseResponseMeta = *bytemuck::from_bytes(
		&acquire_resp.meta[..std::mem::size_of::<AcquireLeaseResponseMeta>()],
	);
	assert_eq!(acquire_meta.status, LeaseStatus::GRANTED);

	let release = ReleaseLeaseRequestMeta {
		range_start,
		range_end,
		epoch: node.ring.current_epoch(),
		holder: node.local_node,
	};
	let release_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::LEASE_RELEASE_REQ,
		bytemuck::bytes_of(&release),
		None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(release_resp.header.msg_type, MsgType::LEASE_RELEASE_RESP.0);
	let release_meta: ReleaseLeaseResponseMeta = *bytemuck::from_bytes(
		&release_resp.meta[..std::mem::size_of::<ReleaseLeaseResponseMeta>()],
	);
	assert_eq!(release_meta.status, LeaseStatus::GRANTED);
	assert!(release_resp.trailing.is_none());
	assert!(node.state_machine.state_snapshot().await.leases.is_empty());

	support::abort_node(&node);
}

#[tokio::test]
async fn transfer_leadership_req_routes_to_control_handler() {
	let leader_secret = dev_transfer_signing_secret(1);
	let target_secret = dev_transfer_signing_secret(2);
	let leader_vk = transfer_verifying_key_bytes(leader_secret);
	let target_vk = transfer_verifying_key_bytes(target_secret);
	let mut leader_cfg = NodeBootstrapConfig::dev(
		1,
		support::tempdir("transfer-rpc-leader"),
		"127.0.0.1:0".into(),
	);
	leader_cfg
		.trusted_transfer_signers
		.extend([(1, leader_vk), (2, target_vk)]);
	let leader = WendelonNode::bootstrap_single_node(leader_cfg)
		.await
		.unwrap();
	support::wait_for_local_leader(&leader).await;
	let mut target_cfg = NodeBootstrapConfig::dev(
		2,
		support::tempdir("transfer-rpc-target"),
		"127.0.0.1:1".into(),
	);
	target_cfg
		.trusted_transfer_signers
		.extend([(1, leader_vk), (2, target_vk)]);
	let target = WendelonNode::bootstrap_single_node(target_cfg)
		.await
		.unwrap();

	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let roles = NodeRoleSet::empty()
		.with(NodeRole::Control)
		.with(NodeRole::Data);
	let mut peers = HashMap::new();
	peers.insert(
		leader.local_node,
		PeerCapabilities {
			address: "127.0.0.1:0".into(),
			tiers,
			roles,
		},
	);
	peers.insert(
		target.local_node,
		PeerCapabilities {
			address: "127.0.0.1:1".into(),
			tiers,
			roles,
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (epoch_tx, epoch_rx) = tokio::sync::watch::channel(Arc::new(epoch));

	let target_service: Arc<dyn HrftService> = target.router.clone();
	let (forward_factory, pool) =
		support::loopback_factory_from(leader.local_node, target.local_node, target_service);
	let leader_service: Arc<dyn HrftService> = Arc::new(
		MsgRouterBuilder::default()
			.with_control_ring(Arc::new(WendelonControlServer::new(
				leader.raft.clone(),
				leader.state_machine.clone(),
				leader.local_node,
				epoch_tx.clone(),
				epoch_rx.clone(),
				forward_factory.clone(),
				leader.pool.clone(),
				1,
				LEASE_TTL_MS,
			)))
			.build(),
	);
	let (factory, _) = support::loopback_factory(leader.local_node, leader_service);
	let req = TransferLeadershipRequestMeta {
		source_epoch: 1,
		from_leader: leader.local_node,
		to_leader: target.local_node,
	};
	let expires_at = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|d| d.as_millis() as u64)
		.unwrap_or(0)
		.saturating_add(60_000);
	let token = TransferAuthority::from_secret_bytes(leader.local_node, leader_secret, &{
		let mut trusted = std::collections::BTreeMap::new();
		trusted.insert(leader.local_node.id, leader_vk);
		trusted
	})
	.unwrap()
	.sign_token(TransferToken {
		source_epoch: 1,
		target_epoch: 2,
		from_leader: leader.local_node,
		to_leader: target.local_node,
		expires_at: HybridTimestamp {
			physical_ms: expires_at,
			logical: 0,
			node_id: leader.local_node.id,
		},
		_signed_reserved: 0,
		signature: [0; 64],
		_pad: [0; 8],
	});

	let resp = support::call_frame(
		&factory,
		&pool,
		leader.local_node,
		MsgType::TRANSFER_REQ,
		&wire::encode(&req).unwrap(),
		None,
		Some(bytemuck::bytes_of(&token)),
	)
	.await
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::TRANSFER_RESP.0);

	let meta: TransferLeadershipResponseMeta = wire::decode(&resp.meta).unwrap();
	assert!(meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_GRANTED);
	assert_eq!(
		resp.trailing.as_deref().unwrap(),
		bytemuck::bytes_of(&token)
	);
	support::wait_for_local_leader(&target).await;

	support::abort_node(&leader);
	support::abort_node(&target);
}

#[tokio::test]
async fn advance_epoch_req_routes_to_control_handler() {
	let node = support::bootstrap_node("epoch-rpc", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let batch = epoch_config_batch();
	let arrow = support::encode_batch(&batch).unwrap();

	let resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::EPOCH_ADV_REQ,
		&[],
		Some(&arrow),
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::EPOCH_ADV_RESP.0);

	let meta: AdvanceEpochResponseWire = wire::decode(&resp.meta).unwrap();
	assert!(meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_GRANTED);
	assert_eq!(node.epoch_tx.borrow().epoch, 2);
	assert!(
		!node
			.state_machine
			.state_snapshot()
			.await
			.migrations
			.is_empty()
	);

	support::abort_node(&node);
}

#[tokio::test]
async fn advance_epoch_status_survives_restart() {
	let dir = support::tempdir("epoch-rpc-restart");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		1,
		dir.clone(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let arrow = support::encode_batch(&epoch_config_batch()).unwrap();
	let resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::EPOCH_ADV_REQ,
		&[],
		Some(&arrow),
		None,
	)
	.await
	.unwrap();
	let meta: AdvanceEpochResponseWire = wire::decode(&resp.meta).unwrap();
	assert!(meta.accepted);

	support::abort_node(&node);
	drop(node);
	tokio::time::sleep(std::time::Duration::from_millis(25)).await;

	let recovered =
		WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(1, dir, "127.0.0.1:0".into()))
			.await
			.unwrap();
	support::wait_for_local_leader(&recovered).await;

	let recovered_service: Arc<dyn HrftService> = recovered.router.clone();
	let (recovered_factory, recovered_pool) =
		support::loopback_factory(recovered.local_node, recovered_service);
	let status_resp = support::call_frame(
		&recovered_factory,
		&recovered_pool,
		recovered.local_node,
		MsgType::STATUS_REQ,
		&[],
		None,
		None,
	)
	.await
	.unwrap();
	let (status, _) = decode_status_response(&status_resp);
	assert_eq!(status.epoch, 2);
	assert_eq!(status.leader, Some(recovered.local_node.id));
	assert!(status.peers.iter().any(|peer| peer.node_id == 2));

	support::abort_node(&recovered);
}

#[tokio::test]
async fn advance_epoch_commit_emits_raft_audit_once_across_restart() {
	let dir = support::tempdir("epoch-rpc-audit");
	let audit_path = dir.join("audit.ndjson");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		1,
		dir.clone(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let arrow = support::encode_batch(&epoch_config_batch()).unwrap();
	let resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::EPOCH_ADV_REQ,
		&[],
		Some(&arrow),
		None,
	)
	.await
	.unwrap();
	let meta: AdvanceEpochResponseWire = wire::decode(&resp.meta).unwrap();
	assert!(meta.accepted);

	let audit = wait_for_audit_event(&audit_path, "control_ring.epoch_advanced").await;
	assert!(audit.contains("\"authority\":\"raft_log\""));
	assert!(audit.contains("raft_log_index="));
	assert!(audit.contains("from_epoch=1 to_epoch=2"));
	assert!(audit.contains("to_peers=[1,2]"));
	assert_eq!(
		audit
			.matches("\"event_type\":\"control_ring.epoch_advanced\"")
			.count(),
		1
	);

	node.handles.abort();
	drop(node);
	tokio::time::sleep(std::time::Duration::from_millis(25)).await;

	let recovered =
		WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(1, dir, "127.0.0.1:0".into()))
			.await
			.unwrap();
	support::wait_for_local_leader(&recovered).await;

	let recovered_audit = read_audit_log(&audit_path);
	assert_eq!(
		recovered_audit
			.matches("\"event_type\":\"control_ring.epoch_advanced\"")
			.count(),
		1
	);

	recovered.handles.abort();
}
