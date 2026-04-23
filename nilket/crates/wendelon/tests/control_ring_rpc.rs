#[path = "support/control_ring.rs"]
mod support;

use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wendelon::client::ClusterStatusWire;
use wendelon::network::control_server::{
	AdvanceEpochResponseWire, AuditChainHeadResponseWire, CONTROL_REASON_AUTHORIZATION_FAILED,
	CONTROL_REASON_GRANTED, RevocationAdminRequestWire, RevocationAdminResponseWire,
	TenantAdminRequestWire, TenantAdminResponseWire, WendelonControlServer,
};
use wendelon::network::router::MsgRouterBuilder;
use wendelon::network::server::{HrftService, RequestContext};
use wendelon::network::tls::{ActorIdentity, ActorRole, ActorTrustDomain, BreakGlassScope};
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
	LeaseStatus, MsgType, NodeId, ReleaseLeaseRequestMeta, ReleaseLeaseResponseMeta, TenantId,
	TransferToken,
};

fn actor(role: ActorRole, tenant_id: Option<&str>) -> ActorIdentity {
	ActorIdentity::new(
		role,
		None,
		tenant_id.map(str::to_string),
		vec!["cluster".to_string()],
		vec![1, 2, 3, 4],
	)
	.unwrap()
}

fn break_glass_actor(scope: BreakGlassScope) -> ActorIdentity {
	ActorIdentity::new(
		ActorRole::BreakGlassAdmin,
		None,
		None,
		vec![scope.as_str().to_string()],
		vec![4, 3, 2, 1],
	)
	.unwrap()
}

fn context(actor: ActorIdentity, remote: NodeId) -> RequestContext {
	RequestContext { remote, actor }
}

fn tenant_admin_frame(
	node: &WendelonNode,
	stream_id: u32,
	request: &TenantAdminRequestWire,
) -> wendelon::network::core::HrftFrame {
	let meta = wire::encode_into(&node.pool, request).unwrap();
	wendelon::network::core::HrftFrame {
		header: wendelon::schema::FrameHeader {
			magic: wendelon::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TENANT_ADMIN_REQ.0,
			flags: 0,
			stream_id,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta,
		arrow_payload: None,
		trailing: None,
	}
}

fn decode_tenant_admin_response(
	resp: &wendelon::network::core::HrftFrame,
) -> TenantAdminResponseWire {
	wire::decode(&resp.meta).unwrap()
}

fn revocation_admin_frame(
	node: &WendelonNode,
	stream_id: u32,
	request: &RevocationAdminRequestWire,
) -> wendelon::network::core::HrftFrame {
	let meta = wire::encode_into(&node.pool, request).unwrap();
	wendelon::network::core::HrftFrame {
		header: wendelon::schema::FrameHeader {
			magic: wendelon::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::REVOCATION_ADMIN_REQ.0,
			flags: 0,
			stream_id,
			meta_len: meta.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta,
		arrow_payload: None,
		trailing: None,
	}
}

fn decode_revocation_admin_response(
	resp: &wendelon::network::core::HrftFrame,
) -> RevocationAdminResponseWire {
	wire::decode(&resp.meta).unwrap()
}

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

fn audit_chain_head_frame(stream_id: u32) -> wendelon::network::core::HrftFrame {
	wendelon::network::core::HrftFrame {
		header: wendelon::schema::FrameHeader {
			magic: wendelon::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::AUDIT_CHAIN_HEAD_REQ.0,
			flags: 0,
			stream_id,
			meta_len: 0,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: wendelon::network::pool::BufPool::new(1, 16).acquire(),
		arrow_payload: None,
		trailing: None,
	}
}

fn status_frame(stream_id: u32) -> wendelon::network::core::HrftFrame {
	wendelon::network::core::HrftFrame {
		header: wendelon::schema::FrameHeader {
			magic: wendelon::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::STATUS_REQ.0,
			flags: 0,
			stream_id,
			meta_len: 0,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: wendelon::network::pool::BufPool::new(1, 16).acquire(),
		arrow_payload: None,
		trailing: None,
	}
}

fn decode_audit_chain_head_response(
	resp: &wendelon::network::core::HrftFrame,
) -> AuditChainHeadResponseWire {
	wire::decode(&resp.meta).unwrap()
}

fn advance_epoch_frame(
	node: &WendelonNode,
	stream_id: u32,
	batch: &RecordBatch,
) -> wendelon::network::core::HrftFrame {
	let arrow = support::encode_batch(batch).unwrap();
	let mut arrow_payload = node.pool.acquire();
	arrow_payload.as_mut_vec().extend_from_slice(&arrow);
	wendelon::network::core::HrftFrame {
		header: wendelon::schema::FrameHeader {
			magic: wendelon::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::EPOCH_ADV_REQ.0,
			flags: 0,
			stream_id,
			meta_len: 0,
			arrow_len: arrow_payload.len() as u32,
			trailing_len: 0,
		},
		meta: node.pool.acquire(),
		arrow_payload: Some(arrow_payload),
		trailing: None,
	}
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

	let cluster_admin_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::ClusterAdmin, None), node.local_node),
			status_frame(2),
		)
		.await
		.unwrap()
		.unwrap();
	let (cluster_admin_status, cluster_admin_leases) = decode_status_response(&cluster_admin_resp);
	assert_eq!(cluster_admin_status.epoch, status.epoch);
	assert_eq!(cluster_admin_status.leader, status.leader);
	assert_eq!(cluster_admin_leases, leases);

	support::abort_node(&node);
}

#[tokio::test]
async fn audit_chain_head_req_returns_local_head_for_operator_auditor_and_observer() {
	let node = support::bootstrap_node("audit-chain-head-rpc", 1).await;
	support::wait_for_local_leader(&node).await;

	let operator_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::Operator, None), node.local_node),
			audit_chain_head_frame(1),
		)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		operator_resp.header.msg_type,
		MsgType::AUDIT_CHAIN_HEAD_RESP.0
	);
	assert!(operator_resp.arrow_payload.is_none());
	assert!(operator_resp.trailing.is_none());
	let operator_meta = decode_audit_chain_head_response(&operator_resp);
	let expected = node.audit_sink.chain_head().unwrap();
	assert_eq!(operator_meta.head, expected);

	let observer_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::ReadOnlyObserver, None), node.local_node),
			audit_chain_head_frame(2),
		)
		.await
		.unwrap()
		.unwrap();
	let observer_meta = decode_audit_chain_head_response(&observer_resp);
	assert_eq!(observer_meta.head, expected);

	let auditor_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::ComplianceAuditor, None), node.local_node),
			audit_chain_head_frame(3),
		)
		.await
		.unwrap()
		.unwrap();
	let auditor_meta = decode_audit_chain_head_response(&auditor_resp);
	assert_eq!(auditor_meta.head, expected);

	support::abort_node(&node);
}

#[tokio::test]
async fn audit_chain_head_req_rejects_data_node_actor() {
	let node = support::bootstrap_node("audit-chain-head-authz", 1).await;
	support::wait_for_local_leader(&node).await;

	let err = node
		.router
		.handle_frame(
			context(ActorIdentity::data_node(node.local_node), node.local_node),
			audit_chain_head_frame(1),
		)
		.await
		.unwrap_err();
	assert_eq!(err.kind(), ErrorKind::PermissionDenied);

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
				leader.crypto.clone(),
				leader.local_node,
				epoch_tx.clone(),
				epoch_rx.clone(),
				forward_factory.clone(),
				leader.pool.clone(),
				1,
				LEASE_TTL_MS,
				leader.audit_sink.clone(),
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

#[tokio::test]
async fn tenant_admin_create_and_list_flow_commits_through_control_ring() {
	let node = support::bootstrap_node("tenant-admin-create", 1).await;
	support::wait_for_local_leader(&node).await;

	let create_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::TenantAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				1,
				&TenantAdminRequestWire::Create {
					target_tenant_id: TenantId::new(7),
					quota_bytes: 4_096,
					replication_factor: 1,
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let create_meta = decode_tenant_admin_response(&create_resp);
	assert!(create_meta.accepted);
	assert_eq!(create_meta.reason_code, CONTROL_REASON_GRANTED);
	assert_eq!(create_meta.tenant.unwrap().tenant_id, TenantId::new(7));

	let snapshot = node.state_machine.state_snapshot().await;
	assert!(snapshot.tenants.contains_key(&TenantId::new(7)));

	let list_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::ReadOnlyObserver, None), node.local_node),
			tenant_admin_frame(&node, 2, &TenantAdminRequestWire::List),
		)
		.await
		.unwrap()
		.unwrap();
	let list_meta = decode_tenant_admin_response(&list_resp);
	assert!(list_meta.accepted);
	assert!(
		list_meta
			.tenants
			.iter()
			.any(|tenant| tenant.tenant_id == TenantId::new(7))
	);

	support::abort_node(&node);
}

#[tokio::test]
async fn tenant_admin_rotate_tenant_kek_commits_and_returns_new_active_version() {
	let node = support::bootstrap_node("tenant-admin-rotate-kek", 1).await;
	support::wait_for_local_leader(&node).await;

	let create_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::TenantAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				1,
				&TenantAdminRequestWire::Create {
					target_tenant_id: TenantId::new(7),
					quota_bytes: 4_096,
					replication_factor: 1,
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let create_meta = decode_tenant_admin_response(&create_resp);
	let old_kek_id = create_meta
		.tenant
		.as_ref()
		.and_then(|tenant| tenant.active_tenant_kek_id)
		.expect("created tenant should report an active tenant_kek_id");

	let rotate_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::BackupAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				2,
				&TenantAdminRequestWire::RotateTenantKek {
					target_tenant_id: TenantId::new(7),
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let rotate_meta = decode_tenant_admin_response(&rotate_resp);
	assert!(rotate_meta.accepted);
	assert_eq!(rotate_meta.reason_code, CONTROL_REASON_GRANTED);
	let new_kek_id = rotate_meta
		.tenant
		.as_ref()
		.and_then(|tenant| tenant.active_tenant_kek_id)
		.expect("rotated tenant should report a new active tenant_kek_id");
	assert_ne!(new_kek_id, old_kek_id);
	let tenant = rotate_meta.tenant.as_ref().unwrap();
	assert!(tenant.tenant_kek_accounting_complete);
	let old_version = tenant
		.tenant_kek_versions
		.iter()
		.find(|version| version.tenant_kek_id == old_kek_id)
		.expect("historical KEK version should remain visible");
	assert_eq!(old_version.tiered_slab_ref_count, 0);
	assert!(old_version.safe_to_retire);
	let active_version = tenant
		.tenant_kek_versions
		.iter()
		.find(|version| version.tenant_kek_id == new_kek_id)
		.expect("active KEK version should be visible");
	assert!(active_version.is_active);
	assert!(!active_version.safe_to_retire);

	let snapshot = node.state_machine.state_snapshot().await;
	let keyset = snapshot.tenant_keysets.get(&TenantId::new(7)).unwrap();
	assert_eq!(keyset.active_tenant_kek_id, Some(new_kek_id));
	assert!(keyset.keys.contains_key(&old_kek_id));
	assert!(keyset.keys.contains_key(&new_kek_id));
	assert_eq!(
		node.crypto.active_tenant_kek_id(TenantId::new(7)).unwrap(),
		new_kek_id
	);

	support::abort_node(&node);
}

#[tokio::test]
async fn tenant_admin_retire_tenant_kek_removes_safe_historical_version() {
	let node = support::bootstrap_node("tenant-admin-retire-kek", 1).await;
	support::wait_for_local_leader(&node).await;

	let create_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::TenantAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				1,
				&TenantAdminRequestWire::Create {
					target_tenant_id: TenantId::new(7),
					quota_bytes: 4_096,
					replication_factor: 1,
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let create_meta = decode_tenant_admin_response(&create_resp);
	let retired_kek_id = create_meta
		.tenant
		.as_ref()
		.and_then(|tenant| tenant.active_tenant_kek_id)
		.expect("created tenant should report an active tenant_kek_id");

	let rotate_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::BackupAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				2,
				&TenantAdminRequestWire::RotateTenantKek {
					target_tenant_id: TenantId::new(7),
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let rotate_meta = decode_tenant_admin_response(&rotate_resp);
	assert!(rotate_meta.accepted);
	let active_kek_id = rotate_meta
		.tenant
		.as_ref()
		.and_then(|tenant| tenant.active_tenant_kek_id)
		.expect("rotated tenant should report an active tenant_kek_id");
	assert_ne!(active_kek_id, retired_kek_id);

	let retire_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::BackupAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				3,
				&TenantAdminRequestWire::RetireTenantKek {
					target_tenant_id: TenantId::new(7),
					tenant_kek_id: retired_kek_id,
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let retire_meta = decode_tenant_admin_response(&retire_resp);
	assert!(retire_meta.accepted);
	let tenant = retire_meta.tenant.as_ref().unwrap();
	assert_eq!(tenant.active_tenant_kek_id, Some(active_kek_id));
	assert!(
		tenant
			.tenant_kek_versions
			.iter()
			.all(|version| version.tenant_kek_id != retired_kek_id)
	);
	assert!(
		tenant
			.tenant_kek_versions
			.iter()
			.any(|version| version.tenant_kek_id == active_kek_id && version.is_active)
	);

	let snapshot = node.state_machine.state_snapshot().await;
	let keyset = snapshot.tenant_keysets.get(&TenantId::new(7)).unwrap();
	assert_eq!(keyset.active_tenant_kek_id, Some(active_kek_id));
	assert!(!keyset.keys.contains_key(&retired_kek_id));
	assert!(keyset.keys.contains_key(&active_kek_id));

	let local_keysets = node.crypto.tenant_keysets_snapshot().unwrap();
	let local_keyset = local_keysets.get(&TenantId::new(7)).unwrap();
	assert_eq!(local_keyset.active_tenant_kek_id, Some(active_kek_id));
	assert!(!local_keyset.keys.contains_key(&retired_kek_id));
	assert!(local_keyset.keys.contains_key(&active_kek_id));

	support::abort_node(&node);
}

#[tokio::test]
async fn tenant_delete_requires_break_glass_certificate() {
	let node = support::bootstrap_node("tenant-admin-delete-break-glass", 1).await;
	support::wait_for_local_leader(&node).await;

	let create_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::TenantAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				1,
				&TenantAdminRequestWire::Create {
					target_tenant_id: TenantId::new(7),
					quota_bytes: 4_096,
					replication_factor: 1,
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	assert!(decode_tenant_admin_response(&create_resp).accepted);

	let denied_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::TenantAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				2,
				&TenantAdminRequestWire::Delete {
					target_tenant_id: TenantId::new(7),
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let denied_meta = decode_tenant_admin_response(&denied_resp);
	assert!(!denied_meta.accepted);
	assert_eq!(denied_meta.reason_code, CONTROL_REASON_AUTHORIZATION_FAILED);

	let narrow_scope_resp = node
		.router
		.handle_frame(
			context(
				break_glass_actor(BreakGlassScope::Diagnostic),
				node.local_node,
			),
			tenant_admin_frame(
				&node,
				3,
				&TenantAdminRequestWire::Delete {
					target_tenant_id: TenantId::new(7),
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let narrow_scope_meta = decode_tenant_admin_response(&narrow_scope_resp);
	assert!(!narrow_scope_meta.accepted);
	assert_eq!(
		narrow_scope_meta.reason_code,
		CONTROL_REASON_AUTHORIZATION_FAILED
	);

	let delete_resp = node
		.router
		.handle_frame(
			context(break_glass_actor(BreakGlassScope::Full), node.local_node),
			tenant_admin_frame(
				&node,
				4,
				&TenantAdminRequestWire::Delete {
					target_tenant_id: TenantId::new(7),
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let delete_meta = decode_tenant_admin_response(&delete_resp);
	assert!(delete_meta.accepted);
	let snapshot = node.state_machine.state_snapshot().await;
	let tenant = snapshot.tenants.get(&TenantId::new(7)).unwrap();
	assert_eq!(
		tenant.state,
		wendelon::consensus::TenantLifecycleState::Deleted
	);
	assert_eq!(tenant.quota_bytes, 0);
	assert!(!snapshot.tenant_keysets.contains_key(&TenantId::new(7)));

	support::abort_node(&node);
}

#[tokio::test]
async fn tenant_admin_rejects_mutation_for_read_only_or_tenant_scoped_actor() {
	let node = support::bootstrap_node("tenant-admin-authz", 1).await;
	support::wait_for_local_leader(&node).await;

	let observer_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::ReadOnlyObserver, None), node.local_node),
			tenant_admin_frame(
				&node,
				1,
				&TenantAdminRequestWire::Suspend {
					target_tenant_id: TenantId::ZERO,
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let observer_meta = decode_tenant_admin_response(&observer_resp);
	assert!(!observer_meta.accepted);
	assert_eq!(
		observer_meta.reason_code,
		CONTROL_REASON_AUTHORIZATION_FAILED
	);

	let tenant_scoped_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::Operator, Some("7")), node.local_node),
			tenant_admin_frame(&node, 2, &TenantAdminRequestWire::List),
		)
		.await
		.unwrap()
		.unwrap();
	let tenant_scoped_meta = decode_tenant_admin_response(&tenant_scoped_resp);
	assert!(!tenant_scoped_meta.accepted);
	assert_eq!(
		tenant_scoped_meta.reason_code,
		CONTROL_REASON_AUTHORIZATION_FAILED
	);

	let cluster_admin_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::ClusterAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				3,
				&TenantAdminRequestWire::Suspend {
					target_tenant_id: TenantId::ZERO,
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let cluster_admin_meta = decode_tenant_admin_response(&cluster_admin_resp);
	assert!(!cluster_admin_meta.accepted);
	assert_eq!(
		cluster_admin_meta.reason_code,
		CONTROL_REASON_AUTHORIZATION_FAILED
	);

	let tenant_admin_rotate_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::TenantAdmin, None), node.local_node),
			tenant_admin_frame(
				&node,
				4,
				&TenantAdminRequestWire::RotateTenantKek {
					target_tenant_id: TenantId::ZERO,
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let tenant_admin_rotate_meta = decode_tenant_admin_response(&tenant_admin_rotate_resp);
	assert!(!tenant_admin_rotate_meta.accepted);
	assert_eq!(
		tenant_admin_rotate_meta.reason_code,
		CONTROL_REASON_AUTHORIZATION_FAILED
	);

	support::abort_node(&node);
}

#[tokio::test]
async fn advance_epoch_rejects_tenant_scoped_operator() {
	let node = support::bootstrap_node("epoch-rpc-tenant-scoped-authz", 1).await;
	support::wait_for_local_leader(&node).await;

	let resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::Operator, Some("7")), node.local_node),
			advance_epoch_frame(&node, 1, &epoch_config_batch()),
		)
		.await
		.unwrap()
		.unwrap();
	let meta: AdvanceEpochResponseWire = wire::decode(&resp.meta).unwrap();
	assert!(!meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_AUTHORIZATION_FAILED);
	assert_eq!(node.epoch_tx.borrow().epoch, 1);

	support::abort_node(&node);
}

#[tokio::test]
async fn revocation_admin_commits_and_lists_replicated_revocations() {
	let node = support::bootstrap_node("revocation-admin", 1).await;
	support::wait_for_local_leader(&node).await;

	let revoke_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::ClusterAdmin, None), node.local_node),
			revocation_admin_frame(
				&node,
				1,
				&RevocationAdminRequestWire::Revoke {
					trust_domain: ActorTrustDomain::Ops,
					key_id: vec![9, 9, 9, 9],
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let revoke_meta = decode_revocation_admin_response(&revoke_resp);
	assert!(revoke_meta.accepted);
	assert_eq!(revoke_meta.reason_code, CONTROL_REASON_GRANTED);
	assert!(revoke_meta.entries.iter().any(
		|entry| entry.trust_domain == ActorTrustDomain::Ops && entry.key_id == vec![9, 9, 9, 9]
	));

	let snapshot = node.state_machine.state_snapshot().await;
	assert!(
		snapshot
			.revoked_actor_keys
			.iter()
			.any(|(entry, _)| entry.trust_domain == ActorTrustDomain::Ops
				&& entry.key_id == vec![9, 9, 9, 9])
	);

	let list_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::ClusterAdmin, None), node.local_node),
			revocation_admin_frame(&node, 2, &RevocationAdminRequestWire::List),
		)
		.await
		.unwrap()
		.unwrap();
	let list_meta = decode_revocation_admin_response(&list_resp);
	assert!(list_meta.accepted);
	assert!(list_meta.entries.iter().any(
		|entry| entry.trust_domain == ActorTrustDomain::Ops && entry.key_id == vec![9, 9, 9, 9]
	));

	let denied_err = node
		.router
		.handle_frame(
			context(actor(ActorRole::TenantAdmin, None), node.local_node),
			revocation_admin_frame(&node, 3, &RevocationAdminRequestWire::List),
		)
		.await
		.unwrap_err();
	assert_eq!(denied_err.kind(), ErrorKind::PermissionDenied);

	support::abort_node(&node);
}

#[tokio::test]
async fn revocation_admin_rejects_non_ops_and_tenant_scoped_callers() {
	let node = support::bootstrap_node("revocation-admin-authz", 1).await;
	support::wait_for_local_leader(&node).await;

	let data_node_resp = node
		.router
		.handle_frame(
			context(ActorIdentity::data_node(node.local_node), node.local_node),
			revocation_admin_frame(&node, 1, &RevocationAdminRequestWire::List),
		)
		.await
		.unwrap()
		.unwrap();
	let data_node_meta = decode_revocation_admin_response(&data_node_resp);
	assert!(!data_node_meta.accepted);
	assert_eq!(
		data_node_meta.reason_code,
		CONTROL_REASON_AUTHORIZATION_FAILED
	);

	let tenant_scoped_resp = node
		.router
		.handle_frame(
			context(actor(ActorRole::Operator, Some("7")), node.local_node),
			revocation_admin_frame(
				&node,
				2,
				&RevocationAdminRequestWire::Revoke {
					trust_domain: ActorTrustDomain::Ops,
					key_id: vec![1, 2, 3, 4],
				},
			),
		)
		.await
		.unwrap()
		.unwrap();
	let tenant_scoped_meta = decode_revocation_admin_response(&tenant_scoped_resp);
	assert!(!tenant_scoped_meta.accepted);
	assert_eq!(
		tenant_scoped_meta.reason_code,
		CONTROL_REASON_AUTHORIZATION_FAILED
	);

	support::abort_node(&node);
}
