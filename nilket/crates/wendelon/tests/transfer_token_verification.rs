#[path = "support/control_ring.rs"]
mod support;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wendelon::network::control_server::{
	CONTROL_REASON_GRANTED, CONTROL_REASON_INVALID_TOKEN, WendelonControlServer,
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
use wendelon::schema::{HybridTimestamp, MsgType, NodeId, TransferToken};

fn transfer_bootstrap_cfg(
	tag: &str,
	node_id: u32,
	trusted_transfer_signers: BTreeMap<u32, [u8; 32]>,
) -> NodeBootstrapConfig {
	let mut cfg = NodeBootstrapConfig::dev(
		node_id,
		support::tempdir(tag),
		format!("127.0.0.1:{node_id}"),
	);
	cfg.trusted_transfer_signers = trusted_transfer_signers;
	cfg
}

async fn build_transfer_fixture() -> (
	WendelonNode,
	WendelonNode,
	Arc<wendelon::network::factory::TransportProviderFactory>,
	Arc<wendelon::network::pool::BufPool>,
	[u8; 32],
	[u8; 32],
) {
	let leader_secret = dev_transfer_signing_secret(201);
	let target_secret = dev_transfer_signing_secret(202);
	let leader_vk = transfer_verifying_key_bytes(leader_secret);
	let target_vk = transfer_verifying_key_bytes(target_secret);

	let leader = WendelonNode::bootstrap_single_node(transfer_bootstrap_cfg(
		"transfer-token-leader",
		201,
		BTreeMap::from([(201, leader_vk), (202, target_vk)]),
	))
	.await
	.unwrap();
	support::wait_for_local_leader(&leader).await;

	let target = WendelonNode::bootstrap_single_node(transfer_bootstrap_cfg(
		"transfer-token-target",
		202,
		BTreeMap::from([(201, leader_vk), (202, target_vk)]),
	))
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
				epoch_tx,
				epoch_rx,
				forward_factory,
				leader.pool.clone(),
				1,
				LEASE_TTL_MS,
			)))
			.build(),
	);
	let (factory, _) = support::loopback_factory(leader.local_node, leader_service);

	(leader, target, factory, pool, leader_secret, target_secret)
}

fn expires_at(node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map(|d| d.as_millis() as u64)
			.unwrap_or(0)
			.saturating_add(60_000),
		logical: 0,
		node_id,
	}
}

fn req(from_leader: NodeId, to_leader: NodeId) -> TransferLeadershipRequestMeta {
	TransferLeadershipRequestMeta {
		source_epoch: 1,
		from_leader,
		to_leader,
	}
}

fn base_token(from_leader: NodeId, to_leader: NodeId) -> TransferToken {
	TransferToken {
		source_epoch: 1,
		target_epoch: 2,
		from_leader,
		to_leader,
		expires_at: expires_at(from_leader.id),
		_signed_reserved: 0,
		signature: [0; 64],
		_pad: [0; 8],
	}
}

async fn call_transfer(
	factory: &Arc<wendelon::network::factory::TransportProviderFactory>,
	pool: &Arc<wendelon::network::pool::BufPool>,
	target: NodeId,
	req: &TransferLeadershipRequestMeta,
	token: Option<&TransferToken>,
) -> TransferLeadershipResponseMeta {
	let resp = support::call_frame(
		factory,
		pool,
		target,
		MsgType::TRANSFER_REQ,
		&wire::encode(req).unwrap(),
		None,
		token.map(bytemuck::bytes_of),
	)
	.await
	.unwrap();
	wire::decode(&resp.meta).unwrap()
}

#[tokio::test]
async fn valid_signed_transfer_token_is_accepted() {
	let (leader, target, factory, pool, leader_secret, _target_secret) =
		build_transfer_fixture().await;
	let signer = TransferAuthority::from_secret_bytes(
		leader.local_node,
		leader_secret,
		&BTreeMap::from([(
			leader.local_node.id,
			transfer_verifying_key_bytes(leader_secret),
		)]),
	)
	.unwrap();
	let token = signer.sign_token(base_token(leader.local_node, target.local_node));

	let meta = call_transfer(
		&factory,
		&pool,
		leader.local_node,
		&req(leader.local_node, target.local_node),
		Some(&token),
	)
	.await;
	assert!(meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_GRANTED);

	support::abort_node(&leader);
	support::abort_node(&target);
}

#[tokio::test]
async fn missing_transfer_token_is_rejected() {
	let (leader, target, factory, pool, _leader_secret, _target_secret) =
		build_transfer_fixture().await;

	let meta = call_transfer(
		&factory,
		&pool,
		leader.local_node,
		&req(leader.local_node, target.local_node),
		None,
	)
	.await;
	assert!(!meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_INVALID_TOKEN);

	support::abort_node(&leader);
	support::abort_node(&target);
}

#[tokio::test]
async fn tampered_transfer_token_is_rejected() {
	let (leader, target, factory, pool, leader_secret, _target_secret) =
		build_transfer_fixture().await;
	let signer = TransferAuthority::from_secret_bytes(
		leader.local_node,
		leader_secret,
		&BTreeMap::from([(
			leader.local_node.id,
			transfer_verifying_key_bytes(leader_secret),
		)]),
	)
	.unwrap();
	let mut token = signer.sign_token(base_token(leader.local_node, target.local_node));
	token.to_leader = NodeId {
		id: 999,
		_reserved: 0,
	};

	let meta = call_transfer(
		&factory,
		&pool,
		leader.local_node,
		&req(leader.local_node, target.local_node),
		Some(&token),
	)
	.await;
	assert!(!meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_INVALID_TOKEN);

	support::abort_node(&leader);
	support::abort_node(&target);
}

#[tokio::test]
async fn unauthorized_signer_transfer_token_is_rejected() {
	let (leader, target, factory, pool, leader_secret, target_secret) =
		build_transfer_fixture().await;
	let bad_signer = TransferAuthority::from_secret_bytes(
		leader.local_node,
		target_secret,
		&BTreeMap::from([(
			leader.local_node.id,
			transfer_verifying_key_bytes(target_secret),
		)]),
	)
	.unwrap();
	let token = bad_signer.sign_token(base_token(leader.local_node, target.local_node));

	let meta = call_transfer(
		&factory,
		&pool,
		leader.local_node,
		&req(leader.local_node, target.local_node),
		Some(&token),
	)
	.await;
	assert!(!meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_INVALID_TOKEN);
	assert_ne!(leader_secret, target_secret);

	support::abort_node(&leader);
	support::abort_node(&target);
}

#[tokio::test]
async fn direct_target_call_without_leader_peer_identity_is_rejected() {
	let (leader, target, _factory, _pool, leader_secret, _target_secret) =
		build_transfer_fixture().await;
	let signer = TransferAuthority::from_secret_bytes(
		leader.local_node,
		leader_secret,
		&BTreeMap::from([(
			leader.local_node.id,
			transfer_verifying_key_bytes(leader_secret),
		)]),
	)
	.unwrap();
	let token = signer.sign_token(base_token(leader.local_node, target.local_node));
	let target_service: Arc<dyn wendelon::network::server::HrftService> = target.router.clone();
	let attacker = NodeId {
		id: 999,
		_reserved: 0,
	};
	let (factory, pool) =
		support::loopback_factory_from(attacker, target.local_node, target_service);

	let meta = call_transfer(
		&factory,
		&pool,
		target.local_node,
		&req(leader.local_node, target.local_node),
		Some(&token),
	)
	.await;
	assert!(!meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_INVALID_TOKEN);

	support::abort_node(&leader);
	support::abort_node(&target);
}
