#[path = "support/control_ring.rs"]
mod support;

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use wendelon::consensus::{AdvanceEpochRecord, EpochConfig, EpochPeer, WendelonRequest};
use wendelon::network::control_server::{AdvanceEpochResponseWire, CONTROL_REASON_GRANTED};
use wendelon::network::server::HrftService;
use wendelon::network::transport::{
	MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportTier, TransportTierSet,
};
use wendelon::network::wire;
use wendelon::schema::{MsgType, NodeId};

fn data_control_bits() -> (u8, u8) {
	(
		TransportTierSet::empty()
			.with(TransportTier::QuicIoUring)
			.as_u8(),
		NodeRoleSet::empty()
			.with(NodeRole::Data)
			.with(NodeRole::Control)
			.as_u8(),
	)
}

fn epoch_two_members() -> MembershipEpoch {
	let (tier_bits, role_bits) = data_control_bits();
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		NodeId {
			id: 1,
			_reserved: 0,
		},
		PeerCapabilities {
			address: "127.0.0.1:0".into(),
			tiers: TransportTierSet::from_u8(tier_bits),
			roles: NodeRoleSet::from_u8(role_bits),
		},
	);
	peers.insert(
		NodeId {
			id: 2,
			_reserved: 0,
		},
		PeerCapabilities {
			address: "127.0.0.1:1".into(),
			tiers: TransportTierSet::from_u8(tier_bits),
			roles: NodeRoleSet::from_u8(role_bits),
		},
	);
	MembershipEpoch { epoch: 2, peers }
}

fn epoch_config(epoch: &MembershipEpoch) -> EpochConfig {
	let mut peers = BTreeMap::new();
	for (node_id, caps) in &epoch.peers {
		peers.insert(
			node_id.id,
			EpochPeer {
				node_id: *node_id,
				address: caps.address.clone(),
				tier_bits: caps.tiers.as_u8(),
				role_bits: caps.roles.as_u8(),
			},
		);
	}
	EpochConfig {
		epoch: epoch.epoch,
		peers,
	}
}

async fn wait_for_seeded_epoch(node: &wendelon::node::WendelonNode, epoch: u64) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
	loop {
		let snapshot = node.state_machine.state_snapshot().await;
		if node.epoch_tx.borrow().epoch == epoch && !snapshot.migrations.is_empty() {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for seeded epoch {epoch}; visible={}, migrations={}",
			node.epoch_tx.borrow().epoch,
			snapshot.migrations.len()
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

#[tokio::test]
async fn advance_epoch_rpc_auto_seeds_reassigned_ranges() {
	let node = support::bootstrap_node("epoch-seed-rpc", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let (tier_bits, role_bits) = data_control_bits();
	let batch = support::epoch_config_batch(
		&[
			(1, "127.0.0.1:0", tier_bits, role_bits),
			(2, "127.0.0.1:1", tier_bits, role_bits),
		],
		&[1],
		&[2],
	);
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
	let meta: AdvanceEpochResponseWire = wire::decode(&resp.meta).unwrap();
	assert!(meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_GRANTED);

	wait_for_seeded_epoch(&node, 2).await;
	let snapshot = node.state_machine.state_snapshot().await;
	assert_eq!(snapshot.epoch_history.len(), 2);
	assert!(!snapshot.migrations.is_empty());
	for entry in snapshot.migrations.values() {
		assert_eq!(entry.from_epoch, 1);
		assert_eq!(entry.to_epoch, 2);
		assert_eq!(entry.logical_owner.id, 2);
		let sources: Vec<u32> = entry.source_replicas.iter().map(|node| node.id).collect();
		assert_eq!(sources, vec![1]);
	}

	support::abort_node(&node);
}

#[tokio::test]
async fn replayed_committed_epoch_reconciles_missing_seed_batch_idempotently() {
	let node = support::bootstrap_node("epoch-seed-replay", 1).await;
	support::wait_for_local_leader(&node).await;

	let current = node.epoch_tx.borrow().clone();
	let next = epoch_two_members();
	let record = AdvanceEpochRecord {
		from: epoch_config(&current),
		to: epoch_config(&next),
	};

	node.raft
		.client_write(WendelonRequest::AdvanceEpoch(record.clone()))
		.await
		.unwrap();
	node.raft
		.client_write(WendelonRequest::AdvanceEpoch(record))
		.await
		.unwrap();

	wait_for_seeded_epoch(&node, 2).await;
	let snapshot = node.state_machine.state_snapshot().await;
	assert_eq!(snapshot.epoch_history.len(), 2);
	let ids: HashSet<u64> = snapshot.migrations.keys().copied().collect();
	assert_eq!(ids.len(), snapshot.migrations.len());
	for entry in snapshot.migrations.values() {
		assert_eq!(entry.from_epoch, 1);
		assert_eq!(entry.to_epoch, 2);
		assert_eq!(entry.logical_owner.id, 2);
	}

	support::abort_node(&node);
}
