#[path = "support/control_ring.rs"]
mod support;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::client::ClusterStatusWire;
use wendelon::network::control_server::{
	AdvanceEpochResponseWire, CONTROL_REASON_GRANTED, CONTROL_REASON_UNSUPPORTED,
};
use wendelon::network::server::HrftService;
use wendelon::network::traits::{TransferLeadershipRequestMeta, TransferLeadershipResponseMeta};
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::network::wire;
use wendelon::schema::{
	AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, HybridTimestamp, LeaseDescriptor,
	LeaseStatus, MsgType, NodeId,
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
	let (factory, pool) = support::loopback_factory(node.local_node, service);
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
async fn overlapping_remote_lease_requests_race_through_committed_path() {
	let node = support::bootstrap_node("lease-rpc-race", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let (range_start, range_end) = node
		.ring
		.primary_ranges_for(node.local_node)
		.into_iter()
		.next()
		.unwrap();

	let req_a = AcquireLeaseRequestMeta {
		range_start,
		range_end,
		epoch: node.ring.current_epoch(),
		holder: node.local_node,
		requested_hlc: HybridTimestamp {
			physical_ms: 20_000,
			logical: 0,
			node_id: node.local_node.id,
		},
		ttl_ms: 250,
		_pad: 0,
	};
	let req_b = AcquireLeaseRequestMeta {
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

	let (resp_a, resp_b) = tokio::join!(
		support::call_frame(
			&factory,
			&pool,
			node.local_node,
			MsgType::LEASE_REQ,
			bytemuck::bytes_of(&req_a),
			None,
			None,
		),
		support::call_frame(
			&factory,
			&pool,
			node.local_node,
			MsgType::LEASE_REQ,
			bytemuck::bytes_of(&req_b),
			None,
			None,
		),
	);
	let resp_a = resp_a.unwrap();
	let resp_b = resp_b.unwrap();

	let meta_a: AcquireLeaseResponseMeta =
		*bytemuck::from_bytes(&resp_a.meta[..std::mem::size_of::<AcquireLeaseResponseMeta>()]);
	let meta_b: AcquireLeaseResponseMeta =
		*bytemuck::from_bytes(&resp_b.meta[..std::mem::size_of::<AcquireLeaseResponseMeta>()]);

	let statuses = [meta_a.status, meta_b.status];
	assert_eq!(
		statuses
			.iter()
			.filter(|status| **status == LeaseStatus::GRANTED)
			.count(),
		1
	);
	assert_eq!(
		statuses
			.iter()
			.filter(|status| **status == LeaseStatus::DENIED_OVERLAP)
			.count(),
		1
	);

	let winner = if meta_a.status == LeaseStatus::GRANTED {
		req_a.holder
	} else {
		req_b.holder
	};
	let loser = if winner.id == req_a.holder.id {
		&resp_b
	} else {
		&resp_a
	};
	assert!(loser.trailing.is_none());

	let snapshot = node.state_machine.state_snapshot().await;
	let committed = snapshot.leases.get(&range_start).unwrap();
	assert_eq!(committed.holder, winner);
	assert_eq!(committed.range_end, range_end);

	support::abort_node(&node);
}

#[tokio::test]
async fn status_req_includes_topology_and_committed_lease_metadata() {
	let node = support::bootstrap_node("status-with-lease", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
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
async fn transfer_leadership_req_routes_to_control_handler() {
	let node = support::bootstrap_node("transfer-rpc", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let req = TransferLeadershipRequestMeta {
		source_epoch: node.ring.current_epoch(),
		from_leader: node.local_node,
		to_leader: NodeId {
			id: 99,
			_reserved: 0,
		},
	};

	let resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::TRANSFER_REQ,
		&wire::encode(&req).unwrap(),
		None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::TRANSFER_RESP.0);

	let meta: TransferLeadershipResponseMeta = wire::decode(&resp.meta).unwrap();
	assert!(!meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_UNSUPPORTED);
	assert!(resp.trailing.is_none());

	support::abort_node(&node);
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
