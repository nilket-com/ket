#[path = "support/control_ring.rs"]
mod support;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::network::control_server::{
	AdvanceEpochResponseWire, CONTROL_REASON_UNSUPPORTED,
};
use wendelon::network::server::HrftService;
use wendelon::network::traits::{
	TransferLeadershipRequestMeta, TransferLeadershipResponseMeta,
};
use wendelon::network::wire;
use wendelon::schema::{
	epoch_entry_var_schema, AcquireLeaseRequestMeta, AcquireLeaseResponseMeta, HybridTimestamp,
	LeaseDescriptor, LeaseStatus, MsgType, NodeId,
};

fn epoch_config_batch() -> RecordBatch {
	RecordBatch::new_empty(Arc::new(epoch_entry_var_schema()))
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

	let lease: LeaseDescriptor =
		*bytemuck::from_bytes(&resp.trailing.as_ref().unwrap()[..std::mem::size_of::<LeaseDescriptor>()]);
	assert_eq!(lease.range_start, range_start);
	assert_eq!(lease.range_end, range_end);
	assert_eq!(lease.holder, node.local_node);

	let snapshot = node.state_machine.state_snapshot().await;
	assert!(snapshot.leases.contains_key(&range_start));

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
	assert!(!meta.accepted);
	assert_eq!(meta.reason_code, CONTROL_REASON_UNSUPPORTED);

	support::abort_node(&node);
}
