#[path = "support/control_ring.rs"]
mod support;

use std::sync::Arc;
use wendelon::client::topology::{ControlRingTopologySource, TopologySource};
use wendelon::network::server::HrftService;
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};

#[tokio::test]
async fn status_req_returns_live_cluster_status_wire() {
	let node = support::bootstrap_node("status", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);
	let source = ControlRingTopologySource::new(factory, pool, vec![node.local_node]);

	let snapshot = source.fetch().await.unwrap();
	assert_eq!(snapshot.epoch, 1);
	assert_eq!(snapshot.leader, Some(node.local_node.id));
	assert_eq!(snapshot.replication_factor, 1);
	assert_eq!(snapshot.peers.len(), 1);
	assert_eq!(snapshot.peers[0].node_id, node.local_node.id);
	assert_eq!(snapshot.peers[0].address, "127.0.0.1:0");
	assert_eq!(
		TransportTierSet::from_u8(snapshot.peers[0].tier_bits),
		TransportTierSet::empty().with(TransportTier::QuicIoUring)
	);
	assert_eq!(
		NodeRoleSet::from_u8(snapshot.peers[0].role_bits),
		NodeRoleSet::empty()
			.with(NodeRole::Data)
			.with(NodeRole::Control)
	);

	support::abort_node(&node);
}
