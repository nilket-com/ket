#[path = "support/control_ring.rs"]
mod support;

use std::sync::Arc;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, ControlRingTopologySource, TopologySource, WendelonClient,
};
use wendelon::network::server::HrftService;
use wendelon::network::transport::{NodeRole, NodeRoleSet, TransportTier, TransportTierSet};
use wendelon::schema::Locality;

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

#[tokio::test]
async fn wendelon_client_refreshes_topology_from_live_control_ring() {
	let node = support::bootstrap_node("status-client-refresh", 1).await;
	support::wait_for_local_leader(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service.clone());
	let source: Arc<dyn TopologySource> = Arc::new(ControlRingTopologySource::new(
		factory,
		pool,
		vec![node.local_node],
	));

	let bootstrap = ClusterStatusWire {
		epoch: 0,
		leader: None,
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id: node.local_node.id,
			address: "loopback://bootstrap".into(),
			tier_bits: TransportTierSet::empty()
				.with(TransportTier::QuicIoUring)
				.as_u8(),
			role_bits: NodeRoleSet::empty()
				.with(NodeRole::Data)
				.with(NodeRole::Control)
				.as_u8(),
		}],
	};

	let client = WendelonClient::new(support::loopback_client_config_with_topology(
		node.local_node,
		service,
		bootstrap,
		source,
	));

	assert_eq!(client.leader(), None);
	client.refresh_topology().await.unwrap();
	assert_eq!(client.leader(), Some(node.local_node));
	assert_eq!(client.replication_factor(), 1);
	assert_eq!(
		client.replicas_for_locality(Locality::ZERO),
		vec![node.local_node]
	);

	support::abort_node(&node);
}
