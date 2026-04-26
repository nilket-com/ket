//! Capability-negotiation tests.
//!
//! Exercises the per-capability-bit contract from ADR 036
//! §"Capability negotiation contract": each capability variant
//! advertises through `NodeCapabilitySet`, round-trips through the
//! topology snapshot wire shape, and is queryable per-peer.

#[path = "support/capability_harness.rs"]
mod harness;

use harness::{membership_of, peer_advertises, peer_caps};
use wendelon::client::{ClusterPeerWire, ClusterStatusWire};
use wendelon::network::transport::{
	NodeCapability, NodeCapabilitySet, NodeRole, NodeRoleSet, TransportTier, TransportTierSet,
};

fn slab_stats_pruning_only() -> NodeCapabilitySet {
	NodeCapabilitySet::empty().with(NodeCapability::SlabStatsPruning)
}

fn aggregate_pushdown_only() -> NodeCapabilitySet {
	NodeCapabilitySet::empty().with(NodeCapability::PartialAggregatePushdown)
}

#[test]
fn mixed_capability_membership_advertises_per_peer() {
	let membership = membership_of(
		1,
		&[
			(1, slab_stats_pruning_only()),
			(2, NodeCapabilitySet::empty()),
		],
	);
	assert!(
		peer_advertises(&membership, 1, NodeCapability::SlabStatsPruning),
		"peer 1 was built with SlabStatsPruning advertised"
	);
	assert!(
		!peer_advertises(&membership, 2, NodeCapability::SlabStatsPruning),
		"peer 2 was built with no capabilities; must not appear advertised"
	);
}

#[test]
fn aggregate_pushdown_capability_is_independent_bit() {
	let membership = membership_of(
		1,
		&[
			(1, aggregate_pushdown_only()),
			(2, slab_stats_pruning_only()),
		],
	);
	assert!(peer_advertises(
		&membership,
		1,
		NodeCapability::PartialAggregatePushdown
	));
	assert!(!peer_advertises(
		&membership,
		1,
		NodeCapability::SlabStatsPruning
	));
	assert!(!peer_advertises(
		&membership,
		2,
		NodeCapability::PartialAggregatePushdown
	));
}

#[test]
fn absent_peer_does_not_advertise_any_capability() {
	let membership = membership_of(1, &[(1, slab_stats_pruning_only())]);
	assert!(
		!peer_advertises(&membership, 99, NodeCapability::SlabStatsPruning),
		"peer 99 is not in the membership; must not be reported as advertising"
	);
}

#[test]
fn topology_snapshot_round_trips_per_peer_capability_bits() {
	let snapshot = ClusterStatusWire {
		epoch: 7,
		leader: Some(1),
		replication_factor: 1,
		peers: vec![
			ClusterPeerWire {
				node_id: 1,
				address: "loopback://node-1".into(),
				tier_bits: TransportTierSet::empty()
					.with(TransportTier::QuicIoUring)
					.as_u8(),
				role_bits: NodeRoleSet::empty().with(NodeRole::Data).as_u8(),
				capability_bits: slab_stats_pruning_only().as_u8(),
			},
			ClusterPeerWire {
				node_id: 2,
				address: "loopback://node-2".into(),
				tier_bits: TransportTierSet::empty()
					.with(TransportTier::QuicIoUring)
					.as_u8(),
				role_bits: NodeRoleSet::empty().with(NodeRole::Data).as_u8(),
				capability_bits: 0,
			},
		],
	};

	let membership = snapshot.to_membership();
	assert!(peer_advertises(
		&membership,
		1,
		NodeCapability::SlabStatsPruning
	));
	assert!(!peer_advertises(
		&membership,
		2,
		NodeCapability::SlabStatsPruning
	));
}

#[test]
fn peer_caps_helper_pins_capabilities_and_defaults_routing_attributes() {
	let caps = peer_caps("loopback://node-42", slab_stats_pruning_only());
	assert_eq!(caps.address, "loopback://node-42");
	assert!(caps.tiers.contains(TransportTier::QuicIoUring));
	assert!(caps.roles.contains(NodeRole::Data));
	assert!(caps.roles.contains(NodeRole::Control));
	assert!(caps.capabilities.contains(NodeCapability::SlabStatsPruning));
}
