//! Capability-negotiation test harness.
//!
//! Reusable helpers for building mixed-capability membership snapshots
//! and asserting per-peer capability advertisement. The first consumer
//! is `SlabStatsPruning` (ADR 039); every future capability bit is
//! expected to reuse the same helpers per the per-capability-bit
//! contract in ADR 036 §"Capability negotiation contract".
//!
//! Pure data-structure helpers — no node spawning, no transport, no
//! Raft. Tests that need a live dispatcher should compose this harness
//! with the heavyweight `support/control_ring.rs` helper.

use std::collections::HashMap;
use wendelon::network::transport::{
	MembershipEpoch, NodeCapability, NodeCapabilitySet, NodeRole, NodeRoleSet, PeerCapabilities,
	TransportTier, TransportTierSet,
};
use wendelon::schema::NodeId;

/// Build a `PeerCapabilities` with the standard tiers/roles default
/// (QUIC + Data + Control) and the caller-pinned capability set.
pub fn peer_caps(address: &str, capabilities: NodeCapabilitySet) -> PeerCapabilities {
	PeerCapabilities {
		address: address.to_string(),
		tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
		roles: NodeRoleSet::empty()
			.with(NodeRole::Data)
			.with(NodeRole::Control),
		capabilities,
	}
}

/// Build a `MembershipEpoch` from a list of `(node_id, capability_set)`
/// tuples. Addresses default to `loopback://node-{id}`; tiers and roles
/// default per [`peer_caps`]. Use this when the test only cares about
/// per-peer capability advertisement.
pub fn membership_of(epoch: u64, peers: &[(u32, NodeCapabilitySet)]) -> MembershipEpoch {
	let mut map = HashMap::with_capacity(peers.len());
	for (id, caps) in peers {
		let address = format!("loopback://node-{id}");
		map.insert(
			NodeId {
				id: *id,
				_reserved: 0,
			},
			peer_caps(&address, *caps),
		);
	}
	MembershipEpoch { epoch, peers: map }
}

/// Query helper: does the named peer advertise the given capability?
/// Returns `false` if the peer is absent from the membership.
pub fn peer_advertises(
	membership: &MembershipEpoch,
	node_id: u32,
	capability: NodeCapability,
) -> bool {
	membership
		.peers
		.get(&NodeId {
			id: node_id,
			_reserved: 0,
		})
		.is_some_and(|caps| caps.capabilities.contains(capability))
}
