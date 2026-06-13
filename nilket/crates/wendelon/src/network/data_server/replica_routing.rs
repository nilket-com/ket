use super::*;

/// Resolves a routing locality to the replica set that should store it.
///
/// The contract is deliberately narrow: return the ordered list of
/// `NodeId`s that form the replica set for this key. The ordering
/// is not consulted by the fan-out (every member must ack), but a
/// stable ordering helps tracing and topology tooling line up.
///
/// A real implementation projects off the Raft `ring(E)` snapshot
/// and filters for data-capable nodes (ADR 002). The static fallback
/// is [`StaticReplicaRouter`].
pub trait ReplicaRouter: Send + Sync {
	/// Members of the replica set for `locality`. The order is
	/// stable for a given topology so traces align; callers must
	/// not depend on the first entry being the local node.
	fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId>;

	/// Effective replication factor for this router. Exposed so the
	/// quorum math is source-of-truthed in one place.
	fn replication_factor(&self) -> usize;
}

/// Static ring router. Holds the full node list and an RF; resolves
/// each locality bucket's replica set as `RF` consecutive nodes
/// starting at `locality % nodes.len()`. This is a stand-in for the
/// full ADR 002
/// projection — good enough for a bring-up where membership is
/// stable, fine for tests, and will be swapped for a real snapshot
/// reader once the Unified Hash Ring projection lands.
pub struct StaticReplicaRouter {
	nodes: Vec<NodeId>,
	rf: usize,
}

impl StaticReplicaRouter {
	/// Build a router over `nodes` with replication factor `rf`.
	/// The node list is sorted by numeric id for deterministic
	/// ordering; duplicate ids are deduplicated.
	pub fn new(mut nodes: Vec<NodeId>, rf: usize) -> Self {
		nodes.sort_by_key(|n| n.id);
		nodes.dedup_by_key(|n| n.id);
		let rf = rf.max(1);
		Self { nodes, rf }
	}
}

impl ReplicaRouter for StaticReplicaRouter {
	fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId> {
		if self.nodes.is_empty() {
			return Vec::new();
		}
		let n = self.nodes.len();
		let take = self.rf.min(n);
		let start = (locality.get() as usize) % n;
		(0..take).map(|i| self.nodes[(start + i) % n]).collect()
	}

	fn replication_factor(&self) -> usize {
		self.rf.min(self.nodes.len().max(1))
	}
}
