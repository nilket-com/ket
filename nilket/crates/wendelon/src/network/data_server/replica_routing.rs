use super::*;
use crate::schema::table::PlacementKey;

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

	/// ADR 0090: replica set for a record's own placement. Route selection for
	/// a stored record consults its stamped placement, never only its raw
	/// locality — a partitioned record is owned by its partition's replica set.
	fn replicas_for_placement(&self, key: PlacementKey) -> Vec<NodeId>;
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

	fn replicas_for_placement(&self, key: PlacementKey) -> Vec<NodeId> {
		// The static stand-in walks from the key's ring position either way; what
		// differs is which value is allowed to supply that position.
		match key {
			PlacementKey::RawLocality(locality) => self.replicas_for_locality(locality),
			PlacementKey::Partition(partition) => {
				self.replicas_for_locality(Locality::new(partition.get()))
			}
		}
	}
}

/// ADR 0090: the replicas that may serve a placement, transfer-aware.
///
/// A partition covered by an in-flight Migration Ledger entry is served by
/// exactly that entry's owner — the old ring owner must refuse and the new
/// owner must serve, so a transfer never leaves a partition served by both
/// owners or by neither. Raw-locality placements keep ring routing untouched.
pub(crate) fn serving_replicas_for_placement(
	router: &dyn ReplicaRouter,
	migrations: &std::collections::HashMap<u64, MigrationLedgerEntry>,
	key: crate::schema::table::PlacementKey,
) -> std::io::Result<Vec<NodeId>> {
	if let crate::schema::table::PlacementKey::Partition(partition) = key {
		if let Some(transfer) = crate::consensus::partition_transfer_for(partition, migrations)? {
			// The transfer owner is the serving PRIMARY — the anchor every
			// primacy check compares against — not the whole replica set. The
			// remaining replicas come from the ring's own walk, so a transfer
			// never collapses replicated durability to one node.
			let rf = router.replication_factor().max(1);
			let mut replicas = vec![transfer.serving];
			for node in router.replicas_for_placement(key) {
				if replicas.len() >= rf {
					break;
				}
				if replicas.iter().all(|existing| existing.id != node.id) {
					replicas.push(node);
				}
			}
			return Ok(replicas);
		}
	}
	Ok(router.replicas_for_placement(key))
}
