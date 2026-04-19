//! Unified Hash Ring (ADR 002).
//!
//! Every node in a Wendelon cluster — regardless of its role —
//! is projected onto a single `u64` consistent hash space. A
//! query for a replica set binary-searches the ring, walks
//! clockwise, and keeps nodes whose advertised [`NodeRoleSet`]
//! includes the role the caller cares about (typically
//! [`NodeRole::Data`] for replica placement).
//!
//! The decision to filter *after* the walk rather than maintain
//! per-role rings is load-bearing: a single small cluster where
//! every node wears both hats reuses the same data structure as a
//! dedicated deployment where roles are disjoint. The CPU cost is
//! the two-line predicate in [`RingState::replicas_for_locality`].
//!
//! ## Virtual nodes
//!
//! Each physical node is inserted [`VNODES_PER_NODE`] times at
//! positions derived from a stable BLAKE3-based vnode hash.
//! Vnodes
//! smooth load skew when node counts are small and collapse back
//! to the same `NodeId` in the replica set via explicit dedup on
//! the walk, so a single key still maps to `RF` *distinct* nodes.
//!
//! ## Swap semantics
//!
//! [`RingRouter`] holds its state behind an
//! `Arc<RwLock<Arc<RingState>>>`. Reads acquire the read lock,
//! clone the inner `Arc`, and drop the lock — so the hot path
//! never contends with the rare membership-update writer. A
//! background task (see [`RingRouter::spawn_updater`]) drains a
//! `watch::Receiver<Arc<MembershipEpoch>>`, rebuilds a
//! [`RingState`], and swaps it in atomically.

use crate::network::data_server::ReplicaRouter;
use crate::network::transport::{MembershipEpoch, NodeRole, NodeRoleSet};
use crate::schema::{Identity, Locality, NodeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::watch;

/// Number of virtual nodes per physical node. Chosen empirically:
/// 64 is enough to smooth load skew in small clusters without
/// bloating the `Vec<(u64, NodeId)>` — a 1000-node cluster lands
/// at 64000 positions (≈1 MiB), trivial to binary-search.
pub const VNODES_PER_NODE: usize = 64;

/// Stable BLAKE3 key used when placing node ids onto the ring.
///
/// This keeps vnode placement deterministic while letting W1 drop
/// the legacy non-BLAKE3 vnode hash implementation entirely. Record `Identity` hashing remains
/// a separate helper; vnode placement only consumes the first
/// 64 bits of this domain-separated digest.
const RING_BLAKE3_KEY: [u8; 32] = *b"wendelon-ring-vnode-hash-key-v1!";

// ============================================================
//  RING STATE
// ============================================================

/// Immutable snapshot of the ring for a given membership epoch.
/// Swapped wholesale when the cluster topology changes — never
/// mutated in place.
#[derive(Debug)]
pub struct RingState {
	/// Vnode positions sorted by hash, ready for binary search.
	/// Multiple entries per [`NodeId`] (one per vnode) are expected.
	positions: Vec<(u64, NodeId)>,
	/// Per-node role bitmask. Lookups during the walk decide
	/// whether to keep or skip a node.
	roles: HashMap<NodeId, NodeRoleSet>,
	/// Count of distinct nodes whose role set includes
	/// [`NodeRole::Data`]. Precomputed so the router can clamp
	/// `RF` without scanning the map on every call.
	data_count: usize,
	/// Replication factor this ring was built for. Callers set
	/// this from the cluster's authoritative RF (usually surfaced
	/// via the `ClusterStatusWire`); the router clamps the
	/// effective RF against [`Self::data_count`] at read time.
	rf: usize,
	/// Epoch this state was derived from. Tracing only — routing
	/// decisions consult `positions` and `roles`.
	#[allow(dead_code)]
	epoch: u64,
}

/// Stable contiguous hash interval with a fixed ordered replica set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaSegment {
	pub start: u64,
	pub end: u64,
	pub replicas: Vec<NodeId>,
}

impl RingState {
	/// Build a ring from a membership snapshot. Runs in O(N·V·log(N·V))
	/// where N is the node count and V is [`VNODES_PER_NODE`];
	/// call sites only pay this on epoch transitions.
	pub fn build(membership: &MembershipEpoch, rf: usize) -> Self {
		let mut positions: Vec<(u64, NodeId)> =
			Vec::with_capacity(membership.peers.len() * VNODES_PER_NODE);
		let mut roles: HashMap<NodeId, NodeRoleSet> =
			HashMap::with_capacity(membership.peers.len());
		let mut data_count: usize = 0;

		for (node, caps) in &membership.peers {
			roles.insert(*node, caps.roles);
			if caps.roles.contains(NodeRole::Data) {
				data_count += 1;
			}
			for vnode_idx in 0..VNODES_PER_NODE as u64 {
				positions.push((vnode_hash(*node, vnode_idx), *node));
			}
		}
		// Sort by (hash, node_id) so a hash collision between two
		// nodes produces a deterministic order — the walk then
		// sees them in a stable sequence across every replica.
		positions.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.id.cmp(&b.1.id)));

		Self {
			positions,
			roles,
			data_count,
			rf: rf.max(1),
			epoch: membership.epoch,
		}
	}

	/// Ring built from `MembershipEpoch::empty()` with RF=1. The
	/// router is initialised with this so `replicas_for` is safe
	/// to call before the first epoch publication lands.
	pub fn empty() -> Self {
		Self {
			positions: Vec::new(),
			roles: HashMap::new(),
			data_count: 0,
			rf: 1,
			epoch: 0,
		}
	}

	/// Data-capable node count at the time this state was built.
	pub fn data_capable_nodes(&self) -> usize {
		self.data_count
	}

	/// Replica set resolution. See [`ReplicaRouter::replicas_for_locality`] for
	/// the contract — this is the data-ring implementation.
	fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId> {
		if self.positions.is_empty() || self.data_count == 0 {
			return Vec::new();
		}
		let want = self.rf.min(self.data_count);
		let mut out: Vec<NodeId> = Vec::with_capacity(want);

		// Binary search for the first position >= locality. Clockwise
		// walk means we start at that index and wrap around the end.
		let start = self
			.positions
			.binary_search_by(|(h, _)| h.cmp(&locality.get()))
			.unwrap_or_else(|ins| ins);

		let n = self.positions.len();
		// Safety cap: iterate at most `n` slots, so a ring that
		// somehow lacks `want` data-capable positions can still
		// terminate instead of spinning.
		for step in 0..n {
			if out.len() == want {
				break;
			}
			let (_, node) = self.positions[(start + step) % n];
			if out.iter().any(|n| n.id == node.id) {
				// Same physical node appears many times (vnodes);
				// only the first landing counts toward RF.
				continue;
			}
			if !self
				.roles
				.get(&node)
				.copied()
				.unwrap_or_default()
				.contains(NodeRole::Data)
			{
				continue;
			}
			out.push(node);
		}
		out
	}

	/// Primary owner for a routing locality. This is the first
	/// data-capable replica the ring walk would return for the key.
	pub fn owner_of_locality(&self, locality: Locality) -> Option<NodeId> {
		self.replicas_for_locality(locality).into_iter().next()
	}

	/// Primary owner for a record identity. This intentionally
	/// projects through [`Locality::from`] so owner selection remains
	/// a pure function of `Locality`, matching ADR 029.
	pub fn owner_of_identity(&self, identity: Identity) -> Option<NodeId> {
		self.owner_of_locality(Locality::from(identity))
	}

	fn replication_factor(&self) -> usize {
		self.rf.min(self.data_count.max(1))
	}

	/// Contiguous `[start, end]` intervals for which the ordered
	/// replica set is stable. These segments form the deterministic
	/// diff unit for epoch-transition migration seeding.
	pub fn replica_segments(&self) -> Vec<ReplicaSegment> {
		if self.positions.is_empty() || self.data_count == 0 {
			return Vec::new();
		}

		let mut out: Vec<ReplicaSegment> = Vec::new();
		for i in 0..self.positions.len() {
			let replicas = self.replicas_for_locality(Locality::new(self.positions[i].0));
			if replicas.is_empty() {
				continue;
			}
			let start = if i == 0 {
				0
			} else {
				self.positions[i - 1].0.saturating_add(1)
			};
			let end = self.positions[i].0;
			push_or_merge_segment(&mut out, start, end, replicas);
		}

		let wrap_start = self.positions[self.positions.len() - 1].0.saturating_add(1);
		if wrap_start != 0 {
			let replicas = self.replicas_for_locality(Locality::new(self.positions[0].0));
			if !replicas.is_empty() {
				push_or_merge_segment(&mut out, wrap_start, u64::MAX, replicas);
			}
		}

		out
	}

	/// Epoch this ring state was built from. Consumers — the lease
	/// manager in particular — need the epoch so proposed lease
	/// grants carry the correct HLC fencing.
	pub fn epoch(&self) -> u64 {
		self.epoch
	}

	/// All inclusive `[start, end]` locality ranges for which
	/// `node` is the *primary* (first data-capable replica). The
	/// primary is defined as the first entry that
	/// [`Self::replicas_for_locality`] would return at any point in
	/// the range.
	///
	/// Returned ranges are sorted by `start` and collapsed so two
	/// adjacent vnodes owned by the same primary are merged into a
	/// single entry. Wraparound is materialised as two ranges — one
	/// ending at `u64::MAX` and one starting at `0` — so the caller
	/// can treat each entry as a well-ordered interval.
	pub fn primary_ranges_for(&self, node: NodeId) -> Vec<(u64, u64)> {
		if self.positions.is_empty() || self.data_count == 0 {
			return Vec::new();
		}
		// Each vnode at positions[i] owns keys in the half-open
		// interval (positions[i-1].0, positions[i].0]. We compute
		// this per slot and ask who the primary is at that slot; if
		// it matches `node`, we emit the range.
		let n = self.positions.len();
		let mut out: Vec<(u64, u64)> = Vec::new();
		for i in 0..n {
			let slot_locality = Locality::new(self.positions[i].0);
			let primaries = self.replicas_for_locality(slot_locality);
			let Some(primary) = primaries.first() else {
				continue;
			};
			if primary.id != node.id {
				continue;
			}
			let prev_end = if i == 0 {
				// Wraparound: keys after the last position and keys
				// at/below positions[0] both belong to this slot.
				// Represent as two ranges so each is monotonic.
				let tail_start = self.positions[n - 1].0.saturating_add(1);
				if tail_start != 0 {
					out.push((tail_start, u64::MAX));
				}
				0
			} else {
				self.positions[i - 1].0.saturating_add(1)
			};
			out.push((prev_end, slot_locality.get()));
		}
		out.sort_by_key(|(s, _)| *s);
		// Merge adjacent ranges owned by the same primary — reduces
		// the number of lease RPCs a node issues on dense rings.
		let mut merged: Vec<(u64, u64)> = Vec::with_capacity(out.len());
		for (s, e) in out {
			match merged.last_mut() {
				Some(last) if last.1.saturating_add(1) >= s => {
					if e > last.1 {
						last.1 = e;
					}
				}
				_ => merged.push((s, e)),
			}
		}
		merged
	}
}

// ============================================================
//  ROUTER
// ============================================================

/// Dynamic replica router backed by the Unified Hash Ring. Holds
/// a swappable [`RingState`] behind a read-write lock so the hot
/// path is a quick read + `Arc::clone`; the cold path — the
/// membership updater task — takes the write lock just long
/// enough to publish the new state.
pub struct RingRouter {
	state: RwLock<Arc<RingState>>,
}

impl RingRouter {
	pub fn new(initial: RingState) -> Self {
		Self {
			state: RwLock::new(Arc::new(initial)),
		}
	}

	/// Shortcut for the common "start empty, install the updater,
	/// and let membership drive the rest" pattern.
	pub fn empty() -> Self {
		Self::new(RingState::empty())
	}

	/// Atomically replace the active ring state. Called by the
	/// membership-updater task; tests can drive it directly.
	pub fn install(&self, state: RingState) {
		let mut guard = self.state.write().expect("ring state poisoned");
		*guard = Arc::new(state);
	}

	/// Snapshot of the current ring state. Cheap — bumps the Arc
	/// refcount and drops the lock.
	pub fn snapshot(&self) -> Arc<RingState> {
		self.state.read().expect("ring state poisoned").clone()
	}

	/// Convenience delegate for [`RingState::primary_ranges_for`].
	pub fn primary_ranges_for(&self, node: NodeId) -> Vec<(u64, u64)> {
		self.snapshot().primary_ranges_for(node)
	}

	/// Convenience delegate for [`RingState::replicas_for_locality`].
	pub fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId> {
		self.snapshot().replicas_for_locality(locality)
	}

	/// Convenience delegate for [`RingState::owner_of_locality`].
	pub fn owner_of_locality(&self, locality: Locality) -> Option<NodeId> {
		self.snapshot().owner_of_locality(locality)
	}

	/// Convenience delegate for [`RingState::owner_of_identity`].
	pub fn owner_of_identity(&self, identity: Identity) -> Option<NodeId> {
		self.snapshot().owner_of_identity(identity)
	}

	/// Epoch of the currently installed ring state.
	pub fn current_epoch(&self) -> u64 {
		self.snapshot().epoch()
	}

	/// Spawn a background task that watches `rx` for membership
	/// transitions and rebuilds the ring at the caller-supplied
	/// replication factor. Terminates cleanly when the sender
	/// half of `rx` is dropped.
	///
	/// The initial value sitting in the channel is applied before
	/// the task starts waiting — so callers never observe a
	/// window where the router is empty after construction.
	pub fn spawn_updater(
		self: Arc<Self>,
		mut rx: watch::Receiver<Arc<MembershipEpoch>>,
		rf: usize,
	) -> tokio::task::JoinHandle<()> {
		// Apply the current snapshot synchronously so readers that
		// call `replicas_for_locality` before the task runs see the
		// seed epoch instead of the default-empty ring.
		{
			let membership = rx.borrow().clone();
			self.install(RingState::build(&membership, rf));
		}
		tokio::spawn(async move {
			while rx.changed().await.is_ok() {
				let membership = rx.borrow().clone();
				self.install(RingState::build(&membership, rf));
			}
		})
	}
}

impl ReplicaRouter for RingRouter {
	fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId> {
		self.snapshot().replicas_for_locality(locality)
	}

	fn replication_factor(&self) -> usize {
		self.snapshot().replication_factor()
	}
}

// ============================================================
//  HELPERS
// ============================================================

/// Position a single vnode on the ring. Deterministic across every
/// node in the cluster so `node A` and `node B` agree on where
/// `node C`'s vnodes land.
fn vnode_hash(node: NodeId, vnode_idx: u64) -> u64 {
	let mut hasher = blake3::Hasher::new_keyed(&RING_BLAKE3_KEY);
	hasher.update(&node.id.to_le_bytes());
	hasher.update(&vnode_idx.to_le_bytes());
	let digest = hasher.finalize();
	let mut bytes = [0u8; 8];
	bytes.copy_from_slice(&digest.as_bytes()[..8]);
	u64::from_le_bytes(bytes)
}

fn push_or_merge_segment(
	out: &mut Vec<ReplicaSegment>,
	start: u64,
	end: u64,
	replicas: Vec<NodeId>,
) {
	if end < start {
		return;
	}
	match out.last_mut() {
		Some(last)
			if last.end != u64::MAX
				&& last.end.saturating_add(1) == start
				&& last.replicas == replicas =>
		{
			last.end = end;
		}
		_ => out.push(ReplicaSegment {
			start,
			end,
			replicas,
		}),
	}
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::transport::{
		MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportTierSet,
	};

	fn n(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn data_caps() -> PeerCapabilities {
		PeerCapabilities {
			address: "127.0.0.1:0".into(),
			tiers: TransportTierSet::empty(),
			roles: NodeRoleSet::empty().with(NodeRole::Data),
		}
	}

	fn control_only_caps() -> PeerCapabilities {
		PeerCapabilities {
			address: "127.0.0.1:0".into(),
			tiers: TransportTierSet::empty(),
			roles: NodeRoleSet::empty().with(NodeRole::Control),
		}
	}

	fn membership_of(peers: Vec<(u32, PeerCapabilities)>) -> MembershipEpoch {
		let mut map = HashMap::new();
		for (id, caps) in peers {
			map.insert(n(id), caps);
		}
		MembershipEpoch {
			epoch: 1,
			peers: map,
		}
	}

	#[test]
	fn empty_ring_returns_no_replicas() {
		let router = RingRouter::new(RingState::empty());
		assert!(router.replicas_for_locality(Locality::ZERO).is_empty());
		assert!(router.replicas_for_locality(Locality::MAX).is_empty());
		assert_eq!(router.replication_factor(), 1);
	}

	#[test]
	fn ring_produces_rf_distinct_data_nodes() {
		let m = membership_of(vec![
			(1, data_caps()),
			(2, data_caps()),
			(3, data_caps()),
			(4, data_caps()),
			(5, data_caps()),
		]);
		let router = RingRouter::new(RingState::build(&m, 3));
		let replicas = router.replicas_for_locality(Locality::new(0xDEAD_BEEF_CAFE_0001));
		assert_eq!(replicas.len(), 3);
		// Distinct node ids across the replica set — the vnode
		// dedup must collapse repeated walks to the same physical.
		let mut ids: Vec<u32> = replicas.iter().map(|n| n.id).collect();
		ids.sort();
		ids.dedup();
		assert_eq!(ids.len(), 3);
	}

	#[test]
	fn ring_wraps_around_when_key_past_max_vnode() {
		// Two nodes; query at u64::MAX must still yield both of them
		// via wraparound.
		let m = membership_of(vec![(1, data_caps()), (2, data_caps())]);
		let router = RingRouter::new(RingState::build(&m, 2));
		let replicas = router.replicas_for_locality(Locality::MAX);
		assert_eq!(replicas.len(), 2);
	}

	#[test]
	fn control_only_nodes_are_skipped_for_data_routing() {
		// Three physical nodes total, but only one advertises Data.
		// Data-capable count clamps effective RF to 1.
		let m = membership_of(vec![
			(1, control_only_caps()),
			(2, data_caps()),
			(3, control_only_caps()),
		]);
		let router = RingRouter::new(RingState::build(&m, 3));
		let replicas = router.replicas_for_locality(Locality::new(42));
		assert_eq!(replicas.len(), 1);
		assert_eq!(replicas[0], n(2));
		assert_eq!(router.replication_factor(), 1);
	}

	#[test]
	fn rf_clamped_when_request_exceeds_data_nodes() {
		// Ring walk must terminate even though RF=5 > data_count=2.
		let m = membership_of(vec![(1, data_caps()), (2, data_caps())]);
		let router = RingRouter::new(RingState::build(&m, 5));
		let replicas = router.replicas_for_locality(Locality::new(7));
		assert_eq!(replicas.len(), 2);
	}

	#[test]
	fn same_key_always_maps_to_same_replica_set() {
		// Consistent hashing's load-bearing promise: same key →
		// same (ordered) replica set every call.
		let m = membership_of(vec![(1, data_caps()), (2, data_caps()), (3, data_caps())]);
		let router = RingRouter::new(RingState::build(&m, 3));
		let a = router.replicas_for_locality(Locality::new(0xABCD_1234));
		let b = router.replicas_for_locality(Locality::new(0xABCD_1234));
		assert_eq!(a, b);
	}

	#[test]
	fn install_swaps_ring_atomically() {
		let m1 = membership_of(vec![(1, data_caps())]);
		let router = RingRouter::new(RingState::build(&m1, 1));
		assert_eq!(router.replicas_for_locality(Locality::ZERO).len(), 1);

		let m2 = membership_of(vec![
			(10, data_caps()),
			(11, data_caps()),
			(12, data_caps()),
		]);
		router.install(RingState::build(&m2, 3));
		let replicas = router.replicas_for_locality(Locality::ZERO);
		assert_eq!(replicas.len(), 3);
		for r in &replicas {
			assert!([10, 11, 12].contains(&r.id));
		}
	}

	#[tokio::test]
	async fn updater_task_reacts_to_watch_changes() {
		let initial = Arc::new(MembershipEpoch::empty());
		let (tx, rx) = watch::channel(initial);
		let router = Arc::new(RingRouter::empty());
		let handle = router.clone().spawn_updater(rx, 2);

		// Seed ring was empty, so no replicas yet.
		assert!(router.replicas_for_locality(Locality::ZERO).is_empty());

		// Publish a fresh epoch with two data nodes — the task
		// observes the change and installs the new ring.
		let next = Arc::new(membership_of(vec![(1, data_caps()), (2, data_caps())]));
		tx.send(next).unwrap();
		// Yield so the updater can drain the change.
		for _ in 0..32 {
			tokio::task::yield_now().await;
			if !router.replicas_for_locality(Locality::ZERO).is_empty() {
				break;
			}
		}
		assert_eq!(router.replicas_for_locality(Locality::ZERO).len(), 2);

		// Dropping the sender terminates the updater cleanly.
		drop(tx);
		handle.await.unwrap();
	}

	#[test]
	fn rf_zero_is_clamped_to_one() {
		let m = membership_of(vec![(1, data_caps())]);
		let router = RingRouter::new(RingState::build(&m, 0));
		assert_eq!(router.replication_factor(), 1);
		assert_eq!(router.replicas_for_locality(Locality::ZERO).len(), 1);
	}

	#[test]
	fn primary_ranges_cover_whole_space_for_single_data_node() {
		let m = membership_of(vec![(1, data_caps())]);
		let state = RingState::build(&m, 1);
		let ranges = state.primary_ranges_for(n(1));
		assert!(!ranges.is_empty());
		// Every u64 must fall inside one of the returned ranges.
		let covers = |k: u64| ranges.iter().any(|(s, e)| *s <= k && k <= *e);
		assert!(covers(0));
		assert!(covers(1));
		assert!(covers(u64::MAX));
		assert!(covers(0xDEAD_BEEF));
	}

	#[test]
	fn primary_ranges_for_non_member_are_empty() {
		let m = membership_of(vec![(1, data_caps()), (2, data_caps())]);
		let state = RingState::build(&m, 1);
		let ranges = state.primary_ranges_for(n(99));
		assert!(ranges.is_empty());
	}

	#[test]
	fn primary_ranges_agree_with_replicas_for_sampled_keys() {
		let m = membership_of(vec![(1, data_caps()), (2, data_caps()), (3, data_caps())]);
		let state = RingState::build(&m, 1);
		for &node_id in &[1u32, 2, 3] {
			let ranges = state.primary_ranges_for(n(node_id));
			for (s, e) in &ranges {
				// Both endpoints map back to the same node via
				// `replicas_for_locality` — the ranges really do
				// describe
				// this node's primary ownership.
				assert_eq!(
					state
						.replicas_for_locality(Locality::new(*s))
						.first()
						.unwrap()
						.id,
					node_id
				);
				assert_eq!(
					state
						.replicas_for_locality(Locality::new(*e))
						.first()
						.unwrap()
						.id,
					node_id
				);
			}
		}
	}

	#[test]
	fn identity_owner_projection_is_pure_function_of_locality() {
		let m = membership_of(vec![(1, data_caps()), (2, data_caps()), (3, data_caps())]);
		let state = RingState::build(&m, 1);
		for i in 0..1024u64 {
			let identity = Identity::blake3(&i.to_le_bytes());
			let locality = Locality::from(identity);
			assert_eq!(
				state.owner_of_identity(identity),
				state.owner_of_locality(locality)
			);
		}
	}
}
