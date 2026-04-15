# Plan: Implement The Unified Hash Ring Topology

## Summary
This plan details the implementation of Target #5: The Unified Hash Ring Topology. It replaces the current static routing abstractions (`StaticReplicaRouter`) with a dynamic, mathematically rigorous consistent hash ring projection based on the cluster's active `MembershipEpoch`, fulfilling the mandates of ADR 002.

## Motivation
Currently, Wendelon's routing relies on a static, hardcoded list of nodes (`StaticReplicaRouter`), which cannot accommodate cluster topology changes. ADR 002 mandates that all nodes exist in a single unified hash space, and routing must dynamically filter this ring based on node capabilities (e.g., control vs. data). This mathematical projection is the foundational prerequisite for true cluster elasticity and the upcoming autonomous self-healing targets.

## Goals
- **Unified Ring Projection:** Create a routing structure that ingests a `MembershipEpoch` (node list + capabilities) and maps all nodes onto a `u64` consistent hash space.
- **Capability-Aware Routing:** Implement the capability-filtering step. To resolve a replica set for a `key_hash`, the router must binary-search the ring and walk clockwise, collecting `RF` distinct nodes that specifically possess the `Data` capability.
- **Dynamic Updates:** Wire the new router to the `watch::Receiver<Arc<MembershipEpoch>>` so that the active routing table is updated atomically across the server and client whenever the Control Ring advances the epoch.

## Non-Goals
- **Raft Configuration Changes:** The logic to detect dead nodes and propose the epoch transition (Autonomous Eviction) is explicitly deferred to Target #6.
- **Eager Data Rebalancing:** As per ADR 006, updating the ring mathematically is instantaneous, but the physical data migration remains strictly lazy. We will not trigger data movement tasks here.

## Implementation
1. **Core Ring Logic:** Create a new module (e.g., `src/network/ring.rs`). Implement a `RingRouter` backed by a sorted `Vec<(u64, NodeId)>` for fast `O(log N)` binary search. Virtual nodes (vnodes) can be used to improve distribution evenness.
2. **`ReplicaRouter` Integration:** Implement the existing `ReplicaRouter` trait for `RingRouter`. 
   - Given a `key_hash`, perform a binary search to find the nearest clockwise node.
   - Walk the ring forward (wrapping around if necessary).
   - Filter nodes based on their advertised capabilities (e.g., `TransportTierSet` or a dedicated capability bitmask) to ensure they can accept data traffic.
   - Stop when `RF` unique, data-capable nodes are found.
3. **Safety Caps:** The ring walk must safely terminate if the requested `RF` exceeds the total number of data-capable nodes in the cluster.
4. **Dynamic Wiring:** Update `WendelonDataServer` and `WendelonClient` to hold an `ArcSwap<RingRouter>` (or similar concurrent pointer) that is atomically swapped by a background task whenever the `MembershipEpoch` watch channel changes.

## Risks and Mitigations
- **Risk:** Infinite loop during the ring walk if `RF` > data-capable nodes.
  - **Mitigation:** The router must pre-calculate the total number of data-capable nodes upon projection and clamp the effective `RF` to this maximum.
- **Risk:** Routing divergence between client and server if epoch updates propagate at different speeds.
  - **Mitigation:** The protocol already includes epoch-stale redirect hints (`WriteStatus::EPOCH_STALE` / `ReadStatus::E_SAFE_BEHIND`). The client's idempotency and redirect handling from Target #2 will transparently mask this transition window.

## Timeline
This is Target #5. To be implemented immediately to prepare the ground for Target #6 (Autonomous Healing).

## References
- `plans/adr/002_unified_hash_ring.md`
- `src/network/data_server.rs`
- `src/client/topology.rs`