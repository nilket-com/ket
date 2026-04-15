# Plan: Implement Per-Range Leases and Split-Brain Protection

## Summary
This plan details the implementation of: Per-Range Leases. To support the Stellarator Write Path (ADR 003) and guarantee linearizable reads without forcing every data operation through the Raft Control Ring, Wendelon must implement a strict lease mechanism. The Raft Leader grants time-bound, HLC-based leases to Data Nodes for specific hash ranges. Data Nodes must actively hold a lease to accept new writes or serve strongly consistent reads.

## Motivation
Bifurcating the write path (routing Arrow data directly to storage nodes while bypassing the Raft WAL) introduces the "Dual-Accept" vulnerability. During a network partition or epoch transition, two nodes might temporarily believe they are the primary for the same hash range. Without a lease mechanism, both nodes could accept writes, breaking linearizability and creating split-brain data divergence. By forcing primaries to acquire an HLC-bounded lease from the Control Ring before accepting traffic, we cryptographically ensure only one node coordinates writes for a given range at any time.

## Goals
- **Lease Representation:** Promote the placeholder `AcquireLeaseRequestMeta` and `LeaseDescriptor` structs in `src/network/traits.rs` to true `bytemuck` PODs in `src/schema/core.rs`.
- **Lease Granting (Control Ring):** Extend the `WendelonStateMachine` to handle lease grants. The Raft Leader tracks active leases, ensuring no overlapping leases are granted for the same range within the same Epoch.
- **Lease Acquisition (Data Node):** Data nodes must calculate their assigned hash ranges from the `RingRouter` (Target #5) and actively request leases from the Raft Leader before unblocking local traffic for those ranges.
- **Lease Validation:** `WendelonDataServer::write` and `WendelonDataServer::read` (for `LINEARIZABLE` consistency) must assert that a valid, unexpired `LeaseDescriptor` is held locally for the targeted `key_hash`.
- **Epoch Invalidation:** All leases are mathematically invalid the moment the cluster transitions to `E+1`.

## Non-Goals
- **Write Proxying (Forwarding):** If a node receives a write for a range it does not hold a lease for, it will currently reject the request with `NOT_LEADER` or `LEASE_EXPIRED` (relying on the Smart Client to retry against the hint). Proxying the write transparently to the correct leaseholder will be handled in a subsequent target.
- **Data Migration:** Moving data from old leaseholders to new leaseholders (Lazy Migration) is deferred to the Migration target.

## Implementation
1. **Schema Updates:** Move lease-related structs into `src/schema/core.rs` with `#[repr(C)] Pod + Zeroable`. 
2. **Control Ring Handler:** Add a `WendelonRequest::AcquireLease` variant. The `WendelonStateMachine` maintains a `DashMap` of active leases per range.
3. **Data Node Lease Manager:** Add a background task to the `WendelonDataServer` that observes the `RingRouter` epoch. For every range where the local node is the primary (first in the replica set), it periodically sends an `AcquireLease` RPC to the Control Ring.
4. **Validation Gate:** In `write` and `read` (when `Consistency::LINEARIZABLE`), check the local lease cache. If the local HLC exceeds the lease's `lease_expiry` or the expected epoch differs, return the appropriate error status.

## Risks and Mitigations
- **Risk:** HLC Clock Drift causing a node to think its lease is valid longer than the cluster agrees.
  - **Mitigation:** The Bounded Clock Drift crash safeguard (Target #6) already protects against this. If a node's physical clock drifts enough to violate lease safety, the node will abort itself.
- **Risk:** The Raft Leader becomes a bottleneck for lease renewals.
  - **Mitigation:** Leases cover wide hash ranges (vnodes), not individual keys. Renewal traffic is minimal and perfectly manageable for a single Raft Leader.

## Timeline
This is to be implemented following the completion of the Unified Hash Ring and Autonomous Eviction.

## References
- `plans/adr/003_stellarator_bifurcated_writes.md`
- `plans/network/protocol.md`
- `plans/network/read_flow.md`
- `src/schema/core.rs`