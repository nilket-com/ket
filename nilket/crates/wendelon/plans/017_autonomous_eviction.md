# Plan: Implement Autonomous Eviction and Bounded Clock Drift

## Summary
This plan details the implementation of Target #6: Autonomous Eviction and Bounded Clock Drift. As established in ADR 001 and ADR 012, Wendelon acts as a Level 1 autonomous self-healing system. The Raft Leader actively monitors the heartbeats of cluster members and evicts them from the topology if they fail. Simultaneously, every node tracks the median physical time of the cluster. If a node detects its own clock has drifted beyond a strict threshold (ADR 012), it autonomously crashes to prevent poisoning the cluster's Hybrid Logical Clocks (HLCs).

## Motivation
In a large-scale deployment, manual intervention for routing recovery is unacceptable. If a node dies, the cluster must immediately shrink its consistent hash ring to reassign the dead node's responsibilities. Conversely, if a node's physical clock drifts wildly (due to VM pauses or NTP failures), it poses a direct threat to linearizability. The node might erroneously serve stale data or artificially inflate the entire cluster's HLC. We must implement both mathematically guaranteed eviction (for dead nodes) and strict autonomous suicide (for time-drifted nodes).

## Goals
- **Heartbeat & Time Tracking:** Implement a thread-safe tracker on all nodes that records the "last seen" physical timestamp provided by incoming RPCs/heartbeats from active cluster members.
- **Clock Drift Monitor (Fail-Fast):** Create a background task on all nodes that calculates the median cluster time from the tracker. If `abs(local_time - cluster_median) > MAX_OFFSET_MS` (e.g., 500ms), the node MUST execute an immediate `std::process::abort()`.
- **Autonomous Detection (Leader):** Create a background watchdog task on the Leader that periodically sweeps the tracker for liveness. If a node's latency exceeds the eviction threshold (e.g., 5 minutes), it is marked for removal.
- **Epoch Transition Proposal:** When a node is marked for removal, the Leader constructs a new `MembershipEpoch` (omitting the dead node) and submits an `advance_epoch` request to the Control Ring.

## Non-Goals
- **Hardware Provisioning:** Wendelon will NOT call cloud APIs (AWS EC2, Kubernetes) to spin up replacement nodes.
- **Eager Re-replication:** As per ADR 006, the data owned by the dead node is not eagerly re-replicated. It remains in its current degraded RF state until accessed.
- **Internal Time-Sync (NTP):** Wendelon does not attempt to synchronize the host's clock; it only measures the drift and crashes if it exceeds the bounds. Time synchronization remains the responsibility of the OS/infrastructure.

## Implementation
1. **NodeTracker:** In `src/consensus/monitor.rs` (or similar), implement a thread-safe `NodeTracker` that stores `(node_id, remote_physical_timestamp, local_receipt_timestamp)` tuples.
2. **Ping Ingestion:** Update the network layers (Raft and Data Ring) to inject timestamps from valid, authenticated peers into the `NodeTracker`.
3. **Drift Watchdog (All Nodes):** Spawn a task that periodically calculates the median `remote_physical_timestamp + (now - local_receipt_timestamp)` of the cluster. Abort if the local clock drifts outside the threshold (ADR 012).
4. **Eviction Watchdog (Leader Only):** The Leader periodically checks if any node's `local_receipt_timestamp` is older than `EVICTION_THRESHOLD`. If so, construct a new `RecordBatch` representing the new topology (minus the dead node) and call `ControlRing::advance_epoch`.
5. **Guardrails:** To prevent catastrophic cascading evictions during a massive network partition, implement a "Max Eviction Quota" (e.g., the leader refuses to autonomously evict if doing so would drop the cluster below the minimum Raft quorum).

## Risks and Mitigations
- **Risk:** Network partitions causing the Leader to falsely evict healthy nodes that it cannot reach (Split Brain).
  - **Mitigation:** Raft inherently protects against split brain. If the Leader cannot reach a majority, it will lose its leadership and cannot commit the eviction. The `EVICTION_THRESHOLD` should also be conservative (e.g., minutes).
- **Risk:** Network jitter causing false-positive clock drift suicides.
  - **Mitigation:** The `MAX_OFFSET_MS` must account for typical network round-trip time variance. The median calculation smooths out outlier RPC latency spikes.

## Timeline
This is Target #6. To be implemented immediately following the completion of the Unified Hash Ring Topology (Target #5).

## References
- `plans/adr/001_autonomous_healing_boundary.md`
- `plans/adr/012_bounded_clock_drift.md`
- `src/consensus/core.rs`