# ADR 012: Bounded Clock Drift and HLC Safety

## Status
Accepted

## Context
Tech industry paranoia around computer clocks is entirely justified. While modern NTP/Chrony is excellent, physical clocks in distributed systems can and do drift wildly due to hypervisor VM pauses, leap seconds, daemon crashes, or faulty hardware oscillators. 

Wendelon uses Hybrid Logical Clocks (HLCs) to establish causality, order writes (`commit_hlc`), and enforce linearizable reads (`min_hlc`). The HLC algorithm guarantees that logical time always advances, even if the physical clock goes backward. However, if a single node's physical clock jumps an hour into the *future*, its HLC will "infect" the rest of the cluster during normal RPC communication, permanently dragging the entire cluster's logical timeline an hour forward. Conversely, if a node's clock drags far behind, it might erroneously believe its Raft/Data leases are still valid and serve stale data.

## Decision
Wendelon will enforce a **Bounded Clock Drift** model with a strict **Fail-Fast** mandate.

1. **Maximum Clock Offset:** The cluster operates under a configured maximum offset tolerance (e.g., `MAX_OFFSET_MS = 500`).
2. **Median Cluster Time:** Nodes will continually calculate the median physical time of the cluster by piggybacking timestamps on existing `APPEND_ENTRIES` heartbeats and data-plane RPCs.
3. **Autonomous Suicide:** If a node determines that its local physical clock differs from the cluster median by more than `MAX_OFFSET_MS`, it **MUST** immediately terminate its own process (`std::process::abort()`).

## Rationale
- **Correctness Over Availability:** A node with a severely drifted clock is a hazard to data integrity. We refuse to serve incorrect data or permanently poison the cluster's HLC timeline. 
- **Synergy with Autonomous Healing:** If a node crashes itself, the Unified Hash Ring instantaneously routes around it, and the Autonomous Eviction watchdog (ADR 001) will safely evict it from the topology after the heartbeat threshold expires. Crashing is the safest possible action.

## Consequences (The "Negatives" / Rejected Ideas)
- **Operational Requirement:** Infrastructure operators *must* run robust time synchronization (NTP, Chrony, PTP) on all Wendelon machines. We will not attempt to implement complex time-sync algorithms inside the database itself.
- **False Positives:** A perfectly healthy node might kill itself if a network partition momentarily skews its view of the cluster median. We accept this minor availability hit in exchange for absolute linearizable safety.
- **Implementation Overhead:** We must build a rolling window offset tracker into our core networking/consensus layers.
