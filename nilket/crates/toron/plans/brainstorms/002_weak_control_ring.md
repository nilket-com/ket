# Brainstorm 002: The WeakControlRing (Ephemeral Consensus)

## The Premise
While Wendelon provides a robust, persisted consensus layer for storage metadata (DDL, slab locations, etc.), relying on it for transient compute decisions introduces a "persistence tax" (NVMe writes and WAL fsyncs). Toron requires a low-latency, high-availability consensus mechanism for data that has a shelf life of seconds or minutes.

By introducing a **WeakControlRing**—a purely in-memory Raft group shared among Toron Coordinators—we can achieve high-speed coordination for scheduling, resource accounting, and query lifecycle management without the overhead of disk-backed state. The name draws an analogy to `WeakReference`: it provides a shared view of the compute state without the "strong" requirement of persistence.

## 1. High-Availability (HA) Coordinators
Currently, Toron's coordinator model is a single point of failure. If the coordinator managing a query dies, the query and its session state are lost.

*   **Mechanism:** Multiple Coordinators form an ephemeral Raft group. They use the replicated log to share a **Query Admission Registry**.
*   **The Advantage:** If a primary Coordinator fails, a follower in the ring can immediately take over active queries. Since the HLC-pinned descriptors and session metadata were replicated to the follower's memory, the client can continue receiving result batches without a query restart.

## 2. Global Resource Accounting (Cluster-Wide Quotas)
In a multi-tenant environment, enforcing global limits (e.g., "Tenant X is capped at 500 cores") is difficult if coordinators are independent and "guesstimating" cluster load.

*   **Mechanism:** The Control Ring maintains a **Cluster-Wide Slot Map** as part of its state machine. Query admission becomes an atomic Raft operation: "Allocate 64 slots for Tenant Y."
*   **The Advantage:** This prevents the "Thundering Herd" problem. Coordinators have an identical, atomic view of worker utilization, ensuring strict multi-tenant isolation and preventing over-subscription of the worker ring.

## 3. Shuffle Barrier Synchronization
Distributed shuffles (`GROUP BY`, `JOIN`) often require global synchronization points where Stage N must be fully complete across all workers before Stage N+1 can begin.

*   **Mechanism:** Instead of each worker pinging a central coordinator, workers signal completion to the Control Ring. The ring achieves **Stage Consensus** and broadcasts an atomic "Flip to Stage N+1" message.
*   **The Advantage:** This decouples the coordinator's CPU from the heartbeat/barrier processing path and provides a more resilient, distributed way to manage multi-stage execution DAGs.

## 4. Zero-Persistence for Maximum Speed
Because the data managed by this ring (morsel assignments, transient quotas, barrier signals) is inherently transient, the ring can operate with zero-persistence.

*   **Mechanism:** The Raft log and state machine are stored strictly in RAM. There is no WAL on NVMe and no disk snapshotting.
*   **The Advantage:** Bypassing the storage ring's LSM/WAL paths minimizes latency. If the entire compute ring reboots, it starts with a clean slate—a perfectly acceptable trade-off given that the storage truth remains safely anchored in Wendelon.

## Conclusion
The Ephemeral Compute Control Ring transforms Toron from a collection of independent coordinators into a coherent, self-healing **Compute Control Plane**. It allows us to solve the "Integrity" pillar of V1 (multi-tenant quotas and fault tolerance) with the mechanical sympathy required for high-performance analytical workloads.
