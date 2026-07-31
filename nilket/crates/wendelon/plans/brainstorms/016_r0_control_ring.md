# Brainstorm 016: R0 Control Ring and Multi-Raft Architecture

## Context
As Wendelon targets exabyte scale (inspired by the operational limits of massive data systems like Uber's), it is clear that a single Raft consensus ring will inevitably become a bottleneck. A single ring cannot efficiently manage the global topology, schema, membership, and high-throughput data replication simultaneously across thousands of nodes. 

## The Big Idea: Two-Tiered Raft
The path forward to exabyte scale is a two-tiered Raft architecture. We introduce a second-level Raft ring, designated **$R_0$ (The Control Ring)**, which sits above and coordinates a dynamic number of localized data Raft rings, designated **$R_n$ (The Data Rings)**. The use of $R$ aligns with the standard algebraic convention for a formal mathematical ring, distinguishing these structures from more general groups ($G$).

### The Responsibility Boundary

$R_0$ operates strictly as the control plane. It manages the metadata, topology, and global state machine, but stays entirely out of the high-throughput data and compute paths. By isolating the control plane to $R_0$, we ensure the pure Rust, `io_uring`, and RDMA pathways remain entirely unobstructed by metadata management.

| Subsystem | $R_0$ IS Responsible For (Strict Control Plane) | $R_0$ IS NOT Responsible For (Data/Compute Planes) |
| :--- | :--- | :--- |
| **Topology** | Maintaining the global HashRaft boundary map. | Proxying, inspecting, or routing live user queries (Toron). |
| **Membership** | Node discovery, admittance, and health gating. | Load-balancing or executing scatter-gather logic (Toron). |
| **Sequencing** | Periodically syncing physical time bounds (HLCs). | Issuing monolithic transaction IDs or intra-ring ordering. |
| **State & Schema**| Global DDL mutations and table definitions. | Storing, replicating, or managing raw columnar data (Wendelon). |
| **Rebalancing** | Defining keyspace boundaries for lazy handovers. | Executing physical RDMA/network data migrations (Wendelon). |
| **Execution** | Running lightweight, isolated state machines. | Consuming io_uring bandwidth or data-path CPU cycles. |

## Architectural Implications

1. **Strict Control/Data Separation:** 
   - **Toron (Compute/Routing):** Acts as the compute plane. It queries $R_0$ to cache the HashRaft boundary map, but routes live user queries directly to the appropriate $R_n$ leader. $R_0$ is never a proxy.
   - **Wendelon ($R_n$ Data Rings):** Handles the heavy lifting of raw columnar data replication using `io_uring` and RDMA (`rucx`). They do not waste cycles voting on global schema changes unless it affects their specific keyspace.

2. **Infinite Keyspace Scalability:** 
   By decoupling the hash ring boundaries ($R_0$) from the physical data replication ($R_n$), the system can stamp out new $R_n$ rings dynamically. As the cluster grows, $R_0$ simply updates the map.

3. **Lazy Physical Migrations:** 
   When $R_0$ decides to split a keyspace or rebalance, it updates the boundary map. The actual movement of bytes over the network is delegated to the $R_n$ nodes to perform asynchronously via RDMA, keeping the control ring lightweight.

## Design Resolutions

To maintain the extreme latency requirements and avoid enterprise bloat, the following design constraints are established:

### 1. Cross-Ring Transactions (No 2PC)
Toron must **never** become a traditional Two-Phase Commit (2PC) coordinator. Introducing distributed hardware locks holding open transactions across multiple $R_n$ nodes will destroy the latency profile. We maintain speed through two paths:
- **Strict Single-Ring Isolation:** Force data modeling to adhere to the `TableSpreadProjection` routing. If an operation requires atomic consistency, it must logically hash to the same $R_n$ ring.
- **Deterministic SAGAs:** If a transaction must span rings, Toron acts as a stateless orchestrator of independent, reversible operations. No locks are held on the bare metal.

### 2. Map Caching Contract (Epoch Rejection)
Toron does not poll $R_0$ on a timer, which would introduce unnecessary control-plane chatter. Instead, we rely on **Epoch Rejection**.
- Every time $R_0$ updates the topology map, it increments a global integer (the Epoch). 
- Toron caches both the map and the Epoch. 
- When Toron routes a query to an $R_n$ leader, it includes its cached Epoch in the RPC header. 
- If the $R_n$ leader has undergone a split or rebalance and is operating on a newer Epoch, it instantly rejects the query with a lightweight `STALE_EPOCH` error. 
- Toron catches the error, reaches out to $R_0$ for the fresh map, and retries the query seamlessly without client awareness.

### 3. Failure Domains (Degraded State)
If a majority of an $R_n$ ring suffers a catastrophic hardware failure (e.g., an entire rack loses power), $R_0$ does not attempt to magically heal it by inventing non-existent data. 
- $R_0$ marks the ring as **DEGRADED**. 
- Toron continues to route queries to it, hits the timeout, and returns a localized error to the user indicating that the specific HashRaft space is currently unreachable.
- The rest of the Exabyte cluster continues to serve queries normally.

### 4. Sequencing Mechanism (Decentralized HLCs)
$R_0$ does not issue a central, monolithic ID for every transaction, as that would instantly become a bottleneck and overwhelm our CPU limits.
- Instead, $R_0$ only periodically syncs the physical time bounds using Hybrid Logical Clocks (HLC). 
- The individual $R_n$ leaders combine their local NTP time with a logical counter to generate globally sortable identifiers independently.
- This completely decentralizes ID generation while maintaining mathematical causality across the entire system.

## Appendix: The Single Ring Upper Bound

Before adopting the $R_0$/$R_n$ multi-raft split, it is critical to understand the mathematical and physical upper bounds of a single Raft consensus ring optimized with `io_uring` and RDMA. The implementation of the $R_0$/$R_n$ split introduces massive state-transition complexity (lazy migrations, SAGA orchestrations, epoch thundering herds) and should not be undertaken until these physical limits are actively threatened.

### 1. The Network / Heartbeat Amplification Limit
Raft fundamentally requires a single leader to send periodic heartbeats (or `AppendEntries`) to every follower to maintain authority and prevent elections.
*   **The Math:** If your election timeout is aggressive (e.g., 50ms) to ensure low MTTR, the leader must heartbeat every ~15ms.
*   **Amplification:** For $N$ nodes, the leader sends $N-1$ heartbeats every 15ms. At 1,000 nodes, that's ~66,000 RPCs per second *just to stand still*, not including actual data payload or replication acks.
*   **RDMA Factor:** RDMA drastically reduces the CPU cost of this, but it doesn't eliminate the NIC packet-rate limits or the queue-pair memory overhead.
*   **The Bound:** A single Raft ring becomes unstable and highly susceptible to false elections due to network jitter somewhere between **100 and 300 nodes**.

### 2. The Log Sequencing Limit
The single Raft leader must assign an index to every mutating transaction, write it to its own durability log, and replicate it to a quorum.
*   **The Math:** Modern NVMe SSDs via `io_uring` can easily sustain 1M+ IOPS for small writes, but Raft requires *fsync* (or `O_DSYNC`/`IORING_OP_FSYNC`) for durability before advancing the commit index.
*   **Group Commit:** You *must* batch (group commit) to survive. If you batch aggressively, you can push the physical limits of the PCIe bus.
*   **RDMA Factor:** With `rucx` doing zero-copy replication to followers' memory, the replication latency is sub-millisecond. The bottleneck is purely the leader's ability to sequence, batch, and `io_uring` the log to its local NVMe drive.
*   **The Bound:** A highly optimized single leader doing group commits over `io_uring` can realistically sequence **1 million to 3 million transactions per second**.

### 3. The State Machine Replay Limit
How much data can a single ring actually hold before recovery times become fatal?
*   **The Math:** If a single ring holds 100 PB, and a node dies, a new node joining the ring has to receive a snapshot of the state machine.
*   **RDMA Factor:** A 100 Gbps RDMA link can transfer ~12 GB/s. Transferring 1 PB to heal a node takes *over 24 hours* at wire speed, during which the cluster is degraded.
*   **The Bound:** The maximum storage capacity of a single ring is dictated by the acceptable recovery time objective (RTO). To heal a dead node in under an hour over 100 Gbps, the single ring cannot hold more than **~50 to 100 Terabytes** of hot data per node.

**Conclusion:** The current single-ring architecture can handle **~100 to 300 nodes**, **~1M to 3M TPS**, and **~5 PB to 10 PB of total capacity**. The multi-raft split is only required when scaling past these operational ceilings into true exabyte territory.