# Brainstorm 003: Inline Erasure Coding for L1 (Hot Storage)

## The Premise
Conventional wisdom dictates that Erasure Coding (EC) is strictly for cold storage (L2/S3) because the compute overhead and scatter-gather read penalties are too high for the hot ingest path. Therefore, L1 (hot NVMe) usually defaults to 3x replication. 

However, 3x replication has massive hidden costs on the ingest path:
1.  **Network Saturation:** The leader must broadcast 3x the data over the network.
2.  **NVMe Wear & Write Amplification:** Writing the exact same data to three different enterprise NVMe drives burns through their TBW (Terabytes Written) lifespan rapidly.

With modern SIMD acceleration (e.g., the `reed-solomon-erasure` crate with `simd-accel`), encoding is memory-bandwidth bound, not CPU bound. This opens the door to **Inline Erasure Coding for the L1 tier**.

## 1. Network-Efficient Consensus Broadcast
In a standard Raft/Consensus model, the leader receives a 1MB payload and sends 1MB to Node B and 1MB to Node C.
*   **The Idea:** The leader receives 1MB, instantly computes a 4+2 Reed-Solomon erasure code (resulting in six ~250KB fragments). It keeps one fragment and sends the other five to the Ring. 
*   **The Win:** The network bandwidth out of the leader drops dramatically. Instead of broadcasting 3x the total payload size, it broadcasts ~1.5x.

## 2. Asymmetric Storage Topology (Systematic EC)
Read latency is the biggest risk of L1 Erasure Coding. If every hot read requires fetching fragments from 4 nodes over the network, OLAP performance will tank.
*   **The Idea:** We use a **Systematic** Reed-Solomon code. In a systematic code, the original data is preserved exactly as-is in the first `K` fragments, and the remaining `M` fragments are pure parity.
*   **The Win:** The primary node (or leader) for that Locality stores the full, un-encoded contiguous data for blazing-fast local WPBC (Compute Pushdown) and sequential reads. The *followers* in the consensus group only store the much smaller parity fragments. We get the read speed of 1x replication but the durability of 3x replication, while saving massive amounts of NVMe space and wear on the followers.

## 3. Micro-Burst Recovery & Fast Catch-up
When an L1 node crashes and restarts, it needs to catch up on missed writes.
*   **The Idea:** Instead of transferring full heavy snapshots from the leader, the recovering node can stream the much smaller parity fragments from its peers and reconstruct the missing local state.
*   **The Win:** Faster node recovery times and less impact on the primary leader's network interface during cluster turbulence.

## The Risks and Downsides (Why this might fail)
While the NVMe and network savings are tempting, forcing EC into the L1 hot path introduces severe architectural complexities:

1.  **Read-Miss Scatter-Gather Latency:** If the primary node holding the systematic (un-encoded) data crashes, read requests suddenly have to perform a scatter-gather operation across `K` follower nodes to fetch fragments and reconstruct the data. What was a fast, single-node local NVMe read becomes a highly variable, network-bound operation subject to the slowest tail latency of the `K` nodes.
2.  **Consensus State Machine Complexity:** Standard Raft assumes every node applies the exact same log entry. Asymmetric storage (where followers only hold parity) means followers cannot independently execute reads or run WPBC without reconstructing the data first. This breaks the symmetry of the state machine and makes leader election vastly more complicated (a follower might have the most up-to-date parity, but it can't quickly serve read traffic when it becomes leader).
3.  **Partial Write Rollbacks:** If a leader crashes mid-broadcast, leaving only 2 out of 6 fragments on the ring, cleaning up that "torn write" in an erasure-coded setup is significantly harder than simply truncating a Raft log.

## Conclusion
By challenging the assumption that "EC is only for cold data," Wendelon can leverage SIMD-accelerated Reed-Solomon to drastically reduce NVMe wear and network broadcast storms during high-throughput ingest, without sacrificing the blistering read performance required by the L1 tier.