# ADR 003: The "Stellarator Model" (Bifurcated Write Path)

## Status
Accepted

## Context
Raft is the gold standard for strong consistency, but the Raft Leader is an absolute bottleneck for high-throughput data replication. If Wendelon routes multi-gigabyte Baryon file chunks or massive Polariton Arrow batches through a single Raft WAL, the system will immediately choke and fail to saturate modern NVMe drives.

## Decision
Wendelon will enforce a **Strictly Bifurcated Write Path**. Bulk data payloads (Arrow IPC, file chunks) will **NEVER** enter the global Raft WAL. 
1. **Data Quorum:** Heavy data is routed directly to the Data Ring peers via QUIC.
2. **Control Finalization:** Only after Data Quorum is achieved does the primary node send a tiny, 88-byte `FinalizationMarker` to the Raft Control Ring to advance the `E_safe` watermark.

## Rationale
- **NVMe-Saturating Throughput:** By bypassing Raft for the payload, the Data Ring can saturate `io_uring` and NIC queues with zero-copy transfers.
- **Strong Consistency:** Committing the `FinalizationMarker` to Raft ensures we retain strict linearizability and a definitive `E_safe` boundary for garbage collection, without paying the payload penalty.

## Consequences (The "Negatives" / Rejected Ideas)
- **Rejected Standard Raft:** We explicitly reject the standard architecture of appending state-machine mutations directly into the Raft log.
- **Extreme Complexity:** This decision introduces the massive complexity of Per-Range Leases, Hybrid Logical Clocks (HLCs), and Write Proxying. We must actively manage the "Dual-Accept" vulnerability window when topologies change. We accept this engineering burden as the necessary cost of achieving both scale and correctness.