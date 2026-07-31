# Moved from plans/hashraft.md

(This file was copied into plans/consensus/hashraft.md during a planned refactor.)

# HashRaft: The Hybrid Consensus Bridge

This document defines the core mechanism of "HashRaft"—the mathematical and operational bridge that fuses a global Control Ring (Raft) with a high-throughput Data Ring (Consistent Hashing).

## 1. The Stellarator Invariant: Zero Data in the Control WAL

The most critical architectural invariant in Wendelon is that **bulk data payloads (Arrow IPC batches, Baryon file chunks) NEVER enter the global Raft WAL.**

If massive data streams were serialized through a single Raft leader, the system would immediately bottleneck, destroying the scalability of the Data Ring.

### 1.1 The Bifurcated Write Path

A client write operation is bifurcated into two distinct phases:
1. **Data Quorum (The Heavy Lift):** The massive data payload is routed directly to the Data Ring. It is replicated to the N nodes responsible for that specific hash range in the current Epoch (`ring(E)`). This uses the zero-copy QUIC transport but does *not* use Raft.
2. **Control Finalization (The Metadata Sync):** Once Data Quorum is achieved, the primary node for that hash range asynchronously fires a tiny, fixed-size `FinalizationMarker` (88 bytes) to the global Raft Control Ring.

## 2. Per-Range Leases

To coordinate writes without a global lock, the unified hash ring is divided into a fixed number of virtual partitions (e.g., 4096 ranges).

### 2.1 Lease Acquisition
- For each range, exactly one Data Node acts as the **Range Primary**.
- To become a Primary, a node must acquire a **Lease** from the Control Ring via the `AcquireLeaseRequestMeta` RPC.
- A Lease is an exclusive right to coordinate writes for a specific hash range.
- Leases are heavily tied to the Hybrid Logical Clock (HLC). A lease is valid for a specific physical time window (e.g., 5 seconds) plus a logical counter.

### 2.2 Lease Enforcement
- If a client or proxy sends a write to a node that does not hold an active lease for that key's hash, the node rejects the write with `LEASE_EXPIRED` or routes it to the correct Primary (via `proxy_hop`).
- Leases are instantly invalidated cluster-wide the moment the Control Ring advances the global Epoch (from `E` to `E+1`).

## 3. The E_safe Watermark

The `E_safe` watermark is the cornerstone of Wendelon's garbage collection and staleness rejection. It is a monotonically increasing HLC timestamp tracked per-range (and globally aggregated).

### 3.1 Establishing E_safe
1. A Range Primary receives a Write Request at `HLC_W`.
2. It replicates the payload to its Data Ring peers.
3. Upon Data Quorum, it sends a `FinalizationMarker{ range_id, commit_hlc: HLC_W }` to the Control Ring.
4. The Control Ring appends this marker to the Raft WAL.
5. Once committed by Raft, `HLC_W` officially becomes the new `E_safe` watermark for that range.

### 3.2 The Global E_safe Floor
The Control Ring constantly calculates the global minimum `E_safe` across all active ranges. This global floor is the absolute boundary for safe Garbage Collection. No data older than the global `E_safe` floor is needed for reconciliation, allowing nodes to aggressively compact their local storage.

## 4. Reconciliation and Stale Write Rejection

Because Wendelon utilizes a Data Ring without strong serialization (no global lock on the data path), we must gracefully handle network partitions, delayed packets, and proxy loops.

### 4.1 The Rejection Rule
When a Data Node receives a Write Request with a timestamp `HLC_req`:
1. It looks up the current `E_safe` watermark for the target hash range.
2. **If `HLC_req <= E_safe`**: The write is provably stale. A newer, finalized write has already been acknowledged by the Control Ring for this range. The request is **summarily rejected** (or dropped if a delayed packet).
3. **If `HLC_req > E_safe`**: The write is a candidate for the current timeline. The node proceeds with the Data Quorum protocol.

### 4.2 Conflict Resolution (Tie-Breaking)
If two writes hit the same key with identical physical and logical HLC components (exceedingly rare but possible in split-brain scenarios before lease expiry), the `node_id` component of the `HybridTimestamp` is used as a deterministic tie-breaker. The higher `node_id` wins, ensuring all nodes eventually converge on the exact same byte-for-byte state.

## 5. Summary of the HashRaft Flow

1. Client hashes Key K -> gets Range R.
2. Client routes to Data Node holding Lease for R.
3. Node (Primary) assigns `HLC_W` to the Write.
4. Primary replicates Arrow/Bytes to replicas for R.
5. Primary waits for Replica Aks.
6. Primary ACKs to Client (Low Latency).
7. Primary asynchronously sends `FinalizationMarker(R, HLC_W)` to Control Ring.
8. Control Ring commits marker via Raft.
9. `E_safe` for R advances to `HLC_W`.
