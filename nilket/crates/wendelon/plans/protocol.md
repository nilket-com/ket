# Wendelon Protocol & Storage Engine

This document details the HLC (Hybrid Logical Clock) reconciliation protocol, the Per-Range Lease lifecycle, and the Data Ring Storage Engine responsible for persisting the massive Baryon and Polariton payloads.

## 1. Hybrid Logical Clock (HLC) Protocol

Wendelon relies on HLCs to provide strict causality without requiring perfectly synchronized physical clocks.

### 1.1 HLC Structure
An HLC is a 16-byte `#[repr(C)]` struct:
- **`physical_ms` (8 bytes):** Wall-clock milliseconds (NTP synchronized).
- **`logical` (4 bytes):** Monotonic counter, resets to 0 when `physical_ms` advances.
- **`node_id` (4 bytes):** Deterministic tie-breaker.

### 1.2 The Merge Rule (Causality)
Every RPC (Control Ring and Data Ring) carries the sender's HLC.
When a node receives a message with `remote_hlc`:
1. `local_hlc.physical_ms = max(local_hlc.physical_ms, remote_hlc.physical_ms, current_wall_clock)`
2. If `local_hlc.physical_ms` equals `remote_hlc.physical_ms`, then `local_hlc.logical = max(local_hlc.logical, remote_hlc.logical) + 1`
3. Else if `local_hlc.physical_ms` was advanced by the remote or wall clock, `local_hlc.logical = 0`

This guarantees that if Event A causally precedes Event B, then `HLC(A) < HLC(B)`.

## 2. Per-Range Lease Protocol

To coordinate Data Ring writes for a specific hash range without global Raft latency, a node must hold a Per-Range Lease.

### 2.1 The `AcquireLease` RPC
When a node wants to become the primary for Range `R` in Epoch `E`:
1. It sends an `AcquireLeaseRequestMeta` to the Control Ring (Raft Leader).
2. The Raft Leader checks if another node currently holds a valid lease for `R` in Epoch `E`.
3. If free (or expired), the Raft Leader commits a lease grant to the Raft WAL.
4. The Leader returns an `AcquireLeaseResponseMeta` containing a `LeaseDescriptor`.

### 2.2 The `LeaseDescriptor`
The descriptor dictates the exact HLC bounds of the lease:
- `epoch`: The Epoch `E` in which this lease is valid.
- `start_hlc`: The HLC when the lease begins.
- `end_hlc`: The physical HLC deadline when the lease expires.
- `owner`: The `NodeId` of the granted primary.

*Crucial:* If the cluster advances to Epoch `E+1`, ALL leases for Epoch `E` are mathematically invalid, even if `end_hlc` has not been reached.

## 3. Data Ring Storage Engine (Persistence)

The Data Ring must persist massive file chunks (Baryon) and Arrow IPC batches (Polariton) with absolute zero-copy efficiency. 

### 3.1 The Zero-Copy `io_uring` Mandate
Wendelon rejects standard buffered I/O. The storage engine is built directly on Linux `io_uring` using `O_DIRECT`.
- Network payloads arriving via QUIC are DMA'd into page-aligned memory buffers.
- These same buffers are immediately submitted to `io_uring` for disk write.
- **No Serialization:** The `[ FrameHeader | Metadata | Arrow IPC Payload ]` bytes received on the wire are written to disk *exactly* as they arrived.

### 3.2 Append-Only Slab Allocation
To maximize NVMe SSD throughput, Wendelon uses an append-only slab allocator per hash range.
- Each hash range `R` has an active `Slab` file (e.g., `range_R_active.slab`).
- Incoming writes for `R` are appended sequentially to this slab.
- A lightweight in-memory B-Tree (the "Index") maps `KeyHash -> (SlabId, Offset, Length, HLC)`.

### 3.3 Garbage Collection & Compaction
Because Wendelon uses an append-only slab, old versions of keys and deleted data waste space.
1. **The `E_safe` Boundary:** The storage engine periodically checks the global `E_safe` watermark.
2. **Slab Compaction:** A background thread scans older, sealed slabs.
3. If a record's `HLC` is older than `E_safe` AND it has been superseded by a newer write in the Index, the old record is dead.
4. Live records are compacted into a new slab. The old slab is unlinked from the filesystem.

By storing the raw wire bytes directly via `io_uring` and using `E_safe` to drive local compaction, the Data Ring achieves massive throughput while remaining causally consistent with the Control Ring.
