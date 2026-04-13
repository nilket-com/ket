# Data Ring Storage Engine

This document specifies the local storage engine architecture that persists bulk data (Baryon file chunks, Polariton Arrow batches) to local NVMe drives.

## 1. Separation from Raft WAL
The Data Ring storage engine is completely decoupled from the Control Ring's Raft WAL. 
- The Raft WAL (defined in `plans/consensus/raft-core.md`) stores tiny metadata entries and `FinalizationMarker`s.
- The Data Ring Storage Engine stores the massive, multi-megabyte `HRFT` payload frames directly.

## 2. Append-Only Slab Allocation & `io_uring`
To achieve maximum NVMe throughput, the engine bypasses the kernel page cache (`O_DIRECT`).
- **Page Alignment:** Network buffers allocated by `quinn` for incoming payloads are strictly page-aligned (e.g., 4096 bytes).
- **Append-Only:** Writes for a specific hash range `R` are appended sequentially to that range's active slab file (e.g., `range_042_active.slab`).
- **`io_uring` Integration:** A dedicated I/O thread polls a channel for incoming network buffers. It builds Submission Queue Entries (SQEs) for the `io_uring` instance and reaps Completion Queue Entries (CQEs). The `tokio` network tasks `await` a oneshot channel that the I/O thread fires when the CQE arrives.

## 3. Slab Record Format
Because Wendelon stores the raw `HRFT` frames exactly as they arrive from the network, the engine prefixes each frame on disk with a tiny `SlabRecordHeader` to enable crash recovery.
```rust
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct SlabRecordHeader {
    pub magic: u32,             // 0x534C4142 ("SLAB")
    pub _pad_align: u32,        // 4 bytes padding for u64 alignment
    pub key_hash: u64,          // The consistent hash of the key
    pub hlc: HybridTimestamp,   // 16 bytes: the timestamp of the write
    pub frame_len: u32,         // Total length of the HRFT frame that follows
    pub _pad: u32,              // 4 bytes padding to reach 40-byte alignment
}
// static_assert: size_of::<SlabRecordHeader>() == 40
```
On disk, a record looks like: `[ SlabRecordHeader (40B) ][ HRFT Frame (frame_len) ][ Padding to 4096B (O_DIRECT) ]`.

## 4. The In-Memory Index
To serve point reads quickly without scanning slabs, each Data Node maintains an in-memory index per hash range.
- **Structure:** A concurrent hash map (e.g., `DashMap`) or Radix Tree (ART). 
- **Mapping:** `KeyHash -> (SlabId, Offset, Length, HLC)`
- **Updates:** When the `io_uring` CQE confirms the append, the index is updated. If the incoming HLC is newer than the existing HLC for that key, the index pointer is overwritten. The old data remains on disk until background compaction.

## 5. The Read Path
1. Client requests `KeyHash`.
2. Node looks up `KeyHash` in the in-memory index to get `(SlabId, Offset, Length)`.
3. If `SlabId` is local (State = Active or Sealed), a pre-allocated page-aligned buffer is dispatched to `io_uring` for an exact `preadv2` at `Offset` for `Length` bytes.
4. Once the CQE completes, the buffer is handed directly to `quinn` for transmission. (Zero-copy network-to-disk-to-network).

## 5. Startup Recovery
On startup, the node reads the `SlabManifest`s. For any Active or Sealed slabs, it sequentially scans the slab headers (which contain the `KeyHash` and `HLC`) to rebuild the in-memory index.