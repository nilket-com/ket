# Wendelon Tiered Storage & Slab Manifest Schema

To prevent NVMe saturation and ensure cost-effective storage for historical data, Wendelon implements a Tiered Storage hierarchy (L0 DRAM, L1 NVMe, L2 Object Storage) driven by an append-only slab allocator and a background Sweeper.

## 1. Storage Engine Trait

The Data Ring interacts with persistence through a unified `StorageEngine` trait, which abstracts the physical medium:
- `LocalSlabStore`: Handles L1 NVMe operations via `io_uring` and `O_DIRECT`.
- `RemoteObjectStore`: Handles L2 operations (S3/GCS/B2) via HTTP REST APIs.

## 2. Slab Geometry and Chunking

Append-only slabs are large files (e.g., 1 GB) that are not sorted by key.
- To enable efficient reads from L2 without downloading an entire 1GB slab, each slab is logically divided into **Chunks** (e.g., 16 MB).
- The local in-memory B-Tree maps: `KeyHash -> (SlabId, ChunkIndex, OffsetWithinChunk, ByteLength, HLC)`
- When an object lives on L2, the Data Node calculates its exact byte range and issues an HTTP `GET Range: bytes={Start}-{End}` request.

## 3. The Slab Manifest (`#[repr(C)]` POD)

The lifecycle of every slab is tracked in a fast, fixed-size manifest struct, which is persisted locally in the metadata WAL.

```rust
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct SlabManifest {
    pub slab_id: u64,
    /// 0 = Active (NVMe), 1 = Sealed (NVMe), 2 = Uploading, 3 = Stubbed (S3)
    pub state: u8,
    pub _pad: [u8; 7],
    pub min_hlc: HybridTimestamp, // Timestamp of first write
    pub max_hlc: HybridTimestamp, // Timestamp of last write
    pub total_bytes: u64,         // e.g., 1,073,741,824 (1GB)
    pub chunk_size: u32,          // e.g., 16,777,216 (16MB)
    /// Heatmap: counter of reads in the last 24h
    pub temperature: u32,         
    /// Remote routing info (zeroed if local)
    pub remote_bucket: [u8; 32],  // e.g., "nilket-cold-tier-1"
    pub remote_key: [u8; 64],     // e.g., "wendelon/epoch_5/slab_123.wnd"
}
```

## 4. Background Sweeper Policies

The Sweeper is a low-priority Tokio background task that strictly monitors NVMe capacity, slab temperature, and the `E_safe` watermark to drive the state machine.

### Policy 1: Sealing (Active -> Sealed)
- **Trigger:** `slab.total_bytes >= 1 GB` OR `(current_hlc - slab.min_hlc) > 4 hours`.
- **Action:** Close the `O_DIRECT` file descriptor for writing. The slab becomes read-only. Open a new Active slab for the hash range.

### Policy 2: Eviction / Upload (Sealed -> Uploading -> Stubbed)
- **Trigger:** `NVMe utilization > 70%` AND `slab.temperature < THRESHOLD`.
- **Action:** The Sweeper streams the sealed slab to S3 via multipart upload. Once S3 returns a 200 OK + ETag, the `SlabManifest` is updated to `Stubbed`, the local NVMe file is `unlink()`'d, and the NVMe space is reclaimed.

### Policy 3: Promotion (Stubbed -> Sealed)
- **Trigger:** A `Stubbed` slab's `temperature` suddenly spikes (e.g., a massive analytical query scans historical data).
- **Action:** If `temperature > PROMOTE_THRESHOLD`, the Sweeper asynchronously downloads the entire slab back to NVMe, changing its state back to `Sealed` to serve subsequent queries at local SSD speeds.

### Policy 4: E_safe Compaction (Garbage Collection)
- **Trigger:** `slab.max_hlc < Global_E_safe_floor` AND `percentage_of_dead_keys > 50%`.
- **Action:** The Sweeper reads the live keys from the old slab, appends them to the current Active slab, and completely deletes the old slab. (If the old slab is on S3, this saves cloud storage costs).