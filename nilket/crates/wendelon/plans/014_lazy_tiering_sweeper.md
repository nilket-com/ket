# Plan: Implement The Lazy Tiering Background Sweeper

## Summary
This plan details the implementation of Target #3: the Background Sweeper, responsible for storage tiering (L1 -> L2). As per ADR 006, Wendelon does not keep all historical data on NVMe (L1) forever. Instead, a Background Sweeper autonomously seals old slabs, streams them to Object Storage (L2), and safely updates the metadata pointers in the Raft Control Ring.

## Motivation
Storing petabytes of historical analytical data on NVMe SSDs is economically unviable. To be a true tiered storage engine, Wendelon needs the mechanism to physically move cold data to S3 (L2) and update the cluster's metadata. This proves that our storage architecture can seamlessly transition data states between physical tiers without blocking concurrent reads or hot writes.

## Goals
- **Background Worker:** Create an isolated, rate-limited asynchronous tokio task (`SlabSweeper`) that periodically scans the `LocalSlabStore` for cold slabs based on age or size thresholds.
- **L2 Object Storage Integration:** Build the L2 client logic to stream sealed NVMe slabs out to S3-compatible object storage.
- **Control Ring Metadata Update:** Once an L1 slab is successfully uploaded to L2, commit a transition record via the Raft Control Ring to safely swap the slab's location pointer from a local NVMe offset to an L2 object URI.
- **Zero-Downtime Transition:** Guarantee that readers seamlessly handle the transition. Data must remain readable from L1 until the pointer swap is fully committed and locally applied.

## Non-Goals
- **Lazy Migration on Read:** On-demand fetching from an old owner on a read cache-miss during an epoch transition (this will be handled separately as part of the read-path proxying).
- **Intra-Slab Compaction:** Compacting deleted or overwritten records within a slab (the V1 sweeper only moves the entire immutable slab to L2).

## Implementation
1. **L2 Storage Interface:** Define an `ObjectStore` trait in `src/storage/l2.rs` with a concrete S3 implementation (or a local filesystem mock for tests) to handle streaming uploads.
2. **Sweeper Task:** Create `src/storage/sweeper.rs`. Implement a loop that periodically queries the `SlabManager` for slabs eligible for tiering (e.g., closed slabs older than a configured threshold).
3. **Upload & Verify:** Stream the slab to L2 and verify the payload hash to guarantee durability.
4. **Metadata Swap:** Dispatch a new Control Ring message (e.g., `WendelonRequest::TierSlab`) containing the slab ID and the new L2 URI. The State Machine applies this to update the global index.
5. **Garbage Collection (Two-Phase):** The sweeper must *only* unlink/free the L1 file once the local State Machine has applied the Raft commit and the `SlabManager` reflects the new L2 pointer.

## Risks and Mitigations
- **Risk:** Reads failing because the slab was deleted from L1 before the L2 metadata was fully propagated to all followers.
  - **Mitigation:** The sweeper must strictly follow the two-phase commit: upload to L2, commit the pointer swap to Raft, and wait for the local SM application before reclaiming L1 space.
- **Risk:** High network/disk contention starving the hot write path.
  - **Mitigation:** Run the sweeper with a strict rate limit and low I/O priority relative to the `io_uring` append threads.

## Timeline
This is Target #3. To be implemented following the completion of the Smart Client SDK.

## References
- `plans/adr/006_tiered_storage_lazy_migration.md`
- `src/storage/manager.rs`
- `src/consensus/core.rs`