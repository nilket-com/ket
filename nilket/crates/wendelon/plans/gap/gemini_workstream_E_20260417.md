# Workstream E: Tiered Storage Closure
**Date:** 2026-04-17
**Author:** Gemini CLI

## Goal
Turn object storage tiering (L2) from a partially implemented subsystem into a leak-free, production-integrated component.

## Scope & Tasks

### 1. Configuration & Pivot Convergence
*   **Target Files:** `src/config.rs`, `src/storage/sweeper.rs`, `src/node.rs`
*   **Tasks:**
    *   Ensure L2 is fully enabled from config and the `SlabSweeper` is exercised through the shipped daemon in production bootstrap.
    *   Add pending-upload journals to ensure crash-safe transitions from L1 to L2, preventing mid-upload state loss.

### 2. Orphan GC & Remote Reclaim
*   **Target Files:** `src/storage/sweeper.rs`, `src/storage/manager.rs`
*   **Tasks:**
    *   Implement a background reconciliation loop comparing `tiered_slabs` Raft state against actual S3 bucket contents.
    *   Clean up orphaned objects left behind by mid-upload crashes or failed commits.
    *   Implement regular object deletion lifecycle (retention expiry, migration cleanup).

### 3. Range-GET & Multipart Upload
*   **Target Files:** `src/storage/l2.rs`, `src/storage/manager.rs`
*   **Tasks:**
    *   Modify the `RemoteObjectStore` trait to support range reads and multipart uploads.
    *   Update the read path to fetch only the exact bytes required for a specific slab record, avoiding 64MB+ full-slab downloads for point-lookups.

## Exit Criteria
*   L2 tiering works end-to-end on real nodes, configured via TOML.
*   Crash/retry paths converge without routine orphan accumulation in S3.
*   L2 point reads only download the requested bytes (Range-GET).

---
## Open Questions
*   The Range-GET implementation changes the `ObjectStore` trait shape. Should we batch streaming put, multipart upload, and list into a single breaking change early on?