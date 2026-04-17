# Workstream D: Durability Lifecycle Closure
**Date:** 2026-04-17
**Author:** Gemini CLI

## Goal
Finish the control and storage lifecycle so durability claims hold in steady state, preventing unbounded disk growth and incomplete recovery profiles.

## Scope & Tasks

### 1. Live Snapshot Persistence
*   **Target Files:** `src/consensus/raft_storage.rs`
*   **Tasks:**
    *   Implement durable writes for the `snapshot.wnd` file during the live Raft state machine snapshot process (`build_snapshot` / `install_snapshot`).
    *   Implement snapshot chunking/streaming to prevent memory bloat during large snapshot installations.

### 2. WAL Compaction & Checkpoints
*   **Target Files:** `src/consensus/raft_storage.rs`
*   **Tasks:**
    *   Emit `REC_CHECKPOINT` records at runtime (periodic or on snapshot boundaries).
    *   Implement actual WAL truncation on disk (file rewrite or hole-punch) rather than just dropping entries from the in-memory cache.

### 3. Background Scrubbing
*   **Target Files:** `src/storage/core.rs`, `src/storage/manager.rs`
*   **Tasks:**
    *   Add background verification tasks to opportunistically scan and detect bit-rot or corruption in cold L1/L2 slabs based on manifest hashes.
    *   Report detected corruption for operator intervention or trigger automated replica repair.

## Exit Criteria
*   Snapshots and checkpoints are actively produced in steady-state runtime.
*   WAL size is bounded on disk by real file compaction.
*   Recovery tests successfully rebuild state using compacted WALs and persisted snapshots.

---
## Open Questions
*   Is `O_DIRECT` without an explicit `fsync` group-commit policy sufficient for durability, assuming target hardware utilizes PLP NVMe? Or must we implement a strict group-commit fsync?
*   Should snapshot chunking use a fixed byte size, or semantic boundaries?