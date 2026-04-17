# Workstream H — Performance and Scale Hardening

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phase 8)
**Status:** First pass
**Subsystems:** `src/storage/`, `src/consensus/`, `src/network/`, runtime-wide
**Depends on:** Workstreams B (durability) and C (tiering) benefit this workstream but are not hard blockers

---

## Problem Statement

Several performance and scale risks will surface under enterprise
workloads:

1. **Unbounded memory growth.** DashMap caches (idempotency, lease, key
   index, migration registry) have no eviction. Arrow read buffers
   collect entire result sets in memory. L2 read/write paths buffer
   full slabs.
2. **No backpressure.** No per-connection, per-peer, or global in-flight
   limit. A single pathological client can OOM the node.
3. **No WAL group commit.** Every `append_to_log` batch issues its own
   fsync. Under concurrent Raft entries this is a latency tax.
4. **Startup scales with data.** Slab index rebuild scans every file
   record-by-record. At enterprise scale this creates poor restart SLOs.
5. **No benchmarks.** The "100 ns-scale hot path" claim from
   `plans/ops/benchmarks.md` is unverified.
6. **No NUMA/CPU affinity.** The io_uring thread and tokio workers are
   not pinned.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| Memory budgeting | Runtime-wide; `src/network/server.rs`, `src/network/data_server.rs` | Global budget + per-path limits |
| Backpressure | `src/network/server.rs`, `src/network/data_server.rs` | Admission control |
| WAL group commit | `src/consensus/raft_storage.rs` | fsync coalescer |
| Snapshot compression | `src/consensus/raft_storage.rs` | Workstream B prerequisite |
| Slab index persistence | `src/storage/manager.rs`, `src/storage/core.rs` | Avoid full rescan |
| Background scrubber | `src/storage/`, new scrubber module | L1 + L2 integrity |
| Benchmarks | `benches/` (new) | criterion or custom |
| NUMA/affinity | `src/storage/io_uring.rs`, `src/main.rs` | Thread pinning |

---

## Ordered Implementation Steps

### H.1 — Memory Budgeting and Backpressure

| # | Step | Target | Details |
|---|------|--------|---------|
| H.1.1 | Global memory budget tracker | New utility | Track allocated bytes across: in-flight writes, read buffers, L2 transfer buffers, idempotency cache. Expose current usage as a metric. |
| H.1.2 | Backpressure on write path | `src/network/data_server.rs` | When in-flight writes exceed budget, return a `BUSY` / `OVERLOADED` status to the client instead of accepting. |
| H.1.3 | Bounded Arrow buffers on read | `src/network/data_server.rs` | `execute_query_with_migration_merge` should stream batches rather than collecting into one. Cap per-query memory. |
| H.1.4 | Streaming L2 transfers | `src/storage/sweeper.rs`, `src/storage/manager.rs` | Tier uploads: stream from disk to S3 without full in-memory copy. L2 reads: stream from S3 to disk (already partially done in rehydration, but verify). |
| H.1.5 | OOM test | `tests/` | Pathological client sends unbounded writes. Verify node returns BUSY rather than crashing. |

### H.2 — WAL Group Commit

| # | Step | Target | Details |
|---|------|--------|---------|
| H.2.1 | fsync coalescer | `src/consensus/raft_storage.rs` | Accumulate pending WAL appends for a short window (e.g., 1ms or until N entries). Issue one fsync for the batch. Wake all waiters. |
| H.2.2 | Benchmark: group commit vs. per-entry fsync | `benches/` | Measure throughput and p99 latency under concurrent Raft appends with and without coalescing. |

### H.3 — Persistent Slab Index

| # | Step | Target | Details |
|---|------|--------|---------|
| H.3.1 | Design index persistence strategy | — | Options: (a) periodic checkpoint of DashMap to a file, (b) persistent key-value store (redb, sled), (c) write-ahead index entries alongside slab records. Option (a) is simplest; option (b) is most robust. |
| H.3.2 | Implement persistent index | `src/storage/manager.rs`, `src/storage/core.rs` | On startup, load index from persistent store instead of scanning all slabs. On write, update persistent index alongside slab append. |
| H.3.3 | Fallback: full rescan if index corrupt | `src/storage/manager.rs` | If persistent index fails validation (checksum), fall back to current full-scan behavior. Log loudly. |
| H.3.4 | Benchmark: restart time with persistent index vs. full scan | `benches/` | Measure with 1k, 10k, 100k slabs. |

### H.4 — Background Scrubber

| # | Step | Target | Details |
|---|------|--------|---------|
| H.4.1 | L1 scrubber | `src/storage/` (new) | Periodic walk of all L1 slab files. Read each record, verify blake3 checksum. Report corrupt records via metrics + tracing. Quarantine corrupt records (mark as unreadable, don't delete). |
| H.4.2 | L2 scrubber | `src/storage/` (new) | Periodic download of L2 objects, verify against stored manifest hash. Report mismatches. Rate-limit to avoid S3 cost spikes. |
| H.4.3 | Scrub scheduling | Config | Configurable interval (e.g., weekly). Scrub should be low-priority and yield to production I/O. |
| H.4.4 | Repair workflow | Documentation + metrics | When corruption is found: surface it via metrics/alert, operator can trigger replica repair (read from a healthy replica and overwrite the corrupt copy). Automatic repair is a stretch goal. |

### H.5 — Benchmarks

| # | Step | Target | Details |
|---|------|--------|---------|
| H.5.1 | Benchmark framework setup | `benches/` | Use criterion. Benchmarks for: single-record write (local), quorum write (3-node), point read (L1 hit), point read (L2 miss), query scan. |
| H.5.2 | Quorum write throughput | `benches/` | Saturate with concurrent writes. Measure ops/sec and p50/p99 latency. |
| H.5.3 | Cold-read latency | `benches/` | Read a tiered record from L2. Measure end-to-end latency. |
| H.5.4 | Restart time vs. data volume | `benches/` | Measure startup time with increasing slab count. Establish the scaling curve. |
| H.5.5 | CI regression gate | CI config | Run benchmarks in CI, alert on >10% regression. |

### H.6 — NUMA and CPU Affinity

| # | Step | Target | Details |
|---|------|--------|---------|
| H.6.1 | Pin io_uring thread to core | `src/storage/io_uring.rs` | Use `core_affinity` or `nix::sched::sched_setaffinity`. Configurable via TOML. |
| H.6.2 | Tokio worker thread pool sizing | `src/main.rs` | Explicit worker count rather than default. Document recommendation for NUMA-aware deployments. |
| H.6.3 | Benchmark: pinned vs. unpinned | `benches/` | Measure write throughput on a multi-socket system with and without pinning. |

---

## Test Plan

| Test | Type | What it proves |
|------|------|----------------|
| OOM test (pathological client) | Integration | Backpressure triggers, node survives |
| Group commit throughput | Benchmark | Coalescing improves throughput |
| Persistent index restart time | Benchmark | Restart is faster than full scan |
| Scrubber detects corruption | Unit | Injected bit-flip is caught |
| Cold read latency | Benchmark | L2 read meets target SLO |

---

## Rollout and Migration Concerns

- **Persistent index format.** If using a persistent store (redb/sled),
  this adds a new dependency and a new file to the data directory. Nodes
  upgrading from pre-index code will do a one-time full scan to populate
  the index on first start.
- **Group commit behavior change.** The coalescer introduces a small
  write latency increase (up to the coalesce window). Make the window
  configurable and disable-able for latency-sensitive deployments.
- **Scrubber I/O impact.** The scrubber generates read I/O that competes
  with production reads. Needs I/O priority (ionice) or rate limiting.

---

## Failure Modes and Rollback

- **Persistent index corruption.** Fallback to full scan ensures the
  node still starts. Log the corruption loudly for investigation.
- **Group commit coalescer hangs.** If the coalescer blocks (e.g., fsync
  hangs on a failing disk), all pending writes stall. Need a timeout
  that fails the batch after N seconds rather than waiting forever.
- **Scrubber false positive.** If the scrubber reports corruption that
  isn't real (e.g., a slab that's being actively written), it could
  trigger unnecessary alerts. The scrubber should skip active/open slabs.

---

## Exit Criteria

- OOM test (pathological client) triggers backpressure, not crash.
- Restart time is sub-linear in data volume (persistent index).
- Benchmark numbers are documented and tracked in CI.
- Scrubber detects injected corruption in both L1 and L2.

---

## Open Questions

1. **Persistent index store:** redb, sled, periodic checkpoint file, or
   something else? redb is simple and embeddable. sled has a larger API
   but more maintenance history. A checkpoint file is simplest but has
   a staleness window.
2. **Group commit window:** What's the right coalesce interval? 200us?
   1ms? Adaptive based on load? Need benchmarks to inform this.
3. **Scrubber repair automation:** Should the scrubber automatically
   trigger replica repair when it finds corruption, or only alert and
   leave repair to the operator? Automatic repair is safer but adds
   complexity and I/O load.
4. **Memory budget enforcement mechanism:** Use a global `Arc<AtomicUsize>`
   that all paths check? Or a semaphore-based approach with permits?
   The former is simpler; the latter provides natural queuing.
5. **Slab fsync group-commit vs. O_DIRECT.** Gemini flagged that O_DIRECT
   alone does not guarantee media durability without explicit fsync,
   especially on drives without power-loss protection. Need to clarify:
   is the target hardware PLP NVMe (where O_DIRECT is sufficient) or
   general-purpose SSDs (where fsync is required)? This affects both the
   WAL and the slab append path.
