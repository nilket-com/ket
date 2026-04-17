# Workstream G — Fault Injection and Correctness Proving

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_execution_20260417.md` (Phase 7)
**Status:** First pass
**Subsystems:** `tests/`, new `tests/fault_injection/` module
**Depends on:** Workstreams A (integration gate), B (durability), C (tiering)

---

## Problem Statement

The test suite contains no fault injection beyond a single fsync-crash
test. openraft provides theoretical Raft correctness guarantees, but
there is no harness asserting linearizability under network partition,
node crash, or disk failure. For a system that will hold mission-critical
data, "the framework guarantees it" is not sufficient — the integration
must be proven under adversarial conditions.

The "fix whatever it finds" tail (G.9) is the unbounded part of the
entire project plan. History shows that deterministic fault injection
in storage systems routinely surfaces bugs that unit tests miss.

---

## Code Ownership / Subsystem Boundaries

| Area | Primary Files | Notes |
|------|--------------|-------|
| Simulator harness | `tests/fault_injection/` (new) | madsim, turmoil, or custom |
| Partition tests | `tests/fault_injection/` | Network partition scenarios |
| Crash tests | `tests/fault_injection/` | Node crash at specific points |
| Tiering fault tests | `tests/fault_injection/` | L1→L2 crash scenarios |
| Soak test | `tests/fault_injection/` | Long-running randomized test |

**Boundary note:** This workstream only writes tests and fixes bugs they
find. It does not add features. Bugs found may require fixes in any
subsystem.

---

## Ordered Implementation Steps

### G.1 — Harness Selection and Setup

| # | Step | Details |
|---|------|---------|
| G.1.1 | Evaluate madsim vs. turmoil vs. custom | madsim: rewrites std/tokio, full deterministic simulation, used by RisingWave. turmoil: lighter, Tokio-native, less mature. Custom: most control, most effort. Recommend madsim if io_uring usage can be abstracted behind a trait boundary for simulation. |
| G.1.2 | Set up simulator harness | Create `tests/fault_injection/` with helper functions: `create_cluster(n)`, `partition(nodes_a, nodes_b)`, `heal_partition()`, `crash_node(id)`, `restart_node(id)`, `write(key, value)`, `read(key) -> value`. |
| G.1.3 | Seed-based deterministic replay | All random choices (partition timing, crash points, key selection) derive from a single seed. Failed tests print the seed for reproduction. |

### G.2 — Partition Tests

| # | Step | Details |
|---|------|---------|
| G.2.1 | Majority partition survives | Partition {1,2} from {3}. Write to majority side. Read succeeds. Verify minority side rejects writes. |
| G.2.2 | Minority cannot accept writes | During partition, attempt write to minority side. Must fail or timeout, not silently succeed. |
| G.2.3 | Rejoining minority catches up | Heal partition. Verify node 3 catches up to the writes that happened during partition. All nodes converge to the same state. |
| G.2.4 | Leader in minority steps down | Partition so the current leader is in the minority. Verify majority elects a new leader. Old leader stops accepting writes. |
| G.2.5 | Symmetric partition (all isolated) | All 3 nodes isolated. No writes succeed. Heal, verify cluster recovers. |

### G.3 — Crash Tests

| # | Step | Details |
|---|------|---------|
| G.3.1 | Crash during `append_durable` between WAL and slab | Kill a replica after WAL fsync but before slab fsync (or vice versa). Verify no acknowledged write is lost after recovery. |
| G.3.2 | Crash during replication fan-out | Kill one replica mid-replication. Leader should still achieve quorum with remaining replicas. Restarted replica catches up. |
| G.3.3 | Leader crash during snapshot install | Kill the leader while it's installing a snapshot. New leader elected. Verify state machine consistency. |
| G.3.4 | Crash during WAL compaction | Kill a node mid-WAL-compaction (Workstream B). Verify recovery produces correct state with no data loss or corruption. |
| G.3.5 | Crash during slab rotation | Kill a node while rotating to a new slab. Verify recovery handles partial rotation correctly. |
| G.3.6 | Simultaneous crash of minority | Kill RF/2 nodes simultaneously. Majority survives. Restart killed nodes, verify catch-up. |

### G.4 — Tiering Fault Tests

| # | Step | Details |
|---|------|---------|
| G.4.1 | Crash after L2 upload, before Raft commit | Verify orphan is cleaned by orphan GC (Workstream C). |
| G.4.2 | Crash after Raft commit, before L1 unlink | Verify L1 file is cleaned on restart reconciliation. |
| G.4.3 | S3 transient failure during upload | Simulate S3 returning 500/503. Verify retry succeeds and no orphans. |
| G.4.4 | S3 permanent failure during upload | Simulate persistent S3 failure. Verify sweeper backs off and does not spin. Slab remains in L1. |

### G.5 — Linearizability Soak Test

| # | Step | Details |
|---|------|---------|
| G.5.1 | Design workload generator | Random mix of writes and reads. Each write increments a per-key counter. Each read verifies the counter is monotonically non-decreasing (linearizability check). |
| G.5.2 | Random fault injector | At random intervals (seeded): partition a random subset, crash a random node, inject S3 failure. Heal/restart after random delay. |
| G.5.3 | Run for 10,000+ logical operations | Verify zero linearizability violations. Log all operations and fault events for post-hoc analysis. |
| G.5.4 | CI integration | Run soak test in CI with a fixed seed and moderate operation count (1,000). Full soak runs nightly or on-demand. |

### G.9 — Fix Whatever the Above Finds

This is explicitly unbounded. Each bug found gets:
- A minimal reproduction (with simulator seed).
- A targeted fix in the relevant subsystem.
- The reproduction added as a permanent regression test.

---

## Test Plan

This workstream IS the test plan. The exit criteria below define the
coverage bar.

---

## Rollout and Migration Concerns

- **Simulator dependency.** madsim is a heavy dependency that may
  conflict with production code's direct use of io_uring and std. May
  require abstracting the io layer behind a trait that can be swapped
  for a simulated implementation.
- **CI resource requirements.** The soak test is CPU-intensive. May need
  a dedicated CI runner or a nightly-only schedule for the full soak.
- **Flaky tests.** Deterministic simulation should eliminate flakiness,
  but only if ALL sources of nondeterminism are controlled (timestamps,
  thread scheduling, random number generation). Verify this during
  harness setup.

---

## Failure Modes and Rollback

Not applicable — this workstream only produces tests. There is no
production code to roll back.

The risk is that bugs found in G.9 require changes to production code
that have their own rollout concerns. Those concerns belong to the
workstream that owns the affected code.

---

## Exit Criteria

- Soak test runs for 10,000+ logical operations under random fault
  injection with zero linearizability violations.
- All partition scenarios (G.2.1–G.2.5) pass.
- All crash scenarios (G.3.1–G.3.6) pass.
- All tiering fault scenarios (G.4.1–G.4.4) pass.
- Every bug found has a permanent regression test with a fixed seed.

---

## Open Questions

1. **Simulator choice:** madsim vs. turmoil vs. custom? The key blocker
   is whether io_uring usage can be abstracted for simulation. If the
   io_uring thread is tightly coupled to the storage path, madsim may
   not be able to intercept it. Need to evaluate the abstraction boundary.
2. **io_uring simulation.** The storage path uses raw io_uring via
   `src/storage/io_uring.rs`. Can this be swapped for a simulated
   implementation that the fault injector controls (injecting disk
   errors, latency, partial writes)?
3. **S3 simulation.** For tiering fault tests, need a simulated S3 that
   can be configured to fail deterministically. Options: mock
   `ObjectStore` trait impl, or simulated HTTP endpoint.
4. **Linearizability checker.** Use an existing checker (like Jepsen's
   Knossos) or build a simple monotonic-counter checker? The counter
   approach is simpler but only checks a subset of linearizability
   properties.
5. **Coverage bar for CI vs. nightly.** How many operations in CI
   (speed-constrained) vs. nightly (thoroughness-constrained)? 1,000 in
   CI and 100,000 nightly?
