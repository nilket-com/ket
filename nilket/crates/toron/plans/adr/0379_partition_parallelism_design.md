# ADR 379 - partition parallelism design (design-first, decides 380; no engine behavior)

## Context

The big systemic lever, design-first. 373 confirmed one-thread execution; 374-378 removed the algorithmic waste so the
time cluster's residual is now **real** spill/read/write/Arrow work. The smallest credible first parallel target is the
**partitioned spill/join family** (q07 + q03/q05/q10) - partition-shaped, already isolated by 377/378. Head start: the
runtime is **already multi-thread** (`new_multi_thread`: cli.rs:414, daemon.rs:542) but the executor runs **inline**
(zero `spawn`/`spawn_blocking`/`rayon`/`par_iter` in `exec/`), so cores idle. Parallelism = make the executor use the
idle workers. **This ADR decides the design and chooses 380's mechanism; it ships no engine behavior** (only a throwaway
spike).

## Decision

### 1. The first parallel seam
Per-partition within the partitioned spill/join family: each partition's build-index + probe is independent. Not
per-operator/per-morsel - the partition seam is the smallest credible first target.

### 2. Determinism / order restoration (the first acceptance line for 380)
The digest oracle compares exact output, so parallel output must be **bit-identical to sequential**: collect each
partition's output into a **partition-indexed slot**, **emit in ascending partition order** regardless of completion,
and keep per-partition reduction order sequential (no hash-seed / float-order nondeterminism). **"Partition-indexed
slots, ascending-order emit, digest parity" is the FIRST acceptance line of every implementation ADR in this arc.**

### 3. Runtime boundary - decided by a tiny spike, not a menu
Partition work = async spill I/O + CPU build/probe. Run a **throwaway spike**: 2-4 synthetic partition tasks with mixed
async spill read/write + CPU probe work, comparing **tokio::spawn**, **spawn_blocking**, and **rayon** only enough to
expose failure modes (async-worker starvation, blocking-pool unboundedness, rayon<->tokio handoff). The ADR's output is
**"380 uses X because Y"** - a chosen mechanism with the failure-mode rationale - not an abstract list. The spike is
off-by-default/throwaway; it ships no engine behavior.

### 4. The operational K-bound (parallelism <-> memory)
378 made the invariant one-partition-resident; K concurrent partitions => K indices resident at once. K is **operational**:

```
K = min(configured_parallelism, available_cores,
        floor(parallel_memory_budget / measured_partition_resident_peak))
```

Phase 0 must define:
- **where `measured_partition_resident_peak` comes from** for the target path (the spilled inner-build per-partition
  resident: index bytes + writer/page, the 374-counted resident);
- **how it's obtained** - measured from the first partition at runtime, estimated from 378 telemetry, or pessimistically
  bounded - and the choice's rationale (under-estimate risks budget breach; over-estimate wastes parallelism);
- **what `parallel_memory_budget` is** (a slice of the resident budget reserved for concurrent partitions, not a cap
  bump);
- **K < 2 behavior:** the parallel path is **disabled, sequential fallback runs, and no speedup is claimed** - never a
  fake-parallel path.

### 5. Canary set
**q07 + q03/q05/q10**. q12 (sizing residual), q17/q19/q21 (other shapes), memory-tail cells - later.

## Acceptance (design ADR - decisions + invariants, not implementation)
- **Runtime boundary chosen via the spike, stated as "380 uses X because Y"** with the failure-mode evidence.
- **The K-bound is operational** (the formula above) with Phase 0 defining the resident-peak source/method,
  `parallel_memory_budget`, and the **K < 2 sequential-fallback** behavior.
- **Determinism gate specified** as the first acceptance line for 380 (partition-indexed slots, ascending emit, digest
  parity == sequential).
- **The first implementation cut (380) is named** with its canary set (q07/q03/q05/q10).
- **No engine behavior shipped** (only the throwaway spike); `--no-fail-fast` green; de-branded; no qNN; no cap bump.

## Guardrails
- **Design-first** - the deliverable is the chosen mechanism + operational K-bound + determinism gate + named 380, not
  engine code.
- **Spike decides the boundary** - "X because Y" with failure modes; no abstract menu shipped.
- **K-bound is operational and budget-safe** - K from measured resident peak; K<2 => sequential fallback, no fake
  speedup; `parallel_memory_budget` is a reservation, not a cap bump.
- **Determinism is non-negotiable** - parallel == sequential (digest), the first 380 acceptance line.
- **Reuse the multi-thread runtime; --no-fail-fast; de-branded; no qNN.**

## Risks
- **Resident-peak under-estimate** -> budget breach under K parallelism; Phase 0 picks the estimation method
  conservatively (pessimistic bound or first-partition-measured).
- **Wrong runtime boundary** -> starvation / handoff cost; the spike exposes it before the arc commits.
- **Nondeterminism leak** -> digest mismatch; partition-indexed ascending emit + sequential per-partition reduction.
- **K<2 fake-parallel** -> sequential fallback, no speedup claim.
- **Scope creep into other shapes** -> canary set fixed to the partition family.

## Forward arc
379 (design: mechanism + K-bound + determinism gate) -> **380 (first parallel cut: q07/q03/q05/q10, per-partition
concurrency, K-bounded, determinism-gated, "uses X because Y")** -> re-profile/measure speedup vs DuckDB -> extend to
q17/q19/q21 + other shapes -> memory-tail cells (q02/q08/q09/q18/q22/q21). The design here sets the invariants the whole
parallelism arc rides on.
