# ADR 379 - Design-execution plan: partition parallelism design

Pairs with `plans/adr/0379_partition_parallelism_design.md`. This is a **design ADR**: run a throwaway runtime-boundary
spike, define the operational K-bound, fix the determinism gate, and name 380. **No engine behavior ships** (the spike is
off-by-default/throwaway and is removed or left disabled).

## Phase 0 - the runtime-boundary spike (decide "380 uses X because Y")
- Build a throwaway harness: 2-4 synthetic partition tasks, each doing mixed **async spill read/write** (the real
  `AsyncFileOps` spill page path) + **CPU build/probe** (a synthetic hash index + probe over representative rows).
- Compare **tokio::spawn**, **spawn_blocking**, **rayon** on the existing multi-thread runtime - enough to expose:
  - tokio::spawn: async-worker starvation when CPU spans occupy workers;
  - spawn_blocking: blocking-pool unboundedness / overhead;
  - rayon: rayon<->tokio async-I/O handoff cost / nesting.
- Output: **one chosen mechanism with the failure-mode rationale** ("380 uses X because Y"). The spike code does not
  ship (throwaway or `#[cfg(test)]`/off-by-default).

## Phase 0 - the operational K-bound
- Define `K = min(configured_parallelism, available_cores, floor(parallel_memory_budget / measured_partition_resident_peak))`.
- **`measured_partition_resident_peak`:** the per-partition spilled inner-build resident (the 374-counted
  `resident_index_bytes` + writer/page). Decide its source:
  - measured from the first processed partition (adaptive), OR
  - estimated from 378 telemetry (partition spill bytes -> index bytes), OR
  - pessimistically bounded (largest partition).
  Record the choice + rationale (under-estimate => budget breach; over-estimate => wasted parallelism).
- **`parallel_memory_budget`:** a reservation carved from the existing resident budget for concurrent partitions - **not
  a cap bump**.
- **`configured_parallelism`:** a config knob (default = available_cores), so it can be pinned to 1 for determinism
  debugging.
- **K < 2:** parallel path disabled, sequential fallback (today's path), **no speedup claimed**.

## Phase 0 - the determinism gate (for 380)
- Specify: per-partition outputs into partition-indexed slots; emit in **ascending partition order**; per-partition
  reduction order stays sequential; hash seeds fixed. The 380 acceptance line: **parallel output digest == sequential
  output digest**, plus `configured_parallelism=1` reproduces today exactly.

## Deliverable (this ADR)
A decided design recorded in the results doc:
1. **Runtime boundary:** "380 uses X because Y" + the spike numbers/failure modes.
2. **K-bound:** the formula + the resident-peak source/method + `parallel_memory_budget` reservation + K<2 fallback.
3. **Determinism gate:** the partition-indexed/ascending/digest-parity spec as 380's first acceptance line.
4. **Canary + first cut:** 380 = q07/q03/q05/q10, per-partition concurrency, K-bounded, determinism-gated.

No `crates/toron` engine behavior changes here.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets        # spike compiles if kept off-by-default; else not present
scripts/cargo-cgroup test -p toron --no-fail-fast        # unchanged behavior (spike ships nothing)
git diff --check
# the spike runs are recorded in the results doc (numbers + chosen mechanism); spike code throwaway/off-by-default
```

## Results doc
`crates/toron/plans/impl/0379_partition_parallelism_design_results.md`:
- the spike: tokio::spawn vs spawn_blocking vs rayon - numbers + failure modes + **the chosen mechanism + why**;
- the operational K-bound: formula, resident-peak source/method, `parallel_memory_budget`, K<2 fallback;
- the determinism gate spec (380's first acceptance line);
- 380's named scope (q07/q03/q05/q10, per-partition concurrency, K-bounded, determinism-gated);
- confirmation no engine behavior shipped, spike throwaway/off-by-default, `--no-fail-fast` green, no qNN, no cap bump.

## Risk / blast radius
- **Spike misleads** - keep it representative (real async spill path + representative CPU); it only chooses failure
  modes, 380 validates for real.
- **Resident-peak estimate wrong** - conservative method; K<2 sequential fallback as the floor.
- **Design ships behavior accidentally** - the spike is throwaway/off-by-default; `--no-fail-fast` proves unchanged.

## Seal
`plan: adr 379 partition parallelism design` (the dual). The design-execution / spike, if any code is retained, seals as
`impl: adr 379 ...` off-by-default; otherwise the results doc carries the decision. Do not commit throwaway spike code or
large profile artifacts.
