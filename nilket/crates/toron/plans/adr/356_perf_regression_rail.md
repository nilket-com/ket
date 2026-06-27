# ADR 356 - the perf/regression rail (Toron-only; methodology first)

## Status
Proposed.

## Context

Step 2 of the scale arc (decomposition banked at 354; 355-context shelved). Build the **perf/regression rail** - the
measuring stick for the DuckDB-class goal and the sf=10 precursor. **Make Toron's own perf rail trustworthy first**;
DuckDB becomes an adapter once the measuring stick is credible. Grounding-first (the noise-control methodology is the
deliverable), and **measure, don't optimize** - 356 builds the rail + baseline; perf optimizations are later ADRs gated
by it. No engine behavior change.

## Decision - a Toron-only `perf` subcommand: phase timings, fresh-process runs, recorded board

### The metric correction (Codex) - do NOT reuse `elapsed_ms`
`elapsed_ms` (lib.rs:1422) wraps `run_query_with_tables` - it starts before load/plan/bind/execute and includes digest
timing. **Keep it as reachability telemetry; it is not a perf metric.** ADR 356 adds **discrete phase timings**:
`load_ms`, `plan_ms`, `bind_ms`, `execute_ms`, `digest_ms`, `total_ms`. **The primary perf metric is `execute_ms`** -
gen/load/plan/bind/digest excluded (the q02 13s->1.4s lesson). The timers must be added to the existing shared
Load->Plan->Bind->Execute substrate, not by cloning a separate perf-only execution path.

### Fresh-process measurement (Codex) - RSS + allocator isolation
Each measured run executes in a **fresh child process** (the parent `perf` driver re-execs a hidden single-run mode).
The child loads the pre-generated dataset + runs the query, reporting its **internal execute-only phase timings** +
digest + plan fingerprint + spill bytes (JSON on stdout). The **parent records peak RSS / cgroup stats** of the child
(e.g. `ru_maxrss` / cgroup `memory.peak`) + parent-observed wall time. The child's stdout is exactly one JSON record;
diagnostics/logging go to stderr so the parent can parse deterministically. In-process repeats contaminate peak RSS +
allocator state, so each measurement is a clean process. (If fresh-process proves infeasible, fall back to in-process
with the RSS caveat recorded - but the design target is fresh-process.)

### The noise-control methodology (the load-bearing deliverable)
- **Release-only** (debug ~9x off).
- **N runs + warmup:** `--runs N` measured (default ~7) after `--warmup K` discarded (default 1, cold-cache); report
  **median + min + spread** (stddev or min/max). A single run is not a measurement.
- **Stability gate:** if spread exceeds a threshold, flag the sample untrustworthy (re-run), don't bank noise as truth.
- **Fixed environment:** cgroup-pinned (the existing wrapper), fixed sf/mode, parallelism documented.
- **Regression band wider than the noise floor:** flag a delta only when it exceeds `max(threshold, k*spread)` -
  **recorded flags only, no hard CI gate** until the rail proves stable.
- **Release enforcement:** the perf child rejects or clearly marks `debug_assertions` builds; debug measurements are not
  admitted to the baseline.

### The harness surface (Codex) - a dedicated `perf` subcommand
`perf --sf 1 --mode strict --runs 7 --warmup 1 --query all` - keep `matrix`/`correctness-matrix` clean. Output: **raw
per-run JSONL** (every run, every metric) + an **aggregated summary board** (median/min/spread per query, with
regression flags vs the recorded baseline).

### The metric record (per run)
`engine` (`toron` now; `duckdb` reserved), query, sf, mode, run index, `load_ms`/`plan_ms`/`bind_ms`/`execute_ms`/
`digest_ms`/`total_ms`, optional `parent_wall_ms`, **peak RSS**, **spill bytes**, **plan fingerprint** (stable
physical-plan hash - the silent de-optimization catcher), **digest** (correctness cross-check). `execute_ms` is primary.
`spill_bytes` must be taken from `TaskTelemetry` installed on the executor context; the current TPC-H path returns only
`Vec<RecordBatch>`, so Phase 0 must prove the extraction seam instead of assuming the counter is already on the record.
Plan changes are flagged separately from timing regressions: compare runtime deltas only within the same fingerprint, or
emit `plan_changed` alongside the timing delta.

### DuckDB (Codex) - schema-ready now, adapter later
356 is **Toron-only**. The row schema carries an `engine = toron|duckdb` column so the board is DuckDB-ready, but **live
DuckDB is deferred to a follow-up adapter**. When added, **CLI shell-out is preferred over the Rust crate** (version
transparency + dependency isolation). The DuckDB-killer comparison lands once the Toron measuring stick is proven.

## Acceptance (a trustworthy, recorded Toron perf rail)
- A `perf` subcommand (release/cgroup, fresh-process runs, `--runs`/`--warmup`/`--query`/`--sf`/`--mode`).
- **Discrete phase timings with `execute_ms` primary** (not `elapsed_ms`); `elapsed_ms` stays reachability-only.
- **Fresh-process RSS** (parent measures child peak RSS) + spill bytes + plan fingerprint + digest, per run.
- **Raw JSONL + summary board** (median/min/spread); **regression flags vs baseline, no hard CI gate**; stability gate.
- A **recorded Toron sf=1 baseline** (all 22, release/cgroup/fresh-process, noise-controlled; debug rejected/marked).
- **engine column** present (DuckDB-ready); live DuckDB deferred.
- **No engine behavior change** - the rail measures; the sf=1 reach/correctness board stays 22/22 bit-identical.

## Guardrails
- **`execute_ms` primary, not `elapsed_ms`** - the reachability timer is not the perf metric.
- **Fresh-process isolation** - peak RSS + allocator state per measurement; no in-process contamination.
- **Release + cgroup + N-runs median/min/spread + stability gate** - the noise model precedes any banked number.
- **Recorded flags only** - no hard CI red until the rail proves stable; the regression band exceeds the noise floor.
- **Plan fingerprint discipline** - changed plans are their own flag (`plan_changed`), not silently mixed into
  same-plan perf regression math.
- **Toron-only** - DuckDB schema-ready but deferred; no live-DuckDB dependency in 356.
- **Measure, don't optimize / no engine behavior change** - sf=1 board bit-identical; optimizations are later gated ADRs.

## Risks
- **Noisy single-shot numbers** - the N-runs + spread + stability gate prevent banking noise.
- **`elapsed_ms` reuse** - explicitly rejected; discrete phase timings, `execute_ms` primary.
- **In-process RSS contamination** - fresh-process design; in-process only as a recorded-caveat fallback.
- **Premature DuckDB** - deferred; the measuring stick is proven on Toron first.
- **Hard-gate flakiness** - recorded flags only until stable.

## Forward arc
356 (Toron perf rail + methodology + sf=1 baseline) -> the **DuckDB adapter** (live, CLI shell-out, the `engine` column
populated) -> **perf-optimization ADRs** (each gated: no regression beyond the noise band) -> the **sf=10 atlas**
(re-measure under the rail; the streaming substrate's constant-memory + wall-clock at 10x) -> **Rune-dataframe TPC-H**
(the second frontend on the same oracles + rail). No central roadmap; each a Toron-side ADR.

## Why toron-home
The gauge harness (`crates/toron-tpch`) + measuring `crates/toron`. No Wendelon / cross-ring surface; no engine behavior
change.

## Council provenance
Gemini + Codex + Claude. Grounding-first (methodology is the deliverable). Codex's steer (adopted): (1) **DuckDB =
recorded-reference schema now, live adapter later** (CLI shell-out preferred over the crate); (2) **regression = recorded
board with flags, raw runs + median/min/spread, no hard gate** until stable; (3) **a dedicated `perf` subcommand**
(`perf --sf 1 --mode strict --runs 7 --warmup 1 --query all`). Tightenings: **do not reuse `elapsed_ms`** - add discrete
phase timings (`load/plan/bind/execute/digest/total_ms`), `execute_ms` primary; **fresh child process per measured run**
(parent records peak RSS, child reports execute-only timing) to avoid in-process RSS/allocator contamination. Toron-only
first; DuckDB once the measuring stick is credible.
