# ADR 356 - Implementation plan: the perf/regression rail (Toron-only)

Pairs with `plans/adr/0356_perf_regression_rail.md`. Build a Toron-only `perf` subcommand in `crates/toron-tpch`:
discrete phase timings (`execute_ms` primary), fresh-process measured runs (parent records peak RSS), raw JSONL +
summary board with median/min/spread + recorded regression flags, an `engine` column (DuckDB-ready, deferred). Measure,
don't optimize; no engine behavior change.

## Phase 0 - methodology + instrumentation grounding (BEFORE the driver)
- **Confirm the `elapsed_ms` span:** verify it wraps load/plan/bind/execute(+digest) at lib.rs:1410-1422; document that
  it stays reachability-only and is NOT the perf metric.
- **Phase-timing seams:** identify where to instrument `load_ms` / `plan_ms` / `bind_ms` / `execute_ms` / `digest_ms`
  in the shared load/plan/bind/execute helper (discrete `Instant`s per phase). `execute_ms` = the execute phase only.
  Do not create a perf-only duplicate execution path; reachability/correctness/perf must share the same substrate.
- **Spill bytes seam:** the engine already records `TaskTelemetryCounters::spill_bytes`, but the TPC-H execution helper
  currently returns only `Vec<RecordBatch>`. Phase 0 must install `TaskTelemetry` on the `ExecutionContext` and prove the
  perf record can read it after execution without changing normal reachability/correctness behavior.
- **Plan fingerprint:** define a stable physical-plan fingerprint (same physical structure -> same fingerprint). Do not
  hash a nondeterministic debug dump if it includes unstable ids or addresses.
- **Fresh-process design:** a hidden single-run mode (e.g. `perf-run-one --query qNN --sf .. --mode ..`) that loads the
  pre-generated dataset, runs the query, and prints a JSON record (phase timings + digest + fingerprint + spill bytes)
  on stdout. The parent `perf` driver spawns it, reads the JSON, and records the child's **peak RSS** (`wait4`
  `ru_maxrss` / cgroup `memory.peak`) plus parent-observed wall time. The child must write exactly one JSON record to
  stdout; diagnostics/logging go to stderr. Confirm feasibility; fallback = in-process with a recorded RSS caveat.
- **Release enforcement:** make the single-run child reject or clearly mark `debug_assertions`; debug measurements cannot
  enter the recorded baseline.

## D1 - phase timings (the metric)
- Add discrete phase timers to `run_query_with_tables`; extend the telemetry/record type with `load_ms`/`plan_ms`/
  `bind_ms`/`execute_ms`/`digest_ms`/`total_ms`. Keep `elapsed_ms` for reachability unchanged.
- Surface `spill_bytes` + `plan_fingerprint` into the record.

## D2 - the `perf` subcommand + fresh-process driver
- `perf --sf 1 --mode strict --runs 7 --warmup 1 --query all|qNN`.
- Driver: for each query -> `warmup` discarded child runs, then `runs` measured child runs; each measured run is a fresh
  child (the single-run mode); parent records peak RSS + the child's JSON timings.
- Aggregate per query: median + min + spread (stddev or min/max) of `execute_ms` (+ the other phases, RSS, spill).
- **Stability gate:** flag a query's sample if spread exceeds the threshold (untrustworthy -> re-run).

## D3 - output: raw JSONL + summary board + regression flags
- **Raw JSONL:** one line per run (engine=toron, query, sf, mode, run idx, all phase timings, RSS, spill, fingerprint,
  digest, optional parent wall time) to a perf output path.
- **Summary board:** median/min/spread per query; compare `execute_ms` median against the recorded baseline; **flag**
  when delta > `max(threshold, k*spread)`. **Recorded flags only - exit 0; no hard CI red** (until the rail proves
  stable).
- **Plan-change flag:** if the plan fingerprint differs from baseline, emit `plan_changed` and do not classify the
  execute-time delta as a same-plan regression without that context.
- **engine column** present in both (DuckDB-ready); only `toron` populated in 356.

## D4 - the recorded baseline
- Run `perf --sf 1 --mode strict --runs 7 --warmup 1 --query all` (release/cgroup/fresh-process); commit the baseline
  board (median/min/spread per query) as the regression reference. Document the methodology + environment alongside it.

## Tests / safety net (behavior-neutral)
- **No engine behavior change:** `correctness-matrix` 22/22 + `matrix` 22/22 **unchanged** (the phase-timing
  instrumentation is additive; `execute_ms` measurement must not alter results).
- The phase timers sum sanity: `load+plan+bind+execute+digest <= total` (within overhead); a unit test on the
  aggregation (median/min/spread) + the stability gate + the regression-flag math (flag iff delta > band).
- Child JSON contract test: the hidden single-run mode emits one parseable JSON record to stdout; diagnostics are not
  interleaved with the record.
- Spill counter extraction test: a spilling fixture reports nonzero `spill_bytes` through the perf record.
- Plan-fingerprint stability test: the same query/sf yields the same fingerprint across runs.
- `--lib`, `--test architecture_test`, `--no-fail-fast`, `toron-tpch` green.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix        # 22/22 unchanged (no behavior change)
scripts/cargo-cgroup run -p toron-tpch -- matrix                    # 22/22 unchanged
scripts/cargo-cgroup run -p toron-tpch --release -- perf --sf 1 --mode strict --runs 7 --warmup 1 --query all
git diff --check
#   execute_ms primary; fresh-process RSS; JSONL + board + flags; baseline recorded; board bit-identical 22/22
```

## Results doc
`crates/toron/plans/impl/0356_perf_regression_rail_results.md`: the methodology (phase timings with `execute_ms` primary,
fresh-process RSS, N-runs/warmup/median/min/spread, stability gate, regression band); the `perf` subcommand + the JSONL/
board schema (engine column, DuckDB-ready); the recorded Toron sf=1 baseline (median/min/spread per query, execute_ms +
RSS + spill + fingerprint); confirmation `elapsed_ms` stays reachability-only; the behavior-neutral proof (22/22
unchanged); the deferred DuckDB adapter + the gated perf-optimization arc.

## Risk / blast radius
- **`execute_ms` not `elapsed_ms`** - the perf metric is the execute phase only.
- **Fresh-process RSS** - clean per-measurement RSS/allocator; in-process only as recorded fallback.
- **Recorded flags only** - no hard CI gate until stable; band > noise floor.
- **Toron-only** - DuckDB schema-ready, deferred (CLI shell-out when added).
- **Behavior-neutral** - additive instrumentation; sf=1 22/22 bit-identical; measure-not-optimize.

## Seal
`impl: adr 356 perf regression rail`. Do not commit wip drafts.
