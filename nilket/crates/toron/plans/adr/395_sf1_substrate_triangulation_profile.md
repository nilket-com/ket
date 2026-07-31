# ADR 395 - sf=1 substrate triangulation + materialized q03 profile

## Status
Proposed.

## Context

394 built the `polars_sql` peer instrument; 391/392/393 measured the substrate only at sf=0.01 smoke. 395 is the payoff:
the **sf=1 persisted four-way** (toron, rune, polars_sql, duckdb), the **Rust explicit-join q03 value gate proven in this
run** (not inherited from the Python probe), and a **381 symbol-retaining profile of the Toron/Rune q03 materialized
sf=1 path** to **name the substrate cost** - where the ~50-60x peer gap goes. **Diagnostic-only; no engine change.**
Rune is the primary profile target: it fixes the logical shape while using the same binder/executor/storage substrate,
so its hot stack is substrate, not planner shape; the Toron SQL profile is secondary context.

## Decision

### Phase 0 - a concrete, reproducible value gate (before any ratio)
Perf rows prove only row count / schema, not values. Add a **feature-gated (`polars-sql-reference`) Rust-path
comparator** - a harness helper/test (or a recorded command) that:
- runs `polars_sql` q03 (the Rust **explicit-join** dialect text) at sf=1,
- compares against the **q03 oracle / DuckDB** with **decimal tolerance** and the **q03 tie-order policy** (revenue DESC,
  o_orderdate, then the deterministic tiebreak),
- passes/fails reproducibly.
**No ratio is trusted until this comparator passes.** This replaces the inherited-from-Python value claim (394 note).

### Phase 1 - the four-way at sf=1 persisted
- `perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 --engines toron,rune,polars_sql,duckdb --data-source
  parquet`, release, fixed 4 GB. Medians + the 356 stability gate.
- **Headline ratios: `Rune/Polars` and `Toron/Polars`** (dominated by Toron's stable ~1.3 s, robust to peer noise).
  `Polars/DuckDB`, `Rune/DuckDB`, `Toron/DuckDB`, `SQL/Rune` recorded too; **DuckDB-denominator ratios marked noisy** if
  the stability gate flags DuckDB (392: ~37 ms, spread 0.62).
- Table: engine | status | execute_ms (median/min/spread) | engine_version | rows | correctness | producer.

### Phase 2 - profile the materialized substrate, correctly
**Do not profile `perf --runs N`** - it samples the parent rail, fresh-child startup, export/manifest, and process
churn. Profile the executor only, one of:
- `perf-run-one --engine rune --query q03 ...` directly, or
- a **temporary/off-by-default harness loop** that prepares/binds once and executes Rune q03 **N times inside one
  child** - warm once, **sample only the execute loop**, and record it as a **profiling harness path, not a perf metric
  row**.

381 method: `CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false`, hard symbol gate (`readelf -S`/
`nm -C`), `perf record --call-graph dwarf` **inside** the fixed-cap `--scope`, `perf report -g` for inlined callers.
**`-F 999` is fine but verify DWARF overhead doesn't distort wall time; drop to `-F 499` and loop longer if it does.**
Accumulate steady-state samples (q03 ~1.3 s/run -> loop to >~5-10 s).

### Phase 2 gate - prove the profiled path is materialized, not streamed
A memory-aware gate could stream q03 at sf=1 - then the profile is a *streaming*-substrate profile, not the materialized
core. **Before trusting the profile, record `spill_bytes` (expect 0), the producer / path note (no scan-output /
streamed-scan-build producer), and the plan fingerprint.** If it streamed, label it accordingly and re-pin the
materialized path.

### Deliverable - the named substrate cost
A **dominant-frame % table** from the Rune materialized profile (Arrow take/concat/materialize, per-operator
`execute_bound -> Vec` rebatch, hash build/probe, decimal arith, copies) - the precision-strike target.

## Acceptance (diagnostic-only)
- **Phase 0 value gate:** the reproducible Rust-path comparator (feature-gated) passes - polars_sql q03 == q03 oracle/
  DuckDB, decimal tolerance + tie-order; recorded command/test. No ratio before it passes.
- **sf=1 four-way table** (toron, rune, polars_sql, duckdb), execute_ms median/spread + `engine_version`, persisted
  Parquet, stability gate applied; DuckDB-denominator ratios flagged if noisy.
- **Headline `Rune/Polars` + `Toron/Polars`**; the rest recorded.
- **Profile via perf-run-one / off-by-default execute-loop, not `perf --runs N`**; symbol-gated; DWARF overhead
  verified; steady-state samples.
- **Materialized-path proof:** `spill_bytes=0` + materialized producer + plan fingerprint recorded for the profiled
  run; else labeled streaming-substrate.
- **Named substrate-cost table** (dominant frames %) identifying the next precision-strike target.
- **No engine change** (measurement + profiling; any harness loop is temporary/off-by-default); `crates/toron/src`
  unchanged; `--no-fail-fast`/`toron-tpch` unchanged; no committed `.perf.data`; no qNN.

## Guardrails
- **Value gate before ratios** - concrete reproducible Rust-path comparator, not perf-row row-count.
- **Profile the executor, not the rail** - perf-run-one / off-by-default loop; never `perf --runs N`; warm once, sample
  execute only.
- **Prove materialized** - spill_bytes=0 + producer + fingerprint; a streamed sf=1 is a different (streaming) profile.
- **Profile the optimal shape (Rune)** - isolates substrate from planner shape; SQL profile secondary.
- **Headline peer ratios** - Rune/Polars, Toron/Polars; DuckDB-denominator flagged noisy per the gate.
- **Symbol-gated; DWARF overhead verified; diagnostic-only, no engine change.**

## Risks
- **Profiling samples the rail** - perf-run-one / execute-only loop; explicitly not `perf --runs N`.
- **DWARF sampling distorts wall** - verify; `-F 499` + longer loop.
- **sf=1 silently streamed** - the materialized-path gate (spill_bytes/producer/fingerprint) catches it.
- **Value gate too weak** - decimal tolerance + tie-order, reproducible, on the Rust path.
- **Thin samples** - q03 short; loop to steady-state.

## Forward Arc
395 names where the ~50-60x materialized gap goes. Then: the dominant frame -> a 385-style **precision-strike ADR** on
that cost; re-measure `Rune/Polars` after each strike to track the gap closing toward the peer. The sf=3 **streaming**-
substrate time-wall (393) is the separate, larger thread after the materialized core is understood.

## Grounding evidence (file:line)
394 instrument: `engine=polars_sql` (feature `polars-sql-reference`), `engine_version`, explicit-join q03/q14/q19, sealed
369a77f. 393: Rune q03 sf=1 materialized = 1311.8 ms stable (spread 0.021), spill 0, 11620 rows; sf=3 streaming
time-bound. 392: SQL/Rune ~1.92x; DuckDB sf=1 noisy ~37 ms. perf rail 356; symbol-retaining profile 381; persisted
surface 360. q03 = customer/orders/lineitem + group, ORDER BY revenue DESC, o_orderdate.
