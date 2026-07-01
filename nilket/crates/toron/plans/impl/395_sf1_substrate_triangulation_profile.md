# ADR 395 - Implementation plan: sf=1 substrate triangulation + materialized q03 profile

Pairs with `plans/adr/395_sf1_substrate_triangulation_profile.md`. Diagnostic-only: value gate -> sf=1 four-way ->
profile the Rune q03 materialized substrate and name the cost. No `crates/toron/src` change; any harness loop is
temporary/off-by-default.

## Phase 0 - concrete value gate (feature-gated, reproducible)
- Add a `#[cfg(feature = "polars-sql-reference")]` harness test/helper, e.g.
  `polars_sql_q03_value_compatible_with_oracle`, that:
  - runs `polars_sql` q03 (the Rust explicit-join text) at sf=1,
  - loads the q03 oracle / DuckDB result on the same data,
  - compares with **decimal tolerance** (scale 2 vs 4) and the **q03 tie-order policy** (revenue DESC, o_orderdate, +
    deterministic tiebreak) - not bare row/schema equality.
- Record the exact command/test invocation in the results doc. **Gate: this must pass before any ratio is reported.**

## Phase 1 - sf=1 four-way
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch \
  --features polars-sql-reference --release
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr395_q03_sf1_fourway
```
- Medians + 356 stability gate. Headline `Rune/Polars`, `Toron/Polars`; flag DuckDB-denominator ratios if DuckDB unstable.

## Phase 2 - profile the materialized substrate (Rune q03), correctly
- **Not `perf --runs N`.** Use one of:
  - `perf record ... -- target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 1
    --data-source parquet ...`, or
  - a temporary **off-by-default** harness path (e.g. `TORON_TPCH_PROFILE_LOOP=<N>` on `perf-run-one`) that
    prepares/binds once and executes Rune q03 N times in one child; warm once; sample only the execute loop. Mark it a
    profiling path, **not** a perf metric row. Do not commit it on by default.
- Profiling capture (inside the fixed-cap scope):
```bash
systemd-run --user --same-dir --scope -p MemoryMax=4G -p MemoryHigh=4G -p TasksMax=512 -- \
  perf record -F 999 --call-graph dwarf -o target/tpch/adr395_rune_q03.perf.data -- \
  target/release/toron-tpch perf-run-one --engine rune --query q03 --mode strict --sf 1 \
    --data-source parquet --data-dir target/tpch/data   # + profiling-loop env if used
# symbol gate BEFORE trusting frames:
readelf -S target/release/toron-tpch | rg -i 'debug|symtab'
perf report -g -i target/tpch/adr395_rune_q03.perf.data --stdio | head -120
```
- **Verify DWARF overhead:** compare the profiled wall to the unprofiled execute_ms; if inflated/distorted, drop to
  `-F 499` and loop longer.
- **Materialized-path gate:** from the same run, record `spill_bytes` (expect 0), the producer/path note (no
  scan-output / streamed-scan-build), and the plan fingerprint. If streamed, label streaming-substrate and re-pin.

## Deliverable (results doc)
`crates/toron/plans/impl/395_sf1_substrate_triangulation_profile_results.md`:
- Phase 0 value-gate command + verdict (decimal tolerance + tie-order);
- the sf=1 four-way table + `engine_version` + stability verdict;
- ratios `Rune/Polars`, `Toron/Polars`, `Polars/DuckDB`, `Rune/DuckDB`, `Toron/DuckDB`, `SQL/Rune` (DuckDB-denominator
  flagged if noisy);
- the materialized-path proof (spill_bytes=0, producer, fingerprint);
- the **named substrate-cost table** (dominant frames %, symbol-gated) + the DWARF-overhead check;
- the next precision-strike target;
- confirmation: no engine change, profiling path temporary/off-by-default, no committed `.perf.data`, no qNN.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --features polars-sql-reference --all-targets
scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference   # incl. the value-gate test
scripts/cargo-cgroup test -p toron --no-fail-fast                          # unchanged engine
# Phase 0 value gate passes; Phase 1 four-way; Phase 2 profile via perf-run-one/off-by-default loop (not perf --runs N)
git diff --check
```

## Risk / blast radius
- **Profiling the rail** -> perf-run-one / execute-only loop, never `perf --runs N`.
- **DWARF distortion** -> verify vs unprofiled wall; `-F 499` + longer loop.
- **Silent sf=1 streaming** -> spill_bytes/producer/fingerprint gate.
- **Weak value gate** -> decimal tolerance + tie-order, reproducible, Rust path.
- **Profiling loop left on** -> temporary, off-by-default; not a committed default path.

## Seal
`plan: adr 395 sf1 substrate triangulation profile` (the dual). Measurement + profile (+ the value-gate test, and an
off-by-default profiling loop if used) seal as `impl: adr 395 ...`. Do not commit `.perf.data`.
