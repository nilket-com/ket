# ADR 391 results - Rune perf-decomposition probe

## Summary

ADR 391 added a real `engine="rune"` path to the `toron-tpch` perf rail for q03. The path is harness-only: Rune
authored q03 as a `LogicalPlan`, then the normal binder and executor ran it. No `crates/toron/src` engine code changed.

The probe is useful, but the sf=10 result is not the hoped-for "Rune proves a faster logical shape" branch. The
Rune-authored q03 shape is distinct from SQL and oracle-correct at small scale, but at sf=10 it hits the
`scan output accumulator` memory wall before producing a result. SQL q03 is already past that scan-output term after
ADR 390 and is censored as a time/no-row cell. So this Rune shape is not a better q03 substrate probe at sf=10; it
exposes a logical-shape reach gap in the Rune-authored plan.

## Implementation

- Added `RUNE_Q3_SCRIPT`, authored with `auto_join` over customer -> orders -> lineitem, followed by grouped aggregate,
  projection/rename, and q03 ordering.
- Added `run_rune_perf_query_with_data_source(...)`, emitting real `engine="rune"` perf rows through the same
  `perf-run-one` child-process rail.
- Refactored the harness execute path so SQL and Rune share catalog load, bind, execute, phase timings, plan
  fingerprinting, spill telemetry, digest, and row/schema telemetry.
- Added `rune_q03_isolation_proof(...)` to bind SQL q03 and Rune q03 through the same catalog/config and record bound
  physical plan fingerprints/shapes.
- Added oracle/order gating for Rune q03: a Rune pass row requires `compare_strict_q3`, not digest-only.
- Added CLI support for `--engines rune`; `--engines all` remains Toron+DuckDB because Rune is q03-only in this ADR.

## Phase 0 Isolation

Small-scale release perf rows and the isolation test prove the bound plans differ:

| path | plan fingerprint | note |
| --- | ---: | --- |
| SQL q03 | `b00940564d0c771f` | SQL-produced logical shape |
| Rune q03 | `0fd3784049e71920` | Rune-authored logical shape (`auto_join`) |

The focused test `rune_q03_isolation_proof_differs_from_sql_shape` binds both plans and fails if the fingerprints/shapes
collapse to the same plan. The first Rune draft used pinned `inner_join`; it also differed and passed the oracle, but
hit the same sf=10 scan-output wall. The final script uses `auto_join`, the cleaner comparison point.

## Correctness

Rune q03 passes the strict q03 oracle including order at sf=0.01:

| engine | status | rows | digest/schema |
| --- | --- | ---: | --- |
| toron | pass | 138 | digest `ArrowIpcDigest([172, ... 221])`, schema `fnv1a64:52ced4a401714e39` |
| rune | pass | 138 | same digest and schema |
| duckdb | pass | 138 | schema sanity flags expected cross-engine type mismatch |

The focused test `rune_q03_perf_row_matches_strict_oracle` locks the oracle/order gate. Rune does not complete at
persisted sf=10, so there is no sf=10 Rune correctness row to compare.

## Measurements

Release sf=0.01, in-memory, one run:

| engine | status | execute_ms | plan_ms | bind_ms | rows |
| --- | --- | ---: | ---: | ---: | ---: |
| toron | pass | 12.2 | 0.2 | 0.4 | 138 |
| rune | pass | 5.5 | 1.7 | 0.6 | 138 |
| duckdb | pass | 8.0 | 0.0 | 0.0 | 138 |

Release sf=10, persisted Parquet, one run / bounded probe:

| engine | status | execute_ms | reason / producer | rows |
| --- | --- | ---: | --- | ---: |
| toron SQL | censored | `>120000` | no row before bounded timeout; prior atlas class remains time/no-row | none |
| rune | fail | 744.2 | `memory_budget_exhausted`, producer `scan output accumulator`, requested 1,073,762,000 / 1,073,741,824 | none |
| duckdb | pass | 1031.0 | none | 114003 |

The Rune sf=10 raw row:

```text
engine=rune status=fail phase=execute reason=memory_budget_exhausted
producer="scan output accumulator"
execute_ms=744.197262 peak_rss_kib=1120092 spill_bytes=0
plan_fingerprint=2b72c561c2595512
```

## Ratios

At sf=0.01, where both SQL and Rune complete and the oracle passes:

| ratio | value | interpretation |
| --- | ---: | --- |
| SQL / Rune | 2.19x | small-scale logical-shape/runtime difference in favor of Rune |
| Rune / DuckDB | 0.69x | below meaningful floor; DuckDB has expected schema mismatch flag |
| SQL / DuckDB | 1.52x | small-scale reference only |

At sf=10:

| ratio | value | interpretation |
| --- | ---: | --- |
| SQL / DuckDB | `>116x` | lower bound from the 120s SQL timeout over DuckDB's 1.031s |
| SQL / Rune | not comparable | SQL is censored as time/no-row; Rune fails early at a memory wall |
| Rune / DuckDB | not a substrate ratio | Rune did not produce a correct result; its 0.744s is time-to-memory-failure |

## Verdict

**Verdict: logical-shape reach gap, not a substrate ceiling proof.**

The corrected ADR question was "does a Rune-authored logical shape narrow q03, or is the executor substrate the ceiling?"
For the implemented q03 Rune shape, the answer is: the Rune shape does **not** narrow the sf=10 gap. It reintroduces a
scan-output accumulator wall that SQL q03 has already cleared after ADR 390. Therefore this probe cannot yet quantify
Rune/DuckDB as the substrate ceiling for q03.

The useful finding is negative and precise: the current Rune DataFrame surface can author an oracle-correct q03 and emit
real perf rows, but this hand-authored logical shape does not reach the streaming inner-chain path that made SQL q03
memory-bounded. A future Rune diagnostic should either expose the streaming/reoriented logical shape explicitly or add a
true hand-built physical-plan row before using Rune/DuckDB as the substrate ratio.

No symbol-retaining profile was run for Rune: the sf=10 Rune row is not slow; it fails quickly at a named memory
producer. Profiling belongs after Rune reaches a time-bound row.

## Verification

```text
cargo test -p toron-tpch adr391_rune_perf_decomposition -- --nocapture
cargo test -p toron-tpch perf_engines_accept_rune_without_changing_all -- --nocapture
cargo test -p toron-tpch --no-run
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  scripts/cargo-cgroup run -p toron-tpch --release -- perf \
  --query q03 --mode strict --sf 0.01 --runs 1 --warmup 0 \
  --engines toron,rune,duckdb --data-source memory \
  --output-dir target/tpch/perf/adr391_auto_sf001_release
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  timeout 420s scripts/cargo-cgroup run -p toron-tpch --release -- perf \
  --query q03 --mode strict --sf 10 --runs 1 --warmup 0 \
  --engines rune --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf/adr391_auto_sf10_rune
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  scripts/cargo-cgroup run -p toron-tpch --release -- perf \
  --query q03 --mode strict --sf 10 --runs 1 --warmup 0 \
  --engines duckdb --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf/adr391_sf10_duckdb
CARGO_CGROUP_BACKEND=affinity TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  timeout 120s scripts/cargo-cgroup run -p toron-tpch --release -- perf \
  --query q03 --mode strict --sf 10 --runs 1 --warmup 0 \
  --engines toron --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf/adr391_sf10_toron_timeout
```

