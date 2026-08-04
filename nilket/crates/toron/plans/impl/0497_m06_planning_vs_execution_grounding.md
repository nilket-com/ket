# m06 planning-vs-execution grounding — and a re-pin correction

The fork recorded in the post-497 re-pin (`1c986ca3`) is resolved, and the re-pin
itself needs a correction: its DuckDB arm was **not** single-threaded.

## The 178 ms was a thread-count confound, not join elimination

The re-pin's DuckDB rows carry `duckdb_threads: null` — the ladder run never set
`TORON_TPCH_DUCKDB_THREADS`, so DuckDB ran at its default thread count while the doc
claimed `SET threads=1`. Reproduced directly on the same views:

- default threads: `real 0.181 s, user 2.92 s` (≈16 threads) — matches the re-pin's
  178 ms exactly;
- `SET threads=1`: **1.287 s** — matching the pre-497 ladder's properly pinned
  1,298 ms DuckDB m06 row.

`EXPLAIN` settles the planning question outright: DuckDB's m06 plan is a real
`HASH_JOIN` (48M probe × 12M build) under an ungrouped `count_star()` — **no join
elimination, no semi-join rewrite**. The one planner-side trick it does apply is
key compression (`__internal_compress_integral_uinteger`) before the join, i.e. a
smaller/denser build — a *layout* advantage, not an elimination.

**Verdict: m06's gap is execution, not planning.** The ADR 497 forward kernels
(key-adjacent bucket layout; dense range-directed table) remain aimed at the right
prize, and DuckDB's own key compression is corroborating evidence for the
layout/density direction.

## Corrected ranking

With valid single-thread DuckDB references (the pre-497 ladder's pinned rows,
engine-independent of the Toron commit; m06 re-confirmed by hand at 1,287 ms) and
the re-pin's Toron/Polars-1t rows, the top of the board flips:

| rank | primitive | toron | duckdb 1t | polars 1t | gap-to-best |
|---|---|---:|---:|---:|---:|
| 1 | m04 `GROUP BY l_returnflag` | 2,015 ms | 186 ms | 732 ms | **1,829 ms** |
| 2 | m06 join + COUNT | 2,330 ms | 1,298 ms | 2,802 ms | **1,032 ms** |
| 3 | m10 | 654 ms | 197 ms | 1,315 ms | 457 ms |
| — | m08/m00/m07/m02/m09/m03/m01 | | | | ~360–425 ms each |
| last | m05 (12M groups) | 2,774 ms | 3,846 ms | 2,590 ms | 184 ms |

m06 is 1.8× behind DuckDB 1t, not 13×. **m04's 3-group aggregate floor (1,829 ms,
~10.8× behind) is the top recoverable gap**, consistent with the m07/m08 bare-COUNT
floor (~35×) pointing at the same per-row decode/aggregate substrate.

## Protocol note for future re-pins

The ladder runner records `duckdb_threads` provenance but does not require it: an
unset env silently benchmarks the reference at default parallelism while every
Toron row is single-task. Any future re-pin claiming a 1t reference must set
`TORON_TPCH_DUCKDB_THREADS=1` explicitly and check the row provenance is `"1"`,
not null — same class of loud-contract gap as the gauge budget env, worth closing
in the harness when it is next touched.
