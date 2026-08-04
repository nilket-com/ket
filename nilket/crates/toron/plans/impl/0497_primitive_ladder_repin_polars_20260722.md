# Primitive ladder re-pin — 2026-07-22, post ADR 497 (+ Polars 1t)

Protocol re-pin of the ADR 473 primitive ladder; no decision is made here. It refreshes the ranking
after ADR 497 (batched probe with software prefetch, `0f0782a7`) and adds **Polars** as a second
single-thread reference alongside DuckDB. Live run on the clean sealed binary (built from HEAD
`0f0782a7`, `--features polars-sql-reference`).

Coordinate: strict, sf=8, persisted parquet, release, 2 GiB task / 2 GiB spill reserve, 1 warmup + 5
measured runs. DuckDB is single-thread (v1.5.4, `SET threads=1`); **Polars is pinned single-thread
(`POLARS_MAX_THREADS=1`)** so its row is comparable to the DuckDB reference — multi-thread Polars is
a separate question, deliberately not run here. All 33 engine×primitive rows pass; Toron spill zero.
Artifacts: `target/tpch/adr497_ladder_refresh/`.

Ranked by recoverable time (toron median − fastest reference):

| rank | primitive | toron | duckdb 1t | polars 1t | gap-to-best |
|---|---|---:|---:|---:|---:|
| 1 | m06 join + COUNT | 2,330 ms | 178 ms | 2,802 ms | **2,152 ms** |
| 2 | m04 `GROUP BY l_returnflag` (3 groups) | 2,015 ms | 29 ms | 732 ms | **1,986 ms** |
| 3 | m10 two-column decimal arith | 654 ms | 41 ms | 1,315 ms | 613 ms |
| 4 | m03 predicate + second column | 635 ms | 47 ms | 363 ms | 588 ms |
| 5 | m09 `SUM(l_orderkey)` | 489 ms | 25 ms | 157 ms | 464 ms |
| 6 | m02 `SUM(l_quantity)` | 458 ms | 23 ms | 261 ms | 435 ms |
| 7 | m08 `COUNT(l_orderkey)` | 437 ms | 12 ms | 136 ms | 425 ms |
| 8 | m00 scan proof (sum of literal) | 401 ms | 14 ms | 0.9 ms | 400 ms |
| 9 | m07 `COUNT(l_quantity)` | 391 ms | 12 ms | 207 ms | 379 ms |
| 10 | m01 count-shortcut candidate | 359 ms | 13 ms | 1.0 ms | 358 ms |
| 11 | m05 `GROUP BY l_orderkey` (12M groups) | 2,774 ms | 3,419 ms | 2,590 ms | 184 ms |

## What the board says

**m06 stays first; m04 is a close second.** m06's Toron cost fell again (ADR 497 receipt 2,482 →
2,330 ms here), and m04's 3-group aggregate (2,015 ms vs DuckDB 29 ms) is the persistent
per-row-aggregate floor, corroborated by m07/m08's ~35× on bare counts.

**m05 is resolved, not a target.** ADR 495 already collapsed it; post-495 Toron (2,774 ms) beats
DuckDB 1t (3,419 ms), and against the new Polars-1t floor (2,590 ms) its recoverable gap is 184 ms —
second-to-last on the board. The "m05 has a ~4.6 s grouping gap" framing that motivated this re-pin
was stale (that number was the pre-ADR-495 8,472 ms vs DuckDB 3,846 ms).

**m06's DuckDB 178 ms reframes the m06 target — ground before the next ADR opens.** DuckDB counts
this PK-FK join in 178 ms, far below any full 48M-row hash join. The strong hypothesis is **join
elimination / semi-join**: counting `l_orderkey` over `lineitem ⋈ orders` on the foreign key does
not require materializing the join. If that is the mechanism, most of m06's 2,152 ms recoverable gap
lives in **planning, not the probe/build kernel** — which would make the ADR 497 forward (key-adjacent
build layout, the ~800 ms build bucket) aim at the smaller prize. The next m06 ADR must first ground
whether DuckDB's advantage is planning (join elimination) or execution, and target accordingly.

**Polars-1t is a narrow reference.** It wins only on literal-scan shortcuts (m00/m01 ≈ 1 ms) and
high-cardinality grouping (m05, 2,590 ms). Everywhere else it is slower than DuckDB, and it is slower
than Toron on m06 (2,802 ms) and m10 (1,315 ms). Useful as a second floor; it resets no Toron target.

Old-ladder comparison (ADR 495 repin, same 2 GiB coordinate, DuckDB-only): m06 3,673 → 2,330 ms
(ADR 497); m05 2,744 → 2,774 ms (flat); m04 2,007 → 2,015 ms (flat).
