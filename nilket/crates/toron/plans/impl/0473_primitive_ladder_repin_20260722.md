# Primitive ladder re-pin — 2026-07-22, post ADR 490–493

Protocol re-pin of the ADR 473 primitive ladder; no decision is made here. The old
ranking (64 MiB coordinate, pre-placement-fix executor) was obsolete on both budget
and code: m06 moved 23.3 s → 3.54 s across ADRs 490–493.

Coordinate: strict, sf=8, persisted parquet, release, 2 GiB task / 2 GiB spill
reserve, 1 warmup + 5 measured runs. Toron rows carry
`build_commit=f5add54495f2-dirty` (the ADR 493 candidate build). DuckDB is the
one-thread reference (`SET threads=1`); Polars was not run in this re-pin. All
rows pass with zero spill. Artifacts: `target/tpch/adr494_ladder/{mNN,duckdb_mNN}/`.

| primitive | toron median | duckdb 1t | ratio | absolute gap |
|---|---:|---:|---:|---:|
| m05 `GROUP BY l_orderkey` (12M groups) | 8,472 ms | 3,846 ms | 2.2× | **4,626 ms** |
| m04 `GROUP BY l_returnflag` (3 groups) | 2,444 ms | 186 ms | 13.1× | **2,258 ms** |
| m06 join + COUNT | 3,543 ms | 1,298 ms | 2.7× | 2,245 ms |
| m10 | 664 ms | 197 ms | 3.4× | 467 ms |
| m08 `COUNT(l_orderkey)` | 451 ms | 12 ms | 37.6× | 439 ms |
| m09 `SUM(l_orderkey)` | 497 ms | 99 ms | 5.0× | 398 ms |
| m07 `COUNT(l_quantity)` | 407 ms | 12 ms | 33.9× | 395 ms |
| m02 `SUM(l_quantity)` | 440 ms | 81 ms | 5.4× | 359 ms |
| m03 | 615 ms | 259 ms | 2.4× | 356 ms |
| m01 | 353 ms | 12 ms | 29.4× | 341 ms |

Ranked by absolute recoverable time, the grouping substrate now owns the top two
slots (m05 + m04 ≈ 6.9 s combined gap); the m06 join is third and no longer the
best target. Notable flags for the next deciding ADR:

- m05 peak RSS is 4,765,028 KiB — far above the 2 GiB task coordinate, so the
  12M-group path is not under the admitted-residency regime the join now enjoys.
- m04's 13.1× on a 3-group COUNT points at per-row aggregate/decode overhead, not
  group-table pressure; m07/m08's ~35× on bare COUNT scans corroborates a
  scan/decode floor of ~350–450 ms that caps every primitive.
- m06's residual 2.2 s is CPU-side probe/aggregate (zero spill, in-memory build);
  ADR 493's forward note stands.

Old-ladder comparison (ADR 473 artifacts, 64 MiB): m05 12,700 → 8,472 ms and
m06 7,788 → 3,543 ms, while m02 1,702 → 440 ms and m04 3,995 → 2,444 ms moved
via ADRs 474–482.
