# Primitive ladder re-pin — 2026-07-22, post ADR 495

Protocol re-pin of the ADR 473 primitive ladder; no decision is made here. It
refreshes the ranking after ADR 495 (bounded Int64 grouped count) and its
forward-only exchange-key fix. The binary was built from a clean tree at
`5a46bd0e` (`fix: adr 495 require compact count exchange key match group key`,
the seal of ADR 495 `c10eae41` plus the P2 exchange-key guard), so this re-pin
also supplies the clean commit identity the ADR 495 receipt lacked (it measured
`build_commit=44b0c9489754-dirty`).

Coordinate: strict, sf=8, persisted parquet, release, 2 GiB task / 2 GiB spill
reserve, 1 warmup + 5 measured runs. DuckDB is the one-thread reference
(v1.5.4 Variegata); Polars was not run. All rows pass with zero spill. Artifacts:
`target/tpch/adr495_ladder_refresh/ladder.jsonl`.

| primitive | toron median | duckdb 1t | ratio | absolute gap |
|---|---:|---:|---:|---:|
| m06 join + COUNT | 3,673 ms | 185 ms | 19.9× | **+3,488 ms** |
| m04 `GROUP BY l_returnflag` (3 groups) | 2,007 ms | 34 ms | 59.0× | **+1,973 ms** |
| m10 two-column decimal arith | 675 ms | 44 ms | 15.3× | +631 ms |
| m03 predicate + second column | 625 ms | 46 ms | 13.6× | +579 ms |
| m09 `SUM(l_orderkey)` | 511 ms | 26 ms | 19.6× | +485 ms |
| m08 `COUNT(l_orderkey)` | 448 ms | 13 ms | 34.4× | +435 ms |
| m02 `SUM(l_quantity)` | 450 ms | 23 ms | 19.6× | +427 ms |
| m00 scan proof (sum of literal) | 413 ms | 14 ms | 29.5× | +399 ms |
| m07 `COUNT(l_quantity)` | 410 ms | 14 ms | 29.3× | +396 ms |
| m01 count-shortcut candidate | 342 ms | 12 ms | 28.5× | +330 ms |
| m05 `GROUP BY l_orderkey` (12M groups) | **2,744 ms** | 3,490 ms | **0.79×** | **−746 ms** |

Ranked by absolute recoverable time, the m06 join now owns the top slot; m04's
low-cardinality grouping is second. The next deciding ADR opens against m06's
remaining CPU-side join + COUNT cost (zero spill, in-memory build — the residual
is probe/aggregate CPU, per ADR 493's forward note).

ADR 495 moved m05 off the board: the 12M-group count went 8,472 ms (473 repin) →
2,744 ms here, and Toron now beats DuckDB on one thread (0.79×) at 596 MiB peak
RSS vs DuckDB's 9.7 GiB — the RSS independently reproduces the ADR 495 residency
claim on the clean binary.

Two caveats that must not be laundered into a decision:

- **m06's DuckDB side shifted 1,298 → 185 ms** vs the 473 repin (DuckDB is now
  v1.5.4; a join/count pushdown or result-cache effect on its side — Toron's m06
  held at 3,543 → 3,673 ms, no regression). This inflates the *ratio*, not the
  ranking: even against the older 1,298 ms DuckDB baseline, m06's gap (2,375 ms)
  still tops m04's (1,973 ms). m06 is first either way.
- **peak_rss_kib is invalid for m06–m10** — every one reports the identical
  10,149,004 KiB carried forward from m05's DuckDB run (process high-water, not
  per-run reset). Those RSS values must not support any decision. The execute_ms
  medians are sound (tight sample spreads).

Old-ladder comparison (ADR 473 repin, same 2 GiB coordinate): m05 8,472 →
2,744 ms (ADR 495); m06 3,543 → 3,673 ms (flat, Toron side); m04 2,444 →
2,007 ms.
