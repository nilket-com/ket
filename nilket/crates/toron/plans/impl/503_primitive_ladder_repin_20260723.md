# Primitive ladder re-pin — 2026-07-23, post ADR 503

Protocol re-pin on the sealed ADR 503 binary (`e42ab05e`, batched-prefetch insert). No decision
here; refreshes the board so the next target is chosen on corrected numbers. Live run, DuckDB pinned
single-thread (`duckdb_threads:"1"` verified in the rows — the ADR 501 provenance rail). Coordinate:
strict, sf=8, persisted parquet, 8,192 batch, 2 GiB task / 2 GiB spill, 1 warmup + 5 measured. Polars
omitted (needs the feature build; it is the floor only on m05, already resolved). Artifacts:
`target/tpch/adr503_ladder_repin/`.

Ranked by recoverable time (toron median − DuckDB-1t):

| rank | primitive | toron | duckdb 1t | gap | ratio |
|---|---|---:|---:|---:|---:|
| 1 | m06 join + COUNT | 1,764 ms | 1,234 | 530 ms | 1.43× |
| 2 | m04 3-group GROUP BY | 662 ms | 184 | 478 ms | 3.6× |
| 3 | m10 two-col decimal arith | 467 ms | 193 | 274 ms | 2.4× |
| 4 | m08 COUNT(l_orderkey) | 262 ms | 11 | 251 ms | 23.8× |
| 5 | m09 SUM(l_orderkey) | 308 ms | 97 | 211 ms | 3.2× |
| 6 | m07 COUNT(l_quantity) | 222 ms | 12 | 210 ms | 18.5× |
| 7 | m00 scan proof | 232 ms | 24 | 208 ms | 9.6× |
| 8 | m02 SUM(l_quantity) | 267 ms | 80 | 187 ms | 3.3× |
| 9 | m01 count-shortcut | 184 ms | 12 | 172 ms | 15.3× |
| 10 | m03 predicate + col | 413 ms | 253 | 160 ms | 1.6× |
| 11 | m05 12M-group GROUP BY | 2,386 ms | 3,734 | −1,348 ms | 0.64× |

## What the corrected board says

- **m06 (−18% from ADR 503) is still #1 by absolute gap (530 ms) but only 1.43×** — well-optimized
  (probe 497 + build 503, both prefetch). The only remaining kernel is the held **layout change**
  (#2 key-adjacent / #3 dense range-directed): higher ceiling, higher risk, diminishing returns.
- **m04 is #2 (478 ms) at 3.6×** — a bigger *ratio* gap. ADR 499 bypassed its exchange and ADR 500
  attributed its *residual* to gauge framework, but **the ~420 ms grouped fold itself was never
  optimized** — per-row Utf8 group-key extraction + hash + count over 48M rows into 3 groups. DuckDB
  does the whole query in 184 ms, so its vectorized low-cardinality grouping is ~2.5× ahead. Prior
  art exists (ADR 494 borrowed single-utf8 group probes), and the borrowed on/off question ADR 500
  queued was never answered. This is an **un-optimized, high-ratio, real** target.
- **The high-ratio scan/decode cluster (m00/m01/m07/m08, 172–251 ms at 9–24×) is largely gauge
  artifact** — the tpch parquet decode floor (~156 ms) that ADR 500/501 established, not a production
  engine cost (production scans the native store, not parquet). Not a target.
- m05 (0.64×) is resolved; m03/m10 are mid-band.

## Recommended next target

**m04's grouped fold**, not m06's layout kernel: it has the bigger ratio (3.6× vs 1.43×), it is
genuinely un-optimized (ADR 499/500 touched its exchange and residual, never the fold), and it
carries a lower-risk kernel path (vectorized / borrowed-utf8 grouping, ADR 494 precedent) than m06's
table-layout rewrite. Gate as always: decompose the 420 ms fold (Utf8 key-extraction vs hash vs
count) before choosing the kernel; the m06 layout kernels stay held.
