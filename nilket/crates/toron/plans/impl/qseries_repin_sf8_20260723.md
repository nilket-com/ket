# q-series repin — strict sf=8, composed board — 2026-07-23

Protocol re-pin of the composed TPC-H q-series against DuckDB (single-thread) and Polars, at the
primitive ladder's coordinate. No decision here; it grounds the next council cycle by pricing the
composed gap that the saturated ladder (ADR 506) could not see.

## Conditions and provenance

| item | value |
|---|---|
| board build identity | **`cab8ea9f5952-dirty`** (stamped in every row) |
| q21 re-measure identity | **`05febe6e2cad-dirty`** |
| coordinate | strict, sf=8, persisted parquet, 2 GiB task / 2 GiB spill (ladder budgets) |
| runs | 1 warmup + 3 measured, fresh child per run |
| DuckDB | v1.5.4, `TORON_TPCH_DUCKDB_THREADS=1` |
| Polars | 0.54.4, `polars-sql-reference` feature |
| dataset verification | `full_hash` on 63/66 rows (3 nulls are q11's out-of-scope stubs) |
| artifacts | `target/tpch/qseries_repin_sf8_v2/`, `target/tpch/q21_fix_verify/` |

**The board is not a clean baseline.** Its binary was built from an uncommitted tree; the stamp is
`cab8ea9f5952-dirty`, not `05febe6e`. Treat it as council grounding only. q21's row was re-measured
separately after its correctness fix and carries its own dirty stamp. A clean labelled baseline
requires a re-run from a committed tree.

q11 is **not yet enumerated at sf=8**: the harness renders its HAVING fraction from a table of exact
decimal literals (`Q11Scale`) that lists only sf ∈ {0.01, 1, 10}. This is an enumeration gap, not a
spec or arithmetic limit — sf=8 is exactly representable (`0.0001 / 8 = 0.0000125`, and the oracle's
`SF x 100` integer is 800). 21 measured cells, not 22; enabling q11 here is deferred, not blocked.

## Board

Ranked by recoverable time (toron median − DuckDB-1t median).

| rank | q | toron ms | duckdb-1t ms | polars ms | T/D | spill MB | peak RSS GiB |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | q17 | 35,454 | 766 | — | **46.28x** | 623 | 0.9 |
| 2 | q18 | 19,718 | 1,843 | — | **10.70x** | 990 | 10.4 |
| 3 | q21 | 17,697 | 2,776 | — | **6.38x** | 898 | 0.8 |
| 4 | q09 | 13,337 | 2,044 | — | **6.52x** | 0 | 2.4 |
| 5 | q20 | 9,652 | 646 | — | **14.94x** | 0 | 2.4 |
| 6 | q13 | 9,478 | 1,488 | — | **6.37x** | 5 | 0.6 |
| 7 | q08 | 8,221 | 860 | — | **9.56x** | 0 | 0.8 |
| 8 | q05 | 6,684 | 744 | — | **8.98x** | 0 | 8.3 |
| 9 | q07 | 4,883 | 741 | — | **6.59x** | 0 | 1.4 |
| 10 | q01 | 5,069 | 1,162 | 842 | **4.36x** | 0 | 0.0 |
| 11 | q04 | 3,631 | 567 | — | **6.40x** | 0 | 1.2 |
| 12 | q02 | 2,718 | 160 | — | **16.99x** | 0 | 1.0 |
| 13 | q12 | 2,511 | 628 | — | **4.00x** | 0 | 1.8 |
| 14 | q10 | 3,132 | 1,403 | — | **2.23x** | 0 | 1.5 |
| 15 | q03 | 2,403 | 816 | 967 | **2.94x** | 0 | 3.3 |
| 16 | q16 | 1,656 | 246 | — | **6.73x** | 0 | 0.6 |
| 17 | q19 | 2,154 | 831 | 143 | **2.59x** | 0 | 0.6 |
| 18 | q15 | 1,706 | 397 | — | **4.30x** | 0 | 0.6 |
| 19 | q22 | 841 | 296 | — | **2.84x** | 0 | 0.6 |
| 20 | q06 | 860 | 450 | 148 | **1.91x** | 0 | 0.1 |
| 21 | q14 | 806 | 504 | 100 | **1.60x** | 0 | 0.6 |

All Toron spreads ≤ 0.032. Polars runs 5 of 21 cells; the other 16 are dialect gaps it reports
itself (comma-join FROM lists, non-equi constraints), recorded as `not_attempted`.

## What the board says

**The composed gap is roughly triple the primitive gap.** The ADR 503 ladder pins m00–m10 at
1.4–3.6× (median ~2.4×, with m05 at 0.64×). The composed series runs **1.60×–46.28×, median 6.38×**.
Saturating the primitive ladder did not carry composed queries to the same place, so the remaining
gap is dominated by composition — plan shape, spill and memory policy, exchange — not by scan,
decode, or arithmetic kernels.

**Polars corroborates the peer.** On the five cells it can express, Polars and DuckDB independently
agree Toron is several-fold behind (q01 5,069 vs 1,162 / 842; q14 806 vs 504 / 100). The DuckDB
ratios are not a single-peer artifact.

Three distinct classes, which is the substance for the next cycle:

- **Spill-heavy:** q17 (46.28×, 623 MB), q18 (10.70×, 990 MB), q21 (6.38×, 898 MB).
- **Large non-spill composition gaps:** q20 (14.94×, zero spill), q09 (6.52×, zero spill), and
  q02/q08 in the same shape. Nothing here is explained by spill volume.
- **Correctness:** q21 failed the board deterministically (unflushed unindexed page buffer at the
  probe reservoir's seal). Fixed and re-measured; it is now a performance cell like the others.

**Memory accounting is an open question, not a cap violation.** q18 peaks at 10.4 GiB RSS and q05 at
8.3 GiB against a 2 GiB per-task coordinate. Per-task budgets do not bound process RSS across
concurrent tasks, so this is not automatically a breach — but task multiplicity and the attribution
from per-task budget to process footprint are not currently pinned by any receipt, and belong in the
next grounding.

## Forward

The encoded-column-first premise does not survive this board. It was justified by m04's 478 ms at
one rung; the composed gap is spread across spill-heavy and non-spill classes that no scan-encoding
contract addresses. **The next council topic is composed-gap decomposition**, with spill/memory
policy and non-spill composition as competing mechanisms. Encoded columns remain a candidate, to be
ranked once its attributable share across these queries is measured rather than assumed.

sf=10 stays the later publication board, on the ADR 356 coordinate. Running it now would spend hours
on a different coordinate without changing the immediate priority.
