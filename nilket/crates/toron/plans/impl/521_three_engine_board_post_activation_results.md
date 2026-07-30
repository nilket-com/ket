# post-521 three-engine board snapshot at a1eca4e2

The full q01-q22 board from the `a1eca4e2` release binary (`perf: adr 521 activate outer count
preaggregation`), one homogeneous session per leg. Coordinate: strict sf=8 persisted Parquet
under `full_hash` (manifest format 2), explicit 2 GiB memory / 2 GiB spill budgets, 5 measured
runs + 1 warmup per engine per cell, join-spill scratch cleared between legs.

References follow the one-thread doctrine for the arbitration columns: DuckDB v1.5.4 pinned via
`TORON_TPCH_DUCKDB_THREADS=1`, Polars 0.54.4 pinned via `POLARS_MAX_THREADS=1` (the
`polars-sql-reference` build; the reference engine does not alter the toron engine). These pins
are **operator/session environment settings, not artifact-verifiable facts**: the stored rows
carry `duckdb_threads=null` and `polars_max_threads=null` because the current perf rail does
not persist them — a named harness gap; the rail should record the pins it ran under. The
machine-parallel DuckDB column is disclosure only — the board's honest comparison remains
Toron's one-task coordinate against a one-thread reference. Polars rows exist only for its
ADR 394 capability set ({q01, q03, q06, q14, q19}); the other 17 cells are outside its
supported grammar and are shown sparse rather than zero.

**What the reference rows prove — and do not.** This snapshot is timing and capability
evidence, not cross-engine digest-parity evidence: every DuckDB and Polars row carries
`digest="none"`, and 16 of 22 DuckDB rows in the one-thread sweep carry the informational
`schema_mismatch` flag (cross-engine type presentation; the DuckDB-only machine-parallel sweep
computes no comparison flags). Toron-side result identity is adjudicated by its own census and
correctness rails, not by this board.

Rows are ranked by the absolute toron-minus-duckdb-1t gap, the board's arbitration order.

| cell | toron (ms) | duckdb 1t (ms) | toron/duckdb | polars 1t (ms) | toron/polars | duckdb mt (ms) | toron spill (B) |
|---|---:|---:|---:|---:|---:|---:|---:|
| q21 | 12,646.0 | 2,693.0 | 4.70x | — | — | 434.0 | 27,242,808 |
| q09 | 8,602.7 | 2,020.0 | 4.26x | — | — | 334.0 | 0 |
| q18 | 7,534.1 | 1,765.0 | 4.27x | — | — | 237.0 | 0 |
| q05 | 6,452.3 | 734.0 | 8.79x | — | — | 139.0 | 0 |
| q01 | 5,165.9 | 1,141.0 | 4.53x | 6,029.4 | 0.86x | 156.0 | 0 |
| q07 | 4,710.6 | 736.0 | 6.40x | — | — | 132.0 | 0 |
| q17 | 4,273.8 | 768.0 | 5.56x | — | — | 137.0 | 0 |
| q08 | 3,899.4 | 804.0 | 4.85x | — | — | 174.0 | 0 |
| q04 | 3,359.2 | 553.0 | 6.07x | — | — | 97.0 | 0 |
| q12 | 2,460.8 | 618.0 | 3.98x | — | — | 92.0 | 0 |
| q02 | 1,928.5 | 157.0 | 12.28x | — | — | 50.0 | 0 |
| q10 | 3,022.4 | 1,369.0 | 2.21x | — | — | 619.0 | 0 |
| q13 | 3,059.8 | 1,419.0 | 2.16x | — | — | 206.0 | 0 |
| q03 | 2,328.1 | 797.0 | 2.92x | 6,063.3 | 0.38x | 188.0 | 0 |
| q20 | 2,034.3 | 641.0 | 3.17x | — | — | 113.0 | 0 |
| q16 | 1,576.8 | 234.0 | 6.74x | — | — | 74.0 | 0 |
| q19 | 2,098.2 | 821.0 | 2.56x | 705.1 | 2.98x | 118.0 | 0 |
| q11 | 1,383.2 | 132.0 | 10.48x | — | — | 34.0 | 0 |
| q15 | 1,631.5 | 396.0 | 4.12x | — | — | 94.0 | 0 |
| q22 | 801.8 | 280.0 | 2.86x | — | — | 52.0 | 0 |
| q06 | 837.2 | 444.0 | 1.89x | 639.4 | 1.31x | 61.0 | 0 |
| q14 | 805.3 | 505.0 | 1.59x | 402.2 | 2.00x | 95.0 | 0 |

| aggregate | value |
|---|---:|
| median toron/duckdb ratio | 4.26x |
| geometric-mean ratio | 4.19x |
| sum of per-cell medians, toron | 80.6 s |
| sum of per-cell medians, duckdb 1t | 19.0 s |
| summed-median ratio | 4.24x |

The summed-median figures are arithmetic over the 22 per-cell medians, not an observed
end-to-end wall-clock run.

## Reading the snapshot

- The trajectory — roughly 8x before ADR 511, roughly 6.4x after ADR 512, **4.2x here** — is a
  **reconstruction from prior receipts** (the pre-511 and post-512 Toron walls from the ADR
  511-521 receipt chain against this snapshot's reference sums), not a series of homogeneous
  historical sweeps; only the 4.2x figure is artifact-backed by this snapshot. The 511-521 arc
  removed about 70 s of summed Toron medians at this coordinate.
- q13 (ADR 521, activated at this identity) measures 3,060 ms / 2.16x — among the board's best
  cells — with zero default-arm spill; q20 (ADR 518) holds at 2,034 ms / 3.17x.
- q21 is the only spilling cell left on the board (27,242,808 B) and remains parked pending a
  narrower mechanism than general cache tuning.
- Toron beats one-thread Polars outright on q01 (0.86x) and q03 (0.38x), and trails on q19
  (2.98x), q14 (2.00x), and q06 (1.31x).
- The remaining ratio tail splits in two: big-absolute cells q21 / q09 / q18 / q05 (the named
  board threads), and small-absolute high-ratio cells q02 (12.3x), q11 (10.5x), q16 (6.7x)
  totalling under 5 s of Toron wall combined.

## Artifacts

| leg | location |
|---|---|
| one-thread three-engine sweep | `target/tpch/board_a1eca4e2_ref1/` |
| machine-parallel duckdb sweep | `target/tpch/board_a1eca4e2_duckdb_mt/` |

Every row carries `build_commit=a1eca4e2cb61` and `dataset_verification=full_hash`. The
explicit 2 GiB memory / 2 GiB spill coordinate applies to **Toron**: its 22 rows record both
budget provenances as `explicit`. DuckDB is an external reference process that does not consume
those budgets — its rows record both provenances as `unrecorded`. Polars rows carry the
`explicit` env provenance but Polars does not enforce these budgets either; the budget
coordinate constrains only the Toron arm.
