# ADR 508 clean sf=8 baseline and final attribution

ADR 508 ends with a clean, reproducible 22/22 TPC-H board and one ranked premise per active
invocation class. This receipt records that board from the pushed, unamended implementation identity
that contains q11-at-sf8 support and the q21 invocation-bound stage bridge.

No optimization is authorized or measured here. This is the baseline against which a successor ADR
may price movement.

## Provenance

| item | value |
|---|---|
| implementation identity | **`632777908780f17bf15d1d7d43125b6e28f67d99`** |
| row build stamp | **`632777908780`**, no `-dirty` suffix |
| binary SHA-256 | `0e14ae4941b99007b7bbdc48f210c742629f9d2c75172f43a0e2b76df2bab029` |
| binary mtime stamp | `1784941970` |
| coordinate | strict, sf=8, persisted parquet |
| budgets | 2,147,483,648 B per task; 2,147,483,648 B spill reserve, both explicit |
| Toron / DuckDB runs | 1 warmup + 3 measured; fresh child per run |
| DuckDB | v1.5.4, one thread |
| dataset verification | `full_hash` on every Toron, DuckDB, and Polars row |
| primary artifacts | `target/tpch/adr508_clean_sf8_baseline_63277790/` |
| attribution artifacts | `target/tpch/adr508_clean_sf8_attribution_63277790/` |
| Polars control | `target/tpch/adr508_clean_sf8_polars_63277790/` |

The release binary was built from clean `63277790` with `polars-sql-reference` enabled. The primary
board was one harness invocation, not a shell loop:

```bash
TORON_TPCH_MEMORY_PER_TASK=2147483648 \
TORON_TPCH_SPILL_BYTES_RESERVED=2147483648 \
TORON_TPCH_DUCKDB_THREADS=1 \
target/release/toron-tpch perf \
  --sf 8 --mode strict --runs 3 --warmup 1 --query all \
  --engines toron,duckdb --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/adr508_clean_sf8_baseline_63277790
```

The five attribution cells used the same binary, data, and explicit budgets, with
`TORON_NODE_ATTRIBUTION_PROFILE_PATH` set once per query. The Polars control used the same binary and
coordinate with `--engines polars_sql`.

Before the implementation seal, the workspace gate passed 6,903 tests with zero failures; the Toron
library passed 1,408/1,408; architecture passed 2,093/2,093; formatting was clean; and telemetry plus
both TPC-H feature configurations compiled. The measurement receipt changes documentation only.

The primary artifact contains 176/176 passing raw rows and 44/44 passing summaries: 22 queries for
each engine. Every row carries the same clean build stamp and `full_hash`. q11 now runs in both
engines, returns 7,508 rows in each, and has a stable Toron digest across warmup and measured runs.
The join-spill scratch directory was empty after the board.

## Clean 22/22 board

Medians below are execution time. Spill is Toron's whole-query maximum across measured runs; spread
is `(max - min) / median`.

| query | Toron ms | DuckDB-1t ms | T/D | spill bytes | Toron spread |
|---|---:|---:|---:|---:|---:|
| q01 | 5,115 | 1,159 | 4.41× | 0 | 0.76% |
| q02 | 2,714 | 160 | 16.96× | 0 | 0.97% |
| q03 | 2,415 | 810 | 2.98× | 0 | 0.61% |
| q04 | 3,659 | 564 | 6.49× | 0 | 0.63% |
| q05 | 6,727 | 743 | 9.05× | 0 | 0.41% |
| q06 | 852 | 446 | 1.91× | 0 | 0.65% |
| q07 | 4,883 | 738 | 6.62× | 0 | 1.53% |
| q08 | 8,058 | 819 | 9.84× | 0 | 0.90% |
| q09 | 13,192 | 2,040 | 6.47× | 0 | 0.25% |
| q10 | 3,127 | 1,420 | 2.20× | 0 | 0.66% |
| q11 | 1,471 | 134 | 10.98× | 0 | 2.13% |
| q12 | 2,531 | 619 | 4.09× | 0 | 1.11% |
| q13 | 9,292 | 1,494 | 6.22× | 5,131,808 | 0.80% |
| q14 | 807 | 504 | 1.60× | 0 | 2.06% |
| q15 | 1,704 | 392 | 4.35× | 0 | 2.22% |
| q16 | 1,674 | 244 | 6.86× | 0 | 0.88% |
| q17 | 35,541 | 765 | 46.46× | 622,544,968 | 0.66% |
| q18 | 19,558 | 1,835 | 10.66× | 989,777,032 | 0.90% |
| q19 | 2,171 | 825 | 2.63× | 0 | 1.67% |
| q20 | 9,480 | 644 | 14.72× | 0 | 0.29% |
| q21 | 17,989 | 2,780 | 6.47× | 897,713,384 | 6.29% |
| q22 | 829 | 292 | 2.84× | 0 | 2.25% |

The median ratio is **6.47×**, with a 1.60×–46.46× range. q21's 6.29% spread is materially noisier
than the rest of the board and should not support a small movement claim without more repetitions.

The absolute Toron-minus-DuckDB gap sums to 134,362 ms. The three cells spilling more than 100 MB
(q17, q18, q21) account for 67,707 ms, or **50.4%**. Zero-spill cells account for 58,856 ms, or
**43.8%**. Spill is therefore half the recoverable time, not a universal explanation; the zero-spill
composition front remains comparably material.

The largest absolute gaps are q17 34,776 ms, q18 17,723 ms, q21 15,209 ms, q09 11,152 ms, q20
8,836 ms, q13 7,798 ms, and q08 7,239 ms.

Peak process RSS remains a separate accounting question. q18 reaches 10,872,376 KiB (10.37 GiB) and
q05 8,725,776 KiB (8.32 GiB) against a 2 GiB *per-task* budget. A per-task budget is not a process
RSS cap when tasks coexist, so these rows do not prove a cap violation; they do preserve
task-multiplicity/RSS attribution as owed work.

This is a fresh sf=8 baseline. It is not a movement comparison against ADR 356's sf=10, different
memory, and different CPU-allocation coordinate.

## Final inner attribution at the same coordinate

One clean full-hash run per cell closes the ADR 507/508 chain on q08, q09, q17, q20, and q21. Every
routed invocation passes its handler-to-node bridge and stage contract; every Flow byte domain
reconciles exactly; in-tree residuals are zero; the outer execute residual is below 239 microseconds
against the predeclared 10 ms boundary; and there are no interleaved frames or identity mismatches.

### Join-probe premise: q08 and q09

Both cells commit `reoriented_inner_chain_grouped` through `scan_probe_base` and spill zero at the
2 GiB coordinate:

| cell | handler ms | join-associated lower bound | suffix probe | grouped fold | residual |
|---|---:|---:|---:|---:|---:|
| q08 | 7,728.6 | 6,795.3 ms, **87.9%** | 4,117.1 ms, 53.3% | 49.9 ms, 0.65% | 128.3 ms |
| q09 | 12,584.1 | 10,706.6 ms, **85.1%** | 6,222.7 ms, 49.4% | 240.5 ms, 1.91% | 112.6 ms |

The join-associated lower bound includes runtime/base preparation, base probe and materialization,
repartition, and replay-scoped suffix probe/filter stages. It excludes scan pull, grouped fold,
finalization, cleanup, and residual. The two cells therefore share a measured join-probe premise;
their dominant aggregate-node label does not imply an aggregate-fold remedy.

A successor remedy for these cells must reach suffix/base probe, materialization, repartition, or
runtime preparation. A fold-only remedy cannot address more than 0.65% of q08 or 1.91% of q09 at this
coordinate. The remaining attribution cut is inside `suffix_probe`: hash lookup versus
materialization versus runtime read-back.

### Compute-fold premise: q20 and q17's nested scalar invocation

| invocation | handler ms | grouped fold | `row_lookup_update` within fold | spill |
|---|---:|---:|---:|---:|
| q20 `scan_grouped` | 5,807.2 | 2,846.4 ms, **49.0%** | 2,834.1 ms, **99.57%** | 0 |
| q17 inner `scan_grouped` | 10,584.6 | 9,665.9 ms, **91.3%** | 9,658.5 ms, **99.92%** | 0 |

The clean token profile ranks `row_lookup_update` inside the grouped fold, but does **not** separate
hash lookup from aggregate-state update. The earlier external CPU classifier and growth census were
measured under the harness's 64 MiB default and remain useful grounding, not a baseline-coordinate
share. The successor condition is therefore narrower and honest: any compute-fold remedy must reach
the row lookup/update loop, and a mechanism specific to lookup or state layout needs one more
throughput-preserving split before selection.

### Spill premises: q17 outer invocation and q21

q17's `correlated_scalar_left_outer_global` invocation is independently build-envelope dominated:

| q17 outer stage | ms | share of handler | spill bytes |
|---|---:|---:|---:|
| `join0_build` | 19,595.5 | **79.4%** | 622,544,968 |
| `join0_partition_plan` | 2,422.2 | 9.82% | 0 |
| `join0_probe` | 1,022.3 | 4.14% | 0 |
| explicit residual | 1,258.4 | 5.10% | 0 |

All bytes close under Flow on the committed `partitioned_scan_build` route. The counter localizes
byte ownership to the build envelope; it does not prove which part of that envelope spends time
serializing, writing, sealing, or rebuilding indexes. A successor remedy must reach the build
envelope, first decomposing it into those lifecycle costs. Probe or global-fold work is not the
ranked premise.

q21's `inner_prefix_semi_anti_grouped` invocation splits as follows:

| q21 top-level stage | ms | share of handler | spill bytes |
|---|---:|---:|---:|
| runtime preparation | 5,755.1 | 33.0% | 536 |
| replay 0 | 1,499.2 | 8.60% | 0 |
| deferred replay envelope | 9,461.5 | **54.3%** | 897,712,848 |
| base prep/scan, finalize, cleanup, residual | 717.1 | 4.11% | 0 |

Inside the deferred envelope, reservoir buffering is 4,056.6 ms (42.9%), replay 1 is 4,289.0 ms
(45.3%), reservoir finish is 364.5 ms (3.85%), other replay/fold work is 685.5 ms (7.24%), and the
explicit residual is 65.9 ms (0.70%). The task counter advances by 24,317,328 B during buffering and
873,395,520 B during release/accounting. That last byte assignment says when accounting advances,
not that 873 MB were written in the measured microsecond.

q17 and q21 therefore do not reduce to one spill remedy. q17 ranks a partitioned build envelope; q21
splits almost evenly between reservoir buffering and replay-1 inside a larger deferred envelope, with
runtime preparation also material. The successor must preserve those separate premises and price a
mechanism against the invocation it actually reaches.

## Polars control

Polars 0.54.4 produces 20 passing raw rows for its five expressible queries and 68 explicit
`not_attempted` rows for dialect gaps; summaries are 5 pass / 17 not attempted. All 88 rows carry the
same clean build stamp and `full_hash`.

| query | Toron ms | DuckDB-1t ms | Polars ms |
|---|---:|---:|---:|
| q01 | 5,115 | 1,159 | 855 |
| q03 | 2,415 | 810 | 1,010 |
| q06 | 852 | 446 | 147 |
| q14 | 807 | 504 | 105 |
| q19 | 2,171 | 825 | 140 |

Both independent peers place Toron several-fold behind on these cells. The DuckDB ratios are not a
single-peer artifact.

## Decision handed forward

ADR 508 closes with three independently measured invocation premises:

1. **join-probe:** q08/q09, 85–88% join-associated inside their fused handlers;
2. **compute-fold:** q20 and q17's inner invocation, with 49%/91% in grouped fold and >99.5% of that
   fold in the still-combined row lookup/update loop;
3. **spill lifecycle:** q17's outer invocation at 79.4% build-envelope time, and q21's deferred
   reservoir/replay envelope plus runtime preparation.

These are locations and conditions, not selected mechanisms. q18/q02's join/sort front remains
parked. The next ADR may choose a remedy only after splitting the dominant envelope it intends to
change, and must measure movement against this clean identity and coordinate.
