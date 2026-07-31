# impl 502 — m06 phase-ledger refresh at the 8,192 gauge coordinate (grounding)

Grounding for the next m06 cut. No engine change; this refreshes the ADR 496 local-join phase ledger
on the ADR 501 corrected coordinate so the build target is measured there, not inherited from 1,024.

## Batch-size validation (m06-specific, robust)

This is an **m06-specific** robust sweep (3-run medians). It stands on its own — it does not speak to
the ADR 500 65,536 = 1,200 ms point, which was an **m04** single-run value from the residual
decomposition, a different query (whether that m04 point was noise is untested here):

| batch | median ms |
|---:|---:|
| 4,096 | 2,088.7 |
| 8,192 | 2,084.2 |
| 16,384 | 2,091.2 |
| 32,768 | 2,114.9 |
| 65,536 | 2,125.4 |
| 131,072 | 2,293.0 |

m06 is **flat 4k–16k then rises**: framework savings saturate by ~4k (m06 is framework-light, −5.5%
total from the ADR 501 correction), and parquet decode penalizes large batches past 16k (m06's own
65,536 is 2,125 ms — worse than 8,192, not a hidden optimum; the 56k hypothesis is rejected for m06).
8,192 is on m06's optimum plateau; the ADR 501 "16,384 beat 8,192 for m06" was within-noise. So 8,192
is a valid coordinate for m06, and its wall is mostly **real engine cost**, not gauge artifact.

## Phase ledger @ 8,192 (ledger observer-clean; enabled ≈ disabled)

Total 2,106 ms, 94% covered, residual 134 ms. build_rows 12,000,000, probe/matched 47,989,007, zero
spill, drive_completed.

| phase | ms | share |
|---|---:|---:|
| build | 778.5 | 37.0% |
| probe_key | 756.5 | 35.9% |
| probe_scan | 230.0 | 10.9% |
| output_materialize | 146.8 | 7.0% |
| aggregate_fold | 59.7 | 2.8% |
| residual (framework) | ~134 | 6% |

## What it grounds

**build = 778.5 ms is the dominant recoverable term, unchanged from the 1,024 coordinate** — per-row
hash-table construction (12M orders, ~120 B/row, 1.44 GB), not batch-framework, so the correction did
not touch it. build + probe_key = 73% of the wall: both halves of the Int64 hash-join machinery,
memory-bound on the same 1.44 GB table. ADR 497 optimized probe_key (batched-prefetch, cache-missing
reads); the build is its untouched twin — cache-missing writes (`JoinBuildRows::insert`, ADR 496:
20.6% of process cache-misses).

## Forward — no kernel pre-authorized

Decide the build kernel only after decomposing the 778 ms into hash / insert-cache-miss / orders-scan,
same discipline as the probe_key decomposition. Candidate held kernels (ADR 497 forward): batched
prefetch on inserts (mirror the probe fix), or the layout change (#2 key-adjacent bucket / #3 dense
range-directed table) that shrinks/reorganizes the 1.44 GB table so both build and probe get cheaper.
The acceptance rail will be: the ledger build bucket shrinks on the 8,192 coordinate, digest and 12M
matched rows invariant, zero spill.
