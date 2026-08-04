# ADR 415 results - sf=10 full-board live sweep

## Verdict

ADR 415 completes the live sf=10 census under the fixed 4 GiB scope:

- **12 of 22 queries are green with persisted `pass@compare`:** q01, q03, q04, q06, q08, q10, q12, q14, q15,
  q16, q19, and q20.
- **q11 reaches output but is not green:** Toron and DuckDB both return zero rows, while the independent q11 oracle
  rejects the result through its non-vacuity guard. This is a correctness/fixture diagnostic, not a banked pass.
- **Eight queries stop at named engine memory producers:** q02, q07, q09, q13, q17, q18, q21, and q22.
- **q05 is a reproduced hard OOM:** all three attempts reached the 4 GiB cgroup peak and were killed after about
  42 seconds. ADR 414's reproducibility caveat is now resolved as a current red row, not carried as an implied green.
- **DuckDB completes all 22** under the same 4 GiB scope. The final board records the peer time for every cell and a
  Toron/DuckDB ratio only where Toron reaches output; q05 records time-to-OOM context instead.

This is a measurement result. No engine fix is included. Every red row has a named follow-on, and no cached matrix
row is used as evidence.

## Conditions And Provenance

The sweep ran the plan-seal commit `af8e64cbdbcc5f5b724a20ed2687835bc490135a` with a freshly built release binary:

| item | value |
|---|---|
| binary SHA-256 | `f6f9a2cfa4dce4ab86107147680892a4921cd6c287901c578d916fd72f6b08d5` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| DuckDB | `v1.5.4 (Variegata) 08e34c447b` |
| scope | `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512` |
| reach timeout | 360 seconds per cell |
| correctness timeout | 600 seconds per completing cell |
| data | persisted strict sf=10 Parquet |

The sweep followed the scoped commands and per-cell artifact protocol in the ADR 415 implementation plan. It cleared
`/tmp/toron_join_spill` before the sweep, after every scoped process, and after the final reference run. Scratch was
0 before and after; `/tmp` had 28 GiB free.

Artifact root: `target/tpch/adr415/`.

## Final Board

Times are one live Toron reach attempt, not medians. Peak RSS comes from `/usr/bin/time -v`; q05 uses systemd's
cgroup `MemoryPeak` because the hard kill also killed `time`. Spill is Toron's committed telemetry, not residual
scratch. Ratios are reported only for output-reaching rows; an elapsed-to-wall ratio would not be an end-to-end
performance number.

| query | Toron classification | Toron ms | peak RSS | spill bytes | correctness | DuckDB ms | Toron/DuckDB |
|---|---|---:|---:|---:|---|---:|---:|
| q01 | green | 22,839.440 | 27.9 MiB | 0 | pass@compare | 233 | 98.02x |
| q02 | fail: join output materialization | 855.661 to wall | 972.6 MiB | 0 | n/a | 73 | n/a |
| q03 | green | 14,346.266 | 519.1 MiB | 233,338,304 | pass@compare | 299 | 47.98x |
| q04 | green | 4,682.798 | 1,203.8 MiB | 0 | pass@compare | 149 | 31.43x |
| q05 | OOM x3 | ~42,000 to OOM | 4 GiB | no row | n/a | 219 | >191x to OOM |
| q06 | green | 1,138.152 | 26.4 MiB | 0 | pass@compare | 95 | 11.98x |
| q07 | fail: spilled-build resident index | 3,752.115 to wall | 1,664.7 MiB | 0 | n/a | 213 | n/a |
| q08 | green | 18,250.495 | 1,054.5 MiB | 0 | pass@compare | 272 | 67.10x |
| q09 | fail: spilled-build resident index | 7,050.469 to wall | 3,457.7 MiB | 0 | n/a | 555 | n/a |
| q10 | green | 95,627.273 | 1,825.5 MiB | 215,703,848 | pass@compare | 966 | 98.99x |
| q11 | correctness gap (zero rows) | 4,679.890 | 991.8 MiB | 166,552,400 | oracle guard fail | 47 | 99.57x reach |
| q12 | green | 5,956.313 | 1,398.7 MiB | 159,510,976 | pass@compare | 139 | 42.85x |
| q13 | fail: scan output accumulator | 614.405 to wall | 1,065.1 MiB | 0 | n/a | 364 | n/a |
| q14 | green | 4,499.204 | 280.4 MiB | 36,464,000 | pass@compare | 146 | 30.82x |
| q15 | green | 4,337.751 | 103.6 MiB | 0 | pass@compare | 140 | 30.98x |
| q16 | green | 4,066.125 | 742.6 MiB | 60,078,400 | pass@compare | 114 | 35.67x |
| q17 | fail: scan output accumulator | 478.428 to wall | 1,062.5 MiB | 0 | sf=10 oracle-policy gap | 222 | n/a |
| q18 | fail: scan output accumulator | 5,863.824 to wall | 2,012.9 MiB | 18,862,208 | n/a | 384 | n/a |
| q19 | green | 79,387.628 | 266.0 MiB | 51,471,552 | pass@compare | 191 | 415.64x |
| q20 | green | 21,585.885 | 3,282.0 MiB | 256,104,392 | pass@compare | 178 | 121.27x |
| q21 | fail: scan output accumulator | 722.436 to wall | 1,082.1 MiB | 0 | n/a | 765 | n/a |
| q22 | fail: join spill writer resident state | 1,936.562 to wall | 806.2 MiB | 0 | n/a | 81 | n/a |

DuckDB's q11 median is flagged `unstable` (spread 0.298); all other DuckDB rows are unflagged. This does not affect
the Toron classification.

## Named Walls

The engine payloads make the residual families explicit:

| query | producer | requested | available | component evidence | next cut |
|---|---|---:|---:|---|---|
| q02 | join output materialization | 1,738,000,000 | 1,073,741,824 | no component split | join-output streaming/spill |
| q07 | streamed inner join build spill resident state | 1,074,443,016 | 1,073,741,824 | index 1,074,377,480 + page reserve 65,536 | extend compact spilled index to this seam |
| q09 | streamed inner join build spill resident state | 1,074,443,016 | 1,073,741,824 | same byte-identical index payload as q07 | shared q07/q09 compact-index cut |
| q13 | scan output accumulator | 1,073,760,840 | 1,073,741,824 | 19,016 over | scan-output streaming shape |
| q17 | scan output accumulator | 1,073,767,936 | 1,073,741,824 | 26,112 over | scan-output streaming + oracle-policy follow-up |
| q18 | scan output accumulator | 1,073,742,336 | 1,073,741,824 | 512 over | scan-output streaming shape |
| q21 | scan output accumulator | 1,073,754,240 | 1,073,741,824 | 12,416 over | scan-output streaming shape |
| q22 | join spill writer resident state | 121,631,060 | 67,108,864 | writer state exceeds task budget by 54,522,196 | writer chunk/scratch completion policy |

The q07/q09 payload is the pre-compaction-size resident index seen before ADR 414, but on a different execution seam.
That makes extending the compact primitive index the first evidence-backed lever; it is not a 701 KiB cap adjustment.

The scan-output cells are all just over the 1 GiB accumulator, but the established doctrine still applies: stream
the producer or preserve a named wall; do not increase the cap by the observed edge.

## q05 Resolution

q05 ran three times with fresh scratch. All three attempts were killed by the kernel at the exact cgroup cap and
left about 1.3 GiB of spill scratch, which was removed before the next attempt:

| attempt | result | wall | CPU | cgroup peak |
|---|---|---:|---:|---:|
| 1 | `oom-kill` | 41.949 s | 42.178 s | 4 GiB |
| 2 | `oom-kill` | 42.751 s | 42.992 s | 4 GiB |
| 3 | `oom-kill` | 41.921 s | 42.091 s | 4 GiB |

There is no engine JSON row because the cgroup kill terminated the telemetry process. The durable evidence is the
systemd result, exact `MemoryPeak=4294967296`, and captured journal in
`target/tpch/adr415/q05_attempts.journal`. q05 is therefore **OOM**, not flaky-green and not an inferred regression
from a single run. The next cut is a q05 peak-residency breakdown around the reservoir/base-concat phase; a cap bump
is not implied.

## Correctness Provenance

Persisted sf=10 correctness passed for all 12 green rows:

`q01 q03 q04 q06 q08 q10 q12 q14 q15 q16 q19 q20`

Two non-green correctness findings remain:

- **q11:** reach returned zero rows; the independent oracle failed its non-vacuity guard with
  `rows=0`, `total_german_parts=304774`, `positive_german_parts=304774`, and
  `threshold_fail_groups=304774`. DuckDB also returns zero rows for the bundled query. This needs a fixture/query/
  oracle reconciliation before either engine's zero-row result can be called correct.
- **q17:** the live correctness command returned
  `q17_sf10_oracle_policy_gap: q17 correctness requires --sf 0.02; got 10`. q17 also fails reach earlier at the scan
  output accumulator, so the primary board status is the engine wall and the oracle-policy gap remains secondary.

No completing row is green by implication.

## Atlas Family Re-Proof

The six previously established green controls all re-proved correctness. ADR 414's control table recorded
correctness elapsed time (execution plus oracle/compare work), while this board records reach execution time; those
numbers are different measurement frames and are not used to claim a speedup.

| query | prior state | ADR 415 reach | verdict |
|---|---|---:|---|
| q03 | green | 14,346.266 ms | retained; pass@compare |
| q04 | green | 4,682.798 ms | retained; pass@compare |
| q08 | green | 18,250.495 ms | retained; pass@compare |
| q10 | green | 95,627.273 ms | retained; pass@compare |
| q12 | green | 5,956.313 ms | retained; pass@compare |
| q19 | green | 79,387.628 ms | retained; pass@compare |
| q05 | inconclusive OOM caveat | OOM x3 at ~42 s | resolved red; residency follow-up |

Forensic spill controls remain consistent with the earlier mechanism seals: q03 writes 233,338,304 bytes, q10
215,703,848, q12 159,510,976, and q19 51,471,552.

## Closing Read

The sweep replaces the stale 22-cell picture with a live one:

- The 401-414 heavy-cell work is durable for q03, q08, q10, and q12; q05 is the explicit exception.
- The old memory tail is now concrete: four scan-output cells, two spilled-index cells, one join-output cell, and one
  writer-resident cell.
- Reach and competitive speed remain separate claims. Even the green rows range from 11.98x to 415.64x DuckDB.
- q20 is green but peaks at 3,282 MiB, close enough to the 4 GiB boundary to remain a residency watch item rather
  than being treated as a comfortably settled control.
- The next reach cuts are ranked by shared producer, not by smallest byte edge: q07/q09 compact-index extension,
  q13/q17/q18/q21 scan streaming, q02 output streaming, q22 writer residency, and q05 peak-residency diagnosis.
- q11 is a correctness-instrument issue to resolve before performance work on that cell.

The reach story is now fully classified under the fixed 4 GiB envelope. It is not a 22/22-green claim.

## Hygiene

- Live persisted sf=10 rows only; no matrix or correctness cache row was used.
- Measurement-only: no Toron engine source changed.
- Fixed 4 GiB scope; no cap, multiplier, or cgroup change.
- Scratch was cleared before, between, and after scoped runs.
- DuckDB reference: 22/22 attempted and completed.
- No `.perf.data` is part of this change.
