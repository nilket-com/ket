# ADR 613 - SF8 board re-seal after the grouped-fold cuts: results

Run 2026-09-30 on strix: one step, both engines, all 22 cells, 5 runs +
1 warmup, at the ADR 603 and 608 coordinate. **Sealed** with
`candidate_authority` and `final_authority` true.

**Toron 61.387 s against DuckDB 15.579 s: 3.940×.** That compares with
ADR 608's 62.706 s, 15.600 s, and 4.020×, and ADR 603's 63.430 s,
15.650 s, and 4.053×. The target arithmetic, named twice:
- **Against this board's DuckDB total:** 2 × 15.579 s is 31.158 s.
  Toron is **30.229 s** above it.
- **Against the fixed original threshold** (31.366 s, 2× ADR 591's
  DuckDB sum): Toron is **30.021 s** above it.

Every Toron digest equals ADR 608's. All 44 cells passed and are
stable; the largest `(max − min) / median` is 0.095, against the 0.20
bound. DuckDB's per-cell medians moved by 5 ms or less (−21 ms in
total), reported as drift for context.

Toron's total fell 1,319 ms from ADR 608. The record apportions none of
that change to ADR 609, ADR 610, or anything else. The paired evidence
for the changes since ADR 608 is:
- **q01:** ADR 609, −713.4 ms; ADR 610, −754.9 ms;
- **q09:** ADR 609, not established;
- **q13:** ADR 610, a regression of +122.4 ms, pending.


## Identity

| identity | value |
|---|---|
| measured tree | `906b879e526f99897bb624875d751115f49f712a`, commit `4eb745e8` (this plan's commit; code equal to `artifact/adr611-control`, `1dddc7fe`) |
| executable | `9a84d5c415ce90c738b6da471067a2ae0ca36b5b5c465857bd9bf9eafc17cbf2`. The bigfoot build reproduced ADR 611 control's bytes. Manifest `target/tpch/evidence/build/adr613-measured.manifest` (`38e21577…`) |
| DuckDB | `c478c0f4…`, v1.5.4, one thread, memory limit null (provenance `default`) |
| ladder (bigfoot, measured tree, `--no-fail-fast`) | toron 5,513 passed, 0 failed, 3 ignored; toron-tpch 674 passed, 0 failed, 6 ignored |
| artifact / sealed manifest | `2f02944827b62a8debd3c8adb952ee65e458fdc871d8e87604a06a80344fa89b` / `576dab40d07e900ce99b22318d550bba83fc3e13e9790061b77a121fc2bfa724` (`target/tpch/adr613-board/perf_board.jsonl.evidence.sealed`) |
| evidence tag | `evidence/906b879e526f-2f02944827b6`, fetched to the workstation and pushed with the closing commit |
| build host / run host | bigfoot / strix |

## Procedure

ADR 608's, with this record's paths. The host scripts were generated
from ADR 608's, and the grep for other ADR numbers returned 0. Token
`adr613-5f02de5f08a8`:
- launched 02:41:30 CDT, with the unit 120 s later;
- completion at 02:57:46 (`result=success exit_code=exited
  exit_status=0`);
- before: load 0.03 and 42 °C, scratch empty, output fresh;
- after, by the read-out following the datagram: load 0.79 and 46 °C,
  scratch empty.

The header records:
- the host quiescent at start and end;
- the spill scratch empty before and after;
- the output fresh;
- affinity `0-15`.

264 samples and 44 summaries.

## The board

| query | Toron ms | DuckDB ms | ratio | gap ms | Toron vs 608 | DuckDB vs 608 | Toron vs 603 | Toron peak RSS KiB (label; parent at spawn) | spill bytes |
|---|---|---|---|---|---|---|---|---|---|
| q01 | 3,352.1 | 957.0 | 3.50 | 2,395 | −1,400.6 | +4.0 | −1,305.3 | 29,848 (above; 10,500) | 0 |
| q02 | 1,452.2 | 128.0 | 11.35 | 1,324 | +17.1 | +0.0 | −32.0 | 980,940 (above; 17,796) | 0 |
| q03 | 1,878.9 | 653.0 | 2.88 | 1,226 | +6.8 | −1.0 | −1.6 | 3,453,112 (above; 21,860) | 0 |
| q04 | 2,769.9 | 456.0 | 6.07 | 2,314 | −10.2 | +1.0 | −11.6 | 1,232,232 (above; 124,480) | 0 |
| q05 | 4,752.0 | 591.0 | 8.04 | 4,161 | −12.8 | −1.0 | −533.7 | 9,167,776 (above; 124,480) | 0 |
| q06 | 684.0 | 363.0 | 1.88 | 321 | −57.5 | −2.0 | −3.4 | not stated (ambiguous; 124,480) | 0 |
| q07 | 3,886.9 | 591.0 | 6.58 | 3,296 | −31.4 | −2.0 | −53.7 | 1,600,868 (above; 124,480) | 0 |
| q08 | 3,214.4 | 643.0 | 5.00 | 2,571 | −3.7 | −2.0 | −46.4 | 635,900 (above; 124,480) | 0 |
| q09 | 6,978.4 | 1,629.0 | 4.28 | 5,349 | −17.2 | −3.0 | −135.6 | 2,456,620 (above; 124,480) | 0 |
| q10 | 2,379.6 | 1,124.0 | 2.12 | 1,256 | +25.3 | +0.0 | +7.5 | 1,060,516 (above; 124,480) | 0 |
| q11 | 896.9 | 107.0 | 8.38 | 790 | −16.1 | +0.0 | −8.9 | 851,736 (above; 666,760) | 0 |
| q12 | 2,070.7 | 509.0 | 4.07 | 1,562 | −0.9 | +0.0 | +7.2 | 1,837,544 (above; 666,760) | 0 |
| q13 | 2,638.0 | 1,190.0 | 2.22 | 1,448 | +145.5 | −4.0 | +147.6 | not stated (ambiguous; 666,760) | 0 |
| q14 | 637.6 | 401.0 | 1.59 | 237 | −2.3 | −5.0 | +1.6 | not stated (ambiguous; 666,760) | 0 |
| q15 | 1,281.7 | 311.0 | 4.12 | 971 | +49.9 | −2.0 | +47.4 | not stated (ambiguous; 666,760) | 0 |
| q16 | 1,369.3 | 189.0 | 7.24 | 1,180 | +2.0 | −2.0 | +28.0 | not stated (ambiguous; 666,760) | 0 |
| q17 | 3,100.7 | 625.0 | 4.96 | 2,476 | +2.3 | +0.0 | +1.3 | 3,726,888 (above; 666,760) | 0 |
| q18 | 5,976.7 | 1,479.0 | 4.04 | 4,498 | −13.3 | −5.0 | −62.0 | 4,045,916 (above; 666,760) | 0 |
| q19 | 1,761.0 | 676.0 | 2.60 | 1,085 | −5.0 | +0.0 | −7.8 | not stated (ambiguous; 666,760) | 0 |
| q20 | 1,685.5 | 517.0 | 3.26 | 1,169 | +8.5 | −1.0 | +8.5 | 2,237,656 (above; 666,760) | 0 |
| q21 | 7,972.7 | 2,205.0 | 3.62 | 5,768 | +2.0 | +3.0 | −84.2 | 1,020,524 (above; 666,760) | 27,242,808 |
| q22 | 647.5 | 235.0 | 2.76 | 412 | −7.9 | +1.0 | −6.2 | not stated (ambiguous; 666,760) | 0 |
| **total** | **61,387** | **15,579** | **3.940** | **45,808** | **−1,319** | **−21** | **−2,043** | | |

**RSS.** The same seven cells are `ambiguous` as on ADR 603 and 608:
q06, q13, q14, q15, q16, q19, and q22. They are not stated as peaks.
**Spill.** Only q21 spills, 27,242,808 bytes, as before.

**Changes from ADR 608, observed and unattributed:**
- The largest by far is **q01 (−1,400.6 ms)**. q01 carries both paired
  gains.
- **q13 (+145.5 ms)** is in line with ADR 610's paired regression.
- The rest are within about ±60 ms. The larger ones are q06 (−57.5),
  q15 (+49.9), q07 (−31.4), and q10 (+25.3).
  - q06 has no grouped fold. Its −57.5 ms change is consistent with
    between-board variation, and is not attributed here.
  - q15 folds two-phase under ADR 610 and was not paired. Its +49.9 ms
    is observed, with no causal claim; it is a candidate for a paired
    check.

## Gap ranking

| rank | this board | gap ms | ADR 608 | gap ms |
|---|---|---|---|---|
| 1 | q21 | 5,768 | q21 | 5,769 |
| 2 | q09 | 5,349 | q09 | 5,364 |
| 3 | q18 | 4,498 | q18 | 4,506 |
| 4 | q05 | 4,161 | q05 | 4,173 |
| 5 | q07 | 3,296 | q01 | 3,800 |
| 6 | q08 | 2,571 | q07 | 3,325 |
| 7 | q17 | 2,476 | q08 | 2,573 |
| 8 | q01 | 2,395 | q17 | 2,473 |
| 9 | q04 | 2,314 | q04 | 2,325 |

q01 moves from 5th to 8th. The others keep their order.

## Consequence

- This board is the current-tree baseline. Its sealed manifest,
  `target/tpch/adr613-board/perf_board.jsonl.evidence.sealed`, is what
  the next pairs bind. ADR 603 remains the historical anchor.
- The campaign stands at **3.940×**, with Toron 30.2 s above 2× DuckDB.
  The largest gaps are q21, q09, q18, and q05, each 4.2 to 5.8 s. q13's
  regression and q15's observed change are candidates for paired
  checks.
