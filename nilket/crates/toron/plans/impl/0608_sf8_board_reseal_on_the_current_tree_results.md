# ADR 608 - SF8 board re-seal on the current tree: results

Run 2026-09-29 on strix: one step, both engines, all 22 cells, 5 runs +
1 warmup, at the ADR 603 anchor's coordinate. **Sealed** with
`candidate_authority` and `final_authority` true.

**Toron 62.706 s against DuckDB 15.600 s: 4.020×**, against ADR 603's
63.430 s, 15.650 s, and 4.053×. The target arithmetic, named twice:
- **Against this board's DuckDB total:** 2 × 15.600 s is 31.200 s.
  Toron is **31.506 s** above it.
- **Against the campaign's fixed original threshold:** 31.366 s, which
  is 2× ADR 591's DuckDB sum and the threshold ADR 603 used. Toron is
  **31.340 s** above it.

No new absolute target is substituted.

Every Toron digest equals ADR 603's. Every cell passed and is stable:
the largest `(max − min) / median` is 0.082, against the 0.20 bound.
DuckDB's per-cell medians moved by 9 ms or less (−50 ms in total). That
is drift, reported for context.

Toron's total fell 724 ms between the boards. The record declines to
apportion that change to ADR 606's token or to anything else. Only the
two paired records measured the token: q05 (retained, 444.7 ms) and q09
(not established, 72.3 ms).


## Identity

| identity | value |
|---|---|
| measured tree | `6c052239b85405b14c30de48fb76603154538644`, commit `44238558` (this plan's commit; code equal to `artifact/adr606-treatment`, differing in `plans/` only) |
| executable | `889fa4891020c53a59bd5a2bcb26273caf3d5f180a49818aecaa8f0248e8a2d8` (harness and Toron): the bigfoot build reproduced the ADR 606 treatment's bytes. Manifest `target/tpch/evidence/build/adr608-measured.manifest` (`1de50436…`) |
| DuckDB | `c478c0f49f6755100d55dee32de485c0f00f3729b40b62658a83b0666b684707`, v1.5.4, one thread, `duckdb_memory_limit` null (provenance `default`) |
| recipe | `9af51cbc…` |
| dataset | `fnv1a64:0fc5c06d19c5ed23` |
| ladder (bigfoot, measured tree) | toron 5,492 passed, 0 failed, 3 ignored; toron-tpch 674 passed, 0 failed, 6 ignored |
| artifact / sealed manifest | `7662f6f22943fa6c76ee15bb494af0f06285392d7983a86e8091a7800a3f3dcc` / `ed355df673b964376ffe9b873154cf62c1b78b7f4fa6e9102cea237aff141d82` (`target/tpch/adr608-board/perf_board.jsonl.evidence.sealed`) |
| evidence tag | `evidence/6c052239b854-7662f6f22943`, pushed to strix's seal remote, fetched to the workstation, and pushed with the closing commit |
| archive | harness, Toron, and DuckDB executables under `target/tpch/evidence/archive/` |
| build host / run host | bigfoot / strix |

## Procedure

ADR 603's anchor step, verbatim. Host scripts were generated from ADR
603's with this record's paths, and a grep for other ADR numbers found
none. Token `adr608-1d9aa4d4842f`:
- launched 20:50:06 CDT, with the unit 120 s later;
- completion at 21:06:30 (`result=success exit_code=exited
  exit_status=0`);
- before: load 0.00 and 42 °C, scratch empty, output fresh;
- after, taken by the read-out session once the datagram had arrived:
  load 0.93 and 48 °C, scratch empty.

The header records:
- the host quiescent at start and end;
- the spill scratch empty before and after;
- the output directory fresh;
- affinity `0-15`, requested and observed.

264 samples and 44 summaries. No profiled child ran.

## The board

| query | Toron ms | DuckDB ms | ratio | gap ms | Toron vs 603 | DuckDB vs 603 | Toron peak RSS KiB (label; parent at spawn) | spill bytes |
|---|---|---|---|---|---|---|---|---|
| q01 | 4,752.7 | 953.0 | 4.99 | 3,800 | +95.3 | -4.0 | 29,828 (above; 10,144) | 0 |
| q02 | 1,435.1 | 128.0 | 11.21 | 1,307 | -49.1 | -1.0 | 980,952 (above; 17,884) | 0 |
| q03 | 1,872.1 | 654.0 | 2.86 | 1,218 | -8.3 | -4.0 | 3,452,792 (above; 21,816) | 0 |
| q04 | 2,780.1 | 455.0 | 6.11 | 2,325 | -1.4 | -1.0 | 1,232,364 (above; 124,648) | 0 |
| q05 | 4,764.8 | 592.0 | 8.05 | 4,173 | -520.9 | -5.0 | 9,168,280 (above; 124,648) | 0 |
| q06 | 741.5 | 365.0 | 2.03 | 377 | +54.1 | -1.0 | not stated (ambiguous; 124,648) | 0 |
| q07 | 3,918.3 | 593.0 | 6.61 | 3,325 | -22.3 | +0.0 | 1,600,852 (above; 124,648) | 0 |
| q08 | 3,218.1 | 645.0 | 4.99 | 2,573 | -42.7 | -2.0 | 635,768 (above; 124,904) | 0 |
| q09 | 6,995.5 | 1,632.0 | 4.29 | 5,364 | -118.4 | -3.0 | 2,456,540 (above; 124,904) | 0 |
| q10 | 2,354.4 | 1,124.0 | 2.09 | 1,230 | -17.8 | -6.0 | 1,060,344 (above; 124,904) | 0 |
| q11 | 913.0 | 107.0 | 8.53 | 806 | +7.2 | -1.0 | 851,748 (above; 652,112) | 0 |
| q12 | 2,071.5 | 509.0 | 4.07 | 1,563 | +8.0 | -2.0 | 1,837,672 (above; 652,112) | 0 |
| q13 | 2,492.5 | 1,194.0 | 2.09 | 1,299 | +2.1 | -5.0 | not stated (ambiguous; 652,112) | 0 |
| q14 | 639.9 | 406.0 | 1.58 | 234 | +3.8 | +3.0 | not stated (ambiguous; 652,112) | 0 |
| q15 | 1,231.7 | 313.0 | 3.94 | 919 | -2.5 | +1.0 | not stated (ambiguous; 652,112) | 0 |
| q16 | 1,367.3 | 191.0 | 7.16 | 1,176 | +26.0 | +1.0 | not stated (ambiguous; 652,112) | 0 |
| q17 | 3,098.4 | 625.0 | 4.96 | 2,473 | -1.0 | -1.0 | 3,726,812 (above; 652,112) | 0 |
| q18 | 5,989.9 | 1,484.0 | 4.04 | 4,506 | -48.7 | -8.0 | 4,045,628 (above; 652,112) | 0 |
| q19 | 1,765.9 | 676.0 | 2.61 | 1,090 | -2.8 | -1.0 | not stated (ambiguous; 652,112) | 0 |
| q20 | 1,677.1 | 518.0 | 3.24 | 1,159 | +0.0 | +0.0 | 2,237,532 (above; 652,112) | 0 |
| q21 | 7,970.7 | 2,202.0 | 3.62 | 5,769 | -86.2 | -9.0 | 1,020,412 (above; 652,112) | 27,242,808 |
| q22 | 655.4 | 234.0 | 2.80 | 421 | +1.7 | -1.0 | not stated (ambiguous; 652,112) | 0 |
| **total** | **62,706** | **15,600** | **4.020** | **47,106** | **−724** | **−50** | | |

**RSS labels.**
- "above" is `above_parent_high_water`: the child's `ru_maxrss` exceeds
  the harness parent's high-water at spawn, so it is the child's own
  peak.
- On the seven `ambiguous` cells the reading does not exceed the
  parent's high-water, so it is not stated as that cell's peak. They
  are the same seven cells as on ADR 603's board.
- q05's 9,168,280 KiB and q09's 2,456,540 KiB are consistent with the
  paired records' treatment readings. Here they are unpaired
  observations.

**Spill.** Only q21 spills: 27,242,808 bytes, as on ADR 603's board.

**Changes between boards, unattributed.**
- The largest Toron changes are q05 (−520.9 ms), q09 (−118.4 ms), q01
  (+95.3 ms), q21 (−86.2 ms), and q06 (+54.1 ms).
- q01 routes `scan_grouped` and has no wrapped reoriented stage. Its
  change is between-board variation, and the record reads nothing
  into it.
- No other cell's change is read as an effect.

## Gap ranking

| rank | this board | gap ms | ADR 603 | gap ms |
|---|---|---|---|---|
| 1 | q21 | 5,769 | q21 | 5,846 |
| 2 | q09 | 5,364 | q09 | 5,479 |
| 3 | q18 | 4,506 | q05 | 4,689 |
| 4 | q05 | 4,173 | q18 | 4,547 |
| 5 | q01 | 3,800 | q01 | 3,700 |
| 6 | q07 | 3,325 | q07 | 3,348 |
| 7 | q08 | 2,573 | q08 | 2,614 |
| 8 | q17 | 2,473 | q17 | 2,473 |
| 9 | q04 | 2,325 | q04 | 2,325 |

The ranking is unchanged apart from q05 and q18 swapping places.

## Consequence

- This board is the current-tree baseline. Its sealed manifest,
  `target/tpch/adr608-board/perf_board.jsonl.evidence.sealed`, is what
  ADR 609's pairs bind. The ADR 603 anchor remains the historical
  anchor.
- ADR 609 proceeds, since the board sealed. It covers the q01 borrowed
  multi-key group lookup, against q01's cell here: Toron 4,752.7 ms,
  DuckDB 953.0 ms, 4.99×. q18 is the breadth cell if its plan and
  receipt confirm the same multi-key UTF-8 grouped-fold owner.
