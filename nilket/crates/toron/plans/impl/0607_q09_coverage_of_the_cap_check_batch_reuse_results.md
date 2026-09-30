# ADR 607 - q09 coverage of the cap check's batch reuse: results

Run 2026-09-29 on strix, at the sealed ADR 603 anchor's coordinate, with
ADR 606's executables unchanged: the eligibility gate, one paired
attempt on q09, then the mechanism check.

**The q09 effect is not established.** The treatment's median is
6,972.9 ms against control's 7,056.6 ms. The median paired gain is
**72.3 ms (1.0%)**, against a bar of 74.7 ms (`2 × range_control`,
range 37.3 ms): short by 2.4 ms. All five pairs are strictly positive
(15.6 to 111.3 ms). **No regression guard fired**:
- every row's digest equals the anchor's q09 Toron digest;
- `spill_bytes` is 0 on every row, with no join-spill observations;
- the treatment is not slower.

By the record's predeclared interpretation, the retained token stays
`Reuse`, and this record makes no retain or revert decision.

The driver's verdict block reads
`"consequence": "cut reverted in the same commit; candidate closed"`,
`"status": "not_established"`. That consequence is ADR 588's generic
action label for a cut under test. It does not apply to this coverage
record, and nothing is reverted. The record's own predeclared
interpretation is authoritative.

**Peak RSS** is higher on the treatment by about 123 MB (5.3%) on every
row:
- treatment: 2,455,960–2,456,536 KiB;
- control: 2,322,232–2,333,356 KiB.

This is an observed cost beside the time result, with no cause asserted
from the pair.

The mechanism check shows the predeclared scan counts exactly:
partsupp, orders, and nation are scanned once (twice before), and part
is still scanned twice. This is a q09 cell measurement. It infers no
speed contribution to the 22-cell total.


## Identity and procedure

| identity | value |
|---|---|
| control | ADR 606's `artifact/adr606-control`, commit `1e187602`, tree `2e0c2aab`, executable `bd72eec2c373434aefdd91b4f945dfd92f390c11f43fac73643b28748c3b77a7`, `Discard` |
| treatment | ADR 606's `artifact/adr606-treatment`, commit `85ff3958`, tree `64a8100b`, executable `889fa4891020c53a59bd5a2bcb26273caf3d5f180a49818aecaa8f0248e8a2d8`, `Reuse` (main's code: `ad675ae4` differs in documents only) |
| staging | both executables re-verified with `sha256sum -c` on strix before the gate; manifests `adr606-control.manifest` (`13a48a25…`) and `adr606-treatment.manifest` (`7fd8dee1…`), as launched in ADR 606 |
| sealed anchor | `target/tpch/adr603-anchor/perf_board.jsonl.evidence.sealed`; artifact SHA-256 `d4044c40cfe517085ec5754b4de76ef647bdbaf29cd3294c325496a9249c3273`; bound cell q09 Toron, median 7,114.0 ms, plan fingerprint `5b3e374cd2d74c4a` |
| coordinate | SF8 strict parquet, task memory and spill reserve 2 GiB each (explicit), partition parallelism 1, affinity `0-15`, dataset `fnv1a64:0fc5c06d19c5ed23` |
| build | none; no code changed in this record |

Host scripts were generated from this record's paths, and a grep for
ADR-numbered paths found only the intended references:
`adr606-{control,treatment}.manifest`, `adr603-anchor`, and `adr607-*`.
`snapshot.sh` takes the output directory as an argument. Both launches
recorded:
- `join_spill_scratch_entries: 0`;
- `output_directory_exists_before_run: no`.

## The eligibility gate

One q09 execution per arm with `TORON_REORIENTED_CAP_CHECK_RECEIPT_PATH`
(`adr607-strix-host-record/receipt-gate`). Both runs passed, with
digest and plan fingerprint equal to the anchor's and `spill_bytes` 0.
The spill scratch was empty afterwards. Table ids are those of the
sealed compact shape: t1 part, t4 partsupp, t5 orders, t6 nation.

| replay | build child | decision | control path | partitions | reused (Discard / Reuse) | equivalence |
|---|---|---|---|---|---|---|
| 1 | Filter (EvaluateInToron) part | materialized_batches | streamed_filter_exchange_attempt | 4 | false / false | not checked |
| 2 | Scan partsupp | materialized_batches | bound_exchange | 4 | false / true | passed |
| 3 | Scan orders | materialized_batches | bound_exchange | 4 | false / true | passed |
| 4 | Scan nation | materialized_batches | bound_exchange | 4 | false / true | passed |

The receipts match the plan's table, and the gate passed. The perf row
has no route field. At the gate, the route
`reoriented_inner_chain_grouped` is **inferred** from these receipts,
since only that route's wrapped arm writes them. The census below
observes it directly.

## The pair

Token `adr607-7e3b60c0c4e6`, launched 20:11:19 CDT. The first child ran
at 20:13:27, and completion arrived at 20:15:23
(`result=success exit_code=exited exit_status=0`).
- **Separated launch:** verified, with the first child 127.9 s after
  the launcher's logout, and no session events in the measurement
  window.
- **Start gate:** admitted at load 0.02 and 37 °C (bounds 0.3 and
  45 °C) after the 5 s settle.
- **Control variability:** ratio 0.0053, within the 0.04 bound.
- **Distance from the anchor:** control's median is 57.4 ms (0.8%)
  below the sealed q09 median.

| pair | order | control ms | treatment ms | gain ms |
|---|---|---|---|---|
| 0 | C, T | 7,045.5 | 7,029.8 | 15.6 |
| 1 | T, C | 7,081.4 | 6,970.2 | 111.3 |
| 2 | C, T | 7,056.6 | 6,972.9 | 83.7 |
| 3 | T, C | 7,066.8 | 7,009.2 | 57.7 |
| 4 | C, T | 7,044.1 | 6,971.8 | 72.3 |
| **median** | | **7,056.6** | **6,972.9** | **72.3** (bar 74.7) |

Warmups: control 7,086.9 ms, treatment 7,003.3 ms. Every row passed ADR
592's validator, including the `query` check (`q09`).

| interpretation (predeclared) | outcome |
|---|---|
| established: median gain > `2 × range_control`, every pair positive | no: 72.3 ≤ 74.7, though every pair is positive |
| not established, without regression | **this one** |
| regression: digest mismatch; spill on a treatment row only; treatment slower by > `2 × range_control` with every pair negative | none |

## The mechanism check

ADR 605's `scan-work-census` on q09 against the treatment executable
(token `adr607m-1abc29f1456b`, launched 20:15:54 CDT, completion
20:18:21, success). The cell is complete, with no exclusions:
- T2 and T3 passed, with digests equal to the anchor's;
- `scan_work_valid` is true, so the progress fold reconciles with the
  ledger;
- plan fingerprint `5b3e374cd2d74c4a` agrees across T2, T3, and the
  anchor;
- the route is **observed**: `reoriented_inner_chain_grouped`;
- D2 passed.

The comparison is with ADR 605's q09 census, a pre-606 executable
(`2560c00c…`), not this pair's control.

| table | predeclared scans | Toron scans, 605 → 607 | rows visited, 605 → 607 | decoded bytes, 605 → 607 | DuckDB rows visited |
|---|---|---|---|---|---|
| partsupp | 1 | 2 → **1** | 12,800,000 → 6,400,000 | 410,247,040 → 205,123,520 | 6,400,000 |
| orders | 1 | 2 → **1** | 24,000,000 → 12,000,000 | 288,593,280 → 144,296,640 | 12,000,000 |
| nation | 1 | 2 → **1** | 50 → 25 | 1,552 → 776 | 25 |
| part | 2 | 2 → 2 | 3,200,000 → 3,200,000 | 174,292,404, unchanged | 1,600,000 |
| supplier | 1 | 1 → 1 | 80,000 | unchanged | 80,000 |
| lineitem | 1 | 1 → 1 | 47,989,007 | unchanged | 47,989,007 |

T2's parquet ledger drops by exactly the reachable volume:
- rows: 88,069,057 → 69,669,032 (−18,400,025);
- raw bytes: 4,333,220,308 → 3,983,799,372 (−349,420,936);
- batch scans: 10,757 → 8,509;
- decode time: 821,664 µs → 731,719 µs.

The runtime-filter bridge still carries no filter (`no_observed_effect`
on every table). By the two engines' reported visited-row
counters (an operational comparison, as in ADR 605), part now visits
twice DuckDB's rows. It is the only q09 table where Toron's visited
rows exceed DuckDB's.

## Consequence

- The token stays `Reuse`, which is ADR 606's decision. This record
  changes no code. The closing commit adds this results record and the
  ADR's status.
- On q09 the reuse removes the predeclared duplicated scans. The time
  effect is under the bar, and the peak-RSS cost is about 123 MB.
- Part's second scan remains. Removing it needs either the treatment to
  reproduce the streamed filter-exchange path, or the `LIKE` filter at
  the scan (ADR 605's filter-placement candidate). Either is a later
  record.
- Next, per the plan's Forward: re-seal the full board on main's tree
  at the anchor coordinate, then choose the next owner by absolute gap.
