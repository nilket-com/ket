# ADR 616 - reused build batches skip an exchange whose partitions are re-concatenated: results

Run 2026-09-30 on strix, at ADR 613's coordinate, bound to ADR 613's
sealed board. The sequence was the 22-cell gate, the q05 pair, the q09
breadth pair, and then the mechanism runs.

**Retained on q05.** The treatment's median is 3,404.6 ms against
control's 4,745.3 ms. The median paired gain is **1,340.7 ms (28.3%)**,
against a bar of 121.7 ms (`2 × range_control`, range 60.8 ms), and every
pair is strictly positive (1,283.2 to 1,365.3 ms). The token stays
`Bypass`.

**The q09 breadth pair gained, with no regression.** The treatment's
median is 6,720.6 ms against control's 6,986.2 ms. The median paired
gain is **260.0 ms (3.7%)**, against q09's own bar of 167.8 ms (range
83.9 ms), and every pair is positive (207.5 to 327.0 ms). Under ADR
607's interpretation, this is coverage of the token retained on q05: an
established q09 effect, and ADR 607's regression guard does not fire.

Both pairs had digest parity on every row, the same plan fingerprint as
ADR 613's cell, and no spill observed. **Peak RSS fell on both cells**
(`above_parent_high_water` on every row):

| cell | control KiB | treatment KiB |
|---|---|---|
| q05 | 9,156,764–9,531,208 | 6,475,132–6,480,140 |
| q09 | 2,459,152–2,460,860 | 2,237,948–2,239,056 |

The q05 drop, about 2.7 GB, is consistent with the per-partition copy of
the 48 M-row build input no longer being made. That is observed, and not
otherwise established.

No 22-cell total has been measured with this token.


## Identity

| identity | value |
|---|---|
| control | commit `de3a13a3`, tree `a59304d2b50fedbcc15a1b4dff17fc71e96c3b52`, tag `artifact/adr616-control`; executable `8ccc3f7d7c2f20803d3ff07d423e3d4611b0378b98d8bb6d77d90b90ae25eabb`; manifest `adr616-control.manifest` (`b1705e89…`); `REUSE_BUILD_EXCHANGE = Repartition` |
| treatment | commit `5c193a72`, tree `00ac4114ffdd11b4d71c98fb77d2c1eabf892638`, tag `artifact/adr616-treatment`; executable `5eadef2de5eccbae29431468319b186e5d22e602cfe0cb72d093bf3d306868a8`; manifest `adr616-treatment.manifest` (`e73534fc…`); `Bypass` |
| difference | one statement in `exec/join/streaming.rs`. rustfmt joins the treatment's shorter line, so the diff is two lines out and one in |
| sealed baseline | `target/tpch/adr613-board/perf_board.jsonl.evidence.sealed` (`576dab40…`) |
| ladders (bigfoot, both trees, fmt clean) | toron 5,517 passed, 0 failed, 3 ignored (2 new ADR 616 fixtures); toron-tpch 674 passed, 0 failed, 6 ignored; release builds warning-free under both token settings |
| build host / run host | bigfoot / strix |

Control's q05 median (4,745.3 ms) is 6.7 ms below ADR 613's sealed q05
cell (4,752.0), and control's q09 (6,986.2) is 7.8 ms above ADR 613's
(6,978.4). Both are recorded and not attributed.

**Implementation review.** Codex's review of the arms asked for one
amendment before staging. It rides in this commit's plan:
- per-row spill status is labelled only as **observed no spill,
  spilling, or unreadable**. An empty observation list is evidence of
  nothing either way, and zero `spill_bytes` supports only "no spill
  observed";
- the per-stage path is named only where an observation or the untimed
  estimate receipt supplies it, and the receipt's path is **the path its
  estimate implies**, not independent proof of the path taken;
- the gate fails on missing or unreadable spill fields.


## The gate

Scripts: `adr616_gate.sh` (`8ab37012…`) and `adr616_gate_read.py`
(`c76d9c52…`), with the read-out in `readout.txt` (`86d38aec…`), under
`target/tpch/adr616-strix-host-record/gate/`.

**The gate passed:**
- All 22 cells on both arms: status pass, and digest and plan
  fingerprint equal to ADR 613's sealed cells.
- Spill status is the same on both arms for every cell. Only q21 is
  spilling, with 27,242,808 bytes on both arms, as on the board. No
  cell spilled under the treatment alone, and no field was unreadable.
- **The untimed q05 preflight,** with the cap-check receipt, per stage
  (replays 1 to 4):
  - control: reused, `Repartition`, `partition_match` equivalence
    passed;
  - treatment: reused, `Bypass`, `commutative_row_digest_sketch`
    passed (a sketch, not proof of multiset equality);
  - the build estimate agrees on both arms for every stage. For replay
    1 it is 5,758,680,840 bytes against a memory cap of 34,359,738,368
    bytes, implying `in_memory`. That cap is 32 GiB, while the
    coordinate's task memory is 2 GiB. This is observed, and not
    investigated here.
- **The armed attribution runs** (q03, q05, q07, q08, q09), one per arm,
  all digest-gated and with refinements reconciled:
  - control shows `prep_exchange` on q05's four replays (replay 1:
    1,647.5 ms), q07's (225.5), q09's replays 2 and 3 (196.6 and 200.8),
    and q08's (at most 15.9). q03 has none;
  - the treatment shows **no `prep_exchange` in any of the five
    cells**.

**Two corrections to the read-out, both before the gate's verdict was
used,** with the same saved artifacts re-read:
1. **A positive control.** As first written, the check that q05 has no
   `prep_exchange` under `Bypass` would have passed vacuously if the
   reader had failed to find the stage at all. The reader now also
   requires control's q05 to show `prep_exchange` on all four replays.
   The version before is `adr616_gate_read-v0.py` (`7aa14deb…`).
2. **The spill label.** The reader first labelled q11, q14, and q19
   "spilling" because their rows carry a join-spill observation, but
   those observations say `path: in_memory` with 0 bytes. The label now
   reads spilling only when `spill_bytes` is positive or an observation
   has positive bytes or a path other than `in_memory`. The mislabel was
   the same on both arms, so it could not have changed the verdict. The
   version before is `adr616_gate_read-v1.py` (`ae1a319a…`).


## The pairs

ADR 597's driver, bound to ADR 613's sealed board, with ADR
592/593/595/599, one attempt each. For both, the start gate admitted the
run (load 0.00 and 0.06, package 35 and 37 °C), and launch separation
and the measurement window were verified. The spill scratch was empty
after each run.

| cell | control median ms | treatment median ms | median gain ms | bar ms | pairs positive | verdict |
|---|---|---|---|---|---|---|
| **q05** | 4,745.3 | 3,404.6 | **1,340.7** | 121.7 | 5 of 5 | **retained** |
| q09 | 6,986.2 | 6,720.6 | 260.0 | 167.8 | 5 of 5 | coverage: gain, no regression |

Summaries: `target/tpch/adr616-q05-pair/summary.json` (`392402fe…`) and
`target/tpch/adr616-q09-pair/summary.json` (`b197cf7c…`).


## The mechanism

Five armed q05 runs per arm, alternating control and treatment, with
ADR 615's instrument. There was no cap-check receipt (recorded per run)
and no scan ledger. **Every run passed its gate:**
- digest and plan fingerprint equal to ADR 613's q05 cell;
- `refinements_reconciled` is true;
- all four replays have a `prep_build`;
- the sub-stages plus the residual equal `prep_build` exactly;
- the expected sub-stages are present: four under control, and three
  under the treatment, with no `prep_exchange`.

Scripts `adr616_mech.sh` (`b0abd638…`) and `adr616_mech_read.py`
(`7e09e869…`); read-out `readout.txt` (`3ea42f73…`), under
`target/tpch/adr616-strix-host-record/mechanism/`.

**Replay 1's `prep_build`, median of five (range):**

| sub-stage | control ms | treatment ms | difference |
|---|---|---|---|
| `prep_exchange` | 1,644.8 (1,642.6–1,663.7) | absent | −1,644.8 |
| `prep_scan_decision` | 926.0 (925.7–926.7) | 925.1 (923.6–929.7) | −0.9 |
| `prep_table_build` | 774.5 (772.9–775.0) | 776.2 (775.1–778.1) | +1.7 |
| **`prep_input_materialize`** | 241.9 (241.9–242.2) | **557.3** (556.7–558.4) | **+315.4** |
| residual | 77.8 (77.7–78.0) | 43.4 (43.0–43.6) | −34.4 |
| **`prep_build`** | **3,664.1** (3,662.4–3,682.9) | **2,301.3** (2,299.5–2,309.6) | **−1,362.8** |

Each displayed value is its own five-run median, so the displayed
sub-stage medians need not add up to the displayed `prep_build` median.
The exact reconciliation holds per run.

Whole-query `execute_ms` in these armed runs: 4,797.0 against 3,472.2
ms. These are diagnostics, not paired.

**Reading, as observed:**
- The exchange's time is gone.
- **`prep_input_materialize` rose by 315 ms.** Under `Repartition`,
  `concat_or_empty` joined four partition batches. Under `Bypass`, it
  joins the cap check's batches directly, as the scan produced them. So
  the build still pays 557 ms to concatenate 48 M rows into one batch.
  Why that concatenation costs more than the one over four partitions is
  not established: the number and size of the scan's batches were not
  recorded.
- The scan decision and the table build are unchanged. The join-build
  profile's replay-1 record is identical on both arms: `HashPartitioned`,
  `int64` keys, 47,989,007 build rows, 12,000,000 unique keys, at most 7
  rows per key, and 480,000,000 row-list bytes. `build_micros` medians
  are 774.5 and 776.2 ms.
- Replays 2 to 4 are small on both arms (replay 2: 6.9 against 5.5 ms).


## Consequence

- **`REUSE_BUILD_EXCHANGE = Bypass` is retained.** On q05, 1,340.7 ms per
  query; on q09, 260.0 ms of coverage. Peak RSS is lower on both.
- **The next question inside this span,** stated as a question: the
  concatenation of 48 M rows into one batch takes 557 ms under
  `Bypass`. The in-memory runtime keeps that batch: the table is built
  over it, and the probe reads build payload from it
  (`probe_reoriented_inner_join_in_memory`). Could the table be built,
  and probed, over batch-and-row references into the cap check's
  batches, with payload access preserved, instead of over one
  concatenated copy? That would need its own plan, mechanism, and
  pair.
- The Forward note stands: whether other routes, for example q21's
  `runtime_prep`, re-concatenate an exchange's output into a single
  build is established per route, by its own evidence.
- A board refresh is needed before any campaign-level claim.
