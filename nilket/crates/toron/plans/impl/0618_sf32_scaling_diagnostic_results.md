# ADR 618 - SF32 scaling diagnostic: results

Run 2026-09-30 on strix: a same-session ladder at SF8, SF16, and SF32,
at a fixed 2 GiB task memory, then the untimed diagnostics. It is a
diagnostic: no token, no pair, no cut, and no saving is projected. SF16
and SF32 artifacts are unsealed and never enter the SF8 board.

**What the third point adds** (candidate readings; the stage figures
come from one armed run per scale):
- **q21 and q18: a single step observed between SF8 and SF16, then
  approximately doubling from SF16 to SF32.**
  - q21's Toron time grows ×3.04 then ×2.08, and its gap goes 3.62× →
    5.13× → 5.09×.
  - q18's grows ×2.40 then ×2.06, and its gap goes 4.06× → 4.61× →
    4.49×.

  Three points do not rule out a later step.
- **q09: the gap widens at both measured doublings.** Toron grows ×2.24
  then ×2.25, against DuckDB's ×2.05 then ×2.06. The gap goes 4.13× →
  4.52× → 4.93×. There is no spill, and the stage topology is the same
  at all three scales. The same probe, materialize, and preparation
  stages exceed ×2 at both steps. That is consistent with a per-row
  cost rising with data size. It remains a hypothesis.
- **q05's gap narrows at SF32** (5.76× → 5.63× → 5.01×). DuckDB grows
  ×2.30 from SF16 to SF32, while Toron stays near ×2. Toron's q05 peak
  RSS reaches 25,848,608–25,861,072 KiB (26.47–26.48 GB, 24.65–24.66
  GiB).
- **q01's ratio holds** (3.50× → 3.49× → 3.47×). Both engines double.

These are scaling observations. **The next SF8 owner is chosen by
absolute cost** from the sealed board, not from these ratios.


## Identity

| item | value |
|---|---|
| plan commit | `7a34e632`; `src` verified identical to `3a782d97` |
| executable | `5eadef2de5eccbae29431468319b186e5d22e602cfe0cb72d093bf3d306868a8` (the build from the plan commit resolved to ADR 616's and ADR 617's executable); manifest `adr618.manifest` (`fc5b9aec…`) |
| DuckDB | `c478c0f4…` (v1.5.4), 1 thread, the default memory limit (proven from each block's artifact header) |
| SF32 data | generated on bigfoot with ADR 613's executable in 2 min 43 s, with a peak RSS of 948 MB. Manifest `fnv1a64:0b9c9a278df6a94a`; `lineitem` 192,000,551 rows, and the other tables ×4 of SF8; 13 GB. Copied to strix; the combined SHA-256 over every file (`7a8fef96…`) and the byte total (13,675,236,238) are identical on both hosts |
| SF8, SF16 data | as in ADR 617 (`fnv1a64:0fc5c06d…`, `fnv1a64:32703e00…`) |
| coordinate | strict parquet, task memory and spill reserve 2 GiB, partition parallelism 1, DuckDB threads 1, affinity 0-15. Every timed row reports these, and the reader checks them |
| host at launch | 2.73 TB free disk and 129 GB available memory, observed immediately before launch; no query process or `adr6*` unit present |
| scripts | under `target/tpch/adr618-scripts/`: `adr618_timed.sh` (`14706a5c…`), `adr618_diag.sh` (`047398cd…`), the scan-work helper reused unchanged (`b7c774d4…`), `adr618_timed_read.py` (`da57348d…`), `adr618_diag_read.py` (`1d5e5e51…`). Launch `launch-timed.sh` (`44ca7e62…`): `RuntimeMaxSec=21600`, `KillMode=control-group`, `SendSIGKILL=yes`, and an `ExecStopPost` hook |
| read-outs | `adr618-timed/readout.txt` (`9458f1be…`), `gated_groups.json` (`380232b2…`); `adr618-diag/readout.txt` (`2beeb703…`) |
| order and log | `declared_order.txt` (`38bd5e31…`), `campaign.log` (`682d52cc…`) |

**The readers are ADR 617's v3**, extended to three scales and both
adjacent steps, with no loosening. They ran their negative controls on
strix before their outputs were used:
- **the timed reader:**
  - a wrong RSS label withholds all 30 groups;
  - a schedule off by one round exits with a structural failure (rc 3)
    and publishes nothing;
  - a wrong SF32 dataset fingerprint withholds exactly the five Toron
    SF32 groups;
  - the real reader, re-run afterwards, reproduced its read-out byte for
    byte.
- **the diagnostics reader:**
  - a correctness failure forced on q09 at SF32 suppresses exactly
    q09's SF16→SF32 readings (its attributed run, its stage comparison,
    and its per-row ratio);
  - a wrong DuckDB executable hash withholds all 15 scan-work
    profiles.

**The amendment, reviewed before launch.** No additional telemetry was
armed. The attributed runs carry only ADR 617's passive instruments
(node attribution, the join-build profile, and the scan ledger).

A read-only code trace was run alongside the review. It names the
decision behind q21's deferral: a replay-1 partition count chosen in
`runtime_prep`, and then an admission guard. It found no env-armed
profile that names q18's spilling join. A code trace identifies a
possible condition, not which branch ran. The runtime evidence below
comes from the armed runs.

**The launch.** The unit started 120 s after its SSH session closed.
- All 210 blocks exited 0, with no leftover child and an empty scratch
  after each. The driver's stderr is empty.
- The campaign ran from 1790772294.9 s to 1790779472.0 s, about 2.0 h.
- The diagnostics' 63 blocks all exited 0 under the same enforcement.

**A departure from the launch rule, disclosed.** The launch's last
output line was a snapshot note, which hid the `systemd_run_exit` line.
I read the host record once, read-only, to confirm the unit had been
scheduled. The read ended at 1790772181 s, **113.9 s before block 1
started**, inside the settle window. No sample overlaps it. It is
recorded as a launch-settle departure.


**A runner departure, disclosed.** The plan requires the per-block
scratch check to fail when the scratch cannot be listed.
`adr618_timed.sh` (and ADR 617's `adr617_timed.sh`) computes it as
`ls -A $SCR | wc -l`, which **does not propagate a listing failure**:
an unreadable scratch would read as 0. So the requirement was **not
enforced** in the timed runner. The diagnostics script does enforce it.

**The audit:**
- The runner creates the scratch directory with `mkdir -p` before its
  first block.
- Every logged block check reported 0.
- Both campaigns' unit stderr logs are empty (0 bytes), and a listing
  error would have been written there. So **no enumeration error was
  recorded.**
- One caveat: ADR 618's launch snapshot reports
  `join_spill_scratch_entries: 0` from a failed listing. The directory
  did not yet exist at launch, and the `cannot access` error was seen in
  the launch session's output. That snapshot value is not evidence of an
  empty scratch.

The archived measured scripts are not changed. Future timed runners use
the diagnostics script's fail-closed check.

**Provenance of the q21 and q18 facts.** They come from a read-only
extraction, `adr618_facts_read.py` (`4aca0422…`), with its output in
`adr618-diag/facts.txt` (`4219046e…`).
- It reads only the identity-gated attributed runs, refusing any run
  whose row does not match its gated timed group, and cites each source
  file's SHA-256:
  - q21's node files: SF8 `31bcb087…`, SF16 `525c27a3…`, SF32
    `d62f196d…`;
  - q18's join-build files: `f5d06ea5…`, `b0153e39…`, `6999876e…`.
- For q21 it names each `inner_1_facts` field's presence. `build_rows`
  and `build_spill_bytes` are present on the `spilled` branch (SF8), and
  null on the `partitioned` branch.
- The effective compact bytes and the admission guard's inputs are
  **not recorded**, and are not presented as established.
- It also lists the per-scale scans per table, below.

**Scans per table, per engine** (from the extraction): unchanged across
the three scales on both engines.

| cell | Toron | DuckDB |
|---|---|---|
| q01 | `lineitem` 1 | `lineitem` 1 |
| q05 | six tables, 1 each | the same |
| q09 | `part` 2, others 1 | all 1 |
| q18 | `lineitem` 3, `customer` 2, `orders` 1 | `lineitem` 2, `customer` 1, `orders` 1 |
| q21 | `lineitem` 3, `orders` 3, `nation` 3, `supplier` 1 | `lineitem` 3, others 1 |

**Visited rows** at SF8 / SF16 / SF32:

| cell | Toron | DuckDB |
|---|---|---|
| q01 | 47,989,007 / 95,988,640 / 192,000,551 | 47,989,007 / withheld / 192,000,551 |
| q05 | 61,269,037 / 122,548,670 / 245,120,581 | the same |
| q09 | 69,669,032 / 139,348,665 / 278,720,576 | 68,069,032 / 136,148,665 / 272,320,576 |
| q18 | 158,367,021 / 316,765,920 / 633,601,653 | 109,178,014 / 218,377,280 / 436,801,102 |
| q21 | 180,047,096 / 360,125,995 / 720,321,728 | 156,047,046 / 312,125,945 / 624,321,678 |

**A units erratum for ADR 616 and ADR 617.** Their KiB tables are
correct, but prose conversions to GB there were wrong. At 1 KiB = 1,024
bytes:
- **ADR 616's q05 peak RSS** "about 9.17 → 6.48 GB" is 9,156,764–9,531,208
  → 6,475,132–6,480,140 KiB, which is **9.38–9.76 → 6.63 GB**
  (8.73–9.09 → 6.18 GiB).
- **ADR 616's q09** "2.46 → 2.24 GB" is **2.52 → 2.29 GB**.
- **ADR 617's q05 SF16** "13.0 GB" is 12,958,624–12,960,060 KiB,
  **13.27 GB** (12.36 GiB).
- **ADR 617's** "DuckDB's q05 stays under 0.3 GB at SF16" is 295,240–297,824
  KiB, **0.30–0.305 GB** (0.28 GiB).

This record states RSS in KiB, with exact conversions.

## The timed campaign

It is ADR 617's runner: one `perf --runs 1 --warmup 1` block per cell,
engine, and scale, with a fresh output directory. Each measured sample
follows its own untimed execution of the same query, a best-effort warm
cache.

The order was declared before launch, and the reader regenerated it
independently:
- 7 rounds × 30 blocks;
- cell order rotated by round;
- scale order (8, 16, 32) in rounds 1, 4, 7; (16, 32, 8) in rounds 2
  and 5; (32, 8, 16) in rounds 3 and 6;
- engine order Toron-first in rounds 1, 2, 5, 6.

**All 30 groups passed.** Median of seven, with the range (all samples
retained):

| cell | engine | SF8 ms | SF16 ms | SF32 ms | ×16/8 | ×32/16 |
|---|---|---|---|---|---|---|
| q01 | Toron | 3,342.8 (3,328.6–3,366.1) | 6,666.7 (6,657.5–6,741.3) | 13,309.8 (13,293.0–13,336.3) | 1.994 | 1.996 |
| q01 | DuckDB | 956.0 (954–962) | 1,913.0 (1,907–1,937) | 3,834.0 (3,826–3,854) | 2.001 | 2.004 |
| q05 | Toron | 3,424.8 (3,410.7–3,470.0) | 6,894.5 (6,889.2–6,901.3) | 14,112.5 (14,074.2–14,156.0) | 2.013 | 2.047 |
| q05 | DuckDB | 595.0 (590–598) | 1,225.0 (1,213–1,228) | 2,816.0 (2,810–2,841) | 2.059 | 2.299 |
| q09 | Toron | 6,755.2 (6,736.3–6,785.6) | 15,125.2 (15,059.6–15,169.2) | 34,081.1 (33,975.4–34,222.8) | 2.239 | 2.253 |
| q09 | DuckDB | 1,637.0 (1,634–1,640) | 3,347.0 (3,344–3,373) | 6,911.0 (6,896–6,957) | 2.045 | 2.065 |
| q18 | Toron | 6,025.9 (6,005.8–6,067.9) | 14,453.7 (14,356.6–**18,459.5**) | 29,762.8 (29,707.9–30,090.8) | 2.399 | 2.059 |
| q18 | DuckDB | 1,484.0 (1,476–1,487) | 3,137.0 (3,131–3,151) | 6,629.0 (6,599–6,649) | 2.114 | 2.113 |
| q21 | Toron | 7,991.1 (7,969.8–8,026.0) | 24,328.1 (24,288.9–**30,845.5**) | 50,717.7 (50,469.9–51,205.7) | 3.044 | 2.085 |
| q21 | DuckDB | 2,207.0 (2,198–2,212) | 4,743.0 (4,737–4,771) | 9,973.0 (9,945–10,026) | 2.149 | 2.103 |

**Outliers.** Each of the two spilling cells at SF16 has one outlier
sample, shown in bold. Both are retained. The medians are reported, and
the outliers are not explained here.

**The gap, Toron/DuckDB by medians:**

| cell | SF8 | SF16 | SF32 |
|---|---|---|---|
| q01 | 3.497× | 3.485× | 3.472× |
| q05 | 5.756× | 5.628× | 5.012× |
| q09 | 4.127× | 4.519× | 4.931× |
| q18 | 4.061× | 4.607× | 4.490× |
| q21 | 3.621× | 5.129× | 5.086× |

ADR 617's SF8 and SF16 medians, from another session, are context only.
Every cell and engine agrees within 1%, except Toron's q21: at SF8 it
was 8,225.1 there and 7,991.1 here (2.8% lower), and at SF16 25,938.6
there and 24,328.1 here (6.2% lower). Not attributed.

**Toron spill and peak RSS** (every row labelled
`above_parent_high_water`):

| cell | spill bytes SF8 / SF16 / SF32 | peak RSS KiB at SF32 (approximate range midpoints, SF8 → SF16 → SF32) |
|---|---|---|
| q01 | 0 / 0 / 0 | 47,768–49,316 (about 33 k → 38 k → 49 k) |
| q05 | 0 / 0 / 0 | 25,848,608–25,861,072 (6.48 M → 12.96 M → 25.85 M) |
| q09 | 0 / 0 / 0 | 8,893,572–8,895,020 (2.24 M → 4.46 M → 8.89 M) |
| q18 | 0 / 420,825,376 / 845,536,568 | 16,126,052–16,127,056 (4.05 M → 8.07 M → 16.13 M) |
| q21 | 27,242,808 / 1,846,534,680 / 3,771,087,240 | 3,074,676–3,075,668 (1.02 M → 1.60 M → 3.08 M) |

**DuckDB's peak RSS at SF32** (KiB):

| cell | KiB |
|---|---|
| q01 | 560,096–561,136 |
| q05 | 530,848–533,364 |
| q09 | 4,476,552–4,478,256 |
| q18 | 3,488,936–3,489,724 |
| q21 | 1,555,972–1,557,316 |


## The diagnostics

**Every gate passed except one scan-work profile:**
- the attributed runs (15) and the q05 receipts (3);
- Toron's progress folds, each reconciling with its ledger receipt;
- the DuckDB sweeps;
- correctness, **pass on all 15 cell-scale combinations**;
- the direct scan-work profiles. **q01 at SF16 is withheld again:** its
  profile identity is unestablished, as in ADR 617, and serialization
  differences are a possible explanation. No correctness inference is
  drawn. q01's SF32 profile passes.

**Diagnostic minus timed median** (context: one armed run, overhead and
drift not apportioned):

| cell | SF8 ms | SF16 ms | SF32 ms |
|---|---|---|---|
| q01 | +107.0 | +239.2 | +466.2 |
| q05 | +338.4 | +706.6 | +1,478.3 |
| q09 | +505.3 | +968.2 | +2,155.7 |
| q18 | +798.9 | **+5,294.4** | +9,095.6 |
| q21 | +574.9 | **+3,786.6** | +6,979.7 |

The armed runs of the two spilling cells differ from their timed
medians by far more than in ADR 617, where the differences were +1,985.0
and +1,266.4 ms at SF16. **So q18's and q21's single-run stage
figures here are weak evidence.**

**Each engine's time per visited row,** with its own counter, over each
step:

| cell | Toron 8→16 | Toron 16→32 | DuckDB 8→16 | DuckDB 16→32 |
|---|---|---|---|---|
| q01 | 0.997 | 0.998 | withheld | withheld |
| q05 | 1.006 | 1.023 | 1.029 | 1.149 |
| q09 | 1.119 | 1.127 | 1.022 | 1.032 |
| q18 | 1.199 | 1.029 | 1.057 | 1.056 |
| q21 | 1.522 | 1.042 | 1.074 | 1.051 |

**Stage attribution.** Stages are paired only where the route and the
topology correspond.

- **q09** (paired at both steps; top-level reconciled; strict-incomplete
  in the replay-2 suffix sub-stages, as in ADR 617):

  | stage | ×8→16 | ×16→32 |
  |---|---|---|
  | `base_probe` | 2.523 | 2.559 |
  | `base_materialize` | 2.554 | 2.714 |
  | `suffix_probe` replay 1 | 2.364 | 2.392 |
  | `suffix_probe` replay 2 | 2.387 | 2.219 |
  | `runtime_prep` | 2.137 | 2.316 |
  | `scan_pull` | 2.064 | 2.041 |
  | `repartition` | 1.996 | 2.003 |
  | `fold` | 2.042 | 2.097 |
  | `cleanup` | 2.073 | 2.068 |

  The probe and materialize stages exceed ×2 at both steps. The scan,
  repartition, fold, and cleanup stages stay near ×2. At SF32 the
  largest stages are `runtime_prep` (7,746.6 ms), `suffix_probe`
  replay 2 (6,645.3), `base_probe` (4,243.7), and `suffix_probe`
  replay 1 (4,168.7).
- **q21:**
  - **The replay-1 decision** (`inner_1_facts`, recorded by node
    attribution):

    | scale | branch | `partition_count` | estimated build bytes | sizing rows |
    |---|---|---|---|---|
    | SF8 | `spilled` | 1 | 701,512,560 | 5,845,938 |
    | SF16 | `partitioned` | 2 | 1,403,352,360 | 11,694,603 |
    | SF32 | `partitioned` | 4 | 2,806,194,120 | 23,384,951 |

    SF8→SF16 changes topology (not paired), as in ADR 617.
  - **SF16→SF32 is paired**, with the same topology and strict
    passing:
    - `deferred_replay` 17,175.3 → 33,858.7 ms (×1.971), with spill
      1,846,534,144 → 3,771,086,704 B;
    - `runtime_prep` ×1.996;
    - `base_scan` ×2.018;
    - **`inner_replay_0` 2,609.0 → 7,180.6 ms (×2.752).**

    Inside `deferred_replay` at SF32: `reservoir_buffer` 14,397.1 ms,
    `reservoir_finish` 4,021.7, `inner_replay_1` 13,460.9.

    Given this cell's armed-run excess at both scales, `inner_replay_0`'s
    ×2.75 is an observation to re-check, not a finding.
- **q18:** no stage vocabulary, so node-kind buckets only. The join
  bucket (3 calls) carries all of the spill: 420,825,376 B at SF16 and
  845,536,568 B at SF32.
  - **The join-build profile** records only in-memory builds. SF8's
    five records include a `replay_id` 0 build of 12,000,000 rows (all
    unique keys). At SF16 and SF32 that record is absent, while the
    other builds scale (`replay_id` 2: 47,989,007 → 95,988,640 →
    192,000,551 rows, with 12 M → 24 M → 48 M unique keys).
  - **`replay_id` 0 is a candidate** because its in-memory profile
    record is absent at SF16 and SF32. **The spilling join is not
    identified.** Record ids can be local to their context (SF8 has two
    records each with `replay_id` 1 and 2), and a record can be absent
    for other reasons. The join kind's time and spill remain an
    aggregate over three calls.
  - Its bucket timings here (966 → 7,576 → 12,796 ms) come from runs
    with large armed-run excess, so they are not used for ratios.
- **q05** (paired at both steps): `runtime_prep` ×1.977 then ×2.016.
  The other stages of at least 150 ms range from ×1.97 to ×2.26.
  - **The receipt:** the replay-1 build estimate is 5,758,680,840 →
    11,518,636,800 → 23,040,066,120 bytes, against the 34,359,738,368-byte
    cap. It implies `in_memory` at every scale; that is estimate-implied.
- **q01** (paired, strict at both steps): every stage of at least 150
  ms is ×1.99 to ×2.01.


## Reading (candidates)

- **q09 is the leading scaling candidate.** Its gap widens at both
  measured doublings. There is no spill and no topology change, and
  its time per visited row rises ×1.12 and ×1.13 at the two steps,
  against DuckDB's ×1.02 and ×1.03. The same stages exceed ×2 at both
  steps: probe, materialize, and preparation. What makes them grow
  faster than linearly is not established here.
- **q21 and q18: a single observed step, then approximately doubling.**
  - For q21, the step coincides with the replay-1 decision moving from
    `spilled` (one partition) to `partitioned`: 2 partitions at SF16, 4
    at SF32, as the estimated build doubles each time. Within the
    partitioned regime, the whole query grows ×2.08 from SF16 to SF32,
    and the deferred replay ×1.97.
  - For q18, the join kind's spill (an aggregate over three calls)
    doubles with scale once it starts. Which join spills is not
    identified.
  - A later step at a larger scale is not ruled out.
- **q05:** its gap narrows at SF32 because DuckDB's time grows faster.
  Toron's peak RSS doubles at each step:
  - 6.63 GB at SF8, 13.27 GB at SF16, and 26.47–26.48 GB at SF32
    (24.65 GiB);
  - against DuckDB's 530,848–533,364 KiB at SF32 (0.54–0.55 GB,
    0.51 GiB).
- **q01:** ratios hold, with every stage ×2.


## Consequence

- **No cut is proposed here.**
- **The scaling priority** is q09's superlinear probe, materialize, and
  preparation stages, a candidate owner for a successor record with its
  own plan. It should first gather row-flow, group, and cardinality
  evidence before calling it a per-row kernel issue. After it come
  q21's partitioned regime, and q18's join spill (the spilling join not
  yet identified; `replay_id` 0 is a candidate). q21's and q18's weak
  single-run timings are not a mechanism decomposition.
- **The SF8 campaign** chooses its next owner by absolute cost from the
  sealed board.
- A proportional-memory panel, and whether larger scales show further
  steps, remain separate records.
