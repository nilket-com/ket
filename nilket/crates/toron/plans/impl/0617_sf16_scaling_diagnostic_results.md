# ADR 617 - SF16 scaling diagnostic: results

Run 2026-09-30 on strix: the timed campaign, then the untimed
diagnostics. It is a diagnostic: no token, no pair, no cut, and no
saving is projected. SF16 artifacts are unsealed and never enter the
SF8 board.

**Two transitions were observed at SF16,** at a fixed 2 GiB task
memory:
- **q21**'s execution-stage topology changes. A deferred,
  reservoir-buffered replay appears, holding 1.85 GB of spill (27 MB at
  SF8, all of it then in `runtime_prep`). Toron's time grows ×3.15
  against DuckDB's ×2.16, and the gap goes from **3.73× to 5.44×**.
- **q18**'s join node-kind bucket (three join calls) starts spilling:
  421 MB, none at SF8. The bucket grows ×4.31, the whole query ×2.40
  against DuckDB's ×2.11, and the gap goes from **4.03× to 4.58×**.

**q09's gap also widens,** from 4.11× to 4.51× (Toron ×2.24, DuckDB
×2.04), **without** an observed spill or topology change. Its reading is
unresolved.

**q01 and q05 approximately double** on both engines. Their ratios are
similar at the two measured scales:
- q01 3.49× and 3.49×;
- q05 5.75× and 5.62×.

These are candidate readings. Two scales do not separate cost kinds
uniquely, and the per-stage figures come from one armed run per scale.


## Identity

| item | value |
|---|---|
| plan commit | `78ba632e`; `src` verified identical to ADR 616's closing commit `3a782d97` |
| executable | `5eadef2de5eccbae29431468319b186e5d22e602cfe0cb72d093bf3d306868a8`, byte-identical to ADR 616's treatment executable (same `src`, reproducible build); manifest `adr617.manifest` (`2e5c3188…`) |
| DuckDB | `c478c0f4…` (v1.5.4), 1 thread, default memory limit (no explicit limit requested; read back as 98.5 GiB on every timed row) |
| SF16 data | manifest `fnv1a64:32703e0082d64670`. Generated on bigfoot with ADR 613's executable; copied to strix; all nine files' SHA-256 and byte total identical on both hosts. The harness full-hashes against the manifest on every run (`dataset_verification: full_hash` on every row) |
| coordinate | strict parquet, task memory and spill reserve 2 GiB, partition parallelism 1, DuckDB threads 1, affinity 0-15. The timed unit's values are in `launch-timed.sh`'s `--setenv` lines. Each timed row reports its memory, spill reserve, parallelism, and requested and observed affinity, and the v2 reader checks them. The diagnostics' values are in `coordinate_env.txt` |
| run scripts | under `target/tpch/adr617-scripts/`: `adr617_timed.sh` (`c28b6737…`), `adr617_diag.sh` (`44c32b3f…`), `adr617_duckdb_scanwork.py` (`b7c774d4…`; first version kept as `-v0`, `8fce7525…`) |

**Readers, v1 and v2.** Codex's closing review found the v1 readers not
fail-closed. They gated identity and status but did not validate:
- rows, blocks, or the isolation log;
- the node tree, completeness, or stage correspondence;
- the receipts;
- the Toron row denominators;
- the preserved DuckDB profiles.

**All versions are kept.** The v2 and v3 readers read the same saved
artifacts. Every figure below is from v3, and v3 reproduces v2's.

| reader | v1 (script / read-out) | v2 (script / read-out) |
|---|---|---|
| timed | `adr617_timed_read.py` (`ace7e9c6…`) / `adr617-timed/readout.txt` (`5b3be90b…`) | `adr617_timed_read_v2.py` (`511b29c1…`) / `adr617-timed/readout-v2.txt` (`461fd312…`); gated identities in `gated_groups.json` (`f19b4e9c…`) |
| diagnostics | `adr617_diag_read.py` (`4235ed2d…`) / `adr617-diag/readout.txt` (`78d67da0…`) | `adr617_diag_read_v2.py` (`8da5da96…`) / `adr617-diag/readout-v2.txt` (`f87f6de6…`) |

**v3, the last validator refinements (Codex's reread).** No figure
changed:
- **Campaign-structure failures are fatal.** These are:
  - the declared order against the plan's schedule, regenerated
    independently in the reader;
  - the log against the order;
  - duplicates;
  - the completion line;
  - the group count.

  Any of these exits before publishing anything. Row failures stay
  scoped to their own groups.
- **The DuckDB memory coordinate** is proven present, from each block's
  artifact header: `duckdb_memory_limit` present and null, with
  provenance `default`. Fingerprints and read-backs must be non-empty.
- **The diagnostics reader checks correctness first.** A failing
  cell/scale publishes no timing-derived reading.

| reader | v3 script / read-out |
|---|---|
| timed | `adr617_timed_read_v3.py` (`c8a32c13…`) / `adr617-timed/readout-v3.txt` (`461fd312…`, byte-identical to v2's); `gated_groups-v3.json` (`f19b4e9c…`, identical to v2's) |
| diagnostics | `adr617_diag_read_v3.py` (`9a15fe7e…`) / `adr617-diag/readout-v3.txt` (`3dd9f891…`) |

The v3 diagnostics read-out has the same keys and the same parsed
values as v2's. Only line order and dictionary key order differ: v3
prints correctness first, and the per-table scan dictionary is built by
iterating a set, whose order changes with Python's per-process string
hashing.

v3's negative controls:
- a schedule off by one round exits (rc 3) with nothing published;
- a correctness failure forced on q21 SF16 suppresses q21's per-row
  ratio, its attributed run, and its stage comparison.

Neither control wrote any file.

**The v2 timed reader** requires:
- the 140 declared blocks, logged uniquely and in order;
- each block's log line to show rc 0, no leftover child, an empty
  scratch, and start before end;
- exactly one warm-up and one measured row per block, both passing,
  with matching query, engine, scale, and mode. Warm-up and measured
  rows must have the same digest and fingerprint;
- a finite non-negative duration;
- an integer RSS with the recognized label;
- the coordinate fields;
- Toron: the dataset fingerprint of its scale, and valid non-negative
  integer spill counters;
- DuckDB: a SHA-256 result digest, a present memory read-back, and no
  explicit limit;
- exactly 20 groups of seven rounds, each with a single non-null
  digest, fingerprint, and manifest (and read-back, for DuckDB). A
  group that fails publishes no ratio.

**It was tested by negative controls on strix,** each run from a
temporary copy with one expectation broken:
- a wrong RSS label withheld all 20 groups;
- a wrong SF16 dataset fingerprint withheld exactly the five Toron SF16
  groups.

The negative copies overwrote `gated_groups.json`. The real v2 reader
was then re-run, and its read-out was byte-identical to its first run.

**The v2 diagnostics reader** uses only gated timed groups, and adds:
- **A. Node-tree validity** (ADR 614/615): in-tree residual 0, no stage
  or bridge mismatch, no interleaved frames, and attributed exclusive
  time equal to the bound span. It names **strict** (`passed`) and
  **top-level** (`status: reconciled`, `arithmetic_reconciled`,
  `within_residual_boundary`) completeness separately. Stage ratios are
  published only between top-level-reconciled invocations with the same
  route and the same stage, replay, and ordinal topology.
- **B. The q05 receipts:** exactly replays 1 to 4, all reused, `Bypass`,
  the named sketch passed, and a valid estimate and cap.
- **C. Toron visited rows:** ADR 605's fail-closed fold of the progress
  stream, reconciled with each row's `parquet_scan_receipt` on batches,
  rows, and raw bytes.
- **D. DuckDB:** the sweep's digest and read-back, and the direct
  profiles re-verified from their preserved files:
  - the recomputed SQL fingerprint;
  - the read-back parsed from the saved stdout;
  - the result digest recomputed from the saved stdout;
  - the executable hash;
  - every scan's counter, and its table.
- **E. Correctness.**

**The launch.** The timed campaign ran as a transient systemd unit,
starting 120 s after its SSH session closed.
- `declared_order.txt` (`0a69b43d…`) lists all 140 blocks, and
  `campaign.log` (`fd72faab…`) records every one.
- Every block exited 0. After each, no query child remained and the
  spill scratch was empty. The driver's stderr is empty.
- The diagnostics then ran 43 blocks with the same enforcement, all
  exiting 0.

**A departure from the launch rule, disclosed.** After the launch, I
copied a helper script to strix by `scp`. It finished at
1790766297.8 s. The first block started at 1790766314.9 s, so the copy
fell inside the 120 s settle window, **17.1 s before measurement**. No
sample overlaps it, and none is flagged or excluded. It is recorded as a
launch-settle departure.


## The timed campaign

Each block is one `perf --runs 1 --warmup 1` of one cell, engine, and
scale. It goes through `toron-bench`'s parent path, into a fresh output
directory. So each measured sample follows its own untimed execution of
the same query: a best-effort warm-cache comparison, not a guarantee of
equal cache state.

The order was declared before launch:
- seven rounds;
- cell order rotated by round;
- scale order alternating by round;
- engine order Toron-first in rounds 1, 2, 5, and 6.

**All 20 groups passed v2's validation.**

Median of seven, with the range (all samples retained):

| cell | engine | SF8 ms | SF16 ms | SF16/SF8 |
|---|---|---|---|---|
| q01 | Toron | 3,329.5 (3,322.3–3,356.4) | 6,659.4 (6,646.3–6,998.2) | 2.000 |
| q01 | DuckDB | 955.0 (953–970) | 1,910.0 (1,907–1,928) | 2.000 |
| q05 | Toron | 3,406.5 (3,398.2–3,453.1) | 6,894.4 (6,877.8–6,919.4) | 2.024 |
| q05 | DuckDB | 592.0 (590–596) | 1,226.0 (1,217–1,232) | 2.071 |
| q09 | Toron | 6,727.6 (6,707.5–6,774.1) | 15,087.7 (15,064.2–15,184.2) | 2.243 |
| q09 | DuckDB | 1,638.0 (1,631–1,645) | 3,343.0 (3,328–3,362) | 2.041 |
| q18 | Toron | 5,991.3 (5,987.4–6,033.7) | 14,348.1 (14,295.7–14,376.1) | 2.395 |
| q18 | DuckDB | 1,486.0 (1,480–1,502) | 3,135.0 (3,121–3,150) | 2.110 |
| q21 | Toron | 8,225.1 (7,963.5–8,329.4) | 25,938.6 (24,782.7–26,175.6) | 3.154 |
| q21 | DuckDB | 2,202.0 (2,190–2,214) | 4,765.0 (4,748–4,774) | 2.164 |

**Spread.** q01's SF16 Toron range reaches 6,998.2, and q21's ranges
are the widest of the Toron groups (366 ms at SF8, 1,393 ms at SF16).
All samples are kept.

**The gap, Toron/DuckDB by medians:**

| cell | SF8 | SF16 |
|---|---|---|
| q01 | 3.486× | 3.487× |
| q05 | 5.754× | 5.623× |
| q09 | 4.107× | 4.513× |
| q18 | 4.032× | 4.577× |
| q21 | 3.735× | 5.444× |

**Toron spill and peak RSS** (every row labelled
`above_parent_high_water`):

| cell | spill SF8 → SF16 | peak RSS KiB SF8 → SF16 |
|---|---|---|
| q01 | none observed → none observed | 32,324–34,428 → 37,180–38,704 |
| q05 | none → none | 6,475,232–6,476,976 → 12,958,624–12,960,060 |
| q09 | none → none | 2,238,356–2,239,824 → 4,457,316–4,458,688 |
| q18 | none → **420,825,376 B** | 4,048,752–4,050,016 → 8,073,068–8,075,108 |
| q21 | 27,242,808 B → **1,846,534,680 B** | 1,023,532–1,028,940 → 1,587,996–1,610,256 |

**DuckDB's peak RSS** (same label), SF8 → SF16:

| cell | KiB |
|---|---|
| q01 | 165,056–165,672 → 297,120–297,720 |
| q05 | 168,528–168,968 → 295,240–297,824 |
| q09 | 1,152,624–1,156,432 → 2,261,864–2,262,868 |
| q18 | 992,604–1,092,300 → 1,761,020–1,761,880 |
| q21 | 417,848–419,172 → 802,024–803,808 |

**Plan fingerprints.** Every Toron cell's fingerprint differs between
the scales. The fingerprint hashes the plan's debug text, minus routing
hints (`physical_plan_fingerprint`), so a scale-dependent value inside
the plan changes it too. Below, changes are read from the
**execution-stage topology** that attribution records. That is not a
comparison of the bound physical plans.


## The diagnostics

**Every gate passed except one scan-work profile:**
- the attributed runs (10): each valid against its gated timed group,
  and each node tree reconciling;
- the q05 receipt preflights (2): each fully validated;
- Toron's progress folds (10): each reconciling with its row's ledger
  receipt;
- the harness's DuckDB sweeps (10): digest and read-back equal to the
  timed rows';
- the direct scan-work profiles: nine re-verified from their preserved
  files. **q01 at SF16 is withheld:** its profile identity is
  unestablished, because the result digest recomputed from its saved
  output differs from the timed digest. Serialization differences are a
  possible explanation. No correctness inference is drawn either way;
- the harness's `correctness` command: **pass on all ten cell-scale
  combinations.**

**Each armed run's `execute_ms` against its timed median** is context:
one armed run, with overhead and drift not apportioned. The stage
observations below come from these runs.

| cell | SF8, diag minus timed ms | SF16 |
|---|---|---|
| q01 | +138.1 | +649.0 (7,308 against 6,659) |
| q05 | +315.5 | +697.8 (7,592 against 6,894) |
| q09 | +504.9 | +800.9 |
| q18 | +806.0 | +1,985.0 (16,333 against 14,348) |
| q21 | +321.0 | +1,266.4 |

**Rows visited, per engine, with each engine's own counter:**
- Toron: raw rows from ADR 605's fold, reconciled with the ledger
  receipt.
- DuckDB: `OPERATOR_ROWS_SCANNED`, summed over every scan. Every scan
  carried the counter and resolved to a TPC-H table.

| cell | Toron SF8 → SF16 (scans per table) | DuckDB SF8 → SF16 (scans per table) |
|---|---|---|
| q01 | 47,989,007 → 95,988,640 (`lineitem` 1) | 47,989,007 → withheld at SF16 |
| q05 | 61,269,037 → 122,548,670 (6 tables, 1 each) | 61,269,037 → 122,548,670 (the same) |
| q09 | 69,669,032 → 139,348,665 (`part` 2, others 1) | 68,069,032 → 136,148,665 (all 1) |
| q18 | 158,367,021 → 316,765,920 (`lineitem` 3, `customer` 2, `orders` 1) | 109,178,014 → 218,377,280 (`lineitem` 2, `orders` 1, `customer` 1) |
| q21 | 180,047,096 → 360,125,995 (`lineitem` 3, `orders` 3, `nation` 3, `supplier` 1) | 156,047,046 → 312,125,945 (`lineitem` 3, others 1) |

**Each engine's time per visited row, SF16 over SF8,** using its own
counter and the timed medians:

| cell | Toron | DuckDB |
|---|---|---|
| q01 | 1.000 | withheld |
| q05 | 1.012 | 1.035 |
| q09 | 1.121 | 1.020 |
| q18 | 1.197 | 1.055 |
| q21 | 1.577 | 1.082 |

The counters are operational, not identical work (ADR 605), so no
cross-engine throughput is inferred from their absolute values.

**Stage attribution, one armed run per scale.** Single-run ratios carry
run-to-run noise: q01's `fold` reads ×2.15 armed, where the timed query
is ×2.000.

- **q01** (`scan_grouped`): top-level reconciled and strict-complete at
  both scales, the same topology, so the stages are paired.
  `stream_pull` ×2.01, `projection` ×1.98, `fold` ×2.15.
- **q05** (`reoriented_inner_chain_grouped`): paired, since it is
  top-level reconciled with the same topology at both scales. It is
  **strict-incomplete at both scales:** replay 2's `suffix_probe` lacks
  its nested `suffix_lookup`, `suffix_match_expand`, and
  `suffix_payload_take`, as in ADR 614.
  - The dominant `runtime_prep` goes 2,421.9 → 4,841.5 ms (×2.00).
  - Most stages fall between ×1.9 and ×2.2. The exceptions are small
    stages: `base_scan_filter` 38.2 → 64.6 ms (×1.69) and
    `suffix_probe` replay 3 19.1 → 49.0 ms (×2.57).
  - **The receipts:** the replay-1 build estimate goes 5,758,680,840 →
    11,518,636,800 bytes, against a 34,359,738,368-byte cap. The path
    that estimate implies is `in_memory` at both scales. That is
    estimate-implied, not a proof of the path taken. Every replay is
    reused under `Bypass`, with the sketch passing.
- **q09** (same route): paired; strict-incomplete in the same way as
  q05; no spill.
  - `runtime_prep` ×2.18, `scan_pull` ×2.04, `repartition` ×2.00,
    `fold` ×2.01, `cleanup` ×2.07;
  - `base_probe` ×2.36, `base_materialize` ×2.49;
  - `suffix_probe` replays 1 to 4 at ×2.31, ×2.36, ×2.17, and ×2.14.
  - The largest stages are `runtime_prep` (1,532.5 → 3,336.1 ms) and
    `suffix_probe` replay 2 (1,273.2 → 3,001.8 ms).
- **q18**: neither invocation is top-level reconciled at either scale.
  `scan_grouped` has no stage attribution, and `uncovered_generic_fold`
  has no handler span. So only node-kind totals are read. **Each is a
  bucket over several calls, not one identified operator:**

  | node kind | calls | SF8 ms | SF16 ms | ratio | spill |
  |---|---|---|---|---|---|
  | join | 3 | 961.7 | 4,141.2 | ×4.31 | all 420,825,376 B |
  | aggregate | 2 | 3,910.0 | 8,263.9 | ×2.11 | none |
  | exchange | 8 | 1,234.2 | 2,538.4 | ×2.06 | none |
  | scan | 3 | 544.6 | 1,102.2 | ×2.02 | none |
  | filter | 1 | 146.6 | 287.2 | ×1.96 | none |

  Which of the three join calls owns the time and the spill is not
  identified.
- **q21** (`inner_prefix_semi_anti_grouped` at both scales, top-level
  reconciled and strict-complete at both). **The execution-stage
  topology changes**, so its stages are shown separately, not paired:
  - **SF8:** `runtime_prep` 4,027.2 ms (holding the whole 27 MB
    spill), `base_scan` 625.6, `inner_replay_0` 1,134.9,
    `inner_replay_1` 2,150.9, `inner_replay_2` 273.8, semi 101.0, anti
    79.3, and fold 3.4.
  - **SF16:** `runtime_prep` 6,991.0 ms (536 B of spill), `base_scan`
    1,236.1, `inner_replay_0` 2,594.7, then **`deferred_replay`,
    16,272.4 ms, holding 1,846,534,144 B of spill.** Inside it:
    - `reservoir_buffer` 6,677.4 ms;
    - `reservoir_finish` 1,987.2 ms;
    - `inner_replay_1` 6,639.3 ms;
    - `inner_replay_2` 489.0 ms;
    - semi 191.3, anti 165.3, and fold 16.5 ms.


## Reading (candidates)

- **q21: an observed transition.** Three observations support it:
  - the execution-stage topology changes (a deferred replay through a
    reservoir);
  - the spill moves from `runtime_prep` into that stage and grows
    ×67.8;
  - Toron's time per visited row rises ×1.58, against DuckDB's ×1.08.

  At SF16, the reservoir's buffer and finish take 8.7 s. The
  `inner_replay_1` inside the deferred stage takes 6.6 s, against 2.2 s
  at SF8. What triggers the deferral is not established here, and
  neither is whether a larger task memory avoids it.
- **q18: a spill transition in the join bucket.** The three join calls
  together grow ×4.31 and carry all the new spill. The other node kinds
  stay near ×2. The owning join is not yet identified.
- **q09: its gap widens without an observed transition.** No spill, and
  the same topology. Probe and materialize stages grow ×2.3 to ×2.5 in
  single armed runs. Unresolved.
- **q01 and q05: approximately doubled.** Their time per visited row is
  flat on Toron (1.000 and 1.012), and their gaps are similar at the two
  scales. That is consistent with input-dependent cost. It does not
  choose a remedy: filter placement, join direction, avoided scans or
  copies, and other changes could each reduce it.
- **Memory.** Toron's q05 peak RSS doubles to 13.0 GB. DuckDB's q05
  stays under 0.3 GB at SF16. q05's build is all of `lineitem` (ADR
  615), and its estimate doubles with it.


## Consequence

- **No cut is proposed here.** Two observed transitions are named as
  candidate owners for successor records, each needing its own plan:
  1. **q21's deferred replay** and what triggers it. At SF8, q21's
     smaller spill sits in `runtime_prep` instead, so the two are
     separate observations.
  2. **q18's join spill,** starting with which of the three join calls
     owns it.
- **A proportional-memory panel** would test how sensitive each cell is
  to the budget, and which transitions persist when memory scales with
  the data. It does not by itself separate budget effects from
  algorithmic scaling. It is its own record.
- **SF32** stays a separate, reviewed follow-up.
- The SF8 campaign target and its sealed board are unchanged.
