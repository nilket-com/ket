# ADR 622 - SF8 board re-seal after the build-side cuts: results

Run 2026-09-30 on strix: one step, both engines, all 22 cells, 5 runs +
1 warmup, at the ADR 603, 608 and 613 coordinate. **Sealed** with
`candidate_authority` and `final_authority` true.

**Toron 58.672 s against DuckDB 15.635 s: 3.753×.** ADR 613's board had
61.387 s, 15.579 s and 3.940×; ADR 603's anchor had 63.430 s, 15.650 s
and 4.053×. The target arithmetic, named twice:
- **Against this board's DuckDB total:** 2 × 15.635 s is 31.270 s.
  Toron is **27.402 s** above it.
- **Against the fixed original threshold** (31.366 s, 2× ADR 591's
  DuckDB sum): Toron is **27.306 s** above it.

Every cell's result identity equals ADR 613's: Toron's `digest` and
DuckDB's `duckdb_result_sha256`, each a single value across all of that
cell's samples, warmups included. All 44 cells passed and
are stable; the largest `(max − min) / median` is 0.097, against the
0.20 bound.

Toron's total fell 2,714.9 ms from ADR 613, and DuckDB's rose 56.0 ms.
The record apportions none of Toron's change to ADR 616, ADR 621, or
anything else. The paired evidence for the changes since ADR 613 is:
- **q05:** ADR 616, −1,340.7 ms;
- **q09:** ADR 616 breadth, a 260.0 ms gain; ADR 621, a 1,053.4 ms
  gain;
- **q01:** ADR 621's no-change control, reported only.


## Identity

| identity | value |
|---|---|
| measured tree | `f98a142d6c586a295f1d8ea4a0459afdaaed423c`, commit `ac32c48e` (this plan's commit). Its code equals `artifact/adr621-treatment` (`f0b63449`); the trees differ only in `plans/` |
| executable | `18df5747a7dc247cc0caeb5db2e84cd05440d2375f1254697f29c35246cd7ee5`. The bigfoot build reproduced ADR 621 treatment's bytes. Manifest `target/tpch/evidence/build/adr622-measured.manifest` (`0431d89871b6e053d26e2ca0e58138da7f445f9650aaf03f5c54af9a4a18b12f`) |
| DuckDB | `c478c0f4…`, v1.5.4, one thread; `duckdb_memory_limit` present and null, with provenance `default` |
| ladder (bigfoot, measured tree, `--no-fail-fast`) | fmt clean; toron 46 suites, 5,533 passed, 0 failed, 5 ignored; toron-tpch 674 passed, 0 failed, 6 ignored; release build 0 warnings |
| artifact / sealed manifest | `54e99bdf3f63608630fbdfd7b7bca1c8d0144ed4a025ede41e5d38e28753c0e9` / `78c631f92c87f83c71b7b77d41085cecb49f9c24160ae4a2b0136f1f75e3e3fa` (`target/tpch/adr622-board/perf_board.jsonl.evidence.sealed`) |
| evidence tag | `evidence/f98a142d6c58-54e99bdf3f63`, targeting tree `f98a142d…`. Fetched to the workstation and pushed with the closing commit |
| archived executables | `target/tpch/evidence/archive/18df5747…` (Toron and harness) and `…/c478c0f4…` (DuckDB) |
| build host / run host | bigfoot / strix |

**Required identity, checked on every row.** Every one of the 264
sample rows passed, with `full_hash` verification and the measured
identity: executable `18df5747…`, DuckDB `c478c0f4…`, source tree
`f98a142d…`, commit `ac32c48e`, and no authority issues or untracked
build inputs.

Dataset fingerprints:
- Toron's rows and the header carry the SF8 dataset fingerprint,
  `fnv1a64:0fc5c06d19c5ed23`.
- DuckDB's rows carry a per-query fingerprint. Each equals ADR 613's
  for the same query.

**Result identity, from the raw samples.** Each cell has one nonempty
identity across all six of its samples, warmups included, and it equals
ADR 613's for the same query and engine:
- Toron: the `digest`, which also equals the cell's summary digest;
- DuckDB: `duckdb_result_sha256`, 64 hex. DuckDB's summary `digest` is
  the string `"none"` and is not used.

## Procedure

ADR 613's procedure, with this record's paths and the runner
safeguards the plan requires. Before the launch, strix's checkout was
verified at HEAD `ac32c48e`, tree `f98a142d…`, clean. The manifest
(`0431d898…`) and the staged executable's full hash (`18df5747…`) were
also verified, and these readings were kept in the host record
(`staging_verification.txt`).

The launch:
- Token `adr622-9d79caf50ff81c9c`: launched at unix ms 1790818466577,
  with the unit 120 s later.
- Before: load 0.00, 39 °C, scratch empty, output fresh, 0 leftover
  processes.
- Completion at unix ms 1790819426223 (`result=success
  exit_code=exited exit_status=0`).
- After, by the read-out following the datagram: load 0.85, 50 °C,
  scratch 0 entries, 0 leftover processes.
- There was no SSH to strix between the launch and the datagram.

The timed child's recorded environment (`launch_env.txt`) holds only
the coordinate:
- affinity `0-15`;
- memory and spill reserve 2 GiB each;
- partition parallelism 1, DuckDB threads 1;
- the join-spill directory;
- the ADR 595 token and launch time.

No diagnostic, receipt, ablation or skip-hash variable was present.

The header records:
- the host quiescent at start and end;
- the spill scratch empty before and after;
- the output fresh;
- affinity `0-15` requested and observed;
- diagnostics disabled.

264 samples and 44 summaries.

## The board

| query | Toron ms | DuckDB ms | ratio | gap ms | Toron vs 613 | DuckDB vs 613 | Toron vs 603 | Toron peak RSS KiB (label; parent at spawn) | spill bytes |
|---|---|---|---|---|---|---|---|---|---|
| q01 | 3,324.7 | 961.0 | 3.46 | 2,364 | −27.4 | +4.0 | −1,332.7 | 33,716 (above; 11,952) | 0 |
| q02 | 1,454.3 | 129.0 | 11.27 | 1,325 | +2.1 | +1.0 | −29.9 | 995,380 (above; 20,380) | 0 |
| q03 | 1,882.4 | 654.0 | 2.88 | 1,228 | +3.5 | +1.0 | +1.9 | 3,456,512 (above; 24,324) | 0 |
| q04 | 2,781.7 | 455.0 | 6.11 | 2,327 | +11.8 | −1.0 | +0.2 | 1,236,144 (above; 127,168) | 0 |
| q05 | 3,425.6 | 591.0 | 5.80 | 2,835 | −1,326.4 | +0.0 | −1,860.1 | 6,477,036 (above; 127,168) | 0 |
| q06 | 681.6 | 364.0 | 1.87 | 318 | −2.4 | +1.0 | −5.9 | not stated (ambiguous; 127,168) | 0 |
| q07 | 3,686.1 | 595.0 | 6.20 | 3,091 | −200.8 | +4.0 | −254.5 | 1,296,456 (above; 127,168) | 0 |
| q08 | 3,220.3 | 645.0 | 4.99 | 2,575 | +5.9 | +2.0 | −40.5 | 656,544 (above; 127,168) | 0 |
| q09 | 5,696.9 | 1,640.0 | 3.47 | 4,057 | −1,281.5 | +11.0 | −1,417.1 | 2,057,776 (above; 127,168) | 0 |
| q10 | 2,342.2 | 1,127.0 | 2.08 | 1,215 | −37.4 | +3.0 | −30.0 | 1,063,988 (above; 127,168 and 127,424) | 0 |
| q11 | 893.0 | 107.0 | 8.35 | 786 | −3.9 | +0.0 | −12.8 | 855,772 (above; 669,736) | 0 |
| q12 | 2,070.5 | 511.0 | 4.05 | 1,560 | −0.2 | +2.0 | +7.0 | 1,841,680 (above; 669,736) | 0 |
| q13 | 2,628.0 | 1,192.0 | 2.20 | 1,436 | −10.1 | +2.0 | +137.5 | not stated (ambiguous; 669,736) | 0 |
| q14 | 651.0 | 404.0 | 1.61 | 247 | +13.4 | +3.0 | +15.0 | not stated (ambiguous; 669,736) | 0 |
| q15 | 1,289.4 | 313.0 | 4.12 | 976 | +7.7 | +2.0 | +55.1 | not stated (ambiguous; 669,736) | 0 |
| q16 | 1,407.5 | 191.0 | 7.37 | 1,217 | +38.3 | +2.0 | +66.2 | not stated (ambiguous; 669,736) | 0 |
| q17 | 3,126.3 | 629.0 | 4.97 | 2,497 | +25.6 | +4.0 | +26.9 | 3,730,080 (above; 669,736) | 0 |
| q18 | 6,022.0 | 1,489.0 | 4.04 | 4,533 | +45.3 | +10.0 | −16.7 | 4,051,116 (above; 669,736) | 0 |
| q19 | 1,758.1 | 677.0 | 2.60 | 1,081 | −2.9 | +1.0 | −10.7 | not stated (ambiguous; 669,736) | 0 |
| q20 | 1,678.9 | 516.0 | 3.25 | 1,163 | −6.7 | −1.0 | +1.8 | 2,241,404 (above; 669,736) | 0 |
| q21 | 7,999.1 | 2,210.0 | 3.62 | 5,789 | +26.4 | +5.0 | −57.7 | 1,022,132 (above; 669,736) | 27,242,808 |
| q22 | 652.1 | 235.0 | 2.78 | 417 | +4.7 | +0.0 | −1.6 | not stated (ambiguous; 669,736) | 0 |
| **total** | **58,672** | **15,635** | **3.753** | **43,037** | **−2,714.9** | **+56.0** | **−4,758.3** | | |

**RSS.** The same seven cells are `ambiguous` as on ADR 613: q06, q13,
q14, q15, q16, q19, and q22. They are not stated as peaks.
**Spill.** Only q21 spills, 27,242,808 bytes, as on ADR 613.

**Changes from ADR 613, observed and unattributed:**
- **q05 (−1,326.4 ms)** and **q09 (−1,281.5 ms)** are the largest. They
  are the two cells with paired gains since ADR 613. Those paired
  figures are the token evidence; the board changes are reported beside
  them, not as them.
- **q07 (−200.8 ms)** is the next largest. ADR 616's armed runs showed
  `prep_exchange` absent from q07 under the bypass. q07 was not paired,
  and its change is observed with no causal claim. It is a candidate
  for a paired check if attribution is needed.
- **q20 (−6.7 ms)** was named changed but unpaired by ADR 621's gate.
  Its change is observed only.
- The rest are within ±46 ms. The larger ones are q18 (+45.3), q16
  (+38.3), q10 (−37.4), q01 (−27.4), q21 (+26.4) and q17 (+25.6). These
  are observed only.
- Toron's peak RSS fell on q05 (6,477,036 KiB against ADR 613's
  9,167,776), q09 (2,057,776 against 2,456,620) and q07 (1,296,456
  against 1,600,868). This too is observed and unattributed.
- **DuckDB's change** (+56.0 ms in total; per cell from −1.0 to +11.0,
  the largest on q09 and q18) is reported as observed cross-board
  change, for context only. It is not an identified measurement of host
  drift.

## Gap ranking

Cells ranked by absolute gap (Toron − DuckDB median), with each cell's
Toron median:

| rank | this board | gap ms | Toron ms | ADR 613 | gap ms |
|---|---|---|---|---|---|
| 1 | q21 | 5,789 | 7,999.1 | q21 | 5,768 |
| 2 | q18 | 4,533 | 6,022.0 | q09 | 5,349 |
| 3 | q09 | 4,057 | 5,696.9 | q18 | 4,498 |
| 4 | q07 | 3,091 | 3,686.1 | q05 | 4,161 |
| 5 | q05 | 2,835 | 3,425.6 | q07 | 3,296 |
| 6 | q08 | 2,575 | 3,220.3 | q08 | 2,571 |
| 7 | q17 | 2,497 | 3,126.3 | q17 | 2,476 |
| 8 | q01 | 2,364 | 3,324.7 | q01 | 2,395 |
| 9 | q04 | 2,327 | 2,781.7 | q04 | 2,314 |
| 10 | q12 | 1,560 | 2,070.5 | q12 | 1,562 |

How the order moved from ADR 613:
- q09 moves from 2nd to 3rd, and q18 from 3rd to 2nd.
- q05 moves from 4th to 5th, and q07 from 5th to 4th.
- The rest of the top ten keep their places.
- The four largest gaps, q21, q18, q09 and q07, together are 17,470 ms
  of the 43,037 ms total gap.

## Provenance

All sources are on strix under `~/work/ket/nilket/target/tpch/`. The
figures above were read by `adr622-scripts/adr622_board_read.py` (the
final reader). Its offline fixtures, including negative controls, are
`adr622_board_read_test.py`. They passed locally and on strix.

**Reader corrections.**
- **Dataset fingerprints.** The first version required Toron's dataset
  fingerprint on DuckDB rows and refused every DuckDB cell. A second
  hardcoded one DuckDB value, but DuckDB's fingerprint is per query, so
  it refused 21 cells. Both withheld every figure. The version kept as
  `adr622_board_read-v0.py` binds each cell's fingerprint to ADR 613's.
- **Digests.** v0's digest comparison read the summary `digest`. For
  DuckDB that is the string `"none"` on both boards, so its DuckDB
  "equal to ADR 613" was vacuous. v0 also never checked the result
  identities in the raw samples. The final reader takes each cell's
  identity from its raw samples, warmups included, and requires:
  - one nonempty value per cell;
  - for DuckDB, a 64-hex `duckdb_result_sha256`;
  - for Toron, agreement with the summary;
  - equality with ADR 613's for the same query and engine.

  Its negative fixtures cover:
  - a missing, null or `"none"` DuckDB hash;
  - one changed sample or warmup;
  - a whole cell changed consistently, on either board;
  - a Toron summary that disagrees with its samples.

  Rerun read-only on the same artifacts, its figures are identical to
  v0's (`board_readout_v0.txt` and `board_readout_v1.txt` differ only
  in the dropped digest column). No total changed.

| file | sha256 |
|---|---|
| `adr622-scripts/adr622_board_read.py` (final) | `f2d7ec8f8506e05170940d15285f0f83e779bcd60942d392a751db1240f38456` |
| `adr622-scripts/adr622_board_read_test.py` | `afefaeef075c83b6f844faf1c1e1aa330a5743111d9f2f98f71814f07b1ef8bf` |
| `adr622-scripts/adr622_board_read-v0.py` (preserved) | `5089357c510e9377c143b6607665cc114fcd317e27e8bf4f1b87def6c15bc797` |
| `adr622-strix-host-record/board_readout_v1.txt` (final read-out) | `a45ffbfe247340d4da27fe2397a9ebbe65608a6902c6d68e1df54b529481aa76` |
| `adr622-strix-host-record/board_readout_v0.txt` (v0, same artifacts) | `a11aa60e9232dcccb9ea4afceca7f9e49131e585578e11290088ab310843a8b4` |
| `adr622-board/perf_board.jsonl` (artifact) | `54e99bdf3f63608630fbdfd7b7bca1c8d0144ed4a025ede41e5d38e28753c0e9` |
| `adr622-board/perf_board.jsonl.evidence.sealed` | `78c631f92c87f83c71b7b77d41085cecb49f9c24160ae4a2b0136f1f75e3e3fa` |
| `adr622-board/samples.jsonl` | `0c2734e39cd05f2868a2a2aecc30d79e07ae9ff3c106c330a6e1dd31f837e562` |
| `adr622-strix-host-record/staging_verification.txt` | `22b4d1e5a14a8a3a191a1d4ed68fe712a4f7a5c3954f7062a3d201c21d37c14f` |
| `adr622-strix-host-record/board/host_config_before.txt` | `a3793d63f6bf71040ff55dd011476b7ff70347df1e562f4bc1853649ad4feadf` |
| `adr622-strix-host-record/board/host_after.txt` | `b09b828902d4eb7171b66cc445f00c36bf274047b03e8bae7b6082fb33739e5f` |
| `adr622-strix-host-record/board/launch_env.txt` | `df04b87fb9dbc7a2327c4e54c2586b90886edf16ed028ac4fd480cb2361ada19` |
| host scripts `launch.sh` / `readout.sh` / `snapshot.sh` / `env_run.sh` | `7f63fc65…` / `e5b12079…` / `d3320d30…` / `457a7a41…` |

The comparisons read ADR 613's board (`adr613-board/perf_board.jsonl`,
`2f029448…`, and its `samples.jsonl` for the per-cell fingerprints) and
ADR 603's anchor (`adr603-anchor/perf_board.jsonl`). The seal's output
is kept in `adr622-strix-host-record/seal_output.json`.

## Consequence

- This board is the current-tree baseline. Its sealed manifest,
  `target/tpch/adr622-board/perf_board.jsonl.evidence.sealed`, is what
  the next pairs bind. ADR 603 remains the historical anchor.
- The campaign stands at **3.753×**, with Toron 27.4 s above 2× this
  board's DuckDB.
- By absolute gap, the next owners are **q21** (5,789 ms), **q18**
  (4,533), **q09** (4,057) and **q07** (3,091). q21 is the only
  spilling cell. q18's stage breakdown is already queued. The targeted
  SF64 q05 probe also remains queued. The next owner is chosen from
  this ranking.
