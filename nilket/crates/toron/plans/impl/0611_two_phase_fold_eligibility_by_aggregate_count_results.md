# ADR 611 - two-phase fold eligibility by aggregate count: results

Run 2026-09-30 on strix, at the ADR 608 coordinate, bound to ADR 608's
sealed board. The sequence was:
1. the all-cell gate, which refused on the plan's table;
2. the table amendment and a second read-only validation, which passed;
3. the q13 decision pair;
4. the q01 non-regression pair;
5. the q13 diagnostics.

**Not established. The token reverts to `AllTyped`.**

With `AtLeastTwoTyped`, q13's folds run row-at-a-time, and q13 is
**1,042.1 ms slower**: 3,673.4 ms against control's two-phase 2,642.8
ms. All five pairs are negative (−1,006.1 to −1,057.7 ms), against a
bar of 59.6 ms. Peak RSS is about 13 MB higher (274,932–275,816 against
262,312–262,736 KiB).

**The q01 non-regression pair shows no regression.** It is coverage of a
rejected arm, not a reason to retain it. The median gain is 10.2 ms
against a bar of 94.5 ms, with pairs of mixed sign and both arms
two-phase at about 3,330 ms.

**The finding that matters is not the rule.** The treatment's q13 row
loop is the same source path that ADR 610's control ran for q13, with
`row_lookup_update` at 1,532,647 µs and a median of 2,507.0 ms. In this
record's treatment build, that path takes 2,674,129 µs. The builds
differ in code and plumbing as well. A salient difference is the live
branch: in ADR 610's control, the `const RowAtATime` made the two-phase
branch dead code; here both branches are live, and the choice is made
per batch. Other build effects remain open. The cause of the roughly 1.1 s and 13 MB
difference is **unresolved**. The candidates are:
- codegen, inlining, or layout of the row loop;
- the per-batch template and eligibility work (which appears too small
  by count);
- changed allocation or code paths.

**ADR 610's reading of its own q13 regression**, a second scattered
pass, is reopened as a candidate, not an established mechanism. ADR 612
diagnoses it before any further cut.

Main keeps ADR 610's behaviour. `TWO_PHASE_ELIGIBILITY = AllTyped` makes
the rule's branch inert, and the eligibility reasons in the receipt
remain as diagnostics.


## Identity

| identity | value |
|---|---|
| control | commit `1dddc7fe`, tree `7ad8356e4191a274e4b71ec85b93beb84e0ed1a8`, tag `artifact/adr611-control`; executable `9a84d5c415ce90c738b6da471067a2ae0ca36b5b5c465857bd9bf9eafc17cbf2`; manifest `adr611-control.manifest` (`5e9d5ecf…`); `TWO_PHASE_ELIGIBILITY = AllTyped` |
| treatment | commit `18372d0c`, tree `884255bad69420604d6f62d2420ea8f229de0f9b`, tag `artifact/adr611-treatment`; executable `2fad72b7cade1b047ee0c4ed97db49ab6cc46682c30ef76eb680e2bf7dd7dbe2`; manifest `adr611-treatment.manifest` (`fc4ca709…`); `AtLeastTwoTyped` |
| difference | one line in `exec/aggregate/fold_kernel.rs` (rustfmt wraps the `const`; the changed line is the variant) |
| sealed baseline | `target/tpch/adr608-board/perf_board.jsonl.evidence.sealed`; the artifact the gate validation read hashes to `7662f6f2…`, ADR 608's sealed artifact |
| ladders (bigfoot, both trees, fmt clean, `--no-fail-fast`) | toron 5,513 passed, 0 failed, 3 ignored (2 new ADR 611 fixtures); toron-tpch 674 passed, 0 failed, 6 ignored; release builds warning-free under both token settings |
| build host / run host | bigfoot / strix |

**Before the reported ladder,** three fixes were made. They are
recorded because they shaped the arms:
- a first ladder run tested the wrong tree, because the branch had not
  been pushed to bigfoot and a `sed` missed the wrapped `const`. It was
  discarded;
- a test comment tripped the `adr350` query-number guard, and was
  reworded;
- two ADR 610 fixtures fold one-aggregate lists. Under `AtLeastTwoTyped`
  they no longer exercised two-phase, so their helpers now call the
  configured fold with `TwoPhaseEligibility::AllTyped` explicitly. They
  test ADR 610's mechanics. The same test code is in both arms.

## The gate

One run of each of the 22 queries per arm, with the kernel receipt, one
file per arm and query (`adr611-strix-host-record/gate`).

**Validation 1 refused.** The data matched the rule on every instance.
But the plan's table listed q16 only by its zero-aggregate instance, and
omitted q16's second instance: one `COUNT(x)` over 947,557 rows, which
falls back under the rule. The plan says any difference fails the gate.

**The amendment** corrected the table to name both q16 instances. It was
reviewed, and it rides in this closing commit. The amended plan's
SHA-256 is `bf2207dc949be37f9a4b72abda1d8c476d553a147dde77bc6863eab8a53360c3`.

**Validation 2 passed.** It was a fresh, read-only validation of the
same saved artifacts, with no query re-run
(`adr611_gate_validate_v2.py`, SHA-256 `3b997a4f…`). It checks an
explicit per-cell, per-instance table: the aggregate count, the
treatment's reason, and its shape. It also checks:
- the control's reason;
- each arm's rule;
- equal rows between the arms;
- digest, fingerprint, and status on all 44 rows;
- q01's exact kernels.

It hashed all 82 artifacts (44 rows and 38 receipts; q06, q14, and q19
write none). Its verdict file, `gate/verdict-validation-2.json`
(SHA-256 `a3efd755d0d6c53dca8adb32d43e4e75d4ff270e7bb3fa68c2f04a4df709e8aa`),
records the amendment's hash, the validator's hash, and the artifact
hashes. It notes that it supersedes validation 1's refusal. Validation
1's refusal stands in the record.

The observed shapes:
- **Two-phase** (`eligible`): q01 (8 aggregates), q08, q12, and q22
  (2 each).
- **q02:** `untyped_call_list` under both rules.
- **Every other grouped instance:** row-at-a-time,
  `below_aggregate_minimum`. That includes both of q13's, both of
  q15's, and both of q16's.
- **q01:** wholly two-phase, with ADR 610's exact eight kernels.

## The q13 pair (decides the token)

Token `adr611q13-d704f3a0ffe4`, launched 01:31:10 CDT.
- **Timing:** the first child ran at 01:33:18, and completion arrived
  at 01:34:27 (`result=success exit_code=exited exit_status=0`).
- **Separation and window:** separation verified (margin 127.8 s); the
  window verified.
- **Start gate:** load 0.00 and 37 °C.
- **Control variability:** ratio 0.011.
- **Distance from the anchor:** control's median is 150.3 ms above ADR
  608's sealed q13 cell (control runs q13 two-phase).

| pair | order | control ms | treatment ms | gain ms |
|---|---|---|---|---|
| 0 | C, T | 2,623.7 | 3,673.4 | −1,049.6 |
| 1 | T, C | 2,642.8 | 3,684.9 | −1,042.1 |
| 2 | C, T | 2,653.0 | 3,659.1 | −1,006.1 |
| 3 | T, C | 2,645.5 | 3,670.4 | −1,024.9 |
| 4 | C, T | 2,623.2 | 3,680.9 | −1,057.7 |
| **median** | | **2,642.8** | **3,673.4** | **−1,042.1** (bar 59.6) |

Digests pass on all 12 rows, and nothing spilled. **Verdict: not
established. The token reverts.** The driver's label, "cut reverted in
the same commit", applies here: this is the deciding pair.

## The q01 non-regression pair

This is coverage of a rejected arm. Token `adr611q01-b2d182ff7de4`,
launched 01:36:52 CDT.
- **Timing:** the first child ran at 01:39:00, and completion arrived
  at 01:40:11 (success).
- **Separation and window:** both verified.
- **Start gate:** load 0.01 and 37 °C.
- **Control variability:** ratio 0.014.

| pair | order | control ms | treatment ms | gain ms |
|---|---|---|---|---|
| 0 | C, T | 3,333.4 | 3,323.3 | 10.2 |
| 1 | T, C | 3,329.6 | 3,323.2 | 6.4 |
| 2 | C, T | 3,331.7 | 3,338.0 | −6.3 |
| 3 | T, C | 3,376.9 | 3,343.7 | 33.2 |
| 4 | C, T | 3,349.8 | 3,321.0 | 28.8 |
| **median** | | **3,333.4** | **3,323.3** | **10.2** (bar 94.5) |

| interpretation (predeclared) | outcome |
|---|---|
| regression: treatment slower by more than `2 × range_control` with every pair negative, or a digest mismatch, or treatment-only spill | none |
| **non-regression** | **this one** |

Peak RSS is 29,216–30,016 KiB across both arms. Both arms run q01's
same two-phase path. Control's median is 1,419.3 ms below ADR 608's
q01 cell, after ADRs 609 and 610; that is not attributed here.

## The q13 diagnostics

Single runs per arm, after both pairs, with node attribution. They are
diagnostics, not a paired decomposition. The figures are for q13's
dominant fold: `scan_grouped`, one `COUNT(o_orderkey)`, 11.87 M rows.

| stage (µs) | ADR 610 control (row-at-a-time, two-phase dead) | ADR 611 control (two-phase) | ADR 611 treatment (row-at-a-time, two-phase live) |
|---|---|---|---|
| `stream_pull` | 228,622 | 228,202 | 232,013 |
| `projection` | 482,237 | 482,240 | 485,228 |
| **`row_lookup_update`** | **1,532,647** | **1,671,386** | **2,674,129** |
| `finalize` | 103,653 | 102,218 | 101,394 |

The ADR 610 column comes from ADR 610's diagnostics on its own control
executable, a different build, and is cited for comparison only.
`row_lookup_update` carries the whole difference. The same row-at-a-time
source costs about 1.1 s more in the build where the two-phase branch
is live. What differs in that build is not yet known.

## Consequence

- The closing commit on main is the control tree
  (`TWO_PHASE_ELIGIBILITY = AllTyped`), with the amended plan and this
  record. Main's behaviour is ADR 610's. The q13 regression ADR 610
  recorded remains open.
- **ADR 612 is a diagnostic record, not a cut.** It uses profiling
  builds of three arms: ADR 610's `RowAtATime`, ADR 611's `AllTyped`
  control, and ADR 611's `AtLeastTwoTyped` treatment. Each has its
  binary, tree, digest, and receipt identity. On q13 it reports:
  - period-weighted `perf` of the row loop, with hot symbols and
    inlining;
  - disassembly and code-size evidence;
  - where the RSS difference is allocated.

  Its timing is diagnostic, never paired latency. It comes before any
  one-token cut, and it reopens ADR 610's second-pass reading as one
  candidate among several.
- In main today, the only grouped fold that runs row-at-a-time is q02's
  untyped one (5,056 rows). Every other typed fold is two-phase under
  `AllTyped`.
- No 22-cell total is inferred.
