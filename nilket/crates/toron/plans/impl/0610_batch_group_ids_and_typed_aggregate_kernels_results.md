# ADR 610 - batch group ids and typed aggregate kernels: results

Run 2026-09-29/30 on strix, at the ADR 608 coordinate, bound to ADR
608's sealed board. The sequence was the all-cell gate, the q01 pair,
the q13 breadth pair, and then the mechanism diagnostics.

**Retained on q01.** The treatment's median is 3,325.1 ms against
control's 4,087.1 ms. The median paired gain is **754.9 ms (18.5%)**,
against a bar of 24.7 ms (`2 × range_control`, range 12.3 ms), and every
pair is strictly positive (733.0 to 769.7 ms). The token stays
`TwoPhase`.

**The q13 breadth pair fired the regression guard.** The treatment's
median is 2,633.2 ms against control's 2,507.0 ms. The median paired
gain is **−122.4 ms (−4.9%)**, beyond the bar of 76.3 ms, with **every
pair negative** (−106.0 to −247.3 ms). By the plan, that opens a new
record on the token, **ADR 611**, and is not a silent revert. **The
retained token carries this known q13 regression until ADR 611
closes.**

Both pairs had digest parity on all rows and no spill.
- **Peak RSS on q01:** 29,200–29,988 KiB across both arms, with
  overlapping ranges and no material difference observed.
- **Peak RSS on q13:** about 2.7 MB higher on the treatment
  (262,316–262,876 against 259,368–260,204 KiB). That is consistent
  with q13's larger group-id buffer, whose diagnostic gate reading is
  2,340,576 B.

No 22-cell total has been measured with this token.


## Identity

| identity | value |
|---|---|
| control | commit `8d73e650`, tree `0a6f91c85e5b526dd9e9514ab05a8f6457a31732`, tag `artifact/adr610-control`; executable `8abc0ef82e99ec3bd6a214a89fe7e41457daab948defe766a4a5cc7ba8058706`; manifest `adr610-control.manifest` (`a2ff5832…`); `GROUPED_FOLD_SHAPE = RowAtATime` |
| treatment | commit `96f0dc85`, tree `2e4da7cd68bdfa198b9f2e6e1d54b7b3805e67c5`, tag `artifact/adr610-treatment`; executable `6cc16286aeb173f6947cd9a94c4329458c070738b09c8026a24e0c6096d7d984`; manifest `adr610-treatment.manifest` (`ba4494b3…`); `TwoPhase` |
| difference | one line in `exec/aggregate/fold_kernel.rs` |
| sealed baseline | `target/tpch/adr608-board/perf_board.jsonl.evidence.sealed` |
| ladders (bigfoot, both trees, fmt clean) | toron 5,511 passed, 0 failed, 3 ignored (13 new ADR 610 fixtures); toron-tpch 674 passed, 0 failed, 6 ignored; release builds warning-free under both token settings |
| build host / run host | bigfoot / strix |

Both arms include ADR 609 (`GROUP_KEY_LOOKUP = Borrowed`). So q01's
control here is 665.6 ms (14.0%) below ADR 608's sealed q01 cell. That
difference is recorded and not attributed.

**The warning cleanup.** ADR 609 left `GroupKeyLookup::Owned`
unconstructed in the `Borrowed` build, and the build warned
`dead_code`. **The warning is fixed by a commented `#[allow(dead_code)]`
on the token enums** (`GroupKeyLookup`, and ADR 610's `GroupedFoldShape`,
which has the same property). The reason: with a `const` token, each
arm's build constructs only the variant it names. The exhaustive
`match form` added to `classify` only future-proofs the classification.
A pattern does not count as construction, and the warning persisted with
the match alone.

**Implementation review.** Codex's review held the build twice before
measurement:
- **The template.** A receipt-armed batch with an untypeable call built
  a state template, including a KLL sketch for `APPROX_QUANTILE`, and
  so changed execution. A template is now built only when every call is
  typeable, and the receipt reads `not_selected` otherwise.
- **Missing fixtures.** Two acceptance fixtures were missing:
  - the MIN, MAX, and `STRING_AGG` fallbacks, as real folds;
  - kernel-against-current-update parity for all 11 input types, with
    and without NULLs: 120 combinations.

## The gate

One run of each of the 22 queries per arm, with
`TORON_GROUPED_FOLD_KERNEL_RECEIPT_PATH` set to one file per arm and
query (`adr610-strix-host-record/gate`). All 44 digests and plan
fingerprints equal ADR 608's sealed cells. Every control instance is
`per_row`, with zero two-phase batches.

**q01 under `TwoPhase`:** one instance; 5,859 two-phase and 0
row-at-a-time batches; 47,313,651 rows. The kernels are exactly:
- `typed:sum_int_decimal128` ×4;
- `typed:avg_decimal_decimal128` ×3;
- `typed:count_star` ×1.

Each has updates equal to `rows_folded`, with **zero** per-row updates.

**Coverage under `TwoPhase`.** 17 cells fold at least one instance
two-phase. The typed aggregate-row updates per cell are:
- q01 378.5 M;
- q13 13.07 M (`count_nonnull` 11,869,667 + `count_star` 1,200,000);
- q15 3.63 M;
- q09 2.61 M;
- under 1 M each: q16, q10, q12, q04, q11, q03, q22, q20, q05, q17, q07,
  q08, q21, q18.

q02 stays row-at-a-time: its MIN reads `not_selected`, `per_row`. q06,
q14, and q19 have no grouped fold. These are mechanism facts, not
timing attribution.

**Group-id buffer peak capacity** (diagnostic gate readings, treatment
receipts):
- q01: 32,540 B (largest batch 8,135 rows);
- q13 `count_nonnull` instance: 32,532 B;
- q13 `count_star` instance: 2,340,576 B (batches of up to 585,144
  rows).

**Breadth cell.** q13, the only non-q01 cell at or above 5 M typed
aggregate-row updates, with its digests passing.

## The q01 pair

Token `adr610q01-dc990379daa9`, launched 00:06:45 CDT.
- **Timing:** the first child ran at 00:08:53, and completion arrived
  at 00:10:09 (`result=success exit_code=exited exit_status=0`).
- **Separation and window:** separation verified (margin 128.1 s); the
  window verified, with no session events.
- **Start gate:** load 0.08 and 37 °C.
- **Control variability:** ratio 0.0030.

| pair | order | control ms | treatment ms | gain ms |
|---|---|---|---|---|
| 0 | C, T | 4,090.4 | 3,320.7 | 769.7 |
| 1 | T, C | 4,079.9 | 3,325.1 | 754.9 |
| 2 | C, T | 4,092.3 | 3,359.3 | 733.0 |
| 3 | T, C | 4,087.1 | 3,336.8 | 750.3 |
| 4 | C, T | 4,084.4 | 3,319.2 | 765.2 |
| **median** | | **4,087.1** | **3,325.1** | **754.9** (bar 24.7) |

Warmups: control 4,092.3 ms, treatment 3,313.6 ms. Peak RSS is
29,200–29,988 KiB across both arms, with overlapping ranges.
**Verdict: retained.**

## The q13 breadth pair

Token `adr610q13-b5f765c8c7f2`, launched 00:10:34 CDT.
- **Timing:** the first child ran at 00:12:42, and completion arrived
  at 00:13:45 (success).
- **Separation and window:** separation verified (margin 128.1 s); the
  window verified.
- **Start gate:** load 0.06 and 37 °C.
- **Control variability:** ratio 0.015.
- **Distance from the anchor:** control's median is 14.4 ms above ADR
  608's sealed q13 cell.

| pair | order | control ms | treatment ms | gain ms |
|---|---|---|---|---|
| 0 | C, T | 2,499.8 | 2,622.2 | −122.4 |
| 1 | T, C | 2,525.9 | 2,631.9 | −106.0 |
| 2 | C, T | 2,507.0 | 2,754.2 | −247.3 |
| 3 | T, C | 2,534.1 | 2,641.9 | −107.8 |
| 4 | C, T | 2,495.9 | 2,633.2 | −137.3 |
| **median** | | **2,507.0** | **2,633.2** | **−122.4** (bar 76.3) |

The driver's verdict block reads `"consequence": "cut reverted in the
same commit; candidate closed"` and `"status": "not_established"`. That
is ADR 588's generic action label, and it does not apply to this
coverage pair.

| interpretation (predeclared) | outcome |
|---|---|
| breadth established | no |
| not established, without regression | no |
| **regression**: treatment slower by more than `2 × range_control` (76.3 ms), with every pair negative | **yes**: −122.4 ms median, all five pairs negative |

Under the plan, the regression opens a new record on the token: ADR
611.

## The mechanism diagnostics

A single q01 and a single q13 run per arm, after both pairs, with node
attribution. These are diagnostics: not paired latency, and not a
paired decomposition. All reconcile, and all digests pass.

| stage (µs) | q01 control | q01 treatment | q13 control | q13 treatment |
|---|---|---|---|---|
| `stream_pull` | 702,431 | 699,344 | 228,622 | 230,230 |
| `projection` | 191,874 | 190,727 | 482,237 | 485,289 |
| `arg_eval_and_readers` | 913,281 | 918,559 | 4 | 1 |
| **`row_lookup_update`** | **2,244,312** | **1,491,435** | **1,532,647** | **1,683,069** |
| `fold` | 3,168,720 | 2,421,524 | 1,535,089 | 1,685,709 |

The q13 figures are its dominant fold: `scan_grouped`, one
`COUNT(o_orderkey)` over about 1.2 M customer groups and 11.87 M rows.

In these runs the observed difference sits in `row_lookup_update` on
both cells: −753 ms on q01 and **+150 ms on q13**. The candidate
mechanism for q13 is the risk the plan named. Under `TwoPhase`, each
row's group state is touched again in a second, scattered pass. With a
single aggregate and a large group set, that could account for the
observed direction. q01's eight aggregates over four groups are
consistent with a per-row dispatch saving outweighing it there. This is
a hypothesis for ADR 611 to test, not an established decomposition.

## Consequence

- The closing commit on main is the treatment tree
  (`GROUPED_FOLD_SHAPE = TwoPhase`), with this record and the ADR's
  status. **The known regression on q13 is pending ADR 611.**
- ADR 611 is the next plan and implementation pair. Its first
  hypothesis is a static eligibility rule: two-phase only for call
  lists of at least two typed aggregates. q13's two instances each have
  one; q01 has eight.
  - A one-token pair compares the current `TwoPhase`, where every
    eligible batch is two-phase, against the ≥2 rule.
  - It is gated on all 22 digests and receipts.
  - q13 tests recovery, and q01 tests non-regression.
  - ADR 610 remains the evidence for q01's gain over `RowAtATime`.
- No refreshed 22-cell total is inferred until a board is sealed.
