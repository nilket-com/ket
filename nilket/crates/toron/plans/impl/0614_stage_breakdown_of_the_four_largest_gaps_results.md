# ADR 614 - stage breakdown of the four largest gaps: results

Run 2026-09-30 on strix: five node-attributed runs each of q21, q09,
q18, and q05. They used ADR 613's measured release executable
(`9a84d5c4…`) through `toron-bench`, one run at a time, with the host
quiet (load 0.16). The runs are diagnostics, not a board and not paired
latency.

**All 20 runs passed their gates, and are top-level reconciled for q21,
q09, and q05. q09 and q05 are strict-incomplete: nested sub-stages are
missing.** The gates were:
- digest and plan fingerprint equal to ADR 613's sealed cell;
- status pass;
- spill scratch empty afterwards.

Every run's profile reconciles:
- the in-tree residual is 0, with no stage or bridge mismatches;
- node exclusives equal the bound span;
- per run, under each coverage measure separately, the covered stages
  plus that measure's uncovered remainder equal execution exactly
  (identity gap 0 µs for both).

Invocation topology and stage sequences match across the five runs of
every cell, so the stage medians below are by ordinal.

**The observed owner: join preparation.** `runtime_prep`, where a
join-chain route builds its hash tables before probing, is the largest
reconciled stage in three of the four cells. It totals **9.2 s** of
their median time:

| cell | `runtime_prep` ms | of execution |
|---|---|---|
| q05 | 3,811 | 77% |
| q21 | 3,691 | 46% |
| q09 | 1,733 | 24% |

In q05, one build stage alone (replay 1's `prep_build`) takes 3,804 ms.
DuckDB's whole q05 takes 611 ms (context only).

**q18 has no stage attribution.** Its time is node-level only:
aggregate 3,516 ms, exchange 1,134 ms, and join 840 ms.

These are observed stage owners. They choose no mechanism and project
no saving.


## Method, as run

- **Script and read-out:** `adr614_run.sh` (SHA-256 `0a35ac81…`) and
  `adr614_read.py` (`6dc1861f…`), with the read-out in
  `target/tpch/adr614-breakdown/readout.json` (`85974851…`).
- **The read-out was revised in review, read-only.** An earlier version
  (`b0fb46d4…`, read-out preserved as `readout-v1.json`, `5953ce5e…`)
  had a flaw. It computed the uncovered remainder and its identity only
  under the strict measure, while also reporting top-level coverage, so
  the two denominators were mixed. The current version computes each
  measure's covered and uncovered time separately, each with its own
  per-run identity check, on the same 20 saved runs.
- **Plan clarification before the read-out** (reviewed, riding in this
  commit): the coverage numerator is only the top-level stage spans of
  reconciled invocations. The uncovered remainder is:
  - other node kinds;
  - unreconciled invocations;
  - reconciled invocations' unstaged time;
  - the outer residual.
- **The DuckDB panel:** ADR 604's D1 operator profiles, with each D1
  result hash checked read-only against ADR 613's sealed DuckDB cell.
  **All four match**, so they are labelled current-cell context.
- **Two coverage measures are reported, each with its own accounting,
  because the profile distinguishes them:**
  - **Strict:** the profile's own `passed`, which requires every
    required stage, nested ones included.
  - **Top-level:** the top-level stages reconcile arithmetically within
    the residual boundary (`status: reconciled`,
    `arithmetic_reconciled`, `within_residual_boundary`).

  q09 and q05 are top-level reconciled but not strict. Their
  `suffix_probe` replay 2 is missing its nested `suffix_lookup`,
  `suffix_match_expand`, and `suffix_payload_take` sub-stages
  (`coverage_complete: false`). The top-level spans are exact, and the
  nested detail for that one replay is absent.
- **A read-out fix, before any reading was used.** The first read-out
  failed on the profile's reconciliation statement, a string: "external:
  root span is a subset of the perf row execute_ms". The read-out was
  corrected to check the in-tree residual, the mismatch lists, and
  nodes against the bound span. It was then re-run read-only on the same
  saved artifacts.
- **Peak RSS** is not in these rows.

## Per cell

Values are the median over five runs, with the range. Each displayed
component is its own five-run median, so the displayed components need
not sum exactly to a displayed median total. The exact identity holds
per run. `execute_ms` is
compared with ADR 613's board median. The difference is observed, and
may include attribution overhead and host drift. It is not apportioned.

### q21: 8,035.6 ms (8,015.7–8,098.9); board 7,972.7 (+62.9)

Route `inner_prefix_semi_anti_grouped`. Coverage is **98.5%**, both
strict and top-level.

| ordinal | stage | ms | range |
|---|---|---|---|
| 0 | `runtime_prep` | **3,691.4** | 3,662.4–3,738.5 |
| 1 | `base_prep` | 0.9 | |
| 2 | `base_scan` | 505.6 | 504.3–507.1 |
| 3 | `inner_replay_0` | 1,136.0 | 1,127.3–1,138.5 |
| 4 | `inner_replay_1` | **2,105.3** | 2,104.2–2,114.1 |
| 5 | `inner_replay_2` | 272.6 | 251.2–273.7 |
| 6 | `semi_replay_3` | 100.8 | |
| 7 | `anti_replay_4` | 80.3 | |
| 8 | `grouped_fold` | 3.1 | |
| 9 | `finalize` | 0.5 | |
| 10 | `cleanup` | 27.9 | |

- **Uncovered, the same under both measures:** 119.9 ms. That is 117.6
  ms unstaged inside the invocation, 1.3 ms of other nodes, and a
  0.9 ms outer residual.
- **Spill:** 27,242,808 bytes, as on the board.
- **`runtime_prep` has no nested refinement.** This route records no
  per-replay `prep_build`.
- **DuckDB context:** 2,218 ms. By kind: join 1,062, scan 748, and
  filter 308 ms. The largest operators are a `HASH_JOIN` at 525 ms and
  `READ_PARQUET`s at 300, 225, and 149 ms.

### q09: 7,134.1 ms (7,094.5–7,217.2); board 6,978.4 (+155.7)

Route `reoriented_inner_chain_grouped`. Coverage is **97.9%** top-level
and 0% strict (the replay-2 nested stages are missing).

| ordinal | stage | ms |
|---|---|---|
| 0 | `runtime_prep` | **1,732.6**, of which `prep_build`: replay 2, 1,009.7; replay 3, 692.4; replay 1, 36.6 |
| 4 | `scan_pull` | 618.5 |
| 5 | `base_probe` | 646.4 |
| 6 | `base_materialize` | 244.0 |
| 7 | `repartition` | 702.8 |
| 8 | `suffix_probe` (replay 1) | 703.7 (`suffix_lookup` 457.9) |
| 9 | `suffix_probe` (replay 2) | **1,204.2** (nested missing) |
| 10 | `suffix_probe` (replay 3) | 222.7 |
| 11 | `suffix_probe` (replay 4) | 108.3 |
| 12 | `fold` | 188.6 |
| 14 | `cleanup` | 584.6, of which `cleanup_build` replay 2: 545.6 |

**Uncovered, top-level:** 145.5 ms (2.1%). That is 92.8 ms unstaged
inside the invocation, 52.3 ms of scan, filter, and exchange outside
it, and a 0.3 ms outer residual. **Strict:** the whole 7,134.1 ms, since
the invocation fails the strict check for its missing replay-2 nested
sub-stages.
DuckDB context: 1,647 ms. By kind: join 880 and scan 640 ms. Its
`HASH_JOIN`s are 431, 374, and 68 ms.

### q18: 5,977.7 ms (5,971.9–6,051.5); board 5,976.7 (+1.0)

**No stage attribution.** Both aggregate invocations are unreconciled,
as on ADR 604's census:
- `scan_grouped`, 1,867 ms, has no stage vocabulary;
- an `uncovered_generic_fold`, 1,645 ms, has no handler span.

Coverage is 0% by both measures. The whole 5,977.7 ms is uncovered under
both: 3,515.7 ms of unreconciled invocations, 2,467.1 ms of other nodes,
and a 0.1 ms outer residual. It is reported by node kind:

| node kind | ms | calls |
|---|---|---|
| aggregate | **3,515.7** | 2 |
| exchange | **1,133.6** | 8 |
| join | 839.7 | 3 |
| scan | 378.9 | 3 |
| filter | 115.6 | 1 |

DuckDB context: 1,488 ms, of which **`HASH_GROUP_BY` is 1,050 ms**
(12,000,000 rows in). Scan is 259 ms and join 105 ms.

### q05: 4,956.4 ms (4,936.6–4,973.5); board 4,752.0 (+204.4)

Route `reoriented_inner_chain_grouped`. Coverage is **99.4%** top-level
and 0% strict (the replay-2 nested stages are missing).

| ordinal | stage | ms |
|---|---|---|
| 0 | `runtime_prep` | **3,811.4**, of which `prep_build` replay 1: **3,804.3**; replay 2: 7.0 |
| 4 | `scan_pull` | 67.9 |
| 5 | `base_scan_filter` | 28.1 |
| 6 | `base_probe` | 65.6 |
| 8 | `repartition` | 27.5 |
| 9 | `suffix_probe` (replay 1) | 198.0 |
| 10 | `suffix_probe` (replay 2) | 566.4 (nested missing) |
| 15 | `cleanup` | 75.3 |

**Uncovered, top-level:** 30.7 ms (0.6%). That is 25.7 ms unstaged
inside the invocation, 4.7 ms of other nodes, and a 0.3 ms outer
residual. **Strict:** the whole 4,956.4 ms, for the same missing
replay-2 nested sub-stages.

DuckDB context: 611 ms. By kind: scan 471 and join 115 ms. The largest
operator is `READ_PARQUET` at 375 ms, emitting 1,557,352 rows.

## Synthesis: observed stage owners

Ranked by median time. These are observed owners: no mechanism is
chosen, and no saving is projected.

| rank | owner | cell, route | ms | recurs |
|---|---|---|---|---|
| 1 | `runtime_prep` / `prep_build` replay 1 | q05, reoriented chain | 3,804 | `runtime_prep` in 3 of 4 cells |
| 2 | `runtime_prep` (unrefined) | q21, inner prefix semi-anti | 3,691 | ″ |
| 3 | aggregate node (unattributed) | q18 | 3,516 | — |
| 4 | `inner_replay_1` | q21 | 2,105 | the replay family in q21 |
| 5 | `runtime_prep` (`prep_build` replays 2 and 3: 1,010 and 692) | q09, reoriented chain | 1,733 | ″ |
| 6 | `suffix_probe` replay 2 | q09 | 1,204 | `suffix_probe` in q09 and q05 |
| 7 | `inner_replay_0` | q21 | 1,136 | |
| 8 | exchange node | q18 | 1,134 | |
| 9 | join node | q18 | 840 | |
| 10 | `repartition`, `base_probe`, `scan_pull`, `cleanup` | q09 | 703, 646, 619, 585 | |

**Join preparation (`runtime_prep`) totals 9,236 ms across q05, q21, and
q09.** It is the largest observed recurring owner. Within it, the
refined cells show single build stages dominating: q05 replay 1 at
3,804 ms, and q09 replays 2 and 3 at 1,010 and 692 ms.

The second family is q21's inner replays: 3,514 ms over replays 0 to 2.

q18's time is outside any stage vocabulary. Its node-level owners are
aggregation (3,516 ms, against DuckDB's 1,050 ms `HASH_GROUP_BY` as
context) and exchange (1,134 ms).

## Consequence

- **No cut is proposed.**
- The next record's owner, by this evidence, is **join preparation**: the
  hash-table builds in `runtime_prep` on the chain routes, led by q05's
  3.8 s replay-1 build. Its mechanism (what the build does per row,
  which input it builds, and how large) is established by that record's
  own plan, from code and targeted evidence.
- **q18 needs stage vocabulary** on its `scan_grouped` and generic-fold
  handlers before its aggregate time can be decomposed. That is a
  separate record.
