# ADR 400 - sf=10 post-399 reach re-check

## Status
Proposed.

## Context

ADR 396, ADR 397, and ADR 399 compounded the Rune q03 sf=1 substrate path from
`Rune/Polars=12.41x` to `3.74x`. ADR 398, measured after ADR 397, still showed
q03/q05/q10 as sf=10 `time_no_row`. ADR 400 is a small measurement-only
re-check: did the compounded sf=1 substrate wins pull any affected heavy cell
toward sf=10 completion, or is the next higher-return cut the SQL/Rune
planner-shape lever?

This ADR does not change engine behavior.

## Decision

Run a focused live sf=10 persisted measurement under the fixed 4 GB cap. Do not
use the sf-agnostic matrix cache.

Measure only:

- q03, q05, q10: directly affected by the join-input pushdown and join-build
  substrate strikes;
- q08: ADR 398's new `reoriented join build accumulator` wall;
- q19: green/no-regress control.

Fill a delta table against ADR 398:

| query | 398_status | 398_toron_ms | 400_status | 400_toron_ms | delta | producer old vs new | correctness | verdict |
|---|---|---:|---|---:|---:|---|---|---|
| q03 | time_no_row | >331s | | | | | | |
| q05 | time_no_row | >327s | | | | | | |
| q10 | time_no_row | >348s | | | | | | |
| q08 | fail, reoriented join build accumulator | 1233.7 | | | | | | |
| q19 | pass | 81479 | | | | | | |

Correctness provenance is required for every pass row. Use persisted sf=10
correctness via the ADR 365 path when available; otherwise mark the row
`reach_only_no_oracle`. Non-pass rows are not green by implication.

## Acceptance

- The delta table is filled from live persisted sf=10 measurements under fixed
  4 GB.
- Every pass row records correctness provenance.
- The result names a fork verdict:
  - if an affected cell moved materially, reprioritize around its new producer;
  - if reach is unchanged, switch to the SQL/Rune planner-shape lever.
- No engine source changes, no cap bump, and no committed `.perf.data`.

## Guardrails

- Measurement-only.
- Live persisted data, not cached matrix state.
- Focused target set: affected cells plus one green control.
- Delta pinned against ADR 398 in the results artifact.
- No green by implication.

## Forward Arc

Expected but not assumed: ADR 399's incremental sf=1 improvement is unlikely to
move q03/q05/q10 all the way across the sf=10 completion threshold. If the board
is unchanged, the next ADR should target the measured SQL/Rune planner-shape gap
on the default SQL path. If a cell moves, its newly exposed producer takes
priority.

## Grounding evidence

ADR 399: seeded `ahash::RandomState` for private join-build maps, Rune q03 sf=1
loop `391.4 -> 355.8 ms`, `Rune/Polars 3.97x -> 3.74x`, sealed at `b438409`
after the determinism note. ADR 398: q03/q05/q10 `time_no_row`, q08
`reoriented join build accumulator`, q19 green control. ADR 365 supplies
persisted correctness. ADR 356 supplies the perf rail.
