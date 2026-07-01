# ADR 398 - sf=10 post-397 mini-atlas

## Status
Proposed.

## Context

397's generic join projection pushdown (`optimize_join_inputs_for_required`) cut q03 sf=1 raw scan 1.09 GiB -> 306.6 MiB
(-72% bytes, -63.8% time). At sf=10 the multi-table cells were **time/no-row** - data-movement-bound, materializing
full-column join inputs. **398 tests whether 397 moved the reach board** by reading far fewer bytes. **Ranking artifact,
measurement-only - no engine change.** Then 399 (the substrate re-profile) is chosen with the board in hand; the two are
not bundled.

## Decision

### Target set (roles)
- **q03 / q05 / q10** - directly affected by 397 (multi-table join projection).
- **q12** - spill-family anchor; validates whether the 389 partition-group family moved.
- **q08** - scan / inner-chain control.
- **q19** - time-path control that should **not** regress.
- **q04** - green correctness/perf control.

### The board table (pinned, not prose)
Pin the prior class from the latest trusted board (386 + 388/389/390); record live post-397:

| query | pre397_class | post397_status | post397_producer | correctness | toron_ms | duckdb_ms | polars_ms | toron/duckdb | toron/polars | verdict |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| q03 | time/no-row (390) | | | | | | | | | |
| q05 | time/no-row (390) | | | | | | | | | |
| q10 | time/no-row (390) | | | | | | | | | |
| q12 | partition-group spill -> time (389) | | | | | | | | | |
| q08 | time/no-row (387) | | | | | | | | | |
| q19 | green (385) | | | | | | | | | |
| q04 | green (374) | | | | | | | | | |

### Correctness provenance - no green by implication
Any post-397 **pass** row must run **persisted sf=10 correctness via the ADR 365 path**, or be marked
**`reach_only_no_oracle`** if the oracle path cannot validate it. A completed perf row is **not** green by implication -
`correctness` column is the passing oracle row or the explicit `reach_only_no_oracle`.

### Measurement conditions
Live persisted sf=10, fixed 4 GB, the perf rail - **NOT** the sf-agnostic matrix / correctness-matrix cache. Polars only
where the SQL subset runs (q03 works; others per the 394 capability table); DuckDB where the row exists.

## Acceptance
- **The pinned board table filled live** - pre397_class pinned; post397_status / producer / execute_ms / ratios measured
  live at persisted sf=10, fixed 4 GB.
- **Correctness provenance per pass row** - persisted sf=10 oracle (365) OR `reach_only_no_oracle`; no green by
  implication.
- **Old-vs-new producer class per cell** - moved / cleared / unchanged, named.
- **Ratios `toron/duckdb`, `toron/polars`** where those engine rows exist.
- **A ranked verdict** - did 397 reprioritize reach? which cells moved, to what producer/green; controls (q19/q04)
  unregressed, q08/q12 recorded.
- **No engine change** (measurement only); `crates/toron/src` unchanged; no cap bump; no committed `.perf.data`.

## Guardrails
- **Ranking artifact, not a strike** - measurement only; not bundled with 399.
- **Live, not cached** - matrix/correctness-matrix are sf-agnostic; sf=10 truth is a live run/perf/correctness.
- **No green by implication** - a pass row is green only with a 365 oracle pass, else `reach_only_no_oracle`.
- **Pinned pre397 class** - from the latest trusted board, in the table, not prose.
- **Controls** (q19/q04 green, q08 scan, q12 spill) confirm 397 didn't regress reach and anchor the family.

## Risks
- **No reach movement** - 397 helped perf but not sf=10 reach; still a clean ranking; 399 continues the substrate loop.
- **Partial movement** - some cells advance (time -> spill / -> green), others don't; record each.
- **Newly-complete but slow** - green-with-ratio (365 oracle); the time gap is the next perf thread, not a reach fail.
- **Oracle can't validate a completed row** - `reach_only_no_oracle`, honest; do not imply green.

## Forward Arc
398 ranks the post-397 sf=10 board. Then **399 = post-397 q03 substrate re-profile**, chosen with the board: if
q03/q05/q10 moved materially the best next substrate cut may shift; if not, 399 continues from the 3.97x Rune/Polars
residual. The board table is also article-grade data (substrate + reach movement from one generic fix).

## Grounding evidence (file:line)
397: `optimize_join_inputs_for_required` binder/pushdown.rs, q03 sf=1 1135->410 ms, raw scan 1.09 GiB -> 306.6 MiB,
`Rune/Polars` 3.97x, sealed 2d83f49. pre-397 sf=10 classes: q03/q05/q10 time/no-row (388-390), q12 partition-group spill
(389), q08 time (387), q19 green (385), q04 green (374). mini-atlas pattern 386; perf rail 356; persisted correctness
365; matrix-cache caveat (verify live).
