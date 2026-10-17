# ADR 0011 implementation findings: scanning on demand

Companion to `0011-scanning-on-demand.md`. Records the cause, what the `impl: adr 0011` commit
delivered, how it departs from the plan, and what it measured.

## Cause

A layout test replaying render's per-frame calls on a 3 MiB line, anchored at row 1, published no
rows on frames 0 to 2, with the budget spent. The scan meanwhile advanced 1, 2 and 3 MiB, and rows
appeared only once the whole line was scanned:

- `normalize_anchor` and `collect_visible_rows` asked `line_rows` for the line's row count.
- `line_rows` asked `WrapIndex::row_count`, which, while the total was unknown, scanned on with all
  of the frame's remaining budget.
- The visible row's lookup then had nothing left, and the line was suppressed (ADR 0009 decision 8).

On the engine, the probe showed alternating empty frames (no visible rows, no tokens) for as long as
the scan ran: about 64,000 frames on a 64 GiB line. The same scan was what starved deep caret
lookups. They resolved in milliseconds once it was gone (measurements below), so the eviction and
pinning lead from ADR 0010's findings was not needed.

## Delivered

- **The index:**
  - `known_rows` peeks at a line's rows without scanning.
  - `advance` takes a stop condition, so `row_start`, `row_for_col` and the cell lookups scan only
    until the frontier passes their target.
  - `row_count`, which still scans toward the end, is left for explicit consumers.
  - `release_read` gives back a hold.
- **Layout:**
  - `line_rows` peeks.
  - `line_rows_scanning` keeps the advancing count for the consumers that need a line's total: a
    backward move into a long preceding line, and full-scan tests.
  - `visible_row_lookup` returns `RowAt::{Known, PastEnd, Unknown}`, and `collect_visible_rows` asks
    for rows one by one, leaving a line only at `PastEnd`.
  - A forward scroll asks for its target row only.
  - `rows_from_anchor_to_position` counts up to a cap, through `line_rows_up_to`, which scans only
    toward the row at the cap:
    - pan and clamp use the viewport's height, and a cursor below the view is reached by scrolling
      back from the cursor;
    - cursor metrics use `CURSOR_ROW_DISTANCE_CAP`, 65,536 rows.
  - The row step is checked at `u32::MAX` (`next_row_offset`).
- **Render's frame order (`before_layout`):** half the budget is held for the window, the carets
  resolve, then a carried line scan advances, and the hold is released for layout. This replaces
  `carry_on`.
- **Tests:**
  - in layout: an unscanned line shows its rows on the first frame; a cursor below a long line does
    not scan it to its end; the row step stops at the last representable row; a visible window, a
    pending deep caret and an End scan all progress within one shared budget;
  - in the engine: `tests/idle_scan_test.rs`, where an idle 4 MiB line stops publishing frames and
    never blanks a drawn window.

  Each test fails when the behaviour it covers is reverted: `line_rows` scanning, the uncapped
  count, and no hold.

## Departures from the plan

1. **P3 shrank to the budget hold.** No frame needed the previous-window fallback once the scan was
   on demand. The caret projection cache was dropped too: the first frame after a deep edit shows the
   right column, and a cache keyed by layout identity would never match across an edit. Both are
   recorded as amendments to ADR decisions 5 and 6.
2. **The cross-line row distance is capped,** where the plan had only audited it. Uncapped, a cursor
   on a line below a huge anchor line would have rescanned that line every frame.
3. **Two existing tests had relied on `line_rows` scanning.** One waited in a loop of 100,000
   iterations for an exact count. They now use `line_rows_scanning`.

## Measurements

Development machine, release `startup_probe` example (untracked), 100x30, wrap on, cold cache.

Idle frames in 8 s after a one-row scroll:

| File | Before (ADR 0010) | After | Distinct states | Empty frames |
| --- | --- | --- | --- | --- |
| 50 MiB words | 99 | 2 | 2 | none |
| 1 GiB sparse | 170 | 8 | 3 | none |
| 64 GiB sparse | ~550 | 341 | 2 | none |

On 64 GiB, a 30 s trace shows the remaining frames run only while `line_count` is still `None`.
That is ADR 0007's ingest counting newlines over 64 GiB, which publishes unchanged frames until the
count lands. It is a follow-on, and not wrap work.

Deep motions and edits on the 64 GiB sparse line, timed from the command to the first frame whose
caret moved (ms):

| Depth | Right | Left | Next word | Prev word | Insert | Backspace | Home |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 71 M cols | 3.1 | 7.2 | 12.3 | 6.7 | 5.2 | 3.8 | 0.08 |
| 4.04 B cols | 3.2 | 3.6 | — | 7.8 | 6.9 | 5.1 | 0.17 |

- At 4.04 billion columns (below `u32::MAX`), the next-word window was cold. It asked for its node,
  and a second press moves it, as ADR 0010 P4 intends.
- Under ADR 0010 the same 64 GiB motions did not resolve within 30 s.
- Past `u32::MAX`, `tests/caret_past_u32_test.rs` (release, `--ignored`) passes.
- Peak RSS stays at about 130 MB.

## Follow-ons

- **Ingest frames:** don't publish unchanged frames while ADR 0007's ingest counts a huge file's
  lines.
- **Cold word windows:** a word motion whose window is cold needs a second press after hydration. A
  carried word-motion intent could finish it instead, if wanted.
