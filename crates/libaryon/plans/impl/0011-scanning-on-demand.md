# ADR 0011 implementation: scanning on demand

Decision: `plans/adr/0011-scanning-on-demand.md`.

Scope: no scan that nothing asked for, the visible window first, and the caret's last known column.
Delivered as `impl: adr 0011 <slice>` commits, squashed into one impl before pushing. Converged with
Codex over chatd (conversation `startup-latency-followons`).

## Phases

### P1: peek, and advance only to a target

- `WrapIndex::known_rows(key, line) -> RowCount` reads the frontier and the total without
  advancing. `line_rows` uses it.
- `LineWrap::advance_until(target, budget)` stops once the frontier passes the target row, byte or
  column. `row_start`, `row_for_col` and `cell` use it. `row_count`, which advances to the end, stays
  for explicit consumers.
- `collect_visible_rows` asks `row_start` row by row and moves on at `PastEnd`.
- `scroll_viewport_anchor` advances toward its target row rather than the line's end.
- Regressions:
  - layout, from the experiment that found the cause: a 3 MiB line anchored at row 1 publishes its
    rows on the first frame, with budget to spare;
  - render: with no input, a huge view stops scheduling continuation frames, counting the frames
    rather than calls.

### P2: the scan's consumers

- An audit of every `row_count` caller, and of anything that relied on the whole-line scan side
  effect:
  - a forward scroll, which needs its target row and the viewport tail;
  - a backward move into a long preceding line, which needs its last row;
  - End and exact counts, which need the total;
  - the scrollbar or minimap;
  - `rows_from_anchor_to_position` across lines.
- Each keeps an explicit request: `scan_to_end`, or `row_count` where it needs exactness.
- Regression: no frames are scheduled once an idle huge view settles, with a bound in frames.

### P3: the window's share of the budget

- `before_layout(carried, index, registry, root, resolve_carets)`, used by render and by the test
  harnesses: hold half the budget (`reserve_read`), resolve the carets, advance a carried line scan,
  then release the hold (`release_read`) for layout. It replaces `carry_on`.
- Regression `a_visible_window_a_pending_caret_and_an_end_scan_all_progress`: with a 64 KiB budget,
  a window drawn at row 1000, an End scan of the line, and a caret deep in it:
  - the window equals the whole-line layout on every frame;
  - the caret resolves and the scan finishes within bounded frames.

  Holding nothing back blanks the window on frame 0.
- The previous-window fallback and the caret projection cache are not built (ADR decisions 5 and 6,
  as amended by measurement).

### P4: measurement and findings

- Idle frames after a 1-row scroll on 50 MiB, 1 GiB and 64 GiB.
- Deep motions and edits on 64 GiB at 50 M columns and past `u32::MAX`.
- Resident pins and evictions, if deep cold lookups still lag.
- Findings in `plans/impl/0011-scanning-on-demand-findings.md`.

## Acceptance

1. With no input, a huge line's view stops scheduling wrap continuation frames within a few frames of
   settling. A window that was drawable is never blanked only because of unrelated scan work; a cold
   first window may be pending. The ADR 0007 ingest's unchanged frames while a huge file's lines are
   counted are recorded as a follow-on, not as quiet.
2. Deep caret motions and edits on the 64 GiB line resolve within a few frames, measured at 71 M and
   4.04 billion columns. Past `u32::MAX` the release test `caret_past_u32_test` covers it.
3. End, far scrolling and the past-`u32` release test behave as before.
4. Ordinary files are unchanged.
