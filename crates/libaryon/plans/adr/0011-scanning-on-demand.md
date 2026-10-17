# ADR 0011: scan a long line only as far as something needs

Status: accepted 2026-10-16, converged by the KET council (user, Claude, Codex). Implementation
plan: `plans/impl/0011-scanning-on-demand.md`. Follows ADR 0009 (the lazy wrap index) and ADR 0010
(the caret on it), whose findings left this open.

## Context

ADR 0010's findings recorded that, with no input, a huge visible line keeps rendering until its
whole scan finishes, and that it blanks on alternate frames. On a 64 GiB line that means about
64,000 frames, or 13 minutes, and deep caret lookups make no progress meanwhile. `origin/main`
behaved the same.

A layout test replaying render's per-frame calls on a 3 MiB line, anchored at row 1, finds the
cause. Frames 0 to 2 publish no rows and leave no budget, while the scan advances 1, 2 and 3 MiB.
Rows appear only once the whole line is scanned:

- `normalize_anchor` and `collect_visible_rows` call `line_rows` to clamp an anchor and to bound
  their row loop.
- `line_rows` calls `WrapIndex::row_count`, which, while the total is unknown, advances the frontier
  with all of the frame's remaining budget.
- Neither caller needs the total. Each needs to know whether some row exists. But the scan of the
  whole line runs first, and the visible row's lookup then has nothing left: `try_visible_row`
  returns `None`, and the line is suppressed (ADR 0009 decision 8).
- `row_start` and the cell lookups advance the frontier the same way: by the whole remaining budget,
  however close the row they want is.

So the busy rendering is a scan nobody asked for, and it starves the window of the frame's budget.
Whether it also explains the slow deep caret lookups on the 64 GiB line is not established: render
resolves carried work and carets before layout. That is a hypothesis to test after this change.

## Decision

1. **Knowing a line's rows does not scan it.** `line_rows` becomes a peek: the rows scanned so far,
   and whether that count is exact. It reads nothing. `normalize_anchor` keeps an anchor past the
   known prefix, as it already does when the count is inexact.
2. **The scan advances only toward a requested row or cell, and stops there.** `row_start`,
   `row_for_col` and the cell lookups advance the frontier until it passes their target, or until the
   frame's budget runs out, not by the whole budget. Anything they request beyond the frontier is
   still reached across frames (ADR 0009 decision 10).
3. **Visible rows are found by asking for them.** `collect_visible_rows` iterates rows from the anchor
   until the viewport is full or the line ends, asking `row_start` for each. Past the end of a line
   that is not fully scanned, it moves to the next line only when `row_start` reports `PastEnd`, not
   when the peeked count runs out.
4. **Only named consumers scan further, each only as far as it needs.**
   - A forward scroll needs its target row and the viewport's rows after it. Reaching `PastEnd`
     proves where the line ends, so it never needs the line's total.
   - A backward move that crosses into a preceding line needs that line's last row. For a long line
     that is a scan to its end, requested explicitly and spread across frames.
   - End (`scan_to_end`, ADR 0010) and an exact count, such as a scrollbar asking for exactness, need
     the line's total.

   With no such consumer, nothing scans, and an idle view of a huge line schedules no more frames.
5. **The visible window has priority, and other demand still progresses.** At the start of
   render's frame, half of the budget is held for the window (`before_layout`). Within the other
   half the carets resolve first, then a requested line scan (End, ADR 0010) advances; the held half
   is then given back for layout. A scan therefore never leaves the window or a pending caret
   without budget. A caret whose target is already indexed resolves within one checkpoint gap. One
   beyond the frontier advances the scan toward it across frames, sharing that half, and once it
   resolves the scan has the half to itself. A window that is still cold may be pending. Measured after decisions 1
   to 4, no frame publishes an empty window once one was drawn, so no previous-window fallback is
   kept (this answered the open question).
6. **No placeholder caret cache.** The planned "last projection while pending" is not built. After
   decisions 1 to 5, the first frame after an edit deep in a 64 GiB line already shows the right
   column (measured), and a cache keyed by the full layout identity would never match across an
   edit, since the edit changes the state. This is a measurement, not a guarantee for every cold
   layout; a pending caret can still show column 0 for a frame.

## Rationale

The busy loop and the blank frames come from the scan running ahead of demand, as the layout test
shows. Bounding the scan by what is asked for removes both. Whether it also cures the slow deep
lookups is measured, not assumed. A total row count is needed only by End, by a backward move into a
long preceding line, and by an exact count, and those can ask for it.

## Consequences

- An idle huge line costs no wrap work after its first frames. The ADR 0007 ingest of a huge file
  still publishes unchanged frames while it counts lines, about 40 a second on 64 GiB until the count
  lands. That is a separate follow-on.
- Scrolling far still converges across frames, at the same per-frame cost; it scans only toward its
  target.
- A scrollbar or "row N of M" on a huge line stays approximate until something asks for the exact
  count. ADR 0009 already says so.
- Resident pins, the last lead in ADR 0010's findings, may still matter for cold lines. They are
  measured after this change, not assumed.

## Resolved in implementation

- The previous-window fallback is not needed (decision 5), and neither is the caret projection cache
  (decision 6). Both are replaced by measurements.
