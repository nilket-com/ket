# ADR 0009: wrap long lines through a lazy wrap index

Status: accepted 2026-10-16, converged by the KET council (user, Claude, Codex). Implementation plan:
`plans/impl/0009-wrapped-long-lines.md`. Follows ADR 0007, which
deferred wrapped rendering of lines longer than one extent. Scope gate: rendering and scrolling only;
caret motion within such lines is ADR 0010.

## Context

Soft wrap is on by default. Every wrap step today works on a whole physical line from display
column 0:

- `LayoutContext::line_wrap` (`engine/layout.rs`) reads the complete line through `read_loaded_slice`.
  That has a 256 MiB limit and fails if any leaf is not resident. On failure it wraps `b""`, so a line
  longer than its resident leaves reports one row to visible-row collection, anchor normalisation,
  scrolling and cursor metrics alike.
- The wrap cache lives for one layout call, eight entries, and the anchor line is re-read and
  re-wrapped several times per frame.
- `WrappedLine` holds one start column per row: about ten million for a 1 GiB line at 100 columns.
- The render query (ADR 0007) publishes the anchor line from its first byte, capped at one extent.
  Both frontends replay that line from column 0 to find the visible rows (baryon `paint_text_segment`,
  polariton `wrap_line_at_starts`), and expand tabs against the absolute column.
- The sum tree's `visual_rows` field exists but production stores 1 per line.

The result: a wrapped line longer than one extent shows one row of its first extent, and the view
cannot scroll into it.

## Decision

1. **Wrap semantics do not change, up to one bound.** Word wrap with hard breaks for long words, tabs
   against the absolute column, whole display cells. No character-wrap or byte-row mode is substituted for
   long lines. The one bound: a single display cell (grapheme cluster) longer than 64 KiB
   (`MAX_HELD_CELL_BYTES`) is not held whole. Past that size the scanner treats the bytes it has as a
   complete cell, ending it at a Unicode scalar boundary, and continues with the next scalar as a new
   cell. Such a grapheme may therefore wrap differently from `compute_line_wrap` on the whole line.
2. **A lazy per-line wrap index.** For a line wider than its resident text can cheaply answer, the
   engine keeps a `WrapIndex`. It scans forward from the line start only as far as the requested rows,
   and stores checkpoints. Each checkpoint is the scanner's complete state at a committed row start:
   absolute byte, display offset, row number, and the physical and display positions of any
   unresolved word suffix and UTF-8 or display-cell carry at an extent boundary. A byte and column
   pair alone cannot resume word wrap. Sequential scrolling continues from the last checkpoint, and a
   seek resumes from the nearest one at or before the target row.
3. **A row start is committed only once it is stable.** A word that starts near the right edge can
   later force a break at its own start. So the scanner commits a row start only after reading through
   the current word to whitespace, or until the word is known to exceed the remaining row width. That
   lookahead is bounded by the text width plus one display cell, never by the word's length. A published
   row is never retracted.
4. **The index is a cache, not state.** It is owned by the buffer view, replacing today's
   per-layout-call eight-entry wrap cache, which cannot survive the repeated layout contexts within one
   frame. It is keyed by buffer revision, text width, tab size and wrap mode, and invalidated when any of
   them changes. It is bounded by an entry and byte budget with eviction, and never persisted. A row
   count beyond the scanned prefix is reported as unknown, not as one.
5. **Render offsets are 64-bit.** A 200 GiB line overflows `DocDisplayCol` (`u32`) before it is indexed.
   The wrap index, its checkpoints and the wire row origin use a distinct `u64` display-offset type.
   Frontends need only the origin's value modulo the tab size and their local `u16` row positions. The
   caret keeps `DocDisplayCol` until ADR 0010, converting at one explicit boundary that reports
   out-of-range rather than truncating.
6. **A row window on the wire.** For rows of indexed lines, `VisibleRow` gains its absolute start byte
   and its absolute 64-bit display offset. `start_col` keeps its meaning, an absolute document column,
   saturating past `u32`. When the view starts inside an indexed line, the render query asks for the
   byte window from the first visible row's start byte instead of the line from its first byte. Tokens
   then begin mid-line, and the pane carries the window's origin (`row_window`): line, start byte,
   absolute display offset, and that offset modulo the tab size. A row of trailing whitespace has no byte
   of its own: a window that starts at such a row starts at the whitespace run, which the index records
   when it ends the line, and its origin is the run's byte paired with the run's display offset. Each
   row keeps its own absolute offset; only the window's origin moves to the run. Without a
   window, every existing field means what it means today.
7. **Frontends lay out from the window origin.** Baryon and polariton start their display column at
   the origin, not 0, so tab expansion and row breaks match the engine. A row's position within the
   window is its absolute offset minus the origin's; nothing subtracts the origin twice. Clicks map
   through the same coordinates.
8. **No row is published from a guessed origin.** If the first visible row of an indexed anchor line
   cannot be reached this frame, render publishes no rows and no tokens for that line, so no frontend
   lays it out from its first byte, and asks for a continuation. A later frame publishes the window.
9. **Scrolling places the cursor from the index.** Scrolling moves the cursor to keep it in view. When
   it moves the cursor to a row of an indexed line, it places the cursor from that row's start byte and
   absolute column, without reading the line from its start, so the depth of a scroll does not depend on
   how much of the line is resident. This works within the caret's `u32` column range; past it the scroll
   keeps the view, until ADR 0010 widens the caret's coordinates. If the cursor's new place cannot be
   reached this frame, the scroll keeps the view for that command rather than move the cursor somewhere
   guessed.
10. **Discovery stays bounded, and scanning stays live.** A window is at most the visible rows' bytes
   plus one extent of lookahead. ADR 0007's per-line cap still applies to the scan behind a window: the
   index advances at most one extent per frame, so a far jump reaches its row over several frames
   rather than in one. While a requested row remains unscanned, the engine schedules a continuation
   refresh, so an unknown target always becomes known without further input.

## Rationale

Keeping word-wrap semantics avoids a hidden mode in which cursor, copy and visual rows behave
differently once a line passes a size. Complete scanner checkpoints make every resume exact. The
engine already owns layout, so a window origin on the wire is the smallest frontend change that stops
both frontends from replaying the line from column 0.

## Consequences

- Scrolling far into a huge wrapped line is bounded per frame. Until the index has scanned that far, the
  view shows the furthest scanned row and the row count is unknown. A scrollbar or "row N of M" for such
  a line is approximate.
- Caret behaviour is otherwise retained as it is today: within the `u32`-representable column range,
  and, except for the scroll placement above, only where the line prefix is resident and under 256 MiB.
  An edit then recomputes the caret's column from the prefix, so editing deep inside a non-resident huge
  line still fails until ADR 0010. Past that, motion inside such a line still
  fails, and this ADR does not claim that arbitrary deep edits work. ADR 0010 moves caret column math,
  horizontal, word and line motions, and `line_col_from_doc_byte_sparse` onto the same index, and
  migrates the caret's coordinates.
- Both frontends change their layout entry point. A third frontend must honour the window origin.
- Lexical highlighting of a window starts at the window, not at the line's start. A window that begins
  inside a string or comment has lost its opening delimiter and may colour the rest of the window
  wrongly. Preserving lexer state across a skipped prefix of up to 200 GiB is out of scope. Highlight
  spans stay absolute document bytes, at or after the window start.
- A frame whose first visible row is unreachable shows that line blank until the next frame.
- The index costs memory per huge visible line, bounded by its budget: checkpoints and recent rows
  within `line_bytes`, plus working memory for the frontier's and one replay's held cell
  (`WORKING_BYTES`).
- A grapheme longer than 64 KiB is wrapped as several cells. Only pathological text has one, typically
  a base character under tens of thousands of combining marks. Holding it whole would make the
  scanner's memory unbounded; an incremental grapheme-and-width state would have to reproduce
  `unicode-width`'s cluster rules exactly. The cap keeps memory bounded at the cost of exact wrapping
  for that text only.

## Open Questions

- Checkpoint spacing: every row, or every K rows with replay? Measure memory and seek latency on a
  1 GiB line.
- Whether ordinary short lines keep today's whole-line path (likely) or go through the index too.
- How `render_rows` (today including rows scrolled off the top) and inline annotations behave in a
  window.
