# ADR 0010: move the caret onto the wrap index

Status: accepted 2026-10-16, converged by the KET council (user, Claude, Codex). Implementation
plan: `plans/impl/0010-caret-on-the-wrap-index.md`. Follows ADR 0009, which made huge wrapped lines
render and scroll but left caret math on today's paths.

## Context

ADR 0009 can show any row of a 64 GiB line, and scrolling places the caret there from the index. Every
other caret operation still needs the line's bytes, from the line start:

- `line_col_from_doc_byte_sparse` (`document/cursor.rs`) reads the line prefix up to the byte through
  `read_loaded_slice`, which has a 256 MiB limit and fails on a non-resident leaf. Selection,
  edits (`runtime/edit.rs`) and the anchor (`session.rs`) all go through it.
- Left and Right (`step_left_visual_col` and `step_right_visual_col`) and the line motions LineStart,
  FirstNonWhitespace, LineEnd and SmartHome read the whole line (`read_line_bytes_sparse`). Left and
  Right fall back to ±1 column when that fails. The line motions do nothing.
- Word motions (`next_word_start`, `prev_word_start`, `next_word_end`) read the whole document
  (`read_loaded_document`) and convert the result back with `line_col_from_byte_offset`.
- Document columns are `DocDisplayCol` (`u32`). ADR 0009 added `DisplayOffset` (`u64`) for render
  origins only, and scroll placement stops at `u32::MAX`, about 4.29 billion columns. A 64 GiB line is
  about 64 billion columns.

## Decision

1. **One 64-bit document column.** `DocDisplayCol` and `DocDisplayColCount` become `u64`.
   `DisplayOffset` folds into `DocDisplayCol`. `CursorPosition`, caret caches, preferred columns,
   command and view fields, and `PaneViewport.cursor_abs_pos` widen with it. Pane- and screen-local
   columns (`PaneLocalCol`, `cursor_pane_col`, `ScreenCol`, `CellCol`) stay narrow. A document column
   becomes a local one only after the visible row's origin is subtracted, through one checked
   conversion.
2. **The wrap index maps between bytes and columns, both ways, by cell.**
   - Byte to column: `cell_for_byte` walks from the nearest checkpoint and returns the start byte and
     column of the cell containing the byte.
   - Column to byte: `cell_for_col` returns the byte and column at which the column resolves.
   - The row, when a caller needs it, comes from the existing `row_for_col` at the resolved column. A
     row start can sit inside a cell, so cells and rows are separate lookups.
   - Both carry tab phase and UTF-8 carry. Each direction keeps the rule today's helpers use:
     - Byte to column (`display_col_for_byte_offset`): every byte of a cell (a tab, a wide or
       combining grapheme, an invalid sequence) maps to the cell's start column and start byte.
     - Column to byte (`byte_offset_for_display_col`): a column inside a tab or a wide cell maps to
       the byte after that cell, and the column after it.
     - The newline, or the line end, maps to the column after the final cell, and back.

   The two directions quantize interior positions differently, on purpose: a click or a vertical
   move inside a tab lands as it does on a short line. Round trips hold at cell starts and the line
   end only; no interior round trip is promised.
3. **Indexed lines take the index; other lines are unchanged.** `line_col_from_doc_byte_sparse`,
   `place_cursor_at_line_col`, vertical motion, and mouse and command targets consult the index for
   lines on ADR 0009's indexed path (`WRAP_INDEX_MIN_LINE_BYTES`, or not fully resident), with wrap on
   or off. With wrap off, the index entry uses the fixed width `u16::MAX`. Every other line keeps
   today's code, byte for byte. An indexed line whose lookup is pending never falls back to a prefix
   read and never moves the caret; it keeps its target until the lookup resolves.
4. **Horizontal and line motions read cells near the caret, not the line.**
   - Left and Right step one cell from the caret's byte. That needs at most one held cell
     (`MAX_HELD_CELL_BYTES`) on either side, and only Left needs the column, from the index.
   - LineStart is column 0.
   - LineEnd is the index's line end. The scanner already records the line's total once it finishes.
     Until then, LineEnd waits in place: the caret stays put, it never presents the scan frontier as
     the end, and render scans the line to its end in the background. Pressing End again once the
     scan has finished moves the caret. There is no queued pending-End intent.
   - FirstNonWhitespace and SmartHome scan forward from the line start for at most one read budget.
     Past that, they act like LineStart.
5. **Word motions stay out of the index.** Word boundaries keep `next_word_start` and friends. Those
   functions run on a bounded window of bytes beside the caret instead of on the whole document: at
   most one extent per press, in the direction of motion, with both window ends aligned to UTF-8
   boundaries.
   - If the window holds no boundary, the caret moves to the window's edge, and the next press
     continues.
   - Data that is not resident or not available is not a boundary. The motion requests hydration and
     keeps the caret this frame. Only a window that was fully read can move the caret to its edge.

   The resulting byte maps back through decision 2. The scanner gains no word model.
6. **Edits recompute from the index, which survives them.** After an edit inside an indexed line, the
   caret's column comes from its byte through decision 2. The acting view carries its index over the
   edit (`WrapIndex::carry_over`) when the command made one delta that adds or removes no newline:
   - every other line's entries are unchanged, since their line-relative bytes are;
   - the edited line keeps the checkpoints fed only from bytes before the edit, and the rows they had
     emitted, and scans on from the last of them;
   - everything derived from later bytes is dropped: the totals, the trailing whitespace, and any
     replay or cell walk under way.

   The caret's column after a deep edit is then one walk from the checkpoint before it, not a rescan
   from the line's start. Anything else clears the index, as before: a newline edit, an undo or redo,
   several deltas, or an index not valid for the state the edit started from. Other views of the same
   buffer clear theirs too, and rescan when they next look.
7. **One budget.** Byte lookups, cell steps and motion windows spend the same per-frame read budget
   (`begin_frame`) and the same per-line memory cap as rendering and scrolling. No caret path reads
   unbounded bytes.

## Rationale

One coordinate width avoids two caret paths that must agree at the `u32` boundary. Pane-local columns
are bounded by the screen, so they need not widen. A byte target is exact where a column target is
quantized to row starts. Composing either direction from row lookups would still walk a row's bytes,
so both return the containing cell. Word semantics are language-shaped and change independently of
wrapping. A bounded window keeps them simple at the cost of multi-press motion across very long
words.

## Consequences

- A wide, mechanical diff: about 420 `DocDisplayCol` references in libaryon, plus baryon and
  polariton. Checked conversions replace `as u32` casts at the pane and screen boundaries.
- `CursorPosition` and `PaneViewport.cursor_abs_pos` change width on the public API. A third
  frontend must accept `u64` document columns.
- A word longer than one read budget (1 MiB) takes more than one press to cross.
- Only the acting view carries its index over an edit. Another view of the same buffer rescans an
  edited huge line from its start, bounded per frame.
- LineEnd on a line that is still being scanned leaves the caret in place and asks render to scan the
  line to its end in the background. End must be pressed again once that scan has finished; there is
  no queued End.
- A lookup reads from the nearest checkpoint before its target, at most one checkpoint gap away (64
  KiB, up to a few MiB once thinned). That read can need one earlier leaf hydrated, never the line
  from its start.
- Caret math on ordinary lines does not change.

## Resolved in review

- LineEnd on a line that is still being scanned waits in place, and repeating End resolves it
  (decision 4).
- The word window is at most one extent per press (decision 5).
- The column widening lands as the first slice under this ADR, not as its own ADR.
