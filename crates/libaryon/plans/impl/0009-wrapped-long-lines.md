# ADR 0009 implementation: wrapped long lines

Decision: `plans/adr/0009-wrapped-long-lines.md`.

Scope: render and scroll a soft-wrapped line of any length through a lazy, bounded wrap index, with a
row window on the wire and both frontends laying out from its origin. Caret motion inside such lines
keeps today's limits (ADR 0010). Delivered as a sequence of `impl: adr 0009 <slice>` commits, one per
phase below. Each is reviewed against this plan and leaves the tree green. Converged with Codex over
chatd (conversation `startup-latency-followons`).

## 1. Which lines take the new path

A line takes the wrap index when it is longer than one cold extent (`COLD_INGEST_EXTENT_SIZE`), or
when its bytes are not all resident. Every other line keeps today's whole-line `compute_line_wrap`,
byte for byte, so ordinary files see no change. The threshold is a single named constant.

## 2. Phases

### P1: incremental wrap scanner and `DisplayOffset`

- A `u64` `DisplayOffset` newtype in `core/coords.rs` for line-relative display positions past
  `u32`. There is one checked conversion to `DocDisplayCol`, which returns `None` out of range.
- A `WrapScanner` in `engine/layout.rs` (or a new `engine/wrap_index.rs`) that consumes a line's bytes
  in arbitrary chunks and emits committed row starts under the ADR's stability rule. Its resumable
  state is the checkpoint type: absolute byte, display offset, row, unresolved word suffix, and
  UTF-8/cell carry.
- **Equivalence test:** for generated lines (words, long words, tabs, wide and combining graphemes,
  invalid bytes) at several widths, feeding the scanner in random chunk sizes yields exactly
  `compute_line_wrap`'s starts. This pins "wrap semantics do not change".

### P2: `WrapIndex` store

- The index is owned by the tab's view state and replaces the per-`LayoutContext` eight-entry cache.
  It is keyed by `(line, buffer revision, text width, TAB_SIZE, wrap)`.
- Checkpoints are kept every K rows (start with K = 64; measure, per the ADR's open question). A seek
  replays from the nearest checkpoint.
- Budget: a fixed entry and byte cap, with eviction of least-recently-used lines. Invalidation follows
  the key.
- Scanning reads one extent per call through the resolver's existing hydration. A target past the
  scanned prefix returns `Unknown { scanned_rows }` and asks for a continuation refresh.

### P3: layout and scrolling

- `line_row_count`, `row_start_col`, `collect_visible_rows`, `normalize_anchor`,
  `scroll_viewport_anchor`, `rows_from_anchor_to_position` and the pan loop consult the index for
  new-path lines.
- An unknown row count stops forward scrolling at the scanned prefix for this frame instead of wrapping
  to one row. Scrolling up into an unknown line lands on its last scanned row.
- `VisibleRow` gains `start_byte: DocByte` and a `DisplayOffset` origin.

### P4: row-window projection

- A render-only query returns tokens for the byte window from the first visible row's start byte,
  covering the visible rows plus at most one extent of lookahead. It uses ADR 0007's
  `query_viewport_with_line_discovery` machinery, starting from a byte instead of a line.
- `PaneViewport.row_window: Option<RowWindow { line, start_byte, start_offset, tab_phase }>`, present
  only when the view starts inside an indexed line. With it, `render_rows == visible_rows` and
  `viewport_row_offset == 0`. Without it, every field is unchanged.
- `VisibleRow` gains `start_byte: Option<DocByte>` and `start_offset: Option<DisplayOffset>`, both
  absolute, set for rows of indexed lines. `start_col` stays absolute.
- Trailing-whitespace rows take the whitespace run's start, recorded by the index when it finishes
  the line, as their origin.
- If the anchor line is indexed and its first visible row is unreachable this frame, that line
  publishes no rows and no tokens, and demand asks for a continuation.
- Highlighting runs over the window from its start (ADR consequence); a test pins that spans are
  absolute and at or after the window start.
- Inline annotations on the window's line are dropped: its tokens start mid-line, so no frontend
  can place its line-end hint. Later lines in the window keep theirs. Listed in the findings.

### P5: frontends

- **Baryon** `draw.rs`: `paint_text_segment` and the token walk start `current_display_col` from the
  window origin, and rows break on `VisibleRow` starts relative to it. Tab width uses the origin's
  modulo value.
- **Polariton** `project_lines` and `wrap_line_at_starts`: the same origin. `wrap_line_at_starts` gains
  a right bound at the next row's start; today it has none.
- Clicks keep using `VisibleRow.start_col`, plus the origin.

### P6: measurement and findings

Covers memory, the first wrapped frame, and scroll latency on a sparse 1 GiB and a 64 GiB
newline-free file, plus a 50 MiB minified-JSON-like line. Findings in
`plans/impl/0009-wrapped-long-lines-findings.md`.

## 3. Acceptance

Agreed with Codex:

1. The first wrapped frame of a sparse multi-extent newline-free file shows that file's head as wrapped
   rows filling the pane.
2. Row-by-row scrolling continues past the first extent with no gap or duplicate row. Page scrolling
   and a far jump converge through continuation refreshes.
3. Word, tab, wide-grapheme and invalid-byte boundaries that straddle an extent boundary wrap exactly
   as `compute_line_wrap` would wrap the whole line (the P1 equivalence, plus an engine-level case).
   A grapheme under `MAX_HELD_CELL_BYTES` (64 KiB) that straddles many reads wraps exactly as well.
   A grapheme over it completes, its line still wraps, and the index's memory stays within
   `line_bytes + WORKING_BYTES` after every call, including while that grapheme is held.
4. Clicks and the displayed caret are correct at a nonzero row start in baryon and polariton, within
   the `u32` column range.
5. Index memory and resident bytes stay within their budgets while scrolling through a 1 GiB line.
6. Ordinary files are unchanged: the existing wrap, layout, draw and polariton suites pass without
   edits.

## 4. Out of scope

Caret column math, horizontal, word and line motions on huge lines, and `DocDisplayCol` migration
(ADR 0010). Horizontal scrolling with wrap off. Persisting the index.
