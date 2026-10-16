# ADR 0007 implementation: long-line projection

Decision: `plans/adr/0007-long-line-projection.md`.

Scope: bound what a render projection discovers, hydrates and publishes for a line longer than one
cold extent, and correct where such a line starts. Render queries stop taking leaves for a line after
one extent and resume at the next line; an unwrapped pane then publishes each line only to its right
edge plus one cell, at true absolute bytes. No horizontal scrolling and no wrap-on layout change.
Follows the post-open memory diagnostic recorded in `startup-latency.md` section 10. Reviewed with
Codex over chatd (conversation `startup-latency-followons`). Deliverable is one `impl: adr 0007` commit
reviewed against this plan.

## 1. Problem

Three defects, each hiding the next.

**1.1 A line spanning unloaded leaves is projected from its last leaf.** In
`query_viewport_internal` (`uast/projection.rs`) phase 1, a leaf with
`target_line == node_end_line` is skipped when `line_starts_at_node_end(text, target_line_in_node)`.
An unloaded leaf resolves to empty text, and `line_starts_at_node_end(&[], 0)` is true (offset 0 equals
length 0). For line 0 of a newline-free file every unloaded leaf is therefore skipped up to the last
one, the only leaf without a next node. The first frame hydrates and pins that leaf, so the pane shows
the file's last extent instead of its head. Every earlier measurement of a sparse fixture, including
the 1 MiB token, was of the final extent.

**1.2 Fixing 1.1 exposes a walk to end of file.** Phase 2 takes leaves until it has collected
`line_count` newlines. Starting at leaf 0 of a newline-free line it walks every remaining leaf, and
`render_pane_viewport` requests hydration of each unloaded visible token it returns
(`engine/render.rs`, the loop over `visible_tokens`). Measured with 1.1 fixed and no bound: a 1 GiB
sparse newline-free file never produced a ready frame within 45 s, in either wrap mode. At 64-200 GiB
it would try to load the file. The highlight-context query (`scroll_line - 60`, `viewport_lines + 120`)
walks the same way every frame; it hydrates nothing but visits and clones every leaf.

**1.3 Each frame publishes whole leaves.** Projection copies each leaf whole
(`resolve_physical_bytes`), so with wrap off a pane 100 columns wide carries up to 1 MiB of one line.
Every frame then hashes it (`render.rs` viewport hash), clones it into the `Viewport`, and both
frontends walk every grapheme of it. There is no horizontal scroll: with wrap off,
`cursor_row_and_pane_col` (`engine/layout.rs`) returns the document column unclamped and nothing
offsets the view, so columns `[0, text_width)` are the only ones a pane shows.

## 2. Change

### 2.1 Seek

In phase 1, skip a leaf at the target line's end only when `target_line_in_node > 0`: a line begins at
a leaf's end only after a newline inside that leaf. For `target_line_in_node == 0` the line began at
or before the leaf's start, and a non-empty leaf holds part of it (zero-length leaves are skipped
earlier).

### 2.2 Bounded discovery for render queries

A render-only inherent method, `UastRegistry::query_viewport_with_line_discovery`, takes an explicit
crate-private `LineDiscovery` policy. The `UastProjection::query_viewport` trait method and its 22
non-render callers stay byte-identical and unbounded (`LineDiscovery::Unbounded`).
`render_pane_viewport` calls the new method with `LineDiscovery::PerLineBytes(COLD_INGEST_EXTENT_SIZE)`
for both the visible and the highlight-context query, in both wrap modes.

Under a per-line cap, phase 2 counts each line's bytes from its start, including unloaded leaves by
their metric length. Once a line has reached the cap, it takes no further leaf for that line. A leaf
beyond the cap is never returned, so it is never hydrated, pinned, highlighted or published.

The next line is seekable only from exact metrics. Before the root's line metrics are complete (a
metric scan pending, or the root marked incomplete: `inspector_root_metrics_incomplete`), the byte at
which the next line starts is unknown, so the projection stops at the capped line. Once they are
complete, the query resumes with a phase 1 descent for the next line and continues until `line_count`
lines are collected or the document ends. A line boundary is never inferred from an extent boundary,
and nothing scans forward past the cap to find one.

One extent is a byte bound on discovery, not a display-cell bound: an unbounded combining or ZWJ
sequence can make a single cell longer than any fixed byte count. A line whose first visible cells need
more than the cap publishes the bounded prefix. That is a documented rendering limit, not a claim that
the cap covers every visible cell.

With wrap on, a line longer than the cap is projected as its first extent. Wrap layout is unchanged: it
still reads the whole line through `read_line_bytes` and falls back to one row when the line is not
resident. Correct wrapped rendering of such lines needs a row/window protocol and is a separate plan.

### 2.3 Exact clip with wrap off

After coalescing, a pure helper beside `coalesce_frontend_text_tokens` clips each line of the visible
tokens at `text_width + CLIP_LOOKAROUND_COLUMNS` display columns, using
`core::text::byte_offset_for_display_col` with `TAB_SIZE`, as layout does. It runs after coalescing so a
cut falls on a grapheme boundary of valid text; a scalar can straddle leaf tokens before then.
Hydration requests and resident pins still see the uncoalesced, unclipped tokens.

1. Track the display column since the last `\n`; keep bytes while it is below the limit. A wide cell or
   tab straddling the limit is kept whole.
2. Drop bytes from the cut to the next `\n`, and resume at that `\n` as a new token whose
   `absolute_start_byte` is the source token's start plus the newline's offset. The newline belongs to
   the clipped line, so the resumed token keeps that line's `absolute_start_line`.
3. A clipped token's `physical_byte_len` is the span of its published text.
4. Folded tokens are untouched; a zero-width text area keeps one cell per line.

`CLIP_LOOKAROUND_COLUMNS` is 1. With no horizontal scroll and whole cells kept, one cell past the edge
is enough for the present renderers, and it puts an end-of-line annotation (`annotation_end_column`,
baryon `ui/draw.rs`) off-screen rather than inside the pane. It is not a continuation signal.

### 2.4 Baryon byte positions

Clipped tokens are not byte-contiguous. Baryon's pane drawing advanced its byte cursor only by the
bytes it painted and never resynced to `token.absolute_start_byte` except after folds, so a gap would
shift highlights, selections and the cursor on every later line. Each text token starts its byte cursor
at its own `absolute_start_byte`. This is identical for contiguous tokens: `global_start_byte` is the
first token's start. Polariton's `project_lines` already resyncs per token.

## 3. Verification

1. **Seek, unit**: three unloaded byte leaves of one line; `query_viewport` for line 0 starts at byte 0.
2. **Discovery, unit**: a line of unloaded leaves longer than the cap followed by short lines; a capped
   query returns no leaf past the cap; with exact metrics it then returns the following lines at their
   true bytes and line numbers, and with the root's metrics incomplete it stops at the capped line;
   `Unbounded` returns every leaf as before.
3. **Clip, unit**: ASCII, tab, wide CJK, combining mark and invalid-byte lines clip at the expected
   byte; the column carries across tokens of one line; a token wholly past the limit is dropped; lines
   after a clip keep their absolute bytes; folded tokens are untouched; zero-width pane.
4. **Engine regression, `tests/long_line_projection_test.rs`**, 40x10 pane:
   - newline-free 1.5 MiB file, wrap off: published text at most 41 bytes, starting at byte 0 with the
     file's head;
   - a long line between short ones, after the scan completes: the line after it is published at its
     true absolute byte;
   - newline-free sparse 1 GiB file, both wrap modes: a ready frame arrives within the deadline, and no
     published token starts at or past one extent.
5. **Frontends**: baryon and polariton suites pass; `startup_latency_pty_test` passes.
6. **Full gate**: `scripts/cargo-ctrl --locked`.
7. **Measurement, in the impl commit's findings note**: 64 GiB sparse, wrap off and wrap on, probe
   draining and not draining: frame text bytes, anonymous memory over a 15 s hold, and per-frame render
   cost. The 83-byte frame applies to wrap off only; wrap on still publishes up to one extent per line.

## 4. Out of scope

- Horizontal scrolling. If added, the clip window becomes `[scroll_col, scroll_col + width + 1)` with a
  leading cut; the absolute-byte bookkeeping is the same.
- Wrap-on rendering of lines longer than one extent, including layout's whole-line read: its own plan.
- The unbounded viewport channel (latest-wins delivery): its own plan, next.
- Frames emitted for an unchanged projection during scans: measured after the channel fix.
