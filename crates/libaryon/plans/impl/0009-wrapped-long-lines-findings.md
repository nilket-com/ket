# ADR 0009 implementation findings: wrapped long lines

Companion to `0009-wrapped-long-lines.md`. Records what the `impl: adr 0009` commit delivered, where it
departs from the plan, what it measured, and the limits left for ADR 0010.

## Delivered

- `engine/wrap_index.rs`:
  - `WrapScanner`, a chunked scanner equivalent to `compute_line_wrap`.
  - `WrapIndex`, keyed by `WrapKey { state, topology, text_width }`, owned by `CommonViewState`.
  - Resumable replay to `Target::Row` or `Target::Col`, a recent-row ring, and the trailing-whitespace
    origin and byte walk.
- `core/coords.rs`: `DisplayOffset` (`u64`) with one checked conversion to `DocDisplayCol`.
- `engine/layout.rs`:
  - `LineLayout::{Whole, Indexed}`, chosen by `WRAP_INDEX_MIN_LINE_BYTES` (one cold extent).
  - Fallible row, pan and clamp entry points (`try_visible_row`, `try_cursor_row_and_pane_col`, and the
    clamp's `Option<CursorTarget>`).
  - `LayoutDemand { pending, more }`, which drives continuation refreshes.
- Navigation calls `WrapIndex::begin_frame` once per command. Scrolling places the caret from the
  index (`Caret::place_at_known`) and keeps the previous view when it cannot place it (decision 9).
- Row window on the wire:
  - `PaneViewport.row_window: Option<RowWindow { line, start_byte, start_offset, tab_phase }>`.
  - `VisibleRow.start_byte` and `start_offset`.
  - `query_viewport_from_byte` projects from the window start, and highlights are clipped to it.
  - The line is suppressed when its first row is unreachable (decision 8).
- Frontends: baryon `draw.rs` and `mouse.rs`, and polariton `editor.rs`, lay out and hit-test from the
  window origin, with tab phase taken from `tab_phase`.
- Tests:
  - scanner equivalence over random chunkings;
  - store budget, replay and liveness cases;
  - `mod wrap_index_layout`;
  - `tests/row_window_test.rs` (sparse, long-string highlight, trailing whitespace, short lines);
  - command-level scrolling and caret cases in `runtime/navigation.rs`, including
    `scrolling_into_trailing_whitespace_places_the_caret_without_the_line_start`.

## Departures from the plan

1. **Checkpoints are spaced by input bytes, not rows.** One checkpoint every 64 KiB of input, which
   bounds replay by bytes read, the unit the frame budget counts. Over `line_bytes` (4 MiB per line),
   every other checkpoint is dropped, so memory stays bounded and a replay covers at most twice as
   far. This answers the ADR's first open question.
2. **The frame budget is per frame, not per call.** `begin_frame` restores one extent
   (`COLD_INGEST_EXTENT_SIZE`) shared by every lookup in the frame. Navigation needed its own
   `begin_frame` per command. Without it, scrolling plateaued once the first budget was spent.
3. **Caret placement was pulled into this ADR** (decision 9). Without it, the caret clamp needed a
   resident line prefix, and scrolling plateaued at the 64 MiB resident budget. `Caret::place_at_known`
   takes the row's byte from the index. Trailing-whitespace rows, which carry an offset but no start
   byte, take it from a resumable walk over the run (`WrapIndex::trailing_whitespace_byte`).
4. **Column 0 is always row 0.** Without this shortcut, the clamp replayed through an evicted line head
   to place a cursor that was already on the line's first row.
5. **Only the window's own line loses its inline annotation** (P4, made precise). The window's
   tokens start mid-line, and frontends place an annotation from its line's tokens counted from column
   zero (baryon `annotation_end_column`), so that line's end-of-line hint has no correct column.
   `window_inline_annotations` drops it. Annotations on later lines in the window are kept, because
   their tokens start at column zero. With a window, `render_rows` equals `visible_rows`, which
   answers the ADR's last open question.

## Measurements

Development machine, release `startup_probe` example (untracked), 100x30, wrap on, cold cache,
8 GiB memory cap. Startup times are three runs in ms. Each scroll run sends `ScrollViewport(rows)`
N times and times each resulting frame. "Deepest" is the furthest `row_window.start_offset`, in
display columns.

| File | First viewport | First text | Startup RSS |
| --- | --- | --- | --- |
| 1 GiB sparse, newline-free | 3.0–4.2 | 25.8–33.6 | 37 MB |
| 64 GiB sparse, newline-free | 52.1–58.7 | 73.1–79.3 | 63 MB |
| 50 MiB minified-JSON-like line | 2.3–2.9 | 33.1–37.9 | 66 MB |
| 50 MiB words, one line | 2.3–2.5 | 25.1–26.7 | 37 MB |

| File | Scroll | p50 / p95 / max ms | Deepest | Peak RSS |
| --- | --- | --- | --- | --- |
| 1 GiB | 1 row x300 | 12.2 / 20.2 / 25.2 | 11,316 | 108 MB |
| 1 GiB | 30 rows x300 | 11.9 / 19.4 / 22.2 | 332,100 | 104 MB |
| 1 GiB | 100,000 rows x1,200 | 10.0 / 18.2 / 28.1 | 512,753,626 | 107 MB |
| 64 GiB | 1 row x300 | 15.5 / 19.2 / 28.4 | 7,052 | 133 MB |
| 64 GiB | 30 rows x300 | 15.8 / 18.3 / 28.5 | 218,940 | 129 MB |
| 64 GiB | 100,000 rows x1,200 | 10.1 / 19.2 / 32.4 | 483,393,526 | 129 MB |
| 64 GiB | 100,000 rows x4,200 | 9.8 / 21.8 / 56.2 | 1,787,822,056 | 130 MB |
| 50 MiB JSON | 1 row x300 | 24.1 / 30.7 / 34.9 | 16,318 | 151 MB |
| 50 MiB JSON | 30 rows x300 | 16.4 / 29.5 / 36.1 | 489,540 | 155 MB |
| 50 MiB JSON | 100,000 rows x1,200 | 5.5 / 12.9 / 36.2 | 52,428,832 (end) | 101 MB |
| 50 MiB words | 1 row x300 | 9.7 / 19.4 / 21.8 | 17,738 | 89 MB |
| 50 MiB words | 30 rows x300 | 10.7 / 18.8 / 22.2 | 509,822 | 88 MB |
| 50 MiB words | 100,000 rows x1,200 | 5.7 / 15.6 / 29.7 | 52,428,750 (end) | 85 MB |

Readings:

- **Far scrolling is scan-bound, not seek-bound.** A far scroll advances at about 45 M columns/s on
  both sparse files: one extent per frame, then a continuation. The 50 MiB lines reach their end. Frame
  latency stays flat as depth grows, and RSS stays flat at about 130 MB however deep the view goes.
- **The fixed plateaus are gone.** During development, deep scrolling on 64 GiB stopped at about
  5 M columns (navigation spent no frame budget), then at about 69 M columns, with p50 53 ms (the
  caret clamp needed a resident prefix). After departures 2 and 3, the same 4,200-frame run reaches
  1.8–2.1 billion columns at p50 of about 10 ms.
- **Startup grows with file size on sparse files.** First viewport is about 3 ms at 1 GiB and about
  54 ms at 64 GiB. This was not investigated here and is recorded for the startup-latency
  follow-ons.

## Limits (ADR 0010 and later)

- **Caret columns are `u32`.** Scroll placement returns no target past `u32::MAX` display columns
  (about 4.29 billion), so scrolling keeps the view there. A 64 GiB line is about 64 billion columns,
  so this limit is reachable.
- Caret motion, edits and `line_col_from_doc_byte_sparse` inside a non-resident huge line keep today's
  limits (ADR consequences).
- A window that begins inside a string or comment highlights from the window start. Highlights can be
  wrong there; this is accepted.
- A grapheme longer than 64 KiB (`MAX_HELD_CELL_BYTES`) wraps as several cells.
- The row count of a line that is not fully scanned is unknown, so a scrollbar for it is approximate.

## Test baseline

These were flaky before this ADR and pass alone:

- `goto_definition_rejects_a_response_after_the_requesting_view_is_closed`;
- `local_theme_override_*`, a crash inside the rust-analyzer crate;
- `search_word_under_cursor_*`;
- the pty startup test: 1 failure in 8 runs at `origin/main`, 2 in 6 with this change.

`scripts/cargo-ctrl --locked` passes both lanes on both SVP backends. The architecture test, polariton
with `--no-default-features`, the long-line, latest-viewport and row-window integration tests, baryon's
tests and polariton's binaries also pass.
