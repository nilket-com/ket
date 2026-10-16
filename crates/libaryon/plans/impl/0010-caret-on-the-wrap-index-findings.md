# ADR 0010 implementation findings: the caret on the wrap index

Companion to `0010-caret-on-the-wrap-index.md`. Records what the `impl: adr 0010` commit delivered,
where it departs from the plan, what it measured, and what is left for ADR 0011.

## Delivered

- **64-bit document columns.**
  - `DocDisplayCol` and `DocDisplayColCount` are `u64`, and `DisplayOffset` is folded into them.
  - The column helpers in `core/text.rs`, the wrap layout, intra-line diff columns, the status bar,
    and click mapping in baryon and polariton are all 64-bit.
  - Pane and screen columns stay narrow.
  - ADR 0009's scroll stop at `u32::MAX` columns is gone.
- **Cell lookups in the wrap index.**
  - `cell_for_byte` and `cell_for_col` walk from the nearest checkpoint, using a row-free clone of its
    scanner, within the frame budget, and resume across frames.
  - They keep the rules of `display_col_for_byte_offset` and `byte_offset_for_display_col`.
- **Caret placement on the index (`CaretIndex`).**
  - `None` means the line is not indexed and takes today's path; `Pending` means the caller keeps
    its target.
  - Used by caret resolution and placement at every navigation entry point: multi-cursor moves,
    adding cursors, clicks, vertical motion, scroll placement and render.
  - Selections use it too, and they wait while a lookup is pending.
- **Motions.**
  - Left takes the start of the cell before the caret; Right takes the column plus one.
  - Home goes to the line's start byte.
  - End is known once the line's scan has finished; until then render scans on (`scan_to_end`), and
    a later End moves.
  - FirstNonWhitespace, SmartHome and word motions read a bounded window from the frame budget
    (`reserve_read`), and cold bytes are never a boundary.
- **Carried work.** Navigation and edits hand render the nodes and refreshes their lookups asked for,
  plus line scans (`CarriedWork`, keyed by `WorkKey`). Render takes them each frame (`carry_on`).
- **Edits keep the index.**
  - `WrapIndex::carry_over` re-keys the index across one newline-free delta. The edited line keeps
    what it learned before the edit and drops everything derived from later bytes.
  - Edit commands begin an index frame. Their resolves, visual yank, delete and change ranges, and
    word-motion deletes use the index.

## Departures from the plan

1. **Cells and rows are separate lookups.** `ColLookup` gained no byte. `cell_for_col` stands alone,
   and a caller that needs the row takes `row_for_col` at the resolved column. A row can start inside
   a cell, so a row start cannot begin a walk.
2. **Column to byte keeps today's rule inside a cell.** A column inside a tab or wide cell resolves
   past the cell, as `byte_offset_for_display_col` does, not to its start. A click inside a tab
   therefore lands the same on a short line and on an indexed one. Round trips hold at cell starts
   only.
3. **The `u32` stop was removed in P3, not P1.** P1 kept it as an explicit `CARET_COL_LIMIT`, so the
   type widening changed no behaviour.
4. **End needs a background scan, and carried work brings one in.** The plan had End resolving on
   repeated presses, but nothing scanned a line to its end. Render now scans a requested line within
   its budget. The caret still moves only on a later End.
5. **Motion windows are charged to the frame budget, at most half of what is left,** so the lookup
   that maps the target can still progress. A multi-cursor command is one frame.
6. **Decision 6 was amended: edits carry the index over.** Under the accepted text, a keystroke 4 GiB
   into a line would have meant rescanning 4 GiB.
7. **Bugs found and fixed on the way:**
   - edit commands never began an index frame, so their lookups ran on another frame's leftovers;
   - the line of a byte was read through UTF-8 text, so a resident leaf that is not valid UTF-8
     looked pending forever (now `line_from_doc_byte_raw`);
   - placement used to re-run `find_node_at_line_col` after a failure, a legacy prefix walk.
8. **Still whole-document, out of scope:** Put, JoinLine's next-line read, sort, uniq, and guarded
   host edits.

## Measurements

Development machine, release `startup_probe` example (untracked), 100x30, wrap on, cold cache. Each
motion is timed from sending the command to the first viewport whose caret moved, after the view has
settled.

| Line | Depth | Right | Left | Next word | Prev word | Insert | Backspace | Home | Peak RSS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50 MiB words | 52 M cols | 7.2 | 11.4 | 9.2 | 8.2 | 11.3 | 9.1 | 9.1 | 86 MB |
| 1 GiB sparse | 224 M cols | 4.6 | 8.5 | 11.6 | 9.3 | 23.5 | 24.5 | 0.1 | 107 MB |

Times are in ms.

- On the sparse line, a word motion moves 512 KiB, half the frame budget: the line is one run of one
  character class.
- End, pressed once a second until it lands: the 50 MiB line takes 3 presses (3 s) and the 1 GiB line
  31 presses (31 s). Each lands exactly on `(len, width)`.
- Past `u32::MAX`: `tests/caret_past_u32_test.rs` (release, `--ignored`, 5 GiB sparse) scrolls past
  4.36 billion columns, places the caret at the first visible row, and moves it with Right, Left, a
  word motion and Home. It runs in about 160 s, almost all of it scanning.

## Unresolved: deep lookups on a 64 GiB line during an active scan

On the 64 GiB sparse line, motions and edits deep in the line do not resolve within 30 s, past
`u32::MAX` columns. At 50 million columns they take 3 to 4 s. The deep scroll itself works: 11,000
frames reached 5.66 billion columns at p50 13.9 ms.

An ADR 0009 behaviour, present on `origin/main` as well, is in play. With no input, a huge visible
line keeps rendering until its whole scan finishes. `row_window` alternates between present and
suppressed on every frame (decision 8), so the line blanks every other frame:

| Build | Idle frames in 8 s, 64 GiB |
| --- | --- |
| `origin/main` | 555, 556 |
| HEAD | 506, 516 |

That can last about 13 minutes on 64 GiB. The 50 MiB line does the same until its scan finishes;
small files render no idle frames. The cause of the slow deep lookups is not established.

A lead for ADR 0011, from Codex: when the origin is unavailable, render publishes no tokens and
replaces the pane's resident pins with an empty set, so the visible leaves may become evictable
between frames. ADR 0011 takes the investigation and the fix:
- a scanning line stays drawn;
- the whole-line scan stops when nothing needs the row count;
- deep caret lookups progress under scanning;
- the viewport shows the last known column while the caret resolves (today it shows column 0).

## Test baseline

`scripts/cargo-ctrl --locked` passes both lanes on both backends, apart from known flakes:
- `rune_source_reload_is_atomic_and_unload_retires_the_generation` (a status race under load);
- the rust-analyzer panic in the semantic reactor ("Unable to get `FileSourceRootInput` ... this is
  a bug"), seen in `open_line_below_full_engine_grafted_rust_file` and
  `persist_langtheme_rejects_invalid_theme_without_file_write`.

Each passes alone. The architecture test, polariton with `--no-default-features`, the long-line,
viewport and row-window tests, baryon's tests, polariton's binaries, and the release past-`u32` test
all pass.
