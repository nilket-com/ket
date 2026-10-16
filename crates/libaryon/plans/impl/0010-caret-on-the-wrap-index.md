# ADR 0010 implementation: caret on the wrap index

Decision: `plans/adr/0010-caret-on-the-wrap-index.md`.

Scope: caret byte↔column, horizontal, line and word motions, and edits inside huge lines, without a
resident prefix and past `u32` columns. Delivered as `impl: adr 0010 <slice>` commits, one per phase,
squashed into one impl before pushing. Each slice is reviewed and leaves the tree green. Converged with
Codex over chatd (conversation `startup-latency-followons`).

## Phases

### P1: 64-bit document columns

The first local slice, isolated for review and kept green.

- `DocDisplayCol` and `DocDisplayColCount` become `u64` in `core/coords.rs`. `DisplayOffset` is
  removed, and its call sites use `DocDisplayCol`.
- Narrowing happens only at document-to-local boundaries, through one checked helper. It returns
  `None` past the local type, and every caller handles that.
- `CursorPosition`, `PaneViewport.cursor_abs_pos`, command payloads, baryon and polariton.
- No behaviour change. The suites pass with their existing assertions, apart from widening the types
  in them. ADR 0009's `u32` scroll-placement stop and `VisibleRow::start_col` saturation stay as an
  explicit `CARET_COL_LIMIT`; P3 removes it with a test past the boundary.

### P2: cell lookups in the wrap index

- `WrapIndex::cell_for_byte(key, line, byte, source)`: the start byte and column of the cell holding
  a byte, or Unknown while it is pending. It returns no row.
- `WrapIndex::cell_for_col(key, line, col, source)`: the byte and column at which a column resolves,
  the cell start or past the cell for an interior column. It is a separate lookup, not a field of
  `ColLookup`, because a row start can sit inside a cell (a hard break within a tab or a wide cell)
  and so cannot start a walk. A caller wanting the row and the byte resolves the cell, then takes
  `row_for_col` at the resolved column.
- Both walk forward from the nearest checkpoint, which sits on a cell boundary, with a clone of its
  scanner that emits no rows.
- Both resume from the nearest checkpoint, with the existing replay liveness rules and the frame
  budget.
- **Equivalence and cell-rule tests:** generated lines (words, tabs, wide and combining graphemes,
  invalid bytes), chunked at random:
  - at every byte, `cell_for_byte` equals `display_col_for_byte_offset`: an interior byte maps to
    its cell's start column and byte;
  - at every column, the column lookup equals `byte_offset_for_display_col`: an interior column maps
    to the byte after its cell and the column after it;
  - the line end maps both ways;
  - byte→column→byte and column→byte→column round-trip at every cell start and at the line end.
    Interior positions are not round-tripped.

### P3: line/col on the index

- `CaretIndex` (layout) wraps the index for caret lookups. `None` means the line is not indexed
  and takes today's path; `Pending` means it is indexed but unreachable this frame, so the caller
  keeps its target and never reads the prefix instead.
- Caret resolution and placement take it: at navigation command entry, in `move_all_cursors`, in
  `add_cursor_at` and `add_cursor_adjacent`, in `handle_navigation_command` (vertical motion, clicks,
  scroll placement), and in render's caret resolution.
- Selections (`resolve_visual_ranges_sparse`) take it in render, their runtime caller: char cell spans,
  line ends and block columns come from the index on indexed lines. Edit callers pass `None` until
  P5.
- `CARET_COL_LIMIT`, ADR 0009's `u32` scroll-placement stop, is removed. The regression,
  `tests/caret_past_u32_test.rs`, scrolls a 5 GiB sparse line past `u32::MAX` columns, checks the
  caret is placed at the first visible row, and checks that Right moves it one cell. Scanning that far
  takes minutes in debug, so it is `#[ignore]`d and this ADR's gate runs it in release with
  `--ignored`.
- Wrap off: caret lookups key the line at the fixed width `u16::MAX`.
- A pending selection resolves to no ranges that frame and asks for the next; edits take the index in
  P5.

### P4: motions

- Left: the start of the cell holding the byte before the caret (`cell_at_byte`). Right: column + 1,
  which placement resolves to the next cell boundary, as `step_right_visual_col` does.
- LineStart: the line's start byte, with no read. FirstNonWhitespace and SmartHome read one extent
  from the line's start; with no non-whitespace cell in it, they act like LineStart.
- LineEnd: `CaretIndex::line_end`, known once the line's scan has finished. Until then the caret
  stays and the command sets `scan_to_end`; a later End moves.
- Word motions: `word_motion_target` reads a window of one extent beside the caret, for every line,
  instead of `read_loaded_document`. Its ends are aligned to UTF-8. A window with no boundary moves
  to its edge, and cold bytes are never a boundary: the motion requests the node, and the caret
  stays.
- One budget: a motion's window is granted from the frame's read budget (`WrapIndex::reserve_read`):
  at most one extent, and at most half of what is left, so the lookup that maps its target can still
  progress. A walk that runs out resumes on the next press. A multi-cursor command
  (`move_all_cursors`) is one frame for all its carets (`handle_navigation_command`'s `new_frame`).
- Carried work (`CommonViewState.carried`, `CarriedWork`): navigation hands render the nodes and the
  refresh its lookups asked for, plus `scan_to_end`, tagged with a `WorkKey` (root, state, topology,
  key width). Render's `carry_on` takes it each frame within the frame's budget: hydrate the nodes,
  refresh, advance the scan, and ask again while it is unfinished. Work for another key is dropped.
- Tests:
  - motions against the whole-line helpers on a cold-head indexed line;
  - End waiting while render scans over several frames, then a second End;
  - cold word and line-head windows requesting their nodes;
  - stale carried work being dropped;
  - with an 8 KiB budget, motion windows and their lookups staying within the frame's budget, and two
    carets sharing one frame.

### P5: edits

- `WrapIndex::carry_over(from, to, Option<LineEdit>)` re-keys the index across an edit.
  `LineWrap::truncate_at` keeps the checkpoints with `fed() <= offset` and the recent rows before the
  last of them, resets the frontier there, and clears the totals, the trailing whitespace, the
  whitespace walk, the replay and the cell walk. With no edit, or an index not valid for `from`, it
  clears.
- `UndoLedger::last_single_delta(from)`: the one delta the last entry took from `from` to the current
  state, if that is what it is. `single_line_edit` turns it into a `LineEdit` when it touches no
  newline.
- `handle_edit_doctab_command`:
  - begins an index frame and keys lookups before the edit by the revision the index was built for,
    since the topology bump at the command's start changes no bytes;
  - resolves its carets, and the visual yank, delete and change ranges, through the index;
  - carries the index over after the edit, resolves the caret through it, and carries any pending
    demand to render.
- `line_from_doc_byte_raw` counts a byte's line from the raw bytes of its leaf, so a leaf that is not
  valid UTF-8 never looks pending.
- Delete, change and yank by word motion (`word_motion_delta_at_cursor`) read the same bounded,
  budget-charged window as navigation's word motions, not the whole document. A cold window requests
  its node.
- Still whole-document, and out of this ADR: Put, JoinLine's next-line read, sort, uniq, and guarded
  host edits. They fail on a document past the snapshot limit, as before.
- Tests:
  - carried-over lookups equal a fresh index for inserts and deletes before, at and after checkpoints,
    with a stale replay and cell walk left under way;
  - an edit keeps other lines and what came before it;
  - newline edits, no edit and stale keys clear;
  - a command-level InsertChar, Delete and Backspace deep in a cold-headed line, each placing the caret
    within the command from the carried index;
  - the raw line lookup on bytes that are not valid UTF-8.

### P6: measurement and findings

On the 64 GiB sparse file: motion latency at depth, caret placement past `u32::MAX` columns, and RSS.
Findings in `plans/impl/0010-caret-on-the-wrap-index-findings.md`.

## Acceptance

1. On a non-resident line longer than 256 MiB and wider than `u32::MAX` columns, the caret is placed,
   displayed and moved (Left, Right, Home, End, word motions), and edits keep its column, with the
   line head evicted.
2. Byte→column and column→byte round-trip at every cell start and at the line end. An interior byte
   maps to its cell's start. An interior column (inside a tab or a wide cell) maps past its cell, as
   P2 specifies.
3. Every caret path stays within the frame read budget and the per-line memory cap.
4. Ordinary lines are unchanged: the existing cursor, motion, selection and edit suites pass with
   their assertions intact.

## Out of scope

Exact word wrapping for graphemes over 64 KiB. Persisting the index. Horizontal scrolling with wrap
off. A resumable word model in the scanner.
