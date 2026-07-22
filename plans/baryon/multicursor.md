# Specification: Point Multicursor Editing

## Overview

Add point-based multicursor editing to text documents. The first delivery supports both ways of
creating cursors requested for the feature:

- `Alt-j` / `Alt-k` add a cursor on the next / previous visible document line at the same preferred
  display column.
- `Alt` + left click adds a cursor at an arbitrary visible document position.

Typing, paste, newline, tab, backspace, forward delete, `x` / `X`, `r`, and `~` apply at every
cursor as one transaction. Point motions move every cursor. One cursor remains the **primary**
cursor: it owns the terminal's hardware cursor, status-bar position, and viewport panning. Adding a
cursor makes the new cursor primary, which makes repeated `Alt-j` / `Alt-k` extend naturally.

This is engine state, not a frontend simulation. Each document tab retains its own cursor set,
semantic grafts resynchronize every cursor, and undo / redo restores both text and the cursor set.

Multiple visual selections, multicursor operators (`dd`, `dw`, `ciw`, put, and so on), and cursor
dragging are follow-up scope. Entering Visual mode or folding collapses to the primary cursor
explicitly. An unsupported mutating command is rejected while multiple cursors exist instead of
silently editing only the primary; secondary cursors are never kept invisibly while a single-cursor
command runs.

## 1. Ground truth

- `DocTab` owns one `CursorState` (`src/engine/session.rs`). Its cursor is stored as a UAST leaf and
  node-local byte offset plus absolute line / display column. `active_visual` is a separate optional
  anchor and kind.
- Navigation mutates the one cursor through `handle_navigation_command`
  (`src/engine/runtime/navigation.rs`). Plain mouse clicks already resolve wrapped rows and gutter
  offsets to a `CursorPosition`, then send `EditorCommand::ClickCursor` (`src/ui/mouse.rs`).
- `apply_deltas_to_document_internal` already accepts a **group** of `TextDelta`s, applies them in
  supplied order, and records the group as one `UndoLedger` entry. Existing grouped-delete tests pin
  descending source-offset order and single-step undo (`src/engine/document/edit.rs`). This is the
  correct mutation primitive for multicursor edits; commands must not be replayed sequentially at
  each cursor.
- `UndoLedger` currently stores one post-replay cursor byte. Its history metadata must be widened to
  cursor snapshots or undo would silently discard the cursor set.
- Rendering exposes one primary pane row / column and uses the terminal hardware cursor. It already
  carries `cursor_lines: Vec<DocLine>` for gutter styling, so secondary cell overlays and multiple
  gutter lines fit the projection without a second rendering architecture.
- Async semantic grafting saves and restores only the current cursor's byte. Every cursor must be
  saved before `apply_graft` and resolved afterward because grafting replaces node identities.
- `KeyPattern::Char` intentionally ignores modifiers. A new exact `Alt(char)` pattern must outrank
  `Char` and Insert's `AnyChar`; otherwise Alt-j would move in Normal and insert `j` in Insert.

## 2. State model

### 2.1 Non-empty normalized cursor set

Replace the field-oriented single position with private types whose constructors preserve the
important invariants:

```rust
pub(crate) struct Caret {
	node: NodeId,
	offset: u32,
	abs_line: DocLine,
	abs_col: DocDisplayCol,
	preferred_col: DocDisplayCol,
}

pub(crate) struct CursorSet {
	primary: Caret,
	secondary: Vec<Caret>,
}
```

`primary + secondary` makes an empty cursor set unrepresentable. The fields stay private; all
mutation goes through `CursorSet` methods that resolve document bytes, sort secondary cursors by
byte, and remove duplicates. If a transformed secondary collides with the primary, the primary
wins. Cursor count is bounded (initially 1,024) so a stuck terminal key cannot create unbounded edit
work; attempting to exceed it leaves the set unchanged and reports a status message.

`preferred_col` preserves the requested column across short lines. Horizontal motion and mouse
placement reset it to the resolved column; vertical motion and adjacent-cursor creation retain it.
This behavior is introduced for every caret together rather than letting secondary cursors drift
after passing a short line.

### 2.2 Point cursors and Visual mode are disjoint

Represent selection state as a sum:

```rust
pub(crate) enum CursorMode {
	Points(CursorSet),
	Visual {
		caret: Caret,
		anchor: DocByte,
		kind: VisualKind,
	},
}
```

`CursorState` contains `CursorMode` plus fold-related state. This makes “multiple point cursors plus
one unexplained visual selection” unrepresentable. Entering Visual mode consumes a `CursorSet`,
keeps its primary, and drops its secondaries. Leaving Visual constructs a single-cursor set.

Normal and Insert modes may carry `Points`; Command, Search, and Confirm temporarily leave the
underlying point set alone unless the executed engine command has an explicit collapse policy.

### 2.3 Snapshots

History and graft boundaries use a node-independent snapshot:

```rust
pub(crate) struct CursorSnapshot {
	primary: DocByte,
	secondary: Vec<DocByte>,
}
```

Snapshots are normalized and clamped when restored. Node IDs are never stored in history because
edits, reloads, and semantic grafts can replace them.

## 3. Commands and interaction

Add closed protocol variants:

```rust
pub enum VerticalDirection { Up, Down }

EditorCommand::AddCursorAdjacent(VerticalDirection)
EditorCommand::AddCursorAt(CursorPosition)
EditorCommand::CollapseCursors
```

`ClickCursor` retains its existing meaning and additionally collapses the set before placing the
new primary. `AddCursorAt` is text-document-only, uses the same wrap-aware mouse coordinate
resolution as `ClickCursor`, and makes the added position primary. Adding an existing position
promotes that caret instead of creating a duplicate.

`AddCursorAdjacent` starts from the primary, targets the next / previous visible document line using
the same fold-aware line stepping as `j` / `k`, resolves at `preferred_col`, and makes the result
primary. At the first or last line it is a no-op. “Adjacent row” means a visible **document line**,
not another soft-wrapped segment of the same line.

### 3.1 Keys

Extend the typed keymap with `KeyPattern::Alt(char)`, precedence:

```
Ctrl > Alt > Special > Char > AnyChar
```

Add derived-help bindings in Normal Ready and Insert:

- `Alt-j`: add cursor below
- `Alt-k`: add cursor above
- Normal `Esc`: retain its current selection/search clearing and also collapse secondary cursors.
  Esc from Insert enters Normal while preserving the cursor set; a second Esc collapses it.

The bindings are absent from File Browser and pinned Markdown Preview contexts. Alt-j / Alt-k there
retain their existing context behavior.

### 3.2 Mouse

In the existing left-button path:

- plain left click sends `ClickCursor` (move and collapse);
- Alt-left-click sends `AddCursorAt`;
- clicks in the tab bar, gutter, minimap, divider, file browser, or pinned preview preserve their
  current meanings and never add a text cursor.

Alt-click adds only while the frontend is in Normal or Insert mode. In Visual, Command, Search, and
Confirm modes it follows the existing plain-click behavior; this prevents frontend mode state from
disagreeing with the engine's point/visual cursor mode.

Extract the modifier-to-command choice and content hit-test into testable pure helpers. Mouse drag,
rectangular selection, and removal by clicking an existing cursor are not part of this milestone.
Normal Esc or a plain click provides an unambiguous reset.

## 4. Projection and rendering

Keep the current primary fields in `PaneViewport` for compatibility and add projected secondary
carets:

```rust
pub struct ProjectedCaret {
	pub pane_row: PaneLocalRow,
	pub pane_col: PaneLocalCol,
	pub abs_pos: CursorPosition,
	pub abs_byte: DocByte,
}

pub secondary_carets: Vec<ProjectedCaret>
pub cursor_count: usize
```

The primary alone drives viewport panning, minimap focus, status line / column, and the hardware
cursor. Every visible secondary is projected through the same sum-tree, fold, and wrap coordinate
logic as the primary. `cursor_lines` becomes the sorted unique set of all caret lines so gutter
highlighting reflects the whole set. The status bar shows `N cursors` when `N > 1`.

After text painting, draw each visible secondary as a reverse-video cell. This works at ordinary
characters and on the blank cell at end-of-line and is independent of terminal cursor-shape
support. If multiple carets project to one folded placeholder or screen cell, paint it once.
Structural fold commands collapse to the primary in milestone one, preventing hidden secondary
carets from editing folded text unexpectedly.

## 5. Transactional multicursor edits

### 5.1 Plan from one immutable document state

Introduce an edit planner for point-local operations:

```rust
struct PlannedEdit {
	deltas: Vec<TextDelta>,              // descending source byte
	before: CursorSnapshot,
	after: CursorSnapshot,
}
```

For each caret, resolve the operation against the same pre-edit document. Normalize the results by
source byte, reject inconsistent overlaps, coalesce identical deletion coverage where possible,
and sort deltas from highest byte to lowest before mutation. Applying commands once per caret in a
loop is forbidden: later commands would observe shifted bytes, create multiple undo steps, and
schedule redundant semantic work.

Post-edit caret positions are calculated in source coordinates with explicit boundary affinity,
then rebased across the complete delta group. This is necessary when an edit below one cursor shifts
every cursor above it. The planner returns no delta for a caret where the operation is unavailable
(for example backspace at byte zero) while preserving that caret.

The first milestone plans these operations for all cursors:

- Insert: character, newline, tab, pasted / inserted text.
- Point deletion: backspace, forward delete, Normal `x` / `X`.
- Point replacement: Normal `r` and `~`.
- Insert entry positioning: `i`, `a`, `I`, `A` applies its cursor positioning to the whole set.

`o` / `O`, put, line and word operators, text objects, and join are rejected with a concise status
message while more than one cursor exists in milestone one. Editing only the primary would be data
loss from the user's perspective. The behavior is centralized in an exhaustive command policy, not
scattered conditionals:

```rust
enum MultiCursorPolicy {
	FanOut,
	Preserve,
	Collapse,
	RejectWhileMultiple,
}
```

The `EditorCommand` match has no wildcard, so adding a command forces an explicit policy decision.
Visual entry and folding use `Collapse`; cursor-neutral view, pane, and tab operations use
`Preserve`; supported edits and relative motions use `FanOut`; unsupported point edits use
`RejectWhileMultiple`. Global Ex edits collapse unless their implementation explicitly rebases and
returns a cursor set.

### 5.2 Navigation

Relative point motions (`h/j/k/l`, arrows, `w/b/e`, `0/^/$`, Home/End, and Insert navigation) map
over all carets from the same document state and normalize collisions afterward. Find-character
motions run independently on each caret; a miss on one caret does not prevent other carets moving.
The primary result alone controls panning and status messages.

Absolute jumps, search-result jumps, folding, and structural selections collapse to the primary in
milestone one. Page / viewport commands and tab / pane changes preserve the cursor set. This
boundary avoids surprising sets of cursors all coalescing at one absolute target and can be widened
later.

### 5.3 Undo and redo

Change text-history entries to retain both `before` and `after` `CursorSnapshot`s. Forward edits
record one delta group and both sets; undo replays inverse deltas and restores `before`, while redo
replays forward deltas and restores `after`. Single-cursor edits use the same representation with an
empty secondary vector.

Cursor addition, removal, and motion are not history entries. Generation swaps and document reloads
collapse to one cursor explicitly. A semantic graft snapshots and restores the live set without
touching undo history.

One multicursor keystroke schedules one render / semantic update and advances `StateId` once.

## 6. Invariants and tests

### 6.1 Cursor-set tests

- A set is always non-empty and has exactly one primary.
- Add-at and add-adjacent sort and deduplicate by resolved document byte.
- Adding an existing secondary promotes it; adding the primary is idempotent.
- Repeated down / up creation grows in the expected direction.
- Preferred columns survive short, empty, tabbed, and UTF-8 lines.
- Collision after motion retains the primary and removes duplicate secondaries.
- The 1,024-cursor limit is enforced without partial mutation.

### 6.2 Edit and history tests

- Insert on adjacent and arbitrary carets produces descending deltas and correct text.
- Newline, tab, multibyte characters, paste, backspace-at-BOF, delete-at-EOF, `r`, and `~` have
  pinned results and caret positions.
- Edits on the same line and on lines of different lengths rebase all positions correctly.
- One undo restores the entire text and pre-edit cursor set; one redo restores both post-edit values.
- Dirty state advances once for a multicursor transaction.
- Semantic highlights and diagnostics rebase once across the group.
- Async semantic grafting preserves every cursor byte while replacing every stale node identity.

### 6.3 UI and integration tests

- `Alt` patterns outrank unmodified `Char` and Insert `AnyChar`; existing modifier semantics remain
  pinned for every other key.
- Derived `:help keys` output contains Alt-j / Alt-k in Normal and Insert sections.
- Alt-click emits add while plain click emits move-and-collapse, including wrapped rows and gutters.
- Projection returns primary and visible secondary screen coordinates for wrap, tabs, folds, EOL,
  split panes, and non-focused tabs.
- Secondary cursor cells and all cursor gutter lines render in a `TestBackend` snapshot.
- File browser, pinned previews, pane focus, document load / reload, and tab switching never retain
  invalid or invisible secondary cursors.

## 7. Commit sequence

Run `cargo fmt`, the focused tests, the full suite, and strict Clippy after every commit.

1. `test: characterize cursor edit and projection boundaries` — pin single-cursor click, movement,
   grouped-delta order, undo cursor restoration, graft restoration, wrap / fold projection, and
   modifier behavior before changing representation.
2. `refactor: represent cursor state as normalized modes` — add `Caret`, `CursorSet`, `CursorMode`,
   and snapshots; migrate all single-cursor readers and reset paths with no behavior change.
3. `feat: add and render multiple cursors` — protocol commands, Alt key pattern and bindings,
   Alt-click, projection, secondary cell overlays, gutter lines, count, limits, and collapse rules.
4. `refactor: plan grouped point edits` — immutable-source planner, overlap validation, cursor
   rebasing, and one semantic/render boundary; single-cursor behavior still matches baseline.
5. `feat: restore cursor sets through history and grafts` — before/after history snapshots,
   undo/redo, semantic graft resynchronization, and document-boundary collapse policies. This lands
   before multicursor mutation so no intermediate commit can discard secondary cursors on undo.
6. `feat: edit at every cursor` — insert/text/paste/newline/tab, backspace/delete/x/X, r/~, and
   i/a/I/A positioning fan out through the planner.
7. `feat: move every cursor` — relative Normal and Insert motions, preferred-column behavior,
   collision normalization, and primary-only panning.
8. `docs: document multicursor controls` — help wording, manual verification, and final regression
   audit. If all help is already derived in commit 3, this commit contains only user-facing prose and
   snapshots rather than duplicating key data.

## 8. Verification

- Automated: `cargo fmt --all -- --check`, `cargo check --all-targets`,
  `cargo clippy --all-targets -- -D warnings`, and the full test suite after every commit.
- Manual, unwrapped file: build a vertical stack with Alt-j / Alt-k; type, paste, newline,
  backspace, delete, `r`, and `~`; undo / redo once; confirm cursor count and positions.
- Manual, arbitrary: Alt-click positions on the same line, different lines, tabs, UTF-8 text, empty
  lines, and EOL; plain click and Normal Esc collapse correctly.
- Manual, layout: repeat in a wrapped document and split pane; switch document tabs; trigger a Rust
  semantic graft; verify only the primary pans and every secondary remains correctly placed.
- Manual boundaries: file browser, help preview, folded text, reload, Visual entry, and unsupported
  operators collapse or reject exactly as specified—never leave hidden live cursors.
