# Specification: Split Panes (Layout Tree)

## Objective
Refactor Baryon's single-pane architecture into a hierarchical layout tree supporting horizontal and vertical splits, multi-pane rendering, and pane-focus management.

## 1. Data Model Refactor (`src/engine/session.rs`)

### 1.1 The Layout Tree
Replace `EditorSession.pane` with a recursive `LayoutNode`.
```rust
pub(crate) enum LayoutNode {
    Split {
        direction: SplitDirection, // Horizontal | Vertical
        children: Vec<LayoutNode>,
        ratios: Vec<u32>,          // Relative weights within the parent
    },
    Leaf(LayoutLeaf),
}

pub(crate) enum SplitDirection {
    Horizontal, // Top-to-bottom
    Vertical,   // Left-to-right
}
```

### 1.2 Focus Management
Add `focused_pane_id: PaneId` to `EditorSession`.
-   Input commands are routed to the `EditorPane` corresponding to the focused ID.
-   Pane-prefix motions like `sh/sj/sk/sl` will navigate the focus through the layout tree.

## 2. Rendering Pipeline (`src/ui/draw.rs`)

### 2.1 Recursive Layout Calculation
Update the `Frontend::draw` loop to calculate the `Rect` for each `LayoutNode` before rendering.
-   **Split**: Divides its assigned `Rect` among its children based on `ratios`.
-   **Leaf**: Resolves a `LayoutLeaf` and renders the `active_tab` of its `EditorPane` into its assigned `Rect`.

### 2.2 Chrome Per Pane
Each leaf pane will now render its own:
-   **Tab Bar**: Specific to the documents open in that pane.
-   **Status Bar**: Remains global for now.
-   **Minimap**: Contained within the pane's width.

## 3. Command Routing (`src/engine/runtime.rs`)

### 3.1 Targeted Commands
Update the `match command` loop to find the focused `EditorPane` in the `LayoutNode` tree by `PaneId`.
```rust
let focused_pane = session.layout.find_pane_mut(session.focused_pane_id)?;
let effect = handle_command(..., focused_pane.active_tab_mut(), ...);
```

### 3.2 Layout Commands
Implement new `EditorCommand` variants:
-   `SplitPane(SplitDirection)`: Splits the focused pane horizontally or vertically.
-   `ClosePane`: Removes a leaf and rebalances the parent split.
-   `FocusPaneDirection(PaneFocusDirection)`: Moves focus.

## 4. Shared Documents (Future)

Currently, `DocTab` owns the `DocumentSession`. 
-   **Refactor**: Move `DocumentSession` into a central document registry in `EditorSession`, keyed by a stable `DocumentId`.
-   **View State**: `DocTab` becomes a "View" into a document, holding its own `CursorState`, `ViewState`, and a `DocumentId`.
-   **State Audit**: Split today's `SemanticState` and cache fields into document-level state vs. view-local state instead of assuming all of it belongs on the tab.
-   **Sync**: Multiple panes looking at the same document will stay in sync because they reference the same `DocumentSession`.

## Phase 0: Coordinate Hygiene (Implemented)

To prevent "Coordinate Conflation" and resolve rendering bugs (like the "one-line-off" mouse click), a rigorous newtype hierarchy was implemented in `src/core/coords.rs`:

### 1. The Coordinate Hierarchy
- **Document Space (Logical)**:
    - `DocLine`: The `\n` delimited line index.
    - `DocByte`: The absolute byte offset in the file.
- **Projection Space (Visual)**:
    - `DocDisplayCol`: (Renamed from `VisualCol`) The column index *after* tab expansion, but *before* word wrap.
    - `DisplayRow`: The row index after folding and word-wrap are applied. This is the vertical peer to `DocDisplayCol`.
- **Screen Space (Physical)**:
    - `ScreenCol` / `ScreenRow`: Absolute terminal coordinates (0-indexed from top-left).
    - `PaneCol` / `PaneRow`: Coordinates relative to a specific split/pane's text area.
    - `PaneRect`: A typed bounding box using `ScreenCol`, `ScreenRow`, `PaneCol`, and `PaneRow`.

### 2. Implementation Benefits
- **Type Safety**: The compiler now prevents accidental mixing of `DocLine` and `DisplayRow`.
- **Mouse Accuracy**: Mouse-to-document translation now explicitly accounts for `ScreenRow -> PaneRow -> DisplayRow -> DocLine`.

## Phase 1: Layout Tree (Implemented)

The `EditorSession` was refactored to support a hierarchical layout:

### 1.1 The Layout Tree
- `LayoutNode`: A recursive enum supporting `Split` (Horizontal/Vertical) and `Leaf` (containing a `LayoutLeaf`, which wraps `PaneId` and `EditorPane`).
- `PaneId`: Unique identifier for each leaf to track focus and target commands.

### 1.2 Focus Management
- `focused_pane_id`: Tracks which pane receives input.
- `PaneFocusDirection`: Logic implemented to navigate focus (Left/Right/Up/Down) by comparing `PaneRect` midpoints and overlaps.

### 1.3 Terminology
- Keep `Pane` as the internal runtime/layout term.
- In command vocabulary, "split" means the leaf pane created and managed by the split layout.
- This keeps command names memorable (`hsplit` / `vsplit` / `csplit`) without forcing every future pane kind (editor, file browser, data view, plot view, etc.) to encode split-layout history in its type names.

## Phase 2: Multi-Pane Rendering & Input (Implemented)

The renderer and frontend were upgraded from a single viewport to a multi-pane frame:

### 2.1 Multi-Pane Frame
- `Viewport` now carries multiple `PaneViewport`s plus `focused_pane_id`.
- Each `PaneViewport` owns its own `PaneRect`, tab summaries, cursor placement, viewport metrics, highlights, diagnostics, minimap, and visible-row projection.
- The focused pane remains the ergonomic "default view" through `Deref`, which keeps a large amount of single-pane code and tests readable.

### 2.2 Per-Pane Chrome
- Each pane renders its own tab bar and minimap inside its assigned `PaneRect`.
- The status bar remains global for now.
- Mouse hit-testing first resolves `ScreenRow` / `ScreenCol` into a target `PaneRect`, then translates into pane-local document coordinates.

### 2.3 Current Behavioral Constraint
- A newly created split currently opens as an empty pane.
- Duplicating the current document into the new pane is intentionally deferred until shared-document plumbing exists.

## Phase 3: Symmetric Command Model & Resizing (Implemented)

The split UX now uses a small, explicit command family that is safe in a terminal TUI and avoids `Ctrl-w`.

### 3.1 Command Surface
- Canonical ex commands:
    - `:hsplit` / `:hs` : Horizontal split (Top/Bottom).
    - `:vsplit` / `:vs` : Vertical split (Left/Right).
    - `:csplit` / `:cs` : Close the focused split pane.
- Old transitional aliases like `:split`, `:close`, and `:sp` were removed.
- Keyboard pane prefix:
    - `s` arms a one-shot pane command prefix in normal mode.
    - `sh/sj/sk/sl` move focus Left/Down/Up/Right.
    - `ss` creates a horizontal split.
    - `sv` creates a vertical split.
    - `sc` closes the focused pane.
    - `Esc` cancels the pending `s` prefix without taking action.

### 3.2 Sticky Resize Mode
Resizing manipulates the `ratios: Vec<u32>` in the `LayoutNode` tree rather than raw pixels, so layouts remain proportional after terminal resize.
- `sr` enters a sticky pane-resize submode.
- While resize mode is active:
    - `h` makes the focused pane narrower.
    - `j` makes the focused pane taller.
    - `k` makes the focused pane shorter.
    - `l` makes the focused pane wider.
    - `Esc` exits resize mode.
- The normal-mode indicator shows `NOR s` while the pane prefix is armed and `NOR sr` while resize mode is active.
- Axis rule:
    - `h` / `l` target the nearest vertical ancestor split containing the focused pane.
    - `j` / `k` target the nearest horizontal ancestor split containing the focused pane.
- Adjustment rule:
    - Each resize command changes the focused pane by one rendered cell when possible: one column for width changes, one row for height changes.
    - The focused pane's share is rebalanced against its siblings without allowing a pane to collapse below its minimum share.
    - If no ancestor split exists in the requested axis, treat the command as a no-op and surface a status message.
    - If the focused pane cannot grow or shrink further, surface a size-limit status message.

## Phase 4: Shared Document Sessions (Next Big Architectural Step)

Currently, a `DocTab` owns its `DocumentSession`. This prevents two panes from looking at the same file without duplicating the entire state (and potentially diverging).

### 4.1 The Document Registry
- Move `DocumentSession` into a central registry owned by the engine thread.
- Use a stable `DocumentId` for pane/view references.
- Maintain a path index (for example `HashMap<PathBuf, DocumentId>`) so opening an already-loaded file reuses the existing document session instead of cloning it.
- `DocTab` becomes a **View** entity, containing only:
    - `cursor: CursorState` (Independent per pane).
    - `view: ViewState` (Independent scroll/wrap/minimap).
    - `document_id: DocumentId`.
- State partitioning:
    - Document-level state should move with the shared `DocumentSession` (undo ledger, root_id, semantic diagnostics/highlights, sum-tree cache, base minimap cache, language identity, backing path, dirty state).
    - View-level state should stay on the tab (cursor position, viewport anchor, wrap/minimap toggles, per-pane overlay state).

### 4.2 Benefits
- **Real-time Sync**: Typing in one pane immediately reflects in another pane looking at the same file.
- **Memory Efficiency**: Large files (like the 200MB CSVs) are only loaded into memory once, regardless of how many splits are open.
- **Clean Future Pane Types**: Keeping `Pane` generic internally leaves room for file-browser, data, and plot panes without renaming the core layout vocabulary later.
