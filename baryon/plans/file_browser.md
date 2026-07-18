# Specification: File Browser (Pane-Local Browser Document)

## Objective
Add a NeoVim-style file browser as a real pane inside the existing layout tree. The browser should reuse Baryon's normal rendering, cursor, scrolling, and search pipeline, but it should not force filesystem state into the `LanguageProvider` abstraction.

## 1. Design Constraints

### 1.1 What The Browser Is
- The file browser is a **read-only synthetic document** rendered inside a normal `EditorPane`.
- The visible text is rebuilt from filesystem state whenever the tree is opened, refreshed, or expanded/collapsed.
- The browser owns row metadata such as `PathBuf`, depth, and directory/file kind so actions can target real filesystem entries instead of parsing rendered text.

### 1.2 What The Browser Is Not
- It is **not** a normal source-language provider that owns scan state.
- It does **not** use generic UAST fold placeholders for directories in v1. The current folded placeholder rendering loses the directory name, which makes it a bad fit for a file tree.
- It does **not** depend on virtual-text overlays, Nerdfonts, git-status badges, or permission annotations in v1.

### 1.3 V1 Scope
- One file-browser sidebar at a time.
- Root path is the process working directory captured when `:tree` first opens the browser.
- Directories can expand/collapse.
- Files can open into a non-browser editor pane.
- Search works through the existing search pipeline because the browser is still backed by document text.
- Create/delete/rename/git-status/permission columns are deferred.

## 2. Session Model (`src/engine/session.rs`)

### 2.1 Add An Explicit Document Kind
Extend `DocumentSession` so the runtime can distinguish normal editable text documents from the browser:

```rust
pub(crate) enum DocumentKind {
    Text,
    FileBrowser(FileBrowserState),
}

pub(crate) struct FileBrowserState {
    pub(crate) root_path: PathBuf,
    pub(crate) expanded_dirs: HashSet<PathBuf>,
    pub(crate) visible_rows: Vec<FileBrowserRow>,
}

pub(crate) struct FileBrowserRow {
    pub(crate) path: PathBuf,
    pub(crate) depth: u16,
    pub(crate) kind: FileBrowserRowKind,
}

pub(crate) enum FileBrowserRowKind {
    Directory { expanded: bool },
    File,
}
```

### 2.2 Document Session Rules
- Normal documents remain `DocumentKind::Text`.
- Browser documents use:
    - `kind = DocumentKind::FileBrowser(...)`
    - `root_id = Some(...)` pointing at the synthetic browser document
    - `file_path = Some(root_path.display().to_string())` for tab/status display
    - `backing_path = None`
    - `shadow = None`
    - `active_language_id = Some("fs")` if an `FsProvider` is registered for styling
- Browser documents are always read-only and should never become dirty.

### 2.3 Why This Is Needed
The current runtime routes edit, write, undo, and navigation commands through the active tab with no notion of pane type or document kind. Without an explicit browser kind, the file tree would be treated as a normal editable buffer.

## 3. Browser Builder (`src/engine/file_browser.rs`)

### 3.1 Engine-Owned Browser State
Create a new engine-side module responsible for:
- Scanning directories with `std::fs::read_dir`
- Flattening the visible tree from `expanded_dirs`
- Producing both:
    - `visible_rows: Vec<FileBrowserRow>`
    - synthetic document text / UAST nodes used for rendering

This module owns filesystem traversal. It is not part of `src/svp`.

### 3.2 Row Ordering
Each visible directory entry becomes one logical row.
- Omit `.` and `..`
- Sort directories before files
- Sort each group lexicographically by file name
- Preserve stable row ordering across rebuilds so cursor restoration is predictable

### 3.3 Text Format
Render each row directly as document text rather than as an overlay:

```text
v src/
  > engine/
    render.rs
  main.rs
Cargo.toml
README.md
```

Rules:
- Two spaces per depth level
- Collapsed directory prefix: `> `
- Expanded directory prefix: `v `
- File prefix: two spaces
- Directory names include a trailing `/`
- Every row ends with `\n` so `DocLine` maps 1:1 to visible entries

### 3.4 Synthetic UAST Shape
For v1, keep the UAST simple:
- Root node: one `RelationalTable`
- One `Token` leaf per visible row

This intentionally avoids representing directories as branch nodes inside the UAST. The browser's expand/collapse model lives in `FileBrowserState`, not in `registry.is_folded`.

### 3.5 Rebuild Behavior
Any browser state change rebuilds the synthetic document in place:
- open browser
- refresh browser
- expand directory
- collapse directory

After rebuild:
- restore cursor to the same path if it still exists
- otherwise snap to the nearest surviving row
- reset scroll as little as possible

## 4. Styling (`src/svp/fs_provider.rs`, `src/app.rs`)

### 4.1 `FsProvider` Is Optional But Narrow
If we want browser-specific colors without adding renderer-only styling rules, add an `FsProvider` with:
- `language_id() == "fs"`
- `label() == "Files"`
- empty `extensions()`
- lexical highlighting only
- no semantic reactor work
- no structural topology generation

### 4.2 What `FsProvider` Does
It may inspect the rendered browser bytes and highlight:
- directory names and trailing `/`
- tree markers (`>`, `v`) as punctuation/muted accents
- file names as plain text

It must **not**:
- own filesystem scan state
- decide expand/collapse
- map rows to real paths

### 4.3 Why Keep It Narrow
`LanguageProvider` already fits "given bytes, return highlights". It does not fit "own a live file tree model and mutate it from UI events".

## 5. Browser Commands (`src/engine/protocol.rs`, `src/engine/runtime.rs`)

### 5.1 New Commands
Add explicit browser commands instead of overloading `LoadFile`:

```rust
EditorCommand::OpenFileBrowser
EditorCommand::RefreshFileBrowser
EditorCommand::ActivateBrowserEntry
EditorCommand::CollapseBrowserEntryOrParent
```

### 5.2 Runtime Routing
The runtime should branch on `DocumentKind` before executing document mutations:
- Browser documents allow:
    - movement
    - scrolling
    - search
    - browser commands
    - pane commands
    - local view commands like wrap/minimap/theme
- Browser documents reject:
    - text edit commands
    - write commands
    - undo/redo
    - substitution/sort/uniq

Rejected commands should be harmless no-ops with a status message such as `File browser is read-only`.

## 6. Sidebar Integration (`src/engine/session.rs`)

### 6.1 Dedicated Sidebar Helper
Do not build the sidebar through the existing generic `split_focused_pane()` flow alone. That flow preserves the old pane as the left child and creates a new empty right child, which is the wrong shape for `:tree`.

Add a dedicated helper:
- `EditorSession::open_file_browser_sidebar(...)`

Behavior:
- If a browser pane already exists, focus it and stop.
- Otherwise, replace the currently focused leaf with a vertical split whose children are:
    1. new left browser pane
    2. previous editor pane on the right

### 6.2 Initial Sidebar Width
The layout system only supports proportional `ratios`, not fixed-width panes.
- On creation, derive the initial ratio from a target width around 24 columns.
- After creation, let normal ratio-based resize behavior handle the sidebar like any other pane.
- Do not promise a permanently fixed-width sidebar in v1.

### 6.3 Browser Pane Detection
Add a layout/session helper to find panes whose active tab is a browser document. This is needed for:
- focusing an existing browser pane
- preventing the browser from targeting itself when opening files
- tests

## 7. Opening Files From The Browser

### 7.1 Target Pane Selection
`ActivateBrowserEntry` on a file should open into a non-browser pane.

Selection rule:
1. Prefer the nearest non-browser pane to the right of the browser.
2. Otherwise use any other non-browser pane.
3. If none exists, create a new editor pane to the right and target that pane.

### 7.2 Targeted Load
Add a runtime helper that can load a file into a specific `PaneId` rather than always using `focused_pane_id`.

This should be implemented as engine-side logic, not as a one-off frontend hack.

### 7.3 Focus After Open
After opening a file from the browser:
- focus the target editor pane
- keep the browser pane intact

This matches the expected sidebar workflow better than replacing the browser document or leaving focus trapped in the tree.

## 8. Input Model (`src/ui/command.rs`, `src/ui/normal.rs`)

### 8.1 Ex Commands
Add:
- `:tree` -> `EditorCommand::OpenFileBrowser`

For v1, `:tree` should mean "open the browser if absent, otherwise focus the existing browser pane". Closing still uses the normal pane-close commands.

### 8.2 Normal-Mode Browser Keys
When the active document is a browser:
- `Enter` -> `ActivateBrowserEntry`
- `l` -> `ActivateBrowserEntry`
- `h` -> `CollapseBrowserEntryOrParent`
- `r` -> `RefreshFileBrowser`

Existing motions remain valid:
- `j` / `k`
- `gg` / `G`
- `/`, `n`, `N`
- pane-prefix commands

### 8.3 Why Browser-Specific Keys Are Needed
The current normal-mode mapping has no `Enter` behavior and treats `r` as replace-char. Browser interactions need a document-kind-aware branch in the frontend.

## 9. Testing

### 9.1 Builder Tests
Add unit tests for:
- directory-before-file ordering
- stable lexicographic sorting
- nested flattening from `expanded_dirs`
- text generation with one line per row

### 9.2 Runtime/System Tests
Add engine tests covering:
- `:tree` creates a left sidebar and preserves the original editor on the right
- `:tree` focuses an existing browser pane instead of creating duplicates
- activating a directory expands/collapses it without treating it as a generic fold
- activating a file opens it in a sibling editor pane, not in the browser pane
- browser documents reject writes and text edits
- browser search works through the existing search system

## 10. Deferred Work
- Git status annotations
- Permission annotations
- Create/delete/rename operations
- Multiple browser panes
- Shared document registry integration

## Summary
The file browser should be implemented as an engine-owned, read-only browser document rendered through the normal pane pipeline. `FsProvider` can still exist, but only as a stateless lexical highlighter and status label provider. The actual tree model, expand/collapse behavior, and cross-pane file opening belong in the engine/session/runtime layer.
