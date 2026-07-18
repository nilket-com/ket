# Specification: Multi-Tab Architecture

## Objective
Evolve Baryon from a single-buffer editor to a multi-document environment. This specification establishes a "Modern Pane" architectural hierarchy (similar to VS Code) where a global sidebar exists alongside a main editor area. The main area contains `EditorPane`s, and each pane contains a collection of `DocTab`s.

## 1. Architectural Hierarchy
The UI layout will follow this structure:
```text
[ Global Sidebar (Future) ] | [       Editor Area (Main)        ]
                            | [ EditorPane (Currently just one) ]
                            | [ Tab Bar: main.rs | lib.rs [+]   ]
                            | [                                 ]
                            | [        Active DocTab View       ]
```
*Note: The global sidebar (File Browser) and multiple split panes will be implemented in subsequent phases. This phase establishes the foundation that makes those features possible.*

## 2. State & Settings Refactoring (`src/engine/session.rs`)

### 2.1 Settings: Persistent vs. Ephemeral
To provide maximum flexibility without sacrificing code elegance, settings will be divided into persistent defaults and ephemeral per-tab overrides.
*   **`SettingsPersistent`**: Loaded from and saved to disk. Used as the blueprint when creating new tabs.
*   **`SettingsLocal`**: Ephemeral settings owned by a specific tab. Commands like `:theme onedark` modify these. Commands like `:set theme onedark` modify both local and persistent settings and write to disk.

### 2.2 The `DocTab` Struct
Encapsulate all buffer-specific state into a new `DocTab` struct to avoid the word "Tab" colliding with indentation terminology.
```rust
pub(crate) struct DocTab {
    pub(crate) document: DocumentSession,
    pub(crate) cursor: CursorState,
    pub(crate) view: ViewState, // Incorporates SettingsLocal (theme, wrap, minimap)
    pub(crate) search_matches: Vec<SearchMatch>, // Matches are local
    pub(crate) match_index: Option<usize>,
    pub(crate) match_info: Option<String>,
    pub(crate) semantic: SemanticState,
}
```

### 2.3 The `EditorPane` Struct
A container for tabs. Currently, there will only be one, but this paves the way for split panes.
```rust
pub(crate) struct EditorPane {
    pub(crate) tabs: Vec<DocTab>,
    pub(crate) active_tab_index: usize,
}
```

### 2.4 `EditorSession` Update
Update the root session to hold global state and the primary pane.
```rust
pub(crate) struct EditorSession {
    pub(crate) pane: EditorPane,
    pub(crate) clipboard: ClipboardSession,
    pub(crate) search_pattern: Option<String>, // Pattern and history are global
    pub(crate) search_case_insensitive: bool,
    pub(crate) search_confirm: Option<ConfirmState>,
    pub(crate) ui: UiState,
    pub(crate) persistent_settings: SettingsPersistent,
}
```

## 3. Engine Loop Refactoring (`src/engine/runtime.rs`)

### 3.1 Resolving the "Variable Explosion"
Currently, `run_engine` destructures the entire `EditorSession` into ~50 local variables to appease the borrow checker during command handling. With a `Vec<DocTab>`, this approach is unmaintainable.
*   **Solution**: Refactor command handlers in `runtime/*.rs` to accept grouped contexts (e.g., `&mut DocTab`, `&mut EditorSession` (for globals)) instead of dozens of individual mutable references.
*   The `run_engine` loop will extract `&mut active_doctab` from the active `EditorPane` and pass it to the relevant handlers.

### 3.2 Global Semantic Dispatch
To prevent background tab results from being discarded or incorrectly applied, the engine must implement a global dispatch mechanism for `SemanticReactor` responses.
*   **The Problem**: Currently, only the active tab polls the reactor. Responses for background tabs are consumed and discarded if they don't match the active tab's ID.
*   **The Fix**: 
    1.  `render_frame` (or the `run_engine` loop) will poll all available responses from the `SemanticReactor` at the start of each frame.
    2.  For each response, the engine will iterate through *all* `DocTab`s in the `EditorPane`.
    3.  If a response's `request_id` matches a tab's `pending_semantic_request_id`, the results (highlights, diagnostics, and structural graft) will be applied to that specific tab immediately.
    4.  This ensures background tabs complete their semantic analysis correctly and prevents "content bleeding" between tabs.

## 4. Command Implementation

### 4.1 Tab Management Commands (Engine)
Implement the following `EditorCommand` variants:
*   `DocTabNew`: Creates a new, empty `DocTab`.
*   `DocTabEdit(String)`: Opens a file in a new `DocTab`.
*   `DocTabNext` / `DocTabPrev`: Cycles the `active_tab_index` within the current `EditorPane`.
*   `DocTabClose`: Removes the current `DocTab`. If it's the last tab, it acts as a quit request.

### 4.2 Settings Commands
*   Update existing settings commands (e.g., `SetTheme`, `SetWrap`, `SetMinimap`) to only affect the active `DocTab`'s `SettingsLocal` (ephemeral).
*   Introduce new variants or flags for persistent changes (e.g., `:set theme onedark` -> `EditorCommand::PersistTheme(String)`).

### 4.3 Frontend Command Mapping (`src/ui/command.rs`)
Map the following colon commands to their respective `EditorCommand` variants:
*   `:tabnew` -> `DocTabNew`
*   `:tabe {path}`, `:tabedit {path}` -> `DocTabEdit(path)`
*   `:tabn`, `:tabnext` -> `DocTabNext`
*   `:tabp`, `:tabprev`, `:tabprevious` -> `DocTabPrev`
*   `:tabc`, `:tabclose` -> `DocTabClose`

### 4.4 Normal Mode Mappings (`src/ui/normal.rs`)
Implement the following chord mappings:
*   `gt` -> `DocTabNext`
*   `gT` -> `DocTabPrev`

## 5. UI Layer (`src/ui/draw.rs`)

### 5.1 Tab Bar Rendering
*   Allocate the top row of the terminal viewport for the tab bar.
*   Iterate through `session.pane.tabs`.
*   Render the filename (or `[No Name]`) for each tab.
*   Apply distinct styling to the active tab.
*   Display a modified indicator (e.g., `+` or `●`) next to the filename if `doctab.document.ledger.is_dirty()` is true.

### 5.2 Viewport Adjustment
*   Subtract 1 from the available vertical space (`max_height`) before calculating the `geometry` for the main text viewport to accommodate the Tab Bar row.

## 6. Initialization & Viewport Fixes

### 6.1 Viewport Dimension Inheritance
*   **The Bug**: Hardcoded `80x50` dimensions in `DocTab::new` cause rendering to break when new tabs are spawned.
*   **The Fix**: Update `DocTab::new` to accept `width` and `lines` as arguments. Ensure `spawn_empty_doctab` in `runtime.rs` copies these dimensions from the currently active tab so the new tab matches the terminal size immediately.

### 6.2 App Startup (`src/app.rs`)
Update `App::run` and `Engine::new` to initialize the `EditorSession` with a single `EditorPane` containing one `DocTab`. If an `initial_file` is provided, that first `DocTab` will load it; otherwise, it will be an empty buffer.
