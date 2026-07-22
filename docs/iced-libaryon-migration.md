# Polariton: replace the in-house editor with libaryon

Status: plan (2026-07-18)
Related: `baryon/plans/headless-baryon.md`, `baryon/plans/input-contract.md`,
`baryon/plans/platforms.md`, `docs/architecture.md`

## Framing

This is an editor-engine migration plus a new iced renderer, not a widget swap.
libaryon is headless: it exposes `EngineHandle::spawn(EngineConfig)`, command and
input senders, and immutable `Viewport` projections
(`crates/libaryon/src/engine/host.rs`, `src/uast/projection.rs`). Its only
existing renderer is baryon's ratatui frontend. Per `headless-baryon.md`, the iced
frontend is born inside polariton; a `baryon-iced` crate is extracted only if a
second iced consumer appears.

Polariton's app-side coupling is small — one `CodeEditor` in `gui/state.rs:28`,
rendered in one pane, consumed as a `String` for run/save/load, with ~a dozen call
sites funnelled through `CodeEditor` + `EditorMsg`. The in-house editor to retire
is `crates/polariton/src/editor/` (custom `iced::advanced` widget, ropey buffer,
tree-sitter highlighting, vim layer, folding, search, undo, minimap).

## Known blockers and decisions

1. **In-memory documents (blocker).** Polariton creates/replaces editors from
   in-memory SQL strings (saved statements). libaryon's public API only loads
   from files (`EditorCommand::LoadFile`, `EngineConfig.initial_file`); a giant
   `InsertText` is not acceptable. First deliverable: a focused public
   `ReplaceDocument { text, language, .. }` command (or equivalent in-memory
   document host API) in libaryon, with an engine-level integration test.
2. **Panes/tabs stay polariton's.** Polariton owns the outer pane layout. Render
   only the focused libaryon pane inside the existing code-editor pane; consider
   engine tabs/splits later.
3. **Lifecycle ownership.** `EngineHandle` starts threads; polariton must own
   spawn/shutdown deterministically (shutdown on app exit, and when swapping is
   not needed once ReplaceDocument exists — one long-lived engine).
4. **Color bridge.** Orphan rule prevents `From<Rgb> for iced::Color` in
   libaryon; polariton hosts the conversion (`headless-baryon.md`).
5. **Viewport delivery.** Channels are blocking `std::mpsc`; wrap the receiver in
   an iced 0.14 subscription on a thread (`headless-baryon.md`). The UI thread
   must never block on the receiver — no `recv()` from `update` or `view`; the
   subscription's background work waits and emits, and app state coalesces stale
   viewports (keep only the latest). Async adapter is deferred.
6. **Input mapping.** No frontend-neutral keymap layer yet
   (`input-contract.md`/`keymap.md` are future work); polariton translates iced
   keyboard/mouse events to `InputEvent`/`EditorCommand` directly, including
   pixel→row/column mapping for mouse and resize.
7. **Platform.** ~~libaryon is Linux-only.~~ Corrected 2026-07-20: the SVP
   resolver has a `BlockingBackend` selected under
   `cfg(not(target_os = "linux"))`, and `io-uring` is declared under
   `[target.'cfg(target_os = "linux")'.dependencies]`, so non-Linux builds have
   a real path. Linux gets the io_uring backend; other platforms fall back to
   blocking I/O.
8. **Compile weight.** libaryon pulls rust-analyzer and ruff crates; per-language
   feature gates are flagged in `headless-baryon.md` as follow-up work.

## Implementation order

1. **libaryon: in-memory document API.** ✅ Done (2026-07-18):
   `EditorCommand::ReplaceDocument { text, language }` atomically swaps the
   focused document for in-memory text (resets undo/selection/search/folds/
   caret/scroll, clears the file association and its canonical-path claim;
   unknown language rejects without touching the document), and
   `EditorCommand::QueryDocumentText` returns the full text as a one-shot
   `Viewport::document_text` reply — this is how polariton's `content()` reads
   the SQL back out. Engine-level tests cover the SQL replace → edit → query
   round-trip and the file-association reset.
2. **Minimal iced bridge in polariton.** ✅ Foundation done (2026-07-18):
   `crates/polariton/src/editor.rs` owns one long-lived engine, drains the
   viewport receiver on a non-blocking 16 ms iced tick (coalescing projections),
   translates keyboard input, renders focused-pane tokens in a fixed-width Iced
   scrollable, and maps window pixels to the engine cell grid. Polariton's
   run/save/load paths now use the bridge's replace/query text round-trip. The
   bridge-level SQL round-trip test passes. Mouse input, pane-accurate resize,
   syntax spans, cursor/selection painting, and all remaining visual parity are
   intentionally deferred to the visual layers below.
3. **Visual layers.** ✅ Done (2026-07-18): `editor` maps
   `PaneViewport` highlight spans and the engine UI palette to iced rich-text
   fragments, renders engine-projected line numbers, selections, diagnostics,
   folded placeholders, and the primary cursor. The engine remains authoritative
   for wrap rows, wheel scrolling, minimap navigation, and click-to-cursor
   placement. `AppState` derives the code viewport from Polariton's actual pane
   grid after window and splitter changes, so libaryon's wrapping geometry tracks
   the rendered code pane.
3a. **Upstream follow-up:** `UiPalette` has no selection color, so the bridge
   hardcodes the selection background; add a `selection` field to libaryon's
   `UiPalette`/`UiColorOverrides` and consume it in polariton.
4. **Parity gate, removal, and rename.** ✅ Done (2026-07-18): bridge tests cover
   in-memory SQL replace/query, rapid typing convergence, modal `dw`/undo/`yy`/
   put editing, Insert-mode system paste, Normal-mode `Ctrl+V` block selection,
   scrolling, minimap navigation, wrapping, and mouse cursor placement. The old
   `crates/polariton/src/editor/` has been deleted; `DEJAVU_SANS_MONO` now lives in
   `fonts`, and the bridge is the app-facing `crates/polariton/src/editor.rs` with
   `CodeEditor`, preserving an engine-agnostic surface for a later
   `baryon-iced` extraction.

## Risk

Medium-high for the renderer/input work, low for app wiring. Baseline verified:
`cargo check -p polariton -p libaryon` passes on a clean tree. Early checks worth
doing during milestone 2: vim-parity coverage of libaryon's modal layer against
polariton's current vim bindings, and clipboard behavior through the engine.
