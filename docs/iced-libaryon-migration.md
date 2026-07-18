# Polariton: replace the in-house editor with libaryon

Status: plan (2026-07-18)
Related: `baryon/plans/headless-baryon.md`, `baryon/plans/input-contract.md`,
`baryon/plans/platforms.md`, `docs/architecture.md`

## Framing

This is an editor-engine migration plus a new iced renderer, not a widget swap.
libaryon is headless: it exposes `EngineHandle::spawn(EngineConfig)`, command and
input senders, and immutable `Viewport` projections
(`baryon/crates/libaryon/src/engine/host.rs`, `src/uast/projection.rs`). Its only
existing renderer is baryon's ratatui frontend. Per `headless-baryon.md`, the iced
frontend is born inside polariton; a `baryon-iced` crate is extracted only if a
second iced consumer appears.

Polariton's app-side coupling is small — one `CodeEditor` in `gui/state.rs:28`,
rendered in one pane, consumed as a `String` for run/save/load, with ~a dozen call
sites funnelled through `CodeEditor` + `EditorMsg`. The in-house editor to retire
is `polariton/src/editor/` (custom `iced::advanced` widget, ropey buffer,
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
7. **Platform.** libaryon's SVP layer uses Linux `io_uring`; polariton inherits
   Linux-only builds until `platforms.md`'s resolver-backend split lands.
   Accepted for now.
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
2. **Minimal iced bridge in polariton** (behind a flag or side-by-side pane):
   engine spawn/shutdown, subscription over the viewport receiver, fixed-font
   monospace text rendering of `PaneViewport` tokens, resize, keyboard input,
   and the run/save/load string round-trip.
3. **Visual layers, incrementally:** syntax colors + gutter → selection + cursor
   → scrolling + wrap → minimap, folds, diagnostics.
4. **Parity gate, then removal.** Parity tests for SQL editing and the
   saved-statement flows (create, load, swap, run). Only then delete
   `polariton/src/editor/` and rewire the remaining call sites to the new facade
   (`new/content/set_content/view/update/subscription`-shaped shim keeps the app
   diff small).

## Risk

Medium-high for the renderer/input work, low for app wiring. Baseline verified:
`cargo check -p polariton -p libaryon` passes on a clean tree. Early checks worth
doing during milestone 2: vim-parity coverage of libaryon's modal layer against
polariton's current vim bindings, and clipboard behavior through the engine.
