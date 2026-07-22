# Specification: Headless Baryon

## Overview

Split Baryon into a headless engine library and a terminal frontend so the same editor engine
drives both the existing standalone TUI and an iced GUI embedding (polariton). The deliverable is a
two-crate workspace:

- **`libaryon`** — the headless library: `core`, `ecs`, `engine`, `svp`, `uast`, plus the theme,
  color, and settings modules that currently live in `ui` but are engine concerns. No terminal
  dependencies. This is what polariton imports.
- **`baryon`** — the standalone terminal frontend binary: `ui`, `app`, `main`, with a `[[bin]]`
  named `baryon`.
  Owns ratatui, crossterm, ratatui-image, and the cargo-deb packaging metadata.

Two crates, not one with feature flags and not three. Feature flags keep the headless boundary
honorary: features are additive, and the boundary regresses silently the first time an engine file
gains a `use ratatui::` import. A crate boundary makes the compiler enforce it, and polariton never
compiles ratatui or crossterm at all. A third crate (`baryon-iced`) is not warranted: the iced
frontend should be born inside polariton against the `libaryon` API and extracted only if a
second iced consumer ever appears. A bin and lib coexist in one crate, so "standalone TUI plus
library" needs exactly two. `libaryon` makes the embedding boundary explicit while reserving the
clean `baryon` package name for the application users install and run.

Every commit is zero behavior change: the TUI binary behaves identically before and after the
split. Follow-up scope, explicitly out: the iced frontend itself, a frontend-neutral modal keymap,
an async channel adapter, and per-language feature gates on the heavy dependencies.

## 1. Ground truth

- The engine already runs headless. `App::run` wires three mpsc channels —
  `Sender<EditorCommand>` in, `Receiver<Viewport>` out, plus an IO-notify channel bridged to
  `EditorCommand::InternalRefresh` — and spawns the engine on its own thread
  (`src/app.rs:85-125`). The frontend never touches engine state directly; `Frontend` holds only
  the terminal, the channel pair, and input state (`src/ui/core.rs:28`).
- `src/engine/test_support.rs` already drives the engine through this seam in tests, so the
  library boundary is exercised today; the split makes it structural.
- The `Viewport` projection is token/span-based (`RenderToken`, `HighlightSpan`, `VisibleRow`),
  not a terminal cell grid, so it is medium-neutral already.
- Exactly one ratatui type escapes `ui`: `ratatui::style::Color` used as a value type in
  `src/uast/projection.rs:10` (fields at lines 138, 145, 166) and `src/svp/projector.rs:4`.
  Every color the engine produces is `Color::Rgb`: themes sample colormaps into RGB
  (`src/ui/theme.rs`, 329 `Color::Rgb` occurrences; the single `Color::Reset` at line 124 is an
  array initializer that is always overwritten), and `hex_to_color` only builds `Color::Rgb`.
- The engine reaches backward into `ui` for exactly four things, all frontend-neutral:
  - `Theme` (`src/engine/session.rs:20`, `src/engine/runtime/system.rs:18`) — colormap sampling,
    no terminal types beyond `Color` values.
  - `hex_to_color` / `color_to_hex` / `SEARCH_MATCH_BG` (`src/engine/render.rs:29`,
    `src/ui/colors.rs:5-72`).
  - `DEFAULT_SEARCH_HIGHLIGHT_HEX` (`src/ui/colors.rs:70`, used across engine runtime, semantic,
    session, and test_support).
  - `settings::persist_ui_settings` (`src/engine/runtime/system.rs:18`). `src/ui/settings.rs`
    imports only `atomic_file`, theme/color helpers, and std — no terminal dependencies.
- ratatui-image is confined to `ui` (`diff.rs`, `draw.rs`, `minimap.rs`, `markdown_preview.rs`).
  `src/engine/markdown_image_preview.rs` and `src/engine/minimap.rs` produce frontend-neutral
  source/snapshot data; rasterization and the `image`, `font8x8`, `fontdb`, `fontdue`, and
  `pulldown-cmark` dependencies all remain in the TUI today. Clipboard is arboard
  (`src/engine/clipboard.rs`) — platform clipboard, not terminal; it stays in the library.
- Today there is one package: `src/lib.rs` exposes all modules, `src/main.rs` calls
  `baryon::ui::startup::run`, and `[package.metadata.deb]` ships `target/release/baryon`
  (`Cargo.toml:9-15`).
- Sibling conventions: nilket is a workspace with `crates/*` members, resolver 3, and shared
  release profiles at the workspace root. polariton is iced 0.14 + tokio and will consume the
  `libaryon` via a path dependency.

## 2. Crate topology

```
baryon/
	Cargo.toml            # [workspace] members = ["crates/*"], shared [profile.release]
	crates/
		libaryon/           # lib: core, ecs, engine, svp, uast, theme, settings
			Cargo.toml
			examples/headless.rs
			src/
		baryon/             # bin "baryon": ui, app, main
			Cargo.toml        # [[bin]] name = "baryon"; [package.metadata.deb]
			src/
```

- The workspace root inherits nilket's shape: `resolver = "3"`, `[profile.release]` with fat LTO
  moved from the package to the workspace, `[workspace.dependencies]` for versions shared by both
  crates.
- `crates/libaryon/Cargo.toml` keeps the dependencies actually used by `core`, `ecs`, `engine`,
  `svp`, and `uast`: arboard, crossbeam-queue, imara-diff, memchr, ra_ap_*, regex-automata,
  ruff_*, rune, rustc-hash, sqlparser, triomphe, io-uring, and any other dependency confirmed by
  the split compiler pass. It must not list ratatui, crossterm, ratatui-image, or the TUI-only
  rasterization/Markdown dependencies.
- `crates/baryon/Cargo.toml` keeps: ratatui, crossterm, ratatui-image, image, font8x8,
  fontdb, fontdue, pulldown-cmark, plus `libaryon = { path = "../libaryon" }`. The compiler and
  `cargo machete` (if available) are the authority for the final direct-dependency lists; shared
  versions live in `[workspace.dependencies]`, but each crate still declares every dependency it
  imports directly. The deb metadata moves here; `cargo deb -p baryon` still ships
  `/usr/bin/baryon` because the `[[bin]]` target is named `baryon`.
- `baryon` imports through the library root (`use libaryon::engine::...`), which forces the
  public-API audit: every `pub(crate)` item the frontend uses becomes deliberate `pub` surface.
  `test_support` stays `pub(crate)`; TUI-side tests that need engine fixtures use the public
  embedding API instead.

## 3. Neutral color type

Add `Rgb` to `core` (`src/core/color.rs`):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rgb {
	pub r: u8,
	pub g: u8,
	pub b: u8,
}

impl Rgb {
	pub fn from_hex(hex: &str) -> Option<Self> { ... }  // replaces hex_to_color
	pub fn to_hex(self) -> String { ... }               // replaces color_to_hex
}
```

- `ratatui::style::Color` admits `Reset`, named, and indexed variants the engine never produces;
  narrowing to `Rgb` makes projection colors total — the invalid states become unrepresentable
  rather than merely unused.
- All engine-side uses switch: `Theme::syntax_colors`, `PaneViewport::theme_colors` and
  `background_override`, `Viewport::search_highlight`, `HighlightProjector` /
  `DiagnosticProjector`, settings parsing, and the search-highlight constants.
- The TUI converts at its draw boundary with a local function such as
  `fn ratatui_color(rgb: Rgb) -> ratatui::style::Color`. It cannot implement
  `From<baryon::Rgb> for ratatui::style::Color`: both the trait and target type are foreign to
  `baryon`, so Rust's orphan rule rejects that implementation. polariton uses an equivalent
  local conversion to `iced::Color` (f32 RGBA). Since `color_to_hex` returns `None` only for
  non-RGB variants today, the `Rgb`-typed replacement is total and the `Option` disappears at call
  sites — a signature change with no behavior change.

## 4. Relocations out of `ui`

Move, without modification beyond the `Rgb` swap and path updates:

- `src/ui/theme.rs` → `src/engine/theme.rs` (it is engine session state; `session.rs` and
  `runtime/system.rs` already own its lifecycle).
- Color conversion helpers and shared constants from `src/ui/colors.rs` → `src/core/color.rs`.
  Anything genuinely terminal-specific left in `ui/colors.rs` stays.
- `src/ui/settings.rs` and `src/ui/atomic_file.rs` → engine-side modules (`src/engine/settings.rs`,
  `src/core/atomic_file.rs`). Replace the loader-shaped `LoadedSettings` at the public boundary
  with `EngineConfig`; the TUI settings loader produces that config, while another host may build
  it directly. `ui/history.rs` also uses the generic atomic-write helper, so the split must either
  expose that helper deliberately from `libaryon` or retain a TUI-local wrapper; do not discover
  this coupling only during the file move.

Exit criterion: `rg 'crate::ui' src/engine src/uast src/svp src/core src/ecs` returns
nothing. After the crate split this is compiler-enforced; before it, the search is the check.

## 5. Public embedding API

The wiring in `App::run` (`src/app.rs:81-133`) — channel construction, `UastRegistry`,
`SvpResolver`, the IO-notify bridge thread, `build_language_registry`, the eleven-argument
`Engine::new` call, and the engine thread spawn — is the embedding boilerplate every frontend
needs. Move it into the library:

```rust
pub struct EngineConfig {
	pub settings: EngineSettings,
	pub initial_file: Option<String>,
}

pub struct EngineHandle {
	commands: mpsc::Sender<EditorCommand>,
	viewports: Option<mpsc::Receiver<Viewport>>,
	engine_thread: Option<thread::JoinHandle<()>>,
}

impl EngineHandle {
	pub fn spawn(config: EngineConfig) -> EngineHandle { ... }
	pub fn command_sender(&self) -> mpsc::Sender<EditorCommand> { ... }
	pub fn take_viewports(&mut self) -> Option<mpsc::Receiver<Viewport>> { ... }
	pub fn shutdown(self) -> thread::Result<()> { ... }
	pub fn detach(self) { ... }
}
```

- `EngineConfig` owns `EngineSettings` rather than mirroring its fields, and replaces the
  positional `Engine::new` arguments at the public boundary with a stable `Default`; `App::run`
  shrinks to settings load → config construction →
  `EngineHandle::spawn` → terminal setup → `Frontend::run`.
- The handle owns engine lifecycle as well as channels. Define and test explicit shutdown
  semantics around the existing `EditorCommand::Quit`. `shutdown` sends `Quit` and joins the
  engine, resolver, semantic, and bridge workers. `Drop` must never block or discard shadow
  recovery state: it requests a non-destructive internal termination and detaches. The standalone
  TUI uses `detach` after its own `Quit` request to retain its historical process-owned exit
  behavior. The receiver accessor consumes that single field so iced can move the single-consumer
  `mpsc::Receiver` to its subscription worker while the lifecycle handle remains.
- The channels stay blocking `std::mpsc`. iced 0.14 subscriptions wrap a blocking receiver on a
  dedicated thread without trouble; an async adapter is follow-up scope, not a blocker.
- `crates/libaryon/examples/headless.rs` is the living proof: spawn the engine, load a file, send
  edit and search commands, print the projected text of the focused pane, exit. It compiles with
  zero terminal dependencies and doubles as the embedding documentation.

## 6. Invariants and tests

- The full existing suite passes unchanged at every commit; tests move with their modules.
- The existing `ui/colors.rs` tests already pin 3/4/5/6-digit parsing, invalid input, optional
  `#`, case, RGB round-trips, and rejection of non-RGB ratatui variants. Move those tests with the
  implementation and preserve their behavior through the `Rgb` swap; add cases only for an
  uncovered contract.
- Before any relocation: a headless integration test (the example's assertions as a test) pins the
  channel seam — commands in, projected text and colors out — so later commits cannot silently
  change the embedding contract.
- After the split: `cargo tree -p libaryon -e normal` contains neither `ratatui` nor `crossterm`;
  `cargo build -p libaryon` succeeds alone; `cargo run -p libaryon --example headless` runs on a
  sample file.
- TUI parity: existing `TestBackend` snapshot and key-dispatch tests (`src/ui/key_dispatch_tests.rs`)
  pass unmodified in `baryon` after the move.

## 7. Commit sequence

Run `cargo fmt`, the full suite, and strict Clippy after every commit.

1. `test: pin headless engine seam` — add the headless integration test driving the engine through
   channels only; keep the existing color-conversion characterization tests passing unchanged.
2. `refactor: replace ratatui Color with core Rgb in engine state` — introduce `core::Rgb`; swap
   projection, projector, theme, settings, and engine call sites; `baryon`-bound draw code
   converts through its local adapter function. Zero behavior change.
3. `refactor: move theme, settings, and color helpers out of ui` — relocations from section 4;
   engine no longer imports `crate::ui`. Zero behavior change.
4. `refactor: extract engine host wiring from App` — `EngineConfig` + `EngineHandle::spawn` in the
   engine; `App::run` becomes a thin caller. Zero behavior change.
5. `refactor: split workspace into libaryon and baryon crates` — workspace root, file moves,
   public-API audit, deb metadata and profile relocation, headless example. Zero behavior change;
   the binary is still `baryon`.
6. `docs: document the embedding API` — README, lib-level rustdoc on `EngineHandle` /
   `EditorCommand` / `Viewport`, and the polished example.

## 8. Verification

- Automated, every commit: `cargo fmt --all -- --check`, `cargo check --workspace --all-targets`,
  `cargo clippy --workspace --all-targets -- -D warnings`, full workspace test suite.
- After commit 5: `cargo tree -p libaryon -e normal | rg 'ratatui|crossterm'` is empty;
  `cargo run -p libaryon --example headless` works; `cargo deb -p baryon` produces a package
  installing `/usr/bin/baryon`.
- Manual TUI parity session on the split binary: open a Rust file, edit, undo, search, split
  panes, toggle minimap and markdown preview, run a diff, switch themes, persist a settings
  change, yank to system clipboard, quit. Behavior must match the pre-split binary exactly.
- Acceptance for the library: a polariton spike (out of tree, not part of this plan's commits)
  adds `libaryon` as a path dependency, spawns `EngineHandle`, and renders one `Viewport` in an
  iced widget. Friction found there feeds the follow-up plans below rather than expanding this
  one.

## 9. Follow-up scope

- **`baryon-iced`**: born inside polariton; extracted to a crate only on a second consumer.
- **Frontend-neutral modal keymap**: the vim state machine (`InputState`, `NormalPending`,
  `src/ui/keymap.rs`) is frontend-independent but written against crossterm `KeyEvent`. Moving it
  into the library over a neutral `KeyInput` type — with crossterm and iced translators — is its
  own plan; until then polariton drives `EditorCommand` directly.
- **Async channel adapter** for tokio/iced subscriptions, if the blocking-receiver thread proves
  awkward.
- **Per-language feature gates** on ra_ap_* and ruff to cut polariton's inherited compile times.
