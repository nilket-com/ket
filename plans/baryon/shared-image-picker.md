# Shared image picker

## Problem

`Frontend::new` constructs the minimap, diff bridge, UAST graph, and Markdown preview controllers.
Each controller calls `ratatui_image::picker::Picker::from_query_stdio()` before it starts its worker.
That repeats terminal capability detection four times on the UI thread. Each attempt writes graphics,
device-attribute, cell-size, and status queries and can wait up to two seconds for a reply.

The startup PTY probe confirmed four capability-query rounds before the first content-bearing paint.
The terminal’s graphics capability and cell size are properties of this frontend session, so asking
separately gives no additional information.

## Decision

Perform capability detection once in `Frontend::new`, after Baryon has entered the alternate screen.
If detection fails, use `Picker::halfblocks()` as today. Clone the resolved `Picker` once for each
controller worker:

- `MinimapController::new(picker: Picker)`
- `MarkdownPreviewController::new(picker: Picker)`
- `DiffBridgeController::new(picker: Picker)`
- `UastGraphController::new(picker: Picker)`

`Picker` is `Clone` and contains the resolved protocol, font size, background, tmux flag, and
capabilities. Each worker owns its clone and continues to build its own `Protocol` values. This does
not introduce shared mutable frontend state or move terminal I/O off the UI thread.

Keep the font-size extraction in the minimap and Markdown constructors, but derive it from the
passed picker. No controller re-queries stdio.

## Verification

1. Extend `startup_latency_pty_test` to count capability-status queries (`ESC[5n`) emitted before
   the M4 output proxy succeeds. Assert exactly one in both S1 and S2. `ESC[5n` is issued by
   `Picker::from_query_stdio`; use it rather than cursor-position traffic, which Ratatui owns.
2. Retain the existing pty terminal responses and the S1/S2 M0-M4 and M4-M5 budgets. Their passing
   proves one query still selects a usable protocol and every worker can render with its clone. The
   picker-query count is reset when the child starts and checked at M4 in every test run; it is a
   correctness invariant, independent of whether timing budgets are enforced.
3. `cargo-ctrl` passes `--nocapture` to controlled integration tests so their startup JSON reaches
   the runner log.
3. Run the existing Baryon UI/unit test suite. No product behavior or fallback changes: a terminal
   that does not answer still falls back once to halfblocks instead of timing out four times.

## Scope

This is a frontend startup cleanup, not a terminal-capability redesign. It does not cache results
across Baryon processes, change `ratatui-image`, or add image-controller coordination. Add a
separate plan if capability refresh after terminal attachment or resize becomes necessary.
