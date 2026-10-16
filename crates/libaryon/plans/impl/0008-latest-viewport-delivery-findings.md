# ADR 0008 implementation findings: latest-viewport delivery

Companion to `0008-latest-viewport-delivery.md`. Records the field audit, where the
`impl: adr 0008` commit departs from the plan, and what it measured.

## Delivered

- `engine/viewport_channel.rs`: `channel()`, `ViewportSender`, `ViewportReceiver`. The receiver has
  `recv`, `recv_timeout`, `try_recv` and `try_iter` with `std::sync::mpsc` error types. It includes
  the carry-forward rule, the reply queue bounded by `ENGINE_REQUEST_QUEUE_LIMIT` (now `pub(crate)`),
  and a final frame that stays readable after the sender drops.
- Engine, render and test support use the new types, and `EngineHandle::take_viewports` returns a
  `ViewportReceiver`. The headless example and the startup-latency test switched types.
- Baryon: `poll_engine_item` takes a `try_recv` closure, so it serves the viewport receiver and its
  existing integer-channel test.
- Polariton: the merged `engine_outputs` stream is split into an ordered host-event channel and a
  latest-wins viewport channel fed by the forwarder thread. `EngineOutput` is removed.
- Tests:
  - 6 channel unit tests;
  - polariton `document_text_replies_survive_a_stalled_poll_in_request_order`;
  - `tests/latest_viewport_test.rs`.

  Each fails when its rule is broken:
  - treating no frame as a reply loses polariton's first content reply;
  - treating every frame as a reply leaves two frames waiting after the stall.

## Field audit (plan 4.1)

Only two fields are set from a `take()` at render time and therefore appear in exactly one frame:

- `status_message` (`render.rs`, `ui.status_message.take()`);
- `document_text` with `document_text_buffer_id` and `document_text_state_id`.

`should_quit` is latched (`ui.pending_quit` is never cleared) and rides on the engine's last frame.
Every other `Viewport` and `PaneViewport` field is recomputed from engine state on each render, so it
is a snapshot. That includes `confirm_prompt`, `picker`, `input`, `yank_flash` (copied, and cleared by
a timer), minimap, markdown cells and previews, the UAST graph, diff pairs, diagnostics, git state and
host-panel revisions. No clipboard, bell, scroll or jump requests travel in a viewport.

Consumers:

- **Baryon** derives the busy-spinner restart and the completion Pending to Idle status clear from
  consecutive frames. Skipping a frame skips only that animation, as ADR 0008's consequences state.
- **Polariton** pops one pending text query per `document_text` reply (`poll`). Reply frames are
  never replaced, and the polariton test covers this. Host events are drained before the viewports in
  each `poll`. `DocumentEditResult` queues separately, and panel reconciliation runs after the latest
  viewport, so `poll` relies on no order between the two streams.

## Departures from the plan

1. **The channel machinery is a crate-private generic over a `Frame` trait.** `Viewport` implements
   it, and the unit tests use a small frame type. `Viewport` derives nothing, not even `Default`, so
   the tests could not build frames cheaply. Adding derives to a large public struct only for tests
   was the worse change. The public API is the concrete `ViewportSender` and `ViewportReceiver`
   wrappers, so no crate-private bound leaks into a public type (Codex review).
2. **`poll_engine_item` takes a closure** instead of a receiver type, so baryon's existing
   disconnection test keeps using a plain `mpsc` channel.
3. **The engine regression test uses a 64 MiB sparse file in debug.** That scan produces only a few
   frames in the 2 s stall, so the queue-every-frame mutation leaves two frames waiting, not hundreds.
   The assertion is exact (one frame), so the margin is enough. The 64 GiB measurement below shows
   the scale.

## Measurements (development machine, release, 64 GiB newline-free sparse file, 100x30, 15 s hold, no reader)

| Mode | Before (ADR 0007) | After |
| --- | --- | --- |
| wrap on (default) | 357 frames queued, 425 MB peak RSS | 1 frame queued, 62 MB |
| wrap off | 1,230 frames queued, 64 MB | 1 frame queued, 58 MB |

## Test baseline

Plain `cargo test -p libaryon --lib` (all workers, no lane separation) fails one or two
timing-sensitive tests per run at HEAD as well as with this change. A different test fails each run.
Examples: `search_word_under_cursor_*` at HEAD; `scroll_viewport_to_line_reaches_end_of_wrapped_document`,
`rune_source_reload_is_atomic_and_unload_retires_the_generation` and
`goto_definition_rejects_a_response_after_the_requesting_view_is_closed` with it. Each passes alone, and
`scripts/cargo-ctrl --locked` passes both lanes. In the Rune reload test each status wait follows a
single command that produces one status, so it does not depend on two statuses surviving one unread
window.

## Pre-existing, not touched

- `architecture_test::product_0059_store_stays_capability_closed_and_later_consumers_are_explicit` fails
  at HEAD (see `0007-long-line-projection-findings.md`).
- `cargo fmt --check -p polariton` fails at HEAD in `gui/state.rs`, `gui/state/local_rune_tests.rs` and
  `main.rs`. The lines this commit changed are formatted.
