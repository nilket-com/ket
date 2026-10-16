# ADR 0008 implementation: latest-viewport delivery

Decision: `plans/adr/0008-latest-viewport-delivery.md`.

Scope: replace the unbounded engine-to-frontend viewport channel with a single-slot, latest-wins
channel that never blocks the engine, carries every one-shot payload forward when it replaces an unread
frame, and keeps the receiver API frontends and tests already use. Polariton's forwarding thread gets
the same bound. Follows ADR 0007. Deliverable is one `impl: adr 0008` commit reviewed against
this plan.

## 1. Problem

`EngineHandle::take_viewports` returns a `std::sync::mpsc::Receiver<Viewport>` from an unbounded
channel. The engine sends from one place (`engine/render.rs`, `tx_view.send`). A consumer that stops
reading, or reads more slowly than the engine renders, queues every frame:

- the startup probe that stopped reading reached about 954 MB in 15 s on a 64 GiB file and, in the
  crashed session, was OOM-killed at 59 GB;
- with wrap on (the default), a newline-free line still publishes one 1 MiB extent per frame, at about
  24 frames a second during a scan: 425 MB in 15 s with no reader (`0007-long-line-projection-findings.md`);
- polariton drains the engine channel on a background thread into its own unbounded `engine_outputs`
  channel (`polariton/src/editor.rs`, the viewport forwarder), so a stalled GUI update path queues the
  same frames one hop later.

The intended contract is already coalescing. `take_host_events` documents its stream as the one that
"must not be coalesced", unlike viewports, and polariton's `poll` keeps only the latest viewport. What is
missing is a bound.

## 2. One-shot payloads

A viewport is mostly a snapshot, but some fields describe an occurrence that a frontend must observe
even if the frame carrying it is replaced before it is read:

| Field | Meaning | On replacement of an unread frame |
| --- | --- | --- |
| `document_text`, `document_text_buffer_id`, `document_text_state_id` | one-shot reply to `QueryDocumentText`; polariton pops one pending query per reply | carried forward when the newer frame has none; if both carry one, the slot keeps a queue of replies (see 3.3) |
| `should_quit` | terminal transition | OR-ed |
| `status_message` | baryon and polariton keep the latest `Some` until replaced | the older `Some` is carried forward when the newer frame has `None` |

The busy edge (baryon resets its spinner when `busy` turns on) and input completion's Pending to Idle edge
are derived from the frontend's current frame and the new one, so a skipped intermediate frame changes
only what is animated, not what is shown. That is acceptable and is the documented coalescing contract.

The plan's audit step (4.1) confirms this list against every `Viewport` field before implementation;
any other occurrence-like field joins the table.

## 3. Change

### 3.1 Channel

A new `engine::viewport_channel` module provides `ViewportSender` and `ViewportReceiver` around one
`Mutex<Slot>` and a `Condvar`. `Slot` holds the pending frame, disconnection state, and the carried
document-text replies.

- `ViewportSender::send(viewport)` never blocks on the reader: it merges into the pending frame
  (3.2) or stores it, then notifies. It returns `Err` once the receiver is gone, as
  `mpsc::Sender::send` does, so the engine's existing `let _ =` is unchanged.
- `ViewportReceiver` keeps the method names and error types frontends and tests use: `recv`,
  `recv_timeout`, `try_recv` (returning `mpsc::RecvError`, `mpsc::RecvTimeoutError`,
  `mpsc::TryRecvError`), and `try_iter`. Disconnection is reported once the slot is empty and the
  sender is dropped.
- `EngineHandle::take_viewports` returns `Option<ViewportReceiver>`. Its doc states the contract:
  frames may be coalesced; one-shot payloads are never lost.

### 3.2 Merge

When `send` finds an unread frame, the newer frame replaces it and inherits per section 2: `should_quit`
OR-ed, the older `status_message` when the newer has none, and the older document-text reply when the
newer has none.

### 3.3 More than one document-text reply

Two `QueryDocumentText` replies can be produced before a reader looks. The slot keeps them in order
and `recv` yields one frame per reply: the frames carry the replies oldest first, and only the last
carries the latest snapshot. So a frontend that pops one query per reply still sees every reply.

The engine's request queue (`ENGINE_REQUEST_QUEUE_LIMIT`, 2,048) bounds commands not yet processed, not
replies already produced: a frontend that keeps querying without reading would grow the reply queue.
The slot therefore holds at most `ENGINE_REQUEST_QUEUE_LIMIT` replies. A send that would exceed it waits
until the reader takes a frame. That is the only case in which the engine blocks on the viewport path,
and it blocks only on replies its own frontend requested and is not reading.

### 3.4 Polariton forwarder

The forwarder thread stops queueing every viewport. It holds received frames in a shared slot with
the same merge and reply rules, so reply order within the viewport stream is preserved, sends one wake,
and `poll` takes the slot. Host events keep their ordered unbounded channel. No ordering between the
two streams is assumed: if the audit (4.1) finds that a host event must precede a particular reply,
the forwarder represents that relation explicitly rather than relying on wake timing.

### 3.5 Callers

Baryon's `Frontend` (`rx_view` field and `poll_engine_item`), the headless example, the startup and
long-line tests, and the engine's test-support constructors switch from `mpsc` types to the new ones.
No frontend logic changes beyond the type.

## 4. Verification

1. **Audit**, recorded in the findings note: every `Viewport` field classified as snapshot or
   occurrence; every consumer of `take_viewports` and of `engine_outputs` checked for frame-by-frame
   assumptions.
2. **Unit, channel**: a stalled reader holds at most one pending frame across many sends;
   `should_quit`, a status message and a document-text reply survive replacement; two replies produce
   two frames in order; past the reply bound `send` waits for the reader; `send` after the receiver drops errors; `recv` reports disconnection after the
   last frame; `recv_timeout` times out.
3. **Polariton**: issue ordered document-text requests while `poll` is stalled, then poll; every
   reply arrives, in request order, and at most one snapshot frame is held.
4. **Engine regression**: the startup probe scenario as a test: 64 MiB newline-free sparse file, wrap
   on, the reader sleeps through the background scan, then reads. At most one frame is queued, and the
   process's anonymous memory stays bounded.
5. **Suites**: `scripts/cargo-ctrl --locked`, baryon tests including the pty test, polariton tests,
   and libaryon integration tests. A test that relied on seeing every intermediate frame is fixed to wait
   for the state it needs and listed in the findings.
6. **Measurement**: the 64 GiB probe without draining, wrap on and wrap off: peak RSS over a 15 s
   hold, against `0007-long-line-projection-findings.md`.

## 5. Out of scope

- Frames emitted for an unchanged projection during scans (frame rate): measured after this.
- Moving one-shot replies to the host-event stream. That would be cleaner, but it changes polariton's
  query protocol; the carry-forward rule keeps the current protocol correct.
- Wrap-on rendering of lines longer than one extent.
