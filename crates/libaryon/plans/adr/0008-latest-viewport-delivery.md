# ADR 0008: deliver the latest viewport

Status: accepted 2026-10-16, converged by the KET council (user, Claude, Codex). Implementation plan:
`plans/impl/0008-latest-viewport-delivery.md`.

## Context

The engine publishes viewports on an unbounded `std::sync::mpsc` channel. A frontend that stops
reading, or reads more slowly than the engine renders, queues every frame. A stalled reader reached
954 MB in 15 s on a 64 GiB file, and the crashed session's probe was OOM-killed at 59 GB. With wrap on
(the default), a newline-free line still publishes one 1 MiB extent per frame during a scan (ADR 0007).
Polariton drains the engine channel into its own unbounded queue, so a stalled GUI update path queues
the same frames one hop later.

The intended contract is already coalescing: the host-event stream is documented as the one that must
not be coalesced, and polariton keeps only the latest viewport. What is missing is a bound. Two
viewport fields are occurrences the engine emits in exactly one frame: `status_message`, and the
`document_text` reply to `QueryDocumentText` with its buffer and state ids. Polariton pairs one pending
query with each reply.

## Decision

1. The viewport stream is a single-slot, latest-wins channel. An unread snapshot is replaced by the
   next one, and the engine never blocks on a snapshot.
2. A replaced frame hands on its occurrences: `should_quit` is OR-ed and its `status_message` passes to
   a newer frame that has none. A frame carrying a document-text reply is never replaced. Replies queue
   in order, and every reply is older than the pending snapshot.
3. At most `ENGINE_REQUEST_QUEUE_LIMIT` replies are held. Past that, the engine waits for the reader,
   blocking only on replies its own frontend requested and is not reading.
4. A pending frame stays readable after the engine drops its sender, so the final frame is not lost.
5. The receiver keeps `std::sync::mpsc` method names and error types. Polariton's forwarder uses the
   same channel, and its host events keep their ordered stream. No order between the two streams is
   relied on.

## Rationale

The bound belongs in the channel, not in each frontend, so every consumer gets it, including tests
and tools that stop reading. Carrying occurrences forward keeps the current frontend protocols correct
without moving document-text replies to the host-event stream, which would be a protocol migration
inside a memory fix.

## Consequences

- A frontend sees fewer frames. Edges it derives between consecutive frames (baryon's busy-spinner
  restart, the input-completion Pending to Idle status clear) can be skipped. That is cosmetic, and it is
  the documented coalescing contract.
- An occurrence added to `Viewport` later must be added to the channel's carry-forward rule.
- A frontend that issues 2,048 unread document-text queries stalls the engine until it reads.
