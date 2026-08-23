# ADR 0091 - Typed mid-stream query errors

**Status:** Proposed 2026-08-23. Follow-up banked from the ADR 0090 seal: a real
storage defect surfaced to clients as bare `storage unavailable: early eof`, and
diagnosing it required source-level probes because the actual cause never left
the server. This record changes how execution errors are reported, not how any
query executes.

## Context

A streaming table query that fails server-side after dispatch propagates its
`io::Error` up through `handle_data_stream`, and the router closes the stream
without writing any frame. The client's `read_frame` then fails with
`UnexpectedEof("early eof")`, which Toron maps to `storage unavailable` — three
layers of translation, each discarding the cause. During the ADR 0090 seal, a
placement cross-check refusal with a precise, actionable message ("record frame
declares partition … but its index entry reads as raw-locality") reached the
operator as nothing but `early eof`; finding it required patching probes into
the dispatch seam and rebuilding.

The wire already supports better. The streaming client accepts a terminal frame
— `STREAM_END` with a `QueryResponseMeta` — at any point in the stream, and maps
a non-OK status to a typed error; `table_query_error_frame` builds exactly that
shape and is already used for pre-dispatch refusals (authorization, malformed
meta). Only errors raised *during execution* bypass it.

## Decision

Every server-side execution error after dispatch reaches the client as a typed
terminal frame carrying a `ReadStatus`, whether it occurs before the first data
frame or between data frames. The bare unframed close is reserved for the one
case where writing is impossible: the transport itself has failed.

The status is a typed code, never prose; clients must not be given error text as
their only discriminator. Structured detail may ride the terminal frame's
trailing section, as the table-reference-integrity mechanism already does.

The server logs the error at the dispatch seam — the point where today it is
silently discarded — so a failure is diagnosable from the server's own log even
when the client is not in hand.

## Acceptance

- An execution error injected mid-stream (after at least one data frame) reaches
  the client as a typed error naming its status, not as `early eof`. The control
  fails on the present dispatch, which closes unframed.
- The same holds for an error before the first data frame.
- A transport-level failure (peer gone, write refused) still surfaces as an io
  error; the control distinguishes the two by killing the connection rather than
  the query.
- The server log at the dispatch seam records the underlying error for a request
  that failed mid-stream.
- Pre-dispatch refusals are unchanged, byte for byte.

## Guardrails

A status is the discriminator; message text is diagnostic only. No client or
test may branch on the message.

The unframed close must not quietly become a second error channel: a new
execution error path that reaches the router as a bare `Err` rather than a
typed frame is the defect this record exists to remove.

## Risks and deferrals

A terminal error frame after data frames changes what a partially-consumed
stream looks like to a consumer that assumed only OK terminals; the streaming
client already errors on non-OK terminal status, so this is a narrowing of an
existing behaviour, not a new one.

Not addressed: retry semantics for mid-stream failures, and whether Toron should
resume a partially-received scan. Those are consumer policy, not reporting.

## Forward

Implement in the table-query streaming dispatch first — the path the 0090
diagnosis went through — then audit the other `handle_data_stream` families
(query, bulk write, stream-fin) for the same unframed-close shape.
