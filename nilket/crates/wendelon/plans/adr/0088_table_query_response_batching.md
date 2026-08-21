# ADR 0088 - Table-query response batching

**Status:** Proposed 2026-08-21. Responds to the post-fix PRODUCT-0137 scan
evidence and its Toron-side diagnostic. It changes response framing, not table
layout, materialization, or the SQL result contract.

## Context

The scatter-threshold rerun removed two false leads. Ordinary CTAS and
`materialize` tables read at the same approximately 460 rows per second, and
the Arrow-only Polariton path costs the same as text rendering. The problem is
therefore neither the materialization certificate nor CLI presentation.

The direct diagnostic locates a concrete part of the cost. Against a live
in-process Wendelon node, bypassing the Toron daemon and SQL wire, it bulk
wrote 5,000 two-`Float64` rows in 14.688 ms, then read the same rows in 1.078 s
as **5,000 result batches**. `WendelonStorageScan` is a batch pass-through and
`StreamingQueryResult::next_batch` consumes one HRFT frame per batch. The
per-row batches therefore survive to the SQL response path, where they add a
second per-frame cost. This does not prove that every remaining read cost is
framing, but it proves a framing cost paid once per row.

The source is explicit. `SingleRow` is the default table layout; its write path
stores each logical row as its own location. Descriptor-backed whole-record
queries materialize, filter, and execute one location, then immediately call
`TableQueryResponseStreamWriter::emit_batch` before reading the next. The
current Toron integration test pins that behavior: `LIMIT 3` returns three
one-row batches and a streaming consumer can drop after three frames.

That immediate emission was a useful bounded-consumer property, but it made
the storage unit accidentally become the response unit. It is not part of the
table-query value contract: clients receive ordered Arrow rows with a schema
and a terminal `QueryResponseMeta`, not a promise of one frame per location.
The old test is consequently a behavioral boundary to change deliberately,
not evidence that a Toron-side rebatcher should preserve it.

The HRFT framing layer caps each payload section at 5 MiB. Wendelon's point
row-group discipline already uses a 4,096-row target and an 8,192-row maximum.
Those are appropriate precedents for a bounded response unit, provided the
wire-byte cap remains authoritative for wide values.

## Decision

### 1. Wendelon owns table-query response batching

Wendelon coalesces compatible result rows before they reach
`TableQueryResponseStreamWriter` or the non-streaming `QueryExecutionPlan`
batch vector. Toron remains a streaming batch consumer; it must not add a
post-scan concatenation layer to conceal a Wendelon protocol that still emits
one Arrow frame per stored row. Other Wendelon clients benefit through the
existing `StreamingQueryResult` API without a new wire message or client API.

This is response batching, not a change to `SingleRow`. A one-row storage
location remains a valid write and locality choice. `MultiRow` is neither
required nor changed by this record.

### 2. A response batch is bounded by rows and encoded bytes

The table-query executor accumulates ordered, schema-compatible result batches
into a response accumulator. It flushes when either of these bounds is met:

- `TABLE_QUERY_RESPONSE_TARGET_ROWS = 4,096`; or
- `TABLE_QUERY_RESPONSE_ESTIMATE_BYTES = 4 MiB` of sliced in-memory size.

**The estimate is not the limit, and must not be mistaken for one.** In-memory
size is not the quantity the wire carries — encoding may compress or expand —
so a bound computed from it can only ever be a heuristic that flushes early.
The hard limit is HRFT's 5 MiB section cap, enforced at the flush seam on the
**encoded** payload:

- flush encodes the combined batch and measures the actual bytes;
- a payload over the cap is halved and retried until each piece fits, so
  batching never turns a legal read into a refused frame;
- only a single row that cannot fit is refused, which is the pre-existing
  oversized-payload condition this record does not change;
- a size-accounting failure propagates. Treating it as zero would silently
  disable the estimate for exactly the batches whose size could not be
  determined.

Because the encode happens at the seam, the writer takes the already-encoded
payload rather than encoding a second time, and the direct `emit_batch` entry
is removed — the accumulator is structurally the only producer of a response
frame, rather than merely the conventional one.

An individual result larger than the row target is sliced into bounded output
batches. An individual result that cannot fit inside the existing 5 MiB frame
discipline retains the protocol's existing oversized-payload refusal; batching
does not invent truncation. The accumulator uses explicit byte accounting and
never grows without a row or byte bound. It preserves input order, schema,
row identity columns, the terminal row count, and the maximum HLC.

The row target is a delivery quantum, not a license to infer cardinality from
a column or to buffer a complete query. The 5 MiB wire limit wins for wide
rows; the row target prevents a tiny-row scan from creating an unbounded
in-memory Arrow batch.

### 3. Bounded reads remain per-location; output framing becomes batched

`LIMIT`, cancellation, budgets, and deadline checks remain in the existing
per-location loop. Once a row limit is satisfied, no further location is read,
even if the accumulator has not reached its target; its tail is flushed before
the terminal response. Cancellation and deadline checks continue before each
next location and before an accumulator flush.

The changed contract is intentional: a streaming consumer receives up to the
response target in one `next_batch()` call, rather than exactly one location.
Dropping after a received batch therefore bounds producer work by the response
quantum, not by one row. It must still stop before whole-query materialization;
the server must never pre-read past the current bounded accumulator merely to
fill an output batch.

The old `LIMIT 3` result becomes one three-row batch, not three one-row frames.
That is the correct observable consequence of separating the limit's row
semantics from physical frame granularity.

### 4. One accumulator serves streaming and collected execution

The same accumulator owns all descriptor-backed table-query result emission.
In streaming execution it flushes through `TableQueryResponseStreamWriter`; in
the non-streaming path it appends the same bounded output batches to
`QueryExecutionPlan::batches`. It is not permissible to batch only one path:
that would make frame count and memory posture depend on whether a caller
happened to request a stream.

Descriptorless schema grouping remains its existing compatibility path. A
schema transition is a flush boundary, never an attempt to concatenate unlike
Arrow schemas.

## Acceptance

- A live `SingleRow` table with 5,000 narrow rows returns the same rows in the
  same order and schema in two response batches (4,096 and 904), rather than
  5,000. The control counts public `next_batch()` results, so it fails if a
  later Toron layer merely hides the frames.
- A matching non-streaming table query produces the same two output batches
  and terminal facts. Streaming and collected execution cannot drift.
- `LIMIT 3` returns exactly three rows in one tail batch and proves that no
  additional source locations were acquired after the third row.
- A consumer that receives one batch and drops a scan larger than one quantum
  does not cause the producer to materialize the full source. The test observes
  bounded source progress, not merely that the consumer discarded rows.
- A wide-value fixture whose rows are individually small but collectively
  encode past 5 MiB is split, and every emitted batch is re-encoded and
  asserted under the cap. The control checks its own premise — that the
  fixture really does encode past the cap, and that its row count stays under
  the row target so bytes are demonstrably the bound in play — because a
  fixture that compresses under the cap would pass while proving nothing.
- A single row that cannot fit is refused by name rather than truncated or
  emitted for a receiver to reject.
- Cancellation and a query deadline observed while an accumulator is nonempty
  return their existing typed terminal outcomes and emit no later data batch.
- Toron's `WendelonStorageScan` remains a `next_batch()` pass-through. A source
  guard rejects a Toron-side response rebatcher, and the revised integration
  controls no longer assert one result batch per location.

## Guardrails

The accumulator has exactly two flush bounds — the 4,096-row target and the
4 MiB in-memory estimate — over one hard limit, the 5 MiB encoded HRFT cap.
Any new bound or an unbounded collection requires an explicit record. A direct
descriptor-backed emit that bypasses the accumulator is not merely forbidden:
the entry point no longer exists, so the guardrail is structural.

Batch boundaries are transport performance facts, not query semantics. Tests
must assert row order, schema, row count, terminal facts, `LIMIT`, and bounded
producer work; they must not reinstate per-location frame count as a proxy for
correct cancellation.

The existing ignored throughput diagnostic stays an instrument, not a
wall-clock CI gate. Its durable mechanism control is batch count: performance
claims must show fewer frames than rows before attributing a rate improvement
to this decision. Fewer frames is necessary and not sufficient — the in-process
rerun shows the mechanism changing with the rate flat, so a rate claim needs
its own measurement on the path where the frames were being paid for.

## Risks and deferrals

The first batch may wait for up to one response quantum, and a consumer that
used one-row frames for cursor-like behavior now receives a bounded batch.
That is an accepted response-framing change. A pull-based row protocol or a
latency-based partial-flush policy would be a separate API and scheduling
decision.

This record does not batch storage reads, rewrite `SingleRow` data into
`MultiRow`, change scatter thresholds, or establish a ten-million-row read
claim. The direct diagnostic still charges per-location storage materialization
and PBC execution; after framing is amortized, profile the remaining read cost
before choosing a row-group or storage-layout cut.

## What the direct rerun does and does not show

Measured after implementation, on the same in-process diagnostic that found the
defect: 5,000 rows return in **2 response batches** instead of 5,000. The
framing mechanism changed exactly as this record specifies.

It did **not** get faster in that setting: 4,505 rows/s after, against 4,638
rows/s before — effectively unchanged, and if anything marginally slower. That
is not a failure of the decision; it is the scope of the decision. The
diagnostic runs in-process, with no Toron daemon and no SQL wire, so the
per-frame cost it removes was already small there. What it still charges is
per-location storage materialization and PBC execution, which this record
explicitly does not address.

The expected win is downstream, where each per-row Wendelon frame also became a
per-row Toron SQL frame and paid a second hop — the ~460 rows/s the viz slice
measured through the daemon. That win is **predicted, not demonstrated**. No
performance claim may be attributed to this record until the daemon-path rerun
supplies the number.

## Forward

The daemon-path rerun is an evidence gate, not a formality: until it reports a
rate, this record has changed frame count and nothing else that has been
measured. Rerun the viz materialized-versus-plain control after this lands. It must first
clear the 50,000-row scatter threshold within the admitted statement timeout
before threshold tuning resumes. If the remaining per-location read cost is
still material at that point, measure `SingleRow` and `MultiRow` under the same
schema and cardinality and decide storage read batching separately.
