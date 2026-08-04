# ADR 341 - Exchange input streaming (output-handling-gated)

## Status
Proposed.

## Context

The streaming substrate's second cut (ADR 339 stage 341). ADR 340 moved q07/q21 off the raw `Join(0)` output wall;
**q21's new wall is exactly the 340 filter accumulator** (1.07 GB), q07's is a later join (1.18 GB). 339 Gate-2 measured
the exchange as "currently materializes, locally pipeable by batch partition-forward." 341 streams the filtered batches
**into** the Exchange instead of accumulating them in the Filter result `Vec`. **q21 is the sharper acceptance target**
(its wall is the accumulator); **q07 is the composition target** (already past to a later join). Wall-mover discipline:
acceptance is **green-or-next-wall**, set by what the executor boundary actually permits.

### Grounded (340 + the code)
- `bound_exchange` (exec.rs:2239): `execute_bound(input) -> full Vec` -> partition into `partition_batches:
  Vec<Vec<RecordBatch>>` -> **return `Vec<RecordBatch>`**. That `execute_bound(Filter)` Vec is q21's wall (the 340
  accumulator the exchange consumes).
- 340's `try_streaming_join0_filter` (exec.rs:1686) currently **returns a `Vec<RecordBatch>`**. Internally it
  materializes contiguous join-output chunks, filters each chunk, then accumulates filtered chunks in
  `materialize_join_selection_chunks_filter_accumulate`. That accumulator is q21's wall. 341 must reuse/refactor the
  chunk/filter logic as a chunk visitor or producer; calling the existing Vec-returning helper as the "stream" is
  explicitly the wrong cut.

## Decision - stream the filter batches into the Exchange, behind a hard output-handling gate

The cut: for the matched `Exchange <- Filter <- Join(0)` shape, pull the ADR 340 filtered chunks one at a time,
**partition each on arrival**, drop the source batch - the full filtered Vec (q21's accumulator) never materializes.
This likely means extracting a shared "materialize join selection chunks and visit filtered batch" helper used by both
the existing Filter-returning path and the new Exchange-input path.

### Phase 0 - the output-handling gate (a DECISION POINT, like 340's selectivity gate)
**Prove whether the Exchange has a consumer path that can avoid returning a full `Vec<RecordBatch>`** - measure the
executor boundary before setting q21's acceptance:
1. **Exchange can partition-forward into its consumer without materializing returned partitions** -> 341 **may target
   q21 green** (the filtered rows flow through the exchange to the consumer without accumulating).
2. **Exchange must still return `Vec<RecordBatch>`** (the materialize-each consumer interface) -> 341 is a
   **wall-mover**: remove the Filter accumulator, **budget + name** the Exchange-output accumulator, and verify the
   next producer via ADR 332. q21-green is then 342's stage (the consumer streamed), not 341's - unless the partitioned
   output is small enough to fit on its own.
- In Branch 2, budget the **total retained returned exchange output**, not only each partition batch. A four-way
  repartition can make individual partition batches look harmless while the returned `Vec<RecordBatch>` still retains
  the full q21 filtered result.
- **Either branch:** the row-order parity test against the current materialized exchange path, **including
  Float64-sensitive data** (the repartition must preserve per-partition fold order).

## Acceptance - green-or-next-wall, set by the gate branch
- **Branch 1 (partition-forward):** q21 **green** (`pass@execute` at sf=1) is the target, measured; if it instead moves
  to a later boundary, that's an honest wall-move named via 332.
- **Branch 2 (must return Vec):** q21's 340 filter-accumulator wall **removed**; the wall moves to the **named,
  budgeted** Exchange-output accumulator (the 340 no-silent-wall lesson), verified via 332 retention. **No silent OOM.**
- **q07 (both branches):** its 555 MB filtered output streams **through** the exchange; q07's wall remains the later
  join (342); 341 advances composition, does not claim q07 green.
- **sf=0.01 correctness preserved:** q07/q21 `pass@compare` (per-partition order invariant - the Float64 parity test);
  full `correctness-matrix` 22/22.
- **No source-wide rewrite:** the `Exchange <- Filter <- Join(0)` boundary only; non-matching shapes unchanged.

## Risks
- **Output handling is the gate** - if the exchange must return a Vec, 341 only relocates q21's wall (filter accumulator
  -> exchange-output accumulator); that is still valuable (Filter accumulator removed, q07 composes) but is **not**
  q21-green. Measure before promising green (the 340 discipline).
- **No silent exchange wall** - any returned/accumulated partition output must be budget-accounted + named, not a silent
  OOM (the 340 accumulator-budget lesson). Account the total returned exchange output retained by the caller.
- **Wrong primitive trap** - do not route through 340's current `try_streaming_join0_filter` result and call that
  streaming; it returns the very `Vec` this cut exists to remove. Extract the chunk/filter producer before Exchange
  calls `execute_bound(input)`.
- **Order through repartition** - the exchange hash-repartitions; 341 must preserve per-partition row order vs the
  materialize-each exchange so the downstream grouped fold is bit-identical, including dev Float64. The parity test is
  mandatory in **both** branches.
- **Detection before execute** - the streamed path must replace `execute_bound(input)` for the matched shape, not follow
  it.

## Forward arc
- Branch 1: q21 green here; q07 -> 342 (later join). Branch 2: q21's wall -> budgeted exchange output; 342 (consumer
  streaming / join-to-join) is q21's green stage and q07's later-join stage. Either way the staged cadence continues,
  each cut a measured green-or-next-wall step. 341's size also calibrates: if partition-forward needs a streaming
  consumer interface, that interface is the substrate's core and reframes 342+.

## Why toron-home
Engine cut in `crates/toron` (exec.rs exchange/filter/join boundary); driven via `toron-tpch` reach + correctness at
sf=1. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. 339's stage-341, q21-sharp / q07-compose. Operator's tightening: the **output-handling
gate** is a hard Phase 0 decision point - prove whether the Exchange can partition-forward without a returned `Vec`
(then 341 may target q21 green) or must return `Vec` (then 341 is a wall-mover: remove the Filter accumulator, budget +
name the exchange-output accumulator, verify next producer via 332). Row-order parity (Float64-sensitive) mandatory in
both branches. Same discipline as 340: measure output handling first, set acceptance to green-or-next-wall by what the
boundary permits.
