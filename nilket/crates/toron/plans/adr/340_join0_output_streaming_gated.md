# ADR 340 - Join(0) output streaming: first substrate cut (selectivity-gated)

## Status
Proposed.

## Context

The streaming substrate's first **cut** (after ADR 339's sizing). 339's staged plan, stage 340: stream `Join(0)`
output into the next operator, removing the first 1.98 GB materialization; "next wall likely the immediately following
exchange/materialized downstream join." **This is a wall-mover, not a pass claim** (operator's banked acceptance):
prove the `Join(0)` raw-output materialization wall is removed, preserve order, verify the next wall appears where 339
predicts. q07/q21 green is out of scope unless the evidence surprises us.

### Grounded (339 + the code)
- q07/q21 wall = `Join(0)`'s **1.98 GB raw output** (probe = lineitem 805 MB; build = supplier 1.69 MB; in-memory;
  `materialize_join_selection`). The immediate consumer is a **Filter** (exec.rs:819,
  `filter_batches_with_bound_predicate(execute_bound(Join) -> Vec)`), then Exchange -> Join(1).
- ADR 338's `try_streaming_collapsing_global_aggregate` returns a finalized `Vec` (folds into a *terminal* aggregate);
  340's consumer is **non-terminal** (Filter -> Exchange -> ...), so 340 **accumulates the filtered result** (a `Vec`
  for the Exchange), not the raw output.

## Decision - stream Filter <- Join(0) (filter-on-the-fly), behind a hard selectivity gate

The cut: consume `Join(0)`'s output in **bounded contiguous selection chunks** (reuse 338's chunk-materialize machinery),
**filter each chunk**, and **accumulate only the filtered rows** into the returned `Vec` - the raw 1.98 GB output never
fully materializes. Selection row order preserved across chunks (the 338 invariant). The filtered result flows to the
Exchange via the existing materialize-each path (the Exchange is 341).

### Phase 0 - the hard selectivity gate (a DECISION POINT, not just a measurement)
Measure q07/q21's `Join(0) -> Filter` **filtered output bytes** at sf=1. This cannot be a passive log on the current
path: today the ADR 335 output guard fires before the Filter runs. Phase 0 therefore needs a temporary, env-gated
probe path that chunk-materializes `Join(0)` output, applies the Filter, records post-filter rows/bytes, and is removed
before seal. Do not weaken or bypass the production guard outside that probe. Run release, 4G cap, then **branch**:
1. **Filtered materially smaller than the raw 1.98 GB** -> **proceed** with the `Join(0) -> Filter` chunk filter +
   accumulate cut.
2. **Filtered roughly the same size** -> **stop and re-plan toward Exchange streaming first.** 340 would only change
   allocation shape, not move the wall - an inert cut. The honest move is to make 341 (exchange streaming) the first
   substrate cut instead, and re-cut this ADR accordingly.
3. **Filtered fits unexpectedly (whole chain may move)** -> treat q07/q21 moving *farther* than 339 predicted as
   evidence, but **still document the actual next wall** (it will not be the filtered output - it will be the next
   materializing boundary the streamed filter exposes).

## Acceptance (operator's banked shape; wording corrected)
- **sf=1 wall moved (via ADR 332 retention):** q07/q21 still `memory_budget_exhausted@execute` is acceptable **only
  if** the retained `diagnostic_message`'s **producer and bytes identify the first boundary *after* the streamed filter
  path** (the `Join(0)` output guard no longer the producer). The new boundary may be the budgeted filtered-output
  accumulator, the Exchange, or - if the filter is selective enough - `Join(1)` bytes; the acceptance is that the wall
  is the first boundary past the streamed filter, **not** specifically "the filtered output."
- **No silent filtered-Vec wall:** the accumulated filtered result must be budget-accounted and emit a named
  `memory_budget_exhausted@execute` diagnostic if the filtered result is still too large. 340 must not replace the raw
  `Join(0)` OOM guard with an unaccounted filtered-output allocation.
- **sf=0.01 correctness preserved:** q07/q21 stay `pass@compare` (within-partition order preservation - the
  filter-per-chunk path runs at sf=0.01 too); full `correctness-matrix` 22/22.
- **Wall movement + preserved correctness, not q07/q21 green** (green only if the selectivity surprise of branch 3
  lands - a finding, documented with the actual next wall).
- **No source-wide rewrite:** the `Filter <- Join(0)` (inner) boundary only; non-matching shapes keep the existing
  path; the chunk-machinery reuse is behavior-neutral.

## Bounded surface / non-goals
The `Filter <- Join(0)` inner boundary only, reusing 338's chunk-materialize + accumulate-into-`Vec`. **Not** the
Exchange (341, unless gate branch 2 promotes it to first), **not** join-to-join (342+), **not** q21's semi/anti joins,
**not** a general `execute_stream` rewrite, **not** q17 (build-side/spilled, separate).

## Risks
- **Inert-cut risk (the gate guards it)** - if the filter is not selective, chunk-filtering only changes allocation
  shape; the Phase 0 gate must stop and re-plan toward the Exchange rather than ship a no-op.
- **Acceptance wording** - the next diagnostic identifies the first boundary *after* the streamed filter, which may not
  be the filtered output (if it fits, it's the Exchange/Join(1)); do not hard-code "= filtered output."
- **Order preservation** - the filter-per-chunk must keep selection order (the Float64/Decimal parity test); a reorder
  passes Decimal, breaks Float64.
- **Detection before execute** - the streaming path must replace `execute_bound(Join) -> Vec`, not follow it, or the
  cut is inert.

## Forward arc
If gate branch 1: 340 moves the wall to the next boundary (named by the diagnostic) -> 341 (exchange streaming) -> 342+
(join-to-join, q21 semi/anti fenced) -> q07/q21 green. If gate branch 2: 341 (exchange) becomes the first cut and this
ADR re-cuts. Either way the staged failure-atlas cadence continues, each cut a measured wall-mover until the chain
closes. Also a calibration: 340's *size* says whether the substrate stays a sequence of bounded cuts or needs a
general stream interface.

## Why toron-home
Engine cut in `crates/toron` (exec.rs Filter/Join boundary); driven via `toron-tpch` reach + correctness at sf=1.
No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. 339's stage-340 wall-mover, with the **selectivity gate as a hard Phase 0 decision point**
(operator): proceed if filtered << raw; stop + re-plan toward Exchange if filtered ~= raw (else an inert cut);
document the actual next wall if it fits unexpectedly. Acceptance wording corrected: the retained diagnostic identifies
the **first boundary after the streamed filter path**, not necessarily the filtered-output bytes. Wall movement +
preserved sf=0.01 correctness, not q07/q21 green. Measure-don't-predict, gated.
