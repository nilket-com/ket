# ADR 338 - bounded streaming proof cut: Join -> Filter -> Aggregate(global) for q14/q19

## Status
Proposed.

## Context

The streaming arc's first **implementation** cut, deliberately bounded. ADR 337 measured the only direct collapsing
segment - q14/q19: `Project -> Aggregate(global) -> Filter -> Join(0)` (~828.9 MB retained inputs, ~1.65 GB
materialized output, no exchange) - and predicted those two cells move under an output-streaming cut. **This ADR
proves the streaming primitive on that segment and verifies 337's prediction. It is not a substrate cut.**

The code matches the bounded surface: the Aggregate arm materializes its input before folding
(`execute_bound(input) -> Vec`, exec.rs:872); `bound_global_aggregate` already folds rows batch-by-batch
(exec.rs:1493); `materialize_join_selection` (exec.rs:3210) is exactly the ~1.65 GB allocation to avoid.

## Decision - a special-case streaming path for the direct collapsing segment

Execute `Aggregate(global) <- Filter? <- InnerJoin` as a **chunk-fold stream**: build/probe the join to produce the
same ordered `JoinOutputRow` selection the materializing path already uses, then **materialize + filter + fold the
output in bounded chunks** into the global aggregate state, **dropping each chunk** - never materializing the full
~1.65 GB output. q14/q19 hit the **spilled** inner-join path at sf=1, so the stream must be wired at the shared
selection-consumption boundary used by both in-memory and spilled joins (`materialize_join_selection` /
`probe_spilled_hash_join`), not only at the in-memory hash-join return site.

Three load-bearing implementation constraints (Codex):

1. **Shape detection happens BEFORE `execute_bound(input)` in the Aggregate arm.** If detection runs after executing
   the child, the join output has already materialized and the cut is inert. The arm must recognize the segment and
   take the streaming path *instead of* calling `execute_bound(input) -> Vec`.
2. **Do not collect chunks into a `Vec` and reuse `bound_global_aggregate`** - that rebuilds the wall. Factor the
   aggregate state into **init / fold / finalize** helpers; the streaming path inits once, folds each filtered chunk,
   drops it, finalizes. (The existing materialize-each path reuses the same helpers, behavior-neutral.)
3. **The correctness invariant is row-order preservation, NOT associativity.** Strict q14/q19 use Decimal (exact),
   but **dev uses Float64, where addition is not associative.** So bit-identical results are guaranteed *only* because
   the chunks are **contiguous ranges of the same join selection in the same order** - the aggregate folds values in
   the identical order to full materialization. The cut must preserve selection row order across chunk boundaries;
   it must not reorder.

**Peak after the cut:** held inputs (~829 MB) + selection (~48 MB) + one chunk + scalar state ~ <1 GB < 4 G -> q14/q19
fit. (The held inputs remain - which is why this works only for the direct segment, not q07/q17/q21.) The selection
may be accumulated first in this bounded proof cut; streaming/probing the selection itself is substrate work unless
Phase 3 shows the selection term, not output materialization, is now the next wall.

## Bounded surface (explicit non-goal: no source-wide rewrite)
A streaming path for the `Aggregate(global) <- Filter? <- InnerJoin` shape **only**, plus the init/fold/finalize
refactor of the global-aggregate state and a reusable join-selection chunk consumer that covers the sf=1 spilled
route. **Not** a general `execute_bound` dispatch rewrite, **not** the join build side, **not** streaming join probe
selection, **not** the sink, **not** exchange. Every other plan shape keeps the existing materialize-each path.

## Acceptance
- **q14/q19 move `memory_budget_exhausted@execute` -> `pass@execute` at sf=1**, OR the miss is explained with measured
  evidence (expected-vs-actual against 337 - e.g. a retained-input term 337 under-measured).
- **No correctness regression, proven for both modes:** a **Float64 (dev) and Decimal (strict) parity test** that the
  chunked path equals full materialization (the row-order invariant - Float64 is the one that would break if order
  were not preserved). q14/q19 stay `pass@compare` at **sf=0.01 and sf=1** (scale correctness, since this is the first
  scale-pass cut); full `correctness-matrix` 22/22.
- **q07/q17/q21 remain expected-partial** - not opened (join-to-join / exchange substrate).
- **No source-wide executor rewrite** - only the direct collapsing segment path gets the primitive; the init/fold/
  finalize refactor is behavior-neutral for the existing path.

## Explicit non-goals (337's other families)
- q07/q17/q21: join-to-join / exchange streaming (substrate - a later, intentionally-opened arc).
- q12/q18: retained-input walls. q04/q20: spill-writer resident-state walls.

## Risks
- **Detection placement** - must gate the `execute_bound(input)` call, not follow it, or the cut is inert.
- **No chunk-collecting** - fold-and-drop via init/fold/finalize; collecting into a `Vec` rebuilds the wall.
- **Order, not associativity** - the Float64/Decimal parity test is the proof; a reorder across chunks would pass
  Decimal and silently break Float64.
- **Shape fences** - a non-matching shape (grouped aggregate, join-to-join, exchange present) must keep the existing
  path; the primitive must not wrongly engage.
- **Spill-route coverage** - q14/q19 are spilled joins at sf=1; an implementation that only chunks the in-memory
  join path will pass small synthetic tests and still miss the actual cells.

## Forward arc
If q14/q19 land clean, the primitive is proven and 337's prediction verified. The next deliberate decision is then
**streaming substrate** (q07/q17/q21: join-to-join + exchange) vs **the logical-completeness pivot** - made with 338's
evidence (the primitive works; its size is known). If q14/q19 miss, the miss is measured evidence about the executor
that reshapes the arc.

## Why toron-home
Engine cut in `crates/toron` (exec.rs Aggregate arm + the global-aggregate state refactor + the segment stream);
driven via `toron-tpch` reach + correctness at sf=1. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. Bounded proof cut, not substrate (337's verdict). Codex's tightenings folded: (1) shape
detection **before** `execute_bound(input)` or the cut is inert; (2) **init/fold/finalize** helpers + fold-and-drop,
not chunk-collecting (which rebuilds the wall); (3) the invariant is **row-order preservation, not associativity**
(dev Float64 is non-associative) - prove it with a Float64/Decimal parity test; plus q14/q19 `correctness --sf 1` in
the seal (first scale-pass cut). Measure-don't-predict: verify 337's expected coverage against actuals.
