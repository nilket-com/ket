# ADR 342 - consumer-pull interface sizing pass (bounded vs general execute_stream)

## Status
Proposed.

## Context

The streaming substrate's **core** question, and per the cadence a **sizing/design pass, not an implementation jump**.
340/341 were bounded wall-movers; they proved the *upstream* half (Filter no longer materializes before Exchange) but
left q21 on the same ~1.07 GB shape one boundary later, because **Exchange still returns a full `Vec<RecordBatch>`** to
its consumer. q21's green and q07's later-join both sit behind the **consumer-pull interface**: `Join(1)` pulling
Exchange partitions incrementally instead of `execute_bound(Exchange) -> Vec`. **The load-bearing question (operator):
can consumer-pull be introduced for `Join(1) <- Exchange <- Filter <- Join(0)` only, keeping the rest of
`execute_bound` untouched, or does that boundary immediately demand a general `execute_stream` substrate?** This ADR
measures that before opening the largest cut in the arc.

**It does not imply consumer-pull = q21 green (Codex):** removing the returned Exchange `Vec` may simply re-materialize
the rows one operator later (Join(1) output, or a post-Join accumulator). 342 sizes the boundary; q21 coverage is
explicitly **green-or-next-wall**.

### Grounded (341 + the code)
- `execute_bound(child) -> Vec<RecordBatch>` is the universal consumer interface (exec.rs:768); the join consumes
  `execute_bound(left/right) -> Vec` (exec.rs:1000-1001); the exchange returns `Vec` (`bound_exchange`, exec.rs:2362).
- 341 (branch 2): Filter no longer accumulates before Exchange, but Exchange's returned `Vec` re-accumulates the
  re-partitioned output (q21's current wall ~1.07 GB).

## Decision - size the consumer-pull boundary (measure + design, no implementation)

### Phase 0
1. **`Join(1)`+ build/probe map - labeled STRUCTURAL, not sf=1-measured (Codex).** sf=1 currently dies at the
   Exchange-output wall **before** `Join(1)` executes, so its build/probe sides and sizes cannot be sf=1-observed. Map
   them from the **bound plan shape + sf=0.01 traces** and **label every `Join(1)+` datum structural**, not measured
   (the 337/339 don't-guess-unreached-bytes discipline). The question: does the large intermediate stay **probe-side**
   (pullable) downstream, or become a **build side** (held -> its own build-boundary fix first)?
2. **The consumer-pull interface design:** the minimal abstraction for `Join(1)` to pull Exchange partitions without a
   returned `Vec` (a partition iterator / pull-based child). What it touches - just the `Join <- Exchange` arm, or the
   `execute_bound` signature broadly.
3. **The bounded-vs-general gate (the operator's question):**
   - **Bounded** - consumer-pull only for `Join(1) <- Exchange <- Filter <- Join(0)` (special-case, like 340/341), the
     rest of `execute_bound` untouched -> the next cut is bounded.
   - **General** - the boundary forces `execute_bound` itself to become pull-based (`execute_stream` everywhere) -> the
     substrate's core rewrite, a larger arc; re-scope and stage it.
4. **q21 coverage: green-or-next-wall (Codex), not green-by-assumption.** If the pulled rows are **consumed and
   dropped** through to the collapse, q21 greens; if they are **re-materialized one operator later** (Join(1) output /
   a post-Join accumulator), the wall moves there. Predict which, with the structural evidence; the implementing ADR
   verifies expected-vs-actual.

## Acceptance - a sized verdict (no implementation, no pass claim)
- **The `Join(1)`+ build/probe map**, every datum **labeled structural vs measured** (sf=1 cannot reach past the
  Exchange-output wall; do not present structural sides as measured bytes).
- **The bounded-vs-general verdict** with evidence - the deliverable.
- **The consumer-pull interface design** (minimal shape + what it touches), so the implementing ADR's size is known.
- **q21 expected coverage = green-or-next-wall**, with the predicted next boundary if not green (Join(1) output /
  post-Join accumulator) and which producer the implementing ADR should budget+name.
- **No implementation, no rewrite, no pass claim.**

## Scope
- **q21 + q07** (behind the consumer-pull boundary). **q17** stays a separate build-side branch.
- **Non-goals:** the implementation; q21 semi/anti reordering; q12/q18 (retained-input), q04/q20 (spill-writer).

## Risks
- **Don't imply green** - consumer-pull removes the returned Exchange `Vec`; it does not by itself fold the rows away.
  q21 greens only if downstream consumes-and-drops; else the wall relocates (Join(1)/post-Join). State it as
  green-or-next-wall.
- **Structural vs measured** - `Join(1)+` is unreachable at sf=1 today; its sides are structural inference, not sf=1
  bytes. Mislabeling structural as measured repeats the base-table-inference error 337 caught.
- **Bounded-vs-general is the gate** - a wrong guess is the difference between a bounded next cut and an open-ended
  rewrite; measure before opening it.
- **Probe discipline** - size + design only; no streaming implementation here.

## Forward arc
Bounded verdict -> the next ADR is a contained `Join(1) <- Exchange` consumer-pull cut (green-or-next-wall for q21,
compose for q07). General verdict -> the substrate's `execute_stream` core, staged as its own arc (and weighed against
the logical-completeness pivot, now that its cost is known). Either way the verdict is the input to the largest
decision in the scale arc.

## Why toron-home
Engine grounding/design in `crates/toron` (exec.rs join/exchange consumer boundary); driven via `toron-tpch` at sf=1 +
sf=0.01 structure. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. The consumer-pull interface is the substrate's core; per the cadence it gets a sizing pass,
not an implementation jump. Operator's gate: bounded (`Join(1) <- Exchange` only) vs general (`execute_stream`). Codex's
tightenings folded: label `Join(1)+` evidence **structural vs measured** (sf=1 cannot reach past the Exchange-output
wall); q21 coverage is **green-or-next-wall** (consumer-pull may re-materialize one operator later), not
green-by-assumption. Measure-don't-predict at the substrate's core.
