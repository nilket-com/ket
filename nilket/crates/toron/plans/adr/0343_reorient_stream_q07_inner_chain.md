# ADR 343 - reorient + stream the q07 inner-join chain (build small, probe the fact)

## Status
Proposed.

## Context

The streaming substrate's first **green-target** cut, q07-first. ADR 342 proved q07/q21's large intermediate is
**build-side at every downstream join** (left-deep, fact-intermediate-as-build), so consumer-pull alone hits a
build-side wall. **The combined requirement:** reorient the matched inner joins to **build the small dimensions** and
**stream the growing fact/intermediate as the probe** - both together (reorient alone is inert while
`execute_hash_join` concatenates both children; consumer-pull alone is inert while the intermediate is build-side).
**q07 (all-Inner) is the clean proof target; q21 is strictly out of scope** - a "not covered" control, because its
LeftSemi/LeftAnti joins make swap safety a different problem.

### Grounded (342 + the code)
- `execute_hash_join` concatenates **both** children before building/probing, so even a big probe is materialized
  today; the cut must consume the reoriented probe as a **stream**.
- Physical **left = build, right = probe** (342); q07's fact-intermediate is physical-left (build) at Join(1-4).
- The orientation hook for q07's joins is **HashPartitioned**, **not** the broadcast `SwappedRightBuild`
  (binder/join.rs:12/786). Where physical-left/build is chosen for HashPartitioned (cost model /
  `probe_linearity_fence`, join_order.rs:648/1298) is a Phase 0 question - but the **preferred mechanism is
  executor-local**, below.

## Decision - bounded, executor-local reorient + stream for q07's matched all-Inner chain

For q07's matched `Join(i) <- Exchange <- ...` all-Inner chain, a streaming helper **chooses "small dimension build,
streamed intermediate probe" locally**, leaving the bound plan and general `execute_bound` unchanged. Build the small
dimension into the held hash table; **pull the fact/intermediate as a probe stream** (reuse 340/341 chunk/visitor
machinery), emitting matched batches up the chain to the grouped aggregate. Non-matching shapes keep the current
`execute_bound -> Vec` + current orientation (the fallback).

### Three load-bearing gates
1. **Executor-local orientation (preferred, strict).** The reorientation lives in the matched streaming helper, **not**
   a cost-model rewrite. The bound plan's orientation and general `execute_bound` behavior are unchanged - so the
   regression surface is narrow and the existing green-cell guard is meaningful. (If Phase 0 finds orientation can only
   be changed globally in the cost model, that is a re-scope signal, not this cut.)
2. **No hidden materialization.** The reoriented probe side **must be consumed as a stream**. If the helper still
   concatenates the streamed probe into a `Vec` before joining, **the cut is inert** - a test must prove the probe is
   pulled incrementally, not concatenated. The same rule holds between reoriented joins: a streamed output that is
   immediately accumulated as the next join's build just recreates the ADR 342 wall one boundary later. Each q07 inner
   join output must feed the next reoriented join as the **next probe stream** (with exchange repartitioning applied
   batch-by-batch), until the grouped aggregate consumes the stream.
3. **Projection/order restore.** Swapping physical build/probe must still emit columns **and** rows in the materialized
   plan's **observable order**. Inner joins are commutative in result *set*, but the engine's output schema/row order is
   not unless the helper **restores it deliberately** (the swap+restore discipline). sf=0.01 correctness parity is the
   proof.

## Acceptance (green-or-next-wall, q07-first)
- **q07 sf=1: green (`pass@execute` + `pass@compare`) is the target**, OR an honest next-wall with a **measured
  producer** (ADR 332 retention) if a residual boundary remains - the first wide multi-join query to pass sf=1 via
  streaming.
- **q21 not claimed** - strictly a "not covered" control (semi/anti build-side fenced); deferred.
- **sf=0.01 correctness parity + order:** q07 `pass@compare` at sf=0.01 (projection/order restore + probe-stream fold
  order - Float64/Decimal parity); full matrix 22/22.
- **Cost-model regression (the green-cell guard):** existing green cells' plans/orientation **unchanged**
  (q01/q03/q05/q06/q10/q13/q16/q22 + q14/q19 stay green, same plans) - because the reorientation is executor-local to
  the matched shape, not a cost-model shift.

## Guardrails
- **No global `execute_stream`** - bounded matched-shape helper + fallback.
- **No semi/anti swapping** - inner-only; q21 fenced.
- **No hidden materialization** - the probe streams, and intermediate outputs do not become build-side accumulators
  between reoriented joins (gate 2).
- **Projection/order restored** - observable column+row order unchanged (gate 3).
- **Executor-local, not cost-model** - bound plan unchanged (gate 1); green cells regression-checked.

## Risks
- **Reorient-alone / pull-alone is inert** - the cut must pair them; gate 2 (no hidden materialization) catches a probe
  that still concatenates, or an intermediate output that is re-accumulated as the next build side.
- **Order** - the swap must restore observable order; gate 3 + sf=0.01 parity catch a reorder (Decimal passes, Float64
  breaks; row/column order also matters for the compare).
- **Regression** - if orientation can't be made executor-local (cost-model-only), this cut re-scopes rather than
  risking green-cell re-planning (gate 1).
- **q07-only** - q21's semi/anti is a separate problem; not in this cut.

## Forward arc
q07 green -> the first wide multi-join query passing sf=1 via streaming; the executor-local reorient+stream helper is
the reusable substrate primitive. q21 next (its semi/anti build-side: stream/spill/budget, since swap is fenced).
Then the remaining scale board (q17 build-side; q12/q18 retained-input; q04/q20 spill-writer) each scoped from its
named wall.

## Why toron-home
Engine cut in `crates/toron` (exec.rs join/exchange streaming helper); driven via `toron-tpch` reach + correctness at
sf=1 / sf=0.01. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude. 342's finding (fact-intermediate build-side) -> the combined reorient+stream requirement,
q07-first. Operator's tightenings: **executor-local orientation** (preferred, strict - bound plan unchanged, narrow
regression surface); **no-hidden-materialization gate** (the probe must stream or the cut is inert);
**projection/order gate** (swap must restore observable column+row order for sf=0.01 correctness); **q21 strictly
out of scope** (semi/anti swap safety is a different problem). Green-or-next-wall acceptance; no cost-model / global
`execute_stream` claim.
