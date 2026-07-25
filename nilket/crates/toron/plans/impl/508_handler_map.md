# impl 508 step 1 — the aggregate handler map

ADR 508 step 1: pin every dominant `Aggregate` node to the streaming handler that took it, per
invocation, as a durable receipt rather than a memory of a trace. This is the minimal instrumentation
scope step 2 works from.

## How the routing is evidenced

ADR 507's grounding identified handler families with a throwaway `eprintln` patch — reproducible only
by re-patching, which is exactly what "a receipt, not a memory of a trace" rules out. The routing is
now recorded by the instrument itself: the aggregate dispatcher stamps the open node with the arm that
won (`node_attribution::record_route`), and the node profiler emits one `aggregate_invocations` record
per invocation carrying its route and its own exclusive cost. It is env-gated with the rest of the
profile and folded into the perf row, so any run reproduces the map.

Stamping targets the frame, not the query. The dispatcher stamps after its winning arm's children have
popped, so the top of the stack is the `Aggregate` frame being routed — pinned by a test that a route
lands on the open frame and not a completed child, and by a second test that two invocations under one
tree keep separate routes and costs.

**An invocation no streaming arm claimed is recorded as `uncovered_generic_fold`**, never left blank
or folded into a neighbour.

## Measurement provenance

| item | value |
|---|---|
| build identity in every row | **`2eba67b58c51-dirty`** |
| dataset verification | `full_hash` |
| coordinate | strict, sf=8, persisted parquet, 2 GiB task / 2 GiB spill |
| runs | 1 measured, 0 warmup, per cell |
| artifacts | `target/tpch/handlermap/<cell>/` |

**This map was measured from a dirty tree**, not from the commit that seals it: the routing instrument
was still uncommitted when the runs were taken, so every row carries `2eba67b58c51-dirty` (ADR 508's
plan commit plus the working-tree instrument). That is acceptable here — a handler map is a *routing*
fact, and routing is determined by plan shape and dispatch order, neither of which a build stamp
changes. It is **not** acceptable for a timing baseline, which is why ADR 508 gates the clean 22/22
board on a sealed, pushed, never-amended commit. Nothing in this receipt should be read as measured
from `5be868d0`, and the `%span` and millisecond figures here are routing context, not baseline
numbers.

## The map

`%span` is the invocation's exclusive share of the `execute_bound` root span.

| cell | plan fingerprint | inv | route | exclusive ms | %span | spill MB |
|---|---|---:|---|---:|---:|---:|
| q02 | `9b8b8e011663bda5` | 0 | **uncovered_generic_fold** | 322 | 12% | 0 |
| q08 | `06e7c08c71d79255` | 0 | reoriented_inner_chain_grouped | 7,587 | 96% | 0 |
| q09 | `a80cd80fa01877c9` | 0 | reoriented_inner_chain_grouped | 12,301 | 95% | 0 |
| q17 | `0bb3976ddac68006` | 0 | scan_grouped | 10,993 | 31% | 0 |
| q17 | `0bb3976ddac68006` | 1 | correlated_scalar_left_outer_global | 24,369 | 69% | 623 |
| q18 | `1c3870f3f1f9af20` | 0 | scan_grouped | 2,447 | 13% | 0 |
| q18 | `1c3870f3f1f9af20` | 1 | **uncovered_generic_fold** | 0 | 0% | 0 |
| q20 | `e92495e248cd91be` | 0 | scan_grouped | 5,893 | 62% | 0 |
| q21 | `aa4d618880dd4c55` | 0 | inner_prefix_semi_anti_grouped | 17,269 | 100% | 898 |

Every cell closes the invocation→node link exactly: the number of records equals the node's `calls`,
and the invocation exclusive figures sum to the `aggregate` node-kind total on **both** time and spill
(verified per cell, all `True`). No invocation is unmatched.

## What the per-invocation view changed

**q17's "aggregate 100%" is two invocations with two different handlers.** The per-kind total from
ADR 507 could not show this. Split: `scan_grouped` at 31% with no spill, and
`correlated_scalar_left_outer_global` at 69% carrying **all 623 MB**. A single inner profile for q17
would have covered 69% and left nearly a third of the dominant node invisible while reconciling
perfectly against itself — the precise failure mode ADR 508's matched-invocation requirement exists to
catch. It is not hypothetical, and it is the largest cell on the board.

It also splits q17 across both fronts — spill-bound in the correlated-scalar invocation, compute-bound
with zero spill in the other — which is why the front assignment below moves from query-level to
invocation-level.

## The minimal instrumentation scope for step 2

Four handler families carry the aggregate share; one is already decomposed.

| handler | cells (share of their span) | spill | inner attribution today |
|---|---|---:|---|
| reoriented_inner_chain_grouped | q08 96%, q09 95% | 0 | **none** |
| correlated_scalar_left_outer_global | q17 inv 1, 69% | 623 MB | **none** |
| scan_grouped | q20 62%, q17 inv 0 31%, q18 inv 0 13% | 0 | **none** |
| inner_prefix_semi_anti_grouped | q21 100% | 898 MB | **yes** — chain profiler (76% `inner_replay_1`) |
| uncovered_generic_fold | q02 12%, q18 inv 1 0% | 0 | n/a (not a streaming handler) |

So step 2 builds **three** inner instruments, not five: `reoriented_inner_chain_grouped`,
`correlated_scalar_left_outer_global`, and `scan_grouped`. q21's handler is already decomposed and its
vocabulary is the starting reference. The generic fold accounts for 12% of one small cell (q02, 2.7 s)
and is not worth an instrument at this ranking — recorded as a known, named gap rather than covered.

## The step-1 front hypothesis was invocation-level, not query-level

ADR 508 assigned whole cells to fronts because that was the granularity available. The per-invocation
map supersedes that: **q17 belongs to both fronts, in different invocations**, and a cell's front is a
property of each invocation's route and spill, not of the query.

| front | invocations |
|---|---|
| **compute-fold** | q08 inv 0 (reoriented_inner_chain_grouped, 96%), q09 inv 0 (reoriented_inner_chain_grouped, 95%), q20 inv 0 (scan_grouped, 62%), **q17 inv 0** (scan_grouped, 31%, zero spill) |
| **spill-replay** | **q17 inv 1** (correlated_scalar_left_outer_global, 69%, 623 MB), q21 inv 0 (inner_prefix_semi_anti_grouped, 100%, 898 MB — already decomposed) |
| **parked (join/sort)** | q18 both invocations, q02 inv 0 |

Two instruments still cover the compute front (`reoriented_inner_chain_grouped`, `scan_grouped`) and
one the uncovered spill front (`correlated_scalar_left_outer_global`).

**q18's coverage is incidental and must not be counted.** Instrumenting `scan_grouped` for the compute
front will also produce a profile for q18 inv 0 (13%), because they share a handler. That is a free
by-product, not a reason to weight q18 in the compute ranking: q18 is join-dominated (65%) and stays
parked, and its scan_grouped invocation is 13% of a cell that is not on this front. Treat any q18
profile that falls out of step 2 as evidence held for the parked front's eventual re-opening, and
exclude it when ranking the compute premise.

## Post-step-2 correction and outcome

The table above is the hypothesis this routing receipt handed to step 2, not the final assignment.
Inner attribution falsified two parts of it:

- q08/q09's `reoriented_inner_chain_grouped` invocations are **join-probe**, not compute-fold; their
  final-coordinate join-associated lower bounds are 87.9%/85.1%, while grouped fold is only
  0.65%/1.91%;
- q21's legacy chain profile decomposed a drive but did not close the invocation-bound
  handler-to-node bridge. The corrective `StageToken` vocabulary now closes it on time and Flow
  bytes.

Step 2 ultimately instrumented all four handlers, each closing the full chain — inner stages ⊆
handler span ⊆ matched invocation exclusive span ⊆ `execute_bound` span — through the ADR 507
contract on both axes. The clean 22/22 baseline and final assignments are recorded in
`impl/508_clean_sf8_baseline.md`.
