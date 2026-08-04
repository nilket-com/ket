# ADR 486 implementation attempt — falsified the easy routes, re-scoped (no code shipped)

Option A implementation opened with the make-or-break question — how to route m06 to
the existing in-memory `execute_hash_join`. **Two empirical probes falsified the
design's core assumption** (that admission is a binder exchange-decision tweak). The
in-memory path is gated behind executor contracts that neither probe could satisfy.
No code shipped; the design (ADR 486) needs the revision below before implementation.

## What the design assumed

`build_in_memory_join_rows` / `execute_hash_join` (`join.rs:196,1300`) already build
a hash table in memory; m06 spills only because it binds `HashPartitioned` + `Exchange`
and routes to the streamed spilled path. The design inferred that emitting a
non-exchanged (or single-partition) join in the binder would reach the in-memory
executor. Both forms were probed (env-gated, reverted).

## Probe 1 — `partition_count = 1` (the ScalarLocalCross gather): **still spills**

Forcing the inner-join exchange to a single partition left m06's spill **exactly
88,559,536 bytes** (identical to the 4-way default), ~4% faster, digest identical.
The `partition_count <= 1 → Ok(None)` fallbacks (`streaming.rs:5082,5306,5560`) are in
specific reoriented sub-strategies, not the main streamed inner build — which spills
the 88 MB build side **unconditionally**, independent of partition count. So ADR 485's
"spill = the N-way shuffle" was imprecise: the build-side materialization spills even
at one partition. The gather does not reach the in-memory path.

## Probe 2 — skip the exchange entirely: **rejected by the executor**

Emitting the `HashPartitioned` join with no exchange produced `JoinKeyExchangeMismatch`
at execution. The in-memory `execute_hash_join` contract requires exchanged inputs; a
`HashPartitioned` plan without its exchanges is invalid. So the in-memory path is
**not** reached by omitting the exchange either.

## The re-scoped finding

The in-memory executor exists but is reached only by a plan shape the binder does not
currently emit for this case, and the two cheap routes are both blocked by executor
contracts (unconditional build spill in the streamed path; exchange-required in the
direct path). Routing m06 to it therefore requires a **genuine local-join plan node**
that the binder emits and the executor accepts as a valid in-memory shape — not a
partition-count or skip-exchange tweak. That is a larger binder+executor cut than the
design scoped, with its own shape-validation and distributed-gate surface.

The distributed-correctness crux is unchanged and still load-bearing: whatever local
shape is introduced must be admitted only where single-partition execution is provable,
with the distributed negative test as the rail.

## Disposition

Probes reverted; tree clean; m06 back to baseline (pass, digest unchanged). ADR 486's
design stands as the strategy rationale, but its implementation is re-scoped: it needs
a dedicated local-join plan-shape + executor-admission cut, not a binder exchange
tweak. This is the profile/validate-first discipline catching an under-scoped design at
the probe stage — before building a gate and test on a route that does not reach the
target. The next attempt starts by mapping which plan shape `execute_hash_join`
accepts and how the binder can emit it for a qualifying single-node inner join.
