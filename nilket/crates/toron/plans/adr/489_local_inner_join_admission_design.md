# ADR 489 - local inner join: admission design (provability + layered safety)

The implementation cut opened read-only on the make-or-break question — is
single-partition execution provable where the join strategy is chosen? **It is not.**
This ADR pins the design that resolves it: an execution-mode signal the caller sets,
a binder admission gate, and a three-layer safety that makes a mis-route fail loud
rather than return a wrong answer. Code is the next increment, gated on this.

## The confirmed provability gap

- The binder has **no** distribution/worker-count/single-node awareness (grep empty),
  and `PlannerConfig` has no execution-mode field.
- The **same bound plan** is consumed two ways: single-node via `Executor::with_context`
  (direct), distributed via the coordinator's `fragment_plan` (`coordinator.rs:1128`).
  The binder does not know which will run.
- Therefore the strategy decision is **distribution-blind**. Rail 1 ("admit local only
  when single-partition is proven") **cannot** be a binder-only decision today.

## The provability mechanism: a caller-set execution-mode flag

Add `PlannerConfig.single_partition_execution: bool` (default **false** — conservative).
The **entry that knows the mode sets it**: the single-node executor path sets it true;
the distributed coordinator path leaves it false. The binder can then trust the flag —
single-partition becomes provable *because the caller asserts it*, and the caller is
the one component that actually knows. This is 486's Option A, made concrete: admit
local only in a context the caller proves non-distributed.

## Three-layer safety (why a mis-route fails loud, not wrong)

1. **Binder gate.** Emit the local (no-exchange) inner join only when
   `single_partition_execution == true` **and** the admission rule holds (inner
   equi-join, orders build already selected by the smaller-build comparator per ADR
   488, build fits the in-memory retained cap, probe scan-linear).
2. **Distributed binds non-local.** The distributed path leaves the flag false, so it
   never emits a local join — it keeps `HashPartitioned` with exchanges.
3. **Fragmenter backstop.** If a local join ever reached the fragmenter anyway, its
   `JoinInputNotExchanged` error (`fragment.rs:81`) **fails loud** — a hard error, not
   a silent cross-worker-match loss. `validate_strategy_shape` is the bind-time twin.

Layers 2-3 are what make this safe: the wrong-answer path (local join executed
distributed) is impossible to reach silently — it errors at the fragmenter.

## The representation decision (grounded during the first implementation increment)

The local shape is the existing `HashPartitioned` inner join **without terminal
exchanges**, admitted only behind the caller-set single-partition signal. This is not
a line-count choice: its absence of exchanges is already structural data at the
fragmentation boundary. `fragment_hash_join_plan` requires exchanges and returns
`JoinInputNotExchanged` otherwise, so a local shape cannot be silently fragmented.
No new serialized `JoinStrategy` variant or separately-maintained fragmenter exception
is warranted.

The first implementation increment adds the corresponding rails before admission:
the distributed coordinator forcibly clears `single_partition_execution`, and a
three-worker fragment test proves an exchange-free hash join fails loudly with
`JoinInputNotExchanged`.

## Admission correction — local shape is not enough at the 64 MiB m06 cell

The boundary trace also falsified an assumption in the original acceptance text.
The direct in-memory executor does not admit against the retained spill cap: both
`try_create_streamed_spilled_inner_build` and `build_in_memory_join_rows` compare the
estimated build against the **raw** task-memory budget. m06's already-smaller orders
build estimates at roughly 1.44 GiB, so it cannot become an in-memory join at the
64 MiB ladder budget merely by removing its exchanges (and it does not fit the 1 GiB
16× retained cap either).

Therefore the safe rails and representation are kept, but binder admission is
**deferred**. A successor must first choose and measure an explicit memory policy:
either a larger, coordinate-stamped task budget where the raw in-memory guard admits
the build, or a separately designed retained-cap admission with peak-residency proof.
Neither policy is smuggled into this shape/safety increment.

## The two rails (mandatory for the code cut)

1. **Distributed-correctness gate** = the flag + layers 1-3 above.
2. **Multi-worker negative test**: a genuinely multi-worker fixture proving the join
   stays `HashPartitioned` with exchanges — it must **fail** if a local shape leaks
   into distributed execution (asserting the fragmenter rejects it / the distributed
   bind never emits it).
3. **Admission checks** (necessary, not sufficient): peak residency and digest
   invariance under matched A/B, per ADR 488's correction.

## Acceptance (for the code increment)

- `single_partition_execution` defaults false and the distributed coordinator clears
  it before every bind, including caller-supplied planner configuration.
- The multi-worker negative test is green: an exchange-free hash join fails loudly in
  a three-worker fragment instead of running local subsets.
- A successor picks an explicit memory-policy admission before m06 can emit the local
  shape; only then do digest, peak-residency, spill-elimination, and matched A/B become
  applicable acceptance checks.
- Full engine suite + armed 22-query sweep green; JoinKind guards handled.

## Disposition

The implementation grounding resolved the representation and installed the distributed
rails, but also corrected the admission premise: the current m06 coordinate cannot use
the local in-memory path under its raw 64 MiB budget. The local-shape capability remains
deferred behind an explicit, measured memory-policy decision rather than being forced
through a retained-cap assumption the executor does not implement.
