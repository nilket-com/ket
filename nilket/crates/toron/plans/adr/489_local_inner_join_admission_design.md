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

## The representation question (for the code increment)

The local shape must pass **both** `validate_strategy_shape` and the fragmenter's
exchanged-input contract as a *legitimately non-exchanged* inner join — today both
reject a `HashPartitioned` join without exchanges (ADR 486 probe 2). Options, decided
in the code cut with the JoinKind-variant-guard-checklist in view:
- A minimal marker on the existing `Join` (e.g. a `local: bool` / partition_count=0
  sentinel) that both the validator and fragmenter accept as non-exchanged-by-design,
  routing to `execute_hash_join`'s in-memory path.
- A new `JoinStrategy::LocalHash` variant (cleaner semantics, but trips ~6
  source-scanning guards and needs fragmenter handling — larger).
The marker is likely smaller; the code cut picks with the guard surface measured.

## The two rails (mandatory for the code cut)

1. **Distributed-correctness gate** = the flag + layers 1-3 above.
2. **Multi-worker negative test**: a genuinely multi-worker fixture proving the join
   stays `HashPartitioned` with exchanges — it must **fail** if a local shape leaks
   into distributed execution (asserting the fragmenter rejects it / the distributed
   bind never emits it).
3. **Admission checks** (necessary, not sufficient): peak residency and digest
   invariance under matched A/B, per ADR 488's correction.

## Acceptance (for the code increment)

- `single_partition_execution` threaded from both executor entries; default false.
- m06 single-node admits the local shape (orders build, in-memory, no exchange);
  digest identical; peak residency within budget; spill eliminated; matched A/B win.
- The multi-worker negative test green (local never leaks to distributed).
- Full engine suite + armed 22-query sweep green; JoinKind guards handled.

## Disposition

Read-only design step complete: single-partition is not provable at bind without a
caller-set signal, and the three-layer safety (binder gate, distributed-non-local,
fragmenter loud-fail) makes the local shape correct-by-construction. The code
increment starts with the flag + its wiring (a safe no-op until the binder reads it),
then the admission + representation + the two rails.
