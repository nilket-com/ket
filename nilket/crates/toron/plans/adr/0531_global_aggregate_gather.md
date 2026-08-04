# ADR 531 - Global-aggregate gather under the distributed posture, with an undecomposable-terminal backstop

ADR 529's execution census surfaced two distributed correctness findings that block any timing
follow-up. This ADR resolves the q17 class. The mechanism is fully grounded: the planner's
no-GROUP-BY branch deliberately emits a bare `Aggregate { group_expr: vec![] }` with no
exchange (`sql_planner.rs:4014-4038` — "no distributed exchange is inserted"), Toron has no
two-phase partial aggregation anywhere, and the fragmenter clones everything above a join
verbatim into `(0..partition_count)` terminal consumer tasks with no validity check on that
plan (`fragment.rs:696-733`). A scalar aggregate over a join therefore executes once per
partition and the coordinator concatenates the partials — q17's four rows against the local
oracle's one.

The repair construct already exists and is proven: a gather exchange (`partition_count = 1`,
empty key) is emitted today by the ORDER BY funnel (`sql_planner.rs:3603-3612`) and by
empty-spec windows (`sql_planner.rs:8990-8998`), fragments generically, and is
value-discriminatingly proven across two worker processes
(`distributed_process_test.rs:867-897`). It is simply never emitted for global aggregates.

## Decision

Two mechanisms, one topic — the funnel that makes the shape correct, and the backstop that
makes the gap class unrepresentable:

1. **Binder-side gather, distributed posture only.** In the binder's aggregate-exchange
   module (beside `collapse_redundant_pre_aggregate_exchange`, `binder/aggregate_exchange.rs`),
   add the dual pass: when `single_partition_execution` is false and a bound aggregate has
   empty group keys, wrap its input in a gather exchange unless `plan_partitioning_proof`
   already proves `SinglePartition`. The binder is the right home because it already owns the
   posture (ADR 490) and the partitioning proofs, and because a planner-side insertion would
   reshape every frozen local plan on the board — under the local posture nothing changes,
   byte-identical bind pinned by test.
2. **Fragmenter backstop, typed.** Symmetric with `reject_sort_barrier_inside_join_inputs`
   (`fragment.rs:657`), reject a join-consumer plan containing an aggregate that is not
   partition-decomposable at the join's exchange boundary: empty group keys, or group-key
   roots not covered by the boundary partition keys (reuse the
   `aggregate_exchange.rs:52-139` coverage predicates). New `FragmentError` variant beside
   `JoinInputNotExchanged`; the broadcast-join path gets the same check. After (1) the scalar
   arm should never fire — it exists so the *class* fails loudly instead of diverging if any
   future plan shape slips past the binder discipline.

Rejected alternative: two-phase partial/final aggregation. It would be a new execution
mechanism with merge semantics per aggregate function; the gather reuses a proven construct
and the census's timing question needs correctness first, not aggregate parallelism. Two-phase
stays a named future capability with this receipt as its motivation.

## Acceptance

1. Local posture invariance: gauge-config binds of the affected shapes (q06, q17 among them)
   produce byte-identical plans and fingerprints; pinned by test.
2. Distributed posture: a scalar-aggregate-over-join plan binds with the gather, fragments to
   a single-task terminal aggregate stage, and a two-worker end-to-end test is
   value-discriminating (the global sum is correct only if gathered cross-producer).
3. The backstop variant is typed, carries the offending shape, is classified exhaustively by
   the census rail (the ADR 528 `classify_fragment_error` match gains its arm by compile
   force), and has fragmenter unit tests for both arms (scalar; uncovered group keys) built
   on plans without the binder funnel.
4. The execution census q17 cell, re-run from the sealed binary at the ADR 529 envelope,
   leaves `executes_divergent`: `executes_identical` is the expected outcome; any typed wall
   (for instance a backpressure stall on the new boundary) is an acceptable honest result;
   a remaining divergence fails the ADR. q11 is explicitly out of scope — its constant-key
   HAVING-scalar mechanism is a different class (ADR 532 candidate, two run-discriminable
   hypotheses recorded in the grounding).
5. Standard full battery from the final tree, including the complete `--no-fail-fast` suite
   (engine cut).

## Guardrails

- The gather is a correctness construct, not a performance claim; no timing authority is
  created, and the 529 timing block stands until q11 is also resolved.
- No planner (`sql_planner.rs`) shape change: local plans and the frozen board's fingerprints
  are untouched by construction.
- The backstop must not fire on today's grouped shapes: group-keyed exchanges above joins are
  exchange-key = group-key by construction (`sql_planner.rs:3971-3977`); a backstop that
  rejects a currently-identical census cell is a bug in the backstop.
- No scheduler, store, or streamability change rides along — the 11-cell backpressure family
  is a separate seam with its own upcoming decision.

## Forward

- ADR 532: q11's constant-key HAVING-scalar divergence — discriminate the two grounded
  hypotheses (partition-program mismatch on the literal key vs unfragmented-producer
  semantics) by run, then fix.
- The streamability seam (11 stalls): producer live-vs-sealed publish must consult consumer
  drain eligibility.
- Two-phase partial aggregation: named, unscheduled; becomes a perf candidate only after the
  substrate can execute the board identically.
