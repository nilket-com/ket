# Implementation Plan — ADR 279: unqualified correlated EXISTS resolution

## Summary
Resolve unqualified correlated EXISTS refs in the binder (catalog-aware), reusing the ADR 270 `column_side`
machinery, so q04's `EXISTS (… l_orderkey = o_orderkey …)` lowers to its `orders ⋉ lineitem` `LeftSemi`.
Scoped to correlated EXISTS only. Engine cut (`crates/toron`), full `-p toron` gate. The handoff mechanism is
grounded + measured in Phase 0 — **not pre-claimed localized**.

## Phase 0 — ground + decide the handoff mechanism (load-bearing; do this first)
Read end-to-end: `sql_planner::lower_exists_join` + `classify_exists_conjunct` + `scoped_column_expr` /
`resolve_column_scope` (the frontend reject), and `binder/outer_join_demotion::recognize_cross_equi_joins` +
`column_side` / `RelationScope` / `conjunct_column_sides` (the binder resolution model). Also ground the current
IR constraint: `LogicalPlan::Join` has only `on` keys and no residual join predicate, and there is no retained
`Expr::Exists` / `LogicalPlan::Exists` node. Decide the **minimal concrete** representation for an
unclassifiable correlated EXISTS crossing frontend→binder. Acceptable shapes include a narrow
deferred-EXISTS/semi-join marker or an equivalent pre-bind handoff that carries the unresolved predicate until
the binder has scopes. Do **not** use a fake keyless `LeftSemi`, `Cross` + filter, or filter-above-semi
placeholder: those either ignore correlation, multiply outer rows, or lose the inner columns.

Qualified correlated EXISTS should keep lowering at the frontend as today unless grounding proves a unified
handoff is materially simpler and preserves the existing behavior.

Record the measured mechanism size in the results doc. If the concrete handoff is not small, say so — the
honest size is the deliverable, not a "localized" label.

## Phase 1 — frontend: defer, don't reject
In `sql_planner`, change only the unqualified correlated EXISTS path so an unclassifiable correlated
equi-conjunct is **carried by the Phase-0 handoff** rather than `Err("unsupported EXISTS column reference")`.
Qualified refs and inner-residual conjuncts keep their current handling unless Phase 0 explicitly chooses a
single unified path. Unsupported predicate shapes still reject, either syntactically in the frontend or
scope-aware in the binder; do not silently demote a correlated predicate to an uncorrelated keyless EXISTS.

## Phase 2 — binder: classify + lift (reuse ADR 270 machinery)
In the binder (the `recognize_cross_equi_joins` neighborhood), resolve each deferred correlated EXISTS ref
against the outer and inner relation scopes: classify outer vs inner by **unique column ownership**; `None`
(ambiguous / unresolved) → reject with a clear error; lift the validated `(outer, inner)` pairs into a real
`LeftSemi` / `LeftAnti` `on` list and keep inner-only residuals on the inner input. Preserve EXISTS semantics
(semi-join, not a multiplying inner join).

## Phase 3 — measure q04 (verify-first)
`run --query q04 --mode dev|strict` + `matrix`: expect `pass@execute` (q04 the 7th reach cell) or the next
measured wall (classify it precisely; don't force a pass). Confirm the q04 row in the matrix.

## Phase 4 — tests
- A binder/decorrelation test: an **unqualified** correlated EXISTS lowers to a `LeftSemi` with the resolved
  keys (sibling of the existing `exists_decorrelation_test` qualified cases).
- An **ambiguity** test: a correlated EXISTS whose ref is owned by both scopes (or neither) still rejects
  cleanly — the safe-resolution invariant.
- `exists_decorrelation_test` (existing qualified cases) unchanged; the toron-tpch six reach cells + q06/q01
  correctness stay green; q04 flips in `tests/harness.rs` (drop or update its
  `q04_blocks_at_plan_on_unqualified_correlated_exists` to the measured row — likely `q04_executes_in_both_modes`,
  and on the explicit large-stack wrapper only if the semi-join future overflows the default test stack).

## Verification gate (engine cut — full `-p toron`, via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt   -p toron -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL guard suite, after fmt
scripts/cargo-cgroup test  -p toron --no-fail-fast
scripts/cargo-cgroup test  -p toron-tpch
scripts/cargo-cgroup run   -p toron-tpch -- run --query q04 --mode dev    --allow-blocked
scripts/cargo-cgroup run   -p toron-tpch -- run --query q04 --mode strict --allow-blocked
scripts/cargo-cgroup run   -p toron-tpch -- matrix
git diff --check
```

## Seal
`impl: adr 279 unqualified correlated exists`, `Co-Authored-By: Claude Opus 4.8 (1M context)
<noreply@anthropic.com>`. Add `crates/toron/plans/impl/0279_unqualified_correlated_exists_results.md` with the
measured q04 row + the **measured handoff-mechanism size** (Phase 0's finding).

## Risk notes
- **Size is measured, not claimed.** Phase 0 grounds the handoff; the results doc states the actual plumbing
  size — no "localized" unless proven.
- **EXISTS semantics preserved.** The deferred path must stay a `LeftSemi` (no outer-row multiplication); a
  keyless semi placeholder or Cross+Filter would change the answer.
- **Ambiguity rejects; no name heuristics.** Classification by `column_side` ownership; `None` → reject.
- **Scope held.** Correlated EXISTS only; scalar/`IN`/q17/q20/q22 and the runtime date-interval kernel stay
  deferred.
- **No frontend catalog.** `SqlPlanner` stays catalog-free; resolution lives in the binder.
