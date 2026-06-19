# Implementation Plan — ADR 270: comma-join equi-join recognition

## Summary
A binder-side, syntax-agnostic optimizer rewrite. A new pre-bind pass lifts cross-bridging equality conjuncts
from a `Filter` over a keyless `Cross` into the join's `on` keys (`Cross` → `Inner`), leaving residual
conjuncts as the filter. The keyed-join engine is already landed; this only produces the `Inner`+keys logical
shape. Then reconcile the affected guards and remove the ADR 269 q14 scale cap (q14 is no longer a cartesian).
Cut crosses `toron` (binder + guards) and `toron-tpch` (q14 cap + re-measure).

## Phase 0 — verify the board
`scripts/cargo-cgroup run -p toron-tpch -- matrix` → q14 dev=pass@execute strict=blocked(decimal_case@bind),
q01/q06 green. Confirm the current keyless-`Cross` lowering with a one-off bind trace of q14.

## Phase 1 — the recognition pass (`crates/toron/src/binder/`)
Add a fallible async sibling normalization (for example `recognize_cross_equi_joins(plan) ->
Result<LogicalPlan, BindingError>`) wired in `binder.rs` right after `demote_null_rejected_outer_joins` (`:222`)
and before `bind_plan_with_trace`. It runs on the post-SQL-planner `LogicalPlan`, so it is frontend-agnostic
(SQL and Rune alike), and it can use the catalog-backed scope resolution the SQL planner lacks.

Reuse the demotion module's existing, grounded helpers (the recognizer is mostly assembly, not new primitives):
- Build the left/right `RelationScope`s for a `Cross`'s children via `logical_scope_for_outer_demotion`
  (`outer_join_demotion.rs:348`) — catalog-backed, so unqualified columns resolve.
- `split_logical_and_conjuncts` (`:1066`) to split the filter predicate.
- `column_side(column_ref, left_scope, right_scope)` (`:1220`) to resolve a column to `LogicalJoinSide` —
  it already returns `None` for the ambiguous `(Some, Some)` and unresolved `(None, None)` cases (so ambiguity
  never silently picks a side). `conjunct_column_sides` (`:1240`) for the recursive operand classification.

Algorithm (route conjuncts to the lowest owning join boundary):
1. Match `Filter { predicate, input: Join { kind: Cross, on: [], left, right } }`.
2. Build `left_scope` / `right_scope` from `left` / `right`.
3. Split `predicate` into conjuncts. For each conjunct, classify its referenced columns against the current
   left/right scopes. If the conjunct resolves wholly to the left child, push it as a filter into `left` and
   recursively run the recognizer there; likewise for a right-only conjunct. This is load-bearing for n-ary
   comma joins: in `Cross(Cross(a,b), c)`, `a.k=b.k` is left-only at the outer Cross and must be routed into the
   inner Cross before it can lift.
4. For each remaining `Expr::Eq(l, r)` where `l`, `r` are direct columns: if `l` resolves uniquely Left and `r`
   uniquely Right → key pair `(l, r)`; if reversed → orient to `(r, l)` so the left-expr is the left-input
   column; otherwise (ambiguous, unresolved, non-column, non-`=`) → residual.
5. If ≥1 key extracted: rewrite the `Cross` to `Join { kind: Inner, on: keys, null_safe_keys: false,
   strategy_intent, .. }`, preserving the original `strategy_intent`; re-wrap residual conjuncts as the parent
   `Filter` (drop the `Filter` entirely if no residual remains).
6. Also recurse through children in plan shapes without an enclosing filter so already-nested filters can be
   normalized.

Engine untouched: the existing `plan_scoped_join_with_strategy` consumes the preserved `strategy_intent` and
picks hash-partitioned or keyed-broadcast; `fragment_hash_join_plan` / `fragment_broadcast_join_plan` build it.

## Phase 2 — guard reconcile (per the ADR's grounded list)
- Leave `cross_join_where_filters_after_cartesian` (`cross_join_test.rs:77`) unchanged (single-side literal).
- Leave the SQL-planner shape guards (`comma_join_lowers_to_keyless_cross`,
  `adr239_cross_join_and_comma_lower_to_keyless_cross`) — they assert pre-binder lowering, still keyless `Cross`.
- Retarget the n-ary comma guards (`:56`, `:185`) to the no-equi case; add an n-ary keyed-extraction test with
  a predicate that must be routed into an inner `Cross` before it can lift.
- A new `adr270_*` architecture guard: the recognizer emits only `JoinKind::Inner` (no new variant) and is wired
  before bind; the keyed-Inner invariant is value-discriminating (would fail if the rewrite were a no-op).

## Phase 3 — behavior tests (`crates/toron/src/binder/tests.rs` + `cross_join_test.rs`)
Discriminating (each fails under the rejected Cross+Filter / Route-A alternative):
- q14-shaped **unqualified** comma SQL binds to `Join { Inner, on: [(l_partkey, p_partkey)] }`, no residual
  `Cross`, equi not in any residual filter.
- A **qualified** two-table comma equi also lifts (not only the unqualified path).
- `a CROSS JOIN b WHERE a.k = b.k` → keyed `Inner`; `a CROSS JOIN b WHERE a.k < b.k` and `a CROSS JOIN b` stay
  `Cross`.
- Ambiguous unqualified equality does not lift; non-equi / single-side conjuncts stay residual.
- A scalar-subquery `Cross` (ADR 257) is not rewritten.
- n-ary: `FROM a, b, c WHERE a.k=b.k AND b.j=c.j` lifts both edges left-deep.
- Result identity: compare the rewritten output to Cross+Filter semantics on a tractable fixture, including
  column order/schema. Do not pin q14's old digest as the identity proof because strategy changes can perturb
  floating aggregate order.

## Phase 4 — lift the q14 scale cap (`crates/toron-tpch/`)
- Remove `Q14_GAUGE_SCALE` + `resolve_q14_scale` and the q14 arm's cap/refusal in `main.rs`; q14 now resolves
  `--sf` like any query (the general ADR 261 `parse_scale_factor` validation **stays**).
- Update `harness.rs`: replace `q14_scale_is_capped_at_the_gauge_scale` with a test that q14 binds to a keyed
  join and runs at the default scale without a cartesian; keep `q14_dev_executes_and_strict_blocks_at_decimal_case`
  (re-measure — strict still `unsupported_decimal_case@bind`).
- Reconcile `q14_probe.rs`: `q14_without_optimizer_facts_walls_at_cross_join` **inverts** — with the recognizer
  the equi lifts before strategy selection, so no-facts q14 no longer walls at the cross-join (it binds keyed,
  hash-partitioned). Retarget it to assert the keyed-Inner recognition (and that the seeded-facts case still
  reaches the decimal-CASE wall).
- Update the ADR 269 results doc references to the lifted cap (a short note; don't reopen 269's sealed prose
  beyond the cap reconciliation the matrix/tests need).

## Verification gate (via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt  -p toron -p toron-tpch -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check
```
(`--all-targets` check not required — no new widely-constructed IR field; the pass reuses `JoinKind::Inner`.)

## Seal
`impl: adr 270 comma join equi recognition`, `Co-Authored-By: Claude Opus 4.8 (1M context)
<noreply@anthropic.com>`.

## Risk notes
- **Orientation.** `on` pairs are `(left_input_expr, right_input_expr)`; orient via `column_side` (a reversed
  `b.k = a.k` must become `(a.k, b.k)`).
- **Result-identity.** Assert column order / schema unchanged vs the Cross+Filter form on a small deterministic
  fixture — the rewrite is strategy-only. q14's digest may change if floating aggregate order changes.
- **Ambiguity & misfire.** Lean on `column_side`'s `None` for ambiguous/unresolved; never lift a scalar-subquery
  `Cross` (no base-relation bridging equi).
- **Don't touch the ADR 261 boundary.** Remove only the q14-specific cap; keep general `--sf` validation.
- **Recurse correctly.** Each equi attaches to the lowest `Cross` whose two child scopes separately contain its
  operands — verify with the n-ary test.
