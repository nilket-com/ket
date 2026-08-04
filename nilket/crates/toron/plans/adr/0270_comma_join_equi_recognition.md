# ADR 270 — comma-join equi-join recognition

## Status
Proposed.

## Context
ADR 269 brought `tpch.q14` — the first multi-table query — into the reachability gauge and proved a structural
gap: faithful TPC-H **comma joins** execute as a broadcast cartesian + residual filter. `FROM a, b WHERE a.k =
b.k` lowers to `Filter(a.k = b.k) over Join { kind: Cross, on: [] }` (`sql_planner.rs:3664-3667`, `:3748`,
`:3818-3827`); the equi-predicate is **never lifted** into a join key. Explicit `FROM a JOIN b ON a.k = b.k`
*does* lift (`lower_join_on` → `build_auto_join_plan`), producing `Join { kind: Inner, on: [(a.k, b.k)] }`, and
the binder strategy-selection + fragmenter (`fragment_hash_join_plan` / `fragment_broadcast_join_plan`) already
build keyed inner joins. So the keyed-join **engine is fully landed**; only *recognition* of the comma-join
equi is missing.

That gap forced ADR 269's q14 scale cap: a keyless `Cross` over two real tables is O(left × part), intractable
above the gauge scale, so q14 is capped at `Q14_GAUGE_SCALE`.

The recognition **cannot live in the SQL planner.** Official TPC-H q14 uses *unqualified* join columns
(`WHERE l_partkey = p_partkey`), and at SQL-lowering time Toron has no catalog: a bare identifier lowers to an
unqualified `ColumnRef` (`sql_planner.rs:5915`) and the frontend scope classifier `sql_column_ref` returns an
error for any bare identifier (`:9214-9222`). A SQL-level recognizer could only handle qualified predicates —
i.e. it would force TPC-H to be rewritten into a Toron dialect, which defeats the gauge. TPC-H uses unqualified
columns pervasively, so recognition must run **where the catalog/relation scopes exist: the binder.**

## Decision
ADR 270 is a **binder-side, syntax-agnostic optimizer rewrite** that lifts cross-bridging equalities out of a
filtered keyless cartesian into a keyed inner join. **It widens the grammar, not the engine** — it produces the
`Inner`+keys logical shape the binder/fragmenter/executor already handle; no new join kind, operator, or IR
field.

A pre-bind normalization pass (sibling to `demote_null_rejected_outer_joins`, `binder.rs:222`) recognizes:

```
Filter { predicate, input: Join { kind: Cross, on: [], left, right } }
```

splits `predicate` into top-level `AND` conjuncts, and routes each conjunct to the lowest join boundary that
can own it. For each `Expr::Eq(l, r)` of direct columns where one operand resolves **uniquely** to the left
child scope and the other **uniquely** to the right (catalog-backed `RelationScope`), the pass lifts the pair
into the join's `on` (oriented left-expr/right-expr), demoting the `Cross` to `Inner` with `null_safe_keys =
false` and preserving the original `strategy_intent`. Conjuncts that resolve wholly to one child are pushed
into that child and recursively reconsidered there; this is what lets `FROM a, b, c WHERE a.k=b.k AND b.j=c.j`
lift both edges even though `a.k=b.k` is left-only at the outer `Cross`. Only conjuncts that cannot be lifted or
routed are re-wrapped as the residual `Filter`.

**Syntax-agnostic (the resolved fork).** By binder time a comma fold and an explicit `CROSS JOIN` are *both*
`JoinKind::Cross { on: [] }` with no provenance distinguishing them. Rather than add an IR provenance marker to
preserve a syntax distinction, the rewrite fires on **any** keyless `Cross` carrying an extractable bridging
equi, comma or explicit. This is sound and strictly beneficial:

- **Result-identical, not merely equivalent.** `a CROSS JOIN b WHERE a.k = b.k` ≡ `a INNER JOIN b ON a.k =
  b.k`: identical rows (NULL keys are dropped the same way — `NULL = …` filters in both), identical schema
  (both concat `left ++ right`), identical SQL column order (the existing broadcast right-build `swap +
  restore_declared_join_order_project` preserves it). The rewrite changes *strategy*, never the answer.
- **Strictly improves explicit cross.** Today `CROSS JOIN … WHERE a.k = b.k` either hits
  `cross_requires_broadcast_error` (`binder/join.rs:630`) when no side is broadcast-eligible — a hard rejection
  — or runs the O(n×m) cartesian. Recognition turns both into a keyed join: it lifts a rejection and removes a
  cartesian. There is no regression case.
- **The cartesian guardrail stays where it's real.** A genuine cartesian — `CROSS JOIN b` with no bridging
  equi, or `WHERE col = literal` — has nothing to extract, stays keyless `Cross`, and keeps its broadcast-only
  guardrail. The guardrail is removed only where it was spurious (the predicate already *was* an equi-join).

Carrying syntax provenance would add IR state purely to preserve a strictly-worse plan that never changes
results — rejected.

## Watch-items (operator) → guards
- **Result-identity.** The rewrite must not change rows, schema, or column order — it is strategy-only. A
  discriminating fixture should compare the rewritten plan's output to the old Cross+Filter semantics on a
  tractable dataset. Do not require q14's old digest to be byte-stable: changing join strategy can change
  floating aggregate accumulation order even when the SQL result is the same.
- **Ambiguity never silently picks a side.** `column_side` (`outer_join_demotion.rs:1220`) returns `None` when
  a column resolves to both scopes or neither; an ambiguous unqualified equality must **not** lift — it stays
  residual and falls through the normal ambiguous-column path.
- **No misfire on scalar-subquery crosses.** ADR 257 pulls uncorrelated scalar subqueries up as bounded scalar
  `Cross` joins (row_upper_bound ≤ 1, `__scalar_N` columns). Those carry no base-relation bridging equi, so the
  recognizer leaves them untouched — pinned by a test.
- **`Cross` is preserved where it should be.** Explicit `CROSS JOIN` with no extractable equi, non-equi
  cross-side predicates (`a.k < b.k`), and OR-disjoined equis stay keyless `Cross`.
- **q14 scale cap lifts only on proof.** The ADR 269 q14 cap (`resolve_q14_scale` / `Q14_GAUGE_SCALE`) is
  removed only after the harness proves q14 binds to a keyed (non-cartesian) join and runs at the default
  scale. The general ADR 261 `--sf` validation (`parse_scale_factor`) is untouched.

### Guards to reconcile (discipline #4 — grounded names)
- **Unchanged:** `cross_join_test.rs:77 cross_join_where_filters_after_cartesian` — its filter is `left_t.id =
  1` (a single-side literal, not a bridging equi), so it correctly stays `Filter over Cross`.
- **Stay (frontend-shape guards):** `comma_join_lowers_to_keyless_cross` (`:42`) and
  `adr239_cross_join_and_comma_lower_to_keyless_cross` (`architecture/cross_join.rs:43`) assert the **SQL
  planner** output, which still emits keyless `Cross` (the binder pass rewrites it later). They may stay; the
  new keyed-`Inner` invariant is asserted by new **bind-trace** tests rather than by rewording these.
- **Retarget:** `n_ary_comma_join_folds_left_deep_cross` (`:56`) / `n_ary_comma_join_binds_left_deep_broadcast_cross_chain`
  (`:185`) — keep no-equi n-ary as `Cross`; add an n-ary keyed-extraction test where one predicate initially
  classifies as left-only at the outer `Cross` and must be routed into the inner `Cross`.
- **Reconcile (inverts):** `q14_without_optimizer_facts_walls_at_cross_join` (ADR 269 `q14_probe.rs`) — with the
  recognizer the equi is lifted *before* strategy selection, so even with no optimizer facts q14 no longer
  walls at the keyless cross-join (it binds to a keyed inner join, hash-partitioned absent broadcast facts).
  Retarget that probe.
- **Add:** `a CROSS JOIN b WHERE a.k = b.k` → keyed `Inner` (new behavior under the syntax-agnostic rewrite, not
  an inversion — no existing test covers explicit-cross-with-bridging-equi).
- The pass emits `JoinKind::Inner` (an existing variant) — the JoinKind-variant guards do not change.

## Acceptance
- `FROM a, b WHERE a.k = b.k` binds as a keyed `Inner` join for **both** qualified and unqualified key columns;
  `… AND b.flag` keeps `b.flag` residual; `a.k < b.k` / `CROSS JOIN b` stay keyless `Cross`.
- `a CROSS JOIN b WHERE a.k = b.k` binds as keyed `Inner`; ambiguous unqualified equalities do not lift; a
  scalar-subquery `Cross` is not rewritten; n-ary comma joins lift one edge per applicable left-deep `Cross`,
  including edges discovered only after routing a conjunct into a child.
- `tpch.q14` (unqualified, unchanged official shape) binds/traces to `Join { kind: Inner, on: [(l_partkey,
  p_partkey)] }` with no residual `Cross`; dev `pass@execute` through a non-cartesian keyed join at the default
  harness scale; strict still `blocked(unsupported_decimal_case@bind)`; q01/q06 unchanged; the q14 scale cap is
  removed/raised after measurement.

## Forward arc
- q14's Decimal mini-arc resumes from the ADR 269 strict blocker: CASE Decimal-vs-integer unification →
  Decimal-literal coercion in `*` → scalar `Decimal ÷ Decimal`.
- The multi-table TPC-H frontier opens (q03/q05/q10/… are all comma-joined and unqualified).
- Cost-based join **reordering** stays a separate frontier — ADR 270 creates the keyed edges that
  `binder/join_order.rs` (star/graph actuation) can then order; it does not change ordering.

## Why toron-home
Binder recognition + the reachability gauge. No cross-ring surface.
