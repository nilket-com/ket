# ADR 279 — Results: unqualified correlated EXISTS resolution (q04 flips green)

## Measured row
```
q04 dev=pass@execute  strict=pass@execute   (was blocked(unsupported_correlated_exists@plan))
Reachability matrix: q01/q03/q04/q05/q06/q10/q14 all dev+strict pass@execute (seven reach cells)
Correctness: q06 + q01 strict=pass@compare (unchanged)
```
q04 — the gauge's first correlated-subquery query — reaches execute: its unqualified correlated EXISTS
(`l_orderkey = o_orderkey`) now resolves, the official `+ INTERVAL '3' MONTH` (literal+literal) folds, and the
semi-join + `COUNT`/`GROUP BY`/`ORDER BY` follow. The 7th reach cell.

## The measured handoff size (Phase 0's deliverable — NOT localized)
The chosen handoff is a **new IR node, `LogicalPlan::DeferredExists { outer, inner, predicates, negated }`**
(frontend-produced, binder-consumed, never reaching exec). The grounded reason a placeholder wouldn't do:
`LogicalPlan::Join` carries only resolved `on` keys (no residual field), and there is no `Expr::Exists` — so an
un-classified correlated EXISTS has nowhere to live without a real node. The measured blast radius:
- **+1 LogicalPlan variant** (27 → 28): bumped the variant-count guard in **5 places** (`core_invariants` +
  `ddl_admin` ×2 + `dml_delete_update` ×2 — they all assert the same count under historical ADR labels) and
  the **adr176 binder-root bound** (5,605 → 5,706, for the resolve method incl. its scratch-trace probe binds,
  per the guard's "bump for real new IR nodes/passes").
- **~14 exhaustive-match arms**: passthrough (recurse outer+inner) in the pre-bind logical rewrites
  (`demote_null_rejected_outer_joins`, `recognize_cross_equi_joins` — they run before bind, so the outer
  query's own joins still process); structural in the analyses (`plan_qualifiers`→outer, `filter_count`,
  `logical_scope_for_outer_demotion`→None, `distinct_input_unsupported_message`→outer, `join_order`,
  `StatementClass`→Select, conjunct routing); `NotImplemented` in the legacy unbound-logical executor; recurse
  in two test helpers.
- **Frontend** (`sql_planner`): `lower_exists_join` defers to `DeferredExists` **only** on the unqualified-ref
  error (`unsupported EXISTS column reference`); genuinely-unsupported QUALIFIED shapes (outer-only, non-equi,
  computed keys) still reject at the frontend as before.
- **Binder resolve** (`bind_deferred_exists` + `classify_deferred_exists_predicate`, reusing the ADR 270
  `column_side`/`RelationScope` machinery): bind both sides → classify each predicate by **unique column
  ownership** → correlated equalities become outer-first `on` keys, inner-only/constant become an inner
  residual filter, ambiguous/outer-only/non-column-equi **reject** → bind through the normal `LeftSemi`/
  `LeftAnti` path. The scope-probe binds run on **scratch trace/render state** (review fix) so the discarded
  plans don't double-record optimizer decisions / render edges into the real trace (which would corrupt
  EXPLAIN/replay IDs + shift join indices). ~250–300 LOC across ~10 files + guards — a real engine cut, exactly
  as the review warned; not a narrow hook.

## Invariants held
- **Safe resolution, no heuristics:** an unqualified ref resolves to the scope that *uniquely* owns it;
  ambiguous (`k` in both `fact` and `dim`) → reject (pinned by `unqualified_ambiguous_correlated_exists_rejects_in_binder`).
  Classification is by `column_side` ownership, never by `l_`/`o_` name shape.
- **`SqlPlanner` stays catalog-free** — it only *defers*; resolution lives in the binder.
- **EXISTS semantics preserved** — resolves to `LeftSemi`/`LeftAnti`, no outer-row multiplication.
- **Scope held** — correlated EXISTS only; scalar/`IN`/q17/q20/q22 and the runtime `column ± interval` kernel
  stay deferred.

## Tests
- `toron-tpch::q04_executes_in_both_modes` — q04 dev+strict `pass@execute` (the integration proof of
  unqualified-resolves). `correlated_exists_error_classifies_precisely` updated to the binder's ambiguity reject.
- `exists_decorrelation_test::unqualified_unique_correlated_exists_matches_qualified` — an unqualified unique
  pair (`active`/`flag`) lifts to the *same keyed semi-join digest* as its qualified equivalent (proves a real
  keyed lift, not a keyless/wrong fallback — review fix).
- `exists_decorrelation_test::unqualified_ambiguous_correlated_exists_rejects_in_binder` — the ambiguity guard.
- The 3 existing EXISTS reject-tests reconciled: qualified-unsupported shapes still frontend-reject; the
  unqualified-ambiguous case moved frontend→binder.

## Gate (engine cut — full `-p toron`)
fmt clean · `check -p toron --all-targets` clean · `test -p toron --lib` 1110 · **full `architecture_test`
2053** (incl. the 5 variant-count guards + adr176) · `test -p toron --no-fail-fast` **4408 passed, 0 failed** ·
`test -p toron-tpch` green (harness 18, correctness 11) · live `matrix` shows q04 `pass@execute` (seven reach
cells) · `git diff --check` clean.

## Forward arc
With q04 green, the remaining frontier is the runtime date-interval kernel (`column ± interval`) and the harder
correlated forms — scalar/`IN` correlated (q17/q20), and q22. A q03/q05 correctness oracle stays available.
