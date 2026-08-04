# ADR 282 — Results: binder-aware correlated scalar subquery resolution

## Measured rows (tiered acceptance met)
```
q17 dev=pass@execute   strict=blocked(unsupported_decimal_scalar@bind)
```
- **DEV reaches execute** (94 ms at sf=0.0001): the unqualified correlated scalar resolves end-to-end over the
  `Float64` proxy. The **required** tier (past `UnknownColumn { column: "p_partkey" }`) AND the dev **target**
  (`pass@execute`) are met.
- **STRICT advances past the resolution** to the precisely-named **third wall**: `0.2` / `7.0` are `Float64`
  literals against the official `Decimal128` columns → a recognized `unsupported_decimal_scalar` blocker. This
  is the Decimal-literal-typing cut, **deferred to ADR 283** (the acceptance's defer tier). The correlated-scalar
  resolution itself is proven by dev; strict simply hits the next, orthogonal Decimal gap.

## What was built (engine cut — `crates/toron`)
- **New IR node `LogicalPlan::DeferredScalarSubquery`** (27→28→**29** variants), the scalar analogue of ADR
  279's `DeferredExists`: frontend-produced, binder-consumed, never reaching exec.
- **Frontend** (`sql_planner`): `pull_up_scalar_subqueries` detects an UNqualified correlation candidate
  (`where_has_unqualified_correlation_candidate` — a bare-identifier equality the qualifier-driven
  `sql_expr_references_outer_qualifier` misses) and defers instead of taking the uncorrelated path.
  `replace_scalar_aggregate` extracts the single aggregate and rewrites the projection wrapper with the
  aggregate replaced by `__scalar_N.value`. **Design choice:** the `0.2 *` wrapper rides the **rewritten outer
  expression** (`l_quantity < 0.2 * __scalar_N.value`) with the subplan computing the bare aggregate — provably
  equivalent to the plan's subplan-`Project` framing, but it reuses ADR 260's exact bare shape and the existing
  arithmetic/binder/executor paths, so no subplan `Project` or placeholder binding is needed (no bespoke
  evaluator). `PulledScalar` became an enum (`Resolved` | `Deferred`); `join_scalar_subplans` wraps a `Deferred`
  in the node.
- **Binder** (`bind_deferred_scalar_subquery`): scratch-trace probe binds for scopes (the ADR 279 review fix),
  classifies each predicate by **inner-first** ownership, then builds ADR 260's shape directly — `Exchange` →
  `Aggregate`(group by inner keys) → `Alias(__scalar_N[key_i…, value])` → `bind_join` `LeftOuter` (or keyless
  `Cross` when uncorrelated).

## The load-bearing insight: inner-first ownership
q17's **inner** subquery FROM is `lineitem`, which is **also** in the outer FROM (`lineitem, part`). Symmetric
`column_side` (279's EXISTS classifier) flags `l_partkey` ambiguous (in both scopes) → wrongly rejects. SQL
subquery scoping is **inner-first** — the inner `lineitem` shadows the outer — so `l_partkey` is the inner key
and `p_partkey` (only in the outer) is the correlation key. `scalar_column_side` /
`classify_deferred_scalar_predicate` implement this; it generalizes the disjoint-scope EXISTS case (where
inner-first and symmetric agree).

## Constraints held (the council's three)
- **Scratch trace** for the scope-probe binds (the ADR 279 medium bug) — copied.
- **Value-preserving handoff** — the wrapper rides existing expression paths; no bespoke evaluator.
- **257/260 frontend-only guards retargeted** (not deleted): the comments carve out the unqualified binder
  handoff; the assertions (planner-side machinery + IR/exec/fragment stay clean of the decorrelation functions)
  still hold.

## Guards / blast radius
LogicalPlan 28→29 (5 variant-count sites bumped); `adr176` binder-root 5,706 → 5,843; ~14 exhaustive match arms
(passthrough in pre-bind rewrites, structural in analyses, `NotImplemented` in legacy exec, recurse in tests);
`adr257`/`adr260` comments retargeted. COUNT in an unqualified correlated scalar is rejected (empty-group
coalesce out of scope; q17 is AVG).

## Review fixes (post-seal)

Two review findings on the sealed cut, plus a hollow-green correction — all folded back in:

- **Finding 1 (High) — default-scale cartesian / OOM.** The CLI default is `sf=0.01`, but the gauge test ran
  only `sf=0.0001`. At `sf=0.01` `q17 dev` ran ~178 s then OOM-killed (exit 137). **Root cause:** the comma
  join lowers to `Cross(lineitem, part)` with the bridging equality (`p_partkey = l_partkey`) in a WHERE filter
  above; ADR 270's `recognize_cross_equi_joins` only folds `Filter(equi, Cross)` when the `Cross` is *directly*
  under the filter, but the new `DeferredScalarSubquery` node sits **between** them — stranding the equality, so
  the comma join executed as a materialized 120M-row cartesian. **Fix:** the recognizer now pushes a deferred
  scalar's outer-only conjuncts down into its `outer` (a valid pushdown through the left-preserving recombine;
  `push_deferred_scalar_outer_filter`, a separate future so the deep-recursing dispatch's poll frame stays
  small), so the comma join folds to a keyed inner join. `q17 dev sf=0.01`: OOM/178 s → **133 ms**.
- **Finding 2 (Medium) — inner-residual parity.** `classify_deferred_scalar_predicate` only accepted a
  column=column equality, rejecting an inner-only / constant residual (e.g. `dim.active = true`) that the EXISTS
  sibling keeps (ADR 260 parity). **Fix:** rewritten to mirror `classify_deferred_exists_predicate` — any
  predicate that references no outer-owned column is an inner residual; the column-equality requirement is
  reserved for genuinely-correlated predicates.
- **Hollow-green correction.** At `sf=0.0001` no `part` matches `Brand#23`+`MED BOX`, so the gauge ran the
  recombine over an **empty** result — the resolution was never exercised with data. New engine-level tests
  cover it with matching rows.

## Tests
- `toron-tpch::q17_dev_executes_strict_hits_decimal_literal_wall` — dev `pass@execute`, strict
  `blocked(unsupported_decimal_scalar@bind)`.
- `toron::correlated_scalar_test` (new, engine-level, over matching data):
  - `comma_join_under_deferred_scalar_lifts_to_keyed_inner` — Finding 1, discriminating (pre-fix the oversized
    keyless Cross fails loud; verified by reverting the recognizer arm) + a non-equi control that stays a Cross.
  - `deferred_correlated_scalar_with_inner_residual` — Finding 2, discriminating (pre-fix the residual rejects
    at bind; verified by reverting the classifier).
  - `deferred_correlated_scalar_executes_over_matching_data` — anti-hollow-green: correct rows over matching data.

## Gate (engine cut — full `-p toron`)
fmt --check · `check --all-targets` · full `architecture_test` **2053** (incl. the 5 variant-count guards +
adr176 — the new test file added to its surface allowlist — + the retargeted 257/260 guards) · `--no-fail-fast`
**4412, 0 failed** · `-p toron-tpch` (incl. q17) · q17 dev `pass@execute` and `sf=0.01` tractable (133 ms).

A pre-existing deep test (`group_by_test::sql_aggregate_derived_top_k_executes`, predates this cut) overflowed
libtest's ~2 MB thread stack under cumulative bind-frame growth; given the repo's `q10` precedent it was moved
onto an explicit 64 MB test stack (production/CLI use larger stacks). Verified unrelated to the deferred-scalar
path (it overflows with the recognizer arm reverted).

## Forward arc
ADR 283: the strict Decimal-literal-typing wall (`0.2`/`7.0` `Float64` literals vs `Decimal128`) — turning
strict q17 green. Then q02 (adds `partsupp`) and q22 open on the reach side; q05 correctness on the rail.
