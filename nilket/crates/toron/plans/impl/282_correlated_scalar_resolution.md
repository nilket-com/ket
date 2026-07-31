# Implementation Plan — ADR 282: binder-aware correlated scalar subquery resolution

## Summary
The engine cut named by ADR 281's q17 wall (`UnknownColumn { column: "p_partkey" }@bind`): resolve an
unqualified outer ref inside a correlated scalar subquery in the **binder** (catalog-aware), the scalar
analogue of ADR 279. The frontend defers the unclassifiable correlated scalar to a transient
**value-preserving** handoff; the binder classifies by `column_side` ownership and builds ADR 260's
decorrelated grouped-aggregate + `LeftOuter` recombine, with the projection wrapper (`0.2 * AVG`) folded into a
`Project` (no bespoke evaluator). Full `-p toron` gate. Aim q17 → `pass@execute`; defer only a precisely-named
third wall.

## Phase 0 — ground + decide the handoff (verify-first)
Re-confirm the wall (`run --query q17` → `fail(unrecognized@bind)`, raw `UnknownColumn`). Read
`pull_up_scalar_subqueries` (the 257 uncorrelated / 260 correlated branches), `sql_expr_references_outer_qualifier`
(qualified-only detector), `scalar_subquery_bare_aggregate` (bare-only projection), and the recombine
(`join_scalar_subplans` / the `PulledScalar { plan, on }` shape). Decide the handoff representation: a
`LogicalPlan::DeferredScalarSubquery`-style transient node (mirrors `DeferredExists`) carrying
`{ outer_plan, inner_plan, predicates, aggregate call, value_expr_with_placeholder, scalar alias }`.
`PulledScalar` is planner-private, so expect the binder to construct the grouped aggregate + `LeftOuter`
directly while mirroring ADR 260's shape.

## Phase 1 — frontend deferral (`sql_planner.rs`)
In `pull_up_scalar_subqueries`, when a scalar subquery is correlated-*shaped* (a WHERE conjunct references a
bare identifier the frontend cannot prove inner) and the qualifier-driven classify cannot safely handle it,
emit the deferred handoff instead of the uncorrelated `lower_query_scoped` path. Preserve the value expression:
discover exactly one aggregate call in the projection (reject zero / multiple — out of scope), lower the
aggregate call, and lower the projection with that call replaced by an internal placeholder column reference.
Qualified-correlated (260) + uncorrelated (257) paths are untouched (the deferral triggers only on the
unclassifiable unqualified case).

## Phase 2 — binder resolve (`binder.rs` + a resolve helper)
A `bind_deferred_scalar_subquery` mirroring `bind_deferred_exists`:
- **Scratch-trace probe binds** (ADR 279 review fix): probe-bind both outer and inner on a throwaway
  `OptimizerTrace` + `None` render-trace to obtain scopes; only the final real bind records.
- **Classify by ownership:** for each handoff predicate, `column_side` against the outer + inner scopes — a
  simple equality with one uniquely outer-owned side and one uniquely inner-owned side → correlation key pair
  (outer, inner); inner-only → residual; ambiguous / outer-only / non-equi → reject (reuse
  `classify_deferred_exists_predicate`'s shape, or a scalar sibling).
- **Build 260's shape:** inner residual `Filter` → `Exchange`(inner keys) → `Aggregate`(group by inner keys,
  aggr = raw aggregate) → `Project`(keys…, `value_expr` with the placeholder bound to the raw aggregate column)
  → `Alias(__scalar_N: key_i…, value)` → hash-partitioned `LeftOuter` to the outer on the resolved keys,
  exposing `__scalar_N.value` to the rewritten outer expression. The `Project` is where the `0.2 *` wrapper
  lives (wall 2 folded generically).

## Phase 3 — exhaustive match arms (if a new LogicalPlan variant)
Compiler-driven via `check -p toron --all-targets` + `test -p toron --lib`: passthrough (recurse children) for
the pre-bind logical rewrites (`demote_null_rejected_outer_joins`, `recognize_cross_equi_joins`), structural for
analyses (`plan_qualifiers`, `filter_count`, `logical_scope_for_outer_demotion`, `set_ops`, `join_order`,
`StatementClass`), `NotImplemented` in the legacy unbound-logical executor, recurse in the test helpers.
(Resolution eliminates the node at bind, so post-bind sites never see it.)

## Phase 4 — guard reconciliation
- **Variant-count guards** (`core_invariants` + `ddl_admin` ×2 + `dml_delete_update` ×2): bump for the new
  variant (the LogicalPlan checklist — `--lib`/`--all-targets` does NOT catch these).
- **`adr176` binder-root cap:** bump for `bind_deferred_scalar_subquery` (per its "real new IR node/pass" rule).
- **`adr257_scalar_subquery_pull_up_is_frontend_lowering_only` + `adr260_correlated_scalar_is_frontend_only`:**
  retarget explicitly — frontend-only remains true for the classifiable 257/260 paths; the unqualified case now
  has a transient handoff. Update the guard's scan/prose to carve out the handoff, not delete the invariant.
- Run the **FULL** `architecture_test` AFTER fmt (the seal gate).

## Phase 5 — measure q17 (verify-first the second wall)
Re-run q17 in both modes after Phase 2. If the value-preserving handoff carries the wrapper, q17 should reach
`pass@execute`. If a strict Decimal-typing wall on `0.2`/`7.0` surfaces, classify it precisely and **defer to
ADR 283** (don't bloat). Record the measured outcome.

## Phase 6 — tests
- **Engine** (`exists_decorrelation_test.rs` or a scalar sibling): an unqualified bare-aggregate scalar resolves
  to the same plan/digest as its qualified bare-aggregate control (the discriminating test — fails under the
  old qualifier-driven path); a wrapped unqualified scalar exercises the `0.2 * AVG` projection handoff; an
  **ambiguous** unqualified ref (`fact`/`dim` both own `k`) rejects (safe-resolution invariant); a
  trace-stability test (the probe binds don't perturb the recorded trace). Reuse the `fact`/`dim` fixture with a
  distinct value column for the wrapped-aggregate path.
- **Harness** (`toron-tpch`): flip the q17 row test from `fail(unrecognized@bind)` to its measured outcome
  (`pass@execute` + digest, or the precisely-named next wall). The seven reach + three correctness cells stay
  green.

## Verification gate (engine cut — full `-p toron`, via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt   -p toron -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --lib
scripts/cargo-cgroup test  -p toron --test architecture_test     # FULL, after fmt
scripts/cargo-cgroup test  -p toron --no-fail-fast
scripts/cargo-cgroup test  -p toron-tpch
scripts/cargo-cgroup run   -p toron-tpch -- run --query q17 --mode dev --allow-blocked
scripts/cargo-cgroup run   -p toron-tpch -- run --query q17 --mode strict --allow-blocked
scripts/cargo-cgroup run   -p toron-tpch -- matrix               # q17 measured outcome; others unchanged
git diff --check
```

## Seal
`impl: adr 282 correlated scalar resolution`, `Co-Authored-By: Claude Opus 4.8 (1M context)
<noreply@anthropic.com>`. Add `crates/toron/plans/impl/282_correlated_scalar_resolution_results.md` with the
measured q17 row + the handoff/resolve shape + (if deferred) the named third wall.

## Risk notes
- **Scratch trace** — the ADR 279 footgun; the scope-probe binds must not record into the real trace (verify
  with a trace-stability test, and the full `-p toron` EXPLAIN/replay suite).
- **Value-preserving handoff, not bespoke eval** — the wrapper rides the existing `Project`/expression paths;
  don't add a special-case evaluator for `0.2 * AVG`.
- **Guard retarget, not delete** — keep the 257/260 frontend-only invariant for the classifiable paths.
- **Variant blast radius** — full LogicalPlan checklist; `check` alone misses `cfg(test)`/integration matches.
- **Measured acceptance** — required = past `UnknownColumn`; target = `pass@execute`; defer only a precisely-
  named third wall. Don't promise green before Phase 5.
- **Official q17 unqualified** — qualified form is a control, not a replacement; don't pre-qualify to dodge.
