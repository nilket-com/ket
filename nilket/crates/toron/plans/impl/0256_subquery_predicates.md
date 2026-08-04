# Implementation Plan — ADR 256: Uncorrelated Subquery Predicates

## Summary

Frontend-lowering desugar in `sql_planner.rs`: `IN (subquery)` / uncorrelated `EXISTS` / `NOT EXISTS`
become `LeftSemi`/`LeftAnti` joins via the existing `build_join_plan`. No IR / binder / executor /
fragmenter change. `NOT IN`, scalar subqueries, correlated `IN`, and `SELECT` without `FROM` stay
rejected.

## Verify-first probes (done in the iter cycle — recorded)

1. Keyed `LeftSemi` membership (`IN` shape) binds + executes → `(A,2),(B,1)`. ✓
2. Keyless `LeftSemi` (`on = []`, uncorrelated `EXISTS` shape) → pass-all iff non-empty (5 / 0). ✓
   No new operator needed.
3. **Fence**: a SEMI JOIN with a derived right side trips adr145 — so build the `Join` node directly in
   the IN-lowering, never via SQL JOIN-with-derived syntax.

## Phase 1 — `IN (subquery)` in WHERE lowering

`lower_where` (`:5201`) already routes `EXISTS` conjuncts to `lower_exists_join`. Add a sibling arm for
`SqlExpr::InSubquery { expr, subquery, negated }`:
- `negated == true` → `NOT IN` → reject ("NOT IN (subquery) is not supported; use NOT EXISTS") — deferred.
- else → `lower_in_subquery_join(outer_plan, expr, subquery)`:
  - validate the subquery is uncorrelated + projects exactly one column (reuse / mirror
    `validate_exists_*` shape checks; reject correlated IN — "correlated IN is not supported");
  - lower the subquery to a standalone plan (its single output column = the membership key);
  - `build_join_plan(outer, subquery_plan, LeftSemi, on = [(self.lower_expr(expr), <subquery col_0
    ref>)], HashPartitioned, …)` — the same call `lower_exists_join` makes.
- Remove the `SqlExpr::InSubquery { .. } => Err(...)` reject at `:5440` (the expression-position reject;
  IN-subquery is now handled in WHERE-conjunct position). Keep rejecting IN-subquery in non-WHERE
  expression positions (it only lowers as a WHERE predicate this cut).

## Phase 2 — uncorrelated `EXISTS` (keyless semi) + `NOT EXISTS`

In `lower_exists_join` (`:5227`), the `on.is_empty()` branch (`:5264`) rejects uncorrelated EXISTS. Replace
the reject with a **keyless** build: `build_join_plan(outer, inner_plan, kind, on = [], …)` where `kind`
is `LeftAnti` for `NOT EXISTS`, `LeftSemi` for `EXISTS` (the `negated` flag already drives this below the
reject). Probe 2 confirmed the keyless path executes. `NOT EXISTS` keyed/keyless both already work
(`LeftAnti`).

## Phase 3 — Tests, guards, verification, seal

### Behavioral gates (`tests/group_by_test.rs` — reuses the left_dups/right_dups/empty_sales fixture)

`in_subquery_lowers_to_semi_join` (`label IN (SELECT … )` membership),
`uncorrelated_exists_passes_all_rows_iff_subquery_nonempty` (non-empty vs `empty_sales`),
`not_exists_lowers_to_anti_join`, `not_in_and_scalar_subquery_stay_rejected`. Note the FixtureScan pushed-predicate drop: avoid relying on a
pushed scan filter inside the subquery (use membership over base columns / aggregates, as the 254/255
tests do).

### Architecture guards (`tests/architecture/subquery_predicates.rs`, new module + mod.rs)

`adr256_in_and_uncorrelated_exists_reuse_semi_anti_join`,
`adr256_subquery_predicates_are_frontend_lowering_only`, `adr256_not_in_and_scalar_subquery_stay_deferred`.

### Verification gate (every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test group_by_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr256
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (new test file → adr176 allowlist; reject-string scanners)
scripts/cargo-cgroup test  -p toron --test exists_decorrelation_test   # correlated-EXISTS regression
scripts/cargo-cgroup test  -p toron --test distributed_process_test
scripts/cargo-cgroup test  -p toron --lib                              # no IR/wire change
cargo fmt -p toron -- --check
git diff --check
```

### Seal

`impl: adr 256 subquery predicates`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes

- **Build the Join directly** (probe-3 fence): the IN-lowering constructs `LogicalPlan::Join` via
  `build_join_plan`, not by synthesizing SQL JOIN-with-derived syntax (which trips adr145).
- **Uncorrelated detection**: the subquery must not reference outer columns; reuse the EXISTS
  correlation classifier so correlated IN rejects cleanly rather than mis-lowering.
- **`NOT IN` stays rejected** with a message pointing to `NOT EXISTS` (the deferred null-aware cut).
- Reject-string scanners: removing the `IN subquery` / `uncorrelated EXISTS` rejects may trip architecture
  guards pinning them — grep `adr*` and reconcile (as in 250/252/254).
