# ADR 264 — Rejection-Accounting Reconcile + Seal-Gate Expansion

## Status

Proposed.

## Context

The full `cargo test -p toron` integration suite carried **6 pre-existing red "should-reject" accounting
tests** that hid outside the council's standard seal gate (`--lib` + `--test architecture_test` +
`toron-tpch`). ADR 263's thorough post-fmt run surfaced them. Grounded classification (read each test,
ran the planner, git-blamed the behavior change):

| # | Test | Construct no longer rejecting | Verdict | Landed by |
|---|------|-------------------------------|---------|-----------|
| 1 | `aggregate_projection_accounting::malformed_aggregate_arity_or_modifiers_reject_cleanly` | `COUNT(DISTINCT x)`, `SUM(DISTINCT x)` | stale — real dedup (`Aggregate{Distinct{…}}`) | ADR 240 |
| 2 | `clause_accounting::unsupported_query_level_clauses_are_loud` | `WITH …` (CTE) | stale | ADR 254 |
| 3 | `clause_accounting::unsupported_select_level_clauses_are_loud` | `SELECT DISTINCT` | stale | ADR 231 |
| 4 | `predicate_widening::in_list_rejects_empty_over_limit_subquery_unnest_or_rowvalue` | `x IN (SELECT …)` | stale | ADR 256 |
| 5 | `predicate_widening::unsupported_pattern_predicates_and_case_subquery_in_remain_unsupported` | `x IN (SELECT …)` | stale | ADR 256 |
| 6 | `logical_type_ddl::derived_table_source_scope_rejects_general_subquery_features` | JOIN inside a derived table | intended feature (254) **+ narrow bind bug** | ADR 254 |

**Zero lost-rejects.** Five are stale accounting tests — a later ADR added the feature and updated *its*
tests but not these older lists, which went red off the green path. The 6th is a real binder
feature bug (logical-typed column through a join-bodied derived table → `unknown logical type
LogicalTypeId(1025)` at bind) exposed by a now-obsolete rejection test. Every other construct in the six
tests still rejects correctly (FETCH / FOR UPDATE / QUALIFY / GROUP-BY-no-agg / HAVING / empty &
over-limit IN-list / UNNEST / row-value IN / LIKE ANY / SIMILAR TO / RLIKE / `~~` …) — verified.

This is twofold drift: stale tests AND a process hole (the standard gate doesn't run integration tests,
so feature ADRs silently left these red). 264 reconciles both; the binder bug is split to ADR 265.

## Decision

### 1. Reconcile the 5 stale rejection-accounting tests
Drop **only** the now-supported construct from each rejection list, keeping every still-rejected sibling.
Add a one-line comment at each deletion pointing at the owning ADR's positive coverage (e.g. ADR 256
`in_subquery_lowers_to_semi_join`, ADR 240 dedup tests) so the removal reads as an intentional fence
move, not a weakened guard.

### 2. Case 6 — reframe + known-gap assertion (fix deferred to ADR 265)
Remove the join-in-derived-table case from `derived_table_source_scope_rejects_general_subquery_features`
(it's supported), keeping the genuinely-rejected features (nested derived tables, lateral, …). Add a
**known-gap assertion** that pins the *current honest behavior*: a join-bodied derived table exposing a
**logical-typed** column errors at bind with the `unknown logical type` message. The assertion is
discriminating — it trips the day ADR 265 fixes the propagation, forcing the test to flip to a positive
bind+execute assertion. Comment nominates ADR 265.

### 3. Seal-gate expansion (full integration suite + documented skips)
Add `scripts/cargo-cgroup test -p toron --no-fail-fast` (the full integration suite) to the standard seal
gate so this drift class surfaces on the green path. This is additive: the existing `--lib`,
`architecture_test`, and `toron-tpch` legs stay in the run-book. Known-flaky families (for example,
`LiveWendelonFixture` / distributed isolation-ordering, if repeated reruns prove them flaky) go behind a
**documented, explicit `--skip` list** — auditable debt that should shrink over time, not a curated
inclusion list (a curated subset recreates the same off-path rot for anything not curated in). The skip
list is empirically grounded: only families *proven* flaky by repeated runs are skipped; if today's full
run is clean (it was, modulo the 6 stale tests), the skip list starts minimal/empty. Encoded in
`plans/conventions/council.md` as the run-book gate and the future impl-plan gate template.

## Fences preserved (must still reject — the reconcile is surgical)
FETCH FIRST / FOR UPDATE / QUALIFY / GROUP BY without aggregate / HAVING without GROUP-BY-or-aggregate /
empty `IN ()` / over-limit IN-list / `IN UNNEST(...)` / row-value IN / `LIKE ANY` / column-pattern LIKE /
`ESCAPE` / `SIMILAR TO` / `RLIKE` / `~~` / `~~*` / aggregate arity errors (`COUNT()`, `SUM(a,b)`) /
experimental funcs (`LINEAR_REGRESS`, `APPROX_QUANTILE`) / nested derived tables / lateral.

## Deferred (named)
**ADR 265 — logical types through join-bodied derived tables.** Thread the logical-type registry /
schema propagation so a logical-typed column survives a join inside a derived table; flip the 264
known-gap assertion to a positive bind+execute test.

## Honesty / discriminators
- Each reconciled test still FAILS if a genuine fence is deleted (the still-rejected siblings remain) — it
  is not blanket-weakened.
- The case-6 known-gap assertion fails when ADR 265 lands (a live tripwire, not an `#[ignore]` that rots).
- The seal-gate change is enforced by being in the run-book + impl template; the very next ADR's gate
  runs the full suite, so a fresh stale-accounting red can't hide again.

## Why toron-home
Pure toron frontend rejection accounting + toron's seal-gate run-book. No cross-ring surface.
