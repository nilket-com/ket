# Implementation Plan — ADR 264: Rejection-Accounting Reconcile + Seal-Gate Expansion

## Summary
Reconcile 5 stale "should-reject" accounting tests (the rejected construct became supported via ADR
231/240/254/256), reframe the 6th + add a known-gap assertion for the derived-table logical-type bind bug
(fix deferred to ADR 265), and expand the standard seal gate by adding the full integration suite with a
documented skip list. The existing `--lib`, `architecture_test`, and `toron-tpch` legs remain. No
production-code behavior change — this is test reconciliation + process.

## Phase 1 — reconcile the 5 stale tests
Per test, delete ONLY the now-supported case from the rejection list; keep every other case; add a
`// ADR NNN: <construct> is supported (see <positive test>)` comment at the deletion.

- `aggregate_projection_accounting_test.rs` → `malformed_aggregate_arity_or_modifiers_reject_cleanly`:
  remove `"SELECT COUNT(DISTINCT amount) FROM sales"` and `"SELECT SUM(DISTINCT amount) FROM sales"`
  (ADR 240 — dedup via `Aggregate{Distinct{…}}`). Keep `COUNT()`, `COUNT(a,b)`, `SUM()`, `SUM(a,b)`,
  `LINEAR_REGRESS(amount)`, `APPROX_QUANTILE(...)`.
- `clause_accounting_test.rs` → `unsupported_query_level_clauses_are_loud`: remove the `WITH …` CTE case
  (ADR 254). Keep FETCH / FOR UPDATE.
- `clause_accounting_test.rs` → `unsupported_select_level_clauses_are_loud`: remove the `SELECT DISTINCT`
  case (ADR 231). Keep GROUP-BY-no-agg / HAVING / QUALIFY.
- `predicate_widening_test.rs` → `in_list_rejects_empty_over_limit_subquery_unnest_or_rowvalue`: remove
  `"… WHERE id IN (SELECT id FROM sales)"` (ADR 256 → LeftSemi join). Keep empty/over-limit/UNNEST/rowvalue.
- `predicate_widening_test.rs` → `unsupported_pattern_predicates_and_case_subquery_in_remain_unsupported`:
  remove the same `IN (SELECT …)` case. Keep all LIKE/SIMILAR/RLIKE/`~~` cases.

Grounding probe before editing: re-run each test to reconfirm exactly which case fails today (planner
behavior can have moved again) — delete what the suite proves supported, nothing more.

## Phase 2 — case 6 reframe + known-gap assertion
- `logical_type_ddl_test.rs` → `derived_table_source_scope_rejects_general_subquery_features`: remove the
  JOIN-inside-derived-table case (ADR 254 — supported; plans + binds for plain columns). Keep the truly
  rejected features (nested derived tables, lateral, …) — verify each still rejects.
- Add `join_bodied_derived_table_with_logical_type_is_adr265_known_gap` (same file): build the
  logical-typed fixture, plan the join-bodied derived table, and assert bind currently errors with
  `unknown logical type` (the `LogicalTypeId(...)` propagation gap). Comment: ADR 265 flips this to a
  positive bind+execute assertion. Discriminating — fails the moment 265 lands.

## Phase 3 — seal-gate expansion
- `plans/conventions/council.md`: add the full integration suite to the documented seal gate —
  `scripts/cargo-cgroup test -p toron --no-fail-fast` — run AFTER `cargo fmt` (per
  the ADR 263 finding that fmt can invalidate source-scanning guards), with the rule: a green run
  requires zero failures
  except an explicit, commented `--skip <family>` list of *proven-flaky* families (auditable debt,
  shrinks over time). Curated-inclusion subsets are explicitly rejected (same off-path rot).
- Establish the skip list empirically: run the full suite 3× clean; skip only families that flake
  (candidates: `LiveWendelonFixture` / distributed isolation-ordering). If clean across runs, the list
  starts empty and the gate is the bare `--no-fail-fast`. Document whatever is skipped + why.
- Add or update a short impl-plan gate template in `plans/conventions/council.md` so future ADRs inherit
  the additive full-suite command instead of copying the old three-leg gate.

## Verification gate (cargo via scripts/cargo-cgroup; run AFTER fmt)
```
scripts/cargo-cgroup fmt -p toron -- --check
scripts/cargo-cgroup test -p toron --test architecture_test          # FULL (unchanged guards green)
scripts/cargo-cgroup test -p toron --no-fail-fast [-- --skip <flaky>] # the NEW gate — 0 unexpected fails
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron-tpch                              # existing standard gate leg remains
git diff --check
```
Acceptance: the 6 formerly-red tests are green (5 reconciled, 1 reframed + known-gap), the full suite is
green modulo the documented skip list, and the fences in §"Fences preserved" still reject.

## Seal
`impl: adr 264 rejection-accounting reconcile and seal gate expansion`,
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes
- **Don't over-delete.** Removing more than the proven-supported case silently weakens a fence — the
  honesty check is that each reconciled test still fails if a kept sibling stops rejecting.
- **Skip-list creep.** Every `--skip` is debt; comment each with the flaky family + a note to fix-or-remove.
  An empty skip list is the goal.
- **Case-6 known-gap drift.** Assert the specific `unknown logical type` message so the test is a precise
  tripwire for ADR 265, not a vague "errors somehow".
