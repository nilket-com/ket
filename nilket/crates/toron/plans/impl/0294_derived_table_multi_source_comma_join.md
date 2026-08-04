# ADR 294 — Implementation plan: derived-table comma-list FROM body

Pairs with `plans/adr/0294_derived_table_multi_source_comma_join.md`. Engine cut (`crates/toron`), shared planner
surface. Anchors are current-tree.

## Phase 0 — confirm the wall + the reuse
Confirm before editing: q07 → `blocked(unsupported_derived_multi_source@plan)`, raw
`"derived table subquery must have exactly one FROM source"` from `validate_derived_select_shape`
(`sql_planner.rs:9264`). Confirm `lower_derived_table` (`:5148`) calls `self.lower_select(select, cte)` and that
`lower_select`'s comma-join fold (`is_comma_join = from.len() > 1`, `:3667`) + `recognize_cross_equi_joins` are
the path the relaxed body will flow through.

## D1 — relax `validate_derived_select_shape` (`crates/toron/src/sql_planner.rs:9242`)
Replace the `select.from.len() != 1` guard with per-FROM-entry validation. Cover both the top-level
`TableWithJoins.relation` and each explicit `join.relation`, so later comma-list entries cannot become a new
nested-derived escape hatch:
```rust
if select.from.is_empty() {
    return Err(unsupported("derived table subquery must have at least one FROM source"));
}
for source in &select.from {
    validate_derived_table_source(source)?;
}
Ok(())
```
Sketch:
```rust
fn validate_derived_table_source(source: &TableWithJoins) -> Result<(), FrontendError> {
    validate_derived_table_factor(&source.relation)?;
    for join in &source.joins {
        validate_derived_table_factor(&join.relation)?;
    }
    Ok(())
}
```
- Preserve the surrounding `validate_derived_query_shape` / `validate_derived_select_shape` clause fences
  (top/into/lateral_views/window/qualify/etc.) — only the FROM-arity rule changes.
- Keep the factor check narrow (ordinary `Table` ok; `Derived` = nested/lateral/sampled derived reject; everything
  else = unsupported source). Do **not** broaden to nested-derived/lateral here. Let `lower_table_factor` keep
  owning table-factor detail checks such as table args, hints, versions, samples, and ordinality.

## D2 — lowering (reuse) + guard reconcile
- **No new join code.** `lower_derived_table` → `lower_select` already folds the comma list to a left-deep `Cross`
  chain; `recognize_cross_equi_joins` keys it. Verify the body lowers + the per-entry aliases resolve (q07's
  `nation n1`/`n2` are the next-wall probe — see below; the *focused* test avoids the self-join).
- **Architecture guard:** grep for any guard pinning `"exactly one FROM source"` (e.g. an ADR-NNN reject-string
  guard); retarget it to the new rule, don't delete (run the full `architecture_test`).

## Phase 3 — verify-first re-measure q07 (do NOT fix the self-join here)
After D1/D2, run:
```
target/debug/toron-tpch run --query q07 --mode dev   --sf 0.01 --allow-blocked
target/debug/toron-tpch run --query q07 --mode strict --sf 0.01 --allow-blocked
```
Record the **new** measured row verbatim:
- a self-join wall (`nation n1, nation n2`) → names **ADR 295**; update the q07 harness test + results to the new
  precise blocked reason (add a precise classifier only if the message is specific; otherwise `fail(unrecognized)`,
  no broad allowlisting);
- a further wall → record it precisely;
- `pass@execute` (keyed at sf=0.01) → q07 greens, reach → 12; flip the q07 harness test to pass + assert keyed.
**Do not implement self-join alias handling in this ADR.**

## Tests
- **Engine** (`crates/toron/tests/` — a derived-table test home): a derived **multi-FROM-entry comma-join body, no
  self-join**, with an equality predicate, binds + executes correct rows
  (`SELECT … FROM (SELECT … FROM supplier, customer WHERE s_nationkey = c_nationkey) AS d`). Discriminating:
  revert the validator relaxation → it rejects at `validate_derived_select_shape` (the pre-cut behavior).
- **Regression fences:** nested-derived body still rejects (`nested derived tables are not supported`); a
  nested/lateral derived factor in a later comma source or explicit join factor still rejects; **ADR 254
  single-FROM-entry join-bodied derived tests unchanged**.
- **Gauge** (`toron-tpch`): q07 row updated to the verify-first measured outcome (new wall / green).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`) — FULL toron (shared planner cut)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```
Plus empirical: the two live q07 runs (Phase 3) → the recorded measured row.

## Results doc
`crates/toron/plans/impl/0294_derived_table_multi_source_comma_join_results.md`: the validator relaxation; the
reused `lower_select`/`recognize_cross_equi_joins` path; the focused engine test; q07's new measured row (the
ADR 295 self-join wall, a further wall, or green); confirmation existing derived/CTE/reach/correctness rows
unchanged.

## Risk / blast radius
- **`validate_derived_select_shape` is shared** by every derived table / subquery path — the relaxation must keep
  single-FROM-entry + ADR 254 join-bodied + CTE behavior intact. Full `-p toron --no-fail-fast` is the net.
- **Keyed, not cartesian** — confirm the body's comma-join keys via `recognize_cross_equi_joins` at sf=0.01 (q07
  re-measure), not a tiny-scale cartesian.
- **No scope creep into the self-join** — ADR 294 is the comma-list FROM body only; the self-join is ADR 295.

## Seal
`impl: adr 294 derived table comma list from body`. Do not commit wip drafts.
