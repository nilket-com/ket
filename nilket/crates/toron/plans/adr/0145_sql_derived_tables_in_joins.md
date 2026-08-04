# ADR 145: SQL Derived Tables in Joins (+ Projected Build-Side Byte Estimation)

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini).** SQL rejects a
derived table as a join factor (`sql_planner.rs:2778, 2792`), so a user cannot
author the **projected** filtered-dimension shape Rune builds directly —
`Join(fact, Alias(Project(Filter(Scan(dim)))))` — that prunes the build side to
the join's needed columns. ADR 145 lifts that reject **and** makes the projection
actually shrink the build-side byte estimate, so a wide dimension that hashes as a
full-column 143 filter **broadcasts** when projected to a few columns.

**The cut has two coupled halves** (the load-bearing correction this council made:
the column-pruning win is *not* free from the frontend reject-lift):
- **D1 (frontend):** admit v1 derived tables as join factors in all-inner chains.
- **D2 (binder):** re-derive `estimated_byte_size` at a pure-subset `Project` from
  the projected schema — the mechanism that earns the headline.

The promotion headline **fails if either half is reverted**.

**Placement:** Toron-ring — SQL frontend lowering (D1) + binder estimate
refinement (D2). No Wendelon, executor, chooser, or storage change.
**Predecessors:** 143 (side-local pushdown + the all-inner fence reused here),
144 (the range estimate the derived inner filter spends), 137 (the
`preserve_through_project` helper D2 reopens), 138 (the estimate-channel byte model
D2 extends), [[feedback_continuous_refactoring_travels_with_adr]] (D2 is a latent
over-estimate fix that travels with this surface), [[feedback_cut_must_deliver_the_name]]
(the both-halves honesty gate).
**Migration:** `plans/impl/0145_sql_derived_tables_in_joins.md`.

## The verified byte path (why D2 is required, not optional)
Without D2 the headline is impossible — `Join(fact, Alias(Project(Filter(Scan))))`
carries the **same** byte estimate as the full-column 143 form:
- `Filter` derives `estimated_byte_size` from the filter plan schema — for
  `Filter(Scan(customers))` that is the **full** customers schema (`binder.rs:614`).
- `Project` calls `preserve_through_project`; for a pure distinct-column subset it
  returns facts **unchanged** (`binder.rs:235`) — the full-width estimate survives.
- The join chooser reads the stored `estimated_byte_size` (138 D3); it does **not**
  recompute build bytes from the projected schema.

So the projection narrows the schema *after* byte estimation was already spent on
the full schema. D2 moves the estimate to reflect the projected schema.

## Decision

**D1 — Admit derived join factors only in all-inner chains.** `lower_select`
admits a derived table on either side of a join **iff the whole FROM chain is
all-inner** (`join_operator_kind_for_filter_pushdown`, already computed at
`:2782`); else the reject stands. Admitted derived tables are exactly today's v1
derived tables (`validate_derived_select_shape`: alias required; plain `SELECT`;
one simple base source; no joins-inside / nesting / lateral / sample / window).
`lower_table_factor`/`lower_derived_table` already lower them to ordinary
`Alias`/`Project`/`Filter`/`ScanNamed` plan shapes; `plan_qualifiers` resolves the
alias. **No new logical-plan node.** Plain non-derived outer/semi/anti join
behavior is unchanged — ADR 145 only gates the *derived* surface.

**D2 — Re-derive estimated bytes at pure-subset projects.**
`PlanOptimizerFacts::preserve_through_project` takes the project `output_schema`;
for a pure distinct-column-root project it preserves row-local facts **and** calls
`derive_estimated_byte_size(output_schema)`:
```rust
fn preserve_through_project(self, exprs: &BoundExprProgram, output_schema: &Schema) -> Self {
    if project_roots_are_distinct_columns(exprs) {
        return self.preserve_volume_non_increasing()
                   .derive_estimated_byte_size(output_schema);
    }
    Self { origin: self.origin, byte_upper_bound: None, estimated_byte_size: None,
           estimated_row_count: self.estimated_row_count, row_upper_bound: self.row_upper_bound }
}
```
`derive_estimated_byte_size` already (a) runs only for `SingleBaseRelation`,
(b) needs a row source, (c) combines by `min(existing, new)`, (d) returns `None`
for unestimable schemas — so D2 is a **monotonic estimate shrink** for
column-dropping projects, **estimate channel only** (never shrinks or invents the
hard `byte_upper_bound`). Expression/duplicate/aggregate/composite projects keep
today's conservative behavior. **Latent-bug fix that travels with the ADR:** every
pure-subset project today (Rune `.select(subset)` included) carries an inflated
full-width byte estimate and can fail to broadcast when it should; D2 fixes it
generally.

**D3 — Headline requires both halves (honesty gate).**
`sql_join_projected_derived_dim_broadcasts_where_full_column_hashes`, seeded with:
a large fact-side byte bound (customers is the build candidate); customer
row/range facts so 144 estimates the **same** filtered row count in both SQL forms;
a **wide** customer schema where the full-column filtered build exceeds the
broadcast threshold; a projected derived schema (e.g. `customer_id, amount`) whose
re-derived bytes fall below it. The full-column 143 form
(`… JOIN customers c ON … WHERE c.amount BETWEEN 10 AND 19`) **hashes**; the
projected derived form **broadcasts**. **Fails on `main`; revert D1 → rejected;
revert D2 → projected build keeps full-width bytes → hashes.**

**D4 — Alias/hint semantics stay generic.** Join keys, projection refs, outer
`WHERE` refs, and broadcast hints target the derived alias; alias column lists ride
`alias_plan`; hints resolve via `plan_qualifiers` recursion through `Alias`. No
derived-specific hint parser, binder alias special case, or chooser special case.

**D5 — Preserve 143 pushdown; no through-alias fact lookup.** Side-local outer
`WHERE` pushdown stays all-inner. The headline works via **fact propagation**: the
selective predicate lives *inside* the derived table, so its inner `Filter` reads
scan facts directly (144 range estimate), and D2 carries the reduced
estimate through the projected byte re-estimate up through `Alias`
(`binder.rs:560`). `column_range_for_input`/`column_ndv_for_input` intentionally do
**not** see through `Alias`/`Project`; an *outer* `WHERE` pushed onto a derived
alias is correct but unestimated (the inner filter already did the work). No new
through-alias lookup.

**D6 — Reconcile the guards D2 reopens (discipline #4).**
- Retarget `adr143_sql_join_filter_pushdown_is_planner_lowering_only` → no longer
  assert a blanket derived-join reject; still own the all-inner pushdown fence.
- `adr137_project_preserves_byte_bound_only_for_column_subset` → 137 still proves
  pure-subset preserves row-local facts and treats the **hard** `byte_upper_bound`
  conservatively; ADR 145 now owns the **estimated**-byte re-derivation.
- `adr138_estimate_and_derivation_live_in_one_helper` → 138 still proves Toron
  writes only `estimated_byte_size`, never `byte_upper_bound`.

## Acceptance gates
`sql_join_projected_derived_dim_broadcasts_where_full_column_hashes` (headline);
`pure_subset_project_shrinks_estimated_byte_size` (**pure-binder** unit — pins D2 /
the latent fix, SQL-independent); `expression_project_keeps_byte_estimate_none`
(D2 fence); `sql_join_{right,left}_derived_range_filtered_dim_broadcasts_by_estimate`;
`sql_join_derived_alias_column_list_resolves_join_keys`;
`sql_broadcast_hint_targets_derived_alias`;
`sql_outer_join_around_derived_table_stays_rejected`;
`sql_join_inside_derived_table_stays_rejected`;
`sql_nested_lateral_sampled_derived_join_fences_hold`;
`sql_derived_join_digest_matches_hand_authored_logical_plan`.

## Architecture guards
`adr145_derived_tables_allowed_in_inner_join_chains`,
`adr145_derived_join_rejects_non_inner_chains`,
`adr145_derived_join_keeps_v1_derived_fences`,
`adr145_pure_subset_project_rederives_projected_byte_estimate`,
`adr145_expression_projects_do_not_shrink_estimated_bytes`,
`adr145_broadcast_hints_resolve_derived_aliases`,
`adr145_no_wendelon_executor_storage_or_chooser_change`; retargets per D6.

## Non-goals (named follow-ons)
Derived factors in outer/semi/anti/full/cross/apply/as-of/array chains;
joins/nesting/lateral/sample inside derived tables; expression-project byte shrink;
hard `byte_upper_bound` from projects; through-`Alias`/`Project` NDV/range lookup
for outer pushed predicates; CTEs/set-ops/decorrelation; join reordering. Carried
arc: hard zero-row folds (complete-membership / out-of-range); 136-W hard
table-size producer; KLL/quantile range precision; Theta-sketch join-key
intersection → join selectivity → reordering.

## Consensus trail
D1 (frontend reject-lift, all-inner) was 3-of-3 from iter-1. **The load-bearing
correction is D2:** iter-1 (all three drafts, Claude included) asserted the
column-pruning win falls out of the frontend reject-lift; Claude's iter-2 checked
the byte path and showed it does **not** — the pure-subset `Project` preserves the
full-width estimate (`binder.rs:235`; byte derived at the Filter on the full
schema), so D2 (a small, general, latent-bug-fixing binder change) is required to
earn the headline. Codex iter-2 adopted D2 and the merged final plan carries the
both-halves honesty gate. No remaining divergence.
