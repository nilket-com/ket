# Impl Plan — ADR 145: SQL Derived Tables in Joins (+ Projected Build-Side Byte Estimation)

**Migration for** `plans/adr/145_sql_derived_tables_in_joins.md`.
**Two coupled halves:** D1 frontend reject-lift (`sql_planner.rs`) + D2 binder
byte re-derivation (`binder.rs`). D2 is required — D1 alone is inert for the
headline. Anchors verified against `main` (post-143/144).

## Surfaces
| Concern | Anchor |
|---|---|
| Derived-in-join rejects (lift, gate all-inner) | `sql_planner.rs:2778` (leftmost), `:2792` (right input) |
| all-inner classifier (already computed) | `sql_planner.rs:2782` `all_inner_join_chain` |
| Derived lowering + fences (reuse) | `lower_derived_table:3624`, `validate_derived_select_shape`, `alias_plan`, `table_factor_is_derived:5994` |
| Alias resolution / fact preservation | `plan_qualifiers` through `Alias` (`:6286`); `Alias` bind preserves facts (`binder.rs:560`) |
| **D2 helper** | `preserve_through_project:235` (sole call `:632`, Project arm `:617`); `derive_estimated_byte_size:291` (schema-width, `min`, `SingleBaseRelation`-gated) |
| Filter byte derivation (full schema) | `binder.rs:614` |
| 144 range estimate (inner filter, reads scan facts) | `filter_range_selectivity_estimated_row_count` |
| Guards to retarget | `adr143_sql_join_filter_pushdown_is_planner_lowering_only`; `adr137_project_preserves_byte_bound_only_for_column_subset`; `adr138_estimate_and_derivation_live_in_one_helper` |

## Phase 0 — failing gates first
- `pure_subset_project_shrinks_estimated_byte_size` — **pure-binder** unit
  (`Project(subset, Filter(col range, Scan{facts}))` byte estimate < unprojected).
  Fails on `main`; pins D2 / the latent fix, SQL-independent.
- Headline `sql_join_projected_derived_dim_broadcasts_where_full_column_hashes`
  (binder test, the D3 fixture). Fails on `main`.

## Phase 1 — D2 first (D1 is inert without it)
Project arm (`:617`): compute `output_schema` before facts and thread it in:
```rust
let output_schema = schema_for_exprs_with_output_names(&exprs, output_names);
let facts = input.facts.preserve_through_project(&exprs, &output_schema);
```
Change `preserve_through_project` per ADR D2 (pure-subset → `preserve_volume_non_increasing()
.derive_estimated_byte_size(output_schema)`; else today's `byte/estimated_byte = None`,
row-local kept). Monotonic shrink only (`min` + `SingleBaseRelation` gate); never
touches `byte_upper_bound`.

## Phase 2 — D1 frontend reject-lift (gate all-inner)
Move `all_inner_join_chain` above the leftmost-derived check; both rejects become
`&& !all_inner_join_chain` (else admit). When admitted,
`lower_table_factor(&derived)` returns the existing `Alias`-wrapped derived plan
and the existing join build + 143 pushdown proceed unchanged (derived alias is a
single qualifier). No new IR.

## Phase 3 — tests
Binder behavior (144 fixtures per D3): headline + the honesty twin (full-column 143
form **hashes** under identical facts); `..._{right,left}_derived_range...broadcasts`;
`..._alias_column_list_resolves_join_keys`; `sql_broadcast_hint_targets_derived_alias`;
rejection fences (`..._outer_join_around_derived...`, `..._join_inside_derived...`,
`..._nested_lateral_sampled_derived...`); `..._digest_matches_hand_authored_logical_plan`.
Pure-binder: `pure_subset_project_shrinks_estimated_byte_size`,
`expression_project_keeps_byte_estimate_none`.
Architecture: add the seven `adr145_*` guards; retarget the three guards (D6).

## Phase 4 — verification
```sh
cargo test -p toron
cargo test -p toron --test architecture_test adr145
cargo test -p toron --test architecture_test adr143
cargo test -p toron --test architecture_test adr137
cargo test -p toron --test architecture_test adr138
cargo check -p toron --all-targets
git diff --check
```
129–144 regressions expected green. **Watch (D2 reopens 137/138):** a pure-subset
project that previously carried a full-width byte estimate now carries the narrower
one — if any landed 137/138 test asserted the old (inflated) estimate or a
project-then-broadcast negative, reconcile it explicitly (intended; it's the latent
fix), do not paper over it.

## Risk register
- **Inert-without-D2** — the iter-2 finding; phase D2 first + the honesty gate
  (headline fails if D2 reverted) prevent shipping D1 alone.
- **D2 reopens 137 code** ([[feedback_small_cut_bias_structure_ossification]] — the
  byte behavior lives there; reopen it). `min` + `SingleBaseRelation` gate keep it
  monotonic-shrink; the pure-binder unit + 137/138 reruns are the guard.
- **Expression-project over-shrink** — gated to `project_roots_are_distinct_columns`;
  `expression_project_keeps_byte_estimate_none` pins the fence.
- **Outer-derived / through-alias** — out of scope; rejection-fence tests pin it.
