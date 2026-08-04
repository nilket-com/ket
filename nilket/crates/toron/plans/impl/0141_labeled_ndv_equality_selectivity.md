# Impl Plan — ADR 141: Labeled NDV Equality Selectivity Estimates

**Migration for** `plans/adr/0141_labeled_ndv_equality_selectivity.md`.
**Scope:** `catalog.rs` (`DistinctCountFact` + `column_ndv` on `TableOptimizerFacts` /
`DescriptorOptimizerFacts`) · `ir/execution.rs` (projection-mapped NDV on `ExecutionPlan::Scan`) ·
`binder.rs` (`scan_plan` populates Scan NDV; `estimated_row_count` on `PlanOptimizerFacts`;
equality-selectivity recognizer; `derive_estimated_byte_size` consumes it) · tests/guards.
**Non-scope:** SQL join-filter pushdown (the SQL headline — follow-on); hard-from-exact; quantile
facts; multi-column/join selectivity; 141-W (Wendelon producer — named); chooser change.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| catalog fact home | `TableOptimizerFacts { byte_upper_bound, row_upper_bound }` `catalog.rs:79` (derives `Copy,Eq` today); `DescriptorOptimizerFacts` `:85`; `for_descriptor` `:94` |
| Scan carries plan metadata (137) | `ExecutionPlan::Scan { local_table_id, table, .., primary_key_columns: Vec<ColumnOrdinal> }` `ir/execution.rs` |
| scan builder (projection-map NDV like PK) | `scan_plan(binding, projection)` `binder.rs:3619` |
| 137 recognizer to generalize | `filter_includes_primary_key_row_cap` `binder.rs:3706` (reads `Scan.primary_key_columns`); Eq + top-level-AND walk `:3719` |
| facts type (keep Copy) | `PlanOptimizerFacts { origin, byte_upper_bound, estimated_byte_size, row_upper_bound }` `binder.rs:165` |
| 138 estimate derivation | `derive_estimated_byte_size` / `estimate_materialized_byte_size` (uses `row_upper_bound`) |
| filter selectivity site | `LogicalPlan::Filter` arm; bound predicate available before pushdown; 137 cap site `binder.rs:542-556` |
| SQL out (where-after-join) | `lower_select` wraps `WHERE` `Filter` over the lowered join `sql_planner.rs:3693` |

## Phases
0. **Failing gates first:** `rune_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate` +
   `direct_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate` **fail on `main`** (no NDV →
   full-row estimate → hash). `missing_ndv_keeps_137_138_behavior` green before + after.
1. **`DistinctCountFact` + catalog fact (`catalog.rs`):** `enum DistinctCountFact { Exact(u64),
   Approx { estimate: u64, rel_err_ppm: u32 } }` — **integer error so the fact derives `Eq`/`Hash`**
   (Codex: an `f64` `std_err` would force `PartialEq`-only). Add a serde-defaulted descriptor
   entry list, e.g. `column_ndv: Vec<DescriptorColumnNdvFact>`, to `DescriptorOptimizerFacts`; add
   `column_ndv: BTreeMap<ColumnOrdinal, DistinctCountFact>` to `TableOptimizerFacts`. `for_descriptor`
   validates descriptor identity, filters out-of-range ordinals, resolves duplicates
   deterministically, and normalizes the descriptor list into the table map. These catalog structs
   become `Clone` (the map/list loses `Copy`; keep `Eq` via the integer error). Existing descriptor
   bootstrap JSON without `column_ndv` must continue to load. **`cargo check -p toron --all-targets`**
   (Copy→Clone churn — [[feedback_verify_all_targets_on_new_ir_field]]).
2. **Carry NDV on the Scan (`ir/execution.rs` + `binder.rs`):** add `column_ndv: Vec<(ColumnOrdinal,
   DistinctCountFact)>` to `ExecutionPlan::Scan`; `scan_plan` populates it from `binding…column_ndv`,
   **projection-mapped to the same ordinal space as `primary_key_columns`** (reuse the PK projection
   mapping). Bind-time-only — the executor ignores it (guard).
3. **`estimated_row_count` channel (`binder.rs`):** add `estimated_row_count: Option<RowCount>` to
   `PlanOptimizerFacts` (stays `Copy`); thread `None` through constructors + carry through 137's
   `preserve_volume_non_increasing`/`preserve_through_project` **without** touching `row_upper_bound`.
4. **Equality-selectivity recognizer (`binder.rs`):** `filter_equality_ndv(&predicate, &input.plan)
   -> Option<DistinctCountFact>` mirroring `filter_includes_primary_key_row_cap`: match
   `Scan.column_ndv`; walk for `col = <non-null literal>` (reuse the Eq + top-level-`AND` walk); on
   `AND`, return the **largest-NDV** factor (most selective); **no product**. In the Filter arm: with
   `Some(fact)` and `base_rows = input.facts.row_upper_bound` → `ndv = fact.point_estimate()`;
   `new = if base_rows == 0 { 0 } else { max(1, ceil(base_rows / max(1, ndv))) }`; `estimated_row_count =
   min(existing_estimated_row_count?, new)` (stacked filters take min — never re-multiply). Write
   **only** `estimated_row_count`. Treating `ndv == 0` as `max(1, ndv)` gives no reduction rather
   than claiming hard absence.
5. **Feed 138 (`binder.rs`):** `derive_estimated_byte_size` uses `min(row_upper_bound,
   estimated_row_count)` when both present, else `estimated_row_count.or(row_upper_bound)`; hard
   bounds + chooser unchanged.
6. **Guards + behavior tests** (the `adr141_*` set + gate list); re-run 129–140 (esp. 138 channel
   honesty + 139/140 replay fences).
7. **Verify:** `cargo test -p toron` · `--test architecture_test -- adr141` (+ adr129..140) ·
   `cargo check -p toron --all-targets` · `git diff --check`.

## Selectivity spec (the written formula)
```
DistinctCountFact { Exact(u64) | Approx { estimate, rel_err_ppm } }   // catalog → Scan (projection-mapped); derives Eq
point_estimate(): Exact(n) => n ; Approx { estimate, .. } => estimate     // rel_err_ppm carried, unused this cut
filter `col = lit` over single-base Scan, base_rows = row_upper_bound, ndv = point_estimate(NDV(col)):
  new = if base_rows == 0 { 0 } else { max(1, ceil(base_rows / max(1, ndv))) }
  estimated_row_count = min(existing_estimated_row_count?, new)          // estimate channel only; stacked → min
top-level AND: pick the single largest-NDV equality (smallest estimate) — never multiply factors
138: estimate row source = min(row_upper_bound, estimated_row_count); hard bounds untouched
```

## Risk register
- **NDV-derived rows leaking into a hard bound** (cardinal sin): selectivity writes only
  `estimated_row_count`; `adr141_selectivity_writes_estimated_row_count_only` +
  `adr141_exact_ndv_does_not_write_hard_bound_this_cut`. Reverting the field leaves 137/138/139/140
  green and only drops *estimated* broadcasts.
- **Independence product / stacked-filter multiply:** single most-selective factor; stacked → `min`;
  `adr141_and_filters_do_not_multiply_ndv_selectivities`.
- **Range/IN/OR borrowing NDV:** recognizer matches `Eq` + non-null literal only;
  `range_filter_does_not_use_ndv_selectivity`.
- **`PlanOptimizerFacts` Copy lost** (Codex): the NDV *map* lives on the catalog fact / Scan, not the
  hot struct (which gains only `estimated_row_count: Option<RowCount>`, Copy).
- **`Eq` lost to `f64`** (Codex): `rel_err_ppm: u32` keeps `DistinctCountFact`/catalog facts `Eq`.
- **NDV on the executed plan/wire:** rides `Scan` (like `primary_key_columns`) but bind-time-consumed
  only; `adr141_scan_ndv_is_bind_time_only_not_executed`.
- **Catalog struct churn (Copy→Clone):** `--all-targets` in phase 1.
- **Inert without producer:** `missing_ndv_keeps_137_138_behavior` (141-T safe before 141-W).

## Acceptance
A Rune + direct-IR equality-filtered dim with an NDV fact infers broadcast via a labeled *estimated*
receipt (fails on `main`); NDV writes only the estimate channel (even for `Exact`); `AND` uses a
single most-selective factor and stacked filters take `min`; ranges/`IN`/`OR` get no NDV; missing
NDV is inert; the `Scan` NDV is bind-time-only; `PlanOptimizerFacts` stays `Copy`; the NDV fact
derives `Eq`; 138 split + 139/140 fences green; `adr141_*` + 129–140 guards green.
