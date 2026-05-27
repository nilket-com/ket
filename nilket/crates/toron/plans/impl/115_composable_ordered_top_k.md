# Impl Plan — ADR 115: Composable Ordered Top-K

**Migration for** `plans/adr/115_composable_ordered_top_k.md`. **Fork A:** one global ordered top-k barrier, a
named fragmenter primitive with a V1 cap. **Scope:** `sql_planner.rs` (query-tail helper + derived fence lift),
`rune_engine.rs` (`RuneSortKey` + `.sort`/`.limit`/`.offset`), `distributed/fragment.rs` (named barrier
detect/split + V1 cap), tests + guards. **Non-scope:** unordered bare-`LIMIT`/take; multiple barriers; merge-sort;
top-k pushdown; new IR/executor kernel/PBC/storage; joins-around-derived; nested-derived/CTE/window/DISTINCT.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Top-level query-tail | `lower_query` (`sql_planner.rs:2420`); `lower_limit_clause`; `LIMIT`-without-`ORDER BY` reject (`:2456`) |
| Derived fences | `validate_derived_query_shape` (`:5742`, lift order/limit) ; `validate_derived_select_shape` (`:5762`, keep) ; `lower_derived_table` |
| Sort IR/exec (reuse) | `LogicalPlan::Sort` (`ir.rs:680`), `ExecutionPlan::Sort` (`ir/execution.rs:232`), `execute_sort` (`exec.rs:2715`) |
| Fragmenter | `fragment_plan` (`fragment.rs:87`): `count_exchanges==0`→1 stage, `==1`→producer+consumer, `>1`→`MultipleExchangeBoundaries`; `is_terminal_sort_over_grouped_aggregate` (`:699`) |
| Rune surface | `RuneDataFrame`, `create_toron_module` (`rune_engine.rs`); carrier pattern `RuneNamedExpr`/`RuneSortKey` |
| Scheduler (ready, untouched) | `StagePlan` + stage barriers (`distributed/scheduler.rs`) |

## Phase 1 — SQL query-tail helper (`sql_planner.rs`)

Extract from `lower_query` an `apply_order_by_limit_tail(plan, aggregate_output_catalog, order_by, limit_clause) ->
LogicalPlan` owning: `lower_limit_clause`, the `LIMIT/OFFSET`-without-`ORDER BY` reject (D4), `lower_order_by`,
hidden aggregate-`ORDER BY` projection, non-negative literal parsing, the single-partition gather sort
(`Sort { input: Exchange { partition_count: 1, key: [] }, … }`), and final helper-column stripping. `lower_query`
calls it after `lower_select`.

## Phase 2 — Derived SQL admission (`sql_planner.rs`)

`validate_derived_query_shape` (`:5742`): drop `query.order_by.is_some()` + `query_has_limit_or_offset(...)` from
the reject; keep `WITH`/`FETCH`/locks/`FOR`/settings/format/pipe/non-`SELECT` rejected. Keep
`validate_derived_select_shape` narrow. `lower_derived_table` runs `validate_derived_query_shape →
validate_derived_select_shape → lower_select → apply_order_by_limit_tail → alias_plan` — **no `lower_query`
delegation** (ADR 106 correction).

## Phase 3 — Rune surface (`rune_engine.rs`)

`RuneSortKey { expr: Expr, descending: bool, nulls_first: bool }` (registered via `m.ty`); `RuneExpr::asc()`/
`.desc()` + free `asc`/`desc`; `RuneSortKey::nulls_first()`/`nulls_last()`. `RuneDataFrame::sort(Vec<RuneSortKey>)
-> VmResult<RuneDataFrame>` rejects empty keys, emits `LogicalPlan::Sort { input: Exchange{partition_count:1,
key:[]}, order_by, limit: None, offset: 0 }`. `.limit(i64)`/`.offset(i64) -> VmResult` reject negatives and reject
unless the root is `LogicalPlan::Sort` (D7).

## Phase 4 — Fragmenter: named barrier primitive + V1 cap (`distributed/fragment.rs`)

- Add `FragmentError::MultipleGlobalSortBarriers` ("multiple global sort barriers are not supported in V1").
- Add the **named primitive**: `global_sort_barrier(&ExecutionPlan) -> Option<…>` + `global_sort_barrier_count`
  (barrier = `Sort` over an empty-key single-partition `Exchange`; do **not** count aggregate repartition
  exchanges). In `fragment_plan`, reject `global_sort_barrier_count > 1` with the pinned error.
- **Implement the one-sort path by reusing the detector/splitter — not a new exact-shape case.** Simple
  derived/Rune sort-over-scan preserves the generic one-exchange path; aggregate+sort reuses/generalizes the
  existing sort-over-aggregate path. The load-bearing discipline is that the V1 cap is over a reusable barrier
  primitive, not over raw `count_exchanges` and not over a third bespoke shape matcher.
- Generalize `is_terminal_sort_over_grouped_aggregate` just enough to allow **unary consumers above the single
  barrier** (`Filter(Project(Sort(Exchange(Aggregate(Exchange(scan))))))` + alias-projection shapes).
- Keep join fragmentation special-cased; joins-around-derived stay planner-fenced. **No** generic multi-barrier
  pipeline, merge-sort, or top-k pushdown.

## Phase 5 — Tests + guards

**SQL:** `sql_derived_top_k_executes`, `sql_derived_offset_executes`, `sql_derived_sort_alias_columns_apply_after_sort`,
`sql_aggregate_derived_top_k_executes`, `multiple_global_sorts_reject_loudly`; retarget
`derived_table_source_scope_rejects_general_subquery_features` (drop the ORDER BY/LIMIT negative, keep the rest).
**Rune:** `rune_dataframe_top_k_executes`, `rune_sql_relation_top_k_executes`, `rune_limit_without_sort_rejects`,
`rune_multiple_global_sorts_reject_loudly`, null-ordering fixture if cheap.
**Guards:** `adr115_derived_query_tail_fence_lifted_only`, `adr115_derived_lowering_does_not_delegate_to_lower_query`,
`adr115_sort_reuses_existing_ir_and_executor`, `adr115_rune_ordered_topk_surface_registered`,
`adr115_sort_builders_use_single_partition_exchange`, `adr115_global_sort_barrier_is_a_named_primitive` (cap over a
reusable primitive; simple shapes reuse `==1`, aggregate shapes reuse the aggregate-sort path; no raw
`count_exchanges` cap and no third bespoke shape), `adr115_multiple_global_sorts_reject_with_pin`,
`adr115_no_merge_sort_or_topk_pushdown`, `adr115_unordered_limit_is_future_take_not_sort`,
`adr115_rune_boundaries_are_authorial_not_sql_sugar` (D9).

## Phase 6 — Verification

```sh
cargo fmt
cargo test -p toron --test architecture_test adr115
cargo test -p toron --test architecture_test adr106
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test group_by_test
cargo test -p toron
git diff --check
```

## Acceptance

SQL-derived + Rune-authored ordered top-k reuse the existing `Sort` substrate (single-partition gather); one global
barrier may feed unary consumers and coexist with a grouped-aggregate shuffle; stacked global sorts reject with the
pinned V1 error in SQL and Rune; `LIMIT` requires `ORDER BY` everywhere; the ADR 106 narrow-derived path stays
intact (no `lower_query` delegation); hidden aggregate-`ORDER BY` helpers don't leak; the fragmenter has a **named
barrier primitive** (barrier-count cap, simple `==1` reuse, aggregate-sort reuse, not shape soup); Rune authored
barriers documented as explicit pipeline boundaries (D9); **no** merge-sort / top-k pushdown / new IR / executor kernel / PBC / storage;
targeted + Toron suite green.
