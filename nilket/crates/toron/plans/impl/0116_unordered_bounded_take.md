# Impl Plan — ADR 116: Unordered Bounded Take

**Migration for** `plans/adr/0116_unordered_bounded_take.md`. **Scope (Toron):** `ir.rs` + `ir/execution.rs`
(`Take` IR + arena sweep), `sql_planner.rs` (bare-`LIMIT`→`Take`, OFFSET reject), `exec.rs` + `coordinator.rs`
(executor early-stop, coordinator-local), `storage.rs` (`ScanRequest.row_limit` + safe pushdown),
`distributed/fragment.rs` (single-source-first / fallback ladder), `rune_engine.rs` (`.take`). **Scope
(cross-ring B):** `wendelon_client` `TableQueryOptions.row_limit` + fixed `TableQueryRequestMeta` field; Wendelon
server decode + `handle_table_query` post-PBC early-return. **Non-scope:** ordered `LIMIT` (115); unordered
`OFFSET`; `take_available`; multi-barrier; true Wendelon→Toron streaming.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Bare-LIMIT reject (lift) | `sql_planner.rs:2481` (in `apply_order_by_limit_tail`) |
| No Take IR today | add `LogicalPlan::Take` (`ir.rs`), `ExecutionPlan::Take` (`ir/execution.rs`) |
| Scan param (cross-ring) | `ScanRequest` (`storage.rs:48`); Wendelon scan `storage.rs:853` (`collect_all`→`from_batches`) |
| Stream consume (early-stop) | `materialize_scan_stream` (`coordinator.rs:49`); ScanStream-collection guard pins to `coordinator.rs` |
| Cancellation | `DownstreamCancellationContext` (059/060) |
| Fragmenter | `fragment_plan` (`fragment.rs:87`); `global_sort_barrier_count` (115 — `Take` must not count) |
| PBC fully-pushed/residual oracle | `partition_bound_predicate` / `is_pbc_pushdown_eligible` |
| Wendelon client/meta/server | `wendelon_client::TableQueryOptions`, `TableQueryRequestMeta` (`_pad[0]` = stats-pruning), `handle_table_query` |

## Phase 1 — `Take` IR (`ir.rs`, `ir/execution.rs`)

`LogicalPlan::Take { input, limit: usize }` + `ExecutionPlan::Take { input, limit, output_schema }` (schema =
input schema). Arena/walker arms (108 `Case`-variant sweep, compiler-enforced); `global_sort_barrier_count`
recurses through `Take` returning child count (**not** a barrier).

## Phase 2 — SQL lowering (`sql_planner.rs`)

In `apply_order_by_limit_tail`: `order_by.is_none()` + `LIMIT n` present → `LogicalPlan::Take { input: plan,
limit: n }` instead of the `:2481` reject. Unordered `OFFSET` (with no `ORDER BY`) still rejects. `ORDER BY …
LIMIT` unchanged (115 `Sort`). `LIMIT 0` → `Take { limit: 0 }`.

## Phase 3 — Executor early-stop (`exec.rs` + `coordinator.rs`) — correctness floor

`ExecutionPlan::Take`: emit until `limit`, **slice the final batch**, `limit == 0` → empty batch + schema (no
child pull), then drop/cancel upstream (059/060). The bounded ScanStream consumption is a `take_n_from_stream`
helper **in `coordinator.rs`** (the collection guard); executor `Take` over non-scan inputs early-stops at the
batch level. Correct for all backends regardless of storage support.

## Phase 4 — Storage pushdown (`storage.rs`) + safety — the cheap path

`ScanRequest.row_limit: Option<usize>`. Set it (planner/fragmenter) **only** when `row_limit` bounds rows after
all storage-visible filtering/projection (post-PBC = post-storage): `Take(Scan)`, `Take(Project(Scan))`,
`Take(Filter(fully-pushed-PBC, Scan))`. **Do not set** for `Take(Filter(residual, Scan))` (executor handles) or
row-identity append scans (V1 carve-out). Oracle = the existing PBC fully-pushed/residual split. **`LIMIT 0`/wire
trap:** short-circuit `LIMIT 0` before issuing a Wendelon request; `Some(0)` at a local backend → zero rows; never
encode `0` as a Wendelon row-limit (`0` = absent).

## Phase 5 — Cross-ring B (`wendelon_client` + Wendelon server)

- `wendelon_client::TableQueryOptions.row_limit: Option<NonZeroU64>` (or equivalent nonzero option, default absent);
  plumb from `ScanRequest.row_limit`.
- **Widen the fixed `TableQueryRequestMeta`** with an explicit `row_limit_rows: u64` field (`0` = absent), **not**
  in `_pad`/`_trace_pad`; update the fixed-meta size/guards intentionally. Server decode + validate.
- `handle_table_query`: after applying PBC predicate/projection (+ runtime filter), **count post-PBC output rows,
  slice the final overlarge batch, stop scanning/planning at the bound, set `QueryResponseMeta.rows` to the
  bounded count, return `OK`** (not `QUERY_BUDGET_EXCEEDED`). **Do not** lower `QueryExecutionBudget.max_rows`.
  **Secondary-index** plans honor the same post-PBC bound, or bypass to the bounded full-scan path (must not
  ignore it).

## Phase 6 — Fragmenter (`distributed/fragment.rs`)

`Take` over a safe scan shape → best: **single bounded scan request** (`row_limit = n`), no exchange/gather/sort;
if it underfills (`< n`, unexhausted) continue to more sources. Acceptable V1 fallback: existing distributed
fanout + per-source `row_limit` + a coordinator `Take`. **Never** return `< n` while rows exist (D3). `Take` over
a barrier (aggregate/sort/join/residual) → after the barrier.

## Phase 7 — Rune `.take` (`rune_engine.rs`)

`RuneDataFrame::take(i64) -> VmResult<RuneDataFrame>` (reject negative) → `LogicalPlan::Take`. Register
`m.associated_function("take", …)`. Leave `.limit` untouched (115 sort-attached). No unordered `.offset`.

## Phase 8 — Tests + guards

All ADR §Behavior gates — **Toron** (incl. `fully_pushed_filter_limit_can_push_to_storage`,
`residual_filter_limit_not_pushed_to_storage`, `bare_limit_zero_returns_empty_with_schema`), **Wendelon**
(`table_query_row_limit_bounds_post_pbc_rows`, `…_slices_final_batch`, `…_is_not_budget_exceeded`,
`…_zero_absent_on_wire`, `…_not_hidden_in_pad`, `secondary_index_plan_honors_or_falls_back_on_row_limit`),
**distributed** (`…_single_bounded_scan_when_source_satisfies_limit`, `…_does_not_return_too_few_when_first_source_underfills`).
Guards: the full `adr116_*` set (ADR §Architecture guards), cashing in 115's `adr115_unordered_limit_is_future_take_not_sort`.
Subset/cardinality assertions, never row identity.

## Phase 9 — Verification

```sh
cargo fmt
cargo test -p toron --test architecture_test adr116
cargo test -p toron --test architecture_test adr115     # ordered top-k unaffected
cargo test -p toron --test sort_ordering_test
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron
cargo test -p wendelon    # table-query row_limit honoring + fixed-meta guard
git diff --check
```

## Acceptance

Bare `LIMIT n` + Rune `.take(n)` lower to a distinct `Take` (not `Sort`), returning `min(n, cardinality)` rows,
no order/identity promise; executor early-stop is the coordinator-local correctness floor (`LIMIT 0` free);
`ScanRequest.row_limit` set only on safe post-PBC shapes; **Wendelon honors an explicit table-query `row_limit`
(fixed-meta field, not `_pad`), stopping after `n` post-PBC rows and returning `OK` not budget-exceedance**;
secondary-index honors or falls back; ordered `ORDER BY … LIMIT` (115) unaffected; unordered `OFFSET` rejected;
distributed preserves `min(limit, cardinality)`; `adr116_*` + cashed-115 guards hold; Toron + Wendelon + distributed
suites green.
