# Impl Plan — ADR 148: IN-List Equality Selectivity from Existing NDV Facts

**Migration for** `plans/adr/148_in_list_equality_selectivity_from_ndv.md`.
**Toron-only consumer cut.** Wendelon already produces the NDV facts and hard row
count this uses; no Wendelon schema, producer, client, or wire changes.

## Surfaces

| Concern | Anchor |
|---|---|
| IN-list IR shape | `ir.rs` `Expr::InList` |
| Bound IN-list expression | `binder.rs` `BoundExprNode::InList`, `append_in_list` callers |
| Existing equality NDV hook | `binder.rs` `filter_equality_ndv_estimated_row_count` |
| Recursive equality helper | `binder.rs` `predicate_node_equality_ndv_estimated_row_count` |
| Single equality estimator | `binder.rs` `equality_ndv_estimated_row_count` |
| NDV lookup | `binder.rs` `column_ndv_for_input` |
| Row estimate helper | `binder.rs` `estimated_rows_for_ndv` |
| Existing null literal predicate | `binder.rs` `literal_is_non_null` |
| Filter bind integration | `LogicalPlan::Filter` arm combines NDV/range estimates via `min_optional_row_count` |
| Byte estimate spend | `PlanOptimizerFacts::derive_estimated_byte_size` |
| Broadcast reason path | `build_byte_candidate` chooses `Estimated` when only estimated bytes are present |

## Phase 0 — failing gates first

Add Toron binder tests before implementation:

- `in_list_filtered_dim_with_wendelon_ndv_infers_broadcast_by_estimate`
- `in_list_ndv_estimates_distinct_non_null_literals`
- `in_list_ndv_deduplicates_duplicate_literals`
- `in_list_ndv_ignores_null_literals`
- `in_list_ndv_all_null_list_fails_open`
- `in_list_non_finite_float_literal_fails_open`
- `in_list_k_exceeds_ndv_caps_at_base_rows`
- `not_in_does_not_use_ndv_selectivity`
- `in_list_without_ndv_or_row_bound_fails_open`
- `in_list_and_equality_keep_tightest_estimate`
- `in_list_does_not_write_hard_bounds_or_empty_relation`
- `equality_ndv_existing_behavior_unchanged`

Add architecture guard:

- `adr148_in_list_ndv_selectivity_is_consumer_only_and_estimate_channel`

## Phase 1 — IN-list selectivity helper

1. Extend `predicate_node_equality_ndv_estimated_row_count`:
   - existing `Eq` arm unchanged;
   - existing `And` arm unchanged (`min` combination);
   - add `BoundExprNode::InList { expr, list, negated: false }` arm;
   - `negated: true` returns `None`.

2. Add:

```rust
fn in_list_ndv_estimated_row_count(
    program: &BoundExprProgram,
    expr: BoundExprId,
    list: &[BoundExprId],
    input: &ExecutionPlan,
    base_rows: RowCount,
) -> Option<RowCount>
```

Rules:

- `expr` must be `BoundExprNode::Column`.
- The column must have an NDV fact via `column_ndv_for_input`.
- Every non-null list item must be a `BoundExprNode::Literal`.
- Unsupported literal shapes fail open for the whole IN-list estimate.
- Null literals are ignored.
- If no distinct non-null literal remains, return `None`.

3. Use the existing equality math:

```rust
let rows_per_value = estimated_rows_for_ndv(base_rows, distinct_count.point_estimate());
let literal_count = u64::try_from(keys.len()).unwrap_or(u64::MAX);
let estimate = RowCount::new(
    base_rows
        .get()
        .min(rows_per_value.get().saturating_mul(literal_count)),
);
```

This preserves `base_rows == 0 -> 0` and avoids multiplication overflow.

## Phase 2 — typed literal cardinality keys

Add a local key type near the equality NDV helpers:

```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum InListLiteralKey {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(u32),
    Float64(u64),
    Utf8(String),
}
```

Add:

```rust
enum InListLiteralCardinality {
    Null,
    Key(InListLiteralKey),
}
```

and:

```rust
fn in_list_literal_cardinality(
    program: &BoundExprProgram,
    id: BoundExprId,
) -> Option<InListLiteralCardinality>
```

Rules:

- `ScalarValue::{Boolean,Int32,Int64,Utf8}(Some(_))` become keys.
- `ScalarValue::Float32(Some(v))` / `Float64(Some(v))` become keys only when
  finite; normalize `-0.0` to `+0.0`, then store `to_bits()`.
- `ScalarValue::*None` and `NullLiteral` become `Null`.
- Non-literal nodes, non-finite floats, or malformed literal indexes return `None`
  to fail open.

Use `BTreeSet<InListLiteralKey>` for deterministic distinct counting and no new
dependency.

## Phase 3 — keep channels and composition unchanged

No changes to the `LogicalPlan::Filter` fact flow beyond the new helper result:

- NDV and range estimates are still combined by `min_optional_row_count`.
- `with_estimated_row_count` is still the only write target.
- `row_upper_bound`, `byte_upper_bound`, and `EmptyRelation` are untouched.
- `derive_estimated_byte_size` spends the estimated row count as before.

Do not alter Wendelon facts, Wendelon client exports, descriptor structs, or the
storage facts mapper.

## Phase 4 — tests

### Binder tests

Use the existing join/catalog helpers that seed:

- a large fact table;
- a dimension table with `row_upper_bound`;
- a `DistinctCountFact` on the IN-list column;
- no hard byte upper bound on the filtered dimension, so the broadcast reason is
  `BroadcastEstimated`.

Test expected behaviors:

- headline filtered dimension broadcasts by estimate;
- duplicate literals count once;
- null literals do not increase `k`;
- all-null list returns no NDV estimate and keeps default behavior;
- a non-finite float literal (`NaN`/`±inf`) fails open the whole estimate (never
  skipped — skipping `+inf` would under-count `k`);
- more distinct literals than NDV caps the estimate at `base_rows`;
- `NOT IN` returns no NDV estimate;
- missing NDV or missing `row_upper_bound` fails open;
- `col = literal AND col2 IN (...)` keeps the tighter estimate;
- hard fact channels remain absent.

### Architecture guard

Add a source-level guard in `crates/toron/tests/architecture/optimizer.rs`:

- `predicate_node_equality_ndv_estimated_row_count` contains `BoundExprNode::InList`;
- the helper checks `negated`;
- `filter_equality_ndv_estimated_row_count` still checks
  `PlanOrigin::SingleBaseRelation`;
- the helper still requires `facts.row_upper_bound`;
- `InListLiteralKey` exists and has `Float32(u32)` / `Float64(u64)` key forms;
- the IN-list helper calls `estimated_rows_for_ndv`;
- the IN-list helper does not mention `byte_upper_bound`, `row_upper_bound: Some`,
  `EmptyRelation`, KLL, CountMin, Theta, tensor, or any Wendelon producer path.

Retain the existing ADR 141 equality guard and ADR 146 row-bound guards.

## Phase 5 — verification

Use the cgroup wrapper for all Rust commands:

```sh
scripts/cargo-cgroup fmt --check
scripts/cargo-cgroup test -p toron in_list_ -- --nocapture
scripts/cargo-cgroup test -p toron equality_ndv_existing_behavior_unchanged -- --nocapture
scripts/cargo-cgroup test -p toron --test architecture_test adr148 -- --nocapture
scripts/cargo-cgroup test -p toron --test architecture_test adr141 -- --nocapture
scripts/cargo-cgroup test -p toron --test architecture_test adr146 -- --nocapture
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron
git diff --check
```

No Wendelon package test is required for ADR 148 unless implementation accidentally
touches Wendelon files. If it does, run the matching Wendelon checks through
`scripts/cargo-cgroup`.

## Risk register

- **Literal duplicate under-count.** Use canonical typed keys; finite floats only;
  normalize `-0.0`; fail open on unsupported/non-finite literals.
- **NULL semantics.** Null list items add no true matches, but all-null lists must
  fail open rather than becoming hard-zero rewrites.
- **`NOT IN` anti-selectivity.** Explicitly out of scope; fail open.
- **Hard-channel leak.** The helper must only produce `estimated_row_count`.
- **Correlation/independence creep.** Keep existing `AND` combination as `min`, not
  product.
- **Producer creep.** No Wendelon schema, stats, or wire changes.

