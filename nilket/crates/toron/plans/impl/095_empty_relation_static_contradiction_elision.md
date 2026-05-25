# Impl Plan — ADR 095: EmptyRelation + Static Contradiction Elision

**Migration for** `plans/adr/095_empty_relation_static_contradiction_elision.md`. **Toron-only.**
A new execution-IR variant (wide match-site sweep) + a conservative binder classifier + a filter-local
elision call. No SQL/logical-IR/PBC/Wendelon/public-wire surface.

## Pre-flight anchors (grounded — confirm against the tree before editing)

| Concern | Anchor |
|---|---|
| `ExecutionPlan` enum / `schema()` | `ir/execution.rs:177` / `:288` |
| Filter seam (classifier insertion) | `plan_filter_with_predicate_pushdown` `binder.rs:1084`; classify before `predicate_pushdown_plan` `:1090` |
| Closed-root evaluator | `Executor::evaluate_bound_program_without_storage(program, batch)` `exec.rs:1017` (reads columns by ordinal) |
| Node walk / column detect | `program.nodes` (crate-visible; `exec.rs:1027` iterates it); `BoundExprNode::Column(_)` |
| `IS NULL` node spelling | `BoundExprNode::Unary { op: BoundUnaryOp::IsNull, input }`; use `BoundExprProgram::node(input)` for the direct-column check |
| Filter NULL semantics | Arrow `filter_record_batch` `exec.rs:234`/`:301` — keeps `Some(true)` only |
| Scalar-agg empty input | `bound_global_aggregate` `exec.rs:1117`/`:1126` — always one row |
| Empty-batch idiom | `RecordBatch::new_empty(schema.clone())` (used in exchange/sort/join empty paths) |
| 1-row zero-column dummy | `RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &RecordBatchOptions::new().with_row_count(Some(1)))` |
| Task payload | `distributed/task.rs` `TaskSpec { plan: ExecutionPlan }` (serde-derived — internal variant) |

## Phase 1 — Add the variant

`ir/execution.rs:177`: add `EmptyRelation { schema: SchemaRef }` (near `Scan`). `schema()` arm:
`Self::EmptyRelation { schema } => schema.clone()`. Let serde derive the `TaskSpec` payload (internal
same-version variant, not public protocol).

## Phase 2 — Executor arm

`Executor::execute_bound` (`exec.rs:649`):
`ExecutionPlan::EmptyRelation { schema } => Ok(vec![RecordBatch::new_empty(schema.clone())])`. No
storage call. Return the empty *batch* (schema-bearing), never `Vec::new()` — downstream operators
(esp. scalar aggregate) must see an empty input *relation*, not a missing input.

## Phase 3 — Exhaustive plan-walker sweep (the real bulk)

Update each explicit `ExecutionPlan` match so `EmptyRelation` is a childless, no-scan, no-join,
no-exchange leaf. Run `rg "ExecutionPlan::"` after Phase 1 and fix every compile failure **without a
`_` wildcard**:
- `binder.rs:2370` `optimize_for_required` → return the plan as-is (empty remap; zero-row relation
  needs no projection pruning); `binder.rs:2277` `terminal_exchange` → none.
- `fragment.rs`: top-level `fragment_plan` → exchange count zero ⇒ single-task stage acceptable;
  `first_join`→none (`:883`), `find_exchange`→none (`:934`), `count_exchanges`→0 (`:964`),
  `first_scan_metadata`→none (`:990`), `replace_exchange_with_scan`/`replace_first_join`→unchanged
  (`:1035`/`:1109`); confirm the no-exchange single-task path (`:97-122`).
- `scheduler.rs:769` `first_scan_fingerprint` → `None`.
- `server/mod.rs`: `join_strategy_label`→none (`:862`), `contains_join`→false (`:885`),
  `operator_kind_label`→`"empty_relation"` (`:908`), `contains_broadcast_join`→false (`:1059`),
  `scan_schema_for_table_id`→none (`:2011`).

## Phase 4 — The classifier (binder-local)

```rust
enum StaticFilterClass { AlwaysPass, AlwaysReject, Unknown }

fn static_filter_class(predicate: &BoundExprProgram) -> StaticFilterClass {
    if structural_non_nullable_is_null(predicate) { return StaticFilterClass::AlwaysReject; }
    if predicate.single_root().is_some() && !program_has_column_references(predicate) {
        return closed_root_filter_class(predicate);
    }
    StaticFilterClass::Unknown
}

fn program_has_column_references(p: &BoundExprProgram) -> bool {
    p.nodes.iter().any(|n| matches!(n, BoundExprNode::Column(_)))
}
```

- **`closed_root_filter_class`**: build the 1-row zero-column dummy
  (`RecordBatchOptions::new().with_row_count(Some(1))`), call
  `evaluate_bound_program_without_storage(predicate, &dummy)`; require exactly one output, downcast to
  `BooleanArray`, inspect row 0 → `Some(true)`⇒`AlwaysPass`, `Some(false)`/`None`⇒`AlwaysReject`.
  **Any error / non-boolean / arity surprise ⇒ `Unknown`** (D5 — never let a classification error
  replace the runtime plan).
- **`structural_non_nullable_is_null`**: root is exactly `BoundExprNode::Unary { op:
  BoundUnaryOp::IsNull, input }` where `input` resolves directly to
  `BoundExprNode::Column(column)` and `column.nullable == false`. `ResolvedColumn.nullable` is already
  populated from the input schema at bind time. Do **not** infer through casts/projections/`NOT`/`OR`/
  nesting.
- **No recursive boolean walk.** `FALSE AND id=5` has a column node ⇒ `Unknown`.

**Dependency caveat:** if importing `Executor` into `binder.rs` is an awkward shape, extract only the
needed evaluator entry into a shared Toron module — do not reimplement expression semantics in the
binder.

## Phase 5 — Hook before pushdown

At the top of `plan_filter_with_predicate_pushdown` (`binder.rs:1084`), before `predicate_pushdown_plan`:

```rust
if matches!(static_filter_class(&predicate), StaticFilterClass::AlwaysReject) {
    return (
        ExecutionPlan::EmptyRelation { schema: input.schema() },
        fallback_profile.unwrap_or(QueryProfileHint::Scan),
    );
}
```

Replace *only* the Filter node — no PBC, no Filter above; don't `apply_scan_profile_hint` (no scan to
profile). The parent operator binds over `EmptyRelation`, so the cardinality fence is structural.
UPDATE-source elision falls out (routes through this helper); DELETE untouched.

## Phase 6 — Behavior fixtures (`predicate_widening_test.rs` / binder tests)

`where_false_literal_elides_to_empty_relation`; `where_literal_comparison_false_elides_to_empty_relation`
(`1=0`); `where_closed_null_predicate_elides_to_empty_relation` (build with `ScalarValue::Boolean(None)`
if SQL lacks a boolean-NULL literal); `where_non_nullable_column_is_null_elides_to_empty_relation`;
`where_nullable_column_is_null_does_not_elide`; `where_column_predicate_does_not_elide` (`id=5`);
`where_false_and_column_predicate_stays_out_of_scope`; `where_false_and_erroring_expression_does_not_elide`
(closed strict cast that errors ⇒ stays a Filter; execution raises); `empty_relation_executor_returns_one_empty_batch`;
`where_static_false_does_not_scan_storage` (recording scan: 0); `count_star_over_static_false_filter_returns_single_zero`;
`grouped_aggregate_over_static_false_filter_returns_zero_groups`; `project_over_static_false_filter_preserves_schema`.
Guard-lite (optional): `update_where_static_false_uses_empty_source_and_writes_zero_rows`;
`empty_projection_preserves_logical_identity` (CTAS-over-empty retains durable identity).

## Phase 7 — Architecture guards (`tests/architecture/`)

`adr095_empty_relation_is_execution_plan_only`; `adr095_filter_elision_runs_before_pbc_pushdown`;
`adr095_static_filter_classifier_preserves_where_3vl` (closed `Boolean(None)` is always-rejecting,
value-dependent unknown is not); `adr095_static_filter_classifier_is_error_conservative` (no recursive
`FALSE AND _`; column-free whole root + structural `IsNull` only; error ⇒ `Unknown`);
`adr095_empty_relation_executor_does_not_scan_storage` (behavior); `adr095_scalar_aggregate_cardinality_survives_empty_relation`;
`adr095_plan_walkers_handle_empty_relation_as_leaf` (fragment/scheduler/binder/server name the variant;
no wildcard); `adr095_no_sql_pbc_wendelon_storage_surface`; `adr095_refinement_persistence_deferred_to_096`.

## Phase 8 — Verification

```
cargo fmt -p toron
cargo test -p toron --test predicate_widening_test --test update_test
cargo test -p toron --test architecture_test        # adr095_* + keep-green
cargo test -p toron                                  # full suite; cardinality + no regression
```

**Acceptance:** impossible filters elide with zero scans; the error-fence + data-dependent +
nullable-`IS NULL` fixtures prove the conservative boundary (no false elision, no masked errors);
`COUNT(*)` over static-false returns one `0` row, grouped returns zero groups, project preserves
schema; the ~15-site sweep compiles with no wildcard (guarded); full suite green, not red.

## Risks / notes

- **Honest scope:** *not* "no wire" — `EmptyRelation` is a same-version Toron-internal `TaskSpec`
  payload variant; no public/SQL/Wendelon/PBC surface; one internal execution variant.
- **Error preservation is load-bearing:** the closed-root path must reuse the eager evaluator so a
  strict-cast error during classification surfaces as `Unknown` (don't elide). Pinned by
  `where_false_and_erroring_expression_does_not_elide`.
- **The dummy batch must be 1 row** (`with_row_count(Some(1))`); a zero-row dummy yields a zero-length
  array with no inspectable scalar.
- **No recursive short-circuit / no always-true removal** — both deferred; the column-free precondition
  is the same fence that prevents error masking.
- **EmptyRelation carries a `SchemaRef`**, so 087–093 logical identity rides the schema structurally;
  the optional identity fixture is a regression guard, not new machinery.
