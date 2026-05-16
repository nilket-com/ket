# Implementation Plan: ADR 019 — MIN / MAX Aggregate Kernels

**Companion ADR:**
`plans/adr/019_min_max_aggregate_kernels.md`

## Status

Promoted and implemented.

First kernel-catalog widening under the post-ADR-018 cycle.
One enum-variant widening (5 → 7, adding `Min` and `Max`),
two binder arm extensions, two executor arm extensions, four
new private executor enum variants, eight new helper
functions, two architecture-guard table entries, regression
tests across planner / binder / executor / scope / ambiguity
/ distributed surfaces, and a lightweight bench artifact (per
Codex's argument that ADR 011's no-bench precedent applies
only to making existing kernels reachable, not to adding new
stable ones).

## Goal

Land MIN and MAX end-to-end over Int32/Int64 inputs:

- planner accepts MIN and MAX in scalar and grouped
  projections, HAVING predicates, and ORDER BY keys; lowers
  each via the existing ADR 015 / 016 / 017 / 018 substrate;
- `lower_min_aggregate` and `lower_max_aggregate` produce
  `(AggregateFunction::{Min,Max}, vec![<lowered arg>])`;
- `bind_aggregate_call` validates Int32/Int64 input via
  `min_max_output_data_type`; `aggregate_field` produces
  identity-typed output (`Int32` → `Int32`, `Int64` →
  `Int64`), nullable;
- `bound_global_aggregate` and `bound_grouped_aggregate`
  track running min/max per row via `update_min_state` /
  `update_max_state`; NULL-on-empty contract enforced via
  `Option<T>` state representation;
- ADR 016 walker, ADR 017 LoweredSelect seam, ADR 018
  scalar surface — all generalize to MIN/MAX without
  modification;
- `bind_comparison` numeric-literal coercion (ADR 016)
  composes with MIN/MAX's identity-type output against
  Int32 / Int64 RHS in HAVING;
- ADR 011's per-partition aggregation contract inherits:
  MIN/MAX execute single-stage scalar, per-partition
  grouped;
- in-process digest parity against hand-built MIN/MAX
  `Aggregate` plan; multi-process digest parity for grouped
  MIN + MAX across worker counts;
- bench artifact captures correctness + throughput numbers.

## Substrate

- `crates/toron/src/ir.rs`
  - `AggregateFunction` enum at `ir.rs:107-114`: add `Min`
    and `Max` variants. Five-to-seven variant widening.

- `crates/toron/src/sql_planner.rs`
  - `lower_aggregate_function` at `sql_planner.rs:526-556`:
    add `"MIN"` and `"MAX"` arms.
  - New `lower_min_aggregate` and `lower_max_aggregate`
    helpers paralleling `lower_sum_aggregate` at
    `sql_planner.rs:576-584`.
  - `aggregate_output_name` at `sql_planner.rs:1101`: add
    arms returning `"min"` and `"max"`.
  - `GroupedAggregateSignature` enum at
    `sql_planner.rs:974-979`: add `Min(ColumnRef)` and
    `Max(ColumnRef)` variants.
  - `grouped_aggregate_signature` at `sql_planner.rs:1110`:
    add arms using inline match destructure (parallel
    SUM/COUNT pattern).

- `crates/toron/src/binder.rs`
  - `bind_aggregate_call` at `binder.rs:1076`: extend the
    `Count | Sum` pattern to `Count | Sum | Min | Max`;
    add a parallel `if matches!(function,
    AggregateFunction::Min | AggregateFunction::Max)`
    validation block.
  - Local helper `aggregate_function_keyword` returning
    `"MIN"` / `"MAX"` strings for error messages.
  - `aggregate_field` at `binder.rs:1322-1337`: add MIN and
    MAX arms producing identity-typed nullable fields.
  - New `min_max_output_data_type` helper paralleling
    `sum_output_data_type` at `binder.rs:1339-1352`.

- `crates/toron/src/exec.rs`
  - `AggState` enum at `exec.rs:2740-2744`: add four
    variants — `MinInt32 { value: Option<i32> }`,
    `MinInt64 { value: Option<i64> }`, `MaxInt32 { value:
    Option<i32> }`, `MaxInt64 { value: Option<i64> }`.
    Enum **stays `Copy`** because all variants are stack-
    only.
  - `init_states` (cfg(test) at `exec.rs:2753-2771`) gains
    MIN and MAX arms for the legacy unbound path and defaults
    to `Int64`; do not use it to pin Int32 grouped MIN/MAX
    semantics. `init_bound_states` (production at
    `exec.rs:2773-2795`) becomes result-returning and
    dispatches via `min_state_for_data_type` /
    `max_state_for_data_type`.
  - New `min_state_for_data_type` and
    `max_state_for_data_type` helpers paralleling
    `sum_state_for_data_type` at `exec.rs:2818-2829`.
  - `bound_global_aggregate` at `exec.rs:871-924`: add MIN
    and MAX arms. Accumulate via `update_min_state` /
    `update_max_state`; emit via `min_max_state_array`.
  - `bound_grouped_aggregate` at `exec.rs:1059-1158`: add
    MIN and MAX arms in the per-row accumulation loop and
    the column emission loop. Emit via
    `min_max_column_from_groups`.
  - New helpers: `update_min_state`, `update_max_state`,
    `min_max_state_array`, `min_max_column_from_groups`.

- `crates/toron/src/distributed/fragment.rs` — no change.
  MIN/MAX inherit ADR 011's grouped-aggregate fragmenter
  routing (hash-partitioned exchange below `Aggregate`) and
  ADR 018's scalar routing.

- `crates/toron/tests/group_by_test.rs` — ADR 019 adds
  planner + binder + executor test cases for MIN and MAX,
  V1 exclusion regressions, empty-input pins, HAVING +
  ORDER BY composition tests.

- `crates/toron/tests/distributed_process_test.rs` — ADR
  019 adds one multi-process fixture (grouped ordered MIN +
  MAX) to pin digest parity across worker counts.

- `crates/toron/tests/architecture_test.rs` — ADR 019 adds
  `adr019_min_max_aggregate_kernels_guards_are_narrow` test
  with variant-count delta, new table entries, `AggState`
  Copy-derivation preserved, cross-ring-absence guards.

- `crates/toron/plans/impl/019_min_max_aggregate_kernels_bench_results.md` —
  bench artifact (lightweight; no speed gate).

## Files

Expected code changes:

- `crates/toron/src/ir.rs`:
  - Two-line enum variant addition (Min, Max).
- `crates/toron/src/sql_planner.rs`:
  - `lower_aggregate_function` arms for `"MIN"` and
    `"MAX"`.
  - New `lower_min_aggregate` / `lower_max_aggregate`
    helpers (~20 lines).
  - `aggregate_output_name` arms (~3 lines).
  - `GroupedAggregateSignature::Min(ColumnRef)` and
    `Max(ColumnRef)` variants (~2 lines).
  - `grouped_aggregate_signature` arms (~6 lines).
- `crates/toron/src/binder.rs`:
  - `bind_aggregate_call` MIN/MAX combined arm (~15
    lines).
  - Local `aggregate_function_keyword` helper (~5 lines).
  - `aggregate_field` MIN and MAX arms (~12 lines).
  - New `min_max_output_data_type` helper (~6 lines).
- `crates/toron/src/exec.rs`:
  - `AggState` four new variants (~4 lines).
  - `init_states` (cfg(test)) MIN and MAX arms (~2 lines).
  - `init_bound_states` result-returning signature plus MIN
    and MAX arms (~16 lines).
  - New `min_state_for_data_type` /
    `max_state_for_data_type` helpers (~20 lines).
  - New `update_min_state` / `update_max_state` helpers
    (~60 lines).
  - New `min_max_state_array` helper (~15 lines).
  - New `min_max_column_from_groups` helper (~30 lines).
  - `bound_global_aggregate` MIN and MAX arms (~20 lines).
  - `bound_grouped_aggregate` MIN/MAX accumulation +
    emission arms (~15 lines).
  - Count emission arm variant extension (~4 lines).
- `crates/toron/tests/group_by_test.rs`:
  - Scalar / grouped / dual-rewrite MIN/MAX plan-shape
    tests.
  - V1 exclusion regression tests (`MIN()`, `MIN(*)`,
    `MIN(DISTINCT col)`, non-Int32/Int64 input — incl.
    explicit Float64 rejection and Utf8 rejection, since
    those are deferred-not-broken).
  - Empty-input pin (NULL contract): scalar one-row NULL,
    grouped zero rows.
  - HAVING / ORDER BY composition tests for MIN and MAX.
  - Ambiguity tests for MIN/MAX (parallel ADR 016 / 017 /
    018 patterns).
  - Qualified-reference pre-pass tests.
  - In-process executor digest parity (scalar + grouped +
    HAVING + ORDER BY).
- `crates/toron/tests/distributed_process_test.rs`:
  - Multi-process fixture: grouped ordered MIN + MAX.
- `crates/toron/tests/architecture_test.rs`:
  - `adr019_min_max_aggregate_kernels_guards_are_narrow`
    test with variant-count assertion, two table-entry
    assertions, AggState Copy assertion, cross-ring-
    absence assertion.

Updated documentation:

- `crates/toron/plans/adr/019_min_max_aggregate_kernels.md`
  (companion ADR).
- `crates/toron/plans/impl/019_min_max_aggregate_kernels.md`
  (this file).
- `crates/toron/plans/impl/019_min_max_aggregate_kernels_bench_results.md`
  (bench artifact).
- `crates/toron/plans/wip/roadmap.md` — ADR 019 retires
  into "Implemented". ADR 020 candidates are seeded with
  AVG, float MIN/MAX, Utf8 MIN/MAX, ADR 02X (Wendelon AGG
  consumer wiring), post-aggregate projection.
- Auto-memory `adr-015-cycle-shape` updates to note that
  ADR 019 extends the cycle's source-level boundary clarity
  property to the kernel-catalog surface.

Not expected:

- New IR variants beyond `Min` and `Max` enum widening;
- new `FilterDisposition`, `JoinKind`, `JoinStrategy`,
  `MessageKind`, `TaskFailureKind`, `BudgetDimension`, or
  `TaskOutputBinding` variants;
- new `bind_expr` match arm;
- AVG kernel (deferred to ADR 020);
- float MIN/MAX (deferred to follow-on ADR with NaN
  policy);
- Utf8 MIN/MAX (deferred to substrate-refactor ADR);
- new shared helper extracted across SUM/MIN/MAX
  (deferred to post-AVG);
- changes to `RuntimeFilterDescriptor`, wire frame,
  cost-model knob, ADR 011 aggregate-output naming, ADR
  015 detector site, ADR 016 `Filter`-inside-gather
  contract, ADR 017 substrate, ADR 018 scalar surface,
  `bind_comparison` coercion;
- changes to executor's `Aggregate` / `Filter` / `Sort`
  dispatch;
- changes to `lower_order_by`'s signature;
- changes to the grouped-aggregate branch of
  `lower_select`;
- Wendelon AGG consumer wiring at
  `crates/toron/src/storage.rs` (ADR 02X);
- removal of `Copy` from `AggState` (Utf8 deferral
  preserves it).

## Phase 0 — Audit and shape decision

- **P0.1: `AggregateFunction` enum widening.** Add `Min`
  and `Max` variants at `ir.rs:107-114`. Architecture
  guard at `architecture_test.rs:473` updates 5 → 7.
  Mechanical.

- **P0.2: SQL planner catch-all reject lift.** Add `"MIN"`
  and `"MAX"` arms at `sql_planner.rs:537` (before the `_`
  catch-all). The catch-all stays; MIN and MAX remove
  themselves from its scope. Cycle's source-level boundary
  clarity property preserved at the kernel-catalog
  surface.

- **P0.3: Type domain — Int32, Int64 only.** Conservative
  intersection of Codex's `Int32`/`Int64`/`Utf8` and
  Gemini's `Int32`/`Int64`/`Float64`. Defers Float (NaN
  policy ADR) and Utf8 (AggState Copy-refactor ADR).
  Identity-type output: Int32 input → Int32 output, Int64
  input → Int64 output.

- **P0.4: MIN/MAX state shape — `Option<T>`.**
  `MinInt32 { value: Option<i32> }`, `MinInt64 { value:
  Option<i64> }`, and symmetric MAX variants. `None`
  represents the no-values-seen state; first non-NULL
  candidate sets `Some(candidate)`; subsequent candidates
  update via `value.min(candidate)` / `value.max
  (candidate)`. All stack-only; **AggState stays Copy**.

- **P0.5: Empty-input contract.** No non-NULL values seen
  → emit NULL. Scalar one-row NULL (via
  `min_max_state_array`); grouped zero rows (no groups
  exist) or per-group NULL where the group has only NULL
  values.

- **P0.6: Single feature commit.** ADR 019 ships as one
  commit (`feat: ADR 019 MIN/MAX aggregate kernels`). The
  catch-all-arm lift, binder + executor arms, scope +
  signature variants, helper functions, and architecture-
  guard table delta land together.

- **P0.7: Pre-pass and walker symmetry.** Automatic
  inheritance. `reject_qualified_having_column_references`
  runs unchanged. `rewrite_grouped_aggregate_calls`
  rewrites MIN/MAX references in HAVING / ORDER BY against
  the MIN/MAX projection. Four-way ambiguity classification
  applies unchanged.

- **P0.8: Architecture guards (semantic only).**
  - **Variant count:** `AggregateFunction` enum 5 → 7.
  - **Table entries (new):** `Min` / `"min"` and `Max` /
    `"max"` rows at `architecture_test.rs:485-507`.
  - **AggState Copy preservation:** explicitly asserted
    via `derives` inspection. ADR 020's Utf8 widening
    would flip this.
  - **Absence (preserved):** catch-all at
    `sql_planner.rs:554` stays; no `Expr::AggregateCall`
    in `bind_expr`; no `AggregateExpression` /
    `AggregateFunctionTag` symbols in
    `crates/toron/src/storage.rs` (cross-ring absence).
  - **Inherited (ADR 016 / 017 / 018):** five walker /
    scope / pre-pass reject strings remain unchanged.

- **P0.9: `bind_comparison` inheritance.** MIN/MAX's
  identity-type output composes with inherited ADR 016
  numeric-literal coercion. `Expr::Gt(Expr::column("min"),
  Literal(Int32(0)))` binds: LHS column type Int32 from
  `Aggregate { aggr_expr: [(Min, ...)] }`'s output schema
  for Int32 input; RHS Int32(0) matches directly. Mixed
  Int32/Int64 columns coerce via the existing substrate.

- **P0.10: SUM kernel substrate untouched.** ADR 019's
  MIN/MAX implementation is parallel to SUM (separate
  helpers). Refactor decision deferred to post-AVG.

- **P0.11: Fragmenter routing — zero change.** MIN/MAX
  inherit ADR 011's hash-partitioned exchange and ADR
  018's scalar routing.

- **P0.12: Bench artifact required.** Codex's argument:
  ADR 011 made existing kernels reachable for grouped SQL;
  ADR 019 adds new stable kernels. The Wendelon-ADR-070
  bench-required rule applies. **Lightweight artifact, no
  speed gate** — correctness fixtures (Int32 / Int64
  scalar and grouped, mixed-NULL inputs) and throughput
  numbers relative to nearby SUM rows.

## Phase 1 — Implementation

`crates/toron/src/`. One commit: `feat: ADR 019 MIN/MAX
aggregate kernels`.

### Step 1: enum widening at `ir.rs:107-114`

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunction {
    CountAll,
    Count,
    Sum,
    Min,
    Max,
    LinearRegress,
    ApproxQuantile(f64),
}
```

### Step 2: SQL planner arms at `sql_planner.rs`

Add MIN and MAX match arms in `lower_aggregate_function`:

```rust
"MIN" => self.lower_min_aggregate(args),
"MAX" => self.lower_max_aggregate(args),
```

Two new helpers paralleling `lower_sum_aggregate`:

```rust
fn lower_min_aggregate(
    &self,
    args: &[FunctionArg],
) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
    if args.len() != 1 {
        return Err(unsupported("MIN requires one argument"));
    }
    Ok((AggregateFunction::Min, vec![self.lower_arg(&args[0])?]))
}

fn lower_max_aggregate(
    &self,
    args: &[FunctionArg],
) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
    if args.len() != 1 {
        return Err(unsupported("MAX requires one argument"));
    }
    Ok((AggregateFunction::Max, vec![self.lower_arg(&args[0])?]))
}
```

`aggregate_output_name`:

```rust
AggregateFunction::Min => "min",
AggregateFunction::Max => "max",
```

`GroupedAggregateSignature` enum:

```rust
Min(ColumnRef),
Max(ColumnRef),
```

`grouped_aggregate_signature` arms (inline match
destructure):

```rust
AggregateFunction::Min => match args {
    [Expr::Column(column)] => Some(GroupedAggregateSignature::Min(column.clone())),
    _ => None,
},
AggregateFunction::Max => match args {
    [Expr::Column(column)] => Some(GroupedAggregateSignature::Max(column.clone())),
    _ => None,
},
```

### Step 3: binder arms at `binder.rs`

Extend the `Count | Sum` arm pattern in
`bind_aggregate_call`. MIN and MAX share an `if matches!`
block because their type constraint is identical:

```rust
AggregateFunction::Count
| AggregateFunction::Sum
| AggregateFunction::Min
| AggregateFunction::Max => {
    let args = args
        .iter()
        .map(|expr| bind_expr(expr, input_schema, config))
        .collect::<Result<Vec<_>, _>>()?;
    if matches!(function, AggregateFunction::Sum) {
        // Existing SUM validation — unchanged.
    }
    if matches!(function, AggregateFunction::Min | AggregateFunction::Max) {
        let Some(arg) = args.first() else {
            return Err(BindingError::UnsupportedExpression {
                message: format!("{} requires one argument",
                    aggregate_function_keyword(function)),
            });
        };
        if min_max_output_data_type(&arg.data_type()).is_none() {
            return Err(BindingError::TypeMismatch {
                message: format!("{} expects Int32 or Int64 input, got {:?}",
                    aggregate_function_keyword(function), arg.data_type()),
            });
        }
    }
    Ok(AggregateCall { function: function.clone(), args })
}
```

Local helper:

```rust
fn aggregate_function_keyword(function: &AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
        _ => "aggregate", // unreachable in current call sites
    }
}
```

`aggregate_field`:

```rust
AggregateFunction::Min => Field::new(
    "min",
    call.args.first()
        .and_then(|e| min_max_output_data_type(&e.data_type()))
        .unwrap_or(DataType::Int64),
    true,
),
AggregateFunction::Max => Field::new(
    "max",
    call.args.first()
        .and_then(|e| min_max_output_data_type(&e.data_type()))
        .unwrap_or(DataType::Int64),
    true,
),
```

New helper:

```rust
fn min_max_output_data_type(input: &DataType) -> Option<DataType> {
    match input {
        DataType::Int32 | DataType::Int64 => Some(input.clone()),
        _ => None,
    }
}
```

### Step 4: executor at `exec.rs`

`AggState` variants:

```rust
#[derive(Debug, Clone, Copy)]
enum AggState {
    Count(i64),
    SumInt { total: i128, seen: bool },
    SumFloat { total: f64, seen: bool },
    MinInt32 { value: Option<i32> },
    MinInt64 { value: Option<i64> },
    MaxInt32 { value: Option<i32> },
    MaxInt64 { value: Option<i64> },
}
```

**`AggState` keeps `Copy`** because all variants are
stack-only (Int32/Int64 + `Option<T>` of integers).

`init_states` gains MIN/MAX arms for the legacy unbound path.
It defaults to Int64 because it has no bound argument types; do
not use it to pin Int32 grouped MIN/MAX semantics.

`init_bound_states` should become result-returning so invalid
hand-built bound plans fail during state initialization rather
than silently defaulting:

```rust
// init_states (#[cfg(test)] at exec.rs:2753-2771):
AggregateFunction::Min => AggState::MinInt64 { value: None },
AggregateFunction::Max => AggState::MaxInt64 { value: None },

// init_bound_states (production at exec.rs:2773-2795):
AggregateFunction::Min => call
    .args
    .first()
    .map(|arg| min_state_for_data_type(&arg.data_type()))
    .transpose()?
    .unwrap_or(AggState::MinInt64 { value: None }),
AggregateFunction::Max => call
    .args
    .first()
    .map(|arg| max_state_for_data_type(&arg.data_type()))
    .transpose()?
    .unwrap_or(AggState::MaxInt64 { value: None }),
```

The `bound_grouped_aggregate` group lookup must then stop using
`or_insert_with(|| init_bound_states(aggr_expr))`, because that
closure cannot propagate `ExecutionError`. Use an entry match:

```rust
let states = match groups.entry(key) {
    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
    std::collections::btree_map::Entry::Vacant(entry) => {
        entry.insert(init_bound_states(aggr_expr)?)
    }
};
```

New helpers:

```rust
fn min_state_for_data_type(data_type: &DataType) -> Result<AggState, ExecutionError> {
    match data_type {
        DataType::Int32 => Ok(AggState::MinInt32 { value: None }),
        DataType::Int64 => Ok(AggState::MinInt64 { value: None }),
        _ => Err(ExecutionError::InvalidExpression {
            message: format!("MIN does not support input type {:?}", data_type),
        }),
    }
}

fn max_state_for_data_type(data_type: &DataType) -> Result<AggState, ExecutionError> {
    match data_type {
        DataType::Int32 => Ok(AggState::MaxInt32 { value: None }),
        DataType::Int64 => Ok(AggState::MaxInt64 { value: None }),
        _ => Err(ExecutionError::InvalidExpression {
            message: format!("MAX does not support input type {:?}", data_type),
        }),
    }
}

fn update_min_state(state: &mut AggState, array: &ArrayRef, row: usize)
    -> Result<(), ExecutionError>
{
    if array.is_null(row) {
        return Ok(());
    }
    match state {
        AggState::MinInt32 { value } => {
            let candidate = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| ExecutionError::InvalidExpression {
                    message: "MIN(Int32) array type mismatch".to_string(),
                })?
                .value(row);
            *value = Some(value.map_or(candidate, |v| v.min(candidate)));
            Ok(())
        }
        AggState::MinInt64 { value } => {
            let candidate = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ExecutionError::InvalidExpression {
                    message: "MIN(Int64) array type mismatch".to_string(),
                })?
                .value(row);
            *value = Some(value.map_or(candidate, |v| v.min(candidate)));
            Ok(())
        }
        _ => Err(ExecutionError::InvalidExpression {
            message: "MIN value reached non-MIN accumulator".to_string(),
        }),
    }
}

fn update_max_state(state: &mut AggState, array: &ArrayRef, row: usize)
    -> Result<(), ExecutionError>
{
    if array.is_null(row) {
        return Ok(());
    }
    match state {
        AggState::MaxInt32 { value } => {
            let candidate = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| ExecutionError::InvalidExpression {
                    message: "MAX(Int32) array type mismatch".to_string(),
                })?
                .value(row);
            *value = Some(value.map_or(candidate, |v| v.max(candidate)));
            Ok(())
        }
        AggState::MaxInt64 { value } => {
            let candidate = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ExecutionError::InvalidExpression {
                    message: "MAX(Int64) array type mismatch".to_string(),
                })?
                .value(row);
            *value = Some(value.map_or(candidate, |v| v.max(candidate)));
            Ok(())
        }
        _ => Err(ExecutionError::InvalidExpression {
            message: "MAX value reached non-MAX accumulator".to_string(),
        }),
    }
}

fn min_max_state_array(state: &AggState) -> Result<ArrayRef, ExecutionError> {
    match state {
        AggState::MinInt32 { value } | AggState::MaxInt32 { value } => {
            Ok(Arc::new(Int32Array::from(vec![*value])))
        }
        AggState::MinInt64 { value } | AggState::MaxInt64 { value } => {
            Ok(Arc::new(Int64Array::from(vec![*value])))
        }
        _ => Err(ExecutionError::InvalidExpression {
            message: "MIN/MAX finalize over non-MIN/MAX state".to_string(),
        }),
    }
}

fn min_max_column_from_groups(
    groups: &BTreeMap<Vec<GroupValue>, Vec<AggState>>,
    agg_idx: usize,
    output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
    match output_type {
        DataType::Int32 => {
            let values: Vec<Option<i32>> = groups
                .values()
                .map(|states| match states[agg_idx] {
                    AggState::MinInt32 { value } | AggState::MaxInt32 { value } => value,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = groups
                .values()
                .map(|states| match states[agg_idx] {
                    AggState::MinInt64 { value } | AggState::MaxInt64 { value } => value,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        _ => Err(ExecutionError::InvalidExpression {
            message: format!("unsupported MIN/MAX output type: {:?}", output_type),
        }),
    }
}
```

`bound_global_aggregate` MIN arm at `exec.rs:871-924`:

```rust
AggregateFunction::Min => {
    let mut state = min_state_for_data_type(&call.args[0].data_type())?;
    for batch in input_batches {
        let arr = self.evaluate_bound_expr(&call.args[0], batch)?;
        for row in 0..batch.num_rows() {
            update_min_state(&mut state, &arr, row)?;
        }
    }
    results_cols.push(min_max_state_array(&state)?);
}
```

MAX arm symmetric.

`bound_grouped_aggregate` per-row accumulation loop
extension:

```rust
AggState::MinInt32 { .. } | AggState::MinInt64 { .. } => {
    update_min_state(state, array, row)?;
}
AggState::MaxInt32 { .. } | AggState::MaxInt64 { .. } => {
    update_max_state(state, array, row)?;
}
```

Column emission loop MIN/MAX arm:

```rust
AggregateFunction::Min | AggregateFunction::Max => {
    let output_type = output_schema.field(columns.len()).data_type().clone();
    columns.push(min_max_column_from_groups(&groups, agg_idx, &output_type)?);
}
```

Count emission arm extension:

```rust
AggState::Count(value) => value,
AggState::SumInt { .. }
| AggState::SumFloat { .. }
| AggState::MinInt32 { .. }
| AggState::MinInt64 { .. }
| AggState::MaxInt32 { .. }
| AggState::MaxInt64 { .. } => 0,
```

### Step 5: planner tests in `group_by_test.rs`

- Plan-shape: scalar MIN, grouped MIN, scalar MAX, grouped
  MAX, dual MIN+MAX in same projection.
- Composition: scalar MIN with HAVING; grouped MIN with
  HAVING + ORDER BY + LIMIT; ORDER BY MAX(...).
- Lowercase recognition (`min(amount)` parses).
- Rejection: `MIN()` (`"MIN requires one argument"`);
  `MIN(*)` via Wildcard reject; `MIN(DISTINCT col)` via
  inherited DISTINCT reject; `MIN(name_string_col)` via
  Int32-or-Int64 type check; **`MIN(float64_col)` rejects
  explicitly** (pins V1 exclusion §1); **`MIN(utf8_col)`
  rejects explicitly** (pins V1 exclusion §2).
- Ambiguity: `SELECT MIN(amount), MIN(amount) FROM sales
  HAVING MIN(amount) > 0` rejects with
  `"ambiguous HAVING aggregate-call"`.
- Non-selected: `SELECT MIN(amount) FROM sales ORDER BY
  MAX(amount)` rejects with `"ORDER BY aggregate-call must
  appear in SELECT list"`.

### Step 6: executor tests in `group_by_test.rs`

- `scalar_min_int32_executes`: `[10, NULL, 3]` → 3 (Int32).
- `scalar_max_int64_executes`: `[10_i64, -2, NULL]` →
  10_i64 (Int64).
- `scalar_min_max_combined_executes`: same projection,
  both kernels, both correct.
- `grouped_min_executes_per_region`: returns per-region
  minimum; mixed-NULL groups handled correctly.
- `grouped_max_executes_per_region`: symmetric for MAX.
- `scalar_min_empty_input_returns_null`: empty fixture →
  `vec![None]` (Int64 typed NULL).
- `scalar_min_all_null_input_returns_null`: all-NULL
  fixture → `vec![None]`.
- `grouped_min_empty_input_returns_zero_rows`.
- `grouped_min_all_null_group_emits_null`: group with only
  NULL values → group present in output, value NULL.
- `scalar_min_sql_matches_hand_built_aggregate_plan`:
  digest parity scalar.
- `grouped_min_having_order_by_sql_matches_hand_built_plan`:
  digest parity composition.

### Step 7: distributed fixture in `distributed_process_test.rs`

- `ordered_grouped_min_max_process_digest_is_stable_across_worker_counts`:
  fixture-driven test for `SELECT val, MIN(val), MAX(val)
  FROM fixture GROUP BY val ORDER BY MAX(val) DESC LIMIT 5`
  across two- and four-worker process configurations.
  Asserts byte-identical normalized digest and unique
  worker count matches pool size.

### Step 8: architecture-test addition in `architecture_test.rs`

```rust
#[test]
fn adr019_min_max_aggregate_kernels_guards_are_narrow() {
    let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
        .expect("read SQL planner source");
    assert!(planner.contains("\"MIN\" => self.lower_min_aggregate"));
    assert!(planner.contains("\"MAX\" => self.lower_max_aggregate"));
    assert!(planner.contains("MIN requires one argument"));
    assert!(planner.contains("MAX requires one argument"));
    // Inherited catch-all preserved:
    assert!(planner.contains("unsupported aggregate function"));

    let binder = std::fs::read_to_string(manifest_dir().join("src/binder.rs"))
        .expect("read binder source");
    assert!(binder.contains("AggregateFunction::Min => Field::new(\"min\","));
    assert!(binder.contains("AggregateFunction::Max => Field::new(\"max\","));
    assert!(binder.contains("fn min_max_output_data_type"));
    assert!(binder.contains("expects Int32 or Int64 input"));

    let ir_source = std::fs::read_to_string(manifest_dir().join("src/ir.rs"))
        .expect("read IR source");
    let aggregate_function = ir_source
        .split("pub enum AggregateFunction")
        .nth(1)
        .and_then(|rest| rest.split("#[derive").next())
        .expect("AggregateFunction enum");
    assert_eq!(enum_variant_count(aggregate_function), 7);

    let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs"))
        .expect("read exec source");
    assert!(exec.contains("AggState::MinInt32"));
    assert!(exec.contains("AggState::MinInt64"));
    assert!(exec.contains("AggState::MaxInt32"));
    assert!(exec.contains("AggState::MaxInt64"));
    // AggState retains Copy derive — Utf8 widening would flip this.
    let before_agg_state = exec
        .split("enum AggState")
        .next()
        .unwrap_or("");
    let derives_above = before_agg_state.rsplit("#[derive").next().unwrap_or("");
    assert!(derives_above.contains("Copy"));

    // Cross-ring absence: no AGG protocol engagement.
    let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
        .expect("read storage source");
    assert!(!storage.contains("AggregateExpression"));
    assert!(!storage.contains("AggregateFunctionTag"));
}
```

Variant-count table at `architecture_test.rs:485-507`
gains two rows:

```rust
("AggregateFunction::Min", "\"min\"", "Field::new(\"min\","),
("AggregateFunction::Max", "\"max\"", "Field::new(\"max\","),
```

### Step 9: bench artifact

Lightweight artifact at
`crates/toron/plans/impl/019_min_max_aggregate_kernels_bench_results.md`
covering:

- Correctness fixtures: Int32 / Int64 scalar and grouped,
  mixed-NULL, all-NULL, empty input.
- Throughput numbers relative to nearby SUM rows on the
  same fixture (sales table, ~10M rows). No minimum-
  speedup gate — the artifact exists because the
  Wendelon-ADR-070 pattern applies.

Verification: `cargo test --locked -p toron`.

## Phase 2 — Fragmenter inspection (zero-code)

`crates/toron/src/distributed/fragment.rs`: no changes.
MIN/MAX inherit ADR 011's grouped-aggregate fragmenter
routing and ADR 018's scalar routing. The detectors match
on `LogicalPlan::Aggregate` structurally without kernel-
kind inspection.

## Phase 3 — Verification across layers

**Binder coverage** (`group_by_test.rs`):
- MIN/MAX identity-type output composes with
  `bind_comparison` for HAVING / ORDER BY predicates.
- `min_max_output_data_type` accepts Int32/Int64; rejects
  Float64/Utf8/other at the binder.

**Executor coverage** (`group_by_test.rs`):
- Per Step 6's executor test additions.
- Empty-input contract pinned (no-non-NULL-seen → NULL).

**Distributed coverage** (`distributed_process_test.rs`):
- Per Step 7's grouped ordered MIN + MAX fixture.

**In-process digest parity** (`group_by_test.rs`):
- Per Step 6's `_sql_matches_hand_built_plan` tests.

**Bench evidence**:
- Per Step 9's bench artifact.

## Phase 4 — Documentation and roadmap

- ADR + impl plan promoted to
  `plans/adr/019_min_max_aggregate_kernels.md` and
  `plans/impl/019_min_max_aggregate_kernels.md` with
  status "Promoted and implemented."
- Bench artifact at
  `plans/impl/019_min_max_aggregate_kernels_bench_results.md`.
- `plans/wip/roadmap.md` ADR 019 entry retires into
  "Implemented" with cycle-property note ("first kernel-
  catalog widening preserving source-level boundary clarity
  at the catch-all reject"); ADR 020 candidates section
  seeded with AVG, float MIN/MAX, Utf8 MIN/MAX, ADR 02X,
  post-aggregate projection.
- Auto-memory `adr-015-cycle-shape` updates to record
  that the cycle's property extends to the kernel-catalog
  surface; ADR 019 is the first instance.

## Verification

Minimum:

```text
cargo fmt --all --check
cargo check --workspace --locked
cargo test --locked -p toron --test group_by_test --no-fail-fast
cargo test --locked -p toron --test sort_ordering_test --no-fail-fast
cargo test --locked -p toron --test distributed_process_test --no-fail-fast
cargo test --locked -p toron --test architecture_test --no-fail-fast
git diff --check
```

Promotion:

```text
cargo test --locked --workspace --no-fail-fast
cargo bench -p toron --bench aggregate_kernels  # or equivalent
```

## Risks + mitigations

- **R1: AggState Copy preservation cascade.** If a future
  helper accidentally moves `states[agg_idx]` instead of
  borrowing, the compiler catches it (Copy is the
  current contract). Mitigation: architecture-test
  assertion that `AggState` derives `Copy`.

- **R2: Type-domain narrowness surprises users.** Users
  may expect `MIN(float_col)` to work. Mitigation:
  explicit rejection messages ("MIN expects Int32 or
  Int64 input, got Float64"). The future float-MIN/MAX
  ADR carries the lift.

- **R3: Architecture-guard variant-count cascade.** The
  variant count guard going from 5 → 7 may cascade.
  Mitigation: grep before commit; update assertion
  targets.

- **R4: Wendelon AGG protocol drift.** Wendelon-side may
  evolve `Min { column }` / `Max { column }` shape.
  Mitigation: ADR 019 does NOT engage the AGG protocol.

- **R5: `bind_comparison` mixed-int coercion gap.**
  ADR 016 inheritance assumes Int32×Int64 coercion for
  HAVING. Verification: existing bind_comparison tests
  cover this.

- **R6: Bench artifact scope creep.** The bench artifact
  is intentionally lightweight (correctness + throughput
  ratio vs SUM). Mitigation: explicit "no minimum-speedup
  gate" framing in §"Promotion evidence" and in the
  bench artifact itself.

- **R7: MIN/MAX shared dispatch refactor pressure.** With
  4 new helpers per kernel (state-for-type, update, state-
  to-array, column-from-groups) × 2 kernels = 8 new
  helpers, the duplication is meaningful. Mitigation:
  ADR 020 (AVG) ratifies the shared-helper refactor
  decision at three or four kernels.

- **R8: Wendelon-side AGG protocol post-planning drift.**
  Re-verify (`grep "Min { column }" crates/wendelon/src/
  schema/aggregate.rs`) immediately before promotion
  commit.

## Definition of done

- Single commit (`feat:`): two enum variants, two planner
  arms, one binder arm extension, two executor arm
  extensions, four new `AggState` variants, eight new
  helpers (`min_state_for_data_type`,
  `max_state_for_data_type`, `update_min_state`,
  `update_max_state`, `min_max_state_array`,
  `min_max_column_from_groups`, `min_max_output_data_type`,
  `aggregate_function_keyword`), two new SQL planner
  helpers (`lower_min_aggregate`, `lower_max_aggregate`),
  architecture-guard updates.
- Planner accepts MIN and MAX in scalar and grouped
  contexts; HAVING and ORDER BY compose via inherited
  substrate.
- All V1 exclusion items §1-§11 reject via inherited or
  new narrow named paths. Explicit regression tests for
  Float64 and Utf8 rejection (pins deferred-not-broken
  state).
- Empty-input contract pinned: scalar one-row NULL;
  grouped zero rows; all-NULL group emits NULL.
- Identity-type output for all supported input types
  (Int32 → Int32, Int64 → Int64).
- `AggState` retains `Copy` derive (architecture-test
  asserts).
- In-process digest parity for scalar and grouped MIN/MAX.
- Multi-process digest parity for grouped ordered MIN + MAX
  across two- and four-worker fixtures.
- Architecture-test additions land; variant count 7
  confirmed; two new table entries; cross-ring absence
  guard.
- **Bench artifact lands** at
  `plans/impl/019_min_max_aggregate_kernels_bench_results.md`
  with correctness fixtures and throughput numbers
  relative to SUM. No minimum-speedup gate.
- Wendelon AGG protocol consumer surface untouched
  (ADR 02X is the cross-ring ADR).
- SUM kernel substrate untouched (refactor decision
  deferred to post-AVG).
- Full Toron + Wendelon workspace test suites green.
- Memory file `adr-015-cycle-shape` updated: cycle's
  property extends to the kernel-catalog surface;
  ADR 019 is the first instance.
- Roadmap entry retired into "Implemented" with cycle-
  property note; "ADR 020 candidates" section seeded with
  AVG (lead), float MIN/MAX, Utf8 MIN/MAX, ADR 02X
  (Wendelon AGG consumer wiring), post-aggregate
  projection.
