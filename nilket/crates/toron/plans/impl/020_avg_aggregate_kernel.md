# Implementation Plan: ADR 020 — AVG Aggregate Kernel

**Companion ADR:**
`plans/adr/020_avg_aggregate_kernel.md`

## Status

Drafted. Awaiting promotion commit.

Second kernel-catalog widening under the post-ADR-018
cycle. One enum-variant widening (7 → 8, adding `Avg`),
one binder arm extension, two executor arm extensions (one
of which is the legacy unbound path with a simpler shape
than MIN/MAX), two new private executor enum variants
(`AggState` 7 → 9 variants, stays `Copy`), six new helper
functions, one renamed architecture-guard test, regression
tests across planner / binder / executor / scope /
ambiguity / distributed / sticky-NaN / bench surfaces, and
a lightweight bench artifact.

## Goal

Land AVG end-to-end over all SUM-compatible numeric inputs:

- planner accepts AVG in scalar and grouped projections,
  HAVING predicates, and ORDER BY keys; lowers each via
  the existing ADR 015 / 016 / 017 / 018 / 019 substrate;
- `lower_avg_aggregate` produces `(AggregateFunction::Avg,
  vec![<lowered arg>])`;
- `bind_aggregate_call` validates all SUM-compatible
  numeric input via `avg_output_data_type`;
  `aggregate_field` produces `Field::new("avg",
  DataType::Float64, true)`;
- `bound_global_aggregate` and `bound_grouped_aggregate`
  track running `(total, count)` per row via
  `add_avg_value`; finalize via `avg_finalize` to
  `total / count if count > 0 else NULL`;
- overflow discipline mirrors SUM's: `checked_add` with
  explicit `ExecutionError::Kernel`, mixed-type table
  support via `AvgFloat` accepting `SumValue::Int`,
  `AvgInt × SumValue::Float` as hard error;
- sticky-NaN follows ordinary `f64` arithmetic — NaN
  inputs propagate through the running total and produce
  NaN output. Hard-gate regression test pins the
  behavior;
- legacy unbound `Executor::execute_logical` uses a single
  `AvgFloat` accumulator (no first-batch type dance, no
  empty-input error path);
- ADR 016 walker, ADR 017 LoweredSelect seam, ADR 018
  scalar surface, ADR 019 kernel substrate — all
  generalize to AVG without modification;
- `bind_comparison` numeric-literal coercion (ADR 016)
  composes with AVG's Float64 output against Int32 /
  Int64 RHS in HAVING;
- ADR 011's per-partition aggregation contract inherits:
  AVG executes single-stage scalar, per-partition
  grouped;
- in-process digest parity against hand-built AVG
  `Aggregate` plan; multi-process digest parity for
  grouped AVG across worker counts;
- bench artifact captures correctness + throughput
  numbers including the first Float64-coverage case
  under the kernel-catalog-widening pattern.

## Substrate

- `crates/toron/src/ir.rs`
  - `AggregateFunction` enum at `ir.rs:107-116`: add `Avg`
    variant. Seven-to-eight variant widening.

- `crates/toron/src/sql_planner.rs`
  - `lower_aggregate_function` at `sql_planner.rs:526-556`:
    add `"AVG" => self.lower_avg_aggregate(args)` arm.
  - New `lower_avg_aggregate` helper paralleling
    `lower_sum_aggregate` at `sql_planner.rs:585-595`.
  - `aggregate_function_name` at `sql_planner.rs:1116+`:
    add `"AVG" => Some("AVG")`.
  - `aggregate_output_name` at `sql_planner.rs:1128+`:
    add arm returning `"avg"`.
  - `GroupedAggregateSignature` enum at
    `sql_planner.rs:998-1003`: add `Avg(ColumnRef)`
    variant.
  - `grouped_aggregate_signature` at
    `sql_planner.rs:1149+`: add arm using inline match
    destructure (parallel SUM/MIN/MAX pattern).

- `crates/toron/src/binder.rs`
  - `bind_aggregate_call` at `binder.rs:1076-1122`:
    extend the `Count | Sum | Min | Max` pattern to
    `Count | Sum | Min | Max | Avg`; add a parallel
    `if matches!(function, AggregateFunction::Avg)`
    validation block.
  - `aggregate_function_keyword` at `binder.rs:1402+`:
    add `AggregateFunction::Avg => "AVG"` arm.
  - `aggregate_field` at `binder.rs:1322-1382`: add AVG
    arm producing `Field::new("avg", DataType::Float64,
    true)`.
  - New `avg_output_data_type` helper paralleling
    `sum_output_data_type` at `binder.rs:1339-1352`.

- `crates/toron/src/exec.rs`
  - `AggState` enum at `exec.rs:2849-2862`: add two
    variants — `AvgInt { total: i128, count: u64 }` and
    `AvgFloat { total: f64, count: u64 }`. Enum
    **stays `Copy`** because both variants are stack-
    only. Field name `total` matches existing `SumInt
    { total: i128 }` convention.
  - `init_states` (cfg(test) at `exec.rs:2864-2887`)
    and `init_bound_states` (production at
    `exec.rs:2890-2925`): both gain AVG arms following
    SUM's infallible default pattern.
  - New `avg_state_for_data_type` helper paralleling
    `sum_state_for_data_type` at `exec.rs:2818-2829`.
  - `Executor::execute_logical` legacy unbound at
    `exec.rs:285-358`: add AVG arm using single
    `AvgFloat` accumulator throughout (no first-batch
    type dance, no empty-input error path — distinct
    from MIN/MAX's pattern).
  - `bound_global_aggregate` at `exec.rs:870-980`: add
    AVG arm. Accumulate via `add_avg_value`; emit via
    `avg_state_array`.
  - `bound_grouped_aggregate` at `exec.rs:1060-1260`:
    add AVG arms in the per-row accumulation loop,
    column emission loop, and (cfg(test)) field-name
    dispatch.
  - New helpers: `add_avg_value`, `avg_finalize`,
    `avg_state_array`, `avg_column_from_groups`.
  - **Permitted narrow rename:** if AVG genuinely
    shares the SUM-only numeric extraction helper
    (`SumValue`), it may be renamed to a neutral
    numeric-extraction helper. Broader unification is
    deferred to ADR 021.

- `crates/toron/src/distributed/fragment.rs` — no change.
  AVG inherits ADR 011's grouped-aggregate fragmenter
  routing (hash-partitioned exchange below `Aggregate`)
  and ADR 018's scalar routing.

- `crates/toron/tests/group_by_test.rs` — ADR 020 adds
  planner + binder + executor test cases for AVG, V1
  exclusion regressions, empty-input pins, all-null
  pins, sticky-NaN pin, HAVING + ORDER BY composition
  tests.

- `crates/toron/tests/distributed_process_test.rs` —
  ADR 020 adds one multi-process fixture (grouped
  ordered AVG) to pin digest parity across worker
  counts.

- `crates/toron/tests/architecture_test.rs` — existing
  `adr016_having_guards_are_narrow` test is renamed to
  `aggregate_function_enum_guards_are_narrow` and
  updated for variant count 8. New table entry for
  AVG.

- `crates/toron/plans/impl/020_avg_aggregate_kernel_bench_results.md` —
  bench artifact (lightweight; no speed gate).

## Files

Expected code changes:

- `crates/toron/src/ir.rs`:
  - One-line enum variant addition (Avg).
- `crates/toron/src/sql_planner.rs`:
  - `lower_aggregate_function` arm for `"AVG"`.
  - New `lower_avg_aggregate` helper (~10 lines).
  - `aggregate_function_name` arm (~1 line).
  - `aggregate_output_name` arm (~1 line).
  - `GroupedAggregateSignature::Avg(ColumnRef)`
    variant (~1 line).
  - `grouped_aggregate_signature` arm (~3 lines).
- `crates/toron/src/binder.rs`:
  - `bind_aggregate_call` AVG arm via parallel `if
    matches!` block (~10 lines) and extension of the
    top-level `Min | Max → Min | Max | Avg` arity
    check.
  - `aggregate_function_keyword` arm (~1 line).
  - `aggregate_field` AVG arm (~1 line).
  - New `avg_output_data_type` helper (~10 lines).
- `crates/toron/src/exec.rs`:
  - `AggState` two new variants (~2 lines).
  - `init_states` (cfg(test)) AVG arm (~1 line).
  - `init_bound_states` AVG arm (~4 lines).
  - New `avg_state_for_data_type` helper (~6 lines).
  - New `add_avg_value` helper (~40 lines).
  - New `avg_finalize` helper (~15 lines).
  - New `avg_state_array` helper (~5 lines).
  - New `avg_column_from_groups` helper (~10 lines).
  - `Executor::execute_logical` legacy unbound AVG arm
    (~15 lines).
  - `bound_global_aggregate` AVG arm (~12 lines).
  - `bound_grouped_aggregate` AVG arms — per-row
    accumulation (~5 lines), column emission arm
    (~3 lines), Count emission arm variant extension
    (~2 lines), cfg(test) field-name dispatch (~1
    line).
  - Permitted narrow rename of `SumValue` to neutral
    name if AVG shares it (~10-20 sites; ~0 net
    lines).
- `crates/toron/tests/group_by_test.rs`:
  - Scalar / grouped / dual-rewrite AVG plan-shape
    tests.
  - V1 exclusion regression tests (`AVG()`, `AVG(*)`,
    `AVG(DISTINCT col)`, non-numeric input — incl.
    explicit Utf8 / Boolean / Date / Timestamp
    rejection since those are deferred-not-broken).
  - Empty-input pin (NULL contract): scalar one-row
    NULL via explicit `vec![None]` assertion,
    grouped zero rows, all-NULL group emits NULL.
  - **Sticky-NaN pin:** `SELECT AVG(col) FROM
    fixture_with_nan` returns `NaN`; grouped query
    with any NaN in a group produces NaN for that
    group's average.
  - HAVING / ORDER BY composition tests for AVG.
  - Ambiguity tests for AVG (parallel ADR 016 / 017
    / 018 patterns).
  - Qualified-reference pre-pass tests.
  - In-process executor digest parity (scalar +
    grouped + HAVING + ORDER BY).
- `crates/toron/tests/distributed_process_test.rs`:
  - Multi-process fixture: grouped ordered AVG.
- `crates/toron/tests/architecture_test.rs`:
  - Rename `adr016_having_guards_are_narrow` →
    `aggregate_function_enum_guards_are_narrow`.
  - Update variant count assertion 7 → 8.
  - Add `AVG` row to the variant-mapping table.

Updated documentation:

- `crates/toron/plans/adr/020_avg_aggregate_kernel.md`
  (companion ADR).
- `crates/toron/plans/impl/020_avg_aggregate_kernel.md`
  (this file).
- `crates/toron/plans/impl/020_avg_aggregate_kernel_bench_results.md`
  (bench artifact).
- `crates/toron/plans/wip/roadmap.md` — ADR 020 retires
  from "candidates" into active/promoted work; it should
  only move to "Implemented" after code, tests, and bench
  artifact land. ADR 021 candidates section is seeded with
  kernel-registry refactor as lead, post-aggregate
  projection (ADR 022) as strong runner-up, float / Utf8
  MIN/MAX, ADR 02X (Wendelon AGG consumer wiring),
  Kahan-Neumaier stability.
- Auto-memory `adr-015-cycle-shape` updates to note
  that ADR 020 is the second instance of the cycle
  property extending to the kernel-catalog surface
  (after ADR 019 MIN/MAX).

Not expected:

- New IR variants beyond `Avg` enum widening;
- new `FilterDisposition`, `JoinKind`, `JoinStrategy`,
  `MessageKind`, `TaskFailureKind`, `BudgetDimension`,
  or `TaskOutputBinding` variants;
- new `bind_expr` match arm;
- kernel-registry trait or shared dispatch helpers
  (ADR 021);
- post-aggregate projection (ADR 022);
- Wendelon AGG consumer wiring (ADR 02X);
- Decimal output for integer AVG (V1.x Decimal ADR);
- Kahan-Neumaier summation (future stability ADR);
- float / Utf8 MIN/MAX (separate kernel ADRs);
- changes to `RuntimeFilterDescriptor`, wire frame,
  cost-model knob, ADR 011 aggregate-output naming,
  ADR 015 detector site, ADR 016 `Filter`-inside-
  gather contract, ADR 017 substrate, ADR 018 scalar
  surface, ADR 019 MIN/MAX substrate, `bind_comparison`
  coercion;
- changes to executor's `Aggregate` / `Filter` / `Sort`
  dispatch;
- changes to `lower_order_by`'s signature;
- changes to the grouped-aggregate branch of
  `lower_select`;
- removal of `Copy` from `AggState`.

## Phase 0 — Audit and shape decision

- **P0.1: `AggregateFunction` enum widening.** Add `Avg`
  variant at `ir.rs:107-116`. Architecture guard
  `aggregate_function_enum_guards_are_narrow` (renamed)
  updates 7 → 8. Mechanical.

- **P0.2: SQL planner catch-all reject lift.** Add the
  `"AVG"` arm at `sql_planner.rs:537` (before the `_`
  catch-all). The catch-all stays; AVG removes itself
  from its scope. Cycle's source-level boundary
  clarity property preserved at the kernel-catalog
  surface (second instance after ADR 019 MIN/MAX).

- **P0.3: Output type — Float64-uniform.** AVG output is
  Float64 regardless of input numeric width. Diverges
  from Postgres / MySQL (Decimal-by-default for
  integer inputs); matches DuckDB / SQLite. Toron has
  no Decimal substrate; Decimal output for integer AVG
  awaits the V1.x Decimal ADR. Integer totals and counts
  up to 2^53 are exactly representable before division;
  fractional averages still round to Float64.

- **P0.4: AVG accumulator state shape.** `(total: i128,
  count: u64)` for integer inputs; `(total: f64,
  count: u64)` for float inputs. Two new `AggState`
  variants. Field name `total` matches existing
  `SumInt { total: i128 }` convention. The `i128`
  width inherits from ADR 011's `SumInt.total: i128`
  substrate. Wendelon's `SignedAverageParts { sum:
  i128, count: u64 }` shape alignment is a
  structural-equivalence observation (modulo overflow
  bit), **not pinned as cross-ring contract**. ADR
  02X engages the cross-ring contract.

- **P0.5: Empty-input contract.** `count == 0 → NULL`
  finalization. Scalar AVG over empty: one row with
  NULL. Grouped AVG over empty: zero rows (no groups
  exist). All-NULL group: row exists, AVG is NULL.
  Pinned by `vec![None]` regression assertions
  (Codex's explicit pin requirement).

- **P0.6: Sticky-NaN behavior.** Float AVG uses
  ordinary `f64 += v` arithmetic. NaN inputs propagate
  through the running total; finalization
  `total / count` is NaN. Negative zero follows
  ordinary `f64` arithmetic. **Hard-gate regression
  test (per Gemini)** explicitly asserts this
  behavior. No special NaN handling is introduced.

- **P0.7: Overflow discipline mirrors SUM exactly.**
  `add_avg_value` uses `checked_add` (parallel SUM's
  `exec.rs:2833-2840`) and returns explicit
  `ExecutionError::Kernel { message: "integer AVG
  overflow" }` on overflow. `AvgFloat` accepts
  `SumValue::Int` and casts to f64 (parallel SUM's
  `exec.rs:2847-2851`). `AvgInt × SumValue::Float` is
  a hard error (parallel SUM's `exec.rs:2852-2854`).
  Count overflow is an explicit error (pathological
  case at >2^64 rows; the discipline matters).

- **P0.8: Single feature commit.** ADR 020 ships as one
  commit (`feat: ADR 020 AVG aggregate kernel`). The
  catch-all-arm lift, binder + executor arms, scope +
  signature variant, helper functions, sticky-NaN
  test, architecture-guard rename, and bench artifact
  land together.

- **P0.9: Pre-pass and walker symmetry.** Automatic
  inheritance.
  `reject_qualified_having_column_references` runs
  unchanged. `rewrite_grouped_aggregate_calls`
  rewrites AVG references in HAVING / ORDER BY
  against the AVG projection. Four-way ambiguity
  classification applies unchanged.

- **P0.10: Architecture guards (semantic only).**
  - **Variant count:** `AggregateFunction` enum
    7 → 8.
  - **Guard rename:**
    `adr016_having_guards_are_narrow` →
    `aggregate_function_enum_guards_are_narrow`. The
    guard is no longer ADR-016-specific; it's the
    catalog-wide invariant.
  - **Table entry (new):** `Avg` / `"avg"` /
    `Field::new("avg",` row at
    `architecture_test.rs:485+`.
  - **AggState Copy preservation:** explicitly
    asserted (carry-forward from ADR 019).
  - **Absence (preserved):** catch-all at
    `sql_planner.rs:556` stays; no
    `Expr::AggregateCall` in `bind_expr`; no
    `AggregateExpression` / `AggregateFunctionTag`
    symbols in `crates/toron/src/storage.rs`.
  - **Inherited (ADR 016 / 017 / 018 / 019):** all
    existing walker / scope / pre-pass / MIN/MAX
    reject strings remain unchanged.

- **P0.11: `bind_comparison` inheritance.** AVG's
  Float64 output composes with inherited ADR 016
  numeric-literal coercion. `Expr::Gt(Expr::column
  ("avg"), Literal(Int32(100)))` binds: LHS column
  type Float64 from `Aggregate { aggr_expr: [(Avg,
  ...)] }`'s output schema; RHS Int32(100) coerces to
  Float64. **Zero binder code change beyond the AVG
  arm in `bind_aggregate_call`.**

- **P0.12: SUM / MIN / MAX kernel substrates
  untouched.** AVG implementation is parallel to
  existing kernels (separate helpers). Permitted
  narrow rename: `SumValue → NumericValue` (or
  similar neutral name) *if* AVG shares the numeric-
  extraction helper. Broader unification is **ADR
  021's scope**.

- **P0.13: Fragmenter routing — zero change.** AVG
  over GROUP BY routes via ADR 011's hash-
  partitioned exchange. Scalar AVG routes via ADR
  018's single-task or generic one-exchange path.
  The grouped detector matches on
  `LogicalPlan::Aggregate` structurally without
  kernel-kind inspection.

- **P0.14: Bench artifact required.** Following ADR
  019's precedent — new stable kernel; bench
  artifact lands at
  `plans/impl/020_avg_aggregate_kernel_bench_results.md`.
  Lightweight, no speed gate. 5 new
  `storage_execution.rs` cases including the first
  Float64-coverage case under the kernel-catalog-
  widening pattern.

## Phase 1 — Implementation

`crates/toron/src/`. One commit: `feat: ADR 020 AVG
aggregate kernel`.

### Step 1: enum widening at `ir.rs:107-116`

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunction {
    CountAll,
    Count,
    Sum,
    Min,
    Max,
    Avg,
    LinearRegress,
    ApproxQuantile(f64),
}
```

### Step 2: SQL planner arms at `sql_planner.rs`

Add the AVG match arm in `lower_aggregate_function`:

```rust
"AVG" => self.lower_avg_aggregate(args),
```

New helper paralleling `lower_sum_aggregate`:

```rust
fn lower_avg_aggregate(
    &self,
    args: &[FunctionArg],
) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
    if args.len() != 1 {
        return Err(unsupported("AVG requires one argument"));
    }
    Ok((AggregateFunction::Avg, vec![self.lower_arg(&args[0])?]))
}
```

`aggregate_function_name`:

```rust
"AVG" => Some("AVG"),
```

`aggregate_output_name`:

```rust
AggregateFunction::Avg => "avg",
```

`GroupedAggregateSignature` enum:

```rust
Avg(ColumnRef),
```

`grouped_aggregate_signature` arm (inline match
destructure, parallel SUM/MIN/MAX):

```rust
AggregateFunction::Avg => match args {
    [Expr::Column(column)] => Some(GroupedAggregateSignature::Avg(column.clone())),
    _ => None,
},
```

### Step 3: binder arms at `binder.rs`

Extend the `Count | Sum | Min | Max` arm pattern in
`bind_aggregate_call` to include `Avg`. Extend the top-
level arity check from `Min | Max` to `Min | Max | Avg`:

```rust
AggregateFunction::Count
| AggregateFunction::Sum
| AggregateFunction::Min
| AggregateFunction::Max
| AggregateFunction::Avg => {
    if matches!(
        function,
        AggregateFunction::Min | AggregateFunction::Max | AggregateFunction::Avg
    ) && args.len() != 1
    {
        return Err(BindingError::UnsupportedExpression {
            message: format!(
                "{} requires one argument",
                aggregate_function_keyword(function)
            ),
        });
    }
    let args = args
        .iter()
        .map(|expr| bind_expr(expr, input_schema, config))
        .collect::<Result<Vec<_>, _>>()?;
    if matches!(function, AggregateFunction::Sum) {
        // Existing SUM validation — unchanged.
    }
    if matches!(function, AggregateFunction::Min | AggregateFunction::Max) {
        // Existing MIN/MAX validation — unchanged.
    }
    if matches!(function, AggregateFunction::Avg) {
        let arg = &args[0];
        if avg_output_data_type(&arg.data_type()).is_none() {
            return Err(BindingError::TypeMismatch {
                message: format!(
                    "AVG expects numeric input, got {:?}",
                    arg.data_type()
                ),
            });
        }
    }
    Ok(AggregateCall { function: function.clone(), args })
}
```

`aggregate_function_keyword` adds:

```rust
AggregateFunction::Avg => "AVG",
```

`aggregate_field`:

```rust
AggregateFunction::Avg => Field::new("avg", DataType::Float64, true),
```

New helper:

```rust
fn avg_output_data_type(input: &DataType) -> Option<DataType> {
    match input {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        | DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
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
    AvgInt { total: i128, count: u64 },
    AvgFloat { total: f64, count: u64 },
}
```

Both `init_*` functions gain AVG arms. The test-only
unbound default may use `AvgInt`; the production bound
path validates the bound argument type defensively:

```rust
// init_states (#[cfg(test)] at exec.rs:2864-2887):
AggregateFunction::Avg => AggState::AvgInt { total: 0, count: 0 },

// init_bound_states (production at exec.rs:2890-2925):
AggregateFunction::Avg => call
    .args
    .first()
    .ok_or_else(|| ExecutionError::InvalidExpression {
        message: "AVG requires one argument".to_string(),
    })
    .and_then(|arg| avg_state_for_data_type(&arg.data_type())),
```

New helpers:

```rust
fn avg_state_for_data_type(data_type: &DataType) -> Result<AggState, ExecutionError> {
    match data_type {
        DataType::Float32 | DataType::Float64 =>
            Ok(AggState::AvgFloat { total: 0.0, count: 0 }),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 =>
            Ok(AggState::AvgInt { total: 0, count: 0 }),
        _ => Err(ExecutionError::InvalidExpression {
            message: format!("AVG does not support input type {:?}", data_type),
        }),
    }
}

fn add_avg_value(state: &mut AggState, value: SumValue)
    -> Result<(), ExecutionError>
{
    match (state, value) {
        (AggState::AvgInt { total, count }, SumValue::Int(v)) => {
            *total = total
                .checked_add(v)
                .ok_or_else(|| ExecutionError::Kernel {
                    message: "integer AVG overflow".to_string(),
                })?;
            *count = count.checked_add(1)
                .ok_or_else(|| ExecutionError::Kernel {
                    message: "AVG count overflow".to_string(),
                })?;
            Ok(())
        }
        (AggState::AvgFloat { total, count }, SumValue::Float(v)) => {
            *total += v;
            *count = count.checked_add(1)
                .ok_or_else(|| ExecutionError::Kernel {
                    message: "AVG count overflow".to_string(),
                })?;
            Ok(())
        }
        (AggState::AvgFloat { total, count }, SumValue::Int(v)) => {
            *total += v as f64;
            *count = count.checked_add(1)
                .ok_or_else(|| ExecutionError::Kernel {
                    message: "AVG count overflow".to_string(),
                })?;
            Ok(())
        }
        (AggState::AvgInt { .. }, SumValue::Float(_)) => {
            Err(ExecutionError::InvalidExpression {
                message: "floating AVG value reached integer accumulator".to_string(),
            })
        }
        _ => Err(ExecutionError::InvalidExpression {
            message: "AVG value reached non-AVG accumulator".to_string(),
        }),
    }
}

fn avg_finalize(state: &AggState) -> Result<Option<f64>, ExecutionError> {
    match state {
        AggState::AvgInt { total, count } => Ok(if *count == 0 {
            None
        } else {
            Some(*total as f64 / *count as f64)
        }),
        AggState::AvgFloat { total, count } => Ok(if *count == 0 {
            None
        } else {
            Some(*total / *count as f64)
        }),
        _ => Err(ExecutionError::InvalidExpression {
            message: "AVG finalize over non-AVG state".to_string(),
        }),
    }
}

fn avg_state_array(state: &AggState) -> Result<ArrayRef, ExecutionError> {
    let value = avg_finalize(state)?;
    Ok(Arc::new(Float64Array::from(vec![value])))
}

fn avg_column_from_groups(
    groups: &BTreeMap<Vec<GroupValue>, Vec<AggState>>,
    agg_idx: usize,
) -> Result<ArrayRef, ExecutionError> {
    let values: Vec<Option<f64>> = groups
        .values()
        .map(|states| avg_finalize(&states[agg_idx]))
        .collect::<Result<_, _>>()?;
    Ok(Arc::new(Float64Array::from(values)))
}
```

`Executor::execute_logical` legacy unbound AVG arm —
single `AvgFloat` accumulator throughout (favorable
asymmetry vs MIN/MAX's per `exec.rs:311-358`):

```rust
AggregateFunction::Avg => {
    let mut state = AggState::AvgFloat { total: 0.0, count: 0 };
    for batch in &input_batches {
        let arr = self.evaluate_expr(&args[0], batch)?;
        for row in 0..batch.num_rows() {
            if let Some(value) = sum_value(&arr, row)? {
                add_avg_value(&mut state, value)?;
            }
        }
    }
    results_cols.push(avg_state_array(&state)?);
    results_fields.push(arrow::datatypes::Field::new(
        "avg",
        DataType::Float64,
        true,
    ));
}
```

No "cannot infer output type from empty unbound input"
error path — AVG's Float64-uniform output makes this
trivially correct for empty input (one row, NULL).

`bound_global_aggregate` AVG arm:

```rust
AggregateFunction::Avg => {
    let mut state = avg_state_for_data_type(&call.args[0].data_type())?;
    for batch in input_batches {
        let arr = self.evaluate_bound_expr(&call.args[0], batch)?;
        for row in 0..batch.num_rows() {
            if let Some(value) = sum_value(&arr, row)? {
                add_avg_value(&mut state, value)?;
            }
        }
    }
    results_cols.push(avg_state_array(&state)?);
}
```

`bound_grouped_aggregate` per-row accumulation loop
extension:

```rust
AggState::AvgInt { .. } | AggState::AvgFloat { .. } => {
    if let Some(value) = sum_value(array, row)? {
        add_avg_value(state, value)?;
    }
}
```

Column emission loop AVG arm:

```rust
AggregateFunction::Avg => {
    columns.push(avg_column_from_groups(&groups, agg_idx)?);
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
| AggState::MaxInt64 { .. }
| AggState::AvgInt { .. }
| AggState::AvgFloat { .. } => 0,
```

cfg(test) field-name dispatch:

```rust
AggregateFunction::Avg => "avg",
```

### Step 5: planner tests in `group_by_test.rs`

- `avg_aggregate_lowers_in_scalar_and_grouped_projections`:
  scalar `SELECT AVG(amount) FROM sales` and grouped
  `SELECT region, AVG(amount), AVG(id) FROM sales GROUP
  BY region HAVING AVG(amount) > 7 ORDER BY AVG(id) DESC
  LIMIT 2` both lower to expected plan shapes.
- `avg_aggregate_with_no_arg_rejects`:
  `SELECT AVG() FROM sales` rejects via
  `"AVG requires one argument"`.
- `avg_aggregate_with_star_argument_rejects`:
  `SELECT AVG(*) FROM sales` rejects via the existing
  `lower_arg` Wildcard reject path.
- `avg_aggregate_with_distinct_rejects`:
  `SELECT AVG(DISTINCT amount) FROM sales` rejects via
  inherited ADR 011 `"DISTINCT is not supported"`.
- `avg_aggregate_with_non_numeric_input_rejects`:
  `SELECT AVG(region) FROM sales` (and Utf8 / Boolean /
  Date / Timestamp variants) rejects at the binder via
  `"AVG expects numeric input, got {type}"`.
- `avg_aggregate_having_ambiguity_is_rejected`:
  `SELECT AVG(amount), AVG(amount) FROM sales HAVING
  AVG(amount) > 0` rejects with
  `"ambiguous HAVING aggregate-call"`.
- `avg_aggregate_order_by_not_in_select_rejects`:
  `SELECT SUM(amount) FROM sales ORDER BY AVG(amount)`
  rejects with `"ORDER BY aggregate-call must appear in
  SELECT list"`.
- `avg_aggregate_having_qualified_reference_rejects`:
  `SELECT AVG(amount) FROM sales HAVING t.amount > 0`
  rejects with `"qualified HAVING column references are
  not supported"`.

### Step 6: executor tests in `group_by_test.rs`

- `scalar_avg_integer_executes`: Int32 / Int64 fixture
  produces fractional Float64 result (e.g., `AVG([1, 2,
  3])` = `2.0`; `AVG([1, 2])` = `1.5`).
- `scalar_avg_executes_with_one_row_output`:
  `SELECT AVG(amount) FROM sales` returns one row.
- `grouped_avg_executes_per_region`:
  `SELECT region, AVG(amount) FROM sales GROUP BY
  region` returns per-region AVG; mixed-NULL groups
  handled correctly.
- `scalar_avg_empty_input_returns_null_explicit_vec_none`:
  empty fixture → assert `vec![None]` as output (per
  Codex's explicit pin).
- `scalar_avg_all_null_input_returns_null`: all-NULL
  fixture → `vec![None]`.
- `grouped_avg_empty_input_returns_zero_rows`.
- `grouped_avg_all_null_group_emits_null`: group with
  only NULL values → group present in output, value
  NULL.
- `scalar_avg_float_input_executes`: Float32 / Float64
  column; AVG output Float64.
- `scalar_avg_mixed_null_input_executes`: null values
  in the input; nulls excluded from total AND count.
- `scalar_avg_sticky_nan_returns_nan`: Float64 fixture
  with a NaN value → result is NaN (Gemini's hard-
  gate test).
- `grouped_avg_sticky_nan_per_group`: groups with NaN
  → that group's AVG is NaN; other groups unaffected.
- `scalar_avg_int_with_float_input_rejects_at_runtime`:
  pathological state-vs-value mismatch (defensive test
  against `add_avg_value`'s `AvgInt × Float` hard-
  error arm).
- `scalar_avg_sql_matches_hand_built_aggregate_plan`:
  digest parity between SQL and hand-built
  `LogicalPlan::Aggregate { aggr_expr: [(Avg, ...)] }`.
- `grouped_avg_having_order_by_sql_matches_hand_built_plan`:
  digest parity for grouped AVG + HAVING + ORDER BY +
  LIMIT.

### Step 7: distributed fixture in `distributed_process_test.rs`

- `ordered_grouped_avg_process_digest_is_stable_across_worker_counts`:
  fixture-driven test for `SELECT val, AVG(val) FROM
  fixture GROUP BY val ORDER BY AVG(val) DESC LIMIT 5`
  across two- and four-worker process configurations.
  Asserts byte-identical normalized digest and unique
  worker count matches pool size.

### Step 8: architecture-test changes in `architecture_test.rs`

Rename `adr016_having_guards_are_narrow` to
`aggregate_function_enum_guards_are_narrow`. Update
variant count assertion from 7 → 8. Add new table row:

```rust
("AggregateFunction::Avg", "\"avg\"", "Field::new(\"avg\","),
```

The other ADR-019-specific assertions
(`adr019_min_max_aggregate_kernels_guards_are_narrow`)
stay unchanged — they pin MIN/MAX-specific guards
that ADR 020 doesn't disturb.

Verification: `cargo test --locked -p toron`.

### Step 9: bench artifact

Lightweight artifact at
`crates/toron/plans/impl/020_avg_aggregate_kernel_bench_results.md`
covering:

- 5 new `storage_execution.rs` cases:
  `avg_nullable_i32`, `avg_i64`, `avg_float64`,
  `avg_all_null_i32`, `group_avg`. The `avg_float64`
  case is the first Float64-coverage case under the
  kernel-catalog-widening pattern.
- Throughput numbers relative to nearby SUM / MIN /
  MAX rows on the same fixture. No minimum-speedup
  gate — the artifact exists because new stable
  kernels need bench evidence per ADR 019 precedent.
- Brief environment summary (cargo / rustc / arrow
  versions; commit hash).

## Phase 2 — Fragmenter inspection (zero-code)

`crates/toron/src/distributed/fragment.rs`: no changes.
AVG inherits ADR 011's grouped-aggregate fragmenter
routing and ADR 018's scalar routing. The detectors
match on `LogicalPlan::Aggregate` structurally without
kernel-kind inspection.

## Phase 3 — Verification across layers

**Binder coverage** (`group_by_test.rs`):
- AVG's Float64 output composes with `bind_comparison`
  Float64-from-Int32 / Int64 coercion for HAVING /
  ORDER BY predicates.
- `avg_output_data_type` accepts the full SUM-compatible
  numeric type set; rejects others at the binder via
  `BindingError::TypeMismatch`.

**Executor coverage** (`group_by_test.rs`):
- Per Step 6's executor test additions.
- Empty-input contract pinned via explicit `vec![None]`
  assertions.
- Overflow discipline mirrors SUM (`checked_add` +
  explicit error).
- Sticky-NaN pinned via hard-gate test (Gemini's
  requirement).

**Distributed coverage** (`distributed_process_test.rs`):
- Per Step 7's grouped ordered AVG fixture.

**In-process digest parity** (`group_by_test.rs`):
- Per Step 6's `_sql_matches_hand_built_plan` tests.

**Bench evidence**:
- Per Step 9's bench artifact.

## Phase 4 — Documentation and roadmap

- ADR + impl plan promoted to
  `plans/adr/020_avg_aggregate_kernel.md` and
  `plans/impl/020_avg_aggregate_kernel.md` with status
  matching the actual lifecycle: "Promoted. Implementation
  pending" at plan promotion, and "Promoted + implemented"
  only after code, tests, and bench artifact land.
- Bench artifact at
  `plans/impl/020_avg_aggregate_kernel_bench_results.md`.
- `plans/wip/roadmap.md` ADR 020 entry retires from
  "candidates" into active/promoted work, with
  "Implemented" reserved for the post-code update. The
  entry keeps the cycle-property note ("second instance
  of kernel-catalog widening preserving source-level
  boundary clarity at the
  catch-all reject"); ADR 021 candidates section
  seeded with kernel-registry refactor as lead, post-
  aggregate projection (ADR 022) as strong runner-up,
  float / Utf8 MIN/MAX, ADR 02X (Wendelon AGG consumer
  wiring), Kahan-Neumaier stability.
- Auto-memory `adr-015-cycle-shape` updates to record
  that ADR 020 is the second kernel-catalog-widening
  instance (after ADR 019 MIN/MAX); cycle's property
  continues at the kernel surface.

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
cargo bench -p toron --bench storage_execution -- --quick
```

## Risks + mitigations

- **R1: SUM/MIN/MAX/AVG arm duplication drift.** The
  binder arm's parallel `if matches!` blocks have
  grown from 2 (ADR 019) to 3 (ADR 020). Mitigation:
  ADR 021 ratifies the kernel-registry refactor at
  the moment it earns its keep.

- **R2: `AggState` variant growth.** ADR 019 went 3 →
  7; ADR 020 goes 7 → 9. Mitigation: AggState stays
  `Copy` (architecture-test asserts); ADR 021's
  refactor decision can introduce the trait shape
  if useful.

- **R3: Sticky-NaN behavior surprises users.** Some
  users may expect NaN-as-NULL or NaN-as-error.
  Mitigation: explicit hard-gate regression test
  pins V1 behavior. Future ADR can adopt different
  semantics with deliberate decision.

- **R4: Precision loss in worst-case Float64 AVG.**
  AVG of many-small-plus-outlier inputs can lose ~3-4
  decimal digits in worst case. Mitigation: V1 §1
  exclusion names this as acceptable cost; Kahan-
  Neumaier stability ADR composes after.

- **R5: Architecture-guard rename cascade.** Renaming
  `adr016_having_guards_are_narrow` may cascade if
  other places reference the name. Mitigation: grep
  before commit (`grep -rn
  "adr016_having_guards_are_narrow" crates/toron/`);
  update references.

- **R6: Wendelon AGG protocol drift.** Wendelon-side
  may evolve `SignedAverageParts` shape. Mitigation:
  ADR 020 does NOT engage the AGG protocol. ADR 02X
  is the cross-ring ADR.

- **R7: `bind_comparison` Float64-coercion gap.**
  ADR 016 inheritance assumes Float64 ↔ Int32 / Int64
  coercion is supported. Verification: existing
  `bind_comparison` numeric-coercion tests cover
  this; ADR 020's HAVING composition test surfaces
  any gap as regression.

- **R8: AVG overflow discipline mirroring SUM.** AVG
  inherits SUM's `checked_add` + explicit
  `ExecutionError::Kernel`. If SUM later moves to a
  different discipline, AVG must follow in the same
  ADR — ADR 011's SUM and ADR 020's AVG share the
  kernel-overflow contract.

- **R9: Float64 precision loss for large integer
  averages.** AVG of integers with total > 2^53 loses
  precision in the Float64 finalization. Acceptable
  V1 cost (V1 §2 exclusion).

- **R10: Wendelon-side AGG protocol post-planning
  drift.** Re-verify (`grep "SignedAverageParts"
  crates/wendelon/src/schema/aggregate.rs`)
  immediately before promotion commit.

- **R11: Gemini's bundling concern.** The maintenance
  cliff Gemini named is real — 9-variant `AggState`
  with parallel match arms across 8+ dispatch sites
  is friction. Mitigation: ADR 021's kernel-registry
  refactor is the named follow-on; the dissent is
  recorded in §V1 §9 of ADR 020 so ADR 021's review
  ratifies the refactor with full council
  awareness.

## Definition of done

- Single commit (`feat:`): one enum variant, one
  planner arm, one binder arm extension, two
  executor arm extensions (legacy unbound + bound),
  two new `AggState` variants, six new helpers
  (`avg_output_data_type`, `avg_state_for_data_type`,
  `add_avg_value`, `avg_finalize`, `avg_state_array`,
  `avg_column_from_groups`), one new SQL planner
  helper (`lower_avg_aggregate`), architecture-guard
  rename + update, optional narrow `SumValue` rename.
- Planner accepts AVG in scalar and grouped contexts;
  HAVING and ORDER BY compose via inherited
  substrate.
- All V1 exclusion items §1-§12 reject via inherited
  or new narrow named paths.
- Empty-input contract pinned: `count == 0 → NULL`
  (scalar one-row NULL via explicit `vec![None]`;
  grouped zero rows; all-NULL group emits NULL).
- Float64-uniform output for all SUM-compatible
  numeric input types; divergence from Postgres-
  style Decimal-by-default named in §V1 §2.
- Overflow discipline mirrors SUM exactly:
  `checked_add` + explicit `ExecutionError::Kernel`;
  `AvgFloat` mixed-type support; `AvgInt × Float`
  hard error.
- Sticky-NaN behavior pinned by hard-gate
  regression test.
- Both `init_states` (cfg(test)) and
  `init_bound_states` (production) gain AVG arms.
- Legacy unbound path: single-state `AvgFloat`
  accumulator; no empty-input error.
- In-process digest parity for scalar and grouped
  AVG.
- Multi-process digest parity for grouped ordered
  AVG across two- and four-worker fixtures.
- Architecture-test rename:
  `adr016_having_guards_are_narrow` →
  `aggregate_function_enum_guards_are_narrow`;
  variant count 8 confirmed; new table entry for
  AVG; cross-ring absence guard preserved.
- Bench artifact lands at
  `plans/impl/020_avg_aggregate_kernel_bench_results.md`
  with 5 new cases and throughput numbers. No
  minimum-speedup gate.
- Wendelon AGG protocol consumer surface
  untouched (ADR 02X is the cross-ring ADR).
- SUM / MIN / MAX kernel substrates broadly
  untouched (refactor decision deferred to ADR
  021). Permitted narrow rename:
  `SumValue → NumericValue` (or similar) if AVG
  shares the helper.
- Full Toron + Wendelon workspace test suites
  green.
- Memory file `adr-015-cycle-shape` updated:
  ADR 020 is the second kernel-catalog-widening
  instance.
- Roadmap entry moves from "candidates" into active
  ADR 020 work during promotion, then into "Implemented"
  after code, tests, and bench artifact land; "ADR 021
  candidates" section seeded with kernel-registry refactor
  as lead, post-aggregate projection (ADR 022) as strong
  runner-up, float / Utf8 MIN/MAX, ADR 02X (Wendelon AGG
  consumer wiring), Kahan-Neumaier stability.
