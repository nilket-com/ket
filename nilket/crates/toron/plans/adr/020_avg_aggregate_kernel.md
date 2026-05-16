# ADR 020: AVG Aggregate Kernel

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Companion implementation plan:**
`plans/impl/020_avg_aggregate_kernel.md`

## Status

Drafted. Awaiting promotion commit.

Second kernel-catalog widening under the post-ADR-018
cycle. Three-way council convergence reached: AVG joins
`COUNT`, `SUM`, `MIN`, `MAX` as the fifth stable aggregate
kernel and completes the basic analytical set. Three load-
bearing council positions ratified:

- **AVG ships alone; kernel-registry refactor deferred to
  ADR 021.** Two of three voices (Claude / Codex) voted to
  split; Gemini argued for bundling the substrate refactor
  into the AVG commit as a hard gate ("the maintenance
  cliff justifies bundling"). The split position carries
  ADR 019's precedent (MIN/MAX shipped as parallel arms,
  refactor deferred) and the iter-2 finding that a
  trait-dispatch refactor is *organizational* (~30-50% body-
  line reduction with ~40 enum match arms preserved as
  one-line delegations), not line-count-reductive. The
  trait-shape ratification deserves its own council round.
  A narrow exception per Codex: `SumValue` may be renamed
  to a neutral numeric-extraction helper *if* AVG genuinely
  shares it; broader registry work stays out.
- **AVG's accumulator is `(total, count)` with Float64-
  uniform output.** Multi-state aggregator — the canonical
  partial-state shape. Field name `total` matches existing
  `SumInt { total: i128, seen: bool }` convention for
  cross-kernel consistency. Wendelon-side
  `SignedAverageParts { sum: i128, count: u64, overflow:
  bool }` is *structurally equivalent* to Toron's
  `AggState::AvgInt { total: i128, count: u64 }` modulo the
  saturation indicator (Toron's `checked_add` + explicit
  `ExecutionError::Kernel` is the semantic equivalent of
  Wendelon's overflow bit). The alignment is noted in the
  handoff but **not pinned as a cross-ring contract** —
  ADR 019 iter-3 precedent. ADR 02X engages the cross-ring
  contract.
- **AVG's legacy unbound path uses a single `AvgFloat`
  accumulator regardless of input type.** Float64-uniform
  output means the unbound path doesn't need MIN/MAX's
  "infer output type from first batch" dance. AVG's
  unbound arm matches the bound path's `count == 0 →
  NULL` semantics naturally; no "cannot infer output type
  from empty unbound input" error path.

ADR 020 also pins three smaller decisions:

- **Sticky-NaN behavior for Float64 AVG** — naive f64
  arithmetic propagates NaN through the running total;
  any NaN input produces a NaN output. Hard-gate test
  (per Gemini) explicitly asserts this behavior.
- **Architecture guard renamed.**
  `adr016_having_guards_are_narrow` becomes
  `aggregate_function_enum_guards_are_narrow` — the guard
  is now effectively the catalog-wide architecture guard
  across all kernel-widening ADRs.
- **Lightweight bench artifact** extends
  `storage_execution.rs` with 5 new cases
  (`avg_nullable_i32`, `avg_i64`, `avg_float64`,
  `avg_all_null_i32`, `group_avg`). The `avg_float64`
  case is the first Float64-coverage case under the
  kernel-catalog-widening pattern (MIN/MAX deferred
  Float64 to its NaN-policy ADR). ADR 019 precedent: new
  stable kernel; bench artifact lands; no minimum-speedup
  gate.

Load-bearing antecedents: ADR 010 (terminal `Sort`-over-
gather-`Exchange` wrap), ADR 011 (`Aggregate` operator
surface, projection accounting, SUM/COUNT kernel
substrate, `AggState` enum, P0.5 single-stage scalar
shape, hash-partitioned grouped exchange), ADR 015
(terminal-gather shape), ADR 016 (HAVING walker
substrate, four-way ambiguity classification,
`bind_comparison` numeric-literal coercion), ADR 017
(`LoweredSelect` seam, shape-neutral walker,
`ResolveOutcome`), ADR 018 (scalar-aggregate surface,
`GroupedAggregateScope::from_projection(&[], &aggr_expr)`,
inlined HAVING-attachment in the global-aggregate
branch), ADR 019 (MIN/MAX kernel-catalog widening,
identity-type output, `Option<T>` state pattern, cycle's
source-level boundary clarity property extended to the
kernel surface).

## Goal

Enable `AVG(<numeric column>)` in scalar and grouped
aggregate SELECT projections, HAVING predicates, and ORDER
BY keys. The canonical analytical patterns:

```sql
SELECT AVG(amount) FROM sales
```

```sql
SELECT region, AVG(amount) FROM sales GROUP BY region
```

```sql
SELECT region, AVG(amount) FROM sales
GROUP BY region
HAVING AVG(amount) > 100
ORDER BY AVG(amount) DESC LIMIT 10
```

Today the first rejects at `sql_planner.rs:556`
(`lower_aggregate_function`'s
`_ => Err(unsupported("unsupported aggregate function"))`
catch-all for `"AVG"`). The latter two compound the same
rejection through the grouped-projection path. ADR 020
removes AVG from the catch-all's scope and adds one narrow
named arm.

## Scale boundary

**Primary:** Toron planner + binder + executor.
**Aggregate-kernel-catalog surface.**

In scope:

- `AggregateFunction` enum widening at `ir.rs:107-116`
  (7 → 8 variants — `Avg` added).
- `lower_avg_aggregate` arm in `sql_planner.rs` parallel
  to `lower_sum_aggregate`.
- `bind_aggregate_call` AVG arm in `binder.rs:1076-1122`;
  new `avg_output_data_type` helper paralleling
  `sum_output_data_type`.
- `aggregate_field` AVG arm at `binder.rs:1322+`:
  `Field::new("avg", DataType::Float64, true)`.
- Two new `AggState` variants at `exec.rs:2849+`:
  `AvgInt { total: i128, count: u64 }` and
  `AvgFloat { total: f64, count: u64 }`. `AggState`
  **stays `Copy`** (both variants are stack-only).
- `Executor::execute_logical` legacy unbound AVG arm —
  single `AvgFloat` accumulator throughout (no first-batch
  type dance, no empty-input error).
- `bound_global_aggregate` and `bound_grouped_aggregate`
  AVG arms; supporting helpers `avg_state_for_data_type`,
  `add_avg_value`, `avg_finalize`, `avg_state_array`,
  `avg_column_from_groups`.
- `GroupedAggregateSignature::Avg(ColumnRef)` variant;
  `grouped_aggregate_signature` arm;
  `aggregate_output_name` returning `"avg"`.
- Architecture-guard updates: variant count 7 → 8; new
  table row for AVG; test renamed from
  `adr016_having_guards_are_narrow` to
  `aggregate_function_enum_guards_are_narrow`.
- Planner / binder / executor / scope / ambiguity test
  coverage in `group_by_test.rs`.
- One distributed digest-parity multi-process fixture in
  `distributed_process_test.rs` (grouped ordered AVG
  across two- and four-worker pools).
- Sticky-NaN regression test pinning Float64 NaN
  propagation through AVG.
- Bench artifact extending `storage_execution.rs` with 5
  new cases.
- Bench results doc at
  `plans/impl/020_avg_aggregate_kernel_bench_results.md`.

Explicitly unchanged:

- ADR 010's `Sort` operator surface and unconditional
  `Sort + Exchange(1, [])` wrap.
- ADR 011's `Aggregate` operator surface, projection-
  shape contract, P0.5 single-stage scalar shape, hash-
  partitioned grouped exchange, SUM/COUNT kernel
  substrate.
- ADR 015's nested-shape detector, ADR 016's
  `Filter`/`EvaluateInToron` placement, ADR 017's
  `LoweredSelect` seam, ADR 018's scalar-surface
  `GroupedAggregateScope::from_projection(&[], &aggr_expr)`
  pattern, ADR 019's MIN/MAX kernel substrate.
- Walker, scope, message helpers, four-way ambiguity
  classification — all generalize to AVG without
  modification because `Avg(ColumnRef)` slots into
  `GroupedAggregateSignature` mechanically.
- `bind_comparison` numeric-literal coercion (ADR 016):
  composes with AVG's Float64 output against integer
  literal RHS in HAVING.
- `Filter` / `Sort` operator surfaces; `JoinKind`,
  `JoinStrategy`, scheduler-barrier semantics, `TaskSpec`,
  `TaskFailureKind`, `BudgetDimension`, `MessageKind`,
  `TaskOutputBinding` variants; table-query wire frame;
  runtime-filter descriptor; PBC bytecode.
- Wendelon AGG protocol consumer surface (separate future
  ADR — ADR 02X).
- `AggState` derives — **stays `Copy`** (AVG variants are
  stack-only; matches ADR 019 MIN/MAX precedent).
- SUM / MIN / MAX kernel substrate — untouched except for
  a permitted narrow rename of `SumValue` to a neutral
  numeric-extraction helper *if* AVG genuinely shares
  the helper. Broader kernel-registry refactor is **ADR
  021's scope**.

No new IR variant beyond the enum widening. No new
fragmenter recognition site. No new shuffle binding. No
new stage role. No new failure kind. No new wire surface.
No new cross-ring engagement.

## Cycle property at the kernel surface

ADR 019 extended the cycle's source-level boundary clarity
property to the kernel-catalog surface — every SQL
aggregate function either lowers to a known kernel arm or
rejects via a deterministic planner reject. ADR 020 is the
second instance.

AVG removes itself from the `sql_planner.rs:556` catch-all
and adds one named arm. Architecture-guard delta:

- **Variant count (was 7, now 8):** the architecture
  guard pinning the variant count is renamed from
  `adr016_having_guards_are_narrow` to
  `aggregate_function_enum_guards_are_narrow`. The guard
  is no longer ADR-016-specific; it's the catalog-wide
  invariant across kernel-widening ADRs.
- **Table entry (new):** `Avg` / `"avg"` /
  `Field::new("avg",` row at `architecture_test.rs:485+`.
- **Absence (preserved):** the
  `_ => Err(unsupported("unsupported aggregate function"))`
  catch-all at `sql_planner.rs:556` remains for future
  kernels (Float/Utf8 MIN/MAX next).

## Context

The catch-all reject site:

- `SqlPlanner::lower_aggregate_function` at
  `sql_planner.rs:526-556` — match arm at line 556 rejects
  any unknown function name. `AVG(...)` queries currently
  fail here.

The binder dispatch sites:

- `bind_aggregate_call` at `binder.rs:1065-1122` — five
  arms (`CountAll`, `Count | Sum | Min | Max`,
  `LinearRegress`, `ApproxQuantile`); AVG joins the
  numeric arm.
- `aggregate_field` at `binder.rs:1322+` — six arms; AVG
  adds a seventh.
- `sum_output_data_type` at `binder.rs:1339+` and
  `min_max_output_data_type` at `binder.rs:1399+` —
  numeric-input type-check helpers. AVG uses a parallel
  `avg_output_data_type` returning Float64 for all
  numeric inputs.

The executor dispatch sites:

- `Executor::execute_logical` legacy unbound at
  `exec.rs:285-358` — Count, Sum, Min/Max bundled arm,
  LinearRegress (gated); AVG adds an arm.
- `bound_global_aggregate` at `exec.rs:870-980` — five
  arms; AVG adds a sixth.
- `bound_grouped_aggregate` at `exec.rs:1060-1260` — five
  arms across per-row accumulation, column emission, and
  field-name dispatch; AVG adds three (one per site).
- `AggState` enum at `exec.rs:2849-2862` — seven variants
  (`Count`, `SumInt`, `SumFloat`, `MinInt32`, `MinInt64`,
  `MaxInt32`, `MaxInt64`); AVG adds two (`AvgInt`,
  `AvgFloat`).
- `add_sum_value` at `exec.rs:2990+` — sum-accumulation
  discipline. AVG uses a parallel `add_avg_value` with
  matching `checked_add` discipline.

The Wendelon AGG protocol (noted but NOT engaged):

- `crates/wendelon/src/schema/aggregate.rs:111-115`
  declares `AggregateFunction::SignedAverageParts {
  column }` and `UnsignedAverageParts { column }` with
  partial state `(sum: i128 / u128, count: u64,
  overflow: bool)`. Toron-side has no AGG consumer at
  `crates/toron/src/storage.rs`. ADR 02X engages this
  surface.

## Decision

### §1. Enum widening: `AggregateFunction::Avg`

Add the variant at `ir.rs:107-116`:

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

The renamed architecture guard
`aggregate_function_enum_guards_are_narrow` updates to
variant count 8.

### §2. SQL planner arm

Insert into `lower_aggregate_function`:

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

`aggregate_function_name` adds `"AVG" => Some("AVG")`.

`aggregate_output_name` arm:

```rust
AggregateFunction::Avg => "avg",
```

`GroupedAggregateSignature` enum gains:

```rust
Avg(ColumnRef),
```

`grouped_aggregate_signature` arm (inline match destructure,
parallel SUM/MIN/MAX):

```rust
AggregateFunction::Avg => match args {
    [Expr::Column(column)] => Some(GroupedAggregateSignature::Avg(column.clone())),
    _ => None,
},
```

### §3. Binder arm

`bind_aggregate_call` at `binder.rs:1076-1122` extends the
`Count | Sum | Min | Max` pattern to include `Avg`. AVG's
validation mirrors SUM's (single argument, numeric input
check via `avg_output_data_type`):

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

`aggregate_function_keyword` at `binder.rs:1402+` extends:

```rust
fn aggregate_function_keyword(function: &AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
        AggregateFunction::Avg => "AVG",
        _ => "aggregate",
    }
}
```

`aggregate_field` adds:

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

AVG output type is **Float64-uniform** regardless of input
numeric width — diverging from Postgres / MySQL's Decimal-
by-default for integer inputs, matching DuckDB / SQLite.
Toron has no Decimal substrate; Decimal output for integer
AVG awaits the V1.x Decimal ADR. Integer totals and counts
up to 2^53 are exactly representable before division;
fractional averages still round to Float64. The numeric input
domain matches SUM (`sum_output_data_type` at
`binder.rs:1339-1352`) — all signed and unsigned integer
widths plus Float32/Float64.

### §4. Executor — `AggState` variants and accumulation arms

Two new `AggState` variants at `exec.rs:2849+`:

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

Field name `total` matches the existing `SumInt { total:
i128, seen: bool }` convention for cross-kernel naming
consistency.

**`AggState` keeps `Copy`** — `AvgInt` is `(i128, u64)`,
`AvgFloat` is `(f64, u64)`, both stack-only. Matches
ADR 019 MIN/MAX precedent.

The `i128` / `u64` widths inherit from ADR 011's
`SumInt.total: i128` substrate. **The widths align with
Wendelon's `SignedAverageParts { sum: i128, count: u64 }`
shape — structural equivalence with different error
discipline.** Toron's `checked_add` + explicit
`ExecutionError::Kernel` is the semantic equivalent of
Wendelon's overflow bit. **Not pinned as cross-ring
contract.** ADR 02X engages the cross-ring contract.

Both `init_states` (cfg(test)) and `init_bound_states`
(production) gain AVG arms. The test-only unbound default
may use `AvgInt`; the production bound path should validate
the bound argument type defensively:

```rust
// init_states (cfg(test)):
AggregateFunction::Avg => AggState::AvgInt { total: 0, count: 0 },

// init_bound_states (production):
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
```

`add_avg_value` matches SUM's overflow discipline exactly:
`checked_add` returning explicit `ExecutionError::Kernel`
on overflow (not `saturating_add`); `AvgFloat` accepts
`SumValue::Int` and casts to f64 (matching `add_sum_value`'s
mixed-type support); `AvgInt` reaching `SumValue::Float`
is a hard error (matching SUM's defensive pattern).

**Sticky-NaN semantics:** Float AVG accumulates with
ordinary `f64 += v` arithmetic. If any input is `NaN`, the
running total becomes `NaN` and the finalization
`total / count` is `NaN`. Negative zero follows ordinary
`f64` arithmetic. This is intentional and pinned by a
hard-gate regression test (§"Promotion evidence" §3).
No special NaN handling is introduced.

Finalization:

```rust
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

`avg_state_array` and `avg_column_from_groups` have simpler
signatures than the SUM equivalents (no `data_type`
parameter) because AVG output type is Float64-uniform per
§V1 §1. This is a *deliberate parallel-not-shared* design,
consistent with ADR 019's MIN/MAX divergence from SUM's
`(state, data_type)` signature. Refactor into shared
helpers is deferred to ADR 021 (kernel-registry refactor).

`Executor::execute_logical` legacy unbound AVG arm —
single-state, no first-batch dance:

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

Single `AvgFloat` accumulator regardless of input — no
empty-input error path (contrast MIN/MAX at
`exec.rs:311-358` which errors on empty unbound input
because output type depends on input). AVG's Float64-
uniform output makes the legacy unbound path structurally
simpler.

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

Column emission loop adds:

```rust
AggregateFunction::Avg => {
    columns.push(avg_column_from_groups(&groups, agg_idx)?);
}
```

The Count emission arm extends to include the new variants
in the 0-default branch:

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

Field-name dispatch in the cfg(test) `grouped_aggregate`
arm gains `AggregateFunction::Avg => "avg"`.

### §5. Scope, ambiguity, and clause-neutral walker — inheritance

`GroupedAggregateScope::from_projection`,
`GroupedAggregateScope::resolve`, `ResolveOutcome`, four-
way ambiguity classification,
`rewrite_grouped_aggregate_calls`,
`resolve_grouped_aggregate_reference`,
`aggregate_call_not_in_select_message`,
`ambiguous_aggregate_call_message`,
`reject_qualified_having_column_references` — all
unchanged. AVG composes with the existing substrate the
same way SUM/MIN/MAX do. The `Avg(ColumnRef)` signature
variant slots in mechanically.

### §6. Empty-input contract

- **Scalar:** `SELECT AVG(empty_table.col)` emits one row
  with NULL via `count == 0 → None` in `avg_finalize`.
- **Grouped:** `SELECT k, AVG(v) FROM empty GROUP BY k`
  emits zero rows (no groups exist).
- **All-NULL input** (scalar): emits one row with NULL.
- **All-NULL group** (grouped): emits that group with
  NULL.

Matches MIN/MAX precedent (ADR 019 §6) and SUM's
inheritance. ADR 020 pins the contract in regression
tests with explicit `vec![None]` assertions, not merely
HAVING-dropped rows.

### §7. HAVING / ORDER BY composition — pure inheritance

`SELECT AVG(amount) FROM sales HAVING AVG(amount) > 100`
composes through the ADR 016 walker substrate.
`AVG(amount) > 100` lowers as
`Expr::Gt(Expr::column("avg"), Literal(Int32(100)))`;
`bind_comparison` numeric-literal coercion (ADR 016)
coerces the Int32 RHS to Float64. Zero new binder code.

`SELECT AVG(amount) FROM sales ORDER BY AVG(amount) DESC`
composes through the ADR 017 walker substrate against the
ADR 018-extended scope (`Some(scope)` for scalar
aggregate-projection queries). Zero new planner code
beyond the new signature variant.

### §8. No new surface

ADR 020 introduces no new IR variant beyond the enum
widening; no new `JoinKind`, `JoinStrategy`, `MessageKind`,
`TaskFailureKind`, `BudgetDimension`, `FilterDisposition`,
scheduler barrier kind, `TaskOutputBinding` variant; no
new wire frame, runtime-filter descriptor field, PBC
opcode; no new `bind_expr` match arm; no new planner
cost-model knob; no fragmenter recognition site; no new
architecture guard *class*; no cross-ring engagement.

## V1 exclusions

ADR 020-specific exclusions:

1. **Numerical precision loss in Float64 AVG of many-
   small-plus-outlier inputs is acceptable V1 cost.** AVG
   of 1M Float64 values can lose ~3-4 decimal digits in
   the worst case. DuckDB / SQLite ship naive AVG with
   the same trade-off. Toron V1 target ("First Query
   with Integrity") is correctness for analytical
   results, not numerical-analysis-grade precision.
   Kahan-Neumaier stability ADR composes across SUM and
   AVG together.
2. **Decimal output for integer AVG inputs.** Postgres /
   MySQL ship integer-AVG as Decimal-by-default; Toron
   has no Decimal substrate. V1.x Decimal ADR introduces
   the surface; an AVG-output-type refinement follows.
3. **`AVG(*)`** — rejected via `lower_arg`'s existing
   Wildcard reject; ADR 020 adds no special handling.
4. **`AVG(DISTINCT col)`** — inherited ADR 011 reject
   (`"DISTINCT is not supported"`).
5. **`AVG` over non-numeric inputs.**
   `avg_output_data_type` returns `None`; rejected via
   `"AVG expects numeric input, got {type}"`.
6. **Two-phase distributed AVG aggregation.** ADR 020
   ships AVG single-stage scalar / per-partition grouped
   under ADR 011's existing distribution shape. Two-phase
   partial/final aggregation is a separate future ADR
   (ADR 02X) that engages Wendelon's AGG consumer
   protocol.
7. **Storage-pushed AVG via Wendelon AGG protocol.**
   Wendelon ships `SignedAverageParts` /
   `UnsignedAverageParts`; Toron's `wendelon_client` does
   not expose them. ADR 02X wires the consumer.
8. **Aliased AVG output** (`SELECT AVG(amount) AS mean`).
   Inherits ADR 011's post-aggregate projection reject.
   Future post-aggregate-projection ADR (ADR 022) lifts.
9. **Kernel-registry refactor (shared dispatch helpers
   across SUM/MIN/MAX/AVG).** Deferred to ADR 021.
   Iter-2 finding 1: refactor is organizational, not
   line-count-reductive (~30-50% body-line reduction,
   ~40 match arms preserved as one-line delegations).
   The trait-shape ratification deserves its own council
   round. Gemini's iter-3 dissent — "the maintenance
   cliff justifies bundling" — is recorded; the
   precedent from ADR 019 (MIN/MAX shipped as parallel
   arms, refactor deferred) carried the council
   majority.
10. **AVG as a window function.** Window aggregates need
    their own decision matrix. Future ADR.

Inherited rejects (preserved verbatim):

11. **Computed aggregate-call arguments** — ADR 011
    reject.
12. **Experimental kernels under GROUP BY** —
    `lower_aggregate_function` rejects `LINEAR_REGRESS` /
    `APPROX_QUANTILE` in `Grouped` mode.

## Consequences

- The three canonical AVG patterns (scalar, grouped,
  grouped + HAVING + ORDER BY + LIMIT) lower, fragment,
  and execute end-to-end.
- Walker, scope, `ResolveOutcome`, four-way ambiguity
  classification, pre-pass, message helpers, and
  `bind_comparison` literal coercion — all generalize to
  AVG without modification.
- `AggregateFunction` enum: 7 → 8 variants.
- `AggState` enum: 7 → 9 variants (private executor
  type). `AggState` retains `Copy` (`AvgInt`, `AvgFloat`
  are stack-only).
- Architecture-guard table at
  `architecture_test.rs:485+` gains AVG row.
- Architecture-guard test renamed from
  `adr016_having_guards_are_narrow` to
  `aggregate_function_enum_guards_are_narrow` —
  reflects the catalog-wide role.
- `aggregate_output_name`, `aggregate_field`, and
  `aggregate_function_keyword` gain AVG arms.
- ADR 011's per-partition aggregation contract inherits:
  AVG executes single-stage scalar, per-partition
  grouped.
- Wendelon AGG protocol consumer wiring named as future
  ADR 02X; ADR 020 stays Toron-local. Structural
  equivalence between `AggState::AvgInt` and
  `SignedAverageParts` (modulo overflow handling) is
  noted in handoff.
- Sticky-NaN behavior pinned by hard-gate regression
  test.
- Second proof that the cycle's source-level boundary
  clarity property extends to the kernel-catalog
  surface. Catch-all reject at `sql_planner.rs:556`
  remains for Float/Utf8 MIN/MAX and future kernels.
- The kernel-dispatch duplication across binder /
  planner / executor (now 5 stable numeric kernels)
  earns the kernel-registry refactor; ADR 021 ratifies.

## Promotion evidence

Implementation promotion requires:

1. **Planner** —
   - Canonical patterns lower per §1-§5.
   - `AggregateFunction::Avg` variant added; variant
     count 8 confirmed.
   - V1 exclusion items §1, §3-§5, §8, §10-§12 reject via
     inherited or new narrow named paths.

2. **Binder / executor** —
   - Numeric-input type check via `avg_output_data_type`
     (accepts all integer widths and Float32/64).
   - HAVING comparison binds via inherited
     `bind_comparison` Float64 coercion.
   - ORDER BY rewrite binds via inherited ADR 017
     walker.
   - Overflow discipline matches SUM (`checked_add` +
     explicit `ExecutionError::Kernel`).
   - Empty-input contract pinned: scalar one-row NULL;
     grouped zero rows; all-NULL group emits NULL.
   - Legacy unbound path: single-state `AvgFloat`
     accumulator; no empty-input error.

3. **Sticky-NaN regression** —
   - Hard-gate regression test confirms `AVG([1.0, NaN,
     3.0])` produces `NaN` (scalar) and that a grouped
     query with any NaN in a group produces `NaN` for
     that group's average. Signed-zero ordinary `f64`
     behavior is also exercised.

4. **In-process** —
   - SQL result digest-matches a hand-built logical plan
     for scalar AVG, grouped AVG, and grouped AVG +
     HAVING + ORDER BY.

5. **Distributed** —
   - Grouped AVG `SELECT region, AVG(amount) FROM sales
     GROUP BY region ORDER BY AVG(amount) DESC LIMIT 5`
     produces byte-identical normalized output across
     two- and four-worker fixtures.

6. **Architecture guard** —
   - `aggregate_function_enum_guards_are_narrow`
     (renamed) pins variant count 8, new table entry
     for AVG, `AggState` `Copy` preserved, and cross-
     ring-absence (no `AggregateExpression` /
     `AggregateFunctionTag` symbols in Toron-side
     source).

7. **Bench artifact** —
   - `storage_execution.rs` gains 5 new cases:
     `avg_nullable_i32`, `avg_i64`, `avg_float64`,
     `avg_all_null_i32`, `group_avg`. The `avg_float64`
     case is the first Float64-coverage case under the
     kernel-catalog-widening pattern.
   - `plans/impl/020_avg_aggregate_kernel_bench_results.md`
     captures correctness fixtures and throughput
     numbers relative to nearby SUM / MIN / MAX rows.
     **No minimum-speedup gate.** Following ADR 019's
     precedent: new stable kernel; lightweight bench
     artifact.

## What not to do

- **Do not pin `AggState::AvgInt` widths as cross-ring
  contract.** Use `i128` / `u64` because ADR 011's
  precedent uses them; Wendelon-side `SignedAverageParts`
  alignment is structural equivalence with different
  error discipline, not a contract. ADR 02X pins the
  cross-ring contract when it engages.
- **Do not engage the Wendelon AGG consumer protocol.**
  `wendelon_client::TableQueryOptions` stays as-is.
  ADR 02X is the cross-ring ADR.
- **Do not bundle kernel-registry refactor into ADR 020.**
  Iter-2 finding 1: refactor is organizational, not
  reductive. Gemini's iter-3 dissent ("the maintenance
  cliff justifies bundling") is recorded; ADR 019
  precedent (MIN/MAX shipped as parallel arms; refactor
  deferred) carries the council majority. The trait-
  shape ratification belongs to ADR 021.
- **Do not introduce Kahan-Neumaier summation for AVG.**
  Inherited from ADR 011's SUM kernel; precision-loss
  V1 cost is acceptable per §V1 §1. Future Kahan
  stability ADR composes across SUM and AVG together.
- **Do not introduce Decimal output for integer AVG.**
  V1.x Decimal ADR concern. Float64-uniform output is
  the V1 contract; future Decimal output narrows.
- **Do not refactor the SUM, MIN, or MAX kernel
  substrates broadly.** ADR 020's AVG implementation is
  parallel to existing kernels (separate helpers
  `avg_state_for_data_type`, `add_avg_value`,
  `avg_finalize`, `avg_state_array`,
  `avg_column_from_groups`). A *narrow* exception is
  permitted: if AVG genuinely shares the
  `SumValue`-style numeric-extraction helper currently
  named for SUM, the helper may be renamed to a neutral
  numeric-extraction name. Broader unification of
  per-kernel helpers is **ADR 021's scope**.
- **Do not extract a shared
  `*_output_data_type` helper across kernels.** Each
  has different return logic (SUM widens int→Int64 /
  float→Float64; MIN/MAX is identity-typed Int32/Int64;
  AVG is Float64-uniform). ADR 021's kernel registry
  can unify if the trait shape supports it.
- **Do not use `saturating_add` for AVG accumulation.**
  Match SUM's `checked_add` + explicit
  `ExecutionError::Kernel` discipline. Overflow fails
  loudly; pathological inputs surface as errors, not as
  clamped garbage.
- **Do not retire the
  `_ => Err(unsupported(...))` catch-all at
  `sql_planner.rs:556`.** Future kernels (Float / Utf8
  MIN/MAX, graduated experimentals) still need it. The
  cycle's source-level boundary clarity property
  requires the catch-all stay until every kernel-
  catalog widening has landed.
- **Do not name AVG empty-input contract as "inherited
  from ADR 019."** AVG's empty contract derives from
  SUM / MIN / MAX semantics generically (`count == 0 →
  NULL` for AVG; `seen == false → NULL` for SUM;
  `value == None → NULL` for MIN/MAX). ADR 020 makes
  the contract explicit for AVG via `vec![None]`
  regression assertions.
- **Do not add special NaN handling.** Sticky-NaN
  follows ordinary `f64` arithmetic. Any future ADR
  that changes float-aggregate NaN semantics (e.g.,
  treat-NaN-as-NULL) is a deliberate semantic change
  with its own decision matrix.
- **Do not extend `aggregate_function_keyword` to cover
  all kernels.** The helper is local to MIN/MAX/AVG arm
  error formatting. Extending to SUM/COUNT would
  require ADR 021's refactor decision; for ADR 020,
  the keyword helper covers the kernels that share the
  parallel `if matches!` validation block.
- **Do not add a defensive `args.first()` None-arm in
  the AVG binder block.** ADR 019's post-iter-5
  simplification removed the equivalent for MIN/MAX
  (the top-level `args.len() != 1` check guarantees
  `args.first()` is `Some`); AVG follows the same
  simplified pattern. Direct `let arg = &args[0]`
  after the arity check.

## ADR 021 handoff

ADR 021 selection starts from the post-ADR-020 remainder:

- **Kernel-registry refactor.** Five stable numeric
  kernels (SUM, MIN, MAX, AVG, plus Count which is
  trivially handled). The dispatch duplication across
  8+ match sites earns refactor. Trait-dispatch shape
  proposal: `AggregateKernel` trait with `output_name`,
  `output_data_type`, `init_state`, `update`,
  `finalize_scalar`, `finalize_grouped` associated
  functions. Realistic net change is organizational
  (~30-50% body-line reduction; ~40 match arms
  preserved as one-line delegations), not line-count-
  reductive. Closest candidate to ADR 021's
  nomination. Gemini's preferred bundling position
  was recorded as ADR 020 dissent and carries forward
  as the nomination for ADR 021's lead.
- **Post-aggregate projection (ADR 022).** Naming-
  asymmetry signal (17 `GroupedAggregateScope` /
  `grouped_aggregate_scope` references; 12 used in
  scalar paths) is over legibility threshold. ADR 022
  renames into a broader output-name catalog and
  lifts the aliased / computed-expression / key-
  subset surface.
- **Wendelon AGG consumer wiring (ADR 02X).** Toron-
  side client wiring through to Wendelon's
  `AggregateExpression` schema; partial/final two-
  phase shape; lifts ADR 011/019/020's per-partition
  scalar ceiling. **The right home for two-phase
  aggregation work.** Inherits the structural-
  equivalence alignment between `AggState::AvgInt`
  and `SignedAverageParts`. Bench artifact required
  (cross-ring perf characteristic).
- **Float `MIN` / `MAX`.** ADR 019 V1 §1 deferred for
  NaN/signed-zero policy ratification. Smaller scope
  than AVG with a sharper semantic-contract question.
- **Utf8 `MIN` / `MAX`.** ADR 019 V1 §2 deferred for
  `AggState` Copy-refactor. Substrate refactor with
  cascading effects.
- **Kahan-Neumaier stability ADR.** Composes across
  SUM and AVG; MIN/MAX is order-independent and
  doesn't participate.
- **DISTINCT.** Own decision matrix; loosens ADR 011's
  zero-aggregate reject.
- **AVG as a window function.** Window aggregates need
  their own decision matrix.
- **GROUP BY over joins.** Brainstorm 003 seam
  (parked).
- **Kernel graduation** (`LinearRegress`,
  `ApproxQuantile`, `TimeBucket`). The bound/unbound
  parity gap at `exec.rs:912-916` and
  `exec.rs:1035-1039` remains evidence pressure;
  gated behind `allow_experimental_kernels = false`,
  does not promote as ADR 021 substitute.

**Code-evidence defects continue to outrank the
printed list when one surfaces.** Post-ADR-020 sweep
is the next council round's first task.

The roadmap's `ADR 021 candidates` section owns the
forward-looking detail.
