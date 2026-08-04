# ADR 019: MIN / MAX Aggregate Kernels

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Companion implementation plan:**
`plans/impl/0019_min_max_aggregate_kernels.md`

## Status

Promoted and implemented.

First kernel-catalog widening under the post-ADR-018 cycle.
Three-way council convergence reached after divergent
iter-5 positions: Claude proposed AVG with Float64-uniform
output, Codex proposed MIN-only with `Int32` / `Int64` /
`Utf8` domains, Gemini proposed `MIN` / `MAX` bundled with
`Int32` / `Int64` / `Float64` domains. The merged contract
takes the council majority on three load-bearing votes:

- **MIN + MAX bundled (2 of 3).** Wendelon-side `Min { column }`
  / `Max { column }` ship as parallel variants with
  identical signatures and identical accumulator state
  structure at `crates/wendelon/src/schema/aggregate.rs:94-125`.
  ADR 008's bundling precedent — "same algorithm, kind
  discriminates" — maps cleanly to MIN/MAX: "same
  accumulator shape, comparator discriminates." Codex's
  preference for MIN-only is honored as a substrate-
  evidence escape hatch (if the type-generic dispatch
  design proves to need a heavier substrate change, the
  bundle splits naturally), but the default is bundle.
- **Conservative type domain: `Int32` and `Int64` only
  (intersection).** Codex flagged that float `MIN` / `MAX`
  needs an explicit NaN and signed-zero policy ratified
  by ADR, not silently inherited from Arrow's IEEE 754
  ordering. Gemini's Float64 inclusion is correct in
  spirit but the policy ratification belongs in a follow-
  on ADR. Codex's Utf8 inclusion forces a substantial
  substrate refactor (`AggState` would lose `Copy`
  because `String` owns its buffer) — worth its own
  ratification round. The conservative intersection
  `Int32` + `Int64` preserves `AggState: Copy` and
  defers the policy decisions to dedicated ADRs.
- **Lightweight bench artifact required.** Codex's
  sharper reading of the roadmap rule: ADR 011's no-
  bench precedent applies because ADR 011 *made existing
  kernels reachable* for grouped SQL — it did not
  introduce new stable aggregate functions. ADR 019 does.
  The Wendelon-ADR-070 pattern's bench requirement
  applies, but the artifact is lightweight (no speed
  gate, no minimum-speedup requirement) — correctness
  evidence first, perf characterization second.

ADR 019 stays Toron-local. Wendelon already ships the AGG
pushdown protocol at `crates/wendelon/src/schema/aggregate.rs:94-125`
(`Min { column }`, `Max { column }`, `SignedAverageParts`,
`SignedSum`, etc.); Toron's `wendelon_client::TableQueryOptions`
does not yet expose aggregate pushdown. ADR 02X is the
named home for the cross-ring consumer wiring; ADR 019
does not engage it.

Load-bearing antecedents: ADR 010 (terminal `Sort`-over-
gather-`Exchange` wrap), ADR 011 (`Aggregate` operator
surface, projection accounting, kernel substrate, `AggState`
enum, P0.5 single-stage scalar shape, hash-partitioned
grouped exchange), ADR 015 (terminal-gather shape), ADR 016
(HAVING walker substrate, four-way ambiguity
classification, `bind_comparison` numeric-literal
coercion), ADR 017 (`LoweredSelect` seam, shape-neutral
walker, `ResolveOutcome`), ADR 018 (scalar-aggregate
surface, `GroupedAggregateScope::from_projection(&[],
&aggr_expr)`, inlined HAVING-attachment in the global-
aggregate branch).

## Goal

Enable `MIN(<integer column>)` and `MAX(<integer column>)`
in scalar and grouped aggregate SELECT projections, HAVING
predicates, and ORDER BY keys. The canonical analytical
patterns:

```sql
SELECT MIN(amount), MAX(amount) FROM sales
```

```sql
SELECT region, MIN(amount), MAX(amount) FROM sales GROUP BY region
```

```sql
SELECT region, MIN(amount), MAX(amount) FROM sales
GROUP BY region
HAVING MIN(amount) > 0
ORDER BY MAX(amount) DESC LIMIT 10
```

Today the first rejects at `sql_planner.rs:554`
(`lower_aggregate_function`'s
`_ => Err(unsupported("unsupported aggregate function"))`
catch-all for both `"MIN"` and `"MAX"`). The latter two
compound the same rejection through the grouped-projection
path. ADR 019 removes MIN and MAX from the catch-all's
scope and adds two narrow named arms.

## Scale boundary

**Primary:** Toron planner + binder + executor.
**Aggregate-kernel-catalog surface.**

In scope:

- `AggregateFunction` enum widening at `ir.rs:107-114`
  (5 → 7 variants — `Min` and `Max` added).
- `lower_min_aggregate` and `lower_max_aggregate` arms in
  `sql_planner.rs` parallel to `lower_sum_aggregate` at
  `sql_planner.rs:576-584`.
- `bind_aggregate_call` MIN/MAX arms in `binder.rs:1065-1129`;
  new `min_max_output_data_type` helper paralleling
  `sum_output_data_type` at `binder.rs:1339-1352`.
- `aggregate_field` MIN/MAX arms at `binder.rs:1322-1337`:
  output type matches input type (Int32 → Int32; Int64 →
  Int64), nullable.
- Four new `AggState` variants at `exec.rs:2740-2744`:
  `MinInt32 { value: Option<i32> }`,
  `MinInt64 { value: Option<i64> }`,
  `MaxInt32 { value: Option<i32> }`,
  `MaxInt64 { value: Option<i64> }`. AggState **stays Copy**.
- `bound_global_aggregate` and `bound_grouped_aggregate`
  MIN/MAX arms; supporting helpers
  `min_state_for_data_type`, `max_state_for_data_type`,
  `update_min_state`, `update_max_state`,
  `min_max_state_array`, `min_max_column_from_groups`.
- `GroupedAggregateSignature::Min(ColumnRef)` and
  `Max(ColumnRef)` variants at `sql_planner.rs:974`;
  `grouped_aggregate_signature` arms;
  `aggregate_output_name` returning `"min"` / `"max"`.
- Architecture-guard updates at
  `architecture_test.rs:466-507` (variant count 5 → 7;
  new table rows for MIN and MAX).
- Planner / binder / executor / scope / ambiguity test
  coverage in `group_by_test.rs`.
- One distributed digest-parity multi-process fixture in
  `distributed_process_test.rs` (grouped ordered MIN + MAX
  across two- and four-worker pools).
- Bench-evidence artifact at
  `plans/impl/0019_min_max_aggregate_kernels_bench_results.md`
  documenting correctness fixtures and throughput numbers
  relative to nearby SUM rows (no speed gate).

Explicitly unchanged:

- ADR 010's `Sort` operator surface and unconditional
  `Sort + Exchange(1, [])` wrap.
- ADR 011's `Aggregate` operator surface, projection-shape
  contract, P0.5 single-stage scalar shape, hash-
  partitioned grouped exchange, SUM/COUNT kernel
  substrate.
- ADR 015's nested-shape detector, ADR 016's
  `Filter`/`EvaluateInToron` placement, ADR 017's
  `LoweredSelect` seam, ADR 018's scalar-surface
  `GroupedAggregateScope::from_projection(&[], &aggr_expr)`
  pattern.
- Walker, scope, message helpers, four-way ambiguity
  classification — all generalize to MIN/MAX without
  modification because `Min(ColumnRef)` and
  `Max(ColumnRef)` slot into `GroupedAggregateSignature`
  mechanically.
- `bind_comparison` numeric-literal coercion (ADR 016):
  composes with MIN/MAX's identity-type output against
  integer literal RHS in HAVING.
- `Filter` / `Sort` operator surfaces; `JoinKind`,
  `JoinStrategy`, scheduler-barrier semantics, `TaskSpec`,
  `TaskFailureKind`, `BudgetDimension`, `MessageKind`,
  `TaskOutputBinding` variants; table-query wire frame;
  runtime-filter descriptor; PBC bytecode.
- Wendelon AGG protocol consumer surface (separate future
  ADR — ADR 02X).
- `AggState` derives — **stays `Copy`** because Int32/Int64
  domains use stack-only state. Utf8 inclusion would force
  removal of `Copy`; that is explicitly deferred.

No new IR variant beyond the enum widening. No new
fragmenter recognition site. No new shuffle binding. No new
stage role. No new failure kind. No new wire surface. No
new cross-ring engagement.

## Cycle property at the kernel surface

ADR 015 / 016 / 017 / 018 established the cycle's
*source-level boundary clarity* property at the SQL
*clause* surface (GROUP BY, HAVING, ORDER BY). ADR 019
extends the property to the SQL *kernel-catalog* surface:

> Every SQL aggregate function either lowers to a known
> kernel arm or rejects via a deterministic planner reject.
> No silent acceptance. No silent drop. The reject is a
> catch-all (`"unsupported aggregate function"`) for
> kernels not yet promoted; promotion replaces the catch-
> all with a narrow named arm.

MIN and MAX are the first kernel-catalog widening under
this property. The Wendelon-ADR-070 pattern governs the
*evidence discipline*; the cycle's invariant governs the
*reject discipline*. Both apply.

Architecture-guard delta:

- **Variant count (was 5, now 7):**
  `architecture_test.rs:473`.
- **Table entries (new):** `Min` / `"min"` /
  `Field::new("min",` and `Max` / `"max"` /
  `Field::new("max",` rows at
  `architecture_test.rs:485-507`.
- **Absence (preserved):** the
  `_ => Err(unsupported("unsupported aggregate function"))`
  catch-all at `sql_planner.rs:554` remains for future
  kernels (AVG and Utf8/Float MIN/MAX next).

## Context

The catch-all reject site:

- `SqlPlanner::lower_aggregate_function` at
  `sql_planner.rs:526-556` — match arm at line 554 rejects
  any unknown function name. `MIN(...)` and `MAX(...)`
  queries currently fail here.

The binder dispatch sites:

- `bind_aggregate_call` at `binder.rs:1065-1129` — five
  arms; MIN and MAX add two more.
- `aggregate_field` at `binder.rs:1322-1337` — five arms;
  MIN and MAX add two more.
- `sum_output_data_type` at `binder.rs:1339-1352` —
  numeric-input type-check helper. MIN/MAX use a parallel
  `min_max_output_data_type` returning the input type for
  `Int32` / `Int64` only.

The executor dispatch sites:

- `bound_global_aggregate` at `exec.rs:871-924` — five
  arms; MIN and MAX add two more.
- `bound_grouped_aggregate` at `exec.rs:1059-1158` — five
  arms; MIN and MAX add two more.
- `AggState` enum at `exec.rs:2740-2744` — three variants;
  MIN and MAX add four (`MinInt32`, `MinInt64`,
  `MaxInt32`, `MaxInt64`).
- `add_sum_value` at `exec.rs:2831-2859` — sum-
  accumulation discipline. MIN/MAX use a parallel
  comparison-based pattern (no overflow concern; just
  "is candidate strictly less/greater than current").

The Wendelon AGG protocol (noted but NOT engaged):

- `crates/wendelon/src/schema/aggregate.rs:94-125` declares
  `AggregateFunction::Min { column }` and
  `Max { column }` with parallel signatures. Toron-side
  has no AGG consumer at `crates/toron/src/storage.rs`.
  ADR 02X engages this surface.

## Decision

### §1. Enum widening: `AggregateFunction::Min` and `Max`

Add two variants at `ir.rs:107-114`:

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

Architecture guard at `architecture_test.rs:473` updates
to 7.

### §2. SQL planner arms

Insert into `lower_aggregate_function` at
`sql_planner.rs:535-555`:

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

`aggregate_output_name` arms:

```rust
AggregateFunction::Min => "min",
AggregateFunction::Max => "max",
```

`GroupedAggregateSignature` enum gains:

```rust
Min(ColumnRef),
Max(ColumnRef),
```

`grouped_aggregate_signature` arms (inline match
destructure, parallel SUM/Count):

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

### §3. Binder arms

`bind_aggregate_call` at `binder.rs:1076-1097` extends the
`Count | Sum` pattern. MIN and MAX validation parallels
SUM's two-block structure exactly, sharing one helper
because their type constraint is identical:

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
        // SUM validation — unchanged.
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

A small local helper `aggregate_function_keyword(function:
&AggregateFunction) -> &'static str` returns `"MIN"` /
`"MAX"` for the error messages. Shared with the MIN/MAX
arm only — not extracted across SUM/COUNT for the reason
ADR 018 / 019 substrate-fidelity convention stands (parallel
helpers don't share until three+ kernels surface duplication
pain; MIN and MAX as a single bundle count as one kernel
group here).

`aggregate_field` adds:

```rust
AggregateFunction::Min => Field::new(
    "min",
    call.args
        .first()
        .and_then(|arg| min_max_output_data_type(&arg.data_type()))
        .unwrap_or(DataType::Int64),
    true,
),
AggregateFunction::Max => Field::new(
    "max",
    call.args
        .first()
        .and_then(|arg| min_max_output_data_type(&arg.data_type()))
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

MIN/MAX output type is **identity-typed**: `Int32` input
→ `Int32` output, `Int64` input → `Int64` output. This
diverges from SUM (which always widens to Int64 / Float64)
and from AVG (Float64-uniform). The identity-type contract
is the natural fit for MIN/MAX semantics — the result is
literally one of the input values, not a derived
accumulation.

### §4. Executor — `AggState` variants and accumulation arms

Four new `AggState` variants at `exec.rs:2740-2744`:

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

**`AggState` stays `Copy`** because all variants use stack-
only state (no `String`, no heap data). The Utf8 inclusion
Codex considered would have forced removal of `Copy`; ADR
019 explicitly defers that substrate decision to a future
type-widening ADR.

`init_states` (cfg(test) variant at `exec.rs:2753-2771`)
gains MIN/MAX arms for the legacy unbound path. It may
default to Int64 because it has no bound argument types;
do not use that path to pin Int32 grouped MIN/MAX semantics.

`init_bound_states` (production variant at `exec.rs:2773-2795`)
should become result-returning so invalid hand-built bound
plans fail during state initialization rather than silently
defaulting:

```rust
// init_states (cfg(test)): legacy unbound default to Int64
AggregateFunction::Min => AggState::MinInt64 { value: None },
AggregateFunction::Max => AggState::MaxInt64 { value: None },

// init_bound_states (production): dispatch on input type and return Result
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

`bound_grouped_aggregate` must then initialize vacant group
state with an error-propagating entry match instead of the
current `or_insert_with(|| init_bound_states(aggr_expr))`
closure:

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
```

`update_max_state` mirrors `update_min_state` with
`v.max(candidate)`. The two helpers are kept parallel —
shared dispatch via a comparator parameter would obscure
the per-variant array-downcast pattern. ADR 020 (post-AVG)
revisits the shared-helper extraction once a third
comparable kernel surfaces.

Finalization:

```rust
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

MAX arm is symmetric, calling `max_state_for_data_type`
and `update_max_state`.

`bound_grouped_aggregate` MIN/MAX arms — per-row
accumulation loop extends:

```rust
AggState::MinInt32 { .. } | AggState::MinInt64 { .. } => {
    update_min_state(state, array, row)?;
}
AggState::MaxInt32 { .. } | AggState::MaxInt64 { .. } => {
    update_max_state(state, array, row)?;
}
```

Column emission loop adds:

```rust
AggregateFunction::Min | AggregateFunction::Max => {
    let output_type = output_schema.field(columns.len()).data_type().clone();
    columns.push(min_max_column_from_groups(&groups, agg_idx, &output_type)?);
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
| AggState::MaxInt64 { .. } => 0,
```

### §5. Scope, ambiguity, and clause-neutral walker — inheritance

`GroupedAggregateScope::from_projection`,
`GroupedAggregateScope::resolve`, `ResolveOutcome`, four-
way ambiguity classification,
`rewrite_grouped_aggregate_calls`,
`resolve_grouped_aggregate_reference`,
`aggregate_call_not_in_select_message`,
`ambiguous_aggregate_call_message`,
`reject_qualified_having_column_references` — all
unchanged. MIN and MAX compose with the existing substrate
the same way SUM does. The `Min(ColumnRef)` and
`Max(ColumnRef)` signature variants slot in mechanically.

### §6. NULL and empty-input contract

MIN and MAX ignore NULL input values.

**Scalar behavior:**
- At least one non-NULL value: returns the min/max of
  non-NULL values.
- Empty input: emits one row with NULL.
- All-NULL input: emits one row with NULL.

**Grouped behavior:**
- Empty input: emits no rows (no groups exist).
- Each group ignores NULL values independently.
- A group with only NULL values emits that group with NULL.

The scalar empty-input contract is pinned on the bound SQL
path. The legacy unbound logical executor has no schema
when an empty input produces zero batches; it must not
fabricate a typed NULL for unbound empty MIN/MAX. Empty-
input SQL semantics tests use the bound path.

### §7. HAVING / ORDER BY composition — pure inheritance

`SELECT MIN(amount) FROM sales HAVING MIN(amount) > 0`
composes through the ADR 016 walker substrate.
`MIN(amount) > 0` lowers as `Expr::Gt(Expr::column("min"),
Literal(Int32(0)))`; `bind_comparison` numeric-literal
coercion (ADR 016) handles type matching for Int32 / Int64
RHS. Zero new binder code.

`SELECT MIN(amount) FROM sales ORDER BY MIN(amount) DESC`
composes through the ADR 017 walker substrate against the
ADR 018-extended scope (`Some(scope)` for scalar
aggregate-projection queries). Zero new planner code
beyond the new signature variants.

### §8. No new surface

ADR 019 introduces no new IR variant beyond the enum
widening; no new `JoinKind`, `JoinStrategy`, `MessageKind`,
`TaskFailureKind`, `BudgetDimension`, `FilterDisposition`,
scheduler barrier kind, `TaskOutputBinding` variant; no new
wire frame, runtime-filter descriptor field, PBC opcode; no
new `bind_expr` match arm; no new planner cost-model knob;
no fragmenter recognition site; no new architecture guard
*class*; no cross-ring engagement.

## V1 exclusions

ADR 019-specific exclusions:

1. **Float `MIN` / `MAX` (Float32, Float64).** Requires an
   explicit NaN and signed-zero policy ratified by ADR.
   Arrow's bytewise IEEE 754 ordering is deterministic but
   surprising as aggregate semantics — NaN handling
   (treat as NULL? sort last? error?) needs council
   ratification. Separate follow-on ADR.
2. **Utf8 `MIN` / `MAX`.** Requires `AggState` to lose
   `Copy` derivation (because `String` owns its buffer).
   That's a substantial substrate refactor with cascading
   effects across the existing `match states[agg_idx]`
   patterns. Worth its own ratification round; a future
   type-widening ADR handles it.
3. **Boolean, decimal, date, timestamp `MIN` / `MAX`.**
   Each adds its own type contract and dispatch decision.
   Separate ADRs.
4. **`AVG` aggregate.** Multi-state aggregator (sum +
   count); uniform-output-type policy (Float64 vs Decimal)
   diverges from MIN/MAX's identity-type. Deferred to ADR
   020 candidate slate.
5. **`MIN(*)`** / **`MAX(*)`** — rejected via `lower_arg`'s
   existing Wildcard reject; ADR 019 adds no special
   handling.
6. **`MIN(DISTINCT col)`** / **`MAX(DISTINCT col)`** —
   inherited ADR 011 reject (`"DISTINCT is not
   supported"`).
7. **Two-phase distributed MIN/MAX aggregation.** ADR 019
   ships MIN/MAX single-stage scalar / per-partition
   grouped under ADR 011's existing distribution shape.
   Two-phase partial/final aggregation is a separate
   future ADR (ADR 02X) that engages Wendelon's AGG
   consumer protocol.
8. **Storage-pushed MIN/MAX via Wendelon AGG protocol.**
   Wendelon ships `Min { column }` / `Max { column }`;
   Toron's `wendelon_client` does not expose them. ADR
   02X wires the consumer.
9. **Aliased MIN/MAX output** (`SELECT MIN(amount) AS
   smallest`). Inherits ADR 011's post-aggregate
   projection reject.

Inherited rejects (preserved verbatim):

10. **Computed aggregate-call arguments** — ADR 011 reject.
11. **Experimental kernels under GROUP BY** —
    `lower_aggregate_function` rejects `LINEAR_REGRESS` /
    `APPROX_QUANTILE` in `Grouped` mode.

## Consequences

- The three canonical MIN/MAX patterns (scalar, grouped,
  grouped + HAVING + ORDER BY + LIMIT) lower, fragment,
  and execute end-to-end.
- Walker, scope, `ResolveOutcome`, four-way ambiguity
  classification, pre-pass, message helpers, and
  `bind_comparison` literal coercion — all generalize to
  MIN/MAX without modification.
- `AggregateFunction` enum: 5 → 7 variants.
- `AggState` enum: 3 → 7 variants (private executor
  type). `AggState` retains `Copy` because all new
  variants are stack-only.
- Architecture-guard table at
  `architecture_test.rs:485-507` gains MIN and MAX rows.
- `aggregate_output_name` and `aggregate_field` gain
  MIN/MAX arms.
- ADR 011's per-partition aggregation contract inherits:
  MIN/MAX execute single-stage scalar, per-partition
  grouped.
- Wendelon AGG protocol consumer wiring named as future
  ADR 02X; ADR 019 stays Toron-local.
- First proof that the cycle's source-level boundary
  clarity property extends to the kernel-catalog surface.
  Catch-all reject at `sql_planner.rs:554` remains for AVG
  (ADR 020) and future kernels.

## Promotion evidence

Implementation promotion requires:

1. **Planner** —
   - Canonical patterns lower per §1-§5.
   - `AggregateFunction::Min` and `Max` variants added;
     variant count 7 confirmed.
   - V1 exclusion items §1-§3, §5-§7, §9-§11 reject via
     inherited or new narrow named paths.

2. **Binder / executor** —
   - Int32/Int64 input type check via
     `min_max_output_data_type`.
   - HAVING comparison binds via inherited
     `bind_comparison` Int32/Int64 coercion.
   - ORDER BY rewrite binds via inherited ADR 017 walker.
   - Identity-type output: Int32 input → Int32 output;
     Int64 input → Int64 output.
   - Empty-input contract pinned: scalar one-row NULL;
     grouped zero rows.

3. **In-process** —
   - SQL result digest-matches a hand-built logical plan
     for scalar MIN/MAX, grouped MIN/MAX, and grouped
     MIN/MAX + HAVING + ORDER BY.

4. **Distributed** —
   - Grouped MIN/MAX `SELECT val, MIN(val), MAX(val)
     FROM fixture GROUP BY val ORDER BY MAX(val) DESC LIMIT 5`
     produces byte-identical normalized output across two-
     and four-worker fixtures.

5. **Architecture guard** —
   - `adr019_min_max_aggregate_kernels_guards_are_narrow`
     pins variant count 7, new table entries for MIN and
     MAX, `AggState` `Copy` derivation preserved, and
     cross-ring-absence (no `AggregateExpression` /
     `AggregateFunctionTag` symbols in Toron-side source).

6. **Bench artifact** —
   - `plans/impl/0019_min_max_aggregate_kernels_bench_results.md`
     captures correctness fixtures (Int32 / Int64 scalar
     and grouped, mixed-NULL inputs) and throughput
     numbers relative to nearby SUM rows. **No minimum-
     speedup gate.** The artifact exists because the
     Wendelon-ADR-070 pattern applies (ADR 019 adds a new
     stable kernel; ADR 011 precedent does not transfer —
     ADR 011 made existing kernels reachable for grouped
     SQL).

## What not to do

- **Do not engage the Wendelon AGG consumer protocol.**
  `wendelon_client::TableQueryOptions` stays as-is.
  ADR 02X is the cross-ring ADR.
- **Do not include AVG in ADR 019.** Multi-state
  aggregator with output-type policy; deferred to ADR 020
  candidate slate.
- **Do not include float `MIN` / `MAX` in ADR 019.** NaN
  and signed-zero policy needs explicit ratification.
  Future follow-on ADR.
- **Do not include Utf8 `MIN` / `MAX` in ADR 019.** Forces
  `AggState` to lose `Copy`; substantial substrate
  refactor. Worth its own ratification round.
- **Do not pin `AggState::MinInt*` widths as cross-ring
  contract.** Use the natural `i32` / `i64` per input
  type; the Wendelon-side
  `AggregateExpression { tag, column_ordinal,
  parameter_block }` shape is a different wire format.
  ADR 02X pins the cross-ring contract when it engages.
- **Do not skip the bench artifact.** Codex's argument is
  sharper than the ADR 011 precedent reading: ADR 011 made
  existing kernels reachable for grouped SQL; ADR 019 adds
  new stable kernels. The bench artifact exists; it is
  lightweight; it carries no speed gate.
- **Do not refactor the SUM kernel substrate.** ADR 019's
  MIN/MAX implementation is parallel to SUM (separate
  helpers). Refactoring SUM and MIN/MAX into shared
  helpers is the natural move *after* AVG (ADR 020) lands
  — at three kernels, the substrate-share decision earns
  its keep.
- **Do not extract a shared
  `aggregate_function_keyword` helper across all kernels.**
  The helper is local to the MIN/MAX arm because MIN and
  MAX share an error-message keyword. Shared-helper
  pressure does not earn its keep across the full kernel
  catalog yet.
- **Do not retire the
  `_ => Err(unsupported(...))` catch-all at
  `sql_planner.rs:554`.** Future kernels (AVG, Utf8 / float
  MIN/MAX, graduated experimentals) still need it. The
  cycle's source-level boundary clarity property requires
  the catch-all stay until every kernel-catalog widening
  has landed.
- **Do not silently inherit Arrow's IEEE 754 ordering as
  the float `MIN` / `MAX` contract.** Float aggregation
  needs explicit NaN policy. ADR 019 rejects float input
  at the binder; the future float-MIN/MAX ADR pins the
  policy explicitly.
- **Do not store ordering metadata (collation, locale) for
  Utf8.** Utf8 is deferred; when it lands, the collation
  question lands with it as part of the dedicated ADR.

## ADR 020 handoff

ADR 020 selection starts from the post-ADR-019 remainder:

- **AVG kernel.** Closest follow-on under Wendelon-ADR-070.
  Multi-state accumulator `(sum, count)`; Float64-uniform
  output (V1 default; Decimal output for integer inputs
  awaits V1.x Decimal ADR). Wendelon-side ships
  `SignedAverageParts { sum: i128, count: u64 }` as
  canonical partial state — alignment is a happy property,
  not yet a cross-ring contract.
- **Float `MIN` / `MAX`.** Needs NaN and signed-zero
  policy ratification. Smaller in absolute scope than AVG
  but with a sharper semantic-contract question.
- **Utf8 `MIN` / `MAX`.** Forces `AggState` to lose `Copy`;
  worth its own substrate-refactor ratification. Could
  bundle with collation/locale decision matrix or stay
  bytewise-only.
- **Shared-helper refactor for SUM/MIN/MAX/AVG (deferred
  to post-AVG).** At three or four parallel numeric
  kernels, the duplication between per-kernel
  state helpers earns refactor. ADR 020 ratifies the
  refactor decision explicitly.
- **Wendelon AGG consumer wiring (ADR 02X).** Toron-side
  client wiring through to Wendelon's `AggregateExpression`
  schema; partial/final two-phase shape; lifts ADR 011's
  per-partition scalar ceiling. **This is the right home
  for two-phase aggregation work.** Bench artifact
  required (cross-ring perf characteristic).
- **Post-aggregate projection.** Renames
  `GroupedAggregateScope` to a broader output-name
  catalog; enables aliased aggregate output, computed
  expressions over aggregates, key subsets, reordered
  keys. Naming-asymmetry signal continues from
  ADR 018/019.
- **DISTINCT.** Own decision matrix; loosens ADR 011's
  zero-aggregate reject.
- **GROUP BY over joins.** Brainstorm 003 seam (parked).
- **Kernel graduation** (`LinearRegress`,
  `ApproxQuantile`, `TimeBucket`). The bound/unbound
  parity gap at `exec.rs:912-916` and `exec.rs:1035-1039`
  remains evidence pressure; gated behind
  `allow_experimental_kernels = false`, does not promote
  as ADR 020 substitute.
- **Kahan-Neumaier stability ADR.** Composes across SUM
  and AVG together; MIN/MAX is order-independent so
  doesn't participate.
- **V1.x runtime-filter extensions.** Off-axis cluster
  parked.

**Code-evidence defects continue to outrank the printed
list when one surfaces.** Post-ADR-019 sweep is the next
council round's first task.

The roadmap's `ADR 020 candidates` section owns the
forward-looking detail.
