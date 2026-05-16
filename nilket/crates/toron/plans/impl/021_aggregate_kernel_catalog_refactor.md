# Implementation Plan: ADR 021 — Aggregate Kernel Catalog Refactor

**Companion ADR:**
`plans/adr/021_aggregate_kernel_catalog_refactor.md`

## Status

Promoted.

Internal substrate refactor. One new private module
(`crates/toron/src/aggregate_kernel.rs`), four modified
source files (`lib.rs`, `sql_planner.rs`, `binder.rs`,
`exec.rs`), one modified architecture test file (replaces
the ADR 020 enum-count guard with two new catalog-centered
guards), and a lightweight bench artifact. The
implementation is **observationally invisible**: no SQL
output changes, no plan digest changes, no IR shape
change. Every existing test stays green; every binder
error string is preserved byte-for-byte; every per-row
state-machine body is unchanged.

## Goal

Land the catalog refactor across six phases without
behavior change. The implementation rule:

> Catalog metadata moves to `aggregate_kernel.rs`;
> executor state-machine behavior stays in `exec.rs`.

Concretely:

- Centralize stable aggregate metadata into one
  private `AggregateKernelSpec` table indexed by
  `StableAggregateKernel` (`#[repr(u8)]` compact tag).
- Replace `aggregate_function_name`,
  `aggregate_output_name`, the three private
  `*_output_data_type` helpers, the binder's per-kernel
  arity / input-domain / output-type / output-name /
  keyword matches, and the executor's per-kernel init
  match with catalog-driven dispatch.
- Preserve every row-update body, every state helper,
  every finalize helper, every error string, every
  `AggState` variant, and every plan digest byte-for-
  byte.
- Replace the ADR 020 enum-count architecture guard
  with a source-read catalog-exhaustiveness guard;
  add a companion `bind_expr_remains_aggregate_free`
  guard following the ADR 015 reject-string-absence
  pattern.
- Record a bench artifact with digest parity and per-
  aggregate-row timings; no minimum-speedup gate.

## Substrate

### New file

- `crates/toron/src/aggregate_kernel.rs` — private
  catalog module. Imports limited to
  `arrow::datatypes::{DataType, Field}` and
  `crate::ir::{AggregateCall, AggregateFunction}`. No
  references to `crate::binder::*` or `crate::exec::*`
  to keep the catalog source-readable in isolation.

### Modified files

- `crates/toron/src/lib.rs`
  - Add `mod aggregate_kernel;` (private; not
    `pub mod`).
- `crates/toron/src/sql_planner.rs`
  - `aggregate_function_name` at
    `sql_planner.rs:1124`: delegate to catalog.
  - `aggregate_output_name` at
    `sql_planner.rs:1140`: delegate to catalog.
  - `GroupedAggregateSignature` arms stay explicit
    (aggregate-call rewrite behavior, not metadata).
- `crates/toron/src/binder.rs`
  - `bind_aggregate_call` at `binder.rs:1065`:
    refactor to read `stable_spec_for_function` once,
    then dispatch arity / input-domain / output-type.
  - `aggregate_field` at `binder.rs:1358`: delegate to
    `stable_output_field`.
  - `sum_output_data_type` at `binder.rs:1392`,
    `min_max_output_data_type` at `binder.rs:1407`,
    `avg_output_data_type` at `binder.rs:1414`: remove
    (logic folds into `AggregateOutputRule` matching
    inside `aggregate_kernel.rs`).
  - `aggregate_function_keyword` at `binder.rs:1430`:
    delegate to catalog.
- `crates/toron/src/exec.rs`
  - `init_bound_states` at `exec.rs:2955`: rewrite to
    map over a new `init_bound_state_for_call` helper.
  - New `init_bound_state_for_call(call:
    &AggregateCall) -> Result<AggState, ExecutionError>`
    helper.
  - Scalar-aggregate dispatch at `exec.rs:968-1010`:
    optional centralization via
    `finalize_scalar_for_call`. Direct matches if
    centralization doesn't improve readability.
  - Grouped-aggregate dispatch at `exec.rs:304-372`:
    optional centralization via
    `finalize_grouped_for_call`. Direct matches if
    centralization doesn't improve readability.
  - cfg(test) helpers `aggregate_state_keyword`,
    `aggregate_state_output_name`: delegate to catalog
    or remove.
  - **Unchanged.** `AggState` enum and derives;
    `NumericValue`; `sum_state_for_data_type` at
    `exec.rs:3021`; `min_state_for_data_type` at
    `exec.rs:3034`; `max_state_for_data_type` at
    `exec.rs:3044`; `avg_state_for_data_type` at
    `exec.rs:3054`; `add_sum_value` at `exec.rs:3074`;
    `add_avg_value` at `exec.rs:3112`;
    `update_min_state` at `exec.rs:3156`;
    `update_max_state` at `exec.rs:3193`;
    `min_max_state_array` at `exec.rs:3230`;
    `sum_state_array` at `exec.rs:3244`;
    `avg_state_array` at `exec.rs:3368`; scalar and
    grouped row loops.
- `crates/toron/tests/architecture_test.rs`
  - Remove `aggregate_function_enum_guards_are_narrow`
    (the ADR 020 enum-count guard).
  - Add `aggregate_kernel_catalog_guards_are_narrow`
    (catalog-exhaustiveness source-read guard).
  - Add `bind_expr_remains_aggregate_free` (ADR 015
    reject-string-absence pattern).
  - Adjust ADR 019 guard
    (`adr019_min_max_aggregate_kernels_guards_are_narrow`)
    and ADR 020 guard
    (`adr020_avg_aggregate_kernel_guards_are_narrow`)
    to read invariants through the catalog rather than
    via parallel planner/binder match arms.

### Added planning artifacts

- `crates/toron/plans/adr/021_aggregate_kernel_catalog_refactor.md`
  (this ADR).
- `crates/toron/plans/impl/021_aggregate_kernel_catalog_refactor.md`
  (this impl plan).
- `crates/toron/plans/impl/021_aggregate_kernel_catalog_refactor_bench_results.md`
  (after Phase 6).

## Catalog contents (Phase 1)

The six static specs in
`STABLE_AGGREGATE_KERNELS` (Phase 1 verifies values
against current binder helpers before pinning):

| `kind`     | `sql_name` | `output_name` | `arity` | `input_domain` | `output_rule` | `nullable` | `grouped_supported` |
|------------|------------|---------------|---------|----------------|---------------|------------|---------------------|
| `CountAll` | `COUNT`    | `count`       | `Zero`  | `Any`          | `Int64`       | `false`    | `true`              |
| `Count`    | `COUNT`    | `count`       | `One`   | `Any`          | `Int64`       | `false`    | `true`              |
| `Sum`      | `SUM`      | `sum`         | `One`   | `SumNumeric`   | `SumNumeric`  | `true`     | `true`              |
| `Min`      | `MIN`      | `min`         | `One`   | `IntMinMax`    | `SameAsInput` | `true`     | `true`              |
| `Max`      | `MAX`      | `max`         | `One`   | `IntMinMax`    | `SameAsInput` | `true`     | `true`              |
| `Avg`      | `AVG`      | `avg`         | `One`   | `AvgNumeric`   | `Float64`     | `true`     | `true`              |

The `output_name` values match the existing
`aggregate_output_name` arms in
`sql_planner.rs:1140`. Phase 1 verifies — any mismatch
between this table and the actual planner output names
must resolve toward what the planner produces today (the
catalog preserves observable behavior, not what this
table prescribes).

`stable_output_data_type` implementation:

```rust
pub(crate) fn stable_output_data_type(
    function: &AggregateFunction,
    input: Option<&DataType>,
) -> Option<DataType> {
    let spec = stable_spec_for_function(function)?;
    match (spec.output_rule, input) {
        (AggregateOutputRule::Int64, _) => Some(DataType::Int64),
        (AggregateOutputRule::Float64, _) => Some(DataType::Float64),
        (AggregateOutputRule::SameAsInput, Some(t)) => Some(t.clone()),
        (AggregateOutputRule::SumNumeric, Some(t)) => match t {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(DataType::Int64),
            DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
            _ => None,
        },
        (_, None) => None,
    }
}
```

Phase 1 verifies `SumNumeric` and `AvgNumeric` acceptance
domains against `binder.rs:1392-1414`; the match above is
the current domain and should remain byte-for-byte
equivalent after relocation.

## Phases

### Phase 1 — Private catalog

**Goal.** Land `aggregate_kernel.rs` with the catalog,
specs, and helper API. No call-site changes.

**Tasks.**
- Create `crates/toron/src/aggregate_kernel.rs`:
  - `StableAggregateKernel` `#[repr(u8)]` enum (6
    variants).
  - `AggregateArity`, `AggregateInputDomain`,
    `AggregateOutputRule` enums.
  - `AggregateKernelSpec` struct (pure-data, Copy).
  - `STABLE_AGGREGATE_KERNELS` static slice (6 rows).
  - Helper API: `stable_kernel_for_function`,
    `stable_spec`, `stable_spec_for_function`,
    `stable_aggregate_sql_name`,
    `experimental_aggregate_sql_name`,
    `stable_output_data_type`, `stable_output_field`.
- Add `mod aggregate_kernel;` to `lib.rs` (private;
  not `pub mod`).
- **Verify type domains.** Read
  `binder.rs:1392-1414` and confirm the
  `SumNumeric` / `AvgNumeric` acceptance domains in
  `stable_output_data_type` match the existing helpers
  byte-for-byte. Extend the match arms if the actual
  binder supports a broader set.
- Cargo build + run full test suite. New module is
  unused by other code; suite stays green.

**Definition of phase done.**
- `aggregate_kernel.rs` compiles.
- Module is private (`mod aggregate_kernel;` in
  `lib.rs`; no `pub mod`).
- Type domains in `stable_output_data_type` match
  existing binder behavior byte-for-byte.
- Test suite green; no behavior change yet.

### Phase 2 — Planner migration

**Goal.** `sql_planner.rs` delegates to the catalog.

**Tasks.**
- Rewrite `aggregate_function_name` at
  `sql_planner.rs:1124`:

```rust
fn aggregate_function_name(name: &ObjectName) -> Option<&'static str> {
    let upper = /* existing single-ident decomposition */;
    aggregate_kernel::stable_aggregate_sql_name(&upper)
        .or_else(|| aggregate_kernel::experimental_aggregate_sql_name(&upper))
}
```

  Keep the existing `ObjectName` decomposition logic
  local. Only the post-uppercase lookup delegates.

- Rewrite `aggregate_output_name` at
  `sql_planner.rs:1140`:

```rust
fn aggregate_output_name(function: &AggregateFunction) -> &'static str {
    if let Some(spec) = aggregate_kernel::stable_spec_for_function(function) {
        return spec.output_name;
    }
    // Experimental kernel output names stay explicit (gated upstream).
    match function {
        AggregateFunction::LinearRegress => "linreg",
        AggregateFunction::ApproxQuantile(_) => "quantile",
        _ => unreachable!("stable_spec_for_function covers stable kernels"),
    }
}
```

- **Keep `GroupedAggregateSignature` arms explicit.** This
  enum encodes aggregate-call rewrite behavior, not
  metadata. Codex iter-3 specifically flags this.

- Run `cargo test -p toron --test distributed_process_test`
  and the full suite. Plan digests must be byte-for-byte
  unchanged.

**Definition of phase done.**
- Planner per-kernel metadata matches removed.
- Plan digests byte-for-byte unchanged across the suite.
- All planner / distributed-process tests green.

### Phase 3 — Binder migration

**Goal.** `binder.rs` delegates to the catalog;
output-type helpers removed; error strings preserved
byte-for-byte.

**Tasks.**
- Refactor `bind_aggregate_call` at `binder.rs:1065`:

```rust
fn bind_aggregate_call(/* ... */) -> Result<AggregateCall, BindingError> {
    // Identify the kernel.
    let function = /* existing function resolution */;
    let spec = aggregate_kernel::stable_spec_for_function(&function)
        .ok_or_else(|| /* experimental binding path or unsupported reject */)?;

    // CountAll is special in today's lowered logical plan: the
    // planner still carries the existing Expr::column("count")
    // placeholder, and the binder discards it into a zero-arg
    // AggregateCall. Preserve that shape for digest stability.
    if matches!(spec.kind, StableAggregateKernel::CountAll) {
        return Ok(AggregateCall {
            function,
            args: Vec::new(),
        });
    }

    // Arity check.
    let arity_expected = match spec.arity {
        AggregateArity::Zero => 0,
        AggregateArity::One => 1,
    };
    if args.len() != arity_expected {
        return Err(binding_error(format!(
            "{} requires {} argument{}",
            spec.sql_name,
            arity_expected,
            if arity_expected == 1 { "" } else { "s" }
        )));
    }

    // Input-domain check (error strings preserved byte-for-byte).
    let input_type = args.first().map(|a| a.data_type());
    let domain_ok = match spec.input_domain {
        AggregateInputDomain::Any => true,
        AggregateInputDomain::SumNumeric => /* match input against SUM domain */,
        AggregateInputDomain::AvgNumeric => /* match input against AVG domain */,
        AggregateInputDomain::IntMinMax => matches!(
            input_type, Some(DataType::Int32) | Some(DataType::Int64)
        ),
    };
    if !domain_ok {
        return Err(binding_error(match spec.kind {
            StableAggregateKernel::Sum => "SUM expects numeric input",
            StableAggregateKernel::Avg => "AVG expects numeric input",
            StableAggregateKernel::Min => "MIN expects Int32 or Int64 input",
            StableAggregateKernel::Max => "MAX expects Int32 or Int64 input",
            _ => unreachable!("Any domain never fails"),
        }.into()));
    }

    // Output type computed via spec.output_rule (handled when constructing AggregateCall).
    Ok(AggregateCall { function, args, /* ... */ })
}
```

  **Error strings load-bearing.** Preserved byte-for-byte:
  - `SUM requires one argument`
  - `SUM expects numeric input`
  - `MIN requires one argument`
  - `MIN expects Int32 or Int64 input`
  - `MAX requires one argument`
  - `MAX expects Int32 or Int64 input`
  - `AVG requires one argument`
  - `AVG expects numeric input`

- Replace `aggregate_field` at `binder.rs:1358`:

```rust
fn aggregate_field(call: &AggregateCall) -> Field {
    aggregate_kernel::stable_output_field(call)
        .expect("stable_output_field covers stable kernels")
}
```

- Replace `aggregate_function_keyword` at
  `binder.rs:1430`:

```rust
fn aggregate_function_keyword(function: &AggregateFunction) -> &'static str {
    aggregate_kernel::stable_spec_for_function(function)
        .map(|s| s.sql_name)
        .unwrap_or_else(|| /* experimental fallback */)
}
```

- Remove `sum_output_data_type` (`binder.rs:1392`),
  `min_max_output_data_type` (`binder.rs:1407`),
  `avg_output_data_type` (`binder.rs:1414`). Their
  logic now lives in `AggregateOutputRule::SumNumeric`
  / `SameAsInput` / `Float64` matching inside
  `aggregate_kernel.rs`.

- **Error-string fidelity check.** Before merging Phase
  3: `grep -F` for each of the eight error strings in
  `binder.rs`; assert each still appears.

- Keep experimental binding gates explicit and unchanged.

**Definition of phase done.**
- Binder per-kernel metadata matches removed.
- Three `*_output_data_type` helpers deleted.
- Error strings preserved byte-for-byte (verified via
  grep).
- Binder tests green.

### Phase 4 — Executor cleanup

**Goal.** Centralize init dispatch; preserve every row-
path and state-machine body.

**Tasks.**
- Introduce in `exec.rs`:

```rust
fn init_bound_state_for_call(call: &AggregateCall)
    -> Result<AggState, ExecutionError>
{
    use AggregateFunction::*;
    match &call.function {
        CountAll | Count => Ok(AggState::Count(0)),
        Sum => Ok(sum_state_for_data_type(&call.args[0].data_type())),
        Min => min_state_for_data_type(&call.args[0].data_type()),
        Max => max_state_for_data_type(&call.args[0].data_type()),
        Avg => avg_state_for_data_type(&call.args[0].data_type()),
        LinearRegress | ApproxQuantile(_) => {
            Err(ExecutionError::NotImplemented(
                "experimental kernel".into()))
        }
    }
}
```

- Rewrite `init_bound_states` at `exec.rs:2955`:

```rust
fn init_bound_states(aggr_expr: &[AggregateCall])
    -> Result<Vec<AggState>, ExecutionError>
{
    aggr_expr.iter().map(init_bound_state_for_call).collect()
}
```

- **Optional.** Centralize scalar-aggregate dispatch
  (`exec.rs:968-1010`) via `finalize_scalar_for_call`
  and grouped-aggregate dispatch (`exec.rs:304-372`)
  via `finalize_grouped_for_call`. Implementer's choice
  whether this improves readability. If introduced, the
  centralized helper is a direct match over
  `AggregateFunction` and `AggState` — **no row-path
  trait dispatch, no `dyn`, no `Box`, no function
  pointers, no ZST trait objects.**

- cfg(test) helpers (`aggregate_state_keyword`,
  `aggregate_state_output_name`): delegate to
  `stable_spec_for_function(...).map(|s| s.sql_name)`
  / `.output_name`, or remove if redundant.

- **Hard preservation list.** The following are
  byte-identical pre- and post-refactor:
  - `AggState` (variants, derives, `Copy`-ness).
  - `NumericValue`.
  - `sum_state_for_data_type`,
    `min_state_for_data_type`,
    `max_state_for_data_type`,
    `avg_state_for_data_type`.
  - `add_sum_value`, `add_avg_value`,
    `update_min_state`, `update_max_state`.
  - `sum_state_array`, `min_max_state_array`,
    `avg_state_array`.
  - Scalar and grouped row loops.

**Definition of phase done.**
- `init_bound_states` delegates through
  `init_bound_state_for_call`.
- `AggState: Copy` preserved.
- Row-update bodies byte-identical (verified via diff
  against pre-refactor).
- `group_by_test.rs`,
  `distributed_process_test.rs`,
  `aggregate_projection_accounting_test.rs` green.

### Phase 5 — Architecture guard

**Goal.** Replace the ADR 020 enum-count guard with the
catalog-exhaustiveness source-read guard; add the ADR 015
reject-string-absence guard for the aggregate surface.

**Tasks.**
- In `tests/architecture_test.rs`:
  - Remove `aggregate_function_enum_guards_are_narrow`.
  - Add `aggregate_kernel_catalog_guards_are_narrow`:

```rust
#[test]
fn aggregate_kernel_catalog_guards_are_narrow() {
    // This is an integration test. Keep aggregate_kernel private
    // and source-read the module instead of importing it.
    let catalog = std::fs::read_to_string(
        manifest_dir().join("src/aggregate_kernel.rs")).unwrap();

    // 1. Catalog rows match the expected StableAggregateKernel values.
    for variant in ["CountAll", "Count", "Sum", "Min", "Max", "Avg"] {
        assert!(catalog.contains(variant));
    }

    // 2. COUNT shares one SQL name across two catalog rows.
    assert_eq!(catalog.matches("sql_name: \"COUNT\"").count(), 2);

    // 3. Unique SQL names: exactly five.
    for name in ["COUNT", "SUM", "MIN", "MAX", "AVG"] {
        assert!(catalog.contains(name));
    }

    // 4. Experimental kernels are not stable catalog rows.
    let stable_rows = catalog
        .split("STABLE_AGGREGATE_KERNELS")
        .nth(1)
        .and_then(|rest| rest.split("];").next())
        .unwrap_or("");
    assert!(!stable_rows.contains("LinearRegress"));
    assert!(!stable_rows.contains("ApproxQuantile"));

    // 5. Module is private.
    let lib_rs = std::fs::read_to_string(
        manifest_dir().join("src/lib.rs")).unwrap();
    assert!(lib_rs.contains("mod aggregate_kernel;"));
    assert!(!lib_rs.contains("pub mod aggregate_kernel"));

    // 6. Planner catch-all preserved.
    let planner = std::fs::read_to_string(
        manifest_dir().join("src/sql_planner.rs")).unwrap();
    assert!(planner.contains("unsupported aggregate function"));

    // 7. AggState still derives Copy.
    let exec = std::fs::read_to_string(
        manifest_dir().join("src/exec.rs")).unwrap();
    // Extract the AggState definition and assert derive contains Copy.
    let agg_state_block = /* extract enum AggState { ... } block */;
    assert!(agg_state_block.contains("Copy"));
}
```

  - Add `bind_expr_remains_aggregate_free`:

```rust
#[test]
fn bind_expr_remains_aggregate_free() {
    // ADR 015 reject-string-absence pattern at the aggregate surface.
    let binder = std::fs::read_to_string("src/binder.rs").unwrap();
    let bind_expr_body = /* extract `fn bind_expr` body */;
    assert!(!bind_expr_body.contains("AggregateFunction"));
    assert!(!bind_expr_body.contains("AggregateCall"));
}
```

- Adjust ADR 019 and ADR 020 guards:
  - `adr019_min_max_aggregate_kernels_guards_are_narrow`:
    assert via catalog that MIN and MAX have
    `input_domain == IntMinMax` and `output_rule ==
    SameAsInput`.
  - `adr020_avg_aggregate_kernel_guards_are_narrow`:
    assert via catalog that AVG has `input_domain ==
    AvgNumeric` and `output_rule == Float64`.

  Remove any sub-assertion in those guards that depends
  on duplicated planner/binder match arms ADR 021
  intentionally removes.

**Definition of phase done.**
- New guards pass.
- ADR 019 / ADR 020 guards read through the catalog
  and retain their invariants.

### Phase 6 — Verification + bench

**Goal.** Confirm the refactor is observationally
invisible; record bench artifact.

**Tasks.**
- Run focused tests:

```sh
cargo test -p toron --test architecture_test
cargo test -p toron --test group_by_test
cargo test -p toron --test distributed_process_test
cargo test -p toron aggregate_projection_accounting
```

- Run broader checks:

```sh
cargo fmt --check
cargo test -p toron
```

- Run bench:

```sh
cargo bench -p toron --bench storage_execution -- --quick --storage-execution-only
```

- Write
  `crates/toron/plans/impl/021_aggregate_kernel_catalog_refactor_bench_results.md`
  matching ADR 020's bench artifact shape:
  - Environment block (commit, rustc, cargo, Arrow
    versions).
  - Fixture description.
  - Per-aggregate-row baseline / engaged ms, digest,
    bytes, columns.
  - `pbc_engaged` status (no aggregate pushdown
    expected post-refactor; should remain `false`).
  - Nearby SUM / MIN / MAX / AVG rows for comparison.
  - Readout: digest parity asserted; no minimum-
    speedup gate; investigate any aggregate row that
    regresses > 10%.

**Definition of phase done.**
- All tests green.
- Bench artifact recorded.
- No aggregate row regression > 10%.

## Risk register

### R1: Behavior drift through helper relocation

**Risk.** Relocating `*_output_data_type` bodies into
`AggregateOutputRule` matching could introduce subtle
behavior changes (e.g., a different `Option<DataType>`
return in an edge case).

**Mitigation.** Phase 1 verifies type domains against
existing binder helpers byte-for-byte before pinning
catalog contents. Phase 3 binder tests catch divergence.
The `SumNumeric` match arm in `stable_output_data_type`
is a direct copy of `sum_output_data_type`'s body.

### R2: Plan digest divergence

**Risk.** Catalog-driven `aggregate_output_name`
returning a different string (e.g., case change) breaks
plan digests and `distributed_process_test`.

**Mitigation.** Phase 2 explicitly verifies digests
unchanged across the full suite. The catalog's
`output_name` values are pinned in Phase 1's spec table;
any divergence from the existing
`sql_planner.rs:1140` arms must resolve toward the
planner's current output.

### R3: Row-update dispatch divergence

**Risk.** Phase 4's centralization might inadvertently
change how row-update bodies are called.

**Mitigation.** Row-update bodies (`add_sum_value`,
`add_avg_value`, MIN/MAX update helpers) stay in their
current call sites — only the surrounding `match
call.function` collapses, not the inner loops. Code
review explicitly diffs row-update sites against
pre-refactor.

### R4: AggState relocation creep

**Risk.** Phase 4 temptation to relocate `AggState`
variants into `aggregate_kernel.rs`.

**Mitigation.** ADR §Executor boundary explicit:
`AggState` stays in `exec.rs`. The catalog never
references `AggState`. Code review gates this; Phase 5
architecture guard sources-reads `src/exec.rs` for
`Copy` derive preservation.

### R5: Catalog exhaustiveness vs experimental kernels

**Risk.** The new guard must assert experimental
kernels are NOT in the catalog while letting them
exist in `AggregateFunction`.

**Mitigation.** Phase 5 guard source-reads the stable
catalog rows and explicitly asserts `LinearRegress` and
`ApproxQuantile` are absent from that row block while
allowing them in the adjacent experimental helper.

### R6: Output-rule enum extensibility

**Risk.** `AggregateOutputRule` may need new variants
for future kernels (e.g., Decimal with scale/precision
preservation).

**Mitigation.** Iter-4 accepts the four current variants
(`Int64`, `Float64`, `SameAsInput`, `SumNumeric`). Future
ADRs add variants without backward-compat shims. The
compact-tag form composes with SoA plan payload better
than fn pointers (see ADR §SoA / SIMD forward
compatibility).

### R7: Plan-digest churn from output-name strings

**Risk.** If the catalog's `output_name` values differ
from existing
`sql_planner.rs:1140` arms (e.g., `"sum"` vs `"SUM"`),
plan digests change.

**Mitigation.** Phase 1 explicitly preserves observable
output names — read `sql_planner.rs:1140` arms and pin
the catalog's `output_name` field to match. Code review
diffs catalog values against pre-refactor planner output.

### R8: Bench-artifact false positive

**Risk.** A small (< 5%) variation between pre-refactor
and post-refactor aggregate rows is within noise.

**Mitigation.** No minimum-speedup gate; >10% threshold
for investigation, not contract. Single-row regressions
that survive a re-run are the real signal; one-off
noise dismissed.

### R9: cfg(test) helper drift

**Risk.** cfg(test) `aggregate_state_keyword` /
`aggregate_state_output_name` may encode different
invariants than the catalog (e.g., test-specific
keyword strings).

**Mitigation.** Phase 4 sweeps these helpers: delete if
redundant, delegate if test-specific. No silent
divergence allowed; if the helpers return strings the
catalog doesn't, that's a divergence to surface.

### R10: ADR 022 (Float MIN/MAX) interlock

**Risk.** ADR 022 needs to extend `IntMinMax` or add a
`FloatMinMax` input domain.

**Mitigation.** `AggregateInputDomain` is a private
crate-internal enum; ADR 022 adds variants without
breakage. `AggregateOutputRule::SameAsInput` already
supports identity output, so Float MIN/MAX is purely
additive against the new spine. Recorded as ADR 022
substrate-readiness.

### R11: Trait re-introduction pressure

**Risk.** A future ADR may re-raise the trait-dispatch
question.

**Mitigation.** ADR 021's no-trait decision is recorded
with full iter-0-through-iter-3 dissent context. The
SoA / SIMD forward-compat section in the ADR gives
future ADRs an evidence-base requirement for trait
re-introduction — polymorphic accumulation should make
the case alongside SoA plan-layout evidence.

### R12: COUNT vs SUM/AVG nullability

**Risk.** `nullable: bool` field on the spec may
oversimplify per-kernel nullability rules.

**Mitigation.** Spec values match existing
`aggregate_field` behavior:
- `CountAll`: `nullable: false`
- `Count`: `nullable: false`
- `Sum`: `nullable: true`
- `Min`: `nullable: true`
- `Max`: `nullable: true`
- `Avg`: `nullable: true`

Phase 3 verifies against pre-refactor field shapes.

### R13: Type domain assumption error

**Risk.** The Phase 1 catalog may under-specify type
domains relative to the actual binder.

**Mitigation.** Phase 1 reads the actual binder helpers
(`sum_output_data_type` body) and reproduces the
acceptance domain exactly. The catalog preserves
*current* behavior: signed and unsigned integer widths
(`Int8` through `Int64`, `UInt8` through `UInt64`) plus
`Float32` and `Float64` for SUM/AVG. ADR 021 makes no
claim about widening or narrowing the domain.

## Test plan

Existing test files that must stay green:

- `crates/toron/tests/architecture_test.rs` — Phase 5
  rewrites two guards; ADR 019 / ADR 020 guards adjust
  to read through the catalog.
- `crates/toron/tests/group_by_test.rs` — full grouped
  aggregate coverage.
- `crates/toron/tests/distributed_process_test.rs` —
  digest parity gate for plan-digest preservation.
- `crates/toron/tests/aggregate_projection_accounting_test.rs`
  — projection accounting unaffected.
- `crates/toron/tests/bind_aggregate.rs` (if exists) —
  binder-side coverage; error-string preservation
  checked here.
- `crates/toron/tests/planner_*.rs` — planner-side
  coverage.

No new test files required.

## Definition of done

The refactor lands when:

1. `crates/toron/src/aggregate_kernel.rs` exists with
   the catalog and helper API.
2. `mod aggregate_kernel;` private declaration in
   `lib.rs`.
3. Planner metadata delegates to catalog; plan
   digests byte-for-byte unchanged.
4. Binder metadata delegates to catalog; three
   private `*_output_data_type` helpers removed; error
   strings preserved byte-for-byte.
5. Executor `init_bound_state_for_call` introduced;
   `init_bound_states` delegates through it. Row-update
   bodies and `AggState` byte-identical.
6. `AggState: Copy` preserved; stays in `exec.rs`.
7. `aggregate_kernel_catalog_guards_are_narrow`
   replaces the ADR 020 enum-count guard;
   `bind_expr_remains_aggregate_free` added.
8. ADR 019 / ADR 020 guards adjusted to read through
   the catalog.
9. All existing tests green.
10. Bench artifact at
    `plans/impl/021_aggregate_kernel_catalog_refactor_bench_results.md`
    records digest parity and per-aggregate-row
    timings; no row regresses > 10%.
11. Plan digests byte-for-byte unchanged across the
    entire suite (refactor is observationally
    invisible).

## Phase ordering rationale

Phase 1 lands the catalog as unused code so the rest
of the suite confirms it doesn't break compilation
before any call site changes. Phase 2 (planner) is the
shallowest delegation; its digest-parity check
validates the catalog's `output_name` and SQL-name
contents before binder/executor depend on them.
Phase 3 (binder) is the deepest semantic delegation
and the highest risk for error-string drift; its
explicit grep-based fidelity check gates progress.
Phase 4 (executor) is the most conservative — only
init dispatch centralizes; row-path bodies untouched.
Phase 5 lands the new architecture guard after the
substrate it asserts about exists. Phase 6 confirms
observational invisibility via bench artifact.

Phase 5 could move earlier (Phase 1.5) — landing the
guard before migration begins so it asserts as the
substrate appears. The iter-3 / iter-4 council
preference was: keep Phase 5 separate so the guard's
assertions reference post-migration state (e.g.,
`bind_expr_remains_aggregate_free` only makes sense
after binder migration removes any latent
`AggregateFunction` references from `bind_expr`'s
scope). Implementer's choice if Phase 5 lands earlier
with deferred-assertion fragments; the merged plan
documents the iter-3 / iter-4 preference.
