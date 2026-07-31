# Impl Plan - ADR 168: Type/Unit-Aware Aggregation

Pairs with `plans/adr/168_type_unit_aware_aggregation.md`.

This is a contained Toron binder-schema change. It extends the ADR 113 aggregate
output identity helper so `SUM`/`AVG` preserve only declared linear unit logical
types. It does not change aggregate kernels, execution, PBC, Wendelon, storage,
or optimizer facts.

## Anchors

| Concern | Anchor |
|---|---|
| Aggregate schema decision | `aggregate_schema` -> `aggregate_field` -> `aggregate_output_logical_type` in `crates/toron/src/binder.rs` |
| Selector aggregate rule | ADR 113 `MIN`/`MAX` branch in `aggregate_output_logical_type` |
| Durable unit metadata | `LogicalTypeSpec.unit: Option<LogicalTypeUnit>` in `crates/toron/src/ir.rs` |
| Durable lookup pattern | `LogicalTypeResolver::with_registry(...)`, already used by ADR 167 unit binding |
| Pruned-plan rebuild | `push_down_scan_projection` -> `optimize_for_required`; the aggregate arm rebuilds `aggregate_schema` after ordinal rewrites |
| Physical kernels | `crates/toron/src/aggregate_kernel.rs` remains Arrow-physical only |
| Tests | behavior in `logical_type_ddl_test.rs`; guards in `tests/architecture/nominal_type_identity.rs` |

## Phase 1 - Thread the Logical-Type Registry

Update the aggregate schema helpers:

```rust
fn aggregate_schema(
    group_expr: &BoundExprProgram,
    aggr_expr: &[BoundAggregateCall],
    logical_types: &LogicalTypeRegistry,
) -> SchemaRef

fn aggregate_field(
    call: &BoundAggregateCall,
    logical_types: &LogicalTypeRegistry,
) -> Field

fn aggregate_output_logical_type(
    call: &BoundAggregateCall,
    output_type: &DataType,
    logical_types: &LogicalTypeRegistry,
) -> LogicalTypeId
```

Update the normal aggregate bind path to call:

```rust
let logical_types = self.catalog.logical_type_registry();
let output_schema = aggregate_schema(&group_expr, &bound_aggr_expr, &logical_types);
```

The exact local binding can follow existing borrowing patterns; the invariant is
that `aggregate_output_logical_type` receives the same catalog registry that
bound logical casts and unit expressions.

Also update pruning:

```rust
fn push_down_scan_projection(
    plan: ExecutionPlan,
    logical_types: &LogicalTypeRegistry,
) -> Result<ExecutionPlan, BindingError>

fn optimize_for_required(
    plan: ExecutionPlan,
    required_output: Option<BTreeSet<ColumnOrdinal>>,
    logical_types: &LogicalTypeRegistry,
) -> Result<(ExecutionPlan, OrdinalRemap), BindingError>
```

Every recursive `optimize_for_required` call must forward `logical_types`. The
aggregate arm must rebuild:

```rust
let output_schema = aggregate_schema(&group_expr, &aggr_expr, logical_types);
```

`bind_with_trace` should pass `catalog.logical_type_registry()` into
`push_down_scan_projection`. Unit tests that call `optimize_for_required`
directly can pass `&LogicalTypeRegistry::default()` unless they are explicitly
testing unit-aware pruning.

## Phase 2 - Add the Unit-Only `SUM`/`AVG` Rule

Keep the existing ADR 113 selector branch for `MIN`/`MAX`. Add a helper for
combination aggregates:

```rust
fn additive_unit_aggregate_logical_type(
    call: &BoundAggregateCall,
    output_type: &DataType,
    logical_types: &LogicalTypeRegistry,
) -> LogicalTypeId {
    let Some(logical_type) = call.args.root_logical_type(0) else {
        return LogicalTypeId::NAKED;
    };
    let Some(input_type) = call.args.root_type(0) else {
        return LogicalTypeId::NAKED;
    };
    if logical_type.is_naked() || input_type != output_type {
        return LogicalTypeId::NAKED;
    }
    let Some(spec) = LogicalTypeResolver::with_registry(logical_types).spec(logical_type) else {
        return LogicalTypeId::NAKED;
    };
    if spec.unit.is_some() && spec.physical == *output_type {
        logical_type
    } else {
        LogicalTypeId::NAKED
    }
}
```

Then partition `aggregate_output_logical_type`:

- `AggregateFunction::Min | AggregateFunction::Max` -> existing selector logic;
- `AggregateFunction::Sum | AggregateFunction::Avg` ->
  `additive_unit_aggregate_logical_type`;
- `AggregateFunction::CountAll | AggregateFunction::Count |
  AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_)` ->
  `LogicalTypeId::NAKED`.

Do not use `logical_type_spec(id)` for this lookup. That is seed-only and cannot
see durable unit definitions created by `CREATE LOGICAL TYPE`.

## Phase 3 - Behavioral Gates

Add or extend tests in `crates/toron/tests/logical_type_ddl_test.rs`.

- `aggregation_is_type_unit_aware`
  - create/use `CustomerId`, `Mile`, `Kilometer`, and `Kilogram`;
  - bind and execute a query proving `MIN(customer_id)` carries `CustomerId`,
    `SUM(distance)` carries `Mile`, `SUM(customer_id)` is naked, and
    `COUNT(distance)` is naked;
  - assert values as well as schema logical identities.
- `sum_avg_preserve_additive_unit_identity`
  - `SELECT SUM(distance), AVG(distance) FROM trips`;
  - both aggregate output fields carry `Mile`.
- `aggregate_unit_convert_preserves_quantity`
  - `SELECT CONVERT(SUM(distance), Kilometer) FROM trips`;
  - output carries `Kilometer` and value is `1.609344` for the current fixture.
- `sum_after_convert_preserves_target_unit`
  - `SELECT SUM(CONVERT(distance, Kilometer)) FROM trips` carries `Kilometer`.
- `sum_avg_of_non_unit_nominal_identity_are_naked`
  - `SUM(CustomerId)` and `AVG(CustomerId)` output no logical metadata.
- `count_of_dimensioned_unit_is_naked`
  - `COUNT(distance)` and `COUNT(*)` are naked.
- `aggregated_unit_composes_with_167_safety`
  - `SUM(distance) + SUM(mass)` rejects with `dimension mismatch`;
  - `SUM(distance) + SUM(other_distance)` rejects with `require explicit CONVERT`.
- `group_by_with_type_unit_aggregate_preserves_identity`
  - group-key identity survives through `schema_for_exprs(group_expr)`;
  - aggregate output identities survive through `aggregate_field`.
- `unit_aggregate_identity_flows_through_ctas`
  - CTAS of `SUM(distance)` persists and decodes the unit identity through the
    existing ADR 088/089 metadata path.
- `sum_mixed_unit_expression_requires_convert_before_aggregate`
  - `SELECT SUM(distance + other_distance) FROM trips` rejects in expression
    binding before aggregation.

If helper coverage for `LINEAR_REGRESS` or `APPROX_QUANTILE` is awkward in this
file, keep their naked behavior pinned by the architecture guard and name the
behavior test as deferred rather than broadening the ADR.

## Phase 4 - Architecture Guards

In `crates/toron/tests/architecture/nominal_type_identity.rs`:

- Retarget `adr113_min_max_preserve_aggregate_identity`.
  - Keep the `Min | Max` selector assertions.
  - Keep the input/output physical equality assertion.
  - Stop asserting `Sum` and `Avg` are always in the naked arm.
  - Assert `Sum | Avg` route through the additive-unit helper.
  - Keep `CountAll`, `Count`, `LinearRegress`, and `ApproxQuantile(_)` naked.
- Retarget `adr089_group_keys_preserve_identity_aggregate_outputs_are_adr113_owned`.
  - It should still prove group keys are emitted by `schema_for_exprs(group_expr)`.
  - It should still prove aggregate output identity is centralized in
    `aggregate_output_logical_type`.
  - It should allow the registry argument through `aggregate_schema` and
    `aggregate_field`.

Add ADR 168 guards:

- `adr168_sum_avg_preserve_only_declared_unit_identity` - source contains the
  `Sum | Avg` branch, registry resolver, `unit.is_some()`, and physical equality
  checks.
- `adr168_non_unit_nominal_sum_avg_stay_naked` - unit-missing or unresolved
  logical ids return `LogicalTypeId::NAKED`.
- `adr168_aggregate_schema_threads_logical_registry_through_pruning` -
  signatures and aggregate rebuild calls carry `LogicalTypeRegistry`.
- `adr168_count_and_statistical_aggregates_stay_naked` - count and experimental
  aggregate arms remain naked.
- `adr168_aggregate_kernel_executor_pbc_stay_physical_only` - reuse/extend the
  ADR 113 physical-only guard over `aggregate_kernel.rs`, `exec.rs`, and
  `pbc.rs`.
- `adr168_no_wendelon_wire_or_storage_surface` - no Wendelon schema, descriptor,
  storage, or PBC opcode change.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test aggregation_is_type_unit_aware
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test sum_avg_preserve_additive_unit_identity
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test aggregate_unit_convert_preserves_quantity
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test aggregated_unit_composes_with_167_safety
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test
scripts/cargo-cgroup test -p toron --test architecture_test adr168
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

## Risks

- **Durable unit lookup miss.** Seed-only logical type resolution will make
  `SUM(Mile)` stay naked. Use `LogicalTypeResolver::with_registry`.
- **Generic nominal leakage.** Accidentally preserving `SUM(CustomerId)` would
  violate ADR 113's computation-erases rule. Pin with behavior and guards.
- **Pruning drift.** Required-column pruning rebuilds aggregate schemas; thread
  the registry there or optimized plans can drop units.
- **Over-claiming statistical aggregates.** Keep `ApproxQuantile` and
  `LinearRegress` naked in this cut.
- **Duplicating ADR 167 safety.** ADR 168 should only stamp aggregate output
  metadata. Downstream arithmetic, comparison, and `CONVERT` behavior should
  flow through the existing 167 paths.

## Deferred

Strict additive types that reject non-quantity arithmetic and aggregates
together; affine units and temperature; point/delta algebra; window and
DISTINCT aggregate identity propagation; derived units and ratios; more carrier
types; unit display formatting; richer Rune unit syntax and gauges.
