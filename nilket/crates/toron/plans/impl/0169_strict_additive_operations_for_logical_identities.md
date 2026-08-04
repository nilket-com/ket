# Impl Plan - ADR 169: Strict Additive Operations for Logical Identities

Pairs with `plans/adr/0169_strict_additive_operations_for_logical_identities.md`.

This is a contained Toron binder-domain change. It rejects non-unit logical
identities in binary `+`, `SUM`, and `AVG`, while preserving ADR 167 unit
arithmetic and ADR 168 unit aggregation. No executor, aggregate-kernel, PBC,
Wendelon, storage, wire, or optimizer-fact change lands here.

## Anchors

| Concern | Anchor |
|---|---|
| Scalar arithmetic seam | `bind_arithmetic` in `crates/toron/src/binder.rs` |
| Current unit arithmetic gate | `bind_dimensioned_arithmetic`, `logical_unit_for_node`, `require_compatible_units` |
| Durable logical type lookup | `LogicalTypeResolver::with_registry(...)` |
| Aggregate schema decision | `aggregate_schema -> aggregate_field -> aggregate_output_logical_type` |
| ADR 168 additive aggregate helper | `additive_unit_aggregate_logical_type` |
| Selector aggregate rule | `AggregateFunction::Min | AggregateFunction::Max` branch in `aggregate_output_logical_type` |
| Rune frontend parity | `RuneExpr::add` / `RuneExpr::cast` + `type_i64()` lower to `Expr::Add` / `Expr::Cast`; `sum` / `avg` lower to the same aggregate calls as SQL |
| Behavior tests | `crates/toron/tests/logical_type_ddl_test.rs` |
| Architecture tests | `crates/toron/tests/architecture/nominal_type_identity.rs` |

## Phase 1 - Classify Logical Additive Domains

Add a binder-local helper near the ADR 167 unit helpers:

```rust
enum AdditiveLogicalDomain {
    Naked,
    Quantity(LogicalTypeSpec),
    Identity(LogicalTypeSpec),
}

fn additive_logical_domain_for_node(
    ctx: &ExprBindContext<'_>,
    builder: &BoundExprProgramBuilder,
    id: BoundExprId,
) -> Result<AdditiveLogicalDomain, BindingError>
```

Behavior:

- `LogicalTypeId::NAKED` -> `Naked`;
- non-naked id resolves through
  `LogicalTypeResolver::with_registry(ctx.logical_type_registry).spec(id)`;
- resolved spec with `unit.is_some()` -> `Quantity(spec)`;
- resolved spec with `unit.is_none()` -> `Identity(spec)`;
- unresolved non-naked id fails closed with a binding error.

`unit.is_none()` intentionally includes refinement-only logical types such as
`PersonId AS BIGINT CHECK (VALUE > 0)`. The refinement predicate is still a
logical receipt; additive carrier arithmetic requires explicit primitive
erasure first.

Reuse this helper for scalar addition. If it is also useful for aggregates,
add a sibling helper over `LogicalTypeRegistry` and `LogicalTypeId`:

```rust
fn additive_logical_domain_for_type(
    logical_type: LogicalTypeId,
    logical_types: &LogicalTypeRegistry,
) -> Result<AdditiveLogicalDomain, BindingError>
```

Keep the helpers private to `binder.rs` unless a local pattern strongly argues
otherwise.

## Phase 2 - Gate Binary Addition Before Physical Fallback

Update `bind_arithmetic` so `BoundBinaryOp::Add` checks logical additive domains
after temporal/calendar handling and before ordinary numeric fallback.

Suggested shape:

```rust
if matches!(op, BoundBinaryOp::Add) {
    if let Some(output) = bind_logical_addition(
        builder, ctx, left, right, &left_type, &right_type,
    )? {
        return Ok(output);
    }
}
```

`bind_logical_addition` should implement:

- `Naked + Naked` -> `Ok(None)`; let existing physical arithmetic continue.
- `Quantity + Quantity` -> existing ADR 167 same-unit path:
  `require_compatible_units`, then `append_dimensioned_arithmetic` with the left
  logical id.
- `Quantity + Naked` or `Naked + Quantity` -> existing ADR 167 reject:
  "unit arithmetic requires both operands to be the same unit; CONVERT
  explicitly before combining with a scalar".
- any `Identity + _` or `_ + Identity` -> new non-additive reject.

Leave `Sub`, `Mul`, and `Div` on the existing `bind_dimensioned_arithmetic`
path in this ADR. Do not broaden the behavior change beyond binary `+`.

Both SQL and Rune lower binary `+` to `Expr::Add` -> `bind_arithmetic`, so this
single gate covers both frontends; do not add a Rune-specific arithmetic path.
The only Rune-side delta is a parity behavior test (Phase 4).

The non-additive diagnostic can be a `BindingError::TypeMismatch` with a stable,
high-signal message if adding a new enum variant is too much churn. Prefer a
single helper:

```rust
fn non_additive_logical_type_error(type_name: &str, context: &str) -> BindingError
```

Diagnostic text should include:

- the logical type name;
- that it is not an additive quantity;
- the operation context (`+`, `SUM`, or `AVG`);
- explicit primitive `CAST` as the escape hatch.

## Phase 3 - Retarget `SUM` and `AVG`

ADR 168 made aggregate schema helpers return plain fields. ADR 169 needs the
`SUM`/`AVG` path to fail bind for Identity operands, so thread `Result` through
the aggregate schema helpers:

```rust
fn aggregate_schema(
    group_expr: &BoundExprProgram,
    aggr_expr: &[BoundAggregateCall],
    logical_types: &LogicalTypeRegistry,
) -> Result<SchemaRef, BindingError>

fn aggregate_field(
    call: &BoundAggregateCall,
    logical_types: &LogicalTypeRegistry,
) -> Result<Field, BindingError>

fn aggregate_output_logical_type(
    call: &BoundAggregateCall,
    output_type: &DataType,
    logical_types: &LogicalTypeRegistry,
) -> Result<LogicalTypeId, BindingError>
```

Update all callers:

- normal aggregate bind path in `Binder::bind_plan_with_trace`;
- `optimize_for_required` aggregate rebuild path;
- internal unit tests that call pruning helpers, if signatures require it.

Retarget `additive_unit_aggregate_logical_type` to return
`Result<LogicalTypeId, BindingError>`:

- naked root logical id -> `Ok(LogicalTypeId::NAKED)`;
- resolved Identity -> `Err(non_additive_logical_type_error(..., "SUM"))` or
  `"AVG"` before the physical-equality preservation check;
- resolved Quantity with matching physical output -> `Ok(logical_type)`;
- resolved Quantity with mismatched physical output -> `Ok(LogicalTypeId::NAKED)`;
- unresolved non-naked id -> fail closed.

The exact order can follow the existing stable aggregate metadata behavior, but
the externally visible invariant must hold: `SUM(customer_id)` and
`AVG(customer_id)` reject, while `SUM(raw)`/`AVG(raw)` remain naked and
`SUM(CAST(customer_id AS BIGINT))`/`AVG(CAST(customer_id AS BIGINT))` remain
naked.

Keep `CountAll`, `Count`, `LinearRegress`, and `ApproxQuantile(_)` naked. Keep
`Min | Max` behavior from ADR 113/168 unchanged.

## Phase 4 - Behavioral Tests

Add tests in `crates/toron/tests/logical_type_ddl_test.rs`. Reuse the ADR
167/168 durable logical type fixtures where possible.

Headline:

- `strict_additive_operations_reject_identity`
  - `customer_id + 1` rejects;
  - `customer_id + customer_id` rejects;
  - `SUM(customer_id)` rejects;
  - `AVG(customer_id)` rejects;
  - all errors contain the non-additive diagnostic.

Supporting:

- `explicit_cast_erases_identity_for_addition`
  - `CAST(customer_id AS BIGINT) + 1` executes;
  - result has no logical metadata.
- `explicit_cast_sum_avg_erasure_stays_valid`
  - `SUM(CAST(customer_id AS BIGINT))` and
    `AVG(CAST(customer_id AS BIGINT))` bind/execute and are naked.
- `quantity_addition_and_aggregation_still_work`
  - `distance + distance`, `SUM(distance)`, and `AVG(distance)` keep `Mile`;
  - `CONVERT(SUM(distance), Kilometer)` still scales.
- `identity_comparison_and_selection_still_allowed`
  - equality or ordering comparison over `CustomerId` still binds;
  - `MIN(customer_id)` and `MAX(customer_id)` preserve `CustomerId`;
  - `COUNT(customer_id)` remains naked.
- `naked_numeric_addition_and_sum_stay_unchanged`
  - raw numeric addition, `SUM(raw)`, and `AVG(raw)` still execute.
- `refinement_identity_addition_rejects`
  - `id + 0`, `id + id`, `SUM(id)`, and `AVG(id)` over a refinement-only
    logical type such as `PersonId AS BIGINT CHECK (VALUE > 0)` reject with the
    non-additive diagnostic;
  - `CAST(id AS BIGINT) + 0` remains valid and produces a naked numeric output.
- `rune_identity_additive_rejects_and_cast_erases`
  - build through the Rune frontend: `customer_id.add(1)`,
    `sum(customer_id)`, and `avg(customer_id)` reject with the non-additive
    diagnostic;
  - `customer_id.cast(type_i64()).add(1)`,
    `sum(customer_id.cast(type_i64()))`, and
    `avg(customer_id.cast(type_i64()))` bind and execute as naked numeric;
  - proves the scalar and aggregate gates plus the escape hatch are
    frontend-shared, not SQL-only.
- Keep existing ADR 167 tests green:
  - same-unit addition;
  - same-dimension different-unit addition reject;
  - cross-dimension addition reject;
  - unit plus naked numeric reject.
- Retarget ADR 168 tests:
  - `sum_avg_of_non_unit_nominal_identity_are_naked` becomes a reject test;
  - any architecture/source guard with the same claim becomes an ADR 169
    reject guard.

## Phase 5 - Architecture Guards

In `crates/toron/tests/architecture/nominal_type_identity.rs`:

- Add `adr169_addition_gates_logical_identity_before_numeric_fallback`.
  Assert the `Add` path classifies logical domains before ordinary
  `arithmetic_output_type` fallback and uses
  `LogicalTypeResolver::with_registry`.
- Add `adr169_non_unit_logical_identity_is_not_treated_as_naked`.
  Assert resolved `unit.is_none()` goes to a non-additive reject path, not
  `Ok(None)`.
- Add `adr169_sum_avg_reject_non_additive_identity`.
  Retarget `adr168_non_unit_nominal_sum_avg_stay_naked`; it should now look for
  reject behavior in the `Sum | Avg` helper.
- Add `adr169_selection_cardinality_and_comparison_stay_identity_aware`.
  Assert `Min | Max` and `Count` arms are not routed through the new
  non-additive reject, and comparison code still calls the nominal compatibility
  path.
- Add `adr169_identity_additive_gate_is_frontend_shared`.
  Assert the additive domain check sits in the shared `bind_arithmetic`/aggregate
  seam and that `rune_engine.rs` has no parallel arithmetic gate. Mirrors
  `adr167_frontends_share_convert_bind_path`.
- Add or extend `adr169_no_runtime_storage_or_wire_surface`.
  Source blocks for `aggregate_kernel.rs`, `exec.rs`, `pbc.rs`, and Wendelon
  schema/storage/wire surfaces should not gain logical additive domain checks.

Retarget any stale ADR 168 guard that says non-unit `SUM`/`AVG` stay naked.
Keep ADR 167 unit arithmetic guards intact.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test strict_additive_operations_reject_identity
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test explicit_cast_erases_identity_for_addition
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test explicit_cast_sum_avg_erasure_stays_valid
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test quantity_addition_and_aggregation_still_work
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test identity_comparison_and_selection_still_allowed
scripts/cargo-cgroup test -p toron --test rune_relation_handles_test rune_identity_additive_rejects_and_cast_erases
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test
scripts/cargo-cgroup test -p toron --test architecture_test adr169
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

If the signature changes in aggregate schema helpers touch many compile sites,
run `scripts/cargo-cgroup test -p toron --test group_by_test` as an additional
aggregate regression gate.

## Risks

- **Behavior change.** `customer_id + customer_id` and `SUM(customer_id)` were
  accepted before this ADR. The change is intentional, but run a grep/audit for
  existing tests or fixtures relying on nominal identity carrier arithmetic.
- **Overreach into comparison.** Identities must remain comparable/selectable.
  Keep the new gate out of `bind_comparison` and `MIN`/`MAX`.
- **Perturbing units.** `Mile + Mile`, `SUM(Mile)`, `AVG(Mile)`, and
  `CONVERT(SUM(Mile), Kilometer)` must remain unchanged.
- **Aggregate helper churn.** Returning `Result` through `aggregate_schema` and
  pruning rebuilds can invert source-shape guards. Reconcile ADR 089, 113, and
  168 guards in the same cut.
- **Diagnostic drift.** The error should guide users to explicit primitive
  `CAST`, not imply `CONVERT` can make an Identity additive.

## Deferred

Subtraction and point/delta semantics; non-unit multiplication/division
strictness; affine units and temperature; derived dimensions and ratios; unit
display formatting; `DISTINCT`/window aggregate identity policy; statistical
aggregate unit propagation; richer Rune erasure syntax.
