# Impl Plan - ADR 171: Affine Unit CONVERT With Point/Delta Safety Fence

Pairs with `plans/adr/0171_affine_unit_convert_with_point_delta_safety_fence.md`.

Adds durable point/delta kind metadata plus a signed `offset_to_base` to shared
logical-type unit metadata, extends `CONVERT` to dispatch between affine point
conversion and linear delta conversion, and fences point arithmetic / `SUM` /
`AVG` behind a point/delta diagnostic. Comparison, `MIN`, `MAX`, and `COUNT` stay
point-safe. No executor kernel, PBC opcode, optimizer fact, storage runtime, or
descriptor policy surface.

## Anchors

| Concern | Anchor |
|---|---|
| Shared unit metadata | `crates/wendelon/src/integration/compute.rs::UnitScale`, `LogicalTypeUnit`, `LogicalTypeDefinitionCreate`, `LogicalTypeDefinition` |
| Logical-type encode/decode | `encode_logical_type_definition*`, `decode_logical_type_definition*` |
| Toron unit specs | `crates/toron/src/ir.rs::LogicalTypeSpec`, `LogicalTypeResolver::with_registry` |
| DDL unit parse | `crates/toron/src/sql_planner.rs::parse_create_logical_type_clauses`, `parse_positive_decimal_scale` |
| CONVERT desugar | `crates/toron/src/binder.rs::bind_sql_convert`, `unit_scale_to_f64` |
| Arithmetic fence | `bind_logical_arithmetic`, `bind_dimensioned_arithmetic`, `reject_logical_arithmetic_with_untyped_null` |
| Aggregate fence | `additive_unit_aggregate_logical_type` |
| Comparison/selection | `bind_dimensioned_comparison`, `aggregate_output_logical_type` `Min`/`Max` arms |
| Runtime non-surface | `crates/toron/src/exec.rs::Expr::SqlConvert` error arm, PBC/predicate pushdown files |
| Frontend parity | `crates/toron/src/rune_engine.rs` `convert()` builder |

## Phase 1 - Shared Metadata

In `crates/wendelon/src/integration/compute.rs`, add public unsuffixed unit
metadata:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitQuantityKind {
    Delta,
    Point,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UnitOffset {
    pub numerator: i64,
    pub denominator: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogicalTypeUnit {
    pub dimension: String,
    pub scale_to_base: UnitScale,
    pub quantity_kind: UnitQuantityKind,
    pub offset_to_base: UnitOffset,
}
```

Implementation notes:

- Keep `UnitScale` unchanged and positive-only.
- Add `UnitQuantityKind::default()` or equivalent returning `Delta`.
- Add `UnitOffset::ZERO = 0/1`, `is_zero`, `validate`, and `to_f64` or
  binder-side conversion.
- `UnitOffset::validate` rejects denominator zero and allows negative numerator.
- `LogicalTypeUnit::validate_for_physical` keeps the existing `Float64` and
  dimension checks, validates scale and offset, and rejects nonzero
  `offset_to_base` when `quantity_kind == Delta`.
- Use product-neutral public names. Avoid new `V1`, `Descriptor`, `Policy`, or
  Toron-branded types for this metadata.

## Phase 2 - No Premature Backward-Compatibility Scaffolding

Follow `crates/wendelon/plans/conventions/no-premature-backward-compat.md`.
Change the logical-type unit metadata shape directly:

- no `LegacyLogicalTypeUnit` / old-shape helper structs;
- no tolerant fallback decoders;
- no old-shape decode tests;
- no "reserved for later" fields or version-suffix names.

The defaulting behavior ADR 171 needs is source-level DDL behavior: unit DDL
without `POINT`/`DELTA` or `OFFSET_TO_BASE` creates `Delta` with
`UnitOffset::ZERO`.

## Phase 3 - Toron Plumbing

Mirror/pass the Wendelon metadata through every place currently carrying
`LogicalTypeUnit`:

- Toron storage/catalog descriptor sync in `crates/toron/src/storage.rs`;
- `crates/toron/src/ir.rs::LogicalTypeSpec`;
- schema/catalog/information-schema projections that expose unit metadata;
- tests and fixtures constructing `LogicalTypeUnit`.

Prefer helper constructors in tests:

```rust
fn delta_unit(dimension: &str, scale: UnitScale) -> LogicalTypeUnit
fn point_unit(dimension: &str, scale: UnitScale, offset: UnitOffset) -> LogicalTypeUnit
```

Avoid Toron-private shadow enums. Use the shared metadata directly.

## Phase 4 - SQL DDL Parsing

Extend `parse_create_logical_type_clauses` after `SCALE_TO_BASE`:

1. Parse scale with the existing `parse_positive_decimal_scale`.
2. Optionally parse `OFFSET_TO_BASE <signed decimal>`.
3. Optionally parse exactly one kind token: `POINT` or `DELTA`.
4. Pass the remaining text to the existing refinement parser.

Rules to implement and test:

- Missing kind means `Delta`.
- Missing offset means `0/1`.
- Nonzero offset with omitted kind rejects.
- Nonzero offset with `DELTA` rejects.
- `POINT` with omitted/zero offset is legal.
- `POINT`/`DELTA` and `OFFSET_TO_BASE` require `DIMENSION`.
- Unit metadata remains `DOUBLE`/`FLOAT64` only.

Add `parse_signed_decimal_offset` beside `parse_positive_decimal_scale`.
It should:

- accept optional `+` or `-`;
- reject exponent syntax;
- require digits in the same style as scale parsing;
- reduce the rational;
- check `i64` numerator and `u64` denominator overflow;
- produce `UnitOffset::ZERO` for `0`, `+0`, `-0`, and decimal zero forms.

Keep syntax ordering simple for this ADR:

```sql
DIMENSION <id> SCALE_TO_BASE <positive-decimal>
  [OFFSET_TO_BASE <signed-decimal>] [POINT|DELTA] [CHECK (...)]
```

## Phase 5 - Bind Affine CONVERT

Refactor `bind_sql_convert` into small helpers but keep the same high-level path:

1. Resolve target spec from `ctx.logical_type_specs`.
2. Bind source expression.
3. Resolve source spec through `LogicalTypeResolver::with_registry`.
4. Require source and target units, `Float64` physical carrier, matching
   dimension, and matching `quantity_kind`.
5. Emit arithmetic based on kind.
6. Stamp only the final emitted node with the target logical type.

Formula helpers:

```rust
fn unit_scale_to_f64(scale: &UnitScale) -> f64
fn unit_offset_to_f64(offset: &UnitOffset) -> f64
fn append_linear_unit_convert(...)
fn append_affine_unit_convert(...)
```

Delta path:

```text
source * source.scale_to_base / target.scale_to_base
```

Point path, emitted either literally or as reduced constants:

```text
source * (source.scale_to_base / target.scale_to_base)
  + ((source.offset_to_base - target.offset_to_base) / target.scale_to_base)
```

Keep the zero-shift/linear fast path as a single multiply where possible so ADR
167's linear gates and downstream expression-shape expectations remain stable.
The point path may append naked Float64 intermediate nodes; only the final node
gets `target_spec.id`.

Diagnostics:

- keep the existing dimension mismatch style;
- add point/delta mismatch naming source type/kind and target type/kind;
- add invalid affine transform if scale/offset conversion produces non-finite
  constants.

## Phase 6 - Point Arithmetic Fence

Extend the shared binder path; do not add frontend-specific checks.

Add helper:

```rust
fn quantity_kind_for_spec(spec: &LogicalTypeSpec) -> Option<UnitQuantityKind>
fn reject_point_arithmetic(spec: &LogicalTypeSpec, context: &str) -> BindingError
```

In `bind_logical_arithmetic`:

- keep ADR 170 identity rejects first;
- before returning `Ok(None)` for non-`Add` quantity cases or invoking
  `append_dimensioned_arithmetic`, reject if either `Quantity(spec)` is `Point`;
- leave `Delta` behavior unchanged.

In `bind_dimensioned_arithmetic`:

- add defense-in-depth: if either resolved unit is `Point`, return the same
  point/delta diagnostic. This protects future reorderings where a point could
  bypass `bind_logical_arithmetic`.

In `reject_logical_arithmetic_with_untyped_null`:

- ADR 170 already rejects quantity with untyped NULL. If the typed operand is a
  point, prefer the point-specific diagnostic; otherwise keep the existing unit
  NULL diagnostic.

For aggregates:

- In `additive_unit_aggregate_logical_type`, reject `SUM` and `AVG` for
  `Quantity(spec)` when `spec.unit.quantity_kind == Point`.
- Leave `MIN`, `MAX`, and `COUNT` untouched.

Suggested diagnostic:

```text
point quantity {type_name} does not support arithmetic for {context}; point/delta
quantity algebra is deferred, use an explicit primitive CAST for raw math
```

## Phase 7 - Behavioral Tests

Toron tests, likely in `crates/toron/tests/logical_type_ddl_test.rs`:

- `create_logical_type_affine_metadata_roundtrips`
  - create `Celsius`, `Kelvin`, `Fahrenheit`, `CelsiusDelta`,
    `FahrenheitDelta`;
  - assert kind and offset are present in resolved metadata.
- `convert_temperature_point_uses_affine_offset_and_preserves_target_identity`
  - `CONVERT(CAST(0.0 AS Celsius), Kelvin)` returns `273.15`;
  - output logical type is `Kelvin`.
- `convert_fahrenheit_point_to_celsius_point_uses_offset`
  - `CONVERT(CAST(32.0 AS Fahrenheit), Celsius)` returns approximately `0.0`.
- `convert_temperature_delta_uses_scale_only`
  - `CONVERT(CAST(9.0 AS FahrenheitDelta), CelsiusDelta)` returns approximately
    `5.0`.
- `point_delta_convert_mismatch_rejects`
  - point to delta and delta to point reject.
- `point_quantity_arithmetic_rejects_until_delta_result_identity_exists`
  - point `+`, `-`, `*`, `/`, `SUM`, and `AVG` reject.
- `point_comparison_and_selection_stay_valid`
  - same-unit point comparison works;
  - same-dimension different-unit comparison rejects until explicit `CONVERT`;
  - `MIN`/`MAX` preserve point identity;
  - `COUNT` is naked.
- `linear_unit_ddl_defaults_to_delta_zero_offset`
  - DDL without new clauses behaves as before.
- `linear_unit_convert_arithmetic_and_aggregation_regressions_stay_green`
  - Mile/Kilometer `CONVERT`, same-unit linear arithmetic, scalar scaling,
    `SUM(Mile)`, and ADR 167/168/170 gates pass.
- `nonzero_offset_requires_point`
  - omitted-kind nonzero offset and explicit-delta nonzero offset reject.
- `rune_affine_convert_matches_sql_bind_path`
  - Rune `convert()` produces the same point value and target identity as SQL.
- `affine_metadata_survives_descriptor_resync`
  - created point/delta metadata survives catalog or descriptor reload.

Wendelon integration contract tests:

- current `LogicalTypeUnit` round-trips with `Point` and a signed offset;
- validation rejects zero offset denominator;
- validation rejects nonzero delta offset;
- public names stay unsuffixed and brand-clean.

Use tolerance for Fahrenheit because `0.5555555555555556` is a finite decimal
approximation of `5/9`. Prefer exact Celsius/Kelvin assertions for the headline.

## Phase 8 - Architecture Guards

Add ADR 171 guards, preferably in the nominal/logical-type architecture suite or
a small dedicated logical-unit file if the current file is too crowded:

- `adr171_unit_metadata_is_brand_clean_and_unsuffixed`
- `adr171_unit_metadata_has_kind_and_signed_offset`
- `adr171_no_premature_legacy_decode_scaffolding`
- `adr171_delta_offset_rejects`
- `adr171_affine_convert_is_bind_desugared_not_executor_kernel`
- `adr171_convert_dispatches_by_quantity_kind`
- `adr171_point_quantities_do_not_reach_linear_dimensioned_arithmetic`
- `adr171_sum_avg_reject_point_quantities`
- `adr171_frontends_share_affine_convert_bind_path`
- `adr171_no_executor_pbc_or_optimizer_surface`
- `adr171_cast_path_still_contains_no_unit_transform`

Retarget prior guards:

- ADR 167's "no affine offset" guard must become "affine offset is present only
  as unit metadata, and point arithmetic is fenced."
- Any source-block guard that assumes `LogicalTypeUnit` has only
  `dimension + scale_to_base` must be updated to the new shape rather than
  quietly deleted.

Guard details worth pinning:

- `bind_sql_convert` references both `quantity_kind` and `offset_to_base`.
- `exec.rs` contains no `evaluate_sql_convert`.
- Rune still constructs `Expr::SqlConvert`.
- `bind_dimensioned_arithmetic` has point defense-in-depth.
- PBC/predicate-pushdown/optimizer files do not gain affine-specific branches.
- Public metadata names contain no new `V1`, `Descriptor`, `Policy`, or
  Toron-branded names.

## Verification

Run at least:

```bash
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p wendelon --all-targets logical_type
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test -- affine
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test -- point
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test -- convert
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test -- strict
scripts/cargo-cgroup test -p toron --test architecture_test -- adr171
scripts/cargo-cgroup test -p toron --test architecture_test -- adr167
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

If exact Wendelon test filters differ, preserve the coverage: metadata roundtrip,
validation, Toron affine behavior, Rune parity, full architecture binary, and
ADR 167/168/170 regressions.

## Risks

- **Offset-only regression:** if implementation uses nonzero offset as the whole
  affine discriminator, `Kelvin` and `KelvinDelta` collapse. Review for explicit
  `quantity_kind` usage.
- **Premature compatibility drift:** do not add legacy decode structs or
  old-shape tests for this in-flight metadata change.
- **Delta offset validation:** default kind is `Delta`; a nonzero offset with no
  `POINT` token must reject.
- **Formula placement:** only point `CONVERT` applies offsets; delta `CONVERT`
  remains scale-only.
- **Point leak-through:** point quantities must not reach linear
  `bind_dimensioned_arithmetic` or `SUM`/`AVG` identity preservation.
- **Fast-path drift:** zero-offset delta conversion should keep the ADR 167
  simple multiply shape where feasible.
- **Scope creep:** point/delta result algebra and converted predicate pushdown
  are follow-ons, not part of 171.

## Deferred

Point/delta result algebra; durable delta result identity selection;
`AVG(Point)` semantics; derived dimensions and ratios; unit display formatting;
converted predicate pushdown; Rune optimizer cockpit and receipt API;
duration-unit `CONVERT`; statistical aggregate unit policy beyond `SUM`/`AVG`;
richer Rune unit constructors.
