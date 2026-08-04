# ADR 167: CONVERT - Dimensioned Unit Coordinate Transforms

**Status:** Promoted.

ADR 167 adds the first dimensioned-unit operation on the ADR087-092 nominal
identity spine. A unit such as `Mile` or `Kilometer` is a durable logical type
with optional unit metadata: a dimension and a linear scale to that dimension's
base coordinate. `CONVERT(expr, TargetUnit)` transforms a value within one
dimension and carries the target logical identity forward.

The product is not the multiplication. The product is bind-time dimensional
safety: `Mile -> Kilogram`, `Mile + Kilogram`, and `Mile + 3.0` reject before a
query runs.

## Grounding

- `LogicalTypeSpec` currently carries `{ id, name, physical, refinement }`; it
  has no unit metadata.
- Wendelon's `LogicalTypeDefinition` has the same durable logical-type shape and
  is the schema authority for logical type definitions.
- SQL logical CAST is already transient IR and resolves logical names through
  the durable path before bind. ADR091/092 deliberately reject seed-first SQL
  logical name lookup.
- `ExprBindContext` currently carries `logical_casts` by name. CONVERT also
  needs to resolve a source column's `LogicalTypeId` back to a full
  `LogicalTypeSpec`.
- `rune_engine.rs` already has expression builders, so a minimal Rune CONVERT
  helper can lower to the same IR as SQL.
- Numeric arithmetic falls through to the physical numeric lattice after
  temporal/calendar special cases. Unit safety must run before that fallback.

## Decisions

### D1 - CAST and CONVERT stay separate

CAST keeps ADR092 semantics. It may perform ordinary physical cast work needed
to produce the target logical type's physical carrier, then stamps or reconciles
the logical identity. It never performs a unit transform.

`CAST(CAST(1.0 AS Mile) AS Kilometer)` rejects as a logical identity conflict.
The legal spelling is explicit:

```sql
CONVERT(CAST(1.0 AS Mile), Kilometer)
```

### D2 - Units are durable logical-type metadata

Extend Wendelon's logical type definition, Wendelon client create/definition
requests, and Toron's `LogicalTypeSpec` with optional unit metadata:

```rust
pub struct LogicalTypeUnit {
    pub dimension: String,
    pub scale_to_base: UnitScale,
}

pub struct UnitScale {
    pub numerator: u64,
    pub denominator: u64,
}
```

The promoted Rust names must remain brand-clean and unsuffixed. `dimension` is a
one-part ASCII identifier. `scale_to_base` is a positive rational; `Mile` can be
represented as `201168/125` metres.

There is no parallel unit registry and no built-in SQL seed shortcut for units.
Tests create durable logical types.

### D3 - Linear Float64 only

A logical type with unit metadata must have physical `Float64`.

This avoids the first-cut traps:

- integer truncation after conversion;
- Float32 downcast ambiguity;
- affine offset handling without a point/delta model;
- decimal carrier semantics before the exact numeric story exists.

Temperature, affine offsets, point/delta quantities, Float32, decimal carriers,
and duration integration are deferred.

### D4 - DDL extends CREATE LOGICAL TYPE

The current logical-type DDL grows an optional unit clause:

```sql
CREATE LOGICAL TYPE Mile AS DOUBLE
  DIMENSION Length SCALE_TO_BASE 1609.344;

CREATE LOGICAL TYPE Kilometer AS DOUBLE
  DIMENSION Length SCALE_TO_BASE 1000;

CREATE LOGICAL TYPE CustomerId AS BIGINT;
```

If `DIMENSION` appears, `SCALE_TO_BASE` is required. If unit metadata appears on
anything other than `DOUBLE`/`FLOAT64`, bind rejects. Existing non-unit logical
types remain valid.

### D5 - CONVERT lowers as transient expression IR

SQL recognizes a special function:

```sql
CONVERT(expr, Kilometer)
```

and lowers it to transient IR:

```rust
Expr::SqlConvert {
    expr,
    target_logical_name: "Kilometer",
}
```

Rune gets a minimal expression builder, for example
`convert(expr, "Kilometer")`, that lowers to the same IR. Rich Rune method sugar
such as `distance.to(Kilometer)` is deferred. The invariant is that SQL and Rune
share the same binder path.

### D6 - CONVERT carries target identity

`CONVERT(distance_mile, Kilometer)` produces a `Float64` value stamped with the
`Kilometer` logical type. The output is not naked. This is required for chained
conversion, downstream arithmetic safety, and CTAS/schema metadata
preservation.

### D7 - CONVERT is bind-desugared, not an executor primitive

For source unit `S` and target unit `T` in the same dimension:

```text
target_value = source_value * S.scale_to_base / T.scale_to_base
```

The binder folds literal conversions when possible. Otherwise it appends
existing numeric arithmetic nodes with a `Float64` factor and stamps the final
node with the target logical type. No executor kernel, scan opcode, PBC opcode,
optimizer fact, or storage pushdown claim lands in this ADR.

### D8 - Unit arithmetic is explicit and safe

When either operand carries unit metadata, the binder runs unit rules before the
ordinary numeric lattice:

- `unit + same_unit` and `unit - same_unit` are allowed and preserve the unit.
- `unit + same_dimension_different_unit` rejects with an explicit CONVERT hint.
- `unit + other_dimension` rejects as a dimension mismatch.
- `unit + naked_numeric` rejects.
- `unit * naked_numeric`, `naked_numeric * unit`, and `unit / naked_numeric`
  are allowed and preserve the unit.
- `unit * unit`, `unit / unit`, and `naked_numeric / unit` reject because
  derived dimensions and dimensionless ratios are deferred.
- Comparisons follow the same rule: same unit ok; same dimension but different
  unit requires explicit CONVERT; cross dimension and unit-vs-naked reject.

Do not weaken non-unit nominal behavior. `CustomerId + ProductId` still rejects
through the existing nominal identity path.

### D9 - Durable-only SQL resolution is load-bearing

Do not add `Mile`, `Kilometer`, `Meter`, or `Kilogram` as static SQL seed names.
That would invert ADR091/092's seed-exclusion guards. The examples are durable
logical types created by DDL or test catalog setup.

### D10 - Visibility without automation

The first cut should make unit metadata visible enough to debug and review:
catalog round-trip tests are required, and `information_schema.logical_types`
may expose `dimension` and `scale_to_base` if that can be done without widening
scope.

This ADR does not rewrite predicates such as
`CONVERT(miles, Kilometer) > 10` into raw column predicates. That is a named
pushdown follow-on.

## Headline gate

`convert_cast_mile_to_kilometer_preserves_quantity_and_carries_target_identity`

Proof shape:

1. Create durable logical types:
   `Mile(Float64, Length, 1609.344)` and
   `Kilometer(Float64, Length, 1000)`.
2. Bind and execute `SELECT CONVERT(CAST(1.0 AS Mile), Kilometer) AS km`.
3. Assert value `1.609344` within a narrow tolerance.
4. Assert the output logical identity is `Kilometer`.
5. Assert there is no executor CONVERT kernel and no PBC opcode.

Pair it with `rune_convert_matches_sql_convert_bind_path` if the Rune helper
lands in the same implementation.

## Supporting gates

- `cast_between_units_is_not_convert`
- `convert_rejects_naked_source`
- `convert_rejects_nondimensioned_target`
- `convert_rejects_cross_dimension`
- `convert_between_same_units_carries_target_identity_without_aliasing_source`
- `same_unit_add_preserves_unit`
- `same_dimension_different_unit_arithmetic_requires_convert`
- `cross_dimension_arithmetic_rejects`
- `unit_plus_naked_numeric_rejects`
- `dimensioned_scalar_multiplication_preserves_unit`
- `derived_dimension_multiplication_is_deferred`
- `dimensioned_comparison_requires_same_unit_or_explicit_convert`
- `create_logical_type_unit_metadata_survives_catalog_resync`
- `unit_metadata_requires_float64_and_positive_scale`
- `unit_sql_names_are_durable_only_not_seeded`
- `convert_expression_is_not_pbc_pushdown`

## Architecture guards

- `adr167_cast_path_contains_no_unit_transform`
- `adr167_convert_is_transient_ir_and_bind_desugared`
- `adr167_no_executor_kernel_or_pbc_opcode`
- `adr167_units_are_durable_logical_type_metadata_not_arrow_sidecar`
- `adr167_unit_sql_resolution_is_durable_only_not_seed_first`
- `adr167_dimensioned_arithmetic_rejects_implicit_unit_coercion`
- `adr167_linear_float64_only_no_affine_offset`
- `adr167_frontends_share_convert_bind_path`
- `adr167_wendelon_unit_names_are_brand_clean_and_unsuffixed`

Retarget ADR092 guards that hard-code `logical_casts` as the only expression
context field. The invariant to preserve is durable logical-type name resolution
before bind, not the old field name.

## Consensus trail

All peers converge on CONVERT, CAST/CONVERT separation, linear-first scope, and
explicit mixed-unit arithmetic. The real fork is seeded built-in units versus
durable metadata. Gemini and Codex choose durable metadata; Claude's seeded
built-in path is intentionally rejected because it conflicts with ADR091/092's
seed-exclusion posture and does not support stored unit columns.

Claude's frontend-agnostic point is kept: Rune receives the same IR/binder path.
Gemini's bind-time desugaring point is kept: no new execution primitive.

## Forward arc

`167 CONVERT (linear durable units, dimensional safety, SQL+Rune) -> affine
units (Temperature: offset plus scale) -> point/delta algebra -> unit-aware
aggregation/comparison polish -> converted predicate pushdown`.

The parked join residual and the Rune optimizer cockpit remain separate breadth
candidates.

## Deferred (named)

Affine units and offsets; point/delta algebra; temperature; Float32 and decimal
unit carriers; derived dimensions such as `mph`; dimensionless ratios; implicit
mixed-unit coercion; converted predicate pushdown; richer Rune unit syntax and
operator-facing gauges; schema-scoped unit names; computed join-key CONVERT
support while computed join keys remain restricted.
