# Impl Plan - ADR 167: CONVERT (dimensioned unit coordinate transforms)

Pairs with `plans/adr/167_convert_dimensioned_units.md`.

## Scope

Implement durable, linear `Float64` unit metadata on logical types and add
explicit `CONVERT(expr, TargetUnit)` as a shared SQL/Rune expression surface.
CONVERT binds to existing arithmetic and stamps the target logical identity. No
storage scan behavior, PBC opcode, executor kernel, or optimizer fact changes
land in this ADR.

## Anchors

| Concern | Anchor |
|---|---|
| Toron logical spec | `crates/toron/src/ir.rs`, `LogicalTypeSpec { id, name, physical, refinement }` |
| Wendelon logical definition | `crates/wendelon/src/integration/compute.rs`, `LogicalTypeDefinition*` |
| Wendelon client request | `crates/wendelon/crates/wendelon-client/src/lib.rs`, logical type create request |
| SQL logical type DDL | `crates/toron/src/sql_planner.rs`, `try_lower_create_logical_type` |
| SQL CAST lower/bind | `Expr::SqlLogicalCast`, `resolve_logical_casts`, `bind_sql_logical_cast` |
| Rune expression builders | `crates/toron/src/rune_engine.rs`, `RuneExpr` helpers and module registration |
| Expression bind context | `ExprBindContext`, currently carrying only `logical_casts` by name |
| Arithmetic/comparison gates | `bind_arithmetic`, `bind_comparison`, `nominal_compatible` |
| Metadata round trip | logical type metadata writer/decoder and catalog descriptor sync |
| Architecture tests | `crates/toron/tests/architecture/nominal_type_identity.rs` plus an ADR167 guard module |

## Durable unit metadata

Add brand-clean, unsuffixed unit metadata types near the existing logical type
definition surfaces:

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

Extend:

- `LogicalTypeDefinitionCreate`
- `LogicalTypeDefinition`
- Wendelon client create/definition request structs
- Toron create-logical-type request and descriptor sync mapping
- `LogicalTypeSpec`
- `LogicalTypeRegistry` registration/lookup
- catalog metadata serialization/deserialization used for table descriptors
- optional `information_schema.logical_types` rows if first-cut visibility is
  selected

Validation:

- unit metadata is optional;
- `dimension` is a non-empty one-part ASCII identifier;
- `scale_to_base.numerator != 0`;
- `scale_to_base.denominator != 0`;
- unit metadata requires `LogicalTypePhysical::Float64`;
- no offset field, affine kind, derived-dimension vector, or placeholder enum
  variant.

Use an exact rational in durable metadata even though execution multiplies by
`f64`. Add a parser helper that accepts ordinary positive decimals and reduces
the fraction:

```rust
parse_positive_decimal_scale("1609.344")
    -> UnitScale { numerator: 201168, denominator: 125 }
```

Reject scientific notation for the first cut unless an existing decimal parser
already makes it simple and exact.

## CREATE LOGICAL TYPE unit clause

Extend the current hand-parsed DDL:

```sql
CREATE LOGICAL TYPE Mile AS DOUBLE DIMENSION Length SCALE_TO_BASE 1609.344;
CREATE LOGICAL TYPE Kilometer AS DOUBLE DIMENSION Length SCALE_TO_BASE 1000;
CREATE LOGICAL TYPE CustomerId AS BIGINT;
```

Rules:

- keep the existing `CREATE LOGICAL TYPE <Name> AS <primitive>` prefix;
- parse optional `DIMENSION <identifier> SCALE_TO_BASE <decimal>` before any
  existing refinement/check clause;
- reject `DIMENSION` without `SCALE_TO_BASE`;
- reject `SCALE_TO_BASE` without `DIMENSION`;
- reject unit metadata on non-`DOUBLE` physical types;
- preserve durable-only logical name resolution from ADR091/092.

No built-in unit names are added to SQL seed resolution. Test fixtures should
create durable logical type definitions.

## CONVERT lowering

Add transient IR:

```rust
Expr::SqlConvert {
    expr: Box<Expr>,
    target_logical_name: String,
}
```

SQL lowering:

```sql
CONVERT(expr, Kilometer)
```

Accept exactly two positional arguments. The target must be a one-part unquoted
identifier. A string-literal target spelling is optional; if included, it must
run through the same identifier validator. Reject named args, wildcards,
qualified wildcards, and subquery targets.

Rune lowering:

```rust
convert(expr, "Kilometer")
```

or an equivalent minimal helper on `RuneExpr`. Register it in the Toron Rune
module and lower to the same `Expr::SqlConvert` variant. Do not introduce richer
Rune method sugar in this ADR.

Update every `Expr` traversal:

- PBC eligibility returns false for `SqlConvert`;
- structural hashing includes the target name and inner expression;
- hidden sort key validation and reference walkers descend into the inner expr;
- expression rewrite/clone helpers preserve the target name;
- logical-type-name collection includes names from both `SqlLogicalCast` and
  `SqlConvert`.

## Expression bind context

Refactor `ExprBindContext` so expression binding can resolve both target names
and source logical ids:

```rust
struct ExprBindContext<'a> {
    config: &'a PlannerConfig,
    session_timezone: Tz,
    logical_type_specs: &'a HashMap<String, LogicalTypeSpec>,
    logical_type_registry: &'a LogicalTypeRegistry,
    relation_scope: Option<&'a RelationScope>,
}
```

The exact field names can differ, but the capabilities are required:

- collect logical type names from CAST and CONVERT before expression bind;
- resolve those names through `resolve_durable_logical_type_by_name`;
- pass a registry or overlay that can resolve a bound source node's
  `LogicalTypeId` to a full `LogicalTypeSpec`;
- keep SQL planner resolution-free;
- keep seed-first lookup out of SQL logical type names.

Retarget ADR092 architecture guards from the old `logical_casts` field shape to
these invariants.

## Bind CONVERT

Add `bind_sql_convert` beside the existing logical-cast bind path.

Algorithm:

1. Resolve the target spec from the pre-resolved logical type map.
2. Require target unit metadata.
3. Require target physical `Float64`.
4. Bind the source expression.
5. Read the source node's logical type id.
6. Resolve that id to a source spec through the bind-context registry/overlay.
7. Require source unit metadata.
8. Require source physical `Float64`.
9. Require matching dimensions.
10. Compute `factor = source.scale_to_base / target.scale_to_base` as `f64`.
11. Reject non-finite, zero, or otherwise invalid factors.
12. Fold non-null numeric literals when straightforward.
13. Otherwise append existing arithmetic with a `Float64` factor.
14. Stamp the final node with the target logical type id.

Use multiplication by the precomputed ratio instead of emitting a separate
multiply and divide unless the existing builder strongly prefers two operations.

No-op conversion needs care. If the factor is `1.0` but the target logical id
differs, do not mutate a shared source node in place. Append a fresh identity
node, no-op cast, or explicit helper that creates a new root with the target id.
Pin this with `convert_between_same_units_carries_target_identity_without_aliasing_source`.

Errors:

- target not found: existing logical type not found error;
- target not unit: `CONVERT target must be a dimensioned logical type`;
- naked source: `CONVERT source must be a dimensioned logical type`;
- source/target non-Float64: `CONVERT supports Float64 dimensioned logical types only`;
- dimension mismatch: include both dimension names;
- invalid scale: include the target/source logical type name.

## Unit arithmetic and comparison

Add helpers near arithmetic/comparison binding:

```rust
fn logical_unit_for_node(...) -> Result<Option<ResolvedLogicalUnit>, BindingError>;
fn bind_dimensioned_arithmetic(...) -> Result<Option<BoundExprId>, BindingError>;
fn bind_dimensioned_comparison(...) -> Result<Option<BoundExprId>, BindingError>;
```

Run these helpers after temporal/calendar special cases and before ordinary
numeric fallback.

Arithmetic rules:

- Add/Sub:
  - same unit id: existing numeric op, output stamped with that unit;
  - same dimension, different unit id: reject with explicit CONVERT hint;
  - different dimensions: reject dimension mismatch;
  - unit with naked/non-unit numeric: reject.
- Mul:
  - unit times naked numeric or naked numeric times unit: existing numeric op,
    output stamped with the unit;
  - unit times unit: reject derived dimensions deferred.
- Div:
  - unit divided by naked numeric: existing numeric op, output stamped with the
    unit;
  - naked numeric divided by unit or unit divided by unit: reject deferred ratio
    semantics.

Comparison rules:

- same unit id: existing comparison;
- same dimension, different unit id: reject with explicit CONVERT hint;
- different dimensions: reject dimension mismatch;
- unit with naked/non-unit: reject.

Leave non-unit nominal behavior untouched. Existing `NominalTypeMismatch` cases
must not become unit errors unless both logical ids resolve to unit metadata.

## Tests

Behavior gates:

- `create_logical_type_unit_metadata_survives_catalog_resync`
- `convert_cast_mile_to_kilometer_preserves_quantity_and_carries_target_identity`
- `rune_convert_matches_sql_convert_bind_path`
- `convert_column_unit_to_target_unit_carries_target_metadata`
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
- `convert_expression_is_not_pbc_pushdown`
- `unit_metadata_requires_float64_and_positive_scale`
- `unit_sql_names_are_durable_only_not_seeded`
- `non_unit_nominal_mismatch_still_uses_nominal_error`

Wendelon/logical-type gates:

- logical type create/definition round trips preserve optional unit metadata;
- non-unit legacy definitions decode with no unit metadata;
- invalid unit physical type rejects;
- duplicate logical type names remain rejected exactly as before.

Architecture guards:

- `adr167_cast_path_contains_no_unit_transform`
- `adr167_convert_is_transient_ir_and_bind_desugared`
- `adr167_no_executor_kernel_or_pbc_opcode`
- `adr167_units_are_durable_logical_type_metadata_not_arrow_sidecar`
- `adr167_unit_sql_resolution_is_durable_only_not_seed_first`
- `adr167_dimensioned_arithmetic_rejects_implicit_unit_coercion`
- `adr167_linear_float64_only_no_affine_offset`
- `adr167_frontends_share_convert_bind_path`
- `adr167_wendelon_unit_names_are_brand_clean_and_unsuffixed`

Retargeted guards:

- ADR091/092 seed-exclusion and durable-name-resolution guards remain green.
- ADR092 guards that reference `logical_casts` are updated to the generalized
  logical type spec context rather than removed.

## Verification

```sh
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p wendelon logical_type
scripts/cargo-cgroup test -p toron convert
scripts/cargo-cgroup test -p toron unit
scripts/cargo-cgroup test -p toron logical_type
scripts/cargo-cgroup test -p toron rune_convert
scripts/cargo-cgroup test -p toron --test architecture_test adr167
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

If Wendelon has a separate architecture binary for schema conventions, run that
full binary as well.

## Risks

- **Seed trap:** static unit names would invert ADR091/092. Keep SQL unit names
  durable-only and test that explicitly.
- **Source spec visibility:** a `LogicalTypeId` alone is insufficient for
  CONVERT. Bind context must carry a registry or overlay.
- **No-op aliasing:** factor-one conversions still need a fresh output identity
  when source and target logical ids differ.
- **Arithmetic hole:** CONVERT without unit arithmetic gates leaves the product
  half-built. Pin `Mile + Kilogram` and `Mile + Float64` rejection.
- **Precision:** durable rational scales are exact, execution is `f64`; value
  tests need tolerance assertions.
- **PBC overclaim:** converted predicates do not push down in this ADR.
- **Rune scope creep:** include only the minimal shared helper; richer user
  experience belongs in a follow-on.
- **Wire/name hygiene:** no numeric version suffixes, placeholder enum variants,
  or Toron-branded Wendelon schema names.

## Deferred

Affine offsets; point/delta algebra; temperature; Float32 and decimal carriers;
derived dimensions and dimensionless ratios; implicit mixed-unit coercion;
converted predicate pushdown; richer Rune unit syntax and optimizer/statistics
gauges; schema-scoped unit names; computed join-key CONVERT support while
computed join keys remain restricted.
