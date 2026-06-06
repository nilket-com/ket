# ADR 171: Affine Unit CONVERT With Point/Delta Safety Fence

**Status:** Promoted. **Migration:** `plans/impl/171_affine_unit_convert_with_point_delta_safety_fence.md`.

ADR 171 is the first affine-unit cut after the ADR 167-170 unit/identity spine.
It extends durable unit metadata enough for correct temperature conversion, while
refusing to pretend the full affine-space algebra exists.

The load-bearing decision: **do not ship offset-only metadata.** A temperature
point and a temperature delta can have the same scale and offset shape but
different semantics. The metadata must say which one it is. ADR 171 therefore
adds both a point/delta kind and a signed offset, makes `CONVERT` dispatch by
that kind, and fences point arithmetic until a follow-on ADR defines delta result
identities.

## Grounding

- Wendelon's shared logical-type metadata is currently
  `LogicalTypeUnit { dimension, scale_to_base }` with positive
  `UnitScale { numerator: u64, denominator: u64 }`
  (`crates/wendelon/src/integration/compute.rs`). It has no offset and no
  point/delta tag.
- Toron's DDL parser accepts only
  `DIMENSION <name> SCALE_TO_BASE <positive decimal>` for units
  (`crates/toron/src/sql_planner.rs`) and keeps the ADR 167 `Float64`-only rule.
- `bind_sql_convert` validates source/target dimension and emits a single
  multiply by `source.scale_to_base / target.scale_to_base`, stamping the final
  node with the target logical type. `Expr::SqlConvert` is still an executor
  error if binding fails to desugar it.
- ADR 170 routes all four arithmetic operators through `LogicalArithmeticDomain`
  before dimensioned or physical fallback. That seam can reject point quantities
  without touching comparison, selection, or runtime kernels.
- Brainstorm 009's warning is decisive: offset without point/delta is the trap.
  A temperature point uses offset during conversion; a temperature delta uses
  scale only.

## Decisions

**D1 - Unit metadata grows a kind and a signed offset.** Extend shared unit
metadata with:

- `quantity_kind: UnitQuantityKind`, public values `Delta` and `Point`;
- `offset_to_base: UnitOffset`, an exact signed rational with signed numerator
  and positive denominator.

Existing unit DDL without the new clauses creates `Delta` with zero offset.
`UnitScale` remains positive-only. `UnitOffset` is separate because offsets can
be negative and because reusing a scale type would make validation misleading.

**D2 - Public names are brand-clean and unsuffixed.** The public Wendelon/Toron
metadata names should be neutral unit-language names such as
`UnitQuantityKind` and `UnitOffset`. Do not add new `V1`, `Descriptor`, `Policy`,
or Toron-branded names for this unit metadata.

**D3 - DDL is explicit and defaults to legacy linear semantics.** Existing unit
DDL keeps working:

```sql
CREATE LOGICAL TYPE Mile AS DOUBLE
  DIMENSION Length SCALE_TO_BASE 1609.344;
```

That creates a `Delta` unit with zero offset. The affine extension is:

```sql
CREATE LOGICAL TYPE Celsius AS DOUBLE
  DIMENSION Temperature SCALE_TO_BASE 1 OFFSET_TO_BASE 273.15 POINT;

CREATE LOGICAL TYPE Kelvin AS DOUBLE
  DIMENSION Temperature SCALE_TO_BASE 1 POINT;

CREATE LOGICAL TYPE Fahrenheit AS DOUBLE
  DIMENSION Temperature SCALE_TO_BASE 0.5555555555555556
  OFFSET_TO_BASE 255.3722222222222 POINT;

CREATE LOGICAL TYPE CelsiusDelta AS DOUBLE
  DIMENSION Temperature SCALE_TO_BASE 1 DELTA;

CREATE LOGICAL TYPE FahrenheitDelta AS DOUBLE
  DIMENSION Temperature SCALE_TO_BASE 0.5555555555555556 DELTA;
```

Rules:

- `POINT` and `DELTA` are legal only on dimensioned logical types.
- Missing kind defaults to `Delta`.
- Missing offset defaults to `0/1`.
- Nonzero offset with omitted kind rejects, because omitted kind means `Delta`.
- Nonzero offset with explicit `DELTA` rejects.
- `POINT` with omitted or zero offset is legal, which is how `Kelvin` is encoded.
- Unit metadata remains `Float64`-only.

**D4 - CONVERT dispatches by kind.** `CONVERT(source, Target)` still requires
source and target to be dimensioned `Float64` logical types in the same
dimension, and still preserves the target logical identity. It now also requires
matching `quantity_kind`.

- Delta to delta: `source * source.scale_to_base / target.scale_to_base`.
- Point to point:
  `(source * source.scale_to_base + source.offset_to_base - target.offset_to_base)
   / target.scale_to_base`.
- Point to delta and delta to point reject with a semantic mismatch diagnostic.

The binder may emit the point formula as `source * factor + shift`, where
`factor = source.scale / target.scale` and
`shift = (source.offset - target.offset) / target.scale`. Offsets are applied
only for `Point`.

**D5 - Point arithmetic is fenced.** Any `Point` quantity operand rejects binary
`+`, `-`, `*`, and `/` before the linear `bind_dimensioned_arithmetic` path can
run. `SUM(point)` and `AVG(point)` reject before ADR 168 can preserve the point
identity on an additive result.

This is intentionally stricter than the eventual algebra. `Point - Point =
Delta`, `Point + Delta = Point`, and `AVG(Point)` are meaningful in principle,
but they need durable result-delta identity rules. ADR 171 does not implement
those halfway.

**D6 - Delta arithmetic remains the ADR 167/168/170 behavior.** Legacy linear
units and explicit delta units continue to use the current dimensioned arithmetic
rules: same-unit delta `+`/`-` preserve the unit; delta scaled by a naked scalar
preserves the unit; unit-by-unit multiplication/division and ratios remain
deferred; same-dimension different-unit arithmetic requires explicit `CONVERT`;
cross-dimension arithmetic rejects.

**D7 - Comparison and selection remain point-safe.** Point values can be selected,
compared under the existing same-logical-type/same-unit comparison rules, and
passed through `MIN`/`MAX`. `COUNT` remains naked. ADR 171 adds no implicit unit
coercion for comparison, so same-dimension different-unit comparisons still need
explicit `CONVERT`.

**D8 - Still bind-desugared and frontend-shared.** SQL `CONVERT` and Rune
`convert()` both lower to `Expr::SqlConvert` and hit `bind_sql_convert`. There
is no executor kernel, PBC opcode, optimizer-fact surface, or storage runtime
operation for affine conversion.

**D9 - No premature backward-compatibility scaffolding.** Wendelon convention
`no-premature-backward-compat.md` applies: this is an in-flight schema contract,
not a deployed external stability boundary. Change the unit metadata shape
directly. Do not add legacy decode structs, tolerant decoders, or old-shape
decode tests for ADR 171.

## Headline Gates

- `convert_temperature_point_uses_affine_offset_and_preserves_target_identity` -
  `CONVERT(CAST(0.0 AS Celsius), Kelvin)` returns `273.15` and the output logical
  type is `Kelvin`.
- `convert_fahrenheit_point_to_celsius_point_uses_offset` -
  `CONVERT(CAST(32.0 AS Fahrenheit), Celsius)` returns approximately `0.0`.
- `convert_temperature_delta_uses_scale_only` -
  `CONVERT(CAST(9.0 AS FahrenheitDelta), CelsiusDelta)` returns approximately
  `5.0`, proving offsets are not applied to deltas.
- `point_delta_convert_mismatch_rejects` - point-to-delta and delta-to-point
  `CONVERT` reject at bind.
- `point_quantity_arithmetic_rejects_until_delta_result_identity_exists` -
  point `+`, `-`, `*`, `/`, `SUM`, and `AVG` reject with the point/delta safety
  diagnostic.

## Supporting Gates

- `point_comparison_and_selection_stay_valid` - point comparison, `MIN(point)`,
  `MAX(point)`, and `COUNT(point)` keep the existing behavior.
- `linear_unit_ddl_defaults_to_delta_zero_offset` - DDL with only
  `DIMENSION + SCALE_TO_BASE` creates `Delta` with `offset_to_base = 0/1`.
- `linear_unit_convert_arithmetic_and_aggregation_regressions_stay_green` -
  Mile/Kilometer `CONVERT`, same-unit linear arithmetic, `SUM(Mile)`, and ADR
  167/168/170 gates still pass.
- `nonzero_offset_requires_point` - both DDL and Wendelon validation reject
  nonzero offsets on default/explicit delta units.
- `rune_affine_convert_matches_sql_bind_path` - Rune point `convert()` matches
  SQL value and target logical type.
- `affine_metadata_survives_descriptor_resync` - created point/delta metadata
  survives catalog/descriptor reload, not just in-process DDL binding.

## Architecture Guards

- `adr171_unit_metadata_is_brand_clean_and_unsuffixed` - public affine unit
  metadata introduces no new `V1`, `Descriptor`, `Policy`, or Toron-branded names.
- `adr171_unit_metadata_has_kind_and_signed_offset` - `LogicalTypeUnit` carries a
  `UnitQuantityKind` and a signed `UnitOffset`, not a reused positive `UnitScale`.
- `adr171_no_premature_legacy_decode_scaffolding` - Wendelon does not add
  legacy unit structs, tolerant old-shape decoders, or old-shape decode tests
  for this in-flight metadata change.
- `adr171_delta_offset_rejects` - validation rejects nonzero offsets on `Delta`.
- `adr171_affine_convert_is_bind_desugared_not_executor_kernel` -
  `bind_sql_convert` consumes `quantity_kind` and `offset_to_base`; `exec.rs`
  still has no `evaluate_sql_convert`.
- `adr171_convert_dispatches_by_quantity_kind` - point/delta mismatch rejects
  before arithmetic nodes are emitted.
- `adr171_point_quantities_do_not_reach_linear_dimensioned_arithmetic` - point
  operands are rejected before linear dimensioned arithmetic can apply delta
  rules.
- `adr171_sum_avg_reject_point_quantities` - aggregate identity preservation
  rejects point `SUM`/`AVG`.
- `adr171_frontends_share_affine_convert_bind_path` - Rune still constructs
  `Expr::SqlConvert`; there is no Rune-only affine path.
- `adr171_no_executor_pbc_or_optimizer_surface` - no affine-specific branches in
  executor kernels, PBC compilation, predicate pushdown, or optimizer facts.
- `adr171_cast_path_still_contains_no_unit_transform` - primitive `CAST` still
  ascribes/erases only; all value-changing unit transforms stay in `CONVERT`.

## Non-Goals

- no `Point - Point = Delta`, `Point + Delta = Point`, or result-delta identity;
- no `AVG(Point)` semantics;
- no derived dimensions, ratios, or synthesized units;
- no implicit unit coercion for arithmetic or comparison;
- no unit display formatting;
- no converted predicate pushdown;
- no duration/interval unit generalization;
- no non-`Float64` unit carriers;
- no executor kernel, PBC opcode, optimizer fact, storage runtime, or descriptor
  policy surface for affine conversion.

## Consensus Trail

The council chose the affine step but scoped it to kind-aware `CONVERT` plus a
point/delta safety fence rather than the full algebra. Iteration exposed one
meaningful split: an offset-only model is smaller, but it cannot distinguish
`Kelvin` the point unit from `KelvinDelta` the delta unit and cannot reject
point/delta `CONVERT` mismatches. ADR 171 therefore promotes the explicit
point/delta kind as part of the metadata, while still rejecting point arithmetic
until the delta-result algebra lands.

## Forward Arc

`170 complete identity arithmetic -> 171 affine CONVERT + point/delta fence ->
172 point/delta algebra (delta type, point-delta arithmetic, delta-result
identity, AVG-of-points) -> unit display formatting -> converted predicate
pushdown`.

After 171, Toron can represent and convert temperature points and temperature
deltas safely. The fence guarantees no point operation silently computes a wrong
answer, and 172 can replace selected rejections with real point/delta result
typing.

## Deferred (named)

Point/delta result algebra; durable delta result identity selection;
`AVG(Point)` semantics; derived dimensions and ratios; unit display formatting;
converted predicate pushdown; Rune optimizer cockpit and receipt API;
duration-unit `CONVERT`; statistical aggregate unit policy beyond `SUM`/`AVG`;
richer Rune unit constructors.
