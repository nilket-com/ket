# ADR 168: Type/Unit-Aware Aggregation

**Status:** Promoted. **Migration:** `plans/impl/0168_type_unit_aware_aggregation.md`.

ADR 168 is the additive-unit follow-on ADR 113 explicitly deferred. It does not
re-open the nominal aggregate doctrine wholesale. It makes aggregate output
identity depend on both the aggregate class and the operand's logical metadata:

- selection aggregates are type-aware: ADR 113 already makes `MIN`/`MAX`
  preserve any compatible non-naked logical identity;
- combination aggregates are unit-aware: this ADR makes `SUM`/`AVG` preserve
  only declared linear unit identities;
- cardinality and statistical aggregates stay naked.

The cut is Toron-local binder schema metadata. It has no aggregate-kernel,
executor, PBC, Wendelon, storage, or descriptor change.

## Grounding

Current Toron already has the right single decision point:

- `aggregate_schema` builds group fields via `schema_for_exprs(group_expr)` and
  aggregate output fields via `aggregate_field(call)`.
- `aggregate_field` asks `aggregate_output_logical_type(call, field.data_type())`
  whether to stamp the aggregate output field with logical metadata.
- ADR 113 already makes `MIN`/`MAX` preserve an input logical identity when the
  aggregate output physical type exactly equals the input root physical type.
- ADR 113 keeps `SUM`/`AVG` naked because no declared additive property existed
  then.
- ADR 167 added durable linear unit metadata on `LogicalTypeSpec.unit` and
  bind-time dimensional safety for same-unit addition, unit/scalar arithmetic,
  comparisons, and `CONVERT`.

ADR 168 spends that new unit metadata in the same aggregate-output helper.
Durable units require `LogicalTypeResolver::with_registry(...)`; seed-only
`logical_type_spec(id)` cannot resolve user-created `Mile`, `Kilometer`, or
`Kilogram` logical types.

## Decisions

**D1 - ADR 113 selector behavior stands.** `MIN` and `MAX` continue to preserve
any non-naked operand logical identity when the output physical type exactly
matches the input physical type. This covers both unit and non-unit identities:
`MIN(customer_id)` -> `CustomerId`; `MAX(distance_mile)` -> `Mile`.

**D2 - `SUM` and `AVG` preserve declared linear units only.** For
`AggregateFunction::Sum | AggregateFunction::Avg`, preserve the operand root
logical id only when all of the following hold:

- the root logical id is non-naked;
- the root physical type equals the aggregate output physical type;
- the id resolves through `LogicalTypeResolver::with_registry(logical_types)`;
- the resolved `LogicalTypeSpec` has `unit.is_some()`;
- the resolved spec physical type equals the aggregate output physical type.

Otherwise the output is `LogicalTypeId::NAKED`. `SUM(CustomerId)` and
`AVG(CustomerId)` are naked, not `CustomerId`. This is the additive-unit
exception, not a generic computed-nominal propagation rule.

**D3 - The aggregate input expression chooses the output unit.** Aggregation has
one value expression, so 168 performs no aggregate-time mixed-unit coercion.
Users choose the output unit explicitly:

- `SUM(distance_mile)` -> `Mile`;
- `AVG(distance_mile)` -> `Mile`;
- `SUM(CONVERT(distance_mile, Kilometer))` -> `Kilometer`;
- `SUM(CAST(distance_mile AS DOUBLE))` -> naked;
- `SUM(distance_mile + other_distance_km)` rejects before aggregation unless
  one side is explicitly converted.

**D4 - Count and statistical aggregates stay naked.** `COUNT`, `COUNT(*)`,
`LinearRegress`, and `ApproxQuantile` emit naked output. `ApproxQuantile` may be
unit-valued in a future policy, but it is approximate/statistical and remains
deferred in this cut.

**D5 - The mechanism is binder schema metadata.** Extend
`aggregate_schema -> aggregate_field -> aggregate_output_logical_type` to carry
`&LogicalTypeRegistry`. Stable aggregate kernels remain physical-only; the
binder alone stamps logical metadata via `field_with_logical_type_metadata`.

**D6 - Pruning rebuilds must use the same registry-backed rule.**
`optimize_for_required` rebuilds aggregate schemas after column-pruning ordinal
rewrites. Thread `&LogicalTypeRegistry` through `push_down_scan_projection`,
`optimize_for_required`, and every recursive call, or the main bind path and the
pruned plan path can disagree about unit identity.

**D7 - 168 composes with 167; it does not duplicate safety.** Once an aggregate
output carries a unit, existing ADR 167 rules handle downstream behavior:

- `SUM(distance_mile) + SUM(mass_kg)` rejects as cross-dimension;
- `SUM(distance_mile) + SUM(other_distance_km)` rejects with an explicit
  `CONVERT` diagnostic;
- `CONVERT(SUM(distance_mile), Kilometer)` scales normally;
- `SUM(distance_mile) > 10.0` rejects unit-vs-naked.

No new arithmetic, comparison, `CONVERT`, executor, PBC, or storage rule lands
in this ADR.

**D8 - Downstream schema flow is inherited.** Once the aggregate output field
has logical metadata, projection, aliases, derived tables, and CTAS use the
existing ADR 088/089 decode/re-emit loop. No parallel unit sidecar or wire
surface is introduced.

## Headline Gates

- `aggregation_is_type_unit_aware` - bind and execute a query proving
  `MIN(customer_id)` carries `CustomerId`, `SUM(distance_mile)` carries `Mile`,
  `SUM(customer_id)` is naked, and `COUNT(distance_mile)` is naked.
- `aggregate_unit_convert_preserves_quantity` -
  `CONVERT(SUM(distance_mile), Kilometer)` returns the scaled total and carries
  `Kilometer`.
- `aggregated_unit_composes_with_167_safety` - `SUM(distance_mile) +
  SUM(mass_kg)` rejects as a dimension mismatch, and `SUM(distance_mile) +
  SUM(other_distance_km)` requires explicit `CONVERT`.

## Supporting Gates

- `min_max_preserve_non_unit_nominal_identity` - ADR 113 regression for the
  non-unit selector case.
- `min_max_preserve_unit_identity` - selector aggregate over a unit keeps the
  unit.
- `sum_avg_preserve_additive_unit_identity` - `SUM`/`AVG(distance_mile)` carry
  `Mile`.
- `sum_after_convert_preserves_target_unit` -
  `SUM(CONVERT(distance_mile, Kilometer))` carries `Kilometer`.
- `sum_avg_of_non_unit_nominal_identity_are_naked` - `SUM(CustomerId)` and
  `AVG(CustomerId)` are naked.
- `sum_avg_of_naked_input_stay_naked` - ordinary numeric aggregates are
  unchanged.
- `count_of_dimensioned_unit_is_naked` - `COUNT(distance_mile)` and `COUNT(*)`
  are naked.
- `experimental_unit_aggregates_are_naked` - `LINEAR_REGRESS` and
  `APPROX_QUANTILE` do not claim unit identity.
- `group_by_with_type_unit_aggregate_preserves_identity` - group-key identity
  and aggregate identities both survive.
- `unit_aggregate_identity_flows_through_ctas` - CTAS of `SUM(distance_mile)`
  persists and decodes the unit metadata through the existing schema path.
- `sum_mixed_unit_expression_requires_convert_before_aggregate` - expression
  binding rejects the mixed-unit input before the aggregate rule runs.

## Architecture Guards

- `adr168_sum_avg_preserve_only_declared_unit_identity` - the `SUM`/`AVG` path
  resolves through `LogicalTypeResolver::with_registry(logical_types)`, checks
  `unit.is_some()`, and retains the input/output physical equality guards.
- `adr168_non_unit_nominal_sum_avg_stay_naked` - the source rule cannot
  preserve `SUM`/`AVG` by logical id alone.
- `adr168_aggregate_schema_threads_logical_registry_through_pruning` -
  `aggregate_schema`, `aggregate_field`, `aggregate_output_logical_type`,
  `push_down_scan_projection`, and `optimize_for_required` carry the registry
  wherever aggregate schemas are rebuilt.
- `adr168_count_and_statistical_aggregates_stay_naked` - count and experimental
  aggregate arms remain naked.
- `adr168_aggregate_kernel_executor_pbc_stay_physical_only` - no logical
  metadata writer or logical-type registry lookup in `aggregate_kernel.rs`,
  `exec.rs`, or `pbc.rs`.
- `adr168_no_wendelon_wire_or_storage_surface` - no Wendelon schema, descriptor,
  storage, or PBC opcode change.
- Retarget ADR 113 and ADR 089 aggregate guards so they permit the unit-only
  `SUM`/`AVG` exception while preserving the non-unit nominal naked rule.

## Non-Goals

- No affine units, offsets, temperature, or point/delta algebra.
- No derived units such as `mile/hour` or `mile*money`.
- No implicit mixed-unit coercion inside aggregate calls.
- No Decimal or Float32 unit carrier.
- No `ApproxQuantile` unit propagation.
- No Wendelon, storage, PBC, optimizer-fact, or executor-kernel change.

## Consensus Trail

The council converged from "unit-aware aggregation" to the sharper
"type/unit-aware" split. ADR 113 already owns the type-aware selector half:
`MIN`/`MAX` preserve any compatible identity. ADR 168 owns the unit-aware
combination half: `SUM`/`AVG` preserve only identities that declare unit
metadata. The main risk identified in iteration was accidentally preserving
non-unit nominal identities through combination; the promoted rule keeps those
outputs naked.

## Forward Arc

`167 CONVERT -> 168 type/unit-aware aggregation -> additive-type strictness
(coordinated arithmetic plus aggregate rejection for non-quantity identities) /
affine units and point/delta algebra -> unit display formatting -> converted
predicate pushdown`.

After 168, linear units flow through the common analytical path:
source column -> `CONVERT` -> arithmetic -> `SUM`/`AVG` -> downstream
`CONVERT`/comparison, without weakening the ADR 087-113 nominal identity spine.

## Deferred

Strict additive types that reject non-quantity `+` and `SUM`/`AVG` together;
affine units and temperature; point/delta algebra; window and DISTINCT aggregate
identity propagation; derived units and ratios; more carrier types; unit display
formatting; richer Rune unit syntax and gauges.
