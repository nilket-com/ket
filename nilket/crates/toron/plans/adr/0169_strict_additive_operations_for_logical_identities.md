# ADR 169: Strict Additive Operations for Logical Identities

**Status:** Draft. **Migration:** `plans/impl/0169_strict_additive_operations_for_logical_identities.md`.

ADR 167 made declared unit quantities safe under arithmetic and `CONVERT`. ADR
168 let those unit quantities flow through `SUM` and `AVG`. ADR 169 closes the
remaining nominal-identity hole: a non-unit logical identity such as
`CustomerId` is not an additive quantity just because its physical carrier is
numeric.

The cut is Toron-local binder safety. Binary `+`, `SUM`, and `AVG` reject
non-unit logical identities unless the author explicitly erases the identity
with a primitive cast. Unit quantities and naked numeric values keep their
existing behavior.

## Grounding

The current bind path already exposes the seam:

- `bind_arithmetic` binds both operands, handles calendar/temporal arithmetic,
  then calls `bind_dimensioned_arithmetic` before ordinary physical numeric
  arithmetic fallback.
- `bind_dimensioned_arithmetic` uses `logical_unit_for_node`, which resolves the
  node's logical id through `LogicalTypeResolver::with_registry(...)` and returns
  a unit spec only when `LogicalTypeSpec.unit.is_some()`.
- A non-unit logical id resolves to a spec with `unit.is_none()`, so it currently
  behaves like "no unit" and can fall through to physical numeric addition.
- Refinement-only logical types such as
  `PersonId AS BIGINT CHECK (VALUE > 0)` also have `unit.is_none()`. ADR 169
  intentionally includes them in the Identity bucket: arithmetic over the
  carrier is meaningful only after an explicit primitive cast erases the logical
  receipt.
- ADR 168's aggregate rule intentionally left `SUM(CustomerId)` and
  `AVG(CustomerId)` naked rather than preserved. That was safe relative to
  identity propagation, but still permits meaningless carrier arithmetic.

That makes the type-system distinction precise:

- **Quantity:** a non-naked logical type whose durable spec has unit metadata.
  Quantities are additive under ADR 167/168 rules.
- **Identity:** a non-naked logical type whose durable spec has no unit metadata.
  Identities are comparable/selectable, but not additive.
- **Naked:** no logical identity. Naked numeric values use ordinary physical
  arithmetic.

No new durable field is required. The distinction is derived from the existing
unit metadata through the durable logical-type registry.

## Decisions

**D1 - Quantity vs Identity is derived from unit metadata.** A logical type is a
Quantity only when it resolves through
`LogicalTypeResolver::with_registry(logical_types)` to a spec with
`unit.is_some()`. A non-naked logical type with `unit.is_none()` is an Identity.
This includes refinement-only logical types: a `CHECK` predicate constrains the
carrier, but does not make the logical type an additive quantity. Do not use
seed-only logical type lookup.

**D2 - Binary `+` rejects Identity operands.** If either operand of
`BoundBinaryOp::Add` is an Identity, bind rejects before physical numeric
coercion. This covers:

- `customer_id + customer_id`;
- `customer_id + 1`;
- `customer_id + raw_numeric`;
- `customer_id + CAST(other_customer_id AS BIGINT)`.

The diagnostic should say the logical type is not an additive quantity and
should suggest an explicit primitive cast if carrier arithmetic is intentional.

**D3 - Explicit primitive cast erases the identity.**
`CAST(customer_id AS BIGINT) + 1` and
`CAST(customer_id AS BIGINT) + CAST(other_customer_id AS BIGINT)` remain ordinary
naked numeric addition. The escape hatch is the visible receipt that the author
chose carrier arithmetic, and it is frontend-shared: SQL spells it
`CAST(expr AS BIGINT)`, Rune spells it `expr.cast(type_i64())`, and both lower to
the same `Expr::Cast` erasure before `bind_arithmetic`. The additive gate itself
lives in the shared binder seam, so both the reject and the escape hatch behave
identically for SQL and Rune; this ADR adds no frontend-specific arithmetic path.

**D4 - Unit addition is unchanged.** ADR 167's unit rules stay in force:

- same-unit `+` is valid and preserves the unit identity;
- same-dimension different-unit `+` rejects and requires explicit `CONVERT`;
- cross-dimension `+` rejects with a dimension mismatch;
- unit plus naked numeric still rejects.

**D5 - `SUM` and `AVG` reject Identity operands.** Retarget ADR 168's
`SUM`/`AVG` fallback for non-unit logical identities. For
`AggregateFunction::Sum | AggregateFunction::Avg`:

- Quantity operand -> preserve unit identity under ADR 168;
- Identity operand -> reject as non-additive;
- naked operand -> naked output, unchanged.

This coordinates scalar addition and combination aggregation: non-quantity
logical identities are not silently added in either form.

**D6 - Selection, cardinality, and comparison are unchanged.** `MIN` and `MAX`
continue to preserve compatible logical identities under ADR 113/168.
`COUNT`/`COUNT(*)` remain valid and naked over any input. Comparisons over
logical identities keep existing behavior; an identity can be compared or
selected, it just cannot be added.

**D7 - Subtraction, multiplication, division, and statistical aggregates are out
of scope.** This ADR is strict additive operations: binary `+`, `SUM`, and
`AVG`. Linear-unit subtraction and broader non-unit arithmetic strictness are
plausible follow-ups, but subtraction is where the future affine point/delta
model will matter. `LINEAR_REGRESS` and `APPROX_QUANTILE` keep ADR 168 behavior.

**D8 - Binder-only, no runtime or storage surface.** This is a bind-time domain
check. No aggregate kernel, executor, PBC, Wendelon schema, descriptor, storage,
optimizer-fact, or wire change lands in this ADR.

## Headline Gates

- `strict_additive_operations_reject_identity` - `customer_id + 1`,
  `customer_id + customer_id`, `SUM(customer_id)`, and `AVG(customer_id)` reject
  with a non-additive logical type diagnostic.
- `explicit_cast_erases_identity_for_addition` -
  `CAST(customer_id AS BIGINT) + 1` binds and executes as naked numeric
  addition.
- `quantity_addition_and_aggregation_still_work` - `distance + distance`,
  `SUM(distance)`, and `AVG(distance)` still carry `Mile`.

## Supporting Gates

- `identity_comparison_and_selection_still_allowed` - `customer_id = 7`,
  `customer_id = other_customer_id`, `MIN(customer_id)`, and `MAX(customer_id)`
  keep existing behavior.
- `same_dimension_addition_still_requires_convert` -
  `distance_mile + distance_km` still rejects with the `CONVERT` diagnostic.
- `cross_dimension_addition_still_rejects` - `distance_mile + mass_kg` still
  rejects with dimension mismatch.
- `naked_numeric_addition_and_sum_stay_unchanged` - ordinary numeric `+`,
  `SUM(raw)`, and `AVG(raw)` still work and remain naked.
- `refinement_identity_addition_rejects` - a refinement-only logical type such
  as `PersonId AS BIGINT CHECK (VALUE > 0)` rejects `id + 0`, `id + id`,
  `SUM(id)`, and `AVG(id)`, while `CAST(id AS BIGINT) + 0` remains the explicit
  erasure path.
- `count_and_statistical_aggregates_stay_unchanged` - `COUNT(customer_id)` stays
  naked; experimental/statistical aggregate policy is not widened.
- `explicit_cast_sum_avg_erasure_stays_valid` -
  `SUM(CAST(customer_id AS BIGINT))` and `AVG(CAST(customer_id AS BIGINT))`
  produce naked numeric outputs.
- `rune_identity_additive_rejects_and_cast_erases` - through the Rune frontend,
  `customer_id.add(1)`, `sum(customer_id)`, and `avg(customer_id)` reject with
  the same non-additive diagnostic, while
  `customer_id.cast(type_i64()).add(1)`, `sum(customer_id.cast(type_i64()))`,
  and `avg(customer_id.cast(type_i64()))` bind as naked numeric operations,
  proving the gate and its escape hatch are frontend-shared, not SQL-only.

## Architecture Guards

- `adr169_addition_gates_logical_identity_before_numeric_fallback` -
  `bind_arithmetic` classifies `Add` operands through the durable registry
  before ordinary `arithmetic_output_type` fallback.
- `adr169_non_unit_logical_identity_is_not_treated_as_naked` - non-naked
  logical ids with `unit.is_none()` reject instead of returning `Ok(None)` from
  the logical arithmetic path.
- `adr169_sum_avg_reject_non_additive_identity` - retarget ADR 168's non-unit
  `SUM`/`AVG` guard from "stays naked" to "rejects".
- `adr169_selection_cardinality_and_comparison_stay_identity_aware` - `MIN`,
  `MAX`, `COUNT`, and comparison paths are not broadened by the additive gate.
- `adr169_identity_additive_gate_is_frontend_shared` - the additive domain check
  sits in the shared `bind_arithmetic`/aggregate seam reached by both SQL and
  Rune lowering; no Rune-specific arithmetic bypass exists. Mirrors
  `adr167_frontends_share_convert_bind_path`.
- `adr169_no_runtime_storage_or_wire_surface` - no aggregate-kernel, executor,
  PBC, Wendelon, storage, descriptor, or wire change.

## Non-Goals

- no affine units, temperature offsets, or point/delta algebra;
- no derived dimensions or ratios;
- no implicit mixed-unit coercion;
- no unit display formatting;
- no `DISTINCT` or window aggregate identity policy;
- no statistical aggregate unit propagation;
- no Wendelon or storage schema change;
- no new public SQL/Rune surface; identity erasure reuses each frontend's
  existing primitive cast (`CAST(... AS BIGINT)` / `expr.cast(type_i64())`).

## Consensus Trail

The council converged on the "addition" branch after ADR 168. Codex argued for
the smallest binary-`+` safety cut; Claude and Gemini argued for coordinating
addition with `SUM`/`AVG` so the aggregate and scalar forms do not disagree. The
promoted draft takes the coordinated additive scope: binary `+`, `SUM`, and
`AVG`, with subtraction and broader arithmetic strictness explicitly deferred.

## Forward Arc

`167 CONVERT -> 168 type/unit-aware aggregation -> 169 strict additive
operations -> affine units and point/delta algebra -> unit display formatting ->
converted predicate pushdown`.

After 169, the nominal spine has a clean split: identities are comparable and
selectable; quantities are additive; explicit primitive casts are the receipt
for raw carrier arithmetic.
