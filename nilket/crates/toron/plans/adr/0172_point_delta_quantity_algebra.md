# ADR 172: Point/Delta Quantity Algebra

**Status:** Promoted. **Migration:** `plans/impl/0172_point_delta_quantity_algebra.md`.

ADR 171 landed affine point units and a **safety fence**: any `Point` quantity
rejected `+`/`-`/`*`/`/`, `SUM`, and `AVG`, because the result identities needed a
point/delta model that did not yet exist. ADR 172 builds that model and replaces
the fence rejections with the real algebra.

### The algebra (intuition)

A temperature *reading* is a **point**; a temperature *change* is a **delta**.
The lawful operations:

| Operation | result |
|---|---|
| `Point - Point` (same unit) | **Delta** (`37°C - 36°C = 1Δ°C`) |
| `Point + Delta`, `Delta + Point` | **Point** (`37°C + 1Δ°C = 38°C`) |
| `Point - Delta` | **Point** |
| `Delta ± Delta`, `Delta * / scalar` | **Delta** (unchanged, 167/170) |
| `AVG(Point)` | **Point** (mean reading is a reading) |
| `Point + Point` | reject (meaningless) |
| `Point * / scalar`, `Point * / Point` | reject (scaling/products of points) |
| `SUM(Point)` | reject (sum of readings is meaningless) |

The only operation that needs a *new* result identity is `Point - Point`, which
must name a **Delta** type. Everything else resolves to an identity that already
exists: `Point ± Delta` → the point's own id; `AVG(Point)` → the point's own id.

### How `Point - Point` names its Delta: declared pairing

A point unit declares its delta counterpart by name in DDL
(`... POINT DELTA_UNIT CelsiusDelta`). `celsius - celsius` yields the declared
`CelsiusDelta`. This keeps the nominal spine clean (the result is a *named*
durable type), is unambiguous, and follows the arc's declared-over-inferred
doctrine. `DROP LOGICAL TYPE` does not exist, so there is no dangling-reference
risk and no referential-integrity machinery is needed.

**Why the pairing requires matching scale (load-bearing).** The point/delta
arithmetic is raw carrier arithmetic with a different identity stamp:
`celsius(37) - celsius(36) = 37 - 36 = 1`, stamped `CelsiusDelta`. That carrier
difference is the correct delta magnitude *only because* `CelsiusDelta` has the
same `scale_to_base` as `Celsius` (one point-step equals one delta-step). The
pairing validation (D2) enforces that, so no per-operation rescaling is needed.

## Grounding

- 171's fence lives in three seams that 172 converts into algebra:
  `bind_logical_arithmetic` (Point rejected for all ops, `binder.rs:9373`, point
  checks `9400`/`9405`), `bind_dimensioned_arithmetic` (defense-in-depth Point
  reject, `9486`/`9491`), and `additive_unit_aggregate_logical_type` (Point
  SUM/AVG reject, `binder.rs:10780`, point check `10800`).
- The fence already keys on `quantity_kind == Point` via `quantity_kind_for_spec`
  (`binder.rs:9673`), so 172 dispatches on the same classification.
- Unit metadata is the shared `LogicalTypeUnit { dimension, scale_to_base,
  quantity_kind, offset_to_base }` (`wendelon/src/integration/compute.rs:352`),
  serde-derived; a `delta_unit` reference rides the same bounded surface as 171's
  offset (struct + validate + DDL + direct serde shape change).
- Logical types are keyed `by_id` (`u32`) and `by_name`
  (`compute.rs:662/667`). `validate_for_physical` has no catalog, so it cannot
  check the pairing — pairing validation is a catalog-aware create-time step.
- `DROP LOGICAL TYPE` is unsupported (no `drop_logical*` anywhere), so a stored
  delta reference can never dangle.
- CONVERT already resolves a logical type by name (`ctx.logical_type_specs.get`),
  the pattern the delta_unit name resolution reuses.

## Decisions

**D1 - Point units gain an optional `delta_unit` name reference.** Extend the
shared `LogicalTypeUnit` with `delta_unit: Option<String>` (the name of the paired
delta type). Legal only when `quantity_kind == Point`; a `Delta` unit declaring
`delta_unit` rejects in local validation, and SQL DDL rejects `DELTA_UNIT`
outside a point unit clause. Absent = the point has no paired delta (see D7). It
rides the same cross-ring surface as 171's offset (struct + serde + DDL); no new
proto. This is a direct in-flight shape change: `None` is encoded explicitly in
the new `LogicalTypeUnit`, and ADR 172 must not add `#[serde(default)]`, legacy
unit structs, tolerant old-shape decoders, or old-shape decode tests. Stored
**by name** (not by id): it matches CONVERT's name resolution, performs
create-time name lookup for validation, stores no id, and `DROP`-absence makes
the name stable. (By-id foreign-key is the reversible alternative.)

**D2 - The pairing is validated at create (catalog-aware).** When a point declares
`DELTA_UNIT D`, a create-time check (where the catalog is authoritative) requires
`D` to: exist, have `quantity_kind == Delta`, the same `dimension`, and the same
`scale_to_base` as the point (so the carrier difference is the correct delta
magnitude — see the intro). Deltas already have zero offset (171).
`validate_for_physical` keeps the local (Float64 / scale / offset / point-only
`delta_unit`) checks; the existence/kind/dimension/scale check is the new
catalog-aware step. Forward references are not supported: the paired delta type
must already exist when the point type is created.

**D3 - `Point ± Delta = Point`.** `point + delta`, `delta + point`, and
`point - delta` bind when `delta` is the point's declared `delta_unit`, producing
the **point's own** logical identity. The emitted value is the raw carrier `±`
stamped with the point id. A delta that is not the point's paired delta rejects
(mismatched delta; CONVERT if cross-unit).

**D4 - `Point - Point = Delta`.** Same-unit `point - point` (both operands the same
point id) binds when the point declares a `delta_unit`, producing that delta type
(raw carrier subtraction stamped with the delta id). `Point + Point` rejects
(meaningless). Cross-unit point subtraction (`celsius - kelvin`) rejects and
requires explicit `CONVERT` first (167 cross-unit rule); same-unit only in this
cut.

**D5 - Point scaling and products stay rejected.** `Point * scalar`,
`scalar * Point`, `Point / scalar`, `Point * Point`, `Point / Point`,
`Point */ Delta` all reject — scaling or multiplying a point is not affine-meaningful
(derived dimensions remain deferred).

**D6 - `AVG(Point) = Point`; `SUM(Point)` stays rejected.** Un-fence `AVG` for
point operands in `additive_unit_aggregate_logical_type`, returning the point id
(the mean of readings is a reading). `SUM(Point)` keeps rejecting (the sum of
readings is meaningless). `MIN`/`MAX`/`COUNT` unchanged (171 D7).

**D7 - A point without a declared `delta_unit` degrades gracefully.** Such a point
still supports `CONVERT`, comparison, `MIN`/`MAX`, `COUNT`, and `AVG` (none need a
delta). `point - point` and `point ± delta` reject with "point type {name} has no
declared delta unit". (Kelvin declared `POINT` without `DELTA_UNIT` is the
example.)

**D8 - Delta and linear arithmetic unchanged; comparison/selection unchanged.**
Delta and legacy linear units keep ADR 167/170 behavior. Point comparison
(same-unit), `MIN`/`MAX`, `COUNT` keep ADR 171 D7 behavior. No new implicit unit
coercion.

**D9 - Binder + direct unit-metadata shape change only.** ADR 172 changes the
shared logical-type metadata shape directly, like ADR 171's offset change, and
adds the DDL clause that writes that metadata. Beyond that cross-ring metadata
surface, this is a bind-time change. No executor kernel, PBC opcode, optimizer
fact, storage runtime operation, or descriptor policy surface is added. `DROP`
being unsupported means no referential-integrity surface.

**D10 - Frontend-shared.** The algebra lives in the shared binder; SQL and Rune
arithmetic builders hit the same seams. `DELTA_UNIT` is the SQL DDL surface; Rune
unit creation follows the existing path.

## Headline Gates

- `point_minus_point_yields_declared_delta` - `celsius - celsius` binds, carries
  `CelsiusDelta`, and equals the raw difference (e.g. `37 - 36 = 1`).
- `point_plus_delta_yields_point` - `celsius + celsius_delta` and
  `celsius_delta + celsius` bind, carry `Celsius`, and equal the raw sum.
- `avg_of_points_is_point_sum_of_points_rejects` - `AVG(celsius)` carries `Celsius`
  and returns the mean; `SUM(celsius)` rejects.
- `point_plus_point_and_point_scaling_still_reject` - `celsius + celsius`,
  `celsius * 2`, `celsius / 2` reject.

## Supporting Gates

- `point_without_delta_unit_subtraction_rejects` - `kelvin - kelvin` rejects with
  "no declared delta unit"; `AVG(kelvin)`, `kelvin > kelvin`, `MIN(kelvin)` still
  work (D7).
- `cross_unit_point_subtraction_requires_convert` - `celsius - fahrenheit` rejects;
  `celsius - CONVERT(fahrenheit, Celsius)` binds to `CelsiusDelta`.
- `mismatched_delta_arithmetic_rejects` - `celsius + fahrenheit_delta` rejects
  (not the point's paired delta).
- `delta_unit_pairing_validated_at_create` - `DELTA_UNIT` rejects when the target
  is missing, not a `Delta`, a different dimension, or a different scale; and when
  declared on a `Delta`/non-unit type. The target delta must already exist.
- `delta_arithmetic_and_comparison_unchanged` - `celsius_delta + celsius_delta`,
  `celsius_delta * 2`, `mile + mile`, point comparison/`MIN`/`MAX` keep prior
  behavior.
- `point_delta_roundtrip` - `(celsius + (celsius - celsius)) = celsius` value and
  identity (delta produced then re-applied).

## Architecture Guards

- `adr172_point_delta_arithmetic_replaces_fence` - `bind_logical_arithmetic`
  dispatches point operands to the point/delta rules (not a blanket reject);
  retarget the 171 fence guard.
- `adr172_point_minus_point_resolves_declared_delta` - same-unit `Point - Point`
  stamps the declared `delta_unit`; `Point + Point` rejects.
- `adr172_point_plus_delta_preserves_point_identity` - `Point ± Delta` stamps the
  point id when the delta is the paired one.
- `adr172_avg_of_points_preserved_sum_still_rejects` -
  `additive_unit_aggregate_logical_type` returns the point id for `AVG(Point)` and
  rejects `SUM(Point)`.
- `adr172_point_scaling_and_point_plus_point_reject` - `*`/`/` and `Point + Point`
  reject for points.
- `adr172_delta_unit_pairing_validated_at_create` - the catalog-aware pairing check
  exists (delta kind/dimension/scale, point-only).
- `adr172_delta_unit_is_direct_shape_change_no_legacy_default` - `delta_unit` is a
  direct `LogicalTypeUnit` field with no `#[serde(default)]`, legacy unit struct,
  tolerant old-shape decoder, or old-shape test.
- `adr172_no_runtime_storage_or_descriptor_surface` - `delta_unit` is
  logical-type metadata; no executor/PBC/storage-runtime/descriptor-policy
  surface beyond the direct metadata shape.
- `adr172_point_delta_is_frontend_shared` - algebra in the shared binder; no
  Rune-specific path.

## Non-Goals

- no cross-unit point subtraction without `CONVERT` (same-unit only);
- no point scaling, point products, or derived dimensions;
- no `SUM(Point)`;
- no implicit point↔delta `CONVERT` (171 D4 mismatch stays);
- no inferred (un-declared) pairing;
- no `DROP`/referential-integrity machinery;
- no Float32/decimal carrier; no executor/PBC/optimizer-fact/storage-runtime
  change; no legacy wire compatibility scaffolding.

## Consensus Trail

The council took the affine arc to completion: 172 replaces 171's fence with real
point/delta algebra. The fork was how `Point - Point` names its delta; the decision
is **declared `DELTA_UNIT` pairing** (F1) — nominal-clean, unambiguous, on the
declared-identity doctrine. Grounding removed F1's main cost: `DROP` is
unsupported, so there is no dangling-reference problem. The pairing is stored by
name (CONVERT-style resolution and create-time validation, but no stored id;
by-id FK is the reversible alternative). The split fallback (F3: ship
`Point ± Delta` + `AVG` now, defer `Point - Point`) was available but unnecessary
once the pairing cost collapsed.

## Forward Arc

`171 affine CONVERT + fence -> 172 point/delta algebra -> cross-unit point delta
(point - point across units) / unit display formatting -> converted predicate
pushdown`.

After 172, affine units are fully functional: convert, compare, select, average,
apply deltas, and difference readings. The unit/identity arc (167-172) is complete
for linear and affine quantities.

## Deferred (named)

Cross-unit point subtraction without explicit CONVERT; point scaling / derived
dimensions / ratios; `SUM`-of-points policy; Float32/decimal carriers; unit display
formatting; converted predicate pushdown; richer Rune affine syntax.
