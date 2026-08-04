# Impl Plan - ADR 172: Point/Delta Quantity Algebra

Pairs with `plans/adr/0172_point_delta_quantity_algebra.md`.

Replaces 171's point safety-fence with real point/delta algebra. Adds an optional
`delta_unit` name reference to unit metadata (cross-ring, 171 offset precedent),
validates the pairing at create, and turns the three fenced seams into the algebra.
Beyond the direct logical-type metadata shape change, there is no
executor/aggregate-kernel/PBC/storage-runtime/descriptor-policy surface. `DROP`
is unsupported, so no referential-integrity surface.

## Anchors

| Concern | Anchor |
|---|---|
| Shared unit metadata | `LogicalTypeUnit` (`wendelon/src/integration/compute.rs:352`), `validate_for_physical` (`:357`), `LogicalTypeDefinition[Create]` (`:449`/`:427`) |
| Catalog keys (id/name) | `LogicalTypeIdCatalogKey` (`compute.rs:662`), `LogicalTypeNameCatalogKey` (`:667`); create/validate flow |
| Arithmetic seam (171 fence → algebra) | `bind_logical_arithmetic` (`binder.rs:9373`; point checks `9400`/`9405`) |
| Dimensioned defense-in-depth | `bind_dimensioned_arithmetic` (`binder.rs:9486`/`9491` point reject) |
| Aggregate seam | `additive_unit_aggregate_logical_type` (`binder.rs:10780`; point check `10800`) |
| Kind helper / point error | `quantity_kind_for_spec` (`binder.rs:9673`), `point_arithmetic_error` (`:9677`) |
| CONVERT name resolution pattern | `bind_sql_convert` / `ctx.logical_type_specs.get(name)` |
| DDL unit parse | `parse_create_logical_type_clauses`, `parse_signed_decimal_offset` (171) in `sql_planner.rs` |
| Behavior tests | `crates/toron/tests/logical_type_ddl_test.rs` (`setup_temperature_samples` @`4346`) |
| Architecture tests | `crates/toron/tests/architecture/nominal_type_identity.rs` (171 fence guards to retarget) |

## Phase 1 - `delta_unit` metadata (cross-ring)

In `wendelon/src/integration/compute.rs`, extend `LogicalTypeUnit`:

```rust
pub struct LogicalTypeUnit {
    pub dimension: String,
    pub scale_to_base: UnitScale,
    pub quantity_kind: UnitQuantityKind,
    pub offset_to_base: UnitOffset,
    pub delta_unit: Option<String>,   // paired delta type name; Point-only
}
```

- `validate_for_physical` adds the **local** check: `delta_unit.is_some()` requires
  `quantity_kind == Point` (reject `Delta` units that declare a `delta_unit`).
  It cannot check existence/kind/dimension/scale of the target (no catalog) —
  that is Phase 2.
- SQL DDL rejects `DELTA_UNIT` outside a point unit clause.
- Do **not** add `#[serde(default)]`, legacy unit structs, tolerant old-shape
  decoders, or old-shape decode tests. This follows
  `crates/wendelon/plans/conventions/no-premature-backward-compat.md`: the field
  is optional semantically, but `None` is encoded explicitly in the new in-flight
  shape.
- Mirror the 171 offset surface (struct + serde + DDL). Verify no manual
  (de)serializer bypasses the derive.

## Phase 2 - Catalog-aware pairing validation at create

In the logical-type create/definition flow that has catalog access (where named
references resolve, mirroring how names map to ids), when `unit.delta_unit = Some(d)`:

- resolve `d`; reject "delta unit {d} not found" if absent;
- require `d.unit.quantity_kind == Delta`;
- require `d.unit.dimension == point.dimension`;
- require `d.unit.scale_to_base == point.scale_to_base` (load-bearing: makes the
  carrier difference the correct delta magnitude);
- (offset on `d` already zero by 171 delta validation).

Place this beside the existing logical-type create validation (the layer that owns
id assignment / name resolution), not in `validate_for_physical`.
Forward references are not supported: the delta type must already exist before
the point type that names it is created. Cover the Wendelon catalog create path
and the Toron mock/storage create paths used by logical-type DDL tests.

## Phase 3 - Point/delta scalar algebra

Add a helper resolving a spec's paired delta spec by name via the registry:

```rust
fn paired_delta_spec(point: &LogicalTypeSpec, logical_types: &LogicalTypeRegistry)
    -> Option<LogicalTypeSpec>   // resolve point.unit.delta_unit name
```

In `bind_logical_arithmetic` (`binder.rs:9373`), replace the blanket point reject
(171, checks `@9400`/`9405`) with a dispatch when either operand is a `Point`
quantity:

- `Point(u) - Point(u)` (same id, `op == Sub`): if `u` has a paired delta `d`,
  emit `append_binary(Sub)` stamped with `d.id`; else reject "point type {u} has
  no declared delta unit".
- `Point(u) + Delta(d)` / `Delta(d) + Point(u)` (`op == Add`),
  `Point(u) - Delta(d)` (`op == Sub`): if `d == paired_delta(u)`, emit
  `append_binary(op)` stamped with `u.id` (the point); else reject (mismatched
  delta / cross-unit needs CONVERT).
- `Point + Point` (`op == Add`): reject (`point_plus_point` message).
- any `Point` with `op in {Mul, Div}`: reject (reuse `point_arithmetic_error`).
- `Point(u1) ∓ Point(u2)` different units: reject "requires explicit CONVERT".

Keep this above the `if !matches!(op, Add)` handoff so point cases never fall to
`bind_dimensioned_arithmetic`. Linear/delta-only operands fall through to existing
170/171 logic unchanged. The `bind_dimensioned_arithmetic` point defense-in-depth
reject (171, `9486`/`9491`) can stay as an unreachable backstop.

Untyped-NULL: `reject_logical_arithmetic_with_untyped_null` keeps rejecting a Point
opposite untyped NULL (a NULL has no kind to pair) — unchanged from 171.

## Phase 4 - Aggregate algebra

In `additive_unit_aggregate_logical_type` (`binder.rs:10780`, point check `10800`),
split the 171 point reject:

- `AVG` + `Point(spec)` -> `Ok(spec.id)` (preserve the point identity);
- `SUM` + `Point(spec)` -> `Err(...)` (keep rejecting);
- `Delta` operands -> existing unit-preserve behavior.

(`aggregate_name` already distinguishes SUM/AVG in this helper.)

## Phase 5 - DDL

Extend `parse_create_logical_type_clauses` to accept `DELTA_UNIT <name>` after the
`POINT|DELTA` kind token (171 ordering):

```sql
DIMENSION <id> SCALE_TO_BASE <pos-dec>
  [OFFSET_TO_BASE <signed-dec>] [POINT|DELTA] [DELTA_UNIT <name>] [CHECK (...)]
```

- `DELTA_UNIT` legal only with `POINT` (reject with `DELTA`/omitted-kind);
- store the name in `unit.delta_unit`;
- name validated by Phase 2 at create; create delta types before point types in
  fixtures.

## Phase 6 - Behavioral tests

In `logical_type_ddl_test.rs`, extend `setup_temperature_samples` (`@4346`) so
`Celsius` declares `DELTA_UNIT CelsiusDelta` and `Fahrenheit` declares `DELTA_UNIT
FahrenheitDelta`; keep `Kelvin` as `POINT` without a delta (D7 case):

- `point_minus_point_yields_declared_delta` - `celsius - celsius` → `CelsiusDelta`,
  value = raw difference.
- `point_plus_delta_yields_point` - `celsius + celsius_delta`,
  `celsius_delta + celsius`, `celsius - celsius_delta` → `Celsius`, raw value.
- `avg_of_points_is_point_sum_of_points_rejects` - `AVG(celsius)` → `Celsius`;
  `SUM(celsius)` rejects.
- `point_plus_point_and_point_scaling_still_reject`.
- `point_without_delta_unit_subtraction_rejects` - `kelvin - kelvin` rejects;
  `AVG(kelvin)`, `kelvin > kelvin`, `MIN(kelvin)` still work.
- `cross_unit_point_subtraction_requires_convert` - `celsius - fahrenheit` rejects;
  `celsius - CONVERT(fahrenheit, Celsius)` → `CelsiusDelta`.
- `mismatched_delta_arithmetic_rejects` - `celsius + fahrenheit_delta` rejects.
- `delta_unit_pairing_validated_at_create` - DDL rejects missing/non-delta/
  wrong-dimension/wrong-scale `DELTA_UNIT`, and `DELTA_UNIT` on a `DELTA` type;
  forward references reject.
- `delta_arithmetic_and_comparison_unchanged` - delta `+`/`*`, linear `mile + mile`,
  point comparison/`MIN`/`MAX` regress green.
- `point_delta_roundtrip` - `celsius + (celsius - celsius) = celsius`.

## Phase 7 - Architecture guards

In `nominal_type_identity.rs`: add the `adr172_*` guards from the ADR and
**retarget** the 171 fence guards
(`adr171_point_quantities_do_not_reach_linear_dimensioned_arithmetic`,
`adr171_sum_avg_reject_point_quantities`): point `+`/`-` are no longer blanket
rejects; assert the new dispatch (Sub→delta, Add point+delta→point, point+point
reject, Mul/Div reject, SUM(Point) reject, AVG(Point) preserve). Keep the
`no-premature-backward-compat` guard satisfied (`delta_unit` is a direct shape
change, no `#[serde(default)]`, no legacy decoder).

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test point_minus_point_yields_declared_delta
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test avg_of_points_is_point_sum_of_points_rejects
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test point_without_delta_unit_subtraction_rejects
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test
scripts/cargo-cgroup test -p wendelon --test compute_integration_contract
scripts/cargo-cgroup test -p toron --test architecture_test adr172
scripts/cargo-cgroup test -p toron --test architecture_test adr171
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

## Risks

- **Pairing validation placement.** Must be catalog-aware (Phase 2), not in
  `validate_for_physical`; otherwise an unresolvable/mismatched delta slips through
  to a bind-time panic.
- **Scale-match strictness.** Require `delta.scale == point.scale` so the carrier
  subtraction is unit-correct; a mismatched-scale pairing would silently produce a
  wrong-magnitude delta.
- **Same-unit-only subtraction.** `Point - Point` must require the same point id;
  cross-unit must reject with the CONVERT hint, not silently subtract carriers.
- **AVG/SUM split.** Only `AVG(Point)` un-fences; `SUM(Point)` stays rejected —
  don't relax both.
- **171 guard retarget.** The blanket-point-fence guards invert; reconcile in this
  cut.
- **Cross-ring field.** `delta_unit` mirrors the 171 offset surface (struct + serde
  + DDL); verify no manual (de)serializer bypass.
- **Premature compatibility drift.** Do not add `#[serde(default)]`, legacy unit
  structs, tolerant old-shape decoders, or old-shape tests for this in-flight
  metadata change.

## Deferred

Cross-unit point subtraction without CONVERT; point scaling / derived dimensions;
`SUM`-of-points; Float32/decimal carriers; unit display formatting; converted
predicate pushdown; richer Rune affine syntax.
