# Impl 088: Catalog-Carried Logical Column Identity

**Pairs with:** `plans/adr/088_catalog_carried_logical_column_identity.md` (Promoted,
genuine 3-of-3). **Posture:** catalog→binder continuation. The id stays in Arrow field
metadata (no `TableBinding` vector, no serialized `ResolvedColumn` field); the binder
reads it at column resolution and stamps ADR 087's `node_logical_types` sidecar — the
**sole** durable carrier. New logic = a decoder + a stamp + one shared `reconcile_identity`.

## Invariants

- `node_logical_types` (087) remains the only serialized logical carrier; `ResolvedColumn`
  stays physical-only (it serializes inside `BoundExprNode::Column`).
- Identity source = Arrow field metadata (durable) + explicit ascription (authoring),
  reconciled by one helper (add/restate/not-contradict).
- The nominal gate is unchanged — reuse 087's `nominal_compatible`. 088 only changes
  *where source identity comes from*.
- Behavior-additive: a field with no key (or `"0"`) decodes to NAKED; all-naked queries
  are behavior-identical to today.
- Metadata rides the `SchemaRef` through scan-pruning + joins (grounded:
  `scan_output_schema`, `project_schema`, `join_schema` all whole-`Field` clone). It is
  dropped only at `schema_for_exprs` (the `Field::new` rebuild) = ADR 089 scope.

## Phase 1 — decoder (`ir.rs`, beside 087's registry)

```rust
pub const LOGICAL_TYPE_ID_METADATA_KEY: &str = "nilket.toron.logical_type_id";

// Core decode/validate; error as message so each caller maps to its own error type.
pub fn decode_column_logical_type(field: &Field) -> Result<LogicalTypeId, String> {
    let Some(raw) = field.metadata().get(LOGICAL_TYPE_ID_METADATA_KEY) else {
        return Ok(LogicalTypeId::NAKED);                       // missing → naked
    };
    let id = raw.parse::<u32>().map(LogicalTypeId)
        .map_err(|_| format!("invalid logical_type_id {raw:?}"))?;
    if id.is_naked() { return Ok(id); }                        // "0" → naked
    let spec = logical_type_spec(id)
        .ok_or_else(|| format!("unknown logical type {}", logical_type_name(id)))?;
    if &spec.physical != field.data_type() {                   // D3 physical match
        return Err(format!(
            "logical type {} expects {:?}, got {:?}",
            spec.name, spec.physical, field.data_type()));
    }
    Ok(id)
}
```

Reuses 087's `logical_type_spec`/`logical_type_name`/registry. No name heuristic (D12).

## Phase 2 — fail-closed catalog validation (`catalog.rs`)

In `TableBinding::from_descriptor` (`:85`), after `descriptor.arrow_schema()`:

```rust
for field in schema.fields() {
    decode_column_logical_type(field)
        .map_err(|message| CatalogError::FingerprintResolution { message })?;  // D4 fail closed
}
```

No `logical_column_types` vector stored (D5). Validation only — the durable source is the
schema metadata; the read happens later at resolve time.

## Phase 3 — resolve + stamp + reconcile (`binder.rs`)

```rust
struct ResolvedLogicalColumn { column: ResolvedColumn, logical_type: LogicalTypeId }   // binder-local (D6)

fn resolve_logical_column(name: &str, schema: &SchemaRef)
    -> Result<ResolvedLogicalColumn, BindingError> {
    let column = resolve_column(name, schema)?;                       // unchanged, physical-only
    let logical_type = decode_column_logical_type(schema.field(column.ordinal.0 as usize))
        .map_err(|message| BindingError::UnsupportedExpression { message })?;  // validated upstream; defensive
    Ok(ResolvedLogicalColumn { column, logical_type })
}

fn reconcile_identity(existing: LogicalTypeId, declared: LogicalTypeId, physical: &DataType)
    -> Result<LogicalTypeId, BindingError> {                          // D9/D10 single helper
    match (existing, declared) {
        (e, d) if e == d        => Ok(e),                  // restate
        (e, d) if d.is_naked()  => Ok(e),
        (e, d) if e.is_naked()  => Ok(d),                  // add
        (e, d) => Err(BindingError::NominalTypeMismatch {  // contradict
            left: logical_type_name(e), right: logical_type_name(d),
            physical: format!("{physical:?}"),
        }),
    }
}
```

- **`Expr::Column` (`:2802`) stamps (D7):** replace
  `builder.append_column(resolve_column(...)?)` with resolve-then-stamp:
  ```rust
  let resolved = resolve_logical_column(&column_ref.name.0, input_schema)?;
  let node = builder.append_column(resolved.column);
  builder.set_node_logical_type(node, resolved.logical_type);
  Ok(node)
  ```
- **`bind_logical_ascription` → reconcile-then-set (D9 expr path):** the inner column now
  arrives stamped with its catalog id, so:
  ```rust
  let inner = bind_expr_into_program(builder, inner, input_schema, config)?;
  let physical = builder.node_type(inner).cloned().expect("typed");
  validate_logical_ascription_physical(&physical, logical_type)?;   // existing 087 check
  let merged = reconcile_identity(
      builder.node_logical_type(inner).expect("stamped"), logical_type, &physical)?;
  builder.set_node_logical_type(inner, merged);
  Ok(inner)
  ```

## Phase 4 — join keys (`binder.rs`)

`join_key_column_ref` (087) unchanged (returns explicit id / NAKED + `ColumnRef`).
`resolved_join_key` decodes the resolved column's catalog id and reconciles (D8 + D9):

```rust
fn resolved_join_key(column: ResolvedColumn, explicit: LogicalTypeId, schema: &SchemaRef)
    -> Result<ResolvedJoinKey, BindingError> {
    let catalog = decode_column_logical_type(schema.field(column.ordinal.0 as usize))
        .map_err(|message| BindingError::UnsupportedExpression { message })?;
    let logical_type = reconcile_identity(catalog, explicit, &column.data_type)?; // bare → (catalog, NAKED)
    validate_logical_ascription_physical(&column.data_type, logical_type)?;
    Ok(ResolvedJoinKey { column, logical_type })
}
```

The resolved column comes from `Scope` resolution over `plan.schema()` (metadata-bearing —
grounded). The 087 join gate (`validate_join_key_types` → `nominal_compatible`) is
unchanged; it now sees catalog identities.

## Phase 0 — fixtures first

No DDL (D12) → tests build descriptors with Arrow field metadata directly. Add
`field_with_logical(name, dt, id)` = `Field::new(name, dt, true).with_metadata(
[(LOGICAL_TYPE_ID_METADATA_KEY.into(), id.to_string())].into())` + descriptor builders
marking `customer_id`/`product_id`/`id`. The 10 gates:

1. `catalog_metadata_column_vs_literal_binds`
2. `catalog_metadata_same_id_comparison_binds`
3. `catalog_metadata_different_id_comparison_rejects`
4. `catalog_metadata_one_side_naked_nonliteral_rejects`
5. `catalog_invalid_logical_metadata_rejects_at_descriptor_bind` (unknown id / non-decimal
   / physical mismatch — three sub-cases)
6. `catalog_join_same_id_binds`
7. `catalog_join_different_id_rejects`
8. `catalog_identity_survives_join_into_filter` (the metadata-resident payoff)
9. `explicit_ascription_restate_binds_contradict_rejects_cast_erase_rebinds` (D9 triad)
10. existing all-naked SQL/Rune suites stay green

## Phase 5 — guards (`tests/architecture/nominal_type_identity.rs`, extend)

The 11 `adr088_*` guards from the ADR, notably:
`adr088_no_table_binding_parallel_logical_vector` (no `logical_column_types` in
`catalog.rs`); `adr088_resolved_column_stays_physical_only` (`ResolvedColumn` has no
`logical_type`); `adr088_identity_reconciliation_is_single_shared_helper` (exactly one
`fn reconcile_identity`, called by both `bind_logical_ascription` and the join path);
`adr088_field_metadata_decoder_validates_parse_registry_and_physical_type`;
`adr088_descriptor_binding_validates_declared_logical_metadata`;
`adr088_column_binding_stamps_bound_expr_logical_sidecar`;
`adr088_join_key_takes_identity_from_catalog_metadata`;
`adr088_explicit_ascription_cannot_contradict_existing_identity`;
`adr088_no_new_projection_or_ctas_logical_metadata_writer`. Carry 087's guards.

## Verification

```
cargo fmt
cargo test -p toron --test <catalog/binder nominal target> catalog_
cargo test -p toron --test architecture_test adr088_
cargo test -p toron --test rune_relation_handles_test rune_nominal_   # 087 still green
cargo test -p toron --test join_kind_test                            # 087 join gate intact
cargo test -p toron --test group_by_test --test predicate_widening_test  # all-naked green
cargo test -p toron --lib                                            # catalog + distributed
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron                                                  # full package
```

## Evidence doc (`plans/impl/088_..._evidence.md`)

Record: files touched (`ir.rs`/`catalog.rs`/`binder.rs` + tests); the decoder + the
fail-closed validation loop; that no `TableBinding` vector and no `ResolvedColumn` logical
field were added; the single `reconcile_identity` + its two call sites; the 10 fixture
results (incl. join-survives-into-filter + the D9 triad); confirmation no
Wendelon/executor/PBC/Rune change; that the fingerprint now reflects declared metadata
(behavior-additive for no-key descriptors); full-package + clippy green.

## Watch items

1. **Decoder error-type split** — one decode core (`Result<_, String>`); `from_descriptor`
   maps to `CatalogError` (fail-closed), resolve paths to `BindingError` (defensive;
   upstream already validated, so it should not fire).
2. **Blast radius of `resolve_column`** — many callers; do NOT change its signature. Add
   `resolve_logical_column` as a thin wrapper used only where identity matters (the
   `Expr::Column` bind site + join resolution). Other callers stay physical-only.
3. **Reconcile drift** — `reconcile_identity` is the *single* add/restate/contradict policy
   point (guard pins one definition + two call sites), exactly as 087 did for
   `nominal_compatible`.
4. **Fingerprint coupling** — declaring metadata changes the logical schema fingerprint
   (it's in the IPC). Correct semantics; behavior-additive for no-key descriptors. Note in
   evidence; no action.
5. **The 089 line** — confirm 088 adds no writer at `schema_for_exprs`; inherited metadata
   riding through scan/join clones is fine (reading), synthesizing new output metadata is
   089 (the no-propagation-writer guard).

## Acceptance checklist

- [ ] `decode_column_logical_type` + metadata key in `ir.rs`; reuses 087 registry; `"0"`→NAKED.
- [ ] `from_descriptor` fail-closed validation; no `TableBinding` vector.
- [ ] `ResolvedColumn` unchanged (physical-only); binder-local `ResolvedLogicalColumn`.
- [ ] `Expr::Column` stamps the sidecar; `bind_logical_ascription` reconciles-then-sets.
- [ ] Join keys reconcile catalog + explicit via the one `reconcile_identity`.
- [ ] 087 `nominal_compatible` reused unchanged.
- [ ] 10 fixtures green (incl. join-survives-into-filter + D9 triad).
- [ ] 11 `adr088_*` guards + carried 087 guards green; lib (catalog/distributed) + full
      package + clippy clean; all-naked suites behavior-identical.
