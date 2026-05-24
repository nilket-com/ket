# Impl 089: Projection / CTAS Logical Identity Propagation

**Pairs with:** `plans/adr/089_projection_ctas_logical_identity_propagation.md` (Promoted,
genuine 3-of-3). **Posture:** a schema-construction writer — the symmetric inverse of 088,
not expression-graph propagation, not lineage inference. The 087 sidecar is the source; the
088 reserved field-metadata key is the sink; one helper converts at the projection boundary.

## Invariants

- One central writer is the *only* **production** path that emits
  `LOGICAL_TYPE_ID_METADATA_KEY`; test fixtures/catalog helpers may still author metadata to
  simulate persisted 088 descriptors (the guard scans `crates/toron/src/**` only).
- Identity is read from the bound **root sidecar** only (`root_logical_type`) — never names,
  lineage, child traversal, or descriptor lookup.
- Computed roots stay naked (087 D6) → writer emits nothing; explicit ascription is the only
  computed escape (D9).
- **Round-trip invariant:** every emitted non-naked field decodes cleanly via 088's
  `decode_column_logical_type` (a non-naked sidecar's physical always equals the registry
  physical) → 089 never creates a mid-pipeline fail-closed.
- Behavior-additive: naked roots emit nothing → all-naked schemas byte-identical to today.
- `coordinator.rs` **unchanged**: CTAS `IF NOT EXISTS` keeps exact logical-schema equality.

## Phase 0 — fixtures first

Extend `catalog_logical_identity_test.rs` (or add `..._propagation_test.rs`). The 15 gates:

1. `projection_alias_preserves_identity` (`customer_id AS cid`; `cid = raw_id` rejects)
2. `projection_alias_same_identity_binds`
3. `projection_alias_different_identity_rejects`
4. `computed_projection_stays_naked` (`customer_id + 0 AS v`)
5. `explicit_ascribed_computed_projection_carries_identity`
6. `aggregate_group_key_preserves_identity`
7. `aggregate_output_stays_naked`
8. `project_pruning_preserves_identity`
9. `ctas_direct_projection_materializes_identity_in_descriptor`
10. `ctas_computed_projection_stays_naked_in_descriptor`
11. `ctas_if_not_exists_over_naked_target_rejects_schema_mismatch`
12. `ctas_if_not_exists_identity_target_is_idempotent_on_reissue` (round-trip fidelity)
13. `ctas_created_logical_projection_rebinds_as_source_without_fail_closed`
14. `projection_alias_round_trips_without_fail_closed` (the invariant)
15. existing all-naked projection/group-by/CTAS/Rune/lib suites stay green

## Phase 1 — central writer (`ir.rs`, beside the 088 decoder)

```rust
pub fn field_with_logical_type_metadata(
    name: impl Into<String>,
    data_type: DataType,
    nullable: bool,
    logical_type: LogicalTypeId,
) -> Field {
    let field = Field::new(name, data_type, nullable);
    if logical_type.is_naked() {
        return field;                                  // naked → no key (byte-identical to today)
    }
    field.with_metadata(HashMap::from([(
        LOGICAL_TYPE_ID_METADATA_KEY.to_string(),
        logical_type.0.to_string(),                    // same decimal the 088 decoder reads
    )]))
}
```

The *only* production writer of the reserved key. No type names, no parsing, no inference.

## Phase 2 — root accessor + generic emit (`ir/execution.rs`)

```rust
impl BoundExprProgram {
    pub fn root_logical_type(&self, root_index: usize) -> Option<LogicalTypeId> {
        self.node_logical_type(*self.roots.get(root_index)?)
    }
}
```

`schema_for_exprs` (`:792`) builds each field via the writer:

```rust
let logical_type = exprs.root_logical_type(idx).unwrap_or(LogicalTypeId::NAKED);
field_with_logical_type_metadata(name, data_type, nullable, logical_type)
```

This one change gives **D3** (projection) and, transitively, **D6** (aggregate group keys via
`aggregate_schema`'s `schema_for_exprs(group_expr)`).

## Phase 3 — named / pruned rebuilds + CTAS (`binder.rs`)

- **`schema_for_exprs_with_output_names` (`:3559`)** — replace the trailing `Field::new`
  with `field_with_logical_type_metadata(name, data_type, nullable,
  exprs.root_logical_type(idx).unwrap_or(NAKED))`. (D4)
- **`schema_for_projected_exprs_with_existing_names` (`:3582`)** — same swap, keeping the
  existing-name selection (scan-projection pruning). (D5)
- **`aggregate_schema` (`:3544`)** — *no change*; group identity flows from D3, and
  `aggregate_field` (`:3610`) stays untouched → aggregate outputs naked. (D6 — **do not add a
  second emission path here**.)
- **`infer_ctas_schema` (`:760`)** — after the existing name/type validation, decode then
  re-emit reserved-only:
  ```rust
  let logical_type = decode_column_logical_type(field)
      .map_err(|error| BindingError::UnsupportedExpression { message: error.to_string() })?;
  fields.push(field_with_logical_type_metadata(
      field.name().clone(), field.data_type().clone(), field.is_nullable(), logical_type));
  ```
  Materializes identity into the created descriptor; drops any non-reserved source metadata. (D7)

## Phase 4 — CTAS idempotency (`coordinator.rs`) — no change (D12)

`coordinator.rs:1106` `existing.schema.as_ref() == schema.as_ref()` stays exact. Naked
existing target vs identity-bearing CTAS → `SchemaMismatch` (the honest answer). **The watch
item is the happy path:** a re-issued *identical* identity-bearing CTAS must still hit
`== → no-op`, which requires the persist→decode round-trip to reproduce exactly
`{reserved_key: "<id>"}` (fixture 12).

## Phase 5 — guards (`tests/architecture/nominal_type_identity.rs`)

Retarget `adr088_no_new_projection_or_ctas_logical_metadata_writer` →
`adr089_logical_metadata_writer_is_centralized` (exactly one `field_with_logical_type_metadata`
emits the key; grep **production** `src/**` for no ad-hoc reserved-key `with_metadata`).
Plus: `adr089_schema_for_exprs_writes_root_sidecar`;
`adr089_named_projection_preserves_metadata_on_rename`;
`adr089_project_pruning_preserves_root_identity`;
`adr089_group_keys_preserve_identity_aggregate_outputs_naked` (`aggregate_field` does not
emit); `adr089_ctas_reemits_reserved_logical_metadata_only`;
`adr089_ctas_if_not_exists_exact_equality_policy` (`coordinator.rs:1106` unchanged);
`adr089_no_lineage_or_name_heuristic_inference`. Carry 087/088 guards.

## Verification

```
cargo fmt
cargo test -p toron --test catalog_logical_identity_test
cargo test -p toron --test architecture_test adr089_
cargo test -p toron --test architecture_test            # carried 087/088 guards
cargo test -p toron --test group_by_test                # all-naked aggregate green
cargo test -p toron --lib                               # catalog + distributed + CTAS unit paths
cargo test -p toron --test rune_relation_handles_test rune_nominal_   # 087/088 intact
cargo clippy -p toron --all-targets -- -D warnings      # --all-targets: lint test targets too
cargo test -p toron
git diff --check
```

## Evidence doc (`plans/impl/089_..._evidence.md`)

Record: files touched (`ir.rs`/`ir/execution.rs`/`binder.rs` + tests — **coordinator
unchanged**); the single writer + its call sites; decode-then-reemit in `infer_ctas_schema`;
that `aggregate_schema`/`aggregate_field`/`coordinator` were not modified; the 15 fixture
results (esp. the idempotent re-issue — persist→decode reproduces the reserved-key
entry-set); no Wendelon/executor/PBC/DDL/Rune change; all-naked + full-package +
`--all-targets` clippy green.

## Watch items

1. **CTAS round-trip fidelity (verify-first):** fixture 12 — `infer_ctas_schema` emit →
   `create_table` persist → `from_descriptor` decode → fresh `infer_ctas_schema` must yield a
   byte-equal reserved-key entry-set so a re-issued identity-bearing CTAS is a no-op. If
   persistence injects/strips a field-metadata entry, normalize the comparison to the reserved
   key — but ground it, don't assume.
2. **One production writer only** — the retargeted guard scans `src/**`; it must allow test
   fixtures to author metadata (Codex R4). Grep the reserved-key string + `with_metadata` in
   production schema builders.
3. **Don't double-write group keys** — D6 is automatic via `schema_for_exprs`; do not also
   emit in `aggregate_schema`, and keep `aggregate_field` naked.
4. **`--all-targets` clippy** — plain `clippy -p toron` skips integration-test targets (088
   review finding); use `--all-targets` so a test-only unused import can't slip.

## Acceptance checklist

- [ ] `field_with_logical_type_metadata` in `ir.rs`; only production reserved-key writer; naked → no key.
- [ ] `root_logical_type` accessor; `schema_for_exprs` emits via writer.
- [ ] named + pruned projection rebuilds use the writer.
- [ ] `aggregate_schema`/`aggregate_field`/`coordinator.rs` unchanged → group keys carry, outputs naked, exact CTAS equality.
- [ ] `infer_ctas_schema` decode-then-reemit reserved-only.
- [ ] 15 fixtures green (incl. idempotent-re-issue + round-trip-no-fail-closed).
- [ ] 088 no-writer guard retargeted to centralized-writer (production-scoped); `adr089_*` + carried guards green.
- [ ] all-naked (projection/aggregate/CTAS/Rune) byte-identical; full package + `--all-targets` clippy clean.
