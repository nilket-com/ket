# ADR 089 Evidence: Projection / CTAS Logical Identity Propagation

## Implementation Summary

Touched production files:

- `crates/toron/src/ir.rs`
- `crates/toron/src/ir/execution.rs`
- `crates/toron/src/binder.rs`

Touched tests/docs:

- `crates/toron/tests/catalog_logical_identity_test.rs`
- `crates/toron/tests/architecture/nominal_type_identity.rs`
- `crates/toron/tests/architecture/rune_frontend.rs`
- `crates/toron/plans/impl/0089_projection_ctas_logical_identity_propagation_evidence.md`

`rune_frontend.rs` had a test-helper-only clippy cleanup required by
`cargo clippy -p toron --all-targets -- -D warnings`.

No `coordinator.rs`, Wendelon, storage adapter, executor kernel, PBC opcode, SQL DDL/domain,
or Rune production surface was changed.

## Writer And Propagation

`ir.rs` now owns the single production writer:

```rust
field_with_logical_type_metadata(name, data_type, nullable, logical_type)
```

The writer constructs a plain `Field::new` for `LogicalTypeId::NAKED` and writes only
`LOGICAL_TYPE_ID_METADATA_KEY` for non-naked ids. It does not parse names, inspect
lineage, or clone arbitrary metadata.

`BoundExprProgram::root_logical_type(idx)` exposes the root sidecar. `schema_for_exprs`
uses that root sidecar plus the writer, which makes ordinary projections and aggregate
group keys preserve logical identity.

`binder.rs` updates the two projection schema rebuilds:

- `schema_for_exprs_with_output_names`
- `schema_for_projected_exprs_with_existing_names`

Both keep their existing naming behavior and write metadata from the selected root sidecar.

`aggregate_schema` and `aggregate_field` were not modified. Group keys carry identity via
`schema_for_exprs(group_expr)`; aggregate outputs remain naked.

## CTAS

`infer_ctas_schema` now decodes the planned output field with `decode_column_logical_type`
and re-emits through `field_with_logical_type_metadata`.

That path materializes direct/ascribed identity into the created descriptor while preserving
only the reserved logical metadata key. Non-reserved Arrow metadata is not cloned into CTAS
descriptors.

`coordinator.rs` remains unchanged. CTAS `IF NOT EXISTS` continues to use exact schema
equality, including logical metadata:

```text
existing.schema.as_ref() == schema.as_ref()
```

A physically-compatible naked target therefore returns `SchemaMismatch` when the inferred
CTAS schema is identity-bearing. An identical identity-bearing re-issue is a no-op, proving
the writer -> descriptor persistence -> decode round trip preserves the reserved metadata
entry set.

## Behavior Fixtures

`catalog_logical_identity_test.rs` now includes the ADR 089 gate:

- `projection_alias_preserves_identity`
- `projection_alias_same_identity_binds`
- `projection_alias_different_identity_rejects`
- `computed_projection_stays_naked`
- `explicit_ascribed_computed_projection_carries_identity`
- `aggregate_group_key_preserves_identity`
- `aggregate_output_stays_naked`
- `project_pruning_preserves_identity`
- `ctas_direct_projection_materializes_identity_in_descriptor`
- `ctas_computed_projection_stays_naked_in_descriptor`
- `ctas_if_not_exists_over_naked_target_rejects_schema_mismatch`
- `ctas_if_not_exists_identity_target_is_idempotent_on_reissue`
- `ctas_created_logical_projection_rebinds_as_source_without_fail_closed`
- `projection_alias_round_trips_without_fail_closed`

Result:

```text
cargo test -p toron --test catalog_logical_identity_test
23 passed
```

The idempotent CTAS re-issue fixture creates an identity-bearing CTAS target, reissues the
same `IF NOT EXISTS` plan, observes an empty no-op result, and verifies the resolved target
field still decodes as `CustomerId`.

## Architecture Guards

`nominal_type_identity.rs` retargets ADR 088's temporary no-writer guard into eight
positive ADR 089 guards:

- `adr089_logical_metadata_writer_is_centralized`
- `adr089_schema_for_exprs_writes_root_sidecar`
- `adr089_named_projection_preserves_metadata_on_rename`
- `adr089_project_pruning_preserves_root_identity`
- `adr089_group_keys_preserve_identity_aggregate_outputs_naked`
- `adr089_ctas_reemits_reserved_logical_metadata_only`
- `adr089_ctas_if_not_exists_exact_equality_policy`
- `adr089_no_lineage_or_name_heuristic_inference`

Results:

```text
cargo test -p toron --test architecture_test adr089_
8 passed

cargo test -p toron --test architecture_test
1051 passed
```

The one-writer guard is production-scoped, so ADR 087/088 fixtures may still author source
metadata.

## Regression Verification

Focused regression runs:

```text
cargo test -p toron --test ctas_test ctas_if_not_exists
8 passed

cargo test -p toron --test ctas_test
28 passed

cargo test -p toron --test group_by_test
125 passed

cargo test -p toron --test rune_relation_handles_test
53 passed

cargo test -p toron --lib
353 passed
```

Full package:

```text
cargo test -p toron
PASS
```

The full package run completed all Toron test targets and doc-tests; the live Wendelon
storage execution target retained its expected simulated-admin ignored fixture
(`11 passed; 1 ignored`).

Static checks:

```text
cargo fmt
cargo clippy -p toron --all-targets -- -D warnings
git diff --check
```

All passed after the test-helper-only clippy cleanup in `architecture/rune_frontend.rs`.

## Residual Scope

ADR 089 intentionally does not add SQL/domain authoring, dynamic registries, aggregate output
identity semantics, refinement predicates, Wendelon awareness, executor behavior, PBC
surface, or Rune production APIs. Those remain future ADR work.
