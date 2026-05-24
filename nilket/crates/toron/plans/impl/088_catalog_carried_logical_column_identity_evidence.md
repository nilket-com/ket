# ADR 088 Evidence: Catalog-Carried Logical Column Identity

## Implementation Summary

Touched production files:

- `crates/toron/src/ir.rs`
- `crates/toron/src/catalog.rs`
- `crates/toron/src/binder.rs`

Touched tests/docs:

- `crates/toron/tests/catalog_logical_identity_test.rs`
- `crates/toron/tests/architecture/nominal_type_identity.rs`
- `crates/toron/plans/impl/088_catalog_carried_logical_column_identity_evidence.md`

No Wendelon descriptor, Wendelon wire, executor kernel, PBC opcode, SQL DDL/domain, or Rune
surface file was changed.

## Carrier And Decode

`ir.rs` now owns the reserved metadata key:

```text
nilket.toron.logical_type_id
```

`decode_column_logical_type(field)` reads Arrow field metadata, treats a missing key or
`"0"` as `LogicalTypeId::NAKED`, parses decimal `u32`, checks the ADR 087 registry, and
requires known non-naked ids to match the field physical `DataType`. Parse failure,
unknown id, and physical mismatch return a bounded typed error
(`LogicalTypeMetadataError`) rather than an unstructured public `Result<_, String>`.

No column-name heuristic was added; names like `customer_id` and `product_id` matter only
in fixtures.

## Catalog Gate

`TableBinding::from_descriptor` validates every decoded Arrow field immediately after
`descriptor.arrow_schema()` succeeds:

```text
descriptor.arrow_schema() -> validate_logical_column_metadata(schema.as_ref()) -> row identity
```

Invalid declared identity fails closed as `CatalogError::FingerprintResolution`. No
`TableBinding.logical_column_types` vector was introduced; the descriptor schema metadata
remains the durable catalog carrier.

## Binder Gate

`ResolvedColumn` remains physical-only. ADR 088 adds only a binder-local wrapper:

```rust
struct ResolvedLogicalColumn {
    column: ResolvedColumn,
    logical_type: LogicalTypeId,
}
```

`Expr::Column` now resolves a `ResolvedLogicalColumn`, appends the physical
`ResolvedColumn`, and stamps `BoundExprProgram.node_logical_types` with
`set_node_logical_type`. That sidecar remains the only serialized logical carrier.

`bind_logical_ascription` now reads the existing node identity and calls the single shared
`reconcile_identity(existing, declared, physical)` helper before setting the sidecar. This
allows add/restate, rejects contradiction with `NominalTypeMismatch`, and preserves the
ADR 087 cast-erase path because cast outputs remain naked.

Join key resolution now returns `ResolvedLogicalColumn`; `resolved_join_key` reconciles
catalog identity with any explicit join-key ascription before the unchanged ADR 087 join
gate (`nominal_compatible`) runs. A bare join key is therefore `(catalog_id, NAKED)` and
keeps the catalog id.

No projection, aggregate, CTAS, or DML output metadata writer was added. Existing scan
projection and join schema cloning may carry source field metadata; `Field::new` rebuilds
still drop metadata for derived outputs, which remains ADR 089 scope.

## Behavior Fixtures

`catalog_logical_identity_test.rs` pins the ADR 088 behavior gates:

- metadata column vs naked literal binds and executes
- same catalog id comparison binds
- different catalog id comparison rejects
- catalog id vs naked non-literal rejects
- invalid metadata rejects at descriptor binding: non-decimal, unknown id, physical mismatch
- same-id catalog join binds
- different-id catalog join rejects
- filter above join sees source identities carried by cloned join fields
- D9 triad: restate binds, contradict rejects, cast-erase-then-reascribe binds

Result:

```text
cargo test -p toron --test catalog_logical_identity_test
9 passed
```

## Architecture Guards

`nominal_type_identity.rs` now carries 11 `adr088_*` guards in addition to ADR 087 guards:

- Arrow field metadata key, no Wendelon descriptor field
- no `TableBinding` parallel logical vector
- `ResolvedColumn` physical-only
- decoder validates parse, registry, and physical type
- descriptor binding validates declared logical metadata
- column binding stamps `node_logical_types`
- join key takes identity from catalog metadata
- exactly one shared `reconcile_identity` helper with both call sites
- explicit ascription cannot contradict existing identity
- no column-name heuristic
- no projection/CTAS logical metadata writer

Results:

```text
cargo test -p toron --test architecture_test nominal_type_identity
21 passed

cargo test -p toron --test architecture_test
1044 passed
```

## Regression Verification

Focused all-naked/Rune regressions:

```text
cargo test -p toron --test group_by_test --test join_kind_test --test rune_relation_handles_test --test rune_sandbox_test
group_by_test: 125 passed
join_kind_test: 8 passed
rune_relation_handles_test: 53 passed
rune_sandbox_test: 8 passed
```

Library/catalog/distributed coverage:

```text
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
cargo clippy -p toron -- -D warnings
git diff --check
```

All passed.

## Residual Scope

ADR 088 intentionally stops at catalog-carried source identity. Direct projection
propagation, aggregate/output metadata synthesis, CTAS emission, SQL DDL/domain syntax,
dynamic registries, and refinement predicates remain future ADR work, with ADR 089 as the
direct propagation successor.
