# Impl Plan — ADR 093: SQL `TRY_CAST(x AS LogicalType)`

**Migration for** `plans/adr/093_sql_logical_try_cast.md`. **Toron-only, `binder.rs`-localized.**
The production change is two un-gates + one branch in `bind_sql_logical_cast`; everything else is
fixtures and guards. No Wendelon, executor kernel, PBC, planner/IR, or Rune change.

## Pre-flight anchors (grounded in the dual — do not re-derive)

| Concern | Anchor |
|---|---|
| Planner already lowers logical TRY_CAST | `sql_planner.rs:3246` → `SqlLogicalCast { try_cast: true }` |
| Async durable resolution pass | `resolve_logical_casts` `binder.rs:330`; collector `collect_logical_cast_names` `binder.rs:798` |
| Rejection site #1 (collection) | `binder.rs:809-813` |
| Rejection site #2 (bind) | `binder.rs:3119-3123` |
| Logical-cast binder (the one to branch) | `bind_sql_logical_cast` `binder.rs:3111-3159`; reconcile at `:3140`; physical step `:3141-3152`; stamp at `:3153` |
| Numeric lattice (read-only, **do not widen**) | `is_supported_explicit_numeric_cast` `binder.rs:3291-3311` |
| TryCast node builder / nullability | `append_try_cast` `ir/execution.rs:385`; `root_nullable(TryCast)=Some(true)` `:603` |
| Same-physical executor passthrough (no new kernel) | `evaluate_try_cast` `exec.rs:4117-4120` |
| 089 nullability writer (CTAS path) | `binder.rs:3735` / `:3764` thread `root_nullable`; `infer_ctas_schema` preserves `is_nullable` `:941` |
| `exec.rs` `SqlLogicalCast` exhaustiveness arm | unchanged — `SqlLogicalCast` (try or not) is binder-consumed, never reaches exec |
| 092 guard holding the rejection assert | `adr092_binder_resolves_sql_logical_casts_registry_only_before_sync_bind` (`nominal_type_identity.rs:762`, assert at `:773`) |

## Phase 1 — Production un-gate (two edits, both `binder.rs`)

**1a. `collect_logical_cast_names` — make name collection try_cast-agnostic.** Delete the
`if *try_cast { return Err(...) }` block (`binder.rs:809-813`); always collect + recurse:

```rust
Expr::SqlLogicalCast {
    expr,
    logical_name,
    try_cast: _,                       // collection no longer depends on try_cast (D2)
} => {
    names.insert(logical_name.clone());
    collect_logical_cast_names(expr, names)
}
```

Effect: logical TRY_CAST names now flow through `resolve_logical_casts` → durable-only spec map,
identical to strict CAST. Unknown names still → `LogicalTypeNotFound`.

**1b. `bind_sql_logical_cast` — delete the gate, branch the physical step (contract γ).** Remove
`binder.rs:3119-3123`. Keep the spec lookup, `bind_expr_into_program`, `source`/`existing` reads,
and the `reconcile_identity` call (`:3140`) **exactly as-is** (reconcile stays first — D4). Replace
the physical-step `let output = …` (`:3141-3152`) with a `try_cast` branch:

```rust
let logical_type = reconcile_identity(existing, spec.id, &source)?;     // D4 — unchanged, runs first
let output = if try_cast {
    // D5/D7: bind wherever strict CAST binds; categorically nullable.
    if source == spec.physical || is_supported_explicit_numeric_cast(&source, &spec.physical) {
        // D6: same-physical served by the exec.rs:4118 passthrough — no new kernel.
        builder.append_try_cast(inner, spec.physical.clone(), spec.physical.clone())
    } else {
        return Err(BindingError::TypeMismatch {
            message: format!(
                "unsupported TRY_CAST: {source:?} to logical type {}({:?}); only same-physical \
                 casts or Int32, Int64, Float32, and Float64 numeric conversions are supported",
                spec.name, spec.physical
            ),
        });
    }
} else {
    // D8: ADR 092 strict branch, byte-identical.
    if source == spec.physical {
        inner
    } else if is_supported_explicit_numeric_cast(&source, &spec.physical) {
        builder.append_cast(inner, spec.physical.clone(), spec.physical.clone())
    } else {
        return Err(BindingError::TypeMismatch {
            message: format!(
                "unsupported CAST: {source:?} to logical type {}({:?}); only same-physical casts \
                 or Int32, Int64, Float32, and Float64 numeric conversions are supported",
                spec.name, spec.physical
            ),
        });
    }
};
if !builder.set_node_logical_type(output, logical_type) {               // stamp on the (new) node — D5/D10
    return Err(BindingError::UnsupportedExpression {
        message: "SQL logical cast target is malformed".to_string(),
    });
}
Ok(output)
```

**Invariant check (carry from 087):** `append_try_cast` pushes one node + one `NAKED`
`node_logical_types` entry (via `append_node`), then `set_node_logical_type` overwrites it — the same
arity-preserving pattern the strict numeric branch already uses with `append_cast`. No
`NodeLogicalTypeArityMismatch` risk. Identity stamping stays in `bind_sql_logical_cast` only; physical
`bind_try_cast` is **not touched** (D10 — 087 erase doctrine intact).

That is the entire production diff.

## Phase 2 — Behavior fixtures (`tests/logical_type_ddl_test.rs`)

Reuse the 092 durable-registration harness (the fixtures register `{CustomerId=Int64,
ProductId=Int64}` durably, seed excluded). Add durable defs `SmallCustomerId AS INT` (Int32),
`Sku AS TEXT` (Utf8), `Flag AS BOOLEAN` for the new shapes.

1. `try_cast_widening_logical_type_binds_nullable` — `TRY_CAST(raw_i32 AS CustomerId)`: root type
   `Int64`, root logical `CustomerId`, `root_nullable == Some(true)`; execute → no NULLs (widening
   can't fail).
2. `try_cast_narrowing_failure_yields_null_keeps_identity` — `TRY_CAST(big_i64 AS SmallCustomerId)`
   with a value `> i32::MAX`: NULL in that row; output schema carries logical `SmallCustomerId`,
   nullable. *(The only gate that behaviorally differs from strict CAST, which would `Kernel`-error.)*
3. `try_cast_same_physical_numeric_binds_nullable` — `TRY_CAST(id_i64 AS CustomerId)` (Int64→Int64):
   appends a `TryCast` node (assert via plan shape / `root_nullable == Some(true)`); values
   unchanged.
4. `try_cast_same_physical_utf8_binds_nullable_without_new_kernel` — `TRY_CAST(sku AS Sku)`
   (Utf8→Utf8): binds, passthrough (`exec.rs:4118`), nullable, logical `Sku` stamped.
5. `try_cast_same_physical_boolean_binds_nullable` — `TRY_CAST(active AS Flag)` (Boolean→Boolean).
6. `try_cast_conflicting_identity_rejects_at_bind` — `TRY_CAST(product_id AS CustomerId)` →
   `NominalTypeMismatch` at bind; assert **no** plan executes, **no** NULL. (The doctrine gate.)
7. `try_cast_explicit_primitive_erase_then_relabel_binds` — `TRY_CAST(CAST(product_id AS BIGINT) AS
   CustomerId)` binds (inner physical cast erases → naked → re-ascribe).
8. `try_cast_undefined_fails_closed` — `TRY_CAST(x AS NotDefined)` → `LogicalTypeNotFound` (durable-
   only; assert the static seed name does **not** satisfy it, mirroring 091/092).
9. `try_cast_unsupported_cross_physical_rejects` — `TRY_CAST(name_utf8 AS CustomerId)` (Utf8→Int64)
   → `TypeMismatch`, same as strict CAST.
10. `safe_cast_logical_type_aliases_try_cast` — `SAFE_CAST(raw_i32 AS CustomerId)` == fixture 1.
11. `ctas_try_cast_materializes_nullable_logical_identity_and_survives_resync` — `CREATE TABLE t AS
    SELECT TRY_CAST(raw AS CustomerId) AS cid …`: persisted `cid` is **nullable** + carries
    `CustomerId`; re-decode after a catalog resync round-trips (089 ∘ 090). Compare against the 092
    strict-CTAS fixture which is non-nullable.

**Flip:** `logical_cast_try_cast_to_logical_target_rejects` (`logical_type_ddl_test.rs:518`) — rewrite
from "must reject" to assert the bound `TryCast` root + logical id + nullable (or fold into fixture 1
and delete).

## Phase 3 — Architecture guards (`tests/architecture/`)

**Invert (discipline #4):**
- In `adr092_binder_resolves_sql_logical_casts_registry_only_before_sync_bind`
  (`nominal_type_identity.rs:762`), **remove** the assertion at `:773`
  (`assert!(collect.contains("TRY_CAST/SAFE_CAST to logical type is not supported"))`) — both
  rejection strings (`binder.rs:811`, `:3121`) are deleted in Phase 1. The rest of that 092 guard
  (registry-only-before-sync-bind) stays.

**Add:**
- `adr093_binder_collects_both_cast_and_try_cast_logical_names` — `collect_logical_cast_names` source
  no longer rejects `try_cast` and collects the name for both arms (the positive inverse of the
  removed assertion).
- `adr093_logical_try_cast_reconciles_identity_before_try_cast` — `bind_sql_logical_cast` source has
  `reconcile_identity(` textually **before** `append_try_cast(` (the doctrine ordering).
- `adr093_logical_try_cast_categorically_nullable_no_same_physical_elide` — the `try_cast` branch
  appends `append_try_cast` and does **not** reuse `inner` (logical mirror of the physical no-elide
  guard).
- `adr093_logical_try_cast_uses_existing_try_cast_node_no_executor_or_pbc` — no new `BoundExprNode`
  variant; `exec.rs` gains no new arm; PBC opcode set unchanged.
- `adr093_physical_bind_try_cast_remains_identity_free` — `source_block(&binder, "fn bind_try_cast")`
  still has no `set_node_logical_type` (re-pins 087/091; explicitly proves 093 stayed out of the
  physical helper).
- `adr093_ctas_try_cast_nullability_flows_through_089_writer` — the schema writers
  (`schema_for_exprs_with_output_names`, `infer_ctas_schema`) thread `root_nullable` / `is_nullable`
  into `field_with_logical_type_metadata` (no hardcoded `false`).

**Keep green (verified physical-`bind_try_cast`-scoped — do not edit):**
- `sql_planner_surface.rs:867-878` (physical no-elide + numeric-only gate + append shape).
- `nominal_type_identity.rs:187,192-193` (physical TryCast identity-free).
- 092's resolved-spec-not-seed-only + `SqlLogicalCast`-transient guards; PBC Cast/TryCast
  ineligibility.

## Phase 4 — Verification

```
cargo test -p toron --test logical_type_ddl_test          # behavior gates (Phase 2)
cargo test -p toron --test architecture_test              # guards: inverted + added + kept-green
cargo test -p toron                                        # full suite — 087–092 + all-naked + strict-CAST green
```

**Acceptance:** all eleven behavior gates pass; the inverted guard (assert removed) + flipped fixture
are green in their new form; the six new guards pass; every keep-green guard stays green (proving 093
left the physical try-cast path, strict CAST, and 089 untouched); the full suite is **green, not red**
(the 090-review lesson — reconcile guards in-change).

## Risks / notes

- **The only behavioral divergence from strict CAST** is fixture 2 (narrowing → NULL vs Kernel) and
  the categorical nullability of same-physical/widening outputs. Everything else (what binds, what
  rejects, identity reconcile) is identical to 092 — keep the strict `else` branch byte-identical to
  avoid regressing the 092 gates.
- **No serialization/wire surface** changes, so no mixed-version caveat (unlike 087).
- If a future ADR wants `TRY_CAST` to non-numeric *cross*-physical (e.g. `Utf8 → CustomerId(Int64)`
  parsing with NULL-on-unparseable), that needs new physical try-cast kernels and is explicitly out
  of 093's scope (rejects at bind today, same as strict CAST).
