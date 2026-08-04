# Impl 092: SQL `CAST(x AS LogicalType)` — Coercing Ascription

**Pairs with:** `plans/adr/0092_sql_logical_cast.md` (Promoted, genuine 3-of-3). **Shape:**
transient `Expr::SqlLogicalCast` (planner-emitted, binder-consumed) + an **async pre-resolution
pass** (names → durable `LogicalTypeSpec` map) + a **sync binder coercing helper**
(reconcile-before-coerce, fed the resolved spec). `Expr::LogicalAscription` is **unchanged** (no
coerce flag). Toron-only.

## Invariants

- The resolved **spec** (physical + id), not just the id, reaches the binder (seed-only
  `logical_type_spec` would fail durable physical validation — the load-bearing correction).
- Resolution is **durable-only** (091's `resolve_durable_logical_type_by_name`, seed excluded,
  refresh-on-miss); the static seed never satisfies a SQL CAST.
- **Reconcile before coerce:** the conflict check runs on the operand's identity *before* any
  physical cast node is appended.
- Physical coercion is **existing-lattice-only** (no new kernels); same-physical (incl. Utf8/Boolean)
  = no coercion.
- The recursive expression binder stays **sync**; `SqlLogicalCast` never reaches `BoundExprProgram`
  or execution; `LogicalAscription`/Rune/087 untouched (the cast-erase escape is byte-identical).

## Phase 1 — IR (`ir.rs`)

```rust
// transient: planner emits, binder consumes; never in BoundExprProgram / execution.
Expr::SqlLogicalCast { expr: Box<Expr>, logical_name: String, try_cast: bool }
```
`is_pbc_pushdown_eligible` → not eligible (it's gone by bind). `Expr::LogicalAscription`
**unchanged**.

## Phase 2 — SQL lowering (`sql_planner.rs`)

At the CAST lowering site (`:3199`): for `CastKind::Cast`/`TryCast`/`SafeCast`, if the target is
`SqlDataType::Custom(name, mods)`, validate the 091 name shape (one-part, unquoted ASCII, no
modifiers, not `naked`; else bounded plan-time reject) and emit
`Expr::SqlLogicalCast { expr, logical_name: name, try_cast: matches!(kind, TryCast|SafeCast) }`
— **no catalog/seed lookup**. Primitive targets keep `Expr::Cast`/`Expr::TryCast` via
`lower_cast_target_type` (`:135`). The structural-hash / structural-eq / output-name /
qualified-column walks gain a `SqlLogicalCast` arm.

## Phase 3 — async pre-resolution pass (`binder.rs`, in `bind`)

Before each existing sync expression bind, resolve the expression slice's logical CAST targets:
walk nested expressions, collect `SqlLogicalCast.logical_name`s, resolve each via
`catalog.resolve_durable_logical_type_by_name(name).await` (091, durable-only, refresh-on-miss)
into a `LogicalCastResolutions(HashMap<String, LogicalTypeSpec>)`. Unknown → `LogicalTypeNotFound`
*before* sync bind. `try_cast: true` with a custom logical target → bounded "TRY_CAST logical
target unsupported" reject (D10). Thread the map into the expression-bind context (alongside
`config`).

Cover every general-expression bind site, not just SELECT projection/filter: `Filter` predicates,
`Project` expressions, `Aggregate` group expressions and aggregate arguments, `Exchange` keys,
`DELETE` predicates, `UPDATE` predicates/assignments, `RETURNING`, and any inserted projection
programs that call `bind_expr_program`. Join keys can be scanned for consistency, but they remain
the existing restricted key shape; a logical cast join key still rejects as a computed key unless a
later ADR expands join-key expressions.

## Phase 4 — sync binder coercing helper (`binder.rs`)

`bind_expr_into_program` (`:2861`) gains a `SqlLogicalCast` arm → `bind_logical_cast`:

```text
spec     = resolutions[logical_name]           // resolved DURABLE spec (physical + id) from the map
inner    = bind_expr_into_program(expr)        // bind the operand
existing = builder.node_logical_type(inner)    // its current sidecar
reconcile_identity(existing, spec.id, &source_physical)?   // 088, BEFORE coercion — conflict rejects here
node = if source_physical == spec.physical { inner }       // same physical (incl Utf8/Boolean): no coercion
       else if is_supported_explicit_numeric_cast(&source_physical, &spec.physical) {
                 builder.append_cast(inner, spec.physical.clone()) }   // existing lattice
       else { return Err(/* existing unsupported-physical-cast error */) }
builder.set_node_logical_type(node, spec.id); Ok(node)
```
No `BoundExprNode` variant; identity only in `node_logical_types` (087). The spec comes from the
map — **not** seed-only `logical_type_spec`/`validate_logical_ascription_physical`.

## Phase 5 — exec arm + CTAS composition

- `exec.rs` `evaluate_expr` over unbound `Expr` (the 087 passthrough site): add a `SqlLogicalCast`
  arm that fails loudly as an internal/unreachable expression if one survives binding. Do **not**
  passthrough to the inner expression; a missed binder site must not silently erase logical CAST.
- **CTAS: no new code.** The projection root produced by `CAST(raw AS CustomerId)` carries the
  `CustomerId` sidecar; 089's writer materializes it into the CTAS descriptor; 090 persists; 088
  decodes. Just a composition fixture.

## Phase 0 — fixtures first

`cast_defined_logical_type_ascribes_identity`; `cast_static_seed_does_not_satisfy_sql_name`;
`cast_undefined_fails_closed` (`LogicalTypeNotFound`); `cast_restates_same_identity`;
**`cast_conflicting_identity_rejects`** (`NominalTypeMismatch`);
**`cast_explicit_primitive_erase_then_relabel_binds`**;
`cast_int32_to_logical_int64_uses_existing_cast`; `cast_utf8_same_physical_ascribes_without_kernel`;
`cast_unsupported_cross_physical_rejects`;
**`ctas_projection_cast_materializes_and_survives_resync`** (089 ∘ 090);
**`rune_cast_then_ascribe_still_relabels`** (087 escape — `LogicalAscription` unchanged);
`try_cast_logical_target_rejects`; `explicit_typed_ctas_column_list_rejects`; regression
087/088/089/090/091 + all-naked.

## Phase 6 — guards

The `adr092_*` set (ADR list), notably
`adr092_resolved_durable_spec_reaches_binder_not_seed_only_logical_type_spec`;
`adr092_cast_reconciles_identity_before_physical_cast`;
`adr092_logical_ascription_unchanged_no_coerce_flag`;
`adr092_physical_coercion_reuses_existing_cast_lattice_no_new_kernels`;
`adr092_logical_cast_resolves_durable_only_before_sync_expr_binder`. **Reconcile** 091's
`no_cast_or_typed_ctas_authoring_yet` + the `lower_cast_target_type → DataType` cast-shape guards.
Carry 087/088/089/090/091.

## Verification

```
cargo fmt
cargo test -p toron --test <logical cast tests>
cargo test -p toron --test architecture_test            # FULL suite — reconcile, do not ship red (090/091 lesson)
cargo test -p toron --test logical_type_ddl_test --test logical_type_definition_test  # 090/091 intact
cargo test -p toron --test catalog_logical_identity_test --test rune_relation_handles_test  # 087/088 + Rune escape
cargo test -p toron --test ctas_test --test group_by_test  # all-naked
cargo test -p toron --lib
cargo clippy -p toron --all-targets -- -D warnings
cargo test -p toron
git diff --check
```

## Evidence doc (`plans/impl/092_..._evidence.md`)

Record: files touched (`ir.rs`/`sql_planner.rs`/`binder.rs`/`exec.rs` + tests; no Wendelon);
`Expr::SqlLogicalCast` transient + `LogicalAscription` unchanged; the async pre-pass + spec-map
threading (the durable spec, not seed-only, reaches the binder); reconcile-before-coerce; lattice
reuse (no new kernels); typed CTAS via 089 + survives-resync; TRY_CAST rejects; 087 escape
byte-identical; the reconciled 091 + cast-shape guards (full suite green, not red); 087–091 +
all-naked + `--all-targets` clippy green.

## Watch items

1. **Spec, not id, to the binder** — the binder MUST use the resolved spec from the pass; seed-only
   `logical_type_spec`/`validate_logical_ascription_physical` would fail durable types. Guard it.
2. **Reconcile before coerce** — conflict check on the operand identity precedes `append_cast`
   (`cast_conflicting_identity_rejects` + the guard).
3. **087 escape unchanged** — `LogicalAscription` gains no coerce flag; `rune_cast_then_ascribe_still_relabels`
   stays byte-identical.
4. **Reconcile guards in-change** — `SqlLogicalCast` bumps Expr-variant/cast-shape guards; reconcile
   them and run the FULL architecture suite (the explicit 090 lesson, honored by 091).
5. **No lattice widening / no new kernels** — coercion reuses `is_supported_explicit_numeric_cast`.
6. **Async pass covers all expression sites** — projections, filters, aggregates, exchange keys,
   DML predicates/assignments, and returning paths; a missed site leaves a `SqlLogicalCast`
   unresolved at sync bind — make that a loud error, not silent.

## Acceptance checklist

- [ ] `Expr::SqlLogicalCast` (transient, `try_cast` flag); `LogicalAscription` unchanged (no coerce flag).
- [ ] Planner emits it for `Custom` targets (091 name policy); no catalog/seed at plan time.
- [ ] Async pre-pass resolves durable-only → spec map; unknown → `LogicalTypeNotFound`; TRY_CAST logical → reject.
- [ ] Binder coercing helper: reconcile-before-coerce (operand identity), spec from the map, lattice-only coercion, sidecar set; no `BoundExprNode`/executor/PBC.
- [ ] Same-physical (incl Utf8/Boolean) no coercion; cross-physical numeric via existing lattice; else unsupported.
- [ ] Typed CTAS via 089 (no new grammar) + survives resync; explicit typed CTAS column list rejects.
- [ ] 087 cast-erase escape byte-identical; TRY_CAST logical target rejects.
- [ ] 091 + cast-shape guards reconciled; FULL architecture suite + `--all-targets` clippy green (not red); 087–091 + all-naked green.
