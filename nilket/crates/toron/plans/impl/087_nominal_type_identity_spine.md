# Impl 087: Nominal Type Identity Spine

**Pairs with:** `plans/adr/087_nominal_type_identity_spine.md` (Promoted, genuine
3-of-3). **Posture:** substrate leap (IR + `BoundExprProgram` + binder), piloted via
Rune. The **`node_logical_types` sidecar ripple is the bulk**; `Expr::LogicalAscription`
and the Rune surface are small. No Arrow-`DataType` / Wendelon-wire / executor / PBC
change.

The shape to hold in mind: a database `PhantomData<T>`. The physical Arrow layout is
unchanged and is what executes; `LogicalTypeId` is the phantom marker the **binder**
type-checks; `cast(type_i64())` is the explicit "erase the marker" operation.

## Invariants

- Arrow `DataType` stays the physical currency; `LogicalTypeId` is a parallel sidecar.
- `BoundExprProgram` arity: `nodes.len() == node_types.len() == node_logical_types.len()`.
- `Expr::LogicalAscription` disappears at bind into sidecar metadata — **no
  `BoundExprNode`, no executor branch, no PBC opcode**.
- The nominal gate engages only when a non-naked identity is present → all-naked
  queries unchanged.
- Same-version Toron deploy (the serialized `BoundExprProgram` payload gains a field;
  `serde(default)` can't rescue mixed versions — the arity invariant fails on legacy).

## Phase 1 — IR (`ir.rs`)

- `LogicalTypeId(u32)` (`derive Copy/Eq/Hash/Serialize/Deserialize`; `NAKED = Self(0)`).
- `LogicalTypeSpec { id, name: &'static str, physical: DataType }` + a tiny static
  pilot registry (`CustomerId → (1, Int64)`, `ProductId → (2, Int64)`) with
  `spec_for(id)` / lookup-by-marker.
- `Expr::LogicalAscription { expr: Box<Expr>, logical_type: LogicalTypeId }`.

## Phase 2 — `BoundExprProgram` sidecar + the ripple (`ir/execution.rs`) — THE BULK

- Add `node_logical_types: Vec<LogicalTypeId>` to `BoundExprProgram` (`:98`).
- `BoundExprProgramError::NodeLogicalTypeArityMismatch { nodes, node_logical_types }`;
  `validate` (`:599`) asserts the new arity alongside `node_types`.
- Builder (`:141`): every `append_*` pushes `LogicalTypeId::NAKED`; add
  `node_logical_type(id)` getter + `set_node_logical_type(id, lt)` setter; `build`
  (`:478`) and `empty` (`:504`) carry the vector.
- **Preserve the sidecar at every clone/rewrite/merge site** (verified to exist — each
  must copy/extend `node_logical_types`, default NAKED for new nodes):
  `clone_bound_subgraph` (`binder.rs:1369`), `project_program_roots` (`:2414`),
  `rewrite_program_ordinals` (`:2511` — move-by-value, must carry the field),
  `merge_aggregate_args` (`:2614`), and any direct `BoundExprProgram { … }` in
  tests/helpers. The arity invariant + the clone/merge guard are the teeth.

## Phase 3 — error (`error.rs`)

`BindingError::NominalTypeMismatch { left: String, right: String, physical: String }`;
message `nominal type mismatch: CustomerId(Int64) vs ProductId(Int64)` (distinct from
physical `TypeMismatch`).

## Phase 4 — binder (`binder.rs`)

- **`LogicalAscription` bind:** bind inner → resolve spec → require
  `inner_physical == spec.physical` (else binding error) → `set_node_logical_type` on
  the bound root → return same physical root.
- **One shared helper** `nominal_compatible(left_phys, right_phys, left_id, right_id,
  left_is_literal, right_is_literal) -> Result<(), …>` implementing the strict rule
  (D7): naked/naked OK; same-non-naked OK; non-naked vs naked-**literal** OK; different
  non-naked → mismatch; **non-naked vs naked-non-literal → mismatch** (one-side bypass
  blocked).
- **Comparison gate:** `bind_comparison` (`:3130`) calls it **after**
  `coerce_comparison_literals` (`:3221`) + the existing physical equality check.
  **Literal-coercion preserves identity** (Codex iter-3): the implicit coercion path
  must NOT reset a node's sidecar — a naked literal stays naked, an ascribed literal
  stays ascribed (fixture 10 guards this).
- **`IN` gate:** `bind_in_list` calls the same helper for target/each-member after the
  existing per-member coercion + physical check (one policy, not two).
- **Join gate:** `ResolvedJoinKey { column: ResolvedColumn, logical_type }`;
  `bind_join_key_pair` (`:1844`) accepts `Column(..)` and
  `LogicalAscription { expr: Column(..), logical_type }` (peel to column-ordinal +
  identity), rejecting other computed keys as today; `validate_join_key_types`
  (`:1904`) calls the shared helper. Execution still stores `left_keys`/`right_keys`
  ordinals only (`:557-582`).

## Phase 5 — Rune surface (`rune_engine.rs`)

```rust
#[derive(Any)] struct CustomerId;
#[derive(Any)] struct ProductId;

impl RuneExpr {
    fn logical_ascription(self, lt: LogicalTypeId) -> RuneExpr {
        RuneExpr { expr: Expr::LogicalAscription { expr: Box::new(self.expr), logical_type: lt } }
    }
}
fn as_customer_id(e: RuneExpr) -> RuneExpr { e.logical_ascription(LogicalTypeId(1)) }
fn as_product_id(e: RuneExpr) -> RuneExpr { e.logical_ascription(LogicalTypeId(2)) }

m.ty::<CustomerId>()?; m.ty::<ProductId>()?;
m.associated_function(rune::Params::new("as_logical_type", [CustomerId::HASH]), as_customer_id)?;
m.associated_function(rune::Params::new("as_logical_type", [ProductId::HASH]),  as_product_id)?;
```

No `.as_logical_type(String)`, no parser, no source normalizer (081/085 doctrine).
**Watch:** confirm the exact `rune::Params::new` signature + `T::HASH` constant at impl
(grounded that `params.rs` exists + `.method::<T>()` works; verify the precise API).

## Phase 0 — fixtures first (`tests/rune_relation_handles_test.rs` + a binder/join test)

The 13 gates from the ADR:

1. `rune_nominal_turbofish_binds_vs_literal`
2. `rune_nominal_same_identity_binds`
3. `rune_nominal_different_identity_rejects`
4. `rune_nominal_one_side_oops_rejects`
5. `rune_nominal_ascription_physical_mismatch_rejects`
6. `rune_nominal_both_side_cast_binds` (marker erased on both)
7. `rune_nominal_one_side_cast_still_rejects` (the resolved strict rule)
8. `rune_nominal_in_list_same_identity_binds`
9. `rune_nominal_in_list_different_identity_rejects`
10. `rune_nominal_ascribed_literal_identity_is_preserved` (the D6 subtlety — implicit
    literal coercion must not strip an explicit ascription)
11. `pure_join_gate_rejects_nominal_mismatch` (hand-built `LogicalPlan::Join`)
12. `pure_join_gate_accepts_nominal_match` (hand-built `LogicalPlan::Join`)
13. existing all-naked SQL/Rune nets stay green (the behavior-additivity safety proof)

## Phase 6 — guards (`tests/architecture/*`)

The 10 `adr087_*` guards: `..._logical_type_id_is_sidecar_not_arrow_replacement`;
`..._bound_expr_program_tracks_logical_type_arity`;
`..._logical_ascription_has_no_bound_runtime_node`;
`..._sidecar_preserved_through_clone_and_merge` (source-read `clone_bound_subgraph` +
`merge_aggregate_args` for the sidecar copy);
`..._nominal_comparison_gate_after_physical_coercion`;
`..._nominal_in_list_gate_reuses_shared_helper`;
`..._nominal_join_key_gate_extends_existing_type_gate`;
`..._nominal_mismatch_has_dedicated_error`; `..._rune_surface_uses_params_not_strings`;
`..._cast_outputs_naked_identity`.

## Verification

```
cargo fmt
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test join_kind_test          # pure join-key gate fixtures
cargo test -p toron --test architecture_test adr087_
cargo test -p toron --test group_by_test           # all-naked stays green
cargo test -p toron --test predicate_widening_test # all-naked IN stays green
cargo test -p toron --test rune_sandbox_test
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron                                # full package (incl. distributed)
```

## Evidence doc (`plans/impl/087_nominal_type_identity_spine_evidence.md`)

Record: files touched (`ir.rs` / `ir/execution.rs` / `binder.rs` / `error.rs` /
`rune_engine.rs` + tests); the sidecar-ripple sites updated (the 7 + direct
constructions); the 13 fixture results; the `Params` registration shape that compiled;
confirmation no Arrow-`DataType` / Wendelon / executor / PBC change; the same-version
serialized-payload note + that the full-package (distributed) suite stays green.

## Watch items

1. **The ripple is the failure mode** — a clone/rewrite/merge that drops
   `node_logical_types` either trips the arity invariant (good) or silently strips
   identity (bad). The clone/merge guard + arity validation are the safety net; run the
   full suite (distributed paths construct `BoundExprProgram`).
2. **Strict rule** — one-side cast must reject (assert fixture 7); the naked-vs-literal
   exception keeps `customer_id = 7` viable.
3. **Literal-coercion preserves identity** — the implicit coercion path must not reset
   the sidecar (fixture 10). This is the one place "new node = naked" does NOT apply:
   coercing an existing literal in place keeps whatever identity it already carried.
4. **`LogicalAscription` leaves no runtime node** — guard forbids
   `BoundExprNode::LogicalAscription`; binding must fold it into the sidecar.
5. **Serialized payload** — same-version Toron assumed; `serde(default)` can't rescue
   mixed versions (arity invariant). Record the assumption.
6. **`rune::Params` API** — confirm the exact constructor + `T::HASH` at impl (grounded
   it exists; verify the call shape compiles).

## Acceptance checklist

- [ ] `LogicalTypeId` sidecar + `LogicalTypeSpec` registry; Arrow `DataType` unchanged.
- [ ] `Expr::LogicalAscription` binds to sidecar metadata; **no `BoundExprNode`**.
- [ ] `node_logical_types` + arity invariant; sidecar preserved at all clone/merge/
      rewrite/build/empty sites.
- [ ] One shared nominal helper; strict rule; comparison/`IN`/join gates call it.
- [ ] Implicit literal coercion preserves identity (fixture 10).
- [ ] `ResolvedJoinKey` join gate; execution stores only key ordinals.
- [ ] `BindingError::NominalTypeMismatch`.
- [ ] Rune `.as_logical_type::<T>()` via `Params` (no string/parser).
- [ ] 13 fixtures green (incl. one-side-rejects / both-side-binds / literal-preserved /
      pure-join / all-naked-green).
- [ ] 10 `adr087_*` guards green; full package (distributed) + clippy clean.
