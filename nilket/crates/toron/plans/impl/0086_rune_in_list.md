# Impl Plan 086: Rune InList Membership Surface

**Status:** Promoted. Paired with `plans/adr/0086_rune_in_list.md`. Genuine 3-of-3.
Builds on the landed ADR 085 feat. **Posture:** pure additive surface in
`rune_engine.rs` — 2 `VmResult`-returning `RuneExpr` methods + a shared helper +
a length constant + registration; lowers to existing `Expr::InList` (ADR 074).

## Invariants

- Lower to existing `Expr::InList { expr, list, negated }`. No new IR/binder/executor/
  PBC opcode/SQL grammar.
- **Re-validate the ADR 074 frontend bound in Rune** (planner `lower_in_list` bypassed):
  non-empty + `<= 32`, SQL-parity messages.
- Keep 079–085 behavior intact. No change outside `rune_engine.rs` + tests.

## Phase 0 — tests first (`tests/rune_relation_handles_test.rs`)

Reuse the 082–085 `sales` fixture (`id: Int64`, `status: Utf8`, `region: Utf8 nullable`,
`amount: Int32`). All bind/execute; ids hand-verified.

- `rune_in_list_pushes_to_scan_pbc` (gate) — `filter(col("id").in_list([lit_i64(1),
  lit_i64(2)]))` → ids `[1,2]`; `FullyPushed`; `scan_pbc(scan).is_some()`; `OP_IN_LIST`.
- `rune_not_in_list_pushes_to_scan_pbc` — `filter(col("status").not_in_list([lit_str("open")]))`
  → ids `[3]`; `FullyPushed`; `OP_IN_LIST` + `OP_NOT`.
- `rune_not_in_list_excludes_null_target` —
  `filter(col("region").not_in_list([lit_str("east")]))` → ids `[2,5]` (NULL excluded).
- `rune_in_list_literal_coercion_executes` — `col("id").in_list([lit(1), lit_i64(2)])`
  → ids `[1,2]`.
- `rune_in_list_computed_member_is_residual` (type-correct) —
  `col("id").in_list([col("amount").sub(lit_i64(9)), col("amount").sub(lit_i64(18))])`
  → ids `[1,2]`; `EvaluateInToron`, no scan PBC.
- `rune_in_list_empty_list_panics` — `.in_list([])` → "IN list must not be empty".
- `rune_in_list_over_limit_panics` — 33 members → "IN list exceeds 32 items".
- `rune_in_list_mismatch_is_binding_error` — `col("status").in_list([lit(1)])` →
  existing `BindingError` containing "IN operands".

## Phase 1 — production (`rune_engine.rs`)

```rust
const RUNE_MAX_IN_LIST_ITEMS: usize = 32; // parity with SqlPlanner MAX_IN_LIST_ITEMS

impl RuneExpr {
    pub fn in_list(self, list: Vec<RuneExpr>) -> VmResult<RuneExpr> {
        build_in_list(self.expr, list, false)
    }
    pub fn not_in_list(self, list: Vec<RuneExpr>) -> VmResult<RuneExpr> {
        build_in_list(self.expr, list, true)
    }
}

fn build_in_list(expr: Expr, list: Vec<RuneExpr>, negated: bool) -> VmResult<RuneExpr> {
    if list.is_empty() {
        return VmResult::panic("IN list must not be empty");
    }
    if list.len() > RUNE_MAX_IN_LIST_ITEMS {
        return VmResult::panic(format!("IN list exceeds {RUNE_MAX_IN_LIST_ITEMS} items"));
    }
    VmResult::Ok(RuneExpr {
        expr: Expr::InList {
            expr: Box::new(expr),
            list: list.into_iter().map(|e| e.expr).collect(),
            negated,
        },
    })
}
```

Register: `m.associated_function("in_list", RuneExpr::in_list)?;` +
`m.associated_function("not_in_list", RuneExpr::not_in_list)?;`. (`VmResult` return —
`ctx.sql` precedent.) No exact exported `in`/`not` names; native negation, not
`.logical_not`.

## Phase 2 — guards (`tests/architecture/rune_frontend.rs`)

- **Retarget `adr082_typed_literal_cut_does_not_expand_operator_surface`**:
  `in_list`/`not_in_list` → **required**; keep `like`, `ilike`, exact bare `not`, and
  `safe_cast` forbidden.
- `adr086_rune_in_list_methods_lower_to_existing_expr_variant` — `in_list`→`negated:false`,
  `not_in_list`→`negated:true`, via `build_in_list` → `Expr::InList`.
- `adr086_rune_in_list_methods_validate_frontend_bounds` — `RUNE_MAX_IN_LIST_ITEMS: usize = 32`
  + the empty/over-limit `VmResult::panic`s.
- `adr086_rune_in_list_authoring_bounds_match_sql_planner` — read **both**
  `rune_engine.rs` (`RUNE_MAX_IN_LIST_ITEMS`) and `sql_planner.rs` (`MAX_IN_LIST_ITEMS`),
  assert both `= 32` (no drift).
- `adr086_rune_in_list_methods_are_registered` — both registered; no exact exported
  `in`/`not` names.
- `adr086_rune_in_list_pushes_when_pbc_eligible` — `FullyPushed` + `OP_IN_LIST`.
- `adr086_rune_in_list_reuses_existing_substrate` — no
  `BindingError`/`BoundExpr`/`ExecutionPlan`/`normalize_`/new-opcode markers in
  `rune_engine.rs`.

## Verification

```
cargo fmt
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test architecture_test adr086_
cargo test -p toron --test architecture_test adr082_   # retargeted scope guard green
cargo test -p toron --test predicate_widening_test     # SQL IN/NOT IN net intact
cargo test -p toron --test group_by_test
cargo test -p toron --test rune_sandbox_test
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron
```

## Evidence doc (`plans/impl/0086_rune_in_list_evidence.md`)

Record: `rune_engine.rs` + two test files as only touched; the 8 fixture results
(pushed `[1,2]`+`OP_IN_LIST` / `[3]`+`OP_NOT` / NULL-3VL `[2,5]` / coercion `[1,2]` /
computed residual `[1,2]` / empty + over-32 + type-mismatch rejects); the
`RUNE_MAX_IN_LIST_ITEMS == MAX_IN_LIST_ITEMS` parity; confirmation no
engine/binder/PBC/SQL-grammar change; command counts.

## Watch items

1. **Two parity guards** — `validate_frontend_bounds` (panics + local constant) and
   `authoring_bounds_match_sql_planner` (cross-file equality). The second catches a
   future SQL-bound bump leaving Rune behind.
2. **Native negation** — `not_in_list` uses `negated: true` (pushes `OP_IN_LIST`+`OP_NOT`).
3. **NOT IN + NULL (3VL)** — assert the NULL-`region` row excluded (`[2,5]`).
4. **Computed target/member → residual** — type-correct member fixture (`lit_i64`
   members); assert `EvaluateInToron`.
5. **`VmResult` method** — empty/over-limit surface as bounded frontend errors, not
   host crashes.

## Acceptance checklist

- [ ] `.in_list`/`.not_in_list` (`VmResult`) → existing `Expr::InList`.
- [ ] In-Rune non-empty + `<= 32` validation; `RUNE_MAX_IN_LIST_ITEMS == MAX_IN_LIST_ITEMS`
      guard reads both source files.
- [ ] 8 fixtures green (pushed `[1,2]`/`[3]`, NULL-3VL `[2,5]`, coercion, computed
      residual, empty/over-32/type-mismatch rejects).
- [ ] in_list pushes (`FullyPushed` + `OP_IN_LIST`); computed target/member residual.
- [ ] Scope guard retargeted (in_list/not_in_list required; like/ilike/exact bare
      not/safe_cast forbidden).
- [ ] No production change outside `rune_engine.rs`; 079–085 guards intact; full
      package + clippy clean.
