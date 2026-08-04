# Impl Plan 083: Rune Predicate Operator Surface

**Status:** Promoted. Paired with `plans/adr/0083_rune_predicate_operator_surface.md`.
Genuine 3-of-3. Builds on the landed ADR 082 feat. **Posture:** pure additive surface
in `rune_engine.rs` — 9 `RuneExpr` methods + registration; tests + guards carry the rest.

## Invariants

- Lower to existing `Expr::{Lt,Lte,Gte,Neq,And,Or,Not,IsNull,IsNotNull}`. No new IR.
- Keep `lit*`, `.gt`/`.eq`, `.named`, `.project`/`.project_named`, aggregate helpers,
  079–082 behavior intact. **No method named `not`** (Rune keyword). No arithmetic.
- No change outside `rune_engine.rs` + tests; no shim; no Rune-local evaluation.

## Phase 0 — tests first (`tests/rune_relation_handles_test.rs`)

Reuse ADR 082's `sales` fixture (incl. `price: Float64`, `active: Boolean`).
Execution fixtures use `rune_plan`/`run_logical`; the pushdown fixture binds the
same predicate and inspects `FilterDisposition`. Ids are hand-verified under Kleene
3VL.

- `rune_predicate_operator_compound_filter_executes` (gate):
  ```rune
  ctx.table("sales").filter(
      col("id").lt(lit_i64(4))
          .and(col("amount").gte(lit(10))).and(col("amount").lte(lit(20)))
          .or(col("status").ne(lit_str("open")).and(col("price").is_null()))
          .and(col("region").is_not_null()))
  ```
  → ids `[1, 2, 3]`. (Exercises `.lt/.gte/.lte/.ne/.and/.or/.is_null/.is_not_null`;
  load-bearing 3VL: `NULL OR TRUE = TRUE` at id3; rows with NULL amount do not pass
  unless another branch is true, so id5 remains excluded.)
- `rune_logical_not_executes_against_boolean_column` —
  `filter(col("active").logical_not())` → ids `[2, 5]`.
- `rune_null_predicate_executes_against_nullable_column` —
  `filter(col("price").is_null())` → id `[3]`.
- `rune_predicate_operators_preserve_pushdown_path` — bind the same compound gate
  predicate; assert `FilterDisposition::FullyPushed` + `scan_pbc(scan).is_some()`
  (reuse 079's `find_filter`/`scan_pbc`). This is grounded in the existing PBC
  compiler/opcode support for comparisons, booleans, null predicates, string/int
  literals, and PBC-eligible fixture columns; do not weaken to a one-operator range
  unless implementation disproves that grounding.

## Phase 1 — production methods (`rune_engine.rs`, in `impl RuneExpr` beside `gt`/`eq` `:362-368`)

```rust
pub fn lt(self, other: RuneExpr)  -> RuneExpr { RuneExpr { expr: Expr::Lt(Box::new(self.expr), Box::new(other.expr)) } }
pub fn lte(self, other: RuneExpr) -> RuneExpr { RuneExpr { expr: Expr::Lte(Box::new(self.expr), Box::new(other.expr)) } }
pub fn gte(self, other: RuneExpr) -> RuneExpr { RuneExpr { expr: Expr::Gte(Box::new(self.expr), Box::new(other.expr)) } }
pub fn ne(self, other: RuneExpr)  -> RuneExpr { RuneExpr { expr: Expr::Neq(Box::new(self.expr), Box::new(other.expr)) } }
pub fn and(self, other: RuneExpr) -> RuneExpr { RuneExpr { expr: Expr::And(Box::new(self.expr), Box::new(other.expr)) } }
pub fn or(self, other: RuneExpr)  -> RuneExpr { RuneExpr { expr: Expr::Or(Box::new(self.expr), Box::new(other.expr)) } }
pub fn logical_not(self) -> RuneExpr { RuneExpr { expr: Expr::Not(Box::new(self.expr)) } }
pub fn is_null(self)     -> RuneExpr { RuneExpr { expr: Expr::IsNull(Box::new(self.expr)) } }
pub fn is_not_null(self) -> RuneExpr { RuneExpr { expr: Expr::IsNotNull(Box::new(self.expr)) } }
```

Register after `eq` (`:436`):
```rust
m.associated_function("lt", RuneExpr::lt)?;
m.associated_function("lte", RuneExpr::lte)?;
m.associated_function("gte", RuneExpr::gte)?;
m.associated_function("ne", RuneExpr::ne)?;
m.associated_function("and", RuneExpr::and)?;
m.associated_function("or", RuneExpr::or)?;
m.associated_function("logical_not", RuneExpr::logical_not)?;
m.associated_function("is_null", RuneExpr::is_null)?;
m.associated_function("is_not_null", RuneExpr::is_not_null)?;
```

**Do not** add a `.not` method (Rune keyword) or arithmetic.

## Phase 2 — guards (`tests/architecture/rune_frontend.rs`)

- **Retarget `adr082_typed_literal_cut_does_not_expand_operator_surface`**
  (discipline #4): move `lt/lte/gte/ne/and/or/logical_not/is_null/is_not_null` to the
  **required** set; keep `not, add, sub, mul, div, like, ilike, in_list, cast,
  try_cast` **forbidden**. Keep the `adr082_` provenance prefix with a comment, or
  rename to `adr083_…operator_scope…` — council call (I lean keep + comment).
- `adr083_rune_predicate_operators_lower_to_existing_expr_variants` — each method
  body contains its `Expr::{Lt|Lte|Gte|Neq|And|Or|Not|IsNull|IsNotNull}`.
- `adr083_rune_predicate_operators_are_registered` — all 9 registered; assert `"not"`
  is **not** a registered name.
- `adr083_logical_not_avoids_keyword_collision_with_not` — composes
  `adr081_authored_rune_methods_avoid_reserved_keywords`.
- `adr083_predicate_operator_cut_does_not_change_engine_substrate` — no
  `BindingError`/`BoundExpr`/`ExecutionPlan`/`normalize_`/`evaluate_expr` in
  `rune_engine.rs`; `binder.rs`/`exec.rs`/`ir.rs` not in the staged set.

## Verification

```
cargo fmt
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test architecture_test adr083_
cargo test -p toron --test architecture_test adr082_   # retargeted guard green
cargo test -p toron --test architecture_test adr081_
cargo test -p toron --test rune_sandbox_test
cargo test -p toron --test group_by_test
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron
```

## Evidence doc (`plans/impl/0083_rune_predicate_operators_evidence.md`)

Record: `rune_engine.rs` + two test files as only touched; the 4 fixture results
(`[1,2,3]`/compound FullyPushed/`[2,5]`/`[3]`); the
retargeted 082 guard; confirmation `binder.rs`/`exec.rs`/`ir.rs` unchanged and no
`.not`/arithmetic registered; command counts.

## Watch items

1. **`not` must never be registered** — the keyword guard backstops it; `logical_not`
   is the only negation surface.
2. **3VL correctness** — assert exact ids (the `NULL OR TRUE = TRUE` at id3 and
   id5 staying excluded despite NULL amount are the load-bearing Kleene cases), not
   just counts.
3. **Pushdown claim** — the compound gate is the intended FullyPushed assertion; if it
   fails, treat that as an implementation finding and either fix the latent PBC path
   or explicitly record the narrower fallback.

## Acceptance checklist

- [ ] 9 predicate methods exist, lower to existing `Expr`; no `.not`, no arithmetic.
- [ ] Registered as `RuneExpr` associated functions.
- [ ] 4 fixtures green (`[1,2,3]`/compound FullyPushed/`[2,5]`/`[3]`).
- [ ] 082 scope guard retargeted (predicates required; arithmetic still forbidden).
- [ ] No production change outside `rune_engine.rs`; 079–082 guards intact; full package + clippy clean.
