# Impl Plan 074: Predicate Widening — `IN` + `BETWEEN`

**Status:** Promoted, paired with `plans/adr/074_predicate_widening_in_between.md`.
**Posture:** Widen WHERE predicates to **first-class `IN`** (→ native
`OP_IN_LIST`) + **`BETWEEN` desugar** (no IR). **Toron-bounded** — the
`OP_IN_LIST` opcode + its Wendelon execution already exist; 074 adds no Wendelon
/ wire / storage / audit change. `LIKE`/`ILIKE` deferred to ADR 075 (with the
partial-pushdown residual-split). Five phases.

## Verified anchors

- **IN opcode (exists):** `wendelon/src/schema/pbc.rs:66` `OP_IN_LIST = 0x30` +
  `Op::InList(count)` + `predicate_from_in_list` — Wendelon already executes it.
  Toron `pbc.rs:7` already imports Wendelon PBC byte constants; add `OP_IN_LIST`
  there and emit bytes, not Wendelon's private `Op` enum.
- **All-or-nothing binding (the design driver):** `lower_where`
  (`sql_planner.rs:2778`) combines conjuncts into one `Filter`; binder
  `compile_pbc(&bound_predicate)` ⇒ `FullyPushed | EvaluateInToron`
  (`binder.rs:105-116`), no partial pushdown.
- **lower_expr:** `sql_planner.rs:2922` (catch-all `:3028` rejects InList/Between).
  **Expr eval:** `exec.rs:807+`. Existing equality + Kleene-`or` kernels already
  define residual parity (`exec.rs:24-25`); Arrow `in_list`/`in_list_utf8` is only
  an optional fast path if it proves identical. **NULL literal reject:** `:2943`.
- **PBC stack bound:** Wendelon `STACK_DEPTH_LIMIT = 64`; `OP_IN_LIST(count)`
  consumes a target plus `count` live member values, so Toron caps
  `MAX_IN_LIST_ITEMS` at **32** to leave headroom for mixed predicates.
- **ADR-014 guard to retarget:** `adr014_does_not_add_sql_in_lowering_to_toron_
  planner` (`architecture_test.rs:470`).

## Invariants carried in

- **No Wendelon change** (opcode + execution pre-exist). `IN`
  first-class → native opcode (not an OR-tree). `BETWEEN` desugars (no IR/opcode/
  exec arm). `LIKE`/`ILIKE`/`CASE`/subquery-`IN`/`ALTER` rejected. NULL literals
  stay rejected. Parser accounting (name InList/Between; reject neighbors);
  sqlparser stays in `sql_planner.rs`. No SQL-wire/worker-wire/storage/audit
  change; no new dependency.

## Phase 1 — Logical IR (`ir.rs`)

- Add `Expr::InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool }`. **No
  `Expr::Between`** (desugars). Update `is_pbc_pushdown_eligible`: `InList` ⇒
  eligible iff `expr` + all members eligible. Update exhaustive `Expr` visitors.

## Phase 2 — Planner (`sql_planner.rs` `lower_expr`)

- `SqlExpr::InList { expr, list, negated, .. }` → `Expr::InList` (reject empty /
  `> MAX_IN_LIST_ITEMS=32` / `IN (SELECT…)` / `UNNEST` / row-value / NULL
  members; members via existing `lower_expr`).
- `SqlExpr::Between { expr, low, high, negated }` → **desugar** `And(Gte,Lte)` /
  `Or(Lt,Gt)` over `lower_expr`-ed parts (no new IR).
- **Reject** `SqlExpr::{Like, ILike, SimilarTo, RLike, InSubquery, InUnnest,
  Case}` and PostgreSQL `~~` / `~~*` binary-operator spellings explicitly (no
  wildcard accept).

## Phase 3 — Binder (`ir/execution.rs`, `binder.rs`)

- `BoundExprNode::InList { expr, list, negated }` + builder, validation,
  `pretty_node`, child-walk, nullable propagation, offset/remap. `bind_in_list`:
  type-check target vs each member (existing comparison coercion lattice; bounded
  errors).

## Phase 4 — Exec + PBC

- **`exec.rs`** (residual path): `InList` folds existing equality + Kleene-`or`
  kernels over members; `negated` wraps with existing boolean `not`. Arrow
  `in_list`/`in_list_utf8` is allowed only as a tested fast path with **exact
  parity** to `x = a OR x = b` and the PBC path, including nullable target/member
  columns.
- **`pbc.rs`**: compile `BoundExprNode::InList` by pushing **target first, then
  members**, matching Wendelon's existing `OP_IN_LIST` consumer (`count` top stack
  entries are items; the next entry is the target). Emit the **existing**
  `OP_IN_LIST` byte plus little-endian u16 count; `negated` wraps with existing
  `OP_NOT`. Confirm the max list pushes without tripping bytecode or stack limits.

## Phase 5 — Guards, fixtures, evidence

- 12 + meta guards (incl. **in-list-compiles-to-existing-OP_IN_LIST**,
  **adds-no-new-wendelon-opcode**, negation-uses-existing-not, residual-eval,
  pushdown-eligible-iff-members, in-list-bound, between-desugars-no-IR,
  reject-neighbors, alter-unsupported, **retargets-adr014-sql-in-absence-guard**,
  no-cross-ring, sqlparser-quarantined, prior-guards-pass).
- 13 fixtures (ADR list) — anchored by `where_in_list_pushes_op_in_list_to_
  wendelon`, `in_list_at_max_items_pushes_without_pbc_limit_error` (opcode-vs-
  OR-tree win at the 32-item stack-safe bound),
  `where_not_in_list_pushes_op_in_list_plus_not`,
  `between_pbc_matches_equivalent_and_chain`, `like_ilike_case_subquery_in_remain_
  unsupported`.
- `plans/impl/074_predicate_widening_in_between_evidence.md`: `architecture_test
  adr074` + full (incl. retargeted adr014 + the prior SQL guards green);
  `cargo test -p toron` (predicate + pbc-pushdown + planner/binder/exec suites);
  the existing WHERE/filter corpus green; `cargo check -p toron` feature matrix;
  `clippy -D warnings`; `git diff --check`. Include a **PBC-bytecode snapshot**
  showing `IN` → `OP_IN_LIST` (not an OR-tree).

## Watch-items

1. **`IN` emits the native `OP_IN_LIST`** (compact), not a desugared OR-tree — the
   point: robust pushdown at the list bound under the all-or-nothing binder.
2. **No Wendelon change** — opcode + execution pre-exist; assert via the
   no-new-opcode + no-cross-ring guards.
3. **Residual `IN` parity** — `exec.rs` `InList` must match `x=a OR x=b` and the
   PBC path exactly; Arrow `in_list` is an optimization only after parity proof.
4. **Reject the neighbors** — `LIKE`/`ILIKE`/`SIMILAR`/`RLIKE`/subquery-`IN`/`CASE`/
   `ALTER` explicitly unsupported (no wildcard accept); `LIKE` defer is intentional
   (residual-split is ADR 075).
5. **ADR-014 retarget** — the no-SQL-`IN` guard now allows list-`IN`; subquery-`IN`
   stays unsupported.
6. **NULL literals stay rejected** — `IN (…,NULL)` / `BETWEEN NULL` reject via the
   existing path.
7. **PBC stack order and bound** — emit target before members; keep the static
   list cap at 32 unless Wendelon changes `STACK_DEPTH_LIMIT` or the opcode
   contract.

## Acceptance checklist

- [ ] Phase 1: `Expr::InList` (no `Between`); pushdown-eligibility (members).
- [ ] Phase 2: lower_expr InList / Between(desugar); reject neighbors + 32-item
      bound.
- [ ] Phase 3: `BoundExprNode::InList` + `bind_in_list` (type checks).
- [ ] Phase 4: exec parity (folded `eq`/`or`, optional proven Arrow fast path);
      pbc emits target-then-members `OP_IN_LIST`; negation via `OP_NOT`; bound ≤
      opcode/program/stack limits.
- [ ] Phase 5: 12 guards + 13 fixtures (incl. OP_IN_LIST snapshot) + evidence;
      adr014 retargeted; prior SQL corpus green; no Wendelon/wire change.
