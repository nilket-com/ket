# Impl Plan — Toron ADR 235: Set Operations

Pairs with `plans/adr/0235_set_operations.md`. One new primitive — binary concat `LogicalPlan::Union`
(UNION ALL) + a physical concat node — plus a set-op-private null-safe row equality. Concat forwards
all rows from both inputs; absent a top-level `ORDER BY`, it must not promise result order even if the
local executor drains the left input before the right. Everything else
desugars onto the arc: `UNION` = `Distinct(concat)` [231/232]; `INTERSECT`/`EXCEPT`/`MINUS` =
`Distinct(LeftSemi/LeftAnti join)` [121+]. Scope = five operators DISTINCT-form + UNION ALL;
INTERSECT ALL/EXCEPT ALL/MINUS ALL + BY NAME deferred.

## Touch surface

- `sql_planner.rs` — recursive `SetExpr::SetOperation` lowering in `lower_query` (3220; replaces the
  :3260 catch-all for set-op bodies): recurse left/right, reconcile schema, dispatch
  `op`×`set_quantifier` to the D2/D3 desugars; `Minus`→normalize to `Except`; `*ByName`→named
  unsupported; `All` accepted only for `Union`. Build INTERSECT/EXCEPT via `build_join_plan` (:5115)
  with `LeftSemi`/`LeftAnti` and a null-safe all-columns ON. Keep `apply_order_by_limit_tail` after.
- `ir.rs` — add `LogicalPlan::Union { left, right }` (concat); the null-safe equality carrier
  (private join-key mode preferred; private bound `IsNotDistinctFrom` acceptable fallback).
- `binder.rs` — bind `Union` (ordinal schema reconcile via the `case_common_type` lattice :4098, with
  set-op-specific diagnostics rather than CASE wording; arm casts; left names; nullability = union of
  arms); bind null-safe set-op join keys; **add a `Union` arm to
  `distinct_input_unsupported_message` (:2560) that recurses both children**; arity/type-mismatch errors.
- `exec.rs` — physical concat (forward all rows from both inputs, normalized to the reconciled schema;
  no order guarantee without `ORDER BY`); null-safe key compare in the join probe (NULL==NULL only
  under the set-op flag; partition hash already null-encodes, :3866).
- `distributed/fragment.rs` — fragment concat as a two-child forwarding node; desugared
  `Distinct`/`Join` reuse 232/233 shuffles. If too wide, an explicit distributed fence + guard (D8).
- `tests/set_op_test.rs` (new), `tests/distributed_process_test.rs`, `tests/architecture/*`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Set-op body reject / insertion | `lower_query` `SetExpr::Select` (sql_planner.rs:3259), catch-all (:3260) |
| AST | `SetExpr::SetOperation { op, set_quantifier, left, right }` (sqlparser 0.62) |
| New concat IR | `LogicalPlan` enum (ir.rs:939) |
| Semi/anti join | `JoinKind::{LeftSemi,LeftAnti}` (ir.rs:875-877); `build_join_plan` (:5115) |
| Dedup reuse | `LogicalPlan::Distinct` (ir.rs:962), `wrap_distinct_if_needed` (:42) |
| DISTINCT binder fence (needs Union arm) | `distinct_input_unsupported_message` (binder.rs:2560) |
| Type unification | `case_common_type` (binder.rs:4098) |
| NULL-safe partitioning (already works) | `partition_key_hash` null-encode (exec.rs:3866-3882) |
| Tail | `apply_order_by_limit_tail` (after the body in `lower_query`) |

## Phase 0 — Red discriminators

`intersect_except_are_null_equal` (cardinal); `ordinary_join_stays_null_unequal` (scoping guard);
`bare_union_dedups_union_all_does_not`; `intersect_keeps_only_common_rows`;
`except_minus_removes_right_rows`; `union_distinct_dedups_across_partitions` (distributed). Red on
`main`. **The NULL tests are promotion blockers** — failing them means the desugar is semantically
wrong.

## Phase 1 — UNION / UNION ALL (the new primitive)

1. `LogicalPlan::Union { left, right }` + physical concat (forward all rows from both inputs;
   reconciled schema; no order guarantee without `ORDER BY`).
2. Bind: arity check; `case_common_type` lattice per column with set-op-specific diagnostics; insert
   arm projection casts; left names; nullability = union of arms. Add the `Union` arm to
   `distinct_input_unsupported_message`.
3. `lower_query`: `Union`+`All` → `Union(l,r)`; `Union`+`None`/`Distinct` → `Distinct(Union(l,r))`.
4. Fragmenter: concat forwards both children; UNION's `Distinct` rides 232.

## Phase 2 — INTERSECT / EXCEPT / MINUS (null-safe desugar)

1. Add the null-safe equality carrier (join-key mode preferred). Set-op ON = conjunction of per-column
   null-safe equalities over all output columns.
2. `lower_query`: `Intersect` → `Distinct(build_join_plan(l, r, LeftSemi, on))`; `Except`/`Minus` →
   `Distinct(build_join_plan(l, r, LeftAnti, on))`. `Minus` normalizes to `Except`.
3. Reject `Intersect`/`Except`/`Minus` + `All` and all `*ByName` with sharp messages.
4. Exec probe: NULL==NULL only under the set-op flag; ordinary joins unchanged.

## Phase 3 — Distributed + tail

Concat fragments as a two-child forwarder; INTERSECT/EXCEPT ride distributed join placement with the
null-safe mode in the plan; UNION rides 232 dedup. Confirm `apply_order_by_limit_tail` wraps a set-op
body. If distributed concat can't land cleanly → explicit fence + guard (no hidden coordinator path).

## Phase 4 — Behavior coverage

- UNION (dedup) vs UNION ALL; INTERSECT/EXCEPT/MINUS basic; **NULL-equal** cardinal;
  **ordinary-join-stays-null-unequal**; type unification (Int32 ∪ Int64); arity mismatch + unreconcilable
  type reject; nested chains (`a UNION b INTERSECT c` — INTERSECT binds tighter); ORDER BY/LIMIT tail.
- **Distributed:** UNION-distinct dedups across workers (rides 232); INTERSECT null-safe matching
  across partitions — fixture must produce a shared row from >1 producer (233 discipline), assert >1
  worker.
- Deferred fences: INTERSECT ALL / EXCEPT ALL / MINUS ALL / UNION BY NAME reject by message.

## Phase 5 — Guards

Add the seven `adr235_*` guards (esp. `..._is_null_safe_and_ordinary_joins_stay_null_unequal` and
`..._union_all_is_the_only_new_set_op_primitive`). Retarget the `unsupported query body` catch-all
guard (set-op bodies lower; others reject). Assert 231/232 Distinct + 121+ join guards reused; null-safe
equality additive; no optimizer/Wendelon surface.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test set_op_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test set
scripts/cargo-cgroup test  -p toron --test architecture_test adr235
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — retargets participate
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. NULL mishandled — `intersect_except_are_null_equal` fails under a plain-`=` semi/anti join.
2. Null-safe mode leaked to ordinary joins — `ordinary_join_stays_null_unequal` fails.
3. UNION doesn't dedup — `bare_union_dedups_union_all_does_not` fails.
4. A new `Intersect`/`Except` IR variant instead of desugar — `..._only_new_set_op_primitive` fails.
5. MINUS diverges from EXCEPT — `adr235_minus_aliases_except` / digest fails.
6. ALL-multiplicity / BY NAME silently accepted — deferred-fence tests fail.
7. Distributed set-op matching wrong across partitions — distributed gate fails (fixture must split a
   shared row across producers, else it passes for the wrong reason — the 233 lesson).
8. `Distinct(Union)` rejected by the binder fence — UNION tests fail (the D9 arm is missing).

## Hygiene

- No m-word; no version-suffix idiom; one new physical op (concat) — guarded as the *only* new set-op
  primitive; null-safe equality additive + set-op-private; goldens limited to set-op result rows.
- `--all-targets`; cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Null-safe equality home: confirm the join-key path can carry a private null-safe flag (preferred);
   else the private bound `IsNotDistinctFrom` fallback — and guard that ordinary `=` joins stay
   NULL-unequal.
2. Confirm `case_common_type` covers the set-op type lattice (or name the gap); arm casts inserted;
   nullability = union of arms; names from left.
3. Confirm the new `Union` arm in `distinct_input_unsupported_message` (and any other exhaustive
   `LogicalPlan` walkers) compiles + recurses both children.
4. Confirm the distributed INTERSECT fixture produces a shared row from >1 producer before the match
   (233 discipline) — else the cardinal passes for the wrong reason.
5. Decide distributed concat support vs explicit fence (D8) — never a hidden coordinator-only path.
6. Confirm set-op-as-derived/EXISTS-subquery is supported or an explicit named deferral.
