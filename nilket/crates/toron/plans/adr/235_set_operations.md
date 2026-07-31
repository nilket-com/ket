# Toron ADR 235: Set Operations (UNION / UNION ALL / INTERSECT / EXCEPT / MINUS)

**Status:** Promoted. **Migration:** `plans/impl/235_set_operations.md`.
**Category:** Toron core query surface — SQL query composition. **One new IR/physical primitive
(binary concat = `UNION ALL`) plus a set-op-private null-safe row equality; everything else reuses the
arc:** `UNION` = `Distinct`(concat) [231/232]; `INTERSECT`/`EXCEPT`/`MINUS` = `Distinct`(semi/anti
`Join`) [121+]. No new optimizer-evidence surface, durable key, system table, Rune-only surface, or
Wendelon change.

## Why this cut exists

After the DISTINCT/aggregate arc (231–234), set operations open the next family — query composition.
`SetExpr::SetOperation` dies today on the `unsupported query body` catch-all (sql_planner.rs:3260); no
set-op IR exists. The family decomposes almost entirely onto machinery the arc already built:
concatenation is the one new node, dedup is `LogicalPlan::Distinct`, membership is the existing
`LeftSemi`/`LeftAnti` joins.

## Grounding (verified)

- **AST (sqlparser 0.62):** `SetExpr::SetOperation { op, set_quantifier, left, right }`; `op ∈ {Union,
  Except, Intersect, Minus}` (**MINUS is its own operator, semantically == EXCEPT**); `set_quantifier
  ∈ {All, Distinct, ByName, AllByName, DistinctByName, None}`. `None` on
  UNION/INTERSECT/EXCEPT/MINUS = **DISTINCT** (SQL default). `*ByName` = DuckDB `UNION BY NAME`
  (out of scope).
- **No set-op machinery.** Only `SetExpr::Select`/`Values` lower in `lower_query` (sql_planner.rs:3220,
  catch-all :3260). No `Union`/`Intersect`/`Except` in `ir.rs` / `ir/execution.rs`.
- **Semi/anti joins exist.** `JoinKind::{LeftSemi, LeftAnti}` (ir.rs:875-877), built via
  `build_join_plan` (:5115); already used by EXISTS decorrelation. INTERSECT/EXCEPT reuse them.
- **NULL trap — set ops are NULL-equal; there is NO null-safe equality.** `IS NOT DISTINCT FROM` /
  null-safe join keys are absent. Join `=` is NULL-*unequal* (right for joins, wrong for set ops):
  `(NULL) INTERSECT (NULL)` must return `(NULL)`; a plain-`=` semi-join returns empty. **Cardinal pin.**
  `UNION` dedup via `Distinct` is already NULL-equal (correct, free).
- **Distributed null partitioning already works.** `partition_key_hash` null-encodes (exec.rs:3866) →
  NULL keys hash consistently; only the join *probe* compare must learn NULL==NULL.
- **DISTINCT binder fence is an exhaustive walk.** `distinct_input_unsupported_message` (binder.rs:2560)
  `match`es every `LogicalPlan` variant; the new `Union` node must get a both-children-recursing arm
  (like `Join`), else `UNION = Distinct(Union)` fails to compile/bind. (Verified.)
- **Type unification + tail exist to reuse.** `case_common_type` (binder.rs:4098); `lower_query`
  applies `apply_order_by_limit_tail` after the body, so `(a UNION b) ORDER BY 1 LIMIT k` composes.

## Decisions (promotion-locked)

**D1 — One new primitive: binary concat `LogicalPlan::Union { left, right }` (UNION ALL)** + a matching
physical concat node (forward all rows from both inputs under the reconciled schema). No
`Intersect`/`Except` IR variant — they desugar. Nested set ops recurse (AST is binary). The physical
node may be variadic for future multi-way concat, but the logical node is binary. This is not an
ordering contract: absent a top-level `ORDER BY`, set-op result order remains unspecified even if the
local executor happens to drain the left input before the right.

**D2 — `UNION ALL` → `Union`; bare `UNION` defaults to DISTINCT.** `All` → `Union(l, r)`; `None`/
`Distinct` → `Distinct(Union(l, r))` (reuses 231/232 distributed dedup, NULL-equal). The SQL default
(UNION dedups; only UNION ALL doesn't) is the second trap, pinned in tests.

**D3 — `INTERSECT`/`EXCEPT`/`MINUS` are DISTINCT-form join desugars.**
`INTERSECT` → `Distinct(LeftSemi(l, r, on))`; `EXCEPT`/`MINUS` → `Distinct(LeftAnti(l, r, on))`, where
`on` is null-safe equality over **all** output columns. `MINUS` normalizes to `Except` before
lowering. No set-op-specific physical membership operator.

**D4 — Cardinal pin: set-op row matching is NULL-equal, and it stays set-op-private.** The `on` is a
per-ordinal-column null-safe equality (`IS NOT DISTINCT FROM`). **Ordinary joins keep `=` NULL-unequal
semantics, guarded.** Representation is an implementation choice, both kept private to set-op lowering
(must not publish a general scalar surface): *preferred* a null-safe comparison mode on the
set-op-generated join keys (Codex); *acceptable* a private bound `IsNotDistinctFrom` operator
(Gemini). Distributed shuffle is already null-consistent (exec.rs:3866); only the probe compare changes.

**D5 — Schema reconciliation is ordinal.** Equal column count (else explicit error); types unify via
`case_common_type` (reject pairs outside that lattice — no new set-op coercion lattice); insert arm
projection casts so downstream sees one schema; output **names** from the left arm; output
**nullability** = the union of arm nullabilities (a column is nullable if either arm is). Because
`case_common_type` is currently a CASE helper, the implementation should reuse the lattice but surface
set-op-specific diagnostics instead of leaking CASE wording into set-op errors.

**D6 — Scope = the five operators in DISTINCT form + UNION ALL.** In: `UNION`, `UNION DISTINCT`,
`UNION ALL`, `INTERSECT [DISTINCT]`, `EXCEPT [DISTINCT]`, `MINUS [DISTINCT]`. Deferred (genuine
mechanism boundary): **`INTERSECT ALL` / `EXCEPT ALL` / `MINUS ALL`** (row-multiplicity expansion —
a new kernel); **`BY NAME`** quantifiers (name-based alignment + missing-column policy); per-arm
parenthesized `ORDER BY`/`LIMIT`.

**D7 — Tail applies to the combined relation.** Top-level `ORDER BY`/`LIMIT`/`OFFSET` stay outside the
set-op body via `apply_order_by_limit_tail` — they sort/limit the combined result, not an arm.

**D8 — Distributed is in scope, not a hidden local fallback.** Concat fragments as a two-child
forwarding node; `UNION` rides 232's dedup shuffle; `INTERSECT`/`EXCEPT` ride existing distributed
join placement with the null-safe mode in the join plan. If concat fragmentation proves too wide to
land cleanly, retain an **explicit distributed-unsupported fence + guard** — never a hidden
coordinator-only path.

**D9 — Binder fence reconciliation.** Add a `Union` arm to `distinct_input_unsupported_message`
(binder.rs:2560) that recurses through both children, so `UNION = Distinct(Union)` is accepted.

**D10 — No new optimizer / durable / Wendelon / Rune surface.** Reuse dedup/join/exchange; concat adds
no optimizer-evidence.

## Semantics

```sql
-- in scope
SELECT a FROM t UNION     SELECT a FROM u          -- dedup (DISTINCT default)
SELECT a FROM t UNION ALL SELECT a FROM u          -- concat, no dedup
SELECT a FROM t INTERSECT SELECT a FROM u          -- Distinct(t LeftSemi u), null-safe
SELECT a FROM t EXCEPT    SELECT a FROM u          -- Distinct(t LeftAnti u), null-safe
SELECT a FROM t MINUS     SELECT a FROM u          -- == EXCEPT
(SELECT a FROM t UNION SELECT a FROM u) ORDER BY 1 LIMIT 5
-- still rejected (deferred)
SELECT a FROM t INTERSECT ALL SELECT a FROM u      -- multiplicity expansion
SELECT a FROM t UNION BY NAME SELECT a FROM u      -- by-name alignment
```

## Honesty gates (discriminating)

- **`intersect_except_are_null_equal`** — `(NULL) INTERSECT (NULL)` = `(NULL)`; `(NULL) EXCEPT (NULL)`
  = ∅. *Cardinal* (fails under a plain-`=` semi/anti join).
- **`ordinary_join_stays_null_unequal`** — a normal `a JOIN b ON a.k=b.k` with NULL keys produces no
  match (the null-safe mode did not leak). *Guards D4's scoping.*
- **`bare_union_dedups_union_all_does_not`** — `a UNION a` collapses, `a UNION ALL a` doubles (D2).
- `intersect_keeps_only_common_rows`; `except_minus_removes_right_rows` (MINUS == EXCEPT digest).
- `union_distinct_dedups_across_partitions` — distributed concat + cross-worker dedup (rides 232).
- `intersect_except_null_safe_matching_across_partitions` — distributed: a shared row produced by >1
  producer is matched (apply the 233 discipline — the fixture must split a shared row across workers,
  not pass for the wrong reason).
- `set_op_type_unification_uses_existing_common_type` (Int32 ∪ Int64 → Int64);
  `set_op_column_count_mismatch_rejects`; `set_op_unreconcilable_type_rejects`;
  `set_op_order_by_limit_tail_applies_after_combined_rows`.
- Deferred fences: `intersect_all_except_all_minus_all_are_named_unsupported`,
  `set_op_by_name_quantifiers_are_named_unsupported`.

## Architecture guards (promotion-locked)

- `adr235_union_all_is_the_only_new_set_op_primitive` (one logical `Union` + one physical concat; no
  `Intersect`/`Except` IR variant)
- `adr235_union_desugars_to_distinct_over_concat`
- `adr235_intersect_except_desugar_to_null_safe_semi_anti_join`
- `adr235_set_op_matching_is_null_safe_and_ordinary_joins_stay_null_unequal`
- `adr235_minus_aliases_except`
- `adr235_set_ops_defer_all_multiplicity_and_by_name`
- `adr235_set_ops_add_no_optimizer_or_wendelon_surface`

**Reconciled prior guards (named):**
- `unsupported query body` catch-all (sql_planner.rs:3260) → retargeted: `SetOperation` lowers;
  `Insert`/`Update`/`Table` bodies stay rejected.
- ADR 231/232 `Distinct` marker / ordinal-safe binder guards → reused (UNION dedup + set-op output).
  `distinct_input_unsupported_message` gains a `Union` arm (D9).
- ADR 121+ join guards / `LeftSemi`/`LeftAnti` → reused; null-safe equality is **additive and
  set-op-private** — existing `=` join semantics unchanged (guarded).

## Consensus (transparent)

3-of-3. The five-operator family was the operator's steer over Claude's iter-0 UNION-first lean.
Claude iter-1 dual proposed the one-new-primitive design and surfaced the INTERSECT/EXCEPT fork
(null-safe join vs group-by-counting). Codex iter-2 and Gemini iter-1 both **closed the fork on the
null-safe semi/anti-join route** and converged on every load-bearing point (concat-only new primitive,
UNION=Distinct(concat), MINUS==EXCEPT, NULL-equal cardinal, `case_common_type` reuse, DISTINCT-form
scope, distributed-or-fenced). Two minor divergences resolved with the grounded call: **null-safe
representation** — private join-key mode preferred (Codex), private bound `IsNotDistinctFrom` an
acceptable fallback (Gemini), both guarded so ordinary joins stay NULL-unequal; **output nullability**
— union of arm nullabilities (Codex; correct when one arm is nullable), names from the left.
Codex's load-bearing catch (`distinct_input_unsupported_message` must recurse the concat children) is
verified and locked as D9.

## Forward arc

```text
235 set operations (this) -> INTERSECT ALL / EXCEPT ALL (multiplicity kernel)
-> multi-dedup distinct aggregates (234 deferred) -> window functions
```

## Deferred (named)

`INTERSECT ALL` / `EXCEPT ALL` / `MINUS ALL` (row-multiplicity expansion); `BY NAME` quantifier
family; per-arm parenthesized `ORDER BY`/`LIMIT`; set-op bodies as derived/EXISTS subquery inputs
(the shape validators single-source today — lift only if aliasing/catalog/tail compose). Plus the
standing arc: multi-dedup distinct aggregates, grouping sets, window functions.
