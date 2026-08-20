# ADR 566 - IS [NOT] DISTINCT FROM

**Status:** Proposed 2026-08-20.

## Context

`a IS DISTINCT FROM b` is the standard's null-safe comparison: it treats two
NULLs as not distinct and a NULL against a value as distinct, and it never
returns NULL. Toron has no arm for it — both spellings die in the planner's
generic unsupported-expression fallthrough — yet the engine already owns the
semantics at the join layer (`null_safe_keys` on `LogicalPlan::Join`, reachable
only from Rune, never from SQL).

Three grounded facts decide the shape. First, the bound-expression evaluator is
a flat program in topological order with memoized slots: each node index is
computed exactly once, and every parent reference reuses the slot;
`BoundExprProgram::validate` enforces topology and bounds but not tree-ness, so
a node with multiple parents is legal. Binding an operand once and referencing
its `BoundExprId` from several nodes therefore evaluates it exactly once, by
construction. Second, `RANDOM` is registered `PerCall` as the reference
non-deterministic function, so single evaluation is testable, not just argued.
Third, the composition below is only correct under Kleene three-valued
`AND`/`OR` — the ADR 0002 contract, restored by the fix that precedes this
record after the grounding found the shipped kernels null-propagating.

## Decision

### 1. A bind-time rewrite onto shared operand nodes, not new operators

`bind_is_distinct_from` binds each operand **once** and composes existing
nodes over the two shared `BoundExprId`s:

- `a IS NOT DISTINCT FROM b` →
  `(a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND a = b)`
- `a IS DISTINCT FROM b` → `NOT` of the same composition.

No new `BoundBinaryOp` variant, no executor arm, no wire change: the
composition inherits `Eq`'s type domain (whatever `=` accepts, `IS DISTINCT
FROM` accepts, and nothing else), and single evaluation is a property of the
memoized program rather than of careful duplication. Under Kleene logic every
disjunct is two-valued in every row — `IS NULL`/`IS NOT NULL` never yield NULL,
and the one nullable conjunct (`a = b`) is guarded by `FALSE AND NULL = FALSE`
— so the output is always non-null, which is the predicate's defining property.

### 2. Both frontends lower to the same rewrite

SQL (`Expr::IsDistinctFrom`/`IsNotDistinctFrom` from the parser) and the Rune
surface lower through one binder entry point, so parity is structural. Rune
naming follows the existing predicate conventions.

## Acceptance

- The complete truth table, projected (not filtered, where NULL and FALSE are
  indistinguishable): both NULL → not distinct; NULL vs value → distinct;
  equal values → not distinct; unequal values → distinct — for both spellings.
- No row of the projected result is NULL, for every input null combination.
  The declared metadata stays whatever the generic composition rule derives —
  conservatively nullable is sound; the runtime never-NULL property is the
  predicate's contract and is what the battery pins.
- SQL and Rune produce identical results on the same batch.
- Single-evaluation, structurally: the bound program of
  `RANDOM() IS DISTINCT FROM x` contains exactly **one** `Random` node — a
  duplicate-bind rewrite fails this.
- Single-evaluation, at runtime: a hand-built program whose `Eq` references one
  shared `Random` node twice evaluates all-true — per-reference re-evaluation
  fails this almost surely.
- Type-domain inheritance: a pair `=` rejects is rejected with the same error
  by `IS DISTINCT FROM`.

## Guardrails

The rewrite is the only implementation: no `BoundExprNode` or `BoundBinaryOp`
addition rides this cut, and the join layer's `null_safe_keys` stays untouched.

## Risks and deferrals

Using `IS NOT DISTINCT FROM` as a join key does not engage the hash path's
`null_safe_keys` — it binds as a residual predicate composition. Promoting it
to a null-safe join key is a planner-recognition decision deferred until a
workload asks for it.

## Forward

The same shared-node rewrite pattern serves `NULLIF` and null-safe `<=>`
spellings if they are ever admitted, without new executor surface.
