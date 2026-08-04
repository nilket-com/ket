# ADR 283 — Decimal operand alignment (strict q17)

## Status
Proposed.

## Context

ADR 282 resolved q17's unqualified correlated scalar subquery, leaving a measured reachability split — the only
red row on the board:

```text
q17 dev=pass@execute   strict=blocked(unsupported_decimal_scalar@bind)
```

Strict q17 (official `Decimal128(15,2)` `lineitem`) hits **two** Decimal operand-alignment walls. Binding is
bottom-up (`Filter` before `Project`), so they surface in this order:

1. **Comparison (measured first wall).** `l_quantity < (0.2 * AVG(l_quantity))`. Captured raw bind error:
   `comparison operands have different types: Decimal128(15, 2) and Decimal128(38, 12)`. `l_quantity` is
   `Decimal128(15,2)`; `AVG` widens to `(19,6)`, and the existing `Mul` literal coercion already rewrites `0.2`
   to Decimal, so the product is `Decimal128(38,12)`. Neither side is a bare literal, so `coerce_comparison_literals`
   does nothing and `bind_comparison` rejects (Arrow Decimal comparison requires identical `(p,s)`).
2. **Division (behind the first).** `SUM(l_extendedprice) / 7.0`. `coerce_decimal_arithmetic_literal` is gated to
   `Add|Sub|Mul`, not `Div`, so `Decimal128 / Float64-literal` never coerces.

The ADR 282 forward arc named this "Decimal-literal typing (`0.2`/`7.0` Float64 vs Decimal)." **The code refines
that**: `0.2` is already coerced before `*`, so the measured first wall is not literal typing but reconciling
two **non-literal** Decimal operands of differing scale. `7.0` remains the second, literal-coercion wall behind
that comparison. This ADR adds the engine's first internal **Decimal→Decimal rescale Cast** for the first wall
and extends the existing literal rewrite to `Div` for the second.

## Decision

Align strict q17's Decimal operands as one engine cut: two binder edges + one executor prerequisite.

### D1 — Decimal comparison reconciliation (binder + exec)
When both comparison operands are `Decimal128` of differing `(p,s)` and neither is a literal, rescale to a common
**value-preserving** type and cast the operand(s) that differ, then run the existing equality/nominal checks.

- **Common type:** `scale = max(s1,s2)`; `int_digits = max(p1−s1, p2−s2)`; `precision = int_digits + scale`
  (signed intermediate arithmetic). **Fail loud** (`TypeMismatch`) if `precision > 38` — no silent scale-cap /
  range loss (matches the decimal-division "no silent scale-cap" stance, `arithmetic.rs:1176`). q17 aligns
  `(15,2)` and `(38,12)` → cast the LHS to `(38,12)`.
- **Mechanism (Option L):** a **dedicated comparison-reconciliation helper** that derives the common type and
  inserts the cast via `append_cast` **directly**. `is_widening_cast` is **not** extended. Rationale:
  `is_widening_cast` gates the *implicit* coercion pass `cast_if_needed` (used by CASE/UNION/IN-list/arithmetic) —
  extending it would silently bless Decimal rescale at all those unrelated sites for no q17 benefit; a 10^Δ
  Decimal rescale is a distinct concern from `is_widening_cast`'s primitive-numeric widenings. (The user-facing
  CAST gate is the separate `is_supported_explicit_numeric_cast` — untouched either way; no user CAST surface is
  added.)
- **Logical-identity guard:** insert the implicit rescale **only where the operands' logical identities are naked
  or already nominally compatible** — never to silently erase a *declared* logical identity (receipt doctrine: a
  Cast reconciles, it does not relabel). If both operands carry the same declared identity and the cut chooses
  to reconcile them, the inserted cast output must preserve that identity with `set_node_logical_type`; otherwise
  restrict the reconciliation to all-naked operands and leave declared-identity cases to reject, as today. (q17's
  columns are naked, so the rescale fires either way.)
- **Exec prerequisite:** `evaluate_cast` (`exec.rs:7045`) has **no** `Decimal128→Decimal128` arm (the
  fallthrough is `"unsupported combination (binder gate failure)"`). Add one delegating to
  `arrow::compute::cast`. A binder-appended `BoundExprNode::Cast` already routes to `evaluate_cast`
  (`exec.rs:1368`), so this is the only load-bearing exec change. `evaluate_try_cast` remains out of scope unless
  an implementation can give it true `TRY_CAST` semantics and pin that behavior; public Decimal `TRY_CAST` is
  still binder-gated. Arrow's ordinary cast is safe here precisely because the binder only emits
  value-preserving widenings (scale never decreased, integer digits never lost).
- **General over binary comparisons, not q17-special:** applies at the shared `bind_comparison` site for all
  binary comparison ops (`=,!=,<,<=,>,>=`). The IN-list path is deliberately not widened in this cut; it is not
  needed by q17 and would be a separate shared-site decision. Value-preserving binary comparison rescale
  (widening scale is exact, no rounding) leaves comparison meaning unchanged.

### D2 — Decimal `/` literal coercion (binder)
Extend the `coerce_decimal_arithmetic_literal` gate in `bind_arithmetic` from `Add|Sub|Mul` to include `Div`. The
coercion body is op-agnostic (it rewrites a numeric literal next to a Decimal operand in place); the existing
`decimal_division_output_type` + the executor `Decimal/Decimal` division path then carry it. In-place literal
rewrite (no Cast) — `7.0` *is* a literal.

## Why this shape (intuition)
- **Cast vs in-place rewrite is the hinge.** A *literal* operand can be rewritten to the target Decimal at bind
  (D2). An *expression* operand can't — its value is computed at runtime — so reconciling it needs a real rescale
  at exec, i.e. a `Cast` (D1). Widening scale is exact, so nothing is lost.
- **Why both, why now.** Bottom-up binding makes the comparison the first wall, so a `Div`-only cut ships zero
  measurable q17 movement; the cut must carry both edges to deliver its name.
- **Why Option L over reusing `is_widening_cast`.** Tightly scoping to comparison reconciliation avoids quietly
  generalizing Decimal widening across CASE/IN/arithmetic, which this ADR is not trying to bless.

## Boundaries
- Official q17 SQL unchanged — no harness deviation for `0.2`/`7.0`.
- **No user-facing `CAST(...) AS DECIMAL` surface.** The Decimal cast is binder-inserted for reconciliation;
  `is_supported_explicit_numeric_cast` and `is_widening_cast` are both untouched.
- **Reachability, not answer-correctness.** Decimal division rounding/precision policy (q17's `/7.0`) is a
  correctness-rail question — a q17 oracle later, out of scope here.
- No silent Decimal precision capping. No q02/q22/q05 in this cut.

## Acceptance (tiered)
- **Required:** `q17 strict != blocked(unsupported_decimal_scalar@bind)`.
- **Target:** `q17 dev=pass@execute   strict=pass@execute`.
- **Honest-red fallback:** a *third* wall after both edges (e.g. a Decimal division output-scale surprise) is
  named from the raw measured error as ADR 284 — not hidden under the generic Decimal scalar bucket.

## Tests
- **D1 discriminating:** two non-literal `Decimal128` of differing `(p,s)` compared over matching data — binds +
  executes post-cut, rejects at bind pre-cut. Plus a `precision > 38` case that **fails loud**.
- **D2:** `SUM(<decimal>) / 7.0` (Decimal / Float-literal) binds + executes; rejected pre-cut.
- **Gauge:** flip the q17 harness test to assert `strict = pass@execute` (dev stays green); correct its
  mis-framed "Float64-vs-Decimal literal" comment.

## Forward arc
Closing strict q17 leaves the correlated-scalar family green in both modes. Next: q17 **correctness** oracle
(division rounding/scale policy) on the rail; q05 correctness; q02 (needs a `partsupp` fixture) / q22 (needs
`substring`) reach. ADR 284 only if the post-fix probe surfaces a third wall.

## Why toron-home
Binder + executor changes inside `crates/toron`, with a `toron-tpch` harness assertion flip. No Wendelon /
cross-ring surface.

## Council provenance
3/3 consensus (Claude, Codex, Antigravity). The cast mechanism converged on Option L (dedicated reconciliation
helper + `append_cast`, `is_widening_cast` left untouched) after a corrected premise: `is_widening_cast` gates
the implicit-coercion pass, not the user-facing CAST surface, so extending it would generalize Decimal widening
across unrelated binder sites for no q17 benefit. The logical-identity guard is carried so the general comparison
change never silently relabels a declared identity.
