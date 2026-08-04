# ADR 302 — Decimal add/sub operand alignment (mixed precision/scale)

## Status
Proposed.

## Context

ADR 301's q09 strict measured `blocked(unsupported_decimal_arithmetic@bind)`:
```text
TypeMismatch: "arithmetic operands must be Int32, Int64, Float32, or Float64, got Decimal128(32, 4) and Decimal128(31, 4)"
```
The profit subtraction `l_extendedprice*(1-l_discount) − ps_supplycost*l_quantity` produces two Decimal products
of compatible scale (4) but different precision (32 vs 31). `bind_arithmetic` (`arithmetic.rs:1001`) accepts
Decimal `+`/`-` only at **identical (p,s)** (ADR 266, `:1162`) — its own comment defers
"mixed-scale/precision +/-" — so the binder rejects before execute. (`÷` is already general via ADR 271.)

## Decision — binder output typing only (grounded; no casts)

**Grounding (Codex, verified against local Arrow 58.3.0):** `arrow_arith::numeric` `add`/`sub` **already** handle
differing-(p,s) Decimal operands — they internally rescale to `max(s1,s2)` and stamp the Hive-rule result
(`result_scale = max(s1,s2)`, `result_precision = max(s1,s2) + max(p1-s1, p2-s2) + 1`, capped at Arrow's max
precision). Toron's executor already routes Decimal `+`/`-` through Arrow numeric (not `evaluate_arithmetic`). So
the executor needs **no change and no operand casts** — the only gap is the binder's type rule.

### D1 — generalize the Decimal `+`/`-` binding
- Add `decimal_add_sub_output_type(p1, s1, p2, s2) -> Option<DataType>` near the other Decimal helpers, mirroring
  Arrow's supported-shape output rule: `scale = max(s1,s2)`; `precision = min(38, max(s1,s2) + max(p1-s1, p2-s2) + 1)`;
  return `None` on an invalid shape (negative scale, `p-s < 0`).
- In `bind_arithmetic`, **replace the same-(p,s) `+`/`-` block** with a general Decimal `+`/`-` block: for any
  `Decimal128 +/- Decimal128`, type the binary node via `decimal_add_sub_output_type`; **leave the operands as
  bound** (no casts — Arrow rescales at runtime). The same-(p,s) case is subsumed (`max(p-s)+max(s)+1` = `p+1` at
  equal (p,s), capped — identical to the old arm).
- **No rename / no reuse of the comparison machinery** — `decimal_comparison_common_type` / `append_comparison_cast`
  stay comparison-specific (arithmetic has the distinct `+1`-carry result type; a dedicated helper is clearer and
  avoids 283/286 call-site + guard churn).
- Logical-identity guard: differing declared identities don't reconcile silently — leave the existing logical-type
  arithmetic checks to reject (the 283/266 discipline).

For q09: `Decimal128(32,4) − Decimal128(31,4)` → `scale 4`, `precision = 4 + max(28,27) + 1 = 33` →
`Decimal128(33,4)`; the node types to that; Arrow's `sub` computes it at runtime.

### Overflow policy — preserve the existing cap (do not introduce fail-loud here)
The existing same-(p,s) arm caps (`min(38, p+1)`), and Arrow's runtime also **caps** at max precision — so capping
the bound type **matches the executor**. This cut preserves cap semantics (consistent + matches Arrow); it does
**not** add ADR 283-style fail-loud. (If the council later wants fail-loud, apply it **uniformly** to all Decimal
`+`/`-` incl. same-(p,s) — a separate decision; don't split cap-vs-reject for the same theoretical width.)

## Scope / boundaries
- Decimal `+`/`-` at any (p,s) (q09's different-precision/same-scale, and different-scale). `÷` unchanged (ADR
  271). Int/Float arithmetic, the ADR 266 literal coercion, and the result-cap policy unchanged. No `is_widening_cast`
  expansion; no public `CAST` surface; no exec change (Arrow already handles it).

## Acceptance
- A focused Decimal behavior test: `Decimal128(p1,s) ± Decimal128(p2,s)` with `p1 != p2` (q09's shape) and a
  different-scale pair bind + **execute** correct values, typed per `decimal_add_sub_output_type` (= Arrow's Hive
  rule); same-(p,s) `+`/`-` and Int/Float arithmetic unchanged. Extend `arrow_decimal_probe` with the mixed case
  (pin that Arrow's output type == `decimal_add_sub_output_type`). Discriminating: rejects pre-cut at `:1196`.
- **q09 strict advances past `unsupported_decimal_arithmetic`** — verify-first re-measure: `pass@execute` (keyed
  at sf=0.01) → **reach 14**, or a precise further wall (then the next ADR). q09 dev stays green. (If the executor
  unexpectedly walls at *execute* — i.e. Arrow doesn't handle it as grounded — fall back to operand casts; the
  verify-first run is the check.)
- Full `-p toron --no-fail-fast` green (`bind_arithmetic` is shared); reach (q01-08/10/12/14/17/20) + the 13
  correctness cells unchanged.

## Risks
- **`bind_arithmetic` is shared** — the generalized block must fire only for Decimal-Decimal `+`/`-`; the `÷` arm,
  the multiply arm, Int/Float, and literal coercion stay. Full suite is the net.
- **Binder type must match Arrow's runtime exactly** — `decimal_add_sub_output_type` mirrors Arrow's Hive rule +
  cap; the `arrow_decimal_probe` pins it so the bound `(p,s)` can't drift from the executor's stamp.
- **The same-(p,s) subsumption** — confirm the generalized block reproduces the old `min(38, p+1)` for equal (p,s)
  (no regression on the ADR 266 path).

## Forward arc
q09 strict greens → **reach 14**; then q09 correctness (the subtraction-profit shape — a new rail arithmetic) is
the catchup candidate, resuming the 2:1 cadence. q13/q19/q22 remain reach machetes.

## Why toron-home
A `crates/toron` binder cut (`bind_arithmetic` typing); no exec change (Arrow numeric already handles it). q09's
strict row flips in `toron-tpch` with no harness logic change. No Wendelon / cross-ring surface.

## Council provenance
3/3 on the topic; **Codex's iter-1 Arrow grounding reshaped the mechanism**: the executor (Arrow numeric) already
handles differing-(p,s) Decimal `+`/`-`, so the fix is **binder output typing only** (`decimal_add_sub_output_type`),
**no casts, no rename** — grounding beats the iter-0 cast/rename assertions (Claude/Gemini). Overflow: **preserve
the existing cap** (matches the same-(p,s) arm + Arrow), fail-loud deferred as a uniform decision. The arithmetic
counterpart to ADR 283/286 (comparison), but typing-only.
