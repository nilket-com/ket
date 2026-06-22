# ADR 321 — Decimal × Int multiply operand alignment (q11)

## Status
Proposed.

## Context

q11's measured strict wall (ADR 320 verify-the-flip): `unsupported_decimal_arithmetic@bind` on
`ps_supplycost * ps_availqty` — `Decimal128(15,2) * Int32`, where `ps_availqty` is a **non-literal** Int operand.
The fix is the **non-literal analogue of ADR 266's literal coercion**, scoped to multiply.

## The wall (grounded)
- The Mul arm (ADR 263, arithmetic.rs:1153) handles `Decimal128 * Decimal128` only.
- `coerce_decimal_arithmetic_literal` (ADR 266) coerces an Int/Float **literal** → Decimal via `builder.literal(...)`,
  which is `Some` **only for literals**. A non-literal Int operand (column, or a computed int expression) gets no
  coercion → falls through → `arithmetic_output_type` rejects "operands must be Int/Float, got Decimal128 and
  Int32" → `unsupported_decimal_arithmetic`. Dev (`Float64 * Int32`) coerces, hence the q11 split.

## Decision — coerce a non-literal Int operand to Decimal in the Mul arm
A `crates/toron` binder cut. The clean post-bind predicate is **"a non-literal Int32/Int64 operand beside a
Decimal128"** (column *or* computed integer expression — not strictly a column). For `Mul`, cast that Int operand
to `Decimal128(int_prec, 0)` (Int32 → `Decimal128(10, 0)`, Int64 → `Decimal128(19, 0)`), then the existing ADR 263
rule: `Decimal128(p, s) * Decimal128(int_prec, 0)` → `Decimal128(min(38, p + int_prec + 1), s)`.
- q11: `Decimal128(15,2) * Decimal128(10,0)` → `Decimal128(26, 2)`.
- Exec: insert an internal `Cast(Int → Decimal128(int_prec, 0))` on the Int operand with `append_cast` (not
  `cast_if_needed`; the primitive widening helper does not admit Int→Decimal). Arrow `cast` already supports the
  runtime conversion, then the existing Decimal×Decimal `numeric::mul` runs. No new kernel.
- **Cap policy unchanged:** the output keeps the existing Mul `.min(38)` precision cap (this ADR does **not**
  reopen the older Decimal overflow policy).

## Scope / boundaries — multiply only
- **In scope:** `Decimal128 * <non-literal Int32/Int64>` (and the symmetric `Int * Decimal`), the non-literal
  analogue of ADR 266.
- **Logical identities:** do not relabel identity-bearing integers as Decimal. The existing logical-arithmetic
  gate runs before the Decimal arm and rejects non-arithmetic logical identities; this cut only inserts the
  internal cast for arithmetic-eligible/naked Int operands.
- **Deferred:** `+`/`-`/`÷` with a non-literal Int operand — the coercion is op-agnostic, but the title is
  *multiply* and those cases aren't query-measured; a later ADR folds them (testing each op) if a query needs them.
  **Decimal-op-Float-column** (a distinct Decimal-vs-Float precedence question). Literal coercion (ADR 266)
  unchanged.

## Acceptance — verify-the-flip (reach, NOT q11 correctness)
- **q11 strict moves off `unsupported_decimal_arithmetic@bind`** — verify-first re-measure: `pass@execute`
  (keyed; per-row multiply, no cartesian) → **q11 both-modes-green → reach 20**. Or a deeper measured blocker
  (the Decimal×Int multiply was q11's only remaining strict *bind* wall).
- **Engine tests:** `Decimal128(15,2) * Int32-col` → `Decimal128(26,2)` correct values; `* Int64-col` →
  `Decimal128(35,2)`; a **computed** int expression (not just a column) beside Decimal coerces; `Int * Decimal`
  symmetric; the output type pinned; an overflow pair (`p + int_prec + 1 > 38`) **caps per the existing `.min(38)`
  policy**; two-Decimal / Int-Int / Float / literal paths unchanged.
- Full `-p toron --no-fail-fast` + `--all-targets` + `architecture_test` green (`bind_arithmetic` is shared);
  **q15/q18/q19 + all other reach + correctness cells unchanged**; ADR 266 literal coercion unchanged.

## Reach/correctness boundary — q11 green is NOT a correctness signal (Codex caught this)
After this cut q11 strict reaches execute, but q11's HAVING threshold `… > (SELECT SUM(...) * 0.0001 …)` hits the
existing **Float→Decimal literal simplification**: `decimal_literal_from_f64` rounds the literal to the target
Decimal's scale, so when the `SUM` is scale 2, `0.0001` rounds to `0.00` — the threshold collapses to `> 0`. That
is acceptable for **reachability** (q11 executes) but means **q11's green cell does NOT imply correct results**. The
future q11 correctness oracle must account for it, or a separate ADR fixes the literal-scale (widen the literal to
its own scale rather than rounding to the operand's). Record this explicitly so q11-green is never mistaken for
q11-correct.

## Risks
- **`bind_arithmetic` is shared** — the non-literal Int coercion must fire only for Decimal×(non-literal Int);
  Int-Int, Float, two-Decimal, and literal paths unchanged. Full suite is the net.
- **Output type / cap** — Int32→`(10,0)` / Int64→`(19,0)`; the carry can exceed 38 for a wide Decimal × Int64;
  keep the existing `.min(38)` cap (don't reopen the overflow policy). q11's `Decimal128(26,2)` is well under.
- **Reach ≠ correctness** — the `* 0.0001` rounding boundary above; don't claim q11 correct on the green cell.

## Forward arc
q11 strict greens → **reach 20**. q11 correctness then must handle the `* 0.0001` literal-scale boundary (a likely
sub-ADR or oracle accommodation). Atlas remaining: q18 (grouped EXISTS-body → re-measure the composite path vs
q15's A1 substrate), q21 (non-equi EXISTS). The q11 chain (314 reach → 320 HAVING-scalar → 321 Decimal×Int) closes
on reach.

## Why toron-home
A `crates/toron` binder cut (`bind_arithmetic` non-literal Int coercion) + the existing Int→Decimal cast /
Decimal×Decimal exec. q11's strict row flips in `toron-tpch` with no harness logic change. No Wendelon /
cross-ring surface.

## Council provenance
Operator + Codex + Claude. Codex's grounding folded: (1) scope is **non-literal** Int operand (column or computed),
not strictly column; (2) **multiply only** (title-driven; +/-/÷ deferred); (3) the **`* 0.0001` reach/correctness
boundary** — `decimal_literal_from_f64` rounds to the operand scale (`0.0001 → 0.00` at scale 2), so q11-green ≠
q11-correct; (4) **match the existing `.min(38)` cap**, don't reopen the overflow policy. The non-literal analogue
of ADR 266; reuses ADR 263 mul + Arrow Int→Decimal cast.
