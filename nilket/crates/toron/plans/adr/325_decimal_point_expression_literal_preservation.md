# ADR 325 — decimal-point expression literal preservation

## Status
Proposed.

## Context

The fix that flips ADR 324's honest red (`q11 strict = fail(decimal_literal_scale@compare)`) to green. A
decimal-point literal like `0.0001` currently lowers to `Float64` and loses its scale at parse, so strict-mode
Decimal coercion rounds it to the operand's scale (`0.00`). Preserve the literal's exact value+scale so `0.0001`
survives. **The riskiest cut in the recent arc** — every decimal-point expression literal changes representation —
so Phase 0 probes the breadth first, and the **dev rewrite (D2) is the load-bearing detail**.

## The root cause (grounded)
`lower_expr` (sql_planner.rs:6488-6496): `Value::Number(n, _)` tries `parse::<i32>()` → `Int32`, else
`parse::<f64>()` → **`Float64`**. So `0.0001` becomes an f64 and the text's 4-dp scale is gone. The text
(`n = "0.0001"`) is available — the scale is recoverable. Strict then rounds the f64 to the operand scale → `0.00`.

## Decision — decimal-point expression literal → exact Decimal128 (with the dev rewrite as a hard requirement)

### D1 — exact lowering
- A `Value::Number` with a **plain decimal point and no exponent** lowers to an exact
  `ScalarValue::Decimal128 { value, precision, scale }`. **Precision and scale come from the text**
  (`0.0001 → (5,4)`, `0.05 → (3,2)`, `100.00 → (5,2)`). Precision is the decimal digit count after removing the
  sign and decimal point; leading and trailing zeros are preserved because they define the literal's declared width.
  **Precision > 38 rejects** (a clean lowering error — never a silent cap/truncation).
- **Integer literals stay on the existing integer path** (`Int32` when it fits; otherwise the current fallback
  behavior), unchanged.

### D2 — the dev literal rewrite (LOAD-BEARING — impossible to miss)
Introducing Decimal128 literals **breaks dev** without this: `coerce_decimal_arithmetic_literal` only coerces
Int/Float literals *toward* Decimal, so a Float operand meeting a `Decimal128` **literal**
(`l_discount [Float64] = 0.05`, `Float64_expr * 0.0001`) matches no coercion arm → `arithmetic_output_type`
rejects. **Without D2, this correctness fix breaks dev reach for Float queries that use a fractional constant**
(q06's `0.05`/`0.07`, etc.).
- Add a **literal-only** rewrite: a `Decimal128` *literal* → `Float32`/`Float64` when its counterpart operand is
  `Float32`/`Float64` (`Float64` is the TPC-H dev path),
  in **both comparison and arithmetic** contexts. The value is computed deterministically from the exact decimal raw
  value; this preserves existing dev `Float64` semantics, not decimal exactness.
- **Explicitly NOT:** a general `Decimal128 → Float` cast, any widening-cast expansion, or a user-facing CAST
  surface. Literal-only.

## Phase 0 — probe the breadth before the broad change
Lower one decimal-point literal to `Decimal128` and probe end-to-end **before** converting the path; write findings
first:
1. **The dev rewrite (widest ripple):** confirm `Float64_expr OP Decimal128_literal` rejects today, then that the
   literal-only rewrite fixes it in *both* comparison and arithmetic. This is the part most likely to surprise.
2. **Strict + dev paths:** a Decimal literal vs a Decimal column (strict) and vs a Float64 column (dev); inside
   arithmetic (ADR 263/266/302/321) — each binds+executes.
3. **Goldens / preimage:** does a literal's type change shift any postcard golden? Re-pin in this cut if so.
4. **Precision/reject:** precision from text; `> 38` rejects cleanly.
5. **Negative literal helpers:** `LAG`/`LEAD` defaults negate scalar literals after lowering. A default like `-0.1`
   currently works through `Float64`; after D1 the helper must either negate `Decimal128` literals or keep that path
   explicitly out of scope with a measured reject. Prefer preserving the existing behavior.

## Scope / boundaries
- **In scope:** decimal-point **expression** literals (no exponent) → exact `Decimal128`; the literal-only dev
  rewrite (comparison + arithmetic); the `> 38` reject; integers unchanged; negative decimal-point window defaults
  if the existing helper path is affected.
- **Out (unless Phase 0 proves trivial):** **scientific notation** (`1e-4`) and **INSERT VALUES** number lowering.
- **Never:** a general `Decimal → Float` cast, widening-cast expansion, or a user CAST surface.

## Acceptance — verify-the-flip
- **q11 strict `fail(decimal_literal_scale@compare) → pass@compare`** (correctness **18**) — the threshold uses the
  real `0.0001`. Flip the ADR 324 expected-red test to expect pass.
- **q11 dev unchanged; reach unchanged (22/22).**
- **No regression across every literal-using query** — q06 (`0.05`/`0.07`) and any fractional constant, in *both*
  modes (the dev rewrite is what protects these); **goldens** re-pinned in-cut if the preimage shifts;
  `--all-targets` (broad change). Full `-p toron --no-fail-fast` + `-p toron-tpch` + goldens.
- **Engine tests:** a decimal-point literal binds to exact `Decimal128(text p,s)`; `> 38` precision rejects;
  integers keep the existing type behavior; `Float64_expr OP Decimal128_literal` works in dev (comparison +
  arithmetic) via the literal rewrite; strict operates at the literal's own scale; no general Decimal→Float cast
  added; negative decimal-point `LAG`/`LEAD` defaults keep working if they were supported pre-cut.

## Risks
- **D2 is the break-risk** — without the dev literal rewrite, dev reach breaks for Float + fractional-constant
  queries. First-class, tested in both comparison and arithmetic.
- **Breadth** — every decimal-point expression literal changes type; full suite + goldens + `--all-targets` are the
  net; Phase 0 probes first.
- **Exactness** — `> 38` rejects (no silent truncation); preserve text precision/scale.
- **No cast-surface creep** — literal-only rewrite; do not open a general Decimal→Float cast.
- **Golden re-pin** — if literals feed the preimage, re-pin in the same cut.

## Forward arc
q11 strict → `pass@compare` (correctness 18); decimal literals are now exact for all queries (a latent correctness
improvement beyond q11). Then the correctness backlog: q15/q18/q19/q21 oracles. Scientific notation / INSERT VALUES
literal exactness wait for a measured need.

## Why toron-home
A `crates/toron` cut: the expression literal lowering (sql_planner.rs:6488) + the literal-only dev rewrite (binder
arithmetic/comparison coercion). q11's strict correctness cell flips in `toron-tpch`. No Wendelon / cross-ring
surface.

## Council provenance
Operator + Codex + Claude. Convergent on the fix; Codex tightened the scope and Claude verified the load-bearing
catch (`coerce_decimal_arithmetic_literal` is Int/Float-literal→Decimal only, so `Float64_expr * Decimal128_literal`
rejects in dev). Folded: name is **decimal-point expression literal** (not broad numeric); exact text precision/scale
with **`> 38` reject** (not cap); integers on the integer path; **scientific notation + INSERT VALUES out** unless
Phase 0 trivial; the **dev literal-only Decimal→Float rewrite in comparison *and* arithmetic** is a hard
requirement, not a fallback; **no general Decimal→Float cast / widening-cast / user CAST surface**.
