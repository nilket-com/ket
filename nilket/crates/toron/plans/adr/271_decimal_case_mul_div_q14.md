# ADR 271 — Decimal mini-arc: CASE / literal `*` / scalar `÷` (strict q14 → pass@execute)

## Status
Proposed.

## Context
ADR 269 brought `tpch.q14` into the gauge and measured its strict wall — `CASE result branches cannot be
unified: Decimal128(32,4) and Int32` (`binder.rs:4684`). ADR 270 opened the comma-join path. The remaining
strict-q14 blockers are now all in one Decimal expression tree, three adjacent nodes:

```sql
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
            / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
```

1. **CASE Decimal-vs-integer** — `case_common_type` (`binder.rs:4672`) unifies via `arithmetic_output_type`
   (`ir/execution.rs:1047`), which has no `Decimal128` arm, so a Decimal `THEN` cannot meet an `Int32`
   `ELSE 0`.
2. **Decimal-literal `*`** — `100.00` lowers to `ScalarValue::Float64` (`sql_planner.rs:5920`); ADR 266's
   literal coercion (`coerce_decimal_arithmetic_literal`) is gated to `Add|Sub` (`binder/arithmetic.rs:1079`),
   so `Float64 * Decimal` falls through to the non-Decimal reject.
3. **Scalar `Decimal128 / Decimal128`** — `Div` has no Decimal binder arm and no executor route; it falls
   through to the same reject.

The engine for the surrounding pieces is already landed: `1 - l_discount` is ADR 266 Decimal `-` literal
coercion, `l_extendedprice * (…)` is ADR 263 Decimal `*`, `SUM(Decimal)` carries its input type (ADR 263,
`aggregate_kernel.rs`), and ADR 084 makes div-by-zero yield NULL. So this cut **widens coercion and adds one
divide arm** — it does not build new aggregate or join machinery.

## Decision
ADR 271 is the **full strict-q14 Decimal expression slice in one cut** (3-of-3 council consensus); the
single-wall menus (CASE-only) are too granular for adjacent nodes of one expression tree. strict q14 reaching
`pass@execute` is the gauge.

1. **CASE Decimal-vs-numeric-literal unification (literal-scoped).** In `bind_case`, after the result branches
   are bound but before `case_common_type`, if a **single** Decimal branch type is present among the non-null
   branches, rewrite each **numeric-literal** branch (`Int*/Float*`, e.g. `ELSE 0`) in place to that type via
   the existing checked `coerce_literal_value` (`binder.rs:5250`, the ADR 266 pattern). **Not** in this cut:
   coercing arbitrary non-literal columns, or rescaling across *multiple differing* Decimal branch types
   (a real Cast) — those keep the existing reject.
2. **Extend literal coercion to `Mul`** — add `BoundBinaryOp::Mul` to the gate (`binder/arithmetic.rs:1079`).
   The existing `coerce_decimal_arithmetic_literal` already orients either side; once `100.00` is coerced to
   the Decimal operand's `(p,s)`, the landed Decimal `*` arm (`binder/arithmetic.rs:1146`, exec routes to Arrow
   `numeric::mul`, `exec.rs:2509`) carries it. `100.00` still lowers as Float64 then coerces — Decimal
   text-literal purity stays deferred.
3. **Scalar Decimal `÷`.** Binder arm beside the Decimal `*`/`+`/`-` arms:
   `Decimal128(p1,s1) / Decimal128(p2,s2)` → `scale = s1 + 4`, `precision = (p1 - s1 + s2 + scale).min(38)`
   (the grounded Arrow/Hive divide shape; `p1/s1` are the *numerator*'s). Compute this in checked signed
   intermediates, then **reject cleanly at bind** if the result is invalid (`scale < 0`, `scale > 38`,
   `scale > precision`, or a negative numerator-scaling exponent for this slice) — Toron validates explicitly
   rather than inheriting Arrow's silent scale cap or negative-scale denominator-rescale path. Executor: a
   **custom Toron i128 divide route** in the binary dispatch (`evaluate_bound_binary`), *not* raw Arrow
   `numeric::div`, because Arrow errors on a zero divisor but Toron must yield **NULL** (ADR 084). Per row:
   ```
   if numerator or divisor is NULL { -> NULL slot }
   scale_delta = scale + s2 - s1                            // checked; non-negative in this slice
   scaled_num = numerator_i128 * 10^scale_delta             // checked
   if divisor_i128 == 0 { -> NULL slot }                   // ADR 084
   quotient  = scaled_num / divisor_i128
   remainder = scaled_num % divisor_i128
   if 2*|remainder| >= |divisor_i128| {                    // half-away-from-zero (ADR 268)
       quotient += sign(scaled_num) * sign(divisor_i128)   // sign-correct for a negative denominator
   }
   // checked: quotient fits `precision`, else a kernel error
   ```
   Rounding is **half-away-from-zero** (reusing ADR 268's `avg_decimal_finalize` remainder logic,
   `exec.rs:6589`) so Toron keeps a single Decimal-rounding convention — but the sign adjustment uses
   `sign(num)·sign(divisor)` (the quotient's sign), since binary `÷` (unlike AVG's positive count) can have a
   negative denominator.

### q14 `(precision, scale)` trace (verified — SUM carries its input type)
```
1 - l_discount                         (15,2) - (15,2) -> Decimal128(16,2)
l_extendedprice * (1 - l_discount)     (15,2) * (16,2) -> Decimal128(32,4)   ← the measured THEN type
CASE ... THEN <(32,4)> ELSE 0          0 coerced to (32,4) -> Decimal128(32,4)
SUM(CASE ...)                          SUM carries input  -> Decimal128(32,4)
100.00 * SUM(CASE ...)                 100.00 -> (32,4); (32,4)*(32,4) -> Decimal128(38,8)
SUM(l_extendedprice*(1-l_discount))    carries -> Decimal128(32,4)
numerator / denominator                (38,8)/(32,4): scale=8+4=12, precision=min(38,38-8+4+12)=38
                                       -> Decimal128(38,12)   (scale ≤ precision; valid) -> EXECUTE
```
Caveat (state in the cut): capping precision to 38 trims the integer-digit budget — a known Decimal
*reachability* simplification (like the prior f64→Decimal literal rounding), not answer-exactness; the
verify-first probe confirms q14's values fit.

## Watch-items (operator) → guards
- **CASE coercion stays literal-scoped.** A guard asserts the new coercion only rewrites numeric *literal*
  branches to a single Decimal target — not an arbitrary-column type-lattice expansion; multiple differing
  Decimal branches still reject.
- **Decimal `÷` is an explicit Toron route.** A guard asserts the divide is routed explicitly in
  `evaluate_bound_binary` and does **not** call raw Arrow `numeric::div` (which would turn div-by-zero into a
  kernel error instead of NULL).
- **Divide output type is validated, not saturated.** Over-wide (`scale > 38` etc.) and negative scale/exponent
  cases reject at bind; pinned by a reject test.
- **Verify-first division probe is kept as a regression** (`decimal_probe.rs`): empirically pins the divide
  output `(p,s)`, half-away rounding on a value that discriminates it from truncation, a **negative
  denominator**, NULL propagation, and `÷ 0 → NULL` — rather than trusting the source rule.
- **No engine feature sneaks in.** strict mode stays `Decimal128` (no Float64 shortcut); dev q14, q01, q06 and
  their digests unchanged.

## Acceptance
- `CASE WHEN … THEN <Decimal> ELSE 0 END` binds to the Decimal branch type and executes; a non-literal
  incompatible branch still rejects.
- `100.00 * <Decimal>` binds and executes (literal coerced).
- `<Decimal> / <Decimal>` binds at `Decimal128(scale=s1+4, precision=min(38,p1-s1+s2+scale))`, executes with
  half-away-from-zero rounding (correct for negative denominators), propagates NULL, and `÷ 0 → NULL`; an
  invalid over-wide output type rejects at bind.
- **Gauge:** strict q14 moves `blocked(unsupported_decimal_case@bind)` → `pass@execute` at the default scale;
  dev q14, q01, q06 unchanged. Verified **wall-by-wall** during impl (implement CASE → re-measure advances to
  the `*` wall, then the `÷` wall, then execute) — confirm the predicted sequence; if a narrower divide wall
  surfaces, classify it precisely and let it name ADR 272.

## Forward arc
strict q14 closes as a reachability query. Next TPC-H work is chosen from a fresh matrix read — likely another
multi-table query now that comma-join recognition (ADR 270) exists. Deferred (named): Decimal text-literal
purity; general Decimal casts; CASE rescale across multiple differing Decimal branches; mixed-scale Decimal
`+`/`-` beyond q14; Decimal32/64/256 division; official TPC-H answer-set correctness.

## Why toron-home
Binder coercion + the exec arithmetic kernel + the reachability gauge. Cut crosses `toron` (binder/exec +
guards) and `toron-tpch` (q14 re-measure). No cross-ring surface.
