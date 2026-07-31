# ADR 268 — AVG(Decimal)

## Status
Proposed.

## Context
q01's measured next wall (267 seal): strict q01 = `blocked(unsupported_decimal_avg@bind)`. The strict
`lineitem` schema types `l_quantity`, `l_extendedprice`, `l_discount` as `Decimal128(15,2)`, and q01 takes
`AVG` of all three. Two coupled gaps, both grounded:

1. **Bind gate.** `is_avg_numeric_input` (`aggregate_kernel.rs:224`) omits `Decimal128`; the input domain
   `AggregateInputDomain::AvgNumeric` rejects with `"AVG expects numeric input, got Decimal128(15, 2)"`
   (classified `unsupported_decimal_avg`). SUM(Decimal) works only because its rule
   (`AggregateOutputRule::SumNumeric` → `sum_numeric_output`) returns `Decimal128(p,s)`; AVG's rule is the
   flat `AggregateOutputRule::Float64`.
2. **Output type + finalize.** AVG is typed `Float64` by rule, finalized through `AvgInt`/`AvgFloat`, and
   materialized as `Float64Array` (`avg_finalize`: `*total as f64 / *count as f64`). There is **no decimal
   division anywhere** (263 = `*`, 266 = `±`; `÷` is genuinely absent).

## Decision

**Decimal-faithful AVG, not a Float64 collapse.** `AVG(Decimal128(p,s)) → Decimal128`. The reachability
gauge would go green either way (its digest doesn't validate against the official answer), so this is a
doctrine call: the arc 263/266 set — SUM(Decimal) stays Decimal, `±` carries scale — says a decimal
column's average is still money/quantity, not a float. Collapsing strict q01 to `Float64` would be the
engine's first silent Decimal→float loss; we decline it. (A Float64 fast path remains legitimate for *dev*
mode / perf experiments — strict mode must exercise the Decimal path.)

1. **Output type / scale policy.** `AVG(Decimal128(p,s)) → Decimal128(min(38, p+4), s+4)`. The fractional
   budget grows by four; precision grows by four with it so the integer-digit budget `(p−s)` is preserved
   (`(15,2)` keeps 13 integer digits → `(19,6)`, not a narrowed `(15,6)`). `+4` is the council's selected
   fixed increment for this cut: large enough to avoid cents-scale averaging loss without opening a full
   Decimal division policy. If the derived scale cannot fit the derived precision (`s+4 > min(38, p+4)`,
   i.e. a very high-scale input), **reject cleanly at bind** rather than silently truncate.

   *Intuition / why widen at all:* a quotient deserves more fractional precision than its inputs. Averaging
   cents (`s=2`) and forcing the answer back to cents throws away the very information the average exists to
   expose — `AVG(0.06, 0.05)` is `0.055000`, not a coin-flipped `0.06`/`0.05`.

2. **Rounding: half away from zero**, in pure integer arithmetic (no binary float). Scale the i128 total
   from `s` to `s_out` (`scaled = total · 10^(s_out−s)`), then divide by `count` rounding the half *away
   from zero* — exact via the remainder: `q = scaled / count; r = scaled % count; if 2·|r| ≥ count { q +=
   sign(scaled) }`. Symmetric for negatives, so a negative tie is pinned by test, not left to a positive-only
   assumption.

3. **Dedicated accumulator state.** Add `AggState::AvgDecimal { total: i128, count: u64, input_scale: i8 }`
   rather than overloading `AvgInt`. The decimal finalize needs the *input* scale to compute the rescale,
   and a self-describing state keeps "this is a decimal average" out of downstream type-inference guesswork
   (the divide path is genuinely different from the int/float one). A `Decimal128` value already enters the
   accumulator as its raw unscaled i128 (`numeric_value:6513`), so accumulation is a checked i128 add.

4. **AVG-only this cut.** Scalar `Decimal ÷ Decimal` is **not** q01-nominated (q01 has `*`, `±`, SUM, AVG —
   no scalar `/`) and carries its own operand-driven scale policy distinct from AVG's sum/count shape — a
   real policy boundary, not ossification-avoidance. **Deferred and named (ADR 269)**, reusing this cut's
   decimal-divide helper but deciding its own scale.

5. **Conservative logical type.** The additive logical-type helper keeps an aggregate's declared identity
   only when the output physical type still matches the logical type's physical type. Widening
   `(15,2)→(19,6)` breaks that, so a decimal AVG output is **naked** (a plain `Decimal128`, no inherited
   unit) until a deliberate logical-type widening policy is designed. q01's columns are not logical-typed,
   so the gauge is unaffected.

No new IR / opcode / Cast node; no `evaluate_arithmetic` operator change — the divide lives in the AVG
finalize. Non-decimal AVG stays `Float64`, bit-for-bit unchanged.

## Watch-items (operator) → guards
- **Scale policy is pinned, not incidental.** A doc-comment names `s+4 / min(38,p+4) / half-away-from-zero`,
  and a **discriminating** test uses a non-divisible sum (for example,
  `AVG(0.01, 0.02, 0.02) -> 0.016667`) so the widened raw Decimal result differs observably from a
  scale-`s` average. A separate type assertion fails the Float64-collapse path.
- **Input-domain and output-type derivation share one helper.** Invalid high-scale Decimal inputs must not
  be admitted by `is_avg_numeric_input` and then fall through `stable_output_field`'s fallback type.
- **Rounding sign is pinned** by a negative-tie unit test (half *away from zero*, not toward).
- **Overflow is explicit.** The `10^(s_out−s)` pre-multiply can overflow i128 for a sum near the precision
  ceiling — use checked arithmetic with a clear `Kernel { "AVG decimal scale overflow" }`, never a wrap. An
  i256-widened intermediate is a named hardening follow-on, not this cut.
- **`count = 0` / all-NULL group → NULL**, exactly as the existing AVG null semantics, now in decimal.
- **No Float64 collapse in strict:** a guard/test asserts the decimal AVG path materializes `Decimal128`.
- **Window in lockstep:** global, grouped, and window AVG share one decimal finalize so windowed
  AVG(Decimal) can't silently diverge (or stay accidentally rejected).

## Acceptance
- `AVG(Decimal128(15,2))` binds and **executes** (global + grouped), producing `Decimal128(19,6)`:
  `AVG(0.06, 0.05) → 0.055000` (raw unscaled `55000`). Discriminating test (non-divisible sum) + signed-tie
  rounding test lock the policy. Integer/float AVG tests still return `Float64`.
- Gauge: strict q01's three decimal AVGs bind+execute → q01 advances off `unsupported_decimal_avg`. Expected
  `pass@execute` (after 263 `*`/SUM and 266 `±`, AVG is the last arithmetic piece in q01's body) — confirmed
  by *running*, not asserted blind; a deeper wall, if any, is named by the gauge and drives ADR 269+. dev
  q01 unchanged. q06 unchanged.

## Deferred (named)
- **ADR 269 — scalar `Decimal ÷ Decimal`:** the `Operator::Divide` arithmetic arm with its own operand-driven
  scale policy; reuses this cut's decimal-divide helper. Not q01-nominated; let the matrix confirm it as the
  next row.
- **i256-widened AVG intermediate:** removes the `10^δ` pre-multiply overflow ceiling.
- **Decimal AVG logical-type widening policy** (point 5): give the widened average an inherited unit.

## Why toron-home
Toron aggregate kernel + binder type rule + executor finalize. No cross-ring surface.
