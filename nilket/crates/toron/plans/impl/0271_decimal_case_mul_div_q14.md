# Implementation Plan — ADR 271: Decimal mini-arc (CASE / literal `*` / scalar `÷`)

## Summary
Three localized Decimal completions that together move strict q14 to `pass@execute`: (1) CASE numeric-literal
→ Decimal coercion, (2) extend ADR 266 literal coercion to `Mul`, (3) a scalar `Decimal128 / Decimal128`
binder arm + a custom Toron i128 divide executor route. Each reuses landed machinery (ADR 266 `coerce_literal_value`,
ADR 263 Decimal `*`, ADR 268 half-away rounding, ADR 084 div-by-zero→NULL). Cut crosses `toron` and `toron-tpch`.

## Phase 0 — verify the board + verify-first probes (kept as tests)
- `scripts/cargo-cgroup run -p toron-tpch -- run --query q14 --mode strict --allow-blocked` →
  reconfirm `blocked unsupported_decimal_case@bind`. `matrix` → q01/q06 green.
- Add `decimal_probe.rs` probes (kept regressions): a representative `Decimal / Decimal` pinning the output
  `(p,s)`; a value where **half-away ≠ truncation**; a **negative denominator**; NULL propagation; and
  `÷ 0 → NULL`. Write these first so the divide policy is empirically locked, not assumed.

## Phase 1 — CASE Decimal-vs-numeric-literal coercion (`crates/toron/src/binder.rs`)
- In `bind_case` (`:~4520`), after the result branch inputs are bound and before `case_common_type` (`:4672`):
  scan the bound non-null result nodes for Decimal128 types. If **exactly one** distinct Decimal type is
  present, rewrite each **numeric-literal** result node (`Int*/Float*`) in place to that type via
  `coerce_literal_value` (`:5250`) + `builder.rewrite_literal` (the ADR 266 in-place pattern).
- If **multiple distinct** Decimal branch types appear, do nothing — leave them to the existing
  `case_common_type` reject (no rescale Cast in this cut).
- Result: `case_common_type` then unifies the now-uniform Decimal branches.

## Phase 2 — `Mul` literal coercion (`crates/toron/src/binder/arithmetic.rs`)
- Widen the gate at `:1079` from `Add | Sub` to `Add | Sub | Mul`. `coerce_decimal_arithmetic_literal`
  (`:1186`) already handles either-side orientation; the Decimal `*` arm (`:1146`) + Arrow `numeric::mul`
  (`exec.rs:2509`) then carry `100.00 * SUM(...)`. No new Cast surface.

## Phase 3 — scalar Decimal `÷` (`binder/arithmetic.rs` + `exec.rs`)
- **Binder** (beside the Decimal `*`/`+`/`-` arms, before the non-Decimal fallthrough `:1170`): a
  `Decimal128(p1,s1) / Decimal128(p2,s2)` arm computing `scale = s1 + 4`,
  `precision = (p1 - s1 + s2 + scale).min(38)` in checked signed intermediates; return `BindingError` if
  `scale < 0 || scale > 38 || scale > precision` (mirror `decimal_avg_output_type`'s validation,
  `aggregate_kernel.rs:251`). Also compute the executor's numerator scale delta (`scale + s2 - s1`) and reject
  a negative delta in this slice rather than silently adding a denominator-rescale path. Append the binary with
  `Decimal128(precision, scale)`.
- **Executor**: route `Div` with Decimal operands to a new `evaluate_decimal_division` helper in the binary
  dispatch (`evaluate_bound_binary`, near the Decimal `*` special case `exec.rs:2509`) — **keep `evaluate_arithmetic`
  non-Decimal**, Decimal special cases stay visible at the dispatch. The helper loops `Decimal128Array`
  operands:
  ```
  if numerator or divisor is NULL -> push NULL
  scale_delta = checked(scale + s2 - s1)                    // non-negative in this slice
  scaled_num = checked(numerator_i128 * 10^scale_delta)
  if divisor_i128 == 0 -> push NULL (validity = false)        // ADR 084
  quotient  = scaled_num / divisor_i128
  remainder = scaled_num % divisor_i128
  if 2*|remainder| >= |divisor_i128| { quotient += sign(scaled_num) * sign(divisor_i128) }  // ADR 268, sign-correct
  checked: quotient fits `precision`, else ExecutionError::Kernel
  ```
  Build a `Decimal128Array` with the derived `(precision, scale)`.

## Phase 4 — gauge re-measure (`crates/toron-tpch/`)
- `harness.rs`: strict q14 now `pass@execute` — update `q14_dev_executes_and_strict_blocks_at_decimal_case`
  (rename / adjust: strict q14 reaches execute with a digest; the ADR 269 `unsupported_decimal_case@bind`
  assertion is superseded). dev q14, q01, q06, the q14 keyed-at-default-scale test — unchanged.
- Re-run `run --query q14 --mode strict` + `matrix`; record the measured `pass@execute` row.

## Tests & guards
- Behavior (`binder` tests / `sql_case_test.rs` / `decimal_probe.rs`): CASE Decimal+literal binds (non-literal
  incompatible still rejects); `100.00 * <Decimal>` binds+executes; `<Decimal>/<Decimal>` binds at the declared
  type and executes with half-away rounding + negative-denominator + `÷0→NULL`; over-wide divide output rejects
  at bind.
- Architecture guards (`adr271_*`): CASE Decimal coercion is literal-scoped (not arbitrary-column); Decimal `÷`
  is routed explicitly in binary evaluation and does **not** call raw Arrow `numeric::div`.

## Verification gate (via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt  -p toron -p toron-tpch -- --check
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
git diff --check
```
(No `--all-targets` needed — no new widely-constructed IR field; reuses `Expr::Div` / `Decimal128`.)

## Seal
`impl: adr 271 decimal case mul div q14`, `Co-Authored-By: Claude Opus 4.8 (1M context)
<noreply@anthropic.com>`.

## Risk notes
- **SUM carries input type** — the q14 trace is `(32,4)` through the SUMs, not `(38,4)`; the final divide lands
  at `Decimal128(38,12)`. Don't re-introduce the over-widened intermediate.
- **Negative denominator** — the half-away adjustment must use `sign(num)·sign(divisor)` (the quotient sign),
  not just the numerator sign (AVG only ever divides by a positive count).
- **Div-by-zero must be NULL** — never route Decimal `÷` through raw Arrow `numeric::div` (it errors on zero).
- **Validate, don't saturate** — reject an over-wide divide output type or negative scale delta at bind.
- **Verify wall-by-wall** — implement CASE → re-measure (advances to `*`), then `*` → (advances to `÷`), then
  `÷` → execute; if a narrower wall appears, it names ADR 272 rather than a guessed grab-bag.
