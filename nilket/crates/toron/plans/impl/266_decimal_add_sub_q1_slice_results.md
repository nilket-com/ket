# ADR 266 — Results: Decimal `+`/`-` (q01-nominated) + q01 first-wall measurement

## The re-cut (honest, during implementation)

iter-1's verify-first probe used a *single* aggregate (`SUM(l_extendedprice * (1 - l_discount))`) and
measured q01's first wall as the decimal arithmetic bind. The **full** q01 shape (four `SUM`s, three
`AVG`s) blocks two layers earlier, at PLAN, on `ambiguous aggregate projection` — the aggregate-projection
accounting can't disambiguate multiple same-function calls. Confirmed minimal repro:

```
SELECT l_returnflag, SUM(l_quantity), SUM(l_extendedprice) FROM lineitem GROUP BY l_returnflag
  →  unsupported frontend construct: ambiguous aggregate projection   (sql_planner.rs:4889)
```

The decimal `+`/`-` cut is correct and q01-nominated, so it was kept; the acceptance was split into two
independent truths rather than the (now-falsified) "q01 advances off the arithmetic bind".

## Truth 1 — the decimal `+`/`-` slice passes

On the strict `Decimal128(15,2)` fixture (`decimal_add_sub_binds_and_executes`):
- `1 - l_discount` → the integer literal coerces to `Decimal128(15,2)`, `-` types to `Decimal128(16,2)`;
  values `1.00-0.06=0.94`, `1.00-0.05=0.95`.
- `SUM(l_extendedprice * (1 - l_discount))` binds **and executes** end-to-end (coercion → `-` → `*` →
  `SUM`) = `284.0000`.
- `arrow_decimal_probe` asserts Arrow `add`/`sub` `(15,2)±(15,2) → (16,2)` (the typing the binder encodes).

Implementation: binder reuses `coerce_literal_value` for the arithmetic literal (gated to `+`/`-` +
Decimal, so non-decimal arithmetic is untouched) + a same-`(p,s)` `Decimal ± Decimal → Decimal(min(38,
p+1), s)` arm beside 263's `Mul`; executor routes `Add`/`Sub` over two `Decimal128` to Arrow
`numeric::add`/`sub`. No new IR/opcode; no Cast node; no `evaluate_cast` change.

## Truth 2 — q01 attempted, real first wall measured

```
q01 dev=blocked(ambiguous_aggregate_projection@plan) strict=blocked(ambiguous_aggregate_projection@plan)
q06 dev=pass@execute strict=pass@execute        (unchanged; digest [130, 66, 124, 232, …])
```

q01 moved from `not_attempted` to a measured, **recognized** blocker (`ambiguous_aggregate_projection`, a
new telemetry reason classified before the broad `unsupported_query_surface`). Same wall in dev and strict
(it is type-independent). The gauge named the next cut from evidence — it just wasn't the arithmetic wall
iter-1 guessed.

NOT claimed: q01 reaching the arithmetic bind or `pass@execute`.

## Next blockers (named)

- **ADR 267 — multiple same-function aggregate projection** (`rewrite_aggregate_calls_for_projection`):
  q01's measured first wall and genuine prerequisite; the next gauge step.
- **Deeper deferred (behind 267, in q01's bind order):** `AVG(Decimal)` — decimal division + finalize
  policy (`unsupported_decimal_avg`, already classified). Then mixed-scale `+`/`-`, `÷`, exact decimal
  literals, the official interval date expression.

## Guard reconcile
The shared runner `run_q6` was renamed to `run_query(query_id, …)` (it now serves q06 + q01); the ADR 262
guards `adr262_strict_q6_attempts_decimal128_compute` / `..._first_red_row_is_grounded` were retargeted
from `run_q6(` to `run_query(`.

## Gate
`--all-targets` clean · full `--no-fail-fast` green · `architecture_test` green · `toron-tpch` green
(decimal_probe 5 / arrow_decimal_probe 3 / harness 8 / architecture 10) · fmt + `git diff --check` clean.
