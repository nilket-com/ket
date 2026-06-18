# ADR 266 — Decimal `+`/`-` (q01-nominated slice) + q01 first-wall measurement

> Re-cut during implementation: the gauge measured q01's *first* wall as `ambiguous aggregate projection`
> at PLAN (multiple same-function aggregates), not the decimal arithmetic bind the iter-1 single-aggregate
> probe predicted. The decimal `+`/`-` cut is kept (correct, q01-nominated, tested); the acceptance is
> split into two independent truths (below). See `plans/impl/266_..._results.md`.

## Status

Proposed.

## Context
The TPC-H gauge drives the roadmap; q06 reaches execute (ADR 263), q01 is the next rung (join-free:
aggregates + `1 - l_discount` / `1 + l_tax` + GROUP BY char keys). The reachability q01 shape uses a
literal cutoff date (`DATE '1998-09-02'`) rather than the official interval expression, matching q06's
literal date-bound proxy so `unsupported_date_interval` does not mask the decimal wall. A verify-first
probe measured q01's walls against the Decimal128 fixture:
- **first red row:** decimal `+`/`-` with a literal operand — `1 - l_discount` →
  `arithmetic operands must be Int32, Int64, Float32, or Float64, got Int32 and Decimal128(15, 2)`
  (same for `1.0 - l_discount`). ADR 263 wired `*` only (Decimal×Decimal), and its literal coercion fires
  at comparison only — arithmetic never coerces an Int/Float literal to the column's `(p,s)`.
- **next, deeper:** `AVG(l_discount)` → `AVG expects numeric input, got Decimal128(15, 2)`.

Per the gauge discipline (Q6 took 262→263), ADR 266 attacks only the **first** measured wall and lets the
matrix move to the next real blocker — it does **not** guess the rest of the slice.

## Decision
1. **Add a q01 attempt to the gauge** (dev + strict), mirroring 261/262's q06. The strict lineitem fixture
   grows `l_returnflag`, `l_linestatus`, and `l_tax`; the dev fixture mirrors them as `Utf8`/`Float64`.
   The q01 SQL deliberately uses `l_shipdate <= DATE '1998-09-02'` for this cut; the official
   `DATE '1998-12-01' - INTERVAL '90' DAY` surface stays out so the decimal blocker remains visible.
2. **Decimal `+`/`-` — q01's equal-scale operator surface, not just `1 - l_discount`.** Cover
   `lit ± decimal`, `decimal ± lit`, and same-`(p,s)` `decimal ± decimal`:
   - binder: extend the literal-to-decimal coercion currently used by comparisons/IN to the
     **arithmetic** path — rewrite an Int/Float literal operand in place to the other operand's
     `Decimal128(p,s)` via the ADR 263 checked helpers; type same-`(p,s)`
     `Decimal ± Decimal → Decimal128(min(38, p + 1), s)` (`+1` precision for carry, scale unchanged).
   - executor: a `Decimal128` `+`/`-` arm routing to Arrow `numeric::add`/`sub` (mirror 263's `*` arm),
     branching around `evaluate_arithmetic`'s Int/Float-only assertion. The binder-declared output type
     must match Arrow's (verify empirically, as with 263's `*`).
3. **Matrix-distinguishable block reasons** (toron-tpch telemetry): add `unsupported_decimal_arithmetic`
   (classifies the "arithmetic operands must be …" + `Decimal128` bind error — the wall 266 clears,
   distinct from the older `unsupported_decimal_scalar` Q6 comparison gap) **and**
   `unsupported_decimal_avg` (classifies "AVG expects numeric input … Decimal128" — q01's post-266 deeper
   blocker) to both the classifier and the recognized-blocker allowlist, before the existing broad
   decimal fallback, so q01's row reads as an honest `blocked(<reason>@bind)` and not a `fail`.
4. **AVG stays out.** AVG(Decimal) introduces decimal **division** + a finalize/rounding policy — a
   different semantic surface than fixed-scale `+`/`-`. Deferred to ADR 267 (named below).

## Acceptance — two independent truths
1. **The decimal slice passes.** `SUM(l_extendedprice * (1 - l_discount))` (and `1 - l_discount` itself)
   **binds and executes** on the strict `Decimal128(15,2)` fixture — the integer literal coerces to
   `Decimal128(15,2)`, `±` types to `Decimal128(16,2)`, `*` widens, `SUM` carries. This is the
   q01-nominated op the cut delivers.
2. **q01 is attempted and reports its real first wall.** The full q01 shape is run through the gauge and
   honestly records `blocked(ambiguous_aggregate_projection@plan)` — a recognized blocker, in both dev and
   strict. q01 moved from `not_attempted` to a measured, named blocker; the gauge named the next cut from
   evidence (it just wasn't the arithmetic wall iter-1 guessed).

NOT claimed: that q01 reaches the arithmetic bind or `pass@execute` — it blocks two layers earlier on
multiple same-function aggregates.

## Deferred (named)
- **ADR 267 — multiple same-function aggregate projection:** q01's measured first wall. The
  aggregate-projection accounting (`rewrite_aggregate_calls_for_projection`) can't disambiguate two
  same-function calls (e.g. `SUM(a), SUM(b)`); q01 has four `SUM`s + three `AVG`s. This is the genuine q01
  prerequisite and the next gauge step.
- **Deeper deferred (behind 267, in q01's bind order):** `AVG(Decimal)` — decimal division (SUM/COUNT) +
  finalize/rounding policy (`unsupported_decimal_avg`, already classified).
- Mixed-scale/precision Decimal `+`/`-` via Decimal casts; Decimal `÷`; exact decimal literals
  (answer-correctness); the official q01 interval date expression; durable-type error names (265).

## Tests / discriminators
- gauge (truth 2): `run_strict_q1`/`run_dev_q1` (telemetry query id `q01`) report
  `blocked(ambiguous_aggregate_projection@plan)` — q01 attempted, real first wall measured.
- decimal slice (truth 1): `SUM(l_extendedprice * (1 - l_discount))` binds + executes on the strict
  Decimal128 fixture (`decimal_add_sub_binds_and_executes`).
- binder: `decimal ± int/float literal` binds (coercion) and same-`(p,s)`
  `decimal ± decimal → Decimal128(min(38, p + 1), s)`; a `(15,2)` case pins the exact `(16,2)` output.
- executor: `SUM(l_extendedprice * (1 - l_discount))` over a small fixture computes the expected decimal
  (end-to-end: coercion → `-` → `*` → SUM).
- negative kept: non-decimal arithmetic still hits the Int/Float domain error (the Decimal arm is additive,
  it does not loosen the assertion for other types); mixed-scale Decimal `+`/`-` remains a named deferred
  gap unless the implementation can prove it without adding Decimal cast machinery; `unsupported_decimal_avg`
  classifies as `blocked`, an unrelated engine error still classifies as `fail` (262's honesty invariant).

## Why toron-home
Toron binder/exec decimal ops + the toron-side TPC-H gauge. No cross-ring surface.
