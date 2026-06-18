# ADR 266 — Q1 reachability slice: decimal `+`/`-`

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

## Acceptance (monotonic-progress)
q01 strict **no longer blocks on arithmetic bind**: the matrix cell moves from
`blocked(unsupported_decimal_arithmetic@bind)` to the measured `blocked(unsupported_decimal_avg@bind)` —
or, unexpectedly, `pass@execute`. A remaining red row must be the *earned* deeper blocker, not the one
266 set out to clear.

## Deferred (named)
- **ADR 267 — AVG(Decimal):** decimal division (SUM/COUNT) + finalize/rounding policy; flips q01 off the
  `unsupported_decimal_avg` blocker.
- Mixed-scale/mixed-precision Decimal `+`/`-` via Decimal casts; Decimal `÷` generally; exact decimal
  literals (answer-correctness); the official q01 interval date expression; durable-type error names
  (265).

## Tests / discriminators
- gauge: `run_strict_q1` advances past `@bind` with telemetry query id `q01` (the q01 row moves off
  `unsupported_decimal_arithmetic`).
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
