# ADR 564 - CAST beyond numerics

**Status:** Proposed 2026-08-19.

## Context

`CAST` supports fourteen numeric pairs and nothing else: no string source, no
string target, no temporal conversion. `CAST('12' AS BIGINT)`,
`CAST(id AS VARCHAR)`, and `CAST(text AS TIMESTAMPTZ)` are all core-standard
spellings a user tries first, and today each dies with the numeric-only
message. The string↔temporal half is ADR 0558's own named deferral, recorded
"so the two spellings do not diverge" — the parsers, the session-zone capture,
and the `Raises`/`YieldsNull` doctrine all exist now.

Two grounding facts shape the design. First, the cast surface is **three
hand-kept matrices that must agree**: the binder's whitelist, `evaluate_cast`'s
arms, and `evaluate_try_cast`'s arms. Diffing them found they already disagree —
the whitelist lists identity pairs the executor never sees, the executor
implements three `Decimal128` arms the whitelist does not admit (reachable only
through binder-synthesized arithmetic coercions, not user casts), and
`TRY_CAST` lacks those decimal arms entirely. Nothing is broken today, but the
structure is three populations served by three lists with the agreement
documented nowhere — the drift this record's first decision removes. Second,
`BoundExprNode::Cast` carries no session zone, and zone-less timestamp text
needs one — but the ADR 0558 kernel nodes already carry it, captured at bind.

## Decision

### 1. One support table is the matrix's single source of truth

A single function classifies every `(source, target)` **type-family** pair —
`Decimal128` is one family, not one entry per `(precision, scale)` — along two
dimensions:

- **support**: unsupported, infallible, or fallible;
- **admission**: `SqlCast` (a user may write it) or `SynthesizedOnly` (only the
  binder's own coercions construct it).

The admission dimension exists because the grounding found the executor's
`Decimal128` arms are reachable only through binder-synthesized arithmetic
coercions. A table keyed on support alone would either expose those to user
`CAST` by accident or delete coercions arithmetic requires. The SQL binder
admits a pair only when it is `SqlCast`; executor conformance covers every
supported pair regardless of admission, since a synthesized cast still executes.
The strict arm raises and the `TRY` arm yields NULL for every fallible pair.
Three matrices become one declaration and two conforming implementations.

### 2. String → temporal casts bind onto the ADR 0558 kernels

`CAST(text AS TIMESTAMPTZ)` binds to the same `ToTimestamp` scalar node
`TO_TIMESTAMP(text)` binds to, zone captured at bind; `TRY_CAST` binds to
`TryToTimestamp`; `DATE` likewise onto `ToDate`/`TryToDate`. The two spellings
cannot diverge because they are one node — the non-divergence ADR 0558 asked
for, delivered by construction rather than by parallel maintenance. Behaviour
is inherited wholesale: explicit offsets win, zone-less text resolves against
the session zone, dates are zone-independent, `TRY` yields NULL per element and
is unconditionally nullable through `ScalarFunctionFailure::YieldsNull`.

### 3. String → numeric parses per row, strict and TRY

`Utf8` and `LargeUtf8` sources cast to `Int32`, `Int64`, `Float32`, `Float64`,
after trimming surrounding ASCII whitespace — the standard's treatment, and
what every arriving user's data assumes. Strict raises an execution error
naming the offending value; `TRY_CAST` yields NULL for it, exactly the
`try_cast_f64_to_i32` shape the executor already has. An integer target rejects
fractional text rather than truncating it — `'1.5'` does not become `1`, for
the same reason `date + INTERVAL '36 hours'` did not become a day.

### 4. Numeric and temporal → string render one canonical form each

- Integers: decimal digits, `-` for negatives.
- Floats: the shortest round-trip decimal form (Rust's float `Display`), so
  `CAST(CAST(x AS VARCHAR) AS DOUBLE)` returns `x` exactly; non-finite values
  render `NaN`, `inf`, `-inf`.
- `Decimal128(p, s)`: scale-faithful digits with exactly `s` fractional places.
- Timestamps: the existing RFC3339 rendering in UTC; dates: `YYYY-MM-DD` — the
  same canonical forms results already print, so a cast agrees with output.

Rendering cannot fail, so these pairs are infallible and `TRY_CAST` is
identical by inheritance, not by a second implementation. The target is `Utf8`;
`VARCHAR` and `TEXT` join the lowerable target types.

### 5. TRY_CAST's metadata is untouched

`BoundExprNode::TryCast` remains unconditionally nullable, and the string →
temporal rewrite preserves the property through `YieldsNull` on the kernel it
binds to. No new nullability path is introduced; the two existing ones are the
two this cut uses.

## Acceptance

- `CAST('12' AS BIGINT)` is 12; `CAST(' 12 ' AS BIGINT)` is 12;
  `CAST('1.5' AS BIGINT)` raises naming the value; `TRY_CAST` yields NULL for
  it in a batch whose other rows convert, proving per-element failure.
- `CAST('1.5' AS DOUBLE)` is 1.5 from both string widths.
- `CAST(id AS VARCHAR)` renders decimal digits;
  `CAST(CAST(price AS VARCHAR) AS DOUBLE)` round-trips exactly.
- `CAST(decimal_price AS VARCHAR)` carries exactly the declared scale.
- `CAST(text AS TIMESTAMPTZ)` equals `TO_TIMESTAMP(text)` on the same rows —
  asserted as result equality, the non-divergence made falsifiable — and the
  bound plans carry the same kernel node.
- `TRY_CAST('not a date' AS DATE)` yields NULL and is declared nullable over a
  non-nullable input; strict `CAST` raises.
- `CAST(ts AS VARCHAR)` matches the engine's canonical timestamp rendering;
  `CAST(d AS VARCHAR)` is `YYYY-MM-DD`.
- Every family pair the support table declares is exercised strict and TRY;
  every pair it excludes still gets the unsupported-cast error, updated to name
  the wider rule.
- A `SynthesizedOnly` pair — `CAST(amount AS DECIMAL(15, 2))` — stays rejected
  for user SQL while the arithmetic coercions that synthesize it keep working,
  proving the admission dimension separates the two populations.

## Guardrails

The support table is the only place a pair is classified. A bind check or an
executor arm admitting a pair the table does not declare is the drift this
record exists to prevent; the per-pair conformance test walks the table, so an
arm added without a declaration fails it.

The round-trip control for floats must use a value whose shortest form
exercises precision (not `1.0`), and the fractional-text rejection must assert
the error names the value.

## Risks and deferrals

Boolean casts, `Decimal128` targets from strings, casts between string widths,
and non-decimal integer text (hex, exponents for integers) are deferred.
Exponent notation for float text follows Rust's parser and is documented as
such. `TIMESTAMP WITHOUT TIME ZONE` stays rejected; `TIMESTAMPTZ` is the
spelling, as everywhere else in the engine.

## Forward

With `CAST(id AS VARCHAR)` available, `STRING_AGG` over non-string values
becomes expressible by composition rather than by widening its value domain —
the composition this engine prefers to a special case.
