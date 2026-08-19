# ADR 559 - Temporal format vocabulary

**Status:** Proposed 2026-08-19.

## Context

ADR 558 gave the engine row-wise temporal parsing, but only for the fixed
grammar the literal path already accepted. Rendering is fixed too, and
differently: `format_timestamp_in_zone` emits RFC3339 and `format_date_ymd`
emits `YYYY-MM-DD`. No pattern vocabulary exists anywhere in the engine — the
`YYYY-MM-DD` strings in the source are error messages describing accepted input,
not a language.

So the two directions already disagree about shape, and each new fixed form
widens the gap. `TO_CHAR` cannot be added without inventing a vocabulary, and a
vocabulary invented for rendering alone would entrench the divergence rather than
close it. This record introduces one vocabulary and makes both directions speak
it, which is why `TO_CHAR` and the two-argument `TO_TIMESTAMP` land together.

## Decision

### 1. One closed token set, shared by both directions

A pattern is a sequence of tokens drawn from a closed set:

| Token | Meaning |
| --- | --- |
| `YYYY` | Four-digit year |
| `MM` | Two-digit month |
| `DD` | Two-digit day |
| `HH24` | Two-digit hour on a 24-hour clock |
| `MI` | Two-digit minute |
| `SS` | Two-digit second |
| `FF9` | Nine-digit fractional second |

Non-alphanumeric characters are literal separators. An alphanumeric run that is
not a known token is a **binding error**, never literal output. Literal
alphanumeric text uses an explicit escape: a double-quoted run is emitted
verbatim and matched verbatim.

Passing unknown runs through as literal text, as some engines do, was rejected.
It makes a typo render as itself — `YYYY-MM-DDD` would silently emit a trailing
`D` — and this codebase has consistently refused the reading where a mistake
produces plausible output instead of an error.

The set is deliberately small. A token is added when a caller needs it, the way
every other registry property in this arc arrived.

### 2. The pattern is a constant argument, compiled at bind time

The pattern argument must bind to a string literal. The binder reads it,
compiles it, and stores the compiled form in the scalar parameter. This follows
`SUBSTRING`'s constant-bounds precedent, where the binder reads a literal
argument's value rather than evaluating it per row.

A malformed pattern therefore fails during binding, before any row is touched,
and every fragment, worker, and retry executes one already-compiled program
rather than recompiling text.

Row-valued patterns are deferred. Admitting them requires a per-row compilation
and failure policy — including whether a bad pattern in one row fails the
statement or yields NULL — and that decision belongs with the evidence of a
caller who needs it.

### 3. One compiled representation serves rendering and parsing

`TO_CHAR` and two-argument `TO_TIMESTAMP` share the compiled pattern type. A
token means the same thing in both directions: what `YYYY` writes, `YYYY` reads.
This is the property that makes pairing the directions worth the cut, and it is
enforced by construction rather than by convention, because there is one
vocabulary and one compiler.

### 4. The session zone is captured at bind time

`TO_CHAR` renders a timestamp in the session zone, captured into the parameter
at bind time exactly as ADR 558 does for parsing. The evaluator never reads
planner configuration. `TO_CHAR` over a `Date32` involves no zone, for the same
reason `TO_DATE` does not.

### 5. Omitted fields default when parsing, and are rejected when they cannot

Parsing must construct a whole value, so what a pattern omits has to come from
somewhere:

- Omitted **time** fields default to zero. `TO_TIMESTAMP('2024-01-01',
  'YYYY-MM-DD')` is midnight in the session zone, stated rather than implied.
- Omitted **date** fields have no defensible default. A pattern that cannot
  determine a calendar date — a time-only pattern such as `HH24:MI:SS` — is a
  **binding error** for parsing. Defaulting to the epoch would invent a date the
  user never wrote, and this arc has consistently refused to invent.

The requirement is directional, and deliberately so. Rendering discards what a
pattern does not name, so `TO_CHAR` accepts a time-only pattern happily. Parsing
must construct, so it demands the date fields. One vocabulary, two obligations.

### 6. Rendering never invents precision the value lacks

`TO_CHAR` over a `Date32` with a pattern containing any time token is a
**binding error**. A date carries no time, and rendering `00:00` would
manufacture a precision the value does not have. Both the argument type and the
pattern are known at bind time, so this is caught before evaluation.

This is the mirror of ADR 557's rule that `date + INTERVAL '36 hours'` is an
error rather than a truncation. There the concern was discarding information the
user supplied; here it is inventing information the value never had. Inventing is
the worse of the two.

### 7. `FF9` renders nine digits and parses one to nine

Rendering always emits exactly nine zero-padded digits, so `TO_CHAR` output is
one fixed width rather than a value-dependent one. Parsing accepts between one
and nine digits and scales them as a decimal fraction of a second, so `.5` is
500 000 000 nanoseconds and `.000000001` is one.

The asymmetry is deliberate and preserves the round trip: rendering's nine-digit
output lies inside the range parsing accepts. Requiring exactly nine on the way
in would reject the three- and six-digit fractions real data carries, for no gain.

### 8. Round trips are guaranteed only where the pattern carries the fields

For a pattern containing every field a value needs — year, month, and day for a
date; those plus hour, minute, second, and fractional second for a timestamp —
`TO_TIMESTAMP(TO_CHAR(value, pattern), pattern)` returns the original value,
**provided both calls resolve the same session zone**. The zone is captured per
bind, so this holds within one statement by construction; across sessions with
different zones it does not hold and is not claimed.

A pattern that omits a field cannot round-trip a value whose omitted field is
non-zero, and this record does not pretend otherwise. The guarantee is scoped to
patterns that carry the required fields, and the omitted-field case is asserted
to lose exactly what the pattern does not name rather than to fail confusingly.

## Acceptance

- Every token in the closed set renders and parses, and a value round-trips
  exactly through a pattern carrying all required fields.
- An unknown alphanumeric run is a binding error naming the run, for both
  `TO_CHAR` and two-argument `TO_TIMESTAMP`.
- A double-quoted escape renders verbatim and is matched verbatim, including a
  run that would otherwise be a valid token.
- A non-literal pattern argument is a binding error, not a per-row failure.
- A malformed pattern fails at bind time, before evaluation.
- `TO_CHAR` renders in the session zone: one instant under two session zones
  yields two different strings, and the zone is carried in the bound parameter.
- A pattern omitting a field round-trips a value whose omitted field is zero,
  and loses exactly that field otherwise.
- `TO_CHAR` over a `Date32` carries no zone.
- `TO_TIMESTAMP('2024-01-01', 'YYYY-MM-DD')` is midnight in the session zone.
- A time-only pattern is a binding error for parsing and is accepted for
  rendering, in the same statement, proving the requirement is directional.
- `TO_CHAR` over a `Date32` with any time token is a binding error; the same
  pattern over a timestamp succeeds.
- `FF9` renders exactly nine digits, and parses one, three, six, and nine digit
  fractions to the values a decimal fraction implies.

## Guardrails

The token set is closed, and one table defines it for both directions. A token
added for rendering without a parsing meaning, or the reverse, is the defect this
record exists to prevent; the round-trip control covers every token, so a
one-directional token cannot pass.

The escape control must use a run that is otherwise a valid token. An escape test
over arbitrary letters passes even when the escape is ignored and the letters
happen to be unknown.

The unknown-token control must assert the binding error names the offending run.
A bare "invalid pattern" cannot distinguish a rejected typo from a rejected
escape.

Any architecture guard over the shared token table matches semantic tokens, not
a formatter layout. A table pinned by its rendered shape breaks when an unrelated
token is added and the formatter reflows it, which is a false red rather than a
caught regression.

## Risks and deferrals

Row-valued patterns, `HH12` with meridiem, era and ordinal forms, localized month
and day names, and timezone tokens are deferred. Localized names in particular
carry a locale decision this engine has not made anywhere else.

Compiling at bind time means a pattern's cost is paid once per statement rather
than once per row, which is the intended trade. It also means a pattern cannot be
chosen by data, which is the deferral above and not an oversight.

## Forward

The compiled pattern is the first program this registry stores in a scalar
parameter rather than a value. If a later cut wants row-valued patterns, the
compiler is already separate from the evaluator and can be reused per row; what
that cut must decide is the failure policy, not the vocabulary.
