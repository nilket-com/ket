# ADR 558 - Row-wise temporal parsing

**Status:** Proposed 2026-08-19.

## Context

Toron can format temporal values but cannot read them. `CAST` covers numeric
conversions only — there is no `Utf8 -> Timestamp` or `Utf8 -> Date32` arm — so
neither `CAST('2024-01-01' AS TIMESTAMP)` nor any spelling of `TO_TIMESTAMP`
works today. A user with a string column holding timestamps has no way to reach
the temporal machinery this registry has spent several cuts building.

The parsing itself already exists, but only as a bind-time literal facility:
`parse_timestamp_text`, `parse_explicit_timestamptz`, `parse_wall_clock_timestamp`,
and `parse_date_ymd` are used when a literal is bound. What is missing is running
them **per row over an array**, where success depends on the content of each
string rather than on a type.

That makes this the first registry family that can fail on data. The channel is
already proven: `SPLIT_PART`'s part-number error and ADR 557's row-valued sub-day
rejection are data-dependent execution errors that propagate through local and
distributed execution. What is new is the need to offer a non-failing spelling,
and that is where the registry runs out of vocabulary.

`ScalarFunctionNullBehavior` has exactly two derivations: `Propagates` makes a
node nullable when an argument is nullable, and `Ignores` makes it
`Some(false)`. Neither can express "may be NULL regardless of input". A `TRY_`
function registered as `Propagates` over a non-nullable column would be declared
non-nullable while producing NULLs, and declared nullability feeds
null-rejection analysis and outer-join demotion. That is the unsafe direction,
so this cannot be evaluator-only knowledge.

## Decision

### 1. Row-wise parsing reuses the existing parsers

`TO_TIMESTAMP` applies `parse_timestamp_text` per element; `TO_DATE` applies
`parse_date_ymd`. No second parsing vocabulary is introduced. A string that
carries an explicit offset is honoured as written; a string without one is a
wall-clock reading. Daylight-saving gaps and ambiguities keep the behaviour the
literal path already has, including their existing error text.

Output types are `Timestamp(Nanosecond, Some("UTC"))` and `Date32`. Both `Utf8`
and `LargeUtf8` inputs are accepted, and the output type does not vary with the
input's offset width.

### 2. Strict and TRY are separate registrations, not a mode

`TO_TIMESTAMP` and `TO_DATE` raise an execution error on an unparseable value.
`TRY_TO_TIMESTAMP` and `TRY_TO_DATE` yield NULL for it. These are four registry
identifiers, following the `CAST` and `TRY_CAST` precedent this codebase already
sets, and matching how `CONCAT` and `||` were split rather than parameterised.

A mode flag was rejected for the reason a flag is always rejected here: the two
spellings have different nullability, so the difference has to be visible to the
binder, not resolved inside an evaluator.

### 3. Fallibility is its own metadata property

Scalar metadata gains a failure property that is **orthogonal** to
`ScalarFunctionNullBehavior`. Input-null handling and failure-induced
nullability are different questions, and folding the second into the first would
distort `Propagates` and `Ignores` for every existing function.

A function declared as yielding NULL on failure is unconditionally nullable, the
way `BoundExprNode::TryCast` already is. `node_nullable` consults the new
property first and falls through to the existing null-behaviour derivation when
a function cannot fail. Strict `TO_TIMESTAMP` keeps ordinary `Propagates`
nullability, because it never returns NULL for an input it rejects — it fails
the statement instead.

### 4. The session zone is bind-time data carried in the payload

`TO_TIMESTAMP` resolves the session timezone at bind time and stores it in its
scalar parameter, the way `EXTRACT` already carries its resolved zone. The
evaluator reads the zone from the bound node and never consults planner
configuration, so every fragment, worker, and retry parses against the same
zone. This keeps the bound plan as the propagation boundary that ADR 556
established.

`TO_DATE` carries no zone. A calendar date is zone-independent, and inventing a
zone for it would make the same string parse to different dates in different
sessions.

### 5. Format patterns are out of scope

Only the single-argument forms are registered. The two-argument
`TO_TIMESTAMP(text, format)` needs a format-pattern vocabulary, which is the
same vocabulary `TO_CHAR` needs to render. Deciding it once, in the cut that has
both directions in front of it, is better than deciding half of it here.

## Acceptance

- A strict parse failure surfaces as an execution error naming the offending
  value, and fails the statement rather than producing a row.
- The same value under `TRY_TO_TIMESTAMP` and `TRY_TO_DATE` yields NULL, in a
  batch that also contains parseable rows, proving the failure is per element.
- A `TRY_` node over a non-nullable input is declared nullable; a strict node
  over a non-nullable input is not.
- A zone-less string parses against the session timezone: one string under two
  session zones yields two different instants. A string with an explicit offset
  yields the same instant under both.
- A distributed fragment parses against the coordinator's resolved zone, not its
  own configuration.
- `Utf8` and `LargeUtf8` inputs both parse and produce the same result for the
  same text.
- `TO_DATE` is unaffected by the session timezone.
- A daylight-saving gap keeps the error the literal path already produces.

## Guardrails

The zone control must use one instant that differs between the two zones — a
string that parses identically under both proves nothing about whether the zone
was consulted.

The `TRY_` control must place a failing and a succeeding value in the same
batch. A batch that fails entirely cannot distinguish per-element NULL from a
whole-array fallback.

The new failure property is metadata, and `node_nullable` is its only consumer
for nullability. No evaluator may compensate for a wrong declaration, and no
second nullability path may be introduced beside it.

## Risks and deferrals

Row-wise parsing is materially slower than the numeric casts the evaluator
already runs, because each element takes a parse. This record does not add a
fast path; if profiling demands one, it belongs in a later cut with its own
measurement rather than as an untested assumption here.

`TO_CHAR`, format patterns, `TO_TIMESTAMP` from an epoch number, and
`Utf8 -> Timestamp` support inside `CAST` and `TRY_CAST` are deferred. The last
of those is worth revisiting once this family exists, so the two spellings do not
diverge.

## Forward

The failure property introduced here is the fallibility field ADR 553 deferred
as speculative. It arrived when a function needed it rather than ahead of need,
which is the pattern this registry has followed for the keyword payload, the
null-ignoring behaviour, and per-statement evaluation. Constant folding is the
next plausible consumer: a function that can fail must not be folded
speculatively, and that rule now has a field to read.
