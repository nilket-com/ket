# ADR 557 - Date32 arithmetic and CURRENT_DATE

**Status:** Proposed 2026-08-19.

## Context

ADR 0556 made `NOW` statement-stable: one UTC instant is captured per execution,
baked into the bound plan, and rendered into the session timezone at output.
`CURRENT_DATE` cannot reuse that output-rendering path. `Date32` carries no
timezone, so the calendar date has to be decided *with* the session zone at bind
time; deferring it to rendering would render a date that was already wrong.

Both inputs are available at the same bind site. `ExprBindContext` already
carries `statement_timestamp_nanos` and `session_timezone: Tz`, so
`CURRENT_DATE` needs no new plumbing — only a decision about what it produces.

Date32 arithmetic is partly supported today, which is easy to misread as a
single missing feature. Verified in `binder/arithmetic.rs`:

- `Date32 ± CalendarMonthInterval -> Date32` already works;
- `Date32 ± Interval` (fixed duration) is explicitly deferred with
  "use Timestamp arithmetic";
- `Date32 - Date32` falls through to non-temporal arithmetic;
- `Date32 ± integer` was never a temporal path.

The form users actually write, `CURRENT_DATE - 7`, needs the last of those. It
is also the only one that changes what an operator *means* for a type pair that
previously errored. Bundling it with a registration and two type rules would
hide an operator-semantics change inside a convenience cut, so this record keeps
the four decisions separable and separately testable.

## Decision

### 1. CURRENT_DATE resolves at bind time and stays a per-statement node

Binding `CURRENT_DATE` converts the statement's captured instant into the
session timezone, takes the calendar date, and stores the day count as
`ScalarFunctionParameter::CurrentDate { days: i32 }` on a `PerStatement` scalar
node with output type `Date32`. Distributed fragments, workers, and retries
inherit the value through the bound plan, exactly as `NOW` does.

**It must not constant-fold to `Expr::Literal(ScalarValue::Date32(..))`.**
`Expr::is_deterministic` answers `true` for every literal, so folding would
reclassify a per-statement value as pure and license the cross-statement reuse
ADR 0556 exists to prevent. The payload shape keeps the node honest about where
its value came from, and keeps `EXPLAIN` readable.

`CURRENT_DATE` takes no parentheses, matching the parser's value-function form
already used for `CURRENT_TIMESTAMP`. Rune exposes `current_date()`.

### 2. Fixed-duration intervals on a date stay a date, and reject sub-day parts

`Date32 ± Interval` produces `Date32` when the interval is a whole number of
days. Widening to `Timestamp` was rejected: `date + INTERVAL '1 month'` already
yields `Date32`, so widening only for fixed durations would make the result type
depend on which interval flavour the user wrote. Silently truncating a sub-day
interval was also rejected — it discards information the user supplied, and the
existing temporal rules reject rather than coerce.

A sub-day component is rejected on **two** paths, because the interval need not
be a literal:

- **Binding** rejects a constant interval carrying a sub-day component, so
  `date + INTERVAL '36 hours'` fails before execution and names the component.
- **Execution** rejects a row whose column-valued interval carries a sub-day
  component. The binder cannot see that value, so the evaluator must carry the
  same rule and the same error wording.

Neither path may fall back to the other's behaviour: a bind-time-only rule would
let a column of sub-day intervals silently truncate, and an execution-only rule
would defer a knowable error past planning.

### 3. Date difference yields whole days

`Date32 - Date32` produces `Int32` days. This is the difference direction of the
same calendar, and it is what "days since signup" needs on date-typed columns.
It deliberately does not produce an `Interval`: the timestamp difference already
owns that shape, and a day count is the useful answer for a date.

The subtraction is checked. Two `Date32` values at opposite ends of the
representable range differ by more than `i32::MAX` is guaranteed to hold under
wrapping arithmetic, so the difference is computed through a widened
intermediate and converted back with a checked conversion that errors rather
than wrapping.

### 4. A bare integer means days, scoped to Date32 add and subtract

`Date32 ± Int` produces `Date32`, treating the integer as a day count. This is
the only decision here that gives an existing operator a new meaning for an
operand pair that previously errored, so it is scoped as narrowly as it can be:
`Date32` only, `Add` and `Sub` only, integer only. It does not extend to
`Timestamp ± Int`, which stays an error, because a bare integer has no defensible
unit against a sub-second instant.

Both `Int32` and `Int64` day counts are accepted, because an integer literal's
width is a property of how it was written rather than of what the user meant.
The addition is checked in the widened domain and the result converted back to
`Date32` with a checked conversion: a day count that carries the date outside
the representable range is an error, never a wrapped date. This mirrors the
existing temporal-arithmetic overflow handling rather than introducing a second
overflow policy.

### 5. Operand direction follows the operator, and closes an existing gap

`Add` accepts either order — `date + 7`, `7 + date`, `date + INTERVAL '7 days'`,
and `INTERVAL '7 days' + date` all bind. `Sub` requires the date on the left;
`7 - date` stays an error, as subtraction is not commutative.

This also closes a pre-existing asymmetry rather than inheriting it. The calendar
path today has arms for `Timestamp + CalendarMonthInterval`,
`CalendarMonthInterval + Timestamp`, and `Date32 + CalendarMonthInterval`, but
**not** `CalendarMonthInterval + Date32`, so
`INTERVAL '1 month' + signup_ts` binds while `INTERVAL '1 month' + signup_date`
errors. No rule justifies the difference; the constant folder mirrors the same
gap. This cut adds the missing arm so that commutativity is a property of the
operator rather than of which temporal type is involved.

## Acceptance

- `CURRENT_DATE` is the `Date32` calendar date of the statement-captured instant
  after conversion to the session timezone. A fixed instant near a day boundary
  yields different dates under two session zones, and the same date under the
  same zone.
- A distributed fragment and a retry evaluate the same `CURRENT_DATE` value as
  the coordinator, driven from the same captured instant.
- `CURRENT_DATE` binds to a `PerStatement` scalar node, not a literal, and
  `Expr::is_deterministic` returns false for it.
- `date + INTERVAL '7 days'` yields `Date32`; `date + INTERVAL '36 hours'` is a
  binding error naming the sub-day component; a column-valued interval whose row
  carries a sub-day component is an execution error with the same wording.
- `date - date` yields `Int32` days, including a negative result when the right
  operand is later, and a checked conversion error at the range extremes rather
  than a wrapped count.
- `CURRENT_DATE - 7` binds and yields `Date32`; `ts - 7` remains an error. Both
  `Int32` and `Int64` day counts bind, and a day count that leaves the
  representable range is an error rather than a wrapped date.
- `Add` binds in either operand order for integers and both interval flavours,
  including `INTERVAL '1 month' + date` which errors today; `Sub` binds only
  with the date on the left.
- Timestamp arithmetic is unchanged: the ADR 152 and ADR 153 rules keep their
  existing results and error text.

## Guardrails

Each of the five decisions carries its own control. A test that would pass under
a different choice does not discriminate: the interval rule needs both the
bind-time and the row-valued sub-day rejection, the difference rule needs a
negative case and a range-extreme case, the integer rule needs a
`Timestamp ± Int` case proving the widening did not leak, and the direction rule
needs the reversed-operand form for each accepted shape. The `CURRENT_DATE`
zone control must use one instant near a day boundary under two session zones —
an assertion that it returns "today" passes even when the zone is ignored.

Overflow is checked in one place per operation, reusing the existing temporal
overflow error rather than adding a second policy. No path may wrap.

The session-zone conversion belongs at bind time in the binder, beside the
existing `session_timezone` resolution. No evaluator may read a clock or a zone
for this function; the bound plan stays the propagation boundary ADR 0556
established.

## Risks and deferrals

Resolving `CURRENT_DATE` in the session zone means two sessions in different
zones observe different dates for one instant. That is the intended SQL
behaviour and the reason the conversion cannot be deferred to rendering.

`CURRENT_TIME`, explicit precision, and `date ± INTERVAL` with month and day
parts combined are deferred. `AT TIME ZONE` on a `Date32` remains an error.

## Forward

`DATE_ADD`, `DATE_SUB`, and `DATEDIFF` are the named spellings users arrive with,
and they are **not** aliases: their argument conventions differ from
`± INTERVAL`. They follow the `STRPOS` precedent — a distinct
`ScalarFunctionId` sharing an evaluator, with ordering keyed on identity rather
than a spelling. That is a separate cut and should not be folded into this one.
