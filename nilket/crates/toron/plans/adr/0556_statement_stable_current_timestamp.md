# ADR 556 - Statement-stable current timestamp

**Status:** Proposed 2026-08-18. **Implemented and measured 2026-08-18.** `NOW` and
`CURRENT_TIMESTAMP` are the first registry functions whose value is
non-deterministic across statements but stable within one statement.

## Context

The scalar registry currently distinguishes deterministic functions from
`RANDOM`, whose value may change on every evaluation. That boolean is enough
for rewrite safety, but it cannot describe the third SQL behavior: current
timestamp is clock-derived and therefore not deterministic across statements,
yet every occurrence in one statement must denote the same instant.

The executor is deliberately session-stateless. A distributed query is bound
at the coordinator and its `BoundExprNode` programs are serialized into task
specifications. Calling the wall clock from a worker, fragment, retry, or row
evaluation would therefore make one SQL statement observe multiple instants.
The session timezone is a separate bind-time concern: it remains planner
configuration and result rendering metadata, while the instant itself is one
UTC value.

## Decision

### 1. Replace the boolean with an explicit evaluation scope

Scalar metadata carries `ScalarFunctionEvaluation` with three variants:

- `Deterministic` — equal inputs produce a stable value;
- `PerCall` — the value may differ each evaluation, as with `RANDOM`;
- `PerStatement` — the value is fixed for one statement but changes between
  statements, as with `NOW`.

`Expr::is_deterministic` remains conservative: only `Deterministic` functions
are pure for optimizer rewrites. This is a semantic distinction, not a license
to duplicate a per-statement expression across independent statements.

### 2. Capture once at execution entry and bind it into the plan

`ExecutionContext` captures one UTC epoch-nanosecond timestamp when the query
execution context is created. The coordinator passes that value to the binder
for every bind and retry belonging to that execution. Binding `NOW` or
`CURRENT_TIMESTAMP` creates a `PerStatement` scalar node carrying that value as
non-child parameter data.

The bound node produces a UTC `Timestamp(Nanosecond, Some("UTC"))`. It is an
instant, not a session-local wall-clock value. Existing daemon result restamping
continues to render UTC timestamp results in the request's session timezone.

This makes the bound plan the propagation boundary: every local executor,
distributed fragment, worker, and retry evaluates the same serialized instant.
Workers do not read the wall clock for this function, and a rebind caused by a
retry reuses the execution context's original timestamp.

### 3. SQL and Rune surface

`NOW()` is the canonical SQL registry spelling and `CURRENT_TIMESTAMP` is its
SQL alias, including the parser's no-parentheses value-function form. Rune
exposes `now()` through the same registry identifier. Both spellings have
niladic arity and the same timestamp output type.

The v1 cut does not add `CURRENT_DATE`, `CURRENT_TIME`, precision arguments, or
interval arithmetic. Those require separate type and calendar decisions.

## Acceptance

- `NOW()` and `CURRENT_TIMESTAMP` bind to one typed registry identifier;
  `CURRENT_TIMESTAMP` and `CURRENT_TIMESTAMP()` are both accepted.
- Two NOW occurrences in one projection return the same timestamp, including
  when the projection is evaluated in multiple batches.
- A new execution context receives a new capture, while a fixed test context
  makes the result exact and reproducible.
- A distributed task context/plan preserves the captured value; no worker or
  retry reads its own clock.
- `Expr::is_deterministic` rejects NOW and accepts ordinary deterministic
  functions, while `RANDOM` remains per-call non-deterministic.
- Session timezone rendering continues to apply to the UTC instant without
  changing its ticks.

## Guardrails

The registry, binder, bound-expression serializer, evaluator, SQL planner, and
Rune registration remain one complete admission path. No special NOW branch may
be added outside those typed seams without a corresponding architecture test.

The statement timestamp is execution data, not `PlannerConfig`: planner policy
must not mint a second clock value, and a retry must not silently change the
meaning of an already-started statement.

## Risks and deferrals

Capturing before binding means a statement's current timestamp reflects
execution entry rather than the instant its first row is evaluated. That is the
chosen statement-time boundary and is stable under planning delay and
distributed scheduling. Nanosecond epoch values use Arrow's existing timestamp
range; dates outside that representable range are not a supported execution
context.

`CURRENT_DATE`, `CURRENT_TIME`, explicit precision, and interval arithmetic are
deferred. They should reuse the captured instant rather than introduce another
clock, but each has additional SQL type and timezone semantics.

## Forward

The next temporal cut, if demanded, is to decide whether current-date/time
forms are aliases or distinct typed functions. It must not weaken the invariant
that one statement has one captured clock value.
