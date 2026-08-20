# ADR 571 - Non-aggregate scalar subqueries

**Status:** Proposed 2026-08-20.

## Context

`(SELECT x FROM t WHERE …)` in a value position is refused today with
"non-aggregate scalar subqueries are not supported" — the machinery for
aggregate scalars exists (ADR 257's keyless Cross broadcast, ADR 260's keyed
decorrelation with a LeftOuter recombine), but nothing enforces the
standard's at-most-one-row contract at runtime, and admitting the spelling
without the contract would silently truncate.

## Decision

### 1. The runtime contract, stated once

Zero rows yields NULL. One row yields its value — including a NULL value,
which is distinct from the zero-row case only in how it arose. More than one
row **raises**, never truncates. For a correlated subquery the contract holds
**per outer correlation key**: a key with two rows raises even when every
other key is well-behaved, and a key with no rows yields NULL for exactly the
outer rows it correlates with.

### 2. The contract is an aggregate kernel, not a new plan node

The enforcement mechanism is an internal at-most-one accumulator in the ADR
561 registry: state is a row count and the single value; a second row per
group raises with a message naming the scalar-subquery contract. This choice
is what makes the rest inherit rather than duplicate:

- Correlated: ADR 260's path — grouped on the correlation keys, LeftOuter
  recombine — works verbatim with this kernel as the aggregate. Per-key
  assertion falls out of per-group accumulation; NULL-fill for unmatched
  outer rows is the LeftOuter's existing behavior.
- Uncorrelated: ADR 257's shape — a global aggregate on the right of a
  broadcast Cross — likewise, with the aggregate wrapping the **fully
  lowered** subquery plan. Full lowering is what makes `ORDER BY … LIMIT 1`
  inside the subquery valid: an author who bounds the subquery to one row
  has satisfied the contract, and the plan says so.

No `ExecutionPlan` variant is added; no traversal census is paid.

### 3. The kernel is internal, and the internal namespace is closed

The kernel registers as `__SCALAR_SUBQUERY_VALUE` for registry-invariant
purposes only. SQL text cannot reach it: the aggregate lowering refuses any
double-underscore function name as reserved for the engine, with its own
pinned message — the guard that keeps "internal" true rather than asserted.
Both WHERE and SELECT positions route through the one pull-up, so the two
surfaces cannot diverge.

## Acceptance

- Uncorrelated 0/1/2 rows: zero → NULL (distinct from an error), one → the
  value (and a one-row NULL value → NULL), two → an execution error naming
  the scalar-subquery contract — never a silently truncated result.
- Correlated with per-key variance: one key with two rows raises; with that
  key filtered away, the remaining keys produce their values and an
  unmatched outer row produces NULL — the assertion is per key, not
  whole-result.
- `(SELECT x FROM t ORDER BY y LIMIT 1)` is valid and returns the bounded
  row's value.
- SELECT/WHERE parity where both are expressible: the same subquery as a
  projected value and as a comparison operand produces consistent results
  through one lowering.
- `__SCALAR_SUBQUERY_VALUE(x)` in SQL text is refused as reserved; the
  registry's uniqueness and completeness suites accept the internal entry.

## Guardrails

The kernel's raise-on-second-row is the contract; an implementation that
caps, samples, or keeps the first row is the truncation this record exists
to forbid, and the 2-row control fails it. The double-underscore refusal is
exact-message pinned.

## Risks and deferrals

Correlated subqueries keep ADR 260's admission boundary (equi-correlation in
the WHERE; no ORDER BY/LIMIT in the correlated form — the keyed decorrelation
has no per-key ordering machinery, and admitting it is its own decision).
Row-valued subqueries (`(SELECT a, b …)`) stay rejected: one column only.
Boolean subquery expressions (EXISTS/IN in projections) remain the named
follow-on this record does not open.

## Forward

With scalar subqueries closed in both positions, the remaining structural
gap is SELECT-list boolean subqueries — which would also make ADR 568's
UNKNOWN directly projectable.
