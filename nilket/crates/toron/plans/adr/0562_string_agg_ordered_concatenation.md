# ADR 562 - STRING_AGG ordered concatenation

**Status:** Proposed 2026-08-19. The first validation case ADR 561 named.

## Context

Grouped results often need labels, tags, or names combined into one value, and
the engine has no way to say it. `STRING_AGG` is the remaining high-friction
reporting gap after the scalar arc closed, and ADR 561 designated it the first
registration to validate the aggregate registry: it needs a separator, an
ordering clause, a new accumulator shape, and a NULL rule at once, so any
classification consumer it forces open is a registry gap surfacing immediately.

Two grounding facts shape the design. `LogicalPlan::Aggregate` carries
`(AggregateFunction, Vec<Expr>)` — there is no ordering slot, and widening that
tuple would touch every aggregate constructor for one function's need. And the
executor's `GroupValue` orders floats by canonical bits, which is correct for
grouping equality and wrong for sorting, so ordering keys need their own
totally-ordered representation.

The parser accepts both spellings already: sqlparser 0.62 carries
`FunctionArgumentClause::OrderBy` for the in-call form
`STRING_AGG(x, ',' ORDER BY y)` and `Function.within_group` for the standard
`LISTAGG … WITHIN GROUP` form.

## Decision

### 1. The ordering clause is required, not optional

`STRING_AGG(value, separator ORDER BY key [ASC | DESC], …)` is the accepted
form. A call without `ORDER BY` is a binding error naming the reason: an
unordered concatenation publishes row-arrival order, which under exchange is
nondeterministic run to run.

This is stricter than the engines the spelling comes from, deliberately. The
float-sum nondeterminism this engine already admits is epsilon-scale; unordered
`STRING_AGG` is wholesale, visible reordering presented as a result. An
optional unordered form is a deferral with a name, not an oversight — admitting
it later is a relaxation, which is the safe direction to move.

Ties still resolve in arrival order, as they do everywhere, so the registry
entry stays `order_sensitivity: Sensitive` — the clause makes the common case
deterministic; it does not make ties deterministic, and the classification
tells the truth about that.

### 2. Identity plus payload, in the aggregate form ADR 561 established

`AggregateFunction::StringAgg(Box<StringAggCall>)`, where the payload carries
the **non-expression** call data: the separator and the per-key sort
directions. The value and the ordering-key expressions travel as ordinary
arguments — `args[0]` is the value, `args[1..]` are the keys — and bind through
the normal expression program, one root each.

This mirrors the scalar keyword-payload doctrine exactly: expressions in the
child channel where binding, typing, and traversal already work; grammar
choices in the payload where identity, equality, and serialization carry them.
The registry stays keyed on the dataless kind, and
`function_for_payloadless_kind` answers `None` for it, as it does for the
quantile.

### 3. The separator is a constant, and NULL is not a separator

The separator must bind to a string literal, following the `SUBSTRING`-bounds
and `TO_CHAR`-pattern precedent: a row-varying separator is a decision nobody
has needed, and a constant one lets the finalizer join once without per-row
re-evaluation. An empty string is a valid separator. A NULL literal or a
non-literal expression is a binding error.

### 4. NULL values are skipped; NULL keys order last ascending, first descending

- A NULL **value** contributes nothing — no element and no separator — matching
  `SUM` and `AVG`'s treatment and every engine's `STRING_AGG`.
- A group whose values are all NULL, and an empty group, answer NULL:
  `nullable_for_nonempty: true`, `nullable_for_empty: true`.
- A NULL **ordering key** is admitted and placed last under `ASC`, first under
  `DESC` — the Postgres default. `NULLS FIRST`/`NULLS LAST` syntax is deferred.

### 5. Types: string widths preserved, keys from the ordered set

The value must be `Utf8` or `LargeUtf8`, and the output preserves the value's
width — a `LargeUtf8` column does not silently narrow. The registry gains a
string input domain and reuses `SameAsInput` for the output rule.

Ordering keys must come from the engine's ordered value set. Floats compare by
total order in the accumulator's key representation, not by `GroupValue`'s
canonical bits — that representation exists for grouping equality and would
order negative floats wrongly, which is why the keys get their own.

### 6. The accumulator buffers, and says so

`AggregateStateFamily` gains a `StringAgg` family: per group, a buffer of
`(key values, string)` pairs, sorted at finalization and joined with the
separator. Accumulation stays in the executor; the registry names only the
family.

The buffer is unbounded in this cut and that is stated rather than hidden: a
group's every string and key is held until finalization. The existing quantile
state accounting is the precedent for metering aggregate state, and wiring this
family into that accounting is the named follow-up before `STRING_AGG` is used
against adversarial group sizes.

### 7. LISTAGG is a deferral with its own grammar, not an alias

`LISTAGG` requires `WITHIN GROUP (ORDER BY …)` — a different calling convention,
which is exactly what `sql_aliases` is documented not to carry. It follows the
`DATE_PART` precedent instead: its own parse arm lowering to the same
identifier, in a later cut, with the parser support already present.

## Acceptance

- `STRING_AGG(name, ', ' ORDER BY name)` returns the group's non-NULL values in
  key order, joined; the same call with `DESC` returns the reverse.
- A call without `ORDER BY` is a binding error naming nondeterminism.
- A NULL value contributes neither element nor separator: a group with NULLs
  interleaved equals the same group with them filtered out.
- An all-NULL group and an empty group answer NULL.
- A NULL ordering key places last ascending and first descending.
- A non-literal or NULL separator is a binding error; an empty-string separator
  concatenates without joining text.
- A `LargeUtf8` value yields a `LargeUtf8` result.
- A float ordering key with negative values orders numerically, proving the key
  representation is not `GroupValue`'s bit order.
- Multiple ordering keys sort lexicographically with per-key directions.
- The result is identical across repeated runs of the same distributed plan.
- **The registry validation itself:** registering `STRING_AGG` edits no
  classification consumer — order sensitivity, window eligibility, logical
  type, gating, nullability, and output naming all come from the entry. The
  binder's construction arm, the executor's new family, and the planner's parse
  arm are the expected new code; a change anywhere else is a registry gap and
  fails this cut's purpose.

## Guardrails

The classification-consumer criterion is checked by diff inspection at review,
not by a test that cannot see a diff: the reviewer confirms the consumer files
ADR 561 migrated are untouched except where this ADR names new code.

The tie behavior control must use tied keys and assert only membership and
boundary properties, never a specific tie order — a test pinning tie order
would pass today and flake under distribution.

The registry's exact-set guards force the new kind, entry, state family, and
input domain to be classified in review, exactly as they forced ADR 561's own
additions.

## Risks and deferrals

Unbounded per-group buffering is the real risk and is named in decision 6, with
state-accounting integration as the follow-up gate before adversarial use.
`DISTINCT`, `NULLS FIRST`/`LAST`, an optional unordered form, `LISTAGG`'s
grammar, and a delimiter-escaping or overflow policy are deferred.

## Forward

`MEDIAN` and `PERCENTILE_CONT` remain the last named aggregate work, and they
are a decision about exactness before any registration: an approximate
implementation may not wear an exact name, and `APPROX_QUANTILE` already exists
for callers who want the trade stated.
