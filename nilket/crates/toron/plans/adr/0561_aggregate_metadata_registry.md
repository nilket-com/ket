# ADR 561 - Aggregate metadata registry

**Status:** Proposed 2026-08-19.

## Context

ADR 553 gave scalar functions a registry: one admission point, and every
planner-wide rule reads metadata through a canonical identifier. Aggregates never
got one. They have a partial seam — `aggregate_kernel::stable_spec_for_function`
carries `sql_name`, `output_name`, `arity`, `input_domain`, `output_rule`,
`nullable`, and `grouped_supported` — and everything else is decided by matching
on `AggregateFunction` at the point of use.

ADR 560 measured that cost. Adding four spellings touched roughly **thirty
consumer sites**: sixteen matching `AggregateFunction` and about fourteen
matching `AggState`, across the binder, three executor paths, the window path,
the planner, and join analysis. Five architecture guards fired, four of them
exact-count classification gates. Three defects were introduced and caught by the
codebase rather than by review — a `#[derive]` detached from `AggState`, an
array-order-versus-discriminant mismatch caught by its own `debug_assert`, and a
`Decimal128` scale bug that only an acceptance control surfaced. The `exec` root
line cap was raised purely to hold hand-written variant lists.

The tax is not typing. It is thirty opportunities to classify a function wrongly,
and the last cut took three of them.

The decisions consumers currently re-derive:

| Decision | Sites | Current shape |
| --- | --- | --- |
| Logical-type propagation | binder | `MIN`/`MAX` preserve, `SUM`/`AVG` additive-unit, rest naked |
| Order sensitivity | join analysis | `SUM`/`AVG` conditional on float input, rest fixed |
| Window eligibility | planner **and two** executor paths | the same set enumerated three times |
| State kind | three init sites | which accumulator a function starts in |
| Experimental gating | binder | only `LINEAR_REGRESS` |
| SQL aliases | ad hoc | added per-cut, most recently for `VARIANCE`/`STDDEV` |
| Rune name | nowhere | aggregates carry none, unlike scalars |

Window eligibility enumerated in three places is the clearest case: one fact with
three chances to disagree.

## Decision

### 1. One registry carries every planner-wide aggregate property

The kernel spec grows to cover the decisions above: logical-type propagation,
order sensitivity, window eligibility, state kind, experimental gating, SQL
aliases, and a Rune name. A consumer reads metadata; it does not match on the
identifier to re-derive a property the registry already knows.

`nullable` is split into **`nullable_for_nonempty`** and
**`nullable_for_empty`**. It currently conflates "this function can answer NULL
for a non-empty group" with "an empty group answers NULL", which are different
facts and are separately true of different functions — `COUNT` is never NULL for
either; `VAR_SAMP` is NULL for a one-row group and for an empty one; `MIN` is
NULL only when the group is empty or every value is.

The names are deliberately unabbreviated. A single `nullable` is exactly the kind
of field a consumer reads for the question it did not answer, and ADR 560's
degenerate-count rules are where that would first go wrong.

### 2. Identity stays separate from payload

`AggregateFunction::ApproxQuantile(f64)` keeps its quantile. That value is **call
data**, exactly as a scalar keyword parameter is, and forcing it into metadata
would make the registry key depend on a value rather than on which function was
called.

The registry is keyed on a dataless kind. One centralized mapping turns an
`AggregateFunction` into that kind, and it is the only place the correspondence
is written down. This is the aggregate form of ADR 553's identity-plus-payload
split, not a workaround for it.

### 3. Resolution is name plus arity, never name alone

`COUNT(*)` and `COUNT(expr)` are distinct registry entries sharing one SQL name
and differing in arity and input domain. A lookup therefore takes a name **and**
an argument count. Treating a name as a key would make `COUNT` ambiguous and
would silently pick whichever entry happened to come first.

Aliases resolve to a canonical name before this lookup, so an alias cannot
acquire a different arity rule than the spelling it stands for.

### 4. Metadata names the state; the executor owns the computation

The registry records **which** accumulator shape a function uses. It does not
record how that accumulator updates or finalizes. This mirrors
`ScalarFunctionEvaluator`, which names a computation in metadata while the code
lives in the executor.

The distinction is load-bearing. Collapsing accumulation into metadata would hide
that Welford's update genuinely differs from a running sum — an algorithmic fact
that belongs where it can be read and tested, not flattened into a table. The
registry removes classification drift; it does not pretend the kernels are
interchangeable.

### 5. Consumers migrate to reads, and the guards follow

Each hand-written match that re-derives a registry property becomes a metadata
read. The exact-count guards that today assert a variant count become assertions
that the registry is **complete** — every identifier has an entry, and every
entry is reachable from a name-and-arity lookup.

## Acceptance

- Every `AggregateFunction` maps to exactly one registry kind, and the mapping is
  exhaustive: adding a variant without an entry fails to compile or fails a
  guard, not a query.
- `COUNT(*)` and `COUNT(expr)` resolve to different entries, and a lookup by name
  alone is not expressible in the API.
- `VARIANCE` and `STDDEV` resolve to the sample entries through the alias table,
  and an alias inherits its target's arity rule.
- Window eligibility has exactly one source: the planner and both executor paths
  read the same property, and a function made window-eligible becomes so in all
  three without further edits.
- Order sensitivity, logical-type propagation, and experimental gating are read
  from metadata at every site that previously matched.
- `nullable_for_nonempty` and `nullable_for_empty` are separately readable, and a
  function differing on them — `VAR_SAMP` against `COUNT` — is registered and read
  correctly at both.
- No two canonical entries share a `(sql_name, arity)` pair, and an alias
  resolves to an existing pair rather than creating one.
- `ApproxQuantile`'s quantile remains on the call, not in metadata: two calls
  with different quantiles share one registry entry.
- Every entry carries a Rune name, and the Rune surface exposes it.

## Guardrails

The identifier-to-kind mapping is one function. A second mapping, or a consumer
that pattern-matches the identifier to recover a property, is the defect this
record exists to prevent.

**Every canonical `(sql_name, arity)` pair is unique**, and that uniqueness is
asserted rather than assumed. Aliases may point at an existing pair; they may not
introduce a new one. Without this, a duplicate row stays reachable and resolution
silently depends on which entry the table lists first — the same
order-dependence that made `STABLE_AGGREGATE_KERNELS` return the wrong spec
during ADR 560, caught there only by a `debug_assert` someone had written for it.

Completeness is guarded by an exact set, not a count. A count tells you something
changed; the set tells you what, and forces the new entry to be classified in
review — which is how ADR 553's registry catches new scalar functions today.

Accumulate and finalize stay in the executor. A registry field that encodes *how*
a kernel updates has crossed the line this record draws.

## Risks and deferrals

Partial aggregation remains absent: Toron exchanges rows and aggregates once, so
no merge step exists and the registry does not describe one. If partial
aggregation lands, mergeability becomes a per-kernel property and this registry
is where it belongs — but inventing the field now would be speculative.

`COVAR_*`, `CORR`, and `REGR_*` are deferred. They take two inputs, which the
current arity model does not express beyond `Zero` and `One`.

The migration touches every consumer ADR 560 touched. That is the point, and it
is the last time those sites should need editing for a new aggregate.

## Forward

`STRING_AGG` is the first validation case, and deliberately so: it needs an
ordering clause and a separator, so it exercises call payload, a new state shape,
and a NULL rule at once. If registering it requires editing a consumer, the
registry is incomplete and the gap is visible immediately rather than three cuts
later. `MEDIAN` and `PERCENTILE_CONT` follow, once their exactness question is
settled independently.
