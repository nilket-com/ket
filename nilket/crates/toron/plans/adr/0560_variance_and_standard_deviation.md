# ADR 560 - Variance and standard deviation

**Status:** Proposed 2026-08-19. Closes ADR 552's named follow-on.

## Context

ADR 552 left `stddev` and `variance` open, describing the blocker as "a
mergeable, numerically stable variance across partitions". Grounding that phrase
splits it in two, and only one half is live.

**Toron does not merge aggregate state across partitions.** The pipeline is
exchange-then-aggregate: `prepare_aggregate_input_exchange` shuffles rows by
group key, `collapse_redundant_pre_aggregate_exchange` drops that shuffle only
when the input is already partitioned on the key, and
`gather_unproven_global_aggregate_input` pulls a global aggregate to one
partition. `ExecutionPlan::Aggregate` carries an input, a group program, calls,
and a schema — no partial-or-final mode. `BoundAggregateCall` carries a function
and its arguments — no state. There is no `merge_state`, no partial-aggregate
plumbing, and no test vocabulary for either. `outer_count_preaggregation` is a
binder plan rewrite, not state merging.

So every row of a group reaches one accumulator, and **mergeability is not a
prerequisite for this cut**. What remains is numerical stability inside that one
accumulator, which is a real concern rather than a theoretical one: the textbook
`sum(x²) − n·mean²` loses catastrophic precision exactly where this engine is
pointed — large-magnitude, low-variance columns like timestamps-as-numbers and
money.

Aggregates also lack the metadata registry scalar functions received in ADR 553.
There is a partial seam, `aggregate_kernel::stable_output_field`, and five
`unreachable!("stable aggregate metadata missing")` sites that mark what it must
cover. This record extends that seam; it deliberately does not build an
aggregate registry, which is its own decision.

## Decision

### 1. Welford's online algorithm, as a decision rather than an implementation detail

The accumulator keeps `(count, mean, m2)` and updates them per value. This is
chosen for numerical stability, not for elegance: the naive sum-of-squares form
is accurate enough on small synthetic data and wrong on the data this engine
actually serves, so the choice cannot be left to whoever writes the loop.

Accumulation happens over `Float64` regardless of the input's numeric type. A
variance is fractional even when every input is an integer.

### 2. Four explicit spellings, and the bare names say which they mean

`VAR_POP`, `VAR_SAMP`, `STDDEV_POP`, and `STDDEV_SAMP` are the registrations.
The SQL standard defines exactly these four and does **not** define bare
`VARIANCE` or `STDDEV`.

The bare names are admitted as SQL aliases of the sample forms, for
compatibility with the established convention across engines a caller is likely
to have used. That mapping is recorded here rather than left to folklore:
`VARIANCE` is `VAR_SAMP` and `STDDEV` is `STDDEV_SAMP`. Choosing the other
mapping would silently differ by a factor of `n/(n − 1)` — a discrepancy small
enough to escape notice and large enough to matter.

`STDDEV_*` is the square root of the corresponding `VAR_*`, computed from the
same accumulator rather than by a second pass.

### 3. Degenerate counts answer NULL rather than zero or an error

- `VAR_SAMP` and `STDDEV_SAMP` return NULL when the count is 0 or 1. The sample
  forms divide by `n − 1`, and one observation carries no information about
  spread. NULL is the standard's answer and the honest one.
- `VAR_POP` and `STDDEV_POP` return NULL when the count is 0, and 0 when the
  count is 1. A single observation is a population with no spread.

Returning 0 for the sample forms at n = 1 was rejected: it is indistinguishable
from a genuinely constant column, which is a different fact about the data.

### 4. Decimal narrows to the accumulator, and non-finite values propagate

`Decimal128` is an accepted input and is converted to `Float64` on the way into
the accumulator. The result is `Float64`, not a decimal. This is a deliberate
narrowing and differs from `SUM`, which preserves `Decimal128` and emits a
`Decimal128Array`: a sum of exact decimals is exact at the same scale, while a
variance divides and takes a square root, so no decimal scale is the right one.
Callers who need exact decimal spread are better served by an explicit `CAST`
than by an aggregate that pretends a scale it cannot honour.

A NaN or infinite input propagates: the accumulator carries it through and the
result is non-finite, rather than an error or a skipped value. Aggregates have no
non-finite rejection today — the only NaN handling is group-key
canonicalisation — so propagating keeps these four consistent with `SUM` and
`AVG` rather than introducing a policy only variance obeys.

### 5. NULL inputs are skipped, not counted

A NULL value does not contribute to the count, the mean, or `m2`, matching `SUM`
and `AVG`. A group of entirely NULL values therefore has count 0 and answers
NULL by the rule above.

## Acceptance

- A column of large-magnitude, low-variance values yields the correct variance,
  and the naive sum-of-squares form does not. The control must use data where the
  two disagree, or it does not test the decision.
- `VARIANCE` equals `VAR_SAMP` and `STDDEV` equals `STDDEV_SAMP` on the same
  input, and both differ from their `_POP` counterparts on that input.
- `STDDEV_POP` is the square root of `VAR_POP`, and likewise for the sample pair.
- A one-row group answers NULL for both sample forms and 0 for both population
  forms.
- An empty group and an all-NULL group both answer NULL for all four.
- NULL values are skipped: a group with NULLs interleaved yields the same result
  as the same group with them removed.
- All four spellings work grouped and ungrouped, and through the distributed
  posture where the group's rows arrive after an exchange.
- The integer case yields a fractional result rather than truncating.
- A `Decimal128` input yields a `Float64` result computed at the column's
  declared scale. The reader hands the accumulator an unscaled integer, so a
  spread that ignored the scale is wrong by `10^(2·scale)` — the control asserts
  the scaled value rather than merely the output type.
- A NaN or infinite input yields a non-finite result rather than an error or a
  silently skipped row, and a NULL in the same group is still skipped — the two
  are distinct.

## Guardrails

The stability control is the one that decides whether this record was worth
writing. It must compare against a known-correct expected value on data where the
naive form visibly fails — not against the naive form's own output, and not on
data small enough for both to agree.

`aggregate_kernel::stable_output_field` must cover all four spellings. The
`unreachable!("stable aggregate metadata missing")` sites exist to catch a
function admitted without metadata, and adding one to those match arms without
covering it in the seam would convert a compile-time guard into a runtime panic.

## Risks and deferrals

If a later cut introduces partial aggregation, these four aggregates need a merge
step, and the answer is already known: combine `(count, mean, m2)` triples with
Chan's parallel formula rather than re-deriving from sums. That is recorded here
so the future cut discovers it in the ADR rather than in a wrong result.

**The unbound executor paths reject rather than compute, deliberately.** The
global and grouped unbound aggregate paths answer `NotImplemented` for all four
spellings. The planner always produces bound aggregate calls, so those paths are
unreachable for a query; rejecting keeps them from acquiring a second, untested
Welford implementation that could drift from the bound one. If a caller is ever
found reaching them, that is a planner defect to fix rather than a reason to
duplicate the accumulator.

`COVAR_POP`, `COVAR_SAMP`, `CORR`, and `REGR_*` are deferred. They share the
moment-accumulator shape and would reuse this decision, but each adds a second
input column and its own NULL-pairing rule.

An aggregate metadata registry, matching what ADR 553 gave scalar functions, is
deferred and is the obvious next structural cut if the aggregate surface keeps
growing.

## Forward

`STRING_AGG` and the `MEDIAN`/`PERCENTILE_CONT` exactness question are the
remaining named aggregate work. Neither reuses this accumulator: one is an
ordered concatenation and the other is a decision about whether an approximate
implementation may wear an exact name.
