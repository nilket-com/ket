# Plotting capabilities — consumer contract for ADRs 545, 546, 547

Companion to the three implementation receipts, written for the consumer rather
than the implementer. Everything here is on `main`. Nothing below is a defect;
it is the contract, collected in one place because a consumer would otherwise
have to read four records to assemble it.

## What is available

| Capability | Spelling | Record |
| --- | --- | --- |
| Numeric histogram | `FLOOR(value / width) * width` as a bucket alias, grouped by that alias | ADR 545 |
| Percentiles, global and grouped | `APPROX_QUANTILE(value, q)` | ADR 546 |
| Box plot | the seven-column Tukey relation | ADR 547 |

`FLOOR` and `APPROX_QUANTILE` are stable. Neither needs
`allow_experimental_kernels`.

## The temporal gate — read this before building time histograms

**`TIME_BUCKET(interval, timestamp)` is still experimental** and refuses with
`ExperimentalKernel` unless `allow_experimental_kernels` is set. Numeric
histograms therefore work out of the box; *time* histograms do not.

ADR 545 deliberately declined to add a numeric analogue of `TIME_BUCKET`,
because temporal bucketing carries duration and timestamp semantics that plain
numeric arithmetic does not, and a named numeric operator would additionally
have to decide origin and width policy. That reasoning stands, but it leaves the
temporal path where it was. Graduating `TIME_BUCKET` to stable is separate,
unscoped work. If a time-bucketed histogram is wanted, that is the dependency —
not anything in 545/546/547.

`TIME_BUCKET` also accepts only an `Int64` millisecond timestamp and a duration
string literal.

## Quantile contracts

### Values are stable within an execution, not across executions

The substrate is randomized (Wendelon ADR 83 selects the fully mergeable KLL
construction, whose guarantee requires randomness independent of the data). One
seed is drawn per query execution and shared by every fragment, so a single
request is internally consistent — the same quartiles feed fence computation,
whisker selection, and outlier classification.

Between executions, values may differ within the rank bound. **Do not build
caching, diffing, or snapshot comparison on exact equality across runs.** A box
rendered twice may shift slightly; it will never be internally inconsistent.

### ε = 1% rank error

The declared additive rank error is 1% of the population. This is a *rank*
bound, never a value-distance bound: the answer is an observed value whose true
rank is within 1% of the requested rank. On a small category the visible effect
can be larger than intuition suggests, because 1% of a small population is
still a whole observation.

Below the construction's first compaction threshold every observation is
retained at weight one and the answer is exact.

### At most 1,024 quantile populations per execution

Execution admits at most 1,024 distinct `(group, value expression)` quantile
populations. Beyond that it **refuses with a typed error** rather than degrading
precision, spilling, or silently dropping groups. The cap is enforced as a
count, independently of the memory budget, because the confidence contract is
derived from it.

One sketch is shared per `(group, value expression)` and finalized at every
requested quantile, so q1/median/q3 for a category cost **one** population, not
three. A box plot over 1,024 categories fits; a percentile report grouped by a
high-cardinality column may not.

### Types

`APPROX_QUANTILE` accepts naked `Int32`, `Int64`, `Float32`, `Float64` and
returns nullable `Float64`. Nulls are skipped; an empty or all-null group
returns null. Non-finite floats refuse with a typed execution error. Named
logical types and `Decimal128` refuse at bind time. `q` must be a finite literal
in `[0, 1]`.

## Histogram contracts

`FLOOR` accepts `Int32` and `Int64` (returned unchanged) and `Float32` and
`Float64` (floored toward negative infinity, so `FLOOR(-2.1) = -3`). Nulls pass
through. Non-finite floats refuse at evaluation; `Decimal128`, named logical
types, and a datetime `FLOOR` field refuse at bind time.

Bucketing is authored composition, not a named operator: the caller supplies the
width and therefore the origin, and both stay visible in the query. Width
validation is the caller's — a positive nonzero width is required, and division
by zero keeps its existing typed behaviour.

**Empty bins are absent rows.** The query emits only occupied intervals. A
consumer needing explicit zero-count bins must supply an interval relation and
left join it. Grouping must be by the projected alias; a computed `GROUP BY`
key still refuses.

## Box-plot relation contracts

Seven columns: `category`, `whisker_low`, `q1`, `median`, `q3`, `whisker_high`,
and a nullable `outlier`.

- **One row per outlier observation.** A category with no outliers still emits
  exactly one row, with `outlier = NULL`.
- **Duplicate outlier values remain duplicate rows.** Observation multiplicity
  is preserved; points are not deduplicated.
- The six summary values repeat on every outlier row for that category. This is
  intentional flat denormalization — an array-valued cell would require a nested
  type and a second consumption protocol.
- Whiskers are **observed values**, never the mathematical fence positions.
  Points exactly on a fence are inside. The multiplier is exactly 1.5. A zero
  IQR is not special-cased.
- Rows with a null category or null observation are excluded, so a category with
  no non-null observations has **no** summary row. There is no empty-category
  sentinel.
- Column names are authored by the query. They are the interop contract, not
  reserved keywords.

Toron adds no `BOX_PLOT`, `TUKEY`, `WHISKERS`, or `OUTLIERS` syntax, and does
not recognise this spelling to substitute a special plan. The relation is
ordinary SQL composition over one statement and therefore one snapshot.

## Not a consumer concern

Wendelon ADR 83's remaining CSB storage-migration work — v2 payload, mixed-fleet
fence, rebuild driver — concerns optimizer range-distribution facts, which were
measured to be unreachable in production and are unrelated to the aggregate path
these capabilities use. It does not gate anything here.
