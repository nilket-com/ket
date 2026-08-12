# ADR 546 - Stable grouped approximate quantiles

**Status:** Accepted and implemented 2026-08-11. Consumes Wendelon ADR 83's
aggregate quantile core, which landed in the same cut.

`APPROX_QUANTILE` appears to exist throughout Toron: it has an
`AggregateFunction` variant, SQL lowering, a Rune binding, binder arms, output
schema rules, and a legacy executor test. A live query proves that this is not a
production capability.

```sql
SELECT APPROX_QUANTILE(revenue, 0.5)
FROM default.demo_percent_bars
```

refuses with:

```text
unsupported aggregate projection item
```

and the grouped form refuses with:

```text
experimental aggregate functions are not supported with GROUP BY
```

ADR 028 deliberately excluded `ApproxQuantile` from the stable aggregate
family. That exclusion remains accurate: `StableAggregateKernel`, its metadata,
the grouped accumulator table, and bound global execution have no quantile
state. The remaining legacy arm accepts only Int32, inserts raw signed
big-endian bytes, sorts retained bytes lexically, and selects a rounded retained
index. It orders negative integers incorrectly, ignores every other advertised
numeric type, and is not reached by the production SQL path.

This record graduates one aggregate honestly. It replaces the legacy behavior
with one bounded Wendelon KLL implementation shared by global and grouped
execution. `LinearRegress` remains experimental.

Stable grouped quantiles are useful for percentile reporting and are a
prerequisite for conventional box summaries. They are explicitly **not** by
themselves an unblocker for box plots: Tukey whiskers require a fence pass and
outliers require a variable-length relation. ADR 547 owns that separate query
shape.

## Decision

### 1. `APPROX_QUANTILE(value, q)` joins the stable aggregate catalog

The accepted SQL spelling remains:

```sql
APPROX_QUANTILE(value_expression, q_literal)
```

`value_expression` is one computed aggregate argument under ADR 028. `q` is a
finite numeric literal in the closed interval `[0, 1]`; it is an aggregate
parameter, not a second row-valued argument. Nonliteral, null, NaN, infinite,
and out-of-range parameters refuse during planning with a diagnostic naming
`APPROX_QUANTILE` and the `[0, 1]` requirement.

The stable input domain is naked Int32, Int64, Float32, and Float64. The output
is nullable Float64. Input nulls are skipped; an empty or all-null group returns
null. Named logical values and Decimal128 refuse at bind time rather than
silently losing units or choosing a decimal interpolation rule.

Non-finite Float32/Float64 observations refuse with a typed execution error.
Quantiles require a total numeric ordering; NaN is not silently dropped or
assigned an implementation-dependent rank.

This partially supersedes ADR 028's experimental-kernel exclusion for
`ApproxQuantile` only. All other ADR 028 expression and signature rules remain.

### 2. The parameter is part of aggregate identity

ADR 028's `GroupedAggregateSignature` is currently keyed by
`StableAggregateKernel + Vec<Expr> + distinct`. Simply adding one
`StableAggregateKernel::ApproxQuantile` would make these collide:

```sql
APPROX_QUANTILE(value, 0.25)
APPROX_QUANTILE(value, 0.50)
APPROX_QUANTILE(value, 0.75)
```

The planner-private signature therefore gains a closed parameter key:

```text
None
Quantile(canonical_f64_bits)
```

Validation precedes key construction. Accepted `q` is canonicalized so `-0.0`
and `0.0` name the same quantile; no NaN reaches the key. Equality and hashing
remain planner-private. Public `AggregateFunction`, `Expr`, and `ScalarValue`
do not gain `Eq` or `Hash`.

The parameter also travels with the bound call to state finalization. It is not
encoded as a fake literal root in the row expression program. Output-name
disambiguation from ADR 267 remains authoritative, so three quantiles over the
same value expression receive three distinct internal outputs and preserve
their authored aliases.

### 3. One canonical, bounded KLL state replaces the legacy estimate

The implementation uses Wendelon's repaired quantile substrate. Wendelon ADR 83
establishes that the current `KllSketch` computes no valid quantile — an
ascending median returns 899 where 500 is correct — and this record consumes
that repair rather than the present implementation.

Three properties this record previously asserted are supplied by ADR 83 and are
**not** decided here:

- **The rank-error bound.** `ceil(1_000_000 / k) = 5_000 ppm` is withdrawn. It
  derives from no theorem. ADR 83 now pins the conservative Theorem 3
  derivation, execution-wide confidence, 1,024-state cap and capacity schedule;
  ε remains its one explicit Stage 0 product choice. This record consumes the
  selected tuple and does not restate a different figure.
- **The seed.** A fixed, named compaction seed is withdrawn. The selected
  algorithm is randomized, and its guarantee requires randomness independent of
  the data, which a constant known before the data cannot supply. ADR 83 issues
  one coordinator seed per query execution.
- **Memory.** "At most `k` values plus fixed metadata" is withdrawn; a mergeable
  KLL retains a capacity schedule of lower-level buffers plus sampler state.
  This record consumes ADR 83's accounting.

Consequently the contract offered to consumers is **values stable within one
query execution, including across its fragments, with variation between
executions governed by the declared rank bound** — not byte-identical results
across executions. Changing the configuration, canonical encoding, or quantile
selection rule remains a semantic change requiring evidence.

The parameter is not user-configurable in this cut.

Values enter the sketch through Wendelon's existing order-preserving optimizer
fact encoders and return through matching decoders:

- signed canonical encoding for Int32 and Int64;
- canonical Float32 and Float64 encodings for finite values.

Raw `to_be_bytes()` insertion is deleted. The live negative-value fixture must
make lexical raw-byte ordering produce a different answer, so retaining the old
implementation cannot pass accidentally.

Wendelon gains one canonical `query_quantile(q)` operation over the repaired
sketch. Toron does not reproduce quantile selection by sorting retained values,
and `query_buckets()` is not repurposed as an approximate quantile API.

Selection is by **cumulative level weight**, not by position in a flat sample.
The operation walks retained items in sorted order accumulating each item's
weight 2^level until the accumulated weight reaches the requested population
rank, and returns that item's encoded value. Mapping a population rank into a
retained sample by ratio — treating retained items as equal-weight — is the
defect Wendelon ADR 83 removes, and this record must not reintroduce it at the
query end.

While the sketch has not yet reached the selected construction's first
compaction threshold, every item is present at weight one and the answer is
exact. That threshold is a property of ADR 83's capacity schedule, not the
bare `n <= k` test the previous draft assumed. The advertised error is rank
error, never value-distance error.

The result is converted to Float64 only at finalization. Integer values are
exactly representable only within Float64's usual integer range; the output
contract is the same Float64 contract the existing schema arm already claimed.

### 4. Global and grouped execution use the stable accumulator path

`AggState` gains a quantile state carrying the canonical value family and one
bounded `KllSketch`. Initialization derives the family from the bound argument
type. Fold skips nulls, refuses non-finite floats, encodes one value, and inserts
it. Finalization calls the canonical Wendelon query and decodes one nullable
Float64.

Both bound global aggregation and the grouped aggregate table use that state.
The legacy direct `exec.rs` arm and its permissive test are removed or redirected
through the same producer; there is no second quantile implementation left to
drift.

**One sketch is shared per `(group, value expression)` and finalized at every
requested `q`.** The parameter remains part of aggregate identity for output
naming, planning, and §2's signature key; what changes is that identity no
longer implies a separate accumulator state.

This is a correctness requirement under a randomized algorithm, not the deferred
optimization the previous draft described. ADR 83's all-quantiles guarantee
covers multiple ranks read from **one** sketch. Three independently randomized
sketches for q1, median and q3 are three independent draws, each within the rank
bound but not jointly ordered — they can return q1 > median. That would make
ADR 547's fence arithmetic produce an inverted box and falsify its claim that a
rendered summary is never internally inconsistent.

Sharing the state also removes the sampling cost of three passes. If a
multi-output accumulator shape proves impossible in the stable catalog, the
fallback is one sketch per `(group, value expression)` with shared
population-level randomness, plus an asserted `q1 ≤ median ≤ q3` at
finalization — not three independent states.

### 5. Memory is accounted, not merely bounded in the sketch

One state's footprint is whatever Wendelon ADR 83's memory accounting declares —
a capacity schedule of per-level buffers plus sampler state and fixed metadata,
not `k` values. "At most `k` canonical encoded values" was the old flat
representation's bound and is withdrawn. This record quotes ADR 83's figure
rather than deriving its own.

The grouped aggregate table charges that declared footprint to the existing task
memory budget when a group is admitted. It does not treat heap-owning sketch
state as the size of an enum discriminant. Because one state now serves every
requested `q` for a group, the charge is per `(group, value expression)` rather
than per `(group, value expression, q)`.

Memory admission is necessary but cannot license the confidence claim by
itself. This cut separately declares
`MAX_QUANTILE_STATES_PER_EXECUTION = 1_024`. The count is over distinct
`(group, value expression)` sketches; q1, median and q3 finalized from one
shared sketch count once. The coordinator divides that execution-wide allowance
deterministically across terminal aggregate tasks, and the task allowances sum
to at most 1,024. Unused allowance is not silently multiplied per worker or
recovered by admitting above the execution cap in this cut.

The 1,025th state refuses **before construction** through typed query-budget
exhaustion with a diagnostic naming `MAX_QUANTILE_STATES_PER_EXECUTION` and the
value 1,024. This count gate is enforced even when bytes remain. Conversely, the
ordinary memory gate may refuse below 1,024 when actual charged bytes are
exhausted; that only makes ADR 83's execution-wide union bound more
conservative. Inferring the state count from the byte budget is forbidden,
because the admitted count would then vary with physical value width and heap
layout while the advertised confidence continued to assume a fixed maximum.
This is a user-visible product limit: a grouped percentile query supports at
most 1,024 distinct `(group, value expression)` populations per execution in
this cut. Three requested quartiles over one population still consume one state,
not three.

If admitting another group or growing its sketch exceeds the budget, execution
refuses through the existing typed aggregate-memory path. The implementation
does not lower `k`, spill, silently skip values, or merge unrelated groups to
fit.

### 6. Existing distributed aggregate posture remains authoritative

ADR 531 gathers global aggregates under the distributed posture. Grouped
aggregates exchange on their complete group keys before folding. This record
uses those correctness boundaries; it does not introduce partial/final
aggregation or a serialized KLL exchange state.

Local and multi-worker results must be byte-identical for the same input,
partitioning posture, query, **and execution seed** — that is, within one
execution, across its fragments. Byte-identity is not claimed between separate
executions, which may differ within the declared rank bound, nor across
differing merge sequences: ADR 83 declines to assert a system-wide canonical
merge order because no component establishes one. If the existing plan can split
one logical quantile population across terminal accumulators, the record halts
rather than concatenating approximate answers.

Wendelon's `KllSketch::merge` remains useful substrate and is tested for
equivalence, but no new distributed merge protocol is implied.

### 7. Rune and SQL share the graduated kernel

The existing Rune `approx_quantile` binding resolves to the same stable bound
aggregate and receives the same type, null, parameter, memory, and execution
rules. `allow_experimental_kernels` no longer controls ApproxQuantile and still
controls LinearRegress.

No exact `QUANTILE`, interpolation selector, percentile array, weighted
quantile, `MEDIAN` alias, or window-function form enters this record.

## Sequence

1. Bank the two live production refusals and the negative-ordering defect in the
   legacy executor.
2. Add the parameter identity key and prove three quantiles remain distinct
   before changing execution.
3. Add Wendelon's canonical `query_quantile` with exact small-sample and bounded
   compacted-sample tests.
4. Add the stable metadata, type matrix, and bound accumulator state with
   explicit memory accounting.
5. Route global, grouped, SQL, and Rune paths through the one state; delete the
   legacy estimate.
6. Prove local/distributed correspondence and run the complete battery.

## Acceptance

1. **Prior refusals become real results.** The exact Stage 0 global and grouped
   SQL queries execute without an experimental flag. Their previous
   `unsupported aggregate projection item` and grouped-experimental errors are
   asserted absent.
2. **Parameter identity.** One grouped query requests q=0.25, 0.50, and 0.75
   over the same computed expression with three aliases. All three columns are
   present, distinct, and contain their corresponding values. A deliberately
   skewed fixture makes accidental deduplication visible.
3. **Small exact oracle.** For each accepted physical type, a group that has not
   reached the selected construction's first compaction threshold returns the
   exact nearest-rank observation at q=0, 0.25, 0.5, 0.75, and 1. The threshold
   comes from ADR 83's capacity schedule; `k` unique observations is the
   withdrawn flat-representation test. Negative integers and mixed-sign floats
   are included.
4. **Compaction oracle.** An input much larger than the compaction threshold
   forces compaction. The answer's rank in the exact sorted population is within
   ADR 83's declared bound plus the single-rank discretization allowance. The
   bound is quoted from ADR 83, not restated here; ADR 83 selected ε = 1% from
   its priced Stage 0 table, so this item is executable. The test asserts no
   value-distance bound and proves compaction occurred. Ordered inputs are
   included: ascending and descending arrival are the orderings that falsified
   the previous substrate.
5. **Canonical encoding.** A fixture whose signed raw big-endian byte order
   differs from numeric order returns the numeric answer. Float encodings cover
   negative values, both zero spellings, and positive values. No direct raw-byte
   insertion remains in Toron's aggregate implementation.
6. **Null and invalid matrix.** Nulls are skipped; empty/all-null groups return
   null. Non-finite observations and named/Decimal inputs refuse through their
   stated layers. q outside `[0,1]`, nonfinite q, null q, and a column-valued q
   each produce the stable parameter diagnostic; a finite literal sibling
   passes.
7. **Budget accounting.** A one-group fixture proves the state stays within
   Wendelon ADR 83's declared byte footprint under sustained insertion — not the
   withdrawn "retained values never exceed `k`", which described the discarded
   flat representation. A many-group fixture crosses a deliberately small task
   budget and receives the existing typed memory refusal rather than OOM,
   truncation, or reduced precision. The charge is per `(group, value
   expression)`, since one state serves every requested `q`. An independent
   count fixture admits exactly 1,024 sketches and refuses the 1,025th before
   construction with the cap named; changing physical value width or increasing
   the byte budget does not move that boundary. A distributed control proves
   terminal-task allowances sum to 1,024 rather than granting 1,024 per worker.
8. **Wendelon API authority.** Toron calls `query_quantile`; it never sorts
   `retained_values()` or derives a quantile from `query_buckets()`. Direct
   Wendelon tests cover empty, exact, compacted, and merge-compatible sketches.
9. **Global/grouped and SQL/Rune parity.** Equivalent SQL and Rune queries agree
   for global and grouped fixtures, including computed value arguments.
10. **Distributed correspondence.** Local and at least two-worker execution
    produce identical schemas and values for global and grouped quantiles
    **when given the same execution seed**. Without that qualifier the
    comparison is not well posed under a randomized algorithm. The global plan
    retains ADR 531's gather and the grouped plan proves complete group
    ownership before fold.
11. **One state per group serves every quantile.** A control proves q1, median
    and q3 for a group are finalized from a single shared sketch — not three —
    and that `q1 ≤ median ≤ q3` holds across the corpus, including ordered and
    skewed inputs. Three independent states would satisfy the rank bound
    individually while violating this, which is the failure ADR 547's
    internal-consistency claim depends on being impossible.
12. **Experimental boundary narrows exactly once.** ApproxQuantile appears in
    the stable catalog and no longer consults `allow_experimental_kernels`;
    LinearRegress remains refused without that flag. No other kernel graduates.
13. **No box-plot overclaim.** The receipt states that grouped quartiles alone
    do not supply Tukey whiskers or outlier rows. No box, histogram, or plotting
    dependency appears in production code.
14. **Full battery.** The final tree passes the complete Toron/Wendelon battery
    and current workspace checks, with no ignored or tolerated failures.

## Guardrails and stop conditions

- If Wendelon's retained-sample representation cannot support a documented
  `query_quantile` with the stated rank bound, halt. Do not bless the legacy
  sort-and-index estimate as KLL semantics.
- If KLL heap ownership cannot be charged through the existing aggregate task
  budget, halt rather than ship an unaccounted per-group allocation.
- If the execution-wide 1,024-state admission cannot be divided across terminal
  tasks without any task exceeding the shared total, halt rather than relabel a
  per-task cap as execution-wide confidence.
- If current distributed planning can split one logical population among
  independent terminal quantile states, halt and record partial-state merge or
  a correctness funnel separately.
- If stable signature identity cannot distinguish the quantile parameter
  without public float `Eq`/`Hash` or a fake expression root, halt and revise
  the planner-private key.
- Exact interpolation, Decimal128, logical types, weighted values, and window
  use are scope triggers, not incidental extensions. **Shared multi-quantile
  sketches are no longer among them**: §4 makes one shared state per
  `(group, value expression)` a correctness requirement of this record, not a
  future extension.

## Rejected

- **Enable the existing experimental arm.** It is Int32-only, orders signed raw
  bytes incorrectly, and is not the bound/grouped execution path.
- **Sort `retained_values()` in Toron.** It makes Toron the authority for a
  Wendelon sketch's query semantics and repeats the defect already present.
- **Treat q as an ordinary aggregate argument.** It is constant configuration,
  not one value per input row, and doing so obscures signature identity.
- **One signature for every q over the same expression.** It aliases distinct
  authored results and can return the median in the quartile columns.
- **Exact quantiles under the approximate name.** An unbounded exact sort has a
  different memory and execution contract.
- **Call grouped quantiles a box-plot unblocker.** Conventional whiskers and
  outlier observations remain absent; ADR 547 is the complete prerequisite.

## Consequences

Toron gains stable, bounded percentile reporting across global and grouped
queries, with one algorithm and one advertised rank error. The feature is useful
without plotting and gives the next record quartile values it can trust.

ADR 83 selects ε=1%. At the 1,024-state ceiling, retained quantile payload alone
is at most 43.8 MiB for Int64/Float64 and 21.9 MiB for Int32/Float32. The former
is about 68% of the default 64 MiB task budget before grouped hash-table, key,
batch and sketch metadata. Operators should expect byte-budget refusal below
1,024 for wide or otherwise memory-heavy grouped queries; that does not move the
independent count boundary or weaken the execution-wide confidence claim.

Nothing in this record authorizes a range-whisker/no-outlier box plot. Partial
delivery is progress for Toron users, but it is not a green light for that
Polariton surface.
