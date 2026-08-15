# ADR 552 - Relation-side kernel density

**Status:** Implemented 2026-08-15. The σ/variance follow-on remains open.

## Context

Ket asks Toron for a density capability so the violin mark can follow the standing
rule that marks render and relations compute. The request is recorded in
`plans/wip/toron-density-capability-request.md`; this record answers it and owns
the engine semantics the request explicitly declined to decide.

Polariton computes the density today in `violin.rs`, and the parameter that
controls it is not derived from data:

```rust
let bandwidth = (y_max - y_min).max(0.001) * 0.03;
```

`y_min` and `y_max` are hoisted above the per-series loop, so every series in a
figure is smoothed by three percent of the **figure's** range. A tight
distribution and a diffuse one receive identical smoothing, and adding one outlier
to any series rescales the smoothing of every other. Relocating that expression
into the engine unchanged would move a fabrication rather than remove one, so the
substantive content of this record is the bandwidth rule, not the relocation.

The precedent is ADR 547's Tukey summary: a summary *relation* the mark consumes,
not a function returning an opaque value. ADR 546's stable grouped approximate
quantiles are the statistic this record builds on.

ADR 547 also establishes a composition preference: its canonical query is ordinary
SQL composition and it deliberately adds no box-plot function, array aggregate,
renderer, or plot-specific server type. That preference does not make density a
composition cut. Toron currently has neither the math scalars a Gaussian kernel
needs (`exp` and a fifth-root/power operation, among others) nor a series/grid
generator for the shared evaluation points. ADR 547 could compose because its
summary used primitives already present; composing this capability would first
require several unrelated primitive families and would force each caller to
re-author the bandwidth rule. A Rust-side relation kernel is therefore the
smaller, more enforceable capability: it keeps the single bandwidth expression
owned here while adding no new SQL surface.

## Decision

### 1. Bandwidth is quantile-derived, and this cut does not add σ

The bandwidth for each group is computed from that group's own observed spread:

```text
h = 0.9 * (IQR / 1.349) * n^(-1/5)
```

`IQR` and `n` come from that group alone, never from the figure or from any other
group.

**This is the decision the request left open, and it is settled here rather than
during implementation.** Silverman's rule as usually stated needs
`min(σ, IQR/1.349)`, and Toron has no σ: `stddev` and `variance` are absent from
the aggregate registry, and adding them is not a small change in a distributed
engine — a mergeable, numerically stable variance across partitions is its own
surface with its own controls, and folding it in here would hide that work inside
a density record.

The quantile-only form reuses ADR 546's grouped quantiles, which are already
stable under grouping, and it is strictly better than what it replaces: the
smoothing responds to the data. Choosing `min(σ, IQR/1.349)` over `IQR/1.349`
narrows the bandwidth only when a distribution is lighter-tailed than Gaussian;
the cost of not having it is over-smoothing in that case, not a wrong shape.

**σ is a named follow-on, not a silent gap.** If over-smoothing on light-tailed
series proves material in practice, `stddev`/`variance` and the full
`min(σ, IQR/1.349)` form are a separate record. Nothing here forecloses it — the
rule is stated in one place so a later record replaces one expression.

### 2. Zero interquartile spread falls back within the same statistic

`IQR / 1.349` is zero when more than half a group's values are identical, even
where the tails carry real spread. Rather than refuse those groups or reach for a
range-derived constant — which is the defect this record exists to remove — the
fallback stays quantile-derived:

```text
if IQR > 0:   h = 0.9 * (IQR / 1.349)   * n^(-1/5)
else:         h = 0.9 * ((p90 - p10) / 2.563) * n^(-1/5)
```

Both divisors are the Gaussian-equivalent scalings of their spans. If the
`p90 - p10` span is also zero the group has no spread at all and falls under the
degenerate policy below.

### 3. Degenerate groups emit no rows

A group emits no density rows when `n < 2`, or when every value is identical, or
when both spread measures above are zero. A single observation has no density, and
emitting a spike would fabricate a width the data does not support.

Emitting nothing is distinguishable from emitting zeros: the group is absent from
the relation, and a consumer that needs to tell "no density" from "zero density"
can. Consumers must not read an absent group as a flat one.

### 4. The evaluation grid is shared across groups

The extent is the union of finite values across every group in the invocation, and
every group is evaluated on the same grid. A violin plot exists to compare
categories, and per-group extents make the comparison misleading by giving each
violin its own axis. This grants the request's stated preference, on its stated
reasoning.

The grid point count is authored, with a default of 128 in the Rune convenience
form. It is a property of the relation, not of any texture: the current
renderer's 256 bins are a rendering constant and must not become the engine's.

### 5. The kernel is Gaussian and fixed

Bandwidth dominates kernel choice for any reasonable kernel. An authored kernel is
a parameter with no demonstrated demand; it can be added later without disturbing
the shape decided here.

### 6. Non-finite values stay sticky, per ADR 022

A group containing a non-finite value produces non-finite density for that group,
following the sticky-NaN semantics ADR 022 pinned for float aggregates. The
density path does not invent a second, quieter convention by filtering them out.

Ket's substrate refuses non-finite rows, so the mark declines to render that group
rather than rendering a fabricated one. That is the intended outcome: a group whose
input was not clean is visibly absent rather than silently repaired.

### 7. The output is a relation

```text
group | evaluation_value | density
```

One row per group and evaluation point, consumed as ordinary rows. Following
ADR 547, not a function returning an opaque blob.

## Acceptance

- Bandwidth responds to the data: equal-cardinality groups with materially
  different spread produce peak heights in the ratio implied by their separate
  bandwidths. This control fails against today's figure-global constant, and is
  the record's point.
- Adding an outlier to one group leaves every other group's bandwidth unchanged.
  Because the evaluation grid is shared, the observable regression compares
  those groups at common evaluation points; an outlier that extends the union
  extent may add grid points but must not change the other group's common rows.
- The zero-IQR fallback engages: a group with over half its values identical and
  genuine tail spread receives a data-dependent bandwidth and non-empty rows;
  its peak must distinguish that fallback from today's figure-global constant.
- Degenerate groups emit **no rows**, asserted as absence, not as zeros.
- The grid is shared: every emitted group carries an identical set of
  `evaluation_value`s.
- Non-finite input yields non-finite density for that group and leaves other
  groups finite, matching ADR 022 rather than silently dropping.
- Grouped bandwidth is stable in the sense ADR 546 established for the quantiles
  it rests on.

## Guardrails

No renderer constant enters the engine — not the 256 bins, and not any figure-wide
range. If an implementation finds itself reaching for the extent of all data to
scale a *smoothing* parameter, it has reproduced the defect.

The record decides one bandwidth expression in one place. An implementation that
spreads bandwidth logic across call sites forfeits the follow-on σ change.

## Risks

Quantile-derived bandwidth over-smooths light-tailed distributions relative to the
`min(σ, IQR/1.349)` form. Accepted knowingly; the alternative is an undeclared
dependency on a distributed variance aggregate.

Approximate quantiles carry ADR 546's error bounds into the bandwidth. The
bandwidth is a smoothing parameter rather than a reported statistic, so bounded
error changes smoothness slightly and never moves a rendered value.

Shared extent resolves a narrow group across fewer effective grid points than a
per-group extent would. This is the cost of comparability and is the requested
trade.

## Forward

Ket re-surveys the delivered schema before drafting its substrate — the request
document says so, and names three prior occasions where a proposed shape was read
as a delivered one. This record's column list is a decision, but the types and the
authored spelling are settled at implementation, and ket should read them from the
implementation rather than from here.

`stddev`/`variance` and the `min(σ, IQR/1.349)` bandwidth are the named follow-on,
gated on evidence that over-smoothing is material.
