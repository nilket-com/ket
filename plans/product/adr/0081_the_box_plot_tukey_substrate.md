# ADR 0081: the box plot Tukey substrate

Status: proposed 2026-08-12. This record reshapes the box plot kernel's prepared
data to represent what Toron ADR 547 actually returns — five summary values plus
zero-or-more outlier observations per category — replaces the `min`/`max` names
whose meaning would otherwise change silently, decides outlier geometry and hit
testing, and removes the kernel's second aggregation authority. It is a substrate
cut: no Rune word and no figure member. It makes one narrow colour decision —
§5 — because an outlier point cannot be painted without a fill and stroke
authority; the box body's fill repair stays with the surface record.

## Context

Toron ADR 547's composition was traced against a purpose-built probe relation
(`box_probe`) rather than read from the record. The rows:

| category | whisker_low | q1 | median | q3 | whisker_high | outlier |
|---|---|---|---|---|---|---|
| lowhigh | 30 | 31 | 33 | 34 | 35 | −50 |
| lowhigh | 30 | 31 | 33 | 34 | 35 | 200 |
| none | 20 | 21 | 23 | 24 | 25 | *(null)* |
| twin | 10 | 12 | 14 | 16 | **16** | 99 |
| twin | 10 | 12 | 14 | 16 | **16** | 99 |

Five facts that shape everything below, none of them inferable from the shape
alone:

- **Duplicates survive.** `twin` yields two rows both `99.0`. The outlier column
  is a multiset; deduplicating on ingest destroys an observation.
- **Cardinality is `max(1, outliers)` per category**, and the five summary values
  repeat verbatim on every row of a category.
- **Degenerate categories collapse rather than refuse.** A single observation
  (`single`) and four identical values (`flat`) both return all five summaries
  equal, with no outliers.
- **A whisker may coincide with a box edge on ordinary data.** `twin`'s
  `whisker_high` is `16`, exactly `q3`. Zero-length whiskers are not an edge
  case.
- **`whisker_high` is not `max`.** `twin`'s largest observation is `99`.

### The two hover traps

`BoxStats{min,q1,median,q3,max}` (`plot/kernels/boxplot.rs:180`) is consumed at
seven sites. Two of them change *meaning* rather than value under the rename:

```rust
if data_y >= stats.min && data_y <= stats.max {          // :143
    return Some(format!("{}\nMax: {}\nQ3: {}\nMedian: {}\nQ1: {}\nMin: {}",  // :145
```

Outliers lie *beyond* the whiskers by construction, so a containment test over
the whisker span makes every outlier **unhoverable** — the one datum a reader is
most likely to interrogate. And reporting `whisker_high` under the label `Max`
tells a reader `Max: 16` when the maximum is `99`: a false statement, the same
drawable-but-misread class that refused range-only whiskers.

### What is already banked, and what is not

PRODUCT-0080's oracle banks box plot **paint** — five operations,
`FillRectangle(midpoint)` plus strokes at 1.0/3.5/1.0/3.5
(`plot/mark_style_oracles.rs:129-137`). **Hover is not banked at all**, and hover
is exactly where this record changes meaning. §Implementation sequence banks it
before touching the shape.

The paint bank's fixture constructs a `BoxStats` literal (`:114`), so the shape
change breaks it at compile time — the good failure, and the reason the bank must
be re-expressed deliberately rather than discovered broken.

## Decision

### 1. Prepared data carries a summary and an outlier multiset

```rust
pub struct TukeyBox {
    whisker_low: f64,   // fields are private; see §8
    q1: f64,
    median: f64,
    q3: f64,
    whisker_high: f64,
    outliers: Vec<f64>,
}
```

One `TukeyBox` per category. `outliers` is empty for a category ADR 547 returned
with a null outlier, and holds one entry per outlier row otherwise.

### 2. `min` and `max` do not survive the rename

The fields are `whisker_low` and `whisker_high`, and no alias is retained. This
is the record's central safety property: both surviving consumers of `min`/`max`
would have kept compiling and changed meaning, so the names are removed to make
the change a compile error rather than a silent one.

Hover text says **`Whisker high`** and **`Whisker low`**, never `Max`/`Min`. A
box plot's summary does not carry the extremes, and labelling whiskers as
extremes misreports the data to the reader.

### 3. Outliers are a multiset in producer order

Duplicate values are retained as separate entries. Order within a category is the
order ADR 547's rows arrived.

Collapsing a category's repeated summary rows into one `TukeyBox` is
**denormalization ingestion** — undoing a join's row multiplication. It is not
licence to sort, deduplicate, or otherwise reorder the outliers that
multiplication carried. Two observations at `99` are two observations.

### 4. Coincident outliers separate along the band axis, never the value axis

Two outliers at the same value would paint as one circle: operations preserved,
multiplicity destroyed in the artifact. Hover cannot repair that, because an
exported AVIF has no hover.

Each outlier is therefore drawn at a **deterministic offset along the categorical
band axis**. The value axis position is untouched.

This is safe precisely because a point's position *within* a band carries no
information — the band is the category, and where a mark sits across its width
means nothing. Offsetting along the value axis would be the opposite: for an
outlier, the value *is* the information, and moving it would misreport the datum
to fix a drawing problem.

**Only equal-valued clusters move.** Outliers are grouped by exact value within a
category; a value held by one outlier draws centred on the band, exactly as it
would with no offset rule at all. Distinct nearby values are not spread — they
are already distinguishable on the axis that matters, and moving them would
introduce apparent structure across the band where none exists.

Outliers are grouped into clusters by **finite numeric equality** (`==`), so
`-0.0` and `+0.0` are one cluster: they are the same observation value, and
splitting them on sign of zero would separate two points that must coincide.

For a cluster of `n` outliers sharing a value, the member at cluster index `i`
(counting from zero in producer order) is offset by

```
FOOTPRINT_RADIUS = RADIUS + OUTLINE_WIDTH / 2.0        // 3.0 + 0.5 = 3.5
usable_span      = max(0.0, band_width - 2.0 * FOOTPRINT_RADIUS)
spacing          = if n == 1 { 0.0 }
                   else { min(2.5 * RADIUS, usable_span / (n - 1) as f32) }
offset(i, n)     = (i as f32 - (n - 1) as f32 / 2.0) * spacing
```

The casts are not decoration. `(n - 1) / 2` on integers truncates, so a literal
translation makes an even-sized cluster asymmetric: at `n = 2` the divisor
becomes `0`, giving offsets `0` and `spacing` — one member centred, one to its
right — so the cluster drifts right by `spacing / 2` instead of straddling the
band centre. The arithmetic is floating-point throughout.

The formula is symmetric about the band centre, so a cluster does not drift the
mark sideways; it preserves producer order left to right; and `n = 1` yields
`offset = 0`, so the singleton case is the same rule rather than an exception.

**`usable_span` is what actually contains the marks, and the footprint is wider
than the fill.** §5's outline is stroked *centred on the path*, so a radius-`3.0`
circle at outline width `1.0` occupies radius `3.5`; bounding the fill alone
would let separator ink cross the band edge by half the stroke.

Two errors were corrected here, and both are recorded because each passes a gate
the other fails. Dividing `band_width` by `n` bounded the *centres* rather than
the marks: at `band_width = 10`, `RADIUS = 3`, `n = 2` it gave spacing `5`,
centres `±2.5`, extents `±5.5` against a `±5` band. Subtracting a diameter and
dividing by `n - 1` — the number of **gaps**, not of points — fixed the centres
but still measured the fill: it gave centres `±2.0` and outlined extents `±5.5`.
Only subtracting `2 * FOOTPRINT_RADIUS` yields centres `±1.5` and extents exactly
`±5.0`.

So **complete outlined marks are contained whenever
`band_width >= 2 * FOOTPRINT_RADIUS`**.

Determinism matters beyond taste: the same relation must render identically
across runs and exports, or byte-parity gates and reproducible artifacts both
fail. The offset is a function of the index, never of a random source.

### 5. Outlier points have pinned geometry and a named colour authority

A point cannot be emitted without a fill and stroke authority, so this record
supplies one rather than deferring it — deferring all point styling while
requiring point paint and hit gates would be internally impossible.

| aspect | value |
|---|---|
| shape | circle at the outlier's value, offset per §4 |
| `RADIUS` | `3.0` |
| fill | `mark_style.fill` |
| outline | `mark_style.decoration`, stroke width `1.0` |

Both roles come from PRODUCT-0080's tier, which exists for exactly this. Note the
transient inconsistency this creates and which the surface record ends: the box
*body* still fills from the legacy ramp (`:50`, §10), so within one mark the
outlier points are skin-resolved and the body is not until that repair lands.

### 6. Hover is two different questions

| target | test | reports |
|---|---|---|
| the box | cursor within the band **and** within the whisker span | category, `Whisker high`, `Q3`, `Median`, `Q1`, `Whisker low` |
| an outlier | discrete point hit-test against the drawn point's position and radius | that observation's value |

Outlier hover is **not** a widening of box containment. Widening the span to
include outliers would make the gap between whisker and outlier — which contains
no data — report the summary, claiming a reading where there is none.

Because §4 separates coincident outliers, two observations at `99` are two hit
targets, and multiplicity is answerable by pointing at either.

**Arbitration is pinned, because point regions can overlap** — §4 tightens
spacing below `2.5 * RADIUS` for large clusters, and nearby distinct values can
fall within a radius of each other:

1. **Outlier points are tested first.** A point's region may extend into the
   whisker span when its value sits just beyond a whisker, so points win over the
   box wherever both would answer. Otherwise an outlier adjacent to its whisker
   would be unreachable.
2. **Among candidate points** — those whose centre is within `RADIUS` of the
   cursor — the **nearest by Euclidean distance** is selected.
3. **Ties break to the lowest global producer ordinal.** Cluster index is not
   sufficient: two distinct singleton values both carry cluster index zero, so it
   cannot order them. Each outlier therefore retains its ordinal within the
   category's producer order, and that is the final tie-break. Cluster index
   orders members *within* one equal-valued cluster and nothing else. Coincident
   points differ only in band offset, so an exactly equidistant cursor is
   reachable and must resolve deterministically rather than by iteration order.
4. **If no point is a candidate**, the box test of the table above applies.

### 7. The value range spans whiskers and every outlier

The rule is stated exactly, not as "spans", and preserves the existing padding
arithmetic (`:315`) with outliers added to the extrema:

```
extrema = min/max over every whisker_low, whisker_high, and outlier
        = (0.0, 1.0) when there is no category at all
pad     = (extrema.1 - extrema.0).max(0.001) * 0.1
range   = (extrema.0 - pad, extrema.1 + pad)
```

Three properties carried over deliberately. The `.max(0.001)` floor keeps a
collapsed category (`flat`, every value `5.0`) from producing a zero-width range.
The `(0.0, 1.0)` fallback is applied to the **extrema**, before padding, so empty
input yields `(-0.1, 1.1)` exactly as today rather than a literal `(0, 1)`. And
no category is fabricated for empty input — the fallback supplies a range, not a
box.

Including outliers in the extrema is the change. An outlier outside the range
would paint outside the plot area or be clipped away — the datum most worth
seeing, invisible.

**The existing 1px box-height floor is unchanged.** `(q1_px.y - q3_px.y)
.abs().max(1.0)` (`:59`) already materializes a collapsed box, so `flat` and
`single` draw a visible line rather than nothing. An earlier characterization
that a degenerate box "may vanish" was wrong: the floor is deliberate and stays.

### 8. A validated constructor over relation rows, with private fields

Ingestion takes ADR 547's rows — neutral `(category, whisker_low, q1, median,
q3, whisker_high, outlier)` tuples — and returns `Result`. `TukeyBox`'s fields
are **private**, and no public constructor accepts a struct literal, so a test
cannot bypass an invariant the production path enforces. This is
PRODUCT-0078's `HistogramBins` manoeuvre applied to a richer shape.

Accepted only when all of the following hold, each refusal naming the category
and the failing rule:

| invariant |
|---|
| every summary value and every outlier is finite |
| `whisker_low <= q1 <= median <= q3 <= whisker_high` |
| every outlier lies **strictly outside** `[whisker_low, whisker_high]` |
| every row of a category carries **bit-identical** summary values (`f64::to_bits`) |
| a null outlier row means no outliers, and there is **exactly one** such row |
| null and non-null outlier rows never coexist for one category |
| empty input yields no category at all, with §7's `(0.0, 1.0)` extrema fallback |

The strict-outside rule is the one that would otherwise rot quietly: an outlier
inside the whisker span is not a Tukey outlier, and admitting it would let the
renderer draw a point the statistics do not classify as one.

**What the repeated-summary check proves is bounded**, and the record says so
rather than implying more: it catches malformed input, and it establishes
**nothing about provenance**. The guarantee that a box and its outliers describe
one snapshot comes from ADR 547 computing them in a single request, not from the
values matching. Repetition is denormalization, and identical values could
equally come from two snapshots that happened to agree.

### 9. The second aggregation authority is removed

`prepare_box_plot_data` (`:197`) computes quartiles from raw values inside the
renderer — the same second authority PRODUCT-0078 removed from the histogram.
It is deleted, together with `ResolvedKernelFields::BoxPlot` and the factory arm,
and automatic resolution of `PlotType::BoxPlot` returns a **typed refusal**
naming the variant as producerless, exactly as PRODUCT-0078 did.

This is available at no migration cost because there is no authored producer:
`PlotType::BoxPlot`'s references are `PlotType::ALL`, the `Display` arm, the
resolution arm, and tests. `BoxPlotKernel` is constructed directly from a
`TukeyBox` sequence in tests until the surface record supplies a producer.

### 10. What this record does not decide

No Rune word, no column mapping, no orientation.

The box body still fills from `settings.color_theme.get_color(t)` (`:50`).
PRODUCT-0080 migrated this kernel's *decoration* to `mark_style.decoration`
(`:52`) as scoped and deliberately left the fill. The surface record repairs it
by consuming `mark_style.fill`, discharging PRODUCT-0064's obligation — the same
hand-off PRODUCT-0079 §9 made for the histogram.

Outlier point geometry and colour are **not** deferred — §5 pins them, because
a point cannot be painted or hit-tested without them. What remains for the
surface record is the box *body* fill at `:50`, not the points.

## Implementation sequence

**Stage 0 — bank paint *and* hover.** Paint is already banked; hover is not, and
must be captured before any shape change, including its current `Max:`/`Min:`
text. Banking after the change would bank the new behaviour and prove nothing.

**Stage 1 — the shape.** `TukeyBox`, with no consumer migrated.

**Stage 2 — the consumers.** All seven sites, plus the domain of §7 and the
hover split of §6.

**Stage 3 — the deletion.** `prepare_box_plot_data`, the resolved fields, the
factory arm, and the typed refusal.

## Acceptance gates

1. **Paint parity on the coincident fixture.** A no-outlier category whose
   summaries equal the banked `BoxStats{0,1,2,3,4}` emits operations byte-identical
   to PRODUCT-0080's bank — same fill, same four strokes at 1.0/3.5/1.0/3.5.
2. **Hover changes exactly as intended.** Against the Stage 0 hover bank, the
   summary string differs in precisely the two labels: `Max` → `Whisker high`,
   `Min` → `Whisker low`. Values and order are unchanged. Asserted as a diff
   against the bank, not as a fresh string, so an unintended change to any other
   part fails.
3. **`min` and `max` are gone.** Neither field name exists on the prepared data,
   and no alias or accessor reintroduces them.
4. **Duplicates survive ingestion.** The `twin` shape — two rows both `99.0` —
   produces a `TukeyBox` with `outliers == vec![99.0, 99.0]`. Length two, not
   one.
5. **Duplicates are visually distinct; singletons do not move.** Two coincident
   outliers emit two point operations at **different** band-axis positions and
   **identical** value-axis positions — the first half proves multiplicity is
   visible, the second proves §4's safety property. A category whose outliers all
   differ in value emits every point **centred**, and `lowhigh` (−50 and 200) is
   that fixture: a rule that spread all outliers rather than only clusters would
   fail it.
6. **The offset formula contains whole circles, not centres.** For `n = 3`
   coincident outliers the three band offsets are symmetric about the centre and
   ordered by producer index. The containment assertion is on **outlined
   extents**: `|offset| + FOOTPRINT_RADIUS <= band_width / 2` for every point,
   checked at a tight band where the constraint binds. Two banked
   counter-examples make it discriminating, at `band_width = 10`, `RADIUS = 3`,
   `n = 2`: a centre-only bound gives centres `±2.5`; a fill-only bound gives
   `±2.0`; §4 gives **`±1.5`**, the only one whose outlined extents reach exactly
   `±5.0`. A gate asserting centres, or asserting extents without the stroke,
   passes an implementation that puts separator ink outside the band.
7. **Point geometry is pinned.** Each outlier emits a circle of radius `3.0`
   filled with `mark_style.fill` and stroked with `mark_style.decoration` at
   width `1.0`; under a second skin both colours differ, proving they resolve.
8. **Outliers are hoverable as points.** Hovering each of the two `99.0` points
   returns that observation; hovering the empty gap between the whisker and the
   outliers returns `None`. The gap probe is the discriminating half — a widened
   containment test would return the summary there.
9. **Hit arbitration resolves deterministically.** An outlier whose point region
   overlaps the whisker span returns the **point**, not the box. With two
   candidate points, the nearer is returned. With a cursor exactly equidistant
   between two candidates, the **lowest global producer ordinal** is returned —
   asserted for two coincident members of one cluster *and* for two distinct
   singleton values, which share cluster index zero and therefore cannot be
   ordered by it. The result is identical across repeated calls; an
   iteration-order-dependent implementation fails the repetition assertion.
10. **Box hover stays inside the whisker span.** A cursor above `whisker_high` but
   below an outlier returns `None`, not the summary.
11. **Order is preserved.** Categories appear in first-seen order and outliers in
   producer order, asserted with a fixture whose producer order differs from
   sorted order — otherwise the gate cannot tell preservation from sorting.
12. **Every ingestion invariant refuses, and each has a valid twin.** One case per
   §8 row, each naming the category and the failing rule: a non-finite summary; a
   non-finite outlier; `q1 > median`; an outlier **inside** the whisker span;
   rows of one category disagreeing on `q1`; two null outlier rows for one
   category; a null and a non-null outlier row coexisting. Each is paired with
   the same input made valid, which must ingest — otherwise a blanket rejection
   passes all seven.
13. **Invariants cannot be bypassed.** `TukeyBox`'s fields are private and no
   public constructor accepts a struct literal, asserted by a trybuild fixture
   with banked stderr, per PRODUCT-0049's finding that polariton is binary-only
   and doctests never run.
14. **The domain rule is exact.** `lowhigh` (−50 and 200 against whiskers 30/35)
    yields extrema `(-50, 200)` and range `(-75, 225)` after the 10% pad — the
    fixture proves outliers enter the extrema. Two carried-over cases are
    asserted unchanged: `flat` exercises the `.max(0.001)` floor rather than
    producing a zero-width range, and **empty input yields `(-0.1, 1.1)`** — the
    `(0.0, 1.0)` fallback padded, not a literal `(0, 1)` — with no category
    fabricated.
15. **The collapsed box still materializes.** `flat` and `single` emit a box
    rectangle of height ≥ 1px, asserted unchanged from current behaviour.
16. **The second authority is gone.** `prepare_box_plot_data`,
    `ResolvedKernelFields::BoxPlot`, and the factory arm are absent;
    `PlotType::BoxPlot` resolution returns the typed refusal; `PlotType::BoxPlot`
    still has no production producer afterwards.
17. **Every traced case is covered.** The fixture set is `twin` (duplicate
    outliers), `lowhigh` (two-sided), `none` (no outliers), `single`, and `flat` —
    the five `box_probe` groups, each asserted end to end.

## Stop condition

1. **Band-axis offset cannot be computed without the band width.** §4 assumes the
   kernel knows its band at paint time. If the offset must be decided before
   layout, it becomes a layout concern rather than a paint one and needs its own
   decision.
2. **A category's rows disagree in production.** §8 refuses. If a real ADR 547
   result ever disagrees, the assumption that one request yields one consistent
   summary is wrong, and that is a database finding before it is a rendering one.
3. **A band narrower than one point, or a cluster too dense to separate.** Two
   distinct failures, and leaving the band is no longer one of them — §4 contains
   whole circles for any supported geometry:
   - `band_width < 2 * FOOTPRINT_RADIUS` makes `usable_span` zero and containment
     impossible for even a single point. That is a geometry the mark cannot
     satisfy at any spacing, and it needs a decision about minimum band width or
     point size rather than a spacing tweak.
   - A cluster dense enough that tightened spacing no longer visually separates
     its members preserves multiplicity in the operations while losing it in the
     artifact — the §4 failure returning by another route. A count-aware
     treatment is a different design and belongs to the surface record with a
     real exhibit.

## Supersession

Supersedes no record. Applies PRODUCT-0078's no-second-authority rule to the
second kernel that violated it, and inherits its producerless-refusal pattern.
Preserves PRODUCT-0080's box plot paint bank and its `mark_style.decoration`
migration unchanged.

## Rejected

**Keep `min`/`max` as aliases for the whiskers.** Attractive because it touches
the fewest lines and the paint sites would need no edit. Rejected because both
hover consumers would keep compiling with changed meaning, which is the exact
silent redefinition this record exists to prevent — the compile error is the
feature.

**Widen box containment to include outliers.** Attractive because it makes
outliers hoverable with one changed comparison. Rejected because the gap between
whisker and outlier contains no data, and reporting the summary there claims a
reading that does not exist.

**Deduplicate coincident outliers and report a count.** Attractive because it
removes the overlap problem outright and the count is honest. Rejected because it
discards observations at ingest to solve a drawing problem, and the count would
live only in hover — invisible in an exported artifact.

**Jitter along the value axis.** Attractive because it is the conventional
beeswarm treatment and reads well at density. Rejected because for an outlier the
value is the information; moving it misreports the datum. The band axis carries
no information and is free to use, which is why §4 uses it.

**Random jitter.** Attractive as the simplest separation. Rejected because the
same relation must render identically across runs and exports, or parity gates
and reproducible artifacts both fail.

## Consequences

The box plot becomes representable for the first time: outliers exist in the
prepared data, whiskers mean what Tukey means, and the renderer computes no
statistics.

Hover changes deliberately — `Max`/`Min` become `Whisker high`/`Whisker low`.
This is a **semantic correction banked before surface exposure**, not a
user-facing one: §9's finding is that no user can reach this kernel, and calling
it user-facing would contradict it. Gate 2 diffs against a bank rather than
asserting a new string so the correction is provably the only change.

`box_probe` is retained as the working fixture until the substrate and the
eventual live exhibit are accepted; it is the only relation currently covering
duplicate, two-sided, absent, single, and flat categories together.

Two things are handed forward: the Rune spelling with its column mapping, and the
box *body* fill repair at `:50`. Outlier point styling is **not** among them —
§5 decides it. PRODUCT-0064's obligation is discharged by the surface record when
it repairs the body.
