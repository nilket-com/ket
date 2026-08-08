# ADR 0054: the series line surface

Status: proposed 2026-08-08. This record makes the existing grouped-line
substrate authorable as `line("x", "y", "series")` and repairs the two routing
seams that Stage 0 found before exposing it: authored series colors must reach
line paint and legend as resolved assignments, and the inherited line modifier
must reach a standalone series line.

## Context

The visual audit after PRODUCT-0053 found an asymmetric surface. Scatter and
bar admit a third categorical field; line explicitly refuses it in legacy,
relation-sugar, canonical view, figure-body, and modified-mark positions.
PRODUCT-0012 deferred that spelling for evidence, and PRODUCT-0047 excluded it
so bar could not borrow line's decision.

This request is DB-10's recorded trigger: the first concrete request to inspect
a multi-series line beside the now-visible grouped bar and scatter forms.

Below the envelope, the capability already exists. `ResolvedKernelFields::Line`
has `series: Option<String>`, channel projection reads `Channel::Color`,
`prepare_line_data` partitions by the series field, `LineMarkRenderer` paints
one path per partition, and the legend emits named `LineSample` entries. The
missing surface is therefore an arity, not a new mark or geometry.

Stage 0 found two reasons this cannot honestly be an arity-only record:

- line preparation assigns each local partition a normalized `color_t`, and
  paint plus legend sample `ResolvedPlotSettings.color_theme` from it. Unlike
  scatter and series bar, line never consumes the exact
  `ResolvedChannel.colors` written by PRODUCT-0029. Filtering or faceting to a
  subset can therefore recolor the same series inside one figure;
- `.stroke_width(...)` belongs to line by PRODUCT-0046's mark-register rule,
  but a categorized modified line takes the standalone styled path, whose
  factory currently admits scatter and bar only. The new form would parse and
  then fail during completion unless that existing handoff admits line.

Both repairs are consequences of making the spelling real. Shipping the arity
without them would expose a form that violates accepted color identity and its
inherited register on first use.

## Decision

### 1. Line admits an optional series field in every standalone position

```rune
plot.line(relation, "x", "y", "series")
relation.line("x", "y", "series")
relation.views([line("x", "y", "series")])

figure([
    palette("cividis"),
    line("x", "y", "series").stroke_width(2.0),
])
```

The three compatibility spellings are exact projections of one normal form.
Canonical and figure positions admit two or three fields; the legacy call
admits a relation plus two or three fields. All fields remain string literals
under the existing mark-argument rule.

The third field lowers to `Channel::Color = Named(series)`, exactly as for the
three-field scatter and bar. Resolution remains exact and attributed: it never
substitutes another field or infers a series.

`line(x, y, series)` remains unavailable inside `layers([...])`, matching the
existing three-field scatter rule. A data-driven series partition is one mark;
it is not an additional authored layer in PRODUCT-0013's shared-domain sum.

### 2. Series paths preserve relation order and never cross partitions

The existing line geometry stands. Rows are partitioned by the resolved series
field; within each partition points connect in their relation order. No segment
connects the last point of one series to the first point of another. This
record adds no implicit x sort: authors who mean increasing x order state it in
the relation pipeline, where ordering remains visible and testable.

The two-field line stays the same synthetic one-series path and remains
byte-identical by default.

### 3. Resolved categorical colors become the line's source of truth

For an authored series channel, line preparation receives the exact
`CategoryColorAssignment` values already present on `ResolvedPlotSpec`.
Prepared series retain the resolved color paired with their data-read name.
Paint and legend both consume that retained color; neither independently
samples a local ramp.

This is the line-shaped application of PRODUCT-0029 §1, not a new allocation
rule. Palette positions, capacity, first-seen display order, continuation
identity, filtering, and atomic refusal remain owned by resolution. A
channel-absent synthetic line may retain the compatibility theme sample used
today; no authored categorical identity may fall back silently.

The same routing applies to existing composite consumers that prepare a line
from a resolved color channel, including scatter-line overlay, so its line and
point samples cannot disagree about one series. Unrelated dashboard line
consumers without a resolved channel remain unchanged.

### 4. The inherited line register reaches the standalone form

PRODUCT-0046 already decides that line's register contains
`.stroke_width(factor)` and that new spellable encodings inherit the register
by mark kind. The standalone styled factory therefore admits a series line and
resolves its retained `LayerSettings` at draw, in the same five-tier order as
every other line.

This is routing, not a second lifecycle or modifier. The ordinary two-field
modified line may keep its existing one-layer path; both forms must produce the
same width semantics, paint, and hit tolerance.

### 5. Legend, facets, and identity inherit their accepted rules

Named series earn `LineSample` entries in display-domain order. A faceted
figure unions those entries into one figure legend, and a series absent from a
cell keeps the same resolved color wherever it appears. The two-field
synthetic `All Data` entry remains omitted.

Changing a view between `line(x, y)` and `line(x, y, series)` changes its
encoding, not its view identity, panel, placement group, or label.

## Acceptance gates

1. Legacy, relation-sugar, and canonical three-field spellings normalize to
   one program; direct, named-view, figure, and two-field controls retain their
   existing identities and semantics.
2. The third field lowers exactly to a named color channel. Missing,
   non-categorical, or over-capacity series fields refuse the complete sink at
   its attributed span without preventing sibling sinks from completing.
3. Four interleaved series with at least three points each produce four paths,
   preserve within-series relation order, and contain no cross-series segment.
4. Paint and `LineSample` legend swatches equal the resolved categorical
   assignments byte-for-byte. Reordering, filtering out a series, and restoring
   it never recolor retained identities.
5. A facet whose cells contain different series subsets keeps panel-blind
   colors and renders one figure legend containing the display-domain union.
6. `.stroke_width(2.0)` succeeds on the three-field form directly and in a
   faceted figure; paint width and geometric hit tolerance match the accepted
   two-field modifier semantics at every cascade tier.
7. A named one-series field keeps its one-entry line legend, while the
   two-field synthetic control has none. Entry order follows the resolved
   display domain, not partition iteration.
8. Scatter-line overlay uses identical resolved colors for each line and point
   series after the routing repair; ordinary scatter, grouped bars, and stacked
   or fill-between kernels pass unchanged.
9. Four fields, nonliteral fields, duplicate/cross-register modifiers, and a
   three-field line inside `layers([...])` refuse at the existing contextual
   boundaries. Ordinary Rune outside a proven sink remains ordinary.
10. Every existing two-field line SVG, paint/hit oracle, panel lifecycle test,
    and layered-line baseline is byte-identical. No baseline is refreshed.
11. The implementation adds `../uat/test18.rn` over `demo_sales`, explicitly
    sorted within product, showing four product lines and their named legend.
    The already-created `../uat/test17.rn` remains the corresponding
    four-product scatter exhibit.

## Stop condition

> This record may widen standalone line arity, lower the third field into the
> existing named color channel, route resolved assignments through line paint
> and legend (including resolved composite consumers), admit line in the
> standalone styled factory, and add the named gates and UAT fixture. If the
> implementation requires new line geometry, implicit sorting, a new color or
> capacity rule, series-aware hover prose, a new modifier, three-field line
> admission inside `layers`, or any change to two-field pixels, the work stops
> and that requirement returns as its own record.

## Rejected

- **`grouped_line(...)`:** grouping is an encoding of the line mark, not a
  second mark; bar and scatter already establish the optional third field.
- **Express the data series as `layers([line(...), ...])`:** that repeats field
  names and fixes the series cardinality in source. One categorical column is
  data-driven partitioning, not N authored marks.
- **Expose the current local ramp unchanged:** it makes a series' color depend
  on which siblings survive a filter or facet and contradicts resolved color
  identity.
- **Automatically sort each series by x:** changes existing line semantics and
  hides an ordering decision that belongs in the relation pipeline.
- **Admit categorized lines inside `layers`:** couples this surface to shared
  color-scale composition that PRODUCT-0014 explicitly excluded for the
  identical scatter arity.

## Consequences

- Line, scatter, and bar all use the same two-or-three-field standalone shape:
  x, y/value, and an optional categorical identity.
- A multi-line figure becomes concise without sacrificing stable palette
  identity under filtering or facets.
- The visual audit gains paired UAT exhibits: `test17.rn` for grouped scatter
  now, and `test18.rn` for series lines when this record lands.
- The line substrate stops being the remaining categorical renderer that
  reconstructs resolved colors from local cardinality.
