# ADR 0072: percent-of-total stacking

Status: proposed 2026-08-10. This record adds `.percent()` as a normalized
stack-scale modifier for the two authored stacked families: stacked bars and
areas. It defines the denominator over the canonical first-row-wins values
PRODUCT-0064 and PRODUCT-0069 already render, fixes the value domain at
`0..100`, gives the shared axis chrome a percentage formatter, and retains raw
values in hover. It adds no mark, channel, aggregation, palette identity, or
persistence-schema change.

## Context

Rune can now author both absolute stacked families:

```rune
bar("region", "revenue", "product").stacked()
area("period", "revenue", "product")
```

Neither can express percentage of total. The missing capability is not a
relation transform. The relation still contains absolute revenue, hover still
needs to report it, and colour identity still follows the same product. What
changes is the value scale used to place each cumulative boundary: the same
canonical stack is projected from absolute units onto percentage points.

The two kernels already agree on the data semantics this projection consumes.
PRODUCT-0064 §8 and PRODUCT-0069 §§2-3 make their prepared matrices:

- first-row-wins for duplicate `(category, series)` or `(x, series)` keys;
- finite and strictly positive, with every other sample canonicalized to zero;
- ordered by the resolved display domain when one exists; and
- the single authority for range, geometry, hit, and hover.

That agreement is load-bearing. A percentage denominator computed from raw
rows would silently aggregate duplicates the mark does not draw. One computed
from an independently filtered series could disagree about negatives or
non-finite values. The denominator must describe the rendered relation, not a
more convenient relation reconstructed beside it.

The current layout model has one missing semantic fact. `Cartesian` carries
`x_axis_type` and `y_axis_type`, but `CategoricalX` and `CategoricalY` hardcode
their numeric tick labels as `AxisType::Linear` in shared chrome. A kernel-only
formatter would therefore fix one renderer while grids, facets, and edge text
continued to label the same coordinates differently. Percent formatting must
travel with `PlotLayout`, the existing geometry and chrome authority.

PRODUCT-0068 pre-filed the guide consequence. A percent area with a positive
total at every x fills the complete `0..100` plot height. A vertical stacked bar
with `bar_width(1.0).category_gap(0.0)` and a positive total in every category
can cover the complete frame as well. Gridlines beneath those opaque marks are
fully occluded. This record must either reopen the global stratum or accept axis
ticks and resolved segment boundaries as the available proportion guides.

## Decision

### 1. `.percent()` selects a stack scale

`.percent()` is a zero-argument member of the area register and of the bar
register when that bar also carries `.stacked()`:

```rune
bar("region", "revenue", "product").stacked().percent()
bar("revenue", "region", "product").percent().stacked()
area("period", "revenue", "product").percent()
```

Modifier order is irrelevant. The register is a set, not a pipeline, so the
two bar spellings lower identically. Duplicate `.percent()` and any argument
refuse.

On a bar, `.percent()` without `.stacked()` refuses with
`percent() requires stacked() on bar marks`. Grouped percentage bars are a
different visual form and are not smuggled in as a side effect. On line and
scatter it refuses as a cross-register member. Area is inherently stacked, so
it needs no second modifier.

The resolved model gains a closed `StackNormalization` vocabulary:

```text
Absolute   shipped default
Percent    cumulative percent of the local positive total
```

This fact belongs to `AuthoredPlotSpec` and the resolved kernel contract, not
to `LayerSettings` or the five-tier presentation cascade. It changes the value
scale's meaning; it is not a style that an application theme or pane may
override.

### 2. Normalization does not change family, composition, or identity

The resolved composition remains `StackedBar`, `HorizontalStackedBar`, or
`StackedArea`. `.percent()` does not name a new mark member.

Layer identity remains the authored `Bar` or `Area` family exactly as
PRODUCT-0055 §4, PRODUCT-0064 §3, and PRODUCT-0070 §4 define it. Toggling
absolute/percent changes geometry and tick formatting but cannot start a new
categorical lineage or recolour a series. Legend content and order remain
identical.

`polariton_session` remains schema version 2. The durable payload stores family
and colour allocation, not a copy of the Rune plot specification; the restored
buffer remains the authority for `.percent()`. Adding a fifth durable family or
a new runtime plot type would misclassify a scale choice as identity and is
outside this record.

### 3. The denominator is the canonical rendered total

Preparation computes and retains one total per stack from the canonical matrix:

```text
stacked bar    total[category] = sum(category_values[category][series])
stacked area   total[x]        = sum(category_values[series][x])
```

Those values already embody first-row-wins and positive-finite
canonicalization. Raw relation rows are never rescanned and duplicate keys are
never summed for normalization. An author who wants aggregation must express it
in the relation pipeline before the mark, as PRODUCT-0007 §5 requires.

Geometry projects cumulative absolute values, not independently rounded segment
shares:

```text
percent(cumulative, total) = 100 * cumulative / total   when total > 0
                           = 0                           when total == 0
```

Projecting the cumulative boundary makes the final positive boundary exactly
`100` because its numerator is the same retained total. It avoids the drift
from summing separately divided shares. Paint, rectangle hit, area polygons,
area boundaries, and hover all consume this same projection and the same total
vector.

A zero-total bar emits no rectangles. A zero-total area sample pinches every
band to zero at that x, retaining PRODUCT-0069's continuous polygon topology.
The plot still has the fixed percentage domain; zero total is absence of a
share, not a collapsed scale and not `0%` data fabricated from no denominator.

The two-field forms remain legal. A positive one-series stack occupies `100%`;
a zero-total stack occupies nothing. This is algebraically redundant but follows
PRODUCT-0064 §7 and PRODUCT-0070 §7's accepted degenerate-one-series posture.

### 4. Percent is a resolved axis type with a fixed domain

`AxisType` gains `Percent`. It formats the existing linear percentage-point
value and appends `%`:

```text
0      -> 0%
12.5   -> 12.5%
100    -> 100%
```

`CategoricalX` carries its numeric y-axis type and `CategoricalY` carries its
numeric x-axis type, just as `Cartesian` already carries both. Absolute kernels
populate `Linear`, preserving every existing byte. Percent vertical bars put
`Percent` on y, horizontal bars put it on x, and percent area puts it on y.
Shared chrome derives formatting from the layout; kernels do not format axis
ticks independently.

The normalized value domain is exactly `(0.0, 100.0)` in both orientations and
at every area x. No 5% or 10% automatic headroom is added. Faceted percent
figures therefore share the identical domain by construction.

Authored bounds on the normalized value axis refuse. `y_bounds` refuses for a
vertical percent bar and percent area; `x_bounds` refuses for a horizontal
percent bar. Even `(0, 100)` refuses: the domain is part of the scale contract,
not an authored coincidence, and accepting one spelling would invite cropping
later. Bounds on area's x axis retain their existing meaning.

Tick steps and minor subdivisions remain applicable to the normalized value
axis. Their unit is a percentage point: `y_tick_step(25)` produces `0%`, `25%`,
`50%`, `75%`, and `100%`. `TickPlacement::Step` therefore admits both `Linear`
and `Percent`; date/time refusals remain unchanged. Count placement retains the
authored/library interval count rather than hiding a percent-specific density
override.

### 5. Hover reports absolute truth and normalized placement

Normalization never discards the absolute value. In percent mode, a stacked-bar
segment reports all four quantities:

```text
East: Atlas (Value: 30.00, Share: 25.0%, Cumulative: 25.0%, Total: 120.00)
```

An area segment reports the same facts across its existing multiline form:

```text
X: 3
Atlas: 30.00
Share: 25.0%
Cumulative: 25.0%
Total: 120.00
```

`Value` and `Total` are absolute units. `Share` is the segment's fraction of
the retained denominator. `Cumulative` is the segment's upper rendered
boundary and therefore explains where the cursor landed. The absolute-mode
hover prose remains byte-identical; this record does not retroactively rename
its accepted cumulative `Total` label.

At a zero-total area sample, hover may report the absolute total `0.00` but may
not report a share or manufacture `0.0%`; no denominator exists. Bar gaps and
zero-total bar bands continue to return no segment hover.

### 6. PRODUCT-0068's stratum stays global

Percent marks receive no overlay-grid exception. Gridlines remain beneath marks
for every kernel. A fully positive percent area, or a gapless full-width percent
bar wall, may occlude every interior gridline.

That is accepted. Percentage tick labels and tick marks remain above marks and
outside the plot extent; resolved segment boundaries remain above each area
fill, and bar segment edges remain geometrically exact. These are sufficient
references for the scale. An author can retain visible grid through bar gaps by
using the existing width/gap controls, but that is not required for correctness.

This closes PRODUCT-0068's named trigger without reopening its stratum. No
per-mark guide order, transparency, adaptive chrome, or duplicated overlay line
is introduced.

### 7. Existing data, colour, and legend semantics remain authoritative

Percent mode does not alter category/series order, first-row selection,
positive-finite canonicalization, missing-value behavior, colour assignments,
legend provenance, bar width, category gap, area interpolation, or boundary
style.

In particular, a duplicate-key fixture whose first value is `30` and later
value is `300` uses `30` in both the numerator and denominator. A result based
on `330`, `300`, or the raw row sum is wrong even if its segments happen to end
at `100%`.

Facets keep PRODUCT-0065's shared category and numeric domains, PRODUCT-0048's
congruent cells, and PRODUCT-0044's one figure-level legend. The `Percent` axis
type must survive shared-layout merging alongside the fixed range; taking one
cell's numeric formatting by coincidence does not discharge this rule.

### 8. Scope boundary

This record adds percent-of-total only. It does not add percent formatting to
arbitrary numeric axes, grouped percentage bars, negative/diverging stacks,
raw-row aggregation, weighted denominators, percentages across categories,
labels inside segments, or a generic relation normalization verb.

The denominator is local to each existing stack: one bar category or one area x
sample. It is never figure-wide, facet-wide, or series-wide.

## Implementation sequence

1. Bank absolute stacked-bar and area layout, SVG/draw, hit, hover, and facet
   oracles. Extend `AxisType` and the two categorical `PlotLayout` variants,
   populate `Linear` everywhere, and make shared chrome consume the carried type.
   This step must be byte-identical before normalization exists.
2. Add `StackNormalization` plus retained denominator vectors. Route percent
   layout, geometry, hit, boundary, and hover through one cumulative projection
   while leaving the default `Absolute` path byte-identical.
3. Add `.percent()` to the bar and area registers, thread it through authored
   and resolved specifications, enforce its bar dependency and value-axis bound
   refusal, and admit percentage-point tick steps.
4. Add `../uat/test24.rn` with vertical, horizontal, faceted, gapless, area,
   two-field, duplicate-row, and zero-total exhibits. Update the corpus, README,
   and language-as-built.

Step 1 lands green without a baseline refresh. A formatting or layout change
hidden under the new surface would otherwise be indistinguishable from intended
percent pixels.

## Acceptance gates

1. `.stacked().percent()` and `.percent().stacked()` lower identically for
   vertical and horizontal three-field bars. `area(...).percent()` lowers in
   two- and three-field forms. The modifier takes no arguments, may appear once,
   and PRODUCT-0058 static field bindings remain accepted in every field slot.
2. `.percent()` without `.stacked()` refuses on bar with the named dependency;
   line and scatter refuse it as cross-register use. The refusal is atomic: no
   partial sink, setting, or view survives.
3. Step 1 is byte-identical for every existing absolute bar and area oracle,
   including both orientations, facets, bounds, tick steps, draw operations,
   hit, hover, and legends. `AxisType::Linear` is carried rather than inferred
   from the old hardcoded branch.
4. A deliberately asymmetric stacked-bar fixture asserts each segment's raw
   value, denominator, share, cumulative percent, rectangle boundary, hit, and
   hover jointly. The final positive boundary equals exactly `100.0` rather than
   a tolerance-rounded sum.
5. The equivalent non-square area fixture asserts every x-specific denominator,
   polygon boundary, hover band, and final `100.0` boundary jointly. Denominators
   differ substantially across x so a global-total implementation fails.
6. Duplicate `(category, series)` and `(x, series)` fixtures place radically
   different later values after the accepted first row. Both families use the
   first canonical value in numerator and denominator; raw-row sums,
   last-row-wins, and implicit aggregation are asserted absent.
7. Negative, zero, non-finite, and missing samples contribute exactly zero to
   numerator and denominator in both families. A mixed stack normalizes its
   remaining positive values; an all-zero stack emits no bar rectangles or
   share prose, pinches every area band to zero, and retains a `0..100` layout.
8. Two-field positive bars and areas reach `100%` and retain Synthetic legend
   provenance with no legend. Their zero-total controls render no invented
   share.
9. Vertical bars expose `CategoricalX { y_axis_type: Percent, y_range: (0,100) }`,
   horizontal bars expose `CategoricalY { x_axis_type: Percent, x_range:
   (0,100) }`, and areas expose Cartesian `y_axis_type: Percent`. Absolute
   controls expose `Linear` and their existing ranges.
10. Shared chrome renders exact percent labels from the carried layout type in
    ordinary and faceted figures. `y_tick_step(25)` or horizontal
    `x_tick_step(25)` yields exactly `0%`, `25%`, `50%`, `75%`, `100%`; the same
    tick setting on an absolute plot retains linear labels. Date/time step
    refusal is unchanged.
11. Value-axis bounds refuse in percent mode in all three geometries, including
    `(0,100)`, with the figure-element span and a reason naming the fixed percent
    domain. Area x bounds remain applicable. Tick steps and minor ticks remain
    applicable in percentage points.
12. A faceted percent bar and area carry `Percent` through shared-layout merge,
    share exact `0..100` domains, retain congruent cells and panel-blind colours,
    and render one figure-level legend. No cell silently falls back to linear
    labels.
13. Toggling absolute/percent on an otherwise identical producer preserves
    every exact resolved colour, `LayerIdentity`, durable `AuthoredFamily`, and
    restored palette position. `polariton_session` stays version 2 and a real
    checkpoint succeeds without a payload difference beyond already-authored
    buffer text.
14. A PRODUCT-0057 Vulkan control drives a fully positive area and a gapless
    full-width bar wall. Grid operations exist before marks, no grid-composited
    fill pixel survives inside the occupied frame, percent ticks and area
    boundaries remain visible, and no overlay-grid exception is present. A bar
    control with gaps still shows the same grid on exposed canvas.
15. Absolute-mode hover prose and all pre-record pixels remain byte-identical.
    Percent hover contains the exact absolute value, share, cumulative percent,
    and absolute denominator; no label silently reinterprets absolute `Total`
    as a percentage.
16. `../uat/test24.rn`, the full Polariton/workspace suite, formatting checks,
    architecture gates, trybuild, and PRODUCT-0057 Vulkan probes pass with no
    ignored gate newly admitted. Baseline changes are confined to the new opt-in
    percent fixtures; no existing baseline refresh is permitted.

## Consequences

One modifier now expresses percent-of-total consistently across every authored
stacked geometry. The surface is small because the semantics are not: the
record makes the denominator, zero-total behavior, axis unit, bounds
applicability, hover truth, and guide occlusion explicit rather than hiding them
behind division in a renderer.

`AxisType` now represents a resolved display scale as well as a source dtype.
That was already conceptually true for date/time formatting; `Percent` makes it
visible. Categorical layouts become semantically complete enough for shared
chrome to label their numeric axes without knowing which kernel produced them.

Fully occluding percent marks can have no visible interior grid. This is a
known consequence, not a regression against PRODUCT-0068. The scale remains
readable from above-mark axis furniture and segment boundaries.

A relation containing duplicate `(category, series)` or `(x, series)` rows
yields percentages computed from the first row per key, not from the column
total. The displayed shares can therefore differ from a hand-computed total
over the raw rows. That is PRODUCT-0069's first-row-wins rule made visible as a
number rather than a position, and aggregation remains relation-stage work.

## Supersession

This record closes PRODUCT-0064's deferred percentage semantics, PRODUCT-0068's
full-occlusion trigger, and PRODUCT-0069's denominator question. It does not
change those records' absolute-stack behavior.

It extends PRODUCT-0037/0038 only by admitting `TickPlacement::Step` on a
`Percent` axis measured in percentage points. Their zero-anchored lattice,
capacity, minor-subdivision, and span-attributed refusal rules remain accepted.

It extends `PlotLayout::CategoricalX` and `CategoricalY` with the numeric axis
type required by shared chrome. PRODUCT-0065's shared-domain and PRODUCT-0048's
congruence invariants remain unchanged.

## Rejected

- **Normalize in the relation pipeline automatically:** rewrites authored data,
  discards the absolute value hover needs, and duplicates a transform the author
  can already express deliberately.
- **Sum raw duplicate rows for the denominator:** makes a share describe values
  the mark does not render and silently reverses PRODUCT-0064/0069's
  first-row-wins decision.
- **`.stack("percent")`:** supersedes `.stacked()` and turns a closed geometric
  selector into a stringly mode switch after the absolute spelling shipped.
- **`.normalize()` or `.percent_of_total()`:** the former omits the unit and
  denominator; the latter is explicit but needlessly repeats the only meaning
  `percent()` can have on an admitted stacked mark.
- **Allow `.percent()` on grouped bars:** creates a distinct grouped-proportion
  form and makes the same modifier mean two geometries.
- **Use a `0..1` domain:** makes authored tick steps read as fractions while the
  axis and hover present percentages. `0..100` gives every guide value one unit.
- **Allow value-axis bounds, including `(0,100)`:** turns a fixed semantic domain
  back into authored geometry and creates pressure for misleading cropped
  percentages.
- **Append `%` inside each kernel:** leaves shared/faceted chrome on its hardcoded
  linear formatter and creates two authorities for one axis.
- **Reopen grid-over for percent marks:** changes resolved fill pixels and takes
  the per-mark exception PRODUCT-0068 explicitly forbids.
- **Make opaque fills translucent so grids show through:** changes palette
  colours to rescue guides and introduces a new opacity policy.
- **Add a Percent plot type or durable family:** normalization changes scale,
  not mark identity, and would recolour or strand existing sessions.

## Stop condition

> This record may add `.percent()` to stacked bars and areas, add a closed stack
> normalization fact, retain canonical totals, project geometry/hit/hover onto
> `0..100`, add `AxisType::Percent` and numeric axis types to categorical
> layouts, refuse value-axis bounds, admit percentage-point tick steps, and add
> the named gates and fixture. If implementation requires rescanning raw rows,
> aggregation, a new plot type or durable family, a presentation-cascade
> setting, a per-mark guide stratum, palette or opacity changes, or a different
> denominator between geometry and hover, work stops.

> If the categorical layout extension cannot land byte-identically with
> `Linear` populated everywhere before percent mode exists, work stops. The
> axis formatter may not be hidden inside the new kernel path to preserve an
> oracle.

> If a final cumulative boundary cannot be made exactly `100` from the retained
> denominator without rounding segment shares, work stops. The record may not
> paper over projection drift with a pixel tolerance.
