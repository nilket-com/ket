# ADR 0056: top-down categorical-y reading order

Status: accepted 2026-08-08. This record makes categorical display-domain
order equal visual reading order on y: index zero renders at the top for
horizontal bars, horizontal stacked bars, and heatmap rows. It adds no Rune
surface and changes no identity, color, domain, or sorting rule.

## Context

The first Rune-authored horizontal-bar exhibit exposed an order disagreement
that no earlier record chose. `test19.rn` sorts `region.asc()` and
`product.asc()`. Its y axis nevertheless reads West, South, North, East from
top to bottom, while its legend reads Atlas, Beacon, Cinder, Delta and each
bar cluster reads Delta, Cinder, Beacon, Atlas. The guide and the marks name
the same display domains in opposite visual directions.

The behavior is coherent below the language boundary but inherited rather
than designed. Four independent inversions produce it:

```text
CoordinateTransformer::categorical / CategoricalY
    center_y = height - index * band - band / 2

BarPlotKernel horizontal category geometry
    category_top = (category_count - 1 - category_index) * band

BarPlotKernel horizontal group geometry
    slot_index = group_count - 1 - group_index

StackedBarPlotKernel horizontal category geometry
    category_top = (category_count - 1 - category_index) * band
```

Axis chrome delegates category positions to the transformer, but reconstructs
physical frame extents from category identity. Both
`draw_categorical_y_grid` and `draw_categorical_y_axes` treat category zero as
the bottom band and the final category as the top band. The numeric x-axis
baseline and its tick labels are likewise placed at category zero's lower
edge. A direct transformer flip would therefore cross the computed top and
bottom edges and move the x axis beneath the new top band instead of beneath
the plot.

`CategoricalXY` carries the same latent convention. `categorical_2d` puts y
index zero at the bottom, and the shared categorical-xy chrome derives its
frame and x-axis baseline from the first and last y categories. Heatmap paint
and hover already consume `categorical_2d` without reconstructing row
geometry. A correlation-matrix fixture would conceal a row reversal because
its values are symmetric, so the absence of a visible defect in a natural
heatmap specimen is not evidence for the current order.

The other apparent consumers are bounded. Grids delegate to the shared
categorical-y grid and axis helpers, so facets acquire the repair without
another ordering implementation. Tick-placement applicability reads only the
numeric `x_range`; it is order-blind. Vertical categorical-x plots, ordinary
Cartesian y inversion, and categorical-xy x order are independent.

PRODUCT-0053 described the horizontal category-gap equation as preserving the
existing reversed series order. PRODUCT-0055 then required “category labels
and reversed group order remain correct” in its Stage 0 prerequisite. The
first phrase proved label/mark agreement under inherited geometry; the second
accidentally promoted that inheritance into an accepted premise. This record
supersedes that order explicitly rather than editing accepted history.

## Decision

### 1. Categorical display-domain order is visual reading order on y

For every categorical y dimension:

```text
display-domain index 0       -> top band
display-domain index 1       -> next band down
...
display-domain index n - 1   -> bottom band
```

`CategoricalY` follows this rule for horizontal bars and horizontal stacked
bars. Within a grouped horizontal-bar category, series also follow their
resolved display domain from top to bottom. The existing figure-level legend
continues to list that same domain from top to bottom, so guide and marks agree
without an orientation exception in legend policy.

`CategoricalXY` follows the same rule for its y domain, so heatmap row zero is
the top row. Its x domain remains left to right. Vertical `CategoricalX` bars
remain left to right, and numerical Cartesian y coordinates retain their
accepted bottom-origin transformation.

The record does not reorder data or domains. The relation pipeline and
resolved display domain remain authoritative. `asc()` begins at the top;
`desc()` puts its first, commonly largest, row at the top. Authors who want the
opposite order express it in relation sorting rather than through an implicit
mark reversal.

### 2. Stage 1 removes category identity from physical frame geometry

Before changing a pixel, the implementation removes every dependency that
makes “first category” or “last category” stand in for a physical plot edge:

- horizontal bar and horizontal stacked-bar kernels derive a category band's
  center and extent from `CoordinateTransformer`, then compute its top from
  those returned coordinates instead of repeating the inverse formula;
- categorical-y grid and axis chrome use the physical plot top and bottom for
  their vertical extent, and use the physical bottom for the numeric x-axis
  baseline and tick labels;
- categorical-xy chrome likewise uses physical frame edges and places its x
  axis at the physical bottom, independent of which y row has index zero.

This stage is a pure dependency refactor. Every existing horizontal-bar,
horizontal-stacked-bar, heatmap, grid, axis, tick, hit, hover, and draw oracle
must be byte-identical before the semantic flip proceeds. The banked result is
load-bearing: it separates an incorrect derivation from an incorrect order
change instead of allowing both to land as one unexplained pixel delta.

The lasting property is behavioral rather than a source-text prohibition.
Categorical frame top is always zero, frame bottom and each x-axis baseline are
always the plot height, and categorical-y gridlines span that complete extent
under one, two, and several categories and under domain reordering. A textual
ban on literal `0` or `len() - 1` is rejected: it asserts spelling, is trivial
to evade through a local variable, and does not prove where the rendered frame
lands.

### 3. Stage 2 changes the two authoritative y mappings and group order

Only after Stage 1 banks byte identity:

```text
CategoricalY center_y  = index * band_height + band_height / 2
CategoricalXY center_y = index * band_height + band_height / 2
horizontal group slot = group_index
```

Because both horizontal kernels now consume the first mapping, changing it
moves category labels, grid structure, bars, stacked bars, hit regions, and
hover regions together. Heatmap paint and hover already consume the second
mapping, so cells, row labels, and interaction move together. The direct group
index makes each grouped-bar cluster follow the unchanged legend domain.

This is one semantic change even though it has three arithmetic sites:
categorical domain order is no longer inverted merely because the categorical
dimension is vertical.

### 4. Correspondence, not independent presence, is the oracle

Separate tests that find every label and every mark can pass while the two are
desynchronized. Stage 2 therefore uses deliberately asymmetric joint fixtures:

- every bar category has a unique value-derived length;
- every grouped series has a unique length and exact resolved color;
- the heatmap is non-square and every row/cell position has a unique value.

The gates join label, band, identity, and rendered payload. The band labeled
East must contain East's unique bar lengths; the series labeled Atlas must
occupy the Atlas-colored slot in the same top-to-bottom position as its legend
entry; the heatmap row labeled `foo` must contain `foo`'s exact ordered cell
values. A symmetric matrix, equal bar values, or colors without positional
assertions cannot discharge the record.

### 5. The visible change is bounded and enumerated

The semantic stage intentionally changes existing dashboard horizontal-bar,
horizontal-stacked-bar, and heatmap pixels:

- horizontal categories reflect vertically into display-domain order;
- grouped series reverse within each horizontal category into display-domain
  order;
- horizontal stacked segments keep their existing left-to-right stack order;
- heatmap rows reflect vertically into display-domain order.

Axis labels, marks, and hit regions move with their payloads. Values, category
and series domains, category-gap and bar-width magnitudes, numeric x geometry,
colors, legend order, stack order, hover prose, identity, and figure layout do
not change. Every refreshed baseline is enumerated in implementation evidence
with only the relevant vertical permutation admitted.

### 6. No language or policy surface is added

There is no Rune modifier, orientation control, reverse-scale setting, legend
exception, or implicit sorting policy. `bar(...)` keeps PRODUCT-0055's
pair-resolved orientation and authored-family identity. Dashboard-only
`HorizontalStackedBar` remains dashboard-only. Heatmap remains automatic and
unavailable as a new Rune mark. Facets, shared domains, legend union, and
numeric-axis applicability retain their accepted rules.

## Implementation sequence

The two stages are a stop boundary, not merely a preferred patch order.

1. Refactor the four production areas named in §2: bar geometry, stacked-bar
   geometry, categorical-y chrome, and categorical-xy chrome. Run the full
   existing suite and bank exact output. If any pixel moves, repair or stop;
   do not proceed by refreshing a baseline.
2. Apply §3's top-down mappings and direct group order. Refresh only the
   enumerated semantic baselines, then discharge the asymmetric joint
   correspondence gates.

The stages land as separate implementation commits. Stage 1's green suite and
unchanged baseline ledger are therefore independently reproducible rather than
inferred from the final picture or asserted after both changes have mixed.

## Acceptance gates

1. Stage 1 renders every existing horizontal-bar, horizontal-stacked-bar,
   heatmap, categorical-y/xy chrome, grid, draw-operation, hit, and hover oracle
   byte-identically. No premise is refreshed during this stage.
2. With category counts `1`, `2`, and at least `5`, categorical-y and
   categorical-xy frame top equals `0`, frame bottom and both x-axis baselines
   equal plot height, and categorical-y gridlines span the full frame at two
   substantially different extents. Reordering the domain changes none of
   those frame coordinates. The one-category case proves count independence
   but does not discharge the order flip because both conventions share its
   center.
3. For even category counts `2` and `6`, transformer centers are exactly
   `band / 2`, `3 * band / 2`, and so on from top to bottom in both `CategoricalY`
   and the y dimension of `CategoricalXY`; the old formula
   `height - index * band - band / 2` differs at every index and is asserted
   absent. Odd counts
   remain in gate 2's count-independence sweep but do not discharge this
   negative control: their middle band is a fixed point shared by both
   mappings. The positive index-by-index equality remains required for every
   tested count.
4. An asymmetric grouped horizontal-bar fixture joins each rendered category
   label to unique bar lengths and joins each named legend entry to the exact
   color, identity, and top-to-bottom slot of its bar. Independent label,
   rectangle, or legend-presence assertions do not discharge this gate.
5. The grouped fixture retains PRODUCT-0053's exact `bar_width` and
   `category_gap` equations at both tested heights. Cluster centroids equal the
   top-down transformer answer; paint and hit consume the same rectangles;
   category reserve remains noninteractive; hover reports the unchanged value
   and identity for the rectangle under the cursor.
6. An asymmetric horizontal-stacked-bar fixture joins each category label to
   its unique total and ordered segment values. Categories read top-down while
   stack segments, colors, hover prose, and numeric x geometry remain
   unchanged.
7. A non-square asymmetric heatmap fixture joins each y label to its exact
   ordered cell values and colors, and hover over every tested cell returns the
   corresponding x label, y label, and value. Its x order remains left to right.
   A square or symmetric matrix cannot discharge this gate.
8. Vertical bars, vertical stacked bars, every `CategoricalX` consumer,
   Cartesian transforms, categorical-xy x order, legend order, resolved color
   assignments, display domains, layer identity, and plot descriptions pass
   byte-identically.
9. A faceted grouped horizontal bar acquires the top-down mapping through the
   shared transformer and chrome helpers. Labels remain joined to their cell's
   payload, cells remain congruent, shared domains and panel-blind colors hold,
   and the figure retains one unioned legend. No grid-specific inversion is
   added.
10. Dashboard-selected `HorizontalBar` and `HorizontalStackedBar` both acquire
    the new order without changing selection or inference. Rune's
    pair-resolved horizontal bar keeps `display_label == "plot.bar"`, resolved
    composition `HorizontalBar`, family-keyed lineage, and all PRODUCT-0055
    axis-applicability behavior.
11. Every refreshed SVG, image, and draw-operation baseline is enumerated in
    evidence. The admitted difference classes are only categorical-y vertical
    permutation, grouped-series vertical permutation, and heatmap-row vertical
    permutation. A change to bounds, size, numeric x positions, colors, legend,
    title, gutter, facet geometry, or noncategorical plots fails the gate.
12. `/home/me/work/uat/test19.rn` remains source-identical. Its ascending region
    order renders East, North, South, West top-down, and each cluster renders
    Atlas, Beacon, Cinder, Delta top-down in exact agreement with the unchanged
    named-swatch legend.

## Implementation evidence

- The live plan is `75e0b794`; the earlier `4b46a8a6` object was amended away
  and is not part of the accepted history. Implementation preserves the
  record's diagnostic boundary as two commits rather than collapsing the
  refactor and visible change.
- Stage 1 is independently banked by `4e053142`. Horizontal bar and stacked-bar
  bands began deriving their centers and heights from
  `CoordinateTransformer`; categorical-y and categorical-xy chrome began using
  physical frame edges and the physical bottom x-axis baseline. The full
  workspace suite passed before Stage 2 with no expected output, premise, or
  baseline changed.
- `product_0056_categorical_frames_and_x_axes_ignore_domain_order_and_count`
  observes rendered paths and text at 120x100 and 360x500 with one, two, and
  five categories in both domain directions. Frames remain `(0, 0)` through
  `(width, height)`, x axes stay at the physical bottom, gridlines span the
  complete height, and labels use transformer-owned centers.
- `product_0056_transformers_map_every_categorical_y_index_top_down` checks
  every index for one, two, five, and six categories at both extents in
  `CategoricalY` and `CategoricalXY`. The positive equality is required for
  every count; the old formula is asserted absent only for even counts, where
  no reversal fixed point exists.
- The horizontal-bar oracle uses asymmetric values `10`, `11`, `20`, and `21`
  across two categories and two named series. It joins each top-down rectangle
  to its category, exact resolved color, value-derived width, hover identity,
  and the unchanged legend order. The horizontal-stacked-bar oracle makes the
  equivalent join for labels, totals, ordered segments, colors, numeric
  geometry, and hover.
- The heatmap oracle is deliberately non-square and asymmetric: three x
  columns by two y rows, with unique values in every cell and scrambled input
  rows. Exact cell position, row label, color, and hover payload agree while x
  remains left to right. The faceted horizontal-bar oracle separately proves
  that two cells acquire the shared mapping, keep exact panel-blind colors and
  payload correspondence, and retain one Atlas/Beacon legend.
- Stage 2's initial full Polariton run changed exactly three existing
  PRODUCT-0021 premises:
  `product_0021_full_band_default_retires_the_legacy_inset` vertically permutes
  the two horizontal SVG rectangles;
  `product_0021_bar_width_occupies_the_full_band_slot_in_both_orientations`
  vertically permutes the horizontal width samples; and
  `product_0021_bar_hit_uses_the_same_visible_rectangle_as_paint` moves the
  horizontal A-band probe from the inherited bottom band to the top band. No
  size, width, numeric x coordinate, color, or vertical-bar expectation
  changed, and no stacked-bar, heatmap, grid, legend, or layout baseline
  required refresh.
- Six PRODUCT-0056 tests cover transformer order and physical frames, grouped
  bars, stacked bars, heatmaps, and facets. After the enumerated premise
  updates, Polariton passes 473 unit tests with five live tests ignored, 41
  architecture tests, and the trybuild UI pair. `cargo check --workspace
  --all-targets` passes. The initial final sweep exposed and repaired an
  unrelated deterministic libaryon test-fixture failure; the fixture in
  `engine/runtime.rs` owns the general rule and its two phase-controlled gates.
  The final `cargo test --workspace --all-targets --no-fail-fast` passes,
  including 1,439 libaryon unit tests with 22 ignored.
- `crates/polariton/README.md` now states the authored rule: categorical y
  display domains read top-down, relation sorting remains authoritative,
  grouped bars agree with their named legend, and heatmap x remains
  left-to-right. `/home/me/work/uat/test19.rn` was not edited. The live exhibit
  `/home/me/Pictures/Screenshots/Screenshot From 2026-08-08 12-36-23.png`
  confirms East, North, South, West and Atlas, Beacon, Cinder, Delta top-down,
  with the gridlines retaining the complete physical frame.
- The record originated in the first live screenshot of PRODUCT-0055, not in a
  renderer gate: the horizontal bars opposed the legend even though each was
  independently correct under inherited dashboard geometry. Rune did not
  create that pre-language behavior; it created the first reason to inspect
  it. This is another bounded example for the deferred decision about what the
  SVG backend is licensed to prove: payload and label presence could not prove
  their visual reading-order correspondence.

## Stop condition

> This record may refactor category-band and frame-edge ownership without a
> Stage 1 pixel change, then change categorical-y and heatmap-row display order,
> direct horizontal group order, the named tests, and enumerated baselines. If
> implementation requires changing resolved domain order, adding implicit
> sorting or authored reversal, changing numeric axes or stack order, changing
> colors, identity, legend policy, hover prose, grid layout, Rune surface, or
> any vertical/Cartesian pixels, work stops and the requirement returns to its
> owning record.

## Supersession

This record supersedes only two inherited-order premises:

- PRODUCT-0053 §2's phrase that horizontal category-gap geometry preserves
  “its existing reversed series order”; the gap equation and centering
  invariant remain unchanged.
- PRODUCT-0055 Stage 0's requirement that “category labels and reversed group
  order remain correct,” together with the corresponding implementation
  evidence. Category-label/mark agreement remains required; reversed group
  order becomes direct display-domain order.

It does not supersede PRODUCT-0055's pair resolution, canonical composition,
physical axes, authored-family identity, color lineage, display label,
modifier register, facets, or refusal boundaries.

## Rejected

- **Reverse the horizontal legend:** makes one guide orientation-dependent and
  leaves ascending categories and ranked bars reading upward. Legend order is
  already the display domain; marks should not oppose it.
- **Reverse the resolved domains:** changes semantic identity and continuation
  state to repair presentation. The domains are correct; only their y mapping
  is inverted.
- **Implicitly sort bars or heatmap rows:** sorting belongs to the relation
  pipeline and would make mark geometry inspect or reorder data.
- **Flip only `BarPlotKernel`:** desynchronizes bars from transformer-owned
  labels, grid structure, hit regions, facets, and stacked bars.
- **Refactor and flip without an intervening oracle:** makes a derivation error
  indistinguishable from the intended semantic pixel change.
- **Leave `CategoricalXY` bottom-up:** establishes “display order is reading
  order” for one categorical y layout while preserving the identical inversion
  in another. Heatmap already has the cheaper, fully transformer-derived path.
- **Use a symmetric heatmap oracle:** a correlation matrix can conceal a
  complete row reversal. An oracle that a dumb reversal can satisfy is
  vacuous for this defect.
- **Textually forbid category-index spellings in chrome:** asserts code shape
  rather than physical frame truth and is trivially evaded by indirection.
- **Add a reverse-axis modifier:** no exhibit asks for the inherited order.
  The set does not grow to preserve an accidental default.

## Consequences

- Ascending categorical lists begin at the top, descending ranked bars put
  their first result at the top, and grouped horizontal bars agree directly
  with their legends.
- Heatmap rows follow the same top-down reading rule as every other categorical
  y display instead of retaining a second convention.
- Physical frame geometry no longer depends on category identity. Future order
  changes cannot silently drag the x axis or truncate the grid extent.
- Horizontal dashboard pixels change deliberately and narrowly; vertical and
  Cartesian figures remain exact controls.
- The implementation banks a no-op refactor before spending it on the visible
  correction, preserving a distinct diagnosis if either stage later regresses.
