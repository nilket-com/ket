# ADR 0077: facet-edge tick label containment at occupied shared boundaries

Status: proposed 2026-08-11. This record repairs a shipped containment defect in
facet grids: a cell's bound-coincident tick label is centred on its tick and
places ink outside its own cell — into the shared gutter always, and into the
neighbouring cell once the tick format is wide enough. It moves pixels only in
faceted figures that have an occupied interior boundary and a tick lying exactly
on the axis bound. It changes no layout geometry — `GRID_GAP` stays
12px, cells stay congruent, no text measurement enters layout — and it changes
no single-panel figure. It supersedes nothing; it extends PRODUCT-0068's
containment principle from gridline strokes to numeric tick labels.

## Context

`uat/test21.rn` panel `horizontal-faceted-percent` renders its two facet columns
with x tick labels reading `100%0%` at the shared boundary: the left column's
`100%` and the right column's `0%` interpenetrate. The panels directly above it
in the same grid arrangement — `horizontal-faceted-grouped` (`0 40 … 160`) and
`horizontal-faceted-stacked` (`0 100 … 400`) — keep a clean gap. The defect is
therefore not "facets are too tight"; it is that nothing bounds how far a label
may travel from its own cell.

### The geometry, and why only the percent panel shows it visibly

PRODUCT-0048 hoists axis label space to the grid edge once and tiles the cells
congruent. `grid_tiled_area` (`plot/grid.rs:814`) removes
`plot_padding_left` (100) and `plot_padding_bottom` (80) from the whole grid,
and `grid_plot_area` (`plot/grid.rs:842`) then removes only
`plot_padding_top` (50) and `plot_padding_right` (20) from each cell. A cell's
plot area is consequently **flush to its own left and bottom edges**.

So at an interior vertical boundary between adjacent cells:

```
left cell's last label  overhangs right by   half_width - plot_padding_right
right cell's first label overhangs left by   half_width          (no inset)
space available                              GRID_GAP
```

with `GRID_GAP = 12.0` (`plot/grid.rs:29`) and `plot_padding_right = 20.0`
(`plot/common.rs:505`), giving a collision exactly when

```
half(last) + half(first) > 32
```

At `x_tick_size` 18.0 (`plot/common.rs:510`), `100%` is roughly 44px and `0%`
roughly 24px: 22 + 12 = 34 > 32, a ~2px overlap, which is what the glyphs show.
`400` and `0` give 15 + 5 = 20, clearing by 12px.

### Spill is the failure; visible contact is only its symptom

That arithmetic describes when two spills *meet*. It does not describe when the
invariant is broken, and reading it as the defect would scope this record to the
percent panel alone. Because a cell's plot area is flush to its left edge, a
right-hand cell's first numeric label is centred on a tick sitting exactly on
that edge, so **half its ink is in the gutter always** — in
`horizontal-faceted-grouped` and `horizontal-faceted-stacked` as much as in the
percent panel. The same holds vertically: the upper cell's bottom y label is
centred on a tick at the cell's flush bottom edge and projects roughly half a
line height into the horizontal gutter, today, in every faceted figure that
draws y text.

So every faceted panel in the current showcase already violates the invariant
this record adopts. What the percent scale contributes is not a new failure but
enough width for two existing spills to interpenetrate, which is what made the
class visible. Consequently the banked red state is **nonzero tick-label ink in
both gutters**, not "labels touch"; a remedy that separated the glyphs while
leaving them in the gutter would satisfy a contact test and still leave the
invariant broken.

### The principle already exists, scoped one operation too narrowly

PRODUCT-0068 already gates this exact class. `plot/grid.rs:3713` asserts:

> "PRODUCT-0068: no facet grid may paint in a gutter or neighboring clip"

That assertion filters `StrataOperationKind::Stroke(grid_color)`. Gridlines are
confined to their cell's clip; chrome text is not clipped and not covered. The
defect is not a missing rule but an unclipped exception to a rule we hold.

### Why it escaped until now

Two masks, both of the arc's recurring shape — the fixture occupying the one
configuration where the defect is invisible. No artifact combined a percent
scale with a horizontal facet grid until PRODUCT-0076's showcase, and
`demo_sales` carried a single quarter until 2026-08-11, so every faceted cell
held one group and the showcase's faceted half was degenerate. Expanding the
fixture to four quarters and adding the percent panels exposed it immediately.

## Decision

### 1. The defect is containment, not spacing

The invariant this record adds:

> **At an occupied shared boundary, a cell's bound-coincident tick label places
> no ink beyond its own cell — neither in the shared gutter nor in the
> neighbouring cell.**

Gutter ink is the violation. Two labels touching is one consequence of the
violation and the reason it was noticed, but a figure whose labels sit half in
the gutter without meeting anything is already outside the invariant, and most
of the current showcase is in exactly that state.

`GRID_GAP` is not the remedy and does not change. A larger constant defers the
same failure to the next wider label, which is the substitution this arc has
already rejected once, in PRODUCT-0074's divider reservation. Containment is
established by where a label is anchored, not by how much room is left beside
it.

### 2. Anchoring is the mechanism, and only bound-coincident ticks are anchored

At a shared boundary, the label of a tick lying **exactly on the axis bound** is
anchored inward instead of centred. Every other tick keeps `Center`.

The qualifier is load-bearing. `TickPlacement::Count` distributes positions
across `range.0..=range.1` inclusive (`plot/common.rs:95-97`), so both endpoints
always coincide with the bounds. `TickPlacement::Step` computes
`first = ceil(range.0 / step)` and `last = floor(range.1 / step)`
(`plot/common.rs:99-100`), so a stepped range may omit its endpoints entirely:
`y_bounds(0, 42)` with `y_tick_step(10)` produces a last tick at 40, which is
inset from the bound and projects across nothing. Anchoring "the last generated
tick" would displace an interior tick away from its true position for no reason
— reporting a wrong location to fix a collision that does not exist.

Bound-coincidence is compared with an explicit tolerance derived from the axis
range, not with `==` on `f64`.

### 3. Scope is occupied interior boundaries only

A boundary qualifies when the neighbouring grid slot on that side is occupied by
a cell. Two positions keep centred labels:

- **Outer figure edges.** Ordinary single-plot figures and the outer perimeter
  of a grid are untouched, so no existing non-faceted figure moves a pixel and
  no oracle in the arc needs refreshing.
- **Vacant neighbouring slots.** PRODUCT-0048 deliberately draws partial-row x
  labels into the empty below-slot; a vacant neighbour is space the grid has
  already decided a label may use. Anchoring there would revoke a decision this
  record has no reason to revisit.

### 4. Both axes, always inward

At a boundary to a cell's right, its bound-coincident x label anchors `Right`;
to its left, `Left`. Below it, its bound-coincident y label anchors `Bottom`;
above it, `Top`. In every case the label grows away from the boundary and back
into the cell that owns it.

The vertical half is not anticipatory. Upper cells already spill their bottom
y label into the horizontal gutter in every faceted figure that draws y text;
that spill simply has not met an opposing one, because the lower cell's top tick
is inset 50px by `plot_padding_top` and cannot project upward. A rule written
for the horizontal axis alone would leave a live violation of its own invariant
standing on the other axis.

### 5. Scope is numeric and percent axes; categorical labels are excluded

Bound-coincidence has no meaning for a category tick — a band centre is not an
axis bound — so categorical axes fall outside the rule on its own terms rather
than by special case. This is deliberate narrowing, and the residue is stated
plainly rather than implied:

**An arbitrary categorical string can still project across a boundary, and this
record does not stop it.** Containing arbitrary text requires either measuring
it or clipping it, and §6 declines both. The claim here is precisely
"facet-edge tick labels do not project across occupied shared boundaries", not
"all chrome text obeys cell bounds."

### 6. No text measurement, and no clipping, enters layout

The plot layer measures no text anywhere today. `measure_legend`
(`plot/legend.rs:300`) reads like measurement but is fixed allotment — 150px per
entry column, 25px per row. This record does not change that, and specifically
does not introduce the font pipeline into grid layout.

Anchoring makes the logical text extent grow inward without knowing its width.
Gate 12 separately proves that the shipped renderer's realized glyph ink obeys
that intent for the closed tick formatter and current font pipeline.

The two are not the same claim, and this record does not conflate them.
Alignment governs the logical text box the renderer is asked to place; it cannot
by itself govern ink, because side bearings and renderer and font metrics never
appear in the operation stream. If alignment alone settled containment, gate 12
would be redundant with gates 1 and 2 — it is not, and that is the whole reason
PRODUCT-0057 classifies this property as a backend gap.

The intent carries one limit that is geometric rather than rasterization-
dependent: a single label wider than its cell's entire plot area overflows the
opposite edge instead. That limit is the honest boundary of a no-measurement
remedy, and it is a stop-condition trigger rather than a silent caveat.

### 7. What does not change

`GRID_GAP` stays 12.0. `plot_padding_*` are untouched. PRODUCT-0048's gutter
model and its congruence invariant are untouched — anchoring changes no cell
rectangle, so cells remain congruent by construction. Tick *positions* do not
move; only the anchor of the text drawn at two of them. No mark, gridline,
legend, or panel label is affected. `polariton_session` is unchanged, and no
Rune surface is added: this record introduces no authorable spelling.

## Implementation sequence

**Stage 0 — bank the spill.** Capture faceted figures through the production
renderer and record tick-label ink in **both** gutters as the banked red state:
the vertical gutter between adjacent columns and the horizontal gutter between
adjacent rows. Both censuses are nonzero today, per the Context. Capture the
percent panel's interpenetration as well, but as the contact symptom, not as the
failure definition — a remedy is evidenced by driving both spill censuses to
zero, not by separating glyphs.

**Stage 1 — the anchor decision.** Add bound-coincidence detection and boundary
occupancy to the grid's per-cell chrome inputs. This is a pure decision layer
with no drawing change; it is gated independently so a wrong anchor choice is
attributable before any pixel moves.

**Stage 2 — the numeric emitters, in all three layouts.** Apply the anchors at
every numeric-axis label site and nowhere else:

| layout | function | numeric label/alignment site |
| --- | --- | --- |
| `Cartesian` | `draw_cartesian_axes` (`plot/common.rs:1415`) | y `:1469`, x `:1512` |
| `CategoricalX` | `draw_categorical_axes` (`:1608`) | numeric y `:1660` |
| `CategoricalY` | `draw_categorical_y_axes` (`:1773`) | numeric x `:1822` — the observed defect |

Two sites in range are **excluded** and must be asserted unchanged: the
`CategoricalX` category-x label (`:1687`) and the whole of
`draw_categorical_xy_axes` (`:1850`, including `:1897`), whose axes are
categorical on both sides. `:1897` is named explicitly because it is the
plausible mis-target: it is the nearest rotation site to the observed defect and
lies in a different function, so an implementation that follows proximity rather
than layout would edit a heatmap category label and leave the reported defect
in place.

## Acceptance gates

1. **The observed defect, horizontally.** In a two-column facet grid on a
   percent x axis, the left cell's `100%` is emitted with `Horizontal::Right`
   and the right cell's `0%` with `Horizontal::Left`.
2. **The corresponding vertical boundary.** In a two-row grid on a percent y
   axis, the upper cell's bound label is emitted with `Vertical::Bottom` and the
   lower cell's with `Vertical::Top`.
3. **Both labels survive.** Neither endpoint label is suppressed, and the tick
   count per cell is unchanged from the banked state. This gate exists because
   the cheapest wrong fix — dropping one label — would pass gates 1 and 2
   vacuously if they only asserted "no overlap".
4. **Outer edges stay centred.** The grid's outer perimeter labels, and every
   label of a single-panel figure, remain `Center` on both axes.
5. **Vacant neighbours stay centred.** A three-cell grid in a 2×2 arrangement
   keeps centred labels on the boundary facing the empty slot, and PRODUCT-0048's
   partial-row below-slot behaviour is asserted unchanged.
6. **One-cell parity.** A one-cell figure is byte-identical to the banked
   oracle.
7. **Bound-coincidence, positively and negatively, on both axes.**
   `y_bounds(0, 50)` with step 10 anchors its last tick; `y_bounds(0, 42)` with
   step 10 leaves its last tick (40) `Center`. The negative half is the gate
   that distinguishes this rule from "anchor the last generated tick" — without
   it the two designs are indistinguishable. The negative case is asserted for
   **both** x and y, unless the implementation routes both axes through one
   shared coincidence helper, in which case the gate asserts that structurally
   and tests the helper once. Coverage by shared implementation must be proven,
   not assumed: two emitters that happen to agree today are not one helper.
8. **All three numeric layouts are exercised.** Gates 1, 2, and 7 are run
   against `Cartesian`, `CategoricalX` numeric-y, and `CategoricalY` numeric-x.
   `CategoricalY` carries the observed defect and `Cartesian` is the layout most
   likely to be left behind, since no reported figure exercises it.
9. **Excluded sites are unchanged.** The `CategoricalX` category-x label
   (`:1687`) and every label in `draw_categorical_xy_axes` (`:1850`) remain
   `Center` in all positions, including at occupied interior boundaries. This
   gate fails an implementation that generalizes anchoring to categorical text.
10. **Geometry is untouched.** `GRID_GAP` is asserted 12.0, cell rectangles are
   asserted congruent and identical to the banked values, and tick positions are
   asserted unchanged.
11. **No measurement entered layout.** A structural guard that the grid and
   chrome layout paths acquire no font, glyph, or text-extent dependency.
12. **Containment through the production renderer, measured as text
    contribution.** By PRODUCT-0057's classifier, "no label ink lies in the
    gutter" is a **backend gap**: the draw list carries content, position, size,
    and anchor but never the realized extent, so two renderers can disagree and
    no assertion over the operation list can settle it.

    A plain census of gutter pixels does not discriminate, because axis tick
    strokes and other furniture legitimately occupy that region — such a gate
    could read nonzero against a fully correct implementation, and would be
    reading furniture rather than text. The gate therefore isolates the **text
    contribution** by differencing paired production-path captures that differ
    in exactly one input, `tick_label_text`:

    - **Capture A** sets `tick_label_text` to a unique sentinel colour.
    - **Capture B** sets `tick_label_text` to `canvas_background`.
    - Geometry, emitted operations, and every other chrome role are identical
      between the two.
    - The two captures are differenced over both gutter regions.

    The difference is by construction the tick-label glyph ink and nothing else:
    furniture is common to both captures and cancels. This needs no test-only
    visibility seam, no suppression path that production never takes, and no
    production colour role that exists only to be measured — the probe drives
    the shipped renderer through a setting it already has.

    The census runs over **both** gutter regions — vertical (between columns)
    and horizontal (between rows) — since one column proves only the horizontal
    case, and §4's vertical spill is live today. Accepted state is zero text
    contribution in both; the Stage 0 banked nonzero censuses are the negative
    control, proving the probe detects the spill it claims to exclude.

    Gates 1, 2, and 3 remain as anti-vacuity proof over the draw operations: a
    capture showing zero gutter text is equally consistent with correct
    anchoring and with the endpoint labels having been deleted, and only the
    operation-level gates distinguish those two.

## Stop condition

Halt and report rather than widening the record if any of these fire:

1. **Label rotation becomes reachable from the authored surface.**
   `x_label_rotation` defaults to 0.0 (`plot/common.rs:483`) and appears in no
   file outside `plot/common.rs` — nothing in the settings cascade or the Rune
   surface writes it. Anchoring proves containment only for axis-aligned text; a
   rotated label's extent along the boundary axis depends on its unmeasured
   width. Making rotation authorable invalidates this record's mechanism and
   needs its own decision.
2. **A label wider than its cell's plot area is reached.** §6's guarantee
   inverts there — the anchored label overflows the opposite edge — and the
   remedy requires measurement or clipping, both declined here.
3. **Containment is demanded for categorical labels.** §5 excludes them
   deliberately; extending to them is the measurement decision, not this one.

## Supersession

Supersedes nothing. Extends PRODUCT-0068's containment principle from
`Stroke(grid_color)` (`plot/grid.rs:3713`) to bound-coincident numeric tick
labels. PRODUCT-0048's gutter model, congruence invariant, and partial-row
below-slot behaviour are honoured unchanged and are gated as such (gates 5, 10).

## Rejected

**Measured gutter — compute the gap from actual label extents.** Attractive
because it is correct for all text including categorical strings and rotated
labels, and because "reservation, not a constant" is the lesson PRODUCT-0074
just taught. Rejected because it introduces the font pipeline into a layout
layer that has never had one; because PRODUCT-0048's congruence forces a single
global gutter, so the widest boundary in the grid would dictate every gap and
spend plot area everywhere to fix one place; and because the gutter would then
depend on data and tick format, resizing every cell when a label changes.

**Suppress the interior endpoint label.** Attractive because it costs nothing,
needs no measurement, and is what several plotting libraries do. Rejected
because on a percent axis it deletes `0%` and `100%` — the two most informative
labels on the scale — and in general it removes the reader's only statement of
where a cell's domain ends.

**Widen `GRID_GAP`.** Attractive as a one-line change. Rejected as the
constant-instead-of-containment substitution: it fixes the instance, fails at the
next wider label, and spends plot area in every grid including those that never
had a collision.

**Clip labels to the cell.** Attractive because it makes containment
unconditional and reuses the clip machinery PRODUCT-0068 already applies to
gridlines. Rejected because clipping truncates glyphs mid-character: `100%`
becomes `10` and silently reports a different value. An overlap is visibly
wrong and a truncation is invisibly wrong, and the arc has consistently refused
the drawable-but-misread option — most recently in declining range-only
whiskers.

## Consequences

Pixels move only in faceted figures that satisfy all three conditions at once:
an occupied interior boundary, a numeric or percent axis, and a tick coincident
with the bound. No single-panel figure changes, so no existing oracle in the arc
requires refreshing — which is the specific reason the scope was narrowed to
interior boundaries rather than written as a general edge rule.

That set is wider than the reported defect, and deliberately so. Because spill
rather than contact is the failure, every faceted numeric panel currently
spilling a bound-coincident label — which is most of `test21.rn`'s faceted half,
not only `horizontal-faceted-percent` — moves. Scoping to the visibly
interpenetrating panel would have left the same violation standing everywhere it
had not yet become legible, and would have required this record to be reopened
by the next wide tick format.

`uat/test21.rn` becomes this record's exhibit and regression fixture, now
repository-owned and reachable from a fresh clone.

Three kinds of chrome text remain uncontained and are named rather than
implied: categorical band labels of arbitrary length, any future rotated label,
and a label wider than its own cell. Each has a stop-condition trigger. The
common remedy for all three is text measurement, which this record declines and
a later one may accept — at which point the natural shape is to reopen the
measured-gutter option above with a real consumer behind it, rather than to
generalize anchoring past what alignment can prove.
