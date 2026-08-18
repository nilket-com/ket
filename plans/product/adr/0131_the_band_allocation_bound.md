# ADR 0131: the band allocation bound

Status: proposed 2026-08-18. A band requirement is a *demand* computed without
reference to the figure it will occupy. `plot_area` currently treats that demand
as an *allocation*, subtracts it unconditionally, and clamps the remainder with
`.max(0.0)`. A figure whose bands outgrow it therefore renders a zero-height
plot and reports success.

This record separates demand from allocation. Requirements keep knowing nothing
about pane size -- that independence is what PRODUCT-0114 bought and
PRODUCT-0127 preserved -- and the allocation that consumes them becomes bounded
by the container it is dividing.

## Context

### The collapse is banked, not inferred

Stage 0 evidence is in the tree (`test: bank the container collapse baselines`,
`f6ee81ad`), rendered on the PRODUCT-0057 designated adapter. A line figure over
a `Datetime(Milliseconds, UTC)` axis whose Count labels are 19 characters, with
an authored `x_axis().tick_rotation(...)`:

| figure | 36 degrees | 45 degrees | 90 degrees |
|---|---:|---:|---:|
| 480x360 | **34.7 px** plot | 5.9 px | 0.0 px |
| 320x240 | 0.0 px | 0.0 px | 0.0 px |

Bottom-band ink is nonzero in every row (27, 253, 124, and 108 in all three
collapsed 320x240 rows). The figure draws label ink into a band belonging to a
plot that no longer exists.

That count is every luminous pixel below the plot area, so it includes the
x-axis title as well as the tick labels; it is deliberately not called tick ink.
It is sufficient for the claim being made -- reserved space still receives ink
after the plot is gone -- and Stage 1 should isolate tick-label ink if it wants
to assert anything narrower.

The 480x360 / 36-degree row is the discriminating control. Without a row that is
genuinely nonzero today, every collapse row would read zero both before and
after any repair, and the bank would prove nothing.

### It is not a rotation defect, and not a temporal one

Rotation is the fastest route to a large band, not the cause. The bottom band is
`x_label_offset + estimated_x_width * sin(theta) + x_tick_size * cos(theta)`, so
the dominant term is the label's *estimated width*: at 19 characters and an
18 px tick size the run is 256.5 px before any rotation is applied. At 320x240
the plot is already zero at 36 degrees, the shallowest angle measured.

The same shape exists on the left edge. PRODUCT-0126 made that band a sequence
of tick run plus title, and PRODUCT-0127 made its width the exact selected label
set; neither bounded it against the figure. A sufficiently long y category
collapses a narrow pane by the same arithmetic.

### Why this cannot be an authoring refusal

The arc already refuses capacity conditions at the authoring span:
`MAX_GENERATED_TICK_POSITIONS`, and PRODUCT-0130's over-dense `tick_every`.
Those are knowable before layout from domain and policy alone.

Band-exceeds-figure is not. It depends on the pane's current size, which changes
as the user drags a divider. Refusing it at admission would either import
geometry into requirements -- reopening the `extent -> band -> ticks -> extent`
cycle PRODUCT-0114 broke and PRODUCT-0127 §2 restated -- or make a program's
validity flap between refusal and acceptance as the window resizes. Neither is
acceptable, so the decision belongs at render time.

### Half of the idiom already exists, in one path only

`grid_tiled_area` (`plot/common.rs:2015-2035`) already clamps each band against
its container: `left.min(outer.width())`, `bottom.min(outer.height())`, and
`heading.min(outer.height() - bottom)`. `plot_area` (`:1997-2013`) does not; it
subtracts the full demand and clamps only the remainder.

So the two paths disagree today, and the grid's clamp is incomplete anyway: it
bounds each member at *the whole container*, which prevents negative geometry
but still permits a zero-extent plot. This record is therefore less an invention
than the completion of an idiom the grid path started -- and it removes an
inconsistency between composed and single figures that no record chose.

### The distinction the code is missing

`AxisBandRequirement` computes what the labels *need*. `plot_area` decides what
they *get*. Today those are the same number by construction, so an unsatisfiable
demand silently consumes the entire figure. Every rule the arc has written about
requirements -- domain-derived, geometry-free, exact -- is about the demand. No
record has yet said anything about the allocation.

## Decision

### 1. A band's allocation is bounded by the figure it divides

`plot_area` bounds the total reserved extent on each axis so the plot retains a
stated minimum share of the figure. Each axis has a *leading* and a *trailing*
band, and the bound divides between them explicitly rather than scaling both:

```text
bound_v    = height * (1 - PLOT_MIN_SHARE)
leading_v  = min(heading + facet + padding_top, bound_v)
trailing_v = min(bottom, bound_v - leading_v)

bound_h    = width * (1 - PLOT_MIN_SHARE)
leading_h  = min(left, bound_h)
trailing_h = min(right, bound_h - leading_h)
```

**The leading band is satisfied first, and the trailing band takes what
remains.** A leading band that exceeds the bound on its own is clamped to it,
leaving the trailing band zero. Content that does not fit its allocation clips
per §3.

The priority is not arbitrary. The leading bands carry the figure's identifying
text and its value-axis labels -- a title and a y scale -- while the trailing
bands carry x tick text and the axis title, whose truncation PRODUCT-0119
already established as an acceptable visible outcome. Given a forced choice, the
arc has already decided which text may be cut.

The requirement itself is untouched. Nothing upstream of `plot_area` learns the
container size, so PRODUCT-0114's cycle prohibition and PRODUCT-0127 §2 hold
exactly as written.

`PLOT_MIN_SHARE` is a single banked layout constant, in the form
PRODUCT-0126 used for `Y_AXIS_TITLE_GAP`: a number justified by Stage 0's
measurements rather than derived from a formula that does not exist.

### 2. A plot area is never zero when the figure is not

If the figure has positive extent, the plot has positive extent. The
`.max(0.0)` clamp remains as a floor against degenerate figures, but it stops
being reachable from an ordinary band computation. A collapsed plot is a
rendering outcome no author can request and no label length can force.

### 3. The overflow clips, and that is the visible signal

When the demand exceeds the allocation, the excess band content is clipped at
the figure edge exactly as PRODUCT-0119 already clips endpoint labels. Truncated
tick text *is* the diagnostic: it is visible, it is local to the labels that
caused it, and it resolves itself when the pane grows.

No refusal, no status-bar message, and no automatic font shrinking. A message
would flap on resize; shrinking text would make an estimate choose a glyph size,
which PRODUCT-0116 prohibits and which would reintroduce the measurement
PRODUCT-0093 §7 defers.

### 4. Bounding is proportional, not per-occupant

The bound applies to the axis total, not to individual band members. It does not
re-rank heading against bottom, or tick run against axis title; those orders are
PRODUCT-0106's, PRODUCT-0115's, and PRODUCT-0126's, and this record does not
reopen them. When the total is cut, each occupant keeps its existing sequence and
the last content simply falls outside the figure.

### 5. Grids bound once, figure-wide

PRODUCT-0048's congruence rule is unchanged. A grid applies the bound to its
shared reservation before cells divide, so every cell keeps the same geometry. A
per-cell bound would make cells differ by their own label lengths, which is the
lie PRODUCT-0048 exists to prevent.

`grid_tiled_area`'s existing per-member clamps are replaced by the same bound,
not layered under it. Leaving both would give composed figures two collapse
policies, which is the disagreement §"Half of the idiom" identifies; the grid
keeps its own reservation order, and only the bound becomes shared.

## Implementation sequence

**Stage 0 -- bank the collapse.** Complete, `f6ee81ad`. Six rendered rows with a
discriminating nonzero control, plus the nonzero bottom-band ink proving label
content still draws into the reserved region.

**Stage 1 -- bound the allocation.** Introduce `PLOT_MIN_SHARE` and the bounded
subtraction in `plot_area`, leaving every figure whose bands already fit
byte-identical. Only the banked collapse rows move.

**Stage 2 -- restate the bank.** Replace the collapse baselines with their
bounded values and assert the zero rows are now unreachable.

## Acceptance gates

1. **Every figure keeps the promised share, on both axes.** For every Stage 0
   row, including 320x240 at 90 degrees:

   ```text
   plot_area.height >= height * PLOT_MIN_SHARE
   plot_area.width  >= width  * PLOT_MIN_SHARE
   ```

   Asserting only `height > 0` would pass on a one-pixel plot and would leave
   `PLOT_MIN_SHARE` a number the tree never checks. The banked pre-change zeros
   are the negative control and must be cited, not deleted.
2. **The discriminating control survives.** The 480x360 / 36-degree row remains
   nonzero before and after, and its exact bounded height is banked. A gate that
   only asserts "greater than zero" everywhere passes trivially once §2 holds.
3. **Requirements are unchanged.** For every Stage 0 figure, the
   `BandRequirements` values and estimated widths are byte-identical before and
   after. A mutation that bounds the requirement instead of the allocation fails
   this gate and gate 4 together.
4. **Requirements consult no geometry.** The requirement computation still takes
   domain, axis type, placement, and font size only. Structural, over the
   production signature, as PRODUCT-0127 gate 9 established.
5. **Figures that already fit do not move.** Every existing UAT capture, oracle,
   and banked geometry is byte-identical. The bound is reachable only by a
   figure that would otherwise have collapsed.
6. **Overflow clips rather than relocates.** No label acquires a translation,
   scale, or alignment it did not have before, and none is dropped to make it
   fit. Each label keeps the same origin *relative to its own tick*, and each
   tick keeps the same position *relative to the plot area*.

   The gate is stated relatively on purpose. The allocation deliberately moves
   the plot edge, so absolute tick coordinates *do* change and must be allowed
   to; a gate demanding unchanged absolute positions would fail against a
   correct implementation. What is forbidden is any extra per-label adjustment
   layered on top of that. This is PRODUCT-0116 and PRODUCT-0119 jointly, and it
   is what separates this record from a layout heuristic.
7. **No text measurement capability.** `PlotBackend` gains no glyph-extent API.
   PRODUCT-0093 §7 holds.
8. **Grid cells remain congruent.** A faceted figure whose shared band would
   exceed the grid bounds once, not per cell; cell geometry stays equal.
9. **The bound is a banked constant with a stated basis.** `PLOT_MIN_SHARE` is
   justified by a Stage 0 measurement, and a test pins the share it guarantees
   at the banked figure sizes.

## Stop conditions

> If bounding the allocation requires any requirement to learn the container
> size, stop. That is the PRODUCT-0114 cycle returning through the back door,
> and it invalidates PRODUCT-0127's selected-label exactness with it.

> If a bounded band cannot be implemented without moving tick ink, stop. The
> estimate may lose space; it may never reposition a label.

> If the overflow needs an author-visible diagnostic to be understandable, stop.
> A runtime condition that changes on resize needs its own reporting decision,
> and this record deliberately does not make one.

> If a grid needs a per-cell bound to keep its plots visible, stop. That
> contradicts PRODUCT-0048's congruence rule and needs a different layout
> authority.

> If `PLOT_MIN_SHARE` cannot be banked without realized glyph extents, stop.
> That is PRODUCT-0093 §7's measurement decision.

## Supersession

Supersedes nothing. It adds the allocation rule that PRODUCT-0106,
PRODUCT-0114, PRODUCT-0115, PRODUCT-0126, and PRODUCT-0127 each assumed without
stating: that a band's demand can always be paid. Their derivations, sequences,
and exactness claims are unchanged, and remain the input this record bounds.

## Rejected

**Leave the clamp silent.** It is the status quo and it renders a figure
indistinguishable from a broken one. The arc's "valid-but-ugly renders" doctrine
covers figures that are hard to read; a plot of zero height is absent, not ugly.

**Refuse at the authoring span.** It matches `tick_step` capacity and would give
a precise, local message. But band-exceeds-figure depends on the pane's current
size, so the same program would be valid at one window size and refused at
another, and requirements would have to import geometry to know.

**Shrink the tick font until the band fits.** The most user-pleasing option, and
the one that breaks the most rules: an estimate would choose a glyph size, which
PRODUCT-0116 prohibits, and knowing whether the shrink succeeded needs the
measurement PRODUCT-0093 §7 defers.

**Bound each band member separately.** It would preserve more of each occupant.
It also re-ranks occupants against each other, quietly reopening the sequences
PRODUCT-0115 and PRODUCT-0126 established.

**Let the author cap the band with `padding()`.** PRODUCT-0120's authored
padding is a floor, not a ceiling, and inverting it for this case would give one
member two opposite meanings.

## Consequences

Long labels stop being able to erase a plot. On a small pane they will be
clipped instead, which is worse-looking and strictly more informative: the
figure still shows its marks, and the truncated text names its own cause.

The durable change is the distinction. Every band record so far has reasoned
about what labels *need*, and the code has silently equated that with what they
*get*. Separating demand from allocation is what makes a reservation safe to
compute in ignorance of the figure -- which is the property the whole band arc
depends on, and the one it never wrote down.
