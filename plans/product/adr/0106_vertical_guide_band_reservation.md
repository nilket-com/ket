# ADR 0106: vertical guide band reservation

Status: proposed 2026-08-14. The plot area has never reserved a band for the
title, the facet label, or radar's tick labels — it works for most marks only
because most marks do not draw where those live. PRODUCT-0105's eight-dimension
parallel figure draws exactly there, and three collisions became visible at once.
This record reserves three vertical bands from **resolved offsets and font
sizes**, adds no measurement, and changes **no geometry formula** — though it
necessarily moves realized coordinates in titled and faceted figures, which §6
admits and gates.

It precedes violin, the arc's last step.

## Context

### The title band is not reserved; it is merely usually empty

`title_offset: 20.0` and `subtitle_offset: 50.0` (`common.rs:527-528`) are fixed
offsets from the **bounds top**, and `editor.rs:1528` builds the transformer from
the **whole widget rectangle**. The plot area therefore *includes* the title band.

That is invisible for Cartesian marks, whose axes are at the bottom and whose data
occupies the middle. It is immediately visible for parallel coordinates, whose
dimension headers are drawn at `transform.categorical(i, range.1)` — the top of
the transformer's bounds — by shared `draw_parallel_axes` (`common.rs:2218`).

`grid_plot_area` (`grid.rs:877-886`) reserves only `plot_padding_top`, which is
why the collision is **layout-dependent**: a tall cell's padding hides it, a tight
one exposes it. The top-right panel of `test33.rn` shows `d3`/`d7` overlapping
"Parallel · eight authored dimensions"; the taller faceted panel below does not.

### Parallel emits no text of its own

The kernel has **zero** `fill_text` calls. Every dimension header comes from
shared layout, so this is a layout defect that happens to surface through one
mark — and its repair benefits every mark that draws near the top.

### Radar labels ticks on every spoke

Two `fill_text` sites (`radar.rs:128`, `:161`). The vertical spokes are legible;
the horizontal pair run their labels **through the plot body**, producing a dense
numeric band across the centre. The rule scales badly with spoke count and is
already unreadable at four.

### Facet labels are drawn into the cell body

`grid.rs:753`, at the cell's horizontal centre — so in `test33.rn`'s faceted
panel `East` and `West` appear *interleaved* between `d3` and `d7` rather than
heading their cells.

### All three are vertical, and none needs glyph widths

The title band is `subtitle_offset + subtitle_size`, both already in
`ResolvedPlotSettings`. Radar thinning is a **count** decision. A facet row's
height is font-size-derived. PRODUCT-0093 §7's measurement deferral does not
fire — see §5, which keeps it that way deliberately.

## Decision

### 1. The plot area reserves the title band

Reserved height derives from the resolved values already present:
`subtitle_offset + subtitle_size` when a subtitle is shown, `title_offset +
title_size` when only a title is, and **zero when neither is** — a figure without
a title must not lose vertical space to an empty band.

The reservation is applied where the plot area is computed, so both the
single-plot path (`editor.rs:1528`) and `grid_plot_area` (`grid.rs:877`) inherit
it rather than each subtracting its own.

### 2. Facet labels get their own row

A font-sized row above each cell's plot area, not the cell body (`grid.rs:753`).
Its height derives from the resolved facet label size; when no facet label is
shown, the row is zero.

### 3. Radar labels ticks on one dedicated spoke

Tick labels are drawn on a **single** spoke rather than all of them. Every other
spoke keeps its dimension name and loses its numeric ladder.

**Which spoke is a decision, not an accident**: the **first** dimension in
authored order, so PRODUCT-0105's authored dimension order determines it and an
author can move the labelled axis by reordering. Choosing "the vertical one" would
make the labelled spoke depend on dimension count.

### 4. Reservation composes; it does not stack per producer

Title band, facet row, and plot area are computed **once, in order**, by one
authority. Each producer draws into the band it was given.

Three producers each subtracting their own height is how the current defect
arose in miniature — `grid_plot_area` already subtracts `plot_padding_top` while
the title subtracts nothing, and the two do not know about each other.

### 5. Horizontal header fitting is explicitly deferred

Eight short dimension names (`d8`, `d1`, …) fit; eight long ones would not. This
record does **not** address horizontal crowding of dimension headers.

That is a **fitting** question — does this text fit this width — and
PRODUCT-0093 §7 deferred backend text measurement with exactly that trigger:
*"the first product requirement for tight fitting rather than safe
non-clipping."* Under PRODUCT-0057's classifier it is a **backend gap** needing
production-renderer evidence, not an assertion gap.

Keeping it out is the point. A vertical-band repair that quietly opened the
measurement door would take an architectural decision as a side effect of a
layout fix.

### 6. What this record does not do

No backend measurement (§5). No changes to title or subtitle content, offsets, or
sizes. No horizontal fitting. No new authored options.

**But realized geometry does move, and the record must say so.** An earlier draft
claimed "no mark geometry," which is impossible: reserving a band shrinks the plot
area, and every plotted coordinate is expressed relative to it. The honest
statement separates the formula from its result:

| what | changes? |
|---|---|
| kernel geometry **formulas** | **no** — every mark computes as before |
| realized coordinates in a **titled or faceted** figure | **yes** — offset or compressed by **exactly** the reserved bands |
| realized coordinates in an **untitled, non-faceted** figure | **no** — §1's zero case |
| radar's spokes | move only by the shared band, like every mark; **§3 changes only tick text** |

So PRODUCT-0104's and PRODUCT-0105's geometry decisions are untouched as
decisions. What moves is where their output lands, by a known amount, and gates
9, 11, and 12 pin that amount rather than asserting it is zero.

## Implementation sequence

**Stage 0 — bank.** Every figure with a title, every faceted figure, and every
radar — the three producers' current output, including the `test33.rn`
collisions as **negative controls**.

**Stage 1 — the reservation authority.** §§1, 2, 4, with no producer migrated.

**Stage 2 — the consumers.** Plot area, facet row, and the single-plot path
consume it.

**Stage 3 — radar ticks.** §3.

## Acceptance gates

1. **Parallel's headers clear the title.** In the tight grid cell that collides
   today, `d3` and `d7` render below the subtitle. Against the banked collision.
2. **A figure with no title loses no space.** Plot area with neither title nor
   subtitle is byte-identical to Stage 0 — §1's zero case, which a
   constant-height reservation would fail.
3. **Title-only and title-plus-subtitle reserve different heights.** Two cases,
   since one height for both would either overlap the subtitle or waste a band.
4. **Facet labels head their cells.** `East` and `West` render in their own row,
   not interleaved among the dimension headers. Against the banked interleaving.
5. **A non-faceted figure gains no facet row.** §2's zero case.
6. **Radar labels ticks on exactly one spoke.** The other spokes carry dimension
   names and **no** numeric ladder — both halves, since removing all tick labels
   would satisfy the first alone.
7. **The labelled spoke follows authored order.** Reordering `radar(...)`'s
   dimensions moves the labelled spoke to the new first dimension — §3's decision,
   which a "pick the vertical one" implementation fails.
8. **Reservation is computed once.** Structural: one authority produces the title
   band, facet row, and plot area, and no producer subtracts its own height.
   Per §4, and the reason the current defect exists.
9. **Untitled, non-faceted figures are byte-identical.** §1's and §2's zero
   cases: with no title, no subtitle, and no facet label, the plot area and every
   rendered coordinate match Stage 0 exactly. This is the gate that catches a
   reservation applied unconditionally.
10. **No backend measurement capability is added.** `PlotBackend` gains no glyph
    or text-extent method — structural, per §5, so the deferral cannot erode into
    "just enough measurement to place a header."
11. **Titled and faceted figures move by exactly the reserved bands.** Not
    byte-identical — an earlier draft asserted that, which contradicts §1. Two
    parts:

    - **the plot bounds** equal the previous bounds inset by the computed band
      heights, to the pixel;
    - **the data mapping is proportional** — a value at a known fraction of its
      domain lands at the same fraction of the **new** plot area.

    Together these say the formula is unchanged and only its input rectangle
    moved. A gate asserting byte-identity would fail a correct implementation; one
    asserting only "something moved" would pass a wrong inset.
12. **Kernel geometry formulas are unchanged.** Structural: no mark's coordinate
    computation is edited, and radar's only edit is §3's tick text. The band
    reservation happens entirely in the plot-area authority.

## Stop conditions

> If any of the three reservations needs a realized glyph width, stop.
> PRODUCT-0093 §7's trigger has fired and this is no longer a layout record — it
> is the backend-measurement decision that record deferred, and it needs
> production-renderer evidence rather than a band constant.

> If a titled Cartesian figure moves by anything **other than** the reserved band
> height, stop. §1 expects it to move by exactly that; a different offset means
> the reservation is being applied more than once or composed wrongly, which is
> the defect §4 exists to prevent. And if an **untitled** figure moves at all, the
> reservation is unconditional and gate 9 has caught it.

> If the labelled radar spoke cannot follow authored order, stop. §3 chose the
> first authored dimension so the choice is the author's; falling back to a
> geometric rule would make PRODUCT-0105's ordering decision partly cosmetic.

> If a fourth text producer turns out to draw into a reserved band, stop and widen
> the survey. Three were found by one exhibit; a fourth means the bands are not
> enumerable from the producers this record traced.

## Supersession

Supersedes no record. Repairs a layout defect exposed by PRODUCT-0105's
eight-dimension figure. Leaves PRODUCT-0093 §7's measurement deferral **standing,
with its trigger intact** (§5). Changes no geometry **decision** of PRODUCT-0104
or PRODUCT-0105, while moving realized coordinates in titled and faceted figures
by the reserved band heights (§6).

## Rejected

**Reserve a fixed-height title band.** Attractive because it is one constant and
needs no resolved values. Rejected because a figure without a title would lose
the space anyway, and a figure with a large title font would still collide — the
band must derive from the same values that place the text.

**Fix parallel's header placement in the parallel kernel.** Attractive because
parallel is where the collision shows. Rejected because parallel emits no text at
all: the headers come from shared `draw_parallel_axes`, and a parallel-local fix
would leave the band unreserved for the next mark that draws at the top.

**Thin radar tick labels by count rather than choosing one spoke.** Attractive
because it keeps ticks on every axis and only reduces density. Rejected because
the collision is *radial*, not dense — horizontal spokes put labels through the
plot body regardless of how few there are.

**Measure the headers and thin them to fit.** Attractive because it would solve
horizontal crowding at the same time. Rejected as §5: that is a different class of
problem with a deferral and a trigger already recorded, and bundling it would take
an architectural decision as a side effect.

**Let each producer subtract its own band.** Attractive because it needs no shared
authority and each producer knows its own height. Rejected because that is the
present defect: `grid_plot_area` subtracts `plot_padding_top`, the title subtracts
nothing, and neither knows about the other.

## Consequences

Three collisions resolve, and the underlying cause is named: the plot area has
always been the whole widget, and the title band was safe only by convention.
Parallel coordinates were the first mark to violate the convention, which is why
eight dimensions exposed something that had nothing to do with dimension count.

**Every titled figure in the product moves**, by exactly the band it should always
have reserved. That is the cost of the repair and the reason §6 separates formula
from placement: a record claiming "no geometry change" would have been describing
a fix that cannot exist, and its parity gate would have failed a correct
implementation while passing an unreserved one.

PRODUCT-0093's measurement deferral survives with its trigger intact, and §5
records why the temptation was real: horizontal header fitting sits one step from
these three defects, shares a symptom with them, and would have opened the
backend-measurement door as a side effect of a layout repair.

This is the sixth defect in the arc surfaced by a new figure rather than a survey,
and the third — after PRODUCT-0093's clipped gradient and PRODUCT-0101's
chord-closed arcs — where the figure had already passed review. Exhibits keep
finding what gates cannot express, and a signed-off exhibit is not an exhausted
one.
