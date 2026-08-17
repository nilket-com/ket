# ADR 0126: the left axis-title sequence

Status: proposed 2026-08-17. PRODUCT-0114 replaced the left edge's fixed
`plot_padding_left` with a requirement-derived band, but treated its two text
occupants as alternatives. A long y-tick label therefore occupies the same
space as the rotated y-axis title. The Multivariate showcase makes the defect
plain: `Reliability` draws through `capability`.

This record makes the left edge a sequence. It generalizes the rule
PRODUCT-0115 applied to the bottom edge: **a band is a maximum only when its
occupants can overlap; when one must clear another, it is a sequence.**

## Context

### The current reservation and placement disagree

`VerticalGuideBands::resolve_with_requirements_and_x_label`
(`common.rs:1494-1501`) computes the left band as:

```text
tick_width = 10 + estimated_y_width(y_tick_size)
title_width = y_label_padding + y_label_size
left = max(tick_width, title_width)
```

The draw path independently puts the title at:

```text
x = plot_area.x - y_label_padding
```

The two formulas are compatible only while the tick run ends before the fixed
85 px title offset. For `Reliability`, the declared ASCII budget estimates
eleven characters at roughly 198 px. Its right-aligned tick run reaches about
208 px left of the plot area, while the title remains at 85 px—inside the run.

This is not an estimator failure. The conservative estimate did reserve enough
space for the ticks; the placement model put unrelated ink inside that space.
Adding right padding only narrows the plot from the opposite edge and cannot
repair an overlap on the left.

### The same shape was repaired below the plot

PRODUCT-0114 initially described the bottom band as a maximum. PRODUCT-0115
found that rotated x-tick labels and the x-axis title stop overlapping once the
ticks extend far enough, and replaced the max with a sequenced guide layout.
The bottom title now clears `tick_end` and both reservation and placement read
the same value.

The left edge always had the same two-occupant shape. The earlier repair was
correct locally and incomplete globally: it did not survey the other edges.
The Multivariate heatmap is the third showcase exhibit to expose a geometry
rule that the existing gate suite did not generalize.

### An unreserved axis title has no placement at all

The same rule has an opposite endpoint. `draw_axis_labels` now skips the shared
Cartesian title draw when both `BandRequirements` are empty. Radial and
parallel kernels may expose field names for hover or diagnostics, but their
layouts have neither a Cartesian axis band nor a title placement.

Before that guard, a visible resolved label reached the missing Cartesian
guide path. The shared guide authority asserts that every visible Cartesian
axis title has a placement; radial and parallel layouts reserve no such band,
so an ordinary figure authoring `x_axis().label(...)` violates that assertion
and panics. An empty label merely happened not to take the path. Existing UAT
exhibits had not exercised an authored unbanded axis label; the Multivariate
showcase reached the same path through parallel's nonempty fallback label.
That is why this valid Rune path had escaped the gate suite.

The rule is the same in both cases: **an axis title is placed by the authority
that reserved space for it, or it is not drawn.** The left sequence supplies
that authority for Cartesian y titles; the radial/parallel guard correctly
declines to invent one.

### The four-edge census closes the class

Stage 0 must enumerate every reservation, placement, and clip on every edge:

| edge | current occupants | current authority | decision in this record |
|---|---|---|---|
| top | title and subtitle | PRODUCT-0106 heading maximum | inherit |
| bottom | x ticks and x title | PRODUCT-0115 guide sequence | inherit unchanged |
| left | y ticks and y title | PRODUCT-0114 maximum plus an independent constant placement | replace |
| right | legend gutter and authored plot padding | PRODUCT-0093 / PRODUCT-0120 | survey; no automatic label reservation is assumed |

The legend is not silently counted as a right-edge band: `legend_layout` first
shrinks the content bounds, while `padding().right(...)` is an authored floor.
If the survey finds an additional automatic right-edge occupant, it stops
rather than smuggling a fourth-band decision into this record.

## Decision

### 1. The left edge is a shared sequence

The y-tick run and visible y-axis title are horizontally disjoint occupants.
The title must sit outside the tick run, not compete with it for a maximum.

```text
tick_end = 10 + estimated_y_width(y_tick_size)
title_x  = max(y_label_padding, tick_end + title_gap)
left     = title_x + y_label_size
```

All values are distances left from the plot area. `title_x` drives the title's
draw position and `left` drives the reserved geometry. There is no independent
`y_label_padding` consumer after this record.

`title_gap` is banked from the short-label legacy geometry in Stage 0. It is a
single guide-layout constant, not a new authored setting: its job is preserving
the already-acceptable short-label placement while making long ticks clear the
title.

If the title is hidden, `left = tick_end`; no gap is spent on absent ink.

### 2. One guide layout owns both halves

Introduce the left-edge equivalent of PRODUCT-0115's `XAxisGuideLayout`. It
owns `tick_end`, optional `title_x`, and final `left`; `VerticalGuideBands`,
the title draw path, single plots, and grids read it. No renderer re-derives a
placement from `y_label_padding`, and no grid adds a local inset.

This is the one-authority property PRODUCT-0106 established. A correct band
with an independently placed title still renders the collision this record
exists to remove.

### 3. Estimates reserve geometry; they do not choose a glyph's relative placement

PRODUCT-0116 remains in force. The y-tick estimate may grow the band and move
the **plot edge** right; it must not shift individual tick labels or alter
their tick-relative position. The title is a distinct figure-level occupant,
and its placement follows the guide layout as a whole, never any realized or
per-label width.

This is the safe analogue of the x-axis repair: the estimate changes the
available space, while each tick remains where its transform places it.

### 4. Short labels preserve their accepted placement; long labels move honestly

When `tick_end + title_gap <= y_label_padding`, the title remains at the
legacy offset and the new formula reduces to today's geometry. When it
exceeds that offset, the plot moves right by exactly the guide's growth and the
title follows the tick run outward by the same authority.

This is **not** byte-identical parity for all figures. Long y labels are a
known geometry move, and the record asserts the exact delta instead of waiving
it. The Multivariate heatmap is the acceptance exhibit: `Reliability` must no
longer overlap `capability`.

### 5. Grids reserve the sequence once, figure-wide

PRODUCT-0048's grid rule remains unchanged. The grid takes the maximum
left-guide requirement across its layouts once before cells divide. Facet cells
therefore remain congruent even when only one has long labels, and the global
y-axis title clears the widest tick run without a second local reservation.

### 6. Right-edge policy is inherited, not extended

This record changes no right-edge geometry. PRODUCT-0093's legend gutter and
PRODUCT-0120's authored padding retain their separate owners. Endpoint label
clipping remains PRODUCT-0119's deliberately visible outcome until an author
uses `padding().right(...)`.

### 7. Unbanded layouts do not draw Cartesian axis titles

When both requirements are empty, `draw_axis_labels` returns before resolving
or emitting Cartesian x/y title operations. This applies to radial and
parallel layouts alike. It does not suppress text owned by those marks --
radar dimension names, pie labels, funnel stage labels, and parallel headers
remain the kernels' own placed text.

This is a rendering change, not compiler-test support. It restores the shared
guide invariant for valid Rune: an authored or kernel-provided Cartesian label
on an unbanded layout renders without aborting, but emits no generic title
because no authority reserved a placement. It does not suppress a visual
symptom or invent a fixed-offset fallback.

## Implementation sequence

**Stage 0 — census and bank.** Record top, bottom, left, and right occupants,
their coordinate spaces, their reservations, and their clips. Bank a short
numeric y-axis capture and the failing Multivariate heatmap capture, including
the current overlap. Also bank the unbanded-layout operation trace with visible
resolved x/y labels, and separately identify the kernel-owned text that must
remain.

**Stage 1 — left guide layout.** Route the current left reservation through the
typed guide value, without changing the short-label capture.

**Stage 2 — sequenced title placement.** Replace the fixed y-title offset with
the guide's `title_x`, then bank the long-label geometry delta and grid case.

## Acceptance gates

1. **Stage 0 names all four edges.** The survey lists every reservation,
   placement, and clip authority at top, bottom, left, and right. It is a
   blocking input, not a documentation afterthought.
2. **Long labels clear the title.** In the Multivariate heatmap, the estimated
   `Reliability` tick run and `capability` title are disjoint by at least the
   banked title gap. This fails the current rendering.
3. **The left guide is a sequence, not a max.** With a tick run wider than the
   legacy offset, `title_x = tick_end + title_gap` and `left = title_x +
   y_label_size`; a maximum of old arms cannot pass.
4. **Both consumers read one guide.** The reservation and the title draw
   position derive from the same `YAxisGuideLayout` value. A fixed
   `y_label_padding` in either production consumer fails structurally.
5. **The hidden-title zero case spends no gap.** With y-label presence hidden,
   `title_x` is absent and `left == tick_end`. An unconditional title gap
   fails.
6. **Short numeric labels retain their placement.** The banked short-label
   capture and operation trace remain byte-identical. This proves the new
   sequence reduces to the accepted legacy geometry where it should.
7. **Long-label movement is exact.** The plot area's left edge and the y-title
   move right/left respectively by exactly `new_left - old_left`; marks move
   only through that changed plot rectangle. No byte-identical claim hides the
   intended geometry move.
8. **Ticks remain tick-relative.** Long and short y-tick text operations keep
   the same transform-derived x origin relative to their ticks. The estimate
   may reserve space; it may not move a tick label's ink.
9. **Grid cells remain congruent.** A faceted fixture with unequal y-label
   lengths takes one maximum left guide before tiling. Cell widths and shared
   global title placement remain equal; no cell gets a second local inset.
10. **The bottom guide is unchanged.** PRODUCT-0115's x-tick/title sequence
    has identical operations and bottom extent before and after this record.
11. **Top and right are unchanged.** The Stage 0 top capture remains identical;
    right padding and legend content bounds retain their existing deltas. This
    record does not repurpose either owner.
12. **Padding still composes as a floor.** `padding().left(large)` retains the
    authored request and enlarges the effective left band only when it exceeds
    the guide. It never becomes a second title-placement authority.
13. **No text measurement capability.** `PlotBackend` gains no glyph extent
    API. The existing face-bound conservative estimator remains the only width
    input, preserving PRODUCT-0093 §7.
14. **Visible unbanded labels render without a Cartesian title.** A radial and
    a parallel figure each author visible x/y labels and render without
    aborting; neither emits a shared Cartesian axis-title operation, while
    each retains its kernel-owned labels. A fixed-offset fallback, a
    missing-guide panic, or suppressing the mark's own text fails.

## Stop conditions

> If Stage 0 finds an unenumerated automatic right-edge occupant, stop. This
> record closes the left sequence; it must not guess a new right-band policy.

> If a safe title gap cannot be banked from operation geometry without realized
> glyph extents, stop. That is PRODUCT-0093 §7's backend-measurement decision,
> not a constant to invent here.

> If a grid needs a per-cell left inset to avoid collision, stop. The proposed
> fix has violated PRODUCT-0048's congruence rule and needs a different layout
> authority.

> If the implementation changes tick-relative label origins, stop. The estimate
> has escaped reservation and violated PRODUCT-0116.

> If an unbanded layout requires an axis-title-like text role, stop. It needs a
> mark-owned placement and reservation decision; this record must not revive a
> generic Cartesian fallback.

## Supersession

Supersedes PRODUCT-0114 §3 **only for the left edge**. Its former maximum was
necessary for the then-known tick reservation but insufficient once a visible
y-axis title shares that edge. It also supersedes the pre-0126 implicit
assumption that a resolved Cartesian label may draw without a band: an empty
requirement now means no shared Cartesian axis title. PRODUCT-0115's bottom
sequence is inherited, not changed. PRODUCT-0048's shared grid reservation,
PRODUCT-0116's estimate invariant, PRODUCT-0119's clipping policy, and
PRODUCT-0120's authored padding floor remain in force.

## Rejected

**Add more right padding.** It reduces plot width on the opposite edge and
leaves the left collision untouched.

**Keep the maximum and increase `y_label_padding`.** It merely moves the
collision threshold. Long labels still enter the title's fixed region, and the
reservation and placement remain separate authorities.

**Move tick labels by their estimated width.** This makes estimates place ink,
the defect PRODUCT-0116 prohibits. Ticks must remain attached to their axis;
the distinct title moves around the reserved geometry instead.

**Add a per-cell facet inset.** It makes one cell narrower than another and
undoes PRODUCT-0048's grid congruence.

**Give radial and parallel layouts a generic fixed axis-title offset.** The
offset would place ink in space no layout authority reserved. A mark that needs
such text must own a placement for its own geometry.

## Consequences

The showcase remains an exhibit, not a workaround. Its long categorical label
will move the heatmap's plot area right only after this record's implementation,
and the title will sit outside the tick run. Short numeric plots stay where
they are. Radial and parallel figures no longer emit a generic Cartesian title
merely because a field name resolves; their own placed text remains. The
radial/parallel guard repairs a crash found by authoring an ordinary
Multivariate figure: it makes visible unbanded labels render safely rather than
reach a missing Cartesian guide. This is the seventh defect the arc exposed by
exhibiting a figure rather than by an existing gate, and the first crash. The
durable change is broader: all four edges have now been surveyed as a class,
rather than repaired one screenshot at a time.
