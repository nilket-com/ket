# ADR 0132: rotated tick-text rasterization

Status: proposed 2026-08-18. `x_axis().tick_rotation(n)` has never drawn a
label. The operations are emitted, the band is reserved for them, and the
production renderer rasterizes nothing. PRODUCT-0115 shipped the surface and its
acceptance probe has reported success at every angle since.

This record is scoped to **rasterization of rotated tick text**. It does not
revisit rotation policy, band derivation, tick selection, or the allocation
bound; each of those is settled and each is confirmed working by the same
measurements that expose this.

## Context

### The labels are emitted and never appear

Two independent observations, taken 2026-08-18 on the PRODUCT-0057 designated
adapter.

**The operation stream contains them.** A Cartesian figure over a
`Datetime(Milliseconds, UTC)` axis at 24 degrees emits six text operations --
three y labels and all three timestamps:

```text
ROTOPS deg=24 text_ops=6 ::
  ["0", "50", "100",
   "2024-02-29 00:00:00", "2024-03-05 03:33:20", "2024-03-10 07:06:40"]
```

**The raster does not.** The same figure, rendered at three sizes with a healthy
plot and an adequately reserved band:

| figure | rotation | plot height | band height | band ink |
|---|---:|---:|---:|---:|
| 820x440 | 0 | 350.0 | 20.0 | **593** |
| 820x440 | 24 | 292.9 | 77.1 | **31** |
| 820x440 | 36 | 249.8 | 120.2 | **27** |

The residual 27--31 is the x-axis title, which is unrotated. No tick label ink
survives at any nonzero angle.

### It is not the collapse, and not the showcase's settings

PRODUCT-0131 repaired a real defect -- a band outgrowing its figure and clamping
the plot to zero -- and that repair holds: the plot heights above are healthy and
the bands are fully allocated. Tuning the exhibit's angle and tick size changed
nothing because neither was ever the cause; those two commits are reverted
(`2e39f49c`, `83a1e63e`).

### PRODUCT-0115's own fixture never rendered either

The clearest evidence is the acceptance probe's own captures. Its categorical
bar fixture at 0 degrees draws its four category labels; at +45 degrees the band
below the bars is empty. The feature has been non-functional since it shipped.

### Why the gate passed

`product_0115_rotation` asserts exactly two things:

- `assert_ne!(zero.rgba, positive.rgba)` -- "rotation must reach the production
  renderer";
- `exterior_luminous_pixels(capture, bounds) == 0` -- "the figure clip must
  contain the axis overlay".

Deleting the labels satisfies both. The raster genuinely differs from the
unrotated one, because the labels are gone; and absent ink escapes no clip. The
probe cannot distinguish *contained* from *absent*, and it reports
`contained/contained/contained` while drawing none.

This is the arc's own zero-result rule, unapplied: an absence-of-ink oracle must
be proven nonzero against the unchanged implementation. PRODUCT-0077 added
exactly that assertion for its containment gate; PRODUCT-0115's, written later,
did not.

### The sharpest lead for Stage 0

Rotated text *can* rasterize in this renderer. The y-axis title is drawn through
the same `with_save` / `translate` / `rotate` / `fill_text` sequence at -90
degrees and appears in every production screenshot.

So the failure is not "iced cannot rotate text". Stage 0's job is to find what
differs between the two call sites, and the source shows three differences.

The x tick path is **nested two transforms deep, inside a clip**
(`common.rs:2288-2358`):

```text
with_clip(bounds)
  with_save -> translate(plot_area.x, plot_area.y)
    [per label] with_save -> translate(tick) -> rotate -> fill_text
```

The y title path is a **single unnested transform with no clip**
(`common.rs:2388`, drawn after the clip closure has returned, at `:2452-2470`):

```text
with_save -> translate(plot_area.x - title_x, ...) -> rotate -> fill_text
```

So the candidates are transform composition (a rotate nested inside an outer
save-and-translate), the clip context, and the alignment arguments -- the title
sets `align_x` and `align_y` explicitly while the tick path sets only `align_x`
from `XTickTextPlacement`. The nesting is the most suspicious because it is the
one difference that changes the *matrix* rather than the region or the anchor,
but Stage 0 must establish which, and not assume. A fourth cause is possible.

## Decision

### 1. The acceptance oracle is positive in-band ink

A rotated tick label is proven by ink **inside** the reserved band, counted and
asserted greater than a banked threshold. Containment -- the absence of ink
outside the figure -- is retained but demoted: it may never again stand alone as
the evidence that rotation works.

Every angle under test carries its own positive count. A single "some ink
somewhere" assertion would pass on the axis title alone, which is what the
current 27--31 residual would supply.

### 2. Rotated labels rasterize inside the band their rotation reserved

The band already grows with the angle -- PRODUCT-0115's derivation is correct and
PRODUCT-0131 allocates it. The defect is that nothing lands in it. After this
record, the ink a rotated angle reserves is the ink it draws.

### 3. Scope is rasterization only

This record changes no band arithmetic, no tick selection, no rotation policy,
and no authored surface. If the repair appears to require any of those, the
scope was wrong and the record stops.

### 4. PRODUCT-0115's gate is strengthened, not replaced

Its containment assertion stays. Its `assert_ne!` on rasters stays. Both gain a
positive in-band count per angle, so the pair can no longer be satisfied by
deletion. The record does not delete a gate that was pointed at a real property;
it supplies the control the property always needed.

## Implementation sequence

**Stage 0 -- locate the loss.** Determine where rotated tick ink goes, by
differential comparison against the y-axis title, which rotates and renders.
Bank the answer before changing anything. The two named candidates are the clip
context and the alignment arguments; a third is possible and Stage 0 owns
finding it.

**Stage 1 -- the positive oracle.** Add per-angle in-band ink counts to the
PRODUCT-0115 probe and bank today's failing values. This stage must go **red**
against the current implementation; a Stage 1 that passes has reproduced the
vacuity it exists to remove.

**Stage 2 -- the repair.** Make rotated tick text rasterize, and restate the
banked counts.

## Acceptance gates

1. **Positive in-band ink per angle.** At 0, +45, and -45 degrees, tick-label
   ink inside the reserved band exceeds a banked threshold, asserted separately
   for each angle. The threshold excludes the axis title, whose 27--31 px
   residual is what makes a naive nonzero count pass today.
2. **The oracle fails before the fix.** The Stage 1 assertions are demonstrated
   red against the unrepaired renderer, and that demonstration is recorded. This
   is the gate PRODUCT-0115 lacked, and adding it without proving it
   discriminates would repeat the defect exactly.
3. **Containment survives, subordinated.** No ink escapes the figure clip at any
   angle -- unchanged from PRODUCT-0115 -- but the assertion is now paired with
   gate 1 so absence cannot satisfy it.
4. **The unrotated control is byte-identical.** The 0-degree capture is
   unchanged, proving the repair touched rotation and not text drawing at large.
5. **The operation stream is unchanged.** The same six text operations are
   emitted before and after. Emission was never the defect, and a repair that
   changes what is emitted has misdiagnosed it.
6. **Band geometry is unchanged.** PRODUCT-0115's derivation and PRODUCT-0131's
   allocation produce identical numbers before and after. The plot heights and
   band heights in this record's table are the banked control.
7. **Ticks stay tick-relative.** Each label keeps its origin relative to its own
   tick. PRODUCT-0116 holds; this record moves ink from nowhere to somewhere, not
   from one place to another.
8. **No text measurement capability.** `PlotBackend` gains no glyph-extent API.
   PRODUCT-0093 §7 holds.
9. **The temporal exhibit renders its labels.** With rotation restored to
   `uat/temporal_tick_intervals.rn`, the Count-control pane draws visible rotated
   timestamps at a normal pane size -- the observation that started this record.

## Stop conditions

> If the repair requires changing band geometry, tick selection, or the
> allocation bound, stop. Those are settled by PRODUCT-0115, PRODUCT-0127, and
> PRODUCT-0131, and all three are confirmed correct by this record's own
> measurements.

> If rotated text cannot be rasterized without measuring glyph extents, stop.
> That is PRODUCT-0093 §7's deferred decision.

> If the loss turns out to be a defect in the graphics dependency rather than in
> Polariton's use of it, stop and record the dependency decision. A version bump
> or an upstream workaround is not a layout choice and must not be made silently
> inside a rendering repair.

> If the positive oracle cannot be written without a glyph-level notion of which
> ink belongs to which label, stop. Counting ink in a region is the instrument
> this arc uses; inventing per-label attribution is a new capability.

## Supersession

Supersedes nothing. PRODUCT-0115's decisions stand: rotation is authored, the
band tracks the angle, and the figure clip contains the overlay. Only its
acceptance evidence was insufficient, and this record supplies what was missing
rather than reopening what was decided.

## Rejected

**Retire `tick_rotation`.** It would remove a non-functional feature and its
misleading gate in one step. But the surface, the cascade, the band derivation,
and the refusals are all correct and tested; only the final rasterization is
missing, and deleting a nearly-complete feature to avoid finishing it is a poor
trade.

**Treat it as a showcase problem.** Two commits already tried tuning the angle
and tick size. The measurements above show the plot healthy and the band
allocated at every size, so no authored value can fix it. Those commits are
reverted.

**Pre-render rotated labels to images.** It would sidestep whatever the
transform problem is. It also introduces a raster path for text, which needs
glyph metrics to size, which is PRODUCT-0093 §7's deferred measurement arriving
through the back door.

**Assert only that the raster changed with rotation.** This is what
PRODUCT-0115 does, and it is precisely what deletion satisfies.

## Consequences

A shipped, spelled, cascaded, band-derived feature turns out never to have drawn
a pixel, and the gate that was supposed to prove it reported success at three
angles for the entire time. The repair is likely small; the lesson is not.

The durable finding is about the shape of the evidence. PRODUCT-0115's probe
asked two questions that a working implementation answers yes to -- *did the
image change?* and *did anything escape?* -- and never asked the one question
only a working implementation answers: *is the thing there?* Absence answers the
first two as convincingly as success does. Every future gate over drawn output
owes a positive count of the ink it claims to be verifying.
