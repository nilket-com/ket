# ADR 0118: minimal x-tick endpoint correction

Status: proposed 2026-08-16. PRODUCT-0115 made rotated x-tick labels stay
inside a figure by pulling the outward endpoint inward. Its helper knows only
that a tick is the minimum or maximum endpoint; it assumes that tick is the
containment edge and retracts the **entire** projected run to it. That is true
for a Cartesian endpoint and false for a categorical endpoint, whose tick sits
inside the figure. The final label therefore moves by its whole estimated width
while every interior label stays on its tick. This record supplies the typed
figure boundary that the helper is missing and corrects by the minimum required
overflow only.

It follows PRODUCT-0117. It changes no Rune spelling, font budget, rotation
angle, guide-band reservation, or facet-divider policy.

## Context

### Containment is satisfied, but minimality is not

PRODUCT-0116's `XTickTextPlacement` currently derives an endpoint correction
from the projected run alone:

```text
minimum endpoint: correction = -run_min_x
maximum endpoint: correction = -run_max_x
```

That is sufficient to keep a run inside its tick coordinate. It also passes
PRODUCT-0116 gate 7, which asks only whether the corrected extent is inside the
figure. It cannot distinguish a necessary correction from one that retracts a
label far more than the figure requires.

For `North American enterprise renewal commitments` at the maximum categorical
tick, the result is visible: the label is the only run shifted away from its
tick, and its shift follows its entire projected width rather than the space it
actually lacks. On a resize every ordinary label moves with its tick; this one
is governed by a different distance and reads as though it is doing its own
thing.

That endpoint is established from the live Toron relation, not inferred from
the SQL seed file's insertion sequence. Bar preserves the relation's category
order, and the probe currently arrives as `Ship`, `International…`,
`Regulated…`, `Self-service…`, `Build`, `Learn`, `Plan`, `North American…`.
The two-width fixture mirrors that delivered order; a reordered test would
silently turn the observed endpoint back into an interior control.

The missing property is **minimality**:

```text
overflow   = run_outer_extent - containment_boundary
correction = max(0, overflow)
```

A fitting run receives no correction. An overflowing run receives exactly its
overflow, leaving its outer extent on the containment boundary rather than at
its tick.

### The current carrier loses the boundary before the helper runs

`draw_cartesian_axes`, `draw_categorical_axes`, and
`draw_categorical_xy_axes` run after their overlay path translates into the
plot rectangle. The helper receives a bare `XTickEndpoint::{Min, Max}` plus an
estimated width; it receives neither the figure rectangle nor a bound expressed
in the same coordinate space as the transformed tick run.

The single-plot overlay clips x-axis text to its widget `bounds` at
`common.rs:1767`. The grid overlay clips to the whole figure `bounds` at
`grid.rs:804`. Those are the actual containment boundaries. Passing a raw
`f32` margin from either caller would recreate the defect class PRODUCT-0049
eliminated: the figure bound is widget-space while the helper computes in
plot-local coordinates after the translation. A numerically plausible wrong
space renders plausibly and cannot be reviewed from its type.

### A facet cell edge is deliberately not the boundary

PRODUCT-0048 established the grid's figure-wide x-axis text clipping: labels
may cross an internal facet divider and stop only at the figure edge. This is
observable in `test37`: the cells share a categorical domain and labels may
occupy the neighbouring cell's guide space.

An intuitive repair would use each cell's edge as the categorical endpoint
boundary. That would over-correct every interior-cell endpoint, recreating the
same retract-more-than-necessary failure one level down. Whether labels should
cross facet dividers is a separate policy question. This record inherits
PRODUCT-0048's answer and does not reopen it.

## Decision

### 1. Carry a typed figure containment rectangle to the overlay seam

Each x-axis overlay receives a `Rect<Widget>` representing its actual figure
clip: the whole widget for a single plot and the whole composed figure for a
grid. It is not a cell rectangle, a plot rectangle, or a raw width.

At the one overlay seam which already translates from widget space into its
plot-local frame, this rectangle is converted into one typed local containment
value. The conversion is explicit and one-way. `XTickTextPlacement` receives
only that local value, so it cannot accidentally subtract a widget-space x
from a plot-space tick coordinate.

This uses PRODUCT-0049's typed geometry vocabulary for the exact boundary it
was created to protect. No unchecked `f32` boundary or margin crosses the
seam.

### 2. Correction is exact overflow beyond the figure bound

For a rotated run at local tick `x`, let `run_min_x` and `run_max_x` be its
estimated transformed extents relative to that tick, and let the typed local
figure bounds be `[figure_min_x, figure_max_x]`.

```text
minimum overflow = figure_min_x - (x + run_min_x)
maximum overflow = (x + run_max_x) - figure_max_x

minimum correction = max(0, minimum overflow)
maximum correction = -max(0, maximum overflow)
```

The sign is the only difference between sides. The policy is one rule: move a
run only by the amount by which its conservative outer extent exceeds the
figure boundary. At zero rotation, the existing legacy alignments and zero
translation remain unchanged.

Cartesian endpoint ticks naturally have no plot-edge margin. Categorical and
categorical-XY endpoint ticks retain the usable space between their band centre
and the **figure** bound, including any plot padding or figure-wide guide
space. They are not special cases; their geometry simply supplies a different
local tick coordinate to the same formula.

### 3. The 0116 estimate invariant stays intact

PRODUCT-0116 remains unchanged: an estimate may reserve space and may pull a
run inward, but may never determine vertical placement or distance from the
tick baseline. This record retains its figure-space x-only correction and its
single `XTickTextPlacement` authority.

The estimate still makes containment conservative. An over-estimate may pull
an overflowing endpoint a little farther inward than its realized ink needs,
but the operation-list correction remains exactly the overflow of the declared
conservative run. It may not retract a fitting conservative run merely because
it is an endpoint.

### 4. One carrier serves all three x-axis paths and both origins

The carrier reaches:

| origin | layouts |
|---|---|
| single-plot overlay | Cartesian, categorical-X, categorical-XY |
| grid overlay | Cartesian, categorical-X, categorical-XY |

There is one `XTickTextPlacement` resolution path. The three layout-specific
draw loops supply their local tick coordinates; neither chooses a separate
endpoint policy. This prevents a categorical repair from leaving
categorical-XY with the same full-retraction defect.

## Implementation sequence

**Stage 0 — bank the failing endpoint.** Capture `test37` at 45° in its
current state and record the final categorical label's tick origin, projected
conservative extent, and full-retraction offset. Add a focused fixture where a
categorical endpoint fits inside the figure, proving that current code moves a
run which needs no correction.

**Stage 1 — typed boundary transport.** Thread `Rect<Widget>` through each
overlay caller and establish the sole widget-to-plot-local conversion at its
translation seam. No correction formula changes in this stage; its operation
traces are parity-preserving.

**Stage 2 — exact-overflow correction.** Replace full retraction with §2's
formula in `XTickTextPlacement`, then exercise all three layouts and both
origins. Re-capture `test37`; the endpoint must remain contained while joining
the ordinary labels' tick-relative behaviour.

## Acceptance gates

1. **The boundary is typed.** Structural: the overlay carrier is a
   `Rect<Widget>` (or a derived typed plot-local bound), never a raw `f32`.
   The only widget-to-plot conversion is adjacent to the existing translation.
   A calculation which compiles only by stripping both markers fails this gate.

2. **The boundary is the whole figure.** A two-cell fixture records an
   interior cell's edge and the figure edge separately. The endpoint's local
   containment bound resolves to the latter. This fails a helpful cell-local
   repair while preserving PRODUCT-0048's divider-crossing policy.

3. **A fitting categorical endpoint stays on its tick.** With a projected run
   wholly within the figure bound, the correction is exactly zero. The current
   full-retraction helper fails this gate while a no-correction implementation
   passes only this half.

4. **An overflowing categorical endpoint moves exactly its overflow at two
   widths.** A shared eight-category grid renders the same outer label in a
   wide figure where its conservative run fits and a narrow figure where it
   overflows. Its tick-relative offset is zero in the wide case and exactly
   the smaller overflow in the narrow case — never one label-width at both
   widths. The compact cell whose identical domain has another pane of
   figure-wide space remains the zero-offset control. Its corrected
   conservative outer extent equals the figure boundary, not the tick and not
   a point short of the boundary. This rejects both the current full
   retraction and an under-correcting nudge.

5. **Cartesian is the zero-margin instance.** A Cartesian outer tick at the
   figure edge uses the same formula and retains PRODUCT-0116's existing
   containment behavior. A categorical-only branch fails this gate.

6. **Categorical-XY shares the authority.** The same fitting and overflowing
   pair passes through `draw_categorical_xy_axes`; no second endpoint helper
   or copied arithmetic exists.

7. **Both single and grid overlays supply their real clips.** A single plot
   and a faceted grid each expose their widget figure bound to the same
   placement authority. This catches a repair which works only in the UAT grid
   while a single plot still retracts to its tick.

8. **0116's vertical invariant survives.** At both rotation signs, all
   corrected endpoint translations have `Δy = 0`, interior origins remain on
   their ticks, and no estimated quantity becomes a vertical placement term.

9. **Zero rotation is unchanged.** At `0°`, operation traces remain identical
   to PRODUCT-0115's banked legacy placement. The containment carrier must not
   become an unconditional nudge.

10. **Production corroboration.** `test37` at 45° shows the final `North
    American enterprise renewal commitments` label contained at the outer
    figure edge without the present full-width detachment. Its visible
    improvement corroborates the structural minimality gates; pixels do not
    substitute for their arithmetic claim.

## Stop conditions

> If the figure clip cannot be expressed as `Rect<Widget>` through the overlay
> seam without raw relabelling or a second coordinate authority, stop. A loose
> float margin is not an acceptable fallback for a coordinate-space contract.

> If any existing origin clips axis text to a boundary other than the figure
> rectangle, stop and enumerate the policy before using its nearest cell or
> plot edge. The formula is only honest when its boundary is the renderer's
> real boundary.

> If exact conservative overflow still leaves the representative endpoint
> visually detached from its tick, stop. That would be an estimator/legibility
> question, not evidence for moving it farther than the boundary requires.

> If a projected run is wider than the whole figure, do not add a second
> clamp. Exact-overflow correction must leave its outward end on the applicable
> figure boundary; its opposite end then necessarily crosses the other bound.
> PRODUCT-0115's uniform figure-edge clip is the deliberate backstop for this
> irreducible case, not evidence that the correction formula is wrong.

## Supersession and inheritance

PRODUCT-0116 gate 7 required the corrected conservative run to remain inside
the figure bound. That containment-only assertion was necessary but
insufficient: the present full-retraction defect satisfies it by landing well
inside the bound. Gates 3 and 4 of this record replace that one-direction
contract with minimality in both directions — no correction when the run fits,
and exactly its overflow when it does not. PRODUCT-0116's intended behavior,
figure containment through an x-only correction, is preserved; only the
correction's magnitude changes.

Three authorities are inherited unchanged:

- PRODUCT-0048's figure-wide grid clip remains the containment boundary;
  individual facet-cell edges are not substituted for it.
- PRODUCT-0116's estimate invariant remains: an estimate may reserve space and
  pull inward, never set vertical placement or tick-relative distance.
- PRODUCT-0117's pinned-face, per-class ASCII estimator remains the declared
  conservative run used by the formula. This record neither changes its budget
  nor introduces measurement.

## Rejected

**Retract every endpoint to its tick.** Rejected. It satisfies containment but
not minimality, producing the visibly detached `North American…` endpoint.

**Use a half-band margin.** Rejected. The half-band is the distance to the plot
edge, not necessarily to the actual figure clip. It would still detach a run
from its tick whenever figure padding remains available.

**Use each facet cell's boundary.** Rejected. PRODUCT-0048 permits guide text
to cross internal facet dividers. Cell containment would impose a new policy,
over-correct interior endpoints, and disguise that policy as arithmetic.

**Pass an untyped x margin.** Rejected. It hides widget/plot-local mismatch at
the very seam PRODUCT-0049 made explicit.

**Fix categorical-X only.** Rejected. Categorical-XY has the same band-centre
geometry and would retain a duplicate defect.

## Consequences

An endpoint label moves only when it would otherwise cross the same figure
edge which clips it. Most categorical endpoints therefore behave like interior
labels, staying on their ticks; long endpoints shift only by their actual
conservative overflow. This resolves the visible special-case behavior without
changing font budgets, rotation, tick density, or the established policy that
axis text may cross facet dividers.

The stricter gate is the durable result: containment alone is insufficient
because it accepts arbitrary over-correction. Any future endpoint policy must
prove both directions — no movement when fitting and exactly enough movement
when overflowing.
