# ADR 0115: the x-tick rotation surface

Status: proposed 2026-08-16. PRODUCT-0114 made the bottom band correct for
rotated x-tick labels without making rotation authorable. `test37.rn` then
showed the remaining problem: long categorical labels have enough vertical
room, but overlap each other horizontally. Rotation is the author-controlled
legibility response. It must not, however, turn the first or last label into
an off-figure crop.

This record adds `x_tick_rotation(degrees)`, makes its angle a typed,
cascaded value, anchors only the endpoint that a nonzero transformed label run
carries toward the figure edge, and gives single plots the same explicit
figure-edge clip that facets already use. Anchoring is the visible policy;
clipping is the uniform backstop.

## Context

### The existing field describes the wrong text

`ResolvedPlotSettings` calls the renderer field `x_label_rotation`, but
`x_label` is the authored axis title (`common.rs:1522-1533`). The field is
used only by the three x-**tick** draw sites: Cartesian, categorical-x, and
categorical-xy (`common.rs:1906`, `:2081`, `:2297`). A public spelling named
`x_label_rotation(45)` would promise to tilt the axis title and do something
else.

The neighbouring Rune vocabulary is already accurate: `x_tick_step`,
`x_minor_ticks`, and the blocked `x_tick_size` configure generated x ticks.
The new spelling therefore belongs in that family:

```rune
x_tick_rotation(45)
```

Degrees are intentional authoring units. The renderer alone converts them to
radians.

### Rotation exists in paint and nowhere else

The resolved field defaults to `0.0` and each draw site calls
`rotate(x_label_rotation.to_radians())`. It is absent from `PlotTypography`,
`PlotSettingsLayer`, the five-tier cascade, Rune presentation, and authored
settings persistence. This is not an unexposed setting waiting for a word; it
is a renderer-local literal with no authoring path.

PRODUCT-0112 established the required shape for a bounded user value:
private representation, one validating constructor, typed carriers, and a
partial `Override` which wins over an atomic typography carrier within one
tier. Leaving this field as a raw `f32` would reopen the two-authorities gap:
Rune could validate degrees while a theme, pane, or persisted setting could
inject an invalid raw angle directly.

### The current endpoint policy is incomplete, not wrong

`AxisTextAnchoring::x_alignment` already returns `Left` or `Right` for a
bound-coincident value on an **occupied shared facet edge**. PRODUCT-0077
uses it to keep two adjacent facet cells from each drawing a centred label
through their common boundary. At outer figure edges and in ordinary single
plots it deliberately returns `Center`; facets then clip the complete overlay
to the figure (`grid.rs:780-783`).

This is useful precedent, not the complete answer. A nonzero rotation changes
which horizontal endpoint overhangs. Applying inward anchoring to both figure
endpoints would move one label that does not overhang, while clipping it would
make a legible label look accidentally cut off.

The sign must be derived from the renderer, not inferred from a screenshot.
`SvgBackend::rotate` delegates to `kurbo::Affine::rotate`; in the y-down canvas
a positive angle turns a positive-x text run clockwise. Whether a particular
label extends down-left or down-right also depends on its alignment. This
record therefore speaks only of the **transformed run's leading and trailing
edges**, never of an unqualified visual direction for a signed angle.

### Single plots lack the explicit clip that facets have

The facet overlay is drawn inside `with_clip(figure_bounds)`. The corresponding
single-plot overlay draws with save/translate but no explicit figure clip.
Whether an enclosing widget happens to crop it is not a plot contract. The
two paths can therefore disagree for the one case clipping must backstop: a
label wider than the figure itself.

An overlay-only crop in Polariton would be worse than either current path. It
could crop text while its position still claims it fits. The visible output and
the layout decision would disagree. The plot authority must first anchor what
can be anchored, then clip every overlay by the same figure boundary.

### PRODUCT-0114 has already reserved the vertical extent

For an estimated label width `w`, tick size `h`, and angle `θ`, the bottom
band includes:

```text
w · |sin θ| + h · |cos θ|
```

That is the complete rotated vertical extent. Endpoint correction may change a
label's alignment and translation, but it must consume this same derived
extent; it does not get a second, endpoint-only bottom reservation. Repeating
the formula in each draw loop would be a second geometry authority.

## Decision

### 1. Expose `x_tick_rotation(degrees)`, not `x_label_rotation`

`x_tick_rotation(number)` is a generated-text word in a figure. It accepts one
finite number of degrees in the inclusive interval `[-90, 90]`:

```rune
figure([
    x_tick_rotation(45),
    bar("product", "value"),
])
```

`0` is the default and preserves every existing label placement exactly.
`-90` and `90` are accepted: fully vertical tick labels are a normal response
to very long categories, and PRODUCT-0114's formula deliberately covers their
full estimated width.

The word is valid for every figure, regardless of its eventual x-axis type.
There is no reason a categorical axis owns rotation while a temporal or linear
axis does not; all three x-axis draw paths render the same generated-text role.

### 2. `TickRotation` is a typed, partial setting through every carrier

Introduce `TickRotation`, a private finite bounded degree value. It owns:

- `MIN_DEGREES = -90`, `MAX_DEGREES = 90`, and `ZERO`;
- validation and a diagnostic which says x-tick rotation is in degrees;
- the sole degree-to-radian conversion used by paint.

Rename the resolved field to `x_tick_rotation: TickRotation`; the old internal
name is not preserved merely because it predates an authoring surface.
`PlotTypography` carries `TickRotation`, `PlotSettingsLayer` has an
independent `Override<TickRotation>`, and `ResolvedPlotSettings` carries the
typed value. The partial slot follows PRODUCT-0091 and PRODUCT-0112: atomic
typography can supply a complete value, but the specific slot wins within one
tier. The five tiers retain their existing order.

No raw degree reaches a renderer. The only conversion to `f32` radians occurs
inside `TickRotation::radians()` at the existing backend `rotate` seam.

### 3. At zero, retain today’s anchoring exactly

At `TickRotation::ZERO`, Cartesian labels use the existing
`AxisTextAnchoring::x_alignment`; categorical labels remain centred. Shared
facet-edge anchoring remains exactly PRODUCT-0077's policy. There is no new
translation and no new endpoint exception.

This is both compatibility and a discriminating zero case: an implementation
which applies an endpoint correction unconditionally may look fine at `45°`
but silently moves every existing figure.

### 4. At nonzero angles, anchor only the endpoint whose transformed run overhangs

For a nonzero `θ`, one helper owns every x-tick run's placement. It consumes the typed
rotation, the existing axis anchoring, the label's producer-keyed estimate from
PRODUCT-0114, the endpoint’s position, and the figure boundary. It returns the
horizontal alignment and the compensating translation together. Every rotated
run receives the translation required to put its transformed upper edge at,
never above, the tick baseline; a centred interior run therefore receives half
its projected-height translation even though its horizontal pivot stays centred.

The helper determines which endpoint the transformed text run carries toward a
figure edge from the sign of `θ` and the run's alignment. It then anchors only
that endpoint inward (`Left` or `Right` as appropriate). The opposite endpoint
remains centred, because it is already carried away from its boundary.

Changing the horizontal alignment rotates the label about a different end of
its run. The same helper supplies the compensating vertical translation that
keeps the whole run below the axis, using the estimate already owned by
PRODUCT-0114. Thus there is one decision about both where the run begins and
where it lands; a renderer loop cannot independently nudge it down.

At `+45°` exactly one endpoint changes; at `-45°` the other changes. The record
does not state which textual side that is in prose, because the answer follows
the renderer's transformed run rather than a guessed screen-coordinate
convention.

### 5. Sequence the x-axis title after the rotated tick run

PRODUCT-0114 §3 reserved the bottom band as the maximum of the independently
anchored tick and title extents. That was correct at `0°`: ticks ended at
`x_label_offset + x_tick_size = 28`, while the title began at
`x_label_padding = 45`, so the title's extent dominated the same band without
overlap. Rotation falsifies the independence: a `45°` tick run can extend past
the fixed title top, leaving the band tall enough while putting its occupants
on top of each other.

The shared bottom-guide authority therefore derives both reservation and title
placement in order:

```text
tick_end  = x_label_offset + rotated_tick_height
title_top = max(x_label_padding, tick_end + title_gap)  // if a title exists
bottom    = title_top + x_label_size                     // if a title exists
bottom    = tick_end                                     // otherwise
```

`title_gap` preserves the existing zero-degree clearance, so this sequence
reduces exactly to PRODUCT-0114's maximum at `0°`. It is not a second rotation
allowance: the title's drawn origin and the plot area's bottom inset consume
the same `XAxisGuideLayout`. An absent axis title spends neither its clearance
nor its height.

This refines, rather than invalidates, the earlier heuristic. A band is a
maximum when its occupants are alternatives or independently anchored and one
dominates; it is a sequence when one occupant must clear the other. PRODUCT-
0106's heading remains the former. Rotated x ticks and their title are the
latter.

### 6. Clip every axis overlay to its figure as a backstop

Both single and faceted overlays run inside an explicit clip equal to the
complete figure bounds. This is not the primary containment mechanism: ordinary
endpoint labels must be visibly inward-anchored, not cleanly sawn off at the
edge. The clip exists for the irreducible case — a label whose own estimated or
realized extent exceeds the entire figure.

Facet cell clips remain cell-local for marks that require them. Axis text keeps
the existing figure-wide overlay clip so labels may cross an internal facet
boundary; only the outer figure edge constrains it.

### 7. Do not add y-tick rotation

Y ticks remain horizontal and consume a width-bound left band. Rotating them
would change both their reservation and their collision direction, and it has
no x-axis counterpart that makes the policy automatic. This record authorizes
x ticks because `test37` demonstrates their crowding and PRODUCT-0114 already
reserved their rotated extent. The asymmetry is a decision, not an omission.

### 8. Rotation does not decide legibility policy

Rotation makes a chosen label orientation available. It neither thins ticks,
changes their format, truncates category values, nor promises that arbitrarily
many labels become readable. A visibly clipped label after inward anchoring is
evidence for a future legibility decision, not permission to add an implicit
maximum angle, abbreviation, or tick-elision policy here.

## Implementation sequence

**Stage 0 — bank the rotated exhibit.** Extend `test37.rn` at `+45°` and
`-45°` by setting the renderer-local value only for the probe. Bank operation
traces and production pixels for a single plot and an asymmetric facet grid.
The capture must identify the endpoint alignment and the figure clip, not only
show fewer overlaps.

**Stage 1 — typed settings and Rune surface.** §§1–2: `TickRotation`, complete
and partial carriers, five-tier resolution, persistence, `x_tick_rotation`,
its exact source span, bounds, duplicate refusal, and README vocabulary.
No paint behavior changes yet; resolved zero is byte-identical.

**Stage 2 — one placement and guide authority.** §§3–5: route all three
x-axis draw sites through the endpoint helper and route the axis title through
the shared guide layout. Use the PRODUCT-0114 producer-keyed estimate; do not
recompute a character budget locally.

**Stage 3 — uniform overlay clip.** §6, including the single-plot path, then
run the production-renderer evidence target. SVG operation traces establish
policy and scope; production pixels establish the realized clipped result.

## Acceptance gates

1. **The public word names ticks.** `x_tick_rotation(45)` reaches authored
   settings with its whole-call span. `x_label_rotation(...)` is unknown. The
   latter matters because accepting the historical field name permanently
   publishes a false promise about the axis title.
2. **Inclusive, typed degrees.** `-90`, `0`, and `90` compile; `-90.1`,
   `90.1`, non-finite values, and non-numbers refuse at the word span. Every
   resolved carrier holds `TickRotation`, not raw degrees; the only radians
   conversion is the newtype method.
3. **Cascade and atomic typography agree.** All five tiers resolve in order;
   the partial rotation slot wins within a tier over a complete `PlotTypography`.
   Removing it exposes the atomic value again. This is the two-authorities pair:
   testing only the winning case passes an implementation that replaces the
   whole typography carrier.
4. **Zero preserves placement, with one explicit clip addition.** At `0°`, the
   single and facet traces retain their banked text positions, alignments, and
   PRODUCT-0077 shared-edge anchors exactly; there is no endpoint translation.
   The sole admitted difference is the single plot's new figure-bounds clip,
   which gate 9 asserts positively. This is not a waived parity claim: a
   pre-existing label which overflows the figure is now visibly cropped at
   `0°`, while an in-bounds label has byte-identical pixels. The pair catches
   both an unconditional zero-angle nudge and an omitted clip.
5. **Exactly one outer endpoint moves at either sign.** At `+45°`, one and only
   one outer figure endpoint changes from centred to an inward alignment; at
   `-45°`, the opposite endpoint does. The interior labels remain centred.
   Testing only one sign passes an implementation that hard-codes a screen-side
   guess.
6. **One helper owns every correction half.** Every rotated run's vertical
   containment translation, plus the changed endpoint's horizontal alignment,
   come from one helper consuming the PRODUCT-0114 estimate. No x-axis draw
   loop contains its own character estimate, angle-sign branch, or endpoint
   nudge.
7. **The compensated run stays in the reserved band.** At `±90°` the vertical
   placement reaches, but does not exceed, the bottom extent already derived by
   PRODUCT-0114. This catches an anchor-only change that swings the run into
   the plot despite a sufficient band.
8. **All three x-axis paths obey the same policy.** Cartesian formatted labels,
   categorical-x labels, and categorical-xy labels produce the same
   zero/nonzero endpoint behavior. A Cartesian-only repair passes the long
   category exhibit while leaving heatmaps and categorical plots wrong.
9. **Figure clipping is uniform.** Both single and facet overlays emit an
   explicit clip of their complete figure bounds. An intentionally wider-than-
   figure label is cropped only at that outer boundary, never at an interior
   facet boundary.
10. **The uniform clip is inert in the ordinary case.** A label which fits
    within the figure after endpoint correction renders byte-identically with
    and without the mandatory figure clip. The operation remains present; only
    its pixels are inert. A crop-only implementation passes gate 9 while
    visibly retaining the defect, and a conditional-clip implementation would
    violate its uniformity.
11. **No geometry or legibility policy sneaks in.** At any fixed angle the
    PRODUCT-0114 band formula and the set of tick values are unchanged. No
    thinning, truncation, format change, or backend glyph measurement is added.
12. **No y-tick spelling or rotation path.** `y_tick_rotation(...)` is unknown,
    and y-axis draw sites do not call the new endpoint helper. This pins §7 as a
    decision rather than letting API symmetry decide it later.
13. **Production-renderer evidence.** Extend `product_0057_render_evidence` to
    capture both signs and the too-wide-label backstop. SVG traces establish the
    semantic policy; iced/wgpu pixels establish that clipping and rotation
    realize it. Neither oracle is credited with the other’s claim.
14. **The sequence preserves PRODUCT-0114 at zero.** At `0°`, the shared guide
    layout reduces exactly to PRODUCT-0114 §3's banked maximum and preserves
    the title's historical top. This catches a sequence which changes every
    existing title merely because it is now able to clear a rotated run.
15. **A present title clears every rotated tick run.** At both signs and at
    `±90°`, the title top is at or below `tick_end + title_gap`, and the final
    bottom is that top plus the title size. The pre-correction `45°` exhibit
    fails this gate while still fitting both extents in its band.
16. **An absent title spends no phantom gap.** With no rendered x-axis title,
    the final bottom equals `tick_end` and there is no title top. This catches
    an implementation which adds a clearance for content it does not draw.

## Stop conditions

> If the desired endpoint alignment cannot be represented by iced text
> alignment plus a translation without changing glyph shaping or adding a
> backend API, stop. This record deliberately uses existing anchors and
> PRODUCT-0114's estimate; a shaping requirement is a measurement/backend
> decision, not a local fallback.

> If a rotation within `[-90, 90]` makes the bottom reservation insufficient
> when the §§4–5 authorities use the PRODUCT-0114 estimate, stop. That falsifies the
> record’s substrate premise and returns to the band authority rather than
> adding endpoint-only padding.

> If explicit figure clipping changes parent/child z-order in the live renderer,
> stop. `grid.rs` already documents that nested clipping has renderer-specific
> ordering consequences; reproducing that constraint in single plots needs its
> own render-layer decision.

> If the rotated exhibit remains unreadable after anchoring without overflowing
> or clipping, stop. §8 says that is a tick-density/format policy question, not
> evidence for a more aggressive placement heuristic.

## Supersession

Supersedes PRODUCT-0114 gate 12, which correctly prohibited a rotation surface
until its rotation-aware band existed. It also supersedes PRODUCT-0114 §3's
bottom-band maximum: that formula was correct only while x ticks and the axis
title were independently anchored and one dominated. The sequenced form in §5
reduces to that maximum at `0°`, preserving 0114's parity captures and gate 3,
but becomes the current contract once rotation makes the tick run reach the
title.

It does not supersede PRODUCT-0077: shared facet-edge anchoring remains its
rule at zero rotation and combines with §4 only when a transformed run reaches
an outer figure edge.

The historical internal name may disappear in the same implementation. The
public Rune vocabulary does not inherit it.

## Rejected

**Publish `x_label_rotation`.** Rejected because `x_label` names the axis
title, not its generated ticks. A spelling that sounds correct but rotates
different text is worse than an unimplemented feature.

**Keep raw `f32` degrees.** Rejected because the Rune constructor would
validate one path while theme, pane, and persistence carriers could inject
another. The newtype is the boundary that makes invalid values unrepresentable
after every entry path, not just after authoring.

**Anchor both endpoints inward.** Rejected because a nonzero transformed run
has a direction. One endpoint is carried away from the figure boundary, so
moving it is a visible and needless loss of tick-to-label centering.

**Clip instead of anchoring.** Rejected because a clean vertical crop reads as
a rendering error and leaves a label that could fit looking broken. Clipping is
necessary for a label wider than the entire figure, but it is not the ordinary
placement policy.

**Make clipping Polariton-only decoration.** Rejected because the overlay would
paint as though a boundary existed while the plot’s layout authority had not
reserved or stated one. Both heads must receive the same figure clip through
`PlotBackend`.

**Add y-tick rotation for symmetry.** Rejected because it has no surveyed
reservation or crowding contract. Surface symmetry would create an authoring
promise before the geometry exists to keep it.

## Consequences

Long category, temporal, and formatted numeric labels gain an honest author
response: the author chooses a conventional angle, the shared bottom guide
reserves the rotated height and places the axis title after it, and only the
endpoint actually carried outward is shifted inward. The uniform figure clip
also repairs a pre-existing single-plot escape: unrotated labels which already
paint beyond the figure now crop at its edge. That visible change is intentional
and gated; labels that fit remain pixel-identical. A label wider than the figure
can still crop, but that is an explicit, uniform backstop rather than the
default experience.

The record also cleans up a naming trap before it becomes public. The internal
field survived harmlessly while it was unreachable; a Rune word would make the
misnomer part of the language. `x_tick_rotation` keeps behavior, neighbouring
vocabulary, and diagnostics aligned.
