# ADR 0120: the plot padding surface

Status: proposed 2026-08-16. PRODUCT-0119 establishes the least-surprising
endpoint rule: every x-tick label stays at its tick and an overlong run clips
at the figure edge. The clip is correct, but an author sometimes wants to
trade plot area for room at that edge. Polariton already has four plot-padding
values; it has no Rune surface that can set one without rebuilding all eight
unrelated padding and offset values.

This record adds `padding().top(n)`, `.right(n)`, `.bottom(n)`, and `.left(n)`
as explicit authored **floors** on plot-edge reservation. It leaves label
placement, clipping, estimator policy, and automatic guide reservation alone.

It follows PRODUCT-0119. It is not an automatic trailing-edge reservation:
the author, not a long endpoint label, chooses whether more right-side space is
worth a narrower plot.

## Context

### The clipped endpoint is correct, not a cue for automatic geometry

At positive rotation, a long final label can extend rightward beyond the
figure. PRODUCT-0119 keeps its origin at the final tick and the existing
figure-edge clip crops its tail. That is more truthful than the old
estimate-derived endpoint translation: clipping shows a limitation; detaching
the label makes it appear to name another tick.

An automatic right-band derivation would solve the crop by shrinking every
figure whose longest endpoint might overhang. It would be invisible to the
author, vary with data membership, and make a plot narrower for a trade the
author did not request. PRODUCT-0114's automatic bands are different: they
reserve space for guide occupants that would otherwise overlap the plot itself.
An endpoint crop is a legibility choice once its tick attachment is preserved.

The explicit answer is therefore:

```rune
padding().right(120)
```

The plot becomes 100 pixels narrower than its default right margin, and the
author can see both the cost and the endpoint text recovered by that choice.

### The carrier exists but has the wrong granularity

`PlotPadding` already carries `top`, `bottom`, `left`, `right`, and four
independent heading/label offsets. `PlotSettingsLayer` carries it only as an
atomic `Override<PlotPadding>`. Constructing a right edge from Rune through
that slot would require inventing values for the other seven fields.

This is PRODUCT-0091's partial-override problem for a third time. Its
continuous-ramp slots and PRODUCT-0112's `hover_size` established the rule:
when an author owns one field of a broader carrier, resolution gets a typed
specific slot instead of reconstructing the carrier.

### Requirements already reserve two edges, but do not compose with padding

PRODUCT-0114 derives left width from y-label requirements and bottom height
from x-label requirements. Those values currently replace
`plot_padding_left` and `plot_padding_bottom` when requirements exist. That
makes a future authored `padding().left(…)` or `.bottom(…)` silently inert in
the figures where guide room matters most.

The relationship is a floor, not a replacement:

```text
effective_left   = max(derived_left, authored_left)
effective_bottom = max(derived_bottom, authored_bottom)
effective_top    = authored_top
effective_right  = authored_right
```

Here “authored” includes the resolved cascade value: an absent Rune modifier
preserves the current default/application/pane value. Top and right have no
requirement-derived occupancy in this cut. Heading and facet rows continue to
compose outside `effective_top`, as PRODUCT-0106 already established.

This has a deliberate visible consequence: `padding().bottom(50)` can appear
to do nothing when a rotated label requirement already needs 120 pixels. The
author asked for a **minimum**, not permission to draw guide text into 50
pixels; the derived reservation wins.

### An edge is one concept with four named members

Four flat words such as `plot_padding_right(120)` would broaden the global
presentation vocabulary for four facets of one concept. A parameterized
`padding("right", 120)` would turn a semantic edge into a stringly argument
and lose the receiver/method vocabulary Rune uses for figure intent.

`padding()` is the concept-owning member, and its named edge modifiers make
the ownership visible:

```rune
padding().right(120)
padding().right(120).bottom(24)
```

This is not a dummy member manufactured to hold a modifier. `padding` owns all
four edges, admits repeated-edge refusal at the second modifier span, and
leaves room for future edge policy without adding unrelated top-level words.

## Decision

### 1. `padding()` owns four named edge modifiers

`padding()` is a figure member. Its associated `.top(n)`, `.right(n)`,
`.bottom(n)`, and `.left(n)` modifiers each accept one finite non-negative
pixel value. A modifier may occur at most once; different edges compose in any
order. `padding()` contributes no text, mark, guide, or layout role until at
least one edge is set.

The values use one bounded `PlotPaddingPixels` newtype through every carrier.
It admits every finite non-negative pixel value; it is not capped by the
current window or by an unrelated facet-gutter limit. The author value persists
unchanged, while the existing geometry clamp remains a safety limit on a
too-small realized widget rather than becoming a hidden second setting. `0` is
accepted and is an explicit no-op for that edge.

The modifiers are spellable on every figure member under Rune's associated
method model, so non-padding receivers refuse at the modifier-call span. A
bare `padding()` is valid but has no resolved edge overrides; it is not a
shortcut for rebuilding the complete `PlotPadding` carrier.

### 2. Each edge has a partial typed override

`PlotSettingsLayer` gains one `Override<PlotPaddingPixels>` slot per edge.
Within a cascade tier, an edge slot wins over the atomic `padding` carrier,
following PRODUCT-0091 and PRODUCT-0112's partial-override precedence. The
four slots resolve independently, persist as authored intent, and do not
construct a replacement `PlotPadding` value.

The existing atomic carrier remains for application/default sources that
legitimately own all eight padding and offset values. This record changes only
the authored figure's field granularity.

### 3. Left and bottom compose by `max`; top and right remain direct edges

`VerticalGuideBands` computes `effective_left` and `effective_bottom` with the
floor relation above. The same authority which derives requirements applies the
author's floor; no draw loop, kernel, or grid cell recomputes it.

Top keeps PRODUCT-0106's heading/facet composition and then applies the
resolved top padding. Right is subtracted once from the plot rectangle as it
is today. Neither is inferred from label text, rotation, or endpoint overflow.
The author can therefore add right room for a clipped final label without
changing its origin, its alignment, or its clip boundary.

### 4. The setting changes geometry, never text placement

At a fixed figure size, increasing one edge by Δ changes the plot rectangle by
exactly Δ on that edge, subject only to the existing zero-size safety clamp.
The transform and marks consume the smaller rectangle proportionally. Axis
labels remain attached to transformed ticks under PRODUCT-0119; no padding
value becomes an x/y text offset or a new endpoint branch.

This is an intentional geometry move when used. A figure without authored
padding is byte-identical, including the default 20-pixel right margin and its
current clipped endpoint behaviour.

### 5. The surface is figure-local, not pane or facet spacing

`padding()` is inside one figure's plot rectangle. It is neither
PRODUCT-0110's `facet_gutter`, which reserves shared pixels between facet cells,
nor PRODUCT-0111's `config.pane_divider_gutter(columns, rows)`, which reserves
terminal cells between editor panes. Applying an edge floor per grid cell would
double-count it and violate PRODUCT-0048's shared guide geometry.

## Implementation sequence

**Stage 0 — bank the unspelled carrier.** Capture default single and faceted
plot rectangles, including a rotated `test37` endpoint at the 20-pixel default
right margin. Bank the five-tier cascade precedence for an atomic application
`PlotPadding` versus one prospective edge slot.

**Stage 1 — partial carrier and resolver.** Add the non-negative newtype and four
partial slots, then prove every carrier, cascade tier, and snapshot path uses
them. With no Rune producer yet, all legacy output remains byte-identical.

**Stage 2 — `padding()` member and edges.** Add the member register, modifiers,
origins, duplicate/refusal behaviour, and lowering to the partial slots.

**Stage 3 — one geometry authority.** Apply the floors in
`VerticalGuideBands`, re-capture the right-padding exhibit, and verify the
tick-origin invariant and the zero case.

## Acceptance gates

1. **Each edge is independently authorable.** One Rune figure accepts
   `padding().right(120).bottom(24)` in either modifier order. It resolves two
   typed edge slots and no values for top, left, title offset, subtitle offset,
   x-label padding, or y-label padding. This fails an atomic-carrier
   reconstruction which can render but invents unrelated settings.

2. **Second edge modifiers refuse at their own spans.** A second `.right(...)`
   refuses at that second call; a `.right(...)` on `title(...)` or `skin(...)`
   refuses at the modifier span. The first distinguishes duplicate state from
   two different edges; the second distinguishes spellability from admission.

3. **The newtype is total through every carrier.** `PlotPaddingPixels`, not a
   raw `f32`, appears in authored intent, all four partial slots, resolution,
   persistence, and geometry input. Negative and non-finite values refuse;
   `0` and an oversized finite request such as `right(100000)` accept.

4. **Partial wins within a tier; the carrier stays atomic elsewhere.** A tier
   containing an atomic `PlotPadding` and `right` partial resolves the partial
   only for right; the atomic carrier still supplies the other seven fields.
   Across all five tiers, the normal cascade order remains unchanged. This
   rejects a partial implementation that replaces the carrier.

5. **The derived left/bottom band is a floor.** A fixture whose y/x guide
   requirements exceed its authored left/bottom values resolves the derived
   values; one whose authored values exceed the requirements resolves the
   authored values. Both halves are necessary: an unconditional authored value
   breaks guide room, while an unconditional derived value makes the surface
   inert.

6. **Right is explicit, not automatic.** At the same data and rotation,
   omitted `padding()` preserves the banked 20-pixel right margin and clipped
   `test37` endpoint. `padding().right(120)` moves only the plot's right edge
   left by 100 pixels, bringing room to the endpoint without deriving a value
   from its label. This rejects a silent long-label reservation.

7. **Right padding makes the endpoint more visible.** At one fixed figure
   size, compare the same overflowing endpoint with no padding and with
   `padding().right(120)`. The endpoint tick moves left by exactly 100 pixels
   while the figure clip rectangle is byte-identical. Those operation-list
   facts entail that 100 additional horizontal pixels of the same attached run
   lie inside the clip, without measuring a glyph. This fails a repair that
   moves the clip to the plot rectangle, where padding would not reveal any
   additional endpoint text.

8. **Tick attachment survives all edges.** At two figure widths, every
   x-label's logical origin remains its transformed tick after each individual
   edge floor. PRODUCT-0119's motion-coherence invariant cannot be weakened by
   a layout setting.

9. **One edge moves one rectangle boundary.** With an asymmetric fixture, each
   edge changes only its own plot-rectangle boundary by its effective delta;
   data mapping remains proportional in the new area. This rejects applying a
   padding value twice, treating it as outer figure margin, or changing text
   placement compensatingly.

10. **Facets apply a shared edge once.** A two-cell grid with `right(120)`
   subtracts that edge once from the composed figure before cells divide it.
   It does not add 120 pixels per cell or become a `facet_gutter`.

11. **Zero and omission preserve legacy output.** `padding().right(0)` and a
   figure with no padding member reproduce the banked operation trace exactly.
   This catches an unconditional reservation or a bare `padding()` side effect.

12. **Oversized padding is a request, not a geometry state.**
   `padding().right(100000)` persists and round-trips as `100000`; on a
   900-pixel figure its effective plot width clamps at zero rather than becoming
   negative. Reopening that figure in a wider window applies the original
   request, never a prior effective width. This is PRODUCT-0110 §4's
   request-versus-safety-clamp rule applied inside one plot rectangle.

13. **The renderer has no new text capability.** No `PlotBackend` measurement
   method, realized paragraph width, endpoint translation, or automatic
   requirement-derived right band appears. PRODUCT-0093 §7 remains deferred.

## Stop conditions

> If one partial edge cannot be carried through a cascade tier and persistence
> without reconstructing all of `PlotPadding`, stop. A whole-carrier replacement
> is the two-authorities failure PRODUCT-0091's partial-slot pattern prevents.

> If a left or bottom authored value cannot compose with its derived guide band
> in the single `VerticalGuideBands` authority, stop. Do not let a kernel or
> grid cell choose a second `max` policy.

> If a proposed repair uses a label's text, rotation, or endpoint overflow to
> infer right padding, stop. That is automatic geometry policy, deliberately
> excluded from this record.

> If an endpoint remains unreadable after an explicit author-selected padding
> value, stop. Abbreviation, thinning, formatting, or a different rotation is
> a legibility record; extra padding must not grow into an automatic substitute.

> If an oversized finite request cannot preserve its authored value while
> geometry clamps only the live rectangle, stop. Persisting the effective
> rectangle would create the hidden second setting PRODUCT-0110 rejected.

## Supersession and inheritance

This record does not supersede PRODUCT-0119. It inherits its rule exactly:
labels stay at ticks and the figure clip handles overflow. `padding().right`
changes the available plot rectangle before ticks are transformed; it never
changes a label after transformation.

It extends PRODUCT-0114's band authority to make its existing left/bottom
derivations compose with explicit floors. PRODUCT-0114 continues to own
requirements and PRODUCT-0117 continues to own their estimate. Neither gains a
trailing-edge inference, so the clip remains the default unless an author
chooses otherwise.

PRODUCT-0110 and PRODUCT-0111 are inherited as distinct reservations:
facet gutters and editor-pane divider gutters do not become padding aliases.

## Rejected

**Automatically reserve the endpoint's projected width.** Rejected. It hides a
data-dependent geometry cost in every long-label figure and gives the author
no control over the trade between plot width and complete text.

**Keep a flat `plot_padding_right(n)` vocabulary.** Rejected. Four global
words represent one edge-owned concept and do not compose naturally.

**Use `padding("right", n)`.** Rejected. A string argument makes a closed
semantic edge set unchecked vocabulary instead of named figure members with
separate origins and duplicate rules.

**Let authored bottom/left override requirements.** Rejected. A too-small
value would knowingly draw guide occupants into insufficient space. Floors
preserve the author’s request whenever it is safe and preserve layout honesty
when it is not.

**Treat padding as a pane or facet gutter.** Rejected. Those are shared space
between objects; padding is space inside one figure's plot rectangle. Conflating
them would apply the same pixels under incompatible ownership and units.

## Consequences

Authors gain a visible, local control for the precise trade the screenshot
exposes:

```rune
padding().right(120)
```

The plot narrows and the endpoint gets more room, but the label remains
attached to its tick whether it is fully visible or clipped. The default stays
compact and clips honestly; no data value silently expands any figure.

This is the third reuse of the partial-override pattern and the first to prove
that an authored layout value can floor a derived guide requirement. Its gates
therefore protect both the language surface and the existing reservation
authority from becoming two competing layout systems.
