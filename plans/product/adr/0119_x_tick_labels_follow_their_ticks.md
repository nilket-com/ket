# ADR 0119: x-tick labels follow their ticks

Status: proposed 2026-08-16. PRODUCT-0118 made an outer x-tick label's
correction mathematically minimal: it moves only by its overflow beyond the
figure edge. A pane-resize capture shows that this is still the wrong policy.
Every ordinary label moves with its tick; the final label's position also
depends on the available figure width, so it appears to move independently of
the axis it labels.

This record restores the direct invariant: **every x-tick label originates at
its tick.** The existing figure-edge clip handles any endpoint overflow. An
incomplete but correctly attached label degrades information; a displaced label
claims the wrong tick.

It follows PRODUCT-0118. It changes no Rune spelling, rotation angle, font,
estimator, guide-band reservation, facet-divider policy, or tick-density
policy.

## Context

### Motion coherence is the property, not containment

At one static width, a containment assertion can make several placements look
reasonable. During a pane resize, an axis is plainly one object: ticks and
their labels should translate together. The `test37` endpoint exposes the
difference. `North American enterprise renewal commitments` is the final
category in the live ordered relation. As the pane becomes narrower, ordinary
labels retain their tick-relative origin while that label moves left by the
changing overflow of its conservative projected run.

PRODUCT-0118's formula is correct arithmetic for its stated objective:

```text
correction = overflow beyond the figure boundary
```

It is nevertheless a second placement authority. The tick determines a label's
normal position; the figure width determines the endpoint's exceptional one.
Minimality improved the earlier full-width retraction, but it did not restore
the relation the author sees while resizing.

The direct, exact property is simpler:

```text
for every x-tick label: text_origin.x - tick.x == 0
```

That applies to endpoints, interior labels, every width, every x-axis path,
and both rotation signs. It is an operation-list assertion, so it needs no
glyph measurement and no inferred overflow curve.

### Clipping degrades honestly; displacement lies

An endpoint run can extend outside the figure. PRODUCT-0115 already gives
single and faceted overlays an explicit figure-edge clip. With the direct
invariant, that clip is the primary overflow policy rather than the backstop
after a translation attempt.

The resulting `North American enterpri…` may be incomplete, but its beginning
remains at the tick it names. Pulling the complete label over an adjacent tick
instead makes it look as though it names that other category. Truncation is
degraded information; detachment is wrong information.

This does not decide whether clipping, abbreviation, thinning, or a different
rotation is the best legibility policy for an overcrowded figure. It decides
only that such a policy must not be implemented by moving a label away from its
tick.

### The estimate has acquired an exception it does not need

PRODUCT-0116 improved its invariant after an estimate created a
length-proportional vertical cascade:

> An estimate may reserve space and may apply a figure-space inward containment
> correction. It may never determine a run's vertical placement or its distance
> from the tick baseline.

The figure-space correction is the remaining carve-out. PRODUCT-0118's
`XTickTextPlacement` receives a typed `Rect<Plot>` solely to calculate it, and
then applies an estimate-derived `x_offset` to one endpoint. The resize movie
shows that an estimate has no more standing to decide horizontal tick-relative
distance than vertical distance.

The smaller invariant is the durable one:

> **An estimate reserves space. It never places ink.**

PRODUCT-0114 and PRODUCT-0117 still use the estimate for guide-band
requirements. PRODUCT-0116 still chooses a rotation pivot whose transformed
run has no negative vertical extent. Neither requires a placement correction.

### Probe order is already explicit and stays separate

`test37` now sorts `axis_labels_probe` by an explicit `axis_order` before
building the figure. That change stabilizes the test's endpoint identity; it is
not part of this policy decision. A probe whose final category changes with
relation delivery order cannot demonstrate endpoint motion coherently.

## Decision

### 1. Every x-tick label's logical origin is its tick

Every Cartesian, categorical-X, and categorical-XY x-tick text operation uses
the transformed tick coordinate as its text origin. At every rotation and
width:

```text
text_origin.x == tick.x
```

There is no endpoint enum, figure-bound transport, overflow formula, or
estimate-derived horizontal translation. The first and final labels have the
same positioning rule as all labels between them.

At nonzero rotation, PRODUCT-0116's leading-edge pivot remains responsible for
the run's vertical direction. It does not change the origin: the rotated run
still begins at its tick. At zero rotation, existing centred legacy placement
remains unchanged; the origin passed to the text operation is still the tick.

### 2. The figure clip is the complete endpoint-overflow policy

PRODUCT-0115's explicit figure-edge clip remains present for both single and
faceted overlays. Labels may cross internal facet boundaries under
PRODUCT-0048, and the clip applies only at the whole figure boundary.

An overflowing endpoint is emitted at its own tick and clipped at that bound.
No branch first asks whether the run fits, and no caller chooses a cell edge,
plot edge, fixed margin, or width-dependent translation. The clip is uniform:
it is emitted whether or not pixels currently reach it, and is inert for a
fitting label.

### 3. Delete the endpoint-placement apparatus rather than neutralising it

Remove PRODUCT-0118's containment rectangle transport, `XTickEndpoint`,
overflow arithmetic, and `x_offset`. Retaining a zero-valued carrier or a
fitting/overflow branch would preserve a second authority ready to be revived
by a later change.

This does not discard typed geometry generally. It removes a typed value whose
only purpose was a policy this record supersedes. Coordinate types continue to
protect actual cross-space geometry seams elsewhere.

### 4. Keep estimates in reservation authority only

The pinned-face, per-class estimator stays the sole authority for
PRODUCT-0114's band requirements. It supplies neither a text translation nor
an endpoint decision. No realized-width measurement is introduced.

This is stricter than PRODUCT-0116's original formulation: an estimate may
reserve space, and nothing else. Any future legibility improvement must operate
on labels, ticks, formats, or clipping policy directly; it may not reintroduce
an estimate-derived position.

## Implementation sequence

**Stage 0 — bank the motion failure.** Keep the explicit `axis_order` probe
and capture `test37` at two pane widths and both rotation signs. Record each
x-tick's logical origin relative to its transformed tick. The current outer
endpoint is the negative control: its offset changes with figure width while
the others remain zero.

**Stage 1 — delete correction transport and arithmetic.** Remove the
`Rect<Widget>`/`Rect<Plot>` endpoint carrier, endpoint classification, overflow
calculation, and `x_offset`; simplify all three x-axis paths to emit raw tick
origins. This must leave PRODUCT-0115's clips in place.

**Stage 2 — replace the old proof.** Retire PRODUCT-0118's exact-overflow
tests and architecture assertion deliberately. Add the direct motion-coherence
checks below, then re-capture the two-width fixture to corroborate the removal
of the free-moving endpoint.

## Acceptance gates

1. **Every origin is its tick at two widths.** An operation-list fixture with
   short and long labels at wide and narrow figure widths asserts
   `text_origin.x == tick.x` for every x tick, including both outer endpoints.
   The current overflow implementation fails on the overflowing endpoint; a
   fixed or minimal correction fails whenever it is nonzero.

2. **Both rotation signs share the rule.** The gate repeats at positive and
   negative rotation. No sign-specific endpoint path, alignment branch, or
   translation may make one outer label exceptional.

3. **All three x-axis producers share the authority.** Cartesian,
   categorical-X, and categorical-XY each emit raw transformed tick origins
   for their labels. A categorical-only deletion that leaves either copied
   endpoint policy fails structurally and behaviourally.

4. **The grid is coherent while it resizes.** The two-cell `test37`-shaped
   categorical grid runs at two widths. The delivered `North American…`
   endpoint, its compact-cell controls, and every other label all retain zero
   tick-relative horizontal offset. This names the observed failure directly:
   no label may follow figure width independently of its tick.

5. **Single and grid overlays keep the same policy.** One single plot and one
   faceted grid both route their x-axis text through the zero-offset authority.
   The figure-wide grid clip remains distinct from a cell edge, preserving
   PRODUCT-0048.

6. **The endpoint mechanism is gone.** Structural: no
   `XTickEndpoint`, endpoint-bound `Rect<Widget>`/`Rect<Plot>` transport,
   overflow formula, or `x_offset` survives in x-tick placement. The absence
   matters: a dormant carrier would still be a second placement authority.

7. **The clip is present and primary.** Each single and grid x-axis overlay
   emits its existing figure-edge clip whether or not a label overflows. A
   fitting label has identical pixels with that clip; an overlong endpoint is
   cropped rather than translated. A conditional clip or an overlay without a
   clip fails this gate.

8. **Vertical placement remains PRODUCT-0116's.** At both signs and at ±90°,
   rotated runs share their baseline y and use the pivot whose transformed run
   has no negative vertical extent. Deleting horizontal correction must not
   revive the length-proportional cascade.

9. **Zero rotation is unchanged.** At `0°`, the text-operation trace remains
   byte-identical to the PRODUCT-0118 banked trace. The deleted nonzero
   endpoint correction must not become a new legacy nudge.

10. **Production corroboration shows one moving axis.** At two pane widths,
    `test37` shows all x labels moving with their ticks. The final `North
    American…` label may be clipped at the figure edge, but it may not cross
    an adjacent tick by a width-dependent translation. Pixels corroborate the
    operation-list invariant; they do not replace it.

11. **No measurement capability.** `PlotBackend` gains no text-extent method,
    no realized paragraph width enters placement, and PRODUCT-0093 §7 remains
    deferred. The direct operation-list relation is sufficient evidence.

## Stop conditions

> If any x-axis path cannot emit the raw transformed tick as its text origin
> while retaining its existing figure-edge clip, stop. A path-specific offset
> is not an acceptable compatibility fallback.

> If the primary clip is absent, uses a cell boundary, or cannot crop an
> overflowing endpoint in either single or faceted overlays, stop and repair
> the clipping contract before deciding a different placement policy.

> If a correctly attached but clipped endpoint is not legible enough for the
> product, stop. That is a separate abbreviation, thinning, format, or rotation
> decision; it does not authorise moving the label away from its tick.

> If a future proposal needs actual glyph bounds to position a run, stop at
> PRODUCT-0093 §7's measurement boundary. Estimation may not regain a
> placement carve-out as an easier alternative.

## Supersession and inheritance

This record **supersedes PRODUCT-0118**, rather than refining it. PRODUCT-0118
correctly replaced full retraction with minimum overflow under a containment
objective. The resize exhibit changed the objective: containment is not enough
when one label ceases to move as part of its axis. Its typed boundary carrier,
overflow formula, endpoint branch, and minimality gates are removed and
replaced by gates 1–7's direct tick-origin contract.

It also narrows earlier rotation decisions:

- PRODUCT-0115's endpoint-inward policy is superseded. Its uniform figure-edge
  clip survives and becomes the endpoint-overflow policy.
- PRODUCT-0116's figure-space inward-correction exception is deleted. The
  invariant is now simply that an estimate reserves space and never places ink.
- PRODUCT-0117's estimator and PRODUCT-0114's band authority are inherited
  unchanged, but have no placement consumer.
- PRODUCT-0048's figure-wide grid clip remains unchanged; this record does not
  decide whether labels should cross an internal facet divider.

## Rejected

**Keep exact-overflow correction.** Rejected. It is mathematically minimal but
still makes one label follow available figure width rather than its tick.

**Return to full endpoint retraction.** Rejected. It intensifies the same
motion defect and can make a label appear to name an adjacent tick.

**Use a fixed or maximum endpoint nudge.** Rejected. A constant hides the
resize dependency at one width only and remains a second placement authority.

**Measure realized text width to improve correction.** Rejected. It cannot
make a width-derived translation motion-coherent, and it spends the backend
measurement capability PRODUCT-0093 §7 defers for a problem the operation list
can solve exactly.

**Suppress or abbreviate only the endpoint.** Rejected. That is a legibility
policy for all dense axes, not an endpoint-placement repair. It must be decided
separately if the uniform figure clip is insufficient.

## Consequences

An overflowing endpoint may visibly crop at the figure edge. That is an honest
and uniform consequence of keeping the label attached to its tick. The figure
no longer promises that complete endpoint text is always visible; it does
promise that a label never silently changes the tick it appears to describe.

The implementation gets smaller: three records' endpoint machinery is deleted
in favour of the relation the renderer already knows how to draw. The
acceptance gates become exact and stable under resize because they assert the
motion the author sees instead of deriving a proxy from containment arithmetic.

This arc reached “labels sit at their ticks” through five records: PRODUCT-0115
introduced endpoint correction, PRODUCT-0116 changed its mechanism,
PRODUCT-0118 made it minimal, and this record deletes it. That is not wasted
work. Each decision was correct under the containment objective available in a
static capture; only a resize made the stronger motion-coherence objective
visible. Some invariants exist only while a figure changes, so a suite of
static captures cannot express them on its own.
