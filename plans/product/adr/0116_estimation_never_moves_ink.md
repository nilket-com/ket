# ADR 0116: estimation reserves space; it never moves ink

Status: proposed 2026-08-16. PRODUCT-0115 keeps a rotated tick run out of the
plot by translating it down by an amount derived from PRODUCT-0114's
**conservative width estimate**, while the renderer draws the run at its
**realized** width. The two are not the same number, so every label is pushed
below the tick baseline by half its own estimation error — a few pixels for
`Ship`, over a hundred for a fifty-character label.

This record removes the vertical translation entirely by choosing the pivot
instead of compensating for it, and states the general rule the defect
violated:

> **A conservative estimate may reserve space and may apply a figure-space
> inward containment correction. It may never determine a run's vertical
> placement or its distance from the tick baseline.**

## Context

### One symbol meant two different widths

`XTickTextPlacement::resolve` (`common.rs:1306-1341`) computes

```text
run          = [-estimated_width/2, +estimated_width/2]   // Center alignment
min_y        = min(run_start·sin θ, run_end·sin θ)
y_offset     = max(-min_y, 0)
```

and the backend then draws the string at its realized width. At 90° with
centred alignment the translation is `estimated_width/2` while the run is
`[-actual_width/2, +actual_width/2]`, so the realized top lands at

```text
(estimated_width - actual_width) / 2
```

which is zero only when the estimate is exact. `ASCII_LABEL_ADVANCE_EM = 1.0`
against roughly `0.7` em for mixed-case prose makes that gap proportional to
label length, which is why short labels sit at the axis and long ones are
stranded far below it.

### Why nothing caught it

PRODUCT-0115's gate 7 constrains the run against the band's **bottom** extent,
which this defect satisfies — the run is pushed further from the bottom, not
past it. No gate constrained where runs start **relative to each other**, and
the derivation that justified the translation was written in terms of a single
`w`, so it was self-consistent and wrong. The estimate appeared on both sides
of an equation where only one side was real.

It was found by looking at a figure. That is the third defect in this arc
surfaced by an exhibit rather than by a gate.

### Alignment is currently doing two jobs

`resolve` picks alignment to correct an outer endpoint inward
(`common.rs:1320-1331`), and that same alignment then determines the run's
vertical extent through `min_y`. The two purposes conflict: at positive
rotation, `Left` yields `run = [0, w]` and a zero vertical correction, while
the endpoint rule wants `Right` at the maximum endpoint, which yields
`[-w, 0]` and reintroduces the full translation.

One knob cannot serve both. §§3-4 separate them.

## Decision

### 1. The invariant

> An estimate may reserve space and may apply a figure-space inward containment
> correction. It may never determine a run's vertical placement or its distance
> from the tick baseline.

The precision matters, because §4 does let an estimate decide where one run's
ink lands horizontally. That use is bounded and self-limiting: an over-estimate
pulls the run further inside a boundary it was at risk of crossing, and the
worst outcome is a slightly over-indented endpoint label.

The forbidden uses are the two where error compounds instead of self-limiting.
Vertical placement turns estimation error into a length-proportional cascade —
the defect this record repairs. Distance from the tick baseline would detach a
label from the tick it names by half its own error, which is why §3 anchors at
the tick and spends no width at all.

Any placement expression mixing an estimated width with a realized one in those
two dimensions is wrong by construction.

### 2. Alignment is chosen so the transformed run has no negative vertical extent

The pivot is selected — not compensated for. For a run rotated by θ, the
alignment whose transformed extent satisfies `min_y = 0` is chosen, so the run
begins at the tick baseline and extends downward only.

Which alignment that is follows from the sign of the transformed run, computed
in one place, exactly as PRODUCT-0115 derives its endpoint selection. It is not
written down as a prose claim about a rotation sign, because the answer depends
on the y-down canvas convention and would be wrong half the time if asserted
rather than derived.

The consequence is that **the vertical correction becomes structurally zero**
at every nonzero rotation, rather than a computed value that happens to be
right when the estimate is exact.

### 3. Rotated runs anchor their leading edge at the tick, with no centring

§2 fixes the pivot, which raises where the run sits along the axis. At `0°` a
label is centred on its tick. At nonzero rotation it is **not**: the run's
leading edge is anchored at the tick and the text reads as emanating from it.

This is the conventional treatment and it is chosen here for an exact reason:
it uses **no width at all**. Keeping rotated labels visually centred would mean
shifting each by half its width, and the only width available is the estimate —
reintroducing the defect on the other axis. At 45° a fifty-character label's
estimation error is on the order of a hundred pixels, so an estimate-centred
label would visibly detach from the tick it names.

Interior runs therefore receive **no correction of any kind**: their origin is
their tick, their alignment comes from §2, and no estimated quantity
participates in either. "Uncorrected" means uncorrected, not centred by
estimate.

### 4. Endpoint inward correction is a figure-space x translation

The outer-endpoint correction PRODUCT-0115 expressed as an alignment change
becomes a translation instead — applied in **figure space**.

The draw path translates, then rotates, then fills text at the local origin. A
correction folded into the pre-rotation translation vector is a pure
figure-space shift; one applied to the text's position inside the rotated frame
is a local offset that rotation converts into vertical movement, reintroducing
exactly what §2 removes. The correction is therefore added to the translation
vector, and gate 8 asserts its `Δy` is zero rather than merely calling it
horizontal.

The estimate remains appropriate here: pulling a run inward by an over-estimated
amount moves it further inside the figure, which is the conservative direction
and cannot displace it vertically.

### 5. Every run on an axis shares one baseline y

With §2 in force, all rotated runs on the same axis are emitted at the same
`y`. Their tops coincide regardless of their lengths or of how wrong the
estimate is, and the labels read as one block hanging from the axis instead of
a cascade.

### 6. What the estimate may still do

Unchanged: PRODUCT-0114's band derivation, including `rotated_tick_height` and
the sequenced title placement. Unchanged: PRODUCT-0115's containment intent and
its figure-edge clip. The estimate keeps sizing the space and now also supplies
§4's horizontal pull. Its conservatism moves to the **bottom** of the band,
where unused reservation is invisible, instead of appearing as a gap above the
text.

### 7. This narrows PRODUCT-0115 §5

PRODUCT-0115 stated the endpoint correction as an alignment change and gated it
that way. §4 replaces the mechanism while preserving the behaviour: exactly one
outer endpoint is still corrected inward, at either sign, and the interior runs
are still uncorrected. Only the means changes, from pivot to translation.

## Implementation sequence

**Stage 0 — bank the failing pair.** Capture `test37` at 45° and 90°, and
record the current run-top disagreement between the shortest and longest label
on the same axis. This is the negative control: it must be red before the fix
and green after.

**Stage 1 — pivot selection and tick anchoring.** §§2-3 in the one helper, with
the vertical term removed rather than set to zero by a branch.

**Stage 2 — horizontal endpoint correction.** §4, restoring PRODUCT-0115's
endpoint behaviour through a figure-space translation.

## Acceptance gates

1. **Zero vertical correction exists at all.** Structural: the helper emits no
   vertical term at any rotation. Per §2, so the invariant is enforced by the
   absence of the expression rather than by a value that could drift.
2. **All rotated runs on an axis share one baseline y.** Every rotated x-tick
   text operation carries the same `y` — the **logical text-run origin**, not
   the glyph ink top, which font bearings can shift slightly even under
   identical origins and transforms. Operation-list assertion, exact, no
   measurement.
3. **The chosen alignment has no negative vertical extent.** At both signs and
   at ±90°, the transformed run's `min_y` is zero **for any positive realized
   width**. This is the half that makes gate 2 meaningful — equal `y` with
   centred runs still puts every run top at a different height, so the two only
   prove common tops together.
4. **The short/long pair agrees.** The shortest and longest label on one axis
   have equal logical run tops at 45° and at 90°. Against Stage 0's banked
   disagreement, so a gate that passes today would be vacuous.
5. **Interior runs originate at their ticks.** At nonzero rotation an interior
   label's text origin is its tick position exactly: no centring offset, no
   estimated quantity, no boundary correction. Per §3, and the gate that
   distinguishes "uncorrected" from "centred by estimate" — an implementation
   shifting every interior run by half an estimated width would satisfy gates
   2-4 while detaching labels from the ticks they name.
6. **No estimated width appears in a vertical expression.** Structural, per §1:
   the estimate reaches the band derivation and §4's horizontal translation and
   nothing else. This is the general rule, gated once.
7. **The corrected endpoint is contained, not merely nudged.** Exactly one
   outer endpoint is corrected at either sign — PRODUCT-0115's gate 5 behaviour
   surviving §4's change of mechanism — **and** that run's conservative
   horizontal extent lands inside the figure bound afterwards. Both halves:
   a one-pixel `Δx` satisfies "an endpoint moved" and gate 8 while preserving
   none of PRODUCT-0115's containment.
8. **The endpoint correction's `Δy` is zero.** Asserted on the emitted
   translation vector in figure space, not described as "horizontal" — a local
   offset applied inside the rotated frame is horizontal in its own space and
   vertical in the figure's. Per §4, and the gate that catches the one
   coordinate-space mistake this design can make.
9. **Zero rotation is byte-identical.** At `0°` the operation trace matches
   PRODUCT-0115's banked capture exactly, including its figure clip.
10. **The band is unchanged.** PRODUCT-0114's derived bottom extent is identical
   before and after at every angle. This record moves ink within the band; it
   does not resize it.
11. **Production pixels corroborate the absence of the cascade.** One headless
    capture confirms that run tops no longer vary with label length. It does
    **not** demand exact glyph-bound equality: bearings differ per string, so
    the operation list is where the invariant is exact and the pixels are where
    the symptom is absent.
12. **No backend measurement.** Structural: `PlotBackend` gains no glyph or
    text-extent method, and no test substitutes realized paragraph width for
    the operation-list assertions. PRODUCT-0093 §7 stands.

## Stop conditions

> If the vertical invariant cannot hold without knowing a realized width, stop.
> The pivot-selection argument has failed, and what remains is the
> backend-measurement decision rather than this record.

> If endpoint correction cannot be expressed horizontally without reintroducing
> a pivot change, stop. Alignment would again be serving two purposes, which is
> the defect's structural cause rather than its symptom.

> If any figure's band changes size, stop. This record redistributes ink inside
> PRODUCT-0114's reservation and must not renegotiate it.

## Supersession

Narrows PRODUCT-0115 §5's mechanism (§6) while preserving its behaviour.
Changes no decision of PRODUCT-0114. Leaves PRODUCT-0093 §7's deferral standing
with its trigger intact, and deliberately does not spend a measurement
capability on a property that turns out to be checkable from the operation list
once the pivot is chosen correctly.

## Rejected

**Compensate more accurately.** Attractive because it is a one-line change:
translate by the realized width instead of the estimate. Rejected because
realized width is exactly the measurement PRODUCT-0093 §7 defers, and because
correcting the number leaves the structure — an estimate in a placement
expression — intact for the next author.

**Keep alignment for endpoints and translate vertically for containment.**
Attractive because it is the current design and only the magnitude is wrong.
Rejected per §6: one knob serving two purposes is why the estimate reached a
vertical expression at all.

**Tighten `ASCII_LABEL_ADVANCE_EM` toward the realized average.** Attractive
because it shrinks the visible gap immediately. Rejected because it treats a
correctness defect as a tuning problem: the gap would shrink and remain
proportional to error, and a tighter advance would under-reserve for all-caps
labels, converting a placement bug into a clipping bug.

**Fold this into a legibility record.** Attractive because the visible symptom
is a figure whose labels look too large. Rejected because the causes are
unrelated: legibility policy addresses labels that are genuinely too long,
while this addresses ink displaced by estimation error. Fixing the second makes
the first easier to judge honestly.

## Consequences

Rotated labels form one block hanging from the axis. The estimator's
conservatism stops being visible as a gap above the text and becomes unused
space at the bottom of a band, which is what conservatism is supposed to look
like.

The general rule is now stated and gated once: an estimate reserves and pulls
inward, never places. That rule did not exist while the only estimate in the
system sized a legend gutter, where error is invisible. Rotation was the first
consumer that could turn error into displacement.

The defect also documents a limit of the arc's gate discipline. PRODUCT-0115's
derivation was internally consistent and used one symbol, `w`, for two
quantities that were equal only under an assumption nobody wrote down. A gate
written from that derivation inherits its blind spot, which is why the exhibit
found it and the suite could not.
