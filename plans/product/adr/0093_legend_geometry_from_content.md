# ADR 0093: legend geometry from content

Status: proposed 2026-08-12. The gradient guide reserves a hard-coded 60 px
(`legend.rs:323`) and starts its endpoint text 30 px in, so any label wider than
about 30 px is clipped by the canvas. PRODUCT-0092's signed heatmap domain made
this visible for the first time: `-30.0` renders as `-30.(`. This record replaces
the constants with one conservative estimator over the formatted strings and one
shared geometry authority consumed by both measurement and rendering.

It precedes the mark-field transport refactor
(`plans/wip/plot-catalog-restoration.md` step 7 of 15), because it is a live defect in
a shipped default guide rather than a cleanup.

## Context

### It contradicts PRODUCT-0087's own principle

0087 established that a legend section carries **every value it prints**, and
repaired a gradient guide that displayed colours the data never used. A clipped
endpoint prints a value the reader cannot read — the same family of defect, at
the same guide, one layer out. 0087 called its case "a shipped misrepresentation,
not a missing feature"; so is this.

### What exists, read 2026-08-12

**`measure_legend` measures nothing.** Every width it returns is a constant
(`legend.rs:317-328`): `150.0` per entry column, **`60.0`** for a gradient,
`180.0` for a size sample, `250.0` for anything multi-section, plus an early
return of `Size::new(250.0, 240.0)` for the Gradient+SizeSample pair (`:310-314`).

**There are two geometry authorities.** The single-gradient path draws against
`measure_legend`'s width (`:509-510`). The Gradient+SizeSample path re-declares
`let width = 250.0` (`:416`) and derives `bar_x = x + width - 70.0` (`:426`)
without consulting it. The same number in two places, which is a divergence
waiting rather than a divergence today.

**The strings are already exact and already available.** Endpoints format as
`{high:.1}` / `{low:.1}` (`:443,452` and `:605,614`), and `measure_legend`
receives `ResolvedPlotSettings`, which carries `legend_size` (`common.rs:466`,
default `14.0`). No new carrier is needed.

**The backend deliberately has no glyph measurement.** `common.rs:577` and
`export.rs:16` both document that the abstraction excludes "realized glyph
extents." Any measurement here is an estimate by construction.

## Decision

### 1. One conservative text-width estimator

A single function estimates a rendered string's width from the string itself and
the resolved font size. It is used nowhere else and claims nothing else.

It is **an estimate, not a measurement** — the backend exposes no glyph extents
(`common.rs:577`), so precision is unavailable by design rather than by omission.

### 2. Over-reserving is permitted; under-reserving is not

The estimator may return more than the true rendered width. It must never return
less for the fixtures §5 declares.

The asymmetry is the whole design. Over-reserving costs a few pixels of gutter
that nobody will notice; under-reserving clips a value the guide claims to print,
which is the defect being repaired. An estimator tuned for tightness would
reintroduce it at the first font or format change.

### 3. One shared geometry authority

Legend geometry — overall size, bar position, text origins — is computed once and
consumed by **both** `measure_legend` and the render path. The `250.0` at
`legend.rs:416` and the `- 70.0` at `:426` stop being independent declarations.

A measured size and a drawn size that can disagree is the same two-truths shape
PRODUCT-0084 and PRODUCT-0089 §1 each removed. Here they happen to agree; the
record removes the possibility, not a present symptom.

### 4. The longest label is an endpoint, and that is why endpoints suffice

The estimator inspects the two endpoint strings, not every tick. Under `{:.1}`
formatting a label's length is `sign + integer digits + 2`, both maximized at an
extreme of the domain: the most negative value carries the sign, the largest
magnitude carries the digits. So `max(len(low), len(high))` bounds every
intermediate label the path formats (`:499`, `:659`).

**This holds because the format is fixed-precision.** It would fail under
adaptive precision or thousands separators, which is why it is written down as a
dependency rather than assumed — see the second stop condition.

### 5. Both paths, and three fixture classes

Covered:

- the **single-gradient** path (`measure_legend` → `:509`), which heatmap uses;
- the **Gradient+SizeSample** path (`:310-314`, `:416`), which bubble will use at
  step 9.

Fixtures, each asserted on both paths:

| class | example | why |
|---|---|---|
| negative | `-30.0` | the live defect; **banked at the current 60 px as the failing control** |
| large magnitude | `123456.0` | width from digits rather than sign |
| short | `5.0` | the estimator must not over-reserve so bluntly that a narrow guide becomes absurd |

The negative case is banked *failing* first. A repair whose red state was never
captured cannot show that it repaired anything.

### 6. No `PlotBackend` measurement capability

This record adds none, and `common.rs:577`'s exclusion stands.

Adding one would be a **backend gap** under PRODUCT-0057's classifier, not an
assertion gap: two renderers can legitimately disagree about glyph extents, so it
could not be discharged with SVG evidence and would need the production renderer.
That is a real architectural decision and does not belong inside a defect repair.

### 7. Backend measurement is deferred, with a trigger

The deferred item's trigger is **the first product requirement for tight fitting
rather than safe non-clipping** — a constrained layout where the reserved gutter
must equal the rendered width, not merely bound it.

Until then §2's asymmetry is sufficient, and "the legend looks a little wide" is
not the trigger.

### 8. What this record does not do

No new legend sections, no repositioning, no font changes, no `max_legend_rows`
work. No change to what the guide *prints* — PRODUCT-0087 decided that; this
record only ensures the reader can see it. No entry-section measurement: the
`150.0`-per-column constant is untouched, since entries clip differently and no
fixture demonstrates a defect there.

## Implementation sequence

**Stage 0 — bank the red state.** The `-30.0` heatmap guide at the current 60 px,
on both paths, as the failing control. Plus every currently-correct legend, which
must not move.

**Stage 1 — the estimator.** §1 and §2, with no consumer.

**Stage 2 — the shared authority.** §3, with the existing constants preserved as
its outputs, so this stage moves no pixel.

**Stage 3 — content-aware widths.** The authority consumes the estimator. The
banked red state turns green here and nowhere earlier.

## Acceptance gates

1. **The banked clip is repaired.** The `-30.0` endpoint renders complete, against
   Stage 0's clipped control, on the single-gradient path.
2. **The same repair holds on the two-section path.** Gradient+SizeSample with a
   negative endpoint renders complete. Both paths, since they had independent
   geometry.
3. **Large magnitudes fit.** `123456.0` renders complete on both paths — width
   driven by digits, not by the sign that gate 1 exercises.
4. **Short values do not over-reserve absurdly.** A `5.0`–`9.0` domain reserves
   less than the `123456.0` case. The estimator must respond to content, not
   return one safe maximum for everything, which would pass gates 1–3 while being
   no more content-aware than `60.0`.
5. **Reserved width never falls below rendered width.** For every declared
   fixture, the estimate is greater than or equal to the true rendered extent.
   Over-reserving passes; under-reserving fails. §2's asymmetry, asserted.
6. **Correct legends are unmoved.** Every banked legend that did not clip is
   byte-identical to Stage 0, or differs only in reserved width with all text and
   bar positions consistent.
7. **One geometry authority.** `measure_legend` and the render path derive size,
   bar position, and text origins from the same computation. Asserted
   structurally: no literal legend width or bar offset survives in the render
   path.
8. **Measurement and drawing agree by construction.** For a set of contents
   spanning both paths, the size `measure_legend` reports equals the extent the
   render path occupies. This is what gate 7 buys, and it fails today only by
   luck.
9. **The font size is honoured.** A figure with a non-default `legend_size`
   reserves proportionally more or less. Without this the estimator could ignore
   `settings` entirely and pass every fixed-size fixture.
10. **No backend measurement capability is added.** `PlotBackend` gains no glyph
    or text-extent method — asserted structurally, so §6 cannot erode into "just
    a small helper."
11. **Endpoint bounding holds on a mixed domain.** On `-30…40`, no intermediate
    label formats wider than the widest endpoint, per §4. A fixture rather than an
    argument, so a future format change fails here rather than in the field.

## Stop conditions

> If a conservative estimate cannot keep gate 4 honest — if the only estimate that
> never under-reserves is a constant wide enough for anything — stop. That result
> means the estimate is not content-aware at all, and the case for §6's deferred
> backend measurement has arrived earlier than expected.

> If the endpoint formatting changes to adaptive precision, thousands separators,
> or units, stop. §4's bound assumes fixed `{:.1}` precision, and under adaptive
> formatting an intermediate label can exceed both endpoints.

> If unifying the two geometry authorities moves a pixel in Stage 2, stop. That
> stage is defined as behaviour-preserving; a change there means the two
> authorities already disagreed and the divergence is a separate finding to record
> before it is erased.

## Supersession

Supersedes no record. Repairs a defect in the guide PRODUCT-0087 built, without
changing what that guide prints. Precedes the mark-field transport refactor and
the bubble surface, which would otherwise inherit the defect through the
Gradient+SizeSample path. Opens a deferred backend-measurement item with the
trigger in §7.

## Rejected

**Increase `60.0` to a larger constant.** Attractive because it is one character
and fixes the screenshot. Rejected because it relocates the threshold instead of
removing it: the next wider label clips, and nothing records what "wide enough"
means. It would also leave the two geometry authorities intact.

**Add glyph measurement to `PlotBackend`.** Attractive because it is correct
rather than conservative. Rejected here as scope: it is a backend gap under
PRODUCT-0057, needing production-renderer evidence, and `common.rs:577`
deliberately excluded it. Deferred with §7's trigger rather than refused
outright.

**Truncate or ellipsize the label instead of widening the gutter.** Attractive
because the layout stays fixed. Rejected because it violates PRODUCT-0087's rule
directly — an ellipsized endpoint is a value the section prints but does not
name, which is the defect 0087 repaired.

**Measure every tick label rather than the endpoints.** Attractive because it
needs no argument about which is widest. Rejected because §4's bound is sound
under fixed precision and the second stop condition catches the case that would
break it; measuring all fifty would spend work to avoid writing down one
assumption.

**Keep `measure_legend` and the render path separate but assert they agree.**
Attractive as a smaller change than unifying them. Rejected because a gate that
compares two authorities is exactly what "a gate verifies a decision; it cannot be
the only place making one" warns against. Gate 8 asserts agreement *because* §3
makes it structural, not instead of.

## Consequences

The gradient guide stops clipping values it prints, and the defect cannot recur
by relocation: the reserved width now derives from the string being rendered.

Legend geometry gains a single authority. The `250.0` duplicated between
`measure_legend` and the render path — agreeing today by coincidence — becomes one
computation, which matters more at step 9, when bubble makes the two-section path
a live user surface for the first time.

The estimate remains an estimate. Polariton reserves slightly more legend width
than it strictly needs, which is the deliberate price of never clipping, and the
deferred backend-measurement item names the condition under which that price
stops being acceptable.

PRODUCT-0092's signed heatmap domain found this. A guide that had only ever
displayed non-negative values had never rendered a label wide enough to clip —
another defect surfaced by the first figure that could reach it, rather than by
the survey that looked directly at the code.
