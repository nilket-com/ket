# ADR 0087: honest gradient guide stops

Status: proposed 2026-08-12. The gradient legend misrepresents every multi-stop
ramp it describes. This record replaces its two endpoint colours with an ordered
sequence of `(data_value, colour)` stops and makes rendering interpolate between
adjacent supplied stops. It is an **honesty repair**: the guide comes to report
exactly what the marks already do. It does not change what the marks do, and it
does not make diverging ramps correct.

## Context

`LegendSection::Gradient` carries two colour stops (`plot/legend.rs:38-44`) and
the swatch is painted by `lerp(*low_color, *high_color, t)` (`:587`) — a single
linear blend between endpoints. The marks sample the full ramp: many
`ColorTheme` entries are `map_color(t, &[(f32, f32, f32); N])`
(`plot/colors.rs:219-230`), which spaces `N` stops evenly at `i/(N-1)` and
interpolates piecewise between them — viridis has 5 stops
(`colors.rs:232-243`), `green_purples` has 3. Others are procedural, linear, or
constant rather than array-backed; §2 covers that heterogeneity, which an earlier
draft missed.

The two disagree everywhere except the endpoints. Measured:

| ramp | marks at `t = 0.5` | legend at `t = 0.5` | |
|---|---|---|---|
| viridis | `(33, 145, 141)` teal | `(161, 116, 60)` olive | RGB distance **154** |
| `green_purples` | `(255, 255, 255)` white | `(64, 70, 76)` dark grey | neutral **destroyed** |

The diverging row is the headline. A diverging palette's entire semantic is a
neutral midpoint; the legend renders that midpoint dark grey, so the guide does
not merely mismatch — it erases the property that makes the palette diverging.

This also violates PRODUCT-0043's own rule that a legend section carries **every
value it prints**. This section prints colours it never names, and the renderer
invents them.

**Three** production producers exist, one outside the catalog restoration list:
`kernels/bubble.rs:97`, `kernels/heatmap.rs:94`, and `kernels/hexbin.rs:84`. The
two `plot/factory.rs` sites (`:1675`, `:1703`) are inside the `#[cfg(test)]`
module opened at `:521` — they are **oracles, not producers**, and an earlier
draft counted them as five.

## Decision

### 1. A gradient carries ordered `(data_value, colour)` stops

```rust
Gradient {
    label: String,
    stops: GradientStops,   // validated; see below
}

struct GradientStop {
    value: f64,     // a data-domain value, not a palette coordinate
    color: Color,
}
```

`GradientStops` is a **public opaque type with private fields**, not a bare
`NonEmpty` and not a private type — it appears inside the public
`LegendSection`, so a private type would leak or trip private-interface
diagnostics. Construction and element access are crate-scoped, so an external
struct literal cannot bypass validation; gate 7 asserts that with a compile-fail
fixture.

It is a validated sequence.
`NonEmpty` guarantees one element and nothing else — it admits a single stop, a
`NaN` value, and a descending list, all of which a renderer walking neighbours
would have to handle at paint time. The constructor requires **at least two**
stops, all values finite, and values **strictly ascending**. A constant ramp is
expressed as two distinct positions carrying identical colours, so "no variation"
remains representable without weakening the ordering rule.

`low`, `high`, `low_color`, and `high_color` are removed. The endpoints are the
first and last stops, so nothing is lost and one representation replaces two.

**Positions are data-domain values, not normalized palette coordinates.**
Normalized positions cannot honestly place a neutral midpoint: on a `-50…100`
domain, palette position `0.5` is data value `25`, and a legend carrying `0.5`
would require every reader to recover the domain separately in order to know
what the colour means. Carrying `25` says it directly.

### 2. Producers project palette positions through their own domain

A producer builds its stops by taking the ramp's canonical positions and mapping
each through the domain it already uses for its marks:

```text
for each canonical palette position p in 0..N:
    value = domain.lo + p * (domain.hi - domain.lo)
    color = ramp.color_at(p)
```

**`ColorTheme` does not uniformly have stop arrays to expose**, and an earlier
draft claimed it did. Only some variants are `map_color` over an array. `rainbow`
is procedural (`colors.rs:178-190`), `greys(t)` is `Color::from_rgb(t, t, t)`
(`:195`), `black(_t)` is a constant (`:203`), and every `*_rev` delegates. An
accessor that merely surfaced existing arrays would cover a fraction of
`ColorTheme::ALL`'s 37 members.

**Canonical stops therefore become the single authority, and `get_color` samples
them.** Every variant declares its stops; `get_color(t)` is defined as
interpolation over that declaration. Inverting the relationship this way is what
prevents drift: two independent definitions of one ramp will eventually
disagree, and the guide is precisely where that disagreement becomes visible.

Each current shape converts losslessly — a linear ramp is two stops, a constant
is two stops of one colour, a reversed ramp is its base reversed, and `rainbow`'s
**five** linear segments are represented by **six** stops. Gate 3 proves that per
variant rather than assuming it.

The domain is each producer's **existing** one, unchanged — heatmap's
`0..max_val`, bubble's `color_range`. §5 is explicit that this record does not
revisit either.

**A zero-width domain omits the gradient section.** All three producers can
reach one: bubble when `color_min == color_max`, heatmap and hexbin when
`max_val` is `0`. Projecting canonical positions through such a domain yields
equal data values, which §1's strictly-ascending rule refuses — so the outcome
must be pinned rather than left to a constructor failure.

The section is **omitted**, through **one shared constructor** returning
`Option<LegendSection>` that all three producers call. A per-producer guard would
be three chances to disagree about the same rule.

The same constructor pins two further shapes.

**A descending domain panics**, naming PRODUCT-0087. `low > high` is not a guide
that disambiguates nothing — every current producer derives an ordered range, so
it is an invariant violation, and omitting the section would hide a caller bug
behind a policy. Gate 5 pairs it with an ascending twin so the constructor cannot
pass by panicking universally.

**A non-finite domain omits.** A `NaN` or infinite bound also yields `None`. `legend_content` returns `Option<LegendContent>` and cannot
propagate a `GradientStops` construction error, so an unpinned non-finite domain
would otherwise force a panic or a silently malformed section at paint time.

**This extends PRODUCT-0044, and the record says so rather than claiming
otherwise.** `apply_policy` today removes only a lone synthetic `Entries` section
and its doc comment calls that "deliberately the only omission decision"
(`legend.rs:73`). A gradient omitted for a degenerate domain is a **second**
omission decision. It is the same principle — a guide that disambiguates nothing
is omitted — but PRODUCT-0044's sentence becomes false unless amended, so §6 and
Supersession record the extension instead of asserting 0044 is untouched.

Bubble's `SizeSample` section is unaffected and still published when present, so
a degenerate colour range does not silently remove a size guide that still says
something.

### 3. Rendering interpolates between adjacent supplied stops

The swatch walks the supplied stops and interpolates between neighbours. It
**never** reconstructs a ramp from the first and last stop, and it never samples
`ColorTheme` directly.

**There are two gradient render paths, and both route through one helper.**
Bubble's paired gradient-plus-size layout (`legend.rs:401`) is separate from the
ordinary section path (`:569`), and each destructures the endpoint fields today.
A single stop-sampling helper serves both. Without it bubble could keep endpoint
interpolation while the ordinary renderer passes every midpoint gate — so gate 2 is
asserted **on both paths**.

Fidelity is then **exact as a model**: the marks and the guide evaluate the same
piecewise-linear function over the same declared stops, rather than two functions
that happen to be close. The legend does not approximate the marks.

Realized `f32` results are a separate question. Identical mathematics evaluated
through different code paths can differ in the last bits, so gate 11 pins a small
arithmetic budget stated in ULPs. That is not the same as a tolerance: an RGB
distance chosen because it makes a test pass would conceal the present defect at
lower amplitude, which stop condition 2 refuses.

This is what makes the guide honest by construction rather than by tuning. A
renderer that reached for `ColorTheme` itself would be a second sampler that
could drift from the marks, which is the defect one level over.

### 4. All three producers migrate, hexbin included

`bubble.rs:97`, `heatmap.rs:94`, and `hexbin.rs:84`. Hexbin is not in the
catalog-restoration list and is repaired anyway: it publishes the same
misrepresenting section, and leaving one producer on the old shape would keep the
two-endpoint form alive to be copied. The two `factory.rs` test sites are
updated as oracles.

Heatmap's hardcoded `label: "Value"` and `low: 0.0` are carried across
unchanged, and the resulting limit is banked rather than glossed. Because
`map_color` clamps `t` to `0..1` (`colors.rs:220`), a negative cell already
paints the **first colour**, indistinguishable from zero — and the guide's domain
starts at `0.0`, so those observations are absent from it entirely.

So B1 makes the swatch faithful **within heatmap's declared `0..max` scale**. It
does not make a signed heatmap's guide honest about its observations: negatives
still clamp, and still fall outside the reported domain. Both halves are **B2's**
substrate decision.

### 5. Mark normalization is deliberately unchanged

This record touches no mark colour. Every kernel keeps its current
normalization, and in particular:

> On a `-50…100` domain, a diverging ramp's neutral colour continues to land on
> data value **25**, in the marks and now truthfully in the legend too.

That is not a defect this record introduces or hides — it is the existing
behaviour, and after this record the guide **reports it accurately**. Zero
anchoring is owned downstream and split: **B2** takes heatmap's two-ended signed
domain; **B3** takes diverging semantics for bubble and for authoring generally.
B2 cannot change bubble's normalization.

The distinction is the record's point. Making the guide faithful to the marks is
not making the marks correct, and "honest gradient" must not be read as
"diverging ramps now work."

### 6. What does not change

No mark colour, no palette, no `ColorTheme` variant, no Rune surface, no
persistence, no legend **placement or merging** policy, and no
`LegendSection::Entries` or `SizeSample`. PRODUCT-0044's **omission** policy is
extended by §2's degenerate-domain rule and is the one part of it this record
touches. Gradient legend sizing keeps its
current fixed allotment; stop count does not change layout.

## Implementation sequence

**Stage 0 — bank the misrepresentation, through the right oracle for each
claim.**

*Through the old sampler helper*, at exact `t = 0.5`: `green_purples` legend
`(64, 70, 76)` against marks `(255, 255, 255)`, and viridis `(161, 116, 60)`
against `(33, 145, 141)`. The exact midpoint is a **sampler** fact and can only
be banked there.

*Through the production renderer*, only the two **central operations** at
`i = 24` and `i = 25` on both paths — `(63, 71, 76)` and `(64, 69, 77)` for
`green_purples`. The renderer steps at `t = i / 49` and never evaluates `0.5`, so
banking an exact midpoint through it is not possible.

Also bank, in the same stage, **gate 3's complete 37-theme bit oracle**: the
`Color` component bits of every `ColorTheme::ALL` member at every breakpoint and
one interior point per segment. This must exist **before Stage 1 changes
`get_color`**; calling it "banked" inside a gate does not sequence it, and once
`get_color` is stop-derived the pre-repair values are unrecoverable.

These are the negative controls, and they exist today — the defect is live, so no
synthetic red state is needed.

**Stage 1 — expose canonical stops.** A `ColorTheme` accessor returning the
ramp's positions, with no consumer changed.

**Stage 2 — the content model and renderer.** `GradientStop`, the section
change, and adjacent-stop interpolation.

**Stage 3 — the three producers**, with the two `factory.rs` oracles updated.

## Acceptance gates

1. **The sampler is correct at the exact midpoint.** A **unit** oracle over the
   stop-sampling helper: `green_purples` yields `(255, 255, 255)` at `t = 0.5`
   and viridis yields `(33, 145, 141)`, against banked endpoint-lerp values of
   `(64, 70, 76)` and `(161, 116, 60)`.

   This is a sampler gate, not a rendered one. **Neither render path ever
   evaluates `t = 0.5`**: both draw 50 steps at `t = i / 49`
   (`legend.rs:429`, `:580`), so `i = 24` gives `0.4898` and `i = 25` gives
   `0.5102`. An earlier draft asserted the rendered swatch contained the exact
   midpoint colour, which it cannot.
2. **The two central rendered operations change, on both paths.** At `i = 24` and
   `i = 25`, each of the bubble (`:428`) and ordinary (`:579`) paths emits a
   colour equal to `get_color(t)` rather than the banked endpoint interpolation.
   For `green_purples` that is `(251, 252, 250)` and `(252, 250, 252)` against
   banked `(63, 71, 76)` and `(64, 69, 77)` — the neutral returns at the
   positions the renderer actually paints.
3. **Every `ColorTheme` member converts bit-exactly.** For all 37 entries of
   `ColorTheme::ALL`, the stop-derived `get_color` reproduces the pre-repair
   `Color` components **bit for bit** — `to_bits()` equality, not a tolerance —
   over a banked set containing **every old breakpoint plus at least one interior
   point per segment**, banked before the inversion.

   Bit-exactness is required here and nowhere else: gate 15 promises mark
   operations are byte-identical, and marks call this function directly. The ULP
   allowance of gate 11 belongs solely to the realized legend, where a data-domain
   projection adds arithmetic the marks never perform. An arbitrary count of
   evenly spaced samples could also miss a breakpoint entirely, which is exactly
   where a translation error would sit. The array-backed
   variants are the easy ones; the gate exists for `rainbow` (procedural),
   `greys` (linear), `black` (constant), and the `*_rev` delegates, where a
   stop declaration is a **translation** rather than an extraction and could
   silently alter a ramp.
4. **A zero-width domain omits the gradient, per producer.** Bubble with
   `color_min == color_max`, heatmap with `max_val == 0`, and hexbin with
   `max_val == 0` each publish **no** `Gradient` section — asserted for all
   three, since one producer's guard proves nothing about the others' domains.
   Bubble's `SizeSample` section is still published in that case, so the omission
   is scoped to the section that disambiguates nothing.
5. **A descending domain panics, naming this record.** Per §2, the shared
   constructor panics naming PRODUCT-0087 for `low > high`, and an **ascending
   valid twin** proves it does not panic universally.
6. **Non-finite domains omit too.** A producer whose domain bound is `NaN` or
   infinite publishes no `Gradient`, through the same shared constructor as the
   zero-width case. Asserted because `legend_content` returns `Option` and cannot
   propagate a construction error — an unpinned non-finite domain would surface
   as a panic or a malformed section at paint time.
7. **`GradientStops` cannot be built or read from outside the crate.** An
   external struct literal and an external field read each fail to compile,
   asserted by trybuild fixtures with banked stderr per PRODUCT-0049's
   binary-only finding, with an in-crate should-pass twin. The type is public
   because it sits inside the public `LegendSection`; only its construction is
   closed.
8. **The stop sequence cannot hold an invalid shape.** `GradientStops` refuses a
   single stop, a non-finite value, and a descending or equal-valued pair;
   a two-stop constant ramp with identical colours is **accepted**. Asserted at
   construction, so no consumer handles these at paint time.
9. **The fixture has an odd stop count.** Both ramps above have an odd number of
   stops, so `map_color` returns the middle stop exactly and the expected value
   is a constant rather than an interpolation. An even-stop ramp would make the
   midpoint itself interpolated and weaken the comparison.
10. **Endpoints are unchanged.** The first and last swatch colours equal the
   pre-repair ones. Endpoints agreed under the old implementation, so this gate
   proves the repair did not disturb what already worked — and its absence would
   let a wholesale change pass gates 1 and 2.
11. **Legend and marks agree at every rendered position, on both paths.** The
   oracle is the renderer's own sampling: the **50** swatch steps it already
   draws at `t = i / 49` (`legend.rs:428` on the bubble path, `:579` on the
   ordinary path). At each of those 50 positions, on **each** path, the swatch
   colour equals `ramp.get_color(t)` within **4 ULP per `f32` channel**.

   The fixture domain is **`10..50`**, not `0..1`. A unit domain makes the
   data-domain projection an identity and cannot distinguish correct
   interpolation from an implementation that never projects at all.

   Both numbers are fixed here rather than at implementation. Fifty is not a
   chosen sample count — it is what the renderer does, so the gate cannot be
   satisfied by testing somewhere the renderer never paints. Four ULP is the
   round trip through the data-domain projection: one multiply-add out to a
   value, one subtract-divide back to a position. Exceeding it is stop
   condition 2, not a budget to raise.
12. **No endpoint reconstruction survives.** A structural guard that the legend
   render path contains no interpolation between a section's first and last stop
   only, and no `ColorTheme` sampling — a second sampler could drift from the
   marks.
13. **All three producers emit stops.** `bubble.rs`, `heatmap.rs`, and
   `hexbin.rs` each construct a `Gradient` with more than two stops for a
   multi-stop ramp; none constructs the removed endpoint form, which no longer
   exists. The two `factory.rs` oracles are updated as tests.
14. **Stops are data-domain values.** For a producer with domain `10..50` and a
   5-stop ramp, the stop values are `10, 20, 30, 40, 50` — not `0, 0.25, 0.5,
   0.75, 1`. A palette-coordinate implementation renders identically and fails
   only here.
15. **Mark colours are byte-identical.** Every kernel's emitted mark operations
    are unchanged against a pre-repair bank. This record's claim is that only
    the guide moves.
16. **Heatmap's signed limitation is banked for B2.** With a cell of `-10`
    against a positive `max_val`, the cell paints the ramp's **first colour**
    (`map_color` clamps at `colors.rs:220`), the guide's first stop is `0.0`, and
    `-10` therefore lies outside the reported domain. Asserted so B2 inherits a
    stated starting state rather than rediscovering it, and so this record cannot
    be read as having made signed heatmaps honest.
17. **Bubble's `-50…100` neutral stays at 25, banked for B3.** A **bubble**
    fixture with `color_range = (-50.0, 100.0)` under a diverging ramp places the
    neutral colour at data value 25 in both marks and legend, and the stop list
    says `25`. Named as bubble's rather than heatmap's: bubble's normalization is
    already two-ended and correct, so its zero anchoring belongs to **B3**
    (diverging semantics and authoring), while **B2** owns heatmap's domain and
    cannot change bubble's. This gate asserts the **deliberate non-fix** of §5.

## Stop conditions

> If declaring canonical stops requires changing any ramp's colours or spacing,
> stop. Some ramps are array-backed and some are piecewise functions —
> `rainbow`, `greys`, the constants, and the delegated reversals have no arrays
> to expose — but every one must translate **losslessly** into stops. A repair
> that alters a ramp is a palette change wearing a guide repair's clothes.

> If legend and marks cannot be made to agree — **identical as a model, and
> within the predeclared ULP budget once realized** — stop and report the
> residual. Approximate model agreement is the current defect at a smaller
> amplitude, and an RGB tolerance chosen to make a test pass would conceal it.
> A stated ULP budget for realized `f32` is not that tolerance.

> If a producer has no domain to project through, stop. Every current producer
> has one; a future one without a domain cannot honestly label a gradient and
> needs its own decision rather than a fabricated range.

## Supersession

Supersedes `LegendSection::Gradient`'s two-endpoint shape from PRODUCT-0043, and
restores that record's own rule that a section carries every value it prints.

**Extends PRODUCT-0044's omission policy.** That record's "deliberately the only
omission decision" (`legend.rs:73`) becomes false with §2's degenerate-domain
rule, so the sentence is amended to name both: a lone synthetic `Entries`
section, and a `Gradient` whose domain is zero-width or non-finite. Placement and
merging are untouched. No mark record is affected.

## Rejected

**Keep two endpoints and have the renderer sample `ColorTheme`.** Attractive
because the section stays small and the swatch would look right. Rejected
because it creates a second sampler that can drift from the marks, and because
the section would still print colours it does not name — PRODUCT-0043's rule
broken in a less visible way.

**Carry normalized palette positions with the colours.** Attractive as the
smaller change, since the ramp already thinks in `0..1`. Rejected because a
reader cannot place a neutral midpoint without separately recovering the domain:
on `-50…100`, `0.5` means data value `25`, and a guide that requires external
arithmetic to interpret is not a guide.

**Fix zero anchoring here too.** Attractive because the `-50…100` case is the
motivating example and it feels incomplete to leave it. Rejected because
anchoring changes **mark** colours, which makes it a substrate decision with its
own exhibit and its own parity subject — and bundling it would mean this record
could no longer claim that only the guide moves.

**Repair only the catalog producers and leave hexbin.** Attractive as a smaller
diff aligned to the restoration plan. Rejected because the old shape would
survive somewhere to be copied, and hexbin publishes the identical
misrepresentation today.

## Consequences

The gradient legend becomes exactly the function the marks evaluate, rather than
a straight line between its endpoints. For any multi-stop ramp this is a visible
change in every figure that publishes one — heatmap, bubble, and hexbin — and the
diverging case changes most, since its neutral returns.

The record deliberately leaves a wrong thing wrong: a diverging ramp on an
asymmetric domain still puts its neutral at the domain's midpoint rather than at
zero. After this record the guide says so plainly, which is what makes the
remaining defect legible enough for B2 and B3 to fix. That is the intended
sequence — an honest instrument first, then the measurement it reports on.
