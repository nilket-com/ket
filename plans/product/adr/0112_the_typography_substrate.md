# ADR 0112: the typography substrate

Status: proposed 2026-08-16. Seven text sizes already resolve through the
five-tier cascade, and every one of them is read by canvas code today — what
cannot be said is any single one of them. The **carrier** is the problem:
`PlotSettingsLayer` holds them as one whole-struct override with no Rune
spelling and no consumer outside `theme.rs`. This record makes **one** role
authorable — `hover_size` — to establish the partial-override pattern and the
generated-text spelling without touching a single layout reservation.

It follows PRODUCT-0110's facet gutter and precedes the typography layout
record, which exposes the seven canvas roles. The division is deliberate and
§5 states its boundary: this record proves the settings model end to end, and
**does not** prove delivery to a kernel.

## Context

### The seven sizes resolve today; they simply cannot be said

`PlotTypography` (`plot/theme.rs:103-111`) holds `title_size`, `subtitle_size`,
`x_label_size`, `y_label_size`, `x_tick_size`, `y_tick_size`, and
`legend_size`. `PlotSettingsLayer` already carries it at `theme.rs:205` as
`typography: Override<PlotTypography>`, and `apply_layer` flattens it into
`ResolvedPlotSettings` at `theme.rs:368-375`.

So resolution is finished work, and the *resolved* fields have many readers —
`radar.rs:128` takes `x_label_size`, `:161` takes `x_tick_size`, and the shared
chrome in `common.rs` reads all seven. What is missing is granularity and a
spelling. `Override<PlotTypography>` is **all or nothing**: authoring one size
means supplying all seven. And a census of `plot/` finds zero consumers of the
`typography` **slot** outside `theme.rs` — the carrier is staged but unspelled,
which is the permitted substrate posture and not a usable surface.

### The partial-slot precedent sits twelve lines above it

`theme.rs:193-196` splits the two continuous ramps out of the atomic theme with
the comment *"deliberately partial rather than reconstructed theme specs"* —
PRODUCT-0091 met exactly this problem. `theme.rs:353-354` states the resulting
precedence rule: **the specific slot wins over the atomic theme within one
tier.** `point_size` (`plot/spec/mod.rs:95`) supplies the scalar shape: a
private `f32`, a `new()` returning `Result`, a bounded range, and a typed error,
so that *"invalid and renderer-hostile sizes"* are unrepresentable after
authoring.

This record composes those two precedents. It invents no mechanism.

### Every canvas text role is reservation-coupled by construction

PRODUCT-0106 reserves the title band, the facet row, and the plot area **from
resolved font sizes**, precisely so a reservation tracks the text it must
clear. That is the correct design, and its consequence is that a canvas text
role which changes no reservation probably does not exist. A substrate record
whose first consumer were a canvas role would therefore have to make a geometry
decision — which is the one thing a substrate record must not do.

### Hover text is outside the canvas and outside layout

`gui/plot_pane/view.rs:36-49` pushes `container(text(info)).padding(6)` onto an
iced `stack!` above the two canvas layers. iced measures and sizes it; it
carries no explicit alignment; nothing in plot layout consumes its extent.
Kernels only ever return `Option<String>` (`plot/common.rs:995`) — no geometry
returns to the caller. It consumes **no** `PlotBackend` capability, so it
cannot drift toward PRODUCT-0093 §7's deferred text measurement.

The delivery path also already exists: `HostPlotState` carries
`plot_settings: ResolvedPlotSettings` (`gui/plot_pane/state.rs:33`) and the
view already reads it two lines below the hover container
(`view.rs:43`, `surrounding_background_color`). Only the field, the slot, and
the word are new.

### Stage 0's receiver census, stated here because it is load-bearing

The surface split in §1 rests on a count, not a preference. The authored figure
members in `rune_presentation.rs` are:

```text
__title        __subtitle        __x_label        __y_label        ← four receivers
__x_tick_step  __x_minor_ticks   __y_tick_step    __y_minor_ticks  ← tick configuration, no text
__legend_at    __hide_legend                                       ← placement and visibility, no text
```

Four text-bearing receivers for seven roles. Ticks expose only configuration,
so a `.font_size()` modifier would force an author to supply a tick step in
order to set a size. The legend exposes only placement and visibility, so
`legend_at(x, y).font_size(12)` would couple size to position and
`hide_legend().font_size(12)` is meaningless.

This census is recorded as evidence rather than as explanation, so that a later
consistency pass cannot "unify" the two spellings by inventing carrier members
or by attaching font size to tick placement.

## Decision

### 1. `hover_size(n)` is a word, because hover text is generated

The surface rule for all of typography:

> **`.font_size()` attaches to text the author supplied. A `*_size` word names
> text the system generates.**

```rune
title("Q3 Revenue").font_size(24)   // the author typed this string
hover_size(12)                      // derived from the data under the cursor
```

Hover content is composed from the row beneath the cursor, so it takes the word
form. This record introduces **only** the word form; the layout record
introduces the modifier form on its first authored-text consumer.

The rule is not a hybrid. It follows authorship, and it is the same joint the
existing member list already carves at — which is why exactly those four
receivers exist and the other three do not.

### 2. `HoverTextSize` is a bounded newtype with stated bounds

Following `PointSize` (`spec/mod.rs:95`) exactly: a private `f32`, a
`new(points: f32) -> Result<Self, HoverTextSizeError>`, and a typed error
carrying the rejected value. `Eq` is justified on the same grounds — the
constructor excludes NaN, so equality is reflexive over every representable
value.

The range is **inclusive `6.0 ..= 96.0` logical pixels**, and the numbers are
part of the contract rather than an implementation detail:

- **6.0** is the floor at which an antialiased glyph still resolves its strokes
  at ordinary scale. Below it the container's own `padding(6)` exceeds the text
  it wraps, so the overlay is no longer legible as text at any zoom.
- **96.0** is the ceiling at which one line plus padding is about 108 px — past
  the usable height of a small plot pane, so a larger value produces an overlay
  the pane cannot show.

Both are **work bounds, not taste bounds**, on PRODUCT-0041's precedent: they
exclude sizes the renderer cannot usefully produce, not sizes that would look
bad. A valid-but-unattractive hover size renders. Both endpoints are admitted;
gate 10 pins the boundary in both directions, because a bound stated only as
"out of range refuses" is satisfied by any implementation that refuses
something.

### 3. The type survives every carrier; `f32` appears only at the view

`HoverTextSize` is the field type in **all three** carriers — the
`PlotTypography` field, the `Override` slot, and `ResolvedPlotSettings` — with
a `points()` accessor, and conversion to `iced::Pixels` happening only at the
view. `ResolvedPlotSettings` already mixes typed and raw fields this way:
`point_size: PointSize` sits among the raw `f32` sizes (`common.rs:498`).

Without this the newtype would guard only the Rune door. A theme or an
application-tier default could still inject a raw `f32` — a negative size, a
NaN, a 400 — through a carrier that never validates, and the resolved value
would be invalid despite every authored path being checked. That is the
two-authorities shape: one fact with two sources that disagree only where
nothing tests them.

The seven existing sizes stay raw `f32` in this cut. The asymmetry is
deliberate and temporary: typing them is the layout record's work, done
alongside the reservation each one moves, and doing it here would touch seven
canvas roles in a record that claims to touch none.

### 4. The slot is partial, and the atomic carrier keeps the role

Two changes, mirroring the ramps:

- `PlotTypography` gains an eighth field, so a theme can carry a hover size
  with the other seven;
- `PlotSettingsLayer` gains `hover_size: Override<HoverTextSize>`, resolved
  into a new `ResolvedPlotSettings::hover_size`.

Within one tier the specific slot wins over the atomic `typography` override,
which is the rule `theme.rs:353-354` already states for
`sequential_ramp`/`diverging_ramp`. Across tiers the ordinary five-tier
precedence applies unchanged.

Both halves matter. A partial slot that silently replaced the atomic carrier
would make the layout record's seven roles unreachable from a theme; an atomic
carrier without a partial slot is the present defect.

### 5. Delivery is to the GUI, and this record does not claim more

The resolved value is consumed at `view.rs:38` as the hover container's text
size. **No `PlotBackend` method is added and no kernel reads the field.**

The boundary is stated rather than implied: this record proves the spelling,
the cascade, the resolution, and delivery to a rendered consumer. It does
**not** prove that a typography setting reaches a plot kernel. The layout
record proves that, for its first canvas role, together with the reservation
delta that role moves.

### 6. No reservation moves, and that is the point

The hover container is an overlay in an iced `stack!` with no explicit
alignment. Changing its size resizes the container and displaces nothing —
neither the canvas beneath it nor any band PRODUCT-0106 reserves. The canvas
draw list is unchanged at every hover size, which gate 7 pins.

### 7. Nothing is persisted, and the session schema is untouched

An authored `hover_size(12)` persists the only way authored presentation
persists: as Rune source in the buffer that produced the figure. The session
schema carries `DurableLayer { ordinal, family }`
(`session_persistence.rs:453-456`) and no plot settings at all, so no
serialization changes.

The application tier remains a user preference rather than document meaning,
matching PRODUCT-0110 §2's split for the facet gutter.

## Implementation sequence

**Stage 0 — measure the default, and settle how the widget is observed.** Two
outputs, and the second decides the shape of gates 2 and 3.

*The default.* The container inherits iced's default text size rather than
naming one, so **measure what it resolves to today** and set the library
default to exactly that. A plausible-looking constant that merely resembles it
would change every existing figure's hover text.

*The observation path.* The plot oracles capture canvas draw lists, and the
hover overlay is an iced widget outside them, so there is no existing way to
observe its realized size. Stage 0 must determine which of these holds and say
so in the evidence:

- **Widget observation is available** — PRODUCT-0057's headless capture can
  render the pane's whole widget tree, not only its canvas. Then gates 2 and 3
  observe the rendered overlay directly, which is the stronger form.
- **Widget observation is not available** — then the hover overlay's size is
  produced by one named helper taking `ResolvedPlotSettings` and returning the
  size the container is built with, and gates 2 and 3 assert that helper's
  output plus the structural fact that the overlay is constructed only through
  it. This is a routing contract of the PRODUCT-0110 §3 shape — one authority
  for one fact — not a source-text check for a literal.

Choosing between them is Stage 0's job precisely because guessing wrong
produces the unsatisfiable capture gate this record has already had to remove
once. Re-run the receiver census and bank it with the record.

**Stage 1 — the type and the slot, unspelled.** `HoverTextSize`, the eighth
`PlotTypography` field, the `Override` slot, the resolved field, and the
precedence wiring. No Rune word yet; substrate staging is permitted.

**Stage 2 — the word and the consumer.** `hover_size(n)` in the envelope, and
`view.rs:38` reading the resolved value.

## Acceptance gates

1. **The default is measured, not guessed.** The library default equals the
   value banked in Stage 0. Asserted against the banked constant, so a
   hand-picked default that merely looks right fails.
2. **An unauthored figure's hover text is unchanged.** With no `hover_size`,
   the resolved value is the library default **and** that value is what the
   overlay is built with — observed by whichever path Stage 0 established.
3. **An authored size reaches the overlay.** `hover_size(20)` resolves to 20
   **and** the overlay is built at 20, again through Stage 0's path. Both
   halves are required: resolution alone would pass while the view still
   ignored the field, which is the only interesting way this can be wrong.
4. **The specific slot beats the atomic carrier within one tier.** A single
   tier setting `typography` with one hover size *and* `hover_size` with
   another resolves to the `hover_size` value — PRODUCT-0091's rule at
   `theme.rs:353-354`, now exercised by a second family.
5. **The atomic carrier still delivers the role.** A tier setting only
   `typography` resolves its hover size. This is the half that a partial slot
   implemented as a replacement would silently break.
6. **Five-tier precedence holds for the new slot.** Set at all five tiers, then
   cleared downward, matching the existing `point_size` cascade test
   (`theme.rs:730-745`).
7. **The canvas is byte-identical at every hover size.** Render the same figure
   at the default and at a large authored size; the canvas draw lists match
   exactly. This is §6's non-claim made falsifiable, and it is the gate that
   goes red the day hover size reaches a reservation.
8. **No backend capability and no kernel consumer.** Structural: `PlotBackend`
   gains no method, and no kernel reads `hover_size`. Per §5, so the substrate
   cannot quietly become the first canvas role.
9. **No session-schema change.** `DurableLayer` is unchanged and no plot
   setting enters the payload. Per §7.
10. **The bounds are pinned at their endpoints.** `hover_size(6)` and
    `hover_size(96)` are **accepted**; near-miss values outside them — 5.9 and
    96.1 — are **refused**. Near-misses rather than the nearest representable
    neighbours, because a Rune literal is `f64` and narrowing
    `6.0f32::next_down()` through it can round back to 6.0, which would fail a
    correct implementation. Plus non-finite and wrong-typed values,
    all refusing at the `hover_size(...)` span and carrying the rejected value
    in the `PointSizeError` shape. Endpoint pairs are required because "out of
    range refuses" is satisfied by an implementation that refuses anything,
    and an exclusive bound would fail §2's stated inclusive contract.
11. **The type survives every carrier.** Structural, per §3: the
    `PlotTypography` field, the `Override` slot, and the `ResolvedPlotSettings`
    field are all `HoverTextSize`, and no code path assigns a hover size from a
    raw `f32`. The `iced::Pixels` conversion appears once, at the view. Without
    this, a theme or application default could inject an unvalidated size that
    every authored-path gate would still pass.
12. **The authored value round-trips as source.** Re-executing the buffer
    reproduces the authored size. This is the whole of the persistence claim;
    §7 states there is no other mechanism to prove.
13. **The word is the only new spelling.** Structural: no `.font_size()`
    modifier and no `font_size(role, n)` form is added by this record. The
    modifier form belongs to the layout record and its first authored-text
    receiver.

## Stop conditions

> If hover placement, sizing, or any figure layout turns out to consume the
> hover extent, stop. The off-canvas premise in §6 is false, the seam between
> this record and the layout record has collapsed, and hover is no longer a
> reservation-free first consumer.

> If the library default cannot be made to reproduce today's rendering, stop
> and record why before choosing a number. A substrate record is not entitled
> to change how every existing figure's hover text looks as a side effect.

> If the partial slot cannot coexist with the atomic carrier — if delivering
> one requires reconstructing the whole `PlotTypography` — stop.
> PRODUCT-0091's pattern did not transfer, and the layout record's seven roles
> need a different carrier before they are worth exposing.

> If a canvas text role is found that changes no reservation, stop and
> reconsider. It would be a better first consumer than hover, because it would
> prove kernel delivery too, and §5's boundary could then be narrower.

## Supersession

Supersedes no record. Narrows nothing. Leaves PRODUCT-0093 §7's
backend-measurement deferral standing with its trigger intact — §5's structural
gate makes it unreachable from this record. Extends PRODUCT-0091's
partial-slot precedent to a second setting family, and applies PRODUCT-0017's
bounded-newtype shape to a second scalar.

## Rejected

**`font_size("hover", 12)`.** Attractive because one word would cover all eight
roles at once. Rejected because it turns a semantic member into a typo-prone
string, weakens span attribution to a string literal rather than the role, and
hides which layout coupling an edit is changing. Distinct roles get distinct
spellings.

**`.font_size()` on hover.** Attractive for consistency with the layout
record's four modifiers. Rejected because hover has no receiver: the text is
composed from the row under the cursor, not supplied by the author, so there is
no authored member to attach to. Inventing one purely to carry a modifier is
the "meaningless carrier" the Stage 0 census exists to prevent.

**Expose `Override<PlotTypography>` as the surface.** Attractive because the
slot already exists and needs no new field. Rejected because authoring one size
would mean supplying all eight, so every figure that wanted a larger hover
would silently pin seven other roles against future default changes.

**Start with a canvas role.** Attractive because it would prove kernel delivery
in the same record. Rejected because every canvas text role is
reservation-coupled by PRODUCT-0106's construction, so the substrate would have
to make a geometry decision to exist — bundling the two records rather than
sequencing them.

**Add all eight roles now.** Attractive because the mechanism is identical for
each. Rejected because seven of them move reservations, and the reservation
delta is exactly what the layout record must prove role by role. Spellings are
earned by their consumers, one at a time.

## Consequences

Typography becomes authorable at one role, and the cheapest possible one: the
whole path from Rune word to rendered pixel is exercised while the canvas
provably does not move.

The cost is an explicit gap, stated in §5 rather than discovered later. Nothing
here proves a typography setting reaches a kernel, because no reservation-free
canvas role exists to prove it with. That gap is the layout record's first
obligation, not a deferral this record is entitled to leave unnamed.

The receiver census also fixes a fact that would otherwise have surfaced as an
implementation blocker three-quarters of the way through the layout record —
no receiver for ticks, no receiver for the legend, and a half-built modifier
surface already committed. Counting the receivers before choosing the spelling
cost one grep; discovering it during implementation would have cost a partly
shipped surface.
