# ADR 0113: the typography layout surface

Status: proposed 2026-08-16. PRODUCT-0112 made one typography role authorable
off-canvas and established the rule this record inherits: **a role is exposed
only when a reservation tracks its size.** Applying that rule to the seven
canvas roles exposes exactly two — `title` and `subtitle` — as
`.font_size(...)` modifiers on the members that already carry their text. It
repairs the parallel dimension header, and states why the other five are
blocked: four on a reservation that does not exist, and `legend_size` on a
substrate defect this record names rather than fixes.

It adds **no** new top-level Rune words. That is a narrower cut than "font
sizes on seven elements," and §3 gives the measurement behind the narrowing
rather than asserting it.

## Context

### Three roles reserve from their size; four draw into a constant

Every one of the seven resolved sizes has canvas consumers. Only three of them
have a reservation that grows when the size grows — and one of those three
turns out to be unusable for a different reason:

| role | reserves from its own size? | evidence |
|---|---|---|
| `title_size` | **yes** | `common.rs:1166` — `title_offset + title_size`, PRODUCT-0106 §1 |
| `subtitle_size` | **yes** | `common.rs:1171` — `subtitle_offset + subtitle_size` |
| `legend_size` | yes — but it is two roles, see §8 | `legend.rs:368` — `estimated_width(legend_size)` sets the gutter; **also** sizes facet labels, which is why it is not exposed |
| `x_label_size` | **no** | drawn at `plot_area.bottom + x_label_padding` (`common.rs:1525`), a constant `45.0` |
| `y_label_size` | **no** | drawn at `plot_area.x - y_label_padding` (`common.rs:1537`), a constant `85.0` |
| `x_tick_size` | **no** | drawn inside `plot_padding_bottom`, a constant `80.0` |
| `y_tick_size` | **no** | drawn inside `plot_padding_left`, a constant `100.0` |

The four unreserved roles sit in fixed bands sized for today's defaults. From
the constants alone the slack looks thin: the x band is `80.0` and the axis
title's own baseline is already `45.0` into it at size `20.0`, so roughly a
quarter of the band remains. Doubling `x_label_size` would put the axis title
past the bottom of the space that exists to hold it.

**These are derived numbers, not measured ones**, which is why Stage 0's first
job is to measure the real headroom per role rather than trust this arithmetic.

### `legend_size` is one field serving two unrelated roles

Its reservation does track it — `estimated_width(legend_size)` sets the legend
gutter. But `VerticalGuideBands::facet_label_size` is
`(settings.legend_size + 2.0).max(10.0)` (`common.rs:1194-1196`), and the facet
row height is `FACET_LABEL_TOP_INSET + facet_label_size` (`:1177`). So the
field also sizes every facet header and the row reserved for it.

This is not the two-authorities defect — it is its inverse: **one source for
two facts**. Exposing it would let an author resize facet headers by asking for
legend text, through a `+ 2.0 / max(10.0)` derivation no spelling mentions. The
two roles must be separated before either is authorable, which is substrate
work and not this record's.

### This is PRODUCT-0106's defect, on two more edges

PRODUCT-0106 found exactly this at the **top** — a band that was safe only by
convention, because nothing reserved from the text's size — and reserved it
from resolved values. The bottom and left edges have the same shape and have
not been repaired. Exposing `x_tick_size(36)` today would ship a collision on
purpose.

So the four remaining roles are not deferred on effort. They are blocked on a
bottom-and-left reservation record that is PRODUCT-0106's sibling, and which
will move every Cartesian figure's geometry the way 0106 did. Bundling it with
a surface record would repeat the mistake 0064's halt caught: a geometry repair
smuggled in beside a spelling.

### Modifiers exist on marks, not on figure elements

`RuneMark` (`rune_presentation.rs:209-215`) carries `modified: bool` plus one
`Option<Origin>` per modifier — `point_size_origin`, `stroke_width_origin`,
`bar_width_origin` — which is how PRODUCT-0018's register refuses a duplicate
at the second call's span. `RuneFigureElement` (`:193-196`) has no such
register; it carries only `value: Result<FigureElementKind, PresentationFailure>`
and an `origin`.

The mechanism therefore transplants rather than invents: figure elements gain
the register shape marks already have.

Because `RuneFigureElement` is one Rune type covering every member,
`.font_size(...)` is **spellable** on all of them — `skin("greys_rs2")
.font_size(12)` will parse. Applicability is a refusal, not an absence, which
is PRODUCT-0046 §1's spellable-forms situation and is decided in §5.

### Two hardcoded sizes remain in `plot/`

Of 48 `fill_text` sites, exactly two do not read a resolved size:
`common.rs:2410`, parallel coordinates' dimension header at `Pixels(22.0)` and
a hardcoded `top_px.y - 20.0` offset; and `funnel.rs:170`, the in-stage value
label at `Pixels(16.0)`, centred on both axes inside its trapezoid. §6 and §7
dispose of them in opposite directions, deliberately.

## Decision

### 1. `.font_size(n)` attaches to a member; a word exists only without one

The rule PRODUCT-0112 §1 stated, now with its first modifier consumers:

```rune
figure([
    title("Q3 Revenue").font_size(24),
    subtitle("by region").font_size(14),
    bar("quarter", "revenue"),
])
```

`title` and `subtitle` are strings the author supplied, so they take the
modifier — and both members already exist, so this record introduces no
top-level word at all.

**Top-level words are for roles with no member to attach to**, which is the
sharper form of PRODUCT-0112's authored/generated rule and the reason
`hover_size(n)` is a word: hover has no member and no plausible receiver.

The converse is narrower than it first sounds, and the Rejected section holds
the line: a role should take a modifier only where a **semantic** member
exists or is independently warranted — one that owns its concept, as `title`
owns the title. A member invented purely to hang a modifier on is a spelling
unearned by use, and adds namespace rather than saving it. `legend()` qualifies
only if it becomes the real legend member owning placement, visibility, and
typography together (§8); it does not qualify as a hook for one setting.

### 2. Exactly two roles are exposed in this cut

`title` and `subtitle`. Each already has a reservation that grows with it
(PRODUCT-0106 §1), and each already has a member carrying its text, so
authoring a larger size moves layout **correctly** and costs no namespace.

Five roles are **not** exposed, for two different reasons:

- `x_label`, `y_label`, `x_tick`, `y_tick` — nothing reserves space from their
  size (§3);
- `legend_size` — its reservation tracks it, but the field also sizes facet
  headers, so no honest spelling exists for it yet (§8).

The mechanism would work for all five. The block in each case is that the thing
being named is not yet the thing that would move.

### 3. The four remaining roles are blocked on a reservation, and Stage 0 says so numerically

Stage 0 produces a headroom measurement per unreserved role: at the default
size, how much of its containing band is unused. That table is the evidence for
the next record's scope, and it converts "these are blocked" from a claim into
a number.

The record that unblocks them is the bottom-and-left counterpart to
PRODUCT-0106 — one ordered authority computing the below-plot and left-of-plot
bands from resolved tick and label sizes, exactly as 0106 does for the title
band. It is not this record, and this record must not partially build it.

### 4. The two exposed sizes become typed; the five blocked ones stay `f32`

Following PRODUCT-0112 §3, each exposed role's size becomes a bounded newtype
through every carrier — `PlotTypography`, its partial `Override` slot, and
`ResolvedPlotSettings` — with `f32` appearing only at the `iced::Pixels`
conversion. Ranges are stated in the same work-bound form as `HoverTextSize`'s
`6.0 ..= 96.0`, and Stage 0 fixes each from the reservation that consumes it.

The five blocked roles keep raw `f32` until the record that unblocks each one
types it alongside. Typing a role whose size nothing reserves from — or whose
field secretly drives a second role — would protect the value while leaving the
geometry wrong, which is precision in the wrong place.

### 5. `.font_size(...)` on a non-text member refuses at the modifier's span

The modifier is spellable on every `RuneFigureElement` because they share one
Rune type. `skin("greys_rs2").font_size(12)` therefore refuses, attributed to
the `.font_size(...)` call rather than to `skin(...)`, since the member is
valid and the modifier is what cannot apply.

A duplicate refuses at the second call's span, per the register shape
`RuneMark` already uses. Two spans, two refusals, both naming the modifier —
which is the attribution the `font_size("title", 24)` form would have lost.

### 6. Parallel's dimension header routes through `x_label_size` — size **and** offset

`common.rs:2410`'s `Pixels(22.0)` becomes the resolved `x_label_size`, and its
hardcoded `top_px.y - 20.0` offset derives from that same resolved value.

Both halves, because routing the size alone reproduces the two-authorities
shape one level down: the glyphs would grow while their distance from the axis
stayed fixed, so PRODUCT-0106's band would be right about the text and wrong
about the gap it must clear.

This is a **substrate repair, not an authoring change**. `x_label_size` is not
exposed by §2, so the header simply begins tracking the theme-supplied value it
should always have used.

The consequence to state rather than discover: `x_label_size` is now shared
axis-naming typography, governing Cartesian axis titles (`common.rs:1528`),
radar spoke names (`radar.rs:135`), and parallel dimension headers. When the
blocked-roles record exposes it, enlarging a parallel header will also enlarge
every axis title in the figure. That is correct — they are all axis-naming text
— and it is a decision, not a side effect.

### 7. Funnel's in-mark value label is excluded, and the exclusion is tested

`funnel.rs:170` stays at `Pixels(16.0)`. It is neither figure chrome nor safely
reservable: it is centred inside a trapezoid whose height comes from the data,
so no reservation can exist for it and an oversized label overflows the mark
rather than displacing a band.

Sizing it needs a mark-text and overflow decision this record does not make.
The exclusion is pinned by gate 9 as a **negative control** rather than by a
source-text check, so it stays deliberate instead of becoming a stale census.

### 8. `legend_size` needs its roles separated before it needs a spelling

Not exposed, and not by a word. Two independent reasons, either sufficient:

1. The field drives both legend text and facet-header size (Context). A
   spelling for one would silently move the other.
2. A top-level `legend_size(n)` spends namespace on a role that has a plausible
   receiver, against §1.

The record that unblocks it separates facet-label size from legend size into
two settings, and decides the legend's receiver question — very likely
`legend()` carrying `.font_size(n)`, `.at(x, y)`, and `.hide()`, which would
supersede PRODUCT-0050's `legend_at` and `hide_legend` and leave the namespace
one word smaller than today rather than one larger.

That consolidation is attractive and is deliberately **not** decided here:
superseding a shipped surface is its own record, and bundling it beside two
modifiers would hide it.

## Implementation sequence

**Stage 0 — measure, then bank.** Three outputs:

1. **Headroom per unreserved role** (§3): at default sizes, the unused space in
   each containing band. This is a measurement, not the arithmetic in Context.
2. **Reservation deltas for the two exposed roles**: how far the heading band
   moves per point of size, and how `title.max(subtitle)` responds to each
   role in isolation, so gates 3 and 4 can assert an amount rather than a
   direction.
3. **Parity captures** for every titled, subtitled, legend-bearing, parallel,
   and funnel figure — including the two hardcoded sites as they render today.

**Stage 1 — the register.** `RuneFigureElement` gains the modifier register
`RuneMark` already has, with `.font_size(...)` and its refusals. No role wired.

**Stage 2 — the two roles.** Typed newtypes, partial `Override` slots, and the
two modifiers.

**Stage 3 — the parallel repair.** §6, size and offset together.

## Acceptance gates

1. **Unauthored parity.** Every Stage 0 capture is byte-identical, except
   parallel — whose header §6 deliberately moves from `22.0` to the resolved
   `x_label_size`. That one exception is asserted as a specific expected
   change, not waived.
2. **The modifier reaches the reservation.** `title("T").font_size(48)` renders
   the title at 48 **and** the reserved band grows to `title_offset + 48`.
   Both halves: a size that draws larger without reserving more is the defect
   PRODUCT-0106 repaired.
3. **The band moves by exactly the delta.** Against Stage 0's measurement: the
   plot area's top edge moves by precisely the size increase, not merely
   downward. A gate asserting only "something moved" passes a wrong inset.
4. **Title and subtitle each contribute to the heading maximum.** The band is
   `title.max(subtitle)` (`common.rs:1180`), **not** the sum, so this is three
   isolated fixtures: title-only, subtitle-only, and both. Each single-role
   fixture's band equals that role's own contribution, and the combined
   fixture equals the larger of the two. An additive assertion would fail
   correct code, and a both-present-only assertion would pass an
   implementation that ignored one role entirely.
5. **The blocked roles refuse in their correct forms.** Two different failures,
   because the forms differ: `x_tick_size(...)` and `y_tick_size(...)` refuse
   as **unknown spellings**, while `x_label(...).font_size(...)` and
   `y_label(...).font_size(...)` **parse** — the member and the modifier both
   exist — and refuse at the `.font_size(...)` span as an unwired role. A gate
   expecting one failure shape for both would go red against a correct
   implementation.
6. **`legend_size` is neither spelled nor decoupled.** Structural, per §8: no
   `legend_size(...)` word and no `legend()` member is added, and
   `facet_label_size` still derives from `settings.legend_size`
   (`common.rs:1194`) exactly as today. The second half matters — a partial
   decoupling would leave two half-owned roles and no record describing
   either.
7. **Non-text members refuse at the modifier span.** `skin("greys_rs2")
   .font_size(12)` refuses, attributed to `.font_size(...)`. Per §5.
8. **Duplicates refuse at the second span.** `title("T").font_size(20)
   .font_size(24)` refuses at the second call, matching the mark register.
9. **Funnel is unchanged by every exposed role.** Set the `title` and
   `subtitle` sizes to extremes; the funnel value label's size is identical to
   Stage 0. A negative control that goes red the day funnel is wired to a role.
10. **Parallel's header tracks the resolved value, size and offset.** Two
    different `x_label_size` values change both the rendered header size and
    its distance from the axis. A literal or a local constant cannot pass; nor
    can routing size while leaving the offset fixed.
11. **The header clears the reserved band at both sizes.** PRODUCT-0106 §1's
    band and §6's now-derived offset agree, so the collision 0106 repaired does
    not return at a larger label size.
12. **Types survive every carrier, for the two exposed roles.** Structural,
    per §4 and PRODUCT-0112 §3: the `PlotTypography` field, the `Override`
    slot, and the `ResolvedPlotSettings` field are the newtype, and the five
    blocked roles remain `f32` — both halves, so a well-meant sweep that typed
    all seven fails.
13. **Bounds pinned at their endpoints.** Each exposed role accepts both range
    endpoints and refuses near-misses outside them, in the PRODUCT-0112 gate 10
    form — near-misses rather than nearest representable neighbours, since a
    Rune literal is `f64`.
14. **The five-tier cascade holds and the partial slot wins within a tier.**
    Per role, matching PRODUCT-0112's precedence test.
15. **No backend measurement capability.** Structural: `PlotBackend` gains no
    glyph or text-extent method. PRODUCT-0093 §7's deferral survives a record
    that changes text sizes, which is when it is most tempting to erode.

## Stop conditions

> If exposing `title` or `subtitle` requires touching the below-plot or
> left-of-plot bands, stop. Those belong to the reservation record §3 names,
> and reaching them from here is the bundling 0064's halt exists to prevent.

> If any exposed role's reservation turns out **not** to track its size after
> Stage 0 measures it, stop and move that role to the blocked list. The rule is
> that a reservation tracks the role, not that any particular count ships.

> If the parallel header cannot clear PRODUCT-0106's band at a larger
> `x_label_size` without measuring realized glyph extents, stop. PRODUCT-0093
> §7's trigger has fired and the repair is no longer a layout change.

> If a fifth text producer is found drawing into a reserved band, stop and
> widen the survey before wiring roles. PRODUCT-0106 found three by one
> exhibit; this record's census found two more constants, and a sixth would
> mean the producers are not enumerable by the method used twice now.

## Supersession

Supersedes no record. Completes PRODUCT-0112's arc by giving the substrate its
first canvas consumers. Leaves PRODUCT-0093 §7's measurement deferral standing
with its trigger intact (gate 15). Changes no geometry **decision** of
PRODUCT-0106 while moving realized coordinates in figures that author a title
or subtitle size — by exactly the amount the heading band grows.

Names, but does not open, the bottom-and-left reservation record that
PRODUCT-0106 left undone on two edges.

## Rejected

**Spell `legend_size(n)` as a top-level word.** Attractive because its
reservation genuinely tracks it, which is this record's own criterion.
Rejected twice over: the field also sizes facet headers, so the word would
misname what it does; and it spends a top-level spelling on a role that could
have a receiver. Both are fixable — in the record that fixes them.

**Expose all seven roles now.** Attractive because it is what was asked for and
the mechanism is identical for each. Rejected because four of them have no
reservation that tracks their size, so authoring them would ship deliberate
collisions — the exact defect PRODUCT-0106 repaired at the top edge.

**Expose all seven and clamp the four unreserved ones to available space.**
Attractive because it looks safe and nothing overlaps. Rejected because it is a
silent lie: the author asks for 36 and gets 22 with no diagnostic. It also
differs from PRODUCT-0110 §4's clamp, which is a resize safety valve that
preserves the authored request — this would make clamping the routine path.

**Build the bottom and left reservations in this record.** Attractive because
it would deliver all seven at once. Rejected on PRODUCT-0064's precedent: it
moves every Cartesian figure's geometry, which is a repair that deserves its
own parity oracle and its own halt conditions rather than riding beside a
spelling.

**`font_size("title", 24)`.** Attractive because one word covers every role and
needs no figure-element register. Rejected per PRODUCT-0112: it turns a
semantic member into a typo-prone string and re-sites refusal attribution from
the role to a string literal.

**Give ticks and the legend bare receiver members to hang modifiers on.**
Attractive because it would make one spelling cover all roles. Rejected because
a member existing only to carry a modifier is a spelling unearned by use, and
PRODUCT-0112's census recorded the receiver count precisely so a later
unification could not invent them.

**Route parallel's header size but leave its offset constant.** Attractive
because it is the smaller change and the size is what looks wrong. Rejected
because it splits one fact across two authorities — the glyphs would track the
setting while their clearance did not — which is the shape §6 exists to close.

## Consequences

`title("Q3 Revenue").font_size(24)` works, and it works honestly: the band
above the plot grows by exactly what the larger glyphs need, because
PRODUCT-0106 built that reservation from the same value the text is drawn at.
The figure namespace is exactly as wide as it was before.

The cost is the narrowing. Five of the seven roles asked for are not delivered,
and the reasons are worth keeping visible. PRODUCT-0106 repaired one edge of a
three-edge defect, and the other two only became urgent when typography made
the sizes authorable. `legend_size` was never one role: it has been sizing
facet headers through a `+ 2.0 / max(10.0)` derivation that no record
mentions, found only because a spelling was proposed for it. Naming a thing is
how you discover it was two things.

Parallel's dimension header stops being the last hardcoded chrome size in
`plot/`, which leaves exactly one — funnel's in-mark label — now pinned by a
negative control rather than by a comment. Two hardcoded sizes were found by
counting 48 draw sites; the discipline that found them is cheaper than the
collision that would otherwise have found them later, on screen, after review.
