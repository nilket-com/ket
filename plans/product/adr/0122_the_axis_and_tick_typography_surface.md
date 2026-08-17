# ADR 0122: the axis and tick typography surface

Status: proposed 2026-08-16. PRODUCT-0113 exposed `title` and `subtitle` sizes
and blocked four roles — `x_label`, `y_label`, `x_tick`, `y_tick` — for one
stated reason: nothing reserved space from their size, so authoring a larger
one would have shipped a collision. PRODUCT-0114 built those reservations.
The blocker is discharged, and this record exposes all four.

It also repairs a gap PRODUCT-0113 shipped without noticing: sizing a text
member required **restating its content**. Axis labels are usually derived from
column names, so the first author to want a larger `value` label would have had
to author the string `"value"` to resize it.

Unlike every geometry record since PRODUCT-0112, this one is **zero-pixel by
default**. Nothing moves until an author asks.

## Context

### The reservations now track all four roles

`VerticalGuideBands` derives the left band from both y roles
(`common.rs:1491-1497`):

```rust
let title = (!matches!(&settings.y_label, ResolvedPresence::Hide))
    .then_some(settings.y_label_padding + settings.y_label_size)
    .unwrap_or(0.0);
(10.0 + requirements.estimated_y_width(settings.y_tick_size)).max(title)
```

and the bottom band from both x roles through `x_axis_guides`
(`common.rs:1512-1534`), where `x_tick_size` sets the rotated tick extent and
`x_label_size` places the axis title below it.

PRODUCT-0113 §2's test — *is the thing being named the thing that would move?* —
is now satisfied for every one of the four.

### Sizing should not require restating content

`y_label` is a `Presence<Arc<String>>` resolved against the kernel's own label
(`common.rs:1519-1520`), which is why a figure reads `value` on its y axis
without anyone authoring it. Under PRODUCT-0113's surface the only way to reach
a size is through the content member, so resizing a derived label would silently
convert it into an authored one — coupling two unrelated decisions and freezing
a string that should track the data.

Titles are usually authored, so this went unnoticed. Axis labels are usually
not, so it surfaces immediately.

### A bare member that does nothing is a typo

PRODUCT-0120's `padding()` compiles today with no edge modifier and no effect.
That is a spelling which cannot mean anything, and it is indistinguishable from
an author who meant to add `.right(120)` and did not.

## Decision

### 1. Four roles are exposed, each by the rule that fits it

```rune
y_label().font_size(20)          // derived text, new size
x_label("quarter").font_size(18) // authored text and size
y_tick_size(14)                  // generated labels, no member
x_tick_size(12)
```

The axis titles take modifiers because `__x_label` and `__y_label` are existing
text-bearing members. Tick labels take words because they have no member and
PRODUCT-0113's receiver census recorded that inventing one — a bare `x_ticks()`
whose only purpose is to carry a size — is a spelling unearned by use.

This completes PRODUCT-0113's split at four authored-text modifiers and two
generated-text words. `legend_size` remains unexposed and is **not** blocked by
layout: it is blocked because the field also sizes facet headers
(`common.rs:1194-1196`), which is a substrate defect this record does not fix.

### 2. Every text member gains a no-argument handle

`title()`, `subtitle()`, `x_label()`, and `y_label()` accept no argument and
serve as modifier receivers. The precedent is PRODUCT-0120's `padding()`.

**The bare form does not touch `Presence`.** It neither shows, hides, nor
replaces content; it only provides an attachment point, and the size lands in
the typography partial slot. The three states stay distinct:

| spelling | content | size |
|---|---|---|
| absent | fallback (kernel-derived) | inherited |
| `y_label()` | fallback, unchanged | authored |
| `y_label("Revenue")` | authored | inherited or authored |

Getting this wrong is silent in both directions: a bare handle resolving to
`Show("")` blanks the axis title while passing any size assertion, and one
resolving to `Show(fallback)` freezes a derived string. Gates 3 and 4 pin both.

This extends PRODUCT-0113 retroactively — `title()` and `subtitle()` gain the
same handle, so resizing an existing title no longer requires restating it.

### 3. A member whose bare form has no effect refuses

Refused at the member's own span, at lowering time:

- `padding()` with no edge modifier;
- `title()`, `subtitle()`, `x_label()`, `y_label()` carrying neither text nor
  a modifier.

The rule is deliberately stated per member rather than as "bare members
refuse," because a bare form can be meaningful: a future `legend()` that makes
a hidden legend visible would carry meaning with no argument at all. The test
is whether the bare spelling changes anything, not whether it has arguments.

This supersedes PRODUCT-0120's inert `padding()`, which nothing can depend on.

### 4. The four sizes become typed; `legend_size` stays raw

Following PRODUCT-0112 §3 and PRODUCT-0113 §4, each newly exposed role becomes
a bounded newtype through all three carriers — the `PlotTypography` field, its
partial `Override` slot, and `ResolvedPlotSettings` — with `f32` appearing only
at the `iced::Pixels` conversion.

The range is **inclusive `6.0 ..= 96.0` logical pixels** for all four, matching
`HoverTextSize` and the two roles PRODUCT-0113 shipped, so the typography family
has one range rather than five. The work-bound justification is stronger here
than for hover: below 6.0 an antialiased glyph stops resolving its strokes, and
above 96.0 a single label exceeds any plausible pane — but unlike the hover
overlay, the band **grows to accommodate** an authored size rather than
overflowing, so the upper bound excludes the unrenderable rather than the
merely large.

`legend_size` remains `f32` until the record that separates it from
`facet_label_size` types it alongside. Typing a field that secretly drives a
second role would protect a value while leaving the coupling wrong.

### 5. `x_label_size` is shared axis-naming typography, now authorable

PRODUCT-0113 §6 recorded that `x_label_size` governs three producers: Cartesian
axis titles (`common.rs:1529`), radar spoke names (`radar.rs:135`), and —
after that record's repair — parallel-coordinate dimension headers, **including
their clearance from the axis** (`common.rs:2411,2413`).

Until now that sharing was inert, because nothing could change the value.
Making `x_label()` authorable makes it visible: enlarging a parallel figure's
dimension headers also enlarges every Cartesian axis title and every radar
spoke name in that figure.

This is correct — all three are axis-naming text — and PRODUCT-0113 accepted it
as a consequence. It is restated here with a gate because an authorable setting
invites a well-meant split: someone will want parallel headers sized
independently and will add a fourth field, at which point 0113's repair is
undone and the band derivation stops matching the glyphs it must clear.

### 6. Default output is byte-identical

No default changes. Every existing figure renders exactly as it does today,
because the bands already derive from these values and the values are unchanged.

This is the first record since PRODUCT-0112 whose parity gate is plain
byte-identity rather than movement-by-exact-delta. It is a property of the
sequencing — PRODUCT-0114 already paid the geometry cost — and gate 1 states it
rather than leaving it implied.

No implementation stage may move a default, including the intermediate ones.

## Implementation sequence

**Stage 0 — bank.** Every figure with axis labels or ticks, at default sizes.
The parity claim in §5 is the strongest assertion this record makes and it must
be measured, not assumed.

**Stage 1 — types and slots.** Four newtypes, four partial `Override` slots,
resolution wiring. No spelling yet.

**Stage 2 — the handles and the bare-form rule.** §§2-3, including
`padding()`'s correction.

**Stage 3 — the spellings.** Two modifiers and two words.

## Acceptance gates

1. **Default output is byte-identical.** Every Stage 0 capture matches exactly.
   Per §6, and the gate that catches a "harmless" default adjustment made while
   introducing a newtype.
2. **Each role moves its own band.** Two values each for `y_label_size`,
   `y_tick_size`, `x_label_size`, `x_tick_size`; each moves its band by the
   amount its derivation predicts and leaves the other three unchanged. Four
   roles, both bands, since a role whose setting resolves but whose band ignores
   it looks identical at default and collides the moment anyone uses it.
3. **A size-only handle preserves derived content.** `y_label().font_size(20)`
   renders the **kernel-derived** text — the same string as with no `y_label`
   at all — at size 20. A `Show("")` implementation blanks the label and passes
   any size-only assertion.
4. **Content and size are independent.** `y_label("Revenue").font_size(20)`
   renders `Revenue` at 20; `y_label("Revenue")` alone renders `Revenue` at the
   inherited size; `y_label()` alone refuses per §3. Three cases, because one
   proves neither independence nor the refusal.
5. **Bare no-effect members refuse at their own span.** `padding()`, `title()`,
   `subtitle()`, `x_label()`, `y_label()` each refuse with attribution to the
   member call. Five cases, since `padding()` is the supersession and the four
   text members are the new surface.
6. **Duplicate modifiers refuse at the second span.** `y_label().font_size(20)
   .font_size(24)`, matching the register PRODUCT-0113 added to
   `RuneFigureElement`.
7. **Wrong receivers still refuse.** `skin("greys_rs2").font_size(12)` refuses
   at the modifier span. PRODUCT-0113 §5's behaviour survives the new handles.
8. **Types survive every carrier, for exactly the exposed roles.** Structural:
   the four newly exposed sizes are newtypes in all three carriers and
   `legend_size` remains `f32` — both halves, so a sweep that types all seven
   fails.
9. **Bounds pinned at endpoints.** Each of the four roles accepts `6` and `96`
   and refuses `5.9` and `96.1`, in PRODUCT-0112 gate 10's form — near-misses
   rather than nearest representable neighbours, since a Rune literal is `f64`.
10. **`x_label_size` reaches all three producers.** One authored
    `x_label(...).font_size(n)` changes the Cartesian axis title, the radar
    spoke names, **and** the parallel dimension header's size *and* its
    clearance, in one figure each. Per §5, so the routing PRODUCT-0113 unified
    cannot silently split once the value is authorable.
11. **Five-tier cascade holds, partial wins within a tier.** Per role, matching
    PRODUCT-0112's precedence test.
12. **No tick member is invented.** Structural: no `x_ticks()` or `y_ticks()`
    receiver is added. Per §1, so the word form cannot drift into a member that
    exists only to carry a size.
13. **No backend measurement.** `PlotBackend` gains no text-extent method.
    PRODUCT-0093 §7 stands.

## Stop conditions

> If any exposed role's band does not respond to its size, stop and return that
> role to the blocked list. PRODUCT-0113's rule is that a reservation tracks the
> role, not that a particular count ships.

> If default output moves at all, stop. §6's byte-identity is the whole reason
> this record is cheap, and a record that quietly resizes existing figures is a
> geometry record wearing a surface record's scope.

> If a bare handle must be permitted for one member to be useful, stop and
> state the per-member rule explicitly rather than weakening §3 into "bare
> members are inert."

> If exposing `y_label` pressures `legend_size` to ship alongside, stop. Its
> blocker is the `facet_label_size` coupling, not layout, and shipping it here
> would let an author resize facet headers by asking for legend text.

## Supersession and inheritance

Completes PRODUCT-0113 §2's blocked list for four of its five roles, and
extends PRODUCT-0113's surface with no-argument handles for `title` and
`subtitle`. Supersedes PRODUCT-0120's inert bare `padding()`.

Inherited unchanged: PRODUCT-0114's band authority and its derivations,
PRODUCT-0117's pinned-face per-class estimator, PRODUCT-0119's tick-attachment
invariant, and PRODUCT-0093 §7's measurement deferral.

## Rejected

**Require restating content to set a size.** Attractive because it needs no new
spelling. Rejected because it converts a derived label into an authored one as
a side effect of typography — the string stops tracking the data, and nothing
in the figure says why.

**A `y_label_size(n)` word instead of a handle.** Attractive because it needs no
`Presence` reasoning. Rejected per PRODUCT-0113 §1: `y_label` is an existing
member that owns the concept, so a flat word would spend namespace on something
already spoken for.

**Invent `x_ticks()` to make all four roles modifiers.** Attractive for
uniformity. Rejected because a member existing only to carry a modifier is a
spelling unearned by use, and PRODUCT-0112 recorded the receiver census
precisely so a later pass could not invent one.

**Let bare members stay inert.** Attractive because it is the status quo for
`padding()` and costs nothing to keep. Rejected because a spelling that cannot
mean anything is indistinguishable from an unfinished one, and two members
answering the same question differently reads as an accident.

**Ship `legend_size` too.** Attractive because it would complete the original
seven-role request. Rejected because that field also sizes facet headers, so
the spelling would misname what it does — a substrate defect, not a layout one.

## Consequences

The original ask — font sizes on seven independent figure elements — is
delivered for six of them: `hover`, `title`, `subtitle`, `x_label`, `y_label`,
`x_tick`, `y_tick`. Only `legend_size` remains, and its blocker is now the only
one left that is not about layout at all.

The no-argument handle also closes a gap that PRODUCT-0113 shipped invisibly.
It went unnoticed because titles are usually authored, and it surfaced within
minutes of the first person wanting a larger axis label — which is the ordinary
case, not the exception. A surface record's blind spots are found by using it.
