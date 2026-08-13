# ADR 0095: the bubble surface

Status: proposed 2026-08-13. Bubble has a validated observation substrate
(PRODUCT-0090), a continuous colour scale (PRODUCT-0089), a transport
(PRODUCT-0094), and no way to author it. This record adds `bubble(...)` — four
required numeric roles plus an **optional label** — sequential-only, and makes
bubble the first live consumer of the combined Gradient+SizeSample legend that
PRODUCT-0093 repaired.

It is step 9 of `plans/wip/plot-catalog-restoration.md` and the second
prerequisite of PRODUCT-0091.

## Context

### What exists, surveyed 2026-08-13

**The substrate and transport are ready.** `BubbleFields { numeric:
BubbleFieldNames, label: Option<String> }` (`spec/bubble_fields.rs:50-53`) landed
with 0094, with the four numeric names private behind
`get(BubbleNumericField)`. `BubbleNumericField` carries `Display` yielding
`"x" | "y" | "size" | "color"` (`:11-19`). `MarkFields::Bubble` exists and its
`all()` already handles 4-or-5 arity.

**No authored path, and no compatibility arm.** `semantic_is_compatible` has no
`PlotType::Bubble` entry, so its `_ => false` fallback refuses every authored
bubble channel — the same gap heatmap had.

**Optional channels exist, with three states.** `OptionalAuthoredChannel` is
`Named | Auto | Absent` (`spec/mod.rs:283-287`), used today only for the colour
channel (`:432`).

**PRODUCT-0090 §9 decided label semantics for two of those states.** Encoding not
selected → generated `Point {i}` placeholders; encoding selected with a null
value → `None`, omitting the first hover line; encoding selected with the column
absent → refuse.

**Bubble is the only consumer of the combined legend.** `Gradient + SizeSample`
(`legend.rs:310-314`) has never had a live figure. PRODUCT-0093 repaired its
geometry through fixtures.

**Size normalization is fixed.** `radius = 2.0 + t_size * 28.0`
(`bubble.rs:128`), with no authored control.

## Decision

### 1. One word, two arities

```text
bubble("gdp", "life", "population", "growth")
bubble("gdp", "life", "population", "growth", "country")
```

Following `raw_box`'s layout — field strings, then one injected `(start, end)`
span pair per field, then the mark origin — the accepted lengths are **14** (four
roles) and **17** (five). Any other length refuses.

One word rather than a `bubble` / `bubble_labeled` pair: the label is an optional
argument, not a different mark, and two words would double every downstream case
for a decoration.

### 2. Label is named-or-absent; `Auto` is not exposed

The surface accepts a label **field name** or **no fifth argument**. It does not
expose `OptionalAuthoredChannel::Auto`.

`Auto` means "infer a compatible unused field from the schema"
(`resolve_required:2354`). For a label that is Polariton silently selecting a
nominal column the author never named and printing it on every hover — the
silent-selection defect class of PRODUCT-0078 and radial dial, arriving through a
channel state instead of a `.get(0)`.

Widening to `Auto` later is cheap; retracting a silent selection after figures
depend on it is not.

**The two exposed states map onto PRODUCT-0090 §9 exactly:** no fifth argument is
"encoding not selected" (`Point {i}` placeholders); a named field is "encoding
selected" (null → `None`, absent column → refuse).

### 3. Compatibility: quantitative axes, quantitative extra roles, nominal label

`semantic_is_compatible` gains:

```rust
(PlotType::Bubble, Channel::X | Channel::Y) => semantic == SemanticType::Quantitative,
```

and the per-role loop requires:

| role | semantic |
|---|---|
| `size` | Quantitative |
| `color` | Quantitative |
| `label` (when selected) | Nominal |

Load-bearing, as in PRODUCT-0092 §3: without the entry the `_ => false` fallback
refuses everything, and an `_ => true` written instead would produce a surface
with no validation.

Bubble's axes are quantitative — unlike heatmap's, which are categorical — so
this arm cannot be copied from that record.

### 4. `ChannelError` gains tagged attribution, because channels cannot name roles

`ChannelError` requires a `Channel` (`spec/mod.rs:1473-1477`) and its formatter
always emits `plot {channel} channel …` (`:1479`). A bubble `size`, `color`, or
`label` failure therefore has no honest attribution today — it must masquerade as
X, Y, or Color. An earlier draft asserted role-named diagnostics without funding
this, making its gates unimplementable.

```rust
pub enum FieldAttribution {
    Channel(Channel),
    Role(&'static str),
}
```

`ChannelError`'s `channel: Channel` becomes `attribution: FieldAttribution`, and
the formatter branches: channels keep today's wording exactly, roles read
`plot {role} field …`.

**`Role(&'static str)` rather than a role enum.** The per-role binding loops
already carry role names as string literals — `("whisker_low",
&fields.whisker_low)` (`spec/mod.rs:1795-1799`) — so this matches existing
practice and introduces no new vocabulary. A unified `MarkRole` enum across box
plot, heatmap, and bubble would be a *fifth* role vocabulary alongside the four
that exist.

**Scope is bounded:** 30 construction sites, all in `spec/mod.rs`, all becoming
`FieldAttribution::Channel(..)` mechanically, plus five test reads of
`error.channel` at `:3301-3367` that migrate to attribution assertions. Bubble is
the first mark to prove the channel-only vocabulary insufficient; it is not the
first that would have benefited, since box plot's five extra roles all report as
`Channel::Y` today.

### 5. One source for bubble's role words

Two bubble role vocabularies exist: **`BubblePreparationChannel`** (`bubble.rs:45-51`)
covering all five roles including `Label`, and **`BubbleNumericField`**
(`spec/bubble_fields.rs:3-9`) covering the four numeric ones. They are not
duplicates — different domains — and an earlier draft named the wrong one:
`BubbleNumericField` cannot express `label`.

`BubblePreparationChannel` is the surface's vocabulary, and per PRODUCT-0094 §4's
precedent it **moves to `plot/spec`** so the spec layer does not depend on
`plot/kernels`.

It gains `const fn as_str(&self) -> &'static str`, and **both** binding-time
`FieldAttribution::Role` and preparation-time refusals take their words from it.
An earlier draft claimed binding and preparation "speak the same words"; that was
textual coincidence between two independent `Display` impls, not one authority.
This makes it structural.

`BubbleNumericField` stays as 0090's numeric-only subset, unchanged.

**All five spans are retained** to planning, per PRODUCT-0092 §4's precedent, with
preparation refusals mapping onto them:

| refusal | span |
|---|---|
| null / non-finite in role *r* | that role's argument |
| missing or uncastable column for role *r* | that role's argument |
| missing label column | the label argument |

PRODUCT-0092 §4 routed heatmap's `value` through `Channel::Color` because heatmap
has no colour channel. Bubble **does** — `color` is one of its four numeric roles
— so that mapping must not be copied. `FieldAttribution::Role` is what makes not
copying it possible.

### 6. Projection carries four fields, or five with a label

Through `MarkFields::Bubble` and 0094's exhaustive `all()`. No new projection
arm: bubble is the transport's first new consumer, and adding one would mean 0094
did not land its property.

### 7. Kernel and preparation stay on `BubbleFields`

`prepare_bubble_data` and the kernel keep the specific payload. Per PRODUCT-0094
§8's rule — the sum type belongs where marks are mixed, the specific type where
the mark is already known — a bubble-only function must not unwrap a variant it
can only ever receive one value of.

### 8. Sequential-only; PRODUCT-0091's tripwire stays intact

The mark requests `ContinuousScaleIntent::SEQUENTIAL` and gains no job word.
`architecture_test.rs:464-465` is unchanged and must still pass.

### 9. Prerequisite F stays open

`size` authors a *field name*, not a size scale. `radius = 2.0 + t_size * 28.0`
(`bubble.rs:128`) is existing kernel behaviour and this record does not touch it.

Fixed normalization is not a blocker to authoring the mark — the same judgement
heatmap made about relation-side aggregation. Size-scale control is its own
record, and **F remains open**.

### 10. Surface positions

| position | verdict |
|---|---|
| `relation.views([bubble(...)])` | **accept** |
| `view("name", bubble(...))` | **accept** |
| `figure([...])`, named or bare | **accept** |
| `layers([...])` | **refuse** |
| legacy `plot.bubble(...)` | **refuse** |
| `relation.bubble(...)` | **refuse** |

Matching PRODUCT-0092 §5. `layers([...])` refuses for the same reason: composing
bubble with another mark has no defined meaning until a record decides one.

### 11. The exhibit exercises the combined legend on a live figure

`uat/bubble_probe.sql` and `uat/test28.rn`, following `heat_probe.sql` /
`test27.rn`.

**Bubble is the first live consumer of `Gradient + SizeSample`**
(`legend.rs:310-314`). PRODUCT-0093 repaired that path's geometry through
fixtures, never through a figure. The exhibit must therefore show **both legend
sections rendering together, unclipped**, with:

- a **signed colour domain**, so the gradient endpoint carries a minus sign — the
  case 0093 repaired;
- a **wide size range**, so the size sample prints distinguishable endpoints;
- **one labelled and one unlabelled figure**, so §2's two states are both visible;
- a **null label row** in the labelled figure, so PRODUCT-0090 §9's omitted hover
  line has a live demonstration.

### 12. No persistence footprint, no colour identity

Bubble allocates no `ColorIdentity`, so it never reaches
`AuthoredFamilyId::parse`. `session_persistence.rs` names only `Bar |
HorizontalBar | Line | Scatter | StackedArea` and gains nothing. Gated as
PRODUCT-0092 gate 17 gated it, scoped to the owner payload.

### 13. What this record does not do

No job word, anchor, or slot palettes — PRODUCT-0091. No size scale (§9). No
`Auto` label (§2). No hexbin. No changes to `BubbleObservations` or its refusals
(0090). No new projection arm (§6). No transport changes (0094).

## Implementation sequence

**Stage 0 — bank.** Every current bubble figure. As with heatmap, this is a
**refactor oracle, not user pixels**: bubble has no production producer, so
nothing banked is user-reachable. It exists so the authored path can be shown to
change nothing about the automatic one.

**Stage 1 — compatibility and roles.** §3, testable before reachable.

**Stage 2 — the word.** `bubble(...)` at both arities, with span injection.

**Stage 3 — diagnostics.** §§4–5 provide the attribution, vocabulary, and span
mapping through to the host.

**Stage 4 — the exhibit.** `bubble_probe.sql`, `test28.rn`, README entry.

## Acceptance gates

1. **Both arities render.** The four-argument and five-argument forms each
   produce a figure. Any other length refuses, naming the expected arities.
2. **Automatic bubbles are unmoved.** Every banked automatically-resolved bubble
   figure is byte-identical to Stage 0.
3. **X and Y accept quantitative and refuse categorical.** A numeric column binds
   as `x`; a String column named as `x` refuses. Both halves — and note this is
   the *opposite* polarity from heatmap gate 3, so a copied entry fails.
4. **Size and color accept quantitative, refuse string and boolean.** Per §3, with
   `Boolean` refusing rather than casting — the surprising case, as in
   PRODUCT-0092 gate 6.
5. **Label accepts nominal and refuses numeric.** A String column binds as
   `label`; a numeric column refuses.
6. **`Auto` is not exposed for label.** No spelling produces
   `OptionalAuthoredChannel::Auto` for the label role — asserted structurally, per
   §2, since the claim is about what can be written.
7. **No fifth argument yields placeholders.** Observations carry `Point 0`,
   `Point 1`, … per PRODUCT-0090 §9.
8. **A selected label column that is absent refuses**, naming the label argument —
   it does not fall back to placeholders. Paired with gate 7, since a placeholder
   is correct in exactly one of the two label-absent situations.
9. **A null label is `None` and omits one hover line.** Four quantitative hover
   lines remain, with no leading blank and no `Point {i}` substitute — asserted by
   line count **and** content.
10. **Projection carries four or five fields.** With a label, five; without, four.
    Both, and through 0094's `all()` with **no new projection arm** — §6's claim
    that the transport landed its property.
11. **Every role survives projection.** For each of the five, the projected frame
    contains it. Mirrors PRODUCT-0092 gate 14, which caught the omission 0094 made
    impossible.
12. **Diagnostics speak role names, not channels.** A bad `size` names `size`, not
    `Channel::Y`; a bad `color` names `color`. Specifically, no bubble diagnostic
    routes through `Channel::Color` the way heatmap's `value` does — §5's
    non-transferable case.
13. **Channel diagnostics are unchanged.** Every existing `ChannelError` message
    for every mark reads exactly as before the `FieldAttribution` migration —
    30 constructors, all mechanical. The five existing `error.channel` test reads
    migrate to attribution assertions. The migration's whole warrant is that it
    adds a variant without disturbing the existing one.
14. **Role and channel attribution are distinguishable.** A `Role`-attributed
    error and a `Channel`-attributed one produce different wording, so a consumer
    can tell which it has. Without this, `FieldAttribution` could collapse to a
    string and lose the distinction it was added for.
15. **Binding and preparation use one word source.** For each of the five roles,
    the binding-time role name and the preparation-time role name are equal *and*
    both derive from `BubblePreparationChannel::as_str` — asserted structurally,
    not by string comparison, since equal strings from two `Display` impls is the
    coincidence §5 replaces.
16. **All five spans point at their argument.** A refusal for any role carries a
    span covering that argument, not the whole `bubble(...)` call. Five cases.
17. **Preparation refusals map to role spans.** PRODUCT-0090's null, non-finite,
    and column faults each surface with the span from §5's table, through the host
    rather than the whole-figure fallback.
18. **The intent is sequential.** An authored bubble yields `{ Sequential, None }`,
    and `architecture_test.rs:464-465` still passes unchanged.
19. **The combined legend renders both sections, unclipped.** Gradient and
    SizeSample together on the live exhibit, with a **negative** colour endpoint
    fully visible. This is PRODUCT-0093's repair on its first real consumer, and
    the gate 0093 could not write because no figure existed.
20. **Kernel and preparation take `BubbleFields`.** Asserted structurally: no
    `MarkFields` unwrap appears in bubble-only functions, per §7 and PRODUCT-0094
    §8.
21. **The exhibit shows both label states.** One labelled figure and one
    unlabelled, plus a null-label row whose hover omits its first line.
22. **No persistence footprint.** Wire schema unchanged, panel mapping retained,
    empty colour footprint, no bubble or family spelling **in the owner payload**,
    restored panel unresolved, rerun rejoins the same panel.
23. **No categorical identity is allocated.** Resolving an authored bubble against
    a known `ColorIdentity` snapshot returns it unchanged.

## Stop conditions

> If the combined legend cannot render both sections unclipped on the exhibit,
> stop and reopen PRODUCT-0093 rather than adjusting the fixture. That path had no
> live consumer when it was repaired; this is the first evidence that the repair
> holds, and a fixture tuned until it fits would discard exactly that evidence.

> If `size` or `color` cannot validate as extra roles without routing through
> `Channel::Color`, stop. Bubble has a real colour role, and collapsing the two
> would make §5's diagnostics name the wrong thing for one of them.

> If authoring bubble requires touching `radius = 2.0 + t_size * 28.0`, stop.
> §9 leaves prerequisite F open deliberately; a surface record that reaches the
> size mapping has taken F's decision without its evidence.

> If projection needs a bubble-specific arm, stop. PRODUCT-0094's whole warrant
> was that the next mark would not need one, and needing one here means the
> exhaustive `all()` did not land its property.

## Supersession

Supersedes no record. Consumes PRODUCT-0089's scale, PRODUCT-0090's observations,
PRODUCT-0093's legend geometry, and PRODUCT-0094's transport unchanged.
Completes the second of PRODUCT-0091's two prerequisites. Leaves prerequisite
**F** open. Adds nothing to persistence.

## Rejected

**Two words — `bubble(...)` and `bubble_labeled(...)`.** Attractive because each
has fixed arity, matching box plot and heatmap. Rejected because the label is an
optional argument rather than a different mark, and two words would double every
downstream case for a decoration.

**Expose `Auto` for the label.** Attractive because the machinery exists and the
colour channel already uses it. Rejected because inferring a label means silently
printing a column the author never named — PRODUCT-0078's defect class through a
channel state. §2 keeps it retractable by not shipping it.

**Copy heatmap's `Channel::Color` attribution for an extra role.** Attractive
because PRODUCT-0092 §4 established it one record ago. Rejected because heatmap
has no colour channel and bubble does; the same mapping here would make two
different roles report as the same channel.

**Accept a channel placeholder for role failures.** Attractive because
`ChannelError` stays as it is and no migration is needed. Rejected because it
means a `size` failure reports as X, Y, or Color — a diagnostic that names the
wrong field is worse than a vague one, and every consumer downstream would then
be reading a channel that the author cannot map to anything they wrote.

**Ship a size scale with the surface.** Attractive because `size` is authorable
and its mapping is not, which reads as half-finished. Rejected as prerequisite
**F**: fixed normalization is existing kernel behaviour, not a defect introduced
here, and the same judgement let heatmap ship without relation-side aggregation.

**Let the exhibit use a non-negative colour domain.** Attractive because the
figure is prettier and the gradient simpler. Rejected because the negative
endpoint is precisely what PRODUCT-0093 repaired, and bubble is the first live
consumer of the path it repaired — a positive-only exhibit would leave that
untested where it matters.

## Consequences

Both of PRODUCT-0091's prerequisites are met, and the continuous colour authoring
surface can proceed with two authorable quantitative marks to author over.

The combined Gradient+SizeSample legend gets its first live consumer. PRODUCT-0093
repaired geometry that no figure had ever exercised; gate 19 is the first evidence
that the repair holds in production rather than in fixtures — and the first stop
condition refuses the temptation to tune the fixture if it doesn't.

PRODUCT-0094's transport gets its first new consumer, and §6 is the test of
whether it earned its record: if bubble needs no new projection arm, the
exhaustive `all()` did what it was built for. Gate 10 and the fourth stop
condition are that claim made falsifiable.

Prerequisite **F** remains open with a mark now authorable on top of it. That is
deliberate but worth naming: bubble figures can be written and their radii cannot
be controlled, which is the most likely first request once anyone uses it.
