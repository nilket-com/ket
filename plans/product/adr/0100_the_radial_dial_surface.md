# ADR 0100: the radial dial surface

Status: proposed 2026-08-13. Radial dial has a validated reading substrate
(PRODUCT-0099), a repaired faceted hover seam (PRODUCT-0097), a transport
(PRODUCT-0094), and no way to author it. This record adds
`radial_dial(value, maximum[, label])` and makes radial dial the **second**
consumer of PRODUCT-0086's extensible categorical-family path — the first test of
whether PRODUCT-0098 established something reusable or something bespoke.

It is step 15 of `plans/wip/plot-catalog-restoration.md`.

## Context

### What PRODUCT-0098 established, and what this record tests

0098 consumed prerequisite **A** and claimed in its Consequences that each
remaining categorical mark would add "one token and one mapping arm rather than
rediscovering the path." Radial dial is the first mark in a position to falsify
that.

### What exists, surveyed 2026-08-13

**Colour is positional and index-keyed.** `t = i as f32 / (num_cats - 1) as f32`,
`settings.color_theme.get_color(t)` (`radial_dial.rs:49-54`). Reordering
categories recolours every ring. This is funnel's pre-0098 shape exactly.

**Both colour consumers already exist.** `legend_content` at `:78` and the paint
loop at `:49-54` derive colour independently today.

**The ungrouped label is already inserted at the presentation sites.**
PRODUCT-0099's implementation added `reading.category().unwrap_or("All Data")` at
`legend_content:86` and `hover:123`, with a test pinning it at `:880`. The
substrate carries `category: None` honestly; the kernel supplies the label where
it is displayed.

**No compatibility arm.** `semantic_is_compatible` has no `PlotType::RadialDial`
entry, so `_ => false` refuses every authored channel — the gap heatmap, bubble,
and funnel each had.

**The resolver already fixes the field contract.** `label: optional_category(0)`,
`value: number(0, "value")?`, `maximum: number(1, "maximum")?`
(`spec/mod.rs:2753-2757`).

## Decision

### 1. `radial_dial(value, maximum[, label])` — two arities

Following `raw_box`'s layout, the accepted lengths are **8** (two roles) and
**11** (three).

**The optional label goes last**, matching PRODUCT-0095 §1's variable-arity
grammar for bubble. The resolver places `label` at `optional_category(0)`, but
resolver field order is an internal binding concern and not public syntax: a
leading optional argument makes two-versus-three arity ambiguous to a reader, and
no existing word does it.

### 2. A `RadialDial` variant in the transport

`MarkFields::RadialDial(RadialDialFields)` and the spanned counterpart, per
PRODUCT-0094 §1. `RadialDialFields { value, maximum, label: Option<String> }` —
2-or-3 arity through the exhaustive `all()`, which PRODUCT-0094 §3 sized for
exactly this. **No new projection arm.**

### 3. Nominal label, quantitative value and maximum

```rust
(PlotType::RadialDial, Channel::X | Channel::Y) => semantic == SemanticType::Nominal,
```

for the label channel, with `value` and `maximum` binding through the per-role
loop as Quantitative. Load-bearing as in PRODUCT-0092 §3.

PRODUCT-0099 §7 already fixes dtype admission at the substrate; this arm states
at binding what preparation enforces, and the two must agree.

### 4. Diagnostics carry field and row, and one carries neither

`FieldAttribution::Role` (PRODUCT-0095 §4) for the three roles, with
PRODUCT-0099 §7's refusals mapped to argument spans.

**`UngroupedMultipleRows` attributes to the mark origin**, per 0099 §5: in
ungrouped mode there is no label argument to point at. It is the only refusal in
this record that names no field, and the host mapping must carry that case rather
than defaulting it to a whole-figure span for the wrong reason.

### 5. One family token, one conversion arm

- `AuthoredFamilyId::RADIAL_DIAL`, added to `ALL`, taking it to six.
- `from_authored_layer` gains **exactly one** arm.
- Both `unreachable!` arms stay distinct, per PRODUCT-0098 §5.

**This is the record's test of PRODUCT-0086.** If the persistence path costs more
than one token and one arm, the reusable categorical path has failed its second
consumer — see the first stop condition, which is scoped to that path alone.

### 6. Complete assignments; positional fallback only for unauthored figures

PRODUCT-0098 §6 transfers directly:

**Authored radial dial:** each category resolves to a complete
`CategoryColorAssignment`; paint and `legend_content` consume **one shared
accessor**. No authored category may reach the fallback.

**Unassigned legacy or automatic radial dial:** the positional ramp
(`radial_dial.rs:49-54`) is retained, carrying no durable identity guarantee.

**The fallback is not label-keyed**, for 0098 §6's reason: a label-keyed fallback
is a second colour allocator beside PRODUCT-0086, and its disagreement is
invisible because both paths produce plausible colours.

### 7. The ungrouped label is ratified, not introduced

`reading.category().unwrap_or("All Data")` already exists at
`legend_content:86` and `hover:123`. This record **ratifies and exposes** it as
the surface's contract rather than adding it.

The substrate invariant is unchanged and remains load-bearing: `category: None`
travels through `RadialDialReadings` untouched, and the string appears only where
a label is displayed. A future consumer that needs the distinction still has it.

### 8. Surface positions

| position | verdict |
|---|---|
| `relation.views([radial_dial(...)])` | **accept** |
| `view("name", radial_dial(...))` | **accept** |
| `figure([...])`, named or bare | **accept** |
| `layers([...])` | **refuse** |
| legacy `plot.radial_dial(...)` | **refuse** |
| `relation.radial_dial(...)` | **refuse** |

Matching PRODUCT-0092 §5, PRODUCT-0095 §9, and PRODUCT-0098 §8.

### 9. What this record does not do

No colour authoring. No authored category ordering — PRODUCT-0099 §4 kept lexical
ordering with its own trigger. No aggregation. No changes to
`RadialDialReadings` or its refusals (0099). No hover-seam work (0097). No new
projection arm (§2).

## Implementation sequence

**Stage 0 — bank.** Every radial dial figure after 0099, plus a session
round-trip with no radial dial in it, so §5's additions can be shown not to
disturb existing documents.

**Stage 1 — compatibility, transport, and the word.** §§1–3.

**Stage 2 — the family.** §5.

**Stage 3 — identity and colour.** §6.

**Stage 4 — diagnostics.** §4, including the mark-origin case.

**Stage 5 — the exhibit.** `radial_dial_probe.sql`, `test31.rn`, README entry.

## Acceptance gates

1. **Both arities render.** Two-argument and three-argument forms each produce a
   figure; any other length refuses, naming the expected arities.
2. **Label accepts nominal and refuses numeric**; **value and maximum accept
   quantitative and refuse string**. Both halves each.
3. **Binding and preparation agree on dtype.** A combination admitted at binding
   is admitted by `RadialDialReadings::from_rows`, and one refused at binding
   never reaches it.
4. **Every 0099 refusal carries its argument span**, per §4.
5. **`UngroupedMultipleRows` attributes to the mark origin**, not to a field and
   not to a whole-figure fallback. The only refusal here naming no field, so a
   host mapping that defaults it would pass gate 4 and fail this.
6. **Projection carries two or three fields with no new arm.** Both arities,
   through 0094's `all()`; a bespoke radial-dial branch in
   `project_named_channels` fails this.
7. **`AuthoredFamilyId::RADIAL_DIAL` exists and is catalogued.** `ALL` has six
   members and contains it.
8. **`ExtensibleFamilies` admits and round-trips it**; **the closed policies
   refuse it.** Three cases — the refusals are what prove 0086's bounded design,
   since an admit-everything policy passes the first alone.
9. **`from_authored_layer` gains exactly one arm**, and both `unreachable!` arms
   remain distinct with their existing messages. Structural, per §5.
10. **Authored categories never reach the fallback.** Every category in an
    authored radial dial resolves through a `CategoryColorAssignment`.
    Structural, per §6.
11. **Paint and legend use one accessor.** A category's ring colour equals its
    legend entry colour, for every category — asserted through the shared
    accessor, not by comparing two computed values.
12. **The fallback stays positional.** An unassigned radial dial still colours by
    index, so §6's boundary cannot erode into a second allocator.
13. **Durable identity survives an asymmetric lineage.** The discriminating gate,
    and **automated, not exhibited**: because PRODUCT-0099 §4 sorts categories
    lexically, simply reordering rows changes nothing about render order and is
    therefore weak evidence. The gate instead re-runs a lineage whose **category
    set differs** — one added, one removed — and asserts that categories present
    in both retain their established assignment while the new one allocates
    fresh.
14. **Assignments persist and restore**, and **reattachment restores the same
    allocation** rather than a fresh one.
15. **Existing documents are undisturbed.** A session with no radial dial
    round-trips byte-identically after §5's additions.
16. **The ungrouped label is a presentation fact only.** `legend_content` and
    `hover` show `"All Data"` for `category: None`; the string appears **nowhere**
    in `RadialDialReadings` or its errors. §7's invariant, asserted at the
    substrate boundary rather than at the display.
17. **Radial dial hovers in a non-origin facet cell.** PRODUCT-0097's repair holds
    for an *authored* radial dial, which did not exist when it landed.
18. **Surface positions match §8 exactly.** Six cases.

## Stop conditions

> If the **PRODUCT-0086 persistence path** costs more than one family token and
> one conversion arm, stop. This scope is deliberate: the surface as a whole
> necessarily costs more plumbing — a word, a transport variant, a compatibility
> arm, diagnostics, and colour wiring. It is only the *persistence* path that
> 0098 claimed would be one-token-one-arm, and only there that a larger cost is a
> finding about 0086's reusability rather than ordinary work.

> If any `"All Data"` string reaches `RadialDialReadings` or its errors, stop.
> §7's invariant is that the substrate carries `category: None`; a label in the
> collection means the surface's presentation concern has leaked into the
> substrate PRODUCT-0099 built to keep it out.

> If gate 13 cannot be written without relying on row reordering, stop and
> reconsider the fixture. Lexical ordering makes reordering unobservable, so a
> reorder-based gate would assert durable identity while testing nothing.

> If authoring radial dial requires touching PRODUCT-0099's refusals or its
> lexical ordering, stop. Both were decided one record ago with their own
> evidence, and a surface that reopens them has taken a substrate decision
> without it.

## Supersession

Supersedes no record. Consumes PRODUCT-0099's readings, PRODUCT-0097's hover
seam, and PRODUCT-0094's transport unchanged. Second consumer of PRODUCT-0086,
extending `AuthoredFamilyId::ALL` from five members to six. Ratifies the
ungrouped presentation label PRODUCT-0099's implementation introduced.

## Rejected

**Put the optional label first, matching the resolver.** Attractive because
`optional_category(0)` places it first in the binding. Rejected because resolver
field order is internal: a leading optional argument makes two-versus-three arity
ambiguous at the call site, and PRODUCT-0095 §1 already established the trailing
form for bubble.

**Claim this record introduces the `"All Data"` label.** Attractive because it
reads as a complete surface decision. Rejected because the label already exists
at `legend_content:86` and `hover:123` after PRODUCT-0099; a record that claims
to add what is already there misleads the next reader about where the behaviour
came from and what its gate is protecting.

**Demonstrate durable colour identity by reordering rows in `test31.rn`.**
Attractive because it mirrors PRODUCT-0098's funnel exhibit. Rejected because
funnel preserves relation order and radial dial sorts lexically — reordering rows
leaves the rendering identical, so the demonstration would prove nothing. The
asymmetric-lineage gate replaces it, and stays automated.

**Make the positional fallback label-keyed.** Attractive as the consistent
choice. Rejected for PRODUCT-0098 §6's reason: it is a second colour allocator
beside PRODUCT-0086, and its disagreement is invisible because both paths produce
plausible colours.

**Add the remaining categorical marks' family tokens while `ALL` is open.**
Attractive because four more tokens is a small diff. Rejected because a token
with no mark behind it is unobservable and its gates vacuous — PRODUCT-0091 §10's
reason for refusing to spell status, and PRODUCT-0098's for refusing this same
shortcut.

## Consequences

Radial dial becomes authorable, and PRODUCT-0086's categorical path acquires its
second consumer. Whether 0098 established a reusable route or a bespoke one is
answered here, and the first stop condition is what makes the answer
falsifiable — scoped to persistence, where the claim was actually made.

The exhibit and the identity gate separate deliberately. `test31.rn` shows what a
reader can see — out-of-order input rendering lexically, and an ungrouped dial —
while durable identity is asserted where it can actually be observed, against an
asymmetric lineage. A mark that sorts its categories cannot demonstrate colour
stability by reordering them, and noticing that before writing the exhibit is
cheaper than discovering it in review.

Three categorical marks remain: pie, radar with parallel, and violin. Each still
owes its own cardinality decision, since prerequisite **E** was consumed for
three specific marks and pie aggregates by design.
