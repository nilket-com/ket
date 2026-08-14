# ADR 0103: the pie surface

Status: proposed 2026-08-13. Pie has a validated slice substrate (PRODUCT-0102),
a repaired arc contract (PRODUCT-0101), a transport (PRODUCT-0094), and no way to
author it. This record adds `pie("category", "value")` — **grouped only** — makes
pie PRODUCT-0086's third categorical consumer, corrects two role
misattributions, and owes the live `.aggregate(...)` exhibit that 0102 could not
provide while pie was producerless.

It is step 18 of `plans/wip/plot-catalog-restoration.md`.

## Context

### The authorable contract is narrower than the kernel's

PRODUCT-0102 §3 made pie's category optional, because automatic resolution has
always permitted an ungrouped pie and legacy figures depend on it. That is a
*substrate* accommodation. It does not follow that Rune should expose it — see
§1.

### Colour resolution has two enumeration sites, not one

`authored_categories` is checked at `spec/mod.rs:2566` **and** `:2652`, and both
currently name only `Funnel` and `RadialDial`.

This is the first evidence about how reusable PRODUCT-0086's path really is.
PRODUCT-0100 claimed — and this record's predecessor confirmed — that
**persistence** costs one token and one arm. That claim was deliberately scoped
to persistence, and the scoping turns out to have been right: *colour* costs two
more enumerations, and a record that had generalized 0100's claim beyond
persistence would have missed them both.

### Two role misattributions

**The sector stroke reads `chrome.canvas_background`.** Its role is
`mark_style.separator` — an internal boundary between two filled sectors, exactly
PRODUCT-0098 §7's paper-side classification. The two values coincide under every
current projection, so this is a **semantic correction with pixel parity**.

**The stroked path is mixed geometry, and stays mixed.** One closed path carries
both the radial dividers *and* the outer silhouette, so assigning it wholly to
`separator` gives the outer arc a role PRODUCT-0098 §7 would classify as
`fill_detail`. The assignment still goes to `separator`, because the internal
divider is the path's distinguishing role — and the mixed path is recorded as an
**existing limitation** rather than fixed here. Splitting it into two paths is
geometry work that would invalidate this record's pixel parity, and it belongs to
whichever record has a reason to touch pie's paths.

**The `Row N` fallback carries `LegendProvenance::Named`.** It is a positional
placeholder, not a name. `Synthetic` is the correct provenance, and it matters:
PRODUCT-0044 omits a legend consisting of a **lone** synthetic entry
(`legend.rs:91`), while multiple rows retain their useful key.

The `Row N` fallback itself is already correctly outside `PieSlices` — 0102 §3
put it at presentation — so only its provenance is wrong.

## Decision

### 1. `pie("category", "value")` — grouped only, no one-argument form

Two fields, and the category is **required at the surface** even though the
substrate accepts its absence.

**Why this differs from radial dial.** PRODUCT-0100 §1 exposed an ungrouped form
because an ungrouped dial is exactly *one* reading with one presentation label.
An ungrouped pie is **arbitrarily many anonymous slices**, and `Row 1, Row 2, …`
are positional labels rather than durable identities: inserting a row renames
every slice after it, and under §4's colour identity, **recolours them too**. A
mark whose slice identities shift when a row is inserted has no stable legend and
no durable assignment.

It would also **bypass the categorical capacity** enforced on named slices — a
limit that exists precisely because unbounded categorical allocation is what
PRODUCT-0086's bounded design guards against.

The optional category stays in the substrate for automatic and legacy
compatibility. Rune requires real slice identity.

### 2. A `Pie` variant in the transport

`MarkFields::Pie(PieFields)` and the spanned counterpart, per PRODUCT-0094 §1.
`PieFields { category, value }`, two fields through the exhaustive `all()` — **no
projection arm**, which the transport already handles and which is now its third
consecutive new consumer without one.

### 3. A dedicated pie resolver

Category **Nominal**, value **Quantitative**, with role-specific diagnostics and
PRODUCT-0102's refusals mapped to argument spans:

| refusal | span |
|---|---|
| null / duplicate category, unsupported dtype | the `category` argument |
| null, non-finite, negative, inexact value | the `value` argument |
| non-finite or zero total | the `value` argument |

The duplicate diagnostic names `.aggregate(...)`, per 0102 §1 — and §9's exhibit
is where that instruction is finally demonstrated rather than merely printed.

### 4. Complete assignments; the fallback stays positional

Transferring PRODUCT-0098 §6 and PRODUCT-0100 §6 unchanged:

**Authored pie** — each category resolves to a complete `CategoryColorAssignment`;
paint and legend consume **one shared accessor**.

**Automatic pie** — the positional ramp is retained, with no durable-identity
guarantee, and **not** made label-keyed: that would be a second colour allocator
beside PRODUCT-0086, whose disagreement is invisible because both paths produce
plausible colours.

### 5. Pie joins both `authored_categories` enumerations

`spec/mod.rs:2566` and `:2652`. Both, and gated as two, because they are separate
matches that happen to list the same marks — updating one and not the other
produces a pie that allocates identity in one code path and falls back in the
other, which renders plausibly.

### 6. One family token, one conversion arm

`AuthoredFamilyId::PIE`, added to `ALL`, taking it to **seven**;
`from_authored_layer` gains exactly one arm; both `unreachable!` arms stay
distinct per PRODUCT-0098 §5. Closed schema policies refuse the token; the
extensible policy round-trips it.

### 7. Two role corrections, one with pixel parity and one without

**The sector stroke becomes `mark_style.separator`.** Values coincide under every
current projection, so no pixel moves — which makes this a gate that must be
written **structurally**, since a pixel-parity assertion passes trivially and
proves nothing.

**The `Row N` legend provenance becomes `Synthetic`.** Consequence: a **lone**
`Row 1` legend is now omitted per PRODUCT-0044 (`legend.rs:91`), while multiple
rows keep their entries.

**This one does not have pixel parity, and is not meant to.** It deliberately
removes a legend from one-row automatic pies — a real, visible behaviour change,
and the correct one, because a single positional placeholder is a legend that
names nothing. Only the separator correction is parity-preserving; conflating the
two would let gate 14's visible change hide inside a parity claim.

### 8. Surface positions

Matching PRODUCT-0092 §5, PRODUCT-0095 §9, PRODUCT-0098 §8, and PRODUCT-0100 §8:
free constructor inside `figure`/`view`/`relation.views` accepted; `layers([...])`
and both method-style spellings refused.

### 9. The exhibit demonstrates the whole workflow

`uat/pie_probe.sql` and `uat/test32.rn`:

- raw transaction rows containing **duplicate categories**;
- Rune executes `.aggregate([col("category")], [sum(col("value"))])`;
- `.project_named(...)` restores `category` / `value`;
- an explicit `.sort(...)` chooses **clockwise order**, per PRODUCT-0102 §4;
- the aggregated relation renders;
- the **raw companion refuses**, at the `category` argument, naming
  `.aggregate(...)`.

This is what PRODUCT-0102 owed and could not provide: pie was producerless, so
the record that removed the mark's aggregation had no authored figure in which to
show the replacement. The last bullet is the discriminating one — without it the
exhibit shows a pie that works and cannot distinguish "the author aggregated" from
"Polariton did."

### 10. What this record does not do

No ungrouped Rune form (§1). No colour authoring. No changes to `PieSlices` or
its refusals (0102). No arc-contract work (0101). No new projection arm (§2).
**No path splitting** — the mixed separator/silhouette path stays as it is, per
Context; separating it is geometry work that would forfeit §7's pixel parity.

## Implementation sequence

**Stage 0 — complete.** Findings in Context. Bank every pie figure against the
PRODUCT-0102 baseline, plus a session round-trip with no pie in it.

**Stage 1 — resolver, transport, word.** §§1–3.

**Stage 2 — colour.** §§4–5, both enumerations.

**Stage 3 — the family.** §6.

**Stage 4 — role corrections.** §7.

**Stage 5 — the exhibit.** §9.

## Acceptance gates

1. **`pie("category", "value")` renders**, and any other arity refuses naming the
   expected one.
2. **No ungrouped Rune form exists.** No spelling produces a pie without a
   category channel — asserted over the registered surface, per §1, since the
   claim is about what can be written.
3. **Automatic ungrouped pie still renders.** The substrate's optional category is
   unchanged: **preparation, geometry, and marks are identical**, and the only
   change is §7's guide-policy consequence — a lone synthetic legend is now
   omitted. An earlier draft said "legacy figures are unaffected," which
   contradicts §7. Paired with gate 2: the capability is retained where it exists
   and not exposed where it does not.
4. **Category accepts nominal and refuses numeric; value accepts quantitative and
   refuses string.** Both halves each.
5. **Every 0102 refusal carries its argument span**, per §3's table.
6. **The duplicate diagnostic names `.aggregate(`**, reaching the host with the
   `category` span rather than a whole-figure fallback.
7. **Projection carries both fields with no new arm.** Third consecutive consumer
   of PRODUCT-0094's exhaustive `all()` without one.
8. **Authored categories never reach the fallback**, and **paint and legend share
   one accessor** — a slice's fill equals its legend swatch, asserted through the
   accessor rather than by comparing two computed values.
9. **Both `authored_categories` enumerations name pie.** Two structural
   assertions, `spec/mod.rs:2566` and `:2652`. Updating one alone produces a pie
   that allocates identity on one path and falls back on the other, and **both
   render plausibly** — so a behavioural gate cannot separate them.
10. **`AuthoredFamilyId::PIE` exists; `ALL` has seven members.**
11. **`ExtensibleFamilies` admits and round-trips it; both closed policies refuse
    it.** Three cases — the refusals are what test the bounded design.
12. **`from_authored_layer` gains exactly one arm**, and both `unreachable!` arms
    keep their distinct messages.
13. **The sector stroke resolves through `mark_style.separator`.** **Structural**,
    per §7: the values coincide under every current projection, so a pixel gate
    passes trivially and proves nothing about the role.
14. **`Row N` provenance is `Synthetic`, and the omission is PRODUCT-0044's, not
    pie's.** Four parts:

    - the pie kernel **emits** a lone `Synthetic` entry — it does not return
      `None`;
    - shared `apply_policy` **omits** it (`legend.rs:91`);
    - a two-row pie **emits two** synthetic entries;
    - and `apply_policy` **retains** them.

    The emit/omit split is the point. A pie-local `None` produces the identical
    visible outcome while moving the omission decision into the kernel — a second
    guide authority beside PRODUCT-0044, of exactly the kind §5 catches for
    colour. Asserting only the rendered result cannot tell the two apart.
15. **Assignments persist and restore**, and reattachment restores the same
    allocation.
16. **Existing documents are undisturbed.** A session with no pie round-trips
    byte-identically after §6's additions.
17. **The exhibit's aggregated relation renders and its raw companion refuses.**
    Both halves in one exhibit, per §9 — the refusal is what distinguishes an
    author who aggregated from a mark that did.
18. **Slice order follows `.sort(...)`, asserted automatically.** An **automated
    pair**: the same uniquely-valued aggregate rendered under **opposite sort
    orders** produces reversed slice order, proving PRODUCT-0102 §4's
    relation-order rule is reachable from Rune. Not a manual mutation of the
    exhibit — the UAT stays focused on aggregate-versus-refusal (§9), and ordering
    is proved where it can be run.
19. **Surface positions match §8 exactly.** Six cases.
20. **Pie hovers in a non-origin facet cell.** PRODUCT-0097's and PRODUCT-0102's
    geometry repairs hold for an *authored* pie, which did not exist when either
    landed.

## Stop conditions

> If the **PRODUCT-0086 persistence path** costs more than one token and one arm,
> stop. Scoped to persistence deliberately: §5 already shows colour costs two
> enumerations, and the surface as a whole costs a word, a transport variant, a
> resolver, and diagnostics. Only persistence carried the one-token-one-arm claim.

> If a third `authored_categories`-style enumeration appears, stop and record it.
> §5 found two; a third means colour-consumer registration is diffuse enough to
> need its own authority rather than another list to remember.

> If correcting the sector stroke moves a pixel, stop. §7's premise is that
> `chrome.canvas_background` and `mark_style.separator` coincide under every
> current projection; a moved pixel means they do not, and that is a projection
> finding rather than a role correction.

> If exposing an ungrouped Rune form becomes necessary to render some legacy
> figure, stop. §1's reasoning is about durable identity, and a legacy figure
> needing the surface form means automatic resolution is not covering a case it
> was assumed to cover.

## Supersession

Supersedes no record. Consumes PRODUCT-0102's `PieSlices`, PRODUCT-0101's arc
contract, and PRODUCT-0094's transport unchanged. **Third categorical consumer of
PRODUCT-0086**, extending `AuthoredFamilyId::ALL` from six to seven. Discharges
the exhibit PRODUCT-0102 §"Prerequisite C" owed. Corrects two role
misattributions: the separator with pixel parity, the `Row N` provenance with a
deliberate change — a lone synthetic legend is now omitted.

## Rejected

**Expose an ungrouped `pie("value")` form, matching radial dial.** Attractive
because the substrate accepts it and PRODUCT-0100 exposed the analogous form.
Rejected because an ungrouped dial is *one* reading with one label while an
ungrouped pie is arbitrarily many anonymous slices: `Row N` labels shift when a
row is inserted, taking their colours with them, and the form bypasses the
categorical capacity that named slices enforce.

**Make the automatic fallback label-keyed so `Row N` colours are stable.**
Attractive because it would remove §1's objection. Rejected for PRODUCT-0098 §6's
reason — a second colour allocator beside PRODUCT-0086 — and because stabilizing a
*positional* label is stabilizing the wrong thing: `Row 2` means a different
slice after an insertion no matter what colour it keeps.

**Assert the sector-stroke correction with a pixel-parity gate.** Attractive
because parity is exactly what §7 claims. Rejected because the values coincide, so
the gate passes whether or not the role changed — it would be a vacuous assertion
of a true fact, which is the failure this arc has caught repeatedly.

**Keep `Row N` provenance as `Named` to preserve the one-row legend.** Attractive
because omitting a legend is a visible loss. Rejected because a lone positional
placeholder is a legend that names nothing, which is what PRODUCT-0044's omission
rule exists for.

**Add the remaining categorical marks' tokens while `ALL` is open.** Attractive as
a small diff. Rejected for PRODUCT-0091 §10's and PRODUCT-0098's reason: a token
with no mark behind it is unobservable and its gates vacuous.

## Consequences

Pie becomes authorable, grouped only, and the mark that had the strongest case for
aggregating now demonstrates the alternative end to end — raw rows, an explicit
`.aggregate(...)`, an explicit `.sort(...)`, and a companion that refuses. That
exhibit is the argument PRODUCT-0102 made and could not show.

PRODUCT-0086's third consumer confirms the reusable path and bounds it. Persistence
is one token and one arm, as claimed. **Colour is two enumerations**, which no
earlier record had reason to notice — and which is why 0100's claim was scoped to
persistence rather than stated generally. A generalization would have been wrong
in a way that renders plausibly.

Two role misattributions are corrected, and they need different evidence. The
**separator** moves no pixel, because `chrome.canvas_background` and
`mark_style.separator` coincide under every current projection — so gate 13 must
be structural, since a pixel assertion would pass without the role changing at
all. That is the shape PRODUCT-0080 established: roles separate by meaning even
when resolved colours coincide, and a gate that can only see colour cannot see
the separation.

The **`Row N` provenance** is the opposite: it changes what renders, removing a
lone synthetic legend, and gate 14 asserts the emit/omit split precisely because
the visible outcome alone cannot distinguish PRODUCT-0044 doing the omitting from
pie doing it.

Two marks remain: radar with parallel over prerequisite **D**, and violin.
