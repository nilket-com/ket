# ADR 0109: palette-kind decides categorical allocation

Status: proposed 2026-08-15, **redrafted the same day**. A first draft proposed an
opt-in ordered mode with a Rune spelling. That was wrong: reading the allocator
showed the ordered behaviour **already exists**, and only lineage retention
overrides it. This record instead keys allocation on the **kind of palette named**
— continuous-derived palettes recompute in the mark's visible category order and hold no
durable assignments, curated categorical palettes keep separation-first
durability.

No new Rune word, no author-facing mode, no opt-in trade.

## Context

### The allocator already increments; retention overrides it

Bar and violin call the **same allocator**, and on a fresh lineage it assigns in
the category sequence it receives, evenly spread across the palette:

| categories | assigned positions (20-swatch ramp) |
|---|---|
| 4 | `0, 6, 13, 19` |
| 7 | `0, 3, 6, 10, 13, 16, 19` |

Both monotonic. That **is** "increment across the theme" only if the allocator
receives the sequence the mark will show.

The first `uat/test35.rn` repair exposed a second cause of a shuffled violin:
the categorical resolver preserved the post-join physical row order, while
PRODUCT-0107/0108's density collection renders group labels lexically. The
allocator walked the ramp correctly in an invisible order and the kernel showed
those assignments in another. Retained `ColorIdentity` assignments are one
possible cause of disorder; mismatched category authorities are another.

### The screenshots were evidence of state, not of policy

Two hypotheses were proposed from the figures — that bar and violin allocate from
differently-ordered inputs, and that four categories versus seven interacts with
slot count. **Both were wrong.**

The figures showed a *stateful* result read as a pure function of palette and
categories. The same figure on a fresh lineage renders monotonically, so no
additional screenshot could have corrected the inference, and a proposed
four-cohort experiment would have returned whichever answer that lineage happened
to hold. Only reading the allocator settled it.

Recorded because the arc has repeatedly found what gates could not express *by
looking at figures* — and this is the counter-case, where looking harder was the
wrong instrument.

### libaryon already declares the distinction

`ThemeSpec` has three kinds: **32 `continuous`**, 13 `curated`, 8
`curated_categorical` (`libaryon/src/engine/theme.rs`). `blues` is `continuous`
(`:1745`), and `CategoricalPaletteSelection::from_theme_name` derives a
categorical palette *from* it (`spec/mod.rs:1272`).

So the property this record keys on is already stated by the theme. Nothing new
needs authoring.

## Decision

### 1. Allocation follows the palette's kind

| palette kind | fresh allocation | continued allocation | durable assignments |
|---|---|---|---|
| **continuous-derived** (`blues`, `reds`, `greys`) | even, in visible category order | **recomputed** — always fresh | **none** |
| **curated categorical** (`dark2`, `tab20`) | even, in visible category order | **maximin into vacant positions**, preserving prior categories | **retained** |

**Fresh allocation is identical for both kinds** — even, in visible category order. An
earlier draft described curated as "separation-first," which overstates: maximin
applies only when *adding to a retained lineage*. The distinction this record
makes is entirely about **continuation**, and the promise that fresh behaviour is
unchanged holds for both columns.

A continuous ramp's order is its entire content, so a category's *position* and
its *colour* are the same fact — there is nothing durable to persist that is not
already implied by the current domain. A curated categorical palette's swatches
have no intrinsic order, so identity across changing membership is the only
meaning position carries, and separation-first is right.

### 2. The kind is carried, never inferred from the name

`CategoricalPaletteSelection` cannot currently see it. `Theme`
(`libaryon/src/engine/theme.rs:443`) retains `syntax_colors`, `palette`, and
`categorical_palette` — the kind is dropped before the selection is built, even
though `ThemeFamily` exists (`:1656`) with `ThemeSpec::family()` (`:1711`).

So libaryon must expose a small **allocation-kind value**, carried into the
selection and through to the allocator.

**It must never be inferred from the identity string.** Matching `"blues"` or
`"dark2"` would reproduce PRODUCT-0091 §2's rejected shape one layer down — a
name deciding a behaviour the catalogue already declares — and would break for
every theme modifier the parser accepts (reverse, clips, shifts) as well as for
aliases such as `greys`/`grays`.

### 3. Continuous-derived figures recolour on membership change, inherently

Drop a category and later ones shift. This is not a trade an author opts into —
it follows from §1: if allocation is a function of the current visible category domain, a changed
domain changes the allocation.

Stated so it is not later mistaken for a regression.

### 4. A palette-kind switch replaces atomically

| switch | behaviour |
|---|---|
| curated → continuous | **discard** that lineage's persisted assignments; recompute from current visible category order |
| continuous → curated | begin a **fresh** durable allocation from the current visible category order |

**Never retain inactive assignments beside a recomputing resolver.**

A palette edit deliberately changes colour semantics, so refusing the switch would
make an ordinary author action brittle. But retaining stale assignments while the
resolver recomputes is the two-authority failure this arc has removed five times:
two sources of a category's colour, disagreeing only when membership changes, both
rendering plausibly.

Atomic replacement is the only option that is neither brittle nor silently
inconsistent.

### 5. No Rune spelling, no mode

`palette(...)` is unchanged as a word. The kind is a property of the theme it
names, so authors express the choice by choosing a palette — which is what they
were already doing.

The first draft proposed `palette(...)` plus an ordered mode. Rejected below: it
would have asked authors to declare something the theme already declares.

### 6. What this record does not do

No change to the allocator's *fresh-lineage* spacing — it already increments.
No new palettes or theme kinds. No change to PRODUCT-0086's family tokens: a
continuous-derived figure still has a lineage and still allocates a family, it
simply persists no assignments. Each mark provides its declared display sequence
before allocation; violin supplies its PRODUCT-0108 lexical group order instead
of a relation join's incidental row order.

## Acceptance gates

1. **A fresh continuous-derived figure increments in visible order.** Seven cohorts on `blues`
   render `0, 3, 6, 10, 13, 16, 19` — monotonic, matching the allocator's existing
   fresh-lineage behaviour.
2. **An established continuous-derived lineage still increments.** The
   discriminating gate: re-run `uat/test35.rn` against a lineage that already
   holds assignments and get the **same** monotonic result. This fails today, and
   it is the entire defect — gate 1 passes already.
3. **Curated categorical retains identity.** On `dark2`, a category keeps its
   swatch when membership changes. Paired with gate 2, since one rule cannot serve
   both and a gate covering only one would pass an implementation that applied it
   everywhere.
4. **Continuous-derived figures persist no assignments.** Structural: a
   checkpointed `blues` figure writes no `ColourAssignment` rows.
5. **Curated categorical figures still persist assignments.** The pair to gate 4 —
   without it, an implementation that stopped persisting entirely would pass.
6. **Reload with unchanged membership reproduces colours.** Derived afresh for
   continuous, restored for curated; indistinguishable in output, which is why
   gates 4 and 5 must be structural.
7. **Curated → continuous discards.** After the switch, no assignments from the
   prior lineage survive, and colours match a fresh continuous allocation over the
   current domain.
8. **Continuous → curated allocates fresh.** A new durable allocation over the
   current domain, not a revival of anything previously persisted.
9. **No inactive assignments coexist with a recomputing resolver.** Structural,
   per §4 — the state that would render plausibly while being wrong.
10. **Bar and violin agree, given the same history.** The same palette over the
    **same category sequence** and an **equivalent lineage history** produces the
    same assignment in both marks.

    Not "on both fresh and established lineages" — an earlier draft said that, and
    it would fail a correct implementation: bar and violin carry distinct
    `AuthoredFamilyId`s, so independently established histories may legitimately
    differ under §1's curated continuation rule.
11. **The allocation kind is carried, not matched.** Structural: the allocator
    receives a kind value originating in libaryon's theme catalogue, and no code
    path compares a palette identity string to decide it. Per §2 — and asserted
    with a **modified** identity (`blues_r`, or a `greys`/`grays` alias) that a
    string match would classify wrongly.
12. **Family tokens are unaffected.** A continuous-derived figure still allocates a
    `AuthoredFamilyId`; only its assignments are absent.
13. **A scrambled violin relation still walks its visible lexical order.** Given
    post-join rows in `Ember, Fjord, Cascade, Grove, Aurora, Drift, Boreal`,
    `yellows` assigns `Aurora … Grove` positions `0, 3, 6, 10, 13, 16, 19`.
    This distinguishes the mark's category authority from the relation's
    incidental row order.

## Stop conditions

> If the durable layer cannot represent "lineage present, assignments absent"
> without a schema change, stop and record it. The survey says it can — resolution
> already clones `ColorIdentity` and returns the next state atomically, and the
> snapshot format already represents an empty identity, so §4 deletes a lineage's
> assignments **and** its allocation space without a bump. A schema change would
> mean that survey was wrong, which is a finding about PRODUCT-0086 rather than a
> step here.

> If discarding on a palette-kind switch cannot be made atomic — if there is any
> window where old assignments and a recomputing resolver coexist — stop. §3's
> whole content is that the intermediate state must not exist.

> If any curated categorical figure's colours move, stop. §1 changes **continued**
> allocation only for continuous-derived palettes, and fresh allocation for neither; a moved curated colour means the rule is
> being applied on the wrong side of the distinction.

## Rejected

**An opt-in ordered mode with a Rune spelling.** This record's own first draft.
Rejected because it asks the author to declare something the theme already
declares, and because it framed the ordered behaviour as new when the allocator
already produces it on a fresh lineage. It would have added a word to re-obtain a
default.

**Redefine allocation to increment for every palette.** Attractive because it is
one rule. Rejected because a curated categorical palette's swatches have no
intrinsic order, so incrementing through them buys nothing and forfeits the
identity that separation-first exists to provide.

**Refuse a palette-kind switch.** Attractive because it avoids deciding what
happens to persisted assignments. Rejected because editing a palette is an
ordinary authoring action, and refusing it would make a normal edit brittle to
serve an implementation detail.

**Retain persisted assignments through a switch, inactive.** Attractive because
switching back would restore prior colours. Rejected as the two-authority
failure: two sources for a category's colour, disagreeing only when membership
changes, both plausible on screen.

## Consequences

Bar and violin stop disagreeing, and the fix is a deletion rather than a feature —
continuous-derived palettes stop retaining what they should recompute. Violin
also now allocates from the category order it renders, not whichever order an
upstream join happened to emit.

The record's own first draft is the useful artifact here. It proposed a mode,
because the screenshots showed shuffled colours and a mode would have produced
ordered ones. The behaviour was already correct on a fresh lineage only when its
domain and display order agreed. **A figure can show a true fact and support a
false inference**, and this arc's habit of trusting figures is exactly what made
that trap available.
