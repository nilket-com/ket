# ADR 0123: the axis member

Status: proposed 2026-08-17. Eleven top-level Rune words share an `x_` or `y_`
prefix and all mean the same thing: *a property of that axis*. The prefix is a
receiver spelled as a namespace. This record gives the axes real members and
retires the eleven words.

It follows PRODUCT-0121's `facet(...).gutter(px)` and PRODUCT-0120's
`padding()`, and applies the same rule at the largest remaining site:
**settings live on the member that owns them.**

It is a spelling change. No geometry, no cascade, no resolution semantics move.

## Context

### The prefix is doing a receiver's job

| current word | sites | files |
|---|---|---|
| `y_tick_step(n)` | 11 | test21, test25, test26 |
| `y_bounds(a, b)` | 9 | test21, test25, test26, test35 |
| `x_tick_step(n)` | 8 | test21, test25 |
| `x_bounds(a, b)` | 6 | test21, test25 |
| `y_minor_ticks(n)` | 4 | test21 |
| `x_minor_ticks(n)` | 4 | test21 |
| `y_label(s)` | 2 | test26 |
| `x_tick_rotation(d)` | 1 | test37 |
| `x_label(s)` | 0 | — |
| `x_tick_size(n)` | 0 | — |
| `y_tick_size(n)` | 0 | — |

Forty-five call sites across five fixtures. The migration is small **now** and
grows with every figure written, which is the argument for doing it before the
next surface record rather than after.

### PRODUCT-0122's no-argument handles were a workaround for this

PRODUCT-0122 added bare `x_label()` and `y_label()` so an author could size an
axis title without authoring its text — because the only path to the size ran
through the content member. That is a symptom of the missing receiver, not a
feature: with an axis member owning both, `x_axis().label_size(18)` sizes
derived content directly and the handle has nothing left to do.

`title()` and `subtitle()` keep their handles. Those members have no owning
parent to move a size onto, so the workaround is the right shape there.

### The axes are not symmetric, and that is deliberate

There is no `y_tick_rotation`. PRODUCT-0115 exposed rotation for x only,
because the bottom band's derivation is rotation-aware and the left band's is
not: a rotated y tick label's horizontal extent would have to feed
`derived_left`, which today takes the estimated label width unrotated
(`common.rs:1497`).

The eleven retired words become **thirteen modifiers across two members**:
`x_axis()` carries seven — `label`, `label_size`, `bounds`, `tick_step`,
`minor_ticks`, `tick_size`, `tick_rotation` — and `y_axis()` carries six, that
list without `tick_rotation`. (`label_size` is new to each, arriving from
PRODUCT-0122's `.font_size()` modifier rather than from a retired word.) §8
states the asymmetry so the gap reads as a decision rather than an omission.

## Decision

### 1. `x_axis()` and `y_axis()` own every axis property

Two levels, flat modifiers:

```rune
x_axis().label("Quarter").label_size(18),
x_axis().tick_size(12).tick_rotation(45),
y_axis().label_size(20).bounds(0, 100),
y_axis().tick_step(25).minor_ticks(4),
```

The mapping is mechanical — `<axis>_<property>(args)` becomes
`<axis>_axis().<property>(args)` — with one exception in §4.

Three levels were rejected: with an axis owning both a label and ticks,
`x_axis().ticks().font_size(n)` and `x_axis().label().font_size(n)` say the
same thing in four levels that two flat modifier names say in two.

### 2. Separate axis calls compose; a duplicated property refuses across them

Existing fixtures author axis settings as independent figure members, and the
migration must not force them into one chain:

```rune
x_axis().bounds(0, 10),
x_axis().tick_step(2),
```

Two calls carrying disjoint properties are legal and resolve **identically** to
one chained call. This keeps the migration mechanical — each retired word
becomes its own `x_axis().<property>(...)` element — rather than requiring
fixtures to be restructured.

The converse must also hold: a property authored twice refuses, **whether the
duplication is within one chain or across two separate calls**. Composition
merges disjoint properties; it never silently lets a later call overwrite an
earlier one.

Both halves are gated, because an implementation can fail in either direction —
rejecting disjoint separate calls, or accepting a duplicate spread across two
of them.

### 3. The eleven words are retired, with no aliases

`x_bounds(...)` and its ten siblings become unknown spellings. A deprecated
alias would be a second authority on one setting, and the language has no
external consumers to migrate.

### 4. `label_size` is a sibling of `label`, not a modifier on it

```rune
x_axis().label("Quarter")    // content
x_axis().label_size(18)      // size, whether content is authored or derived
```

This is the one place the mapping is not mechanical, and it is the point of the
record. Sizing derived content never has to manufacture a content member, so
PRODUCT-0122's `x_label()`/`y_label()` handles are **removed**: the axis member
subsumes them.

The three states PRODUCT-0122 §2 established survive unchanged — absent,
size-only, content-authored — and are now expressible without a bare handle.

### 5. `label_size`/`tick_size` here, `font_size` there — the difference is real

`title().font_size(24)` is correct because the member *is* the text.
`x_axis().font_size(18)` would be ambiguous because an axis has two kinds of
text, so the modifier must name which one.

The naming difference tracks modifier-on-text versus property-of-container. It
is stated here so a later consistency pass does not "unify" them into
`x_axis().font_size(...)` and reintroduce the ambiguity.

### 6. `tick_size` is the tick **label** text size

Ticks have marks and labels. Only the labels are authorable, and `tick_size`
means their text size — the same value `x_tick_size` meant.

If tick-mark geometry ever becomes authorable it needs a distinct name,
`tick_length`, because `tick_size` is taken by typography. Recording this now
costs a sentence and prevents a collision that would otherwise be discovered
during a later record's Stage 0.

### 7. A bare `x_axis()` refuses

Per PRODUCT-0122 §3: a member whose bare form has no effect refuses at its own
span. `x_axis()` and `y_axis()` carrying no modifier change nothing and are
indistinguishable from an unfinished line.

### 8. `y_axis()` has no `tick_rotation`, deliberately

Adding it for symmetry would require the left band to derive from a rotated y
tick extent, which it does not (`common.rs:1491-1497`). That is the same
blocker PRODUCT-0113 applied to unreserved roles: expose a setting only when a
reservation tracks it.

Gate 8 pins the absence structurally, so symmetry cannot be restored without
the derivation that would make it honest.

### 9. Refusals move from the word's span to the modifier's

`x_bounds(0, "x")` refuses today at `x_bounds(...)`. After this record,
`x_axis().bounds(0, "x")` refuses at `.bounds(...)`, with the member call
remaining as context. Strictly finer.

Any word that currently refuses at **figure scope** rather than at its call
becomes receiver-local, as `facet_gutter` did in PRODUCT-0121. Stage 0
enumerates which of the eleven behave that way; the record claims narrowing,
not elimination.

## Implementation sequence

**Stage 0 — bank and enumerate.** Render all five fixtures at current
spellings. Enumerate, per word, whether its refusals are call-scope or
figure-scope, so §9's claim is measured rather than assumed.

**Stage 1 — the members.** `x_axis()`/`y_axis()` with all modifiers, lowering
to the existing slots. The eleven words still work; nothing is retired yet.

**Stage 2 — migrate the fixtures.** All 45 sites across five files, with
Stage 0's captures as the parity oracle.

**Stage 3 — retire.** Delete the eleven words and PRODUCT-0122's two
no-argument label handles.

## Acceptance gates

1. **Spelling-only parity.** Each of the five migrated fixtures renders
   byte-identically to its Stage 0 capture. This is the gate that keeps the
   record a surface change.
2. **The eleven words are gone.** Structural: each refuses as an unknown
   spelling, with no alias, deprecation shim, or retained recogniser entry.
3. **The two label handles are gone.** Structural, per §4: bare `x_label()`
   and `y_label()` no longer parse. `title()` and `subtitle()` still do — both
   halves, so a sweep that removes all four fails.
4. **Size without authoring content.** `y_axis().label_size(20)` renders the
   kernel-derived label at 20. This is PRODUCT-0122 gate 3's property surviving
   the change of spelling; a `Show("")` implementation blanks the label and
   passes any size assertion.
5. **Content and size stay independent.** `y_axis().label("Revenue")` alone,
   `.label_size(20)` alone, and both together each produce the expected pair.
   Three cases, since two prove neither independence nor the default.
6. **Bare members refuse at their own span.** `x_axis()` and `y_axis()` with no
   modifier, per §7.
7. **Duplicates refuse at the second span.** `y_axis().bounds(0,1).bounds(0,2)`,
   matching the register PRODUCT-0113 added to `RuneFigureElement`.
8. **`y_axis()` has no `tick_rotation`.** Structural:
   `y_axis().tick_rotation(45)` refuses, and no `y_tick_rotation` field is
   introduced. The asymmetry is enforced, not merely documented. Per §8.
9. **Wrong receivers refuse at the modifier span.** `skin("greys_rs2")
   .bounds(0, 1)` and `x_axis().gutter(4)` both refuse where they are written.
10. **No authority moves.** Structural: each modifier lowers to the same
    settings slot its retired word wrote. No modifier introduces a second path
    into axis resolution.
11. **Disjoint separate calls compose; duplicates refuse across them.** Two
    `x_axis()` members carrying different properties resolve identically to one
    chained call carrying both; two carrying the *same* property refuse at the
    second. Per §2, and both halves, since an implementation can reject the
    legal case or accept the illegal one.
12. **Attribution narrows as claimed.** For each word Stage 0 recorded as
    figure-scope, the equivalent modifier refuses at its own call instead. Per
    §9, measured against Stage 0's enumeration rather than asserted.

## Stop conditions

> If any fixture cannot be migrated without a rendering difference, stop. §1's
> spelling-only claim is the whole scope; a geometry change hiding inside a
> rename is the shape PRODUCT-0064's halt exists to catch.

> If a modifier needs a settings slot that its retired word did not write,
> stop. That is a new capability wearing a migration's clothes and belongs in
> its own record.

> If `y_axis().tick_rotation(...)` is wanted during implementation, stop. It
> needs the left band's derivation to become rotation-aware first, which is a
> geometry record, not a spelling one.

## Supersession and inheritance

Supersedes the eleven axis words across six records: PRODUCT-0036 (numeric
axis bounds), PRODUCT-0038 (tick steps), PRODUCT-0039 (minor tick
subdivision), PRODUCT-0085 (axis titles), PRODUCT-0115 (x tick rotation), and
PRODUCT-0122 (tick sizes). It also removes PRODUCT-0122's `x_label()` and
`y_label()` no-argument handles. Every underlying setting, cascade tier,
resolution rule, and geometry decision in those records remains current; only
the Rune producer moves.

Inherits unchanged: PRODUCT-0122 §3's bare-member rule, PRODUCT-0113's modifier
register on `RuneFigureElement`, PRODUCT-0114's band derivations, and
PRODUCT-0115's x-only rotation decision.

The legend consolidation — `legend()` absorbing `legend_at`, `hide_legend`, and
a future `legend_size` — inherits this record's member-ownership rule but is
**not** attempted here. It remains blocked on separating `facet_label_size`
from `legend_size`, which is a substrate defect rather than a spelling one.

## Rejected

**Three levels: `x_axis().ticks().font_size(n)`.** Attractive because ticks are
genuinely an object with several properties. Rejected because it says in four
levels what two flat modifier names say in two, and because `x_axis()
.font_size(n)` would then be ambiguous rather than absent.

**`x()` instead of `x_axis()`.** Attractive for terseness. Rejected because
every member in the language is descriptive — `facet`, `padding`, `title`,
`skin` — and a single letter reads as a variable at the call site.

**Keep the words as aliases.** Attractive because it needs no fixture
migration. Rejected because two spellings for one setting is a second
authority, and the migration is 45 sites in five files today.

**Put `label_size` on a `label()` handle.** Attractive for symmetry with
`title().font_size(n)`. Rejected per §4: it would preserve the workaround this
record exists to remove, and force sizing derived content to manufacture a
content member.

**Add `y_axis().tick_rotation()` for symmetry.** Attractive because asymmetric
surfaces invite bug reports. Rejected per §8: the left band does not derive
from a rotated y extent, so the setting would resolve and collide.

## Consequences

Eleven top-level words become two members. The figure namespace stops carrying
a per-axis prefix, and an author reading `x_axis().tick_size(12)` can see what
owns the setting without knowing the language's naming convention.

PRODUCT-0122's no-argument label handles lived for less than a day. They were
not a mistake — under that record's surface they were the only honest way to
size derived content — but they were a workaround for an absent receiver, and
the receiver's arrival makes them redundant. That is the second time in this
arc a record was superseded by the one that removed its reason to exist, after
PRODUCT-0118.

The x/y asymmetry is now enforced rather than incidental. It was invisible
while both axes were flat word families; a member makes it a visible gap, which
is why §8 states the reason and gate 8 keeps it from being closed by symmetry
alone.
