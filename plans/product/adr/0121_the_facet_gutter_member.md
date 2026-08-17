# ADR 0121: the facet gutter member

Status: proposed 2026-08-16. PRODUCT-0110 made the space between facet cells
an authored figure-grid reservation, but exposed it as the independent word
`facet_gutter(px)`. A gutter is a property of the facet member that creates
the grid. This record moves only that spelling to `facet("field").gutter(px)`;
it preserves PRODUCT-0110's two-tier cascade, geometry authority, default, and
safety clamp.

It follows PRODUCT-0120's padding member. Padding established that a real
figure concept can own named edge modifiers without reconstructing a broader
carrier. Facet is the stronger instance: it already owns the grid that has the
gutter.

## Context

The current surface permits this disconnected pair:

```rune
figure([
	facet("region"),
	facet_gutter(24),
	bar("quarter", "sales"),
])
```

The relationship is meaningful only because `facet("region")` creates a grid.
Reading the workbook requires the author to connect the two words themselves.
The member spelling makes that ownership explicit:

```rune
figure([
	facet("region").gutter(24),
	bar("quarter", "sales"),
])
```

The modifier is still spellable on unrelated figure members. `RuneFigureElement`
is one runtime type for text and other figure furniture, and `RuneSkin` carries
the analogous associated-method surface. Thus this record cannot make an
invalid receiver unrepresentable. It can, however, move the error from a
figure-wide post-decode check to the modifier call that caused it:

```rune
title("Sales").gutter(24) // .gutter(...) refuses here
skin("greys_rs2").gutter(24) // .gutter(...) refuses here
```

That locality matters. The old error says a complete figure lacks `facet(...)`;
the new error says the receiver is not a facet, at the exact modifier span.

PRODUCT-0110's `facet_gutter` carries a scalar authored override into
`FigureGridLayoutSettings`. That derivation is already shared by paint, hover,
extent, clips, and facet labels. It is not under review here.

## Decision

### 1. `facet(...).gutter(px)` is the sole Rune spelling

`gutter(px)` is an associated modifier on the `facet(field)` figure member. It
accepts one finite, non-negative canvas-pixel value and may occur once. It
lowers to the existing authored `facet_gutter` value without creating another
settings carrier or changing the existing default of 12 px.

`facet_gutter(px)` is removed rather than retained as an alias. The repository
has no external compatibility boundary for this authoring vocabulary. Keeping
both forms would create two spellings for one figure-grid reservation and make
future examples choose arbitrarily between them.

### 2. Wrong receivers refuse at their modifier spans

Because the associated method is spellable on all figure members, `.gutter(px)`
checks that its receiver is `facet(...)` immediately. A text-bearing member and
a non-text member both refuse at the modifier call span. The error is not
deferred until figure assembly, and it does not claim that a later sibling
could repair the receiver.

The modifier register retains an origin slot for `gutter`, so a second
`.gutter(...)` refuses at the second modifier span. The value is validated by
the same finite non-negative rule PRODUCT-0110 already established.

### 3. PRODUCT-0110's grid semantics are unchanged

The new modifier writes exactly the authored figure-grid value that
`facet_gutter(px)` wrote. `FigureGridLayoutSettings`, `grid_cell_bounds`, the
application-default → authored-figure cascade, and its per-axis safety clamp
remain unchanged. This record introduces no plot padding, cell-local inset,
or GUI preference.

The existing figures migrate their source spelling:

```rune
facet("facet").gutter(0)
```

`test36.rn` remains the authored gutter exhibit; `test37.rn` keeps its zero
gutter because unequal facet domains make shared grid geometry observable.

## Supersession

This record supersedes PRODUCT-0110 §1's public `facet_gutter(px)` spelling.
The underlying figure-grid setting, its two-tier cascade, and every geometry
decision in PRODUCT-0110 remain current; only the Rune producer moves to
`facet(...).gutter(px)`.

It also supersedes PRODUCT-0110 §1's figure-scope applicability refusal. That
refusal does **not** disappear: Rune still permits `.gutter(...)` to be spelled
on unrelated figure members. PRODUCT-0121 §2 replaces the deferred question
“does this complete figure contain a facet?” with the local question “is this
modifier's receiver a facet?”, attributed at the `.gutter(...)` call that
caused it.

## Acceptance gates

1. **Spelling-only parity.** `facet("f").gutter(24)` produces byte-identical
   cell bounds, clips, transforms, hover routing, and figure-grid settings to
   the banked PRODUCT-0110 `facet("f"), facet_gutter(24)` figure. This rejects
   a surface migration that re-derives or relocates the grid reservation.
2. **Receiver-local refusal.** `title("T").gutter(4)` and
   `skin("greys_rs2").gutter(4)` both refuse at their own `.gutter(4)` spans.
   The two receiver kinds prevent a check limited to the text furniture that
   PRODUCT-0113 already modified.
3. **One spelling remains.** `facet_gutter(24)` is an unknown Rune spelling;
   it is not a deprecated alias, hidden rewrite, or second producer of the
   authored grid gutter.
4. **Modifier admission remains exact.** A second `.gutter(...)` refuses at
   the second call; negative and non-finite values refuse; zero and an
   oversized finite value reach the existing PRODUCT-0110 request-versus-clamp
   path unchanged.
5. **No facet-grid authority moves.** Structurally, the modifier lowers to the
   existing authored `facet_gutter` field, and no new grid-bound calculation,
   padding field, or figure-grid cascade tier appears.

## Stop conditions

> If the member spelling cannot reproduce PRODUCT-0110's geometry byte for
> byte, stop. That would make this a grid-layout record rather than a surface
> record.

> If a receiver-local refusal needs a whole-figure scan to decide, stop. The
> only benefit over the old spelling is immediate attribution at the modifier.

> If a compatibility requirement for `facet_gutter(...)` appears, stop and
> record an explicit migration policy rather than silently keeping two
> authorities.

## Rejected

**Keep `facet_gutter(...)` as an alias.** Rejected because it leaves two
authoring paths for one setting without an external compatibility need.

**Make `.gutter(...)` unavailable on non-facet receivers.** Rejected because
Rune's associated-method model still makes the form spellable across the
runtime receiver types. A precise modifier-span refusal is the honest contract.

**Move the gutter into `PlotPadding`.** Rejected again: PRODUCT-0110 already
showed that a shared inter-cell reservation is not an inside-each-cell inset.

## Consequences

Facet workbooks read in ownership order: the member that declares the facet
also declares the optional space between its cells. PRODUCT-0110 remains the
authority for what that space means; this record only removes an unnecessarily
global spelling and improves the error's locality.
