# ADR 0124: the facet-label typography surface

Status: proposed 2026-08-17. `legend_size` currently sizes two unrelated facts:
legend text and facet headers. A legend is data-derived figure furniture; a
facet header is the label of the member that creates the figure grid. One field
is therefore governing two owners.

This record separates the facet-header role into its own typed setting and
exposes it where it belongs:

```rune
facet("region").label_size(16)
```

It is zero-pixel by default. Production facet headers are already always
16 logical pixels — `facet_label_size` derives `(legend_size + 2).max(10)`,
and every production `legend_size` is 14. The default split preserves that
value exactly while making the two roles independently authorable.

## Context

### `legend_size` is not only a legend setting

`PlotTypography` carries one raw `legend_size: f32`. Its legend consumers are
legitimate: `legend.rs` sizes legend text and estimates its width from that
same value. But `VerticalGuideBands::facet_label_size` also derives

```rust
(settings.legend_size + 2.0).max(10.0)
```

and uses the result twice: the facet-row reservation in `common.rs` and the
centered facet-header draw in `grid.rs`.

The two roles differ in owner, placement, and geometry. An authored
`legend_size(20)` would resize every facet header through a spelling that says
nothing about facets. That is the inverse of the two-authorities failure: one
source purports to own two independent facts.

### The split has an exact default parity value

The survey found no production `PlotTypography` literal and no production
override of `legend_size`. The resolved production value is therefore always
14, and the existing derived facet-header size is always 16. This record gives
the new `FacetLabelTextSize` a default of 16, preserving the facet row height
and every header draw exactly.

The test-only `PlotTypography` literals are deliberately not evidence of a
production theme choice. They are cascade probes and must be updated to name
both independent roles explicitly.

### Facet already owns the right receiver

PRODUCT-0121 established `facet("field").gutter(px)`: a shared inter-cell
reservation belongs to the member that creates the grid. Facet-header
typography is more direct still — it names the grid's own category heading.

`label_size(...)` already exists as an associated Rune modifier for axis
members. The word has the right broad meaning, but its receiver decides which
label role it changes:

```rune
x_axis().label_size(18)       // axis title
facet("region").label_size(16) // facet header
```

It remains spellable on unrelated `RuneFigureElement` receivers because Rune
uses one runtime figure-element type. The modifier must consequently refuse
at its own span on `title(...)`, `skin(...)`, and other non-axis/non-facet
receivers. PRODUCT-0121's local-attribution rule applies; this record does not
claim the invalid state is unrepresentable.

## Decision

### 1. A facet header is its own typed typography role

`FacetLabelTextSize` is a bounded logical-pixel newtype through all three
carriers:

- `PlotTypography`, the atomic theme carrier;
- `PlotSettingsLayer`, as a partial `Override<FacetLabelTextSize>` slot;
- `ResolvedPlotSettings`, consumed by both the facet-row reservation and
  facet-header draw.

Its inclusive range is `6.0 ..= 96.0`, matching every other authorable text
role. Its default is 16.0 — the exact production value of the former formula.
As PRODUCT-0112 established, the specific partial slot wins over the atomic
typography carrier within the same cascade tier.

`facet_label_size(settings)` becomes a typed field read rather than a
derivation from `legend_size`. No `f32` crosses from authoring into geometry;
the only scalar conversion is at the existing canvas-text boundary.

### 2. `facet(...).label_size(n)` is the sole facet-header spelling

`label_size(n)` attaches to the figure's single `facet(field)` member, accepts
one bounded numeric value, and may occur once on that member. It writes only
the new facet-label partial slot:

```rune
facet("region").gutter(24).label_size(18)
```

`gutter` and `label_size` are independent facet properties and compose in
either order. The latter never manufactures a second facet member, grid
authority, or local cell inset.

An axis keeps the meaning PRODUCT-0123 gave it: `label_size` changes an axis
title. The shared modifier spelling is not a shared settings field; receiver
ownership selects the typed role.

### 3. Facet and legend typography become independent

Changing `facet(...).label_size(n)` changes the facet-header draw and the
facet-row reservation by its exact size delta. It does not change legend text,
legend width estimation, or legend placement.

Conversely, the atomic legacy `legend_size` path remains legend-only after the
split: a nondefault theme value must not affect a facet header. It remains raw
and has no Rune spelling in this record. The later legend-member record owns
typing it and moving `legend_at`/`hide_legend`; this record neither anticipates
nor exposes that surface.

### 4. Default geometry is byte-identical

At the new default 16, the facet row reserves exactly the former 16 pixels and
draws text at exactly the former 16 pixels. Figures without facets remain
untouched. This is a surface-plus-carrier record, not another geometry change.

An author who changes the setting deliberately moves the plot area downward by
the header-size delta. This is ordinary layout coupling: the reservation and
the glyphs are two consumers of one resolved fact, so the figure cannot collide
with its own header.

## Implementation sequence

**Stage 0 — bank the fixed default.** Capture a faceted figure and a
facet-plus-legend figure at the production defaults. Confirm the production
theme census remains `legend_size == 14` and the existing facet result is 16.

**Stage 1 — split the carrier.** Add `FacetLabelTextSize`, all three carrier
fields, five-tier resolution, and the two geometry consumers. Set the default
to 16 and remove the formula. The Stage 0 captures must remain byte-identical.

**Stage 2 — route the modifier.** Let `facet(...).label_size(n)` write the
partial facet slot; preserve axis `label_size` routing and add receiver-local
refusals, duplicate refusal, and bounds checks.

## Acceptance gates

1. **Default facet parity.** A banked faceted figure has byte-identical row
   height, plot area, header draw size, and pixels after Stage 1. A non-faceted
   figure is also byte-identical. This catches a 14-vs-16 default swap and an
   unconditional reservation.
2. **One resolved value drives both facet consumers.** At two authored
   `facet(...).label_size` values, the facet-row reservation and header text
   size change by the same exact delta; legend text and its gutter do not move.
   A draw-only implementation overlaps the plot, and a reservation-only one
   leaves the glyph at the old size.
3. **The old coupling is actually gone.** A nondefault atomic
   `PlotTypography.legend_size` changes legend text/estimation but leaves the
   resolved facet-label size and facet row unchanged. This distinguishes a
   split carrier from an added surface that still reads the old formula.
4. **Typed through every carrier.** `FacetLabelTextSize` is present in the
   atomic carrier, the partial override, and the resolved settings. Raw `f32`
   cannot enter the role through a theme or application layer.
5. **Five-tier cascade; partial wins within a tier.** The facet-label role
   resolves through library, application, authored, layer, and pane tiers;
   its explicit partial slot defeats an atomic `PlotTypography` value in the
   same tier. PRODUCT-0112's precedence rule applies unchanged.
6. **The modifier is receiver-local and bounded.**
   `facet("region").label_size(6)` and `...label_size(96)` succeed; `5.9` and
   `96.1` refuse at `.label_size(...)`. `title("T").label_size(16)` and
   `skin("greys_rs2").label_size(16)` refuse at their modifier spans.
7. **Facet composition and duplication stay exact.** `gutter` and
   `label_size` compose in either order on the one facet member. A second
   `.label_size(...)` refuses at its second modifier span. This does not widen
   PRODUCT-0121's one-facet-member rule merely to imitate PRODUCT-0123's
   separate-axis-member composition.
8. **Axes retain their own label-size roles.** Existing `x_axis()` and
   `y_axis()` label-size fixtures are byte-identical, and `label_size` still
   lowers to their respective axis slots. A receiver dispatch that routes all
   label sizes to the new facet field fails.
9. **No legend surface leaks in.** `legend_size` remains absent from Rune, and
   `legend_at`/`hide_legend` remain the only shipped legend spellings. The
   legend-member consolidation is a later decision, not an alias hidden here.
10. **No text measurement capability.** `PlotBackend` gains no text-extent
    method. Header height is a known logical size, so the existing reservation
    is exact in the direction this role needs.

## Stop conditions

> If the production theme census finds a `legend_size` other than 14, stop.
> The claimed parity is then a migration problem across real theme values, not
> a constant split, and its policy belongs in the record before implementation.

> If another production consumer of `facet_label_size` appears, stop and widen
> the owner survey. This record is honest only while the setting names every
> fact it controls.

> If a facet-label size cannot move reservation and draw together through one
> resolved value, stop. A second geometry derivation would recreate the
> coupling this record removes.

> If enabling the modifier needs a generic label member or changes axis
> `label_size` behaviour, stop. `facet(...)` is already the legitimate owner;
> inventing another receiver would be a different surface decision.

## Supersession and inheritance

This record supersedes PRODUCT-0113 §8's decision to leave `legend_size`
unexposed because it also sized facet headers. The reason is discharged for
the facet role only: the two fields are now separate. It does **not** expose or
type the remaining legend role.

It inherits PRODUCT-0121's facet-member ownership and local modifier-span
refusals, PRODUCT-0112's typed partial-override pattern, and PRODUCT-0106's
rule that a reservation tracks its text size. PRODUCT-0123's fieldwise
composition remains specific to its repeatable axis members; it does not alter
PRODUCT-0121's one-facet-member figure grammar.

## Rejected

**Expose `legend_size(n)` now.** Rejected because it would still resize facet
headers under a legend spelling, preserving the defect while making it public.

**Keep the formula and add only a facet modifier.** Rejected because two
authoring paths would compete for one derived result, and a theme legend size
would still move a facet header invisibly.

**Put facet-label size in `PlotPadding`.** Rejected: a header's glyph size is
not an inset. The row reservation derives from the text role; padding only
changes a boundary.

**Give the facet header a top-level `facet_label_size(n)` word.** Rejected per
PRODUCT-0123: `facet(...)` already owns the concept, and a global word would
again make readers connect the property to its owner themselves.

## Consequences

Facet headers become the ninth distinct typography role and the first one owned
by the facet/grid member. The original seven-role request is therefore not
merely completed; its hidden eighth role is made explicit.

The next record is smaller and cleaner: `legend()` can own placement,
visibility, and its newly independent text size without changing facet layout.
