# ADR 0033: order-sensitive plot skins and explicit duotone

Status: proposed 2026-08-06. This record corrects PRODUCT-0032's mandatory
set-based projection. Ordinary plot skins preserve resolved palette ordering;
the accepted strongest-pair treatment remains available through an explicit
`.duotone()` modifier.

## Context

The first live artifact exposed an information-loss defect in PRODUCT-0032.
`skin("tab20c_s5")` and `skin("tab20c_s8")` render the same chrome because the
mandatory projection consumes only the palette's globally most contrasting
pair. Reverse and shift preserve the set, so the plot surface collapses much of
the compact color space expressed by libaryon's theme grammar.

That collapse is useful when requested: a quiet paper/ink pair is a good plot
treatment. It is not an honest default for ordinary `skin(name)`. The modifier
must name the lossy projection so the unmodified spelling continues to expose
theme-engine order.

## Decision

The two figure spellings are:

```rune
skin("tab20c_s8")             // order-sensitive
skin("tab20c_s8").duotone()  // explicit set-based strongest pair
```

Ordinary skin uses the resolved palette's semantic background position (slot
19, libaryon's `Whitespace` position) as its ordered anchor and chooses that
anchor's most contrasting mate from the same resolved palette. The pair is
oriented darker-paper/lighter-ink and completed through PRODUCT-0032's existing
alpha policy and eight contrast checks. Thus reverse and shift can change the
anchor and completed chrome without spending unrelated swatches across roles.

Explicit Duotone preserves PRODUCT-0032's current global-most-contrasting-pair
projection exactly. Set-preserving reverse and shift may intentionally collapse
under this spelling. Clipping remains observable when it changes the set.

`duotone()` takes no arguments, may appear once, and is valid only immediately
after a direct figure `skin(string_literal)` member. It is not a figure body,
mark modifier, standalone member, or ordinary plot setting. The authored normal
form carries the theme identity plus one closed projection choice; no resolved
palette or chrome enters the envelope.

Both projections validate chrome against chrome only. Neither colors marks,
data swatches, ramps, guide contents, or application chrome. Omitted skin still
uses PRODUCT-0031's exact compatibility default. Projection failure remains
destination-local and stale-safe.

## Acceptance gates

1. `skin(name)` selects the ordered projection and
   `skin(name).duotone()` selects explicit Duotone.
2. Modifier arity, duplication, receiver, contextual-position, and literal-name
   refusals are exact and span-attributed.
3. `tab20c_s5` and `tab20c_s8` ordinary skins produce different complete valid
   chrome; their explicit Duotone forms are identical.
4. `viridis_r -> viridis_s5` changes both ordinary skin chrome and ordered data
   colors while positions stay fixed; explicit Duotone keeps chrome fixed while
   the data colors change.
5. Nord's ordinary base projection remains byte-for-byte equal to the accepted
   PRODUCT-0032 Nord chrome.
6. Every successful result passes five 4.5:1 text and three 3:1 non-text checks;
   failures are structured and never fall back.
7. Skin/palette, mark, Guide, host-chrome, identity, lifecycle, and omitted-skin
   boundaries from PRODUCT-0032 remain unchanged.

## Consequences

- Theme modifiers remain materially expressive by default.
- Duotone remains available as an explicit, intentionally lossy treatment.
- Plot chrome stays visually restrained to one paper/ink pair in either mode.

## Supersession

This record supersedes PRODUCT-0032 §3-4 only where those sections require one
mandatory set-based projection and reject `.duotone()`. All other PRODUCT-0032
decisions remain accepted.
