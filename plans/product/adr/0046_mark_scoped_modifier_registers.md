# ADR 0046: mark-scoped modifier registers

Status: proposed 2026-08-07. This record decides one thing: a closed modifier
register attaches to a **mark kind**, not to an encoding arity. Its first
application admits `.point_size(...)` on the three-field category scatter. It
adds no substrate — PRODUCT-0017 already plumbed `PointSize` through every
scatter render and hit path, and PRODUCT-0022 already generalized the styled
standalone carrier — and it supersedes exactly one clause of PRODUCT-0018.

## Context

PRODUCT-0018 §4 admitted `point_size` "only for a two-field scatter mark in
this cut," and gate 6 pinned the refusal of a modified three-field scatter.
That limit was carrier-shaped, not semantic: the styled-single carrier of the
day could hold only what `LayerSpec` could hold — a two-field xy encoding with
an `Absent` color channel — so a mark with an authored color channel had
nowhere to ride. PRODUCT-0022 §3 then replaced that carrier with "a general
styled standalone mark: one ordinary plot sink plus `LayerSettings`,"
precisely so bar — never layer-shaped — could take a modifier. The three-field
scatter is the same shape, and the restriction it still meets is the old
carrier's silhouette surviving as an arity check.

The motivating fixture is ahead of the language, for the seventh time:
`../uat/test15.rn` spells `scatter("units", "revenue", "product")
.point_size(2.0)` and refuses at that span. The motivation is ordinary
authored presentation under the rope rule: a palette chosen for effect rather
than analysis wants above-default marks, and the plot choices one makes for
marketing and for science are not the same choices — which is exactly why
size is authored, not inferred.

PRODUCT-0007's own structure says the limit was accidental: encoding and
Layer-stage settings are different stages (§5), and a channel resolving per
§2 is orthogonal to a settings value resolving per §7. Nothing in either
resolution consults the other.

## Decision

### 1. A register belongs to the mark

> A closed modifier register is defined per mark kind. Every encoding form of
> that mark admits its register; encoding arity never changes register
> membership.

`scatter`'s register is `point_size`, so both `scatter(x, y)` and
`scatter(x, y, cat)` admit it. `line`'s register is `stroke_width` and
`bar`'s is `bar_width`; neither has a second **spellable** encoding form
today, so the rule binds them vacuously until one exists — stated now so a
future series line inherits its register by rule rather than by a new record.
The rule ranges over spellable forms only: a substrate-only encoding — the
three-field bar PRODUCT-0044 gate 3 tests below the envelope, for instance —
cannot fire it, so no register widens by a form an author cannot write.

Cross-register refusals are unchanged: `point_size` still refuses on line and
bar, `.skin(...)` still refuses on every mark, unknown modifiers and
duplicates still refuse atomically (0018 §1, 0025 §4).

### 2. The styled category scatter rides the 0022 carrier

`scatter(x, y, cat).point_size(f)` lowers to the ordinary three-field scatter
sink plus `LayerSettings { point_size: Set(f) }` through PRODUCT-0022 §3's
general styled standalone carrier. Channel resolution is untouched — the
category channel resolves per PRODUCT-0007 §2 exactly as unmodified — and the
setting resolves per PRODUCT-0017's five tiers exactly as on the two-field
form. The resolved size reaches the category scatter's vector, downsampled,
raster, and geometric-hit paths, which PRODUCT-0017 §6 already made one
radius contract.

### 3. Layers are untouched

Inside `layers([...])`, the three-argument scatter still refuses at its span
— PRODUCT-0014 §4's rule stands, because a layer's color channel must be
`Absent` (PRODUCT-0013 §2) regardless of styling. This record widens which
*standalone* marks take modifiers; it does not widen what a layer may be.

### 4. Nothing else moves

Identity, labels, placement, legend content (size never enters a legend,
0017 §7 / 0044), palette allocation, figure members, and every diagnostic
except the one refusal are unchanged. A size-only edit to the same slot
reuses its panel, per the standing content-not-identity rule (0016 §9,
0030 §11).

One visible consequence is accepted and named: a doubled-point category
scatter renders 2× marks beside 1× `PointSample` legend glyphs, because
swatch sizing is the one Guide consumer still deferred — not in
PRODUCT-0044's docket, waiting on its own record. `test15.rn` therefore
becomes that record's first concrete exhibit, the way the 4× "Value"
screenshot was PRODUCT-0044's.

## Acceptance gates

1. `scatter("units", "revenue", "product").point_size(2.0)` lowers to the
   unchanged three-field encoding plus exact `Set(PointSize(2.0))`, in
   `.views` position and as a figure body, through one shared stamped result.
2. The category scatter paints and hit-tests six-pixel points through
   vector, downsampled, and raster paths, with raster cache identity
   including the size — PRODUCT-0017 gates 6–8 re-asserted on the category
   path.
3. Factor `1.0` renders byte-identically to the unmodified three-field
   control; per-category colors and legend entries are byte-identical at
   every factor.
4. `scatter("x","y","cat").point_size(...)` inside `layers([...])` refuses
   at its span exactly as the unmodified form does; PRODUCT-0014 gate 5's
   both-halves assertion still holds.
5. Bounds, duplicate, unknown-modifier, and cross-register refusals are
   inherited unchanged; `point_size` on line and bar still refuses.
6. A size-only edit preserves view identity, panel, group, and labels.
7. `../uat/test15.rn` runs as written: four facets, one figure-level legend
   of Named product entries, doubled points.
8. Every other envelope, substrate, identity, placement, and lifecycle
   behavior is unchanged.

## Stop condition

> This record may extend the scatter register's admissibility to the
> three-field form, route it through the existing styled standalone carrier,
> and add the category-path size assertions. If the implementation requires
> a renderer rule beyond PRODUCT-0017's existing plumbing, a carrier beyond
> PRODUCT-0022's, a layers change, a cascade change, or a new modifier, the
> work stops and the requirement returns as its own record.

The expected trigger is discovering that the category path does not already
honor the resolved radius somewhere — which would be renderer work beyond
PRODUCT-0017's plumbing, and a halt rather than a fix-in-passing. Stage 0
checked the likely sites before drafting closed: `ScatterRasterKey` carries
the size (`scatter.rs:45`), the rasterizer derives its radius from the
resolved setting with no series-conditional branch (`:436`), and the vector,
downsampled, and hit paths consume the same resolved radius (`:190`, `:214`,
`:343`). If the implementation finds a site Stage 0 missed, that is the
trigger firing, not evidence against it.

## Supersession

This record supersedes, **only** where they restrict the register by arity:

- PRODUCT-0018 §4's "accepted only for a two-field scatter mark in this cut"
  and the three-field clause of its gate 6. The line clause of that gate, and
  every other PRODUCT-0018 rule — closed register, atomicity, flat lowering,
  duplicate refusal — remain in force.

## Rejected

- **A per-arity register:** makes the register's membership depend on which
  channels an author encoded, entangling two stages PRODUCT-0007 §5 keeps
  separate — the confusion this record exists to name and end.
- **Admitting the styled three-field scatter inside `layers([...])`:** the
  layer color rule is PRODUCT-0013 §2's and is about scale composition, not
  styling; widening it is that deferred record, not this one.
- **A figure-wide default point size instead:** a real deferred surface
  (0018 consequences), but it answers a different sentence — "all marks
  larger" — where the fixture says "these points larger"; neither substitutes
  for the other.

## Consequences and sequencing

- The register rule becomes what it always looked like: three registers, one
  per mark, orthogonal to encoding — and the next encoding form any mark
  grows will inherit its modifiers by rule.
- `test15.rn` stops being ahead of the language, closing the seventh
  ahead-of-language fixture.
- The figure-wide size default remains deferred with its trigger unchanged.
