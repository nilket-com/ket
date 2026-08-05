# ADR 0029: stable categorical palette positions

Status: accepted 2026-08-05. This record decides one thing: how Polariton maps
categorical meanings onto PRODUCT-0028's naturally ordered palettes without
recoloring an established meaning as live results change. It replaces baked
colors in categorical continuation state with stable palette positions, adds a
resolved band-fill treatment for two-field bars, and combines geometry and
palette capacity facts honestly. It adds no Rune spelling, plot-theme chooser,
guide policy, general scale composition, or libaryon behavior. PRODUCT-0030 owns
the first authored palette selection.

## Context

PRODUCT-0007 §8 already decides the lifecycle. Categorical identity is
per-plot continuation state keyed by resolved field, layer identity, and
category value. It grows monotonically; filtering retains absent assignments;
changing the categorical field starts a new lineage; and exhaustion refuses
rather than evicting, reassigning, or cycling. A resolved plot remains
self-contained while `ColorIdentity` carries only what a later live resolution
needs to extend that lineage.

The implementation currently proves those rules with the wrong retained value.
`ColorIdentity` stores `ResolvedColor` directly
(`crates/polariton/src/plot/spec/mod.rs:789`), and
`resolve_category_colors` assigns from a private twelve-color constant
(`:1691`, `:1751`). A color is therefore fixed before any resolved palette can
participate. PRODUCT-0028's natural categorical sequence cannot be selected
without either discarding the continuation state or baking another palette into
it.

Two-field bars expose the related renderer gap. `bar("cat", "val")` has no
color channel and prepares one synthetic group named `Value`; `BarPlotKernel`
therefore samples one midpoint color from `ResolvedPlotSettings.color_theme`
for every bar (`plot/kernels/bar.rs:170`). A categorical palette cannot make the
bands distinct through that path. Giving the x field an implicit color channel
would be worse: PRODUCT-0007 §2 forbids the resolver from copying a field into a
channel the author did not name.

PRODUCT-0028 now supplies the missing measurable fact. A resolved libaryon
theme either has no categorical palette or has one immutable, non-empty natural
sequence of length `m`. Continuous families and `tab20*`, `dark2`, `kelly`, and
`prism` have accepted sequences; syntax-only curated families such as `nord` do
not. Libaryon does not know whether a geometry can consume those colors.

The remaining product decisions are position assignment, live extension, the
relationship between geometry and palette limits, and how a band-styled bar
carries exact colors to rendering.

## Decisions

### 1. Identity retains a position; resolved artifacts retain colors

The continuation value becomes a closed product-side type:

```text
PalettePosition = private integer index into one resolved categorical palette

ColorIdentity:
    (resolved field, layer identity, category value) -> PalettePosition
```

`PalettePosition` is not an `Rgb`, normalized `t`, category ordinal, or render
order. Its constructor validates `position < palette_len`; arbitrary integers
cannot enter production state.

Resolution projects each retained position through the selected
`ResolvedCategoricalPalette` and writes the exact resulting `ResolvedColor`
into `ResolvedPlotSpec`. The renderer consumes only those resolved colors. It
does not receive the palette, theme expression, position allocator, or
`ColorIdentity`.

This preserves PRODUCT-0007's ownership split:

- `ColorIdentity` is small continuation state and can extend a live lineage;
- `ResolvedPlotSpec` is viewport-independent and sufficient for rendering or
  pinning; and
- changing renderer settings cannot recolor an already resolved artifact.

The local twelve-color constant and `categorical_color(index)` are retired from
the palette-aware path. Recreating PRODUCT-0028 colors in Polariton is not an
implementation alternative.

### 2. Palette resolution is explicit and product-side

The palette-aware resolver consumes one explicit selection containing:

```text
CategoricalPaletteSelection {
    identity: the exact libaryon theme expression,
    palette:  borrowed ResolvedCategoricalPalette,
}
```

Polariton obtains it only through `Theme::try_new(identity)` followed by
`Theme::categorical_palette()`. It does not parse names, aliases, `r`, `c`, `C`,
or `sN`, sample a continuous function, infer a curated sequence, pad a short
palette, or fall back to `Theme::palette()`.

Three failures remain different:

```text
unknown theme identity                   existing libaryon refusal
known theme without categorical palette categorical capability refusal
present palette too short for domain     capacity refusal (§5)
```

This record adds no user-facing default. Tests and internal assays pass an
explicit selection. PRODUCT-0030 decides how the five-layer plot settings
cascade supplies that selection, what absence means, and how changing an
authored selection affects already-live continuation state. Until then, no
production path may silently retarget one `ColorIdentity` between palette
sequences of different natural lengths.

The existing unselected resolver is a compatibility path, not an implicit
PRODUCT-0028 selection. It retains the current twelve colors and current
sequential assignment exactly, but stores their ordinals as validated
`PalettePosition` values rather than baking RGB into continuation state. It
does not gain band fills. A lineage records whether its positions were allocated
in the legacy twelve-slot space or an explicit PRODUCT-0028 palette space; the
two modes cannot be mixed silently in one continuation.

This temporary dual entrypoint is the honest cost of keeping PRODUCT-0029 a
substrate cut. PRODUCT-0030 must switch the production Rune caller to one
explicit categorical selection in the same change that makes selection
authorable. Only then may a later cleanup retire the compatibility palette. A
legacy caller is not evidence for any gate in §§3–7.

### 3. A known fresh domain is spread evenly

When a lineage has no retained assignments, the current resolved domain is
known in full. For `n` categories and a palette of length `m`:

```text
n = 0     no assignments
n = 1     position 0
n > 1     position(i) = round(i * (m - 1) / (n - 1)), i = 0..n-1
```

The mapping follows the display domain's established first-seen category order.
It is computed as one mapping over the domain, not as independent membership
sets or an assertion about the sorted positions.

For six categories over `tab20c`, the exact mapping is:

```text
A -> 0   B -> 4   C -> 8   D -> 11   E -> 15   F -> 19
```

and therefore selects natural `c01`, `c05`, `c09`, `c12`, `c16`, and `c20`.
The minimum adjacent gap is three.

Initial allocation does not use the live-growth selector. Sequential maximin
from an empty twenty-position palette would choose
`0, 19, 9, 14, 4, 2`, whose sorted minimum gap is two. Knowing `n` up front
permits the better even spread. The two algorithms answer different questions
and are intentionally not unified.

### 4. A live extension uses deterministic maximin

When a lineage already has retained positions, existing assignments never move.
Each newly observed category, in the display domain's first-seen order, takes
the unoccupied position whose distance to its nearest occupied position is
greatest:

```text
score(candidate) = min(abs(candidate - occupied))
```

The greatest score wins; an exact tie chooses the lower position. Occupied
positions include assignments reserved for categories absent from the current
filtered result. Selection repeats after each new assignment, so several new
categories in one result extend deterministically.

The canonical live trace over twenty positions is driven through three
successive resolutions of the same lineage:

```text
{A}       A -> 0
{A, B}    A -> 0, B -> 19
{A, B, C} A -> 0, B -> 19, C -> 9
```

The third position is `9`, not `10`, because both have nearest distance nine and
the lower-position tie break wins. Calling a selector three times with a caller-
managed occupied set does not prove the gate; the test must demonstrate that
`ColorIdentity` is returned, retained, and consulted across live resolutions.

This history dependence is inherited from PRODUCT-0007. A fresh final
three-category domain receives `{0, 10, 19}` while a domain grown one category at
a time retains `{0, 19, 9}`. Stability is preferred over reconstruction.

### 5. Geometry and palette impose independent limits

Before mutating continuation state, resolution computes the lineage cardinality
after adding every distinct missing category. Admission requires:

```text
final cardinality <= geometry limit
final cardinality <= categorical palette length
```

Neither limit overrides the other. A structured `CategoricalCapacityFailure`
reports:

- the attributed field, layer, and role;
- current retained cardinality and requested final cardinality;
- geometry limit;
- exact palette identity and natural palette limit; and
- whether geometry, palette, or both were exceeded.

When both limits are exceeded, both facts appear in one failure. Check order may
not decide which fact survives. Examples:

```text
dark2, 10 categories, geometry limit 12  -> palette limit 8 exceeded
tab20c, 13 categories, geometry limit 12 -> geometry limit exceeded
dark2, 13 categories, geometry limit 12  -> both limits exceeded
```

Failure is atomic: no new position enters `ColorIdentity`, existing assignments
remain usable, and no partial resolved color or band fill reaches a kernel.

An unavailable categorical palette is not a zero-length palette and is not a
capacity failure. PRODUCT-0028 makes zero length unrepresentable; a known
syntax-only identity instead produces the distinct capability refusal in §2.

### 6. Two-field bars receive band styling, not an encoding

For a bar whose authored color channel is `Absent`, the categorical axis domain
is eligible for **band fill**. Resolution assigns its category values stable
palette positions using §§3–5 and places exact category-to-color
`BandFillAssignment` values in the resolved artifact.

Band fill does not:

- create or resolve `Channel::Color`;
- copy the x or y field into another channel;
- change `ColorJob`, scale family, or domain;
- add a categorical legend; or
- affect bar width, geometry, hover, axes, or ordering.

It is mark styling keyed by the already-resolved band domain. The palette-aware
bar kernel path uses the exact assignment for each category when drawing marks;
it does not sample `ColorTheme` by `t` for those bars. Because band fill adds no
color encoding, it adds no legend item.

If a bar has an authored categorical color channel, that encoding is
authoritative. Its categories receive ordinary color-channel assignments and
band fill is absent. No bar has two competing color sources.

This distinction preserves PRODUCT-0007 §2: styling bands is not a resolver-
originated repetition into a color channel. A later authored color channel may
name the same field deliberately, and that authored encoding remains legal.

### 7. Palette order and modifiers remain observable

Positions are interpreted against the resolved categorical sequence exactly as
PRODUCT-0028 exposes it. With the six-category mapping from §3:

- `tab20c` maps positions to `c01, c05, c09, c12, c16, c20`;
- `tab20c_r` retains positions `0, 4, 8, 11, 15, 19` and maps them through the
  reversed sequence; and
- `tab20c_s1` retains those positions and maps them through libaryon's rotated
  sequence.

The algebra is a color cycler. For a palette of natural length `m`, positions
are residues in `Z/mZ` and `sN` acts by cyclic rotation. Reverse is the
orientation-reversing symmetry; shifts plus reverse generate the corresponding
dihedral action. Continuous `c` and `C` change the sampled carrier and therefore
produce a different cycle rather than acting inside one unchanged cycle.

The allocator is deliberately not a perceptual metric on that group. It sees
the resolved cycle at the cut and orientation selected by the theme expression,
then assigns ordinal positions deterministically. Index separation makes no
claim that two swatches are equally separated in hue, luminance, saturation, or
human discriminability. Making allocation hue-aware would introduce color-
science policy here and would tend to make rotations observationally
irrelevant—the opposite of the compact cycler grammar.

`_r` and `_s1` carry different proof burdens. Reverse proves that colors change
while positions hold. Shift proves authority delegation: a local special-case
reverse could fake the first result, while consuming an already-resolved
rotation demonstrates that Polariton did not reimplement the theme grammar.

Assertions compare complete category -> position -> color mappings. Comparing
only a set of positions or colors is disqualified because it is invariant under
category reassignment, the exact failure stable identity exists to catch.

### 8. `ColorJob` remains authoritative

PRODUCT-0007 §8 remains the authority on typed color encoding. A categorical
palette satisfies `ColorJob::Categorical`; it does not make a sequential,
diverging, or status job categorical and does not change compatibility tables.

Band fill is the one non-channel consumer authorized by §6. It is available only
to a bar with an absent color channel and a categorical band domain. Scatter and
line without a color channel do not acquire repeated colors merely because a
palette exists.

Sequential, diverging, status, plot chrome, table skins, `UiPalette`, and the
legacy `ColorTheme` migration outside this categorical path remain unchanged.
Retiring the rest of Polariton's duplicated plot-ramp catalog requires its own
record and parity evidence.

### 9. PRODUCT-0007 lifecycle is inherited, not rewritten

This record changes the retained value and its allocation, not the lineage
rules:

- field plus layer identity still defines a lineage;
- filtering and sorting cannot shrink or reorder retained assignments;
- a returning category keeps its position and therefore its palette color;
- changing the categorical field begins a new lineage;
- pinned resolved plots retain exact colors without needing a palette; and
- no assignment is evicted, reassigned, or cycled at capacity.

Cross-palette live retargeting is deliberately left to PRODUCT-0030 because no
authored palette selection exists in this record. An implementation that resets,
rekeys, normalizes, or silently reprojects an existing continuation merely to
make a future selection change convenient has crossed this record's scope.

### 10. No Rune surface or guide policy lands here

PRODUCT-0029 adds no accepted constructor, modifier, figure member, argument, or
method. `palette("tab20c")`, `skin("tab20c")`, and mark-level `.skin(...)` remain
unrecognized in Rune.

PRODUCT-0030 separately decides the figure-wide authored spelling and the live
lifecycle of changing it. Plot chrome skinning is a different projection.
Legend merging, swatch policy, categorical scale editing, layered color,
cross-layer palette sharing, and an explicit recolor/reset affordance remain
deferred.

PRODUCT-0030 must also carry real-window acceptance over `test2`'s existing
six-category bar fixture. PRODUCT-0029 can prove that `tab20c` deterministically
selects `c01`, `c05`, `c09`, `c12`, `c16`, and `c20`; it cannot prove that the
pale green and pale lavender remain distinguishable at bar scale on the actual
plot background. That is an aesthetic acceptance question, not a reason to
weaken this record's mapping gates.

## Acceptance gates

1. `PalettePosition` is product-side, immutable, and cannot be constructed at
   or beyond the selected palette length through a production API.
2. `ColorIdentity` retains positions rather than `ResolvedColor`; a completed
   `ResolvedPlotSpec` retains exact colors and renders without a palette or
   continuation state.
3. A fresh six-category `tab20c` lineage maps
   `A..F -> 0,4,8,11,15,19 -> c01,c05,c09,c12,c16,c20`. The assay compares the
   complete mapping, not unordered sets.
4. The same six-category mapping under `tab20c_r` and `tab20c_s1` retains every
   category's position and consumes the exact reversed or shifted libaryon
   color at that position.
5. Fresh allocation covers `n=0`, `n=1`, and all admitted `n`; the six-of-twenty
   control has minimum adjacent gap three, while the documented greedy-from-
   empty counterexample has minimum gap two.
6. Three successive live resolutions of one lineage produce
   `A->0`, then `B->19`, then `C->9`; neither prior assignment moves and a fresh
   three-category control independently yields `0,10,19`.
7. Filtering out a category retains its occupied position; its return restores
   the same color, and a category first seen while another is filtered cannot
   take the reserved position.
8. Palette-only, geometry-only, and both-limit failures report the complete
   structured facts in §5, mutate no identity, and produce no partial resolved
   artifact. A subsequent admissible resolution still succeeds unchanged.
9. Unknown `Theme::try_new` failure, known categorical absence (`nord`), and
   present-but-insufficient capacity (`dark2`) remain three distinct outcomes;
   none falls back to the syntax palette or another theme.
10. A two-field bar over six categories carries six exact resolved band fills
    and renders them in category order without creating `Channel::Color` or
    consulting renderer-time `ColorTheme` sampling.
11. A bar with an authored categorical color channel carries color-channel
    assignments and no band-fill assignments. The authored field and
    `ColorJob::Categorical` remain visible in the resolved specification.
12. Scatter and line with absent color stay single-treatment; sequential,
    diverging, status, chrome, table-skin, `UiPalette`, geometry, hover, and
    interaction behavior remain unchanged.
13. PRODUCT-0007 filtering, changed-field lineage, capacity, pinned-artifact,
    and renderer-self-sufficiency tests remain green with positions substituted
    for baked continuation colors. The unselected compatibility path retains
    its exact twelve colors, sequential category mapping, and absence of band
    fills.
14. No libaryon source, Rune grammar/envelope, execution, transport, panel,
    placement, dashboard, table, or workspace configuration changes.

## Stop condition

Stop if implementation requires changing libaryon's palette order, name grammar,
modifier behavior, optional capability, or natural lengths; inferring or padding
a palette; originating a color channel for band fill; letting a renderer assign
positions or sample a categorical palette; moving exact colors out of the
resolved artifact; weakening PRODUCT-0007 lineage stability; silently
retargeting a live identity across palette lengths; changing non-categorical
color jobs; adding a Rune spelling; or changing execution, transport, panels,
placement, dashboard, tables, or workspace configuration.

The anticipated trigger is the boundary between resolved plot settings and
domain resolution. If supplying one explicit categorical selection requires
migrating every `ColorTheme` consumer, teaching a kernel the theme grammar, or
inventing a default below the future surface, work stops. That is broader plot-
theme migration, not palette-position assignment.

## Consequences

- PRODUCT-0028's natural order becomes usable by plots without becoming a
  second theme parser or an inferred sequence.
- `tab20c` finally behaves as a categorical palette rather than a syntax-role
  permutation or a continuous gradient.
- Stable identity becomes independent of concrete RGB values; a resolved plot
  remains self-contained.
- Two-field bars can spend a categorical palette across visible bands without
  claiming the author encoded an unmentioned color field.
- Short curated palettes impose an honest capacity below some geometry limits.
- Initial known domains get better spacing than a greedy online allocator;
  later growth pays the unavoidable cost of preserving earlier meanings.
- Positional spread is deterministic rather than perceptual. If one cut through
  a palette produces a washed-out categorical selection, `_sN` is the authored
  lever: it rotates colors through the same stable category positions without
  changing allocation policy. The first response to that limitation is a
  different cut such as `_s2`, not a hue-aware selector that erases cycler
  observability.
- PRODUCT-0030 receives a proved product-side consumer and owns only authoring,
  default selection, the atomic production-path switch, and cross-palette live
  lifecycle. Its real-window gate judges the six-category bar where assertions
  cannot.

## Rejected

- **Retain baked RGB in `ColorIdentity`:** prevents one semantic identity from
  being projected through an authored palette and makes modifiers ineffective
  after first resolution.
- **Store normalized `t`:** hides natural palette length, admits ambiguous
  rounding, and can collapse distinct categories onto one short-palette color.
- **Use the syntax-oriented `Theme::palette()`:** repeats the PRODUCT-0028 defect
  and scrambles natural categorical order.
- **Select the first `n` colors:** gives six-category `tab20c` four adjacent blue
  shades before reaching the later hue groups.
- **Use sequential maximin for the initial known domain:** the six-of-twenty
  numerical counterexample has a smaller minimum gap than even spread.
- **Recompute even spread whenever the domain grows:** moves every existing
  category and violates PRODUCT-0007's continuation guarantee.
- **Evict, cycle, or reuse filtered positions:** silently recolors an established
  meaning.
- **Check only the geometry limit:** lets a twelve-category geometry index past
  `dark2`'s eight declared colors.
- **Check only the palette limit:** loses the geometry's independent supported
  cardinality.
- **Report whichever limit is checked first:** makes one failure hide another
  and turns diagnostic truth into implementation order.
- **Treat missing categorical capability as length zero:** collapses an absent
  design decision into an impossible empty palette and produces the wrong
  diagnostic.
- **Auto-bind the bar category field to `Channel::Color`:** originates an
  encoding the author did not name, explicitly forbidden by PRODUCT-0007 §2.
- **Let bars sample the selected palette by render index:** binds color to draw
  order and makes filtering recolor the remaining bands.
- **Give every mark a palette treatment when color is absent:** confuses a
  bar-specific band style with a universal implicit encoding.
- **Silently reinterpret an existing unselected lineage as a named palette:**
  changes both its allocation space and its colors before an author has selected
  anything; PRODUCT-0030 owns the explicit switch.
- **Make palette identity part of the semantic lineage key in this record:**
  changes the lifecycle PRODUCT-0007 already accepted and prejudges the authored
  retargeting decision PRODUCT-0030 owns.
- **Add the Rune spelling now:** merges substrate identity, renderer handoff,
  diagnostics, and language lowering into one proof burden; the substrate/surface
  split has repeatedly caught real defects before syntax made them user-facing.
