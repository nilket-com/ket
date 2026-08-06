# ADR 0026: palette-forward table skin projection

Status: accepted 2026-08-05, with contrast admission amended by PRODUCT-0035.
This record decides one thing: that a resolved
libaryon palette has two explicit Polariton table-skin projections,
`PaletteForward` and `Duotone`. It preserves the accepted PRODUCT-0025 treatment
exactly as Duotone, adds a palette-forward treatment whose text ink is measured
neutral rather than palette-derived, and gives the new capability no Rune
caller. PRODUCT-0027 separately decides the surface default and `.duotone()`.

## Context

Libaryon's themes began as Polariton plotting themes. They are ordered
colormaps: compact expressions such as `yellows`, `terrain_rs10`, and `greens_r`
select and rearrange twenty-color ramps. PRODUCT-0024 exposes that exact resolved
identity. Syntax highlighting and PRODUCT-0025 table skins are consumer
projections of it; future plot chrome may become another. Data coloring remains
an Encoding/Scale concern rather than artifact chrome.

PRODUCT-0025's first table policy uses the palette's most contrasting pair as
paper and ink, then admits restrained ordered accents. The real window proves
that treatment is useful and visually coherent. It is nevertheless a
**duotone** projection of a colormap, not the only meaning of a palette.
`blues` and `greens` visibly compress toward one dark anchor, and `yellows`
exposes the semantic limit. The mechanism is localized in
`table_skin.rs:167-196`; the head reaches it through the one-argument call at
`state.rs:810`.

`yellows` resolves normally in libaryon. Its extreme palette colors have about
`4.685:1` contrast, but PRODUCT-0025's tinted backgrounds and declared selection
overlay reduce the completed table's weakest visible pair to about `3.175:1`.
The projection therefore refuses correctly under PRODUCT-0023. The mistaken
assumption is not validation; both projections must validate. It is requiring
text to come from the colormap. Near-black ink on the yellow surfaces is plainly
legible and frees the ordered swatches to remain chromatic. The accepted seven
visible checks and failure selection remain centralized at
`table_skin.rs:216-249`.

This is the third application of one authority principle:

> Lower layers report mechanism and measurable facts; upper layers compose
> defaults, alternatives, and user-facing policy.

PRODUCT-0023 kept table semantics out of libaryon. PRODUCT-0024 kept artifact
suitability out of libaryon. This record likewise keeps Rune's default out of
the table substrate. Libaryon reports an ordered palette and color math;
Polariton's table substrate reports a projection, failed role, and measured
ratio; PRODUCT-0027's Rune head alone may decide what absent syntax means or
which alternative to suggest.

PRODUCT-0025 §2 already calls its mapping a tunable Polariton role policy and
authorizes later policy changes without reopening theme identity or grammar.
This record therefore does not amend PRODUCT-0025. It names and preserves that
policy, then adds a second capability beside it.

## Decision

### 1. Projection is an explicit closed choice

The table substrate adds one two-variant value:

```text
TableSkinProjection = PaletteForward | Duotone

TableSkin::from_theme_name(name, projection)
    -> Result<TableSkin, TableSkinProjectionError>
```

`TableSkinProjection` has no `Default` implementation, implicit conversion, or
optional form. The one-argument `from_theme_name(name)` API is removed. Every
caller below Rune must state the mechanism it requests, so a surface default
cannot silently drift through a helper signature.

Theme resolution remains exactly PRODUCT-0025 §1: both variants call only
`Theme::try_new`, consume only its `ResolvedThemePalette`, and define no catalog,
alias, suffix parser, sampler, or `UiPalette` bridge. Unknown theme identity is
resolved before projection and is not a projection failure.

### 2. Duotone is the accepted PRODUCT-0025 policy, named

`Duotone` is byte-for-byte the projection implemented and accepted by
PRODUCT-0025:

- the most contrasting palette pair establishes darker paper and lighter ink;
- the same ordered positions, encoded-sRGB tint weights, and explicit alphas
  populate the same twelve roles;
- the same seven post-composite checks and `4.5:1` threshold decide suitability;
  and
- success and failure report the same values and visible diagnostic text.

The implementation may rename or extract the current
`project_theme_palette` body, but it may not reinterpret it. PRODUCT-0025's
`terrain_rs10` role values and exact seven ratios remain the compatibility
oracle.

Every existing production and test caller migrates to explicit `Duotone` in
this record. The Rune head therefore continues rendering exactly what it renders
today. `PaletteForward` has no production caller, and no source buffer can reach
it until PRODUCT-0027.

### 3. Palette-forward spends the palette on surfaces, not ink

`PaletteForward` projects ordered swatches substantially into table-interior
surfaces and accents. It does not begin with `most_contrasting_pair()` and then
reduce major surfaces to small tints of one anchor.

Its deterministic role policy obeys these constraints:

1. at least four opaque, non-text roles take colors directly from four distinct
   positions in the resolved ordered palette;
2. gridline, selection, and scrollbar treatments consume ordered positions with
   explicit alpha;
3. base, `r`, and `sN` order remains observable in the completed skin rather
   than only in intermediate indices; and
4. all twelve PRODUCT-0023 roles are complete before validation.

The exact position-to-role assignment and interaction alphas are tunable
Polariton table policy under PRODUCT-0025 §2. They are recorded in implementation
evidence, not promoted into libaryon or Rune semantics. Retuning may not weaken
the four constraints above or any acceptance gate.

“Palette-forward” does not promise that every one of twenty swatches appears in
one table. It promises that the ordered colormap materially supplies the
artifact's surfaces instead of being collapsed into an extreme pair with faint
accents.

### 4. Text ink is measured neutral

Palette-forward text is selected from a fixed near-black/near-white neutral pair
owned by Polariton's table policy. Those neutrals are ink candidates, not new
members of the libaryon palette.

For each text role group, the projection evaluates both candidates against every
background on which that role is visibly drawn and selects the candidate with
the greatest **minimum** contrast ratio. Cell text is therefore selected across
even row, odd row, selected-even, and selected-odd backgrounds together; header,
type, and gutter text use their own actual surfaces. Exact ties resolve to the
near-black candidate so selection is deterministic.

The chosen candidate is then stored in the ordinary PRODUCT-0023 text role.
Neither candidate receives a waiver: the complete skin still passes the same
seven checks after declared-alpha compositing. If no completed projection meets
`4.5:1`, palette-forward refuses rather than changing palette positions,
clamping, inventing a third ink, or falling back to Duotone/default.

The exact neutral RGB values are part of the tunable table policy and are
recorded in evidence. The invariant is their role and measured maximin
selection, not a particular shade of near-black.

### 5. Suitability facts follow the failed mechanism

The projections fail for different reasons. Their structured facts are
therefore different evidence shapes, not one generic “insufficient contrast”
record with a projection label.

Duotone reports:

```text
theme identity
darker and lighter anchor indices and colors
anchor-pair contrast ratio
weakest completed visible role
that role's observed ratio
required ratio
```

This distinguishes the palette range supplied by the anchors from the weaker
visible result after tint and alpha policy. In particular, `yellows` has an
anchor pair slightly above `4.5:1` while its selected-row result is below it;
the head must not falsely say the raw pair itself missed the threshold.

PaletteForward reports:

```text
theme identity
shared text-role group
near-black candidate's limiting surface and minimum ratio
near-white candidate's limiting surface and minimum ratio
required ratio
```

This names polarity conflict: neither neutral ink clears every surface served by
one text role. The limiting surfaces may differ, and cell text may be evaluated
over four surfaces rather than exactly two. The substrate reports those measured
facts without deciding how a user should respond.

Neither failure says which projection is default, recommends removing syntax,
or probes another projection. Those are PRODUCT-0027 head policies. The existing
Duotone diagnostic string remains compatible under §2 even though its structured
error becomes more informative.

`UnknownTheme` remains independent: a name that does not resolve has no palette
and therefore no failed projection, role, ratio, or alternative. Neither error
class silently substitutes another projection or `TableSkin::DEFAULT`.

### 6. `yellows` is the differential assay

The accepted distinction is executable:

> A palette whose Duotone projection refuses for insufficient visible contrast
> is accepted under PaletteForward, with text chosen as near-black or near-white
> by measured contrast.

The concrete palette is `yellows`. Its Duotone result must continue refusing at
the established weakest visible ratio rather than being repaired under the old
name. Its PaletteForward result must complete all twelve roles, select the
near-black candidate for the yellow surfaces, and pass all seven PRODUCT-0023
checks. This proves the new capability fixes the projection assumption, not the
theme registry or validation threshold.

### 7. The both-projections-fail path is constructible

The registered catalog is not required to contain a palette that defeats
PaletteForward. The failure path is nevertheless production behavior and must be
proved before PRODUCT-0027 composes a diagnostic around it.

A uniform flat palette is not the right assay: neutral ink is specifically what
makes one uniform surface easy to render. Instead, a synthetic twenty-swatch
**split-polarity midtone palette** places one shared cell-text role over both a
darker and a lighter visible row surface. The darker surface requires the
near-white candidate; the lighter requires near-black; neither candidate meets
`4.5:1` across both. At the same time, the palette's total luminance range is too
small to provide Duotone's paper/ink contrast.

The assay enters below theme-name resolution through a private Polariton
projection seam over twenty colors. It adds no public palette constructor and no
libaryon change. Both projections must return structured unsuitability with
their mechanism-specific §5 facts. Neither may substitute the other or default.
This makes PRODUCT-0027's “Duotone failed and PaletteForward also failed” branch
reachable without depending on future catalog contents.

### 8. Order remains semantic under both projections

Set-preserving `r` and `sN` modifiers must remain observable in PaletteForward
just as PRODUCT-0025 requires for Duotone. Representative base, reverse, and
shift expressions whose resolved order differs produce distinct complete skins
when each projection succeeds.

The assay observes final twelve-role values, not a palette vector or selected
index list. A role policy that consults order and then normalizes all three
expressions to the same artifact fails this record.

### 9. No Rune surface or lifecycle changes

`RuneTableSink` remains `skin_name: Option<String>` with no projection field.
The existing head passes `TableSkinProjection::Duotone` explicitly. The meaning
of absent syntax, table-register vocabulary, parser, normal form, outer-span
attribution, sibling completion, `TableArtifact`, stale retention, panel
identity, placement, SQL/Markdown defaults, plot behavior, and dashboard all
remain unchanged.

PRODUCT-0027 is reserved for exactly two coupled surface decisions:

```text
table().skin("name")            -> PaletteForward
table().skin("name").duotone() -> Duotone
```

It must land the default flip and modifier in one indivisible commit so accepted
Duotone behavior is never unreachable. It also owns the diagnostic policy that
may probe PaletteForward after a Duotone refusal and suggest removing
`.duotone()` only when that exact alternative validates. This record authorizes
neither behavior.

## Acceptance gates

1. `TableSkinProjection` is exactly `PaletteForward | Duotone`, has no default
   or optional representation, and every `TableSkin::from_theme_name` call
   supplies it explicitly. Name resolution still delegates only to
   `Theme::try_new`.
2. An exhaustive pre-refactor oracle over every registered base, `r`, `c`, `C`,
   and `s0..s19` expression records all twelve role RGBA values or the existing
   failure's name, role, ratio, minimum, and display text. Explicit Duotone is
   byte-identical to every success; every old failure fact and its display text
   is unchanged, while the new anchor indices, colors, and pair ratio truthfully
   describe the same failed projection.
3. The existing Rune head and every other current caller select Duotone
   explicitly. PRODUCT-0025's exact `terrain_rs10` role values and seven ratios,
   bare-table behavior, lifecycle, and real draw output are unchanged.
4. PaletteForward completes all twelve roles; at least four opaque non-text
   roles equal colors from four distinct ordered palette positions, and its
   alpha-bearing roles consume ordered palette positions without an anchor-only
   normalization.
5. PaletteForward chooses each text group from the fixed near-black/near-white
   candidates by greatest minimum measured contrast across its actual visible
   backgrounds, with deterministic near-black tie-breaking. No palette-derived
   text or unmeasured fallback path exists.
6. `yellows` refuses under Duotone at its established insufficient visible
   contrast, succeeds under PaletteForward with neutral near-black ink, and its
   complete result passes all seven PRODUCT-0023 checks at `4.5:1` or greater.
   Its Duotone error distinguishes the above-threshold raw anchor pair from the
   below-threshold completed selected-row role.
7. A synthetic split-polarity midtone palette makes the shared cell-text group
   fail neutral maximin selection under PaletteForward while its insufficient
   total range also fails Duotone. The Duotone error reports anchors, pair range,
   and weakest completed role; the PaletteForward error reports both neutral
   candidates and each one's limiting surface and minimum. Neither falls back.
   The assay uses a private Polariton projection seam and adds no libaryon
   constructor.
8. Every successful projection, in both modes, passes all seven exact
   PRODUCT-0023 checks using declared selection alpha. Unknown identity and
   per-projection unsuitability remain distinct, stable, non-fallback failures.
9. Representative base, `_r`, and `_s1` expressions produce distinct final
   twelve-role PaletteForward skins where their resolved order differs; an
   intermediate-vector or selected-index assertion does not discharge this
   gate.
10. `RuneTableSink`, Rune grammar and registers, sink completion/lifecycle,
   SQL/Markdown defaults, `TableArtifact`, renderer geometry/draw behavior,
   `UiPalette`, plots, identity, placement, and dashboard are behaviorally
   unchanged. No production path selects PaletteForward.
11. No libaryon source changes. No table role, contrast formula, threshold,
    alpha-compositing rule, theme name, alias, modifier, palette, or syntax color
    changes.

## Stop condition

> This record may add explicit `TableSkinProjection`, preserve the accepted
> PRODUCT-0025 implementation as Duotone, add one deterministic PaletteForward
> role policy with neutral maximin ink, extend structured suitability facts with
> the mechanism-specific measurements in §5, and migrate all current callers
> explicitly to Duotone. If implementation requires changing libaryon,
> PRODUCT-0023 roles or
> validation, any theme identity or modifier, Rune syntax/default/normal form,
> user-facing alternative suggestions, SQL or Markdown skin selection, table
> geometry or lifecycle, plot behavior, `UiPalette`, configuration, execution,
> transport, identity, panel protocol, placement, or dashboard, it stops.

## Rejected

- **Replace PRODUCT-0025's treatment:** discards a useful high-contrast visual
  style when the evidence calls for naming it, not deleting it.
- **Flip Rune's default in the substrate cut:** creates a commit where Duotone
  is no longer reachable from source before `.duotone()` exists.
- **Keep a one-argument projection helper:** lets a lower-layer default drift
  independently of the Rune surface that owns the meaning of absence.
- **Call Duotone “contrast”:** both projections are contrast-validated, and the
  name would imply PaletteForward does not care about the accepted threshold.
- **Take text from the colormap in PaletteForward:** recreates the exact
  assumption exposed by `yellows` and spends chromatic range on ink rather than
  surfaces.
- **Use only black or only white globally:** refuses palettes that the opposite
  measured neutral would render legibly and replaces projection with polarity
  preference.
- **Silently switch projections:** makes authored mechanism unobservable and
  turns a suitability error into undeclared fallback.
- **Use one generic suitability payload:** hides the distinction between
  insufficient Duotone anchor range and PaletteForward polarity conflict, so an
  upper layer cannot explain the actual repair without recomputing substrate
  internals.
- **Put projection in the theme string:** makes a table-artifact mechanism part
  of libaryon's shared palette identity and leaks it into syntax and future
  consumers.
- **Teach the substrate which projection is default or helpful:** crosses the
  authority boundary this record exists to preserve; PRODUCT-0027's head owns
  that composition.
- **Treat mark coloring as the next projection:** bar/line/scatter data color is
  an Encoding/Scale decision, not table or plot chrome.

## Consequences

- The accepted high-contrast table treatment survives under its accurate name,
  Duotone.
- Ordered colormaps can materially dress table surfaces without being reduced
  to paper and ink.
- `yellows` becomes a successful palette-forward skin without weakening one
  contrast check or changing its valid libaryon identity.
- Every substrate caller states projection intent; only PRODUCT-0027 may define
  absence in Rune.
- PRODUCT-0027 can be a small surface record whose default flip, `.duotone()`
  escape hatch, and conditional teaching diagnostic land atomically.
