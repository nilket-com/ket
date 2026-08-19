# ADR 0024: observable resolved theme palettes

Status: accepted 2026-08-05. This record decides one thing: a resolved libaryon
`Theme` retains and exposes the exact ordered twenty-color palette from which its
syntax colors were assigned. It centralizes generic luminance and contrast math
with that color authority. It changes no theme name or modifier behavior, adds
no table projection or Rune table surface, and reserves PRODUCT-0025 for
`table().skin(name)`.

## Context

Libaryon is the color-theme authority. `Theme::try_new(name)` already owns the
canonical catalog and the modifier grammar: `r` reverses, `c` clips the low end,
`C` clips the high end, and `sN` rotates (`theme.rs:21-68`). Polariton must not
duplicate or reinterpret that vocabulary merely because a table will consume
it.

Today the resolved value exposes only
`syntax_colors: [Option<Rgb>; CATEGORY_COUNT]` (`theme.rs:72-73`). That is a
semantic projection for token foregrounds, not the palette itself. It contains
no table backgrounds, row stripes, selection fill, or artifact chrome. Those
roles remain Polariton's responsibility under PRODUCT-0023.

The palette nevertheless already exists during resolution. `THEMED_CATEGORIES`
contains exactly twenty ordered categories (`theme.rs:4-25`). Continuous
families sample exactly twenty colors, then reverse and rotate that sequence
before assigning it to those categories (`theme.rs:121-141`). Curated families
fill all twenty categories, collect their colors in that same order, then apply
reverse and rotation before reassignment (`theme.rs:88-113`). An independent
census confirms every one of the twenty-one curated fills assigns every themed
category; a fixed `[Rgb; 20]` is therefore an honest representation for both
families.

PRODUCT-0023 currently carries its own WCAG relative-luminance and contrast
implementation in `polariton/src/gui/table_skin.rs`. Once libaryon exposes
generic palette contrast queries, retaining a second copy would create two
color-math authorities that can drift. Selection alpha compositing and the seven
visible table checks remain artifact-specific and stay in Polariton.

That compositing produces fractional encoded-sRGB channels. Quantizing the
visible result back into eight-bit `Rgb` before computing contrast would change
PRODUCT-0023's accepted assay. The shared primitive therefore needs a normalized
sRGB value as well as the existing byte representation; centralizing the formula
must not silently narrow its inputs.

## Decision

### 1. A resolved palette is an exact fixed value

Libaryon adds a public `ResolvedThemePalette` containing exactly twenty ordered
`Rgb` values. It is complete by construction: no `Vec`, string-keyed map,
`Option<Rgb>`, missing entry, sampler, or family-specific variant enters the
public value.

```text
ResolvedThemePalette([Rgb; 20])
```

The array is private behind indexed and whole-sequence observation methods, so
callers may inspect identity and order but cannot mutate a resolved `Theme`.
`Theme` retains one palette and exposes it by reference. `syntax_colors` remains
available with its existing type and visibility.

Adding a private field closes external `Theme { syntax_colors: ... }` literals.
That tightening is deliberate: a public literal could manufacture disagreement
between syntax colors and the palette, violating §2 by construction. A workspace
census finds no such literals outside `theme.rs`; `Theme::try_new` is already the
sole production construction path. This is an accepted pre-release source-API
tightening, not a claim that adding a Rust struct field is semver-compatible.

The palette is not a table skin, UI palette, or plot data encoding. It contains
no background, foreground, accent, selection, opacity, or artifact-role labels.
Those are consumer projections over one color identity.

### 2. Palette and syntax colors come from one resolution

`Theme::try_new` resolves the twenty-color sequence once. It applies the
existing family and modifier behavior to that sequence, stores it as the
`ResolvedThemePalette`, and assigns the same ordered values to
`THEMED_CATEGORIES`.

For every themed-category position `i`:

```text
theme.syntax_colors[THEMED_CATEGORIES[i]] == Some(theme.palette[i])
```

This equation is structural, not a reconstruction performed by callers.
Polariton may not infer a palette later from token categories.

`BodyText` is not in `THEMED_CATEGORIES`. Curated fills may continue assigning
it independently, and continuous families may continue leaving it absent. Its
existing value is part of the syntax-color compatibility oracle but not the
resolved palette.

### 3. Existing modifier behavior becomes observable unchanged

The applicability matrix is:

| modifier | continuous family | curated family |
|---|---|---|
| `r` | reverse the resolved sequence | reverse the resolved sequence |
| `sN` | rotate right by `N mod 20` | rotate right by `N mod 20` |
| `c` | sample from `0.2` instead of `0.0` | no effect |
| `C` | sample through `0.8` instead of `1.0` | no effect |

Modifier order and parsing remain exactly what `parse_theme_modifiers` accepts
today. Aliases and case handling remain unchanged. No sampler is exposed, and
no caller applies modifiers itself.

The curated `c`/`C` no-op is **observed and preserved, not accepted as correct**.
`Theme::try_new` currently returns from the curated branch before clipping is
consulted, so `nord_c` and `nord_C` equal `nord`. Whether inapplicable clipping
should instead refuse is undecided. Changing it would alter shipped
`:set langtheme` behavior and requires its own record; this observability cut
neither fixes nor endorses it.

### 4. Generic luminance and contrast belong to libaryon color primitives

Libaryon adds an immutable normalized encoded-sRGB value with finite channels in
`[0.0, 1.0]`:

```text
Srgb::new(red, green, blue) -> Option<Srgb>
Srgb::relative_luminance() -> f64
Srgb::contrast_ratio(other: Srgb) -> f64
Rgb::as_srgb() -> Srgb
```

Construction accepts both endpoints and every finite value between them. It
rejects NaN, either infinity, negative values, and values above one; it never
clamps. Negative zero is stored as positive zero so numerically equal visible
colors cannot retain two channel representations. Fields are private, making
invalid channels unrepresentable after construction.

`Rgb` may expose convenience luminance and contrast methods that delegate through
`Srgb`; the normalized type is the formula's authority. These are pure color
operations. They know nothing about palettes, syntax, tables, alpha, thresholds,
or suitability. The formula is the one already proved by PRODUCT-0023;
Polariton removes its duplicate implementation and delegates to these
primitives.

Alpha compositing stays in Polariton. Only the table projection knows that a
translucent `selection_fill` is seen over two alternating row backgrounds.
Polariton constructs `Srgb` from its fractional composited channels without
rounding them through `u8`, then delegates luminance and ratio. PRODUCT-0023's
seven checks retain their exact inputs and 4.5:1 threshold after delegation.

### 5. Pair selection is total and deterministic

`ResolvedThemePalette::most_contrasting_pair()` examines distinct index pairs
`(i, j)` with `i < j` and returns both indices, both colors, and their contrast
ratio. It is total because the palette always contains twenty entries.

Selection and orientation are exact:

1. choose the greatest contrast ratio;
2. on an equal ratio, choose the lexicographically earliest `(i, j)`;
3. orient the result as darker then lighter by relative luminance;
4. when the selected colors have equal luminance, the lower index occupies the
   darker slot.

A flat palette therefore returns a distinct indexed pair with ratio `1.0`; it
does not fail. Tests use a synthetic palette with exact duplicate entries so
both tie rules are constructible and observable rather than prose over an
accidental production fixture.

Returning indices is part of the contract. A later artifact projection can keep
contrast anchors distinct from ordered accent positions without reverse-mapping
equal RGB values.

### 6. Palette validity is not artifact suitability

A palette that resolves successfully is a valid theme palette. Pair selection
reports what it contains; it does not decide whether the contrast is adequate
for a table, plot, editor surface, or any other consumer.

PRODUCT-0025 may refuse a valid theme as an invalid table skin when its complete
projection cannot pass PRODUCT-0023's checks. Such a refusal names insufficient
luminance range or the exact failed table role. It does not report an unknown
theme and does not retroactively invalidate syntax use.

### 7. Permutation remains meaningful to consumers

`r` and `sN` change order while preserving the palette's set. The exposed
sequence retains that order exactly. `most_contrasting_pair` is a capability
query, not a complete artifact projection: using only its colors could erase
reverse and shift whenever the extreme set is unchanged.

PRODUCT-0025 must consume ordered palette positions as well as safe contrast
anchors, and must prove representative `_r` and `_s1` names produce distinct
table skins from their base while satisfying PRODUCT-0023. This record states
that downstream obligation but adds no table behavior itself.

### 8. Resolved behavior is additive and boundary-safe

Existing Baryon syntax consumers continue reading `syntax_colors`; none must
migrate to the palette API. `UiPalette`, language-theme selection, pane focus,
theme persistence, help, completion, and rendering behavior are unchanged.
"Additive" here describes resolved behavior: the source-API tightening for
direct struct literals is the explicit exception in §1.

Polariton consumes only the generic `Rgb` contrast primitive in this cut. No
`TableSkin`, table role, opacity, Rune sink, plot theme, data ramp, or artifact
policy enters libaryon. No palette value crosses the host-panel protocol.

## Acceptance gates

1. `ResolvedThemePalette` contains exactly `[Rgb; 20]`; incomplete, optional,
   variable-length, string-keyed, and family-specific resolved forms are
   unrepresentable. The retained palette is private, a source census finds no
   production `Theme` struct literals outside its defining module, and no public
   construction path can create palette/syntax disagreement.
2. Every curated fill assigns all twenty themed categories, and every curated
   and continuous theme returns one complete resolved palette.
3. A pre-refactor oracle over every canonical family, alias, `r`, `c`, `C`, and
   shifts `0..19` is byte-identical after the expansion for the complete
   `syntax_colors` array, including `BodyText` and absent categories.
4. For every accepted theme expression and every index, the exposed palette
   value is exactly the value assigned to the corresponding themed category.
5. Continuous and curated `r`/`sN` follow the matrix in §3; continuous clipping
   changes the sampled set, while `nord_c == nord` and `nord_C == nord` remain
   guarded as known, unendorsed compatibility behavior.
6. `Srgb` relative luminance and contrast ratio match the PRODUCT-0023 formula
   on black/white, equal-color, channel-boundary, fractional, and established
   reference vectors. Construction accepts inclusive zero/one, canonicalizes
   negative zero, and rejects non-finite or out-of-range channels without
   clamping. `Rgb` delegates without changing its exact byte values. Polariton's
   seven table checks pass their fractional composited results without `u8`
   quantization and retain their prior ratios.
7. A synthetic palette containing duplicate colors proves greatest-ratio
   selection, lexicographic pair ties, darker/lighter orientation, the
   equal-luminance lower-index rule, and returned index/color correspondence.
8. A flat palette produces a ratio-`1.0` pair without becoming an invalid
   `Theme`; no artifact threshold or refusal enters libaryon.
9. Existing syntax rendering, theme selection, persistence, help, completion,
   `UiPalette`, pane focus, and aliases are unchanged; no existing consumer is
   required to use the new palette.
10. No table projection, `TableSkin`, alpha operation, Rune syntax, sink normal
    form, configuration surface, plot behavior, execution, transport, panel,
    placement, dashboard, or artifact lifecycle change is introduced.

## Stop condition

> This record may add `ResolvedThemePalette`, retain it on `Theme`, refactor
> existing theme resolution so palette and syntax projection share one sequence,
> add normalized `Srgb` plus generic luminance/contrast operations and
> deterministic pair
> selection, and make Polariton's existing table contrast checks delegate to
> libaryon. If implementation changes any existing syntax color, theme name,
> alias, modifier parse or behavior—including curated clipping's current
> no-op—exposes a raw sampler, adds an artifact role or threshold to libaryon,
> changes `UiPalette`, requires an existing syntax consumer to migrate, or adds
> table projection, Rune surface, configuration, plotting, execution, protocol,
> panel, placement, or dashboard work, it stops.

## Rejected

- **Refusal semantics for inapplicable `c`/`C` clipping on curated themes
  (ledger CI-06, closed 2026-08-19):** the no-op is now accepted as correct
  rather than merely observed and preserved. Refusal would convert a silently
  harmless authoring habit into an error that no user has asked to be
  diagnosed, and would change behavior for both projections PRODUCT-0028 §5
  preserves. Reopens only on a concrete report of a user misled by the no-op.
  Ratified in `plans/product/review/deferred_triage.md` § Ratification —
  2026-08-19.

- **Expose the continuous sampler:** makes continuous families privileged and
  lets callers produce colors that do not reflect the post-`sN` twenty-swatch
  identity used by syntax highlighting.
- **Reconstruct the palette from public syntax colors:** makes every consumer
  know `THEMED_CATEGORIES`, mishandles non-themed categories, and turns one
  structural equation into repeated inference.
- **Use a variable-length or optional palette:** admits states the complete
  curated-fill census and fixed continuous sampling prove do not exist.
- **Fix or refuse curated clipping here:** changes accepted `:set langtheme`
  behavior in a record whose purpose is faithful observability.
- **Put backgrounds or artifact roles in the resolved palette:** confuses one
  color identity with a consumer-specific presentation.
- **Keep the WCAG formula copied in Polariton:** creates two authorities for
  generic sRGB color math.
- **Quantize a composited table color back to `Rgb`:** changes the visible color
  before measuring it and would make PRODUCT-0023's exact alpha assay regress
  merely to fit the older byte type.
- **Let pair selection enforce 4.5:1:** moves a table text threshold into a
  generic palette query and would make a valid syntax theme invalid globally.
- **Use only the contrast pair for future skins:** erases set-preserving `r` and
  `sN` semantics from the artifact projection.

## Consequences

- Libaryon's theme identity becomes fully observable without changing what any
  existing syntax consumer sees.
- Curated and continuous themes share one complete public resolved value.
- Generic sRGB contrast math has one authority; artifact compositing and
  thresholds remain with their consumers.
- PRODUCT-0025 can project the same named, ordered identity into a portable
  `TableSkin` and prove permutation sensitivity through `test8.rn`.
- The current curated clipping no-op is visible, tested, and explicitly not
  blessed as correct.
