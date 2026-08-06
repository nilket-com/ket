# ADR 0032: theme-projected plot chrome skins

Status: accepted 2026-08-06, with §3-4 amended by PRODUCT-0033 and contrast
admission amended by PRODUCT-0034. Evidence:
`ec3fc6a9`. This record decides one thing: how a libaryon theme
name becomes a validated Polariton `PlotChrome` and how a Rune figure selects it
with `skin(name)`. Plot chrome has one duotone-shaped projection, not the two
table treatments. `skin` never colors marks and `palette` never colors chrome.
The record adds no Guide behavior, application skin registry, arbitrary role
authoring, or default visual change.

## Context

PRODUCT-0031 gives the renderer a closed eleven-role `PlotChrome`, preserves the
legacy default exactly, and establishes visible 4.5:1 text and 3:1 non-text
assays. PRODUCT-0024 already exposes libaryon's exact resolved twenty-color
theme palette and owns the theme grammar. The remaining work is one consumer
projection and one figure-wide spelling.

The surface has a natural sibling already:

```rune
figure([
    skin("nord"),
    palette("tab20c"),
    bar("cat", "val"),
])
```

PRODUCT-0030 places categorical `palette(name)` at the figure level because it
selects the data-color vocabulary shared by the picture. A chrome skin is also
figure-wide metadata, but it dresses the artifact rather than encoding data.
Putting `.skin("nord")` on `bar` would imply a mark property; reusing the table
postfix would obscure the receiver boundary accepted since PRODUCT-0025.

Tables gained both Duotone and PaletteForward because their broad row, header,
and gutter surfaces can materially spend an ordered palette. Plot chrome has one
large background and otherwise thin lines and text. Distributing unrelated
swatches across axes and gridlines would make those structural cues compete
with the data. A palette-forward plot projection is therefore not merely
deferred; this record rejects it for this vocabulary.

## Decision

### 1. `skin(name)` is figure-wide metadata

The canonical spelling is:

```rune
figure([
    title("Revenue"),
    skin("nord"),
    palette("tab20c"),
    bar("cat", "value"),
])
```

`skin` takes exactly one string literal and appears at most once in a figure.
Its position relative to title, subtitle, palette, and the one body has no
meaning. It is recognized only as a direct member of a proven
`figure([...])` sink inside `.views([...])`.

These refuse at their own proven spans:

- duplicate `skin` members;
- zero, two, or more arguments;
- a non-string-literal identity;
- `skin(...)` inside `layers([...])`;
- `skin(...)` used as the figure body;
- `bar(...).skin(...)`, `line(...).skin(...)`, or any other mark postfix; and
- a nested figure or any otherwise malformed figure shape.

Outside proven sink positions, `skin(...)` remains ordinary Rune. The table
register remains independently valid as `table().skin(name)` and keeps its
optional `.duotone()` treatment. Plot figures gain no `.duotone()`,
`.palette_forward()`, or other projection selector.

### 2. The normal form carries one authored identity

`RunePlotIntent.authored_settings` gains one figure-wide property equivalent to:

```text
plot_skin: Override<Arc<String>>
```

The envelope carries only the authored string. It carries no `Theme`, resolved
palette, `PlotChrome`, RGBA, projection enum, contrast result, table skin, or
`UiPalette`. Figure flattening discards metadata source order and does not copy
the identity into a mark or resolved plot specification.

Omission is `Override::Inherit`. It preserves PRODUCT-0031's exact compatibility
default and existing application/pane cascade; it does not resolve a secret
theme name. This is deliberately different from PRODUCT-0030's explicit
`viridis` categorical library default, because naming an implicit skin here
would change the legacy gridline and other default pixels.

The figure-wide precedence remains:

```text
library compatibility chrome -> application chrome -> authored skin -> pane override
```

The graphical-layer tier is inapplicable and remains inherited. The head
resolves the effective authored identity once, projects it to a complete
`PlotChrome`, and places that value in the authored settings tier before the
ordinary renderer flattening. The renderer never resolves names.

### 3. There is exactly one plot-chrome projection

The public substrate shape is equivalent to:

```text
PlotChrome::from_theme_name(name)
    -> Result<PlotChrome, PlotChromeProjectionError>
```

There is no `PlotChromeProjection` enum, default mode, optional mode, overload,
or alternative probe. The one projection is duotone-shaped:

1. resolve `name` only through `Theme::try_new`;
2. read only its `ResolvedThemePalette`;
3. use the palette's most contrasting pair to establish a quiet darker
   background and lighter decoration ink;
4. derive restrained alpha variants of that same decoration ink for grid and
   neutral legend samples; and
5. return the complete eleven-role value only if every PRODUCT-0031 authored
   contrast check passes after actual compositing.

The exact grid/sample alpha policy is deterministic Polariton plot-chrome
policy and is recorded in implementation evidence. It may select the least
prominent admitted alpha from a closed search or use fixed declared alphas; it
may not introduce a third hue, consume unrelated palette positions for separate
decorations, weaken a threshold, or silently fall back. Retuning later remains
plot-chrome policy so long as the five constraints above and all gates hold.

“Duotone-shaped” does not create a user-selectable Duotone mode. It explains why
there is only one projection: the colormap supplies paper and ink, while alpha
establishes hierarchy among thin structural roles.

### 4. Many-to-one theme expressions are intentional

Unlike PaletteForward table skins and categorical data palettes, plot chrome
does not promise to expose the order of twenty swatches. It consumes the most
contrasting pair and their luminance orientation. Set-preserving `r` and `sN`
may therefore project to the same `PlotChrome`; continuous `c` and `C` remain
observable when they change the available color set.

This is not a second interpretation of libaryon's grammar. Libaryon still
resolves every modifier exactly once; Polariton truthfully projects the resolved
palette into a role vocabulary that has no design use for twenty ordered
accents. Requiring reverse and shift to differ would force swatch spending into
gridlines or invent a meaningless order-dependent polarity switch.

That distinction remains intentional when both consumers appear in one figure.
For example, change both identities in this figure from `viridis_r` to
`viridis_s5`:

```rune
figure([
    skin("viridis_r"),
    palette("viridis_r"),
    bar("cat", "val"),
])
```

The bars recolor because categorical palette projection consumes the resolved
order. The chrome remains identical because both expressions contain the same
color set and plot chrome consumes its contrast pair. The shared modifier is not
broken or inconsistently parsed: libaryon resolves it once for each named
consumer, then each projection observes the aspect its own semantics require.
The executable co-occurrence assay uses `viridis` because PRODUCT-0028 gives it
a categorical palette; `nord` is a valid skin source but deliberately declares
no categorical palette and therefore cannot prove this case.

### 5. Suitability is complete chrome contrast

There are two diagnostic classes:

- `UnknownTheme` means `Theme::try_new` rejected the identity; and
- `UnsuitableTheme` means the theme resolved but the completed projection's
  weakest named role is below its role-specific threshold.

Unsuitability reports the theme name, darker/lighter anchor indices and colors,
raw anchor-pair ratio, weakest completed role, that role's observed visible
ratio, and its required ratio. The required ratio is `4.5` for text and `3.0`
for non-text. A resolved theme remains valid for syntax, table projection, or
data palette use even if it is unsuitable here.

No clamping, palette substitution, neutral-ink injection, warning-only
admission, `PlotChrome::DEFAULT` fallback, table projection, or alternative
projection probe occurs. Failure belongs to the named destination and preserves
its previous completed plot stale under the existing lifecycle.

`nord` is the positive surface assay. A constructed low-range palette enters
through a private Polariton projection seam and proves the unsuitable path
without adding a libaryon theme or public palette constructor.

### 6. Validation stops at the chrome rope

The projection validates chrome against chrome only:

- title, subtitle, axis-label, tick-label, and legend text against their chrome
  backgrounds at 4.5:1;
- axes, gridlines, and legend-swatch borders against their adjacent chrome
  backgrounds at 3:1.

It never compares a mark fill, stroke, in-mark label, categorical swatch,
continuous ramp, or resolved palette color to plot chrome. Therefore this is
valid authored intent if both names resolve for their independent jobs:

```rune
figure([
    skin("nord"),
    palette("yellows"),
    bar("cat", "value"),
])
```

Pale bars on a pale or otherwise awkward plot are the author's choice. No
perceptual distance, mark/background contrast, hue-count, saturation, aesthetic
warning, automatic palette rotation, or coupled skin/palette substitution is
added. Structural validity and chrome's internal legibility are the whole gate.

### 7. `skin` and `palette` are disjoint siblings

The invariant is exact:

```text
skin    -> PlotChrome only
palette -> categorical mark colors only
```

Changing only `skin(name)` leaves every `ResolvedPlotSpec` categorical position
and exact data RGB unchanged. Changing only `palette(name)` leaves every
`PlotChrome` role unchanged. Source order does not create precedence between
them because they address different properties.

A figure may carry either setting without a current visible consumer. A skin is
still visible on any plot canvas; a palette on a plot without categorical color
remains accepted and inert under PRODUCT-0030. Neither setting manufactures a
mark, channel, scale, guide entry, or projection for the other.

### 8. Legend coloring decides no Guide semantics

The skin colors the existing legend background, text, neutral sample, and
sample/color-bar border. Encoded swatch and ramp colors remain data colors. The
record does not decide legend presence, entry identity, merging, order, labels,
grouping, or placement.

PRODUCT-0013 §3's provisional layered legend delegation remains provisional and
unchanged. A layered figure with `skin("nord")` colors whichever temporary
legend the bridge already emits; it does not make last-layer delegation an
accepted Guide policy.

### 9. Completion is atomic and destination-local

Projection occurs per plot destination after contextual parsing and before the
completed plot is installed. Unknown or unsuitable skin failure refuses that
destination, leaves a sibling view free to complete, and retains the prior
kernel/settings/chrome together as stale. A successful change replaces content
and chrome together.

Skin identity is presentation, not `LocalViewIdentity`, producer origin,
placement, or categorical-continuation lineage. Adding, changing, reordering,
or removing `skin` reuses the same host panel and palette-position bank. It
opens no dashboard and crosses no Toron protocol.

## Acceptance gates

1. `skin(name)` is recognized exactly once as an order-independent direct
   figure member with one string literal; every malformed/proven-position case
   in §1 refuses at its own span and ordinary Rune outside those positions is
   unchanged.
2. The normal form carries only `Override<Arc<String>>` identity in the authored
   figure settings layer. It carries no resolved theme, palette, chrome, color,
   projection mode, or renderer object.
3. Omitted plot skin preserves the exact PRODUCT-0031 compatibility chrome; no
   implicit named skin, validation repair, or default gridline change occurs.
4. Name resolution delegates only to `Theme::try_new` and consumes only
   `ResolvedThemePalette`; Polariton adds no catalog, alias, suffix parser, or
   sampler.
5. The plot substrate exposes exactly one name-to-chrome projection and no
   projection enum, alternate mode, table treatment, fallback, or probing path.
6. Every successful projection completes all eleven roles and passes the five
   post-composite 4.5:1 text checks plus three post-composite 3:1 non-text checks.
7. `nord` succeeds and exact role values/ratios are recorded. A private
   low-range synthetic palette returns structured unsuitability naming truthful
   anchors, weakest role, observed ratio, and that role's 4.5 or 3.0 threshold.
8. Unknown and unsuitable identities produce distinct destination-local
   diagnostics at the outer `skin(...)` span. Neither renders default content,
   mutates categorical continuation, or blocks valid siblings.
9. Representative base, reverse, and shift expressions prove the explicit
   many-to-one rule in §4; clipping remains observable where it changes the
   resolved set, and no caller reparses modifiers.
10. `figure([skin("nord"), palette("tab20c"), bar(...)])` produces exact Nord
    chrome and exact `tab20c` data colors. Permuting the two metadata members is
    identical; changing each independently leaves the other's completed values
    byte-for-byte unchanged. In a sibling figure, changing both
    `skin("viridis_r")`/`palette("viridis_r")` to
    `skin("viridis_s5")`/`palette("viridis_s5")` recolors the bars through the
    ordered categorical sequence while leaving every set-projected chrome role
    identical.
11. A structurally valid but visually awkward skin/palette pair renders without
    mark/background or swatch/background scoring, warning, refusal,
    substitution, or rotation.
12. No mark fill, stroke, in-mark text, categorical swatch, continuous ramp, or
    resolved data color reads the authored skin. Background-colored raster
    clears/knockouts remain only PRODUCT-0031's explicit compositing cases.
13. Legend chrome changes, but legend presence, entries, order, labels,
    grouping, placement, and PRODUCT-0013's provisional layered bridge are
    byte/operation-compatible.
14. Skin success, change, removal, and failure preserve destination and producer
    identity, panel placement, focus, tabs, source order, categorical
    continuation, and dashboard absence; replacement/stale retention never mix
    plot content and chrome from different completions.
15. Table `.skin(...)` and `.duotone()` behavior, categorical palette
    allocation, data ramps, libaryon theme behavior, application/pane controls,
    execution, transport, configuration, export, and durability are unchanged.

## Stop condition

> This record may add the one libaryon-theme-to-`PlotChrome` projection,
> structured suitability facts, the direct figure `skin(string_literal)`
> member, one authored-settings identity, head resolution, and atomic completed
> plot handoff. If implementation requires a second projection or selector,
> ordered swatch spending, changing the compatibility default, mark/data
> contrast validation, recoloring a mark or encoded swatch, Guide behavior,
> table behavior, application skin registry, arbitrary role authoring,
> libaryon change, execution, transport, identity, placement, dashboard,
> configuration, export, or durability work, it stops.

## Rejected

- **Add PaletteForward beside Duotone:** plots lack the broad neutral surfaces
  that justified spending multiple swatches on tables; colored gridlines and
  axes compete with encoded data.
- **Expose `.duotone()` anyway:** creates a mode selector for a one-mode
  capability and imports table vocabulary without a second meaning.
- **Require `r` and `sN` to change chrome:** forces order into a projection whose
  honest input is contrast range and whose structural roles do not need twenty
  accents.
- **Put `.skin(name)` on marks:** makes artifact chrome look like mark styling
  and collides with the closed mark modifier registers.
- **Reuse `palette(name)` for chrome:** couples artifact treatment to data
  encoding and makes one authored choice silently alter two semantic systems.
- **Validate marks against the background:** rejects aesthetically awkward but
  structurally valid author choices and makes admission depend on a skin/palette
  pair rather than each artifact's own contract.
- **Use neutral ink to rescue every palette:** adds a third color outside the
  named theme and turns the intended duotone projection into another policy.
- **Let a skin determine legend entries:** acquires Guide semantics while
  claiming only to color chrome.
- **Resolve theme names in the renderer:** makes errors late, duplicates work,
  and prevents a completed plot from carrying exact presentation.

## Consequences

- `skin` and `palette` become clean figure-level siblings with disjoint effects.
- Plot chrome gets one restrained palette projection rather than inheriting the
  table's two-treatment complexity.
- Authored skins meet appropriate text and non-text contrast criteria while the
  exact omitted-skin compatibility path remains honest about its legacy grid.
- Legend framing can follow a skin without deciding anything about Guide.
