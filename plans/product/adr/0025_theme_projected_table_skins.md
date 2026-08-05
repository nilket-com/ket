# ADR 0025: theme-projected table skins

Status: accepted 2026-08-05. This record decides one thing: how a libaryon theme
name becomes a validated Polariton table skin and how Rune selects it with
`table().skin(name)`. Libaryon remains the theme and palette authority;
Polariton owns the artifact-role projection. It adds no editor-theme coupling,
workspace skin registry, plot theme, data coloring, or configuration surface.

## Context

PRODUCT-0023 made `TableSkin` a closed twelve-role value consumed atomically with
its frame. PRODUCT-0024 made libaryon's exact ordered twenty-color resolved theme
palette observable and centralized generic sRGB contrast math. The remaining
work is a consumer projection plus one contextual Rune spelling.

The target is `../uat/test8.rn`:

```rune
table().skin("terrain_rs10")
```

The name already means something to libaryon. `Theme::try_new` owns canonical
families, aliases, and the `r`, `c`, `C`, and `sN` modifiers. Polariton must not
create a second catalog or reinterpret the suffix. A valid syntax theme may
still be unsuitable for table text; PRODUCT-0024 deliberately kept that
artifact judgment out of libaryon.

`terrain_rs10` is the actual dogfood identity. It exercises two set-preserving
modifiers together, so a projection based only on contrast extrema would erase
the authored choice. The projection must be visibly palette-derived without
spending contrast that a weaker palette may not have.

## Decision

### 1. One name resolves through libaryon

Polariton resolves every requested skin name through `Theme::try_new` and reads
only the resulting `ResolvedThemePalette`. It defines no theme aliases,
modifiers, samplers, built-in name table, or user/workspace override.

The active `:set langtheme` value is not consulted. A Rune file names its skin
explicitly and therefore renders the same way wherever that libaryon theme
identity exists. Editor syntax and a table may share a name without either
implicitly following the other.

Vibe means shared palette identity, ordering, and accents—not identical
backgrounds. `UiPalette` remains editor chrome and never enters the table skin.

### 2. The projection is one deterministic, tunable role policy

Let `p[0..20]` be the resolved ordered palette, and let `dark` and `light` be the
darker and lighter colors from `most_contrasting_pair()`. Polariton assigns all
twelve PRODUCT-0023 roles under four constraints:

1. contrast anchors establish the table's background/text polarity;
2. header, gutter, row, grid, selection, type, and scrollbar treatment consumes
   ordered palette positions, so `r` and `sN` remain observable;
3. opaque tints use encoded-sRGB interpolation and interactive fills use explicit
   alpha; and
4. the complete result passes §3 before it can render.

The exact position-to-role choices, tint weights, and opacities are one named
Polariton projection policy, not language or libaryon semantics. They are tuned
against the real `test8.rn` window and recorded in implementation evidence once
the cut is accepted. Tuning them during this cut requires no ADR amendment as
long as the four constraints above and all gates continue to hold. Later changes
to an accepted mapping are ordinary table-skin policy changes; they do not reopen
theme identity or Rune grammar.

### 3. Complete visible validation decides suitability

Projection runs PRODUCT-0023's seven exact visible text checks, including the
declared selection alpha composited over both row backgrounds. A result is
returned only when every ratio is at least `4.5:1`.

There are two diagnostic classes:

- `UnknownTheme` means `Theme::try_new` rejected the name;
- `UnsuitableTheme` means the theme resolved, but the projected table skin's
  weakest named role fell below `4.5:1`.

The latter reports the name, weakest role, observed ratio, and threshold. It
does not relabel the theme as unknown or invalid for syntax. No clamping,
fallback to `TableSkin::DEFAULT`, palette substitution, or silent repair occurs.

### 4. Each sink kind owns a closed modifier register

PRODUCT-0018's register rule generalizes:

> Each recognized sink kind owns a closed postfix modifier register. In a
> proven sink position, a modifier outside the receiver's register, a duplicate,
> or an unknown modifier refuses the complete sink. Outside proven positions,
> the call remains ordinary Rune.

The table register contains only `skin(string_literal)`. Mark registers retain
only their accepted Layer-stage settings. Therefore these refuse atomically:

```rune
table().skin("nord").skin("terrain")
table().point_size(2.0)
table().frobnicate(1)
scatter("x", "y").skin("nord")
```

Modifier order is immaterial where a register eventually contains more than one
entry; duplicates still refuse rather than right-biasing. The complete chain is
flattened before the normal form.

### 5. Recognition remains contextual and literal

`table().skin("name")` is recognized only as the sink of an element in a proven
`.views([...])` list, directly or under `view(name, sink)`. The argument must be
exactly one Rune string literal. Computed names, missing or extra arguments, and
malformed chains refuse at the outer sink span and make the view list atomic.

The relation sugar `r.table()` remains unchanged and carries no skin spelling.
Selecting a skin requires the canonical `.views([table().skin(...)])` form.
Outside that position, `table().skin(...)` is ordinary Rune and is not globally
reserved.

### 6. The normal form carries identity, not colors

The table view intent carries `Option<String>`: absent means the exact existing
`TableSkin::DEFAULT`; present means the authored libaryon theme expression. It
does not carry twelve colors, a `Theme`, or a `TableSkin` through the envelope.

The head resolves the name into a complete `TableSkin` as sink completion is
formed. Successful output carries that resolved skin beside the shared frame;
failure is attributed to the outer sink span and does not prevent sibling sinks
over the same origin from completing.

### 7. Frame and skin remain atomic

The resolved skin enters `TableArtifact { frame, skin }` through the existing
required-skin constructor. A successful rerun replaces both together. An unknown
or unsuitable skin marks the destination failed/stale and leaves its previous
complete frame/skin pair untouched. No state can show new data with an old skin
or stale data with a new skin.

Default `table()` continues to lower to `TableSkin::DEFAULT` and retains exact
drawing compatibility. SQL result and Markdown tables remain default-skinned;
this cut authors skins only from Rune table sinks.

### 8. Identity, labels, placement, and chrome do not change

Skin is content, not identity. Adding, removing, or changing `.skin(...)` on the
same named or positional table view reuses its `HostPanelId`, presentation label,
group membership, tile, focus, and tab state. It changes no panel header or
editor chrome and never opens the dashboard.

## Acceptance gates

1. `TableSkin::from_theme_name` delegates name and modifier resolution only to
   `Theme::try_new`; Polariton defines no parallel catalog or suffix parser.
2. The projection assigns all twelve roles, consumes ordered palette positions
   as well as contrast anchors, and `terrain_rs10` resolves successfully.
3. Every successful projection passes all seven PRODUCT-0023 checks using the
   declared selection alpha; no successful skin bypasses validation.
4. An unknown name and a resolved-but-unsuitable palette produce distinct,
   stable diagnostics; neither silently falls back or changes theme validity.
5. Base, `_r`, and `_s1` representatives yield distinct skins where their
   resolved order differs, while each successful result remains valid.
6. Bare `table()` is byte/draw compatible with the existing default and no SQL
   result or Markdown table changes skin.
7. `table().skin("name")` lowers in direct and named-view positions to one table
   intent carrying the exact authored string; no resolved color enters the
   envelope.
8. Nonliteral/invalid arity, duplicates, unknown table modifiers, cross-register
   modifiers, and malformed receivers refuse atomically at the outer sink span.
9. The same calls outside a proven `.views([...])` sink position remain ordinary
   Rune, and relation `r.table()` sugar is unchanged.
10. A valid skin reaches the real table artifact with the same shared result
    identity; invalid suitability fails only its destination and preserves any
    previous frame/skin pair stale.
11. Changing only the skin reuses identity, label, topology, ratios, focus, tabs,
    and placement; panel chrome, `UiPalette`, plots, and dashboard remain
    unchanged.
12. Installed `../uat/test8.rn` parses and completes its `terrain_rs10` table
    alongside the existing headed/styled plots and default second table.

## Stop condition

> This record may add the deterministic Polariton theme-to-table projection,
> suitability diagnostics, a table-sink skin-name field, the closed contextual
> `skin(string_literal)` modifier, and the resolved-skin Rune table handoff. If
> implementation requires changing libaryon theme resolution or palettes,
> PRODUCT-0023's roles or threshold, editor `UiPalette`, active-theme coupling,
> user/workspace configuration, SQL or Markdown skin authoring, plot themes or
> data colors, relation sugar, execution, transport, identity, panel protocol,
> placement, dashboard, or table geometry, it stops.

## Rejected

- **Follow the active editor theme:** makes a committed Rune artifact depend on
  user configuration and destroys cut-and-paste portability.
- **Duplicate libaryon's theme registry in Polariton:** creates two authorities
  for names and modifiers.
- **Use only the contrast pair:** erases reverse and shift when the palette set
  is unchanged.
- **Treat every valid syntax theme as a valid skin:** confuses palette validity
  with artifact suitability and permits unreadable tables.
- **Fallback silently to the default:** makes a typo look successful and hides
  precisely the authored decision the surface exists to expose.
- **Put `.skin(...)` on every sink:** replaces closed per-kind registers with an
  accumulating options language and conflates table chrome with mark styling.
- **Skin the host-panel header:** crosses the table artifact boundary into
  libaryon-owned editor chrome.

## Consequences

- Theme names become portable table-style identities without becoming table
  skins inside libaryon.
- The same ordered palette can visibly inform syntax and table artifacts while
  each keeps its own semantic roles.
- Some valid syntax themes may refuse as table skins with an explicit reason.
- PRODUCT-0025's role mapping has one deliberately visible aesthetic review:
  `test8.rn` beside the same `terrain_rs10` language theme.
