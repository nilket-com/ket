# Specification: Configurable UI Colors

## Status

Proposed.

## Objective

Give Baryon's non-syntax UI a coherent, configurable color palette without expanding syntax
themes beyond their existing responsibility.

The public interface uses canonical dotted keys under `colors`:

```text
:set colors.accent 8fbcbb
:set colors.minimap.accent bf616a
:set colors.surface 181818
:set colors.preview.surface 202020
:set colors.editor.background 101010
:set colors.search.highlight ffd700
```

Existing commands remain aliases:

```text
:set background 101010       # colors.editor.background
:set searchhighlight ffd700  # colors.search.highlight
```

The feature has two goals:

1. One semantic setting should recolor related UI elements together.
2. A scoped override should provide an escape hatch without breaking future changes to the
   shared setting.

## Architectural Boundary

`Theme` continues to describe only syntax colors for content represented by a UAST. A theme may
vary by language, document, or pane. UI chrome does not belong to a UAST and must not be added to
`Theme` or derived from the currently focused pane's theme.

UI colors instead live in a separate, session-level `UiPalette`:

```text
UAST / pane                              editor session
┌──────────────────────────┐             ┌──────────────────────────┐
│ Theme                    │             │ UiPalette                │
│ syntax token categories  │             │ accent, surface, search │
└──────────────────────────┘             └──────────────────────────┘
```

Consequences:

- `:theme gruvbox` changes syntax colors but does not change UI colors.
- `:set langtheme rust ...` never changes tabs, gutters, the status bar, or the minimap frame.
- Moving focus between panes with different languages cannot recolor shared editor chrome.
- Markdown preview content may still use syntax colors from its source UAST; its pane surface comes
  from `UiPalette`.
- A future named UI palette or combined color-scheme feature can be designed separately. It is not
  implicit in this work.

### Explicit v1 Tradeoff

This feature makes the neon accent configurable; it does not automatically choose UI colors that
match a syntax theme. After:

```text
:set theme gruvbox
```

the UI accent remains unchanged until the user configures it. That limitation is intentional in v1
and should be stated in release notes rather than presented as automatic theme integration.

A future session-level command such as `:set colors.preset gruvbox` could select coordinated UI
roots from a separate palette-preset registry. Such a preset would remain independent of UAST
`Theme`, language overrides, and pane focus. Automatically seeding a UI preset merely because its
name matches a syntax theme is out of scope: it would reintroduce the coupling this boundary exists
to prevent and make behavior ambiguous when language themes differ by pane.

## Key Grammar

Canonical keys follow one of three shapes:

```text
colors.<role>                 shared semantic role
colors.<scope>.<role>         scoped override
colors.<scope>.<state>.<role> state-specific scoped override
```

Examples:

```text
colors.accent
colors.surface
colors.text
colors.editor.background
colors.editor.text
colors.search.highlight
colors.minimap.accent
colors.minimap.surface
colors.gutter.accent
colors.gutter.surface
colors.gutter.text
colors.status.surface
colors.status.text
colors.tab.accent
colors.tab.surface
colors.tab.active.surface
colors.tab.text
colors.preview.surface
colors.preview.text
```

Use `colors`, plural, because it names the configured palette. Use scope (and, where needed, a
semantic state) before role so global and scoped values map naturally to TOML. A role-first layout
such as `colors.accent.minimap` would require `accent` to be both a scalar and a table. Path depth
does not imply inheritance; the registry's explicit parent metadata remains the sole fallback
authority.

Names describe semantic roles, not rendering structure. Prefer `colors.tab.accent` over names such
as `colors.tab.title.highlight`: the latter exposes an implementation detail and becomes brittle if
the tab renderer changes.

Only registered keys are valid. Arbitrary dotted paths do not create settings.

## Initial Color Registry

The registry is the source of truth for canonical names, command aliases, fallback relationships,
built-in defaults, help text, and completion.

| Canonical key | Legacy alias | Parent | Built-in | Initial consumers |
|---|---|---|---|---|
| `colors.accent` | — | — | `#FFFFFF` | Unscoped accent uses, including the folded placeholder and busy indicator |
| `colors.surface` | — | — | `#121212` | Shared override for secondary UI surfaces |
| `colors.text` | — | — | `#DADADA` | Shared override for ordinary chrome text |
| `colors.editor.background` | `background` | — | `#121212` | Editor content background when no pane-local override applies |
| `colors.editor.text` | — | — | `#DADADA` | Plain editor text not assigned a UAST syntax color |
| `colors.search.highlight` | `searchhighlight` | — | `#4CFF00` | Search matches and minimap search markers |
| `colors.minimap.accent` | — | `colors.accent` | `#FFFFFF` | Viewport frame and cursor-line marker in terminal and image renderers |
| `colors.minimap.surface` | — | `colors.surface` | `#10141C` | Minimap background in terminal and image renderers |
| `colors.gutter.accent` | — | `colors.accent` | `#FFFFFF` | Current-line number and other location indicators in the gutter |
| `colors.gutter.surface` | — | `colors.surface` | `#121212` | Line-number gutter background |
| `colors.gutter.text` | — | `colors.text` | `#6C6C6C` | Non-current line numbers and gutter text |
| `colors.status.surface` | — | `colors.surface` | `#121212` | Status bar background |
| `colors.status.text` | — | `colors.text` | `#FFFFFF` | Ordinary status text |
| `colors.tab.accent` | — | `colors.accent` | `#FFFFFF` | Active document-tab foreground |
| `colors.tab.surface` | — | `colors.surface` | `#0C0E12` | Document-tab bar background |
| `colors.tab.active.surface` | — | — | `#20262E` | Contrasting tab surface; used as the active-tab background in focused panes and the bar background in unfocused panes |
| `colors.tab.text` | — | `colors.text` | `#949494` | Inactive document-tab text |
| `colors.preview.surface` | — | `colors.surface` | `#121212` | Markdown image background and terminal fallback |
| `colors.preview.text` | — | `colors.text` | `#E0E5EB` | Markdown preview body text when no UAST color applies |
| `colors.divider.surface` | — | `colors.surface` | `#121212` | Split dividers and otherwise unpainted pane boundaries |

The table deliberately does not expose every constant in `ui/colors.rs`. Additional settings should
be added when they represent a stable semantic role and a credible customization need, not merely
because a renderer contains an RGB value.

Equal built-in values do not imply equal roles. For example, editor background and surface may both
default to `#121212`, but changing `colors.surface` must not unexpectedly recolor the text buffer.

Configurable surfaces need configurable foregrounds. Without the `colors.*.text` roles, a light
surface could make fixed grey or terminal-indexed foregrounds unreadable. The initial text roles do
not promise a complete light-theme system—specialized Markdown code and alternating-row surfaces
remain candidates for later registry entries—but they ensure ordinary chrome and preview body text
can be paired with a configured background. User-configured color combinations are not
automatically contrast-corrected.

The editor is not chrome. Its independently configurable pair is `colors.editor.background` and
`colors.editor.text`; neither inherits from `colors.surface` or `colors.text`. This symmetry prevents
a single chrome-wide text override from producing unreadable buffer text against an unchanged
editor background.

## Fallback Semantics

This is a shallow, static fallback model rather than a general cascade.

For a scoped role, resolution is:

```text
scoped user value
-> global user value
-> scoped built-in default
-> global built-in default
```

For example:

```text
colors.minimap.accent -> colors.accent -> #FFFFFF
colors.preview.surface -> colors.surface -> #121212
colors.gutter.text -> colors.text -> #6C6C6C -> #DADADA
```

A scoped built-in is checked before the global built-in so the default palette can preserve
existing intentional differences. An explicit global user value still overrides every unpinned
chrome leaf. For example, `:set colors.text 000` makes ordinary gutter, status, tab, and preview
text black, while leaving editor text alone. An unset `colors.text` retains the chrome leaves'
distinct built-in values.

This ordering intentionally makes curated scoped built-ins fragile under an explicit global
override. For example, the minimap uses its bluer built-in `#10141C` while `colors.surface` is
unset, but `:set colors.surface 202020` makes every unpinned surface—including the minimap—resolve
to `#202020`. A global setting is an instruction to flatten those distinctions; users can pin any
leaf they want to preserve.

Each scoped key has at most one parent, declared in the registry. Config files cannot contain
references, define new parents, create arbitrary depth, or form cycles. There is no specificity or
selector machinery.

An explicit scoped value remains pinned when its parent changes:

```text
:set colors.accent 8fbcbb
:set colors.minimap.accent bf616a
:set colors.accent a3be8c       # minimap remains #BF616A
```

The value `default` removes an explicit value and resumes fallback:

```text
:set colors.minimap.accent default  # follows colors.accent again
:set colors.accent default          # returns to built-in #FFFFFF
```

`default` is a state transition, not a stored color literal. The settings writer omits cleared
values.

## Querying Values and Provenance

Append `?` to a canonical key or alias to query it without changing settings:

```text
:set colors.accent?
:set colors.minimap.accent?
:set background?
```

The result reports both the resolved value and why it has that value:

```text
colors.accent = #FFFFFF (built-in default)
colors.minimap.accent = #FFFFFF (inherited from colors.accent)
colors.gutter.text = #6C6C6C (scoped built-in default)
colors.editor.background = #101010 (explicit; queried as background)
```

The engine answers queries because it owns explicit overrides and fallback provenance. A resolved
`UiPalette` alone is insufficient: it deliberately contains values rather than configuration
history. Querying does not persist settings or force a redraw.

## Color Values

Commands accept RGB hex with an optional leading `#`:

```text
0f0
#0f0
00ff00
#00ff00
```

Persisted and displayed values use uppercase `#RRGGBB`. Alpha is not part of the configuration;
image paths derive an opaque RGBA value from the same RGB source used by terminal paths.

The accepted grammar should be exactly three or six hexadecimal digits. The current shared parser's
acceptance of four- and five-digit strings should not become part of this new public interface;
either tighten it for all callers in a separately tested change or validate UI color input before
calling it.

## Search Highlight and Accent Are Independent

Search highlight defaults to `#4CFF00`, while the more conservative UI accent defaults to
`#FFFFFF`. They remain independent settings with no inheritance relationship.

```text
colors.search.highlight  - content match visibility
colors.accent            - current location and active UI emphasis
```

Changing one must not change the other. This formalizes the behavior already implied by
`:set searchhighlight`, which changes search matches without changing the compile-time neon UI
constants.

## Editor Background Compatibility

`colors.editor.background` is the canonical spelling of the existing `background` setting, not a
new layer painted over it. The alias must preserve the current command's pane/document behavior and
persistent default behavior.

If a pane already carries an explicit `background_override`, that pane-local value wins over the
resolved session default. This feature should consolidate the representation when practical, but it
must not silently change which panes an existing `:set background` command affects.

## Settings File Shape

Canonical persistence uses nested tables:

```toml
[ui]
theme = "gruvbox"
minimap = true

[ui.colors]
accent = "#8FBCBB"
surface = "#181818"
text = "#E5E5E5"

[ui.colors.editor]
background = "#101010"

[ui.colors.search]
highlight = "#FFD700"

[ui.colors.minimap]
accent = "#BF616A"
surface = "#10141C"

[ui.colors.tab]
surface = "#0C0E12"

[ui.colors.preview]
surface = "#202020"
```

The hand-written settings parser must recognize the registered color sections and keys. Persistence
should take a palette/override structure or map rather than adding one positional parameter per
color to `persist_ui_settings` and `render_settings`.

### Migration

- Continue accepting `:set background` and `:set searchhighlight` indefinitely.
- Show canonical keys in completion, help, status messages, and newly written settings.
- Read legacy `[ui]` keys `background` and `search_highlight` as aliases.
- If both a canonical and legacy setting are present, the canonical setting wins.
- Write only canonical color tables on the next successful settings save.
- Do not create separate internal values for an alias and its canonical key.

Because the current writer rewrites the settings file wholesale, the first successful save after
an upgrade removes legacy keys. Downgrading afterward will lose those settings unless the user
restores the old spellings. This is acceptable for the initial migration but must be called out in
release notes.

No compatibility alias is needed for experimental color keys that have not shipped.

## Adjacent Language-Theme Cleanup

The same namespace cleanup exposes `langtheme` and `nolangtheme` as irregular settings commands.
Their canonical replacement should be:

```text
:set language.theme rust gruvbox
:set language.theme rust default
```

Compatibility forms remain available:

```text
:set langtheme rust gruvbox    # alias for language.theme rust gruvbox
:set nolangtheme rust          # compatibility form for language.theme rust default
```

`nolangtheme` remains a compatibility spelling rather than a registry alias because it has a
different argument shape. Its own help and completion entry therefore advertises
`:set nolangtheme LANG`, while `langtheme` is a true alias of `language.theme`.

`default` removes the language-specific override and resumes the global syntax theme. This is the
same user-facing reset word used by color overrides, although the two settings systems remain
architecturally independent.

The singular `language` namespace matches the existing `:language` command and reads as a property
of the named language. The language id remains an argument rather than becoming a dynamic dotted
key, which keeps registry-based completion and validation straightforward.

This cleanup is adjacent rather than part of `UiPalette`:

- It may share the option-registry and settings-persistence refactor.
- It should land as a separate commit with its own compatibility tests.
- It must not cause UI colors to enter `Theme`; language themes remain syntax/UAST-only.
- The UI-color feature does not depend on renaming these commands.

## Data Model

Use two distinct representations:

1. `UiColorOverrides` contains only explicit user choices.
2. `UiPalette` contains the fully resolved RGB values required for rendering.

A representative shape is:

```rust
pub enum UiColorKey {
    Accent,
    Surface,
    Text,
    EditorBackground,
    EditorText,
    SearchHighlight,
    MinimapAccent,
    MinimapSurface,
    GutterAccent,
    GutterSurface,
    GutterText,
    StatusSurface,
    StatusText,
    TabAccent,
    TabSurface,
    TabText,
    PreviewSurface,
    PreviewText,
    DividerSurface,
}

pub struct UiColorOverrides {
    values: [Option<Rgb>; UI_COLOR_COUNT],
}

pub struct UiPalette {
    pub accent: Rgb,
    pub surface: Rgb,
    pub text: Rgb,
    pub editor_background: Rgb,
    pub editor_text: Rgb,
    pub search_highlight: Rgb,
    pub minimap_accent: Rgb,
    pub minimap_surface: Rgb,
    pub gutter_accent: Rgb,
    pub gutter_surface: Rgb,
    pub gutter_text: Rgb,
    pub status_surface: Rgb,
    pub status_text: Rgb,
    pub tab_accent: Rgb,
    pub tab_surface: Rgb,
    pub tab_text: Rgb,
    pub preview_surface: Rgb,
    pub preview_text: Rgb,
    pub divider_surface: Rgb,
}
```

The exact container may change, but the separation must remain. Persistence needs explicit values;
rendering needs resolved values. A fixed `Option<Rgb>` array is preferred over a `BTreeMap` because
the key set is closed and registry-backed; it mirrors the existing syntax-theme representation and
makes resolution allocation-free. `UiColorKey` supplies a stable index and the registry supplies
iteration order for persistence and help.

Prefer one typed command over one protocol variant per setting:

```rust
EditorCommand::PersistUiColor {
    key: UiColorKey,
    value: Option<Rgb>, // None means `default`
}

EditorCommand::QueryUiColor(UiColorKey)
```

The command parser resolves aliases and validates hex before constructing the command. The engine
updates `UiColorOverrides`, persists it, resolves a new `UiPalette`, and publishes a new viewport.

The frontend receives one resolved RGB-only `UiPalette` on `Viewport`, not several `Option<Rgb>`
fields. It should never need access to config precedence or hex strings.

## Rendering Integration

Every affected renderer must consume the resolved palette rather than a compile-time color or a
copied byte array.

- Terminal rendering converts `Rgb` to `ratatui::Color` at the final boundary.
- Image rendering converts the same `Rgb` to opaque `image::Rgba<u8>` at the final boundary.
- Minimap terminal and image paths use the same resolved minimap accent and surface.
- Markdown preview image and terminal fallback use the same resolved preview surface and ordinary
  text color.
- Document-tab text and accent are rendered on the resolved `colors.tab.surface` and
  `colors.tab.active.surface` contrast pair. The renderer swaps their focused/unfocused roles, so
  both surfaces must remain configurable.
- Async image request fingerprints include every palette value that affects their output so a color
  change invalidates cached images.
- A settings change triggers a viewport publish and redraw without requiring a restart.

Remove role-specific RGB/RGBA copies such as neon constants and minimap byte arrays as their call
sites migrate. Conversion helpers should derive alternate representations from `Rgb`; arrays must
not be maintained independently.

Deduplication must preserve semantic roles. Replacing every occurrence of `18,18,18` with one
generic `DARK_GREY` constant would hide rather than fix the model.

## Registry and Discoverability

The color registry should drive:

- Canonical-key parsing.
- Alias parsing.
- Parent fallback metadata.
- Built-in defaults.
- Help summaries.
- `:set colors.<Tab>` key completion.
- `:set colors.minimap.<Tab>` scoped completion.
- `?` query parsing and provenance labels.

This follows the existing declarative option-registry direction while avoiding a growing list of
nearly identical value-option parsers.

Unknown-key suggestions reuse the command layer's closest-match mechanism over canonical color
keys and aliases. Offer a suggestion only when that matcher produces a unique close result;
otherwise report the unknown key without guessing.

Status messages should name the canonical key even when the command used an alias:

```text
colors.editor.background set to #101010
colors.minimap.accent reset to colors.accent
```

## Non-Goals

- UI colors do not become part of UAST syntax `Theme`.
- No CSS-like selectors, specificity, arbitrary inheritance, or user-defined references.
- No per-language UI palette.
- No automatic derivation of UI colors from a syntax-token category.
- No automatic syntax-theme-to-UI-preset matching in v1.
- No alpha channel or gradients in v1.
- No ANSI-indexed values in configuration; terminal color reduction, if needed, is a rendering
  concern.
- No requirement to expose every existing UI color in the first release.
- No combined named syntax/UI color-scheme format in this change.

## Implementation Plan

### Phase -1: Normalize the Existing Visual Baseline

Preparatory behavior fixes must land before the zero-behavior-change palette refactor:

1. Resolve the minimap background drift. Treat `MINIMAP_BG` (`#10141C`) as the intended value and
   update the stale image-path background (`#121212`) to match it. Lock terminal and image output to
   the same RGB in a focused test.
2. Normalize configurable terminal-defined foreground defaults to explicit RGB before they enter
   the RGB-only palette. Convert index 242 to `#6C6C6C`, 246 to `#949494`, and 253 to `#DADADA`.
   Normalize ratatui's named `Color::White` to `#FFFFFF` and `Color::Black` to `#000000`; those are
   exact, behavior-preserving mappings because ratatui uses `Gray` for the non-bright ANSI color and
   reserves `White` for bright white. Make `contrasting_text_color` return those explicit RGB
   values as well. Only the indexed conversions can change appearance for terminals with customized
   ANSI palettes, and that possibility must be recorded.
3. Verify that Markdown preview image and fallback backgrounds agree at the current built-in value.
   If another drift is found, fix it here rather than hiding it in palette plumbing.

These changes should be one or more explicit fix/baseline commits with their own tests. Phase 1's
no-visual-change guarantee is relative to the normalized baseline produced here.

### Phase 0: Stabilize the Model

1. Stop extending the experimental one-command/one-field accent implementation.
2. Introduce `UiColorKey`, the registry, `UiColorOverrides`, and resolved `UiPalette`.
3. Add unit tests for fallback, pinning, reset, and accent/search independence.

This phase may replace uncommitted experimental fields rather than preserving them as compatibility
surface.

### Phase 1: Preserve Behavior While Removing Copies

1. Route existing built-in accent, surface, and text consumers through a default `UiPalette`.
2. Add `Rgb` conversion helpers for terminal and image paths.
3. Remove duplicated RGB/RGBA literals only after each consumer has a semantic palette field.
4. Include palette values in minimap and Markdown-preview fingerprints.

The default palette must produce no intended visual change.

### Phase 2: Commands and Persistence

1. Register canonical `colors.*` keys and legacy aliases.
2. Add generic persistent UI-color command handling.
3. Add provenance-aware UI-color query handling.
4. Add `default` reset behavior.
5. Teach the settings parser and writer the `[ui.colors]` tables.
6. Migrate legacy `background` and `search_highlight` keys on write.
7. Add dotted-key completion and generated help entries.

### Phase 3: Scoped Rendering

1. Apply accent to gutter, minimap, tabs, and unscoped accent consumers.
2. Apply surface to gutter, minimap, status, tabs, preview, and divider consumers.
3. Apply independently configured editor text and shared chrome text to their respective consumers.
4. Preserve pane-local editor background precedence.
5. Verify live updates in terminal-only and image-capable render paths.

### Phase 4: Documentation and Polish

1. Document canonical keys, aliases, defaults, and reset behavior in `README.md` and `:help`.
2. Add completion tests for every registered color key.
3. Audit remaining hard-coded UI colors and propose new semantic roles only where useful.

## Test Plan

### Registry and Resolution

- Built-in palette matches the current intended colors.
- A global accent changes every unpinned accent leaf.
- A scoped accent remains unchanged after its parent changes.
- Resetting a scoped accent resumes its parent.
- Resetting a global role resumes the built-in value.
- Search highlight and accent remain independent even when their defaults match.
- Unknown dotted keys fail with a useful error and suggestion.
- Scoped text leaves retain their distinct built-ins until `colors.text` is explicitly set.
- A global text value changes every unpinned child of `colors.text`.
- A global text value does not change independent editor text.
- A global surface value overrides the distinct built-ins of every unpinned surface, including the
  minimap and tab bar.
- The independent tab surface pair retains contrast when `colors.tab.surface` changes, and both
  focused/unfocused branches use the configured pair.

### Commands and Persistence

- Three- and six-digit RGB input canonicalizes to `#RRGGBB`.
- Invalid length, invalid digits, and alpha input are rejected.
- Legacy command aliases produce the same typed command as canonical keys.
- Canonical and alias queries report the same resolved value and canonical key.
- Queries distinguish explicit, inherited, scoped built-in, and global built-in provenance.
- Legacy TOML keys load into canonical overrides.
- Canonical TOML wins over a legacy duplicate.
- Saving emits only canonical nested tables and omits reset values.
- Settings round-trip without changing resolved colors.

### Rendering

- Gutter, minimap, tab, status, preview, and divider styles use their resolved fields.
- Minimap terminal and image paths agree on accent and surface RGB.
- Markdown preview image and fallback agree on surface and ordinary text RGB.
- Changing an image-affecting color changes the corresponding request fingerprint.
- A color command causes a new viewport and redraw.
- Switching syntax theme, language, document, or pane does not change `UiPalette`.
- A pane-local editor background continues to override the session editor background.

### Architecture

- `Theme` remains syntax/UAST-only.
- Frontend rendering receives resolved RGB values and contains no config-resolution logic.
- Adding a new registered leaf does not require a new `EditorCommand` variant or another positional
  persistence argument.

## Acceptance Criteria

The feature is complete when:

1. `:set colors.accent 0f0` recolors all unpinned accent consumers immediately.
2. `:set colors.minimap.accent f00` overrides only the minimap accent.
3. `:set colors.minimap.accent default` makes it follow `colors.accent` again.
4. `:set colors.surface 202020` recolors all unpinned surface consumers without changing the text
   buffer background.
5. `:set colors.text 111` recolors ordinary text on all unpinned chrome surfaces without changing
   editor text or UAST syntax colors.
6. Canonical editor-background and search-highlight commands behave exactly like their legacy
   aliases.
7. Settings persist under `[ui.colors]` and legacy settings migrate without data loss.
8. Syntax theme and language changes never modify the UI palette.
9. Terminal and image render paths do not maintain independent copies of configurable colors.
10. `:set colors.minimap.accent?` reports its resolved color and whether it is explicit, inherited,
   or built in.
11. `colors.tab.surface` and `colors.tab.active.surface` configure both sides of the tab renderer's
    focused/unfocused contrast swap.
