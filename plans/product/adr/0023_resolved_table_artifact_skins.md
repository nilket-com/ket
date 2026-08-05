# ADR 0023: resolved table artifact skins

Status: accepted 2026-08-04; sequencing references amended 2026-08-05 after
libaryon's theme authority made an observable resolved palette a prerequisite.
This record decides one thing: a rendered table
receives one complete, validated skin instead of reading Polariton's compile-time
GUI color constants. The skin covers only the table artifact's interior chrome.
It colors no data, changes no editor chrome, and adds no Rune surface;
PRODUCT-0025 is the first authorized `.skin(...)` spelling.

## Context

`Table` currently accepts only a frame, row offset, optional column width, and
the column-type flag (`table.rs:76-105`). Its `draw` implementation ignores the
provided Iced theme (`table.rs:656-665`) and reads `BG_PRIMARY`, `BG_SECONDARY`,
`TABLE_ROW_EVEN`, `TABLE_ROW_ODD`, `TABLE_BORDER`, text colors, selection, and
scrollbar colors directly from `gui::colors` (`table.rs:676-1008`; the constants
are at `colors.rs:3-38`). There is no table-theme model to migrate.

This chrome is not data coloring. Alternating rows, headers, selection, and a
scrollbar style the table artifact; gradients, categorical assignments, and
conditional cells encode values. PRODUCT-0007 §10 already keeps plot data ramps
separate from interface palettes. Its current `ColorTheme` is a data-ramp enum
containing colormaps such as Blues, Cividis, and Viridis (`plot/colors.rs:4-83`),
so it is not reused for table chrome.

There are two production `Table` consumers. Host-panel content constructs one at
`editor.rs:2702`, covering the singleton SQL result panel and ordinary Rune table
panels. Markdown-cell output constructs the other at `editor.rs:3827`. Both must
receive a resolved skin when the widget stops consulting global constants.

## Decision

### 1. A table skin is a complete resolved value

`TableSkin` has one color for each semantic role:

```text
body_background
header_background
gutter_background
row_even_background
row_odd_background
gridline
cell_text
header_text
type_text
gutter_text
selection_fill
scrollbar_thumb
```

Colors retain alpha where the renderer uses it. The representation is closed and
typed: it contains no string-keyed property map, no missing role, and no
`Option<Color>` fallback. A `TableSkin` is ready to draw without consulting
`gui::colors`, `UiPalette`, a registry, or renderer defaults.

The header's `#` cell uses `header_background` and `header_text`. Row numbers use
`gutter_background` and `gutter_text`. The scrollbar track remains the artifact's
`body_background`; only its thumb has a separate role.

### 2. The existing appearance becomes the default skin

`TableSkin::DEFAULT` maps each role to the exact color the table consumes today.
This cut is an extraction, not a visual redesign: rendering the default skin must
produce the same fills and text colors in the same order as the pre-extraction
table.

Every production built-in table skin is a complete `TableSkin` and passes §3.
This record establishes the typed catalog boundary but exposes no authored name.
PRODUCT-0024 makes libaryon's existing theme identity observable as one resolved
palette. PRODUCT-0025 decides the theme-to-table projection, missing-name and
insufficient-range behavior, and the authored spelling. User or workspace
configuration does not enter this substrate cut.

### 3. Built-ins have executable contrast criteria

Contrast is calculated with the WCAG relative-luminance formula. An alpha color
is first composited over the exact background on which the renderer draws it,
using the alpha declared by that `TableSkin`; tests never compare an
uncomposited declaration to text or substitute an assumed opacity. An opaque
`selection_fill` is legitimate and makes the selected-row checks equivalent to
testing that opaque fill by construction; the assay still consumes the declared
alpha rather than special-casing it.

Every production built-in satisfies all of these at **4.5:1 or greater**:

1. `cell_text` against `row_even_background`;
2. `cell_text` against `row_odd_background`;
3. `header_text` against `header_background`;
4. `type_text` against `header_background`;
5. `gutter_text` against `gutter_background`;
6. `cell_text` against `selection_fill` composited over
   `row_even_background`; and
7. `cell_text` against `selection_fill` composited over
   `row_odd_background`.

Gridlines and scrollbar thumbs are UI components rather than text. This record
does not add a contrast gate for them. If a later record does, it uses the WCAG
non-text criterion of 3:1 rather than applying the 4.5:1 text threshold to a
hairline.

### 4. Skin consumption is required by construction

`Table::new` requires a resolved `TableSkin`; there is no constructor that can
omit one and no late fallback inside `draw`. Both production call sites must
supply it. This follows the house rule established by PRODUCT-0013's
`NonEmpty<LayerSpec>` and two-variant `LayerMark`: when an invalid state can be
excluded by the representation, it is not deferred to a runtime check.

The draw path consumes only the stored skin for the twelve roles in §1. Removing
or forgetting a consumer is therefore a compile failure, not a blank or partly
themed table discovered at runtime.

### 5. Frame and skin form one table artifact

Completed table content stores the frame and its resolved skin together as one
`TableArtifact`. `PanelContent::Table` carries that artifact rather than a bare
`DataFrame`; markdown `NotebookCellOutput::Table` uses the same shape. Replacing a
table replaces both values together.

This makes completion atomic by structure. `PanelRegistry::fail_or_stale`
already leaves completed `PanelContent::Table` untouched and changes only status
(`editor.rs:2620-2635`), so a failed rerun retains the previous frame and its
previous skin together. It cannot show stale data with a newly authored skin.
PRODUCT-0025 may rely on this lifecycle without adding a failure-path rule.

### 6. Skin ends at the artifact boundary

The skin covers pixels drawn by `Table` inside its content bounds. It does not
cover the host-panel title, tab, focus border, split divider, toolbar, status
rail, markdown container, or any other editor surface. Those remain libaryon or
Polariton application chrome governed by `UiPalette` and existing editor styles.

No `TableSkin`, skin name, or table color crosses the libaryon protocol. A skin
change cannot alter panel identity, title, placement, focus, tab membership, or
layout geometry.

### 7. Skin is presentation, not data or interaction

Changing a skin changes only the twelve color roles. It does not change the
`DataFrame`, row or column order, formatting, widths, heights, scrolling,
selection range, hit-testing, copying, column types, or loaded-row behavior.
Selection keeps its existing semantics; only its resolved fill changes.

There is no cell-value inspection in skin resolution. Conditional formatting,
per-column styling, gradients, categorical assignment, and value-driven text or
background colors are data-encoding work and remain unrepresented.

### 8. The Rune register generalization belongs to PRODUCT-0025

The intended surface is `table().skin("name")`, but this record adds no parser,
normal form, theme resolution, or Rune handoff. PRODUCT-0025 must generalize
PRODUCT-0018's rule explicitly:

> Each sink kind owns a closed modifier register. In a proven sink position,
> registered modifiers flatten under PRODUCT-0018's order-independent,
> duplicate-refusing rules; receiver-incompatible and unregistered modifiers
> refuse atomically. Outside proven sink positions, calls remain ordinary Rune.

That future rule makes `table().point_size(2.0)`,
`scatter("x", "y").skin("nord")`, `table().frobnicate(1)`, and duplicate skins
precise refusals without reserving any method name globally. It is recorded here
only as the sequencing contract; PRODUCT-0025 owns its executable gates.

## Acceptance gates

1. `TableSkin` is a closed, complete value with exactly the twelve roles in §1;
   no role is optional or string-keyed.
2. `TableSkin::DEFAULT` maps role-for-role to the current constants. A recording
   renderer captures the ordered list of fill operations and text-color
   operations emitted by `Table::draw`; that list is exactly equal to a
   pre-extraction baseline. Equality between the skin value and constants alone
   does not discharge this gate.
3. Every production built-in passes all seven 4.5:1 checks in §3 using
   alpha-composited selection colors over both alternating rows.
4. `Table::new` requires a resolved skin, and no table draw path reads a table
   color from `gui::colors`, `UiPalette`, or an implicit default.
5. Host-panel tables—including `RESULT_PANEL` and Rune table views—and markdown
   cell tables render through the same required-skin widget without losing their
   existing default appearance.
6. A non-default assay skin changes every declared role in the recorded draw
   output while leaving table geometry, formatting, selection coordinates,
   scrolling, copying, and data unchanged.
7. Completed host and markdown tables store frame plus skin as one
   `TableArtifact`; replacement moves both together, and host failure retains
   the previous pair while marking it stale.
8. A skin change affects no host-panel or markdown-container pixel outside the
   table content bounds and changes no panel identity, title, placement, focus,
   tab membership, or layout geometry.
9. `ColorTheme`, `PlotThemeSpec`, plot ramps, plot chrome, `UiPalette`, and every
   data-coloring path are unchanged.
10. Rune syntax, sink normal forms, execution, transport, configuration, name
    resolution, dashboard behavior, and every existing table interaction remain
    unchanged.

## Stop condition

> This record may add the closed `TableSkin` value, default and built-in values,
> WCAG contrast helpers and tests, `TableArtifact`, required skin consumption at
> the two existing `Table` construction sites, and default-compatibility assays.
> If implementation requires Rune syntax or envelope work, a skin registry or
> workspace/user configuration, cell-value inspection, conditional formatting,
> plot-theme or data-ramp change, `UiPalette` or libaryon change, panel chrome,
> another table renderer, table geometry or interaction change, execution,
> transport, identity, placement, or dashboard work, it stops.

## Rejected

- **Reuse `ColorTheme`:** it is a plot data ramp, not artifact chrome, and would
  undo PRODUCT-0007 §10's separation.
- **Read `UiPalette` directly in `Table::draw`:** makes a table's completed
  appearance implicit application state and leaves authored skins nowhere to
  live atomically with their frames.
- **Leave `Table::new` optional and fall back internally:** permits an omitted
  consumer and makes partial theming representable.
- **Store skin beside `PanelEntry`:** allows a frame and skin to change on
  different paths; the artifact is their atomic ownership boundary.
- **Include panel headers or markdown containers:** crosses from artifact chrome
  into editor chrome and would require ownership this record rejects.
- **Inline arbitrary properties:** creates an open styling language before a
  closed semantic vocabulary has been proved.
- **Validate declared translucent colors without compositing:** tests a color
  the reader never sees and can pass while selected text is illegible.
- **Use 4.5:1 for gridlines and scrollbars:** applies a text criterion to
  non-text UI components; any future gate there uses the appropriate 3:1 rule.

## Consequences

- Table chrome becomes a portable, testable artifact value rather than ambient
  renderer constants.
- SQL results, Rune table views, and markdown cell tables share one renderer and
  one default without requiring a protocol change.
- A later `.skin(...)` cut has an atomic carrier and a renderer whose complete
  vocabulary is already proved.
- A broader visualization-theme identity remains deferred until table and plot
  evidence both exist.
