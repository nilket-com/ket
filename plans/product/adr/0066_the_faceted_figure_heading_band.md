# ADR 0066: the faceted figure heading band

Status: proposed 2026-08-09. This record separates a multi-cell figure's
global heading from its first-row facet labels. It adds no Rune surface and
changes only multi-cell figures whose global title or subtitle is visible.

## Context

PRODUCT-0048 retained identical per-cell top space for non-deduplicable facet
labels, but the implementation tiles the first row at the figure origin.
`render_global_headings` draws the figure title at `title_offset == 20`, while
`PlotGridKernel::render_overlay_layer` draws each first-row facet label at
`cell_bounds.y + 8`. Both therefore occupy the same top band. `test21.rn`
exposes the collision in all four faceted bar forms.

This is not repairable by moving one string. The title is figure chrome and a
facet label is cell chrome; their coordinate spaces need distinct regions.
PRODUCT-0048's stop condition explicitly returns any panel-label placement
change as its own record.

## Decision

### 1. A visible global heading owns one band above the tiled cells

For a multi-cell grid, `grid_tiled_area` reserves a top inset before dividing
the remainder into congruent cells:

```text
figure bounds
├── global heading band
└── congruent tiled cells
    └── each cell's existing panel-label top space
```

A fallback or shown title reserves the existing `plot_padding_top`; this is the
same title budget an ordinary plot already uses. A shown subtitle extends the
band through `subtitle_offset + subtitle_size`, with the facet label's existing
8-pixel inset as clearance. When both heading properties resolve hidden, the
band is zero and the cells reclaim it.

The inset is computed once and consumed by plot, overlay, hover, legend
anchoring, and test geometry through the same `grid_tiled_area` function. No
renderer or cell gains an independent offset.

### 2. Cell geometry and one-cell parity remain authoritative

Every cell keeps the same `plot_padding_top`, label placement relative to its
cell, width, and height as every sibling. Only the common tiled origin and
available height move. PRODUCT-0048's congruence invariant therefore survives.

One-cell grids retain their direct ordinary-plot path and remain byte-identical.
Ungridded figures, title text positions, font sizes, and the title/subtitle
surface are unchanged.

## Acceptance gates

1. A four-cell figure with a shown title renders every first-row facet label
   below `title_offset + title_size`; presence assertions alone do not
   discharge the gate.
2. The title remains at its exact pre-correction position. The cells move; the
   heading does not.
3. The identity holds at two substantially different figure heights, proving
   the separation is a fixed heading band rather than a viewport fraction.
4. Shown subtitle content extends the band below the subtitle; hidden title and
   subtitle reclaim it.
5. Plot, overlay, hover, and default legend placement consume the identical
   tiled rectangle. A cursor at a visibly rendered mark still hits that mark.
6. Every cell's inner width and height remain numerically congruent.
7. The one-cell grid/original-plot parity oracle and every ungridded baseline
   remain byte-identical.
8. `test21.rn` is the live exhibit; all eight bar forms remain otherwise
   unchanged, and the full workspace and PRODUCT-0057 raster evidence pass.

## Consequences

Faceted titles and panel labels become separate chrome bands. Multi-cell plots
with visible headings lose one fixed strip of vertical plot space; that is the
only intended pixel difference. Hidden-heading grids reclaim the strip.

This is another assertion-gap specimen under PRODUCT-0057: every title and
panel label existed at its expected independent coordinate, but no gate joined
their extents. The draw-operation stream contains enough position and nominal
size information to automate the corrected correspondence.

## Rejected

- **Tune `title_offset` in `test21.rn`:** there is no authored offset, and a
  viewport-specific number would repeat PRODUCT-0051's compensation defect.
- **Move facet labels within the existing cell top band:** title and cell chrome
  would still share one region, and smaller cells would collide again.
- **Shrink or abbreviate titles:** changes content to compensate for missing
  layout.
- **Give only first-row cells more top padding:** breaks cell congruence under
  one shared domain.

## Stop condition

> This record may reserve one shared heading band above a multi-cell tile,
> consume it through the existing grid geometry authority, and add the named
> gates. If the repair requires unequal cells, measured text as a layout
> authority, a Rune setting, a title-content rewrite, a one-cell pixel change,
> or any change outside multi-cell figures with visible headings, work stops
> and the requirement returns to PRODUCT-0048.
