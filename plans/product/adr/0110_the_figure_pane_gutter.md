# ADR 0110: the figure pane gutter

Status: proposed 2026-08-15. Faceted figures presently use an unowned
`GRID_GAP = 12.0`: a useful hairline separation, but not an authored layout
choice. A dashboard composition sometimes needs a deliberate 24 px gutter.
This record makes that shared space a figure-grid setting without mistaking it
for plot padding, a border, or a per-pane margin.

It follows PRODUCT-0106's vertical-guide reservation and precedes typography.
0106 established that title, facet, and plot-area reservations have one ordered
authority. A pane gutter is another layout reservation with a different owner:
it exists **between** cells.

## Context

`PlotGridKernel` computes every cell through `grid_cell_bounds`
(`plot/grid.rs:892`). The current formula subtracts `GRID_GAP` once for every
internal divider, then divides the remainder:

```text
cell_width  = (outer_width  - gap_x * (columns - 1)) / columns
cell_height = (outer_height - gap_y * (rows    - 1)) / rows
cell_origin = outer_origin + index * (cell_size + gap)
```

Paint, hover, shared-extent calculation, and the facet-label row all obtain
their cells from that function. This is already the right geometry authority;
the defect is that its 12 px input is a private constant.

`plot_padding_*` is not a candidate home. It belongs inside every cell's plot
area, after the title and facet bands have been reserved. A gutter belongs
between cell bounds, before either cell owns its plot area. Applying a gutter as
padding would charge it to both neighbouring cells and make a requested 24 px
divider consume 48 px.

## Decision

### 1. `pane_gutter(px)` is a figure element for faceted grids

Rune adds one figure element:

```rune
figure([
	facet("region"),
	pane_gutter(24),
	bar("quarter", "sales"),
])
```

It accepts one finite, non-negative numeric literal, at most once. A duplicate
refuses at the second call's span. It is meaningful only to a faceted grid;
using it in a figure with no `facet(...)` refuses at `pane_gutter(...)` rather
than silently authoring a setting no rendered figure can consume.

The unit is physical canvas pixels, matching `plot_padding_*`, title offsets,
and the existing 12 px default. This record does not introduce relative spacing
or a responsive unit.

### 2. Figure-grid layout has its own two-tier cascade

`FigureGridLayoutSettings` carries `pane_gutter`, separately from
`PlotSettingsLayer`:

```text
application grid default → authored figure override
```

The application default is 12.0, reproducing the current geometry. A future GUI
preference may write that first tier; `pane_gutter(...)` writes only the authored
figure tier. There is deliberately no graphical-layer tier and no pane-local
override: a divider has two neighbouring panes, so no single pane owns it.

The authored value persists as Rune source with its figure. The application
default is a user preference, not document meaning, and does not enter the
session schema.

### 3. `grid_cell_bounds` remains the only cell-geometry authority

The resolved gutter replaces the private constant only at
`grid_cell_bounds`. Every caller—paint, hover, shared extent, clip bounds, and
facet-label placement—passes the same resolved figure-grid settings. No caller
subtracts a gutter independently.

For a requested gutter `g`, each internal divider is exactly `g` wide in both
geometry and hit testing. The formula subtracts `g * (count - 1)` once before
division; it does not apply `g` to each pane. An equivalent implementation may
assign `g / 2` to each adjoining cell only if the resulting cell bounds and
inter-cell distance are identical to this formula.

### 4. Dynamic-size safety retains the existing per-axis clamp

At a small window a physical request can exceed the available tiled extent.
Layout therefore retains the current safety rule independently on each axis:

```text
effective_gap_x = min(requested_gutter, outer_width  / columns)
effective_gap_y = min(requested_gutter, outer_height / rows)
```

This keeps cell extents non-negative under resize. It is a geometric safety
clamp, not a second authored value: the resolved requested gutter remains one
number, and normal-sized figures render it exactly. A future requirement to
preserve a minimum cell size or to report a clipped gutter is a layout-policy
record, not an excuse for a hidden second setting here.

### 5. Gutter composes outside PRODUCT-0106's guide bands

The order is fixed:

```text
legend / outer padding
→ figure heading band
→ tiled grid with pane gutters
→ per-cell facet band + plot padding
→ plot area
```

Thus a gutter changes cell rectangles and consequently the available plot area,
but does not change the title, subtitle, facet-label, or plot-padding formulas.
`pane_gutter(0)` removes only the inter-cell reservation; it does not remove a
facet row or merge the cells' independent coordinate transforms.

### 6. This is not a border or a typography setting

No stroke is drawn and no plot chrome changes. The visible gap is the existing
canvas background between distinct clipped cells. Typography remains the next
track: it can change guide reservations inside a cell, but it must not acquire a
second way to alter inter-pane geometry.

## Acceptance gates

1. **Default parity.** A legacy faceted figure with no authored gutter retains
   its exact 12 px horizontal and vertical internal dividers.
2. **One gutter, once.** A two-column figure with `pane_gutter(24)` has cell
   width `(outer_width - 24) / 2` and an inter-cell distance of exactly 24 px;
   it is not 48 px.
3. **Two axes.** A 2×3 grid uses exactly one 24 px divider between every adjacent
   row and column, with no outer gutter invented at the tiled bounds.
4. **Zero is local.** `pane_gutter(0)` makes adjacent cell bounds meet while
   retaining their separate clip regions, transforms, facet labels, and plot
   areas.
5. **Paint and hover agree.** A point inside a non-origin cell is painted and
   hovered by that same cell under an authored 24 px gutter. This is the
   PRODUCT-0097 coordinate-seam pair applied before another grid geometry input
   can diverge.
6. **Extent agrees.** Shared gridlines and figure-level annotations derive their
   extent from the same guttered first and last cells; no unguttered extent
   survives beside guttered paint.
7. **Normal formula, unchanged marks.** A known domain fraction lands at the
   same fraction of its new cell plot area. Mark geometry formulas are unchanged;
   only their input rectangle moves.
8. **Small-window safety does not become state.** Under a small tiled extent,
   an oversized authored gutter cannot create negative cell dimensions or
   overlapping bounds; its effective gap follows §4's per-axis clamp. The
   resolved figure-grid setting still holds the authored requested value, not
   that effective value.
9. **Figure-only Rune surface.** `pane_gutter(24)` appears once in a valid
   faceted figure, reaches the authored figure-grid tier, and never sets a
   `PlotSettingsLayer` padding or border field.
10. **Invalid authoring refuses precisely.** Negative, non-finite, duplicate,
    and non-faceted uses refuse at the offending `pane_gutter(...)` call.
11. **Clamp round-trips as a request, not a result.** Reopening
    `pane_gutter(10000)` after it rendered in a small window preserves 10000;
    rendering it in a larger window applies 10000 rather than the former
    small-window effective gap. Changing the GUI default likewise does not
    rewrite an authored value.
12. **No second geometry authority.** Structural: `GRID_GAP` disappears, and
    every production cell-bound calculation routes through the resolved
    figure-grid gutter.
13. **No-grid parity.** A non-faceted single-plot figure is byte-identical to
    its pre-gutter rendering. This is the zero case for the feature itself: a
    gutter applied at a plot, outer-layout, or global-settings layer would move
    this figure even though it has no internal pane boundary to reserve.

## Stop conditions

> If an authored gutter requires a pane-local override to be useful, stop. A
> shared divider cannot have two independent owners; that requirement would need
> a different concept, such as outer cell margin, rather than weakening §2.

> If a normal-sized 24 px gutter has to be subtracted by any caller other than
> `grid_cell_bounds`, stop. That is the double-authority shape PRODUCT-0106
> removed, now expressed horizontally and vertically between cells.

> If the gutter alters a title, facet, padding, or mark formula rather than only
> the rectangle supplied to it, stop. The composition order in §5 has failed.

> If responsive layout needs a minimum-cell-size policy or visible clipping
> feedback, stop and record that policy separately. §4 only preserves safe,
> non-negative geometry.

## Rejected

**Put the gutter in `PlotPadding`.** Attractive because padding already changes
space. Rejected because it is owned by each plot area; two adjacent panes would
apply the same divider twice and a single-cell plot would gain unrelated margin.

**Use a plot border width.** Attractive because a line can make panes look
separated. Rejected because a border is chrome around one cell, while this is
empty shared layout space. It cannot supply a 24 px breathing room without
inventing strokes and changing plot appearance.

**One pane writes its neighbouring gutter.** Attractive because it avoids a new
figure-layout carrier. Rejected because left/right and top/bottom order would
then decide who owns the same space; the value would be non-commutative and
impossible to persist honestly.

**Make all gutter uses GUI-only.** Attractive because the GUI already knows the
window size. Rejected because dashboard spacing is authored composition. A GUI
default is useful, but it cannot replace an explicit figure override.

**Add responsive or percentage units now.** Attractive because panes resize.
Rejected because the existing layout language is pixel-based and §4 already
handles safety. Relative spacing is a separate visual-design decision.

## Consequences

Authors retain the current hairline-like default while gaining intentional
dashboard spacing where it communicates grouping. The cost is honest: a larger
gutter reduces every cell's available rectangle, so data marks move and scale
only through their existing transforms.

The record also gives typography a clean boundary. Font-size changes reserve
space within cells; pane gutters reserve space between them. Neither may become
the other's escape hatch.
