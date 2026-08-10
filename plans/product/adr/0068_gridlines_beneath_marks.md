# ADR 0068: gridlines beneath marks

Status: proposed 2026-08-09. This record places plot grids behind data marks for
every applicable layout and repairs the existing stacked-area separator needed
to evaluate that ordering honestly. It adds no Rune surface.

## Context

Polariton currently renders canvas and marks in `render_plot_layer`, then draws
major and minor grids together with axes and text in `render_overlay_layer`.
Gridlines therefore overpaint every bar, point, line, heat-carrying mark, and
filled area they cross.

PRODUCT-0067 raises the compatibility-default gridline from alpha `0.2` to the
accepted `0.35` floor. Over canvas this repairs CI-09. Over an opaque mark it
has a different effect: translucent chrome composites into the resolved data
colour, so the displayed pixel is no longer the colour assigned by the palette.
No single fixed grid colour can also guarantee `3:1` against every arbitrary
palette fill. Leaving grids above marks would therefore make PRODUCT-0067
necessary but permanently incomplete.

The Stage 0 Vulkan exhibit compares shipped grid-over, corrected grid-over, and
corrected grid-under for both a continuous stacked area and grouped opaque bars.
The bar fixture contains intra- and inter-category gaps and substantial
headroom. Underlay gridlines remain continuous and readable through the gaps
and exposed canvas, while every bar pixel retains its resolved colour. The
overlay control paints visible grey bands across all three saturated fills.
Bars are the decisive no-tradeoff case: the gaps preserve every grid level, so
underlay loses no reference information while exact mark colour is restored at
every intersection. Continuous area has a real occlusion tradeoff; bars do not.

The first area comparison exposed a falsifiability problem. The existing
stacked-area boundary is hardcoded 20%-black at `0.5px`, nearly invisible on the
dark themes used by the exhibit. Grid-under would consequently be judged while
its intended replacement structural cue was disabled. The corrected comparison
keeps the exact `0.5px` width and substitutes the already-resolved
`mark_decoration_color`; only then does it compare strata.

## Decision

### 1. Plot rendering has four ordered strata

Every ordinary and multi-cell plot renders in this order:

```text
canvas background
major and minor gridlines
data marks
axes, tick marks, tick text, labels, frames, panel labels, legend, headings
```

Gridlines are guides beneath data. Axes and the remaining furniture stay above
data. The rule is global: kernels cannot request overlay grids and no mark kind
gets an orientation-, fill-, or opacity-specific exception.

This ordering preserves resolved data colour at every mark/grid intersection.
It also completes the gridline contrast rule. A visible gridline is adjacent to
`canvas_background`; where an opaque mark covers it, the guide is occluded
rather than rendered with unspecified contrast.

### 2. Layout remains the single geometry authority

The existing Cartesian, categorical-x, and categorical-y grid helpers continue
to derive positions, counts, styles, widths, major/minor subdivision, and
visibility from the same layout, transformer, and resolved settings. They move
as whole draw operations from overlay to the plot layer immediately after the
cell background and before `kernel.plot_with_settings`.

Ordinary and composite rendering share one layout-grid dispatch helper rather
than copying a second match. For a multi-cell grid, each cell performs
background → grid → mark inside its existing cell clip. The overlay pass keeps
the corresponding axes and shared-edge text in the same coordinates. One-cell
grids continue through the ordinary path.

`CategoricalXY`, parallel, and radial layouts draw no independent grid today;
this record does not manufacture one. Axis-like parallel coordinates remain
furniture and keep their accepted ordering.

### 3. Stacked-area boundaries consume mark decoration

The dashboard-reachable `StackedAreaPlotKernel` replaces its hardcoded
`rgba(0, 0, 0, 0.2)` separator with
`ResolvedPlotSettings.mark_decoration_color`. Its width remains exactly
`0.5px`; its path, segment order, fills, range, hover, and ramp-derived colours
remain unchanged in this record.

This is a bounded prerequisite, not the stacked-area surface repair.
`mark_decoration_color` already serves box, violin, candlestick, and funnel mark
structure, so no new role or setting is introduced. The later surface record
still owns the fifth resolved-colour bypass, category ordering, non-positive
semantics, and Rune spelling.

### 4. PRODUCT-0067 and PRODUCT-0068 land as one pixel transition

The two records retain separate plan and behavioral gates: a marks-free oracle
isolates PRODUCT-0067's alpha repair, while intersection controls isolate this
record's order. Their implementation lands together so existing figures do not
pass through an intermediate release where a stronger `0.35` overlay first
tints marks and a following commit removes that tint.

Baseline evidence enumerates the combined final difference classes:

- exposed default gridlines change from alpha `0.2` to `0.35`;
- every grid operation precedes marks instead of following them;
- opaque marks retain exact resolved pixels at intersections;
- the stacked-area separator changes only from hardcoded translucent black to
  the resolved mark-decoration colour; and
- axes, ticks, text, frames, legends, headings, geometry, hit, and hover do not
  move.

Named skins retain their exact chrome values; only their grid draw order changes
where a mark overlaps the guide.

## Acceptance gates

1. A draw-operation oracle pins grid before mark and axis/tick furniture after
   mark for Cartesian, categorical-x, and categorical-y layouts. The assertion
   message states that data colours survive guides; bare inequality does not
   discharge the gate.
2. A Vulkan grouped-bar fixture asserts all three facts jointly: a grid pixel in
   a category gap is the expected grid-over-canvas composite, a grid/bar
   intersection is the bar's exact resolved colour, and a synthetic
   overlay-order control produces a different tinted pixel. Presence of all
   bars and all grids is asserted separately to close vacuity.
3. The bar fixture includes intra- and inter-category gaps, at least three
   saturated series colours, multiple grid crossings, and headroom. A solid
   wall of bars or a grid that never intersects one cannot discharge the gate.
4. A stacked-area Vulkan fixture keeps the exact `0.5px` boundary width, consumes
   `mark_decoration_color`, leaves resolved fills exact, shows grid in headroom,
   and shows no grid-composited fill pixels below the stack top. The old
   hardcoded black treatment is asserted absent.
5. Line and scatter controls retain their exact stroke/fill colours at a grid
   crossing while the same gridline remains visible on adjacent canvas.
6. Multi-cell Cartesian, categorical-x, and categorical-y figures perform the
   same order independently inside every cell clip. A grid cannot escape its
   cell, paint a gutter, or cross a neighboring cell's marks.
7. Faceted axes, labels, titles, shared-edge suppression, heading bands, and
   legends remain at their exact accepted coordinates. Only grid/mark document
   order changes inside each plot extent.
8. One-cell composite/original parity remains exact under the new ordering.
9. Grid visibility, major/minor subdivision, styles, widths, and tick-position
   identity remain unchanged. With every grid disabled, non-stacked-area
   renderings are byte-identical.
10. Layout, rectangles, paths, hit regions, hover prose, domain resolution, and
    category order remain unchanged. Paint and hit continue consuming their
    existing shared geometry authorities.
11. Named plot skins resolve to byte-identical `PlotChrome` values before and
    after this record; no palette assignment or mark alpha changes.
12. Existing baseline refreshes are enumerated once for the combined
    PRODUCT-0067/0068 landing and contain only §4's named difference classes.
13. The Stage 0 comparison remains reproducible through the opt-in Vulkan
    evidence target, and its current/overlay/underlay controls all render real
    grids and marks rather than empty fixtures.
14. The complete Polariton/workspace suite, formatting checks, and all
    PRODUCT-0057 Vulkan probes pass with no ignored test newly admitted.

## Consequences

Opaque marks occlude gridlines. Bars retain reference lines through their gaps;
continuous filled marks rely on their resolved boundaries while exposed
headroom retains the grid. Points and lines visually outrank the guides they
cross.

The rendering model distinguishes guides from furniture for the first time.
This deliberately narrows the earlier convenient use of “chrome” as one
z-order class: grid geometry remains chrome-owned, but its stratum is below
marks; axes, ticks, labels, frames, legends, and headings remain above. The
PRODUCT-0041 grid and PRODUCT-0056 categorical-y evidence are superseded only
where they observed grid/mark order. Their clipping, transformer, domain, and
physical-frame conclusions remain accepted.

The canvas-relative `3:1` rule becomes complete without constraining categorical
palettes or changing authored data opacity. A future translucent mark naturally
composites with the grid beneath it; that is the mark's authored/translucent
behavior, not chrome overpainting an opaque resolved colour.

A mark that fully covers the plot extent leaves no visible grid. Percent-of-total
stacked area is the known instance; gapless full-width percent bars can create
the same condition, while ordinary bars retain grid through their gaps. The
percent-of-total record must decide whether axis ticks and resolved boundaries
are sufficient reference for proportions. If it requires reference lines above
opaque marks, it reopens this stratum explicitly; it may not take an unrecorded
per-mark exception to §1.

## Rejected

- **Keep gridlines above marks:** permanently leaves data-relative contrast
  unspecified and changes palette-resolved pixels at every intersection.
- **Choose per mark:** makes guide semantics depend on whichever kernel happens
  to draw and creates inconsistent layered figures.
- **Make filled marks translucent:** changes data colour and introduces a new
  authored/default-opacity policy to rescue chrome.
- **Adapt grid colour locally:** adds a value- and palette-dependent rendering
  system, breaks one resolved chrome value, and still needs a boundary rule.
- **Draw axes beneath marks too:** axes and tick furniture delimit the frame;
  the exhibit and contrast argument concern grids only.
- **Judge grid-under with the hardcoded stacked-area boundary:** the control is
  nondiscriminating on dark themes because both intended structural cues are
  nearly absent.
- **Strengthen the stacked-area boundary width:** the experiment needs a
  visible resolved colour, not a second geometry change.
- **Land PRODUCT-0067 first and refresh again here:** creates a known-temporary
  stronger overpaint state and churns the same baselines twice.

## Stop condition

> This record may move existing grid draw operations before existing marks in
> every ordinary and composite layout, factor shared dispatch, replace the
> stacked-area boundary colour with the existing `mark_decoration_color` while
> preserving `0.5px`, add the named gates and evidence, and co-land with
> PRODUCT-0067. If the change requires a per-kernel grid policy, a new role or
> Rune setting, mark or palette opacity changes, adaptive contrast, changed grid
> geometry, changed axes/ticks/text/legend order, a boundary-width change,
> altered hit/hover/domain behavior, or a cell-clipping exception, work stops
> and the requirement returns as a broader rendering record.
