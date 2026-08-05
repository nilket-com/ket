# ADR 0021: normalized bar-slot width

Status: accepted 2026-08-04. This record decides one thing: a bar mark may carry
a normalized width within its allocated categorical slot, and bar painting and
hit geometry consume the same resolved rectangle. It adds no Rune surface;
PRODUCT-0022 is the first authorized spelling.

## Context

`BarPlotKernel` currently reserves 80% of each category band for its bar group,
divides that group among series, then hardcodes 5% padding on both sides of each
subgroup slot (`bar.rs:60-80`, with the horizontal mirror at `:98-120`). Thus a
bar occupies 72% of its full-band subgroup slot through two independently hidden
insets.

Hover independently reconstructs only the broader subgroup slot and counts its
padding as a hit (`bar.rs:151-215`). Once width is authorable, blank padding
would grow while remaining interactive. Paint and hit geometry must become one
calculation before the setting can ship.

Point size and stroke width established typed Layer-stage settings, but bar
width is evidence against a generic numeric setting: it is a bounded proportion,
not a relative pixel factor.

## Decision

### 1. Width is normalized slot occupancy

`BarWidth` is a private validated fraction in `(0.0, 1.0]`. The complete
category band is divided equally among the bar's resolved series; the value is
the fraction of one resulting subgroup slot occupied by the visible rectangle.
No additional category-group inset is applied.

`BarWidth::DEFAULT` is `0.9`, leaving 5% of the full subgroup slot on each side.
`1.0` fills the slot exactly: adjacent series and adjacent categories share a
boundary and their bars touch. `0.9999` leaves only its normalized subpixel-scale
remainder, not a hidden fixed gap.

This deliberately changes the old default geometry from 72% to 90% of the full
subgroup slot. Preserving the old pixels with a surprising `0.72` default would
make the new public scale permanently dishonest. This cut does not introduce a
separate category, cluster, or absolute-pixel gap. Vertical and horizontal bars
use the same fraction.

### 2. Existing cascade selection applies

`bar_width: Override<BarWidth>` joins typed `LayerSettings` and PRODUCT-0007's
five tiers. Later tiers select; fractions never multiply. Existing constructors
inherit and defaults resolve to `BarWidth::DEFAULT`.

`PointSize`, `StrokeWidth`, and `BarWidth` remain separate validated types. The
third setting validates the closed modifier/cascade architecture, not shared
units or bounds.

### 3. One rectangle owns paint and hit geometry

Bar geometry is factored once from prepared values, orientation, transform, and
resolved width. Painting fills those rectangles. Hover tests those exact same
rectangles; padding is not a hit.

Hover wording remains unchanged. This record changes only whether a cursor over
visible geometry counts as a hit, not how a hit is described.

### 4. Width is render-time presentation

`BarPreparedData` contains no width. Width changes neither categorical bands,
domains, axis labels, prepared values, nor source resolution. The fixed legend
swatch remains Guide behavior and does not resize.

### 5. Styleability is not layer composability

`LayerMark` remains exactly `Line | Scatter`. PRODUCT-0013's ordered shared-
Cartesian layer set does not gain Bar. A bar may consume a Layer-stage setting
as a standalone mark without becoming legal in `layers([...])`.

## Acceptance gates

1. `BarWidth::DEFAULT` is `0.9`; finite values greater than zero through `1.0`
   are representable and zero, negatives, values above one, NaN, and infinities
   are not.
2. All five tiers select right-biased values without multiplication.
3. Default vertical and horizontal bars occupy exactly 90% of the full-band
   subgroup slot; `0.5` occupies exactly half; and `1.0` makes neighboring
   rectangles share a boundary with no hidden gap.
4. Drawing and hit-testing consume one rectangle calculation in both
   orientations.
5. A cursor in visible fill hits, while one in width-created padding misses;
   hover prose for a retained hit is unchanged.
6. `BarPreparedData` has no width and width changes no layout, prepared value,
   label, category, group, or guide output.
7. `LayerMark` remains the two-variant line/scatter type and multi-layer shared
   domain, ordering, and compatibility behavior is unchanged.
8. Rune syntax, execution, transport, identity, placement, headings, and
   dashboard behavior are unchanged.

## Stop condition

> This record may add `BarWidth`, typed settings carriage, a standalone bar
> layer-settings carrier, full-band subgroup allocation, and shared bar
> paint/hit geometry. If implementation requires adding Bar to `LayerMark`,
> changing category domains or series membership,
> prepared-data width, guide output, hover wording, another renderer, plot-spec
> migration, Rune syntax, Toron or libaryon, execution, identity, placement, or
> dashboard work, it stops.

## Rejected

- **Relative multiplier:** values above one overlap neighboring slots and make
  the accepted bound depend on existing padding constants.
- **Absolute pixels:** breaks categorical responsiveness and orientation parity.
- **An implicit one-pixel gap at `1.0`:** mixes normalized and device-pixel
  units, changes with DPI and export resolution, and makes full occupancy false.
- **Preserve the legacy pixels with a `0.72` default:** keeps two historical
  insets observable forever and gives the normalized scale a surprising default.
- **`bar_percent`:** leaves `0.8` versus `80` ambiguous.
- **`gap_width`:** describes the inverse and cannot distinguish category,
  cluster, and subgroup gaps.
- **Generic numeric setting:** the three settings now prove different units and
  validation domains.

## Consequences

- Narrow bars have narrow hit regions rather than invisible interactive wings.
- The legacy 80% category-group inset is retired; spacing is now exactly what
  `bar_width` states.
- PRODUCT-0022 may spell this setting without weakening PRODUCT-0013's layer
  type.
