# ADR 0017: relative scatter point size

Status: accepted 2026-08-04. This record decides one thing: scatter marks may
carry a resolved, relative point size at PRODUCT-0007's Layer stage, and every
scatter rendering mode and geometric hit-test must honor it. It adds no Rune
surface. PRODUCT-0018 may spell the setting as the closed postfix modifier
`scatter(...).point_size(2.0)` only after this substrate is proved.

## Context

Scatter radius is currently an unowned preparation argument. `factory.rs:80`
and `factory.rs:317` both pass the literal `3.0`, and
`ScatterPreparedData.point_size_px` carries it beside relation-derived data.
Vector, downsampled, and geometric hit-testing read that field, but raster does
not. `ScatterRasterKey` also omits it. Making the value authorable at the Rune
surface first would therefore create two correctness failures: rasterized plots
would ignore the setting, and a size change could reuse an image cached under a
different size.

PRODUCT-0007 already owns the semantics. Its plot grammar includes the Layer
stage, its settings model reserves the five-tier precedence

```text
library defaults -> application theme -> authored spec -> layer -> pane override
```

and its only value-setting algebra is right-biased `Override<T>`. What is
missing is a typed layer-local setting and renderer plumbing faithful to that
model. Today host plots flatten `PlotSettings` before a kernel runs, and
`LayeredPlotKernel` gives the same `ResolvedPlotSettings` to every mark. A
single global `PlotSettings.layer` value cannot describe two scatter layers of
different sizes.

## Decision

### 1. Point size is a bounded relative linear factor

`PointSize` is a validated, unitless factor. `1.0` preserves the existing
three-logical-pixel scatter radius. `2.0` produces a six-pixel radius: twice the
linear extent and therefore four times the area. It is neither a data encoding
nor an absolute pixel value in source.

The accepted first-cut domain is `0.25..=8.0`. Values outside that interval,
zero, negatives, NaN, and infinities have no `PointSize` value. The upper bound
limits raster work, which grows with the area of a mark; the lower bound avoids
promising sub-pixel behavior that the raster path cannot reproduce faithfully.
Widening either bound requires renderer evidence, not a surface-only change.

### 2. Cascade tiers select; they do not multiply

Point size uses `Override<PointSize>` in the existing five-tier cascade. Later
tiers replace earlier tiers. If the authored tier says `2.0` and the pane tier
says `0.5`, the result is `0.5`, not `1.0`.

Multiplication is itself associative, but it would be a third cascade algebra
and would turn precedence into accumulation. PRODUCT-0007 §6 authorizes only
right-biased `Override` and `Presence`; this record adds no algebra.

### 3. Each graphical layer owns typed settings

```text
LayerSpec { mark, encoding, settings: LayerSettings }
LayerSettings { point_size: Override<PointSize> }
```

The settings record is typed and flat. It is not a string-keyed option map and
contains no modifier syntax. Existing constructors produce all-Inherit
settings. Existing `AuthoredPlotSpec` remains unchanged.

For a layered plot, resolution substitutes each `LayerSpec.settings` into the
one accepted `layer` tier and resolves the five tiers independently. Library,
application, authored, and pane tiers are shared; the layer tier varies. This
is the first production realization of the previously dormant layer slot.

An unstyled one-layer specification keeps PRODUCT-0013's established kernel
path. A styled one-layer specification uses the layer-aware renderer path; it
cannot discard the setting merely to retain the old concrete kernel type.

### 4. The cascade reaches kernels without unflattening resolved settings

`ResolvedPlotSettings` remains the flattened renderer projection required by
PRODUCT-0007. `PlotWidget` retains the full `PlotSettings` model beside that
projection and calls new cascade-aware kernel hooks.

The hooks have defaults that resolve once and delegate to the existing
`PlotKernel` methods, so ordinary kernels and their consumers do not migrate.
`LayeredPlotKernel` alone overrides them: it resolves once per authored layer
and passes that layer's flattened settings to its mark renderer. Geometric
hit-testing follows the same route.

This is not a second settings model. There remains one `PlotSettings`, one
right-biased resolution operation, and one flattened `ResolvedPlotSettings`
per rendering decision.

### 5. Size is presentation, never prepared relation data

`ScatterPreparedData` contains series, domains, axes, labels, colors, and point
counts derived from the stamped result. It contains no point size. Preparing
the same relation and specification for two sizes produces the same prepared
data identity and values.

The scatter renderer derives its logical-pixel radius from the resolved
`PointSize` at draw or hit-test time. Changing a pane override therefore does
not require data preparation or kernel reconstruction.

### 6. All renderer paths share one radius contract

Vector, downsampled, rasterized, and geometric hit-testing use the same resolved
radius. Rasterization draws a clipped filled disc rather than depositing one
pixel, and `ScatterRasterKey` includes the point-size identity. A size change
cannot return a cache entry built at another size.

The geometric hit radius remains the drawn radius plus the existing two-pixel
interaction tolerance. The tolerance is unchanged; only the drawn radius is
now resolved rather than prepared.

### 7. Guide behavior remains deferred

Point size does not resize, merge, or otherwise reinterpret the legend swatch.
PRODUCT-0013's guide bridge remains unchanged. A data-driven size channel,
size legend, or guide-scale decision belongs to PRODUCT-0007's Encoding and
Guide stages and requires another record.

### 8. Acceptance gates

1. `PointSize::ONE` resolves to the existing three-pixel radius and `2.0` to a
   six-pixel radius; invalid and out-of-range values are unrepresentable.
2. Point size follows all five precedence tiers by right-biased selection. A
   pane override beats a differing layer value, and no pair is multiplied.
3. Two scatter layers over one shared result can resolve different point sizes
   and paint those distinct radii without changing their encodings or domain.
4. Scatter preparation contains no size and is identical regardless of the
   resolved point size used later.
5. Default-size standalone and layered vector/downsampled rendering retains the
   established three-pixel circles; a factor of two produces six-pixel circles.
6. Raster rendering paints a clipped disc from the same radius contract rather
   than one deposited pixel.
7. Raster cache identity includes point size: changing only size misses the old
   cache and produces the new footprint; repeating the same size reuses it.
8. A cursor outside the default hit radius but inside the doubled hit radius
   misses at `1.0` and hits at `2.0`, through the cascade-aware layered path.
9. Unstyled one-layer line and scatter specifications retain PRODUCT-0013's
   existing kernel path and rendering evidence; non-scatter kernels are
   unchanged.
10. Changing point size does not change legend geometry or wording.
11. No Rune syntax, envelope recognition, view identity, routing, execution,
   panel placement, dashboard behavior, or Toron transport changes.

### 9. Stop condition

> This record may add `PointSize`, `LayerSettings`, the point-size field in the
> existing settings cascade, retention of the full cascade at the plot widget,
> default cascade-aware kernel hooks, the layered-kernel override, and scatter
> vector/downsample/raster/hit plumbing. If implementation requires a Rune
> spelling, a new cascade algebra, data-driven size, a guide decision, changing
> a non-scatter renderer, migrating `AuthoredPlotSpec`, changing shared-domain
> compatibility, or changing execution, transport, identity, placement, or the
> dashboard, the work stops.

The expected trigger is treating the raster defect as optional. A surface that
can select size may not ship while any accepted scatter render mode ignores it.

### 10. Not authorized

No `.point_size` Rune method yet. No `layer([...])` constructor. No figure-wide
point-size spelling, pane control, data-driven size encoding, bubble-plot
change, scale composition, legend sizing, interaction wording, new mark, or
dashboard change. No change to `ScatterLinePlotKernel` paint order.

## Rejected

- **Keep size in `ScatterPreparedData`:** confuses presentation with stamped
  relation data and makes a settings-only change require preparation.
- **Apply the layer value after flattened settings:** lets layer beat pane
  override and contradicts PRODUCT-0007's accepted order.
- **One global layer value for a layered plot:** cannot represent two scatter
  layers with different settings.
- **Multiplicative cascade:** introduces a third algebra and changes override
  precedence into accumulation.
- **Leave raster as one pixel:** makes the setting render-mode-dependent and
  makes its cache observably stale.
- **Resize the legend swatch:** acquires Guide semantics in a mark-style record.

## Consequences and sequencing

- Scatter size becomes a renderer setting rather than prepared data.
- The dormant Layer precedence tier becomes independently meaningful for each
  graphical layer without migrating ordinary kernels.
- Raster output at the default size intentionally changes from an incorrect
  one-pixel deposit to the accepted three-pixel-radius footprint.
- PRODUCT-0018 may add the closed postfix LayerModifier register and
  `.point_size(2.0)`. It must lower to `LayerSettings` and add no renderer rule.

