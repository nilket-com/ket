# ADR 0019: relative line stroke width

Status: accepted 2026-08-04. This record decides one thing: line marks may carry
a resolved, relative stroke width at PRODUCT-0007's Layer stage, and line paint
and geometric hit-testing honor it. It adds no Rune surface; PRODUCT-0020 is the
first authorized spelling.

## Context

`LineMarkRenderer::draw` hardcodes a two-logical-pixel `Stroke`, while its
geometric hit-test uses a fixed six-pixel radius. Width is absent from
`LinePreparedData`, which is already the correct ownership boundary. Making
width authorable requires moving the draw constant into the accepted settings
cascade and relating the hit region to the visible stroke.

PRODUCT-0017 established the shape for mark presentation: a validated relative
factor in typed `LayerSettings`, selected through PRODUCT-0007's five
right-biased tiers and resolved independently per authored layer. This record
reuses that shape without abstracting `PointSize` and `StrokeWidth` into a
generic relative-value type. Two examples are not enough evidence for that
abstraction.

## Decision

### 1. Stroke width is a bounded relative linear factor

`StrokeWidth` is a private validated factor. `1.0` preserves the existing 2px
stroke; `2.0` produces 4px. The domain is finite `0.25..=8.0`, yielding 0.5px
through 16px. It is neither absolute source pixels nor a Scale-stage value.

### 2. Existing cascade selection applies

`stroke_width: Override<StrokeWidth>` is added to typed `LayerSettings` and the
existing five-tier `PlotSettings` cascade. Later tiers select rather than
multiply. Each line layer may resolve a distinct value over one shared plot
settings model. Existing constructors inherit; defaults resolve to
`StrokeWidth::ONE`.

### 3. Width is render-time presentation

`LinePreparedData` remains unchanged. `LineMarkRenderer` derives the full stroke
width from `ResolvedPlotSettings` when drawing. Ordinary line kernels continue
through the same renderer, so the default path has one drawing implementation
and byte-identical SVG.

A styled one-layer plot retains that mark's established prepared ranges. Merely
entering the layer-aware carrier must not route a line through PRODUCT-0013's
multi-layer shared-domain padding: width is presentation, not scale. Genuine
multi-layer compositions keep PRODUCT-0013's shared-domain path unchanged.

### 4. Hit geometry follows the visible stroke

The current 2px line has a 6px geometric hit radius: one visible half-stroke plus
five pixels of interaction tolerance. Preserve that tolerance exactly:

```text
hit radius = resolved stroke width / 2 + 5px
```

Thus a 4px line has a 7px radius. Hover wording and topmost-layer ranking remain
PRODUCT-0013's provisional interaction bridge and do not change.

### 5. Guide behavior is unchanged

The line legend's fixed 3px sample is not resized. That sample belongs to Guide,
not the mark renderer, and PRODUCT-0013 deliberately leaves guide composition
provisional. A later Guide record may relate samples to marks.

## Acceptance gates

1. `StrokeWidth::ONE` resolves to 2px and `2.0` to 4px; invalid values are
   unrepresentable.
2. All five tiers select right-biased values, pane override wins, and no values
   multiply.
3. Two line layers over one result can paint distinct widths without changing
   encoding, domain, or order.
4. `LinePreparedData` contains no width and is identical across rendered widths.
5. Default standalone and layered line SVG remains byte-identical, including a
   styled one-layer factor `1.0` retaining the standalone line range; factor
   `2.0` changes only the stroke and paints it at 4px.
6. A cursor between the default 6px and doubled 7px hit radii misses at `1.0`
   and hits at `2.0` through the layer-aware path.
7. Legend SVG is identical across stroke widths.
8. Non-line renderers, Rune syntax, execution, identity, placement, headings,
   and dashboard behavior are unchanged.

## Stop condition

> This record may add `StrokeWidth`, typed settings fields, existing-cascade
> carriage, and line draw/hit consumption. If implementation requires Rune
> syntax, prepared-data width, a new cascade algebra, guide or hover wording,
> another renderer change, plot-spec migration, execution, transport, identity,
> placement, or dashboard work, it stops.

## Rejected

- **Absolute source pixels:** contradicts the relative precedent and couples
  source to display density.
- **Scale the six-pixel hit radius:** multiplies interaction tolerance and makes
  thick lines disproportionately easy to select.
- **Resize the legend sample:** acquires Guide behavior.
- **Generic relative-factor type now:** two settings do not establish which
  bounds, units, and projections a third setting will share.

## Consequences

- Line styling becomes per-layer render state while relation preparation stays
  unchanged.
- PRODUCT-0020 may add `.stroke_width(2.0)` to the closed modifier register and
  must add no renderer semantics.
