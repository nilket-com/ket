# PRODUCT-0042 implementation plan: the facet figure surface

Status: implemented 2026-08-06. This plan implements the accepted surface in
`plans/product/adr/0042_the_facet_figure_surface.md`.

## Scope

- Add `facet(...)` as structural figure metadata, including static literal
  binding resolution and span-attributed duplicate, arity, context, and body
  refusals.
- Carry the facet intent beside the sink output into the Rune head; keep it out
  of `PlotSettingsLayer`, cascade resolution, mark ink, palette, and skin.
- Construct the accepted value-facet grid at head completion, using the
  derived floor-sqrt arrangement and the stamped source frame so a facet field
  omitted from the mark projection remains available.
- Route unfaceted figures through the one-cell grid path as well as faceted,
  preserving ordinary output through the banked parity oracle. Support plain,
  styled, and layered bodies without changing identity or host lifecycle.
- Exercise `../uat/test14.rn`, whose two figures share bound headings and facet
  bindings while covering skins, palettes, bounds, ticks, minor ticks, and the
  degenerate mark-domain overlap.

## Sequencing

1. Bank the one-cell SVG parity oracle before changing head routing (`906bb8a9`).
2. Add the structural envelope field and `facet(...)` lowering.
3. Add the derived arrangement and head/grid construction for plain, styled,
   layered, and one-cell paths.
4. Run focused and full Polariton/architecture suites, then record the gate
   evidence and fixture location.

## Stop conditions

Stop and split a new record if implementation requires a new grid semantic,
capacity rule, scale-sharing policy, authored arrangement, guide behavior,
identity/lifecycle change, or mark/palette/skin coupling.
