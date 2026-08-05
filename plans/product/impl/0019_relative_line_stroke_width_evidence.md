# PRODUCT-0019 implementation evidence

Recorded 2026-08-04 after the two substrate commits, the PRODUCT-0020 host-path
correction, and the complete cross-feature verification pass. PRODUCT-0019 is
substrate-only; PRODUCT-0020 supplies its first Rune producer.

## Stage 0 findings

All four feasible branches held.

1. `LineMarkRenderer` remains the single drawing body used by standalone and
   layered line paths. No parallel stroke implementation was introduced.
2. `MarkRenderer::hit_test` already receives the layer-resolved settings, so
   the visible-width hit rule required no interaction protocol change.
3. `LinePreparedData` remains presentation-free and line has no raster or cache
   mode to extend.
4. The existing typed `LayerSettings` path carried a second private validated
   value without a generic relative-factor abstraction.

PRODUCT-0020 later exposed one missing substrate boundary: a styled single line
entered PRODUCT-0013's 10% multi-layer padding rather than retaining line's 5%
prepared range. PRODUCT-0020 halted; PRODUCT-0019 §3 and gate 5 were amended,
and the one-layer factory branch now takes its layout from the prepared mark.
The multi-layer shared-domain path is unchanged.

## Gate ledger

1. **Stroke width is validated and linearly projected.**
   `product_0019_stroke_width_is_bounded_and_layer_constructors_inherit` covers
   both accepted boundaries, zero, negatives, over-bound values, NaN, and both
   infinities. It observes `ONE -> 2px` and `2.0 -> 4px`; private fields and the
   validating constructor make invalid values unrepresentable.
2. **All five tiers select rather than multiply.**
   `product_0019_stroke_width_selects_each_layer_before_pane_override` supplies
   distinct library, application, authored, layer, and pane factors. It peels
   the winning tiers and includes a non-product control.
3. **Sibling line layers resolve independently.**
   The cascade assay resolves two distinct layer values, and
   `product_0019_line_layers_paint_independent_resolved_widths` observes 1px and
   4px strokes in authored order over one shared domain.
4. **Preparation owns no width.**
   `product_0019_stroke_width_is_render_state_not_prepared_line_data` asserts
   that `LinePreparedData` has no width carrier and that drawing consumes
   `settings.stroke_width.width_px()`.
5. **Defaults and a factor of one preserve the established plot.**
   `product_0019_default_and_doubled_line_widths_keep_one_drawing_body` pins the
   exact standalone and layered SVG baselines and the 4px doubled stroke. The
   production host lifecycle in
   `product_0020_stroke_width_reaches_host_and_combines_with_point_size` compares
   bare and styled `1.0` SVG byte-for-byte, thereby covering both rendering and
   the mark-specific single-layer range.
6. **Hit geometry follows half the stroke plus fixed tolerance.**
   `product_0019_styled_hit_follows_half_stroke_plus_fixed_tolerance` places a
   cursor at 6.5px: factor one misses and factor two hits through resolved layer
   settings.
7. **Guide output stays fixed.**
   `product_0019_stroke_width_does_not_acquire_guide_behavior` compares exact
   legend SVG across factors one and two.
8. **No unrelated renderer or product subsystem changed.**
   Only line paint/hit consumption and the styled-one-layer layout correction
   changed plot behavior. Full Rune execution, identity, headings, placement,
   dashboard, Toron, and libaryon suites remain green; multi-layer domain tests
   retain PRODUCT-0013's 10% shared ranges.

## Commit and scope trace

- `cbd78b70` — accepted PRODUCT-0019/0020 records and implementation plan;
- `ddb46930` — validated value, typed layer settings, and five-tier selection;
- `0d3740ec` — render-time line paint/hit resolution, fixed guide, and
  architecture closure;
- `01a66f4d` — styled-one-layer layout correction discovered by PRODUCT-0020's
  production-host gate, plus the first Rune producer.

`LinePreparedData`, all non-line renderers, Toron, libaryon, dashboard,
execution, identity, and placement production code were untouched.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
281 passed; 5 live-service tests ignored; 19 architecture tests passed

cargo check -p polariton --no-default-features
passed with the pre-existing test-only create_layered_plot dead-code warning

cargo test -p libaryon
1411 passed; 22 ignored; 13 architecture tests passed; quit-latency test passed

cargo build -p polariton
passed

git diff --check
passed
```

All eight PRODUCT-0019 gates are discharged.
