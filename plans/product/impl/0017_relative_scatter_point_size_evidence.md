# PRODUCT-0017 implementation evidence

Recorded 2026-08-04 after the three implementation commits and the complete
cross-feature verification pass. PRODUCT-0017 is substrate-only; no Rune fixture
can exercise it until PRODUCT-0018 adds the closed postfix modifier register.

## Stage 0 findings

All four feasible branches held.

1. Host plots now retain the existing `PlotSettings` model beside its resolved
   projection. No panel content, protocol, execution, or transport type changed.
2. `PlotKernel::plot_with_settings` and `hover_with_settings` are default hooks.
   Every ordinary kernel retains its established methods; only
   `LayeredPlotKernel` overrides the new seam.
3. Typed `LayerSettings` rides `LayerSpec -> LayerChannelBinding ->
   ResolvedLayerSpec -> LayerRenderer` without changing encoding, compatibility,
   domain, or paint order.
4. `ScatterMarkRenderer` still owns one raster cache. Its existing key gained
   point-size identity; no second cache or renderer path was introduced.

The halt rule did not fire.

## Gate ledger

1. **Point size is validated and linearly projected.**
   `product_0017_point_size_is_bounded_and_layer_constructors_inherit` covers
   both accepted boundaries, zero, negatives, values beyond each boundary, NaN,
   and both infinities. It observes `ONE -> 3px` and `2.0 -> 6px`; the private
   representation plus the validating constructor prevents invalid values.
2. **The five tiers select rather than multiply.**
   `product_0017_point_size_selects_each_layer_before_pane_override` supplies
   distinct library, application, authored, layer, and pane values. Pane wins;
   removing it exposes layer; a positive control proves the result is not the
   product.
3. **Sibling layers resolve independently.** The same cascade assay resolves
   two `LayerSettings` values independently. The vector/downsample assay then
   paints 1.5px and 6px scatter circles over one shared result and domain.
4. **Preparation owns no size.** `ScatterPreparedData` and both preparation
   signatures contain no size. The architecture assay rejects the old
   `point_size_px` carrier, and render assays reuse prepared data while changing
   only resolved settings.
5. **Vector and downsampled paths share the radius.** PRODUCT-0013's exact 3px
   SVG baselines remain unchanged. The PRODUCT-0017 layered assay observes both
   1.5px and 6px circles under explicit Vector and Downsampled resolution.
6. **Raster paints clipped discs.**
   `product_0017_raster_draws_clipped_discs_and_keys_the_resolved_size` observes
   more than one changed pixel at the default radius, a larger footprint at
   double radius, and a smaller nonempty footprint when the disc crosses two
   viewport edges.
7. **Raster cache identity includes size.** The same test uses `Arc::ptr_eq` on
   cached RGBA storage: identical keys reuse storage, size-only change does not,
   and repetition at the new size reuses the new storage.
8. **Geometric hit testing uses the resolved layer.**
   `product_0017_styled_hit_uses_the_layer_resolved_radius` places a cursor six
   pixels from a mark. Factor 1 misses after the unchanged tolerance; factor 2
   hits through `hover_with_settings` and returns the established bridge text.
9. **Unstyled fast paths remain.** Factory dispatch retains the existing kernel
   path exactly when a one-layer spec carries default `LayerSettings`.
   PRODUCT-0013's one-layer line/scatter type and exact SVG evidence remain
   green; only a styled one-layer spec takes the layer-aware path.
10. **Guide behavior is unchanged.**
    `product_0017_point_size_does_not_acquire_guide_behavior` renders the legend
    at factors 1 and 2 and compares exact SVG equality.
11. **There is no Rune surface or architecture drift.**
    `product_0017_point_size_is_render_state_without_a_rune_surface` proves the
    envelope contains no `point_size` recognition. Full execution, host-panel,
    placement, dashboard, Toron, and architecture suites remain green; none of
    their production files changed for Stage 3 and no forbidden subsystem
    changed anywhere in the cut.

## Commit and scope trace

- `da0f15fb` — accepted record and implementation plan;
- `9f04f24f` — validated value, typed layer settings, and five-tier resolution;
- `17cf1f59` — render-time vector/downsample/hit resolution and retained cascade;
- `960fc26a` — filled-disc rasterization, cache identity, legend and architecture
  closure.

`rune_relation_envelope.rs`, `gui/dashboard/`, libaryon, Toron, and transport
were untouched. `AuthoredPlotSpec`, layered compatibility, shared domains, and
`ScatterLinePlotKernel` paint order were not changed.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
263 passed; 5 live-service tests ignored; 17 architecture tests passed

cargo check -p polariton --no-default-features
passed with the pre-existing test-only create_layered_plot dead-code warning

cargo test -p libaryon
1411 passed; 22 ignored; 13 architecture tests passed; quit-latency test passed

git diff --check
passed
```

PRODUCT-0018 remains the first authorized producer of non-default layer point
size from Rune source.

