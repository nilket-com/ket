# PRODUCT-0021 implementation evidence

Recorded 2026-08-04 after both substrate commits, complete cross-feature
verification, and the PRODUCT-0022 production-host assay. Normalized bar width
is a typed Layer-stage setting; one rectangle calculation now owns both paint
and hit geometry for vertical and horizontal bars.

## Stage 0 findings

The existing bar kernel could retain typed standalone `LayerSettings` and
resolve them through `PlotKernel::plot_with_settings` and
`hover_with_settings`. No prepared-data, layout, specification, or host change
was required. The duplicated vertical/horizontal paint geometry, hidden 80%
group inset, and broader hover reconstruction were replaced by one private
full-band rectangle path rather than a second renderer.

The surface feasibility finding also held: styleability did not require layer
composability. `LayerMark` remains exactly `Line | Scatter`; PRODUCT-0022 uses a
general envelope-only styled-single carrier for Bar.

## Gate ledger

1. **The normalized type owns its domain and default.**
   `product_0021_bar_width_is_normalized_and_layer_constructors_inherit`
   asserts `0.9`, finite values in `(0, 1]`, and rejection of zero, negatives,
   values above one, NaN, and infinities.
2. **The five tiers select without multiplication.**
   `product_0021_bar_width_selects_each_layer_before_pane_override` walks every
   cascade tier, independently resolves two layer values, and proves the pane
   override remains last.
3. **The fraction occupies the full subgroup slot.**
   Legacy vertical and horizontal SVG baselines landed before the geometry
   refactor. `product_0021_full_band_default_retires_the_legacy_inset` proves the
   intentional default change, while
   `product_0021_bar_width_occupies_the_full_band_slot_in_both_orientations`
   measures exact 90%, 50%, and touching 100% rectangles.
4. **Paint and hit consume one rectangle calculation.**
   Both orientations flow through `BarPlotKernel::bar_rectangles`; the
   `product_0021_bar_width_is_render_state_without_bar_layer_admission`
   architecture assay pins both paint and hover to that shared path.
5. **Only visible fill hits.**
   `product_0021_bar_hit_uses_the_same_visible_rectangle_as_paint` proves a
   visible hit, a width-created padding miss, and unchanged hover prose.
6. **Width changes no preparation, layout, label, category, group, or guide.**
   `product_0021_bar_width_changes_no_layout_preparation_or_guide` compares
   those artifacts directly, while the architecture assay proves
   `BarPreparedData` contains no width.
7. **Ordered shared-Cartesian layers remain closed.**
   The architecture assay pins `LayerMark` to Line and Scatter. PRODUCT-0013's
   paint and structural regressions remain green, and `scatter_line.rs` is
   untouched.
8. **No language or application behavior entered the substrate commits.**
   Their scope is limited to `plot/{common,factory,theme,spec}` and the existing
   bar kernel. Rune, Toron, libaryon, execution, identity, placement, headings,
   and dashboard files are absent.

## Commit and scope trace

- `453a5b16` — accepted PRODUCT-0021/0022 records and implementation plan;
- `bf4be7a0` — validated normalized type and five-tier carriage;
- `671ce765` — legacy baseline capture and shared full-band vertical/horizontal paint-hit
  rectangles.

No Bar variant was added to `LayerMark`; the hidden 80% category-group inset is
retired, width remains out of prepared data, and guide drawing and hover wording
are unchanged.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
292 passed; 5 live-service tests ignored; 21 architecture tests passed

cargo check -p polariton --no-default-features
passed (feature-disabled factory paths report dead-code/unused-import warnings)

cargo test -p libaryon
1411 passed; 22 ignored; 13 architecture tests and quit-latency test passed

cargo build -p polariton
passed

git diff --check
passed

target/debug/polariton ../uat/test7.rn
installed fixture launched without a terminal error and was closed deliberately
```

All eight PRODUCT-0021 gates are discharged.
