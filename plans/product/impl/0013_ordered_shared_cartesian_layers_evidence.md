# PRODUCT-0013 implementation evidence

Recorded 2026-08-03 after the full verification pass.

## Gate ledger

1. **Authored order survives resolution into painting.**
   `product_0013_resolution_preserves_authored_layer_order` resolves
   `[Line, Scatter]` without normalization and retains that order in the
   resolved non-empty sequence. The SVG evidence in gate 2 observes the same
   order after lowering and painting.
2. **Paint order is observed at the backend.**
   `product_0013_layered_svg_paints_scatter_after_line` renders through
   `SvgBackend` and asserts that the scatter fill path follows the line stroke
   in SVG document order. It does not inspect an in-memory layer vector.
3. **Differing x or y identities refuse atomically.**
   `product_0013_layer_compatibility_names_both_field_identities` covers both
   channels and asserts the offending second layer, expected field, and actual
   field.
4. **Layered color remains unaccepted.**
   `product_0013_layer_color_refuses_named_and_auto_with_absent_control`
   refuses both `Named` and `Auto` at the offending layer and proves that the
   equivalent `Absent` control resolves.
5. **A channel failure produces no partial plot.**
   `product_0013_layer_channel_failure_is_atomic_and_attributed` observes one
   attributed `LayeredPlotError` and no resolved artifact when a later layer
   names a missing field.
6. **One-layer lowering preserves today's rendering byte-for-byte.**
   `product_0013_pre_extraction_line_svg_baseline` and
   `product_0013_pre_extraction_scatter_svg_baseline` captured exact SVG before
   extraction. `product_0013_one_layer_line_keeps_existing_svg` and
   `product_0013_one_layer_scatter_keeps_existing_svg` lower layered specs to
   the established kernels and compare against those exact baselines. Both
   kernels and layered lowering delegate to the same extracted mark renderers;
   there is no parallel drawing implementation.
7. **The shared domain comes from common source columns before preparation.**
   `product_0013_shared_domain_comes_from_common_source_columns` uses two line
   layers over source ranges `0..100` and `10..30`. The composite reports the
   independently resolved 10%-padded domains `-10..110` and `8..32`, while the
   line renderer prepares its existing 5%-padded ranges `-5..105` and `9..31`.
   The explicit inequality proves mark preparation did not choose or contribute
   to the shared layout.
8. **An empty plot is unrepresentable.** `NonEmpty<T>` stores a required first
   element plus a rest and exposes no `Default`, empty constructor, or fallible
   `Vec` conversion. `LayeredPlotSpec::new` requires one `LayerSpec`, and
   `LayerMark` admits only `Line` and `Scatter`.

## Non-gating bridge coverage

- `product_0013_bridge_hover_prefers_topmost_geometric_hit` places one cursor
  within both marks, observes the scatter hit for `[Line, Scatter]`, then
  reverses authored order and observes the line hit instead. `MarkHit` contains
  only Cartesian geometry and a mark-local index; the composite supplies the
  existing coordinate wording.
- `product_0013_bridge_legend_delegates_to_last_authored_layer` observes only
  the scatter swatch for `[Line, Scatter]`, with no merged line legend. Existing
  kernels and the bridge call the same extracted legend helpers.

These tests pin the temporary PRODUCT-0013 §3 bridge against accidental drift.
They are not acceptance gates and make no Guide or interaction promise.

## Implementation boundary

`LayeredPlotSpec` is additive beside `AuthoredPlotSpec`. `NonEmpty<LayerSpec>`
owns authored order and per-layer encodings; `LayeredChannelBinding` resolves
each layer through the existing PRODUCT-0007 channel resolver before applying
the narrow identical-field, compatible-scale, and absent-color rule.

Domain resolution scans the stamped result through the first compatible
binding, then stamps that exact x/y domain onto later bindings while retaining
their own layer identities. `LayeredPlotKernel` supplies one Cartesian layout
and calls extracted line/scatter mark renderers first-to-last. The scatter
renderer owns the existing vector/downsample/raster selection and raster cache.
The public `ScatterPlotKernel::prepared_data`, both existing kernels' hover
contracts, and `ScatterLinePlotKernel` remain unchanged.

The path is intentionally test-only until PRODUCT-0014 supplies a Rune caller.
No Rune envelope, GUI, editor, dashboard, transport, durability, theme,
`PlotComposition`, other kernel, or `AuthoredPlotSpec` consumer changed. The
halt rule did not fire.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
237 passed; 5 live-service tests ignored; 16 architecture tests passed

cargo check -p polariton --no-default-features
passed
```

All eight acceptance gates are discharged. The implementation landed as four
stage commits: `9b581151`, `446c9216`, `4c35071d`, and `0e565a9c`.
