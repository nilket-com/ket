# PRODUCT-0156 implementation evidence

Status: implemented 2026-09-04 against the amended plan `86fa052f`;
review corrections verified; awaiting acceptance.

## Implemented

- `plot/series.rs` owns exact field/value identity, the four idempotent
  visibility actions, and deterministic figure ordinals. Unknown identities
  are ignored; unidentified series remain visible.
- Preparation retains identities for line, categorical scatter, stacked
  area, radar, and parallel coordinates. Stacked-area accumulation keys use
  original scalars rather than formatted labels. For formatted temporal
  values that print alike, this can change the picture: distinct values now
  produce distinct bands and entries, as §1 requires.
- Radar and parallel kernels mint row identities in validated frame order;
  the standalone multivariate validation module has no identity dependency.
  A shared printed category is actionable only when every contributing row
  has the same non-null identity; direct actions retain all exact identities.
- Both host panes and notebook surfaces apply actions through their update
  path. Visibility is a pane override, independent of viewport domains, and
  is reset on kernel replacement. The inspector displays a read-only row
  outside the writeback registry.
- Legend union preserves conflicting identities as non-actionable. Drawing
  and input share entry rectangles. Gutter and authored-overlay presses own
  their sequence before pan/brush; modifiers and cancellation are explicit.
  Identical printed triples also deduplicate within the first content.
- Hidden marks, line/scatter/area hits, scatter projected regions, and
  series-specific selection emphasis are suppressed. Scatter raster keys
  include visibility so stale raster pixels cannot be reused after a toggle.
- SVG groups carry only ordinals. `SvgFigure` retains the complete identity
  mapping in memory, including hidden series; publication writes only SVG
  bytes and adds no metadata asset. PNG/AVIF and recorder grouping use the
  backend's default no-op.

## Raster boundary decided in §5

Rasterized scatter renders a single density raster combining every
series. Its pixels are computed from accumulated density and average colour
(`scatter.rs::build_raster_rgba_inner`), so it cannot be split into
independently hideable SVG groups while preserving the current appearance.
The amended ADR retains this raster policy: desktop visibility rebuilds the
image with hidden series excluded, and SVG embeds the current state as one
ungrouped image at its fixed extent. Vector and downsampled scatter retain
per-series groups. Browser re-rasterization belongs to PRODUCT-0153's
recomputation probes, not this cut.

Gate 2's dedicated regression forces the existing raster spelling on an
overlapping two-series fixture. It verifies that a visibility change refuses
the old desktop bitmap even during motion, rebuilds once, matches a cold
cache, keeps the image extent, and restores the original pixels. SVG embeds
exactly the current-state export raster as one image without `data-series`.
The interactive worker rounds its transform to bitmap dimensions; export
uses the plot's fractional bounds, so each path is compared with its own
reference rather than asserting unrelated cross-path pixel equality.

## Verification

- Full default-head suite:
  `cargo test --locked -p polariton --no-fail-fast --tests`.
  All 17 targets pass, including both unchanged multivariate trybuild cases;
  the unit binary reports 1,192 passed and 20 ignored.
- Full reduced-head suite:
  `cargo test --locked -p polariton --no-default-features`
  with `--no-fail-fast --tests`.
  All 16 enabled targets pass; the unit binary reports 882 passed and
  8 ignored. The Toron-only session-discovery target is feature-gated.
- Eighteen focused PRODUCT-0156 tests cover the following in both heads.
  They cover action algebra, temporal label collisions, direct/gesture
  equivalence, overlay routing, cancellation, modifier handling, host and
  notebook updates, viewport independence, fixed stacked-area bands, scatter
  raster/hit/region behavior, multivariate identities, shared layer/grid
  identities, export ordinals, and PNG pixel equivalence. The multivariate
  collision test exercises both row orders, identified/unidentified rows,
  unanimous identity, and direct actions behind inert printed entries in
  both radar and parallel kernels.
- The feature-gated production-seam render probe is run separately:
  `cargo test --locked -p polariton --features product-0057-render-evidence`
  with `--test product_0057_render_evidence`.
  Passes on host `slim`, Intel UHD Graphics 770, Vulkan hardware adapter,
  Mesa 26.0.8. The production-seam and overlap, clip, MSAA, and text probes
  complete with their automated dispositions. Five fixture initializers
  now explicitly carry no identity, preserving their pre-cut rendering.
- Workspace architecture gate
  (`cargo test --locked --test architecture_test`):
  165 passed across five targets, including Polariton's 122 tests.
- Both native checks pass: `cargo check --locked -p polariton` and
  `cargo check --locked -p polariton --no-default-features`.
- `cargo fmt --check` and `git diff --check` pass. The final workspace
  architecture rerun after the last changes also passes all 165 tests.
- All three PRODUCT-0152 cross targets were attempted in both heads:
  `x86_64-pc-windows-msvc`, `x86_64-apple-darwin`, and
  `aarch64-apple-darwin`. All failed in native dependency build scripts on
  this Linux host (`aws-lc-sys`, `ring`, and stack support), before Polariton
  could be checked. Default-head controls fail as well. This does not claim
  cross-platform compilation or release readiness.

Latest check logs are under `/tmp/adr0156-*.log` during this session; they are
temporary diagnostics, not durable acceptance artifacts. No publication
metadata, browser rendering policy, or cross-platform success is claimed.
