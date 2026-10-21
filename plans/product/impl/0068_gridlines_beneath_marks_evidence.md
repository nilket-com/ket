# PRODUCT-0068 implementation evidence: gridlines beneath marks

Status: accepted 2026-08-09.

Plan: `2637b290`
(`plans/product/adr/0068_gridlines_beneath_marks.md`).

Co-landed contrast repair: PRODUCT-0067, default plot gridline contrast.

## Result

Ordinary and multi-cell plots now render four explicit strata: canvas, grids,
marks, then furniture. One shared `draw_layout_grid` dispatch owns Cartesian,
categorical-x, and categorical-y guide geometry. The ordinary plot layer calls
it immediately before the kernel; every composite cell calls the same helper
inside that cell's existing clip before its kernel. Axes, ticks, labels,
frames, legends, panel labels, and global headings remain in the overlay.

Opaque marks therefore retain their exact resolved colour where they cross a
guide. The guide remains visible on adjacent canvas and through categorical
gaps. The Vulkan evidence target proves this with production grouped bars, a
diagonal line, and a scatter point, each compared with a deliberately restored
grid-over control.

The dashboard-reachable stacked-area kernel also replaces its hardcoded
20%-black separator with the already-resolved `mark_decoration_color`. The
separator stays exactly `0.5px`; its paths, fills, ordering, ranges, hover, and
unresolved colour ramp are outside this cut.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Four strata for all gridded layouts | `product_0068_guides_precede_marks_and_furniture_follows_for_every_gridded_layout` records operations for Cartesian, categorical-x, and categorical-y and requires every grid before the real mark and every axis after it | pass |
| 2. Production bar pixels | The opt-in Vulkan target renders the real `BarPlotKernel`; cyan remains `[1,205,254]` at a crossing while the overlay control becomes `[45,213,254]`, and a category gap retains `[44,44,44]` grid over black canvas | pass |
| 3. Discriminating bar fixture | The fixture has three saturated resolved series, three categories, `bar_width(0.78)`, `category_gap(0.16)`, five grid intervals, and headroom; all three colours are asserted present separately | pass |
| 4. Stacked-area boundary | The Vulkan production fixture finds 1,620 exact-fill intersections plus 1,485 visible headroom-grid pixels. `stacked_area_boundaries_use_resolved_mark_decoration_at_the_existing_width` separately records two real fills and boundaries, pins the resolved role at `0.5px`, and asserts hardcoded black absent | pass |
| 5. Line and scatter controls | The Vulkan target drives the production line and scatter kernels. It finds 36 and 68 exact-colour intersection pixels respectively that the overlay control tints, while the same rows retain 1,178 and 776 visible adjacent grid pixels | pass |
| 6. Composite cell isolation | `product_0068_each_facet_cell_clips_grid_then_mark_before_shared_furniture` requires two distinct cell clips, real grids before the mark in each, no grid outside those clips, and shared furniture after both marks | pass |
| 7. Facet furniture does not move | The production delta removes only grid calls from overlay and inserts the shared helper in plot; existing shared-edge, heading-band, legend, and categorical-domain gates pass unmodified | pass |
| 8. One-cell parity | `one_cell_grid_matches_the_ordinary_svg_path`, including its legend variant, remains exact because the composite still delegates to the ordinary path | pass |
| 9. Grid-off identity | `product_0068_disabling_every_grid_is_byte_identical_to_the_retired_strata` compares complete operation streams for all three layouts against a no-plot-layer-grid control | pass |
| 10. Geometry and interaction | Grid helpers and every kernel geometry producer are unchanged; existing rectangle, hit, hover, domain, and category-order suites pass | pass |
| 11. Named skins and marks | Theme projection, palette assignment, and mark alpha code are unchanged; named-skin projection banks pass | pass |
| 12. One bounded transition | No checked-in baseline was refreshed. The complete production delta matches the five difference classes in §4; the marks-free and intersection controls distinguish alpha from order | pass |
| 13. Reproducible exhibit | `PRODUCT_0068_STAGE0_PNG=/tmp/product_0068_stacked_area_and_bar_strata.png` on the opt-in evidence target regenerates the three-way stacked-area and grouped-bar comparison; production controls assert non-empty marks and grids | pass |
| 14. Complete verification | Polariton/workspace tests, formatting, checks, and every PRODUCT-0057 raster probe pass; no ignored test was added | pass |

## Implementation finding

The production Vulkan gate made the global decision sharper than the Stage 0
prototype. Bars prove the no-tradeoff case: the grid remains readable through
their real intra- and inter-category gaps while no longer altering any resolved
fill. The line and scatter controls then show the same rule is not an opaque-fill
special case.

The composite path could not inherit the ordinary ordering merely by sharing
helpers: it owns a separate clipped cell loop. Its dedicated oracle therefore
associates each operation with the active physical clip and proves both order
and containment. This is the load-bearing difference between moving one call
site and establishing a renderer-wide stratum.

## Baseline disposition

No curated baseline was refreshed. Expected differences are confined to:

- compatibility grid alpha `0.20` → `0.35` on exposed canvas;
- grid operations moving before marks;
- exact opaque mark pixels replacing grid-composited pixels at intersections;
- stacked-area boundary colour moving from hardcoded translucent black to the
  resolved mark-decoration role at unchanged width.

Axes, tick geometry, text, frames, legends, headings, mark geometry, hit, and
hover remain unchanged. With every grid disabled, the operation stream is
exactly identical to the retired order.

## Verification

- `cargo test -p polariton product_0068 -- --nocapture` — ordinary,
  composite, grid-off, and stacked-area gates passed.
- `cargo test -p polariton --bin polariton` — complete Polariton unit suite
  passed with only the five pre-existing live-Toron tests ignored.
- `cargo test --workspace --all-targets --no-fail-fast --quiet --
  --test-threads=1` and `cargo check --workspace --all-targets` — passed.
- `cargo test -p polariton --test product_0057_render_evidence --features
  product-0057-render-evidence -- --nocapture` — grouped-bar, line, scatter,
  overlap, clipping, MSAA, and text probes passed on the Intel UHD 770 Vulkan
  adapter.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

Two default-parallel workspace sweeps each surfaced a different libaryon timing
fixture under the 1,505-test library load (`grandchild pid` before output and a
post-ABA source timeout). The exact first test and then the complete libaryon
library passed in isolation; the complete one-test-thread workspace sweep passed.
The failures are disclosed here rather than baselined or repaired inside an
unrelated renderer record.

## Implementation history

PRODUCT-0067 and PRODUCT-0068 are compressed into one implementation commit.
The Stage 0 harness remains opt-in because it uses a real hardware adapter; its
semantic thresholds are asserted, not checked in as cross-machine raster bytes.
