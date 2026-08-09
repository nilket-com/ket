# PRODUCT-0066 implementation evidence: the faceted figure heading band

Status: accepted 2026-08-09.

Plan: `c0e1dfa5`
(`plans/product/adr/0066_the_faceted_figure_heading_band.md`).

## Result

Multi-cell figures now reserve one global heading band above their congruent
tile. The figure title remains at its existing coordinate; the complete grid,
including each cell's existing panel-label space, begins below it. Plot,
overlay, hover, shared-axis extent, and legend anchoring consume the same
`grid_tiled_area`, so the correction cannot desynchronize interaction from
paint.

The ordinary title budget is reused rather than reconstructed from font
metrics. A shown subtitle extends the band through its configured offset and
nominal size. Hiding both global headings reclaims the band. One-cell grids
continue to bypass the multi-cell composite and remain byte-identical to the
ordinary plot path.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Labels clear the title | `product_0066_global_heading_band_separates_first_row_facet_labels` reads the rendered SVG and requires `panel-0` to begin at or below `title_y + title_size` | pass |
| 2. Title does not move | The same gate pins the rendered global heading's y coordinate to the unchanged `title_offset` | pass |
| 3. Fixed across scale | The correspondence is repeated at 800×300 and 800×900; both use the exact `plot_padding_top` inset rather than a viewport fraction | pass |
| 4. Subtitle and hiding | The gate pins a shown subtitle's band to `subtitle_offset + subtitle_size`, with the panel label's existing inset supplying clearance; it then hides both properties and requires a zero band | pass |
| 5. One geometry authority | Production plot, overlay, and hover paths all call the same `grid_tiled_area`; existing hover-under-nonzero-origin and default-legend plot-extent gates pass unchanged | pass |
| 6. Congruence | The gate computes all four inner rectangles after the inset and asserts identical width and height | pass |
| 7. One-cell parity | `one_cell_grid_matches_the_ordinary_svg_path`, including its legend variant, passes unchanged; the one-cell composite still takes its direct ordinary path | pass |
| 8. Live exhibit and suite | `../uat/test21.rn` remains the eight-form exhibit. All 514 Polariton tests complete with 509 passed and only the five pre-existing live-Toron tests ignored | pass |

## Implementation finding

The collision was an assertion gap rather than a text-metrics problem. Both
strings were present at their independently intended coordinates, but the grid
had no figure-level region separating those coordinate systems. The repair
therefore moves neither string relative to its owner: it inserts the missing
figure heading band before the cell tile is divided.

Using the existing `plot_padding_top` for a title avoids promoting backend text
measurement into layout authority. The only nominal extent calculation added
is for an explicitly shown subtitle, whose accepted settings already define
both its offset and size.

## Verification

- `cargo test -p polariton product_0066 -- --nocapture` — the two-viewport
  title, subtitle, hidden-heading, and congruence gate passed.
- `cargo test -p polariton --bin polariton` — 509 passed, five pre-existing
  live-Toron tests ignored, zero failures.
- `cargo test --workspace --all-targets --no-fail-fast --quiet` — passed across
  the complete workspace, including both architecture suites and both trybuild
  suites; no ignored count changed.
- `cargo test -p polariton --test product_0057_render_evidence --features
  product-0057-render-evidence -- --nocapture` — all four automated raster
  probes passed on the Intel UHD 770 Vulkan adapter.
- `cargo fmt --all -- --check`, `cargo check -p polariton --all-targets`, and
  `git diff --check` — passed.

## Implementation history

The contact screenshot became one short correction record and one implementation
commit. No UAT compensation value or authored surface was added.
