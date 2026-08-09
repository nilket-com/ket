# PRODUCT-0065 implementation evidence: shared categorical grid domains

Status: accepted 2026-08-09.

Plan: `5db1d7f5`
(`plans/product/adr/0065_shared_categorical_grid_domains.md`).

## Result

Categorical facet grids now resolve one complete domain at the grid boundary.
Every `CategoricalX` cell receives the resolved x-category sequence and the
union of local y ranges; `CategoricalY` receives the mirror. The resolved body,
not facet order, supplies category order, so interleaved `A:X, B:Y, A:Z` data
remains `X,Y,Z`.

The first correspondence probe found that layout sharing alone was unsafe:
bar, stacked-bar, box, and violin kernels addressed prepared payloads by local
ordinal. Those renderers now resolve category names through the shared
`CoordinateTransformer` domain before paint, hit, or hover geometry is made.
An absent category therefore leaves an empty shared slot instead of moving the
next mark under the wrong label. Categorical scatter already consumed the
resolved domain and needed no repair.

No Rune surface or domain representation changed. Explicit-bounds facets,
complete category crosses, one-cell grids, ungridded figures, Cartesian grids,
and the other layout families retain their prior behavior. The only intended
pixel difference is an unbounded categorical facet whose local category or
numeric domain differed from a sibling.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Fixtures discriminate | `product_0065_categorical_x_shares_ragged_domain_and_payload_slots` and its categorical-y mirror assert unequal local category sequences, unequal local numeric ranges, and an omitted category in every partition before they inspect the merge | pass |
| 2. Categorical X shares both halves | The interleaved vertical fixture pins the resolved sequence `X,Y,Z`, rejects both local sequences as incomplete, and requires every merged layout to carry that sequence plus the finite union of local y ranges | pass |
| 3. Categorical Y is symmetric | The independent horizontal fixture repeats the complete proof for `North,East,South` and the unioned x range | pass |
| 4. Labels, marks, and values correspond | Both ragged fixtures render each cell through the merged transform and assert every rectangle centroid equals the transformer slot found by that payload's category name; a missing middle category is therefore observable as an empty band | pass |
| 5. Congruence is numeric | Both orientations compare the same payload value across cells and assert exact cell-relative geometry. Count-based major ticks plus two minor ticks per interval are transformed through every merged layout and required to yield identical pixel coordinates | pass |
| 6. Authored bounds remain authoritative | The merge unions the local layout ranges, so an authored pair repeated by every local kernel is returned unchanged. The existing explicit-bounds facet exhibits and full baseline suite pass without refresh | pass |
| 7. Other families do not move | The `shared_layout` and `merge_layout` matches preserve the existing Cartesian arm and return `CategoricalXY`, `Parallel`, and `Radial` unchanged. Existing Cartesian sharing and one-cell SVG parity gates pass in the full suite | pass |
| 8. Existing renderers consume it | The vertical and horizontal bar fixtures exercise grouped bar geometry; `product_0065_box_and_violin_address_shared_slots_by_category_identity` jointly checks paint and hover for both renderers; `product_0065_categorical_scatter_already_uses_the_resolved_domain` proves scatter's existing resolved-domain lookup | pass |
| 9. No representation or surface grows | `PlotGrid`, `GridCell`, `DomainGroupId`, `PlotLayout`, and the Rune grammar are unchanged. The production delta is confined to grid sharing, one transformer lookup, and categorical renderer consumers | pass |
| 10. Pixel movement is bounded | No baseline was refreshed. The 508-test Polariton suite, 44 architecture gates, coordinate-space trybuild pair, 1,483-test libaryon suite, and complete workspace passed with only the pre-existing ignored tests | pass |
| 11. Workspace green | The workspace all-target sweep and PRODUCT-0057 Vulkan raster-evidence target passed; formatting and diff checks are clean | pass |

## Implementation finding

The initial repair shared the complete `PlotLayout` but left renderer-local
category ordinals untouched. The deliberately missing-middle-category fixture
then placed `Z` in shared slot 1 under label `Y`. That was not a second facet
policy; it exposed a general renderer invariant: prepared category identity,
not its position in a partition, chooses the transform slot. The plan was
amended before implementation continued, and all four ordinal consumers were
repaired together. The scatter control proves this was already the established
resolved-data design where the renderer had not reconstructed it.

The PRODUCT-0064 stacked-facet test lands with the resumed surface because it
was the consumer that exposed the broken PRODUCT-0041 premise. Once this shared
domain and semantic slot lookup are present, that later consumer inherits them
without a facet-specific branch.

## Verification

- `cargo test -p polariton product_0065 -- --nocapture` — four targeted gates
  passed; zero ignored.
- `cargo test --workspace --all-targets --no-fail-fast` — passed across the
  workspace. Polariton reported 508 passed and five pre-existing live-Toron
  tests ignored; libaryon reported 1,483 passed and 22 pre-existing manual or
  benchmark tests ignored. Its 44/18 architecture suites and both trybuild
  suites passed.
- `cargo test -p polariton --test product_0057_render_evidence --features
  product-0057-render-evidence -- --nocapture` — all four automated raster
  probes passed on the Intel UHD 770 Vulkan adapter.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

## Implementation history

The correction was implemented and verified as its own prerequisite commit
before PRODUCT-0064 resumed. Its plan amendments and implementation are
compressed independently in the final history pass.
