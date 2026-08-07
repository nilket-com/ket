# PRODUCT-0044 implementation evidence: the legend policy

Accepted 2026-08-07.

Plan: `5a0a97e7` (`plan: adr 0044 the legend policy`).
Implementation: this commit (`impl: adr 0044 the legend policy`).

## Result

`LegendEntry` now carries `Named | Synthetic` provenance. Kernels continue to
produce legend content; the one policy function is the only place that omits a
lone synthetic section. It also merges exact entry triples, retaining the
`Named` provenance when an otherwise-identical synthetic and named entry meet.

Layered figures contribute content in paint order. Grid figures contribute in
cell order and render their one merged legend at figure bounds. A two-field
band-filled bar produces no entries, while an authored bar series retains a
named entry. Categorical line and scatter preparation restore source
first-appearance order after Polars partitioning, keeping marks, palette
positions, facets, and legend entries on the display-domain order.

No Rune spelling, legend geometry, chrome role, or mark path changed. Existing
figure-bounds placement is retained, including the possibility that a grid
legend overlays a panel.

## Gate ledger

| gate | evidence | result |
|---:|---|---|
| 1 | `product_0044_layered_legend_unions_paint_order_without_flattening_samples`; exact-triple/named-wins unit test | pass |
| 2 | `product_0044_grid_renders_one_figure_legend_from_cell_union` | pass |
| 3 | `product_0044_band_fill_bars_omit_legends_but_authored_series_keep_them` | pass |
| 4, 4a | `product_0044_policy_omits_only_lone_synthetic_entries`; provenance is read only by `apply_policy` | pass |
| 5 | `product_0044_series_order_follows_the_display_domain` | pass |
| 6 | existing `product_0043_gradient_and_dual_legends_keep_their_structural_geometry` | pass |
| 7 | existing grid label tests retain verbatim cell labels; source carries no 0041 label bridge marker | pass |
| 8 | layered legend bridge test is replaced; existing hover bridge test remains unchanged | pass |
| 9 | `product_0043_corpus_is_historical_and_renderer_is_byte_stable_for_fixed_content` retains the corpus and checks fixed-content renderer bytes | pass |
| 10 | full Polariton and architecture suites | pass |

## Verification

- `cargo fmt --all` — passed.
- `cargo test -p polariton` — 399 passed, 5 ignored.
- `cargo test -p polariton --test architecture_test` — 38 passed.
- `git diff --check` — passed.
