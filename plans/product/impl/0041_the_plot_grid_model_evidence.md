# PRODUCT-0041 implementation evidence: the plot grid model

Accepted 2026-08-06.

Plan: `72a09faf` (`plans/product/impl/0041_the_plot_grid_model.md`).
Implementation: `6b40d206`, `45db31a2`, and `7e2e3245`.

## Result

The accepted substrate now has a closed `PlotGrid` representation with
non-empty positioned cells, resolved data slices, per-axis domain-group IDs,
and one shared body identity. The accepted value-facet producer partitions a
stamped result in first-appearance display order, preserves each partition's
exact rows, and uses a one-cell degenerate form for an empty relation.

`PlotGridKernel` is a composite `PlotKernel`: it lays out disjoint cell
regions, resolves one shared Cartesian range across the cells, draws each
cell's existing marks and full chrome inside the cell clip (including axis
labels and correctly translated legends), and routes hover geometry to the
containing cell. A one-cell grid delegates through the ordinary widget path,
including the widget title, and is byte-identical to the ordinary SVG oracle.
The constructor refuses differing axis groups, differing body identities,
duplicate positions, undersized arrangements, and more than 512 cells before
render allocation. The multi-cell seam remains internal; no Rune grammar,
identity, panel lifecycle, arrangement policy, or production construction was
added.

## Gate ledger

| gate | evidence | result |
|---:|---|---|
| 1 | `one_cell_grid_matches_the_ordinary_svg_path` compares complete SVG output, including a fallback title, after the composite seam | pass |
| 2 | `four_cell_value_facet_renders_four_regions` asserts four clipped regions and compares each rendered mark with its partition's expected shared transform; value-facet tests also assert exact rows assigned to each ordered cell | pass |
| 3 | `faceted_cartesian_cells_share_their_domain` compares every cell's resolved Cartesian ranges after union | pass |
| 4 | the four-cell SVG test extracts every rendered major/minor tick path, normalizes cell origins, and compares all four lattices | pass |
| 5 | `grid_rejects_mixed_groups_bodies_and_positions` covers x-group refusal; the constructor has symmetric y-group validation | pass |
| 6 | the same refusal test constructs a distinct body Arc and observes the attributed cell error | pass |
| 7 | every non-zero group is rejected by the same group diagnostic, so free scales cannot silently degrade | pass |
| 8 | 128 cells are rendered (256 plot/overlay clips); 513 cells refuse with field, count, and 512 limit before a grid is returned | pass |
| 9 | partitioning restores first-appearance order rather than hash order, re-derives after reorder/filter, and retains exact per-partition slices | pass |
| 10 | empty relations produce one cell with an empty data slice; identity and host lifecycle remain outside this substrate cut | pass (bridge scope) |
| 11 | `faceting_by_the_authored_color_field_renders_sticky_colors` observes the same resolved RGB assignment in both rendered cells; no new palette rule is introduced | pass |
| 12 | the same colored-field fixture carries the authored color column through both partitions and renders without refusal, warning, or substitution | pass |
| 13 | the four-cell SVG test asserts A/B/C/D panel labels; labels remain identifying bridge text only and do not alter guides | pass (bridge scope) |
| 14 | source census finds no `PlotGrid`/`PlotGridKernel` reference outside `plot/grid.rs`; no Rune arm exists and ordinary Polariton behavior remains green | pass |

## Verification

- `cargo fmt --all` — passed.
- `cargo test -q -p polariton` — 384 passed, 5 ignored.
- `cargo test -q -p polariton --test architecture_test` — 38 passed.
- `cargo test -q -p libaryon` — 1,438 passed, 22 ignored; auxiliary suites passed.
- `cargo check -q -p polariton --no-default-features` — passed with existing
  feature-gated unused warnings only.
- `git diff --check` — passed.
