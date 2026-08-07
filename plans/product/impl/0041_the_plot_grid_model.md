# PRODUCT-0041 implementation plan: the plot grid model

Status: implemented 2026-08-06. This plan implements the accepted substrate in
`plans/product/adr/0041_the_plot_grid_model.md`. It adds no Rune producer; the
surface and arrangement spelling remain PRODUCT-0042.

## Scope

- Add the closed `PlotGrid`/`GridCell` representation with resolved data slices,
  per-axis domain group IDs, positions, and an explicit arrangement.
- Add value-facet production over a stamped `DataFrame`, preserving display
  order and using the degenerate one-cell form for an empty relation.
- Resolve one shared Cartesian domain for the accepted group-0 case and reject
  differing group IDs, differing bodies, and more than 512 cells before render
  allocation.
- Add the grid composite rendering seam and containing-cell hit routing without
  changing ordinary one-plot kernel output.
- Keep the multi-cell constructor test-reachable only. No envelope arm, host
  identity, panel placement, lifecycle, or arrangement policy is added.

## Sequencing

1. Bank the existing one-cell plot SVG as the parity oracle.
2. Land the model, value-facet partitioning, shared-domain resolution, and
   attributed capacity/shape refusals.
3. Land the composite renderer and bridge-only panel labels; exercise disjoint
   regions, partition assignment, shared ranges, tick positions, and hit routing
   through rendered output.
4. Run the Polariton and libaryon suites, then record evidence and corpus status.

## Stop conditions

Stop and split a new record if implementation requires Rune lowering, a grid
shape heuristic, partial scale sharing, per-cell bodies, Guide policy beyond the
identifying label bridge, or changes to ordinary kernel output.
