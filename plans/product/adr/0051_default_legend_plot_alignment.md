# ADR 0051: align the default legend with the plot extent

Status: proposed 2026-08-08. This record repairs one ambiguity in 0050's
default legend gutter: "top-aligned" means aligned with the marks' actual
plot extent, not with the figure edge above the title band.

## Context

0050 correctly moved default legends out of the cells and into a measured
right gutter, but `legend_layout` receives the full figure bounds and places
the default legend at their top. The plot area is inset afterward. A figure
with a title therefore starts its legend above the marks it describes.

`test16.rn` exposed the missing anchor by compensating with
`legend_at(1.06, 0.14)`. The horizontal value manually rebuilt the gutter and
the vertical value encoded the roughly 62-pixel title band as a fraction of
`panel_height - legend_height`. On resize that fraction denotes a different
distance, so a placement tuned at one size clips at another. The implementation
is correct under 0050: authored out-of-range overlays are deliberately rendered.
The defect is the default anchor that made the authored compensation necessary.

Stage 0 closes the apparent layout cycle. Titles and subtitles are unwrapped
canvas text at fixed offsets; they are not measured into layout.
`plot_padding_top` is fixed, and the legend gutter changes width only. Ordinary
plots already name their authoritative `plot_area`; grids already name the
union as `grid_plot_extent`, including the per-cell panel-label inset. Neither
vertical origin depends on the gutter width.

## Decision

For `ResolvedPresence::Fallback`, the measured right gutter remains exactly as
0050 specifies, but its legend origin is resolved only after plot geometry:

- an ordinary figure uses `plot_area.y()`;
- a multi-cell figure uses `grid_plot_extent(...).y()`.

The implementation consumes those rectangles directly. It does not reconstruct
title, subtitle, panel-label, or padding arithmetic beside the geometry that
already owns it.

0050's other states are unchanged. `Show(LegendPosition)` remains an authored
overlay over full figure bounds, including its accepted out-of-range rope.
`Hide` and absent content render no legend and reserve no gutter. The
`Presence<LegendPosition>` type equation and both Rune spellings stand.

**The legend's vertical budget is not decided here.** `max_legend_rows` remains
the existing fixed row/column content policy; it does not wrap against either
figure height or plot-extent height. Aligning a tall legend's top with the plot
extent may leave its bottom below that extent, where existing canvas clipping
applies. Reflow, height-aware row budgeting, scrolling, or bottom containment
needs its own exhibit and record.

## Acceptance gates

1. With a title, with a title and subtitle, and with neither, a default ordinary
   legend's top equals the actual `plot_area.y()`; no independently reconstructed
   inset appears in the legend code.
2. The equality in gate 1 holds at concretely different `400x300` and
   `1200x900` figure sizes. The test must fail if the old proportional
   `legend_at(1.06, 0.14)` compensation is substituted.
3. A multi-cell figure aligns the default legend to
   `grid_plot_extent(...).y()`, including panel-label space, while 0048 cell
   congruence and shared-edge geometry remain unchanged.
4. Authored overlay coordinates retain 0050's exact formula; `Hide`, absent
   content, one-cell parity, hover geometry, and every 0044 content-policy
   behavior pass unchanged.
5. Every draw baseline changed by the second consecutive default-placement
   move is enumerated in implementation evidence as an 0014 §9a premise
   update. The sole admitted difference class is a default gutter legend's
   vertical translation from figure top to plot-extent top; a baseline with
   any mark, axis, cell, content, size, or authored-overlay difference fails.
6. A legend exceeding the plot extent's height retains the same measurement,
   row partition, and content order. The test records the vertical-budget
   non-decision rather than inventing height-aware wrapping.
7. `test16.rn` deletes `legend_at(1.06, 0.14)` and returns to inherited gutter
   placement. No Rune surface, type, identity, lifecycle, or interaction
   behavior changes.

## Stop condition

> This record may change only the default gutter legend's vertical anchor,
> add its geometry and premise-update evidence, and remove the compensating
> `test16.rn` member. If the repair requires a new authored member or type,
> title measurement or wrapping, legend reflow or row-policy changes, a gutter
> side choice, per-cell legends, or any mark/cell geometry change, the work
> stops and the requirement returns as its own record.

## Supersession

This record supersedes only 0050 §1's underspecified "top-aligned" phrase:
the reference edge is now the authoritative plot extent. It does not supersede
0050's gutter, visibility, placement property, overlay arithmetic, clipping
posture, or content policy.

## Rejected

- **Add `legend_in_gutter(alignment)` now:** the structural anchor deletes the
  only exhibit for authored gutter alignment. The language grows when a use
  survives the repair, not in the same cut that removes it.
- **Retune `legend_at` for the larger panel:** preserves a fixed distance in a
  moving unit and fails at the next size.
- **Reconstruct the title band in legend code:** duplicates ordinary layout
  arithmetic and misses the grid's panel-label inset.
- **Make existing overlays plot-relative:** silently changes an accepted
  authored spelling and breaks 0050 gate 4 instead of repairing the default.

## Consequences

- The default legend begins level with the marks it describes at every figure
  size and under both ordinary and gridded geometry.
- `test16.rn` becomes shorter while its picture becomes more correct: the
  strongest evidence that `1.06, 0.14` was compensation rather than intent.
- A future tall-legend exhibit starts from an explicit non-decision instead of
  being misclassified as a regression in this anchor repair.
