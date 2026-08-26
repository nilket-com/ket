# The facet audit — per-cell authority and geometry

**Status:** audit, 2026-08-26. Not a record and not a plan. Read-only: no code
changed. It discharges the follow-on PRODUCT-0139 §7 defers, tracing the
multi-cell composite arm the step-0 audit explicitly left unread, and answers
one question: **do per-cell authority and per-cell geometry support the same
reuse predicate the unfaceted path does?**

Traced at `e3d92fb9e`, after §6's authority rule and §3's mark-declared clip
landed. Every claim cites the source it was read from.

## Verdict

**Geometry: yes. Authority: no.**

Per-cell geometry supports the predicate — better than expected, because every
cell renders **one shared domain**, so a viewport change moves all cells
identically and each cell's affine map is computable from its own key (§3).

But **the composite bypasses the widget render path entirely**, hand-rolling its
own draw at all three entry points. §6's authority rule therefore does not reach
it: a facet cell's raster key would take its domain and size from the
**figure-wide** resolution while taking `point_size`, `background`, and
`series_colors` from the **cell kernel's own** resolution. That is D1 — the
mixed-authority key — reintroduced in the one path §6 did not touch (§4).

**Reuse stays confined to the unfaceted path**, exactly as §7 has it. Lifting
that exclusion is not a policy question; it needs the composite routed through
one authority first, which is a real change to `grid.rs` and is scoped in §7.

## 1. The composite hand-rolls the draw at all three entry points

`PlotGridKernel` implements `PlotComposite`, and `PlotWidget` delegates to it
and returns before reaching any of the geometry it would otherwise derive
(`plot/common.rs:2662-2671`). The composite then re-derives everything itself,
three times over, in the same shape each time:

| entry point | figure-wide resolve | per-cell layouts | per-cell plot area |
|---|---|---|---|
| `render_plot_layer` (`grid.rs:632`) | `:654` | `:675` | `:694` |
| `render_overlay_layer` (`grid.rs:742`) | `:764` | `:772` | `:792` |
| `hit` (`grid.rs:924`) | `:932` | `:940` | `:962` |

Each begins `let resolved = settings.resolve();` — the **figure-wide**
resolution, `PlotSettings::resolve()`, not any kernel's own.

## 2. Per-cell geometry: one shared domain, per-cell plot areas

`cell_layouts` (`grid.rs:573-584`) asks every cell for its layout **under the
figure-wide resolution**, computes a `shared_layout` across them, and then
`merge_layout`s each cell's layout against it.

`shared_layout` (`grid.rs:1123`) takes the **union** of all cells' ranges — min
of mins, max of maxes — and `merge_layout` (`grid.rs:1224`) then **replaces**
each cell's own range with the shared one, keeping only the cell's axis types
and temporal domains.

So every cell renders the same `x_range`/`y_range`. What varies per cell is the
plot rectangle: `grid_plot_area(cell_bounds, label, &resolved)` (`:694`), from
which `CoordinateTransformer::new(layout, inner)` (`:695`) is built.

That is the fact that matters for reuse, and it is favourable:

- the **domain** term of every cell's raster key is identical and moves as one;
- the **size** term is per-cell and constant under a viewport change;
- so the affine map of §2 is computable per cell from that cell's own key, with
  no cross-cell information required.

## 3. The predicate itself holds per cell

Checking PRODUCT-0139 §2's conditions against a facet cell:

| condition | per cell |
|---|---|
| layout is `Cartesian` | as for any cell kernel; the merge preserves the variant (`grid.rs:1224-1244`) |
| raster size unchanged | `inner` is stable while the figure's bounds are |
| `point_size`, background, colours, series count unchanged | unchanged by a viewport move |
| only `x_range`/`y_range` differ | yes, and identically across cells |

Nothing here is weaker than the unfaceted case. **Facet reuse is not blocked by
geometry.**

## 4. What does not hold: the authority rule

Marks are drawn by `cell.kernel.plot_with_settings(..., settings)`
(`grid.rs:716-722`, `:730-736`), which passes the **model** and lets the kernel
resolve per its own rule — `resolve_for_layer` for a scatter. The transform
those marks are drawn through, however, came from `cell_layouts(resolved)`, the
**figure-wide** resolution.

For a scatter cell the consequence is exactly the defect §6 closed for
unfaceted plots. `ScatterRasterKey` (`plot/kernels/scatter.rs:335`) would take:

- `width`/`height` and `x_range`/`y_range` — from the figure-wide resolution;
- `point_size`, `background`, `series_colors` — from the cell kernel's own.

No single resolution owns the tuple. §6's rule — *the resolution the mark
renderer draws with is the authority, and the transform, the mode decision, the
key, and the draw all consume it* — is unsatisfied here, because the composite
never calls `PlotWidget::new` for the geometry it uses.

A third resolution is in play in the same cell draw: the `helper` widget
constructed at `grid.rs:696-704` is used **only** for `draw_layout_grid`
(`:715`, `:729`), and after §6 its settings come from the cell kernel's
resolution — while the `layout` and `transform` handed to that same call came
from the figure-wide one.

**This is not a regression from §6.** The composite resolved figure-wide before
that change too; what §6 did was fix the unfaceted path and leave this one
visibly behind. It is the reason §7's exclusion is a scope boundary rather than
a verdict.

## 5. §3's clip already reaches facets, correctly

The composite honours the mark's declaration: `let clipped_plot =
cell.kernel.requires_plot_area_clip();` (`grid.rs:705`). In the clipped branch
the cell background is emitted inside `with_clip(cell_bounds)` and the grid and
marks inside a **sibling** `with_clip(inner)` (`:706-726`, `:727-738`), so no parent-level
fill coexists with a clipped child and the `grid.rs:656-661` iced_wgpu hazard is
respected here as it is in the widget path.

A faceted scatter therefore already takes the clipped branch as of `e3d92fb9e`,
and the suite is green across that change (1084 passing).

## 6. Caches are per cell, with no aliasing

Each cell builds its own kernel — `let kernel = factory(&cell.body,
&cell.data)?` per cell (`grid.rs:527`) — so each owns a distinct
`ScatterMarkRenderer` and therefore a distinct `Mutex<Option<ScatterRasterCache>>`
(`scatter.rs:26`). No two cells can hit or evict each other's entry, and a
per-cell reuse decision needs no coordination.

## 7. What lifting §7's exclusion would require

Not a policy change — a plumbing change, in this order:

1. **One authority per cell.** The composite must derive each cell's layout and
   plot area from that cell kernel's own resolution, not from the figure-wide
   one — the composite's analogue of §6. The shared-domain union of §2 must then
   be computed across those per-cell resolutions rather than across one.
2. **Decide what the figure-wide resolution still owns.** Legend union
   (`grid.rs:668-673`), band requirements, tiling, and cell bounds are genuinely
   figure-wide and should stay so. The split is between *figure chrome* and
   *cell geometry*, and only the second moves.
3. **Then re-run the §2 checks**, which §3 above shows should pass unchanged.

Step 1 is the whole of the risk: three entry points re-derive the same sequence
independently (§1), so the change has to land in all three or they will diverge
from each other — a divergence with the same shape as the one step 0 found
between `settings` and `settings_model`.

## 8. What this audit did not establish

- **No measurement.** No faceted raster has been timed; the raster path has
  never been exercised under a facet at all, and the gate divides the **inner
  plot area**, which is far smaller per cell than in an unfaceted figure — so a
  facet cell reaches the raster threshold at a different point count than the
  single-cell figures measured so far. That interacts with PRODUCT-0140 and is
  named here only as a question.
- **`CategoricalX`/`CategoricalY` facets were not traced** beyond noting that
  `merge_layout` preserves the variant. Categorical layouts are excluded from
  reuse by §2 regardless.
- **The single-cell composite arm** (`grid.rs:640-652`) is not covered by any of
  this: it routes through `PlotWidget::new` and `render_plot_layer`, so it
  already satisfies §6. Only the multi-cell arm is at issue.
- **Nothing was run to observe a stale cell frame.** Every claim above follows
  from the cited source, and no gesture exists to produce one.
