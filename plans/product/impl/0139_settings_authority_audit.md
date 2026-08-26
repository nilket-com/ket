# Step 0 — the settings-authority audit

**Status:** audit, 2026-08-25. Not a record and not a plan. Read-only: no code
changed. It traces every path from the two settings authorities to the screen,
names each point where they can diverge, and states what that constrains for
the image-space reuse policy (step 1) and the raster gate (step 2). It decides
nothing.

Engine read at `87a05eb9b`. Every claim below cites the source it was read
from; nothing here is inferred from a consumer line without reading its
producer.

## Summary

There are **two settings authorities on one widget**, and they reach the screen
through different functions:

- `PlotWidget.settings: ResolvedPlotSettings` (`plot/common.rs:2053`) — the
  **geometry** authority. It feeds `kernel.layout(...)`, the guide bands, and
  therefore the plot area and the `CoordinateTransformer`.
- `PlotWidget.settings_model: PlotSettings` (`plot/common.rs:2054`) — the
  **mark** authority. It feeds `kernel.plot_with_settings(...)` and
  `kernel.hit_with_settings(...)`, and so decides render mode, point size, and
  colours.

Nothing enforces a relationship between them. The invariant
`settings == settings_model.resolve()` holds at every production construction
site today, but it holds **by convention at four separate call sites**, not by
construction.

The finding that matters most for the reuse arc is narrower and sharper than
"two structs can disagree":

> **`ScatterRasterKey` is a mixed-authority key.** Its `width`/`height` and
> `x_range`/`y_range` come from the geometry authority; its `point_size`,
> `background`, and `series_colors` come from the mark authority
> (`plot/kernels/scatter.rs:335`). No single struct owns the tuple the raster
> cache is keyed on.

That is why the divergence would surface as an intermittent stale frame rather
than a wrong number: a cache entry can be stored under a key describing a state
that neither authority was ever in.

Second finding, independent of the first and equally load-bearing for step 1:

> **There are three caches above the scatter draw, keyed by three different
> things**, and the outermost one is not keyed on settings at all
> (§4). A gesture that moves the viewport without bumping `render_revision`
> produces **no redraw whatsoever**.

## 1. The two paths, traced

### 1.1 Geometry — `PlotWidget.settings`

`render_plot_layer_in_widget` (`plot/common.rs:2610`) is the whole of it:

```
legend_content   ← self.settings            common.rs:2637
legend_layout    ← self.settings            common.rs:2641
layout           ← self.settings            common.rs:2642   kernel.layout(self.settings.clone())
requirements     ← layout, self.settings    common.rs:2643
plot_area        ← requirements, self.settings (via plot_area(), common.rs:2590)
transform        ← layout, plot_area        common.rs:2647   CoordinateTransformer::new
```

So the domain (`x_min…y_max`, `theme.rs:181-184`, applied at `theme.rs:514-519`)
reaches marks **only** through the transform, and the plot-area rectangle —
which is the raster's pixel size and the auto gate's pixel budget — is a
function of the geometry authority's typography and padding.

The identical sequence appears three more times, each re-deriving rather than
sharing: the overlay layer (`common.rs:2689`), the temporal-tick helper
(`common.rs:2548`), and the live event path (`common.rs:3010`).

### 1.2 Marks — `PlotWidget.settings_model`

```
kernel.plot_with_settings(backend, plot_area, &transform, …, &self.settings_model)
                                                            common.rs:2651
```

Scatter overrides that seam (`scatter.rs:107`) and resolves
`settings.resolve_for_layer(&self.layer_settings)`. Inside `draw`
(`scatter.rs:388`) that resolved value — not the widget's `settings` — decides
the render mode.

Kernels that do **not** override `plot_with_settings` take the trait default
(`common.rs:1569`), which calls `settings.resolve()` on the *model*. That is a
third resolution distinct from both `PlotWidget.settings` and
`resolve_for_layer`, and it coincides with the first only while the invariant
holds.

### 1.3 Where the two resolutions differ by design

`resolve()` (`theme.rs:363`) and `resolve_for_layer()` (`theme.rs:374`) are
identical except in the layer tier:

| tier | `resolve()` | `resolve_for_layer()` |
|---|---|---|
| library defaults | base | base |
| application theme | `apply_layer` | `apply_layer` |
| authored spec | `apply_layer` | `apply_layer` |
| **layer** | `apply_layer(&self.layer, …)` — **every field** (`theme.rs:367`) | `apply_layer_settings(layer)` — **four fields** (`theme.rs:378`) |
| pane override | `apply_layer` | `apply_layer` |

`apply_layer_settings` (`theme.rs:545`) writes only `point_size`,
`stroke_width`, `bar_width`, `category_gap`. `apply_layer` writes everything,
**including the domain carrier** (`theme.rs:514-519`) and **the scatter
carrier** (`theme.rs:520-525`: render mode, max vector points, downsample
target, raster threshold).

So a `domains` or `scatter` override placed on the `PlotSettings.layer` tier is
honoured by the geometry path and silently dropped by the mark path.

**Status: latent, not live.** `PlotSettings.layer` has no production writer —
every assignment to it in the crate is in the cascade precedence tests
(`theme.rs:608`–`theme.rs:1271`). The `pane_override` tier is likewise unwritten
outside tests. This is a trap laid for the first feature that uses either tier,
which a viewport gesture plausibly would.

## 2. Where the invariant is maintained, and where it is not

Production sites, all consistent:

| site | how |
|---|---|
| `gui/plot_pane/state.rs:46-48` | `plot_settings = plot_settings_model.resolve()` at construction |
| `gui/plot_pane/state.rs:68-70` | same, on `replace()` |
| `plot/grid.rs:647` (composite, single cell) | `settings: settings.resolve()`, `settings_model: settings.clone()` — derived together |
| `editor.rs:1504`, `editor.rs:1772` | both copied from one `HostPlotState` |

`HostPlotState.plot_settings` is written at exactly those two lines in
`state.rs` and nowhere else in the crate. So today the invariant is true.

Sites that already **rely on it being false**:

- `scatter.rs:1361-1371` (the raster evidence exhibit) sets the domain on
  `settings` while passing `PlotSettings::default()` as the model. It works
  precisely because the domain flows only through the geometry authority.
- `scatter.rs:1211-1224` (the render-matrix exhibit) sets the thresholds on
  **both**, with a comment recording that an earlier revision set them on the
  wrong struct and produced three identical arms.
- `adapters/toron/mod.rs:272`, `:391`, `:557`, `:705`, `:771` pass
  `ResolvedPlotSettings::default()` beside `PlotSettings::default()`. Safe only
  because `PlotSettings::default().resolve()` is `ResolvedPlotSettings::default()`;
  the moment one of those exhibits customises `settings`, it diverges silently.

This is the concrete history behind the audit: the seam has already produced two
plausible-but-wrong measurements, and the only thing that caught them was
counting draw calls.

## 3. The divergence catalogue

Each entry: mechanism, current status, and what it does under a gesture.

**D1 — the mixed-authority raster key.** `ScatterRasterKey`
(`scatter.rs:335-348`) takes `width`/`height` from `bounds` (geometry) and
`x_range`/`y_range` from the transform (geometry), but `point_size`,
`background`, and `series_colors` from the mark authority's resolved settings.
*Live.* Under a gesture that updates one authority and lets the other lag by a
frame — the natural optimisation, since re-resolving the cascade per frame is
wasteful — the entry stored is keyed on a hybrid state. The next frame that
returns to the earlier state gets a **hit against a bitmap that was never
correct for it**.

**D2 — the auto gate is a joint function of both authorities.**
`resolve_render_mode` (`scatter.rs:569`) compares `total_points` against
`settings.scatter_max_vector_points` and `scatter_raster_threshold` (mark
authority) and against `pixel_budget = bounds.width * bounds.height`
(geometry authority, and it is the **inner plot area**, not the widget rect).
*Live.* This is the mechanism behind the ~5,203,000 measured gate rather than a
6,912,000 constant: chrome moves the gate. A gesture that changes chrome
(a legend appearing, a tick-label band widening) can flip the render mode
without any settings change.

**D3 — the layer tier drops `domains` and `scatter`.** §1.3. *Latent.* If a
gesture is implemented by writing `PlotSettings.layer.domains`, the picture
moves (geometry path honours it) while any layer-tier scatter override is
dropped — mode and geometry then disagree about which regime the plot is in.

**D4 — the hover path mixes authorities.** `update()` builds its transform from
`self.settings` (`common.rs:3010`) and hit-tests through
`self.settings_model` (`common.rs:3021`); scatter's `hit_with_settings`
(`scatter.rs:142-148`) resolves `resolve_for_layer` for its hit radius.
*Live, and this is the only event path that exists today.* Divergence here puts
the hit tolerance in a different settings state from the geometry the cursor is
being tested against — a hover that reports the wrong point, or none.

**D5 — `hit()` without settings resolves a fresh default.**
`scatter.rs:138` builds `PlotSettings::default().resolve_for_layer(...)`,
discarding both authorities. *Live but confined:* only reachable through the
settings-free `hit` seam. Any interaction work that reaches for `hit()` instead
of `hit_with_settings()` silently gets library-default tolerances.

**D6 — under resize, geometry comes from neither authority.** `draw()`
(`common.rs:2915-2920`) substitutes `state.stable_bounds` — a `RefCell` on the
canvas state — for the real bounds while `resize_render_suspended` is set. *Live.*
The raster key's `width`/`height` then describe the frozen bounds. A gesture
overlapping a resize is keyed on a rectangle that is not on screen.

**D7 — the categorical x axis is entirely unkeyed.** `current_x_range_bits`
(`scatter.rs:462`) returns `(0, 0)` for any non-Cartesian layout, and
`current_y_range_bits` (`scatter.rs:469`) returns `(0, 0)` for anything that is
neither Cartesian nor `CategoricalX`. Scatter genuinely supports a categorical x
(`scatter.rs:58`, and `scatter_point` routes to `transform.categorical` at
`scatter.rs:478`), where point placement depends on the **category count and
order** through `band_width` (`common.rs:829-831`). *Live defect, low
reachability:* a categorical-x scatter large enough to rasterize, whose category
set changes while size, colours, and y-range hold, gets a cache **hit** on a
stale bitmap. Cheap to close — it needs a category-identity term in the key —
but it should be closed deliberately, not folded silently into reuse work.

## 4. Three caches, three keys

Reuse policy has to name which of these it is changing. They are independent and
only the innermost is settings-keyed.

| # | cache | keyed on | cleared when |
|---|---|---|---|
| 1 | `canvas::Cache` (`common.rs:2533-2534`) | `render_revision` + bounds **size** (`common.rs:2921-2925`) | `render_revision` changes; `HostPlotState.replace()` is its only production bumper (`state.rs:72`) |
| 2 | `ScatterRasterCache` (`scatter.rs:26`, `Mutex<Option<…>>` on the renderer) | `ScatterRasterKey` — mixed authority | key mismatch → full `build_raster_rgba` rescan of every source point |
| 3 | `ScatterPreparedData` (`Arc`, immutable after prepare) | kernel identity | kernel replacement |

Two consequences worth stating before step 1 opens:

**A viewport gesture must bump `render_revision` every frame or nothing
redraws.** Cache 1 sits above everything and is blind to settings. This is not
hypothetical: mutating `plot_settings.x_min` alone changes the picture in no
way the canvas will ever draw.

**Bumping it every frame is correct for cache 1 and useless for cache 2.**
Cache 2 survives the bump — it lives on the kernel, not the canvas state — and
then misses on its own domain term, which is exactly the measured pan cost
(5,361 ms at 7M points against an 8.5 ms steady hit). The reuse policy's whole
subject is cache 2's miss path; it must not accidentally be written as a
statement about cache 1.

## 5. What this constrains for step 1 (findings, not decisions)

**5.1 The Cartesian projection is exactly affine, and its parameters are
already in the key.** `cartesian()` (`common.rs:805-820`) is
`pixel_x = (data_x − x_range.0) · width / Δx`, `pixel_y = height − (data_y −
y_range.0) · height / Δy`. No axis-type term enters it: `AxisType`
(`common.rs:38-50`) is `Linear | Percent | Date | Datetime | Time`, all linear
in the stored numeric value, differing only in tick labelling. So the
old→new image-space map for a domain change is computable **from the key
alone** — the two range pairs plus width and height — with no access to the
data. That is a real enabling finding for image-space reuse, and it is why
reuse is bounded to Cartesian: the categorical branch (`common.rs:822-836`)
places by index and band width, which is not a function of the key at all
(D7).

**5.2 Point radius is baked into the bitmap, and the key term for it is
sound.** `build_raster_rgba` stamps a disc of `settings.point_size.radius_px()`
(`scatter.rs:509`), and `radius_px() = BASE_RADIUS_PX * factor`
(`spec/mod.rs:119-121`) is a pure function of the keyed `factor` bits. So the
key faithfully captures radius — but a blit that rescales the bitmap rescales
the radius with it. Reuse under zoom is therefore a **degraded preview**, and
the record has to say so; under pure translation the radius is preserved
exactly.

**5.3 A reuse predicate needs both authorities at once, atomically.** It must
answer "is this the raster path?" (mark authority, D2 — which also needs the
geometry authority's plot area) and "has the viewport moved affinely?"
(geometry authority). Reading them at two different moments is the divergence
D1 describes. The cheapest structural fix is to stop having two: derive the
resolved settings once per frame from the model and pass that one value to both
the layout and the mark seam. That is a design option for step 1 to weigh, not
a decision this audit makes — it touches every kernel's `plot_with_settings`
contract.

**5.4 The invariant deserves a gate before it carries weight.** House rule:
a zero-result oracle must be proven nonzero against the unchanged
implementation. The candidate assertion is
`settings == settings_model.resolve()` at widget construction, but three
existing call sites deliberately violate it (§2) — so the honest form is a
constructor that *derives* `settings` from the model rather than an assertion
that both were supplied consistently, with the exhibits changed to set the
model. That conversion is itself the falsifiable evidence: if the exhibits still
report the same numbers after being rewritten to a single authority, the
invariant costs nothing; if they change, the old numbers were reading a
divergence.

## 6. What this audit did not establish

- **No measurement was taken.** Every figure quoted here is from the committed
  evidence in `plans/product/impl/0137_scatter_threshold_evidence.md` and is
  engine-versioned to that run.
- **D7 is read, not run.** The categorical-x staleness follows from the two
  cited functions; no test was executed to observe a stale frame, and the point
  counts required to reach the raster path with a categorical x may make it
  unreachable in practice. Confirm before treating it as a defect to fix.
- **The composite/facet path was traced only to its single-cell arm**
  (`grid.rs:647`). The multi-cell arm re-resolves at `grid.rs:655` and was not
  followed through its per-cell geometry; a faceted raster scatter is out of
  scope here and should be checked before reuse is claimed for facets.
- **No gesture exists to observe.** ADR 0133 stages 3–4 are open, so every
  "under a gesture" statement above is a prediction from the code, not an
  observation. They are written to be falsifiable by the first gesture
  implementation.
