# ADR 0139: the image-space raster reuse policy

Status: **accepted 2026-08-26**, as amended in review (§4, §6, §7, and gates 3
and 6–7), then amended again on implementation evidence (§3, §7, and gate 5).
It follows PRODUCT-0138's withdrawal and the
step-0 settings-authority audit
(`plans/product/impl/0139_settings_authority_audit.md`),
which is its prerequisite and which it cites throughout.

It decides whether a scatter mark that has already been rasterized may reuse
that bitmap while the viewport moves, what such a frame is permitted to claim,
and the exact conditions under which reuse is valid. It decides that reuse is a
**motion-only degraded preview** and never a rendering mode.

It does not decide the raster threshold — PRODUCT-0140 follows this record and
is bound by it, because the threshold's correct value turns on whether motion is
cheap, which is what this record settles. It does not decide
`scatter_max_vector_points` or `scatter_downsample_target`, which are a coupled
pair held for their own record. It does not decide the gesture vocabulary,
which is PRODUCT-0133 stages 3–4. It does not change render-mode resolution, the
vector or downsampled paths, or `canvas::Cache` policy. It adds no authoring
surface: nothing here is spellable in Rune.

## Context

Rasterization is the only scatter path that survives a million points, and it
has never been exercised in the product: every measurement to date stopped at
the gate's exact value, and the comparison is `<=`.

When it was finally driven, at 7,000,000 points on engine `cde25904`, it
produced three figures that define this record's problem:

| scenario | median |
|---|---|
| first render, cache miss | 5,538 ms |
| steady re-render, same domain | 8.5 ms |
| pan, domain moves each frame | 5,361 ms |

**That pan cost is this cache's design, not rasterization.** Moving a bitmap is
cheap; this path never moves one. `ScatterRasterKey` includes `x_range` and
`y_range` (`plot/kernels/scatter.rs:335`), and any key miss calls
`build_raster_rgba` (`scatter.rs:500`), which scans **every source point** and
stamps it into a fresh buffer. A one-pixel pan therefore costs exactly what a
cold start costs. There is no bitmap reuse anywhere in the path.

The figures are engine-versioned. They price this decision; they are not a
standing benchmark, and no later record may splice new numbers onto them
without saying which binary produced each.

**Re-measured 2026-08-26, after §6 landed** (`impl: adr 0139 single settings
authority for geometry and marks`, `0f9f09c6b`; `evidence` profile; same host —
i7-12700K, 61 GiB, Linux 7.0.0-29-generic). The table above is **retained
unchanged** as the measurement that priced this decision. These are a separate,
later measurement on a different binary, recorded beside it rather than spliced
into it.

| scenario | 2026-08-25, engine `cde25904` | 2026-08-26 run 1 | run 2 |
|---|---|---|---|
| first render, cache miss | 5,538 ms | 4,938 ms | 5,297 ms |
| steady re-render, same domain | 8.5 ms | 7.8 ms | 7.9 ms |
| pan, domain moves each frame | 5,361 ms | 4,839 ms | 5,164 ms |

**Every structural discriminator is identical across the rewrite**: inner plot
area 1075×605, auto gate 5,203,000, the raster key moves on both pan and zoom,
and the exhibit draws `images=1 circles=0`. The exhibit is therefore still
measuring the raster path, and §10 gate 8's clause — *if the numbers move, the
prior ones were reading the divergence* — **does not apply**: a wrong path would
have shown up in those discriminators before it showed up in a timing.

**The timing differences are not attributed to the authority rewrite.** The two
heavy scenarios differ between run 1 and run 2 by 7.3% and 6.7%, which brackets
their distance from the 2026-08-25 figures; and the rewrite adds a resolution
per widget construction rather than removing work, so it is not a plausible
source of a speedup. One residue is recorded rather than dismissed: the steady
hit is 7.8 and 7.9 ms against 8.5, consistent across both runs and outside that
path's own 1.3% spread. It is smaller than a single frame and most plausibly
codegen, and it is not explained here.

**Reuse measured 2026-08-27, on the implementation** (`impl: adr 0139 motion
only raster reuse`, `d8920ab8f`, plus the exhibit's motion arm; `evidence`
profile; same host). §9's stop condition — *the blit is not materially cheaper
than a rescan, in which case this record is moot* — is **discharged**. Every arm
below is from **one run on one binary**, so the reuse figure and the baseline it
is compared against are not spliced across builds.

| scenario | ms |
|---|---|
| first render, cache miss | 5,220.8 |
| steady re-render, same domain | 7.9 |
| pan, no reuse (the baseline this record was written against) | 5,153.7 |
| **pan, frames declared as motion (reuse)** | **7.9** |
| settle after motion | 5,039.6 |

**A reused frame costs what a cache hit costs — 7.9 ms against 7.9 ms.** That is
the expected result and worth stating as such: reuse performs no pixel work at
all, so the only cost left in the frame is the chrome and the single image draw
that a cache hit already pays. Against the 5,153.7 ms rescan it is a **656×**
difference, and the reason is §2's — the map is computed from the two keys, and
the bitmap is placed rather than rebuilt.

Two further facts fall out of the same run, at a scale no unit gate reaches:

**Settle rasterizes truly at 7M points.** `settle_after_motion` is 5,039.6 ms —
a miss, not a hit — for a domain that had been *reused* but never rasterized. §5
holds where it costs the most.

**And that same figure is no-chaining evidence.** Had any reuse frame been
stored as the cache entry, the settled frame at that domain would have been a
7.9 ms hit. It was not. The record's rule that reuse composes only from the last
true raster is observable from the outside, not merely asserted in the code.

### What the audit established that this record depends on

**The Cartesian projection is exactly affine, and its parameters are already in
the key.** `CoordinateTransformer::cartesian` (`plot/common.rs:805-820`) is a
pure linear map from the domain to plot-area-relative pixels, with no axis-type
term: `AxisType` (`common.rs:38-50`) is `Linear | Percent | Date | Datetime |
Time`, every variant linear in the stored numeric value, differing only in how
ticks are labelled. So the old-to-new image-space map for a domain change is
computable **from the two keys alone**, with no access to the data — and the
cache already stores the key it was built under (`scatter.rs:38`), so reuse
requires no new retained state.

**The primitive already exists.** `plot_rasterized` (`scatter.rs:305-325`) draws
the cached handle with `draw_image_handle(Point::ORIGIN, bounds_size, handle)`
(`common.rs:1311`). Reuse is that same call with a different top-left and size.
It performs no pixel work at all: one draw call, whatever the point count.

**Three caches sit above the scatter draw, keyed on three different things**,
and only the innermost is settings-keyed. `canvas::Cache`
(`common.rs:2533-2534`) clears only when `render_revision` changes
(`common.rs:2921-2925`), whose sole production bumper is
`HostPlotState::replace` (`gui/plot_pane/state.rs:72`). This record governs the
**innermost** cache's miss path and nothing else.

**The two settings authorities are consistent today only by convention.**
`PlotWidget.settings` (`common.rs:2053`) drives geometry; `settings_model`
(`common.rs:2054`) drives marks; and `ScatterRasterKey` mixes them — size and
domain from the first, `point_size`, `background`, and `series_colors` from the
second. No struct owns the tuple the cache is keyed on.

PRODUCT-0133 §9 already decided where viewport state lives: zoom and pan are
**pane overrides**, session-scoped and ephemeral, reusing `Override<T>` in
PRODUCT-0007 §7's fifth precedence layer, and never mutating Rune source. That
tier is applied by both resolutions (`plot/theme.rs:368`, `:379`), so it does not
suffer the layer-tier divergence the audit records as latent.

## Decision

### 1. A reused frame is a motion-only degraded preview

Reuse re-places an existing bitmap under an affine map. It does not re-project
marks, does not consult the data, and does not produce the image a true
rasterization at the new domain would produce. It is therefore permitted only
**while the viewport is moving**, and every gesture ends in exactly one true
rasterization at the settled domain (§5).

This is the whole of the judgement this record makes. A reader who thinks a
moving scatter must remain per-point faithful is disagreeing with that
judgement, not with the measurements — and the alternative it is measured
against is 5,361 ms per frame, which is not a moving picture at all.

### 2. Reuse is valid only where the map is affine and derivable from the key

Every one of these must hold, and each is checked against the same resolved
settings value (§6):

- the layout is `PlotLayout::Cartesian` — categorical placement
  (`common.rs:822-836`) is by category index and band width, which is not a
  function of the key;
- the raster size is unchanged: `width` and `height` both equal the cached
  entry's;
- `point_size`, `background`, `series_colors`, and the series count are all
  unchanged from the cached key;
- the figure is not faceted, per §7;
- only `x_range` and `y_range` differ.

Under those conditions the map is determined by the two key range pairs and the
raster size. In x, writing the cached range `(x0ᵒ, x1ᵒ)` and the new range
`(x0ⁿ, x1ⁿ)` with `Δ` for each extent:

```
scale_x     = Δxᵒ / Δxⁿ
translate_x = (x0ᵒ − x0ⁿ) · W / Δxⁿ
```

y takes the same scale form, `Δyᵒ / Δyⁿ`, with its offset fixed by mapping
`y0ᵒ` through the inverted vertical axis. Both are exact: they are the
composition of the cached projection's inverse with the new one, and
`cartesian` is linear, so nothing is approximated except by the resampling of
the blit itself.

A pure pan gives `scale = 1` in both axes, which is a translation.

### 3. The plot-area clip is a property of the mark, not of the kernel wrapping it

Scatter marks are drawn under `with_save`, not `with_clip`
(`common.rs:2659-2663`), because no scatter kernel requests the clip. That is
harmless today: the image is drawn at `Point::ORIGIN` at exactly the plot-area
size. A translated or scaled image is not so bounded — it would paint over the
axis bands and the legend. **Reuse must not be implemented without the clip.**

The rule this record sets is where that requirement lives:

> **A mark declares whether it must be clipped to the plot area, and every
> kernel that renders that mark honours the declaration.** A kernel does not
> decide it.

Two findings force that wording, both established by implementation rather than
by reading, and the first is why the original "scatter requests the clip"
phrasing was incomplete.

**One mark, two kernels.** `scatter("x","y")` lowers to a plain
`ScatterPlotKernel`, while `scatter("x","y").point_size(1.0)` lowers to a
**one-layer `LayeredPlotKernel`** — `gui/state.rs:7291` asserts exactly that,
requiring every modified mark to take the layer-aware output. Putting the clip
on `ScatterPlotKernel` alone therefore makes the same picture clip differently
depending on whether any mark modifier is spelled, which
`product_0018_point_size_reaches_every_host_path_and_preserves_the_view` catches
as an SVG difference of exactly the two `clipPath` elements. **The modifier
spelling and the bare spelling must clip identically**, and that is an
acceptance requirement, not an implementation detail.

**The clip cannot be narrowed to the image draw.** Wrapping only
`plot_rasterized`'s `draw_image_handle` in `with_clip` would be the smaller
change and would be spelling-independent, but it is unsafe: in the unclipped
branch the canvas background is a **parent-level** fill, and
`plot/grid.rs:656-661` records that iced_wgpu z-orders a parent-level fill above
every clipped child. The background would paint over the raster. The kernel-level
flag is what moves the background into a clipped child in the first place, so
the clip must be requested at kernel level — which is precisely why the mark's
declaration has to reach every kernel that renders it.

The implementation halts and reports if the clip cannot be introduced without
moving another draw (§9). That condition did not fire: the clipped branch already
emits the canvas background as its first clipped child
(`common.rs:2659-2667`), which is the mitigation the hazard note prescribes.

### 4. The exposed band carries plot chrome and no marks, and that is forced

A translated bitmap has no pixels for the region the viewport moved into. What
shows there is not nothing: the grid is drawn **beneath** the marks and is
redrawn every frame at the new domain (`common.rs:2650-2651`), so the uncovered
band shows the freshly drawn canvas background and gridlines — already correct
for the new viewport — with no marks in it.

The accepted preview is therefore precisely this: **no marks in the exposed
band; the underlying plot chrome remains visible.**

Filling that band with marks is not available, and the reason is worth stating
because it is what makes this forced rather than chosen:

- **Rasterizing just the exposed band costs what a full raster costs.**
  `build_raster_rgba` iterates every point of every series (`scatter.rs:514`);
  selecting only the points that fall in a band still requires visiting all of
  them, because `ScatterPreparedData` carries no spatial index. Partial fill
  buys nothing.
- **Rasterizing beyond the plot area in advance** — an overscan margin — is
  rejected in its own right below.

So marks at the leading edge are absent until the gesture settles, and this
record states that plainly as the fidelity cost it is accepting. Zoom-in
exposes nothing, being a crop and a magnification; zoom-out exposes on all four
sides.

One consequence is a compositing claim, not a buffer claim, and §10 gates it as
such. The raster is drawn over the grid, and its empty pixels carry the canvas
background's own alpha (`scatter.rs:544-549`), so wherever that background is
opaque a steady raster frame **occludes** the grid across the plot area. Reuse
then *reveals* chrome in the exposed band that the covered region hides, and
the two regions need not look alike. Whether that reads as a legible motion
artifact or as a broken frame cannot be settled from the RGBA buffer — it is a
property of the composited result and is gated against a real backend.

### 5. Every gesture ends in exactly one true rasterization, and reuse never chains

Two rules, and they are what keep §1 honest:

**Settle.** When motion ends, the mark rasterizes at the settled domain and that
frame replaces the preview. A preview must never persist into a steady frame.

**No chaining.** A reused frame is never stored as the cache entry. Every
reuse composes from the last *true* raster, not from a previous reuse.
Chaining would compound resampling blur and accumulate translation error across
a drag, and it would do so invisibly — the picture would degrade with the
length of the gesture rather than with anything the reader could see a reason
for.

### 6. One resolution is the authority, and geometry consumes it

Reuse asks two questions at once — *is this the raster path?* and *has the
viewport moved affinely?* — and the audit's §5.3 finding is that those questions
are answered today from two different structs. Reading them at two moments is
what would put a divergence under the gesture path, surfacing as an
intermittent stale frame rather than a wrong number.

The remedy is not simply to derive `PlotWidget.settings` from `settings_model`.
That would leave geometry on `PlotSettings::resolve()` (`theme.rs:363`) while
scatter's marks continue to resolve `resolve_for_layer(&self.layer_settings)`
(`scatter.rs:107`), and those two differ **at the layer tier**: `resolve()`
applies the `PlotSettings.layer` carrier in full, including the `domains` and
`scatter` carriers (`theme.rs:367`, `:514-525`), while `resolve_for_layer`
replaces that tier with four fields (`theme.rs:378`, `:545`). Deriving one from
the other narrows the gap without closing it.

Nor is a single widget-wide resolved value available in general: a layered
kernel resolves **per layer**, `resolve_for_layer(layer.settings())`
(`layered.rs:221`, `:246`), which is exactly what the `plot_with_settings` seam
exists to permit (`common.rs:1559-1560`).

So the rule is stated in terms of authority rather than of a single struct:

> **The resolution the mark renderer draws with is the authority.** The
> transform, the render-mode decision, the raster key, and the draw itself must
> all consume that same resolved value, obtained once per frame.

For a single-layer scatter that value is
`settings_model.resolve_for_layer(&self.layer_settings)`, resolved once and
threaded through `kernel.layout(...)` as well — so the plot area and the domain
that keys the raster come from the same resolution as `point_size`,
`background`, and `series_colors`. `PlotWidget.settings` ceases to be an
independent input on this path.

Where a scatter mark renderer sits inside a layered kernel whose layers resolve
to different geometry-affecting values, there is no single authority to name:
`PlotWidget` can express one geometry per figure, not one per layer.
**Reuse is refused for that kernel**, and the implementation halts and reports
if it finds the case reachable rather than inventing a rule for it (§9).

Two consequences bind the gesture implementation, both from the audit:

- A gesture writes `pane_override.domains` per PRODUCT-0133 §9, **re-resolves
  through the authority above, and bumps `render_revision`.** Without the bump
  the canvas cache serves the old geometry and no frame is drawn at all — the
  viewport change is invisible, not merely stale. The pane-override tier is
  applied by both resolutions (`theme.rs:368`, `:379`), so it is not subject to
  the layer-tier divergence above.
- Bumping `render_revision` every frame is correct for the canvas cache and does
  nothing for the raster cache, which survives it. The raster cache's miss is
  the cost this record addresses; nothing here should be read as a statement
  about `canvas::Cache`.

### 7. Facets are correctness-eligible and intentionally unimplemented

Reuse applies when the resolved mode is `Rasterized`. The vector and downsampled
paths hold every point and re-project them per frame; they have no bitmap to
reuse and are untouched. Mode resolution itself (`scatter.rs:569`) is unchanged.

**The facet exclusion is no longer a correctness boundary.** When this record was
accepted, the composite's multi-cell arm had not been read, so §2's conditions
and §6's authority rule were unestablished there. Both have since been settled by
work this record sequenced, and the exclusion's original reason is discharged:

- the facet audit (`plans/product/impl/0139_facet_geometry_audit.md`) found the
  composite derives **one shared domain** across cells — `shared_layout` takes
  the union of every cell's ranges and `merge_layout` replaces each cell's range
  with it — so a viewport change moves all cells identically and each cell's
  affine map is computable from its own key;
- §6's authority rule now reaches the composite: every entry point derives
  per-cell layout and plot area through that cell kernel's own resolution;
- §3's clip is honoured per cell, and each cell owns its own raster cache, so
  no cell can hit or evict another's entry;
- both of §2's per-cell conditions are **gated**, not argued: cells share one
  domain before and after a move, and a cell's plot rectangle — the raster's
  width and height — does not move with the domain.

**It stays unimplemented anyway, and the reason is not correctness.** §4's
accepted cost is that marks at the leading edge are absent until settle. In one
plot that is a single mark-free band. In a facet grid it is **one band per
cell**, opening and closing together across the figure on every drag. That is
the same cost in kind and a materially different thing to look at, and §1's
judgement — that a moving picture is worth more than the marks a preview drops —
was made about one plot, not about a grid of them.

No gesture exists yet, so nobody can look at it. This arc's record on that point
is unambiguous: five defects shipped green and were found by a person examining
a figure. **Facet reuse waits for PRODUCT-0133 stages 3–4 to make the multi-cell
preview observable**, and the judgement is then made against the thing itself
rather than against a description of it.

**Amended by PRODUCT-0150 at gesture implementation.** Motion now must reach
every composite cell so viewport-dependent preparation does not run per moving
frame. The exclusion is therefore enforced by an explicit interactive reuse
policy: lone plots receive `Placed`, while facet cells receive `Refused` and
draw only an exact-key bitmap or nothing. The old by-construction assertion
became false at the point the gesture made motion real and is re-derived by the
same test against the interactive path, with the unfaceted placed preview as
its discriminating control.

Admitting facet placement remains the same later judgement: change `Refused`
only through an attributed amendment after observing the multi-cell preview.
The architecture guard now requires motion in both hand-rolled composite
branches, because preparation and overlay must agree on the moving frame.

### 8. PRODUCT-0138's withdrawal is not disturbed, and D7 stays out of scope

The vector threshold and the downsample target remain a coupled pair awaiting a
joint record. Nothing here changes either constant or the band they govern.

The audit's **D7** — `current_x_range_bits` (`scatter.rs:462`) returns `(0,0)`
for any non-Cartesian layout, leaving a categorical-x scatter's x axis entirely
unkeyed while `transform.categorical` places by category index and band width —
is **explicitly out of scope**. §2 excludes categorical layouts from reuse
anyway, so this record neither depends on that defect nor repairs it. It was
read, not run, and its reachability is unconfirmed: a categorical-x scatter must
first exceed the raster gate. Confirm it, then fix it in its own change. Reuse
must not be cited as having addressed it.

### 9. Stop conditions

Implementation halts and returns here if any of these is found:

- §6's authority rule cannot be satisfied — the transform, the mode decision,
  the key, and the draw cannot be made to consume one resolution — without
  changing the `plot_with_settings` contract for kernels other than scatter;
- a scatter mark renderer is found reachable inside a layered kernel whose
  layers resolve to different geometry-affecting values, so §6 has no single
  authority to name for that figure;
- the plot-area clip of §3 cannot be introduced without relocating another draw
  call, i.e. the `grid.rs:656` hazard is live for this path — **this condition
  was tested and did not fire**;
- a kernel is found that renders a clip-requiring mark and cannot honour the
  mark's declaration, so §3's two spellings cannot be made to agree;
- a settled re-raster is not byte-identical to a cold raster at the same domain
  (§10, gate 2) — that would mean reuse mutated state it does not own;
- the blit is not materially cheaper than a rescan, in which case this record is
  moot and should be withdrawn rather than amended.

### 10. Acceptance gates

Each gate names the boolean property it tests and renders both states it
distinguishes.

1. **Rescan count under motion.** An instrumented count of `build_raster_rgba`
   entries. A pan sequence of *n* frames under reuse performs **0** rescans.
   The negative control is the same sequence with reuse disabled, which must
   perform **exactly n**, and that control's nonzero value is asserted in the
   tree — not merely stated here. A zero-result oracle that has never been shown
   nonzero against the unchanged implementation proves nothing.
2. **Settle.** Gesture end performs exactly one rescan, and the resulting buffer
   is byte-identical to a cold raster at the settled domain. Equality is
   legitimate here because both frames come from the same code path.
3. **Exposed band, against a real backend.** Pan far enough that points would
   enter view. In the preview frame the exposed band contains **no marks**, and
   the chrome beneath it — canvas background and gridlines at the new domain —
   is present and correct; **the same band after settle contains marks.** Both
   states are rendered, so the gate distinguishes "reuse left stale points
   there", "reuse correctly showed no marks", and "settle never happened",
   which a single-state probe cannot. This is a **composited** claim: what the
   band shows is the result of the raster being drawn over the grid, so it is
   gated with a tolerant semantic-region probe on the production renderer, not
   by reading the RGBA buffer the reuse path produces.
4. **No chaining.** After *n* consecutive reuse frames, the cache entry's key is
   unchanged and identifies the last true raster.
5. **Both spellings of a mark clip identically.** A bare `scatter(...)` and a
   modified `scatter(...).point_size(n)` must produce the same clip structure,
   even though they lower to different kernels (§3). The discriminating control
   is a layered kernel whose only layer is a **line**, which must not request
   the clip — without it the gate would pass on an implementation that clips
   every layered plot unconditionally.
6. **Refusal, four separate cases.** A size change, a `point_size` change, a
   series-colour change, and a categorical-x layout each force a rescan,
   asserted individually. A single "any change" case would pass on the strength
   of whichever one happens to work.
7. **Axis type does not invalidate, and that is the point.** An `AxisType`
   change alone — same numeric ranges, same size, same colours — must **not**
   force a rescan, and the marks must be pixel-identical across it. This gate
   is the converse of gate 6 and it exists because an earlier draft of this
   record listed axis type among the refusals, contradicting its own §2 and the
   affine finding it rests on: every `AxisType` variant is linear in the stored value
   (`common.rs:38-50`) and changes only tick labels, which are chrome drawn
   outside the raster. Axis type is deliberately **not** in `ScatterRasterKey`,
   so a refusal on it would not be implementable without first adding it —
   adding a key term to invalidate on something that cannot move a mark is how
   a cache acquires cost it cannot justify.
8. **Single authority.** The audit's §5.4 conversion, in §6's stronger form:
   geometry consumes the mark renderer's resolution, with the three exhibits that currently set the
   two fields independently (`scatter.rs:1211-1224`, `scatter.rs:1361-1371`, and
   the `adapters/toron/mod.rs` sites) rewritten to set the model. Their reported
   numbers must hold across the rewrite. If they move, the prior numbers were
   reading the divergence and must be re-taken before anything cites them.

Oracle classification, per the house rule. "Did the raster rescan", "was the
settled buffer byte-identical", and "are the marks pixel-identical across an
axis-type change" are answerable from the instrument and the RGBA buffer:
**assertion gaps**, no production renderer required, because the reuse map is
computed in image space and the buffer is available before it reaches a
backend.

Two gates are **backend gaps** and neither can be discharged from the buffer or
the draw-operation list. §3's clip: whether a translated image escapes the plot
area. And gate 3's band: what the exposed band shows is produced by the raster
being composited over the grid — the grid is drawn first (`common.rs:2650-2651`)
and the raster's own empty pixels carry the canvas background's alpha
(`scatter.rs:544-549`), so the buffer cannot answer what a viewer sees where the
two meet. Both need tolerant semantic-region probes against a real backend, and
neither is licensed to use a GPU byte baseline.

## Rejected

**An overscan margin.** Rasterize a margin beyond the plot area so short pans
have real pixels at the leading edge, eliminating §4's mark-free band. Genuinely
attractive: it is the only option that makes a moving frame truthful rather
than merely fast. Rejected because the margin's size would be exactly the kind
of unmeasured constant PRODUCT-0138 was withdrawn for, and because it inflates
the cold miss — already 5,538 ms at 7M points — by the margin's area fraction,
paying on every first render for a benefit only a gesture collects.

**Incremental band rasterization.** Fill only the newly exposed band. Rejected
on §4's reasoning: it is O(n) in the source points regardless of band size,
because there is no spatial index, so it costs what the full raster costs.
Attractive enough to be worth reopening if a spatial index is ever built for
another reason — but a spatial index is a data-structure record, not this one.

**Reuse as a rendering mode, with no settle.** Simpler, and it would make a
million-point scatter feel fast all the time. Rejected because it makes the
degraded preview the product: the picture a user reads and screenshots would
never be the picture the data implies, and nothing in the frame would say so.

**Chaining reuse from a reused frame.** Cheaper still, and the natural
implementation if the cache is written naively. Rejected in §5: error that
grows with gesture length and is invisible at every individual step.

**Deciding the raster gate here.** The gate and reuse are related the way
PRODUCT-0138 found the vector threshold and downsample target to be related,
and the temptation is to settle both at once. Rejected because the direction of
dependence is clear and one-way: the gate's correct value depends on whether
motion is cheap, so it must follow this record rather than accompany it.

**A numeric bound on zoom scale.** Restrict reuse to scale factors within some
band, beyond which marks are too distorted to show. Rejected: any value chosen
today would be an unmeasured constant, which is precisely the error that
withdrew PRODUCT-0138. The distortion is real — a blit scales the baked-in point
radius with the image — but it is bounded by the gesture rather than by a
threshold, and whether a bound is additionally needed is a question for evidence
taken against a real gesture path.

**Fixing D7 in this record.** It is a live defect in the same function family
and it would be a small change. Rejected on scope discipline: it is unconfirmed
by execution, it is excluded from reuse by §2 regardless, and folding an
unmeasured repair into a policy record is how a record acquires claims it cannot
support.

## Consequences and sequencing

The change is confined to the scatter kernel's raster path plus the settings
derivation of §6. There is no authoring change, no new Rune word, no migration,
and no change to any accepted record — PRODUCT-0133 §9 supplies the viewport
tier this record assumes, and is not amended by it.

**Nothing here is implementable end to end until PRODUCT-0133 stages 3–4 supply
a gesture.** That is not a reason to defer the record: the existing raster
evidence exhibit already drives domains programmatically (`scatter.rs:1361-1371`
sets a domain directly and renders), so every gate in §10 is reachable before a
gesture exists, and gate 8 changes that exhibit anyway. What cannot be reached
without a gesture is the interaction-frame budget — and this record deliberately
sets no such budget. PRODUCT-0138 §5's ~6,000-point figure remains what it was
called there: a hypothesis derived from a static curve, not a proposal.

Sequencing from here:

1. this record;
2. **done** — the facet audit and its per-cell gates. §7 now records facets as
   correctness-eligible and deliberately unimplemented; the remaining question
   there is observational, not structural;
3. PRODUCT-0140, the raster gate, bound by §1 and §7 and by whether motion
   proves cheap — which it does: a reused frame costs what a cache hit costs;
4. the coupled vector-threshold/downsample-target record, independent of both;
5. D7, confirmed by execution first, then fixed on its own.
