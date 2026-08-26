# The raster-gate audit — call paths and pixel-budget inputs

**Status:** audit, 2026-08-27. Not a record and not a plan. It is the
prerequisite PRODUCT-0140 was asked to have before its decision is drafted: every
path that reaches `resolve_render_mode`, and exactly what the pixel budget it
divides is made of. It decides nothing.

Traced at `92c2c876f`. Measurements are from two `#[ignore]` exhibits added with
this audit, `product_0140_gate_input_evidence` (`scatter.rs`) and
`product_0140_facet_cell_gate_input_evidence` (`grid.rs`), so every number below
is reproducible and none is reconstructed by hand.

## Verdict

Three findings, in the order that matters for the decision.

**1. The two conditions swap which one binds at roughly a 125,000-pixel plot
area.** `scatter_raster_threshold` is not a floor that always applies and the
pixel budget is not always the operative test — which is which depends on the
size of the plot. Every measurement taken in this arc so far sat on one side of
that crossover, which is why the threshold looked inert.

**2. The gate is never the widget rectangle, and on one 1200×720 widget it
ranges over 3,979,208 … 5,203,000 — a 1.31× spread — from chrome alone.** The
naive `width × height × 8` figure of 6,912,000 is not the gate in any
configuration measured.

**3. A facet cell's gate is less than half a single plot's at the same widget**
— 2,421,608 against 5,203,000 — because a cell's inner rectangle is about a
third of the figure.

## 1. Call paths

**There is exactly one production call site.** `resolve_render_mode`
(`plot/kernels/scatter.rs:774`) is called from `ScatterMarkRenderer::draw_with_motion`
(`:480`) and nowhere else; the remaining eleven references are unit tests of the
function itself. The gate has no second caller to keep in step, which is a
smaller surface than §6's authority problem had.

Its four inputs, and where each comes from:

| input | source |
|---|---|
| `total_points` | `prepared_data.total_points`, fixed at preparation |
| `supports_embedded_raster` | the backend |
| `bounds` | the **inner plot area** (§3) |
| `settings` | the kernel's own resolution, per PRODUCT-0139 §6 |

**Backend support is uniform in production and false by default.**
`PlotBackend::supports_embedded_raster` defaults to `false`
(`plot/common.rs:1305`). The four production backends all return `true`:
`IcedBackend` (`common.rs:1431`, the live renderer), `SvgBackend`
(`export.rs:207`), and `AvifBackend`/`PngBackend`, which delegate to the SVG
backend (`export.rs:517`, `:569`). Every other implementor is a test instrument
inheriting `false`. So the gate does not silently answer differently between
export and screen — worth stating, because a mode that varied by backend would
make any evidence path-dependent.

## 2. The arithmetic, and which condition binds

```
ScatterRenderMode::Auto:
    total_points <= scatter_max_vector_points          -> Vector
    supports_embedded_raster
      && total_points > scatter_raster_threshold        (default 1,000,000)
      && total_points > pixel_budget * 8                -> Rasterized
    otherwise                                           -> Downsampled
```

`pixel_budget = bounds.width * bounds.height` (`scatter.rs:795`), and the
default `scatter_raster_threshold` is `1_000_000` (`common.rs:1124`).

Both conditions must hold, so **the larger of the two is the operative one**, and
they cross where `inner_area × 8 = 1,000,000`, i.e. at an inner plot area of
**125,000 px** — roughly a 354×354 rectangle:

- **above it** — any ordinary full-window plot — the pixel budget binds and the
  threshold constant is inert;
- **below it** — small panes, and facet cells in a modest window — the threshold
  binds and the pixel budget is inert.

This resolves the standing note that the raster threshold "has never been
exercised". It has never *bound*: every run in this arc was a 1200×720 unfaceted
figure whose pixel budget (5,203,000) exceeds the threshold fivefold. The
committed 0139 evidence is consistent with exactly this — 1,000,001 points
downsampled (above 1,000,000 but below 5,203,000) and 7,000,000 rasterized.

**`PLOT_MIN_SHARE` floors the budget.** `allocate_bands` reserves at most
`1 − 0.15` of each extent (`common.rs:2333-2342`), so the inner rectangle is at
least 15% of the widget in each dimension — at least 2.25% of its area. At
1200×720 the gate can therefore never fall below about 1,244,160 however much
chrome is added.

## 3. What the pixel budget is made of

`bounds` is the plot area, and the chain that produces it is re-derived
identically at each entry point (`common.rs:2637-2647`):

```
widget rect
  → legend_layout(...).content_bounds        legend.rs:460
  → BandRequirements::from_layout(...)       common.rs:635
  → plot_area(...) = VerticalGuideBands::resolve_with_requirements_and_x_label(...)
                       .plot_area(bounds, padding, settings)
                                             common.rs:2378, :2540
```

Every input that shrinks it:

- **the legend**, but only in `Fallback` presence, which reserves width;
  `Show(position)` overlays and does not shrink, and `Hide` does not either
  (`legend.rs:465-485`);
- **title and subtitle**, each contributing `offset + size` when visible
  (`common.rs:2394-2403`);
- **the facet label**, `FACET_LABEL_TOP_INSET + facet_label_size`
  (`:2404-2407`);
- **the x guide band** — tick text rotated into height, plus the axis title when
  visible (`x_axis_guides`, `:2433-2464`);
- **the y guide band**, symmetrically;
- **authored padding**, which acts as a *floor* under the derived band rather
  than an addition (`:2409-2424`).

One of these is worth singling out because it is not a settings input at all.
`BandRequirements::from_layout` derives the tick bands from the **domain**
(`common.rs:635-658`) — the current `x_range`/`y_range` and axis type, through
formatted-label width estimation. **The gate is therefore a function of the data
currently in view, not of the widget and settings alone.** In principle a pan
that widens tick labels can shrink the plot area and move the gate mid-gesture.

I expected that effect to be material and measured it: it is **not**. Scaling
the y values by 10⁹ moves the inner rectangle from 1075×605 to 1063×605 — twelve
pixels, 1.1% of the gate. Real, worth knowing, and far too small to be the
reason for anything. Recorded here because I predicted otherwise.

## 4. Measured gate inputs, unfaceted

`product_0140_gate_input_evidence`, debug profile — the figures are geometry,
not timings, so the profile does not matter.

| configuration | widget | inner | share | effective gate | naive |
|---|---|---|---|---|---|
| baseline | 1200×720 | 1075×605 | 0.753 | 5,203,000 | 6,912,000 |
| with title | 1200×720 | 1075×557 | 0.693 | 4,790,200 | 6,912,000 |
| 8 series (legend) | 1200×720 | 905×605 | 0.634 | 4,380,200 | 6,912,000 |
| wide y tick labels | 1200×720 | 1063×605 | 0.744 | 5,144,920 | 6,912,000 |
| title + legend + wide labels | 1200×720 | 893×557 | 0.576 | 3,979,208 | 6,912,000 |
| small pane | 400×300 | 275×185 | 0.424 | 407,000 | 960,000 |
| small pane, wide labels | 400×300 | 263×185 | 0.405 | 389,240 | 960,000 |

The baseline reproduces the 1075×605 and 5,203,000 already committed in
PRODUCT-0139's Context, which is the check that this exhibit measures the same
thing the earlier one did.

Two things to read off it. **Chrome moves the gate by 1.31× on one widget** —
adding a title, a legend, and wide labels costs 23% of the budget. And **the
share falls as the widget shrinks** — 0.753 at 1200×720 against 0.424 at
400×300 — because guide bands cost roughly fixed pixels, so they take a larger
fraction of a smaller figure. A gate expressed as a fraction of the widget would
be wrong in a size-dependent way.

## 5. Measured gate inputs, facet cells

`product_0140_facet_cell_gate_input_evidence`, two cells side by side.

| figure | naive gate | per-cell inner | share | per-cell gate |
|---|---|---|---|---|
| 800×400 | 2,560,000 | 321.5×261 | 0.262 | 670,248 |
| 1200×720 | 6,912,000 | 521.5×581 | 0.350 | 2,421,608 |

Both cells are identical, as the shared-domain finding of the facet audit
implies they should be.

At 1200×720 a facet cell's gate is **2,421,608 against a single plot's
5,203,000** — a cell rasterizes at under half the point count. At 800×400 the
per-cell gate is 670,248, which is **below** the 1,000,000 threshold, so that
configuration sits on the other side of §2's crossover and the threshold binds.

A facet is therefore the configuration where both conditions are live and where
which one binds changes with the window size. It is also the configuration for
which no timing has ever been taken.

## 6. What this constrains for the decision

Findings, not proposals.

- **A single constant cannot express this.** The gate today is
  `max(threshold, inner_area × 8)`, and inner area moves with chrome, window
  size, facet count, and — slightly — the domain. Any decision to "raise" or
  "lower" the threshold has to say which of the two terms it is moving and in
  which regime it binds.
- **The `× 8` factor is the unexamined half.** The threshold constant has a
  record behind it; the multiplier appears only as a literal (`scatter.rs:799`)
  and no evidence in this arc prices it. It is the term that actually decides
  every full-window plot.
- **Motion is now cheap, which changes what the gate is trading.** PRODUCT-0139
  measured a reused frame at 7.9 ms against a 5,153.7 ms rescan. Rasterizing
  earlier buys a cheap *moving* picture, which was not true when the current
  values were chosen.
- **The cold cost is what rasterizing early now costs.** The first raster at 7M
  points was 5,220.8 ms, and it is paid on the frame the mode flips. A lower
  gate makes that stall more common, and no measurement in this arc prices the
  cold raster at the cardinalities near the gate itself.

## 7. What this audit did not establish

- **No timing was taken.** Everything here is geometry and code reading; the two
  exhibits measure rectangles. The cold-raster cost near the gate, and any facet
  timing at all, remain unmeasured.
- **The `× 8` multiplier's origin was not traced** beyond confirming it is a
  literal with no record citation in the source.
- **Only the `Auto` arm was audited.** The explicit `Vector`, `Downsampled`, and
  `Rasterized` modes bypass the gate entirely and are unchanged by anything a
  gate record would decide.
- **Non-Cartesian layouts were not measured.** Categorical band requirements
  take a different path through `from_layout` and may shrink the plot area
  differently; no categorical configuration appears in the tables above.

## 8. Pricing, added 2026-08-27 — the multiplier does not do what it says

Measured by `product_0140_gate_pricing_evidence` (`scatter.rs`) and
`product_0140_facet_cold_raster_evidence` (`grid.rs`) on the `evidence` profile,
same host. Every arm builds a fresh kernel, so each raster is cold, and every
arm reports its draw calls so the path is confirmed rather than assumed —
`raster_images=1` and `down_circles>0` throughout.

### Cold raster cost

| plot | points | cold raster |
|---|---|---|
| 1200×720 | 2,600,000 | 1,852.1 ms |
| 1200×720 | 5,203,000 (at the gate) | 3,666.2 ms |
| 1200×720 | 7,800,000 | 5,424.6 ms |
| 400×300 | 500,000 | 337.9 ms |
| 400×300 | 1,000,000 (at the threshold) | 681.0 ms |
| 400×300 | 2,000,000 | 1,339.2 ms |

Rasterizing costs **≈0.69 µs per point and is independent of plot area** — the
same rate at 400×300 as at 1200×720. **At the gate a full-window plot stalls for
3.7 s** on the frame the mode flips, and that stall is what a lower gate makes
more common.

### Cold raster inside a facet

Two cells, each rasterized, one figure. `images=2` confirms both cells took the
raster path.

| figure | points per cell | cold, whole figure |
|---|---|---|
| 800×400 | 1,000,000 | 1,368.0 ms |
| 1200×720 | 1,000,000 | 1,416.0 ms |
| 1200×720 | 2,421,608 (that cell's gate) | 3,417.1 ms |

The cost tracks total points, not cell geometry — 2M points across two cells
costs about what 2.6M costs in one plot, and the 800×400 and 1200×720 figures at
the same per-cell count differ by 3.5%. So a facet's raster cost is the figure's
point count at the same ≈0.69 µs, and the smaller per-cell budget means a facet
reaches that cost at a **lower figure-wide cardinality** than a single plot
would.

### The multiplier, priced against downsampling

| plot | density | points | downsampled | rasterized | winner |
|---|---|---|---|---|---|
| 400×300 | 1 | 50,875 | 228.8 ms | 40.0 ms | raster 5.7× |
| 400×300 | 2 | 101,750 | 388.3 | 73.8 | raster 5.3× |
| 400×300 | 4 | 203,500 | 587.1 | 141.5 | raster 4.1× |
| 400×300 | 8 | 407,000 | 748.1 | 276.1 | raster 2.7× |
| 400×300 | 16 | 814,000 | 851.9 | 555.4 | raster 1.5× |
| 1200×720 | 1 | 650,375 | 845.9 | 498.3 | raster 1.7× |
| 1200×720 | 2 | 1,300,750 | 896.3 | 944.5 | downsample 1.05× |
| 1200×720 | 4 | 2,601,500 | 1,056.7 | 1,849.4 | downsample 1.75× |
| 1200×720 | 8 | 5,203,000 | 1,339.6 | 3,644.0 | downsample 2.7× |
| 1200×720 | 16 | 10,406,000 | 2,017.8 | 7,219.3 | downsample 3.6× |

**Two findings, and the first contradicts an assumption stated earlier in this
audit.**

**The crossover is not size-independent; the two sizes disagree in direction.**
§6 reasoned that both costs are O(n) so the ratio should not depend on plot
area. That is wrong. On the small pane raster wins at *every* density measured
and its advantage shrinks as density rises; on the full window raster wins only
at density 1 and loses from density 2 onward, worsening monotonically. A single
multiplier cannot express both.

**At the multiplier's own value, rasterizing is the slower choice.** At density
8 on a full window — exactly where `× 8` switches the mode — raster costs
3,644 ms against downsampling's 1,339.6 ms, **2.7× worse**, and the gap widens
with density.

The mechanism is visible in the draw counts. Downsampled output **saturates**:
it draws ~139,700 circles at every full-window density from 2 upward, because
the target caps the draw, so its cost flattens (845.9 → 2,017.8 ms across a 16×
point range) and only the O(n) scan grows. Rasterizing has no such cap — it
stamps every point at a flat 0.69 µs. So raster beats downsampling exactly where
the downsampled *draw* dominates, which is at **low** point counts, and loses
where the point count is high. That is the opposite of the regime the gate
selects it for.

**This does not make rasterizing wrong; it makes the stated justification
wrong.** Two things the timings above do not capture:

- **Fidelity.** At 10.4M points the downsampled frame draws ~139,700 marks —
  1.3% of the data — while the raster stamps all of them. The gate's real trade
  at high density is fidelity for latency, not latency for latency.
- **Reuse.** PRODUCT-0139 measured a reused raster frame at 7.9 ms against a
  5,153.7 ms rescan. Downsampling has no equivalent: it re-scans every frame,
  every time. A raster costs 3.7 s once and then moves for 7.9 ms; a downsample
  costs 1.3 s per frame forever.

A decision that wants to keep rasterizing at high density therefore has to
justify it on fidelity and motion, and say so — the "raster is faster above 8
points per pixel" reading that the bare multiplier invites is not supported at
any density measured on a full-window plot.
