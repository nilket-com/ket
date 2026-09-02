# The gesture census — what PRODUCT-0133 stages 3–4 land on

**Status:** census, 2026-09-02. Not a record and not a plan. It is the
prerequisite for drafting the viewport-gesture record that PRODUCT-0139 §7,
PRODUCT-0140 §4, and PRODUCT-0149 all name as their activator. It decides
nothing; where a decision is needed it says so and names the owner.

Traced at `aaa0ec24b` (the ADR 0149 close). The one measurement below was
taken with a throwaway unit test that is not retained; the gesture record's
stage 0 must bank it as a kept exhibit.

## 1. What stages 3–4 owe, and what the raster arc needs

The 0133 implementation plan's stage 3 bundles three things: the pane-override
viewport (wheel zoom, drag pan, gesture arbitration), the typed selection
algebra, and selection-context lifecycle. Stage 4 connects the grid and export
consumers.

Only the first of these is on the raster arc's critical path. PRODUCT-0139's
motion preview and PRODUCT-0149's off-thread substrate are both dormant until
something produces `ViewportMotion::Moving` frames and re-resolves the domain
through the pane-override tier. Selection, contexts, and the grid consumer do
not touch that machinery.

**Finding.** The viewport gesture is separable from selection and is the
smaller half by a wide margin. Everything selection needs (structured hits,
tokens, the algebra) is either shipped or a self-contained addition; nothing in
it depends on the gesture landing first, and nothing in the gesture depends on
selection. The census therefore treats "stage 3a — viewport gesture" as its
own implementation slice and recommends it be recorded that way.

## 2. The event surface today

`PlotWidget::update` (`plot/common.rs:3386`) answers exactly three inputs:

| event | canvas | effect |
| --- | --- | --- |
| `Window(RedrawRequested)` while `Settled` | `Data` / `Full` | computes raster wants, publishes only novel ones (0149) |
| `ModifiersChanged`, `ButtonPressed(Left)` | `OverlayInteractive` / `Full` | Ctrl + double-click opens the settings inspector; single Ctrl-click is tracked and captured |
| `CursorMoved` | `OverlayInteractive` / `Full` | full hit-test, publishes `UpdateHover` when the hit changes |

There is no `ButtonReleased`, no `WheelScrolled`, no drag state, and no
`Moving` frame anywhere in production. `with_viewport_motion`
(`common.rs:2241`) has test callers only.

**Canvas topology.** The host panel is `stack![data, interaction]`
(`gui/plot_pane/view.rs`): the `Data` canvas owns the raster coordinator and
the `OverlayInteractive` canvas owns every pointer gesture. iced's `Stack`
delivers events top-down and stops at the first capture
(`iced_widget-0.14.2/src/stack.rs:249-262`), so a gesture that captures on the
overlay never reaches the data canvas, and the data canvas's redraw seam is
unaffected by pointer capture. The notebook surface is one `Full` canvas
carrying both roles (`editor.rs:4867`).

**Per-canvas state.** `PlotCanvasState` (`common.rs:2718`) already holds
gesture-scoped state across view rebuilds: `settings_click`, `modifiers`,
`last_hover`, the two caches, and `stable_bounds`. Drag anchors and the
in-progress pan belong here by precedent; the committed viewport does not (§3).

**Wheel precedent.** `gui/table.rs:612` and `editor.rs:2633` both consume
`WheelScrolled` with `ScrollDelta::{Lines, Pixels}` arms; the plot gesture
should follow the same delta normalisation rather than a third one.

## 3. The viewport seam already exists and has no writer

`PlotSettings.pane_override.domains: Override<PlotDomainOverrides>`
(`plot/theme.rs:180`, `:240`) carries `x_min/x_max/y_min/y_max`. Both
resolutions apply the pane tier last (`theme.rs` `resolve` and
`resolve_for_layer`), which is exactly PRODUCT-0139 §6's single authority. Every
cartesian kernel consumes it as
`settings.x_min.unwrap_or(prepared.x_range.0)` (`scatter.rs:157`,
`layered.rs:240`; the grid through per-cell layouts).

**No production code writes `pane_override` today.** Every writer is a test
(`theme.rs:609-761`, `presentation_registry.rs:2122`). The gesture will be the
tier's first production author, which is the shape PRODUCT-0007 §7 designed it
for.

**Where the committed viewport lives.** `HostPlotState` (`gui/plot_pane/state.rs`)
holds `plot_settings_model: PlotSettings` and `render_revision`. A committed
pan or zoom is a write to `plot_settings_model.pane_override.domains` followed
by a re-resolve of `plot_settings` and a `render_revision` bump, in `update`,
through a new `PlotPaneMsg` — the same shape `RasterCompleted` uses. 0139 §6's
warning stands: without the bump the canvas cache serves the old geometry and
the change is invisible.

**Notebook parity gap.** `NotebookPlotSurface` (`editor.rs:546`) holds a
`ResolvedPlotSettings`, not a `PlotSettings` model, and rebuilds a model with
`from_resolved` at view time. There is no pane tier to write. Parity with the
host panel (the 0149 precedent) requires the surface to own a model, or a
viewport field applied when the model is rebuilt.

**Kernel replacement.** `HostPlotState::set_plot` and the preview/promote
lifecycle rebuild `plot_settings_model` from `host_plot_settings_model(...)`,
which drops the pane tier. A rerun therefore resets the viewport. That is the
existing "panel replacement lifecycle" the impl plan defers to, and it is the
right default; the record should state it rather than inherit it silently.

**Durability.** `session_persistence.rs` stores panel identity maps and
nothing of the settings model, so a viewport is not durable across sessions.
Consistent with 0133 §9's "session-scoped and ephemeral".

## 4. Motion plumbing: what exists, what does not

- `ViewportMotion` (`common.rs:2161`) is `Settled | Moving`, defaulting to
  `Settled`. `PlotWidget` receives it at construction; the host view passes
  nothing, so the host state needs a `viewport_motion` field the view forwards.
- The 0149 request seam publishes raster wants **only when `Settled`**
  (`common.rs:3393`). A `Moving` frame therefore requests nothing, draws the
  0139 placed preview from the slot's last-good, and the first `Settled` frame
  after release requests exactly one raster. That is 0139 §5 and 0149 §9
  composed, and it needs no new code at the seam.
- Hexbin already honours motion: `prepare_viewport` (`kernels/hexbin.rs:151`)
  marks a stale preview under `Moving` instead of re-aggregating per frame.
  The gesture activates it for free.
- The composite does **not** forward motion to facet cells, by construction and
  gated (0139 §7). Every cell is `Settled` during a drag, so a faceted
  large-rung scatter will churn one raster request per cell per frame and show
  the surface's preparing marker while dragging rather than a placed preview. 0139 §7 wants a person to look at the
  multi-cell preview before deciding; the gesture record should leave the
  exclusion in place and name the observation as a deliverable.
- **Settle has no event for wheel zoom.** A drag settles on `ButtonReleased`;
  a wheel zoom has no terminal event. The codebase's timing precedents are
  `time::every` subscriptions for blink and spinner (`editor.rs:2123-2134`) and
  `Task::perform` over async work. The obvious shape is a generation-stamped
  settle task after a quiet interval; the record must pick the interval on
  evidence and state that a late settle for a superseded generation is dropped,
  which is the 0149 identity discipline again.

## 5. Coordinate math and layout families

- `CoordinateTransformer::pixel_to_cartesian` (`common.rs:883`) inverts the
  cartesian map; anchored zoom at the cursor is a few lines on top of it.
- It returns `None` for `CategoricalX`, `CategoricalY`, `CategoricalXY`,
  `Parallel`, and `Radial`. A category axis has no `x_min`; the gesture must
  scope to the continuous axis or refuse per layout family, stated per family.
- `AxisType` is `Linear | Percent | Date | Datetime{..}` — no log axis (AX-05
  deferred). Every axis is affine in `f64`, temporal domains included, so pan
  and zoom are domain arithmetic with no scale-space special case.
- D7 (`current_x_range_bits` → `(0,0)` for non-cartesian) stays masked as long
  as categorical layouts never enter the `Moving` reuse path; the per-family
  refusal above is what keeps it masked.
- Facets share one domain across cells (the 0139 facet audit), so one pane
  override moves every cell. Anchoring a zoom to the cell under the cursor needs
  the cell rectangle, which the grid's `hit` path already computes.

## 6. Hover is a UI-thread stall today, and a drag multiplies it

`ScatterPlotKernel::hit_point_with_radius` (`scatter.rs:364`) scans every point
of every series on each `CursorMoved`, on the UI thread, regardless of render
mode. Measured (debug profile, 1200×720, one throwaway test, not retained):

| points | one hit-test |
| --- | --- |
| 1,000,000 | 360 ms |
| 7,000,000 | 2,490 ms |

This is a stall on every cursor move over a large scatter **before any gesture
exists**. A drag is a continuous stream of `CursorMoved`; wiring pan onto the
overlay canvas without addressing this would make every `Moving` frame pay a
full scan for a hover nobody can read while dragging.

Two consequences, one for the gesture record and one outside it:

- **In the gesture record:** hit-testing is suppressed while a gesture is
  active (a drag has a fixed intent; hover is meaningless under it), and
  resumes at settle. This is cheap and belongs to gesture arbitration.
- **Outside it:** hover on large scatters is its own stall, of the same
  character as the one 0149 removed from rasterization, and deserves its own
  record (spatial index or off-thread hit resolution with the 0149 identity
  discipline). It is not a precondition for the gesture.

Release-profile numbers were not obtainable at census time; see §9.

## 7. The inspector already exposes the pane tier, which creates a promote path

`XAxisBounds` and `YAxisBounds` are registered settings
(`presentation_registry.rs:1470-1497`) whose effective value is projected from
the resolved `x_min/x_max`, and whose provenance is reported as
`ProvenanceTier::PaneOverride` when the pane layer authors them
(`presentation_registry.rs:1954`).

So after a pan, the inspector will show the viewport as the effective X and Y
bounds with "pane" provenance. If the user then edits and submits that row, the
0142 writeback machinery writes it into Rune. That is precisely the "explicit
promote viewport to authored domain" that 0133 §9 says "may be added by a later
record": it already exists implicitly, as an ordinary inspector submit.

Writeback itself never reads `pane_override` (`settings_writeback.rs` plans
from the authoritative model and the draft string), so **the gesture alone
cannot mutate Rune** — §9's guarantee holds by construction and needs only a
gate. What needs a decision is whether the implicit inspector promote is
acceptable as the promote path, or whether the pane-provenance row should
refuse submit until a promote record exists. Recommendation: accept it and
name it, since it is explicit, user-initiated, and already disclosed by the
provenance column; add a gate that the row's provenance reads "pane" after a
gesture.

## 8. Export under a viewport

`host_plot_exports` (`editor.rs:1872`) snapshots `plot_settings_model`, pane
tier included, so an export taken while zoomed renders the zoomed domain.
0133 §10 says export resolves the current relation reference and gets no policy
of its own; it says nothing about presentation state. Recommendation: export
what is on screen, and state it, because the alternative (strip the pane tier
on export) is a second export policy of exactly the kind §10 rejects.

## 9. Release profile

`cargo check --release --tests -p polariton` fails on the clean tree at
`aaa0ec24b`: twelve errors, all the same shape — test-only widgets built with
a `()` renderer that fails iced's `Renderer`, `Paragraph`, and `Editor` bounds
(`gui/table.rs:1278-1279`, `editor.rs:6026-6031`, and siblings). The debug
test build is unaffected. This is unrelated to plots, but the raster arc's step
after the gesture is **release-profile** threshold pricing, and the 0140
successor cannot run on a profile whose tests do not build. It is a
prerequisite fix for the pricing record, not for the gesture, and it explains
why no release-profile hover number appears in §6.

## 10. Selection and grid consumers (stages 3b and 4), for completeness

- No `SelectionIntent`, selection context, or predicate type exists anywhere in
  the crate.
- `PlotHit` (`common.rs:1856`) is the structured hit 0133 §2 asked for:
  `mark_family`, `dimensionality`, typed `fields`, `facet`, `paint_order`,
  `sibling_ordinal`. A brush is a rectangle over `pixel_to_cartesian` corners
  producing typed constraints from the same field vocabulary.
- `ArtifactToken` is minted on every publication path (stage 1) and now also
  rides in every raster subject (0149), so the token discipline selection needs
  is already exercised by two consumers.
- Table selection (`gui/table.rs:494-700`) is unchanged since the 0133 census.
- Gesture arbitration must be decided once, for both slices: Ctrl-click and
  Ctrl-double-click are taken by the settings gesture; a bare left-drag is the
  natural pan; a brush will need either a modifier or a mode. Deciding the
  brush modifier in the gesture record, even before brush exists, prevents the
  pan from occupying a chord the brush later needs.

## 11. What the viewport-gesture record has to decide

1. Vocabulary: left-drag pan, wheel zoom anchored at cursor, double-click
   reset, and the chord reserved for brush.
2. Per-layout-family scope: cartesian both axes; categorical the continuous
   axis only or refuse; parallel and radial refuse.
3. Settle: release for drag; quiet-interval generation-stamped task for wheel,
   interval chosen on evidence.
4. Ownership: committed viewport in the host state's pane tier with a
   `render_revision` bump; in-progress drag in `PlotCanvasState`; notebook
   surface gains a model for parity.
5. Reset on kernel replacement, stated; no durability, stated.
6. Hit-testing suppressed during an active gesture.
7. Inspector provenance "pane" accepted as the disclosed promote path, or
   refused until a promote record.
8. Export renders the on-screen viewport, stated.
9. Facet cells stay `Settled` under motion (0139 §7 exclusion kept); the
   multi-cell preview observation is a named deliverable, not a decision.

## 12. Baselines the record's stage 0 must bank

- The hover hit-test cost above, as a kept `#[ignore]` exhibit, debug and
  release.
- A synthetic drag against a 7M-point rasterized scatter proving zero
  `build_raster_rgba` calls and zero hit-tests during `Moving` frames and
  exactly one coordinator publication after release (the 0149 gate 12 shape,
  driven by real events through `Program::update`).
- Rune source bytes before and after a full pan/zoom/reset sequence, identical.
- The release-profile test build status.
