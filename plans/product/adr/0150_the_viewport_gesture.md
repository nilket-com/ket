# ADR 0150: the viewport gesture

Status: accepted 2026-09-02, revised twice the same day: once after review
found four contract defects (composite motion and facet reuse, settle
identity, the promote path, anchor geometry) and two sharpenings (the scope
of the no-scan guarantee, the gesture's active rectangle and constants),
and again for four residues (the reflow-drift formula, the settle state
machine across both lifetimes, per-cell hexbin settle cost, D7's exact
behaviour under a categorical gesture). Amended at implementation to bind
the canvas-local drag anchor to the surface lifetime, make hover clearing
part of the viewport-start transition, and revalidate pane provenance at
the writeback consumer. The implementation audit also reconciled gate 17
with Stage 0: the explicitly required kept hover-cost exhibit is the one
new ignored test; no other ignored or tolerated failure is admitted. A
post-landing review found that iced dispatches one frame's event batch against
one widget tree before rebuilding it; the canvas therefore chains consecutive
gesture generations and wheel geometry locally rather than assuming a rebuild
between input events. This record is the production activator that PRODUCT-0139
§7, PRODUCT-0140 §4, and PRODUCT-0149 each name
and none of them contains: the first caller of
`ViewportMotion::Moving`, the first production author of the pane-override
domain tier, and the gesture whose settle frame the off-thread substrate
was built to receive. It is PRODUCT-0133 stage 3's viewport half, split
from the selection half on the census finding
(`plans/product/impl/0133_gesture_census.md`) that the two share no
dependency and differ in size by a wide margin. It decides the gesture
vocabulary, its active rectangle, per-layout scope, anchor geometry,
settle identity, ownership, arbitration, reset, hover suppression, the
composite motion seam, the inspector's promote path, and export under a
viewport. It does not decide selection, contexts, brushes, large-scatter
hit-testing, facet raster reuse, thresholds, or any Rune spelling.

## Context

### Every piece of the machinery is dormant

`ViewportMotion` (`plot/common.rs:2161`) has test callers only.
`pane_override.domains` (`plot/theme.rs:180`, `:240`) is applied last by
both resolutions and read by every cartesian kernel, and nothing in
production writes it. The 0149 request seam publishes raster wants only
while settled (`common.rs:3393`), so a moving frame already requests nothing
and draws a placed preview — the composition of 0139 §5 and 0149 §9 is
waiting for a frame that says `Moving`. Hexbin's `prepare_viewport` marks a
stale preview under motion (`kernels/hexbin.rs:151`). The gesture is the
missing input, not missing machinery.

### The composite does not carry motion, and that is now a defect

0139 §7 withheld the affine motion preview from facet cells "by
construction": the composite calls `plot_with_settings`, so every cell is
`Settled` whatever the frame was told, and a gate asserts it. Two things
have changed since that sentence was written.

First, 0149's interactive draw path (`scatter.rs:580`) places **any**
affine-compatible last-good bitmap without consulting motion or in-flight
state. The composite's interactive branch (`grid.rs:903`) reaches it per
cell. So a facet cell under a drag would already receive a placed preview;
the by-construction exclusion no longer excludes anything on the
interactive path, and this record would activate facet reuse before the
judgement 0139 §7 reserves.

Second, the composite calls each cell's `prepare_viewport` with `Settled`.
For a faceted hexbin that means a **synchronous re-bin on every moving
frame**, which is exactly the per-frame aggregation the `Moving` arm of
`prepare_viewport` exists to prevent. The lone hexbin is protected; the
faceted one is not.

Motion must therefore reach the composite: to `prepare_viewport`, so
aggregation honours it, and to the cell draw, so reuse can be refused
where it is not yet admitted. §9 is that seam.

### The event surface is three arms wide

`PlotWidget::update` answers `RedrawRequested` (raster wants, on the data
canvas), `ModifiersChanged` plus `ButtonPressed` (the Ctrl-double-click
settings gesture, on the overlay canvas), and `CursorMoved` (hover). There
is no `ButtonReleased`, no `WheelScrolled`, and no drag state. The host
panel is `stack![data, interaction]`; iced delivers events top-down and
stops at the first capture, so a gesture captured on the overlay never
reaches the data canvas and the redraw seam is undisturbed. The notebook
surface is one `Full` canvas carrying both roles.

### Hover is already a stall, and a drag is a stream of hovers

`hit_point_with_radius` scans every point of every series on the UI thread
on each `CursorMoved`: 360 ms at one million points and 2,490 ms at seven
million, in debug. Wiring pan without addressing this would make each
moving frame pay a full scan for a hover nobody can read while dragging.
The hover stall itself is a separate record (§10); the gesture's obligation
is to not multiply it.

### Settle has no event for the wheel, and lifetimes collide

A drag ends at `ButtonReleased`. A wheel zoom has no terminal event, and
0139 §5's "exactly one true rasterization at settle" needs one. A
quiet-interval timer is the shape, and a timer is a message that arrives
later from a sender that may no longer exist: notebook republication
replaces `NotebookPlotSurface` under the same cell identity
(`editor.rs:1251`), so a timer minted by the old surface can arrive at the
new one. Per-surface counters restart with the surface. The 0149 lesson —
lifetime identity, not value identity, rejects the stale arrival — applies
verbatim.

### The plot rectangle moves when the domain moves

The inner plot rectangle is not fixed: PRODUCT-0114 derives the axis bands
from the formatted tick labels of the layout's own domain, so a domain
change that widens a y-tick label shifts the plot area's left edge and
width. Any anchor arithmetic that assumes the rectangle is invariant across
the gesture is wrong at exactly the transitions a user will notice.

### The inspector shows the pane tier but cannot yet promote it

`XAxisBounds` and `YAxisBounds` are registered settings whose effective
value projects from the resolved bounds, whose provenance is `PaneOverride`
when the pane layer authors them, and whose row is editable with the
effective value displayed and the provenance badge beside it
(`gui/plot_pane/view.rs:129-160`). But the displayed value is **not** a
draft: `SettingSubmitted` carries only the key, commit looks up the drafts
map (`gui/state.rs:1331`), and an untouched row refuses with "enter a value
before committing" (`gui/state.rs:1396`). Promoting a viewport today means
retyping it. Writeback itself never reads `pane_override`; a gesture alone
cannot reach Rune.

### The notebook has no pane tier

`NotebookPlotSurface` (`editor.rs:546`) holds a `ResolvedPlotSettings` and
rebuilds a model at view time. There is nothing to write a viewport into.
0149 established host/notebook parity as a gated property; this record
inherits that standard.

## Decision

### 1. The vocabulary, and where it is active

- **Left-drag pans.** Press, move, release. The domain follows the cursor
  so that the data point under the press stays under the cursor (§3).
- **Wheel zooms, anchored at the cursor.** Each wheel step scales both
  continuous extents by a fixed multiplicative factor about the data point
  under the cursor. Line and pixel deltas are normalised the way
  `gui/table.rs:612` normalises them; the factor is an implementation
  constant tunable on evidence, not a policy.
- **Double-click resets.** A bare double-click clears the pane domain tier
  (`Override::Inherit`) and settles. Ctrl-double-click remains the settings
  gesture and is untouched.
- **Shift-drag is reserved for the brush** and does nothing in this
  record, so the pan never occupies the chord selection needs.
- A press with Ctrl held never starts a pan; Ctrl chords stay with the
  settings gesture.

**A gesture is active only inside the inner plot rectangle** — the area
the coordinate transform maps, after title, legend, and axis bands. A press
or wheel step over a title, legend, tick band, or facet gutter captures
nothing and publishes nothing. In a facet grid the rectangle is the cell's
inner area under the cursor.

### 2. Scope per layout family, and the domain floor

| layout | pan | zoom |
| --- | --- | --- |
| `Cartesian` | both axes | both axes |
| `CategoricalX` | y only | y only |
| `CategoricalY` | x only | x only |
| `CategoricalXY`, `Parallel`, `Radial` | refused | refused |

A refused gesture captures nothing and publishes nothing. A category axis
has no `x_min`.

**D7 under a categorical gesture, stated exactly.** A rasterized
`CategoricalX` scatter under a y-only pan *does* enter the reuse path: its
raster key carries `(0, 0)` x-range bits (`current_x_range_bits`), the
y-only change leaves every non-range field equal, so `reusable_for` says
yes and `reuse_placement` is consulted. It then **fails closed** on the
degenerate x extent and answers no placement. The observable behaviour is
therefore a preparing marker during motion and exact pixels after settle —
correct, and slower than a cartesian pan, because no preview is available.
This record keeps that behaviour and gates it; repairing the key to carry
the axis family and a real categorical extent stays with the 0140
successor, where the census already parked it.

Every continuous axis is affine in `f64` — `Linear`, `Percent`, `Date`,
`Datetime` — so pan and zoom are domain arithmetic with no scale-space
special case.

**The domain floor.** A zoom step refuses, leaving the domain unchanged,
when either resulting extent would be non-finite or smaller than the
larger of two floors: a **relative** floor of `2⁻³⁰` of the prepared extent
(about thirty halvings — three hundred wheel steps at a 1.1 factor — before
a scatter's whole range is one pixel wide at any plausible viewport), and
an **absolute** floor of `2¹⁰` ulps at the magnitude of the domain's
endpoints, below which `hi - lo` loses the precision the raster key and the
transform divide by. Both are derivations from representable arithmetic,
not tuning knobs; the evidence gate records the values in force.

### 3. Anchor geometry: the press-time transform is the reference

The **in-progress drag** stores, at press, the pixel position of the press,
the domain at press, and the **plot rectangle at press** — together the
press-time `CoordinateTransformer`. Every subsequent move computes the new
domain as the press-time domain shifted by the cursor displacement
converted at the press-time data-per-pixel scale. The reference never moves
during the drag, so a tick-label reflow mid-drag changes the rectangle the
frame is drawn into but not the arithmetic that positions it; the drag is
repeatable and does not feed back on itself.

**Zoom anchors against the pre-reflow rectangle**, and the record says so
rather than promising more. The anchor data point is resolved through the
current transform, the extents are scaled about it, and the domain is
written; if the resulting layout's bands differ from the current ones, the
plot rectangle changes and the anchored point is displaced. The
displacement is the affine rectangle-reflow term, **per axis**, with `u`
the cursor's normalised position within the old rectangle on that axis:

```text
drift = (new_origin − old_origin) + u × (new_size − old_size)
```

It is zero when the bands do not change and is not, in general, the band
delta alone — a wider left band shifts the origin *and* shrinks the size,
and the two terms weigh differently across the rectangle. A fixed-point
solve against the post-reflow rectangle is a well-defined stronger promise
and is **rejected** here (see Rejected): it couples the gesture to band
derivation for a drift of a few pixels that occurs only at label-width
transitions. The gate measures the drift and pins it to the formula, so a
future record can strengthen the promise on evidence.

### 4. Ownership: committed viewport in the model, in-progress drag in the canvas

The **committed viewport** is `plot_settings_model.pane_override.domains`
on the surface's state — `HostPlotState` and the notebook surface alike.
Every mutation goes through the surface's `update` as a `PlotPaneMsg`,
re-resolves `plot_settings`, and bumps `render_revision`, exactly as
`RasterCompleted` does. 0139 §6's warning is honoured by construction:
without the bump the canvas cache serves the old geometry and the change is
invisible.

The **in-progress drag** of §3 lives in `PlotCanvasState`, beside
`settings_click`, and is cleared on release. **Implementation finding:**
iced can retain that canvas state across a host kernel replacement or a
notebook republication. The drag anchor therefore carries the surface stamp
that accepted its press; each move advances that stamp with the published
generation, and a move or release against another stamp clears the anchor
and publishes nothing. A predecessor's press-time transform can never reach
a replacement plot.

**One event batch may outlive the widget stamp.** iced sends every event
collected for a frame through one widget tree and rebuilds only after the
batch. The canvas therefore keeps a local continuation seeded from the widget
stamp. A press, every move, release consumption, and every wheel step advance
that continuation; moves mint from the drag's own latest stamp, and wheel
steps also carry forward the locally zoomed geometry so none is lost. A rebuilt
widget with a newer generation, or any different incarnation, replaces the
continuation and clears a predecessor drag. A widget behind the continuation
is the ordinary same-batch case, not replacement.

The **motion flag** is a `viewport_motion` field on the surface state that
the view forwards to every `PlotWidget` it builds, so the data canvas and
the overlay canvas see the same frame the same way.

The **notebook surface gains a `PlotSettings` model**, built once from its
resolved settings at construction, so it owns a pane tier and the same
lifecycle; the view stops rebuilding a model per frame.

### 5. Per-move messages, and what a moving frame does not do

Each `CursorMoved` during a drag, and each wheel step, publishes one
viewport message carrying the new domains and `Moving`. That message
rebuilds the view, which is what a domain change requires — the layout,
ticks, gridlines, and the placed preview all derive from the model. This is
a message **per input event**, not per redraw; the 0149 lesson was about
the latter. Hover already works this way.

The guarantee a moving frame carries is scoped exactly:

- **no raster construction** — the data canvas requests nothing while
  `Moving` (0149 seam) and the scatter `Rasterized` arm draws a placed
  bitmap or the preparing marker;
- **no hit-testing** — §7;
- **no viewport-dependent preparation** — `prepare_viewport` receives
  `Moving` on every path, composite included (§9), so hexbin draws its
  stale preview instead of re-binning.

Vector and downsampled scatter marks are **unchanged**: the vector arm
re-projects every point per frame and the downsampled arm rebuilds its
sample per frame, as both do today on every settled frame. Their cost is
the existing draw cost of those modes, bounded by 0140's unchanged gate,
and this record neither adds to it nor claims to remove it.

### 6. Settle is one event, identified by incarnation and generation

Every surface carries a **surface incarnation** — a process-wide,
never-reused id minted at construction — and a **viewport generation** that
advances on **every accepted gesture event**: press, each move, each wheel
step, double-click, and a *matching* settle, which consumes the generation
it matched and advances past it. A **stale settle advances nothing**; it is
dropped before it can touch the counter. Advancing on press rather than on
the first mutation is the point: a press during a pending wheel settle
changes nothing in the domain but must still invalidate the timer, or the
timer fires before the first move and settles the active drag. The press
also sets `Moving`.

**Kernel replacement is a gesture event too.** The host panel's
`HostPlotState` is *not* reconstructed on replacement — `set_plot` and the
preview/promote lifecycle swap the kernel and rebuild the model inside the
same state, so its incarnation survives. Replacement therefore advances the
generation, sets `Settled`, and by that advance invalidates any pending
wheel timer. The notebook surface *is* reconstructed on republication, and
its incarnation changes; both are the same rule seen from two lifetimes.

**A wheel step during a drag is refused.** It publishes no viewport
mutation, advances nothing, and spawns no timer; the drag's release is the
only settle in flight.

- **Drag** settles on `ButtonReleased`: the overlay publishes a settle
  message carrying the incarnation and the generation it observed.
- **Wheel** settles by a quiet interval: each wheel step spawns a task that
  sleeps the interval and then publishes a settle message stamped with the
  incarnation and generation at spawn.
- A settle message whose incarnation or generation is not the surface's
  current pair **does nothing** — no motion change, no revision bump. A
  late wheel timer after a newer wheel step, after a press, or from a
  surface that republication has since replaced, is dropped by that rule
  alone.
- A matching settle sets `viewport_motion = Settled` and bumps
  `render_revision`. The next `RedrawRequested` finds the want novel and the
  0149 coordinator mints exactly one request. 0139 §5 holds end to end.

**The quiet interval's initial value is 150 ms**, derived rather than
picked: a notched wheel delivers steps 50–100 ms apart and inertial
trackpad scrolling delivers them at frame cadence, so the interval must
exceed the largest gap inside one continuous gesture (~100 ms) and sit
well under the threshold at which a pause reads as "stopped" (~300 ms).
The evidence gate banks the value in force and the cadence it was tuned
against.

### 7. Hover is suppressed while a gesture is active

From press to release, and from a wheel step to its settle, the overlay
performs **no hit-test** and publishes no hover. At gesture start the hover
bubble is cleared, so a stale reading does not ride along with the pan.
**Implementation finding:** a canvas action publishes one message, so
`ViewportStarted` and `ViewportChanged` clear `hovered_info` in the surface
update instead of attempting a second `UpdateHover(None)` publication. The
observable state transition is the same. Hover resumes on the first
`CursorMoved` after settle. This is gesture arbitration, not a hover policy.

### 8. Pan and zoom never reach Rune; the inspector promotes a displayed pane value

A gesture writes the pane domain tier and nothing else. It never pushes a
`PlotWritebackGesture`, never consults `plan_writeback`, never mints an
authored revision, never changes the source buffer. This is 0133 §9 stated
as a gate.

**The promote path is the inspector's bounds row, and this record has to
finish it.** Today the row shows the viewport with the `pane` badge but
submit refuses without a typed draft. This record adds one scoped
obligation to the 0142 seam: **when a bounds row's provenance is
`PaneOverride` and no draft exists, `SettingSubmitted` commits the
displayed effective value.** Nothing else about writeback changes — the
value flows through `plan_writeback` and the compile-candidate gate like
any typed draft, and rows of every other provenance keep refusing an
untouched submit. The path is explicit (Enter on a row the user is looking
at), user-initiated, and disclosed before the submit by the badge this
record gates. No new command is added; the earlier claim that no new code
was needed was wrong, and this is the code. The consumer revalidates both
the current row's `PaneOverride` provenance and the displayed value before
using the fallback; a delayed submit from an older view cannot promote after
the pane tier or its value has changed.

### 9. The composite motion seam: aggregation honours motion, facet reuse is refused

The composite gains motion on its interactive path and forwards it twice:

- to every cell's `prepare_viewport`, so each cell of a faceted hexbin
  under `Moving` marks its own stale preview exactly as the lone hexbin
  does. Each cell owns its own kernel, lattice, and artifact, so settle
  costs **one re-bin per affected cell**, not one per surface;
- to every cell's interactive draw, together with a **reuse policy**.

The interactive scatter draw takes that policy. For a lone scatter it is
`Placed`: the 0139 preview under motion and the 0149 in-flight preview,
unchanged. For a **facet cell it is `Refused`**: a slot draws its bitmap
only when the bitmap's key equals the current key exactly; otherwise it
contributes no marks and the surface shows the preparing marker. This
withholds the affine preview from facets on the interactive path the way
0139 §7 believed the exact path already did, and it withdraws the in-flight
facet preview that 0149 §7 extended without saying so — that extension is
retracted here with attribution, because 0139 §7's judgement is the older
and explicit one.

Facet cells still move together on their shared domain (the 0139 facet
audit), and the zoom anchor is the data point under the cursor in the cell
under the cursor, resolved through the composite's cell-geometry seam.

**Admitting facet reuse is a 0139 amendment, not a flip of this policy.**
The observation 0139 §7 asked for is made on a local build with the policy
set to `Placed` for facets; if the multi-cell preview is judged legible,
an attributed amendment to 0139 §7 records the judgement and changes the
policy. Until then `Refused` is gated.

### 10. Reset on replacement, no durability, export what is shown

- **Kernel replacement resets the viewport.** `set_plot` and the
  preview/promote lifecycle already rebuild the model from
  `host_plot_settings_model`, dropping the pane tier; a rerun starts at the
  authored domain. Stated and gated rather than inherited silently.
- **The viewport is not durable.** `session_persistence.rs` stores panel
  identity only. 0133 §9's "session-scoped and ephemeral" is the existing
  behaviour and this record keeps it.
- **Export renders the on-screen viewport.** `host_plot_exports` snapshots
  the model, pane tier included. 0133 §10 forbids export a policy of its
  own, and stripping the pane tier on export would be exactly that.

### 11. What this record does not do

No selection, brush, or context — stage 3b. No change to hover's own cost:
large-scatter hit-testing is its own record, with the 0149 identity shape.
No facet raster reuse — refused here, admitted only by a 0139 amendment. No
change to `resolve_render_mode`, either 0140 constant, or the ×8 factor. No
Rune spelling for a viewport, no promote command beyond the inspector row.
No post-reflow anchor solve. No keyboard pan, no Escape-to-cancel, no
minimap change. No durable viewport.

## Implementation sequence

**Stage 0 — bank the baselines.** The hover hit-test cost as a kept
`#[ignore]` exhibit at one and seven million points; the source buffer's
bytes before a gesture sequence; the current `Program::update` refusing
`ButtonReleased` and `WheelScrolled`; a faceted hexbin re-binning per
settled frame and a facet scatter cell placing an affine preview on the
interactive path (the two composite controls); an untouched pane-provenance
row refusing submit; the census's release-profile build status recorded,
not repaired.

**Stage 1 — ownership, identity, and the notebook model.** Surface
incarnation, `viewport_motion`, and the viewport generation on both
surface states; the notebook surface's `PlotSettings` model; the viewport
and settle messages through both `update` paths with re-resolve and
`render_revision` bump; parity gated.

**Stage 2 — the composite motion seam.** Motion and the reuse policy
forwarded through the composite's interactive branch to `prepare_viewport`
and the cell draw; `Refused` in facet cells; the 0139 §7 guard extended to
require forwarding in both cell branches; the two stage-0 controls go red.

**Stage 3 — the gesture.** Press/move/release pan against the press-time
transform, wheel zoom against the pre-reflow rectangle, the active
rectangle, per-layout scope, the domain floor, drag state in
`PlotCanvasState`, hover suppression, Ctrl and Shift arbitration,
double-click reset.

**Stage 4 — settle.** Release settle; the quiet task for wheel; the
incarnation-plus-generation drop rule; the replacement race; the composed
0139/0149 settle gate driven through real events.

**Stage 5 — promote, export, evidence.** The pane-provenance submit
obligation on the 0142 seam; the `pane` badge gate; export-what-is-shown;
the synthetic drag exhibit against a seven-million-point rasterized
scatter; the constants in force banked; full battery.

## Acceptance gates

1. **The gesture is the first `Moving` caller.** After stage 3, a
   production path constructs `PlotWidget` with `Moving`; the census control
   goes red.
2. **A moving frame does the three things it may not.** A synthetic
   left-drag driven through `Program::update` with real `ButtonPressed`,
   `CursorMoved`, and `ButtonReleased` events over a seven-million-point
   rasterized scatter performs zero `build_raster_rgba` calls, zero
   hit-tests, and zero viewport preparations during moving frames, draws a
   placed preview each frame, and after release the coordinator mints
   exactly one request and publishes exactly once. Vector and downsampled
   fixtures under the same drag draw as they do today, asserted against
   the settled baseline rather than against zero.
3. **Wheel settles once, and only for its own lifetime.** N wheel steps
   inside the quiet interval produce one settle, one request, one
   publication; a settle carrying a stale generation changes no state
   **and advances no generation**; a press during a pending wheel settle
   advances the generation, sets `Moving`, and the wheel's late settle is
   dropped before the first move; a wheel step during a drag publishes no
   mutation and spawns no timer;
   **replacement race, both lifetimes:** a notebook surface replaced under
   the same cell identity drops a settle minted by its predecessor even
   when the generation value coincides, and a host panel whose kernel is
   replaced in place advances its generation, reads `Settled`, and drops a
   wheel settle spawned before the replacement. One unchanged widget fed
   press, two moves, and release publishes the full consecutive chain and its
   settle; two wheel steps in the same event batch have distinct generations
   and cumulative zoom.
4. **Rune is untouched.** Source buffer bytes identical after
   pan/zoom/reset; no `PlotWritebackGesture` pushed; `plan_writeback` never
   called; the pane tier is the only model tier that changed.
5. **The inspector discloses, then promotes the displayed value.** After a
   gesture the bounds rows show the viewport with `pane` rendered in the
   badge; `SettingSubmitted` on that row with no draft commits the
   displayed value through the existing 0142 path and the row's provenance
   becomes `AuthoredSpec`; the same untouched submit on a row of any other
   provenance still refuses with the existing message.
6. **Per-layout scope holds.** Cartesian both axes; `CategoricalX` y-only
   and `CategoricalY` x-only leave the category axis untouched;
   `CategoricalXY`, `Parallel`, and `Radial` capture and publish nothing;
   a rasterized `CategoricalX` scatter under a y-only drag shows the
   preparing marker on every moving frame (reuse fails closed on the
   degenerate x extent) and exact pixels after settle.
7. **The active rectangle is the inner plot area.** A press or wheel step
   over the title, legend, an axis band, or a facet gutter captures and
   publishes nothing; the same event one pixel inside the plot area starts
   the gesture.
8. **Drag is repeatable across reflow.** A drag whose path crosses a domain
   at which a y-tick label widens produces the same final domain as the
   same pixel path against a fixture whose labels never change width,
   because the press-time transform is the reference throughout.
9. **Zoom is anchored pre-reflow and floored.** With no band change the
   data point under the cursor is invariant across a step to tolerance;
   across a label-width transition its drift equals, per axis,
   `(new_origin − old_origin) + u × (new_size − old_size)` for the cursor's
   normalised position `u` in the old rectangle, to tolerance, and is
   banked; a step that would breach either floor refuses and leaves
   the domain unchanged; no non-finite domain is ever written; the floors
   and zoom factor in force are recorded with their derivations.
10. **Hover is suppressed and resumes.** No `UpdateHover` between press and
    settle; the bubble is cleared at gesture start; the first move after
    settle publishes a hover again.
11. **Arbitration.** Ctrl-press never starts a pan and the settings gesture
    still activates; Shift-drag does nothing; a bare double-click resets
    and does not open the inspector.
12. **Replacement resets; nothing persists.** `set_plot` and preview
    promotion clear the pane domains; the session checkpoint contains no
    viewport.
13. **Export what is shown.** An export taken under a zoomed viewport
    renders the zoomed domain in all three formats.
14. **Notebook parity.** The notebook surface owns a model, accepts the same
    messages, and passes gates 2–4, 10, and the replacement half of 3
    through its `Full` canvas.
15. **The composite carries motion and refuses facet reuse.** A drag over a
    two-cell facet changes one shared domain; the zoom anchor resolves in
    the cell under the cursor; a two-cell faceted hexbin performs zero
    re-bins during moving frames and exactly one re-bin **per affected
    cell** at settle; a facet scatter cell places **no**
    affine preview under motion or in flight and draws only an exact-key
    bitmap or nothing, with one preparing marker on the surface; both
    stage-0 composite controls are red; the extended 0139 §7 guard requires
    forwarding in both cell branches.
16. **Threshold machinery untouched.** `resolve_render_mode`, both 0140
    constants, and the ×8 factor asserted byte-identical.
17. **Full battery.** Polariton unit, architecture, trybuild, GUI, and
    workspace checks pass. The Stage-0 hover-cost exhibit is the one planned
    new `#[ignore]`; there is no other new ignored or tolerated failure.

## Guardrails and stop conditions

- If a moving frame is found to construct a raster, hit-test, or run a
  viewport-dependent preparation on any path, composite included, that is
  a defect in this record's premise; do not accommodate it.
- If the settle cannot be made exactly-once through the incarnation and
  generation rule without a second timer or an abort handle, halt and
  redesign; do not reach for `JoinHandle::abort()`.
- If `pane_override.domains` cannot express a family's viewport without
  widening `PlotDomainOverrides`, halt and negotiate the shape.
- If the pane-provenance submit cannot be scoped to that provenance
  without loosening the untouched-submit refusal elsewhere, halt; the
  refusal exists for a reason.
- If refusing facet reuse on the interactive path breaks a 0149 gate, the
  gate is re-derived with attribution, not the policy.
- The release-profile test build failure is recorded in stage 0 and **not
  repaired here**; it gates threshold pricing, not this record.
- Selection, brush, facet reuse, large-scatter hover, and the minimap each
  reopen their own boundary as a separate record.

## Rejected

**Drawing the in-progress drag from canvas state alone.** Draw could read
the anchor, but layout, ticks, grid, and the data canvas all derive from the
model, and the two canvases would disagree. Per-event messages are the cost
of one authority.

**Anchoring the drag against the current frame's transform.** The
rectangle moves when bands reflow; the arithmetic would feed back on
itself and the same pixel path would land at different domains.

**A post-reflow fixed-point anchor for zoom.** Well defined — resolve the
bands for the candidate domain, re-anchor, repeat — but it couples the
gesture to band derivation for a drift of a few pixels at label-width
transitions. Stated as a weaker promise and measured instead; a later
record may strengthen it on the banked drift.

**A synchronous hit-test under the drag.** Multiplies a measured stall by
the number of moves in a gesture for a reading nobody reads.

**Generation advancing on mutation only.** Leaves a press unable to
invalidate a pending wheel timer, which then settles an active drag.

**Per-surface generation as the whole settle identity.** Republication
recreates a surface under the same identity and restarts its counter; a
predecessor's timer can match. Incarnation is the lifetime half.

**Claiming the promote path exists.** It shows the value and refuses the
submit; a path that requires retyping the number on screen is not a
promote. Seeding the displayed value for pane provenance is the smallest
change that makes the claim true.

**A dedicated promote command.** A second path to the same Rune edit.

**Leaving the composite at `Settled`.** Faceted hexbin re-bins per moving
frame, and facet scatter reuses through the interactive path anyway.
Forwarding motion with an explicit reuse policy is the only shape under
which 0139 §7's exclusion is real.

**Admitting facet reuse in this record.** 0139 §7 reserved the judgement
for a person looking at the thing; this record makes the thing observable
and leaves the judgement with its owner.

**Stripping the pane tier on export.** A second export policy, rejected by
0133 §10 in advance.

**Bare-drag brush, pan on a modifier.** Pan is the exploratory default on
every plotting surface a user has met; the brush is the intentional act and
takes the chord.

**Escape to cancel a drag.** Cheap, but the table's Escape already means
"clear selection" and stage 3b decides that chord.

## Consequences

PRODUCT-0139's 7.9 ms motion frames and PRODUCT-0149's live settle become
something a person can drag. The composite finally carries motion, which
closes a per-frame aggregation hole in faceted hexbin that no record had
named, and makes 0139 §7's exclusion true on the interactive path for the
first time — at the price of retracting an in-flight facet preview 0149
extended without saying so. The multi-cell preview 0139 §7 deferred becomes
observable on a local build, so its judgement can be made against the
thing itself and recorded as an amendment. 0140 §4's "expected motion" term
acquires the real gestures its successor needs to price the threshold, on
a profile whose test build the census has shown must be repaired first.
The pane-override tier gains its first production author in the shape
PRODUCT-0007 §7 designed for it, and the inspector's bounds row becomes the
promote it was already dressed as. Selection lands next on a surface that
already arbitrates its chord.
