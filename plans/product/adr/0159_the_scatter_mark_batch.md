# ADR 0159: the scatter mark batch

Status: proposed 2026-09-05. Plotting arc. On 2026-09-04 a 1,157,625-point
categorical scatter held the desktop's UI thread for minutes in a debug
build, and by PRODUCT-0140's own release pricing for more than a second per
frame in release. The evidence in `plans/product/impl/0159_stall_evidence.md`
attributes every sampled cycle to lyon tessellating the one compound fill
path that the scatter's downsampled arm emits per series, tens of thousands
of overlapping circles whose pairwise intersections the non-zero fill must
resolve. This record decides how scatter marks reach a backend so that cost
is linear in marks, what overlapping translucent marks mean, and what stays
byte-identical. It changes no gate constant, no downsample budget, no
preparation, and no other kernel.

## Context

### What the arms emit

`plot_vector` and `plot_downsampled` (`plot/kernels/scatter.rs`) build one
`fill_path` per series whose closure draws every mark as a circle subpath.
The iced backend hands that path to lyon's fill tessellator with the
non-zero rule; the SVG backend writes it as one `<path>` with a
`fill-opacity`. Every other circle-drawing kernel — bubble, box-plot
outliers, violin medians — fills one circle per call.

### Why one path is quadratic here

The downsampler keeps one point per grid cell under a 200,000-cell budget
(`scatter_downsample_target`), which at a 780×205 plot area is a 1.55 px
cell under a 6 px default mark. Nearly every retained circle overlaps a
dozen neighbours. Lyon must intersect every pair to fill one path
correctly, and its event queue inserts by linear scan, so the work grows
superlinearly with overlap. The measurement in the evidence note isolates
it: the same 66,817 marks cost 88.9 s in one path and 0.156 s as separate
fills in the dev profile; shrunk to non-overlap they cost 3.2 s in one
path, so overlap, not count, is the driver. The vector arm has the same
shape below 50,000 points and the same failure at high overlap.

### What the batch means semantically

One non-zero path is a union: overlapping translucent marks show one
coverage. Separate fills composite per mark: overlaps darken. Every
plotting library composites per mark; the union is an artefact of
batching, never authored. Colours carry an alpha byte
(`plot/spec/mod.rs`), so the difference is representable and must be
decided, not assumed away.

### What is held elsewhere

PRODUCT-0140 §2 retains `scatter_raster_threshold` and the `× 8` multiplier
unchanged and halts any implementation that moves them; its §4 names the
session policy that may. PRODUCT-0139 holds `scatter_max_vector_points`
and `scatter_downsample_target` as a coupled pair for their own record.
Fidelity, saturation, and the gate are those records'. This record changes
none of them.

## Decision

### 1. Marks are a backend batch, not a path

`PlotBackend` gains `fill_marks(centres: &[Point], radius: f32, color:
Color)`: a batch of identical circles filled with one colour, composited
per mark. Its default implementation calls `fill_path` once per centre, so
every recorder and test backend sees one operation per mark and nothing
else changes for them. Scatter's vector and downsampled arms emit their
series through it and never build a compound circle path again. No other
kernel changes in this cut; bubble, box, and violin already draw one
circle per call.

### 2. The iced backend fills one mark at a time

The iced backend implements the batch as one lyon fill per mark, each a
single circle, so no path ever carries two marks and lyon never processes
an intersection between them. This is the route the evidence measured:
66,817 marks in 0.156 s against 88.9 s, dev profile, on the same
tessellator. Every fill lands in the same solid buffer, so the frame still
uploads one mesh. Instancing one tessellated circle into a mesh is
possible — `iced_wgpu::Geometry::Live` exposes its mesh list and the wgpu
renderer draws solid meshes — but the fallback renderer of the reduced head
would need its own arm and nothing has measured it; it is a later
optimization only if a measured need appears, not this cut.

### 3. The SVG backend keeps its bytes for opaque marks

For an opaque colour the union and the per-mark composite are the same
pixels, so the SVG backend emits an opaque batch exactly as today: one
`<path>` of circle subpaths with the series fill. Every existing scatter
SVG, PNG, package, and golden stays byte-identical. For a translucent
colour it emits one `<defs>` circle and one `<use>` per centre with the
batch's fill and opacity, so the SVG and its resvg rasterization composite
per mark, the same as the desktop. PRODUCT-0057's oracle licence holds:
the SVG proves the desktop's compositing in both cases.

### 4. Overlapping translucent marks composite per mark, everywhere

This is a visible behaviour change and this record makes it: an authored
translucent scatter colour now darkens where marks overlap, on the desktop,
in the SVG, and in the PNG alike. It was never authored the other way; the
union was the batching's side effect. A discriminating test pins it in
both directions.

### 5. Nothing about the gate or the budget moves

`scatter_render_mode` resolution, the raster threshold, the multiplier,
`scatter_max_vector_points`, and `scatter_downsample_target` are untouched.
The downsampler still retains ~66,000 overlapping marks per series at the
fixture's pane; it just draws them in linear time. Whether it should retain
fewer is the coupled-pair record's question, now separable from the stall.

### 6. What this record does not decide

The session policy of PRODUCT-0140 §4; the coupled downsample budget;
moving Rune-view kernel preparation off the UI thread, which is a separate
one-second defect with its own bare fix; any change to hit testing, the
raster arm, or the raster coordinator; and mark shapes other than circles.

## Acceptance gates

1. **Linear.** The tessellation probe's production case, 66,817 marks of
   radius 3 px on 1.55 px cells, runs under 0.5 s in the dev profile
   through the iced backend's batch, against 88.9 s before, and the vector
   arm's worst case, 50,000 marks at the same overlap, alike; a
   200,000-mark batch stays under 1.5 s. Counted as well as timed: the
   batch performs exactly one fill per mark and no compound path, asserted
   through the recording backend for both arms.
2. **Opaque output is unchanged where it can be, and bounded where it
   cannot.** For every semantic showcase scatter and the fixture scatter
   at three pane sizes, with opaque marks, the SVG bytes and the PNG
   export are byte-identical before and after the cut, which §3 gives by
   construction; translucent output intentionally changes per §4 and is
   gate 3's. Desktop pixels through the test renderer are asserted, not
   assumed, identical for every fixture whose opaque marks do not overlap,
   at scale factors 1.0 and 2.0, with a no-overlap fixture as the positive
   control. Where opaque marks overlap, separately tessellated circles may
   differ from the union at antialiased edges; the gate bounds that in
   physical pixels: at each scale factor every differing pixel lies within
   one physical pixel of a mark boundary, widened by lyon's flattening
   tolerance at that scale, no interior or background pixel differs, and
   the count and maximum per-channel delta are reported in the
   implementation notes.
3. **Translucent overlap composites per mark, on every backend.** A
   fixture of two overlapping marks at alpha 0.5 renders the overlap darker
   than either mark on the desktop and in the SVG through resvg, and the
   PNG export is the resvg rasterization of that SVG, so it agrees with the
   SVG pixel for pixel by construction. The desktop and resvg agree on the
   composite value at the overlap centre and at a single-mark centre within
   PRODUCT-0057's tolerance; pixel-for-pixel agreement between the two
   rasterizers is not licensed by PRODUCT-0057 and is not claimed. The
   pre-cut union rendering is the negative control and differs.
4. **Recorders see marks.** The recording backend receives one fill per
   mark for both arms; the PNG grouping and series-seam tests of
   PRODUCT-0156 and PRODUCT-0157 are unchanged, including `data-series`
   groups wrapping the batch.
5. **The desktop breathes, measured at the event loop.** The application
   gains an opt-in latency probe, `POLARITON_LOOP_LATENCY=1`: a 16 ms tick
   subscription armed at application start, before any fixture work, whose
   handler measures each tick's lateness against its scheduled deadline —
   the first tick included, so a stall before the first tick is handled is
   seen — rather than the gap between handled ticks, and prints the maximum
   lateness and the count over 100 ms to stderr on each new maximum and on
   exit. It stays active through confirmed first paint of every pane. Opening `uat/plot_fixtures_showcase.rn` with the release
   binary at the operator's pane size through first paint: the maximum
   lateness is under 500 ms. That number is the gate, over the whole run
   from arming: if preparation alone exceeds it, the gate fails and says
   so; no phase is excluded. `/proc` sampling of the main
   thread is retained as supporting evidence, not a gate, because it can
   miss work between samples and a run state does not say events were
   serviced. Whether GNOME's not-responding dialog appears, in release and
   in debug, is the operator's observation, recorded beside the numbers.
6. **Nothing else moved.** An architecture guard asserts the gate
   constants, the multiplier, the downsample target, and
   `resolve_render_mode` are byte-identical to PRODUCT-0140's retained
   text, and that no kernel other than scatter calls the batch.
7. **Both heads** under the PRODUCT-0152 gate, and the full architecture
   suite green after formatting.

## Guardrails and stop conditions

1. If linear cost needs a change to the gate or the budget, stop; that is
   PRODUCT-0140 §7's halt and the coupled-pair record.
2. If the SVG and the desktop cannot be made to composite translucent
   overlap identically, stop and report; PRODUCT-0057's licence does not
   bend.
3. If an opaque showcase changes by a pixel, stop: the instancing has
   drifted from lyon's circle, and the segment rule is wrong.
4. Recheck the record number before the plan commit; two commits, plan
   then one `impl:` cut, corrections amended under review, no push until
   the cut is called stable.

## Risks

- **Separately filled circles differ from the union at antialiased
  overlapping edges.** Not hidden: gate 2 bounds the difference to edge
  pixels and reports its magnitude, and PRODUCT-0057 already licenses the
  SVG oracle as unable to prove antialiasing.
- **Per-mark fills cost more than instancing at the largest batches.**
  Measured at 0.156 s for 66,817 marks in dev; gate 1 bounds 200,000. If a
  measured need appears, §2 names the mesh route and its unmeasured half.
- **A translucent scatter someone relied on as a union.** Named as a
  behaviour change in §4 and pinned in both directions by gate 3; the
  union was never authorable.
- **The batch tempts other kernels.** Gate 6 confines it to scatter in this
  cut; bubble's per-mark strokes and varying radii are a different batch.

## Forward

With the arm linear, the coupled-pair record can price fidelity honestly:
whether 66,000 overlapping marks in a 780×205 area show anything a 4,550
mark grid at the mark's own diameter does not, which the evidence note's
last row makes a measurable question. The Rune-view preparation fix is a
bare cut. PRODUCT-0158's driver would have opened this fixture without a
pointer and reported the stall from `/proc` in the receipt.
