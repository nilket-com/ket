# ADR 0138: the scatter vector-rendering threshold

Status: **WITHDRAWN 2026-08-25, before acceptance. Do not cite.** Its central
outcome is not established, and one section is affirmatively wrong.

The draft claimed that lowering the threshold makes plots in the affected band
trade per-point fidelity for a re-render dropping ~255 ms to ~103 ms. Both
halves fail. `plot/kernels/scatter.rs:586` routes anything above the threshold
to `Downsampled`; `:287` sets `per_series_target = scatter_downsample_target /
series_count`, i.e. 200,000/N; and `downsample_series` allocates a
~target-cell grid and scans **every** point. For a single series at
20,001..=50,000 points against 200,000 cells, nearly every point keeps its own
cell — so little or no fidelity is lost, and the drawn point count is
essentially unchanged. The 102.8 ms figure was the **vector** cost at 20,000,
not the downsampled cost of a 20,001-point plot; the draft equated two code
paths.

It follows that the two constants are **coupled**: reaching ~103 ms at 50,000
points requires lowering `scatter_downsample_target`, not
`scatter_max_vector_points`. §6's claim that the downsample target "is not the
lever it appears to be" is therefore wrong in this band, where it may be the
only lever.

Sound and reusable in a replacement record: §3 (policy set against current
downsampling semantics, not freezing their implementation), §5 (the
interaction threshold as a contingent hypothesis, not a proposal), and the
Rejected section. The replacement must decide a **coupled**
vector-threshold/downsample-target policy rather than treating the threshold
as independently meaningful, must name the band strictly as `20_001..=50_000`,
and must not cite the evidence file until it is restored, corrected, and
committed.

Superseded status: proposed 2026-08-25. This record sets `scatter_max_vector_points`, the
cardinality above which a scatter mark stops drawing every point and begins
downsampling. PRODUCT-0007 gave it the value 50,000 without a benchmark behind
it; that value has now been measured and is too high.

It decides one number and the policy that justifies it: the acceptable trade
between per-point fidelity and re-render latency in the band the change moves.
It does not decide the downsample target, the raster threshold, the mechanism
by which downsampling selects points, any interaction-driven threshold, or
anything about time-to-first-plot. It adds no new authoring surface.

## Context

`scatter_max_vector_points = 50_000` has governed `ResolvedPlotSettings` since
PRODUCT-0007 as an unmeasured judgement. The comparison is `<=`
(`plot/kernels/scatter.rs`), so the threshold engages strictly above its value.

It is now measured. Nine cardinalities through the production path —
PRODUCT-0137's terminal `ket` route to materialize, reopen as
`ket.table(...).views([scatter("x","y")])`, transfer, then render at 1200×720
with `ResolvedPlotSettings::default()` — give the vector regime as linear:

| points | steady re-render | µs/point |
|---|---|---|
| 1,000 | 6.0 ms | 6.00 |
| 5,000 | 26.0 ms | 5.20 |
| 10,000 | 51.6 ms | 5.16 |
| 20,000 | 102.8 ms | 5.14 |
| 30,000 | 153.7 ms | 5.12 |
| 40,000 | 207.6 ms | 5.19 |
| 50,000 | 254.8 ms | 5.10 |

`render_ms ≈ 0.5 + 0.00509 × points`, holding across a 50× range, and the
50,000 point reproduced to within 2% across independent invocations.

**At the threshold, a steady re-render costs ~255 ms — roughly four frames per
second.** That is the value chosen as the *maximum* at which drawing every point
is still acceptable.

Three facts bound what this record may conclude.

**The measurement is static-render evidence only.** It draws marks once against
a fixed viewport. Nothing here measures a changing viewport, so these figures
cannot be repurposed as an interaction-frame budget.

**The figures are engine-versioned.** They were taken on one engine binary
during a period when the engine moved repeatedly. They price the rendering
choice; they are not a standing benchmark.

**Rendering is not the dominant term in getting a plot on screen.** Transfer
costs ~12.9 µs/point against rendering's ~5.09 — about 2.5× — so at 20,000
points the read is ~270 ms against ~103 ms of rendering.

## Decision

### 1. `scatter_max_vector_points` becomes 20,000

A measured point on the curve, not an extrapolation: 102.8 ms steady re-render,
against 254.8 ms at the present value.

### 2. The decision is a fidelity-versus-latency policy, not a benchmark result

What changes is confined to one band: **scatter marks between 20,000 and 50,000
points, which today draw every point, will downsample.** Those plots trade
per-point fidelity for a re-render dropping from ~255 ms to ~103 ms.

The curve prices that band. It does not choose within it. The judgement this
record makes is that in a plot large enough to reach 20,000 points, overplotting
already dominates the visual, and a re-render near the ~100 ms mark at which a
discrete interaction still reads as immediate is worth more than the points a
downsample removes. A reader who disagrees is disagreeing with that judgement,
not with the measurement.

### 3. The policy is set against current downsampling semantics and does not
### freeze their implementation

The threshold decides *what trade is acceptable*; the downsampling
implementation decides *how selection is achieved*. They are coupled only
through which points survive and the fidelity that results.

A later change removing the O(n) per-render selection scan **inherits this
decision unchanged if it preserves selection and fidelity**. If it changes
either, it must amend this record's threshold policy rather than inherit a
number chosen under different semantics.

Selection semantics are part of what a chart asserts about its data.
Performance is one constraint on how they are computed, and a faster
downsampler that changes which points survive is a different picture, not an
optimization.

### 4. This threshold governs re-render smoothness only

It has no material effect on time-to-first-plot, which transfer dominates. The
record states this so the change is not later credited with, or blamed for,
plot latency.

### 5. The interaction threshold is a separate, contingent decision

A continuously re-rendering surface — drag-to-zoom, brushing — would need a
frame budget this value does not meet. Inverting the static curve puts that
near 6,000 points (~33 ms), and **that figure is a hypothesis, not a
proposal.** It was derived from a curve measuring a different workload: a
changing viewport can move projection, culling, and axis work into the loop,
none of which this measurement captured.

It is to be measured against a real gesture execution path when ADR 0133 stages
3–4 provide one, and decided in its own record. It is not adopted here, and
this record must not be read as having pre-judged it.

### 6. `scatter_downsample_target` and the raster threshold are unchanged

The downsample target is not the lever it appears to be: it reduces the draw
but not an O(n) scan over every point, so no value of it makes a
million-point plot interactive. That is a design problem, held for its own
record.

The raster threshold has **never been exercised** — every run to date stopped
at its exact value, and the comparison is `<=`. It is not adjusted here on
absent evidence.

## Rejected

**6,000 now.** It would degrade every static plot between 6,000 and 20,000
points to buy a frame budget nothing in the product currently pays. Adopting
it before an interaction path exists would also silently convert a hypothesis
into policy.

**Keeping 50,000.** A ~255 ms re-render meets no interactive budget, and the
value was never chosen against evidence in the first place.

**Letting the curve choose.** "The benchmark picked 20,000" is not a decision.
The curve prices options across the band; someone must decide which cost is
acceptable, and this record does that explicitly so it can be argued with.

**Lowering `scatter_downsample_target` instead.** It does not bound render
cost, for the reason in §6.

## Consequences and sequencing

The change is one constant and its tests; there is no new surface, no
authoring change, and no migration.

Two records follow and neither gates this one. The raster measurement above
1,000,000 — which also discharges the heavy-churn materialize verification the
db team is waiting on — informs, but does not decide, the downsampling design.
The non-O(n) downsampling record follows it and is bound by §3.

The evidence lives in `plans/product/impl/0137_scatter_threshold_evidence.md`.
That document's projection tables carry an erroneous "README claim" of
10,000,000 rows; there is no such claim in the README, and it should be struck
before the file is treated as current.
