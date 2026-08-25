# Evidence slice — scatter thresholds, and what blocked measuring them

**Status:** measurement attempt, 2026-08-21. Not a record and not a plan. It
reports what one real workflow does end to end, and the two defects that stop
the intended measurement from being taken.

**Headline: the thresholds could not be measured.** Reading a materialized
table runs at roughly **460 rows per second**, and any query exceeding ~30
seconds dies on an unconfigured QUIC idle timeout before either configured
timeout can fire. The smallest threshold under test is 50,000 points, which
would need ~109 seconds of reading. The exercise is blocked on the engine,
not on the plotting layer.

## What was exercised

The fifth arc's whole chain, in one process, for the first time:

```rune
ket.range(N).project_named([
    seeded_random(lit_i64(20260821), col("ordinal")).named("x"),
    seeded_random(lit_i64(20260822), col("ordinal")).named("y"),
]).materialize("evidence_scatter_N")
```

routed **through the production compiler** (PRODUCT-0137's terminal `ket`
route), then reopened as `ket.table(...).views([scatter("x","y")])` and
rendered. The exhibit is `live_scatter_threshold_evidence` in
`crates/polariton/src/adapters/toron/mod.rs`, opt-in behind `--ignored`.

## Environment

| item | value |
|---|---|
| CPU | 12th Gen Intel Core i7-12700K, 16 threads |
| memory | 61 GiB (40 GiB free during runs) |
| kernel | Linux 7.0.0-29-generic |
| nilket binary | release, `607d653c…`, built from `4b5bc818` (ADR 0575) |
| deployment | `nilket local`, same data dir, dev-insecure KEK |
| renderer | `SvgBackend`, 1200×720, `ResolvedPlotSettings::default()` |

The binary was verified identical (same SHA-256) to the operator's
reinstalled `~/.cargo/bin/nilket`, so nothing here is a stale-build artifact.

## What works

Materialization is sound end to end. For N = 49,999:

- the terminal `ket` relation compiled with zero presentation slots;
- the receipt reported **49,999 rows written** and
  **certificate format version 1**;
- the data is correct — `SELECT x … LIMIT 1` returns `0.996570334516858`, a
  valid unit-interval `seeded_random` value;
- bounded reads return exactly the requested rows.

## Defect 1 — reads run at ~460 rows/second, linearly

| rows | wall time | rate |
|---|---|---|
| 2,000 | 4.39 s | 456 rows/s |
| 5,000 | 10.76 s | 465 rows/s |
| 13,000 | 28.19 s | 461 rows/s |

Constant ~2.2 ms per row for two `Float64` columns. No cliff, no quadratic
term — a flat per-row cost. Extrapolated against the thresholds under test:

| threshold | projected read time |
|---|---|
| 50,000 | ~1.8 min |
| 200,000 | ~7.2 min |
| 1,000,000 | ~36 min |

**Not established:** whether this is specific to materialized tables. The
deployment had no comparable non-materialized table (`demo_sales` is absent),
and the obvious control — `CREATE TABLE … AS SELECT … LIMIT 14000` — hits
defect 2 before it can produce one. Answering this is the first diagnostic
step, and it decides whether the fault is in the materialization write path's
resulting layout or in the scan path generally.

## Defect 2 — the QUIC idle timeout preempts every configured timeout

Every query over the boundary fails at **exactly 30.01 s** with
`Toron transport: connection lost`. The daemon does **not** crash: toron kept
one PID across every failure, 85 MB RSS, with 40 GiB free. Its own log shows
only `QUIC worker connection error: query IO error: connection lost`, and
Wendelon reports `sending stopped by peer` — the downstream cancel, not a
cause.

The configured timeouts are both longer and therefore **unreachable**:

| bound | value | reachable? |
|---|---|---|
| QUIC idle (unconfigured; library default) | ~30 s | fires first |
| SQL statement timeout (`toron.toml`) | 60 s | never |
| client `DEFAULT_QUERY_TIMEOUT` | 120 s | never |

Neither `idle_timeout` nor `keep_alive_interval` appears anywhere in
`crates/toron` or `crates/toron-client`, so the transport default governs. A
query that produces no packets for 30 s loses its connection regardless of
either policy.

Two consequences worth separating:

1. **No long query can ever report a timeout.** A statement that should be
   refused at 60 s reports a transport failure at 30 s instead — a typed,
   actionable error replaced by one indistinguishable from a network fault.
2. **The 14,000-row boundary was an artifact**, not a limit. 14,000 rows takes
   ~30.4 s, which straddles the cut; it passed on one binary and failed on
   another for that reason alone. Anyone bisecting this will find a "size
   limit" that does not exist.

## What this says about the thresholds

Nothing yet, and deliberately so. `scatter_max_vector_points = 50_000`,
downsample at 200,000 and raster at 1,000,000 remain **unmeasured**, and this
slice does not propose changing them. Rendering was never reached at any
threshold cardinality.

The exercise remains the right one. It cost one session and found two engine
defects that no amount of further design work would have surfaced — including
one, the unreachable timeout configuration, that is independent of
materialization entirely and affects every long query the product can issue.

## Post-fix rerun — 2026-08-21, after TORON-0577 and TORON-0578

Rerun on a release build of `5e32f991…` (from `e7b285de`), same data
directory, same machine.

**Defect 1 is fixed.** `SELECT COUNT(*)` now returns
`Toron StatementTimeout: statement timeout exceeded` at **60.01 s** — a typed
outcome at the configured bound, where it previously returned an untyped
`connection lost` at 30 s. The statement timeout is authoritative and
reachable. TORON-0577 also widened the policy to internal worker links, which
this slice had not identified.

**Defect 2 is unchanged, and is now much better characterised.**

| rows | materialized | plain CTAS control | Arrow path (no text) |
|---|---|---|---|
| 2,000 | 4.38 s | 4.38 s | 4.39 s |
| 5,000 | 10.93 s | 10.77 s | 10.92 s |
| 13,000 | 28.47 s | 27.50 s | 27.53 s |

Three controls, one conclusion. The open question from the first run is
**answered: the cost is not materialization-specific.** A table built by an
ordinary `CREATE TABLE … AS SELECT` reads at the same rate as one built by
`materialize`, so the fifth arc's write path is exonerated. And routing
through the Arrow-IPC adapter, which renders no text table, is equally slow,
so the CLI's formatting is exonerated too.

**~460 rows/s (≈2.2 ms/row) is general Toron scan throughput.**

TORON-0578 fixed a real defect — `COUNT(*)` over a `Range` panicking once
column pruning left an empty schema — but that is a different bug from this
one, and this slice never reached it: its `COUNT(*)` ran over a materialized
Wendelon table, not a range.

### What this bounds

At ~460 rows/s the 60 s statement timeout caps any query at roughly **27,000
rows**. That is the product-wide ceiling today, not a plotting concern:

| target | rows | projected |
|---|---|---|
| statement timeout ceiling | ~27,000 | 60 s |
| smallest scatter threshold | 50,000 | ~1.8 min |
| downsample threshold | 200,000 | ~7.2 min |
| raster threshold | 1,000,000 | ~36 min |
| 10,000,000 rows (see correction below) | 10,000,000 | ~6 h |

The scatter thresholds remain unmeasured and untuned. Rendering has still
never been reached at any threshold cardinality.

## Second rerun — after WENDELON-0088 (response batching)

0088 batched Wendelon table-query responses; the db-team handoff set the
criterion: if the full path rose toward ~4,500 rows/s, per-frame daemon
overhead was the material cost. Measured on binary `b1602ac5…` (from
`cb0037e9`), same machine and data directory, load average 1.57:

| rows | pre-0088 | post-0088 | post rate |
|---|---|---|---|
| 2,000 | 4.38 s | 8.77 / 8.66 s | ~230 rows/s |
| 5,000 | 10.77 s | 17.69 s | ~283 rows/s |
| 13,000 | 27.50 s | 27.02 s | ~481 rows/s |

**Decisive negative result: response framing is not the dominant cost.** At
the largest size measured the path runs at 481 rows/s against 461 before.

Two further observations, both handed to the db team:

- **Small results roughly doubled** while large ones did not move. The cost
  shape changed from flat per-row to a large fixed component plus per-row,
  consistent with a batch-accumulation threshold or timer. Until that is
  explained, no benchmark on this path is stable — the baseline moved for
  unstated reasons.
- **Large-size numbers are ±30%**: 13,000 rows measured 27.02 s on the plain
  control and 35.19 s on the materialized table in one session. The
  2,000-row point is the reliable regression detector for now.

The ceiling is therefore ~29,000 rows at the 60 s statement timeout, and a
50,000-point read still cannot complete. Thresholds remain unmeasured and
untuned.

## Third rerun — 2026-08-24, after TORON-0093 and TORON-0094

**The scan blocker is gone and rendering was reached for the first time.**
Every earlier run in this document failed before rendering at any threshold
cardinality. Measured on binary `0b8d76d5…` at engine commit `ef27685c7`,
verified as the running `nilket toron` process before the run rather than
merely installed.

| points | materialize | transfer + prepare | first render | steady render |
|---|---|---|---|---|
| 49,999 | 865.8 ms | 656.9 ms | 260.8 ms | 256.6 ms |
| 50,000 | 791.7 ms | 635.1 ms | 255.8 ms | 256.1 ms |

Reads run at roughly **76,000 rows/s**, against ~481 rows/s on 2026-08-21.

### Build basis, which is not the obvious one

`cargo test --release` **cannot compile this workspace**:
`iced_core-0.14.0/src/renderer.rs:2` gates the null renderer behind
`#[cfg(debug_assertions)]`, and polariton's widget tests construct
`iced::Element<'static, _, _, ()>`. Optimized measurement therefore requires
assertions left on, which is why the `evidence` and `evidence-calibration`
profiles exist. Neither is a shipping-binary benchmark: `debug-assertions` and
`panic = "unwind"` are forced by the renderer and the test harness. They
calibrate shipping-grade *codegen*, not the shipping binary.

### Verdict: `scatter_max_vector_points = 50_000` is too high

At exactly the threshold a steady vector re-render costs **~256 ms** — roughly
4 frames per second, excluding the 635 ms transfer. 50,000 is the value chosen
as the *maximum* at which vector rendering is still acceptable, and it meets no
interactive budget.

A replacement value is not proposed from these two points alone. The curve
below 65,536 was measured separately and is linear at ~5.09 µs/point; on that
basis 20,000 (~103 ms) is the defensible replacement, and 6,000 (~33 ms) would
be required if ADR 0133 stages 3–4 introduce continuous drag-to-zoom. Changing
the constant is a PRODUCT record decision, not one this document makes.

### Tuning the threshold does not improve time-to-first-plot

Transfer costs ~12.7 µs/point here, about 2.5× the render cost.
`scatter_max_vector_points` governs re-render smoothness only; the dominant
term in getting a plot on screen is the read, and that lever is not in the
plotting layer.

## Fourth rerun — TORON-0094 fixed; every threshold cardinality reached

**Chained writes work.** `c92833aff` clears the rejection below, and for the
first time in this document all six threshold cardinalities materialize, read,
and render. Binary `42608211…`, verified as the running `nilket toron` process.

| points | materialize | transfer + prepare | steady render | µs/point |
|---|---|---|---|---|
| 49,999 | 970 ms | 666 ms | 272.3 ms | 5.45 |
| 50,000 | 1,177 ms | 647 ms | 271.1 ms | 5.42 |
| 199,999 | 3,630 ms | 2,580 ms | 655.2 ms | 3.28 |
| 200,000 | 4,460 ms | 2,567 ms | 658.6 ms | 3.29 |
| 999,999 | 27,720 ms | 13,483 ms | 1,624.0 ms | 1.62 |
| 1,000,000 | 33,141 ms | 13,344 ms | 1,606.9 ms | 1.61 |

**Profile caveat:** taken under `evidence` (thin LTO), which measured ~7%
slower than fat-LTO codegen on the 50,000 case. These are conservative bounds;
a calibration-grade rerun is outstanding before any threshold constant moves.

Transfer holds **~74,000–77,000 rows/s across a 20× range**, so chaining costs
nothing on the read side. Write throughput falls with size — ~52,000 rows/s at
199,999 against ~30,000 rows/s at 1,000,000 — which is the chained path's own
scaling and is not a plotting concern.

### The downsample threshold does not bound render cost

This was tested by prediction and the prediction failed. If
`scatter_downsample_target = 200_000` bounded cost, a 1,000,000-point render
should cost about what a 200,000-point render costs (~658 ms). It costs
**1,624 ms**, 2.5× that.

Cost per point falls monotonically — 5.09 µs vector, 3.28 µs at 200k, 1.62 µs
at 1M — but never flattens. That is the signature of an O(n) selection pass
over every point followed by drawing the survivors: **the threshold reduces the
draw, not the scan.** A million-point plot costs ~1.6 s to re-render regardless
of what the target is set to.

Consequence: lowering `scatter_downsample_target` cannot make large plots
interactive. Bounding that cost requires not visiting every point — a different
mechanism from choosing how many to draw.

### The raster threshold is still unmeasured, and the exhibit cannot reach it

The comparisons are `<=` (`plot/kernels/scatter.rs:586`), so a threshold
engages strictly *above* its value. `CARDINALITIES` tops out **at** 1,000,000,
so the raster path is never exercised: 999,999 and 1,000,000 render identically
(1,624.0 ms and 1,606.9 ms), both on the downsample path.

The list's own comment claims "just below, at, and above each scatter
boundary," which holds for the vector threshold and the downsample threshold
but not the top one. Reaching raster needs a cardinality above 1,000,000. This
was invisible until now because no run had ever got this far.

### Fifth rerun — TORON-0095 clears the cleanup timeouts

On binary `89c00b4a…` (commit `95574557e`) all six cardinalities complete with
**zero cleanup failures**, where the previous engine failed five of six.

| points | materialize | transfer + prepare | steady render |
|---|---|---|---|
| 49,999 | 992 ms | 647 ms | 273.1 ms |
| 50,000 | 1,232 ms | 659 ms | 275.6 ms |
| 199,999 | 4,389 ms | 2,589 ms | 639.9 ms |
| 200,000 | 4,294 ms | 2,567 ms | 650.0 ms |
| 999,999 | 29,921 ms | 13,203 ms | 1,614.1 ms |
| 1,000,000 | 32,418 ms | 13,278 ms | 1,597.4 ms |

Render and transfer are unchanged from the fourth rerun, so asynchronous
reclaim does not perturb the measurements. Total run time fell from **478.9 s
to 137.0 s** — the ~342 s difference matches the five 60 s timeouts the
previous run was paying, confirming those were real wall-clock stalls rather
than merely reported errors.

**What this run does not show.** 0095 makes reclaim asynchronous, so a
successful `DROP` now means *accepted*, not *reclaimed*. The exhibit therefore
lost a capability it had this morning: it can no longer detect a failed
reclaim, because the failure would happen after the response. Storage
occupancy, not the drop's return value, is the only viz-side signal left.

### Superseded regression: `DROP TABLE` exceeds the statement timeout

**Five of six cleanups failed**, each with
`Toron StatementTimeout: statement timeout exceeded` — at 49,999, 199,999,
200,000, 999,999, and 1,000,000. Only 50,000 succeeded. The previous engine
(`0b8d76d5…`) produced zero drop errors on the same workload.

This is a typed timeout, not a recurrence of the 0093 catalog wedge. Notably it
does **not** track table size: 49,999 failed while 50,000 succeeded, and both
ends of a 20× size range failed alike, which argues against a per-row cost.
Visible only because the exhibit reports cleanup failures instead of discarding
them.

## Superseded — TORON-0094 acceptance: chained writes were rejected

**The 200,000 and 1,000,000 thresholds remain unmeasurable**, now for a
different reason than before. The 64Ki admission cap no longer refuses them —
TORON-0094 chains writes above it — but the first chained write fails:

```
materialize of 199999 rows failed (TORON-0094 chains writes above 65,536):
RuneDispatchFailure { kind: Execution,
  message: "invalid PBC program for descriptor", span: None }
```

199,999 rows of two `Float64` columns plans to four chunks
(65,536 × 3 + 3,391), well inside the 5 MiB section bound. `exceeds maximum
65536` is gone, so admission passed and the rejection came from the receiver,
past planning.

**Corrected by the db team's diagnosis:** the *first* chunk commits. The
descriptor fallback for a provisional CTAS object was gated on write-id
equality with the saga's `row_write_id`, which admits chunk 0 alone; the
**second** frame — the first derived id — was rejected with
`PBC_REJECT_SCHEMA_UNKNOWN`. The statement fails on its first chained attempt,
which is what this slice observed, but the failing frame is the second.

This is recorded as the current 0094 acceptance state, not as a discarded run.
The exhibit is deliberately fatal here rather than skipping the cardinality, so
the failure is visible in the run itself. Handed off in
`nilket/crates/toron/plans/wip/claude_iter_6_0094_chained_write_rejected.md`,
with two asks: that `PbcRejectCode` survive into the public error — Toron
flattens `TableWriteRejected` to a fixed string at `error.rs:136`, discarding
which of ~30 causes fired — and that a live 199,999-row regression exist, since
the current coverage exercises the planner in-process and cannot catch a
wire-side rejection.

**Excluded hypothesis:** the `u16` row-count sentinel. Every chained write
necessarily contains at least one chunk of exactly 65,536 rows, which ADR 0094
§2 flags as the sentinel case, but `encode_table_write_row_count` /
`decode_table_write_row_count` (`wendelon/src/schema/table.rs:551-575`)
round-trip it correctly. Recorded so it is not re-investigated.

## TORON-0093 confirmed from this side

The 2026-08-23 defect — a half-completed `DROP` leaving a local descriptor with
no finalized Wendelon marker, after which `materialize` issued a certificate
for a table that could not be read back — did not recur. The exhibit now
reports every cleanup failure instead of discarding it, and the run produced
**zero** drop errors.

## Render matrix — 2026-08-25, the measurement PRODUCT-0138 was withdrawn for

The withdrawn ADR 0138 claimed that lowering `scatter_max_vector_points` to
20,000 would trade per-point fidelity for a re-render falling from ~255 ms to
~103 ms. **Both halves were wrong**, and this section is the measurement that
replaces the claim.

The error was structural: 102.8 ms was the **vector** cost at 20,000 points,
quoted as though it were the **downsampled** cost of a 20,001-point plot. Two
different code paths. `plot/kernels/scatter.rs:586` routes above-threshold work
to `Downsampled`; `:287` sets `per_series_target = scatter_downsample_target /
series_count`; and `downsample_series` allocates a ~target-cell grid while
scanning every point. Against 200,000 cells, a plot of 20,001..=50,000 points
keeps most of them.

Measured with a pure-render exhibit —
`product_0138_scatter_render_matrix_evidence` in `plot/kernels/scatter.rs`,
no engine required. Latency comes from `SvgBackend`; surviving-point counts
come from a separate counting backend that tallies `PathBuilder::circle`
inside `fill_path`/`stroke_path` and `draw_image_*` separately, so the resolved
path is **observed rather than assumed**. Median of five renders, 1200×720.

### The band: the proposed change is an unfavourable trade throughout

Single series; `auto-present-50k` reproduced `forced-vector` exactly at every
cell, which is the harness self-check.

| points | vector | proposed 20k | latency saved | points retained |
|---|---|---|---|---|
| 20,001 | 112.7 ms | 107.4 ms | 4.7% | 93.0% |
| 25,000 | 135.3 ms | 129.8 ms | 4.1% | 91.4% |
| 30,000 | 159.6 ms | 150.3 ms | 5.8% | 90.0% |
| 40,000 | 216.5 ms | 195.3 ms | 9.8% | 87.0% |
| 50,000 | 268.6 ms | 235.9 ms | 12.2% | 84.4% |

**At every cardinality in the band the fidelity surrendered exceeds the latency
gained**, and the gap never closes. Lowering the threshold to 20,000 discards
7–16% of points to recover 4–12% of render time.

Series count is immaterial here — 18,604 / 18,667 / 18,623 survivors at 1, 4,
and 8 series — because `per_series_target` divides 200,000 by the series count
while the total cell budget stays fixed.

**Consequence: `scatter_max_vector_points` is not independently meaningful.**
In this band the surviving-point count is governed by
`scatter_downsample_target`, not by the threshold that selects the path. Any
record proposing a threshold change must decide both together.

### Above the band: rasterization is the real lever

| case | prepare | render | survivors | mode |
|---|---|---|---|---|
| 1,000,001 | 99.3 ms | 879.6 ms | 139,542 (14.0%) | downsampled |
| 7,000,000 | 692.4 ms | 7.9 ms | 0 circles, 1 image | rasterized |

Re-render falls **~110×** across that boundary, because rasterization moves the
cost into preparation — paid once, not per frame. Nothing available inside the
vector/downsample bands approaches it.

### Why raster had never been exercised

`resolve_render_mode` requires **both** `total_points > scatter_raster_threshold`
**and** `total_points > pixel_budget * 8`. At 1200×720 the pixel budget is
864,000, so the second gate demands **more than 6,912,000 points**.

`1_000_001` therefore downsamples at this viewport; it does not rasterize. That
is the second reason raster had never been reached, independent of every run
having stopped at 1,000,000. The unit test that shows `Rasterized` uses a
100×100 viewport, where the same gate sits at 80,000.

## Raster policy — 2026-08-25, and a correction to the section above

The render-matrix section reports a 7.9 ms raster render and calls
rasterization "the real lever", with cost "paid once, not per frame."
**That conclusion is withdrawn.** The 7.9 ms was a cache *hit*, and the claim
only holds for a viewport that never moves.

`ScatterRasterKey` is `{ width, height, point_size, x_range, y_range,
background, series_colors }` (`plot/kernels/scatter.rs:46`), and its ranges come
from the resolved transform via `current_x_range_bits`. **A pan or a zoom moves
`x_range`, so it invalidates the cache.** Measured by
`product_0138_raster_cache_and_gate_evidence`, which asserts the key actually
moves before timing anything:

| scenario | median |
|---|---|
| first render, cold cache | 5,538.7 ms |
| steady state, domain unchanged | **8.5 ms** |
| pan, domain moves every frame | **5,361.4 ms** |

A gesture frame costs what a cold miss costs. Against the 879.6 ms a
1,000,001-point downsampled render takes, rasterization under interaction is
roughly **six times worse**, not 110× better.

**Raster is therefore a static-viewport optimisation.** Any policy that widens
it must be conditioned on whether the viewport is moving; a pixel-density gate
alone would push large plots into a mode costing ~5.4 s per gesture frame.

### The gate is not where the widget rect suggests

`resolve_render_mode` divides the **inner plot area**, not the widget rect. The
rasterizer's own output reports that area as 1075×605 at a 1200×720 widget:

| | pixels | gate (`× 8`) |
|---|---|---|
| widget rect | 1200 × 720 = 864,000 | 6,912,000 |
| inner plot area | 1075 × 605 = 650,375 | **5,203,000** |

The earlier section's 6,912,000 is wrong by 25%, which is why a 6,900,000-point
frame rasterized when that figure predicted it would not. Chrome — axes, labels,
padding — shrinks the budget, so the gate moves with title and label
configuration rather than being a property of the window.

### Method note

Both corrections trace to the same cause: two different structs feed two
different decisions. The render **mode** resolves from `settings_model`
(`plot_with_settings` → `resolve_for_layer`), while the **domain** reaches the
raster key through `PlotWidget.settings`, which `layout()` consumes. An earlier
revision set domains on the model, moved nothing, and timed cache hits while
labelling them invalidations. The exhibit now asserts the key changes before
reporting any timing.

## Correction: the "README claim" attribution was wrong

Earlier revisions of this document listed a "README claim" of 10,000,000 rows
in their projection tables. **There is no such claim in the README.** The only
occurrence in product prose is ADR 0135's aside — "an engine that answers
10M-row queries" — which concerns reads, not materialization, and is rhetorical
framing for why cancellation matters. The row is retained above with its
attribution struck, since the projection itself was used in reasoning at the
time.

## Recommended next steps

1. ~~**db team:** configure QUIC liveness~~ — **done**, TORON-0577.
2. ~~**db team:** establish the scan control~~ — **done here**: general, not
   materialization-specific, and not CLI formatting. Three controls agree.
3. ~~**db team:** general scan throughput~~ — narrowed further: **not
   response framing** (WENDELON-0088, decisive negative). Remaining direction
   is profiling the Wendelon table-query path — per-location materialization
   and PBC — plus a separate explanation of the post-0088 small-result fixed
   latency before any benchmark on this path is treated as stable.
4. ~~**viz team:** rerun once reads clear 50,000 rows~~ — **done**, third
   rerun above. Reads run at ~76,000 rows/s and rendering was reached.
5. ~~**db team:** TORON-0094's chained write is rejected~~ — **fixed**,
   `c92833aff`. All six cardinalities now complete.
6. **db team:** `DROP TABLE` exceeds the 60 s statement timeout on five of six
   tables, new in `42608211…`. Size-independent, so not a per-row cost.
7. ~~**viz team:** `scatter_max_vector_points` should fall to 20,000~~ —
   **retracted.** The render matrix shows that change discards 7–16% of points
   to recover 4–12% of render time, an unfavourable trade at every cardinality
   in the band. ADR 0138 was withdrawn before acceptance for this reason. Any
   replacement must decide `scatter_max_vector_points` and
   `scatter_downsample_target` **together**; the threshold alone selects a path
   without governing what that path retains. The larger lever is
   rasterization, which cuts re-render ~110x but is gated behind
   `pixel_budget * 8`.
8. **viz team:** rerun under `evidence-calibration` before any constant moves —
   the fourth-rerun figures are `evidence`-grade and ~7% conservative.
9. **viz team:** add a cardinality above 1,000,000. The raster threshold has
   never been exercised, because `CARDINALITIES` stops at its exact value and
   the comparisons are `<=`.
10. **Open question, not a tuning task:** the downsample threshold reduces the
    draw but not the O(n) scan, so no value of `scatter_downsample_target`
    makes a million-point plot interactive. Bounding that needs a mechanism
    that does not visit every point.
