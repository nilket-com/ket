# PRODUCT-0159 implementation evidence

Status: implemented 2026-09-05 against the plan; review corrections folded in
(scheduled-instant observer, gate 1 timings through the batch, showcase and
three-size export comparisons, resvg cross-check with gate 3 amended, trace
after render); awaiting re-review.

## Implemented

- `PlotBackend::fill_marks(centres, radius, color)` (plot/common.rs): a batch
  of identical circles composited per mark; the default fills one mark per
  call, so recorders see one operation per mark and the iced backend never
  hands lyon a compound circle path.
- Scatter's vector and downsampled arms emit their series through the batch
  inside the PRODUCT-0156 series seam; no other kernel calls it.
- The SVG backend (plot/export.rs) writes an opaque batch as one `<path>` of
  circle subpaths, byte-for-byte the compound path's output, and a
  translucent batch as one `<defs><circle id="mark_N">` plus one `<use>` per
  centre under the current transform. PNG and AVIF delegate to it, so
  exports of opaque scatters are byte-identical to before the cut.
- The shell scopes `mark_N` definitions and references per figure like clip
  ids (publication.rs `scope_svg_ids`).
- Gate 5's instrument, opt-in by `POLARITON_LOOP_LATENCY=1`: a 16 ms tick
  subscription armed at application start. iced's timer starts one period
  after the subscription, skips missed ticks, and yields tokio's scheduled
  instant for each delivered tick; that instant travels in the message.
  Per delivered tick the probe measures *lateness*, now minus the tick's
  scheduled instant, and *skipped*, the gap to the previous scheduled
  instant beyond one period net of the lateness already reported, and
  reads their maximum as *unserviced*; nothing derives from a tick count,
  so skipped ticks never accumulate into drift. The first tick's arrival
  after arming is reported separately and, beyond the one period the timer
  needs to start, counts toward the maximum, so a subscription that starts
  late with an on-time first tick cannot hide a startup stall. Two unit
  tests drive the pure observer with synthetic instants through delayed and
  skipped delivery under both tokio reporting conventions, a late first
  tick, and a delayed subscription start, and assert one stall is measured
  once at its true length. The maximum and the count
  over 100 ms print on each new maximum and on drop. Two traces share the same arming clock: the scatter's live
  resolved arm with its point count and plot area at each interactive draw,
  and a "geometry built" line per pane after its plot cache has rebuilt its
  data layer, which is geometry generation, not presentation.
- Architecture guard: the three gate constants and four lines of
  `resolve_render_mode` are retained verbatim; scatter is the only kernel
  calling the batch; no compound circle loop remains in the downsampled arm.

## Gates

| gate | evidence |
|---|---|
| 1 linear, both arms | Counted: `product_0159_both_arms_fill_one_mark_per_call_and_never_a_compound_path`, 12,000 vector marks and the downsampled arm's retained marks, each fill carrying exactly one circle. Timed, through the trait's own batch on iced's tessellator (`probe_batch_through_the_trait_on_the_iced_tessellator`, dev profile, one fill per mark asserted): **50,000 marks 0.111 s; 66,817 marks 0.161 s; 200,000 marks 0.465 s**, against 88.9 s for 66,817 marks in one compound path (`probe_tessellate_downsampled_marks_at_the_raster_fixture_size`). Both bounds hold. |
| 2 opaque output | SVG and PNG exports: `product_0159_showcase_and_fixture_exports_are_byte_identical_to_the_compound_path` renders the Trends showcase scatter (its 24 rows embedded) and the fixture scatter at 320×220, 950×420, and 360×360 through the widget to the SVG and PNG backends, once with the batch and once through a borrowing adapter around each supplied backend reference that restores the pre-cut compound path, and asserts byte identity of the SVG and of the PNG pixels in every case; `product_0159_opaque_batch_is_byte_identical_to_the_compound_path` covers the backend under a transform. Desktop, production renderer with MSAA×4 (`PRODUCT_0159 mark_batch` in the 0057 evidence run): non-overlapping opaque marks identical at scale 1 and 2; the dense 1.5 px lattice of 6 px marks differed in **0 pixels** at both scales, maximum channel delta 0, so the bounded case is empty on this adapter. |
| 3 translucent per mark | SVG and resvg PNG: `product_0159_translucent_batch_composites_per_mark_in_svg_and_png`. Desktop (0057 probe): single 128, overlap 192; the compound union control 128 / 128. The same pair through the SVG backend and resvg in the same probe: single 128, overlap 184, within PRODUCT-0057's tolerance of the desktop at both points; pixel-for-pixel agreement between the two rasterizers is not claimed, per the amended gate. |
| 4 recorders see marks | the counting test above; the PRODUCT-0156 and PRODUCT-0157 seam and grouping tests unchanged and green in the ladder. |
| 5 event loop | below. |
| 6 nothing else moved | `product_0159_gate_constants_and_render_mode_are_retained_and_the_batch_is_scatters`. |
| 7 both heads | below. |

## Gate 5 measurement

Release binary at the impl tree with the corrected probe,
`POLARITON_LOOP_LATENCY=1`, host `slim`, GNOME 50 Wayland, default window,
`uat/plot_fixtures_showcase.rn` run by the operator's Ctrl+Enter about ten
seconds after arming; the operator observed all three panes drawn. The
trace confirms the arm under test rather than inferring it from the pane:

```
POLARITON_LOOP_LATENCY armed
POLARITON_LOOP_LATENCY first tick handled 115.2 ms after arming (timer starts one period after subscription)
POLARITON_LOOP_LATENCY new maximum: max_unserviced_ms=96.1 over_100ms=0 ticks=1
POLARITON_LOOP_LATENCY geometry built title="plot.area" revision=0 at_ms=3267
POLARITON_LOOP_LATENCY geometry built title="plot.line" revision=0 at_ms=3268
POLARITON_LOOP_LATENCY scatter mode=Downsampled points=1157625 plot_area=781x211 at_ms=9932
POLARITON_LOOP_LATENCY geometry built title="plot.scatter" revision=0 at_ms=10157
POLARITON_LOOP_LATENCY new maximum: max_unserviced_ms=259.1 over_100ms=1 ticks=621
```

| measure | value |
|---|---|
| live resolved arm | Downsampled, 1,157,625 points, plot area 781×211 (the stall's shape) |
| maximum unserviced time, from arming | **259.1 ms** at tick 621 (9.9 s), the scatter's geometry build; bound 500 ms |
| ticks with more than 100 ms unserviced, over 621 | 1 |
| first tick after arming, reported separately; 99.2 ms of it counts as unserviced, below the maximum | 115.2 ms |
| supporting: longest continuous main-thread busy run, `/proc` at 50 ms | 5 samples, about 250 ms |
| supporting: main-thread CPU over the two-minute window | 10.15 s |

The "geometry built" timestamps are geometry generation on the probe's
clock; that the panes were presented is the operator's observation of the
same run, and the two are kept distinct. Two earlier runs are superseded:
one without the mode trace (270.8 ms, arm unproven) and one with the first
probe (291.6 ms on the downsampled arm), whose deadline was derived from a
tick count and could accumulate drift after skipped ticks. The
not-responding dialog did not appear. The operator's debug-binary
observation is recorded when made. The 166 s main-thread episode captured
before the cut is context for the reader, not a controlled speedup
measurement; the controlled measurements are the probes under gate 1.

## Verification

All on host `slim`, 2026-09-05, on the final tree of the impl cut, through
`nilket/scripts/cargo-cgroup` with `--locked`, after `cargo fmt`.

- Full default-head suite, `-p polariton --no-fail-fast --tests`: 43
  targets, 1,351 passed, 0 failed, 24 ignored (the four ignored probes
  among them). Includes the four PRODUCT-0159 tests and the Polariton
  architecture target with the new guard (124 passed), and the two probe
  observer tests.
- Full reduced-head suite, `--no-default-features --no-fail-fast --tests`:
  39 targets, 1,037 passed, 0 failed, 12 ignored.
- Render-evidence probe, `--features product-0057-render-evidence --test
  product_0057_render_evidence`: passes, 18 probes at their dispositions,
  `PRODUCT_0159 mark_batch` among them; Intel UHD Graphics 770, Vulkan,
  Mesa 26.0.8.
- Workspace architecture gate, `--test architecture_test` across the
  workspace: 167 passed, 0 failed.
- `cargo check --no-default-features -p polariton`, `cargo fmt --check`, and
  `git diff --check` pass.

The geometry-built timestamps above are geometry generation on the probe's
clock; that the panes were presented is the operator's observation of the
same run, and the two are kept distinct.
