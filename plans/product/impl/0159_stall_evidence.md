# PRODUCT-0159 evidence: the desktop stall on a dense scatter

Observed 2026-09-04 on host `slim` (i9-12900K, GNOME 50 Wayland) while
opening `uat/plot_fixtures_showcase.rn` with the debug binary: Polariton held
one core at ~100% for minutes with GNOME's not-responding dialog; the scatter
pane was blank, then drew a flat block. 1,157,625 points, three series,
default point size, pane about 780×205 plot pixels, so the automatic gate
chose the downsampled arm.

## Excluded first

Ignored probes in `plot/kernels/scatter.rs` (`preparation_timing_probe`),
dev profile, on the fixture's exact shape:

| phase | time |
|---|---|
| whole preparation | 1.18 s (selection candidates 1.07 s) |
| one frame through `PlotWidget::render` to the SVG backend, 950×420 | 1.31 s, raster arm |
| one frame, 360×360 | 1.27 s, raster arm |

Neither is minutes; the SVG probe also took the raster arm at its layout and
so never exercised the live path.

## Attributed

Per-thread `/proc` sampling during the episode: the main thread at ~95% of one
core, 166 s of main-thread CPU on the first episode, every tokio and polars
worker asleep, RSS flat at 1.37 GB.

Two gdb stacks ten seconds apart, captured inside the episode by a watcher
that armed on the main thread's run state (files under `~/stall/`):

```
lyon_tessellation::fill::FillTessellator::{process_events, process_intersection,
  event_queue::insert_into_sorted_list}
iced_wgpu::geometry::Frame::fill
polariton::plot::common::{impl PlotBackend for iced}::fill_path   common.rs:2136
ScatterMarkRenderer::plot_downsampled                             scatter.rs:600
ScatterMarkRenderer::draw_interactive  (Downsampled arm)          scatter.rs:780
PlotGridKernel::render_plot_layer_interactive                     grid.rs:1428
PlotWidget::render_plot_layer_in_widget → canvas draw
```

`perf record -g`, twenty seconds inside the episode. On this hybrid CPU the
main thread's samples are in the `cpu_core` event section, not `cpu_atom`:
80K samples, 99.9% on the main thread.

| symbol | share |
|---|---|
| lyon FillTessellator::process_events | 52.0% |
| lyon FillTessellator::tessellate_impl | 18.9% |
| lyon EventQueue::insert_into_sorted_list | 13.2% |
| Vec::retain | 6.8% |
| lyon log_active_edges (debug-only) | 6.5% |

## Mechanism, measured

`probe_tessellate_downsampled_marks_at_the_raster_fixture_size` drives the
same lyon tessellator iced uses (`iced_wgpu::geometry::Frame`, no device) on
one series of the fixture in a 780×205 plot area, dev profile:

| case | marks | radius | time |
|---|---|---|---|
| production: every mark of the series in one fill path | 66,817 | 3 px | 88.9 s |
| same marks, one fill path each | 66,817 | 3 px | 0.156 s |
| same count, shrunk so no marks overlap | 66,817 | 0.74 px | 3.2 s |
| downsample cell equal to the mark diameter, one path | 4,550 | 3 px | 0.127 s |

The downsampler's 200,000-cell budget puts cells 1.55 px wide under 6 px
marks (overlap ratio 3.9). Lyon's non-zero fill of one compound path must
process every pairwise intersection, with a linearly-scanned event queue, so
cost is superlinear in overlap. Three series at 89 s each is the episode.
Separate fills of the same marks remove the intersections entirely.

PRODUCT-0140 priced this arm at 1.3 s per frame in release at full window and
retained the gate as an unpriced compatibility heuristic; the record halts
any change to its constants. This defect is the arm's own cost model, not
the gate, and is fixed without touching either constant.
