# Relative scatter point-size substrate

Scope: implement PRODUCT-0017 without a Rune spelling. Deliverable is three
`impl:` commits followed by one evidence-only commit. PRODUCT-0017 §8's eleven
gates are assigned exactly once below.

## Halt rule

Stop on any Rune-envelope edit, new cascade algebra, `AuthoredPlotSpec`
migration, data-driven size channel, guide behavior, non-scatter renderer
change, shared-domain compatibility change, dashboard edit, or execution,
transport, identity, and placement change.

Permitted production files are:

- `plot/spec/mod.rs` for `PointSize`, typed `LayerSettings`, and their carriage
  through layered resolution;
- `plot/theme.rs` and `plot/common.rs` for the existing five-tier cascade,
  retained render context, and default cascade-aware kernel hooks;
- `plot/factory.rs` and `plot/kernels/{layered,line,scatter}.rs` for layer-aware
  renderer construction, settings-aware geometric hits, and scatter modes;
- `gui/plot_pane/{state,view}.rs` plus mechanical `PlotWidget` construction
  sites in `editor.rs`, `gui/state.rs`, and `adapters/toron/mod.rs` to retain or
  wrap the settings model; and
- this plan, its evidence file, and the product corpus in the evidence commit.

No file under `rune_relation_envelope.rs`, `gui/dashboard/`, libaryon, Toron, or
`toron-client` is permitted.

## Stage 0: feasibility findings

This stage changes no code and discharges no gate. Record these findings in the
evidence file before implementation:

1. `PlotSettings` already retains the five accepted tiers, but
   `HostPlotState` stores only `ResolvedPlotSettings`. Retaining both at the
   widget boundary is sufficient; no protocol or panel-content change is
   needed.
2. `PlotKernel` can gain default cascade-aware draw and hover hooks that resolve
   and call the existing required methods. Only `LayeredPlotKernel` needs an
   override, so ordinary kernels and exporters keep their current contract.
3. `LayerRenderer` is already the per-layer boundary and `LayerSpec` already
   survives resolution as `ResolvedLayerSpec`. A typed settings record can ride
   that path without changing encoding, domain, or paint order.
4. Raster owns a cache beside `ScatterMarkRenderer`; moving size out of
   prepared data and into resolved settings leaves cache ownership in the right
   place. The key can include size without a new cache.

Stop before Stage 1 if any finding fails.

## Stage 1: model point size and per-layer precedence

Add validated `PointSize` with constants for the accepted domain, `ONE`, and the
legacy three-pixel base radius. Add typed `LayerSettings` to `LayerSpec`, default
it to all-Inherit in every established constructor, and carry it unchanged into
`ResolvedLayerSpec`.

Add `point_size: Override<PointSize>` to `PlotSettingsLayer` and a resolved
factor to `ResolvedPlotSettings`. Factor the existing cascade application so
`PlotSettings::resolve_for_layer(&LayerSettings)` substitutes the supplied
settings at exactly the layer tier before applying pane override. `resolve()`
continues to use the existing `PlotSettings.layer` value.

Assays:

- validate the complete numeric boundary including NaN and infinities;
- prove all five precedence tiers with distinct values and prove selection,
  not multiplication;
- resolve two sibling `LayerSettings` values through one shared base cascade
  and observe different results; and
- prove all existing constructors carry Inherit.

Gates: 1, 2, 3.

Commit: `impl: model relative point size per plot layer`.

## Stage 2: make scatter size render-time state

Capture the established default vector and downsampled SVG evidence before
moving any field. Remove `point_size_px` from `ScatterPreparedData` and remove
the argument from every preparation function and factory call.

Add a retained render-settings context at `PlotWidget`: a resolved projection
for its established drawing code plus the full `PlotSettings` model for
layer-aware kernel hooks. Legacy callers wrap their already-resolved settings as
library defaults; Rune host plots retain the model used to obtain their
projection.

Add default cascade-aware `PlotKernel` draw/hover hooks. Add resolved settings
to `MarkRenderer::hit_test`; line ignores them. Make `LayerRenderer` carry its
typed `LayerSettings`, and have `LayeredPlotKernel` resolve each layer before
drawing or hit-testing. Only unstyled one-layer specifications take the old
single-kernel fast path.

Scatter vector and downsampled paths derive radius from resolved settings.
Prove preparation equality structurally and compare default SVG output with the
pre-change evidence before asserting the doubled radius.

Gates: 4, 5, 8, 9.

Commit: `impl: resolve scatter size at draw and hit time`.

## Stage 3: repair raster size and close regressions

Replace one-pixel raster deposition with clipped filled-disc rasterization using
the same resolved radius. Add point-size identity to `ScatterRasterKey`. The
cache assay must distinguish a true same-key reuse from equal-looking output;
instrument privately in the module test if necessary.

Assert the footprint at default and doubled sizes, edge clipping, cache miss on
size change, and cache hit on repetition. Assert legend SVG is identical across
point sizes. Run the complete single/layered and architecture suites, and prove
the Rune envelope and dashboard were untouched.

Gates: 6, 7, 10, 11.

Commit: `impl: honor scatter size in raster rendering`.

## Evidence commit

Write `0017_relative_scatter_point_size_evidence.md` with Stage 0 findings, the
gate ledger, commit/scope trace, and exact command results. Add a corpus history
entry without claiming a Rune capability; PRODUCT-0018 remains the first
surface caller.

Commit: `evidence: close relative scatter point-size substrate`.

## Gate ledger

| Gate | Stage | Evidence |
|---|---:|---|
| 1 | 1 | validated factor and exact 3px/6px projection |
| 2 | 1 | five distinct tiers, pane wins, no multiplication |
| 3 | 1 | sibling layer resolutions differ over one base |
| 4 | 2 | prepared data has no setting and compares identically |
| 5 | 2 | pre-change SVG evidence plus doubled circles |
| 6 | 3 | raster filled-disc footprint and clipping |
| 7 | 3 | cache miss on size change and hit on repetition |
| 8 | 2 | cascade-aware geometric miss/hit boundary |
| 9 | 2 | unstyled one-layer fast paths and regressions |
| 10 | 3 | unchanged legend SVG |
| 11 | 3 | full architecture and no-surface regression |

Every gate appears exactly once.

## Verification

Before every implementation commit:

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
git diff --check
```

Final evidence also runs `cargo test -p libaryon` because the full arc crosses
host panels even though this cut changes no libaryon file.

