# Normalized bar width and its Rune modifier

Scope: implement PRODUCT-0021, then PRODUCT-0022. Deliverable is two substrate
`impl:` commits, one surface `impl:` commit, and one evidence commit.

## Halt rule

Stop on Bar in `LayerMark`, changed category domains or series membership,
prepared-data width, guide output, hover wording, another renderer, plot-spec
migration, open modifier dispatch, Toron or libaryon, execution, identity,
placement, headings, or dashboard change.

## Stage 0: feasibility findings

1. Vertical and horizontal paint duplicate the same four geometry constants;
   hover independently reconstructs the wider subgroup slot. One private bar-
   rectangle iterator can divide the complete category band among series and
   serve both paint and hover without a second pane or renderer tree.
2. `PlotKernel::plot_with_settings` and `hover_with_settings` already receive
   the retained five-tier model. `BarPlotKernel` can hold typed standalone
   `LayerSettings`, defaulting to Inherit, and resolve it without host changes.
3. A general envelope-only styled sink can hold `DisposableRunePlotSink` plus
   `LayerSettings`. Line/scatter can project back to their existing `LayerSpec`
   handoff; bar can use a new styled resolved-plot task. `LayerMark` remains
   untouched.
4. Bar width does not affect planning or layout. `ResolvedPlotSpec` and
   `BarPreparedData` need no field.

## Stage 1: model normalized slot width

Add private validated `BarWidth`, default `0.9`, and `bar_width` to typed layer
and cascade settings. Preserve Inherit in every existing constructor and keep
the three numeric setting types distinct.

Assay validation, five distinct tiers, selection rather than multiplication,
and the unchanged two-variant `LayerMark` boundary.

PRODUCT-0021 gates: 1, 2, 7, 8.

Commit: `impl: model normalized bar width per plot layer`.

## Stage 2: unify bar paint and hit geometry

Capture exact legacy vertical and horizontal SVG baselines first. Factor one
rectangle calculation used by paint and hit-testing. Resolve bar width at render
time, retire the hidden 80% group inset, divide the complete category band among
series, and retain hover prose and guide drawing. Keep width out of prepared data
and layout.

Assay exact 90%, half-slot, and touching 100% rectangles in both orientations,
including deliberate divergence from the captured legacy baseline; visible hit
versus padding miss; unchanged hover text for a retained hit;
layout/preparation identity; and exact guide SVG.

PRODUCT-0021 gates: 3, 4, 5, 6.

Commit: `impl: resolve normalized bar paint and hit geometry`.

## Stage 3: add the closed Rune surface

Generalize the envelope-only styled-single carrier to ordinary sink plus typed
settings. Extend the parser with one explicit `bar_width` case. Direct and
figure bars use a styled resolved-plot task; line/scatter retain the existing
layered handoff. Do not admit a modified bar in `parse_layer`.

Assay exact lowering and spans, all receiver combinations, bounds and atomicity,
ordinary Rune retention, shared-result identity, a bare/default/narrow named
host lifecycle, labels, headings, placement, and empty dashboard. Update
`../uat/test7.rn` with the near-touching `0.9999` semantic boundary case.

PRODUCT-0022 gates: 1–8.

Commit: `impl: add Rune bar-width layer modifiers`.

## Evidence

Write separate PRODUCT-0021 and PRODUCT-0022 evidence files, update corpus
v0.22, run all verification, build, and launch `../uat/test7.rn`.

Commit: `evidence: close normalized Rune bar width`.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
cargo test -p libaryon
cargo build -p polariton
git diff --check
```
