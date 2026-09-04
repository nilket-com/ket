# PRODUCT-0157 implementation evidence

Status: implemented 2026-09-04 against the amended plan; awaiting review and
the operator's browser gate.

## Implemented

- `plot/series.rs` gains `SeriesLabel`, a prepared identity beside the label
  preparation formatted for it. `PlotKernel::series_labels` is implemented by
  line, scatter, stacked area, radar, parallel coordinates, the layered
  kernel, and the grid, each reading the label next to the identity before
  any legend union. The registry stays the sole authority for ordinals; the
  writer joins by exact identity equality and refuses a figure whose
  registry identity has no prepared label (`PublishError::SeriesLabel`).
- Coverage (§1): `PlotKernel::rasterizes_identified_series` and the composite
  twin answer, at the plot area the render path hands the kernel, whether any
  identified occurrence resolves to the density raster on an embedding
  backend — the same `resolve_render_mode` the scatter renderer draws with.
  `PlotWidget::series_covered` dispatches like the projection seam. A figure
  is actionable when covered, identified, and within the 4,096 bound.
- The package (§2, §3): `figures/<id>/series.json` (schema 1) with the
  `viewBox`, `actions`, `initially_hidden`, one row per registry identity
  (ordinal, label, field label and semantic type, display-bearing hit value),
  and the legend bounds and entry rectangles from `legend_hit_geometry` at
  the export bounds. The manifest figure entry gains `visibility`:
  `actionable`, `inert` with reason `raster`, or `absent` with reason
  `no-identified-series` or `series-limit` and the observed count. An
  actionable figure renders and projects with the pane's hidden set cleared;
  every other figure renders as the desktop displayed it.
- The projection (§6): `RegionEntry` and the `Region` DTO carry an optional
  series ordinal; scatter sets it. `hits.json` is schema 2.
- The legend seam (§5): `PlotBackend::with_legend_entry`, a no-op everywhere
  but SVG, wraps the entry's sample only, as `<g data-legend="sN">`.
- The shell (§5): two CSS rules, `g[data-series].polariton-hidden` and
  `g[data-legend].polariton-hidden`, and a second non-executable data element
  per figure, `script.polariton-series`, under PRODUCT-0155 §6's escaping.
- The script (§4, §5, §6): pure functions `polaritonApplyVisibility`,
  `polaritonLegendAction`, `polaritonLegendEntryAt`, and
  `polaritonLegendSequence` carry the desktop's algebra and gesture rules;
  `polaritonResolve` takes the hidden set and skips a hidden series' regions
  as a counted operation; `polaritonApplyToSvg` toggles one class by
  ordinal-addressed selector. The installer applies `initially_hidden` before
  the first pointer event, arms a toggle on a bare left press inside the
  legend, fires it on release over the same entry, isolates on shift-press,
  and cancels on release elsewhere, `pointercancel`, or window blur. The
  script hash is re-pinned by construction.
- Guards: the glue assay refuses `setAttribute`, `getAttribute`, `getBBox`,
  `getCTM`, `getTotalLength`, `getPointAtLength`, and path-data access with
  a positive control; the shell emitter guard asserts exactly the two rules;
  a new guard scans the series DTO region for `ExactScalar`, `ScalarType`,
  `scalar_type`, `zone`, `scale:`, `candidate`, and `FieldRef` with a
  specimen row.

## Review findings folded into the cut

Five browser-side defects found in read-only review, each fixed with a
test that drives the shipped asset rather than inferring from its pure
functions (`product_0157_installed_handlers_own_the_sequence_through_a_document_stub`,
`product_0157_resolver_at_the_bound_with_the_maximum_hidden_set`):

1. The snapshot is now applied at shell emission: the inlined SVG of an
   actionable figure carries `class="polariton-hidden"` on the hidden
   groups, so nothing hidden paints before the script runs; `figure.svg`
   stays complete and class-free (`snapshot_classes`).
2. The resolver takes a precomputed constant-time membership set
   (`polaritonHiddenSet`), so one visibility check is one lookup; the
   installer rebuilds the set on state change, never per pointer event. A
   32,768-region projection with 4,095 hidden ordinals answers within the
   counted bound.
3. Only actionable figures receive the snapshot class; an inert figure's
   already-dimmed swatch is never dimmed again, and no gesture listener is
   installed on it.
4. A bare press takes pointer capture; `pointerup` on the window,
   `pointercancel`, `lostpointercapture`, and window blur all disarm, so a
   release outside the SVG cannot leave a stale press.
5. Alt, ctrl, and meta are rejected before shift is consulted, in one
   gesture state machine (`polaritonGesturePress` and
   `polaritonGestureRelease`) shared by the pure sequence and the installed
   handlers; the parity test covers shift+ctrl and shift+meta.

## Decisions taken at implementation

- The field's semantic type is the role the desktop minted for the partition
  channel (line and scatter mint the series field as nominal); the value's
  kind follows the scalar, so a temporal partition of a nominal series field
  crosses as a nominal field with a temporal display value. Gate 1's fixture
  asserts exactly that.
- A hidden series' legend entry stays drawn in the desktop's hidden state in
  inert and over-bound figures, as PRODUCT-0156 §3 requires; gate 8's wording
  was amended under review to say so rather than claiming the swatch group
  absent.
- The over-bound fixture prepares its scatter kernel directly: the planner
  authors a colour channel over a bounded palette and refuses 4,097
  categories, which is the planner's rule and not this record's. Its hit
  projection also exceeds PRODUCT-0155's byte bound on its own, so gate 8's
  region check runs at the projection seam the writer uses for an inert
  figure rather than through a `hits.json` that is legitimately absent.
- The 0114 grid guard banks the series-coverage traversal as the eleventh
  per-cell resolution; the series DTO guard scans code lines only, since the
  DTO comments name what must not cross.
- PRODUCT-0155 gate 2's per-region budget is three counted operations, so the
  asset's parity test asserts under 98,304 rather than 65,536.

## Verification

All on host `slim`, 2026-09-04, against the final tree of the impl cut,
through `nilket/scripts/cargo-cgroup` with `--locked`.

- Full default-head suite, `-p polariton --no-fail-fast --tests`: 43
  targets, 1,342 passed, 0 failed, 20 ignored. Includes the ten
  PRODUCT-0157 tests and the Polariton architecture target (123 passed).
- Full reduced-head suite, `--no-default-features --no-fail-fast --tests`:
  39 targets, 1,028 passed, 0 failed, 8 ignored.
- Render-evidence probe, `--features product-0057-render-evidence --test
  product_0057_render_evidence`: passes; Intel UHD Graphics 770, Vulkan,
  Mesa 26.0.8; every probe at its automated or production disposition.
- Workspace architecture gate, `--test architecture_test` across the
  workspace: 166 passed, 0 failed, after `cargo fmt`.
- `cargo check --no-default-features -p polariton`, `cargo fmt --check`, and
  `git diff --check` pass.

Gate 10, the browser acceptance, is the operator's and is recorded in
`0157_browser_acceptance.md` when run. No cross-platform compilation is
claimed.
