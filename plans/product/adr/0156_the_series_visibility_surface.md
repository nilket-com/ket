# ADR 0156: the series visibility surface

Status: proposed 2026-09-04, revised the same day after review found overlay
legends needing press priority over pan and brush, deduplicated entries with
conflicting identities needing an explicit non-actionable rule, identity
needing its scalar type and exact-value rules, stacked-area hiding needing a
stated accumulation rule, and the actions needing scope, cancellation,
unknown-identity, and no-op rules, and after implementation found that the
density raster a scatter draws above the PRODUCT-0140 gate is one image
across every series and needed its own rule. Plotting arc. The 2026-09-04
browser dogfood of the inspectable packages found no demand for clicking
marks and one compelling interaction everywhere a figure has series: hide a
category, show only a few, restore them all, from the legend. PRODUCT-0153
§5 says the web follows the desktop and never invents a competing
vocabulary, and the
desktop has no such vocabulary today. This record gives it one, narrowly: a
stable series identity distinct from its label, four pane-tier actions that
address that identity, fixed geometry throughout, and one backend grouping
seam so that every drawing surface can carry series identity without
knowing what a browser is. It decides no Rune spelling, no persistence, no
filtering, no recomputation, no cross-figure coordination, no keyboard or
agent surface, and nothing on the web.

## Context

### What exists

Every series-bearing kernel knows its series by display name. Line's
`SeriesData`, scatter's `ScatterSeries`, and stacked area's colour
assignments carry a `name`, a colour, and a `LegendProvenance`; radar and
parallel coordinates carry rows named by category. None carries the source
field or the exact source value the name was formatted from, so a series
has a label but no semantic identity. PRODUCT-0044 §3 defines a legend
entry's identity as the whole entry — label, swatch, sample — which is the
right rule for deduplication and the wrong handle for an action: two
figures can print the same label for different values, and a temporal or
numeric series prints a formatted label that is not its value.

The pane tier is the right home for a view state that never reaches Rune.
PRODUCT-0150 §4 put the committed viewport in
`plot_settings_model.pane_override.domains`, mutated only through the
surface's `update` as a `PlotPaneMsg`, re-resolving settings and bumping
`render_revision`; PRODUCT-0133 §9 made pane overrides session-scoped and
non-round-tripping, and PRODUCT-0142 shows them in the inspector with
`PaneOverride` provenance. A hidden-series set is that kind of state.

`PlotBackend` has `with_save` and `with_clip` but no grouping. The SVG
backend therefore emits marks with no series attribution, and a browser
cannot hide a series without knowing which paths belong to it. The kernels
already draw series by series — scatter fills one path per series, line
strokes one per series — so a grouping seam costs each kernel one wrapping
call.

The widget routes presses by modifier: shift begins a selection brush,
alt pans, a bare press is the 0150 gesture's start. The legend is laid out
by `legend_layout` and drawn by `render_legend` from a computed geometry,
but no press consults it.

### What the dogfood asked for, and did not

Line, area, radar, and parallel coordinates: toggle categories from the
legend, show only a few of many. Pie, funnel, dial, bar, histogram: nothing
now. Scatter, bubble, heatmap, box: hover is enough. No mark wanted a
click. The operator also named the two possible meanings without deciding
between them: hide the ink and keep the scales, or filter the data and
recompute. Only the first is a view; the second is a query.

## Decision

### 1. A series has a semantic identity, distinct from its label

`SeriesIdentity` pairs a source field reference — name, semantic type, and
scalar type,
PRODUCT-0151's `FieldRef` shape — with the exact source value the series
was partitioned on, in PRODUCT-0151's `ExactScalar` vocabulary. It is
minted by preparation from the series channel: line's series field,
scatter's colour field when it is categorical, stacked area's series
field, and the category column of radar and parallel rows. A series whose
partition has no source column — synthetic single-series figures, layers
without a series channel — has no identity and is not actionable. The
display label stays what it is; identity never formats. ExactScalar's
canonical equality and hashing apply, including temporal units and zones
and decimal scale. Null, non-finite, and unsupported partition values mint
no identity; their marks remain visible and their entries are not actionable.
Preparation must retain exact partitions even when formatted labels collide;
existing label-keyed lookups must not collapse those partitions.

The action universe is the current figure's distinct identities across its
supported layers and facet cells. Equal field/value identities in that
figure act together; there is no cell-local or layer-local isolation in
this cut. Separate figures never share pane state.

Legend entries gain an optional `identity`. PRODUCT-0044 §3's entry triple
still governs deduplication and printing; the identity is what an action
addresses. An entry without identity is drawn as today and ignores
presses. Union retains an actionable identity only when every contributor
to the printed triple has the same non-null identity. Conflicting identities
or identified/unidentified contributors make the entry non-actionable;
union retains that ambiguity through subsequent merges, never selecting
the first contributor. Its swatch stays unchanged. Underlying identities
remain individually addressable by actions. This preserves 0044's printed
union at the cost of pointer access for an ambiguous entry; changing printed
deduplication is a separate decision.

### 2. Four pane-tier actions, addressed by identity

`SeriesVisibilityAction` is `Hide(identity)`, `Show(identity)`,
`Isolate(identity)`, and `RestoreAll`. The hidden set lives in
`pane_override` beside the viewport domains, as `hidden_series:
Override<HiddenSeries>`, a set of identities. Every mutation is a
`PlotPaneMsg::SeriesVisibility(action)` through the surface's `update`,
re-resolves settings, and bumps `render_revision`, exactly as PRODUCT-0150
§4 requires. A kernel replacement — rerun, republication, layer change —
resets the set, as it resets the viewport; the set is session-scoped and
never persists, per PRODUCT-0133 §9. Nothing reaches Rune, the source
buffer, or a writeback; the inspector lists the hidden set in one
read-only row with `PaneOverride` provenance and offers no promote path.

For the current identity universe U and hidden set H, Hide(i) adds i,
Show(i) removes i, Isolate(i) sets H to U minus i, and RestoreAll clears H.
Unknown identities are ignored, including Isolate of an unknown identity;
repeated actions are idempotent and do not bump the revision if state is
unchanged. Isolate counts identities, not printed entries, and leaves
unidentified marks visible. Clearing H removes the hidden-series override.
Viewport changes preserve H; visibility changes preserve viewport domains.

### 3. Geometry, scales, domains, layout, and prepared data are fixed

Hiding changes ink and hits and nothing else. Axes, ticks, domains, the
colour assignment, legend geometry, facet layout, rasters' extents, and
the prepared data are byte-identical whether or not any series is hidden.
A hidden series draws no marks and answers no hit — its regions are absent
from PRODUCT-0155's projection where implemented — while its legend entry
remains, drawn in
a hidden state that keeps its label and position and hollows or dims its
swatch. A figure whose every mark-bearing series is hidden draws its frame,
axes, and legend; it does not refuse. Unidentified or out-of-scope layers
remain visible. Stacked area still accumulates every category and uses the
original normalization totals: hiding a lower band leaves a gap, never
moves an upper band or renormalizes percentages. Selection emphasis and
hover overlays obey visibility too; an already displayed hit is invalidated
when its series is hidden. The selection predicate itself is unchanged.

### 4. The legend is the gesture surface

A bare left press and release on the same legend entry with an identity
toggles it
between hidden and shown. A shift-press on an entry isolates it; a
shift-press on the only shown identity restores all. Releasing outside the
pressed entry, losing focus, or replacing the kernel cancels a pending
toggle. Shift acts once on press and consumes its release without toggling.
Other modifier combinations dispatch no visibility action. Presses on entries
without identity, on gradient and size sections, and on legend background
do nothing. The legend's entry rectangles come from the same
`legend_geometry` that draws them, exposed as a hit function beside
`render_legend`; no independent layout is computed. PRODUCT-0050 and
PRODUCT-0051 place the default legend in a gutter, but authored legends
can overlay marks. A press inside visible legend bounds is routed before
the 0150 gesture and 0151 brush, and its sequence is consumed even over a
non-actionable section or background. A gesture begun outside the legend
keeps its existing ownership through release. Hidden legends have no hit
rectangles. Plot gestures outside the legend retain their existing behavior.

### 5. One backend grouping seam, meaningful to SVG, a no-op elsewhere

`PlotBackend` gains `with_series(&mut self, group: SeriesGroup, f)`, whose
default calls `f` and nothing more, so iced, PNG, AVIF, and every test
recorder are unchanged. The SVG backend emits `<g data-series="sN">` around
the group with an opaque per-figure ordinal id, never the label, the field
name, or the value: raw category strings do not become selectors or
attributes. Kernels in this cut wrap identified series' drawing in the
seam. The ordinal registry is allocated from the complete prepared figure
before visibility is applied, in deterministic layer/cell/series traversal
order; equal identities reuse an ordinal across disjoint groups. Hiding
never renumbers another series; the registry includes hidden identities.
Unidentified marks are not grouped. Save/clip nesting and paint order are
preserved. The export result retains the ordinal-to-identity mapping in
memory for a later package to carry. No new package asset or field/value
serialization ships here; carriage is the web record's decision.

A rasterized scatter is the one exception, and it is a recording, not a
deferral of the surface. The desktop rasterizes a scatter only above the
PRODUCT-0140 gate, which today means more than one million points and
more than eight points per pixel, or when a spelling asks for it; the
raster accumulates density and averages colour across every series into one
image, and the SVG backend embeds that image as one `<image>` element.
Splitting it into per-series rasters would change the drawn density and
colour, which §3 forbids, and emitting a million circles as vector marks
would abandon the raster policy PRODUCT-0139 and PRODUCT-0140 settled for
the desktop and make the export unreasonably large. So the desktop actions
apply to a rasterized scatter by rebuilding the raster with the hidden
series excluded, its raster key carrying visibility so no stale pixels are
reused; the exported SVG shows that state as one ungrouped `<image>` whose
extent is fixed; and the browser cannot toggle a rasterized series without
re-rasterizing, which is a semantic recomputation in PRODUCT-0153 §5's
sense and is decided by the PRODUCT-0153 §6 probes, not here. Vector and
downsampled scatter, which is every scatter below the gate, are grouped.

### 6. Actions are the surface; the legend is one client of it

`SeriesVisibilityAction` addresses identities and never coordinates. The
legend press resolves an entry to its identity and dispatches the same
action a keyboard, a UAT harness, or an authorized agent would dispatch by
naming the series. Those clients are not built here, but nothing in this
record is shaped so that only a pointer can use it.

### 7. Families in this cut

Line, scatter with a categorical colour series, stacked area, radar, and
parallel coordinates. Bar and stacked bar groups are series in the same
sense and follow in the next plotting cut with their own gates; heatmap,
histogram, box, violin, pie, funnel, dial, candlestick, fill-between,
hexbin, and bubble have no series channel and are out of scope.

### 8. What this record does not decide

A Rune spelling for hidden series; persistence in sessions or workspaces;
filtering, recomputation, or any change of domain when a series is
hidden; coordination across figures; keyboard, command, or agent surfaces
beyond the action type's shape; the web projection, which is the next
web-lane record and names its own corpus row; new hover or selection
semantics beyond suppressing hidden-series hits and overlays; and changes
to the viewport gesture beyond legend press ownership in §4.

## Acceptance gates

1. **Identity is not the label.** Two series with the same printed label
   and different exact values — a formatted temporal series is the
   fixture — are distinct identities; hiding one leaves the other drawn.
   Cover null/unsupported values and canonical equality. Layer/grid union
   tests cover equal identities, conflicting identities with identical
   printed triples, and identified/unidentified collisions in either order.
2. **No ink, no hit.** With a series hidden, the SVG contains no path for
   it, the desktop never answers its hit (a visible overlapping mark may
   still answer), and no stale hover or selection overlay redraws it.
   Scatter's PRODUCT-0155 projection carries no region for it; showing it
   restores those regions. Families without projections remain unsupported;
   families without desktop hits acquire no new hit implementation.
   A rasterized scatter with a hidden series rebuilds its raster without
   that series' points and reuses no raster built with it, on the desktop
   and in the SVG alike; the SVG carries one ungrouped `<image>` for it.
3. **Everything else is fixed.** Axes, ticks, domains, colour assignments,
   legend geometry, and facet layout are byte-identical in the SVG before
   and after hiding, isolating, and restoring; prepared data is the same
   allocation.
   Compare surviving mark geometry too, including absolute and percent
   stacked area with a hidden lower band and facets/layers sharing an
   identity. Exercise all-hidden state and mixed unidentified marks.
4. **The legend shows state.** Every entry remains at its position with its
   label; a hidden entry's swatch is drawn in the hidden state; entries
   without identity are unchanged.
5. **Four actions, pane-tier, session-scoped.** Hide, show, isolate, and
   restore-all mutate only `pane_override.hidden_series`, bump the render
   revision when state changes, reset on kernel replacement, never persist,
   never reach the
   source buffer, and appear in the inspector as one `PaneOverride` row.
   Verify unknown and repeated actions are no-ops, restore removes the
   override, and viewport and visibility updates preserve one another.
6. **Legend gestures.** Press and release on an entry toggles; shift-press
   isolates and, on the only shown entry, restores; presses elsewhere in
   the legend and on gradient and size sections do nothing. Test gutter and
   authored overlay placement, cancellation, modifier combinations, and
   sequence ownership; retain existing viewport and brush regression tests.
7. **The seam is opaque and harmless.** The SVG groups each series under
   `data-series="sN"` with no label, field, or value in any attribute; the
   PNG export is pixel-identical with the seam present at the same visibility
   state; a recorder backend sees the same operations. Check stable ordinals
   across hide/show, shared identity in multiple cells/layers, and clip/save
   nesting. No category metadata is added to the published package.
8. **Actions address identities.** A test dispatches every action by
   identity with no pointer and observes the same state the corresponding
   legend sequence produces, including restore after isolation and show
   after hide. Ambiguous entries remain inert while direct actions work.
9. **Both heads** under the PRODUCT-0152 gate, and the full architecture
   suite green after formatting.

## Guardrails and stop conditions

1. If any family needs to change a domain, tick, or layout to hide a
   series, stop: that is filtering, and it is not this record.
2. If identity cannot be minted from a source column for a family, that
   family's entries have no identity and are not actionable; do not
   substitute the label.
3. If a legend press cannot be told apart from the 0150 or 0151 gestures
   without a new modifier, stop and report rather than steal one.
4. Recheck the record number before the plan commit; two commits, plan
   then one `impl:` cut, corrections amended under review, no push until
   the cut is called stable.

## Risks

- **Identity minting drifts from partitioning.** The series set and the
  identity set must be the same partition. Mitigated by minting identity
  inside preparation, where the partition is made, never afterwards.
- **A crowded legend is a poor gesture surface.** The dogfood's own case,
  twenty series, is where isolation matters most and where entries are
  smallest. Mitigated by isolate-with-shift, so one gesture reaches the
  wanted state; a search or keyboard surface is a later record.
- **The seam tempts kernels to group other things.** Mitigated by a guard
  that the seam is entered only with a `SeriesGroup` minted from a series
  identity.
- **The raster exception is read as a precedent.** A published rasterized
  scatter is the one figure in this cut a browser cannot toggle. Mitigated
  by naming the boundary: it is the density raster, above a gate this
  record does not move, and the web record inherits it as a corpus caveat
  rather than a reason to compute density in the browser.

## Forward

The web-lane successor projects this vocabulary: the package carries the
ordinal-to-identity mapping, the shell's legend dispatches the same four
actions against the SVG groups with fixed scales, and PRODUCT-0155 §6's
"never mutate an SVG mark" narrows to "never compute geometry", since
toggling a group's visibility computes nothing. That record names the
*category-visible published dashboard* corpus row. The command-surface
posture, when written, inherits §6 as its first worked example.
