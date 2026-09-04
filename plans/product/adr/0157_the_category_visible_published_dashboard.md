# ADR 0157: the category-visible published dashboard

Status: accepted 2026-09-05 after the browser gate was recorded in
`plans/product/impl/0157_browser_acceptance.md`, with the Alt-Tab check
observed by the operator on the direct-file package only and the desktop
tessellation stall left open on its own record; proposed 2026-09-04,
revised the same day after review found an
already-hidden series exported without the marks a browser would need to
show it again, a per-row grouping flag that could not express a figure
mixing grouped marks with a rasterized occurrence of the same identity,
the raster section contradicting the manifest on whether a table exists,
and the registry promised as a source for labels it does not hold; and
revised again after review found a covered figure over the table bound
would have cleared its hidden set with no snapshot to carry it.
Publication lane, third record; it names and advances one new web corpus
row, *category-visible published dashboard*, and no other. PRODUCT-0156
gave the desktop a series identity distinct from its label, four pane-tier
visibility actions, fixed geometry under hiding, a legend gesture surface,
and one backend grouping seam whose SVG form is an opaque per-figure
ordinal; its export keeps the ordinal-to-identity mapping in memory and
ships no package field. This record decides how that mapping crosses into
the package, what a published figure's ink is when the desktop had hidden
something, what a browser legend may do, how packaged hits obey
visibility, what the byte-pinned script may now touch, and what a figure
with a rasterized occurrence remains. It decides no persistence, no
cross-figure coordination, no selection material, no recomputation, and
no change to any desktop answer.

## Context

### What the package carries and lacks

PRODUCT-0154's shell inlines each `figure.svg` and, since PRODUCT-0155,
embeds `hits.json` beside it as a non-executable data element; the one
executable script is `inspect.js`, byte-pinned by content hash in the
content security policy, permitted only to test packaged primitives and
position a tooltip, and forbidden to inspect or mutate any SVG mark.

Since PRODUCT-0156 the SVG groups each identified series' marks under
`<g data-series="sN">` (`plot/export.rs`), ordinals allocated from the
complete figure in deterministic layer/cell/series order so that equal
identities in different cells or layers share one ordinal and hiding never
renumbers. `SvgFigure` carries the `SeriesRegistry` beside the SVG bytes,
and publication writes only the bytes. The legend inside the SVG is drawn
by `render_legend` from `legend_geometry`, and the desktop's press routing
reads the same geometry through `legend_hit_geometry`, which yields one
rectangle per entry with the entry's optional identity. Nothing in the SVG
names a legend entry, and the swatches are not grouped.

`hits.json` regions carry a paint ordinal and a hit index, not a series.
A browser that hid a series would still answer its hover.

### What the export contains when the desktop has hidden a series

Publication renders through the widget's resolved settings, which carry
the pane's `hidden_series`; PRODUCT-0154 §5 rebuilds a selected figure
unfiltered before publication, but nothing clears the hidden set. A
hidden series therefore leaves the desktop with no marks and no regions,
exactly as PRODUCT-0156 §3 requires on the desktop, and a browser that can
only toggle classes could never show it. The publish moment needs a rule.

### What the registry holds, and what it does not

The `SeriesRegistry` holds exact identities in ordinal order and nothing
else. Printed labels live where preparation mints them — line's series
data, scatter's series, stacked area's assignments, radar and parallel
rows — as an identity beside a label, and reach the legend as entries
whose union under PRODUCT-0044 §3 keeps an identity only when every
contributor agrees, discarding it otherwise. A label can be read at
preparation, next to the identity it was formatted from; it cannot be
recovered from the legend after union, and matching by label would
reintroduce the confusion PRODUCT-0156 §1 removed.

### What the mapping is, and what disclosure it would be

A `SeriesIdentity` is a `FieldRef` — name, semantic type, scalar type —
and an `ExactScalar`, PRODUCT-0151's vocabulary with temporal units and
zones and decimal scale. PRODUCT-0155 §4 held that exactness across the
wire is specified by the record that first sends a selection back, and
published no field reference and no exact scalar. Nothing a browser does
in this record needs the exact value: the ordinal is the identity within
the package, because PRODUCT-0156 §5 already made equal identities share
an ordinal. What the browser needs is the ordinal, a label to show, the
legend rectangle that addresses it, and whether the figure's marks can be
addressed at all.

### What the desktop settled and the browser follows

PRODUCT-0153 §5: the web consumes the desktop's vocabulary and never
invents one. The four actions, their algebra, the legend gestures, and the
fixed-geometry rule are settled by PRODUCT-0156 and are consumed here
verbatim. PRODUCT-0153 §5 also distinguishes a transform, which needs no
plot logic, from a semantic recomputation. Toggling a group's visibility
computes nothing. PRODUCT-0156 §5 recorded the one exception: a scatter
above the PRODUCT-0140 gate is one density raster across every series,
exported as one ungrouped `<image>`, and a browser cannot toggle it
without re-rasterizing, which is recomputation and belongs to
PRODUCT-0153 §6's probes. A layered or faceted figure can hold such a
raster beside grouped marks, and the two can share an identity.

## Decision

### 1. A figure is actionable only when every identified occurrence is grouped

A figure's marks are **covered** when every occurrence of every identified
series, across its layers and facet cells, is drawn inside the SVG series
seam. One rasterized occurrence — a scatter above the PRODUCT-0140 gate or
a spelling that asks for the raster — leaves the figure uncovered, even
when the same identity is grouped elsewhere in the figure, because hiding
only the grouped occurrences would break PRODUCT-0156 §1's rule that
equal identities act together. Coverage is a property of the whole figure,
computed by the export from the kernel's own render-mode decisions, never
inferred from the SVG text. A figure is **actionable** when it is covered,
has at least one identity, and is within the table bound of §2; every other
figure is **inert** in the browser, its legend dispatches nothing, and it
publishes with the desktop's visibility applied (§3). An over-bound figure
is inert for the same reason a raster one is: without a table there is no
snapshot to carry, so the desktop's hidden set must stay in the ink.

### 2. The package carries a series table, not the identity

Each figure with at least one identified series gains
`figures/<id>/series.json`, canonical JSON, schema version 1, listed in the
manifest and embedded in the shell beside the SVG exactly as `hits.json`
is, under the same escaping and the same polyglot obligations. It holds:

- the `viewBox` size;
- `actions`: `available`, or `unavailable` with the reason `raster`;
- `initially_hidden`: the ordinals the desktop had hidden at publish (§3);
- `series`, ordered by ordinal, one row per registry identity: the
  ordinal; the label preparation formatted for that identity; the source
  field's label and semantic type in PRODUCT-0154's vocabulary; and the
  partition value as a PRODUCT-0155 §2 hit value, the tagged DTO whose
  display string is authoritative;
- `legend`: the legend's bounds and one rectangle per legend entry in
  `viewBox` units, in drawing order, each with the ordinal it addresses or
  none when the entry has no identity or an ambiguous union identity.

It carries **no** `ExactScalar`, no scalar type, no temporal zone or
decimal scale, and no selection candidate: the ordinal is the handle, the
value crosses only as the display-bearing DTO PRODUCT-0155 already ships,
and exactness across the wire stays the navigable rung's decision.

Two sources feed the table and neither is reconstructed from the other.
The registry `SvgFigure` already holds is the sole authority for ordinals
and row order. Labels and field metadata come from preparation: `PlotKernel`
gains `series_labels`, returning every prepared series as an identity
paired with the label preparation formatted for it, in the same traversal
order the registry was built from, before any legend union; the export
joins it to the registry by exact identity equality and never by label.
Two identities that print alike are two rows with two ordinals and equal
label strings; formatting may collapse the display value as well, and that
is not a defect. An ambiguous union entry has no identity and addresses no
ordinal, while each identity it printed keeps its row.

The legend rectangles come from `legend_hit_geometry` at the export
bounds, the same geometry that draws the legend, so the desktop and the
browser address one layout; no second layout is computed. Hidden legends
yield no rectangles.

The manifest's figure entry gains `visibility`, which separates whether a
table exists from whether actions are available: `actionable` with the
series count; `inert` with the series count and the reason `raster`, the
table present; `absent` with a reason — no identified series, or a table
over the bound with the observed count — and no table. The bound is
4,096 series; a figure over it publishes without a table and the rest of
the dashboard publishes.

### 3. An actionable figure publishes complete, with the desktop's hidden set as initial state

An actionable figure is exported with an empty hidden set: every series'
marks, legend swatch, and hit regions are in the package, so a browser can
show what the desktop had hidden. The desktop's hidden set at the publish
moment crosses as `initially_hidden`, and the shell applies it as the
figure's starting state before the first paint, so what the viewer first
sees is what the publisher saw. This is a snapshot of the publisher's
view, taken once; it is not persistence. Nothing a viewer does afterwards
is written anywhere, and a reload returns to the snapshot.

An inert figure publishes as PRODUCT-0156 §5 left it: the desktop's hidden
set applied, a hidden series absent from the ink and the projection, a
raster showing the state at publish, and, when a table exists, its
`initially_hidden` recording that state for the reader. A figure over the
bound has no table and carries the state in its ink alone; a hidden series
is never exposed because the snapshot had nowhere to go. The browser cannot change it and does not pretend
to.

### 4. The browser dispatches the desktop's four actions

The shell's legend is the SVG's own legend. The script resolves a pointer
into `viewBox` units by PRODUCT-0155 §5's arithmetic and tests it against
the packaged legend rectangles: a bare press and release on the same
actionable entry toggles its ordinal between hidden and shown; a
shift-press isolates it, or restores all when it is the only shown
ordinal; release elsewhere, pointer cancellation, and loss of window focus
cancel a pending toggle; presses on inert entries, on the legend
background, and with other modifiers do nothing. The per-figure hidden set
is a set of ordinals held in memory. The algebra is PRODUCT-0156 §2's
verbatim over the ordinal universe the table declares: isolate is the
universe minus the pressed ordinal, unknown ordinals are ignored, repeated
actions are no-ops. Figures never share state. An inert figure's legend
dispatches nothing.

### 5. Hiding is one class on a group, never geometry

The shell declares two rules: a group carrying the hidden class draws
nothing, and a legend swatch group carrying it draws at a quarter of its
opacity, the desktop's own hidden-swatch factor. The script toggles that
one class on the elements addressed by `data-series="sN"` and, for the
swatch, `data-legend="sN"`; it changes no other attribute and reads no
geometry. Axes, ticks, domains, colours, legend positions and labels,
facet layout, raster extents, and every path's data are the SVG's bytes,
untouched. Stacked area keeps its gap, because the bands were computed on
the desktop under PRODUCT-0156 §3 and the browser only stops painting one
of them.

To address the swatch, `PlotBackend` gains `with_legend_entry(group, f)`,
default calling `f`, which the legend renderer enters around the entry's
sample only — the swatch or the line sample, never the label text — for
each entry with an actionable identity, and which the SVG backend emits as
`<g data-legend="sN">` with the same ordinal the marks carry. It is the
same opaque ordinal, carries no label or value, and is a no-op on iced,
PNG, AVIF, and every recorder, so desktop pixels and the PNG licence are
unchanged.

PRODUCT-0155 §6's "inspect or mutate any SVG mark" narrows to: the script
may toggle one named class on elements it addresses only by the packaged
ordinals, and may never read, compute, or write geometry, colour, or any
other attribute.

### 6. Packaged hits obey visibility

A region gains an optional `series` ordinal, set by a kernel whose region
belongs to an identified series — scatter with a categorical colour series
in this cut; heatmap, bubble, and box plot carry none. The shipped
resolver skips a region whose series is hidden before any containment
test; the skip is a counted operation. PRODUCT-0155 gate 2's per-region
budget becomes three operations — one visibility check, one containment
test, one distance compare — so the bound is 98,304 operations per pointer
event over 32,768 regions, still counted by the engine-executed asset.
With nothing hidden the resolver's answers are byte-identical to today's.

### 7. The desktop is unchanged

No desktop hit, action, gesture, or pixel changes. The additions on the
plot side are the legend-entry seam of §5, a no-op everywhere but SVG; the
labelled series source of §2, which reads what preparation already holds;
the series ordinal on scatter's hit regions, which the desktop hit path
already suppresses by identity and which the projection now labels; and
an export-time render with an empty hidden set for actionable figures,
which the desktop pane never sees.

### 8. What this record does not decide

Persistence of hidden state in the URL, storage, or a package beyond the
publish-moment snapshot; cross-figure coordination or linked highlighting;
keyboard or agent dispatch in the browser; the bar and stacked-bar
families, which follow their desktop cut; any coordinated hover emphasis;
exact scalars across the wire; and any browser recomputation, including
the raster.

## Acceptance gates

1. **The table is a handle, not a disclosure.** `series.json` for a figure
   with a formatted temporal series carries two rows with distinct
   ordinals, equal labels, and no exact-scalar field, scalar type, zone,
   or candidate, whether or not their display values also collide; the
   architecture guard that keeps selection material out of the package
   refuses a specimen row carrying any of them. Equal identities across
   facet cells and layers yield one row; an ambiguous union entry yields
   a legend rectangle with no ordinal while its identities keep their
   rows. Labels are joined by identity: a fixture whose two identities
   swap labels between publishes moves each label with its ordinal.
2. **Determinism.** Publishing twice yields byte-identical tables; a relabel
   changes the label field and no ordinal, rectangle, or value.
3. **One legend layout.** Every packaged legend rectangle equals the
   rectangle `legend_hit_geometry` yields on the desktop at the export
   bounds, in `viewBox` units, for gutter and authored-overlay placements.
4. **Complete ink, snapshot state.** A figure published with one series
   hidden on the desktop carries that series' marks, swatch group, and
   regions, lists its ordinal in `initially_hidden`, and the shell applies
   the hidden class to it before first paint; the same figure published
   with nothing hidden differs only in that list. The desktop pane's
   hidden set is unchanged by publishing.
5. **Gesture parity through the shipped asset.** The state algebra and the
   legend resolver are pure functions of the asset, executed in the
   `boa_engine` harness of PRODUCT-0155 gate 1. Press sequences — toggle,
   shift-isolate, shift on the only shown entry, release elsewhere,
   pointer cancellation, window blur between press and release, press on
   an inert entry, on the background, and with other modifiers — over the
   packaged legend produce the same hidden set of ordinals the desktop's
   `legend_action` produces over the same presses, from an empty state and
   from a non-empty initial state, unknown and repeated actions included.
   Zero disagreements.
6. **Hits obey visibility.** For a categorical scatter, one thousand pointer
   positions under three hidden sets — none, one series, all but one — the
   asset's hit equals the desktop's structured hit under the same
   `hidden_series`, including none. With nothing hidden the projection's
   answers are unchanged from PRODUCT-0155's. The operation count is
   asserted under 98,304.
7. **Ink changes by class alone.** The shell's CSS declares exactly the two
   rules of §5; the SVG carries `data-legend` groups around the sample of
   every actionable entry, around no label text, and around no inert
   entry, with only the ordinal in the attribute; the desktop PNG and the
   recorder operation stream are identical with the seam present. The
   asset contains no attribute write other than the class toggle and no
   geometry read — a guard refuses `setAttribute`, `getBBox`, `getCTM`,
   `getTotalLength`, `getPointAtLength`, and path-data access, with a
   positive-control specimen — and the script hash is re-pinned.
8. **Coverage decides, and mixed figures are inert.** A scatter forced
   above the raster gate publishes one ungrouped image, a table with
   `actions: unavailable`, legend rectangles without ordinals, and a
   manifest entry of `inert` with reason `raster`; the asset dispatches
   nothing for its legend. A layered fixture that draws one identity as
   grouped line marks and the same identity inside a rasterized scatter
   is inert as a whole, and its hidden-on-the-desktop series is absent
   from both the ink and the projection. A figure with no identified
   series and a figure over the 4,096 bound each publish with `absent`
   and their reason, and no table; the over-bound fixture is published
   with one series hidden on the desktop, and that series' marks and
   regions are absent from the package while its legend entry stays drawn
   in the desktop's hidden state, as PRODUCT-0156 §3 requires.
9. **Both heads** under the PRODUCT-0152 gate, and the full architecture
   suite green after formatting.
10. **Browser acceptance.** The operator, on the semantic showcases in the
    UAT browser: hides, isolates, and restores series from the legend on
    line, stacked area, radar, parallel coordinates, and a categorical
    scatter; opens a dashboard published with a series hidden and sees it
    hidden, then shows it; sees the stacked-area gap rather than a
    renormalized band; sees hover answer nothing over a hidden series and
    answer again after restore; sees a hostile label survive as text; and
    sees the rasterized scatter's legend do nothing. Recorded in the
    implementation notes.
11. **The corpus moves.** The *category-visible published dashboard* row is
    added between the inspectable and navigable rows and becomes
    executable citing the gates, naming the raster caveat and the bar
    families as pending on their desktop cut; no other row moves.

## Guardrails and stop conditions

1. If the browser needs any geometry, colour, or attribute beyond one class
   to hide a series, stop; §5 is the boundary and PRODUCT-0153 §5 names
   the alternative as recomputation.
2. If legend parity under gate 3 needs a second layout in either the
   package writer or the script, stop: the geometry has forked.
3. If a label or field can only be found by matching printed strings, stop:
   the preparation source of §2 is missing for that family and must be
   added there, not worked around.
4. If any consumer in this cut needs the exact scalar, stop and report; it
   is the navigable rung's material, and this record ships the display
   DTO only.
5. Recheck the record number before the plan commit; two commits, plan
   then one `impl:` cut, corrections amended under review, no push until
   the cut is called stable.

## Risks

- **A hidden series that still answers hover would read as a defect of the
  whole rung.** Mitigated by gate 6's parity under hidden sets, not just
  by skipping.
- **Two renders per actionable figure at publish.** The complete export of
  §3 is a second resolution of the same settings with the hidden set
  cleared; PRODUCT-0139 §6 already makes that resolution the authority, so
  the marks cannot disagree with the desktop's. Cost is one render per
  figure, which publication already pays.
- **Crowded legends are small targets in a browser too.** Isolate on
  shift-press is the desktop's own answer; a search or keyboard surface is
  a later record on both surfaces.
- **The script's permission list grows.** It grows by one verb, toggling a
  class on an ordinal-addressed element, and the guard gains the reads and
  writes that verb must never become. The hash pin means any drift is a
  visible byte change.
- **Inertness for a whole mixed figure is blunt.** It is also the only rule
  that keeps equal identities acting together. A reader meets the reason
  three times — the manifest, the table, and the corpus row — before
  blaming the viewer.

## Forward

With categories toggled in the browser, the next observation is whether
people reach for the desktop's viewport — pan and zoom — or for selection.
Either is the navigable rung, where PRODUCT-0151's exactness first crosses
the wire and where the series table of §2 would gain the exact value it
deliberately lacks today. Bar and stacked bar arrive here when their
desktop cut lands. Re-rasterizing a scatter in the browser remains the
first concrete demand for PRODUCT-0153 §6's probes.
