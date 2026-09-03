# The selection census — what PRODUCT-0133 stage 3b lands on

**Status:** census, 2026-09-02, revised the same day after review. Not a
record and not a plan. It is the prerequisite for drafting the selection
record that PRODUCT-0133 §3–§8 and §11 describe and that the gesture census
(`0133_gesture_census.md` §1, §10) split off as stage 3b. It decides nothing
that the contract has not already decided; where the record must decide it
says so.

Traced at `d203b0759` (the PRODUCT-0150 close). No measurements were taken;
§13 names the ones the record's stage 0 must bank.

## 1. What stage 3b owes, and what is already in hand

The 0133 implementation plan's stage 3 remainder is the typed selection
algebra and the context lifecycle; stage 4 is the grid and export consumers.
PRODUCT-0150 discharged the pane-override viewport and the arbitration half
of stage 3: it reserved Shift-drag for brush and left Shift-press
deliberately unhandled.

Two of the contract's prerequisites are shipped and exercised:

- **Tokens.** `ArtifactToken` (`result_artifact.rs:32`) is an opaque
  `NonZeroU64`, minted only by `ResultArtifact` constructors, inherited by
  `from_polars_derived` (`:243`), and already guarded by two consumers:
  `RasterSubject.publication` (`plot/raster.rs:29`) and hexbin's
  `source_token` (`kernels/hexbin_viewport.rs:131`, `matches` at `:665`).
  `ResultRegistry::artifact_token` (`result_registry.rs:146`) is marked
  `#[allow(dead_code)] // Selection-context consumption lands in the next stage.`
- **The algebra text.** §6 is complete as prose: OR within a field, AND
  across fields and views, `subtract` only for nominal/ordinal.

The third prerequisite, structured hits, is shipped as a **presentation**
substrate and is not yet a selection substrate (§2).

**Finding.** Nothing selection-shaped exists in code. No `SelectionIntent`,
context, predicate, mask, emphasis, or input identity symbol appears
anywhere under `crates/`. Stage 3b has four new seams — a selectable hit
substrate (§2), a product-owned input identity (§3), a rebuild recipe
beside each plot surface (§4), and an emphasis projection contract (§7) —
before the algebra and lifecycle can be built on them.

## 2. The hit substrate is presentation-only

`PlotHit` (`plot/common.rs:1881`) carries `fields: Vec<HitField>` where
`HitField { label: String, semantic_type: SemanticType, value: HitValue }`
(`:1859`). Nothing in it names a source field.

- **Labels are presentation.** Scatter and line label their two fields
  `"X"` and `"Y"` (`kernels/scatter.rs:319`, `:328`; `kernels/line.rs:45`,
  `:54`). Histogram emits synthetic `"Range"` (an `Interval`), `"Count"`,
  and `"Y-Value"` (`kernels/histogram.rs:98–120`), none of which is a
  column. Hexbin emits `"Q"` and `"R"` lattice ordinals (`kernels/hexbin.rs:261`)
  and a count. A predicate minted from these labels would name fields the
  relation does not have.
- **Values are projections.** `axis_hit_value` mints temporal and
  quantitative values from the painted `f64` (`scatter.rs:322`), and
  `HitValue::Temporal { value: f64, display }` (`common.rs:1786`) carries no
  unit, zone, or exact integer. PRODUCT-0145 established that a nanosecond
  `Datetime` exceeds the f64 exact-integer range and that adjacent
  timestamps collapse under projection; it kept validation on the typed
  source values and projection for paint only. A temporal click-select
  minted from the hit would repeat the defect 0145 routed around.
- **Derived marks have no row.** A histogram bin, a hexbin cell, a box, a
  violin band, and a pie slice denote aggregates. Some map back to a source
  constraint (a bin is an interval on its source field); some do not (a
  hexbin cell is a lattice coordinate; `"Y-Value"` is a computed height).
- **The hit dies at the widget.** `PlotWidget::update` maps
  `hit.to_string()` at `common.rs:4674` and `:4701`, publishes
  `UpdateHover(Option<String>)`, and `HostPlotState.hovered_info`
  (`gui/plot_pane/state.rs:67`) stores prose. No pane sees `fields`.

**Finding.** The record needs a corrective hit substrate: presentation
fields stay as they are, and a hit gains a separate, optional set of
**selectable constraints** carrying exact source-field provenance and
exact owned values (typed temporal integers with unit and zone, not the
projected f64). A kernel admits selectability per field; derived fields
that cannot be expressed as a constraint on a source field are explicitly
non-selectable. Click-select therefore has a per-kernel admission list,
not a blanket scope, and the list is a deliverable of the record.

## 3. Publication identity: the token exists, the input identity does not

§4 keys contexts on the **input identity** of the relation a view
originates from. §3 carries both `input_ref` and `artifact_token` on every
intent. The contract's Rejected section rules out keying contexts on the
resolved relation expression, and the stage 1 plan rules out a
relation-expression hash for the token. The census therefore does not
offer `TransformedRuneRelation.origin` (`rune_relation_envelope.rs:777`, a
source-text concatenation from `rune_view_compiler.rs:865`) as a
candidate: it is exactly the rejected key.

**What is decided.** The input identity is a product-owned opaque value
minted at the logical publication binding, preserved across republication
of that binding, and inherited through derived artifacts the way the token
already is (`from_polars_derived`). The census maps where the binding lives
on each path so the record can name the minting site:

| path | binding today | what carries it forward |
| --- | --- | --- |
| annotated SQL | `-- @result <name>` in `ResultRegistry`, keyed globally by name (`result_registry.rs:60`); buffer and connection ride as provenance, not key (`:66`) | the name |
| unannotated SQL | none; `RESULT_PANEL` replaced in place by `begin_query` (`editor.rs:1339`) | the producer buffer |
| Rune `views([...])` | `(BufferId, LocalViewIdentity)` per sink in `view_panel_ids` (`editor.rs:1458`); the relation itself is scheduler-keyed only (`gui/state.rs:4389`) | one identity per relation, shared by all its sinks |
| notebook | `MarkdownCellIdentity` (`editor.rs:472`) | the cell |

- **No panel holds a token or an artifact.** `TableArtifact`
  (`editor.rs:3461`) is a bare `DataFrame` clone; `HostPlotState` holds a
  kernel and settings; `HostPlotExport` (`editor.rs:3436`) and
  `NotebookPlotSurface` (`editor.rs:544`) likewise. `set_query_plot`
  (`editor.rs:1856`) and `set_query_result` (`:1852`) take a kernel and a
  frame; the `Arc<ResultArtifact>` is dropped at `gui/state.rs:4240` and
  `:5232`.
- **Sibling views share a token at the message layer and lose it at the
  factory.** Every sink of one `views([...])` carries the same
  `Arc<ResultArtifact>` (asserted at `gui/state.rs:4434`). The Rune plot
  path then routes through `PlotGridKernel::from_grid_with_figure_grid`
  (`plot/grid.rs:480`) and `from_styled_grid_with_figure_grid` (`:557`),
  both of which hardcode the *minting* factory. Only the layered path
  threads the source (`gui/state.rs:538`). Internally consistent today,
  because a kernel is the only holder of its own token; a defect the
  moment a context compares a plot's token against its publication's.
  Architecture tests pin the non-grid path (`tests/architecture_test.rs:5397`)
  but not the grid path.

## 4. A predicate is a rebuild, and the rebuild recipe is not retained

§7 says a view resolves against every predicate in its context except its
own; the stage 3 stop condition says the first implementation operates
through the existing materialized relation seam.

- **No filter capability exists.** No `DataFrame::filter`, mask, or
  predicate call appears in `plot/` or the relation layer. The only
  row-subsetting operation is faceting: `PlotGrid::value_facet`
  (`plot/grid.rs:309`) partitions by a field and hands each `GridCell` its
  own `Arc<DataFrame>` (`:105`).
- **Kernel construction is separable from execution**, but it needs more
  than rows. The three construction sites and their inputs:

  | site | needs |
  | --- | --- |
  | `build_resolved_plot_task` (`gui/state.rs:307`) | `ResolvedPlotSpec`, artifact, width, height |
  | `build_facet_resolved_plot_task` (`:405`) | the above plus facet field, `FigureGridLayoutSettings`, optional `LayerSettings` |
  | `build_facet_layered_plot_task` (`:480`) | the above plus the layered composition |

  A kernel cannot be reversed into any of these. `HostPlotState` retains
  none of them. `NotebookPlotSurface` retains `composition: PlotComposition`
  (`editor.rs:545`, the layer list only) and the cell's artifact lives in
  `notebook_cell_data` (`:472`); neither holds the resolved spec, the facet
  configuration, or the layer settings.
- **A rebuild re-prepares.** Scatter re-rasterizes, hexbin re-bins, every
  kernel re-projects. The token-guarded consumers refuse the rebuilt
  kernel's products unless the rebuild inherits the publication token
  (`create_plot_from_artifact`, `factory.rs:60`), which is the §3 grid gap
  again. Cost is the settle-frame cost PRODUCT-0149 already prices; a
  predicate change is a settle, not a motion.
- **Export follows the kernel** (`editor.rs:1882`). §10 is satisfied for
  free if the filtered kernel is what the panel holds; export needs no seam
  change and must not get one.

**Finding.** The record needs an explicit **rebuild recipe** beside every
plot surface: the artifact, the resolved specification or composition, the
facet and grid configuration, and the settings the factory consumes.
Applying a predicate is then "subset the projection, run the recipe," with
no engine round trip, no new protocol, and no adapter branch. The stop
condition is not triggered. `retained_rune_relations` (`gui/state.rs:80`)
and `notebook_cell_data` show the retention pattern; the per-surface
linkage does not exist.

## 5. Invalidation: the rule is decided, the events need mapping

§8 and the stage 1 sequence decide the edge: predicates bound to a
publication clear at that publication's **retirement**, before the
replacement becomes observable. Retirement is a dispatch-side event. The
census maps each path's retirement and visibility events so the record can
bind the rule to them:

| path | retirement (clear here) | replacement visibility |
| --- | --- | --- |
| annotated SQL | `ResultRegistry::begin` → `bound.remove(name)` (`result_registry.rs:73`), at dispatch | `Message::RunResult` → `begin_query` → `set_query_result` (`gui/state.rs:4239`) |
| unannotated SQL | no retirement event exists; dispatch is `run_code` (`gui/state.rs:966`) | the same `begin_query` swap (`editor.rs:1365`) |
| Rune | `prepare_rune_view_panels` (`editor.rs:1370`), called before dispatch at `gui/state.rs:1152` | `RunRuneViewSetResult` (`gui/state.rs:4378`) under the buffer-revision guard (`:4392`) |
| notebook | `begin_markdown_cell` (`editor.rs:1251`), called at dispatch (`gui/state.rs:3521`); removes the prior artifact and output before execution | the surface swap under the same `MarkdownCellIdentity` |

Between retirement and visibility the old rows stay on screen without
their predicate. That is the contract's chosen behaviour, not a gap: the
gesture record's incarnation discipline handles the analogous window for
viewport stamps. Three paths already have a dispatch-side retirement event
the record can bind to. The one path with none is unannotated SQL, which
the record must give one.

## 6. Brush geometry and typed constraints

- **Continuous inverse exists only for cartesian.** `pixel_to_cartesian`
  (`common.rs:885`, `#[allow(dead_code)]`) answers `PlotLayout::Cartesian`
  only. `ViewportGeometry` (`common.rs:2254`) is the gesture-path carrier
  and already yields ranges per layout family: both axes for cartesian, one
  for `CategoricalX`/`CategoricalY`, none for `CategoricalXY`, `Parallel`,
  `Radial` (`:2279`). A rectangle brush over a continuous axis is two corner
  inversions through the geometry PRODUCT-0150 uses for pan, under the same
  press-time-transform rule.
- **No band inverse.** Nothing maps a pixel to a category index; bar
  (`bar.rs:152`) and violin (`violin.rs:152`) re-derive bands ad hoc. A
  categorical brush needs a transformer method or must refuse.
- **Temporal constraints need a new typed inverse.** The layout has what
  §6 requires: `AxisType::Datetime { unit, zone }` (`common.rs:41`) and
  `TemporalAxisDomain` (`:60`) with exact first and last ticks. But
  `pixel_to_cartesian` yields an f64, and an f64 cannot mint an exact
  endpoint (§2, the 0145 collision). The inverse for a temporal axis is
  therefore normalized pixel position interpolated over the exact domain
  in the domain's integer unit, producing an exact, zone-bearing interval.
  This is a new transformer method, not a wrapper over the f64 inverse.
- **Facet provenance is ready.** The grid composite stamps
  `HitFacet { field, value }` (`grid.rs:1280`); a brush inside a cell is
  localized through the same cell-bounds arithmetic (`grid.rs:1219–1287`)
  and conjoined with the facet value, as §7 requires. `HitFacet` carries
  the facet value as a string; the record must decide whether the facet
  constraint is minted from the partition column's typed value instead.

**Finding.** Brush scope per layout family should be stated as
PRODUCT-0150 §2 stated viewport scope: cartesian both axes, categorical the
continuous axis only, others refuse. This matches the shipped inverse
geometry exactly.

## 7. Emphasis needs a projection contract

§7 renders a view's own selection as emphasis. There is no concept of it:
no `highlight`, `selected`, or opacity-by-state channel in `PlotSettings`,
and every production draw path passes `Cursor::Unavailable` into `plot`
(`common.rs:4126`, `:4139`, `:7446`). Violin's hovered median dot
(`violin.rs:152`) is the only cursor-driven mark emphasis and is
unreachable.

`PlotHit` carries no anchor: no pixel point and no data-space point
(`common.rs:1881`). A stored screen-space ring or rectangle is therefore
false after the next pan, zoom, band reflow, or filter rebuild. Two cases
separate cleanly:

- **Continuous cartesian brushes** can retain typed domain geometry (the
  minted interval) and reproject it through `ViewportGeometry` on every
  frame. This is overlay-only, needs no kernel participation, and survives
  pan, zoom, reflow, and rebuild.
- **Click emphasis** on categorical, radial, and aggregate marks has no
  reprojectable geometry unless the kernel supplies it. That requires a
  projection contract: a kernel maps a selectable constraint back to mark
  geometry in the current transform.

**Finding.** Emphasis and click-family scope are one decision (§12 item 6).
PRODUCT-0133 §7 requires the source view to render its own selection as
emphasis, so a family either supplies the projection contract or refuses
click-selection; admitting a family without emphasis would need an
explicit 0133 amendment, which this census does not propose. Mark-level
emphasis (dimming unselected marks inside every kernel and the raster
substrate) is a hover-migration-sized item and belongs to a later record.

## 8. Gesture arbitration and the operation vocabulary

| chord | plot canvas | table |
| --- | --- | --- |
| bare left press / drag | pan (0150) | select / extend |
| bare double-click | viewport reset (0150) | free |
| Shift + left press / drag | **declined, unhandled** (`common.rs:4523`, pinned at `:3656`) | extend (`table.rs:541`) |
| Ctrl + left / double | settings inspector on host panels (`common.rs:3082`) | free |
| Alt + left press / drag | **enters the pan arm**: the guard excludes only Ctrl and Shift (`common.rs:4523`) | free |
| wheel, no Ctrl | zoom; **Shift not excluded** (`:4610`) | scroll |
| Escape | free | clear visual selection (`table.rs:691`) |

Shift-drag identifies the brush chord. It does not yet identify the
operation: §6 needs `replace`, `add`, and `subtract`, and every surface
needs `clear`, or a selection is sticky until republication. Constraints
on the mapping:

- Ctrl is taken by the settings gesture on host panels, and its tracking
  arm consumes any Ctrl-press regardless of Shift (`common.rs:3086`), so
  Ctrl+Shift is not free there.
- Alt-left is not free in the plot canvas: the pan guard admits it. Any
  Alt-bearing selection chord requires amending PRODUCT-0150's press
  arbitration to exclude Alt, or the operation modifiers must live on the
  Shift chord alone.
- The wheel guard excludes an active drag but not Shift; a wheel during a
  brush would zoom under the rectangle. The brush should suppress wheel as
  the drag does.
- A Shift-press that captures needs the `FocusPane` compensation the pan
  press already has (`editor.rs:2391`).
- Table Shift-click means positional extend today; it cannot also mean
  `add`. Table Escape clears the rectangle only; it must also clear the
  published predicate.

Every reachable mapping therefore either amends the pan guard or keeps
all three operations under Shift. The census offers two reachable
candidates and leaves the choice to the record (§12 item 8): (a) amend the
pan guard to bare-left only, then Shift `replace`, Shift+Alt `add`, Alt
`subtract`; or (b) leave pan untouched and put the operation on the
release: Shift-drag `replace`, Shift+Alt-drag `add`, Shift+Alt-click on a
selected mark `subtract`, with Alt pressed after the Shift-press so the
pan guard never sees it. Escape on the focused view is `clear` under both.
`subtract` refuses with a status message on quantitative and temporal per
§6 either way.

## 9. The grid consumer (stage 4)

`TableSelection` (`gui/table.rs:408`) is a positional `(column, row)`
anchor/active rectangle inside iced tree state (`TableState`, `:443`).

- **Nothing is emitted.** The widget never calls `shell.publish`; selection
  is consumed only by the highlight fill and Ctrl-C.
- **No single-column distinction.** `range()` and the copy loop are
  symmetric in both axes; nothing branches on `min_col == max_col`.
- **Values are strings.** `cell_str` (`table.rs:252`) formats `AnyValue`
  and discards it. Column typing is polars `DataType` only; `SemanticType`
  (`plot/spec/mod.rs:1391`) is not imported. A single-column intent needs
  the typed `AnyValue` and a dtype-to-semantic mapping; the exact temporal
  discipline of §2 applies here too.
- **Visual selection dies on rerun.** Tree state is dropped when
  `PanelContent` swaps through `Running`; the same hazard `col_widths`
  mirrors into a caller-owned store (`table.rs:86`). This clears the
  rectangle, not a published predicate; the predicate clears through the
  §5 retirement event, never through widget lifetime.
- **The release edge exists.** `ButtonReleased(Left)` with `is_selecting`
  (`table.rs:611`) is the only place the widget learns a gesture finished.
- **Tests pin the TSV.** `selection_contract_at` (`table.rs:1704`) asserts
  `"A\t1\nB\t2\nC\t3\n"`; three pixel oracles include the selection fill.
  Stage 4's Ctrl-C byte-equivalence gate is already a kept test.

## 10. The status lane

The non-error lane is `status_msg`, set by `set_sql_status`
(`gui/state.rs:822`) or by direct assignment (the plot writeback path at
`:1587` is the closest analogue). There is no timeout; the only automatic
clearing is prefix-scoped (`"SQL · "`, `"Rune · "`) on editor activity
(`:875`). A selection message would persist until the next SQL or Rune
status overwrote it. The record should adopt a recognised prefix or add an
arm; a timer would be a new mechanism.

## 11. Identity of `source_view`

§3's `source_view` has three shapes today: `RESULT_PANEL` for SQL,
`(BufferId, LocalViewIdentity)` for Rune sinks, and `MarkdownCellIdentity`
for notebook outputs. `HostPanelId` covers the first two; the notebook
surface is not a host panel. The record must name one value all three
produce, or scope the first record to host panels and say so.

## 12. What the selection record has to decide

1. **The selectable-constraint substrate** (§2): its shape beside
   `PlotHit`, the exact temporal value type, and the per-kernel admission
   list of selectable fields, with derived fields explicitly refused.
2. **Where the input identity is minted** on each of the four paths (§3),
   given that it is product-owned, opaque, and binding-scoped.
3. **The rebuild recipe** per surface and where it is retained (§4), with
   the grid factory token inheritance as a gated prerequisite fix (§3).
4. **The retirement event for unannotated SQL and the notebook** (§5),
   binding the decided rule to named events.
5. **Brush scope per layout family** (§6), and refusal of categorical brush
   until a band inverse exists.
5a. **A typed temporal inverse** on the transformer: normalized pixel
   position over the exact `TemporalAxisDomain`, never the f64 inverse (§6).
6. **Click-family admission and emphasis together** (§7): which families
   supply a projection contract; the rest refuse click-selection per 0133 §7.
7. **Temporal and facet constraint minting** from axis type, domain, and
   the partition column's typed value (§6).
8. **The operation vocabulary**: reachable chords for replace, add,
   subtract, clear on both surfaces, including whether the pan guard is
   amended; wheel under brush; capture compensation (§8).
9. **Table typed values**: dtype-to-semantic mapping ownership (§9).
10. **Status prefix** for selection feedback (§10).
11. **`source_view` value** across host panels and notebook (§11).
12. **A stale-token discriminator test** in the shape of the 0150
    same-widget batch test: an intent minted against a predecessor token
    after replacement, asserting no context mutation and the `status_msg`
    text.

## 13. Baselines the record's stage 0 must bank

- Rebuild cost of a filtered kernel from a retained recipe for the 1M-row
  scatter and the hexbin exhibits, debug and release, as kept `#[ignore]`
  exhibits; this prices a predicate change as a settle.
- Sibling-view token identity: two sinks of one `views([...])` produce
  kernels whose raster subjects carry the publication's token (fails today
  on the grid path).
- Temporal exactness: two rows one nanosecond apart, click-selected, yield
  two distinct exact constraints, never one collapsed f64.
- Rune source bytes before and after a replace, add, subtract, clear
  sequence, identical.
- Ctrl-C bytes before and after the table consumer lands, identical.

## 14. Queue

Stage 3b was chosen as the next arc on 2026-09-02, ahead of release-profile
threshold pricing and readiness diagnostics. The priority-plan queue is
reordered accordingly.
