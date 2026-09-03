# ADR 0151: the selection context

Status: accepted 2026-09-02, proposed and revised the same day after review found the
projection contract at the wrong level, the exact-value vocabulary too
loose, the admitted-family list open and semantically wrong for histogram
and bar, the rebuild without newest-wins identity, the Rune binding
undefined, and the table seam self-contradictory; and again for five
contract residues (field type identity, the anchor vocabulary, the
absent-field rule, export under a pending rebuild, the multi-publication
context's supersession of PRODUCT-0133 §8 and the lineage's root alias)
and one integration pin (one preparing marker per surface); and a third
time for three contract repairs (the supersession note's cross-buffer
example, brush rounding that widened, retirement that left survivors
filtered by retired predicates) and two sharpenings (export snapshots its
predicate at dispatch; the refused families are named). Amended at
implementation review on 2026-09-03 after live event routing exposed an
unscoped Escape clear, an unsigned-domain edge, and a stale-export fallback.
This record is
PRODUCT-0133 stage 3's selection half and stage 4's consumers, split from
the viewport half PRODUCT-0150 shipped, on the census finding
(`plans/product/impl/0133_selection_census.md`) that four seams the
contract assumed do not exist: a selectable hit substrate, a product-owned
input identity, a rebuild recipe beside each plot surface, and an emphasis
projection contract. It decides those four, the exact scalar vocabulary
they share, the relation lineage the Rune compiler must carry, the closed
list of families that admit click-selection and what each constrains, the
context and its lifecycle bound to the retirement events each publication
path already has, newest-wins identity for predicate rebuilds, the brush
and click gestures with reachable operation chords, the typed temporal
inverse, the table's emission seam, export's publication record, and the
status lane. It does not decide mark-level emphasis, categorical brushing,
cross-relation mapping, disjunctive ranges, named contexts, filter
pushdown through a presentation chain, or any Rune spelling. Where
PRODUCT-0133 already decided a point, this record binds it to code and
does not reopen it.

## Context

### The hit is presentation, and the contract needs provenance

`PlotHit` carries `HitField { label, semantic_type, value }`. The labels
are what the hover shows: scatter and line say `"X"` and `"Y"`; histogram
says `"Range"`, `"Count"`, `"Y-Value"`; hexbin says `"Q"` and `"R"`. No
field names a source column, and the names are lost exactly where a
predicate would need them: histogram drops `bin_end`, heatmap drops its
value column, box plot drops all five Tukey columns, bar drops its group
column. The names exist in `ResolvedKernelFields` and reach the kernel
constructors positionally; nothing retains them. The values are the
painted `f64`: `axis_hit_value` mints `HitValue::Temporal { value: f64 }`
from the projection, candlestick and fill-between compute the exact
temporal integer and discard it, and scatter and line cast to `Float64`
before anything else sees the column. PRODUCT-0145 already established
that a nanosecond `Datetime` exceeds the f64 exact-integer range. The hit
then dies at the widget: `PlotWidget::update` publishes `hit.to_string()`.

### Some marks are not rows

A line hit is a linear interpolation along a segment; only a one-point
series returns a row value. A bar is the first row encountered for a
(category, group) pair, with later rows silently discarded. A box, a
violin curve, and a histogram bin are rows of an already aggregated
relation, so their constraints name the aggregate's columns, not the
input's. A hexbin cell is a viewport-dependent lattice coordinate. A
heatmap cell is exactly one row, refused on duplicate. `sibling_ordinal`
is not a row index: histogram and hexbin pass a count, box plot passes
zero for every outlier, scatter passes a null-skipping counter.

### Nothing names the input, and no surface holds its publication

`ArtifactToken` is shipped, opaque, inherited through
`from_polars_derived`, and guarded by the raster and hexbin substrates. No
panel, notebook output, or export snapshot holds one. The input identity
PRODUCT-0133 §4 keys contexts on has no representation. The Rune compiler
discriminates relations by `origin`, a verbatim splice of every non-view
call in the chain, whitespace included, so `trends.views([...])` and
`trends.sort([...]).views([...])` are two executions and two
publications. `RelationRootIdentity` is `Workspace | Ket` and nothing
else. The compiler does have a symbol table that resolves a `let`-bound
receiver to its initializer, and the origin's accumulation begins with the
first call on a bare root path, so the head of a chain is recoverable
structurally even though nothing recovers it today. The grid factory also
loses the token: `from_grid_with_figure_grid` hardcodes the minting
`create_plot`, so sibling views of one `views([...])` carry different
tokens in their kernels.

### A kernel cannot be rebuilt from itself, and rebuilds can race

The factory is separable from execution and faceting already proves
per-cell subsets. But a kernel cannot be reversed into its resolved spec,
facet configuration, composition, or layer settings, and `HostPlotState`
retains none of them. Preparation runs off the UI thread; several rapid
selections would launch several rebuilds whose completions may arrive in
any order. `RasterCoordinator::complete` already refuses a superseded
completion and the interactive draw already has a preparing marker, so
the shape exists.

### Retirement has a named event on three paths

PRODUCT-0133 §8 and stage 1 decide that predicates clear at publication
retirement, before the replacement is observable. Annotated SQL retires in
`ResultRegistry::begin`, keyed globally by result name; Rune retires in
`prepare_rune_view_panels`; the notebook retires in `begin_markdown_cell`.
Unannotated SQL has no retirement event.

### Emphasis has no anchor, and the chords that are actually free

The hit carries no pixel or data-space point. Bare left is pan, Ctrl is
the settings gesture, and Alt alone reaches the pan arm because the guard
excludes only Ctrl and Shift. Shift-press is declined and reserved;
Shift+Alt is excluded from pan by Shift and consumed by nothing. Table
Shift-click is positional extend; table Escape clears the rectangle only;
the table widget publishes no message today.

## Decision

### 1. One exact scalar vocabulary

`ExactScalar` is the physical value vocabulary every constraint, interval
endpoint, and table cell shares:

| variant | source dtypes | notes |
| --- | --- | --- |
| `Bool(bool)` | Boolean | |
| `Int(i64)` | Int8–Int64 | |
| `UInt(u64)` | UInt8–UInt64 | never folded into `Int` |
| `Float(f64)` | Float32, Float64 | canonical: `-0.0` stored as `+0.0`; NaN and infinities mint nothing |
| `Str(String)` | String, Categorical, Enum | |
| `Date { days: i32 }` | Date | |
| `Datetime { ticks: i64, unit, zone }` | Datetime | the column's unit and zone, never converted |
| `Time { nanos: i64 }` | Time | |
| `Duration { ticks: i64, unit }` | Duration | |
| `Decimal { mantissa: i128, scale: u8 }` | Decimal | |

Null cells, List, Array, Struct, Binary, Object, and any dtype not in the
table mint nothing and refuse with disclosure. Equality is structural over
the canonical form and hashing follows it.

`ScalarType` is the variant with its parameters: `Datetime { unit, zone }`,
`Duration { unit }`, `Decimal { scale }`, and the parameterless rest.
Ordering is defined within one `ScalarType` only, so an interval's
endpoints share one. A **field reference** is
`FieldRef { name: SourceField, semantic_type: SemanticType, scalar_type: ScalarType }`:
the semantic type is the emitting view's (a plot's bound channel, or the
table's dtype mapping: strings and booleans nominal, numbers quantitative,
temporal dtypes temporal) and governs the algebra; the scalar type
governs application. A constraint applies to a view only when that view's
relation carries the field name with an equal `ScalarType`, unit, zone,
and scale included (§7). A projected `f64` is never a source of an
`ExactScalar`.

### 2. The selection candidate is a second substrate beside the hit

`PlotHit` gains `candidate: Option<SelectionCandidate>`, where
`SelectionCandidate { constraints: Vec<(FieldRef, ExactScalar)> }` is
the conjunction a click on that mark means, over typed source fields.
Presentation fields are untouched and the prose seam does not render the
candidate. An admitted kernel retains, beside its projected paint values,
the source column name and the exact value for every field its candidate
names, and mints the candidate from the retained value only. A field that
is not a source column — a count, a cursor-inverted height, a lattice
coordinate, a direction — never appears in a candidate.

### 3. Admission is projection of a predicate, and the list is closed

`PlotKernel` gains
`project_predicate(&self, &Predicate) -> Option<Vec<Box<dyn EmphasisAnchor>>>`,
defaulting to `None`. An `EmphasisAnchor` is an opaque **data-space**
value the kernel owns, with one method,
`reproject(&self, &CoordinateTransformer) -> Option<EmphasisShape>`, where
`EmphasisShape` is a widget-space rectangle, circle, or segment. The
overlay calls `reproject` on every frame and stores no screen-space
value. Each family's anchor and its complete inputs are stated below; an
anchor never draws from fewer inputs than its family names, so a
candlestick emphasis is one candle column from x alone and a heatmap
emphasis is one cell from both categories, never a cross of two bands.
**A kernel admits click-selection by implementing `project_predicate`,
and only so.** The widget emits a candidate only when the kernel
projects the candidate as a predicate to at least one anchor whose
reprojection contains the hit. Projection enumerates the retained exact
values once per predicate change or rebuild, never per frame, so emphasis
is one scan per selection, priced in stage 0.

The admitted families, what a click constrains, and the anchor's inputs:

| family | candidate | anchor, from |
| --- | --- | --- |
| scatter | x, y, and the color field when bound, all exact per point | one point circle, from x and y |
| bar | category, and group when bound | one bar rectangle, from category and group |
| box plot, box hit | category | one box band, from category |
| box plot, outlier hit | category and the outlier value | one outlier circle, from category and outlier |
| violin | group | one violin band, from group |
| heatmap | x category and y category | one cell rectangle, from both categories |
| histogram | `bin_start` and `bin_end`, both exact | one bin rectangle, from both fields |
| candlestick | x | one candle column, from x |
| fill-between | x | one x-column segment across the band, from x |

**Every other kernel refuses click-selection**: line (a segment hit is an
interpolation, not a row), hexbin, pie, funnel, radar, parallel, radial
dial, stacked area, and any kernel not named in this table. A bar plot
whose preparation discarded a duplicate (category, group) row refuses
click-selection on that plot with disclosure, because its candidate would
select rows it never displayed; refusing duplicates at preparation, as
heatmap does, is a substrate change this record names and does not make.
**This table is closed by this record.** The implementation may not admit
a family it does not list; admitting one is an amendment.

### 4. The input identity, and the relation lineage the compiler carries

`InputIdentity` is an opaque product-owned value with `ArtifactToken`'s
discipline. A `Publication { input: InputIdentity, token: ArtifactToken }`
rides on `HostPlotState`, `TableArtifact`, both notebook outputs, and
`HostPlotExport`; derived artifacts inherit both through
`from_polars_derived`. The identity is minted once per logical binding
and preserved across republication:

| path | binding | minting key |
| --- | --- | --- |
| annotated SQL | the result name, globally, as `ResultRegistry` keys it | the name |
| unannotated SQL | the producer buffer's result panel | the buffer |
| Rune | the producer buffer and the relation's lineage | `(BufferId, RelationLineage)` |
| notebook | the cell | the cell identity |

**`RelationLineage`** is new and the compiler carries it. It is the
structural head of a chain: the name of the first method applied to the
bare root path and that call's literal arguments as parsed values,
resolved through the existing `let` symbol table. The root is
**normalized away**: the compiler already folds `ket` into `workspace`
when it builds the origin, so the two spellings are aliases of one input
and the lineage carries neither. It is not origin text (whitespace and formatting do not enter it),
not the resolved expression (no call after the head enters it), and it
distinguishes two relations in one buffer. `trends.views([...])` and
`trends.sort([...]).views([...])` carry one lineage. The compiler records
the head's method name as data and matches on nothing, so PRODUCT-0003's
fence on relation verbs holds. A head with a non-literal argument carries
no lineage; its views receive no `Publication.input`, emit no intents,
and disclose when selected on.

Sibling Rune views over one lineage are separate publications with
separate tokens, and so are two buffers over one table name. The context
therefore keys by `InputIdentity` but binds each predicate to the
publication it was minted against (§7); it never holds one token for the
input. **This supersedes PRODUCT-0133 §8's "current token for that input
identity"**, which assumed one live publication per input; the staleness
check moves from the context to the source view's own publication, and
the retirement rule narrows from the input to the retired publication.
The supersession is recorded in 0133 §8 with attribution to this record
and takes effect on its acceptance. Two buffers over one table name are two bindings and two contexts
in this record; sharing them is deferred with its reason, that it would
need a second live-publication rule the contract does not have.

The grid factory repair is a prerequisite: `from_grid_with_figure_grid`
and `from_styled_grid_with_figure_grid` take the publication and construct
through the `_from_artifact` variants, gated by an architecture test that
the facet path contains no `ArtifactToken::mint`.

### 5. The rebuild recipe lives beside the kernel

Each plot surface retains `PlotRecipe { artifact: Arc<ResultArtifact>,
publication, construction }` where `construction` is exactly one of the
three shapes the factory sites consume: single, facet, or layered, each
with the resolved spec or composition and the settings it needs. The
plot-setting seams take the recipe with the kernel; a kernel never arrives
without one. A rebuild filters the artifact's projection, runs the same
construction through the same factory, and inherits the publication. It
is **always from the unfiltered artifact**, never from a previous filtered
kernel.

**A predicate rebuild is a re-preparation, not a replacement**: the
pane-override viewport survives, the viewport generation advances as a
settle does, the surface stays `Settled`, `HostPlotState` and its raster
coordinator are the same allocation. PRODUCT-0150 §10's reset applies to a
new publication, not a new predicate over the same one.

### 6. Rebuilds are newest-wins

Every context carries a `SelectionRevision` that advances on each
mutation and each clear. A rebuild job is stamped with
`RebuildStamp { publication, surface_incarnation, selection_revision }`
and runs off the UI thread. Its completion is applied only when all three
still match the surface; any mismatch drops it. A completion for revision
two arriving after revision three's is dropped by the revision alone.

While a rebuild is pending the surface keeps its current kernel on
display, marks itself `SelectionDisplay::Pending`, and shows **the
surface's one preparing marker**: the raster preparing marker and the
selection pending state are two inputs to a single surface-level marker,
never two stacked markers, preserving PRODUCT-0149's singularity rule.
Hover remains available over the displayed kernel. Export under a pending
rebuild is decided in §13: it never renders a kernel behind the context.

### 7. The context, the intent, and the algebra bound to forms

`SelectionContexts` lives beside the panels in the surface owner, keyed by
`InputIdentity`. A context holds, per `SourceView` (`HostPanel(HostPanelId)`
or `NotebookCell(MarkdownCellIdentity)`), the predicate and the
`ArtifactToken` it was minted against.

A `Predicate` is a conjunction of `FieldConstraint { field, form }` with
one entry per field, `form` being `Values(set of ExactScalar)` or one
`Interval`. PRODUCT-0133 §6 binds to those forms:

- OR within a field is set union over `Values`.
- AND across fields and across views is the conjunction.
- `replace` installs the intent's predicate.
- `add` unions `Values` into `Values` on one field, or conjoins a field
  the predicate lacks. An intent whose candidate names more than one field
  refuses `add`: unioning it would widen to a cross product, the same
  widening PRODUCT-0133 §11 refuses for the grid. `add` of an `Interval`
  onto a field that carries one, or of `Values` onto an `Interval`,
  refuses as the disjunctive range §6 excludes.
- `subtract` removes from `Values` and is admitted for nominal and ordinal
  only; on any other form it refuses.
- `clear` discards the view's predicate.

**Every intent is preflighted before the context mutates.** The
predicate the intent would produce is checked against every live view of
the input other than the source: each field must be present in that
view's relation with an equal `ScalarType` (§1). If any view cannot
apply any field, the whole intent refuses, the context is unchanged, and
the status lane names the view and the field. A context never holds a
predicate some member cannot apply; PRODUCT-0133's algebra is total over
the context or not entered. This record filters the published projection
and does not push a predicate through a presentation chain, which is why
a view whose chain dropped a field refuses the intent rather than being
exempted from it.

`SelectionIntent` is §3's value with `input_ref`, `artifact_token`,
`source_view`, `constraints`, and `within_source: Replace | Add | Subtract
| Clear`. An intent whose token differs from its source surface's current
`Publication.token` is rejected without mutation and disclosed
("Selection expired because the source relation was refreshed; select
again."). Every refusal is disclosed and mutates nothing.

After a mutation the revision advances, every view of the input except
the source rebuilds against the conjunction of the other views'
predicates, and the source view projects its own predicate for emphasis.

### 8. Retirement clears, on the events that exist

When a publication is retired, the context for its input drops every
predicate bound to that publication's token before the replacement is
observable, and the revision advances. The call is made from the
dispatch-side event each path has:

- annotated SQL: with `ResultRegistry::begin`;
- Rune: in `prepare_rune_view_panels`, per panel publication;
- notebook: in `begin_markdown_cell`;
- unannotated SQL: a new `retire_publication` call at the same point in
  `run_code`, which this record adds.

Old rows remain on screen without their predicate until the replacement
arrives, as the contract chose. A retired view's emphasis clears with its
predicate; sibling views over other publications of the same input keep
theirs. **Retirement also rebuilds every surviving member.** A survivor
may be displaying a kernel filtered by the retired predicates, so the
retirement call schedules a newest-wins rebuild (§6) against the
remaining predicates, and an emphasis refresh, for every live view of the
input other than the retired ones, under the shared pending marker until
each catches up. Clearing the context without rebuilding its members
would leave a filter on screen that no predicate any longer justifies.

### 9. The brush, its scope, and the typed temporal inverse

A brush begins with a Shift-press inside PRODUCT-0150's active rectangle,
tracks a rectangle through moves, and mints on release. Scope per layout
family follows 0150 §2: cartesian both axes, `CategoricalX` and
`CategoricalY` the continuous axis only, every other family refuses on
release with disclosure. Brush admission is by layout family, so line
brushes although it refuses click. The categorical axis is not brushed by
this record.

Each brushed continuous axis mints one `Interval` on the axis's source
field, and the endpoints are `ExactScalar` of the axis column's variant:

- the rectangle's two edges are normalized so the lower edge is the
  lesser value before any rounding;
- a floating axis inverts the edges through the press-time
  `ViewportGeometry` and mints exact `Float` endpoints, unrounded;
- an integral axis inverts the same way and mints `ceil(lower)` and
  `floor(upper)`, so no value whose plotted position lay outside the
  rectangle is admitted; when an unsigned axis's lower edge lies below
  zero, that edge clamps inward to zero while a representable upper edge
  continues to define the interval;
- a temporal axis inverts through a new
  `CoordinateTransformer::pixel_to_temporal(axis, pixel) -> ExactScalar`
  that interpolates the normalized pixel position over the exact
  `TemporalAxisDomain` in the domain's integer unit and then applies the
  same inward rule, `ceil` for the lower tick and `floor` for the upper,
  with the zone from `AxisType::Datetime`. The f64 cartesian inverse is
  never the source of a temporal endpoint.

An inward rounding can leave no tick: both edges between the same two
adjacent integers or ticks. The interval is then **empty**, and the brush
selects nothing rather than widening to the neighbours; the intent is
still emitted, so a `replace` with an empty brush clears the view's rows
and the overlay still draws the rectangle. Rounding never widens.

Inside a facet cell the rectangle is clipped to the press cell, localized
through the grid's cell-bounds arithmetic, and conjoined with the cell's
facet constraint minted from the partition column's exact value, which
`GridCell` retains beside its label. Wheel is suppressed while a brush is
active. A brush that never leaves a small threshold of its press point is
a click (§10). Brush emphasis is the minted intervals reprojected through
the current `ViewportGeometry` on every overlay frame.

### 10. Click-select, and the reachable operation chords

All three operations live on the Shift chord, which the pan guard already
excludes, so PRODUCT-0150's arbitration is not amended:

| chord | on a mark | on empty plot area | as a drag |
| --- | --- | --- | --- |
| Shift | `replace` with the mark's candidate | `clear` | brush, `replace` |
| Shift+Alt | toggle: `subtract` if every value of the candidate is already in the own predicate's `Values` for its field, else `add` | nothing | brush, `add` |
| Escape, cursor over a view with an active predicate | `clear` | | |

Alt is pressed with Shift already down, so the pan arm never sees it;
bare Alt keeps its 0150 meaning. Toggle on a multi-field candidate
refuses per §7 and is disclosed. A Shift-press captures and the editor
applies the `FocusPane` compensation it applies to the pan press.

**Amended at implementation review.** Iced sends a keyboard event through
every live canvas, so focus alone let one Escape schedule a sibling rebuild
from every selectable plot. Plot Escape therefore emits only while the cursor
is over that view and the view owns a non-empty predicate. A clear with no
bound predicate is a context no-op (no revision and no rebuild list), and a
ready plot or table deduplicates an incoming predicate equal to the one it
already displays. These are three defenses around one event-routing boundary,
not three selection operations.

### 11. Hover and selection are separate lanes

The hit reaches the pane on a new message carrying the structured hit,
beside the existing prose hover. Hover suppression under motion
(PRODUCT-0150 §7) applies to both. Selection gestures hit-test at press
and release only.

### 12. The table emits through an explicit seam

`Table` gains `on_selection(impl Fn(TableSelectionEvent) -> Message)`, in
the builder style of `scroll_request`, and publishes through it on release
of a selection gesture and on Escape; it publishes nothing else. The host
maps the event to an intent. `TableArtifact` carries its `Publication`.

A rectangle spanning one column emits `Values` of that column's exact
cells for the selected rows, typed by §1 from the column dtype. A
rectangle spanning several columns emits nothing and discloses per
PRODUCT-0133 §11; the rectangle and Ctrl-C are untouched. A selected range
containing a null or an unrepresentable dtype emits nothing and discloses.
Escape clears the rectangle and emits `clear`. Shift-click keeps its
positional meaning.

The table consumes as any view does: it rebuilds over the conjunction of
other views' predicates by filtering its frame, never on its own. Its own
emphasis is derived from its predicate — rows whose column value is in the
set — not from stored positions, because a rebuild reorders rows.

### 13. Export records the publication and adds no policy

`HostPlotExport` carries the surface's `Publication`, its `PlotRecipe`,
and a **snapshot taken at export dispatch** of the context's resolved
predicate for that view and its `SelectionRevision`. Export resolves that
snapshot, as PRODUCT-0133 §10 and its plan require of the current filter
context: when the displayed kernel's revision equals the snapshot's, the
export renders it; otherwise the export job rebuilds from the recipe and
the snapshot's predicate inside its own blocking context before
rendering. The export records the snapshot revision it rendered. A
selection made after dispatch does not restart, cancel, or alter the
running export; the next export takes the next snapshot. An export never
renders a kernel behind the revision it was dispatched at, adds no flag,
and needs no 0133 amendment. If the required snapshot rebuild fails, the
export refuses with disclosure; it never falls back to the stale displayed
kernel.

### 14. The status lane

Every disclosure in this record writes `status_msg` with the
`"Selection · "` prefix, clears `status_error`, and never populates
`StatusDetail`. The prefix joins the transient-clearing arm. No timer.

### 15. What this record does not do

No mark-level emphasis. No categorical brush. No click-selection on
refused families. No filter pushdown through a presentation chain: an
intent a view cannot apply is refused, not exempted. No cross-buffer sharing
of a Rune lineage. No cross-relation filtering, named contexts,
disjunctive ranges, or Rune spelling. No bar duplicate refusal at
preparation.

## Implementation sequence

**Stage 0 — bank the baselines.** A filtered rebuild of the 1M-row
scatter and the hexbin exhibits from a recipe, and one predicate
projection over the 1M-row scatter, debug and release, as kept
`#[ignore]` exhibits; two sinks of one `views([...])` carrying different
raster-subject tokens (the grid control); two rows one nanosecond apart
hit-tested to one collapsed f64 (the temporal control); a bar plot with
a duplicate pair displaying the first row; two chains over one head with
different origins (the lineage control); the source buffer's bytes; the
table's Ctrl-C bytes.

**Stage 1 — identity and publication.** `ExactScalar`, `ScalarType`,
`FieldRef`, `InputIdentity`,
`Publication`, `RelationLineage` in the compiler with symbol-table
resolution and root normalization, the 0133 §8 attribution, the four
minting sites, the grid factory inheritance,
`Publication` on every surface and the export snapshot,
`retire_publication` for unannotated SQL.

**Stage 2 — the candidate substrate.** `SelectionCandidate`, field-name
and exact-value retention in the nine admitted kernels,
`project_predicate` with data-space anchors, the bar duplicate refusal,
`pixel_to_temporal`, the structured-hit message.

**Stage 3 — recipe, context, and newest-wins.** `PlotRecipe` through the
setting seams, rebuild as re-preparation from the unfiltered artifact,
`SelectionContexts`, the intent, the typed algebra with its refusals, the
preflight, `SelectionRevision`, `RebuildStamp`, the pending display state
on the single surface marker, retirement on the four events, stale
rejection.

**Stage 4 — the gestures.** Brush with scope, integral rounding, facet
clipping, wheel suppression, and the reprojected overlay; Shift and
Shift+Alt click with toggle; Escape; capture compensation; the status
prefix.

**Stage 5 — the table, export, evidence.** `on_selection`, single-column
emission with the §1 refusals, multi-column disclosure, value-derived
emphasis, table rebuild; export with recipe, predicate, and the pending
rebuild; full battery.

## Acceptance gates

1. **No candidate without provenance.** Every candidate names columns
   present in the recipe's projection; a synthetic kernel emitting a
   `"Count"` field fails an assay.
2. **Exact values.** Two rows one nanosecond apart clicked in turn yield
   two `Datetime` candidates with distinct `ticks`; an `Int64` above
   2^53 round-trips exactly; `-0.0` and `0.0` mint one `Float`; a NaN
   cell mints nothing and discloses.
3. **Admission is projection.** A kernel with the default
   `project_predicate` emits no candidate under Shift-click and discloses.
3a. **Anchors draw from their full inputs.** A heatmap predicate on two
   categories reprojects to one cell rectangle; a candlestick predicate on
   x reprojects to one candle column; a histogram predicate reprojects to
   one bin from both fields.
4. **The table is closed.** An architecture assay lists the kernels
   implementing `project_predicate` and fails on any not in §3.
5. **Each admitted family constrains what §3 says.** Per family, the
   candidate's field set equals the table's; histogram carries both
   `bin_start` and `bin_end`; line emits none.
6. **Bar duplicates refuse.** A bar over a duplicate pair refuses
   click-select and discloses; the stage-0 control goes green.
7. **Projection is one scan.** Over the 1M-row scatter, a predicate change
   runs projection once; ten overlay frames run it zero times.
8. **Lineage is structural.** `t.views(...)` and `t.sort(...).views(...)`
   carry one `RelationLineage`; `workspace.table("a")` and
   `workspace.table("b")` in one buffer carry two; `ket.table("a")` and
   `workspace.table("a")` carry one; reformatting a chain does not change
   it; a non-literal head carries none.
9. **Identity survives republication; token does not.** Rerunning each
   of the four paths keeps `InputIdentity` and mints a new token.
10. **The grid inherits.** The facet factory path contains no
    `ArtifactToken::mint`; the stage-0 control goes green.
11. **Rebuild is a re-preparation from the unfiltered artifact.** After
    two successive predicates the second kernel's rows are the artifact
    filtered by the second predicate alone; pane-override domains are
    unchanged; generation advanced; surface `Settled`; same allocation.
12. **Reversed completions are dropped.** Revision two's completion
    applied after revision three's changes nothing; a completion after
    surface replacement changes nothing.
13. **Pending display and one marker.** Between dispatch and completion
    the surface shows the prior kernel with exactly one preparing marker,
    also when a raster preparation overlaps; export dispatched in that
    window rebuilds from the recipe and the dispatch-time predicate, and
    its bytes equal an export taken after that revision completes; a
    selection made while the export job runs changes neither its bytes
    nor its recorded revision; a failed dispatch-snapshot rebuild refuses
    the export rather than rendering the stale displayed kernel.
14. **The rebuild never leaves the process.** No adapter call; priced as
    one settle by the stage-0 exhibit.
15. **Self-exclusion and emphasis.** The source view's rows are unchanged
    after its own selection and its overlay draws the projected anchors;
    a sibling's rows are the filtered subset.
16. **The algebra refuses where §7 says.** `add` of a multi-field
    candidate, `add` of a second interval, `subtract` on a quantitative
    set, and an intent naming a field a live sibling lacks or carries with
    a different unit, zone, or scale each leave the context unchanged,
    rebuild nothing, and write the `"Selection · "` message with
    `status_error` empty; the last names the view and the field.
16a. **Subtract reads the semantic type.** A `Str` field bound ordinal
    accepts `subtract`; the same `Str` values from a quantitative-bound
    field do not arise, and a quantitative `Values` set refuses.
17. **OR within, AND across.** Two table values on one field select rows
    matching either; a brush from one view and a click from another
    select rows matching both.
18. **Retirement precedes visibility on all four paths**, clearing only
    the retired publication's predicates, and **survivors catch up**: a
    sibling displaying rows filtered by the retired predicate shows, after
    its scheduled rebuild completes, the rows of its artifact filtered by
    the remaining predicates alone, with the shared marker in between;
    the assertion is on displayed rows, not registry state.
19. **Stale intents are rejected.** An intent against the predecessor
    token mutates nothing and discloses; against the current token it
    succeeds.
20. **Brush scope and endpoints.** Cartesian mints two intervals with the
    column variant; a floating axis mints unrounded endpoints; an integral
    axis mints `ceil(lower)` and `floor(upper)` and admits no value plotted
    outside the rectangle; a temporal axis mints ticks with the zone by the
    same inward rule and never passes through `pixel_to_cartesian`;
    an unsigned lower edge below zero clamps to zero when its upper edge is
    representable; refused families disclose.
20a. **Between two ticks is empty.** A brush whose edges both fall between
    the same adjacent integers, and one whose edges both fall between the
    same adjacent temporal ticks, each mint an empty interval; the source
    view's siblings show no rows and the overlay draws the rectangle;
    neither neighbour is selected.
21. **Facet brushes carry the cell** with the partition column's exact
    value, clipped to the press cell.
22. **The overlay reprojects.** After pan, zoom, reflow, and rebuild the
    emphasis lies over the same data.
23. **Chords are reachable; pan untouched.** Shift and Shift+Alt presses
    never reach the pan arm; bare Alt still pans; the 0150 arbitration
    test is unchanged. Escape publishes `clear` only with the cursor over a
    view that owns a predicate; an empty context clear advances no revision
    and schedules no rebuild, and ready plot/table consumers deduplicate an
    unchanged displayed predicate.
24. **Toggle is exact.** Shift+Alt on a selected category subtracts; on an
    unselected one adds; on a two-field candidate refuses.
25. **The table emits only lossless**, through `on_selection`: single
    column emits; multi-column, null, and unrepresentable dtype disclose;
    Ctrl-C bytes equal stage 0; Escape emits `clear`.
26. **Table emphasis follows values** after a sibling reorders it.
27. **Rune is untouched.** Source bytes identical after every operation;
    no writeback gesture pushed.
28. **No engine branch.** No selection module names an adapter.
29. **Prose is unchanged.** Every hover string is byte-identical after
    stage 2.

## Guardrails and stop conditions

- If an admitted kernel cannot retain exact source values without
  re-reading the artifact per hit, halt and negotiate retention; never
  mint from the projection.
- If a family's predicate cannot be projected with the existing forward
  transform, it stays refused; admitting it is an amendment.
- If `RelationLineage` cannot be carried without matching a relation verb
  in the compiler, halt; PRODUCT-0003's fence stands.
- If a rebuild cannot keep the pane-override viewport through the replace
  seam, halt and split the seam.
- If the temporal inverse needs a wider integer than the unit provides,
  halt; do not round through f64.
- If the table's value-derived emphasis requires row identity, stop and
  record it.
- Mark-level emphasis, categorical brushing, chain pushdown, cross-buffer
  lineage sharing, bar duplicate refusal, and cross-relation mapping each
  reopen their own boundary as a separate record.

## Rejected

**Minting from `HitField`.** Hover labels and paint projections; 0145
showed the projection cannot carry a nanosecond.

**Projecting one constraint at a time.** A scatter point is x and y
together; a heatmap cell is two categories; nothing projects half a
conjunction.

**`Quantitative(f64)` and a zone-less temporal.** Large integers, `Date`
days, and `Time` nanoseconds do not fit; a physical vocabulary does.

**Admitting line.** The hit is an interpolation.

**Admitting bar over discarded duplicates.** The predicate would select
rows the bar never showed.

**An open admission list.** A census that promotes families amends the
record without saying so.

**Origin text, or the resolved expression, as the Rune key.** The
rejected key of PRODUCT-0133; also whitespace-sensitive.

**One identity per buffer, or one per execution.** The first merges
independent relations; the second splits one input by its sort.

**One token per context.** Two chains over one lineage are two live
publications; a single token would make one of them permanently stale.

**Cross-buffer lineage sharing now.** Needs a live-publication rule the
contract lacks.

**Rebuilding from the previous filtered kernel.** Compounds filters and
cannot widen.

**Applying stale completions by arrival order.** Reversed completions
would display the older predicate.

**`add` of a multi-field candidate.** A cross product wider than the
gesture; §11's refusal applies.

**Exempting a sibling that cannot apply a field.** The context would
hold a predicate one member ignores, which is not PRODUCT-0133's algebra;
refuse the intent and say which view and field.

**Exporting the displayed kernel under a pending rebuild.** Recording the
older revision does not make the image current; 0133 §10 wants the
current context, so the export rebuilds.

**Two stacked preparing markers.** 0149 decided one per surface.

**A raw root in the lineage.** The compiler already treats `ket` and
`workspace` as one; carrying the spelling would split aliases.

**Same-variant compatibility.** A `Datetime` in milliseconds and one in
nanoseconds share a variant and not a meaning.

**Outward or nearest rounding of brush edges.** Admits values plotted
outside the rectangle; the inward rule and the empty interval do not.

**Clearing a retired publication's predicates without rebuilding
survivors.** Leaves a filter on screen that nothing justifies.

**An export that tracks the live context.** A selection during the job
would force a restart or a cancel; the dispatch-time snapshot is the
current context the contract means.

**A widget that emits without a seam.** A release intent is a message or
a callback; nothing else exists.

**Alt-only chords.** Bare Alt reaches the pan arm.

**A timer on the status lane.** A prefix suffices.

## Consequences

The typed hit PRODUCT-0133 stage 2 built reaches something that can act
on it, on a candidate substrate that carries source names and physical
values and refuses everything else. The compiler learns the one fact about
a relation it never recorded, its head, and that fact keys a context the
way §4 asked without touching the fence PRODUCT-0003 put around relation
verbs. Every surface learns which publication it shows, the grid stops
minting tokens nobody compared, and a selection lands as a newest-wins
rebuild through the recipe the surfaces now keep, priced as one settle on
0149's substrate, leaving 0150's viewport where the user put it. Line,
hexbin, pie, funnel, radar, parallel, radial dial, stacked area, and the
remaining unsupported families refuse rather than approximate, and the
refusals say so. Mark-level emphasis, the categorical brush, chain
pushdown, and cross-buffer sharing each have a named boundary and a named
reason to wait.
