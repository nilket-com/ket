# ADR 0075: retiring the nested dashboard

Status: proposed 2026-08-10. This record removes Polariton's pre-Rune nested
dashboard after PRODUCT-0074 proved ordinary host panels are an acceptable
spatial replacement. It preserves the three accepted producer capabilities
that still route through the dashboard today: a plain Rune buffer's single
plot, a Markdown Rune cell with `output = pane`, and Plot -> Export. The first
two move to existing host-panel identities; export is repaired to consume the
plots users actually author. No plot kernel, Rune spelling, saved-session
schema, or logical-layout schema changes.

## Context

Polariton currently has two independent pane systems.

- libaryon owns the workspace's logical layout, ordinary document panes, and
  host panels. Rune relation views already render here and PRODUCT-0061 through
  PRODUCT-0063 persist their placement and meanings.
- Polariton's `DashboardState` owns a second `gui::pane_grid::PaneState` inside
  fixed host panel `DASHBOARD_PANEL = 2`. Its panes contain `PlotPaneState`
  objects and derived `Arc<dyn PlotKernel>` values that no persistence record
  accepts.

The dashboard predates the Rune plotting language. Its Plot -> New picker was
once the only GUI path to twenty renderer variants. Rune now authors scatter,
line, all four bar members, stacked area, facets, and percent normalization in
ordinary host panels. PRODUCT-0074 removed the terminal-cell divider
reservation that made those panels 38.4px by 22px coarser than the dashboard,
and its accepted exhibits showed a dense Rune grid plus split document panes
with no logical-layout or saved-byte change. The practical reason to keep a
second layout owner is gone.

Stage 0 nevertheless disproved the tempting description of this record as
pure subtraction. Four production paths still depend on the old surface:

| path | dashboard dependency today | disposition here |
|---|---|---|
| Plot -> New | builds a renderer from `AppState.data_frame` and inserts a nested pane | retire |
| plain Rune buffer | `RuneBufferPlotReady` replaces `rune_plot_panes[BufferId]` | preserve in a durable host panel |
| Markdown Rune `output = pane` | `NotebookPlotReady` inserts a nested pane | preserve in the shared Results panel |
| Plot -> Export | iterates only `DashboardState.content` | repair to export host-panel plots |

The export item is not migration overhead. It is a shipped capability defect:
every figure authored through `.views` -- including the live exhibits from the
plotting arc -- is invisible to Plot -> Export. That is why the arc's evidence
has relied on screenshots. Removing the obsolete surface forces the command to
address the surface users actually use.

There is one saved-layout edge. PRODUCT-0061 includes every live host-panel
identity in libaryon's census, including fixed role `DASHBOARD_PANEL = 2`, while
PRODUCT-0062 deliberately excludes fixed roles from its semantic map. A saved
session may therefore restore panel 2 with no Polariton meaning. Leaving it as
an unresolved placeholder would make a deleted feature look like damaged
session data. The old identity must be retired explicitly.

The remaining renderer variants are not a reason to retain the dashboard.
Box, violin, bubble, candlestick, fill-between, funnel, heatmap, hexbin,
histogram, parallel, pie, radar, and radial-dial kernels remain valid substrate
and direct-test targets. This record removes their manual picker, not their
implementations, and grants none of them a Rune spelling by accident.

## Decision

### 1. A plain Rune plot uses the existing anonymous view identity

The plain Rune evaluator does not enter `rune_relation_envelope`; a terminal
`.views(...)` is what selects that route. It does, however, enforce a stronger
cardinality rule at the fallback boundary:

```text
0 plot requests   -> no plot
1 plot request    -> one resolved plot
2 or more         -> refusal
```

Its one possible plot is therefore semantically output ordinal zero. The host
assigns it the existing `LocalViewIdentity::Anonymous(0)` and routes it through
`prepare_rune_view_panels` and `set_rune_view_plot` rather than
`rune_plot_panes` and `DashboardState`.

This is a routing bridge, not a new identity kind. PRODUCT-0062 already
persists `Anonymous { ordinal: u64 }`, so the durable key remains exactly

```text
(DocumentId, LocalViewIdentity::Anonymous(0)) -> HostPanelId
```

and `polariton_session` remains schema version 2. A checkpoint written from a
plain plot is readable by an existing v2 decoder because its vocabulary and
shape are unchanged.

The relation and plain evaluators are mutually exclusive for one source
revision. If a document changes from a plain one-plot program to a relation
program whose first unnamed sink is also `Anonymous(0)`, that first semantic
output keeps the panel. This is the same anonymous-ordinal behaviour PRODUCT-
0060 and PRODUCT-0062 already accept: identity follows producer plus output
ordinal, not evaluator implementation. Named sinks get their named identities;
removed anonymous outputs become orphaned through the existing reconciliation
rule.

The bridge preserves the plain surface rather than widening it. A plain buffer
still accepts at most one plot, still exposes only its existing scatter/line
API, and does not acquire `.views`, authored categorical palettes, facets,
plot-skin modifiers, or a new color-continuation promise. Its presentation uses
the resolved composition's existing title and default `PlotSettingsLayer`.

A successful plain run with no plot prepares an empty view set for that
producer. It may not leave the prior plot marked fresh merely because the
dashboard map disappeared.

### 2. Markdown `output = pane` uses the shared Results panel

The fixed `RESULT_PANEL = 1` already presents pane-targeted SQL tables and
non-row messages. A completed Markdown Rune plot with `output = pane` now calls
the same begin/refresh operation and replaces that panel's content with a
`HostPlotState`. The next table, message, or plot replaces it in turn.

The role is named **Results**, not table results. New opens use a neutral host
kind such as `result`; a restored descriptor carrying the older
`result-table` kind remains readable because the descriptor does not select
Polariton content. The stable panel ID, placement, revision behaviour, and
focus policy do not change.

`output = inline` remains an inline notebook plot. `output = none` remains
non-presentational. This record neither requires `.views` nor removes an
accepted Markdown output spelling.

The Results panel is a fixed role, not a producer-semantic mapping. Its current
derived table, message, or plot content remains outside `polariton_session` and
restores honestly unresolved until a result runs again, as PRODUCT-0061 and
PRODUCT-0062 already specify.

### 3. Plot -> Export exports ordinary host-panel plots

The Plot menu keeps Export SVG, Export PNG, and Export AVIF. It loses the New
submenu.

For export, `CodeEditor` projects an immutable snapshot from the same objects
the production host-panel view consumes:

```text
latest Viewport panes
    intersect PanelRegistry entries whose content is Plot
    -> kernel, display title, resolved settings/model, render revision,
       resize-suspension state, and rendered content extent
```

No dashboard state, nested `PaneId`, authored relation, or persisted owner is
consulted. Table, message, running, failed, and unresolved panels are excluded.
A stale panel whose last good plot is still visibly rendered is included: the
command exports what the window shows, not whether the latest producer run
succeeded.

A host panel may be projected through more than one view. Export emits one file
per unique `HostPanelId`, taking the first occurrence in viewport presentation
order. Duplicate projections may not duplicate a file.

Files retain the existing simple ordinal convention, independent of titles:

```text
plot_1.svg, plot_2.svg, ...
plot_1.png, plot_2.png, ...
plot_1.avif, plot_2.avif, ...
```

Ordinals follow the deduplicated presentation order for that invocation. A
title is user prose and may contain slashes, non-UTF-8-derived labels, or two
equal strings; it is not silently promoted into a filename policy. Export has
never promised stable file identity across layout edits.

Each export uses the plot canvas's actual rendered size, not the outer pane and
not a fixed 1200x720 substitute. The extent is derived from the projected pane
rectangle through Polariton's own presentation metrics: the 22px pane header
and the host surface's 4px padding on each side are excluded; borders paint
inside that padding and are not subtracted again. This is the same geometry
PRODUCT-0074 measured. A non-positive residual extent refuses that panel rather
than constructing a zero-sized backend.

Rendering continues through `PlotWidget` with `PlotRenderLayer::Full` and the
existing SVG, SVG-derived PNG, and SVG-derived AVIF backends. PRODUCT-0057's
license boundaries are unchanged. If no projected host panel contains a plot,
the command emits no asynchronous job and reports `No live plots to export.`
rather than doing nothing or claiming zero successful files.

### 4. Legacy dashboard panel 2 is a migration tombstone

`DASHBOARD_PANEL` is renamed to a migration-only constant such as
`RETIRED_DASHBOARD_PANEL`. No production action can open it and no Polariton
content registry treats it as a surface.

After a restored viewport's pending PRODUCT-0062 meanings reach a terminal
reconciliation outcome -- installed, refused, or absent -- the frontend finds
every projected occurrence of host panel 2 and sends the ordinary engine
command

```text
CloseDocTab { pane_id, view_id }
```

for that occurrence. The engine remains the sole logical-layout owner and
chooses the surviving pane/focus result under its existing close rules.
Polariton does not rewrite `libaryon_layout`, fabricate a census, or interpret
placement. Repeated projections while the command is in flight must not emit
an unbounded stream of duplicate close commands.

The cleanup happens only after the current projection has been used for the
PRODUCT-0062 join or its refusal. It therefore cannot make a restored
application's meanings fail by racing the census against which they are
validated, while a malformed or absent application owner cannot strand the
retired UI. On the next checkpoint, panel 2 is naturally absent from
libaryon's census and no special tombstone enters either owner payload.

If panel 2 occurs in more than one restored view, every occurrence is closed.
Other documents, host panels, semantic mappings, groups, and colour
continuations survive. A layout containing only panel 2 still follows
libaryon's ordinary last-view/last-pane invariant rather than being replaced by
a Polariton-authored layout.

There is no migration for the nested dashboard's derived kernels or settings.
PRODUCT-0061 explicitly refused to persist them, so no committed generation
contains content that could honestly be recovered.

### 5. The dashboard implementation is deleted; kernels remain

After §§1-4 are green:

- remove `gui/dashboard` and the dashboard-only `gui/pane_grid`;
- remove `DashboardState`, `DashboardMsg`, `DashboardEffect`, resize ticks,
  nested pane rendering, `open_dashboard`, `dashboard_bounds`, and
  `rune_plot_panes`;
- remove the Plot -> New submenu and its `PlotType::ALL` message parser;
- remove `AppState.data_frame`, whose remaining writes exist only to seed or
  rebuild dashboard plots;
- pare `gui/plot_pane` down to `HostPlotState`, the host plot view, and the
  interaction message it still consumes; remove `PlotPaneState` and its
  dashboard-only settings editor;
- remove dashboard-special casing from `PanelRegistry::surface` and from the
  editor view signature.

Plot kernels, factories, `PlotType` variants, theme models, and direct tests do
not depend on that UI ownership and remain. Making a currently picker-only
kernel authorable still requires its own surface record.

`PlotType::ALL` also remains, deliberately. Its two current consumers -- the
New submenu and dashboard settings picker -- disappear, but the constant is
reclassified from picker input to the exhaustive renderer catalog. Surface
records need one checkable answer to "which substrate variants exist?", and a
catalog beside the enum is a better owner than reconstructing that list from a
retired UI. A narrow architecture gate compares the enum's declared variants
with the catalog entries exactly, including duplicate detection. This is a
source-shape property by definition; no runtime reflection exists from which a
behavioural oracle could recover the enum vocabulary.

The dashboard settings editor is retired with the dashboard. Rune-authored
plot settings are the accepted authoring surface; this record does not invent a
second host-panel settings modal.

### 6. This supersedes the dashboard deferrals without changing schemas

This record closes the dashboard-fate deferrals in PRODUCT-0010, PRODUCT-0061,
PRODUCT-0062, and PRODUCT-0074.

- PRODUCT-0010's export and resize obligations move to ordinary host panels or
  disappear with the nested owner.
- PRODUCT-0061's dashboard topology exclusion becomes deletion plus the panel-2
  tombstone; libaryon remains the only logical-layout owner.
- PRODUCT-0062's exclusion of `rune_plot_panes` is superseded by eliminating
  that map. Plain plots enter its existing `(DocumentId, LocalViewIdentity)`
  relation instead.
- PRODUCT-0074's accepted host-panel geometry is the prerequisite evidence for
  deleting the spatial alternative, not a promise that the deletion itself is
  pure subtraction.

`libaryon_documents`, `libaryon_layout`, and `polariton_session` bytes keep
their current schema versions. The runtime addition of an existing
`Anonymous(0)` mapping changes payload population, not payload grammar.

## Implementation sequence

1. Bank the current plain Rune cardinality, Markdown output routing, dashboard
   export formats, legacy panel-2 restore, and direct construction of
   picker-only kernels. No pixels or accepted spelling move in this step.
2. Add the plain-output identity bridge. Prepare `Anonymous(0)` before kernel
   construction, populate the resulting ordinary host panel on completion,
   reconcile an empty output on a no-plot run, and prove v2 bytes remain v2.
   Keep the dashboard code present but unused by this path.
3. Add plot content to the shared Results panel and route Markdown
   `output = pane` through it. Bank table -> plot -> message -> table
   replacement and the inline/none controls.
4. Implement host-panel export snapshots and move Plot -> Export to them while
   the old dashboard remains available as a comparison oracle. Render all
   three formats, verify dimensions and deduplication, and make the empty case
   explicit.
5. Add post-join retirement of every projected panel-2 occurrence. Checkpoint
   and reopen the cleaned layout before deleting the old producer.
6. Delete the nested dashboard, picker, dashboard-only plot editor, obsolete
   data-frame cache, and special rendering paths. Retain kernel/factory tests
   and re-document `PlotType::ALL` as the exhaustive renderer catalog.
7. Update the named premise tests and architecture guards from PRODUCT-0010,
   PRODUCT-0061, PRODUCT-0062, and PRODUCT-0074. Document export's repaired
   source and the panel-2 tombstone in the implementation evidence.
8. Run a live acceptance with: one plain Rune scatter/line plot; a Markdown
   `output = pane` plot replacing a table in Results; the dense `test24.rn`
   host-panel figure; export of the visible plot set; and a saved-session
   checkpoint/reopen showing the plain plot as an honest unresolved host panel
   that repopulates in place on rerun. Capture the final host-panel figure; no
   dashboard comparison is required because PRODUCT-0074 already banked and
   accepted it as the deletion gate.

## Gates

1. A plain Rune buffer publishing one plot prepares exactly
   `(producer, Anonymous(0))`, opens an ordinary expansive host-panel group,
   and never opens panel 2 or constructs a dashboard pane. Its existing
   zero/one/many request rule is unchanged; two requests still refuse, and no
   `.views` spelling is required.
2. Checkpointing the plain plot emits `polariton_session` schema version 2 and
   an ordinary anonymous panel mapping. A current v2 decoder round-trips it; an
   earlier valid v2 payload still decodes; no `Plain`, `Dashboard`, or third
   identity variant enters the durable vocabulary.
3. Checkpoint, quit, and reopen a plain plot. It restores as an unresolved
   ordinary host panel under the same `HostPanelId`; rerunning the buffer
   populates that exact panel. A new session mints a new identity, and changing
   the document path does not. This gate uses a scratch document as its
   pathless control.
4. Rerun a plain buffer after removing its plot request and assert its prior
   panel becomes orphaned rather than remaining fresh. Change the same producer
   to a relation view set whose first unnamed sink is `Anonymous(0)` and assert
   it reuses, rather than rebinds or duplicates, the panel. A named sink is the
   control and receives its own identity.
5. A Markdown Rune plot with `output = pane` replaces the shared Results panel
   in place. A pane-targeted table then replaces that plot, and a later plot
   replaces the table, all under panel ID 1 with monotonically refreshed
   revisions. `output = inline` still creates only inline content and
   `output = none` creates neither pane nor inline content.
6. Plot -> New and every `plot:new:*` action are absent. Plot -> Export SVG,
   PNG, and AVIF remain visible and dispatch their existing format messages.
   No accepted Rune or Markdown spelling is removed.
7. Export enumerates every projected `PanelContent::Plot` from plain Rune,
   relation `.views`, and the shared Results panel; excludes table, message,
   running, failed, and unresolved content; and includes a visibly stale last-
   good plot. Two viewport occurrences of one `HostPanelId` produce one file.
8. Export order follows first viewport occurrence and produces exactly
   `plot_1` through `plot_N` for each selected format. Two equal display titles,
   a title containing `/`, and a non-ASCII title neither collide nor influence
   filenames.
9. Export dimensions equal the rendered host plot canvas after subtracting one
   pane header and two 4px padding edges from the projected pane extent.
   Borders are asserted to remain inside that result rather than being counted
   twice. A deliberately undersized pane refuses its export with no zero-sized
   renderer construction.
10. SVG, PNG, and AVIF export each produce a non-empty artifact containing the
    expected plot, with the existing backend-license tests still green. With no
    projected plot, no background task or file is created and the exact status
    is `No live plots to export.`
11. Restore a saved layout containing panel 2 beside a document, a result
    panel, and two persisted Rune panels. The PRODUCT-0062 meanings join first;
    then every projected occurrence of panel 2 receives one bounded close
    intent. Other panel/group/colour identities remain exact, and checkpoint
    plus reopen contains no panel 2.
12. Corrupt or omit Polariton's application owner in the gate-11 fixture and
    prove legacy panel cleanup does not reinterpret, repair, or synthesize that
    owner. A session with no panel 2 emits no cleanup command. Panel 2 never
    appears in the semantic map, while the full shared census still includes it
    until libaryon processes the close.
13. Production source contains no `DashboardState`, `DashboardMsg`,
    `DashboardEffect`, `rune_plot_panes`, dashboard-special `PanelRegistry`
    branch, `plot:new:`, or Polariton `gui::pane_grid`. The retired source
    directories are absent. The only fixed value 2 reference is the named
    migration tombstone and its tests.
14. Every picker-only plot kernel and `PlotType` variant that existed before
    this record still compiles and its direct kernel/factory tests pass. The
    absence of a GUI producer is not discharged by deleting or weakening those
    tests. `PlotType::ALL` remains, has no duplicate, and an architecture gate
    proves its entries equal the variants declared by `PlotType` exactly; it is
    documented as renderer vocabulary rather than picker data.
15. `AppState` no longer holds the dashboard's derived `DataFrame` cache, and
    ordinary SQL/Markdown result publication still passes its existing table,
    registry, and UAST-owner gates without changed result bytes.
16. The full workspace suite, all-target/all-feature check, PRODUCT-0057 render
    evidence target, and saved-session process gates pass with no ignored or
    tolerated failure. Any parallel timeout is triaged using the established
    isolation discriminator rather than labelled flaky from one sweep.
17. The live acceptance in implementation step 8 shows no dashboard host
    panel or nested gutter, preserves plain and Markdown pane plots, exports
    the visible Rune figures, and repopulates the restored plain panel in
    place. The exhibit and generated files are evidence; they do not replace
    gates 1-16.

## Consequences

Polariton has one logical pane owner. Every durable plot visible in the editor
occupies a libaryon host panel; plot content and color meaning remain
Polariton's, while placement and close behaviour remain libaryon's.

The manual Plot -> New catalog and its interactive dashboard settings editor
are gone. Kernels without Rune spellings remain tested substrate, not hidden UI
features. `PlotType::ALL` remains their exhaustive catalog. Giving one a
language surface remains an explicit product decision.

Plain Rune gains durable placement without gaining new language or persistence
vocabulary. The bridge makes explicit a fact its cardinality rule already
implied: the sole output is anonymous ordinal zero.

Markdown pane plots become intentionally singular, like SQL results. Running a
new pane-targeted result replaces the previous one rather than growing a
pinboard. Authors who need simultaneous durable figures use named `.views`.

Export finally covers Rune-authored figures. It exports currently visible
host-panel plot state, including a stale last-good rendering, and uses simple
invocation-local filenames rather than pretending titles are safe file
identities.

Old saved dashboard placement is removed on first projection and disappears
from the next checkpoint. Its derived content was never persisted, so no user
data is discarded by declining to reconstruct it.

## Rejected

- **Require every plot to use `.views`:** removes an accepted plain Rune
  spelling to simplify a UI retirement. Surface removal does not authorize a
  language change.
- **Add `LocalViewIdentity::Plain` and schema version 3:** duplicates a fact
  already represented by `Anonymous(0)`. The plain path's one-plot constraint
  proves the ordinal without extending the durable vocabulary.
- **Keep `rune_plot_panes` beside the durable semantic map:** preserves two pane
  identity systems and leaves plain plots unable to survive restart.
- **Send Markdown pane plots to a new fixed panel:** multiplies result roles
  without a need. Tables and plots are mutually replacing results already.
- **Delete Markdown `output = pane`:** avoids one routing change by removing an
  accepted spelling that has a coherent shared-results destination.
- **Keep export dashboard-only:** makes the command useless for the authored
  plotting surface and retains dead state solely to serve a stranded feature.
- **Export every kernel in `PanelRegistry`, projected or not:** can emit closed
  or pending-disposal content the user cannot see. Export describes the current
  window.
- **Use plot titles as filenames:** introduces sanitization, collision, and
  non-UTF-8 display-policy questions unrelated to retirement.
- **Persist or translate `DashboardState`:** serializes derived kernels and a
  second layout tree PRODUCT-0061 explicitly excluded. No committed dashboard
  content exists to migrate.
- **Leave restored panel 2 unresolved:** turns intentional retirement into a
  permanent damaged-looking pane and keeps a deleted fixed role in every later
  census.
- **Delete picker-only kernels with their picker:** confuses reachability with
  renderer ownership and makes future surface records rebuild accepted
  substrate.
- **Delete `PlotType::ALL` when its picker consumers leave:** removes the one
  explicit renderer catalog merely because its first consumers were UI code.
  Reclassify and guard it instead of leaving it accidental or reconstructing it
  ad hoc in each later surface record.

## Stop conditions

- If the plain fallback can publish more than one accepted plot or can coexist
  with a relation view set in one revision, halt. `Anonymous(0)` is licensed by
  the proven cardinality and mutual exclusion, not by convenience.
- If assigning `Anonymous(0)` requires a new durable field, identity variant,
  or schema interpretation, halt and write the schema record rather than
  widening v2 under its existing number.
- If host-panel export cannot derive the same content extent the production
  view renders without reconstructing logical placement, halt. Export may read
  projected geometry; it may not become a second layout owner.
- If closing legacy panel 2 before or after the PRODUCT-0062 join changes the
  meaning of another panel, group, or colour relation, halt and repair the
  migration ordering before deleting the producer.
- If preserving plain Rune, Markdown `output = pane`, or all three export
  formats requires retaining nested dashboard ownership, halt and narrow the
  deletion openly rather than silently dropping a capability.
- If dashboard deletion requires removing a kernel, changing an accepted Rune
  spelling, or altering any session/layout schema version, halt. Those are
  separate records, not cleanup.
