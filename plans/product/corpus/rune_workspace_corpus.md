# Rune workspace acceptance corpus

Version 0.40. Charter: PRODUCT-0002 §9. This is a living, versioned acceptance
artifact — not an ADR. It records the complete destination matrix for the Rune
workspace with honest per-family status. A row's status changes only with
landed, cited evidence; the only expansion path for new rows is dogfood diary
evidence. "Pending" rows are the destination stated in advance, not omissions.

Statuses: **executable** (landed with cited evidence) · **pending** (destination,
not yet implemented) · **proposed** (a spelling under consideration; no accepted
record, and not a commitment) · **deferred-by-design** (excluded deliberately by
a cited record).

## Relation construction and transformation

| family | status | evidence |
|---|---|---|
| workspace table lookup (`workspace.table`) | executable | `rune_source_executes_over_the_authenticated_query_boundary_with_typed_failures` |
| select / projection (`project`) | executable | same real-transport fixture |
| filter / predicate | executable | same real-transport fixture |
| group / aggregate (`aggregate`) | executable | same real-transport fixture |
| sort | executable | same real-transport fixture |
| limit | executable | same real-transport fixture |

## Views

| family | status | evidence |
|---|---|---|
| bounded table view (`table(r)`; accepted sugar `r.table()` and canonical `r.views([table()])`, inherits the 128 MiB `toron-client` collection ceiling) | executable | `terminal_table_is_substituted_and_mapped` + `rune_relation_results_feed_the_existing_table_and_plot_surfaces`; live acceptance `live_rune_relation_reaches_table_bar_and_scatter_surfaces`; PRODUCT-0008 sugar equivalence in `product_0008_single_view_sugars_are_exact_canonical_projections` (`d4c2d8a8`) |
| theme-projected table skin (`table().skin("theme_expression")`; optional `.duotone()`) | executable | PRODUCT-0025 establishes identity resolution, atomic frame/skin lifecycle, and the closed table register. PRODUCT-0027 makes named absence select PaletteForward and `.duotone()` select the preserved two-anchor treatment (`74d00472`). PRODUCT-0035 makes authored contrast observational: every mechanically defined projection renders exactly, including `yellows.duotone()`, both `dark2_s1` projections, and identical paper/ink, without advice or repair (`cb2daa89`). Unknown identity and other no-defined-answer cases still refuse. Bare Rune, SQL, and Markdown tables retain PRODUCT-0023's contrast-gated compiled default; plot marks still refuse `.skin(...)`. |
| bar sink (`plot.bar`; accepted sugar `r.bar(x, y)` and canonical `r.views([bar(x, y)])`) | executable | `plot_forms_retain_only_view_intent` + `rune_relation_results_feed_the_existing_table_and_plot_surfaces`; live acceptance with axis-identity assertions (PRODUCT-0007 stage 2, `755e850a`); PRODUCT-0008 sugar equivalence (`d4c2d8a8`) |
| scatter sink (`plot.scatter`; accepted sugar `r.scatter(x, y)` and canonical `r.views([scatter(x, y)])`) | executable | as bar, plus the categorical-x resolution that closed the field-substitution defect (PRODUCT-0007 §2, `755e850a`); PRODUCT-0008 sugar equivalence (`d4c2d8a8`) |
| line sink (`plot.line`; accepted sugar `r.line(x, y[, series])` and canonical `r.views([line(x, y[, series])])`) | executable | PRODUCT-0012 establishes the two-field form. PRODUCT-0054 adds the optional named series channel in every standalone position, preserves within-series relation order, routes exact resolved colors through paint/legend/composites, admits native `.stroke_width(...)`, and keeps categorized lines out of `layers([...])`; `test18.rn` is the four-product exhibit. |
| shared-execution view fan-out (`r.views([...])`, one execution, N sinks) | executable | `rune_relation_results_feed_the_existing_table_and_plot_surfaces` proves every sink, including line, holds the *same* `Arc`, not equivalent copies; direct/factored/legacy equivalence plus accepted-surface live axis identity (`d4c2d8a8`, PRODUCT-0012) |
| multi-origin view program (one execution per canonical relation origin) | executable | `multi_origin_dispatch_is_independent_while_each_origin_keeps_shared_identity` proves canonical-origin deduplication and independent dispatch; `execution_grouping_cannot_reorder_interleaved_host_panels` completes origins out of order without changing presentation (`b99338f8`, `cad42680`) |
| optional view identity (`view("name", sink)` or anonymous positional identity) | executable | session-scoped: duplicate names refuse with both spans; names retain one host-panel id across reruns and form changes; anonymous views reuse their presentation slot; durable restart identity remains deferred by PRODUCT-0010's recorded protocol halt (`284e88e0`, `cad42680`) |
| producer-scoped view panel placement (automatic visible tiling; no inferred tabs) | executable | PRODUCT-0015's thirteen gates: `product_0015_host_panel_groups_tile_atomically_and_fallback_visibly` proves atomic 2x2 placement and all-visible decline; `rune_view_collection_tiles_one_forward_producer_group_without_tabs` proves forward table/layered/table/bar order, Compact/Expansive sizing, unchanged reruns, and Retile no-op; detachment/reset/lifecycle and two-producer isolation land in `6d67e95b`, `fd968496`, `a287b770`, and `67c8f2f5`. PRODUCT-0011's explicit `Split`/`TabWith` remains unchanged. |
| layered plot sink (`layers([line(x, y), scatter(x, y)])`) | executable | PRODUCT-0013's ordered substrate and PRODUCT-0014's surface are accepted; thirteen surface gates land in `c3edc520`, `a10b4995`, `56c8cb88`, and `d52fc31d`, with installed `test6.rn` dogfood confirming labels, paint order, ordinary host panels, and no dashboard |
| figure-wide headings (`figure([title(text), subtitle(text), body])`) | executable | PRODUCT-0016's twelve gates in `4278e762`, `267e48c1`, `a39ffd9d`, and `764e2b64`; exact grammar spans, independent settings/body lowering, projected source-current chrome, rendered completion-atomic canvas, stale/removal lifecycle, and installed `test7.rn` launch |
| authored numeric axis bounds (`figure([x_bounds(min, max), y_bounds(min, max), body])`) | executable | PRODUCT-0036 adds paired finite literal bounds at figure scope and lowers both axes through one existing domain override (`2ad9357e`). Numeric applicability is checked per resolved axis: a vertical bar admits `y_bounds` and attributes categorical `x_bounds` locally. Excluding all data and any strictly ordered finite span, including `0.0..1e-300`, render exactly; collapse, inversion, non-finite values, malformed literals, and duplicates refuse at the constructor. Layered plots retain one shared Cartesian domain; omission is unchanged. |
| authored numeric tick step (`figure([x_tick_step(step), y_tick_step(step), body])`) | executable | PRODUCT-0038 exposes the closed PRODUCT-0037 `Step` placement through figure-wide guide settings. Positive finite literals, including direct integer and decimal spellings, lower independently per axis; bounds are optional and data-derived domains use the zero-anchored lattice. Categorical/band, temporal, and dense-capacity requests refuse at the exact constructor span. Omission remains Count(8), layered bodies retain one shared placement, and no minor/count surface is added (`2385a8d5`, `3c1b277f`). |
| authored minor tick subdivision (`figure([x_minor_ticks(count), y_minor_ticks(count), body])`) | executable | PRODUCT-0039 exposes the existing per-interval minor subdivision count through figure-wide guide settings. Zero disables marks and grids together; positive non-negative integer literals lower independently, and Count/Step major placements produce exactly that many strictly interior ticks. Categorical/band, temporal, and combined-lattice capacity requests refuse at the authored span; checked arithmetic prevents overflow before allocation. Omission preserves the default count of 4, with visibility flags and absolute spacing remaining internal (`a914f46c`). |
| static literal bindings in figure elements and mark fields (`let name = literal`) | executable | PRODUCT-0040 resolves direct and aliased string/numeric literals for title, subtitle, skin, palette, bounds, major tick step, and minor tick count at the envelope boundary. PRODUCT-0058 completes every established bar/line/scatter field position: bound fields become concrete strings before schema/channel and bar-pair resolution, while numeric register bindings remain separate. Computed values remain refused, bindings are retained, `test13.rn` factors headings, and `test16.rn` factors category/value/series fields. |
| theme-projected plot chrome (`figure([skin("theme_expression"), body])`; optional `.duotone()`) | executable | PRODUCT-0032 establishes exact figure grammar and atomic host lifecycle. PRODUCT-0033 makes ordinary skin preserve the ordered background slot and retains strongest-pair collapse as explicit `.duotone()` in `5871be41`. PRODUCT-0034 makes contrast observational: every known theme renders exact projected chrome, including below-threshold `tab20c_s9`, while unknown identity still refuses (`f904536d`). Skin never colors data; palette never colors chrome; omission preserves PRODUCT-0031's compatibility default. |
| relative scatter point size (`scatter(x, y).point_size(factor)`) | executable | PRODUCT-0017 proves the typed layer setting and all scatter renderer/hit/cache paths; PRODUCT-0018's closed contextual register, exact lowering/refusals, shared-result handoff, byte-identical `1.0`, six-pixel-radius `2.0`, identity/label lifecycle, and no-dashboard host completion land in `5d180e0c` and `0478f8a2`; installed `test7.rn` exercises the accepted `8.0` maximum |
| relative line stroke width (`line(x, y).stroke_width(factor)`) | executable | PRODUCT-0019 proves the typed layer setting, five-tier selection, exact line paint/hit behavior, fixed guide, and styled-one-layer scale preservation; PRODUCT-0020's closed contextual lowering/refusals, shared-result handoff, byte-identical `1.0`, exact 4px `2.0`, identity/label lifecycle, and combined point-size host assay land in `01a66f4d`; installed `test7.rn` exercises both modifiers in one figure |
| normalized bar slot width (`bar(category, value).bar_width(fraction)`) | executable | PRODUCT-0021 proves the typed `(0, 1]` Layer-stage setting, default `0.9`, five-tier selection, full-band subgroup allocation, touching `1.0`, shared vertical/horizontal paint-hit rectangles, exact padding misses, fixed preparation/layout/guide output, and no Bar layer admission in `bf4be7a0` and `671ce765`; PRODUCT-0022's closed contextual lowering/refusals, shared-result styled-Bar handoff, byte-identical bare/`0.9`, narrow `0.5`, identity/heading/placement lifecycle, and no-dashboard host assay land in `db4d2be6`; installed `test7.rn` exercises near-touching `0.9999` beside the existing line and scatter modifiers |
| durable live plot | pending | — |
| durable pinned plot | pending | — |

## Accepted extension: multi-origin views and view identity

Status: **accepted** by PRODUCT-0010 and executable in `284e88e0`, `b99338f8`,
and `cad42680`. PRODUCT-0008's one-relation `ViewSet` remains the unit of shared
execution. PRODUCT-0010 wraps several such units in one buffer program:

```text
BufferViewProgram {
    executions:         canonical origin -> ordered sinks,
    presentation_order: ordered view identities,
}

LocalViewIdentity = Named(name) | Anonymous(presentation_position)
```

Canonical origin identity is textual after the existing binding resolution and
`ket.` → `workspace.` fold. Equal origins execute once and every sink in that
execution shares one stamped `Arc`; distinct origins execute independently and
may complete in any order. Execution grouping cannot change the global
PRODUCT-0008 presentation order.

`view("name", sink)` is an identity wrapper recognized only inside a proven
`.views([...])` list, not a fourth sink. Names are unique per buffer and buy a
stable session address; anonymous views remain freely repeatable and use global
presentation position. Polariton realizes both as ordinary host panels, so two
tables and two plots from the dogfood buffer have four destinations and no view
completion opens or mutates the dashboard. The current key uses `BufferId` and
therefore survives reruns, not application restarts; durable document identity
and restored layouts require a future libaryon record.

## Accepted extension: line views

Status: **accepted** by PRODUCT-0012 and extended by PRODUCT-0054. The legacy,
relation-sugar, and canonical forms are exact projections of one line intent
with two authored strings plus an optional categorical series string. A named
series lowers to the resolved color channel, partitions paths without crossing
or implicit sorting, and consumes exact resolver assignments in both paint and
legend. The two-field synthetic path remains unchanged. Categorized lines are
standalone marks, inherit `.stroke_width(...)`, and remain excluded from
`layers([...])`. Origin-keyed execution, shared-result identity, view addressing,
panel placement, and staleness remain unchanged.

## Accepted substrate: ordered shared-Cartesian layers

PRODUCT-0013 is accepted and executable in `9b581151`, `446c9216`, `4c35071d`,
and `0e565a9c`. It adds
`LayeredPlotSpec { layers: NonEmpty<LayerSpec> }` beside `AuthoredPlotSpec` and
admits only line and scatter marks. Each layer owns and independently resolves
its encoding; every layer must resolve identical x/y field identities and scale
families, and color must be `Absent`.

One shared Cartesian domain is derived from the common source columns before
mark preparation. Extracted line and scatter renderers consume that layout and
paint first-to-last in authored order; SVG evidence observes scatter marks after
the line path for `[line, scatter]`. One-layer specs retain their existing
kernels and exact SVG output. The required `PlotKernel` hover and legend methods
use a temporary, non-gating bridge: topmost geometric hit first and the last
authored layer's unmerged legend. PRODUCT-0013 itself added no Rune envelope,
panel, dashboard, transport, guide, or interaction surface; the separately
accepted PRODUCT-0014 surface below is its first production caller.

## Accepted surface: the Rune layers surface

Status: **accepted** by PRODUCT-0014, with all thirteen implementation gates
executable in `c3edc520`, `a10b4995`, `56c8cb88`, and `d52fc31d`. The installed
`test6.rn` dogfood run closes the visual proof: `Line + Scatter` on the canvas,
`Line + Scatter 2` in anonymous panel chrome, scatter vertices above the line,
four ordinary host panels, and no dashboard.

Inside a proven `.views([...])` list,
`layers([line("x", "y"), scatter("x", "y")])` is one sink, one identity, and
one host panel. The literal list is structurally non-empty and preserves authored
paint order. Marks are line or scatter with exactly x and y string fields;
empty, nested, non-literal, unsupported-mark, and colored-scatter forms refuse
at their decided spans. Outside the proven position, `layers(...)` remains
ordinary retained Rune.

Lowering constructs PRODUCT-0013's `LayeredPlotSpec` and delegates all field and
scale compatibility to its factory. Rune host panels now consume
`ViewPresentation { content_kind, display_label }` plus a kernel through
composition-free `HostPlotState`; the dashboard retains its separate
composition-owning state unchanged. `[line, scatter]` displays exactly
`Line + Scatter`, while named panel chrome continues to display the view name.
One-layer form replacement retains positional identity and renders byte-identical
SVG to the established bare mark.

## Accepted surface: the Rune figure surface

Status: **accepted** by PRODUCT-0016 and executable in `4278e762`, `267e48c1`,
`a39ffd9d`, and `764e2b64`. Inside a proven `.views([...])` sink position,
`figure([...])` carries zero or one literal `title`, zero or one literal
`subtitle`, and exactly one existing mark or `layers` body. Metadata order is
irrelevant; layer order remains paint order. Outside that position, `figure`,
`title`, and `subtitle` remain ordinary Rune.

Lowering preserves two independent artifacts: the body follows its existing
single or layered specification path, while headings enter PRODUCT-0007's
authored `PlotSettingsLayer` as `Presence::Show`. Omission remains `Inherit`;
computed text and title hiding are not accepted. The existing
`Presence::Hide -> None -> generated label` defect remains explicitly deferred.

Panel chrome is source-current and uses view name, else source-authored title,
else the generated structural label, retaining the anonymous positional suffix.
Canvas headings resolve through the full settings cascade and become visible
atomically with the completed kernel. A failed revision can therefore show its
new source title in chrome while retaining the prior complete canvas as stale;
later heading removal restores the generated canvas/chrome fallback without
changing `HostPanelId`, group state, geometry, tabs, focus, routing, or dashboard
state.

## Accepted surface: view fan-out

Status: **accepted** by PRODUCT-0008 and executable in `d4c2d8a8`. The view rows
carry the accepted spelling beside the original free-function spelling and retain
every earlier evidence citation. This section remains the detailed invariant and
recognition contract the implementation inherits.

### Load-bearing invariant

> `RuneDataFrame` → `ViewSet` is simultaneously the type-state boundary, the
> fan-out boundary, and the Toron–Polariton process boundary. `.views([...])` is
> the transition.

The three coincide, which is the argument for the surface rather than a
consequence of it:

- **Type-state.** `RuneDataFrame` has no sink methods; `ViewSet` has no relation
  methods. A chain that appears to make one view consume another is
  unrepresentable rather than merely discouraged.
- **Fan-out.** Sinks in a `ViewSet` are siblings over one relation. This is the
  same algebra as the shared-execution fan-out row above: one execution, N sinks,
  one stamped result shared by identity.
- **Process.** Everything at or below `RuneDataFrame` executes on Toron;
  everything above `.views([...])` stays in Polariton. PRODUCT-0002's
  execution/presentation split stops being a rule a reviewer enforces and becomes
  one the receiver kind enforces.

`.views([...])` names the operation that actually occurs — fan one relation into
several presentation sinks — without asserting that every sink consumes an
encoding. Encoding is a property of the marks that have one, not of the boundary,
so channels live inside the individual plot specifications where richer
Vega-style channel sets naturally belong.

### Recognition by receiver provenance

The invariant is stated in terms of `RuneDataFrame`, which is Toron's type.
Polariton has no access to it: rune 0.14 exposes no type model, so the envelope
analysis cannot know what any expression returns. Recognition therefore runs on a
locally tracked **kind**, deliberately named `RelationCandidate` rather than
`Relation`, because it records *provenance* and claims nothing. Toron still
validates whether the peeled remote expression actually returns a relation, and
a candidate that does not is a server-side typed failure exactly as today.

Kind is rooted at `workspace` — the injected entrypoint parameter — and
propagates:

```text
workspace-rooted chain
  unknown method        → RelationCandidate
  .views([...])         → ViewSet
  direct .scatter(x, y) → sugar for .views([scatter(x, y)])
  direct .bar(x, y)     → sugar for .views([bar(x, y)])
  direct .table()       → sugar for .views([table()])

ViewSet
  .views([...])         → ViewSet with the further sinks appended
```

Bindings preserve the inferred kind and the accumulated specification. Binding
alone emits nothing. Only a **top-level expression of `ViewSet` kind** becomes an
output root.

Two properties of this table are load-bearing:

- **Unknown methods are kind-preserving.** The analysis must never enumerate
  which methods return relations, because `project`, `filter`, `aggregate`,
  `sort`, `limit`, `join`, and `window` are precisely the names the PRODUCT-0003
  containment assay forbids it from knowing
  (`../impl/0003_typed_output_root_discovery.md`). Stating propagation
  negatively keeps the analysis ignorant by construction rather than by
  discipline: `workspace.table("t").sort([...])` stays a `RelationCandidate`
  without the analysis holding any opinion about `sort`.
- **`.views(...)` is kind-preserving on a `ViewSet`.** Applying it again appends
  rather than nesting or replacing, so sinks accumulate in list order and then
  chain order. Sink **order** therefore falls out of program order without a
  separate rule, and every sugared form reduces to the same accumulation.

Receiver provenance is a narrower recognition rule than the free-function name
matching it replaces. `table(r)` and `plot.bar(...)` are global names a user may
legitimately own, which is why step 1 needs whole-buffer shadow analysis; a sink
method is recognized only on a receiver the analysis already traced to
`workspace`, so a user-defined `.scatter()` on an unrelated value is never a
root. The shadowing machinery is a compatibility concern for the current
spelling, not a permanent cost of the proposed one.

### Sink constructors are contextual

`scatter(...)`, `bar(...)`, and `table()` are recognized as sink constructors
**only as arguments to `.views([...])`**. The identical call written anywhere
else is ordinary Rune and reaches the executor untouched.

This is receiver provenance applied one level inward: the argument position has
already been proven to belong to a `RelationCandidate` → `ViewSet` transition, so
the names carry meaning only where that proof exists. Reservation is confined to
a position rather than claimed over a namespace, which is what keeps a
user-defined `fn table(x)` and a bare `table(1)` legal — the same collision class
step 1 must handle for the current free-function spelling, and which this
spelling avoids by construction.

### Normal form

```text
ViewSet {
    relation_origin,
    sinks: flat ordered list,
}
```

Every surface operation appends to that list. Nesting never enters the
representation, so list order followed by chain order **is** execution order by
construction rather than by a rule someone has to maintain.

This remains the per-origin shape. PRODUCT-0010's `BufferViewProgram` groups
these sets by canonical relation origin and separately carries global
presentation order. The separation preserves this local fan-out algebra while
preventing execution grouping from becoming visible in the UI.

### Form hierarchy

| form | role |
|---|---|
| `r.views([...])` | canonical algebra: a relation and an explicit list of sinks |
| `r.scatter(x, y)` | single-view sugar, defined as exactly `r.views([scatter(x, y)])` |
| `r.bar(x, y)` | single-view sugar, defined as exactly `r.views([bar(x, y)])` |
| `r.table()` | single-view sugar, defined as exactly `r.views([table()])` |

Written out, the canonical form is the working snippet:

```rune
workspace
    .table("test2")
    .sort([asc(col("cat"))])
    .views([
        scatter("cat", "val"),
        bar("cat", "val"),
        table(),
    ])
```

Rules the hierarchy must satisfy:

- **Sugar is a strict projection.** Each sugared form is definable in terms of
  the canonical form and may carry no semantics the canonical form cannot
  express. A sugared form that grows a private option is a second language.
- **Binding is lazy.** Constructing a `ViewSet` executes nothing and emits
  nothing. Only a top-level displayable expression emits output; a
  bound-but-unused set renders nothing.
- **Sinks need not be alike.** `scatter(...)` and `bar(...)` carry channels;
  `table()` carries none. Because the boundary is `.views([...])` rather than an
  encoding, a grid sitting beside two marks requires no explanation — there is no
  ambient encoding for it to ignore.

Receiver kind also retires the earlier namespace question. `workspace.table(name)`
selects a source and `table()` inside a `views` list constructs a grid sink; the
position and receiver disambiguate, so neither a `plot.` nor a `view.` prefix is
required to keep them apart.

### Equivalence acceptance cases

The direct and factored forms are one acceptance obligation, not two:

```rune
r.views([scatter("cat", "val"), bar("cat", "val")]);
```

```rune
let v = r.views([scatter("cat", "val"), bar("cat", "val")]);
v
```

They must be indistinguishable in executions issued, sinks produced, and stamped
result identity.

### Requirements this surface imposes

1. **Typed output-root discovery is a prerequisite, not an optimization.** The
   current envelope recognizes a contiguous trailing view block positionally. In
   the factored form the boundary is a binding rather than a chain position, so
   recognition must be driven by the receiver's type. Positional peeling cannot
   express the second acceptance case above, and will pass a demo while failing
   the first refactor a user performs.
2. **Data-changing operations lower before the boundary.** Vega-style `bin`,
   `aggregate`, and calculated fields must lower into `RuneDataFrame` operations
   before a `ViewSet` is constructed. After `.views([...])`, an operation may
   describe presentation but must never execute a data transformation locally. An
   operation that cannot be expressed as a relation operation before the boundary
   is a typed capability refusal, not a local fallback — otherwise the invariant
   degrades quietly into a second, undeclared execution engine inside the
   workbench.

### Respelling stop condition

> Accepting `.views([...])` may add only parsing, provenance analysis, and
> lowering into the existing `ViewSet` normal form. If implementation requires
> changing execution, channel resolution, durability, or rendering — or
> invalidates existing executable evidence — the work stops: the proposal is not
> a respelling and requires a broader record.

This is a stop condition rather than an acceptance checklist, and the difference
is where it acts. A checklist confirms completion at the end; this halts the work
the moment a change of the forbidden kind is required, which is when the
discovery is cheapest and before the record's stated scope has quietly become
untrue.

The corollary is that the accepting record's own evidence is small and
predictable: direct-versus-factored equivalence, sink ordering, and contextual
constructor recognition. Everything else it needs is already proved. Every
`executable` row above must still cite exactly what it cites today, with the new
record adding its own citations rather than replacing any.

### What this section does not decide

`PlotSpec` fields, scales, guides, themes, interaction, named views, and durable
plot binding remain open. The plotting record decides them; this section
constrains where they may live.

**Shared encoding is explicitly undecided.** An earlier draft made `.encode(x, y)`
the boundary, with sinks chained after it. That form is withdrawn: it asserted
that every sink consumes an encoding, which is false for `table()`, and it
promoted a convenience — hoisting channels two marks happen to share — into the
process boundary. `.views([...])` expresses the general algebra without it.

Shared encoding may return later as **presentation sugar over the sink list**,
lowering to the same `ViewSet` and carrying no semantics the list cannot express.
It must not become the boundary again, and nothing depends on it existing.

**Bindable sink specifications are undecided.** A form such as

```rune
let chart = scatter("cat", "val");
r.views([chart])
```

would require `SinkSpec` provenance — the same binding-and-kind analysis extended
to a third kind, so that a bound constructor stays recognizable inside a `views`
list. It is a natural extension of the existing mechanism rather than a
prerequisite, and the first plotting record need not solve it. Until it is
decided, sink constructors are recognized only in argument position.

## Accepted substrate: resolved table artifact skins

PRODUCT-0023 is accepted and executable in `f2dc10ae`, `e8b3f956`, `94749e4a`,
and `022f6ab6`. `TableSkin` is a closed twelve-role value for table-interior
chrome, separate from plot data ramps and editor `UiPalette`. Every production
built-in passes seven alpha-composited 4.5:1 text-contrast checks. The default
retains the exact pre-extraction Iced draw sequence, while a twelve-sentinel
assay proves every role reaches the real renderer without changing geometry,
text, selection, scrolling, or data.

SQL results, Rune table views, and Markdown cell tables store one
`TableArtifact { frame, skin }`; completion replaces the pair together and host
failure retains it together as stale. Drawing remains clipped to the artifact,
and a skin-only replacement cannot affect panel identity, descriptor, placement,
focus, tabs, geometry, Markdown container chrome, or libaryon. This substrate
adds no Rune capability, so no row above changes status. PRODUCT-0024 owns the
resolved libaryon palette prerequisite; PRODUCT-0025 owns the first
`.skin("name")` surface and the per-sink closed-register generalization.

## Accepted substrate: observable resolved theme palettes

PRODUCT-0024 is accepted and executable in `463ad2b5`, `69f25294`, `3b5c4939`,
and `5581f6ad`. Every libaryon theme now retains the exact ordered twenty-color
palette used to populate its themed syntax categories. A checked-in textual
oracle covers all 1,420 canonical, alias, modifier, shift, and composed
expressions across every syntax slot; it remains byte-identical through the
resolution refactor. Curated and continuous families expose the same fixed
`ResolvedThemePalette`, while `BodyText` retains its independent existing
behavior.

Libaryon now owns validated normalized encoded-sRGB luminance and contrast math.
Polariton retains table-specific alpha compositing and passes its fractional
visible colors without eight-bit quantization, preserving PRODUCT-0023's seven
established ratios. The palette's contrast-pair query is total and deterministic,
returns source indices as well as colors, and imposes no artifact threshold.
Reverse and shift remain observable order-preserving permutations; continuous
clipping changes the sampled set, while curated clipping's existing no-op is
guarded without being endorsed. PRODUCT-0025 now consumes this substrate as the
first accepted table-skin projection and `.skin("name")` surface.

## Accepted surface: theme-projected Rune table skins

PRODUCT-0025 is accepted and executable in `5a526200`, `f8884673`, and
`4ba40bb3`. Inside a proven `.views([...])` sink position,
`table().skin("theme_expression")` carries exactly one authored libaryon theme
identity. The envelope holds no colors. Polariton resolves the name and all
modifiers only through `Theme::try_new`, then projects the exact ordered palette
into a complete `TableSkin` and admits it only after all seven alpha-composited
PRODUCT-0023 checks pass.

Unknown and resolved-but-unsuitable identities are different diagnostics and
neither falls back. Failure is per destination and preserves the previous
frame/skin pair stale. Bare Rune tables, SQL results, and Markdown tables retain
the byte/draw-compatible default. Skin remains content rather than identity or
layout: changing it reuses the same host panel, never reaches `UiPalette`, and
never opens the dashboard. The installed `test8.rn` uses `terrain_rs10`; its
exact two-origin, four-sink source shape is an executable completion assay and
the live window remains the intentionally human aesthetic oracle.

## Accepted substrate: explicit table skin projections

PRODUCT-0026 is accepted and executable in `5de8826b`, `fe72f0b9`, and
`296ec1b3`. The accepted PRODUCT-0025 treatment survives byte-for-byte as the
explicit `Duotone` projection. A second `PaletteForward` projection spends eight
ordered swatches directly on table surfaces and accents, then selects fixed
near-black or near-white text per role group by measured maximin contrast over
the actual visible surfaces. Both modes still pass the unchanged seven
alpha-composited PRODUCT-0023 checks or refuse with mechanism-specific facts.

The exact 8,480-expression pre-refactor Duotone oracle remains unchanged.
`yellows` continues to refuse under Duotone because its completed selected-row
contrast falls below threshold despite an above-threshold raw anchor pair, and
now succeeds under PaletteForward with `#101010` ink. A constructed
split-polarity palette proves that either projection can refuse independently.
The Rune head still selects Duotone explicitly and no grammar row changes
status; PRODUCT-0027 owns the indivisible PaletteForward default flip and
`.duotone()` escape hatch.

## Accepted surface: palette-forward Rune table skins

PRODUCT-0027 is accepted and executable in `74d00472`, with historical and
runtime evidence recorded in `201e3f52`. The Rune table modifier register now
flattens a named skin and an optional zero-argument treatment into one bounded
intent:

```text
table()                                  -> compiled TableSkin::DEFAULT
table().skin(name)                       -> PaletteForward(name)
table().skin(name).duotone()             -> Duotone(name)
table().duotone().skin(name)             -> the same Duotone intent
```

The implementation commit contains both `.duotone()` recognition and the
PaletteForward named-skin default flip; its parent contains neither. `yellows`
completes under ordinary `.skin(name)` and explicit Duotone truthfully refuses
with a verified instruction to remove the modifier. Unknown names are never
projection failures, and `dark2_s1` proves the both-projections-fail branch does
not recommend a broken alternative. Advice probes never become fallback
content.

Projection remains content rather than identity. Changing or removing
`.duotone()` reuses the destination and producer layout, replaces frame plus
skin atomically on success, and preserves the prior pair stale on failure.
Bare Rune tables and SQL/Markdown tables retain the compiled default. No plot
mark accepts `skin` or `duotone`; the ahead-of-language `test9.rn` fixture is
unchanged and supplies no acceptance evidence for plot skin syntax.

## Accepted substrate: optional categorical theme palettes

PRODUCT-0028 is accepted and executable in `0153dc83`, `9685e530`, and
`6880046c`. A resolved libaryon theme now retains an optional naturally ordered
categorical palette beside the unchanged twenty-position syntax palette.
Continuous families expose their twenty accepted samples. Exactly `tab20`,
`tab20b`, `tab20c`, `dark2`, `kelly`, and `prism` expose explicit curated data
sequences; other curated syntax themes such as `nord` remain valid and report no
categorical capability.

The two projections share one parsed theme identity but preserve their own
authoritative order and natural length. Thus `tab20c` exposes `c01..c20` for
data while retaining its deliberately contrasted syntax-role permutation, and
`dark2_s20` rotates its eight data colors by four while its twenty syntax slots
remain unchanged. A structurally non-empty immutable value exposes only length,
checked indexing, and ordered iteration. No geometry, domain, capacity,
diagnostic, artifact policy, Polariton production caller, or Rune row is added;
PRODUCT-0029 owns the first stable categorical plot-position consumer.

## Accepted substrate: stable categorical palette positions

PRODUCT-0029 is accepted and executable in `3d76e06b`, `13d9f7f4`,
`d13cde78`, and `94a0aeb6`. Categorical continuation state now retains a
validated palette position rather than baked RGB, while every completed
`ResolvedPlotSpec` remains self-contained with exact colors. The existing
unselected production resolver preserves its twelve colors, sequential
assignment, filtering lifecycle, capacity refusal, and bar rendering exactly;
PRODUCT-0030 owns the atomic switch to an authored palette.

The explicit substrate consumes PRODUCT-0028's natural sequence without
parsing theme grammar. A fresh six-category `tab20c` domain maps
`A..F -> 0,4,8,11,15,19 -> c01,c05,c09,c12,c16,c20`; reverse and shift keep
positions while consuming libaryon's exact permuted colors. Live growth retains
prior meanings (`A->0`, `B->19`, `C->9`), filtering reserves absent positions,
and geometry and palette capacity failures report either or both limits without
mutating continuation state.

For a two-field bar with no authored Color channel, the resolved artifact now
carries exact band-fill assignments. The kernel draws those colors in category
order without receiving a palette, allocator, or theme expression and without
originating an encoding or legend. Authored bar color remains authoritative;
scatter and line with absent color stay single-treatment. No Rune spelling or
production palette caller lands in this substrate cut.

## Accepted substrate: resolved plot artifact chrome

PRODUCT-0031 is accepted and executable in `6ea267e6`, `0f96e594`, and
`eb37715b`. Every completed plot now carries one closed eleven-role
`PlotChrome`; renderers consume those roles directly while legacy mark ink and
application/settings-drawer colors remain separate. The exact pre-extraction
SVG remains unchanged, including the deliberately measured—not endorsed—legacy
gridline contrast debt of approximately 1.66:1.

Five visible text roles are assayed at 4.5:1 and three non-text roles at 3:1
after alpha compositing. Plot chrome changes no mark color, encoded legend
swatch, guide content, geometry, interaction, identity, or Rune normal form.
PRODUCT-0032 owns the first theme-name projection and direct figure
`skin(name)` member.

## Mutation and definition lifecycle

| family | status | evidence |
|---|---|---|
| create | pending | — |
| insert | pending | — |
| update | pending | — |
| delete | pending | — |
| drop | pending | — |
| save (definition) | pending — blocked on artifact-store ADR | — |
| cache | pending | — |
| materialize | pending | — |
| pin | pending | — |
| export | pending | — |

## Durability

| family | status | evidence |
|---|---|---|
| materialized restart durability | pending | — |

## Deferred by design

| family | status | record |
|---|---|---|
| bounded finite `collect` | deferred-by-design | `plans/product/impl/0003_rune_relation_first_contact.md` |

## Change log

- v0.58 (2026-08-08): PRODUCT-0058 closes DB-03. Every established mark-field
  slot accepts PRODUCT-0040 static string bindings across legacy calls,
  relation sugar, canonical and named views, figure bodies, modified marks,
  and shared-Cartesian layers. The envelope resolves concrete field strings
  before schema/channel and pair-resolved bar orientation; identifiers do not
  enter authored or resolved plot structures. `test16.rn` now factors its
  repeated category, value, and series fields with zero pixel change.

- v0.40 (2026-08-06): PRODUCT-0040 accepts top-level names bound to source
  literals as arguments to figure presentation elements. Direct and chained
  string/numeric bindings feed headings, skin, palette, numeric bounds, major
  tick step, and minor tick count; computed expressions remain refused and the
  resolved values pass through each element's existing validation. Bindings are
  retained, and `../uat/test13.rn` demonstrates factoring shared headings across
  two figures.

- v0.39 (2026-08-06): PRODUCT-0039 exposes `x_minor_ticks(count)` and
  `y_minor_ticks(count)` as figure-wide subdivision controls. Zero disables
  minor marks and grids together; positive counts place strict interior ticks
  relative to each major interval under both Count and Step placement. The
  combined major-plus-minor lattice is checked with overflow-safe arithmetic
  before allocation, with categorical, temporal, and over-capacity requests
  refusing at the authored element. Visibility flags and absolute spacing stay
  internal.

- v0.38 (2026-08-06): PRODUCT-0038 exposes `x_tick_step(step)` and
  `y_tick_step(step)` as figure-wide Rune elements selecting the closed
  PRODUCT-0037 Step lattice. Positive finite literals lower independently;
  bounds remain optional, zero anchoring applies to data-derived domains, and
  categorical, temporal, or over-capacity requests refuse at their element
  span. Omission remains Count(8), minor spacing and tick count stay closed,
  and layered plots retain one shared placement.

- v0.37 (2026-08-06): PRODUCT-0037 accepts the closed major tick placement
  substrate without adding Rune grammar. Count(8) remains the compatibility
  default and is byte-identical through a sixteen-site position oracle. The
  future `Step` placement is a zero-anchored lattice with shared major/minor
  positions, declared capacity refusal, and no categorical or temporal
  semantics; PRODUCT-0038 is its pending figure surface.

- v0.36 (2026-08-06): PRODUCT-0036 exposes the existing numeric domain slot as
  figure-wide `x_bounds(min, max)` and `y_bounds(min, max)`. Paired finite
  literals, including direct unary negatives, lower once and reach every
  existing Cartesian renderer. Exclusion and arbitrarily small positive spans
  render as authored; collapse, inversion, malformed values, duplicates, and
  bounds on a resolved discrete axis refuse at the exact constructor. No
  minimum-span threshold, per-mark domain, kernel change, or silent drop exists.

- v0.35 (2026-08-06): PRODUCT-0035 makes authored table-skin contrast an inert
  measurement rather than an admission gate. Every registered theme expression
  now completes through both mechanically defined projections; Yellows Duotone
  renders at 3.175:1, and a degenerate pair renders with paper equal to ink. The
  prior `.duotone()` removal advice, fallback probe, suitability errors, repair,
  and warnings are absent. Polariton-selected defaults remain bound by
  PRODUCT-0023's seven 4.5:1 checks.

- v0.34 (2026-08-06): PRODUCT-0034 makes plot-chrome contrast an observable
  quality fact rather than an artifact admission gate. Every known theme now
  renders its exact Ordered or Duotone projection; `tab20c_s9` completes while
  truthfully reporting its 3.526:1 title contrast. Unknown identities and
  malformed syntax still refuse. No warning, fallback, repair, mark/data
  recoloring, default change, or lifecycle change is introduced.

- v0.33 (2026-08-06): PRODUCT-0033 corrects ordinary plot skin's accidental
  collapse of theme-engine order. Position 19 remains the actual paper and its
  most contrasting mate supplies validated ink, so `tab20c_s5` and `_s8`
  visibly select different canvases. The accepted global strongest-pair
  treatment remains available as explicit `skin(name).duotone()`, where
  set-preserving shifts may intentionally coincide. Palette/data, marks, Guide,
  defaults, and atomic destination lifecycle remain disjoint and unchanged.

- v0.32 (2026-08-06): PRODUCT-0032 adds the direct figure-wide
  `skin("theme_expression")` member and one validated plot-chrome projection.
  Skin colors only the closed eleven-role chrome; categorical `palette` colors
  only data. The paired `viridis_r -> viridis_s5` assay keeps positions fixed,
  recolors bars, and keeps chrome identical, explicitly proving ordered and
  set-based consumers can observe the same modifier differently. Nord passes
  five 4.5:1 text and three 3:1 non-text checks, failures remain
  destination-local and stale-safe, and omission retains the legacy default.

- v0.31 (2026-08-06): PRODUCT-0031 replaces the plot renderer's ambiguous
  background/decoration pair with a closed eleven-role `PlotChrome`. The exact
  library/application rendering oracle stays byte-identical; the legacy 0.2
  white grid over black is classified below the 3:1 non-text threshold without
  being changed or certified. Mark ink, data colors, guide behavior, and host
  chrome remain separate, and Rune syntax is unchanged pending PRODUCT-0032.

- v0.29 (2026-08-05): PRODUCT-0029 replaces baked categorical continuation
  colors with stable validated palette positions while resolved artifacts keep
  exact render colors. Fresh known domains use even spread; live extensions use
  deterministic maximin without moving retained meanings. Explicit `tab20c`,
  `_r`, and `_s1` mappings prove natural-order and modifier delegation, and
  independent geometry/palette limits refuse atomically with complete facts.
  Two-field bars carry exact band styling without manufacturing a Color channel
  or legend; existing production behavior remains byte-compatible behind the
  unselected resolver. Rune palette authoring and the production switch remain
  PRODUCT-0030.
- v0.28 (2026-08-05): PRODUCT-0028 exposes a second authoritative optional
  sequence from libaryon's existing compact theme identity. Continuous families
  retain twenty accepted samples; six curated families expose explicit natural
  categorical declarations, while other curated syntax themes remain valid
  with no categorical capability. Syntax colors and every table-skin outcome
  remain byte-identical. Reverse and shift apply to each projection at its own
  natural length, including `dark2_s20` rotating eight data colors by four while
  leaving twenty syntax positions unchanged. No plot or Rune consumer lands;
  PRODUCT-0029 owns categorical identity, capacity, and band-fill policy.
- v0.27 (2026-08-05): PRODUCT-0027 changes named Rune table skins to explicit
  PaletteForward-by-absence and adds `.duotone()` as the one-token escape hatch
  in the same indivisible implementation commit. The bounded normal form makes
  a treatment without a name and a named skin without a selected projection
  unrepresentable. `yellows`, unknown identity, and `dark2_s1` exercise all
  three diagnostic branches without fallback; projection changes preserve
  panel identity, layout, and atomic frame/skin lifecycle. Bare Rune, SQL, and
  Markdown tables remain compiled-default, and plot skin stays unaccepted.
- v0.26 (2026-08-05): PRODUCT-0026 names the existing table treatment
  `Duotone` without changing any of its 8,480 canonical outcomes and adds an
  explicit PaletteForward substrate beside it. PaletteForward preserves the
  resolved colormap's order across five opaque surfaces and three alpha-bearing
  roles, while neutral maximin ink makes `yellows` legible without weakening a
  contrast gate. A complete two-mode census and synthetic opposite-polarity
  assay prove success and both mechanism-specific refusal paths. Rune remains
  explicitly Duotone in this substrate cut, so the executable `.skin(name)` row
  and every SQL/Markdown default remain unchanged pending PRODUCT-0027.
- v0.25 (2026-08-05): PRODUCT-0025 makes libaryon's existing theme vocabulary
  selectable as validated Rune table skins with
  `table().skin("theme_expression")`. Polariton consumes the exact resolved
  ordered palette, retains reverse/shift observability, projects all twelve
  table-interior roles, and refuses any result whose weakest one of seven
  visible text contrasts falls below 4.5:1. The envelope carries only the
  authored name; completion attaches the resolved skin atomically to the shared
  frame. Unknown and unsuitable identities remain distinct, bare/SQL/Markdown
  tables stay defaulted, and skin changes preserve host identity and layout.
  The exact `terrain_rs10` `test8.rn` shape closes the high-density surface
  regression without making the repository depend on a live Toron service.
- v0.24 (2026-08-05): PRODUCT-0024 makes libaryon's exact resolved twenty-color
  theme palette observable beside the unchanged syntax-color projection. A
  1,420-expression textual oracle preserves every full syntax array while
  category/palette equations prove both projections share one sequence. Generic
  normalized-sRGB luminance and contrast math moves to libaryon without
  quantizing Polariton's fractional table composites. Deterministic indexed
  contrast pairs expose palette capability without artifact policy; reverse,
  shift, continuous clipping, and curated clipping's known no-op retain their
  existing behavior. No Rune row changes status; PRODUCT-0025 remains the first
  table projection and `.skin("name")` caller.
- v0.23 (2026-08-04): PRODUCT-0023 replaces compile-time table color reads with
  one closed, validated `TableSkin` passed through the real widget. The default
  preserves the exact 39-operation pre-extraction draw oracle; every built-in
  passes seven alpha-composited WCAG text checks, and a twelve-sentinel assay
  proves complete renderer consumption without geometry or interaction change.
  SQL-result, Rune-view, and Markdown-cell completions now retain frame plus skin
  as one atomic artifact; stale failure preserves the pair, and skin-only
  replacement cannot escape table content bounds or perturb host layout. No Rune
  row changes status; PRODUCT-0024 exposes the resolved libaryon palette and
  PRODUCT-0025 remains the first `.skin("name")` caller.
- v0.22 (2026-08-04): PRODUCT-0021 makes normalized bar-slot occupancy a typed
  Layer-stage setting and PRODUCT-0022 spells it as the third member of the
  closed postfix register: `bar(category, value).bar_width(fraction)`. The
  domain is `(0, 1]`; the complete category band is divided among series before
  the fraction applies. Default `0.9` leaves 5% padding on each side, `0.5`
  occupies half its full-band subgroup slot, and `1.0` makes neighbors touch
  without a hidden group inset or device-pixel gap. Vertical and horizontal
  paint and hover consume one exact rectangle calculation, so width-created
  padding is not hittable. Bar remains a standalone mark outside `LayerMark`
  and `layers([...])`; preparation, domains, guides, identity, headings,
  placement, and dashboard behavior remain unchanged. The installed `test7.rn`
  combines all three governed mark modifiers in one two-origin fixture.
- v0.21 (2026-08-04): PRODUCT-0019 makes relative line stroke width a typed
  Layer-stage setting and PRODUCT-0020 spells it as the second member of the
  closed postfix register: `line(x, y).stroke_width(factor)`. A factor of `1.0`
  preserves the established 2px stroke and mark-specific scale, while `2.0`
  paints 4px; geometric hits retain five pixels beyond the visible half-stroke
  and the guide sample remains fixed. Wrong receivers, incompatible chains,
  duplicates, computed or invalid factors, and unknown methods refuse
  atomically. The installed `test7.rn` combines a doubled line with its existing
  maximum-sized scatter points in one headed layered figure.
- v0.20 (2026-08-04): PRODUCT-0018 makes relative scatter point size
  scriptable as the closed Layer-stage postfix modifier
  `scatter(x, y).point_size(factor)`. The complete chain flattens atomically
  into PRODUCT-0017's typed `LayerSettings`; unknown and duplicate modifiers,
  computed or invalid factors, line receivers, and modified category scatters
  refuse at the mark. Direct, figure-body, and layered positions share the
  stamped result and the proved layer-aware renderer. `1.0` is byte-identical
  to bare scatter, `2.0` yields a six-pixel radius, and point-size edits preserve
  panel identity, labels, placement, guides, headings, and the no-dashboard
  path. The installed `test7.rn` fixture keeps `8.0` as a visible maximum-bound
  dogfood case.
- v0.19 (2026-08-04): PRODUCT-0017 lands the substrate for relative scatter
  point size without adding Rune syntax. A validated layer-local factor resolves
  through PRODUCT-0007's existing five tiers; vector, downsampled, raster, and
  geometric hit-testing share one radius. Size leaves prepared relation data,
  raster paints clipped discs, and cache identity includes size. Existing 3px
  vector output, one-layer fast paths, guides, execution, panels, placement, and
  dashboard behavior remain unchanged. No capability row changes status;
  PRODUCT-0018 is the first proposed surface caller.
- v0.18 (2026-08-03): PRODUCT-0015 replaces Rune's reverse-emitted split chain
  with one engine-owned producer group. Four views tile visibly as 2x2 in direct
  presentation order; generic Compact tables and Expansive plots shape rows
  without content-derived reflow. Automatic/Manual state preserves authored
  geometry, omission retains orphan tiles, detachment survives reruns, and
  Retile outputs performs the sole exact-program reconciliation. Two producers
  remain independent; tabs stay explicit; result panels, dashboard, Rune
  semantics, and layout persistence are unchanged.
- v0.17 (2026-08-03): PRODUCT-0013 and PRODUCT-0014 are accepted after the
  installed `test6.rn` dogfood run closes the surface's real-window evidence.
  The layered sink row becomes executable. The canvas and anonymous panel chrome
  simultaneously show `Line + Scatter` and `Line + Scatter 2`; scatter marks
  visibly paint above the line; all four results remain ordinary host panels
  without a dashboard. `test5.rn` retains the bare-line fixture.
- v0.16 (2026-08-03): PRODUCT-0014's four implementation stages close thirteen
  gates while the record remains proposed. Contextual grammar lowers only line
  and scatter layers, compatibility stays in PRODUCT-0013, host plot state no
  longer needs `PlotComposition`, and identity/lifecycle plus byte-identical
  one-layer SVG are executable.
- v0.15 (2026-08-03): PRODUCT-0013 lands an additive ordered shared-Cartesian
  substrate and adds the proposed layered-sink row. Shared source domains,
  faithful SVG paint order, absent color, atomic compatibility failures, and
  exact one-layer line/scatter rendering are executable without a Rune caller.
- v0.14 (2026-08-03): PRODUCT-0012 adds the strict x/y line view through all
  three accepted spellings. Exact authored channels now reach the existing line
  kernel contract; shared-result evidence includes line; and the two-origin
  dogfood fixture replaces its scatter with a line while retaining four ordinary
  host panels and no dashboard mutation. Series/group styling remains outside
  the accepted surface.
- v0.13 (2026-08-03): PRODUCT-0011 adds advisory `Split` or `TabWith` placement
  while libaryon retains layout authority. Its original automatic grouping
  policy was reversed after dogfood: Polariton now requests one split per view,
  because neither a run nor a relation origin says that its views are
  alternatives. Four views remain simultaneously visible in four output panes;
  `TabWith` remains available for author-arranged tabs and a future explicit
  grouping signal. Existing panels never relocate on rerun, missing tab anchors
  fall back without a frontend error, descriptor titles make manually grouped
  host tabs legible, and closing one tab disposes only its panel (`8535d662`).
  Query results and the dashboard retain their prior split behavior. Layout
  persistence and durable document identity remain explicitly undecided.
- v0.12 (2026-08-03): PRODUCT-0010 makes multi-origin view programs and optional
  view identity executable. Canonically equal origins retain one dispatch and
  shared `Arc`; distinct origins dispatch and fail independently; global
  presentation order survives reverse completion. `view("name", sink)` provides
  a session-stable address, anonymous forms are freely repeatable positional
  views, and `DuplicateForm` is retired. Tables and plots now populate ordinary
  host panels with stale/orphan lifecycle markers, including the two-table
  `test5.rn` fixture, without opening or mutating the dashboard (`284e88e0`,
  `b99338f8`, `cad42680`). Durable identity across restarts remains pending on a
  future libaryon document-identity and layout-persistence record.
- v0.11 (2026-08-03): PRODUCT-0008 accepts and implements `.views([...])` plus
  its three strict single-view sugars. The four executable view rows gain the new
  spelling and PRODUCT-0008 evidence without replacing any existing citation.
  `EnvelopeSemantics` proves direct, factored, sugared, and legacy equivalence;
  ordering is list-then-chain within a set and emission position across roots;
  contextual constructors refuse atomically; and the live gate preserves axis
  identity against the installed deployment (`d4c2d8a8`). The implementation
  halt fired once on PRODUCT-0007's stale `.views(` prohibition and retired only
  that superseded premise, preserving the `EncodedViewSet` and `SinkSpec` guards.
- v0.10 (2026-08-03): added the respelling stop condition — the normative limit
  on what an accepting record may implement, and the halt rule if it needs more.
  Recorded because it is falsifiable mid-work rather than only at the end, and
  because it fixes the accepting record's evidence to three items with every
  existing `executable` citation left untouched.
- v0.9 (2026-08-03): four rows become **executable** — the three view sinks and
  shared-execution fan-out. The live relation gate now runs green against an
  installed deployment and asserts axis identity (`kernel.x_label()` /
  `y_label()`) rather than path presence, and PRODUCT-0007 closed the
  field-substitution defect that made scatter's rendered channels untrustworthy.
  Fan-out is proved from both sides: one `dispatch_rune` produces the live
  result, three ordered sinks consume it, and the automated fixture asserts every
  sink holds the same `Arc` rather than equivalent copies — direct evidence for
  one execution feeding N sinks, independent of any spelling. The `experimental`
  qualifier is dropped from the bar and scatter rows; their spelling is
  unchanged. These status changes are deliberately separate from any decision
  about the proposed surface, so the evidence trail for the current spelling
  stays intact whatever that record decides.
- v0.8 (2026-08-02): banked the `ViewSet` normal form — relation origin plus a
  flat ordered sink list, which every surface operation only appends to, so
  execution order holds by construction and step 1's `EnvelopeSemantics` already
  compares the right shape. Recorded that sink constructors are contextual,
  recognized only in `.views([...])` argument position, confining name
  reservation to a proven position rather than a namespace. Added bindable sink
  specifications as an explicit non-decision. No row changed status.
- v0.7 (2026-08-02): `.views([...])` becomes the boundary and the canonical
  form; `EncodedViewSet` is renamed `ViewSet`; the three single-view sugars lower
  to `.views([...])`. `.encode(...)` is withdrawn from the proposal to an
  explicit non-decision — it asserted that every sink consumes an encoding, which
  is false for `table()`, and promoted a shared-channel convenience into the
  process boundary. Encoding channels belong inside individual plot
  specifications; shared encoding may return as presentation sugar over the sink
  list. Supersedes the v0.5 entry below where that entry names `.encode(...)` as
  the boundary. No row changed status.
- v0.6 (2026-08-02): recorded receiver-provenance recognition beside the
  invariant — the `RelationCandidate` kind, its propagation table, and the two
  properties that make it implementable without a type model (unknown methods
  and sink methods both kind-preserving). Follows step 1 landing
  (`impl: typed output root discovery`), which proved the binding analysis this
  rule extends. No row changed status.
- v0.5 (2026-08-02): added the proposed encoded-view-fan-out surface and the
  `proposed` status. Expansion justified by dogfood evidence: the working
  three-view snippet made the fan-out point visible as a binding, and the
  terseness exercise found that `.encode(...)` is simultaneously the type-state,
  fan-out, and process boundary. Records two prerequisites — typed output-root
  discovery, and lowering data-changing operations before the boundary. No
  existing row changed status.
- v0.4 (2026-08-01): added the shared-execution view fan-out row (pending,
  fan-out pair target), expansion justified by measurement #2 dogfood
  evidence: one relation must feed a table and plot in one execution.
- v0.3 (2026-08-01): the six native relation families became executable over
  the public authenticated Rune route. The three view rows retain pending
  status until the ignored live-profile fixture and user dogfood acceptance
  exercise the installed cross-product path.
- v0.2 (2026-08-01): "streamed table view" corrected to "bounded table view"
  with the inherited 128 MiB ceiling cited, per PRODUCT-0003; rows and
  statuses otherwise unchanged.
- v0.1 (2026-08-01): created with the first-contact plan; all rows pending.
