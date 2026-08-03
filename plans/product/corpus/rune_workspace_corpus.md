# Rune workspace acceptance corpus

Version 0.10. Charter: PRODUCT-0002 §9. This is a living, versioned acceptance
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
| bounded table view (`table(r)`, inherits the 128 MiB `toron-client` collection ceiling) | executable | `terminal_table_is_substituted_and_mapped` + `rune_relation_results_feed_the_existing_table_and_plot_surfaces`; live acceptance `live_rune_relation_reaches_table_bar_and_scatter_surfaces` |
| bar sink (`plot.bar`) | executable | `plot_forms_retain_only_view_intent` + `rune_relation_results_feed_the_existing_table_and_plot_surfaces`; live acceptance with axis-identity assertions (PRODUCT-0007 stage 2, `755e850a`) |
| scatter sink (`plot.scatter`) | executable | as bar, plus the categorical-x resolution that closed the field-substitution defect (PRODUCT-0007 §2, `755e850a`) |
| shared-execution view fan-out (one execution, N sinks) | executable | `rune_relation_results_feed_the_existing_table_and_plot_surfaces` proves every sink holds the *same* `Arc`, not equivalent copies; live acceptance issues one `dispatch_rune` and consumes three ordered sinks (`71445df3`, `755e850a`) |
| durable live plot | pending | — |
| durable pinned plot | pending | — |

## Proposed surface: view fan-out

Status: **proposed**. This section records a candidate spelling and the invariant
that justifies it. No record accepts it, no implementation depends on it, and the
executable and pending rows above continue to describe the surface that exists.
If a later record accepts this surface, the view rows are respelled with that
citation rather than silently migrated.

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

This is already the shape step 1 compares. `EnvelopeSemantics`
(`../impl/0003_typed_output_root_discovery.md`) is a resolved relation origin
plus ordered, unspanned sink intents — the same normal form. The existing
equivalence and ordering proofs therefore judge this surface without
modification, which is what makes step 3 reviewable as lowering alone.

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
