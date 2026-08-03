# Rune workspace acceptance corpus

Version 0.6. Charter: PRODUCT-0002 §9. This is a living, versioned acceptance
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
| bounded table view (`table(r)`, inherits the 128 MiB `toron-client` collection ceiling) | pending — live first-contact acceptance | automated seams: `terminal_table_is_substituted_and_mapped` + `rune_relation_results_feed_the_existing_table_and_plot_surfaces` |
| experimental bar sink (`plot.bar`) | pending — live first-contact acceptance | automated seams: `plot_forms_retain_only_view_intent` + `rune_relation_results_feed_the_existing_table_and_plot_surfaces` |
| experimental scatter sink (`plot.scatter`) | pending — live first-contact acceptance | automated seams: `plot_forms_retain_only_view_intent` + `rune_relation_results_feed_the_existing_table_and_plot_surfaces` |
| shared-execution view fan-out (one execution, N sinks) | pending — fan-out pair target | expansion justified by measurement #2 dogfood evidence, 2026-08-01 |
| durable live plot | pending | — |
| durable pinned plot | pending | — |

## Proposed surface: encoded view fan-out

Status: **proposed**. This section records a candidate spelling and the invariant
that justifies it. No record accepts it, no implementation depends on it, and the
executable and pending rows above continue to describe the surface that exists.
If a later record accepts this surface, the view rows are respelled with that
citation rather than silently migrated.

### Load-bearing invariant

> `RuneDataFrame` → `EncodedViewSet` is simultaneously the type-state boundary,
> the fan-out boundary, and the Toron–Polariton process boundary.

The three coincide, which is the argument for the surface rather than a
consequence of it:

- **Type-state.** `RuneDataFrame` has no sink methods; `EncodedViewSet` has no
  relation methods. A chain that appears to make one view consume another is
  unrepresentable rather than merely discouraged.
- **Fan-out.** Sinks appended to an `EncodedViewSet` are siblings over one
  relation. This is the same algebra as the shared-execution fan-out row above:
  one execution, N sinks, one stamped result shared by identity.
- **Process.** Everything at or below `RuneDataFrame` executes on Toron;
  everything above `.encode(...)` stays in Polariton. PRODUCT-0002's
  execution/presentation split stops being a rule a reviewer enforces and becomes
  one the receiver type enforces.

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
  unknown method                → RelationCandidate
  .encode(...) / .views(...)    → EncodedViewSet
  direct .scatter(x, y)         → sugar for .encode(x, y).scatter()
  direct .bar(x, y)             → sugar for .encode(x, y).bar()
  direct .table()               → EncodedViewSet with a relation-table sink

EncodedViewSet
  .scatter() / .bar() / .table() → EncodedViewSet plus one ordered sink
```

Bindings preserve the inferred kind and the accumulated specification. Binding
alone emits nothing. Only a **top-level expression of `EncodedViewSet` kind**
becomes an output root.

Two properties of this table are load-bearing:

- **Unknown methods are kind-preserving.** The analysis must never enumerate
  which methods return relations, because `project`, `filter`, `aggregate`,
  `sort`, `limit`, `join`, and `window` are precisely the names the PRODUCT-0003
  containment assay forbids it from knowing
  (`../impl/0003_typed_output_root_discovery.md`). Stating propagation
  negatively keeps the analysis ignorant by construction rather than by
  discipline: `workspace.table("t").sort([...])` stays a `RelationCandidate`
  without the analysis holding any opinion about `sort`.
- **Sink methods are kind-preserving too.** `.scatter()` returns an
  `EncodedViewSet` with one sink appended, or `v.scatter().bar()` breaks after
  the first sink. This is also what makes sink **order** fall out of chain order
  rather than needing a separate rule, and what lets the direct sugar and the
  chained form share exactly one algebra.

Receiver provenance is a narrower recognition rule than the free-function name
matching it replaces. `table(r)` and `plot.bar(...)` are global names a user may
legitimately own, which is why step 1 needs whole-buffer shadow analysis; a sink
method is recognized only on a receiver the analysis already traced to
`workspace`, so a user-defined `.scatter()` on an unrelated value is never a
root. The shadowing machinery is a compatibility concern for the current
spelling, not a permanent cost of the proposed one.

### Form hierarchy

| form | role |
|---|---|
| `r.views([...])` | canonical algebra: a relation and an explicit list of sinks |
| `r.encode(x, y).scatter().bar()` | shared-encoding sugar; sinks append as siblings |
| `r.scatter(x, y)` | single-view sugar, defined as exactly `r.encode(x, y).scatter()` |
| `r.table()` | displays the underlying relation; ignores any encoding |

Rules the hierarchy must satisfy:

- **Sugar is a strict projection.** Each sugared form is definable in terms of
  the canonical form and may carry no semantics the canonical form cannot
  express. A sugared form that grows a private option is a second language.
- **Binding is lazy.** Constructing an `EncodedViewSet` executes nothing and
  emits nothing. Only a top-level displayable expression emits output; a
  bound-but-unused set renders nothing.
- **`table` is not a mark.** Marks consume an encoding; a grid consumes a
  relation. `r.table()` shows the full relation on either receiver, and the fact
  that it ignores an encoding in scope is stated rather than inferred.

Receiver typing also retires the earlier namespace question. `workspace.table(name)`
selects a source and `EncodedViewSet.table()` appends a grid sink; the receiver
disambiguates, so neither a `plot.` nor a `view.` prefix is required to keep them
apart.

### Equivalence acceptance cases

The direct and factored forms are one acceptance obligation, not two:

```rune
r.encode("cat", "val").scatter().bar();
```

```rune
let v = r.encode("cat", "val");
v.scatter().bar();
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
   before an `EncodedViewSet` is constructed. After `.encode(...)`, an operation
   may describe presentation but must never execute a data transformation
   locally. An operation that cannot be expressed as a relation operation before
   the boundary is a typed capability refusal, not a local fallback — otherwise
   the invariant degrades quietly into a second, undeclared execution engine
   inside the workbench.

### What this section does not decide

`PlotSpec` fields, scales, guides, themes, interaction, named views, durable plot
binding, and the exact spelling of `encode` remain open. The plotting record
decides them; this section constrains where they may live.

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
