# ADR 0013: ordered shared-Cartesian plot layers

Status: accepted 2026-08-03. This record decides one thing: that a **layered**
plot specification carries an ordered, non-empty list of layers, each owning its
own encoding, and that its marks paint in authored order. It is additive —
existing single-plot specifications remain valid and unchanged. It is the
substrate half of layered plotting and adds **no Rune surface**; the
`layers([...])` spelling is PRODUCT-0014, which depends on this record. It
inherits PRODUCT-0007's grammar, channel resolution, and cascade algebras
unchanged.

## Context

A surface record wants to spell this:

```rune
ket.table("test1").sort([col("x").asc()]).views([
    layers([line("x", "y"), scatter("x", "y")]),
])
```

PRODUCT-0007 §5 already accepted the stage that describes it — the plot side is
`Facet -> Layer -> Encoding -> Scale -> Guide` — and §7 already places `layer` in
the precedence cascade. Layer is decided vocabulary, not a new concept. What does
not exist is a representation faithful to it.

Today the substrate is shaped the other way around:

- `AuthoredPlotSpec` owns **one global** `x`, `y`, `color`, and `color_job`
  (`spec/mod.rs:124`), while `PlotComposition` carries only mark types
  (`PlotLayer { plot_type }`). Encoding is global; marks are a list. The grammar
  says each layer owns an encoding.
- `PlotComposition::add_layer` **rewrites** (Line, Scatter) into
  `[Scatter, Line]`, discarding authored order, and `can_add_layer_type` is a
  hardcoded permission table over a few pairs.
- `ScatterLinePlotKernel` is a fixed pair rather than a list, and it draws
  scatter and then line (`kernels/scatter_line.rs:70-72`), so the line paints
  **over** the points.
- `PlotKernel` owns `layout`, `plot`, `draw_legend`, and `hover` together
  (`common.rs:490`). Composing complete kernels would therefore compose layout,
  legend, and interaction — precisely the stages this record defers.

So an ordered kernel list would not be enough. Turning the fixed pair into a
`Vec` would leave encoding global, leave authored order normalized away, and
compose four stages while claiming to compose one.

## Decision

### 1. A layered specification is layer-shaped

```text
LayeredPlotSpec { layers: NonEmpty<LayerSpec> }
LayerSpec       { mark, encoding }
```

This is a **new specification beside `AuthoredPlotSpec`, not a migration of it**.
Existing single-plot specifications remain valid, and every existing consumer
keeps its current type. A one-layer `LayeredPlotSpec` lowers through the existing
path per §7. Nothing here mandates converting `AuthoredPlotSpec`, and §10
prohibits it.

`NonEmpty` is structural: a plot with no layers is not a degenerate plot, it is
not a plot, and making it unrepresentable removes a class of resolution failure
rather than reporting it.

The list is ordered and its order is authored. No normalization, reordering, or
canonical-primary rewriting may occur between authoring and painting. The
`add_layer` normalization and the `can_add_layer_type` permission table do not
survive into this path.

### 2. Encoding moves into the layer, and differing encodings are representable

Each `LayerSpec` carries its own encoding, resolved independently through
PRODUCT-0007 §2's total, non-substituting channel resolution. A layer's channels
resolve exactly or that layer fails; nothing is inferred, copied between layers,
or borrowed from a sibling.

Differing encodings across layers are therefore **representable in the
specification and refused by an accepted compatibility rule**:

> Every layer must resolve identical x and y field identities, and their scales
> must belong to the same family. Every layer's color channel must be `Absent`.

The color clause is a refusal, not an omission. A `LayerSpec` encoding can carry
a color channel because it is an ordinary PRODUCT-0007 encoding, so the
implementation would otherwise have to guess. `Named` and `Auto` both refuse at
the offending layer.

Even identical color fields across layers would be more than this record intends:
they would require a shared color identity, a `color_job` compatibility rule, and
a legend that says something coherent about a scale spanning two marks. Each is a
color-scale decision, and all three are deferred together rather than acquired
one at a time by accident.

The distinction is the point of this section. Differing fields are not erased
into one global encoding, which would make the grammar a lie, and they are not
quietly permitted, which would promise a scale-composition semantics no record
has decided. They are expressible and rejected, so relaxing the rule later is a
new decision over an existing representation rather than a change of shape.

Relaxation belongs to PRODUCT-0007's Scale stage: the moment two layers name
different fields, someone must decide shared domain versus dual axis, and that is
a scale-composition record.

### 3. The rendering boundary is a mark renderer, not a kernel composition

```text
shared resolved layout -> ordered mark renderers -> paint in source order
```

A **mark renderer** draws its layer's marks into a shared coordinate transform
and bounds, given its resolved data and the resolved settings. It owns no layout,
no legend, no axis labels, and no interaction policy. Those remain the
composite's, computed once from the shared domain.

A mark renderer **may expose geometric hit-testing** — which of its marks, if
any, lies under a point. That is geometry, which only the renderer knows. It does
not decide what a hit means, whether a hit is preferred over a sibling's, or how
any content is worded for a reader; those are interaction policy and belong to a
later record.

This is what keeps the record's scope honest. `PlotKernel` is a complete artifact
that owns four stages; a composite built by nesting complete kernels would
inherit four stages' worth of behavior and would be deciding guides and
interaction by construction while claiming to defer them.

The layered composite implements `PlotKernel` so every existing consumer —
`PlotWidget`, the SVG backend, export, panel content — is untouched.

#### The bridge, and why it is non-normative

`PlotKernel` requires total `hover` and `draw_legend`, so a composite that
implements the trait cannot decline to answer. This subsection authorizes exactly
enough behavior to satisfy that existing contract and nothing more:

- **hover** searches layers back-to-front and returns the first geometric hit,
  unchanged in wording from whatever that layer already reports;
- **the legend** delegates to the **last authored layer** — the topmost painted
  one — without merging, deduplicating, or rewriting its output.

"Last authored" is chosen because it is total over any non-empty list and needs
no knowledge of marks. Naming a preferred mark would encode a guide preference in
the substrate, which is the thing being deferred. For the motivating
`layers([line, scatter])` it delegates to scatter, reproducing what
`ScatterLinePlotKernel` already chooses, but as a consequence of a general rule
rather than a special case.

> The bridge behavior in this subsection is authorized **only** to satisfy the
> existing `PlotKernel` contract. It carries no compatibility guarantee, is not an
> acceptance gate, and is not a decision this record makes about Guide or
> interaction. Any behavior beyond the bridge — merging, deduplicating, or
> reconciling legend entries; ranking hits by anything other than paint order;
> composing or rewording hover content — is a guide or interaction decision and
> triggers §9's halt.

The implementation may cover the bridge with ordinary tests, and should mark them
as bridge coverage. Such tests pin current behavior against accidental drift; they
do not make it accepted, and a later Guide or interaction record replaces them
without amending this one. That distinction is why the bridge appears in no
acceptance gate: a gate is a promise, and this is a placeholder.

The legend deliberately delegates rather than emitting "one entry per distinct
resolved series." That phrasing sounds neutral but presumes an identity for
"distinct resolved series" that no record has defined, and choosing one would be
guide-merging policy smuggled in as an implementation detail. Delegation is
visibly a placeholder, which is the property wanted here.

### 4. Paint order is authored order

Layers paint first to last. The first authored layer is the bottom.

For `layers([line, scatter])` the points therefore paint above the line, which is
both the useful arrangement and the **reverse** of what ships today. This section
is not cosmetic: it is the one guarantee the record exists to make, and the
existing pair kernel contradicts it.

### 5. The accepted capability is narrow

- one relation and one stamped result;
- Cartesian `line` and `scatter` marks only;
- each layer resolves independently;
- identical x/y field identities and compatible scale families across layers;
- an `Absent` color channel on every layer;
- one shared Cartesian layout and domain;
- layers paint in authored order;
- resolution is atomic — if any layer fails, the plot fails as one artifact,
  because a chart missing a layer is a wrong chart rather than a partial one.

Unaccepted, and explicitly not promised by anything here: differing fields, dual
axes, non-Cartesian marks, facets, guide merging, interaction policy, and layered
color scales.

### 6. Content kind and display label are different things

Routing asks what surface presents an artifact; description asks what to call it.
A layered plot is a **plot** for routing, and its label derives from its ordered
composition.

```text
content kind    Table | Plot
display label   table | line | scatter | line + scatter | ...
```

PRODUCT-0010 §4 already declared mark form non-identifying. This record requires
only that the substrate expose a faithful ordered label and that no routing
decision consult mark identity. The `RuneViewForm` refactor that follows from it
belongs to the surface record, where that type lives.

### 7. Single-layer specifications keep today's path

A one-layer specification lowers to the kernel it lowers to today. Radar, violin,
candlestick, bubble, stacked area, fill-between, and every other kernel are
untouched, and no existing `AuthoredPlotSpec` consumer changes behavior.

Only `line` and `scatter` gain mark renderers. The layered path is additive, and
PRODUCT-0007's landed evidence stays valid because nothing it cites is rerouted.

### 8. Acceptance gates

1. A two-layer `[line, scatter]` specification preserves authored order through
   resolution into painting, with no normalization step observable anywhere.
2. **Painting order is observed, not inspected.** Rendering through the SVG
   backend, the scatter marks appear *after* the line path in document order.
   A test that asserts the contents of the layer vector does not discharge this
   gate: the vector is the thing that was already almost right, and the defect
   being fixed lives between the vector and the canvas.
3. Layers with differing x or y field identities refuse as one artifact, naming
   the offending layer and both field identities.
4. A layer carrying a `Named` color channel refuses, and a layer carrying `Auto`
   refuses, each naming the offending layer. This is what gives "layered color
   scales are unaccepted" executable meaning; without it the phrase is a comment.
   A control asserts the same specification with `Absent` color resolves.
5. A layer whose channel fails to resolve fails the whole plot; no partial chart
   is produced.
6. A one-layer `line` specification and a one-layer `scatter` specification each
   lower to their existing kernel and render byte-identically to today. Only the
   two affected paths are pinned; no other kernel is re-evidenced.
7. The composite reports one shared layout and one shared domain, resolved from
   the layers' common x and y source columns **before** any renderer-specific
   sampling or preparation. Unioning post-preparation ranges does not discharge
   this gate: identical bindings make a common domain derivable directly, and
   reconciling prepared ranges is scale composition, which §10 does not authorize.
8. An empty layer list is unrepresentable, proved by construction rather than by
   a runtime refusal test.

### 9. The stop condition

> This record may add a layered specification, per-layer encoding resolution,
> mark renderers for line and scatter, geometric hit-testing on those renderers,
> the §3 bridge, and an ordered painting composite. If the implementation
> requires a Rune surface change, a scale-composition rule, a guide or
> interaction decision beyond the §3 bridge, a change to any existing kernel's
> rendering, or a change to `AuthoredPlotSpec` or its consumers, the work stops
> and the requirement returns as its own record.

The §3 bridge is the single, bounded exception to the guide-and-interaction
clause, and it is bounded by that subsection's own terms rather than by judgment.

### 10. What this record does not authorize

No Rune syntax of any kind, including `layers([...])`. No change to Toron, the
transport, the envelope, panels, or the dashboard. No new mark. No scale
composition, guide merging, facet, or interaction policy beyond the §3 bridge.
No retirement of `ScatterLinePlotKernel` or any other existing kernel.

**No migration of `AuthoredPlotSpec`.** It keeps its shape, its global `x`, `y`,
`color`, and `color_job`, and every one of its consumers. `LayeredPlotSpec` is a
second specification beside it. A record that later unifies the two is welcome to
exist; it is not this one, and an implementation that begins the unification has
triggered §9.

## Rejected

- **An ordered `Vec` of kernels:** leaves encoding global, so the specification
  still cannot say what the grammar says, and composes layout, legend, and hover
  while claiming to compose marks.
- **Erasing per-layer encodings into one global encoding:** the shortest path to
  the same picture, and it makes the representation contradict PRODUCT-0007 §5's
  grammar. The compatibility rule in §2 gets the same initial capability without
  lying about the shape.
- **Permitting differing fields immediately:** promises a scale-composition
  semantics no record has decided, discovered later as an accidental commitment.
- **Keeping `can_add_layer_type` as the layering rule:** a permission table over
  hardcoded pairs, which would become the language's grammar the moment a surface
  record lowered onto it.
- **Deferring until Guide and interaction are decided:** would block the whole
  arc on stages the motivating case does not need. §3 names its provisional
  answers instead of pretending it has none.
- **Converting every kernel to mark renderers:** a large refactor of working,
  cited code for the benefit of two marks. §7 keeps the change additive.
- **Spelling this stage `overlay`:** PRODUCT-0007 §6 already uses "overlay" for
  the right-biased cascade operation on `Override<T>` and `Presence<T>`. One
  product, one meaning per word.

## Consequences and sequencing

- **The interaction deferral closes (ledger IG-02, 2026-08-19).** PRODUCT-0133
  decides hit ranking and sibling preference (§1) and hover wording (§2), and
  retires §3's hover bridge. The bridge's "no compatibility guarantee" held:
  ranking is now dimensional before paint order, and prose became a
  presentation of a structured hit rather than a kernel's private sentence.
  Closed as decision-complete, not as implemented — PRODUCT-0133 stages 3–4
  remain unbuilt. Ratified in
  `plans/product/review/deferred_triage.md` § Ratification — 2026-08-19.

- The arc is three records, in order: this one; then PRODUCT-0014, the Rune
  surface (`layers([...])`, positional constructor roles, depth-one nesting, one
  sink and one panel and one identity, cross-origin refusal); then scale
  composition, guides, interaction, facets, and any `.points()` sugar.
- PRODUCT-0014 is blocked on this record and should not be drafted as though it
  were independent. Its proof burden is lowering; this record's proof burden is
  painting.
- Two plot specifications coexist until some later record earns the right to
  unify them. That is a real cost — a second shape a reader must learn — and it is
  smaller than migrating every `AuthoredPlotSpec` consumer to serve two marks.
- `ScatterLinePlotKernel` acquires a successor without being retired. Whether it
  is eventually deleted depends on evidence that nothing reaches it, which is a
  later cleanup rather than a decision here.
- PRODUCT-0007's channel-resolution guarantees are load-bearing here and gain a
  second consumer. Nothing in this record weakens them: per-layer resolution is
  the same total, non-substituting resolution applied more than once.
- The provisional legend and hover behavior in §3 is the first thing a Guide or
  interaction record should replace, and it is marked in the source as
  provisional so that record can find it.
- A faithful ordered display label becomes available, which the surface record
  needs in order to stop `RuneViewForm` serving both routing and description.
