# ADR 0007: the plot specification contract

Status: accepted 2026-08-02. PRODUCT-0002 §8 blocked this record behind the
relation contract and required it to reference relation identity as definition,
residence, provenance, binding, and stamped result. That record is accepted, so
this one specifies what a plot *is*. It decides specification, resolution,
cascade, and durability. It does not decide the Rune surface spelling, which the
corpus carries as a proposal.

## Context

Polariton already renders. Twenty kernels exist (`plot/core.rs:2`), three
backends share one widget (`plot/common.rs:347`), and the fan-out path delivers
one stamped result to several sinks. What is missing is not rendering but
**identity**.

`PlotRequest` carries an explicit `projection: Option<(String, String)>`
(`plot/request.rs:25`). `ResolvedPlot` does not (`:35`). The requested columns
are applied as a `select` and then discarded, so channel identity survives only
as *column order* in a data frame. Downstream, `factory.rs:102-108` reconstructs
channels by partitioning columns by dtype: the first string column becomes the
series, the first numeric column becomes x, and y falls back to x when no second
numeric column exists.

For `plot.scatter(r, "cat", "val")` that yields x = `val`, y = `val`, series =
`cat`. The user named two fields and the renderer used one of them twice. The
same `unwrap_or(previous)` inference repeats across Line, Bubble, Candlestick,
FillBetween, and RadialDial. No test covers any of it.

This is not a rendering defect to patch. It is the predictable consequence of a
pipeline in which an authored intent has no representation that survives to the
kernel. A specification that cannot carry "the user asked for `cat` on x" cannot
be prevented from guessing.

The remaining pressure is compositional. Scales, guides, themes, facets, and
interactive overrides all need somewhere to live, and the option-bag path — a
verb that accumulates keyword arguments forever — is how comparable systems
reached their expressiveness ceiling.

## Decision

### 1. A plot is three artifacts, not one

| artifact | holds | lifetime |
|---|---|---|
| **`AuthoredPlotSpec`** | portable intent: requested field, declared or automatic semantic type, scale policy, styling | authored by a user; saved as a product-workspace artifact |
| **`ResolvedPlotSpec`** | exact field identity, schema-derived semantic type, chosen scale family, domain decisions, provenance | derived from an authored spec plus a stamped result |
| **`ResolvedLayout`** | viewport-dependent pixel ranges, tick positions, geometry | derived from a resolved spec plus a viewport |

Each boundary exists because something different changes across it. An authored
spec is portable across data; a resolved spec is portable across viewports; a
layout is portable across neither. Collapsing any pair is how the current
pipeline lost channel identity.

Resolution has two phases, because its inputs arrive at different times.
**Channel resolution** binds each channel to a field, a semantic type, and a
scale family, and needs only the returned schema. **Domain resolution** decides
scale domains and categorical identity, and needs the returned values. Only the
completed artifact is a `ResolvedPlotSpec`; the intermediate is a channel
binding, not a specification, and is never named as one.

Both phases run after execution. Kernels consume a `ResolvedPlotSpec` and never a
data frame plus inference.

### 2. Channel resolution is total, and never substitutes

A channel either resolves its requested field with a compatible scale, or it
fails with an attributed error. There is no third outcome.

Every channel is authored in one of three states, and omission is not among them:

```text
AuthoredChannel         = Named(field) | Auto
OptionalAuthoredChannel = Named(field) | Auto | Absent
```

- **`Named`** must resolve exactly: its field is used, or the channel fails.
- **`Auto`** explicitly authorizes inference for that channel.
- **`Absent`**, available only where a geometry makes a channel optional, stays
  absent.

Authority is therefore always written down. A fully automatic plot lowers to a
specification whose channels are explicitly `Auto`, so no channel acquires
inference authority merely by having been left out of a specification that named
others. Omission cannot grant permission, because omission is not representable.

Resolution may not:

- resolve a `Named` channel to any field other than the one named;
- originate a repetition by copying a resolved field into a channel the author
  did not name it for;
- infer a `Named` or `Absent` channel; or
- inspect values to decide what a field means.

Deliberate repetition is legitimate and must work. `scatter("val", "val")` plots
a field against itself and resolves to both channels. The defect this record
addresses is a resolver *originating* a repetition the author did not write.

A missing field, an incompatible semantic type, or an unsupported scale for a
geometry is a typed refusal naming the channel, the requested field, and the
reason. Failure is per-sink and carries the sink's Rune span, so one bad channel
fails one view while its siblings render — the isolation the fan-out path already
provides.

Every inferred outcome is written into the resolved specification explicitly, so
a resolved spec never records that something was left to chance.

### 3. Semantic type and scale are distinct

**Semantic type** is what a field means: nominal, ordinal, quantitative,
temporal. **Scale** is how it maps to a visual range: point, band, linear, log,
time, and others per geometry.

Automatic resolution may infer both from the returned schema, but the resolved
specification must name each decision explicitly before reaching a kernel. This
is what makes the substitution above structurally impossible rather than merely
corrected, and it gives explicit overrides — declaring an integer field ordinal,
or a quantitative field log-scaled — an honest place to live.

The distinction is not redundant: one semantic type admits several scales, and
the choice is a presentation decision the author may make or delegate.

### 4. Durability pins the resolved spec, not the layout

PRODUCT-0002 §8 defines live and pinned durable plots. Against the three
artifacts:

- a **live** durable plot re-runs both resolution phases after every execution,
  so it follows the data, the schema, and the current revision of the relation it
  names;
- a **pinned** durable plot retains its `ResolvedPlotSpec` and recomputes only
  `ResolvedLayout` when the viewport changes.

Under PRODUCT-0002 a pinned reference names an exact revision, so a later current
revision is not a threat to it — it is simply irrelevant. A live plot is where a
column changing from integer to string legitimately re-resolves a linear scale to
a point scale.

What pinning the resolved spec protects against is therefore not data drift but
**resolver drift**. Resolution is not a function of schema alone: it also depends
on the product's inference defaults, its admissible-scale tables, and — for
categorical identity — the order in which values were first seen. A future
release that improves an inference default would silently re-resolve a saved
plot even at an unchanged revision. Retaining the resolved spec makes a pinned
plot reproducible across product upgrades, which pinning the authored spec cannot
achieve and pinning the layout would buy by freezing the plot to one window size.

A pinned plot fails visible only when its **pinned revision** becomes unavailable
or unverifiable, per PRODUCT-0002 §7: render the last stamped result with an
explicit stale or unverifiable banner rather than silently re-resolving against
whatever is current.

### 5. The specification is a grammar, not an option bag

The full grammar-of-graphics pipeline spans two owners, and the split is where
the process boundary already is:

```text
relation side    Data -> Transform
plot side        Facet -> Layer -> Encoding -> Scale -> Guide
```

`AuthoredPlotSpec` contains the plot-side stages only. Data and Transform belong
to the relation: binning, aggregation, and calculated fields lower into relation
operations before a specification exists, so a specification describes
presentation and executes no data transformation locally. Writing the pipeline as
one undivided list would state the opposite of that rule in the same breath as
the rule.

Each plot-side stage is a named position with its own vocabulary. New capability
extends a stage; it does not extend a verb's argument list. This is the
structural alternative to the ceiling reached by systems whose visualization
surface is a terminal verb with accumulating options.

### 6. Two cascade algebras, and only two

Composition uses exactly two parameterized types:

```text
Override<T> = Inherit | Set(T)
Presence<T> = Inherit | Show(T) | Hide
```

Overlay is right-biased, and `Inherit` is the identity element, so cascading is
associative with a unit — a monoid, not a pile of special cases.

A single universal `Setting<T>` is rejected. It would admit `Hide` for values
where absence is meaningless, such as a font size, making an unrepresentable
state representable. Properties that can be turned off use `Presence`; properties
that only have values use `Override`.

### 7. Precedence has five layers, in one order

```text
library defaults -> application theme -> authored spec -> layer -> pane override
```

Later layers win. The pane override is an interactive, session-scoped
adjustment; whether and how it round-trips into the authored spec is deliberately
left to the implementation record, because it is an editing-model question rather
than a specification one.

### 8. Color is typed by job, and identity is fixed by the resolved domain

A color specification names its **job** — categorical, sequential, diverging, or
status — not a ramp. A ramp is selected to satisfy the job, and a job mismatch,
such as a sequential ramp for unordered categories, is a specification error
rather than an aesthetic preference.

Series identity binds to a **color identity**, not to render order and not to the
domain of the latest result. Filtering, sorting, or reordering a relation
therefore never recolors a series.

That requires two distinct domains, because a live plot re-resolves after every
execution and rebuilding identity from each result would recolor the survivors of
a filter — the precise outcome this rule forbids:

- the **display domain** comes from the current stamped result and decides what
  is drawn;
- the **color identity** is per-plot threaded state — passed into domain
  resolution and returned possibly extended — and grows **monotonically**: a
  value is assigned on first sighting and is never removed or reordered for the
  life of the plot.

The distinction that matters is not input versus output, since resolution both
consumes and returns it. It is that identity is **carried forward** and never
reconstructed from the latest result.

A category absent from a filtered result therefore keeps its color reserved
rather than donating it to its neighbor, and a category that returns later
returns with the color it had.

Ownership divides cleanly:

- `ResolvedPlotSpec` carries the exact category-to-color assignments required to
  render. A renderer needs nothing else, so a pinned plot's colors are complete
  inside the artifact it retains.
- `ColorIdentity` is continuation state, consulted only when a later live
  resolution extends the same lineage.

A lineage is keyed by resolved field and layer identity together with the
category value. Changing the categorical field begins a **new lineage** rather
than inheriting assignments from a field that no longer encodes the series:
colors follow a meaning, not a position.

Monotonic does not mean unbounded. A geometry declares the categorical
cardinality it supports, and exhausting it is an attributed `DomainError` naming
the channel and the limit. Identities are never evicted, reassigned, or silently
cycled, because each of those recolors a series that already exists — the outcome
this section exists to prevent. Refusing is the only response to exhaustion that
does not quietly violate the rule.

This is also the second reason a pinned plot must retain its resolved
specification: color identity is history-dependent, so it is not reconstructible
from a revision and an authored spec alone.

### 9. Dual axes are unrepresentable in the Cartesian coordinate system

A Cartesian plot has one x scale and one y scale. A second, independently scaled
y axis is not offered, discouraged, or warned about — it has no representation.

This is a property of the coordinate system, not a global prohibition on
multiple ranges. Parallel coordinates and radar are distinct coordinate systems
with their own scale structures, and both remain shipped geometries. The rule
constrains Cartesian; it does not claim every multi-range display is dishonest.

### 10. The plot theme is not the interface palette

`PlotThemeSpec` is a separate artifact from `UiPalette`
(`libaryon/src/engine/ui_colors.rs:495`). Data-encoding color and interface
chrome color answer different questions, are validated against different
criteria, and change for different reasons.

The existing plot ramps do not move into `UiPalette`. A bridge maps only chrome
roles — background, axis, gridline, label — so a plot sits correctly inside the
application's light or dark surface without its data colors becoming interface
colors.

Default themes are validated as a build-time test against the accessibility and
consistency checks the project's visualization guidance defines, so a default
that fails contrast or ordering cannot ship.

### 11. Two registers, one specification

A terse register exists for low activation energy, and a grammar register exists
for full control. They are one language: the terse register is a **strict
projection** of the grammar, defined in terms of it, carrying no semantics the
grammar cannot express.

A terse form that grows a private option becomes a second language whose users
hit a wall that only makes sense in grammar terms. Neither register uses or
implies global mutable plotting state.

### 12. Boundaries preserved

- Toron executes relations. The specification, resolution, cascade, and rendering
  are product-side and never enter an engine.
- The relation reaching a specification is identified per PRODUCT-0002 —
  definition, residence, provenance, binding, stamped result. Materialized
  sources are v1; deferred relations join later without changing this contract.
- A presentation identifier is never a relation handle. Pane, layer, and view
  identity are product-local and may not be used to address data.
- Arrow remains interchange. Nothing here requires a compulsory in-memory
  representation.

## Rejected

- **Inferring channels from dtype order:** the present defect. It cannot
  represent an authored request, so it cannot be prevented from overriding one.
- **Falling back to a substituted field when a channel cannot resolve:** produces
  a plot that answers a question nobody asked, which is worse than a refusal
  because it looks like an answer.
- **One `PlotSpec` covering authored, resolved, and layout concerns:** guarantees
  that resolution-time and viewport-time decisions contaminate a portable
  artifact, which is how channel identity was lost in the first place.
- **Pinning the authored spec:** an exact revision already prevents data drift,
  but an authored spec plus a revision cannot reproduce a resolution — inference
  defaults, admissible-scale tables, and history-dependent color identity all sit
  outside it. The pin would bind the data and leave the presentation free to move
  under a product upgrade.
- **Evicting, reassigning, or cycling color identities at capacity:** each
  recolors a series that already exists, which is the defect the section forbids
  arriving through an exhaustion path instead of a filtering one.
- **Pinning the layout:** freezes a saved plot to the viewport that created it.
- **Sniffing values to determine semantic type:** makes a plot's meaning depend
  on the rows that happened to return.
- **A universal `Setting<T>` cascade:** admits `Hide` where absence is
  meaningless and makes an unrepresentable state representable.
- **Naming ramps instead of color jobs:** loses the only information that makes a
  palette checkable, and lets a sequential ramp encode unordered categories.
- **Binding series color to render order:** makes filtering recolor a chart,
  breaking comparison across views of the same relation.
- **Rebuilding color identity from each live result:** the same defect arriving
  through the back door, since a live plot re-resolves continuously and a
  result-derived identity shrinks whenever a filter narrows.
- **Treating a changed current revision as staleness for a pinned plot:** a pin
  names an exact revision, so later revisions are irrelevant to it rather than
  threatening; staleness is loss of the pinned revision itself.
- **Omission as inference authority:** a channel left out of a specification that
  names others is ambiguous between "infer this" and "the author forgot", and
  resolving that ambiguity silently is how a resolver acquires permission nobody
  granted.
- **Dual Cartesian y axes:** two independently scaled ranges sharing one plotting
  area make visual comparison arbitrary; the geometries that legitimately carry
  several ranges have their own coordinate systems.
- **Merging plot ramps into `UiPalette`:** conflates data encoding with interface
  chrome and makes theme changes silently change what a chart means.
- **A terse register with private semantics:** two languages wearing one name.
- **Local data transformation inside a specification:** an undeclared second
  execution engine in the workbench.

## Consequences and sequencing

- The scatter substitution becomes unrepresentable rather than fixed. A channel
  that cannot resolve fails at its Rune span with the field named.
- Kernels stop inferring. They consume a resolved specification, which is a
  narrowing of their input contract and removes the `unwrap_or(previous)` chains
  from every affected kernel.
- The resolved specification becomes the artifact durability pins, which gives
  PRODUCT-0002 §8's live and pinned variants a concrete difference.
- The corpus's proposed `.views([...])` surface lowers into authored
  specifications. This record does not accept that spelling; it defines what the
  spelling would produce, so acceptance remains a separate decision.
- The workspace artifact store remains undecided per PRODUCT-0002. Saved
  authored specs and durable plots cannot ship before that record exists.
- Implementation is vertically staged and receives its own plan: artifacts and
  resolver first, kernels narrowed to resolved channels second, surface lowering
  and durability third. Each stage carries acceptance evidence; the first fixture
  is `scatter("cat", "val")` resolving `cat` to nominal/point and `val` to
  quantitative/linear, or failing that sink alone.
- Rendering performance thresholds are captured as fixtures before kernels are
  touched, so a specification change cannot silently move a baseline that is
  currently untested.
