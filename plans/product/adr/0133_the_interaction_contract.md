# ADR 0133: the interaction contract

Status: accepted 2026-08-19. This record is the interaction record PRODUCT-0044
names when it says the hover half of PRODUCT-0013 §3's bridge "stands until an
interaction record exists." It replaces that hover half, and it fills the
round-trip question PRODUCT-0007 §7 deferred for the pane-override layer.

It decides hit resolution, the shape of a hit, the typed selection intent,
selection-context identity, the composition algebra, predicate lifetime across
republication, viewport state, and which existing surfaces consume the contract.
It introduces one new product value, the `ArtifactToken` of §5, and requires
every published-relation path to mint one. It does not decide Rune spelling for any
of them; PRODUCT-0007 sends spelling to a surface record and this record stays a
contract. It does not decide guide policy beyond the hover half it replaces, does
not add a data-driven size channel, does not decide cross-relation filtering, and
does not decide log axes, nice-number ticks, or any deferred axis work.

## Context

Interaction is the largest unwritten contract in the plotting arc. Every chart in
Polariton today answers exactly one event. `PlotWidget::update`
(`crates/polariton/src/plot/common.rs:2539`) matches `Event::Mouse(CursorMoved)`
and falls through to `None` for everything else: no button press, no wheel, no
drag state. There is no zoom, pan, brush, click-to-filter, cross-filter, or
linked selection anywhere in the crate.

The one interaction that exists is provisional by construction. PRODUCT-0013 §3
authorized a hover bridge "**only** to satisfy the existing `PlotKernel`
contract," carrying "no compatibility guarantee," ranking hits by nothing but
back-to-front paint order, and reporting "whatever that layer already reports"
verbatim. That subsection states that any behavior beyond it — "ranking hits by
anything other than paint order; composing or rewording hover content" — "is a
guide or interaction decision and triggers §9's halt." PRODUCT-0044 retired the
legend half of that bridge and left the hover half explicitly pending this
record.

Twenty kernels have since been written against the bridge. `fn hover(&self,
transform, cursor) -> Option<String>` (`plot/common.rs:1563`) returns prose, so
every kernel today answers "what is under the cursor" by formatting a sentence.
Selection cannot be built on that signature: a sentence is not a hit, and a hit
is what a brush, a click, and a cross-filter all need. The longer the mark
catalog grows against the prose signature, the more expensive this record's
implementation becomes.

The surrounding contracts are already in place and are the reason this record can
be small. PRODUCT-0007 §7 defines five precedence layers ending at "pane
override," describes it as "an interactive, session-scoped adjustment," and
defers "whether and how it round-trips into the authored spec … to the
implementation record, because it is an editing-model question rather than a
specification one." PRODUCT-0002 §3 gives relation lifetime three independent
axes with `save`/`cache`/`materialize`/`pin`/`export` as separate explicit verbs;
§4 separates a relation's input identities from its versioned expression; and its
newest-wins discipline already rules that "stale results never overwrite a newer
published view." PRODUCT-0097 established the faceted hover coordinate seam.
KET-0005 §3 and PRODUCT-0002 §1 both forbid product logic from branching on
engine brand.

Two consumers outside the plot layer are already waiting. `gui/table.rs:494-700`
implements rectangular cell selection with shift-extend, drag-extend, Ctrl-C to
TSV, and Escape — a complete selection gesture with nowhere to send its meaning.
And `Plot ▸ Export` must be told, once selections exist, whether it emits the
filtered or unfiltered relation.

## Decision

### 1. Hit resolution is dimensional, then ordinal, and it is total

A hit is resolved by three rules applied in order:

1. **Dimensionality**, most specific first: 0-D marks (points, markers, bubble
   centers, dial readings) beat 1-D marks (strokes, lines, whisker spans) beat
   2-D marks (areas, bars, cells, slices, violin bodies).
2. **`paint_order`**, later wins, when dimensionality ties. This is the
   z-order in which layers are painted; the topmost painted layer takes the hit.
3. **`sibling_ordinal`**, ascending, when both tie. This is the relation row
   ordinal within a single layer, and it resolves overlapping marks of the same
   kernel and dimensionality — two coincident scatter points, say. First row
   wins, matching the first-row-wins convention the stacking and area families
   already use for duplicate keys.

The two ordering rules are deliberately given distinct names because they are
distinct concepts: `paint_order` ranks *between* layers and later wins,
`sibling_ordinal` ranks *within* one layer and earlier wins. A single "layer
ordinal" would leave the implementation with two competing z-order rules and no
statement of which direction is authoritative.

Dimensionality is a property of the mark's hit geometry, not of its visual
weight: a thick stroke is still 1-D and a small rectangle is still 2-D. The rule
is stated dimensionally because "most specific" is not testable and
paint order alone cannot explain why a point on a line wins.

Composite kernels — layered, faceted, grid — do not mint a second policy. They
report the provenance of the hit they forward (§7) and let this rule decide.
PRODUCT-0013 §3's back-to-front bridge retires here.

### 2. A hit is structured; prose is a presentation of it

`PlotKernel`'s hover method is replaced by a total method returning an optional
structured hit carrying, at minimum: the mark family that was hit, the resolved
channel values with their semantic types, the series or color identity when the
mark has one, the facet coordinate when the plot is faceted, and the layer
ordinal.

Hover wording becomes a presentation of that hit rather than a kernel's private
sentence. The same hit is the input to selection, so hit-testing is written once
per kernel and not twice.

This is a breaking change to every kernel that implements the trait. It is
budgeted explicitly in Consequences rather than discovered during
implementation.

### 3. Selection is a typed intent, not a callback

A gesture emits one value:

```text
SelectionIntent {
    input_ref,             // the stable selection-context key, §4
    artifact_token,        // the current publication identity, §5
    source_view,           // the emitting view identity
    constraints,           // a conjunction of typed constraints, §6
    within_source,         // replace | add | subtract, §6
}
```

`input_ref` and `artifact_token` are separate fields answering separate
questions. `input_ref` says *which context this belongs to* and is stable across
republication. `artifact_token` says *which publication it was computed against*
and changes on every dispatch. Collapsing them would make every republication
look like a new context.

The intent is data, not a plot-specific callback. Plots, tables, and any later
view kind emit the same value, which is what keeps cross-filtering a relation
concern rather than a renderer feature.

`within_source` is named for what it does. It is a local gesture operation over
the emitting view's own predicate; it is not the algebra by which predicates
compose, which is §6's and belongs to the selection context.

### 4. Selection contexts key on input identity

A selection context is keyed by the **input identity** of the relation the view
originates from — the notion PRODUCT-0002 §4 already separates from the versioned
relation expression — and not by the fully resolved expression.

Two views whose presentation chains differ therefore share one context when they
originate from the same published input. `trends.views([...])` and
`trends.sort([...]).views([...])` cross-filter each other. A selection made on
the sorted view filters the unsorted view against the same input relation.

This consequence is intended, not tolerated. Keying on the resolved expression
would put two figures showing visibly the same data into different contexts,
which no user would predict.

Unrelated relations do not cross-filter. Cross-relation filtering requires an
explicit mapping or join and is deferred to a future record.

### 5. This record introduces `ArtifactToken`, and every published relation mints one

The intent carries a Polariton-owned `ArtifactToken`: the identity of the
publication a selection was computed against. **This record introduces it. It is
not an existing value this record borrows.**

`ResultRegistry` is today the closest thing to a producer, but it is not
universal: `ResultRegistry::begin` (`crates/polariton/src/result_registry.rs:54`)
is keyed by name and runs only for SQL carrying a `-- @result name` annotation.
Unannotated SQL, Rune relation views, and the other adapter paths publish
relations that receive no revision at all. A contract keyed on what
`ResultRegistry` mints today would therefore cover a minority of the surfaces it
must cover.

**Every published-relation path mints an `ArtifactToken`.** `ResultRegistry`
becomes one producer of a token it does not exclusively own, and its existing
per-name revisioning becomes one implementation of the identity rather than its
definition.

The token is never a Toron revision. Revisions are a Toron concept; SQLite,
Postgres, MySQL, BigQuery, and Parquet have none. Keying this contract on one
would confine cross-filtering to a single engine and branch product logic on
engine brand, which KET-0005 §3 and PRODUCT-0002 §1 both forbid.

### 6. The algebra: OR within a field, AND across fields, one predicate per view

Each view contributes exactly one source-local predicate to its context. The
context resolves predicates as follows:

- Selected values **within one field** compose with **OR**.
- Constraints **across fields** compose with **AND**.
- Predicates **from different views** compose with **AND**.

`within_source` operates only on the emitting view's own predicate: `replace`
discards it, `add` unions into it, `subtract` removes from it. A new selection
from a view never alters another view's predicate.

Constraints are typed and preserve nominal, ordinal, quantitative, and temporal
semantics. A temporal constraint carries a timezone-bearing interval in the
civil frame PRODUCT-0128 through PRODUCT-0130 established; a brush on a
`Datetime{zone}` axis emits a temporal interval, never a float pair. A scatter or
bubble brush constrains every channel the gesture bounds, so `constraints` is a
conjunction and not a single field.

**`subtract` is admitted only for nominal and ordinal value sets.**
Subtracting an interval from an interval yields a disjoint set and would force
disjunctive ranges into the algebra on its first day. Quantitative and temporal
`subtract` refuse rather than approximating.

### 7. A view does not filter itself, and facet provenance rides with the constraint

A view resolves against the conjunction of every predicate in its context
**except its own**. Its own selection renders as emphasis, not as a filter.

Without this rule a brush deletes the data around itself and becomes
unadjustable: widening a selection requires the unselected data to still be
drawn. Emphasis also gives the source view the context that makes a selection
legible.

A faceted figure is one view over many panels. A gesture inside a facet cell
emits its brush **conjoined with that cell's facet-field value**, routed through
PRODUCT-0097's faceted hover coordinate seam rather than a second coordinate
story. A facet brush that omitted its panel would silently mean something the
user did not gesture.

### 8. Stale intents are rejected visibly, and republication clears the context

Two distinct staleness cases exist, and only one is a race.

**An arriving intent may be stale.** Before an intent mutates a context, its
`artifact_token` is checked against the current token for that input identity. A
stale intent is rejected without altering selection state, reusing PRODUCT-0002's
newest-wins discipline rather than inventing a second staleness rule.

Rejection is **surfaced**, not silent. A brush that does nothing and says nothing
is indistinguishable from a defect. It is surfaced through the ordinary
`status_msg` lane as a non-error notification — for example, "Selection expired
because the source relation was refreshed; select again." It does **not**
populate `status_error` or the `StatusDetail` overlay, which remain reserved for
failures and copyable diagnostics respectively.

The surface is chosen by user agency and payload, not by rarity. A stale intent
is a race the user did not cause and cannot act on beyond repeating the gesture,
and it carries nothing worth copying. §11's refused rectangle is classified by
the same test and lands in the same lane.

**A stored predicate may be stranded.** Rejecting arriving intents does nothing
about predicates already held in a context when the relation republishes.
Therefore: **when publication for an input identity is retired or replaced, every
predicate bound to the prior artifact token is cleared before the new artifact
becomes observable.**

Without that rule a selection made against one result silently filters the next
one — a row set the user never saw, narrowed by criteria they cannot see. The
ordering is load-bearing as much as the clearing: predicates are cleared *before*
the new artifact becomes observable, never after, so the window in which that
could happen never opens.

This is the discipline `ResultRegistry::begin` already applies to the artifact
itself — it calls `self.bound.remove(name)` on dispatch so that, in its own
words, "no consumer can observe stale data while the query runs." This section
extends that existing rule to the predicates bound alongside the artifact rather
than introducing a second lifecycle.

### 9. Zoom and pan are pane overrides, and do not round-trip

Interactive zoom and pan occupy PRODUCT-0007 §7's existing fifth precedence
layer. They introduce no new cascade algebra and reuse `Override<T>`.

Viewport state is session-scoped and ephemeral. **Dragging or scrolling must not
mutate Rune source, mint an authored revision, or change durable plot meaning.**
An explicit "promote viewport to authored domain" command may be added by a later
record; the gesture alone never promotes.

This answers the question PRODUCT-0007 §7 deferred, in the direction that keeps
exploratory viewport state and specification distinct.

### 10. Export resolves the current reference and gets no policy of its own

Whether an export reflects a filtered view is not an export decision. Once
selections are typed predicates over a relation, a filtered relation is an
ordinary relation reference and export resolves it exactly as PRODUCT-0002 §3's
`export` verb already resolves any other.

A separate export-filtering policy would be the second source of truth
PRODUCT-0002 was written to prevent.

### 11. The grid emits this contract only where the algebra is lossless

`gui/table.rs`'s selection emits `SelectionIntent` like any other view. It does
not keep a private selection meaning beside this one. Its existing gesture
vocabulary — click, shift-extend, drag-extend, Ctrl-C, Escape — is unchanged;
only the meaning it publishes is new.

But an arbitrary rectangle is not representable by §6's algebra, and the failure
is not a refusal — it is a silent widening. `TableSelection` is an
anchor/active rectangle over positional `(column, row)` pairs
(`gui/table.rs:408`). A rectangle spanning columns A and B over rows 2–4
denotes three row tuples. Under OR-within-field and AND-across-fields it would
resolve to `A ∈ {a₂,a₃,a₄} AND B ∈ {b₂,b₃,b₄}` — a cross product admitting
`(a₂, b₄)`, a tuple the user did not select. The predicate would be strictly
wider than the gesture, and nothing in the interface would show it.

Therefore:

- **A selection within a single column emits an intent.** It denotes values in
  one field and is exactly OR-within-field. Lossless.
- **A selection spanning multiple columns does not emit an intent.** It remains
  a clipboard selection, as it is today.

The multi-column case **refuses rather than approximating**, consistent with how
this arc has treated every other unrepresentable request. Making it expressible
requires either a row-identity constraint — a key concept the product does not
have — or disjunctive tuple constraints in the algebra. Both are deferred, and
either would be a future record.

Ctrl-C is unaffected in both cases: copying is a clipboard operation and carries
no relational meaning.

**The refusal is disclosed as ordinary interaction feedback.** On mouse-release
of a multi-column rectangle, the existing status surface reports that filtering
is limited to single-column selections and that the clipboard gesture remains
available — for example, "Filtering requires a single-column selection. The
selected rectangle remains available for Ctrl-C."

The refusal **is not an error**. It carries no error tone, publishes through the
ordinary `status_msg` lane rather than the `status_error` lane of
`app_status_bar` (`crates/polariton/src/gui/components.rs:330`), and does **not**
route through the `StatusDetail` overlay, which is framed as copyable SQL
diagnostics and would misclassify a normal gesture as a fault. It mutates no
state and does not clear or alter the visual table selection: the rectangle the
user drew stays exactly as drawn.

Silence here would be indistinguishable from a broken filter. A user who drags
across two columns and sees nothing happen has no way to learn that the gesture
is valid for the clipboard and merely unrepresentable as a predicate.

## Rejected

- **An arbitrary relation graph.** Automatic downstream re-emission admits
  feedback loops and makes the resolved filter set unexplainable to the user who
  produced it. The star is bounded and total; a later record may add explicit
  mappings.
- **A filter chain.** Order-dependence in a chain means the same two gestures
  produce different results depending on which was made first, with nothing in
  the interface showing the order.
- **A Toron revision as the binding token.** Engine-branded contract; forbidden
  by KET-0005 §3 and PRODUCT-0002 §1, and it would confine cross-filtering to one
  adapter.
- **Keying contexts on the resolved relation expression.** Splits visibly
  identical data across contexts whenever presentation chains differ.
- **Views filtering themselves.** Makes brushes unadjustable and destroys the
  context that makes a selection readable.
- **`subtract` over quantitative and temporal ranges.** Requires disjunctive
  ranges in the algebra before anything has asked for them.
- **Retaining prose hover.** A sentence cannot serve as a selection candidate,
  and keeping both would mean hit-testing written twice per kernel.
- **A separate export-filtering policy.** Duplicates PRODUCT-0002 §3.
- **Emitting multi-column grid rectangles as conjunctive predicates.** Produces a
  predicate strictly wider than the gesture, admitting tuples the user did not
  select, with nothing in the interface disclosing the widening.
- **A row-identity constraint to make rectangles expressible.** Requires a key
  concept the product does not have, across adapters that do not agree on one.
- **Reusing `ResultRegistry`'s per-name revision as the binding token.** It is
  minted only for `-- @result`-annotated SQL, leaving Rune views, unannotated
  SQL, and the other adapter paths with no token at all.
- **Leaving stored predicates in place across republication.** Rejecting stale
  arriving intents does not address predicates already held; a selection would
  silently filter a result it was never computed against.
- **A single "layer ordinal" tiebreak.** Between-layer and within-layer ordering
  run in opposite directions; one name for both leaves the direction unstated.
- **Routing interaction feedback through `StatusDetail` or `status_error`.**
  Both stale-intent rejection and refused multi-column selections are ordinary
  outcomes of valid gestures that the user neither caused nor can act on, and
  neither carries a copyable payload. Classifying them as faults would misreport
  normal interaction and erode the meaning of the diagnostic surface. Surface
  choice follows user agency and payload, not rarity.
- **Zoom round-tripping into the authored spec.** Collapses PRODUCT-0007's
  distinction between exploratory viewport state and specification, and makes
  every scroll a document mutation.
- **Implicit cross-relation filtering.** Requires a join or mapping the product
  has not specified.
- **Deciding Rune spelling here.** PRODUCT-0007 sends spelling to a surface
  record; this one stays a contract.

## Consequences and sequencing

**The hover-signature migration is the largest line item and is not optional.**
Replacing `Option<String>` with a structured hit touches every kernel
implementing `PlotKernel` — roughly twenty files under
`crates/polariton/src/plot/kernels/`, plus the composite paths in
`plot/common.rs`, `plot/kernels/layered.rs`, and `plot/grid.rs`. Kernels whose
hover currently returns `None` (`kernels/radar.rs:227`) still require a total
method. This work is mechanical but broad, and it must land before any selection
gesture can be built. Any implementation plan that sizes interaction without it
is wrong.

**PRODUCT-0013 §3's hover bridge retires**, closing the last live clause of that
subsection after PRODUCT-0044 retired its legend half. Bridge tests marked as
bridge coverage are replaced rather than preserved, as PRODUCT-0013 anticipated.
PRODUCT-0044's pending clause is discharged.

**PRODUCT-0007 §7's deferred round-trip question is answered** by §9. The
pane-override layer itself is unchanged.

**This record makes deferred-ledger IG-02 decision-complete.** Its three named
items — hover ranking beyond paint order, hit preference between siblings, and
how content is worded — are decided by §1 and §2. It does not itself close the
ledger entry: under the closure discipline stated in
`plans/product/review/deferred_triage.md`, triage ratification must copy the
closure reason into the ledger and the owning records. That discipline has never
run, and this record does not run it unilaterally. IG-01's remaining guide half,
IG-04's size encoding, AX-05 log axes, and AX-07 nice-number ticks are untouched.

**Ordering.** This record must be accepted before implementation of result-grid
selection or export hardening: §11 makes the grid a consumer of this contract,
and §10 removes a decision export hardening would otherwise have to invent.
Its implementation may follow the connection, cancellation, metadata, CSV, and
export engineering items. Acceptance is the contract gate, not an
implementation-order claim.

**§5 has a prerequisite the plotting arc does not contain.** Minting an
`ArtifactToken` on every published-relation path touches the Rune view path, the
unannotated SQL path, and each adapter's publication seam — not only
`ResultRegistry`. That work precedes any selection gesture and belongs in the
implementation plan as its own stage.

**Left open by design.** Named independent selection contexts, cross-relation
mapping, disjunctive range constraints, promotion of viewport to authored domain,
and any authorable Rune spelling for selection or viewport each require their own
record. None of them destabilizes the model decided here, which is the reason
that model is deliberately one context, many views, conjunctive filtering, and no
implicit graph traversal.
