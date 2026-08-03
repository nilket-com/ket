# ADR 0010: multi-origin view execution and view identity

Status: accepted 2026-08-03. This record decides three things: that one buffer
may present views over more than one relation, that a view may carry a name that
is its address, and that Polariton realizes a view as a libaryon host panel. It
inherits ADR 0008's normal form, recognition rules, and ordering, and extends
them where multiplicity makes them insufficient. It authorizes no dashboard work,
no layout policy, and no new sink form.

## Context

ADR 0008 accepted `.views([...])` as a respelling under a stop condition: the
work halts if it requires changing execution. It does. The dogfood buffer that
provoked this record asks for two plots over two unrelated tables:

```rune
ket.table("test1").sort([col("x").asc()]).views([scatter("x", "y"), table()])
ket.table("test2").sort([col("cat").asc()]).views([bar("cat", "val"), table()])
```

This refuses today with `one view set must share one relation`. The refusal is
not incidental. It protects a load-bearing invariant stated in corpus v0.10 —
*one execution, N sinks, one stamped result shared by identity* — which
`rune_view_outcome` implements by wrapping one frame in one `Arc` and which
`state.rs` asserts with `Arc::ptr_eq` across every sink. Two relations means two
executions and two frames, and that assertion becomes false by construction.

Two further facts, established while negotiating this record, moved its scope
beyond execution.

**Form is not identity.** Presentation panes are keyed `(BufferId,
RuneViewForm)`, which is why two views of one form refuse. That key cannot
survive multiplicity: two relations may each want a `bar`. Any positional
replacement — a root index — supplies location rather than identity, and silently
retargets a pane when an earlier view is inserted or reordered.

**The head already has the substrate.** Polariton reaches presentation through
libaryon's host panel protocol, which already owns panel identity, geometry,
focus, and tab membership; already refcounts one `HostPanelId` across several
splits and tabs so that closing one reference does not discard content visible
elsewhere; already carries `PanelRevision` and `RefreshHostPanel` so a frontend
can *"mark or replace stale output"*; and already defines `PanelRestored` for a
persisted layout referencing a panel with no live content. What this record needs
from a head is therefore largely built. Polariton simply uses it at a population
of two compile-time constants, `RESULT_PANEL` and `DASHBOARD_PANEL`, and grows a
private pane tree inside the second one.

## Decision

### 1. Execution is keyed by canonical relation origin

> Within one buffer revision, output roots whose existing canonical-origin
> procedure produces the same identity execute once and share one stamped result.

This preserves today's fan-out exactly: `table(r)` and `plot.bar(r, ...)` over one
binding remain two roots, one execution, one shared `Arc`. Roots whose canonical
origins differ execute independently.

Identity is **textual after canonicalization**, using the procedure that already
exists — binding resolution through `resolve_relation_origin`, then the `ket.` →
`workspace.` folding in `canonical_relation_origin`. It is not semantic query
equivalence. Whitespace variants, reordered operations, and otherwise equivalent
expressions are distinct origins unless the existing canonicalization already
equates them. This is identity, not query optimization, and the record chooses the
narrow reading deliberately: a dedupe rule that reasons about equivalence would be
an optimizer, and PRODUCT-0003 forbids this analysis from holding opinions about
relation verbs.

The guarantee is a **guarantee**, not an optimization. Under one execution the
sharing was unobservable; under several it is, and the existing `Arc::ptr_eq`
assertion already promises it. Callers may rely on identical canonical origins
yielding one result by identity.

Distinct origins are **independent**. There is no cross-origin transaction, no
shared snapshot, and no ordering guarantee between their dispatches. They may
execute concurrently and complete out of order. A sequential first implementation
must not be read as a promise; only the presentation order in section 2 is
deterministic.

### 2. Execution grouping never changes presentation order

ADR 0008 §5's order stands unchanged and remains the oracle: **output-root source
position, then list order, then chain order**. Grouping roots by origin for
execution is invisible to presentation.

```rune
table(a);
plot.scatter(b, "x", "y");
plot.bar(a, "x", "y");
```

Execution groups the first and third sinks under `a`. Presentation order remains
table-`a`, scatter-`b`, bar-`a`. A record that let query grouping reorder views
would make execution strategy observable in the UI, which is the defect this
section exists to prevent.

### 3. A view may be named; anonymity means no stable address

```rune
ket.table("test1").views([
    view("test1-points", scatter("x", "y")),
    view("test1-data", table()),
])
```

`view(name, sink)` is a contextual identity wrapper recognized only in
`.views([...])` argument position, under ADR 0008 §3's contextual recognition
and its atomicity rule. It is not a fourth sink form. The name is a string
literal.

- **Names are unique per buffer.** Two views claiming one name is an attributed
  refusal naming **both** spans. Uniqueness is per buffer rather than per view set
  because a name is an address, and an address ambiguous across relations is not
  one.
- **Anonymous views are legal in any multiplicity**, including several of one
  form. An anonymous view has no stable address; the head assigns it a destination
  positionally, in the section 2 order. Reordering may shuffle anonymous
  destinations. Declining to name a view *is* the statement that its placement
  need not persist.
- **`DuplicateForm` is deleted.** It was a consequence of keying panes by form.
  Two anonymous bars are two views with two destinations. The rule disappears
  rather than generalizing into duplicate-name refusal, because an implicit name
  derived from a form would collide across relations and refuse the ordinary
  two-table script this record exists to admit.
- **The sugars gain no naming form.** `r.bar(x, y)` cannot express a name, so
  naming means writing the canonical `.views([view("n", bar(x, y))])`. This is
  ADR 0008 §2 operating as intended: sugar carries no semantics the canonical form
  cannot express, and here it simply carries one fewer.

### 4. The normal form gains names, not addresses

```text
BufferViewProgram {
    executions:         canonical origin -> ordered sinks
    presentation_order: ordered view references
}

ViewIdentity = Named(producer, name) | Anonymous(producer, position)
```

`ViewIdentity` is the language's address. `HostPanelId` is **not** in the normal
form and never enters it: it is the head's realization of a view address, and a
head with no host panels is still able to implement this language. Polariton
resolves `ViewIdentity` to `HostPanelId`; the resolution is head-owned and
invisible to `EnvelopeSemantics`.

A named view **keeps its identity across sink form changes**. Editing
`view("trend", bar(...))` into `view("trend", scatter(...))` retargets the same
address with different content. The name is the address; `RuneViewForm` is not.

### 5. Named addresses need a durable key

A runtime `BufferId` is a session identity and cannot key a panel meant to
survive a restart. The durable key has the shape:

```text
(workspace identity, durable document identity, view name) -> HostPanelId
```

- A buffer with a durable document identity — a saved file — contributes it.
- An untitled or otherwise non-durable buffer receives a **session-only**
  identity. Its named views behave identically within the session and simply do
  not survive it.
- **Rename** migrates the binding key with the document: the same work under a
  new name keeps its panels.
- **Save-as producing a copy** gives the new document no bindings. Its named views
  allocate fresh panels on next run. Two documents must never claim one panel,
  which inheriting the bindings would allow.
- The same view name in two documents is two keys and therefore two panels.

The persistence mechanism is an implementation detail and is not decided here.
Whether libaryon today exposes a durable document identity suitable for this key
is a precondition the implementation plan must establish rather than assume.

### 6. Producer state reuses the existing panel protocol

This record defines producer state and **maps it onto the host panel protocol**.
It does not introduce a parallel state vocabulary. Inventing one beside
`PanelRevision`, `RefreshHostPanel`, `PanelContent`, and `PanelRestored` would
repeat, one level down, the mistake of building a second pane system inside a
panel.

- A **successful envelope analysis makes the new set of named views
  authoritative.** Names no longer present become orphaned.
- **Parse or envelope failure is not deletion.** Existing destinations become
  stale or error-marked; nothing is orphaned on the strength of a syntax error.
- **A failed origin marks only that origin's views stale.** Successful origins
  refresh normally in the same run. This is the isolation independent dispatch is
  for; without it, multiplicity buys nothing.
- **A failed sink resolution marks only that sink stale.**
- **A returning name reattaches to its preserved destination and layout.** This is
  why deletion orphans rather than closes: a panel the author arranged is worth
  more than strict correspondence with the current buffer text.
- **Anonymous destinations carry no persistence guarantee** across structural
  edits. Excess positional destinations may be closed or recycled. Authors who
  want layout stability name the view. Without this, orphaned anonymous
  destinations accumulate with no address through which they could ever return.

### 7. The dashboard is out of scope, with one negative requirement

> Completing a view must neither open nor mutate the dashboard.

Today `RuneRelationPlotReady` calls `open_dashboard()` before placing a plot, so
that routing decision makes the dashboard the place where plots become real.
Removing the call is necessary but not sufficient: ordinary host-panel content
currently has no plot variant, while tables already render as
`PanelContent::Table`. This record therefore authorizes Polariton to add
host-owned plot panel content and the per-panel backing state needed to render
it. That is realization of an accepted view in the existing host-panel protocol,
not a new sink, a dashboard redesign, or a libaryon protocol capability.

This record demotes the dashboard to a **consumer** of views and decides nothing
else about it. Whether it survives as a curated pinboard or is retired in favor of
ordinary host panels is a separate record, made on its own evidence, once views
exist independently of it. Its export bounds and resize-suspension behavior are
real features that would need re-expressing per panel; they are properties of plot
content rather than of a layout container, and this record neither migrates nor
discards them.

### 8. Acceptance gates

The implementation is judged by these, and none may be satisfied by a test that
inspects only the analysis:

1. Canonically identical relation origins execute once and share one `Arc`.
2. Distinct origins dispatch independently, with no shared-transaction guarantee.
3. Execution grouping never changes ADR 0008's global presentation order.
4. Two anonymous views of one form are legal; `DuplicateForm` no longer exists.
5. Duplicate explicit names refuse, naming both spans.
6. A named view reuses its `HostPanelId` across reruns.
7. A named view changing sink form retains its identity and destination.
8. Anonymous views reuse positional destinations only within their documented
   session and structural lifetime.
9. Two `table()` sinks populate two distinct host panels.
10. A failed origin leaves its previous panels stale while successful origins
    refresh in the same run.
11. A structurally valid revision missing a former named view leaves its restored
    panel unresolved or orphaned, not closed.
12. Parse or envelope failure is not interpreted as deletion.
13. Completing any view neither opens nor mutates the dashboard.

### 9. The stop condition

> This record may add multi-origin dispatch, view naming, view-identity
> resolution to host panels, and the producer-state mapping in section 6. If the
> implementation requires a new libaryon protocol capability, a change to Toron or
> the transport, a new sink form, or a layout placement policy, the work stops and
> the requirement returns as its own record.

Automatic tab placement is the anticipated trigger. `OpenHostPanel` carries a
`split_direction` for a panel's first appearance and no way to request placement
as a tab beside an existing panel. Views therefore open as ordinary host panels
and authors arrange or tab them by hand. That proves independent view identity
without smuggling a layout policy into this record, and a libaryon protocol
extension for requested tab placement is deferred to the work that needs it.

### 10. What this record does not authorize

No change to Toron, the transport, `PlotSpec` artifacts, theming, or kernels. No
new sink form or geometry. No relation names or handles. No shared-encoding sugar.
No retirement of the free-function spelling. No dashboard migration, retirement,
or export change. No layout persistence mechanism, and no tab placement policy.

## Rejected

- **One execution per output root:** breaks the landed fan-out. Two roots over one
  binding would dispatch twice and falsify the `Arc::ptr_eq` assertion for the
  case that already works.
- **One execution returning a tuple of relations:** requires Toron's Rune result
  protocol to carry several result sets — a transport change crossing the process
  boundary — to amortize nothing, since distinct origins share no work.
- **Semantic query equivalence as the dedupe key:** makes the envelope analysis an
  optimizer and requires it to know relation verbs it is forbidden to know.
- **Implicit names derived from sink form:** tidy, and it would unify duplicate-form
  and duplicate-name refusal into one rule, but two relations each with an
  unnamed `bar()` would then collide across the buffer. It refuses the ordinary
  script this record exists to admit.
- **`(BufferId, root_index, RuneViewForm)` as the destination key:** an acceptable
  prototype and a poor address. Inserting a view above silently retargets panes,
  and a runtime `BufferId` cannot survive a restart.
- **A whole-buffer `table()` singleton:** the minimal correct implementation
  against today's single results grid, and a leak of a head limitation into the
  language. `PanelContent::Table` already renders; two tables collide only because
  `RESULT_PANEL` is a constant.
- **`HostPanelId` in the semantic normal form:** a head realization leaking into
  the language, which would make a head without host panels unable to implement it.
- **Deciding the dashboard's fate here:** bundles a layout question with an
  execution and identity question, so a problem in either becomes a problem in
  both.
- **Automatic tab placement in this record:** needs a libaryon protocol capability
  that does not exist, and section 9 halts on exactly that.

## Consequences and sequencing

- The dogfood buffer runs as written, including both `table()` sinks, once views
  resolve to distinct host panels.
- The concrete new head work is `HostPanelId` **allocation** plus ordinary
  host-panel plot content. Polariton's panel population is two compile-time
  constants; named views need dynamically allocated ids that are stable across
  runs, which means Polariton owns the section 5 table and, eventually, its
  persistence. Plots additionally need a `PanelContent` realization beside the
  table realization that already exists.
- Whether libaryon exposes a durable document identity suitable for the section 5
  key is an open precondition, not an assumption. The implementation plan
  establishes it before depending on it.
- `PanelRestored` documents itself as having *"no producer yet: this arrives with
  layout persistence for host panels."* Section 6's orphaned state is the first
  consumer that will want one, which is a dependency to sequence rather than a
  blocker: within a session, orphaning needs no restore path.
- ADR 0008 §7 halted correctly, and its section 6 additivity holds: the
  free-function spelling still lowers, now into a multi-origin program.
- Corpus rows for view fan-out gain a multi-origin row beside the shared-execution
  row, each retaining its own citations. The load-bearing invariant is narrowed
  from *one buffer, one execution* to *one canonical origin, one execution*, which
  is what the evidence actually proved.
- The plotting language stops being blocked on a layout question, and the
  dashboard's export and resize behavior can be migrated or retired later under a
  record with evidence behind it.
- A future record may add relation handles, requested tab placement, shared-encoding
  sugar, or bindable sink specifications. None is authorized here, and each
  inherits this normal form.
