# Implement the interaction contract

Scope: implement PRODUCT-0133. Add the adapter-neutral publication identity,
replace prose hover with structured hits, implement the session-scoped pane
interaction and selection context, and connect the existing table and export
surfaces as consumers. The implementation has no Rune spelling, no
cross-relation mapping, no row-identity constraint, and no disjunctive range
algebra.

The work is six commits, in this order:

```text
test: adr 0133 capture interaction and publication baselines
impl: adr 0133 mint adapter-neutral artifact tokens
impl: adr 0133 replace hover prose with structured hits
impl: adr 0133 add pane interaction and selection contexts
impl: adr 0133 connect table and export consumers
evidence: close interaction contract
```

The first commit changes no production behavior. It makes the current hover,
publication, table-selection, and export boundaries visible before the breaking
substrate work. The evidence commit records the gate ledger and verification;
it contains no implementation change.

Production stages require PRODUCT-0133 to be accepted. Creating this plan does
not close IG-02, ratify `deferred_triage.md`, or copy closure reasons into the
ledger and owning records.

## Halt rule

PRODUCT-0133 permits an adapter-neutral artifact token, structured plot hits,
the dimensional/paint/sibling hit policy, typed single-context selection,
session-scoped pane overrides, visible stale/refusal feedback, and the table
and export consumer seams. If implementation requires a Rune spelling,
cross-relation mapping, a row-identity model, disjunctive tuple/range
constraints, persisted viewport state, durable selection contexts, or a
product-specific adapter branch, stop and return the finding to PRODUCT-0133
or a new owning record.

The work also stops if a selection has to be applied by changing the source
relation's meaning rather than by resolving a filtered reference for the
current view, or if an adapter must invent a different selection algebra. The
contract is product-side; adapter differences are capabilities at the
publication or relation-evaluation seam, never branches in hit or context
logic.

### Where work may land

Expected production areas are:

- `crates/polariton/src/result_artifact.rs` or a product-side companion module
  for `ArtifactToken` and the immutable publication identity;
- `crates/polariton/src/result_registry.rs` and `crates/polariton/src/gui/state.rs`
  for publication-token minting and retirement across named and unannotated SQL;
- `crates/polariton/src/adapters/common.rs` plus each adapter publication seam
  for paths that bypass the plain SQL registry, without adding engine-specific
  token semantics;
- `crates/polariton/src/plot/common.rs`, `plot/grid.rs`, and
  `plot/kernels/*.rs` for structured hits and hit resolution;
- `crates/polariton/src/gui/plot_pane/`, `gui/messages.rs`, and the editor's
  plot-host projection for hover presentation, pane interaction, and status
  messages;
- `crates/polariton/src/interaction.rs` or an equivalent product-side module
  for typed constraints, context state, and selection composition;
- `crates/polariton/src/gui/table.rs` for the single-column consumer and
  multi-column disclosure; and
- the existing export/reference seam for resolving the current filtered
  relation without creating a second export policy.

Any change to Rune grammar or lowering, cross-relation semantics, durable
session schema, engine protocol, or adapter-specific selection behavior is a
halt unless a cited Stage 0 finding proves that the change is the existing
consumer seam rather than a new contract.

## Stage 0 — feasibility, census, and baselines

### 0a. Census the current interaction surfaces

Record an exact source census before production edits:

- every `PlotKernel::hover` and `PlotComposite::hover` implementation,
  including test-only kernels and composites;
- every `Option<String>` hover state and message path, including
  `PlotCanvasState::last_hover`, `HostPlotState::hovered_info`, editor-owned
  plot state, and `PlotPaneMsg::UpdateHover`;
- every plot path that constructs a transform and performs hit testing;
- every publication path returning `ExecutionResult::Rows` or
  `RuneDispatchResult`, including named SQL, unannotated SQL, Rune views, and
  each adapter;
- every current result/input identity and revision field;
- table selection creation, range extraction, Ctrl-C, Escape, and status
  projection; and
- export snapshot construction, filtered-reference availability, and write
  completion/error reporting.

The census must distinguish production kernels from test-only implementations.
It is a source guard against migrating the obvious twenty kernels while leaving
an ordinary composite or test contract on prose.

### 0b. Capture the current hover and publication baseline

Add tests that preserve current behavior before the breaking trait change:

- representative hover strings for nominal, quantitative, temporal, stacked,
  faceted, composite, and no-hit cases;
- the current back-to-front behavior for an overlapping layered fixture;
- one current result publication and latest-dispatch-wins race for named SQL;
- unannotated SQL, Rune, and adapter result construction showing the paths that
  currently lack a common publication token;
- current single-column and multi-column table selection plus Ctrl-C; and
- current export snapshot selection and completion behavior.

The baseline may use existing test fixtures and must not freeze an accidental
hover string as the new interaction contract. Its purpose is to prove which
behavior changes deliberately when the structured hit and selection stages
land.

**Stage 0 stop condition.** If a result path cannot be named by a stable input
identity, or if a current plot consumer needs a second hover policy to preserve
its output, stop. Do not solve either finding by borrowing a Toron revision or
by retaining an undocumented composite exception.

Commit: `test: adr 0133 capture interaction and publication baselines`.

## Stage 1 — mint `ArtifactToken` at every publication seam

### The two identities remain separate

Add an opaque, product-owned `ArtifactToken`. It identifies one published
artifact and is not an adapter revision, query ID, connection identity, or
relation-expression hash. Keep the stable `input_ref` separate: it selects the
selection context, while the token validates that an intent was computed
against the current publication.

The token must be minted at the product publication boundary, not by parsing
an engine response. Where a path already has a dispatch claim, the token may
be carried beside that claim. `ResultRegistry`'s per-name revision remains an
implementation detail of one producer; it is not the token's definition.

### Cover every path

Trace and cover all of these paths explicitly:

- named SQL with `-- @result name`;
- unannotated SQL result panels;
- Rune relation views and their fan-out results;
- SQLite, Postgres, MySQL, BigQuery, and Parquet row results; and
- Toron SQL and Rune results, without exposing a Toron revision in the product
  value.

Prefer one central product-side minting seam after an adapter returns a
materialized result. Add adapter-local forwarding only where a result bypasses
that seam. The token is immutable once attached to the published artifact.

### Publication lifecycle

Extend the existing newest-wins lifecycle:

1. When an input identity is republished or its current publication is retired,
   invalidate the old artifact token before exposing the replacement.
2. Clear every selection predicate bound to the old token before the new
   artifact is observable.
3. Publish the new artifact and token together.
4. Reject an arriving intent whose token is no longer current, without changing
   context state, and report the rejection through ordinary `status_msg` with
   `status_error` clear.

Do not make token invalidation depend on whether a result has a visible panel;
the same publication identity must govern tables, plots, Rune consumers, and
export references.

### Acceptance gates

- `ArtifactToken` is opaque and cannot be constructed from an adapter revision.
- Repeated publication of one `input_ref` keeps the context identity and mints
  a new token.
- Every listed path receives a token, including unannotated SQL and Rune.
- Any projection or derived-artifact wrapper over an existing publication
  inherits that publication's token; only a fresh publication mints a new one.
- An old token is invalid before the replacement artifact is observable.
- A latest-dispatch-wins race cannot publish or validate an old token.
- Token creation and invalidation contain no adapter-name or engine-brand
  branch.

Commit: `impl: adr 0133 mint adapter-neutral artifact tokens`.

## Stage 2 — replace hover prose with structured hits

### Product-side hit vocabulary

Add structured hit types beside the existing plot contract. The hit must carry
the facts needed by both presentation and selection, including:

- mark family and hit dimensionality (`Point`, `Line`, or `Area`/equivalent);
- source field identity and typed channel values with semantic types;
- series/color identity where one exists;
- facet coordinate and facet-field provenance where applicable;
- `paint_order` for the containing layer; and
- `sibling_ordinal` within that layer.

Hover text is derived from the structured hit at the host/presentation seam.
No kernel returns a user-facing sentence as its interaction result. A no-hit is
still `None`; “total” means every kernel implements the structured method and
returns an explicit no-hit rather than leaving an unimplemented path.

### Trait migration

Replace `PlotKernel::hover` and `PlotComposite::hover` with structured hit
methods, including their settings-aware variants. Migrate every production and
test implementation. The migration must include kernels whose current method
always returns `None`, such as the radar path, because the trait contract is
total and future selection cannot depend on a partial implementation.

The common canvas retains one hit cache/state value, but it publishes a
structured hit. `HostPlotState`, editor-owned plot state, and plot-pane
messages retain only the structured value; the view formats it for the existing
hover overlay. Preserve the current hover placement and typography in this
stage unless a cited interaction gate requires changing them.

### Resolution policy

Implement and test the exact order from PRODUCT-0133:

1. 0-D hit geometry beats 1-D, which beats 2-D;
2. later `paint_order` wins between layers; and
3. lower `sibling_ordinal` wins within one layer.

Dimensionality is geometry, not stroke width or visual area. Layered, faceted,
and grid composites forward provenance and do not invent a second policy.

Add fixtures for a point over a line, a thick line over a small rectangle, two
coincident rows in one scatter layer, two layers with opposing paint order, and
one hit in a facet cell. Assert the chosen hit identity, not merely the final
hover sentence.

### Acceptance gates

- All `PlotKernel` and `PlotComposite` implementations use the structured
  method; no production `Option<String>` hover contract remains.
- Existing hover presentation is derived from the hit at one host seam.
- The dimensional, paint-order, and sibling-order fixtures pass independently.
- Facet and layer provenance survive composite forwarding.
- A kernel returning no hit remains a valid total implementation.
- Existing rendering output is unchanged outside interaction state.

Commit: `impl: adr 0133 replace hover prose with structured hits`.

## Stage 3 — implement pane interaction and selection contexts

### Pane override viewport

Add session-scoped zoom and pan state to the existing pane-override layer. The
state overlays the resolved viewport through `Override<T>` and never changes
Rune source, authored specification, durable plot meaning, or relation
identity. It is cleared or replaced with the host plot state according to the
existing panel replacement lifecycle.

Implement wheel zoom, drag pan, and the gesture arbitration needed to keep
selection drag distinct from pan. The interaction record does not add a Rune
spelling or an implicit “promote viewport” action. Add tests proving that a
viewport gesture changes only the pane override and that a rerun does not
mutate the source buffer.

### Typed selection algebra

Add product-side selection types with no engine-specific payloads:

```text
SelectionIntent {
    input_ref,
    artifact_token,
    source_view,
    constraints,
    within_source: replace | add | subtract,
}
```

Constraints preserve nominal, ordinal, quantitative, and temporal semantics.
Temporal intervals retain their civil timezone frame. Implement the bounded
algebra:

- OR among values selected from one field;
- AND across fields and across different view sources;
- `replace`, `add`, and `subtract` only within the emitting source; and
- subtraction only for nominal and ordinal value sets.

Keep one source-local predicate per view. A view resolves against all context
predicates except its own and renders its own selection as emphasis. A facet
cell adds its facet-field equality constraint before the intent enters the
context.

### Context lifecycle

Key contexts by stable `input_ref`, never by the resolved transform chain.
Store the current `artifact_token` with the context. On publication retirement,
clear predicates before replacement visibility. On a stale arriving intent,
leave the context unchanged and set an ordinary non-error status message such
as “Selection expired because the source relation was refreshed; select
again.”

Tests must cover:

- two views over the same input with different sort/presentation chains;
- self-filter exclusion and source-view emphasis;
- two source predicates composing with AND;
- multiple values within one field composing with OR;
- temporal and multi-channel brush constraints retaining their types;
- facet provenance; and
- both stale-arrival and republication-clearing cases, including ordering
  before replacement visibility.

**Stage 3 stop condition.** If applying a predicate requires a cross-relation
  join, a row identity, a new engine protocol, or a different algebra for one
  adapter, stop and record the finding. The first implementation must operate
  through the existing materialized relation/reference seam.

Commit: `impl: adr 0133 add pane interaction and selection contexts`.

## Stage 4 — connect grid and export consumers

### Table selection

Make the existing table selection publish the contract only when its projection
is lossless:

- a single-column selection emits the selected typed values as one source-local
  predicate;
- a multi-column rectangle remains a valid clipboard selection but emits no
  relational intent; and
- mouse release reports the ordinary non-error status message explaining that
  filtering requires a single-column selection while Ctrl-C remains available.

Do not add row identity or silently convert a rectangle into a cross-product
predicate. Preserve shift-extend, drag-extend, Ctrl-C, Escape, visual selection,
and column-resize behavior. Add tests for single-column filtering, multi-column
refusal/disclosure, and unchanged TSV copying.

### Export reference

Thread the current filtered relation/reference into the existing export snapshot
seam. Export must resolve the current reference exactly as any other consumer;
it must not add an export-specific “include filters” flag or second selection
policy. A view's current filter context is applied before export snapshotting,
and the export path records the artifact token/reference it resolved.

This stage does not add a file chooser, naming policy, overwrite policy, or new
format. Those remain export-quality work outside PRODUCT-0133. If the current
export implementation cannot resolve a filtered reference without inventing
one of those policies, stop at the seam and report the dependency rather than
expanding this plan.

### Acceptance gates

- Single-column table selection emits typed values and filters compatible views.
- Multi-column selection never widens into a conjunctive cross-product.
- The refusal is visible through `status_msg`, not `status_error` or
  `StatusDetail`.
- Ctrl-C output is byte-equivalent before and after the consumer integration.
- Export resolves the current filtered reference with no separate filter flag or
  engine-specific branch.
- A replacement publication clears the filter before export can observe the
  replacement.

Commit: `impl: adr 0133 connect table and export consumers`.

## Cross-stage guards

- No Rune grammar, Rune surface, or authored specification changes.
- No `PlotKernel` implementation returns hover prose after Stage 2.
- No interaction logic branches on Toron, SQLite, Postgres, MySQL, BigQuery, or
  Parquet identity.
- `input_ref` and `ArtifactToken` remain separate in every producer and
  consumer.
- No relation context is keyed by a pane, panel, layer, buffer, or transformed
  expression alone.
- No source view filters itself; self-selection is emphasis only.
- No implicit re-emission creates a chain or arbitrary graph.
- No multi-column table rectangle becomes a predicate.
- No viewport gesture mutates Rune, durable plot state, or relation identity.
- No implementation changes the existing export format or path UX.
- Every negative guard has a synthetic violating control where a static source
  assay is insufficient.

## Evidence and review checklist

The evidence commit records the source census, gate ownership, and test names.
It must include at least:

- the complete hover implementation census and proof that every implementation
  migrated;
- publication-path coverage for named/unannotated SQL, Rune, and all adapters;
- token uniqueness, stable input identity, invalidation ordering, and stale
  intent tests;
- structured-hit dimensional, paint-order, sibling-order, facet, and no-hit
  tests;
- pane-override zoom/pan tests proving no authored round-trip;
- selection algebra tests for OR/AND, typed temporal values, subtract refusal,
  self-filter exclusion, and source composition;
- grid single-column, multi-column disclosure, and Ctrl-C parity tests;
- export current-reference tests; and
- focused Polariton tests, all workspace tests, formatting, feature-matrix
  checks, and a clean diff/status check.

The evidence record must state explicitly that IG-02 becomes decision-complete
through PRODUCT-0133 but remains unclosed until deferred-triage ratification
copies the closure reason into the ledger and owning records.
