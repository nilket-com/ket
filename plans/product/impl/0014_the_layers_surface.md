# The Rune `layers([...])` surface

Scope: implement PRODUCT-0014 as the production caller for PRODUCT-0013. Add
contextual recognition of `layers([...])`, lower it exactly to
`LayeredPlotSpec`, hand compatibility and rendering to PRODUCT-0013, and complete
the resulting kernel in an ordinary host panel through the label-only
presentation seam. Deliverable is four `impl:` commits, one per stage, followed
by one evidence-only commit that updates the corpus.

The implementation is judged by PRODUCT-0014 §9's thirteen gates. Every gate is
assigned to exactly one stage below.

## Halt rule

PRODUCT-0014 §10 binds this work:

> This record may add the `layers` constructor, its contextual recognition and
> refusals, lowering into `LayeredPlotSpec`, and the routing/label separation in
> §7. If the implementation requires changing PRODUCT-0013's model,
> compatibility rules, domain resolution, renderer behavior, or bridge — or
> requires a new mark, a transport change, or a change to panel identity,
> placement, or staleness — the work stops and the requirement returns as its
> own record.

Concretely, stop if the surface needs any change to:

- `LayeredPlotSpec`, `LayerSpec`, `LayerMark`, `NonEmpty`, layered channel/domain
  resolution, or PRODUCT-0013's error rules;
- `LayeredPlotKernel`, either mark renderer, paint order, shared-domain behavior,
  legend delegation, or hit ranking;
- `AuthoredPlotSpec`, `PlotComposition`, Toron, `toron-client`, libaryon, or any
  transport protocol;
- panel identity allocation, placement, refresh, stale/orphan handling, or the
  dashboard.

The one exception is PRODUCT-0014 §7's presentation seam: Rune host-panel plots
may stop carrying `PlotComposition` and instead carry a content kind, display
label, and kernel. That exception must not alter dashboard behavior. If the
dashboard needs an edit, stop.

### Where work may land

- `crates/polariton/src/rune_relation_envelope.rs` — contextual grammar,
  `LayeredPlotSpec` lowering, and `ViewPresentation`;
- `crates/polariton/src/gui/messages.rs` — layered sink/build outcomes and the
  presentation descriptor carried through completion;
- `crates/polariton/src/gui/state.rs` — PRODUCT-0013 handoff, asynchronous kernel
  construction, span attribution, and ready/error handling;
- `crates/polariton/src/editor.rs` — content-kind routing and the host-panel plot
  completion signature;
- `crates/polariton/src/gui/plot_pane/state.rs`, `view.rs`, and `mod.rs` — the
  label-plus-kernel host plot state authorized by §7;
- `crates/polariton/src/plot/factory.rs` — only removal of the test-only/dead-code
  annotation when `create_layered_plot` acquires its production caller;
- `crates/polariton/src/adapters/toron/mod.rs` — test-only match-arm citation
  updates forced by the `RunePlotIntent` split; adapter production behavior is
  excluded;
- this plan, the PRODUCT-0014 evidence file, and the Rune acceptance corpus.

No file under `gui/dashboard/` is permitted. `plot/composition.rs`,
`plot/spec/mod.rs`, and every plot kernel are also excluded. Any required edit
there is a halt rather than an implementation detail.

## Pre-implementation finding: the presentation seam is locally feasible

The §7 seam does not require a dashboard change:

- `PanelContent::Plot` is Polariton's host-panel backing object in `editor.rs`;
  the dashboard owns its plot states separately.
- `plot_pane::host_view` is already a distinct host-only rendering entrypoint and
  emits only `PlotMessage`, never `DashboardMsg`.
- The host surface consumes only a kernel, label, resolved settings, hover state,
  and render revision. It does not need `PlotComposition`'s layer mutation or
  rebuild policy.

Implement a small `HostPlotState` beside, not inside, `PlotPaneState`:

```text
HostPlotState {
    display_label,
    kernel,
    plot_settings,
    hovered_info,
    render_revision,
    resize_render_suspended,
}
```

`PlotPaneState` remains the dashboard/composition-owning type. Shared default
settings may be extracted into a private helper used by both states, but its
resolved value must remain identical. The host state handles the messages the
host view can observably produce; it does not acquire layer editing or rebuild
behavior.

This is the branch the implementation must take. Supplying a dummy, first-mark,
or directly constructed `PlotComposition` to satisfy the existing host type is
forbidden: it would fail gate 12 while making the code appear to work.

## Stage 1: contextual grammar, lowering, and presentation normal form

### Normal form

Replace `RuneViewForm`'s combined routing/description job with an explicit
presentation value:

```text
ViewContentKind = Table | Plot

ViewPresentation {
    content_kind:  ViewContentKind,
    display_label: String,
}

RuneViewIntent =
    Table
  | Plot(SinglePlotIntent | LayeredPlotSpec)
```

The exact internal enum names may follow the surrounding module, but the
separation is normative:

- panel routing consults only `content_kind`;
- diagnostics and anonymous panel titles consult only `display_label`;
- identity remains `LocalViewIdentity` and consults neither;
- no `HostPanelId` enters the envelope.

Preserve the established labels exactly: `table`, `plot.bar`, `plot.line`, and
`plot.scatter`. A layered intent uses `LayeredPlotSpec::title()`; `[line,
scatter]` therefore yields exactly `Line + Scatter`.

### Parser boundary

Extend `parse_contextual_sink_inner` only after the expression is proven to be a
`.views([...])` element. `layers` must not enter whole-buffer shadow analysis or
global name reservation.

`parse_layer_list` enforces only PRODUCT-0014 grammar:

- one argument, and it is a literal list;
- at least one element;
- every element is exactly `line(x, y)` or `scatter(x, y)` with two string
  literals;
- `layers` nested inside the list refuses at the inner call span;
- `table`, `bar`, unknown calls, malformed mark arity, and non-string fields
  refuse at their own spans;
- `scatter(x, y, color)` refuses as grammar in layer position while retaining
  its established legality in view position.

Build the first valid mark with `LayeredPlotSpec::new(LayerSpec::xy(...))`, then
append the remainder with `push`. Do not build an intermediate possibly-empty
`Vec`, call PRODUCT-0013 resolution, inspect a schema, or mention scales.

Recognition remains atomic: any malformed member returns no `ParsedSink`, no
output root, and no dispatch. A syntactically identical `layers(...)` call
outside a proven view list remains ordinary retained Rune in statements,
bindings, and unrelated call arguments.

**Gates discharged: 1, 3, 4, 5, 7, and 8.** Gate 1 asserts both source orders in
the lowered `LayeredPlotSpec`. Gate 5 must assert the legal view-position control
in the same test as the layer-position refusal. Gate 8 observes no partial
envelope or dispatch artifact.

Commit: `impl: add the contextual Rune layers grammar`.

## Stage 2: PRODUCT-0013 handoff and attributed failure

### Keep the layered request distinct

Do not manufacture `AuthoredPlotSpec`, `ResolvedPlot`, or `PlotComposition` for a
layered sink. Extend the sink-output pipeline with a distinct layered request:

```text
RuneRelationSinkOutput =
    Table
  | Plot(ResolvedPlot)
  | LayeredPlot(LayeredPlotSpec)
```

The containing `RuneRelationSinkOutcome::source` remains the same `Arc<DataFrame>`
as the view-set result. `rune_view_outcome` performs no compatibility check for
the layered variant; it merely preserves the lowered specification, outer sink
span, presentation, identity, and shared frame.

Add a non-blocking layered build task in `gui/state.rs`. It moves the authored
`LayeredPlotSpec` and shared frame into the existing blocking pool and invokes
`create_layered_plot` exactly once. That factory call is the sole compatibility,
domain, preparation, and kernel-construction boundary. Remove its
`#[allow(dead_code)]` annotation now that this task is the first production
caller; do not otherwise edit the factory.

The completion message carries:

- source buffer and revision;
- destination panel id;
- `ViewPresentation`;
- the outer sink span;
- `Result<Arc<dyn PlotKernel>, String>`.

On PRODUCT-0013 failure, the update branch checks the current revision, marks
only that destination failed/stale through the existing lifecycle method, and
formats the error with the outer sink byte range. The PRODUCT-0013 message must
remain intact so it still names the offending layer and both incompatible field
identities. Do not parse or reinterpret that message in the envelope or head.

### Compatibility-handoff assay

Use a syntactically valid two-layer surface whose second layer names a different
x field. Assert, in order:

1. envelope transformation succeeds and returns a `LayeredPlotSpec`;
2. no envelope error variant represents field or scale incompatibility;
3. PRODUCT-0013 construction refuses and names layer 2 plus expected/actual
   fields;
4. the user-facing head diagnostic attributes the unchanged refusal to the
   outer `layers(...)` sink span;
5. no kernel or partial plot content is installed.

This test must cross the real handoff. Calling `resolve_layered_channels`
directly proves PRODUCT-0013 again and does not discharge PRODUCT-0014 gate 6.

**Gate discharged: 6.**

Commit: `impl: hand Rune layers to the layered plot factory`.

## Stage 3: label-only host plot presentation

### Host state

Add and export `HostPlotState` in `gui::plot_pane`. It owns no
`PlotComposition`. Its constructor and replacement method take only:

```text
display_label: String
kernel: Arc<dyn PlotKernel + Send + Sync>
```

`host_view` accepts `HostPlotState` and passes its label directly to both plot
render layers. `PanelContent::Plot` stores `HostPlotState`. Refreshing a Rune
plot replaces label and kernel, clears hover, increments render revision, and
preserves the existing panel status transition. Table, failed, stale, and
orphaned content behavior remains unchanged.

The ordinary single-plot ready path must use the same seam: carry its existing
`ViewPresentation` through `RuneRelationPlotReady` and complete the host panel
with `display_label + kernel`. `ResolvedPlot` remains available for status and
kernel construction, but its `PlotComposition` never enters the host-panel
completion call. The layered ready path uses the identical completion method.

`prepare_rune_view_panels` routes `Table` versus `Plot` solely through
`ViewContentKind`, and derives an anonymous first-appearance title solely from
`display_label`. Named views retain their names as panel titles. Existing panels
continue to receive only `RefreshHostPanel`; this record does not add retitling
or relocation behavior.

### Dashboard quarantine

- `PlotPaneState`, its composition fields, and dashboard layer-editing behavior
  remain unchanged.
- No file under `gui/dashboard/` changes.
- `PlotComposition` is absent from `HostPlotState`, `set_rune_view_plot`, and the
  layered completion message.
- Existing dashboard tests and existing ordinary Rune plot rendering remain
  green.

Gate 12 is both behavioral and structural: the real update path completes a
layered kernel into one `PanelContent::Plot`, dashboard state remains empty, and
the types on that path cannot carry a `PlotComposition`. A grep alone is not the
gate, but the type boundary is the negative proof that the runtime assertion
cannot observe.

Gate 13 checks the exact `Line + Scatter` label through the host surface and all
four established single-form labels. It must distinguish the plot canvas label
from the host-panel chrome title: known panel identities are refreshed, not
retitled, exactly as PRODUCT-0010/0011 leave them.

**Gates discharged: 12 and 13.**

Commit: `impl: complete Rune plots through label-only host state`.

## Stage 4: identity, one-artifact behavior, and regression

### One sink remains one view

Drive a valid `[line, scatter]` layered sink through envelope analysis, panel
preparation, shared-result outcome, PRODUCT-0013 construction, and host
completion. Observe one presentation entry, one `LocalViewIdentity`, one
`HostPanelId`, and one plot content object. No per-layer identity or destination
type may exist.

### Lifecycle and one-layer equivalence

- Run `view("n", layers([line, scatter]))`, record its panel id, reorder or
  remove a layer, rerun, and observe the same id and destination with refreshed
  content.
- For an anonymous positional slot, run bare `line(x, y)`, replace it with
  `layers([line(x, y)])`, and observe the same positional identity and panel id.
- Render the two resulting kernels through `SvgBackend` under the same bounds
  and assert byte-identical SVG. Reuse PRODUCT-0013's existing one-layer factory
  path; do not add a surface-specific renderer comparison.

### Regression anchor

Run every existing envelope, multi-origin, identity, placement, diagnostic, and
dashboard test. Type-oriented tests that named `RuneViewForm` may be updated to
assert `ViewPresentation`; behavioral expectations and cited executable corpus
evidence may not change. Existing free-function, sugar, bare view constructors,
names, ordering, shared-`Arc`, sink failure isolation, stale/orphan behavior,
panel placement, and dashboard non-routing all remain intact.

**Gates discharged: 2, 9, 10, and 11.**

Commit: `impl: prove layered view identity and lifecycle`.

## Final evidence-only commit

Write `plans/product/impl/0014_the_layers_surface_evidence.md` and update the Rune
workspace corpus. Map all thirteen gates to behavioral or structural evidence,
record the exact implementation commits, and state whether any type-oriented
test citations changed under §9a. Advance the layered sink row only when the
record has been accepted and the implementation evidence is landed.

The evidence commit contains no Rust changes.

Commit: `evidence: close the Rune layers surface`.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
```

Before every implementation commit:

- run the stage's targeted gates;
- run `git diff --check`;
- inspect `git diff --name-only` against the permitted-file list;
- confirm `plot/composition.rs`, `plot/spec/mod.rs`, every kernel, Toron,
  libaryon, and `gui/dashboard/` are untouched.

## Review checklist

- [ ] `layers` is meaningful only in a proven `.views([...])` element.
- [ ] The parser constructs `LayeredPlotSpec` directly in authored order and
      never calls PRODUCT-0013 resolution.
- [ ] Empty and nested layers refuse for the specified structural/grammar
      reasons and at the specified spans.
- [ ] Three-argument scatter is legal as a view and illegal as a layer in one
      paired test.
- [ ] Ordinary Rune outside `.views` retains every `layers(...)` call.
- [ ] Malformation is atomic: no partial sink, dispatch, or panel exists.
- [ ] Differing fields cross the envelope and fail only at PRODUCT-0013.
- [ ] Every layered sink holds the execution's exact shared `Arc<DataFrame>`.
- [ ] `ViewPresentation` separates content routing from display description and
      carries no identity or host address.
- [ ] A Rune host plot completes from label plus kernel without constructing a
      `PlotComposition`.
- [ ] `HostPlotState` owns no composition and the dashboard retains
      `PlotPaneState` unchanged.
- [ ] One layered sink produces one identity, panel, and content object.
- [ ] Named and positional identity survive layer edits and bare/one-layer
      respelling.
- [ ] One-layer SVG remains byte-identical through the production surface.
- [ ] `Line + Scatter` is exact and all existing labels remain exact.
- [ ] No PRODUCT-0013, execution, transport, panel lifecycle, placement,
      dashboard, guide, or interaction behavior changes.
