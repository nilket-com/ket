# The Rune `figure([...])` surface

Scope: implement PRODUCT-0016 as one contextual figure root over the existing
single and layered plot bodies. Carry source-authored heading settings through
the existing PRODUCT-0007 cascade, refresh source-current host-panel chrome,
and install the resolved canvas heading atomically with the completed kernel.
Deliverable is four `impl:` commits followed by one evidence-only commit.

The implementation is judged by PRODUCT-0016 §10's twelve gates. Every gate is
assigned to exactly one stage below.

## Halt rule

PRODUCT-0016 §11 binds this work:

> This record may add the `figure` constructor, `title` and `subtitle`
> recognition, lowering into the authored `PlotSettingsLayer`, the chrome and
> canvas chains, the `ViewPresentation` field that carries an authored title,
> and descriptor refresh on a repeated `EnsureHostPanelGroup` request. If the
> implementation requires repairing the `Presence::Hide` round trip, evaluating
> computed heading text, changing any kernel, migrating either plot
> specification, or changing panel identity, placement, group state, or
> PRODUCT-0010's content lifecycle, the work stops.

Concretely, stop if implementation requires:

- changing `Presence`, `apply_presence`, `ResolvedPlotSettings`, or the fallback
  in `PlotWidget`; the existing `Hide -> None -> generated label` defect must
  remain exactly where PRODUCT-0016 cites it;
- changing `AuthoredPlotSpec`, `LayeredPlotSpec`, `LayerSpec`, plot composition,
  compatibility, scale/domain resolution, any mark renderer, or any kernel;
- evaluating a Rune expression for a heading or adding any heading value to the
  Rune execution result, Toron transport, or process protocol;
- adding a libaryon protocol command, event, projection shape, or second panel
  registry; the existing `EnsureHostPanelGroup` request already carries the
  descriptor this cut needs;
- changing `HostPanelId`, `ViewIdentity`, execution grouping, stamped-result
  sharing, panel placement, automatic/manual transitions, retained/detached
  membership, focus, tab membership, stale/orphan behavior, or dashboard state;
  or
- retiring or reinterpreting `plot.*`, adding sink methods, or admitting any
  figure child beyond the grammar PRODUCT-0016 accepts.

The one libaryon change is narrow: after an otherwise valid repeated
`EnsureHostPanelGroup`, copy the requested descriptor onto an already existing
member's backing `HostPanelBuffer`. Descriptor refresh must be observationally
inert with respect to layout and lifecycle.

### Where work may land

- `crates/libaryon/src/engine/session.rs` — existing host-panel descriptor
  refresh and its session/projection evidence;
- `crates/polariton/src/rune_relation_envelope.rs` — contextual figure grammar,
  plot-body/settings normal form, refusals, and `ViewPresentation`;
- `crates/polariton/src/gui/messages.rs` — carrying the authored settings beside
  single/layered completion data;
- `crates/polariton/src/gui/state.rs` — preserving the settings through result
  planning, asynchronous construction, success, and failure;
- `crates/polariton/src/editor.rs` — source-current chrome derivation and atomic
  host-plot replacement;
- `crates/polariton/src/gui/plot_pane/state.rs` and test-only use of
  `view.rs` — resolving authored settings for `HostPlotState` and observing the
  actual canvas heading;
- `crates/polariton/src/adapters/toron/mod.rs` — only match-arm and test updates
  forced by the Rune plot-intent normal form; adapter behavior is excluded;
- this plan, PRODUCT-0016's evidence file, and the Rune workspace corpus in the
  final evidence commit.

No file under `gui/dashboard/` is permitted. `plot/common.rs`, `plot/theme.rs`,
`plot/spec/mod.rs`, `plot/composition.rs`, `plot/factory.rs`, every plot kernel,
libaryon's protocol/runtime/renderer, Toron, and `toron-client` are excluded. A
required edit in one of those files is a halt, not cleanup.

## Stage 0: record the three feasibility findings

Stage 0 changes no code and discharges no gate. Record all three findings in the
evidence file; if implementation evidence contradicts one, stop before Stage 1.

### 0a. Existing descriptors can refresh without a protocol change

`HostPanelGroupMember` already carries `panel_id`, `kind`, and `title`.
`HostPanelBuffer` owns a mutable `HostPanelDescriptor`, and `BufferStore::get_mut`
plus `Buffer::host_panel_mut` reach it by the existing panel-to-buffer mapping.
The render projection reads that buffer-owned descriptor, so no event or new
projected field is needed.

Refresh only after the request has passed the existing producer and membership
conflict checks. Apply it to every requested member whose backing buffer already
exists, before the `missing.is_empty()` return. New members continue to receive
the same descriptor through `create_host_panel_view`; retained members omitted
from the current request retain their prior descriptor.

Both descriptor fields refresh. `kind` and `title` describe the current source
view, while panel identity is form-independent under PRODUCT-0010. Updating the
descriptor mutates no layout node, group state, panel revision, focus, or tab.

### 0b. Authored settings can ride beside both plot specifications

The existing Rune intent has one plot-body choice — single or layered — and the
existing plot settings model already has the authored layer. Represent those as
orthogonal fields rather than adding title fields to either specification:

```text
RunePlotIntent {
    body: Single(DisposableRunePlotSink) | Layered(LayeredPlotSpec),
    authored_settings: PlotSettingsLayer,
}
```

Exact internal enum names may follow the module, but the separation is
normative. Bare sinks construct `PlotSettingsLayer::default()`. A figure
constructs the identical body plus `Presence::Show` values in that layer.
`AuthoredPlotSpec` and `LayeredPlotSpec` therefore remain byte-for-byte
unmodified.

`PlotSettingsLayer` is `PartialEq`, not `Eq`, because other accepted settings
contain floating-point values. Removing an `Eq` derive from the containing
Rune-only intent is a type consequence, not permission to change equality
semantics or plot types.

### 0c. Host canvas settings can change atomically with the kernel

`HostPlotState` already owns a complete `ResolvedPlotSettings`, while
`PlotWidget` already draws resolved title/subtitle after kernel drawing.
Extend `HostPlotState::new` and `replace` to receive the authored layer and
resolve it through `PlotSettings`; do not teach the kernel or view about prose.

Replacement assigns the new display label, kernel, and resolved settings in one
synchronous state transition, then clears hover and advances the render
revision. `fail_or_stale` already leaves an existing `PanelContent::Plot`
untouched, so failure naturally retains the old kernel and its old canvas
heading together.

Factor resolution so an ordinary test can provide an application-theme layer.
Production continues to use exactly the current default application theme plus
the new authored layer. Do not add an application-theme producer or pane
override in this cut.

## Stage 1: refresh existing host-panel descriptors

Add one private session helper that updates the `HostPanelDescriptor` for an
existing `HostPanelId` and reports whether a backing host-panel buffer existed.
Call it for validated current members in both the existing-group and first-group
paths before either path decides that no view creation is necessary.

The refresh must not:

- allocate a buffer or view;
- increment `PanelRevision`;
- rebuild, tag, split, collapse, or inspect the layout tree;
- change current/retained/detached membership or Automatic/Manual state;
- activate a tab or move focus; or
- emit a host event.

### Engine assay

Create a group, record its panel ids, backing buffer ids, topology, ratios,
group state, focus, and active tabs, then repeat the same authoritative member
set with changed `kind` and `title`. Assert:

1. the projected descriptor changes for every current panel, including when no
   member is missing;
2. all recorded identities and layout observations are exact matches;
3. both Automatic and Manual groups remain in their prior state; and
4. an omitted retained member keeps its previous descriptor.

Also cover multiple views/tabs of one host-panel buffer: because the descriptor
is buffer-owned, every projection reference must observe the same refreshed
text without changing reference counts.

No PRODUCT-0016 gate is claimed yet. This is the engine half of gates 8 and 10,
and it remains unused by authored headings until Stage 3.

Commit: `impl: refresh existing host panel descriptors`.

## Stage 2: add the authored-settings carrier without changing behavior

Refactor the Rune plot intent into the Stage 0b body-plus-settings shape. Every
existing bare single or layered sink supplies `PlotSettingsLayer::default()`;
no `figure`, `title`, or `subtitle` spelling is recognized in this commit.

### Preserve `Presence` through the handoff

Carry the complete authored `PlotSettingsLayer` beside the body through:

```text
Rune plot intent
  -> spanned view intent / result planning
  -> single or layered sink output
  -> asynchronous completion message
  -> set_rune_view_plot
  -> HostPlotState::new or replace
  -> PlotSettings.authored_spec
  -> existing PlotSettings::resolve
```

The carrier may be a small plot-output struct shared by the single and layered
variants or a field on each plot variant. It must not be attached to table
output, converted into `Option<String>`, copied into a kernel, or embedded in
either plot specification.

Extend `ViewPresentation` with a source-authored-title projection that preserves
`Presence<Arc<String>>`. Table and every existing bare plot present
`Presence::Inherit`. `display_label` remains mandatory and unchanged.

### Host resolution seam

Resolve host settings from:

```text
library defaults
  -> current application theme
  -> carried authored settings
  -> unchanged layer settings
  -> unchanged pane override
```

For production bare sinks this must produce the same `ResolvedPlotSettings` as
before. Capture one single and one layered host-plot rendering before the
refactor or use the existing shared code path structurally; then assert the
post-refactor SVG is identical. Do not create a second renderer.

Use an ordinary test-only application layer with `title = Show("application")`.
Assert the resolved host state and rendered SVG show that title while
`ViewPresentation` remains `Inherit` and the prepared group descriptor remains
the generated structural label. This proves that theme text reaches canvas but
never enters the source-only chrome chain.

Structurally assert that:

- the carried field is `PlotSettingsLayer`/`Presence`, not an option;
- `apply_presence` remains the first and only existing flattening boundary;
- `AuthoredPlotSpec` and `LayeredPlotSpec` have no heading/settings field; and
- no message or completion type can pass prose to a kernel.

**Gates discharged: 7 and 11.**

Commit: `impl: carry authored settings to Rune host plots`.

## Stage 3: recognize figures and activate both label chains

The recognition switch and the first non-inherited authored settings must land
in this same commit. Do not commit a state in which `figure([...])` is accepted
but its heading is ignored, or one in which heading constructors affect existing
bare sinks.

### Contextual grammar

Recognize `figure(...)` only while parsing the sink of a proven `.views([...])`
element, directly or below the one permitted outer `view(name, sink)` wrapper.
Calls spelled `figure`, `title`, or `subtitle` anywhere else remain ordinary Rune
and enter no global shadow/name analysis.

`figure` takes exactly one literal list. Classify its children into named slots:

- `title(<one string literal>)` — zero or one;
- `subtitle(<one string literal>)` — zero or one; and
- one body — `bar`, `line`, `scatter`, or `layers` with exactly their already
  accepted body-position grammar.

Metadata order is ignored. Preserve order only inside `layers`. Parse the body
through the existing mark/layer helpers; do not recursively create a second
`ParsedSink`, identity, output root, or relation origin.

Refuse at the offending span:

- wrong outer arity or a non-literal figure list;
- duplicate title or duplicate subtitle;
- heading arity other than one or a non-string heading argument;
- no body or a second body;
- `table()` as the body;
- nested `figure`;
- `view` inside a figure; and
- `title` or `subtitle` in a layer list.

Any malformed child invalidates the entire outer sink. Assert no partial figure,
view, execution, dispatch, or panel artifact. Keep the outer `figure(...)` span
as the sink span for later result/build diagnostics; inner grammar errors retain
their own spans.

### Lowering and presentation

Build the same single or layered body as its bare spelling. Populate only:

```text
authored_settings.title    = Show(Arc<title>)    when present
authored_settings.subtitle = Show(Arc<subtitle>) when present
```

Every other authored setting remains `Inherit`. Omission is `Inherit`, and an
empty string is `Show("")`. No `Hide` value is produced.

Derive presentation without overwriting the structural label:

```text
ViewPresentation {
    content_kind: Plot,
    display_label: existing single label or layered title,
    authored_title: clone of authored_settings.title,
}
```

Chrome derivation in `prepare_rune_view_panels` is exact:

```text
Named(name)       -> name
Anonymous + Show  -> authored title + positional suffix
Anonymous + other -> display_label + positional suffix
```

Subtitle and resolved settings are unavailable to that branch. The resulting
member descriptor is sent in the ordinary pre-dispatch group request; Stage 1
makes repeated requests source-current without touching layout.

### Canvas observation

Drive both a single-body and layered figure through the real outcome and host
completion path. Render `PlotWidget` through `SvgBackend`; inspecting only
`ResolvedPlotSettings` does not discharge a canvas gate. Assert authored title
and subtitle appear, the generated title does not replace an authored title,
and the layered kernel still paints in its established order.

For `subtitle` without `title`, use an application layer with no title and assert
the generated structural label plus the authored subtitle in the rendered SVG.
Keep the separate Stage 2 control proving that an application-theme title wins
when one exists.

### Chrome observation

Project actual host-panel descriptors, not only the outgoing command. Assert:

- named+titled uses the name;
- anonymous+titled uses title plus position;
- anonymous+untitled uses structural label plus position;
- subtitle text never occurs in a descriptor; and
- two anonymous `Revenue` figures remain disambiguated by their existing
  positional suffixes.

`figure([line(x,y)])` and bare `line(x,y)` in the same named or anonymous slot
must resolve one identity and `HostPanelId`, with equivalent body, generated
label, inherited settings, and SVG.

**Gates discharged: 1, 2, 3, 4, 5, and 6.**

Commit: `impl: add the contextual Rune figure surface`.

## Stage 4: prove source-current chrome, canvas atomicity, and regression

### Success, failure, and removal assay

Use one already completed plot panel and drive three analyzed revisions through
the real editor/update path:

1. complete a figure with title A, subtitle A, and kernel A;
2. successfully analyze title B but fail its origin or plot construction; and
3. successfully analyze the same body with title/subtitle omitted and complete
   kernel C.

After step 1, record view identity, `HostPanelId`, backing buffer id, group id,
layout state, topology, ratios, geometry, focus, active tabs, content object
status, and the rendered canvas heading.

After step 2, assert chrome is source-current B, the panel is stale, and the
stored/rendered canvas still pairs heading A, subtitle A, and kernel A. No
completion path may install settings before its kernel or mutate only one of
them.

After step 3, assert chrome has returned to the generated structural label,
canvas settings are inherited so `PlotWidget` renders that same fallback, and
kernel C is installed. Every recorded identity/layout observation remains
unchanged; descriptor refresh did not enter Manual, retile, focus, or activate a
tab.

Repeat the title-change/removal portion for a named view. Its chrome must remain
the name in every revision while its canvas headings follow successful
completion.

### Existing lifecycle and surface regression

Run every existing envelope, multi-origin, identity, placement/group,
single/layered sink, stale/orphan, and dashboard test. In particular:

- origin execution count and shared-`Arc<DataFrame>` identity remain exact;
- changing between bare, figure, single, and layered forms never changes named
  or positional identity;
- descriptor refresh changes only requested `kind`/`title`, including the
  generated-label correction for an existing anonymous panel;
- failure without prior complete content remains the existing Failed behavior;
- failure with complete content remains Stale and preserves that content;
- no query completion emits `EnsureHostPanelGroup` or any placement request;
- table content, independent panels, result panels, `Split`, `TabWith`, manual
  groups, reset, retained/detached members, and producer release are unchanged;
- the dashboard remains unopened and empty for every Rune figure; and
- legacy `plot.*`, relation sugars, bare constructors, and `layers` retain their
  exact accepted behavior.

Gate 12 permits only PRODUCT-0016's explicit descriptor correction and heading
surface. No pre-existing sink gains a setting, kernel path, identity, placement,
or lifecycle change.

**Gates discharged: 8, 9, 10, and 12.**

Commit: `impl: prove Rune figure heading lifecycle`.

## Final evidence-only commit

Write `plans/product/impl/0016_the_figure_surface_evidence.md` and update the Rune
workspace corpus with the accepted figure spelling and label/lifecycle rules.
Map all twelve gates to behavioral or structural evidence, record the three
Stage 0 findings and exact four implementation commits, and state whether the
halt fired.

Record `Presence::Hide` as a known unresolved defect, not an accepted capability
or a failed PRODUCT-0016 gate. Advance the corpus only after the record is
accepted, all automated evidence is green, and an installed Rune buffer visibly
shows authored title/subtitle, source-current chrome, and no dashboard.

The evidence commit contains no Rust changes.

Commit: `evidence: close the Rune figure surface`.

## Gate assignment ledger

| Gate | Stage | Proof burden |
| --- | --- | --- |
| 1 | Stage 3 | layered body plus independent authored settings; no kernel prose |
| 2 | Stage 3 | bare/one-body figure equivalence and same destination |
| 3 | Stage 3 | complete bounded grammar refusal matrix with exact spans |
| 4 | Stage 3 | subtitle-only figure with cascade fallback canvas title |
| 5 | Stage 3 | three-case chrome chain plus independent rendered canvas |
| 6 | Stage 3 | subtitle absent from projected chrome |
| 7 | Stage 2 | application-theme canvas title excluded from source chrome |
| 8 | Stage 4 | title changes preserve identity and full group geometry/state |
| 9 | Stage 4 | title removal restores both fallbacks without replacement |
| 10 | Stage 4 | source-current chrome plus atomic success/stale canvas assay |
| 11 | Stage 2 | Presence carrier, no new Option flattening, specs unmigrated |
| 12 | Stage 4 | complete sink, execution, placement, lifecycle, dashboard regression |

Every gate appears exactly once. Stage 0 is feasibility evidence and Stage 1 is
the independently tested engine prerequisite; neither claims a complete product
gate.

## Verification

Before every implementation commit:

```text
cargo fmt --all -- --check
cargo test -p libaryon
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
git diff --check
```

Also inspect `git diff --name-only` against the permitted-file list and confirm:

- `apply_presence`, `ResolvedPlotSettings`, and `PlotWidget` are unchanged;
- both plot specifications, plot composition, factory, and every kernel are
  unchanged;
- no protocol/event/projection type and no Toron/transport file changed;
- no dashboard file changed;
- no completion handler emits group placement; and
- the only libaryon production edit is descriptor refresh inside the existing
  group request.

## Review checklist

- [ ] Stage 0 records descriptor, settings-carrier, and host-state feasibility.
- [ ] Existing group requests refresh buffer-owned `kind` and `title` before the
      no-missing return and touch no layout/lifecycle state.
- [ ] Bare sinks carry an all-Inherit authored layer and render identically.
- [ ] `Presence` survives intact until the pre-existing `apply_presence` defect.
- [ ] `figure` is meaningful only as a proven view sink, directly or inside one
      outer `view` wrapper.
- [ ] Figure metadata is unordered; only `layers` carries order.
- [ ] Every bounded-tree refusal is atomic and attributed to its own span.
- [ ] Exactly one existing single/layered body is built; neither specification
      gains settings or prose.
- [ ] Title/subtitle populate only the authored settings layer, and omission is
      `Inherit` rather than an option or empty string.
- [ ] Structural display label, source-authored title, chrome label, and resolved
      canvas title remain four separately testable values.
- [ ] Name wins chrome; subtitle and theme/pane titles never enter chrome.
- [ ] Existing panels retitle from successful analysis without replacement,
      focus, reflow, or Manual transition.
- [ ] Canvas heading and kernel replace together; failure retains both stale.
- [ ] Removing title/subtitle restores inherited/generated behavior rather than
      retaining the prior successful strings.
- [ ] Theme title reaches the canvas only through PRODUCT-0007's cascade.
- [ ] `Hide` remains broken only at its two cited existing lines and gains no
      Rune spelling.
- [ ] No computed heading evaluation or value transport exists.
- [ ] Identity, execution, shared results, placement/groups, stale/orphan,
      dashboard, and all existing sink behavior remain green.
- [ ] Corpus and gate ledger land only in the evidence commit.
