# The categorical palette figure surface

Scope: implement PRODUCT-0030 only. Deliverable is one combined ADR/plan commit,
one pre-switch compatibility-oracle commit, one indivisible production-and-tests
commit, one evidence commit, and one documentation-only closure commit that pins
the evidence commit's hash. The implementation commit must introduce
`palette(name)`, select explicit `viridis` on absence, switch Rune plots to
PRODUCT-0029's palette-aware resolver, and commit returned continuation with
visible content together. No production or test cleanup commit follows it.

## Halt rule

Stop on any libaryon theme, palette, name, modifier, optional-capability, or
natural-length change; PRODUCT-0029 allocation or rendering policy change;
`AuthoredPlotSpec` or `LayeredPlotSpec` migration; inferred Color channel;
perceptual palette validation or substitution; cycling at capacity; guide or
plot-chrome change; non-categorical job change; broad `ColorTheme` migration;
Toron source, execution, adapter transport, or stamped-result identity change;
host-panel identity, placement, dashboard, table, or workspace configuration
change; durable continuation; or a need to split surface recognition, the Rune
default, the stateful caller, and successful-content commit across production
commits, flags, dead grammar, or cleanup.

## Stage 0: feasibility and historical-proof findings

### 0a. Figure parsing has one bounded metadata seam

`parse_figure` in `rune_relation_envelope.rs` already flattens one literal body,
`title`, and `subtitle` into `RunePlotIntent { body, authored_settings }`.
Add one duplicate-checked literal `palette` member there and lower it to one
`Override<Arc<String>>` figure-wide settings property. No mark parser, modifier
register, relation sugar, `AuthoredPlotSpec`, or `LayeredPlotSpec` needs a new
field.

Recognition remains contextual. `palette` is special only as a direct member of
a proven figure sink. The existing call parser can distinguish it from a figure
body, a nested layer member, a mark postfix, and ordinary Rune outside the
envelope without reserving the name globally.

If accepting metadata requires preserving its source order, attaching it to a
mark, or changing the four-level PRODUCT-0016 tree, halt. The expected branch is
one unordered figure member and no new nesting level.

### 0b. Palette selection is one pre-domain settings projection

Add a categorical-palette identity property to `PlotSettingsLayer`, defaulting
to `Override::Inherit`. Resolve it through a dedicated settings method in the
accepted precedence order, with the graphical-layer tier inapplicable and
therefore inherited:

```text
library "viridis" -> application -> authored -> pane override
```

This method returns an identity string; it does not resolve libaryon, colors, or
positions. The Rune head then calls
`CategoricalPaletteSelection::from_theme_name` once. Existing renderer
`PlotSettings::resolve()` continues returning the exact `ResolvedPlotSettings`
it returns today; categorical palette identity does not enter
`ResolvedPlotSettings.color_theme` or `PlotThemeSpec` in this cut.

The current application plot layer sets `theme = PlotThemeSpec::default()` and
does not set the new independent property. The library value is therefore the
only absent-source producer and equals that theme's existing categorical
`ColorTheme::Viridis` choice. If honoring the new property requires translating
every `ColorTheme`, editing the plot-pane chooser, or changing renderer settings,
halt for the broader ramp-migration record.

### 0c. A default-free stateful request can coexist with `plan`

Keep `plot::request::plan` byte-compatible for plain Rune fallback, Markdown,
durable, and non-Rune callers. Add a second explicit request function that takes:

```text
PlotRequest
CategoricalPaletteSelection
ColorIdentity snapshot
```

and returns:

```text
ResolvedPlot
proposed next ColorIdentity
```

It reuses `resolve_channels`,
`resolve_domains_with_categorical_palette`, and the existing named-channel
projection. It owns no default and contains no Rune syntax. Unknown identity and
missing categorical capability are resolved before calling it and retain
PRODUCT-0029's distinct errors.

The legacy request function and PRODUCT-0029 snapshot remain green after Rune
switches callers. A shared helper may remove duplicated request plumbing only if
the old function's resolved spec, projected frame, error strings, and caller
surface remain exact. Otherwise keep the small duplication; compatibility is
more valuable than premature request unification.

### 0d. Prior continuation can be snapshotted before independent dispatch

The head owns a session-local bank:

```text
(BufferId, LocalViewIdentity, palette_len) -> ColorIdentity
```

Before `start_rune_relation_task` moves work into independent origin tasks, it
can resolve each plot view's effective palette identity, obtain its natural
length, clone the matching bank entry or an empty identity, and attach that
closed context to the destination work. An invalid name or missing capability is
retained as that destination's prepared failure; it does not cancel the shared
query or prevent sibling sinks from completing.

Each origin task receives only snapshots for its own ordered views. It never
borrows `AppState`, mutates the bank, or shares a mutable identity with another
origin. `rune_view_outcome` calls the explicit request function for single and
styled-single plots and returns the proposed identity beside the resolved plot.
Layered plots validate the authored palette but have no categorical consumer or
proposal because PRODUCT-0013 keeps their Color channels absent.

If the same `LocalViewIdentity` can occur twice in one buffer or one origin task
needs mutable access to another destination's bank, halt: that would contradict
PRODUCT-0010's buffer-unique identity and independent-destination model.

### 0e. The existing ready message is the content-commit seam

`RunRuneViewSetResult` schedules kernel construction but does not install plot
content. `Message::RuneRelationPlotReady` first rejects a superseded revision and
then calls `set_rune_view_plot` (`gui/state.rs:1698`). Extend only this internal
message with the destination identity, natural length, and proposed
`ColorIdentity`. For a current revision:

1. install the kernel and authored settings in the existing panel;
2. only after installation succeeds, replace the matching bank entry; and
3. publish the ordinary success status.

The current panel setter is infallible once a current prepared panel exists. A
single-plot kernel build produces no ready message if its task panics, and the
styled path already carries build failure separately. Resolution, capacity, and
selection failures produce no proposal. Superseded ready messages return before
both content and continuation. Thus no failed or invisible attempt can reserve a
position.

Tests must drive real PRODUCT-0029 domain resolutions and the real update
messages. Calling the bank's `insert`, the maximin selector, or a transaction
helper directly does not prove continuation-commits-with-content.

### 0f. Actual producer release is not projected to Polariton

Libaryon's `release_buffer` observes producer release internally and marks
engine-owned host-panel groups producerless (`engine/runtime.rs:1994`). It emits
`PanelDisposed` only when the released buffer itself is a host panel. Polariton
receives no event for an ordinary text producer and cannot eagerly collect bank
entries without a new protocol capability.

`EditorSession` allocates BufferIds monotonically (`engine/session.rs:943`). An
actually reopened file therefore receives a new key and cannot observe an old
bank even though its unreachable entry remains allocated. PRODUCT-0030 takes
this branch: bank memory is retained until process teardown. Do not infer release
from visible panes; jump history may keep a buffer alive after its last visible
view, exactly as PRODUCT-0015 established.

Adding a producer-release event, scanning only visible panes, or tying bank
lifetime to `PanelDisposed` triggers the halt. The first changes protocol; the
other two destroy live identity on ordinary editor actions.

### 0g. The indivisible property is historical

Let the combined ADR/plan commit be `P` and the pre-switch oracle commit be `O`.
Before implementation, record both exact hashes and verify that `O` contains no
non-test production behavior change and that its tree has all four pre-switch
facts:

```text
figure parser:      no palette member
Rune default:       no libaryon categorical selection
Rune request:       legacy plan with fresh ColorIdentity
ready completion:   no continuation proposal or bank commit
```

The implementation commit `I` must have `I^ == O`. Evidence inspects:

```text
git rev-parse I^
git diff --name-status I^ I
git diff --unified=100 I^ I -- \
  crates/polariton/src/rune_relation_envelope.rs \
  crates/polariton/src/plot/theme.rs \
  crates/polariton/src/plot/request.rs \
  crates/polariton/src/gui/messages.rs \
  crates/polariton/src/gui/state.rs
```

That one diff must contain syntax recognition, explicit `viridis`, the
palette-aware request, the bank snapshot/proposal path, and commit after visible
content. No half may exist in `O`, a flag, a dormant production branch, or a
follow-up code commit.

As in PRODUCT-0027, the evidence commit cannot contain its own hash. Use:

```text
E = documentation-only implementation evidence commit
C = documentation-only closure commit that records E and accepts the record
```

After `E`, record its exact hash and evaluate immutable range `I..E`. The range
may contain only the evidence document. Commit `C` records `E`, the command and
result, accepts the ADR, advances the corpus, and explains in its commit message:

```text
Records I..E because E cannot contain its own hash.
```

Squashing `C` into `E` destroys the recorded historical proof and is not history
cleanup.

## Stage 1: bank pre-switch compatibility evidence

Add a checked-in textual oracle through the existing public legacy
`plot::request::plan`, legacy resolver continuation assays, and current renderer
path, with no production-source behavior change. Colocated `#[cfg(test)]`
coverage is permitted. One line per case records:

- categorical scatter category -> exact legacy color for admitted `n`;
- filtering and reappearance through the existing compatibility identity;
- the thirteen-category refusal;
- a two-field bar's ordered single-treatment SVG fill operations; and
- the exact projected frame fields and request errors.

The oracle deliberately exercises the non-Rune compatibility entrypoint that
must remain after the switch. It is not an expected-value oracle for the new Rune
default. Keep the PRODUCT-0029 legacy snapshot unchanged; reuse it if it already
contains every observation rather than checking in a redundant copy. If new
coverage is needed, add a PRODUCT-0030 pre-switch snapshot beside it.

Record `P` and `O` immediately after this commit and perform Stage 0g's pre-state
inspection before touching production source.

This stage discharges no PRODUCT-0030 gate. Stage 2 reruns it unchanged as the
non-Rune regression half of gate 18.

Commit: `test: bank the Rune categorical caller boundary`.

## Stage 2: land the entire surface, caller, and continuation atomically

This is the only production-and-tests commit. Work may be organized locally in
the following sections, but none may be committed, flagged, or left dormant
separately.

### 2a. Add the figure member and bounded carrier

Extend `PlotSettingsLayer` with the categorical palette identity override and
add its dedicated identity resolver without changing renderer resolution. Extend
`parse_figure` with literal, duplicate-free `palette`. Flatten metadata to one
normal-form override and add contextual recognition/refusal assays for direct
and named views, metadata permutations, wrong arity, computed identity,
duplicates, nested/layer/body positions, mark methods, relation sugar, and
ordinary Rune outside proven positions.

Add the three attachment points and rule of thumb to type-oriented corpus/test
language where useful, but do not update the product corpus until closure.

### 2b. Add the explicit request and session bank

Add the default-free stateful request from Stage 0c. Add the AppState bank from
Stage 0d plus small helpers that only snapshot and commit by full key. Prepare
each destination's resolved selection or factual failure before dispatch without
skipping the shared query. Carry explicit selection and prior identity into the
origin task; return resolved plots with proposed next identities.

Do not put `ColorIdentity` into the Rune normal form, `AuthoredPlotSpec`,
`LayeredPlotSpec`, `ResolvedPlotSpec`, kernel, Toron source, database adapter, or
host-panel registry. The internal GUI outcome/message path is permitted because
it is exactly the asynchronous head-owned completion boundary, not external
transport.

### 2c. Switch the Rune caller and default in the same diff

Resolve omitted figure palette to exact `viridis`, authored names to their exact
libaryon identity, and every Rune single/styled-single plot through the explicit
PRODUCT-0029 request. Remove the fresh `ColorIdentity::default()` legacy plan
from the Rune relation-view path. Keep plain Rune fallback, Markdown, durable,
and all non-Rune callers on existing behavior.

An explicit palette on line, absent-color scatter, or accepted layered content
validates successfully and has no visual consumer. Unknown identity and known
absence fail that destination factually even when its body would not consume
categorical colors. Never infer a Color channel merely to make a valid setting
visible.

### 2d. Commit proposals only with current visible content

Extend single and styled Rune sink outputs and the ready message with the
destination-local proposal. Preserve `Arc::ptr_eq` for all sinks over an origin.
On ready, reject old revisions first, install content second, and commit the bank
third. Resolution failure, capacity failure, styled build failure, task panic,
superseded result, and sibling failure leave the bank untouched.

Drive a real sequence of successful results through the full task/update path:
`A->0`, then `B->19`, then `C->9`; filter and restore a category; introduce a
failed capacity attempt; and verify the next admissible run observes only
positions that became visible. A selector-only or bank-only test is
disqualified.

Drive same-length retargeting through exact mappings for
`tab20c -> tab20c_r -> tab20c_s1 -> viridis`; categories retain positions and
consume each selected sequence's exact colors. Drive `dark2` as an independent
eight-position bank, return to the prior twenty-position assignments, then
return to the retained eight-position assignments. Include two views and two
origins completed in opposite orders.

### 2e. Prove sovereignty, diagnostics, and unchanged boundaries

Add exact mapping/render assays for:

- absent-source six-category `viridis` bar band fills with no Color channel;
- authored `tab20c` six-category band fills;
- `_r` and `_s1`, with `_s1` proving libaryon delegation;
- authored categorical scatter using ordinary channel assignments and no band
  fills;
- line, absent-color scatter, and layers accepting an unused palette without
  gaining repeated colors;
- a declared but visually weak palette rendering exact colors with no scoring,
  warning, refusal, substitution, or shift;
- unknown identity, `nord`, `dark2` palette capacity, geometry capacity, and
  both-limit failures with no fallback or mutation; and
- panel/content stale behavior, sibling completion, destination identity,
  source order, group topology, focus, tabs, and dashboard absence.

Use a deliberately poor but structurally valid registered selection as the rope
control. The test asserts exact authored RGBs, not that the colors are attractive.
No contrast or perceptual helper may enter the plot path.

Add architecture guards that:

- keep libaryon unchanged and the envelope independent of resolved palette and
  GUI types;
- keep the stateful request explicit/default-free;
- keep the legacy request and PRODUCT-0029 snapshot reachable only from
  compatibility callers, not Rune relation views;
- keep exact palette colors in resolved specs and out of kernels;
- keep band fills outside `Channel::Color` and guides;
- keep `palette`, `skin`, and mark modifiers in their separate registers; and
- keep continuation out of external transport, panels, dashboard, and tables.

Run formatting, focused PRODUCT-0007/0010/0013/0016/0028/0029/0030 tests,
architecture tests, the Stage 1 oracle, and the full Polariton suite before
creating the one commit:

Commit: `impl: author categorical palettes on Rune figures`.

PRODUCT-0030 gates discharged: 1–13, 15, 17, and 18.

## Stage 3: dogfood, prove history, and close without code changes

Immediately capture implementation commit `I`, verify `I^ == O`, and run Stage
0g's implementation-diff assay before another commit is made. Then update the
external dogfood fixture `../uat/test9.rn` from the ahead-of-language mark
spelling/comment to:

```rune
figure([
    title("SQL = 💩"),
    subtitle("Wendelon was here"),
    palette("tab20c"),
    bar("cat", "val").bar_width(0.98),
])
```

Run the installed application and record:

- six exact authored bar colors in established category mapping;
- one ordinary host panel and no dashboard;
- unchanged panel/group identity across `tab20c_s1` and back;
- visibly shifted colors with unchanged category positions; and
- no aesthetic refusal or substitution, even where the selection is pale.

`../uat` is outside this repository and is not part of `I`, `E`, or `C`. Record
its exact source and manual observation in evidence; do not treat a screenshot as
proof of stable positions without the automated mapping assay from Stage 2.

Run the complete verification set:

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo test -p libaryon
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```

Write
`plans/product/impl/0030_the_categorical_palette_figure_surface_evidence.md`
with Stage 0 findings, old/new caller oracles, syntax normal form, complete color
mappings, live continuation traces, failure/rollback evidence, bank lifecycle,
historical diff, eighteen-gate ledger, file census, dogfood observation, and full
verification. Commit that file alone as `E`:

Commit: `evidence: record the categorical palette figure surface`.

Capture `E = git rev-parse HEAD`, run the immutable `git diff --name-only I..E`
census, and update the evidence document with `E` plus the exact command/result.
In closure commit `C`:

- flip PRODUCT-0030 to accepted;
- advance the Rune workspace corpus to v0.30;
- add the general three-attachment-point orientation table and rule of thumb to
  the corpus, not merely the palette row;
- accept the executable `palette(name)` figure row and explicit `viridis`
  absence behavior;
- cite the installed `test9.rn` dogfood separately from automated evidence; and
- record why unreachable producer banks remain until process teardown and that
  this is the second independent need for producer release; a third triggers a
  dedicated `BufferReleased` protocol record rather than another workaround.

After `I`, commits `E` and `C` may change only:

```text
plans/product/adr/0030_the_categorical_palette_figure_surface.md
plans/product/impl/0030_the_categorical_palette_figure_surface_evidence.md
plans/product/corpus/rune_workspace_corpus.md
```

Commit subject: `evidence: close the categorical palette figure surface`.

Commit body: `Records I..E because E cannot contain its own hash.`

PRODUCT-0030 gates discharged: 14 and 16.

## Gate ledger

| PRODUCT-0030 gate | stage |
|---|---|
| 1 | 2 |
| 2 | 2 |
| 3 | 2 |
| 4 | 2 |
| 5 | 2 |
| 6 | 2 |
| 7 | 2 |
| 8 | 2 |
| 9 | 2 |
| 10 | 2 |
| 11 | 2 |
| 12 | 2 |
| 13 | 2 |
| 14 | 3 |
| 15 | 2 |
| 16 | 3 |
| 17 | 2 |
| 18 | 2 |

Every gate is assigned exactly once. Stage 0 proves feasibility and selects the
existing-protocol branch; Stage 1 banks history and discharges none. Stage 2 is
the sole production commit. Stage 3 observes the two properties that cannot be
proved from the final tree alone: indivisible history and real-window fidelity.

## Permitted implementation files

```text
crates/polariton/src/rune_relation_envelope.rs
crates/polariton/src/plot/theme.rs
crates/polariton/src/plot/request.rs
crates/polariton/src/gui/messages.rs
crates/polariton/src/gui/state.rs
crates/polariton/src/plot/spec/snapshots/product-0030-pre-switch-categorical.txt
crates/polariton/tests/architecture_test.rs
plans/product/adr/0030_the_categorical_palette_figure_surface.md
plans/product/impl/0030_the_categorical_palette_figure_surface.md
plans/product/impl/0030_the_categorical_palette_figure_surface_evidence.md
plans/product/corpus/rune_workspace_corpus.md
```

The new snapshot is optional if PRODUCT-0029's existing oracle already covers
every Stage 1 fact. Colocated tests in permitted production files are permitted
and must land in `I` unless they are source-only Stage 1 tests. The external
`../uat/test9.rn` dogfood edit is permitted after `I` and is not a repository
commit.

Any additional production file requires a cited Stage 0 finding and halt review
before editing. In particular, do not edit libaryon, `plot/spec/mod.rs`, plot
kernels/factory, `gui/plot_pane`, `editor.rs`, panel/layout code, table code,
configuration, adapters, or Toron merely to shorten the permitted path.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo test -p libaryon
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```
