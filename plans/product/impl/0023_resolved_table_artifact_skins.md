# Resolved table artifact skins

Scope: implement PRODUCT-0023 only. Deliverable is one pre-extraction evidence
commit, three `impl:` commits, and one evidence commit. PRODUCT-0024's resolved
theme palette, PRODUCT-0025, and `.skin(...)` remain unimplemented.

## Halt rule

Stop on Rune or envelope work, a skin-name registry, workspace or user
configuration, value-driven cell coloring, plot theme or ramp changes,
`UiPalette` or libaryon changes, panel or markdown-container chrome, another
table renderer, table geometry or interaction changes, execution, transport,
identity, placement, or dashboard work.

## Stage 0: feasibility findings

### 0a. Gate 2 can observe the actual draw path

Iced's core `Renderer` and text `Renderer` traits can be implemented by a
test-only `RecordingRenderer`. Its associated paragraph and editor types may be
`()`, whose complete null implementations Iced already supplies in debug builds.
The recorder therefore needs to intercept only `fill_quad` and `fill_text` while
the real `Table` widget performs layout and calls `draw`.

This is not a second table renderer or an extracted paint model. The production
draw path emits the operations; the test renderer records them in order. If this
cannot be made to exercise `Table::draw` directly, the work halts rather than
replacing gate 2 with structural color assertions.

### 0b. The existing default passes the proposed contrast gates

Using the §3 WCAG formula and compositing the declared 0.25-alpha selection fill
over both rows, the current colors resolve to these ratios:

```text
cell / even row             15.402:1
cell / odd row              13.158:1
header / header background  14.951:1
type / header background     8.139:1
gutter / gutter background  14.951:1
cell / selected even row    12.817:1
cell / selected odd row     10.921:1
```

The substrate can preserve the current appearance while enforcing 4.5:1. The
assay must compute these values from `TableSkin::DEFAULT`, not bank the numbers
above as expected outputs.

### 0c. One artifact can cover both lifecycle families

`PanelContent::Table(Box<DataFrame>)` is the host-panel backing object and
`NotebookCellOutput::Table(Box<DataFrame>)` is the markdown backing object
(`editor.rs:2454-2465`, `:404-409`). Both are private to `editor.rs` and may carry
one Polariton-owned `TableArtifact` without protocol work.

`PanelRegistry::fail_or_stale` already retains completed table content and
changes only its status (`editor.rs:2620-2635`). Once the skin sits beside the
frame in that content, host failure atomicity requires no new failure branch.
Markdown completion replaces its private output value as one operation.

### 0d. The consumer census is complete and enforced by the constructor

There are exactly two production `Table::new` sites: host panels at
`editor.rs:2702` and markdown cells at `editor.rs:3827`. Making the resolved skin
a required constructor argument turns any missed consumer into a compile error.
The host site covers `RESULT_PANEL`, Rune view tables, and any other table-backed
ordinary panel through the same `PanelContent` variant.

## Stage 1: capture the pre-extraction draw oracle

Add the test-only `RecordingRenderer` inside `gui/table.rs`. Drive the actual
widget through layout and draw with a fixed two-column, alternating-row frame,
column types enabled, one selected cell, and bounds that exercise both scrollbar
thumbs. Record the ordered sequence of colored quad fills and colored text
operations, including bounds and content where needed to disambiguate semantic
roles.

Bank that exact operation sequence before `TableSkin` exists. Do not introduce a
paint-command abstraction, a second draw function, or a value-only assertion.
This stage establishes the oracle but discharges no PRODUCT-0023 gate; gate 2
closes only after the same recorder observes the skin-driven renderer.

Commit: `test: capture the table artifact draw oracle`.

## Stage 2: model complete validated skins

Add a Polariton-owned `TableRgba` preserving declared alpha and the closed
twelve-role `TableSkin`. Define `TableSkin::DEFAULT` from the exact colors banked
in Stage 1. Keep the representation independent of Iced drawing, `UiPalette`,
plot `ColorTheme`, and string-keyed registries; conversion to Iced color occurs
only at the table-renderer edge.

Implement the WCAG sRGB-to-linear relative-luminance calculation and source-over
alpha compositing as pure private helpers. Test all seven §3 pairs by iterating
every production built-in. Include an opaque-selection control to prove the
assay consumes the declared alpha rather than assuming 0.25.

Add a compile-time/architecture assay that pins exactly twelve fields, rejects
`Option` and string maps in the skin representation, and keeps `ColorTheme`,
`PlotThemeSpec`, and `UiPalette` out of the model.

PRODUCT-0023 gates: 1, 3.

Commit: `impl: model validated table artifact skins`.

## Stage 3: make the real renderer consume only its skin

Add a required `TableSkin` argument to `Table::new`, retain it on the widget,
and replace every table-artifact color read in `draw` with its semantic role.
The draw path may not consult `gui::colors`, the generic Iced theme argument,
`UiPalette`, or an internal fallback. Pass `TableSkin::DEFAULT` temporarily at
both production construction sites; Stage 4 moves ownership into the artifacts.

Run the Stage 1 recorder unchanged against the default and require exact
operation-sequence equality. Then use an assay skin with a unique sentinel color
for every role. Assert every sentinel appears in the expected operation and that
the operation bounds, text, order, table layout, scroll state, selection range,
hit behavior, and copied data are identical to the default run.

Add an architecture guard that `table.rs` contains no production reference to
the retired table constants and that plot theme/ramp sources are unchanged. The
shared `SCROLLBAR_THUMB` constant may remain for plot-pane consumers; the table
must no longer read it.

PRODUCT-0023 gates: 2, 4, 6, 9.

Commit: `impl: render tables from resolved artifact skins`.

## Stage 4: bind frame and skin into one completed artifact

Introduce `TableArtifact { frame, skin }` and make both
`PanelContent::Table` and `NotebookCellOutput::Table` carry it. Update SQL-result,
Rune-view, and markdown completion entrypoints to construct one artifact with the
default skin; keep retained DataFrame accessors and scroll routing behavior
unchanged. Both `Table::new` sites must destructure the artifact and pass its
stored skin rather than manufacturing a default at view time.

Assay replacement with two distinct frames and skins, then host failure. Observe
new frame plus new skin together after completion and the previous pair together
after failure, never a mixed pair. Cover markdown replacement through its own
output path. Exercise `RESULT_PANEL` and a Rune table panel separately even
though both share the host variant.

For the artifact boundary, record that every operation emitted by `Table::draw`
is clipped to its supplied content bounds, and compare the host projection,
panel descriptor, focus, tab membership, geometry, and surrounding wrapper
style before and after a skin-only artifact replacement. The markdown assay does
the corresponding container comparison. No libaryon source or protocol type may
change.

PRODUCT-0023 gates: 5, 7, 8, 10.

Commit: `impl: retain table frames and skins atomically`.

## Evidence

Write `plans/product/impl/0023_resolved_table_artifact_skins_evidence.md` with
the Stage 0 findings, ten-gate ledger, consumer census, operation baseline,
contrast results computed from the landed built-ins, commit trace, and complete
verification. Add a corpus v0.23 substrate entry without changing any Rune
capability-row status; PRODUCT-0025 remains the first surface caller.

Commit: `evidence: close resolved table artifact skins`.

## Gate ledger

| PRODUCT-0023 gate | stage |
|---|---|
| 1 | 2 |
| 2 | 3 |
| 3 | 2 |
| 4 | 3 |
| 5 | 4 |
| 6 | 3 |
| 7 | 4 |
| 8 | 4 |
| 9 | 3 |
| 10 | 4 |

Each gate is assigned exactly once. Stage 1 deliberately discharges none: it
banks the independent pre-extraction oracle that makes Stage 3's gate 2
falsifiable.

## Permitted production files

```text
crates/polariton/src/gui/table.rs
crates/polariton/src/gui/table_skin.rs       (new, if separation is useful)
crates/polariton/src/gui/mod.rs              (module declaration only)
crates/polariton/src/gui/colors.rs           (retire table-only constants only)
crates/polariton/src/editor.rs
crates/polariton/tests/architecture_test.rs
plans/product/corpus/rune_workspace_corpus.md
plans/product/impl/0023_resolved_table_artifact_skins_evidence.md
```

Test modules colocated with those production files are permitted. Any need to
touch `rune_relation_envelope.rs`, `gui/state.rs`, plot production files,
libaryon, configuration schemas, or another renderer triggers the halt rather
than expanding this list.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
cargo test -p libaryon
cargo build -p polariton
git diff --check
```
