# Resolved plot artifact chrome

Scope: implement PRODUCT-0031 only. Deliverable is one pre-extraction renderer
oracle commit, two production-and-tests commits, and one evidence commit. The
exact library and application defaults must survive the extraction. Theme-name
projection and Rune `skin(...)` remain PRODUCT-0032.

## Halt rule

Stop on Rune or envelope work, libaryon theme/palette changes, a skin-name
projection or registry, a default gridline visual change, mark or encoded-swatch
recoloring, Guide behavior, plot geometry or interaction changes, editor-theme
or protocol changes, table behavior, execution, transport, panel identity,
placement, dashboard, configuration, export, or durability work.

## Stage 0: feasibility and boundary findings

### 0a. The role census must classify every old field read

Before editing production code, classify every read of
`background_color`/`decoration_color` in `plot/common.rs`, every kernel, scatter
rasterization, and `gui/plot_pane`. The expected classes are:

```text
PlotChrome role
background compositing/knockout
legacy mark decoration
plot-pane application chrome/control compatibility
test-only observation
```

No read may disappear into an “other” bucket. Check the census into evidence
with file, line, semantic class, replacement role, and oracle case. If a read
cannot be classified without deciding mark semantics or Guide contents, halt.

The known mark-decoration group includes box-plot outlines/medians/whiskers,
violin outlines/medians, candlestick open/close ticks, and funnel borders and
in-mark values. The known background-compositing group includes scatter raster
clear pixels, axis halos, and pie separators. Legend framing is chrome; encoded
legend fills are data.

### 0b. The existing backend seam can record real rendering

`PlotBackend` already abstracts fill, stroke, text, image, translation, rotation,
save, and clip operations. Extend a test-only recording implementation rather
than extracting a second paint model. Drive `PlotWidget::render` and real kernel
`plot`/`draw_legend` paths so the oracle observes the same calls production uses.

The kernel matrix must cover at least:

- Cartesian and categorical axes, ticks, labels, major/minor grids, title, and
  subtitle;
- a categorical legend and a continuous color-bar legend;
- bubble size-legend fill/border/text and its distinct legend opacity;
- radar and parallel coordinate labels/lines;
- scatter raster background bytes and pie separator knockouts; and
- box plot, violin, candlestick, and funnel legacy mark-decoration operations.

One synthetic kernel may cover common widget chrome, but it cannot stand in for
kernel-owned legend or mark paths. If the matrix cannot reach a current color
consumer through its production draw method, halt instead of replacing the
oracle with field equality.

### 0c. Default parity and universal validation conflict in one known role

Compute visible ratios from the recorded effective colors using libaryon's
normalized sRGB math. The expected black-canvas control is:

```text
opaque white decoration                 21.000:1
white at alpha 0.2 over black grid       1.662:1
```

The `.001` library background produces approximately `20.968:1` and `1.665:1`.
Bank computed values rather than rounded constants as assertions. The extraction
must preserve them. Do not raise alpha, omit gridlines from the assay, or compare
opaque white. PRODUCT-0032 is the first path required to return only validated
skins.

### 0d. Alpha ownership must become singular

Introduce one plot-local RGBA value whose validated channels and declared alpha
convert to Iced only at renderer edges. For roles whose current alpha is fixed by
the semantic use—gridline and neutral size sample—move that opacity into the
role and remove the renderer-side override. For kernel-specific legend backing
opacity and secondary radial/parallel tick hierarchy, preserve the explicitly
named use-site treatment and make the contrast helper assay every actual
effective alpha.

If one role needs an unnamed or unassayed opacity multiplier, halt and split the
role rather than preserving hidden post-validation treatment.

### 0e. The old plot-pane controls are compatibility producers

The settings pane's background and decoration inputs currently mutate the
flattened pair and also use the decoration value to style UI outside the canvas.
Keep their visible behavior without making authored `PlotChrome` escape the
artifact:

- expand a background edit to the background-family roles;
- expand a decoration edit to the current decoration-family chrome roles and
  the isolated legacy mark-decoration value;
- retain a separate pane/application color for settings-drawer controls; and
- preserve the current automatic contrast-color update after a background edit.

This is a migration of an existing pane override, not a new role editor. If
preservation requires exposing eleven controls or changing the app theme
protocol, halt.

### 0f. Existing settings ownership already gives atomic replacement

`HostPlotState` retains its kernel, `ResolvedPlotSettings`, and `PlotSettings`
model together, and replacement rebuilds those fields in one path. Rune ready
messages install kernel plus authored settings together. Exercise those seams;
do not create a second `PlotArtifact` merely to imitate tables if the existing
state already proves no mixed completion.

## Stage 1: bank the pre-extraction renderer oracle

Add the recording backend and the Stage 0b kernel matrix with the old two fields
still intact. Store a checked-in textual snapshot of ordered operations including
operation kind, effective RGBA, semantic fixture label, text where relevant,
stroke width/dash, bounds/points sufficient to distinguish roles, and raster
background bytes.

Add a checked-in source census from Stage 0a or generate an exact test assertion
over the known read sites. The oracle commit may add test-only code and snapshots
but changes no production rendering or settings behavior.

Commit: `test: capture the plot chrome renderer oracle`.

This stage discharges no PRODUCT-0031 gate; it creates the independent baseline
used by Stage 3.

## Stage 2: model complete chrome and contrast facts

Add a plot-local RGBA value and closed eleven-role `PlotChrome`. Define the
library and application compatibility constants from Stage 1's actual pixels,
not from the dead `PlotChromeSpec.axis`/`.gridline` values. Add helpers that:

- composite declared alpha and explicit legend treatment over the exact chrome
  background without `u8` quantization;
- report all five 4.5:1 text facts and all three 3:1 non-text facts;
- name each semantic role and its required threshold; and
- return the weakest applicable fact without treating the compatibility default
  as a successful projected skin.

Test valid/invalid alpha construction, black/white reference controls, the exact
legacy grid result, a fully passing synthetic chrome, and independent 4.5/3.0
failures. Add a compile-time/architecture assay pinning exactly eleven complete
fields and keeping theme names, palettes, tables, and `UiPalette` out of the
resolved type.

PRODUCT-0031 gates: 1, 4, 5.

Commit: `impl: model resolved plot artifact chrome`.

## Stage 3: route every renderer consumer through the boundary

Replace `ResolvedPlotSettings.background_color` and `.decoration_color` with
required resolved chrome plus explicitly non-chrome compatibility fields where
Stage 0a requires them. Migrate common widget drawing, every kernel legend,
scatter rasterization, and background-compositing case. Isolate mark-decoration
reads so a `PlotChrome` sentinel cannot alter them.

Retire `PlotChromeSpec` only after mapping its *actual current projection* into
full roles. `PlotThemeSpec` may contain the resolved `PlotChrome`; ramps remain
unchanged. Update the plot-pane two-control producer per Stage 0e without adding
role controls or allowing plot skins to style the drawer.

Rerun Stage 1's snapshot unchanged and require exact equality for library and
application defaults. Then run a sentinel chrome with a unique RGBA per role and
assert:

- every declared role appears at every expected consumer;
- operation order, geometry, strings, tick values, guide entries, and hit/hover
  output are unchanged;
- mark-decoration and encoded swatch operations remain baseline-exact;
- only explicit raster-clear/halo/pie-knockout pixels follow the background; and
- legend content and layered delegation are unchanged.

Exercise pane background/decoration edits, successful kernel/settings
replacement, and failed rerun/stale retention. Add architecture guards against
old field reads, renderer fallback, skin roles in mark semantics, and canvas
roles in host/settings UI.

PRODUCT-0031 gates: 2, 3, 6-12.

Commit: `impl: render plots from resolved artifact chrome`.

## Evidence

Write `plans/product/impl/0031_resolved_plot_artifact_chrome_evidence.md` with
the complete old-read census, role mapping, operation snapshots, computed
contrast ledger, explicit legacy-grid exception, sentinel differential, mark and
Guide boundary results, settings-pane compatibility, lifecycle assays, commit
trace, twelve-gate ledger, and full verification. Advance the product corpus
only with a substrate entry; no Rune surface row changes status.

Commit: `evidence: close resolved plot artifact chrome`.

## Gate ledger

| PRODUCT-0031 gate | stage |
|---|---|
| 1 | 2 |
| 2 | 3 |
| 3 | 3 |
| 4 | 2 |
| 5 | 2 |
| 6 | 3 |
| 7 | 3 |
| 8 | 3 |
| 9 | 3 |
| 10 | 3 |
| 11 | 3 |
| 12 | 3 |

Every gate is assigned exactly once. Stage 1 deliberately discharges none; it
banks history before the renderer vocabulary changes.

## Permitted implementation files

```text
crates/polariton/src/plot/common.rs
crates/polariton/src/plot/theme.rs
crates/polariton/src/plot/chrome.rs                         # new, if useful
crates/polariton/src/plot/mod.rs                            # module declaration only
crates/polariton/src/plot/kernels/*.rs                      # classified color reads only
crates/polariton/src/gui/plot_pane/state.rs
crates/polariton/src/gui/plot_pane/view.rs                  # boundary-preserving UI migration
crates/polariton/src/gui/state.rs                           # lifecycle tests only if needed
crates/polariton/tests/architecture_test.rs
crates/polariton/src/plot/snapshots/product-0031-chrome.txt # exact path may follow local convention
plans/product/adr/0031_resolved_plot_artifact_chrome.md
plans/product/corpus/rune_workspace_corpus.md
plans/product/impl/0031_resolved_plot_artifact_chrome_evidence.md
```

Colocated tests are permitted. A wildcard does not authorize unrelated kernel
cleanup: each edited kernel must appear in the Stage 0a census with only its
classified color read and test coverage changed. Any need to edit the Rune
envelope, libaryon, plot specification/domain resolution, categorical palette
code, tables, adapters, Toron, panels/layout, or dashboard triggers the halt.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo test -p libaryon
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```
