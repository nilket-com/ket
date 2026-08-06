# Theme-projected plot chrome skins

Scope: implement PRODUCT-0032 only after PRODUCT-0031 is accepted. Deliverable
is one pre-surface boundary-oracle commit, one indivisible production-and-tests
commit, one evidence commit, and one documentation-only closure commit that pins
the evidence commit's hash. The implementation commit must introduce the sole
projection, `skin(name)`, destination-local resolution, and atomic completed
settings together. No follow-up production cleanup commit is permitted.

## Halt rule

Stop on a second projection or surface selector, ordered palette spending,
libaryon theme/palette behavior change, compatibility-default visual change,
mark/data contrast validation, any mark or encoded-swatch recoloring, Guide
contents or merging, table behavior, application/workspace skin registry,
arbitrary role authoring, execution, transport, identity, placement, dashboard,
configuration, export, or durability work.

## Stage 0: feasibility and historical-proof findings

### 0a. Figure parsing has the same bounded metadata seam

`parse_figure` already flattens `title`, `subtitle`, and `palette` into
`RunePlotIntent { body, authored_settings }`. Add one duplicate-checked literal
`skin` member beside them and lower only its identity into the authored settings
layer. No mark parser, postfix register, `AuthoredPlotSpec`, `LayeredPlotSpec`,
or relation sugar needs a new field.

Recognition remains contextual. The existing call parser can distinguish a
direct member from a body, nested layers, a mark postfix, table's separate
`.skin(...)`, and ordinary Rune outside the envelope. If accepting skin requires
preserving metadata order or putting identity on a mark/specification, halt.

### 0b. Projection can stay entirely in Polariton

Use `Theme::try_new(name)`, `Theme::palette()`, and
`most_contrasting_pair()`. Add a private projection seam over a resolved
twenty-color sequence so a synthetic low-range palette can exercise failure
without a public libaryon constructor.

The production API has one `from_theme_name(name)` shape and no projection enum.
The projection assigns darker paper, lighter ink, and deterministic alpha
variants to all PRODUCT-0031 roles, then runs PRODUCT-0031's exact visible assay.
Tune the grid and neutral-sample alpha policy against `nord` before committing;
record exact values and ratios in evidence. If `nord` cannot pass without a
third hue, threshold waiver, or role omission, halt and return the projection
policy to architecture.

Run a catalog census over every canonical base, alias, `r`, `c`, `C`, and
`s0..s19` expression. Record success with all eleven RGBA values and eight ratios
or failure with exact structured facts and display text. This becomes the
projection oracle and proves no panic/fallback across the accepted grammar.

### 0c. Identity resolution precedes infallible renderer flattening

The envelope's authored layer carries `plot_skin: Override<Arc<String>>`.
Before building/installing a plot, the head resolves an authored identity to a
complete `PlotChrome` and places it into the resolved authored chrome producer
used by the ordinary settings cascade. Omission leaves that producer inherited;
it does not call libaryon.

Keep name resolution fallible and destination-local before
`PlotSettings::resolve()` remains an infallible flatten of complete values. If
this requires resolving inside a kernel/renderer or adding `Result` to every
settings read, halt and introduce a bounded prepared-settings carrier instead.

The graphical-layer tier remains inapplicable to chrome. Existing application
theme and pane override producers retain precedence. In particular, manual pane
color edits may override an authored skin without changing the authored
identity or creating a second grammar.

### 0d. Existing Rune completion carries authored settings atomically

Single, styled-single, and layered Rune outputs already carry
`PlotSettingsLayer` into their ready messages and install kernel plus settings at
the current-revision boundary. Extend preparation so skin failure creates no
ready content and success carries the resolved authored chrome with those same
settings. Reuse existing stale retention and sibling completion.

Drive real update messages; a direct projection unit test does not prove that a
failed rerun cannot install a new skin on old content. Skin identity stays out of
`LocalViewIdentity` and categorical-continuation keys.

### 0e. `skin`/`palette` independence is observable at completed values

Use a two-field bar fixture with categorical band fills and a plot fixture with
a legend. Record exact `PlotChrome` roles, exact `ResolvedPlotSpec` category
positions/colors, encoded legend fills, and chrome legend operations across:

```text
skin nord / palette tab20c
skin nord / palette tab20c_r
skin nord_r / palette tab20c_r
skin omitted / palette tab20c_r
```

The first palette change may alter only data values. The skin change may alter
only chrome and may be a no-op when the resolved contrast pair is the same under
PRODUCT-0032 §4. Removing skin returns exactly to PRODUCT-0031 compatibility
chrome without changing categorical positions.

Add the valid same-expression co-occurrence transition:

```text
skin viridis_r  / palette viridis_r
skin viridis_s5 / palette viridis_s5
```

The exact categorical positions remain stable and their colors follow the
changed ordered sequence; all eleven chrome roles remain byte-for-byte equal
because reverse and shift preserve the set and therefore its contrast pair.
Use `viridis`, not `nord`, because PRODUCT-0028 deliberately exposes no
categorical palette for `nord`.

Include `skin("nord")` with `palette("yellows")` as the rope control. Assert the
exact authored data colors and absence of any cross-contrast diagnostic; do not
make the test judge whether the result is attractive.

### 0f. Guide observation must exclude Guide policy

Record existing legend operations before the surface switch for ordinary and
layered plots. After skinning, normalize only the PRODUCT-0031 chrome RGBA values
and require content, entry order, labels, geometry, encoded fills, and
PRODUCT-0013 last-layer delegation to remain exact. Mark these tests as bridge
compatibility, not acceptance of the provisional semantics.

### 0g. The surface/projection commit is historically indivisible

Let the combined ADR/plan commit be `P` and the pre-surface oracle commit be `O`.
Verify `O` contains no production behavior change and records:

```text
figure parser:     no direct skin member
authored settings: no plot-skin identity
projection:        no name-to-PlotChrome path
completion:        no prepared skin failure/value
```

The implementation commit `I` must have `I^ == O`. Its one diff must contain
syntax recognition, the only projection, full validation, destination-local
failure, and completed settings handoff. Evidence commit `E` cannot contain its
own hash, so a documentation-only closure `C` records `E` and explains:

```text
Records I..E because E cannot contain its own hash.
```

Do not squash `C` into `E`.

## Stage 1: bank the pre-surface boundaries

Add test-only snapshots/assays for Stage 0e and 0f using the PRODUCT-0031 default
with no plot-skin grammar. Add contextual refusal controls proving direct
`skin(...)` is not yet special while table `.skin(...)` remains accepted in its
own register. Change no production source behavior.

Record `P` and `O` immediately after this commit and perform Stage 0g's pre-state
inspection.

Commit: `test: bank the plot skin surface boundary`.

This stage discharges no PRODUCT-0032 gate.

## Stage 2: land projection, surface, and completion atomically

This is the only production-and-tests commit.

### 2a. Add the sole validated projection

Implement the darker-paper/lighter-ink role policy and deterministic alpha
hierarchy. Return structured unknown/unsuitable errors with anchor and weakest
visible-role facts. Run the complete catalog census, positive `nord` assay,
private low-range failure, exact 4.5/3.0 threshold-edge controls, and base/reverse/
shift/clipping many-to-one observations.

Architecture guards reject a projection enum, table projection dependency,
neutral third ink, local name parser, fallback, and unvalidated success.

### 2b. Add contextual figure syntax and bounded identity

Extend `PlotSettingsLayer` and `parse_figure` with the direct, literal,
duplicate-refusing skin member. Cover metadata permutations, named/direct views,
wrong arity, computed strings, duplicates, body/layers/mark positions, malformed
figures, table postfix coexistence, and ordinary Rune outside proven positions.

No string or resolved chrome enters `AuthoredPlotSpec`, `LayeredPlotSpec`,
`ResolvedPlotSpec`, a mark renderer, or external transport.

### 2c. Resolve per destination and install complete settings

Prepare skin outcome for every plot destination before independent origin work.
Unknown/unsuitable skin fails only that destination and does not cancel its
shared query or siblings. On success, carry complete authored chrome beside the
existing authored settings through single, styled, and layered ready paths. On
current-revision completion install kernel/settings/chrome together; on failure
or supersession retain the previous completed set stale.

Exercise success, live skin change, removal, unknown failure, unsuitable
failure, stale revision, two sibling views, and two origins completing in
opposite orders. Assert panel/view identity and categorical bank keys never
include skin identity.

### 2d. Prove the palette, mark, and Guide ropes

Rerun Stage 1 snapshots with only chrome RGBA normalized where a skin applies.
Drive Stage 0e's four-state matrix and awkward `nord`/`yellows` control through
real completion and rendering. Drive its paired
`viridis_r -> viridis_s5` transition as one figure-level assertion: exact bars
recolor, exact categorical positions stay fixed, and exact chrome does not
change. Include box plot, violin, candlestick, funnel, scatter raster, pie
separators, categorical legend, continuous legend, and the layered bridge.

Assert no data/mark contrast helper call, warning, fallback, palette rotation,
mark recoloring, swatch recoloring, guide entry change, or editor/settings UI
skin consumer exists. Pane override still wins after authored skin under the
accepted cascade.

Run formatting, focused PRODUCT-0013/0016/0024/0029/0030/0031/0032 tests,
architecture tests, Stage 1 oracles, and the full Polariton suite before the one
commit.

Commit: `impl: author plot chrome skins on Rune figures`.

PRODUCT-0032 gates discharged: 1-13 and 15.

## Stage 3: dogfood, prove history, and close

Immediately capture implementation commit `I`, verify `I^ == O`, and inspect
the full implementation diff before another commit. Update the external
categorical-plot dogfood fixture (currently `../uat/test9.rn`) to include:

```rune
figure([
    title("SQL = 💩"),
    subtitle("Wendelon was here"),
    skin("nord"),
    palette("tab20c"),
    bar("cat", "val").bar_width(0.98),
])
```

Run the installed application and record visible Nord chrome, unchanged exact
`tab20c` bar colors, one ordinary host panel and no dashboard, identity retention
across skin changes/removal, and a stale retained prior plot after an unsuitable
skin. Human observation is aesthetic evidence only; automated role/data
snapshots remain the sovereignty proof.

Write
`plans/product/impl/0032_theme_projected_plot_chrome_skins_evidence.md` with
the catalog census, exact Nord roles and eight ratios, failure facts, syntax
normal form, skin/palette differential, awkward-pair rope control, mark and
Guide boundary snapshots, lifecycle trace, historical diff, fifteen-gate
ledger, file census, dogfood observation, and complete verification. Commit that
file alone as `E`:

Commit: `evidence: record theme-projected plot chrome skins`.

Capture `E`, run the immutable `I..E` name-status census, then create closure
commit `C` which records `E`, accepts PRODUCT-0032, and advances the Rune corpus
with the direct figure skin row and the explicit `skin`/`palette` sovereignty
rule. After `I`, `E` and `C` may change only the ADR status, evidence, and corpus.

Commit: `evidence: close theme-projected plot chrome skins`.

Commit body: `Records I..E because E cannot contain its own hash.`

PRODUCT-0032 gate discharged: 14.

## Gate ledger

| PRODUCT-0032 gate | stage |
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

Every gate is assigned exactly once. Stage 0 proves feasibility and Stage 1
banks the independent boundary; neither discharges a gate.

## Permitted implementation files

```text
crates/polariton/src/plot/chrome.rs
crates/polariton/src/plot/theme.rs
crates/polariton/src/rune_relation_envelope.rs
crates/polariton/src/gui/messages.rs
    # only if prepared chrome needs the ready carrier
crates/polariton/src/gui/state.rs
crates/polariton/src/gui/plot_pane/state.rs             # cascade/lifecycle tests only
crates/polariton/tests/architecture_test.rs
crates/polariton/src/plot/snapshots/product-0032-plot-skins.txt
plans/product/adr/0032_theme_projected_plot_chrome_skins.md
plans/product/impl/0032_theme_projected_plot_chrome_skins_evidence.md
plans/product/corpus/rune_workspace_corpus.md
```

Colocated tests are permitted. The snapshot path may follow the local snapshot
directory established by PRODUCT-0031. Any need to edit libaryon, plot kernels,
plot specification/domain/categorical allocation, tables, adapters, Toron,
panel/layout code, dashboard, or configuration triggers halt review rather than
silent scope expansion. The external `../uat/test9.rn` edit is not a repository
commit.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo test -p libaryon
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```
