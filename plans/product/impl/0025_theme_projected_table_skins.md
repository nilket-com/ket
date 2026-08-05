# Theme-projected table skins

Scope: implement PRODUCT-0025 only. Deliverable is one plan commit, three
implementation commits, and one evidence commit. Any aesthetic adjustment after
the real-window `test8.rn` review must remain confined to the documented role
mapping.

## Halt rule

Stop on any libaryon theme/palette change; PRODUCT-0023 role or threshold change;
`UiPalette` or active-theme coupling; configuration or workspace registry; SQL
or Markdown skin authoring; plot theme/data-color work; relation-sugar change;
execution, transport, identity, protocol, placement, dashboard, or geometry work.

## Stage 0: feasibility findings

### 0a. The target palette exercises order and has usable contrast

`terrain_rs10` resolves through PRODUCT-0024 as reversed terrain rotated right
by ten. Prototype the role policy using the exact PRODUCT-0023 assay before
production changes. It must visibly consume order and all seven ratios must
exceed `4.5:1`; otherwise tune only the Polariton projection before Stage 1.

### 0b. The envelope can keep colors out

`RuneViewIntent::Table` is the sole table normal form, and `RuneRelationSinkOutput`
already separates table completion from plot planning. Replace the unit table
intent with a closed table sink carrying `Option<String>`, then resolve it in the
head into `RuneRelationSinkOutput::Table(TableSkin)`. No GUI type or palette
value needs to enter the envelope.

### 0c. Existing lifecycle already provides atomic retention

`set_rune_view_table_with_skin` stores one `TableArtifact { frame, skin }`, and
`fail_or_stale` leaves completed content untouched. A failed projection can use
the existing per-sink error path; no failure-state or protocol change is needed.

## Stage 1: implement and validate the projection

Move PRODUCT-0023's visible compositing and seven named contrast checks into
production-private helpers without changing their inputs or thresholds. Add
`TableSkinProjectionError::{UnknownTheme, UnsuitableTheme}` and
`TableSkin::from_theme_name` implementing ADR §1–3 exactly.

Assay all twelve roles with sentinel palettes, record the final `terrain_rs10`
role values and ratios, cover unknown and synthetic flat/insufficient palettes,
and prove the full success invariant. Prove representative base/reverse/shift
skins differ because ordered positions are consumed. Keep `TableSkin::DEFAULT`
and its draw oracle unchanged.

PRODUCT-0025 gates: 1, 2, 3, 4, 5, 6.

Commit: `impl: project libaryon themes into validated table skins`.

## Stage 2: add the closed table modifier register

Replace the unit table intent with `RuneTableSink { skin_name: Option<String> }`.
Recognize and flatten exactly `table().skin(string_literal)` in proven sink
positions, including `view(name, ...)`. Preserve bare `table()` and relation
`r.table()` as default intents. Do not resolve the name in the envelope.

Add atomic parser assays for direct/named lowering, exact string retention,
argument shape, duplicate/unknown/cross-register modifiers, malformed receivers,
outer span attribution, and ordinary-Rune behavior outside `.views([...])`.
Update type-oriented legacy tests only where the table variant shape requires it;
behavioral expectations must remain unchanged.

PRODUCT-0025 gates: 7, 8, 9.

Commit: `impl: add the closed Rune table skin modifier`.

## Stage 3: carry the resolved skin through completion

Resolve the optional name while forming each table sink output. Bare tables carry
`TableSkin::DEFAULT`; named skins carry the validated projection; errors remain
per-sink and are attributed by the existing outer-span path. Change the output
variant to require a `TableSkin`, then call the existing required-skin editor
handoff.

Assay one shared result feeding a styled and default table plus a plot; exact
`Arc` identity must survive. Prove a bad skin fails only its destination and
retains prior content stale, while changing a good skin reuses panel identity,
label, geometry, group state, focus, and tabs. Keep SQL, Markdown, `UiPalette`,
plot, and dashboard assertions unchanged. Parse the installed `test8.rn` and, if
a live Toron is available, complete it in the ordinary host-panel path.

PRODUCT-0025 gates: 10, 11, 12.

Commit: `impl: complete Rune tables with resolved skins`.

## Evidence

Write `plans/product/impl/0025_theme_projected_table_skins_evidence.md` with the
Stage 0 ratios, landed twelve-role mapping, suitability census, gate ledger, parser and
lifecycle evidence, commit trace, full verification, and real-window result if
available. Add corpus v0.25 and the executable `.skin(name)` row; accept the ADR
only after all automated gates pass. Aesthetic acceptance remains explicitly
identified as `test8.rn` evidence rather than disguised as an assertion.

Commit: `evidence: close theme-projected table skins`.

## Gate ledger

| PRODUCT-0025 gate | stage |
|---|---|
| 1 | 1 |
| 2 | 1 |
| 3 | 1 |
| 4 | 1 |
| 5 | 1 |
| 6 | 1 |
| 7 | 2 |
| 8 | 2 |
| 9 | 2 |
| 10 | 3 |
| 11 | 3 |
| 12 | 3 |

Each gate is assigned exactly once.

## Permitted production files

```text
crates/polariton/src/gui/table_skin.rs
crates/polariton/src/gui/state.rs
crates/polariton/src/gui/messages.rs
crates/polariton/src/rune_relation_envelope.rs
crates/polariton/src/editor.rs
crates/polariton/tests/architecture_test.rs
plans/product/adr/0025_theme_projected_table_skins.md
plans/product/impl/0025_theme_projected_table_skins_evidence.md
plans/product/corpus/rune_workspace_corpus.md
../uat/test8.rn                         (verification only; already authored)
```

Colocated tests are permitted. Any need to touch libaryon, table geometry/draw,
SQL or Markdown completion, plots, configuration, execution adapters, protocol,
placement, or dashboard code triggers the halt.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo test -p libaryon
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```
