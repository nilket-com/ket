# Observable resolved theme palettes

Scope: implement PRODUCT-0024 only. Deliverable is one pre-refactor oracle
commit, three `impl:` commits, and one evidence commit. PRODUCT-0025's
theme-to-table projection and `.skin(...)` remain unimplemented.

## Halt rule

Stop on any changed syntax color, theme name, alias, modifier parse or behavior,
including curated `c`/`C` no-op behavior; a public sampler; an artifact role,
alpha operation, or contrast threshold in libaryon; `UiPalette`; migration of an
existing syntax consumer; table projection; Rune or envelope work; configuration;
plotting; execution; protocol; panels; placement; or dashboard work.

## Stage 0: feasibility findings

### 0a. Twenty colors is an honest invariant

`THEMED_CATEGORIES` contains exactly twenty entries. A source census over all
twenty-one curated fill functions finds an assignment for every one; continuous
families already allocate `[Rgb; THEMED_CATEGORIES.len()]`. Both family branches
can therefore produce `ResolvedThemePalette([Rgb; 20])` without padding,
optional values, a family enum, or runtime length validation.

Stage 3 must make curated completeness executable rather than trusting the
census. If any curated fill cannot produce all twenty values, implementation
halts instead of weakening the type.

### 0b. Closing `Theme` literals has no workspace migration

A workspace-wide `Theme { ... }` census finds no construction outside
`theme.rs`. Apparent textual matches are `PersistLangTheme`, Iced's unrelated
`Theme`, and Polariton's `ColorTheme`. Production construction already flows
through `Theme::try_new`; adding a private retained palette therefore tightens
the public source API without requiring a workspace consumer to migrate.

Stage 4 records that tightening in an architecture assay. If an external
compatibility requirement appears, work halts rather than adding a public field
that could disagree with `syntax_colors`.

### 0c. The complete syntax oracle is finite and bankable

Before resolution changes, serialize the complete `syntax_colors` array as
canonical hex-or-absent entries for:

- every canonical `THEME_SPECS` name and alias;
- each unmodified name and its independent `_r`, `_c`, and `_C` variants;
- `_s0` through `_s19`; and
- the existing composed reverse/clip/shift regression expressions.

Store the exact textual snapshot under
`engine/snapshots/product-0024-theme-syntax.txt` and compare it with
`include_str!`. The snapshot, not a helper that reproduces resolution, is the
oracle. Stable length or FNV summaries may make review easier but do not replace
the byte comparison.

This stage also records the current modifier applicability matrix directly,
including `nord_c == nord` and `nord_C == nord`. It discharges no gate until the
same unchanged tests pass after Stage 4.

### 0d. Generic contrast can cross without quantizing composited colors

`Rgb` already lives in public libaryon core, and Polariton already consumes it.
Adding normalized `Srgb` beside it and moving relative luminance and contrast
ratio there adds no dependency and no protocol value. `TableRgba` continues to
composite locally, but passes the resulting fractional encoded-sRGB channels to
`Srgb` rather than rounding them through `u8`. All seven PRODUCT-0023 inputs and
thresholds therefore remain exact.

## Stage 1: capture the pre-refactor syntax oracle

Add the expression-census serializer and checked-in exact snapshot described in
Stage 0c. Drive only the current public `Theme::try_new` path. Include full
arrays so `BodyText`, absent categories, and themed-category order are visible;
do not snapshot only `THEMED_CATEGORIES` or compare selected token roles.

Keep the existing modifier tests. Add explicit matrix assertions only where
they state current behavior and require no production change. Mark curated clip
coverage as preserved PRODUCT-0024 compatibility behavior, not acceptance of
the no-op as correct.

Commit: `test: capture the resolved theme syntax oracle`.

## Stage 2: centralize generic sRGB contrast math

Add immutable, validated normalized `Srgb`, conversion from `Rgb`, and
`Srgb::relative_luminance` / `Srgb::contrast_ratio` using PRODUCT-0023's existing
WCAG formula. Invalid, non-finite, and out-of-range channels are not
representable: construction accepts inclusive zero/one, rejects NaN, infinities,
negative values, and values above one without clamping, and canonicalizes
negative zero to positive zero. Cover every refusal class, both endpoints,
black/white, equal colors, both sides of the channel threshold, fractional
inputs, symmetry, and established reference vectors. Optional `Rgb` convenience
methods must delegate to `Srgb` rather than duplicate the formula.

Retain alpha compositing and the seven table-role checks in Polariton, but
remove its duplicate relative-luminance and opaque ratio implementation. Convert
the composited visible channels directly to `Srgb`, convert the opaque background
through `Rgb`, then delegate. Assert that no `u8` conversion occurs on the
composited path. The existing PRODUCT-0023 ratios and opaque-selection positive
control must remain unchanged.

No palette type or `Theme` refactor lands in this commit.

PRODUCT-0024 gate: 6.

Commit: `impl: centralize generic sRGB contrast math`.

## Stage 3: model complete palettes and deterministic pairs

Add private-storage `ResolvedThemePalette([Rgb; 20])` plus immutable indexed and
whole-sequence observation. Add public indexed darker/lighter pair output that
retains both source indices, colors, and ratio. Do not expose mutation or a
general public constructor.

Implement `most_contrasting_pair` by enumerating distinct `i < j` pairs,
maximizing ratio, breaking equal-ratio ties by lexicographically earliest pair,
then orienting darker/lighter by luminance with lower index winning equal
luminance. A flat palette returns indices `0` and `1` at ratio `1.0`.

Assay the type shape structurally. Within the defining module, construct
synthetic palettes with exact duplicates, equal-luminance distinct colors, a
unique maximum, and a flat sequence. Prove every tie and index/color
correspondence explicitly. No theme family, syntax category, table threshold,
or artifact policy enters these tests.

PRODUCT-0024 gates: 1, 7, 8.

Commit: `impl: model complete resolved theme palettes`.

## Stage 4: retain the one palette used by syntax resolution

Refactor both `Theme::try_new` branches around one resolved twenty-color
sequence:

- curated fills first populate their existing syntax map; extract exactly the
  twenty themed-category values or fail the internal invariant; apply only the
  existing reverse/shift operations; reassign those categories; retain the
  sequence;
- continuous families sample the existing start/end range into the same fixed
  sequence; apply existing reverse/shift operations; assign and retain it.

Add the private palette field to `Theme` and a borrowing accessor. Keep
`syntax_colors` public and otherwise unchanged. Do not canonicalize or retain a
new theme-name string, expose `ThemePalette`, or make `THEMED_CATEGORIES` public.

Run the Stage 1 snapshot unchanged. Independently assert palette/category
equality for every expression in the census. Prove exact `r`, `sN`, continuous
`c`/`C`, and curated no-op behavior from the exposed sequences, including
rotation modulo twenty and representative alternative modifier ordering already
accepted by the parser.

Add an architecture assay for the absence of workspace `Theme` literals,
unchanged consumer access through `syntax_colors`, no raw sampler or artifact
vocabulary in the public API, and no libaryon protocol changes.

PRODUCT-0024 gates: 2, 3, 4, 5, 9, 10.

Commit: `impl: retain resolved palettes on themes`.

## Evidence

Write
`plans/product/impl/0024_observable_resolved_theme_palettes_evidence.md` with
the Stage 0 findings, ten-gate ledger, exact oracle census, curated completeness
result, modifier matrix, fractional contrast delegation, synthetic tie evidence, source-API
tightening, commit trace, and full verification. Add a corpus v0.24 substrate
entry without changing any Rune capability-row status. PRODUCT-0025 remains the
first table-skin surface and `../uat/test8.rn` remains intentionally ahead of the
implementation.

Commit: `evidence: close observable resolved theme palettes`.

## Gate ledger

| PRODUCT-0024 gate | stage |
|---|---|
| 1 | 3 |
| 2 | 4 |
| 3 | 4 |
| 4 | 4 |
| 5 | 4 |
| 6 | 2 |
| 7 | 3 |
| 8 | 3 |
| 9 | 4 |
| 10 | 4 |

Each gate is assigned exactly once. Stage 1 deliberately discharges none: it
banks the independent pre-refactor oracle that makes Stage 4's compatibility
claim falsifiable.

## Permitted production files

```text
crates/libaryon/src/core/color.rs
crates/libaryon/src/engine/theme.rs
crates/libaryon/src/engine/mod.rs
crates/libaryon/src/engine/snapshots/product-0024-theme-syntax.txt  (new)
crates/polariton/src/gui/table_skin.rs
crates/polariton/tests/architecture_test.rs                       (if useful)
plans/product/adr/0023_resolved_table_artifact_skins.md            (amendment)
plans/product/corpus/rune_workspace_corpus.md
plans/product/impl/0023_resolved_table_artifact_skins.md           (reference amendment)
plans/product/impl/0023_resolved_table_artifact_skins_evidence.md  (reference amendment)
plans/product/impl/0024_observable_resolved_theme_palettes_evidence.md
```

Colocated test modules and a libaryon architecture assay are permitted. Any need
to touch `UiPalette`, settings, input commands, help, completion, runtime theme
selection, a renderer, host-panel protocol, Polariton's envelope or table draw
path, plot production files, execution, placement, or dashboard code triggers
the halt rather than expanding this list.

## Verification

```text
cargo fmt --all -- --check
cargo test -p libaryon
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```
