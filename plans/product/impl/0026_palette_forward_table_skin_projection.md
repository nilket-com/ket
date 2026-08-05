# Palette-forward table skin projection

Scope: implement PRODUCT-0026 only. Deliverable is one combined ADR/plan commit,
one pre-refactor oracle commit, one projection implementation commit, one
boundary/regression commit, and one evidence commit. PRODUCT-0027's Rune default,
`.duotone()` modifier, and teaching diagnostic remain unimplemented.

## Halt rule

Stop on any libaryon source change; PRODUCT-0023 role, formula, threshold, or
alpha-compositing change; theme name, alias, modifier, resolved palette, or
syntax-color change; Rune grammar, register, default, or normal-form change;
user-facing alternative suggestion; SQL/Markdown skin selection; table geometry
or lifecycle; plot behavior; `UiPalette`; configuration; execution; transport;
identity; panel protocol; placement; or dashboard work.

## Stage 0: feasibility findings

### 0a. The Duotone oracle is finite and bankable

`THEME_SPECS` contains fifty-three canonical families. Enumerate the Cartesian
product of:

```text
53 bases
× 2 reverse states
× 2 low-clip states
× 2 high-clip states
× 20 shift states
= 8,480 expressions
```

Use one canonical suffix order and spell the all-false, zero-shift case as the
bare base. PRODUCT-0024 already guards parser ordering, aliases, explicit `s0`,
curated clipping's preserved no-op, and syntax/palette identity; this oracle
guards the table projection of every distinct canonical modifier state.

Before any extraction or type change, serialize each current
`TableSkin::from_theme_name(name)` outcome into a checked-in textual snapshot:

- exactly one expression occupies exactly one line, with every role or failure
  fact inline before the terminating newline;
- success records all twelve roles in declaration order as exact RGB bytes plus
  `f32::to_bits()` alpha;
- failure records name, weakest role, `f64::to_bits()` observed ratio,
  `f64::to_bits()` minimum, and exact `Display` text.

Store it at
`crates/polariton/src/gui/snapshots/product-0026-duotone-table-skins.txt` and
compare it with `include_str!`. A digest, regenerated expected value, selected
role sample, or numeric tolerance is not the oracle. The snapshot is captured
in its own commit before production changes and rerun unchanged after Stage 2.

### 0b. Synthetic projection failures need no libaryon constructor

`ResolvedThemePalette::colors()` exposes `&[Rgb; 20]`, while its constructor and
`ResolvedThemePair` fields remain private. Factor private Polariton projection
seams over the color array. Production Duotone obtains its anchors only from
`ResolvedThemePalette::most_contrasting_pair()` and converts that public result
into private Duotone facts; it must not reimplement pair selection.

The synthetic both-failure assay may call the private color-array seam and
supply its known Duotone anchors as fixture facts. That is test input to the
projection, not a second pair-finding algorithm or a public invalid palette
state. If the assay requires a libaryon constructor or copied contrast-pair
search, halt rather than weaken PRODUCT-0024's authority boundary.

### 0c. Mechanism-specific failures can preserve displayed compatibility

Separate pure projection failure facts from name resolution:

```text
UnknownTheme { name }
Unsuitable {
    name,
    facts: DuotoneFailure | PaletteForwardFailure,
}
```

Duotone facts carry anchor indices/colors/ratio and the weakest completed role.
PaletteForward facts carry the text group and both neutral candidates' limiting
surfaces/minima. The wrapper attaches the theme name. `Display` for Duotone can
continue emitting PRODUCT-0025's exact prior string from its weakest-role facts;
no current user-visible diagnostic needs to change in this substrate cut.

PRODUCT-0027 can later compose advice from the structured facts without either
recomputing color policy or teaching this layer which projection is default.

### 0d. Palette-forward feasibility is decided before extraction

Prototype the tunable role policy against the exact PRODUCT-0023 assay before
Stage 1. It must establish all of the following simultaneously:

- four opaque non-text roles are direct colors from four distinct ordered
  palette positions;
- gridline, selection, and scrollbar consume ordered positions with explicit
  alpha;
- fixed near-black and near-white candidates are selected per text group by
  greatest minimum contrast over every actual surface, with near-black winning
  exact ties;
- `yellows` chooses near-black and passes all seven checks while current
  Duotone retains its approximately `4.685:1` anchor pair and approximately
  `3.175:1` weakest selected-row refusal;
- representative base/reverse/shift names differ in final twelve-role output;
  and
- a constructed split-polarity midtone array fails both mechanisms for the two
  different reasons stated by PRODUCT-0026 §5 and §7.

Record the chosen positions, alphas, neutral RGB values, limiting surfaces, and
exact ratios in evidence. These are tunable Polariton policy, not new ADR
semantics. If no policy meets every constraint without changing PRODUCT-0023 or
libaryon, halt before banking or refactoring code.

### 0e. The current caller census is small and explicit

The sole production theme-to-table caller is `rune_view_outcome` in
`gui/state.rs`; other `from_theme_name` occurrences are colocated tests. SQL and
Markdown paths construct `TableSkin::DEFAULT` directly and therefore do not
participate in projection selection. Stage 2 can migrate every projection call
to explicit Duotone without touching the envelope, editor table handoff, or
renderer.

## Stage 1: bank the exhaustive pre-refactor Duotone oracle

Add the 8,480-expression serializer and checked-in exact snapshot from Stage 0a
against the current one-argument API. Drive only canonical names through the
existing public `Theme::try_new` resolution path; do not duplicate theme parsing
or palette construction in the oracle. Assert exactly 8,480 newline-terminated
records and reject embedded newlines so the snapshot cannot silently drift into
a multi-line-per-expression format.

Include one readable assertion for the census cardinality and expression
uniqueness. Keep PRODUCT-0025's named `terrain_rs10` values/ratios and `yellows`
failure test visible beside the broad snapshot so a reviewer can understand a
failure without reading thousands of lines.

This stage changes no production source and discharges no gate. It banks the
independent evidence that makes gate 2 falsifiable after Stage 2.

Commit: `test: capture the duotone table skin oracle`.

## Stage 2: add both explicit projection mechanisms

Add closed `TableSkinProjection::{PaletteForward, Duotone}` with no `Default`,
optional conversion, or one-argument projection API. Refactor the accepted body
into the private Duotone seam without changing one operation. Add the private
PaletteForward seam, neutral maximin selection, shared exact visible-surface
assay, and mechanism-specific structured facts described by ADR §3–7.

Production resolution remains one function:

```text
TableSkin::from_theme_name(name, projection)
```

It calls `Theme::try_new` once, dispatches explicitly, validates once through
the shared PRODUCT-0023 checks, and never probes or substitutes the other mode.
Unknown identity remains independent of projection failure. Migrate the Rune
head and every test caller to explicit Duotone; no production call may select
PaletteForward.

Rerun the Stage 1 snapshot unchanged. Extend the named `terrain_rs10` assay to
pin every accepted Duotone role and ratio plus new truthful anchor facts. Add:

- all-twelve-role and direct-position PaletteForward assertions;
- neutral-candidate maximin and deterministic tie assays over actual visible
  surfaces;
- `yellows` Duotone-failure/PaletteForward-success evidence, including its
  above-threshold raw pair and below-threshold completed role;
- the split-polarity synthetic both-failure assay through the private seam;
- a full 8,480-expression two-mode census proving every success passes all
  seven visible checks and every failure is structured/non-fallback; and
- final-role—not intermediate-index—base/reverse/shift inequalities.

Do not add a generic error variant that discards mechanism facts. Do not let the
synthetic seam become public, construct a `ResolvedThemePalette`, or reproduce
libaryon's pair search.

PRODUCT-0026 gates: 1, 2, 4, 5, 6, 7, 8, 9.

Commit: `impl: add explicit table skin projections`.

## Stage 3: prove unchanged callers and authority boundaries

Add architecture coverage that makes the negative contracts reviewable:

- the projection enum has exactly two variants and no `Default` implementation;
- no one-argument `from_theme_name` call remains;
- production `state.rs` requests only explicit Duotone;
- no Rune envelope/register/normal-form source mentions either projection or
  `duotone`;
- SQL and Markdown paths still use `TableSkin::DEFAULT`;
- palette-forward and synthetic seams remain private to `table_skin.rs`;
- no production source probes a second projection after failure; and
- no `UiPalette`, plot, renderer geometry, dashboard, or libaryon production
  path acquired table-projection vocabulary.

Re-run PRODUCT-0023's draw oracle, skin-role consumption, alpha-compositing,
artifact atomicity, and skin-only layout assays. Re-run PRODUCT-0025's shared
result, stale retention, panel reuse, no-dashboard, and exact high-density
fixture assays. Assert that successful Rune output still carries the exact
Duotone `terrain_rs10` skin and that failure text for an existing unsuitable
theme remains byte-identical.

The compiled two-argument API is the primary proof of explicit selection;
source architecture assertions are reintroduction guards, not substitutes for
the type boundary. Gate 11 is discharged by the permitted-file census and full
libaryon oracle/suite, not by a grep alone.

PRODUCT-0026 gates: 3, 10, 11.

Commit: `test: guard table projection boundaries`.

## Evidence

Write
`plans/product/impl/0026_palette_forward_table_skin_projection_evidence.md`
with the Stage 0 findings, 8,480-expression oracle format and result, landed
PaletteForward role policy, exact neutral candidates, `yellows` values, both
mechanism-specific synthetic failures, two-mode suitability census, eleven-gate
ledger, caller/boundary census, commit trace, and full verification. Add corpus
v0.26 as an accepted substrate entry without changing the executable Rune
`.skin(name)` row or its current Duotone behavior. Accept the ADR only after all
automated gates pass.

PRODUCT-0027 remains the first PaletteForward production caller and the sole
owner of the default flip, `.duotone()` modifier, and conditional teaching
diagnostic.

Commit: `evidence: close palette-forward table skin projection`.

## Gate ledger

| PRODUCT-0026 gate | stage |
|---|---|
| 1 | 2 |
| 2 | 2 |
| 3 | 3 |
| 4 | 2 |
| 5 | 2 |
| 6 | 2 |
| 7 | 2 |
| 8 | 2 |
| 9 | 2 |
| 10 | 3 |
| 11 | 3 |

Each gate is assigned exactly once. Stage 1 deliberately discharges none: the
oracle must exist before the production refactor whose compatibility it judges.

## Permitted production files

```text
crates/polariton/src/gui/table_skin.rs
crates/polariton/src/gui/state.rs
crates/polariton/src/gui/snapshots/product-0026-duotone-table-skins.txt  (new)
crates/polariton/tests/architecture_test.rs
plans/product/adr/0026_palette_forward_table_skin_projection.md
plans/product/impl/0026_palette_forward_table_skin_projection_evidence.md
plans/product/corpus/rune_workspace_corpus.md
```

Colocated test modules are permitted. The snapshot is test evidence rather than
production behavior. Any need to edit libaryon, the Rune envelope, GUI messages,
editor table handoff, table renderer/geometry, plot files, configuration,
execution adapters, protocol, placement, or dashboard triggers the halt rather
than expanding this list.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo test -p libaryon
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```
