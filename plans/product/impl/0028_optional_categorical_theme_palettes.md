# Optional categorical theme palettes

Scope: implement PRODUCT-0028 only. Deliverable is one combined ADR/plan commit,
one categorical model/declaration commit, one resolution/modifier commit, one
boundary/regression commit, and one evidence commit. PRODUCT-0029's plot
positions, capacity policy, diagnostics, and rendering remain unimplemented.

## Halt rule

Stop on any changed syntax color, existing `Theme::palette()` value, table-skin
outcome, theme name, alias, case behavior, modifier parse/order, modifier result,
or error string; a categorical declaration for a curated family outside the
accepted six; inferred/deduplicated/padded colors; public empty construction;
artifact, geometry, capacity, plot, or diagnostic vocabulary in libaryon; any
Polariton production change; Rune; `UiPalette`; execution; transport; panels;
placement; or dashboard work.

## Stage 0: feasibility findings

### 0a. Existing oracles already bank the compatibility boundary

PRODUCT-0024's checked-in textual snapshot covers every syntax category for all
canonical names, aliases, independent modifiers, shifts `s0..s19`, and composed
modifier controls. Its palette/category equality tests pin every existing
`Theme::palette()` position. PRODUCT-0026's 8,480-expression snapshot pins every
table-skin role, alpha, ratio, failure fact, and diagnostic derived from those
palettes.

These are independent pre-refactor oracles already committed before PRODUCT-0028.
Do not replace, regenerate, narrow, or copy them. Stage 3 reruns them unchanged.
No new compatibility-oracle commit is needed.

### 0b. Six curated declarations are recoverable without taste decisions

Source inspection establishes the exact initial capability matrix:

```text
tab20, tab20b, tab20c    twenty named canonical colors
dark2                    eight named ColorBrewer colors; whitespace is separate
kelly, prism             twenty named ordered colors
all other curated        no accepted categorical sequence
all continuous           twenty accepted resolved samples
```

Record exact base arrays for the six capable curated families before refactoring
their fill functions. For each, compare the refactored syntax fill output with a
pre-change direct call inside the same test commit or rely on the complete
PRODUCT-0024 snapshot after the single-source change; do not infer a sequence
from post-fill token assignments.

If any accepted sequence requires choosing, dropping, or reordering a color not
already expressed by its declaration, halt and remove that family from the
initial capability set rather than making an aesthetic decision in code.

### 0c. Non-empty storage can be structural without a public `Vec`

Use an immutable resolved type equivalent to:

```text
ResolvedCategoricalPalette { first: Rgb, rest: Box<[Rgb]> }
```

or another representation whose fields and constructors make emptiness
unrepresentable. Expose `len`, `color(index)`, and ordered iteration. Do not
expose mutation, `Vec`, a public unchecked constructor, or a sentinel color.

The static curated declaration should likewise carry a required first color and
optional remainder rather than an empty-capable slice. Continuous resolution is
known to produce twenty samples and may construct the same resolved type through
a private non-empty path.

If ergonomic whole-sequence observation requires allocation, expose iteration
rather than weakening the invariant. Tests may collect iterators locally.

### 0d. One parser can resolve projections of different lengths

`Theme::try_new` already parses `(base, reverse, clip_low, clip_high, shift)`
once. Preserve that call exactly. Resolve the existing twenty-position sequence
through its current branch unchanged, then resolve the optional categorical
sequence from the same parsed facts:

- continuous: clone the accepted post-clip twenty samples before syntax
  assignment, then apply the same reverse and modulo-twenty shift;
- curated present: copy its explicit natural sequence, reverse if requested,
  and rotate right by `shift % natural_len`;
- curated absent: retain `None` without examining modifiers.

Do not call `parse_theme_modifiers` twice or factor a second suffix interpreter
into the categorical type. An architecture assertion guards the single call.

### 0e. Single-source constants preserve syntax parity

Move the six accepted curated sequences into private static/constant
non-empty declarations. Rewrite their existing fill functions to name colors by
index or destructuring from those declarations. Keep every current token-role
assignment exactly where it is; only the source of each `Rgb` changes.

`dark2`'s whitespace neutral remains an independent syntax-only constant and is
not appended to its categorical declaration. `tab20c`'s categorical declaration
is `c01..c20`, while its fill still assigns `String <- c09`, `Comment <- c20`,
and every other existing role unchanged.

If single-sourcing pressures a fill into natural order rather than its existing
syntax mapping, halt. The two projections are supposed to differ.

### 0f. The new API has no production consumer

A workspace census currently finds no categorical palette accessor because it
does not exist. PRODUCT-0028 adds observation only. No Polariton source, table
projection, syntax renderer, runtime setting, or protocol path should call it.
Only colocated libaryon tests consume it until PRODUCT-0029.

## Stage 1: model non-empty categorical declarations and resolved values

Add private non-empty categorical declaration storage and public immutable
`ResolvedCategoricalPalette`. Give the resolved type exact `len`, indexed color,
and ordered iteration APIs. Add `Option<ResolvedCategoricalPalette>` to `Theme`
behind a borrowing `categorical_palette()` accessor.
Re-export the resolved type through `libaryon::engine` beside
`ResolvedThemePalette`; the accessor must return a publicly nameable type for
PRODUCT-0029 without exposing the private `theme` module.

Extend `ThemePalette::Discrete`/`ThemeSpec::curated` to carry an optional
non-empty categorical declaration independently of `ThemeFamily`. Keep
`ThemeFamily` unchanged. Continuous specs need no duplicated static declaration;
their categorical sequence is derived during resolution in Stage 2.

Single-source the six accepted curated arrays and update their fill functions as
Stage 0e describes. Every other curated spec passes `None`. Add structural tests
that no production API constructs an empty palette and exact base-declaration
tests for all six sequences, especially complete `tab20c` and eight-color
`dark2` without whitespace.

At the end of this commit the field/accessor may return `None` for all themes;
no resolved categorical behavior is claimed until Stage 2. Existing theme
resolution, syntax, and tables must remain green.

PRODUCT-0028 gates: 1, 3, 4, 5.

Commit: `impl: declare optional categorical theme palettes`.

## Stage 2: resolve categorical projections through the existing parser

Populate `Theme.categorical_palette` in both existing `Theme::try_new` branches
using the single parsed modifier result:

- continuous families retain their exact post-clip, post-reverse,
  post-shift twenty-sample sequence as `Some`;
- capable curated families resolve their natural sequence with reverse and
  shift modulo natural length;
- incapable curated families return `None` under base names and every modifier.

Do not change construction or reassignment of the existing syntax-oriented
palette. Add a full categorical capability snapshot containing one line per
canonical PRODUCT-0024 expression with either `-` or the natural length and
ordered hex colors. Keep one expression per line and compare through
`include_str!`; do not replace it by a digest or regenerate expected values from
the implementation under test.

Because the capability did not exist before this commit, this new snapshot is a
future drift oracle, not independent acceptance evidence for its initial
contents. Gates 2 and 6–8 are discharged by the explicit capability census,
hand-authored canonical arrays, and direct modifier equations below. The
snapshot cannot substitute for those assertions.

Beside the broad snapshot, add readable assays for:

- exact capability census and natural lengths;
- `nord`, `nord_r`, and `nord_s1` remaining valid with `None`;
- `tab20c` natural versus existing syntax-oriented order;
- `dark2` base, `_r`, `_s1`, and `_s20`;
- `terrain_s12` equality between the two twenty-position projections;
- continuous `c`/`C` equality between projections; and
- curated `c`/`C` preserved no-op for both present and absent capability.

The `_r` assay proves behavior; `_s1` and `_s20` prove delegation to a
projection-local sequence length rather than a copied modulo-twenty operation.

PRODUCT-0028 gates: 2, 6, 7, 8.

Commit: `impl: resolve categorical theme palettes`.

## Stage 3: prove byte parity and authority boundaries

Rerun PRODUCT-0024's checked-in syntax snapshot, complete palette/category
equation, family completeness, reverse/shift/clipping matrix, alias/case/parser
tests, and exact error controls unchanged. Rerun PRODUCT-0026's exhaustive
8,480-expression table-skin oracle and all PRODUCT-0023/0025/0027 theme-to-table
regressions unchanged.

Add architecture coverage that:

- `Theme::try_new` calls `parse_theme_modifiers` exactly once;
- no categorical declaration is inferred from a fill map or existing resolved
  palette;
- the capable-curated set is exactly the accepted six;
- a curated `ThemeFamily` value may return either `Some` or `None`;
- the new resolved type has private non-empty storage and no public mutation or
  empty-capable constructor;
- no libaryon artifact/capacity/plot vocabulary enters theme resolution; and
- no Polariton production file mentions `categorical_palette` or
  `ResolvedCategoricalPalette`.

Use the unchanged existing snapshots as the primary compatibility proof;
source scans are reintroduction guards, not substitutes. Record the permitted
file census and verify that no table snapshot changed.

PRODUCT-0028 gates: 9, 10, 11.

Commit: `test: guard categorical palette boundaries`.

## Evidence

Write
`plans/product/impl/0028_optional_categorical_theme_palettes_evidence.md` with
the Stage 0 findings, exact curated capability declarations, non-empty type
evidence, categorical snapshot census, projection-local modifier examples,
unchanged PRODUCT-0024 and PRODUCT-0026 oracle results, eleven-gate ledger,
permitted-file census, commit trace, and full verification.

Advance the corpus with an accepted PRODUCT-0028 substrate entry without adding
or accepting a plot palette row. Accept the ADR only after all automated gates
pass. PRODUCT-0029 remains the first categorical plot consumer.

Commit: `evidence: close optional categorical theme palettes`.

## Verification

Run at minimum:

```text
cargo fmt --all -- --check
cargo test -p libaryon
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
cargo build -p polariton
git diff --check
```

## Gate ledger

| PRODUCT-0028 gate | stage |
|---|---|
| 1 | 1 |
| 2 | 2 |
| 3 | 1 |
| 4 | 1 |
| 5 | 1 |
| 6 | 2 |
| 7 | 2 |
| 8 | 2 |
| 9 | 3 |
| 10 | 3 |
| 11 | 3 |

Every gate is assigned exactly once. Existing PRODUCT-0024/0026 snapshots are
the pre-refactor oracle; no new Stage 1 compatibility bank is necessary.

## Permitted implementation files

```text
crates/libaryon/src/engine/theme.rs
crates/libaryon/src/engine/mod.rs
crates/libaryon/src/engine/snapshots/product-0028-categorical-palettes.txt  (new)
crates/polariton/tests/architecture_test.rs
plans/product/adr/0028_optional_categorical_theme_palettes.md
plans/product/impl/0028_optional_categorical_theme_palettes_evidence.md
plans/product/corpus/rune_workspace_corpus.md
```

Any additional production file triggers the halt and a plan amendment before
work continues.
