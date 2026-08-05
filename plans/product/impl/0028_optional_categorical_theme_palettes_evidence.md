# PRODUCT-0028 implementation evidence

Recorded 2026-08-05 after three staged implementation commits and full
cross-feature verification. A resolved libaryon theme now exposes an optional,
naturally ordered categorical palette beside its unchanged syntax-oriented
twenty-color palette. PRODUCT-0029 remains the first authorized plot consumer.

## Feasibility findings carried into the implementation

The curated syntax fills were not categorical palettes in disguise. `tab20c`
declares the natural Matplotlib sequence `c01..c20`, then deliberately maps
syntax roles out of that order: `Keyword <- c01`, `String <- c09`, and
`Comment <- c20`. `dark2` declares eight ColorBrewer colors and adds a ninth
anonymous whitespace neutral only for editor syntax. Reconstructing data colors
from syntax-role order or deduplicating the result would therefore destroy
authorial order or manufacture capacity.

The accepted initial capability matrix is explicit:

```text
continuous families                Some(20 resolved samples)
tab20, tab20b, tab20c              Some(20 natural colors)
kelly, prism                       Some(20 natural colors)
dark2                              Some(8 ColorBrewer colors)
all other curated families         None
```

`ThemeFamily::Curated` and categorical capability are independent facts.
`nord`, `nord_r`, and `nord_s1` remain valid themes whose categorical accessor
returns `None`.

## Representation and single source

`ResolvedCategoricalPalette { first: Rgb, rest: Box<[Rgb]> }` has private
storage and no public constructor or mutation. Public observation is limited to
`len`, checked `color(index)`, and ordered iteration; emptiness is not a
production state.

The six curated declarations use the same private structurally non-empty
declaration shape. Their syntax fill functions now name colors from those
declarations while retaining every previous token-role assignment. In
particular, `tab20c` still maps `String <- c09` and `Comment <- c20`, and
`dark2`'s `Rgb(60, 60, 60)` whitespace color remains outside its eight-color
categorical declaration. Exact hand-authored tests cover every color in all six
base declarations. PRODUCT-0024's syntax snapshot supplies the independent
proof that single-sourcing changed no rendered token color.

## Resolution and modifier evidence

`Theme::try_new` calls the existing `parse_theme_modifiers(name)` exactly once.
Both projections consume that parse, but permutation is local to each resolved
sequence:

```text
dark2          categorical length 8
dark2_s20      categorical rotate-right 20 % 8 = 4
dark2_s20      syntax rotate-right 20 % 20 = 0
terrain_s12    both twenty-position projections rotate-right 12
```

Reverse changes both projections while preserving each one's membership.
Continuous `c` and `C` produce the same clipped accepted samples in both
projections. Curated clipping remains the pre-existing no-op for both present
and absent categorical capability; it is preserved, not newly endorsed.

The new textual oracle contains 1,420 one-line expressions: 962 resolve a
categorical sequence and 458 record `-`. It covers the same canonical names,
aliases, independent modifiers, shifts, and composed controls as PRODUCT-0024.
Because this capability had no prior output, that snapshot is a future drift
oracle rather than initial-correctness evidence; initial correctness comes from
the explicit capability census, exact natural arrays, and direct modifier
equations.

Readable anchors include:

```text
nord       -
dark2      8  #1B9E77 #D95F02 #7570B3 #E7298A #66A61E #E6AB02 #A6761D #666666
tab20c     20 #3182BD #6BAED6 ... #3F007D #969696
terrain_s12 categorical == existing resolved twenty-sample palette
```

## Compatibility and authority boundary

The complete PRODUCT-0024 syntax oracle, palette/category equality, family,
reverse, shift, clipping, alias, case, and error controls pass unchanged. The
PRODUCT-0026 8,480-expression table-skin oracle and all PRODUCT-0023/0025/0027
table regressions also pass unchanged.

Architecture coverage proves that only the accepted six curated specs carry an
explicit declaration, no fill-map inference or deduplication exists, and a
curated family may resolve either `Some` or `None`. Libaryon gained no geometry,
domain, capacity, `ColorJob`, `ColorIdentity`, plot, diagnostic, or artifact
policy. No Polariton production source mentions `categorical_palette` or
`ResolvedCategoricalPalette`; this substrate remains test-only until
PRODUCT-0029.

## Gate ledger

1. **Non-empty immutable value.** Private `first + rest` storage and the public
   read-only API make empty construction and mutation unavailable.
2. **Exact capability census.** Every continuous family has twenty colors;
   exactly the six accepted curated families have the declared natural length.
3. **Natural `tab20c`.** The full `c01..c20` sequence is exposed while its
   syntax-role order remains deliberately different.
4. **Natural `dark2`.** Exactly eight named ColorBrewer colors are exposed;
   whitespace furniture is excluded and syntax remains unchanged.
5. **Other accepted declarations.** Complete exact arrays cover `tab20`,
   `tab20b`, `kelly`, and `prism`.
6. **Reverse.** Each present categorical sequence reverses exactly; absent
   capability remains absent.
7. **Projection-local shift.** `_s1`, `dark2_s20`, and `terrain_s12` prove the
   modifier is applied modulo the sequence being projected.
8. **Clipping applicability.** Continuous projections share clipped samples;
   curated present and absent projections retain the known no-op.
9. **Byte parity.** PRODUCT-0024 and PRODUCT-0026 checked-in oracles pass
   unchanged.
10. **One identity language.** Existing names, aliases, case, modifier parser,
    public accessors, and error strings are unchanged; one parser call serves
    both projections.
11. **No artifact consumer.** Polariton, Rune, plots, tables, execution,
    protocol, panels, placement, and dashboard behavior are untouched.

## Commit and file trace

```text
d02d21e3  plan: product 0028 optional categorical theme palettes
0153dc83  impl: declare optional categorical theme palettes
9685e530  impl: resolve categorical theme palettes
6880046c  test: guard categorical palette boundaries
```

Implementation files are confined to the permitted census:

```text
crates/libaryon/src/engine/theme.rs
crates/libaryon/src/engine/mod.rs
crates/libaryon/src/engine/snapshots/product-0028-categorical-palettes.txt
crates/polariton/tests/architecture_test.rs
```

## Verification

```text
cargo fmt --all -- --check                         passed
cargo test -p libaryon                             1430 passed; 22 ignored;
                                                   14 architecture + 1 latency passed
cargo test -p polariton --features toron           318 passed; 5 ignored;
                                                   28 architecture passed
cargo check -p polariton --no-default-features     passed (existing warnings)
cargo build -p polariton                            passed
git diff --check                                   passed
```

One libaryon event-bus integration test timed out during an intermediate full
matrix run while waiting for a viewport. Its immediate isolated rerun passed;
the final complete libaryon run recorded above was clean. All eleven gates are
discharged without a production caller or any PRODUCT-0029 semantics.
