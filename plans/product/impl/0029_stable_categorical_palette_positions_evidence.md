# PRODUCT-0029 implementation evidence

Recorded 2026-08-05 after five staged implementation commits and full cross-
feature verification. Polariton now has a proved stable-position consumer for
PRODUCT-0028 categorical palettes, while PRODUCT-0030 remains the first Rune and
production caller.

## Feasibility and compatibility oracle

The existing production entrypoint could remain exact while its continuation
representation changed. Before touching production source, commit `3f8c0331`
banked one-line resolved mappings for every admitted legacy cardinality, live
filter/return behavior, and the thirteen-category refusal. Existing vertical and
horizontal SVG baselines independently pinned the real two-field bar draw path.

After the refactor the oracle remains byte-identical:

```text
n=1  c0=#4C78A8FF
n=6  c0=#4C78A8FF,...,c5=#EDC948FF
n=12 c0=#4C78A8FF,...,c11=#FF7F0EFF
live first     a=#4C78A8FF,b=#F28E2BFF
live filtered  b=#F28E2BFF identity=2
live returned  a=#4C78A8FF,b=#F28E2BFF
overflow       declared limit 12
```

`resolve_domains` is visibly documented as the PRODUCT-0029 compatibility
entrypoint. It stores legacy ordinals as positions in a tagged twelve-slot
space, projects the same exact colors, and produces no band fills. Neither Rune,
`plot::request`, nor durable plotting calls the explicit entrypoint.

## Representation and authority

`PalettePosition(usize)` has private storage. `PalettePosition::new` rejects an
index at or beyond the supplied allocation-space length; zero-length and
boundary controls execute this contract. `ColorIdentity` maps its established
field/layer/category key to this type and separately tags each lineage as legacy
or explicit with its natural length. Legacy and explicit spaces, and explicit
spaces of different lengths, cannot mix silently.

`CategoricalPaletteSelection::from_theme_name` is the only production
constructor. It calls `Theme::try_new` once, clones the exact optional
`ResolvedCategoricalPalette`, and distinguishes unknown identity from known
absence. Polariton neither parses modifiers nor accepts an arbitrary color
sequence. Only `plot/spec/mod.rs` mentions this libaryon capability; factory and
kernel receive exact resolved colors.

## Allocation and modifier evidence

Fresh even spread over twenty positions produces the complete ordered mapping:

```text
A ->  0 -> tab20c c01 #3182BD
B ->  4 -> tab20c c05 #E6550D
C ->  8 -> tab20c c09 #31A354
D -> 11 -> tab20c c12 #C7E9C0
E -> 15 -> tab20c c16 #DADAEB
F -> 19 -> tab20c c20 #969696
```

`tab20c_r` and `tab20c_s1` retain every category's position while comparing its
exact color with libaryon's resolved sequence at that position. Reverse proves
modifier observability; shift proves the local resolver did not special-case
reversal.

The six-position minimum adjacent gap is three. The rejected greedy-from-empty
control executes as `0,19,9,14,4,2`, whose sorted minimum gap is two. `n=0`,
`n=1`, and every admitted fresh cardinality are covered.

Continuation is tested through three returned identities, not direct helper
calls:

```text
resolution 1  A -> 0
resolution 2  A -> 0, B -> 19
resolution 3  A -> 0, B -> 19, C -> 9
fresh ABC     A -> 0, B -> 10, C -> 19
```

Filtering `B` out reserves position 19; adding `C` takes 9, and returning `B`
restores both position and exact color.

## Capacity and failure facts

The three independent cases execute before mutation:

```text
dark2  10 categories  geometry 12  palette 8   palette-only
tab20c 13 categories  geometry 12  palette 20  geometry-only
dark2  13 categories  geometry 12  palette 8   both
```

Each `CategoricalCapacityFailure` carries channel, field, layer, role, retained
and requested cardinality, both named limits, and both exceeded flags. The
caller identity remains empty and an admissible follow-up succeeds. Unknown
theme, known `nord` without categorical capability, and present-but-short
`dark2` are three different typed outcomes.

The same short-palette assay over a two-field bar attributes failure to field
`cat` and role `bar band fill`, rather than pretending a Color channel exists.

## Exact band-fill rendering

An explicit two-field six-category bar resolves with no `Channel::Color` and
six `BandFillAssignment`s. The real factory and SVG backend observe fills in
category draw order:

```text
rgb(49, 130, 189)
rgb(230, 85, 13)
rgb(49, 163, 84)
rgb(199, 233, 192)
rgb(218, 218, 235)
rgb(150, 150, 150)
```

The kernel looks up exact resolved assignments by category. It has no `Theme`,
`ResolvedCategoricalPalette`, selection, allocator, or continuation state. Its
legacy branch retains renderer-time `ColorTheme` sampling unchanged. A bar with
an authored categorical color field has channel assignments and no band fills;
uncolored scatter and line have neither repeated treatment nor band fills.

## Gate ledger

1. **Validated position.** Private storage and checked construction reject all
   out-of-range positions.
2. **Continuation/artifact split.** Identity owns positions; resolved specs own
   exact colors and render without identity or palette.
3. **Fresh `tab20c`.** Complete category-position-color mapping matches natural
   c01/c05/c09/c12/c16/c20.
4. **Modifiers.** `_r` and `_s1` retain mappings and consume exact libaryon
   colors.
5. **Fresh allocator.** Empty, singleton, all admitted sizes, gap-three assay,
   and gap-two greedy counterexample pass.
6. **Live maximin.** Three returned live identities produce 0/19/9; fresh
   control produces 0/10/19.
7. **Filtering.** Reserved absent positions cannot be reused and returning
   categories restore their color.
8. **Independent limits.** Palette-only, geometry-only, and both failures carry
   complete facts and are atomic.
9. **Distinct resolution outcomes.** Unknown, absent capability, and short
   present palette never fall back.
10. **Band-fill draw path.** Six exact SVG fills appear in category order with
    no Color channel or kernel sampling.
11. **Authored Color authority.** An explicit color field suppresses band fill
    and retains `ColorJob::Categorical`.
12. **Other marks/jobs.** Scatter, line, non-categorical jobs, guides, geometry,
    hover, tables, and chrome retain their prior paths.
13. **PRODUCT-0007 regression.** Legacy snapshot, filtering, changed-field,
    capacity, pinned-artifact, renderer-self-sufficiency, and full suites pass.
14. **Boundary.** No libaryon source, Rune, execution, transport, panel,
    placement, dashboard, table, or workspace configuration changed.

## Commit and file trace

```text
baa2a472  plan: product 0029 stable categorical palette positions
3f8c0331  test: bank legacy categorical plot oracle
3d76e06b  impl: retain categorical palette positions
13d9f7f4  impl: resolve stable categorical palettes
d13cde78  impl: render resolved categorical bar fills
94a0aeb6  test: guard categorical palette position boundaries
```

Implementation changes are confined to:

```text
crates/polariton/src/plot/spec/mod.rs
crates/polariton/src/plot/spec/snapshots/product-0029-legacy-categorical.txt
crates/polariton/src/plot/factory.rs
crates/polariton/src/plot/kernels/bar.rs
crates/polariton/tests/architecture_test.rs
plans/product/adr/0029_stable_categorical_palette_positions.md
plans/product/impl/0029_stable_categorical_palette_positions.md
```

The PRODUCT-0007 blanket ban on any libaryon word inside plot specs was updated
because PRODUCT-0029 supersedes that premise. Dedicated PRODUCT-0028/0029 guards
now permit only categorical-palette facts in `plot/spec/mod.rs` and prohibit
palette policy in renderers or production callers. No existing behavioral
expectation changed.

## Verification

```text
cargo fmt --all -- --check                         passed
cargo test -p polariton --features toron           330 passed; 5 live-only ignored
polariton architecture tests                       29 passed
cargo test -p libaryon                             1430 passed; 22 ignored;
                                                   14 architecture + 1 latency passed
cargo check -p polariton --no-default-features     passed (existing warnings)
cargo build -p polariton                            passed
git diff --check                                   passed
```

One libaryon event-bus supersession test timed out during a parallel verification
load. Its immediate isolated rerun passed, and the subsequent complete libaryon
run recorded above passed cleanly. All fourteen PRODUCT-0029 gates are closed;
the explicit path remains unreachable from Rune until PRODUCT-0030.
