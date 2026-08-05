# PRODUCT-0024 implementation evidence

Recorded 2026-08-05 after the pre-refactor oracle, three implementation commits,
and complete cross-feature verification. Resolved libaryon themes now retain the
exact ordered palette from which their themed syntax categories were assigned.
No theme expression changed, no artifact policy entered libaryon, and
PRODUCT-0025 remains the first table projection and Rune surface.

## Stage 0 findings

`THEMED_CATEGORIES` has exactly twenty entries. The registry contains twenty-one
curated and thirty-two continuous families; every curated fill supplies every
themed category, so `[Rgb; 20]` is an honest resolved representation for both
branches. The landed construction path makes missing curated entries a named
internal resolution error instead of padding or weakening the type.

A workspace census found no `Theme` struct literal outside its defining module.
Closing the value with a private retained palette therefore required no consumer
migration. The architecture assay keeps `syntax_colors` public, the palette
private, and direct theme construction owned by `Theme::try_new`.

The pre-refactor oracle is finite and readable. Fifty-three canonical families
and six aliases each contribute the base expression, independent `r`, `c`, and
`C`, and shifts `s0` through `s19`; four existing composed expressions complete
the 1,420-line census. Each line records all twenty-six syntax slots as named
hex-or-absent entries. The checked-in file contains 612,911 bytes and is compared
directly through `include_str!`, not summarized by a digest or rebuilt through a
second resolver.

PRODUCT-0023's compositing seam carries normalized fractional channels.
`Srgb::new` accepts finite inclusive `[0, 1]`, rejects rather than clamps every
invalid channel, and canonicalizes negative zero. Polariton still owns alpha
compositing, constructs `Srgb` directly from the fractional result, and delegates
only generic luminance and contrast math. The seven established default ratios
remain:

```text
cell / even row             15.401817251018779:1
cell / odd row              13.158388521894944:1
header / header background  14.950831546071543:1
type / header background     8.139026492916567:1
gutter / gutter background  14.950831546071543:1
cell / selected even row    12.817453560238190:1
cell / selected odd row     10.921309527435287:1
```

## Modifier matrix

The unchanged syntax snapshot and the exposed palette tests jointly prove:

| modifier | continuous | curated |
|---|---|---|
| `r` | exact sequence reversal | exact sequence reversal |
| `sN` | exact right rotation modulo twenty | exact right rotation modulo twenty |
| `c` | low-domain clipping before sampling | preserved no-op |
| `C` | high-domain clipping before sampling | preserved no-op |

Every registered family is checked for every shift from zero through nineteen.
`terrain_c` and `terrain_C` prove continuous set changes. `nord_c == nord` and
`nord_C == nord` preserve the curated behavior as a known, unendorsed no-op.

## Gate ledger

1. **The resolved value is fixed, complete, and closed.**
   `ResolvedThemePalette([Rgb; 20])` has private storage, immutable indexed and
   whole-array observation, and no public constructor. `Theme` retains it
   privately; the architecture census rejects external literals.
2. **Every family resolves completely.** All twenty-one curated and thirty-two
   continuous families return one twenty-color palette; curated extraction fails
   rather than inventing a missing value.
3. **Syntax compatibility is byte-exact.** The 1,420-expression, full-array
   textual snapshot captured in `463ad2b5` is unchanged after `5581f6ad`.
4. **Palette and syntax share one sequence.** Every census expression asserts
   equality at all twenty themed-category positions.
5. **Modifier behavior is unchanged and observable.** All families prove exact
   reverse and shift permutations; terrain and nord prove the clip applicability
   matrix, including the preserved curated no-op.
6. **Generic color math has one exact authority.** `Srgb` validates inclusive
   endpoints, every refusal class, and negative-zero bits/hash; black/white,
   equal, transfer-boundary, fractional, symmetry, and reference vectors cover
   the formula. `Rgb` delegates, and Polariton's non-byte-aligned composite assay
   plus seven prior ratios prove no quantization entered the handoff.
7. **Pair selection is deterministic.** Synthetic duplicate extrema prove the
   greatest ratio, lexicographically earliest source pair, darker/lighter
   orientation, and returned index/color correspondence.
8. **Flat palettes remain valid.** A uniform synthetic palette returns indices
   zero and one at `1.0:1`, with the lower index in the darker slot; no threshold
   or refusal enters theme resolution.
9. **Existing consumers remain unchanged.** Production syntax users still read
   `syntax_colors`; the complete libaryon and Polariton suites preserve theme
   selection, persistence, aliases, rendering, `UiPalette`, and focus behavior.
10. **No artifact or surface work escaped the substrate.** Changes are confined
    to core color math, theme resolution/modeling, Polariton's existing contrast
    assay, and architecture/evidence files. No table projection, alpha operation
    in libaryon, Rune grammar, configuration, plot, execution, protocol, panel,
    placement, or dashboard production path changed.

## Commit and scope trace

- `af4158c4` — PRODUCT-0024 record and implementation plan;
- `463ad2b5` — complete pre-refactor syntax oracle and modifier observations;
- `69f25294` — validated normalized sRGB and exact Polariton delegation;
- `3b5c4939` — fixed palette and deterministic indexed contrast pair;
- `5581f6ad` — one retained palette shared with syntax projection.

The implementation touched only the permitted libaryon color/theme modules,
the permitted Polariton table contrast assay, the libaryon architecture test,
and the checked-in snapshot. `../uat/test8.rn` remains intentionally ahead of
the implementation for PRODUCT-0025's real-window judgment.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p libaryon
1424 passed; 22 ignored; 14 architecture tests and quit-latency test passed

cargo test -p polariton --features toron
300 passed; 5 live-service tests ignored; 24 architecture tests passed

cargo check -p polariton --no-default-features
passed (the existing feature-disabled styled-plot warnings remain)

cargo build -p polariton
passed

git diff --check
passed
```

All ten PRODUCT-0024 gates are discharged.
