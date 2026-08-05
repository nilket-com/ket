# PRODUCT-0026 implementation evidence

Recorded 2026-08-05 after three implementation commits and complete
cross-feature verification. The table substrate now exposes two explicit
projections of one resolved libaryon palette: the accepted PRODUCT-0025 policy
as `Duotone`, and a new `PaletteForward` capability with measured neutral ink.
Rune still selects Duotone explicitly; PRODUCT-0027 remains the first authorized
PaletteForward caller and owns the default flip plus `.duotone()`.

## Banked compatibility oracle

Before the projection body moved, the implementation enumerated the exact
finite canonical space:

```text
53 theme bases × r × c × C × s0..s19 = 8,480 expressions
```

`product-0026-duotone-table-skins.txt` contains exactly 8,480 newline-terminated
lines. Each success records all twelve role RGBA values as exact channel bytes
and alpha bits; each refusal records the name, weakest role, ratio bits,
minimum bits, and exact displayed diagnostic. The file is 2,414,805 bytes and
remained byte-identical after the one-argument implementation was extracted as
explicit Duotone. The named `terrain_rs10` twelve roles and seven ratios from
PRODUCT-0025 remain unchanged beside that broad oracle.

## Landed palette-forward policy

The policy consumes the resolved palette in final order:

```text
body background       p[0], opaque
header background     p[1], opaque
gutter background     p[2], opaque
even-row background   p[3], opaque
odd-row background    p[4], opaque
gridline              p[5], alpha 0.25
selection fill        p[6], alpha 0.18
scrollbar thumb       p[7], alpha 0.60
near-black ink        #101010
near-white ink        #F5F5F5
```

Cell ink is selected by maximin contrast over ordinary and selected even/odd
rows together. Header, type, and gutter ink use their actual visible surfaces.
Exact ties choose near-black. The completed twelve-role skin is then admitted
only through PRODUCT-0023's unchanged seven post-composite checks.

For `yellows`, the ordered policy produces:

```text
body/header/gutter/even/odd  #FCFCCC #FCF8BB #FCF4AB #FCF19B #FCED8B
grid/selection/scrollbar     #FCE97B@.25 #FCE56B@.18 #FCE25B@.60
all four text roles          #101010

cell / even row             16.48606345091655:1
cell / odd row              15.970546997635292:1
header / header             17.472898926939433:1
type / header               17.472898926939433:1
gutter / gutter             16.91455653525024:1
cell / selected even row    16.204945923105214:1
cell / selected odd row     15.790320818844014:1
```

The same palette truthfully remains unsuitable under Duotone: its anchors are
p[19] `#996600` and p[0] `#FCFCCC` at `4.685107773600702:1`, while the completed
selected-odd role falls to `3.1750332126087506:1`. Thus the differential assay
changes projection mechanism without weakening validation or theme identity.

Across all 8,480 canonical expressions, Duotone accepts 5,355 and refuses
3,125; PaletteForward accepts 5,979 and refuses 2,501. Every one of the 11,334
successes passes all seven exact PRODUCT-0023 checks. Every refusal retains its
mechanism-specific structured facts; neither mode probes or substitutes the
other.

The constructed split-polarity palette uses `#595959` and `#959595`. Duotone's
anchor and weakest completed ratio are both `2.3385374813713655:1`.
PaletteForward reports a cell-group polarity conflict: near-black is limited by
the even row at `2.7164662590863826:1`, while near-white is limited by the odd
row at `2.7474417380531273:1`. Both are below `4.5:1`, proving the two-failure
branch without adding a libaryon palette constructor.

## Gate ledger

1. **Explicit closed selection.** `TableSkinProjection` is exactly
   `PaletteForward | Duotone`, implements no default, and the only public entry
   point requires the projection argument before calling `Theme::try_new` once.
2. **Duotone parity.** The 8,480-line pre-refactor oracle is unchanged, including
   every role, failure fact, and displayed diagnostic. New anchor facts describe
   the same result rather than changing it.
3. **Current callers remain Duotone.** Production `state.rs` has one projection
   call and names Duotone. Exact PRODUCT-0025 result sharing, stale retention,
   identity reuse, fixture completion, and drawing assays remain green.
4. **Palette materially supplies surfaces.** Eight ordered positions reach five
   opaque surfaces and three alpha-bearing roles; `yellows` pins every final role.
5. **Neutral ink is measured.** Tests exercise actual visible surfaces, maximin
   choice, opposite-polarity selection, and deterministic near-black tie-breaking.
6. **`yellows` distinguishes mechanisms.** Its above-threshold anchor pair,
   below-threshold completed Duotone role, and seven valid PaletteForward ratios
   are asserted together.
7. **Both-failure path is real.** The private synthetic assay returns complete,
   distinct Duotone and PaletteForward facts without fallback or a copied pair
   search.
8. **Validation remains universal.** The complete two-mode census validates
   every success through the shared PRODUCT-0023 assay and classifies every
   failure by requested mechanism.
9. **Order remains observable.** `greens`, `greens_r`, and `greens_s1` differ in
   final twelve-role PaletteForward skins, not merely intermediate vectors.
10. **Rune and lifecycle are unchanged.** Architecture guards keep projection
    vocabulary out of the envelope and editor artifact paths, keep
    PaletteForward out of production, and retain SQL/Markdown defaults.
11. **Authority boundaries are intact.** No libaryon source, theme identity,
    color formula, table role, renderer, plot, dashboard, or protocol path
    changed; the complete libaryon suite remains green.

## Commit and scope trace

- `6c811de8` — record handoff and implementation plan;
- `5de8826b` — exact 8,480-expression pre-refactor Duotone oracle;
- `fe72f0b9` — explicit projections, PaletteForward policy, structured facts,
  and unchanged explicit-Duotone caller;
- `296ec1b3` — projection-boundary guards and exact diagnostic regression.

Production changes are confined to `gui/table_skin.rs` and the explicit caller
in `gui/state.rs`. The snapshot and architecture assertions are test evidence.
No Rune envelope, editor handoff, renderer, libaryon, plot, configuration,
execution, panel, placement, or dashboard production file changed.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
317 passed; 5 live-service tests ignored; 26 architecture tests passed

cargo test -p libaryon
1424 passed; 22 ignored; 14 architecture tests and quit-latency test passed

cargo check -p polariton --no-default-features
passed (the existing feature-disabled styled-plot warnings remain)

cargo build -p polariton
passed

git diff --check
passed
```

All eleven PRODUCT-0026 gates are discharged. No source-visible default has
changed: PRODUCT-0027 must add `.duotone()` and switch absent projection to
PaletteForward in one indivisible surface commit.
