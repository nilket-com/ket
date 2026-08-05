# Relative line stroke width and its Rune modifier

Scope: implement PRODUCT-0019, then PRODUCT-0020. Deliverable is two substrate
`impl:` commits, one surface `impl:` commit, and one evidence commit.

## Halt rule

Stop on prepared-data width, new cascade algebra, guide behavior, hover wording,
non-line renderer change, plot-spec migration, open modifier dispatch, Toron or
libaryon change, execution, identity, placement, or dashboard change.

## Stage 0: feasibility findings

1. `LineMarkRenderer` is the single drawing body used by standalone and layered
   line kernels; resolving width there avoids parallel implementations.
2. `MarkRenderer::hit_test` already receives per-layer resolved settings, so the
   visible-width hit rule needs no interaction protocol change.
3. Line has no raster/cache mode and `LinePreparedData` already contains no
   presentation width.
4. PRODUCT-0018's styled-single carrier and contextual parser admit another
   typed `LayerSettings` field without a body, host, or identity change.

## Stage 1: model relative stroke width

Add validated `StrokeWidth`, `ONE`, accepted bounds, and 2px projection. Add
`stroke_width: Override<StrokeWidth>` beside point size in `LayerSettings` and
all five cascade tiers. Preserve Inherit in every existing constructor.

Assay validation, five distinct tiers, selection rather than multiplication,
and two sibling line settings over one base cascade.

PRODUCT-0019 gates: 1, 2, 3.

Commit: `impl: model relative stroke width per plot layer`.

## Stage 2: consume width in line paint and hits

Capture existing standalone/layered line SVG before changing the hardcoded
stroke. Make `LineMarkRenderer` draw from resolved width and compute geometric
hit radius as half the visible stroke plus five pixels. Do not change prepared
data, line legend, hover prose, ordering, or any other renderer.

Assay preparation independence, byte-identical defaults, exact 4px output,
6px/7px miss-hit boundary, and identical legend SVG. A styled one-layer factor
`1.0` must retain the mark-specific prepared layout; only a genuine composition
uses PRODUCT-0013's shared-domain layout.

PRODUCT-0019 gates: 4, 5, 6, 7, 8.

Commit: `impl: resolve line stroke width at render time`.

## Stage 3: extend the closed Rune register

Add `stroke_width` manually beside `point_size` in the contextual parser. Keep
one flat `LayerSettings`; reject duplicates, wrong receiver, computed/invalid
values, and incompatible mixed chains atomically. Preserve ordinary Rune.

Exercise layers, direct, and figure positions through `rune_view_outcome`. Run a
bare/1x/2x named lifecycle through real host SVG and assert identity, exact label,
one panel, and empty dashboard. Extend `../uat/test7.rn` so its layered figure
uses both independent modifiers.

PRODUCT-0020 gates: 1–8.

Commit: `impl: add Rune stroke-width layer modifiers`.

## Evidence

Write separate PRODUCT-0019 and PRODUCT-0020 evidence files, update corpus v0.21,
run all verification, build, and launch `../uat/test7.rn`.

Commit: `evidence: close relative Rune line stroke width`.

## Verification

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
git diff --check
```

Final evidence also runs `cargo test -p libaryon` and the installed fixture.
