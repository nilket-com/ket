# PRODUCT-0020 implementation evidence

Recorded 2026-08-04 after the surface commit, complete cross-feature
verification, and an installed launch of `../uat/test7.rn`. The high-density
fixture now combines `line("x", "y").stroke_width(2.0)` with its existing
maximum-bound `scatter("x", "y").point_size(8.0)` in one headed figure.

## Stage 0 findings

The closed PRODUCT-0018 postfix register admitted a second explicitly named
case. Parsing still peels Rune's typed field-access AST, lowers to one flat
`LayerSettings`, and runs only in proven mark positions. No open modifier map,
runtime method dispatch, or generic PointSize/StrokeWidth abstraction was added.

Gate 6 caught a substrate seam before closure: a styled single line changed
from line-specific to multi-layer scale padding. The PRODUCT-0020 stop condition
fired, PRODUCT-0019 was amended under its own presentation-only invariant, and
the correction landed with the surface commit. This is recorded in both ADRs;
multi-layer domain resolution did not change.

## Gate ledger

1. **Exact typed lowering.**
   `product_0020_stroke_width_flattens_in_every_proven_mark_position` compares
   the unchanged line mark and x/y encoding, inherited point size, and exact
   `Override::Set(StrokeWidth::new(2.0))`.
2. **Every proven position reaches the existing shared-result path.**
   `product_0020_stroke_width_reaches_host_and_combines_with_point_size`
   observes layer-list, direct-sink, and figure-body `LayeredPlot` outputs and
   uses `Arc::ptr_eq` to prove all three retain one stamped result.
3. **Literal, domain, arity, duplicate, and unknown forms refuse atomically.**
   `product_0020_stroke_width_inherits_bounds_and_refuses_atomically` accepts
   `0.25`, `1`, `2.0`, and `8.0`; it refuses missing, extra, computed, negative,
   zero, below-bound, above-bound, duplicate, and unknown forms at the complete
   modified mark.
4. **The register remains mark-compatible and closed.**
   `product_0020_modifier_compatibility_is_closed_and_order_independent`
   refuses stroke width on scatter, point size on line, and incompatible mixed
   chains in both orders. Bare line and scatter controls remain unchanged.
5. **Unproven calls remain Rune.**
   `product_0020_stroke_width_is_ordinary_rune_outside_proven_positions`
   retains binding and argument calls byte-for-byte.
6. **The real host path preserves factor one and paints factor two.**
   The host lifecycle compares bare and `1.0` SVG byte-for-byte and finds an
   exact `stroke-width="4"` after changing the same named view to `2.0`.
7. **Width is neither identity nor presentation labeling.**
   The same assay retains one `HostPanelId`, one panel, and `plot.line` across
   all three revisions, while independently asserting an empty dashboard.
8. **The high-density figure composes the two governed settings.**
   The integration assay retains authored title/subtitle, line-then-scatter
   paint order, exact line/scatter settings on their respective layers, and one
   shared result. The installed `test7.rn` fixture exercises the same shape.

The architecture assay
`product_0020_extends_the_closed_typed_modifier_register` pins the two explicit
spellings, typed constructors and handoff, and the absence of an open modifier
map.

## Commit and scope trace

- `cbd78b70` — accepted PRODUCT-0019/0020 records and implementation plan;
- `ddb46930` and `0d3740ec` — PRODUCT-0019 substrate;
- `01a66f4d` — closed Rune recognition, typed flattening/refusals, production
  host lifecycle, combined modifier assay, and the recorded single-layer
  substrate correction.

No modifier reaches Toron source. No kernel type, prepared-data type, execution,
transport, identity, placement, heading, dashboard, or libaryon production file
changed.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
281 passed; 5 live-service tests ignored; 19 architecture tests passed

cargo check -p polariton --no-default-features
passed with the pre-existing test-only create_layered_plot dead-code warning

cargo test -p libaryon
1411 passed; 22 ignored; 13 architecture tests passed; quit-latency test passed

cargo build -p polariton
passed

git diff --check
passed

target/debug/polariton ../uat/test7.rn
installed fixture launched without a terminal error and was closed deliberately
```

All eight PRODUCT-0020 gates are discharged.
