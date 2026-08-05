# The Rune layer-modifier surface

Scope: implement PRODUCT-0018 on the proved PRODUCT-0017 substrate. Deliverable
is two `impl:` commits followed by one evidence-only commit. The ten acceptance
gates are assigned exactly once.

## Halt rule

Stop on any plot renderer, kernel, settings-cascade, Toron, libaryon, dashboard,
execution, identity, or placement change. Permitted production code is the Rune
relation envelope plus the existing Rune-output match needed for its
styled-single carrier. Tests may exercise the host path without changing it.

## Stage 0: feasibility findings

Record before implementation:

1. Rune parses a postfix method as an outer `Expr::Call` whose callee is
   `Expr::FieldAccess` and whose receiver is the prior expression. The complete
   chain can therefore be peeled without string-parsing the root.
2. `LayerSpec.settings` and `PointSize::new` are public typed handoff points;
   the envelope needs no duplicate bound or renderer rule.
3. A modified standalone mark can be represented as an envelope-only styled
   single `LayerSpec`, labeled from its mark, then wrapped in
   `LayeredPlotSpec::new` at the existing result handoff. No plot model or host
   seam changes.
4. Ordinary source outside a proven presentation position never calls the
   contextual sink/layer parser, so it remains Rune without a shadowing pass.

Stop before Stage 1 if any finding fails.

## Stage 1: recognize and flatten the closed register

Add one contextual mark parser shared by layer-list, direct-sink, and figure-body
positions. Peel postfix calls, recognize only `point_size`, reject duplicates,
decode one numeric literal, and validate solely through `PointSize::new`.

Build one `LayerSpec` with flat `LayerSettings`. Refuse non-scatter roots,
three-field scatter, unknown/figure-wide modifiers, computed values, wrong
arity, and invalid factors at the complete modified-mark span. Preserve ordinary
Rune outside proven positions and retain the existing diagnostics for bare marks.

Add the styled-single envelope variant and presentation label. Do not alter
`LayeredPlotSpec`, `AuthoredPlotSpec`, or any settings/rendering file.

Gates: 1, 3, 4, 5, 6, 7.

Commit: `impl: lower Rune point-size layer modifiers`.

## Stage 2: complete styled marks through the proved host path

At the Rune output handoff only, wrap the styled-single layer in a non-empty
`LayeredPlotSpec` and emit the existing layered output. Exercise all three
positions against one materialized result. Render SVG through the production
host-panel completion path: bare and `1.0` must be identical; `2.0` must expose
the proved six-pixel radius.

Rerun named and anonymous views while changing only size and assert exact panel
identity, structural labels, grouping, geometry, title lifecycle, and no
dashboard content. Retain all PRODUCT-0014/0016/0017 regressions.

Gates: 2, 8, 9, 10.

Commit: `impl: complete styled Rune scatter views`.

## Evidence commit

Write `0018_the_layer_modifier_surface_evidence.md` with feasibility findings,
gate ledger, commit/scope trace, exact verification, and the `../uat/test7.rn`
dogfood result. Add corpus v0.20 and make point size executable.

Commit: `evidence: close the Rune layer-modifier surface`.

## Gate ledger

| Gate | Stage | Evidence |
|---|---:|---|
| 1 | 1 | exact typed lowering |
| 2 | 2 | layers, sink, and figure production paths |
| 3 | 1 | literal/boundary/refusal table |
| 4 | 1 | closed register and atomic pruning |
| 5 | 1 | duplicate refusal |
| 6 | 1 | mark/arity controls |
| 7 | 1 | ordinary-Rune retention |
| 8 | 2 | host SVG default and doubled radii |
| 9 | 2 | identity and structural-label lifecycle |
| 10 | 2 | complete cross-feature regression |

Every gate appears exactly once.

## Verification

Before each implementation commit:

```text
cargo fmt --all -- --check
cargo test -p polariton --features toron
cargo check -p polariton --no-default-features
git diff --check
```

Final evidence also runs `cargo test -p libaryon` and launches the built
Polariton binary with `../uat/test7.rn`.
