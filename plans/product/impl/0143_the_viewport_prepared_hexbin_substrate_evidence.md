# PRODUCT-0143 viewport-prepared hexbin substrate — implementation evidence

**Status:** implemented, 2026-08-30.

## Shipped boundary

Polariton now owns one executor-neutral hexbin preparation contract:
`HexLatticeSpec`, `HexbinPreparationRequest`, and the source-, generation-, and
lattice-tagged `HexbinViewportArtifact`. `MaterializedHexbinPreparer` is the one
local executor over opaque, fully validated `HexbinObservations`.
Planned plots thread their retained `ResultArtifact` token through the factory;
only the explicitly fresh-dataframe entry point mints a new publication token.

The common plot and grid paths reserve a source-stable count-guide envelope,
resolve the final plot rectangle, then prepare the viewport before paint,
displayed guide content, or hit-testing. The kernel consumes only the tagged
artifact. Settled frames are exact; moving frames may reuse a prior exact
artifact with hit disabled, and the first moving frame prepares rather than
rendering blank. The current moving preview is frozen plot-local geometry,
clipped to the new plot area; it does not claim the gesture transform deferred
with PRODUCT-0133 stages 3–4. Out-of-order completions are newest-wins.

Hex geometry is pointy-top at a fixed ten logical-pixel radius and plot-local
origin. Preparation and hit-testing share one ties-to-even axial assignment
operation. The old data-space `0.02` radius, Polars expression binner, silent
numeric fallbacks, and cursor-as-center hover are gone.

The count guide has separate reservation and display truth. Reservation uses
the validated source count's decimal digit count at the widest digit advance;
display and paint use the viewport artifact's actual `max_count`. Its rendered
labels are exactly `0` and the unabbreviated integer maximum.

No Rune constructor, mark-field carrier, layer mark, colour-job word, deferred
source, Toron protocol, or remote cancellation path was added.

## Discriminating evidence

- regular radii and side lengths under unequal scales, plus screen-space radius
  invariance across different numeric domains;
- resize and domain rebins, sparse conservation, negative/duplicate/exact-domain-endpoint
  observations, source/lattice identity refusal, and bounded lattice values;
- complete validation before viewport filtering, including null, NaN,
  infinities, inexact integers, incompatible fields, and every temporal dtype
  in both channels;
- actual bin-center hit identity, stale-hit refusal, first-moving-frame exact
  preparation, paint-only cache reuse, and delayed generations completing out
  of order;
- empty/off-viewport guide behavior, actual maximum distinct from source count,
  the power-of-ten and widest-digit envelope traps, and closed integer labels;
- compile-fail opacity for `HexbinObservations`; and
- architecture guards for artifact-only rendering, one contract/executor,
  final-layout ordering, and the absence of premature Rune/Toron surfaces.

## Verification at close

```text
cargo fmt --all -- --check                                      pass
cargo test -p polariton product_0143                            pass
cargo test -p polariton --bin polariton --no-fail-fast          pass
cargo test -p polariton --test architecture_test --no-fail-fast pass
cargo test -p polariton --all-targets --no-fail-fast            pass
cargo check --workspace --all-targets --all-features            pass
cargo test --workspace --all-targets --no-fail-fast \
  -- --test-threads=1                                            pass
```

The workspace retains its pre-existing ignored live/external-fixture tests;
PRODUCT-0143 adds no ignored or tolerated failure.
