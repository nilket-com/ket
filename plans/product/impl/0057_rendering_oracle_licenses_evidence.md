# ADR 0057 rendering-oracle licenses evidence

Prepared 2026-08-08. Original proposal commit `11e945e3`; implementation
commit `1bcfcb84`. The accepted ADR revision, renderer harness, source licenses,
and architecture guard travel together in that implementation cut.

## Execution environment

The designated render-evidence command is:

```text
cargo test -p polariton --test product_0057_render_evidence \
  --features product-0057-render-evidence
```

It ran on:

| Field | Value |
| --- | --- |
| Capture date | 2026-08-08 |
| Host label | `slim` |
| Adapter | Intel(R) UHD Graphics 770 (ADL-S GT1) |
| Vendor/device | `0x8086` / `0x4680` |
| Device type | Integrated GPU |
| Backend | Vulkan |
| Driver | Intel open-source Mesa driver |
| Driver version | Mesa 26.0.3-1ubuntu1 |
| Base viewport | 256 × 192 physical pixels, scale factor 1.0 |
| Scale viewport | 448 × 320 physical pixels, scale factor 1.0 |
| Text viewports | 240 × 96 and 420 × 160 physical pixels, scale factor 1.0 |
| Antialiasing | `MSAAx4`, plus `None` for the MSAA comparison |

The target is an explicitly required-feature, harness-free Cargo test target.
It hard-fails unless it acquires a hardware integrated or discrete GPU through
Vulkan. Host, adapter, and driver identity are recorded facts, not equality
assertions: the semantic probes decide whether rendering is acceptable. The
receipt below is the designated `slim` evidence; a successful run elsewhere is
reproduction evidence and does not replace this receipt without a deliberate
evidence update. The ordinary workspace suite does not execute the target and
makes no render-evidence claim.

### Discriminating red controls

Each accepted probe was temporarily deprived of the distinction it claims to
observe, run to a clean failure, and restored before the green receipt:

| Probe | Temporary sabotage | Named failure |
| --- | --- | --- |
| Overlap | Render the clipped child with the parent's blue | child-only red fraction `0.000` |
| Clip | Route the accepted fixture through the unclipped path | clip-exterior background fraction `0.000` |
| MSAA | Construct both compared renderers with `None` | expected MSAA intermediate run, got `0` |
| Text | Give the narrow control the accepted measured width | accepted/narrow ink both `225` |

No sabotage remains in the working tree. These runs prove the controls reject
the corresponding missing mechanism instead of merely sampling a real raster.

## Probe dispositions

### Hazardous parent-fill/pasted-child winner

- Property: a deliberately hazardous parent frame containing a direct blue
  fill authored before a pasted, clipped red child has one unambiguous realized
  overlap winner.
- Fully qualified oracle: offscreen iced/wgpu RGBA traversing
  `plot::common::IcedBackend` and iced's production `Frame::with_clip`.
- Disposition: automated, locally byte-repeatable.
- Semantic regions: red child-only interior, blue parent-only interior, and the
  overlap region; each control region requires at least 95% of pixels within 12
  channel values of its fixture color.
- Falsifiability controls: asymmetric red-only and blue-only regions prove both
  primitives reached the raster before the overlap is classified.
- Repeated capture: byte-identical at 256 × 192.
- Scale result: the same winner at 448 × 320.
- Observed outcome: `parent fill` won. That identity is characterization data,
  not a permanent expected value.
- Manual cause/protocol: not applicable; protocol entry O below remains the
  explicit fallback if the public production path later becomes unreachable or
  nondiscriminating.

The changed-winner response is review of the clipped-background workaround in
`plot/grid.rs`. The existing structural SVG guard remains protective unless a
successor decision retires it.

### Realized production clipping

- Property: a transformed green primitive is present inside the clip and absent
  outside all four clip edges after iced/wgpu realization.
- Fully qualified oracle: offscreen iced/wgpu RGBA traversing
  `plot::common::IcedBackend::with_clip` and iced `Frame::with_clip`.
- Disposition: automated, locally byte-repeatable.
- Semantic regions: one interior and four exterior regions, each requiring at
  least 95% of pixels within 12 channel values of green or background.
- Falsifiability control: the same transformed primitive rendered through
  `with_save` instead of `with_clip` must be green at all four exterior regions.
- Repeated capture: byte-identical at 256 × 192.
- Scale result: interior and four-edge predicates stable at 448 × 320.
- Observed outcome: every clipped exterior was background; every corresponding
  unclipped control was green.
- Manual cause/protocol: not applicable; protocol entry C is retained.

`plot::grid::tests::multi_cell_layers_draw_nothing_outside_clip_groups`
separately proves the production facet assertion gap by walking SVG clip depth.
The raster probe does not substitute for that structural guard.

### MSAA effect

- Property: `MSAAx4` creates intermediate blends on a diagonal edge that are
  absent with antialiasing disabled, while an integer-device-coordinate
  rectangle remains nondiscriminating.
- Fully qualified oracle: offscreen iced/wgpu RGBA through renderers constructed
  directly with `Engine::new(..., Some(MSAAx4), ...)` and
  `Engine::new(..., None, ...)`.
- Disposition: automated, locally byte-repeatable.
- Semantic predicate: pixels with red channel strictly between 16 and 239 are
  intermediate blends on the black/white fixture.
- Falsifiability controls: disabled diagonal and the direct 128 × 96 rectangle
  at integer physical coordinates under identity transform and scale 1.0.
- Repeated capture: the MSAA diagonal was byte-identical at 256 × 192.
- Scale result: 350 intermediate pixels with MSAA versus 0 without at 256 ×
  192; 580 versus 0 at 448 × 320.
- Observed outcome: the integer rectangle contained zero intermediate pixels in
  both modes and the two rectangle captures were byte-identical.
- Manual cause/protocol: not applicable; protocol entry A is retained.

### Production table-header fit

- Property: the real production `margin` header fits at its measured width,
  paints neither a second line nor the neighboring-cell region, and differs
  from a deliberately narrow control.
- Fully qualified oracle: offscreen iced/wgpu RGBA reached through
  `gui::table::draw_product_0057_table_evidence`, which initializes private
  widget state and calls the ordinary `Table::draw` implementation.
- Disposition: automated, locally byte-repeatable.
- Production inputs: default table skin and font, bold 14 px header text,
  `Shaping::Basic`, `Wrapping::None`, ordinary padding, the production
  `measure_col_width` result, and the production clip bounds.
- Semantic predicate: header ink has maximum channel above 120; accepted text
  must contain ink while the bottom 22% of the header and a 24-pixel region
  beyond the column remain ink-free.
- Falsifiability control: an explicit 28-pixel column width traverses the same
  `Table::draw` path.
- Repeated capture: byte-identical at 240 × 96.
- Scale result: accepted-width predicates and 225 ink pixels remained stable at
  420 × 160.
- Observed outcome: production measured width 70.60 px, 225 accepted-width ink
  pixels, zero second-line pixels, zero neighboring-region pixels, and 88 ink
  pixels in the narrow control.
- Manual cause/protocol: not applicable; protocol entry T is retained.

The feasibility spike's earlier 233-pixel count established reachability only.
The accepted/narrow distinction above is the falsifiability evidence.

## Named-oracle inventory

Source documentation and the architecture guard pin these exact licenses:

- `plot::common::tests::RecordingBackend` records translated stroke points and
  text; it discards fills, rectangles, rotation, and clip bounds and flattens
  `with_clip` into `with_save`.
- `gui::table::tests::RecordingRenderer` records requested quads and text, text
  clip bounds, and the active layer stack; it does not realize glyphs.
- The four module-qualified `RectangleBackend` implementations in heatmap,
  stacked bar, the PRODUCT-0013 factory tests, and grid record rectangle tuples
  only. Their shared short name grants no shared license.
- `SvgBackend` records serialized primitives, order, authored geometry, text,
  transforms, clip declarations, and clip nesting. It does not prove production
  realization.
- `PngBackend` and `AvifBackend` each own a `SvgBackend`, finish that SVG, and
  rasterize it through resvg. The architecture guard pins both derivations.
- Offscreen iced/wgpu RGBA proves only the tested adapter, driver,
  configuration, and semantic predicate.

## Manual fallback protocol

These entries are inactive because all four probes earned automated
dispositions. A future manual disposition must name the precise unreachable or
nondiscriminating cause in this evidence schema and run the corresponding entry
at the fixed base and scale viewports. Failure to acquire a hardware Vulkan
adapter is a hard failure and never invokes this protocol.

- **O — hazardous overlap:** inspect child-only, parent-only, and overlap
  regions; record the one unambiguous winner without prescribing its identity.
- **C — clipping:** inspect the transformed primitive inside the clip and just
  outside each of four edges, then compare the deliberately unclipped control.
- **A — MSAA:** compare the diagonal boundary profile under `None` and `MSAAx4`,
  then verify the integer-coordinate rectangle remains nondiscriminating.
- **T — table text:** inspect `margin` at the production measured width and at
  28 pixels; record glyph ink, second-line ink, and neighboring-cell ink.

Categorical reading order may be displayed as an informational live exhibit,
but owns no backend disposition. PRODUCT-0056's asymmetric recorded-operation
gates remain its authority.

## Gate receipt

Verification on the final three-commit tree described by this evidence record:

```text
cargo test -p polariton --test product_0057_render_evidence \
  --features product-0057-render-evidence                 EXIT=0
cargo test -p polariton --test architecture_test          42 passed / 0 failed
cargo test -p polariton product_0056_                       6 passed / 0 failed
cargo test -p polariton multi_cell_layers_draw_nothing_outside_clip_groups
                                                            1 passed / 0 failed
cargo check -p polariton --all-targets --all-features      EXIT=0 / 0 warnings
cargo check --workspace --all-targets                      EXIT=0 / 0 warnings
cargo test --workspace --no-fail-fast --quiet              EXIT=0
cargo fmt --check                                          clean
git diff --check                                           clean
```

The resolver also exposed `Cargo.lock`'s pre-existing missing dependency edge
from `nilket-local` to `tls-pki`; the manifest already declared that edge. That
unrelated repair travels separately as commit `2642a594`. The ADR 0057
implementation's only lockfile change is Polariton's direct test-only
`iced_wgpu` edge.

| ADR gate | Evidence |
| --- | --- |
| 1 | Exact six-property classifier in the accepted ADR |
| 2 | Source licenses plus `product_0057_rendering_oracle_licenses_are_schema_bound` |
| 3 | Named target, fixed schema, repeated and second-viewport captures above |
| 4 | Real `IcedBackend` hazardous fixture, two non-overlap controls, recorded parent winner |
| 5 | Existing SVG clip-depth guard retained |
| 6 | Real transformed `IcedBackend::with_clip` probe plus deliberately unclipped control |
| 7 | Direct `Engine::new` AA pair, 350/0 and 580/0 diagonal results, identical integer control |
| 8 | Real `Table::draw`, measured/narrow controls, 225/88 ink results |
| 9 | PRODUCT-0056 asymmetric correspondence gates retained |
| 10 | Every raster probe exercises both states; bytes are observations only |
| 11 | Four backend entries above; reading order explicitly informational |
| 12 | Portable workspace suite, workspace all-targets, all-feature Polariton check, and the separately named render target all pass |
| 13 | This fixed-schema record names property, oracle, environment, controls, variation, and outcome per probe |

No product geometry, renderer choice, theme, Rune surface, export baseline, or
banked draw-operation output changed.

## Module-graph maintenance receipt — 2026-08-09

PRODUCT-0062 added the feature-gated `session_persistence` root module. The
binary-only PRODUCT-0057 harness path-includes Polariton's private production
modules, so its manual mirror initially omitted that new declaration and the
opt-in all-feature target no longer compiled even though the default all-target
suite stayed green.

The harness now mirrors `session_persistence` under the same `toron` gate.
`product_0057_render_evidence_mirrors_the_binary_module_graph` compares every
binary-root module name and feature gate with the harness in the ordinary
default architecture suite. The maintenance cost of the path-include
instrument is therefore observable whenever a root module is added, rather
than only when someone next requests GPU evidence.

The repaired target was run, not merely checked. All four automated probes
completed on the Intel UHD 770 Vulkan adapter: overlap and clip predicates held,
MSAA discriminated at 350/0 and 580/0 blended pixels with the integer control
identical, and measured text retained 225 pixels of accepted ink with the
88-pixel narrow control.
