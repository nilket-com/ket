# PRODUCT-0057 implementation evidence: rendering-oracle licenses

Status: accepted 2026-08-08.

Plan: `11e945e3`
(`plans/product/adr/0057_rendering_oracle_licenses.md`). Implementation ships as
`impl: adr 0057 rendering-oracle licenses` together with this evidence.

## Result

The serialization classifier is now executable governance rather than a
warning about screenshots. `PlotBackend` and `SvgBackend` document exactly what
their command stream can prove; `PngBackend` and `AvifBackend` state and
symbolically test that they rasterize that same SVG stream. Production-only
truth is tested through a test-only iced/wgpu renderer with no surface or
application window.

The headless harness uses semantic points, regions, and whole-image boundary
censuses. Complete RGBA equality appears only as a local repeated-capture
observation under one adapter and configuration; no image is checked in and no
cross-machine byte promise is made. Every probe runs at `128×128` and
`384×384`, scale factor `1.0`, and carries a deliberately opposite control.

All four backend-gap probes receive disposition 1. The checked-in manual
protocol remains available but is not invoked by this implementation. The
categorical-y correspondence specimen remains at the draw-operation layer,
where PRODUCT-0056 already joins the facts that determine its truth.

## Stage 0 disposition

The reference machine exposed three surface-free adapters:

| Backend | Adapter | Type |
| --- | --- | --- |
| Vulkan | Intel(R) UHD Graphics 770 (ADL-S GT1) | integrated GPU, selected |
| Vulkan | llvmpipe (LLVM 21.1.8, 256 bits) | CPU fallback |
| GL | Mesa Intel(R) UHD Graphics 770 | integrated GPU |

The selected adapter created a device without a surface. The durable run
records Mesa `26.0.3-1ubuntu1`; adapter and driver identity are diagnostic
evidence, not assertions. Repeated identical captures were byte-identical both
within one renderer configuration and in Stage 0 across freshly constructed
device/queue/renderer state. A viewport three times larger retained every
semantic predicate; its bytes were never compared with the smaller viewport.

| Probe | Disposition | Positive evidence | Falsifiability control |
| --- | --- | --- | --- |
| Parent/child overlap | 1 — automated, locally stable | At `(60,60)` green wins although the clipped red child was authored second, reproducing iced/wgpu 0.14's parent-mesh completion defect; SVG independently remains green-then-red | Green-only, red-only, and background points name both primitives and the sampled overlap |
| Clip enforcement | 1 — automated, locally stable | A translated oversized child remains green inside `(40,40)–(80,80)` and black outside every edge | Removing the clip makes all four exterior samples green |
| MSAA | 1 — automated, locally stable | Stage 0's whole-image diagonal census separated disabled `0` from MSAAx4 `290` intermediate pixels; the checked-in gate requires a wide population at both scales | The same predicate reports zero for an explicit integer-coordinate rectangle in both AA states; interior/exterior controls cross the mark |
| `margin` text | 1 — automated, locally stable | Production DejaVu Sans Mono, bold weight, size, padding, shaping, and `Wrapping::None` render through iced/wgpu; the generous case leaves second-line and neighbor regions blank | A `28px` cell produces a distinguishably smaller ink extent than the accepted `96px` cell; no `CHAR_ADVANCE` estimate enters the assertion |

`renderer::Headless::new` hardcodes `MSAAx4`, so the implementation constructs
the public `Engine` directly for the AA-disabled control. `Frame::new` is used
through iced's typed canvas wrapper with the production wgpu frame underneath.
The whole-image blend census replaces Stage 0's first one-dimensional transect,
whose `0` versus `1` margin was mechanically true but too weak to govern.

## Classifier and license ledger

| Exact boolean property | Survives a complete draw-operation list? | Class | Licensed oracle |
| --- | --- | --- | --- |
| Which parent/child primitive wins realized compositing | no | backend gap | offscreen iced/wgpu overlap region |
| Whether production pixels outside a declared clip remain background | no | backend gap | offscreen iced/wgpu interior/exterior regions |
| Whether MSAA changes a non-axis-aligned realized boundary | no | backend gap | offscreen iced/wgpu blend census |
| Whether a labeled band contains that identity's asymmetric payload | yes | assertion gap | PRODUCT-0056 joint draw-operation assertion |
| Whether shaped `margin` glyphs fit without second-line or neighbor ink | no | backend gap | offscreen iced/wgpu text regions |

The source comments at all four backend seams carry the same bounds. The
checked-in manual protocol fixes fixtures, viewports, observations, active
backend fields, and controls for every specimen if a future environment cannot
run one production path. It does not convert missing automated evidence into a
pass.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Per-property classifier | The table above applies the serialization question to all five specimens and derives four backend gaps plus one assertion gap | pass |
| 2. Source licenses and SVG-derived raster exports | Rustdoc at `PlotBackend`, `SvgBackend`, `PngBackend`, and `AvifBackend`; `product_0057_raster_exporters_consume_the_svg_stream_symbolically` consumes each private `SvgBackend` member | pass |
| 3. Stability versus scale | `product_0057_headless_wgpu_probes_have_discriminating_controls` compares two identical 128² captures byte-for-byte, then tests semantics separately at 384² and prints adapter/driver identity | pass |
| 4. Realized overlap versus authored order | The headless overlap region records green as iced/wgpu's winner with asymmetric controls; the paired `SvgBackend` assertion records green before authored-second red | pass |
| 5. Clip enforcement | Transformed clipped and unclipped fixtures assert the interior and all four exterior edges at both scales | pass |
| 6. Observable MSAA | Public `Engine::new` creates None/MSAAx4 renderers; diagonal whole-image census distinguishes them while the explicit integer rectangle remains blind | pass |
| 7. Production text | The embedded production font is loaded before renderer construction; accepted and narrow `margin` cases assert realized ink regions without consulting the width estimate | pass |
| 8. Assertion-gap control | PRODUCT-0056's asymmetric bar and heatmap correspondence gates remain unchanged and no GPU duplicate was added | pass |
| 9. Falsifiable semantic predicates | Every raster property renders both states; only local determinism compares complete bytes | pass |
| 10. Named manual fallback | `plans/product/rendering_oracle_manual_protocol.md` fixes five fixtures, viewports, observations, controls, and evidence fields; no entry is invoked because all backend probes automate | pass |
| 11. Zero product pixels | Changes are test substrate, comments, records, and symbolic export architecture coverage; no grammar, layout, renderer selection, exporter behavior, or baseline changes | pass |
| 12. Independent dispositions | Stage 0 table assigns all four probes disposition 1 without generalizing beyond the observed iced/wgpu adapter | pass |

## Verification

- `cargo test -p polariton product_0057 -- --nocapture` — 2 passed.
- `cargo test -p polariton --no-fail-fast` — 480 passed, 5 live tests ignored;
  41 architecture tests and the trybuild pass/fail pair passed.
- `cargo test --workspace --all-targets --no-fail-fast` — passed with no
  failures, including 1,439 Libaryon tests with 22 ignored.
- `cargo fmt --all -- --check` and `git diff --check` — passed.
