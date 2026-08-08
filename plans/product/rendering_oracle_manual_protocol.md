# Production-renderer manual protocol

PRODUCT-0057 defines this as the explicit fallback when a backend-gap probe
cannot run through the checked-in headless iced/wgpu harness. It is not an
ignored-test waiver and is not ordinary green coverage. Implementation evidence
must name the unavailable probe, record this protocol's observation, date,
active adapter/backend, antialiasing state, viewport, and inspector.

The 0057 implementation automates all four backend-gap probes on the reference
machine. These entries remain the governed fallback for an environment or a
future primitive that cannot reproduce one of their production paths.

## Synthetic fixtures

Use scale factor `1.0`, an opaque black background, and the production iced/wgpu
renderer. Inspect both `128×128` and `384×384`; coordinates below are stated for
the smaller viewport and scale by three for the larger.

| Fixture | Configuration and controls | Required observation |
| --- | --- | --- |
| Parent/child overlap | Green parent rectangle `(20,20)–(90,90)` authored first; red clipped child `(50,50)–(120,120)` authored second; MSAAx4 | `(30,30)` is green, `(105,105)` is red, and `(60,60)` records the realized composite winner. Under iced/wgpu 0.14 on the reference adapter the overlap is green, reproducing the premise repaired by `22d14de0`; the paired SVG remains green-then-red in authored order. |
| Clip enforcement | Green rectangle `(20,30)` size `80×80`, translated by `(10,-5)`, with clip `(40,40)` size `40×40`; repeat with the clip removed | `(60,60)` is green. `(35,60)`, `(85,60)`, `(60,35)`, and `(60,85)` are black only in the clipped rendering and green in the no-clip control. |
| MSAA | White three-pixel diagonal `(16,20)→(112,104)` on black, once with AA disabled and once with MSAAx4 | The whole-image intermediate-value census distinguishes the states: the disabled edge is hard and MSAA produces a substantial blended population. Repeat with a white rectangle at integer coordinates `(32,32)` size `64×64`; that control remains a hard edge in both states. A plot-derived bar is not an admissible substitute. |
| `margin` text | DejaVu Sans Mono bold, `14px`, basic shaping, `Wrapping::None`, eight-pixel horizontal padding; compare a generous `96px` cell with a deliberately narrow `28px` cell | The accepted case has no ink in the region below its `32px` header line or in the neighboring-cell region. The narrow control has a detectably different realized ink extent. Do not infer either result from `CHAR_ADVANCE`, string length, or SVG origins. |

## Reading-order correspondence control

Open `/home/me/work/uat/test19.rn` at a `1600×900` application viewport with
iced/wgpu and MSAA enabled. Record the active table data and palette. The y-axis
must read East, North, South, West from top to bottom, and the bar occupying each
labeled band must encode that region's unique value. Within every cluster the
marks must read Atlas, Beacon, Cinder, Delta top to bottom in the same order as
the legend.

This is the assertion-gap control, not a reason to require raster evidence:
PRODUCT-0056's asymmetric draw-operation assertions already contain and join
the authoritative label, position, identity, color, and value facts. The manual
entry exists so a live exhibit checklist covers all five historical specimens.

## Evidence form

```text
date / inspector:
adapter / backend / driver:
viewport / scale factor / AA:
probe disposition and reason headless automation was unavailable:
positive observation:
negative-control observation:
linked implementation evidence:
```
