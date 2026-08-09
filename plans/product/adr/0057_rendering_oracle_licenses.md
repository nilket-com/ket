# ADR 0057: rendering-oracle licenses

Status: proposed 2026-08-08. This record classifies which visible claims the
backend-independent draw-operation stream can prove, assigns each rendering
oracle an explicit license, and requires production-renderer evidence for
properties that do not survive into that stream. It adds no Rune surface and
changes no product pixels.

## Context

Polariton has deliberately concentrated rendering behavior behind
`PlotBackend`. Its SVG implementation has consequently become both an export
format and the plotting arc's most productive test oracle. Exact SVG and
recorded-operation assertions have banked geometry, order, color, text,
clipping structure, and cross-component correspondence cheaply and
deterministically.

That success can be mistaken for a stronger claim: that SVG is a faithful
oracle for what iced's production renderer finally displays. Five live
specimens bound the mistake.

| Specimen | What the existing oracle contained | What it could not establish |
| --- | --- | --- |
| Facet z-order paste | Parent and clipped-child fills in authored call order | `iced_wgpu` converts a parent frame's direct fills to meshes after pasted clipped children, changing realized composite order |
| `with_clip` | The clip command, bounds, nested operations, and SVG clip group | Whether the production renderer applies the clip after its own transform and compositing rules |
| Canvas antialiasing | The same paths and geometry with MSAA disabled or enabled | MSAA is renderer configuration, absent from the draw-operation stream |
| Categorical-y reading order | Every label position, bar rectangle, legend entry, color, and value | Independent assertions never joined the label to the payload occupying its band |
| Table header fit | `fill_text` content and origin plus the estimated column width | Measured glyph extents, shaping, wrapping, and whether the rendered word fits |

The fourth specimen differs from the other four. PRODUCT-0056's disagreement
between legend and horizontal bars was completely represented in the
draw-operation stream; its gates simply did not ask the relational question.
The z-order, clip, MSAA, and text-fit truths are produced below or beside that
stream and cannot be recovered by a sharper SVG assertion.

The text specimen makes the boundary broader than GPU compositing. The table
width estimate changed `CHAR_ADVANCE` from `0.6` to `0.65` after the header
`margin` visibly became `margi\nn`. The operation oracle observed the new
estimated width but contains no measured glyph extent against which to prove
fit. `Wrapping::None` is already requested in production and the observed
wrap remains unexplained. This record classifies that mystery; it does not
resolve text layout or choose a new wrapping policy.

The raster exporters do not supply an independent answer. `PngBackend` and
`AvifBackend` first render through `SvgBackend`, then rasterize that SVG. They
prove the export pipeline's realization of the SVG stream, not iced/wgpu's
realization of the production canvas.

Stage 0 has a plausible production-renderer seam. `iced_wgpu` 0.14 exposes a
headless renderer constructor and `Renderer::screenshot`, which renders an
offscreen viewport and returns RGBA bytes. A render node is available on the
development machine. Neither fact promises byte stability across adapters,
drivers, or platforms. The feasibility question is therefore whether the
production renderer can yield any raster whose semantic regions can be
sampled, not whether it can become another byte-identical baseline.

## Decision

### 1. Classify the exact property before choosing its oracle

Every gate about visible rendering first states the boolean property it needs
to prove, then asks:

> Could two renderers given the same complete backend-independent
> draw-operation list disagree about this property's truth?

If **no**, the evidence survives serialization and a failure is an
**assertion gap**. The draw-operation or SVG oracle is licensed; the test must
ask a stronger or more relational question.

If **yes**, the evidence does not survive serialization and the failure is a
**backend gap**. No stronger assertion over the same list can prove it. The
gate needs an oracle that runs the responsible production backend.

Classification attaches to a property, not to a feature or API. “The clip
command and its expected bounds were emitted” is an assertion-gap property.
“No production pixel outside those bounds is painted” is a backend-gap
property. One record may need both.

The five specimens classify as follows:

```text
realized parent/child compositing order       backend gap
realized production clipping                 backend gap
MSAA effect on a curved or diagonal edge     backend gap
label-to-payload reading-order correspondence assertion gap
measured text fit, overlap, or wrap           backend gap
```

### 2. Each instrument has a bounded license

The implementation and evidence use these licenses:

| Instrument | Licensed to prove | Not licensed to prove |
| --- | --- | --- |
| Source/architecture guard | Ownership, construction boundaries, admitted implementations, and forbidden dependencies | Runtime rendering behavior |
| Recorded `PlotBackend` operations and SVG | Emitted primitives; exact authored coordinates, paths, colors, text content and origins, command order, transforms, clip declarations and nesting; relations among facts present in the stream | Production compositing, clip enforcement, rasterization, antialiasing, glyph measurement, shaping, wrap, or fit |
| SVG-derived PNG/AVIF | The export renderer's rasterization of that SVG | Any additional production-canvas property |
| Offscreen iced/wgpu RGBA | Realized behavior of the tested iced/wgpu backend for semantic regions and boundary profiles under the recorded adapter and configuration | Cross-backend truth, cross-driver byte identity, or untested display integration |
| Named live exhibit | End-to-end behavior of the active application/backend for the inspected fixture | Automated, continuous, or cross-machine coverage |

SVG's license is narrowed, not demoted. It remains the preferred oracle for
everything it actually contains because it is exact, portable, inspectable,
and cheap. Raster evidence is required only after the classifier establishes
a backend gap.

An offscreen iced/wgpu result proves iced/wgpu. It does not prove the
tiny-skia fallback or any future renderer. Evidence records the adapter,
backend, antialiasing configuration, viewport, and sampling rule instead of
calling a backend-specific result universal.

### 3. Backend probes assert semantic regions, not complete images

Production-raster probes do not join the byte-identical SVG baseline
discipline. GPU rasterization may vary across drivers and hardware without
changing the property under test. A backend gate therefore uses fixtures with
wide margins and tolerant semantic predicates:

- interior regions are predominantly the expected fill;
- exterior or clipped regions remain predominantly the background;
- overlap regions identify which primitive won compositing;
- boundary profiles distinguish a hard transition from intermediate blended
  samples without pinning exact channel bytes.

Thresholds derive from the fixture's own foreground, background, and control
regions. A single magic pixel, an exact RGBA image, or a tolerance broad
enough to admit both expected states does not discharge a backend gate.

Every probe contains a positive and a negative control. The controls prove
that the selected specimen and sampling rule can distinguish the property's
two states. A test that passes identically when the mechanism is enabled and
disabled is vacuous even if it samples the production raster.

### 4. The MSAA probe uses an edge on which MSAA is observable

The antialiasing probe renders the same diagonal line or curved scatter mark
with iced/wgpu antialiasing disabled and with `MSAAx4`. It samples a profile
normal to the edge and requires the enabled rendering to contain a bounded
run of intermediate foreground/background blends that the disabled rendering
lacks. Interior and exterior controls prove that the profile crosses the
intended mark.

An axis-aligned plot-derived bar or rectangle edge cannot discharge this
gate. Fractional band, `bar_width`, and `category_gap` arithmetic commonly
places those edges between device pixels, where MSAA may produce a blend even
though the edge is axis-aligned. The harness therefore constructs its negative
control directly: a rectangle whose edges are explicit integer physical-device
coordinates under an identity transform and scale factor `1.0`. Enabled and
disabled MSAA both produce the same hard step there, and the gate asserts that
the chosen boundary predicate cannot distinguish the two renderings. This
prevents either a fractional plot edge or the horizontal-bar exhibit from
becoming an apparently relevant but mechanically ambiguous control.

### 5. Text fit is observed after production measurement

Text content, origin, authored size, and estimated cell width remain valid
draw-operation claims. Whether shaped glyphs fit, overlap, clip, or wrap is a
backend claim because the operation stream does not contain the measurement
that decides it.

The production-raster text probe uses the known `margin` header and the
production font, weight, size, padding, and `Wrapping::None` configuration. A
generous accepted-width case must leave the second-line and neighboring-cell
regions free of header ink. A deliberately narrow control must produce a
detectably different extent or overflow region under the backend's actual
behavior. The gate observes the outcome; it does not prescribe whether the
narrow case wraps, clips, or overflows.

If the headless harness cannot reproduce or robustly sample production text,
the text probe takes §7's manual disposition rather than replacing measured
evidence with the same `CHAR_ADVANCE` estimate whose truth is in question.
The unresolved `Wrapping::None` behavior remains a separately triggered
product decision.

### 6. Correspondence failures stay at the draw-operation layer

PRODUCT-0056 is the worked assertion-gap specimen. Its labels, rectangles,
colors, legend entries, and unique payloads all survived serialization. The
repair was a joint assertion that the band labeled East contains East's
unique bar, and that each series occupies the same top-to-bottom position as
its named legend entry. A GPU screenshot would have been more expensive and
no more authoritative for that relationship.

Future correspondence gates use asymmetric payloads and join identity,
position, and content. Independent presence assertions, equal-valued bars,
and symmetric matrices are rejected as fixtures whenever reversal or
misassociation is the defect class.

### 7. Capture availability is evidence, never a silent pass

Stage 0 attempts a windowless iced/wgpu render and RGBA capture using the
public headless renderer path. Stability and scale are separate questions. A
probe is captured repeatedly under one identical viewport, adapter, and
configuration to learn whether its bytes repeat. It is then captured at a
second, substantially different viewport to prove that its semantic predicate
survives scale; bytes from different viewports are never compared as a
determinism test.

Availability is recorded independently for each backend-gap probe: overlap,
clip, MSAA, and text fit. Each receives one of three dispositions:

1. automated, with locally repeatable bytes and stable semantic predicates;
2. automated, with varying bytes but stable semantic predicates;
3. not automatable in the harness because its adapter, primitive, font,
   shaping, or widget path is unavailable.

The first disposition does not authorize exact cross-machine raster
baselines. The first two automate that probe using the tolerant rules in this
record. The third does not skip that probe while reporting green; it invokes
the probe's entry in a named manual protocol with the exact fixture, regions
to inspect, active backend, viewport, and expected observation. A harness may
therefore automate overlap, clip, and MSAA while retaining text fit as a
visible manual obligation. It does not discard working automation merely
because one production path is unavailable.

Manual evidence is an explicit fallback, not ordinary test coverage. A future
headless-capable environment replaces it with the same semantic probes rather
than retaining both as competing authorities.

### 8. This is governance and test substrate, not a product repair

The record documents the license at the abstraction and export seams where a
new backend test is likely to be written. It does not reopen the five fixed or
bounded defects, mass-convert existing SVG tests, refresh a rendering
baseline, alter application renderer selection, or change product output.

Beginning with this record, implementation evidence for a visible claim names
the exact classified property and the licensed oracle. Existing tests remain
valid for the claims they actually observe. When a future review discovers an
overclaim, it narrows that claim or adds the correct instrument instead of
discarding the exact SVG evidence around it.

## Stage 0 and implementation sequence

1. Build the smallest test-only headless iced/wgpu harness through the public
   renderer constructor and screenshot path. Draw an opaque interior shape,
   capture it twice under one identical viewport and configuration, then once
   at a second substantially different viewport. Record adapter identity,
   repeated-capture byte stability, and cross-viewport semantic stability as
   separate results. Do not change production renderer setup to make the
   harness succeed.
2. Attempt the overlap, clipping, non-axis-aligned MSAA, and text-fit semantic
   probes independently. Establish each available probe's discriminating
   power by deliberately rendering both expected states before treating the
   accepted state as evidence, and repeat the stability/scale split from step
   1 for that probe.
3. Add the named manual exhibit protocol from §7. For each probe whose
   production path is unavailable to the harness, make automated evidence
   report that probe's unavailability and invoke its manual entry rather than
   reporting success. Preserve automation for every other probe.
4. Document the licenses at `PlotBackend`, `SvgBackend`, and the SVG-derived
   raster exporters. Cite PRODUCT-0056's existing joint correspondence oracle
   as the assertion-gap example; do not duplicate it in a GPU harness.
5. Run the complete workspace suite and confirm that the change is confined
   to test/documentation substrate. No product or banked export output may
   change.

Each probe's capture decision is a Stage 0 boundary. If the public headless
path cannot render a probe's required primitive or production text path
without modifying production behavior or bringing up a real application
window, implementation records that probe's manual disposition and stops
expanding that probe. It continues with every independently available probe
rather than smuggling renderer infrastructure into this governance record.

## Acceptance gates

1. A classifier table names the exact boolean property for all five specimens
   and derives four backend gaps and one assertion gap using §1's question.
   Classification by feature name alone does not discharge this gate.
2. Source documentation at `PlotBackend`, `SvgBackend`, `PngBackend`, and
   `AvifBackend` states their licenses. A test or architecture assertion proves
   that PNG and AVIF still derive from the SVG stream; rasterizing SVG cannot
   be cited as production-canvas evidence.
3. Stage 0 captures an identical windowless iced/wgpu fixture at least twice
   under one fixed viewport and configuration to test byte stability, then at
   a second substantially different viewport to test semantic scale
   stability. It records the renderer, adapter, antialiasing configuration,
   and both results separately. Comparing bytes from different viewport sizes
   cannot discharge determinism. No adapter or capture failure is reported as
   a passing backend probe.
4. With an automated overlap disposition, an asymmetric parent/child fixture
   samples non-overlap controls and the overlap region to prove the realized
   iced/wgpu composite winner. The unchanged SVG assertion separately proves
   authored command order; neither is allowed to stand in for the other.
5. With an automated clip disposition, a transformed clipped primitive has
   foreground controls inside the clip and background controls outside every
   edge. The draw-operation test still asserts the emitted clip bounds and
   nesting, while the raster test proves iced/wgpu enforcement.
6. With an automated MSAA disposition, the test renders the same diagonal or
   curved edge with antialiasing disabled and with `MSAAx4`, then distinguishes
   their boundary profiles under a stated tolerance. Its negative control is
   a directly constructed rectangle at integer physical-device coordinates,
   with an identity transform and scale factor `1.0`; enabled and disabled
   profiles remain nondiscriminating. A plot-derived edge, fractional
   coordinate, or bar-only fixture cannot satisfy this gate.
7. With an automated text disposition, the production `margin` header fixture
   proves accepted-width fit by observing no ink in second-line and
   neighboring-cell regions. A deliberately narrow control produces a
   distinguishable realized extent. Recomputing expected width from
   `CHAR_ADVANCE`, SVG text origins, or string length cannot satisfy the gate.
8. PRODUCT-0056's asymmetric horizontal-bar and heatmap gates remain the
   assertion-gap control: label, identity, position, and payload are joined
   using recorded operations, and no production raster is required. A test
   that merely finds every independent component fails this gate.
9. Every new raster probe proves its fixture falsifiable by exercising both
   expected states, uses semantic regions or profiles rather than complete
   image bytes, and states which variation is tolerated. Exact cross-adapter
   RGBA equality is neither required nor accepted as the only assertion.
10. A checked-in manual protocol names the overlap, clip, diagonal/curved
    MSAA, `margin` text, and reading-order fixtures; fixes their viewports;
    states each observation; and records the active backend. Implementation
    evidence assigns an independent automated-or-manual disposition to every
    backend-gap probe and invokes the manual entry only where its production
    path is unavailable. An ignored or conditionally skipped probe with no
    named disposition and evidence fails this gate.
11. No Rune grammar, plot or table geometry, theme, application renderer
    choice, exporter output, screenshot baseline, or existing draw-operation
    baseline changes. The full workspace suite passes with every premise
    update, if any, confined to comments and test-harness construction.
12. Implementation evidence reports §7's disposition separately for overlap,
    clip, MSAA, and text fit, and demonstrates that each visible claim added by
    this record is paired with an oracle licensed to prove it. Partial harness
    support neither overclaims an unavailable path nor discards a working
    probe. Evidence does not generalize an iced/wgpu result to tiny-skia, SVG,
    or future backends.

## Stop condition

> This record may add test-only production-renderer capture, tolerant semantic
> probes, source documentation, and a manual fallback protocol. A probe that
> would require changing production rendering, opening an application window,
> selecting a different application backend, introducing exact GPU raster
> baselines, or deciding text wrap/clip/overflow policy takes the manual
> disposition instead. If implementation attempts to cross that boundary,
> work stops and the requirement returns as its own substrate or product
> record; independently available probes continue. No existing product pixel
> may be refreshed under this record.

## Rejected

- **Treat SVG as universal rendering truth:** confuses emitted intent with
  production realization and cannot recover facts absent from the stream.
- **Abandon SVG for screenshots:** throws away the exact, portable oracle for
  the much larger assertion-gap class and makes correspondence harder to
  inspect.
- **Treat PNG or AVIF export as an independent backend:** both inherit SVG's
  information boundary before rasterization.
- **Classify an entire feature at once:** clipping alone contains both an
  emitted-command property and a realized-pixel property with different
  answers.
- **Require deterministic headless bytes:** asks drivers to provide a promise
  the observed backend-gap properties do not need. Semantic regions and
  boundary profiles are sufficient.
- **Probe MSAA with a plot-derived axis-aligned bar:** its fractional band
  arithmetic may make the edge discriminating for reasons unrelated to the
  intended negative control. Only a directly constructed, integer-device-
  coordinate edge establishes the deliberately hard step.
- **Use one permissive pixel tolerance:** a tolerance that admits foreground
  and background equally turns every backend probe into a pass. Fixtures own
  explicit controls and thresholds.
- **Infer text fit from `CHAR_ADVANCE`:** restates the estimate under test and
  never observes shaping or measured glyph extent.
- **Resolve `Wrapping::None` here:** the mystery is a backend-gap specimen, not
  evidence for a particular product policy or implementation repair.
- **Silently skip without an adapter:** converts absent evidence into green
  evidence. The fallback must be named, manual, and reported as weaker.
- **Make manual screenshot review the default oracle:** preserves the luck
  that found the five specimens instead of making the instrument boundary
  explicit.
- **Retrofit every historical record:** accepted SVG gates retain their valid
  claims. Overclaims are corrected when identified, and future visible claims
  apply the classifier at authoring time.
- **Fold DB-03 mark-argument bindings into this record:** that is the next
  language-surface decision and has no rendering-oracle dependency.

## Consequences

- A future gate can select its instrument by answering one operational
  question instead of re-arguing “SVG versus screenshot” from precedent.
- Draw-operation and SVG assertions become more credible because their claims
  are explicit and bounded rather than treated as approximations of pixels.
- The production renderer gains focused probes for the properties only it can
  produce, without creating a cross-driver pixel-baseline obligation.
- A probe must demonstrate that its fixture distinguishes both states. The
  axis-aligned MSAA edge and symmetric correspondence fixture remain named
  examples of mechanically vacuous evidence.
- Text measurement joins GPU compositing as a first-class backend-gap source:
  the draw-operation list can reference content whose realized extent it does
  not measure.
- Every backend-gap probe owns its automation disposition. Missing headless
  text support can remain a visible manual obligation without weakening or
  discarding automated overlap, clipping, and MSAA evidence; each obligation
  disappears when its licensed automated instrument becomes available.
- The record changes no product output. DB-03 mark-argument bindings remains
  the next proposed language work after this oracle boundary is accepted.
