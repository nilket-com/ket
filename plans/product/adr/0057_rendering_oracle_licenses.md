# ADR 0057: rendering-oracle licenses

Status: accepted 2026-08-08. This record classifies which visible claims the
backend-independent draw-operation stream can prove, assigns each rendering
oracle an explicit license, and requires production-renderer evidence for
properties that do not survive into that stream. It adds no Rune surface and
changes no product pixels.

## Context

Polariton has deliberately concentrated rendering behavior behind
`PlotBackend`. Its SVG implementation has consequently become both an export
format and the plotting arc's most productive test oracle. Exact SVG and
named recorder assertions have banked geometry, order, color, text, clipping
structure, and cross-component correspondence cheaply and deterministically.
Those instruments are not interchangeable: each license is limited to the
schema that particular oracle actually retains.

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
The clip, MSAA, and text-fit truths are produced below or beside that stream
and cannot be recovered by a sharper SVG assertion. The z-order specimen
contains one backend-gap property and one assertion-gap property: iced/wgpu's
winner for a deliberately hazardous parent-fill/pasted-child structure is a
backend characterization, while the production facet invariant that no such
structure is emitted is fully present in SVG clip nesting.

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

The pre-acceptance feasibility spike proved the production-renderer seam.
`iced_wgpu` 0.14 exposes public `Engine::new` and `Renderer::new` constructors
plus the `renderer::Headless::screenshot` path. `Engine::new` accepts
`Option<Antialiasing>`, so the same public renderer can be constructed with
antialiasing disabled and with `MSAAx4`; using the convenience
`Headless::new`, which hard-codes `MSAAx4`, cannot discharge that comparison.

The designated render-evidence environment is host `slim`, Intel(R) UHD
Graphics 770 (vendor `0x8086`, device `0x4680`), Vulkan, Intel open-source Mesa
driver 26.0.3-1ubuntu1. The pre-acceptance spike acquired that adapter and
captured distinct foreground/background semantic regions through both
antialiasing configurations. It also drove the real `Table::draw` →
`draw_text` → iced/wgpu path for the production `margin` header and observed
233 glyph pixels. These results prove reachability only: they do not discharge
repeat stability, scale stability, MSAA discrimination, or text-fit controls.
No repository CI environment is currently established as equivalent to
`slim`, so the portable workspace suite cannot claim the render-evidence gate.
Adapter acquisition, production table-text reachability, and the named-oracle
enumeration discharge the feasibility spike. Repeated-capture stability,
second-viewport scale stability, probe discrimination, and text fit remain
implementation obligations.

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

The five specimens classify into the following exact properties:

```text
hazardous parent-fill/pasted-child winner     backend gap
production facet emits no hazardous sibling  assertion gap
realized production clipping                 backend gap
MSAA effect on a curved or diagonal edge     backend gap
label-to-payload reading-order correspondence assertion gap
measured text fit, overlap, or wrap           backend gap
```

### 2. Each instrument has a bounded license

Licenses attach to a fully qualified oracle and the schema it actually
records, never to a generic name such as “recorded operations.” An oracle not
named here or in later implementation evidence is unlicensed until its schema
and limits are stated. The implementation and evidence use these licenses:

| Instrument | Licensed to prove | Not licensed to prove |
| --- | --- | --- |
| Source/architecture guard | Ownership, construction boundaries, admitted implementations, and forbidden dependencies | Runtime rendering behavior |
| `plot::common::tests::RecordingBackend` | Recorded stroke-point traces and text objects after accumulated translation | Fills, rectangles, rotation, clip bounds or scope, exact curve geometry, and all production realization |
| The four fully qualified test `RectangleBackend` implementations in `plot::kernels::heatmap`, `plot::kernels::stacked_bar`, `plot::factory::product_0013_baseline_tests`, and `plot::grid` | The rectangle tuples each implementation records today | Strokes, arbitrary filled paths, text, transforms, clip scope, or any claim transferred between the four merely because their short names match |
| `gui::table::tests::RecordingRenderer` | Requested quads and text, text clip bounds, and the active iced layer stack | Shaped glyph extents, rasterized ink, wrap, fit, and ignored transformations |
| `SvgBackend` | Its serialized primitives, authored order, coordinates, paths, colors, text content and origins, transforms, clip declarations, and clip nesting | Production compositing, clip enforcement, rasterization, antialiasing, glyph measurement, shaping, wrap, or fit |
| `PngBackend` and `AvifBackend` | The export renderer's rasterization of `SvgBackend` output | Any additional production-canvas property |
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

Production-path fidelity is part of the property. The clipping fixture
traverses `IcedBackend::with_clip` and iced's real `Frame::with_clip`; the text
fixture traverses the real `Table::draw` and `draw_text` path with the
production font, weight, size, shaping, wrapping, padding, measured width, and
clip bounds. Hand-built renderer primitives cannot substitute for either.
The deliberately synthetic integer-coordinate rectangle required by the MSAA
negative control below is the explicit exception: its purpose is to remove
plot geometry from that control.

### 4. Overlap characterization and the production invariant are separate

The hazardous renderer characterization deliberately constructs a parent
frame with a direct fill beside a pasted `with_clip` child, using the real
`IcedBackend` and iced `Frame` types. It samples both non-overlap controls and
the overlap region. The gate requires an unambiguous winner and records which
primitive won; it does not freeze “parent wins” as a permanent product
expectation. A changed winner after an iced/wgpu update is a finding that
triggers review of the workaround in `plot/grid.rs`, where the canvas
background is emitted as the first clipped child. It does not remove that
workaround automatically.

The product regression property is different: the production facet path must
never emit a parent-level draw beside its clipped children. That assertion gap
is already guarded by
`plot::grid::tests::multi_cell_layers_draw_nothing_outside_clip_groups`, which
walks `SvgBackend`'s `<g clip-path>` depth and fails on any path, rectangle, or
image outside every clip group. A raster probe of the fixed production facet
would be vacuous by construction; the SVG structural guard remains protective
until a successor decision explicitly retires it.

### 5. The MSAA probe uses an edge on which MSAA is observable

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

The harness constructs both renderers through public `Engine::new`, passing
`None` and `Some(Antialiasing::MSAAx4)` respectively. Adapter acquisition and
capture success do not prove that the chosen edge is discriminating. If the
positive fixture and controls are correctly constructed but the semantic
profiles remain nondiscriminating on the designated environment, that is a
recorded implementation finding and takes the named manual disposition; it is
not reported as automated evidence.

### 6. Text fit is observed after production measurement

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
the text probe takes §8's manual disposition rather than replacing measured
evidence with the same `CHAR_ADVANCE` estimate whose truth is in question.
The unresolved `Wrapping::None` behavior remains a separately triggered
product decision.

The pre-acceptance spike proved that the production `margin` path renders
headlessly; 233 observed glyph pixels prove ink, not fit. The accepted-width
case and deliberately narrow control remain load-bearing. If their semantic
regions cannot distinguish the two states after the production path is
reproduced, the probe is nondiscriminating and cannot claim automation.

### 7. Correspondence failures stay at the draw-operation layer

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

### 8. Capture availability is evidence, never a silent pass

Implementation performs windowless iced/wgpu rendering and RGBA capture using
the public headless renderer path. Stability and scale are separate questions.
A probe is captured repeatedly under one identical viewport, adapter, and
configuration to learn whether its bytes repeat. It is then captured at a
second, substantially different viewport to prove that its semantic predicate
survives scale; bytes from different viewports are never compared as a
determinism test.

Availability is recorded independently for each backend-gap probe: overlap,
clip, MSAA, and text fit. Each receives one of three dispositions:

1. automated, with locally repeatable bytes and stable semantic predicates;
2. automated, with varying bytes but stable semantic predicates;
3. manual, because a specifically attempted production path is unreachable or
   its correctly constructed semantic fixture is nondiscriminating.

The first disposition does not authorize exact cross-machine raster
baselines. The first two automate that probe using the tolerant rules in this
record. The third does not skip that probe while reporting green; it invokes
the probe's entry in a named manual protocol with the exact fixture, regions
to inspect, active backend, viewport, and expected observation. A harness may
therefore automate overlap, clip, and MSAA while retaining text fit as a
visible manual obligation. It does not discard working automation merely
because one production path is unavailable.

The disposition is an implementation finding, not a choice made afresh on each
test run. Its checked-in evidence entry records: implementation commit; capture
date; probe and exact boolean property; fully qualified oracle; disposition;
host label; adapter name, vendor and device IDs; backend, driver and driver
version; viewport and scale factor; antialiasing mode; semantic regions,
thresholds, and tolerated variation; positive and negative controls; repeated
capture byte equality as an observation; semantic stability across repeats
and the second viewport; observed outcome; and, for a manual disposition, the
precise unreachable or nondiscriminating cause plus the manual-protocol entry.
The implementation evidence file at
`plans/product/impl/0057_rendering_oracle_licenses_evidence.md` owns this fixed
schema.

Byte stability is recorded, never required. Drivers may vary bytes while the
licensed semantic predicate remains stable. Semantic instability, an absent
control, or a predicate that accepts both expected states is a failed probe.

The designated `product_0057_render_evidence` test target runs on `slim` and
must fail if its adapter is absent. The ordinary portable workspace suite is
not a substitute: it may omit the target through an explicitly named Cargo
feature/required-feature boundary, but no green test may catch adapter failure
or turn it into success. The evidence command and output state whether the
render-evidence target ran. A future CI environment satisfies this gate only
after it is explicitly designated and records its own adapter facts; no such
CI environment exists at acceptance.

Manual evidence is an explicit fallback, not ordinary test coverage. A future
headless-capable environment replaces it with the same semantic probes rather
than retaining both as competing authorities.

### 9. This is governance and test substrate, not a product repair

The record documents the license at the abstraction and export seams where a
new backend test is likely to be written. It does not reopen the five fixed or
bounded defects, mass-convert existing SVG tests, refresh a rendering
baseline, alter application renderer selection, or change product output.

Beginning with this record, implementation evidence for a visible claim names
the exact classified property and the licensed oracle. Existing tests remain
valid for the claims they actually observe. When a future review discovers an
overclaim, it narrows that claim or adds the correct instrument instead of
discarding the exact SVG evidence around it.

## Implementation sequence

1. Add a direct test-only iced_wgpu dependency and a named
   `product_0057_render_evidence` target behind an explicit required feature.
   Build its renderers through public `Engine::new`/`Renderer::new`, not the
   MSAA-hard-coded convenience constructor. On `slim`, draw an opaque interior
   shape, capture it twice under one identical viewport and configuration,
   then once at a second substantially different viewport. Record adapter
   identity, repeated-capture byte stability, and cross-viewport semantic
   stability as separate results. Do not change production renderer setup to
   make the harness succeed.
2. Attempt the hazardous overlap characterization, production-path clipping,
   non-axis-aligned MSAA, and production table text-fit semantic probes
   independently. Establish each probe's discriminating power by deliberately
   exercising both expected states before treating the accepted state as
   evidence, and repeat the stability/scale split from step 1. Separately run
   and retain the existing SVG facet invariant; do not rasterize the fixed
   facet path and call that the overlap characterization.
3. Add the named manual exhibit protocol from §8. A manual disposition is
   justified once by an attempted path that is unreachable or a correctly
   constructed probe that remains nondiscriminating. Automated evidence names
   that finding and its manual entry rather than reporting success. Adapter
   absence on `slim` is a hard failure, not a manual disposition. Preserve
   automation for every independently working probe.
4. Document the licenses at `PlotBackend`, every named test oracle used by the
   record, `SvgBackend`, and the SVG-derived raster exporters. Cite
   PRODUCT-0056's existing joint correspondence oracle and the existing SVG
   facet-clip invariant as assertion-gap examples; do not duplicate either in
   a GPU harness. Enumerate the four module-qualified `RectangleBackend`
   oracles rather than licensing their shared short name.
5. Run the complete workspace suite and confirm that the change is confined
   to test/documentation substrate. No product or banked export output may
   change.

Each probe's capture decision is an implementation boundary. If the public
headless path cannot render a probe's required primitive or production text
path without modifying production behavior or bringing up a real application
window, or if a correctly controlled predicate is nondiscriminating,
implementation records that probe's manual disposition and stops expanding
that probe. “Unavailable” names the exact failed seam; it is never inferred
from an absent adapter or chosen as a per-run fallback. Independently
available probes continue rather than smuggling renderer infrastructure into
this governance record.

## Acceptance gates

1. A classifier table names the exact boolean properties for all five
   specimens, including the facet overlap's separate backend characterization
   and production assertion gap. Classification by feature name alone does
   not discharge this gate.
2. Source documentation at `PlotBackend`, every named recorder used by these
   gates, `SvgBackend`, `PngBackend`, and `AvifBackend` states each oracle's
   schema-bound license. A test or architecture assertion proves that PNG and
   AVIF still derive from the SVG stream. No generic “recorded operations” or
   shared short type name is accepted as an oracle identity.
3. The named render-evidence target captures an identical windowless
   iced/wgpu fixture at least twice under one fixed viewport and configuration,
   then at a second substantially different viewport. It records byte equality
   as an observation and separately requires semantic scale stability. It
   records the full §8 evidence schema. Adapter absence on `slim` fails the
   target; a portable run that does not execute the feature-gated target does
   not claim this gate.
4. The hazardous overlap characterization uses real `IcedBackend`/`Frame`
   types to construct a direct parent fill beside a pasted clipped child.
   Asymmetric non-overlap controls and the overlap region identify an
   unambiguous winner, which evidence records without freezing its identity as
   a permanent expectation. A changed winner triggers review of the named
   `plot/grid.rs` workaround.
5. The production facet invariant remains guarded by
   `multi_cell_layers_draw_nothing_outside_clip_groups`: no path, rectangle, or
   image may occur at SVG clip depth zero outside definitions. A raster of the
   already-fixed production path cannot substitute for this structural gate,
   and the invariant remains until a successor decision retires it.
6. With an automated clip disposition, a transformed clipped primitive travels
   through the real `IcedBackend::with_clip` path and has foreground controls
   inside the clip plus background controls outside every edge. SVG separately
   proves emitted bounds and nesting; iced/wgpu RGBA proves enforcement.
7. With an automated MSAA disposition, public `Engine::new` constructs the
   disabled and `MSAAx4` renderers. The same diagonal or curved edge has a
   distinguishable intermediate-blend profile only when enabled. Its negative
   control is a directly constructed rectangle at integer physical-device
   coordinates under identity transform and scale factor `1.0`; both modes
   remain nondiscriminating there. A correctly constructed but
   nondiscriminating positive fixture takes a named manual disposition rather
   than an automated pass.
8. With an automated text disposition, the actual production `Table::draw`
   path renders `margin` using its production font, weight, size, shaping,
   `Wrapping::None`, padding, measured width, and clip bounds. The
   accepted-width case leaves second-line and neighboring-cell regions free of
   header ink; a deliberately narrow control produces a distinguishable
   extent. The spike's 233 glyph pixels prove reachability only. Recomputing
   expected width from `CHAR_ADVANCE`, SVG origins, or string length cannot
   satisfy this gate.
9. PRODUCT-0056's asymmetric horizontal-bar and heatmap gates remain the
   assertion-gap control: label, identity, position, and payload are joined by
   their specifically named recorders, and no production raster is required.
   A test that merely finds every independent component fails this gate.
10. Every new raster probe proves its fixture falsifiable by exercising both
    expected states, uses semantic regions or profiles rather than complete
    image bytes, and states tolerated variation. Exact RGBA equality is
    recorded for repeated captures but is neither required nor accepted as the
    only assertion.
11. A checked-in manual protocol names only the backend-gap fixtures—hazardous
    overlap, clip, diagonal/curved MSAA, and `margin` text—fixes their viewports,
    states each observation, and records the active backend. Reading order may
    appear as an informational live exhibit but owns no backend disposition.
    Implementation evidence assigns each backend-gap probe one §8 disposition
    and invokes a manual entry only for an implementation finding with a precise
    cause. An ignored, omitted, or conditionally skipped probe with no named
    disposition and evidence fails this gate.
12. No Rune grammar, plot or table geometry, theme, application renderer
    choice, exporter output, screenshot baseline, or existing draw-operation
    baseline changes. The portable full workspace suite and the separately
    named render-evidence target both pass, with premise updates confined to
    comments and test/documentation construction.
13. Implementation evidence uses the fixed §8 schema for overlap, clip, MSAA,
    and text fit and pairs every visible claim with a fully qualified licensed
    oracle. Partial harness support neither overclaims an unavailable path nor
    discards a working probe. Evidence does not generalize an iced/wgpu result
    to tiny-skia, SVG, or future backends.

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
  evidence. Adapter absence on the designated environment is a hard failure;
  a portable suite that omits the explicitly feature-gated target makes no
  render-evidence claim. Manual disposition is reserved for an attempted
  production path that is unreachable or a correctly controlled probe that is
  nondiscriminating.
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
- Named-recorder and SVG assertions become more credible because their claims
  are explicit and schema-bounded rather than treated as approximations of
  pixels.
- The production renderer gains focused probes for the properties only it can
  produce, without creating a cross-driver pixel-baseline obligation.
- A probe must demonstrate that its fixture distinguishes both states. The
  axis-aligned MSAA edge and symmetric correspondence fixture remain named
  examples of mechanically vacuous evidence.
- Text measurement joins GPU compositing as a first-class backend-gap source:
  the draw-operation list can reference content whose realized extent it does
  not measure.
- Every backend-gap probe owns its automation disposition. An unreachable path
  or nondiscriminating controlled fixture can remain a visible manual
  obligation without weakening or discarding independently working automated
  evidence; each obligation disappears when its licensed automated instrument
  becomes available and discriminating.
- The record changes no product output. DB-03 mark-argument bindings landed
  independently while this record was still proposed; neither implementation
  depends on the other.
