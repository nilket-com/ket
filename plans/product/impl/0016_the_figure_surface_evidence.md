# PRODUCT-0016 implementation evidence

Recorded 2026-08-04 after the four implementation commits, the complete
cross-feature verification pass, and an installed launch of `../uat/test7.rn`.
The fixture exercises titled/subtitled single and layered figures beside two
tables in ordinary producer-scoped host panels. Automated SVG and real-engine
projection assays are the reproducible visual evidence; the installed launch
completed without a terminal error and was then closed deliberately.

## Stage 0 findings

All three feasible branches held.

1. `HostPanelGroupMember` already carried the complete mutable descriptor.
   `refresh_host_panel_descriptor` updates the backing `HostPanelBuffer` before
   a valid repeated ensure returns, without allocating, changing revision,
   emitting an event, or touching group/layout state. The engine assay covers
   Automatic and Manual groups, retained members, and multiple views of one
   panel buffer.
2. Rune plot intent now holds orthogonal `RunePlotBody` and
   `PlotSettingsLayer` fields. `AuthoredPlotSpec` and `LayeredPlotSpec` were not
   changed.
3. `HostPlotState::new` and `replace` resolve the carried authored layer and
   install display label, kernel, and resolved settings in one synchronous
   transition. Existing stale handling retains the complete prior state.

No second pane tree, protocol command, result transport, kernel prose, or new
settings resolution vocabulary was required. The halt rule did not fire.

## Gate ledger

1. **Layered body and headings lower independently.**
   `product_0016_figure_lowers_body_and_headings_independently` observes ordered
   line/scatter `LayerSpec` values and a separate authored layer whose title and
   subtitle are `Presence::Show`; the structural label remains
   `Line + Scatter`.
2. **A one-body figure is the established view.**
   `product_0016_one_body_figure_is_the_same_view_as_its_bare_mark` compares
   intent, presentation, and named identity. The extended
   `product_0014_one_layer_spelling_keeps_anonymous_identity_and_svg` proves the
   bare and figure single-mark kernels render identically and reuse one
   anonymous `HostPanelId` across bare, layered, and figure spellings.
3. **The bounded grammar refuses atomically at exact spans.**
   `product_0016_figure_refusals_are_inner_attributed_and_atomic` covers wrong
   outer arity, non-literal body list, duplicate headings, missing/two bodies,
   table, nested figure, inner view, heading inside layers, wrong heading arity,
   and computed text. Every assertion compares the refused source slice; no
   transform artifact is returned.
4. **Subtitle-only figures retain title fallback.**
   `product_0016_subtitle_only_is_legal_and_figure_is_contextual` observes
   title `Inherit` and subtitle `Show`. The real completion test renders
   structural `plot.line` plus the subtitle. The application-theme control
   separately proves a theme title wins when supplied.
5. **Chrome and canvas follow different exact chains.**
   `product_0016_figure_headings_reach_canvas_and_source_only_chrome` projects
   actual host descriptors for named+titled, anonymous+titled, and
   anonymous+untitled views, including two `Revenue` positions, then renders
   every completed host canvas through `SvgBackend`.
6. **Subtitle never enters chrome.** The same test projects three distinct
   subtitles on canvas and asserts none occurs in any host descriptor.
7. **Application theme reaches only canvas.**
   `product_0016_application_theme_title_resolves_without_authored_prose`
   resolves and renders `Application title`; `plot_forms_retain_only_view_intent`
   proves the corresponding bare presentation remains `Presence::Inherit`.
8. **Heading edits preserve identity and layout.**
   `product_0016_heading_lifecycle_keeps_destination_layout_and_atomic_canvas`
   records named and anonymous panel ids plus group id, pane id, rectangle,
   tabs, active tab, and focus across title A, failed title B, and heading
   removal. Every observation is exact and unchanged. The engine descriptor
   assay independently proves both Automatic and Manual layout state remain
   unchanged.
9. **Removal restores generated labels without replacement.** Revision C in
   that lifecycle test changes figure forms to bare line forms, retains both
   panel ids, projects `plot.line 1` for the anonymous panel and `stable` for
   the named panel, and renders the generated canvas fallback.
10. **Chrome is source-current; canvas is completion-atomic.** Revision B
    projects `B 1` before its execution failure, marks the existing content
    stale, and continues rendering title/subtitle A with kernel A. Revision C
    installs inherited settings and kernel C together. No state exposes B on
    the canvas.
11. **Presence survives to the existing flattening boundary.** The production
    carrier is `PlotSettingsLayer`; `ViewPresentation.authored_title` is
    `Presence<Arc<String>>`; plot output variants carry the settings beside,
    not inside, both plot specifications. Tables and kernels have no settings
    or prose field. `apply_presence` remains the first existing conversion into
    `ResolvedPlotSettings`' option, including the documented unrepaired Hide
    defect.
12. **Existing behavior remains exact.** The full libaryon and Polariton suites
    retain multi-origin execution and shared-`Arc` identity, every existing
    sink and layered compatibility rule, failure/staleness, descriptor-neutral
    layout, result/dashboard surfaces, and the no-dashboard Rune path. No file
    under `gui/dashboard`, no plot specification/kernel, and no transport file
    changed.

## Commit and scope trace

- `0360146b` — accepted record and implementation plan;
- `4278e762` — descriptor refresh for existing host-panel members;
- `267e48c1` — authored-settings carrier and host resolution seam;
- `a39ffd9d` — contextual figure grammar, lowering, and label chains; and
- `764e2b64` — source-current/stale/removal lifecycle evidence.

PRODUCT-0016 introduced no `Option` carrier. `Presence::Hide -> None -> generated
label` remains the cited known defect and is neither repaired nor accepted by
this work.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p libaryon
1411 passed; 22 ignored; 13 architecture tests passed; quit-latency test passed

cargo test -p polariton --features toron
255 passed; 5 live-service tests ignored; 16 architecture tests passed

cargo check -p polariton --no-default-features
passed (the pre-existing test-only layered-factory dead-code warning remains)

git diff --check
passed

cargo build -p polariton
passed

target/debug/polariton ../uat/test7.rn
installed fixture launched without a terminal error and was closed deliberately
```

All twelve PRODUCT-0016 gates are discharged.
