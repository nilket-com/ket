# PRODUCT-0014 implementation evidence

Recorded 2026-08-03 after the full verification pass. PRODUCT-0014 remains
proposed; this file records landed implementation evidence and does not sign the
record accepted.

## Gate ledger

1. **Authored order lowers unchanged.**
   `product_0014_layers_lower_in_authored_order` transforms both `[line,
   scatter]` and `[scatter, line]`, extracts the resulting
   `LayeredPlotSpec`, and observes the corresponding `LayerMark` sequence.
2. **One sink remains one view and one panel.**
   `product_0014_named_layer_edits_refresh_one_identity_and_destination`
   observes one presentation value and one panel across three layered
   revisions. `product_0014_layered_view_completes_in_a_label_only_host_panel`
   completes the one layered sink into one `PanelContent::Plot`; no per-layer
   identity or destination type exists.
3. **Empty and nested lists refuse at their own spans.**
   `product_0014_layer_depth_and_admissible_marks_refuse_at_the_inner_span`
   checks the exact source slice for `[]` and for the nested `layers(...)`
   call.
4. **Non-layer sinks refuse at their own spans.** The same test checks exact
   `table()` and `bar("c", "v")` source slices inside a layer list.
5. **Scatter arity is contextual.** `product_0014_scatter_arity_is_contextual`
   asserts the three-field scatter refusal in layer position and, in the same
   test, the established legal three-field scatter view control.
6. **Compatibility crosses the PRODUCT-0013 boundary.**
   `product_0014_compatibility_crosses_the_factory_and_returns_to_the_outer_span`
   first transforms differing x fields successfully, observes a distinct
   `LayeredPlotSpec` request, calls `create_layered_plot`, retains its layer-2
   expected/actual field wording, returns that unchanged error through the real
   completion message, attributes it to the outer `layers(...)` span, and
   observes failed rather than partial plot content.
7. **Outside `.views`, `layers` is ordinary Rune.**
   `product_0014_layers_is_ordinary_rune_outside_a_proven_view_list` retains a
   bare call, a binding initializer, and an unrelated call argument verbatim
   while a separate table view establishes the envelope.
8. **Recognition is outer-attributed and atomic.**
   `product_0014_layers_recognition_is_outer_attributed_and_atomic` checks exact
   spans for wrong outer arity, a non-literal argument, malformed inner marks,
   an unknown constructor, and a valid-prefix/invalid-tail list. Transformation
   returns no artifact for every malformed case.
9. **Named edits retain destination.**
   `product_0014_named_layer_edits_refresh_one_identity_and_destination` runs
   `[line, scatter]`, `[scatter, line]`, and `[line]` under `view("trend", ...)`;
   all three revisions retain the same `LocalViewIdentity` and `HostPanelId`
   while refreshing the display label and content.
10. **One-layer spelling is identity- and render-equivalent.**
    `product_0014_one_layer_spelling_keeps_anonymous_identity_and_svg` replaces
    bare `line(x, y)` with `layers([line(x, y)])` in anonymous slot zero,
    observes the same panel id, and renders both production kernels through
    `SvgBackend` to byte-identical SVG.
11. **Existing sinks retain behavior.** The complete Polariton suite retains
    the PRODUCT-0010 multi-origin, shared-`Arc`, identity, placement,
    failure/staleness, diagnostics, and dashboard regressions, together with
    PRODUCT-0012's line equivalence and contextual refusals.
12. **The head needs no composition and does not touch the dashboard.**
    `product_0014_layered_view_completes_in_a_label_only_host_panel` drives a
    layered kernel into ordinary host plot content and observes empty
    `DashboardState`. `product_0014_host_plot_state_cannot_carry_a_plot_composition`
    exhaustively destructures `HostPlotState`; its type contains label, kernel,
    settings, hover, and render state, with no `PlotComposition` field.
13. **Labels are faithful and old labels are unchanged.**
    `product_0014_layered_view_completes_in_a_label_only_host_panel` observes
    `table`, `plot.bar`, `plot.line`, `plot.scatter`, and exact `Line + Scatter`
    presentations. For the named layered view it separately observes panel
    chrome `trend` and host-canvas state `Line + Scatter`.

## Representation and handoff

`RuneViewIntent` now distinguishes an established single plot from a
`LayeredPlotSpec`. `ViewPresentation { content_kind, display_label }` replaces
the production `RuneViewForm` routing/description coupling: panel routing reads
only `Table | Plot`, identity reads neither field, and layered description comes
directly from `LayeredPlotSpec::title()`.

The envelope recognizes `layers` only after proving `.views([...])` position and
constructs the non-empty specification from its required first mark. It does no
schema or compatibility work. The shared result and specification cross a
distinct sink-output variant into one blocking factory call. Factory failures
return through the destination's revision-checked completion message.

Rune host panels now own `HostPlotState`, which is label-plus-kernel state and
has no authored composition. The dashboard retains `PlotPaneState`,
`PlotComposition`, and all of its existing behavior. No dashboard file,
PRODUCT-0013 model or kernel, `AuthoredPlotSpec`, transport, libaryon protocol,
panel identity, placement, or staleness implementation changed. The halt rule
did not fire.

## PRODUCT-0014 §9a citation trace

Type-oriented citations changed where the new representation made their premise
obsolete:

- `RuneRelationSinkOutcome.form` assertions now inspect `presentation`;
- the host-panel plot-type test helper became a display-label helper because
  routing no longer carries mark identity;
- ignored live Toron tests explicitly match `RunePlotIntent::Single` after the
  intent split.

The old `RuneViewForm` vocabulary remains only under `cfg(test)` for older
behavioral corpus assertions; it is absent from the production normal form. No
behavioral expectation or cited executable fixture was weakened or removed.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
247 passed; 5 live-service tests ignored; 16 architecture tests passed

cargo check -p polariton --no-default-features
passed
```

All thirteen implementation gates are discharged. The implementation landed as
four stage commits: `c3edc520`, `a10b4995`, `56c8cb88`, and `d52fc31d`.

