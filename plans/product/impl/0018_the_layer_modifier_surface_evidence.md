# PRODUCT-0018 implementation evidence

Recorded 2026-08-04 after the two implementation commits, complete cross-feature
verification, and an installed launch of `../uat/test7.rn`. The fixture already
contained the accepted
`scatter("x", "y").point_size(8.0)` spelling, so it was retained unchanged as
the visible maximum-bound dogfood case rather than rewritten to the ordinary
`2.0` example.

## Stage 0 findings

All four feasible branches held.

1. Rune represents the postfix form as an outer call over `Expr::FieldAccess`,
   whose `expr` is the preceding call. `parse_modified_mark` peels that typed AST
   shape and never parses the constructor from a full source string.
2. The envelope imports PRODUCT-0017's public `PointSize::new`, `LayerSettings`,
   and `Override`; it copies no bound, cascade rule, radius, or renderer logic.
3. `RunePlotBody::StyledSingle(LayerSpec)` is envelope-only. At the existing Rune
   result handoff it becomes `LayeredPlotSpec::new(layer)` and the established
   layered output. No plot specification, kernel, host seam, or dashboard type
   changed.
4. Contextual parsing is called only from a proven view sink, figure body, or
   layer list. The ordinary-Rune assay retains identical postfix calls in a
   binding and nested runtime call while extracting an unrelated table view.

The stop condition did not fire.

## Gate ledger

1. **Exact typed lowering.**
   `product_0018_point_size_flattens_in_every_proven_mark_position` compares the
   scatter mark, unchanged x/y encoding, and exact
   `Override::Set(PointSize::new(2.0))` in layer-list, direct-sink, and figure
   positions.
2. **Every proven position reaches one shared result and layer-aware output.**
   `product_0018_point_size_reaches_every_host_path_and_preserves_the_view`
   observes three `LayeredPlot` outputs and `Arc::ptr_eq` from each sink to the
   single stamped result.
3. **The substrate domain is the surface domain.**
   `product_0018_point_size_uses_the_substrate_domain_without_clamping` accepts
   `0.25`, `1`, `2.0`, and `8.0`; wrong arity, computed, negative, zero, below,
   and above-bound forms all refuse at the complete modified mark.
4. **The register is closed and atomically pruned.**
   `product_0018_modifier_register_is_closed_duplicate_free_and_atomic` refuses
   unknown `.opacity` and figure-wide `.title`; the architecture assay requires
   the closed spelling and typed handoff in the envelope. No recognized
   modifier remains in dispatched source.
5. **Duplicates refuse.** The same register assay refuses two `point_size`
   calls over one mark rather than selecting by order.
6. **Mark and arity controls survive.** The assay refuses a line receiver and a
   modified three-field category scatter at their complete spans, while the
   unmodified category-scatter control retains its established single body and
   category field.
7. **Unproven calls stay Rune.**
   `product_0018_modifier_is_ordinary_rune_outside_proven_positions` retains
   both the binding and argument calls byte-for-byte.
8. **The value reaches the real host renderer.** The production host-panel SVG
   assay proves bare scatter and `1.0` byte-identical and observes
   `A 6.00 6.00` for factor `2.0` through `PlotWidget`'s cascade-aware path.
9. **Size is not identity or description.** The same lifecycle reuses one named
   `HostPanelId` across bare, `1.0`, and `2.0`, keeps exactly one panel, and
   retains `plot.scatter` throughout. The envelope separately proves the direct
   and figure styled-single labels.
10. **The arc remains intact.** The full suites preserve ordered layers,
    headings and Unicode, origin-keyed execution and shared identity, guide
    output, producer tiling, staleness, and no-dashboard Rune completion. The
    new host assay independently asserts empty dashboard state.

## Commit and scope trace

- `e32e0dc4` — accepted PRODUCT-0018 record and implementation plan;
- `5d180e0c` — closed contextual postfix recognition, typed flattening,
  styled-single carrier/handoff, exact refusals, and the superseded PRODUCT-0017
  no-surface architecture premise;
- `0478f8a2` — shared-result, production-host SVG, identity, label, and
  no-dashboard evidence.

Production changes are confined to the Rune envelope and its existing output
match. The Toron adapter change is an exhaustive test-fixture assertion; the
architecture change replaces the deliberately superseded no-surface premise.
No plot substrate, Toron transport, libaryon, dashboard, execution, identity, or
placement production file changed.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p polariton --features toron
269 passed; 5 live-service tests ignored; 17 architecture tests passed

cargo check -p polariton --no-default-features
passed (the pre-existing test-only create_layered_plot warning remains)

cargo test -p libaryon
1411 passed; 22 ignored; 13 architecture tests passed; quit-latency test passed

cargo build -p polariton
passed

git diff --check
passed

target/debug/polariton ../uat/test7.rn
installed fixture launched without a terminal error and was closed deliberately
```

All ten PRODUCT-0018 gates are discharged.
