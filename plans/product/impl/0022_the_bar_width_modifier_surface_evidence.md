# PRODUCT-0022 implementation evidence

Recorded 2026-08-04 after the closed Rune surface commit, complete regression
verification, and an installed launch of `../uat/test7.rn`. The high-density
fixture now combines an 8x line stroke, 8x scatter points, figure headings, two
origins, four host panels, and the near-touching
`bar("cat", "val").bar_width(0.9999)` boundary assay.

## Stage 0 findings

The envelope's styled-single carrier generalized without admitting Bar to
PRODUCT-0013's ordered layer type. `StyledRunePlotSink` holds one ordinary sink
plus typed `LayerSettings`; Line and Scatter project to their established
`LayerSpec` path, while Bar reaches the existing standalone kernel through one
styled task. This is boundary pruning: the modifier never reaches Toron.

The modifier parser remains a closed three-case register. It shares chain
traversal and numeric-literal extraction, but each spelling retains its own
receiver and validated type.

## Gate ledger

1. **Exact typed lowering.**
   `product_0022_bar_width_flattens_in_direct_and_figure_positions` compares the
   unchanged Bar sink and exact `Override::Set(BarWidth::new(0.5))` in both
   proven positions.
2. **Both positions retain one stamped result and production renderer.**
   `product_0022_bar_width_reaches_the_shared_result_and_real_host_panel`
   observes direct and figure `StyledPlot` outputs, proves `Arc::ptr_eq`, and
   completes them through the real host-panel path.
3. **Receiver compatibility stays closed and Bar stays out of layers.**
   `product_0022_receiver_rules_keep_bar_out_of_ordered_layers` refuses width on
   Line/Scatter, point/stroke modifiers on Bar, mixed chains in either order,
   and modified Bar inside `layers`, with bare controls retained.
4. **The substrate domain is the only validator.**
   `product_0022_bar_width_uses_substrate_bounds_and_atomic_refusals` accepts
   the boundaries and refuses missing, extra, computed, negative, zero,
   above-one, duplicate, and unknown forms at the complete mark.
5. **Unproven calls remain Rune.**
   `product_0022_bar_width_is_ordinary_rune_outside_proven_positions` preserves
   binding and argument calls byte-for-byte.
6. **The real host path preserves `0.9` and narrows at `0.5`.**
   The host lifecycle compares bare and default SVG byte-for-byte, observes a
   different narrow SVG, and exercises the host-owned kernel to prove the new
   padding misses hover.
7. **Width is neither identity nor presentation.**
   The same lifecycle retains one `HostPanelId`, one panel, `plot.bar`, group
   placement, title/subtitle, and an empty dashboard across bare, `0.9`, and
   `0.5` revisions.
8. **All prior modifier, layer, and envelope behavior remains executable.**
   PRODUCT-0018 and PRODUCT-0020 filtered suites, PRODUCT-0013 structural/paint
   regressions, and the full 292-test Polariton suite remain green.

The architecture assay
`product_0022_bar_width_extends_only_the_closed_styled_single_register` pins the
three explicit spellings, typed handoff, general styled-single carrier, and the
absence of open modifier dispatch or Bar layer admission.

## Commit and scope trace

- `453a5b16` — accepted PRODUCT-0021/0022 records and implementation plan;
- `db4d2be6` — closed recognition, typed flattening and refusals, styled Bar
  handoff, production host lifecycle, and architecture guard.

No modifier reaches Toron source. No Bar variant enters `LayerMark`; neither
plot specification is migrated; execution, transport, identity, placement,
Guide, headings, dashboard behavior, and libaryon production code are unchanged.

## Verification

The complete command ledger is recorded in
`0021_normalized_bar_slot_width_evidence.md`. The installed `test7.rn` fixture
launched without a terminal error and visibly exercises `.bar_width(0.9999)`
without the retired fixed inset.

All eight PRODUCT-0022 gates are discharged.
