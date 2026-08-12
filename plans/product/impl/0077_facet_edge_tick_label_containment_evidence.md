# PRODUCT-0077 implementation evidence: facet-edge tick label containment

Implemented 2026-08-11 against
`plans/product/adr/0077_facet_edge_tick_label_containment.md`.

## Outcome

Bound-coincident numeric tick labels now grow inward at occupied facet
boundaries. X labels anchor left or right and y labels anchor top or bottom;
outer figure edges, vacant neighbouring slots, omitted stepped endpoints, and
categorical labels retain their existing centred alignment.

The decision is carried from exact grid-slot occupancy into the existing
numeric chrome emitters for Cartesian, CategoricalX, and CategoricalY layouts.
It changes no tick position or layout rectangle. `GRID_GAP` remains 12px and
the layout path acquires no font, glyph, or text-measurement dependency.

## Acceptance gates

| gate | evidence | result |
|---|---|---|
| 1. Horizontal endpoints | `product_0077_all_numeric_layouts_emit_inward_endpoints_and_keep_categories_centred` records the production operation stream and pins the interior percent-x endpoints to `Right` and `Left` | pass |
| 2. Vertical endpoints | The same operation gate pins the interior percent-y endpoints to `Bottom` and `Top` | pass |
| 3. Labels survive | The operation gate retains all four visible `0%` and all four visible `100%` labels in its 2×2 Cartesian fixture | pass |
| 4. Outer edges | The operation gate pins outer x-min and y-max labels centred; `AxisTextAnchoring::NONE` preserves ordinary one-panel behavior | pass |
| 5. Vacant neighbours | `product_0077_occupied_neighbors_choose_only_inward_bound_anchors` uses a three-cell 2×2 grid and keeps both boundaries facing the vacant slot unanchored. The inherited PRODUCT-0048 partial-row tests remain green | pass |
| 6. One-cell parity | The inherited one-cell grid versus ordinary SVG parity gate remains byte-identical | pass |
| 7. Bound coincidence | `product_0077_bound_coincidence_is_shared_and_does_not_mean_last_tick` proves the shared x/y helper anchors exact bounds and leaves 40 centred under bounds 0..42 | pass |
| 8. Three numeric layouts | The operation gate exercises Cartesian numeric x/y, CategoricalX numeric y, and CategoricalY numeric x through the production emitters | pass |
| 9. Categorical controls | The operation gate keeps CategoricalX/CategoricalY category axes centred; `product_0077_categorical_xy_ignores_numeric_boundary_anchors` injects every anchor flag and proves the categorical-xy emitter ignores them | pass |
| 10. Geometry | `product_0077_grid_geometry_remains_twelve_pixel_congruent` pins both gaps to 12px and all 2×2 cells congruent. The architecture gate pins the production constant | pass |
| 11. No measurement | `product_0077_facet_tick_containment_does_not_measure_or_resize_layout` bounds the grid-layout and numeric-anchor paths and rejects font, glyph, paragraph, or text-extent dependencies | pass |
| 12. Production pixels | The PRODUCT-0057 Vulkan probe differences two otherwise-identical captures over both complete gutter strips. The banked state was 88 vertical and 111 horizontal tick-text pixels; accepted state is exactly 0 and 0 | pass |

## Implementation finding

### A gutter probe must span the complete perpendicular figure extent

The first Stage-0 crop covered only the tiled plot rectangle and reported zero
in both gutters. That crop omitted the axis-label bands where the defect lives.
A gutter is the boundary strip between cells across the complete perpendicular
figure extent: the vertical strip spans the full figure height and the
horizontal strip spans the full figure width. With those regions the unchanged
renderer produced the discriminating 88/111 banked state. The same regions,
settings, font pipeline, and paired sentinel/background captures produce 0/0
after inward anchoring.

This is an instrument correction, not a rendering correction: no production
geometry changed to make the probe pass.

## Verification

```text
cargo test -p polariton --bin polariton product_0077
  pass (5)
cargo test -p polariton --test architecture_test product_0077
  pass (1)
cargo test -p polariton --test product_0057_render_evidence \
  --features product-0057-render-evidence -- --test-threads=1
  pass (Intel UHD 770, Vulkan; PRODUCT-0077 banked 88/111, accepted 0/0)
cargo check -p polariton --all-targets --all-features
  pass
cargo test --workspace --all-targets --all-features --quiet \
  -- --test-threads=1
  pass
cargo fmt --all -- --check
  pass
git diff --check
  pass
```

No ignored test or tolerated failure was added. The production-renderer probe
is the claim about realized glyph ink; the operation-level gates remain its
anti-vacuity proof that both endpoint labels still exist.
