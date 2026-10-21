# PRODUCT-0070 implementation evidence: the area surface

Implemented 2026-08-09 against
`plans/product/adr/0070_the_area_surface.md`.

## Outcome

Rune now accepts `area(x, y[, series])` as its fourth standalone authored mark
word in legacy, relation-sugar, canonical, named-view, and figure positions.
It resolves to the existing `StackedArea` member, retains the closed
line/scatter layer surface, and carries exact resolved categorical colours into
both filled regions and named legend swatches. The two-field form retains
synthetic provenance at the kernel boundary and is omitted by the existing
legend policy.

`polariton_session` writers now publish schema version 2. Readers accept the
unchanged three-family version-1 vocabulary and the four-family version-2
vocabulary; relabelling an area-bearing v2 payload as v1 refuses, and a
simulated v1 reader refuses the v2 envelope.

Stage 0 found one additional phase-boundary risk during implementation. Adding
`StackedArea` to the authored resolver would also have moved the dashboard's
all-`Auto` producer onto categorical assignment resolution, changing its
existing ramp colours. `legacy_automatic_area` now keeps that assignment-free
projection intact, and a dedicated control proves it. This is not a new
surface rule: it preserves the automatic producer PRODUCT-0070 explicitly
requires to remain byte-compatible.

## Acceptance gates

| gate | evidence | result |
|---|---|---|
| 1. Word and positions | `product_0070_area_spellings_and_static_bindings_share_one_standalone_intent` compares all five accepted positions, both arities, and every statically bound field; `product_0070_area_refusals_keep_layers_and_registers_closed` covers arity, missing binding, and layer refusal | pass |
| 2. Separate colour repair | `product_0069_coincident_fallback_keeps_the_banked_ramp_and_headroom` preserves the assignment-free SVG/ramp oracle. `product_0070_resolved_colors_reach_fills_and_legend_while_two_field_is_synthetic` supplies synthetic assignments directly, and `product_0070_factory_routes_resolved_area_colors_to_fills_and_named_swatches` proves the production factory threads them | pass |
| 3. Channel compatibility | `product_0070_area_compatibility_is_line_x_quantitative_y_and_categorical_series` admits numeric and temporal x and refuses categorical x/y plus numeric series with field and semantic facts. `product_0070_area_reaches_completion_with_cartesian_guides_and_fixed_orientation` proves completion and sink-span attribution without rotation | pass |
| 4. Closed registers | `product_0070_area_refusals_keep_layers_and_registers_closed` proves `.stacked()`, `.bar_width()`, and `.category_gap()` refuse on the whole area chain with receiver-specific reasons | pass |
| 5. Area identity | `product_0070_area_channels_resolve_to_one_fixed_member_and_family_identity` pins `LayerIdentity { ordinal: 0, plot_type: StackedArea }`, stable reruns, and a new lineage after changing the series field. The persistence codec maps that runtime member only to `AuthoredFamily::Area` | pass |
| 6. Exact migration | `product_0070_schema_two_adds_area_without_weakening_version_one` pins writer version 2, unchanged v1/v2 round trips for old families, v2 area round trip, and the old-reader unsupported-schema refusal | pass |
| 7. Vocabulary seal | The same test relabels area-bearing v2 bytes as v1 and requires `authored family area is unsupported in schema version 1`; accepting two envelope integers against one undifferentiated enum fails this assertion | pass |
| 8. Real checkpoint | `product_0070_saved_restart_preserves_product_0062_views_groups_and_colours_before_rerun` drives the production saved-session store in an isolated process. A three-category `StackedArea` identity checkpoints successfully—the old `DurableLayer::from_runtime` refusal point—and restores every exact palette position beside the pre-existing panel/group/bar controls | pass |
| 9. Resolved fills and swatches | The direct kernel and factory tests compare every fill and named swatch with the exact `ResolvedColor`; `product_0070_automatic_dashboard_area_keeps_its_assignment_free_projection` proves the dashboard route still carries no resolved color channel or identity | pass |
| 10. Legend policy and order | The resolved-order kernel test pins `Atlas, Beacon` baseline-outward in both preparation and legend. It also observes the two-field synthetic entry before policy and proves policy removes it | pass |
| 11. Zero, clipping, occlusion | `product_0070_area_reaches_completion_with_cartesian_guides_and_fixed_orientation` sends authored bounds and tick controls through the real Rune completion/factory path. PRODUCT-0069's canonical-matrix gate continues to prove hover and polygon share true-zero accumulation; PRODUCT-0068's strata tests prove opaque marks occlude guides rather than moving them above data | pass |
| 12. PRODUCT-0068 guides | All four PRODUCT-0068 tests remain green, including resolved `mark_decoration_color` at `0.5px`, guide-before-mark ordering, per-cell clipping, and the disabled-grid byte oracle. The feature-gated Vulkan evidence target also passes its stacked-area and bar probes | pass |
| 13. Facets and axes | `product_0070_faceted_areas_share_domains_panel_blind_colors_and_one_legend` asserts two asymmetric cells receive identical Cartesian domains, exact panel-blind fills, and one unioned figure-level swatch per assignment | pass |
| 14. No unrelated movement | Polariton passes 527 unit tests, 44 architecture tests, and the coordinate-space trybuild gate; the five pre-existing live-service tests remain ignored. The serialized workspace sweep passes on one test thread, and the all-feature/all-target check plus PRODUCT-0057/0068 Vulkan evidence target pass | pass |

## Surface and documentation

- `../uat/test23.rn` is the installed live exhibit: single area, resolved
  stacked area, faceted stacked area, and a multi-series line control over the
  existing `demo_trajectories` relation.
- `crates/polariton/README.md` documents the word, channel roles, preparation
  semantics, legend omission, and guide stratum.
- `plans/product/corpus/rune_workspace_corpus.md` advances to v0.70 with an
  executable area row and change-log entry.
- `plans/product/review/language_as_built.md` records the fourth word, its
  fixed member, data semantics, and static-binding coverage.

## Verification

```text
cargo test -p polariton product_0068                         pass (4)
cargo test -p polariton product_0069                         pass (4)
cargo test -p polariton product_0070                         pass (11, including isolated child)
cargo test -p polariton                                      pass (527 + 44 + 1; 5 pre-existing ignored)
cargo test -p polariton --all-features \
  --test product_0057_render_evidence -- --nocapture         pass (Vulkan)
cargo check --workspace --all-targets --all-features         pass
cargo test --workspace -- --test-threads=1                   pass
cargo fmt --all -- --check                                   pass
git diff --check                                             pass
```

No baseline file was refreshed. Existing dashboard stacked-area output stays
on the assignment-free fallback; existing scatter, bar, line, table, facet,
and persistence behaviours remain green.
