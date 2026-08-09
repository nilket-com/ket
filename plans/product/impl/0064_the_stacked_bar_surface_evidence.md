# PRODUCT-0064 implementation evidence: the stacked bar surface

Status: accepted 2026-08-09.

Plan: `7bab6555`
(`plans/product/adr/0064_the_stacked_bar_surface.md`).

Prerequisite correction: PRODUCT-0065, shared categorical grid domains.

## Result

Rune bars now admit the zero-argument `.stacked()` member in the same closed,
order-independent register as `bar_width` and `category_gap`. PRODUCT-0055's
channel-pair rule still chooses orientation first; stacking then selects
`StackedBar` or `HorizontalStackedBar` as the resolved composition member while
`LayerIdentity` remains the authored `Bar` family. There is no new mark word,
channel, authored family, or persistence schema.

The previously dashboard-only kernel now consumes resolved series swatches and
two-field band fills, honors both bar geometry properties, originates segments
at true data zero, and derives domain, paint, hit, and hover from one canonical
positive-segment sequence. The automatic dashboard path keeps its legacy ramp
fallback. Percentage normalization and `StackedArea` remain separate,
explicitly deferred questions.

During the facet gate, the new surface exposed an older PRODUCT-0041 defect:
categorical grid cells did not actually share their complete domain. Work
halted under PRODUCT-0064's pixel boundary, PRODUCT-0065 repaired the grid seam
for every categorical renderer, and this record then resumed as a consumer of
that shared mechanism.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Modifier register | `product_0064_stacked_is_a_zero_argument_order_independent_bar_member` covers two- and three-field forms and both modifier orders; `product_0064_stacked_refusals_are_atomic_and_receiver_scoped` pins repeat, argument, wrong receiver, and layered refusals at the authored span | pass |
| 2. Substrate repair first | The review stack's first implementation stage landed the shared segment producer, resolved assignments, and geometry-property routing before the surface. The unchanged fallback retained every existing stacked test and baseline; `product_0064_substrate_consumes_resolved_colors_in_marks_and_legend` separately drives synthetic series and band-fill assignments so fallback parity cannot satisfy the resolved branch | pass |
| 3. Four members | `product_0064_stacking_selects_members_after_the_bar_pair_rule` crosses vertical/horizontal pairs with stacking absent/present, then compares invalid-pair diagnostics with and without the modifier | pass |
| 4. Identity stable | `product_0064_stacking_preserves_lineage_and_canonical_members_gain_no_authority` pins `LayerIdentity { ordinal: 0, plot_type: Bar }`, exact palette positions and RGBA across grouping/stacking, and a changed-series-field control | pass |
| 5. Persistence untouched | PRODUCT-0062's real checkpoint/reopen fixture now restores a grouped `Bar` colour identity and resolves grouped and stacked forms to identical assignments. Whole-file diff confirms `session_persistence.rs` is unchanged | pass |
| 6. Paint and legend consume colours | The direct substrate test asserts exact synthetic colors in rendered rectangles and named swatches, exact band fills in the two-field path, and retains an empty-assignment automatic fallback. `product_0064_real_surface_joins_geometry_hit_color_legend_and_orientation` repeats the three-field proof through real resolution | pass |
| 7. Geometry modifiers | The real-surface factory gate renders both orientations at two extents, checks the PRODUCT-0053 category reserve and PRODUCT-0021 width equations, compares paint to shared hit rectangles, and proves reserve misses | pass |
| 8. Baseline-outward correspondence | The factory gate uses asymmetric series values and exact resolved colors to join legend entry zero to the zero-adjacent segment and each later entry outward, vertically and horizontally | pass |
| 9. Degenerate form | `product_0064_degenerate_stack_matches_grouped_geometry_and_band_fill` compares the two-field grouped and stacked rectangles and exact category fills under identical modifiers, and requires no legend | pass |
| 10. Negative boundary | The spec gate refuses directly-authored named-channel `StackedBar` and `HorizontalStackedBar`; the unchanged dashboard construction and kernel tests prove automatic canonical members still render | pass |
| 11. Label/description divergence | `product_0064_stacked_members_keep_family_label_and_physical_axis_rules` pins `plot.bar` beside the exact `Stacked Bar Plot` and `Horizontal Stacked Bar Plot` descriptions, with assertion messages naming authored family versus resolved geometry | pass |
| 12. Facets and axes | The GUI completion gate proves bounds, step placement, minor ticks, inverse physical-axis applicability, and both orientations. The PRODUCT-0064 facet gate proves one panel-blind assignment set, one legend, congruent categorical cells, and the PRODUCT-0065 shared summed domain | pass |
| 13. Canonical positive sequence | `product_0064_positive_sequence_owns_domain_rectangles_hit_and_hover` supplies positive, negative, zero, and duplicate rows; it pins first-row-wins, the positive-only 10%-headroom domain, shared rectangle metadata, hit, and exact hover totals | pass |
| 14. True-zero bounds | `product_0064_stacks_start_at_zero_under_nonzero_bounds_in_both_orientations` uses `(20,150)` on the numeric axis, requires running totals `10,40`, samples the visible clipped segment, and joins its geometry to exact hover prose in both orientations | pass |
| 15. Nothing else moves | No baseline was refreshed. The complete workspace all-target suite and PRODUCT-0057 raster-evidence target pass with only pre-existing ignored tests. `../uat/test20.rn` contains grouped, vertical stacked, horizontal stacked, and two-field stacked views with static field bindings and shared palette | pass |

## Implementation findings

The record's staged substrate repair was necessary. The fallback oracle could
prove that replacing four literal `0.8` calculations with the accepted default
did not move the dashboard, but it could not observe resolved assignments that
the dashboard never supplies. The synthetic series and band-fill controls are
therefore independent, load-bearing halves of gate 2.

The facet gate halted on an older reachable defect rather than a stacked
special case. `shared_layout` and `merge_layout` shared only Cartesian ranges;
categorical facets kept local numeric ranges and category lists. Fixing that
inside PRODUCT-0064 would have moved grouped-facet pixels outside this record's
scope. PRODUCT-0065 was planned, implemented, and fully verified first. Its
semantic category lookup also prevents a local missing category from shifting
the next stacked payload under the wrong shared label.

The implementation did not add a fourth persistence family. The stacked bit is
consumed only after pair resolution selects geometry, while every bound channel
retains the `Bar` layer identity. This is why a pre-existing grouped colour
payload restores directly onto stacked geometry without a codec or migration.

## Verification

- `cargo test -p polariton product_0064 -- --nocapture` — all 11 targeted
  surface, substrate, resolution, persistence, facet, and UI gates passed;
  zero ignored.
- `cargo test --workspace --all-targets --no-fail-fast` — passed across the
  workspace. Polariton reported 508 passed and five pre-existing live-Toron
  tests ignored; libaryon reported 1,483 passed and 22 pre-existing manual or
  benchmark tests ignored. Architecture and trybuild suites passed.
- `cargo test -p polariton --test product_0057_render_evidence --features
  product-0057-render-evidence -- --nocapture` — all four automated raster
  probes passed on the Intel UHD 770 Vulkan adapter.
- `cargo fmt --all -- --check` and `git diff --check` — passed.
- `git diff -- crates/polariton/src/session_persistence.rs` — empty.

## Implementation history

The reviewable stack separated the dashboard-reachable substrate repair from
the Rune surface, stopped for PRODUCT-0065 when the facet gate falsified an old
sharing premise, and resumed only after that correction was independently
green. The two PRODUCT-0064 implementation stages are compressed to one
`impl: adr 0064 the stacked bar surface` commit after this evidence audit.
