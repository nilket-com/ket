# PRODUCT-0072 implementation evidence: percent-of-total stacking

Implemented 2026-08-10 against
`plans/product/adr/0072_percent_of_total_stacking.md`.

## Outcome

Rune now accepts `.percent()` on stacked bars and areas. The modifier projects
each category-local or x-local canonical stack onto an exact `0..100` scale;
it does not rewrite the relation, change the mark family, recolour a series, or
extend the persistence schema. Vertical and horizontal bars share the same
normalization, and modifier order remains irrelevant.

Preparation retains one denominator per stack from the existing first-row-wins
positive-finite matrix. Geometry projects cumulative absolute boundaries rather
than summing rounded shares. Hover keeps the absolute value and denominator and
adds share plus cumulative percentage. Zero-total stacks retain the fixed
percent layout without inventing a share.

`AxisType::Percent` is carried by Cartesian and categorical layouts into shared
chrome. Its values are percentage points, so a step of `25` renders `0%`,
`25%`, `50%`, `75%`, and `100%`. Authored bounds on the normalized value axis
refuse even when written as `0..100`.

## Acceptance gates

| gate | evidence | result |
|---|---|---|
| 1. Accepted spellings and bindings | `product_0072_percent_is_an_order_independent_stacked_register_member` compares both bar modifier orders, vertical and horizontal field pairs, two- and three-field area/bar forms, and static bindings in every field role | pass |
| 2. Closed register and atomic refusal | The same envelope test covers unstacked bar, line, scatter, duplicate, and argument refusals at the complete sink; no transformed view survives | pass |
| 3. Absolute byte identity | The pre-surface `plot::` sweep passed 183 existing tests after categorical layouts began carrying explicit `Linear`; the complete 538-test unit sweep passed without a baseline or assertion refresh | pass |
| 4. Joint bar projection | `product_0072_percent_uses_first_row_total_and_one_geometry_for_hover` pins first-row numerator, denominator, raw and cumulative values, exact rectangle boundaries, both orientations, hit, hover, and an exact final `100`, including a subpixel terminal segment that the absolute-mode one-pixel minimum must not distort | pass |
| 5. X-local area projection | `product_0072_percent_area_uses_x_local_first_row_totals` uses substantially different totals at adjacent x samples and jointly pins local denominator, hover band, cumulative boundary, and exact `100` projection | pass |
| 6. Duplicate rows | The bar and area fixtures place `300` after accepted `30`; both retain `30`, totals `120`, and reject raw-sum or last-row readings | pass |
| 7. Invalid and zero samples | `product_0072_invalid_samples_are_zero_and_zero_totals_invent_no_share` and `product_0072_area_invalid_samples_pinch_zero_total_without_inventing_share` cover negative, zero, infinity, NaN, null, mixed-positive, and all-zero stacks. Bars emit nothing for an empty category; area bands meet at the zero-total x; neither invents percentage prose | pass |
| 8. Two-field forms | The same kernel gates prove one positive series reaches `100%`, retains Synthetic provenance, and is removed by the existing legend policy. Zero-total controls remain empty | pass |
| 9. Layout axis types | Direct kernel and production completion gates pin vertical `CategoricalX/Percent`, horizontal `CategoricalY/Percent`, area `Cartesian/Percent`, and exact `0..100`; existing absolute layouts remain `Linear` | pass |
| 10. Shared percent chrome | `product_0072_percent_axis_formats_percentage_points_and_admits_steps` records the rendered categorical-axis text and requires exactly `0%`, `25%`, `50%`, `75%`, `100%`; the same layout under `Linear` contains no percentage label. The faceted gate proves the type survives merge | pass |
| 11. Bounds and tick applicability | `product_0072_percent_completion_fixes_value_domain_and_refuses_bounds` drives vertical, horizontal, and area completion with step ticks, rejects all three value-axis bounds at the figure element with the fixed-domain reason, and leaves area x/domain machinery untouched | pass |
| 12. Facets | `product_0072_faceted_percent_stacks_share_axis_type_and_fixed_domain` uses asymmetric cells and requires a shared `Percent` axis, exact `0..100`, congruent cell layouts, exact panel-blind colours, and one unioned legend | pass |
| 13. Identity and persistence | The existing grouped/stacked lineage gate now adds percent and compares exact resolved assignments. The isolated PRODUCT-0062/0070 saved-session checkpoint restores the durable Bar family and palette positions, then resolves grouped, stacked, and percent forms against the same restored identity with exact colours. Session schema remains v2 and `session_persistence.rs` is unchanged | pass |
| 14. Vulkan occlusion | The PRODUCT-0057 target renders real percent kernels. A fully positive area preserves 3,680 exact-fill intersections and a gapless bar wall preserves 3,486 where an overlay-grid control visibly composites them; the area boundary contributes 1,849 discriminating above-mark pixels, and a gapped bar retains 1,554 real grid pixels on exposed canvas | pass |
| 15. Hover and prior pixels | Absolute hover tests remain byte-identical. Percent gates pin exact absolute Value/Total plus Share/Cumulative prose for both kernels; no existing baseline moved | pass |
| 16. Complete verification | Polariton unit, architecture, trybuild, all-feature check, Vulkan, formatting, workspace, and diff gates pass with no ignored test added | pass |

## Implementation finding

The styled factory needed one additional admitted member. `.percent()` makes an
area use the existing `StyledSingle` carrier even though it introduces no
presentation setting. A plain area percent plot could therefore reach the
ordinary factory, while `y_tick_step(25)` first traversed styled-factory
capacity validation and refused `StackedArea`. Admitting the already-supported
area member at that seam makes both routes consume the same resolved spec; it
does not widen the authored register or the layered surface.

The raster gate also confirms PRODUCT-0068's pre-filed consequence rather than
merely restating it. Fully occupied percent marks erase every interior grid
intersection while retaining exact resolved fill pixels. Gapped bars still
show the same grid on canvas, and area boundaries remain above their fills.

## Surface and documentation

- `../uat/test24.rn` is the live exhibit, backed by
  `../uat/demo_percent_stacks.sql`: vertical, horizontal, faceted, gapless,
  two-field, duplicate-row, zero-total, area, and faceted-area forms.
- `crates/polariton/README.md` documents the spelling, denominator, fixed
  domain, bounds refusal, hover truth, and occlusion consequence.
- `plans/product/corpus/rune_workspace_corpus.md` advances to v0.72 with an
  executable percent-stack row and change-log entry.
- `plans/product/review/language_as_built.md` records `.percent()` as a scale
  selector outside `LayerSettings` and the presentation cascade.

## Verification

```text
cargo test -p polariton --bin polariton product_0072        pass (8)
cargo test -p polariton --bin polariton                     pass (538; 5 pre-existing ignored)
cargo test -p polariton --test architecture_test            pass (46)
cargo test -p polariton --test coordinate_spaces_ui         pass (1 trybuild pair)
cargo test -p polariton --test product_0057_render_evidence \
  --features product-0057-render-evidence -- --nocapture    pass (Vulkan)
cargo check -p polariton --all-targets --all-features       pass
cargo test --workspace --all-targets -- --test-threads=1    pass
cargo fmt --all -- --check                                  pass
git diff --check                                            pass
```

No checked-in baseline was refreshed. The only new pixels belong to opt-in
percent figures and the semantic Vulkan fixtures; absolute plot output remains
unchanged.
