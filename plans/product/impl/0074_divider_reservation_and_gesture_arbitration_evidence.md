# PRODUCT-0074 implementation evidence: divider reservation and gesture arbitration

Implemented 2026-08-10 against
`plans/product/adr/0074_divider_reservation_and_gesture_arbitration.md`.

## Outcome

Divider presses are now pending gestures rather than immediate resizes. Motion
beyond an independent 8px activation deadband promotes the press; release below
the deadband performs the original click at its stored origin. Polariton also
narrows the engine-published cell hit region to an independent 10px proximity
strip, capped by that authority, and renders the matching resize cursor over
the exact candidate region. Pane-header reordering retains priority.

libaryon now accepts a runtime-only `PaneDividerReservation` through
`EngineConfig`. Its default is the original terminal `4 × 1`; Baryon therefore
keeps its existing rectangles and divider projection. Polariton selects `0 ×
0`, so sibling panes tile in cell space and its existing pane chrome supplies
the visible separation. The logical split tree, ratios, and `libaryon_layout`
payload are unchanged.

Under zero reservation a divider remains a projected boundary: its
`visual_rect` has zero extent on the split axis and its `hit_rect` is exactly
the preceding cell. The GUI may reject points within that cell by pixel
distance, but cannot widen the engine's authority or invent adjacency.

## Acceptance gates

| gate | evidence | result |
|---|---|---|
| 1. Baryon byte identity | `product_0074_default_child_rects_bank_the_4_by_1_reservation` and the existing host-panel projection test pin 48/48 columns, 20/20 rows, a 4-column vertical visual, and identical vertical hit/visual rects. The complete Baryon and libaryon sweeps pass without refreshed expectations | pass |
| 2. Shipped click theft | `product_0074_default_overlap_release_places_cursor_without_resizing` runs under the unchanged 4×1 default, releases in the overlapped live row, observes the exact cursor target, and asserts topology plus the absence of an immediate drag | pass |
| 3. Promotion | The same test pins a sub-deadband wobble as a click. `product_0074_zero_reservation_keeps_both_axes_grabbable_and_headers_first` crosses the deadband and observes each real boundary move, with exactly one active drag | pass |
| 4. Proximity only narrows | The default horizontal fixture distinguishes painted gap from live content beyond proximity. `product_0074_proximity_and_activation_deadband_are_independent` pins an empty divider census and a point on the boundary but outside the published hit rect as non-candidates | pass |
| 5. Independent constants and affordance | `product_0074_proximity_and_activation_deadband_are_independent` varies proximity and activation separately around the same six-pixel distances. It also pins the candidate overlay to the full published column or 10px of the published row. Production names are `DIVIDER_GRAB_PROXIMITY_PX` and `DRAG_ACTIVATION_DEADBAND_PX`; neither derives from the other | pass |
| 6. Header priority | The zero-reservation integration gate first proves its header point is a valid divider candidate, then delivers `PaneDragStarted` before the generic click and observes pane drag only | pass |
| 7. Every geometry consumer | `product_0074_zero_reservation_tiles_in_both_axes` pins exact edge adjacency. The group gate pins recursive minimum extents as 8×5 at the terminal profile and 4×4 at zero. The architecture gate forbids fixed divider constants from session and projection geometry | pass |
| 8. Group admission | `product_0074_group_admission_and_reset_follow_the_reservation_profile` uses a 16×6 marginal anchor. Both `ensure_host_panel_group` and `reset_host_panel_group` accept at zero and refuse at the default | pass |
| 9. Rendered 8×30 separation | `product_0074_rendered_host_canvas_separation_is_eight_by_thirty_pixels` runs Iced layout through the exact production `pane_content_stack` and `host_panel_content_frame` functions. Leaf geometry measures 8px side-by-side and 30px stacked; the delta is exactly the 22px header. The consumed style pins each border to 1px inside the measured 8px | pass |
| 10. Both zero boundaries drag | The zero-reservation integration gate projects both axes, begins inside each preceding-cell hit rect, crosses the pixel deadband in only that axis, and observes that logical divider move | pass |
| 11. Logical layout inert | `product_0061_logical_layout_is_head_neutral_before_codec` now captures the same unequal nested tree under terminal and zero profiles. Derived rects differ while encoded `libaryon_layout` bytes remain exactly equal. The architecture gate excludes the profile from the codec | pass |
| 12. Zero extents valid | `product_0074_zero_reservation_projects_zero_visuals_and_live_hits_in_both_axes` drives the production engine and pins zero visual width/height plus one-cell hit width/height ending at the boundary | pass |
| 13. Live exhibit | The 3840×2128 [dense Rune grid](exhibits/0074_dense_rune_grid.png) renders `../uat/test21.rn` as eight live host panels beside its producer. The independent [document split](exhibits/0074_document_split.png) records the 2px seam. Both are accepted against the dashboard comparison below | pass |
| 14. Nothing else moves | The sequential all-target workspace sweep, all-feature check, architecture/trybuild targets, and PRODUCT-0057 Vulkan renderer all pass. No baseline is refreshed and dashboard source is untouched | pass |

## Live exhibit

The exhibit was captured from the production X11 application at 3840×2128.
`../uat/test21.rn` produced all eight bar forms in ordinary host panels while
the authoring document remained visible. Titles, axes, legends, facets, and
marks remain legible in the dense grid, and sibling headers and canvases meet
without terminal-cell dead space.

The comparison is deliberately not described as a density win over the nested
dashboard:

| surface | side-by-side canvas separation | stacked canvas separation |
|---|---:|---:|
| host panels before PRODUCT-0074 | 46.4px | 52px |
| host panels after PRODUCT-0074 | 8px | 30px |
| nested dashboard | 7px | 25px |

The replacement remains 1px looser horizontally and 5px looser vertically
than the dashboard. That residue is pane chrome, not divider reservation, and
is acceptable in the live exhibit. The separate document control shows two
editable panes meeting at the expected 2px border seam without looking cramped
or leaving unallocated space. Neither visual stop condition fires.

Direct manipulation after capture rejected the initial affordance even though
its hidden 8px target existed: without a resize cursor, the 2px seam appeared to
be the whole target. The bounded correction uses the full published column
(9.6px) or 10px of the published row and applies the resize cursor to precisely
that region. It changes neither the screenshots nor pane geometry. With that
premise update, the accepted comparison authorizes PRODUCT-0075 to decide
dashboard retirement.

## Implementation findings

The zero-reservation hit rule cannot be a mechanical substitution. If the
existing visual-to-hit derivation merely consumes zero, both rectangles become
empty and the divider disappears from input. Projection now publishes one
preceding cell on both axes only when the reservation is zero; the terminal
default remains byte-identical.

The admission calculations were independently load-bearing. Tiling, recursive
minimum extents, initial group admission, and group reset each reconstructed
the reservation. The marginal 16×6 fixture is intentionally accepted by only
one profile, so leaving either admission subtraction on the constants fails a
test that ordinary pane tiling cannot satisfy.

The rendered gutter is not 10px. Iced container borders paint inside bounds and
do not participate in layout. Production widget layout measures 8px between
side-by-side host canvases; the two 1px borders form a seam inside those eight
pixels. Stacked canvases measure 30px because the lower pane contributes one
22px header in addition to the two four-pixel content paddings.

## Verification

```text
cargo test -p libaryon product_0074 -- --nocapture       pass (4 unit + 1 architecture)
cargo test -p polariton --bin polariton product_0074     pass (4)
cargo test -p polariton --test architecture_test \
  product_0074                                           pass (1)
cargo test --workspace --all-targets -- --test-threads=1 pass
cargo check --workspace --all-targets --all-features     pass
cargo test -p polariton \
  --features product-0057-render-evidence \
  --test product_0057_render_evidence \
  -- --nocapture --test-threads=1                        pass (Vulkan)
cargo fmt --all -- --check                               pass
git diff --check                                         pass
```

No ignored test was newly admitted. The workspace sweep retains only its
pre-existing environment-gated tests.
