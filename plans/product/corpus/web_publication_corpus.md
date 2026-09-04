# Web publication acceptance corpus

Version 0.1. Charter: PRODUCT-0153 §8. This is a living, versioned acceptance
artifact — not an ADR. It records the destination ladder for the web
programme's two lanes with honest per-rung status. Rows are outcomes, never
mechanisms. A row's status changes only with landed, cited evidence; rows are
added only by a record that names them. "Pending" rows are the destination
stated in advance, not omissions. No row uses the word "static": each names
its data authority, initial rendering, and interaction separately.

Statuses: **executable** (landed with cited evidence) · **pending**
(destination, not yet implemented) · **proposed** (under consideration; no
accepted record, and not a commitment) · **deferred-by-design** (excluded
deliberately by a cited record).

## Publication lane

| rung | data authority · initial rendering · interaction | status | evidence |
|---|---|---|---|
| shared figure image: one figure leaves the desktop as a faithful Polariton SVG or raster | snapshot · pre-rendered · passive | executable | PRODUCT-0154 §6: `product_0154_labels_round_trip_through_an_xml_parser`, `product_0154_package_font_reproduces_the_png_export_for_every_admitted_family` |
| snapshot dashboard publication: a package with figure identity, specification, resolved presentation, shared dataset digests, provenance, and fallback renders | snapshot · pre-rendered · passive | executable | PRODUCT-0154 §1–§8: `product_0154_identity_is_content_and_moves_only_with_ink`, `product_0154_duplicate_view_names_reserve_every_id`, `product_0154_provenance_travels_with_each_artifact_and_shares_content`, `product_0154_canonical_ipc_is_equal_across_backings_and_strips_metadata`, `product_0154_canonical_ipc_ignores_producer_batching_and_refuses_maps`, `product_0154_guard_refuses_secrets_paths_and_endpoints_with_controls`, `product_0154_witness_and_recipe_refusals_name_the_figure`, `product_0154_a_selected_figure_publishes_the_authored_figure_over_its_full_data`, `product_0154_package_dtos_stay_free_of_placement_token_and_renderer_authority` |
| served snapshot publication: an existing package served without credentials, adapters, or execution | snapshot · pre-rendered · passive | executable | PRODUCT-0154 §9: `product_0154_shell_is_a_document_on_the_grid_with_one_pinned_script`, `product_0154_server_pins_verified_bytes_and_refuses_everything_else`, `product_0154_shell_emitter_inlines_one_pinned_script`, `product_0154_browser_plot_authority_is_refused_with_a_positive_control` |
| inspectable published dashboard: hover and inspection over published hit metadata | snapshot · pre-rendered · local over existing geometry | executable | PRODUCT-0155 for heatmap, scatter, bubble, and box plot, browser acceptance in `plans/product/impl/0155_browser_acceptance.md`; bar and histogram deferred (pointer-dependent prose), violin, radar, and parallel deferred (geometry): `product_0155_heatmap_parity_through_the_shipped_asset`, `product_0155_scatter_parity_through_the_shipped_asset`, `product_0155_bubble_parity_through_the_shipped_asset`, `product_0155_box_parity_through_the_shipped_asset`, `product_0155_projection_is_deterministic_and_prose_moves_without_regions`, `product_0155_bounds_omit_the_projection_and_publish_the_rest`, `product_0155_shell_is_a_polyglot_with_one_pinned_script_and_reversible_data`, `product_0155_inspection_asset_is_glue_with_a_positive_control`, `product_0155_javascript_engine_is_boa_dev_only_without_default_features` |
| navigable published dashboard: local pan, zoom, and selection consuming the desktop's settled vocabulary | snapshot · pre-rendered · local over existing geometry | pending | — |
| linked published dashboard: linked highlighting and cross-filtering over packaged data | snapshot · pre-rendered · local | pending | — |
| recomputing published dashboard: axes, bins, aggregation, or hit geometry recomputed by Polariton logic, locally or server-backed | snapshot · either · recomputed | pending | — |
| live published dashboard: re-queried on view against a native workspace | live · either · server-backed | pending | — |
| browser authoring: source edited in the browser, executed by a hosted native workspace | live · either · server-backed | pending | — |

## Distribution lane

| rung | status | evidence |
|---|---|---|
| verified Linux desktop artifacts with content-addressed manifests | pending | — |
| verified macOS and Windows artifacts of the reduced head, built and linked on target | pending | — |
| saleable official downloads with entitlement and fulfilment | pending | — |
| update channels for purchased artifacts | pending | — |

## Deferred by design

| family | record |
|---|---|
| a browser library as plot authority, fallback renderer, or canonical publication form | PRODUCT-0153 §2 |
| credentials, adapter handles, workspace paths, or live execution inside a package | PRODUCT-0153 §4 |
| a browser as the authoritative workspace | PRODUCT-0153 §1 |
| deciding the browser runtime before a rung needs semantic recomputation | PRODUCT-0153 §6 |

## Change log

- v0.4 (2026-09-03): PRODUCT-0155 makes the inspectable rung executable for
  heatmap, scatter, bubble, and box plot through exact kernel regions and
  the byte-pinned inspection script, with browser acceptance recorded; the
  deferred families are named.
- v0.3 (2026-09-03): the PRODUCT-0154 follow-up re-grounds the three executable
  rungs on content identity, artifact-borne provenance, a structural guard,
  a pinning loopback server, and literal escaping and font gates.
- v0.2 (2026-09-03): PRODUCT-0154 lands the snapshot package, the production SVG
  renderer, the publish action, the shell, and the server; the first three
  publication rungs become executable.
- v0.1 (2026-09-03): chartered by PRODUCT-0153 with every publication and
  distribution rung pending; four families deferred-by-design.
