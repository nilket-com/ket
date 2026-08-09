# PRODUCT-0062 implementation evidence: Polariton Rune-view and colour continuation

Status: accepted 2026-08-09.

Plan: `af583cac`
(`plans/product/adr/0062_polariton_rune_view_and_color_continuation.md`).

## Result

Polariton now registers the strict `polariton_session` owner when a saved
libaryon engine starts. Its version-1 JSON payload carries only durable
document/view meanings, opaque panel/group joins, categorical allocation, and
the two semantic allocator high-water marks. Serde is confined to this one
schema module; plot, editor, theme, and envelope types do not become schemas.

Capture re-keys runtime `BufferId`s through the current projected
`DocumentId` bindings. Panel and group relations are filtered against the
authoritative shared census; colour allocation is filtered only by durable
producer document. The witness is copied verbatim from that census and the
owner publishes only through libaryon's coupled checkpoint transaction.

Saved startup decodes before transferring the writable session into the
engine. The decoded state remains inert until the first complete viewport can
join every document, panel, and group against one census. Only then are both
editor maps, both allocator marks, and the application colour map installed.
Restored panels remain unresolved; rerunning Rune consumes the restored
mapping and group through the existing preparation path and resolves in place.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. Strict bounded codec | `product_0062_codec_is_strict_bounded_and_deterministic`, `...allocator_marks_are_structural_state`, and `...palette_positions_are_revalidated_against_their_space` cover deterministic maximal round-trip, the 1 MiB bound and injected small bound, schema/trailing/unknown/duplicate/zero refusals, palette bounds, and allocator floor/dominance/headroom | pass |
| 2. No placement representation | `product_0062_persistence_schema_has_no_placement_or_renderer_authority` bounds the DTO vocabulary and serde sites; `...schema_has_no_placement_or_renderer_vocabulary` checks emitted JSON | pass |
| 3. Exact re-keying | `product_0062_rekeying_uses_document_identity_not_open_order_or_path` reverses runtime open order and includes an alien-session control | pass |
| 4. Allocator continuation | The allocator unit gate rejects a non-dominating mark; the isolated restart oracle restores two identities, reuses them, then proves a fresh mint is above both | pass |
| 5. Split lifetimes | `product_0062_capture_filters_shared_identity_not_colour_lifetime` drops a closed panel, retains a stale panel, and keeps the closed view's colour allocation | pass |
| 6. Projected census witness | Production calls `ApplicationCheckpointWitness::from_census` and requires projected document bindings to equal the census. PRODUCT-0061's exact-three-set, fixed-role, and collected-group convergence gates remain green | pass |
| 7. Capture membership | `validate_capture_membership` checks encoded documents, panels, and groups after filtering and before issuing the engine request; decoder tests remain census-free | pass |
| 8. Total post-spawn join | `product_0062_post_spawn_join_is_total` rejects an absent panel before producing runtime maps; production constructs all maps in locals and has one atomic assignment boundary | pass |
| 9. Colour continuation | The isolated restart fixture persists a non-empty allocation, reopens, and proves its exact category position survives under the restored producer/view/palette key | pass |
| 10. Closed family and rotation | `product_0062_colour_family_is_closed_and_rotation_independent` proves vertical and horizontal bars both encode `bar` and refuses `horizontalbar`, `stackedbar`, and unknown families | pass |
| 11. Anonymous semantics | Maximal codec and restart fixtures include anonymous identities exactly; re-keying preserves their ordinal, leaving PRODUCT-0060's documented positional instability visible | pass |
| 12. Coupled publication | The restart fixture proves ordinary checkpoint refusal and coupled publication. PRODUCT-0061's stale-witness and both-`CURRENT`-fault gates remain the transaction oracle | pass |
| 13. Group reattachment | The restart fixture restores the exact group ID and rerun consumes it through `prepare_rune_view_panels`/`EnsureHostPanelGroup`; PRODUCT-0061's manual/detached one-shot oracle remains green | pass |
| 14. Resolve in place | The restart fixture pins panel IDs and panel-map count across rerun before minting one genuinely new identity | pass |
| 15. Derived exclusion | The DTO architecture gate excludes runtime IDs, placement, descriptors, and renderer vocabulary; the schema contains no dashboard, kernel, relation, result, image, execution, or database type | pass |
| 16. Defaults still | Full pre-existing Polariton and libaryon suites pass without baseline or fixture premise changes; ephemeral construction still uses `EngineHandle::spawn` and never creates a persistence owner | pass |

## Implementation findings

The first full restart oracle exposed one useful distinction: runtime
`BufferId`s are reminted but their raw integer may coincidentally repeat after
a process restart. Inequality is therefore not a valid durability oracle. The
gate keys correspondence on `DocumentId` and reverses open order in the pure
re-keying fixture instead.

The same oracle deliberately seeded a horizontal resolved bar identity. On
restore it became `PlotType::Bar`, not `HorizontalBar`; that is the intended
PRODUCT-0055 canonical authored family, and the assertion names why the two
runtime values differ while the palette position does not.

`DocumentId::new` became public as the narrow validated reconstruction seam
for sibling owner schemas. It still rejects zero and exposes no allocator or
store mutation capability.

PRODUCT-0059's Polariton architecture premise still said no frontend could
mention `SavedSession` or `DocumentId` “before the Polariton owner extension.”
0062 is that extension. The gate was narrowed, not removed: Polariton still
cannot discover or create a `SessionStore`, the transferred writable handle is
confined to the editor host, and durable document identity is confined to that
host plus this schema boundary.

## Verification

- `cargo test -p polariton product_0062_ --no-fail-fast` — codec,
  architecture, re-keying, capture, join, and isolated on-disk restart gates
  passed; zero ignored.
- `cargo check -p polariton --all-targets` — passed without warnings.
- `cargo test -p polariton --all-targets --no-fail-fast` — passed.
- `cargo test -p libaryon --all-targets --no-fail-fast` — passed.
- `cargo test --workspace --all-targets --no-fail-fast` — passed.
- `cargo clippy -p polariton --bin polariton -- ... -D warnings` and
  `cargo clippy -p libaryon --lib -- ... -D warnings` — passed with only the
  repositories' previously recorded lint allowances.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

## Implementation history

The working implementation may use intermediate commits locally, but the
published history is compressed to one `impl: adr 0062 polariton rune view and
color continuation` commit after verification.
