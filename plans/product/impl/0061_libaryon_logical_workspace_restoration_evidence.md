# PRODUCT-0061 implementation evidence: libaryon logical workspace restoration

Status: accepted 2026-08-08.

Plan: `73cf64c6`
(`plans/product/adr/0061_libaryon_logical_workspace_restoration.md`).

## Result

Libaryon's explicit saved-engine checkpoint now publishes two internal owners
in one generation:

```text
libaryon_documents  -> exact ordinary-document identity and bytes
libaryon_layout     -> head-neutral logical splits, tabs, panels, and groups
```

The layout owner stores integer split weights and durable identities, never
rendered rectangles or runtime pane/view/buffer counters. Capture walks the
private `LayoutNode`, positively retains saved ordinary documents and opaque
host panels, removes transient leaves, and canonicalizes the surviving tree.
Restore builds ordinary documents first, then allocates fresh runtime views,
panes, and one backing buffer per unique host panel before the engine thread
starts. An absent or empty layout owner retains PRODUCT-0060's deterministic
one-pane fallback.

One registered application owner can join that same ordered checkpoint through
`spawn_saved_with_application_owner` and
`checkpoint_with_application_owner`. Libaryon never parses the application
bytes. It compares complete document, host-panel, and host-panel-group witness
sets against the state captured at the FIFO head; any mismatch refuses before
owner staging. `libaryon_*` remains legal in the generic store but is reserved
from this application mutation boundary.

Polariton still owns no persisted payload in this record. Its existing
`view_panel_ids` behavior is now explicitly guarded as append-only because the
three-set witness depends on identities never being recycled or rebound.

The accepted amendment projects those same three authoritative sets as
`SharedIdentityCensus` on saved-engine viewports. It is deliberately only an
observational retry instrument: no placement, descriptor, membership, or
ordering fact is representable. `ApplicationCheckpointWitness::from_census`
copies the sets without interpretation. An internally collected group
disappears from the next projection, making a previously stale request
rebuildable instead of permanently unactionable.

## Acceptance-gate ledger

| Gate | Evidence | Result |
| --- | --- | --- |
| 1. PRODUCT-0060 fallback | The full PRODUCT-0060 restore suite passes unchanged; absent and empty layout roots leave `new_restored`'s ascending-`DocumentId` one-pane fallback in place | pass |
| 2. Strict bounded codec | `product_0061_layout_codec_is_strict_deterministic_and_bounded`, `...refuses_structural_mismatches`, and `...refuses_unknown_fields` cover deterministic round-trip, trailing data, missing references, ratio/focus/group faults, small injected budgets, and the pinned 16 MiB production bound; the host pre-start test refuses future schema while generic open preserves its bytes | pass |
| 3. Head-neutral bytes | `product_0061_logical_layout_is_head_neutral_before_codec` captures and encodes the same asymmetric nested tree at 120×40 and 360×120; `libaryon_layout` bytes are identical while `PaneRect`s differ. `product_0061_layout_payload_is_head_neutral_and_application_meaning_free` guards the DTO vocabulary | pass |
| 4. Logical workspace meaning | `product_0061_restored_manual_group_reattaches_without_retiling` restores and recaptures an n-ary unequal-ratio layout byte-for-byte by DTO meaning, including tab order, active tab, focus, group root, manual state, and detached member. The saved-engine test supplies nested horizontal/vertical document and host-panel placement through production | pass |
| 5. Positive filtering | `product_0061_capture_filters_transient_tabs_and_phantom_leaves` removes a transient output leaf and its ratio, rehomes focus, retains document/panel order, and proves an all-transient layout becomes the empty documented fallback. PRODUCT-0060's positive-candidate fixture continues to enumerate output, snapshot, browser, Markdown preview, UAST inspector, and host-panel distinctions | pass |
| 6. Fresh runtime identity | The coupled reopen test restores two occurrences of one `DocumentId` as distinct `ViewId`s sharing one buffer. The DTO architecture gate excludes `BufferId`, `PaneId`, and `ViewId`; host-panel identity alone remains exact | pass |
| 7. Restored panel events | The coupled saved-engine test observes `PanelRestored` before the viewport exposing its revision-zero placeholder. The duplicate-panel fixture restores two views over one backing buffer but emits exactly one event per unique panel; startup drains restoration events before any init-script render | pass |
| 8. Group arrangement and reattachment | The manual/detached fixture restores with no producer, then explicitly reattaches through a live document anchor while preserving the exact tree, manual state, detached set, and panel IDs | pass |
| 9. Application namespace | `product_0061_application_namespace_is_reserved_only_at_mutation_boundary` refuses current/future `libaryon_*` application names, accepts `polariton_workspace`, and proves generic parsing preserves an unknown future internal owner. `...registered_application_owner_is_bounded_before_startup` pins 16 MiB and lock release | pass |
| 10. Independent witness failures | `product_0061_each_application_witness_census_refuses_independently` exercises missing and extra document, panel, and group IDs separately, then publishes only after rebuilding the exact witness. `product_0061_projected_census_is_exactly_three_identity_sets` exhaustively destructures the public projection into three `BTreeSet`s, making any field growth a compile failure | pass |
| 11. FIFO races | `product_0061_fifo_lifecycle_races_refuse_stale_application_snapshots` queues document, panel, and group creation ahead of payloads built from the preceding projection; each request refuses its exact stale census. `product_0061_projected_census_makes_collected_group_refusal_actionable` closes a panel and producer, observes silent group collection in the next census, and publishes with the rebuilt witness | pass |
| 12. Append-only Rune identities | `product_0061_rune_semantic_panel_identity_is_append_only` proves surviving named/anonymous panel IDs, stale removed mappings, high-water allocation, stable producer groups, and fixed-role exclusion. The production field comment and assertion messages name PRODUCT-0061's witness dependency | pass |
| 13. Coupled capability closure | `product_0061_coupled_checkpoint_restores_layout_and_orders_panel_event` proves plain checkpoint refuses before and after coupling while coupled publication succeeds; the complete PRODUCT-0060 suite proves unregistered checkpoint behavior remains intact | pass |
| 14. Atomic publication | `product_0061_coupled_publication_faults_never_mix_owners` changes layout and application bytes across both injected `CURRENT` fault sides. Reopen observes both old before the pointer and both new after it; post-pointer uncertainty poisons later coupled checkpoints | pass |
| 15. No inferred application meaning | The duplicate-panel restore fixture gives distinct IDs identical `{kind,title}` values and retains both placements. The architecture gate excludes Polariton, `LocalViewIdentity`, and `ColorIdentity` from the owner DTO | pass |
| 16. Transient/derived exclusion | The codec contains only logical placement, descriptors, and group arrangement. New view construction resets cursors, selections, scroll, hover, drags, pending work, revisions, and content; the filtering and architecture gates keep output products, kernels, DataFrames, SQL/Toron state, and dashboard state absent | pass |
| 17. Existing surfaces remain still | The full workspace all-target suite passes without image, SVG, draw, Rune, Baryon, or Polariton baseline refresh. The only Polariton production change is the append-only dependency comment | pass |
| 18. Complete verification | Targeted behavior, architecture, downstream identity, full crate/workspace, rustdoc, clippy, formatting, and diff checks all pass; no PRODUCT-0061 test is ignored | pass |

## Implementation findings

Four findings tightened the implementation without widening the record.

First, restored panel events must be drained before startup configuration, not
merely before the ordinary request-loop render. An init script can itself
request the first viewport, so the engine now emits the complete pre-start
`PanelRestored` batch immediately after constructing the host session.

Second, an empty but producer-live group must remain in `libaryon_layout`.
Polariton's group mapping is append-only; dropping the corresponding libaryon
identity merely because its current member set is empty would make the exact
group witness spuriously stale. Empty group records carry no fabricated
placement or producer meaning.

Third, the convenient structural oracle was weaker than the accepted gate.
The first commit banked `LayoutNode::structure()` across a 3× resize. The final
gate now also runs the production capture and codec and compares the actual
owner bytes, retaining changed `PaneRect`s as the anti-vacuity control.

Fourth, producer-less is not synonymous with restored. The first reattachment
branch admitted any group whose producer had disappeared, which reopened a
one-shot restore capability after an ordinary live producer release and broke
PRODUCT-0015's cleanup contract. `HostPanelGroupState` now carries transient
`reattachable_after_restore` state: only pre-start restoration sets it,
successful reattachment consumes it, and ordinary producer release never
recreates it. The legacy release test and the restored-manual-group test pin
both sides.

Fifth, the application could not observe the complete group census that its
witness was required to supply. Panels and documents were projected, but
`HostPanelGroupId` had no viewport field or lifecycle event, and unreachable
groups are collected internally. `SharedIdentityCensus` closes that feasibility
gap with exactly three sets. A 3x viewport-size test holds the census identical
while rectangles change, and the collected-group fixture proves a stale retry
converges from the next projection.

## Implementation history

The review stack and shared-identity census amendment are compressed into one
`impl: adr 0061 logical workspace restoration` commit before publication, so
PRODUCT-0061 remains one plan/implementation pair.

## Verification

- `cargo test -p libaryon product_0061_ --lib` — 13 PRODUCT-0061 behavior
  tests passed, zero ignored.
- `cargo test -p libaryon --test architecture_test product_0061_` — the
  head-neutral/application-free DTO and exact-three-set census gates passed.
- `cargo test -p polariton product_0061_rune_semantic_panel_identity_is_append_only --all-features`
  — the cross-crate append-only premise passed.
- `cargo test -p libaryon --all-targets` — 1,505 unit tests plus integration,
  architecture, and trybuild targets passed; only pre-existing manual
  benchmark/acceptance tests remain ignored.
- `cargo test -p libaryon --doc` — the compile-fail doctest passed.
- `cargo test --workspace --all-targets --no-fail-fast --quiet` — passed across
  the complete workspace with no baseline refresh.
- `cargo clippy -p libaryon --tests -- -A clippy::deref_addrof -A
  clippy::map_entry -A clippy::len_without_is_empty -D warnings` — passed with
  the pre-existing libaryon allowances used by PRODUCT-0059/0060.
- `cargo fmt --all -- --check` and `git diff --check` — passed.
