# PRODUCT-0015 implementation evidence

Recorded 2026-08-03 after the four implementation commits and the complete
cross-feature verification pass. The acceptance evidence is behavioral and
structural; the installed binary was also launched against `../uat/test6.rn` as
a startup/load smoke test. The prior PRODUCT-0014 dogfood image remains the
content-rendering reference; this record does not claim a new manually captured
layout image.

## Stage 0 finding

The feasible branch held. `LayoutNode` remains the only pane topology. A group
is represented by one optional root tag on that tree, and
`HostPanelGroupState` stores producer identity, layout intent, membership,
retention, detachment, and the latest anchor—never child edges, ratios, bounds,
or rectangles. Atomic first placement builds ordinary `LayoutNode` values
off-tree and inserts one subtree. No projection type changed and no second pane
tree was introduced.

Actual producer closure is the successful `release_buffer` removal of the
captured `BufferId`; jump-history refusal returns before that transition. The
implementation therefore agrees with PRODUCT-0015 §5's amended wording rather
than treating last-view disappearance as closure.

## Gate ledger

1. **One atomic visible group.**
   `product_0015_host_panel_groups_tile_atomically_and_fallback_visibly` drives
   one four-member request through the live engine and observes one source plus
   four output panes, source focus, one tab per output, two visible rows, and
   Compact/Expansive width differentiation. The session invariant test observes
   exactly one group-root tag.
2. **Forward dogfood order and generic shape.**
   `rune_view_collection_tiles_one_forward_producer_group_without_tabs` drives
   the `test6.rn` table/layered/table/bar shape through `CodeEditor`, observes
   that exact forward title order in a 2x2 tile, and proves each plot sibling is
   wider than its table sibling. `prepare_rune_view_panels` contains no reverse
   iteration and maps only `ViewContentKind` to generic shapes.
3. **Producer isolation and closure.**
   `rune_view_groups_are_distinct_and_one_producer_does_not_retile_another`
   establishes two producer buffers, distinct group ids, and unchanged first
   producer pane identities/rectangles after the second group appears.
   `product_0015_actual_producer_release_retires_only_after_last_placement`
   proves producerless ensure/reset refusal, live output retention, and
   bookkeeping collection only after the last placement closes.
4. **Advisory decline stays visible.**
   The live engine test repeats the four-member request under deliberately
   unsatisfiable bounds: every panel opens in request order, no tab is inferred,
   no group-root tag survives, and rerun does not gather the fallback panes.
5. **Unchanged reruns preserve layout.**
   The `CodeEditor` fixture records pane/view identities, rectangles, tabs,
   activation, and source focus and compares them exactly after a second ensure.
   The session tests separately compare the authoritative tagged-tree structure
   and ratios across unchanged Manual and Automatic updates.
6. **Completion cannot place.**
   The `CodeEditor` fixture applies failure and stale producer state and observes
   identical pane identity, geometry, tabs, and activation. Placement is sent
   only by `prepare_rune_view_panels`; table frames, plot preparation, descriptor
   length, and completion handlers have no path to a group command.
7. **Only internal geometry dirties.**
   `product_0015_only_internal_geometry_changes_enter_manual` resizes an
   internal divider and observes `Manual`, then uses the source/group boundary
   and terminal resize as controls that remain `Automatic`. Rerun preserves the
   manually changed structure.
8. **Automatic membership semantics.**
   `product_0015_automatic_membership_retiles_only_for_addition` adds a fifth
   member and observes a new visible tile, omits one member without any
   structural change, and returns the same named panel to the same pane while
   clearing retention.
9. **Manual append and temporary close.**
   `product_0015_manual_add_and_reopen_wrap_the_untouched_subtree` compares the
   old descendant structure byte-for-byte after append, closes a current panel,
   and observes the next successful ensure reopen it without changing surviving
   pane identity.
10. **Detachment and cross-group reclaim.**
    `product_0015_detachment_survives_ensure_and_reset_reclaims_across_groups`
    moves one producer's panel into another group, observes owner Automatic and
    destination Manual, proves rerun preserves the external placement, then
    resets and reclaims it without ownership transfer.
11. **Full reset reconciliation.**
    `product_0015_reset_reconciles_retained_missing_and_foreign_once` combines a
    retained orphan, detached current panel, closed current panel, and foreign
    text pane. One reset removes the orphan through ordinary backing-buffer
    release, reclaims/reopens current panels in the tagged tile, preserves the
    foreign pane visibly, clears reconciliation state, and returns Automatic.
    Sending no intervening ensure is the parse/envelope-failure control: the
    latest successful set remains authoritative.
12. **Current Automatic reset is exact.**
    The same test snapshots the complete layout string and focus after
    reconciliation and observes a second reset is an exact no-op. The
    `CodeEditor` fixture independently invokes the visible Retile outputs action
    on an already-current group and compares the full projected assay unchanged.
13. **Existing surfaces remain independent.**
    The complete suites retain `RESULT_PANEL`, `DASHBOARD_PANEL`, direct
    `OpenHostPanel` split/`TabWith`, PRODUCT-0010 identity/orphan/staleness,
    PRODUCT-0014 layered host plots, and no-dashboard behavior. No Rune envelope,
    plot, dashboard, Toron, transport, or projection file changed.

## Commit and scope trace

The accepted plan and record landed together as `ebb45683`. The four ordered
implementation commits are:

- `6d67e95b` — atomic protocol and first tiling;
- `fd968496` — Automatic/Manual preservation and membership updates;
- `a287b770` — detachment, reset, and producer lifecycle; and
- `67c8f2f5` — Polariton producer policy, direct order, and Retile outputs.

The first Polariton `EnsureHostPanelGroup` caller and deletion of
`resolved.into_iter().rev()` are both in `67c8f2f5`; no intermediate commit
contains only one half. The halt rule did not fire.

## Verification

```text
cargo fmt --all -- --check
passed

cargo test -p libaryon
1410 passed; 22 ignored; 13 architecture tests passed; quit-latency test passed

cargo test -p polariton --features toron
248 passed; 5 live-service tests ignored; 16 architecture tests passed

cargo check -p polariton --no-default-features
passed

git diff --check
passed
```

The installed `target/debug/polariton ../uat/test6.rn` launch loaded the exact
two-origin layered fixture against the local Toron workspace. Automated
real-engine evidence, rather than that smoke launch, discharges the 2x2 layout
gate so the evidence does not imply an unrecorded manual observation.
