# PRODUCT-0011 implementation evidence

Recorded 2026-08-03 after the full verification pass.

## Audit finding

The proposed placement shape was implementable without a new projection field
or layout primitive. A host panel is already an ordinary buffer/view and an
`EditorPane` already owns an ordered tab stack.

The audit found one previously invisible projection defect: inactive host-panel
tabs would be titled `_` because `DocTabSummary.title` consulted file paths but
not `HostPanelDescriptor`. PRODUCT-0011 was amended before implementation so an
existing summary uses the descriptor title for a host-panel buffer. No projection
shape or tab chrome changed.

## Gate ledger

1. **Split behavior preserved.**
   `host_panels_project_identity_dividers_and_refcounted_disposal` continues to
   open a host panel in a new pane and observes its projected divider.
2. **Live TabWith request.**
   `host_panel_placement_requests_tab_fallback_and_reopen_without_moving` opens
   an anchor split, requests a sibling tab, and observes unchanged pane count,
   unchanged source focus, unchanged active anchor, and descriptor titles in
   request order.
3. **Unavailable target is advisory.** The same test requests `TabWith` a
   missing identity and observes the panel in a fallback pane with no placement
   failure.
4. **Placement is first-appearance-only.** The same test reopens the inactive
   sibling with a conflicting `Split` request and `focus: true`. Pane count and
   tab order remain fixed while the exact sibling tab activates and receives
   focus.
5. **Tab disposal remains refcounted.**
   `closing_one_host_panel_tab_disposes_only_that_panel` closes the sibling by
   stable `view_id`, observes exactly its `PanelDisposed`, and observes the
   anchor tab and pane still live.
6. **Polariton collection policy.**
   `rune_view_collection_opens_one_split_with_ordered_host_tabs` drives four
   semantic views through `CodeEditor::prepare_rune_view_panels` and the live
   engine. The projection contains the original document pane plus one view
   pane, four descriptor-titled tabs in PRODUCT-0010 presentation order, the
   first tab active, and source focus retained. Existing
   `opening_query_results_keeps_workspace_focus_in_the_source_pane` and
   `opening_dashboard_creates_and_focuses_its_host_panel` remain green with
   their original split directions.

## Implementation boundary

`HostPanelPlacement` has exactly `Split { direction }` and
`TabWith { panel_id }`. It is carried only by `OpenHostPanel`; there is no result
or decline event. A missing tab target currently falls back to an ordinary
horizontal split, but that direction is engine policy rather than protocol
contract.

New tab views inherit dimensions from the target pane. `focus: false` changes
neither pane focus nor its active tab. `focus: true` activates the requested
panel's exact view before focusing its pane. Existing identities ignore all
placement metadata, so reruns cannot relocate author-arranged panels.

Polariton resolves the first view id before opening the collection. The first
unknown panel requests its existing horizontal split; later unknown panels
request `TabWith` the first. Known panels still issue only
`RefreshHostPanel`. Query results and the dashboard mechanically retain their
existing split requests.

No Toron, transport, dashboard, plotting-language, projection-structure,
persistence, or durable-identity file changed. The halt rule did not fire.

## Verification

```text
cargo test -p libaryon
1401 passed; 22 ignored; 13 architecture tests passed; quit-latency test passed

cargo test -p polariton --features toron
218 passed; 4 live-service tests ignored; 16 architecture tests passed

cargo check -p polariton --no-default-features
passed

cargo fmt --all -- --check
passed
```
