# PRODUCT-0010 implementation evidence

## Stage 0 — preconditions

Recorded 2026-08-03 before language implementation began.

### Durable document identity: unavailable without a protocol change

Polariton cannot obtain a stable, normalized document identity for an arbitrary
`BufferId` through the current public libaryon surface:

- `EditorBuffer::file_path()` is `pub(crate)` inside libaryon
  (`engine/session.rs`).
- `PaneViewport.file_name` is a path string, but its normalization is
  unspecified and it exists only for currently projected panes.
- A buffer with no visible pane therefore has no projected path, even though it
  may still be the producer whose view identity must resolve.
- `MarkdownPreviewSnapshot.source_path` is specific to Markdown previews and is
  not a general buffer-identity channel.

Exposing a durable identity for arbitrary buffers requires a projection field or
command, which PRODUCT-0010 §9 explicitly halts on. This implementation takes
the accepted session fallback: the producer key is the runtime `BufferId`, named
views are stable across reruns in that session, and no mapping is persisted.
Durable keying and restored-layout production return in a separate record that
may extend libaryon's protocol.

Acceptance gate 6 is therefore proved across reruns, not restarts. Gate 11 is
proved by in-session orphaning. Untitled and saved buffers have the same
session-scoped behavior in this first cut.

### Dynamic `HostPanelId`: supported by the existing protocol

`HostPanelId` is an opaque frontend-defined `u64`. `OpenHostPanel` accepts an
arbitrary id. The engine's open path first looks up that id and focuses/reuses
the existing host-panel buffer when present; only an unknown id creates a new
host panel. No allocation request or new protocol capability is needed.

Polariton will own one allocator and two identity tables. Fixed application ids
remain in the low half of the namespace (`RESULT_PANEL = 1`,
`DASHBOARD_PANEL = 2`); dynamic view panels use the reserved range beginning at
`1 << 63`. Future fixed ids must remain below that boundary. The allocator is
monotonic and checked for exhaustion, so no two view identities collide within
a session.

Named identities key on `(BufferId, name)`. Anonymous identities key on
`(BufferId, presentation_position)`. Both tables resolve through the one
allocator; no call site constructs a dynamic `HostPanelId` ad hoc.

### Stage decision

The durable-identity branch is **session fallback**. Dynamic panel allocation is
implementable entirely within Polariton. Neither finding triggers the halt rule,
so Stage 1 may begin.

## Gate ledger

### Stage 1 — envelope normalization

- Gate 5: `duplicate_view_names_refuse_through_the_head_with_both_spans`
  drives the duplicate through `run_rune_code`, observes no dispatch, and checks
  both source ranges in the user-visible diagnostic.
- Gate 3 analysis half:
  `origin_resolved_identity_groups_multiple_origins_and_repeated_inline_text`
  proves distinct origin grouping and preserves presentation order separately.
- Gate 4 analysis half: the same fixture admits duplicate anonymous forms; the
  removed `DuplicateForm` and `MixedRelations` variants have no remaining source
  references.
- Gate 1 analysis half: `repeated_inline` remains one execution for two
  separately written, canonically identical origins.

Verification:

```text
cargo test -p polariton --features toron rune_relation_envelope
25 passed

cargo test -p polariton --features toron \
  duplicate_view_names_refuse_through_the_head_with_both_spans
1 passed

cargo check -p polariton --features toron
passed (Stage 2 will consume the two currently test-only program accessors)
```

### Stage 2 — independent dispatch

- Gate 1: `multi_origin_dispatch_is_independent_while_each_origin_keeps_shared_identity`
  sends two roots over one canonical origin through one adapter dispatch and
  asserts both sinks retain the result's exact `Arc`.
- Gate 2: the same test dispatches two distinct origins in reverse presentation
  order and observes two independent outcomes; scheduler coverage completes
  origins independently and starts a pending revision only after the last one.
- Gate 4: two anonymous `bar` sinks over two origins both reach dispatch and
  retain distinct anonymous positions.
- Gate 10 prerequisite:
  `one_origin_failure_does_not_cancel_its_sibling_dispatch` observes one success,
  one attributed failure, and two adapter calls. Stage 3 proves the corresponding
  stale/refresh presentation behavior.

Verification:

```text
cargo test -p polariton --features toron \
  scheduler_executes_in_flight_then_only_the_newest_pending_view
1 passed

cargo test -p polariton --features toron \
  multi_origin_dispatch_is_independent_while_each_origin_keeps_shared_identity
1 passed

cargo test -p polariton --features toron \
  one_origin_failure_does_not_cancel_its_sibling_dispatch
1 passed

cargo check -p polariton --features toron
passed (Stage 3 consumes identity/order fields carried in the outcome)
```

### Stage 3 — ordinary host-panel realization

- Gate 3: `execution_grouping_cannot_reorder_interleaved_host_panels` opens the
  table-`a`, scatter-`b`, bar-`a` panels in global presentation order, completes
  `b` before `a`, and observes the same identities and content order afterward.
- Gates 6 and 7:
  `view_identity_reuses_retargets_or_orphans_one_panel_as_declared` reruns a
  named view, changes it from table to scatter, and observes the same
  `HostPanelId` hosting the new plot content.
- Gate 8: the same test changes the form occupying anonymous presentation slot
  zero and observes reuse of that slot's session-local destination.
- Gate 9:
  `dogfood_multi_origin_views_populate_four_host_panels_without_a_dashboard`
  reproduces `/home/me/work/uat/test5.rn` as written. Its two `table()` sinks
  retain distinct ids and frames (`x`/`y` versus `cat`/`val`), alongside two
  ordinary plot panels.
- Gate 10:
  `failed_origin_keeps_prior_content_stale_while_its_sibling_refreshes` fails
  the `bad` origin first, preserves its prior table with a stale marker, then
  refreshes the `good` origin without clearing the failed panel's state.
- Gate 11: the identity lifecycle test removes `stable` from a valid revision,
  observes its panel retained and marked orphaned, then restores the name onto
  the same id.
- Gate 12: the lifecycle test submits a duplicate-name envelope refusal after a
  successful result and observes stale preserved content rather than orphaning.
- Gate 13: the dogfood test completes both tables and both plot kernels while
  `DashboardState.panes` remains `None` and `DashboardState.content` remains
  empty. `product_0010_relation_views_complete_without_dashboard_routing` is the
  secondary reintroduction guard over the completion handler.

The head allocates all four dogfood destinations from the single upper-half
counter before dispatch. `PanelContent::Plot` owns the existing `PlotPaneState`
directly, and its host surface emits plot interaction without `DashboardMsg`.
Stale and orphan markers live beside content, so neither state destroys the
last successful frame or kernel. A first-run failure instead replaces the
running placeholder with `Failed` content.

Verification:

```text
cargo test -p polariton --features toron
217 passed; 4 ignored (live-service tests), 16 architecture tests passed

cargo check -p polariton --no-default-features
passed

cargo fmt --all -- --check
passed
```

All thirteen acceptance gates are discharged. The Stage 0 session-identity
fallback remains the accepted limit: named view ids survive reruns in this
process, not application restarts. No libaryon, Toron, transport, dashboard, or
layout-policy file changed, so PRODUCT-0010's halt rule was not triggered.
