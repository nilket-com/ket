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

Stages append their behavioral evidence here as they land.
