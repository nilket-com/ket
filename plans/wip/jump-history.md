# Stamped Jump History (`Ctrl-o`, `Ctrl-i`)

Status: implemented and validated 2026-07-28. Stage 6.1 of code navigation.

## Purpose and scope

Add bounded, meaning-preserving backward and forward history for successful
go-to-definition navigation. `Ctrl-o` returns to the location where `gd` or
`:definition` was invoked, including across files; `Ctrl-i` reapplies a jump
after moving backward.

This stage records definition jumps only. It does not turn every cursor motion
into a jump or expose a jumplist UI. `Ctrl-i` and terminal Tab share the
forward action in normal mode; insert-mode Tab and prompt completion are
unchanged.

## Why offsets alone are insufficient

A byte offset is bounds-safe after clamping but not meaning-safe after edits.
History positions therefore use buffer-owned tracked text anchors:

```
TrackedAnchor = (root NodeId, StateId, TextAnchor { byte, affinity })
JumpEntry     = (BufferId, preferred ViewId, TrackedAnchorId)
```

`Affinity::After` gives cursor-like behavior for insertions at the saved
position. The existing text-mutation coordinator already obtains the exact
`FoldEdit` sequence for ordinary edits, undo, and redo. At that boundary it
rebases every tracked anchor for the mutated buffer with the same transform
used for carets.

Rules:

- ordinary edit / undo / redo: rebase the anchor and advance its `StateId`;
- topology-only graft: no change, because bytes and content identity match;
- root replacement, or a state transition without recoverable deltas:
  invalidate the buffer's tracked anchors;
- jump application: require the anchor's root and state to equal the live
  document and require its byte to be in bounds;
- never clamp an invalid or stale history position.

Stale entries are removed while searching backward. If no valid entry remains,
the command reports that no older jump is available.

## Buffer lifetime and bounds

Cross-file phase-6 navigation retargets the initiating view. Without another
owner, that releases the source buffer immediately, making exact jump-back
impossible and potentially forcing a disk reopen with different bytes.

The jump history is therefore a real buffer owner:

- `release_buffer` keeps a zero-view buffer while any back/forward entry
  references it;
- returning to the entry reuses the retained `BufferId`, exact root, undo
  ledger, and in-memory bytes;
- removing or evicting an entry also removes its tracked anchor;
- a now-unreferenced zero-view buffer is released normally.

Back and forward entries have one combined limit of 100. Oldest entries are
evicted first. A new definition jump clears forward history. This bounds both
anchor state and clean detached buffers retained solely for navigation.

A jump that would retarget the last view of a dirty departure buffer is
refused, unless the target is already visible or is in the same buffer. This
prevents a bounded-history eviction from later becoming an implicit discard of
hidden dirty content.

## Recording and application

A history entry is committed only after the definition result and destination
range have passed all validation, immediately before the successful cursor
move/retarget. Its byte is the stamped invocation cursor, not a cursor that may
have moved while the asynchronous request was in flight.

`Ctrl-o` and `Ctrl-i` use the same application path in opposite directions:

1. Examine the newest entry in the selected direction, removing stale entries.
2. Resolve its tracked anchor against the retained target buffer.
3. Prefer its original `ViewId` when that view still displays the buffer;
   otherwise use any visible view for the buffer.
4. If the buffer is detached, retarget the currently focused text view after
   applying the dirty-departure rule.
5. Capture the departure position on the opposite stack, consume the selected
   entry, focus/activate the target view, collapse multiple cursors, and place
   the primary cursor at the exact anchor byte.

Consecutive duplicate origins are coalesced. Failed definition requests,
stale async results, unsupported providers, invalid targets, and refused dirty
retargets do not alter history.

## Input and tests

- Add `EditorCommand::JumpBack`.
- Add `EditorCommand::JumpForward`.
- Bind normal-mode `Ctrl-o` backward and both `Ctrl-i`/terminal Tab forward;
  both have collapse-multicursor policy.
- The command is engine-owned and frontend-neutral.

Tests cover:

- same-file and cross-file definition jump-back;
- cross-file backward → forward round trip and exhausted-forward status;
- exact source-buffer retention across a cross-file retarget;
- insertion/deletion rebasing, including affinity at the anchor;
- undo/redo rebasing;
- topology-only graft stability;
- root replacement invalidation without clamping;
- stale-entry skipping;
- preferred-view fallback and detached-buffer retarget;
- dirty last-view refusal;
- combined history bound, forward clearing, and buffer cleanup;
- failed/stale definition outcomes not recording history; and
- `Ctrl-o` dispatch.

## Implementation sequence

1. Add buffer-owned tracked anchors and mutation-boundary rebasing tests.
2. Add the bounded back/forward history and retained-buffer cleanup.
3. Record successful definition origins and implement jump-back application.
4. Add the command/key binding and end-to-end navigation tests.
5. Regenerate catalogs, run workspace tests and production Clippy, then commit.

All five steps are complete. The shipped implementation keeps tracked anchors
on text buffers, rebases them through ordinary edit/undo/redo deltas,
invalidates them on root replacement, and retains zero-view buffers only while
the bounded back/forward history owns them. `Ctrl-o` and `Ctrl-i` are
frontend-neutral and return to exact retained buffers and rebased bytes.
