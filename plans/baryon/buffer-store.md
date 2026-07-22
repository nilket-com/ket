# Specification: Buffer Store and Object Identity

## Overview

Extract a `BufferStore` that owns mutable document truth. A file has **one** root, undo ledger, dirty
state, backing generation, language, and semantic content. Tabs and panes become **views** onto it.

This is option 2 of `plans/object-identity.md` §7, chosen over the cheaper "tab owns its document"
model for one reason that outweighs the cost: **option 1 would hard-code `document == view` into the
public Rune API.** Reversing that later is an ecosystem-breaking identity change. The buffer/view
distinction has to exist before plugins can depend on either.

It also fixes a live data-loss bug unrelated to scripting: today, opening one file in two tabs
produces two divergent undo histories over one file, and whichever saves last silently wins
(`object-identity.md` §1).

Ground truth lives in `plans/object-identity.md` and is not repeated here.

## 1. Requirements

1. A never-reused `BufferId`, owned by `BufferStore`.
2. A never-reused `ViewId` for each view. `PaneId` cannot identify a tab — a pane holds a `Vec` of
   tabs and `active_tab_index` is positional, not an identity (`object-identity.md` §2).
3. **One live writable buffer per canonical path.**
4. Buffer-owned: root, undo and dirty state, backing generation, language, semantic content.
5. View-owned: carets, viewport, selections, search position, **folds**.
6. **Mutation is coordinated so every attached view is rebased or invalidated coherently.**

## 2. The model

```
EditorSession
  buffers: BufferStore { next_buffer_id, by_id: HashMap<BufferId, Buffer>,
                         by_path: HashMap<CanonicalPath, BufferId> }
  layout: LayoutNode -> EditorPane { views: Vec<View>, active_view_index }
  next_view_id: ViewId

Buffer { id: BufferId, content: BufferContent }
View   { id: ViewId, buffer: BufferId, content: ViewContent, common: CommonViewState }
```

### 2.1 Capability is expressed by the type, on both sides

Synthetic documents **are buffers**, and content is an enum — but the enum must carry the state, not
merely tag it. Hanging a ledger and semantic state off every `Buffer`, or a caret off every `View`,
would leave a rootless markdown preview with a fake text cursor and a nullable undo history. That is
the same class of invalid state as the `NodeId(1)` placeholder caret it is meant to retire
(`object-identity.md` §6.7); removing the placeholder while keeping the shape achieves nothing.

```
enum BufferContent {
    Text(TextBuffer),                       // root, ledger, semantic, file_path, backing_path,
                                            // backing_device_id, file_stamp, language, shadow
    FileBrowser(FileBrowserBuffer),         // real nodes, synthetic content, no ledger
    MarkdownPreview(MarkdownPreviewBuffer), // rootless; content is a String snapshot
}

enum ViewContent {
    Text(TextView),                         // anchors, selection, folds, search position
    FileBrowser(FileBrowserView),
    MarkdownPreview(MarkdownPreviewView),   // scroll only; no caret
}

CommonViewState { viewport, scroll, wrap, minimap, line numbers, resolved theme }
```

**`View.buffer` stays non-optional**, and a `ViewContent` variant must match its buffer's
`BufferContent` variant. Operations are then **capability-checked by construction**: a save, an edit,
or an undo is only reachable through `TextBuffer`, not refused at each call site by a guard someone can
forget.

**There are no bufferless views.** An empty pane receives an **unnamed text buffer**; once sharing
lands, a plain split displays the *current* buffer.

An `Option<BufferId>` would weaken every downstream invariant for the sake of two synthetic kinds. It
is rejected.

### 2.2 Buffers do not point back at views

Back-pointers are one more thing to keep from dangling. View lookup walks the layout, which is cheap
(views are few) and cannot go stale.

## 3. Ownership

Note the qualifier: root, ledger, dirty, and semantic state are **`TextBuffer` capabilities**, not
properties of every buffer. A `FileBrowser` buffer has no ledger; a `MarkdownPreview` buffer has no
root. Likewise carets and folds are **`TextView` capabilities**.

| State | Owner | Note |
|---|---|---|
| root `NodeId`, text | **`TextBuffer`** | |
| `UndoLedger`, dirty | **`TextBuffer`** | `is_dirty()` is derived from the ledger |
| backing generation | **`TextBuffer`** | as of `f6ec891` |
| `file_path`, `backing_path`, `file_stamp`, `shadow` | **`TextBuffer`** | |
| `active_language_id` | **`TextBuffer`** | the *resolved theme* stays per-view (`session.rs:576-590`) |
| semantic highlights, diagnostics, `cached_sum_tree` | **`TextBuffer`** | moved from per-tab. `DocByte`-keyed and identical across views; one copy, one request, one graft |
| `pending_semantic_request_id` | **`TextBuffer`** | §7 |
| anchors, multicursor set, visual selection | **`TextView`** | §5 |
| **folds** | **`TextView`** | §4 |
| search **position** (`match_index`) | **`TextView`** | which match *you* are on |
| scroll, viewport, wrap, minimap, line numbers, resolved theme | `CommonViewState` | every view kind has these |
| `cached_minimap` | `CommonViewState` | `MinimapOverlay` embeds viewport and cursor lines (`minimap.rs:22-30`) |
| search **pattern** | session | global today; unchanged by this spec |
| search **matches** | **`TextBuffer`**, **tagged with the pattern revision** | §3.1 |

### 3.1 Search matches carry a pattern revision

The search pattern is session-global (`session.rs:1526-1527`) while matches become buffer-owned.
Without a tag, a buffer that was not re-searched retains matches for an **obsolete pattern** and
happily highlights them. Matches are therefore stamped with the session pattern revision and are
invalid unless the revision matches. This also subsumes the existing bug where stale `search_matches`
survive `LoadFile` (`object-identity.md` §6.2).

## 4. Folds

Folds are **not a field to move.** `registry.is_folded: PagedArray<AtomicBool>` (`registry.rs:109`) is
a bit per `NodeId`, process-global, and fold-awareness reaches far further than the fold commands:
`uast/projection.rs`, `engine/layout.rs`, `engine/render.rs`, `engine/document/cursor.rs`,
`engine/runtime/{edit,navigation,system}.rs`, `ecs/{registry,chunk}.rs`, and `engine/folding*`.

### 4.1 Thread an opaque fold view, not a concrete collection

All fold-aware APIs take an **opaque `FoldSet` / `FoldPolicy`**. `HashSet` — or any other concrete
storage — must not appear in those signatures. The representation is then free to change after
measurement without touching eleven files twice.

`registry.is_folded` is **deleted**, not bypassed. Leaving it in place guarantees the two diverge.

### 4.2 Storage is deferred to measurement

Fold sets are small (tens of entries) and the projection walk is the hottest loop in the engine.
**Benchmark a hash set against a sorted small collection before choosing.** §4.1 exists precisely so
that this decision can be made late. The earlier draft asserted a `HashSet` was cheap enough; that was
an assertion, not a measurement, and it is not one to make from a chair.

**Stage-3 measurement:** the release-mode, miss-heavy gate selected a sorted `Vec<NodeId>` with binary
search. At 8, 32, and 128 closed folds it measured 1.931, 3.209, and 4.657 ns/query, versus 5.301,
5.330, and 5.425 ns/query for `HashSet` on the implementation machine. The benchmark remains ignored
in the tree as `fold_set_storage_gate`; `FoldSet` keeps the representation private.

### 4.3 Fold restoration is per-mutation, not one rule

"A `NodeId`-keyed fold set survives ordinary edits" is **only true for unaffected nodes.** An edit
splices nodes, and it can detach a folded node's ancestry — including folds belonging to *another*
view, which is precisely the case that does not exist today and that this spec creates. And a range
restore is not a point restore: a range must land on a *node*, so the ambiguity a cursor never faces is
unavoidable here.

Three distinct policies, one per mutation kind:

- **Byte-identical graft** (the semantic reactor: same content, new tree). Restore a fold onto a
  **unique interior node with an exact byte range and a compatible kind**. Otherwise drop it.
- **Delta edit.** Transform fold ranges through the edit mapping, and restore **only where that mapping
  makes the correspondence trustworthy** — a fold whose range was partially overwritten has no
  defensible new extent. Drop it.
- **Reload or generation replacement** (content is different). **Drop folds** unless the operation
  supplies a trustworthy mapping. Blind exact-range matching after a content replacement would fold
  *unrelated new code that happens to occupy the same offsets* — silently collapsing something the
  user never folded, which is worse than losing the fold.

Dropping is the honest failure throughout. Today folds are destroyed by every graft anyway
(`object-identity.md` §3), so the byte-identical case is a strict improvement and the others are no
worse.

### 4.4 Consequence

Two views of one buffer get **independent folds** — folding a function in one window does not collapse
it in the other. That is Vim's behavior, and it is what the current structures actively forbid.

## 5. Carets

`Caret { node: NodeId, offset: u32, abs_line, abs_col, preferred_col }` (`session.rs:137-144`) is a
pointer into a tree that edits splice. It is the single fact that makes shared buffers hard
(`object-identity.md` §5.1).

### 5.1 A bare `DocByte` is not sufficient

An insertion exactly *at* another view's caret is ambiguous: does that caret move with the inserted
text or stay before it? A byte offset cannot express the difference. The logical type is an anchor
with **affinity**:

```
TextAnchor { byte: DocByte, affinity: Affinity /* Before | After */ }
```

`node`/`offset` and `line`/`column` become **derived caches**, recomputed from the anchor.
`preferred_col` is **independent view state** — it is not derivable from a position and must not be
reconstructed from one.

### 5.2 The existing snapshot machinery is not proof of readiness

`CursorSnapshot { primary: DocByte, secondary: Vec<DocByte> }` (`session.rs:176`) is **lossy**: it
drops `preferred_col`, sorts and deduplicates secondary carets, and `restore_snapshot` always installs
point mode — visual mode survives only because the caller separately re-applies it
(`semantic.rs:94-96`). It is a graft-recovery hack, not an anchor model.

**§5 therefore needs its own caret-anchor specification**, not a reuse of this machinery. Two live bugs
fall out of it and are fixed there:

- **`preferred_col` is silently reset by every graft** (`Caret::new` sets `preferred_col = abs_col`,
  `session.rs:143-156`) — a background semantic response landing between two `j` presses changes where
  the next `j` lands.
- **A failed restore after a graft leaves carets in the detached old subtree** (`semantic.rs:88-93`
  returns `false` *after* the mutation has run).

## 6. Mutation coordination

**Fan-out cannot live in `BufferStore`.** The store owns buffers, not the layout, so it cannot reach
the views attached to a buffer. The coordinator belongs at `EditorSession`, which owns both:

```
EditorSession::mutate_buffer(buffer_id, op) ->
    snapshot affected views          (anchors, folds, search position)
    BufferStore::apply(buffer_id, op) -> BufferMutation      // buffer truth only
    for each View with view.buffer == buffer_id:
        rebase anchors and folds from BufferMutation
        clamp scroll; invalidate minimap cache
        clamp or invalidate search position (per §3.1)
```

`BufferStore` mutates buffer truth and **returns an effect**; it does not fan out. `BufferMutation`
describes what changed (deltas, a graft, a root replacement) in terms a view can rebase against.

**Every mutation goes through this boundary — edits, grafts, reloads, and generation swaps.** Today
`apply_deltas_to_document_internal` (`document/edit.rs:464-537`) takes `&mut` to exactly one tab's
cursor, ledger, highlights, and diagnostics and rebases only that tab. That signature is the shape of
the problem.

**The acting view is distinguished**: it gets its cursor placed at the edit, as today. Other views have
their anchors rebased and scroll clamped, but are not moved to the edit site.

### 6.1 Failure semantics

**`BufferStore::apply` may never mutate buffer truth and return only an error.** If it did, the buffer
would have moved while the views did not, and no view could rebase — every attached view would be
silently wrong. This is not a theoretical concern: `rune-scripting.md` §3.12 explicitly accepts
**non-transactional** callbacks, so partial mutation followed by an error is a state the system is
designed to reach.

One of these two contracts, chosen per operation:

- **Preflight then infallible commit.** All rejection happens in a **mutation-free** preflight; once
  commit begins it cannot fail. Preferred wherever the work can be validated in advance — which is the
  same ordering principle as the backing-generation fix (`f6ec891`), where the device id is reserved
  before the save is allowed to touch the filesystem.
- **Return a `BufferMutation` even on error.** If an operation can fail *after* it has begun mutating,
  it must still describe what it managed to do, so views can rebase onto the partially-mutated buffer.
  The error is then reported alongside a valid effect, not instead of one.

Which contract applies is part of each operation's definition, not left to the implementer.

## 7. Semantic routing

Requests become **buffer-scoped**. `pending_semantic_request_id` moves to the buffer; responses route
by `BufferId` rather than by `find_doctab_with_pending_request_mut` (`session.rs:773-787`).

This removes a defect that sharing would amplify: with two tabs on one root, both would independently
decide they need a re-parse (`engine/protocol.rs:155-159`), issue duplicate requests, and both graft.

## 8. Identity, versions, and staleness

- **`BufferId` alone is the durable script handle.** It names the buffer object.
- **`(BufferId, StateId)` is a *content version*** — an optimistic-mutation precondition, not an
  object identity. A script asserts "apply this if the buffer is still at state N."
- **`ViewId`** names a view. Both `BufferId` and `ViewId` are exposed to Rune (§11).

### 8.1 `StateId` must become monotonic per buffer — and keeping `next_state_id` is not enough

`StateId` is reused today (`object-identity.md` §2.1), and the earlier draft's fix was incomplete:

- `UndoLedger::clear()` resets `current_state_id` **and** `saved_state_id` to zero, not just
  `next_state_id`.
- `restore_generation_history` (`undo.rs:236-238`) calls `clear()` and **explicitly reconstructs ids
  from zero**.

The requirement is therefore stronger:

- **A reload mints a fresh saved baseline state** from the buffer's monotonic sequence — it does not
  reset to zero.
- **Restored generations receive ids from that same sequence.**

### 8.2 Markdown preview must key on buffer, not path

`refresh_markdown_image_previews` (`markdown_image_preview.rs:209-215`) compares
`(source_path, source_state_id, scroll_line, viewport_line_count)`. Per-buffer monotonicity **alone
does not save it**: close and reopen the same path and you get a *new* buffer whose sequence restarts,
so a path-keyed comparison can still collide.

The preview tracks **source `BufferId` + `StateId`** for content and **`ViewId`** for scroll.

## 9. Lifecycle and paths

### 9.1 Resolution is placement-neutral

Lookup does **not** decide where a buffer appears:

```
resolve_or_load_buffer(path) -> BufferId
```

It canonicalizes, returns the existing `BufferId` if one is live for that path, and otherwise loads and
mints one. **It creates no views.** Placement is a separate, caller-owned decision — otherwise "an
existing buffer always gets a new view" contradicts `:edit`, which must *retarget* rather than
accumulate views.

- `:edit` — resolve, then **retarget the current view** to that buffer.
- `:tabedit` — resolve, then **create a view** in the current pane.
- A plain **split** — **attach a second view to the current buffer** (once sharing lands). This is the
  feature that motivates the whole spec.

### 9.2 `CanonicalPath` needs a definition

Not a call to `std::fs::canonicalize`: that function **fails on a path that does not exist**, which is
exactly `:w newfile.rs` and opening a file about to be created. The rule must cover nonexistent targets
— canonicalize the longest existing ancestor and append the remainder, or equivalent — and must be
specified here, not inherited from the standard library's behavior on the happy path.

### 9.3 `WriteFileAs` reserves before it writes

Save-as is the one operation that can make two buffers claim one path — the state requirement 3
forbids. The ordering matters:

1. **Reserve/check the target path key** in `by_path` — **before touching the filesystem.**
2. A collision with another live buffer **fails the save, leaving the target file unmodified.**
3. Only after the save succeeds, **commit the re-key** (drop the old key, install the new one)
   atomically.

This is the same ordering principle as the backing-generation fix (`f6ec891`): reserve the identity
before the operation is allowed to make any filesystem change, so a rejection can never leave a
half-written target. `WriteFileAs` must also re-detect the language and re-resolve the theme, which it
does not do today (`object-identity.md` §6.5).

**Close a view.** Detach. If it was the last view of the buffer, **drop the buffer**: unregister the
backing generation, discard the shadow, cancel the metric scan. `ClosePane` already does all three as
of `f6ec891`; this makes it structural rather than a per-site obligation.

**No hidden buffers.** A buffer with zero views is dropped. `:bufhidden` semantics are a later
decision, not a default.

**Unnamed buffers** have no path and are never deduped; they are keyed by `BufferId` only.

## 10. Prerequisite fixes, as explicit prior commits

1. **`StateId` monotonicity** (§8.1) — including `clear()` and `restore_generation_history`.
2. **Stale `search_matches` survive `LoadFile` / `ReloadFile`** (`object-identity.md` §6.2) — subsumed
   by the pattern revision of §3.1, but the bug is fixable first and independently.
3. **`WriteFileAs` does not re-detect language or re-resolve the theme** (`object-identity.md` §6.5).
4. **`has_closed_folds` goes stale** (`object-identity.md` §6.6). Under §4 it becomes a cache of *view*
   state; recompute at every fold mutation, or drop it.
5. **`diff_pairs` entries are never removed** (`object-identity.md` §6.4). Note the rationale correctly:
   `diff_pairs` is keyed by **`PaneId`**, not by a tab or view id, and it is safe today only because
   `PaneId` never recycles. `ViewId` does not change this; the leak is independent and should be fixed
   on its own.

`ClosePane`'s shadow, metric-scan, and backing-device leaks were fixed by `f6ec891` and are **no longer
outstanding**; `object-identity.md` §6.3 is stale and is corrected in the same commit as this spec.

## 11. Decided

- **Both `BufferId` and `ViewId` are exposed to Rune.** Requirement 2 gives views identity; plugins
  need to name both, as `nvim_buf_*` / `nvim_win_*` do.
- **Diffing a buffer against itself is rejected** as a degenerate operation. Under shared buffers,
  `:panediff` on the currently-open path would otherwise attach a second view to the same buffer and
  diff it against itself.
- **`NodeId` non-reuse and node reclamation stay out of scope** (`object-identity.md` §10.6). Buffers
  are now dropped explicitly, which is the first natural place to release a root — but reclaiming nodes
  would reintroduce id reuse, and non-reuse is load-bearing for identity safety. The tension is
  recorded, not resolved.

## 12. Staging

**"No buffer-sharing behavior until the final stage."** Not "zero behavior change" — the prerequisite,
caret, and fold stages deliberately fix visible bugs, and claiming otherwise would be false.

1. **Prerequisite fixes** (§10), each on its own.
2. **Caret anchors** (§5) — `TextAnchor { byte, affinity }`, derived caches, `preferred_col` as
   independent view state. Fixes two bugs; independent of the store.
3. **Folds leave the registry** (§4) — opaque `FoldSet` threaded through the fold-aware surface, with
   restoration under the per-mutation policies of §4.3. **Gated on the §4.2 benchmark**: the storage
   choice is a prerequisite to this stage, not an afterthought within it. Still one view per document.
4. **`ViewId`** minted and threaded.
5. **`BufferStore` extracted**; `Buffer` and `View` split by capability (§2.1), and
   `EditorSession::mutate_buffer` becomes the sole mutation boundary (§6). Still exactly one view per
   buffer — the fan-out runs with N=1.
6. **Semantic content and routing move to the buffer** (§7). Fan-out still N=1.
7. **Canonical-path dedup and shared views** (§9). The first stage where two views can observe one
   buffer.

The fan-out therefore runs with N=1 through steps 5 and 6 before step 7 makes N greater than one.

## 13. Non-goals

- Hidden buffers, buffer lists, `:ls`.
- Reclaiming `NodeId`s (§11).
- Changing the search *pattern*'s session-global scope (§3).
- NeoVim API compatibility. The buffer/view *distinction* is adopted; the API is not
  (`rune-scripting.md` §4).
