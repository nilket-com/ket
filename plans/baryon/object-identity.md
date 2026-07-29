# Ground Truth: Object Identity

Evidence and consequences for `plans/rune-scripting.md` §3.3 and §3.4 — what a script may name, what
survives, and what a stale handle observes. **This document deliberately makes no recommendation.**
It establishes what is true, states the fork, and stops.

The framing question is not "can two panes share a buffer?" It is **which state follows the buffer and
which follows each view** — and Baryon's answer to that today is neither Vim's nor VS Code's, but a
third thing that was never written down.

## 1. The ownership graph

```
EditorSession                                  session.rs:1518-1536
  layout: LayoutNode                  (owned)   :1519
  focused_pane_id: PaneId             (by id)   :1520
  diff_pairs: HashMap<PaneId, DiffPair>(by id)  :1522
  next_pane_id / next_semantic_request_id / next_dynamic_device_id  (allocators) :1533-1535

LayoutNode::Split { children: Vec<LayoutNode>, ratios }   (owns children)  :700-707
LayoutNode::Leaf(LayoutLeaf { pane_id, pane: EditorPane }) (owns pane)      :695-698
EditorPane { tabs: Vec<DocTab>, active_tab_index: usize }  (owns tabs)      :669-672
DocTab { document, cursor, view, search_matches, match_index, match_info, semantic }  :592-600
DocumentSession { kind, root_id: Option<NodeId>, file_path, backing_path,
                  file_stamp, active_language_id, shadow, ledger }          :88-97
```

**The chain breaks at the UAST.** `DocumentSession.root_id` is a *handle*, not ownership. The tree
lives in the process-global `Arc<UastRegistry>` (`engine/core.rs:206`). Nothing in the session tree
owns a node.

**`DocTab` is owned by exactly one pane, by value, in a `Vec`.** There is no `Rc`, no `Arc`, no tab
arena. **Two views of one buffer is not expressible without changing this type.**

**Opening the same path twice produces two independent documents.** `load_root_from_path`
(`runtime/system.rs:164-188`) unconditionally allocates a *fresh dynamic device id* and re-ingests,
yielding a distinct root, a distinct `UndoLedger`, and a distinct `StateId` sequence. There is no
root-by-path map anywhere. Two tabs on one file today = two divergent edit histories over one file on
disk.

**Paths are raw `String`s and are never canonicalized** in the document pipeline (`canonicalize()`
appears only in `engine/git.rs:59` and `svp/rust_provider.rs:429`). So `./x.rs`, `x.rs`, and
`/abs/x.rs` are three different documents.

### The model was chosen, not stumbled into

`plans/doctab.md` states the intent: *"Encapsulate all buffer-specific state into a new `DocTab`
struct"*, under a **"Modern Pane" hierarchy explicitly modelled on VS Code** — panes contain tabs, a
tab owns its document. It also places theme, wrap, and minimap in a per-tab `SettingsLocal`.

This matters for the fork in §7: option 1 is not "keep the accident." It is "keep the decision."

## 2. Identifiers

| Id | Type | Minted | Monotonic | **Reused?** |
|---|---|---|---|---|
| `NodeId` | `NonZeroU32` (`ecs/id.rs:9`) | `reserve_ids`, CAS bump on `next_id` (`registry.rs:235-256`) | yes, global | **Never reused — and never freed.** |
| `PaneId` | `u32` (`core/coords.rs:155`) | `next_pane_id.next()` (`session.rs:1759-1760`) | yes | Never reused; `close_focused_pane` does not return the id |
| `StateId` | `u64` (`core/coords.rs:179`) | **per-document** `UndoLedger.next_state_id` (`undo.rs:57,73`) | within one ledger lifetime only | **REUSED — see §2.1** |
| `RequestId` | `u64` | `next_semantic_request_id` (`session.rs:1534`) | yes, session-global | Never reused |
| `active_tab_index` | `usize` | positional | no | **Not an identity at all** — `tabs.remove` shifts every later index (`runtime.rs:215-218`) |
| device id | `u16` | `next_dynamic_device_id` (`system.rs:170`) | yes | Never recycled — **but see §6.1** |

### 2.1 `StateId` is reused, and one consumer relies on it not being

`UndoLedger::clear()` (`undo.rs:140-145`) resets `current_state_id`, `saved_state_id`, and
`next_state_id = StateId::new(1)`. It runs on **every load and every reload** (`system.rs:229`). After
a reload, `StateId(1)`, `StateId(2)`… are handed out again for entirely different content on a
different root.

**`StateId` equality is therefore not a safe staleness test.** The semantic pipeline survives only by
accident: `RequestId` is monotonic and routing (`session.rs:773-787`) drops the orphaned response
*before* the `StateId` comparison at `engine/protocol.rs:71-75` ever runs.

One consumer does rely on it: `refresh_markdown_image_previews`
(`markdown_image_preview.rs:209-215`) compares only `(source_path, source_state_id, scroll_line,
viewport_line_count)` — no root, no `FileStamp`. **Same path + reload-restarted `StateId` + same
scroll ⇒ the preview believes it is fresh and skips the rebuild.**

`CachedSumTree` is immune because it also keys on `root` and `topology_revision`
(`session.rs:29-37`, `engine/core.rs:165-179`).

### 2.2 `NodeId` slots are reclaimed with non-repeating packed generations

ADR 0001 stages 1–4 first removed the pathological callers, reclaimed orphaned
payload, and made capacity exhaustion non-panicking. ADR 0002 then added slot
reuse: the low 24 `NodeId` bits encode `slot + 1`, while the high 8 bits encode
a per-slot object generation. A generation is never repeated for one slot;
generation-255 retirement burns that slot rather than wrapping.

Retirement still passes through the stage-3b publication fence and bounded
reclaim cursor. A visited slot is not reusable until the old object lifetime
has zero resolver I/O leases. This prevents a late completion from clearing
or publishing into a new object after reuse.

Capacity remains fixed at `UastRegistry::new(capacity)` (16M in production),
but exact reservations draw from reclaimed slots plus the bump tail and
refuse atomically when that combined supply is insufficient. Stage 4 keeps
refusal graceful for genuine live, pending, or burned-slot pressure.

**The load-bearing identity property remains:** a stale `NodeId` never denotes
a different live object. Checked registry entry points compare the packed
generation with the slot's current generation. Once a reclaimed slot becomes
allocator-eligible, stale topology/metric reads are inert; after reuse they
fail the identity check. Resolver publication additionally checks the separate
monotonic `content_generation`, because object lifetime and byte lifetime are
different questions. A failed content-generation increment permanently closes
resolver admission for that slot and burns it at retirement; it never wraps.
The validation-to-I/O-lease transition shares the allocator lock with
retirement, so first reuse cannot occur in the gap between those two operations.

### 2.3 The existing stale-rejection precedent

Semantic responses are rejected by a **two-factor test** (`engine/protocol.rs:71-75`):

```rust
if Some(request_id) != semantic.pending_semantic_request_id
    || state_id != document.ledger.current_state_id { return false; }
```

Routing first: `find_doctab_with_pending_request_mut` (`session.rs:773-787`) walks the layout for a tab
claiming that `RequestId`; **no claimant ⇒ the response is dropped silently.** Note what the test does
*not* check: `root_id`.

This remains the content/publication-level stale-rejection model. It now
complements, rather than substitutes for, `NodeId` object-generation checking.

## 3. Lifecycle: what happens to identity

| Operation | root `NodeId` | ledger / `StateId` | cursor | folds | semantic |
|---|---|---|---|---|---|
| **Ordinary edit** | **same rid, mutated in place** (`edit.rs:482,522`) | new `StateId`, redo cleared | re-derived from post-edit byte | **force-opened** on the edit path (`edit.rs:484-491`) | highlights/diagnostics *rebased* |
| **Undo / redo (text)** | same rid; **inverse deltas replayed**, not a snapshot restore (`edit.rs:1917-1942`) | `current_state_id` moves | restored from `CursorSnapshot` (bytes) | **not restored** — stay open | rebased |
| **Save** | **unchanged.** `rebind_document_spans_to_saved_file` (`io.rs:343-380`) re-points leaf *spans* at the file and drops resident bytes; topology and NodeIds untouched | `mark_saved()`; history kept | untouched | survive | untouched |
| **Save-as** | **unchanged — identity retained** (`system.rs:1228-1255`) | kept + `mark_saved` | untouched | survive | untouched, and **now stale for the new path** |
| **Load / reload** | **NEW root** (`system.rs:227`); old root only gets `cancel_metric_scan` | `ledger.clear()` — **history destroyed, `StateId` restarts** | collapsed, then line/col restored | **all lost** | reset |
| **Generation substitute** | **NEW root** (`search.rs:196`) | `push_swap_generation` | re-placed | **lost** | reset |
| **Undo/redo of a swap** | **NEW root every keystroke** (`edit.rs:1954`) | moves | re-placed | lost; `has_closed_folds` **not** reset | reset |
| **`DocTabNew`** | new 2-node root | fresh | new caret | none | fresh |
| **`DocTabClose`** | **nothing released** (`runtime.rs:203-227`) | dropped | dropped | — | — |
| **Pane split** | `EditorPane::new_empty` — **`root_id = None`**, caret = placeholder `NodeId(1)` (`session.rs:608,621-626`) | fresh | dangling by construction | — | — |
| **`ClosePane`** | **nothing released, no shadow discard, no `cancel_metric_scan`, no `diff_pairs` cleanup** (`session.rs:1856-1875`) | dropped | dropped | — | — |
| **Diff split** | a **completely independent second document** — own root, ledger, device id. Shares nothing but the registry arena. Linked only by `diff_pairs` line ranges | — | — | — | — |
| **File browser** | real nodes, synthetic content; root **reused** across refreshes (`file_browser.rs:241-273`) | cleared | collapsed | — | cleared |
| **Markdown preview** | **`root_id = None`** (`markdown_image_preview.rs:373`) — rootless; content is a `String` snapshot | cleared | collapsed | — | cleared |
| **Semantic graft** | **same root, subtree rebuilt with all-new NodeIds** (`structural_graft.rs:21-58`) | unchanged | **snapshotted to bytes and restored** (`semantic.rs:83,88-93`) | **all lost** (new nodes default `is_folded=false`) | installed |

### 3.1 The graft is the sharpest case

`apply_graft` detaches every existing child (`parent = None`), allocates a brand-new tree, and splices
it under the *same* root. Carets survive **only because the caller snapshots them to document bytes
first** and re-resolves after (`semantic.rs:83-93`). That snapshot covers exactly one `DocTab` — the
one that issued the request.

Two collateral losses, both silent:
- **`preferred_col` is reset by every graft.** `caret_from_doc_byte` → `Caret::new` sets
  `preferred_col = abs_col` (`session.rs:143-156`). A background semantic response landing between two
  `j` presses changes where the next `j` lands.
- **If `restore_snapshot` fails, `apply_graft` has already run** (`semantic.rs:88-93` returns `false`
  *after* the mutation), leaving carets pointing at detached nodes.

## 4. Which state follows the buffer, and which follows the view

| State | Where it lives | Class | Note |
|---|---|---|---|
| text / UAST | registry, by `NodeId` | **buffer** | already shared and globally addressable |
| `UndoLedger` | `DocumentSession` | **buffer** | matches Vim |
| **dirty flag** | *derived*: `is_dirty() = current_state_id != saved_state_id` (`undo.rs:153-155`) | **buffer** | not a field — fused to undo. Matches Vim |
| `file_path`, `backing_path`, `file_stamp`, `shadow` | `DocumentSession` | **buffer** | |
| `active_language_id` | `DocumentSession` | **buffer**… | …but it *resolves* into `ViewState.current_theme` (`session.rs:576-590`), so a language change must re-push into every view (`re_resolve_auto_themes`, `system.rs:400-412`) |
| cursor, multicursor set, visual selection | `CursorState` | **view** | `CursorSet` max 1024 (`session.rs:169`) |
| scroll, viewport, wrap, minimap, line numbers, theme | `ViewState` | **view** | |
| **folds** | **`registry.is_folded: PagedArray<AtomicBool>`, per-`NodeId`, process-global** (`registry.rs:109`) | **neither** | see §4.1 |
| semantic highlights, diagnostics | `SemanticState` per-`DocTab` | **buffer data, stored per-view** | `HighlightSpan{start: DocByte, end: DocByte}` — nothing view-relative. Identical for two views, yet rebased per-tab (`edit.rs:517-518`) |
| `pending_semantic_request_id` | `SemanticState` per-`DocTab` | **the routing key** | §5 |
| `cached_sum_tree` | `SemanticState` per-`DocTab` | **buffer** | every field derives from `(registry, root)` — a per-view copy of a per-buffer index |
| `cached_minimap` | `SemanticState` per-`DocTab` | **genuinely mixed** | body is content; `MinimapOverlay` embeds `viewport_start_line`, `cursor_line`, `active_search_band` (`minimap.rs:22-30`) |
| search **pattern** | **`EditorSession`** (`session.rs:1526-1527`) | **global** | |
| search **matches**, `match_index` | per-`DocTab` | **split** | matches are document coordinates (buffer); which one you're on is view |

### 4.1 Folds are the one place the structures actively forbid the Vim model

In Vim, undo and dirty follow the **buffer**; folds and cursor follow the **window**.

Baryon gets the first half right by accident of `UndoLedger` living on `DocumentSession`. It gets folds
**inverted**: fold state is a bit per `NodeId` in the process-global registry (`registry.rs:109`,
`folding/state.rs:36-64`). There is no per-window fold state and **no place to put one**. The only
per-view fold artifact is a derived cache, `CursorState::has_closed_folds` — which is itself stale
after a graft, a swap, or an edit (`system.rs:1006` is the only place it is recomputed).

Two views of one buffer would be **forced to share fold state**: folding a function in one window
would collapse it in the other.

### 4.2 Search state is split across three levels and none of them is the buffer

The pattern is global (`EditorSession`), the matches are per-tab, the index is per-tab. Whatever the
fork chooses, search does not currently fit any of the three levels cleanly.

## 5. Could two views share content today?

**Reads: already safe.** `UastRegistry` is `Arc`, `unsafe impl Sync` (`registry.rs:156-162`), and every
API is `(registry, root: NodeId)` — nothing is keyed by `DocTab` or pane. Two `DocTab`s on one root
would render correctly today, and would even share the resident-byte cache. There is already one
pane-keyed hook designed for N viewers: `set_viewport_resident_pins(owner, nodes)`
(`registry.rs:513-522`), whose eviction skips any node pinned by **any** owner (`:559-561`).

**Writes: broken in six places.**

1. **Carets are `NodeId`-relative.** `Caret { node: NodeId, offset: u32, abs_line, abs_col, preferred_col }`
   (`session.rs:137-144`) is a pointer *into the tree*. Edits split and splice nodes; only the editing
   tab's cursor is re-placed (`edit.rs:523-535`). A second view's carets would hold nodes whose content
   and length changed, with cached `abs_line`/`abs_col` that nothing rebases. **This is the single most
   consequential fact in this document.**
2. **Highlights and diagnostics are rebased per-tab** (`edit.rs:516-517`). The other view's
   `DocByte`-keyed spans would silently point at the wrong bytes.
3. **Semantic responses route to exactly one `DocTab`** — `find_doctab_with_pending_request_mut`
   returns the *first* tab claiming the `RequestId`. The other view would never be updated, and both
   would independently decide they need a re-parse (`engine/protocol.rs:155-159`), issuing duplicate
   requests and both grafting.
4. **`apply_graft` restores only the dispatched tab's cursors** (§3.1). The second view's carets would
   still hold `NodeId`s from the detached old subtree — readable, orphaned, and resolving to silently
   arbitrary document bytes.
5. **`unfold_ancestor_chain` on the edit path** (`edit.rs:484-491`) mutates shared registry fold bits.
6. **`cancel_metric_scan(root)` on close** (`runtime.rs:209-211`) — closing one of two shared views
   would kill the other's in-progress line-count scan, permanently leaving `exact_line_count(root)`
   as `None`.

**Where mutable aliasing is assumed impossible.** The borrow checker prevents two `&mut DocTab`. It
does **not** prevent two `DocTab`s holding the same `root_id`, because the registry is reached through
`&UastRegistry` with interior mutability. The invariant "one editor per root" is entirely unenforced —
the `unsafe impl Sync` contract (`registry.rs:156-160`) only says *one thread*, not *one editor*.
Every mutation path (`active_tab_mut`, `find_pane_mut`, `for_each_doctab_mut`) hands out one
`&mut DocTab` and mutates registry state through a shared reference.

## 6. Bugs found, independent of this project

These are not identity-design questions. They are defects, and they should be fixed on their own
schedule.

### 6.1 Fixed: saving two files reused one global `FILE_DEVICE_ID`

**Severity: data corruption. REPRODUCED. FIXED.**

The P1 fix makes a device id identify one immutable backing generation. Each save attempt now reserves
a fresh id from the session's `next_dynamic_device_id` before writing; after the file write succeeds,
it registers the saved file through a fallible resolver API and only then rebinds spans and discards
resident bytes. Duplicate device-id registration is rejected instead of replacing the existing
backing. Allocation exhaustion is checked before the save can modify its target.

`DocumentSession` owns the device id for its current reachable root. A successful rebind, file
load/reload, or shadow-generation replay retires the replaced device from both the registry and the
platform resolver; tab close, pane close, and quit retire the final device. Descriptor use is therefore
bounded by live documents rather than by the number of saves or loads. The io_uring backend retains an
`Arc<File>` for every submitted read until its completion, so unregistering a map entry cannot close and
reuse its raw fd under an in-flight SQE.

This retirement relies on a current invariant: replaced roots remain allocated, but are unreachable
from every document and are never read again. Their spans still carry the retired device id. Any future
feature that makes detached roots readable must either retain their backing generation or change this
lifetime model; silently reading them after retirement would be a correctness bug.

If the disk write succeeds but the new backing cannot be registered, the save is still recorded as
clean and the current tree retains its old backing rather than being rebound. Physical-span streaming
prefers that immutable registered handle over `backing_path`, so it remains correct even if a pathname
has been replaced or a shadow-generation file has been unlinked.

The public `EngineHandle` regression saves A, saves B, cold-reads a later extent from A, and saves A
again. It verifies both that A renders A's bytes and that A remains byte-for-byte intact on disk. The
registration tests separately verify that missing paths install nothing and duplicate ids cannot
redirect an existing backing.

The original reproduction was rerun against the fix and independently confirmed both sides: A renders
A rather than B after the cold read, and re-saving A writes no B bytes. This also confirms that the old
write-corruption path was reachable, not merely inferred.

The original root cause was:

- `FILE_DEVICE_ID` is a single constant `0x42` (`engine/core.rs:24`), used for **every** saved
  document at the time.
- `register_device_path` **overwrote**: `device_paths.insert(device_id, path)`
  (`registry.rs:640-646` at the time).
- Every save called `register_device(FILE_DEVICE_ID, path)` then
  `rebind_document_spans_to_saved_file(.., FILE_DEVICE_ID)` (`system.rs:1209-1210`), which points every
  leaf span at device `0x42` **and calls `drop_resident_data(node)`** (`io.rs:376`) — discarding the
  in-memory bytes, so future reads *must* go to the device.

Save file A, then save file B: A's leaves still carry `device_id = 0x42`, but device `0x42` now
resolves to B's path. Any subsequent cold read of a paged-out leaf of A reads bytes out of B. Nothing
re-binds A on focus.

**Reproduced** through the public `EngineHandle` API — no scripting, no TUI, no unsafe access:

```
BARYON_RESIDENT_MIB=4, two ~12 MiB files A ("AAAA-alpha-AAAA") and B ("BBBB-bravo-BBBB")

1. open A, edit, :w        -> device 0x42 -> A; A's leaf bytes dropped by rebind
2. :tabedit B, edit, :w    -> device 0x42 -> B  (mapping OVERWRITTEN)
3. back to tab A, :400000  -> tab A renders "BBBB-bravo-BBBB"
```

A resident budget below the file size is what forces the cold read; on small files the bytes stay
resident and the bug is masked, which is why it has not been noticed. Two edited-and-saved files in one
session over the resident budget is not an exotic state.

The fix is not an identity-design question. The session allocator already existed for loads; the save
path now uses the same allocator for a new backing generation rather than collapsing onto shared
device `0x42`.

### 6.2 Stale `search_matches` survive `LoadFile` and `ReloadFile`

`initialize_loaded_document` (`system.rs:190-238`) clears the ledger, semantic state, cursor, and
scroll — but never touches `search_matches` / `match_index` / `match_info`. The generation-swap path
(`search.rs:208-210`), the undo-swap path (`edit.rs:1963-1965`), the file browser, and the markdown
preview all *do* clear them. So `:e other.rs` keeps and highlights the previous file's match positions.

### 6.3 `ClosePane` leaked more than `DocTabClose` — **FIXED in `f6ec891`**

`close_focused_pane` dropped the pane and all its tabs with no `discard_shadow_session` (recovery dirs
and generation files left on disk, so the file would be "recovered" on next open) and no
`cancel_metric_scan` (background scans running against detached roots), while `DocTabClose` and `Quit`
both discarded shadows.

`ClosePane` now cancels the metric scan, retires the backing device, and discards the shadow
(`runtime.rs:961-972`). **`diff_pairs` removal is still missing** — see §6.4, which is a genuinely
separate leak.

### 6.4 `diff_pairs` entries are never removed

Only ever `insert`ed (`diff.rs:57,64`). Closing a diffed pane leaves a `PaneId` key pointing at a
nonexistent pane forever, and `recompute_all_diffs` iterates the map every time. **Not exploitable
today only because `PaneId` never recycles** — if a buffer store ever introduces id reuse, this
becomes a wrong-object bug. Same for `browser_target_pane_id`, `PendingReload.pane_id`, and
`MarkdownImagePreviewState.source_pane_id`.

### 6.5 `WriteFileAs` does not re-detect language or re-resolve the theme

`system.rs:1228-1255` mutates `file_path` without calling `detect_language`, and the `WriteFile*`
dispatch (`runtime.rs:1279-1311`) never calls `apply_resolved_theme_to_tab`. `:w foo.rs` from an
untitled buffer stays plaintext until reload.

### 6.6 `has_closed_folds` goes stale

Recomputed only in `handle_fold_command` (`system.rs:1006`). Not recomputed after a graft, a generation
swap, or an edit's `unfold_ancestor_chain`. It can remain `true` against a tree with zero folded nodes.

### 6.7 A new pane's caret is a dangling reference by construction

`EditorPane::new_empty` gives `root_id = None` and a hardcoded placeholder caret at
`NodeId(NonZeroU32::new(1))` (`session.rs:608,621-626`) — **a real, live node belonging to some other
document.** Guarded everywhere by `let Some(root) = … else { return }`, but the reference is wrong by
construction rather than absent.

### 6.8 Fixed: a semantic graft rewrote non-UTF-8 documents

A P1 data-corruption bug, in the same family as §6.1 and found while inventorying the caret surface for
the caret-anchor spec. The semantic pipeline sent the reactor a `String::from_utf8_lossy` of the live
bytes (`semantic.rs:173`); the reactor parsed *that* String and returned its bytes
(`svp/semantic.rs:89-90`); `apply_graft` installed those bytes into the leaves (`semantic.rs:86`).

For any document that is not valid UTF-8 — a latin-1 CSV, TOML, or SQL file, all of which have
structural providers — a background graft therefore replaced the offending bytes with U+FFFD **and
truncated the tail**: the reactor's bytes were longer than the root's declared length, so subsequent
reads clipped the overflow. In debug a `debug_assert_eq!` (`structural_graft.rs:52`) caught it; in
release it was silent, and because `stream_document_to_writer` prefers `virtual_data`
(`document/io.rs:151`), the next `:w` wrote the mangled bytes to disk. Reproduced end-to-end through the
public `EngineHandle`.

The fix makes the transport byte-authoritative: `SemanticRequest.content` is `Vec<u8>`, topology is
parsed from the document's own bytes, and deep (string-oriented) semantic analysis is invoked only when
the bytes are valid UTF-8 — a non-UTF-8 file still gets topology and lexical highlighting, just not deep
analysis. As defense in depth, `apply_graft` is now fallible: a mutation-free `validate_graft` preflight
(source length == document length, topology length == source length, every interior node's children
exactly cover its extent) rejects a non-byte-identical graft **before** the old tree is detached, so a
refused graft leaves the document untouched — the same ordering as `f6ec891`.

This establishes the invariant the caret-anchor spec depends on: **a semantic graft is byte-identical by
construction**, so carets need no rebase across one (only `(node, offset)` cache invalidation).

## 7. The fork

### Option 1 — Preserve pane/tab-owned documents; expose a Baryon-specific document-view model

Keep `DocTab` owning its `DocumentSession`. Give scripts a stable `DocumentId` (minted per `DocTab`,
never reused) and a `PaneId`, and say plainly that Baryon has **no buffer/window split**: a document is
a view.

- **Consequences.** No change to §5's six write hazards, because sharing never happens. No fold
  problem (§4.1). Cheapest by a wide margin, and it honors the decision recorded in `plans/doctab.md`.
- **Costs.** Opening one file in two panes remains two divergent documents with two undo histories over
  one file on disk — a data-loss shape users will hit and report. Every NeoVim plugin idiom built on
  "the buffer" (`nvim_buf_*`, one buffer many windows) has no analogue, and porting guides must say so.
- **What it does not answer:** §4.2 (search state fits nowhere) and §6.1–6.7 remain.

### Option 2 — Extract a stable `BufferId` and a buffer store; tabs and panes become views

A `BufferStore` owns `DocumentSession` (root, ledger, path, shadow, dirty). `DocTab` becomes a view:
`{ buffer: BufferId, cursor, view, search_matches, match_index }`. Path-keyed dedup makes two tabs on
one file share one buffer.

- **Consequences.** Fixes the divergent-history shape. Gives scripts the NeoVim object model.
- **Costs.** Requires resolving **all six write hazards in §5**, and the caret problem (§5.1) is not a
  refactor — carets are `NodeId`-relative pointers into a tree that edits splice. Either every view's
  carets are rebased on every edit (the graft already shows the pattern: snapshot to `DocByte`,
  restore after) or `Caret` stops being node-relative.
- **And it forces the fold decision.** Folds are per-`NodeId` and process-global (§4.1). Shared buffers
  mean shared folds unless fold state moves out of the registry into the view — which is a change to
  the registry's storage model, not to the session.

### Option 3 — Share only immutable content; keep independent mutable histories and views

Two views share the *sparse backing* (the physical, saved-file-backed leaves) but each owns its own
mutable overlay, ledger, and view state.

- **Consequences.** Sidesteps §5's write hazards by construction: no two editors ever mutate one root.
  Memory-efficient for the read-mostly huge-file case Baryon is built for, and it fits the SVP model,
  where clean leaves are already device-backed and evictable.
- **Costs.** It is **not** the buffer model anyone expects: two views of one file would still show
  divergent *content* the moment either edits. It arguably formalizes the current bug rather than
  fixing it. And "immutable content" is exactly the state that is *already* shared (the registry is
  global) — so this option's real content is a rule about what may *not* be shared, plus a story for
  how a save in one view invalidates the other's backing (which is §6.1's device-id problem again).

## 8. What actually has to be decided

Not "can panes share a buffer" — reads already work, and the `Vec<DocTab>` is a two-line change.

The decisions are:

1. **Is a caret a pointer into the tree, or a document byte?** Everything in Option 2 follows from
   this, and the graft already demonstrates the byte-based pattern (`semantic.rs:83-93`).
2. **Where do folds live?** They are the only state that actively forbids the Vim model, and they live
   in the registry, not the session.
3. **What is dirty, and what is undo, attached to?** Today: the buffer, correctly, and fused
   (`is_dirty()` is derived from the ledger). This is the one thing that would survive any of the
   three options unchanged.
4. **Does semantic work belong to the buffer or the view?** Today it is buffer *data* routed by a
   view-owned `RequestId` (§5.3). Under any sharing model that routing must change.
5. **What does a stale handle observe?** The precedent exists (§2.3): two-factor `RequestId` +
   `StateId`, with silent drop on no claimant. But `StateId` is reused (§2.1), so that precedent is
   not directly reusable for a script-visible handle.
