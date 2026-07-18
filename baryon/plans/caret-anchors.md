# Specification: Caret Anchors

## Overview

Replace `Caret { node, offset, abs_line, abs_col, preferred_col }` (`session.rs:142-149`) with an
**anchor** — a `DocByte` plus an affinity — and make `node`/`offset` and `line`/`col` **derived caches**,
with `preferred_col` promoted to **independent view state**.

This is stage 2 of `plans/buffer-store.md` §12. §5.2 of that spec requires it to be its own document:
the `CursorSnapshot` machinery already in the tree is a graft-recovery hack, not an anchor model, and
reusing it would carry its four losses forward (§8.1).

The motivation is shared buffers — an edit in one view must rebase every other view's carets, and only
a byte anchor is rebasable against a delta list. But the model stands on its own: it fixes live bugs
(§9) and rests on an invariant the tree only just acquired.

**Prerequisite already landed.** `e3f4a27` made the semantic pipeline send the reactor the document's
exact bytes and graft those same bytes back, so a semantic graft is **byte-identical**
(`object-identity.md` §6.8). §6.3 depends on that.

Ground truth for identity is `plans/object-identity.md`; the buffer/view split is `plans/buffer-store.md`.

## 1. The problem with the current model

**There are three coordinates and no agreement on which is true.**

- **Edit time** treats `(node, offset)` as authoritative (`plan_point_edit` `runtime/edit.rs:147`,
  `caret_doc_byte` `session.rs:322-324`).
- **Render time** treats `(abs_line, abs_col)` as authoritative and **re-derives** `node`/`offset`/
  `abs_col` for every caret on every frame (`render.rs:307-329`).
- **Snapshot/restore** treats `DocByte` as authoritative (`session.rs:176-180`).

The caches can and do disagree — `render.rs:317-325` has a `Pending` branch that leaves `node`/`offset`
stale and raises `projection_pending`. A caret is not one position; it is three positions that are
usually, but not always, equal.

The anchor model does not add a coordinate. **It names one coordinate as truth — the byte — and demotes
the other two to caches** whose validity is explicit (§3).

## 2. The model

```
TextAnchor { byte: DocByte, affinity: Affinity }        // Affinity = Before | After

Caret {
    anchor: TextAnchor,
    preferred_col: DocDisplayCol,      // independent view state; NOT derived (§4)
    cache: CaretCache,                 // derived from `anchor`; see §3
}

CaretSet { primary: Caret, secondary: Vec<Caret> }      // unchanged shape
```

`Affinity` is **not new**. `CursorAffinity { Before, After }` already exists at `runtime/edit.rs:115-119`
and `transform_cursor_byte` (`runtime/edit.rs:229+`) already implements the rebase this model needs.
Today it lives and dies inside `plan_point_edit`. This spec **promotes it to persistent caret state**
(§10 lists what that requires); `transform_cursor_byte` becomes the reference rebase (§6).

### 2.1 Why a bare `DocByte` is not enough

An insertion exactly *at* a caret is ambiguous: does the caret move with the inserted text (`After`) or
stay before it (`Before`)? A byte cannot express the difference. With one view the acting caret is
placed explicitly at the edit site so it never arises; with two views it arises on every keystroke. The
visual anchor (`session.rs:347`) is already a bare `DocByte` and already has this latent bug.

## 3. The cache, and what makes it valid

This is the crux, and it has **two independent invalidation conditions**, not one. The earlier draft's
"valid iff revision matches" was wrong: it missed that the caret moves.

```
CaretCache {
    // Present only when resolved against the CURRENT anchor. Any write to
    // `anchor` clears the cache (§3.1).
    node: Option<(NodeId, u32)>,         // guarded by node-map revision (§3.2)
    node_rev: TopologyRev,
    line: Option<(DocLine, DocDisplayCol)>,   // guarded by content revision (§3.3)
    line_rev: StateId,
}
```

### 3.1 A cache belongs to an anchor value

The cache is derived from `Caret.anchor`. **Any code that writes the anchor — a motion, a click, a search
jump, a rebase — clears the cache.** Navigation is the case the revision scheme alone does not cover:
`MoveCursor` moves the caret's byte (`navigation.rs:590+`) **without changing any buffer revision**, so a
revision stamp would falsely certify a cache computed for the *old* byte. The anchor and its cache live
in the same struct precisely so this coupling is local: mutate the anchor, drop the cache.

Revisions (§3.2, §3.3) guard the orthogonal case — the tree or content changing **under a stationary
anchor** (a background graft, another view's edit). Both conditions are required; neither subsumes the
other.

### 3.2 `TopologyRev` — the node-coordinate-map revision

`node`/`offset` is the position of a byte **within the tree's leaf structure**. It changes whenever the
byte→node/offset mapping changes — which is **not** only when edges are rewired. Per the review:

- Virtual insertion mutates a leaf **in place** (`mutation.rs:274`); resident physical insertion can
  promote in place (`mutation.rs:293`); most deletion virtualizes one leaf (`mutation.rs:480`). These do
  not splice siblings — but they change a leaf's **byte length**, which shifts every byte→node/offset
  result after it. So `TopologyRev` is a **node-coordinate-map revision, not a "tree shape" revision**;
  the earlier draft's "byte edits splice nodes" was overstated.

`TopologyRev` is a **new monotonic per-buffer counter**, bumped by: any byte edit, a graft, a reload, a
generation swap, **and synthetic fold materialization** (`system.rs:1039`, `synthetic.rs:442` — a
production fold command allocates and rewires child chains without changing bytes, exactly like a graft).
It must **not** reuse `next_registry_topology_revision` (`minimap.rs:174`), which returns the
registry-global `registry.next_id`: a node allocated in *another* buffer would bump it and needlessly
invalidate this buffer's caches. It is a tolerant minimap hint, not a caret key. `node` is read only when
`node_rev` equals the buffer's current `TopologyRev`.

**Failure semantics are conservative: bump `TopologyRev` *before* publishing any potentially fallible
node-map mutation, and leave it bumped if a later step fails.** Over-invalidation is harmless — a caret
merely recomputes `node` — whereas retaining a stale `node` cache against a tree the mutation *did* touch
is a correctness bug. The counter records "the node map may have changed," which a partial failure does
not undo.

### 3.3 `StateId` — the content revision

`line`/`col` is a function of the bytes and newlines, not the tree shape. `StateId` is **reused** as the
content revision (no duplicate counter is invented). One qualification: `StateId` is a **stable
content-state token, not a monotonic current counter** — allocation is monotonic and ids are no longer
reused (`18f42e0`), but `current_state_id` moves **backward on undo and forward on redo**
(`undo.rs:163`). That is exactly why §3.1 is mandatory: an undo returns to an older `StateId`, and a
`line` cache stamped with that id would appear valid again even though the anchor was transformed through
the undone edit and no longer denotes the same position. **The anchor-clear of §3.1, not the revision,
is what makes this safe.** `line` is read only when `line_rev` equals the current `StateId` *and* the
cache has not been cleared by an anchor write.

| Mutation | `StateId` | `TopologyRev` | Effect on a *stationary* anchor's cache |
|---|---|---|---|
| Byte edit | ✅ | ✅ | both dropped (and the anchor is rebased, so §3.1 clears it anyway) |
| Byte-identical graft | — | ✅ | `node` dropped; `line` kept |
| Synthetic fold materialization | — | ✅ | `node` dropped; `line` kept |
| Reload / generation swap | ✅ | ✅ | both dropped |

A byte-identical graft can also make a **previously unresolved** coordinate resolvable, by replacing a
cold physical leaf with a hot virtual one. That is a **readiness transition**, not a cache validation: it
is represented as "still `None`, now recomputable," never as a revision-stamped valid value (§7).

## 4. `preferred_col` is independent view state

`preferred_col` is the column a vertical motion *wants*, remembered across a run of `j`/`k` over short
lines. It is the user's intent, not a property of position — **it cannot be reconstructed from a byte**,
and any code that recomputes it from `abs_col` destroys it. It lives in `Caret` beside the anchor,
outside `CaretCache`, and survives every recomputation and every anchor clear.

Reads: the `j`/`k` mechanism (`navigation.rs:236-239`, `:329`) and `add_cursor_adjacent`
(`navigation.rs:477/486/517/526`). Written by `sync_preferred_col` after a non-vertical action, reset by
the `reset_preferred_col` list (`navigation.rs:216-235`). Both behaviors are kept; what changes is that
nothing else may touch it — in particular not cache recomputation.

## 5. Anchor resolution

Resolving an anchor computes its cache from the byte. Per the review's decision this **extends the UAST
order-statistic descent** and does not depend on `SumTree` internals (cold extents collapse to one
aggregate entry, `index.rs:229`; `query_by_byte` cannot resolve a line inside it; `splice` is O(n)).

### 5.1 One descent yields node and, when metrics allow, line

`find_node_at_doc_byte_internal` (`projection.rs:796`) descends root→leaf choosing the child whose
accumulated sibling byte-lengths bracket the target byte. **Extend it to accumulate `newlines` alongside
bytes at every level**, so one descent yields the leaf `node`/`offset` and the newline count of all
*complete* leaves before the target — without the separate leaf→root walk `line_col_from_doc_byte_sparse`
does today (`cursor.rs:293-311`).

### 5.2 Complexity — an honest bound

The descent is **O(fanout × tree height)**, where the grouped-graft `Group` spine bounds each sibling
chain to `TARGET_TREE_FANOUT`. It is **not** O(log n): grouping bounds sibling *width*, not *depth*.
`build_green_recursive` (`rust_provider.rs:220`) preserves the source's full syntactic nesting, so a
deeply nested construct has height O(n) in the worst case, and the UAST carries no balance invariant to
prevent it. The fan-out bound is also **not stable after the graft**: later physical insertion splices
siblings without rebalancing (`mutation.rs:325`) and synthetic fold materialization rewires arbitrary
child chains (`synthetic.rs:442`).

So the win here is **not asymptotic**. It is: (a) one combined descent instead of a descent *plus* a
separate leaf→root newline walk *plus* the fragile dependence on `RootChildLineIndex` that the graft
removes (`structural_graft.rs:141`); and (b) height is small for ordinary source (nesting depth, not file
size). A hard worst-case bound would require a balance invariant on the topology, which is a separate
decision and **out of scope** — recorded here so it is not assumed.

### 5.3 Cold leaves, boundaries, EOF, affinity, and the empty tree

The contract must cover the cases a happy-path helper skips. **Line and column are not uniformly
resolvable while cold**, contrary to the earlier draft:

- **Cold leaf, unknown metrics.** A physical leaf can be cold *and* its newline metric still unknown
  while the background scan runs — `exact_line_count` returns `None` (`registry.rs:959`), and the root
  index stores unknown line metrics separately (`root_line_index.rs:18`). Treating a placeholder
  `metrics.newlines` as exact would cache a wrong line under an unchanged `StateId`. **Line resolves only
  when the relevant metrics are known.**
- **Cold leaf, interior byte.** Even with a leaf's *total* newline count known, an interior byte needs
  the newlines *before its offset within the leaf*, which aggregate metrics cannot give; the current code
  correctly scans the leaf prefix (`cursor.rs:281`). So a cold interior anchor generally resolves only
  `(node, offset)`; `line` needs the local prefix bytes.
- **Column may span leaves.** A line can cross several leaves, so computing the visual column requires
  locating the line start (possibly in an earlier leaf) and hydrating each extent up to the anchor. The
  byte descent does not by itself reveal the preceding newline's byte position; that is a separate
  backward search. Column is therefore the most-hydration-hungry field and the last to resolve.
- **Boundary affinity.** A byte exactly between two leaves: `Before` selects the end of the left leaf,
  `After` the start of the right. Both yield the same `(line, col)`; they differ only in `node`/`offset`.
  **This node-side choice only picks a concrete edit target; it does not by itself make passive anchors
  rebase differently** — `transform_cursor_byte` does that from the affinity (§6). The earlier draft's
  causal claim was wrong.
- **EOF / BOF / empty tree.** `byte == total_bytes` is the append position and must be representable
  (`After` clamps there — no right node). `Before` at BOF has no left leaf. **The empty tree is a real
  case**: deleting the final leaf disconnects it via `prune_empty_ancestors`, which anchors at
  `(parent, 0)` when no previous leaf exists (`mutation.rs:225-227`), and byte lookup then falls back to
  the *root itself* as the target (`projection.rs:875`). **Decision: the buffer maintains a zero-length
  sentinel leaf; it is never pruned below one leaf.** An anchor therefore always resolves its `node` to a
  real leaf at offset 0, never to the root-as-fallback — which keeps "a hot anchor resolves to a leaf" a
  true invariant and gives insertion a leaf to target. The empty document is one zero-length leaf; byte 0
  resolves to `(that leaf, offset 0, line 0, col 0)`.
- **Out of range / tombstones.** **Decision, not either/or:** construction guarantees `byte <=
  total_bytes` — anchors are clamped on creation and after every rebase — so out-of-range never reaches
  resolution. At a boundary, the affinity rule skips zero-length/tombstone leaves: `Before` resolves to
  the end of the nearest non-empty left leaf, `After` to the start of the nearest non-empty right leaf,
  falling back to the sentinel only in the empty document. This closes the case where EOF lookup would
  otherwise select a zero-length node (`projection.rs:838`).

## 6. Rebasing

Every buffer mutation produces a `BufferMutation` (`buffer-store.md` §6). Anchors rebase by one rule:

```
rebase(anchor, mutation) -> TextAnchor      // transform_cursor_byte (runtime/edit.rs:229+), honoring affinity
```

This already exists and is correct for the multicursor path; it becomes the **only** caret rebase,
applied by `EditorSession::mutate_buffer` to every attached view.

**`transform_cursor_byte` has a precondition:** it relies on the **descending, non-overlapping** delta
ordering that `plan_point_edit` establishes (`runtime/edit.rs:195`, `validate_non_overlapping_deltas`
`:215-227`). A `BufferMutation` used for rebase must carry deltas in that contract, or define its own; it
cannot be an arbitrary bag. This is a requirement on `buffer-store.md` §6's `BufferMutation`, surfaced
here.

### 6.1 The primary is currently rebased twice, by two mechanisms

`apply_point_edit` (`runtime/edit.rs:1782-1835`) computes an "after" snapshot for all carets via
`transform_cursor_byte` (`:197-206`), then calls `apply_deltas_to_document_internal`, which
**independently** rebases the primary by round-tripping `DocByte → (line, col) → (node, offset)`
(`document/edit.rs:524-535`) — only the primary, because its signature takes one caret's out-params. The
snapshot is re-applied over the top (`:1830`). **The `line_col` round-trip is deleted**; the delta
transform is the only rebase.

### 6.2 The four-out-param signature goes

`&mut NodeId, &mut u32, &mut DocLine, &mut DocDisplayCol` is threaded through
`place_cursor_at_line_col`, `apply_deltas_to_document{,_internal}`, `apply_document_deltas{,_with_cursor_override}`,
`apply_single_delta`, `handle_navigation_command`, `handle_edit_command`, `handle_search_command`,
`handle_fold_command`, `reload_loaded_document`, and the sort/uniq/file leaves. This is the bulk of the
mechanical diff, and it happens in **two moves that must not be conflated**:

- **Transitional signature (stage 6).** Each four-scalar block collapses to a single
  `&mut LegacyCaretProjection` — the four coordinates already bundled by `legacy_projection` /
  `install_projection` (`session.rs`). This is **strictly mechanical, zero logic change**: the delta chain
  keeps mutating four coordinates, now reached through one struct instead of four `&mut`s. **All caret
  logic — topology/state revisions, affinity choice, cache installation, and pending-target handling —
  stays in the doctab wrappers**, which continue to bridge `Caret ↔ LegacyCaretProjection`. Tests and the
  file-load-into-empty path construct a `LegacyCaretProjection` directly, with no synthetic `Caret`.
  Teaching the delta chain to mutate a `Caret` here would add coupling in the wrong layer only to strip it
  out one stage later.
- **End state (stage 7).** `LegacyCaretProjection` and the wrapper bridges are **deleted**, not migrated.
  The delta chain stops handling cursors entirely (the double rebase collapses, §6.1), placement moves
  onto the anchor model directly, and the acting caret is what remains. The "acting `&mut Caret` /
  `&mut CaretSet`" end state lives here, alongside the graft round-trip removal (§6.3) — because that is
  the same machinery: `legacy_projection` / `install_projection` **is** the snapshot round-trip this model
  exists to delete.

### 6.3 Grafts need no anchor rebase

A semantic graft is **byte-identical**: the pipeline sends the reactor the document's exact bytes and
grafts those same bytes back (`e3f4a27`). So **anchors are untouched by a graft** — no rebase, no
snapshot, no restore — and only `TopologyRev` bumps, dropping `node` and keeping `line` (§3).

Precise attribution: byte identity comes from the **state-gated semantic pipeline**, which sends and
returns the same `Vec<u8>`. `validate_graft` (`structural_graft.rs:55`) is a **backstop**, not the
source of the guarantee — it checks document length, payload length, and child coverage, so a same-length
altered payload would pass it. The invariant rests on the pipeline; validation catches the length/coverage
class of malformed topology before the old tree is detached.

This is the point of the model. The snapshot/restore round-trip at `semantic.rs:94` exists solely because
carets hold `NodeId`s the graft invalidates. Anchors do not, so the round-trip — and both bugs it causes
(§9.1, §9.2) — cease to exist. The invariant was not safe until `e3f4a27`; before it, a graft could
change bytes and this claim would be false.

## 7. Failure and readiness

`caret_from_doc_byte` (`session.rs:326-340`) is **fallible** today: it propagates
`"Byte-to-line index is still building"` out of `restore_snapshot` into `semantic.rs:94`, which returns
`false` **after** the graft has run — stranding carets in a detached subtree (§9.2).

Under this spec **installing an anchor cannot fail** — an anchor is a byte. Only *resolving its cache*
can be incomplete: a cold leaf, unknown metrics, an un-hydrated line prefix (§5.3). A caret with `node ==
None` or `line == None` is a **legal, renderable state** that completes on a later frame.

**A pending cache needs a concrete representation and a wake-up rule.** Metric inflation and leaf
hydration complete **without bumping `StateId`** (they change residency, not content), so revision
equality cannot signal readiness. The spec must define: how an unresolved field records what it is
waiting on (a scan of node N, the bytes of line L), and what re-triggers resolution when that arrives —
the existing SVP refresh bridge (`host.rs:72-81`) turns I/O completion into an `InternalRefresh`, which is
the natural wake, but the caret must re-attempt resolution on that refresh rather than assume a stamped
value. This is left as a defined open contract (§11), not hand-waved.

## 8. Snapshots and undo

### 8.1 Snapshots are lossless

`CursorSnapshot { primary: DocByte, secondary: Vec<DocByte> }` (`session.rs:176-180`) loses four things:

| Loss | Where | Consequence |
|---|---|---|
| `preferred_col` dropped | `session.rs:222-232` | §9.1 |
| Visual mode dropped | `session.rs:438-441` (empty secondary, no kind) | restore forces `Points` (`:445-453`); visual survives only because `semantic.rs:95` re-applies it |
| Secondary order destroyed | sort+dedup+retain (`:226-230`) | caret identity across a restore is positional |
| Carets truncated | `from_snapshot` (`:315-317`) | **defensive, not a demonstrated live bug** — production snapshots come from an already-capped `CursorSet` (`:258`); the replacement simply avoids the silent-loss shape |

The replacement is **the caret set itself** — anchors (byte *and* affinity), `preferred_col` per caret,
and the mode:

```
CaretSnapshot {
    mode: Points { primary: Caret, secondary: Vec<Caret> }
        | Visual { caret: Caret, anchor: TextAnchor, kind: VisualKind },
}
```

Carets are anchors, and anchors are tree-independent, so **a snapshot is the caret set with its caches
dropped** — no lossy projection step. `CursorMode` keeps `Points`/`Visual` mutually exclusive
(`session.rs:342-350`); multi-caret visual selection stays out of scope (§12). Because the mode is *in*
the snapshot, restore restores it, and the `active_visual`/`set_active_visual` dance at
`semantic.rs:94-96` is **deleted** — along with the latent hazard in `set_active_visual`
(`session.rs:409-420`) that would collapse a multicursor set to its primary if one survived a restore
alongside visual mode.

### 8.2 Undo snapshots are a portable view snapshot in buffer-owned history

Undo history lives in `TextBuffer.ledger` (`buffer-store.md` §3). Its caret data is **not buffer state**:
mode, carets, and `preferred_col` are view state. It is a **portable, view-neutral view snapshot stored
in buffer-owned history** — portable because it names no view and its anchors are `DocByte`s meaningful
against the buffer content at the entry's `StateId`. Per the review's decision:

- The snapshot records **no originating `ViewId`.** On undo/redo it is applied to **the invoking
  `TextView`**, whichever that is.
- **The invoking view is not exempt from the rest of the mutation.** Only its *carets* come from history;
  it still processes the undo/redo `BufferMutation` for **folds, scroll, search position, and cache
  invalidation** exactly as any view does. §8.2's "other views" wording must not be read as excluding the
  invoking view from non-caret fan-out.
- **Every other attached view rebases its carets** through the `BufferMutation` (§6).

Two holes this exposes, which the spec must close rather than assume:

- **Generation-swap undo has no delta mapping.** `SwapGeneration` stores two paths and one cursor byte
  (`undo.rs:20`), not deltas — the content is *replaced*, not edited. "Rebase through the mutation" is
  empty here. The policy is the same as the fold-reload case (`buffer-store.md` §4.3): on a generation
  swap, **other views' carets are clamped to valid bytes (or reset), not transformed**, because no
  trustworthy position mapping exists.
- **History creation is not yet lossless.** `push_group` synthesizes one primary byte (`undo.rs:90`),
  generation entries store a bare byte, and `restore_generation_history` fabricates byte zero
  (`undo.rs:259`). The staging plan must migrate **every** history variant and recovery path to the
  lossless `CaretSnapshot`, not only the common `TextDeltas` path.

**Folds and scroll are not in undo history.** They are view state; undo restores carets, not layout.

### 8.3 Undo replay is fallible and mutates before it succeeds

`undo()` pops and moves `current_state_id` **before** replay (`undo.rs`), and replay then applies deltas
or loads a root, which can fail (`runtime/edit.rs:1909`). A failed replay leaves history and state
advanced with no coherent buffer mutation — and under shared buffers that means other views would rebase
against a mutation that did not fully happen.

**The contract is mutation-free preflight then infallible commit** (`buffer-store.md` §6.1's preferred
form), *not* "roll back the ledger move." A ledger rollback is insufficient for generation replay, which
publishes the new `root_id`, `backing_path`, and backing device (`runtime/edit.rs:1954-1957`) as part of
the operation: by the time a later step fails the tree has already been swapped, so recovery would demand
a full state rollback, not just a `StateId` restore. The tractable contract is therefore to do all
fallible work — the `load_root_from_path`, the shadow sync — **before** the ledger and root are touched,
so the commit that moves them cannot fail. This is a prerequisite to stage 6, called out here.

## 9. Bugs fixed

1. **`preferred_col` destroyed by every caret reconstruction.** `Caret::new` sets `preferred_col =
   abs_col` (`session.rs:163`); `caret_from_doc_byte` (`:334`) goes through it, so every snapshot restore
   resets it. A response landing between two `j` presses changes where the next `j` lands. Removed by §4
   and §6.3.
2. **A failed restore after a graft strands carets in the detached old tree** (`semantic.rs:89` grafts,
   `:94` restores and returns `false` after the mutation). `e3f4a27` already removed the byte-changing
   variant; the anchor model removes the round-trip entirely (§6.3, §7).
3. **`sync_preferred_col` syncs only the primary** (`session.rs:295-298`) while `move_all_cursors` reads
   `preferred_col` on every caret (`navigation.rs:327`). Reachable through commands that **retain a
   multicursor set but call the primary-only sync** — e.g. a save or `ClearSearch` — after which the next
   `j` scatters the secondaries to stale columns. (Commands that collapse the set, or reconstruct every
   caret, do not expose it.) Fixed as a prerequisite (§10).

## 10. Prerequisite fixes, as explicit prior commits

1. **`sync_preferred_col` must sync every caret** (§9.3).
2. **`PageUp`/`PageDown` must retain `preferred_col`.** They are in neither `retain_preferred_col`
   (`navigation.rs:212-215`) nor `reset_preferred_col` (`:216-235`), and their handlers clamp `abs_col`
   (`navigation.rs:810`), so the next `j` resurrects a stale column.
3. **`PageUp`/`PageDown` move only the primary in a multicursor set.** They preserve the set
   (`runtime.rs:544`) but `multi_cursor_navigation_supported` (`navigation.rs:279`) excludes them, so
   paging leaves every secondary behind. A **separate bug from #2** (fan-out, not `preferred_col`),
   recorded on its own.

Bugs §9.1 and §9.2 are **not** independently fixable in the current model — one *is* the `Caret::new`
signature, the other *is* the snapshot round-trip — so the refactor is **not** zero-behavior-change, and
§11 says so.

## 11. Contracts resolved during implementation

- **Affinity lifecycle.** Fresh carets, clicks, search jumps, and motions use `After`; a visual anchor
  uses `Before`. Point edits retain the existing command semantics: insert uses `After`, while
  backspace/delete/replace/toggle-case use `Before`. Secondary carets inherit view state until placed,
  then use `After`; undo restores the affinity recorded in its snapshot. File operations that do not
  move the caret preserve its affinity.
- **Same byte, opposite affinity.** Carets deduplicate by byte so one edit site is applied once. The
  primary wins a collision with a secondary; stable secondary order decides secondary/secondary
  collisions. The surviving caret keeps its own affinity and `preferred_col`.
- **`BufferMutation` delta contract** (§6): descending, non-overlapping, or a defined alternative.
- **Pending-cache representation and wake-up.** An unresolved byte-derived line cache stays `None`.
  A line-targeting motion that cannot yet produce a byte records `PendingCaretTarget { line, col,
  reveal_hidden, required_node }` beside the anchor; it is intent, not a second authoritative
  coordinate. Rendering retries resolution on every frame, requests `required_node`, and the existing
  I/O `InternalRefresh` provides the wake-up. Until resolution succeeds the anchor remains unchanged and
  edit commands reject the pending caret.
- **`TopologyRev`'s stage-2 owner.** It initially lives on `DocumentSession` and migrates into
  `TextBuffer` with the buffer store.
- **Revision handling for partial mutation failure — closed by stage 4.** The old byte-apply loop was
  fallible:
  `delete_text_sparse` / `insert_text_sparse` return `?` (`document/edit.rs:494-505`), the former can
  error *after* it has begun mutating (its range-match check is at `:294`), and in a multi-delta group a
  later delta can fail after earlier ones have landed — all *before* `ledger.push_group` advances
  `StateId` (`:519`). So bytes move while `StateId` does not, and a caret could key to a state the buffer
  never committed. Stage 4 hoisted validation ahead of every write and made the byte loop infallible.
  Stage 5 additionally made the trailing `DocByte → (line, col) → (node, offset)` cursor projection
  best-effort derived state, so cold line data cannot turn a committed group into an error before a
  caller records its grouped history. Bytes and `StateId` now advance together or neither moves.

## 12. Staging

1. **Prerequisite fixes** (§10) — three commits.
2. **`TopologyRev`** introduced on `DocumentSession` (§3.2, §11), bumped on every node-map change
   including synthetic fold materialization, with the conservative failure semantics of §3.2. No caret
   change; independently testable.
3. **Extended order-statistic descent** (§5), with `line_col_from_doc_byte_sparse` reimplemented on it.
   No caret change; fixes the redundant leaf→root newline walk independently of anchors.
4. **Atomic multi-delta preflight/commit** (§11 partial-failure contract). The byte-apply loop becomes a
   single commit — deltas validated ahead of any byte write, then applied infallibly, or fully aborted —
   so bytes and `StateId` advance together. **This must precede stage 5, and the dependency is soundness,
   not preference:** without it, the moment `CaretCache` exists a partial edit changes bytes, leaves
   `StateId` unchanged, and leaves a stationary anchor's `line` cache validly stamped against content that
   has already moved — an unsound cache *before* buffer sharing or `BufferMutation` fan-out even exists.
   No caret change yet.
5. **`TextAnchor`** introduced; `Affinity` promoted (§2), with the affinity lifecycle (§11) decided.
   `Caret` gains `CaretCache` with the two-condition validity of §3. Snapshots become lossless (§8.1).
6. **The four-out-param signature collapsed** to the transitional `&mut LegacyCaretProjection` (§6.2).
   Strictly mechanical, zero logic change: the delta chain still mutates four coordinates, now bundled;
   revisions, affinity, cache installation, and pending-target handling all stay in the doctab wrappers.
   No `Caret` mutation enters the delta chain. Tests and file loading build projections directly.
7. **`LegacyCaretProjection` and the wrapper bridges deleted** (§6.2 end state): cursor handling leaves the
   delta chain, placement moves onto the anchor model, and the acting `&mut Caret` / `&mut CaretSet` is
   what remains. This is the same round-trip machinery as **the graft round-trip deleted** (§6.3) and the
   double rebase collapsed (§6.1); undo snapshots become portable and lossless across **every** history
   variant (§8.2), under the preflight/commit replay contract for deltas *and* generation swaps (§8.3).
   Bugs §9.1 and §9.2 die here.

Stages 3 and 7 are each independently valuable and revertable; stage 4 is a hard prerequisite of stage 5.

## 13. Non-goals

- **Multi-caret visual selection** (§8.1). `CursorMode` keeps them exclusive.
- **A balanced-topology / worst-case descent bound** (§5.2). Recorded as a tension, not resolved.
- **Fold storage** — `buffer-store.md` §4, stage 3.
- **Sharing carets across views.** This spec makes carets *rebasable*; attaching two views to one buffer
  is `buffer-store.md` §9, stage 7.
- **`MAX_CURSORS`.** The truncation is made non-silent (§8.1); the cap itself is separate.
- Changing what `preferred_col` means, or the `reset_preferred_col` list beyond §10.
