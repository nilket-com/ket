# Specification: Engine Event Bus

Issue-inventory item 6 of `plans/rune-scripting.md` (the fire points, deferred queue, and drain point
of settled decision 5). This spec defines the **engine-level** event substrate: what fires, what an
event carries, when the queue drains, and what happens under overflow — **with zero subscribers**. It
is deliberately **Rune-independent**; Rune autocmds and background-job completions (items 7, 9) become
thin consumers of this substrate later and add no delivery semantics of their own.

Inherited and not reopened: object identity (`BufferId`/`ViewId` durable ids, `StateId` content
version — `plans/object-identity.md`), byte-text semantics and the whole-leaf hydration/outcome model
(`plans/byte-text-semantics.md` §5), the non-reentrant VM and deferred-event rule
(`plans/rune-scripting.md` §2.2, §2.4–2.5), and the runtime-registry owner model
(`plans/runtime-registry-overlay.md`).

## 1. Ground truth this builds on

- **One ordered request stream.** `EngineRequest = Command | Input | Internal` over a bounded
  `sync_channel(2_048)` (`request.rs:4,7`); `InternalEvent = Refresh | ClearYankFlash |
  PathCompletion | RuneCompile` (`request.rs:14`). Cross-thread completions (SVP, semantic pass) send
  `InternalEvent::Refresh` (`svp/semantic.rs:122`) — a render wake with **no attribution**.
- **The deferred-event rule is already law** (settled decision 4–5): host functions and command
  handlers must not synchronously invoke script; events are *enqueued* and drained **after the current
  callback returns and before rendering or blocking on the next request**.
- **Mutations coordinate through one seam** — `with_text_buffer_mut` (`session.rs:1641`) →
  `EditorSession::mutate_text_buffer` (`session.rs:2749`). **Every** text mutation passes through this
  coordinator: interactive edits, search/substitute, undo/redo, reloads, semantic work, and the
  bounded buffer API (whose `commit_prepared_edits` is one caller among many). Sibling-view caret
  fan-out happens inside it; the ledger records the `StateId` transition. Preflight failure leaves
  bytes and `StateId` unmoved.
- **Buffer/view lifecycle** flows through `BufferStore::insert/remove` (`session.rs:347,427`) and the
  layout's view creation/close paths; ids are never reused.
- **Source lifecycle** flows through `RuneHost::publish_compiled`/`unload_source` keyed by `SourceKey`
  (`lifecycle.rs:111,368`), with generation replacement already atomic.
- **Cold-read completion** is a whole-leaf outcome keyed `(NodeId, content_generation)`
  (`resolver.rs:335`, byte-text-semantics §5); the engine currently learns of it only via the
  unattributed `Refresh`.

## 2. Shape: a session-owned deferred queue, not a channel

The bus is a **`VecDeque<EngineEvent>` owned by `EditorSession`**, *not* a new mpsc channel. Session
ownership is the concrete choice: the mutation coordinator, lifecycle attach/detach operations, and
focus transitions are all session methods and push directly; the run loop reaches the queue through
the session it already holds (via `EditorHost`'s `Deref`).

Helpers that currently operate on split `buffers`/`layout` borrows (notably the static
`with_text_buffer_mut` → `mutate_text_buffer` chain) receive an explicit `&mut EngineEventQueue`
alongside those borrows. They do not rediscover transitions at the request boundary or maintain a
parallel mutation list; the coordinator that already owns the before/after state remains the one fire
point.

- **Fire = push.** Any code running on the engine thread (command handlers, host functions, the drain
  itself) appends; firing never blocks, never re-enters, and needs no locking.
- **Cross-thread completions do not fire events directly.** Worker threads keep sending
  `InternalEvent`s through the existing ordered stream; the engine-side *handler* of that internal
  event is the fire point (§4). This keeps exactly one ingress ordering authority — the request
  channel — and the bus strictly engine-local.
- Subscribers (later) are registry-owned callbacks dispatched **at the drain point only**. This spec
  ships the queue, the fire points, the drain, and the overflow/ordering contract, with the
  subscriber table present but empty.

## 3. Post-commit events only

An event is a **statement of fact about state the engine has already committed** — never a hook, never
cancellable, never pre-state:

- **Emission depends solely on the observed state transition, not on the enclosing operation's
  outcome.** `mutate_text_buffer` accepts an arbitrary `FnOnce -> R` (`session.rs:2749`) — an
  error-shaped return does *not* mean nothing committed, and Rune callbacks are deliberately
  non-transactional (they may commit an edit and *then* fail). The rule: the coordinator compares the
  ledger/root **before and after** the closure — no transition → no content event; a committed
  transition → the event fires, **regardless** of the enclosing operation's eventual error; a later
  callback failure never purges an already-enqueued event. A preflight-only or `NotReady` mutation
  produces no transition and thus emits nothing.
- The same rule for every kind: `BufferOpened` after the **successful high-level attach** (§4 — never
  the raw store insert), `BufferClosed` after the detach, `SourceLoaded` after generation
  publication. There are no `Will*`/`Before*` events in this cut — pre-events are a veto/reentrancy
  surface and are a **non-goal** (§10).
- Consequence: an event consumer can always resolve the event's subject against live state *or*
  observe it already gone (`BufferClosed`); it can never observe a half-applied transition.

## 4. Event taxonomy and payloads

Payloads are **owned identity/version snapshots — never live borrows, never `NodeId`**, and never
content. `NodeId`/roots are internal topology (object-identity); content is re-queried through the
bounded read API against the carried `StateId`.

```
EngineEvent =
  // lifecycle — lossless (§5)
  | BufferOpened   { buffer: BufferId, state: StateId }
  | BufferClosed   { buffer: BufferId }                   // id is dead; no state
  | ViewCreated    { view: ViewId, buffer: BufferId }
  | ViewRetargeted { view: ViewId, old_buffer: BufferId, new_buffer: BufferId }
  | ViewClosed     { view: ViewId, buffer: BufferId }
  | FocusedTextViewChanged { from: Option<ViewId>, to: Option<ViewId> }
  | BufferEdited   { buffer: BufferId, acting_view: ViewId,
                     old_state: StateId, new_state: StateId,
                     delta_count: u64, deleted_bytes: u64, inserted_bytes: u64 }
  | BufferReplaced { buffer: BufferId, old_state: StateId, new_state: StateId }
  | SourceLoaded   { source: SourceId, owner: RegistryOwnerId }
  | SourceUnloaded { source: SourceId, owner: RegistryOwnerId }
  | EventsLost     { dropped: u64 }                       // overflow marker, §5
  // wakes — coalescible (§5)
  | HydrationCompleted { buffer: BufferId }                // attribution: §7
```

- **`ViewRetargeted` exists because `:edit` preserves the `ViewId` while changing its `buffer_id`**
  (`runtime.rs:683`, `session.rs:1509`). Neither `ViewClosed` nor `ViewCreated` is truthful there,
  and without it subscribers silently retain the stale view→buffer mapping. Ordering for a retarget:
  new `BufferOpened` (if the target buffer is new) → `ViewRetargeted` → old `BufferClosed` (if its
  last view detached).
- **Focus is text-projected in this cut: `FocusedTextViewChanged`, with `None` = "focus is currently
  synthetic."** A plain `FocusChanged { to: ViewId }` would conflict with text-only lifecycle scope —
  focus can move to a file-browser/preview view (`session.rs:3106`) the subscriber never saw created.
  It fires at **each actual change of the focused-text projection**, not once per request, so a
  lossless `A→B→A` sequence survives as both transition events (`A→B`, then `B→A`). An initial
  `None→A→B→A` sequence has three; synthetic→synthetic remains `None→None` and emits nothing.
- **No `RenderRequested`.** Render scheduling is control flow, not a committed fact — it would
  contradict §3's defining rule, has no useful subscriber contract, and reintroduces the
  refresh/render feedback risk §6 avoids. `InternalEvent::Refresh` keeps setting `needs_render`
  outside the bus. If plugins ever need render notification, a post-render
  `RenderCompleted { viewport }` is a different, well-defined event (§10).

- **Scope: buffer/view events cover text buffers only** in this cut, matching the byte-text-semantics
  scope. `FileBrowser`/`MarkdownPreview` buffers (`session.rs:194`) have no `StateId` and separate
  narrower capabilities; extending lifecycle events to them is deferred (a fixed `BufferKind` +
  `state: Option<StateId>` is the named shape if ever needed) rather than papering over the type now.
- **Edit events fire from the coordinator, not one caller.** The fire point is
  `mutate_text_buffer`/`with_text_buffer_mut` (`session.rs:1641,2749`) — **after sibling caret
  fan-out completes** — with the summary derived from the **ledger transition**, not from any
  caller's delta list. This catches interactive edits, search/substitute, undo/redo, and the buffer
  API through one seam. A **root/generation replacement** (reload) has no ordinary delta list and
  fires the distinct **`BufferReplaced`** — deliberately not a `BufferEdited` with a fabricated
  whole-buffer summary, because consumers must invalidate everything rather than attempt incremental
  handling, and the type should force that distinction.
- **Lifecycle events fire from successful high-level attach/detach operations, never from raw
  `BufferStore::insert/remove`** (`session.rs:347,427`): the store insert is often followed by
  fallible view construction with `remove` on rollback, and firing at the store would report
  transient buffers that never became live. The fire points are the session-level open/attach and
  close/detach operations after they fully succeed — consistent with §3's post-commit rule.
- **`SourceId` is a new opaque, monotonic id stored in `SourceSlot`.** The host's `SourceKey`
  (`lifecycle.rs:111`) is private and carries a `PathBuf`; putting it in `EngineEvent` would couple
  the Rune-independent bus to Rune lifecycle internals and give events variable-size payloads. The
  id-to-key mapping stays host-side; a future consumer resolves it through the host's API **while
  the slot is live only** — `unload_source` removes the slot (`lifecycle.rs:642`), so after an
  explicit unload a `SourceId` is **equality-only** (it still matches the `SourceUnloaded` event a
  consumer holds, but resolves to nothing). No tombstone mapping; ids are never reused, so equality
  stays unambiguous.
- `BufferEdited` carries the byte-delta **summary**, not the deltas themselves: enough to decide
  relevance and cheapness, with content re-read explicitly bounded. The full delta list stays out of
  the payload — it can be megabytes and would turn the queue into a second undo log.
- **No retrospective events.** Registration of a future subscriber emits nothing for pre-existing
  buffers/views/sources; a subscriber initializes by querying live state, then tracks transitions.
  This is the same resynchronization surface `EventsLost` requires (§5) — recorded there as a
  prerequisite for shipping any subscriber.
- **Adding a kind is additive**; consumers must ignore unknown kinds. The enum is `#[non_exhaustive]`
  toward future subscribers.

## 5. Per-kind overflow semantics

The queue is bounded (`MAX_PENDING_EVENTS`, §9). Kinds declare one of two classes:

- **Lossless (lifecycle):** `BufferOpened/Closed`, `ViewCreated/Retargeted/Closed`,
  `FocusedTextViewChanged`, `BufferEdited`, `BufferReplaced`, `SourceLoaded/Unloaded`. These are
  *state transitions*; dropping one desynchronizes every consumer's model permanently. They are never
  silently dropped.
- **Coalescible (wakes):** `HydrationCompleted`. Push merges with an already-pending equal event
  (same kind + same `buffer`); a thousand leaf completions for one buffer collapse to one wake. Wakes
  carry no per-occurrence information by construction (§7), which is what makes coalescing lossless
  in effect.
- **Overflow — one rule for every kind:** **any** push that cannot fit (a lossless event, *or* a new
  *distinct* coalescible wake that has no pending entry to merge into) clears the queue to a single
  cumulative **`EventsLost { dropped }`** marker. The marker is a **resynchronization barrier**, not
  an ordinary event followed by replayable transitions. Precisely:
  - `dropped` counts **discarded queue records** (a coalesced wake counts as the one record it is),
    plus one for the event whose push triggered the clear. It measures queue loss, not original
    occurrence counts — occurrence counts were already erased by coalescing, and the marker's meaning
    is "your model is invalid," not a statistic.
  - **While the marker remains pending, every later push is absorbed into it:** increment `dropped`
    (saturating at `u64::MAX`) and keep the queue as exactly that one marker. No exact event may sit
    behind it. Otherwise an `EventsLost` handler could enumerate current state — which already
    includes those later committed transitions — and then process the same transitions again.
  - When the drain captures the marker, the live queue becomes fresh. The marker is bus control, not
    an ordinary autocmd: the future subscriber layer must rebuild each subscriber's baseline from the
    complete enumeration surface before normal event delivery resumes, without exposing a mutable
    editor host during that recovery. Events committed only after the marker has been captured queue
    normally for the next pass and therefore follow the rebuilt baseline exactly once.
  - That recovery surface (all-buffer/all-view/all-source enumeration) does **not exist yet** —
    recorded here as a hard prerequisite: **no subscriber ships until a complete, read-only
    resynchronization path exists** (it is the same surface no-retrospective-events registration
    needs, §4). The zero-subscriber cut only needs the marker/barrier semantics pinned.
  - The cap is sized so overflow fires only under pathology (a script editing in a tight loop *is*
    the pathology; the host quota already bounds it per turn).
- `FocusedTextViewChanged` is **lossless, not coalesced**, in this cut: collapsing `A→B, B→A` to
  nothing versus `A→A` is a semantic choice consumers should make, not the queue. Revisit only with
  profiling evidence.

## 6. The drain point and ordering

- **One drain point** in `run_engine`, exactly where settled decision 5 puts it: **after the current
  request's handling (including any callback) returns, before rendering and before blocking on
  `rx_request.recv()`**. Draining is a loop: pop front, dispatch to subscribers (today: none; the
  characterization harness, §8), repeat.
- **Events raised during draining are deferred to the next pass.** The drain services only the events
  pending when it began (swap the queue, or drain a captured length); anything a subscriber's callback
  fires — including via host functions — lands in the fresh queue and waits for the **next** drain
  pass, which runs after the *current* drain completes (the drain point loops until the queue is empty
  or `MAX_DRAIN_PASSES` (§9) is hit, then defers the remainder to the next request cycle). This bounds
  cascade depth structurally — no event-fires-event-fires-event recursion inside one pass — while
  preserving the no-reentrancy rule: a subscriber never runs inside another subscriber's frame.
- **The self-wake is a distinct `InternalEvent::DrainEvents`, sent with `try_send`.** Not `Refresh`:
  reusing it would request renders the deferral doesn't need and blurs scheduler control into event
  delivery. `try_send` suffices: success guarantees a future drain; `Full` means the request queue
  already holds another ordered request, and **every** request's handling is followed by the drain
  point, so a drain is guaranteed regardless. The remainder is never stranded.
- **Startup gets one explicit drain.** `run_init`, initial-file loading, and the optional first
  render all happen before the first `recv()` (`runtime.rs:2110`), so the request-tail drain cannot
  service events raised during initialization. One startup drain runs **after init and initial-file
  loading, before the first render or blocking receive**. This also gives init-registered future
  subscribers precise semantics: transitions committed during init become observable only after
  `main` returns — never mid-init.
- **FIFO within the queue, and fire order = commit order.** Because firing is synchronous with the
  committing code on one thread, queue order **is** the order the transitions happened. Guarantees:
  - `BufferOpened` precedes any `ViewCreated`/`BufferEdited` for that buffer; `ViewClosed` for its
    views precedes `BufferClosed`; nothing referencing an id follows its `Closed` event.
  - When the focused text view closes, `FocusedTextViewChanged` is enqueued before that view's
    `ViewClosed` (both after the close has committed), so the focus transition is the last event that
    may reference the dying `ViewId`. Creating and focusing a new view orders `ViewCreated` before
    `FocusedTextViewChanged`.
  - **Multi-view fan-out order:** one committed edit fires **one** `BufferEdited` — per buffer
    transition, not per view. Sibling-view caret rebasing is part of the same commit and is complete
    before the event is observable; consumers needing per-view consequences query the (already
    consistent) views. When a structural change affects several views (e.g. a pane close removing
    two), the per-view events fire in **deterministic layout-traversal order**, the same order
    `collect_view_ids_for_buffer` walks.
  - **Source lifecycle order:** a `:source` replacing a live generation fires `SourceUnloaded { old
    owner }` **then** `SourceLoaded { new owner }`. Both fire **after** the atomic replacement and
    drain together, so this is a *record of the transition sequence*, not an observable intermediate
    state — a consumer handling `SourceUnloaded` already runs with the new generation live. What the
    order guarantees is bookkeeping coherence: a consumer folding events over owner ids never holds
    two live generations of one source simultaneously. A failed compile/publish fires nothing
    (post-commit rule).
- **Undo/redo fire `BufferEdited`** with `old_state`/`new_state` set to the actual token movement
  (revisited `StateId`s per caret-anchors §3.3); consumers treat `new_state` as ground truth, not as a
  novel version.

## 7. Cold-read completion attribution

**Decision: a buffer-level readiness wake, not a leaf-level report.** `HydrationCompleted { buffer }`
means "readiness for this buffer may have changed — re-query what you were waiting for."

- **Why not richer:** the durable outcome already lives in the generation-checked cache
  (`(NodeId, content_generation)`, byte-text-semantics §5) — the retry *observes* `Ready` or
  `ColdReadFailed` there. Duplicating range/outcome into the event would leak `NodeId`/extent
  topology into payloads (§4 forbids it), and would make coalescing lossy. Success/failure are
  deliberately **not distinguished** in the wake: both mean "retry now"; the retry gets the truth.
- **Why buffer-level, not global:** today's bare `Refresh` forces every waiter to re-poll everything.
  Carrying `BufferId` lets a future autocmd/job consumer filter without re-querying unrelated
  buffers. It's the identity level §4 already permits.
- **No `StateId` on the wake.** Hydration does not advance content state (it materializes existing
  bytes); attaching the current version would falsely imply a content transition.
- **Ownership invariant (established and tested, not assumed).** Whole-leaf requests deduplicate by
  `(NodeId, content_generation)`; capturing one `BufferId` at admission is only correct if no second
  buffer can wait on the same key. The invariant this rests on: **a live `NodeId` belongs to exactly
  one `BufferId`** — every buffer owns its own document root and tree, and `CanonicalPath` dedup means
  one *buffer* per file, not shared nodes between buffers. This spec **asserts and tests** that
  invariant (open two views + attempt every same-file path; assert no cross-buffer node sharing) as a
  prerequisite of single-owner attribution. If the invariant ever breaks (e.g. future
  copy-on-write trees), attribution must move to a bounded waiter set per hydration key — named as
  the fallback, not designed here.
- **Completion suppresses wakes for dead buffers.** The engine-side handler resolves the echoed
  `BufferId` against the live store before firing; a buffer closed while its hydration was in flight
  produces **no** `HydrationCompleted` — otherwise the wake could follow `BufferClosed`, violating
  §6's nothing-references-an-id-after-its-`Closed`-event guarantee. The outcome cache entry becomes
  **unreachable from live buffers** (`NodeId`s are not freed); generation checking keeps any residual
  entry safe.
- Mechanically: the SVP completion path continues sending an internal event cross-thread; the new
  internal event carries the `BufferId` resolved at *request-admission* time (the engine knows the
  buffer when it admits hydration; the completion echoes it back). The engine-side handler
  liveness-checks, fires `HydrationCompleted` onto the bus, and marks render. The unattributed
  `Refresh` remains for pure render pokes, outside the bus (§4).

## 8. Testing (zero subscribers — a test-only characterization tap)

A `#[cfg(test)]` subscriber records `Vec<EngineEvent>`; production dispatch stays a no-op over an
empty table.

- Transition-observed emission: a failing/`NotReady` mutation emits nothing; a committed edit emits
  exactly one `BufferEdited` whose `new_state` equals the buffer's post-commit `StateId`; undo emits
  with the revisited token; **a closure that commits an edit and then returns an error still emits**
  (§3 — emission follows the ledger transition, not the operation's outcome), and the enqueued event
  survives the failure. **Every coordinator caller fires:** an interactive edit, a
  search/substitute, an undo, and a buffer-API edit each produce one `BufferEdited` through the same
  seam; a reload/root replacement produces `BufferReplaced`, not `BufferEdited`.
- Lifecycle correctness: a failed open (store insert followed by rollback `remove`) emits **nothing**;
  a successful open emits one `BufferOpened`; a `FileBrowser`/`MarkdownPreview` buffer emits no
  buffer/view events; subscriber-less session construction emits no retrospective events.
- Retarget and focus: `:edit` in an existing view fires `BufferOpened` (new target) →
  `ViewRetargeted` → `BufferClosed` (old buffer, when its last view detached), with the `ViewId`
  unchanged; focus moving text→synthetic fires `FocusedTextViewChanged { from: Some(A), to: None }`
  and synthetic→text fires `{ from: None, to: Some(B) }`; an `A→B→A` sequence arrives as three
  states but **two transition events**, uncoalesced. Closing focused `A` in favor of `B` emits the
  focus transition before `ViewClosed { view: A, .. }`; creating and focusing `B` emits
  `ViewCreated { view: B, .. }` first; synthetic→synthetic emits no projected-focus event.
- Payload hygiene: events carry ids/versions only (compile-time: no `NodeId`, no borrow, no
  `PathBuf` — the enum's types enforce it); `SourceId` is opaque and monotonic across reload cycles.
- Ordering: open→view→edit→close sequences arrive in commit order; a shared-buffer edit through view
  A fires one `BufferEdited` (not two) with `acting_view = A`; pane close with two views fires
  `ViewClosed` in layout order before `BufferClosed`; `:source` over a live key fires
  unload-then-load with the correct owner ids, both after the atomic replacement.
- Drain discipline: an event fired *by a subscriber during drain* (test subscriber fires one) is
  observed in the **next** pass, not the current one; `MAX_DRAIN_PASSES` exceeded → remainder
  survives, a `DrainEvents` self-wake is attempted, and the remainder drains on the next cycle even
  when `try_send` returned `Full` (another request forces the drain point); events raised by init and
  initial-file loading are drained by the **startup drain** before the first render/blocking receive,
  not stranded until the first request.
- Overflow: pushing past `MAX_PENDING_EVENTS` yields a single `EventsLost` whose `dropped` equals the
  discarded record count; every push while that marker is pending is absorbed into its cumulative
  count (never a second marker, never an exact event behind it); after the drain captures the marker,
  newly committed events enter the fresh queue and arrive on the next pass; a new *distinct*
  coalescible wake at capacity triggers the same collapse while a *mergeable* one coalesces without
  overflow; a flood of `HydrationCompleted` for one buffer coalesces to one entry while a second
  buffer's wake survives distinctly before any overflow.
- Attribution: an admitted cold read whose I/O completes (success and failure both) produces a
  `HydrationCompleted` naming the right `BufferId`, and a retry then observes the cached outcome —
  extending the existing outcome-cache tests; a buffer closed mid-flight produces **no** wake; the
  node-ownership invariant test (no live `NodeId` reachable from two `BufferId`s, including same-path
  reopen) guards single-owner attribution.

## 9. Constants (calibrated at the integration gate)

| Limit | Value | Basis |
|---|---|---|
| `MAX_PENDING_EVENTS` | 4 096 | ≫ any legitimate burst (a 1 024-delta batch is *one* event); inline event storage is asserted ≤ 256 KiB |
| `MAX_DRAIN_PASSES` | 4 | bounds cascade depth; remainder defers with a self-wake |

## 10. Non-goals / deferred

- **Subscribers.** Rune autocmds (item 9 slice) and job completions (item 7) consume this substrate
  later; no subscriber registration API ships here beyond the empty table. **Hard prerequisite
  recorded:** no subscriber ships until the read-only resynchronization surface exists —
  all-buffer/all-view/all-source enumeration — required by both `EventsLost` recovery and
  no-retrospective-events registration (§4–5).
- **Non-text buffer lifecycle events** (`FileBrowser`/`MarkdownPreview`) — the `BufferKind` +
  `Option<StateId>` shape is named in §4 if ever needed.
- **Pre-events / veto hooks** (`BeforeWrite`, cancellable anything) — a reentrancy and atomicity
  surface this design deliberately excludes.
- **Cross-thread event ingress.** Workers keep using `InternalEvent`; the bus is engine-local.
- **Delta-carrying edit events.** `BufferEdited` summarizes; consumers re-read through the bounded
  API.
- **Render events.** Render scheduling stays in `InternalEvent::Refresh`/`needs_render`, outside the
  bus (§4). If plugins ever need render notification, a post-render `RenderCompleted { viewport }`
  is the well-defined future shape.
- **Event persistence/replay**, filtering/pattern language (autocmd patterns are the consumer's
  concern), and per-subscriber queues.
- **File-watch / external-change events** — belong to the reload machinery, a separate fire-point
  addition once that path is reviewed.

## 11. Implementation order

1. **Queue + kinds + overflow classes** — the enum (incl. `EventsLost`, `BufferReplaced`,
   `SourceId`), push/coalesce/overflow logic, unit-tested in isolation.
2. **Drain point** — the swap-and-dispatch loop in `run_engine` with pass bounding, the
   `DrainEvents` self-wake, and the **startup drain**; empty subscriber table, characterization tap.
3. **Node-ownership invariant test** — establish the one-buffer-per-`NodeId` guarantee §7 rests on,
   before any attribution code exists.
4. **Lifecycle fire points** — high-level buffer/view attach/detach/**retarget** seams, the
   text-projected focus transition, and the host's source publish/unload (with `SourceId` introduced
   in `SourceSlot`).
5. **Edit fire point** — `BufferEdited`/`BufferReplaced` from the observed ledger/root transition at
   the `mutate_text_buffer` coordinator, after sibling fan-out, independent of the closure's return.
6. **Hydration attribution** — `BufferId`-carrying internal completion event, engine-side
   liveness-checked `HydrationCompleted` fire, `Refresh` reduced to pure render pokes.
7. **Integration gate** — ordering/overflow/drain tests through the real engine loop; calibrate §9.
